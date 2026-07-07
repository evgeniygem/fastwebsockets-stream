use bytes::Bytes;
use bytes::BytesMut;
use fastwebsockets::{Frame, OpCode, Payload, WebSocket, WebSocketError};
use futures::FutureExt;
use futures::future::BoxFuture;
use std::fmt::Debug;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Future output type for operations that temporarily own the websocket.
///
/// The future returns either an owned `WebSocket<S>` back together with a
/// result value `T`, or a `WebSocketError` if the operation failed.
type FutureResult<S, T> = Result<(WebSocket<S>, T), WebSocketError>;

/// Internal owned frame representation.
///
/// When we read a frame from `WebSocket::read_frame()` it borrows internal
/// buffers. To be able to return both the websocket and the payload across an
/// `await` point we copy the payload into an owned `Bytes` and store the opcode.
struct PayloadFrame {
    /// Opcode of the frame (Text/Binary/Close/etc).
    opcode: OpCode,
    /// Owned payload bytes of the frame.
    payload: Bytes,
}

/// Read state machine for `WebSocketStream`.
///
/// We encode whether we are idle or currently running an owned future that has
/// taken ownership of the underlying `WebSocket` to perform an asynchronous
/// read operation. The owned future returns the websocket together with the
/// read `PayloadFrame`.
enum ReadState<S> {
    /// No read in progress.
    Idle,
    /// A boxed future that owns the websocket and will produce a `PayloadFrame`
    /// (and the websocket) when complete.
    Reading(BoxFuture<'static, FutureResult<S, PayloadFrame>>),
}

/// Write state machine for `WebSocketStream`.
///
/// Similar to `ReadState`, but represents a write operation that owns the
/// websocket until it completes. This single state is shared by regular
/// writes, flushes, and the close performed by `poll_shutdown` — see
/// [`PendingOp`] for how we keep track of which one is actually in flight.
enum WriteState<S> {
    /// No write in progress.
    Idle,
    /// A boxed future that owns the websocket and will complete the
    /// operation, returning the websocket.
    Writing(BoxFuture<'static, FutureResult<S, ()>>),
}

/// Describes which operation the future stored in `WriteState::Writing`
/// actually represents.
///
/// `poll_write`, `poll_flush`, and `poll_shutdown` all funnel through the
/// same `WriteState`, so it is possible for one of them to find an
/// in-flight future that a *different* method started (e.g. `poll_flush`
/// is called while a previous `poll_write` call is still pending). Tracking
/// the operation kind alongside the future lets every caller correctly wait
/// for whatever is in flight and then do its own job, instead of
/// misreporting someone else's operation as its own (e.g. reporting a
/// flush's completion as "0 bytes written").
enum PendingOp {
    /// A plain data write; carries the number of bytes to report once the
    /// underlying frame has actually been written.
    Write(usize),
    /// A flush requested via `poll_flush`.
    Flush,
    /// A close frame written by `poll_shutdown`.
    Close,
}

/// Stream payload type.
///
/// This enum specifies whether the `WebSocketStream` will send/receive Text or
/// Binary application data. It is used to construct frames when writing and
/// validated on frames read from the peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadType {
    /// Binary frames.
    Binary,
    /// UTF-8 Text frames.
    Text,
}

impl From<PayloadType> for OpCode {
    fn from(value: PayloadType) -> Self {
        match value {
            PayloadType::Binary => OpCode::Binary,
            PayloadType::Text => OpCode::Text,
        }
    }
}

/// Map a `WebSocketError` into an `io::Error` for compatibility with the
/// `AsyncRead`/`AsyncWrite` trait surfaces.
fn make_io_err(e: WebSocketError) -> io::Error {
    io::Error::other(format!("Websocket error: {}", e))
}

/// Helper: create a boxed future that owns the websocket and reads a frame.
///
/// The returned future will call `websocket.read_frame().await`, copy the
/// payload into an owned `Bytes`, and return `(websocket, PayloadFrame)` on
/// success or `WebSocketError` on failure.
///
/// This helper is private because it requires taking ownership of the
/// `WebSocket` (which is stored as `Option` inside `WebSocketStream`) and
/// boxing the resulting future so the `WebSocketStream` state machine can store
/// it.
fn read<S>(mut websocket: WebSocket<S>) -> BoxFuture<'static, FutureResult<S, PayloadFrame>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    async move {
        // read_frame() returns Frame<'_> which borrows the websocket's buffers;
        // we immediately copy the payload into an owned Bytes so the PayloadFrame
        // can be returned with the websocket.
        match websocket.read_frame().await {
            Ok(frame) => {
                let payload = match frame.payload {
                    Payload::BorrowedMut(buf) => Bytes::from(buf.to_vec()),
                    Payload::Borrowed(buf) => Bytes::from(buf.to_vec()),
                    Payload::Owned(vec) => Bytes::from(vec),
                    Payload::Bytes(bytes) => bytes.freeze(),
                };

                let owned = PayloadFrame {
                    opcode: frame.opcode,
                    payload,
                };
                Ok((websocket, owned))
            }
            Err(e) => Err(e),
        }
    }
    .boxed()
}

/// Helper: create a boxed future that owns the websocket and writes the provided payload.
///
/// This helper constructs a single-frame message with the chosen `payload_type`
/// (Text or Binary) and writes it with `websocket.write_frame(...)`. The future
/// returns the websocket on success so ownership can be restored to the stream.
fn write<S>(
    mut websocket: WebSocket<S>,
    payload: BytesMut,
    payload_type: PayloadType,
) -> BoxFuture<'static, FutureResult<S, ()>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    async move {
        let frame = Frame::new(true, payload_type.into(), None, Payload::Bytes(payload));
        match websocket.write_frame(frame).await {
            Ok(()) => Ok((websocket, ())),
            Err(e) => Err(e),
        }
    }
    .boxed()
}

/// Helper: create a boxed future that owns the websocket and flushes it.
///
/// This issues a flush on the underlying `WebSocket` (which may flush any
/// internal write buffers) and returns the websocket afterwards.
fn flush<S>(mut websocket: WebSocket<S>) -> BoxFuture<'static, FutureResult<S, ()>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    async move {
        match websocket.flush().await {
            Ok(()) => Ok((websocket, ())),
            Err(e) => Err(e),
        }
    }
    .boxed()
}

/// Helper: create a boxed future that owns the websocket and sends a Close frame.
///
/// This writes a close frame and returns the websocket. Used by `poll_shutdown`.
fn close<S>(mut websocket: WebSocket<S>) -> BoxFuture<'static, FutureResult<S, ()>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    async move {
        let frame = Frame::close_raw(Vec::new().into());
        match websocket.write_frame(frame).await {
            Ok(()) => Ok((websocket, ())),
            Err(e) => Err(e),
        }
    }
    .boxed()
}

/// An `AsyncRead` / `AsyncWrite` adapter over a `fastwebsockets::WebSocket`.
///
/// `WebSocketStream<S>` wraps a `WebSocket<S>` and exposes a byte-stream view
/// (implementing `tokio::io::AsyncRead` and `tokio::io::AsyncWrite`) so that
/// websocket application payloads can be used with existing I/O and codec
/// infrastructure such as `tokio_util::codec::Framed`.
///
/// ## Behavior
///
/// * Incoming WebSocket data frames (Text or Binary depending on the stream's
///   `PayloadType`) are presented as a continuous byte stream. Each data frame's
///   payload is returned in-order; if a read buffer provided by the caller is
///   smaller than a frame payload, the remainder is buffered internally and
///   served on subsequent reads.
/// * Control frames (Ping/Pong) are handled by the underlying `WebSocket`
///   (auto-pong) or ignored by this adapter. A `Close` frame marks EOF and
///   subsequent reads return `Ok(())` with zero bytes (standard EOF semantics).
/// * Writes produce single complete WebSocket data frames of the configured
///   `PayloadType`. Each `poll_write` call sends one WebSocket data frame with
///   the provided bytes as payload. The number of bytes reported as written is
///   the length of `buf` supplied to `poll_write`.
///
/// ## Notes on threading and ownership
///
/// The adapter temporarily takes ownership of the inner `WebSocket` when it
/// needs to perform an asynchronous read or write operation. To achieve this
/// without requiring `WebSocket` itself to be `Sync`/`Send` across await points
/// we spawn a boxed future that owns the websocket and returns it when the
/// operation completes. This is implemented internally using `ReadState` and
/// `WriteState`.
///
/// ## Example
///
/// ```rust
/// use tokio::io::{AsyncReadExt, AsyncWriteExt};
/// use tokio::net::TcpStream;
/// use fastwebsockets::WebSocket;
/// use fastwebsockets_stream::{WebSocketStream, PayloadType};
///
/// // Wrap the websocket and apply a line-based codec:
/// async fn example<S>(_ws: WebSocket<S>)
///     where S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static {
///     // This example is illustrative: constructing a real `WebSocket` requires
///     // an underlying transport (e.g. a `TcpStream`) and the fastwebsockets
///     // connection/handshake. Assume `ws` is a valid WebSocket<TcpStream>.
///
///     let ws: WebSocket<S> = unimplemented!();
///     let mut ws_stream = WebSocketStream::new(ws, PayloadType::Binary);
///
///     // Write bytes -> sends a Binary frame
///     let _n = ws_stream.write(b"hello").await;
///
///     // Read bytes
///     let mut buf = vec![0_u8; 1024];
///     let _ = ws_stream.read(&mut buf).await;
///
///     // Shutdown (sends Close)
///     let _ = ws_stream.shutdown().await;
/// }
/// ```
///
/// Another common usage is to use `tokio_util::codec::Framed` to apply a codec
/// on top of `WebSocketStream` (for example a length-delimited or line-based
/// codec). Example:
///
/// ```rust
/// use tokio_util::codec::{Framed, LinesCodec};
/// use fastwebsockets::WebSocket;
/// use fastwebsockets_stream::{WebSocketStream, PayloadType};
///
/// // Wrap the websocket and apply a line-based codec:
/// async fn example<S>(_ws: WebSocket<S>)
///     where S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static {
///     let ws: WebSocket<S> = unimplemented!();
///     let stream = WebSocketStream::new(ws, PayloadType::Text);
///     let mut framed = Framed::new(stream, LinesCodec::new());
///
///     // Now you can use framed.read() / framed.send() to work with String frames.
/// }
/// ```
pub struct WebSocketStream<S> {
    /// The inner websocket. Stored as `Option`
    /// to allow temporarily taking ownership when starting an owned future
    websocket: Option<WebSocket<S>>,

    /// Buffer containing leftover bytes from the current
    /// incoming message that didn't fit the last caller-provided read buffer
    read_buf: BytesMut,

    /// State machine for an in-progress read future that owns the websocket
    read_state: ReadState<S>,

    /// State machine for an in-progress write future that owns the websocket
    write_state: WriteState<S>,

    /// If `Some(op)`, an operation is in progress in `write_state` and `op`
    /// records which one (write/flush/close) so its completion can be
    /// reported correctly regardless of which method ends up driving it to
    /// completion.
    pending_op: Option<PendingOp>,

    /// Expected and emitted payload type (Text or Binary). Received frames with
    /// a different data opcode are treated as errors
    payload_type: PayloadType,

    /// Set to `true` after a Close frame has been observed.
    /// When `closed` is true, subsequent reads return EOF
    closed: bool,
}

impl<S> WebSocketStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    /// Create a new `WebSocketStream` wrapping the provided `WebSocket`.
    ///
    /// This will enable automatic Pong replies and automatic Close handling on
    /// the wrapped `WebSocket` and initialize internal buffers and state.
    ///
    /// `payload_type` selects whether this stream should read/write Text or
    /// Binary data. If the peer sends data frames with an opcode that does not
    /// match `payload_type`, reads will return an error.
    pub fn new(mut websocket: WebSocket<S>, payload_type: PayloadType) -> Self {
        // Set auto pong and close
        websocket.set_auto_pong(true);
        websocket.set_auto_close(true);

        Self {
            websocket: Some(websocket),
            read_buf: BytesMut::with_capacity(8 * 1024),
            read_state: ReadState::Idle,
            write_state: WriteState::Idle,
            pending_op: None,
            payload_type,
            closed: false,
        }
    }

    /// Consume the adapter and attempt to return the inner `WebSocket`.
    ///
    /// This returns `Some(WebSocket<S>)` if the websocket currently resides in
    /// the adapter. If there is an outstanding future that currently owns the
    /// websocket (i.e. a read or write in progress) this method will return
    /// `None` because the adapter cannot recover the websocket until that
    /// future completes.
    pub fn into_inner(mut self) -> Option<WebSocket<S>> {
        // If there is an outstanding future that currently owns the websocket,
        // we cannot recover it here. We only return the inner websocket if it
        // currently resides in `self.ws`.
        self.websocket.take()
    }

    /// Returns `true` if we've observed a Close frame from the peer and the
    /// stream reached EOF.
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Drives whatever operation currently owns `write_state` (if any) to
    /// completion, restoring the websocket and returning which [`PendingOp`]
    /// just finished.
    ///
    /// Returns `Poll::Ready(Ok(None))` immediately if nothing is in flight.
    /// Callers (`poll_write`, `poll_flush`, `poll_shutdown`) are responsible
    /// for checking whether the returned `PendingOp` is the one they care
    /// about; if it isn't (e.g. `poll_flush` drained a plain write that a
    /// previous call started), they should loop and call this again so
    /// their own operation actually gets started and driven.
    fn poll_drive(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<Option<PendingOp>>> {
        match &mut self.write_state {
            WriteState::Idle => Poll::Ready(Ok(None)),
            WriteState::Writing(fut) => match fut.as_mut().poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok((websocket, ()))) => {
                    self.websocket = Some(websocket);
                    self.write_state = WriteState::Idle;
                    Poll::Ready(Ok(self.pending_op.take()))
                }
                Poll::Ready(Err(e)) => {
                    self.write_state = WriteState::Idle;
                    self.pending_op = None;
                    Poll::Ready(Err(make_io_err(e)))
                }
            },
        }
    }
}

impl<S> AsyncRead for WebSocketStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // If there are buffered bytes from previous frame, satisfy the read.
        if !self.read_buf.is_empty() {
            let to_copy = std::cmp::min(self.read_buf.len(), buf.remaining());
            buf.put_slice(&self.read_buf.split_to(to_copy));
            return Poll::Ready(Ok(()));
        }

        // If we've previously observed Close/EOF, report EOF by returning Ok(())
        if self.closed {
            return Poll::Ready(Ok(()));
        }

        loop {
            // Match current read future state
            match &mut self.read_state {
                ReadState::Idle => {
                    // Start a new read future by taking the websocket
                    let websocket = match self.websocket.take() {
                        Some(websocket) => websocket,
                        None => {
                            return Poll::Ready(Err(io::Error::other("Websocket not available")));
                        }
                    };
                    let future = read(websocket);
                    self.read_state = ReadState::Reading(future);
                }
                ReadState::Reading(fut) => {
                    // Poll the future. If Pending, return Pending. If Ready,
                    // reinstate websocket and handle frame.
                    match fut.as_mut().poll(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(res) => {
                            // Transition back to Idle
                            self.read_state = ReadState::Idle;
                            match res {
                                Ok((websocket, frame)) => {
                                    // Put websocket back
                                    self.websocket = Some(websocket);

                                    match frame.opcode {
                                        OpCode::Binary | OpCode::Text | OpCode::Continuation => {
                                            if matches!(frame.opcode, OpCode::Binary | OpCode::Text)
                                                && frame.opcode != self.payload_type.into()
                                            {
                                                return Poll::Ready(Err(io::Error::other(
                                                    "The received data type is different \
                                                    from the stream data type",
                                                )));
                                            }

                                            // Check frame payload
                                            let payload = frame.payload;
                                            if payload.is_empty() {
                                                // Nothing to return; loop to read next frame
                                                continue;
                                            }

                                            // If payload fits entirely into buf, copy and return.
                                            return if payload.len() <= buf.remaining() {
                                                buf.put_slice(&payload);
                                                Poll::Ready(Ok(()))
                                            } else {
                                                // Copy a part and stash remainder
                                                let take = buf.remaining();
                                                buf.put_slice(&payload[..take]);
                                                self.read_buf.extend_from_slice(&payload[take..]);
                                                Poll::Ready(Ok(()))
                                            };
                                        }

                                        OpCode::Close => {
                                            // Mark EOF and return 0 bytes read (Ok(()))
                                            self.closed = true;
                                            return Poll::Ready(Ok(()));
                                        }
                                        _ => {
                                            // Ignore control frames (Ping/Pong) and loop to
                                            // read the next frame.
                                            continue;
                                        }
                                    }
                                }
                                Err(e) => {
                                    // restore websocket if possible? We don't have it on error.
                                    // Map error to io::Error
                                    return Poll::Ready(Err(make_io_err(e)));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

impl<S> AsyncWrite for WebSocketStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        loop {
            match self.poll_drive(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(Some(PendingOp::Write(n)))) => return Poll::Ready(Ok(n)),
                Poll::Ready(Ok(Some(_))) => {
                    // A flush/close that a previous call started just finished;
                    // write_state is Idle again, loop around to actually start
                    // the write this call was asked to perform.
                    continue;
                }
                Poll::Ready(Ok(None)) => {
                    // Nothing in flight: start a new write, taking the websocket
                    // and creating a future that writes it.
                    let websocket = match self.websocket.take() {
                        Some(websocket) => websocket,
                        None => {
                            return Poll::Ready(Err(io::Error::other("Websocket not available")));
                        }
                    };

                    // Copy buffer into an owned BytesMut so the future can own it.
                    let payload = BytesMut::from(buf);
                    let len = payload.len();
                    self.pending_op = Some(PendingOp::Write(len));
                    self.write_state =
                        WriteState::Writing(write(websocket, payload, self.payload_type));
                    // Loop back around to actually drive the future we just created.
                }
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            match self.poll_drive(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(Some(PendingOp::Flush))) => return Poll::Ready(Ok(())),
                Poll::Ready(Ok(Some(_))) => {
                    // A write/close that was already in flight just finished;
                    // we still owe the caller an actual flush, so continue on.
                    continue;
                }
                Poll::Ready(Ok(None)) => {
                    let websocket = match self.websocket.take() {
                        Some(websocket) => websocket,
                        None => {
                            return Poll::Ready(Err(io::Error::other("Websocket not available")));
                        }
                    };
                    self.pending_op = Some(PendingOp::Flush);
                    self.write_state = WriteState::Writing(flush(websocket));
                }
            }
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Implemented by sending a Close frame through the same state machine
        // used for regular writes/flushes.
        loop {
            match self.poll_drive(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(Some(PendingOp::Close))) => return Poll::Ready(Ok(())),
                Poll::Ready(Ok(Some(_))) => {
                    // A write/flush that was already in flight just finished;
                    // we still owe the caller an actual close, so continue on.
                    continue;
                }
                Poll::Ready(Ok(None)) => {
                    let websocket = match self.websocket.take() {
                        Some(websocket) => websocket,
                        None => {
                            return Poll::Ready(Err(io::Error::other("Websocket not available")));
                        }
                    };
                    self.pending_op = Some(PendingOp::Close);
                    self.write_state = WriteState::Writing(close(websocket));
                }
            }
        }
    }
}

impl<S> Debug for WebSocketStream<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Helper to stringify read_state/write_state variants without requiring Debug on futures.
        fn read_state_name<T>(s: &ReadState<T>) -> &'static str {
            match s {
                ReadState::Idle => "Idle",
                ReadState::Reading(_) => "Reading",
            }
        }

        fn write_state_name<T>(s: &WriteState<T>) -> &'static str {
            match s {
                WriteState::Idle => "Idle",
                WriteState::Writing(_) => "Writing",
            }
        }

        fn pending_op_name(op: &Option<PendingOp>) -> &'static str {
            match op {
                None => "None",
                Some(PendingOp::Write(_)) => "Write",
                Some(PendingOp::Flush) => "Flush",
                Some(PendingOp::Close) => "Close",
            }
        }

        f.debug_struct("WebSocketStream")
            .field("read_buf_len", &self.read_buf.len())
            .field("read_state", &read_state_name(&self.read_state))
            .field("write_state", &write_state_name(&self.write_state))
            .field("pending_op", &pending_op_name(&self.pending_op))
            .field("closed", &self.closed)
            .finish()
    }
}
