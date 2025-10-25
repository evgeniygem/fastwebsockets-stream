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
/// websocket until it completes.
enum WriteState<S> {
    /// No write in progress.
    Idle,
    /// A boxed future that owns the websocket and will complete the write,
    /// returning the websocket.
    Writing(BoxFuture<'static, FutureResult<S, ()>>),
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

    /// If `Some(n)` then a write is in progress and intends to report `n` bytes
    /// written when the write future completes. We store the length separately
    /// because the actual write future only stores the websocket and the
    /// payload it sent
    pending_write_len: Option<usize>,

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
            pending_write_len: None,
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
                    let mut future_pin = unsafe { Pin::new_unchecked(fut) };
                    match future_pin.as_mut().poll(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(res) => {
                            // Transition back to Idle
                            self.read_state = ReadState::Idle;
                            match res {
                                Ok((websocket, frame)) => {
                                    // Put websocket back
                                    self.websocket = Some(websocket);

                                    match frame.opcode {
                                        OpCode::Binary | OpCode::Text => {
                                            // If frame payload type isn't match the desired type,
                                            // return error
                                            if frame.opcode != self.payload_type.into() {
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
                                            // Ignore control frames and loop to read next frame
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
        // If there's already a write-in progress, poll it.
        loop {
            match &mut self.write_state {
                WriteState::Idle => {
                    // Start a new write: take websocket and create future that writes
                    let websocket = match self.websocket.take() {
                        Some(websocket) => websocket,
                        None => {
                            return Poll::Ready(Err(io::Error::other("Websocket not available")));
                        }
                    };

                    // Copy buffer into owned Vec so the future can own it
                    let payload = BytesMut::from(buf);
                    let len = payload.len();
                    let future = write(websocket, payload, self.payload_type);
                    self.pending_write_len = Some(len);
                    self.write_state = WriteState::Writing(future);
                }
                WriteState::Writing(fut) => {
                    // poll the write future
                    let mut future_pin = unsafe { Pin::new_unchecked(fut) };
                    match future_pin.as_mut().poll(cx) {
                        Poll::Pending => return Poll::Pending,

                        Poll::Ready(res) => {
                            // finish write: put websocket back
                            self.write_state = WriteState::Idle;
                            match res {
                                Ok((websocket, ())) => {
                                    self.websocket = Some(websocket);
                                    let n = self.pending_write_len.take().unwrap_or(0);
                                    return Poll::Ready(Ok(n));
                                }
                                Err(e) => return Poll::Ready(Err(make_io_err(e))),
                            }
                        }
                    }
                }
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // If a write is in progress, poll it first.
        match &mut self.write_state {
            WriteState::Writing(_) => {
                // let regular poll_write flow handle it; return Pending so caller
                // should call poll_flush again later. Alternatively, we could
                // poll it here explicitly, but reusing poll_write semantics is fine.
                return Poll::Pending;
            }
            WriteState::Idle => {
                // Start a new flush future by taking the websocket
                let websocket = match self.websocket.take() {
                    Some(websocket) => websocket,
                    None => return Poll::Ready(Ok(())),
                };
                // empty payload for close
                let future = flush(websocket);
                self.write_state = WriteState::Writing(future);

                // fallthrough to poll the just-created future
            }
        }

        // Now poll the write future created above.
        match &mut self.write_state {
            WriteState::Writing(fut) => {
                let mut fut_pin = unsafe { Pin::new_unchecked(fut) };
                match fut_pin.as_mut().poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(res) => {
                        self.write_state = WriteState::Idle;
                        match res {
                            Ok((websocket, ())) => {
                                self.websocket = Some(websocket);
                                Poll::Ready(Ok(()))
                            }
                            Err(e) => Poll::Ready(Err(make_io_err(e))),
                        }
                    }
                }
            }
            _ => Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Implement shutdown by sending a Close frame synchronously via the
        // same state-machine approach: start a write future that sends close.
        // If a write is already in progress, wait for it to complete first.

        // If a write is in progress, poll it first.
        match &mut self.write_state {
            WriteState::Writing(_) => {
                // let regular poll_write flow handle it; return Pending so caller
                // should call poll_shutdown again later. Alternatively, we could
                // poll it here explicitly, but reusing poll_write semantics is fine.
                return Poll::Pending;
            }
            WriteState::Idle => {
                // start a close write
                let websocket = match self.websocket.take() {
                    Some(websocket) => websocket,
                    None => return Poll::Ready(Ok(())),
                };
                // empty payload for close
                let future = close(websocket);
                self.write_state = WriteState::Writing(future);

                // fallthrough to poll the just-created future
            }
        }

        // Now poll the write future created above.
        match &mut self.write_state {
            WriteState::Writing(fut) => {
                let mut fut_pin = unsafe { Pin::new_unchecked(fut) };
                match fut_pin.as_mut().poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(res) => {
                        self.write_state = WriteState::Idle;
                        match res {
                            Ok((websocket, ())) => {
                                self.websocket = Some(websocket);
                                Poll::Ready(Ok(()))
                            }
                            Err(e) => Poll::Ready(Err(make_io_err(e))),
                        }
                    }
                }
            }
            _ => Poll::Ready(Ok(())),
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

        f.debug_struct("WebSocketStream")
            .field("read_buf_len", &self.read_buf.len())
            .field("read_state", &read_state_name(&self.read_state))
            .field("write_state", &write_state_name(&self.write_state))
            .field("pending_write_len", &self.pending_write_len)
            .field("closed", &self.closed)
            .finish()
    }
}
