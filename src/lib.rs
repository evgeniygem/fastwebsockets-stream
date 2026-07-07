//! # fastwebsockets-stream
//!
//! `fastwebsockets-stream` provides an adapter that exposes a `fastwebsockets::WebSocket`
//! as an `AsyncRead` / `AsyncWrite` byte stream compatible with `tokio` and
//! utilities such as `tokio_util::codec::Framed`.
//!
//! ## Overview
//!
//! The adapter type is [`WebSocketStream`], which wraps a `fastwebsockets::WebSocket<S>`
//! and presents websocket application payloads as a continuous byte stream.
//! This is useful when you want to reuse existing codecs (length-delimited,
//! line-based, protobuf, etc.) or any code that operates on `AsyncRead` /
//! `AsyncWrite` without reimplementing websocket framing logic.
//!
//! The adapter supports both text and binary payloads (controlled by
//! [`PayloadType`]) and will validate that incoming data frames match the
//! configured payload type. Control frames (Ping/Pong) are handled automatically
//! by the underlying `fastwebsockets::WebSocket` (auto-pong) and `Close` frames
//! are translated to EOF for `AsyncRead` consumers.
//!
//! ## Key types
//!
//! * [`WebSocketStream<S>`] — the main adapter implementing `tokio::io::AsyncRead`
//!   and `tokio::io::AsyncWrite`.
//! * [`PayloadType`] — selects whether the stream works with Text or Binary
//!   application frames.
//!
//! ## Examples
//!
//! The example below demonstrates a minimal server and client that speak a
//! simple binary protocol in the same process: it creates an actual TCP
//! listener, upgrades the connection to WebSocket using `fastwebsockets` +
//! `hyper`, and then uses `WebSocketStream` as an `AsyncRead`/`AsyncWrite`
//! stream on both ends.
//!
//! ```rust
//! use fastwebsockets::{WebSocketError, handshake, upgrade};
//! use fastwebsockets_stream::{PayloadType, WebSocketStream};
//! use http_body_util::Empty;
//! use hyper::body::Bytes;
//! use hyper::body::Incoming;
//! use hyper::header::{CONNECTION, UPGRADE};
//! use hyper::server::conn::http1;
//! use hyper::service::service_fn;
//! use hyper::{Request, Response};
//! use hyper_util::rt::TokioIo;
//! use std::future::Future;
//! use std::net::Ipv4Addr;
//! use tokio::io::{AsyncReadExt, AsyncWriteExt};
//! use tokio::net::{TcpListener, TcpStream};
//!
//! struct SpawnExecutor;
//!
//! impl<F> hyper::rt::Executor<F> for SpawnExecutor
//! where
//!     F: Future + Send + 'static,
//!     F::Output: Send + 'static,
//! {
//!     fn execute(&self, fut: F) {
//!         tokio::task::spawn(fut);
//!     }
//! }
//!
//! // Server-side connection handler: upgrades the request, then echoes a
//! // single binary message back to the client over `WebSocketStream`.
//! async fn handle(mut request: Request<Incoming>) -> Result<Response<Empty<Bytes>>, WebSocketError> {
//!     assert!(upgrade::is_upgrade_request(&request));
//!     let (response, ws_fut) = upgrade::upgrade(&mut request)?;
//!
//!     tokio::spawn(async move {
//!         let ws = ws_fut.await.unwrap();
//!         let mut ws_stream = WebSocketStream::new(ws, PayloadType::Binary);
//!
//!         let mut buf = [0u8; 6];
//!         ws_stream.read_exact(&mut buf).await.unwrap();
//!         ws_stream.write_all(&buf).await.unwrap();
//!         ws_stream.shutdown().await.unwrap();
//!     });
//!
//!     Ok(response)
//! }
//!
//! # #[tokio::main]
//! # async fn main() {
//! // Bind an ephemeral local port and remember the address we actually got.
//! let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
//! let addr = listener.local_addr().unwrap();
//!
//! // Accept and upgrade incoming connections in the background.
//! tokio::spawn(async move {
//!     loop {
//!         let (stream, _) = listener.accept().await.unwrap();
//!         let io = TokioIo::new(stream);
//!         tokio::spawn(async move {
//!             let _ = http1::Builder::new()
//!                 .serve_connection(io, service_fn(handle))
//!                 .with_upgrades()
//!                 .await;
//!         });
//!     }
//! });
//!
//! // Client: connect to the port the server actually bound above.
//! let stream = TcpStream::connect(addr).await.unwrap();
//! let request = Request::builder()
//!     .method("GET")
//!     .uri(format!("ws://{addr}"))
//!     .header("Host", addr.to_string())
//!     .header(UPGRADE, "websocket")
//!     .header(CONNECTION, "upgrade")
//!     .header("Sec-WebSocket-Key", handshake::generate_key())
//!     .header("Sec-WebSocket-Version", "13")
//!     .body(Empty::<Bytes>::new())
//!     .unwrap();
//!
//! let (ws, _response) = handshake::client(&SpawnExecutor, request, stream)
//!     .await
//!     .unwrap();
//! let mut ws_stream = WebSocketStream::new(ws, PayloadType::Binary);
//!
//! let n = ws_stream.write(b"Hello!").await.unwrap();
//! assert_eq!(n, 6);
//!
//! let mut buf = [0u8; 6];
//! ws_stream.read_exact(&mut buf).await.unwrap();
//! assert_eq!(&buf, b"Hello!");
//! # }
//! ```
//!
//! ## Notes and caveats
//!
//! * Each call to `AsyncWrite::poll_write` will produce a single WebSocket data
//!   frame containing exactly the bytes provided in that call. If you need to
//!   stream a large logical message across multiple websocket frames, you
//!   should implement framing at the codec layer above the `WebSocketStream`.
//! * If the peer sends a data frame with an opcode that doesn't match the
//!   configured `PayloadType` (e.g. the stream is configured `Binary` but the
//!   peer sends `Text`), reads will return an error.
//! * `into_inner` on the adapter will return the inner `WebSocket` only if no
//!   read/write future currently owns it (i.e. there is no in-flight read or
//!   write). If an operation is in-progress, `into_inner` will return `None`.
//!
//! ## See Also
//! - [`fastwebsockets`](https://docs.rs/fastwebsockets)
//! - [`tokio::io::AsyncRead`](https://docs.rs/tokio/latest/tokio/io/trait.AsyncRead.html)
//! - [`tokio::io::AsyncWrite`](https://docs.rs/tokio/latest/tokio/io/trait.AsyncWrite.html)
//!
//! ## License
//!
//! Licensed under the [MIT License](https://opensource.org/licenses/MIT).
//!
//! See the `LICENSE` file in the repository root for details.

mod stream;

pub use stream::PayloadType;
pub use stream::WebSocketStream;
