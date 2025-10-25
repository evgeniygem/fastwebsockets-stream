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
//! The examples below demonstrate a minimal server and a client that speak a
//! simple binary protocol. They create an actual TCP listener, upgrade the
//! connection to WebSocket using `fastwebsockets` + `hyper` and then use
//! `WebSocketStream` as an `AsyncRead`/`AsyncWrite` stream.
//!
//! > Note: these examples are integration-style and spawn network tasks. They
//! > are intended to be runnable in a test/runtime environment (they use
//! > `tokio` and `hyper` helpers).
//!
//! ### Server example
//!
//! ```rust
//! use fastwebsockets::{Frame, OpCode, WebSocketError, upgrade};
//! use fastwebsockets_stream::{PayloadType, WebSocketStream};
//! use http_body_util::Empty;
//! use hyper::Request;
//! use hyper::Response;
//! use hyper::body::Bytes;
//! use hyper::body::Incoming;
//! use hyper::header::CONNECTION;
//! use hyper::header::UPGRADE;
//! use hyper::server::conn::http1;
//! use hyper::service::service_fn;
//! use hyper_util::rt::TokioIo;
//! use std::future::Future;
//! use std::net::Ipv4Addr;
//! use tokio::io::{AsyncReadExt, AsyncWriteExt};
//! use tokio::net::TcpListener;
//!
//! struct SpawnExecutor;
//!
//! impl<F> hyper::rt::Executor<F> for SpawnExecutor
//! where
//!     F: Future + Send + 'static,
//!     F::Output: Send + 'static,
//! {
//!     fn execute(&self, fut: F) {
//!         // Spawn the provided future onto tokio's executor.
//!         tokio::task::spawn(fut);
//!     }
//! }
//!
//! #[tokio::test]
//! async fn server_example() {
//!     // Bind a local ephemeral port.
//!     let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0u16))
//!         .await
//!         .unwrap();
//!     let addr = listener.local_addr().unwrap();
//!
//!     // Spawn a task that accepts incoming TCP connections and upgrades them.
//!     tokio::spawn(async move {
//!         loop {
//!             let (stream, _) = listener.accept().await.unwrap();
//!             let io = TokioIo::new(stream);
//!
//!             // Serve a single HTTP/1.1 connection that supports upgrades.
//!             tokio::spawn(async move {
//!                 if let Err(err) = http1::Builder::new()
//!                     .serve_connection(io, service_fn(handle))
//!                     .with_upgrades()
//!                     .await
//!                 {
//!                     eprintln!("Error serving connection: {:?}", err);
//!                 }
//!             });
//!         }
//!     });
//!
//!     // The server runs in the background; we just ensure it starts successfully.
//!     println!("Server listening on {}", addr);
//! }
//!
//! async fn handle(mut request: Request<Incoming>) -> Result<Response<Empty<Bytes>>, WebSocketError> {
//!     // Ensure this is an upgrade request for WebSocket.
//!     assert!(upgrade::is_upgrade_request(&request));
//!
//!     // Convert the request into a WebSocket upgrade response and a future
//!     // that resolves to the established WebSocket once the upgrade completes.
//!     let (response, ws_fut) = upgrade::upgrade(&mut request)?;
//!
//!     // Spawn the application logic that talks over the websocket.
//!     tokio::spawn(async move {
//!         // Allocate a small buffer for reads.
//!         let mut buf = [0u8; 6];
//!
//!         // Await the completed websocket.
//!         let mut websocket = ws_fut.await.unwrap();
//!
//!         let message = websocket.read_frame().await.unwrap();
//!         assert_eq!(message.opcode, OpCode::Binary);
//!
//!         // Write the echoed message back.
//!         let _ = websocket.write_frame(Frame::binary(message.payload))
//!             .await
//!             .unwrap();
//!
//!         let message = websocket.read_frame().await.unwrap();
//!         assert_eq!(message.opcode, OpCode::Close);
//!     });
//!
//!     Ok(response)
//! }
//! ```
//!
//! ### Client example
//!
//! ```rust
//! use fastwebsockets::handshake::client;
//! use fastwebsockets::handshake;
//! use fastwebsockets::{Frame, OpCode};
//! use fastwebsockets_stream::{PayloadType, WebSocketStream};
//! use http_body_util::Empty;
//! use hyper::header::CONNECTION;
//! use hyper::header::UPGRADE;
//! use hyper::Request;
//! use hyper::body::Bytes;
//! use tokio::io::{AsyncReadExt, AsyncWriteExt};
//! use tokio::net::TcpStream;
//! use std::future::Future;
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
//! #[tokio::test]
//! async fn client_example() {
//!     // Connect to a running server (update address to your server).
//!     // This example assumes a server like the one in the server example is running.
//!     let addr = "127.0.0.1:9000";
//!     let stream = TcpStream::connect(addr).await.unwrap();
//!
//!     // Build a WebSocket client handshake request.
//!     let request = Request::builder()
//!         .method("GET")
//!         .uri("ws://127.0.0.1:9000")
//!         .header("Host", "127.0.0.1")
//!         .header(UPGRADE, "websocket")
//!         .header(CONNECTION, "upgrade")
//!         .header("Sec-WebSocket-Key", handshake::generate_key())
//!         .header("Sec-WebSocket-Version", "13")
//!         .body(Empty::<Bytes>::new())
//!         .unwrap();
//!
//!     // Perform the client handshake. `SpawnExecutor` is used by `hyper` to run
//!     // internal tasks that may be required during the handshake.
//!     let (stream, _response) = client(&SpawnExecutor, request, stream)
//!         .await
//!         .unwrap();
//!
//!     // Wrap websocket with the adapter and perform a small exchange.
//!     let mut ws_stream = WebSocketStream::new(stream, PayloadType::Binary);
//!     let mut buf = [0 as u8; 6];
//!
//!     let mut bytes = ws_stream.write(b"Hello!").await.unwrap();
//!     assert_eq!(bytes, 6);
//!
//!     bytes = ws_stream.read(&mut buf).await.unwrap();
//!     assert_eq!(bytes, 6);
//!     assert_eq!(&buf, b"Hello!");
//! }
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
