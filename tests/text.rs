//! Integration test for a text WebSocket stream using `fastwebsockets` and `hyper`.
//!
//! This test demonstrates a local loopback WebSocket server and client that exchange
//! text messages using `tokio` for async runtime. The server upgrades an incoming
//! HTTP/1.1 request to a WebSocket connection, sends a text message to the client,
//! reads the echoed response, and verifies it matches the original.
//!
//! Key points covered by the test:
//! - Performing a WebSocket handshake from a hyper `Request` on the client side.
//! - Upgrading a hyper `Request<Incoming>` to a WebSocket on the server side.
//! - Reading and writing text frames using `fastwebsockets` primitives.
//! - Using `WebSocketStream` with `PayloadType::Text` for text message handling.
//! - Running the server and client on a loopback TCP listener within the same test.

use fastwebsockets::{Frame, OpCode, WebSocketError, handshake, upgrade};
use fastwebsockets_stream::{PayloadType, WebSocketStream};
use http_body_util::Empty;
use hyper::Request;
use hyper::Response;
use hyper::body::Bytes;
use hyper::body::Incoming;
use hyper::header::CONNECTION;
use hyper::header::UPGRADE;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use std::future::Future;
use std::net::Ipv4Addr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Minimal executor implementation used by `handshake::client`.
///
/// `handshake::client` requires an executor implementing `hyper::rt::Executor` so it
/// can spawn any background tasks needed for the client handshake. This implementation
/// delegates to `tokio::task::spawn` for test convenience.
struct SpawnExecutor;

impl<F> hyper::rt::Executor<F> for SpawnExecutor
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, fut: F) {
        // Delegate to tokio's task spawning.
        tokio::task::spawn(fut);
    }
}

/// Integration test that sets up an in-process WebSocket server and client for text messages.
///
/// Steps performed by the test:
/// 1. Bind a `tokio::net::TcpListener` on localhost using an OS-assigned port.
/// 2. Spawn a background server loop that accepts connections and serves them using
///    `hyper::server::conn::http1` with `with_upgrades()` enabled.
/// 3. Create a Hyper `Request` configured for a WebSocket handshake and call
///    `handshake::client` to perform the client-side handshake over a `TcpStream`.
/// 4. On the server side, wrap the upgraded WebSocket in `WebSocketStream` with `PayloadType::Text`.
/// 5. Server sends a text message (`b"Hello!"`) and asserts the write operation completes.
/// 6. Server reads the echoed response back and verifies it matches the original message.
/// 7. Client reads the initial text frame from the server and verifies it has `OpCode::Text`.
/// 8. Client echoes the received text frame back to the server.
///
/// This test verifies both the handshake and the text frame read/write paths, demonstrating
/// bidirectional text message exchange between client and server.
#[tokio::test]
async fn text_stream() {
    // Bind to an ephemeral port on localhost.
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0u16))
        .await
        .unwrap();

    let addr = listener.local_addr().unwrap();

    // Spawn a server loop that accepts connections and serves them.
    // Each accepted connection is passed into Hyper's HTTP/1.1 connection handler.
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let io = TokioIo::new(stream);

            tokio::spawn(async move {
                if let Err(err) = http1::Builder::new()
                    .serve_connection(io, service_fn(handle))
                    .with_upgrades()
                    .await
                {
                    // Print errors if the connection fails; test will fail on assertion mismatches.
                    println!("Error serving connection: {:?}", err);
                }
            });
        }
    });

    // Create a websocket connection from the client side.
    let stream = TcpStream::connect(addr).await.unwrap();

    // Build an HTTP request that will be used for the WebSocket client handshake.
    let request = Request::builder()
        .method("GET")
        .uri("ws://localhost")
        .header("Host", "localhost")
        .header(UPGRADE, "websocket")
        .header(CONNECTION, "upgrade")
        .header("Sec-WebSocket-Key", handshake::generate_key())
        .header("Sec-WebSocket-Version", "13")
        .body(Empty::<Bytes>::new())
        .unwrap();

    // Perform the client handshake. The `SpawnExecutor` satisfies the executor bound.
    let (mut stream, _response) = handshake::client(&SpawnExecutor, request, stream)
        .await
        .unwrap();

    // Read the initial text frame from the server and verify it's a text opcode.
    let message = stream.read_frame().await.unwrap();
    assert!(message.opcode == OpCode::Text);

    // Echo the received text frame back to the server to complete the round-trip test.
    stream
        .write_frame(Frame::text(message.payload))
        .await
        .unwrap();
}

/// Handle an incoming hyper request and upgrade it to a WebSocket connection for text exchange.
///
/// The function checks that the request is an upgrade request, performs the upgrade
/// using `upgrade::upgrade`, then spawns a task to handle the WebSocket connection:
/// - Wrap the raw WebSocket in `WebSocketStream` with `PayloadType::Text`.
/// - Send an initial text message ("Hello!") to the client.
/// - Read the echoed response back and verify it matches the original.
///
/// Returns the HTTP response produced by the upgrade helper which must be sent back
/// to the client by hyper.
async fn handle(mut request: Request<Incoming>) -> Result<Response<Empty<Bytes>>, WebSocketError> {
    // Confirm the request is an upgrade request; this will panic in tests if violated.
    assert!(upgrade::is_upgrade_request(&request));

    // Perform the upgrade which returns an HTTP response to send and a future that
    // resolves to the upgraded WebSocket connection.
    let (response, ws_fut) = upgrade::upgrade(&mut request)?;

    // Spawn a background task to run the WebSocket message loop so that the HTTP
    // response can be returned immediately and hyper can finish the handshake flow.
    tokio::spawn(async move {
        // Buffer used to read the echoed message back.
        let mut buf = [0u8; 6];

        // Wait for the WebSocket upgrade to complete.
        let websocket = ws_fut.await.unwrap();

        // Wrap the raw WebSocket into a `WebSocketStream` for convenient read/write operations
        // with a declared payload type of Text, indicating we're handling text messages.
        let mut ws_stream = WebSocketStream::new(websocket, PayloadType::Text);

        // Send a text message containing "Hello!" and assert we wrote 6 bytes.
        let mut bytes = ws_stream.write(b"Hello!").await.unwrap();
        assert_eq!(bytes, 6);

        // Read the echoed bytes back into the buffer and assert the length matches.
        bytes = ws_stream.read(&mut buf).await.unwrap();
        assert_eq!(bytes, 6);

        // Finally assert the received payload equals the original message.
        assert_eq!(&buf, b"Hello!");
    });

    Ok(response)
}
