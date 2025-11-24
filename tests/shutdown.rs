//! Integration test for WebSocket connection shutdown using `fastwebsockets` and `hyper`.
//!
//! This test demonstrates proper connection termination by the server after processing
//! a message. The server upgrades an incoming HTTP/1.1 request to a WebSocket connection,
//! reads a single binary message from the client, then initiates a graceful shutdown
//! of the WebSocket stream. The client verifies that the server responds with a Close
//! frame, confirming proper connection termination according to WebSocket protocol.
//!
//! Key points covered by the test:
//! - Performing a WebSocket handshake from a hyper `Request` on the client side.
//! - Upgrading a hyper `Request<Incoming>` to a WebSocket on the server side.
//! - Reading binary messages using `WebSocketStream` with `PayloadType::Binary`.
//! - Initiating graceful connection shutdown using `WebSocketStream::shutdown()`.
//! - Verifying proper Close frame reception on the client side.

use fastwebsockets::{Frame, OpCode, Payload, WebSocketError, handshake, upgrade};
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

/// Integration test that verifies proper WebSocket connection shutdown by the server.
///
/// Steps performed by the test:
/// 1. Bind a `tokio::net::TcpListener` on localhost using an OS-assigned port.
/// 2. Spawn a background server loop that accepts connections and serves them using
///    `hyper::server::conn::http1` with `with_upgrades()` enabled.
/// 3. Create a Hyper `Request` configured for a WebSocket handshake and call
///    `handshake::client` to perform the client-side handshake over a `TcpStream`.
/// 4. Client sends a binary frame containing "Hello!" as a text message payload.
/// 5. Client reads the next frame and verifies it is a Close frame (`OpCode::Close`).
/// 6. On the server side, wrap the upgraded WebSocket in `WebSocketStream` with `PayloadType::Binary`.
/// 7. Server reads the incoming message into a buffer.
/// 8. Server initiates graceful shutdown using `WebSocketStream::shutdown()`.
///
/// This test verifies that the WebSocket implementation properly handles connection
/// termination initiated by the server, ensuring that clients receive the appropriate
/// Close frame and can handle graceful connection teardown.
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

    // Message to send to the server
    let message = "Hello!";

    // Send the message as a binary frame. Note: even though we're sending text content,
    // we're using binary framing in this test.
    stream
        .write_frame(Frame::binary(Payload::Borrowed(message.as_bytes())))
        .await
        .unwrap();

    // Read the response frame from the server and verify it's a Close frame.
    // This indicates the server has initiated connection termination.
    let message = stream.read_frame().await.unwrap();
    assert_eq!(message.opcode, OpCode::Close);
}

/// Handle an incoming hyper request and upgrade it to a WebSocket connection for shutdown testing.
///
/// The function checks that the request is an upgrade request, performs the upgrade
/// using `upgrade::upgrade`, then spawns a task to handle the WebSocket connection:
/// - Wrap the raw WebSocket in `WebSocketStream` with `PayloadType::Binary`.
/// - Read one incoming message from the client into a buffer.
/// - Initiate graceful shutdown of the WebSocket stream using `shutdown()`.
///
/// The `shutdown()` method should trigger the WebSocket protocol's close handshake,
/// causing a Close frame to be sent to the client and the connection to be terminated
/// cleanly.
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
        // Buffer for reading the incoming message
        let mut buf = [0u8; 6];

        // Wait for the WebSocket upgrade to complete.
        let websocket = ws_fut.await.unwrap();

        // Wrap the raw WebSocket into a `WebSocketStream` for convenient read/write operations
        // with a declared payload type of Binary.
        let mut ws_stream = WebSocketStream::new(websocket, PayloadType::Binary);

        // Read the incoming message from the client into the buffer.
        // This expects exactly 6 bytes to match the "Hello!" message.
        let _ = ws_stream.read(&mut buf).await.unwrap();

        // Initiate graceful shutdown of the WebSocket stream.
        // This should send a Close frame to the client and terminate the connection.
        ws_stream.shutdown().await.unwrap();
    });

    Ok(response)
}
