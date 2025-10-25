//! Integration test for WebSocket ping-pong functionality using `fastwebsockets` and `hyper`.
//!
//! This test demonstrates the WebSocket ping-pong heartbeat mechanism, which is used
//! for keep-alive and connection health checking. The server upgrades an incoming
//! HTTP/1.1 request to a WebSocket connection, reads a binary message from the client,
//! sends a ping frame, waits for the automatic pong response, and then echoes the
//! original message back. This verifies that the WebSocket implementation properly
//! handles the control frame protocol.
//!
//! Key points covered by the test:
//! - Performing a WebSocket handshake from a hyper `Request` on the client side.
//! - Upgrading a hyper `Request<Incoming>` to a WebSocket on the server side.
//! - Sending and receiving ping-pong control frames as per WebSocket protocol.
//! - Automatic pong response generation by the WebSocket implementation.
//! - Maintaining data frame exchange alongside control frame communication.

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

/// Integration test that verifies WebSocket ping-pong control frame handling.
///
/// Steps performed by the test:
/// 1. Bind a `tokio::net::TcpListener` on localhost using an OS-assigned port.
/// 2. Spawn a background server loop that accepts connections and serves them using
///    `hyper::server::conn::http1` with `with_upgrades()` enabled.
/// 3. Create a Hyper `Request` configured for a WebSocket handshake and call
///    `handshake::client` to perform the client-side handshake over a `TcpStream`.
/// 4. Wrap the resulting upgraded stream in `WebSocketStream` with `PayloadType::Binary`.
/// 5. Send a binary message (`b"Hello!"`) and assert that the correct number of bytes were written.
/// 6. Read the echoed response back and verify it matches the original message.
/// 7. On the server side, read the binary frame from the client and verify its opcode.
/// 8. Server sends a ping frame with an empty payload to initiate the heartbeat.
/// 9. Server waits for and verifies the automatic pong response from the client.
/// 10. Server echoes the original message back to the client to complete the test.
///
/// This test verifies that the WebSocket implementation properly handles the ping-pong
/// control frame mechanism, which is essential for connection monitoring and keep-alive
/// functionality in real-world WebSocket applications.
#[tokio::test]
async fn ping() {
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
    let (ws, _response) = handshake::client(&SpawnExecutor, request, stream)
        .await
        .unwrap();

    // Buffer used to read the echoed message back.
    let mut buf = [0u8; 6];

    // Wrap the raw stream into a `WebSocketStream` for convenient read/write operations
    // with a declared payload type of Binary.
    let mut ws_stream = WebSocketStream::new(ws, PayloadType::Binary);

    // Send a binary frame containing "Hello!" and assert we wrote 6 bytes.
    let mut bytes = ws_stream.write(b"Hello!").await.unwrap();
    assert_eq!(bytes, 6);

    // Read the echoed bytes back into the buffer and assert the length matches.
    bytes = ws_stream.read(&mut buf).await.unwrap();
    assert_eq!(bytes, 6);

    // Finally assert the received payload equals the original message.
    assert_eq!(&buf, b"Hello!");
}

/// Handle an incoming hyper request and upgrade it to a WebSocket connection for ping-pong testing.
///
/// The function checks that the request is an upgrade request, performs the upgrade
/// using `upgrade::upgrade`, then spawns a task to handle the WebSocket connection:
/// - Read one binary frame from the client and verify its opcode.
/// - Send a ping frame with empty payload to initiate the heartbeat mechanism.
/// - Wait for and verify the automatic pong response from the client.
/// - Echo the original message back to the client to complete the round-trip.
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
        let mut ws = ws_fut.await.unwrap();

        // Read a frame from the client and expect it to be binary.
        let message = ws.read_frame().await.unwrap();
        assert_eq!(message.opcode, OpCode::Binary);

        // Send a ping frame to test the heartbeat mechanism.
        // The empty payload is typical for ping frames, though payloads are allowed.
        ws.write_frame(Frame::new(
            true,
            OpCode::Ping,
            None,
            Payload::Owned(Vec::new()),
        ))
        .await
        .unwrap();

        // Read the next frame and expect it to be a pong response.
        // The WebSocket implementation should automatically respond to ping with pong.
        let pong = ws.read_frame().await.unwrap();
        assert_eq!(pong.opcode, OpCode::Pong);

        // Echo the original message back to the client to complete the test.
        ws.write_frame(message).await.unwrap();
    });

    Ok(response)
}
