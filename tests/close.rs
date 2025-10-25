//! Integration test for WebSocket close handshake using `fastwebsockets` and `hyper`.
//!
//! This test demonstrates a local loopback WebSocket server and client that perform
//! a proper connection closure. The server upgrades an incoming HTTP/1.1 request to
//! a WebSocket connection, reads a binary message from the client, then initiates
//! a close handshake by sending a Close frame. The client verifies the connection
//! is properly closed after receiving the closure.
//!
//! Key points covered by the test:
//! - Performing a WebSocket handshake from a hyper `Request` on the client side.
//! - Upgrading a hyper `Request<Incoming>` to a WebSocket on the server side.
//! - Reading binary frames using `fastwebsockets` primitives.
//! - Initiating a WebSocket close handshake with `CloseCode::Normal`.
//! - Detecting and verifying connection closure state on the client side.

use fastwebsockets::{CloseCode, Frame, OpCode, WebSocketError, handshake, upgrade};
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

/// Integration test that verifies proper WebSocket connection closure.
///
/// Steps performed by the test:
/// 1. Bind a `tokio::net::TcpListener` on localhost using an OS-assigned port.
/// 2. Spawn a background server loop that accepts connections and serves them using
///    `hyper::server::conn::http1` with `with_upgrades()` enabled.
/// 3. Create a Hyper `Request` configured for a WebSocket handshake and call
///    `handshake::client` to perform the client-side handshake over a `TcpStream`.
/// 4. Wrap the resulting upgraded stream in `WebSocketStream` with `PayloadType::Binary`.
/// 5. Send a binary message (`b"Hello!"`) and assert that the correct number of bytes were written.
/// 6. Attempt to read from the stream and verify that 0 bytes are read (indicating EOF).
/// 7. Verify that the WebSocket stream is marked as closed using `is_closed()`.
/// 8. On the server side, read the binary frame from the client and verify its opcode.
/// 9. Server initiates close handshake by sending a Close frame with `CloseCode::Normal`.
///
/// This test verifies the WebSocket close handshake mechanism, ensuring that both
/// client and server can gracefully terminate the connection according to the protocol.
#[tokio::test]
async fn close() {
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

    // Buffer used for reading operations.
    let mut buf = [0u8; 6];

    // Wrap the raw stream into a `WebSocketStream` for convenient read/write operations
    // with a declared payload type of Binary.
    let mut ws_stream = WebSocketStream::new(ws, PayloadType::Binary);

    // Send a binary frame containing "Hello!" and assert we wrote 6 bytes.
    let mut bytes = ws_stream.write(b"Hello!").await.unwrap();
    assert_eq!(bytes, 6);

    // Attempt to read from the stream after server initiates close.
    // A read of 0 bytes indicates the stream has reached EOF (connection closed).
    bytes = ws_stream.read(&mut buf).await.unwrap();
    assert_eq!(bytes, 0);

    // Verify that the WebSocket stream correctly reports its closed state.
    assert!(ws_stream.is_closed());
}

/// Handle an incoming hyper request and upgrade it to a WebSocket connection for close testing.
///
/// The function checks that the request is an upgrade request, performs the upgrade
/// using `upgrade::upgrade`, then spawns a task to handle the WebSocket connection:
/// - Read one binary frame from the client and verify its opcode.
/// - Initiate a close handshake by sending a Close frame with `CloseCode::Normal`.
/// - The empty payload in the Close frame indicates a normal closure without reason.
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

        // Initiate the close handshake by sending a Close frame with normal closure code.
        // The empty payload indicates no specific reason for closure.
        ws.write_frame(Frame::close(CloseCode::Normal.into(), &[]))
            .await
            .unwrap();
    });

    Ok(response)
}
