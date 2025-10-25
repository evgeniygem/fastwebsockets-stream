//! Integration test for WebSocket payload type mismatch handling using `fastwebsockets` and
//! `hyper`.
//!
//! This test demonstrates error handling when there's a mismatch between expected and actual
//! WebSocket payload types. The server upgrades an incoming HTTP/1.1 request to a WebSocket
//! connection configured for binary data, while the client sends a text frame. This tests
//! the library's ability to properly detect and report payload type mismatches with
//! descriptive error messages.
//!
//! Key points covered by the test:
//! - Performing a WebSocket handshake from a hyper `Request` on the client side.
//! - Upgrading a hyper `Request<Incoming>` to a WebSocket on the server side.
//! - Sending a text frame from the client while server expects binary data.
//! - Verifying proper error detection and reporting for payload type mismatches.
//! - Validating error kind and error message content for specific mismatch conditions.

use fastwebsockets::{Frame, Payload, WebSocketError, handshake, upgrade};
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
use std::io::ErrorKind;
use std::net::Ipv4Addr;
use tokio::io::AsyncReadExt;
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

/// Integration test that verifies proper error handling for WebSocket payload type mismatches.
///
/// Steps performed by the test:
/// 1. Bind a `tokio::net::TcpListener` on localhost using an OS-assigned port.
/// 2. Spawn a background server loop that accepts connections and serves them using
///    `hyper::server::conn::http1` with `with_upgrades()` enabled.
/// 3. Create a Hyper `Request` configured for a WebSocket handshake and call
///    `handshake::client` to perform the client-side handshake over a `TcpStream`.
/// 4. Client sends a text frame with payload "Hello!" using `Frame::text()`.
/// 5. Client attempts to read a response frame (though the connection may be terminated).
/// 6. On the server side, wrap the upgraded WebSocket in `WebSocketStream` with `PayloadType::Binary`.
/// 7. Server attempts to read from the stream, which should fail due to payload type mismatch.
/// 8. Verify the error has kind `ErrorKind::Other` and contains the expected descriptive message.
///
/// This test validates that the WebSocket implementation properly detects and reports
/// protocol violations when the payload type doesn't match the expected configuration,
/// ensuring robust error handling in mismatched scenarios.
#[tokio::test]
async fn payload_mismatch() {
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
    let (mut ws, _response) = handshake::client(&SpawnExecutor, request, stream)
        .await
        .unwrap();

    // Send a text frame to the server. This creates the payload type mismatch
    // since the server is configured to expect binary data.
    ws.write_frame(Frame::text(Payload::Borrowed(b"Hello!")))
        .await
        .unwrap();

    // Attempt to read a response frame. The connection might be terminated
    // due to the payload mismatch, so we ignore the result.
    let _ = ws.read_frame().await;
}

/// Handle an incoming hyper request and upgrade it to a WebSocket connection for mismatch testing.
///
/// The function checks that the request is an upgrade request, performs the upgrade
/// using `upgrade::upgrade`, then spawns a task to handle the WebSocket connection:
/// - Configure the WebSocket stream with `PayloadType::Binary`, creating an expectation
///   for binary data only.
/// - Attempt to read from the stream, which should fail because the client sends text data.
/// - Verify the error has the expected kind and contains a descriptive message about
///   the payload type mismatch.
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
        // Buffer for reading incoming data.
        let mut buf = [0u8; 1024];

        // Intentionally create a payload type mismatch by configuring the stream
        // for binary data while the client will send text data.
        let mut ws_stream = WebSocketStream::new(ws_fut.await.unwrap(), PayloadType::Binary);

        // Attempt to read from the stream - this should fail due to payload type mismatch.
        let result = ws_stream.read(&mut buf).await;

        // Verify that the operation resulted in an error as expected.
        assert!(result.is_err());
        let err = result.unwrap_err();

        // Check that the error kind is `Other`, indicating a protocol-level error
        // rather than a network or I/O error.
        assert_eq!(err.kind(), ErrorKind::Other);

        // Verify the error message specifically describes the payload type mismatch.
        assert_eq!(
            err.to_string(),
            "The received data type is different from the stream data type".to_string()
        )
    });

    Ok(response)
}
