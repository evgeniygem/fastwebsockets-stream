//! Integration test for framed WebSocket communication using `fastwebsockets`, `hyper`,
//! and `tokio_util::codec`.
//!
//! This test demonstrates advanced WebSocket usage by combining WebSocket streams with
//! line-based codec framing. The server upgrades an incoming HTTP/1.1 request to a
//! WebSocket connection and uses a `Framed` adapter with `LinesCodec` to handle
//! message-based communication. The client sends multiple binary messages and verifies
//! the echoed responses, demonstrating how to integrate WebSockets with tokio's
//! codec system for structured message handling.
//!
//! Key points covered by the test:
//! - Performing a WebSocket handshake from a hyper `Request` on the client side.
//! - Upgrading a hyper `Request<Incoming>` to a WebSocket on the server side.
//! - Using `Framed` adapter with `LinesCodec` for line-based message processing.
//! - Integrating `WebSocketStream` with tokio's streaming utilities.
//! - Handling multiple message exchanges in a structured communication pattern.

use fastwebsockets::{Frame, OpCode, Payload, WebSocketError, handshake, upgrade};
use fastwebsockets_stream::{PayloadType, WebSocketStream};
use futures::SinkExt;
use futures::StreamExt;
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
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tokio_util::codec::LinesCodec;

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

/// Integration test that demonstrates framed WebSocket communication with line-based codec.
///
/// Steps performed by the test:
/// 1. Bind a `tokio::net::TcpListener` on localhost using an OS-assigned port.
/// 2. Spawn a background server loop that accepts connections and serves them using
///    `hyper::server::conn::http1` with `with_upgrades()` enabled.
/// 3. Create a Hyper `Request` configured for a WebSocket handshake and call
///    `handshake::client` to perform the client-side handshake over a `TcpStream`.
/// 4. Client sends multiple binary messages containing newline-terminated strings.
/// 5. For each message sent, client reads the response frame and verifies:
///    - The opcode is `OpCode::Binary`
///    - The response payload starts with the original message content
/// 6. Client sends a close frame to gracefully terminate the connection.
/// 7. On the server side, wrap the upgraded WebSocket in `WebSocketStream` with `PayloadType::Binary`.
/// 8. Further wrap the stream in `Framed` with `LinesCodec` for line-based message processing.
/// 9. Server uses the `StreamExt` and `SinkExt` traits to read lines and echo them back.
///
/// This test demonstrates how to integrate WebSocket streams with tokio's codec system
/// for structured message handling, enabling higher-level abstractions over raw frame processing.
#[tokio::test]
async fn framed() {
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

    // Test messages to send, each ending with newline to work with LinesCodec
    let messages = &["Hello\n", "World\n", "!\n", "\n"];

    // Send each message and verify the echoed response
    for &message in messages {
        // Send the message as a binary frame
        stream
            .write_frame(Frame::binary(Payload::Borrowed(message.as_bytes())))
            .await
            .unwrap();

        // Read the response frame from the server
        let response = stream.read_frame().await.unwrap();

        // Verify the response is a binary frame
        assert_eq!(response.opcode, OpCode::Binary);

        // Verify the response starts with our original message
        // (the server echoes back the same content)
        assert!(response.payload.starts_with(message.as_bytes()));
    }

    // Send a close frame to gracefully terminate the connection
    stream
        .write_frame(Frame::close_raw(Vec::new().into()))
        .await
        .unwrap();
}

/// Handle an incoming hyper request and upgrade it to a framed WebSocket connection.
///
/// The function checks that the request is an upgrade request, performs the upgrade
/// using `upgrade::upgrade`, then spawns a task to handle the WebSocket connection:
/// - Wrap the raw WebSocket in `WebSocketStream` with `PayloadType::Binary`.
/// - Further wrap the stream in `Framed` with `LinesCodec` for line-based message processing.
/// - Use the `StreamExt` trait to iterate over incoming lines.
/// - Use the `SinkExt` trait to send each received line back as a response.
/// - The loop continues until the client closes the connection.
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
        // Wait for the WebSocket upgrade to complete.
        let websocket = ws_fut.await.unwrap();

        // Create a framed stream by combining WebSocketStream with LinesCodec.
        // This allows us to work with line-based messages rather than raw frames.
        let mut stream = Framed::new(
            WebSocketStream::new(websocket, PayloadType::Binary),
            LinesCodec::new(),
        );

        // Process incoming messages using the StreamExt trait.
        // The `next().await` call yields `None` when the stream is closed.
        while let Some(Ok(message)) = stream.next().await {
            // Echo the received message back to the client using the SinkExt trait.
            stream.send(message).await.unwrap();
        }
    });

    Ok(response)
}
