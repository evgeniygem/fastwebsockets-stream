//! Regression test for reassembly of fragmented WebSocket messages.
//!
//! By default `fastwebsockets` hands the application raw frames as they
//! arrive on the wire: a message that the peer chose to split into several
//! frames shows up as an initial `Text`/`Binary` frame with `fin = false`,
//! followed by one or more `Continuation` frames (the last one with
//! `fin = true`). `WebSocketStream` is expected to transparently stitch all
//! of these payloads back together into a single continuous byte stream.
//!
//! This test sends a message split across three frames, with a `Ping`
//! control frame legally interleaved between two of the fragments (RFC 6455
//! §5.4 explicitly allows control frames to appear in the middle of a
//! fragmented message), and asserts that `WebSocketStream::read_exact`
//! reconstructs the full, original payload.
//!
//! `WebSocketStream` only cares about `AsyncRead`/`AsyncWrite`, so unlike the
//! other integration tests in this crate, this one skips the real TCP
//! listener + hyper HTTP upgrade dance and instead connects both ends over
//! an in-memory `tokio::io::duplex` pipe via `WebSocket::after_handshake`.
//! That keeps the test focused on `WebSocketStream`'s own frame-handling
//! logic and lets every assertion live directly in the test function (no
//! spawned task whose panics could be silently dropped if the test happens
//! to finish first).

use fastwebsockets::{Frame, OpCode, Payload, Role, WebSocket};
use fastwebsockets_stream::{PayloadType, WebSocketStream};
use std::time::Duration;
use tokio::io::{AsyncReadExt, DuplexStream};

/// Creates a connected in-memory `(client, server)` WebSocket pair, already
/// past the handshake, without any real networking involved.
fn connected_pair() -> (WebSocket<DuplexStream>, WebSocket<DuplexStream>) {
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let client = WebSocket::after_handshake(client_io, Role::Client);
    let server = WebSocket::after_handshake(server_io, Role::Server);
    (client, server)
}

/// Sends "Hello, fragmented world!" as three separate frames (with a Ping in
/// between two of them) and verifies `WebSocketStream` reassembles all of it.
#[tokio::test]
async fn fragmented_message_is_reassembled() {
    let (mut client, server) = connected_pair();
    let mut server_stream = WebSocketStream::new(server, PayloadType::Binary);

    // Fragment 1: starts the message, more to come (`fin = false`).
    client
        .write_frame(Frame::new(
            false,
            OpCode::Binary,
            None,
            Payload::Borrowed(b"Hello, "),
        ))
        .await
        .unwrap();

    // Fragment 2: continuation, still more to come.
    client
        .write_frame(Frame::new(
            false,
            OpCode::Continuation,
            None,
            Payload::Borrowed(b"frag"),
        ))
        .await
        .unwrap();

    // A Ping is a control frame and is explicitly allowed to interleave with
    // a fragmented message; it must not disturb reassembly and must not be
    // handed to the application as stream data.
    client
        .write_frame(Frame::new(
            true,
            OpCode::Ping,
            None,
            Payload::Owned(Vec::new()),
        ))
        .await
        .unwrap();

    // Fragment 3: final continuation (`fin = true`), completes the message.
    client
        .write_frame(Frame::new(
            true,
            OpCode::Continuation,
            None,
            Payload::Borrowed(b"mented world!"),
        ))
        .await
        .unwrap();

    // If Continuation frames were (again) silently dropped, this would hang
    // forever waiting for bytes that never arrive, so guard it with a
    // timeout to fail fast with a clear message instead.
    let mut buf = [0u8; 24]; // b"Hello, fragmented world!".len() == 24
    tokio::time::timeout(Duration::from_secs(5), server_stream.read_exact(&mut buf))
        .await
        .expect(
            "timed out reassembling a fragmented message - Continuation frames \
             may be getting dropped again",
        )
        .unwrap();

    assert_eq!(&buf, b"Hello, fragmented world!");
}
