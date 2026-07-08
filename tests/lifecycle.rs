//! Basic sanity checks for parts of `WebSocketStream`'s public API that
//! weren't exercised by any existing test: `AsyncWriteExt::flush()`,
//! `WebSocketStream::into_inner()`, and zero-length data frames.
//!
//! As in `fragmentation.rs` and `error_consistency.rs`, these connect both
//! ends over an in-memory `tokio::io::duplex` pipe via
//! `WebSocket::after_handshake` instead of a real TCP + hyper upgrade, since
//! none of these checks care about the handshake itself.

use fastwebsockets::{Role, WebSocket};
use fastwebsockets_stream::{PayloadType, WebSocketStream};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

/// Creates a connected in-memory `(client, server)` WebSocket pair, already
/// past the handshake, without any real networking involved.
fn connected_pair() -> (WebSocket<DuplexStream>, WebSocket<DuplexStream>) {
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let client = WebSocket::after_handshake(client_io, Role::Client);
    let server = WebSocket::after_handshake(server_io, Role::Server);
    (client, server)
}

/// `flush()` had no test coverage at all before; check that it succeeds both
/// right after a write and again when there's nothing pending.
#[tokio::test]
async fn flush_after_write_succeeds() {
    let (client, server) = connected_pair();
    let mut client_stream = WebSocketStream::new(client, PayloadType::Binary);
    let mut server_stream = WebSocketStream::new(server, PayloadType::Binary);

    let n = client_stream.write(b"Hello!").await.unwrap();
    assert_eq!(n, 6);
    client_stream.flush().await.unwrap();

    let mut buf = [0u8; 6];
    server_stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"Hello!");

    // Flushing again with no write in flight (the `Idle` branch) must also
    // succeed rather than error or hang.
    client_stream.flush().await.unwrap();
}

/// A zero-length data frame is a legal (if unusual) WebSocket message. Make
/// sure `read()` treats it as "no data yet, keep going" rather than
/// mistaking it for EOF (which, for `AsyncRead`, means returning `Ok(())`
/// without filling the buffer at all).
#[tokio::test]
async fn empty_frame_does_not_signal_eof() {
    let (client, server) = connected_pair();
    let mut client_stream = WebSocketStream::new(client, PayloadType::Binary);
    let mut server_stream = WebSocketStream::new(server, PayloadType::Binary);

    client_stream.write(b"").await.unwrap();
    client_stream.write(b"Hello!").await.unwrap();

    let mut buf = [0u8; 6];
    let n = tokio::time::timeout(Duration::from_secs(5), server_stream.read(&mut buf))
        .await
        .expect("timed out - an empty frame may be incorrectly signalling EOF")
        .unwrap();

    assert_eq!(n, 6);
    assert_eq!(&buf, b"Hello!");
    assert!(!server_stream.is_closed());
}

/// `into_inner()` should hand back the underlying `WebSocket` when no read or
/// write future currently owns it.
#[tokio::test]
async fn into_inner_returns_websocket_when_idle() {
    let (client, _server) = connected_pair();
    let stream = WebSocketStream::new(client, PayloadType::Binary);

    assert!(!stream.is_closed());
    assert!(stream.into_inner().is_some());
}
