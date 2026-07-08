//! Regression test for consistent error reporting once the underlying
//! websocket is gone.
//!
//! `WebSocketStream` takes ownership of the inner `WebSocket` only while a
//! read or write future is in flight. If that future ends in a fatal error
//! from `read_frame()`/`write_frame()` itself (a transport-level failure,
//! e.g. the peer disconnects mid-frame), the websocket goes down with it and
//! `self.websocket` is left empty forever - the connection is unusable after
//! that point.
//!
//! Note that this is *not* the same as a payload-type mismatch (as in
//! `error.rs`): that check runs in `poll_read` only *after* `read_frame()`
//! has already succeeded and the websocket has already been restored to
//! `self.websocket`, so the stream stays perfectly usable afterwards. To
//! actually lose the websocket we need `read_frame()` itself to fail, which
//! this test triggers by having the peer disconnect in the middle of a
//! frame.
//!
//! Previously, `poll_write` correctly reported the "websocket is gone" case
//! as an error, but `poll_flush` and `poll_shutdown` silently returned
//! `Ok(())` instead, which looks exactly like "there was nothing to do, all
//! is well" even though nothing was actually flushed or closed. This test
//! checks that `write`, `flush`, `shutdown`, and a second `read` all agree
//! and report an error once the stream is in this state.
//!
//! As in `fragmentation.rs`, this connects over an in-memory
//! `tokio::io::duplex` pipe (via `WebSocket::after_handshake`) rather than a
//! real TCP + hyper upgrade, since only `WebSocketStream`'s own bookkeeping
//! is under test here. Everything happens in a single test function, so a
//! failing assertion always fails the test - there's no spawned task whose
//! panic could be missed.

use fastwebsockets::{Role, WebSocket};
use fastwebsockets_stream::{PayloadType, WebSocketStream};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn write_flush_shutdown_error_after_fatal_read_error() {
    // Build the server side on top of a raw duplex half so we can corrupt
    // the connection at the transport level - a real `WebSocket` on the
    // other end would never let us send a broken frame like this.
    let (mut client_io, server_io) = tokio::io::duplex(1024);
    let server = WebSocket::after_handshake(server_io, Role::Server);
    let mut server_stream = WebSocketStream::new(server, PayloadType::Binary);

    // Write a single byte that only starts a frame header (FIN=1,
    // opcode=Binary), then drop the connection before the frame is
    // complete. Any correct WebSocket implementation must treat this
    // abrupt, mid-frame disconnect as a fatal error, not as a clean EOF.
    client_io.write_all(&[0x82]).await.unwrap();
    drop(client_io);

    let mut buf = [0u8; 16];
    tokio::time::timeout(Duration::from_secs(5), server_stream.read(&mut buf))
        .await
        .expect("timed out - a mid-frame disconnect should error, not hang")
        .unwrap_err();

    // From here on the websocket is gone for good. Every operation should
    // report that clearly instead of some succeeding silently.
    let write_err = server_stream.write(b"x").await.unwrap_err();
    assert_eq!(write_err.to_string(), "Websocket not available");

    let flush_err = server_stream.flush().await.unwrap_err();
    assert_eq!(flush_err.to_string(), "Websocket not available");

    let shutdown_err = server_stream.shutdown().await.unwrap_err();
    assert_eq!(shutdown_err.to_string(), "Websocket not available");

    let second_read_err = server_stream.read(&mut buf).await.unwrap_err();
    assert_eq!(second_read_err.to_string(), "Websocket not available");
}
