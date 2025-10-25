# fastwebsockets-stream

[![Crates.io](https://img.shields.io/crates/v/fastwebsockets-stream.svg)](https://crates.io/crates/fastwebsockets-stream)
[![Documentation](https://docs.rs/fastwebsockets-stream/badge.svg)](https://docs.rs/fastwebsockets-stream)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

`fastwebsockets-stream` provides alightweight adapter that exposes a `fastwebsockets::WebSocket<S>`
as a `tokio`-compatible byte stream by implementing `AsyncRead` and `AsyncWrite`.
It makes it easy to layer existing codecs (for example `tokio_util::codec::Framed`) or to
reuse I/O-based logic over WebSocket application payloads without reimplementing
WebSocket framing.

## Features

- Converts a `fastwebsockets::WebSocket` into a byte stream.
- Supports `Binary` and `Text` payloads via `PayloadType`.
- Implements both `AsyncRead` and `AsyncWrite` traits.
- Handles control frames (Ping/Pong/Close) automatically through the
  underlying `fastwebsockets` settings.
- Integrates seamlessly with the [`fastwebsockets`] crate and `tokio` ecosystem.

##  Example: Server

```rust
use fastwebsockets::{upgrade, WebSocketError};
use fastwebsockets_stream::{PayloadType, WebSocketStream};
use hyper::{Request, Response};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use http_body_util::Empty;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::net::Ipv4Addr;

#[tokio::test]
async fn server_example() {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0u16))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();

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
                    eprintln!("Error: {:?}", err);
                }
            });
        }
    });

    println!("Server listening on {}", addr);
}

async fn handle(mut req: Request<Incoming>) -> Result<Response<Empty<Bytes>>, WebSocketError> {
    assert!(upgrade::is_upgrade_request(&req));
    let (resp, ws_fut) = upgrade::upgrade(&mut req)?;
    tokio::spawn(async move {
        let mut buf = [0u8; 6];
        let websocket = ws_fut.await.unwrap();
        let mut ws_stream = WebSocketStream::new(websocket, PayloadType::Binary);
        ws_stream.write(b"Hello!").await.unwrap();
        ws_stream.read(&mut buf).await.unwrap();
        assert_eq!(&buf, b"Hello!");
    });
    Ok(resp)
}
```

## Example: Client

```rust
use fastwebsockets::{Frame, OpCode, handshake};
use fastwebsockets_stream::{PayloadType, WebSocketStream};
use http_body_util::Empty;
use hyper::header::{UPGRADE, CONNECTION};
use hyper::body::Bytes;
use tokio::net::TcpStream;

#[tokio::test]
async fn client_example() {
    let addr = "127.0.0.1:9000";
    let stream = TcpStream::connect(addr).await.unwrap();
    let request = http::Request::builder()
        .method("GET")
        .uri("ws://127.0.0.1:9000")
        .header("Host", "127.0.0.1")
        .header(UPGRADE, "websocket")
        .header(CONNECTION, "upgrade")
        .header("Sec-WebSocket-Key", handshake::generate_key())
        .header("Sec-WebSocket-Version", "13")
        .body(Empty::<Bytes>::new())
        .unwrap();

    let (mut ws, _) = handshake::client(&tokio::task::spawn, request, stream).await.unwrap();
    let msg = ws.read_frame().await.unwrap();
    assert_eq!(msg.opcode, OpCode::Binary);
    ws.write_frame(Frame::binary(msg.payload)).await.unwrap();
}
```

## API overview

### `WebSocketStream<S>`

An adapter type that implements `tokio::io::AsyncRead` and
`tokio::io::AsyncWrite` for a wrapped `fastwebsockets::WebSocket<S>`.

**Important behaviors**:

* Reads present application data frames (Text/Binary) as a continuous byte
  stream. If a single websocket frame's payload is larger than the caller's
  read buffer, the remainder is buffered internally and delivered by
  subsequent reads.
* A `Close` frame is mapped to EOF; subsequent reads return `Ok(())` with
  zero bytes.
* Writes produce exactly one WebSocket data frame per call to `write`.
* The adapter temporarily takes ownership of the inner `WebSocket` while an
  asynchronous read or write operation is in flight. `into_inner()` will return
  the inner `WebSocket` only if it is not currently owned by an outstanding
  future.

**Key methods**:

* `WebSocketStream::new(websocket, payload_type)` — create a new adapter.
* `into_inner(self) -> Option<WebSocket<S>>` — attempt to recover the inner
  websocket if no operation is in-progress.
* `is_closed(&self) -> bool` — returns `true` if a Close frame was observed.

### `PayloadType`

Enum with variants `Binary` and `Text` specifying which opcode the stream
expects and will emit.

##  Usage tips

* If you want to use a codec that frames logical application-level messages
  (for example `LinesCodec`, length-delimited frames, or protobuf), combine
  `WebSocketStream` with `tokio_util::codec::Framed`.

* Remember that each call to `write` becomes a single websocket frame. If your
  application expects to stream a single large logical message in several
  frames, implement that message-level framing in your codec.

## License

Licensed under the MIT License. See [LICENSE](./LICENSE) for details.

