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

##  Example

```rust

use fastwebsockets::{WebSocketError, handshake, upgrade};
use fastwebsockets_stream::{PayloadType, WebSocketStream};
use http_body_util::Empty;
use hyper::body::Bytes;
use hyper::body::Incoming;
use hyper::header::{CONNECTION, UPGRADE};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use std::future::Future;
use std::net::Ipv4Addr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

struct SpawnExecutor;
impl<F> hyper::rt::Executor<F> for SpawnExecutor
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, fut: F) {
        tokio::task::spawn(fut);
    }
}
// Server-side connection handler: upgrades the request, then echoes a
// single binary message back to the client over `WebSocketStream`.
async fn handle(mut request: Request<Incoming>) -> Result<Response<Empty<Bytes>>, WebSocketError> {
    assert!(upgrade::is_upgrade_request(&request));
    let (response, ws_fut) = upgrade::upgrade(&mut request)?;
    tokio::spawn(async move {
        let ws = ws_fut.await.unwrap();
        let mut ws_stream = WebSocketStream::new(ws, PayloadType::Binary);
        let mut buf = [0u8; 6];
        ws_stream.read_exact(&mut buf).await.unwrap();
        ws_stream.write_all(&buf).await.unwrap();
        ws_stream.shutdown().await.unwrap();
    });
    Ok(response)
}
#[tokio::main]
async fn main() {
    // Bind an ephemeral local port and remember the address we actually got.
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    // Accept and upgrade incoming connections in the background.
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let io = TokioIo::new(stream);
            tokio::spawn(async move {
                let _ = http1::Builder::new()
                    .serve_connection(io, service_fn(handle))
                    .with_upgrades()
                    .await;
            });
        }
    });
    // Client: connect to the port the server actually bound above.
    let stream = TcpStream::connect(addr).await.unwrap();
    let request = Request::builder()
        .method("GET")
        .uri(format!("ws://{addr}"))
        .header("Host", addr.to_string())
        .header(UPGRADE, "websocket")
        .header(CONNECTION, "upgrade")
        .header("Sec-WebSocket-Key", handshake::generate_key())
        .header("Sec-WebSocket-Version", "13")
        .body(Empty::<Bytes>::new())
        .unwrap();
    let (ws, _response) = handshake::client(&SpawnExecutor, request, stream)
        .await
        .unwrap();
    let mut ws_stream = WebSocketStream::new(ws, PayloadType::Binary);
    let n = ws_stream.write(b"Hello!").await.unwrap();
    assert_eq!(n, 6);
    let mut buf = [0u8; 6];
    ws_stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"Hello!");
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

