mod error;
mod fragment;
mod frame;
mod handshake;
mod proxy;
mod stream;

use bytes::Bytes;

use http_body_util::Empty;
use hyper::body::Incoming;
use hyper::header::{CONNECTION, UPGRADE};
use hyper::upgrade::Upgraded;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;

use std::pin::Pin;

use tokio_util::codec::{FramedRead, FramedWrite};

use url::Url;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};

use crate::error::WebSocketError;
use crate::stream::MaybeTlsStream;
use error::Result;
use fragment::FragmentReader;
use frame::frame::Frame;

#[derive(Debug)]
pub struct WebSocketClient<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    stream: S,
}

impl<S: AsyncRead + AsyncWrite + Unpin> WebSocketClient<S> {
    fn after_handshake(stream: S) -> Self {
        WebSocketClient { stream }
    }
}

impl<S> AsyncRead for WebSocketClient<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.stream).poll_read(cx, buf)
    }
}

impl<S> AsyncWrite for WebSocketClient<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        Pin::new(&mut this.stream).poll_write(cx, buf)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.stream).poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.stream).poll_shutdown(cx)
    }
}

fn req(domain: &str, port: u16, scheme: &str, path: &str) -> Result<Request<Empty<Bytes>>> {
    let uri = format!("{}://{}:{}{}", scheme, domain, port, path);
    let req = Request::builder()
        .method("GET")
        .uri(uri)
        .header("Host", format!("{}:{}", domain, port))
        .header(UPGRADE, "websocket")
        .header(CONNECTION, "upgrade")
        .header("Sec-WebSocket-Key", handshake::generate_key())
        .header("Sec-WebSocket-Version", "13")
        .body(Empty::<Bytes>::new())?;
    Ok(req)
}

struct TokioExecutor;

impl<F> hyper::rt::Executor<F> for TokioExecutor
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, fut: F) {
        tokio::spawn(fut);
    }
}

pub async fn connect(url: &str) -> Result<(TokioIo<Upgraded>, Response<Incoming>)> {
    let url = Url::parse(url)?;
    let domain = url
        .domain()
        .ok_or(WebSocketError::InvalidUrl("missing domain".to_string()))?;
    let port = url
        .port_or_known_default()
        .ok_or(WebSocketError::InvalidUrl("missing port".to_string()))?;
    let req = req(domain, port, url.scheme(), url.path())?;
    let socket = MaybeTlsStream::new(domain, port, url.scheme()).await?;
    handshake::client(&TokioExecutor, req, socket).await
}

pub async fn frame_connect(
    url: &str,
) -> Result<(
    FramedRead<ReadHalf<TokioIo<Upgraded>>, frame::FrameDecoder>,
    FramedWrite<WriteHalf<TokioIo<Upgraded>>, frame::FrameEncoder>,
)> {
    let (stream, _) = connect(url).await?;
    let (r, w) = tokio::io::split(stream);
    let decoder = frame::FrameDecoder::default();
    let framed_read = FramedRead::new(r, decoder);
    let encoder = frame::FrameEncoder::default();
    let framed_write = tokio_util::codec::FramedWrite::new(w, encoder);
    Ok((framed_read, framed_write))
}

pub async fn fragment_connect(
    url: &str,
) -> Result<FragmentReader<FramedRead<ReadHalf<TokioIo<Upgraded>>, frame::FrameDecoder>>> {
    let (framed_read, famed_write) = frame_connect(url).await?;

    let fragmented__read = FragmentReader::new(framed_read);
    let fragmented_write = FragmentReader::new(famed_write);
    Ok(fragmented_ws)
}
