mod error;
mod fragment;
mod handshake;
mod protocol;
mod proxy;
mod stream;

use bytes::Bytes;

use futures_util::{Sink, Stream};
use http_body_util::Empty;
use hyper::body::Incoming;
use hyper::header::{CONNECTION, UPGRADE};
use hyper::upgrade::Upgraded;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;

use std::pin::Pin;
use std::str::FromStr;

use tokio_util::codec::{FramedRead, FramedWrite};

use url::Url;

use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};

use crate::protocol::{FrameDecoder, FrameEncoder};
use crate::stream::MaybeTlsStream;
use fragment::{FragmentReader, FragmentWriter};

pub use error::Result;
pub use error::WebSocketError;
pub use fragment::Message;
pub use protocol::frame::Frame;
pub use proxy::{Proxy, open_tunnel};

#[derive(Debug)]
pub struct WebSocketClient<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    stream: S,
}

impl<S: AsyncRead + AsyncWrite + Unpin> WebSocketClient<S> {
    pub fn after_handshake(stream: S) -> Self {
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

pub async fn connect(
    url: &str,
    proxy: Option<Proxy>,
) -> Result<(TokioIo<Upgraded>, Response<Incoming>)> {
    let url = Url::parse(url)?;
    let mut domain = url
        .domain()
        .or(url.host_str())
        .ok_or(WebSocketError::InvalidUrl("missing domain".to_string()))?;
    if domain.starts_with('[') && domain.ends_with(']') {
        domain = &domain[1..domain.len() - 1];
    }
    let port = url
        .port_or_known_default()
        .ok_or(WebSocketError::InvalidUrl("missing port".to_string()))?;
    let req = req(domain, port, url.scheme(), url.path())?;
    let socket = match proxy {
        Some(proxy) => {
            let stream = open_tunnel(domain, port, proxy, true).await?;
            MaybeTlsStream::new(stream, url.scheme(), domain).await?
        }
        None => {
            let stream = Box::new(TcpStream::connect((domain, port)).await?) as _;
            MaybeTlsStream::new(stream, url.scheme(), domain).await?
        }
    };
    handshake::client(&TokioExecutor, req, socket).await
}

pub async fn frame_connect(
    url: &str,
    proxy: Option<Proxy>,
) -> Result<(
    FramedRead<ReadHalf<TokioIo<Upgraded>>, FrameDecoder>,
    FramedWrite<WriteHalf<TokioIo<Upgraded>>, FrameEncoder>,
    Response<Incoming>,
)> {
    let (stream, response) = connect(url, proxy).await?;
    let (r, w) = tokio::io::split(stream);
    let decoder = FrameDecoder::default();
    let framed_read = FramedRead::new(r, decoder);
    let encoder = FrameEncoder;
    let framed_write = tokio_util::codec::FramedWrite::new(w, encoder);
    Ok((framed_read, framed_write, response))
}

pub async fn fragment_connect(
    url: &str,
    proxy: Option<Proxy>,
) -> Result<(ReadHalfStream, WriteHalfSink, Response<Incoming>)> {
    let (framed_read, framed_write, response) = frame_connect(url, proxy).await?;

    let (control_tx, control_rx) = tokio::sync::mpsc::channel::<Message>(200);
    let fragmented_read = FragmentReader::new(framed_read, control_tx);
    let fragmented_write = FragmentWriter::new(framed_write, control_rx);
    Ok((fragmented_read.into(), fragmented_write.into(), response))
}

pub async fn fragment_connect_with_proxy(
    url: &str,
    proxy_url: &str,
) -> Result<(ReadHalfStream, WriteHalfSink, Response<Incoming>)> {
    let proxy = Proxy::from_str(proxy_url)?;
    let (framed_read, framed_write, response) = frame_connect(url, Some(proxy)).await?;
    let (control_tx, control_rx) = tokio::sync::mpsc::channel::<Message>(200);
    let fragmented_read = FragmentReader::new(framed_read, control_tx);
    let fragmented_write = FragmentWriter::new(framed_write, control_rx);
    Ok((fragmented_read.into(), fragmented_write.into(), response))
}

pub struct WriteHalfSink {
    writer: FragmentWriter<FramedWrite<WriteHalf<TokioIo<Upgraded>>, FrameEncoder>>,
}
impl WriteHalfSink {
    pub fn new(
        writer: FragmentWriter<FramedWrite<WriteHalf<TokioIo<Upgraded>>, FrameEncoder>>,
    ) -> Self {
        WriteHalfSink { writer }
    }

    pub fn into_inner(
        self,
    ) -> FragmentWriter<FramedWrite<WriteHalf<TokioIo<Upgraded>>, FrameEncoder>> {
        self.writer
    }
}
impl From<FragmentWriter<FramedWrite<WriteHalf<TokioIo<Upgraded>>, FrameEncoder>>>
    for WriteHalfSink
{
    fn from(
        writer: FragmentWriter<FramedWrite<WriteHalf<TokioIo<Upgraded>>, FrameEncoder>>,
    ) -> Self {
        WriteHalfSink { writer }
    }
}

impl Sink<Message> for WriteHalfSink {
    type Error = WebSocketError;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.writer).poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Message) -> Result<()> {
        let this = self.get_mut();
        Pin::new(&mut this.writer).start_send(item)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.writer).poll_flush(cx)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.writer).poll_close(cx)
    }
}

pub struct ReadHalfStream {
    reader: FragmentReader<FramedRead<ReadHalf<TokioIo<Upgraded>>, FrameDecoder>>,
}

impl ReadHalfStream {
    pub fn new(
        reader: FragmentReader<FramedRead<ReadHalf<TokioIo<Upgraded>>, FrameDecoder>>,
    ) -> Self {
        ReadHalfStream { reader }
    }

    pub fn into_inner(
        self,
    ) -> FragmentReader<FramedRead<ReadHalf<TokioIo<Upgraded>>, FrameDecoder>> {
        self.reader
    }
}

impl Stream for ReadHalfStream {
    type Item = Result<Message>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        Pin::new(&mut this.reader).poll_next(cx)
    }
}

impl From<FragmentReader<FramedRead<ReadHalf<TokioIo<Upgraded>>, FrameDecoder>>>
    for ReadHalfStream
{
    fn from(reader: FragmentReader<FramedRead<ReadHalf<TokioIo<Upgraded>>, FrameDecoder>>) -> Self {
        ReadHalfStream { reader }
    }
}
