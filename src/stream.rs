use std::sync::Arc;

use rustls::{ClientConfig, RootCertStore};
use rustls_pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::error::WebSocketError;
#[derive(Debug)]
pub enum MaybeTlsStream<S> {
    Plain(S),
    Tls(Box<TlsStream<S>>),
}

fn build_tls_connector() -> Result<TlsConnector, WebSocketError> {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    Ok(TlsConnector::from(Arc::new(config)))
}

impl<S: AsyncRead + AsyncWrite + Unpin> MaybeTlsStream<S> {
    pub async fn new(stream: S, scheme: &str, domain: &str) -> Result<Self, WebSocketError> {
        match scheme {
            "ws" => Ok(MaybeTlsStream::Plain(stream)),
            "wss" => {
                let tls_connector = build_tls_connector()?;
                let dnsname = ServerName::try_from(domain)
                    .map_err(|_| WebSocketError::InvalidUrl("invalid domain".to_string()))?
                    .to_owned();
                let tls_stream = tls_connector.connect(dnsname, stream).await?;
                Ok(MaybeTlsStream::Tls(Box::new(tls_stream)))
            }
            _ => Err(WebSocketError::InvalidUrl("unsupported scheme".to_string())),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for MaybeTlsStream<S> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(stream) => std::pin::Pin::new(stream).poll_read(cx, buf),
            MaybeTlsStream::Tls(stream) => std::pin::Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for MaybeTlsStream<S> {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(stream) => std::pin::Pin::new(stream).poll_write(cx, buf),
            MaybeTlsStream::Tls(stream) => std::pin::Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(stream) => std::pin::Pin::new(stream).poll_flush(cx),
            MaybeTlsStream::Tls(stream) => std::pin::Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(stream) => std::pin::Pin::new(stream).poll_shutdown(cx),
            MaybeTlsStream::Tls(stream) => std::pin::Pin::new(stream).poll_shutdown(cx),
        }
    }
}
