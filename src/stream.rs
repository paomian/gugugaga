use std::sync::Arc;

use rustls::{ClientConfig, RootCertStore};
use rustls_pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::error::WebSocketError;
#[derive(Debug)]
pub enum MaybeTlsStream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

fn build_tls_connector() -> Result<TlsConnector, WebSocketError> {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    Ok(TlsConnector::from(Arc::new(config)))
}

impl MaybeTlsStream {
    pub async fn new(domain: &str, port: u16, scheme: &str) -> Result<Self, WebSocketError> {
        match scheme {
            "ws" => {
                let stream = TcpStream::connect((domain, port)).await?;
                Ok(MaybeTlsStream::Plain(stream))
            }
            "wss" => {
                let stream = TcpStream::connect((domain, port)).await?;
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

impl AsyncRead for MaybeTlsStream {
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

impl AsyncWrite for MaybeTlsStream {
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
