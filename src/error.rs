use thiserror::Error;

#[derive(Error, Debug)]
pub enum WebSocketError {
    #[error("invalid url: {0}")]
    InvalidUrl(String),
    #[error("unsupported scheme: {0}")]
    InvalidScheme(String),
    #[error("invalid dns name: {0}")]
    InvalidDnsName(String),
    #[error("proxy response too large")]
    ProxyResponseTooLarge,
    #[error("proxy unexpected eof")]
    ProxyUnexpectedEof,
    #[error("proxy status code: {0}")]
    ProxyStatusCode(String),
    #[error("invalid dns name error: {0}")]
    InvalidDnsNameError(#[from] rustls_pki_types::InvalidDnsNameError),
    #[error("no host name in url")]
    NoHostName,
    #[error("handshake failed: {0}")]
    Handshake(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("tls error: {0}")]
    Tls(String),
    #[error("unexpected opcode: {0}")]
    UnexpectedOpcode(u8),
    #[error("connection closed")]
    ConnectionClosed,
    #[error("hyper error: {0}")]
    Hyper(#[from] hyper::Error),
    #[error("invalid status code: {0}")]
    InvalidStatusCode(u16),
    #[error("invalid Upgrade header")]
    InvalidUpgradeHeader,
    #[error("invalid Connection header")]
    InvalidConnectionHeader,
    #[error("frame payload too large: {size} bytes exceeds {max} bytes")]
    FrameTooLarge { size: usize, max: usize },
    #[error("invalid value")]
    InvalidValue,
    #[error("invalid UTF-8 sequence")]
    InvalidUTF8,
    #[error("invalid fragment")]
    InvalidFragment,
    #[error("invalid continuation frame")]
    InvalidContinuationFrame,
    #[error("frame header reserved bits not zero")]
    ReservedBitsNotZero,
    #[error("invalid opcode: {0}")]
    InvalidOpcode(u8),
    #[error("invalid close sequence")]
    InvalidCloseSequence,
    #[error("UTF-8 error: {0}")]
    Utf8Error(#[from] std::str::Utf8Error),
    #[error("reserved data opcode: {0}")]
    ReservedDataOpcode(u8),
    #[error("reserved control opcode: {0}")]
    ReservedControlOpcode(u8),
    #[error("hyper http error: {0}")]
    HyperHttp(#[from] hyper::http::Error),
    #[error("url parse error: {0}")]
    UrlParse(#[from] url::ParseError),
    #[error("masked frame received from server")]
    MaskedFrameFromServer,
    #[error("tokio timeout elapsed")]
    Elapsed(#[from] tokio::time::error::Elapsed),
}

pub type Result<T> = std::result::Result<T, WebSocketError>;
