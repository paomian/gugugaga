#[derive(Clone, Debug)]
pub struct GuguGagaConfig {
    pub max_frame_size: usize,
    /// Maximum outbound data-frame payload. Smaller values allow finer fragmentation.
    /// Capped at max_frame_size; both sizes must be nonzero.
    pub send_frame_size: usize,
    pub max_message_size: usize,
    pub tcp_timeout_secs: u64,
}

impl Default for GuguGagaConfig {
    fn default() -> Self {
        GuguGagaConfig {
            max_frame_size: 16 * 1024 * 1024,   // 16 MB
            send_frame_size: 16 * 1024 * 1024,  // 16 MB
            max_message_size: 64 * 1024 * 1024, // 64 MB
            tcp_timeout_secs: 10,               // 10 seconds
        }
    }
}

impl GuguGagaConfig {
    pub(crate) fn outbound_frame_size(&self) -> crate::error::Result<usize> {
        if self.send_frame_size == 0 || self.max_frame_size == 0 {
            return Err(crate::error::WebSocketError::InvalidValue);
        }
        Ok(self.send_frame_size.min(self.max_frame_size))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outbound_size_respects_frame_limit_and_rejects_zero() {
        let mut config = GuguGagaConfig::default();
        assert_eq!(config.outbound_frame_size().unwrap(), 16 * 1024 * 1024);
        config.max_frame_size = 1024;
        assert_eq!(config.outbound_frame_size().unwrap(), 1024);
        config.send_frame_size = 512;
        assert_eq!(config.outbound_frame_size().unwrap(), 512);
        config.send_frame_size = 0;
        assert!(config.outbound_frame_size().is_err());
        config.send_frame_size = 512;
        config.max_frame_size = 0;
        assert!(config.outbound_frame_size().is_err());
    }
}
