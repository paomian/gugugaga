#[derive(Clone, Debug)]
pub struct GuguGagaConfig {
    pub max_frame_size: usize,
    pub max_message_size: usize,
    pub tcp_timeout_secs: u64,
}

impl Default for GuguGagaConfig {
    fn default() -> Self {
        GuguGagaConfig {
            max_frame_size: 16 * 1024 * 1024,   // 16 MB
            max_message_size: 64 * 1024 * 1024, // 64 MB
            tcp_timeout_secs: 10,               // 10 seconds
        }
    }
}
