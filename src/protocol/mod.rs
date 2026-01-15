use crate::error::WebSocketError;
use bytes::{Buf, BufMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::protocol::frame::FrameHeader;

pub mod coding;
pub mod frame;
pub mod mask;
pub mod utf8;

pub enum FrameDecoderReadingState {
    Header,
    ExtendedPayloadLength,
    MaskingKey,
    Payload,
}

pub struct FrameDecoder {
    state: FrameDecoderReadingState,
    is_final: bool,
    rsv1: bool,
    rsv2: bool,
    rsv3: bool,
    length_code: u8,
    opcode: coding::OpCode,
    extra: usize,
    masked: bool,
    payload_len: Option<usize>,
    masking_key: Option<[u8; 4]>,
}

impl FrameDecoder {
    pub fn reset(&mut self) {
        self.state = FrameDecoderReadingState::Header;
        self.is_final = false;
        self.rsv1 = false;
        self.rsv2 = false;
        self.rsv3 = false;
        self.length_code = 0;
        self.opcode = coding::OpCode::Data(coding::Data::Continue);
        self.extra = 0;
        self.masked = false;
        self.payload_len = None;
        self.masking_key = None;
    }
}

impl Default for FrameDecoder {
    fn default() -> Self {
        Self {
            state: FrameDecoderReadingState::Header,
            is_final: false,
            rsv1: false,
            rsv2: false,
            rsv3: false,
            length_code: 0,
            opcode: coding::OpCode::Data(coding::Data::Continue),
            extra: 0,
            masked: false,
            payload_len: None,
            masking_key: None,
        }
    }
}

impl Decoder for FrameDecoder {
    type Item = frame::Frame;
    type Error = WebSocketError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            match self.state {
                FrameDecoderReadingState::Header => {
                    if src.remaining() < 2 {
                        return Ok(None);
                    }
                    // Read the header bytes
                    let b1 = src[0];
                    let b2 = src[1];
                    // Parse the header fields
                    let fin = (b1 & 0x80) != 0;
                    let rsv1 = (b1 & 0x40) != 0;
                    let rsv2 = (b1 & 0x20) != 0;
                    let rsv3 = (b1 & 0x10) != 0;
                    let opcode = coding::OpCode::from(b1 & 0x0F);
                    let masked = (b2 & 0x80) != 0;
                    let length_code = b2 & 0x7F;
                    let extra = match length_code {
                        126 => 2,
                        127 => 8,
                        _ => 0,
                    };
                    src.advance(2);

                    self.state = FrameDecoderReadingState::ExtendedPayloadLength;
                    self.is_final = fin;
                    self.rsv1 = rsv1;
                    self.rsv2 = rsv2;
                    self.rsv3 = rsv3;
                    self.length_code = length_code;
                    self.opcode = opcode;
                    self.extra = extra;
                    self.masked = masked;
                }
                FrameDecoderReadingState::ExtendedPayloadLength => {
                    let needed = self.extra;
                    if src.remaining() < needed {
                        return Ok(None);
                    }
                    let payload_len = match self.extra {
                        2 => {
                            let len = src.get_u16();
                            len as usize
                        }
                        8 => {
                            let len = src.get_u64();
                            len as usize
                        }
                        _ => self.length_code as usize,
                    };
                    self.state = FrameDecoderReadingState::MaskingKey;
                    self.payload_len = Some(payload_len);
                }
                FrameDecoderReadingState::MaskingKey => {
                    let masking_key = if self.masked {
                        if src.remaining() < 4 {
                            return Ok(None);
                        }
                        let mask = src.get_u32().to_be_bytes();
                        Some(mask)
                    } else {
                        None
                    };
                    self.state = FrameDecoderReadingState::Payload;
                    self.masking_key = masking_key;
                }
                FrameDecoderReadingState::Payload => {
                    let payload_len = self.payload_len.unwrap_or(0);
                    if src.remaining() < payload_len {
                        return Ok(None);
                    }
                    let mut payload = src.split_to(payload_len).to_vec();
                    if let Some(mask) = self.masking_key {
                        mask::apply_mask(&mut payload, mask);
                    }
                    let frame = frame::Frame::from_payload(
                        FrameHeader {
                            is_final: self.is_final,
                            rsv1: self.rsv1,
                            rsv2: self.rsv2,
                            rsv3: self.rsv3,
                            opcode: self.opcode,
                            mask: self.masking_key,
                        },
                        payload.into(),
                    );
                    // Reset state for next frame
                    self.state = FrameDecoderReadingState::Header;
                    return Ok(Some(frame));
                }
            }
        }
    }
}

pub struct FrameEncoder;

impl Encoder<frame::Frame> for FrameEncoder {
    type Error = WebSocketError;

    fn encode(&mut self, item: frame::Frame, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        let mut writer = dst.writer();
        item.format(&mut writer)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[tokio::test]
    async fn test_frame_encoder_decoder() {
        let mut encoder = FrameEncoder;
        let mut decoder = FrameDecoder::default();

        let original_payload = b"Hello, WebSocket!".to_vec();
        let frame = frame::Frame::from_payload(
            FrameHeader {
                is_final: true,
                rsv1: false,
                rsv2: false,
                rsv3: false,
                opcode: coding::OpCode::Data(coding::Data::Text),
                mask: None,
            },
            Bytes::from(original_payload.clone()),
        );

        let mut buf = bytes::BytesMut::new();
        encoder.encode(frame, &mut buf).unwrap();

        let decoded_frame = decoder.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded_frame.payload(), &original_payload[..]);
    }
}
