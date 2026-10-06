use crate::error::WebSocketError;
use bytes::Buf;
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
    max_frame_size: usize,
}

impl FrameDecoder {
    pub fn new(max_frame_size: usize) -> Self {
        Self {
            max_frame_size,
            ..Self::default()
        }
    }

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
            max_frame_size: crate::config::GuguGagaConfig::default().max_frame_size,
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
                            if len & (1 << 63) != 0 {
                                return Err(WebSocketError::InvalidValue);
                            }
                            usize::try_from(len).map_err(|_| WebSocketError::InvalidValue)?
                        }
                        _ => self.length_code as usize,
                    };
                    if payload_len > self.max_frame_size {
                        return Err(WebSocketError::FrameTooLarge {
                            size: payload_len,
                            max: self.max_frame_size,
                        });
                    }
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
                    let payload_len = self.payload_len.unwrap_or(0);
                    if src.len() < payload_len {
                        // Reserve once per frame, after validating its advertised length.
                        src.reserve(payload_len - src.len());
                    }
                    self.state = FrameDecoderReadingState::Payload;
                    self.masking_key = masking_key;
                }
                FrameDecoderReadingState::Payload => {
                    let payload_len = self.payload_len.unwrap_or(0);
                    if src.remaining() < payload_len {
                        return Ok(None);
                    }
                    let mut payload = src.split_to(payload_len);
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
                        payload.freeze(),
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
        item.format_into_bytes_mut(dst)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn encode_matches_wire_format_without_mutating_shared_payload() {
        for size in [0, 1, 125, 126, 65535, 65536, 1024 * 1024] {
            for mask in [None, Some([0x12, 0x34, 0x56, 0x78])] {
                let shared = Bytes::from(vec![0x5a; size]);
                let frame = frame::Frame::from_payload(
                    FrameHeader {
                        opcode: coding::OpCode::Data(coding::Data::Binary),
                        mask,
                        ..Default::default()
                    },
                    shared.clone(),
                );
                let mut expected = b"existing prefix".to_vec();
                frame.clone().format(&mut expected).unwrap();
                let mut actual = bytes::BytesMut::from(&b"existing prefix"[..]);
                FrameEncoder.encode(frame, &mut actual).unwrap();
                assert_eq!(&actual[..], expected);
                assert!(shared.iter().all(|&b| b == 0x5a));
            }
        }
    }

    #[test]
    fn decoded_payload_survives_following_frames_and_buffer_reuse() {
        let mut wire = Vec::new();
        for payload in [b"first payload".as_slice(), b"second".as_slice()] {
            frame::Frame::from_payload(
                FrameHeader {
                    opcode: coding::OpCode::Data(coding::Data::Binary),
                    mask: Some([1, 2, 3, 4]),
                    ..Default::default()
                },
                Bytes::copy_from_slice(payload),
            )
            .format(&mut wire)
            .unwrap();
        }
        let mut decoder = FrameDecoder::default();
        let mut src = bytes::BytesMut::new();
        let mut received = Vec::new();
        // Exercise headers and payloads split across arbitrary TCP reads.
        for byte in wire {
            src.extend_from_slice(&[byte]);
            if let Some(frame) = decoder.decode(&mut src).unwrap() {
                received.push(frame.into_payload());
            }
        }
        src.extend_from_slice(&vec![0xff; 65536]);
        assert_eq!(&received[0][..], b"first payload");
        assert_eq!(&received[1][..], b"second");
    }

    #[test]
    fn oversized_or_invalid_lengths_fail_before_reservation() {
        let mut decoder = FrameDecoder::new(512);
        decoder.reset();
        let mut src = bytes::BytesMut::from(&[0x82, 126, 0x02, 0x01][..]);
        let capacity = src.capacity();
        assert!(matches!(
            decoder.decode(&mut src),
            Err(WebSocketError::FrameTooLarge {
                size: 513,
                max: 512
            })
        ));
        assert!(src.capacity() <= capacity);
        let mut decoder = FrameDecoder::default();
        let mut src = bytes::BytesMut::from(&[0x82, 127, 0x80, 0, 0, 0, 0, 0, 0, 0][..]);
        let capacity = src.capacity();
        assert!(matches!(
            decoder.decode(&mut src),
            Err(WebSocketError::InvalidValue)
        ));
        assert!(src.capacity() <= capacity);
    }

    #[test]
    fn reserves_complete_payload_once_and_accepts_exact_limit() {
        let size = 1024 * 1024;
        let mut decoder = FrameDecoder::new(size);
        let mut src = bytes::BytesMut::from(&[0x82, 127][..]);
        src.extend_from_slice(&(size as u64).to_be_bytes());
        assert!(decoder.decode(&mut src).unwrap().is_none());
        assert!(src.capacity() >= size);
        let capacity = src.capacity();
        src.extend_from_slice(&[0x5a; 1024]);
        assert!(decoder.decode(&mut src).unwrap().is_none());
        assert_eq!(src.capacity(), capacity);
        src.resize(size, 0x5a);
        let frame = decoder.decode(&mut src).unwrap().unwrap();
        assert_eq!(frame.payload().len(), size);
        assert!(frame.payload().iter().all(|&byte| byte == 0x5a));
    }

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
