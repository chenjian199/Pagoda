// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 事件帧编解码器。
//!
//! 使用 MessagePack 对 `Frame` 进行序列化和反序列化。

use super::frame::Frame;

/// 编解码错误。
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("序列化失败: {0}")]
    EncodeFailed(String),
    #[error("反序列化失败: {0}")]
    DecodeFailed(String),
    #[error("帧格式无效: {0}")]
    InvalidFrame(String),
}

/// 事件帧编解码器。
///
/// 采用 msgpack 格式：`[subject_len: u32][subject: bytes][payload: bytes]`
pub struct Codec;

impl Codec {
    /// 将 Frame 编码为字节：`[subject_len: u32 LE][subject bytes][payload bytes]`
    pub fn encode(frame: &Frame) -> Result<Vec<u8>, CodecError> {
        let subject_bytes = frame.subject.as_bytes();
        let subject_len = subject_bytes.len() as u32;
        let mut buf = Vec::with_capacity(4 + subject_bytes.len() + frame.payload.len());
        buf.extend_from_slice(&subject_len.to_le_bytes());
        buf.extend_from_slice(subject_bytes);
        buf.extend_from_slice(&frame.payload);
        Ok(buf)
    }

    /// 从字节解码 Frame。
    pub fn decode(data: &[u8]) -> Result<Frame, CodecError> {
        if data.len() < 4 {
            return Err(CodecError::InvalidFrame("too short".to_string()));
        }
        let subject_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if data.len() < 4 + subject_len {
            return Err(CodecError::InvalidFrame(format!(
                "expected {} subject bytes, got {}",
                subject_len,
                data.len() - 4
            )));
        }
        let subject = std::str::from_utf8(&data[4..4 + subject_len])
            .map_err(|e| CodecError::InvalidFrame(format!("invalid utf8 subject: {e}")))?;
        let payload = data[4 + subject_len..].to_vec();
        Ok(Frame::new(subject, payload))
    }

    /// 估算编码后的大小（用于预分配缓冲区）。
    pub fn encoded_size_hint(frame: &Frame) -> usize {
        4 + frame.subject.len() + frame.payload.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_codec_roundtrip() {
        let frame = Frame::new("test.subject", b"hello world".to_vec());
        let encoded = Codec::encode(&frame).unwrap();
        let decoded = Codec::decode(&encoded).unwrap();
        assert_eq!(frame, decoded);
    }
}
