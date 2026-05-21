// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Zero-copy TCP decoder — reads frames without extra memory copies when possible.

use bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::Decoder;

/// A decoder optimized for zero-copy frame extraction from the TCP read buffer.
///
/// Uses `BytesMut::split_to` + `freeze` to yield `Bytes` handles
/// that share the underlying allocation with the read buffer.
pub struct ZeroCopyTcpDecoder {
    /// Maximum allowed frame size in bytes.
    max_frame_size: usize,
    /// Current decode state.
    state: DecodeState,
}

#[derive(Debug, Clone, Copy)]
enum DecodeState {
    /// Waiting to read the frame length prefix.
    ReadingLength,
    /// Have the length, waiting for enough bytes.
    ReadingPayload { remaining: usize },
}

impl ZeroCopyTcpDecoder {
    pub fn new(max_frame_size: usize) -> Self {
        Self {
            max_frame_size,
            state: DecodeState::ReadingLength,
        }
    }
}

impl Default for ZeroCopyTcpDecoder {
    fn default() -> Self {
        Self::new(128 * 1024 * 1024) // 128 MiB
    }
}

impl Decoder for ZeroCopyTcpDecoder {
    type Item = Bytes;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            match self.state {
                DecodeState::ReadingLength => {
                    if src.len() < 4 {
                        return Ok(None);
                    }
                    let len = u32::from_le_bytes([src[0], src[1], src[2], src[3]]) as usize;
                    if len > self.max_frame_size {
                        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "frame too large"));
                    }
                    src.advance(4);
                    self.state = DecodeState::ReadingPayload { remaining: len };
                }
                DecodeState::ReadingPayload { remaining } => {
                    if src.len() < remaining {
                        src.reserve(remaining - src.len());
                        return Ok(None);
                    }
                    let payload = src.split_to(remaining).freeze();
                    self.state = DecodeState::ReadingLength;
                    return Ok(Some(payload));
                }
            }
        }
    }
}
