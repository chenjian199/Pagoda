// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Two-part codec: a length-prefixed header followed by a payload body.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

/// A frame consisting of a header and a body, each with independent serialization.
#[derive(Debug, Clone)]
pub struct TwoPartFrame {
    /// Serialized header bytes (e.g. routing metadata).
    pub header: Bytes,
    /// Serialized payload bytes.
    pub body: Bytes,
}

/// Codec that encodes/decodes `TwoPartFrame` on the wire.
///
/// Wire format:
/// ```text
/// [header_len: u32][body_len: u32][header bytes][body bytes]
/// ```
pub struct TwoPartCodec {
    max_frame_size: usize,
}

impl TwoPartCodec {
    pub fn new(max_frame_size: usize) -> Self {
        Self { max_frame_size }
    }
}

impl Default for TwoPartCodec {
    fn default() -> Self {
        Self::new(64 * 1024 * 1024) // 64 MiB
    }
}

impl Decoder for TwoPartCodec {
    type Item = TwoPartFrame;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Need at least 8 bytes for two u32 lengths
        if src.len() < 8 {
            return Ok(None);
        }
        let header_len = u32::from_le_bytes([src[0], src[1], src[2], src[3]]) as usize;
        let body_len = u32::from_le_bytes([src[4], src[5], src[6], src[7]]) as usize;
        let total = 8 + header_len + body_len;
        if header_len + body_len > self.max_frame_size {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "frame too large"));
        }
        if src.len() < total {
            src.reserve(total - src.len());
            return Ok(None);
        }
        src.advance(8);
        let header = src.split_to(header_len).freeze();
        let body = src.split_to(body_len).freeze();
        Ok(Some(TwoPartFrame { header, body }))
    }
}

impl Encoder<TwoPartFrame> for TwoPartCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: TwoPartFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let header_len = item.header.len() as u32;
        let body_len = item.body.len() as u32;
        dst.reserve(8 + item.header.len() + item.body.len());
        dst.put_u32_le(header_len);
        dst.put_u32_le(body_len);
        dst.extend_from_slice(&item.header);
        dst.extend_from_slice(&item.body);
        Ok(())
    }
}
