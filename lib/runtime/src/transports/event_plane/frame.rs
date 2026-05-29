// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 事件平面：二进制帧格式
//!
//! ## 设计意图
//! ZMQ 等"按字节流读"的 transport 在多帧之间没有天然分界 —— 需要应用层
//! 自己定**帧边界 + 版本号**。本文件提供一个最小可用方案：
//!
//! ```text
//!   +---------+--------------------+-----------------------------+
//!   | version | payload_len (u32)  | payload bytes...            |
//!   |   u8    |   big-endian       | payload_len 字节            |
//!   +---------+--------------------+-----------------------------+
//!     共 5 字节固定头
//! ```
//!
//! 选择 5 字节而不是 8 字节：
//! - u32 已经够大（4GB 上限远超单帧实用尺寸）；
//! - 头越小，小消息（KB 量级）的相对开销越低。
//!
//! ## 外部契约
//! - 常量：[`FRAME_VERSION`] = `1`，[`FRAME_HEADER_SIZE`] = `5`。
//! - [`FrameError`]：4 个变体 `IncompleteHeader / IncompletePayload /
//!   UnsupportedVersion / FrameTooLarge`。
//! - [`FrameHeader { version, payload_len }`]：`encode(&mut BytesMut)` /
//!   `decode(&mut impl Buf)` / `frame_size()`。
//! - [`Frame { header, payload }`]：`new(payload)` / `encode() -> Bytes` /
//!   `decode(impl Buf) -> Result<Frame>` / `size()`。
//!
//! ## 实现要点
//! - 字节序使用大端（`put_u32` / `get_u32` 默认即大端）—— 与 lib-copy 一致。
//! - `Frame::encode` 显式预留 `frame_size()` 容量，避免 `BytesMut` 多次扩容。
//! - 解码逻辑全部走"先检查 remaining 再 advance"，保证无 panic。

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

// =============================================================================
// === 常量 ====================================================================
// =============================================================================

/// 当前帧协议版本号。
pub const FRAME_VERSION: u8 = 1;

/// 帧头固定字节数。
pub const FRAME_HEADER_SIZE: usize = 5;

// =============================================================================
// === FrameError ==============================================================
// =============================================================================

#[derive(Debug, Error)]
pub enum FrameError {
    #[error("Incomplete frame header: expected {FRAME_HEADER_SIZE} bytes, got {0} bytes")]
    IncompleteHeader(usize),

    #[error("Incomplete frame payload: expected {expected} bytes, got {available} bytes")]
    IncompletePayload { expected: usize, available: usize },

    #[error("Unsupported protocol version: {0} (expected {FRAME_VERSION})")]
    UnsupportedVersion(u8),

    #[error("Frame too large: {0} bytes exceeds maximum")]
    FrameTooLarge(usize),
}

// =============================================================================
// === FrameHeader =============================================================
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub version: u8,
    pub payload_len: u32,
}

impl FrameHeader {
    /// 把头写到 `buf` 末尾。
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(self.version);
        buf.put_u32(self.payload_len);
    }

    /// 从 `buf` 起始处消费 5 字节解码头。失败时 buf 状态未定义（按约定调用者
    /// 应在错误时丢弃整段 buffer 重新对齐）。
    pub fn decode(buf: &mut impl Buf) -> Result<Self, FrameError> {
        let remaining = buf.remaining();
        if remaining < FRAME_HEADER_SIZE {
            return Err(FrameError::IncompleteHeader(remaining));
        }

        let version = buf.get_u8();
        if version != FRAME_VERSION {
            return Err(FrameError::UnsupportedVersion(version));
        }

        let payload_len = buf.get_u32();
        Ok(FrameHeader {
            version,
            payload_len,
        })
    }

    /// 头 + payload 总字节数。
    pub fn frame_size(&self) -> usize {
        FRAME_HEADER_SIZE + self.payload_len as usize
    }
}

// =============================================================================
// === Frame ===================================================================
// =============================================================================

#[derive(Debug, Clone)]
pub struct Frame {
    pub header: FrameHeader,
    pub payload: Bytes,
}

impl Frame {
    /// 由 payload 构造一帧，版本号取当前 [`FRAME_VERSION`]。
    pub fn new(payload: Bytes) -> Self {
        let payload_len = payload.len() as u32;
        Self {
            header: FrameHeader {
                version: FRAME_VERSION,
                payload_len,
            },
            payload,
        }
    }

    /// 编码到 wire 字节。
    pub fn encode(&self) -> Bytes {
        let total = self.header.frame_size();
        let mut buf = BytesMut::with_capacity(total);
        self.header.encode(&mut buf);
        buf.put(self.payload.clone());
        buf.freeze()
    }

    /// 从 wire 字节解一帧。
    pub fn decode(mut buf: impl Buf) -> Result<Self, FrameError> {
        let header = FrameHeader::decode(&mut buf)?;
        let need = header.payload_len as usize;
        let have = buf.remaining();
        if have < need {
            return Err(FrameError::IncompletePayload {
                expected: need,
                available: have,
            });
        }
        let payload = buf.copy_to_bytes(need);
        Ok(Frame { header, payload })
    }

    /// 帧总字节数。
    pub fn size(&self) -> usize {
        self.header.frame_size()
    }
}

// =============================================================================
// === 单元测试 ================================================================
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// ## 测试过程
    /// FrameHeader encode 后再 decode，字段一致。
    /// ## 意义
    /// 锁定 5 字节头的 wire layout。
    #[test]
    fn test_frame_header_encode_decode() {
        let header = FrameHeader {
            version: FRAME_VERSION,
            payload_len: 1024,
        };

        let mut buf = BytesMut::new();
        header.encode(&mut buf);
        assert_eq!(buf.len(), FRAME_HEADER_SIZE);

        let decoded = FrameHeader::decode(&mut buf).unwrap();
        assert_eq!(decoded.version, header.version);
        assert_eq!(decoded.payload_len, header.payload_len);
    }

    /// ## 测试过程
    /// 完整 Frame 编解码 roundtrip。
    /// ## 意义
    /// 锁定 header+payload 串接顺序的 wire 兼容性。
    #[test]
    fn test_frame_encode_decode_roundtrip() {
        let payload = Bytes::from("hello world");
        let frame = Frame::new(payload.clone());

        let encoded = frame.encode();
        let decoded = Frame::decode(encoded).unwrap();

        assert_eq!(decoded.header.version, FRAME_VERSION);
        assert_eq!(decoded.payload, payload);
    }

    /// ## 测试过程
    /// 只给 3 字节就 decode，应返回 IncompleteHeader(3)。
    /// ## 意义
    /// 锁定头不全时的错误类型契约。
    #[test]
    fn test_frame_error_incomplete_header() {
        let buf = Bytes::from(vec![1, 2, 3]);
        let result = Frame::decode(buf);
        assert!(matches!(result, Err(FrameError::IncompleteHeader(3))));
    }

    /// ## 测试过程
    /// 头声称 1000 字节但只附 5 字节 payload。
    /// ## 意义
    /// 锁定 payload 不全时的错误类型契约。
    #[test]
    fn test_frame_error_incomplete_payload() {
        let mut buf = BytesMut::new();
        let header = FrameHeader {
            version: FRAME_VERSION,
            payload_len: 1000,
        };
        header.encode(&mut buf);
        buf.put_slice(b"short");

        let result = Frame::decode(buf.freeze());
        assert!(matches!(
            result,
            Err(FrameError::IncompletePayload {
                expected: 1000,
                available: 5
            })
        ));
    }

    /// ## 测试过程
    /// version 字节写 99 然后尝试解头。
    /// ## 意义
    /// 锁定版本不匹配时的错误类型契约（用于将来 wire format 演进）。
    #[test]
    fn test_frame_error_unsupported_version() {
        let mut buf = BytesMut::new();
        buf.put_u8(99);
        buf.put_u32(0);

        let result = FrameHeader::decode(&mut buf);
        assert!(matches!(result, Err(FrameError::UnsupportedVersion(99))));
    }

    /// ## 测试过程
    /// 零长度 payload 的帧应能完整 roundtrip，且 wire 上恰好 5 字节。
    /// ## 意义
    /// 锁定空 payload 边界（心跳类帧也走这个路径）。
    #[test]
    fn test_zero_length_payload() {
        let payload = Bytes::new();
        let frame = Frame::new(payload.clone());

        let encoded = frame.encode();
        assert_eq!(encoded.len(), FRAME_HEADER_SIZE);

        let decoded = Frame::decode(encoded).unwrap();
        assert_eq!(decoded.payload.len(), 0);
    }

    /// ## 测试过程
    /// 构造帧后 `size()` 与 `header.frame_size()` 与 encode 后 bytes 长度三者一致。
    /// ## 意义
    /// 锁定 size 计算与实际字节数一致 —— 上层用它做 buffer 预留。
    #[test]
    fn test_frame_size_self_consistent() {
        let payload = Bytes::from_static(b"abcdefg");
        let frame = Frame::new(payload);
        let encoded = frame.encode();
        assert_eq!(frame.size(), FRAME_HEADER_SIZE + 7);
        assert_eq!(frame.header.frame_size(), encoded.len());
    }
}
