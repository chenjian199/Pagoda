// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 事件平面：codec（信封 / 载荷的序列化）
//!
//! ## 设计意图
//! 把"信封"和"业务 payload"的字节编解码集中在一个**可选枚举**里。当前仅
//! [`MsgpackCodec`] 一种实现，但用 `enum Codec` 包了一层 ——
//! - 留出未来扩展（CBOR / JSON / protobuf）的口子；
//! - 让上层在配置时只需要一个 `Codec` 值而不是泛型；
//! - 对热路径几乎零开销：内部 `match` 在常量分支上会被编译器 devirtualize。
//!
//! 选择 MessagePack 而非 JSON / bincode：
//! - 比 JSON 小、比 bincode 兼容性好（带字段名版本演进友好）；
//! - rmp-serde 与本仓库已有依赖一致。
//!
//! ## 外部契约
//! - [`Codec`]：`Default = Msgpack(MsgpackCodec)`；方法 `encode_envelope` /
//!   `decode_envelope` / `encode_payload<T>` / `decode_payload<T>` / `name()`。
//! - [`MsgpackCodec`]：上述方法的具体实现；`name() == "msgpack"`。
//!
//! ## 实现要点
//! - 用 `rmp_serde::to_vec_named` 编码（带字段名）—— 便于跨版本演进；
//!   解码侧用 `from_slice`，rmp_serde 同时支持带名 / 不带名两种 wire 格式。
//! - 所有"实际编解码"集中在 `MsgpackCodec` 里，`Codec` 上的方法直接
//!   delegate，避免重复实现。

use anyhow::Result;
use bytes::Bytes;
use serde::{Serialize, de::DeserializeOwned};

use super::EventEnvelope;

// =============================================================================
// === Codec 枚举 ==============================================================
// =============================================================================

/// 事件平面 codec 选择器。当前仅 MessagePack。
#[derive(Debug, Clone, Copy)]
pub enum Codec {
    Msgpack(MsgpackCodec),
}

impl Default for Codec {
    fn default() -> Self {
        Codec::Msgpack(MsgpackCodec)
    }
}

impl Codec {
    /// 编码 envelope 到 wire bytes。
    pub fn encode_envelope(&self, envelope: &EventEnvelope) -> Result<Bytes> {
        let Codec::Msgpack(c) = self;
        c.encode_envelope(envelope)
    }

    /// 解码 wire bytes 到 envelope。
    pub fn decode_envelope(&self, bytes: &Bytes) -> Result<EventEnvelope> {
        let Codec::Msgpack(c) = self;
        c.decode_envelope(bytes)
    }

    /// 编码强类型 payload 到字节（用于嵌入 envelope.payload）。
    pub fn encode_payload<T: Serialize>(&self, payload: &T) -> Result<Bytes> {
        let Codec::Msgpack(c) = self;
        c.encode_payload(payload)
    }

    /// 反解 payload 字节到强类型。
    pub fn decode_payload<T: DeserializeOwned>(&self, bytes: &Bytes) -> Result<T> {
        let Codec::Msgpack(c) = self;
        c.decode_payload(bytes)
    }

    /// 用于日志识别。
    pub fn name(&self) -> &'static str {
        let Codec::Msgpack(c) = self;
        c.name()
    }
}

// =============================================================================
// === MsgpackCodec 具体实现 ===================================================
// =============================================================================

#[derive(Debug, Clone, Copy, Default)]
pub struct MsgpackCodec;

impl MsgpackCodec {
    pub fn encode_envelope(&self, envelope: &EventEnvelope) -> Result<Bytes> {
        Ok(Bytes::from(rmp_serde::to_vec_named(envelope)?))
    }

    pub fn decode_envelope(&self, bytes: &Bytes) -> Result<EventEnvelope> {
        Ok(rmp_serde::from_slice(bytes)?)
    }

    pub fn encode_payload<T: Serialize>(&self, payload: &T) -> Result<Bytes> {
        Ok(Bytes::from(rmp_serde::to_vec_named(payload)?))
    }

    pub fn decode_payload<T: DeserializeOwned>(&self, bytes: &Bytes) -> Result<T> {
        Ok(rmp_serde::from_slice(bytes)?)
    }

    pub fn name(&self) -> &'static str {
        "msgpack"
    }
}

// =============================================================================
// === 单元测试 ================================================================
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Serialize, serde::Deserialize)]
    struct TestEvent {
        worker_id: u64,
        message: String,
    }

    /// ## 测试过程
    /// MsgpackCodec 编码 envelope，再解回来，字段全等。
    /// ## 意义
    /// 锁定 envelope wire format 的 roundtrip 契约。
    #[test]
    fn test_msgpack_codec_envelope_roundtrip() {
        let codec = MsgpackCodec;

        let envelope = EventEnvelope {
            publisher_id: 12345,
            sequence: 42,
            published_at: 1700000000000,
            topic: "test-topic".to_string(),
            payload: Bytes::from("test payload"),
        };

        let encoded = codec.encode_envelope(&envelope).unwrap();
        let decoded = codec.decode_envelope(&encoded).unwrap();

        assert_eq!(decoded.publisher_id, envelope.publisher_id);
        assert_eq!(decoded.sequence, envelope.sequence);
        assert_eq!(decoded.published_at, envelope.published_at);
        assert_eq!(decoded.topic, envelope.topic);
        assert_eq!(decoded.payload, envelope.payload);
    }

    /// ## 测试过程
    /// 强类型 payload 编解码 roundtrip。
    /// ## 意义
    /// 锁定 payload 泛型 API 的契约。
    #[test]
    fn test_msgpack_codec_payload_roundtrip() {
        let codec = MsgpackCodec;

        let event = TestEvent {
            worker_id: 123,
            message: "hello world".to_string(),
        };

        let encoded = codec.encode_payload(&event).unwrap();
        let decoded: TestEvent = codec.decode_payload(&encoded).unwrap();

        assert_eq!(decoded, event);
    }

    /// ## 测试过程
    /// `Codec::default()` 是 Msgpack；`name()` 返回 `"msgpack"`；通过 Codec 包装
    /// 调用 envelope/payload 编解码两条路径与直接调 MsgpackCodec 等价。
    /// ## 意义
    /// 锁定 Codec 包装层的"零损耗 delegation"契约。
    #[test]
    fn test_codec_default_and_delegation() {
        let codec = Codec::default();
        assert_eq!(codec.name(), "msgpack");

        let envelope = EventEnvelope {
            publisher_id: 7,
            sequence: 7,
            published_at: 7,
            topic: "t".to_string(),
            payload: Bytes::from_static(b"p"),
        };
        let v = codec.encode_envelope(&envelope).unwrap();
        let direct = MsgpackCodec.encode_envelope(&envelope).unwrap();
        assert_eq!(v, direct);

        let p = codec.encode_payload(&42u64).unwrap();
        let back: u64 = codec.decode_payload(&p).unwrap();
        assert_eq!(back, 42);
    }
}
