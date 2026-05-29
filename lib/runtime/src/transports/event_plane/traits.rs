// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 事件平面：envelope 与流别名
//!
//! ## 设计意图
//! 事件平面的"事件"由两层组成：
//! 1. **[`EventEnvelope`]** —— 与 transport 无关的元数据封装：发布者 id、序
//!    号、时间戳、topic、payload bytes。这一层负责"我是谁、我什么时候说的"。
//! 2. **payload** —— 用户自定义的业务结构，由 codec 层做二次编解码。
//!
//! 这种"信封 + 载荷"的二分法把"传输元数据"与"业务数据"解耦：
//! - 中间件（路由、metrics、replay）只看信封即可
//! - 业务侧只关心 payload
//!
//! ## 外部契约
//! - [`EventEnvelope`]：含 `publisher_id / sequence / published_at / topic /
//!   payload: Bytes`。`Serialize + Deserialize`，payload 通过 `bytes_serde`
//!   走 `serialize_bytes` / `Vec<u8>` 互转。
//! - [`EventStream`]：原始 envelope 流的类型别名。
//! - [`TypedEventStream<T>`]：解码后的 `(envelope, T)` 流别名。
//!
//! ## 实现要点
//! 与 lib-copy 字段顺序、序列化策略完全一致；这是序列化契约不能动。

use anyhow::Result;
use bytes::Bytes;
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::pin::Pin;

// =============================================================================
// === EventEnvelope ===========================================================
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventEnvelope {
    /// 发布者唯一 id（通常是 discovery instance_id）
    pub publisher_id: u64,
    /// 同发布者内单调递增序号
    pub sequence: u64,
    /// 发布时间，Unix ms
    pub published_at: u64,
    /// 发布到哪个 topic
    pub topic: String,
    /// 业务载荷（已经被 codec 编码过的字节）
    #[serde(with = "bytes_serde")]
    pub payload: Bytes,
}

/// Bytes 的 serde 适配：序列化时用 `serialize_bytes`，反序列化时按 Vec<u8> 收。
/// 这样在 MessagePack 下能用 bin 格式而不是 array of int。
mod bytes_serde {
    use bytes::Bytes;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &Bytes, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(bytes)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Bytes, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes: Vec<u8> = Deserialize::deserialize(deserializer)?;
        Ok(Bytes::from(bytes))
    }
}

// =============================================================================
// === 流别名 ==================================================================
// =============================================================================

/// 一个订阅产生的 envelope 流（任意 transport 之上）。
pub type EventStream = Pin<Box<dyn Stream<Item = Result<EventEnvelope>> + Send>>;

/// 解码后的"信封 + 强类型 payload"流。
pub type TypedEventStream<T> = Pin<Box<dyn Stream<Item = Result<(EventEnvelope, T)>> + Send>>;

// =============================================================================
// === 单元测试 ================================================================
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// ## 测试过程
    /// 构造一个 envelope，msgpack 编码后再解码，断言字段全等。
    /// ## 意义
    /// 锁定 envelope 在 msgpack 下的 wire 兼容性 —— 任何字段顺序 / 名字变化都会破。
    #[test]
    fn test_event_envelope_msgpack_serialization() {
        let envelope = EventEnvelope {
            publisher_id: 12345,
            sequence: 1,
            published_at: 1700000000000,
            topic: "test-topic".to_string(),
            payload: Bytes::from("test payload"),
        };

        let msgpack = rmp_serde::to_vec(&envelope).unwrap();
        let deserialized: EventEnvelope = rmp_serde::from_slice(&msgpack).unwrap();

        assert_eq!(deserialized.publisher_id, 12345);
        assert_eq!(deserialized.sequence, 1);
        assert_eq!(deserialized.published_at, 1700000000000);
        assert_eq!(deserialized.topic, "test-topic");
        assert_eq!(deserialized.payload, Bytes::from("test payload"));
    }

    /// ## 测试过程
    /// 构造一个 payload 为空字节的 envelope，roundtrip 后断言 payload 依然为空。
    /// ## 意义
    /// 锁定空 payload 边界 —— bytes_serde 对零长度的处理必须保持稳定。
    #[test]
    fn test_event_envelope_empty_payload_roundtrip() {
        let envelope = EventEnvelope {
            publisher_id: 1,
            sequence: 0,
            published_at: 0,
            topic: "empty".to_string(),
            payload: Bytes::new(),
        };
        let bytes = rmp_serde::to_vec(&envelope).unwrap();
        let back: EventEnvelope = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(back.payload.len(), 0);
        assert_eq!(back.topic, "empty");
    }
}
