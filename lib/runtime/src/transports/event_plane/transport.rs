// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 事件平面：transport 抽象 trait
//!
//! ## 设计意图
//! 事件平面要在 NATS / ZMQ 等多种底层之上提供**统一的 pub/sub API**。本文件
//! 定义两个**对象安全**的 trait —— [`EventTransportTx`] 与 [`EventTransportRx`],
//! 分别表示"发送侧"和"订阅侧"。具体后端（NATS / ZMQ）实现这两个 trait，
//! 上层 `event_plane::mod` 通过 trait object 调度，避免把传输类型泄露到 API 上。
//!
//! ## 外部契约
//! 公开类型：
//! - [`WireStream`]：`Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>`
//! - [`EventTransportTx`]：`async fn publish(&self, &str, Bytes) -> Result<()>` + `kind()`
//! - [`EventTransportRx`]：`async fn subscribe(&self, &str) -> Result<WireStream>` + `kind()`
//!
//! ## 实现要点
//! 与 lib-copy 完全相同的对外签名；本文件没有任何运行时逻辑可发散。文档采用
//! 统一的"## 设计意图 / 外部契约"格式以与本仓库其它模块对齐。

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use std::pin::Pin;

use crate::discovery::EventTransportKind;

/// 订阅产生的**原始字节流**。每个 item 是一帧 envelope（已经按 transport 自己的
/// 帧边界拆好），由上层做反序列化。
pub type WireStream = Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>;

/// 发送侧：把一帧字节投递到某个 subject。
#[async_trait]
pub trait EventTransportTx: Send + Sync {
    async fn publish(&self, subject: &str, envelope_bytes: Bytes) -> Result<()>;

    /// 用于日志 / metrics 区分底层是什么 transport。
    fn kind(&self) -> EventTransportKind;
}

/// 订阅侧：拿到某个 subject 的字节流。
#[async_trait]
pub trait EventTransportRx: Send + Sync {
    async fn subscribe(&self, subject: &str) -> Result<WireStream>;

    fn kind(&self) -> EventTransportKind;
}
