// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 事件平面：NATS transport 实现
//!
//! ## 设计意图
//! 把 NATS 作为事件平面的底层 transport。借助 [`DistributedRuntime`] 上已经存在
//! 的高层助手 `kv_router_nats_publish` / `kv_router_nats_subscribe`，本文件只做
//! **薄薄一层适配** —— 把它们包装成 [`EventTransportTx`] / [`EventTransportRx`]
//! trait 实现，让上层不感知 NATS 客户端细节。
//!
//! ## 外部契约
//! - [`NatsTransport::new(drt)`]：从 DRT 构造。
//! - impl `EventTransportTx`：`publish(subject, bytes)` 透传到 DRT。
//! - impl `EventTransportRx`：`subscribe(subject)` 返回 `WireStream`，每帧是
//!   NATS message 的 payload。
//! - `kind()` 返回 [`EventTransportKind::Nats`]。

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;

use super::transport::{EventTransportRx, EventTransportTx, WireStream};
use crate::DistributedRuntime;
use crate::discovery::EventTransportKind;

/// NATS transport 适配器。Clone 廉价（只持有一个 DRT 句柄）。
pub struct NatsTransport {
    drt: DistributedRuntime,
}

impl NatsTransport {
    pub fn new(drt: DistributedRuntime) -> Self {
        Self { drt }
    }
}

#[async_trait]
impl EventTransportTx for NatsTransport {
    async fn publish(&self, subject: &str, envelope_bytes: Bytes) -> Result<()> {
        self.drt
            .kv_router_nats_publish(subject.to_string(), envelope_bytes)
            .await
    }

    fn kind(&self) -> EventTransportKind {
        EventTransportKind::Nats
    }
}

#[async_trait]
impl EventTransportRx for NatsTransport {
    async fn subscribe(&self, subject: &str) -> Result<WireStream> {
        let subscriber = self
            .drt
            .kv_router_nats_subscribe(subject.to_string())
            .await?;

        // 每个 NATS message 的 payload 即一帧。stream 即所求。
        Ok(Box::pin(subscriber.map(|msg| Ok(msg.payload))))
    }

    fn kind(&self) -> EventTransportKind {
        EventTransportKind::Nats
    }
}
