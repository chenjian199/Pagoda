// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 基于 NATS 的事件传输实现。

use async_trait::async_trait;
use super::frame::Frame;
use super::transport::{EventTransportTx, EventTransportRx};
use crate::transports::nats::Client as NatsClient;

/// NATS 事件传输（同时实现 Tx 和 Rx）。
pub struct NatsTransport {
    client: NatsClient,
}

impl NatsTransport {
    /// 从已有的 NATS 客户端创建事件传输。
    pub fn new(client: NatsClient) -> Self {
        Self { client }
    }

    /// 从环境变量创建。
    pub async fn from_env() -> Result<Self, crate::error::PagodaError> {
        let client = NatsClient::from_env().await?;
        Ok(Self { client })
    }

    /// 获取内部 NATS 客户端引用。
    pub fn client(&self) -> &NatsClient {
        &self.client
    }
}

/// NATS 发送端。
pub struct NatsTransportTx {
    client: NatsClient,
}

impl NatsTransportTx {
    pub fn new(client: NatsClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl EventTransportTx for NatsTransportTx {
    async fn send(&self, frame: Frame) -> Result<(), crate::error::PagodaError> {
        use super::codec::Codec;
        let encoded = Codec::encode(&frame).map_err(|e| crate::error::PagodaError::unknown(e.to_string()))?;
        self.client.publish(&frame.subject, &encoded).await
    }

    async fn close(&self) -> Result<(), crate::error::PagodaError> {
        Ok(()) // NATS client is shared; Drop handles cleanup
    }
}

/// NATS 接收端。
pub struct NatsTransportRx {
    client: NatsClient,
    /// 存储 subject → Subscription 映射
    subscriptions: std::collections::HashMap<String, crate::transports::nats::Subscription>,
}

impl NatsTransportRx {
    pub fn new(client: NatsClient) -> Self {
        Self {
            client,
            subscriptions: std::collections::HashMap::new(),
        }
    }
}

#[async_trait]
impl EventTransportRx for NatsTransportRx {
    async fn recv(&mut self) -> Option<Result<Frame, crate::error::PagodaError>> {
        use super::codec::Codec;
        // 从第一个可用 subscription 拉取消息
        for sub in self.subscriptions.values_mut() {
            if let Some(msg) = sub.next_message().await {
                return Some(Codec::decode(&msg.payload).map_err(|e| crate::error::PagodaError::unknown(e.to_string())));
            }
        }
        None
    }

    async fn subscribe(&mut self, subject: &str) -> Result<(), crate::error::PagodaError> {
        if self.subscriptions.contains_key(subject) {
            return Ok(());
        }
        let sub = self.client.subscribe(subject).await?;
        self.subscriptions.insert(subject.to_string(), sub);
        Ok(())
    }

    async fn unsubscribe(&mut self, subject: &str) -> Result<(), crate::error::PagodaError> {
        if let Some(sub) = self.subscriptions.remove(subject) {
            sub.unsubscribe().await?;
        }
        Ok(())
    }
}
