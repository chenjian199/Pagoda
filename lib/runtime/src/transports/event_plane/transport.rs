// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 事件传输 trait 定义。
//!
//! `EventTransportTx` 和 `EventTransportRx` 是事件平面的核心抽象。

use async_trait::async_trait;
use super::frame::Frame;

/// 事件传输发送端 trait。
#[async_trait]
pub trait EventTransportTx: Send + Sync + 'static {
    /// 发送一帧事件。
    async fn send(&self, frame: Frame) -> Result<(), crate::error::PagodaError>;

    /// 批量发送事件帧。
    async fn send_batch(&self, frames: Vec<Frame>) -> Result<(), crate::error::PagodaError> {
        for frame in frames {
            self.send(frame).await?;
        }
        Ok(())
    }

    /// 关闭发送端。
    async fn close(&self) -> Result<(), crate::error::PagodaError>;
}

/// 事件传输接收端 trait。
#[async_trait]
pub trait EventTransportRx: Send + Sync + 'static {
    /// 接收下一帧事件。返回 None 表示流结束。
    async fn recv(&mut self) -> Option<Result<Frame, crate::error::PagodaError>>;

    /// 订阅指定 subject（仅对支持 subject 过滤的传输有效）。
    async fn subscribe(&mut self, subject: &str) -> Result<(), crate::error::PagodaError>;

    /// 取消订阅指定 subject。
    async fn unsubscribe(&mut self, subject: &str) -> Result<(), crate::error::PagodaError>;
}

/// 事件传输工厂 trait，用于创建收发端对。
#[async_trait]
pub trait EventTransportFactory: Send + Sync + 'static {
    /// 创建发送端。
    async fn create_tx(
        &self,
        config: &TransportConfig,
    ) -> Result<Box<dyn EventTransportTx>, crate::error::PagodaError>;

    /// 创建接收端。
    async fn create_rx(
        &self,
        config: &TransportConfig,
    ) -> Result<Box<dyn EventTransportRx>, crate::error::PagodaError>;
}

/// 传输配置。
#[derive(Debug, Clone)]
pub struct TransportConfig {
    /// 传输类型标识
    pub transport_type: TransportType,
    /// 连接端点
    pub endpoint: String,
    /// 初始订阅 subjects
    pub subjects: Vec<String>,
}

/// 传输类型枚举。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportType {
    Nats,
    Zmq,
}
