// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 基于 ZeroMQ 的事件传输实现。

use async_trait::async_trait;
use super::frame::Frame;
use super::transport::{EventTransportTx, EventTransportRx};
use crate::transports::zmq::{ZmqPublisher, ZmqSubscriber};

/// ZMQ 事件传输。
pub struct ZmqTransport {
    _endpoint: String,
}

impl ZmqTransport {
    /// 创建 ZMQ 事件传输实例。
    pub fn new(endpoint: String) -> Self {
        Self { _endpoint: endpoint }
    }

    /// 创建发送端（PUB socket）。
    pub fn create_tx(&self) -> Result<ZmqTransportTx, crate::error::PagodaError> {
        // ZmqPublisher::bind is sync stub; real bind requires async
        // Return error directing callers to use bind_async
        Err(crate::error::PagodaError::cannot_connect(
            "ZmqTransport::create_tx requires async; call bind_async on ZmqPublisher directly"
        ))
    }

    /// 创建接收端（SUB socket）。
    pub fn create_rx(&self) -> Result<ZmqTransportRx, crate::error::PagodaError> {
        Err(crate::error::PagodaError::cannot_connect(
            "ZmqTransport::create_rx requires async; call subscribe_async on ZmqSubscriber directly"
        ))
    }
}

/// ZMQ 发送端。
pub struct ZmqTransportTx {
    _publisher: ZmqPublisher,
}

#[async_trait]
impl EventTransportTx for ZmqTransportTx {
    async fn send(&self, frame: Frame) -> Result<(), crate::error::PagodaError> {
        use super::codec::Codec;
        let encoded = Codec::encode(&frame)
            .map_err(|e| crate::error::PagodaError::unknown(e.to_string()))?;
        self._publisher.publish_async(&frame.subject, &encoded).await
    }

    async fn close(&self) -> Result<(), crate::error::PagodaError> {
        Ok(()) // socket dropped on ZmqPublisher drop
    }
}

/// ZMQ 接收端。
pub struct ZmqTransportRx {
    _subscriber: ZmqSubscriber,
}

#[async_trait]
impl EventTransportRx for ZmqTransportRx {
    async fn recv(&mut self) -> Option<Result<Frame, crate::error::PagodaError>> {
        use super::codec::Codec;
        match self._subscriber.recv_async().await {
            Ok(data) => Some(Codec::decode(&data).map_err(|e| crate::error::PagodaError::unknown(e.to_string()))),
            Err(e) => Some(Err(e)),
        }
    }

    async fn subscribe(&mut self, _subject: &str) -> Result<(), crate::error::PagodaError> {
        // ZMQ SUB filter is set at socket creation time; dynamic subscription not supported here
        Ok(())
    }

    async fn unsubscribe(&mut self, _subject: &str) -> Result<(), crate::error::PagodaError> {
        Ok(())
    }
}
