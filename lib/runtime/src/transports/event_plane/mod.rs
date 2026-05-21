// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 事件平面（Event Plane）抽象层。
//!
//! 提供统一的 pub/sub 事件传输接口，可切换 NATS / ZMQ 后端。

pub mod frame;
pub mod codec;
pub mod transport;
pub mod nats_transport;
pub mod zmq_transport;
pub mod dynamic_subscriber;

pub use frame::Frame;
pub use codec::Codec;
pub use transport::{EventTransportTx, EventTransportRx};
pub use nats_transport::NatsTransport;
pub use zmq_transport::ZmqTransport;
pub use dynamic_subscriber::DynamicSubscriber;

use std::marker::PhantomData;

/// 类型化事件发布者。
///
/// 将类型 `T` 序列化后通过底层 `EventTransportTx` 发送。
pub struct EventPublisher<T> {
    transport: Box<dyn EventTransportTx>,
    subject: String,
    _phantom: PhantomData<T>,
}

impl<T> EventPublisher<T>
where
    T: serde::Serialize + Send + 'static,
{
    /// 创建事件发布者。
    pub fn new(transport: Box<dyn EventTransportTx>, subject: String) -> Self {
        Self {
            transport,
            subject,
            _phantom: PhantomData,
        }
    }

    /// 发布一个事件。
    pub async fn publish(&self, event: &T) -> Result<(), crate::error::PagodaError> {
        let payload = serde_json::to_vec(event)
            .map_err(|e| crate::error::PagodaError::invalid_argument(format!("serialize: {e}")))?;
        let frame = Frame::new(self.subject.clone(), payload);
        self.transport.send(frame).await
    }

    /// 获取关联的 subject。
    pub fn subject(&self) -> &str {
        &self.subject
    }
}

/// 类型化事件订阅者。
///
/// 从底层 `EventTransportRx` 接收并反序列化为类型 `T`。
pub struct EventSubscriber<T> {
    transport: Box<dyn EventTransportRx>,
    _phantom: PhantomData<T>,
}

impl<T> EventSubscriber<T>
where
    T: serde::de::DeserializeOwned + Send + 'static,
{
    /// 创建事件订阅者。
    pub fn new(transport: Box<dyn EventTransportRx>) -> Self {
        Self {
            transport,
            _phantom: PhantomData,
        }
    }

    /// 接收下一个事件。
    pub async fn next_event(&mut self) -> Option<Result<T, crate::error::PagodaError>> {
        match self.transport.recv().await {
            None => None,
            Some(Err(e)) => Some(Err(e)),
            Some(Ok(frame)) => {
                let result = serde_json::from_slice::<T>(&frame.payload)
                    .map_err(|e| crate::error::PagodaError::invalid_argument(format!("deserialize: {e}")));
                Some(result)
            }
        }
    }
}
