// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! ZeroMQ 传输层封装。

use tokio::sync::Mutex;
use crate::error::PagodaError;

#[derive(Debug, Clone)]
pub struct ZmqSocketConfig {
    pub endpoint: String,
    pub high_water_mark: i32,
    pub send_timeout_ms: i32,
    pub recv_timeout_ms: i32,
}

impl Default for ZmqSocketConfig {
    fn default() -> Self {
        Self {
            endpoint: "tcp://127.0.0.1:5555".to_string(),
            high_water_mark: 1000,
            send_timeout_ms: -1,
            recv_timeout_ms: -1,
        }
    }
}

/// ZMQ PUB socket 发布者。
pub struct ZmqPublisher {
    config: ZmqSocketConfig,
    socket: Mutex<zeromq::PubSocket>,
}

impl ZmqPublisher {
    /// 创建并绑定 PUB socket。
    pub async fn bind_async(config: ZmqSocketConfig) -> Result<Self, PagodaError> {
        use zeromq::Socket;
        let mut socket = zeromq::PubSocket::new();
        socket.bind(&config.endpoint).await
            .map_err(|e| PagodaError::cannot_connect(format!("ZMQ PUB bind {}: {e}", config.endpoint)))?;
        Ok(Self { config, socket: Mutex::new(socket) })
    }

    /// 同步包装（骨架兼容：不实际绑定，返回 stub）。
    pub fn bind(config: ZmqSocketConfig) -> Result<Self, PagodaError> {
        // 同步版本无法等待 bind；调用方应使用 bind_async
        Err(PagodaError::cannot_connect("ZmqPublisher::bind requires async context; use bind_async"))
    }

    /// 发布消息到指定 topic（topic + 空格 + payload）。
    pub async fn publish_async(&self, topic: &str, payload: &[u8]) -> Result<(), PagodaError> {
        use zeromq::SocketSend;
        let mut data = Vec::with_capacity(topic.len() + 1 + payload.len());
        data.extend_from_slice(topic.as_bytes());
        data.push(b' ');
        data.extend_from_slice(payload);
        let msg = zeromq::ZmqMessage::from(data);
        self.socket.lock().await
            .send(msg).await
            .map_err(|e| PagodaError::cannot_connect(format!("ZMQ publish: {e}")))
    }

    /// 同步发布（骨架兼容）。
    pub fn publish(&self, _topic: &str, _payload: &[u8]) -> Result<(), PagodaError> {
        Err(PagodaError::cannot_connect("ZmqPublisher::publish requires async context; use publish_async"))
    }

    pub fn send_raw(&self, _data: &[u8]) -> Result<(), PagodaError> {
        Err(PagodaError::cannot_connect("ZmqPublisher::send_raw requires async context"))
    }

    pub fn config(&self) -> &ZmqSocketConfig {
        &self.config
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscriberMode {
    Sub,
    Pull,
}

/// ZMQ SUB/PULL socket 订阅者。
pub struct ZmqSubscriber {
    config: ZmqSocketConfig,
    mode: SubscriberMode,
    socket: Mutex<ZmqSubSocket>,
}

enum ZmqSubSocket {
    Sub(zeromq::SubSocket),
    Pull(zeromq::PullSocket),
}

impl ZmqSubscriber {
    /// 创建并连接 SUB socket，订阅指定 topic。
    pub async fn subscribe_async(config: ZmqSocketConfig, topic: &str) -> Result<Self, PagodaError> {
        use zeromq::{Socket, SubSocket};
        let mut socket = SubSocket::new();
        socket.connect(&config.endpoint).await
            .map_err(|e| PagodaError::cannot_connect(format!("ZMQ SUB connect {}: {e}", config.endpoint)))?;
        socket.subscribe(topic).await
            .map_err(|e| PagodaError::cannot_connect(format!("ZMQ SUB subscribe {topic}: {e}")))?;
        Ok(Self {
            config,
            mode: SubscriberMode::Sub,
            socket: Mutex::new(ZmqSubSocket::Sub(socket)),
        })
    }

    /// 同步包装（骨架兼容）。
    pub fn subscribe(config: ZmqSocketConfig, _topic: &str) -> Result<Self, PagodaError> {
        Err(PagodaError::cannot_connect("ZmqSubscriber::subscribe requires async; use subscribe_async"))
    }

    pub fn pull(config: ZmqSocketConfig) -> Result<Self, PagodaError> {
        Err(PagodaError::cannot_connect("ZmqSubscriber::pull requires async; use pull_async"))
    }

    pub async fn pull_async(config: ZmqSocketConfig) -> Result<Self, PagodaError> {
        use zeromq::{Socket, PullSocket};
        let mut socket = PullSocket::new();
        socket.connect(&config.endpoint).await
            .map_err(|e| PagodaError::cannot_connect(format!("ZMQ PULL connect {}: {e}", config.endpoint)))?;
        Ok(Self {
            config,
            mode: SubscriberMode::Pull,
            socket: Mutex::new(ZmqSubSocket::Pull(socket)),
        })
    }

    /// 接收下一条消息（async）。
    pub async fn recv_async(&self) -> Result<Vec<u8>, PagodaError> {
        use zeromq::SocketRecv;
        let mut guard = self.socket.lock().await;
        let msg = match &mut *guard {
            ZmqSubSocket::Sub(s) => s.recv().await,
            ZmqSubSocket::Pull(s) => s.recv().await,
        }.map_err(|e| PagodaError::cannot_connect(format!("ZMQ recv: {e}")))?;
        Ok(msg.iter().flat_map(|b| b.to_vec()).collect())
    }

    /// 同步 recv 骨架（不可用）。
    pub fn recv(&self) -> Result<Vec<u8>, PagodaError> {
        Err(PagodaError::cannot_connect("ZmqSubscriber::recv requires async; use recv_async"))
    }

    pub fn try_recv(&self) -> Result<Option<Vec<u8>>, PagodaError> {
        Err(PagodaError::cannot_connect("ZmqSubscriber::try_recv requires async"))
    }

    pub fn config(&self) -> &ZmqSocketConfig {
        &self.config
    }

    pub fn mode(&self) -> &SubscriberMode {
        &self.mode
    }
}
