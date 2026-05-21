// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! NATS 客户端封装。

use std::sync::Arc;
use std::time::Duration;

use crate::error::PagodaError;

/// NATS 认证方式。
#[derive(Debug, Clone)]
pub enum NatsAuth {
    None,
    Token(String),
    UserPassword { username: String, password: String },
}

impl Default for NatsAuth {
    fn default() -> Self {
        Self::None
    }
}

/// TLS 配置。
#[derive(Debug, Clone)]
pub struct NatsTlsConfig {
    pub ca_cert_path: Option<String>,
    pub client_cert_path: Option<String>,
    pub client_key_path: Option<String>,
}

/// NATS 客户端连接选项。
#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub url: String,
    pub auth: NatsAuth,
    pub tls: Option<NatsTlsConfig>,
    pub connect_timeout: Duration,
    pub auto_reconnect: bool,
    pub max_reconnects: Option<usize>,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            url: "nats://localhost:4222".to_string(),
            auth: NatsAuth::default(),
            tls: None,
            connect_timeout: Duration::from_secs(5),
            auto_reconnect: true,
            max_reconnects: None,
        }
    }
}

/// NATS 客户端。
#[derive(Clone)]
pub struct Client {
    inner: Arc<async_nats::Client>,
}

impl Client {
    /// 从选项创建并连接到 NATS 服务器。
    pub async fn connect(options: ClientOptions) -> Result<Self, PagodaError> {
        let mut connect_opts = async_nats::ConnectOptions::new()
            .connection_timeout(options.connect_timeout)
            .name("pagoda-runtime");

        connect_opts = match options.auto_reconnect {
            true => match options.max_reconnects {
                Some(n) => connect_opts.max_reconnects(n),
                None => connect_opts,
            },
            false => connect_opts.max_reconnects(0),
        };

        connect_opts = match options.auth {
            NatsAuth::None => connect_opts,
            NatsAuth::Token(token) => connect_opts.token(token),
            NatsAuth::UserPassword { username, password } => {
                connect_opts.user_and_password(username, password)
            }
        };

        let client = connect_opts
            .connect(&options.url)
            .await
            .map_err(|e| PagodaError::cannot_connect(format!("NATS connect failed: {e}")))?;

        Ok(Self { inner: Arc::new(client) })
    }

    /// 从环境变量 `PGD_NATS_SERVER` 创建客户端。
    pub async fn from_env() -> Result<Self, PagodaError> {
        let url = std::env::var(crate::config::environment_names::PGD_NATS_SERVER)
            .unwrap_or_else(|_| "nats://localhost:4222".to_string());
        Self::connect(ClientOptions { url, ..Default::default() }).await
    }

    /// 获取底层 async_nats::Client 引用。
    pub fn inner(&self) -> &async_nats::Client {
        &self.inner
    }

    /// 发布消息到指定 subject。
    pub async fn publish(&self, subject: &str, payload: &[u8]) -> Result<(), PagodaError> {
        self.inner
            .publish(subject.to_string(), payload.to_vec().into())
            .await
            .map_err(|e| PagodaError::cannot_connect(format!("NATS publish: {e}")))?;
        self.inner
            .flush()
            .await
            .map_err(|e| PagodaError::cannot_connect(format!("NATS flush: {e}")))?;
        Ok(())
    }

    /// 订阅指定 subject，返回订阅句柄。
    pub async fn subscribe(&self, subject: &str) -> Result<Subscription, PagodaError> {
        let sub = self
            .inner
            .subscribe(subject.to_string())
            .await
            .map_err(|e| PagodaError::cannot_connect(format!("NATS subscribe: {e}")))?;
        Ok(Subscription { inner: sub, subject: subject.to_string() })
    }

    /// 发送 request-reply 请求。
    pub async fn request(
        &self,
        subject: &str,
        payload: &[u8],
        timeout: Duration,
    ) -> Result<Message, PagodaError> {
        let resp = tokio::time::timeout(
            timeout,
            self.inner.request(subject.to_string(), payload.to_vec().into()),
        )
        .await
        .map_err(|_| PagodaError::unknown(format!("NATS request timeout: {subject}")))?
        .map_err(|e| PagodaError::cannot_connect(format!("NATS request: {e}")))?;

        Ok(Message {
            subject: resp.subject.to_string(),
            payload: resp.payload.to_vec(),
            reply: resp.reply.map(|r| r.to_string()),
        })
    }

    /// 关闭连接。
    pub async fn close(self) -> Result<(), PagodaError> {
        self.inner
            .flush()
            .await
            .map_err(|e| PagodaError::cannot_connect(format!("NATS flush on close: {e}")))?;
        Ok(())
    }
}

/// NATS 订阅句柄。
pub struct Subscription {
    inner: async_nats::Subscriber,
    subject: String,
}

impl Subscription {
    /// 接收下一条消息。
    pub async fn next_message(&mut self) -> Option<Message> {
        use futures::StreamExt;
        self.inner.next().await.map(|msg| Message {
            subject: msg.subject.to_string(),
            payload: msg.payload.to_vec(),
            reply: msg.reply.map(|r| r.to_string()),
        })
    }

    /// 取消订阅。
    pub async fn unsubscribe(mut self) -> Result<(), PagodaError> {
        self.inner
            .unsubscribe()
            .await
            .map_err(|e| PagodaError::cannot_connect(format!("NATS unsubscribe {}: {e}", self.subject)))
    }
}

/// NATS 消息。
#[derive(Debug, Clone)]
pub struct Message {
    pub subject: String,
    pub payload: Vec<u8>,
    pub reply: Option<String>,
}
