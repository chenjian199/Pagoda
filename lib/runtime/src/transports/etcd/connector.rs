// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! etcd 连接建立、TLS 配置和重试策略。

use std::time::Duration;

use crate::error::PagodaError;

/// TLS 配置选项。
#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub ca_cert_path: Option<String>,
    pub client_cert_path: Option<String>,
    pub client_key_path: Option<String>,
}

/// 重试策略配置。
#[derive(Debug, Clone)]
pub struct RetryStrategy {
    pub max_retries: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub multiplier: f64,
}

impl Default for RetryStrategy {
    fn default() -> Self {
        Self {
            max_retries: 5,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            multiplier: 2.0,
        }
    }
}

/// etcd 连接器配置。
#[derive(Debug, Clone)]
pub struct EtcdConnectorConfig {
    pub endpoints: Vec<String>,
    pub tls: Option<TlsConfig>,
    pub connect_timeout: Duration,
    pub retry_strategy: RetryStrategy,
}

impl Default for EtcdConnectorConfig {
    fn default() -> Self {
        Self {
            endpoints: vec!["http://localhost:2379".to_string()],
            tls: None,
            connect_timeout: Duration::from_secs(5),
            retry_strategy: RetryStrategy::default(),
        }
    }
}

/// etcd 连接器，管理到 etcd 集群的连接生命周期。
#[derive(Clone)]
pub struct EtcdConnector {
    config: EtcdConnectorConfig,
}

impl EtcdConnector {
    /// 从配置创建连接器（不立即建立连接）。
    pub fn new(config: EtcdConnectorConfig) -> Self {
        Self { config }
    }

    /// 从环境变量 `PGD_ETCD_ENDPOINTS` 构建连接器。
    pub fn from_env() -> Result<Self, PagodaError> {
        let endpoints_raw = std::env::var("PGD_ETCD_ENDPOINTS")
            .unwrap_or_else(|_| "http://localhost:2379".to_string());
        let endpoints = endpoints_raw
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();
        if endpoints.is_empty() {
            return Err(PagodaError::invalid_argument("PGD_ETCD_ENDPOINTS is empty".to_string()));
        }
        Ok(Self::new(EtcdConnectorConfig { endpoints, ..Default::default() }))
    }

    /// 建立到 etcd 集群的连接，带指数退避重试。
    pub async fn connect(&self) -> Result<EtcdClient, PagodaError> {
        let mut backoff = self.config.retry_strategy.initial_backoff;
        let mut last_err = None;

        for attempt in 0..=self.config.retry_strategy.max_retries {
            let opts = etcd_client::ConnectOptions::new()
                .with_connect_timeout(self.config.connect_timeout);

            match etcd_client::Client::connect(self.config.endpoints.clone(), Some(opts)).await {
                Ok(client) => {
                    tracing::debug!(
                        attempt,
                        endpoints = ?self.config.endpoints,
                        "etcd connected"
                    );
                    return Ok(EtcdClient { inner: client });
                }
                Err(e) => {
                    tracing::warn!(
                        attempt,
                        error = %e,
                        "etcd connect failed, retrying in {:?}",
                        backoff
                    );
                    last_err = Some(e);
                    if attempt < self.config.retry_strategy.max_retries {
                        tokio::time::sleep(backoff).await;
                        backoff = std::cmp::min(
                            Duration::from_secs_f64(
                                backoff.as_secs_f64() * self.config.retry_strategy.multiplier,
                            ),
                            self.config.retry_strategy.max_backoff,
                        );
                    }
                }
            }
        }

        Err(PagodaError::cannot_connect(format!(
            "etcd connect failed after {} attempts: {}",
            self.config.retry_strategy.max_retries + 1,
            last_err.map(|e| e.to_string()).unwrap_or_default()
        )))
    }

    pub fn config(&self) -> &EtcdConnectorConfig {
        &self.config
    }
}

/// 已建立的 etcd 客户端连接句柄。
#[derive(Clone)]
pub struct EtcdClient {
    inner: etcd_client::Client,
}

impl EtcdClient {
    /// 获取底层 etcd_client::Client 的克隆（供 kv/lease/lock 子客户端使用）。
    pub(crate) fn raw(&self) -> etcd_client::Client {
        self.inner.clone()
    }

    /// 获取 KV 操作客户端。
    pub fn kv_client(&self) -> super::kv::KvClient {
        super::kv::KvClient::new(self.inner.clone())
    }

    /// 获取 Lease 操作客户端。
    pub fn lease_client(&self) -> LeaseClient {
        LeaseClient { inner: self.inner.clone() }
    }

    /// 获取 Lock 操作客户端。
    pub fn lock_client(&self) -> LockClient {
        LockClient { inner: self.inner.clone() }
    }
}

/// etcd Lease 子客户端。
#[derive(Clone)]
pub struct LeaseClient {
    inner: etcd_client::Client,
}

/// etcd Lock 子客户端。
#[derive(Clone)]
pub struct LockClient {
    inner: etcd_client::Client,
}
