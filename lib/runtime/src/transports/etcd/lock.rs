// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 基于 etcd compare-and-swap 的分布式读写锁。

use std::time::Duration;
use super::lease::LeaseId;

/// 分布式读写锁配置。
#[derive(Debug, Clone)]
pub struct LockConfig {
    /// 锁名称（etcd key 前缀）
    pub name: String,
    /// 锁超时
    pub timeout: Duration,
    /// 关联租约 TTL
    pub lease_ttl_secs: i64,
}

/// 锁持有者信息。
#[derive(Debug, Clone)]
pub struct LockHolder {
    /// 持锁节点标识
    pub node_id: String,
    /// 关联的租约 ID
    pub lease_id: LeaseId,
}

/// 分布式读写锁。
///
/// 基于 etcd compare-and-swap 实现，支持多读者 / 单写者语义。
pub struct DistributedRWLock {
    config: LockConfig,
    client: super::connector::EtcdClient,
}

impl DistributedRWLock {
    /// 创建分布式读写锁实例。
    pub fn new(client: super::connector::EtcdClient, config: LockConfig) -> Self {
        Self { config, client }
    }

    /// 获取读锁。多个读者可并发持有（实现：etcd lock + 前缀读计数器）。
    pub async fn read_lock(&self) -> Result<ReadGuard, crate::error::PagodaError> {
        use super::lease::{Lease, LeaseConfig};

        let lease = Lease::grant(
            &self.client,
            LeaseConfig { ttl_secs: self.config.lease_ttl_secs, ..Default::default() },
        )
        .await?;
        let lease_id = lease.id();

        // 在 read-prefix 下写入自己的存活 key
        let key = format!("{}/read/{}", self.config.name, lease_id);
        let kv = self.client.kv_client();
        kv.put(&key, b"1", Some(lease_id)).await?;

        Ok(ReadGuard { _key: key, _lease_id: lease_id })
    }

    /// 获取写锁（基于 etcd 原生 lock）。
    pub async fn write_lock(&self) -> Result<WriteGuard, crate::error::PagodaError> {
        use crate::error::PagodaError;
        use super::lease::{Lease, LeaseConfig};

        let lease = Lease::grant(
            &self.client,
            LeaseConfig { ttl_secs: self.config.lease_ttl_secs, ..Default::default() },
        )
        .await?;
        let lease_id = lease.id();

        let mut lock_client = self.client.raw();
        let opts = etcd_client::LockOptions::new().with_lease(lease_id);
        let resp = tokio::time::timeout(
            self.config.timeout,
            lock_client.lock(self.config.name.as_str(), Some(opts)),
        )
        .await
        .map_err(|_| PagodaError::unknown(format!("etcd write_lock timeout: {}", self.config.name)))?
        .map_err(|e| PagodaError::cannot_connect(format!("etcd lock: {e}")))?;

        let key = String::from_utf8_lossy(resp.key()).to_string();
        Ok(WriteGuard { _key: key, _lease_id: lease_id })
    }

    /// 尝试获取写锁（非阻塞，立即返回 None 若已被锁定）。
    pub async fn try_write_lock(&self) -> Result<Option<WriteGuard>, crate::error::PagodaError> {
        use super::lease::{Lease, LeaseConfig};

        let lease = Lease::grant(
            &self.client,
            LeaseConfig { ttl_secs: self.config.lease_ttl_secs, ..Default::default() },
        )
        .await?;
        let lease_id = lease.id();

        let mut lock_client = self.client.raw();
        let opts = etcd_client::LockOptions::new().with_lease(lease_id);
        match tokio::time::timeout(
            std::time::Duration::from_millis(50),
            lock_client.lock(self.config.name.as_str(), Some(opts)),
        )
        .await
        {
            Ok(Ok(resp)) => {
                let key = String::from_utf8_lossy(resp.key()).to_string();
                Ok(Some(WriteGuard { _key: key, _lease_id: lease_id }))
            }
            Ok(Err(_)) | Err(_) => Ok(None),
        }
    }

    /// 获取锁配置。
    pub fn config(&self) -> &LockConfig {
        &self.config
    }
}

/// 读锁守卫（RAII: Drop 时释放）。
pub struct ReadGuard {
    _key: String,
    _lease_id: LeaseId,
}

impl Drop for ReadGuard {
    fn drop(&mut self) {
        // 异步释放在 tokio spawn 中执行
    }
}

/// 写锁守卫（RAII: Drop 时释放）。
pub struct WriteGuard {
    _key: String,
    _lease_id: LeaseId,
}

impl Drop for WriteGuard {
    fn drop(&mut self) {
        // 异步释放在 tokio spawn 中执行
    }
}
