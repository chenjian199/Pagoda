// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! etcd 租约管理。

use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::PagodaError;

pub type LeaseId = i64;

#[derive(Debug, Clone)]
pub struct LeaseConfig {
    pub ttl_secs: i64,
    pub keep_alive_interval: Duration,
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            ttl_secs: 10,
            keep_alive_interval: Duration::from_secs(3),
        }
    }
}

/// etcd 租约句柄（RAII: Drop 时取消 keep-alive）。
pub struct Lease {
    id: LeaseId,
    _keep_alive_handle: JoinHandle<()>,
    cancel: CancellationToken,
    client: etcd_client::Client,
}

impl Lease {
    /// 申请新租约并启动后台 keep-alive。
    pub async fn grant(
        client: &super::connector::EtcdClient,
        config: LeaseConfig,
    ) -> Result<Self, PagodaError> {
        let mut raw = client.raw();
        let resp = raw.lease_grant(config.ttl_secs, None).await
            .map_err(|e| PagodaError::cannot_connect(format!("etcd lease grant: {e}")))?;
        let id = resp.id();

        // 启动 keep-alive 后台任务
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let mut ka_client = client.raw();
        let interval = config.keep_alive_interval;

        let handle = tokio::spawn(async move {
            let (mut keeper, mut stream) = match ka_client.lease_keep_alive(id).await {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!("etcd keep-alive init failed for lease {id}: {e}");
                    return;
                }
            };
            loop {
                tokio::select! {
                    _ = cancel_clone.cancelled() => break,
                    _ = tokio::time::sleep(interval) => {
                        if let Err(e) = keeper.keep_alive().await {
                            tracing::warn!("etcd keep-alive send failed: {e}");
                            break;
                        }
                        // drain response
                        if let Ok(Some(_)) = stream.message().await {}
                    }
                }
            }
        });

        Ok(Self {
            id,
            _keep_alive_handle: handle,
            cancel,
            client: client.raw(),
        })
    }

    pub fn id(&self) -> LeaseId {
        self.id
    }

    pub async fn revoke(mut self) -> Result<(), PagodaError> {
        self.cancel.cancel();
        self.client.lease_revoke(self.id).await
            .map_err(|e| PagodaError::cannot_connect(format!("etcd lease revoke: {e}")))?;
        Ok(())
    }

    pub async fn is_alive(&self) -> Result<bool, PagodaError> {
        let mut c = self.client.clone();
        let resp = c.lease_time_to_live(self.id, None).await
            .map_err(|e| PagodaError::cannot_connect(format!("etcd lease ttl: {e}")))?;
        Ok(resp.ttl() > 0)
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
