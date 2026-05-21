// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 协议层 service 注册表（NATS 请求平面模式专用）。
//!
//! 维护 servicegroup 名 → NATS micro service 对象的映射，
//! 由 `build_nats_service()` 写入，由 `DistributedRuntime` 持有。

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;

// ─────────────────────── 内部状态 ────────────────────────────────

/// 内部注册表：servicegroup 全限定名 → NATS Service 对象。
#[derive(Default)]
pub struct RegistryInner {
    /// key 格式：`"{namespace}/{servicegroup_name}"`
    pub(crate) services: HashMap<String, async_nats::service::Service>,
}

/// 协议层 service 注册表（Clone 只增加 Arc 引用计数）。
#[derive(Clone, Default)]
pub struct Registry {
    pub(crate) inner: Arc<Mutex<RegistryInner>>,
}

impl Registry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(RegistryInner::default())),
        }
    }

    /// 注册 NATS micro service。
    ///
    /// 若同名 service 已存在则忽略（幂等）。
    pub async fn register(
        &self,
        key: impl Into<String>,
        service: async_nats::service::Service,
    ) {
        let key = key.into();
        let mut inner = self.inner.lock().await;
        if inner.services.contains_key(&key) {
            tracing::debug!(key = %key, "NATS service already registered, ignoring");
            return;
        }
        inner.services.insert(key, service);
    }

    /// 注销并停止 NATS micro service。
    pub async fn unregister(&self, key: &str) {
        let service = {
            let mut inner = self.inner.lock().await;
            inner.services.remove(key)
        };
        if let Some(svc) = service {
            if let Err(e) = svc.stop().await {
                tracing::warn!(key = %key, "Failed to stop NATS service: {e}");
            }
        }
    }

    /// 检查是否已注册指定 key 的 service。
    pub async fn contains(&self, key: &str) -> bool {
        self.inner.lock().await.services.contains_key(key)
    }

    /// 已注册的 service 数量。
    pub async fn len(&self) -> usize {
        self.inner.lock().await.services.len()
    }

    /// 注册表是否为空。
    pub async fn is_empty(&self) -> bool {
        self.inner.lock().await.services.is_empty()
    }

    /// 停止并清空所有 service（优雅关闭时调用）。
    pub async fn shutdown_all(&self) {
        let services: Vec<(String, async_nats::service::Service)> = {
            let mut inner = self.inner.lock().await;
            inner.services.drain().collect()
        };
        for (key, svc) in services {
            if let Err(e) = svc.stop().await {
                tracing::warn!(key = %key, "Failed to stop NATS service during shutdown: {e}");
            }
        }
    }
}
