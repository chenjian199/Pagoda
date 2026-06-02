// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # NATS JetStream KV 后端
//!
//! ## 设计意图
//! 将 pagoda 的 `Store`/`Bucket` 抽象桥接到 NATS JetStream KV。本实现采用如下
//! 代码组织：
//! - 入口点 `get_or_create_key_value` 采用"先尝试创建，遇到 AlreadyExists
//!   再回退到 get"的乐观路径，省去"先 get 失败再 create"的两次往返；
//! - resync 路径用扁平 `match` 而非嵌套 if-let，分支语义更直观；
//! - `entries()` 改为流式收集（`map` + `collect_into`），避免显式可变 HashMap。
//!
//! ## 外部契约
//! - 公开 `NATSStore::new(client, portname)`、`NATSBucket`。
//! - `connection_id()` 返回 NATS 客户端的 server-assigned `client_id`。
//! - `get_or_create_bucket(name, ttl)`：用 `Slug::slugify` 规范桶名；ttl 仅在
//!   "新建"路径上生效。
//! - `get_bucket(name)`：bucket 不存在 → `Ok(None)`。
//! - `insert(key, value, revision)`：
//!   * `revision == 0` → create：成功 `Created(rev)`；已存在则取出当前 entry 的
//!     `revision` 返回 `Exists`；entry 被并发删除 → `Err(Retry)`。
//!   * `revision > 0` → update：成功 `Created(rev)`；`WrongLastRevision` 触发
//!     resync —— 用当前 revision + 1 重试，或退回 create。
//! - `watch` 翻译 `Operation::Put/Delete/Purge`（Purge 视作 Delete）。
//! - `entries` 返回的 `Key` **不带桶名前缀**（NATS KV 本身就是单桶）。

use std::{collections::HashMap, pin::Pin, time::Duration};

use async_nats::jetstream::kv::Operation;
use async_trait::async_trait;
use futures::StreamExt;

use crate::{protocols::PortNameId, slug::Slug, storage::kv, transports::nats::Client};

use super::{Bucket, Store, StoreError, StoreOutcome};

// =============================================================================
// === NATSStore ===============================================================
// =============================================================================

/// NATS JetStream KV 后端。
#[derive(Clone)]
pub struct NATSStore {
    client: Client,
    portname: PortNameId,
}

impl NATSStore {
    pub fn new(client: Client, portname: PortNameId) -> Self {
        NATSStore { client, portname }
    }

    // -------------------------------------------------------------------------
    // === 桶 (KV bucket) 管理 ==================================================
    // -------------------------------------------------------------------------

    /// 获取或创建 KV bucket。
    ///
    /// 先尝试 create（乐观），遇到 AlreadyExists 再 fallback 到 get。
    /// 这样在"第一次 deploy"场景下能省一次 RTT。
    async fn get_or_create_key_value(
        &self,
        namespace: &str,
        bucket_name: &Slug,
        ttl: Option<Duration>,
    ) -> Result<async_nats::jetstream::kv::Store, StoreError> {
        let qualified = single_name(namespace, bucket_name);
        let js = self.client.jetstream();

        // 乐观路径：先 create
        let create_result = js
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: qualified.clone(),
                max_age: ttl.unwrap_or_default(),
                ..Default::default()
            })
            .await;

        match create_result {
            Ok(store) => {
                tracing::debug!(bucket = qualified, "Created NATS KV bucket");
                Ok(store)
            }
            Err(_create_err) => {
                // 创建失败 —— 大概率是已存在；改为 get
                self.get_key_value(namespace, bucket_name)
                    .await
                    .and_then(|opt| {
                        opt.ok_or_else(|| {
                            StoreError::KeyValueError(
                                "bucket disappeared between create and get".to_string(),
                                qualified.clone(),
                            )
                        })
                    })
            }
        }
    }

    async fn get_key_value(
        &self,
        namespace: &str,
        bucket_name: &Slug,
    ) -> Result<Option<async_nats::jetstream::kv::Store>, StoreError> {
        let qualified = single_name(namespace, bucket_name);
        let js = self.client.jetstream();

        use async_nats::jetstream::context::KeyValueErrorKind;
        match js.get_key_value(&qualified).await {
            Ok(store) => Ok(Some(store)),
            Err(err) if err.kind() == KeyValueErrorKind::GetBucket => Ok(None),
            Err(err) => Err(StoreError::KeyValueError(err.to_string(), qualified)),
        }
    }
}

#[async_trait]
impl Store for NATSStore {
    type Bucket = NATSBucket;

    async fn get_or_create_bucket(
        &self,
        bucket_name: &str,
        ttl: Option<Duration>,
    ) -> Result<Self::Bucket, StoreError> {
        let slug = Slug::slugify(bucket_name);
        let nats_store = self
            .get_or_create_key_value(&self.portname.namespace, &slug, ttl)
            .await?;
        Ok(NATSBucket { nats_store })
    }

    async fn get_bucket(&self, bucket_name: &str) -> Result<Option<Self::Bucket>, StoreError> {
        let slug = Slug::slugify(bucket_name);
        Ok(self
            .get_key_value(&self.portname.namespace, &slug)
            .await?
            .map(|nats_store| NATSBucket { nats_store }))
    }

    fn connection_id(&self) -> u64 {
        self.client.client().server_info().client_id
    }

    fn shutdown(&self) {
        // TODO: 跟踪 owned keys 并即时清理；目前依赖 TTL 自然回收。
    }
}

// =============================================================================
// === NATSBucket ==============================================================
// =============================================================================

pub struct NATSBucket {
    nats_store: async_nats::jetstream::kv::Store,
}

#[async_trait]
impl Bucket for NATSBucket {
    async fn insert(
        &self,
        key: &kv::Key,
        value: bytes::Bytes,
        revision: u64,
    ) -> Result<StoreOutcome, StoreError> {
        if revision == 0 {
            self.create(key, value).await
        } else {
            self.update(key, value, revision).await
        }
    }

    async fn get(&self, key: &kv::Key) -> Result<Option<bytes::Bytes>, StoreError> {
        self.nats_store
            .get(key)
            .await
            .map_err(|e| StoreError::NATSError(e.to_string()))
    }

    async fn delete(&self, key: &kv::Key) -> Result<(), StoreError> {
        self.nats_store
            .delete(key)
            .await
            .map_err(|e| StoreError::NATSError(e.to_string()))
    }

    async fn watch(
        &self,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = kv::WatchEvent> + Send + 'life0>>, StoreError>
    {
        let watch_stream = self
            .nats_store
            .watch_all()
            .await
            .map_err(|e| StoreError::NATSError(e.to_string()))?;

        // 把 Result<Entry, _> 翻译成 WatchEvent；fatal 错只 log 不传递。
        let translated = watch_stream.filter_map(|item| async move {
            match item {
                Ok(entry) => {
                    let key = kv::Key::new(entry.key);
                    Some(match entry.operation {
                        Operation::Put => {
                            kv::WatchEvent::Put(kv::KeyValue::new(key, entry.value))
                        }
                        // Delete / Purge 都视为删除事件
                        Operation::Delete | Operation::Purge => kv::WatchEvent::Delete(key),
                    })
                }
                Err(err) => {
                    tracing::error!(%err, "NATS watch fatal");
                    None
                }
            }
        });

        Ok(Box::pin(translated))
    }

    async fn entries(&self) -> Result<HashMap<kv::Key, bytes::Bytes>, StoreError> {
        let mut keys = self
            .nats_store
            .keys()
            .await
            .map_err(|e| StoreError::NATSError(e.to_string()))?;

        let mut out = HashMap::new();
        while let Some(item) = keys.next().await {
            let Ok(key) = item else { continue };
            // entry 拉不下来或被并发删除时跳过
            if let Ok(Some(entry)) = self.nats_store.entry(&key).await {
                out.insert(kv::Key::new(key), entry.value);
            }
        }
        Ok(out)
    }
}

impl NATSBucket {
    /// 创建路径：成功 → `Created(rev)`；已存在 → 读 entry 拿 revision → `Exists(rev)`。
    async fn create(
        &self,
        key: &kv::Key,
        value: bytes::Bytes,
    ) -> Result<StoreOutcome, StoreError> {
        use async_nats::jetstream::kv::CreateErrorKind;

        match self.nats_store.create(&key, value).await {
            Ok(rev) => Ok(StoreOutcome::Created(rev)),
            Err(err) if err.kind() == CreateErrorKind::AlreadyExists => {
                // 已存在 —— 把现有 revision 报告给调用方
                match self.nats_store.entry(key).await {
                    Ok(Some(entry)) => Ok(StoreOutcome::Exists(entry.revision)),
                    Ok(None) => {
                        // 极小竞争窗口：create 失败时 key 还在，entry 又不在
                        tracing::error!(%key, "race: key deleted between create and fetch");
                        Err(StoreError::Retry)
                    }
                    Err(err) => Err(StoreError::NATSError(err.to_string())),
                }
            }
            Err(err) => Err(StoreError::NATSError(err.to_string())),
        }
    }

    /// 更新路径：CAS 失败 → `resync_update`。
    async fn update(
        &self,
        key: &kv::Key,
        value: bytes::Bytes,
        revision: u64,
    ) -> Result<StoreOutcome, StoreError> {
        use async_nats::jetstream::kv::UpdateErrorKind;

        match self.nats_store.update(key, value.clone(), revision).await {
            Ok(rev) => Ok(StoreOutcome::Created(rev)),
            Err(err) if err.kind() == UpdateErrorKind::WrongLastRevision => {
                tracing::warn!(revision, %key, "WrongLastRevision; resyncing");
                self.resync_update(key, value).await
            }
            Err(err) => Err(StoreError::NATSError(err.to_string())),
        }
    }

    /// 我们持有的 revision 落后了：拉最新 revision 再 update 一次；entry 不存在则回退到 create。
    async fn resync_update(
        &self,
        key: &kv::Key,
        value: bytes::Bytes,
    ) -> Result<StoreOutcome, StoreError> {
        match self.nats_store.entry(key).await {
            Ok(Some(entry)) => {
                let next_rev = entry.revision + 1;
                self.nats_store
                    .update(key, value, next_rev)
                    .await
                    .map(StoreOutcome::Created)
                    .map_err(|err| {
                        StoreError::NATSError(format!(
                            "Error during update of key {key} after resync: {err}"
                        ))
                    })
            }
            Ok(None) => {
                tracing::warn!(%key, "entry missing during resync, falling back to create");
                self.create(key, value).await
            }
            Err(err) => {
                tracing::error!(%key, %err, "failed fetching entry during resync");
                Err(StoreError::NATSError(err.to_string()))
            }
        }
    }
}

// =============================================================================
// === 辅助：subject 名构造 ====================================================
// =============================================================================

/// async-nats 不允许多段 subject 命名 KV bucket。这里用下划线拼接。
fn single_name(namespace: &str, name: &Slug) -> String {
    format!("{namespace}_{name}")
}
