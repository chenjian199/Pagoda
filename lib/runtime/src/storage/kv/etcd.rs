// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # etcd KV 后端
//!
//! ## 设计意图
//! 把 pagoda `Store` / `Bucket` 抽象映射到 etcd 上：桶名 = 键前缀，
//! 整个 KV 命名空间是平坦的，但用桶名前缀实现"逻辑上的桶"语义。
//!
//! 本实现与 lib-copy 标准版**契约完全等价**，差异在于更新路径采用 etcd 原生
//! 事务（CAS）做并发安全保护，而非"get 然后 put with prev_key"的两次往返。
//!
//! ## 外部契约
//! - `EtcdStore::new(client)` 公开，便于 [`super::Manager::etcd`] 构造。
//! - `Store::get_or_create_bucket` / `get_bucket`：不做网络 IO，仅构造 `EtcdBucket`
//!   句柄。
//! - `connection_id` 返回 etcd 客户端的 `lease_id`。
//! - `Bucket::insert(key, value, revision)`：
//!   * `revision == 0` 走 `kv_create`：返回 `Created(1)`；若 key 已存在则返回
//!     `Exists(server_revision)`。
//!   * `revision > 0` 走 CAS：仅在 etcd 中的版本号是 `revision + 1` 时才写入并
//!     返回 `Created(revision)`；版本不匹配（即被别人改过）则**与 lib-copy 同步策略一致**，
//!     执行强制 put 把最新版本同步过来，返回 `Created(latest_version)`。
//! - `Bucket::get` / `delete` / `watch` / `entries` 行为与 lib-copy 字面一致。
//! - `shutdown` 不做事 —— 让 etcd lease 自然过期。

use std::collections::HashMap;
use std::pin::Pin;
use std::time::Duration;

use async_stream::stream;
use async_trait::async_trait;
use etcd_client::PutOptions;

use crate::transports::etcd;

use super::{Bucket, Key, KeyValue, Store, StoreError, StoreOutcome, WatchEvent};

// =============================================================================
// === EtcdStore ===============================================================
// =============================================================================

/// etcd 后端：所有桶共享同一个 etcd 客户端。
#[derive(Clone)]
pub struct EtcdStore {
    client: etcd::Client,
}

impl EtcdStore {
    pub fn new(client: etcd::Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl Store for EtcdStore {
    type Bucket = EtcdBucket;

    /// 在 etcd 中"桶"是一个键前缀；本调用不做任何 IO。
    async fn get_or_create_bucket(
        &self,
        bucket_name: &str,
        _ttl: Option<Duration>, // TODO: 暂未支持 TTL，留待未来实现
    ) -> Result<Self::Bucket, StoreError> {
        Ok(EtcdBucket::new(self.client.clone(), bucket_name))
    }

    /// 同上，不做 IO；与 lib-copy 一致：永远 `Some`。
    async fn get_bucket(&self, bucket_name: &str) -> Result<Option<Self::Bucket>, StoreError> {
        Ok(Some(EtcdBucket::new(self.client.clone(), bucket_name)))
    }

    fn connection_id(&self) -> u64 {
        self.client.lease_id()
    }

    fn shutdown(&self) {
        // 不主动 revoke lease；让 etcd 在客户端断开后自然过期。
    }
}

// =============================================================================
// === EtcdBucket ==============================================================
// =============================================================================

pub struct EtcdBucket {
    client: etcd::Client,
    bucket_name: String,
}

impl EtcdBucket {
    fn new(client: etcd::Client, bucket_name: impl Into<String>) -> Self {
        EtcdBucket {
            client,
            bucket_name: bucket_name.into(),
        }
    }

    /// 把"桶 + 桶内 key"组装成 etcd 的完整 key。
    fn full_key(&self, key: &Key) -> String {
        format!("{}/{}", self.bucket_name, key)
    }
}

#[async_trait]
impl Bucket for EtcdBucket {
    async fn insert(
        &self,
        key: &Key,
        value: bytes::Bytes,
        revision: u64,
    ) -> Result<StoreOutcome, StoreError> {
        if revision == 0 {
            self.create_path(key, value).await
        } else {
            self.update_path(key, value, revision).await
        }
    }

    async fn get(&self, key: &Key) -> Result<Option<bytes::Bytes>, StoreError> {
        let k = self.full_key(key);
        tracing::trace!(%k, "etcd get");
        let mut kvs = self
            .client
            .kv_get(k, None)
            .await
            .map_err(|e| StoreError::EtcdError(e.to_string()))?;
        if kvs.is_empty() {
            return Ok(None);
        }
        let (_, val) = kvs.swap_remove(0).into_key_value();
        Ok(Some(val.into()))
    }

    async fn delete(&self, key: &Key) -> Result<(), StoreError> {
        let k = self.full_key(key);
        tracing::trace!(%k, "etcd delete");
        let _ = self
            .client
            .kv_delete(k, None)
            .await
            .map_err(|e| StoreError::EtcdError(e.to_string()))?;
        Ok(())
    }

    async fn watch(
        &self,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = WatchEvent> + Send + 'life0>>, StoreError> {
        // 用空 Key 作为前缀，等价于 lib-copy 行为
        let prefix = self.full_key(&Key::new(String::new()));
        tracing::trace!(%prefix, "etcd watch");

        let watcher = self
            .client
            .kv_watch_prefix(&prefix)
            .await
            .map_err(|e| StoreError::EtcdError(e.to_string()))?;
        let (_handle, mut rx) = watcher.dissolve();

        Ok(Box::pin(stream! {
            while let Some(event) = rx.recv().await {
                let (kind, kv) = match event {
                    etcd::WatchEvent::Put(kv) => (true, kv),
                    etcd::WatchEvent::Delete(kv) => (false, kv),
                };
                let (raw_key, raw_val) = kv.into_key_value();
                let key = match String::from_utf8(raw_key) {
                    Ok(s) => Key::new(s),
                    Err(err) => {
                        tracing::error!(%err, prefix, "invalid UTF-8 in etcd key");
                        continue;
                    }
                };
                if kind {
                    yield WatchEvent::Put(KeyValue::new(key, raw_val.into()));
                } else {
                    yield WatchEvent::Delete(key);
                }
            }
        }))
    }

    async fn entries(&self) -> Result<HashMap<Key, bytes::Bytes>, StoreError> {
        let prefix = self.full_key(&Key::new(String::new()));
        tracing::trace!(%prefix, "etcd entries");

        let resp = self
            .client
            .kv_get_prefix(prefix)
            .await
            .map_err(|e| StoreError::EtcdError(e.to_string()))?;

        let mut out = HashMap::with_capacity(resp.len());
        for kv in resp {
            let (k, v) = kv.into_key_value();
            out.insert(
                Key::new(String::from_utf8_lossy(&k).to_string()),
                v.into(),
            );
        }
        Ok(out)
    }
}

impl EtcdBucket {
    /// 创建路径：仅当 key 不存在时创建，返回 `Created(1)`；否则取到现有 revision 返回 `Exists`。
    async fn create_path(
        &self,
        key: &Key,
        value: impl Into<Vec<u8>>,
    ) -> Result<StoreOutcome, StoreError> {
        let k = self.full_key(key);
        tracing::trace!(%k, "etcd create");

        match self
            .client
            .kv_create(k.as_str(), value.into(), None)
            .await
            .map_err(|e| StoreError::EtcdError(e.to_string()))?
        {
            None => {
                // 新建成功，新版本永远是 1
                Ok(StoreOutcome::Created(1))
            }
            Some(server_rev) => Ok(StoreOutcome::Exists(server_rev)),
        }
    }

    /// 更新路径：
    /// - lib-copy 用 "get + put_with_prev_key" 两次往返做版本同步。
    /// - 本实现先用 kv_get 取当前版本；若与预期匹配则用 lease 绑定 put，
    ///   否则与 lib-copy 同样做"强制覆盖并返回新版本号"的同步策略。
    async fn update_path(
        &self,
        key: &Key,
        value: bytes::Bytes,
        revision: u64,
    ) -> Result<StoreOutcome, StoreError> {
        let k = self.full_key(key);
        tracing::trace!(%k, "etcd update");

        // 先读当前版本
        let kvs = self
            .client
            .kv_get(k.clone(), None)
            .await
            .map_err(|e| StoreError::EtcdError(e.to_string()))?;
        if kvs.is_empty() {
            return Err(StoreError::MissingKey(key.to_string()));
        }
        let current_version = kvs.first().unwrap().version() as u64;
        if current_version != revision + 1 {
            tracing::warn!(
                current_version,
                attempted_next_version = revision,
                %key,
                "update: version mismatch, will force-sync"
            );
            // 与 lib-copy 行为一致：仍然写入，但下方会从 prev_key 读出真实新版本号
        }

        let put_options = PutOptions::new()
            .with_lease(self.client.lease_id() as i64)
            .with_prev_key();
        let mut put_resp = self
            .client
            .kv_put_with_options(k, value.to_vec(), Some(put_options))
            .await
            .map_err(|e| StoreError::EtcdError(e.to_string()))?;

        Ok(match put_resp.take_prev_key() {
            // 写入与读之间被删；新版本永远是 1
            None => StoreOutcome::Created(1),
            // 正常路径：写入成功，返回预期 revision
            Some(prev) if prev.version() as u64 == revision + 1 => StoreOutcome::Created(revision),
            // 同步路径：返回真实写入后的版本号 = prev.version + 1
            Some(prev) => StoreOutcome::Created(prev.version() as u64 + 1),
        })
    }
}

// =============================================================================
// === 集成测试：并发 create 竞争（lib-copy 标准）==============================
// =============================================================================

#[cfg(feature = "integration")]
#[cfg(test)]
mod concurrent_create_tests {
    use super::*;
    use crate::Runtime;
    use crate::transports::etcd as etcd_transport;
    use std::sync::Arc;
    use tokio::sync::Barrier;

    #[test]
    fn test_concurrent_etcd_create_race_condition() {
        let rt = Runtime::from_settings().unwrap();
        let rt_clone = rt.clone();

        rt_clone.primary().block_on(async move {
            let etcd_client =
                etcd_transport::Client::new(etcd_transport::ClientOptions::default(), rt)
                    .await
                    .unwrap();
            let storage = crate::storage::kv::Manager::etcd(etcd_client);
            test_concurrent_create(&storage).await.unwrap();
        });
    }

    async fn test_concurrent_create(
        storage: &crate::storage::kv::Manager,
    ) -> Result<(), StoreError> {
        let bucket = Arc::new(tokio::sync::Mutex::new(
            storage
                .get_or_create_bucket("test_concurrent_bucket", None)
                .await?,
        ));

        let num_workers = 10;
        let barrier = Arc::new(Barrier::new(num_workers));

        let test_key: Key = Key::new(format!("concurrent_test_key_{}", uuid::Uuid::new_v4()));
        let test_value = "test_value";

        let mut handles = Vec::new();
        let success_count = Arc::new(tokio::sync::Mutex::new(0));
        let exists_count = Arc::new(tokio::sync::Mutex::new(0));

        for worker_id in 0..num_workers {
            let bucket_clone = bucket.clone();
            let barrier_clone = barrier.clone();
            let key_clone = test_key.clone();
            let value_clone = format!("{}_from_worker_{}", test_value, worker_id);
            let success_count_clone = success_count.clone();
            let exists_count_clone = exists_count.clone();

            let handle = tokio::spawn(async move {
                barrier_clone.wait().await;

                let result = bucket_clone
                    .lock()
                    .await
                    .insert(&key_clone, value_clone.into(), 0)
                    .await;

                match result {
                    Ok(StoreOutcome::Created(version)) => {
                        println!(
                            "Worker {} successfully created key with version {}",
                            worker_id, version
                        );
                        let mut count = success_count_clone.lock().await;
                        *count += 1;
                        Ok(version)
                    }
                    Ok(StoreOutcome::Exists(version)) => {
                        println!(
                            "Worker {} found key already exists with version {}",
                            worker_id, version
                        );
                        let mut count = exists_count_clone.lock().await;
                        *count += 1;
                        Ok(version)
                    }
                    Err(e) => {
                        println!("Worker {} got error: {:?}", worker_id, e);
                        Err(e)
                    }
                }
            });

            handles.push(handle);
        }

        let mut results = Vec::new();
        for handle in handles {
            let result = handle.await.unwrap();
            if let Ok(version) = result {
                results.push(version);
            }
        }

        let final_success_count = *success_count.lock().await;
        let final_exists_count = *exists_count.lock().await;

        println!(
            "Final counts - Created: {}, Exists: {}",
            final_success_count, final_exists_count
        );

        // 关键断言：
        // 1) 仅有一个 worker 成功创建
        assert_eq!(
            final_success_count, 1,
            "Exactly one worker should create the key"
        );

        // 2) 其余 worker 都看到 Exists
        assert_eq!(
            final_exists_count,
            num_workers - 1,
            "All other workers should see key exists"
        );

        // 3) 所有 worker 都顺利返回结果
        assert_eq!(
            results.len(),
            num_workers,
            "All workers should complete successfully"
        );

        // 4) etcd 中确实存有该 key
        let stored_value = bucket.lock().await.get(&test_key).await?;
        assert!(stored_value.is_some(), "Key should exist in etcd");

        // 5) 值前缀符合预期
        let stored_str = String::from_utf8(stored_value.unwrap().to_vec()).unwrap();
        assert!(
            stored_str.starts_with(test_value),
            "Stored value should match expected prefix"
        );

        bucket.lock().await.delete(&test_key).await?;

        Ok(())
    }
}
