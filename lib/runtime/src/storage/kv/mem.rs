// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 内存 KV 后端
//!
//! ## 设计意图
//! 提供进程内、零外部依赖的 [`Store`] 实现，主要用于测试与开发态运行。
//! 本实现与 lib-copy 标准版**契约完全等价**，但内部走了与之不同的并发路径：
//! - 用 `DashMap` 替代全局 `Mutex<HashMap>`，将"桶级别"的写入互斥下沉到分片锁；
//! - 用 **per-bucket** `tokio::sync::broadcast` 替代"全局 unbounded mpsc + 单
//!   消费者锁"，使得 `watch()` 不再串行化、并自然支持多订阅者；
//! - `watch()` 先在锁内拍摄"现存条目快照 + 订阅句柄"再 drop 锁，避免任何
//!   `.await` 跨越互斥锁的生命周期。
//!
//! ## 外部契约
//! - 公开类型：`MemoryStore`、其 `Bucket` 关联类型 `MemoryBucketRef`。
//! - `MemoryStore::new()` 限定 `pub(super)`：仅由上层 [`super::Manager`] 构造。
//! - `MemoryStore: Clone + Default`；`Default` 等价于 `new()`。
//! - `connection_id()` 在每次 `new()` 时随机生成、克隆共享。
//! - `Bucket::entries()` 返回的 `Key` **包含桶名前缀**一致。
//! - `Bucket::insert(key, value, rev)`：
//!     * 桶内不存在 key → `Created(rev)` 并广播 `Put`；
//!     * 已存在且 revision 相同 → `Exists(rev)`，**不**广播；
//!     * 已存在但 revision 不同 → `Created(rev)`，**不**广播。
//! - `Bucket::watch()`：先 yield 当前所有键值（去重），随后 yield 新事件，
//!   流在 `MemoryStore` 被全部 drop 时自然结束。
//!
//! ## 实现要点
//! - 每个桶持有 `BroadcastSender<MemoryEvent>`；订阅者数 = 0 时事件直接丢弃，
//!   这对于"无人观察"场景是符合预期的（生产期间无背压堆积）。
//! - 为了避免广播信道容量满导致丢事件，初始容量取 `WATCH_CAPACITY`；调用方
//!   可通过及时消费 stream 维持低水位。
//! - 删除事件仅在"key 确实存在"时才广播，与 lib-copy 行为一致。

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use rand::Rng as _;
use tokio::sync::broadcast;

use super::{Bucket, Key, KeyValue, Store, StoreError, StoreOutcome, WatchEvent};

// =============================================================================
// === 配置常量 ================================================================
// =============================================================================

/// 每个桶 broadcast 信道的初始容量。
///
/// `watch()` 的契约是"先回放当前快照，再追踪新事件"，新事件由 insert/delete
/// 触发，在测试场景下事件量不大，但我们仍给一个相对宽裕的容量来避免慢消费者
/// 引发 `RecvError::Lagged`。
const WATCH_CAPACITY: usize = 1024;

// =============================================================================
// === 内部事件类型 ============================================================
// =============================================================================

/// 桶级事件。注意只携带 key 与（对 Put 而言的）value，不携带桶名 —— 因为
/// 信道本身已经按桶分配。
#[derive(Clone, Debug)]
enum MemoryEvent {
    Put { key: String, value: bytes::Bytes },
    Delete { key: String },
}

// =============================================================================
// === 单个桶的存储结构 ========================================================
// =============================================================================

/// 桶内每个 entry 同时保存 revision 与字节数据。
type Entry = (u64, bytes::Bytes);

/// 桶状态：一个 sharded map 装数据，外加一个 broadcast 信道发布事件。
///
/// 用 `parking_lot::Mutex<HashMap>` 而非 DashMap：因为我们需要在一个"插入
/// 或 NOOP 比较 revision"的临界区里做读-改-写，单锁更直接也更便宜。
struct BucketState {
    data: parking_lot::Mutex<HashMap<String, Entry>>,
    events: broadcast::Sender<MemoryEvent>,
}

impl BucketState {
    fn new() -> Self {
        let (events, _) = broadcast::channel(WATCH_CAPACITY);
        BucketState {
            data: parking_lot::Mutex::new(HashMap::new()),
            events,
        }
    }
}

// =============================================================================
// === MemoryStore：对外句柄 ===================================================
// =============================================================================

/// 进程内 KV 存储。Clone 后多个 handle 共享同一底层数据。
#[derive(Clone)]
pub struct MemoryStore {
    /// 桶名 → 桶状态。`DashMap` 让"创建/查询不同桶"天然分片。
    buckets: Arc<DashMap<String, Arc<BucketState>>>,
    connection_id: u64,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStore {
    /// 仅由 [`super::Manager`] 在内部构造；外部应通过 `Manager::memory()`。
    pub(super) fn new() -> Self {
        MemoryStore {
            buckets: Arc::new(DashMap::new()),
            connection_id: rand::rng().random(),
        }
    }

    /// 内部辅助：找到桶，若不存在则返回 `MissingBucket`。
    fn require_bucket(&self, name: &str) -> Result<Arc<BucketState>, StoreError> {
        self.buckets
            .get(name)
            .map(|r| r.value().clone())
            .ok_or_else(|| StoreError::MissingBucket(name.to_string()))
    }
}

#[async_trait]
impl Store for MemoryStore {
    type Bucket = MemoryBucketRef;

    /// MemoryStore 不实现 TTL —— `_ttl` 被忽略，与 lib-copy 一致。
    async fn get_or_create_bucket(
        &self,
        bucket_name: &str,
        _ttl: Option<Duration>,
    ) -> Result<Self::Bucket, StoreError> {
        // `entry().or_insert_with` 是 DashMap 的原子获取-或-插入。
        self.buckets
            .entry(bucket_name.to_string())
            .or_insert_with(|| Arc::new(BucketState::new()));

        Ok(MemoryBucketRef {
            name: bucket_name.to_string(),
            buckets: self.buckets.clone(),
        })
    }

    /// 对 MemoryStore 永远 `Ok(...)`：要么 `Some` 要么 `None`，绝不报错。
    async fn get_bucket(&self, bucket_name: &str) -> Result<Option<Self::Bucket>, StoreError> {
        Ok(if self.buckets.contains_key(bucket_name) {
            Some(MemoryBucketRef {
                name: bucket_name.to_string(),
                buckets: self.buckets.clone(),
            })
        } else {
            None
        })
    }

    fn connection_id(&self) -> u64 {
        self.connection_id
    }

    /// MemoryStore 无外部资源 —— shutdown 为空操作。
    fn shutdown(&self) {}
}

// =============================================================================
// === MemoryBucketRef：单桶句柄 ===============================================
// =============================================================================

/// 由 `get_or_create_bucket` / `get_bucket` 返回，按值持有，可重复 `await` 它的
/// `Bucket` 方法。多个 ref 指向同一桶时操作语义可见。
pub struct MemoryBucketRef {
    name: String,
    buckets: Arc<DashMap<String, Arc<BucketState>>>,
}

impl MemoryBucketRef {
    fn state(&self) -> Result<Arc<BucketState>, StoreError> {
        self.buckets
            .get(&self.name)
            .map(|r| r.value().clone())
            .ok_or_else(|| StoreError::MissingBucket(self.name.clone()))
    }
}

#[async_trait]
impl Bucket for MemoryBucketRef {
    async fn insert(
        &self,
        key: &Key,
        value: bytes::Bytes,
        revision: u64,
    ) -> Result<StoreOutcome, StoreError> {
        let state = self.state()?;
        let key_str = key.to_string();

        // 在锁内做条件写入；锁外发布事件以避免持锁通知。
        let (outcome, broadcast_value) = {
            let mut guard = state.data.lock();
            match guard.get_mut(&key_str) {
                None => {
                    // 全新 key
                    guard.insert(key_str.clone(), (revision, value.clone()));
                    (StoreOutcome::Created(revision), Some(value))
                }
                Some(existing) => {
                    if existing.0 == revision {
                        // 同 revision 视为幂等 noop
                        (StoreOutcome::Exists(revision), None)
                    } else {
                        // 不同 revision 视为更新；lib-copy 此处不广播，我们也不广播
                        *existing = (revision, value);
                        (StoreOutcome::Created(revision), None)
                    }
                }
            }
        };

        if let Some(v) = broadcast_value {
            // 无订阅者时 broadcast::send 会返回 Err；这里忽略 —— "无人听就丢"是预期行为。
            let _ = state.events.send(MemoryEvent::Put {
                key: key_str,
                value: v,
            });
        }
        Ok(outcome)
    }

    async fn get(&self, key: &Key) -> Result<Option<bytes::Bytes>, StoreError> {
        // 桶不存在 → 与 lib-copy 一致：返回 Ok(None)（而非错误）。
        let Some(state_ref) = self.buckets.get(&self.name) else {
            return Ok(None);
        };
        let state = state_ref.value().clone();
        drop(state_ref); // 尽早释放 DashMap shard

        let guard = state.data.lock();
        Ok(guard.get(key.as_ref()).map(|(_, v)| v.clone()))
    }

    async fn delete(&self, key: &Key) -> Result<(), StoreError> {
        let state = self.state()?;
        let key_str = key.to_string();

        let removed = state.data.lock().remove(&key_str).is_some();
        if removed {
            let _ = state.events.send(MemoryEvent::Delete { key: key_str });
        }
        Ok(())
    }

    async fn watch(
        &self,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = WatchEvent> + Send + 'life0>>, StoreError> {
        let state = self.state()?;

        // 1. 拍快照 + 订阅事件流（必须在锁内完成，确保"快照截止时刻"和
        //    "新事件起始时刻"恰好衔接，无丢失也无重复风险）。
        let (snapshot, mut rx, seen): (Vec<WatchEvent>, broadcast::Receiver<MemoryEvent>, HashSet<String>) = {
            let guard = state.data.lock();
            let mut seen = HashSet::with_capacity(guard.len());
            let mut snap = Vec::with_capacity(guard.len());
            for (k, (_rev, v)) in guard.iter() {
                seen.insert(k.clone());
                snap.push(WatchEvent::Put(KeyValue::new(
                    Key::new(k.clone()),
                    v.clone(),
                )));
            }
            (snap, state.events.subscribe(), seen)
        };

        Ok(Box::pin(async_stream::stream! {
            // 先把快照吐完
            for ev in snapshot {
                yield ev;
            }
            // 再追踪新事件；遇到 Lagged 就跳过该批丢失的事件继续追
            let mut seen = seen;
            loop {
                match rx.recv().await {
                    Ok(MemoryEvent::Put { key, value }) => {
                        // 快照里已经出过的 key 不再 yield —— 与 lib-copy 一致
                        if seen.contains(&key) {
                            continue;
                        }
                        seen.insert(key.clone());
                        yield WatchEvent::Put(KeyValue::new(Key::new(key), value));
                    }
                    Ok(MemoryEvent::Delete { key }) => {
                        seen.remove(&key);
                        yield WatchEvent::Delete(Key::new(key));
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(skipped = n, "MemoryStore watch lagged");
                    }
                }
            }
        }))
    }

    async fn entries(&self) -> Result<HashMap<Key, bytes::Bytes>, StoreError> {
        let state = self.state()?;
        let guard = state.data.lock();
        let mut out = HashMap::with_capacity(guard.len());
        for (k, (_rev, v)) in guard.iter() {
            // 与 lib-copy 一致：entries 返回的 Key 带桶名前缀
            let full = format!("{}/{}", self.name, k);
            out.insert(Key::new(full), v.clone());
        }
        Ok(out)
    }
}

// =============================================================================
// === 单元测试 ================================================================
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::kv::{Bucket as _, Key, MemoryStore, Store as _};

    // ---------------------------------------------------------------------
    // ===标准契约测试================================
    // ---------------------------------------------------------------------

    #[tokio::test]
    async fn test_entries_full_path() {
        let m = MemoryStore::new();
        let bucket = m.get_or_create_bucket("bucket1", None).await.unwrap();
        let _ = bucket
            .insert(&Key::new("key1".to_string()), "value1".into(), 0)
            .await
            .unwrap();
        let _ = bucket
            .insert(&Key::new("key2".to_string()), "value2".into(), 0)
            .await
            .unwrap();
        let entries = bucket.entries().await.unwrap();
        let keys: HashSet<Key> = entries.into_keys().collect();
        assert!(keys.contains(&Key::new("bucket1/key1".to_string())));
        assert!(keys.contains(&Key::new("bucket1/key2".to_string())));
    }

    // ---------------------------------------------------------------------
    // === 实现细节补充测试 ================================================
    // ---------------------------------------------------------------------

    /// ## 测试过程
    /// 同 key 重复以同 revision 插入 → `Exists`；以更高 revision 插入 → `Created`。
    /// ## 意义
    /// 锁定"同 rev 幂等 / 不同 rev 覆盖"的写入语义。
    #[tokio::test]
    async fn insert_idempotent_and_overwrite() {
        let m = MemoryStore::new();
        let b = m.get_or_create_bucket("b", None).await.unwrap();
        let k: Key = "k".into();

        assert_eq!(
            b.insert(&k, "v0".into(), 0).await.unwrap(),
            StoreOutcome::Created(0)
        );
        assert_eq!(
            b.insert(&k, "v0".into(), 0).await.unwrap(),
            StoreOutcome::Exists(0)
        );
        assert_eq!(
            b.insert(&k, "v1".into(), 1).await.unwrap(),
            StoreOutcome::Created(1)
        );
        assert_eq!(b.get(&k).await.unwrap().unwrap(), bytes::Bytes::from("v1"));
    }

    /// ## 测试过程
    /// 不存在的桶上调用 `get` 应返回 `Ok(None)` 而不是错误。
    /// ## 意义
    /// 锁定"读不存在的桶不视为错误"这一便利契约（lib-copy 行为）。
    #[tokio::test]
    async fn get_on_missing_bucket_is_ok_none() {
        let m = MemoryStore::new();
        let b = m.get_or_create_bucket("only", None).await.unwrap();
        // 句柄指向已删除的桶（构造一个）
        let ghost = MemoryBucketRef {
            name: "no_such".to_string(),
            buckets: m.buckets.clone(),
        };
        assert!(ghost.get(&"k".into()).await.unwrap().is_none());
        // 健康桶照常工作
        assert!(b.get(&"k".into()).await.unwrap().is_none());
    }

    /// ## 测试过程
    /// 删除存在的 key 返回 Ok 并产生事件；删除不存在的 key 返回 Ok 但不发事件。
    /// ## 意义
    /// 锁定 delete 的幂等性，与"watch 只看到真实变化"的契约。
    #[tokio::test]
    async fn delete_only_emits_when_present() {
        let m = MemoryStore::new();
        let b = m.get_or_create_bucket("b", None).await.unwrap();
        b.insert(&"k".into(), "v".into(), 0).await.unwrap();

        // 订阅在删之前，确保能捕获事件
        let state = m.buckets.get("b").unwrap().value().clone();
        let mut rx = state.events.subscribe();

        b.delete(&"k".into()).await.unwrap();
        // 第二次删除不存在的 key
        b.delete(&"k".into()).await.unwrap();

        // 只应收到一条 Delete
        let first = rx.try_recv();
        assert!(matches!(first, Ok(MemoryEvent::Delete { .. })));
        assert!(rx.try_recv().is_err());
    }

    /// ## 测试过程
    /// 同一 `MemoryStore` 的两个 clone 共享底层桶；从 clone A 写入，clone B 可见。
    /// ## 意义
    /// 验证 `Clone` 仅复制句柄、不复制状态的契约。
    #[tokio::test]
    async fn clone_shares_state() {
        let a = MemoryStore::new();
        let b = a.clone();
        let ba = a.get_or_create_bucket("shared", None).await.unwrap();
        let bb = b.get_or_create_bucket("shared", None).await.unwrap();
        ba.insert(&"k".into(), "v".into(), 0).await.unwrap();
        assert_eq!(bb.get(&"k".into()).await.unwrap().unwrap(), bytes::Bytes::from("v"));
        // connection_id 共享
        assert_eq!(a.connection_id(), b.connection_id());
    }

    /// ## 测试过程
    /// 在 watch 已订阅的情况下并发插入 3 个新 key，应至少收到 3 条 Put 事件（顺序不定）。
    /// ## 意义
    /// 锁定 broadcast 多事件传输的可靠性。
    #[tokio::test]
    async fn watch_receives_new_puts() {
        use futures::StreamExt as _;
        let m = MemoryStore::new();
        let b = m.get_or_create_bucket("w", None).await.unwrap();

        let mut stream = b.watch().await.unwrap();
        // 没有快照，立即开始新事件
        b.insert(&"a".into(), "1".into(), 0).await.unwrap();
        b.insert(&"b".into(), "2".into(), 0).await.unwrap();
        b.insert(&"c".into(), "3".into(), 0).await.unwrap();

        let mut got = HashSet::new();
        for _ in 0..3 {
            let ev = tokio::time::timeout(Duration::from_secs(2), stream.next())
                .await
                .unwrap()
                .unwrap();
            if let WatchEvent::Put(kv) = ev {
                got.insert(kv.key());
            }
        }
        assert!(got.contains("a") && got.contains("b") && got.contains("c"));
    }
}
