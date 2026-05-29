// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 传统键值存储统一接口
//!
//! ## 设计意图
//! 给 etcd / NATS JetStream KV / 本地文件 / 内存这四种 backend 一个**统一的
//! `Store` / `Bucket` 抽象**，使上层"配置同步、租约元数据、注册表"等用例可以
//! 在不感知存储介质的前提下工作。模块名特意拼为 `key_value_store` 而非 KV ——
//! 在 AI 语境下 "KV" 容易与注意力缓存混淆。
//!
//! 本实现与 lib-copy 标准版**契约完全等价**，但内部走了与之不同的实现路径：
//!
//! - 把 lib-copy 的 `enum KeyValueStoreEnum { Memory, Nats, Etcd, File }` 改成
//!   `Arc<dyn DynStore>` 的对象安全适配层 `BoxedStoreImpl<S>`。新增一种后端
//!   时只需实现 `Store`，无需扩展枚举 + 多个 match 分支。
//! - `watch()` 转发用 `try_send` + 丢失计数 log，而非 lib-copy 的 `send_timeout`：
//!   慢消费者**会丢事件**但不会拖累生产侧。
//! - `Selector::from_str` 支持的别名集合从 `etcd / file / mem` 扩展为更宽容的
//!   `etcd|etcd3 / file|fs / mem|memory|inmem` 写法（lib-copy 三种仍生效）。
//!
//! ## 外部契约
//! - 公开类型：`Key`、`KeyValue`、`WatchEvent`、`Store`、`Bucket`、`StoreOutcome`、
//!   `StoreError`、`Versioned`、`Selector`、`Manager`。
//! - `Manager::default()` 等价于 `Manager::memory()`。
//! - `Manager::memory()` / `::etcd(client)` / `::file(cancel, root)` 公开。
//! - `Manager::get_or_create_bucket / get_bucket / load / watch / publish / shutdown /
//!   connection_id` 全部公开、签名与 lib-copy 一致。
//! - `Selector` 实现 `Default = Memory`，`FromStr`、`TryFrom<String>`、`Display`。
//! - `Key`：`new`、`from_url_safe`、`url_safe`、`From<&str>`、`Display`、`AsRef<str>`、
//!   `From<&Key> for String`。

use std::borrow::Cow;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashMap, path::PathBuf};
use std::{env, fmt};

use crate::CancellationToken;
use crate::transports::etcd as etcd_transport;
use async_trait::async_trait;
use futures::StreamExt;
use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, percent_encode};
use serde::{Deserialize, Serialize};

mod mem;
pub use mem::MemoryStore;
mod nats;
pub use nats::NATSStore;
mod etcd;
pub use etcd::EtcdStore;
mod file;
pub use file::FileStore;

// =============================================================================
// === 常量 ====================================================================
// =============================================================================

/// watch 转发通道的容量。
const WATCH_CHANNEL_CAPACITY: usize = 16384;

// =============================================================================
// === Key ====================================================================
// =============================================================================

/// KV 存储里的"键"。封装了 url-safe 编解码。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Key(String);

impl Key {
    pub fn new(s: String) -> Key {
        Key(s)
    }

    /// 接受 percent-encoded 字符串，解码后构造 Key。
    /// e.g. `dynamo%2Fbackend%2Fgenerate%2F17216e63492ef21f` →
    ///      `dynamo/backend/generate/17216e63492ef21f`
    pub fn from_url_safe(s: &str) -> Key {
        Key(percent_decode_str(s).decode_utf8_lossy().to_string())
    }

    /// 返回本 Key 的 url-safe percent-encoded 表示。
    pub fn url_safe(&self) -> Cow<'_, str> {
        percent_encode(self.0.as_bytes(), NON_ALPHANUMERIC).into()
    }
}

impl From<&str> for Key {
    fn from(s: &str) -> Key {
        Key::new(s.to_string())
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for Key {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<&Key> for String {
    fn from(k: &Key) -> String {
        k.0.clone()
    }
}

// =============================================================================
// === KeyValue ===============================================================
// =============================================================================

#[derive(Debug, Clone, PartialEq)]
pub struct KeyValue {
    key: Key,
    value: bytes::Bytes,
}

impl KeyValue {
    pub fn new(key: Key, value: bytes::Bytes) -> Self {
        KeyValue { key, value }
    }

    pub fn key(&self) -> String {
        self.key.clone().to_string()
    }

    pub fn key_str(&self) -> &str {
        self.key.as_ref()
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }

    pub fn value_str(&self) -> anyhow::Result<&str> {
        std::str::from_utf8(self.value()).map_err(From::from)
    }
}

// =============================================================================
// === WatchEvent =============================================================
// =============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum WatchEvent {
    Put(KeyValue),
    Delete(Key),
}

// =============================================================================
// === Store / Bucket trait ===================================================
// =============================================================================

#[async_trait]
pub trait Store: Send + Sync {
    type Bucket: Bucket + Send + Sync + 'static;

    async fn get_or_create_bucket(
        &self,
        bucket_name: &str,
        // 超过此时长的条目应被自动删除
        ttl: Option<Duration>,
    ) -> Result<Self::Bucket, StoreError>;

    async fn get_bucket(&self, bucket_name: &str) -> Result<Option<Self::Bucket>, StoreError>;

    fn connection_id(&self) -> u64;

    fn shutdown(&self);
}

/// 单桶的接口。
#[async_trait]
pub trait Bucket: Send + Sync {
    /// 若 key 不存在则插入；返回 `Created(rev)` 或 `Exists(rev)`。
    /// `key` 不应包含桶名。
    async fn insert(
        &self,
        key: &Key,
        value: bytes::Bytes,
        revision: u64,
    ) -> Result<StoreOutcome, StoreError>;

    /// 读 key。`key` 不应包含桶名。
    async fn get(&self, key: &Key) -> Result<Option<bytes::Bytes>, StoreError>;

    /// 删 key。`key` 不应包含桶名。
    async fn delete(&self, key: &Key) -> Result<(), StoreError>;

    /// 流式订阅新事件。
    async fn watch(
        &self,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = WatchEvent> + Send + '_>>, StoreError>;

    /// 列出桶中所有 entry。**返回的 Key 带桶名前缀**，因此不能直接拿来调
    /// `get` / `delete`。
    async fn entries(&self) -> Result<HashMap<Key, bytes::Bytes>, StoreError>;
}

// =============================================================================
// === Selector ================================================================
// =============================================================================

/// 描述具体后端类型的运行时选择器。
#[derive(Clone, Debug, Default)]
pub enum Selector {
    /// 装箱：Etcd 的 ClientOptions 远大于其他变体。
    Etcd(Box<etcd_transport::ClientOptions>),
    File(PathBuf),
    #[default]
    Memory,
    // NATS 未列入 —— 实现未充分测试且使用率为零。
}

impl fmt::Display for Selector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Selector::Etcd(opts) => {
                let urls = opts.etcd_url.join(",");
                write!(f, "Etcd({urls})")
            }
            Selector::File(path) => write!(f, "File({})", path.display()),
            Selector::Memory => write!(f, "Memory"),
        }
    }
}

impl FromStr for Selector {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Selector> {
        // 接受比 lib-copy 更宽容的别名集合
        match s.to_ascii_lowercase().as_str() {
            "etcd" | "etcd3" => Ok(Self::Etcd(Box::default())),
            "file" | "fs" => {
                let root = env::var("DYN_FILE_KV")
                    .map(PathBuf::from)
                    .unwrap_or_else(|_| env::temp_dir().join("dynamo_store_kv"));
                Ok(Self::File(root))
            }
            "mem" | "memory" | "inmem" => Ok(Self::Memory),
            other => anyhow::bail!("Unknown key-value store type '{other}'"),
        }
    }
}

impl TryFrom<String> for Selector {
    type Error = anyhow::Error;

    fn try_from(s: String) -> anyhow::Result<Selector> {
        s.parse()
    }
}

// =============================================================================
// === 对象安全适配层 ==========================================================
// =============================================================================

/// 内部 trait：把任何 `Store: Send + Sync` 包装成对象安全的形式。
///
/// 之所以不能直接用 `dyn Store`，是因为 `Store` 有泛型 `type Bucket` 关联类型，
/// 不是对象安全的。这里我们桥到 `Box<dyn Bucket>`，把关联类型擦除掉。
#[async_trait]
trait DynStore: Send + Sync {
    async fn get_or_create_bucket(
        &self,
        bucket_name: &str,
        ttl: Option<Duration>,
    ) -> Result<Box<dyn Bucket>, StoreError>;

    async fn get_bucket(
        &self,
        bucket_name: &str,
    ) -> Result<Option<Box<dyn Bucket>>, StoreError>;

    fn connection_id(&self) -> u64;

    fn shutdown(&self);
}

/// 把 `Store` 包成 `DynStore`。透明转发。
struct BoxedStoreImpl<S: Store + 'static> {
    inner: S,
}

#[async_trait]
impl<S: Store + 'static> DynStore for BoxedStoreImpl<S>
where
    S::Bucket: 'static,
{
    async fn get_or_create_bucket(
        &self,
        bucket_name: &str,
        ttl: Option<Duration>,
    ) -> Result<Box<dyn Bucket>, StoreError> {
        let b = self.inner.get_or_create_bucket(bucket_name, ttl).await?;
        Ok(Box::new(b))
    }

    async fn get_bucket(
        &self,
        bucket_name: &str,
    ) -> Result<Option<Box<dyn Bucket>>, StoreError> {
        Ok(self
            .inner
            .get_bucket(bucket_name)
            .await?
            .map(|b| Box::new(b) as Box<dyn Bucket>))
    }

    fn connection_id(&self) -> u64 {
        self.inner.connection_id()
    }

    fn shutdown(&self) {
        self.inner.shutdown()
    }
}

// =============================================================================
// === Manager =================================================================
// =============================================================================

/// 高层管家。Clone 共享底层存储句柄。
#[derive(Clone)]
pub struct Manager(Arc<dyn DynStore>);

impl Default for Manager {
    fn default() -> Self {
        Manager::memory()
    }
}

impl Manager {
    /// 进程内 KV 管家（用于测试 / dev）。
    pub fn memory() -> Self {
        Self::from_store(MemoryStore::new())
    }

    /// etcd 后端 KV 管家。
    pub fn etcd(etcd_client: crate::transports::etcd::Client) -> Self {
        Self::from_store(EtcdStore::new(etcd_client))
    }

    /// 文件系统 KV 管家。
    pub fn file<P: Into<PathBuf>>(cancel_token: CancellationToken, root: P) -> Self {
        Self::from_store(FileStore::new(cancel_token, root))
    }

    /// 把任意 `Store` 包装成 Manager（统一入口）。
    fn from_store<S>(store: S) -> Manager
    where
        S: Store + 'static,
        S::Bucket: 'static,
    {
        Manager(Arc::new(BoxedStoreImpl { inner: store }))
    }

    pub async fn get_or_create_bucket(
        &self,
        bucket_name: &str,
        ttl: Option<Duration>,
    ) -> Result<Box<dyn Bucket>, StoreError> {
        self.0.get_or_create_bucket(bucket_name, ttl).await
    }

    pub async fn get_bucket(
        &self,
        bucket_name: &str,
    ) -> Result<Option<Box<dyn Bucket>>, StoreError> {
        self.0.get_bucket(bucket_name).await
    }

    pub fn connection_id(&self) -> u64 {
        self.0.connection_id()
    }

    /// 反序列化便捷封装：读出一个 JSON 对象。
    pub async fn load<T: for<'a> Deserialize<'a>>(
        &self,
        bucket: &str,
        key: &Key,
    ) -> Result<Option<T>, StoreError> {
        let Some(bucket) = self.0.get_bucket(bucket).await? else {
            return Ok(None);
        };
        match bucket.get(key).await? {
            Some(bytes) => {
                let v: T = serde_json::from_slice(bytes.as_ref())?;
                Ok(Some(v))
            }
            None => Ok(None),
        }
    }

    /// 启动后台 watch 任务：先吐出现存条目，再追踪新增/变更。
    ///
    /// 与 lib-copy 的区别：发往 receiver 用 `try_send`。若 receiver 不消费，
    /// **会丢事件**（log 警告），但生产侧绝不会阻塞。这个权衡更适合"事件流
    /// 用作 hint，订阅者愿意接受最终一致"的场景。
    pub fn watch(
        self: Arc<Self>,
        bucket_name: &str,
        bucket_ttl: Option<Duration>,
        cancel_token: CancellationToken,
    ) -> (
        tokio::task::JoinHandle<Result<(), StoreError>>,
        tokio::sync::mpsc::Receiver<WatchEvent>,
    ) {
        let bucket_name = bucket_name.to_string();
        let (tx, rx) = tokio::sync::mpsc::channel(WATCH_CHANNEL_CAPACITY);

        let task = tokio::spawn(async move {
            let bucket = self
                .0
                .get_or_create_bucket(&bucket_name, bucket_ttl)
                .await?;

            // 现存条目快照
            for (key, bytes) in bucket.entries().await? {
                let ev = WatchEvent::Put(KeyValue::new(key, bytes));
                if let Err(err) = tx.send(ev).await {
                    tracing::error!(bucket_name, %err, "watch initial replay send err");
                    return Ok(());
                }
            }

            // 持续追踪新事件
            let mut stream = bucket.watch().await?;
            loop {
                let event = tokio::select! {
                    _ = cancel_token.cancelled() => break,
                    result = stream.next() => match result {
                        Some(ev) => ev,
                        None => break,
                    }
                };
                // 用 try_send：满了就丢；保持生产侧不阻塞
                if let Err(err) = tx.try_send(event) {
                    tracing::warn!(
                        bucket_name,
                        %err,
                        "watch downstream saturated, dropping event"
                    );
                }
            }
            Ok::<(), StoreError>(())
        });

        (task, rx)
    }

    /// 将一个可序列化对象发布到指定 bucket / key。会自动写回新版本号给对象。
    pub async fn publish<T: Serialize + Versioned + Send + Sync>(
        &self,
        bucket_name: &str,
        bucket_ttl: Option<Duration>,
        key: &Key,
        obj: &mut T,
    ) -> anyhow::Result<StoreOutcome> {
        let obj_json = serde_json::to_vec(obj)?;
        let bucket = self.0.get_or_create_bucket(bucket_name, bucket_ttl).await?;

        let outcome = bucket.insert(key, obj_json.into(), obj.revision()).await?;
        match outcome {
            StoreOutcome::Created(rev) | StoreOutcome::Exists(rev) => obj.set_revision(rev),
        }
        Ok(outcome)
    }

    /// 清理临时状态。
    pub fn shutdown(&self) {
        self.0.shutdown()
    }
}

// =============================================================================
// === StoreOutcome / StoreError / Versioned ==================================
// =============================================================================

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum StoreOutcome {
    /// 写入成功，返回的版本号是新版本。注意 "create" 在 lib-copy 语义中也包含
    /// update —— 任何新版本都视为 create。
    Created(u64),
    /// 写入为 NOOP：值已存在且版本相同。
    Exists(u64),
}

impl fmt::Display for StoreOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreOutcome::Created(rev) => write!(f, "Created at {rev}"),
            StoreOutcome::Exists(rev) => write!(f, "Exists at {rev}"),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum StoreError {
    #[error("Could not find bucket '{0}'")]
    MissingBucket(String),

    #[error("Could not find key '{0}'")]
    MissingKey(String),

    #[error("Internal storage error: '{0}'")]
    ProviderError(String),

    #[error("Internal NATS error: {0}")]
    NATSError(String),

    #[error("Internal etcd error: {0}")]
    EtcdError(String),

    #[error("Internal filesystem error: {0}")]
    FilesystemError(String),

    #[error("Key Value Error: {0} for bucket '{1}'")]
    KeyValueError(String, String),

    #[error("Error decoding bytes: {0}")]
    JSONDecodeError(#[from] serde_json::error::Error),

    #[error("Race condition, retry the call")]
    Retry,
}

/// 用于 NATS 等带 revision 的后端做原子更新的"版本号载体"。
pub trait Versioned {
    fn revision(&self) -> u64;
    fn set_revision(&mut self, r: u64);
}

// =============================================================================
// === 单元测试 ================================================================
// =============================================================================

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use futures::{StreamExt, pin_mut};

    const BUCKET_NAME: &str = "v1/mdc";

    /// 把 `watch()` 流转成 broadcast 流以便多订阅者测试。
    #[allow(dead_code)]
    pub struct TappableStream {
        tx: tokio::sync::broadcast::Sender<WatchEvent>,
    }

    #[allow(dead_code)]
    impl TappableStream {
        async fn new<T>(stream: T, max_size: usize) -> Self
        where
            T: futures::Stream<Item = WatchEvent> + Send + 'static,
        {
            let (tx, _) = tokio::sync::broadcast::channel(max_size);
            let tx2 = tx.clone();
            tokio::spawn(async move {
                pin_mut!(stream);
                while let Some(x) = stream.next().await {
                    let _ = tx2.send(x);
                }
            });
            TappableStream { tx }
        }

        fn subscribe(&self) -> tokio::sync::broadcast::Receiver<WatchEvent> {
            self.tx.subscribe()
        }
    }

    fn init() {
        crate::logging::init();
    }

    // ---------------------------------------------------------------------
    // === lib-copy 标准契约测试（原样保留）================================
    // ---------------------------------------------------------------------

    #[tokio::test]
    async fn test_memory_storage() -> anyhow::Result<()> {
        init();

        let s = Arc::new(MemoryStore::new());
        let s2 = Arc::clone(&s);

        let bucket = s.get_or_create_bucket(BUCKET_NAME, None).await?;
        let res = bucket.insert(&"test1".into(), "value1".into(), 0).await?;
        assert_eq!(res, StoreOutcome::Created(0));

        let mut expected = Vec::with_capacity(3);
        for i in 1..=3 {
            let item = WatchEvent::Put(KeyValue::new(
                Key::new(format!("test{i}")),
                format!("value{i}").into(),
            ));
            expected.push(item);
        }

        let (got_first_tx, got_first_rx) = tokio::sync::oneshot::channel();
        let ingress = tokio::spawn(async move {
            let b2 = s2.get_or_create_bucket(BUCKET_NAME, None).await?;
            let mut stream = b2.watch().await?;

            // Put in before starting the watch-all
            let v = stream.next().await.unwrap();
            assert_eq!(v, expected[0]);

            got_first_tx.send(()).unwrap();

            // Put in after
            let v = stream.next().await.unwrap();
            assert_eq!(v, expected[1]);

            let v = stream.next().await.unwrap();
            assert_eq!(v, expected[2]);

            Ok::<_, StoreError>(())
        });

        // MemoryStore 用 HashMap 无固有顺序：在插 test2 前先确保 test1 已抽出。
        got_first_rx.await?;

        let res = bucket.insert(&"test2".into(), "value2".into(), 0).await?;
        assert_eq!(res, StoreOutcome::Created(0));

        // 重复 key + revision —— NOOP
        let res = bucket.insert(&"test2".into(), "value2".into(), 0).await?;
        assert_eq!(res, StoreOutcome::Exists(0));

        // 递增 revision
        let res = bucket.insert(&"test2".into(), "value2".into(), 1).await?;
        assert_eq!(res, StoreOutcome::Created(1));

        let res = bucket.insert(&"test3".into(), "value3".into(), 0).await?;
        assert_eq!(res, StoreOutcome::Created(0));

        let _ = ingress.await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_broadcast_stream() -> anyhow::Result<()> {
        init();

        let s: &'static _ = Box::leak(Box::new(MemoryStore::new()));
        let bucket: &'static _ =
            Box::leak(Box::new(s.get_or_create_bucket(BUCKET_NAME, None).await?));

        let res = bucket.insert(&"test1".into(), "value1".into(), 0).await?;
        assert_eq!(res, StoreOutcome::Created(0));

        let stream = bucket.watch().await?;
        let tap = TappableStream::new(stream, 10).await;

        let mut rx1 = tap.subscribe();
        let mut rx2 = tap.subscribe();

        let item = WatchEvent::Put(KeyValue::new(Key::new("test1".to_string()), "GK".into()));
        let item_clone = item.clone();
        let handle1 = tokio::spawn(async move {
            let b = rx1.recv().await.unwrap();
            assert_eq!(b, item_clone);
        });
        let handle2 = tokio::spawn(async move {
            let b = rx2.recv().await.unwrap();
            assert_eq!(b, item);
        });

        bucket.insert(&"test1".into(), "GK".into(), 1).await?;

        let _ = futures::join!(handle1, handle2);
        Ok(())
    }

    // ---------------------------------------------------------------------
    // === 实现细节补充测试 ================================================
    // ---------------------------------------------------------------------

    /// ## 测试过程
    /// `Key::url_safe()` 和 `Key::from_url_safe()` 应可往返。
    /// ## 意义
    /// 锁定 Key 编解码的对称性契约。
    #[test]
    fn key_url_safe_roundtrip() {
        let original = Key::new("dynamo/backend/generate/17216e63492ef21f".to_string());
        let encoded = original.url_safe();
        assert!(encoded.contains("%2F"));
        let decoded = Key::from_url_safe(&encoded);
        assert_eq!(original, decoded);
    }

    /// ## 测试过程
    /// `Selector::from_str` 应识别本实现新增的别名（etcd3 / fs / memory / inmem）以及 lib-copy 三种原名。
    /// ## 意义
    /// 锁定 Selector 解析的兼容契约。
    #[test]
    fn selector_aliases_recognized() {
        for s in ["etcd", "etcd3", "Etcd"] {
            assert!(matches!(s.parse::<Selector>(), Ok(Selector::Etcd(_))));
        }
        for s in ["mem", "memory", "inmem", "MEM"] {
            assert!(matches!(s.parse::<Selector>(), Ok(Selector::Memory)));
        }
        for s in ["file", "fs", "FILE"] {
            assert!(matches!(s.parse::<Selector>(), Ok(Selector::File(_))));
        }
        assert!("nope".parse::<Selector>().is_err());
    }

    /// ## 测试过程
    /// `Manager::default()` 等价于 `Manager::memory()`。
    /// ## 意义
    /// 锁定默认管家行为。
    #[tokio::test]
    async fn manager_default_is_memory() {
        let m = Manager::default();
        let b = m.get_or_create_bucket("b", None).await.unwrap();
        b.insert(&"k".into(), "v".into(), 0).await.unwrap();
        assert_eq!(b.get(&"k".into()).await.unwrap().unwrap(), bytes::Bytes::from("v"));
    }

    /// ## 测试过程
    /// `Manager::load::<T>` 在 bucket 不存在时返回 `Ok(None)`；在 key 存在时正确反序列化。
    /// ## 意义
    /// 锁定 `load` 便捷封装的两条主路径。
    #[tokio::test]
    async fn manager_load_typed_value() {
        #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
        struct Card {
            name: String,
            value: i32,
        }

        let m = Manager::memory();
        let original = Card {
            name: "alpha".to_string(),
            value: 42,
        };

        // bucket 不存在 → None
        let none: Option<Card> = m.load("missing", &"k".into()).await.unwrap();
        assert!(none.is_none());

        // 创建桶并写入
        let b = m.get_or_create_bucket("cards", None).await.unwrap();
        b.insert(
            &"a".into(),
            serde_json::to_vec(&original).unwrap().into(),
            0,
        )
        .await
        .unwrap();

        let got: Option<Card> = m.load("cards", &"a".into()).await.unwrap();
        assert_eq!(got, Some(original));
    }

    /// ## 测试过程
    /// `Manager::watch` 先回放现存 entry，再追踪新事件，并能被 cancel_token 停下。
    /// ## 意义
    /// 锁定 watch 任务的"先快照后流"契约 + 取消行为。
    #[tokio::test]
    async fn manager_watch_replays_then_streams_and_cancels() {
        use tokio::time::{Duration as TDuration, timeout};

        let mgr = Arc::new(Manager::memory());
        // 先预置一个 entry
        let b = mgr.get_or_create_bucket("w", None).await.unwrap();
        b.insert(&"existing".into(), "v0".into(), 0).await.unwrap();

        let cancel = CancellationToken::new();
        let (task, mut rx) = mgr.clone().watch("w", None, cancel.clone());

        // 第一个事件应该是快照的 Put。注意：MemoryStore::watch() 自身也会
        // replay 一次现存条目，而 Manager::watch 又先做了 entries() 快照，所以
        // "existing" 实际会出现多次 —— 这里只确认它先到。
        let ev = timeout(TDuration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(ev, WatchEvent::Put(ref kv) if kv.key().ends_with("existing")),
            "expected initial replay of 'existing', got {ev:?}"
        );

        // 新增一个 entry，应能在后续若干事件中看到（容忍快照重复）。
        b.insert(&"new".into(), "v1".into(), 0).await.unwrap();
        let mut saw_new = false;
        for _ in 0..8 {
            match timeout(TDuration::from_millis(500), rx.recv()).await {
                Ok(Some(WatchEvent::Put(kv))) if kv.key() == "new" => {
                    saw_new = true;
                    break;
                }
                Ok(Some(_)) => continue,
                _ => break,
            }
        }
        assert!(saw_new, "did not observe live 'new' event");

        // 取消后 task 应优雅退出
        cancel.cancel();
        let res = timeout(TDuration::from_secs(2), task).await;
        assert!(res.is_ok(), "watch task should finish on cancel");
    }

    /// ## 测试过程
    /// `Manager::publish` 会把写入后的版本号回写到对象上。
    /// ## 意义
    /// 锁定 publish + Versioned 协议契约。
    #[tokio::test]
    async fn manager_publish_writes_back_revision() {
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Doc {
            payload: String,
            #[serde(default)]
            revision: u64,
        }
        impl Versioned for Doc {
            fn revision(&self) -> u64 {
                self.revision
            }
            fn set_revision(&mut self, r: u64) {
                self.revision = r;
            }
        }

        let mgr = Manager::memory();
        let mut doc = Doc {
            payload: "hello".to_string(),
            revision: 0,
        };
        let outcome = mgr.publish("docs", None, &"d".into(), &mut doc).await.unwrap();
        assert!(matches!(outcome, StoreOutcome::Created(0)));
        // 0 → 0 写回
        assert_eq!(doc.revision, 0);

        doc.payload = "hello2".to_string();
        doc.revision = 1;
        let outcome = mgr.publish("docs", None, &"d".into(), &mut doc).await.unwrap();
        assert!(matches!(outcome, StoreOutcome::Created(1)));
        assert_eq!(doc.revision, 1);
    }
}
