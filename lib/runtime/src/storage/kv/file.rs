// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 文件系统 KV 后端
//!
//! ## 设计意图
//! 将"桶"映射为目录，"key"映射为文件。提供本地、跨进程可见的 KV 存储，
//! 主要用于单机多进程的 dev/test 与离线调试。
//!
//! 本实现契约完全等价，但内部走了与之不同的实现路径：
//! - 用 `DashMap` / `DashSet` 替代 `parking_lot::Mutex<HashMap/HashSet>`
//!   作为活动目录与 owned-files 集合，避免在 keep-alive / 过期清理时持有
//!   粗粒度互斥锁；
//! - keep-alive / 过期清理改用 tokio 异步任务 + `select!` 监听 cancel_token，
//!   响应停机更迅速；
//! - watcher 的回调用 `try_send`，慢消费者下宁可丢事件也不阻塞 notify 后端线程；
//! - 临时文件命名换前缀 `.dyn-tmp-` 并附加 nanos+rand；
//! - 错误路径上更细颗粒地区分 `MissingKey` 与 `FilesystemError`。
//!
//! ## 外部契约
//! - `FileStore: Clone`，多 handle 共享 `active_dirs` / `connection_id`。
//! - `FileStore::new(cancel_token, root)` 限定 `pub(super)`，由 Manager 构造。
//! - `Store::get_or_create_bucket(name, ttl)`：name 不存在则 `mkdir -p`，
//!   存在但不是目录 → `FilesystemError`。
//! - `Store::get_bucket(name)`：不存在 → `Ok(None)`；不是目录 → `FilesystemError`。
//! - `Bucket::insert` 始终原子写（先写临时文件再 rename），revision 被忽略，
//!   返回 `StoreOutcome::Created(0)`；将路径登记为 owned-file（shutdown 时清理）。
//! - `Bucket::get` 不存在 → `Ok(None)`。
//! - `Bucket::delete` 不存在 → `Err(MissingKey)`；存在则从 owned-files 移除后删盘。
//! - `Bucket::watch` 流的 `Key` **以 root 为前缀剥离后** url-decode 得到，跳过临时文件。
//! - `Bucket::entries` 返回的 `Key` **包含桶名前缀**（如 `v1/tests/key1/multi/part`），
//!   与 lib-copy 一致。
//! - `shutdown` 删除所有 owned files；非 `Drop`，因为 `DistributedRuntime` 在
//!   Python 端可能不会触发 Drop。

use std::cmp;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Context as _;
use async_trait::async_trait;
use dashmap::{DashMap, DashSet};
use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher, event};
use tokio_util::sync::CancellationToken;

use super::{Bucket, Key, KeyValue, Store, StoreError, StoreOutcome, WatchEvent};

// =============================================================================
// === 常量 ====================================================================
// =============================================================================

/// 默认 TTL：与 etcd lease 同步为 10s。
const DEFAULT_TTL: Duration = Duration::from_secs(10);

/// keep-alive 最小间隔，避免在极小 TTL 下狂写磁盘。
const MIN_KEEP_ALIVE: Duration = Duration::from_secs(1);

/// 原子写临时文件前缀。watcher 应过滤掉这些文件。
///
/// 与 lib-copy 的 `.tmp_` 不同 —— 即便两实现在同一目录共存也不会互相冲突识别。
const TEMP_FILE_PREFIX: &str = ".dyn-tmp-";

/// 异步事件转发的内部 channel 容量。
const WATCH_CHANNEL_CAPACITY: usize = 256;

// =============================================================================
// === FileStore ===============================================================
// =============================================================================

/// 文件系统 KV 后端。Clone 后多个 handle 共享底层 `active_dirs`。
#[derive(Clone)]
pub struct FileStore {
    cancel_token: CancellationToken,
    root: PathBuf,
    connection_id: u64,
    /// 我们曾在其中创建/打开过的目录，便于 keep-alive 与 shutdown 清理。
    /// DashMap 让"添加/查询不同桶"天然分片。
    active_dirs: Arc<DashMap<PathBuf, Directory>>,
}

impl FileStore {
    /// 仅由 [`super::Manager`] 构造。
    pub(super) fn new<P: Into<PathBuf>>(cancel_token: CancellationToken, root_dir: P) -> Self {
        let fs = FileStore {
            cancel_token,
            root: root_dir.into(),
            connection_id: rand::random::<u64>(),
            active_dirs: Arc::new(DashMap::new()),
        };
        // 用 tokio 异步任务跑 keep-alive 与过期清理。
        // 注：lib-copy 在 std::thread 里跑；我们改在 tokio 上，是为了能用
        // select! 即时响应 cancel_token，停机时延更小。
        let bg = fs.clone();
        tokio::spawn(async move { bg.expiry_loop().await });
        fs
    }

    // -------------------------------------------------------------------------
    // === 后台维护任务 =========================================================
    // -------------------------------------------------------------------------

    /// keep-alive / 过期清理主循环。`select!` 同时监听 cancel 与 tick。
    async fn expiry_loop(self) {
        loop {
            let ttl = self.shortest_ttl();
            let interval = cmp::max(ttl / 3, MIN_KEEP_ALIVE);

            tokio::select! {
                _ = self.cancel_token.cancelled() => break,
                _ = tokio::time::sleep(interval) => {
                    self.keep_alive();
                    if let Err(err) = self.delete_expired_files() {
                        tracing::error!(error = %err, "FileStore delete_expired_files");
                    }
                }
            }
        }
    }

    fn shortest_ttl(&self) -> Duration {
        let mut ttl = DEFAULT_TTL;
        for entry in self.active_dirs.iter() {
            ttl = cmp::min(ttl, entry.value().ttl);
        }
        ttl
    }

    fn keep_alive(&self) {
        let dirs: Vec<Directory> = self
            .active_dirs
            .iter()
            .map(|r| r.value().clone())
            .collect();
        for dir in dirs {
            dir.keep_alive();
        }
    }

    fn delete_expired_files(&self) -> anyhow::Result<()> {
        let snapshots: Vec<(PathBuf, Directory)> = self
            .active_dirs
            .iter()
            .map(|r| (r.key().clone(), r.value().clone()))
            .collect();
        for (path, dir) in snapshots {
            dir.delete_expired_files()
                .with_context(|| path.display().to_string())?;
        }
        Ok(())
    }

    /// 检查 `p` 是否是已存在的目录；否则 mkdir -p。
    fn ensure_dir(p: &Path) -> Result<(), StoreError> {
        if p.exists() {
            if !p.is_dir() {
                return Err(StoreError::FilesystemError(
                    "Bucket name is not a directory".to_string(),
                ));
            }
            Ok(())
        } else {
            fs::create_dir_all(p).map_err(to_fs_err)
        }
    }
}

#[async_trait]
impl Store for FileStore {
    type Bucket = Directory;

    async fn get_or_create_bucket(
        &self,
        bucket_name: &str,
        ttl: Option<Duration>,
    ) -> Result<Self::Bucket, StoreError> {
        let p = self.root.join(bucket_name);

        // fast-path: 已注册
        if let Some(d) = self.active_dirs.get(&p) {
            return Ok(d.value().clone());
        }

        // slow-path: 创建/校验后注册
        Self::ensure_dir(&p)?;
        let dir = Directory::new(self.root.clone(), p.clone(), ttl.unwrap_or(DEFAULT_TTL));
        // 用 entry().or_insert 防止"创建竞争"导致丢失登记
        let registered = self
            .active_dirs
            .entry(p)
            .or_insert(dir)
            .value()
            .clone();
        Ok(registered)
    }

    async fn get_bucket(&self, bucket_name: &str) -> Result<Option<Self::Bucket>, StoreError> {
        let p = self.root.join(bucket_name);

        if let Some(d) = self.active_dirs.get(&p) {
            return Ok(Some(d.value().clone()));
        }
        if !p.exists() {
            return Ok(None);
        }
        if !p.is_dir() {
            return Err(StoreError::FilesystemError(
                "Bucket name is not a directory".to_string(),
            ));
        }
        let dir = Directory::new(self.root.clone(), p.clone(), DEFAULT_TTL);
        let registered = self
            .active_dirs
            .entry(p)
            .or_insert(dir)
            .value()
            .clone();
        Ok(Some(registered))
    }

    fn connection_id(&self) -> u64 {
        self.connection_id
    }

    /// 关停：删除所有我们 owned 的文件。不是 Drop，因为上层在 Python 端可能
    /// 永远不触发 Drop。
    fn shutdown(&self) {
        // 一次性把所有目录从 active 中取走
        let keys: Vec<PathBuf> = self
            .active_dirs
            .iter()
            .map(|r| r.key().clone())
            .collect();
        let mut dirs: Vec<Directory> = Vec::with_capacity(keys.len());
        for k in keys {
            if let Some((_p, d)) = self.active_dirs.remove(&k) {
                dirs.push(d);
            }
        }
        for mut dir in dirs {
            if let Err(err) = dir.delete_owned_files() {
                tracing::error!(error = %err, %dir, "Failed shutdown delete of owned files");
            }
        }
    }
}

// =============================================================================
// === Directory：单桶 =========================================================
// =============================================================================

/// 单桶（目录）句柄。Clone 后多个引用共享 owned_files 与 ttl。
#[derive(Clone, Debug)]
pub struct Directory {
    root: PathBuf,
    p: PathBuf,
    ttl: Duration,
    /// 本进程内创建的文件集合。shutdown 时删除这些文件。
    owned_files: Arc<DashSet<PathBuf>>,
}

impl Directory {
    fn new(root: PathBuf, p: PathBuf, ttl: Duration) -> Self {
        // 规范化 root，处理 /var -> /private/var 之类 symlink
        let canonical_root = root.canonicalize().unwrap_or_else(|_| root.clone());
        if ttl < MIN_KEEP_ALIVE {
            tracing::warn!(
                path = %p.display(),
                ttl = %humantime::format_duration(ttl),
                "ttl too short, raised to {}", humantime::format_duration(MIN_KEEP_ALIVE)
            );
        }
        let ttl = cmp::max(ttl, MIN_KEEP_ALIVE);
        Directory {
            root: canonical_root,
            p,
            ttl,
            owned_files: Arc::new(DashSet::new()),
        }
    }

    /// touch owned files 以阻止过期清理。
    fn keep_alive(&self) {
        let now = SystemTime::now();
        let paths: Vec<PathBuf> = self.owned_files.iter().map(|r| r.key().clone()).collect();
        for path in paths {
            match OpenOptions::new().write(true).open(&path) {
                Ok(file) => {
                    if let Err(err) = file.set_modified(now) {
                        tracing::error!(
                            path = %path.display(), error = %err,
                            "keep_alive set_modified failed"
                        );
                    } else {
                        tracing::trace!("FileStore keep_alive set {}", path.display());
                    }
                }
                Err(err) => tracing::error!(
                    path = %path.display(), error = %err,
                    "keep_alive open failed"
                ),
            }
        }
    }

    /// 删除超过 ttl 没动过的文件。看的是整个目录，能顺带回收异常退出的进程留下的孤儿文件。
    fn delete_expired_files(&self) -> anyhow::Result<()> {
        let deadline = SystemTime::now() - self.ttl;
        let dirname = self.p.display().to_string();
        for entry in fs::read_dir(&self.p).with_context(|| dirname.clone())? {
            let entry = match entry {
                Ok(e) => e,
                Err(err) => {
                    tracing::warn!(dir = dirname, error = %err, "read_dir entry err");
                    continue;
                }
            };
            if !entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
                tracing::warn!(
                    dir = dirname, entry = %entry.path().display(),
                    "Unexpected non-file entry"
                );
                continue;
            }
            let path_ctx = entry.path().display().to_string();
            let metadata = match entry.metadata() {
                Ok(m) => m,
                Err(err) => {
                    tracing::warn!(path = %path_ctx, error = %err, "metadata failed");
                    continue;
                }
            };
            let last_modified = match metadata.modified() {
                Ok(t) => t,
                Err(err) => {
                    tracing::warn!(path = %path_ctx, error = %err, "mtime failed");
                    continue;
                }
            };
            if last_modified < deadline {
                tracing::info!(path = %path_ctx, ?last_modified, "Expired");
                if let Err(err) = fs::remove_file(entry.path()) {
                    tracing::warn!(path = %path_ctx, error = %err, "remove failed");
                }
            }
        }
        Ok(())
    }

    fn delete_owned_files(&mut self) -> anyhow::Result<()> {
        let mut errs = Vec::new();
        let paths: Vec<PathBuf> = self.owned_files.iter().map(|r| r.key().clone()).collect();
        self.owned_files.clear();
        for p in paths {
            if let Err(err) = fs::remove_file(&p) {
                errs.push(format!("{}: {err}", p.display()));
            }
        }
        if !errs.is_empty() {
            anyhow::bail!(errs.join(", "));
        }
        Ok(())
    }

    /// 生成不会与 lib-copy 冲突的临时文件名。
    fn temp_path(&self) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let r: u64 = rand::random();
        self.p
            .join(format!("{TEMP_FILE_PREFIX}{nanos:032x}-{r:016x}"))
    }
}

impl fmt::Display for Directory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.p.display())
    }
}

#[async_trait]
impl Bucket for Directory {
    /// 原子写：temp -> rename。watcher 只会看到 rename 后的最终文件，不会看到半截写。
    async fn insert(
        &self,
        key: &Key,
        value: bytes::Bytes,
        _revision: u64, // 文件系统不维护 revision
    ) -> Result<StoreOutcome, StoreError> {
        let safe_key = key.url_safe();
        let full_path = self.p.join(safe_key.as_ref());
        let temp_path = self.temp_path();

        fs::write(&temp_path, &value)
            .with_context(|| format!("writing temp {}", temp_path.display()))
            .map_err(a_to_fs_err)?;
        fs::rename(&temp_path, &full_path)
            .with_context(|| {
                format!(
                    "renaming {} -> {}",
                    temp_path.display(),
                    full_path.display()
                )
            })
            .map_err(a_to_fs_err)?;

        self.owned_files.insert(full_path);
        Ok(StoreOutcome::Created(0))
    }

    async fn get(&self, key: &Key) -> Result<Option<bytes::Bytes>, StoreError> {
        let safe_key = key.url_safe();
        let full_path = self.p.join(safe_key.as_ref());
        if !full_path.exists() {
            return Ok(None);
        }
        let str_path = full_path.display().to_string();
        let data: bytes::Bytes = fs::read(&full_path)
            .context(str_path)
            .map_err(a_to_fs_err)?
            .into();
        Ok(Some(data))
    }

    async fn delete(&self, key: &Key) -> Result<(), StoreError> {
        let safe_key = key.url_safe();
        let full_path = self.p.join(safe_key.as_ref());
        if !full_path.exists() {
            return Err(StoreError::MissingKey(full_path.display().to_string()));
        }
        self.owned_files.remove(&full_path);
        fs::remove_file(&full_path)
            .context(full_path.display().to_string())
            .map_err(a_to_fs_err)
    }

    async fn watch(
        &self,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = WatchEvent> + Send + 'life0>>, StoreError> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(WATCH_CHANNEL_CAPACITY);

        // notify 回调跑在它自己的内部线程上。我们用 try_send 把事件转交给
        // tokio runtime；满了就丢一条 —— 不阻塞 notify 线程是关键。
        let mut watcher = RecommendedWatcher::new(
            move |res: Result<Event, notify::Error>| {
                if let Err(err) = tx.try_send(res) {
                    tracing::warn!(error = %err, "FileStore watch channel saturated, dropping event");
                }
            },
            Config::default(),
        )
        .map_err(to_fs_err)?;

        watcher
            .watch(&self.p, RecursiveMode::NonRecursive)
            .map_err(to_fs_err)?;

        let dir = self.p.clone();
        let root = self.root.clone();

        Ok(Box::pin(async_stream::stream! {
            // 让 watcher 与 stream 同生共死
            let _watcher = watcher;

            while let Some(item) = rx.recv().await {
                let event = match item {
                    Ok(ev) => ev,
                    Err(err) => {
                        tracing::error!(error = %err, "watch event err");
                        continue;
                    }
                };

                for raw_path in event.paths {
                    // 忽略针对目录自身的事件
                    if raw_path == dir {
                        continue;
                    }

                    // 跳过临时文件
                    if raw_path
                        .file_name()
                        .map(|n| n.to_string_lossy().starts_with(TEMP_FILE_PREFIX))
                        .unwrap_or(false)
                    {
                        continue;
                    }

                    // canonicalize 路径（remove 时文件已经不存在，回退到原路径）
                    let canonical = raw_path.canonicalize().unwrap_or_else(|_| raw_path.clone());
                    let key = match canonical.strip_prefix(&root) {
                        Ok(stripped) => Key::from_url_safe(&stripped.display().to_string()),
                        Err(err) => {
                            tracing::error!(
                                error = %err,
                                item_path = %canonical.display(),
                                root = %root.display(),
                                "watched path outside root, ignoring"
                            );
                            continue;
                        }
                    };

                    match event.kind {
                        EventKind::Create(event::CreateKind::File)
                        | EventKind::Modify(event::ModifyKind::Data(event::DataChange::Content))
                        | EventKind::Modify(event::ModifyKind::Name(event::RenameMode::To)) => {
                            match fs::read(&raw_path) {
                                Ok(buf) => {
                                    let data: bytes::Bytes = buf.into();
                                    yield WatchEvent::Put(KeyValue::new(key, data));
                                }
                                Err(err) => {
                                    tracing::warn!(error = %err, item = %raw_path.display(), "read failed, skipping");
                                }
                            }
                        }
                        EventKind::Remove(event::RemoveKind::File) => {
                            yield WatchEvent::Delete(key);
                        }
                        _ => {
                            // keep-alive 触发的 mtime 更新等，忽略
                        }
                    }
                }
            }
        }))
    }

    async fn entries(&self) -> Result<HashMap<Key, bytes::Bytes>, StoreError> {
        let read = fs::read_dir(&self.p)
            .with_context(|| self.p.display().to_string())
            .map_err(a_to_fs_err)?;

        let mut out = HashMap::new();
        for entry in read {
            let entry = entry.map_err(to_fs_err)?;

            if !entry.path().is_file() {
                tracing::warn!(
                    path = %entry.path().display(),
                    "Unexpected non-file entry"
                );
                continue;
            }
            if entry
                .file_name()
                .to_string_lossy()
                .starts_with(TEMP_FILE_PREFIX)
            {
                continue;
            }

            let canonical = entry
                .path()
                .canonicalize()
                .unwrap_or_else(|_| entry.path());
            let key = match canonical.strip_prefix(&self.root) {
                Ok(p) => Key::from_url_safe(&p.to_string_lossy()),
                Err(err) => {
                    tracing::error!(
                        error = %err,
                        path = %canonical.display(),
                        root = %self.root.display(),
                        "FileStore path not in root, skipping"
                    );
                    continue;
                }
            };
            let data: bytes::Bytes = fs::read(entry.path())
                .with_context(|| self.p.display().to_string())
                .map_err(a_to_fs_err)?
                .into();
            out.insert(key, data);
        }
        Ok(out)
    }
}

// =============================================================================
// === 错误辅助 ================================================================
// =============================================================================

/// anyhow 路径上保留 context。
fn a_to_fs_err(err: anyhow::Error) -> StoreError {
    StoreError::FilesystemError(format!("{err:#}"))
}

fn to_fs_err<E: std::error::Error>(err: E) -> StoreError {
    StoreError::FilesystemError(err.to_string())
}

// =============================================================================
// === 单元测试 ================================================================
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::kv::{Bucket as _, FileStore, Key, Store as _};
    use std::collections::HashSet;
    use tokio_util::sync::CancellationToken;

    // ---------------------------------------------------------------------
    // === lib-copy 标准契约测试（原样保留）================================
    // ---------------------------------------------------------------------

    #[tokio::test]
    async fn test_entries_full_path() {
        let t = tempfile::tempdir().unwrap();

        let cancel_token = CancellationToken::new();
        let m = FileStore::new(cancel_token.clone(), t.path());
        let bucket = m.get_or_create_bucket("v1/tests", None).await.unwrap();
        let _ = bucket
            .insert(&Key::new("key1/multi/part".to_string()), "value1".into(), 0)
            .await
            .unwrap();
        let _ = bucket
            .insert(&Key::new("key2".to_string()), "value2".into(), 0)
            .await
            .unwrap();
        let entries = bucket.entries().await.unwrap();
        let keys: HashSet<Key> = entries.into_keys().collect();
        cancel_token.cancel(); // 停后台任务

        assert!(keys.contains(&Key::new("v1/tests/key1/multi/part".to_string())));
        assert!(keys.contains(&Key::new("v1/tests/key2".to_string())));
    }

    // ---------------------------------------------------------------------
    // === 实现细节补充测试 ================================================
    // ---------------------------------------------------------------------

    /// ## 测试过程
    /// 写入后立刻读，读到原值；读不存在的 key 返回 Ok(None)。
    /// ## 意义
    /// 锁定 insert/get 的回环契约与缺失键语义。
    #[tokio::test]
    async fn insert_get_roundtrip_and_missing() {
        let t = tempfile::tempdir().unwrap();
        let cancel = CancellationToken::new();
        let s = FileStore::new(cancel.clone(), t.path());
        let b = s.get_or_create_bucket("b", None).await.unwrap();

        b.insert(&"k".into(), "v".into(), 0).await.unwrap();
        assert_eq!(b.get(&"k".into()).await.unwrap().unwrap(), bytes::Bytes::from("v"));
        assert!(b.get(&"missing".into()).await.unwrap().is_none());
        cancel.cancel();
    }

    /// ## 测试过程
    /// 删除不存在的 key 应返回 `MissingKey`；删除存在的 key 成功且 owned 集合更新。
    /// ## 意义
    /// 锁定 delete 错误语义与 owned-files 管账正确性。
    #[tokio::test]
    async fn delete_missing_returns_error() {
        let t = tempfile::tempdir().unwrap();
        let cancel = CancellationToken::new();
        let s = FileStore::new(cancel.clone(), t.path());
        let b = s.get_or_create_bucket("b", None).await.unwrap();

        let err = b.delete(&"absent".into()).await.unwrap_err();
        assert!(matches!(err, StoreError::MissingKey(_)));

        b.insert(&"present".into(), "v".into(), 0).await.unwrap();
        assert!(b.delete(&"present".into()).await.is_ok());
        assert!(b.get(&"present".into()).await.unwrap().is_none());
        cancel.cancel();
    }

    /// ## 测试过程
    /// 用一个文件名占位"非目录"作为 bucket，`get_or_create_bucket` 应返回 FilesystemError。
    /// ## 意义
    /// 验证"bucket 必须是目录"的契约。
    #[tokio::test]
    async fn bucket_must_be_directory() {
        let t = tempfile::tempdir().unwrap();
        let cancel = CancellationToken::new();
        fs::write(t.path().join("not_dir"), b"x").unwrap();
        let s = FileStore::new(cancel.clone(), t.path());

        let err = s.get_or_create_bucket("not_dir", None).await.unwrap_err();
        assert!(matches!(err, StoreError::FilesystemError(_)));
        cancel.cancel();
    }

    /// ## 测试过程
    /// shutdown 之后 owned 文件被删除，且后续读返回 None。
    /// ## 意义
    /// 锁定 shutdown 的清理契约。
    #[tokio::test]
    async fn shutdown_removes_owned_files() {
        let t = tempfile::tempdir().unwrap();
        let cancel = CancellationToken::new();
        let s = FileStore::new(cancel.clone(), t.path());
        let b = s.get_or_create_bucket("b", None).await.unwrap();
        b.insert(&"k".into(), "v".into(), 0).await.unwrap();

        let path = t.path().join("b").join("k");
        assert!(path.exists());

        s.shutdown();
        assert!(!path.exists(), "owned file should be removed by shutdown");
        cancel.cancel();
    }

    /// ## 测试过程
    /// 临时文件前缀的文件不应被 `entries()` 列出。
    /// ## 意义
    /// 锁定"临时文件对外不可见"的契约。
    #[tokio::test]
    async fn entries_skips_temp_files() {
        let t = tempfile::tempdir().unwrap();
        let cancel = CancellationToken::new();
        let s = FileStore::new(cancel.clone(), t.path());
        let b = s.get_or_create_bucket("b", None).await.unwrap();

        let temp = t
            .path()
            .join("b")
            .join(format!("{TEMP_FILE_PREFIX}xyz"));
        fs::write(&temp, b"junk").unwrap();
        b.insert(&"real".into(), "v".into(), 0).await.unwrap();

        let entries = b.entries().await.unwrap();
        let keys: HashSet<Key> = entries.into_keys().collect();
        assert!(keys.iter().any(|k| k.as_ref().ends_with("/real")));
        assert!(!keys.iter().any(|k| k.as_ref().contains(TEMP_FILE_PREFIX)));
        cancel.cancel();
    }
}
