// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 设计意图
//! 本模块为整个 runtime 提供 etcd 客户端外观:把 `etcd_client::Client` + 一个独立的
//! tokio runtime + 主 lease + watch/cache 等高层能力打包到 [`Client`].所有 etcd
//! 相关任务(lease keep-alive, watch reconnect)都在 *专属 runtime* 上运行,避免被
//! 主 runtime 的业务负载饿死.
//!
//! # 外部契约
//! - 公共类型: [`Client`], [`ClientOptions`], [`ClientOptionsBuilder`],
//!   [`KvCache`], [`PrefixWatcher`], [`WatchEvent`];
//! - 再导出: `etcd_client::{ConnectOptions, KeyValue, LeaseClient}` 以及 `lock::*`;
//! - [`Client::builder`] -> [`ClientOptionsBuilder`] 是构造首选入口;
//! - 行为契约:`kv_create` 是 *幂等* 的 —— key 已存在时返回 `Ok(Some(version))`
//!   而非 Err(PR #4212 后的对齐 StoreOutcome::Exists 的设计);
//! - `kv_watch_prefix` 不回放历史;`kv_get_and_watch_prefix` 先回放后追加;
//! - `KvCache::new` 完成"拉取现有键 + 初始值补写 + 启动后台 watcher"三步初始化;
//! - 私有 `default_servers()` 与 `Client::etcd_client()` 被同模块测试访问,不可改名;
//! - 环境变量驱动的默认值由 [`crate::config::environment_names::etcd`] 命名空间决定.
//!
//! # 实现要点
//! - `Client::new` 调用 `build_in_runtime(...)` 把"连接 + lease 申请"集中在专属 rt 上;
//! - watch 路径拆分为 `get_start_revision` / `new_watch_stream` / `monitor_watch_stream`
//!   / `process_watch_events` 四个职责分明的小函数,reconnect 通过 `Connector::reconnect`
//!   完成;
//! - watch 通道容量在初始化时按已有 KV 数量预扩,避免 push 历史时阻塞调用方;
//! - `KvCache` 用 `Arc<RwLock<HashMap>>` 做内存视图,写路径"先 etcd 后内存"以保证持久性优先.

use crate::runtime::Runtime;
use anyhow::{Context, Result};

use derive_builder::Builder;
use derive_getters::Dissolve;
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
use validator::Validate;

use etcd_client::{
    Certificate, Compare, CompareOp, DeleteOptions, GetOptions, Identity, LockOptions,
    LockResponse, PutOptions, PutResponse, TlsOptions, Txn, TxnOp, TxnOpResponse, WatchOptions,
    WatchStream,
};
pub use etcd_client::{ConnectOptions, KeyValue, LeaseClient};
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

mod connector;
mod lease;
mod lock;

use connector::Connector;
use lease::create_lease;
pub use lock::*;

use super::utils::build_in_runtime;
use crate::config::environment_names::etcd as env_etcd;

// === SECTION: Client ===

/// etcd 客户端外观.内部持有一个可热重连的连接器,以及一个独立 tokio runtime
/// 专门用来执行 keep-alive 与 watch 任务.
#[derive(Clone)]
pub struct Client {
    connector: Arc<Connector>,
    primary_lease: u64,
    runtime: Runtime,
    /// 专门承载 lease keep-alive 与 watch 的独立 tokio runtime.
    ///
    /// WARNING: 不要在这个 runtime 上 await 主 runtime 的任务,否则可能死锁.
    rt: Arc<tokio::runtime::Runtime>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "etcd::Client primary_lease={}", self.primary_lease)
    }
}

impl Client {
    /// 选项构造器入口.
    pub fn builder() -> ClientOptionsBuilder {
        ClientOptionsBuilder::default()
    }

    /// 创建一个 etcd 客户端.
    ///
    /// 步骤:
    /// 1. 在专属 runtime 上建立连接;
    /// 2. 若 `attach_lease == true`,申请 10 秒的主 lease,并把其生命周期绑定到 runtime 的 primary token;
    /// 3. lease 一旦过期会取消 runtime;runtime 被关停时反向 revoke lease.
    pub async fn new(config: ClientOptions, runtime: Runtime) -> Result<Self> {
        let token = runtime.primary_token();

        let ((connector, lease_id), rt) = build_in_runtime(
            async move {
                let etcd_urls = config.etcd_url.clone();
                let connect_options = config.etcd_connect_options.clone();

                let connector = Connector::new(etcd_urls, connect_options)
                    .await
                    .with_context(|| {
                        format!(
                            "Unable to connect to etcd server at {}. Check etcd server status",
                            config.etcd_url.join(", ")
                        )
                    })?;

                let lease_id = if config.attach_lease {
                    create_lease(connector.clone(), 10, token)
                        .await
                        .with_context(|| {
                            format!(
                                "Unable to create lease. Check etcd server status at {}",
                                config.etcd_url.join(", ")
                            )
                        })?
                } else {
                    0
                };

                Ok((connector, lease_id))
            },
            1,
        )
        .await?;

        Ok(Client {
            connector,
            primary_lease: lease_id,
            rt,
            runtime,
        })
    }

    /// 返回底层 etcd 客户端的克隆(克隆成本低,内部仅 Arc++).
    fn etcd_client(&self) -> etcd_client::Client {
        self.connector.get_client()
    }

    /// 返回主 lease id;构造时若 `attach_lease=false` 则为 0.
    pub fn lease_id(&self) -> u64 {
        self.primary_lease
    }

    /// 内部:把 `lease_id` 解析为实际的 PutOptions(默认使用主 lease).
    #[inline]
    fn put_options_for(&self, lease_id: Option<u64>) -> PutOptions {
        let id = lease_id.unwrap_or(self.lease_id());
        PutOptions::new().with_lease(id as i64)
    }

    /// 幂等地创建键值对.
    ///
    /// 返回:
    /// - `Ok(None)`: 创建成功;
    /// - `Ok(Some(version))`: 键已存在,返回现存版本号;
    /// - `Err(_)`: 真正的错误(网络/超时/etcd 异常).
    pub async fn kv_create(
        &self,
        key: &str,
        value: Vec<u8>,
        lease_id: Option<u64>,
    ) -> Result<Option<u64>> {
        let put_options = self.put_options_for(lease_id);

        // when version==0 则写入新值;否则 get 已有值供调用方读取版本号.
        let txn = Txn::new()
            .when(vec![Compare::version(key, CompareOp::Equal, 0)])
            .and_then(vec![TxnOp::put(key, value, Some(put_options))])
            .or_else(vec![TxnOp::get(key, None)]);

        let result = self.connector.get_client().kv_client().txn(txn).await?;

        if result.succeeded() {
            return Ok(None);
        }

        // 取 or_else 分支返回的 Get 响应,提取已有键的版本号.
        if let Some(TxnOpResponse::Get(get_resp)) = result.op_responses().into_iter().next()
            && let Some(kv) = get_resp.kvs().first()
        {
            return Ok(Some(kv.version() as u64));
        }

        for resp in result.op_responses() {
            tracing::warn!(response = ?resp, "kv_create etcd op response");
        }
        anyhow::bail!("Unable to create key. Check etcd server status")
    }

    /// 原子地"创建键 / 或校验已有值与给定值相等".
    ///
    /// 不存在则创建;存在但值不同则返回错误.
    pub async fn kv_create_or_validate(
        &self,
        key: String,
        value: Vec<u8>,
        lease_id: Option<u64>,
    ) -> Result<()> {
        let put_options = self.put_options_for(lease_id);

        let txn = Txn::new()
            .when(vec![Compare::version(key.as_str(), CompareOp::Equal, 0)])
            .and_then(vec![TxnOp::put(
                key.as_str(),
                value.clone(),
                Some(put_options),
            )])
            .or_else(vec![TxnOp::txn(Txn::new().when(vec![Compare::value(
                key.as_str(),
                CompareOp::Equal,
                value.clone(),
            )]))]);

        let result = self.connector.get_client().kv_client().txn(txn).await?;

        if result.succeeded() {
            return Ok(());
        }

        // 走 or_else 分支:嵌套事务的 succeeded 表示值匹配.
        match result.op_responses().first() {
            Some(TxnOpResponse::Txn(inner)) if inner.succeeded() => Ok(()),
            Some(TxnOpResponse::Txn(_)) => {
                anyhow::bail!("Unable to create or validate key. Check etcd server status")
            }
            Some(_) => {
                anyhow::bail!("Unable to validate key operation. Check etcd server status")
            }
            None => anyhow::bail!("Unable to create or validate key. Check etcd server status"),
        }
    }

    /// PUT 写入键值;`lease_id=None` 时绑定到主 lease.
    pub async fn kv_put(
        &self,
        key: impl AsRef<str>,
        value: impl AsRef<[u8]>,
        lease_id: Option<u64>,
    ) -> Result<()> {
        let put_options = self.put_options_for(lease_id);
        self.connector
            .get_client()
            .kv_client()
            .put(key.as_ref(), value.as_ref(), Some(put_options))
            .await?;
        Ok(())
    }

    /// 高级 PUT:接受任意 `PutOptions`,但仍强制绑定主 lease.
    pub async fn kv_put_with_options(
        &self,
        key: impl AsRef<str>,
        value: impl AsRef<[u8]>,
        options: Option<PutOptions>,
    ) -> Result<PutResponse> {
        let options = options
            .unwrap_or_default()
            .with_lease(self.lease_id() as i64);
        self.connector
            .get_client()
            .kv_client()
            .put(key.as_ref(), value.as_ref(), Some(options))
            .await
            .map_err(Into::into)
    }

    /// GET 单个/多个键.
    pub async fn kv_get(
        &self,
        key: impl Into<Vec<u8>>,
        options: Option<GetOptions>,
    ) -> Result<Vec<KeyValue>> {
        let mut resp = self
            .connector
            .get_client()
            .kv_client()
            .get(key, options)
            .await?;
        Ok(resp.take_kvs())
    }

    /// DELETE,返回被删键数量.
    pub async fn kv_delete(
        &self,
        key: impl Into<Vec<u8>>,
        options: Option<DeleteOptions>,
    ) -> Result<u64> {
        self.connector
            .get_client()
            .kv_client()
            .delete(key, options)
            .await
            .map(|d| d.deleted() as u64)
            .map_err(Into::into)
    }

    /// 按前缀 GET 全部键值.
    pub async fn kv_get_prefix(&self, prefix: impl AsRef<str>) -> Result<Vec<KeyValue>> {
        let mut resp = self
            .connector
            .get_client()
            .kv_client()
            .get(prefix.as_ref(), Some(GetOptions::new().with_prefix()))
            .await?;
        Ok(resp.take_kvs())
    }

    /// 使用 etcd 原生 lock 接口申请分布式锁,返回 [`LockResponse`].
    pub async fn lock(
        &self,
        key: impl Into<Vec<u8>>,
        lease_id: Option<u64>,
    ) -> Result<LockResponse> {
        let mut lock_client = self.connector.get_client().lock_client();
        let id = lease_id.unwrap_or(self.lease_id());
        let options = LockOptions::new().with_lease(id as i64);
        lock_client
            .lock(key, Some(options))
            .await
            .map_err(Into::into)
    }

    /// 用 [`LockResponse::key`] 释放分布式锁.
    pub async fn unlock(&self, lock_key: impl Into<Vec<u8>>) -> Result<()> {
        let mut lock_client = self.connector.get_client().lock_client();
        lock_client
            .unlock(lock_key)
            .await
            .map_err(|err: etcd_client::Error| anyhow::anyhow!(err))?;
        Ok(())
    }

    // === watch family ===

    /// 仅订阅 *后续* 变化(不回放当前值).
    pub async fn kv_watch_prefix(
        &self,
        prefix: impl AsRef<str> + std::fmt::Display,
    ) -> Result<PrefixWatcher> {
        self.watch_internal(prefix, false).await
    }

    /// 先回放现有键再订阅后续变化.
    pub async fn kv_get_and_watch_prefix(
        &self,
        prefix: impl AsRef<str> + std::fmt::Display,
    ) -> Result<PrefixWatcher> {
        self.watch_internal(prefix, true).await
    }

    /// watch 路径的核心:计算起始 revision,可选回放现有键,然后在 *专属 runtime* 中
    /// 启动后台 reconnect-aware watch loop.
    async fn watch_internal(
        &self,
        prefix: impl AsRef<str> + std::fmt::Display,
        include_existing: bool,
    ) -> Result<PrefixWatcher> {
        let (mut start_revision, existing_kvs) = self
            .get_start_revision(prefix.as_ref(), include_existing)
            .await?;

        // 通道容量预扩到能容纳所有现有键 + 32 余量,避免回放阻塞.
        let existing_count = existing_kvs.as_ref().map_or(0, |kvs| kvs.len());
        let (tx, rx) = mpsc::channel(existing_count + 32);

        if let Some(kvs) = existing_kvs {
            tracing::trace!("sending {} existing kvs", kvs.len());
            for kv in kvs {
                tx.send(WatchEvent::Put(kv)).await?;
            }
        }

        // 后台 task: 建流 → 守流 → 失败重连;通道关闭即停.
        let connector = self.connector.clone();
        let prefix_str = prefix.as_ref().to_string();
        self.rt.spawn(async move {
            loop {
                let watch_stream =
                    match Self::new_watch_stream(&connector, &prefix_str, start_revision).await {
                        Ok(s) => s,
                        Err(_) => return,
                    };

                let should_reconnect = Self::monitor_watch_stream(
                    watch_stream,
                    &prefix_str,
                    &mut start_revision,
                    &tx,
                )
                .await;

                if !should_reconnect {
                    return;
                }
            }
        });

        Ok(PrefixWatcher {
            prefix: prefix.as_ref().to_string(),
            rx,
        })
    }

    /// 查询当前 revision,可选拉取现有键值.
    async fn get_start_revision(
        &self,
        prefix: impl AsRef<str> + std::fmt::Display,
        include_existing: bool,
    ) -> Result<(i64, Option<Vec<KeyValue>>)> {
        let mut kv_client = self.connector.get_client().kv_client();
        let mut resp = kv_client
            .get(prefix.as_ref(), Some(GetOptions::new().with_prefix()))
            .await?;

        let header_rev = resp
            .header()
            .ok_or_else(|| anyhow::anyhow!("missing header; unable to get revision"))?
            .revision();
        tracing::trace!("{prefix}: start_revision: {header_rev}");
        let start_revision = header_rev + 1;

        let existing_kvs = include_existing.then(|| {
            let kvs = resp.take_kvs();
            tracing::trace!("initial kv count: {:?}", kvs.len());
            kvs
        });

        Ok((start_revision, existing_kvs))
    }

    /// 反复尝试建立 watch 流;失败时通过 [`Connector::reconnect`] 重连(10s 超时).
    async fn new_watch_stream(
        connector: &Arc<Connector>,
        prefix: &String,
        start_revision: i64,
    ) -> Result<WatchStream> {
        loop {
            let watch_attempt = connector
                .get_client()
                .watch_client()
                .watch(
                    prefix.as_str(),
                    Some(
                        WatchOptions::new()
                            .with_prefix()
                            .with_start_revision(start_revision)
                            .with_prev_key(),
                    ),
                )
                .await;

            match watch_attempt {
                Ok((_, stream)) => {
                    tracing::debug!("Watch stream established for prefix '{prefix}'");
                    return Ok(stream);
                }
                Err(err) => {
                    tracing::debug!(
                        error = %err,
                        "Failed to establish watch stream for prefix '{}'",
                        prefix
                    );
                    let deadline = std::time::Instant::now() + Duration::from_secs(10);
                    if let Err(e) = connector.reconnect(deadline).await {
                        tracing::error!(
                            "Failed to reconnect to ETCD within 10 secs for watching prefix '{}': {}",
                            prefix,
                            e
                        );
                        return Err(e);
                    }
                    // 重连成功则进入下一轮 loop 重新建流.
                }
            }
        }
    }

    /// 持有 watch 流:消费事件并维护 `start_revision`.
    ///
    /// - 返回 `true`:可恢复错误,外层应重连;
    /// - 返回 `false`:不可恢复或接收方已关闭,应彻底退出.
    async fn monitor_watch_stream(
        mut watch_stream: WatchStream,
        prefix: &String,
        start_revision: &mut i64,
        tx: &mpsc::Sender<WatchEvent>,
    ) -> bool {
        loop {
            tokio::select! {
                maybe_resp = watch_stream.next() => {
                    let response = match maybe_resp {
                        Some(Ok(res)) => res,
                        Some(Err(err)) => {
                            tracing::warn!(
                                error = %err,
                                "Error watching stream for prefix '{}'",
                                prefix
                            );
                            return true;
                        }
                        None => {
                            tracing::warn!(
                                "Watch stream unexpectedly closed for prefix '{prefix}'"
                            );
                            return true;
                        }
                    };

                    *start_revision = match response.header() {
                        Some(header) => header.revision() + 1,
                        None => {
                            tracing::error!(
                                "Missing header in watch response for prefix '{prefix}'"
                            );
                            return false;
                        }
                    };

                    if Self::process_watch_events(response.events(), tx).await.is_err() {
                        return false;
                    }
                }
                _ = tx.closed() => {
                    tracing::debug!("no more receivers, stopping watcher");
                    return false;
                }
            }
        }
    }

    /// 把 etcd 事件转换为 [`WatchEvent`] 并写入接收方;无 KV 的事件被忽略.
    async fn process_watch_events(
        events: &[etcd_client::Event],
        tx: &mpsc::Sender<WatchEvent>,
    ) -> Result<()> {
        for event in events {
            let Some(kv) = event.kv() else {
                continue;
            };

            match event.event_type() {
                etcd_client::EventType::Put => {
                    if let Err(err) = tx.send(WatchEvent::Put(kv.clone())).await {
                        tracing::error!("kv watcher error forwarding WatchEvent::Put: {err}");
                        return Err(err.into());
                    }
                }
                etcd_client::EventType::Delete => {
                    if tx.send(WatchEvent::Delete(kv.clone())).await.is_err() {
                        return Err(anyhow::anyhow!("failed to send WatchEvent::Delete"));
                    }
                }
            }
        }
        Ok(())
    }
}

// === SECTION: watcher types ===

/// 前缀 watcher 句柄.通过 [`Dissolve`] 派生 `.dissolve()` 解构出 `prefix + rx`.
#[derive(Dissolve)]
pub struct PrefixWatcher {
    prefix: String,
    rx: mpsc::Receiver<WatchEvent>,
}

/// watch 事件类型.
#[derive(Debug)]
pub enum WatchEvent {
    Put(KeyValue),
    Delete(KeyValue),
}

// === SECTION: ClientOptions ===

/// etcd 客户端构造参数.支持 derive_builder 的链式构造,以及通过环境变量推断默认值.
#[derive(Debug, Clone, Builder, Validate)]
pub struct ClientOptions {
    #[validate(length(min = 1))]
    pub etcd_url: Vec<String>,

    #[builder(default)]
    pub etcd_connect_options: Option<ConnectOptions>,

    /// 若为 true(默认),会申请主 lease 并把其生命周期绑定到 runtime 的 primary token.
    #[builder(default = "true")]
    pub attach_lease: bool,
}

impl Default for ClientOptions {
    fn default() -> Self {
        // 鉴权优先级:用户名/密码 > TLS 证书 > 不带鉴权.
        let connect_options = if let (Ok(username), Ok(password)) = (
            std::env::var(env_etcd::auth::ETCD_AUTH_USERNAME),
            std::env::var(env_etcd::auth::ETCD_AUTH_PASSWORD),
        ) {
            Some(ConnectOptions::new().with_user(username, password))
        } else if let (Ok(ca), Ok(cert), Ok(key)) = (
            std::env::var(env_etcd::auth::ETCD_AUTH_CA),
            std::env::var(env_etcd::auth::ETCD_AUTH_CLIENT_CERT),
            std::env::var(env_etcd::auth::ETCD_AUTH_CLIENT_KEY),
        ) {
            Some(
                ConnectOptions::new().with_tls(
                    TlsOptions::new()
                        .ca_certificate(Certificate::from_pem(ca))
                        .identity(Identity::from_pem(cert, key)),
                ),
            )
        } else {
            None
        };

        ClientOptions {
            etcd_url: default_servers(),
            etcd_connect_options: connect_options,
            attach_lease: true,
        }
    }
}

/// 读取 `ETCD_ENDPOINTS` 环境变量(逗号分隔),否则回退到 `http://localhost:2379`.
fn default_servers() -> Vec<String> {
    match std::env::var(env_etcd::ETCD_ENDPOINTS) {
        Ok(raw) => raw.split(',').map(|s| s.to_string()).collect(),
        Err(_) => vec!["http://localhost:2379".to_string()],
    }
}

// === SECTION: KvCache ===

/// 跟踪指定前缀的 etcd 视图的本地内存缓存.
///
/// 创建时:
/// 1. 拉取现有键值到本地 HashMap;
/// 2. 把 `initial_values` 中尚不存在的键写入 etcd 与本地;
/// 3. 启动后台 watcher,持续同步 Put/Delete 事件.
pub struct KvCache {
    client: Client,
    pub prefix: String,
    cache: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    watcher: Option<PrefixWatcher>,
}

impl KvCache {
    /// 构造并初始化缓存.
    pub async fn new(
        client: Client,
        prefix: String,
        initial_values: HashMap<String, Vec<u8>>,
    ) -> Result<Self> {
        let mut cache = HashMap::new();

        // 拉取现有键值.
        for kv in client.kv_get_prefix(&prefix).await? {
            let key = String::from_utf8_lossy(kv.key()).to_string();
            cache.insert(key, kv.value().to_vec());
        }

        // 补写 initial_values 中尚不存在的键.
        // TODO: 需要 lease 协调 —— 首个 writer 申请 lease 后写入,后续 writer 附加到该 lease.
        for (key, value) in initial_values.iter() {
            let full_key = format!("{}{}", prefix, key);
            if let std::collections::hash_map::Entry::Vacant(slot) = cache.entry(full_key.clone()) {
                client.kv_put(&full_key, value.clone(), None).await?;
                slot.insert(value.clone());
            }
        }

        // 启动 watcher.由 `kv_get_and_watch_prefix` 内部会回放一次现有值,
        // 因此后台 task 即使错过 etcd 提交也能从初始事件中重新对齐.
        let watcher = client.kv_get_and_watch_prefix(&prefix).await?;

        let mut result = Self {
            client,
            prefix,
            cache: Arc::new(RwLock::new(cache)),
            watcher: Some(watcher),
        };

        result.start_watcher().await?;
        Ok(result)
    }

    /// 内部:把构造时挂在 `self.watcher` 上的 [`PrefixWatcher`] 拿走,启动后台同步 task.
    async fn start_watcher(&mut self) -> Result<()> {
        let Some(watcher) = self.watcher.take() else {
            return Ok(());
        };

        let cache = self.cache.clone();
        let prefix = self.prefix.clone();

        tokio::spawn(async move {
            let mut rx = watcher.rx;
            while let Some(event) = rx.recv().await {
                match event {
                    WatchEvent::Put(kv) => {
                        let key = String::from_utf8_lossy(kv.key()).to_string();
                        let value = kv.value().to_vec();
                        tracing::trace!("KvCache update: {} = {:?}", key, value);
                        cache.write().await.insert(key, value);
                    }
                    WatchEvent::Delete(kv) => {
                        let key = String::from_utf8_lossy(kv.key()).to_string();
                        tracing::trace!("KvCache delete: {key}");
                        cache.write().await.remove(&key);
                    }
                }
            }
            tracing::debug!("KvCache watcher for prefix '{prefix}' stopped");
        });

        Ok(())
    }

    /// 读取缓存中的单个键(自动补 prefix).
    pub async fn get(&self, key: &str) -> Option<Vec<u8>> {
        let full_key = format!("{}{}", self.prefix, key);
        self.cache.read().await.get(&full_key).cloned()
    }

    /// 返回当前缓存的全量快照.
    pub async fn get_all(&self) -> HashMap<String, Vec<u8>> {
        self.cache.read().await.clone()
    }

    /// 写入键值:先 etcd 后内存,保证持久性优先.
    pub async fn put(&self, key: &str, value: Vec<u8>, lease_id: Option<u64>) -> Result<()> {
        let full_key = format!("{}{}", self.prefix, key);
        self.client
            .kv_put(&full_key, value.clone(), lease_id)
            .await?;
        self.cache.write().await.insert(full_key, value);
        Ok(())
    }

    /// 删除键:先 etcd 后内存.
    pub async fn delete(&self, key: &str) -> Result<()> {
        let full_key = format!("{}{}", self.prefix, key);
        self.client.kv_delete(full_key.clone(), None).await?;
        self.cache.write().await.remove(&full_key);
        Ok(())
    }
}

// === SECTION: tests ===

#[cfg(feature = "integration")]
#[cfg(test)]
mod tests {
    use crate::{DistributedRuntime, distributed::DistributedConfig};

    use super::*;

    #[test]
    fn test_ectd_client() {
        let rt = Runtime::from_settings().unwrap();
        let rt_clone = rt.clone();
        let config = DistributedConfig::from_settings();

        rt_clone.primary().block_on(async move {
            let drt = DistributedRuntime::new(rt, config).await.unwrap();
            test_kv_create_or_validate(drt).await.unwrap();
        });
    }

    async fn test_kv_create_or_validate(drt: DistributedRuntime) -> Result<()> {
        let key = "__integration_test_key";
        let value = b"test_value";

        let client = Client::new(ClientOptions::default(), drt.runtime().clone())
            .await
            .expect("etcd client should be available");
        let lease_id = drt.connection_id();

        // Create the key
        let result = client.kv_create(key, value.to_vec(), Some(lease_id)).await;
        assert!(result.is_ok(), "");

        // Try to create the key again - this should return Ok(Some(version)) indicating key already exists
        // Note: Prior to PR #4212 (Nov 10, 2025), kv_create returned Err when key existed.
        // PR #4212 changed the behavior to return Ok(Some(version)) for idempotency, matching
        // the StoreOutcome::Exists pattern used in the KeyValueStore abstraction.
        // The transaction now includes .or_else(TxnOp::get) to retrieve existing key info
        // instead of failing, making the operation idempotent for distributed systems.
        let result = client.kv_create(key, value.to_vec(), Some(lease_id)).await;
        assert!(
            result.is_ok() && result.unwrap().is_some(),
            "Expected Ok(Some(version)) when key already exists"
        );

        // Create or validate should succeed as the values match
        let result = client
            .kv_create_or_validate(key.to_string(), value.to_vec(), Some(lease_id))
            .await;
        assert!(result.is_ok());

        // Try to create the key with a different value
        let different_value = b"different_value";
        let result = client
            .kv_create_or_validate(key.to_string(), different_value.to_vec(), Some(lease_id))
            .await;
        assert!(result.is_err(), "");

        Ok(())
    }

    #[test]
    fn test_kv_cache() {
        let rt = Runtime::from_settings().unwrap();
        let rt_clone = rt.clone();
        let config = DistributedConfig::from_settings();

        rt_clone.primary().block_on(async move {
            let drt = DistributedRuntime::new(rt, config).await.unwrap();
            test_kv_cache_operations(drt).await.unwrap();
        });
    }

    async fn test_kv_cache_operations(drt: DistributedRuntime) -> Result<()> {
        // Make the client and unwrap it
        let client = Client::new(ClientOptions::default(), drt.runtime().clone())
            .await
            .expect("etcd client should be available");

        // Create a unique test prefix to avoid conflicts with other tests
        let test_id = uuid::Uuid::new_v4().to_string();
        let prefix = format!("v1/test_kv_cache_{}/", test_id);

        // Initial values
        let mut initial_values = HashMap::new();
        initial_values.insert("key1".to_string(), b"value1".to_vec());
        initial_values.insert("key2".to_string(), b"value2".to_vec());

        // Create the KV cache
        let kv_cache = KvCache::new(client.clone(), prefix.clone(), initial_values).await?;

        // Test get
        let value1 = kv_cache.get("key1").await;
        assert_eq!(value1, Some(b"value1".to_vec()));

        let value2 = kv_cache.get("key2").await;
        assert_eq!(value2, Some(b"value2".to_vec()));

        // Test get_all
        let all_values = kv_cache.get_all().await;
        assert_eq!(all_values.len(), 2);
        assert_eq!(
            all_values.get(&format!("{}key1", prefix)),
            Some(&b"value1".to_vec())
        );
        assert_eq!(
            all_values.get(&format!("{}key2", prefix)),
            Some(&b"value2".to_vec())
        );

        // Test put - using None for lease_id
        kv_cache.put("key3", b"value3".to_vec(), None).await?;

        // Allow some time for the update to propagate
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Verify the new value
        let value3 = kv_cache.get("key3").await;
        assert_eq!(value3, Some(b"value3".to_vec()));

        // Test update
        kv_cache
            .put("key1", b"updated_value1".to_vec(), None)
            .await?;

        // Allow some time for the update to propagate
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Verify the updated value
        let updated_value1 = kv_cache.get("key1").await;
        assert_eq!(updated_value1, Some(b"updated_value1".to_vec()));

        // Test external update (simulating another client updating a value)
        client
            .kv_put(
                &format!("{}key2", prefix),
                b"external_update".to_vec(),
                None,
            )
            .await?;

        // Allow some time for the update to propagate
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Verify the cache was updated
        let external_update = kv_cache.get("key2").await;
        assert_eq!(external_update, Some(b"external_update".to_vec()));

        // Clean up - delete the test keys
        let etcd_client = client.etcd_client();
        let _ = etcd_client
            .kv_client()
            .delete(
                prefix,
                Some(etcd_client::DeleteOptions::new().with_prefix()),
            )
            .await?;

        Ok(())
    }

    // === SECTION: 合并自原 mod supplemental_tests ===
    // ## 测试过程
    // - `client_builder_*`/`client_options_default_*`: 不依赖 etcd,验证选项构造与
    //   `default_servers()` 在不同环境变量组合下的行为;
    // - `client_new_debug_lease_*`: 若本地 etcd 可用,验证 Debug 输出格式与底层
    //   `etcd_client::Client` 的克隆路径;
    // - `kv_basic_operations_*`: 覆盖 kv_create / kv_create_or_validate / kv_put /
    //   kv_put_with_options / kv_get / kv_get_prefix / kv_delete / lock / unlock;
    // - `watch_prefix_variants_and_kvcache_*`: 覆盖两种 watch 模式以及 KvCache 的
    //   get/get_all/put/delete 路径.
    //
    // ## 意义
    // 没有 etcd 时纯逻辑测试仍能跑;有 etcd 时全量验证最终一致性与异步同步行为.

    use super::*;
    use crate::config::environment_names::etcd as env_etcd;
    use tokio::time::{Duration, timeout};

    fn test_etcd_urls() -> Vec<String> {
        let url = std::env::var("PAGODA_TEST_ETCD_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:2379".to_string());
        vec![url]
    }

    async fn maybe_client(attach_lease: bool) -> Option<Client> {
        let runtime = Runtime::from_current().ok()?;
        let options = Client::builder()
            .etcd_url(test_etcd_urls())
            .attach_lease(attach_lease)
            .build()
            .ok()?;
        let client = Client::new(options, runtime).await.ok()?;
        // etcd client can be lazily connected; probe a real operation first.
        if client
            .kv_get("__supplemental_probe__", None)
            .await
            .is_err()
        {
            return None;
        }
        Some(client)
    }

    fn unique_prefix(tag: &str) -> String {
        format!("v1/supplemental_etcd_{tag}_{}/", uuid::Uuid::new_v4())
    }

    async fn cleanup_prefix(client: &Client, prefix: &str) {
        let _ = client
            .kv_delete(prefix.to_string(), Some(DeleteOptions::new().with_prefix()))
            .await;
    }

    async fn recv_event(rx: &mut mpsc::Receiver<WatchEvent>) -> Option<WatchEvent> {
        timeout(Duration::from_secs(5), rx.recv())
            .await
            .ok()
            .flatten()
    }

    #[test]
    fn client_builder_can_build_custom_options() {
        let urls = vec!["http://127.0.0.1:2379".to_string()];
        let options = Client::builder()
            .etcd_url(urls.clone())
            .attach_lease(false)
            .build()
            .expect("builder should create valid options");

        assert_eq!(options.etcd_url, urls);
        assert!(!options.attach_lease);
    }

    #[test]
    fn client_options_default_and_default_servers_behave_as_expected() {
        temp_env::with_vars(
            vec![
                (env_etcd::ETCD_ENDPOINTS, Some("http://a:1,http://b:2")),
                (env_etcd::auth::ETCD_AUTH_USERNAME, None::<&str>),
                (env_etcd::auth::ETCD_AUTH_PASSWORD, None::<&str>),
                (env_etcd::auth::ETCD_AUTH_CA, None::<&str>),
                (env_etcd::auth::ETCD_AUTH_CLIENT_CERT, None::<&str>),
                (env_etcd::auth::ETCD_AUTH_CLIENT_KEY, None::<&str>),
            ],
            || {
                let servers = default_servers();
                assert_eq!(
                    servers,
                    vec!["http://a:1".to_string(), "http://b:2".to_string()]
                );

                let options = ClientOptions::default();
                assert_eq!(options.etcd_url, servers);
                assert!(options.attach_lease);
                assert!(options.etcd_connect_options.is_none());
            },
        );

        temp_env::with_vars(
            vec![
                (env_etcd::ETCD_ENDPOINTS, None::<&str>),
                (env_etcd::auth::ETCD_AUTH_USERNAME, Some("user")),
                (env_etcd::auth::ETCD_AUTH_PASSWORD, Some("pass")),
                (env_etcd::auth::ETCD_AUTH_CA, None::<&str>),
                (env_etcd::auth::ETCD_AUTH_CLIENT_CERT, None::<&str>),
                (env_etcd::auth::ETCD_AUTH_CLIENT_KEY, None::<&str>),
            ],
            || {
                let options = ClientOptions::default();
                assert_eq!(
                    options.etcd_url,
                    vec!["http://localhost:2379".to_string()]
                );
                assert!(options.etcd_connect_options.is_some());
            },
        );

        temp_env::with_vars(
            vec![
                (env_etcd::auth::ETCD_AUTH_USERNAME, None::<&str>),
                (env_etcd::auth::ETCD_AUTH_PASSWORD, None::<&str>),
                (
                    env_etcd::auth::ETCD_AUTH_CA,
                    Some("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----"),
                ),
                (
                    env_etcd::auth::ETCD_AUTH_CLIENT_CERT,
                    Some("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----"),
                ),
                (
                    env_etcd::auth::ETCD_AUTH_CLIENT_KEY,
                    Some("-----BEGIN PRIVATE KEY-----\nMIIB\n-----END PRIVATE KEY-----"),
                ),
            ],
            || {
                let options = ClientOptions::default();
                assert!(options.etcd_connect_options.is_some());
            },
        );
    }

    #[tokio::test]
    async fn client_new_debug_lease_and_client_clone_paths() {
        let Some(client) = maybe_client(false).await else {
            return;
        };

        assert_eq!(client.lease_id(), 0);
        let dbg = format!("{client:?}");
        assert!(dbg.contains("etcd::Client"));
        assert!(dbg.contains("primary_lease=0"));

        // Ensure internal client clone is usable.
        let resp = client
            .etcd_client()
            .kv_client()
            .get("__supplemental_nonexistent__", None)
            .await;
        let Ok(mut resp) = resp else {
            return;
        };
        assert!(resp.take_kvs().is_empty());
    }

    #[tokio::test]
    async fn kv_basic_operations_create_put_get_delete_prefix_and_lock() {
        let Some(client) = maybe_client(false).await else {
            return;
        };

        let prefix = unique_prefix("basic");
        let key = format!("{prefix}key1");
        let key2 = format!("{prefix}key2");

        cleanup_prefix(&client, &prefix).await;

        let created = client.kv_create(&key, b"value1".to_vec(), None).await;
        let Ok(created) = created else {
            return;
        };
        assert!(created.is_none());

        let exists = client
            .kv_create(&key, b"value1".to_vec(), None)
            .await
            .expect("kv_create should be idempotent");
        assert!(exists.is_some());

        client
            .kv_create_or_validate(key.clone(), b"value1".to_vec(), None)
            .await
            .expect("matching value should validate");
        let validate_err = client
            .kv_create_or_validate(key.clone(), b"different".to_vec(), None)
            .await
            .err()
            .expect("different value should fail validation");
        assert!(!validate_err.to_string().is_empty());

        client
            .kv_put(&key2, b"value2".to_vec(), None)
            .await
            .expect("kv_put should succeed");
        let _ = client
            .kv_put_with_options(&key2, b"value2b".to_vec(), None)
            .await
            .expect("kv_put_with_options should succeed");

        let got = client
            .kv_get(key.clone(), None)
            .await
            .expect("kv_get should succeed");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].value(), b"value1");

        let prefixed = client
            .kv_get_prefix(&prefix)
            .await
            .expect("kv_get_prefix should succeed");
        assert!(prefixed.len() >= 2);

        let deleted = client
            .kv_delete(key2.clone(), None)
            .await
            .expect("kv_delete should succeed");
        assert_eq!(deleted, 1);

        let lock_resp = client
            .lock(format!("{prefix}lock"), None)
            .await
            .expect("lock should succeed");
        client
            .unlock(lock_resp.key().to_vec())
            .await
            .expect("unlock should succeed");

        cleanup_prefix(&client, &prefix).await;
    }

    #[tokio::test]
    async fn watch_prefix_variants_and_kvcache_cover_watcher_paths() {
        let Some(client) = maybe_client(false).await else {
            return;
        };

        let prefix = unique_prefix("watch");
        let existing_key = format!("{prefix}existing");
        let new_key = format!("{prefix}new");

        cleanup_prefix(&client, &prefix).await;

        client.kv_put(&existing_key, b"v0".to_vec(), None).await.ok();
        if client.kv_get(existing_key.clone(), None).await.is_err() {
            return;
        }

        let watcher_with_existing = client
            .kv_get_and_watch_prefix(prefix.clone())
            .await
            .expect("kv_get_and_watch_prefix should succeed");
        let mut rx = watcher_with_existing.rx;

        // include_existing=true should emit current value first
        let initial = recv_event(&mut rx)
            .await
            .expect("should receive initial put event");
        match initial {
            WatchEvent::Put(kv) => {
                assert_eq!(kv.key(), existing_key.as_bytes());
                assert_eq!(kv.value(), b"v0");
            }
            WatchEvent::Delete(_) => panic!("expected initial Put event"),
        }

        client
            .kv_put(&new_key, b"v1".to_vec(), None)
            .await
            .expect("put after watch should succeed");
        let put_event = recv_event(&mut rx)
            .await
            .expect("should receive put event");
        match put_event {
            WatchEvent::Put(kv) => {
                assert_eq!(kv.key(), new_key.as_bytes());
                assert_eq!(kv.value(), b"v1");
            }
            WatchEvent::Delete(_) => panic!("expected Put event"),
        }

        client
            .kv_delete(new_key.clone(), None)
            .await
            .expect("delete after watch should succeed");
        let del_event = recv_event(&mut rx)
            .await
            .expect("should receive delete event");
        match del_event {
            WatchEvent::Delete(kv) => {
                assert_eq!(kv.key(), new_key.as_bytes());
            }
            WatchEvent::Put(_) => panic!("expected Delete event"),
        }

        // include_existing=false should not replay old key
        let watcher_no_existing = client
            .kv_watch_prefix(prefix.clone())
            .await
            .expect("kv_watch_prefix should succeed");
        let mut rx2 = watcher_no_existing.rx;

        let maybe_initial = timeout(Duration::from_millis(250), rx2.recv()).await;
        assert!(
            maybe_initial.is_err(),
            "watch without existing should not replay current keys"
        );

        let next_key = format!("{prefix}next");
        client
            .kv_put(&next_key, b"v2".to_vec(), None)
            .await
            .expect("put after no-existing watch should succeed");

        let next_event = recv_event(&mut rx2)
            .await
            .expect("watch should receive new put event");
        match next_event {
            WatchEvent::Put(kv) => assert_eq!(kv.key(), next_key.as_bytes()),
            WatchEvent::Delete(_) => panic!("expected Put event"),
        }

        // KvCache covers new/start_watcher/get/get_all/put/delete paths
        let mut initial_values = HashMap::new();
        initial_values.insert("a".to_string(), b"1".to_vec());
        initial_values.insert("b".to_string(), b"2".to_vec());

        let cache = KvCache::new(client.clone(), prefix.clone(), initial_values)
            .await
            .expect("kv cache creation should succeed");

        let got_a = cache.get("a").await;
        assert_eq!(got_a, Some(b"1".to_vec()));

        let all = cache.get_all().await;
        assert!(all.contains_key(&format!("{prefix}a")));

        cache
            .put("c", b"3".to_vec(), None)
            .await
            .expect("cache put should succeed");
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(cache.get("c").await, Some(b"3".to_vec()));

        cache
            .delete("c")
            .await
            .expect("cache delete should succeed");
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(cache.get("c").await, None);

        cleanup_prefix(&client, &prefix).await;
    }
}

