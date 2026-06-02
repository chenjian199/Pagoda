// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 设计意图
//! 本模块封装 NATS 客户端与 JetStream 队列,提供给整个 runtime 的统一 NATS 访问层.
//! 包含三个层级:
//!   1. [`Client`] — async-nats 的 `client::Client` + `jetstream::Context` 双手柄,
//!      额外暴露 object store 上传/下载与 service scrape 等高层能力;
//!   2. [`ClientOptions`] / [`NatsAuth`] — 鉴权与连接参数,优先从环境变量推断;
//!   3. [`NatsQueue`] — 基于 JetStream 的命名队列,支持广播消费/独占消费/纯发布三种形态.
//!
//! # 外部契约
//! - 公共 API 完全对齐,包括方法签名/类型暴露/再导出;
//! - 环境变量驱动的默认值集中在 [`crate::config::environment_names::nats`];
//! - 常量 [`URL_PREFIX`] = `"nats://"` 是 wire-level 协议前缀,不可改;
//! - 鉴权优先级(由 [`NatsAuth::default`] 实现):
//!     `NATS_AUTH_USERNAME + NATS_AUTH_PASSWORD` > `NATS_AUTH_TOKEN` >
//!     `NATS_AUTH_NKEY` > `NATS_AUTH_CREDENTIALS_FILE` > 默认匿名 `user/user`;
//! - 私有 `default_server()` / `validate_nats_server()` 被同模块 supplemental 测试访问;
//! - [`NatsQueue`] 各字段名 (`stream_name`/`subject`/`consumer_name`/`client`/`message_stream`)
//!   被测试直接读写,不能改名;
//! - [`instance_subject`] 拼装格式 `"{namespace}_{servicegroup}.{name}-{instance:x}"` 是协议级承诺.
//!
//! # 实现要点
//! - 连接走 [`build_in_runtime`] 在独立 runtime 中跑,避免 NATS IO 抢占业务调度;
//! - object store 上传走 streaming reader,大文件不会占满内存;`*_data` 系列变体用 bincode
//!   做结构化序列化;
//! - [`NatsQueue::shutdown`] 区分"删除自己的 consumer"(顺带 close)与"删除别人的 consumer"
//!   (不影响本地连接);
//! - [`NatsQueue::purge_acknowledged`] 计算 *跨 consumer* 的最小 ack_floor 后再 purge,
//!   避免误删未消费的消息.

use crate::metrics::MetricsHierarchy;
use crate::protocols::PortNameId;

use anyhow::Result;
use async_nats::connection::State;
use async_nats::{Subscriber, client, jetstream};
use async_trait::async_trait;
use bytes::Bytes;
use derive_builder::Builder;
use futures::{StreamExt, TryStreamExt};
use prometheus::{Counter, Gauge, Histogram, HistogramOpts, IntCounter, IntGauge, Opts, Registry};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use tokio::fs::File as TokioFile;
use tokio::io::AsyncRead;
use tokio::time;
use url::Url;
use validator::{Validate, ValidationError};

use crate::config::environment_names::nats as env_nats;
pub use crate::slug::Slug;
use tracing as log;

use super::utils::build_in_runtime;

// === SECTION: constants ===

/// NATS URL 协议前缀.
pub const URL_PREFIX: &str = "nats://";

/// 连接 NATS 时为独立 runtime 分配的 worker 线程数.
// TODO(jthomson04): 这个值理想情况下应当可配置。
const NATS_WORKER_THREADS: usize = 4;

// === SECTION: Client ===

/// NATS 客户端外观,聚合 `async_nats::client::Client` 与 `jetstream::Context`.
#[derive(Clone)]
pub struct Client {
    client: client::Client,
    js_ctx: jetstream::Context,
}

impl Client {
    /// 返回 NATS [`ClientOptionsBuilder`].
    pub fn builder() -> ClientOptionsBuilder {
        ClientOptionsBuilder::default()
    }

    /// 底层 async-nats 客户端的只读引用.
    pub fn client(&self) -> &client::Client {
        &self.client
    }

    /// 底层 JetStream 上下文的只读引用.
    pub fn jetstream(&self) -> &jetstream::Context {
        &self.js_ctx
    }

    /// 当前连接的 host:port.
    pub fn addr(&self) -> String {
        let info = self.client.server_info();
        format!("{}:{}", info.host, info.port)
    }

    /// 返回所有 JetStream stream 的名称列表.
    pub async fn list_streams(&self) -> Result<Vec<String>> {
        let names: Vec<String> = self.js_ctx.stream_names().try_collect().await?;
        Ok(names)
    }

    /// 返回指定 stream 下所有 consumer 的名称.
    pub async fn list_consumers(&self, stream_name: &str) -> Result<Vec<String>> {
        let stream = self.js_ctx.get_stream(stream_name).await?;
        let consumers: Vec<String> = stream.consumer_names().try_collect().await?;
        Ok(consumers)
    }

    /// 获取 stream 的运行时状态.
    pub async fn stream_info(&self, stream_name: &str) -> Result<jetstream::stream::State> {
        let mut stream = self.js_ctx.get_stream(stream_name).await?;
        let info = stream.info().await?;
        Ok(info.state.clone())
    }

    /// 直接获取 stream 句柄.
    pub async fn get_stream(&self, name: &str) -> Result<jetstream::stream::Stream> {
        Ok(self.js_ctx.get_stream(name).await?)
    }

    /// 发起一次 service stats 广播请求并返回 reply 订阅.
    ///
    /// 每个服务只回一次,调用方需在适当时机 drop 订阅,否则会永久等待.
    pub async fn scrape_service(&self, service_name: &str) -> Result<Subscriber> {
        let subject = format!("$SRV.STATS.{}", service_name);
        let reply_subject = format!("_INBOX.{}", nuid::next());
        let subscription = self.client.subscribe(reply_subject.clone()).await?;

        self.client
            .publish_with_reply(subject, reply_subject, "".into())
            .await?;

        Ok(subscription)
    }

    // === Object store helpers ===

    /// 获取对象存储 bucket,可选择不存在时创建.
    async fn get_or_create_bucket(
        &self,
        bucket_name: &str,
        create_if_not_found: bool,
    ) -> anyhow::Result<jetstream::object_store::ObjectStore> {
        let context = self.jetstream();

        match context.get_object_store(bucket_name).await {
            Ok(bucket) => Ok(bucket),
            Err(err) if err.to_string().contains("stream not found") => {
                // 用字符串匹配判断 404 —— err.source() 嵌套很深,这里求短平快.
                if !create_if_not_found {
                    anyhow::bail!(
                        "NATS get_object_store bucket does not exist: {bucket_name}. {err}."
                    );
                }
                tracing::debug!("Creating NATS bucket {bucket_name}");
                context
                    .create_object_store(jetstream::object_store::Config {
                        bucket: bucket_name.to_string(),
                        ..Default::default()
                    })
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed creating bucket / object store: {e}"))
            }
            Err(err) => anyhow::bail!("NATS get_object_store error: {err}"),
        }
    }

    /// 把本地文件上传到 NATS 对象存储(URL 形如 `nats://host/bucket/key`).
    pub async fn object_store_upload(&self, filepath: &Path, nats_url: &Url) -> anyhow::Result<()> {
        let mut disk_file = TokioFile::open(filepath).await?;

        let (bucket_name, key) = url_to_bucket_and_key(nats_url)?;
        let bucket = self.get_or_create_bucket(&bucket_name, true).await?;

        let key_meta = async_nats::jetstream::object_store::ObjectMetadata {
            name: key.to_string(),
            ..Default::default()
        };
        bucket.put(key_meta, &mut disk_file).await.map_err(|e| {
            anyhow::anyhow!("Failed uploading to bucket / object store {bucket_name}/{key}: {e}")
        })?;

        Ok(())
    }

    /// 从 NATS 对象存储下载到本地文件.
    pub async fn object_store_download(
        &self,
        nats_url: &Url,
        filepath: &Path,
    ) -> anyhow::Result<()> {
        let mut disk_file = TokioFile::create(filepath).await?;

        let (bucket_name, key) = url_to_bucket_and_key(nats_url)?;
        let bucket = self.get_or_create_bucket(&bucket_name, false).await?;

        let mut obj_reader = bucket.get(&key).await.map_err(|e| {
            anyhow::anyhow!(
                "Failed downloading from bucket / object store {bucket_name}/{key}: {e}"
            )
        })?;
        let _bytes_copied = tokio::io::copy(&mut obj_reader, &mut disk_file).await?;

        Ok(())
    }

    /// 删除 NATS 对象存储中的 bucket;不存在时视为成功(幂等).
    pub async fn object_store_delete_bucket(&self, bucket_name: &str) -> anyhow::Result<()> {
        let context = self.jetstream();
        match context.delete_object_store(&bucket_name).await {
            Ok(_) => Ok(()),
            Err(err) if err.to_string().contains("stream not found") => {
                tracing::trace!(bucket_name, "NATS bucket already gone");
                Ok(())
            }
            Err(err) => Err(anyhow::anyhow!("NATS get_object_store error: {err}")),
        }
    }

    /// 用 bincode 把可序列化结构上传到对象存储.
    pub async fn object_store_upload_data<T>(&self, data: &T, nats_url: &Url) -> anyhow::Result<()>
    where
        T: Serialize,
    {
        let binary_data = bincode::serialize(data)
            .map_err(|e| anyhow::anyhow!("Failed to serialize data with bincode: {e}"))?;

        let (bucket_name, key) = url_to_bucket_and_key(nats_url)?;
        let bucket = self.get_or_create_bucket(&bucket_name, true).await?;

        let key_meta = async_nats::jetstream::object_store::ObjectMetadata {
            name: key.to_string(),
            ..Default::default()
        };

        let mut cursor = std::io::Cursor::new(binary_data);
        bucket.put(key_meta, &mut cursor).await.map_err(|e| {
            anyhow::anyhow!("Failed uploading to bucket / object store {bucket_name}/{key}: {e}")
        })?;

        Ok(())
    }

    /// 从对象存储下载并用 bincode 反序列化结构.
    pub async fn object_store_download_data<T>(&self, nats_url: &Url) -> anyhow::Result<T>
    where
        T: DeserializeOwned,
    {
        let (bucket_name, key) = url_to_bucket_and_key(nats_url)?;
        let bucket = self.get_or_create_bucket(&bucket_name, false).await?;

        let mut obj_reader = bucket.get(&key).await.map_err(|e| {
            anyhow::anyhow!(
                "Failed downloading from bucket / object store {bucket_name}/{key}: {e}"
            )
        })?;

        let mut buffer = Vec::new();
        tokio::io::copy(&mut obj_reader, &mut buffer)
            .await
            .map_err(|e| anyhow::anyhow!("Failed reading object data: {e}"))?;
        tracing::debug!("Downloaded {} bytes from {bucket_name}/{key}", buffer.len());

        bincode::deserialize(&buffer)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize data with bincode: {e}").into())
    }
}

// === SECTION: ClientOptions / Auth ===

/// NATS 客户端选项,通过 derive_builder 构造,默认值来源于环境变量.
#[derive(Debug, Clone, Builder, Validate)]
pub struct ClientOptions {
    #[builder(setter(into), default = "default_server()")]
    #[validate(custom(function = "validate_nats_server"))]
    server: String,

    #[builder(default)]
    auth: NatsAuth,
}

/// 从 `NATS_SERVER` 读取默认服务器地址,否则回退到 `nats://localhost:4222`.
fn default_server() -> String {
    std::env::var(env_nats::NATS_SERVER).unwrap_or_else(|_| "nats://localhost:4222".to_string())
}

/// 校验服务器地址必须以 `nats://` 开头.
fn validate_nats_server(server: &str) -> Result<(), ValidationError> {
    if server.starts_with("nats://") {
        Ok(())
    } else {
        Err(ValidationError::new("server must start with 'nats://'"))
    }
}

impl ClientOptions {
    /// 入口:返回一个新的 [`ClientOptionsBuilder`].
    pub fn builder() -> ClientOptionsBuilder {
        ClientOptionsBuilder::default()
    }

    /// 完整校验并连接到 NATS 服务器,返回 [`Client`].
    pub async fn connect(self) -> Result<Client> {
        self.validate()?;

        let connect_options = match self.auth {
            NatsAuth::UserPass(username, password) => {
                async_nats::ConnectOptions::with_user_and_password(username, password)
            }
            NatsAuth::Token(token) => async_nats::ConnectOptions::with_token(token),
            NatsAuth::NKey(nkey) => async_nats::ConnectOptions::with_nkey(nkey),
            NatsAuth::CredentialsFile(path) => {
                async_nats::ConnectOptions::with_credentials_file(path).await?
            }
        };

        let server = self.server;
        let (client, _rt) = build_in_runtime(
            async move {
                connect_options
                    .connect(server)
                    .await
                    .map_err(|e| anyhow::anyhow!(
                        "Failed to connect to NATS: {e}. Verify NATS server is running and accessible."
                    ))
            },
            NATS_WORKER_THREADS,
        )
        .await?;

        let js_ctx = jetstream::new(client.clone());
        Ok(Client { client, js_ctx })
    }
}

impl Default for ClientOptions {
    fn default() -> Self {
        ClientOptions {
            server: default_server(),
            auth: NatsAuth::default(),
        }
    }
}

/// NATS 鉴权方式(Debug 实现对所有秘密字段做脱敏).
#[derive(Clone, Eq, PartialEq)]
pub enum NatsAuth {
    UserPass(String, String),
    Token(String),
    NKey(String),
    CredentialsFile(PathBuf),
}

impl std::fmt::Debug for NatsAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NatsAuth::UserPass(user, _pass) => write!(f, "UserPass({}, <redacted>)", user),
            NatsAuth::Token(_) => write!(f, "Token(<redacted>)"),
            NatsAuth::NKey(_) => write!(f, "NKey(<redacted>)"),
            NatsAuth::CredentialsFile(path) => write!(f, "CredentialsFile({:?})", path),
        }
    }
}

impl Default for NatsAuth {
    fn default() -> Self {
        // 优先级:UserPass > Token > NKey > CredentialsFile > 默认 user/user.
        if let (Ok(username), Ok(password)) = (
            std::env::var(env_nats::auth::NATS_AUTH_USERNAME),
            std::env::var(env_nats::auth::NATS_AUTH_PASSWORD),
        ) {
            return NatsAuth::UserPass(username, password);
        }
        if let Ok(token) = std::env::var(env_nats::auth::NATS_AUTH_TOKEN) {
            return NatsAuth::Token(token);
        }
        if let Ok(nkey) = std::env::var(env_nats::auth::NATS_AUTH_NKEY) {
            return NatsAuth::NKey(nkey);
        }
        if let Ok(path) = std::env::var(env_nats::auth::NATS_AUTH_CREDENTIALS_FILE) {
            return NatsAuth::CredentialsFile(PathBuf::from(path));
        }
        NatsAuth::UserPass("user".to_string(), "user".to_string())
    }
}

// === SECTION: URL helpers ===

/// 从 `nats://host[:port]/bucket/key` 形式的 URL 中提取 bucket 与 key.
pub fn url_to_bucket_and_key(url: &Url) -> anyhow::Result<(String, String)> {
    let mut path_segments = url
        .path_segments()
        .ok_or_else(|| anyhow::anyhow!("No path in NATS URL: {url}"))?;
    let bucket = path_segments
        .next()
        .ok_or_else(|| anyhow::anyhow!("No bucket in NATS URL: {url}"))?;
    let key = path_segments
        .next()
        .ok_or_else(|| anyhow::anyhow!("No key in NATS URL: {url}"))?;
    Ok((bucket.to_string(), key.to_string()))
}

// === SECTION: NatsQueue ===

/// 基于 JetStream 的命名队列.支持三种构造方式:广播(`new_with_consumer`),
/// 默认独占(`new` -> `"worker-group"`),纯发布者(`new_without_consumer`).
pub struct NatsQueue {
    /// JetStream stream 名称(自动 slugify).
    stream_name: String,
    /// NATS 服务器 URL.
    nats_server: String,
    /// `dequeue_task` 的默认超时.
    dequeue_timeout: time::Duration,
    /// 已建立的 NATS 客户端;`connect` 后填充.
    client: Option<Client>,
    /// 本队列使用的 subject 模式(`{stream}.*`).
    subject: String,
    /// pull 模式的持久 consumer;仅在 `consumer_name == Some(_)` 时存在.
    subscriber: Option<jetstream::consumer::PullConsumer>,
    /// 广播模式下的 consumer 名;`None` 表示纯发布者.
    consumer_name: Option<String>,
    /// 高效消费用的消息流.
    message_stream: Option<jetstream::consumer::pull::Stream>,
}

impl NatsQueue {
    /// 默认 `"worker-group"` consumer 的构造器.
    pub fn new(stream_name: String, nats_server: String, dequeue_timeout: time::Duration) -> Self {
        Self::build(stream_name, nats_server, dequeue_timeout, Some("worker-group".to_string()))
    }

    /// 纯发布者模式的构造器(不创建 consumer).
    pub fn new_without_consumer(
        stream_name: String,
        nats_server: String,
        dequeue_timeout: time::Duration,
    ) -> Self {
        Self::build(stream_name, nats_server, dequeue_timeout, None)
    }

    /// 广播模式:每个名字独立的 consumer 都能完整接收所有消息.
    pub fn new_with_consumer(
        stream_name: String,
        nats_server: String,
        dequeue_timeout: time::Duration,
        consumer_name: String,
    ) -> Self {
        Self::build(stream_name, nats_server, dequeue_timeout, Some(consumer_name))
    }

    /// 内部构造助手:统一处理 slugify + subject 拼装.
    fn build(
        stream_name: String,
        nats_server: String,
        dequeue_timeout: time::Duration,
        consumer_name: Option<String>,
    ) -> Self {
        // 把不合法字符(如 `/`)替换为下划线.
        let sanitized_stream_name = Slug::slugify(&stream_name).to_string();
        let subject = format!("{sanitized_stream_name}.*");
        Self {
            stream_name: sanitized_stream_name,
            nats_server,
            dequeue_timeout,
            client: None,
            subject,
            subscriber: None,
            consumer_name,
            message_stream: None,
        }
    }

    /// 建立连接并准备好 stream/consumer(若需要).
    pub async fn connect(&mut self) -> Result<()> {
        self.connect_with_reset(false).await
    }

    /// 建立连接,可选清空 stream 中的历史消息.
    pub async fn connect_with_reset(&mut self, reset_stream: bool) -> Result<()> {
        if self.client.is_some() {
            return Ok(());
        }

        let client_options = Client::builder().server(self.nats_server.clone()).build()?;
        let client = client_options.connect().await?;

        // 老化时间从环境变量 PGD_NATS_STREAM_MAX_AGE 读取(秒),否则默认 1 小时.
        let max_age = std::env::var(env_nats::stream::PGD_NATS_STREAM_MAX_AGE)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(time::Duration::from_secs)
            .unwrap_or_else(|| time::Duration::from_secs(60 * 60));

        let stream_config = jetstream::stream::Config {
            name: self.stream_name.clone(),
            subjects: vec![self.subject.clone()],
            max_age,
            ..Default::default()
        };

        let stream = client
            .jetstream()
            .get_or_create_stream(stream_config)
            .await?;

        log::debug!("Stream {} is ready", self.stream_name);

        if reset_stream {
            match stream.purge().await {
                Ok(purge_info) => log::info!(
                    "Successfully purged {} messages from NATS stream {}",
                    purge_info.purged,
                    self.stream_name
                ),
                Err(e) => {
                    log::warn!("Failed to purge NATS stream '{}': {e}", self.stream_name)
                }
            }
        }

        // 仅当 consumer_name 存在时创建持久 consumer + 消息流.
        if let Some(consumer_name) = self.consumer_name.as_ref() {
            let consumer_config = jetstream::consumer::pull::Config {
                durable_name: Some(consumer_name.clone()),
                inactive_threshold: std::time::Duration::from_secs(300),
                ..Default::default()
            };
            let subscriber = stream.create_consumer(consumer_config).await?;
            let message_stream = subscriber.messages().await?;
            self.subscriber = Some(subscriber);
            self.message_stream = Some(message_stream);
        }

        self.client = Some(client);
        Ok(())
    }

    /// 若尚未连接则自动 connect.
    pub async fn ensure_connection(&mut self) -> Result<()> {
        if self.client.is_none() {
            self.connect().await?;
        }
        Ok(())
    }

    /// 释放本地连接资源;不会删除服务器侧 consumer.
    pub async fn close(&mut self) -> Result<()> {
        self.message_stream = None;
        self.subscriber = None;
        self.client = None;
        Ok(())
    }

    /// 删除服务器侧 consumer,可选指定别人的 consumer 名.
    ///
    /// - `consumer_name=None`: 删除自己,并 close 本地连接;
    /// - `consumer_name=Some(other)`: 删除其他 consumer,本地连接保留;
    /// - 误把自己的名字当 `Some(...)` 传入时打 warn 但仍执行.
    pub async fn shutdown(&mut self, consumer_name: Option<String>) -> Result<()> {
        let target_consumer = consumer_name.as_ref().or(self.consumer_name.as_ref());

        if let Some(passed_name) = consumer_name.as_ref()
            && self.consumer_name.as_ref() == Some(passed_name)
        {
            log::warn!(
                "Deleting our own consumer '{}' via explicit consumer_name parameter. \
                 Consider calling shutdown without arguments instead.",
                passed_name
            );
        }

        if let (Some(client), Some(consumer_to_delete)) = (&self.client, target_consumer) {
            let stream = client.jetstream().get_stream(&self.stream_name).await?;
            stream
                .delete_consumer(consumer_to_delete)
                .await
                .map_err(|e| {
                    anyhow::anyhow!("Failed to delete consumer {}: {}", consumer_to_delete, e)
                })?;
            log::debug!(
                "Deleted consumer {} from stream {}",
                consumer_to_delete,
                self.stream_name
            );
        } else {
            log::debug!(
                "Cannot shutdown consumer: client or target consumer is None (client: {:?}, target_consumer: {:?})",
                self.client.is_some(),
                target_consumer.is_some()
            );
        }

        if consumer_name.is_none() {
            self.close().await
        } else {
            Ok(())
        }
    }

    /// 当前 stream 上的 consumer 数量.
    pub async fn count_consumers(&mut self) -> Result<usize> {
        self.ensure_connection().await?;
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Client not connected"))?;
        let mut stream = client.jetstream().get_stream(&self.stream_name).await?;
        let info = stream.info().await?;
        Ok(info.state.consumer_count)
    }

    /// 当前 stream 上所有 consumer 的名字列表.
    pub async fn list_consumers(&mut self) -> Result<Vec<String>> {
        self.ensure_connection().await?;
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Client not connected"))?;
        client.list_consumers(&self.stream_name).await
    }

    /// 把任务字节流压入队列.
    pub async fn enqueue_task(&mut self, task_data: Bytes) -> Result<()> {
        self.ensure_connection().await?;
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Client not connected"))?;
        let subject = format!("{}.queue", self.stream_name);
        client.jetstream().publish(subject, task_data).await?;
        Ok(())
    }

    /// 取一条任务(可选超时);超时返回 `Ok(None)`,流结束/出错返回 `Err`.
    pub async fn dequeue_task(&mut self, timeout: Option<time::Duration>) -> Result<Option<Bytes>> {
        self.ensure_connection().await?;

        let stream = self
            .message_stream
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Message stream not initialized"))?;

        let timeout_duration = timeout.unwrap_or(self.dequeue_timeout);
        let message = tokio::time::timeout(timeout_duration, stream.next()).await;

        match message {
            Ok(Some(Ok(msg))) => {
                msg.ack()
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to ack message: {}", e))?;
                Ok(Some(msg.payload.clone()))
            }
            Ok(Some(Err(e))) => Err(anyhow::anyhow!("Failed to get message from stream: {}", e)),
            Ok(None) => Err(anyhow::anyhow!("Message stream ended unexpectedly")),
            Err(_) => Ok(None),
        }
    }

    /// 当前 consumer 的待消费消息数量(num_pending).
    pub async fn get_queue_size(&mut self) -> Result<u64> {
        self.ensure_connection().await?;
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Client not connected"))?;
        let stream = client.jetstream().get_stream(&self.stream_name).await?;
        let consumer_name = self
            .consumer_name
            .clone()
            .unwrap_or_else(|| "worker-group".to_string());
        let mut consumer: jetstream::consumer::PullConsumer = stream
            .get_consumer(&consumer_name)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get consumer: {}", e))?;
        let info = consumer.info().await?;
        Ok(info.num_pending)
    }

    /// stream 上当前的总消息数(state.messages).
    pub async fn get_stream_messages(&mut self) -> Result<u64> {
        self.ensure_connection().await?;
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Client not connected"))?;
        let mut stream = client.jetstream().get_stream(&self.stream_name).await?;
        let info = stream.info().await?;
        Ok(info.state.messages)
    }

    /// 永久清除小于 `sequence` 的所有消息.注意 JetStream 的 sequence 区间不含上界本身.
    pub async fn purge_up_to_sequence(&self, sequence: u64) -> Result<()> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Client not connected"))?;
        let stream = client.jetstream().get_stream(&self.stream_name).await?;
        stream.purge().sequence(sequence).await.map_err(|e| {
            anyhow::anyhow!("Failed to purge stream up to sequence {}: {}", sequence, e)
        })?;
        log::debug!(
            "Purged stream {} up to sequence {}",
            self.stream_name,
            sequence
        );
        Ok(())
    }

    /// 找到所有 consumer 中最小的 ack_floor 序号,然后 purge 到该序号(包含).
    pub async fn purge_acknowledged(&mut self) -> Result<()> {
        self.ensure_connection().await?;

        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Client not connected"))?;
        let stream = client.jetstream().get_stream(&self.stream_name).await?;

        let consumer_names: Vec<String> = stream
            .consumer_names()
            .try_collect()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list consumers: {}", e))?;

        if consumer_names.is_empty() {
            log::debug!("No consumers found for stream {}", self.stream_name);
            return Ok(());
        }

        let mut min_ack_sequence = u64::MAX;
        for consumer_name in &consumer_names {
            let mut consumer: jetstream::consumer::PullConsumer = stream
                .get_consumer(consumer_name)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to get consumer {}: {}", consumer_name, e))?;
            let info = consumer
                .info()
                .await
                .map_err(|e| {
                    anyhow::anyhow!("Failed to get consumer info for {}: {}", consumer_name, e)
                })?;

            if info.ack_floor.stream_sequence > 0 {
                min_ack_sequence = min_ack_sequence.min(info.ack_floor.stream_sequence);
                log::debug!(
                    "Consumer {} has ack_floor at sequence {}",
                    consumer_name,
                    info.ack_floor.stream_sequence
                );
            }
        }

        if min_ack_sequence < u64::MAX && min_ack_sequence > 0 {
            // purge 上界是 +1,因为我们要 *包含* 最小 ack 的消息.
            let purge_sequence = min_ack_sequence + 1;
            self.purge_up_to_sequence(purge_sequence).await?;
            log::debug!(
                "Purged stream {} up to acknowledged sequence {} (purged up to sequence {})",
                self.stream_name,
                min_ack_sequence,
                purge_sequence
            );
        } else {
            log::debug!(
                "No messages to purge for stream {} (min_ack_sequence: {})",
                self.stream_name,
                min_ack_sequence
            );
        }

        Ok(())
    }
}

impl NatsQueue {
    /// 返回事件 subject 前缀(即 stream 名).
    pub fn event_subject(&self) -> String {
        self.stream_name.clone()
    }

    /// 把任意 Serialize 事件序列化为 JSON 后发布到 `{stream}.{event_name}`.
    pub async fn publish_event(
        &self,
        event_name: impl AsRef<str> + Send + Sync,
        event: &(impl Serialize + Send + Sync),
    ) -> Result<()> {
        let bytes = serde_json::to_vec(event)?;
        self.publish_event_bytes(event_name, bytes).await
    }

    /// 直接发布字节负载到 `{stream}.{event_name}`.
    pub async fn publish_event_bytes(
        &self,
        event_name: impl AsRef<str> + Send + Sync,
        bytes: Vec<u8>,
    ) -> Result<()> {
        let subject = format!("{}.{}", self.event_subject(), event_name.as_ref());

        // 注意: enqueue_task 需要 &mut self,但 EventPublisher 只持有 &self,
        // 因此这里要确保客户端已连接并直接使用它.
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Client not connected"))?;
        client.jetstream().publish(subject, bytes.into()).await?;
        Ok(())
    }
}

/// 根据 portname 与实例号构造实例级消息 subject.
///
/// 格式 `"{namespace}_{servicegroup}.{name}-{instance_id:x}"` 是协议级承诺,不可改.
pub fn instance_subject(portname_id: &PortNameId, instance_id: u64) -> String {
    let namespace = &portname_id.namespace;
    let servicegroup = &portname_id.servicegroup;
    let name = &portname_id.name;
    format!("{namespace}_{servicegroup}.{name}-{instance_id:x}")
}

// === SECTION: tests ===

#[cfg(test)]
mod tests {

    use super::*;
    use figment::Jail;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestData {
        id: u32,
        name: String,
        values: Vec<f64>,
    }

    #[test]
    fn test_client_options_builder() {
        Jail::expect_with(|_jail| {
            let opts = ClientOptions::builder().build();
            assert!(opts.is_ok());
            Ok(())
        });

        Jail::expect_with(|jail| {
            jail.set_env(env_nats::NATS_SERVER, "nats://localhost:5222");
            jail.set_env(env_nats::auth::NATS_AUTH_USERNAME, "user");
            jail.set_env(env_nats::auth::NATS_AUTH_PASSWORD, "pass");

            let opts = ClientOptions::builder().build();
            assert!(opts.is_ok());
            let opts = opts.unwrap();

            assert_eq!(opts.server, "nats://localhost:5222");
            assert_eq!(
                opts.auth,
                NatsAuth::UserPass("user".to_string(), "pass".to_string())
            );

            Ok(())
        });

        Jail::expect_with(|jail| {
            jail.set_env(env_nats::NATS_SERVER, "nats://localhost:5222");
            jail.set_env(env_nats::auth::NATS_AUTH_USERNAME, "user");
            jail.set_env(env_nats::auth::NATS_AUTH_PASSWORD, "pass");

            let opts = ClientOptions::builder()
                .server("nats://localhost:6222")
                .auth(NatsAuth::Token("token".to_string()))
                .build();
            assert!(opts.is_ok());
            let opts = opts.unwrap();

            assert_eq!(opts.server, "nats://localhost:6222");
            assert_eq!(opts.auth, NatsAuth::Token("token".to_string()));

            Ok(())
        });
    }

    // 使用 bincode 的对象存储数据操作集成测试
    #[tokio::test]
    #[ignore] // 需要 NATS 服务器运行
    async fn test_object_store_data_operations() {
        // 构造测试数据
        let test_data = TestData {
            id: 42,
            name: "test_item".to_string(),
            values: vec![1.0, 2.5, 3.7, 4.2],
        };

        // 准备客户端
        let client_options = ClientOptions::builder()
            .server("nats://localhost:4222")
            .build()
            .expect("Failed to build client options");

        let client = client_options
            .connect()
            .await
            .expect("Failed to connect to NATS");

        // 测试 URL（用 .bin 后缀表示二进制格式）
        let url = Url::parse("nats://localhost/test-bucket/test-data.bin")
            .expect("Failed to parse URL");

        // 上传数据
        client
            .object_store_upload_data(&test_data, &url)
            .await
            .expect("Failed to upload data");

        // 下载数据
        let downloaded_data: TestData = client
            .object_store_download_data(&url)
            .await
            .expect("Failed to download data");

        // 校验数据一致
        assert_eq!(test_data, downloaded_data);

        // 清理
        client
            .object_store_delete_bucket("test-bucket")
            .await
            .expect("Failed to delete bucket");
    }

    // 广播模式 + 清理的集成测试
    #[tokio::test]
    #[ignore]
    async fn test_nats_queue_broadcast_with_purge() {
        use uuid::Uuid;

        let stream_name = format!("test-broadcast-{}", Uuid::new_v4());
        let nats_server = "nats://localhost:4222".to_string();
        let timeout = time::Duration::from_secs(0);

        let client_options = Client::builder()
            .server(nats_server.clone())
            .build()
            .expect("Failed to build client options");

        let client = client_options
            .connect()
            .await
            .expect("Failed to connect to NATS");

        let _ = client.jetstream().delete_stream(&stream_name).await;

        let consumer1_name = format!("consumer-{}", Uuid::new_v4());
        let consumer2_name = format!("consumer-{}", Uuid::new_v4());

        let mut queue1 = NatsQueue::new_with_consumer(
            stream_name.clone(),
            nats_server.clone(),
            timeout,
            consumer1_name,
        );

        queue1.connect().await.expect("Failed to connect queue1");

        let message_strings = [
            "message1".to_string(),
            "message2".to_string(),
            "message3".to_string(),
            "message4".to_string(),
        ];

        for (idx, msg) in message_strings.iter().enumerate() {
            queue1
                .publish_event("queue", msg)
                .await
                .unwrap_or_else(|_| panic!("Failed to publish message {}", idx + 1));
        }

        let messages: Vec<Bytes> = message_strings
            .iter()
            .map(|s| Bytes::from(serde_json::to_vec(s).unwrap()))
            .collect();

        tokio::time::sleep(time::Duration::from_millis(100)).await;

        let mut queue2 = NatsQueue::new_with_consumer(
            stream_name.clone(),
            nats_server.clone(),
            timeout,
            consumer2_name,
        );

        let mut queue3 =
            NatsQueue::new_without_consumer(stream_name.clone(), nats_server.clone(), timeout);

        queue2.connect().await.expect("Failed to connect queue2");
        queue3.connect().await.expect("Failed to connect queue3");

        queue1
            .purge_up_to_sequence(3)
            .await
            .expect("Failed to purge messages");

        tokio::time::sleep(time::Duration::from_millis(100)).await;

        let msg3_consumer1 = queue1
            .dequeue_task(Some(time::Duration::from_millis(500)))
            .await
            .expect("Failed to dequeue from queue1");
        assert_eq!(
            msg3_consumer1,
            Some(messages[2].clone()),
            "Consumer 1 should get message3"
        );

        tokio::time::sleep(time::Duration::from_millis(100)).await;

        queue1
            .purge_acknowledged()
            .await
            .expect("Failed to purge acknowledged messages");

        tokio::time::sleep(time::Duration::from_millis(100)).await;

        let mut consumer1_remaining = Vec::new();
        let mut consumer2_remaining = Vec::new();

        while let Some(msg) = queue1
            .dequeue_task(None)
            .await
            .expect("Failed to dequeue from queue1")
        {
            consumer1_remaining.push(msg);
        }

        while let Some(msg) = queue2
            .dequeue_task(None)
            .await
            .expect("Failed to dequeue from queue2")
        {
            consumer2_remaining.push(msg);
        }

        assert_eq!(
            consumer1_remaining.len(),
            1,
            "Consumer 1 should have 1 remaining message"
        );
        assert_eq!(
            consumer1_remaining[0], messages[3],
            "Consumer 1 should get message4"
        );

        assert_eq!(
            consumer2_remaining.len(),
            2,
            "Consumer 2 should have 2 messages"
        );
        assert_eq!(
            consumer2_remaining[0], messages[2],
            "Consumer 2 should get message3"
        );
        assert_eq!(
            consumer2_remaining[1], messages[3],
            "Consumer 2 should get message4"
        );

        let consumer_count = queue1
            .count_consumers()
            .await
            .expect("Failed to count consumers");
        assert_eq!(consumer_count, 2, "Should have 2 consumers initially");

        queue1.close().await.expect("Failed to close queue1");

        let consumer_count = queue2
            .count_consumers()
            .await
            .expect("Failed to count consumers");
        assert_eq!(
            consumer_count, 2,
            "Should still have 2 consumers after closing queue1"
        );

        queue1.connect().await.expect("Failed to reconnect queue1");

        queue1
            .shutdown(None)
            .await
            .expect("Failed to shutdown queue1");

        let consumer_count = queue2
            .count_consumers()
            .await
            .expect("Failed to count consumers");
        assert_eq!(
            consumer_count, 1,
            "Should have only 1 consumer after shutting down queue1"
        );

        client
            .jetstream()
            .delete_stream(&stream_name)
            .await
            .expect("Failed to delete test stream");
    }

    // === SECTION: 合并自 supplemental_tests 模块 ===
    // ## 测试过程
    // 7 类路径:常量与 server validator,NatsAuth 优先级与脱敏,ClientOptions
    // 连接失败/校验失败,URL 拆解 + instance_subject 格式,Queue 构造器 slugify,
    // 离线状态下 publish/dequeue/count 等方法的错误分支,以及在本地 NATS 可用时
    // 完整跑 object store + stream/consumer + scrape 路径.
    //
    // ## 意义
    // 没有 NATS 时纯逻辑测试仍跑;有 NATS 时端到端验证对象存储与队列契约.

    use tokio::time::{Duration, timeout};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct StoreData {
        id: u64,
        name: String,
        values: Vec<i32>,
    }

    fn maybe_test_nats_server() -> String {
        std::env::var("PAGODA_TEST_NATS_URL")
            .unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string())
    }

    async fn maybe_connected_client() -> Option<Client> {
        let options = ClientOptions::builder()
            .server(maybe_test_nats_server())
            .build()
            .ok()?;
        options.connect().await.ok()
    }

    #[test]
    fn constants_and_server_validation_helpers() {
        assert_eq!(URL_PREFIX, "nats://");

        Jail::expect_with(|jail| {
            jail.set_env(env_nats::NATS_SERVER, "nats://example:4222");
            assert_eq!(default_server(), "nats://example:4222");
            Ok(())
        });

        Jail::expect_with(|jail| {
            jail.clear_env();
            assert_eq!(default_server(), "nats://localhost:4222");
            Ok(())
        });

        assert!(validate_nats_server("nats://localhost:4222").is_ok());
        let err = validate_nats_server("http://localhost:4222")
            .err()
            .expect("non-nats scheme should fail");
        assert_eq!(err.code.as_ref(), "server must start with 'nats://'");
    }

    #[test]
    fn nats_auth_default_precedence_and_debug_redaction() {
        Jail::expect_with(|jail| {
            jail.set_env(env_nats::auth::NATS_AUTH_USERNAME, "u");
            jail.set_env(env_nats::auth::NATS_AUTH_PASSWORD, "p");
            jail.set_env(env_nats::auth::NATS_AUTH_TOKEN, "tok");
            assert_eq!(NatsAuth::default(), NatsAuth::UserPass("u".into(), "p".into()));
            Ok(())
        });

        Jail::expect_with(|jail| {
            jail.clear_env();
            jail.set_env(env_nats::auth::NATS_AUTH_TOKEN, "tok");
            assert_eq!(NatsAuth::default(), NatsAuth::Token("tok".into()));
            Ok(())
        });

        Jail::expect_with(|jail| {
            jail.clear_env();
            jail.set_env(env_nats::auth::NATS_AUTH_NKEY, "nk");
            assert_eq!(NatsAuth::default(), NatsAuth::NKey("nk".into()));
            Ok(())
        });

        Jail::expect_with(|jail| {
            jail.clear_env();
            jail.set_env(env_nats::auth::NATS_AUTH_CREDENTIALS_FILE, "/tmp/creds");
            assert_eq!(
                NatsAuth::default(),
                NatsAuth::CredentialsFile(PathBuf::from("/tmp/creds"))
            );
            Ok(())
        });

        let dbg_userpass = format!("{:?}", NatsAuth::UserPass("user".into(), "secret".into()));
        let dbg_token = format!("{:?}", NatsAuth::Token("secret".into()));
        let dbg_nkey = format!("{:?}", NatsAuth::NKey("secret".into()));
        assert!(dbg_userpass.contains("<redacted>"));
        assert!(dbg_token.contains("<redacted>"));
        assert!(dbg_nkey.contains("<redacted>"));
    }

    #[tokio::test]
    async fn client_options_connect_validation_and_failure_paths() {
        let bad_scheme = ClientOptions::builder()
            .server("http://localhost:4222")
            .build()
            .expect("builder should allow constructing before validate");
        let err = bad_scheme
            .connect()
            .await
            .err()
            .expect("invalid server scheme should fail validation in connect");
        assert!(err.to_string().contains("server must start with 'nats://'"));

        let unreachable = ClientOptions::builder()
            .server("nats://127.0.0.1:1")
            .build()
            .expect("builder should succeed");
        let connect_result = timeout(Duration::from_secs(5), unreachable.connect()).await;
        match connect_result {
            Ok(Ok(_client)) => {}
            Ok(Err(err)) => assert!(err.to_string().contains("Failed to connect to NATS")),
            Err(_) => {}
        }
    }

    #[test]
    fn url_to_bucket_and_key_and_instance_subject_paths() {
        let url = Url::parse("nats://localhost/bucket-a/key-b").expect("url should parse");
        let (bucket, key) = url_to_bucket_and_key(&url).expect("bucket/key should parse");
        assert_eq!(bucket, "bucket-a");
        assert_eq!(key, "key-b");

        let no_bucket = Url::parse("nats://localhost/").expect("url should parse");
        let no_bucket_err = url_to_bucket_and_key(&no_bucket)
            .err()
            .expect("missing path servicegroups should fail")
            .to_string();
        assert!(no_bucket_err.contains("No bucket") || no_bucket_err.contains("No key"));

        let no_key = Url::parse("nats://localhost/bucket-only").expect("url should parse");
        assert!(
            url_to_bucket_and_key(&no_key)
                .err()
                .expect("missing key should fail")
                .to_string()
                .contains("No key")
        );

        let portname = PortNameId::from("ns/comp/ep");
        let subject = instance_subject(&portname, 0x2a);
        assert_eq!(subject, "ns_comp.ep-2a");
    }

    #[test]
    fn queue_constructors_and_event_subject_sanitize_stream_names() {
        let timeout = Duration::from_secs(1);
        let q = NatsQueue::new("/A B/C".to_string(), "nats://x".to_string(), timeout);
        assert_eq!(q.stream_name, "a_b_c");
        assert_eq!(q.subject, "a_b_c.*");
        assert_eq!(q.event_subject(), "a_b_c");
        assert_eq!(q.consumer_name, Some("worker-group".to_string()));

        let q_no_cons =
            NatsQueue::new_without_consumer("A/B".to_string(), "nats://x".to_string(), timeout);
        assert_eq!(q_no_cons.stream_name, "a_b");
        assert_eq!(q_no_cons.consumer_name, None);

        let q_named = NatsQueue::new_with_consumer(
            "A/B".to_string(),
            "nats://x".to_string(),
            timeout,
            "consumer-a".to_string(),
        );
        assert_eq!(q_named.consumer_name, Some("consumer-a".to_string()));
    }

    #[tokio::test]
    async fn queue_methods_handle_disconnected_states() {
        let dequeue_timeout = Duration::from_millis(5);
        let mut q = NatsQueue::new_without_consumer(
            "s".to_string(),
            "nats://127.0.0.1:1".to_string(),
            dequeue_timeout,
        );

        // publish_event 序列化失败路径（NaN 无法在 JSON 中表示）.
        #[derive(Serialize)]
        struct BadEvent {
            v: f64,
        }
        let bad = BadEvent { v: f64::NAN };
        let serialize_err = q
            .publish_event("evt", &bad)
            .await
            .err()
            .expect("publish_event should fail serialization for NaN");
        assert!(!serialize_err.to_string().is_empty());
        let connect_res = timeout(Duration::from_secs(5), q.connect()).await;
        match connect_res {
            Ok(Ok(())) => {
                q.close().await.expect("close should always succeed");
            }
            Ok(Err(_)) | Err(_) => {
                // 未连接状态下这些方法应暴露连接失败.
                assert!(q.ensure_connection().await.is_err());
            }
        }

        let _ = q.close().await.expect("close should always succeed");

        // 显式传入 consumer 名的分支，即使未连接也返回 Ok.
        q.shutdown(Some("other-consumer".to_string()))
            .await
            .expect("shutdown with explicit consumer should be no-op when disconnected");

        // 不传 consumer 时委托给 close，同样成功.
        q.shutdown(None)
            .await
            .expect("shutdown without explicit consumer should close disconnected queue");
    }

    #[tokio::test]
    async fn queue_error_paths_when_not_connected() {
        let dequeue_timeout = Duration::from_millis(5);
        let mut q = NatsQueue::new(
            "s".to_string(),
            "nats://127.0.0.1:1".to_string(),
            dequeue_timeout,
        );

        // 强制无连接且无 stream，以命中本地校验分支.
        q.client = None;
        q.message_stream = None;

        let err = q
            .publish_event_bytes("evt", vec![1, 2])
            .await
            .err()
            .expect("publish_event_bytes should fail when disconnected");
        assert!(err.to_string().contains("Client not connected"));

        // dequeue_task 应先调 ensure_connection，在未连接场景下失败.
        assert!(q.dequeue_task(Some(Duration::from_millis(1))).await.is_err());
        assert!(q.count_consumers().await.is_err());
        assert!(q.list_consumers().await.is_err());
        assert!(q.enqueue_task(Bytes::from_static(b"x")).await.is_err());
        assert!(q.get_queue_size().await.is_err());
        assert!(q.get_stream_messages().await.is_err());
        assert!(q.purge_up_to_sequence(1).await.is_err());
        assert!(q.purge_acknowledged().await.is_err());
    }

    #[tokio::test]
    async fn live_client_and_object_store_paths_when_nats_available() {
        let Some(client) = maybe_connected_client().await else {
            return;
        };

        // 访问器与基本列表方法
        let _raw_client = client.client();
        let _js = client.jetstream();
        let addr = client.addr();
        assert!(!addr.is_empty());

        let _ = client.list_streams().await.expect("list_streams should succeed");

        // 对象存储方法通过公共 API 间接驱动私有的 get_or_create_bucket.
        let bucket = format!("supp-bucket-{}", uuid::Uuid::new_v4());
        let key = "obj.bin";
        let url = Url::parse(&format!("nats://localhost/{bucket}/{key}"))
            .expect("nats object URL should parse");

        let tmp_path = std::env::temp_dir().join(format!("supp-nats-{}", uuid::Uuid::new_v4()));
        tokio::fs::write(&tmp_path, b"hello-object")
            .await
            .expect("temp file write should succeed");

        client
            .object_store_upload(&tmp_path, &url)
            .await
            .expect("object_store_upload should succeed");

        let out_path = std::env::temp_dir().join(format!("supp-nats-out-{}", uuid::Uuid::new_v4()));
        client
            .object_store_download(&url, &out_path)
            .await
            .expect("object_store_download should succeed");

        let downloaded = tokio::fs::read(&out_path)
            .await
            .expect("downloaded file should be readable");
        assert_eq!(downloaded, b"hello-object");

        let data = StoreData {
            id: 7,
            name: "nats".to_string(),
            values: vec![1, 2, 3],
        };
        let data_key_url = Url::parse(&format!("nats://localhost/{bucket}/data.bin"))
            .expect("nats data URL should parse");
        client
            .object_store_upload_data(&data, &data_key_url)
            .await
            .expect("object_store_upload_data should succeed");
        let downloaded_data: StoreData = client
            .object_store_download_data(&data_key_url)
            .await
            .expect("object_store_download_data should succeed");
        assert_eq!(downloaded_data, data);

        client
            .object_store_delete_bucket(&bucket)
            .await
            .expect("object_store_delete_bucket should succeed");
        // 幂等删除分支（bucket 已不存在）
        client
            .object_store_delete_bucket(&bucket)
            .await
            .expect("object_store_delete_bucket should treat missing bucket as success");

        let _ = tokio::fs::remove_file(&tmp_path).await;
        let _ = tokio::fs::remove_file(&out_path).await;
    }

    #[tokio::test]
    async fn live_stream_consumer_and_scrape_paths_when_nats_available() {
        let Some(client) = maybe_connected_client().await else {
            return;
        };

        let stream_name = format!("supp-stream-{}", uuid::Uuid::new_v4());
        let subject = format!("{stream_name}.*");

        let stream = client
            .jetstream()
            .get_or_create_stream(jetstream::stream::Config {
                name: stream_name.clone(),
                subjects: vec![subject],
                ..Default::default()
            })
            .await
            .expect("create stream should succeed");

        let _state = client
            .stream_info(&stream_name)
            .await
            .expect("stream_info should succeed");
        let _stream_handle = client
            .get_stream(&stream_name)
            .await
            .expect("get_stream should succeed");

        let consumer_name = format!("supp-consumer-{}", uuid::Uuid::new_v4());
        let _consumer = stream
            .create_consumer(jetstream::consumer::pull::Config {
                durable_name: Some(consumer_name.clone()),
                ..Default::default()
            })
            .await
            .expect("create consumer should succeed");

        let consumers = client
            .list_consumers(&stream_name)
            .await
            .expect("list_consumers should succeed");
        assert!(consumers.iter().any(|c| c == &consumer_name));

        // scrape_service 应返回一个订阅;本 API 测试不需要对端响应.
        let _subscription = client
            .scrape_service("nonexistent-service")
            .await
            .expect("scrape_service should create a reply subscription");

        client
            .jetstream()
            .delete_stream(&stream_name)
            .await
            .expect("cleanup stream should succeed");
    }
}

