// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::network` —— 网络层模块聚合 / 公共类型与协议
//!
//! ## 设计意图
//! 把分布式 pipeline 的网络抽象一次性集中到本模块：协议消息、ingress/egress 入口、
//! codec/tcp/manager 子模块的 `pub mod` 暴露以及顶层 `pub use` re-export。
//!
//! ## 外部契约
//! - `pub mod {codec, egress, ingress, manager, tcp}` 必须保留；
//! - 顶层 `pub use` 列表为稳定契约；**不可** 额外 re-export
//!   `codec::{TwoPartCodec, TwoPartMessage, TwoPartMessageType}` —— 这些类型应继续
//!   通过 `codec::` 路径访问；
//! - 所有公开结构体/枚举、字段与变体作为稳定对外契约。
//!
//! ## 实现要点
//! - 文件本身不写业务逻辑，仅承担"模块根 + 公共数据类型 + re-export"职责。

//! 分布式通信的网络层。
//!
//! 它提供跨多种传输协议的请求分发：
//! - HTTP/2，适用于标准部署；
//! - 带长度前缀协议的 TCP，适用于高性能场景；
//! - NATS，适用于遗留或消息驱动部署。

// === SECTION: 子模块 re-export ===
pub mod codec;
pub mod egress;
pub mod ingress;
pub mod manager;
pub mod tcp;

// === SECTION: 导入与 TCP 大小配置 ===
use crate::SystemHealth;
use std::sync::{Arc, OnceLock};

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use codec::{TwoPartCodec, TwoPartMessage, TwoPartMessageType};
use derive_builder::Builder;
use futures::StreamExt;
// io::Cursor, TryStreamExt
use super::{AsyncEngine, AsyncEngineContext, AsyncEngineContextProvider, ResponseStream};
use serde::{Deserialize, Serialize};

use super::{
    AsyncTransportEngine, Context, Data, Error, ManyOut, PipelineError, PipelineIO, SegmentSource,
    ServiceBackend, ServiceEngine, SingleIn, Source, context,
};
use crate::metrics::MetricsHierarchy;
use ingress::push_handler::WorkHandlerMetrics;
use prometheus::{CounterVec, Histogram, IntCounter, IntCounterVec, IntGauge};

/// request-plane servicegroup 共享的默认 TCP 最大消息大小。
pub(crate) const DEFAULT_TCP_MAX_MESSAGE_SIZE: usize = 32 * 1024 * 1024;

static TCP_MAX_MESSAGE_SIZE: OnceLock<usize> = OnceLock::new();

/// 只读取一次配置好的 TCP 最大消息大小，并在客户端、服务端和零拷贝解码路径之间共享。
pub(crate) fn get_tcp_max_message_size() -> usize {
    *TCP_MAX_MESSAGE_SIZE.get_or_init(|| {
        std::env::var("PGD_TCP_MAX_MESSAGE_SIZE")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(DEFAULT_TCP_MAX_MESSAGE_SIZE)
    })
}

// === SECTION: 公共 trait / 协议枚举 ===
pub trait Codable: PipelineIO + Serialize + for<'de> Deserialize<'de> {}
impl<T: PipelineIO + Serialize + for<'de> Deserialize<'de>> Codable for T {}

/// `WorkQueueConsumer` 是一个通用的工作队列接口，可用于发送和接收数据。
#[async_trait]
pub trait WorkQueueConsumer {
    async fn dequeue(&self) -> Result<Bytes, String>;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StreamType {
    Request,
    Response,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ControlMessage {
    Stop,
    Kill,
    Sentinel,
}

/// 这是 `ResponseStream` 中的第一条消息。
/// 它不会被通用 pipeline 处理，而是一个控制消息，`AsyncEngine::generate`
/// 只有在等到它之后才允许返回。
///
/// 如果其中包含错误，`AsyncEngine::generate` 会直接返回该错误，而不是返回 `ResponseStream`。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResponseStreamPrologue {
    error: Option<String>,
}

pub type StreamProvider<T> = tokio::sync::oneshot::Receiver<Result<T, String>>;

// === SECTION: RegisteredStream + PendingConnections + 响应服务 ===
/// 把 `Drop` 的拥有权放在这里，而不是放在 `RegisteredStream` 上，
/// 这样 `into_parts()` 就能通过普通解构把公开字段移出。
struct Cleanup(Option<Box<dyn FnOnce() + Send + 'static>>);

impl Drop for Cleanup {
    fn drop(&mut self) {
        if let Some(f) = self.0.take() {
            f();
        }
    }
}

/// 可等待的流发送端或接收端句柄。若在不调用 [`into_parts()`] 的情况下 drop，
/// 会执行可选的清理闭包，把注册项从流服务端的映射中移除。
pub struct RegisteredStream<T> {
    pub connection_info: ConnectionInfo,
    pub stream_provider: StreamProvider<T>,
    cleanup: Cleanup,
}

impl<T> std::fmt::Debug for RegisteredStream<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredStream")
            .field("connection_info", &self.connection_info)
            .finish_non_exhaustive()
    }
}

impl<T> RegisteredStream<T> {
    pub(crate) fn new(connection_info: ConnectionInfo, stream_provider: StreamProvider<T>) -> Self {
        Self {
            connection_info,
            stream_provider,
            cleanup: Cleanup(None),
        }
    }

    pub(crate) fn with_cleanup<F>(mut self, cleanup: F) -> Self
    where
        F: FnOnce() + Send + 'static,
    {
        self.cleanup.0 = Some(Box::new(cleanup));
        self
    }

    /// 消费这次注册并解除 RAII 清理。若调用方从未等待流提供者，就需要自行负责清理。
    pub fn into_parts(self) -> (ConnectionInfo, StreamProvider<T>) {
        let Self {
            connection_info,
            stream_provider,
            mut cleanup,
        } = self;
        cleanup.0.take();
        (connection_info, stream_provider)
    }
}

/// 注册流之后，会把 [`PendingConnections`] 对象返回给调用方。
/// 这个对象可用于等待连接建立。
pub struct PendingConnections {
    pub send_stream: Option<RegisteredStream<StreamSender>>,
    pub recv_stream: Option<RegisteredStream<StreamReceiver>>,
}

impl PendingConnections {
    pub fn into_parts(
        self,
    ) -> (
        Option<RegisteredStream<StreamSender>>,
        Option<RegisteredStream<StreamReceiver>>,
    ) {
        (self.send_stream, self.recv_stream)
    }
}

/// `ResponseService` 实现的是一种服务：在特定上下文和主题下关联一条响应流。
#[async_trait::async_trait]
pub trait ResponseService {
    async fn register(&self, options: StreamOptions) -> PendingConnections;
}

// === SECTION: 流发送端 / 流接收端 / 连接信息 / 流选项 ===
pub struct StreamSender {
    tx: tokio::sync::mpsc::Sender<TwoPartMessage>,
    prologue: Option<ResponseStreamPrologue>,
}

impl StreamSender {
    pub async fn send(&self, data: Bytes) -> Result<()> {
        Ok(self.tx.send(TwoPartMessage::from_data(data)).await?)
    }

    pub async fn send_control(&self, control: ControlMessage) -> Result<()> {
        let bytes = serde_json::to_vec(&control)?;
        Ok(self
            .tx
            .send(TwoPartMessage::from_header(bytes.into()))
            .await?)
    }

    #[allow(clippy::needless_update)]
    pub async fn send_prologue(&mut self, error: Option<String>) -> Result<(), String> {
        if let Some(_prologue) = self.prologue.take() {
            let prologue = ResponseStreamPrologue { error };
            let header_bytes: Bytes = match serde_json::to_vec(&prologue) {
                Ok(b) => b.into(),
                Err(err) => {
                    tracing::error!(%err, "send_prologue: ResponseStreamPrologue did not serialize to a JSON array");
                    return Err("Invalid prologue".to_string());
                }
            };
            self.tx
                .send(TwoPartMessage::from_header(header_bytes))
                .await
                .map_err(|e| e.to_string())?;
        } else {
            panic!("Prologue already sent; or not set; logic error");
        }
        Ok(())
    }
}

pub struct StreamReceiver {
    rx: tokio::sync::mpsc::Receiver<Bytes>,
}

/// `ConnectionInfo` 先序列化成 JSON，再作为传输层的一部分再次序列化。
/// 这种双重序列化不会成为性能瓶颈，因为它只会在每次连接建立时执行一次。
/// 把 ConnectionInfo 存成 JSON 字符串的主要原因是为了做类型擦除：
/// 传输层会先检查 [`ConnectionInfo::transport`]，再把 [`ConnectionInfo::info`]
/// 反序列化成对应传输实现内部的连接信息对象。
///
/// 另外一种可选方案是把这个对象改成强类型，但那样就必须枚举所有传输和连接信息的组合。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionInfo {
    pub transport: String,
    pub info: String,
}

/// 在服务端注册新的 TransportStream 时，调用方需要指定该流是发送端、接收端，
/// 还是两者兼有。
///
/// 发送端和接收端共享同一个 Context，但会分别对应到独立的 TCP socket 连接。
/// 在内部，我们可能会用广播通道来协调发送端和接收端 socket 之间的控制消息。
#[derive(Clone, Builder)]
pub struct StreamOptions {
    /// Context
    pub context: Arc<dyn AsyncEngineContext>,

    /// 向服务端注册这条连接会有一个服务端侧 Sender，
    /// 它会被 Request/Forward pipeline 接走。
    ///
    /// 注意：这个选项当前尚未实现，调用会直接 panic。
    pub enable_request_stream: bool,

    /// 向服务端注册这条连接会有一个服务端侧 Receiver，
    /// 它会被 Response/Reverse pipeline 接走。
    pub enable_response_stream: bool,

    /// 在阻塞前允许缓冲的消息数量。
    #[builder(default = "8")]
    pub send_buffer_count: usize,

    /// 在阻塞前允许缓冲的消息数量。
    #[builder(default = "8")]
    pub recv_buffer_count: usize,
}

impl StreamOptions {
    pub fn builder() -> StreamOptionsBuilder {
        StreamOptionsBuilder::default()
    }
}

// === SECTION: Egress / Ingress 引擎适配器 + PushWorkHandler ===
pub struct Egress<Req: PipelineIO, Resp: PipelineIO> {
    transport_engine: Arc<dyn AsyncTransportEngine<Req, Resp>>,
}

#[async_trait]
impl<T: Data, U: Data> AsyncEngine<SingleIn<T>, ManyOut<U>, Error>
    for Egress<SingleIn<T>, ManyOut<U>>
where
    T: Data + Serialize,
    U: for<'de> Deserialize<'de> + Data,
{
    async fn generate(&self, request: SingleIn<T>) -> Result<ManyOut<U>, Error> {
        self.transport_engine.generate(request).await
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RequestType {
    SingleIn,
    ManyIn,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ResponseType {
    SingleOut,
    ManyOut,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RequestControlMessage {
    id: String,
    request_type: RequestType,
    response_type: ResponseType,
    connection_info: ConnectionInfo,
    /// 发送时的墙钟时间戳（自 UNIX epoch 起的纳秒数），用于拆分传输延迟。
    /// 这里使用 `SystemTime`，所以精度取决于前后端主机之间的 NTP 同步情况。
    /// 适合单机性能分析；跨主机的数值应视为近似值。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    frontend_send_ts_ns: Option<u64>,
}

pub struct Ingress<Req: PipelineIO, Resp: PipelineIO> {
    segment: OnceLock<Arc<SegmentSource<Req, Resp>>>,
    metrics: OnceLock<Arc<WorkHandlerMetrics>>,
    /// 仅对应 PortName 的健康检查计时器重置通知器。
    portname_health_check_notifier: OnceLock<Arc<tokio::sync::Notify>>,
}

impl<Req: PipelineIO + Sync, Resp: PipelineIO> Ingress<Req, Resp> {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            segment: OnceLock::new(),
            metrics: OnceLock::new(),
            portname_health_check_notifier: OnceLock::new(),
        })
    }

    pub fn attach(&self, segment: Arc<SegmentSource<Req, Resp>>) -> Result<()> {
        self.segment
            .set(segment)
            .map_err(|_| anyhow::anyhow!("Segment already set"))
    }

    pub fn add_metrics(
        &self,
        portname: &crate::servicegroup::PortName,
        metrics_labels: Option<&[(&str, &str)]>,
    ) -> Result<()> {
        let metrics = WorkHandlerMetrics::from_portname(portname, metrics_labels)
            .map_err(|e| anyhow::anyhow!("Failed to create work handler metrics: {}", e))?;

        // 注册全局传输延迟分解指标（幂等）。
        crate::metrics::work_handler_perf::ensure_work_handler_perf_metrics_registered(
            portname.get_metrics_registry(),
        );

        // 注册 worker 池饱和度指标（幂等）。这些指标是进程全局的，
        // 会被挂到同一共享 TCP 服务端上的所有 portname 共享。
        crate::metrics::work_handler_pool::ensure_work_handler_pool_metrics_registered(
            portname.get_metrics_registry(),
        );

        self.metrics
            .set(Arc::new(metrics))
            .map_err(|_| anyhow::anyhow!("Metrics already set"))
    }

    pub fn link(segment: Arc<SegmentSource<Req, Resp>>) -> Result<Arc<Self>> {
        let ingress = Ingress::new();
        ingress.attach(segment)?;
        Ok(ingress)
    }

    pub fn for_pipeline(segment: Arc<SegmentSource<Req, Resp>>) -> Result<Arc<Self>> {
        let ingress = Ingress::new();
        ingress.attach(segment)?;
        Ok(ingress)
    }

    pub fn for_engine(engine: ServiceEngine<Req, Resp>) -> Result<Arc<Self>> {
        let frontend = SegmentSource::<Req, Resp>::new();
        let backend = ServiceBackend::from_engine(engine);

        // 创建管线。
        let pipeline = frontend.link(backend)?.link(frontend)?;

        let ingress = Ingress::new();
        ingress.attach(pipeline)?;

        Ok(ingress)
    }

    /// 如可用，则返回 metrics 的辅助方法。
    fn metrics(&self) -> Option<&Arc<WorkHandlerMetrics>> {
        self.metrics.get()
    }
}

#[async_trait]
pub trait PushWorkHandler: Send + Sync {
    async fn handle_payload(
        &self,
        payload: Bytes,
        request_id: Option<String>,
    ) -> Result<(), PipelineError>;

    /// 为处理器添加 metrics。
    fn add_metrics(
        &self,
        portname: &crate::servicegroup::PortName,
        metrics_labels: Option<&[(&str, &str)]>,
    ) -> Result<()>;

    /// 设置仅对应 PortName 的健康检查计时器重置通知器。
    fn set_portname_health_check_notifier(
        &self,
        _notifier: Arc<tokio::sync::Notify>,
    ) -> Result<()> {
        // 为兼容旧实现保留的默认实现。
        Ok(())
    }
}

    // === SECTION: 流结束包装器 ===
    /// TODO：改用 Server-Sent Events（SSE）检测流结束，后续会移除。
#[derive(Serialize, Deserialize, Debug)]
pub struct NetworkStreamWrapper<U> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<U>,
    pub complete_final: bool,
}

// === SECTION: 测试 - RegisteredStream 清理语义 ===
#[cfg(test)]
mod registered_stream_tests {
    //! ## 测试矩阵
    //!
    //! | 测试名 | 覆盖维度 |
    //! |---|---|
    //! | `drop_runs_cleanup` | RAII：未 `into_parts` 直接 drop 必触发清理闭包 |
    //! | `into_parts_disarms_cleanup` | `into_parts()` 必须解除 RAII，调用方接管清理 |
    //! | `drop_without_cleanup_is_a_noop` | 未配置 cleanup 时 drop 必须安全无副作用 |
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn dummy_conn_info() -> ConnectionInfo {
        ConnectionInfo {
            transport: "test".to_string(),
            info: "{}".to_string(),
        }
    }

    /// 不调用 `into_parts()` 直接 drop 时，必须执行清理闭包。
    #[test]
    fn drop_runs_cleanup() {
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = flag.clone();

        let (_tx, rx) = tokio::sync::oneshot::channel::<Result<(), String>>();
        let stream = RegisteredStream::new(dummy_conn_info(), rx).with_cleanup(move || {
            flag_clone.store(true, Ordering::SeqCst);
        });

        drop(stream);
        assert!(
            flag.load(Ordering::SeqCst),
            "cleanup must fire when RegisteredStream is dropped"
        );
    }

    /// `into_parts()` 必须解除清理。调用之后，再 drop 返回的各个部分时
    /// 不能触发闭包——清理责任已经转交给调用方。
    #[test]
    fn into_parts_disarms_cleanup() {
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = flag.clone();

        let (_tx, rx) = tokio::sync::oneshot::channel::<Result<(), String>>();
        let stream = RegisteredStream::new(dummy_conn_info(), rx).with_cleanup(move || {
            flag_clone.store(true, Ordering::SeqCst);
        });

        let (conn, provider) = stream.into_parts();
        drop(conn);
        drop(provider);

        assert!(
            !flag.load(Ordering::SeqCst),
            "into_parts() must disarm the cleanup closure"
        );
    }

    /// 没有配置清理的 `RegisteredStream` 必须能够平稳 drop。
    #[test]
    fn drop_without_cleanup_is_a_noop() {
        let (_tx, rx) = tokio::sync::oneshot::channel::<Result<(), String>>();
        let stream: RegisteredStream<()> = RegisteredStream::new(dummy_conn_info(), rx);
        drop(stream); // must not panic; nothing observable to assert beyond that
    }
}