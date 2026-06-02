// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 事件平面：与传输无关的 pub/sub 抽象
//!
//! ## 设计意图
//! 给上层提供"我只关心发什么 / 订阅什么 topic"的 API，**底层走 NATS 还是 ZMQ
//! 由配置决定**。三层结构：
//!
//! ```text
//!   EventPublisher  ─┐
//!                    ├─→  EventTransportTx (trait obj) ─→  NATS / ZMQ-direct / ZMQ-broker
//!   EventSubscriber ─┘                                       │
//!                          ┌──────────────────────────────┐  │
//!                          │  DiscoveryInstance 注册/反注册 │  │
//!                          └──────────────────────────────┘  │
//!                                                            │
//!   多 broker HA 时：DeduplicatingStream 按 (publisher_id, sequence) 去重
//! ```
//!
//! - **Scope**：[`EventScope::Namespace`] / [`EventScope::ServiceGroup`] 决定
//!   subject 前缀（namespace.X / namespace.X.servicegroup.Y）。
//! - **Discovery 注册**：Publisher 在 NATS 模式或 ZMQ 直连模式下会把自己注册到
//!   discovery；broker 模式不需要（订阅侧通过 broker 找到 publisher）。
//! - **Drop 反注册**：Publisher 在 Drop 时把 discovery 实例反注册掉 ——
//!   这里用"绑定到创建时 runtime 的 spawn"以适配 PyO3 finalizer 等无 tokio
//!   上下文场景。
//!
//! ## 外部契约
//! - [`EventScope`] + `subject_prefix / namespace / servicegroup`
//! - [`EventPublisher`]：`for_servicegroup / for_servicegroup_with_transport /
//!   for_namespace / for_namespace_with_transport / publish<T> / publish_bytes /
//!   publisher_id / topic / transport_kind`
//! - [`EventSubscriber`]：上述的对偶 + `next() / typed::<T>()`
//! - [`TypedEventSubscriber<T>`]：`next() -> Option<Result<(EventEnvelope, T)>>`
//! - Re-export：`Codec / MsgpackCodec / DynamicSubscriber / Frame* / EventEnvelope /
//!   EventStream / TypedEventStream / EventTransportRx / EventTransportTx /
//!   WireStream / ZmqPubTransport / ZmqSubTransport / EventTransportKind`
//! - 内部 `parse_broker_url / resolve_zmq_broker / DeduplicatingStream /
//!   BrokerEndpoints` 仍 crate-private 但保留供测试访问。
//!
//! ## 实现要点
//! 1. 把 `EventPublisher::new_internal` 里那个 600 行的"先准备 transport_setup、
//!    再 register discovery"超长函数**拆**成两步：[`prepare_publisher_transport`]
//!    + [`register_publisher_with_discovery`]。每步语义明确、可独立阅读。
//! 2. `parse_broker_url` 内部用一次 `match` 而不是先 trim 再 strip_prefix
//!    串行试错 —— 错误信息字面值稳定（测试在断言这些字串）。
//! 3. Drop 处的 spawn fallback 仍用 `catch_unwind`，但提取成一个常量字符串
//!    避免在 panic 路径再分配。

// =============================================================================
// === 子模块声明 + 重新导出 ===================================================
// =============================================================================

mod codec;
mod dynamic_subscriber;
mod frame;
mod nats_transport;
mod traits;
mod transport;
pub mod zmq_transport;

pub use codec::{Codec, MsgpackCodec};
pub use dynamic_subscriber::DynamicSubscriber;
pub use frame::{FRAME_HEADER_SIZE, FRAME_VERSION, Frame, FrameError, FrameHeader};
pub use traits::{EventEnvelope, EventStream, TypedEventStream};
pub use transport::{EventTransportRx, EventTransportTx, WireStream};
pub use zmq_transport::{ZmqPubTransport, ZmqSubTransport};

// 方便从 event_plane 直接拿到 transport kind 枚举，避免上层多写一行 use。
pub use crate::discovery::EventTransportKind;

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use lru::LruCache;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::DistributedRuntime;
use crate::servicegroup::{ServiceGroup, Namespace};
use crate::discovery::{
    Discovery, DiscoveryInstance, DiscoveryQuery, DiscoverySpec, EventChannelQuery, EventTransport,
};
use crate::traits::DistributedRuntimeProvider;
use crate::utils::ip_resolver::get_local_ip_for_advertise;

// =============================================================================
// === EventScope ==============================================================
// =============================================================================

/// 事件平面的"作用域"—— 决定 subject 前缀和发现层注册的 servicegroup 字段。
#[derive(Debug, Clone)]
pub enum EventScope {
    /// Namespace 级：`namespace.{name}`
    Namespace { name: String },
    /// ServiceGroup 级：`namespace.{namespace}.servicegroup.{servicegroup}`
    ServiceGroup {
        namespace: String,
        servicegroup: String,
    },
}

impl EventScope {
    /// 该作用域的 subject 前缀（不含 topic 段）。
    pub fn subject_prefix(&self) -> String {
        match self {
            EventScope::Namespace { name } => format!("namespace.{}", name),
            EventScope::ServiceGroup {
                namespace,
                servicegroup,
            } => {
                format!("namespace.{}.servicegroup.{}", namespace, servicegroup)
            }
        }
    }

    pub fn namespace(&self) -> &str {
        match self {
            EventScope::Namespace { name } => name,
            EventScope::ServiceGroup { namespace, .. } => namespace,
        }
    }

    pub fn servicegroup(&self) -> Option<&str> {
        match self {
            EventScope::Namespace { .. } => None,
            EventScope::ServiceGroup { servicegroup, .. } => Some(servicegroup),
        }
    }
}

// =============================================================================
// === Broker 解析 =============================================================
// =============================================================================

/// ZMQ broker 模式的两端：xsub（publisher 连入）/ xpub（subscriber 连入）。
#[derive(Debug, Clone)]
struct BrokerEndpoints {
    xsub_endpoints: Vec<String>,
    xpub_endpoints: Vec<String>,
}

/// 从环境变量或 discovery 拿 broker 端点；返回 `None` 表示走直连模式。
async fn resolve_zmq_broker(
    drt: &DistributedRuntime,
    scope: &EventScope,
) -> Result<Option<BrokerEndpoints>> {
    // 第 1 优先级：显式 URL
    if let Ok(broker_url) =
        std::env::var(crate::config::environment_names::zmq_broker::PGD_ZMQ_BROKER_URL)
    {
        let (xsub_endpoints, xpub_endpoints) = parse_broker_url(&broker_url)?;
        tracing::info!(
            num_xsub = xsub_endpoints.len(),
            num_xpub = xpub_endpoints.len(),
            "Using explicit ZMQ broker URL"
        );
        return Ok(Some(BrokerEndpoints {
            xsub_endpoints,
            xpub_endpoints,
        }));
    }

    // 第 2 优先级：discovery 查找
    if std::env::var(crate::config::environment_names::zmq_broker::PGD_ZMQ_BROKER_ENABLED)
        .unwrap_or_default()
        == "true"
    {
        let query = DiscoveryQuery::EventChannels(EventChannelQuery::servicegroup(
            scope.namespace().to_string(),
            "zmq_broker".to_string(),
        ));

        let instances = drt.discovery().list(query).await?;

        // 收集所有 broker 实例（HA 时有多份）
        let mut xsub_endpoints = Vec::new();
        let mut xpub_endpoints = Vec::new();

        for instance in instances {
            if let DiscoveryInstance::EventChannel { transport, .. } = instance
                && let EventTransport::ZmqBroker {
                    xsub_endpoints: xsubs,
                    xpub_endpoints: xpubs,
                } = transport
            {
                xsub_endpoints.extend(xsubs);
                xpub_endpoints.extend(xpubs);
            }
        }

        if xsub_endpoints.is_empty() {
            anyhow::bail!(
                "PGD_ZMQ_BROKER_ENABLED=true but no broker found in discovery for namespace '{}'",
                scope.namespace()
            );
        }

        tracing::info!(
            num_brokers = xsub_endpoints.len(),
            "Discovered ZMQ brokers from discovery plane"
        );

        return Ok(Some(BrokerEndpoints {
            xsub_endpoints,
            xpub_endpoints,
        }));
    }

    Ok(None)
}

/// 解析形如 `"xsub=tcp://h1:5555;tcp://h2:5555 , xpub=tcp://h1:5556"` 的 URL。
fn parse_broker_url(url: &str) -> Result<(Vec<String>, Vec<String>)> {
    let parts: Vec<&str> = url.split(',').map(|s| s.trim()).collect();
    if parts.len() != 2 {
        anyhow::bail!(
            "Invalid broker URL format. Expected 'xsub=<urls> , xpub=<urls>', got: {}",
            url
        );
    }

    let mut xsub_endpoints = Vec::new();
    let mut xpub_endpoints = Vec::new();

    for part in parts {
        if let Some(urls_str) = part.strip_prefix("xsub=") {
            xsub_endpoints = urls_str
                .split(';')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        } else if let Some(urls_str) = part.strip_prefix("xpub=") {
            xpub_endpoints = urls_str
                .split(';')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        } else {
            anyhow::bail!(
                "Invalid broker URL part. Expected 'xsub=' or 'xpub=' prefix, got: {}",
                part
            );
        }
    }

    if xsub_endpoints.is_empty() || xpub_endpoints.is_empty() {
        anyhow::bail!(
            "Broker URL must contain at least one xsub and one xpub portname. Got xsub={:?}, xpub={:?}",
            xsub_endpoints,
            xpub_endpoints
        );
    }

    Ok((xsub_endpoints, xpub_endpoints))
}

// =============================================================================
// === 去重流（多 broker HA 才需要）============================================
// =============================================================================

/// 包装一条 WireStream，按 `(publisher_id, sequence)` 去重。多 broker HA 时使用。
struct DeduplicatingStream {
    inner: WireStream,
    codec: Arc<Codec>,
    seen_events: LruCache<(u64, u64), ()>,
}

impl DeduplicatingStream {
    fn new(inner: WireStream, codec: Arc<Codec>, cache_size: usize) -> Self {
        Self {
            inner,
            codec,
            seen_events: LruCache::new(
                NonZeroUsize::new(cache_size).expect("cache_size must be non-zero"),
            ),
        }
    }
}

impl Stream for DeduplicatingStream {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(bytes))) => {
                    match self.codec.decode_envelope(&bytes) {
                        Ok(envelope) => {
                            let key = (envelope.publisher_id, envelope.sequence);
                            if self.seen_events.contains(&key) {
                                tracing::debug!(
                                    publisher_id = envelope.publisher_id,
                                    sequence = envelope.sequence,
                                    "Filtered duplicate event from multi-broker setup"
                                );
                                continue;
                            }
                            self.seen_events.put(key, ());
                            return Poll::Ready(Some(Ok(bytes)));
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "Failed to decode envelope for deduplication");
                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// =============================================================================
// === EventPublisher ==========================================================
// =============================================================================

/// 发布端：把序列化好的 payload 套上 envelope 发到某个 topic。
pub struct EventPublisher {
    transport_kind: EventTransportKind,
    scope: EventScope,
    topic: String,
    publisher_id: u64,
    sequence: AtomicU64,
    tx: Arc<dyn EventTransportTx>,
    codec: Arc<Codec>,
    runtime_handle: tokio::runtime::Handle,
    /// Drop 时用来反注册的 discovery 句柄
    discovery_client: Option<Arc<dyn Discovery>>,
    discovery_instance: Option<crate::discovery::DiscoveryInstance>,
}

/// 内部表示：根据 transport 类型构造出的"已就位"的 publisher 资源。
enum PublisherSetup {
    Nats(Arc<dyn EventTransportTx>, Arc<Codec>),
    /// 直连 ZMQ：需要把"对外可访问的 portname"注册进 discovery。
    ZmqDirect(Arc<dyn EventTransportTx>, Arc<Codec>, String),
    /// Broker 模式：subscriber 通过 broker 自找 publisher，不必注册。
    ZmqBroker(Arc<dyn EventTransportTx>, Arc<Codec>),
}

/// 把 transport_kind 解析成 PublisherSetup（仅准备 socket / 客户端，不碰 discovery）。
async fn prepare_publisher_transport(
    drt: &DistributedRuntime,
    scope: &EventScope,
    topic: &str,
    transport_kind: EventTransportKind,
) -> Result<PublisherSetup> {
    match transport_kind {
        EventTransportKind::Nats => {
            let transport = Arc::new(nats_transport::NatsTransport::new(drt.clone()));
            let codec = Arc::new(Codec::Msgpack(MsgpackCodec));
            Ok(PublisherSetup::Nats(
                transport as Arc<dyn EventTransportTx>,
                codec,
            ))
        }
        EventTransportKind::Zmq => {
            if let Some(broker) = resolve_zmq_broker(drt, scope).await? {
                // BROKER 模式
                let pub_transport = if broker.xsub_endpoints.len() == 1 {
                    zmq_transport::ZmqPubTransport::connect(&broker.xsub_endpoints[0], topic)
                        .await?
                } else {
                    zmq_transport::ZmqPubTransport::connect_multiple(&broker.xsub_endpoints, topic)
                        .await?
                };

                let codec = Arc::new(Codec::Msgpack(MsgpackCodec));
                Ok(PublisherSetup::ZmqBroker(
                    Arc::new(pub_transport) as Arc<dyn EventTransportTx>,
                    codec,
                ))
            } else {
                // 直连模式：先在 0.0.0.0:0 bind 一个 ZMQ socket，再把宣告 portname 算出来
                let (pub_transport, actual_bind_endpoint) = std::thread::spawn({
                    let topic = topic.to_string();
                    move || {
                        let rt = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .expect("Failed to create Tokio runtime for ZMQ");

                        rt.block_on(async move {
                            zmq_transport::ZmqPubTransport::bind("tcp://0.0.0.0:0", &topic)
                                .await
                                .expect("Failed to bind ZMQ publisher")
                        })
                    }
                })
                .join()
                .expect("Failed to join ZMQ initialization thread");

                let actual_port: u16 = actual_bind_endpoint
                    .rsplit(':')
                    .next()
                    .and_then(|s| s.parse().ok())
                    .expect("Failed to parse port from bind portname");
                let local_ip = get_local_ip_for_advertise();
                let public_endpoint = format!("tcp://{}:{}", local_ip, actual_port);

                let codec = Arc::new(Codec::Msgpack(MsgpackCodec));
                Ok(PublisherSetup::ZmqDirect(
                    Arc::new(pub_transport) as Arc<dyn EventTransportTx>,
                    codec,
                    public_endpoint,
                ))
            }
        }
    }
}

/// 把 PublisherSetup 注册到 discovery（Nats / ZmqDirect 才有这一步），返回
/// (tx, codec, discovery_instance)。
async fn register_publisher_with_discovery(
    drt: &DistributedRuntime,
    scope: &EventScope,
    topic: &str,
    transport_kind: EventTransportKind,
    setup: PublisherSetup,
) -> Result<(
    Arc<dyn EventTransportTx>,
    Arc<Codec>,
    Option<DiscoveryInstance>,
)> {
    match setup {
        PublisherSetup::Nats(tx, codec) => {
            let transport_config = EventTransport::nats(scope.subject_prefix());
            let spec = DiscoverySpec::EventChannel {
                namespace: scope.namespace().to_string(),
                servicegroup: scope.servicegroup().unwrap_or("").to_string(),
                topic: topic.to_string(),
                transport: transport_config,
            };
            let registered = drt.discovery().register(spec).await?;
            tracing::info!(
                topic = %topic,
                transport = ?transport_kind,
                instance_id = %registered.instance_id(),
                "EventPublisher registered with discovery"
            );
            Ok((tx, codec, Some(registered)))
        }
        PublisherSetup::ZmqDirect(tx, codec, public_endpoint) => {
            let transport_config = EventTransport::zmq(public_endpoint);
            let spec = DiscoverySpec::EventChannel {
                namespace: scope.namespace().to_string(),
                servicegroup: scope.servicegroup().unwrap_or("").to_string(),
                topic: topic.to_string(),
                transport: transport_config,
            };
            let registered = drt.discovery().register(spec).await?;
            tracing::info!(
                topic = %topic,
                transport = ?transport_kind,
                instance_id = %registered.instance_id(),
                "EventPublisher registered with discovery (direct mode)"
            );
            Ok((tx, codec, Some(registered)))
        }
        PublisherSetup::ZmqBroker(tx, codec) => {
            tracing::info!(
                topic = %topic,
                transport = ?transport_kind,
                "EventPublisher in broker mode - skipping discovery registration"
            );
            Ok((tx, codec, None))
        }
    }
}

impl EventPublisher {
    /// 组件作用域 publisher，自动选 transport。
    pub async fn for_servicegroup(comp: &ServiceGroup, topic: impl Into<String>) -> Result<Self> {
        let transport_kind = comp.drt().default_event_transport_kind();
        Self::for_servicegroup_with_transport(comp, topic, transport_kind).await
    }

    pub async fn for_servicegroup_with_transport(
        comp: &ServiceGroup,
        topic: impl Into<String>,
        transport_kind: EventTransportKind,
    ) -> Result<Self> {
        let drt = comp.drt();
        let scope = EventScope::ServiceGroup {
            namespace: comp.namespace().name(),
            servicegroup: comp.name().to_string(),
        };
        Self::new_internal(drt, scope, topic.into(), transport_kind).await
    }

    pub async fn for_namespace(ns: &Namespace, topic: impl Into<String>) -> Result<Self> {
        let transport_kind = ns.drt().default_event_transport_kind();
        Self::for_namespace_with_transport(ns, topic, transport_kind).await
    }

    pub async fn for_namespace_with_transport(
        ns: &Namespace,
        topic: impl Into<String>,
        transport_kind: EventTransportKind,
    ) -> Result<Self> {
        let drt = ns.drt();
        let scope = EventScope::Namespace { name: ns.name() };
        Self::new_internal(drt, scope, topic.into(), transport_kind).await
    }

    async fn new_internal(
        drt: &DistributedRuntime,
        scope: EventScope,
        topic: String,
        transport_kind: EventTransportKind,
    ) -> Result<Self> {
        let publisher_id = drt.discovery().instance_id();
        let discovery = Some(drt.discovery());
        let runtime_handle = drt.runtime().secondary();

        let setup = prepare_publisher_transport(drt, &scope, &topic, transport_kind).await?;
        let (tx, codec, discovery_instance) =
            register_publisher_with_discovery(drt, &scope, &topic, transport_kind, setup).await?;

        Ok(Self {
            transport_kind,
            scope,
            topic,
            publisher_id,
            sequence: AtomicU64::new(0),
            tx,
            codec,
            runtime_handle,
            discovery_client: discovery,
            discovery_instance,
        })
    }

    /// 发布一个可序列化对象。
    pub async fn publish<T: Serialize + Send + Sync>(&self, event: &T) -> Result<()> {
        let payload = self.codec.encode_payload(event)?;
        self.publish_bytes(payload.to_vec()).await
    }

    /// 发布裸字节 payload。
    pub async fn publish_bytes(&self, bytes: Vec<u8>) -> Result<()> {
        let envelope = EventEnvelope {
            publisher_id: self.publisher_id,
            sequence: self.sequence.fetch_add(1, Ordering::SeqCst),
            published_at: current_timestamp_ms(),
            topic: self.topic.clone(),
            payload: Bytes::from(bytes),
        };

        let envelope_bytes = self.codec.encode_envelope(&envelope)?;
        let subject = format!("{}.{}", self.scope.subject_prefix(), self.topic);

        self.tx.publish(&subject, envelope_bytes).await
    }

    pub fn publisher_id(&self) -> u64 {
        self.publisher_id
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn transport_kind(&self) -> EventTransportKind {
        self.transport_kind
    }
}

impl Drop for EventPublisher {
    fn drop(&mut self) {
        // 在 drop 里反注册。注意 drop 可能跑在没有 tokio 上下文的线程（典型场景：
        // PyO3 finalizer 在主线程），所以必须用创建时持有的 runtime_handle.spawn，
        // 而不是 tokio::spawn。
        if let (Some(discovery), Some(instance)) =
            (self.discovery_client.take(), self.discovery_instance.take())
        {
            let topic = self.topic.clone();
            let instance_id = instance.instance_id();
            let runtime_handle = self.runtime_handle.clone();

            let spawn_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                runtime_handle.spawn(async move {
                    match discovery.unregister(instance).await {
                        Ok(()) => {
                            tracing::info!(
                                topic = %topic,
                                instance_id = %instance_id,
                                "EventPublisher unregistered from discovery"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                topic = %topic,
                                instance_id = %instance_id,
                                error = %e,
                                "Failed to unregister EventPublisher from discovery"
                            );
                        }
                    }
                });
            }));

            if spawn_result.is_err() {
                tracing::warn!(
                    topic = %self.topic,
                    instance_id = %instance_id,
                    "Skipping EventPublisher unregister during drop because the runtime is unavailable"
                );
            }
        }
    }
}

// =============================================================================
// === EventSubscriber =========================================================
// =============================================================================

/// 订阅端：拿到该 topic 的 envelope 流。可进一步 `typed::<T>()` 解码 payload。
pub struct EventSubscriber {
    stream: EventStream,
    #[allow(dead_code)]
    scope: EventScope,
    #[allow(dead_code)]
    topic: String,
    codec: Arc<Codec>,
}

impl EventSubscriber {
    pub async fn for_servicegroup(comp: &ServiceGroup, topic: impl Into<String>) -> Result<Self> {
        let transport_kind = comp.drt().default_event_transport_kind();
        Self::for_servicegroup_with_transport(comp, topic, transport_kind).await
    }

    pub async fn for_servicegroup_with_transport(
        comp: &ServiceGroup,
        topic: impl Into<String>,
        transport_kind: EventTransportKind,
    ) -> Result<Self> {
        let drt = comp.drt();
        let scope = EventScope::ServiceGroup {
            namespace: comp.namespace().name(),
            servicegroup: comp.name().to_string(),
        };
        Self::new_internal(drt, scope, topic.into(), transport_kind).await
    }

    pub async fn for_namespace(ns: &Namespace, topic: impl Into<String>) -> Result<Self> {
        let transport_kind = ns.drt().default_event_transport_kind();
        Self::for_namespace_with_transport(ns, topic, transport_kind).await
    }

    pub async fn for_namespace_with_transport(
        ns: &Namespace,
        topic: impl Into<String>,
        transport_kind: EventTransportKind,
    ) -> Result<Self> {
        let drt = ns.drt();
        let scope = EventScope::Namespace { name: ns.name() };
        Self::new_internal(drt, scope, topic.into(), transport_kind).await
    }

    async fn new_internal(
        drt: &DistributedRuntime,
        scope: EventScope,
        topic: String,
        transport_kind: EventTransportKind,
    ) -> Result<Self> {
        let discovery = drt.discovery();

        let (wire_stream, codec): (WireStream, Arc<Codec>) = match transport_kind {
            EventTransportKind::Nats => {
                let transport = nats_transport::NatsTransport::new(drt.clone());
                let subject = format!("{}.{}", scope.subject_prefix(), topic);
                let stream = transport.subscribe(&subject).await?;
                let codec = Arc::new(Codec::Msgpack(MsgpackCodec));
                (stream, codec)
            }
            EventTransportKind::Zmq => {
                if let Some(broker) = resolve_zmq_broker(drt, &scope).await? {
                    let codec = Arc::new(Codec::Msgpack(MsgpackCodec));

                    let stream: WireStream = if broker.xpub_endpoints.len() == 1 {
                        // 单 broker：不需要去重
                        let sub_transport = zmq_transport::ZmqSubTransport::connect_broker(
                            &broker.xpub_endpoints[0],
                            &topic,
                        )
                        .await?;
                        sub_transport.subscribe(&topic).await?
                    } else {
                        // 多 broker：需要去重（容量 100k 项）
                        let sub_transport = zmq_transport::ZmqSubTransport::connect_broker_multiple(
                            &broker.xpub_endpoints,
                            &topic,
                        )
                        .await?;
                        let inner_stream = sub_transport.subscribe(&topic).await?;
                        Box::pin(DeduplicatingStream::new(
                            inner_stream,
                            codec.clone(),
                            100_000,
                        ))
                    };

                    (stream, codec)
                } else {
                    // 直连模式：靠 DynamicSubscriber 跟随 discovery 动态连
                    let query = match &scope {
                        EventScope::Namespace { name } => {
                            crate::discovery::DiscoveryQuery::EventChannels(
                                crate::discovery::EventChannelQuery::namespace(name.clone()),
                            )
                        }
                        EventScope::ServiceGroup {
                            namespace,
                            servicegroup,
                        } => crate::discovery::DiscoveryQuery::EventChannels(
                            crate::discovery::EventChannelQuery::topic(
                                namespace.clone(),
                                servicegroup.clone(),
                                topic.clone(),
                            ),
                        ),
                    };

                    let subscriber = Arc::new(DynamicSubscriber::new(discovery, query, topic.clone()));
                    let stream = subscriber.start_zmq().await?;
                    let codec = Arc::new(Codec::Msgpack(MsgpackCodec));
                    (stream, codec)
                }
            }
        };

        // 包一层 filter_map：按 topic 过滤 + 解 envelope
        let topic_filter = topic.clone();
        let codec_for_stream = codec.clone();
        let stream = wire_stream.filter_map(move |result| {
            let codec = codec_for_stream.clone();
            let topic_filter = topic_filter.clone();
            async move {
                match result {
                    Ok(bytes) => match codec.decode_envelope(&bytes) {
                        Ok(envelope) => {
                            if envelope.topic == topic_filter {
                                Some(Ok(envelope))
                            } else {
                                None
                            }
                        }
                        Err(e) => Some(Err(e)),
                    },
                    Err(e) => Some(Err(e)),
                }
            }
        });

        tracing::info!(
            topic = %topic,
            transport = ?transport_kind,
            "EventSubscriber created"
        );

        Ok(Self {
            stream: Box::pin(stream),
            scope,
            topic,
            codec,
        })
    }

    pub async fn next(&mut self) -> Option<Result<EventEnvelope>> {
        self.stream.next().await
    }

    pub fn typed<T: DeserializeOwned + Send + 'static>(self) -> TypedEventSubscriber<T> {
        TypedEventSubscriber {
            stream: self.stream,
            codec: self.codec,
            _marker: std::marker::PhantomData,
        }
    }
}

// =============================================================================
// === TypedEventSubscriber ====================================================
// =============================================================================

/// 包装 EventSubscriber，把 payload 自动反序列化为 `T`。
pub struct TypedEventSubscriber<T> {
    stream: EventStream,
    codec: Arc<Codec>,
    _marker: std::marker::PhantomData<T>,
}

impl<T: DeserializeOwned + Send + 'static> TypedEventSubscriber<T> {
    pub async fn next(&mut self) -> Option<Result<(EventEnvelope, T)>> {
        let envelope = match self.stream.next().await? {
            Ok(env) => env,
            Err(e) => return Some(Err(e)),
        };
        match self.codec.decode_payload(&envelope.payload) {
            Ok(typed) => Some(Ok((envelope, typed))),
            Err(e) => Some(Err(e)),
        }
    }
}

// =============================================================================
// === 时间戳 ==================================================================
// =============================================================================

/// 当前 Unix epoch 毫秒。时钟异常时返回 0（不向上传递错误，节省调用方分支）。
fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// =============================================================================
// === 单元测试 ===============================================
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use crate::config::environment_names::event_plane as env;

    #[test]
    fn test_event_scope_subject_prefix() {
        let ns_scope = EventScope::Namespace {
            name: "test-ns".to_string(),
        };
        assert_eq!(ns_scope.subject_prefix(), "namespace.test-ns");

        let comp_scope = EventScope::ServiceGroup {
            namespace: "test-ns".to_string(),
            servicegroup: "test-comp".to_string(),
        };
        assert_eq!(
            comp_scope.subject_prefix(),
            "namespace.test-ns.servicegroup.test-comp"
        );
    }

    #[test]
    fn test_event_scope_accessors() {
        let ns_scope = EventScope::Namespace {
            name: "my-ns".to_string(),
        };
        assert_eq!(ns_scope.namespace(), "my-ns");
        assert_eq!(ns_scope.servicegroup(), None);

        let comp_scope = EventScope::ServiceGroup {
            namespace: "my-ns".to_string(),
            servicegroup: "my-comp".to_string(),
        };
        assert_eq!(comp_scope.namespace(), "my-ns");
        assert_eq!(comp_scope.servicegroup(), Some("my-comp"));
    }

    #[test]
    fn test_timestamp_generation() {
        let ts = current_timestamp_ms();
        // 2020-01-01 至 2100-01-01
        assert!(ts > 1577836800000, "Timestamp should be after 2020");
        assert!(ts < 4102444800000, "Timestamp should be before 2100");
    }

    #[test]
    fn test_event_envelope_serde() {
        let envelope = EventEnvelope {
            publisher_id: 42,
            sequence: 10,
            published_at: 1700000000000,
            topic: "test-topic".to_string(),
            payload: Bytes::from("test data"),
        };

        let json = serde_json::to_string(&envelope).expect("serialize");
        let deserialized: EventEnvelope = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(deserialized.publisher_id, 42);
        assert_eq!(deserialized.sequence, 10);
        assert_eq!(deserialized.published_at, 1700000000000);
        assert_eq!(deserialized.topic, "test-topic");
        assert_eq!(deserialized.payload, Bytes::from("test data"));
    }

    // === SECTION: 合并自 supplemental_tests 模块 ===
    use super::*;
    use crate::config::environment_names::zmq_broker as env_zmq_broker;
    use crate::discovery::EventTransportKind;
    use crate::distributed::DistributedConfig;
    use crate::transports::event_plane::transport::EventTransportTx;
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::StreamExt;
    use serde::Serialize;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Debug, Clone, PartialEq, Serialize, serde::Deserialize)]
    struct TestPayload {
        id: u64,
        body: String,
    }

    #[derive(Default)]
    struct RecordingTx {
        calls: Mutex<Vec<(String, Bytes)>>,
        fail: bool,
    }

    impl RecordingTx {
        fn with_fail(fail: bool) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                fail,
            }
        }
    }

    #[async_trait]
    impl EventTransportTx for RecordingTx {
        async fn publish(&self, subject: &str, envelope_bytes: Bytes) -> Result<()> {
            if self.fail {
                anyhow::bail!("forced tx failure");
            }
            self.calls
                .lock()
                .await
                .push((subject.to_string(), envelope_bytes));
            Ok(())
        }

        fn kind(&self) -> EventTransportKind {
            EventTransportKind::Zmq
        }
    }

    fn encode_envelope(topic: &str, publisher_id: u64, sequence: u64, payload: &[u8]) -> Bytes {
        let envelope = EventEnvelope {
            publisher_id,
            sequence,
            published_at: 1700000000000,
            topic: topic.to_string(),
            payload: Bytes::copy_from_slice(payload),
        };
        MsgpackCodec
            .encode_envelope(&envelope)
            .expect("envelope encoding should succeed")
    }

    async fn create_process_local_drt() -> DistributedRuntime {
        let runtime = crate::Runtime::from_current().expect("runtime should exist");
        DistributedRuntime::new(runtime, DistributedConfig::process_local())
            .await
            .expect("process-local DRT should initialize")
    }

    #[test]
    fn parse_broker_url_success_and_whitespace() {
        let (xsub, xpub) = parse_broker_url(
            "xsub=tcp://127.0.0.1:5555 ; tcp://127.0.0.1:5557, xpub=tcp://127.0.0.1:5556",
        )
        .expect("parsing valid broker URL should succeed");

        assert_eq!(xsub.len(), 2);
        assert_eq!(xsub[0], "tcp://127.0.0.1:5555");
        assert_eq!(xsub[1], "tcp://127.0.0.1:5557");
        assert_eq!(xpub, vec!["tcp://127.0.0.1:5556"]);
    }

    #[test]
    fn parse_broker_url_rejects_invalid_inputs() {
        let err = parse_broker_url("xsub=tcp://1:1").expect_err("single-part URL must fail");
        assert!(err.to_string().contains("Invalid broker URL format"));

        let err =
            parse_broker_url("foo=tcp://1:1, xpub=tcp://1:2").expect_err("unknown key must fail");
        assert!(err.to_string().contains("Expected 'xsub=' or 'xpub='"));

        let err =
            parse_broker_url("xsub=, xpub=tcp://1:2").expect_err("empty xsub list must fail");
        assert!(err.to_string().contains("must contain at least one xsub"));
    }

    #[tokio::test]
    async fn resolve_zmq_broker_uses_explicit_url_first() {
        temp_env::async_with_vars(
            vec![
                (
                    env_zmq_broker::PGD_ZMQ_BROKER_URL,
                    Some("xsub=tcp://10.0.0.1:5555, xpub=tcp://10.0.0.1:5556"),
                ),
                (env_zmq_broker::PGD_ZMQ_BROKER_ENABLED, Some("true")),
            ],
            async {
                let drt = create_process_local_drt().await;
                let scope = EventScope::Namespace {
                    name: "ns-explicit".to_string(),
                };

                let broker = resolve_zmq_broker(&drt, &scope)
                    .await
                    .expect("resolve should succeed")
                    .expect("explicit URL should produce broker portnames");

                assert_eq!(broker.xsub_endpoints, vec!["tcp://10.0.0.1:5555"]);
                assert_eq!(broker.xpub_endpoints, vec!["tcp://10.0.0.1:5556"]);
                drt.shutdown();
            },
        )
        .await;
    }

    #[tokio::test]
    async fn resolve_zmq_broker_discovers_from_registry_and_handles_missing() {
        temp_env::async_with_vars(
            vec![
                (env_zmq_broker::PGD_ZMQ_BROKER_URL, None::<&str>),
                (env_zmq_broker::PGD_ZMQ_BROKER_ENABLED, Some("true")),
            ],
            async {
                let drt = create_process_local_drt().await;
                let scope = EventScope::Namespace {
                    name: "ns-discovery".to_string(),
                };

                let empty_err = resolve_zmq_broker(&drt, &scope)
                    .await
                    .err()
                    .expect("missing broker should error");
                assert!(empty_err.to_string().contains("no broker found"));

                drt.discovery()
                    .register(DiscoverySpec::EventChannel {
                        namespace: "ns-discovery".to_string(),
                        servicegroup: "zmq_broker".to_string(),
                        topic: "broker-channel".to_string(),
                        transport: EventTransport::ZmqBroker {
                            xsub_endpoints: vec![
                                "tcp://192.168.1.10:6001".to_string(),
                                "tcp://192.168.1.11:6001".to_string(),
                            ],
                            xpub_endpoints: vec![
                                "tcp://192.168.1.10:6002".to_string(),
                                "tcp://192.168.1.11:6002".to_string(),
                            ],
                        },
                    })
                    .await
                    .expect("registering broker event channel should succeed");

                let broker = resolve_zmq_broker(&drt, &scope)
                    .await
                    .expect("resolve should succeed")
                    .expect("discovered broker should exist");
                assert_eq!(broker.xsub_endpoints.len(), 2);
                assert_eq!(broker.xpub_endpoints.len(), 2);

                drt.shutdown();
            },
        )
        .await;
    }

    #[tokio::test]
    async fn resolve_zmq_broker_returns_none_when_not_configured() {
        temp_env::async_with_vars(
            vec![
                (env_zmq_broker::PGD_ZMQ_BROKER_URL, None::<&str>),
                (env_zmq_broker::PGD_ZMQ_BROKER_ENABLED, Some("false")),
            ],
            async {
                let drt = create_process_local_drt().await;
                let scope = EventScope::Namespace {
                    name: "ns-none".to_string(),
                };

                let broker = resolve_zmq_broker(&drt, &scope)
                    .await
                    .expect("resolve should succeed");
                assert!(broker.is_none());

                drt.shutdown();
            },
        )
        .await;
    }

    #[tokio::test]
    async fn deduplicating_stream_filters_duplicates_and_forwards_unique_events() {
        let first = encode_envelope("t", 1, 1, b"a");
        let duplicate = encode_envelope("t", 1, 1, b"a2");
        let second = encode_envelope("t", 1, 2, b"b");
        let expected_first = first.clone();
        let expected_second = second.clone();

        let inner: WireStream = Box::pin(async_stream::stream! {
            yield Ok(first.clone());
            yield Ok(duplicate.clone());
            yield Ok(second.clone());
        });

        let mut dedup =
            DeduplicatingStream::new(inner, Arc::new(Codec::Msgpack(MsgpackCodec)), 8);

        let got1 = dedup
            .next()
            .await
            .expect("first item expected")
            .expect("first item should be Ok");
        assert_eq!(got1, expected_first);

        let got2 = dedup
            .next()
            .await
            .expect("second item expected")
            .expect("second item should be Ok");
        assert_eq!(got2, expected_second);

        assert!(dedup.next().await.is_none());
    }

    #[tokio::test]
    async fn deduplicating_stream_propagates_decode_and_inner_errors() {
        let bad = Bytes::from_static(b"not-msgpack");
        let inner_bad: WireStream = Box::pin(async_stream::stream! {
            yield Ok(bad.clone());
        });

        let mut dedup_bad =
            DeduplicatingStream::new(inner_bad, Arc::new(Codec::Msgpack(MsgpackCodec)), 8);
        let decode_err = dedup_bad
            .next()
            .await
            .expect("decode error item expected")
            .err()
            .expect("decode should fail");
        assert!(!decode_err.to_string().is_empty());

        let inner_err: WireStream = Box::pin(async_stream::stream! {
            yield Err(anyhow::anyhow!("inner stream failed"));
        });
        let mut dedup_err =
            DeduplicatingStream::new(inner_err, Arc::new(Codec::Msgpack(MsgpackCodec)), 8);
        let propagated = dedup_err
            .next()
            .await
            .expect("inner error item expected")
            .err()
            .expect("inner error should propagate");
        assert!(propagated.to_string().contains("inner stream failed"));
    }

    #[test]
    fn deduplicating_stream_new_rejects_zero_cache_size() {
        let inner: WireStream = Box::pin(async_stream::stream! {
            yield Ok(Bytes::from_static(b"unused"));
        });

        let panic_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = DeduplicatingStream::new(inner, Arc::new(Codec::Msgpack(MsgpackCodec)), 0);
        }));
        assert!(panic_result.is_err());
    }

    #[tokio::test]
    async fn event_publisher_publish_bytes_and_getters_work() {
        let tx_impl = Arc::new(RecordingTx::default());
        let tx_trait: Arc<dyn EventTransportTx> = tx_impl.clone();
        let codec = Arc::new(Codec::Msgpack(MsgpackCodec));

        let publisher = EventPublisher {
            transport_kind: EventTransportKind::Zmq,
            scope: EventScope::ServiceGroup {
                namespace: "ns".to_string(),
                servicegroup: "comp".to_string(),
            },
            topic: "topic-a".to_string(),
            publisher_id: 99,
            sequence: AtomicU64::new(0),
            tx: tx_trait,
            codec: codec.clone(),
            runtime_handle: tokio::runtime::Handle::current(),
            discovery_client: None,
            discovery_instance: None,
        };

        assert_eq!(publisher.publisher_id(), 99);
        assert_eq!(publisher.topic(), "topic-a");
        assert_eq!(publisher.transport_kind(), EventTransportKind::Zmq);

        publisher
            .publish_bytes(b"raw-payload".to_vec())
            .await
            .expect("publish_bytes should succeed");

        let calls = tx_impl.calls.lock().await;
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "namespace.ns.servicegroup.comp.topic-a");

        let envelope = codec
            .decode_envelope(&calls[0].1)
            .expect("published envelope should decode");
        assert_eq!(envelope.publisher_id, 99);
        assert_eq!(envelope.sequence, 0);
        assert_eq!(envelope.topic, "topic-a");
        assert_eq!(envelope.payload, Bytes::from_static(b"raw-payload"));
    }

    #[tokio::test]
    async fn event_publisher_publish_serializable_event_and_error_path() {
        let good_tx_impl = Arc::new(RecordingTx::default());
        let good_tx_trait: Arc<dyn EventTransportTx> = good_tx_impl.clone();
        let codec = Arc::new(Codec::Msgpack(MsgpackCodec));

        let publisher = EventPublisher {
            transport_kind: EventTransportKind::Zmq,
            scope: EventScope::Namespace {
                name: "ns-pub".to_string(),
            },
            topic: "topic-b".to_string(),
            publisher_id: 123,
            sequence: AtomicU64::new(7),
            tx: good_tx_trait,
            codec: codec.clone(),
            runtime_handle: tokio::runtime::Handle::current(),
            discovery_client: None,
            discovery_instance: None,
        };

        let payload = TestPayload {
            id: 1,
            body: "hello".to_string(),
        };
        publisher
            .publish(&payload)
            .await
            .expect("publish should succeed");

        let calls = good_tx_impl.calls.lock().await;
        assert_eq!(calls.len(), 1);
        let envelope = codec
            .decode_envelope(&calls[0].1)
            .expect("published envelope should decode");
        let typed: TestPayload = codec
            .decode_payload(&envelope.payload)
            .expect("payload should decode as TestPayload");
        assert_eq!(typed, payload);
        drop(calls);

        let failing_tx_trait: Arc<dyn EventTransportTx> = Arc::new(RecordingTx::with_fail(true));
        let failing = EventPublisher {
            transport_kind: EventTransportKind::Zmq,
            scope: EventScope::Namespace {
                name: "ns-pub".to_string(),
            },
            topic: "topic-b".to_string(),
            publisher_id: 123,
            sequence: AtomicU64::new(0),
            tx: failing_tx_trait,
            codec,
            runtime_handle: tokio::runtime::Handle::current(),
            discovery_client: None,
            discovery_instance: None,
        };

        let err = failing
            .publish_bytes(vec![1, 2, 3])
            .await
            .err()
            .expect("forced tx failure should bubble up");
        assert!(err.to_string().contains("forced tx failure"));
    }

    #[tokio::test]
    async fn event_subscriber_next_and_typed_cover_success_and_errors() {
        let codec = Arc::new(Codec::Msgpack(MsgpackCodec));
        let ok_env = EventEnvelope {
            publisher_id: 1,
            sequence: 1,
            published_at: 1700000000000,
            topic: "typed-topic".to_string(),
            payload: codec
                .encode_payload(&TestPayload {
                    id: 42,
                    body: "ok".to_string(),
                })
                .expect("payload encoding should succeed"),
        };

        let bad_payload_env = EventEnvelope {
            publisher_id: 1,
            sequence: 2,
            published_at: 1700000000001,
            topic: "typed-topic".to_string(),
            payload: Bytes::from_static(b"not-msgpack"),
        };
        let ok_env_for_typed_stream = ok_env.clone();

        let wire_ok = codec
            .encode_envelope(&ok_env)
            .expect("envelope encoding should succeed");
        let wire_bad_payload = codec
            .encode_envelope(&bad_payload_env)
            .expect("envelope encoding should succeed");

        let stream: EventStream = Box::pin(async_stream::stream! {
            yield Ok(ok_env.clone());
            yield Err(anyhow::anyhow!("stream-level-error"));
        });

        let mut subscriber = EventSubscriber {
            stream,
            scope: EventScope::Namespace {
                name: "ns-sub".to_string(),
            },
            topic: "typed-topic".to_string(),
            codec: codec.clone(),
        };

        let first = subscriber
            .next()
            .await
            .expect("first event should be present")
            .expect("first event should be Ok");
        assert_eq!(first.topic, "typed-topic");

        let second_err = subscriber
            .next()
            .await
            .expect("second event should be present")
            .err()
            .expect("second should be error");
        assert!(second_err.to_string().contains("stream-level-error"));

        assert!(subscriber.next().await.is_none());

        let typed_stream: EventStream = Box::pin(async_stream::stream! {
            yield Ok(ok_env_for_typed_stream.clone());
            yield Ok(bad_payload_env.clone());
            yield Err(anyhow::anyhow!("typed-stream-error"));
        });
        let typed_subscriber = EventSubscriber {
            stream: typed_stream,
            scope: EventScope::Namespace {
                name: "ns-sub".to_string(),
            },
            topic: "typed-topic".to_string(),
            codec: codec.clone(),
        };
        let mut typed = typed_subscriber.typed::<TestPayload>();

        let (env, value) = typed
            .next()
            .await
            .expect("typed first should exist")
            .expect("typed first should decode");
        assert_eq!(env.sequence, 1);
        assert_eq!(value.id, 42);
        assert_eq!(value.body, "ok");

        let payload_err = typed
            .next()
            .await
            .expect("typed second should exist")
            .err()
            .expect("typed second should fail on payload decode");
        assert!(!payload_err.to_string().is_empty());

        let stream_err = typed
            .next()
            .await
            .expect("typed third should exist")
            .err()
            .expect("typed third should be stream error");
        assert!(stream_err.to_string().contains("typed-stream-error"));
        assert!(typed.next().await.is_none());

        let _ = codec.decode_envelope(&wire_ok).expect("wire_ok should decode");
        let _ = codec
            .decode_envelope(&wire_bad_payload)
            .expect("wire_bad_payload should decode");
    }

    #[tokio::test]
    async fn event_publisher_and_subscriber_with_transport_zmq_construct() {
        temp_env::async_with_vars(
            vec![
                (env_zmq_broker::PGD_ZMQ_BROKER_URL, None::<&str>),
                (env_zmq_broker::PGD_ZMQ_BROKER_ENABLED, Some("false")),
            ],
            async {
                let drt = create_process_local_drt().await;
                let namespace = drt.namespace("ep-ns").expect("namespace should build");
                let servicegroup = namespace
                    .servicegroup("ep-comp")
                    .expect("servicegroup should build");

                let publisher_servicegroup = EventPublisher::for_servicegroup_with_transport(
                    &servicegroup,
                    "topic-c",
                    EventTransportKind::Zmq,
                )
                .await
                .expect("servicegroup publisher should initialize");
                assert_eq!(publisher_servicegroup.topic(), "topic-c");
                assert_eq!(publisher_servicegroup.transport_kind(), EventTransportKind::Zmq);

                let publisher_namespace = EventPublisher::for_namespace_with_transport(
                    &namespace,
                    "topic-d",
                    EventTransportKind::Zmq,
                )
                .await
                .expect("namespace publisher should initialize");
                assert_eq!(publisher_namespace.topic(), "topic-d");
                assert_eq!(publisher_namespace.transport_kind(), EventTransportKind::Zmq);

                let mut subscriber_servicegroup = EventSubscriber::for_servicegroup_with_transport(
                    &servicegroup,
                    "topic-c",
                    EventTransportKind::Zmq,
                )
                .await
                .expect("servicegroup subscriber should initialize");

                let _typed_servicegroup = EventSubscriber::for_servicegroup_with_transport(
                    &servicegroup,
                    "topic-c",
                    EventTransportKind::Zmq,
                )
                .await
                .expect("servicegroup subscriber for typed conversion should initialize")
                .typed::<TestPayload>();

                let _subscriber_namespace = EventSubscriber::for_namespace_with_transport(
                    &namespace,
                    "topic-d",
                    EventTransportKind::Zmq,
                )
                .await
                .expect("namespace subscriber should initialize");

                let _ = tokio::time::timeout(
                    std::time::Duration::from_millis(50),
                    subscriber_servicegroup.next(),
                )
                .await;

                drt.shutdown();
            },
        )
        .await;
    }

    #[tokio::test]
    async fn event_publisher_and_subscriber_auto_transport_paths_construct() {
        temp_env::async_with_vars(
            vec![
                (env_zmq_broker::PGD_ZMQ_BROKER_URL, None::<&str>),
                (env_zmq_broker::PGD_ZMQ_BROKER_ENABLED, Some("false")),
            ],
            async {
                let drt = create_process_local_drt().await;
                let namespace = drt.namespace("ep-auto-ns").expect("namespace should build");
                let servicegroup = namespace
                    .servicegroup("ep-auto-comp")
                    .expect("servicegroup should build");

                let publisher_servicegroup = EventPublisher::for_servicegroup(&servicegroup, "topic-e")
                    .await
                    .expect("auto servicegroup publisher should initialize");
                assert_eq!(publisher_servicegroup.topic(), "topic-e");

                let publisher_namespace = EventPublisher::for_namespace(&namespace, "topic-f")
                    .await
                    .expect("auto namespace publisher should initialize");
                assert_eq!(publisher_namespace.topic(), "topic-f");

                let _subscriber_servicegroup =
                    EventSubscriber::for_servicegroup(&servicegroup, "topic-e")
                        .await
                        .expect("auto servicegroup subscriber should initialize");
                let _subscriber_namespace =
                    EventSubscriber::for_namespace(&namespace, "topic-f")
                        .await
                        .expect("auto namespace subscriber should initialize");

                drt.shutdown();
            },
        )
        .await;
    }

    #[test]
    fn current_timestamp_ms_is_non_decreasing_over_short_interval() {
        let t1 = current_timestamp_ms();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let t2 = current_timestamp_ms();
        assert!(t2 >= t1);
    }
}

// =============================================================================
// === 补充测试（覆盖内部 helpers / mock transport / discovery 集成）==========
// =============================================================================

