// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 服务发现层：统一抽象、查询/注册模型与事件流。
//!
//! 两个后端：
//! - [`KubeDiscoveryClient`]（生产环境，Kubernetes 原生资源）
//! - [`MockDiscovery`]（测试环境，进程内共享注册表）
//!
//! 三类信息的发布与订阅：
//! - **PortName 服务实例**：工作进程对外暴露的可达入口
//! - **ModelCard**：PortName 上当前加载的模型，携带 `topo_json`
//! - **EventChannel**：ServiceGroup 发布事件的 NATS subject 或 ZMQ endpoint

pub mod kube;
pub mod metadata;
pub mod mock;
pub mod utils;

pub use kube::KubeDiscoveryClient;
pub use metadata::{DiscoveryMetadata, MetadataSnapshot};
pub use mock::MockDiscovery;

use std::fmt;
use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::servicegroup;

// ── 环境变量名 ──────────────────────────────────────────────────────────────
const PGD_EVENT_PLANE: &str = "PGD_EVENT_PLANE";
const PGD_EVENT_PLANE_CODEC: &str = "PGD_EVENT_PLANE_CODEC";

// ══════════════════════════════════════════════════════════════════════════════
// EventTransportKind
// ══════════════════════════════════════════════════════════════════════════════

/// 事件平面传输类型标识。
///
/// 仅表示"用哪种传输协议"，不携带连接地址。可 `Copy`、用作 `HashMap` 键。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EventTransportKind {
    #[default]
    Nats,
    Zmq,
}

impl EventTransportKind {
    /// 读取环境变量 `PGD_EVENT_PLANE`。
    ///
    /// `"nats"` / `""` / 未设置 → `Ok(Nats)`；`"zmq"` → `Ok(Zmq)`；其他 → `Err`。
    pub fn from_env() -> anyhow::Result<Self> {
        match std::env::var(PGD_EVENT_PLANE)
            .unwrap_or_default()
            .to_lowercase()
            .as_str()
        {
            "nats" | "" => Ok(Self::Nats),
            "zmq" => Ok(Self::Zmq),
            other => anyhow::bail!(
                "invalid {PGD_EVENT_PLANE}={other:?}, valid values: nats, zmq"
            ),
        }
    }

    /// 读取 `PGD_EVENT_PLANE`，出错时打印 warn 并返回 `Nats`。
    pub fn from_env_or_default() -> Self {
        match Self::from_env() {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("{e}, using default Nats");
                Self::Nats
            }
        }
    }

    /// 为该传输类型返回合理的默认编解码格式。
    pub fn default_codec(self) -> EventCodecKind {
        match self {
            Self::Nats => EventCodecKind::Json,
            Self::Zmq => EventCodecKind::Msgpack,
        }
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// EventCodecKind
// ══════════════════════════════════════════════════════════════════════════════

/// 事件平面序列化格式，与传输协议正交。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventCodecKind {
    Json,
    Msgpack,
}

impl EventCodecKind {
    /// 读取 `PGD_EVENT_PLANE_CODEC`。
    ///
    /// 未设置/空值 → `Ok(None)`；`"json"` → `Ok(Some(Json))`；`"msgpack"` → `Ok(Some(Msgpack))`；
    /// 无效值 → `Err`。
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        match std::env::var(PGD_EVENT_PLANE_CODEC)
            .unwrap_or_default()
            .to_lowercase()
            .as_str()
        {
            "" => Ok(None),
            "json" => Ok(Some(Self::Json)),
            "msgpack" => Ok(Some(Self::Msgpack)),
            other => anyhow::bail!(
                "invalid {PGD_EVENT_PLANE_CODEC}={other:?}, valid values: json, msgpack"
            ),
        }
    }

    /// `from_env()` 的 None 由 transport 默认值补全，出错时用 transport 默认值并 warn。
    pub fn from_env_or_transport_default(transport: EventTransportKind) -> Self {
        match Self::from_env() {
            Ok(Some(v)) => v,
            Ok(None) => transport.default_codec(),
            Err(e) => {
                tracing::warn!("{e}, using transport default");
                transport.default_codec()
            }
        }
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// EventTransport
// ══════════════════════════════════════════════════════════════════════════════

/// 事件平面完整传输配置（含连接地址）。
///
/// 可被序列化写入发现后端，供订阅方反序列化后直接建立连接。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", content = "config")]
pub enum EventTransport {
    Nats {
        /// NATS subject 前缀，如 `"mynamespace.pagoda.myservicegroup.backend"`
        subject_prefix: String,
    },
    Zmq {
        /// ZMQ 直连地址，如 `"tcp://host:5555"`
        endpoint: String,
    },
    ZmqBroker {
        /// 发布方连接的 XSUB 端点（broker 暴露给 publisher）
        xsub_endpoints: Vec<String>,
        /// 订阅方连接的 XPUB 端点（broker 暴露给 subscriber）
        xpub_endpoints: Vec<String>,
    },
}

impl EventTransport {
    /// 提取变体对应的 [`EventTransportKind`]，避免二次 match。
    pub fn kind(&self) -> EventTransportKind {
        match self {
            Self::Nats { .. } => EventTransportKind::Nats,
            Self::Zmq { .. } | Self::ZmqBroker { .. } => EventTransportKind::Zmq,
        }
    }

    /// 便利构造 Nats 变体。
    pub fn nats(subject_prefix: impl Into<String>) -> Self {
        Self::Nats { subject_prefix: subject_prefix.into() }
    }

    /// 便利构造 Zmq 变体。
    pub fn zmq(endpoint: impl Into<String>) -> Self {
        Self::Zmq { endpoint: endpoint.into() }
    }

    /// 返回主要地址字符串。
    ///
    /// `Nats` → subject_prefix；`Zmq` → endpoint；
    /// `ZmqBroker` → 第一个 xsub 端点（无端点时 `""`）。
    pub fn address(&self) -> &str {
        match self {
            Self::Nats { subject_prefix } => subject_prefix,
            Self::Zmq { endpoint } => endpoint,
            Self::ZmqBroker { xsub_endpoints, .. } => {
                xsub_endpoints.first().map(String::as_str).unwrap_or("")
            }
        }
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// EventChannelQuery
// ══════════════════════════════════════════════════════════════════════════════

/// 事件通道可选层级过滤条件。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EventChannelQuery {
    /// `None` = 不限命名空间
    pub namespace: Option<String>,
    /// `None` = 不限服务组（namespace 需有意义）
    pub servicegroup: Option<String>,
    /// `None` = 不限 topic（前两者需有意义）
    pub topic: Option<String>,
}

impl EventChannelQuery {
    /// 无过滤：匹配所有事件通道。
    pub fn all() -> Self {
        Self::default()
    }

    /// 限命名空间。
    pub fn namespace(ns: impl Into<String>) -> Self {
        Self { namespace: Some(ns.into()), ..Default::default() }
    }

    /// 限服务组。
    pub fn servicegroup(ns: impl Into<String>, sg: impl Into<String>) -> Self {
        Self {
            namespace: Some(ns.into()),
            servicegroup: Some(sg.into()),
            ..Default::default()
        }
    }

    /// 精确 topic。
    pub fn topic(
        ns: impl Into<String>,
        sg: impl Into<String>,
        topic: impl Into<String>,
    ) -> Self {
        Self {
            namespace: Some(ns.into()),
            servicegroup: Some(sg.into()),
            topic: Some(topic.into()),
        }
    }

    /// 返回当前有效过滤层数（0 = 全局，1 = 命名空间，2 = 组件，3 = topic）。
    pub fn scope_level(&self) -> u8 {
        match (&self.namespace, &self.servicegroup, &self.topic) {
            (None, _, _) => 0,
            (Some(_), None, _) => 1,
            (Some(_), Some(_), None) => 2,
            (Some(_), Some(_), Some(_)) => 3,
        }
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// DiscoveryQuery
// ══════════════════════════════════════════════════════════════════════════════

/// 层级范围查询键。
#[derive(Debug, Clone)]
pub enum DiscoveryQuery {
    // ── PortName（RPC 服务可达地址）
    AllPortNames,
    NamespacedPortNames { namespace: String },
    ServiceGroupPortNames { namespace: String, servicegroup: String },
    PortName { namespace: String, servicegroup: String, portname: String },

    // ── ModelCard（模型加载状态）
    AllModels,
    NamespacedModels { namespace: String },
    ServiceGroupModels { namespace: String, servicegroup: String },
    PortNameModels { namespace: String, servicegroup: String, portname: String },

    // ── EventChannel（pub/sub 地址）
    EventChannels(EventChannelQuery),
}

// ══════════════════════════════════════════════════════════════════════════════
// DiscoverySpec
// ══════════════════════════════════════════════════════════════════════════════

/// 注册意图描述（`register` 的输入）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DiscoverySpec {
    PortName {
        namespace: String,
        servicegroup: String,
        portname: String,
        transport: servicegroup::TransportType,
    },
    Model {
        namespace: String,
        servicegroup: String,
        portname: String,
        /// 已序列化的 ModelDeploymentCard，解耦 lib/llm
        card_json: serde_json::Value,
        /// `None` = 基础模型；`Some(slug)` = LoRA adapter
        model_suffix: Option<String>,
        /// 模型实例拓扑属性，供 NUMA/rack 感知路由
        topo_json: serde_json::Value,
    },
    EventChannel {
        namespace: String,
        servicegroup: String,
        topic: String,
        transport: EventTransport,
    },
}

impl DiscoverySpec {
    /// 将任意可序列化类型序列化为 `card_json`，构造 `Model` 变体。
    pub fn from_model<T: Serialize>(
        ns: impl Into<String>,
        sg: impl Into<String>,
        pn: impl Into<String>,
        card: &T,
    ) -> anyhow::Result<Self> {
        Self::from_model_with_suffix(ns, sg, pn, card, None)
    }

    /// 同 `from_model`，附加 LoRA model_suffix。
    pub fn from_model_with_suffix<T: Serialize>(
        ns: impl Into<String>,
        sg: impl Into<String>,
        pn: impl Into<String>,
        card: &T,
        suffix: Option<String>,
    ) -> anyhow::Result<Self> {
        Ok(Self::Model {
            namespace: ns.into(),
            servicegroup: sg.into(),
            portname: pn.into(),
            card_json: serde_json::to_value(card)?,
            model_suffix: suffix,
            topo_json: serde_json::Value::Null,
        })
    }

    /// 附加 `instance_id`，将意图转化为已注册实例。
    pub fn with_instance_id(self, instance_id: u64) -> DiscoveryInstance {
        match self {
            Self::PortName { namespace, servicegroup, portname, transport } => {
                DiscoveryInstance::PortName(servicegroup::Instance {
                    namespace,
                    servicegroup,
                    portname,
                    instance_id,
                    transport,
                    topo_json: serde_json::Value::Object(serde_json::Map::new()),
                })
            }
            Self::Model { namespace, servicegroup, portname, card_json, model_suffix, topo_json } => {
                DiscoveryInstance::Model {
                    namespace,
                    servicegroup,
                    portname,
                    instance_id,
                    card_json,
                    model_suffix,
                    topo_json,
                }
            }
            Self::EventChannel { namespace, servicegroup, topic, transport } => {
                DiscoveryInstance::EventChannel {
                    namespace,
                    servicegroup,
                    topic,
                    instance_id,
                    transport,
                }
            }
        }
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// DiscoveryInstance
// ══════════════════════════════════════════════════════════════════════════════

/// 已注册实例（`register` 的输出 / watch 事件携带的数据）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum DiscoveryInstance {
    /// 直接复用 servicegroup::Instance 类型
    PortName(servicegroup::Instance),
    Model {
        namespace: String,
        servicegroup: String,
        portname: String,
        instance_id: u64,
        card_json: serde_json::Value,
        topo_json: serde_json::Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model_suffix: Option<String>,
    },
    EventChannel {
        namespace: String,
        servicegroup: String,
        topic: String,
        instance_id: u64,
        transport: EventTransport,
    },
}

impl DiscoveryInstance {
    /// 从任意变体提取 `instance_id`。
    pub fn instance_id(&self) -> u64 {
        match self {
            Self::PortName(inst) => inst.instance_id,
            Self::Model { instance_id, .. } | Self::EventChannel { instance_id, .. } => {
                *instance_id
            }
        }
    }

    /// 将 `card_json` 反序列化为调用方指定类型（仅对 `Model` 变体有效）。
    pub fn deserialize_model<T: for<'de> Deserialize<'de>>(&self) -> anyhow::Result<T> {
        match self {
            Self::Model { card_json, .. } => {
                serde_json::from_value(card_json.clone()).map_err(Into::into)
            }
            _ => anyhow::bail!(
                "deserialize_model called on non-Model instance"
            ),
        }
    }

    /// 提取所有标识字段，不携带 card_json / transport 等数据部分。
    pub fn id(&self) -> DiscoveryInstanceId {
        match self {
            Self::PortName(inst) => DiscoveryInstanceId::PortName(PortNameInstanceId {
                namespace: inst.namespace.clone(),
                servicegroup: inst.servicegroup.clone(),
                portname: inst.portname.clone(),
                instance_id: inst.instance_id,
            }),
            Self::Model { namespace, servicegroup, portname, instance_id, model_suffix, .. } => {
                DiscoveryInstanceId::Model(ModelCardInstanceId {
                    namespace: namespace.clone(),
                    servicegroup: servicegroup.clone(),
                    portname: portname.clone(),
                    instance_id: *instance_id,
                    model_suffix: model_suffix.clone(),
                })
            }
            Self::EventChannel { namespace, servicegroup, topic, instance_id, .. } => {
                DiscoveryInstanceId::EventChannel(EventChannelInstanceId {
                    namespace: namespace.clone(),
                    servicegroup: servicegroup.clone(),
                    topic: topic.clone(),
                    instance_id: *instance_id,
                })
            }
        }
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// Instance ID types
// ══════════════════════════════════════════════════════════════════════════════

/// PortName 实例唯一标识。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PortNameInstanceId {
    pub namespace: String,
    pub servicegroup: String,
    pub portname: String,
    pub instance_id: u64,
}

impl PortNameInstanceId {
    /// `{namespace}/{servicegroup}/{portname}/{instance_id:x}`（instance_id 十六进制）
    pub fn to_path(&self) -> String {
        format!(
            "{}/{}/{}/{:x}",
            self.namespace, self.servicegroup, self.portname, self.instance_id
        )
    }

    /// 从路径反解析（4 段，instance_id 十六进制）。
    pub fn from_path(path: &str) -> anyhow::Result<Self> {
        let parts: Vec<&str> = path.splitn(5, '/').collect();
        anyhow::ensure!(parts.len() == 4, "PortNameInstanceId path must have 4 segments: {path}");
        Ok(Self {
            namespace: parts[0].to_string(),
            servicegroup: parts[1].to_string(),
            portname: parts[2].to_string(),
            instance_id: u64::from_str_radix(parts[3], 16)
                .map_err(|e| anyhow::anyhow!("invalid instance_id hex in {path}: {e}"))?,
        })
    }
}

impl fmt::Display for PortNameInstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_path())
    }
}

/// ModelCard 实例唯一标识。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ModelCardInstanceId {
    pub namespace: String,
    pub servicegroup: String,
    pub portname: String,
    pub instance_id: u64,
    /// `None` = 基础模型；`Some(slug)` = LoRA adapter
    pub model_suffix: Option<String>,
}

impl ModelCardInstanceId {
    /// 无 suffix：`{ns}/{sg}/{pn}/{id:x}`；有 suffix：`{ns}/{sg}/{pn}/{id:x}/{suffix}`
    pub fn to_path(&self) -> String {
        match &self.model_suffix {
            Some(suffix) => format!(
                "{}/{}/{}/{:x}/{}",
                self.namespace, self.servicegroup, self.portname, self.instance_id, suffix
            ),
            None => format!(
                "{}/{}/{}/{:x}",
                self.namespace, self.servicegroup, self.portname, self.instance_id
            ),
        }
    }

    /// 接受 4 段（基础模型）或 5 段（LoRA）路径。
    pub fn from_path(path: &str) -> anyhow::Result<Self> {
        let parts: Vec<&str> = path.splitn(6, '/').collect();
        anyhow::ensure!(
            parts.len() == 4 || parts.len() == 5,
            "ModelCardInstanceId path must have 4-5 segments: {path}"
        );
        let instance_id = u64::from_str_radix(parts[3], 16)
            .map_err(|e| anyhow::anyhow!("invalid instance_id hex in {path}: {e}"))?;
        Ok(Self {
            namespace: parts[0].to_string(),
            servicegroup: parts[1].to_string(),
            portname: parts[2].to_string(),
            instance_id,
            model_suffix: parts.get(4).map(|s| s.to_string()),
        })
    }
}

impl fmt::Display for ModelCardInstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_path())
    }
}

/// EventChannel 实例唯一标识。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventChannelInstanceId {
    pub namespace: String,
    pub servicegroup: String,
    pub topic: String,
    pub instance_id: u64,
}

impl EventChannelInstanceId {
    pub fn to_path(&self) -> String {
        format!(
            "{}/{}/{}/{:x}",
            self.namespace, self.servicegroup, self.topic, self.instance_id
        )
    }

    pub fn from_path(path: &str) -> anyhow::Result<Self> {
        let parts: Vec<&str> = path.splitn(5, '/').collect();
        anyhow::ensure!(parts.len() == 4, "EventChannelInstanceId path must have 4 segments: {path}");
        Ok(Self {
            namespace: parts[0].to_string(),
            servicegroup: parts[1].to_string(),
            topic: parts[2].to_string(),
            instance_id: u64::from_str_radix(parts[3], 16)
                .map_err(|e| anyhow::anyhow!("invalid instance_id hex in {path}: {e}"))?,
        })
    }
}

impl fmt::Display for EventChannelInstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_path())
    }
}

/// 三类实例标识的联合枚举。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DiscoveryInstanceId {
    PortName(PortNameInstanceId),
    Model(ModelCardInstanceId),
    EventChannel(EventChannelInstanceId),
}

impl DiscoveryInstanceId {
    /// 跨变体提取数值 ID。
    pub fn instance_id(&self) -> u64 {
        match self {
            Self::PortName(id) => id.instance_id,
            Self::Model(id) => id.instance_id,
            Self::EventChannel(id) => id.instance_id,
        }
    }

    pub fn extract_portname_id(&self) -> anyhow::Result<&PortNameInstanceId> {
        match self {
            Self::PortName(id) => Ok(id),
            other => anyhow::bail!("expected PortName ID, got {other:?}"),
        }
    }

    pub fn extract_model_id(&self) -> anyhow::Result<&ModelCardInstanceId> {
        match self {
            Self::Model(id) => Ok(id),
            other => anyhow::bail!("expected Model ID, got {other:?}"),
        }
    }

    pub fn extract_event_channel_id(&self) -> anyhow::Result<&EventChannelInstanceId> {
        match self {
            Self::EventChannel(id) => Ok(id),
            other => anyhow::bail!("expected EventChannel ID, got {other:?}"),
        }
    }
}

impl fmt::Display for DiscoveryInstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PortName(id) => write!(f, "portname:{id}"),
            Self::Model(id) => write!(f, "model:{id}"),
            Self::EventChannel(id) => write!(f, "event:{id}"),
        }
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// DiscoveryEvent / DiscoveryStream
// ══════════════════════════════════════════════════════════════════════════════

/// 发现层变更事件（仅 Added / Removed，更新用"删除旧+添加新"表达）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiscoveryEvent {
    /// 新实例出现；携带完整数据供消费方立即使用。
    Added(DiscoveryInstance),
    /// 实例消失；只携带 ID（数据已不存在）。
    Removed(DiscoveryInstanceId),
}

/// 类型擦除的发现事件流。每个 item 是 `Result<DiscoveryEvent>`，允许流中间产生网络错误。
pub type DiscoveryStream =
    Pin<Box<dyn Stream<Item = anyhow::Result<DiscoveryEvent>> + Send>>;

// ══════════════════════════════════════════════════════════════════════════════
// Discovery trait
// ══════════════════════════════════════════════════════════════════════════════

/// 发现后端统一抽象契约。
///
/// `register()` 含模型名冲突检测（默认实现），`register_internal()` 是后端必须实现的原子写入钩子。
#[async_trait]
pub trait Discovery: Send + Sync {
    /// 当前后端分配给本进程的唯一 ID（pod name 哈希 / 测试计数器）。
    fn instance_id(&self) -> u64;

    /// 注册实例到发现层（含模型名冲突检测）。
    ///
    /// 非 Model 类型直接透传 `register_internal()`。Model 类型先执行冲突检测，
    /// 写入后再做一次竞态检测，发现冲突时回滚。
    async fn register(&self, spec: DiscoverySpec) -> anyhow::Result<DiscoveryInstance> {
        if let DiscoverySpec::Model {
            ref namespace,
            ref servicegroup,
            ref portname,
            ref card_json,
            ref model_suffix,
            ..
        } = spec
        {
            let requested =
                extract_model_registration_identity(card_json, model_suffix.as_deref())?;
            let existing = self
                .list(DiscoveryQuery::PortNameModels {
                    namespace: namespace.clone(),
                    servicegroup: servicegroup.clone(),
                    portname: portname.clone(),
                })
                .await?;
            if let Some(conflict) = find_conflicting_model_name(&existing, &requested)? {
                anyhow::bail!(
                    "model name conflict on {namespace}/{servicegroup}/{portname}: \
                     cannot register alongside existing model card '{conflict}'"
                );
            }
        }

        let instance = self.register_internal(spec).await?;

        // 竞态检测：写入后再次检查
        if let DiscoveryInstance::Model {
            ref namespace,
            ref servicegroup,
            ref portname,
            ref card_json,
            ref model_suffix,
            ..
        } = instance
        {
            let requested =
                extract_model_registration_identity(card_json, model_suffix.as_deref())?;
            let post_list = self
                .list(DiscoveryQuery::PortNameModels {
                    namespace: namespace.clone(),
                    servicegroup: servicegroup.clone(),
                    portname: portname.clone(),
                })
                .await?;
            // 排除自身后检查
            let others: Vec<_> = post_list
                .iter()
                .filter(|i| i.instance_id() != instance.instance_id())
                .cloned()
                .collect();
            if let Some(conflict) = find_conflicting_model_name(&others, &requested)? {
                let ns_str = namespace.clone();
                let sg_str = servicegroup.clone();
                let pn_str = portname.clone();
                let _ = self.unregister(instance).await;
                anyhow::bail!(
                    "model name race conflict on {ns_str}/{sg_str}/{pn_str}: \
                     rolled back registration, conflicting card '{conflict}'"
                );
            }
        }

        Ok(instance)
    }

    /// 后端必须实现：原子写入存储，不含冲突检测。
    async fn register_internal(&self, spec: DiscoverySpec) -> anyhow::Result<DiscoveryInstance>;

    /// 从存储中删除实例。
    async fn unregister(&self, instance: DiscoveryInstance) -> anyhow::Result<()>;

    /// 一次性快照查询，返回所有当前匹配的实例。
    async fn list(&self, query: DiscoveryQuery) -> anyhow::Result<Vec<DiscoveryInstance>>;

    /// 流式订阅：先发出当前所有实例的 `Added` 事件，再持续推送增量变化。
    async fn list_and_watch(
        &self,
        query: DiscoveryQuery,
        cancel_token: Option<CancellationToken>,
    ) -> anyhow::Result<DiscoveryStream>;

    /// 可选：k8s 后端在关闭时主动撤销自身注册，测试替身可忽略。
    fn shutdown(&self) {}
}

// ══════════════════════════════════════════════════════════════════════════════
// Model name conflict detection（私有，仅供 register() 默认实现）
// ══════════════════════════════════════════════════════════════════════════════

#[derive(Clone, Debug, PartialEq, Eq)]
struct ModelRegistrationIdentity {
    display_name: String,
    source_path: Option<String>,
    is_lora: bool,
}

impl ModelRegistrationIdentity {
    /// 返回兼容键：有 source_path 时用它，否则退回 display_name。
    fn base_identity(&self) -> &str {
        self.source_path.as_deref().unwrap_or(&self.display_name)
    }

    /// 判断两个注册是否兼容（可共存于同一 PortName）。
    fn is_compatible_with(&self, other: &Self) -> bool {
        if self.is_lora || other.is_lora {
            // LoRA 场景：以 base_identity（source_path）为兼容键
            self.base_identity() == other.base_identity()
        } else {
            // 基础模型场景：同名允许多实例（横向扩容），不同名禁止共存
            self.display_name == other.display_name
        }
    }
}

/// 从 `card_json` 和 `model_suffix` 提取 [`ModelRegistrationIdentity`]。
fn extract_model_registration_identity(
    card_json: &serde_json::Value,
    model_suffix: Option<&str>,
) -> anyhow::Result<ModelRegistrationIdentity> {
    let display_name = card_json["display_name"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("card_json missing required field 'display_name'"))?
        .to_string();

    let source_path = card_json["source_path"].as_str().map(ToString::to_string);

    let is_lora = model_suffix.map_or(false, |s| !s.is_empty())
        || !card_json["lora"].is_null();

    Ok(ModelRegistrationIdentity { display_name, source_path, is_lora })
}

/// 遍历 `instances`，找出第一个与 `requested` 不兼容的实例的 `display_name`。
///
/// 返回 `Ok(None)` 表示全部兼容；`Ok(Some(name))` 表示存在冲突。
fn find_conflicting_model_name(
    instances: &[DiscoveryInstance],
    requested: &ModelRegistrationIdentity,
) -> anyhow::Result<Option<String>> {
    for inst in instances {
        if let DiscoveryInstance::Model { card_json, model_suffix, .. } = inst {
            let existing =
                extract_model_registration_identity(card_json, model_suffix.as_deref())?;
            if !requested.is_compatible_with(&existing) {
                return Ok(Some(existing.display_name));
            }
        }
    }
    Ok(None)
}

// ══════════════════════════════════════════════════════════════════════════════
// Tests
// ══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn portname_instance_id_path_roundtrip() {
        let id = PortNameInstanceId {
            namespace: "ns1".into(),
            servicegroup: "sg1".into(),
            portname: "pn1".into(),
            instance_id: 0xdeadbeef,
        };
        let path = id.to_path();
        assert_eq!(path, "ns1/sg1/pn1/deadbeef");
        let parsed = PortNameInstanceId::from_path(&path).unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn model_card_instance_id_path_lora() {
        let id = ModelCardInstanceId {
            namespace: "ns".into(),
            servicegroup: "sg".into(),
            portname: "pn".into(),
            instance_id: 0x1a2b,
            model_suffix: Some("lora-v1".into()),
        };
        let path = id.to_path();
        assert_eq!(path, "ns/sg/pn/1a2b/lora-v1");
        let parsed = ModelCardInstanceId::from_path(&path).unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn event_transport_kind_default_is_nats() {
        let kind = EventTransportKind::default();
        assert_eq!(kind, EventTransportKind::Nats);
        assert_eq!(kind.default_codec(), EventCodecKind::Json);
    }

    #[test]
    fn event_transport_address() {
        assert_eq!(
            EventTransport::nats("ns.pagoda.sg.backend").address(),
            "ns.pagoda.sg.backend"
        );
        assert_eq!(
            EventTransport::ZmqBroker {
                xsub_endpoints: vec!["tcp://host:5555".into()],
                xpub_endpoints: vec!["tcp://host:5556".into()],
            }
            .address(),
            "tcp://host:5555"
        );
    }

    #[test]
    fn model_conflict_detection_base_model_same_name_compatible() {
        let a = ModelRegistrationIdentity {
            display_name: "Llama-3".into(),
            source_path: None,
            is_lora: false,
        };
        let b = ModelRegistrationIdentity {
            display_name: "Llama-3".into(),
            source_path: None,
            is_lora: false,
        };
        assert!(a.is_compatible_with(&b));
    }

    #[test]
    fn model_conflict_detection_base_model_different_name_conflict() {
        let a = ModelRegistrationIdentity {
            display_name: "Llama-3".into(),
            source_path: None,
            is_lora: false,
        };
        let b = ModelRegistrationIdentity {
            display_name: "Mistral-7B".into(),
            source_path: None,
            is_lora: false,
        };
        assert!(!a.is_compatible_with(&b));
    }

    #[test]
    fn model_conflict_detection_lora_same_base_compatible() {
        let base = ModelRegistrationIdentity {
            display_name: "lora-v1".into(),
            source_path: Some("/models/llama-3".into()),
            is_lora: true,
        };
        let other_lora = ModelRegistrationIdentity {
            display_name: "lora-v2".into(),
            source_path: Some("/models/llama-3".into()),
            is_lora: true,
        };
        assert!(base.is_compatible_with(&other_lora));
    }

    #[test]
    fn model_conflict_detection_lora_different_base_conflict() {
        let a = ModelRegistrationIdentity {
            display_name: "lora-v1".into(),
            source_path: Some("/models/llama-3".into()),
            is_lora: true,
        };
        let b = ModelRegistrationIdentity {
            display_name: "lora-v1".into(),
            source_path: Some("/models/mistral-7b".into()),
            is_lora: true,
        };
        assert!(!a.is_compatible_with(&b));
    }

    #[test]
    fn discovery_instance_id_extraction() {
        let inst = DiscoveryInstance::EventChannel {
            namespace: "ns".into(),
            servicegroup: "sg".into(),
            topic: "kv-events".into(),
            instance_id: 42,
            transport: EventTransport::nats("ns.pagoda.sg.kv-events"),
        };
        assert_eq!(inst.instance_id(), 42);
        let id = inst.id();
        assert_eq!(id.instance_id(), 42);
        assert!(id.extract_event_channel_id().is_ok());
        assert!(id.extract_portname_id().is_err());
    }
}
