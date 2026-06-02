// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # Discovery 平面 —— 公共契约与 trait
//!
//! ## 设计意图
//!
//! 本模块定义跨后端（mock / KV store / Kubernetes 原生）统一的 Discovery 抽象：
//!
//! - **DiscoverySpec**：注册请求（无 instance_id）；
//! - **DiscoveryInstance**：已分配 instance_id 的注册结果；
//! - **DiscoveryQuery**：分层查询条件；
//! - **DiscoveryEvent / DiscoveryStream**：watch 流的事件类型；
//! - **Discovery trait**：所有后端的最小可用契约 + 默认的 model 名称冲突防护。
//!
//! ## 外部契约
//!
//! - 所有公开类型的 `derive` / `#[serde(...)]` 必须保持向后兼容
//!   （否则跨后端反序列化会破坏已有数据）；
//! - `PortNameInstanceId / ModelCardInstanceId / EventChannelInstanceId` 的
//!   `to_path` / `from_path` 是 KV / annotation key 的契约，不得改格式；
//! - `Discovery::register` 的默认实现负责跨 backend **model 名称冲突检测**：
//!   先 list → 冲突拒绝；写入后再 list → 仍冲突时回滚（unregister 失败也要冒泡）。
//!
//! ## 实现要点
//!
//! - 模型冲突判定下沉到 [`ModelRegistrationIdentity`]：
//!   - 普通模型：display_name 相等才算同一个；
//!   - LoRA 与基模：`source_path`（fall back to display_name）一致即兼容；
//! - 把“before / after 两次 list”抽出 [`detect_model_conflict`] 共用，
//!   register 主流程因此更线性、可读性更好。

use anyhow::{Context, Result};
use async_trait::async_trait;
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use tokio_util::sync::CancellationToken;

mod metadata;
pub use metadata::{DiscoveryMetadata, MetadataSnapshot};

mod mock;
pub use mock::{MockDiscovery, SharedMockRegistry};

mod kv_store;
pub use kv_store::KVStoreDiscovery;

mod kube;
pub use kube::{KubeDiscoveryClient, hash_pod_name};

pub mod utils;
pub use utils::watch_and_extract_field;

use crate::servicegroup::{DeviceType, TransportType};

// === 事件平面 transport 类型 =================================================

/// 事件平面 transport 的“种类”（不带连接信息），用于配置与环境变量选型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EventTransportKind {
    /// NATS Core pub/sub
    #[default]
    Nats,
    /// ZMQ pub/sub
    Zmq,
}

impl EventTransportKind {
    /// 从环境变量 `PGD_EVENT_PLANE` 解析。
    ///
    /// 未设置 / 空 → `Nats`（分布式部署的合理默认）；
    /// 本地 file/mem 后端请优先使用 `DistributedRuntime::default_event_transport_kind`。
    pub fn from_env() -> Result<Self> {
        match std::env::var(crate::config::environment_names::event_plane::PGD_EVENT_PLANE)
            .as_deref()
        {
            Ok("nats") | Ok("") | Err(_) => Ok(Self::Nats),
            Ok("zmq") => Ok(Self::Zmq),
            Ok(other) => anyhow::bail!(
                "Invalid PGD_EVENT_PLANE value '{}'. Valid values: 'nats', 'zmq'",
                other
            ),
        }
    }

    /// 解析环境变量，无效值时打印 warn 并回退到 NATS。
    pub fn from_env_or_default() -> Self {
        Self::from_env().unwrap_or_else(|e| {
            tracing::warn!("{e}, defaulting to NATS");
            Self::Nats
        })
    }

    /// 获取此 transport 的默认 codec：NATS→JSON，ZMQ→MsgPack。
    pub fn default_codec(&self) -> EventCodecKind {
        match self {
            Self::Nats => EventCodecKind::Json,
            Self::Zmq => EventCodecKind::Msgpack,
        }
    }
}

/// 事件平面 envelope / payload 的序列化格式。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventCodecKind {
    /// 人类可读的 JSON，便于调试
    Json,
    /// 紧凑二进制 MessagePack
    Msgpack,
}

impl EventCodecKind {
    /// 从环境变量 `PGD_EVENT_PLANE_CODEC` 解析；未设置返回 None 让 transport 决定。
    pub fn from_env() -> Result<Option<Self>> {
        match std::env::var(crate::config::environment_names::event_plane::PGD_EVENT_PLANE_CODEC)
            .as_deref()
        {
            Err(_) | Ok("") => Ok(None),
            Ok("json") => Ok(Some(Self::Json)),
            Ok("msgpack") => Ok(Some(Self::Msgpack)),
            Ok(other) => anyhow::bail!(
                "Invalid PGD_EVENT_PLANE_CODEC value '{}'. Valid values: 'json', 'msgpack'",
                other
            ),
        }
    }

    /// 解析环境变量，并按 transport 的默认值兜底。
    pub fn from_env_or_transport_default(transport: EventTransportKind) -> Self {
        Self::from_env()
            .unwrap_or_else(|e| {
                tracing::warn!(
                    "{}, defaulting to {:?} for {:?}",
                    e,
                    transport.default_codec(),
                    transport
                );
                None
            })
            .unwrap_or_else(|| transport.default_codec())
    }
}

/// 事件平面 channel 的完整 transport 配置（kind + 连接信息）。
///
/// 与请求平面的 `TransportType` 刻意分离，避免语义混淆。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", content = "config")]
pub enum EventTransport {
    /// NATS Core：subject 前缀
    Nats {
        /// 例如 "namespace.pagoda.servicegroup.backend"
        subject_prefix: String,
    },
    /// ZMQ pub/sub 直连
    Zmq {
        /// 例如 "tcp://host:port"
        portname: String,
    },
    /// ZMQ broker 模式（用于 broker 发现）
    ZmqBroker {
        /// 发布者连接的 XSUB 端点列表
        xsub_endpoints: Vec<String>,
        /// 订阅者连接的 XPUB 端点列表
        xpub_endpoints: Vec<String>,
    },
}

impl EventTransport {
    /// 获取此 transport 的 kind。
    pub fn kind(&self) -> EventTransportKind {
        match self {
            Self::Nats { .. } => EventTransportKind::Nats,
            Self::Zmq { .. } | Self::ZmqBroker { .. } => EventTransportKind::Zmq,
        }
    }

    /// 便捷构造 NATS transport。
    pub fn nats(subject_prefix: impl Into<String>) -> Self {
        Self::Nats { subject_prefix: subject_prefix.into() }
    }

    /// 便捷构造 ZMQ direct transport。
    pub fn zmq(portname: impl Into<String>) -> Self {
        Self::Zmq { portname: portname.into() }
    }

    /// NATS 返回 subject 前缀；ZMQ 返回 portname；ZmqBroker 返回首个 XSUB。
    pub fn address(&self) -> &str {
        match self {
            Self::Nats { subject_prefix } => subject_prefix,
            Self::Zmq { portname } => portname,
            Self::ZmqBroker { xsub_endpoints, .. } => {
                xsub_endpoints.first().map(|s| s.as_str()).unwrap_or("")
            }
        }
    }
}

// === 查询条件 ================================================================

/// 前缀分层的发现查询。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DiscoveryQuery {
    AllPortNames,
    NamespacedPortNames {
        namespace: String,
    },
    ServiceGroupPortNames {
        namespace: String,
        servicegroup: String,
    },
    PortName {
        namespace: String,
        servicegroup: String,
        portname: String,
    },
    AllModels,
    NamespacedModels {
        namespace: String,
    },
    ServiceGroupModels {
        namespace: String,
        servicegroup: String,
    },
    PortNameModels {
        namespace: String,
        servicegroup: String,
        portname: String,
    },
    /// EventChannel 的统一查询（三个可选作用域字段）
    EventChannels(EventChannelQuery),
}

/// EventChannel 的统一查询条件：三个字段均可选，自上而下逐级收紧。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EventChannelQuery {
    pub namespace: Option<String>,
    pub servicegroup: Option<String>,
    pub topic: Option<String>,
}

impl EventChannelQuery {
    pub fn all() -> Self {
        Self { namespace: None, servicegroup: None, topic: None }
    }
    pub fn namespace(namespace: impl Into<String>) -> Self {
        Self { namespace: Some(namespace.into()), servicegroup: None, topic: None }
    }
    pub fn servicegroup(namespace: impl Into<String>, servicegroup: impl Into<String>) -> Self {
        Self {
            namespace: Some(namespace.into()),
            servicegroup: Some(servicegroup.into()),
            topic: None,
        }
    }
    pub fn topic(
        namespace: impl Into<String>,
        servicegroup: impl Into<String>,
        topic: impl Into<String>,
    ) -> Self {
        Self {
            namespace: Some(namespace.into()),
            servicegroup: Some(servicegroup.into()),
            topic: Some(topic.into()),
        }
    }

    /// 返回当前作用域层级（0=all, 1=namespace, 2=servicegroup, 3=topic）。
    pub fn scope_level(&self) -> u8 {
        match (self.namespace.as_ref(), self.servicegroup.as_ref(), self.topic.as_ref()) {
            (_, _, Some(_)) => 3,
            (_, Some(_), _) => 2,
            (Some(_), _, _) => 1,
            _ => 0,
        }
    }
}

// === Spec & Instance =========================================================

/// 注册请求：尚未分配 instance_id。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiscoverySpec {
    PortName {
        namespace: String,
        servicegroup: String,
        portname: String,
        transport: TransportType,
        /// 异构路由用以区分 CPU / CUDA worker
        device_type: Option<DeviceType>,
    },
    Model {
        namespace: String,
        servicegroup: String,
        portname: String,
        /// ModelDeploymentCard 序列化为 JSON（避免 runtime 直接依赖 llm 类型）
        card_json: serde_json::Value,
        /// LoRA 等场景下追加在 instance_id 之后的可选 path 后缀
        model_suffix: Option<String>,
    },
    EventChannel {
        namespace: String,
        servicegroup: String,
        /// 频道 topic（如 "kv-events"）
        topic: String,
        /// 事件 transport（NATS subject 前缀 / ZMQ portname）
        transport: EventTransport,
    },
}

impl DiscoverySpec {
    /// 由可序列化的 model 类型构造 Model spec。
    pub fn from_model<T>(
        namespace: String,
        servicegroup: String,
        portname: String,
        card: &T,
    ) -> Result<Self>
    where
        T: Serialize,
    {
        Self::from_model_with_suffix(namespace, servicegroup, portname, card, None)
    }

    /// 同上但额外指定 model_suffix（LoRA 等场景）。
    pub fn from_model_with_suffix<T>(
        namespace: String,
        servicegroup: String,
        portname: String,
        card: &T,
        model_suffix: Option<String>,
    ) -> Result<Self>
    where
        T: Serialize,
    {
        Ok(Self::Model {
            namespace,
            servicegroup,
            portname,
            card_json: serde_json::to_value(card)?,
            model_suffix,
        })
    }

    /// 绑定 instance_id 转为 [`DiscoveryInstance`]。
    pub fn with_instance_id(self, instance_id: u64) -> DiscoveryInstance {
        match self {
            Self::PortName { namespace, servicegroup, portname, transport, device_type } => {
                DiscoveryInstance::PortName(crate::servicegroup::Instance {
                    namespace,
                    servicegroup,
                    portname,
                    instance_id,
                    transport,
                    device_type,
                })
            }
            Self::Model { namespace, servicegroup, portname, card_json, model_suffix } => {
                DiscoveryInstance::Model {
                    namespace,
                    servicegroup,
                    portname,
                    instance_id,
                    card_json,
                    model_suffix,
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

/// 已注册的发现对象（带 instance_id）。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
pub enum DiscoveryInstance {
    PortName(crate::servicegroup::Instance),
    Model {
        namespace: String,
        servicegroup: String,
        portname: String,
        instance_id: u64,
        card_json: serde_json::Value,
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
    /// 实例 ID（任意变体）。
    pub fn instance_id(&self) -> u64 {
        match self {
            Self::PortName(i) => i.instance_id,
            Self::Model { instance_id, .. } | Self::EventChannel { instance_id, .. } => {
                *instance_id
            }
        }
    }

    /// 把 `card_json` 反序列化为目标类型；仅 Model 变体支持。
    pub fn deserialize_model<T>(&self) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        match self {
            Self::Model { card_json, .. } => Ok(serde_json::from_value(card_json.clone())?),
            Self::PortName(_) => {
                anyhow::bail!("Cannot deserialize model from PortName instance")
            }
            Self::EventChannel { .. } => {
                anyhow::bail!("Cannot deserialize model from EventChannel instance")
            }
        }
    }

    /// 提取唯一标识，用于 diff / removal。
    pub fn id(&self) -> DiscoveryInstanceId {
        match self {
            Self::PortName(i) => DiscoveryInstanceId::PortName(PortNameInstanceId {
                namespace: i.namespace.clone(),
                servicegroup: i.servicegroup.clone(),
                portname: i.portname.clone(),
                instance_id: i.instance_id,
            }),
            Self::Model {
                namespace,
                servicegroup,
                portname,
                instance_id,
                model_suffix,
                ..
            } => DiscoveryInstanceId::Model(ModelCardInstanceId {
                namespace: namespace.clone(),
                servicegroup: servicegroup.clone(),
                portname: portname.clone(),
                instance_id: *instance_id,
                model_suffix: model_suffix.clone(),
            }),
            Self::EventChannel {
                namespace,
                servicegroup,
                topic,
                instance_id,
                ..
            } => DiscoveryInstanceId::EventChannel(EventChannelInstanceId {
                namespace: namespace.clone(),
                servicegroup: servicegroup.clone(),
                topic: topic.clone(),
                instance_id: *instance_id,
            }),
        }
    }
}

// === InstanceId 三件套 =======================================================

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PortNameInstanceId {
    pub namespace: String,
    pub servicegroup: String,
    pub portname: String,
    pub instance_id: u64,
}

impl PortNameInstanceId {
    /// `{namespace}/{servicegroup}/{portname}/{instance_id:x}`
    pub fn to_path(&self) -> String {
        format!(
            "{}/{}/{}/{:x}",
            self.namespace, self.servicegroup, self.portname, self.instance_id
        )
    }

    /// 与 [`to_path`] 互逆。
    pub fn from_path(path: &str) -> Result<Self> {
        let parts: Vec<&str> = path.split('/').collect();
        anyhow::ensure!(
            parts.len() == 4,
            "Invalid PortNameInstanceId path: expected 4 parts, got {}",
            parts.len()
        );
        Ok(Self {
            namespace: parts[0].to_string(),
            servicegroup: parts[1].to_string(),
            portname: parts[2].to_string(),
            instance_id: u64::from_str_radix(parts[3], 16)
                .map_err(|e| anyhow::anyhow!("Invalid instance_id hex: {}", e))?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ModelCardInstanceId {
    pub namespace: String,
    pub servicegroup: String,
    pub portname: String,
    pub instance_id: u64,
    /// None=基础模型；Some(slug)=LoRA 适配器
    pub model_suffix: Option<String>,
}

impl ModelCardInstanceId {
    /// `{namespace}/{servicegroup}/{portname}/{instance_id:x}[/{model_suffix}]`
    pub fn to_path(&self) -> String {
        match &self.model_suffix {
            Some(s) => format!(
                "{}/{}/{}/{:x}/{}",
                self.namespace, self.servicegroup, self.portname, self.instance_id, s
            ),
            None => format!(
                "{}/{}/{}/{:x}",
                self.namespace, self.servicegroup, self.portname, self.instance_id
            ),
        }
    }

    pub fn from_path(path: &str) -> Result<Self> {
        let parts: Vec<&str> = path.split('/').collect();
        anyhow::ensure!(
            (4..=5).contains(&parts.len()),
            "Invalid ModelCardInstanceId path: expected 4 or 5 parts, got {}",
            parts.len()
        );
        Ok(Self {
            namespace: parts[0].to_string(),
            servicegroup: parts[1].to_string(),
            portname: parts[2].to_string(),
            instance_id: u64::from_str_radix(parts[3], 16)
                .map_err(|e| anyhow::anyhow!("Invalid instance_id hex: {}", e))?,
            model_suffix: parts.get(4).map(|s| s.to_string()),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventChannelInstanceId {
    pub namespace: String,
    pub servicegroup: String,
    pub topic: String,
    pub instance_id: u64,
}

impl EventChannelInstanceId {
    /// `{namespace}/{servicegroup}/{topic}/{instance_id:x}`
    pub fn to_path(&self) -> String {
        format!(
            "{}/{}/{}/{:x}",
            self.namespace, self.servicegroup, self.topic, self.instance_id
        )
    }

    pub fn from_path(path: &str) -> Result<Self> {
        let parts: Vec<&str> = path.split('/').collect();
        anyhow::ensure!(
            parts.len() == 4,
            "Invalid EventChannelInstanceId path: expected 4 parts, got {}",
            parts.len()
        );
        Ok(Self {
            namespace: parts[0].to_string(),
            servicegroup: parts[1].to_string(),
            topic: parts[2].to_string(),
            instance_id: u64::from_str_radix(parts[3], 16)
                .map_err(|e| anyhow::anyhow!("Invalid instance_id hex: {}", e))?,
        })
    }
}

/// 三类 InstanceId 的并集类型。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DiscoveryInstanceId {
    PortName(PortNameInstanceId),
    Model(ModelCardInstanceId),
    EventChannel(EventChannelInstanceId),
}

impl DiscoveryInstanceId {
    /// 透明获取底层 instance_id。
    pub fn instance_id(&self) -> u64 {
        match self {
            Self::PortName(e) => e.instance_id,
            Self::Model(m) => m.instance_id,
            Self::EventChannel(c) => c.instance_id,
        }
    }

    pub fn extract_portname_id(&self) -> Result<&PortNameInstanceId> {
        match self {
            Self::PortName(e) => Ok(e),
            Self::Model(_) => anyhow::bail!("Expected PortName variant, got Model"),
            Self::EventChannel(_) => {
                anyhow::bail!("Expected PortName variant, got EventChannel")
            }
        }
    }

    pub fn extract_model_id(&self) -> Result<&ModelCardInstanceId> {
        match self {
            Self::Model(m) => Ok(m),
            Self::PortName(_) => anyhow::bail!("Expected Model variant, got PortName"),
            Self::EventChannel(_) => anyhow::bail!("Expected Model variant, got EventChannel"),
        }
    }

    pub fn extract_event_channel_id(&self) -> Result<&EventChannelInstanceId> {
        match self {
            Self::EventChannel(c) => Ok(c),
            Self::PortName(_) => anyhow::bail!("Expected EventChannel variant, got PortName"),
            Self::Model(_) => anyhow::bail!("Expected EventChannel variant, got Model"),
        }
    }
}

// === 事件流 ==================================================================

/// watch 流上的事件。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiscoveryEvent {
    Added(DiscoveryInstance),
    Removed(DiscoveryInstanceId),
}

/// 发现事件流类型别名。
pub type DiscoveryStream = Pin<Box<dyn Stream<Item = Result<DiscoveryEvent>> + Send>>;

// === 私有：Model 名称冲突判定 =================================================

/// 模型注册身份：判定“同一 portname 上是否已注册了不兼容的模型”所需的最小信息。
#[derive(Clone, Debug, PartialEq, Eq)]
struct ModelRegistrationIdentity {
    display_name: String,
    source_path: Option<String>,
    is_lora: bool,
}

impl ModelRegistrationIdentity {
    /// 用于 LoRA 比较的基础身份：有 source_path 优先取 source_path，否则 display_name。
    fn base_identity(&self) -> &str {
        self.source_path.as_deref().unwrap_or(&self.display_name)
    }

    /// 兼容性判定：
    /// - 任一方为 LoRA → 比较 base identity（允许 LoRA / 基模并存）；
    /// - 双方均为普通模型 → display_name 必须相同。
    fn is_compatible_with(&self, other: &Self) -> bool {
        if self.is_lora || other.is_lora {
            self.base_identity() == other.base_identity()
        } else {
            self.display_name == other.display_name
        }
    }
}

/// 从 card_json + 可选 suffix 提取 ModelRegistrationIdentity。
fn extract_model_registration_identity(
    card_json: &serde_json::Value,
    model_suffix: Option<&str>,
) -> Result<ModelRegistrationIdentity> {
    let display_name = card_json
        .get("display_name")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| {
            anyhow::anyhow!("failed to deserialize model display_name from card_json")
        })?;
    let source_path = card_json
        .get("source_path")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let is_lora =
        model_suffix.is_some() || card_json.get("lora").is_some_and(|v| !v.is_null());
    Ok(ModelRegistrationIdentity { display_name, source_path, is_lora })
}

/// 在已有实例集合中查找“与请求方不兼容”的模型 display_name。
fn find_conflicting_model_name(
    instances: &[DiscoveryInstance],
    requested: &ModelRegistrationIdentity,
) -> Result<Option<String>> {
    for inst in instances {
        if let DiscoveryInstance::Model { card_json, model_suffix, .. } = inst {
            let existing = extract_model_registration_identity(card_json, model_suffix.as_deref())?;
            if !requested.is_compatible_with(&existing) {
                return Ok(Some(existing.display_name));
            }
        }
    }
    Ok(None)
}

/// 构造冲突错误信息。抽出来避免 register 主路径两处字符串重复。
fn conflict_error(
    requested: &str,
    conflicting: &str,
    namespace: &str,
    servicegroup: &str,
    portname: &str,
) -> anyhow::Error {
    anyhow::anyhow!(
        "Cannot register model '{requested}' on portname '{namespace}/{servicegroup}/{portname}': \
         a different model '{conflicting}' is already registered there"
    )
}

// === Discovery trait =========================================================

/// 跨后端统一的服务发现 trait。
#[async_trait]
pub trait Discovery: Send + Sync {
    /// 当前 worker 的唯一 ID（etcd lease / mem 计数器 / k8s pod hash 等）。
    fn instance_id(&self) -> u64;

    /// 注册一个对象。默认实现内置 Model 名称冲突防护：
    ///
    /// 1. 提取 ModelRegistrationIdentity；
    /// 2. 写入前 list portname 已有模型 → 冲突拒绝；
    /// 3. 写入后再 list → 仍冲突时回滚（unregister 失败也通过 `.context()` 冒泡）。
    ///
    /// 非 Model spec 直接走 [`register_internal`]。
    async fn register(&self, spec: DiscoverySpec) -> Result<DiscoveryInstance> {
        let (namespace, servicegroup, portname, requested) = match &spec {
            DiscoverySpec::Model {
                namespace,
                servicegroup,
                portname,
                card_json,
                model_suffix,
                ..
            } => (
                namespace.clone(),
                servicegroup.clone(),
                portname.clone(),
                extract_model_registration_identity(card_json, model_suffix.as_deref())?,
            ),
            _ => return self.register_internal(spec).await,
        };

        let query = DiscoveryQuery::PortNameModels {
            namespace: namespace.clone(),
            servicegroup: servicegroup.clone(),
            portname: portname.clone(),
        };

        // 预检查。
        if let Some(conflicting) =
            find_conflicting_model_name(&self.list(query.clone()).await?, &requested)?
        {
            return Err(conflict_error(
                &requested.display_name,
                &conflicting,
                &namespace,
                &servicegroup,
                &portname,
            ));
        }

        let instance = self.register_internal(spec).await?;

        // post-check + 回滚
        if let Some(conflicting) =
            find_conflicting_model_name(&self.list(query).await?, &requested)?
        {
            if let Err(unregister_err) = self.unregister(instance.clone()).await {
                return Err(conflict_error(
                    &requested.display_name,
                    &conflicting,
                    &namespace,
                    &servicegroup,
                    &portname,
                ))
                .context(format!(
                    "failed to roll back conflicting model registration for instance {instance_id}: {unregister_err}",
                    instance_id = instance.instance_id()
                ));
            }
            return Err(conflict_error(
                &requested.display_name,
                &conflicting,
                &namespace,
                &servicegroup,
                &portname,
            ));
        }

        Ok(instance)
    }

    /// 后端真实写入实现。
    async fn register_internal(&self, spec: DiscoverySpec) -> Result<DiscoveryInstance>;

    /// 注销实例。
    async fn unregister(&self, instance: DiscoveryInstance) -> Result<()>;

    /// 一次性快照。
    async fn list(&self, query: DiscoveryQuery) -> Result<Vec<DiscoveryInstance>>;

    /// 持续 watch（Added/Removed）。
    async fn list_and_watch(
        &self,
        query: DiscoveryQuery,
        cancel_token: Option<CancellationToken>,
    ) -> Result<DiscoveryStream>;

    /// 主动清理（KV 后端用于在 TTL 之前立即注销自己持有的对象）。默认 no-op。
    fn shutdown(&self) {}
}

// === 单元测试 =================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ── path roundtrip ────────────────────────────────────────────────────────

    /// ## 测试过程
    /// 用 PortNameInstanceId → to_path → from_path 还原，断言相等。
    /// ## 意义
    /// 保证 KV / annotation key 在跨进程通信中可逆。
    #[test]
    fn portname_id_path_roundtrip() {
        let id = PortNameInstanceId {
            namespace: "ns".into(),
            servicegroup: "c".into(),
            portname: "e".into(),
            instance_id: 0xabcd,
        };
        let path = id.to_path();
        assert_eq!(PortNameInstanceId::from_path(&path).unwrap(), id);
    }

    /// ## 测试过程
    /// ModelCardInstanceId 带 suffix 与不带 suffix 各跑一次 path 往返。
    /// ## 意义
    /// LoRA 路径“多一段”的语义不能丢，否则 LoRA 实例会与基模混淆。
    #[test]
    fn model_id_path_roundtrip_with_and_without_suffix() {
        for suffix in [None, Some("lora-x".to_string())] {
            let id = ModelCardInstanceId {
                namespace: "ns".into(),
                servicegroup: "c".into(),
                portname: "e".into(),
                instance_id: 0x10,
                model_suffix: suffix.clone(),
            };
            let back = ModelCardInstanceId::from_path(&id.to_path()).unwrap();
            assert_eq!(back, id);
        }
    }

    /// ## 测试过程
    /// EventChannelInstanceId 做 path 往返。
    /// ## 意义
    /// 与 PortName 走同一编码格式，但 topic 替代 portname，保证字段顺序不串。
    #[test]
    fn event_channel_id_path_roundtrip() {
        let id = EventChannelInstanceId {
            namespace: "ns".into(),
            servicegroup: "c".into(),
            topic: "kv-events".into(),
            instance_id: 7,
        };
        assert_eq!(EventChannelInstanceId::from_path(&id.to_path()).unwrap(), id);
    }

    /// ## 测试过程
    /// 用非法 hex 段 / 段数错误的 path 调用 from_path。
    /// ## 意义
    /// 解析必须返回错误而不是 panic 或返回错值。
    #[test]
    fn from_path_rejects_invalid_input() {
        assert!(PortNameInstanceId::from_path("a/b/c").is_err());
        assert!(PortNameInstanceId::from_path("a/b/c/zz").is_err());
        assert!(ModelCardInstanceId::from_path("a/b/c/1/x/extra").is_err());
        assert!(EventChannelInstanceId::from_path("a/b/c").is_err());
    }

    // ── EventTransport ────────────────────────────────────────────────────────

    /// ## 测试过程
    /// 分别用 nats() / zmq() 构造，调用 kind() / address()。
    /// ## 意义
    /// 验证 kind 分派与地址提取一致，无串变体。
    #[test]
    fn event_transport_kind_and_address() {
        let n = EventTransport::nats("ns.x");
        let z = EventTransport::zmq("tcp://h:1");
        assert_eq!(n.kind(), EventTransportKind::Nats);
        assert_eq!(n.address(), "ns.x");
        assert_eq!(z.kind(), EventTransportKind::Zmq);
        assert_eq!(z.address(), "tcp://h:1");
    }

    /// ## 测试过程
    /// ZmqBroker 变体首个 xsub 端点应被 address() 返回；空列表时返回 ""。
    /// ## 意义
    /// 防止空 broker 列表 panic，并锁定“首个 xsub”这一约定。
    #[test]
    fn event_transport_zmq_broker_address() {
        let b1 = EventTransport::ZmqBroker {
            xsub_endpoints: vec!["tcp://h:1".into(), "tcp://h:2".into()],
            xpub_endpoints: vec![],
        };
        let b0 = EventTransport::ZmqBroker {
            xsub_endpoints: vec![],
            xpub_endpoints: vec![],
        };
        assert_eq!(b1.address(), "tcp://h:1");
        assert_eq!(b0.address(), "");
    }

    /// ## 测试过程
    /// EventCodecKind 默认值 + EventTransportKind::default_codec 映射。
    /// ## 意义
    /// 防止默认值漂移破坏既有部署的事件解码。
    #[test]
    fn default_codec_mapping() {
        assert_eq!(EventTransportKind::Nats.default_codec(), EventCodecKind::Json);
        assert_eq!(EventTransportKind::Zmq.default_codec(), EventCodecKind::Msgpack);
    }

    // ── EventChannelQuery scope_level ─────────────────────────────────────────

    /// ## 测试过程
    /// 构造 all / namespace / servicegroup / topic 四种 query，断言 scope_level 0..3。
    /// ## 意义
    /// 上层据此决定从哪个层级开始扫描，错配将直接导致结果不全或全表扫描。
    #[test]
    fn event_channel_query_scope_level() {
        assert_eq!(EventChannelQuery::all().scope_level(), 0);
        assert_eq!(EventChannelQuery::namespace("n").scope_level(), 1);
        assert_eq!(EventChannelQuery::servicegroup("n", "c").scope_level(), 2);
        assert_eq!(EventChannelQuery::topic("n", "c", "t").scope_level(), 3);
    }

    // ── Model identity ────────────────────────────────────────────────────────

    fn identity(name: &str, src: Option<&str>, is_lora: bool) -> ModelRegistrationIdentity {
        ModelRegistrationIdentity {
            display_name: name.into(),
            source_path: src.map(str::to_owned),
            is_lora,
        }
    }

    /// ## 测试过程
    /// 两个普通模型 display_name 相同 → 兼容；不同 → 不兼容。
    /// ## 意义
    /// 普通模型走严格 display_name 匹配，避免错把不同模型当同一份。
    #[test]
    fn identity_plain_models_compare_by_display_name() {
        assert!(identity("a", None, false).is_compatible_with(&identity("a", None, false)));
        assert!(!identity("a", None, false).is_compatible_with(&identity("b", None, false)));
    }

    /// ## 测试过程
    /// LoRA + 基模 source_path 相同 → 兼容；不同 → 不兼容。
    /// ## 意义
    /// 允许同一基模上挂载 LoRA，但禁止跨基模混挂。
    #[test]
    fn identity_lora_uses_source_path() {
        let lora = identity("adapter", Some("/m"), true);
        let base_same = identity("base", Some("/m"), false);
        let base_diff = identity("base", Some("/other"), false);
        assert!(lora.is_compatible_with(&base_same));
        assert!(!lora.is_compatible_with(&base_diff));
    }

    /// ## 测试过程
    /// 用 model_suffix=Some + card_json 含 display_name 提取 identity。
    /// ## 意义
    /// 验证 is_lora 由 suffix 触发（即使 card_json 中不显式标 lora）。
    #[test]
    fn extract_identity_marks_lora_via_suffix() {
        let card = serde_json::json!({ "display_name": "x", "source_path": "/m" });
        let id = extract_model_registration_identity(&card, Some("lora-a")).unwrap();
        assert!(id.is_lora);
        assert_eq!(id.base_identity(), "/m");
    }

    /// ## 测试过程
    /// card_json 缺 display_name 时调用 extract，应返回 Err。
    /// ## 意义
    /// 模型注册必须有 display_name，否则后续冲突检测无意义。
    #[test]
    fn extract_identity_fails_without_display_name() {
        let card = serde_json::json!({ "x": 1 });
        assert!(extract_model_registration_identity(&card, None).is_err());
    }

    /// ## 测试过程
    /// 已有实例集合含 display_name="a"；请求 display_name="b" → 返回 Some("a")。
    /// ## 意义
    /// 验证 find_conflicting_model_name 在“真冲突”时定位到现有模型名。
    #[test]
    fn find_conflict_returns_existing_name() {
        let existing = DiscoveryInstance::Model {
            namespace: "n".into(),
            servicegroup: "c".into(),
            portname: "e".into(),
            instance_id: 1,
            card_json: serde_json::json!({ "display_name": "a" }),
            model_suffix: None,
        };
        let req = identity("b", None, false);
        assert_eq!(
            find_conflicting_model_name(&[existing], &req).unwrap(),
            Some("a".into())
        );
    }

    /// ## 测试过程
    /// 集合含一个 PortName + 一个同名 Model；请求与该 Model 兼容。
    /// ## 意义
    /// PortName 不应被纳入冲突判定，且兼容时返回 None。
    #[test]
    fn find_conflict_ignores_portnames_and_returns_none_when_compatible() {
        use crate::servicegroup::{Instance, TransportType};
        let ep = DiscoveryInstance::PortName(Instance {
            namespace: "n".into(),
            servicegroup: "c".into(),
            portname: "e".into(),
            instance_id: 2,
            transport: TransportType::Nats("nats://x".into()),
            device_type: None,
        });
        let same_model = DiscoveryInstance::Model {
            namespace: "n".into(),
            servicegroup: "c".into(),
            portname: "e".into(),
            instance_id: 1,
            card_json: serde_json::json!({ "display_name": "a" }),
            model_suffix: None,
        };
        let req = identity("a", None, false);
        assert_eq!(
            find_conflicting_model_name(&[ep, same_model], &req).unwrap(),
            None
        );
    }

    // ── DiscoverySpec helpers ─────────────────────────────────────────────────

    /// ## 测试过程
    /// from_model 后 with_instance_id；检查得到的 DiscoveryInstance::Model 字段。
    /// ## 意义
    /// 验证 spec→instance 转换的字段保留与 instance_id 绑定。
    #[test]
    fn discovery_spec_from_model_then_with_id() {
        #[derive(Serialize)]
        struct Card { display_name: String }
        let spec = DiscoverySpec::from_model(
            "n".into(),
            "c".into(),
            "e".into(),
            &Card { display_name: "m".into() },
        )
        .unwrap();
        let inst = spec.with_instance_id(0x42);
        match inst {
            DiscoveryInstance::Model { instance_id, card_json, .. } => {
                assert_eq!(instance_id, 0x42);
                assert_eq!(card_json["display_name"], "m");
            }
            _ => panic!("expected Model variant"),
        }
    }

    /// ## 测试过程
    /// 用 EventChannel spec→instance；id() 应返回 EventChannel 变体并保留字段。
    /// ## 意义
    /// 验证 EventChannel 路径的 id 提取不串到 PortName/Model。
    #[test]
    fn event_channel_instance_id_extraction() {
        let inst = DiscoverySpec::EventChannel {
            namespace: "n".into(),
            servicegroup: "c".into(),
            topic: "kv".into(),
            transport: EventTransport::zmq("tcp://x:1"),
        }
        .with_instance_id(9);
        match inst.id() {
            DiscoveryInstanceId::EventChannel(id) => {
                assert_eq!(id.topic, "kv");
                assert_eq!(id.instance_id, 9);
            }
            _ => panic!("expected EventChannel id"),
        }
    }

    /// ## 测试过程
    /// 对 PortName instance 调用 deserialize_model::<serde_json::Value>。
    /// ## 意义
    /// 错类型反序列化必须返回 Err，避免静默把 transport 误解为 model。
    #[test]
    fn deserialize_model_rejects_portname() {
        use crate::servicegroup::{Instance, TransportType};
        let inst = DiscoveryInstance::PortName(Instance {
            namespace: "n".into(),
            servicegroup: "c".into(),
            portname: "e".into(),
            instance_id: 1,
            transport: TransportType::Nats("nats://x".into()),
            device_type: None,
        });
        assert!(inst.deserialize_model::<serde_json::Value>().is_err());
    }

    // ── trait default register 行为 ───────────────────────────────────────────

    /// ## 测试过程
    /// 用 MockDiscovery 注册两个**不同 display_name** 模型到同一 portname，
    /// 第二次 register 应返回错误，且 list 仍只有 1 条。
    /// ## 意义
    /// 端到端验证 Discovery::register 默认实现的 model 冲突防护链路。
    #[tokio::test]
    async fn default_register_blocks_conflicting_model() {
        let reg = SharedMockRegistry::new();
        let a = MockDiscovery::new(Some(1), reg.clone());
        let b = MockDiscovery::new(Some(2), reg.clone());

        let spec_a = DiscoverySpec::Model {
            namespace: "n".into(),
            servicegroup: "c".into(),
            portname: "e".into(),
            card_json: serde_json::json!({ "display_name": "m1" }),
            model_suffix: None,
        };
        let spec_b = DiscoverySpec::Model {
            namespace: "n".into(),
            servicegroup: "c".into(),
            portname: "e".into(),
            card_json: serde_json::json!({ "display_name": "m2" }),
            model_suffix: None,
        };

        a.register(spec_a).await.unwrap();
        let res = b.register(spec_b).await;
        assert!(res.is_err());
        let all = a.list(DiscoveryQuery::AllModels).await.unwrap();
        assert_eq!(all.len(), 1);
    }
}
