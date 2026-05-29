// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `component` —— 分布式应用的顶层构件树
//!
//! 本模块定义了构建 Dynamo 分布式应用所需的最核心三层模型：
//!
//! ```text
//! Namespace
//!   └── Component
//!         └── Endpoint
//! ```
//!
//! - [`Namespace`]：逻辑分组，可嵌套（父 namespace → 子 namespace），
//!   用作发现平面（discovery）的命名空间隔离；
//! - [`Component`]：一个具体的"工作单元"，比如 Preprocessor、
//!   SmartRouter，承载若干配置文件与可调用端点；
//! - [`Endpoint`]：网络可达的服务入口，绑定到具体的请求平面（NATS /
//!   TCP / HTTP）；
//! - [`Instance`]：上述 endpoint 的某一次具体注册（连接 ID 维度）。
//!
//! 此外模块还公开了：
//!
//! - [`TransportType`] / [`DeviceType`]：用于发现平面的传输地址与硬件
//!   维度；
//! - [`Registry`] + `RegistryInner`：NATS 服务句柄的进程级表；
//! - [`build_transport_type`]（来自子模块 `endpoint`）的 re-export。
//!
//! ## 设计意图
//!
//! 1. **数据模型与生命周期分离**：本文件只放结构定义、字段拼装、最
//!    常见的查询方法；真正涉及"挂指标 / 启动端点 / 注册到发现平面"
//!    的复杂编排放在子模块 [`endpoint`] 里，避免文件过长。
//! 2. **构造器统一走 derive_builder**：`Component` / `Namespace` 都用
//!    `#[derive(Builder)]`，使外部可以以"小步快跑"的方式拼装对象，同
//!    时通过 `#[validate(...)]` 在 `build()` 出口做名字合法性校验。
//! 3. **可观测树形结构**：`MetricsHierarchy` 让 namespace → component
//!    → endpoint 自然形成一棵指标树，根永远是 `DistributedRuntime`，
//!    路径拼装与 connection_id 透传都在这棵树上完成。
//! 4. **NATS 服务注册的可靠性**：`ComponentBuilder::build()` 在 NATS
//!    模式下必须等到服务被 component registry 真正写入再返回，否则后
//!    续 `serve_endpoint()` 会出现 lookup 失败。本文件在这里用
//!    `block_in_place + blocking_recv` 把异步注册"塞"进同步出口，并
//!    把这一段抽成单独 helper 以便阅读。
//!
//! ## 外部契约（必须严格保持）
//!
//! - 所有 `pub` 类型、`pub` 字段、`pub fn`、`pub use`；
//! - `#[derive(...)]` 与 `#[builder(...)]` / `#[validate(...)]` /
//!   `#[serde(...)]` 属性；
//! - 函数 `validate_allowed_chars`（regex `^[a-z0-9-_]+$`）；
//! - `Instance` 的 `Display` 格式 `ns/comp/ep/id`；
//! - `Instance` 序列化时对 `device_type: None` 的 `skip_serializing_if`。
//!
//! 上述任何一项的变更都属于破坏性改动，必须先评估外部调用方影响。

use std::fmt;

use crate::{
    config::HealthStatus,
    distributed::RequestPlaneMode,
    metrics::{MetricsHierarchy, MetricsRegistry, prometheus_names},
    service::ServiceClient,
    service::ServiceSet,
};

use super::{DistributedRuntime, Runtime, traits::*, transports::nats::Slug, utils::Duration};

use crate::pipeline::network::{PushWorkHandler, ingress::push_endpoint::PushEndpoint};
use crate::protocols::EndpointId;
use async_nats::{
    rustls::quic,
    service::{Service, ServiceExt},
};
use dashmap::DashMap;
use derive_builder::Builder;
use derive_getters::Getters;
use educe::Educe;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, hash::Hash, sync::Arc};
use validator::{Validate, ValidationError};

// ============================================================================
// 子模块声明 & re-export
// ============================================================================

mod client;
#[allow(clippy::module_inception)]
mod component;
mod endpoint;
mod namespace;
mod registry;
pub mod service;

pub use client::Client;
pub(crate) use client::EndpointDiscoverySource;
pub(crate) use client::RoutingOccupancyState;
pub(crate) use client::get_or_create_routing_occupancy_state;
pub use endpoint::build_transport_type;

// ============================================================================
// 传输类型 & 设备类型
// ============================================================================

/// 端点暴露给发现平面的"传输地址"。
///
/// 三种变体对应三种请求平面：
///
/// - [`TransportType::Nats`]：NATS subject 字符串；
/// - [`TransportType::Http`]：完整的 HTTP URL（含 host / port / path）；
/// - [`TransportType::Tcp`]：`host:port/instance_id_hex/endpoint_name` 形式。
///
/// 在 JSON 中以 `snake_case` 输出，且 `Nats` 变体被特意 rename 为
/// `nats_tcp`（保留历史兼容性）。
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TransportType {
    #[serde(rename = "nats_tcp")]
    Nats(String),
    Http(String),
    Tcp(String),
}

/// 端点所属物理设备的粗分类。用于路由层做 GPU/CPU 感知调度。
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum DeviceType {
    Cpu,
    Cuda,
}

// ============================================================================
// NATS 服务注册表
// ============================================================================

/// 进程级 NATS 服务表的底层数据。`Registry` 持有该结构的 Arc<Mutex<>>
/// 句柄。键是 component 的 service_name，值是 NATS 服务句柄。
#[derive(Default)]
pub struct RegistryInner {
    pub(crate) services: HashMap<String, Service>,
}

/// 共享句柄，多处持有同一个 `RegistryInner`。
///
/// 构造与默认值在 [`crate::component::registry`] 子模块里实现。
#[derive(Clone)]
pub struct Registry {
    pub(crate) inner: Arc<tokio::sync::Mutex<RegistryInner>>,
}

// ============================================================================
// Instance：端点的一次具体注册
// ============================================================================

/// 一个端点的"运行时实例"——即"哪个 worker 进程里的哪个连接"在
/// 提供该端点。
///
/// `Instance` 是序列化进发现平面 (`etcd` / `kvstore` 等) 的最终形态，
/// 路由器据此选择目标 worker。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Instance {
    pub component: String,
    pub endpoint: String,
    pub namespace: String,
    pub instance_id: u64,
    pub transport: TransportType,
    /// 设备类型。`None` 表示未知或不关心；为了在 JSON 中省略，标
    /// 注了 `skip_serializing_if = "Option::is_none"`。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_type: Option<DeviceType>,
}

impl Instance {
    /// 返回 `instance_id` 字段，便于上层只关心 ID 时少打字。
    pub fn id(&self) -> u64 {
        self.instance_id
    }

    /// 把三元组 `(namespace, component, endpoint)` 打包成 `EndpointId`。
    ///
    /// 不附带 `instance_id`——这是一个"端点身份"而不是"实例身份"。
    pub fn endpoint_id(&self) -> EndpointId {
        EndpointId {
            namespace: self.namespace.clone(),
            component: self.component.clone(),
            name: self.endpoint.clone(),
        }
    }

    /// 把四元组 `(namespace, component, endpoint, instance_id)` 打包成
    /// `discovery::EndpointInstanceId`，供发现平面定位"具体实例"。
    pub fn endpoint_instance_id(&self) -> crate::discovery::EndpointInstanceId {
        crate::discovery::EndpointInstanceId {
            namespace: self.namespace.clone(),
            component: self.component.clone(),
            endpoint: self.endpoint.clone(),
            instance_id: self.instance_id,
        }
    }
}

impl fmt::Display for Instance {
    /// Display 格式：`{namespace}/{component}/{endpoint}/{instance_id}`。
    /// 这一格式同时被 `Ord` 实现作为排序键，**不可随意更改**。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{}/{}/{}",
            self.namespace, self.component, self.endpoint, self.instance_id
        )
    }
}

impl Ord for Instance {
    /// 直接按 `Display` 字符串做字典序比较。
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.to_string().cmp(&other.to_string())
    }
}

impl PartialOrd for Instance {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// ============================================================================
// Component：分布式应用的核心构件
// ============================================================================

/// 分布式应用的"组件"——可在发现平面被找到，可托管若干 [`Endpoint`]。
///
/// `Component` 通过 [`ComponentBuilder`] 构造（`#[derive(Builder)]`），
/// `name` 字段会被 [`validate_allowed_chars`] 校验。
#[derive(Educe, Builder, Clone, Validate)]
#[educe(Debug)]
#[builder(pattern = "owned", build_fn(private, name = "build_internal"))]
pub struct Component {
    #[builder(private)]
    #[educe(Debug(ignore))]
    drt: Arc<DistributedRuntime>,

    /// Name of the component
    #[builder(setter(into))]
    #[validate(custom(function = "validate_allowed_chars"))]
    name: String,

    /// Additional labels for metrics
    #[builder(default = "Vec::new()")]
    labels: Vec<(String, String)>,

    // todo - restrict the namespace to a-z0-9-_A-Z
    /// Namespace
    #[builder(setter(into))]
    namespace: Namespace,

    /// This hierarchy's own metrics registry
    #[builder(default = "crate::MetricsRegistry::new()")]
    metrics_registry: crate::MetricsRegistry,
}

// ----- Hash / Eq：身份等价定义为 `(namespace.name, name)` -----

impl Hash for Component {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // namespace.name() 已经把祖先链拼成完整路径，再加 component name
        // 即可得到全局唯一身份。
        self.namespace.name().hash(state);
        self.name.hash(state);
    }
}

impl PartialEq for Component {
    fn eq(&self, other: &Self) -> bool {
        self.namespace.name() == other.namespace.name() && self.name == other.name
    }
}

impl Eq for Component {}

impl std::fmt::Display for Component {
    /// `{namespace}.{component}`。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.namespace.name(), self.name)
    }
}

// ----- Runtime / MetricsHierarchy 透传 -----

impl DistributedRuntimeProvider for Component {
    fn drt(&self) -> &DistributedRuntime {
        &self.drt
    }
}

impl RuntimeProvider for Component {
    fn rt(&self) -> &Runtime {
        self.drt.rt()
    }
}

impl MetricsHierarchy for Component {
    fn basename(&self) -> String {
        self.name.clone()
    }

    /// 祖先链 = namespace 的祖先链 + namespace 本身（自根至叶）。
    fn parent_hierarchies(&self) -> Vec<&dyn MetricsHierarchy> {
        let ns_chain = self.namespace.parent_hierarchies();
        let mut out: Vec<&dyn MetricsHierarchy> = Vec::with_capacity(ns_chain.len() + 1);
        out.extend(ns_chain);
        out.push(&self.namespace as &dyn MetricsHierarchy);
        out
    }

    fn get_metrics_registry(&self) -> &MetricsRegistry {
        &self.metrics_registry
    }

    fn connection_id(&self) -> Option<u64> {
        Some(self.drt.connection_id())
    }
}

// ----- 业务方法 -----

impl Component {
    /// 拼出本组件在 NATS / 服务平面的对外名称。
    ///
    /// 格式：`slugify({namespace}_{name})`，确保不出现 `.` 等会破坏
    /// subject 解析的字符。
    pub fn service_name(&self) -> String {
        let raw = format!("{}_{}", self.namespace.name(), self.name);
        Slug::slugify(&raw).to_string()
    }

    /// 返回所属 namespace 引用。
    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    /// 返回组件名。
    pub fn name(&self) -> &str {
        &self.name
    }

    /// 返回所有附加 metrics label。
    pub fn labels(&self) -> &[(String, String)] {
        &self.labels
    }

    /// 在本组件下创建一个新的 [`Endpoint`]。
    ///
    /// ## 副作用
    ///
    /// 会把 endpoint 的 `MetricsRegistry` 作为 child 挂到 component 的
    /// 注册表下，确保 Prometheus scrape 时能遍历到（且避免命名冲突）。
    pub fn endpoint(&self, endpoint: impl Into<String>) -> Endpoint {
        let ep = Endpoint {
            component: self.clone(),
            name: endpoint.into(),
            labels: Vec::new(),
            metrics_registry: crate::MetricsRegistry::new(),
        };
        // 把新 endpoint 的注册表挂到当前 component 的注册表下，让指标
        // 抓取时形成正确的树形遍历。
        self.get_metrics_registry()
            .add_child_registry(ep.get_metrics_registry());
        ep
    }

    /// 列出本组件下所有已注册的端点实例。
    ///
    /// ## 实现步骤
    ///
    /// 1. 构造一次 `DiscoveryQuery::ComponentEndpoints`；
    /// 2. 把发现平面返回的 `DiscoveryInstance` 过滤成只剩 `Endpoint` 变
    ///    体（忽略 ModelCard 之类的非端点条目）；
    /// 3. 按 `Instance::cmp`（字典序）排序后返回。
    pub async fn list_instances(&self) -> anyhow::Result<Vec<Instance>> {
        let discovery = self.drt.discovery();
        let query = crate::discovery::DiscoveryQuery::ComponentEndpoints {
            namespace: self.namespace.name(),
            component: self.name.clone(),
        };
        let raw = discovery.list(query).await?;
        let mut instances: Vec<Instance> = raw
            .into_iter()
            .filter_map(|di| match di {
                crate::discovery::DiscoveryInstance::Endpoint(inst) => Some(inst),
                _ => None,
            })
            .collect();
        instances.sort();
        Ok(instances)
    }
}

// ============================================================================
// ComponentBuilder：构造 + NATS 服务注册的同步等待
// ============================================================================

impl ComponentBuilder {
    /// 由 `Namespace::component` 间接调用。预填 `drt` 字段，外部使用方
    /// 只需提供 name / namespace。
    pub fn from_runtime(drt: Arc<DistributedRuntime>) -> Self {
        Self::default().drt(drt)
    }

    /// 构造 `Component`。
    ///
    /// ## 行为
    ///
    /// 1. 先通过 derive_builder 生成的 `build_internal()` 拿到组件；
    /// 2. 若当前 request plane 是 NATS，则需要立刻把该组件登记到
    ///    `DistributedRuntime` 持有的 NATS 服务表，并**同步**等待结果，
    ///    确保后续 `serve_endpoint()` 能在 registry 里找到该 service。
    ///
    /// 同步等待这一步通过 `tokio::task::block_in_place + blocking_recv`
    /// 实现，并被抽到 [`await_nats_registration`] 助手里。
    pub fn build(self) -> Result<Component, anyhow::Error> {
        let component = self.build_internal()?;
        if component.drt().request_plane().is_nats() {
            await_nats_registration(&component)?;
        }
        Ok(component)
    }
}

/// 同步等待 `register_nats_service` 在 component registry 上写入完成。
///
/// ## 失败语义
///
/// - `Some(Ok(()))` → NATS 服务注册完成；
/// - `Some(Err(e))` → 注册期间报错；
/// - `None` → 注册通道意外关闭（很可能是运行时正在关闭）。
fn await_nats_registration(component: &Component) -> anyhow::Result<()> {
    let drt = component.drt();
    let mut rx = drt.register_nats_service(component.clone());

    // block_in_place 把当前异步任务暂时移出运行时线程，让 blocking_recv
    // 能合法阻塞而不致使整个 runtime 卡死。
    let received = tokio::task::block_in_place(|| rx.blocking_recv());

    match received {
        Some(Ok(())) => {
            tracing::debug!(
                component = component.service_name(),
                "NATS service registration completed",
            );
            Ok(())
        }
        Some(Err(e)) => Err(anyhow::anyhow!(
            "NATS service registration failed for component '{}': {}",
            component.service_name(),
            e
        )),
        None => Err(anyhow::anyhow!(
            "NATS service registration channel closed unexpectedly for component '{}'",
            component.service_name()
        )),
    }
}

// ============================================================================
// Endpoint：可调用入口
// ============================================================================

/// 一个 component 下的具体调用入口。
///
/// `Endpoint` 不走 `#[derive(Builder)]`——构造路径都是
/// `Component::endpoint(name)`，外部不直接 new。它的 lifecycle 编排在
/// [`endpoint`] 子模块中实现。
#[derive(Debug, Clone)]
pub struct Endpoint {
    component: Component,

    // todo - restrict alphabet
    /// Endpoint name
    name: String,

    /// Additional labels for metrics
    labels: Vec<(String, String)>,

    /// This hierarchy's own metrics registry
    metrics_registry: crate::MetricsRegistry,
}

// ----- Hash / Eq：身份等价定义为 `(component, name)` -----

impl Hash for Endpoint {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.component.hash(state);
        self.name.hash(state);
    }
}

impl PartialEq for Endpoint {
    fn eq(&self, other: &Self) -> bool {
        self.component == other.component && self.name == other.name
    }
}

impl Eq for Endpoint {}

// ----- Runtime / MetricsHierarchy 透传 -----

impl DistributedRuntimeProvider for Endpoint {
    fn drt(&self) -> &DistributedRuntime {
        self.component.drt()
    }
}

impl RuntimeProvider for Endpoint {
    fn rt(&self) -> &Runtime {
        self.component.rt()
    }
}

impl MetricsHierarchy for Endpoint {
    fn basename(&self) -> String {
        self.name.clone()
    }

    /// 祖先链 = component 的祖先链 + component 本身。
    fn parent_hierarchies(&self) -> Vec<&dyn MetricsHierarchy> {
        let comp_chain = self.component.parent_hierarchies();
        let mut out: Vec<&dyn MetricsHierarchy> = Vec::with_capacity(comp_chain.len() + 1);
        out.extend(comp_chain);
        out.push(&self.component as &dyn MetricsHierarchy);
        out
    }

    fn get_metrics_registry(&self) -> &MetricsRegistry {
        &self.metrics_registry
    }

    fn connection_id(&self) -> Option<u64> {
        Some(self.component.drt().connection_id())
    }
}

// ----- 业务方法 -----

impl Endpoint {
    /// 返回端点三元组 ID（不含 instance_id）。
    pub fn id(&self) -> EndpointId {
        EndpointId {
            namespace: self.component.namespace().name().to_string(),
            component: self.component.name().to_string(),
            name: self.name.clone(),
        }
    }

    /// 返回端点名（不含命名空间前缀）。
    pub fn name(&self) -> &str {
        &self.name
    }

    /// 返回所属 component 引用。
    pub fn component(&self) -> &Component {
        &self.component
    }

    /// 异步构造一个该端点的 [`Client`]，供调用方发起 push / response。
    pub async fn client(&self) -> anyhow::Result<client::Client> {
        client::Client::new(self.clone()).await
    }

    /// 取一个 endpoint config builder，用于 `start()` 端点。详见
    /// [`endpoint::EndpointConfigBuilder`]。
    pub fn endpoint_builder(&self) -> endpoint::EndpointConfigBuilder {
        endpoint::EndpointConfigBuilder::from_endpoint(self.clone())
    }
}

// ============================================================================
// Namespace：可嵌套的逻辑分组
// ============================================================================

/// 一个命名空间——树形结构的"分组节点"。`name()` 会拼出完整路径
/// `parent.name.child.name` 直到根。
///
/// 通过 [`NamespaceBuilder`]（derive_builder 生成）构造；构造路径主要
/// 是 [`Namespace::new`]、[`Namespace::namespace`]，外部不直接用 builder。
#[derive(Builder, Clone, Validate)]
#[builder(pattern = "owned")]
pub struct Namespace {
    #[builder(private)]
    runtime: Arc<DistributedRuntime>,

    #[validate(custom(function = "validate_allowed_chars"))]
    name: String,

    #[builder(default = "None")]
    parent: Option<Arc<Namespace>>,

    /// Additional labels for metrics
    #[builder(default = "Vec::new()")]
    labels: Vec<(String, String)>,

    /// This hierarchy's own metrics registry
    #[builder(default = "crate::MetricsRegistry::new()")]
    metrics_registry: crate::MetricsRegistry,

    /// Cache for components to avoid duplicate registrations and metrics collisions.
    /// When the same component is requested multiple times, we return the cached instance
    /// to ensure all endpoints share the same Component and MetricsRegistry.
    /// Uses DashMap for lock-free reads and automatic handling of concurrent inserts.
    #[builder(default = "Arc::new(DashMap::new())")]
    component_cache: Arc<DashMap<String, Component>>,
}

// ----- Runtime 透传 -----

impl DistributedRuntimeProvider for Namespace {
    fn drt(&self) -> &DistributedRuntime {
        &self.runtime
    }
}

impl RuntimeProvider for Namespace {
    fn rt(&self) -> &Runtime {
        self.runtime.rt()
    }
}

impl std::fmt::Debug for Namespace {
    /// 自定义 Debug，避免把整张 `component_cache` / `metrics_registry`
    /// 打印出来。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Namespace {{ name: {}; parent: {:?} }}",
            self.name, self.parent
        )
    }
}

impl std::fmt::Display for Namespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

// ----- 业务方法 -----

impl Namespace {
    /// 创建一个**根** namespace。仅 `DistributedRuntime` 调用。
    ///
    /// 会把当前 namespace 的 metrics 注册表挂到 DRT 之下，保证指标抓
    /// 取走的是同一棵树。
    pub(crate) fn new(runtime: DistributedRuntime, name: String) -> anyhow::Result<Self> {
        let ns = NamespaceBuilder::default()
            .runtime(Arc::new(runtime))
            .name(name)
            .build()?;
        ns.drt()
            .get_metrics_registry()
            .add_child_registry(ns.get_metrics_registry());
        Ok(ns)
    }

    /// 在本 namespace 下取/造一个组件。
    ///
    /// ## 同名缓存
    ///
    /// 同一 namespace 多次以同一名字调用 `component(name)`，必须返回
    /// 同一个 `Component` 实例。否则 endpoint 会被挂到不同的
    /// `MetricsRegistry`，造成同名指标重复注册。
    ///
    /// 用 `DashMap` 既保证 lock-free 读，又能处理并发插入。
    pub fn component(&self, name: impl Into<String>) -> anyhow::Result<Component> {
        let name = name.into();

        // 快路径：命中缓存直接返回 clone。
        if let Some(hit) = self.component_cache.get(&name) {
            return Ok(hit.value().clone());
        }

        // 慢路径：构造一个新 component。
        let component = ComponentBuilder::from_runtime(self.runtime.clone())
            .name(&name)
            .namespace(self.clone())
            .build()?;

        // 把 component 的指标注册表挂到 namespace 注册表下（树形挂载）。
        self.get_metrics_registry()
            .add_child_registry(component.get_metrics_registry());

        // 写入缓存。即使并发场景下我们和别的线程同时插入同一个 key，
        // DashMap 也能正确合并。
        self.component_cache.insert(name, component.clone());
        Ok(component)
    }

    /// 在本 namespace 下创建子 namespace（仍是 `Namespace` 类型）。
    pub fn namespace(&self, name: impl Into<String>) -> anyhow::Result<Namespace> {
        let child = NamespaceBuilder::default()
            .runtime(self.runtime.clone())
            .name(name.into())
            .parent(Some(Arc::new(self.clone())))
            .build()?;
        self.get_metrics_registry()
            .add_child_registry(child.get_metrics_registry());
        Ok(child)
    }

    /// 返回当前 namespace 的"完整路径名"——从根开始用 `.` 拼接。
    pub fn name(&self) -> String {
        match &self.parent {
            Some(parent) => format!("{}.{}", parent.name(), self.name),
            None => self.name.clone(),
        }
    }
}

// ============================================================================
// 名字合法性校验
// ============================================================================

/// 校验"名字"字段是否只包含允许的字符。
///
/// 用于 `Component.name` 与 `Namespace.name` 的 `#[validate(custom = ...)]`
/// 钩子。允许字符集合：小写字母 / 数字 / 短横线 `-` / 下划线 `_`。
///
/// ## 入参
///
/// - `input`：待校验字符串。
///
/// ## 返回
///
/// - 匹配 → `Ok(())`；
/// - 不匹配 → `Err(ValidationError::new("invalid_characters"))`。
fn validate_allowed_chars(input: &str) -> Result<(), ValidationError> {
    static ALLOWED: &str = r"^[a-z0-9-_]+$";
    // regex 编译失败属于程序内部 bug，用 unwrap 直接 panic 即可暴露。
    let re = regex::Regex::new(ALLOWED).unwrap();
    if re.is_match(input) {
        Ok(())
    } else {
        Err(ValidationError::new("invalid_characters"))
    }
}

// ============================================================================
// 单元测试
//
// 这里只覆盖纯结构化/纯函数的契约：枚举的 PartialEq / Hash / serde 格
// 式、`Instance` 的 Display / Ord / serde 字段省略、`validate_allowed_chars`
// 的接受/拒绝集合。
//
// 真正涉及 NATS / 发现平面 / DRT 初始化的端到端断言由 `tests/` 与
// `lib/runtime/src/distributed.rs` 内部的 `integration` 测试负责。
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// 构造一个测试用 `Instance`。
    ///
    /// ## 入参
    ///
    /// - `ns` / `comp` / `ep`：三元组各段；
    /// - `id`：实例 ID。
    ///
    /// ## 出参
    ///
    /// 传输用 NATS 拼接、`device_type` 默认 CUDA 的 `Instance`。
    fn fake_instance(ns: &str, comp: &str, ep: &str, id: u64) -> Instance {
        Instance {
            namespace: ns.to_string(),
            component: comp.to_string(),
            endpoint: ep.to_string(),
            instance_id: id,
            transport: TransportType::Nats(format!("{ns}.{comp}.{ep}.{id}")),
            device_type: Some(DeviceType::Cuda),
        }
    }

    // ------------------------------------------------------------------
    // TransportType
    // ------------------------------------------------------------------

    /// ## 测试过程
    /// 构造两个内容相同的 `Nats` 变体与一个内容不同的 `Nats` 变体，
    /// 校验 `PartialEq` 按 (变体 + 内部字符串) 比较。
    #[test]
    fn transport_type_equality() {
        let a = TransportType::Nats("subject.foo".to_string());
        let b = TransportType::Nats("subject.foo".to_string());
        let c = TransportType::Nats("subject.bar".to_string());
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    /// ## 测试过程
    /// `Http` 与 `Tcp` 即使字符串相似也不相等——枚举变体差异即可使
    /// `PartialEq` 返回 false。
    ///
    /// ## 意义
    /// 防止路由层把 HTTP 端点误识别成 TCP，避免请求被打到错误的处理器。
    #[test]
    fn transport_type_http_vs_tcp_not_equal() {
        let http = TransportType::Http("http://localhost:8080/v1/rpc/ep".to_string());
        let tcp = TransportType::Tcp("localhost:9090/abcd1234/ep".to_string());
        assert_ne!(http, tcp);
    }

    /// ## 测试过程
    /// 对三个变体分别做 `serde_json::to_string` → `from_str` 往返，断
    /// 言与原值相等。
    ///
    /// ## 意义
    /// `Instance` 进出发现平面靠 JSON，序列化的正确性直接关系到实例
    /// 发现能否正常工作。
    #[test]
    fn transport_type_serde_roundtrip() {
        for variant in [
            TransportType::Nats("ns.comp.ep.123".to_string()),
            TransportType::Http("http://10.0.0.1:8080/v1/rpc/myep".to_string()),
            TransportType::Tcp("10.0.0.2:9090/deadbeef/myep".to_string()),
        ] {
            let json = serde_json::to_string(&variant).expect("serialize");
            let back: TransportType = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(variant, back);
        }
    }

    /// ## 测试过程
    /// `Nats` 变体的 JSON tag 应是 `"nats_tcp"`（保留兼容性），而不是
    /// 默认的 `"nats"`。
    ///
    /// ## 意义
    /// 锁死这条兼容性，防止后续重构把 rename 抹掉导致存量数据不可读。
    #[test]
    fn transport_type_nats_tag_is_nats_tcp() {
        let v = TransportType::Nats("subj".to_string());
        let json = serde_json::to_string(&v).unwrap();
        assert!(json.contains("nats_tcp"), "got: {json}");
    }

    // ------------------------------------------------------------------
    // DeviceType
    // ------------------------------------------------------------------

    /// ## 测试过程
    /// 对 `Cpu`、`Cuda` 两个变体分别 JSON roundtrip，断言相等。
    #[test]
    fn device_type_serde_roundtrip() {
        for dt in [DeviceType::Cpu, DeviceType::Cuda] {
            let json = serde_json::to_string(&dt).unwrap();
            let back: DeviceType = serde_json::from_str(&json).unwrap();
            assert_eq!(dt, back);
        }
    }

    /// ## 测试过程
    /// 校验 JSON tag 形式：snake_case → `"cpu"` / `"cuda"`。
    #[test]
    fn device_type_snake_case_tags() {
        assert_eq!(serde_json::to_string(&DeviceType::Cpu).unwrap(), "\"cpu\"");
        assert_eq!(
            serde_json::to_string(&DeviceType::Cuda).unwrap(),
            "\"cuda\""
        );
    }

    // ------------------------------------------------------------------
    // Instance
    // ------------------------------------------------------------------

    /// ## 测试过程
    /// `Instance::id()` 与字段 `instance_id` 应严格相等。
    #[test]
    fn instance_id_returns_field() {
        let inst = fake_instance("ns", "comp", "ep", 42);
        assert_eq!(inst.id(), 42);
    }

    /// ## 测试过程
    /// `Instance::endpoint_id()` 应正确从三元组字段提取 `EndpointId`。
    #[test]
    fn instance_endpoint_id_fields() {
        let inst = fake_instance("mynamespace", "mycomp", "myep", 7);
        let eid = inst.endpoint_id();
        assert_eq!(eid.namespace, "mynamespace");
        assert_eq!(eid.component, "mycomp");
        assert_eq!(eid.name, "myep");
    }

    /// ## 测试过程
    /// `Instance::endpoint_instance_id()` 应额外带上 instance_id。
    #[test]
    fn instance_endpoint_instance_id_includes_id() {
        let inst = fake_instance("ns", "comp", "ep", 99);
        let eiid = inst.endpoint_instance_id();
        assert_eq!(eiid.namespace, "ns");
        assert_eq!(eiid.component, "comp");
        assert_eq!(eiid.endpoint, "ep");
        assert_eq!(eiid.instance_id, 99);
    }

    /// ## 测试过程
    /// Display 格式必须是 `ns/comp/ep/id`。
    ///
    /// ## 意义
    /// `Ord` 实现是基于 `to_string()` 的，Display 一旦改动会破坏排序
    /// 稳定性。
    #[test]
    fn instance_display_format() {
        let inst = fake_instance("ns", "comp", "ep", 99);
        assert_eq!(inst.to_string(), "ns/comp/ep/99");
    }

    /// ## 测试过程
    /// 两个组件名字典序不同（alpha < beta）的 `Instance`，比较应符合
    /// 字符串字典序。
    #[test]
    fn instance_ordering_is_lexicographic() {
        let a = fake_instance("ns", "alpha", "ep", 1);
        let b = fake_instance("ns", "beta", "ep", 1);
        assert!(a < b);
    }

    /// ## 测试过程
    /// `device_type = None` 时序列化结果不应包含 `device_type` 字段。
    ///
    /// ## 意义
    /// 这是 `skip_serializing_if` 行为契约：CPU-only 路径不应给发现
    /// 平面额外增加冗余字段。
    #[test]
    fn instance_serde_omits_device_type_when_none() {
        let mut inst = fake_instance("ns", "comp", "ep", 1);
        inst.device_type = None;
        let json = serde_json::to_string(&inst).unwrap();
        assert!(!json.contains("device_type"));
    }

    /// ## 测试过程
    /// `device_type = Some(Cuda)` 时序列化结果应包含 `device_type` 字段。
    #[test]
    fn instance_serde_includes_device_type_when_some() {
        let inst = fake_instance("ns", "comp", "ep", 1);
        let json = serde_json::to_string(&inst).unwrap();
        assert!(json.contains("device_type"));
    }

    /// ## 测试过程
    /// 完整 `Instance` JSON roundtrip，断言所有字段一致。
    #[test]
    fn instance_serde_full_roundtrip() {
        let inst = fake_instance("ns", "comp", "ep", 7);
        let json = serde_json::to_string(&inst).unwrap();
        let back: Instance = serde_json::from_str(&json).unwrap();
        assert_eq!(inst, back);
    }

    // ------------------------------------------------------------------
    // validate_allowed_chars
    // ------------------------------------------------------------------

    /// ## 测试过程
    /// 合法名字：纯小写、含 `-`、含 `_`、含数字混合。全部应通过。
    #[test]
    fn validate_allowed_chars_accepts_valid() {
        for name in ["lowercase", "with-dash", "with_underscore", "abc123", "a"] {
            assert!(
                validate_allowed_chars(name).is_ok(),
                "'{name}' should be valid"
            );
        }
    }

    /// ## 测试过程
    /// 非法名字：含大写、空格、点号、斜杠、以及空串。全部应被拒绝。
    ///
    /// ## 意义
    /// 防止非法字符进入 NATS subject / etcd key，避免下游路径解析
    /// 异常。
    #[test]
    fn validate_allowed_chars_rejects_invalid() {
        for name in ["UPPER", "with space", "dot.name", "slash/name", ""] {
            assert!(
                validate_allowed_chars(name).is_err(),
                "'{name}' should be invalid"
            );
        }
    }
}
