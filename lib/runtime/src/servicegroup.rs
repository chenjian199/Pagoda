// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 服务模型层：Pagoda 新三段式 Namespace → ServiceGroup → PortName。
//!
//! 调用链全同步、网络动作全懒加载。

pub mod client;
pub mod namespace;
pub mod portname;
pub mod registry;
pub mod service;
mod servicegroup_impl;

use std::sync::Arc;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::distributed::DistributedRuntime;
use crate::metrics::MetricsRegistry;
use crate::traits::{DistributedRuntimeProvider, RuntimeProvider};

// ── re-export ──
pub use client::Client;
pub use registry::Registry;

/// 服务可达地址枚举。
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TransportType {
    /// NATS subject: `"{namespace}.{servicegroup}.{portname}.{instance_id}"`
    Nats(String),
    /// HTTP URL: `"http://{host}:{port}/v1/rpc/{portname}"`
    Http(String),
    /// TCP address: `"{host}:{port}/{instance_id_hex}/{portname}"`
    Tcp(String),
}

/// 一个活跃 PortName 实例的完整描述。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Instance {
    pub namespace: String,
    pub servicegroup: String,
    pub portname: String,
    pub instance_id: u64,
    pub transport: TransportType,
    /// 实例拓扑属性（NUMA/rack 信息），供拓扑感知路由使用。
    /// 空 JSON 对象 `{}` 表示无拓扑信息。
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub topo_json: serde_json::Value,
}

impl Instance {
    pub fn id(&self) -> u64 {
        self.instance_id
    }

    pub fn portname_id(&self) -> PortNameId {
        PortNameId {
            namespace: self.namespace.clone(),
            servicegroup: self.servicegroup.clone(),
            portname: self.portname.clone(),
        }
    }
}

impl std::fmt::Display for Instance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}/{}/{}",
            self.namespace, self.servicegroup, self.portname, self.instance_id
        )
    }
}

impl Ord for Instance {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.instance_id.cmp(&other.instance_id)
    }
}

impl PartialOrd for Instance {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// PortName 的三段式标识符。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PortNameId {
    pub namespace: String,
    pub servicegroup: String,
    pub portname: String,
}

impl std::fmt::Display for PortNameId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}/{}",
            self.namespace, self.servicegroup, self.portname
        )
    }
}

/// 命名空间：三段式第一段。
#[derive(Clone, Validate)]
pub struct Namespace {
    runtime: Arc<DistributedRuntime>,
    #[validate(custom(function = "validate_allowed_chars"))]
    name: String,
    parent: Option<Arc<Namespace>>,
    labels: Vec<(String, String)>,
    metrics_registry: MetricsRegistry,
    servicegroup_cache: Arc<DashMap<String, ServiceGroup>>,
}

impl Namespace {
    pub(crate) fn new(drt: DistributedRuntime, name: String) -> anyhow::Result<Self> {
        let ns = Self {
            runtime: Arc::new(drt),
            name: name.clone(),
            parent: None,
            labels: Vec::new(),
            metrics_registry: MetricsRegistry::new(&name),
            servicegroup_cache: Arc::new(DashMap::new()),
        };
        // 校验名称字符集
        ns.validate()
            .map_err(|e| anyhow::anyhow!("Invalid namespace name: {e}"))?;
        Ok(ns)
    }

    /// 幂等获取 ServiceGroup。
    pub fn service_group(&self, name: impl Into<String>) -> anyhow::Result<ServiceGroup> {
        let name = name.into();
        if let Some(sg) = self.servicegroup_cache.get(&name) {
            return Ok(sg.clone());
        }
        let sg = ServiceGroup::new(Arc::clone(&self.runtime), self.clone(), name.clone())?;
        self.servicegroup_cache.insert(name, sg.clone());
        Ok(sg)
    }

    /// 创建子命名空间。
    pub fn namespace(&self, name: impl Into<String>) -> anyhow::Result<Namespace> {
        let name = name.into();
        Ok(Self {
            runtime: Arc::clone(&self.runtime),
            name: name.clone(),
            parent: Some(Arc::new(self.clone())),
            labels: Vec::new(),
            metrics_registry: MetricsRegistry::new(&name),
            servicegroup_cache: Arc::new(DashMap::new()),
        })
    }

    /// 返回全路径名（有 parent 时 "parent.name"）。
    pub fn name(&self) -> String {
        match &self.parent {
            Some(p) => format!("{}.{}", p.name(), self.name),
            None => self.name.clone(),
        }
    }
}

impl std::fmt::Debug for Namespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Namespace")
            .field("name", &self.name)
            .field("parent", &self.parent.as_ref().map(|p| p.name()))
            .finish()
    }
}

impl std::fmt::Display for Namespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl DistributedRuntimeProvider for Namespace {
    fn drt(&self) -> &DistributedRuntime {
        &self.runtime
    }
}

impl RuntimeProvider for Namespace {
    fn rt(&self) -> &crate::runtime::Runtime {
        self.runtime.rt()
    }
}

/// 服务组：三段式第二段。
#[derive(Clone, Validate)]
pub struct ServiceGroup {
    drt: Arc<DistributedRuntime>,
    #[validate(custom(function = "validate_allowed_chars"))]
    name: String,
    labels: Vec<(String, String)>,
    namespace: Namespace,
    metrics_registry: MetricsRegistry,
}

impl ServiceGroup {
    pub(crate) fn new(
        drt: Arc<DistributedRuntime>,
        namespace: Namespace,
        name: String,
    ) -> anyhow::Result<Self> {
        let sg = Self {
            drt,
            name: name.clone(),
            labels: Vec::new(),
            namespace,
            metrics_registry: MetricsRegistry::new(&name),
        };
        sg.validate()
            .map_err(|e| anyhow::anyhow!("Invalid service group name: {e}"))?;
        Ok(sg)
    }

    /// NATS service 名称。
    pub fn service_name(&self) -> String {
        crate::slug::slugify(&format!("{}.{}", self.namespace.name(), self.name))
    }

    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn labels(&self) -> &[(String, String)] {
        &self.labels
    }

    /// 创建 PortName。
    pub fn portname(&self, name: impl Into<String>) -> PortName {
        let name = name.into();
        PortName {
            servicegroup: self.clone(),
            name: name.clone(),
            labels: Vec::new(),
            metrics_registry: MetricsRegistry::new(&name),
        }
    }

    /// 查询此 ServiceGroup 下所有活跃 Instance。
    pub async fn list_instances(&self) -> anyhow::Result<Vec<Instance>> {
        use crate::discovery::DiscoveryInstance;
        use crate::discovery::DiscoveryQuery;
        use crate::traits::DistributedRuntimeProvider;

        let results = self
            .drt()
            .discovery()
            .list(DiscoveryQuery::ServiceGroupPortNames {
                namespace: self.namespace.name(),
                servicegroup: self.name.clone(),
            })
            .await?;

        let instances = results
            .into_iter()
            .filter_map(|di| match di {
                DiscoveryInstance::PortName(inst) => Some(inst),
                _ => None,
            })
            .collect();

        Ok(instances)
    }
}

impl std::fmt::Debug for ServiceGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceGroup")
            .field("name", &self.name)
            .field("namespace", &self.namespace.name())
            .finish()
    }
}

impl std::fmt::Display for ServiceGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.namespace.name(), self.name)
    }
}

impl std::hash::Hash for ServiceGroup {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.namespace.name().hash(state);
        self.name.hash(state);
    }
}

impl PartialEq for ServiceGroup {
    fn eq(&self, other: &Self) -> bool {
        self.namespace.name() == other.namespace.name() && self.name == other.name
    }
}

impl Eq for ServiceGroup {}

impl DistributedRuntimeProvider for ServiceGroup {
    fn drt(&self) -> &DistributedRuntime {
        &self.drt
    }
}

impl RuntimeProvider for ServiceGroup {
    fn rt(&self) -> &crate::runtime::Runtime {
        self.drt.rt()
    }
}

/// 端点：三段式第三段，服务树叶子节点。
#[derive(Debug, Clone)]
pub struct PortName {
    servicegroup: ServiceGroup,
    name: String,
    labels: Vec<(String, String)>,
    metrics_registry: MetricsRegistry,
}

impl PortName {
    pub fn id(&self) -> PortNameId {
        PortNameId {
            namespace: self.servicegroup.namespace().name(),
            servicegroup: self.servicegroup.name().to_string(),
            portname: self.name.clone(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn servicegroup(&self) -> &ServiceGroup {
        &self.servicegroup
    }

    /// 异步构建客户端（订阅发现系统并维护动态实例视图）。
    pub async fn client(&self) -> anyhow::Result<Client> {
        Client::new(self.clone()).await
    }

    /// 返回 PortName 服务端注册配置 Builder。
    pub fn portname_builder(&self) -> portname::PortNameConfigBuilder {
        portname::PortNameConfigBuilder::from_portname(self.clone())
    }

    /// 运行时动态注销实例。
    ///
    /// 注意：通常应通过 [`portname_builder`][Self::portname_builder] 启动时的生命周期
    /// 自动完成注销。此方法适用于手动管理注册生命周期的场景。
    pub async fn unregister_port_instance(&self) -> anyhow::Result<()> {
        tracing::warn!(
            portname = %format!("{}/{}/{}", self.servicegroup.namespace().name(), self.servicegroup.name(), self.name),
            "unregister_port_instance called without instance_id; use portname_builder lifecycle for automatic cleanup"
        );
        Ok(())
    }

    /// 运行时动态重注册实例（使用 NATS 传输，instance_id 自动生成）。
    ///
    /// 注意：推荐使用 [`portname_builder`][Self::portname_builder] 进行完整的服务注册。
    /// 此方法仅在需要轻量重注册（如重连后上报自身存活）时调用。
    pub async fn register_port_instance(&self) -> anyhow::Result<()> {
        use crate::discovery::DiscoverySpec;
        use crate::traits::DistributedRuntimeProvider;
        use crate::servicegroup::TransportType;

        let ns = self.servicegroup.namespace().name();
        let sg = self.servicegroup.name().to_owned();
        let pn = self.name.clone();
        let instance_id = rand::random::<u64>();
        let nats_subject = format!("{ns}.{sg}.{pn}.{instance_id:x}");
        let spec = DiscoverySpec::PortName {
            namespace: ns,
            servicegroup: sg,
            portname: pn,
            transport: TransportType::Nats(nats_subject),
        };
        self.drt().discovery().register(spec).await?;
        Ok(())
    }
}

impl std::hash::Hash for PortName {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.servicegroup.hash(state);
        self.name.hash(state);
    }
}

impl PartialEq for PortName {
    fn eq(&self, other: &Self) -> bool {
        self.servicegroup == other.servicegroup && self.name == other.name
    }
}

impl Eq for PortName {}

impl DistributedRuntimeProvider for PortName {
    fn drt(&self) -> &DistributedRuntime {
        self.servicegroup.drt()
    }
}

impl RuntimeProvider for PortName {
    fn rt(&self) -> &crate::runtime::Runtime {
        self.servicegroup.rt()
    }
}

/// 名称字符集校验：`^[a-z0-9\-_]+$`
fn validate_allowed_chars(input: &str) -> Result<(), validator::ValidationError> {
    let re = regex::Regex::new(r"^[a-z0-9\-_]+$").unwrap();
    if re.is_match(input) {
        Ok(())
    } else {
        Err(validator::ValidationError::new("invalid_characters"))
    }
}
