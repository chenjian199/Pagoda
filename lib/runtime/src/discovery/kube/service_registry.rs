// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 原生 Kubernetes Service / EndpointSlice 注册构建层
//!
//! ## 职责
//!
//! 本模块是**纯构建层**，只负责将注册参数转换为合法的 K8s 对象结构体，
//! 不含业务语义，不持有 K8s 客户端，不产生任何副作用。
//! 对象的持久化由 [`apply_service`] / [`apply_endpoint_slice`] 完成；
//! 业务组装由上层 `objects.rs` 完成。
//!
//! ## 关键设计
//!
//! | 特性 | 说明 |
//! |------|------|
//! | Builder 模式 | [`Registration::builder`] 分阶段填充，防止参数顺序错误 |
//! | `EndpointSliceName` newtype | 携带类型信息，避免裸 `String` 被误用 |
//! | 去重 | 不再含 `sanitize_dns_label` / `short_hash`，统一从 [`super::utils`] 引用 |
//! | server-side apply | 所有写操作幂等，无需先 list 再判断 create/update |
//!
//! ## Label 最少原则
//!
//! - `EndpointSlice` 必须携带 [`SERVICE_NAME_LABEL`]（K8s 标准，kube-proxy 依赖）
//! - `EndpointSlice` 携带 [`MANAGED_BY_LABEL`] 隔离 Pagoda 自管理 slice
//! - 可选 [`REGISTRY_MODE_LABEL`] 进一步缩小查询范围

use std::collections::BTreeMap;
use std::fmt;

use anyhow::{Result, bail};
use k8s_openapi::api::core::v1::{ObjectReference, Service, ServicePort, ServiceSpec};
use k8s_openapi::api::discovery::v1::{
    Endpoint as K8sEndpoint, EndpointConditions, EndpointPort, EndpointSlice,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kube::{
    Api, Client as KubeClient,
    api::{DeleteParams, Patch, PatchParams},
};

use super::utils::{sanitize, short_hash};

// ─── 常量 ────────────────────────────────────────────────────────────────────

/// server-side apply 时声明的 field manager 名称，标识字段所有权归属。
///
/// 同一 field manager 多次 apply 是幂等的（patch 不存在则创建，存在则覆盖自己的字段）。
const FIELD_MANAGER: &str = "pagoda-worker-native";

/// K8s 标准 label：将 EndpointSlice 关联到 Service。
///
/// kube-proxy 和 DNS 控制面通过此 label 发现某个 Service 的所有 EndpointSlice，
/// **缺少此 label 将导致流量路由和服务发现完全失效**，不可省略。
pub const SERVICE_NAME_LABEL: &str = "kubernetes.io/service-name";

/// K8s 标准 label：声明 EndpointSlice 的管理控制器。
///
/// 用于将 Pagoda 自管理的 slice 与 K8s 内置 EndpointSlice 控制器生成的 slice 区分开，
/// 防止内置控制器意外删除或覆盖 Pagoda 的 slice。
pub const MANAGED_BY_LABEL: &str = "endpointslice.kubernetes.io/managed-by";

/// [`MANAGED_BY_LABEL`] 的具体值，标识 Pagoda worker 进程是此 slice 的管理者。
pub const MANAGED_BY_VALUE: &str = "pagoda-worker";

/// 可选的 Pagoda 发现模式 label，区分原生 Service 模式与旧 CRD 模式的对象。
///
/// 在 list/watch 时作为额外过滤条件，避免处理不属于原生模式的遗留对象。
pub const REGISTRY_MODE_LABEL: &str = "bedicloud.com/pagoda-discovery-mode";

/// [`REGISTRY_MODE_LABEL`] 的具体值，标识此对象属于原生 Service 注册模式。
pub const REGISTRY_MODE_VALUE: &str = "native-service";

// ─── EndpointSliceName newtype ────────────────────────────────────────────────

/// `EndpointSlice` 的确定性名称，携带类型信息避免裸 `String` 被误用。
///
/// ## 命名格式
///
/// `pag-<service>-<port>-<hash8>`
///
/// - `<service>`：Service 名称规范化后的前 20 字符
/// - `<port>`：端口名称规范化后的前 15 字符
/// - `<hash8>`：`"<service_name>/<portname>/<pod_name>"` 的 32-bit hash
///
/// ## 确定性保证
///
/// 相同的 `(service_name, portname, pod_name)` 三元组永远产生相同名称，
/// 使 server-side apply 可以幂等地 upsert，无需先查询再决定 create/update。
///
/// ## 长度保证
///
/// 格式设计确保总长度 ≤ 63 字符（K8s DNS label 上限）：
/// - `"pag-"` = 4
/// - service（≤20） + `"-"` = 21
/// - port（≤15） + `"-"` = 16
/// - hash = 8
/// - 合计 ≤ 49 < 63 ✓
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EndpointSliceName(String);

impl EndpointSliceName {
    /// 从三元组计算确定性名称。
    ///
    /// # 参数
    /// - `service_name`：K8s Service 名称（原始值，内部会规范化）
    /// - `portname`：端口名称（对应 Pagoda portname 字符串）
    /// - `pod_name`：Pod 名称（参与 hash 计算，区分同 service 的多个 Pod）
    pub fn new(service_name: &str, portname: &str, pod_name: &str) -> Self {
        let svc = sanitize(service_name, 20);
        let port = sanitize(portname, 15);
        let digest = short_hash(&format!("{service_name}/{portname}/{pod_name}"));
        Self(format!("pag-{svc}-{port}-{digest}"))
    }

    /// 返回内部字符串的引用。
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// `EndpointSliceName` 实现 `Display`，可直接插入 `format!("{}", name)` 而无需 `.as_str()`。
impl fmt::Display for EndpointSliceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// 允许将 `EndpointSliceName` 直接用在需要 `String` 的地方（如 K8s metadata.name）。
impl From<EndpointSliceName> for String {
    fn from(name: EndpointSliceName) -> Self {
        name.0
    }
}

// ─── RegistrationBuilder ──────────────────────────────────────────────────────

/// [`Registration`] 的分阶段构建器。
///
/// ## 使用示例
///
/// ```rust
/// let reg = Registration::builder("svc-name", "grpc", 8080, "pod-0", "uid-abc", "10.0.0.1")
///     .hostname("pod-0")
///     .app_protocol("grpc")
///     .service_annotation("bedicloud.com/pagoda-namespace", "prod")
///     .build()
///     .unwrap();
/// ```
///
/// ## 设计意图
///
/// 原版 `NativeServiceRegistration::new` 接受 6 个位置参数，容易因顺序混淆产生 bug。
/// Builder 模式通过命名方法明确每个参数的语义，并在 `build()` 统一校验。
#[derive(Debug)]
pub struct RegistrationBuilder {
    /// K8s Service 名称（必填）
    service_name: String,
    /// 端口名称，即 Pagoda portname 字符串（必填）
    portname: String,
    /// 端口号，1-65535（必填）
    port: i32,
    /// Pod 名称（必填，用于 ownerReference 和 EndpointSlice targetRef）
    pod_name: String,
    /// Pod UID（必填，用于 ownerReference 精确定位）
    pod_uid: String,
    /// Pod IP（必填，写入 EndpointSlice addresses）
    pod_ip: String,
    /// 可选：EndpointSlice 的 DNS hostname
    hostname: Option<String>,
    /// 传输协议，默认 `"TCP"`
    protocol: String,
    /// 可选：应用层协议（如 `"grpc"`），供 service mesh 感知路由使用
    app_protocol: Option<String>,
    /// 是否创建 headless Service（`clusterIP=None`），默认 `true`
    headless: bool,
    /// Service 额外 labels（如 Pagoda namespace/servicegroup 信息）
    service_labels: BTreeMap<String, String>,
    /// Service 额外 annotations（如 Pagoda 逻辑路径信息）
    service_annotations: BTreeMap<String, String>,
    /// EndpointSlice 额外 labels
    endpoint_slice_labels: BTreeMap<String, String>,
    /// EndpointSlice 额外 annotations（如 transport JSON、portname 路径）
    endpoint_slice_annotations: BTreeMap<String, String>,
}

impl RegistrationBuilder {
    /// 设置 DNS hostname（写入 `portname.hostname`）。
    pub fn hostname(mut self, hostname: impl Into<String>) -> Self {
        self.hostname = Some(hostname.into());
        self
    }

    /// 设置应用层协议（写入 Service port 和 EndpointSlice port 的 `appProtocol`）。
    pub fn app_protocol(mut self, proto: impl Into<String>) -> Self {
        self.app_protocol = Some(proto.into());
        self
    }

    /// 设置传输层协议（默认 `"TCP"`，可改为 `"UDP"` 或 `"SCTP"`）。
    pub fn protocol(mut self, protocol: impl Into<String>) -> Self {
        self.protocol = protocol.into();
        self
    }

    /// 设置是否创建 headless Service（默认 `true`，Pagoda 标准模式）。
    pub fn headless(mut self, headless: bool) -> Self {
        self.headless = headless;
        self
    }

    /// 向 Service 追加一个 label。
    pub fn service_label(mut self, key: impl Into<String>, val: impl Into<String>) -> Self {
        self.service_labels.insert(key.into(), val.into());
        self
    }

    /// 向 Service 追加一个 annotation。
    pub fn service_annotation(mut self, key: impl Into<String>, val: impl Into<String>) -> Self {
        self.service_annotations.insert(key.into(), val.into());
        self
    }

    /// 向 EndpointSlice 追加一个 label。
    pub fn portname_label(mut self, key: impl Into<String>, val: impl Into<String>) -> Self {
        self.endpoint_slice_labels.insert(key.into(), val.into());
        self
    }

    /// 向 EndpointSlice 追加一个 annotation。
    pub fn portname_annotation(mut self, key: impl Into<String>, val: impl Into<String>) -> Self {
        self.endpoint_slice_annotations.insert(key.into(), val.into());
        self
    }

    /// 构建 [`Registration`]，同时执行全字段校验。
    ///
    /// # 返回
    /// 所有必填字段合法时返回 `Ok(Registration)`，否则返回描述问题的错误。
    pub fn build(self) -> Result<Registration> {
        validate_registration(&self.service_name, &self.portname, &self.pod_name,
                              &self.pod_uid, &self.pod_ip, self.port)?;
        Ok(Registration {
            service_name: self.service_name,
            portname: self.portname,
            port: self.port,
            pod_name: self.pod_name,
            pod_uid: self.pod_uid,
            pod_ip: self.pod_ip,
            hostname: self.hostname,
            protocol: self.protocol,
            app_protocol: self.app_protocol,
            headless: self.headless,
            service_labels: self.service_labels,
            service_annotations: self.service_annotations,
            endpoint_slice_labels: self.endpoint_slice_labels,
            endpoint_slice_annotations: self.endpoint_slice_annotations,
        })
    }
}

// ─── Registration ─────────────────────────────────────────────────────────────

/// 注册一个 Pagoda portname 实例所需的全部参数，构建后不可变。
///
/// ## 创建方式
///
/// 通过 [`Registration::builder`] 获得 [`RegistrationBuilder`]，
/// 填充可选字段后调用 [`RegistrationBuilder::build`] 得到此结构体。
///
/// 之后调用：
/// - [`build_service`] 生成 [`Service`] 对象
/// - [`build_endpoint_slice`] 生成 [`EndpointSlice`] 对象
/// - [`apply_service`] / [`apply_endpoint_slice`] 持久化到 K8s
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Registration {
    /// 对应 servicegroup 的 K8s Service 名称（由 [`servicegroup_service_name`] 生成）。
    pub service_name: String,
    /// 端口名称，对应 Pagoda portname 字符串（如 `"grpc"`、`"http"`）。
    pub portname: String,
    /// 端口号（1-65535）。
    pub port: i32,
    /// Pod 名称，用于 ownerReference 和 EndpointSlice targetRef。
    pub pod_name: String,
    /// Pod UID，用于 ownerReference 精确定位（防止同名 Pod 重建后 GC 误操作）。
    pub pod_uid: String,
    /// Pod IP，写入 EndpointSlice `portname.addresses`。
    pub pod_ip: String,
    /// 可选的 DNS hostname，写入 EndpointSlice `portname.hostname`。
    pub hostname: Option<String>,
    /// 传输层协议，通常为 `"TCP"`。
    pub protocol: String,
    /// 可选的应用层协议（如 `"grpc"`），供服务网格感知路由使用。
    pub app_protocol: Option<String>,
    /// 是否创建 headless Service（`clusterIP=None`）。
    ///
    /// Pagoda 默认 `true`：headless 模式下 K8s 不分配虚拟 IP，
    /// 客户端通过直连 Pod IP 通信，适合点对点路由模型。
    pub headless: bool,
    /// Service 额外 labels。
    pub service_labels: BTreeMap<String, String>,
    /// Service 额外 annotations。
    pub service_annotations: BTreeMap<String, String>,
    /// EndpointSlice 额外 labels。
    pub endpoint_slice_labels: BTreeMap<String, String>,
    /// EndpointSlice 额外 annotations。
    pub endpoint_slice_annotations: BTreeMap<String, String>,
}

impl Registration {
    /// 创建 [`RegistrationBuilder`]，开始链式配置。
    ///
    /// # 参数（均为必填）
    /// - `service_name`：K8s Service 名称（通常由 [`servicegroup_service_name`] 生成）
    /// - `portname`：端口名称（Pagoda portname 字符串）
    /// - `port`：端口号
    /// - `pod_name`：Pod 名称
    /// - `pod_uid`：Pod UID
    /// - `pod_ip`：Pod IP
    pub fn builder(
        service_name: impl Into<String>,
        portname: impl Into<String>,
        port: i32,
        pod_name: impl Into<String>,
        pod_uid: impl Into<String>,
        pod_ip: impl Into<String>,
    ) -> RegistrationBuilder {
        RegistrationBuilder {
            service_name: service_name.into(),
            portname: portname.into(),
            port,
            pod_name: pod_name.into(),
            pod_uid: pod_uid.into(),
            pod_ip: pod_ip.into(),
            hostname: None,
            protocol: "TCP".into(),
            app_protocol: None,
            headless: true,
            service_labels: BTreeMap::new(),
            service_annotations: BTreeMap::new(),
            endpoint_slice_labels: BTreeMap::new(),
            endpoint_slice_annotations: BTreeMap::new(),
        }
    }

    /// 计算并返回此注册参数对应的 EndpointSlice 名称。
    ///
    /// 名称由 `(service_name, portname, pod_name)` 三元组确定性生成，
    /// 确保 server-side apply 可以幂等地 upsert 同一 slice。
    pub fn endpoint_slice_name(&self) -> EndpointSliceName {
        EndpointSliceName::new(&self.service_name, &self.portname, &self.pod_name)
    }
}

// ─── 对象构建函数 ─────────────────────────────────────────────────────────────

/// 构建原生 Kubernetes `Service` 对象（不发送到 API server）。
///
/// ## 设计意图
///
/// 将 Pagoda servicegroup 映射为一个 K8s Service：
/// - 同一 servicegroup 下所有 portname 共享此 Service
/// - 每个 portname 对应 Service 的一个具名端口（`spec.ports[i].name = portname`）
/// - `headless=true`（`clusterIP=None`）时 K8s 不分配虚拟 IP，适合直连路由
///
/// ## 处理过程
///
/// 1. 合并 [`REGISTRY_MODE_LABEL`] 与调用方指定的额外 labels
/// 2. 构建 `ServiceSpec`：`clusterIP` 根据 `headless` 标志设置
/// 3. 返回构建好的 `Service`（尚未持久化）
///
/// # 参数
/// - `reg`：已通过 `build()` 校验的 [`Registration`]
///
/// # 返回
/// 构建好的 `Service` 对象
pub fn build_service(reg: &Registration) -> Service {
    // labels：registry-mode 标准 label + 调用方附加 labels
    let labels: BTreeMap<String, String> = std::iter::once((
        REGISTRY_MODE_LABEL.to_owned(),
        REGISTRY_MODE_VALUE.to_owned(),
    ))
    .chain(reg.service_labels.clone())
    .collect();

    Service {
        metadata: ObjectMeta {
            name: Some(reg.service_name.clone()),
            labels: Some(labels),
            annotations: nonempty_map(reg.service_annotations.clone()),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            // headless：clusterIP="None"；非 headless：omit（K8s 自动分配）
            cluster_ip: reg.headless.then(|| "None".to_owned()),
            ports: Some(vec![ServicePort {
                name: Some(reg.portname.clone()),
                port: reg.port,
                protocol: Some(reg.protocol.clone()),
                app_protocol: reg.app_protocol.clone(),
                ..Default::default()
            }]),
            publish_not_ready_addresses: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// 构建由 Pod 所有（ownerReference）的 `EndpointSlice`，并绑定到指定 Service。
///
/// ## 设计意图
///
/// 每个 `(servicegroup, portname, pod)` 三元组对应一个独立的 EndpointSlice。
/// `ownerReference → Pod` 确保 Pod 删除时 GC 自动清理 slice，
/// 避免出现无对应 Pod 的僵尸端点影响路由。
///
/// ## 处理过程
///
/// 1. 组装标准 labels（`service-name`、`managed-by`、`registry-mode`）
/// 2. 合并调用方额外 labels / annotations
/// 3. 设置 `ownerReference → Pod`（`controller=true`，`blockOwnerDeletion=false`）
/// 4. 组装 portnames 列表（单 Pod IP，`ready=true`，`serving=true`）
/// 5. 组装 ports 列表（对应 portname 的具名端口）
///
/// # 参数
/// - `reg`：已通过 `build()` 校验的 [`Registration`]
///
/// # 返回
/// 构建好的 `EndpointSlice` 对象（尚未持久化）
pub fn build_endpoint_slice(reg: &Registration) -> EndpointSlice {
    // labels：3 个标准 label + 调用方附加 labels
    let labels: BTreeMap<String, String> = [
        (SERVICE_NAME_LABEL.to_owned(), reg.service_name.clone()),
        (MANAGED_BY_LABEL.to_owned(), MANAGED_BY_VALUE.to_owned()),
        (REGISTRY_MODE_LABEL.to_owned(), REGISTRY_MODE_VALUE.to_owned()),
    ]
    .into_iter()
    .chain(reg.endpoint_slice_labels.clone())
    .collect();

    EndpointSlice {
        metadata: ObjectMeta {
            name: Some(reg.endpoint_slice_name().into()),
            labels: Some(labels),
            annotations: nonempty_map(reg.endpoint_slice_annotations.clone()),
            owner_references: Some(vec![pod_owner_ref(reg)]),
            ..Default::default()
        },
        address_type: "IPv4".to_owned(),
        endpoints: vec![K8sEndpoint {
            addresses: vec![reg.pod_ip.clone()],
            conditions: Some(EndpointConditions {
                ready: Some(true),
                serving: Some(true),
                terminating: Some(false),
            }),
            hostname: reg.hostname.clone(),
            target_ref: Some(ObjectReference {
                api_version: Some("v1".to_owned()),
                kind: Some("Pod".to_owned()),
                name: Some(reg.pod_name.clone()),
                uid: Some(reg.pod_uid.clone()),
                ..Default::default()
            }),
            ..Default::default()
        }],
        ports: Some(vec![EndpointPort {
            name: Some(reg.portname.clone()),
            port: Some(reg.port),
            protocol: Some(reg.protocol.clone()),
            app_protocol: reg.app_protocol.clone(),
        }]),
    }
}

// ─── K8s API 操作 ────────────────────────────────────────────────────────────

/// 使用 server-side apply 创建或更新 `Service`（幂等操作）。
///
/// ## 设计意图
///
/// server-side apply 保证：
/// - Service 不存在 → 创建
/// - Service 已存在 → 合并本 field manager 声明的字段
/// - `force()` 参数确保即使存在 field manager 冲突也能写入
///
/// # 参数
/// - `client`：K8s API 客户端
/// - `namespace`：目标命名空间（Pod 所在 K8s namespace）
/// - `service`：待 apply 的 `Service` 对象（必须有 `metadata.name`）
///
/// # 返回
/// `Ok(())` 表示成功；API 调用失败返回错误
pub async fn apply_service(client: &KubeClient, namespace: &str, service: &Service) -> Result<()> {
    let name = service
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("Service 对象缺少 metadata.name"))?;

    Api::<Service>::namespaced(client.clone(), namespace)
        .patch(name, &PatchParams::apply(FIELD_MANAGER).force(), &Patch::Apply(service))
        .await
        .map(|_| ())
        .map_err(|e| anyhow::anyhow!("apply Service '{name}' 失败: {e}"))
}

/// 使用 server-side apply 创建或更新 `EndpointSlice`（幂等操作）。
///
/// 名称由 [`EndpointSliceName`] 确定性生成，apply 语义等同于 upsert。
///
/// # 参数
/// - `client`：K8s API 客户端
/// - `namespace`：目标命名空间
/// - `slice`：待 apply 的 `EndpointSlice` 对象（必须有 `metadata.name`）
///
/// # 返回
/// `Ok(())` 表示成功；API 调用失败返回错误
pub async fn apply_endpoint_slice(
    client: &KubeClient,
    namespace: &str,
    slice: &EndpointSlice,
) -> Result<()> {
    let name = slice
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("EndpointSlice 对象缺少 metadata.name"))?;

    Api::<EndpointSlice>::namespaced(client.clone(), namespace)
        .patch(name, &PatchParams::apply(FIELD_MANAGER).force(), &Patch::Apply(slice))
        .await
        .map(|_| ())
        .map_err(|e| anyhow::anyhow!("apply EndpointSlice '{name}' 失败: {e}"))
}

/// 删除指定名称的 `EndpointSlice`，对象不存在时静默成功。
///
/// ## 设计意图
///
/// Pod 主动注销某 portname 时调用，触发集群内其他 Pod 的 watch 立刻收到 Removed 事件，
/// 比等待 Pod 死亡后 GC 响应更及时。
///
/// # 参数
/// - `client`：K8s API 客户端
/// - `namespace`：目标命名空间
/// - `name`：待删除的 EndpointSlice 名称（通常从 [`EndpointSliceName`] 获取）
///
/// # 返回
/// 删除成功或对象已不存在均返回 `Ok(())`；其他 API 错误返回 `Err`
pub async fn delete_endpoint_slice(client: &KubeClient, namespace: &str, name: &str) -> Result<()> {
    match Api::<EndpointSlice>::namespaced(client.clone(), namespace)
        .delete(name, &DeleteParams::default())
        .await
    {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(e)) if e.code == 404 => Ok(()),
        Err(e) => Err(anyhow::anyhow!("删除 EndpointSlice '{name}' 失败: {e}")),
    }
}

/// 删除指定名称的 `Service`，对象不存在时静默成功。
///
/// ## 设计意图
///
/// 当一个 servicegroup Service 下不再有任何端口注册时，由上层调用删除整个 Service，
/// 避免集群中积累大量只含空 spec.ports 的过期 Service。
///
/// # 参数
/// - `client`：K8s API 客户端
/// - `namespace`：目标命名空间
/// - `name`：待删除的 Service 名称
///
/// # 返回
/// 删除成功或对象已不存在均返回 `Ok(())`；其他 API 错误返回 `Err`
pub async fn delete_service(client: &KubeClient, namespace: &str, name: &str) -> Result<()> {
    match Api::<Service>::namespaced(client.clone(), namespace)
        .delete(name, &DeleteParams::default())
        .await
    {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(e)) if e.code == 404 => Ok(()),
        Err(e) => Err(anyhow::anyhow!("删除 Service '{name}' 失败: {e}")),
    }
}

// ─── 命名辅助 ─────────────────────────────────────────────────────────────────

/// 为 Pagoda servicegroup 生成共享 K8s Service 名称。
///
/// ## 约束
///
/// 同一 servicegroup 下的所有 portname 共用一个 Service，通过端口名区分。
/// 使用 `"pag-comp-"` 前缀 + 规范化后的 servicegroup 名（最长 40 字符）。
///
/// # 参数
/// - `servicegroup`：Pagoda servicegroup 字符串（可含任意字符）
///
/// # 返回
/// 符合 K8s Service 命名规范的字符串
pub fn servicegroup_service_name(servicegroup: &str) -> String {
    format!("pag-comp-{}", sanitize(servicegroup, 40))
}

// ─── 内部辅助 ─────────────────────────────────────────────────────────────────

/// 构造指向 Pod 的 `OwnerReference`，用于 K8s GC 自动级联删除。
///
/// - `controller=true`：标记本 Pod 是此对象的控制器
/// - `blockOwnerDeletion=false`：允许 Pod 在等待 slice 删除时直接退出，不阻塞
fn pod_owner_ref(reg: &Registration) -> OwnerReference {
    OwnerReference {
        api_version: "v1".to_owned(),
        kind: "Pod".to_owned(),
        name: reg.pod_name.clone(),
        uid: reg.pod_uid.clone(),
        controller: Some(true),
        block_owner_deletion: Some(false),
    }
}

/// 若 `BTreeMap` 非空则返回 `Some(map)`，空时返回 `None`。
///
/// K8s API 对 `metadata.labels` / `metadata.annotations` 的惯例是：
/// 空 map 和 `null` 语义相同，但传空 map 会产生多余的 patch 字节，故统一转换为 `None`。
fn nonempty_map(map: BTreeMap<String, String>) -> Option<BTreeMap<String, String>> {
    (!map.is_empty()).then_some(map)
}

/// 在 `RegistrationBuilder::build()` 中集中执行字段合法性校验。
///
/// ## 校验规则
///
/// | 字段 | 规则 |
/// |------|------|
/// | `service_name` | 非空字符串 |
/// | `portname` | 非空字符串 |
/// | `pod_name` | 非空字符串 |
/// | `pod_uid` | 非空字符串 |
/// | `pod_ip` | 非空字符串 |
/// | `port` | 1-65535 |
fn validate_registration(
    service_name: &str, portname: &str, pod_name: &str,
    pod_uid: &str, pod_ip: &str, port: i32,
) -> Result<()> {
    if service_name.trim().is_empty() { bail!("service_name 不能为空"); }
    if portname.trim().is_empty()   { bail!("portname 不能为空"); }
    if pod_name.trim().is_empty()    { bail!("pod_name 不能为空"); }
    if pod_uid.trim().is_empty()     { bail!("pod_uid 不能为空"); }
    if pod_ip.trim().is_empty()      { bail!("pod_ip 不能为空"); }
    if !(1..=65535).contains(&port)  { bail!("port 须在 1-65535，当前值 {port}"); }
    Ok(())
}

// ─── 单元测试 ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// 构造一个通过 Builder 填充了基本字段的 `Registration` 的辅助函数。
    fn base_reg() -> Registration {
        Registration::builder("my-svc", "grpc", 8080, "pod-0", "uid-abc", "10.0.0.1")
            .build()
            .unwrap()
    }

    // ── EndpointSliceName ─────────────────────────────────────────────────────

    /// 相同三元组始终生成相同名称（确定性）。
    #[test]
    fn slice_name_is_deterministic() {
        let a = EndpointSliceName::new("svc", "grpc", "pod-0");
        let b = EndpointSliceName::new("svc", "grpc", "pod-0");
        assert_eq!(a, b);
    }

    /// 不同 pod_name 生成不同名称（三元组唯一性）。
    #[test]
    fn slice_name_differs_by_pod() {
        let a = EndpointSliceName::new("svc", "grpc", "pod-0");
        let b = EndpointSliceName::new("svc", "grpc", "pod-1");
        assert_ne!(a, b);
    }

    /// 名称以 "pag-" 开头。
    #[test]
    fn slice_name_has_prefix() {
        let name = EndpointSliceName::new("svc", "port", "pod");
        assert!(name.as_str().starts_with("pag-"), "名称必须以 pag- 开头");
    }

    /// 名称长度不超过 63 字符（K8s DNS label 上限）。
    #[test]
    fn slice_name_within_63_chars() {
        let name = EndpointSliceName::new(
            "very-long-service-name-that-exceeds-limits",
            "inference-grpc-port",
            "worker-99-abcdef0123456789",
        );
        assert!(
            name.as_str().len() <= 63,
            "名称长度 {} 超过 63 字符限制",
            name.as_str().len()
        );
    }

    /// `Display` trait 输出与 `as_str()` 一致。
    #[test]
    fn slice_name_display() {
        let name = EndpointSliceName::new("svc", "p", "pod");
        assert_eq!(format!("{name}"), name.as_str());
    }

    /// `From<EndpointSliceName> for String` 转换正确。
    #[test]
    fn slice_name_into_string() {
        let name = EndpointSliceName::new("svc", "p", "pod");
        let s: String = name.clone().into();
        assert_eq!(s, name.as_str());
    }

    // ── RegistrationBuilder ───────────────────────────────────────────────────

    /// 必填字段校验：service_name 为空时 build() 返回 Err。
    #[test]
    fn builder_rejects_empty_service_name() {
        let result = Registration::builder("", "grpc", 8080, "pod", "uid", "1.2.3.4")
            .build();
        assert!(result.is_err(), "空 service_name 应返回 Err");
    }

    /// 必填字段校验：port=0 时 build() 返回 Err。
    #[test]
    fn builder_rejects_port_zero() {
        let result = Registration::builder("svc", "grpc", 0, "pod", "uid", "1.2.3.4")
            .build();
        assert!(result.is_err(), "port=0 应返回 Err");
    }

    /// 必填字段校验：port=70000 超出范围时 build() 返回 Err。
    #[test]
    fn builder_rejects_port_out_of_range() {
        let result = Registration::builder("svc", "grpc", 70000, "pod", "uid", "1.2.3.4")
            .build();
        assert!(result.is_err(), "port=70000 应返回 Err");
    }

    /// 合法参数 build() 成功。
    #[test]
    fn builder_accepts_valid_params() {
        assert!(base_reg().port == 8080);
    }

    /// 链式方法正确设置可选字段。
    #[test]
    fn builder_optional_fields() {
        let reg = Registration::builder("svc", "grpc", 8080, "pod-0", "uid", "10.0.0.1")
            .hostname("pod-0")
            .app_protocol("grpc")
            .service_annotation("key", "val")
            .portname_label("lk", "lv")
            .build()
            .unwrap();

        assert_eq!(reg.hostname.as_deref(), Some("pod-0"));
        assert_eq!(reg.app_protocol.as_deref(), Some("grpc"));
        assert_eq!(reg.service_annotations.get("key").map(String::as_str), Some("val"));
        assert_eq!(reg.endpoint_slice_labels.get("lk").map(String::as_str), Some("lv"));
    }

    // ── build_service ────────────────────────────────────────────────────────

    /// headless=true 时 clusterIP 必须为 "None"。
    #[test]
    fn build_service_headless() {
        let svc = build_service(&base_reg());
        assert_eq!(svc.spec.unwrap().cluster_ip.as_deref(), Some("None"));
    }

    /// Service metadata.name 与 registration.service_name 一致。
    #[test]
    fn build_service_name() {
        let svc = build_service(&base_reg());
        assert_eq!(svc.metadata.name.as_deref(), Some("my-svc"));
    }

    /// Service spec.ports 中存在对应端口定义。
    #[test]
    fn build_service_port() {
        let svc = build_service(&base_reg());
        let ports = svc.spec.unwrap().ports.unwrap();
        assert_eq!(ports.len(), 1);
        assert_eq!(ports[0].name.as_deref(), Some("grpc"));
        assert_eq!(ports[0].port, 8080);
    }

    /// Service labels 包含 registry-mode label。
    #[test]
    fn build_service_has_registry_mode_label() {
        let svc = build_service(&base_reg());
        let labels = svc.metadata.labels.unwrap();
        assert_eq!(labels.get(REGISTRY_MODE_LABEL).map(String::as_str), Some(REGISTRY_MODE_VALUE));
    }

    /// headless=false 时 clusterIP 为 None（由 K8s 自动分配）。
    #[test]
    fn build_service_non_headless_no_cluster_ip() {
        let reg = Registration::builder("svc", "p", 80, "pod", "uid", "1.2.3.4")
            .headless(false)
            .build()
            .unwrap();
        let svc = build_service(&reg);
        assert!(svc.spec.unwrap().cluster_ip.is_none(), "非 headless 时不应设置 clusterIP");
    }

    // ── build_endpoint_slice ─────────────────────────────────────────────────

    /// EndpointSlice 包含必要的标准 labels。
    #[test]
    fn build_endpoint_slice_has_standard_labels() {
        let slice = build_endpoint_slice(&base_reg());
        let labels = slice.metadata.labels.unwrap();
        assert_eq!(labels.get(SERVICE_NAME_LABEL).map(String::as_str), Some("my-svc"));
        assert_eq!(labels.get(MANAGED_BY_LABEL).map(String::as_str), Some(MANAGED_BY_VALUE));
        assert_eq!(labels.get(REGISTRY_MODE_LABEL).map(String::as_str), Some(REGISTRY_MODE_VALUE));
    }

    /// EndpointSlice ownerReference 指向正确的 Pod（UID 和 name 均匹配）。
    #[test]
    fn build_endpoint_slice_owner_ref() {
        let slice = build_endpoint_slice(&base_reg());
        let owners = slice.metadata.owner_references.unwrap();
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].name, "pod-0");
        assert_eq!(owners[0].uid, "uid-abc");
        assert_eq!(owners[0].kind, "Pod");
    }

    /// EndpointSlice portname.addresses 包含 Pod IP。
    #[test]
    fn build_endpoint_slice_address() {
        let slice = build_endpoint_slice(&base_reg());
        assert_eq!(slice.endpoints[0].addresses, vec!["10.0.0.1".to_owned()]);
    }

    /// EndpointSlice portname 的就绪状态全部为 true。
    #[test]
    fn build_endpoint_slice_ready_conditions() {
        let slice = build_endpoint_slice(&base_reg());
        let cond = slice.endpoints[0].conditions.as_ref().unwrap();
        assert_eq!(cond.ready, Some(true));
        assert_eq!(cond.serving, Some(true));
        assert_eq!(cond.terminating, Some(false));
    }

    /// 带 hostname 的注册参数正确设置 portname.hostname。
    #[test]
    fn build_endpoint_slice_with_hostname() {
        let reg = Registration::builder("svc", "grpc", 8080, "pod-0", "uid", "10.0.0.1")
            .hostname("pod-0.svc.local")
            .build()
            .unwrap();
        let slice = build_endpoint_slice(&reg);
        assert_eq!(slice.endpoints[0].hostname.as_deref(), Some("pod-0.svc.local"));
    }

    // ── servicegroup_service_name ───────────────────────────────────────────────

    /// 相同 servicegroup 产生相同 Service 名称（幂等性，多 Pod 共享同一 Service）。
    #[test]
    fn servicegroup_service_name_idempotent() {
        assert_eq!(servicegroup_service_name("planner"), servicegroup_service_name("planner"));
    }

    /// 不同 servicegroup 产生不同 Service 名称。
    #[test]
    fn servicegroup_service_name_differs() {
        assert_ne!(servicegroup_service_name("a"), servicegroup_service_name("b"));
    }

    /// 名称以 "pag-comp-" 开头。
    #[test]
    fn servicegroup_service_name_prefix() {
        assert!(servicegroup_service_name("test").starts_with("pag-comp-"));
    }
}
