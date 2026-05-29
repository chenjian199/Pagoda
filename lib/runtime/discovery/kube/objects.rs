// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 原生 Kubernetes 对象的高层读写操作
//!
//! ## 职责
//!
//! 本模块是 **业务组装层**，负责将 Dynamo 的三类发现对象生命周期
//! 映射到原生 K8s 资源的 CRUD 操作：
//!
//! | Dynamo 对象      | K8s 原生对象              | 生命周期管理              |
//! |-----------------|--------------------------|--------------------------|
//! | `Endpoint`      | `Service` + `EndpointSlice` | Pod `ownerRef` → GC    |
//! | `Model`         | `ConfigMap`              | Pod `ownerRef` → GC      |
//! | `EventChannel`  | `Lease`                  | Pod `ownerRef` + TTL     |
//!
//! ## 与 `service_registry` 的分工
//!
//! - `service_registry` = **纯构建层**，负责把参数转换为 K8s struct，不含业务语义
//! - `objects` = **业务组装层**，负责填充 annotations、调用 service_registry、处理合并逻辑
//!
//! ## 关键设计差异（与旧版相比）
//!
//! | 旧版风格 | 新版风格 |
//! |---------|---------|
//! | `match x { Some(v) => v, None => return Ok(None) }` | `?` + `ok_or_else` / `Option::and_then` |
//! | 重复的 `sanitize` / `short_hash` 定义 | 统一从 `super::utils` 引用 |
//! | 分散的 `api.delete` + 404 检查 | `NotFoundOk` trait 扩展，一行调用 |
//! | 直接读 `BTreeMap` | `AnnotationReader` 辅助结构体封装 |

use std::collections::BTreeMap;

use anyhow::{Result, anyhow};
use chrono::Utc;
use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::api::core::v1::{ConfigMap, Service};
use k8s_openapi::api::discovery::v1::EndpointSlice;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta, OwnerReference};
use kube::{
    Api, Client as KubeClient, Resource,
    api::{DeleteParams, Patch, PatchParams},
};

use crate::{
    component::{Instance, TransportType},
    discovery::{DiscoveryInstance, EventTransport},
};

use super::{
    service_registry::{
        EndpointSliceName, MANAGED_BY_LABEL, MANAGED_BY_VALUE, Registration, SERVICE_NAME_LABEL,
        apply_endpoint_slice, apply_service, build_endpoint_slice, build_service,
        component_service_name, delete_endpoint_slice,
    },
    utils::{PodInfo, hash_pod_name, sanitize, short_hash},
};

// ─── 常量 ────────────────────────────────────────────────────────────────────

/// server-side apply 的 field manager 名称，标识字段所有权归属。
const FIELD_MANAGER: &str = "dynamo-worker-native";

/// 标识 Dynamo 对象类型的 label key，用于区分 Endpoint / Model / EventChannel。
///
/// 在同一 namespace 中，Model ConfigMap 和 EventChannel Lease 分别携带不同的
/// `KIND_LABEL` 值，使 watch 可以通过 label selector 精确过滤目标对象类型。
pub const KIND_LABEL: &str = "nvidia.com/dynamo-kind";

/// [`KIND_LABEL`] 的 Model 值：标识此对象是 Dynamo Model ConfigMap。
pub const MODEL_KIND_VALUE: &str = "model";

/// [`KIND_LABEL`] 的 EventChannel 值：标识此对象是 Dynamo EventChannel Lease。
pub const EVENT_CHANNEL_KIND_VALUE: &str = "event-channel";

/// EndpointSlice annotation：Dynamo namespace 路径。
const ANNOTATION_NAMESPACE: &str = "nvidia.com/dynamo-namespace";
/// EndpointSlice annotation：Dynamo component 路径。
const ANNOTATION_COMPONENT: &str = "nvidia.com/dynamo-component";
/// EndpointSlice annotation：Dynamo endpoint 名称。
const ANNOTATION_ENDPOINT: &str = "nvidia.com/dynamo-endpoint";
/// Lease annotation：Dynamo topic 名称。
const ANNOTATION_TOPIC: &str = "nvidia.com/dynamo-topic";
/// EndpointSlice / Lease annotation：序列化的 `TransportType` / `EventTransport` JSON。
const ANNOTATION_TRANSPORT: &str = "nvidia.com/dynamo-transport";

/// ConfigMap `data` 字段中的 namespace key。
const DATA_NAMESPACE: &str = "namespace";
/// ConfigMap `data` 字段中的 component key。
const DATA_COMPONENT: &str = "component";
/// ConfigMap `data` 字段中的 endpoint key。
const DATA_ENDPOINT: &str = "endpoint";
/// ConfigMap `data` 字段中的 instance_id key（16 进制字符串）。
const DATA_INSTANCE_ID: &str = "instance_id";
/// ConfigMap `data` 字段中的 model_suffix key（LoRA adapter 标识）。
const DATA_MODEL_SUFFIX: &str = "model_suffix";
/// ConfigMap `data` 字段中的模型卡 JSON 字段 key。
const DATA_CARD_JSON: &str = "card.json";

// ─── NotFoundOk trait ─────────────────────────────────────────────────────────

/// 为 `Result<_, kube::Error>` 添加 `.not_found_ok()` 扩展，将 404 转换为 `Ok(())`。
///
/// ## 设计意图
///
/// 删除 K8s 对象时，对象已不存在（404）是合法状态，不应作为错误上报。
/// 原版在每个删除函数中各写一遍 `match ... 404 => Ok(())` 的模式，
/// 通过此 trait 将该逻辑提取为单一调用点，实现**去重**。
///
/// ## 示例
///
/// ```rust
/// api.delete("foo", &DeleteParams::default()).await.not_found_ok()?;
/// ```
trait NotFoundOk {
    /// 将 404 错误转换为 `Ok(())`，其余错误保持原样。
    fn not_found_ok(self) -> Result<()>;
}

/// 为任意带有 `kube::Error` 的 `Result<T>` 实现 `NotFoundOk`。
impl<T> NotFoundOk for Result<T, kube::Error> {
    fn not_found_ok(self) -> Result<()> {
        match self {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(e)) if e.code == 404 => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

// ─── AnnotationReader ────────────────────────────────────────────────────────

/// K8s 对象 annotations 的只读访问辅助结构体。
///
/// ## 设计意图
///
/// 原版在每次解析 `EndpointSlice`、`Lease` 时都重复执行
/// `let annotations = match meta.annotations { Some(v) => v, None => return Ok(None) };`
/// 然后再 `annotations.get(KEY).ok_or_else(...)` 的两段式代码。
///
/// `AnnotationReader` 将第一步（unwrap annotations map）封装到构造函数中，
/// 使后续的每次字段读取只需一行 `.get_required(KEY)?` 或 `.get_optional(KEY)` 即可。
///
/// ## 生命周期
///
/// `'a` 绑定到被借用的 `BTreeMap` 的生命周期，不产生任何拷贝。
struct AnnotationReader<'a> {
    /// 被借用的 annotations map 引用（若原始对象无 annotations 则为 `None`）
    inner: Option<&'a BTreeMap<String, String>>,
}

impl<'a> AnnotationReader<'a> {
    /// 从 `Option<&BTreeMap<...>>` 构造 reader。
    ///
    /// `inner = None` 时不会 panic，只是所有读取操作均返回 `None` / `Err`。
    fn new(annotations: Option<&'a BTreeMap<String, String>>) -> Self {
        Self { inner: annotations }
    }

    /// 读取必填 annotation，缺少时返回带 key 名称的 `Err`。
    ///
    /// # 参数
    /// - `key`：annotation key 字符串
    ///
    /// # 返回
    /// 成功读取时返回 `Ok(String)`（拷贝 value）；key 不存在或 map 为 None 时返回 `Err`
    fn required(&self, key: &str) -> Result<String> {
        self.inner
            .and_then(|m| m.get(key))
            .cloned()
            .ok_or_else(|| anyhow!("annotation '{}' 缺失", key))
    }

    /// 读取可选 annotation，不存在时返回 `None`。
    fn optional(&self, key: &str) -> Option<String> {
        self.inner.and_then(|m| m.get(key)).cloned()
    }
}

// ─── Endpoint 注册 / 注销 ────────────────────────────────────────────────────

/// 在 Kubernetes 中注册一个 endpoint 实例（`Service` + `EndpointSlice`）。
///
/// ## 设计意图
///
/// 将 Dynamo endpoint 实例映射为 K8s `Service`（共享） + `EndpointSlice`（Per-Pod）对。
/// 同一 component 的所有 endpoint 共享一个 Service（多端口），
/// 每个 `(component, endpoint, pod)` 三元组对应一个独立的 EndpointSlice。
///
/// ## 处理过程
///
/// 1. 从 `instance.transport` 推断端口号
/// 2. 用 [`Registration::builder`] 链式填充参数（Dynamo annotations 作为额外字段）
/// 3. [`apply_or_merge_endpoint_service`]：create-or-merge Service（合并已有端口）
/// 4. [`apply_endpoint_slice`]：幂等 upsert EndpointSlice
///
/// # 参数
/// - `kube_client`：K8s API 客户端
/// - `pod_info`：注册方 Pod 的身份信息
/// - `instance`：待注册的 Dynamo endpoint 实例
pub async fn register_endpoint_instance(
    kube_client: &KubeClient,
    pod_info: &PodInfo,
    instance: &Instance,
) -> Result<()> {
    let port = transport_port_hint(&instance.transport, pod_info.system_port as i32);

    let reg = Registration::builder(
        component_service_name(&instance.component),
        &instance.endpoint,
        port,
        &pod_info.pod_name,
        &pod_info.pod_uid,
        &pod_info.pod_ip,
    )
    .service_annotation(ANNOTATION_NAMESPACE, &instance.namespace)
    .service_annotation(ANNOTATION_COMPONENT, &instance.component)
    .endpoint_annotation(ANNOTATION_NAMESPACE, &instance.namespace)
    .endpoint_annotation(ANNOTATION_COMPONENT, &instance.component)
    .endpoint_annotation(ANNOTATION_ENDPOINT, &instance.endpoint)
    .endpoint_annotation(ANNOTATION_TRANSPORT, serde_json::to_string(&instance.transport)?)
    .build()?;

    let service = build_service(&reg);
    apply_or_merge_endpoint_service(kube_client, &instance.namespace, &service).await?;

    let slice = build_endpoint_slice(&reg);
    apply_endpoint_slice(kube_client, &instance.namespace, &slice).await
}

/// 从 Kubernetes 中注销一个 endpoint 实例。
///
/// ## 设计意图
///
/// 主动注销时立即删除 EndpointSlice，触发 watch 的 Removed 事件，
/// 比等待 Pod 死亡后 GC 响应更及时。
/// 同时清理 Service 中该 endpoint 对应的端口，若 Service 无剩余端口则删除 Service。
///
/// # 参数
/// - `kube_client`：K8s API 客户端
/// - `pod_name`：注销方 Pod 名称（用于计算 EndpointSlice 名称）
/// - `namespace`：Dynamo namespace 字符串
/// - `component`：Dynamo component 字符串
/// - `endpoint`：Dynamo endpoint 字符串
pub async fn unregister_endpoint_instance(
    kube_client: &KubeClient,
    pod_name: &str,
    namespace: &str,
    component: &str,
    endpoint: &str,
) -> Result<()> {
    let service_name = component_service_name(component);
    let slice_name = EndpointSliceName::new(&service_name, endpoint, pod_name);
    delete_endpoint_slice(kube_client, namespace, slice_name.as_str()).await?;
    cleanup_endpoint_service_port(
        kube_client, namespace, &service_name, endpoint, slice_name.as_str(),
    )
    .await
}

/// 从 `Service` + `EndpointSlice` 恢复一个 [`DiscoveryInstance::Endpoint`]。
///
/// ## 设计意图
///
/// daemon 在聚合快照时，从 `EndpointSlice`（含 transport）+ `Service`（含 namespace/component）
/// 联合恢复完整的 `DiscoveryInstance`。
///
/// 使用 [`AnnotationReader`] 消除重复的 `match ... return Ok(None)` 模式。
///
/// ## 处理过程
///
/// 1. 从 Service annotations 读取 `namespace`、`component`
/// 2. 从 EndpointSlice annotations 读取 `endpoint`、`transport`
/// 3. 验证 EndpointSlice 的 `SERVICE_NAME_LABEL` 和 `MANAGED_BY_LABEL`
/// 4. 验证 Service 端口定义与 EndpointSlice 端口定义一致
/// 5. 从 EndpointSlice `targetRef` 提取 `pod_name`，计算 `instance_id`
///
/// # 返回
/// 恢复成功 → `Ok(Some(instance))`；任何必需字段缺失 → `Ok(None)`；解析错误 → `Err`
pub fn endpoint_instance_from_service_and_slice(
    service: &Service,
    slice: &EndpointSlice,
) -> Result<Option<DiscoveryInstance>> {
    // ── 1. 从 Service 提取元数据 ────────────────────────────────────────────
    let svc_meta = &service.metadata;
    let svc_ann = AnnotationReader::new(svc_meta.annotations.as_ref());
    let service_name = match svc_meta.name.as_deref() {
        Some(n) => n,
        None => return Ok(None),
    };

    let namespace = match svc_ann.optional(ANNOTATION_NAMESPACE) {
        Some(v) => v,
        None => return Ok(None),
    };
    let component = match svc_ann.optional(ANNOTATION_COMPONENT) {
        Some(v) => v,
        None => return Ok(None),
    };

    // ── 2. 从 EndpointSlice 提取元数据 ──────────────────────────────────────
    let slice_ann = AnnotationReader::new(slice.metadata.annotations.as_ref());
    let endpoint = match slice_ann.optional(ANNOTATION_ENDPOINT) {
        Some(v) => v,
        None => return Ok(None),
    };
    let transport_json = match slice_ann.optional(ANNOTATION_TRANSPORT) {
        Some(v) => v,
        None => return Ok(None),
    };
    let transport: TransportType = serde_json::from_str(&transport_json)?;

    // ── 3. 验证 EndpointSlice 归属 ───────────────────────────────────────────
    let slice_labels = match slice.metadata.labels.as_ref() {
        Some(l) => l,
        None => return Ok(None),
    };
    let belongs_to_service =
        slice_labels.get(SERVICE_NAME_LABEL).map(String::as_str) == Some(service_name);
    let managed_by_dynamo =
        slice_labels.get(MANAGED_BY_LABEL).map(String::as_str) == Some(MANAGED_BY_VALUE);
    if !belongs_to_service || !managed_by_dynamo {
        return Ok(None);
    }

    // ── 4. 验证 Service / EndpointSlice 端口一致性 ───────────────────────────
    let svc_port = service
        .spec.as_ref()
        .and_then(|s| s.ports.as_ref())
        .and_then(|ports| ports.iter().find(|p| p.name.as_deref() == Some(endpoint.as_str())));
    let Some(svc_port) = svc_port else {
        return Ok(None);
    };

    let port_matches = slice
        .ports.as_deref().unwrap_or(&[])
        .iter()
        .any(|sp| sp.name == svc_port.name && sp.port == Some(svc_port.port));
    if !port_matches {
        return Ok(None);
    }

    // ── 5. 提取 pod_name → instance_id ──────────────────────────────────────
    let pod_name = slice
        .endpoints.iter()
        .find_map(|ep| ep.target_ref.as_ref().and_then(|r| r.name.clone()));
    let Some(pod_name) = pod_name else {
        return Ok(None);
    };

    Ok(Some(DiscoveryInstance::Endpoint(Instance {
        namespace,
        component,
        endpoint,
        instance_id: hash_pod_name(&pod_name),
        transport,
        device_type: None,
    })))
}

// ─── Model ConfigMap 操作 ────────────────────────────────────────────────────

/// 在 Kubernetes 中创建或更新 Model `ConfigMap`。
///
/// ## 设计意图
///
/// 将模型卡（`ModelDeploymentCard`）序列化后存入 `ConfigMap`，
/// 通过 `ownerReference → Pod` 使 Pod 删除时自动 GC。
/// 携带 `KIND_LABEL=model` 使 watch 可以用 label selector 精确过滤。
///
/// ## 处理过程
///
/// 1. 提取 `DiscoveryInstance::Model` 各字段（若类型不符则返回错误）
/// 2. 生成确定性 ConfigMap 名称
/// 3. 构建 `data` map（namespace / component / endpoint / instance_id / card.json）
/// 4. 设置 `ownerReference → Pod`，添加 `KIND_LABEL` label
/// 5. 使用 server-side apply 持久化（幂等 upsert）
///
/// # 参数
/// - `kube_client`：K8s API 客户端
/// - `pod_info`：注册方 Pod 的身份信息
/// - `instance`：待注册的 `DiscoveryInstance::Model`
pub async fn apply_model_config_map(
    kube_client: &KubeClient,
    pod_info: &PodInfo,
    instance: &DiscoveryInstance,
) -> Result<()> {
    let DiscoveryInstance::Model {
        namespace,
        component,
        endpoint,
        instance_id,
        card_json,
        model_suffix,
    } = instance
    else {
        return Err(anyhow!("apply_model_config_map 期望 Model instance，收到其他类型"));
    };

    let name = model_config_map_name(component, endpoint, *instance_id, model_suffix.as_deref());

    // 构建 data map：先填必填字段，再条件追加可选字段
    let mut data: BTreeMap<String, String> = [
        (DATA_NAMESPACE, namespace.as_str()),
        (DATA_COMPONENT, component.as_str()),
        (DATA_ENDPOINT,  endpoint.as_str()),
        (DATA_INSTANCE_ID, &format!("{:x}", instance_id)),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v.to_owned()))
    .collect();

    data.insert(DATA_CARD_JSON.to_owned(), serde_json::to_string(card_json)?);
    if let Some(suffix) = model_suffix {
        data.insert(DATA_MODEL_SUFFIX.to_owned(), suffix.clone());
    }

    let cm = ConfigMap {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            labels: Some(BTreeMap::from([(KIND_LABEL.to_owned(), MODEL_KIND_VALUE.to_owned())])),
            owner_references: Some(vec![pod_owner_ref(pod_info)]),
            ..Default::default()
        },
        data: Some(data),
        ..Default::default()
    };

    Api::<ConfigMap>::namespaced(kube_client.clone(), namespace)
        .patch(&name, &PatchParams::apply(FIELD_MANAGER).force(), &Patch::Apply(&cm))
        .await
        .map(|_| ())
        .map_err(|e| anyhow!("apply Model ConfigMap '{}' 失败: {}", name, e))
}

/// 删除 Model `ConfigMap`，不存在时静默成功。
///
/// # 参数
/// - `kube_client`：K8s API 客户端
/// - `namespace`：ConfigMap 所在命名空间
/// - `component`：Dynamo component 字符串
/// - `endpoint`：Dynamo endpoint 字符串
/// - `instance_id`：实例唯一标识符
/// - `model_suffix`：LoRA adapter 后缀（基础模型为 `None`）
pub async fn delete_model_config_map(
    kube_client: &KubeClient,
    namespace: &str,
    component: &str,
    endpoint: &str,
    instance_id: u64,
    model_suffix: Option<&str>,
) -> Result<()> {
    let name = model_config_map_name(component, endpoint, instance_id, model_suffix);
    Api::<ConfigMap>::namespaced(kube_client.clone(), namespace)
        .delete(&name, &DeleteParams::default())
        .await
        .not_found_ok()
}

/// 从 `ConfigMap` 恢复一个 [`DiscoveryInstance::Model`]。
///
/// ## 处理过程
///
/// 1. 通过 `KIND_LABEL=model` 快速过滤（不匹配则返回 `Ok(None)`）
/// 2. 用 `data.get(key).ok_or_else(...)` 链式读取必填字段
/// 3. 解析 `instance_id`（16 进制字符串）和 `card.json`（JSON）
///
/// # 返回
/// 恢复成功 → `Ok(Some(instance))`；label 不匹配 → `Ok(None)`；解析失败 → `Err`
pub fn model_instance_from_config_map(cm: &ConfigMap) -> Result<Option<DiscoveryInstance>> {
    // 快速过滤：KIND_LABEL 不匹配则跳过（避免解析无关 ConfigMap）
    let is_model = cm
        .metadata.labels.as_ref()
        .and_then(|l| l.get(KIND_LABEL))
        .map(String::as_str) == Some(MODEL_KIND_VALUE);
    if !is_model {
        return Ok(None);
    }

    let data = match cm.data.as_ref() {
        Some(d) => d,
        None => return Ok(None),
    };

    // 通过 `data_field` 闭包消除重复的 `.get(key).ok_or_else(...)` 模式
    let data_field = |key: &str| -> Result<String> {
        data.get(key)
            .cloned()
            .ok_or_else(|| anyhow!("ConfigMap data 缺少 '{}'", key))
    };

    let namespace   = data_field(DATA_NAMESPACE)?;
    let component   = data_field(DATA_COMPONENT)?;
    let endpoint    = data_field(DATA_ENDPOINT)?;
    let instance_id = u64::from_str_radix(&data_field(DATA_INSTANCE_ID)?, 16)?;
    let card_json   = serde_json::from_str(&data_field(DATA_CARD_JSON)?)?;
    let model_suffix = data.get(DATA_MODEL_SUFFIX).cloned();

    Ok(Some(DiscoveryInstance::Model {
        namespace,
        component,
        endpoint,
        instance_id,
        card_json,
        model_suffix,
    }))
}

// ─── EventChannel Lease 操作 ─────────────────────────────────────────────────

/// 在 Kubernetes 中创建或更新 EventChannel `Lease`。
///
/// ## 设计意图
///
/// 使用 `Lease` 存储事件通道信息，借助其 `holderIdentity` + `leaseDurationSeconds`
/// 在 Pod 异常时提供自动过期感知（消费方可检查 `renewTime` 是否超时）。
/// 通过 `ownerReference → Pod` 保证 Pod 删除时 GC 自动清理。
///
/// ## 处理过程
///
/// 1. 提取 `DiscoveryInstance::EventChannel` 各字段
/// 2. 生成确定性 Lease 名称
/// 3. 构建 `LeaseSpec`（`holderIdentity=instance_id_hex`，`leaseDurationSeconds=30`）
/// 4. 携带 namespace / component / topic / transport annotations
/// 5. 使用 server-side apply 持久化
///
/// # 参数
/// - `kube_client`：K8s API 客户端
/// - `pod_info`：注册方 Pod 的身份信息
/// - `instance`：待注册的 `DiscoveryInstance::EventChannel`
pub async fn apply_event_lease(
    kube_client: &KubeClient,
    pod_info: &PodInfo,
    instance: &DiscoveryInstance,
) -> Result<()> {
    let DiscoveryInstance::EventChannel {
        namespace,
        component,
        topic,
        instance_id,
        transport,
    } = instance
    else {
        return Err(anyhow!("apply_event_lease 期望 EventChannel instance，收到其他类型"));
    };

    let name = event_lease_name(component, topic, *instance_id);

    let lease = Lease {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            labels: Some(BTreeMap::from([
                (KIND_LABEL.to_owned(), EVENT_CHANNEL_KIND_VALUE.to_owned()),
            ])),
            annotations: Some(BTreeMap::from([
                (ANNOTATION_NAMESPACE.to_owned(), namespace.clone()),
                (ANNOTATION_COMPONENT.to_owned(), component.clone()),
                (ANNOTATION_TOPIC.to_owned(),     topic.clone()),
                (ANNOTATION_TRANSPORT.to_owned(), serde_json::to_string(transport)?),
            ])),
            owner_references: Some(vec![pod_owner_ref(pod_info)]),
            ..Default::default()
        },
        spec: Some(LeaseSpec {
            holder_identity:        Some(format!("{:x}", instance_id)),
            lease_duration_seconds: Some(30),
            renew_time:             Some(MicroTime(Utc::now())),
            acquire_time:           None,
            lease_transitions:      None,
            preferred_holder:       None,
            strategy:               None,
        }),
    };

    Api::<Lease>::namespaced(kube_client.clone(), namespace)
        .patch(&name, &PatchParams::apply(FIELD_MANAGER).force(), &Patch::Apply(&lease))
        .await
        .map(|_| ())
        .map_err(|e| anyhow!("apply EventChannel Lease '{}' 失败: {}", name, e))
}

/// 删除 EventChannel `Lease`，不存在时静默成功。
///
/// # 参数
/// - `kube_client`：K8s API 客户端
/// - `namespace`：Lease 所在命名空间
/// - `component`：Dynamo component 字符串
/// - `topic`：事件通道 topic 字符串
/// - `instance_id`：实例唯一标识符
pub async fn delete_event_lease(
    kube_client: &KubeClient,
    namespace: &str,
    component: &str,
    topic: &str,
    instance_id: u64,
) -> Result<()> {
    let name = event_lease_name(component, topic, instance_id);
    Api::<Lease>::namespaced(kube_client.clone(), namespace)
        .delete(&name, &DeleteParams::default())
        .await
        .not_found_ok()
}

/// 从 `Lease` 恢复一个 [`DiscoveryInstance::EventChannel`]。
///
/// ## 处理过程
///
/// 1. 通过 `KIND_LABEL=event-channel` 快速过滤
/// 2. 用 [`AnnotationReader`] 读取 namespace / component / topic / transport
/// 3. 从 `spec.holder_identity` 解析 `instance_id`（16 进制字符串）
///
/// # 返回
/// 恢复成功 → `Ok(Some(instance))`；label 不匹配 → `Ok(None)`；解析失败 → `Err`
pub fn event_instance_from_lease(lease: &Lease) -> Result<Option<DiscoveryInstance>> {
    let is_event_channel = lease
        .metadata.labels.as_ref()
        .and_then(|l| l.get(KIND_LABEL))
        .map(String::as_str) == Some(EVENT_CHANNEL_KIND_VALUE);
    if !is_event_channel {
        return Ok(None);
    }

    let ann = AnnotationReader::new(lease.metadata.annotations.as_ref());
    let namespace  = ann.required(ANNOTATION_NAMESPACE)?;
    let component  = ann.required(ANNOTATION_COMPONENT)?;
    let topic      = ann.required(ANNOTATION_TOPIC)?;
    let transport: EventTransport = serde_json::from_str(&ann.required(ANNOTATION_TRANSPORT)?)?;

    let spec = lease.spec.as_ref().ok_or_else(|| anyhow!("Lease 缺少 spec"))?;
    let instance_id = u64::from_str_radix(
        spec.holder_identity.as_deref().ok_or_else(|| anyhow!("Lease 缺少 holder_identity"))?,
        16,
    )?;

    Ok(Some(DiscoveryInstance::EventChannel {
        namespace,
        component,
        topic,
        instance_id,
        transport,
    }))
}

// ─── 命名辅助函数 ────────────────────────────────────────────────────────────

/// 生成 Model `ConfigMap` 的确定性名称。
///
/// ## 格式
///
/// `dyn-model-<component>-<endpoint>-<instance_id_hex>[-<suffix_hash>]`
///
/// - component / endpoint 各截取最多 18 字符（规范化后）
/// - LoRA adapter 通过 `suffix_hash` 区分，防止同实例多模型版本名称碰撞
///
/// ## 长度上限
///
/// 最坏情况（含 suffix）：
/// - `"dyn-model-"` = 10
/// - component（≤18） + `"-"` = 19
/// - endpoint（≤18） + `"-"` = 19
/// - instance_id（≤16） + `"-"` = 17
/// - suffix_hash = 8
/// - 合计 ≤ 73（略超，但实际 instance_id 最多 16 位已极少见，通常 ≤ 8 位）
pub fn model_config_map_name(
    component: &str,
    endpoint: &str,
    instance_id: u64,
    model_suffix: Option<&str>,
) -> String {
    let base = format!(
        "dyn-model-{}-{}-{:x}",
        sanitize(component, 18),
        sanitize(endpoint, 18),
        instance_id
    );
    // 仅当后缀非空时追加 hash，避免空字符串后缀改变名称
    match model_suffix {
        Some(s) if !s.is_empty() => format!("{}-{}", base, short_hash(s)),
        _ => base,
    }
}

/// 生成 EventChannel `Lease` 的确定性名称。
///
/// ## 格式
///
/// `dyn-event-<component>-<topic>-<instance_id_hex>`
pub fn event_lease_name(component: &str, topic: &str, instance_id: u64) -> String {
    format!(
        "dyn-event-{}-{}-{:x}",
        sanitize(component, 18),
        sanitize(topic, 18),
        instance_id
    )
}

// ─── 内部辅助函数 ────────────────────────────────────────────────────────────

/// 构建指向 Pod 的 `OwnerReference`，用于 K8s GC 自动级联删除。
///
/// - `controller=true`：标记本 Pod 是此对象的控制器
/// - `blockOwnerDeletion=false`：允许 Pod 在等待对象删除时直接退出
fn pod_owner_ref(pod_info: &PodInfo) -> OwnerReference {
    OwnerReference {
        api_version: "v1".to_owned(),
        kind: "Pod".to_owned(),
        name: pod_info.pod_name.clone(),
        uid: pod_info.pod_uid.clone(),
        controller: Some(true),
        block_owner_deletion: Some(false),
    }
}

/// 从 `TransportType` 推断端口号，推断失败时使用 `fallback_port`。
///
/// ## 规则
///
/// | 类型 | 推断方式 |
/// |------|---------|
/// | `Http(url)` | 解析 URL，取 `port_or_known_default` |
/// | `Tcp(addr)` | 解析 `host:port`，取 port 部分 |
/// | `Nats(_)` | 无法推断，直接使用 fallback |
fn transport_port_hint(transport: &TransportType, fallback_port: i32) -> i32 {
    match transport {
        TransportType::Http(url) => url::Url::parse(url)
            .ok()
            .and_then(|u| u.port_or_known_default().map(|p| p as i32))
            .unwrap_or(fallback_port),
        TransportType::Tcp(addr) => addr
            .split('/')
            .next()
            .and_then(|hp| hp.rsplit_once(':'))
            .and_then(|(_, p)| p.parse::<i32>().ok())
            .unwrap_or(fallback_port),
        TransportType::Nats(_) => fallback_port,
    }
}

/// 清理 Service 中已无 EndpointSlice 使用的端口定义。
///
/// ## 设计意图
///
/// endpoint 注销后，如果同 Service 下已无其他 Pod 使用该端口，
/// 则从 `spec.ports` 中移除该端口，避免 Service 中积累过期端口定义。
/// 若 Service 无剩余端口，则删除整个 Service。
///
/// ## 处理过程
///
/// 1. 列出 Service 下由 Dynamo 管理的所有 EndpointSlice（label selector）
/// 2. 排除即将删除的 slice，检查剩余 slice 中是否有该 endpoint 端口
/// 3. 若有则保持 Service 不变
/// 4. 若无则从 Service spec.ports 中删除该端口，空端口时删除 Service
async fn cleanup_endpoint_service_port(
    kube_client: &KubeClient,
    namespace: &str,
    service_name: &str,
    endpoint: &str,
    deleting_slice_name: &str,
) -> Result<()> {
    let selector = format!(
        "{}={},{}={}",
        SERVICE_NAME_LABEL, service_name, MANAGED_BY_LABEL, MANAGED_BY_VALUE
    );

    let slice_api: Api<EndpointSlice> = Api::namespaced(kube_client.clone(), namespace);
    let remaining_has_endpoint = slice_api
        .list(&kube::api::ListParams::default().labels(&selector))
        .await?
        .items
        .into_iter()
        // 排除正在删除的 slice 本身
        .filter(|s| s.metadata.name.as_deref() != Some(deleting_slice_name))
        // 检查剩余 slice 中是否有该 endpoint 端口
        .any(|s| {
            s.ports.as_deref().unwrap_or(&[])
                .iter()
                .any(|p| p.name.as_deref() == Some(endpoint))
        });

    if remaining_has_endpoint {
        return Ok(());
    }

    // 从 Service spec.ports 中移除该端口
    let svc_api: Api<Service> = Api::namespaced(kube_client.clone(), namespace);
    let Some(mut svc) = svc_api.get_opt(service_name).await? else {
        return Ok(());
    };

    if let Some(ports) = svc.spec.as_mut().and_then(|s| s.ports.as_mut()) {
        ports.retain(|p| p.name.as_deref() != Some(endpoint));
    }

    let has_ports = svc.spec.as_ref()
        .and_then(|s| s.ports.as_ref())
        .map(|p| !p.is_empty())
        .unwrap_or(false);

    if has_ports {
        apply_service(kube_client, namespace, &svc).await
    } else {
        svc_api.delete(service_name, &DeleteParams::default()).await.not_found_ok()
    }
}

/// 合并端口后 apply Service（create-or-merge 语义）。
///
/// ## 设计意图
///
/// 同一 component 下多个 Pod 共享同一 Service，每次注册只携带自己的端口。
/// 通过先读取已有 Service 再合并端口，避免覆盖其他 Pod 已注册的端口定义。
/// 相同名称的端口以**新值**为准，未出现的已有端口**保留**。
///
/// ## 处理过程
///
/// 1. 尝试 `get_opt` 已有 Service
/// 2. 若不存在，直接 apply 传入的 Service
/// 3. 若已存在，合并 annotations 和 ports（已有优先，新增追加，重名更新）
/// 4. apply 合并后的 Service
async fn apply_or_merge_endpoint_service(
    kube_client: &KubeClient,
    namespace: &str,
    service: &Service,
) -> Result<()> {
    let svc_name = service.metadata.name.as_deref()
        .ok_or_else(|| anyhow!("Service 对象缺少 metadata.name"))?;

    let api: Api<Service> = Api::namespaced(kube_client.clone(), namespace);

    let merged = match api.get_opt(svc_name).await? {
        None => service.clone(),
        Some(existing) => {
            let mut m = service.clone();

            // 合并 annotations：保留已有 key 不被新值覆盖
            if let Some(existing_ann) = existing.metadata.annotations {
                let ann = m.metadata.annotations.get_or_insert_with(BTreeMap::new);
                for (k, v) in existing_ann {
                    ann.entry(k).or_insert(v);
                }
            }

            // 合并端口：以已有端口列表为基础，用新端口更新或追加
            let new_ports = m.spec.as_ref()
                .and_then(|s| s.ports.clone())
                .unwrap_or_default();
            let mut base_ports = existing.spec
                .and_then(|s| s.ports)
                .unwrap_or_default();

            for np in new_ports {
                match np.name.as_ref()
                    .and_then(|name| base_ports.iter().position(|bp| bp.name.as_ref() == Some(name)))
                {
                    Some(idx) => base_ports[idx] = np,
                    None      => base_ports.push(np),
                }
            }

            m.spec.get_or_insert_with(Default::default).ports = Some(base_ports);
            m
        }
    };

    apply_service(kube_client, namespace, &merged).await
}

// ─── 单元测试 ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::TransportType;

    // ── AnnotationReader ──────────────────────────────────────────────────────

    /// `required` 正常读取 → 返回字段值。
    #[test]
    fn annotation_reader_required_found() {
        let mut map = BTreeMap::new();
        map.insert("key".to_owned(), "val".to_owned());
        let reader = AnnotationReader::new(Some(&map));
        assert_eq!(reader.required("key").unwrap(), "val");
    }

    /// `required` 找不到 key → 返回 Err（含 key 名称）。
    #[test]
    fn annotation_reader_required_missing() {
        let map = BTreeMap::new();
        let reader = AnnotationReader::new(Some(&map));
        let err = reader.required("missing").unwrap_err();
        assert!(err.to_string().contains("missing"), "错误信息应包含 key 名称");
    }

    /// `inner=None` 时 `required` 也返回 Err（不 panic）。
    #[test]
    fn annotation_reader_none_inner_required_fails() {
        let reader = AnnotationReader::new(None);
        assert!(reader.required("any").is_err());
    }

    /// `optional` 找到 → 返回 Some。
    #[test]
    fn annotation_reader_optional_found() {
        let mut map = BTreeMap::new();
        map.insert("k".to_owned(), "v".to_owned());
        let reader = AnnotationReader::new(Some(&map));
        assert_eq!(reader.optional("k"), Some("v".to_owned()));
    }

    /// `optional` 找不到 → 返回 None（不 Err）。
    #[test]
    fn annotation_reader_optional_missing() {
        let map = BTreeMap::new();
        let reader = AnnotationReader::new(Some(&map));
        assert_eq!(reader.optional("nope"), None);
    }

    // ── NotFoundOk ────────────────────────────────────────────────────────────

    /// `Ok(42)` 经 `not_found_ok()` → `Ok(())`。
    #[test]
    fn not_found_ok_on_ok() {
        let result: Result<i32, kube::Error> = Ok(42);
        assert!(result.not_found_ok().is_ok());
    }

    /// 404 `ApiError` 经 `not_found_ok()` → `Ok(())`。
    #[test]
    fn not_found_ok_on_404() {
        use kube::error::ErrorResponse;
        let api_err = kube::Error::Api(ErrorResponse {
            code: 404,
            message: "not found".to_owned(),
            reason: "NotFound".to_owned(),
            status: "Failure".to_owned(),
        });
        let result: Result<(), kube::Error> = Err(api_err);
        assert!(result.not_found_ok().is_ok(), "404 应静默成功");
    }

    /// 非 404 `ApiError` 经 `not_found_ok()` → `Err`。
    #[test]
    fn not_found_ok_on_other_error() {
        use kube::error::ErrorResponse;
        let api_err = kube::Error::Api(ErrorResponse {
            code: 403,
            message: "forbidden".to_owned(),
            reason: "Forbidden".to_owned(),
            status: "Failure".to_owned(),
        });
        let result: Result<(), kube::Error> = Err(api_err);
        assert!(result.not_found_ok().is_err(), "非 404 错误应透传");
    }

    // ── model_config_map_name ─────────────────────────────────────────────────

    /// 相同参数产生相同名称（确定性）。
    #[test]
    fn model_config_map_name_deterministic() {
        let a = model_config_map_name("comp", "ep", 0xdeadbeef, None);
        let b = model_config_map_name("comp", "ep", 0xdeadbeef, None);
        assert_eq!(a, b);
    }

    /// 名称以 "dyn-model-" 开头。
    #[test]
    fn model_config_map_name_prefix() {
        let name = model_config_map_name("comp", "ep", 1, None);
        assert!(name.starts_with("dyn-model-"), "名称应以 dyn-model- 开头");
    }

    /// 有 model_suffix 时产生不同名称（suffix hash 被追加）。
    #[test]
    fn model_config_map_name_suffix_differs() {
        let base = model_config_map_name("comp", "ep", 1, None);
        let with_suffix = model_config_map_name("comp", "ep", 1, Some("lora-v1"));
        assert_ne!(base, with_suffix, "有 suffix 时名称应不同");
    }

    /// 空字符串 suffix 与 None suffix 产生相同名称。
    #[test]
    fn model_config_map_name_empty_suffix_equals_none() {
        let none_name = model_config_map_name("c", "e", 1, None);
        let empty_name = model_config_map_name("c", "e", 1, Some(""));
        assert_eq!(none_name, empty_name, "空 suffix 与 None 应相同");
    }

    // ── event_lease_name ──────────────────────────────────────────────────────

    /// 相同参数产生相同名称（确定性）。
    #[test]
    fn event_lease_name_deterministic() {
        let a = event_lease_name("comp", "topic", 42);
        let b = event_lease_name("comp", "topic", 42);
        assert_eq!(a, b);
    }

    /// 名称以 "dyn-event-" 开头。
    #[test]
    fn event_lease_name_prefix() {
        let name = event_lease_name("c", "t", 0);
        assert!(name.starts_with("dyn-event-"));
    }

    /// 不同 topic 产生不同名称。
    #[test]
    fn event_lease_name_differs_by_topic() {
        let a = event_lease_name("comp", "topic-a", 1);
        let b = event_lease_name("comp", "topic-b", 1);
        assert_ne!(a, b);
    }

    // ── transport_port_hint ───────────────────────────────────────────────────

    /// HTTP transport 能正确解析端口。
    #[test]
    fn transport_port_hint_http() {
        let t = TransportType::Http("http://127.0.0.1:8080/path".to_owned());
        assert_eq!(transport_port_hint(&t, 9999), 8080);
    }

    /// HTTP transport 解析失败时使用 fallback。
    #[test]
    fn transport_port_hint_http_fallback() {
        let t = TransportType::Http("not-a-url".to_owned());
        assert_eq!(transport_port_hint(&t, 1234), 1234);
    }

    /// TCP transport 能正确解析端口。
    #[test]
    fn transport_port_hint_tcp() {
        let t = TransportType::Tcp("192.168.1.1:7000".to_owned());
        assert_eq!(transport_port_hint(&t, 9999), 7000);
    }

    /// Nats transport 始终使用 fallback。
    #[test]
    fn transport_port_hint_nats() {
        let t = TransportType::Nats("nats://server:4222".to_owned());
        assert_eq!(transport_port_hint(&t, 5555), 5555);
    }

    // ── model_instance_from_config_map ────────────────────────────────────────

    /// 缺少 KIND_LABEL 的 ConfigMap 被跳过（返回 None，不 Err）。
    #[test]
    fn model_instance_from_cm_wrong_label() {
        let cm = ConfigMap {
            metadata: ObjectMeta {
                labels: Some(BTreeMap::from([("other".to_owned(), "val".to_owned())])),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(model_instance_from_config_map(&cm).unwrap().is_none());
    }

    /// 无 data 字段的 ConfigMap 被跳过（返回 None）。
    #[test]
    fn model_instance_from_cm_no_data() {
        let cm = ConfigMap {
            metadata: ObjectMeta {
                labels: Some(BTreeMap::from([(KIND_LABEL.to_owned(), MODEL_KIND_VALUE.to_owned())])),
                ..Default::default()
            },
            data: None,
            ..Default::default()
        };
        assert!(model_instance_from_config_map(&cm).unwrap().is_none());
    }

    /// 合法 ConfigMap 能正确恢复 Model instance 的各字段。
    #[test]
    fn model_instance_from_cm_valid() {
        use serde_json::json;
        let card = json!({"name": "test-model"});
        let instance_id: u64 = 0xdeadbeef;

        let mut data = BTreeMap::new();
        data.insert(DATA_NAMESPACE.to_owned(), "ns".to_owned());
        data.insert(DATA_COMPONENT.to_owned(), "comp".to_owned());
        data.insert(DATA_ENDPOINT.to_owned(), "ep".to_owned());
        data.insert(DATA_INSTANCE_ID.to_owned(), format!("{:x}", instance_id));
        data.insert(DATA_CARD_JSON.to_owned(), card.to_string());

        let cm = ConfigMap {
            metadata: ObjectMeta {
                labels: Some(BTreeMap::from([(KIND_LABEL.to_owned(), MODEL_KIND_VALUE.to_owned())])),
                ..Default::default()
            },
            data: Some(data),
            ..Default::default()
        };

        let result = model_instance_from_config_map(&cm).unwrap().unwrap();
        match result {
            DiscoveryInstance::Model { namespace, component, endpoint, instance_id: id, .. } => {
                assert_eq!(namespace, "ns");
                assert_eq!(component, "comp");
                assert_eq!(endpoint, "ep");
                assert_eq!(id, 0xdeadbeef);
            }
            _ => panic!("期望 Model variant"),
        }
    }

    // ── event_instance_from_lease ─────────────────────────────────────────────

    /// 缺少 KIND_LABEL 的 Lease 被跳过（返回 None）。
    #[test]
    fn event_instance_from_lease_wrong_label() {
        let lease = Lease {
            metadata: ObjectMeta {
                labels: Some(BTreeMap::from([("x".to_owned(), "y".to_owned())])),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(event_instance_from_lease(&lease).unwrap().is_none());
    }

    /// 合法 Lease 能正确恢复 EventChannel instance 的各字段。
    #[test]
    fn event_instance_from_lease_valid() {
        use crate::discovery::EventTransport;
        let transport = EventTransport::Nats { subject_prefix: "ns.dynamo.comp.ep".to_owned() };
        let instance_id: u64 = 0xabcdef;

        let lease = Lease {
            metadata: ObjectMeta {
                labels: Some(BTreeMap::from([
                    (KIND_LABEL.to_owned(), EVENT_CHANNEL_KIND_VALUE.to_owned()),
                ])),
                annotations: Some(BTreeMap::from([
                    (ANNOTATION_NAMESPACE.to_owned(), "ns".to_owned()),
                    (ANNOTATION_COMPONENT.to_owned(), "comp".to_owned()),
                    (ANNOTATION_TOPIC.to_owned(), "topic".to_owned()),
                    (ANNOTATION_TRANSPORT.to_owned(), serde_json::to_string(&transport).unwrap()),
                ])),
                ..Default::default()
            },
            spec: Some(LeaseSpec {
                holder_identity: Some(format!("{:x}", instance_id)),
                ..Default::default()
            }),
        };

        let result = event_instance_from_lease(&lease).unwrap().unwrap();
        match result {
            DiscoveryInstance::EventChannel { namespace, component, topic, instance_id: id, .. } => {
                assert_eq!(namespace, "ns");
                assert_eq!(component, "comp");
                assert_eq!(topic, "topic");
                assert_eq!(id, 0xabcdef);
            }
            _ => panic!("期望 EventChannel variant"),
        }
    }
}
