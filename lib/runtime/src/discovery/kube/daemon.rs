// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # K8s 原生发现聚合 daemon
//!
//! ## 设计意图
//!
//! 旧版 daemon 把单个 Pod 的全部 portname / model / event channel 信息塞在
//! 一个 `PagodaWorkerMetadata` CR 里聚合；新版改为**四路独立 reflector**：
//!
//! | reflector       | 监听对象                                | 用途                  |
//! |-----------------|----------------------------------------|----------------------|
//! | `eps_store`     | `EndpointSlice` (managed-by=pagoda)     | 就绪门控 + PortName 元数据 |
//! | `svc_store`     | `Service` (registry-mode=native-service)| PortName 补全 namespace/servicegroup |
//! | `cm_store`      | `ConfigMap` (kind=model)                | Model 元数据          |
//! | `lease_store`   | `Lease` (kind=event-channel)            | EventChannel 元数据   |
//!
//! 聚合阶段会按 `instance_id` 把跨对象的信息合并到一份
//! `Arc<DiscoveryMetadata>`，从而保持 `MetadataSnapshot` 的字段结构与上层契约
//! **完全不变**，只是底层数据来源由 CR 切换到了原生对象。
//!
//! ## 外部契约
//!
//! - `pub(super) DiscoveryDaemon`：仅 `kube` 内部使用；签名与历史版本一致：
//!   - `DiscoveryDaemon::new(client, namespace, cancel_token)`
//!   - `DiscoveryDaemon::run(snapshot_tx)`
//! - 输出 `tokio::sync::watch::Sender<Arc<MetadataSnapshot>>`：language-agnostic
//!   的快照管道，上层 `KubeDiscoveryClient` 据此驱动 `list_and_watch`。
//!
//! ## 实现要点
//!
//! - 通过 [`tokio::sync::Notify`] 把四路 reflector 的更新事件汇聚到单一聚合
//!   循环，避免“一对多”锁竞争。
//! - 引入 500ms 去抖：在突发批量更新（例如 Pod 重启时 RC 一次性写 Pod /
//!   EndpointSlice / ConfigMap）下，只触发一次重算。
//! - **generation 不再来自 K8s 对象的 `metadata.generation`**，而是
//!   `serde_json::to_string(metadata)` 的 `DefaultHasher` 摘要——这样跨对象
//!   汇总变化也能反映到 generation 上，准确驱动 `has_changes_from` 的判定。
//! - 就绪门控：只有出现在 ready EndpointSlice 中的 Pod 才被纳入快照，
//!   未就绪 Pod 即便已经写了 Model ConfigMap 也不会泄漏到下游。

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use anyhow::Result;
use futures::StreamExt;
use k8s_openapi::api::coordination::v1::Lease;
use k8s_openapi::api::core::v1::{ConfigMap, Service};
use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::{
    Api, Client as KubeClient,
    runtime::{WatchStreamExt, reflector, watcher, watcher::Config},
};
use tokio::sync::Notify;
use tokio::time::{Duration, sleep};

use crate::CancellationToken;
use crate::discovery::{DiscoveryInstance, DiscoveryMetadata, MetadataSnapshot};

use super::objects::{
    EVENT_CHANNEL_KIND_VALUE, KIND_LABEL, MODEL_KIND_VALUE, endpoint_instance_from_service_and_slice,
    event_instance_from_lease, model_instance_from_config_map,
};
use super::service_registry::{
    MANAGED_BY_LABEL, MANAGED_BY_VALUE, REGISTRY_MODE_LABEL, REGISTRY_MODE_VALUE,
    SERVICE_NAME_LABEL,
};
use super::utils::{extract_portname_info, hash_pod_name};

/// 多路更新事件汇聚后再批处理的去抖窗口。
const DEBOUNCE_DURATION: Duration = Duration::from_millis(500);

// === DiscoveryDaemon =========================================================

/// 聚合 daemon：监听四类原生对象并产出 `MetadataSnapshot`。
pub(super) struct DiscoveryDaemon {
    kube_client: KubeClient,
    namespace: String,
    cancel_token: CancellationToken,
}

impl DiscoveryDaemon {
    pub fn new(kube_client: KubeClient, namespace: String, cancel_token: CancellationToken) -> Self {
        Self {
            kube_client,
            namespace,
            cancel_token,
        }
    }

    /// 运行四路 reflector 与聚合循环。
    ///
    /// 当所有 watcher 都不再有外部接收方（或 `cancel_token` 被触发）时退出。
    pub async fn run(
        self,
        snapshot_tx: tokio::sync::watch::Sender<Arc<MetadataSnapshot>>,
    ) -> Result<()> {
        let notify = Arc::new(Notify::new());

        let eps_store = spawn_reflector::<EndpointSlice>(
            &self.kube_client,
            &self.namespace,
            Config::default()
                .labels(&format!("{MANAGED_BY_LABEL}={MANAGED_BY_VALUE}"))
                .labels(&format!("{REGISTRY_MODE_LABEL}={REGISTRY_MODE_VALUE}")),
            "EndpointSlice",
            notify.clone(),
        );
        let svc_store = spawn_reflector::<Service>(
            &self.kube_client,
            &self.namespace,
            Config::default().labels(&format!("{REGISTRY_MODE_LABEL}={REGISTRY_MODE_VALUE}")),
            "Service",
            notify.clone(),
        );
        let cm_store = spawn_reflector::<ConfigMap>(
            &self.kube_client,
            &self.namespace,
            Config::default().labels(&format!("{KIND_LABEL}={MODEL_KIND_VALUE}")),
            "ConfigMap[model]",
            notify.clone(),
        );
        let lease_store = spawn_reflector::<Lease>(
            &self.kube_client,
            &self.namespace,
            Config::default().labels(&format!("{KIND_LABEL}={EVENT_CHANNEL_KIND_VALUE}")),
            "Lease[event-channel]",
            notify.clone(),
        );

        tracing::info!(
            namespace = %self.namespace,
            "Discovery daemon started (native objects mode, 4 reflectors)"
        );

        let mut sequence: u64 = 0;
        let mut prev = MetadataSnapshot::empty();

        loop {
            tokio::select! {
                _ = notify.notified() => {
                    sleep(DEBOUNCE_DURATION).await;
                    // 抹掉去抖窗口内堆积的额外 notify，确保一轮聚合只触发一次
                    let _ = tokio::time::timeout(Duration::ZERO, notify.notified()).await;

                    let snapshot = aggregate(
                        &eps_store, &svc_store, &cm_store, &lease_store, sequence,
                    );

                    if snapshot.has_changes_from(&prev) {
                        prev = snapshot.clone();
                        if snapshot_tx.send(Arc::new(snapshot)).is_err() {
                            tracing::info!("No watch subscribers, daemon stopping");
                            break;
                        }
                    }
                    sequence = sequence.wrapping_add(1);
                }
                _ = self.cancel_token.cancelled() => {
                    tracing::info!("Discovery daemon received cancellation");
                    break;
                }
            }
        }

        Ok(())
    }
}

// === reflector 启动辅助 =======================================================

/// 启动一个针对类型 `T` 的 reflector，把 store 句柄返回供聚合阶段读取。
///
/// 抽离为泛型函数的目的：四类对象的 watcher 流程结构完全相同，唯一区别是
/// 类型参数与 label selector，集中实现可以避免每类对象都写一遍样板代码。
fn spawn_reflector<T>(
    client: &KubeClient,
    namespace: &str,
    watch_config: Config,
    kind_tag: &'static str,
    notify: Arc<Notify>,
) -> reflector::Store<T>
where
    T: kube::Resource<DynamicType = (), Scope = k8s_openapi::NamespaceResourceScope>
        + Clone
        + std::fmt::Debug
        + Send
        + Sync
        + serde::de::DeserializeOwned
        + 'static,
{
    let api: Api<T> = Api::namespaced(client.clone(), namespace);
    let (reader, writer) = reflector::store();

    let stream = reflector(writer, watcher(api, watch_config))
        .default_backoff()
        .touched_objects()
        .for_each(move |res| {
            match res {
                Ok(_) => {
                    tracing::trace!(kind = kind_tag, "reflector tick");
                    notify.notify_one();
                }
                Err(e) => {
                    tracing::warn!(kind = kind_tag, error = %e, "reflector error");
                    notify.notify_one();
                }
            }
            futures::future::ready(())
        });
    tokio::spawn(stream);

    tracing::info!(kind = kind_tag, "Reflector started");
    reader
}

// === 聚合算法 ================================================================

/// 把四个 store 的当前状态合成一份 [`MetadataSnapshot`]。
///
/// ## 处理过程
///
/// 1. 列举所有 ready 的 `EndpointSlice`，得到 `(instance_id, pod_name)` 元组集；
/// 2. 把所有 Service 按 `metadata.name` 索引为 map，供后续 PortName 恢复；
/// 3. 用 [`endpoint_instance_from_service_and_slice`] 复原每个 PortName 实例；
/// 4. 用 [`model_instance_from_config_map`] / [`event_instance_from_lease`]
///    把每个 ConfigMap / Lease 复原为对应的 DiscoveryInstance；
/// 5. 用 `pod_name → hash_pod_name → instance_id` 将所有实例分组到
///    `HashMap<instance_id, DiscoveryMetadata>`；
/// 6. 仅保留出现在 ready 列表中的 instance_id（“就绪门控”）。
///
/// generation 由聚合后的 metadata JSON 哈希得出，跨任何对象变化都能反映。
fn aggregate(
    eps_store: &reflector::Store<EndpointSlice>,
    svc_store: &reflector::Store<Service>,
    cm_store: &reflector::Store<ConfigMap>,
    lease_store: &reflector::Store<Lease>,
    sequence: u64,
) -> MetadataSnapshot {
    let start = std::time::Instant::now();

    // 第 1 步：ready 条目（gate）
    let mut ready_pods: HashMap<u64, String> = HashMap::new();
    for slice_arc in eps_store.state().iter() {
        for (id, pod) in extract_portname_info(slice_arc.as_ref()) {
            ready_pods.insert(id, pod);
        }
    }

    // 第 2 步：Service 索引
    let svc_index: HashMap<String, Arc<Service>> = svc_store
        .state()
        .iter()
        .filter_map(|s| s.metadata.name.clone().map(|n| (n, s.clone())))
        .collect();

    // 第 3 步：恢复 PortName
    let mut per_pod: HashMap<u64, DiscoveryMetadata> = HashMap::new();
    for slice_arc in eps_store.state().iter() {
        let slice = slice_arc.as_ref();
        let service_name = slice
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get(SERVICE_NAME_LABEL));
        let Some(svc_name) = service_name else { continue };
        let Some(svc) = svc_index.get(svc_name) else {
            continue;
        };

        let recovered = match endpoint_instance_from_service_and_slice(svc, slice) {
            Ok(Some(inst)) => inst,
            Ok(None) => continue,
            Err(e) => {
                tracing::debug!(error = %e, "failed to recover PortName from slice/service");
                continue;
            }
        };

        let pod_name = endpoint_slice_target_pod(slice);
        if let Some(pod) = pod_name {
            let id = hash_pod_name(&pod);
            insert_into_metadata(per_pod.entry(id).or_default(), recovered);
        }
    }

    // 第 4 步：恢复 Model
    for cm_arc in cm_store.state().iter() {
        match model_instance_from_config_map(cm_arc.as_ref()) {
            Ok(Some(inst)) => {
                if let DiscoveryInstance::Model { instance_id, .. } = &inst {
                    let id = *instance_id;
                    insert_into_metadata(per_pod.entry(id).or_default(), inst);
                }
            }
            Ok(None) => {}
            Err(e) => tracing::debug!(error = %e, "failed to recover Model from ConfigMap"),
        }
    }

    // 第 5 步：恢复 EventChannel
    for lease_arc in lease_store.state().iter() {
        match event_instance_from_lease(lease_arc.as_ref()) {
            Ok(Some(inst)) => {
                if let DiscoveryInstance::EventChannel { instance_id, .. } = &inst {
                    let id = *instance_id;
                    insert_into_metadata(per_pod.entry(id).or_default(), inst);
                }
            }
            Ok(None) => {}
            Err(e) => tracing::debug!(error = %e, "failed to recover EventChannel from Lease"),
        }
    }

    // 第 6 步：就绪门控
    let instances: HashMap<u64, Arc<DiscoveryMetadata>> = per_pod
        .into_iter()
        .filter(|(id, _)| ready_pods.contains_key(id))
        .map(|(id, m)| (id, Arc::new(m)))
        .collect();

    // generation = 每个实例 metadata JSON 哈希；跨对象变化都被覆盖
    let generations: HashMap<u64, i64> = instances
        .iter()
        .map(|(id, m)| (*id, content_generation(m.as_ref())))
        .collect();

    tracing::trace!(
        seq = sequence,
        instances = instances.len(),
        ready = ready_pods.len(),
        elapsed_ms = start.elapsed().as_millis() as u64,
        "aggregate snapshot complete"
    );

    MetadataSnapshot {
        instances,
        generations,
        sequence,
        timestamp: std::time::Instant::now(),
    }
}

/// 从 EndpointSlice 中拿到第一个 ready 端点对应的 Pod 名。
fn endpoint_slice_target_pod(slice: &EndpointSlice) -> Option<String> {
    slice.endpoints.iter().find_map(|ep| {
        let ready = ep.conditions.as_ref().and_then(|c| c.ready).unwrap_or(false);
        if !ready {
            return None;
        }
        ep.target_ref.as_ref().and_then(|r| r.name.clone())
    })
}

/// 把单个 `DiscoveryInstance` 追加到 pod 维度的 `DiscoveryMetadata` 桶。
///
/// 抽离为独立函数避免在 `aggregate` 内重复 match 三类型的样板代码。
fn insert_into_metadata(meta: &mut DiscoveryMetadata, instance: DiscoveryInstance) {
    let res = match &instance {
        DiscoveryInstance::PortName(_) => meta.register_portname(instance),
        DiscoveryInstance::Model { .. } => meta.register_model_card(instance),
        DiscoveryInstance::EventChannel { .. } => meta.register_event_channel(instance),
    };
    if let Err(e) = res {
        tracing::warn!(error = %e, "insert_into_metadata failed");
    }
}

/// 计算 metadata 的内容指纹作为 generation。
///
/// 不能用 `metadata.generation`（K8s 维护的对象级版本），因为快照横跨多类对象。
/// 使用 JSON 序列化后哈希——这是“内容稳定”而非“写入次数”的版本号。
fn content_generation(meta: &DiscoveryMetadata) -> i64 {
    let s = serde_json::to_string(meta).unwrap_or_default();
    let mut h = DefaultHasher::new();
    s.hash(&mut h);
    // 取低 63 位，避免 `i64` 转换溢出（generation 上层用 `i64` 仅做对比，不在意符号）
    (h.finish() & 0x7FFF_FFFF_FFFF_FFFF) as i64
}

// === 单元测试 =================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// ## 测试过程
    /// 对同一个 `DiscoveryMetadata` 两次计算 generation，断言相等。
    /// ## 意义
    /// 保证“相同内容产生相同 generation”，否则 daemon 会误判变化导致下游被
    /// 频繁唤醒。
    #[test]
    fn content_generation_deterministic() {
        let m = DiscoveryMetadata::new();
        assert_eq!(content_generation(&m), content_generation(&m));
    }

    /// ## 测试过程
    /// 向空 metadata 注册一个 PortName，断言 generation 相对空状态发生变化。
    /// ## 意义
    /// 验证 generation 能反映内容差异，确保 `has_changes_from` 触发正确。
    #[test]
    fn content_generation_changes_on_register() {
        use crate::servicegroup::{Instance, TransportType};

        let empty_gen = content_generation(&DiscoveryMetadata::new());
        let mut m = DiscoveryMetadata::new();
        m.register_portname(DiscoveryInstance::PortName(Instance {
            instance_id: 1,
            namespace: "ns".into(),
            servicegroup: "c".into(),
            portname: "ep".into(),
            transport: TransportType::Nats("nats://x".into()),
            device_type: None,
        }))
        .unwrap();

        assert_ne!(empty_gen, content_generation(&m));
    }

    /// ## 测试过程
    /// 构造一个 EndpointSlice：包含一个 ready+target_ref 的端点；调用
    /// `endpoint_slice_target_pod`，断言返回 Pod 名。
    /// ## 意义
    /// 验证就绪门控的来源解析正确，避免“ready 但无 pod_name”的边缘情形漏过。
    #[test]
    fn endpoint_slice_target_pod_returns_first_ready() {
        use k8s_openapi::api::core::v1::ObjectReference;
        use k8s_openapi::api::discovery::v1::{Endpoint, EndpointConditions};

        let slice = EndpointSlice {
            address_type: "IPv4".into(),
            endpoints: vec![
                Endpoint {
                    addresses: vec!["10.0.0.1".into()],
                    conditions: Some(EndpointConditions {
                        ready: Some(false),
                        ..Default::default()
                    }),
                    target_ref: Some(ObjectReference {
                        name: Some("not-ready".into()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                Endpoint {
                    addresses: vec!["10.0.0.2".into()],
                    conditions: Some(EndpointConditions {
                        ready: Some(true),
                        ..Default::default()
                    }),
                    target_ref: Some(ObjectReference {
                        name: Some("ready-pod".into()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        assert_eq!(
            endpoint_slice_target_pod(&slice).as_deref(),
            Some("ready-pod")
        );
    }

    /// ## 测试过程
    /// 把同一个 instance 两次插入 `DiscoveryMetadata`（通过 helper），断言总
    /// portnames 计数保持 1。
    /// ## 意义
    /// 验证 `insert_into_metadata` 对同 key 是覆盖语义，避免快照膨胀。
    #[test]
    fn insert_into_metadata_is_idempotent() {
        use crate::servicegroup::{Instance, TransportType};
        let inst = DiscoveryInstance::PortName(Instance {
            instance_id: 1,
            namespace: "ns".into(),
            servicegroup: "c".into(),
            portname: "ep".into(),
            transport: TransportType::Nats("nats://x".into()),
            device_type: None,
        });
        let mut m = DiscoveryMetadata::new();
        insert_into_metadata(&mut m, inst.clone());
        insert_into_metadata(&mut m, inst);
        assert_eq!(m.get_all_portnames().len(), 1);
    }
}
