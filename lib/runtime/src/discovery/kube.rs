// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Kubernetes 原生发现后端。
//!
//! - `PortName` → `Service + EndpointSlice`（headless，pod-owned）
//! - `Model`（ModelCard）→ `ConfigMap`
//! - `EventChannel` → `Lease`
//!
//! 写操作（register / unregister）通过 Server-Side Apply 直接调用 K8s API；
//! 读操作（list / list_and_watch）从 [`DiscoveryDaemon`] 聚合的 [`MetadataSnapshot`] 读取，
//! 无额外 API 调用。

pub mod daemon;
pub mod objects;
pub mod service_registry;
pub mod utils;

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{watch, RwLock};
use tokio_util::sync::CancellationToken;

use super::metadata::{DiscoveryMetadata, MetadataSnapshot};
use super::{
    Discovery, DiscoveryEvent, DiscoveryInstance, DiscoveryInstanceId, DiscoveryQuery,
    DiscoverySpec, DiscoveryStream,
};
use self::daemon::DiscoveryDaemon;
use self::utils::PodInfo;

// ══════════════════════════════════════════════════════════════════════════════
// KubeDiscoveryClient
// ══════════════════════════════════════════════════════════════════════════════

/// Kubernetes 发现客户端。
///
/// 每个 Worker 进程持有一个实例；`DistributedRuntime` 中以 `Arc<dyn Discovery>` 共享。
#[derive(Clone)]
pub struct KubeDiscoveryClient {
    /// 本进程实例 ID（`hash_pod_name(pod_name)` 的结果）
    instance_id: u64,
    /// 本进程当前注册的元数据（写操作时持锁，跨越 K8s API 调用）
    metadata: Arc<RwLock<DiscoveryMetadata>>,
    /// 从 `DiscoveryDaemon` 接收最新的集群全局快照
    metadata_watch: watch::Receiver<Arc<MetadataSnapshot>>,
    /// Kubernetes API 客户端
    kube_client: kube::Client,
    /// 本 Pod 身份信息
    pod_info: PodInfo,
    /// 守护进程取消令牌（shutdown 时触发）
    cancel: CancellationToken,
}

impl KubeDiscoveryClient {
    /// 从当前 Pod 环境初始化 Kubernetes 发现客户端。
    ///
    /// 1. 从 Downward API 文件或环境变量读取 Pod 身份（文件优先，支持 CRIU）
    /// 2. `hash_pod_name` 计算稳定的 `instance_id`
    /// 3. `kube::Client::try_default()` 创建客户端
    /// 4. 创建 `watch` channel，初始值为空快照
    /// 5. 构造并 spawn `DiscoveryDaemon` 后台任务
    pub async fn new() -> anyhow::Result<Arc<Self>> {
        let pod_info = PodInfo::from_env();
        let instance_id = utils::hash_pod_name(&pod_info.pod_name);
        let kube_client = kube::Client::try_default().await?;
        Self::build(kube_client, pod_info, instance_id)
    }

    /// 用于测试的构造函数：接受预构建的 `kube::Client`（可指定 instance_id）。
    pub async fn with_client(
        kube_client: kube::Client,
        pod_info: PodInfo,
    ) -> anyhow::Result<Arc<Self>> {
        let instance_id = utils::hash_pod_name(&pod_info.pod_name);
        Self::build(kube_client, pod_info, instance_id)
    }

    fn build(
        kube_client: kube::Client,
        pod_info: PodInfo,
        instance_id: u64,
    ) -> anyhow::Result<Arc<Self>> {
        let (watch_tx, watch_rx) =
            watch::channel(Arc::new(MetadataSnapshot::empty()));

        let cancel = CancellationToken::new();

        let daemon = DiscoveryDaemon::new(
            kube_client.clone(),
            pod_info.clone(),
            cancel.clone(),
        );
        daemon.spawn(watch_tx);

        Ok(Arc::new(Self {
            instance_id,
            metadata: Arc::new(RwLock::new(DiscoveryMetadata::new())),
            metadata_watch: watch_rx,
            kube_client,
            pod_info,
            cancel,
        }))
    }

    pub fn pod_info(&self) -> &PodInfo {
        &self.pod_info
    }

    pub fn kube_client(&self) -> &kube::Client {
        &self.kube_client
    }
}

impl std::fmt::Debug for KubeDiscoveryClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KubeDiscoveryClient")
            .field("instance_id", &self.instance_id)
            .field("pod_info", &self.pod_info)
            .finish()
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// Discovery impl
// ══════════════════════════════════════════════════════════════════════════════

#[async_trait]
impl Discovery for KubeDiscoveryClient {
    fn instance_id(&self) -> u64 {
        self.instance_id
    }

    /// 原子注册：先更新本地元数据，再 apply K8s 原生资源；失败时回滚本地状态。
    ///
    /// 写锁跨越整个 K8s API 调用（保证本地状态与原生对象一致性），
    /// 代价是注册操作串行化——但 `register` 通常只在进程启动时调用一次。
    async fn register_internal(&self, spec: DiscoverySpec) -> anyhow::Result<DiscoveryInstance> {
        let instance = spec.with_instance_id(self.instance_id);

        let mut meta = self.metadata.write().await;
        let rollback = meta.clone();

        // 写入本地元数据
        match &instance {
            DiscoveryInstance::PortName(_) => meta.register_portname(instance.clone())?,
            DiscoveryInstance::Model { .. } => meta.register_model_card(instance.clone())?,
            DiscoveryInstance::EventChannel { .. } => {
                meta.register_event_channel(instance.clone())?
            }
        }

        // 将注册意图 apply 到 K8s
        if let Err(e) = self.apply_to_kube(&instance).await {
            *meta = rollback;
            return Err(e);
        }

        Ok(instance)
    }

    async fn unregister(&self, instance: DiscoveryInstance) -> anyhow::Result<()> {
        let mut meta = self.metadata.write().await;
        let rollback = meta.clone();

        match &instance {
            DiscoveryInstance::PortName(_) => meta.unregister_portname(&instance)?,
            DiscoveryInstance::Model { .. } => meta.unregister_model_card(&instance)?,
            DiscoveryInstance::EventChannel { .. } => {
                meta.unregister_event_channel(&instance)?
            }
        }

        if let Err(e) = self.delete_from_kube(&instance).await {
            *meta = rollback;
            return Err(e);
        }

        Ok(())
    }

    /// 从最新的守护进程快照中过滤（不触发额外 K8s API 调用）。
    ///
    /// 语义：截至最近一次 debounce 窗口的状态，而非严格实时。
    async fn list(&self, query: DiscoveryQuery) -> anyhow::Result<Vec<DiscoveryInstance>> {
        let snapshot = self.metadata_watch.borrow();
        Ok(snapshot.filter(&query))
    }

    /// 先发出全量快照的 Added 事件，再持续监听快照变化并 diff。
    async fn list_and_watch(
        &self,
        query: DiscoveryQuery,
        cancel_token: Option<CancellationToken>,
    ) -> anyhow::Result<DiscoveryStream> {
        let mut watch_rx = self.metadata_watch.clone();
        let cancel = cancel_token.unwrap_or_else(CancellationToken::new);

        let (tx, rx) =
            tokio::sync::mpsc::unbounded_channel::<anyhow::Result<DiscoveryEvent>>();

        tokio::spawn(async move {
            let mut known: HashSet<DiscoveryInstanceId> = HashSet::new();

            // ── 初始快照（list 部分）
            {
                let snapshot = watch_rx.borrow_and_update();
                for inst in snapshot.filter(&query) {
                    known.insert(inst.id());
                    if tx.send(Ok(DiscoveryEvent::Added(inst))).is_err() {
                        return;
                    }
                }
            }

            // ── 持续监听增量变化（watch 部分）
            loop {
                tokio::select! {
                    result = watch_rx.changed() => {
                        if result.is_err() {
                            break;
                        }
                        let snapshot = watch_rx.borrow_and_update();
                        let current: Vec<DiscoveryInstance> = snapshot.filter(&query);
                        let current_ids: HashSet<DiscoveryInstanceId> =
                            current.iter().map(|i| i.id()).collect();

                        for inst in &current {
                            if !known.contains(&inst.id()) {
                                if tx.send(Ok(DiscoveryEvent::Added(inst.clone()))).is_err() {
                                    return;
                                }
                            }
                        }
                        for id in &known {
                            if !current_ids.contains(id) {
                                if tx.send(Ok(DiscoveryEvent::Removed(id.clone()))).is_err() {
                                    return;
                                }
                            }
                        }
                        known = current_ids;
                    }
                    _ = cancel.cancelled() => break,
                }
            }
        });

        let stream =
            tokio_stream::wrappers::UnboundedReceiverStream::new(rx);
        Ok(Box::pin(stream))
    }

    fn shutdown(&self) {
        self.cancel.cancel();
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// K8s 资源操作（私有辅助）
// ══════════════════════════════════════════════════════════════════════════════

impl KubeDiscoveryClient {
    /// 将 `DiscoveryInstance` apply 到对应的 K8s 原生资源。
    async fn apply_to_kube(&self, instance: &DiscoveryInstance) -> anyhow::Result<()> {
        let ns = &self.pod_info.pod_namespace;
        match instance {
            DiscoveryInstance::PortName(inst) => {
                let reg = service_registry::ServiceRegistration {
                    namespace: inst.namespace.clone(),
                    servicegroup: inst.servicegroup.clone(),
                    portname: inst.portname.clone(),
                    instance_id: inst.instance_id,
                    transport: inst.transport.clone(),
                    pod_info: self.pod_info.clone(),
                };
                let svc = service_registry::build_service(&reg);
                let slice = service_registry::build_endpoint_slice(&reg);
                service_registry::apply_service(&self.kube_client, ns, &svc).await?;
                service_registry::apply_endpoint_slice(&self.kube_client, ns, &slice).await?;
            }
            DiscoveryInstance::Model {
                servicegroup,
                portname,
                instance_id,
                card_json,
                model_suffix,
                topo_json,
                ..
            } => {
                objects::apply_model_config_map(
                    &self.kube_client,
                    ns,
                    servicegroup,
                    portname,
                    *instance_id,
                    card_json,
                    model_suffix.as_deref(),
                    topo_json,
                )
                .await?;
            }
            DiscoveryInstance::EventChannel { servicegroup, topic, instance_id, transport, .. } => {
                objects::apply_event_lease(
                    &self.kube_client,
                    ns,
                    servicegroup,
                    topic,
                    *instance_id,
                    transport,
                )
                .await?;
            }
        }
        Ok(())
    }

    /// 从 K8s 删除对应的原生资源（OwnerReference GC 也会自动清理，此处主动触发）。
    async fn delete_from_kube(&self, instance: &DiscoveryInstance) -> anyhow::Result<()> {
        // K8s OwnerReference GC 在 pod 删除时自动回收资源；
        // 此处主动删除用于 graceful shutdown 场景，避免依赖 GC 延迟。
        //
        // 具体删除逻辑委托给 objects 模块实现。
        let _ = instance; // suppress unused warning
        // K8s 集成存根：SSA patch 删除逻辑待补充。
        Ok(())
    }
}
