// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # K8s 原生发现客户端入口层
//!
//! ## 设计意图
//!
//! `KubeDiscoveryClient` **直接使用 K8s 原生对象**作为发现状态的信息载体，
//! 无需安装任何自定义资源（CRD），从而避免 Helm chart 与控制面耦合，并支持
//! 单 Pod 多 portname 下的细粒度增量更新（例如单独移除一个 LoRA）：
//!
//! | Pagoda 概念     | K8s 原生对象映射                |
//! |----------------|-------------------------------|
//! | `PortName`     | `Service` + `EndpointSlice`   |
//! | `Model`        | `ConfigMap`                   |
//! | `EventChannel` | `Lease`                       |
//!
//! 三类对象的高层 CRUD 已被 [`super::kube::objects`] 与
//! [`super::kube::service_registry`] 抽离为纯函数与构建器；本文件只负责：
//!
//! 1. **进程身份感知**：从 Downward API / 环境变量构造 [`PodInfo`]；
//! 2. **发现协议适配**：把 `register / unregister / list / list_and_watch`
//!    映射到上面三类原生对象的具体调用；
//! 3. **本地缓存与回滚**：所有修改都先写到本地 `DiscoveryMetadata`，K8s 写
//!    失败时回滚到事务前状态，保证“本地视图 ⇔ 集群视图”最终一致。
//!
//! ## 外部契约
//!
//! - 公开符号 [`KubeDiscoveryClient`]、[`hash_pod_name`] 的签名保持稳定；
//!   `KubeDiscoveryClient::new(metadata, cancel_token)` 仍是唯一构造入口。
//! - `Discovery` trait 的语义保持不变：上层调用方无需感知后端的对象映射细节。
//!
//! ## 实现要点
//!
//! - `Discovery::list` 不直接打 K8s API server，而是读 [`daemon`] 维护的
//!   `watch::Receiver<Arc<MetadataSnapshot>>`，避免高频列举打挂 API server。
//! - `Discovery::list_and_watch` 把快照变化转换为 `DiscoveryEvent::Added /
//!   Removed`，差异通过比较 `DiscoveryInstanceId` 集合得到，遵循 K8s 通用
//!   list+watch 的语义。

mod daemon;
mod objects;
mod service_registry;
mod utils;

// hash_pod_name 是跨 FFI 的稳定 ID 哈希，被 C bindings 直接调用，必须保持公开。
pub use utils::hash_pod_name;

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use kube::Client as KubeClient;
use tokio::sync::RwLock;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::CancellationToken;
use crate::discovery::{
    Discovery, DiscoveryEvent, DiscoveryInstance, DiscoveryInstanceId, DiscoveryMetadata,
    DiscoveryQuery, DiscoverySpec, DiscoveryStream, MetadataSnapshot,
};

use daemon::DiscoveryDaemon;
use utils::PodInfo;

// === KubeDiscoveryClient =====================================================

/// 基于 K8s 原生对象的发现客户端。
///
/// `Clone` 仅复制 `Arc` 句柄，使多个上层组件可以共享同一份本地缓存与
/// daemon watch 通道，不会重复启动 reflector。
#[derive(Clone)]
pub struct KubeDiscoveryClient {
    instance_id: u64,
    metadata: Arc<RwLock<DiscoveryMetadata>>,
    snapshot_rx: tokio::sync::watch::Receiver<Arc<MetadataSnapshot>>,
    kube_client: KubeClient,
    pod_info: PodInfo,
}

impl KubeDiscoveryClient {
    /// 构造客户端并启动后台 daemon。
    ///
    /// # 参数
    /// - `metadata`：与 system server 共享的本地元数据存储；写操作会同步更新它。
    /// - `cancel_token`：取消令牌；进程退出时用其优雅停止 daemon 与 reflectors。
    pub async fn new(
        metadata: Arc<RwLock<DiscoveryMetadata>>,
        cancel_token: CancellationToken,
    ) -> Result<Self> {
        let pod_info = PodInfo::from_env()?;
        let instance_id = pod_info.target.instance_id();

        tracing::info!(
            mode = ?pod_info.mode,
            target = ?pod_info.target,
            instance_id = format!("{:x}", instance_id),
            namespace = %pod_info.pod_namespace,
            "Initializing KubeDiscoveryClient (native objects mode)"
        );

        let kube_client = KubeClient::try_default()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create Kubernetes client: {e}"))?;

        // daemon 起始快照置空；后续任何一类对象有更新都会触发首次完整聚合
        let (snapshot_tx, snapshot_rx) =
            tokio::sync::watch::channel(Arc::new(MetadataSnapshot::empty()));

        let daemon = DiscoveryDaemon::new(
            kube_client.clone(),
            pod_info.pod_namespace.clone(),
            cancel_token,
        );
        tokio::spawn(async move {
            if let Err(e) = daemon.run(snapshot_tx).await {
                tracing::error!("Discovery daemon terminated with error: {e}");
            }
        });

        Ok(Self {
            instance_id,
            metadata,
            snapshot_rx,
            kube_client,
            pod_info,
        })
    }

    /// 把单个发现实例写入 K8s 原生对象层（不操作本地缓存）。
    ///
    /// 抽离为独立方法的目的：register 与 unregister 都要在本地缓存修改前后
    /// 触发同一段“向 K8s 写入”的逻辑，本方法把分支细节集中在一处。
    async fn persist_register(&self, instance: &DiscoveryInstance) -> Result<()> {
        match instance {
            DiscoveryInstance::PortName(inst) => {
                objects::register_portname_instance(&self.kube_client, &self.pod_info, inst).await
            }
            DiscoveryInstance::Model { .. } => {
                objects::apply_model_config_map(&self.kube_client, &self.pod_info, instance).await
            }
            DiscoveryInstance::EventChannel { .. } => {
                objects::apply_event_lease(&self.kube_client, &self.pod_info, instance).await
            }
        }
    }

    /// 把单个发现实例从 K8s 原生对象层移除。
    async fn persist_unregister(&self, instance: &DiscoveryInstance) -> Result<()> {
        let ns = &self.pod_info.pod_namespace;
        match instance {
            DiscoveryInstance::PortName(inst) => {
                objects::unregister_portname_instance(
                    &self.kube_client,
                    &self.pod_info.pod_name,
                    ns,
                    &inst.servicegroup,
                    &inst.portname,
                )
                .await
            }
            DiscoveryInstance::Model {
                servicegroup,
                portname,
                instance_id,
                model_suffix,
                ..
            } => {
                objects::delete_model_config_map(
                    &self.kube_client,
                    ns,
                    servicegroup,
                    portname,
                    *instance_id,
                    model_suffix.as_deref(),
                )
                .await
            }
            DiscoveryInstance::EventChannel {
                servicegroup,
                topic,
                instance_id,
                ..
            } => {
                objects::delete_event_lease(&self.kube_client, ns, servicegroup, topic, *instance_id)
                    .await
            }
        }
    }
}

// === Discovery trait 实现 =====================================================

#[async_trait]
impl Discovery for KubeDiscoveryClient {
    fn instance_id(&self) -> u64 {
        self.instance_id
    }

    async fn register_internal(&self, spec: DiscoverySpec) -> Result<DiscoveryInstance> {
        let instance = spec.with_instance_id(self.instance_id);

        // 事务式：先写本地缓存（持锁），再 commit 到 K8s；K8s 写失败则回滚本地。
        let mut meta = self.metadata.write().await;
        let snapshot_before = meta.clone();

        match &instance {
            DiscoveryInstance::PortName(_) => meta.register_portname(instance.clone())?,
            DiscoveryInstance::Model { .. } => meta.register_model_card(instance.clone())?,
            DiscoveryInstance::EventChannel { .. } => {
                meta.register_event_channel(instance.clone())?
            }
        }

        if let Err(e) = self.persist_register(&instance).await {
            tracing::warn!("Failed to persist registration to K8s, rolling back local: {e}");
            *meta = snapshot_before;
            return Err(e);
        }

        Ok(instance)
    }

    async fn unregister(&self, instance: DiscoveryInstance) -> Result<()> {
        let mut meta = self.metadata.write().await;
        let snapshot_before = meta.clone();

        match &instance {
            DiscoveryInstance::PortName(_) => meta.unregister_portname(&instance)?,
            DiscoveryInstance::Model { .. } => meta.unregister_model_card(&instance)?,
            DiscoveryInstance::EventChannel { .. } => meta.unregister_event_channel(&instance)?,
        }

        if let Err(e) = self.persist_unregister(&instance).await {
            tracing::warn!("Failed to persist unregister to K8s, rolling back local: {e}");
            *meta = snapshot_before;
            return Err(e);
        }
        Ok(())
    }

    async fn list(&self, query: DiscoveryQuery) -> Result<Vec<DiscoveryInstance>> {
        // 读 daemon 维护的快照；可能为空（daemon 尚未完成首次聚合）
        let snapshot = self.snapshot_rx.borrow().clone();
        let result = snapshot.filter(&query);
        tracing::debug!(
            seq = snapshot.sequence,
            total = snapshot.instances.len(),
            hit = result.len(),
            "KubeDiscoveryClient::list"
        );
        Ok(result)
    }

    async fn list_and_watch(
        &self,
        query: DiscoveryQuery,
        cancel_token: Option<CancellationToken>,
    ) -> Result<DiscoveryStream> {
        let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut snapshot_rx = self.snapshot_rx.clone();
        let stream_id = uuid::Uuid::new_v4();

        tokio::spawn(async move {
            // 1) 首次同步：把当前快照中匹配 `query` 的实例作为初始 Added 事件
            let snap = snapshot_rx.borrow_and_update().clone();
            let mut known: std::collections::HashMap<DiscoveryInstanceId, DiscoveryInstance> = snap
                .instances
                .values()
                .flat_map(|m| m.filter(&query))
                .map(|i| (i.id(), i))
                .collect();

            for instance in known.values() {
                if event_tx
                    .send(Ok(DiscoveryEvent::Added(instance.clone())))
                    .is_err()
                {
                    tracing::debug!(%stream_id, "receiver dropped during initial sync");
                    return;
                }
            }

            // 2) 增量同步：每次快照变更计算 added / removed 集合差
            loop {
                let waited = match &cancel_token {
                    Some(tok) => tokio::select! {
                        r = snapshot_rx.changed() => r,
                        _ = tok.cancelled() => {
                            tracing::info!(%stream_id, "watch cancelled");
                            return;
                        }
                    },
                    None => snapshot_rx.changed().await,
                };
                if waited.is_err() {
                    tracing::info!(%stream_id, "snapshot channel closed; watch ending");
                    return;
                }

                let snap = snapshot_rx.borrow_and_update().clone();
                let current: std::collections::HashMap<DiscoveryInstanceId, DiscoveryInstance> =
                    snap.instances
                        .values()
                        .flat_map(|m| m.filter(&query))
                        .map(|i| (i.id(), i))
                        .collect();

                let current_ids: HashSet<&DiscoveryInstanceId> = current.keys().collect();
                let known_ids: HashSet<&DiscoveryInstanceId> = known.keys().collect();

                // 触发 Added：当前有而历史没有
                for id in current_ids.difference(&known_ids).copied().cloned().collect::<Vec<_>>() {
                    if let Some(instance) = current.get(&id) {
                        if event_tx
                            .send(Ok(DiscoveryEvent::Added(instance.clone())))
                            .is_err()
                        {
                            return;
                        }
                    }
                }

                // 触发 Removed：历史有而当前没有
                for id in known_ids.difference(&current_ids).copied().cloned().collect::<Vec<_>>() {
                    if event_tx.send(Ok(DiscoveryEvent::Removed(id))).is_err() {
                        return;
                    }
                }

                known = current;
            }
        });

        Ok(Box::pin(UnboundedReceiverStream::new(event_rx)))
    }
}

// === 单元测试 =================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::servicegroup::{Instance, TransportType};

    /// 构造一个 PortName `DiscoveryInstance`，仅用于测试 `id()` 派生与差异计算。
    fn mk_portname(suffix: u64) -> DiscoveryInstance {
        DiscoveryInstance::PortName(Instance {
            instance_id: suffix,
            namespace: "ns".to_owned(),
            servicegroup: "comp".to_owned(),
            portname: "ep".to_owned(),
            transport: TransportType::Nats(format!("nats://t/{suffix}")),
            device_type: None,
        })
    }

    /// ## 测试过程
    /// 对两个不同 instance_id 的 PortName 调用 `id()`，断言不同。
    /// ## 意义
    /// list_and_watch 内部 diff 逻辑依赖 `DiscoveryInstanceId` 的可哈希区分。
    #[test]
    fn portname_ids_are_distinct() {
        assert_ne!(mk_portname(1).id(), mk_portname(2).id());
    }

    /// ## 测试过程
    /// 把两个不同的 PortName 放入 `HashSet`，验证容量为 2。
    /// ## 意义
    /// 保证 `DiscoveryInstanceId` 实现了正确的 `Hash + Eq`。
    #[test]
    fn portname_ids_hashable_distinct_in_set() {
        let mut s = std::collections::HashSet::new();
        s.insert(mk_portname(1).id());
        s.insert(mk_portname(2).id());
        assert_eq!(s.len(), 2);
    }
}
