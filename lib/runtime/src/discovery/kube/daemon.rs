// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 原生资源聚合守护进程。
//!
//! `DiscoveryDaemon` 以 `kube-runtime` 的 `reflector` 机制（本地缓存 + list/watch）
//! 建立四路本地状态存储（EndpointSlice / Service / ConfigMap / Lease），
//! 在任意资源变化时 debounce 500ms 后重新聚合出 [`MetadataSnapshot`]，
//! 通过 `watch::Sender` 广播给所有 [`KubeDiscoveryClient`]。
//!
//! # 为何需要多 reflector 聚合
//!
//! 单靠 `EndpointSlice` 只能知道 pod IP 是否 ready，不含模型卡与事件通道；
//! 单靠 `ConfigMap`/`Lease` 又无法确认 pod 是否 ready。
//! 只有四类资源在同一 pod 身上形成交集时，才能得到真正可用的完整实例视图。

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::utils::PodInfo;
use crate::discovery::metadata::MetadataSnapshot;

/// 事件批量合并窗口：连续 k8s 控制面事件等待此时长后统一聚合，避免快照抖动。
const DEBOUNCE_DURATION: Duration = Duration::from_millis(500);

// ══════════════════════════════════════════════════════════════════════════════
// DiscoveryDaemon
// ══════════════════════════════════════════════════════════════════════════════

/// 后台快照聚合守护进程。
///
/// 内部持有四个 kube-runtime reflector 的本地状态存储（store），
/// 在接收到任意 reflector 更新信号后以 debounce 策略重新聚合 [`MetadataSnapshot`]。
pub(super) struct DiscoveryDaemon {
    kube_client: kube::Client,
    /// 本 pod 信息，用于确定 watch 的 namespace
    pod_info: PodInfo,
    cancel_token: CancellationToken,
}

impl DiscoveryDaemon {
    pub(super) fn new(
        kube_client: kube::Client,
        pod_info: PodInfo,
        cancel_token: CancellationToken,
    ) -> Self {
        Self { kube_client, pod_info, cancel_token }
    }

    /// 在 tokio 后台 spawn daemon，将快照更新推送到 `watch_tx`。
    pub(super) fn spawn(
        self,
        watch_tx: watch::Sender<Arc<MetadataSnapshot>>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            self.run(watch_tx).await;
        })
    }

    /// Daemon 主循环。
    ///
    /// 流程：
    /// 1. 创建共享 `Arc<Notify>` 信号，作为"有资源变更"的通知机制
    /// 2. 为 EndpointSlice / Service / ConfigMap / Lease 各启动一个 reflector task，
    ///    每次收到更新时调用 `notify.notify_one()`
    /// 3. 主循环通过 `select!` 监听：
    ///    - `notify.notified()` → sleep debounce → 排空额外通知 → `aggregate_snapshot()` →
    ///      `has_changes_from` 检查 → 有变化则 `watch_tx.send()`
    ///    - `cancel_token.cancelled()` → break，优雅退出
    async fn run(self, watch_tx: watch::Sender<Arc<MetadataSnapshot>>) {
        let notify = Arc::new(tokio::sync::Notify::new());
        let ns = self.pod_info.pod_namespace.clone();

        // ── 启动四路 reflector（kube-runtime list/watch + 本地 store）──────────
        //
        // 此处为架构占位，具体 reflector 配置在实现 k8s 集成时填充。
        // 每个 reflector 的 for_each 回调中调用 notify.notify_one()。
        //
        // 示例结构（以 EndpointSlice 为例）：
        //
        //   let (endpoint_slice_reader, endpoint_slice_writer) =
        //       kube::runtime::reflector::store();
        //   let endpoint_slice_stream = kube::runtime::watcher(
        //       kube::Api::<k8s_openapi::api::discovery::v1::EndpointSlice>::namespaced(
        //           self.kube_client.clone(), &ns,
        //       ),
        //       kube::runtime::watcher::Config::default()
        //           .labels("pagoda.io/managed=true"),
        //   );
        //   let reflect_task = kube::runtime::reflector(
        //       endpoint_slice_writer, endpoint_slice_stream
        //   );
        //   let notify_clone = notify.clone();
        //   tokio::spawn(async move {
        //       reflect_task
        //           .for_each(|_| { notify_clone.notify_one(); async {} })
        //           .await;
        //   });
        //
        // Service / ConfigMap / Lease reflector 类似。
        //
        // TODO: 在 kube 集成实现时，替换以下 todo!() 为实际 reflector 启动代码。
        let _ = ns; // suppress unused warning until reflectors are implemented

        let mut prev_snapshot = MetadataSnapshot::empty();
        let mut sequence: u64 = 0;

        loop {
            tokio::select! {
                _ = self.cancel_token.cancelled() => {
                    tracing::info!("DiscoveryDaemon: cancel_token triggered, shutting down");
                    break;
                }
                _ = notify.notified() => {
                    // debounce：等待静默期，批量合并高频事件
                    tokio::time::sleep(DEBOUNCE_DURATION).await;

                    // 排空 debounce 期间积压的额外通知（防止立即再次触发）
                    let deadline = tokio::time::Instant::now()
                        + Duration::from_millis(1);
                    loop {
                        match tokio::time::timeout_at(deadline, notify.notified()).await {
                            Ok(_) => continue,
                            Err(_) => break,
                        }
                    }

                    sequence += 1;
                    let snapshot = self.aggregate_snapshot(sequence).await;

                    if snapshot.has_changes_from(&prev_snapshot) {
                        if watch_tx.send(Arc::new(snapshot.clone())).is_err() {
                            // 所有 receiver 已 drop，退出 daemon
                            tracing::warn!("DiscoveryDaemon: all receivers dropped, exiting");
                            break;
                        }
                        prev_snapshot = snapshot;
                    }
                }
            }
        }
    }

    /// 从各 reflector 的本地 store 聚合出集群全局快照。
    ///
    /// 聚合逻辑：
    /// 1. 从 EndpointSlice store 提取所有 ready 端点，得到 (instance_id, pod_name) 列表
    /// 2. 从 Service store 提取服务入口信息
    /// 3. 从 ConfigMap store 重建 Model 实例
    /// 4. 从 Lease store 重建 EventChannel 实例
    /// 5. 遍历 ready pod 列表：若同时具备对应原生对象，加入快照；否则跳过（注册延迟，下次重试）
    async fn aggregate_snapshot(&self, sequence: u64) -> MetadataSnapshot {
        // TODO: 从各 reflector store 读取并聚合
        // 目前返回空快照（reflector 未实现时的占位）
        //
        // 正式实现中，此处应：
        //   let endpoint_slices = endpoint_slice_reader.state();
        //   let services = service_reader.state();
        //   let config_maps = config_map_reader.state();
        //   let leases = lease_reader.state();
        //   ... aggregate logic ...
        MetadataSnapshot {
            instances: std::collections::HashMap::new(),
            generations: std::collections::HashMap::new(),
            sequence,
            timestamp: std::time::Instant::now(),
        }
    }
}

impl std::fmt::Debug for DiscoveryDaemon {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiscoveryDaemon")
            .field("pod_namespace", &self.pod_info.pod_namespace)
            .finish()
    }
}
