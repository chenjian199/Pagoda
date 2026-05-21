// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 服务发现与负载均衡客户端。

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use dashmap::DashMap;
use tokio::sync::watch;

use crate::servicegroup::{Instance, PortName};

const DEFAULT_RECONCILE_INTERVAL: Duration = Duration::from_secs(5);

/// 路由占用状态：追踪 per-instance 的 in-flight 请求数。
#[derive(Debug, Default)]
pub struct RoutingOccupancyState {
    counts: DashMap<u64, std::sync::atomic::AtomicU64>,
    exact_selection_lock: tokio::sync::Mutex<()>,
}

impl RoutingOccupancyState {
    pub(crate) fn increment(&self, instance_id: u64) {
        self.counts
            .entry(instance_id)
            .or_default()
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) async fn select_exact_min_and_increment(&self, instance_ids: &[u64]) -> Option<u64> {
        let _guard = self.exact_selection_lock.lock().await;
        let min_id = instance_ids
            .iter()
            .min_by_key(|&&id| self.load(id))
            .copied()?;
        self.increment(min_id);
        Some(min_id)
    }

    pub(crate) fn decrement(&self, instance_id: u64) {
        if let Some(counter) = self.counts.get(&instance_id) {
            counter.fetch_update(
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |v| Some(v.saturating_sub(1)),
            ).ok();
        }
    }

    pub(crate) fn load(&self, instance_id: u64) -> u64 {
        self.counts
            .get(&instance_id)
            .map(|c| c.load(std::sync::atomic::Ordering::Relaxed))
            .unwrap_or(0)
    }

    pub(crate) fn retain(&self, instance_ids: &[u64]) {
        self.counts.retain(|id, _| instance_ids.contains(id));
    }
}

/// 获取或创建进程内共享的路由占用状态。
pub(crate) async fn get_or_create_routing_occupancy_state(
    portname: &PortName,
) -> Arc<RoutingOccupancyState> {
    use crate::traits::DistributedRuntimeProvider;
    let drt = portname.servicegroup().drt();
    let mut map = drt.routing_occupancy_states().lock().await;
    // 尝试从 Weak 升级
    if let Some(weak) = map.get(portname) {
        if let Some(strong) = weak.upgrade() {
            return strong;
        }
    }
    // 创建新的并以 Weak 存入 map
    let state = Arc::new(RoutingOccupancyState::default());
    map.insert(portname.clone(), Arc::downgrade(&state));
    state
}

/// 服务发现与负载均衡客户端。
///
/// 维护三套实例视图：
/// - `instance_source`: 权威实例列表（发现层）
/// - `instance_avail`: 当前可路由实例 ID（ArcSwap 无锁读）
/// - `instance_free`: 排除过载后的空闲实例 ID
#[derive(Clone, Debug)]
pub struct Client {
    pub portname: PortName,
    pub instance_source: Arc<watch::Receiver<Vec<Instance>>>,
    instance_avail: Arc<ArcSwap<Vec<u64>>>,
    instance_free: Arc<ArcSwap<Vec<u64>>>,
    instance_avail_tx: Arc<watch::Sender<Vec<u64>>>,
    instance_avail_rx: watch::Receiver<Vec<u64>>,
    reconcile_interval: Duration,
}

impl Client {
    pub(crate) async fn new(portname: PortName) -> anyhow::Result<Self> {
        Self::with_reconcile_interval(portname, DEFAULT_RECONCILE_INTERVAL).await
    }

    pub(crate) async fn with_reconcile_interval(
        portname: PortName,
        interval: Duration,
    ) -> anyhow::Result<Self> {
        let instance_source = Self::get_or_create_dynamic_instance_source(&portname).await?;

        let initial_ids: Vec<u64> = instance_source
            .borrow()
            .iter()
            .map(|inst| inst.id())
            .collect();

        let (avail_tx, avail_rx) = watch::channel(initial_ids.clone());

        let client = Self {
            portname,
            instance_source,
            instance_avail: Arc::new(ArcSwap::new(Arc::new(initial_ids.clone()))),
            instance_free: Arc::new(ArcSwap::new(Arc::new(initial_ids))),
            instance_avail_tx: Arc::new(avail_tx),
            instance_avail_rx: avail_rx,
            reconcile_interval: interval,
        };

        client.monitor_instance_source();
        Ok(client)
    }

    pub fn instances(&self) -> Vec<Instance> {
        self.instance_source.borrow().clone()
    }

    pub fn instance_ids(&self) -> Vec<u64> {
        self.instance_source.borrow().iter().map(|i| i.id()).collect()
    }

    pub fn instance_ids_avail(&self) -> arc_swap::Guard<Arc<Vec<u64>>> {
        self.instance_avail.load()
    }

    pub fn instance_ids_free(&self) -> arc_swap::Guard<Arc<Vec<u64>>> {
        self.instance_free.load()
    }

    pub fn instance_avail_watcher(&self) -> watch::Receiver<Vec<u64>> {
        self.instance_avail_rx.clone()
    }

    /// 阻塞直到至少有一个实例可见。
    pub async fn wait_for_instances(&self) -> anyhow::Result<Vec<Instance>> {
        let mut rx = (*self.instance_source).clone();
        loop {
            let instances = rx.borrow_and_update().clone();
            if !instances.is_empty() {
                return Ok(instances);
            }
            rx.changed().await?;
        }
    }

    /// 本地观察到实例不可用，临时剔除（reconcile 后可恢复）。
    pub fn report_instance_down(&self, instance_id: u64) {
        let filtered: Vec<u64> = self
            .instance_avail
            .load()
            .iter()
            .copied()
            .filter(|&id| id != instance_id)
            .collect();
        self.instance_avail.store(Arc::new(filtered.clone()));
        let _ = self.instance_avail_tx.send(filtered);
    }

    /// 更新 busy 过滤视图。
    pub fn update_free_instances(&self, busy_instance_ids: &[u64]) {
        let free: Vec<u64> = self
            .instance_ids()
            .into_iter()
            .filter(|id| !busy_instance_ids.contains(id))
            .collect();
        self.instance_free.store(Arc::new(free));
    }

    fn monitor_instance_source(&self) {
        let client = self.clone();
        tokio::spawn(async move {
            let mut rx = (*client.instance_source).clone();
            loop {
                // 等待实例列表变化
                if rx.changed().await.is_err() {
                    break; // sender dropped — discovery stream ended
                }
                let instances = rx.borrow_and_update().clone();
                let ids: Vec<u64> = instances.iter().map(|i| i.id()).collect();

                // 同步 instance_avail（保留仍在列表中的实例）
                let current_avail: Vec<u64> = client.instance_avail.load()
                    .iter()
                    .copied()
                    .filter(|id| ids.contains(id))
                    .collect();
                // 加入新出现的实例
                let mut new_avail = current_avail;
                for id in &ids {
                    if !new_avail.contains(id) {
                        new_avail.push(*id);
                    }
                }
                client.instance_avail.store(Arc::new(new_avail.clone()));
                let _ = client.instance_avail_tx.send(new_avail.clone());

                // 同步 instance_free（保守：初始与 avail 相同，由外部策略覆盖）
                client.instance_free.store(Arc::new(new_avail.clone()));

                // 清理 RoutingOccupancyState 中已离线的计数
                let occupancy = get_or_create_routing_occupancy_state(&client.portname).await;
                occupancy.retain(&new_avail);
            }
        });
    }

    async fn get_or_create_dynamic_instance_source(
        portname: &PortName,
    ) -> anyhow::Result<Arc<watch::Receiver<Vec<Instance>>>> {
        use crate::discovery::{DiscoveryEvent, DiscoveryQuery};
        use crate::traits::DistributedRuntimeProvider;

        let drt = portname.servicegroup().drt();
        let mut map = drt.instance_sources().lock().await;

        // 尝试从 Weak 升级已有的 watch receiver
        if let Some(weak) = map.get(portname) {
            if let Some(strong) = weak.upgrade() {
                return Ok(strong);
            }
        }

        // 无缓存：向发现层发起 list_and_watch
        let id = portname.id();
        let query = DiscoveryQuery::PortName {
            namespace: id.namespace.clone(),
            servicegroup: id.servicegroup.clone(),
            portname: id.portname.clone(),
        };
        let cancel = drt.rt().child_token();
        let mut stream = drt.discovery().list_and_watch(query, Some(cancel)).await?;

        // 用 watch channel 桥接流式事件 → 实例列表
        let (tx, rx) = watch::channel::<Vec<Instance>>(Vec::new());
        let discovery = drt.discovery().clone();
        let ns = id.namespace.clone();
        let sg = id.servicegroup.clone();
        let pn_name = id.portname.clone();

        tokio::spawn(async move {
            use futures::StreamExt;
            while let Some(event) = stream.next().await {
                match event {
                    Ok(DiscoveryEvent::Added(di)) => {
                        // DiscoveryInstance::PortName 内部直接是 servicegroup::Instance
                        if let crate::discovery::DiscoveryInstance::PortName(instance) = di {
                            tx.send_modify(|list: &mut Vec<Instance>| {
                                if !list.iter().any(|i| i.id() == instance.id()) {
                                    list.push(instance.clone());
                                }
                            });
                        }
                    }
                    Ok(DiscoveryEvent::Removed(did)) => {
                        let removed_id = did.instance_id();
                        tx.send_modify(|list: &mut Vec<Instance>| {
                            list.retain(|i| i.id() != removed_id);
                        });
                    }
                    Err(e) => {
                        tracing::warn!(ns=%ns, sg=%sg, portname=%pn_name, "discovery stream error: {e}");
                    }
                }
                let _ = &discovery; // keep alive
            }
        });

        let arc_rx = Arc::new(rx);
        map.insert(portname.clone(), Arc::downgrade(&arc_rx));
        Ok(arc_rx)
    }
}

// ─── 全局实例快照查询 ─────────────────────────────────────────────────────────

/// 列出集群中所有活跃的 `PortName` 实例（全局一次性快照）。
///
/// 通过 `Discovery::list(AllPortNames)` 获取全量快照，过滤出
/// `DiscoveryInstance::PortName` 变体后转换为 `Instance`，按默认顺序排序返回。
///
/// 适用于运维工具（`pagoda ps`、监控仪表盘）等需要全局视图的场景。
/// 如需实时更新，应改用 `Discovery::list_and_watch()`。
pub async fn list_all_instances(
    discovery_client: std::sync::Arc<dyn crate::discovery::Discovery>,
) -> anyhow::Result<Vec<Instance>> {
    let discovery_instances = discovery_client
        .list(crate::discovery::DiscoveryQuery::AllPortNames)
        .await?;

    let mut instances: Vec<Instance> = discovery_instances
        .into_iter()
        .filter_map(|di| match di {
            crate::discovery::DiscoveryInstance::PortName(instance) => Some(instance),
            _ => None,
        })
        .collect();

    instances.sort();
    Ok(instances)
}
