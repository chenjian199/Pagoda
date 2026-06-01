// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `servicegroup::client` —— 端点客户端 + 路由占用计数
//!
//! ## 设计意图
//!
//! 本文件提供"调用方视角"下访问一个 [`PortName`] 所需的全部基础设
//! 施。其核心问题是：
//!
//! - **怎么知道端点在哪些 worker 上**：需要持续 watch 发现平面
//!   （etcd / kvstore），把"当前活着的实例集合"以
//!   `tokio::sync::watch<Vec<Instance>>` 形式提供给路由层。
//! - **怎么避免大量重复 watch**：一个进程里同时有多个 [`Client`] 监
//!   听同一个端点是常见情况；本文件把发现源做成"按端点共享"——多
//!   个 client 通过 [`PortNameDiscoverySource`] 复用同一个底层 watch
//!   流，控制平面 traffic 不会随 client 数量线性放大。
//! - **怎么实现"最少负载"路由**：所谓 least-loaded 需要进程级共享
//!   的 per-worker 在途计数。[`RoutingOccupancyState`] 用 `DashMap`
//!   实现 lock-free 计数；并用一个 `Mutex<()>` 实现"读最小 + 自增"
//!   两步合一的原子性，否则高并发下会出现多个请求读到相同最小值导
//!   致选择倾斜。
//! - **被 inhibit 的实例如何恢复**：`report_instance_down` 只是把某
//!   id 从 `instance_avail` 临时摘掉；监控 task 在固定周期会拿
//!   `instance_source` 的当前快照重新覆盖 `instance_avail`，确保短
//!   暂屏蔽不会变成永久屏蔽。
//!
//! ## 与外部契约的关系
//!
//! 下列项目对父模块及调用方公开，**签名 / 字段名 / 可见性保持不变**：
//!
//! - `pub struct Client { pub portname, pub instance_source, ... }`
//!   及其全部 `pub fn`；
//! - `pub(crate) struct RoutingOccupancyState` 及其 `pub(crate)` 方
//!   法；
//! - `pub(crate) struct PortNameDiscoverySource`；
//! - `pub(crate) async fn get_or_create_routing_occupancy_state`；
//! - 测试专用 `#[cfg(test)] pub(crate) fn override_instance_avail`。
//!
//!
//! ## 实现结构
//!
//! 按数据流方向自上而下：
//!
//! 1. 路由占用层（`RoutingOccupancyState`）：进程级 per-worker 计数；
//! 2. 占用注册表入口（`get_or_create_routing_occupancy_state`）：按端
//!    点共享；
//! 3. 发现源层（`PortNameDiscoverySource`）：合并 watch / event 双路；
//! 4. 客户端层（`Client`）：对外接口；
//! 5. 后台监控（`monitor_instance_source`）：周期对账 + 清理陈旧计数；
//! 6. 发现源构造（`get_or_create_dynamic_discovery_source`）：跨 client
//!    共享的 watch task。

use std::sync::atomic::{AtomicU64, Ordering};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use anyhow::Result;
use arc_swap::ArcSwap;
use dashmap::DashMap;
use futures::StreamExt;

use crate::servicegroup::{PortName, Instance};
use crate::discovery::{DiscoveryEvent, DiscoveryInstance, DiscoveryInstanceId};
use crate::traits::DistributedRuntimeProvider;

// ============================================================================
// 路由占用计数（per-worker in-flight）
// ============================================================================

/// 进程级共享状态：跟踪每个 worker 的"在途请求数"。
///
/// ## 选型说明
///
/// - `counts: DashMap<u64, AtomicU64>`：DashMap 内部用分片锁实现，
///   `entry` / `get` 是 lock-free 友好的；每个 worker 对应一个
///   `AtomicU64`，常态下 `Relaxed` 加减即可，不需要全局锁。
/// - `exact_selection_lock: Mutex<()>`：用于 [`Self::select_exact_min_and_increment`]，
///   把"读最小 + 自增"组合成原子操作。否则高并发下多个请求会同时
///   读到同一个最小值，命中同一个 worker，造成负载倾斜。
#[derive(Debug, Default)]
pub(crate) struct RoutingOccupancyState {
    counts: DashMap<u64, AtomicU64>,
    exact_selection_lock: tokio::sync::Mutex<()>,
}

impl RoutingOccupancyState {
    /// 将给定 worker 的在途计数 +1。
    ///
    /// ## 入参
    /// - `instance_id`：worker / 实例 ID。
    ///
    /// ## 行为
    /// - 若条目不存在，先插入一个 `AtomicU64(0)`；
    /// - 然后用 `fetch_add(1, Relaxed)` 自增。
    pub(crate) fn increment(&self, instance_id: u64) {
        self.counts
            .entry(instance_id)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    /// "选最小并自增"——least-loaded 路由策略的核心原语。
    ///
    /// ## 入参
    /// - `instance_ids`：候选 worker 集合。空切片返回 `None`。
    ///
    /// ## 出参
    /// - `Some(id)`：被选中的 worker；
    /// - `None`：候选集合为空。
    ///
    /// ## 实现要点
    ///
    /// 先获取 `exact_selection_lock`，保证"按当前负载读最小 + 给被
    /// 选中的 id 自增 1"这两步对其它并发调用是不可分割的；否则两个
    /// 并发请求会同时看到同样的最小值，结果都打到同一个 worker，造
    /// 成"惊群"。
    pub(crate) async fn select_exact_min_and_increment(&self, instance_ids: &[u64]) -> Option<u64> {
        let _guard = self.exact_selection_lock.lock().await;
        let chosen = *instance_ids.iter().min_by_key(|&&id| self.load(id))?;
        self.increment(chosen);
        Some(chosen)
    }

    /// 将给定 worker 的在途计数 -1（带下溢保护）。
    ///
    /// 若条目不存在则 no-op。用 `saturating_sub` 防止变成 `u64::MAX`，
    /// 后者会让该 worker 在 least-loaded 视角下永远是负载最低的，进
    /// 而吸走全部请求。
    pub(crate) fn decrement(&self, instance_id: u64) {
        if let Some(count) = self.counts.get(&instance_id) {
            let _ = count.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(1))
            });
        }
    }

    /// 读取给定 worker 当前在途计数；不存在返回 0。
    pub(crate) fn load(&self, instance_id: u64) -> u64 {
        self.counts
            .get(&instance_id)
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// 把不在存活列表中的 worker 计数条目清除。
    ///
    /// 监控 task 在每次 instance_source 变化或对账周期到达时调用，
    /// 防止已下线 worker 的陈旧计数继续影响路由决策。
    pub(crate) fn retain(&self, instance_ids: &[u64]) {
        let live: HashSet<u64> = instance_ids.iter().copied().collect();
        self.counts.retain(|id, _| live.contains(id));
    }
}

/// 按端点共享的 `RoutingOccupancyState`。
///
/// 多个 [`Client`] 持有同一个端点时，需要共享同一个计数器；这是
/// least-loaded 决策的全局视图前提。
///
/// ## 实现
///
/// - DRT 持有 `routing_occupancy_states()`：一张
///   `Map<PortName, Weak<RoutingOccupancyState>>`；
/// - 命中且能 upgrade → 直接返回；
/// - 否则新建一个 `Arc<...>`，把 Weak 写回注册表，再返回 Arc。
///
/// 用 `Weak` 让没有任何 client 持有的端点状态能被释放，避免内存泄露。
pub(crate) async fn get_or_create_routing_occupancy_state(
    portname: &PortName,
) -> Arc<RoutingOccupancyState> {
    let drt = portname.drt();
    let registry = drt.routing_occupancy_states();
    let mut registry = registry.lock().await;

    // 命中且未被释放 → 复用
    if let Some(weak) = registry.get(portname) {
        if let Some(state) = weak.upgrade() {
            return state;
        }
        // 已被释放：先擦掉过期 Weak
        registry.remove(portname);
    }

    let state = Arc::new(RoutingOccupancyState::default());
    registry.insert(portname.clone(), Arc::downgrade(&state));
    state
}

// ============================================================================
// 后台监控相关常量
// ============================================================================

/// 监控 task 周期对账 `instance_avail` 与 `instance_source` 的间隔。
///
/// 设置得不能太短（无谓 CPU 开销）也不能太长（被 `report_instance_down`
/// 临时屏蔽的实例迟迟不恢复）。5s 是经验值。
const DEFAULT_RECONCILE_INTERVAL: Duration = Duration::from_secs(5);

// ============================================================================
// 发现源（按端点共享的 watch + event 双路）
// ============================================================================

/// 一个端点的"共享发现源"。
///
/// 同一进程里多个 [`Client`] 监听同一端点时，本结构让它们复用同一份
/// 底层 watch task，避免 N 个 client → N 个 etcd watch。
///
/// ## 字段
///
/// - `instance_source`：合并去重后的"当前实例列表"快照。路由器只关
///   心存在 / 不存在，不关心瞬时事件。
/// - `event_subscribers`：原始事件流广播。响应取消等关键路径必须看
///   到每一次 Removed，coalesce 后的 watch 满足不了，所以另开一条。
#[derive(Debug)]
pub(crate) struct PortNameDiscoverySource {
    instance_source: tokio::sync::watch::Receiver<Vec<Instance>>,
    event_subscribers: StdMutex<Vec<tokio::sync::mpsc::UnboundedSender<DiscoveryEvent>>>,
}

impl PortNameDiscoverySource {
    /// 构造一个新的发现源。`instance_source` 由调用方传入，通常是
    /// `watch::channel(vec![]).1`。
    fn new(instance_source: tokio::sync::watch::Receiver<Vec<Instance>>) -> Self {
        Self {
            instance_source,
            event_subscribers: StdMutex::new(Vec::new()),
        }
    }

    /// 取一份 watch::Receiver 的 clone，供 [`Client::instance_source`] 持
    /// 有。每个 client 都拿独立 receiver 以避免互相干扰。
    fn instance_receiver(&self) -> tokio::sync::watch::Receiver<Vec<Instance>> {
        self.instance_source.clone()
    }

    /// 订阅原始事件流。返回的 `UnboundedReceiver` 关闭时由
    /// `broadcast_event` 的 `retain` 自动清理。
    fn subscribe_events(&self) -> tokio::sync::mpsc::UnboundedReceiver<DiscoveryEvent> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        self.event_subscribers.lock().unwrap().push(tx);
        rx
    }

    /// 向所有活跃订阅者广播一个事件。已关闭的订阅者会被自动清除。
    fn broadcast_event(&self, event: &DiscoveryEvent) {
        let subscribers = &mut *self.event_subscribers.lock().unwrap();
        subscribers.retain(|tx| tx.send(event.clone()).is_ok());
    }
}

// ============================================================================
// Client：端点的调用方句柄
// ============================================================================

/// 端点客户端：把"发现 + 可用列表 + 路由占用"打包成调用方可直接使
/// 用的对象。
///
/// ## 字段语义
///
/// - `portname`：本 client 所代表的端点身份；
/// - `portname_discovery_source`：共享发现源（多 client 复用）；
/// - `instance_source`：当前实例列表快照（去重 / 合并后）；
/// - `instance_avail`：当前可用实例 ID（被 `report_instance_down` 临
///   时摘掉的实例不在内）；
/// - `instance_free`：当前空闲实例 ID（busy 之外的）；
/// - `instance_avail_tx/rx`：把 `instance_avail` 的变化广播给外部
///   subscriber；
/// - `reconcile_interval`：周期对账 `instance_avail` ↔ `instance_source`
///   的间隔。
#[derive(Clone, Debug)]
pub struct Client {
    // This is me
    pub portname: PortName,
    // Shared portname discovery source backing both snapshots and raw events.
    portname_discovery_source: Arc<PortNameDiscoverySource>,
    // These are the remotes I know about from watching key-value store
    pub instance_source: Arc<tokio::sync::watch::Receiver<Vec<Instance>>>,
    // These are the instance source ids less those reported as down from sending rpc
    instance_avail: Arc<ArcSwap<Vec<u64>>>,
    // These are the instance source ids less those reported as busy (above threshold)
    instance_free: Arc<ArcSwap<Vec<u64>>>,
    // Watch sender for available instance IDs (for sending updates)
    instance_avail_tx: Arc<tokio::sync::watch::Sender<Vec<u64>>>,
    // Watch receiver for available instance IDs (for cloning to external subscribers)
    instance_avail_rx: tokio::sync::watch::Receiver<Vec<u64>>,
    /// Interval for periodic reconciliation of instance_avail with instance_source.
    /// This ensures instances removed via `report_instance_down` are eventually restored.
    reconcile_interval: Duration,
}

impl Client {
    // ----- 构造 -----

    /// 默认构造路径：使用 [`DEFAULT_RECONCILE_INTERVAL`] 作为对账周期。
    ///
    /// 由父模块 `PortName::client()` 间接调用。
    pub(crate) async fn new(portname: PortName) -> Result<Self> {
        Self::with_reconcile_interval(portname, DEFAULT_RECONCILE_INTERVAL).await
    }

    /// 自定义对账周期的构造路径，主要供单元测试加速对账。
    ///
    /// ## 流程
    ///
    /// 1. 取/造一个共享发现源；
    /// 2. 取 `instance_source` 的当前快照，把所有实例 ID 同时灌进
    ///    `instance_avail` 与 `instance_free`，避免 `wait_for_instances`
    ///    刚返回路由方法就读到空列表；
    /// 3. 创建 watch channel 用于通知外部 subscriber；
    /// 4. 启动 `monitor_instance_source` 后台 task。
    pub(crate) async fn with_reconcile_interval(
        portname: PortName,
        reconcile_interval: Duration,
    ) -> Result<Self> {
        tracing::trace!(
            "Client::new_dynamic: Creating dynamic client for portname: {}",
            portname.id(),
        );
        let portname_discovery_source =
            Self::get_or_create_dynamic_discovery_source(&portname).await?;
        let instance_source = Arc::new(portname_discovery_source.instance_receiver());

        // 把初始快照同时灌给 avail/free，防止 wait_for_instances 返回
        // 后路由方法（random / round_robin / ...）读到空列表。
        let initial_ids: Vec<u64> = instance_source
            .borrow()
            .iter()
            .map(|instance| instance.id())
            .collect();

        let (avail_tx, avail_rx) = tokio::sync::watch::channel(initial_ids.clone());

        let client = Client {
            portname: portname.clone(),
            portname_discovery_source,
            instance_source: instance_source.clone(),
            instance_avail: Arc::new(ArcSwap::from(Arc::new(initial_ids.clone()))),
            instance_free: Arc::new(ArcSwap::from(Arc::new(initial_ids))),
            instance_avail_tx: Arc::new(avail_tx),
            instance_avail_rx: avail_rx,
            reconcile_interval,
        };
        client.monitor_instance_source();
        Ok(client)
    }

    // ----- 只读查询 -----

    /// 取当前实例列表（去重合并后）。返回 owned `Vec<Instance>`，
    /// 调用方可放心修改 / 转移。
    pub fn instances(&self) -> Vec<Instance> {
        self.instance_source.borrow().clone()
    }

    /// 取当前实例 ID 列表。
    pub fn instance_ids(&self) -> Vec<u64> {
        self.instances().into_iter().map(|ep| ep.id()).collect()
    }

    /// 当前可用 ID 集合（`ArcSwap` guard）。
    pub fn instance_ids_avail(&self) -> arc_swap::Guard<Arc<Vec<u64>>> {
        self.instance_avail.load()
    }

    /// 当前空闲 ID 集合（`ArcSwap` guard）。
    pub fn instance_ids_free(&self) -> arc_swap::Guard<Arc<Vec<u64>>> {
        self.instance_free.load()
    }

    /// 拿一个"可用 ID 列表"的 watch::Receiver，供外部 subscribe 状态变化。
    pub fn instance_avail_watcher(&self) -> tokio::sync::watch::Receiver<Vec<u64>> {
        self.instance_avail_rx.clone()
    }

    /// 订阅本端点的"原始发现事件流"。与 `instance_source` 不同，本流
    /// 不会把 Remove→Add 合并掉，因此响应取消逻辑可以可靠地观察每
    /// 一次 Removed。
    pub(crate) fn subscribe_discovery_events(
        &self,
    ) -> tokio::sync::mpsc::UnboundedReceiver<DiscoveryEvent> {
        self.portname_discovery_source.subscribe_events()
    }

    // ----- 阻塞 / 主动写 -----

    /// 等待至少有一个实例可用。
    ///
    /// ## 行为
    ///
    /// 循环读 `instance_source.borrow_and_update()`：非空就返回快照；
    /// 否则 `await rx.changed()` 等下一次变化。
    pub async fn wait_for_instances(&self) -> Result<Vec<Instance>> {
        tracing::trace!(
            "wait_for_instances: Starting wait for portname: {}",
            self.portname.id(),
        );
        let mut rx = self.instance_source.as_ref().clone();
        loop {
            let instances = rx.borrow_and_update().to_vec();
            if !instances.is_empty() {
                tracing::info!(
                    "wait_for_instances: Found {} instance(s) for portname: {}",
                    instances.len(),
                    self.portname.id(),
                );
                return Ok(instances);
            }
            rx.changed().await?;
        }
    }

    /// 临时把某实例从 `instance_avail` 中摘掉。
    ///
    /// 会触发 watch channel 通知外部 subscriber；但不会立刻清除
    /// occupancy 计数——后者由后台监控 task 在 retain 时统一处理。
    ///
    /// 屏蔽是临时的：`reconcile_interval` 到期后，监控 task 会用
    /// `instance_source` 的当前快照重新覆盖 `instance_avail`，从而
    /// 恢复这个实例（除非它真的从发现平面消失）。
    pub fn report_instance_down(&self, instance_id: u64) {
        let filtered = self
            .instance_ids_avail()
            .iter()
            .copied()
            .filter(|&id| id != instance_id)
            .collect::<Vec<_>>();
        self.instance_avail.store(Arc::new(filtered.clone()));
        let _ = self.instance_avail_tx.send(filtered);
        tracing::debug!("inhibiting instance {instance_id}");
    }

    /// 根据"繁忙列表"重新计算空闲列表：`instance_free = instances - busy`。
    ///
    /// 注意基准是 `self.instances()`（即 `instance_source` 的快照），
    /// 而非 `instance_avail`——这是有意为之，让"繁忙判定"独立于
    /// `report_instance_down` 的临时屏蔽。
    pub fn update_free_instances(&self, busy_instance_ids: &[u64]) {
        let all = self.instance_ids();
        let free: Vec<u64> = all
            .into_iter()
            .filter(|id| !busy_instance_ids.contains(id))
            .collect();
        self.instance_free.store(Arc::new(free));
    }

    // ----- 测试钩子 -----

    /// **仅测试用**：直接覆盖 `instance_avail`，模拟"实例列表"与
    /// "可用列表"之间的不一致（构造 race-condition 场景）。
    #[cfg(test)]
    pub(crate) fn override_instance_avail(&self, ids: Vec<u64>) {
        self.instance_avail.store(Arc::new(ids));
    }

    // ----- 后台监控 -----

    /// 启动后台监控 task，负责：
    ///
    /// 1. 监听 `instance_source` 变化，把最新的 ID 列表同步进
    ///    `instance_avail` 和 `instance_free`；
    /// 2. 清理 `RoutingOccupancyState` 中已下线 worker 的陈旧计数；
    /// 3. 通过 `instance_avail_tx` 通知所有外部 subscriber；
    /// 4. 在 `reconcile_interval` 周期触发对账，确保被
    ///    `report_instance_down` 临时屏蔽的实例最终能被恢复。
    fn monitor_instance_source(&self) {
        let reconcile_interval = self.reconcile_interval;
        let cancel_token = self.portname.drt().primary_token();
        let client = self.clone();
        let portname_id = self.portname.id();

        tokio::task::spawn(async move {
            let mut rx = client.instance_source.as_ref().clone();
            while !cancel_token.is_cancelled() {
                let instance_ids: Vec<u64> = rx
                    .borrow_and_update()
                    .iter()
                    .map(|i| i.id())
                    .collect();

                // 同步更新 avail/free（TODO: 未来可细分为各自维护）。
                client.instance_avail.store(Arc::new(instance_ids.clone()));
                client.instance_free.store(Arc::new(instance_ids.clone()));

                // 顺便清理陈旧 occupancy 计数。try_lock 拿不到锁就略过，
                // 下一次循环还会再尝试。
                let registry = client.portname.drt().routing_occupancy_states();
                if let Ok(registry) = registry.try_lock()
                    && let Some(weak) = registry.get(&client.portname)
                    && let Some(state) = weak.upgrade()
                {
                    state.retain(&instance_ids);
                }

                // 通知外部 subscriber。
                let _ = client.instance_avail_tx.send(instance_ids);

                tokio::select! {
                    result = rx.changed() => {
                        if let Err(err) = result {
                            tracing::error!(
                                "monitor_instance_source: The Sender is dropped: {err}, portname={portname_id}",
                            );
                            cancel_token.cancel();
                        }
                    }
                    _ = tokio::time::sleep(reconcile_interval) => {
                        tracing::trace!(
                            "monitor_instance_source: periodic reconciliation for portname={portname_id}",
                        );
                    }
                }
            }
        });
    }

    // ----- 发现源构造 -----

    /// 取/造一个跨 client 共享的 `PortNameDiscoverySource`。
    ///
    /// ## 流程
    ///
    /// 1. 在 DRT 持有的 `portname_discovery_sources` 表上加锁；
    /// 2. 命中且未被释放 → 直接复用；命中但 upgrade 失败 → 擦除过期
    ///    Weak；
    /// 3. 调 `discovery.list_and_watch(...)` 拿到事件流；
    /// 4. 创建一个 `watch::channel` 与 `PortNameDiscoverySource`；
    /// 5. 起后台 task：把每个 `DiscoveryEvent` 既广播给原始订阅者，
    ///    又合入 `map: HashMap<instance_id, Instance>`，最后把 map 的
    ///    values 推到 watch channel；
    /// 6. 把 Weak 写回注册表，返回 Arc。
    async fn get_or_create_dynamic_discovery_source(
        portname: &PortName,
    ) -> Result<Arc<PortNameDiscoverySource>> {
        let drt = portname.drt();
        let sources = drt.portname_discovery_sources();
        let mut sources = sources.lock().await;

        // 命中且未被释放
        if let Some(source) = sources.get(portname) {
            if let Some(source) = source.upgrade() {
                return Ok(source);
            }
            sources.remove(portname);
        }

        let discovery = drt.discovery();
        let discovery_query = crate::discovery::DiscoveryQuery::PortName {
            namespace: portname.servicegroup.namespace.name.clone(),
            servicegroup: portname.servicegroup.name.clone(),
            portname: portname.name.clone(),
        };

        let mut discovery_stream = discovery
            .list_and_watch(discovery_query.clone(), None)
            .await?;
        let (watch_tx, watch_rx) = tokio::sync::watch::channel(vec![]);
        let discovery_source = Arc::new(PortNameDiscoverySource::new(watch_rx));

        // 后台 task 跑在 secondary runtime 上，避免阻塞主调用栈。
        let secondary = portname.servicegroup.drt.runtime().secondary().clone();
        let source_for_task = discovery_source.clone();

        secondary.spawn(async move {
            tracing::trace!(
                "portname_watcher: Starting for discovery query: {:?}",
                discovery_query,
            );
            let mut map: HashMap<u64, Instance> = HashMap::new();

            loop {
                // 同时监听：watch_tx 关闭 / discovery_stream 新事件
                let event = tokio::select! {
                    _ = watch_tx.closed() => break,
                    next = discovery_stream.next() => match next {
                        Some(Ok(ev)) => ev,
                        Some(Err(e)) => {
                            tracing::error!(
                                "portname_watcher: discovery stream error: {}; shutting down for discovery query: {:?}",
                                e, discovery_query,
                            );
                            break;
                        }
                        None => break,
                    },
                };

                // 原始事件先广播一次，让 `subscribe_discovery_events`
                // 的订阅者能看到未合并的状态。
                source_for_task.broadcast_event(&event);

                // 再把事件合入 map：Added 覆盖、Removed 删除。
                match event {
                    DiscoveryEvent::Added(DiscoveryInstance::PortName(instance)) => {
                        map.insert(instance.instance_id, instance);
                    }
                    DiscoveryEvent::Added(_) => {}
                    DiscoveryEvent::Removed(DiscoveryInstanceId::PortName(portname_id)) => {
                        map.remove(&portname_id.instance_id);
                    }
                    DiscoveryEvent::Removed(_) => {}
                }

                // 把当前快照推给 watch channel；失败说明没人在听了。
                let instances: Vec<Instance> = map.values().cloned().collect();
                if watch_tx.send(instances).is_err() {
                    break;
                }
            }
            // 收尾：让所有 watch::Receiver 看到一个"空列表"，避免
            // 调用方继续把请求路由到已经停止 watch 的端点上。
            let _ = watch_tx.send(vec![]);
        });

        sources.insert(portname.clone(), Arc::downgrade(&discovery_source));
        Ok(discovery_source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DistributedRuntime, Runtime, distributed::DistributedConfig};

    // ── RoutingOccupancyState 单元测试 ──────────────────────────────────────

    /// 测试场景：基本的 increment / load / decrement / 下溢保护 完整流程。
    ///
    /// ## 测试过程
    ///
    /// 1. **初始状态**：对未注册的 worker_id=1 调用 `load`，应返回 0
    /// 2. **单次递增**：调用 `increment(1)` 后 `load(1)` 应返回 1
    /// 3. **再次递增**：再调用 `increment(1)` 后 `load(1)` 应返回 2
    /// 4. **正常递减**：调用 `decrement(1)` 后 `load(1)` 应返回 1
    /// 5. **下溢保护**：连续调用 `decrement(1)` 两次（计数已为 1），
    ///    第一次降至 0，第二次不应下溢（仍为 0）
    ///
    /// ## 意义
    ///
    /// 验证基本的增减操作及 `saturating_sub` 下溢保护，防止计数器变为 `u64::MAX`
    /// 导致该 worker 被永远排除在路由之外。
    #[tokio::test]
    async fn test_occupancy_basic_operations() {
        let state = RoutingOccupancyState::default();

        // 初始计数为 0
        assert_eq!(state.load(1), 0);

        // 递增后计数变为 1
        state.increment(1);
        assert_eq!(state.load(1), 1);

        // 再次递增变为 2
        state.increment(1);
        assert_eq!(state.load(1), 2);

        // 递减后变为 1
        state.decrement(1);
        assert_eq!(state.load(1), 1);

        // 递减到 0 后不下溢
        state.decrement(1);
        state.decrement(1); // 多余的 decrement 不应导致下溢
        assert_eq!(state.load(1), 0);
    }

    /// 测试场景：retain 应清除不在存活列表中的 worker 计数条目。
    ///
    /// ## 测试过程
    ///
    /// 1. 对 worker_id 10、20、30 各 increment 一次（计数均为 1）
    /// 2. 调用 `retain(&[10, 30])`（20 不在存活列表中）
    /// 3. 断言 `load(10) == 1`（保留）
    /// 4. 断言 `load(20) == 0`（已被清除，load 返回默认值 0）
    /// 5. 断言 `load(30) == 1`（保留）
    ///
    /// ## 意义
    ///
    /// 验证实例下线时 retain 能正确清理陈旧计数，防止已下线 worker 的高计数值
    /// 影响后续路由决策（被选中概率本应为零，但因陈旧计数可能被误判为低负载）。
    #[tokio::test]
    async fn test_occupancy_retain() {
        let state = RoutingOccupancyState::default();
        state.increment(10);
        state.increment(20);
        state.increment(30);

        state.retain(&[10, 30]);

        assert_eq!(state.load(10), 1);
        assert_eq!(state.load(20), 0); // 已被清除
        assert_eq!(state.load(30), 1);
    }

    /// 测试场景：90 个并发请求应在 3 个 worker 之间均匀分配（每个 30 次）。
    ///
    /// ## 测试过程
    ///
    /// 1. 创建 3 个候选 worker ID（100、200、300）
    /// 2. 并发启动 90 个 tokio 任务，每个任务调用 `select_exact_min_and_increment`
    /// 3. 等待所有任务完成
    /// 4. 断言每个 worker 各被选中 30 次（负载均衡）
    ///
    /// ## 意义
    ///
    /// 验证 `exact_selection_lock` 的互斥语义确保了真正的 least-loaded 分发，
    /// 而非多个并发请求同时读到相同最小值导致的倾斜分配。
    #[tokio::test]
    async fn test_select_exact_min_and_increment_balances() {
        let state = Arc::new(RoutingOccupancyState::default());
        let ids = vec![100u64, 200, 300];
        let total = 90usize;

        let mut handles = Vec::with_capacity(total);
        for _ in 0..total {
            let s = state.clone();
            let i = ids.clone();
            handles.push(tokio::spawn(async move {
                s.select_exact_min_and_increment(&i).await
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        // 90 请求均匀分布到 3 个 worker，每个应得 30
        assert_eq!(state.load(100), 30);
        assert_eq!(state.load(200), 30);
        assert_eq!(state.load(300), 30);
    }

    /// 测试场景：传入空候选列表时，select_exact_min_and_increment 应返回 None。
    ///
    /// ## 测试过程
    ///
    /// 1. 创建默认状态
    /// 2. 以空切片 `&[]` 调用 `select_exact_min_and_increment`
    /// 3. 断言返回 `None`
    ///
    /// ## 意义
    ///
    /// 路由层在无可用实例时调用此方法，None 返回使路由层可以进入等待逻辑，
    /// 而非 panic 或返回错误，保证系统鲁棒性。
    #[tokio::test]
    async fn test_select_exact_min_empty_list() {
        let state = RoutingOccupancyState::default();
        let result = state.select_exact_min_and_increment(&[]).await;
        assert!(result.is_none());
    }

    // ── Client 集成测试（需要 process_local 分布式运行时）──────────────────

    /// 创建测试用 Client 的辅助函数。
    ///
    /// ## 功能
    ///
    /// 以进程本地（无需真实 NATS/etcd）分布式运行时初始化一个 Client，
    /// 用于集成测试中验证 Client 的行为。
    ///
    /// ## 处理过程
    ///
    /// 1. 从当前 tokio 运行时创建 `Runtime`
    /// 2. 以 `DistributedConfig::process_local()` 创建本地 DRT
    /// 3. 在 DRT 中创建 namespace → servicegroup → portname → client
    ///
    /// # 参数
    /// - `ns_name`: 测试用命名空间名（每个测试用唯一名避免冲突）
    ///
    /// # 返回
    /// (Runtime, Client) 元组；测试结束时需调用 `rt.shutdown()` 清理资源。
    async fn make_test_client(ns_name: &str) -> (Runtime, Client) {
        let rt = Runtime::from_current().unwrap();
        let drt = DistributedRuntime::new(rt.clone(), DistributedConfig::process_local())
            .await
            .unwrap();
        let ns = drt.namespace(ns_name.to_string()).unwrap();
        let comp = ns.servicegroup("comp".to_string()).unwrap();
        let ep = comp.portname("ep".to_string());
        let client = Client::with_reconcile_interval(ep, Duration::from_millis(50))
            .await
            .unwrap();
        (rt, client)
    }

    /// 测试场景：report_instance_down 应将目标实例从 instance_avail 中移除。
    ///
    /// ## 测试过程
    ///
    /// 1. 创建 Client 并直接设置 `instance_avail` 为 [1, 2, 3]
    /// 2. 调用 `report_instance_down(2)`
    /// 3. 断言 instance 1 仍在 avail 中
    /// 4. 断言 instance 2 已不在 avail 中
    /// 5. 断言 instance 3 仍在 avail 中
    ///
    /// ## 意义
    ///
    /// 验证 `report_instance_down` 的过滤逻辑：只移除目标实例，不影响其他实例。
    #[tokio::test]
    async fn test_report_instance_down() {
        let (rt, client) = make_test_client("test-report-down").await;

        client.instance_avail.store(Arc::new(vec![1, 2, 3]));
        client.report_instance_down(2);

        let avail = client.instance_ids_avail();
        assert!(avail.contains(&1), "instance 1 should remain");
        assert!(!avail.contains(&2), "instance 2 should be removed");
        assert!(avail.contains(&3), "instance 3 should remain");

        rt.shutdown();
    }

    /// 测试场景：report_instance_down 调用后，watch 订阅者应收到新的可用列表通知。
    ///
    /// ## 测试过程
    ///
    /// 1. 创建 Client 并获取 `instance_avail_watcher`
    /// 2. 直接设置 `instance_avail` 为 [1, 2, 3]
    /// 3. 调用 `report_instance_down(2)`（内部调用 `instance_avail_tx.send`）
    /// 4. 通过 `watcher.borrow()` 读取当前值
    /// 5. 断言 watcher 的当前值为 [1, 3]
    ///
    /// ## 意义
    ///
    /// 验证 `report_instance_down` 不仅更新了内部状态，
    /// 还通过 watch channel 通知了所有外部订阅者（如等待实例可用的调用方）。
    #[tokio::test]
    async fn test_instance_avail_watcher_notified() {
        let (rt, client) = make_test_client("test-watcher-notify").await;

        let watcher = client.instance_avail_watcher();
        client.instance_avail.store(Arc::new(vec![1, 2, 3]));
        client.report_instance_down(2);

        let current = watcher.borrow().clone();
        assert_eq!(current, vec![1, 3]);

        rt.shutdown();
    }

    /// 测试场景：定时对账到期后，instance_avail 应重置为 instance_source 的当前状态。
    ///
    /// ## 测试过程
    ///
    /// 1. 创建 Client（对账间隔 50ms，加速测试）
    /// 2. 直接设置 `instance_avail` 为 [1, 2, 3]（模拟有实例的状态）
    /// 3. 调用 `report_instance_down(2)`（instance_avail 变为 [1, 3]）
    /// 4. 断言当前 avail 为 [1, 3]
    /// 5. 等待 200ms（> 50ms 对账间隔）
    /// 6. 断言 avail 已变为 []
    ///    （因为 instance_source 为空——进程本地 DRT 无真实发现平面——对账后重置为空）
    ///
    /// ## 意义
    ///
    /// 验证对账机制能恢复被临时屏蔽的实例（防止永久屏蔽），
    /// 以及对账是基于 instance_source 而非保留已有 avail 状态。
    #[tokio::test]
    async fn test_instance_reconciliation_restores_avail() {
        let (rt, client) = make_test_client("test-reconciliation").await;

        // 直接设置 avail，模拟已有实例
        client.instance_avail.store(Arc::new(vec![1, 2, 3]));
        client.report_instance_down(2);
        assert_eq!(**client.instance_ids_avail(), vec![1u64, 3]);

        // 等待对账（instance_source 为空，所以最终 avail 应变为 []）
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            client.instance_ids_avail().is_empty(),
            "After reconciliation, avail should match empty instance_source"
        );

        rt.shutdown();
    }

    /// 测试场景：update_free_instances 应从全量实例中排除繁忙实例。
    ///
    /// ## 测试过程
    ///
    /// 1. 创建 Client
    /// 2. 直接设置 `instance_avail` 为 [1, 2, 3, 4]（模拟四个可用实例）
    /// 3. 调用 `update_free_instances(&[2, 3])`（声明 2、3 繁忙）
    /// 4. 断言 `instance_ids_free()` 为空
    ///    （因为 `update_free_instances` 依赖 `instance_ids()` 来自 `instance_source`，
    ///    而进程本地 DRT 的 `instance_source` 为空，所以 free 最终为空）
    ///
    /// ## 意义
    ///
    /// 验证 `update_free_instances` 的过滤逻辑基于全量 `instance_source` 而非 `instance_avail`，
    /// 同时确认方法不会 panic 或产生错误。
    #[tokio::test]
    async fn test_update_free_instances() {
        let (rt, client) = make_test_client("test-update-free").await;

        // 先设置全量实例
        client.instance_avail.store(Arc::new(vec![1, 2, 3, 4]));
        // update_free_instances 依赖 self.instance_ids()（从 instance_source 读），
        // instance_source 为空，所以 free 最终为空
        client.update_free_instances(&[2, 3]);
        // free 应为空（instance_source 为空）
        assert!(client.instance_ids_free().is_empty());

        rt.shutdown();
    }

    /// 测试场景：同端点的两次 get_or_create_routing_occupancy_state 调用应返回同一 Arc。
    ///
    /// ## 测试过程
    ///
    /// 1. 创建 DRT 和 PortName
    /// 2. 第一次调用 `get_or_create_routing_occupancy_state(&ep)` 得到 state_a
    /// 3. 第二次调用同一端点得到 state_b
    /// 4. 使用 `Arc::ptr_eq` 断言二者指向同一内存地址
    ///
    /// ## 意义
    ///
    /// 验证全局唯一性：同端点的所有 Client 共享同一 RoutingOccupancyState，
    /// 确保路由决策基于真实的全局负载视图，而非每个 Client 的本地视图。
    #[tokio::test]
    async fn test_get_or_create_occupancy_state_shared() {
        let rt = Runtime::from_current().unwrap();
        let drt = DistributedRuntime::new(rt.clone(), DistributedConfig::process_local())
            .await
            .unwrap();
        let ns = drt.namespace("test-occupancy-shared".to_string()).unwrap();
        let comp = ns.servicegroup("comp".to_string()).unwrap();
        let ep = comp.portname("ep".to_string());

        let state_a = get_or_create_routing_occupancy_state(&ep).await;
        let state_b = get_or_create_routing_occupancy_state(&ep).await;

        // 应是同一 Arc（指针相等）
        assert!(Arc::ptr_eq(&state_a, &state_b));

        rt.shutdown();
    }

    /// 测试场景：实例从发现平面下线后，monitor 任务应清理其 RoutingOccupancyState 计数。
    ///
    /// ## 测试过程
    ///
    /// 1. 创建 DRT、PortName 和 Client（对账间隔 50ms）
    /// 2. 调用 `register_portname_instance` 注册实例，使发现平面可见
    /// 3. 等待 `wait_for_instances` 确认实例已被 Client 感知
    /// 4. 获取该 worker 的 ID 并对 `RoutingOccupancyState` 递增计数至 1
    /// 5. 调用 `unregister_portname_instance` 下线实例
    /// 6. 轮询等待 monitor 任务清理计数（最多 1 秒）
    /// 7. 断言计数已归零
    ///
    /// ## 意义
    ///
    /// 验证 monitor 任务在实例下线后能通过 `retain` 及时清理陈旧计数，
    /// 防止已下线 worker 的高计数值影响后续路由决策。
    #[tokio::test]
    async fn test_monitor_cleans_occupancy_on_instance_removal() {
        let rt = Runtime::from_current().unwrap();
        let drt = DistributedRuntime::new(rt.clone(), DistributedConfig::process_local())
            .await
            .unwrap();
        let ns = drt.namespace("test-occ-cleanup".to_string()).unwrap();
        let comp = ns.servicegroup("comp".to_string()).unwrap();
        let ep = comp.portname("ep".to_string());

        let client = Client::with_reconcile_interval(ep.clone(), Duration::from_millis(50))
            .await
            .unwrap();

        // 注册并等待实例出现
        ep.register_portname_instance().await.unwrap();
        client.wait_for_instances().await.unwrap();

        let worker_id = client.instance_ids_avail()[0];
        let state = get_or_create_routing_occupancy_state(&ep).await;
        state.increment(worker_id);
        assert_eq!(state.load(worker_id), 1);

        // 下线实例，等待 monitor 清理占用计数
        ep.unregister_portname_instance().await.unwrap();

        for _ in 0..20 {
            if state.load(worker_id) == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(state.load(worker_id), 0, "occupancy should be cleaned after instance removal");

        rt.shutdown();
    }

    /// 测试场景：两个独立的 RoutingOccupancyState Arc 指针共享同一内存，
    ///           select_exact_min_and_increment 的选择应跨 Client 可见。
    ///
    /// ## 测试过程
    ///
    /// 1. 创建 DRT、PortName，分别获取 state1 和 state2（同端点，Arc 相同）
    /// 2. 通过 state1 对候选集 [10, 20, 30] 调用 select_exact_min_and_increment，得到 id1
    /// 3. 断言 state1.load(id1) == 1（被选中一次）
    /// 4. 再次调用 select_exact_min_and_increment，得到 id2（应与 id1 不同）
    /// 5. 断言 id1 != id2（least-loaded 策略选了不同的 worker）
    /// 6. 通过 state2 验证 state1 的计数（因为 Arc 相同，计数应一致）
    /// 7. 通过 state2 递减 id1 的计数，验证 state1 也能看到变化
    ///
    /// ## 意义
    ///
    /// 验证全局共享状态的跨 Client 可见性：一个 Client 的 increment/decrement
    /// 对其他持有同端点 State 的 Client 立即可见，保证路由决策的全局一致性。
    #[tokio::test]
    async fn test_occupancy_state_shared_between_clients() {
        let rt = Runtime::from_current().unwrap();
        let drt = DistributedRuntime::new(rt.clone(), DistributedConfig::process_local())
            .await
            .unwrap();
        let ns = drt.namespace("test-occ-multi-client".to_string()).unwrap();
        let comp = ns.servicegroup("comp".to_string()).unwrap();
        let ep = comp.portname("ep".to_string());

        let state1 = get_or_create_routing_occupancy_state(&ep).await;
        let state2 = get_or_create_routing_occupancy_state(&ep).await;

        let id1 = state1
            .select_exact_min_and_increment(&[10, 20, 30])
            .await
            .unwrap();
        assert_eq!(state1.load(id1), 1);

        let id2 = state1
            .select_exact_min_and_increment(&[10, 20, 30])
            .await
            .unwrap();
        assert_ne!(id1, id2, "second selection should pick a different worker");

        // state2 应看到相同计数（共享同一 Arc）
        assert_eq!(state2.load(10), state1.load(10));
        assert_eq!(state2.load(20), state1.load(20));
        assert_eq!(state2.load(30), state1.load(30));

        state2.decrement(id1);
        let expected = if id1 == id2 { 1 } else { 0 };
        assert_eq!(state1.load(id1), expected);

        rt.shutdown();
    }

    // ==================================================================
    // === lib-copy 标准契约测试（原样保留，验证 API 行为一致）============
    // ==================================================================

    #[tokio::test]
    async fn test_concurrent_select_and_increment() {
        let state = Arc::new(RoutingOccupancyState::default());
        let instance_ids: Vec<u64> = vec![100, 200, 300];
        let num_requests = 90;

        let mut handles = Vec::new();
        for _ in 0..num_requests {
            let state = state.clone();
            let ids = instance_ids.clone();
            handles.push(tokio::spawn(async move {
                state.select_exact_min_and_increment(&ids).await
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        assert_eq!(state.load(100), 30);
        assert_eq!(state.load(200), 30);
        assert_eq!(state.load(300), 30);
    }

    #[tokio::test]
    async fn test_connection_counts() {
        let rt = Runtime::from_current().unwrap();
        let drt = DistributedRuntime::new(rt.clone(), DistributedConfig::process_local())
            .await
            .unwrap();
        let ns = drt.namespace("test_ll_counts".to_string()).unwrap();
        let servicegroup = ns.servicegroup("test_servicegroup".to_string()).unwrap();
        let portname = servicegroup.portname("test_portname".to_string());

        let state1 = get_or_create_routing_occupancy_state(&portname).await;
        let state2 = get_or_create_routing_occupancy_state(&portname).await;

        let picked1 = state1
            .select_exact_min_and_increment(&[10, 20, 30])
            .await
            .unwrap();
        assert_eq!(state1.load(picked1), 1);

        let picked2 = state1
            .select_exact_min_and_increment(&[10, 20, 30])
            .await
            .unwrap();
        assert_ne!(picked1, picked2);

        // state2 should see the same counts (same underlying Arc)
        assert_eq!(state2.load(10), state1.load(10));
        assert_eq!(state2.load(20), state1.load(20));
        assert_eq!(state2.load(30), state1.load(30));

        state2.decrement(picked1);
        assert_eq!(state1.load(picked1), if picked1 == picked2 { 1 } else { 0 });

        rt.shutdown();
    }

    #[tokio::test]
    async fn test_instance_avail_watcher() {
        let rt = Runtime::from_current().unwrap();
        let drt = DistributedRuntime::new(rt.clone(), DistributedConfig::process_local())
            .await
            .unwrap();
        let ns = drt.namespace("test_watcher".to_string()).unwrap();
        let servicegroup = ns.servicegroup("test_servicegroup".to_string()).unwrap();
        let portname = servicegroup.portname("test_portname".to_string());

        let client = portname.client().await.unwrap();
        let watcher = client.instance_avail_watcher();

        // Set initial instances
        client.instance_avail.store(Arc::new(vec![1, 2, 3]));

        // Report instance down - this should notify the watcher
        client.report_instance_down(2);

        let current = watcher.borrow().clone();
        assert_eq!(current, vec![1, 3]);

        rt.shutdown();
    }

    #[tokio::test]
    async fn test_instance_reconciliation() {
        const TEST_RECONCILE_INTERVAL: Duration = Duration::from_millis(100);

        let rt = Runtime::from_current().unwrap();
        let drt = DistributedRuntime::new(rt.clone(), DistributedConfig::process_local())
            .await
            .unwrap();
        let ns = drt.namespace("test_reconciliation".to_string()).unwrap();
        let servicegroup = ns.servicegroup("test_servicegroup".to_string()).unwrap();
        let portname = servicegroup.portname("test_portname".to_string());

        let client = Client::with_reconcile_interval(portname, TEST_RECONCILE_INTERVAL)
            .await
            .unwrap();

        // Initially, instance_avail should be empty
        assert!(client.instance_ids_avail().is_empty());

        client.instance_avail.store(Arc::new(vec![1, 2, 3]));
        assert_eq!(**client.instance_ids_avail(), vec![1u64, 2, 3]);

        client.report_instance_down(2);
        assert_eq!(**client.instance_ids_avail(), vec![1u64, 3]);

        tokio::time::sleep(TEST_RECONCILE_INTERVAL + Duration::from_millis(50)).await;

        assert!(
            client.instance_ids_avail().is_empty(),
            "After reconciliation, instance_avail should match instance_source"
        );

        rt.shutdown();
    }

    #[tokio::test]
    async fn test_least_loaded_state_retain() {
        let state = RoutingOccupancyState::default();

        state.select_exact_min_and_increment(&[1, 2, 3]).await;
        state.select_exact_min_and_increment(&[1, 2, 3]).await;
        state.select_exact_min_and_increment(&[1, 2, 3]).await;
        assert_eq!(state.load(1), 1);
        assert_eq!(state.load(2), 1);
        assert_eq!(state.load(3), 1);

        // Retain only instances 1 and 3 (instance 2 was removed)
        state.retain(&[1, 3]);

        assert_eq!(state.load(1), 1);
        assert_eq!(state.load(2), 0);
        assert_eq!(state.load(3), 1);
    }

    #[tokio::test]
    async fn test_monitor_instance_source_cleans_up_removed_worker_counts() {
        const TEST_RECONCILE_INTERVAL: Duration = Duration::from_millis(50);

        let rt = Runtime::from_current().unwrap();
        let drt = DistributedRuntime::new(rt.clone(), DistributedConfig::process_local())
            .await
            .unwrap();
        let ns = drt.namespace("test_occupancy_cleanup".to_string()).unwrap();
        let servicegroup = ns.servicegroup("test_servicegroup".to_string()).unwrap();
        let portname = servicegroup.portname("test_portname".to_string());

        let client = Client::with_reconcile_interval(portname.clone(), TEST_RECONCILE_INTERVAL)
            .await
            .unwrap();
        portname.register_portname_instance().await.unwrap();
        client.wait_for_instances().await.unwrap();

        let worker_id = client.instance_ids_avail()[0];
        let state = get_or_create_routing_occupancy_state(&portname).await;
        state.increment(worker_id);
        assert_eq!(state.load(worker_id), 1);

        portname.unregister_portname_instance().await.unwrap();

        for _ in 0..10 {
            if state.load(worker_id) == 0 {
                break;
            }
            tokio::time::sleep(TEST_RECONCILE_INTERVAL).await;
        }

        assert_eq!(state.load(worker_id), 0);

        rt.shutdown();
    }
}
