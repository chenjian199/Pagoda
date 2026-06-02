// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::network::egress::push_router` —— Push 路由器与动态 instance 选择
//!
//! ## 设计意图
//! `PushRouter` 是 egress 面向业务调用方的默认路径：调用方只提供 servicegroup/portname
//! 名，在 instance 动态变化下（watch::Receiver<Vec<Instance>>）选一个健康节点 ——
//! round-robin / random / direct，调用该节点的 transport client。应对错误还能
//! 透明重试、换节点。
//!
//! ## 外部契约
//! - 公开结构 / 方法是稳定契约；
//! - 依赖 [`crate::error::BackendError`] / `PagodaError` / `ErrorType` / `match_error_chain`
//!   进行错误分类与重试决策——这些导入路径是契约。
//!
//! ## 实现要点
//! - 路由决策独立于传输层：选定 instance 后全部委托给注入的 `Arc<dyn RequestPlaneClient>`；
//! - 重试参数、退避策略、instance 黑名单都已定型，保持稳定。

use super::{AsyncEngineContextProvider, ResponseStream};
use crate::error::{BackendError, PagodaError, ErrorType, match_error_chain};
use crate::{
    servicegroup::{
        Client, DeviceType, PortName, Instance, RoutingOccupancyState,
        get_or_create_routing_occupancy_state,
    },
    discovery::PortNameInstanceId,
    pagoda_timeline_range,
    engine::{AsyncEngine, AsyncEngineContext, Data},
    metrics::frontend_perf::{STAGE_DURATION_SECONDS, STAGE_ROUTE},
    pipeline::{
        AddressedPushRouter, AddressedRequest, Error, ManyOut, SingleIn,
        error::{PipelineError, PipelineErrorExt},
    },
    protocols::{PortNameId, maybe_error::MaybeError},
    traits::DistributedRuntimeProvider,
};
use async_trait::async_trait;
use futures::Stream;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    marker::PhantomData,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::Poll,
    time::Instant,
};
use tokio_stream::StreamExt;
use tracing::Instrument;

/// 检查错误链是否表明该 worker 应被报告为宕机。
fn is_inhibited(err: &(dyn std::error::Error + 'static)) -> bool {
    const INHIBITED: &[ErrorType] = &[
        ErrorType::CannotConnect,
        ErrorType::Disconnected,
        ErrorType::ConnectionTimeout,
        ErrorType::ResponseTimeout,
        ErrorType::Backend(BackendError::EngineShutdown),
    ];
    match_error_chain(err, INHIBITED, &[])
}

/// 从环境变量读取后端响应不活动超时。
/// 复用 `PGD_HTTP_BACKEND_STREAM_TIMEOUT_SECS` —— 与 `disconnect.rs`
/// 中 HTTP 层安全网相同的环境变量。
fn response_inactivity_timeout() -> Option<std::time::Duration> {
    use crate::config::environment_names::llm::PGD_HTTP_BACKEND_STREAM_TIMEOUT_SECS;
    std::env::var(PGD_HTTP_BACKEND_STREAM_TIMEOUT_SECS)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&secs| secs > 0)
        .map(std::time::Duration::from_secs)
}

struct OccupancyPermit {
    state: Arc<RoutingOccupancyState>,
    instance_id: u64,
    armed: bool,
}

impl OccupancyPermit {
    fn new(state: Arc<RoutingOccupancyState>, instance_id: u64) -> Self {
        Self {
            state,
            instance_id,
            armed: true,
        }
    }

    fn into_tracked_stream<U: Data>(mut self, stream: ManyOut<U>) -> ManyOut<U> {
        self.armed = false;
        let engine_ctx = stream.context();
        ResponseStream::new(
            Box::pin(OccupancyTrackedStream {
                inner: stream,
                state: self.state.clone(),
                instance_id: self.instance_id,
            }),
            engine_ctx,
        )
    }

    fn instance_id(&self) -> u64 {
        self.instance_id
    }
}

impl Drop for OccupancyPermit {
    fn drop(&mut self) {
        if self.armed {
            self.state.decrement(self.instance_id);
        }
    }
}

/// 用于监控 worker 负载并判定繁忙状态的 trait。
/// 实现方可定义自定义的负载指标与繁忙阈值。
#[async_trait]
pub trait WorkerLoadMonitor: Send + Sync {
    /// 启动 worker 负载的后台监控。
    /// 应派发后台任务以更新客户端的空闲实例。
    async fn start_monitoring(&self) -> anyhow::Result<()>;
}

#[derive(Clone)]
pub struct PushRouter<T, U>
where
    T: Data + Serialize,
    U: Data + for<'de> Deserialize<'de>,
{
    // TODO: 这本不应为 pub，但 lib/bindings/python/rust/lib.rs 暴露了它。
    /// Client 是我们从 etcd 收集远端 portname 信息的途径。
    pub client: Client,

    /// 我们如何选择将流量发往哪个实例。
    ///
    /// 设为 KV 意味着我们绝不打算对该 PushRouter 调用 `generate`。我们
    /// 不会把它当作 AsyncEngine 使用。
    /// 取而代之，我们自行决定调用 random/round_robin/direct 并直接调用它们。
    /// pagoda-llm 的 KV Routing 即如此。
    router_mode: RouterMode,

    /// 已处理的轮询请求数。用于决定下一台服务器是哪个。
    round_robin_counter: Arc<AtomicU64>,

    /// 链路中的下一步。PushRouter（本对象）选取一个实例，
    /// 为其寻址，然后传给负责网络流量的 AddressedPushRouter。
    addressed: Arc<AddressedPushRouter>,

    /// 为 false 时，`generate_with_fault_detection` 跳过故障检测逻辑：
    /// 它不会在出错时调用 `report_instance_down`，且使用原始 discovery
    /// 实例列表而非过滤后的可用列表。用于预期会出现瞬时故障的
    /// 恢复/查询路径。
    fault_detection_enabled: bool,

    /// 缓存的响应不活动超时。在构造时从
    /// [`environment_names::llm::PGD_HTTP_BACKEND_STREAM_TIMEOUT_SECS`](crate::config::environment_names::llm::PGD_HTTP_BACKEND_STREAM_TIMEOUT_SECS) 读取一次，以避免每次请求一次系统调用。
    response_timeout: Option<std::time::Duration>,

    /// 用于受跟踪路由模式的共享请求占用状态。
    occupancy_state: Option<Arc<RoutingOccupancyState>>,

    /// 一个内部 Rust 类型。它表明 PushRouter 对 T 和 U 类型泛型，
    /// 即其 `generate` 函数的输入与输出类型。它允许编译器
    /// 在编译期对我们做特化。
    _phantom: PhantomData<(T, U)>,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouterMode {
    #[default]
    RoundRobin,
    Random,
    PowerOfTwoChoices,
    KV,
    Direct,
    LeastLoaded,
    /// 面向异构 worker 的设备感知加权路由。
    DeviceAwareWeighted,
}

impl RouterMode {
    pub fn is_kv_routing(&self) -> bool {
        *self == RouterMode::KV
    }

    pub fn is_direct_routing(&self) -> bool {
        *self == RouterMode::Direct
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION: 路由算法 helper（p2c / device-aware partition / watcher spawn）
// ─────────────────────────────────────────────────────────────────────────────

/// 从两个随机候选中选取在途请求数较少的实例。
/// 若只有一个可用，则返回该实例。
fn p2c_select_from(occupancy_state: &RoutingOccupancyState, instance_ids: &[u64]) -> u64 {
    let count = instance_ids.len();
    if count == 1 {
        return instance_ids[0];
    }
    let mut rng = rand::rng();
    let idx1 = rng.random_range(0..count);
    let idx2 = (idx1 + 1 + rng.random_range(0..count - 1)) % count;
    let id1 = instance_ids[idx1];
    let id2 = instance_ids[idx2];
    let load1 = occupancy_state.load(id1);
    let load2 = occupancy_state.load(id2);
    let selected = if load1 <= load2 { id1 } else { id2 };
    tracing::debug!(
        candidate_a = id1,
        candidate_a_load = load1,
        candidate_b = id2,
        candidate_b_load = load2,
        selected = selected,
        "p2c selection"
    );
    selected
}

/// 为 `DeviceAwareWeighted` 模式下的下一个请求选择目标设备组。
///
/// 若只存在一个类别（全 CPU 或全非 CPU），则直接返回该类别。
/// 若两个类别都存在，则比较能力归一化后的负载并返回负载较低的组。
///
/// 预算检查（整数形式）：
/// 公式：`allowed_cpu_inflight = total_non_cpu_inflight * cpu_count / (ratio * non_cpu_count)`
/// 当 `total_cpu_inflight < allowed_cpu_inflight` 时选择 CPU。
///
/// `ratio` 为 `non_cpu_to_cpu_ratio`（来自 `PGD_ENCODER_CUDA_TO_CPU_RATIO`，
/// 在 `device_aware_weighted` 中默认为 `8`）。
fn device_aware_candidate_group(
    state: &RoutingOccupancyState,
    instance_ids: &[u64],
    device_type_map: &HashMap<u64, Option<DeviceType>>,
    non_cpu_to_cpu_ratio: usize,
) -> Vec<u64> {
    let cpu_ids: Vec<u64> = instance_ids
        .iter()
        .copied()
        .filter(|id| matches!(device_type_map.get(id), Some(Some(DeviceType::Cpu))))
        .collect();
    let non_cpu_ids: Vec<u64> = instance_ids
        .iter()
        .copied()
        .filter(|id| !matches!(device_type_map.get(id), Some(Some(DeviceType::Cpu))))
        .collect();

    if cpu_ids.is_empty() {
        return non_cpu_ids;
    }
    if non_cpu_ids.is_empty() {
        return cpu_ids;
    }

    // 两个类别都存在：为 CPU 在途请求计算预算。
    let total_non_cpu_inflight: u64 = non_cpu_ids.iter().map(|id| state.load(*id)).sum();
    let total_cpu_inflight: u64 = cpu_ids.iter().map(|id| state.load(*id)).sum();
    let cpu_count = cpu_ids.len() as u64;
    let non_cpu_count = non_cpu_ids.len() as u64;
    let allowed_cpu_inflight = total_non_cpu_inflight.saturating_mul(cpu_count)
        / ((non_cpu_to_cpu_ratio as u64).saturating_mul(non_cpu_count));

    if total_cpu_inflight < allowed_cpu_inflight {
        cpu_ids
    } else {
        non_cpu_ids
    }
}

/// 跨所有 `PushRouter` 实例，每个 portname 最多一个 `list_and_watch`。
/// 条目在 watcher 退出时被移除，以便后续 router 可重新启用。
static ENDPOINT_WATCHER_ACTIVE: std::sync::OnceLock<dashmap::DashMap<PortNameId, ()>> =
    std::sync::OnceLock::new();

/// 监视 discovery 的实例移除事件，取消被移除实例上待处理的
/// 响应流注册，以可迁移的 `Disconnected` 错误解锁排队中的请求。
/// 使用原始 `list_and_watch` 事件（而非合并后的快照差异），使同一
/// 身份的快速 移除→重新加入 不会被静默吞掉。以完整的
/// `PortNameInstanceId` 作为键。
fn spawn_instance_removal_watcher(
    portname: PortName,
    addressed: Arc<AddressedPushRouter>,
    cancel_token: tokio_util::sync::CancellationToken,
) {
    use crate::discovery::{
        DiscoveryEvent, DiscoveryInstance, DiscoveryInstanceId, DiscoveryQuery,
    };
    use tokio_stream::StreamExt as _;

    // 每个 portname 一个 watcher：若已有一个在运行，则跳过。
    let guard = ENDPOINT_WATCHER_ACTIVE.get_or_init(dashmap::DashMap::new);
    let portname_id = portname.id();
    if guard.insert(portname_id.clone(), ()).is_some() {
        tracing::debug!(
            ?portname_id,
            "Instance removal watcher already running for this portname, skipping"
        );
        return;
    }

    let portname_name = portname.name().to_string();

    tokio::spawn(async move {
        // 在每条退出路径（包括 panic）上释放；泄露的条目会
        // 静默地禁用移除取消功能，直到进程重启。
        struct GuardRelease(PortNameId);
        impl Drop for GuardRelease {
            fn drop(&mut self) {
                if let Some(map) = ENDPOINT_WATCHER_ACTIVE.get() {
                    map.remove(&self.0);
                }
            }
        }
        let _release = GuardRelease(portname_id);

        let namespace = portname.servicegroup().namespace().name();
        let servicegroup = portname.servicegroup().name().to_string();

        // 在瞬时 discovery 故障时重连；支持取消的退避。
        const RECONNECT_BACKOFF: std::time::Duration = std::time::Duration::from_secs(5);
        'reconnect: loop {
            let query = DiscoveryQuery::PortName {
                namespace: namespace.clone(),
                servicegroup: servicegroup.clone(),
                portname: portname_name.clone(),
            };

            let mut stream = match portname.drt().discovery().list_and_watch(query, None).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(
                        portname = %portname_name,
                        "Failed to start instance removal watcher (will retry): {e}"
                    );
                    tokio::select! {
                        _ = tokio::time::sleep(RECONNECT_BACKOFF) => continue 'reconnect,
                        _ = cancel_token.cancelled() => break 'reconnect,
                    }
                }
            };

            loop {
                tokio::select! {
                    event = stream.next() => {
                        match event {
                            Some(Ok(DiscoveryEvent::Removed(id))) => {
                                if let DiscoveryInstanceId::PortName(eid) = &id {
                                    let n = addressed.cancel_instance_streams(eid).await;
                                    if n > 0 {
                                        tracing::warn!(
                                            namespace = %eid.namespace,
                                            servicegroup = %eid.servicegroup,
                                            portname = %eid.portname,
                                            instance_id = eid.instance_id,
                                            cancelled = n,
                                            "Cancelled pending response streams for removed \
                                             instance (discovery-driven cleanup)"
                                        );
                                    }
                                }
                            }
                            Some(Ok(DiscoveryEvent::Added(DiscoveryInstance::PortName(inst)))) => {
                                let eid: PortNameInstanceId = inst.portname_instance_id();
                                addressed.clear_instance_tombstone(&eid).await;
                            }
                            Some(Ok(_)) => {}
                            Some(Err(e)) => {
                                tracing::warn!(
                                    portname = %portname_name,
                                    "Instance removal watcher stream error: {e}"
                                );
                            }
                            None => {
                                tracing::warn!(
                                    portname = %portname_name,
                                    "Instance removal watcher stream ended; reconnecting"
                                );
                                continue 'reconnect;
                            }
                        }
                    }
                    _ = cancel_token.cancelled() => {
                        break 'reconnect;
                    }
                }
            }
        }

        tracing::debug!(portname = %portname_name, "Instance removal watcher exiting");
    });
}

async fn addressed_router(portname: &PortName) -> anyhow::Result<Arc<AddressedPushRouter>> {
    // 获取网络管理器并创建客户端（不做模式检查！）
    let manager = portname.drt().network_manager();
    let req_client = manager.create_client()?;
    let resp_transport = portname.drt().tcp_server().await?;

    tracing::debug!(
        transport = req_client.transport_name(),
        "Creating AddressedPushRouter with request plane client"
    );

    AddressedPushRouter::new(req_client, resp_transport)
}

impl<T, U> PushRouter<T, U>
where
    T: Data + Serialize,
    U: Data + for<'de> Deserialize<'de> + MaybeError,
{
    /// 创建一个不带 worker 负载监控器的新 PushRouter（无繁忙检测）
    pub async fn from_client(client: Client, router_mode: RouterMode) -> anyhow::Result<Self> {
        Self::from_client_with_monitor(client, router_mode, None).await
    }

    /// 创建一个禁用故障检测的新 PushRouter。
    ///
    /// 与 `from_client` 不同，该 router 不会在瞬时错误时调用
    /// `report_instance_down`，且 `direct()` 使用原始 discovery 实例列表
    /// 而非过滤后的可用列表。用于恢复/查询路径。
    pub async fn from_client_no_fault_detection(
        client: Client,
        router_mode: RouterMode,
    ) -> anyhow::Result<Self> {
        let addressed = addressed_router(&client.portname).await?;

        let occupancy_state = if matches!(
            router_mode,
            RouterMode::PowerOfTwoChoices
                | RouterMode::LeastLoaded
                | RouterMode::DeviceAwareWeighted
        ) {
            Some(get_or_create_routing_occupancy_state(&client.portname).await)
        } else {
            None
        };

        // 当 worker 宕机时取消孤立的待处理响应流。
        spawn_instance_removal_watcher(
            client.portname.clone(),
            addressed.clone(),
            client.portname.drt().primary_token(),
        );

        Ok(PushRouter {
            client,
            addressed,
            router_mode,
            round_robin_counter: Arc::new(AtomicU64::new(0)),
            fault_detection_enabled: false,
            response_timeout: response_inactivity_timeout(),
            occupancy_state,
            _phantom: PhantomData,
        })
    }

    /// 创建一个带可选 worker 负载监控器的新 PushRouter。
    ///
    /// 拒绝路径由 `fault_detection_enabled`（此处为 true）控制；
    /// 繁忙检测本身由监控器通过 `client.update_free_instances(...)` 驱动。
    /// 若监控器未配置阈值（或未提供监控器），
    /// `client.instance_ids_free()` 返回全部实例，闸门绝不拒绝。
    pub async fn from_client_with_monitor(
        client: Client,
        router_mode: RouterMode,
        worker_monitor: Option<Arc<dyn WorkerLoadMonitor>>,
    ) -> anyhow::Result<Self> {
        let addressed = addressed_router(&client.portname).await?;

        // 如提供了监控器且处于动态模式，则启动 worker 监控器
        if let Some(monitor) = worker_monitor.as_ref() {
            monitor.start_monitoring().await?;
        }

        let occupancy_state = if matches!(
            router_mode,
            RouterMode::PowerOfTwoChoices
                | RouterMode::LeastLoaded
                | RouterMode::DeviceAwareWeighted
        ) {
            Some(get_or_create_routing_occupancy_state(&client.portname).await)
        } else {
            None
        };

        // 当 worker 宕机时取消孤立的待处理响应流。
        spawn_instance_removal_watcher(
            client.portname.clone(),
            addressed.clone(),
            client.portname.drt().primary_token(),
        );

        let router = PushRouter {
            client,
            addressed,
            router_mode,
            round_robin_counter: Arc::new(AtomicU64::new(0)),
            fault_detection_enabled: true,
            response_timeout: response_inactivity_timeout(),
            occupancy_state,
            _phantom: PhantomData,
        };

        Ok(router)
    }

    /// 以轮询方式向下一个可用实例发出请求
    pub async fn round_robin(&self, request: SingleIn<T>) -> anyhow::Result<ManyOut<U>> {
        let counter = self.round_robin_counter.fetch_add(1, Ordering::Relaxed) as usize;

        let instance_id = {
            let instance_ids = self.client.instance_ids_avail();
            let count = instance_ids.len();
            if count == 0 {
                return Err(self.no_instances_error());
            }
            instance_ids[counter % count]
        };
        tracing::trace!("round robin router selected {instance_id}");

        self.generate_with_fault_detection(instance_id, request)
            .await
    }

    /// 向随机 portname 发出请求
    pub async fn random(&self, request: SingleIn<T>) -> anyhow::Result<ManyOut<U>> {
        let instance_id = {
            let instance_ids = self.client.instance_ids_avail();
            let count = instance_ids.len();
            if count == 0 {
                return Err(self.no_instances_error());
            }
            let counter = rand::rng().random::<u64>() as usize;
            instance_ids[counter % count]
        };
        tracing::trace!("random router selected {instance_id}");

        self.generate_with_fault_detection(instance_id, request)
            .await
    }

    /// 使用二选一（power-of-two-choices）发出请求：随机选 2 个健康 worker，
    /// 路由到在途请求较少的那个。
    pub async fn power_of_two_choices(&self, request: SingleIn<T>) -> anyhow::Result<ManyOut<U>> {
        let state = self.occupancy_state()?;
        let instance_id = {
            let instance_ids = self.avail_instance_ids_vec();
            if instance_ids.is_empty() {
                return Err(self.no_instances_error());
            }
            p2c_select_from(state.as_ref(), &instance_ids)
        };
        state.increment(instance_id);
        let permit = OccupancyPermit::new(state, instance_id);

        match self
            .generate_with_fault_detection(instance_id, request)
            .await
        {
            Ok(stream) => Ok(permit.into_tracked_stream(stream)),
            Err(err) => Err(err),
        }
    }

    /// 向特定 portname 发出请求
    pub async fn direct(
        &self,
        request: SingleIn<T>,
        instance_id: u64,
    ) -> anyhow::Result<ManyOut<U>> {
        // 当故障检测被禁用时，检查原始 discovery 列表
        // （未被 report_instance_down 过滤），使瞬时故障
        // 不会在后续重试中毒化该实例。
        let found = if self.fault_detection_enabled {
            self.client.instance_ids_avail().contains(&instance_id)
        } else {
            self.client.instance_ids().contains(&instance_id)
        };

        if !found {
            return Err(anyhow::anyhow!(
                "instance_id={instance_id} not found for portname {}",
                self.client.portname.id()
            ));
        }

        self.generate_with_fault_detection(instance_id, request)
            .await
    }

    /// 使用设备感知加权路由发出请求。
    ///
    /// 实例按设备类型（CPU 与非 CPU）划分，随后 router
    /// 应用预算策略并在所选组内选择负载最低的实例。
    ///
    /// 若只存在一个设备类别（全 CPU 或全非 CPU），这会自然
    /// 退化为对可用实例的负载最低路由。
    pub async fn device_aware_weighted(&self, request: SingleIn<T>) -> anyhow::Result<ManyOut<U>> {
        let state = self.occupancy_state()?;
        let instance_ids = self.avail_instance_ids_vec();

        if instance_ids.is_empty() {
            return Err(self.no_instances_error());
        }

        // 对所有 portname 应用统一策略。
        let portname_id = self.client.portname.id();

        // 对于 encoder portname，按设备类型划分
        let instances = self.client.instances();
        let device_type_map: std::collections::HashMap<u64, Option<DeviceType>> = instances
            .iter()
            .map(|inst| (inst.instance_id, inst.device_type.clone()))
            .collect();

        // 应用基于预算的路由以决定发往哪个组
        let cuda_to_cpu_ratio = std::env::var("PGD_ENCODER_CUDA_TO_CPU_RATIO")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v >= 1)
            .unwrap_or(8);
        let candidates = device_aware_candidate_group(
            state.as_ref(),
            &instance_ids,
            &device_type_map,
            cuda_to_cpu_ratio,
        );

        // 在所选组内选择负载最低者
        let instance_id = state
            .select_exact_min_and_increment(&candidates)
            .await
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "no instances in selected device group for portname {}",
                    portname_id
                )
            })?;
        let permit = OccupancyPermit::new(state.clone(), instance_id);
        let is_cpu = matches!(
            device_type_map.get(&instance_id),
            Some(Some(DeviceType::Cpu))
        );
        tracing::info!(
            portname = %portname_id,
            selected_instance = instance_id,
            is_cpu,
            "DeviceAwareWeighted selected instance"
        );

        match self
            .generate_with_fault_detection(instance_id, request)
            .await
        {
            Ok(stream) => Ok(permit.into_tracked_stream(stream)),
            Err(err) => Err(err),
        }
    }

    /// 向活动连接最少的实例发出请求。
    pub async fn least_loaded(&self, request: SingleIn<T>) -> anyhow::Result<ManyOut<U>> {
        let state = self.occupancy_state()?;
        let instance_ids = self.avail_instance_ids_vec();
        let instance_id = state
            .select_exact_min_and_increment(&instance_ids)
            .await
            .ok_or_else(|| self.no_instances_error())?;
        let permit = OccupancyPermit::new(state.clone(), instance_id);
        tracing::trace!(
            "least loaded router selected {instance_id} (connections: {})",
            state.load(instance_id)
        );

        match self
            .generate_with_fault_detection(instance_id, request)
            .await
        {
            Ok(stream) => Ok(permit.into_tracked_stream(stream)),
            Err(err) => Err(err),
        }
    }

    /// 根据路由模式选择下一个 worker。
    /// 如适用则递增轮询计数器。
    /// 对于需要请求生命周期跟踪或显式路由提示的模式返回 None。
    pub fn select_next_worker(&self) -> Option<u64> {
        let instance_ids = self.client.instance_ids_avail();
        let count = instance_ids.len();
        if count == 0 {
            return None;
        }

        match self.router_mode {
            RouterMode::RoundRobin => {
                let counter = self.round_robin_counter.fetch_add(1, Ordering::Relaxed) as usize;
                Some(instance_ids[counter % count])
            }
            RouterMode::Random => {
                let counter = rand::rng().random::<u64>() as usize;
                Some(instance_ids[counter % count])
            }
            RouterMode::PowerOfTwoChoices
            | RouterMode::Direct
            | RouterMode::LeastLoaded
            | RouterMode::DeviceAwareWeighted => None,
            RouterMode::KV => {
                panic!(
                    "select_next_worker should not be called for {:?} routing mode",
                    self.router_mode
                )
            }
        }
    }

    /// 按路由模式稥视下一个 worker 而不递增计数器。
    /// 用于在提交之前检查 worker 是否合适。
    /// 对于需要请求生命周期跟踪或显式路由提示的模式返回 None。
    pub fn peek_next_worker(&self) -> Option<u64> {
        let instance_ids = self.client.instance_ids_avail();
        let count = instance_ids.len();
        if count == 0 {
            return None;
        }

        match self.router_mode {
            RouterMode::RoundRobin => {
                // 仅稥视当前计数器值而不递增
                let counter = self.round_robin_counter.load(Ordering::Relaxed) as usize;
                Some(instance_ids[counter % count])
            }
            RouterMode::Random => {
                // 对于 random，由于无状态，稥视意味着一次新的随机选择。
                // 注意：调用方必须意识到 select_next_worker() 会选出一个不同的随机 worker。
                let counter = rand::rng().random::<u64>() as usize;
                Some(instance_ids[counter % count])
            }
            RouterMode::PowerOfTwoChoices
            | RouterMode::Direct
            | RouterMode::LeastLoaded
            | RouterMode::DeviceAwareWeighted => None,
            RouterMode::KV => {
                panic!(
                    "peek_next_worker should not be called for {:?} routing mode",
                    self.router_mode
                )
            }
        }
    }

    fn occupancy_state(&self) -> anyhow::Result<Arc<RoutingOccupancyState>> {
        self.occupancy_state.clone().ok_or_else(|| {
            anyhow::anyhow!(
                "routing occupancy state not initialized for portname {}",
                self.client.portname.id()
            )
        })
    }

    /// 统一的 "no instances" 错误消息构造器；消除 5 处重复字面量。
    #[inline]
    fn no_instances_error(&self) -> anyhow::Error {
        anyhow::anyhow!(
            "no instances found for portname {}",
            self.client.portname.id()
        )
    }

    /// 返回当前可用 instance_id 列表的 owned `Vec<u64>`；
    /// 抽取自 power_of_two_choices / device_aware_weighted / least_loaded 三处重复表达。
    #[inline]
    fn avail_instance_ids_vec(&self) -> Vec<u64> {
        self.client.instance_ids_avail().iter().copied().collect()
    }



    async fn generate_with_fault_detection(
        &self,
        mut instance_id: u64,
        request: SingleIn<T>,
    ) -> anyhow::Result<ManyOut<U>> {
        let route_start = Instant::now();
        let request_id = request.id().to_string();
        let route_span = if matches!(self.router_mode, RouterMode::KV) {
            tracing::Span::none()
        } else {
            tracing::info_span!(
                "router.route_request",
                request_id = %request_id,
                worker_id = instance_id,
                router_mode = ?self.router_mode,
            )
        };

        // 检查是否所有 worker 都繁忙（当故障检测启用时）。
        if self.fault_detection_enabled {
            let free_instances = self.client.instance_ids_free();
            if free_instances.is_empty() {
                // 检查我们是否确实拥有任何实例
                let all_instances = self.client.instance_ids();
                if !all_instances.is_empty() {
                    tracing::warn!(
                        instance_id,
                        total_workers = all_instances.len(),
                        "Rejecting request: all workers are busy"
                    );
                    let cause = PipelineError::ServiceOverloaded(
                        "All workers are busy, please retry later".to_string(),
                    );
                    return Err(PagodaError::builder()
                        .error_type(ErrorType::ResourceExhausted)
                        .message("All workers are busy, please retry later")
                        .cause(cause)
                        .build()
                        .into());
                }
            }
        }

        // 解析传输地址；若所选实例在选择与派发之间消失，
        // 则回退到另一个可用实例。
        let (address, _transport_kind, instance) = {
            use crate::servicegroup::TransportType;

            let resolve_transport = |id: u64| {
                let instances = self.client.instances();
                instances
                    .iter()
                    .find(|i| i.instance_id == id)
                    .map(|instance| {
                        let (addr, kind) = match &instance.transport {
                            TransportType::Http(http_endpoint) => {
                                tracing::debug!(
                                    instance_id = id,
                                    http_endpoint = %http_endpoint,
                                    "Using HTTP transport for instance"
                                );
                                (http_endpoint.clone(), "transport.http.request")
                            }
                            TransportType::Tcp(tcp_endpoint) => {
                                tracing::debug!(
                                    instance_id = id,
                                    tcp_endpoint = %tcp_endpoint,
                                    "Using TCP transport for instance"
                                );
                                (tcp_endpoint.clone(), "transport.tcp.request")
                            }
                            TransportType::Nats(subject) => {
                                tracing::debug!(
                                    instance_id = id,
                                    subject = %subject,
                                    "Using NATS transport for instance"
                                );
                                (subject.clone(), "transport.nats.request")
                            }
                        };
                        (addr, kind, instance.clone())
                    })
            };

            if let Some(result) = resolve_transport(instance_id) {
                result
            } else {
                // 实例消失 —— 从当前可用列表中选另一个
                // 并重试查找一次。
                let avail = self.client.instance_ids_avail();
                let fallback_id = avail.iter().copied().find(|&id| id != instance_id);
                match fallback_id {
                    Some(id) => {
                        tracing::warn!(
                            original_instance = instance_id,
                            fallback_instance = id,
                            "Instance disappeared during routing, reselecting"
                        );
                        instance_id = id;
                        resolve_transport(id).ok_or_else(|| {
                            anyhow::anyhow!(
                                "Fallback instance {} also not found for portname {}",
                                id,
                                self.client.portname.id()
                            )
                        })?
                    }
                    None => {
                        return Err(anyhow::anyhow!(
                            "Instance {} not found and no other instances available \
                             for portname {}",
                            instance_id,
                            self.client.portname.id()
                        ));
                    }
                }
            }
        };

        let request = request.map(|req| AddressedRequest::with_instance(req, address, instance));

        STAGE_DURATION_SECONDS
            .with_label_values(&[STAGE_ROUTE])
            .observe(route_start.elapsed().as_secs_f64());

        let _nvtx_transport = pagoda_timeline_range!(_transport_kind);
        let stream: anyhow::Result<ManyOut<U>> = self
            .addressed
            .generate(request)
            .instrument(route_span)
            .await;
        match stream {
            Ok(stream) => {
                if !self.fault_detection_enabled {
                    return Ok(stream);
                }
                let engine_ctx = stream.context();
                let client = self.client.clone();
                let client_for_timeout = self.client.clone();
                let stream = stream.map(move |res| {
                    // 检查错误是否可迁移（表明 worker/连接故障）
                    if let Some(err) = res.err()
                        && is_inhibited(&err)
                    {
                        tracing::debug!(
                            "Reporting instance {instance_id} down due to migratable error: {err}"
                        );
                        client.report_instance_down(instance_id);
                    }
                    res
                });

                // 请求平面不活动超时：当后端停止产出输出时，
                // 发出一个 ResponseTimeout 错误项。这会触发 is_inhibited()
                // → report_instance_down() 以隔离该 worker。
                let stream: Pin<Box<dyn Stream<Item = U> + Send>> = if let Some(timeout) =
                    self.response_timeout
                {
                    Box::pin(async_stream::stream! {
                        let mut inner = Box::pin(stream);
                        loop {
                            tokio::select! {
                                biased;
                                item = inner.next() => {
                                    match item {
                                        Some(item) => yield item,
                                        None => break,
                                    }
                                }
                                _ = tokio::time::sleep(timeout) => {
                                    tracing::warn!(
                                        instance_id,
                                        timeout_secs = timeout.as_secs(),
                                        "backend response inactivity timeout — quarantining worker"
                                    );
                                    client_for_timeout.report_instance_down(instance_id);
                                    yield U::from_err(
                                        crate::error::PagodaError::builder()
                                            .error_type(crate::error::ErrorType::ResponseTimeout)
                                            .message("backend response inactivity timeout")
                                            .build()
                                    );
                                    break;
                                }
                            }
                        }
                    })
                } else {
                    Box::pin(stream)
                };

                Ok(ResponseStream::new(stream, engine_ctx))
            }
            Err(err) => {
                if self.fault_detection_enabled && is_inhibited(err.as_ref()) {
                    tracing::debug!("Reporting instance {instance_id} down due to error: {err}");
                    self.client.report_instance_down(instance_id);
                }
                Err(err)
            }
        }
    }
}

#[async_trait]
impl<T, U> AsyncEngine<SingleIn<T>, ManyOut<U>, Error> for PushRouter<T, U>
where
    T: Data + Serialize,
    U: Data + for<'de> Deserialize<'de> + MaybeError,
{
    async fn generate(&self, request: SingleIn<T>) -> Result<ManyOut<U>, Error> {
        match self.router_mode {
            RouterMode::Random => self.random(request).await,
            RouterMode::RoundRobin => self.round_robin(request).await,
            RouterMode::PowerOfTwoChoices => self.power_of_two_choices(request).await,
            RouterMode::KV => {
                anyhow::bail!("KV routing should not call generate on PushRouter");
            }
            RouterMode::Direct => {
                anyhow::bail!(
                    "Direct routing should not call generate on PushRouter directly; use DirectRoutingRouter wrapper"
                );
            }
            RouterMode::LeastLoaded => self.least_loaded(request).await,
            RouterMode::DeviceAwareWeighted => self.device_aware_weighted(request).await,
        }
    }
}

struct OccupancyTrackedStream<U: Data> {
    inner: ManyOut<U>,
    state: Arc<RoutingOccupancyState>,
    instance_id: u64,
}

impl<U: Data> Drop for OccupancyTrackedStream<U> {
    fn drop(&mut self) {
        self.state.decrement(self.instance_id);
    }
}

impl<U: Data> std::fmt::Debug for OccupancyTrackedStream<U> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OccupancyTrackedStream")
            .field("instance_id", &self.instance_id)
            .finish()
    }
}

impl<U: Data> Stream for OccupancyTrackedStream<U> {
    type Item = U;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl<U: Data> AsyncEngineContextProvider for OccupancyTrackedStream<U> {
    fn context(&self) -> Arc<dyn AsyncEngineContext> {
        self.inner.context()
    }
}

impl<U: Data> crate::engine::AsyncEngineStream<U> for OccupancyTrackedStream<U> {}

#[cfg(test)]
mod tests {
    //! ## 测试矩阵
    //!
    //! | 测试名 | 覆盖维度 |
    //! |---|---|
    //! | `p2c_selects_lower_load_worker` | 两节点 load 悬殊 → 选低载 |
    //! | `p2c_selects_single_worker` | 单节点退化 |
    //! | `p2c_treats_missing_counts_as_zero` | 缺失计数等价 0 |
    //! | `p2c_returns_valid_worker_on_tie` | 平局也只返回候选集成员 |
    //! | `occupancy_permit_decrements_before_stream_creation` | Permit 在流创建前 drop -1 |
    //! | `occupancy_tracked_stream_decrements_on_drop` | Stream drop 时 -1 |
    //! | `p2c_lifecycle_tracks_inflight_counts_with_shared_tracker` | 多 Permit 计数闭环 |
    //! | `p2c_never_selects_dominated_worker` | 绝对劣势节点永不胜出 |
    //! | `least_loaded_selects_exact_min_and_tracks_counts` | 最小负载选精确最小值并跟踪计数 |
    //! | `least_loaded_select_and_peek_return_none_with_available_worker` | 无可用 worker 时 select/peek 返回 None |
    //! | `device_aware_cpu_only_selects_least_loaded_instance` | CPU-only 设备感知选最小负载实例 |
    //! | `device_aware_non_cpu_only_selects_least_loaded_instance` | 非 CPU-only 设备感知选最小负载实例 |
    //! | `device_aware_group_uses_ratio_budget` | 设备感知分组按比例预算分配 |
    //! | `device_aware_weighted_select_and_peek_return_none_with_available_worker` | 加权设备感知无可用 worker 时返回 None |
    //! | `transport_resolution_falls_back_when_selected_instance_disappears` | 选中实例消失时传输解析回退 |
    //! | `transport_resolution_errors_when_no_instances_available` | 无实例可用时传输解析报错 |
    //! | `watcher_dedup_guard_released_on_panic` | Drop guard panic 释放 |
    //! | `watcher_dedup_guard_released_on_normal_exit` | Drop guard 正常释放 |
    //! | `router_mode_default_is_round_robin` | `Default::default()` 锁定 `RoundRobin` |
    //! | `router_mode_is_kv_routing_flag` | `is_kv_routing()` 仅在 `KV` 时 true |
    //! | `router_mode_is_direct_routing_flag` | `is_direct_routing()` 仅在 `Direct` 时 true |
    //! | `router_mode_serde_snake_case` | 枚举 JSON 序列化为 snake_case |
    //! | `p2c_select_from_empty_returns_zero_sentinel_or_panic_documented` | 文档化空入参语义（防回归） |
    //! | `device_aware_group_all_unknown_falls_back_to_full_list` | 全 None device_type → 候选退化为全集 |
    use super::*;
    use crate::{
        DistributedRuntime, Runtime,
        distributed::DistributedConfig,
        error::PagodaError,
        pipeline::{ResponseStream, context::Controller},
    };
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Deserialize, Serialize)]
    struct TestResponse {
        error: Option<PagodaError>,
    }

    impl MaybeError for TestResponse {
        fn from_err(err: impl std::error::Error + 'static) -> Self {
            Self {
                error: Some(PagodaError::from(
                    Box::new(err) as Box<dyn std::error::Error + 'static>
                )),
            }
        }

        fn err(&self) -> Option<PagodaError> {
            self.error.clone()
        }
    }

    #[test]
    fn p2c_selects_lower_load_worker() {
        let state = RoutingOccupancyState::default();
        for _ in 0..10 {
            state.increment(1);
        }
        state.increment(2);

        // 仅有两个 worker 时，p2c_select_from 必须同时考虑二者并选中 id=2（负载更低）。
        let result = p2c_select_from(&state, &[1, 2]);
        assert_eq!(result, 2);
    }

    #[test]
    fn p2c_selects_single_worker() {
        let state = RoutingOccupancyState::default();
        assert_eq!(p2c_select_from(&state, &[42]), 42);
    }

    #[test]
    fn p2c_treats_missing_counts_as_zero() {
        let state = RoutingOccupancyState::default();
        for _ in 0..5 {
            state.increment(1);
        }
        // worker 2 没有条目——应视为 0，因此它会胜出。
        let result = p2c_select_from(&state, &[1, 2]);
        assert_eq!(result, 2);
    }

    #[test]
    fn p2c_returns_valid_worker_on_tie() {
        let state = RoutingOccupancyState::default();
        for _ in 0..3 {
            state.increment(1);
            state.increment(2);
        }

        for _ in 0..100 {
            let result = p2c_select_from(&state, &[1, 2]);
            assert!(result == 1 || result == 2);
        }
    }

    #[test]
    fn occupancy_permit_decrements_before_stream_creation() {
        let state = Arc::new(RoutingOccupancyState::default());
        state.increment(42);
        let permit = OccupancyPermit::new(state.clone(), 42);
        assert_eq!(state.load(42), 1);
        drop(permit);
        assert_eq!(state.load(42), 0);
    }

    #[test]
    fn occupancy_tracked_stream_decrements_on_drop() {
        let state = Arc::new(RoutingOccupancyState::default());
        state.increment(7);
        let permit = OccupancyPermit::new(state.clone(), 7);
        let ctx: Arc<dyn AsyncEngineContext> = Arc::new(Controller::default());
        let stream = permit.into_tracked_stream(ResponseStream::new(
            Box::pin(tokio_stream::iter(vec![1u64])),
            ctx,
        ));
        assert_eq!(state.load(7), 1);
        drop(stream);
        assert_eq!(state.load(7), 0);
    }

    #[test]
    fn p2c_lifecycle_tracks_inflight_counts_with_shared_tracker() {
        let state = Arc::new(RoutingOccupancyState::default());
        let mut permits = Vec::new();
        for _ in 0..5 {
            let selected = p2c_select_from(&state, &[1, 2]);
            state.increment(selected);
            permits.push(OccupancyPermit::new(state.clone(), selected));
        }

        let total = state.load(1) + state.load(2);
        assert_eq!(total, 5, "5 in-flight requests should be tracked");

        drop(permits);
        let total = state.load(1) + state.load(2);
        assert_eq!(total, 0, "All guards dropped, counts should be 0");
    }

    #[test]
    fn p2c_never_selects_dominated_worker() {
        let state = RoutingOccupancyState::default();
        for _ in 0..100 {
            state.increment(3);
        }

        let mut selected = [0u32; 3];
        for _ in 0..1000 {
            let result = p2c_select_from(&state, &[1, 2, 3]);
            match result {
                1 => selected[0] += 1,
                2 => selected[1] += 1,
                3 => selected[2] += 1,
                _ => panic!("unexpected worker id"),
            }
        }
        assert_eq!(
            selected[2], 0,
            "Worker 3 (load=100) should never be selected against load=0 workers, but got {} times",
            selected[2]
        );
    }

    #[tokio::test]
    async fn least_loaded_selects_exact_min_and_tracks_counts() {
        let state = Arc::new(RoutingOccupancyState::default());
        state.increment(1);
        state.increment(1);
        state.increment(2);

        let selected = state
            .select_exact_min_and_increment(&[1, 2, 3])
            .await
            .unwrap();
        assert_eq!(selected, 3);

        let permit = OccupancyPermit::new(state.clone(), selected);
        assert_eq!(state.load(selected), 1);
        drop(permit);
        assert_eq!(state.load(selected), 0);
    }

    #[tokio::test]
    async fn least_loaded_select_and_peek_return_none_with_available_worker() {
        let rt = Runtime::from_current().unwrap();
        let drt = DistributedRuntime::new(rt.clone(), DistributedConfig::process_local())
            .await
            .unwrap();
        let ns = drt
            .namespace("test_least_loaded_router".to_string())
            .unwrap();
        let servicegroup = ns.servicegroup("test_servicegroup".to_string()).unwrap();
        let portname = servicegroup.portname("test_portname".to_string());
        let client = portname.client().await.unwrap();

        portname.register_portname_instance().await.unwrap();
        client.wait_for_instances().await.unwrap();

        let router = PushRouter::<u64, TestResponse>::from_client(client, RouterMode::LeastLoaded)
            .await
            .unwrap();

        assert_eq!(router.select_next_worker(), None);
        assert_eq!(router.peek_next_worker(), None);

        rt.shutdown();
    }

    #[tokio::test]
    async fn device_aware_cpu_only_selects_least_loaded_instance() {
        let state = RoutingOccupancyState::default();
        // 所有候选都是 CPU。让 worker 2 成为负载最低者。
        for _ in 0..3 {
            state.increment(1);
        }
        state.increment(3);

        let instance_ids = vec![1, 2, 3];
        let device_type_map = HashMap::from([
            (1, Some(DeviceType::Cpu)),
            (2, Some(DeviceType::Cpu)),
            (3, Some(DeviceType::Cpu)),
        ]);

        let candidates = device_aware_candidate_group(&state, &instance_ids, &device_type_map, 8);
        assert_eq!(candidates, vec![1, 2, 3]);

        let selected = state
            .select_exact_min_and_increment(&candidates)
            .await
            .unwrap();
        assert_eq!(selected, 2);
    }

    #[tokio::test]
    async fn device_aware_non_cpu_only_selects_least_loaded_instance() {
        let state = RoutingOccupancyState::default();
        // 所有候选都是非 CPU。让 worker 2 成为负载最低者。
        for _ in 0..3 {
            state.increment(1);
        }
        state.increment(3);

        let instance_ids = vec![1, 2, 3];
        let device_type_map = HashMap::from([
            (1, Some(DeviceType::Cuda)),
            (2, Some(DeviceType::Cuda)),
            (3, Some(DeviceType::Cuda)),
        ]);

        let candidates = device_aware_candidate_group(&state, &instance_ids, &device_type_map, 8);
        assert_eq!(candidates, vec![1, 2, 3]);

        let selected = state
            .select_exact_min_and_increment(&candidates)
            .await
            .unwrap();
        assert_eq!(selected, 2);
    }

    #[test]
    fn device_aware_group_uses_ratio_budget() {
        let state = RoutingOccupancyState::default();
        // CPU id：1,2；非 CPU id：3,4
        for _ in 0..4 {
            state.increment(3);
            state.increment(4);
        }
        // CPU 在途数可在不同实例间不同；预算使用 CPU 在途总数。
        for _ in 0..3 {
            state.increment(1);
        }
        // 预算示例：total_non_cpu_inflight=8，cpu_count=2，non_cpu_count=2，ratio=2。
        // allowed_cpu_inflight = 8*2/(2*2)=4。
        // total_cpu_inflight=3 < 4，因此选择 CPU 组。
        let instance_ids = vec![1, 2, 3, 4];
        let device_type_map = HashMap::from([
            (1, Some(DeviceType::Cpu)),
            (2, Some(DeviceType::Cpu)),
            (3, Some(DeviceType::Cuda)),
            (4, Some(DeviceType::Cuda)),
        ]);

        let candidates = device_aware_candidate_group(&state, &instance_ids, &device_type_map, 2);
        assert_eq!(candidates, vec![1, 2]);

        // 在所选 CPU 组内，最终应选负载最低的实例（id=2）。
        let selected =
            futures::executor::block_on(state.select_exact_min_and_increment(&candidates)).unwrap();
        assert_eq!(selected, 2);
    }

    #[tokio::test]
    async fn device_aware_weighted_select_and_peek_return_none_with_available_worker() {
        let rt = Runtime::from_current().unwrap();
        let drt = DistributedRuntime::new(rt.clone(), DistributedConfig::process_local())
            .await
            .unwrap();
        let ns = drt
            .namespace("test_device_aware_router".to_string())
            .unwrap();
        let servicegroup = ns.servicegroup("test_servicegroup".to_string()).unwrap();
        let portname = servicegroup.portname("test_portname".to_string());
        let client = portname.client().await.unwrap();

        portname.register_portname_instance().await.unwrap();
        client.wait_for_instances().await.unwrap();

        let router =
            PushRouter::<u64, TestResponse>::from_client(client, RouterMode::DeviceAwareWeighted)
                .await
                .unwrap();

        assert_eq!(router.select_next_worker(), None);
        assert_eq!(router.peek_next_worker(), None);

        rt.shutdown();
    }

    /// 当 router 选中的实例在选择与传输解析之间已注销时，
    /// 应回退到另一个可用实例，而不是返回 500 错误。
    #[tokio::test]
    async fn transport_resolution_falls_back_when_selected_instance_disappears() {
        let rt = Runtime::from_current().unwrap();
        let drt = DistributedRuntime::new(rt.clone(), DistributedConfig::process_local())
            .await
            .unwrap();
        let ns = drt
            .namespace("test_transport_fallback".to_string())
            .unwrap();
        let servicegroup = ns.servicegroup("test_servicegroup".to_string()).unwrap();
        let portname = servicegroup.portname("test_portname".to_string());
        let client = portname.client().await.unwrap();

        // 注册一个真实实例，使其出现在 instance_source 中。
        portname.register_portname_instance().await.unwrap();
        client.wait_for_instances().await.unwrap();

        let real_id = client.instance_ids()[0];

        // 向 instance_avail 注入一个在 instance_source 中不存在的过期 ID。
        // 这模拟实例在选择后、传输解析前注销的竞争窗口。
        let stale_id = real_id + 1000;
        client.override_instance_avail(vec![stale_id, real_id]);

        // 构建 router 并调用 direct() 以 *真实* 实例为目标，
        // 验证 router 仍能为已知实例解析传输。
        let router =
            PushRouter::<u64, TestResponse>::from_client(client.clone(), RouterMode::RoundRobin)
                .await
                .unwrap();

        // 轮询应成功——即使它先选中 stale_id，回退逻辑也应通过
        // real_id 解析传输。
        // 在没有 worker 的情况下无法完整测试网络发送，但可以
        // 通过检查错误（若有）是否为传输/网络错误，而不是
        // "Instance not found"，来验证这一点。
        let request = SingleIn::new(42u64);
        let result = router.generate(request).await;

        // 请求可能在网络层失败（没有真实 worker），但绝不能
        // 以 "Instance X not found" 失败——那说明回退没有生效。
        if let Err(err) = &result {
            let msg = format!("{err}");
            assert!(
                !msg.contains("not found"),
                "Transport resolution should have fallen back, but got: {msg}"
            );
        }

        rt.shutdown();
    }

    /// 当完全没有可用实例（主路径和回退都没有）时，
    /// router 应返回清晰错误。
    #[tokio::test]
    async fn transport_resolution_errors_when_no_instances_available() {
        let rt = Runtime::from_current().unwrap();
        let drt = DistributedRuntime::new(rt.clone(), DistributedConfig::process_local())
            .await
            .unwrap();
        let ns = drt
            .namespace("test_transport_no_fallback".to_string())
            .unwrap();
        let servicegroup = ns.servicegroup("test_servicegroup".to_string()).unwrap();
        let portname = servicegroup.portname("test_portname".to_string());
        let client = portname.client().await.unwrap();

        // 注册一个实例，以便创建 router（需要传输初始化）。
        portname.register_portname_instance().await.unwrap();
        client.wait_for_instances().await.unwrap();

        let router =
            PushRouter::<u64, TestResponse>::from_client(client.clone(), RouterMode::RoundRobin)
                .await
                .unwrap();

        // 覆盖 avail，使其只包含一个没有真实后端实例、
        // 且没有其他可用回退的过期 ID。
        let stale_id = 99999;
        client.override_instance_avail(vec![stale_id]);

        let request = SingleIn::new(42u64);
        let result = router.generate(request).await;

        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(
            msg.contains("not found") && msg.contains("no other instances available"),
            "Expected clear error about missing instance with no fallback, got: {msg}"
        );

        rt.shutdown();
    }

    /// 即使派生任务 panic，watcher 的去重守卫也必须释放。
    /// 否则，watcher 体内任意位置的 panic 都会留下过期的
    /// `ENDPOINT_WATCHER_ACTIVE` 条目，静默地禁用该 portname 的
    /// 孤儿待处理请求取消，直到进程重启。
    ///
    /// 我们直接针对同一个静态对象演练 Drop guard 模式，而不是
    /// 端到端驱动 `spawn_instance_removal_watcher`（那需要构造一个
    /// 会 panic 的 discovery 流）。该测试复现了生产代码的
    /// GuardRelease 形状；如果生产代码停止使用 Drop guard，
    /// 该集成就会回归，现有的孤儿取消测试也会失败。
    #[tokio::test]
    async fn watcher_dedup_guard_released_on_panic() {
        let portname_id = PortNameId {
            namespace: "panic-test-ns".to_string(),
            servicegroup: "panic-test-sg".to_string(),
            name: "panic-test-portname".to_string(),
        };

        // 模拟生产代码在 spawn 前的去重插入。
        let map = ENDPOINT_WATCHER_ACTIVE.get_or_init(dashmap::DashMap::new);
        map.insert(portname_id.clone(), ());

        let portname_id_clone = portname_id.clone();
        let join = tokio::spawn(async move {
            // 与 `spawn_instance_removal_watcher` 中的形状一致。
            struct GuardRelease(PortNameId);
            impl Drop for GuardRelease {
                fn drop(&mut self) {
                    if let Some(map) = ENDPOINT_WATCHER_ACTIVE.get() {
                        map.remove(&self.0);
                    }
                }
            }
            let _release = GuardRelease(portname_id_clone);
            panic!("simulated watcher-task panic");
        });

        let result = join.await;
        assert!(result.is_err() && result.unwrap_err().is_panic());
        assert!(
            !map.contains_key(&portname_id),
            "Drop guard must release the dedup entry even on panic"
        );
    }

    /// 正常退出路径：任务在未 panic 的情况下结束时，Drop guard
    /// 会释放条目。这是日常情况（cancel_token 触发或 discovery
    /// 流关闭）。
    #[tokio::test]
    async fn watcher_dedup_guard_released_on_normal_exit() {
        let portname_id = PortNameId {
            namespace: "normal-test-ns".to_string(),
            servicegroup: "normal-test-sg".to_string(),
            name: "normal-test-portname".to_string(),
        };

        let map = ENDPOINT_WATCHER_ACTIVE.get_or_init(dashmap::DashMap::new);
        map.insert(portname_id.clone(), ());

        let portname_id_clone = portname_id.clone();
        tokio::spawn(async move {
            struct GuardRelease(PortNameId);
            impl Drop for GuardRelease {
                fn drop(&mut self) {
                    if let Some(map) = ENDPOINT_WATCHER_ACTIVE.get() {
                        map.remove(&self.0);
                    }
                }
            }
            let _release = GuardRelease(portname_id_clone);
            // 任务体正常返回
        })
        .await
        .unwrap();

        assert!(!map.contains_key(&portname_id));
    }

    // ── 新增：RouterMode 契约面与小工具语义 ─────────────────────────────────

    #[test]
    fn router_mode_default_is_round_robin() {
        let m = RouterMode::default();
        assert_eq!(m, RouterMode::RoundRobin);
    }

    #[test]
    fn router_mode_is_kv_routing_flag() {
        assert!(RouterMode::KV.is_kv_routing());
        for m in [
            RouterMode::RoundRobin,
            RouterMode::Random,
            RouterMode::PowerOfTwoChoices,
            RouterMode::Direct,
            RouterMode::LeastLoaded,
            RouterMode::DeviceAwareWeighted,
        ] {
            assert!(!m.is_kv_routing(), "{m:?} 不应是 KV routing");
        }
    }

    #[test]
    fn router_mode_is_direct_routing_flag() {
        assert!(RouterMode::Direct.is_direct_routing());
        for m in [
            RouterMode::RoundRobin,
            RouterMode::Random,
            RouterMode::PowerOfTwoChoices,
            RouterMode::KV,
            RouterMode::LeastLoaded,
            RouterMode::DeviceAwareWeighted,
        ] {
            assert!(!m.is_direct_routing(), "{m:?} 不应是 Direct routing");
        }
    }

    #[test]
    fn router_mode_serde_snake_case() {
        // 选两个有大小写差异的样本验证 rename_all = snake_case
        let s = serde_json::to_string(&RouterMode::PowerOfTwoChoices).unwrap();
        assert_eq!(s, "\"power_of_two_choices\"");
        let s = serde_json::to_string(&RouterMode::DeviceAwareWeighted).unwrap();
        assert_eq!(s, "\"device_aware_weighted\"");
        // round-trip
        let back: RouterMode = serde_json::from_str("\"round_robin\"").unwrap();
        assert_eq!(back, RouterMode::RoundRobin);
    }

    #[test]
    fn device_aware_group_all_unknown_falls_back_to_full_list() {
        // device_type 全 None：无法做分区 → 应回退到完整 instance_ids 列表
        let state = RoutingOccupancyState::default();
        let instance_ids = vec![1, 2, 3];
        let device_type_map: HashMap<u64, Option<DeviceType>> =
            HashMap::from([(1, None), (2, None), (3, None)]);

        let candidates =
            device_aware_candidate_group(&state, &instance_ids, &device_type_map, 8);
        // 不强求顺序，但元素集合必须等于全集
        let mut got = candidates.clone();
        got.sort();
        assert_eq!(got, vec![1, 2, 3], "全 None device_type 应回退到全集，got: {candidates:?}");
    }
}
