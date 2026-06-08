// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-License-Identifier: Apache-2.0

//! 驱动「planner 在环」离线重放的公开句柄。
//!
//! 通过 [`RuntimeKind`] 同时支持聚合与分离两种拓扑。
//! Python planner 适配器调用 [`PlannerReplayHandle::advance_to`]
//! 推进仿真、收集指标，并调用 [`PlannerReplayHandle::apply_scaling`]
//! 调整 worker 池规模。

use std::path::Path;
use std::time::Instant;

use anyhow::Result;
use pagoda_kv_router::config::KvRouterConfig;

use super::offline::agg::AggRuntime;
use super::offline::components::{ReplayMode, TrafficStats};
use super::offline::disagg::DisaggRuntime;
use super::{
    OfflineDisaggReplayConfig, ReplayPrefillLoadEstimator, ReplayRouterMode, TraceSimulationReport,
};
use crate::common::protocols::{ForwardPassSnapshot, MockEngineArgs};
use crate::loadgen::Trace;

/// planner tick 之间收集的指标快照。
///
/// 在聚合模式下，prefill 字段为 0，所有数据都在 decode 字段中
///（与 planner 把 agg 视为单一 decode 阶段引擎的方式一致）。
///
/// 此处不包含流量指标 —— 它们跨 tick 累积，
/// 必须仅在吞吐缩放 tick 上通过 [`PlannerReplayHandle::drain_traffic`]
/// 显式排空。每个 tick 都排空会丢弃更频繁的负载缩放 tick
/// 之间的数据。
pub struct PlannerTickData {
    /// 当前仿真时间（毫秒）。
    pub now_ms: f64,
    /// 重放是否已结束（无更多工作）。
    pub is_done: bool,
    /// 自上次 tick 以来的 prefill FPM 快照：(worker_id, snapshot)。
    pub prefill_fpm_snapshots: Vec<(usize, ForwardPassSnapshot)>,
    /// 自上次 tick 以来的 decode（或 agg）FPM 快照：(worker_id, snapshot)。
    pub decode_fpm_snapshots: Vec<(usize, ForwardPassSnapshot)>,
    /// 活跃的 prefill worker 数（agg 模式下为 0）。
    pub active_prefill_count: usize,
    /// 活跃的 decode worker 数（agg 模式下为总活跃数）。
    pub active_decode_count: usize,
    /// 包含待移除在内的 prefill worker 总数（agg 模式下为 0）。
    pub total_prefill_count: usize,
    /// 包含待移除在内的 decode worker 总数（agg 模式下为总数）。
    pub total_decode_count: usize,
}

#[allow(clippy::large_enum_variant)]
enum RuntimeKind {
    Agg(AggRuntime),
    Disagg(DisaggRuntime),
}

pub struct PlannerReplayHandle {
    runtime: RuntimeKind,
    started_at: Instant,
}

impl PlannerReplayHandle {
    /// 为聚合型 trace 文件重放创建一个句柄。
    #[allow(clippy::too_many_arguments)]
    pub fn from_trace_file(
        args: MockEngineArgs,
        router_config: Option<KvRouterConfig>,
        prefill_load_estimator: Option<ReplayPrefillLoadEstimator>,
        trace_path: &Path,
        trace_block_size: usize,
        num_workers: usize,
        arrival_speedup_ratio: f64,
        router_mode: ReplayRouterMode,
    ) -> Result<Self> {
        let args = args.normalized()?;
        let trace = Trace::from_mooncake(trace_path, trace_block_size)?
            .normalize_session_starts()?
            .speed_up_timing(arrival_speedup_ratio)?;
        let runtime = AggRuntime::new_workload(
            &args,
            router_config,
            prefill_load_estimator,
            trace.into_trace_driver_with_block_size(args.block_size)?,
            num_workers,
            ReplayMode::Trace,
            router_mode,
        )?;
        Ok(Self {
            runtime: RuntimeKind::Agg(runtime),
            started_at: Instant::now(),
        })
    }

    /// 为分离型 trace 文件重放创建一个句柄。
    pub fn from_trace_file_disagg(
        config: OfflineDisaggReplayConfig,
        router_config: Option<KvRouterConfig>,
        prefill_load_estimator: Option<ReplayPrefillLoadEstimator>,
        trace_path: &Path,
        trace_block_size: usize,
        arrival_speedup_ratio: f64,
        router_mode: ReplayRouterMode,
    ) -> Result<Self> {
        let config = config.normalized()?;
        let trace = Trace::from_mooncake(trace_path, trace_block_size)?
            .normalize_session_starts()?
            .speed_up_timing(arrival_speedup_ratio)?;
        let runtime = DisaggRuntime::new_workload(
            &config,
            router_config,
            prefill_load_estimator,
            trace.into_trace_driver_with_block_size(config.decode_args.block_size)?,
            ReplayMode::Trace,
            router_mode,
        )?;
        Ok(Self {
            runtime: RuntimeKind::Disagg(runtime),
            started_at: Instant::now(),
        })
    }

    /// 推进仿真至 `until_ms`，收集指标，返回 tick 数据。
    ///
    /// 此处不排空流量指标 —— 请在吞吐缩放 tick 上显式调用 [`drain_traffic`]，
    /// 以保证累加器覆盖整个区间。
    pub fn advance_to(&mut self, until_ms: f64) -> Result<PlannerTickData> {
        match &mut self.runtime {
            RuntimeKind::Agg(rt) => {
                let is_done = rt.advance_to(until_ms)?;
                let fpm = rt.drain_fpm();
                Ok(PlannerTickData {
                    now_ms: rt.now_ms(),
                    is_done,
                    prefill_fpm_snapshots: Vec::new(),
                    decode_fpm_snapshots: fpm,
                    active_prefill_count: 0,
                    active_decode_count: rt.active_worker_count(),
                    total_prefill_count: 0,
                    total_decode_count: rt.total_worker_count(),
                })
            }
            RuntimeKind::Disagg(rt) => {
                let is_done = rt.advance_to(until_ms)?;
                let prefill_fpm = rt.drain_prefill_fpm();
                let decode_fpm = rt.drain_decode_fpm();
                Ok(PlannerTickData {
                    now_ms: rt.now_ms(),
                    is_done,
                    prefill_fpm_snapshots: prefill_fpm,
                    decode_fpm_snapshots: decode_fpm,
                    active_prefill_count: rt.active_prefill_count(),
                    active_decode_count: rt.active_decode_count(),
                    total_prefill_count: rt.total_prefill_count(),
                    total_decode_count: rt.total_decode_count(),
                })
            }
        }
    }

    /// 排空自上次排空以来累积的流量指标。
    ///
    /// 仅在吞吐缩放 tick 上调用，以使窗口覆盖整个
    /// `throughput_adjustment_interval`，而不仅是负载 tick 之间的间隙。
    /// 返回的 [`TrafficStats::avg_kv_hit_rate`] 是窗口内各次准入
    /// 逐请求 ``overlap / isl`` 比值的算术平均 —— 与真实 router 的
    /// 逐请求 Prometheus 直方图一致，其中每个请求无论 ISL 大小
    /// 都贡献一个样本。
    pub fn drain_traffic(&mut self) -> TrafficStats {
        match &mut self.runtime {
            RuntimeKind::Agg(rt) => rt.drain_traffic(),
            RuntimeKind::Disagg(rt) => rt.drain_traffic(),
        }
    }

    /// 应用一个带有独立 prefill 与 decode 目标的缩放决策。
    /// 对于 agg 模式，`target_prefill` 被忽略。
    pub fn apply_scaling(&mut self, target_prefill: usize, target_decode: usize) -> Result<()> {
        match &mut self.runtime {
            RuntimeKind::Agg(rt) => rt.apply_scaling(target_decode),
            RuntimeKind::Disagg(rt) => rt.apply_scaling(target_prefill, target_decode),
        }
    }

    /// 结束重放并返回报告。
    pub fn finalize(self) -> TraceSimulationReport {
        let wall_time_ms = self.started_at.elapsed().as_secs_f64() * 1000.0;
        let report = match self.runtime {
            RuntimeKind::Agg(rt) => rt.finalize_report(),
            RuntimeKind::Disagg(rt) => rt.finalize_report(),
        };
        report.with_wall_time_ms(wall_time_ms)
    }
}
