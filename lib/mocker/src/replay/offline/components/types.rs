// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-License-Identifier: Apache-2.0
//
// API-compatible implementation based on the public interfaces and behavioral
// contracts of NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
// Implementation rewritten by PAGODA.

//! # 离线重放共享类型
//!
//! ## 设计意图
//! 定义离线重放各组件间传递的枚举与效果结构，并实现跨 planner tick 累积流量统计的累加器。
//!
//! ## 外部契约
//! 公开 `TrafficStats`（字段名与 PyO3 绑定、Python 适配器读取的键严格对齐）；`WorkerAdmission`/`ReplayMode` 等类型的字段语义与 Dynamo 保持一致。

use pagoda_kv_router::protocols::RouterEvent;
use uuid::Uuid;

use super::super::runtime_utils::WorkerCompletionPayload;
use crate::common::protocols::{DirectRequest, ForwardPassSnapshot};
use crate::loadgen::ReplayRequestHashes;
use crate::scheduler::AdmissionEvent;

#[derive(Debug, Clone, Copy)]
pub(in crate::replay) enum ReplayMode {
    Trace,
    Concurrency { max_in_flight: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::replay::offline) enum EnginePassMode {
    Visible,
    Hidden,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WorkerAdmission {
    pub(crate) uuid: Uuid,
    pub(crate) worker_idx: usize,
    /// router 在准入时针对前缀缓存匹配到的块数。
    /// 由流量累加器用于为 planner 推导平均 KV 命中率。
    pub(crate) overlap_blocks: u32,
    /// 以块表示的 ISL 总量（ceil(isl_tokens / block_size)），
    /// 与 ``overlap_blocks`` 配对用于计算命中率。
    pub(crate) isl_blocks: u32,
}

#[derive(Debug)]
pub(in crate::replay::offline) struct ScheduledWorkerCompletion {
    pub(in crate::replay::offline) at_ms: f64,
    pub(in crate::replay::offline) payload: WorkerCompletionPayload,
}

#[derive(Debug, Default)]
pub(in crate::replay::offline) struct EngineEffects {
    pub(in crate::replay::offline) admissions: Vec<AdmissionEvent>,
    pub(in crate::replay::offline) pass_start_kv_events: Vec<RouterEvent>,
    pub(in crate::replay::offline) immediate_completions: Vec<WorkerCompletionPayload>,
    pub(in crate::replay::offline) scheduled_completions: Vec<ScheduledWorkerCompletion>,
    /// 本次驱动周期中各 worker 发出的 forward pass 指标快照，
    /// 以 worker 索引为键。用于 planner 集成。
    pub(in crate::replay::offline) fpm_snapshots: Vec<(usize, ForwardPassSnapshot)>,
}

impl EngineEffects {
    pub(in crate::replay::offline) fn is_empty(&self) -> bool {
        self.admissions.is_empty()
            && self.pass_start_kv_events.is_empty()
            && self.immediate_completions.is_empty()
            && self.scheduled_completions.is_empty()
    }
}

#[derive(Debug, Default)]
pub(crate) struct RouterEffects {
    pub(crate) admissions: Vec<WorkerAdmission>,
}

#[derive(Debug)]
pub(in crate::replay::offline) struct ReadyArrival {
    pub(in crate::replay::offline) request: DirectRequest,
    pub(in crate::replay::offline) arrival_time_ms: f64,
    pub(in crate::replay::offline) replay_hashes: Option<ReplayRequestHashes>,
}

/// [`TrafficAccumulator::drain`] 返回的累积流量统计。
///
/// 重要：当此处字段新增或重命名时，需同步更新
/// ``lib/bindings/python/rust/llm/replay.rs`` 中的 PyO3 绑定
/// （drain_traffic 方法），使导出的 JSON dict 匹配。``replay_adapter.py``
/// 中的 Python 适配器按名称读取这些键。
#[derive(Debug, Clone)]
pub struct TrafficStats {
    pub duration_s: f64,
    pub num_req: usize,
    pub avg_isl: f64,
    pub avg_osl: f64,
    pub avg_ttft_ms: f64,
    pub avg_itl_ms: f64,
    /// 窗口内各 router 准入的平均前缀缓存命中率（0.0-1.0），
    /// 按已准入请求计算 ``mean(overlap_blocks / isl_blocks)``
    /// （即逐请求比值的算术平均）。与真实 router 的
    /// ``pagoda_component_router_kv_hit_rate`` Prometheus 直方图
    /// 语义一致 —— 后者每个请求观测一个 ``overlap/isl`` 样本；
    /// PromQL 查询 ``sum(increase(_sum)) / sum(increase(_count))``
    /// 返回这些样本的算术平均，与逐请求 ISL 大小无关。
    pub avg_kv_hit_rate: f64,
}

/// 在 planner tick 之间累积流量统计，用于推导
/// `TrafficObservation`（num_req、平均 ISL、平均 OSL、平均延迟、
/// 窗口内平均 KV 命中率）。
///
/// 延迟样本独立于请求计数跟踪：只有记录了正 TTFT 的请求
/// 才贡献到 ``total_ttft_ms`` / ``ttft_count``，ITL 同理。这意味着
/// ``avg_ttft_ms`` 与 ``avg_itl_ms`` 只反映实际产生该样本的请求，
/// 而不会在部分请求缺少延迟数据（例如在出 token 前失败的
/// 请求）时静默地低估。
///
/// KV 命中率观测来自 router 在准入时（而非完成时），并按
/// 逐请求比值记录，与真实 router 的逐请求直方图一致：每次
/// 准入贡献一个 ``overlap_blocks / isl_blocks`` 样本到运行均值，
/// 因此大请求不会比小请求获得更高权重。
#[derive(Debug)]
pub(in crate::replay::offline) struct TrafficAccumulator {
    window_start_ms: f64,
    num_req: usize,
    total_isl: usize,
    total_osl: usize,
    total_ttft_ms: f64,
    total_itl_ms: f64,
    ttft_count: usize,
    itl_count: usize,
    /// 逐请求命中率比值（``overlap / isl``）的运行总和；
    /// 排空时除以 ``hit_rate_count`` 得到均值。
    total_hit_rate: f64,
    /// 当前窗口内 ISL 块数非零的准入次数。
    hit_rate_count: usize,
}

impl TrafficAccumulator {
    pub(in crate::replay::offline) fn new() -> Self {
        Self {
            window_start_ms: 0.0,
            num_req: 0,
            total_isl: 0,
            total_osl: 0,
            total_ttft_ms: 0.0,
            total_itl_ms: 0.0,
            ttft_count: 0,
            itl_count: 0,
            total_hit_rate: 0.0,
            hit_rate_count: 0,
        }
    }

    /// 记录一个带可选延迟数据的已完成请求。
    pub(in crate::replay::offline) fn on_request(
        &mut self,
        input_tokens: usize,
        output_tokens: usize,
        latencies: Option<(f64, f64)>,
    ) {
        self.num_req += 1;
        self.total_isl += input_tokens;
        self.total_osl += output_tokens;
        if let Some((ttft_ms, mean_itl_ms)) = latencies {
            if ttft_ms > 0.0 {
                self.total_ttft_ms += ttft_ms;
                self.ttft_count += 1;
            }
            if mean_itl_ms > 0.0 {
                self.total_itl_ms += mean_itl_ms;
                self.itl_count += 1;
            }
        }
    }

    /// 将一次 router 准入的前缀缓存重叠记录为逐请求比值。
    /// 在准入时（而非完成时）调用，使平均命中率反映 router
    /// 在路由决策时的视角 —— 与真实 router 的逐请求直方图一致，
    /// 其中每个请求恰好贡献一个 ``overlap/isl`` 样本。
    /// ``isl_blocks == 0`` 的准入被跳过（无意义比值），
    /// 与 ``RequestTracker::kv_hit_rate()`` 在该情况下返回
    /// ``None`` 一致。
    pub(in crate::replay::offline) fn on_admission(
        &mut self,
        overlap_blocks: u32,
        isl_blocks: u32,
    ) {
        if isl_blocks == 0 {
            return;
        }
        self.total_hit_rate += f64::from(overlap_blocks) / f64::from(isl_blocks);
        self.hit_rate_count += 1;
    }

    /// 在给定仿真时间排空累加器，并重置计数器。
    pub(in crate::replay::offline) fn drain(&mut self, now_ms: f64) -> TrafficStats {
        let duration_s = (now_ms - self.window_start_ms) / 1000.0;
        let num_req = self.num_req;
        let avg_isl = if num_req > 0 {
            self.total_isl as f64 / num_req as f64
        } else {
            0.0
        };
        let avg_osl = if num_req > 0 {
            self.total_osl as f64 / num_req as f64
        } else {
            0.0
        };
        let avg_ttft_ms = if self.ttft_count > 0 {
            self.total_ttft_ms / self.ttft_count as f64
        } else {
            0.0
        };
        let avg_itl_ms = if self.itl_count > 0 {
            self.total_itl_ms / self.itl_count as f64
        } else {
            0.0
        };
        let avg_kv_hit_rate = if self.hit_rate_count > 0 {
            self.total_hit_rate / self.hit_rate_count as f64
        } else {
            0.0
        };
        self.window_start_ms = now_ms;
        self.num_req = 0;
        self.total_isl = 0;
        self.total_osl = 0;
        self.total_ttft_ms = 0.0;
        self.total_itl_ms = 0.0;
        self.ttft_count = 0;
        self.itl_count = 0;
        self.total_hit_rate = 0.0;
        self.hit_rate_count = 0;
        TrafficStats {
            duration_s,
            num_req,
            avg_isl,
            avg_osl,
            avg_ttft_ms,
            avg_itl_ms,
            avg_kv_hit_rate,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn traffic_accumulator_drain_with_no_admissions_reports_zero_hit_rate() {
        let mut acc = TrafficAccumulator::new();
        acc.on_request(100, 50, None);
        let stats = acc.drain(1_000.0);
        assert_eq!(stats.num_req, 1);
        assert!((stats.avg_isl - 100.0).abs() < 1e-9);
        assert!((stats.avg_osl - 50.0).abs() < 1e-9);
        assert_eq!(stats.avg_kv_hit_rate, 0.0);
    }

    #[test]
    fn traffic_accumulator_hit_rate_is_mean_of_per_request_ratios() {
        let mut acc = TrafficAccumulator::new();
        // 小请求：大部分命中。大请求：未命中。
        acc.on_admission(3, 4); // 逐请求比值：0.75
        acc.on_admission(0, 12); // 逐请求比值：0.0
        acc.on_request(256, 32, None);
        acc.on_request(768, 32, None);
        let stats = acc.drain(1_000.0);
        assert_eq!(stats.num_req, 2);
        // 逐请求均值与真实 router 的 Prometheus 直方图一致：
        // (0.75 + 0.0) / 2 = 0.375。每个请求无论 ISL 大小都贡献一个
        // 样本，因此大请求不会占主导。
        assert!((stats.avg_kv_hit_rate - 0.375).abs() < 1e-9);
    }

    #[test]
    fn traffic_accumulator_skips_admissions_with_zero_isl_blocks() {
        let mut acc = TrafficAccumulator::new();
        acc.on_admission(0, 0); // 被跳过 -- 无意义比值
        acc.on_admission(2, 4); // 比值 = 0.5
        let stats = acc.drain(1_000.0);
        // 只有非零 ISL 的样本计入均值。
        assert!((stats.avg_kv_hit_rate - 0.5).abs() < 1e-9);
    }

    #[test]
    fn traffic_accumulator_resets_counters_on_drain() {
        let mut acc = TrafficAccumulator::new();
        acc.on_admission(5, 10);
        acc.on_request(100, 50, None);
        let _ = acc.drain(1_000.0);
        // 对同一累加器的第二次排空应看不到任何遗留状态。
        let stats = acc.drain(2_000.0);
        assert!((stats.duration_s - 1.0).abs() < 1e-9);
        assert_eq!(stats.num_req, 0);
        assert_eq!(stats.avg_isl, 0.0);
        assert_eq!(stats.avg_osl, 0.0);
        assert_eq!(stats.avg_kv_hit_rate, 0.0);
    }
}
