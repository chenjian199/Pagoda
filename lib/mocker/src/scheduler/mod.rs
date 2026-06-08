// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-License-Identifier: Apache-2.0
//
// API-compatible implementation based on the public interfaces and behavioral
// contracts of NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
// Implementation rewritten by PAGODA.

//! # 引擎相关的调度实现
//!
//! ## 设计意图
//! 以引擎无关的方式封装调度入口：度量累积（Welford）、前向快照构建、引擎核心与调度器的分发枚举，
//! 以及对外的 [`SchedulerHandle`] trait。
//!
//! ## 外部契约
//! - 对外导出 [`ForwardPassSnapshot`]、`Scheduler`、`MockerMetrics`、[`SchedulerHandle`]；
//!   crate 内导出 `VllmCore`、缓冲/快照相关类型与函数。
//! - [`SchedulerHandle`] 的方法集合与语义保持稳定。
//!
//! ## 实现要点
//! - Welford 在线算法计算 count/sum/总体方差；快照构建对调度与排队两类请求分别累积。

mod kv_event_sink;
pub mod vllm;

use pagoda_kv_router::protocols::RouterEvent;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub use crate::common::protocols::ForwardPassSnapshot;
use crate::common::protocols::{DirectRequest, FpmPublisher, KvEventPublishers, OutputSignal};
pub(crate) use kv_event_sink::{
    CapturedRouterEventBuffer, DeferredFpmBuffer, capture_deferred_kv_publish_sink,
    capture_router_event_sink, publish_deferred_fpm, publish_deferred_kv_events,
};

// === SECTION: Welford 累积器 ===

/// Welford 在线算法，计算 count / sum / 总体方差。
///
/// 对应 `forward_pass_metrics.py` 中的 Python `WelfordAccumulator`。
#[derive(Default)]
pub(crate) struct WelfordAcc {
    pub(crate) count: u32,
    pub(crate) sum: f64,
    mean: f64,
    m2: f64,
}

impl WelfordAcc {
    pub(crate) fn add(&mut self, v: f64) {
        self.count += 1;
        self.sum += v;
        let delta = v - self.mean;
        self.mean += delta / self.count as f64;
        self.m2 += delta * (v - self.mean);
    }

    pub(crate) fn variance(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.m2 / self.count as f64
        }
    }
}

// === SECTION: 前向快照构建 ===

/// 从引擎无关的迭代器构建 [`ForwardPassSnapshot`]。
///
/// 各引擎以自己的迭代器调用本函数，避免重复的方差/累积逻辑。
///
/// - `scheduled_prefills`：每请求的 `(prompt_len, prefix_tokens, tokens_computed)`；
/// - `scheduled_decodes`：每请求的 `sequence_len`；
/// - `queued_prefills`：每个等待 prefill 请求的 `prompt_len`；
/// - `queued_decodes`：每个被抢占 decode 请求的 `kv_tokens`。
pub(crate) fn build_fpm_snapshot(
    scheduled_prefills: impl Iterator<Item = (u64, u64, u64)>,
    scheduled_decodes: impl Iterator<Item = u64>,
    queued_prefills: impl Iterator<Item = u64>,
    queued_decodes: impl Iterator<Item = u64>,
    wall_time_secs: f64,
) -> ForwardPassSnapshot {
    let mut prefill_acc = WelfordAcc::default();
    let mut sum_prefill_tokens: u64 = 0;
    let mut sum_prefill_kv_tokens: u64 = 0;
    for (prompt_len, prefix_tokens, tokens_computed) in scheduled_prefills {
        sum_prefill_tokens += tokens_computed;
        sum_prefill_kv_tokens += prefix_tokens;
        prefill_acc.add(prompt_len as f64);
    }

    let mut decode_acc = WelfordAcc::default();
    for sequence_len in scheduled_decodes {
        decode_acc.add(sequence_len as f64);
    }

    let mut queued_prefill_acc = WelfordAcc::default();
    for prompt_len in queued_prefills {
        queued_prefill_acc.add(prompt_len as f64);
    }

    let mut queued_decode_acc = WelfordAcc::default();
    for kv_tokens in queued_decodes {
        queued_decode_acc.add(kv_tokens as f64);
    }

    ForwardPassSnapshot {
        num_prefill_requests: prefill_acc.count,
        sum_prefill_tokens,
        var_prefill_length: prefill_acc.variance(),
        sum_prefill_kv_tokens,
        num_decode_requests: decode_acc.count,
        sum_decode_kv_tokens: decode_acc.sum as u64,
        var_decode_kv_tokens: decode_acc.variance(),
        num_queued_prefill: queued_prefill_acc.count,
        sum_queued_prefill_tokens: queued_prefill_acc.sum as u64,
        var_queued_prefill_length: queued_prefill_acc.variance(),
        num_queued_decode: queued_decode_acc.count,
        sum_queued_decode_kv_tokens: queued_decode_acc.sum as u64,
        var_queued_decode_kv_tokens: queued_decode_acc.variance(),
        wall_time_secs,
    }
}

pub(crate) use vllm::VllmCore;
pub use vllm::{MockerMetrics, Scheduler};

// === SECTION: 引擎分发枚举 ===

#[derive(Debug, Clone)]
pub(crate) struct AdmissionEvent {
    pub(crate) uuid: Uuid,
    pub(crate) reused_input_tokens: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct EnginePassResult {
    pub(crate) end_ms: f64,
    pub(crate) completed_requests: usize,
    pub(crate) output_signals: Vec<OutputSignal>,
    pub(crate) admissions: Vec<AdmissionEvent>,
    pub(crate) active_decode_blocks: u64,
    /// 控制 replay/live 调度器何时把本轮缓冲的 KV 事件暴露给真实 router 或 publisher。
    pub(crate) router_event_visibility: RouterEventVisibility,
    /// 本轮发出的、对 router 可见的 KV 事件。
    pub(crate) kv_events: Vec<RouterEvent>,
    /// 本轮的前向度量快照。
    pub(crate) fpm: Option<ForwardPassSnapshot>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RouterEventVisibility {
    /// 在本轮开始（建模 sleep 之前）暴露缓冲的 KV 事件。
    PassStart,
    /// 在本轮结束（输出 flush 之前）暴露缓冲的 KV 事件。
    PassEnd,
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum EngineCore {
    Vllm(VllmCore),
}

impl EngineCore {
    pub(crate) fn receive(&mut self, request: DirectRequest) -> Uuid {
        match self {
            Self::Vllm(core) => core.receive(request),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Self::Vllm(core) => core.is_empty(),
        }
    }

    pub(crate) fn num_requests(&self) -> usize {
        match self {
            Self::Vllm(core) => core.num_requests(),
        }
    }

    pub(crate) fn execute_pass(
        &mut self,
        collector: &mut crate::replay::TraceCollector,
        now_ms: f64,
    ) -> EnginePassResult {
        match self {
            Self::Vllm(core) => core.execute_pass(collector, now_ms),
        }
    }

    pub(crate) fn execute_hidden_pass(&mut self, now_ms: f64) -> EnginePassResult {
        match self {
            Self::Vllm(core) => core.execute_hidden_pass(now_ms),
        }
    }
}

#[derive(Clone)]
pub(crate) enum EngineScheduler {
    Vllm(Scheduler),
}

impl EngineScheduler {
    pub(crate) fn new_with_admission(
        args: crate::common::protocols::MockEngineArgs,
        dp_rank: u32,
        output_tx: Option<mpsc::UnboundedSender<Vec<OutputSignal>>>,
        kv_event_publishers: KvEventPublishers,
        cancellation_token: Option<CancellationToken>,
        admission_tx: Option<mpsc::UnboundedSender<AdmissionEvent>>,
        fpm_publisher: FpmPublisher,
    ) -> Self {
        match args.engine_type {
            crate::common::protocols::EngineType::Vllm => {
                Self::Vllm(Scheduler::new_with_admission(
                    args,
                    dp_rank,
                    output_tx,
                    kv_event_publishers,
                    cancellation_token,
                    admission_tx,
                    fpm_publisher,
                ))
            }
        }
    }
}

impl SchedulerHandle for EngineScheduler {
    fn receive(&self, request: DirectRequest) {
        match self {
            Self::Vllm(scheduler) => scheduler.receive(request),
        }
    }

    fn request_sender(&self) -> mpsc::UnboundedSender<DirectRequest> {
        match self {
            Self::Vllm(scheduler) => scheduler.request_sender(),
        }
    }

    fn metrics_receiver(&self) -> tokio::sync::watch::Receiver<MockerMetrics> {
        match self {
            Self::Vllm(scheduler) => scheduler.metrics_receiver(),
        }
    }
}

// === SECTION: 对外句柄 trait ===

/// 引擎无关的调度器接口。
///
/// 各后端调度器实现此 trait，使引擎包装层（`MockEngine`）能以同一 API 操作任意后端。
pub trait SchedulerHandle: Send + Sync {
    /// 把请求送入调度器的等待队列。
    fn receive(&self, request: DirectRequest);

    /// 克隆一份请求发送端，用于直接发送。
    fn request_sender(&self) -> mpsc::UnboundedSender<DirectRequest>;

    /// 获取调度器度量（活跃 decode 块数等）的 watch 接收端。
    fn metrics_receiver(&self) -> tokio::sync::watch::Receiver<MockerMetrics>;
}

/// 调度器压力测试共用的测试工具。
#[cfg(test)]
pub(crate) mod test_utils;

#[cfg(test)]
mod tests {
    //! ## 测试过程
    //! 针对 [`WelfordAcc`] 覆盖空集、单值、已知样本集与一组与 Python 实现对齐的样本，
    //! 校验 count/sum 与总体方差。
    //!
    //! ## 意义
    //! 总体方差是前向度量快照的核心字段，须与 Python `WelfordAccumulator` 数值一致，
    //! 否则下游 planner 决策会偏移。
    use super::*;

    #[test]
    fn empty_accumulator_reports_zeros() {
        let acc = WelfordAcc::default();
        assert_eq!(acc.count, 0);
        assert_eq!(acc.sum, 0.0);
        assert_eq!(acc.variance(), 0.0);
    }

    #[test]
    fn single_sample_has_zero_variance() {
        let mut acc = WelfordAcc::default();
        acc.add(42.0);
        assert_eq!(acc.count, 1);
        assert_eq!(acc.sum, 42.0);
        assert_eq!(acc.variance(), 0.0);
    }

    #[test]
    fn known_sample_set_population_variance() {
        // 样本 2,4,4,4,5,5,7,9，均值 5，总体方差 4.0。
        let mut acc = WelfordAcc::default();
        for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            acc.add(v);
        }
        assert_eq!(acc.count, 8);
        assert_eq!(acc.sum, 40.0);
        assert!((acc.variance() - 4.0).abs() < 1e-10);
    }

    #[test]
    fn matches_python_welford_accumulator() {
        // values = [100, 200, 300]，均值 200，
        // 总体方差 = (10000 + 0 + 10000) / 3 = 20000/3。
        let mut acc = WelfordAcc::default();
        acc.add(100.0);
        acc.add(200.0);
        acc.add(300.0);
        assert_eq!(acc.count, 3);
        assert_eq!(acc.sum, 600.0);
        let expected = 20000.0 / 3.0;
        assert!(
            (acc.variance() - expected).abs() < 1e-10,
            "expected {expected}, got {}",
            acc.variance()
        );
    }
}
