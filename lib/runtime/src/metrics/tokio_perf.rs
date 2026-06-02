// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # Tokio Runtime 指标 + 事件循环 canary
//!
//! ## 设计意图
//!
//! Tokio 的 `RuntimeMetrics` 把 worker 调度细节暴露出来，但它的接口是“当前值”
//! 而非 Prometheus 风格的“可累加 counter”，且采样需要在 runtime 上下文中进行。
//! 本模块提供：
//!
//! 1. **指标集**：14 个 Lazy 单例覆盖全局队列 / blocking / worker 三维度，
//!    并把 worker 单调增量从 RuntimeMetrics 的“总值”差分回 Counter 语义；
//! 2. **采样循环** [`tokio_metrics_and_canary_loop`]：每 1s 更新一次 runtime
//!    指标，同时每 10ms sleep 一次作为**事件循环卡顿探针**（canary）。
//!    sleep 实际延迟与 10ms 的差值落入直方图，>5ms 累计 stall counter。
//!
//! ## 外部契约
//!
//! - 14 个公开 `Lazy` 静态（含 6 个 worker label vec）；
//! - [`tokio_metrics_and_canary_loop`]：异步 task，需 spawn 在被监控 runtime；
//! - 两套注册入口幂等，互不影响（独立 OnceCell）。
//!
//! ## 实现要点
//!
//! - **Counter 差分**：worker_park / steal / overflow + budget_forced_yield 都是
//!   单调递增累计值；本模块在 [`PrevWorkerCounters`] 与 `PREV_BUDGET_FORCED_YIELD`
//!   中保存上一轮采样，每轮只 `inc_by(delta)`。计数缓存由 **单任务** 持有，无锁。
//! - **busy_ratio 代理**：直接的 worker_busy_duration_ratio 在 stable Tokio 缺失，
//!   用 `mean_poll_time / 1ms` 钳到 [0, 1] 后放大 1000 作为 millé 表示——
//!   值 ≥ 950 视为饱和。
//! - **canary 单循环**：sleep + select(cancel) 组成最小骨架，循环体只做
//!   “量延迟 + 必要时调用 `sample_tokio_metrics`”两件事，避免大单体函数。

use once_cell::sync::{Lazy, OnceCell};
use prometheus::{Counter, Gauge, Histogram, HistogramOpts, IntCounterVec, IntGaugeVec, Opts};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;

use super::prometheus_names::{frontend_perf, name_prefix, tokio_perf as names};
use crate::MetricsRegistry;

// === 命名 + 构造 helpers =====================================================

fn tokio_metric_name(suffix: &str) -> String {
    format!("{}_{}", name_prefix::TOKIO, suffix)
}

/// 事件循环 canary 指标使用 `pagoda_frontend_` 前缀（与 frontend_perf 同一命名空间），
/// 因为它们衡量的是“前端事件循环卡顿”这一外部可观察行为。
fn frontend_metric_name(suffix: &str) -> String {
    format!("{}_{}", name_prefix::FRONTEND, suffix)
}

fn tokio_gauge(suffix: &str, help: &'static str) -> Gauge {
    Gauge::new(tokio_metric_name(suffix), help)
        .unwrap_or_else(|e| panic!("failed to build gauge {suffix}: {e}"))
}

fn tokio_counter(suffix: &str, help: &'static str) -> Counter {
    Counter::new(tokio_metric_name(suffix), help)
        .unwrap_or_else(|e| panic!("failed to build counter {suffix}: {e}"))
}

fn tokio_gauge_vec(suffix: &str, help: &'static str, labels: &[&str]) -> IntGaugeVec {
    IntGaugeVec::new(Opts::new(tokio_metric_name(suffix), help), labels)
        .unwrap_or_else(|e| panic!("failed to build gauge vec {suffix}: {e}"))
}

fn tokio_counter_vec(suffix: &str, help: &'static str, labels: &[&str]) -> IntCounterVec {
    IntCounterVec::new(Opts::new(tokio_metric_name(suffix), help), labels)
        .unwrap_or_else(|e| panic!("failed to build counter vec {suffix}: {e}"))
}

// === 全局 runtime 指标 =======================================================

pub static TOKIO_GLOBAL_QUEUE_DEPTH: Lazy<Gauge> = Lazy::new(|| {
    tokio_gauge(names::GLOBAL_QUEUE_DEPTH, "Number of tasks in the runtime global queue")
});

pub static TOKIO_BUDGET_FORCED_YIELD_TOTAL: Lazy<Counter> = Lazy::new(|| {
    tokio_counter(
        names::BUDGET_FORCED_YIELD_TOTAL,
        "Number of times tasks were forced to yield after exhausting budget",
    )
});

pub static TOKIO_BLOCKING_THREADS: Lazy<Gauge> =
    Lazy::new(|| tokio_gauge(names::BLOCKING_THREADS, "Number of blocking threads"));

pub static TOKIO_BLOCKING_IDLE_THREADS: Lazy<Gauge> =
    Lazy::new(|| tokio_gauge(names::BLOCKING_IDLE_THREADS, "Number of idle blocking threads"));

pub static TOKIO_BLOCKING_QUEUE_DEPTH: Lazy<Gauge> = Lazy::new(|| {
    tokio_gauge(
        names::BLOCKING_QUEUE_DEPTH,
        "Number of tasks in the blocking thread pool queue",
    )
});

pub static TOKIO_ALIVE_TASKS: Lazy<Gauge> =
    Lazy::new(|| tokio_gauge(names::ALIVE_TASKS, "Number of alive tasks in the runtime"));

// === Per-worker 指标 ========================================================

pub static TOKIO_WORKER_MEAN_POLL_TIME_NS: Lazy<IntGaugeVec> = Lazy::new(|| {
    tokio_gauge_vec(
        names::WORKER_MEAN_POLL_TIME_NS,
        "Worker mean task poll time (nanoseconds)",
        &["worker"],
    )
});

pub static TOKIO_WORKER_BUSY_RATIO_VEC: Lazy<IntGaugeVec> = Lazy::new(|| {
    tokio_gauge_vec(
        names::WORKER_BUSY_RATIO,
        "Worker busy ratio (0-1) as integer mill ratio; >950 = saturated",
        &["worker"],
    )
});

pub static TOKIO_WORKER_PARK_COUNT_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    tokio_counter_vec(
        names::WORKER_PARK_COUNT_TOTAL,
        "Total number of times worker has parked",
        &["worker"],
    )
});

pub static TOKIO_WORKER_LOCAL_QUEUE_DEPTH: Lazy<IntGaugeVec> = Lazy::new(|| {
    tokio_gauge_vec(
        names::WORKER_LOCAL_QUEUE_DEPTH,
        "Number of tasks in worker local queue",
        &["worker"],
    )
});

pub static TOKIO_WORKER_STEAL_COUNT_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    tokio_counter_vec(
        names::WORKER_STEAL_COUNT_TOTAL,
        "Total number of tasks stolen by worker",
        &["worker"],
    )
});

pub static TOKIO_WORKER_OVERFLOW_COUNT_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    tokio_counter_vec(
        names::WORKER_OVERFLOW_COUNT_TOTAL,
        "Total number of times worker local queue overflowed",
        &["worker"],
    )
});

// === 事件循环 canary ==========================================================

pub static EVENT_LOOP_DELAY_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    Histogram::with_opts(
        HistogramOpts::new(
            frontend_metric_name(frontend_perf::EVENT_LOOP_DELAY_SECONDS),
            "Event loop delay canary: drift from 10ms sleep (seconds)",
        )
        .buckets(vec![
            0.0, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
        ]),
    )
    .expect("event_loop_delay_seconds histogram")
});

pub static EVENT_LOOP_STALL_TOTAL: Lazy<Counter> = Lazy::new(|| {
    Counter::new(
        frontend_metric_name(frontend_perf::EVENT_LOOP_STALL_TOTAL),
        "Number of event loop stalls (delay > 5ms)",
    )
    .expect("event_loop_stall_total counter")
});

// === 双 OnceCell 独立注册 ====================================================

static REGISTERED: OnceCell<()> = OnceCell::new();
static PROMETHEUS_REGISTERED: OnceCell<()> = OnceCell::new();

/// 把 14 个 tokio + canary 指标注册到项目自有 `MetricsRegistry`。**幂等**。
pub fn ensure_tokio_perf_metrics_registered(registry: &MetricsRegistry) {
    let _ = REGISTERED.get_or_init(|| {
        for collector in all_collectors() {
            let _ = registry.add_metric(collector);
        }
    });
}

/// 同上但注册到原生 `prometheus::Registry`。**幂等**。
///
/// 单个注册失败立刻返回 Err，且 `OnceCell` 仅在全部成功时置位，
/// 允许调用方修复 registry 冲突后重试。
pub fn ensure_tokio_perf_metrics_registered_prometheus(
    registry: &prometheus::Registry,
) -> Result<(), prometheus::Error> {
    if PROMETHEUS_REGISTERED.get().is_none() {
        for collector in all_collectors() {
            registry.register(collector)?;
        }
        let _ = PROMETHEUS_REGISTERED.set(());
    }
    Ok(())
}

fn all_collectors() -> [Box<dyn prometheus::core::Collector>; 14] {
    [
        Box::new(TOKIO_GLOBAL_QUEUE_DEPTH.clone()),
        Box::new(TOKIO_BUDGET_FORCED_YIELD_TOTAL.clone()),
        Box::new(TOKIO_BLOCKING_THREADS.clone()),
        Box::new(TOKIO_BLOCKING_IDLE_THREADS.clone()),
        Box::new(TOKIO_BLOCKING_QUEUE_DEPTH.clone()),
        Box::new(TOKIO_ALIVE_TASKS.clone()),
        Box::new(TOKIO_WORKER_MEAN_POLL_TIME_NS.clone()),
        Box::new(TOKIO_WORKER_BUSY_RATIO_VEC.clone()),
        Box::new(TOKIO_WORKER_PARK_COUNT_TOTAL.clone()),
        Box::new(TOKIO_WORKER_LOCAL_QUEUE_DEPTH.clone()),
        Box::new(TOKIO_WORKER_STEAL_COUNT_TOTAL.clone()),
        Box::new(TOKIO_WORKER_OVERFLOW_COUNT_TOTAL.clone()),
        Box::new(EVENT_LOOP_DELAY_SECONDS.clone()),
        Box::new(EVENT_LOOP_STALL_TOTAL.clone()),
    ]
}

// === 采样状态 ================================================================

/// `budget_forced_yield_count` 是 RuntimeMetrics 暴露的“总值”，用 `AtomicU64`
/// 保存上一轮值，每轮只把 `delta = curr - prev` 写入 Counter 实现差分语义。
static PREV_BUDGET_FORCED_YIELD: AtomicU64 = AtomicU64::new(0);

/// 每个 worker 上一轮的单调累计样本；由 [`tokio_metrics_and_canary_loop`]
/// 单任务独占，故无需锁。
struct PrevWorkerCounters {
    park: Vec<u64>,
    steal: Vec<u64>,
    overflow: Vec<u64>,
}

impl PrevWorkerCounters {
    /// 空容器；首次 `ensure_capacity` 才会按 runtime 实际 worker 数扩容。
    fn new() -> Self {
        Self {
            park: Vec::new(),
            steal: Vec::new(),
            overflow: Vec::new(),
        }
    }

    /// 确保三个向量能容纳 `num_workers` 个槽位，**不缩容**；缺失槽位填 0。
    fn ensure_capacity(&mut self, num_workers: usize) {
        if self.park.len() < num_workers {
            for v in [&mut self.park, &mut self.steal, &mut self.overflow] {
                v.resize(num_workers, 0);
            }
        }
    }
}

// === canary + 采样循环 =======================================================

/// 同时跑 tokio runtime 指标采样（1s 节拍）和事件循环 canary（10ms 节拍）。
///
/// 调用方应在“被监控的 runtime”上 `tokio::spawn` 本函数。`cancel` 触发时
/// 优雅退出。
pub async fn tokio_metrics_and_canary_loop(cancel: CancellationToken) {
    let canary_interval = Duration::from_millis(10);
    let stall_threshold = Duration::from_millis(5);
    let collect_interval = Duration::from_secs(1);
    let mut next_collect = Instant::now() + collect_interval;
    let mut prev_counters = PrevWorkerCounters::new();

    loop {
        let cycle_start = Instant::now();
        tokio::select! {
            _ = tokio::time::sleep(canary_interval) => {}
            _ = cancel.cancelled() => {
                tracing::debug!("tokio metrics and canary loop shutting down");
                return;
            }
        }

        observe_canary_delay(cycle_start, canary_interval, stall_threshold);

        let now = Instant::now();
        if now >= next_collect {
            next_collect = now + collect_interval;
            sample_tokio_metrics(&mut prev_counters);
        }
    }
}

/// 记录本轮 sleep 的实际延迟 → 直方图；超阈值时 stall counter +1。
///
/// 抽出独立函数让主循环只剩“sleep/select + 派发”两步骨架，便于阅读。
fn observe_canary_delay(cycle_start: Instant, canary_interval: Duration, stall_threshold: Duration) {
    let delay = cycle_start.elapsed().saturating_sub(canary_interval);
    EVENT_LOOP_DELAY_SECONDS.observe(delay.as_secs_f64());
    if delay > stall_threshold {
        EVENT_LOOP_STALL_TOTAL.inc();
    }
}

/// 一轮 runtime 指标采样：刷新全局 gauge / 差分 counter / 遍历 worker。
fn sample_tokio_metrics(prev: &mut PrevWorkerCounters) {
    let metrics = Handle::current().metrics();

    refresh_global_gauges(&metrics);
    refresh_budget_counter(&metrics);

    let num_workers = metrics.num_workers();
    prev.ensure_capacity(num_workers);
    for worker_index in 0..num_workers {
        sample_worker(&metrics, worker_index, prev);
    }
}

/// 全局 gauge：global queue / blocking 三连 / alive tasks。
fn refresh_global_gauges(metrics: &tokio::runtime::RuntimeMetrics) {
    TOKIO_GLOBAL_QUEUE_DEPTH.set(metrics.global_queue_depth() as f64);
    for (gauge, value) in [
        (&*TOKIO_BLOCKING_THREADS, metrics.num_blocking_threads() as f64),
        (&*TOKIO_BLOCKING_IDLE_THREADS, metrics.num_idle_blocking_threads() as f64),
        (&*TOKIO_BLOCKING_QUEUE_DEPTH, metrics.blocking_queue_depth() as f64),
        (&*TOKIO_ALIVE_TASKS, metrics.num_alive_tasks() as f64),
    ] {
        gauge.set(value);
    }
}

/// budget_forced_yield 是单调累计：用 AtomicU64 差分后只 inc_by 增量。
fn refresh_budget_counter(metrics: &tokio::runtime::RuntimeMetrics) {
    let curr = metrics.budget_forced_yield_count();
    let prev = PREV_BUDGET_FORCED_YIELD.swap(curr, Ordering::Relaxed);
    let delta = curr.saturating_sub(prev);
    TOKIO_BUDGET_FORCED_YIELD_TOTAL.inc_by(delta as f64);
}

/// 单 worker 采样：mean poll → busy ratio 代理；local queue → gauge；
/// park/steal/overflow 三个累计 → 差分写 Counter。
fn sample_worker(
    metrics: &tokio::runtime::RuntimeMetrics,
    worker_index: usize,
    prev: &mut PrevWorkerCounters,
) {
    let worker_label = worker_index.to_string();
    let labels = [worker_label.as_str()];

    let mean_poll = metrics.worker_mean_poll_time(worker_index);
    TOKIO_WORKER_MEAN_POLL_TIME_NS
        .with_label_values(&labels)
        .set(mean_poll.as_nanos() as i64);

    TOKIO_WORKER_LOCAL_QUEUE_DEPTH
        .with_label_values(&labels)
        .set(metrics.worker_local_queue_depth(worker_index) as i64);

    // busy_ratio 代理：mean_poll/1ms 钳到 [0,1] 再 ×1000 得到 millé 表示。
    let busy_proxy = (mean_poll.as_secs_f64() / 0.001).min(1.0);
    TOKIO_WORKER_BUSY_RATIO_VEC
        .with_label_values(&labels)
        .set((busy_proxy * 1000.0) as i64);

    apply_counter_delta(
        &TOKIO_WORKER_PARK_COUNT_TOTAL,
        &labels,
        &mut prev.park[worker_index],
        metrics.worker_park_count(worker_index),
    );
    apply_counter_delta(
        &TOKIO_WORKER_STEAL_COUNT_TOTAL,
        &labels,
        &mut prev.steal[worker_index],
        metrics.worker_steal_count(worker_index),
    );
    apply_counter_delta(
        &TOKIO_WORKER_OVERFLOW_COUNT_TOTAL,
        &labels,
        &mut prev.overflow[worker_index],
        metrics.worker_overflow_count(worker_index),
    );
}

/// 把 “RuntimeMetrics 的总值 - 上一轮总值” 写入 Prometheus Counter。
fn apply_counter_delta(
    counter_vec: &IntCounterVec,
    labels: &[&str],
    previous: &mut u64,
    current: u64,
) {
    let delta = current.saturating_sub(*previous);
    counter_vec.with_label_values(labels).inc_by(delta);
    *previous = current;
}

// === 单元测试 =================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::core::Collector;
    use std::collections::BTreeSet;
    use std::process::Command;

    fn expected_metric_names() -> BTreeSet<String> {
        [
            tokio_metric_name(names::GLOBAL_QUEUE_DEPTH),
            tokio_metric_name(names::BUDGET_FORCED_YIELD_TOTAL),
            tokio_metric_name(names::BLOCKING_THREADS),
            tokio_metric_name(names::BLOCKING_IDLE_THREADS),
            tokio_metric_name(names::BLOCKING_QUEUE_DEPTH),
            tokio_metric_name(names::ALIVE_TASKS),
            tokio_metric_name(names::WORKER_MEAN_POLL_TIME_NS),
            tokio_metric_name(names::WORKER_BUSY_RATIO),
            tokio_metric_name(names::WORKER_PARK_COUNT_TOTAL),
            tokio_metric_name(names::WORKER_LOCAL_QUEUE_DEPTH),
            tokio_metric_name(names::WORKER_STEAL_COUNT_TOTAL),
            tokio_metric_name(names::WORKER_OVERFLOW_COUNT_TOTAL),
            frontend_metric_name(frontend_perf::EVENT_LOOP_DELAY_SECONDS),
            frontend_metric_name(frontend_perf::EVENT_LOOP_STALL_TOTAL),
        ]
        .into_iter()
        .collect()
    }

    fn metric_family_names(families: &[prometheus::proto::MetricFamily]) -> BTreeSet<String> {
        families.iter().map(|f| f.name().to_string()).collect()
    }

    fn emit_tokio_metric_samples() {
        TOKIO_GLOBAL_QUEUE_DEPTH.set(2.0);
        TOKIO_BUDGET_FORCED_YIELD_TOTAL.inc_by(1.0);
        TOKIO_BLOCKING_THREADS.set(1.0);
        TOKIO_BLOCKING_IDLE_THREADS.set(1.0);
        TOKIO_BLOCKING_QUEUE_DEPTH.set(0.0);
        TOKIO_ALIVE_TASKS.set(3.0);
        TOKIO_WORKER_MEAN_POLL_TIME_NS.with_label_values(&["0"]).set(1000);
        TOKIO_WORKER_BUSY_RATIO_VEC.with_label_values(&["0"]).set(500);
        TOKIO_WORKER_PARK_COUNT_TOTAL.with_label_values(&["0"]).inc_by(1);
        TOKIO_WORKER_LOCAL_QUEUE_DEPTH.with_label_values(&["0"]).set(0);
        TOKIO_WORKER_STEAL_COUNT_TOTAL.with_label_values(&["0"]).inc_by(1);
        TOKIO_WORKER_OVERFLOW_COUNT_TOTAL.with_label_values(&["0"]).inc_by(1);
        EVENT_LOOP_DELAY_SECONDS.observe(0.001);
        EVENT_LOOP_STALL_TOTAL.inc();
    }

    fn run_tokio_perf_subprocess(test_name: &str) {
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test_name, "--ignored", "--nocapture"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "subprocess test failed: {}\nstdout:\n{}\nstderr:\n{}",
            test_name,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    /// ## 测试过程
    /// 抽样验证 global queue / busy ratio / event loop delay 三个指标的全名和 help。
    /// ## 意义
    /// 锁定跨 prefix（tokio_ 与 frontend_）的对外契约。
    #[test]
    fn test_supplemental_tokio_metric_name_and_descriptors() {
        assert_eq!(
            tokio_metric_name(names::GLOBAL_QUEUE_DEPTH),
            "pagoda_tokio_global_queue_depth"
        );
        assert_eq!(tokio_metric_name(""), format!("{}_", name_prefix::TOKIO));

        let g = TOKIO_GLOBAL_QUEUE_DEPTH.desc();
        assert_eq!(g.len(), 1);
        assert_eq!(g[0].fq_name, tokio_metric_name(names::GLOBAL_QUEUE_DEPTH));
        assert_eq!(g[0].help, "Number of tasks in the runtime global queue");

        let b = TOKIO_WORKER_BUSY_RATIO_VEC.desc();
        assert_eq!(b.len(), 1);
        assert_eq!(b[0].fq_name, tokio_metric_name(names::WORKER_BUSY_RATIO));

        let e = EVENT_LOOP_DELAY_SECONDS.desc();
        assert_eq!(e.len(), 1);
        assert_eq!(
            e[0].fq_name,
            frontend_metric_name(frontend_perf::EVENT_LOOP_DELAY_SECONDS)
        );
        assert_eq!(
            e[0].help,
            "Event loop delay canary: drift from 10ms sleep (seconds)"
        );
    }

    /// ## 测试过程
    /// 多次 `ensure_capacity`：扩容、缩容（不丢失原值）、再扩容；
    /// 写入预设值后断言每次容量与内容符合预期。
    /// ## 意义
    /// 防止 worker 数动态变化导致索引越界或历史样本被错误清零。
    #[test]
    fn test_supplemental_prev_worker_counters_new_and_ensure_capacity() {
        let mut prev = PrevWorkerCounters::new();
        assert!(prev.park.is_empty());

        prev.ensure_capacity(2);
        assert_eq!(prev.park, vec![0, 0]);

        prev.park[0] = 7;
        prev.steal[1] = 9;
        prev.overflow[1] = 11;

        prev.ensure_capacity(1); // 不缩容
        assert_eq!(prev.park, vec![7, 0]);
        assert_eq!(prev.steal, vec![0, 9]);
        assert_eq!(prev.overflow, vec![0, 11]);

        prev.ensure_capacity(4);
        assert_eq!(prev.park, vec![7, 0, 0, 0]);
        assert_eq!(prev.steal, vec![0, 9, 0, 0]);
        assert_eq!(prev.overflow, vec![0, 11, 0, 0]);
    }

    /// ## 测试过程
    /// 调 `apply_counter_delta(prev=5, curr=8)`，断言 counter inc_by(3) 且 prev=8。
    /// 再调一次相同 curr，断言 inc_by(0)；最后 curr<prev 用 saturating_sub 不爆。
    /// ## 意义
    /// 验证“总值差分写 counter”的核心算法正确性。
    #[test]
    fn test_supplemental_apply_counter_delta_increments_and_saturates() {
        let cv = tokio_counter_vec(
            "test_delta_counter",
            "for delta test",
            &["worker"],
        );
        let labels = ["0"];
        let mut prev = 5u64;

        apply_counter_delta(&cv, &labels, &mut prev, 8);
        assert_eq!(prev, 8);
        assert_eq!(cv.with_label_values(&labels).get(), 3);

        apply_counter_delta(&cv, &labels, &mut prev, 8);
        assert_eq!(cv.with_label_values(&labels).get(), 3);

        // 计数器回退：saturating 行为 = 0 delta
        apply_counter_delta(&cv, &labels, &mut prev, 2);
        assert_eq!(prev, 2);
        assert_eq!(cv.with_label_values(&labels).get(), 3);
    }

    /// ## 测试过程
    /// 在 multi-thread runtime 中两次调用 `sample_tokio_metrics`，
    /// 检查 prev 容量、busy_ratio 在 [0,1000]、worker 计数 monotonic。
    /// ## 意义
    /// 端到端验证一轮采样后所有 worker 指标的取值范围合理。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_supplemental_sample_tokio_metrics_updates_metrics_and_prev() {
        let mut prev = PrevWorkerCounters::new();
        let workers = Handle::current().metrics().num_workers();
        let budget_before = TOKIO_BUDGET_FORCED_YIELD_TOTAL.get();

        sample_tokio_metrics(&mut prev);
        assert!(prev.park.len() >= workers);
        assert!(TOKIO_BUDGET_FORCED_YIELD_TOTAL.get() >= budget_before);

        sample_tokio_metrics(&mut prev);
        let busy = TOKIO_WORKER_BUSY_RATIO_VEC.with_label_values(&["0"]).get();
        assert!((0..=1000).contains(&busy));
    }

    /// ## 测试过程
    /// 启动哨兵循环，休眠 30ms 后取消；在 1s 超时内成功 join。
    /// 断言至少新增 1 个延迟样本，停顿计数不倒退。
    /// ## 意义
    /// 验证哨兵循环能被干净地取消退出，且确实写入直方图。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_supplemental_tokio_metrics_and_canary_loop_cancels_cleanly() {
        let delay_before = EVENT_LOOP_DELAY_SECONDS.get_sample_count();
        let stall_before = EVENT_LOOP_STALL_TOTAL.get();

        let cancel = CancellationToken::new();
        let handle = tokio::spawn(tokio_metrics_and_canary_loop(cancel.clone()));

        tokio::time::sleep(Duration::from_millis(30)).await;
        cancel.cancel();

        let join_result = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("canary loop should stop quickly after cancellation");
        join_result.expect("canary loop task should not panic");

        assert!(EVENT_LOOP_DELAY_SECONDS.get_sample_count() >= delay_before + 1);
        assert!(EVENT_LOOP_STALL_TOTAL.get() >= stall_before);
    }

    /// ## 测试过程 / ## 意义
    /// fork 子进程验证 MetricsRegistry 注册路径幂等。
    #[test]
    fn test_supplemental_metrics_registry_registration_via_subprocess() {
        run_tokio_perf_subprocess(
            "metrics::tokio_perf::tests::subprocess_metrics_registry_registration",
        );
    }

    /// ## 测试过程 / ## 意义
    /// fork 子进程验证 Prometheus 注册路径幂等。
    #[test]
    fn test_supplemental_prometheus_registration_via_subprocess() {
        run_tokio_perf_subprocess(
            "metrics::tokio_perf::tests::subprocess_prometheus_registration",
        );
    }

    /// ## 测试过程 / ## 意义
    /// fork 子进程验证“先 MetricsRegistry 后 Prometheus”路径互不干扰。
    #[test]
    fn test_supplemental_metrics_then_prometheus_independence_via_subprocess() {
        run_tokio_perf_subprocess(
            "metrics::tokio_perf::tests::subprocess_metrics_then_prometheus_independence",
        );
    }

    /// ## 测试过程 / ## 意义
    /// fork 子进程验证：Prometheus 路径首次注册失败后，OnceCell 不被置位，
    /// 换 registry 可重试成功。
    #[test]
    fn test_supplemental_prometheus_error_then_recovery_via_subprocess() {
        run_tokio_perf_subprocess(
            "metrics::tokio_perf::tests::subprocess_prometheus_error_then_recovery",
        );
    }

    #[test]
    #[ignore = "subprocess helper"]
    fn subprocess_metrics_registry_registration() {
        let first = MetricsRegistry::new();
        ensure_tokio_perf_metrics_registered(&first);
        emit_tokio_metric_samples();
        assert_eq!(
            metric_family_names(&first.get_prometheus_registry().gather()),
            expected_metric_names()
        );

        let second = MetricsRegistry::new();
        ensure_tokio_perf_metrics_registered(&second);
        assert!(second.get_prometheus_registry().gather().is_empty());
    }

    #[test]
    #[ignore = "subprocess helper"]
    fn subprocess_prometheus_registration() {
        let first = prometheus::Registry::new();
        ensure_tokio_perf_metrics_registered_prometheus(&first).unwrap();
        emit_tokio_metric_samples();
        assert_eq!(metric_family_names(&first.gather()), expected_metric_names());

        let second = prometheus::Registry::new();
        ensure_tokio_perf_metrics_registered_prometheus(&second).unwrap();
        assert!(second.gather().is_empty());
    }

    #[test]
    #[ignore = "subprocess helper"]
    fn subprocess_metrics_then_prometheus_independence() {
        let mr = MetricsRegistry::new();
        ensure_tokio_perf_metrics_registered(&mr);

        let pr = prometheus::Registry::new();
        ensure_tokio_perf_metrics_registered_prometheus(&pr).unwrap();
        emit_tokio_metric_samples();

        assert_eq!(
            metric_family_names(&mr.get_prometheus_registry().gather()),
            expected_metric_names()
        );
        assert_eq!(metric_family_names(&pr.gather()), expected_metric_names());
    }

    #[test]
    #[ignore = "subprocess helper"]
    fn subprocess_prometheus_error_then_recovery() {
        let conflicting = prometheus::Registry::new();
        let conflicting_gauge = prometheus::Gauge::new(
            tokio_metric_name(names::GLOBAL_QUEUE_DEPTH),
            "conflicting tokio global queue depth gauge",
        )
        .unwrap();
        conflicting.register(Box::new(conflicting_gauge)).unwrap();

        assert!(ensure_tokio_perf_metrics_registered_prometheus(&conflicting).is_err());

        let healthy = prometheus::Registry::new();
        ensure_tokio_perf_metrics_registered_prometheus(&healthy).unwrap();
        emit_tokio_metric_samples();
        assert_eq!(metric_family_names(&healthy.gather()), expected_metric_names());
    }
}
