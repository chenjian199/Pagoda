// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # Worker-Pool 饱和度指标（共享 TCP 后端 server 侧）
//!
//! ## 设计意图
//!
//! 共享 TCP server 在“dispatcher mpsc 队列”与“bounded worker pool”两层之间
//! 容易出现 backpressure / 饿死。本模块暴露 6 个核心指标让运维直接看到：
//!
//! - 队列侧：当前深度 / 配置容量 / 入队拒绝总数；
//! - 池侧：permit 等待耗时直方图 / 当前活跃任务数 / 配置容量。
//!
//! 队列等待与 permit 等待**刻意分开**：前者反映 dispatcher 积压，后者反映
//! 池子规模不够；混在一起会误导扩容决策。
//!
//! ## 外部契约
//!
//! - 6 个公开 `Lazy` 静态：`WORK_HANDLER_QUEUE_DEPTH` / `_CAPACITY` /
//!   `_ENQUEUE_REJECTED_TOTAL` / `_PERMIT_WAIT_SECONDS` /
//!   `_POOL_ACTIVE_TASKS` / `_POOL_CAPACITY`；
//! - [`ensure_work_handler_pool_metrics_registered`]：把 6 个指标注册到
//!   `MetricsRegistry`，**幂等**（OnceCell 保护）。
//!
//! ## 实现要点
//!
//! - 命名拼接收敛到 [`work_handler_metric_name`] + 助手 [`wh_gauge`]、
//!   [`wh_counter`]、[`wh_histogram`]，避免每个静态都写 6 行重复代码；
//! - permit 等待 histogram 的桶分布刻意覆盖 0.1ms..60s 数量级，
//!   既能看到亚毫秒抖动，也能识别分钟级饥饿；
//! - 注册路径采用 `add_metric_or_warn`：单指标注册失败仅 log warn，
//!   不影响其他指标的可观测性，符合“尽量多看见、少全屏失败”的原则。

use once_cell::sync::{Lazy, OnceCell};
use prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge};

use super::prometheus_names::{name_prefix, work_handler};
use crate::MetricsRegistry;

// === 命名 + 构造 helpers =====================================================

/// 拼接 `pagoda_work_handler_<suffix>` 全名。
fn work_handler_metric_name(suffix: &str) -> String {
    format!("{}_{}", name_prefix::WORK_HANDLER, suffix)
}

/// `IntGauge` 构造模板：拼名 + 文档 + 失败即 panic。
fn wh_gauge(suffix: &str, help: &'static str) -> IntGauge {
    IntGauge::new(work_handler_metric_name(suffix), help)
        .unwrap_or_else(|e| panic!("failed to build gauge {suffix}: {e}"))
}

/// `IntCounter` 构造模板。
fn wh_counter(suffix: &str, help: &'static str) -> IntCounter {
    IntCounter::new(work_handler_metric_name(suffix), help)
        .unwrap_or_else(|e| panic!("failed to build counter {suffix}: {e}"))
}

/// `Histogram` 构造模板（支持自定义 buckets）。
fn wh_histogram(suffix: &str, help: &'static str, buckets: Vec<f64>) -> Histogram {
    Histogram::with_opts(
        HistogramOpts::new(work_handler_metric_name(suffix), help).buckets(buckets),
    )
    .unwrap_or_else(|e| panic!("failed to build histogram {suffix}: {e}"))
}

// === 6 个公开指标 ============================================================

/// 当前留在有界 mpsc 工作队列内、等待 dispatcher 取走的条目数。
///
/// 入队成功后 +1，`work_rx.recv()` 返回后立即 -1。
/// **不包含 permit 等待时间**——后者见 [`WORK_HANDLER_PERMIT_WAIT_SECONDS`]。
pub static WORK_HANDLER_QUEUE_DEPTH: Lazy<IntGauge> = Lazy::new(|| {
    wh_gauge(
        work_handler::QUEUE_DEPTH,
        "Current items in the bounded work queue awaiting dispatcher pickup",
    )
});

/// 有界工作队列的配置容量；server 初始化时设置一次。
pub static WORK_HANDLER_QUEUE_CAPACITY: Lazy<IntGauge> = Lazy::new(|| {
    wh_gauge(
        work_handler::QUEUE_CAPACITY,
        "Configured capacity of the bounded work queue",
    )
});

/// `work_tx.send().await` 返回错误的累计次数。
///
/// 对 tokio 有界 mpsc 而言，发送只有在 receiver（dispatcher 任务）已被
/// drop 时才会出错——`full` 状态走 backpressure 而非错误。因此本计数器
/// 增长意味着 dispatcher 异常退出。
pub static WORK_HANDLER_ENQUEUE_REJECTED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    wh_counter(
        work_handler::ENQUEUE_REJECTED_TOTAL,
        "Times enqueuing work failed because the dispatcher channel was closed",
    )
});

/// 取 worker-pool permit 的等待耗时（秒）。
///
/// 健康场景亚毫秒；饱和时 p99 上探到秒级。
pub static WORK_HANDLER_PERMIT_WAIT_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    wh_histogram(
        work_handler::PERMIT_WAIT_SECONDS,
        "Time spent waiting for a worker-pool permit (seconds)",
        vec![0.0001, 0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0],
    )
});

/// 当前 worker-pool 在用的活跃任务数（已占用 permit 数）。
pub static WORK_HANDLER_POOL_ACTIVE_TASKS: Lazy<IntGauge> = Lazy::new(|| {
    wh_gauge(
        work_handler::POOL_ACTIVE_TASKS,
        "Current number of active worker-pool tasks (permits in use)",
    )
});

/// worker-pool 配置容量（总 permit 数）；server 初始化时设置一次。
pub static WORK_HANDLER_POOL_CAPACITY: Lazy<IntGauge> = Lazy::new(|| {
    wh_gauge(
        work_handler::POOL_CAPACITY,
        "Configured worker-pool capacity (total permits)",
    )
});

// === 幂等注册逻辑 ============================================================

/// 保证全套指标只往 `MetricsRegistry` 注册一次。
static METRICS_REGISTERED: OnceCell<()> = OnceCell::new();

/// 把 6 个 worker-pool 饱和度指标注册到 `registry`。**幂等**。
///
/// 注册逐项调用 `add_metric_or_warn`：单项失败只打 warn，不影响其余指标。
pub fn ensure_work_handler_pool_metrics_registered(registry: &MetricsRegistry) {
    let _ = METRICS_REGISTERED.get_or_init(|| {
        for (collector, name) in pool_collectors() {
            registry.add_metric_or_warn(collector, name);
        }
    });
}

/// 集中维护“collector + 对外 metric 名”二元组，便于注册路径与测试共用。
fn pool_collectors() -> [(Box<dyn prometheus::core::Collector>, &'static str); 6] {
    [
        (Box::new(WORK_HANDLER_QUEUE_DEPTH.clone()), "work_handler_queue_depth"),
        (Box::new(WORK_HANDLER_QUEUE_CAPACITY.clone()), "work_handler_queue_capacity"),
        (
            Box::new(WORK_HANDLER_ENQUEUE_REJECTED_TOTAL.clone()),
            "work_handler_enqueue_rejected_total",
        ),
        (
            Box::new(WORK_HANDLER_PERMIT_WAIT_SECONDS.clone()),
            "work_handler_permit_wait_seconds",
        ),
        (Box::new(WORK_HANDLER_POOL_ACTIVE_TASKS.clone()), "work_handler_pool_active_tasks"),
        (Box::new(WORK_HANDLER_POOL_CAPACITY.clone()), "work_handler_pool_capacity"),
    ]
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
            work_handler_metric_name(work_handler::QUEUE_DEPTH),
            work_handler_metric_name(work_handler::QUEUE_CAPACITY),
            work_handler_metric_name(work_handler::ENQUEUE_REJECTED_TOTAL),
            work_handler_metric_name(work_handler::PERMIT_WAIT_SECONDS),
            work_handler_metric_name(work_handler::POOL_ACTIVE_TASKS),
            work_handler_metric_name(work_handler::POOL_CAPACITY),
        ]
        .into_iter()
        .collect()
    }

    fn metric_family_names(families: &[prometheus::proto::MetricFamily]) -> BTreeSet<String> {
        families.iter().map(|f| f.name().to_string()).collect()
    }

    fn emit_work_handler_pool_samples() {
        WORK_HANDLER_QUEUE_DEPTH.set(3);
        WORK_HANDLER_QUEUE_CAPACITY.set(64);
        WORK_HANDLER_ENQUEUE_REJECTED_TOTAL.inc_by(2);
        WORK_HANDLER_PERMIT_WAIT_SECONDS.observe(0.02);
        WORK_HANDLER_POOL_ACTIVE_TASKS.set(4);
        WORK_HANDLER_POOL_CAPACITY.set(16);
    }

    fn run_work_handler_pool_subprocess(test_name: &str) {
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
    /// 验证所有 6 个指标拼出的全名形如 `pagoda_work_handler_*`，
    /// 并检查 `WORK_HANDLER_QUEUE_DEPTH` / `_PERMIT_WAIT_SECONDS` 的 desc 内容。
    /// ## 意义
    /// 锁定对外契约，防止重命名静默切断 Grafana 仪表盘。
    #[test]
    fn test_supplemental_metric_name_and_descriptors() {
        assert_eq!(
            work_handler_metric_name(work_handler::QUEUE_DEPTH),
            "pagoda_work_handler_queue_depth"
        );
        assert_eq!(
            work_handler_metric_name(work_handler::QUEUE_CAPACITY),
            "pagoda_work_handler_queue_capacity"
        );
        assert_eq!(
            work_handler_metric_name(work_handler::ENQUEUE_REJECTED_TOTAL),
            "pagoda_work_handler_enqueue_rejected_total"
        );
        assert_eq!(
            work_handler_metric_name(work_handler::PERMIT_WAIT_SECONDS),
            "pagoda_work_handler_permit_wait_seconds"
        );
        assert_eq!(
            work_handler_metric_name(work_handler::POOL_ACTIVE_TASKS),
            "pagoda_work_handler_pool_active_tasks"
        );
        assert_eq!(
            work_handler_metric_name(work_handler::POOL_CAPACITY),
            "pagoda_work_handler_pool_capacity"
        );
        assert_eq!(
            work_handler_metric_name(""),
            format!("{}_", name_prefix::WORK_HANDLER)
        );

        let qd = WORK_HANDLER_QUEUE_DEPTH.desc();
        assert_eq!(qd.len(), 1);
        assert_eq!(qd[0].fq_name, work_handler_metric_name(work_handler::QUEUE_DEPTH));
        assert_eq!(
            qd[0].help,
            "Current items in the bounded work queue awaiting dispatcher pickup"
        );

        let pw = WORK_HANDLER_PERMIT_WAIT_SECONDS.desc();
        assert_eq!(pw.len(), 1);
        assert_eq!(
            pw[0].fq_name,
            work_handler_metric_name(work_handler::PERMIT_WAIT_SECONDS)
        );
        assert_eq!(pw[0].help, "Time spent waiting for a worker-pool permit (seconds)");
    }

    /// ## 测试过程
    /// 对每个 gauge / counter / histogram 写入已知增量，断言读取值精确匹配。
    /// ## 意义
    /// 端到端验证 `inc_by` / `set` / `observe` 不被 silently no-op。
    #[test]
    fn test_supplemental_metric_observation_deltas() {
        let epsilon = 0.000_001;

        let qd_before = WORK_HANDLER_QUEUE_DEPTH.get();
        WORK_HANDLER_QUEUE_DEPTH.set(qd_before + 2);
        assert_eq!(WORK_HANDLER_QUEUE_DEPTH.get(), qd_before + 2);

        let qc_before = WORK_HANDLER_QUEUE_CAPACITY.get();
        WORK_HANDLER_QUEUE_CAPACITY.set(qc_before + 8);
        assert_eq!(WORK_HANDLER_QUEUE_CAPACITY.get(), qc_before + 8);

        let er_before = WORK_HANDLER_ENQUEUE_REJECTED_TOTAL.get();
        WORK_HANDLER_ENQUEUE_REJECTED_TOTAL.inc_by(3);
        assert_eq!(WORK_HANDLER_ENQUEUE_REJECTED_TOTAL.get(), er_before + 3);

        let pw_count_before = WORK_HANDLER_PERMIT_WAIT_SECONDS.get_sample_count();
        let pw_sum_before = WORK_HANDLER_PERMIT_WAIT_SECONDS.get_sample_sum();
        WORK_HANDLER_PERMIT_WAIT_SECONDS.observe(0.03);
        assert_eq!(
            WORK_HANDLER_PERMIT_WAIT_SECONDS.get_sample_count(),
            pw_count_before + 1
        );
        assert!(
            (WORK_HANDLER_PERMIT_WAIT_SECONDS.get_sample_sum() - (pw_sum_before + 0.03)).abs()
                < epsilon
        );

        let at_before = WORK_HANDLER_POOL_ACTIVE_TASKS.get();
        WORK_HANDLER_POOL_ACTIVE_TASKS.set(at_before + 1);
        assert_eq!(WORK_HANDLER_POOL_ACTIVE_TASKS.get(), at_before + 1);

        let pc_before = WORK_HANDLER_POOL_CAPACITY.get();
        WORK_HANDLER_POOL_CAPACITY.set(pc_before + 4);
        assert_eq!(WORK_HANDLER_POOL_CAPACITY.get(), pc_before + 4);
    }

    /// ## 测试过程
    /// fork 子进程跑 `subprocess_metrics_registry_registration`。
    /// ## 意义
    /// 验证 `METRICS_REGISTERED` OnceCell 在二次 register 时静默跳过，
    /// 保证多 server 实例共享指标不重复注册。
    #[test]
    fn test_supplemental_metrics_registry_registration_via_subprocess() {
        run_work_handler_pool_subprocess(
            "metrics::work_handler_pool::tests::subprocess_metrics_registry_registration",
        );
    }

    #[test]
    #[ignore = "subprocess helper"]
    fn subprocess_metrics_registry_registration() {
        let first = MetricsRegistry::new();
        ensure_work_handler_pool_metrics_registered(&first);
        emit_work_handler_pool_samples();
        assert_eq!(
            metric_family_names(&first.get_prometheus_registry().gather()),
            expected_metric_names()
        );

        let second = MetricsRegistry::new();
        ensure_work_handler_pool_metrics_registered(&second);
        assert!(second.get_prometheus_registry().gather().is_empty());
    }
}
