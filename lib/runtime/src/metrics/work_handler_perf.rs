// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # Work Handler Transport 时延拆分（后端侧）
//!
//! ## 设计意图
//!
//! 端到端请求时延的两段关键拆分：
//!
//! 1. **T1→T2 网络传输**：frontend 发起到 backend 收到（跨进程 wall-clock）；
//! 2. **T2→T3 后端处理**：`handle_payload` 进入到首个响应字节发出。
//!
//! 两个 histogram 分开统计是为了在排障时直接区分“慢在网线还是慢在 backend”。
//!
//! ## 外部契约
//!
//! - `WORK_HANDLER_NETWORK_TRANSIT_SECONDS` / `_TIME_TO_FIRST_RESPONSE_SECONDS`
//!   两个公开静态；
//! - [`ensure_work_handler_perf_metrics_registered`]：项目自有 `MetricsRegistry` 路径，幂等；
//! - [`ensure_work_handler_perf_metrics_registered_prometheus`]：原生 `prometheus::Registry`
//!   路径，幂等，首次失败被缓存为同一 `String`，避免后续部分注册。
//!
//! ## 实现要点
//!
//! - 两套注册路径使用**独立** `OnceCell`，避免先调用 MetricsRegistry 路径
//!   就把 Prometheus 路径“吃掉”导致后者根本没注册；
//! - 公共的 histogram 构造收敛到 [`wh_histogram`]，并提供 [`pool_collectors`]
//!   作为两路注册共享的数据源；
//! - 网络与处理两个 histogram 的桶分布刻意不同：
//!   网络以 0.1ms 为下限关注硬件级抖动；处理以 1ms 为下限并延展到 60s，
//!   适合 LLM 长尾推理 SLA 监控。

use once_cell::sync::{Lazy, OnceCell};
use prometheus::{Histogram, HistogramOpts};

use super::prometheus_names::{name_prefix, work_handler};
use crate::MetricsRegistry;

// === 命名 + 构造 helpers =====================================================

fn work_handler_metric_name(suffix: &str) -> String {
    format!("{}_{}", name_prefix::WORK_HANDLER, suffix)
}

fn wh_histogram(suffix: &str, help: &'static str, buckets: Vec<f64>) -> Histogram {
    Histogram::with_opts(
        HistogramOpts::new(work_handler_metric_name(suffix), help).buckets(buckets),
    )
    .unwrap_or_else(|e| panic!("failed to build histogram {suffix}: {e}"))
}

// === 公开指标 ================================================================

/// frontend 发送 → backend 接收 的网络传输耗时（跨进程 wall-clock，秒）。
pub static WORK_HANDLER_NETWORK_TRANSIT_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    wh_histogram(
        work_handler::NETWORK_TRANSIT_SECONDS,
        "Frontend-to-backend network transit time (cross-process wall-clock, seconds)",
        vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
    )
});

/// backend 处理耗时：`handle_payload` 进入 → prologue 已发出（秒）。
pub static WORK_HANDLER_TIME_TO_FIRST_RESPONSE_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    wh_histogram(
        work_handler::TIME_TO_FIRST_RESPONSE_SECONDS,
        "Backend processing time from handle_payload entry to prologue sent (seconds)",
        vec![
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
        ],
    )
});

// === 注册路径（双 OnceCell 独立保护）=========================================

/// `MetricsRegistry` 路径的幂等闸。
static METRICS_REGISTERED: OnceCell<()> = OnceCell::new();

/// 原生 `prometheus::Registry` 路径的幂等闸；**独立**于 `METRICS_REGISTERED`，
/// 否则先调 MetricsRegistry 路径就会让 prometheus 路径默默 no-op。
static PROMETHEUS_REGISTERED: OnceCell<Result<(), String>> = OnceCell::new();

/// 把指标注册到项目自有 `MetricsRegistry`。**幂等**。
pub fn ensure_work_handler_perf_metrics_registered(registry: &MetricsRegistry) {
    let _ = METRICS_REGISTERED.get_or_init(|| {
        for (collector, name) in named_collectors() {
            registry.add_metric_or_warn(collector, name);
        }
    });
}

/// 把指标注册到原生 `prometheus::Registry`。**幂等**。
pub fn ensure_work_handler_perf_metrics_registered_prometheus(
    registry: &prometheus::Registry,
) -> Result<(), prometheus::Error> {
    let result = PROMETHEUS_REGISTERED.get_or_init(|| register_to_prometheus(registry));
    match result {
        Ok(()) => Ok(()),
        Err(msg) => Err(prometheus::Error::Msg(msg.clone())),
    }
}

/// 共享数据源：两个 collector + 对外指标名。
fn named_collectors() -> [(Box<dyn prometheus::core::Collector>, &'static str); 2] {
    [
        (
            Box::new(WORK_HANDLER_NETWORK_TRANSIT_SECONDS.clone()),
            "work_handler_network_transit_seconds",
        ),
        (
            Box::new(WORK_HANDLER_TIME_TO_FIRST_RESPONSE_SECONDS.clone()),
            "work_handler_time_to_first_response_seconds",
        ),
    ]
}

fn register_to_prometheus(registry: &prometheus::Registry) -> Result<(), String> {
    let collectors: [Box<dyn prometheus::core::Collector>; 2] = [
        Box::new(WORK_HANDLER_NETWORK_TRANSIT_SECONDS.clone()),
        Box::new(WORK_HANDLER_TIME_TO_FIRST_RESPONSE_SECONDS.clone()),
    ];
    collectors
        .into_iter()
        .try_for_each(|c| registry.register(c))
        .map_err(|e| e.to_string())
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
            work_handler_metric_name(work_handler::NETWORK_TRANSIT_SECONDS),
            work_handler_metric_name(work_handler::TIME_TO_FIRST_RESPONSE_SECONDS),
        ]
        .into_iter()
        .collect()
    }

    fn metric_family_names(families: &[prometheus::proto::MetricFamily]) -> BTreeSet<String> {
        families.iter().map(|f| f.name().to_string()).collect()
    }

    fn emit_work_handler_samples() {
        WORK_HANDLER_NETWORK_TRANSIT_SECONDS.observe(0.01);
        WORK_HANDLER_TIME_TO_FIRST_RESPONSE_SECONDS.observe(0.2);
    }

    fn run_work_handler_subprocess(test_name: &str) {
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
    /// 验证两个 histogram 拼出的全名和 desc help 与硬编码字符串一致。
    /// ## 意义
    /// 锁定对外可观测契约。
    #[test]
    fn test_supplemental_work_handler_metric_name_and_descriptors() {
        assert_eq!(
            work_handler_metric_name(work_handler::NETWORK_TRANSIT_SECONDS),
            "pagoda_work_handler_network_transit_seconds"
        );
        assert_eq!(
            work_handler_metric_name(work_handler::TIME_TO_FIRST_RESPONSE_SECONDS),
            "pagoda_work_handler_time_to_first_response_seconds"
        );
        assert_eq!(
            work_handler_metric_name(""),
            format!("{}_", name_prefix::WORK_HANDLER)
        );
        assert_eq!(
            work_handler_metric_name("custom-suffix"),
            "pagoda_work_handler_custom-suffix"
        );

        let n = WORK_HANDLER_NETWORK_TRANSIT_SECONDS.desc();
        assert_eq!(n.len(), 1);
        assert_eq!(
            n[0].fq_name,
            work_handler_metric_name(work_handler::NETWORK_TRANSIT_SECONDS)
        );
        assert_eq!(
            n[0].help,
            "Frontend-to-backend network transit time (cross-process wall-clock, seconds)"
        );

        let r = WORK_HANDLER_TIME_TO_FIRST_RESPONSE_SECONDS.desc();
        assert_eq!(r.len(), 1);
        assert_eq!(
            r[0].fq_name,
            work_handler_metric_name(work_handler::TIME_TO_FIRST_RESPONSE_SECONDS)
        );
        assert_eq!(
            r[0].help,
            "Backend processing time from handle_payload entry to prologue sent (seconds)"
        );
    }

    /// ## 测试过程
    /// 各 observe 一次已知值，检查 sample_count +1 且 sample_sum 精确递增。
    /// ## 意义
    /// 验证 histogram 不被静默替换为 no-op。
    #[test]
    fn test_supplemental_work_handler_histogram_observation_deltas() {
        let epsilon = 0.000_001;

        let nc = WORK_HANDLER_NETWORK_TRANSIT_SECONDS.get_sample_count();
        let ns = WORK_HANDLER_NETWORK_TRANSIT_SECONDS.get_sample_sum();
        WORK_HANDLER_NETWORK_TRANSIT_SECONDS.observe(0.01);
        assert_eq!(WORK_HANDLER_NETWORK_TRANSIT_SECONDS.get_sample_count(), nc + 1);
        assert!(
            (WORK_HANDLER_NETWORK_TRANSIT_SECONDS.get_sample_sum() - (ns + 0.01)).abs() < epsilon
        );

        let rc = WORK_HANDLER_TIME_TO_FIRST_RESPONSE_SECONDS.get_sample_count();
        let rs = WORK_HANDLER_TIME_TO_FIRST_RESPONSE_SECONDS.get_sample_sum();
        WORK_HANDLER_TIME_TO_FIRST_RESPONSE_SECONDS.observe(0.2);
        assert_eq!(
            WORK_HANDLER_TIME_TO_FIRST_RESPONSE_SECONDS.get_sample_count(),
            rc + 1
        );
        assert!(
            (WORK_HANDLER_TIME_TO_FIRST_RESPONSE_SECONDS.get_sample_sum() - (rs + 0.2)).abs()
                < epsilon
        );
    }

    // 以下 4 个测试均通过 fork 子进程隔离两个 OnceCell 全局态。

    /// ## 测试过程 / ## 意义
    /// fork 子进程跑 MetricsRegistry 注册幂等性测试。
    #[test]
    fn test_supplemental_metrics_registry_registration_via_subprocess() {
        run_work_handler_subprocess(
            "metrics::work_handler_perf::tests::subprocess_metrics_registry_registration",
        );
    }

    /// ## 测试过程 / ## 意义
    /// fork 子进程跑 prometheus::Registry 注册幂等性测试。
    #[test]
    fn test_supplemental_prometheus_registration_via_subprocess() {
        run_work_handler_subprocess(
            "metrics::work_handler_perf::tests::subprocess_prometheus_registration",
        );
    }

    /// ## 测试过程
    /// fork 子进程：先注册到 MetricsRegistry，再注册到 prometheus；
    /// 两套 registry 都应包含完整指标族。
    /// ## 意义
    /// 验证“两套 OnceCell 完全独立”——MetricsRegistry 路径不会污染 prometheus 路径。
    #[test]
    fn test_supplemental_metrics_then_prometheus_independence_via_subprocess() {
        run_work_handler_subprocess(
            "metrics::work_handler_perf::tests::subprocess_metrics_then_prometheus_independence",
        );
    }

    /// ## 测试过程 / ## 意义
    /// fork 子进程验证：首次注册失败后错误被缓存，第二次仍返回同一错误。
    #[test]
    fn test_supplemental_prometheus_registration_error_is_cached_via_subprocess() {
        run_work_handler_subprocess(
            "metrics::work_handler_perf::tests::subprocess_prometheus_registration_error_is_cached",
        );
    }

    #[test]
    #[ignore = "subprocess helper"]
    fn subprocess_metrics_registry_registration() {
        let first = MetricsRegistry::new();
        ensure_work_handler_perf_metrics_registered(&first);
        emit_work_handler_samples();
        assert_eq!(
            metric_family_names(&first.get_prometheus_registry().gather()),
            expected_metric_names()
        );

        let second = MetricsRegistry::new();
        ensure_work_handler_perf_metrics_registered(&second);
        assert!(second.get_prometheus_registry().gather().is_empty());
    }

    #[test]
    #[ignore = "subprocess helper"]
    fn subprocess_prometheus_registration() {
        let first = prometheus::Registry::new();
        ensure_work_handler_perf_metrics_registered_prometheus(&first).unwrap();
        emit_work_handler_samples();
        assert_eq!(metric_family_names(&first.gather()), expected_metric_names());

        let second = prometheus::Registry::new();
        ensure_work_handler_perf_metrics_registered_prometheus(&second).unwrap();
        assert!(second.gather().is_empty());
    }

    #[test]
    #[ignore = "subprocess helper"]
    fn subprocess_metrics_then_prometheus_independence() {
        let mr = MetricsRegistry::new();
        ensure_work_handler_perf_metrics_registered(&mr);

        let pr = prometheus::Registry::new();
        ensure_work_handler_perf_metrics_registered_prometheus(&pr).unwrap();

        emit_work_handler_samples();

        assert_eq!(
            metric_family_names(&mr.get_prometheus_registry().gather()),
            expected_metric_names()
        );
        assert_eq!(metric_family_names(&pr.gather()), expected_metric_names());
    }

    #[test]
    #[ignore = "subprocess helper"]
    fn subprocess_prometheus_registration_error_is_cached() {
        let conflicting = prometheus::Registry::new();
        let conflicting_hist = Histogram::with_opts(HistogramOpts::new(
            work_handler_metric_name(work_handler::NETWORK_TRANSIT_SECONDS),
            "conflicting work handler network transit histogram",
        ))
        .unwrap();
        conflicting.register(Box::new(conflicting_hist)).unwrap();

        let first_error =
            ensure_work_handler_perf_metrics_registered_prometheus(&conflicting)
                .unwrap_err()
                .to_string();
        assert!(!first_error.is_empty());

        let healthy = prometheus::Registry::new();
        let cached =
            ensure_work_handler_perf_metrics_registered_prometheus(&healthy)
                .unwrap_err()
                .to_string();
        assert_eq!(cached, first_error);
        assert!(healthy.gather().is_empty());
    }
}
