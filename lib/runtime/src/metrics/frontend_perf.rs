// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # Frontend Pipeline 阶段细粒度性能指标
//!
//! ## 设计意图
//!
//! Frontend 一个请求要经过 preprocess / route / dispatch / postprocess 等多个
//! 阶段，再加上 tokenize、template、detokenize 等更细粒度子任务。本模块统一
//! 暴露这些阶段的：
//!
//! - 当前在途请求数（按 stage + phase 拆分）；
//! - 阶段耗时直方图；
//! - tokenize / template 子任务耗时；
//! - detokenize 累计时长（μs）+ 累计 token 数（让 Prometheus 端算 per-token）。
//!
//! Runtime crate 注册 route / transport_roundtrip 指标；LLM crate 复用同一组
//! 静态注册 preprocess / postprocess / tokenize / template / detokenize 指标。
//!
//! ## 外部契约
//!
//! - 6 个公开 `Lazy` 静态 + 一个 RAII 类型 [`StageGuard`]；
//! - 透传重导出 `STAGE_DISPATCH` / `STAGE_PREPROCESS` / `STAGE_ROUTE` 常量；
//! - 两套注册入口（项目自有 `MetricsRegistry` + 原生 `prometheus::Registry`）
//!   各自独立幂等。
//!
//! ## 实现要点
//!
//! - 公共 builder 收敛到 [`fe_histogram`]、[`fe_counter`]、[`fe_gauge_vec`]、
//!   [`fe_histogram_vec`]，避免每个静态都写 7 行 builder；
//! - 两套 `OnceCell` 独立保护，注册顺序无副作用；
//! - 各 histogram 桶分布按子任务时延量级调整：tokenize 关注 0.1ms..1s，
//!   template 关注 10μs..50ms，stage_duration 覆盖 0.1ms..5s 跨阶段比较；
//! - [`StageGuard`] 用 RAII inc/dec 保证“即使 panic / 早返回，gauge 也会归零”。

use once_cell::sync::{Lazy, OnceCell};
use prometheus::{Counter, Histogram, HistogramOpts, HistogramVec, IntGaugeVec, Opts, Registry};

use super::prometheus_names::{frontend_perf, name_prefix};
use crate::MetricsRegistry;

pub use super::prometheus_names::frontend_perf::{STAGE_DISPATCH, STAGE_PREPROCESS, STAGE_ROUTE};

// === 命名 + 构造 helpers =====================================================

fn frontend_metric_name(suffix: &str) -> String {
    format!("{}_{}", name_prefix::FRONTEND, suffix)
}

fn fe_histogram(suffix: &str, help: &'static str, buckets: Vec<f64>) -> Histogram {
    Histogram::with_opts(
        HistogramOpts::new(frontend_metric_name(suffix), help).buckets(buckets),
    )
    .unwrap_or_else(|e| panic!("failed to build histogram {suffix}: {e}"))
}

fn fe_counter(suffix: &str, help: &'static str) -> Counter {
    Counter::with_opts(Opts::new(frontend_metric_name(suffix), help))
        .unwrap_or_else(|e| panic!("failed to build counter {suffix}: {e}"))
}

fn fe_gauge_vec(suffix: &str, help: &'static str, labels: &[&str]) -> IntGaugeVec {
    IntGaugeVec::new(Opts::new(frontend_metric_name(suffix), help), labels)
        .unwrap_or_else(|e| panic!("failed to build gauge vec {suffix}: {e}"))
}

fn fe_histogram_vec(
    suffix: &str,
    help: &'static str,
    buckets: Vec<f64>,
    labels: &[&str],
) -> HistogramVec {
    HistogramVec::new(
        HistogramOpts::new(frontend_metric_name(suffix), help).buckets(buckets),
        labels,
    )
    .unwrap_or_else(|e| panic!("failed to build histogram vec {suffix}: {e}"))
}

// === 公开指标 ================================================================

/// 每阶段 inflight 请求数（labels: `stage`, `phase`）。
///
/// `phase` 取 `"prefill"|"decode"|"aggregated"`；preprocess 等无 phase 阶段传 `""`。
pub static STAGE_REQUESTS: Lazy<IntGaugeVec> = Lazy::new(|| {
    fe_gauge_vec(
        frontend_perf::STAGE_REQUESTS,
        "Number of requests currently in the given pipeline stage",
        &["stage", "phase"],
    )
});

/// 每阶段耗时（labels: `stage`）。
pub static STAGE_DURATION_SECONDS: Lazy<HistogramVec> = Lazy::new(|| {
    fe_histogram_vec(
        frontend_perf::STAGE_DURATION_SECONDS,
        "Pipeline stage duration (seconds)",
        vec![
            0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0,
        ],
        &["stage"],
    )
});

/// 预处理中 `gather_tokens` 耗时。
pub static TOKENIZE_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    fe_histogram(
        frontend_perf::TOKENIZE_SECONDS,
        "Tokenization time in preprocessor (seconds)",
        vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
    )
});

/// 预处理中 `apply_template` 耗时。
pub static TEMPLATE_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    fe_histogram(
        frontend_perf::TEMPLATE_SECONDS,
        "Template application time in preprocessor (seconds)",
        vec![0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05],
    )
});

/// 累计 detokenization 耗时（μs）；Prometheus 端用 `rate(total) / rate(count)`
/// 算 per-token 平均。
pub static DETOKENIZE_TOTAL_US: Lazy<Counter> = Lazy::new(|| {
    fe_counter(
        frontend_perf::DETOKENIZE_TOTAL_US,
        "Cumulative detokenization time (microseconds)",
    )
});

/// 累计 detokenize 的 token 数。
pub static DETOKENIZE_TOKEN_COUNT: Lazy<Counter> = Lazy::new(|| {
    fe_counter(
        frontend_perf::DETOKENIZE_TOKEN_COUNT,
        "Total tokens detokenized",
    )
});

// === RAII StageGuard =========================================================

/// 进入阶段时 +1、Drop 时 -1 的 RAII guard。
///
/// 任何 control-flow 终止路径（return / `?` / panic / stream 提前结束）
/// 都会自动归还计数，避免 gauge 永远漂高。
pub struct StageGuard {
    gauge: prometheus::IntGauge,
}

impl StageGuard {
    /// 进入阶段时调用：拿到对应 label 的 gauge → `inc()` → 包装返回。
    ///
    /// - `stage`：阶段名，建议使用 `frontend_perf::STAGE_*` 常量；
    /// - `phase`：请求阶段（`"prefill"|"decode"|"aggregated"|""`）。
    pub fn new(stage: &str, phase: &str) -> Self {
        let gauge = STAGE_REQUESTS.with_label_values(&[stage, phase]);
        gauge.inc();
        Self { gauge }
    }
}

impl Drop for StageGuard {
    fn drop(&mut self) {
        // 单独提取 `gauge` 局部变量，便于在 hot-path 上设置调试断点。
        let gauge = &self.gauge;
        gauge.dec();
    }
}

// === 双 OnceCell 独立注册 ====================================================

static REGISTERED: OnceCell<()> = OnceCell::new();
static PROMETHEUS_REGISTERED: OnceCell<()> = OnceCell::new();

/// 注册到项目自有 `MetricsRegistry`。**幂等**。
///
/// 这里使用 `add_metric`（容错版）而非 `add_metric_or_warn`：单个失败
/// 直接忽略，因为 Frontend 指标在 LLM crate 里可能已被独立注册过一次，
/// 我们容忍这种重叠避免主流程被打断。
pub fn ensure_frontend_perf_metrics_registered(registry: &MetricsRegistry) {
    let _ = REGISTERED.get_or_init(|| {
        for collector in all_collectors() {
            let _ = registry.add_metric(collector);
        }
    });
}

/// 注册到原生 `prometheus::Registry`。**幂等**。
///
/// 注意：这里在任意 collector 注册失败时就返回 `Err`，**不缓存错误**；
/// `OnceCell` 仅在全部 collector 注册成功后才置位，从而允许调用方调整
/// registry 后再次重试。该语义与 transport_metrics / request_plane 的
/// “首次失败永久缓存”不同——frontend 指标多被 LLM crate 复用，更适合
/// 让调用方修复 registry 后重试。
pub fn ensure_frontend_perf_metrics_registered_prometheus(
    registry: &Registry,
) -> Result<(), prometheus::Error> {
    if PROMETHEUS_REGISTERED.get().is_none() {
        for collector in all_collectors() {
            registry.register(collector)?;
        }
        let _ = PROMETHEUS_REGISTERED.set(());
    }
    Ok(())
}

fn all_collectors() -> [Box<dyn prometheus::core::Collector>; 6] {
    [
        Box::new(STAGE_REQUESTS.clone()),
        Box::new(STAGE_DURATION_SECONDS.clone()),
        Box::new(TOKENIZE_SECONDS.clone()),
        Box::new(TEMPLATE_SECONDS.clone()),
        Box::new(DETOKENIZE_TOTAL_US.clone()),
        Box::new(DETOKENIZE_TOKEN_COUNT.clone()),
    ]
}

// === 单元测试 =================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// ## 测试过程
    /// 两层嵌套作用域分别创建 StageGuard；每层结束后断言 gauge 准确回归。
    /// ## 意义
    /// 验证 RAII inc/dec 在常规作用域结束时按 LIFO 顺序正确归还。
    #[test]
    fn test_stage_guard_inc_dec() {
        let gauge = STAGE_REQUESTS.with_label_values(&["test_stage", "test_phase"]);
        assert_eq!(gauge.get(), 0);

        {
            let _g = StageGuard::new("test_stage", "test_phase");
            assert_eq!(gauge.get(), 1);
            {
                let _g2 = StageGuard::new("test_stage", "test_phase");
                assert_eq!(gauge.get(), 2);
            }
            assert_eq!(gauge.get(), 1);
        }
        assert_eq!(gauge.get(), 0);
    }

    /// ## 测试过程
    /// 创建 3 个不同 label 组合的 StageGuard，验证彼此 gauge 独立；
    /// 提前 drop 中间一个，断言其他不受影响。
    /// ## 意义
    /// 防止 label 索引串扰导致 gauge 计数互相污染。
    #[test]
    fn test_stage_guard_different_labels() {
        let preprocess = STAGE_REQUESTS.with_label_values(&["preprocess_t", ""]);
        let route_prefill = STAGE_REQUESTS.with_label_values(&["route_t", "prefill"]);
        let route_decode = STAGE_REQUESTS.with_label_values(&["route_t", "decode"]);

        let _g1 = StageGuard::new("preprocess_t", "");
        let _g2 = StageGuard::new("route_t", "prefill");
        let _g3 = StageGuard::new("route_t", "decode");

        assert_eq!(preprocess.get(), 1);
        assert_eq!(route_prefill.get(), 1);
        assert_eq!(route_decode.get(), 1);

        drop(_g2);
        assert_eq!(preprocess.get(), 1);
        assert_eq!(route_prefill.get(), 0);
        assert_eq!(route_decode.get(), 1);
    }

    use prometheus::core::Collector;
    use std::collections::BTreeSet;
    use std::process::Command;

    fn expected_metric_names() -> BTreeSet<String> {
        [
            frontend_metric_name(frontend_perf::STAGE_REQUESTS),
            frontend_metric_name(frontend_perf::STAGE_DURATION_SECONDS),
            frontend_metric_name(frontend_perf::TOKENIZE_SECONDS),
            frontend_metric_name(frontend_perf::TEMPLATE_SECONDS),
            frontend_metric_name(frontend_perf::DETOKENIZE_TOTAL_US),
            frontend_metric_name(frontend_perf::DETOKENIZE_TOKEN_COUNT),
        ]
        .into_iter()
        .collect()
    }

    fn metric_family_names(families: &[prometheus::proto::MetricFamily]) -> BTreeSet<String> {
        families.iter().map(|f| f.name().to_string()).collect()
    }

    fn emit_frontend_metric_samples(stage_suffix: &str) {
        let stage = format!("supplemental_stage_{stage_suffix}");
        let phase = format!("supplemental_phase_{stage_suffix}");

        let _g = StageGuard::new(&stage, &phase);
        STAGE_DURATION_SECONDS.with_label_values(&[&stage]).observe(0.0025);
        TOKENIZE_SECONDS.observe(0.001);
        TEMPLATE_SECONDS.observe(0.0001);
        DETOKENIZE_TOTAL_US.inc_by(42.0);
        DETOKENIZE_TOKEN_COUNT.inc_by(7.0);
    }

    fn run_frontend_perf_subprocess(test_name: &str) {
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
    /// 检查全部 6 个指标全名 + 6 个 desc.fq_name/help。
    /// ## 意义
    /// 锁定对外可观测契约。
    #[test]
    fn test_supplemental_frontend_metric_name_and_descriptors() {
        assert_eq!(
            frontend_metric_name(frontend_perf::STAGE_REQUESTS),
            "pagoda_frontend_stage_requests"
        );
        assert_eq!(
            frontend_metric_name(frontend_perf::STAGE_DURATION_SECONDS),
            "pagoda_frontend_stage_duration_seconds"
        );
        assert_eq!(
            frontend_metric_name(frontend_perf::TOKENIZE_SECONDS),
            "pagoda_frontend_tokenize_seconds"
        );
        assert_eq!(
            frontend_metric_name(frontend_perf::TEMPLATE_SECONDS),
            "pagoda_frontend_template_seconds"
        );
        assert_eq!(
            frontend_metric_name(frontend_perf::DETOKENIZE_TOTAL_US),
            "pagoda_frontend_detokenize_total_us"
        );
        assert_eq!(
            frontend_metric_name(frontend_perf::DETOKENIZE_TOKEN_COUNT),
            "pagoda_frontend_detokenize_token_count"
        );

        let sr = STAGE_REQUESTS.desc();
        assert_eq!(sr.len(), 1);
        assert_eq!(sr[0].fq_name, frontend_metric_name(frontend_perf::STAGE_REQUESTS));
        assert_eq!(sr[0].help, "Number of requests currently in the given pipeline stage");

        let sd = STAGE_DURATION_SECONDS.desc();
        assert_eq!(sd.len(), 1);
        assert_eq!(
            sd[0].fq_name,
            frontend_metric_name(frontend_perf::STAGE_DURATION_SECONDS)
        );
        assert_eq!(sd[0].help, "Pipeline stage duration (seconds)");

        let tk = TOKENIZE_SECONDS.desc();
        assert_eq!(tk.len(), 1);
        assert_eq!(tk[0].fq_name, frontend_metric_name(frontend_perf::TOKENIZE_SECONDS));
        assert_eq!(tk[0].help, "Tokenization time in preprocessor (seconds)");

        let tp = TEMPLATE_SECONDS.desc();
        assert_eq!(tp.len(), 1);
        assert_eq!(tp[0].fq_name, frontend_metric_name(frontend_perf::TEMPLATE_SECONDS));
        assert_eq!(tp[0].help, "Template application time in preprocessor (seconds)");

        let dt = DETOKENIZE_TOTAL_US.desc();
        assert_eq!(dt.len(), 1);
        assert_eq!(
            dt[0].fq_name,
            frontend_metric_name(frontend_perf::DETOKENIZE_TOTAL_US)
        );
        assert_eq!(dt[0].help, "Cumulative detokenization time (microseconds)");

        let dc = DETOKENIZE_TOKEN_COUNT.desc();
        assert_eq!(dc.len(), 1);
        assert_eq!(
            dc[0].fq_name,
            frontend_metric_name(frontend_perf::DETOKENIZE_TOKEN_COUNT)
        );
        assert_eq!(dc[0].help, "Total tokens detokenized");
    }

    /// ## 测试过程
    /// 同一 stage 不同 phase 创建两个 guard，验证 label 隔离；
    /// `move` 后再 drop，验证移动语义不破坏计数；
    /// 显式 `drop(guard)` 验证 RAII 正确归还。
    /// ## 意义
    /// 覆盖“move 后 drop”、“显式 drop”、“label 隔离”三种生命周期路径。
    #[test]
    fn test_supplemental_stage_guard_explicit_drop_and_label_isolation() {
        let primary = STAGE_REQUESTS.with_label_values(&[
            "supplemental_stage_guard_primary",
            "supplemental_phase_primary",
        ]);
        let secondary = STAGE_REQUESTS.with_label_values(&[
            "supplemental_stage_guard_primary",
            "supplemental_phase_secondary",
        ]);

        assert_eq!(primary.get(), 0);
        assert_eq!(secondary.get(), 0);

        let guard = StageGuard::new(
            "supplemental_stage_guard_primary",
            "supplemental_phase_primary",
        );
        assert_eq!(primary.get(), 1);
        assert_eq!(secondary.get(), 0);

        let second_guard = StageGuard::new(
            "supplemental_stage_guard_primary",
            "supplemental_phase_secondary",
        );
        assert_eq!(primary.get(), 1);
        assert_eq!(secondary.get(), 1);

        let moved = guard;
        assert_eq!(primary.get(), 1);

        drop(moved);
        assert_eq!(primary.get(), 0);
        assert_eq!(secondary.get(), 1);

        drop(second_guard);
        assert_eq!(secondary.get(), 0);
    }

    /// ## 测试过程 / ## 意义
    /// fork 子进程验证 MetricsRegistry 注册路径幂等。
    #[test]
    fn test_supplemental_metrics_registry_registration_via_subprocess() {
        run_frontend_perf_subprocess(
            "metrics::frontend_perf::tests::subprocess_metrics_registry_registration",
        );
    }

    /// ## 测试过程 / ## 意义
    /// fork 子进程验证 Prometheus 注册路径幂等。
    #[test]
    fn test_supplemental_prometheus_registration_via_subprocess() {
        run_frontend_perf_subprocess(
            "metrics::frontend_perf::tests::subprocess_prometheus_registration",
        );
    }

    #[test]
    #[ignore = "subprocess helper"]
    fn subprocess_metrics_registry_registration() {
        let registry = MetricsRegistry::new();

        ensure_frontend_perf_metrics_registered(&registry);
        emit_frontend_metric_samples("registry");

        let gathered = registry.get_prometheus_registry().gather();
        assert_eq!(metric_family_names(&gathered), expected_metric_names());

        ensure_frontend_perf_metrics_registered(&registry);
        let gathered2 = registry.get_prometheus_registry().gather();
        assert_eq!(metric_family_names(&gathered2), expected_metric_names());
    }

    #[test]
    #[ignore = "subprocess helper"]
    fn subprocess_prometheus_registration() {
        let mr = MetricsRegistry::new();
        ensure_frontend_perf_metrics_registered(&mr);

        let pr = Registry::new();
        ensure_frontend_perf_metrics_registered_prometheus(&pr).unwrap();
        emit_frontend_metric_samples("prometheus");

        let gathered = pr.gather();
        assert_eq!(metric_family_names(&gathered), expected_metric_names());

        ensure_frontend_perf_metrics_registered_prometheus(&pr).unwrap();
        let gathered2 = pr.gather();
        assert_eq!(metric_family_names(&gathered2), expected_metric_names());
    }
}
