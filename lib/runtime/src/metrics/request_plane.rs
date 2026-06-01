// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # Request-Plane 时延拆分（AddressedPushRouter 侧）
//!
//! ## 设计意图
//!
//! 排障时需要快速定位“是序列化慢、发送慢，还是 transport 往返慢”。本模块在
//! AddressedPushRouter 的关键点埋 3 个 histogram + 1 个 gauge：
//!
//! - QUEUE_SECONDS：`generate()` 进入 → `send_request()` 调用（序列化 + 编码 + 控制消息）；
//! - SEND_SECONDS：`send_request()` 持续时长（frontend 视角：网络 + 排队 + ack）；
//! - ROUNDTRIP_TTFT_SECONDS：`send_request()` → 首个响应到达（端到端 TTFT）；
//! - INFLIGHT：当前在飞请求数（generate 进入 +1，stream 完成 -1）。
//!
//! ## 外部契约
//!
//! - 4 个公开 `Lazy` 静态：`REQUEST_PLANE_QUEUE_SECONDS` / `_SEND_SECONDS` /
//!   `_ROUNDTRIP_TTFT_SECONDS` / `_INFLIGHT`；
//! - 两个注册入口 [`ensure_request_plane_metrics_registered`] 与
//!   [`ensure_request_plane_metrics_registered_prometheus`]，**各自独立** 幂等；
//! - 首次 Prometheus 注册失败时错误被缓存，后续调用返回同一错误。
//!
//! ## 实现要点
//!
//! - Histogram 构造抽到 [`rp_histogram`]，Gauge 抽到 [`rp_gauge`]，避免每个静态
//!   重复 7 行 builder；
//! - 两套 `OnceCell` 完全独立：哪条注册路径先调用都不会污染另一条；
//! - QUEUE / SEND 桶分布 0.1ms..1s 关注亚毫秒抖动；ROUNDTRIP_TTFT 延展到 5s
//!   覆盖 LLM 首 token SLA。

use once_cell::sync::{Lazy, OnceCell};
use prometheus::{Gauge, Histogram, HistogramOpts};

use super::prometheus_names::{name_prefix, request_plane};
use crate::MetricsRegistry;

// === 命名 + 构造 helpers =====================================================

fn request_plane_metric_name(suffix: &str) -> String {
    format!("{}_{}", name_prefix::REQUEST_PLANE, suffix)
}

fn rp_histogram(suffix: &str, help: &'static str, buckets: Vec<f64>) -> Histogram {
    Histogram::with_opts(
        HistogramOpts::new(request_plane_metric_name(suffix), help).buckets(buckets),
    )
    .unwrap_or_else(|e| panic!("failed to build histogram {suffix}: {e}"))
}

fn rp_gauge(suffix: &str, help: &'static str) -> Gauge {
    Gauge::new(request_plane_metric_name(suffix), help)
        .unwrap_or_else(|e| panic!("failed to build gauge {suffix}: {e}"))
}

// === 公开指标 ================================================================

/// `generate()` 进入到 `send_request()` 调用的时间（含序列化 + 编码 + 控制消息）。
pub static REQUEST_PLANE_QUEUE_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    rp_histogram(
        request_plane::QUEUE_SECONDS,
        "Time from generate() entry to send_request() (seconds)",
        vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
    )
});

/// `send_request()` 完成时间（frontend 视角：网络 + 排队 + ack）。
pub static REQUEST_PLANE_SEND_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    rp_histogram(
        request_plane::SEND_SECONDS,
        "Time for send_request() to complete (seconds)",
        vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
    )
});

/// `send_request()` 到首个响应 item 的往返耗时（transport TTFT）。
pub static REQUEST_PLANE_ROUNDTRIP_TTFT_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    rp_histogram(
        request_plane::ROUNDTRIP_TTFT_SECONDS,
        "Time from send_request() to first response item (seconds)",
        vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
    )
});

/// 当前在飞请求数（`generate()` 进入 +1，stream 完成 -1）。
pub static REQUEST_PLANE_INFLIGHT: Lazy<Gauge> = Lazy::new(|| {
    rp_gauge(
        request_plane::INFLIGHT_REQUESTS,
        "Currently in-flight requests at AddressedPushRouter",
    )
});

// === 双 OnceCell 独立注册 ====================================================

static METRICS_REGISTERED: OnceCell<()> = OnceCell::new();
static PROMETHEUS_REGISTERED: OnceCell<Result<(), String>> = OnceCell::new();

/// 注册到项目自有 `MetricsRegistry`。**幂等**。
pub fn ensure_request_plane_metrics_registered(registry: &MetricsRegistry) {
    let _ = METRICS_REGISTERED.get_or_init(|| {
        for (collector, name) in named_collectors() {
            registry.add_metric_or_warn(collector, name);
        }
    });
}

/// 注册到原生 `prometheus::Registry`。**幂等**，首次失败被缓存。
pub fn ensure_request_plane_metrics_registered_prometheus(
    registry: &prometheus::Registry,
) -> Result<(), prometheus::Error> {
    let result = PROMETHEUS_REGISTERED.get_or_init(|| register_to_prometheus(registry));
    match result {
        Ok(()) => Ok(()),
        Err(msg) => Err(prometheus::Error::Msg(msg.clone())),
    }
}

fn named_collectors() -> [(Box<dyn prometheus::core::Collector>, &'static str); 4] {
    [
        (Box::new(REQUEST_PLANE_QUEUE_SECONDS.clone()), "request_plane_queue_seconds"),
        (Box::new(REQUEST_PLANE_SEND_SECONDS.clone()), "request_plane_send_seconds"),
        (
            Box::new(REQUEST_PLANE_ROUNDTRIP_TTFT_SECONDS.clone()),
            "request_plane_roundtrip_ttft_seconds",
        ),
        (Box::new(REQUEST_PLANE_INFLIGHT.clone()), "request_plane_inflight"),
    ]
}

fn register_to_prometheus(registry: &prometheus::Registry) -> Result<(), String> {
    let collectors: [Box<dyn prometheus::core::Collector>; 4] = [
        Box::new(REQUEST_PLANE_QUEUE_SECONDS.clone()),
        Box::new(REQUEST_PLANE_SEND_SECONDS.clone()),
        Box::new(REQUEST_PLANE_ROUNDTRIP_TTFT_SECONDS.clone()),
        Box::new(REQUEST_PLANE_INFLIGHT.clone()),
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
            request_plane_metric_name(request_plane::QUEUE_SECONDS),
            request_plane_metric_name(request_plane::SEND_SECONDS),
            request_plane_metric_name(request_plane::ROUNDTRIP_TTFT_SECONDS),
            request_plane_metric_name(request_plane::INFLIGHT_REQUESTS),
        ]
        .into_iter()
        .collect()
    }

    fn metric_family_names(families: &[prometheus::proto::MetricFamily]) -> BTreeSet<String> {
        families.iter().map(|f| f.name().to_string()).collect()
    }

    fn emit_request_plane_metric_samples() {
        REQUEST_PLANE_QUEUE_SECONDS.observe(0.125);
        REQUEST_PLANE_SEND_SECONDS.observe(0.0625);
        REQUEST_PLANE_ROUNDTRIP_TTFT_SECONDS.observe(0.5);
        REQUEST_PLANE_INFLIGHT.set(3.0);
    }

    fn run_request_plane_subprocess(test_name: &str) {
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
    /// 检查 4 个指标拼出的全名 + 4 个 desc fq_name/help 与硬编码字符串一致。
    /// ## 意义
    /// 锁定对外指标契约。
    #[test]
    fn test_supplemental_request_plane_metric_name_and_descriptors() {
        assert_eq!(
            request_plane_metric_name(request_plane::QUEUE_SECONDS),
            "pagoda_request_plane_queue_seconds"
        );
        assert_eq!(
            request_plane_metric_name(request_plane::SEND_SECONDS),
            "pagoda_request_plane_send_seconds"
        );
        assert_eq!(
            request_plane_metric_name(request_plane::ROUNDTRIP_TTFT_SECONDS),
            "pagoda_request_plane_roundtrip_ttft_seconds"
        );
        assert_eq!(
            request_plane_metric_name(request_plane::INFLIGHT_REQUESTS),
            "pagoda_request_plane_inflight_requests"
        );
        assert_eq!(request_plane_metric_name(""), format!("{}_", name_prefix::REQUEST_PLANE));
        assert_eq!(
            request_plane_metric_name("custom-suffix"),
            "pagoda_request_plane_custom-suffix"
        );

        let q = REQUEST_PLANE_QUEUE_SECONDS.desc();
        assert_eq!(q.len(), 1);
        assert_eq!(q[0].fq_name, request_plane_metric_name(request_plane::QUEUE_SECONDS));
        assert_eq!(q[0].help, "Time from generate() entry to send_request() (seconds)");

        let s = REQUEST_PLANE_SEND_SECONDS.desc();
        assert_eq!(s.len(), 1);
        assert_eq!(s[0].fq_name, request_plane_metric_name(request_plane::SEND_SECONDS));
        assert_eq!(s[0].help, "Time for send_request() to complete (seconds)");

        let r = REQUEST_PLANE_ROUNDTRIP_TTFT_SECONDS.desc();
        assert_eq!(r.len(), 1);
        assert_eq!(
            r[0].fq_name,
            request_plane_metric_name(request_plane::ROUNDTRIP_TTFT_SECONDS)
        );
        assert_eq!(r[0].help, "Time from send_request() to first response item (seconds)");

        let i = REQUEST_PLANE_INFLIGHT.desc();
        assert_eq!(i.len(), 1);
        assert_eq!(
            i[0].fq_name,
            request_plane_metric_name(request_plane::INFLIGHT_REQUESTS)
        );
        assert_eq!(i[0].help, "Currently in-flight requests at AddressedPushRouter");
    }

    /// ## 测试过程
    /// 三个 histogram 各 observe 一次已知值；inflight gauge inc / dec 各一次。
    /// ## 意义
    /// 验证 observe / inc / dec 真实影响内部状态，未被静默替换。
    #[test]
    fn test_supplemental_request_plane_metric_observation_deltas() {
        let epsilon = 0.000_001;

        let qc = REQUEST_PLANE_QUEUE_SECONDS.get_sample_count();
        let qs = REQUEST_PLANE_QUEUE_SECONDS.get_sample_sum();
        REQUEST_PLANE_QUEUE_SECONDS.observe(0.125);
        assert_eq!(REQUEST_PLANE_QUEUE_SECONDS.get_sample_count(), qc + 1);
        assert!(
            (REQUEST_PLANE_QUEUE_SECONDS.get_sample_sum() - (qs + 0.125)).abs() < epsilon
        );

        let sc = REQUEST_PLANE_SEND_SECONDS.get_sample_count();
        let ss = REQUEST_PLANE_SEND_SECONDS.get_sample_sum();
        REQUEST_PLANE_SEND_SECONDS.observe(0.0625);
        assert_eq!(REQUEST_PLANE_SEND_SECONDS.get_sample_count(), sc + 1);
        assert!(
            (REQUEST_PLANE_SEND_SECONDS.get_sample_sum() - (ss + 0.0625)).abs() < epsilon
        );

        let rc = REQUEST_PLANE_ROUNDTRIP_TTFT_SECONDS.get_sample_count();
        let rs = REQUEST_PLANE_ROUNDTRIP_TTFT_SECONDS.get_sample_sum();
        REQUEST_PLANE_ROUNDTRIP_TTFT_SECONDS.observe(0.5);
        assert_eq!(REQUEST_PLANE_ROUNDTRIP_TTFT_SECONDS.get_sample_count(), rc + 1);
        assert!(
            (REQUEST_PLANE_ROUNDTRIP_TTFT_SECONDS.get_sample_sum() - (rs + 0.5)).abs()
                < epsilon
        );

        let ib = REQUEST_PLANE_INFLIGHT.get();
        REQUEST_PLANE_INFLIGHT.inc();
        assert!((REQUEST_PLANE_INFLIGHT.get() - (ib + 1.0)).abs() < epsilon);
        REQUEST_PLANE_INFLIGHT.dec();
        assert!((REQUEST_PLANE_INFLIGHT.get() - ib).abs() < epsilon);
    }

    // ── 注册路径（OnceCell 隔离用 subprocess） ────────────────────────────────

    #[test]
    fn test_supplemental_metrics_registry_registration_via_subprocess() {
        run_request_plane_subprocess(
            "metrics::request_plane::tests::subprocess_metrics_registry_registration",
        );
    }

    #[test]
    fn test_supplemental_prometheus_registration_via_subprocess() {
        run_request_plane_subprocess(
            "metrics::request_plane::tests::subprocess_prometheus_registration",
        );
    }

    /// ## 测试过程 / ## 意义
    /// 验证“先 MetricsRegistry 后 Prometheus”两条路径互不影响。
    #[test]
    fn test_supplemental_metrics_then_prometheus_independence_via_subprocess() {
        run_request_plane_subprocess(
            "metrics::request_plane::tests::subprocess_metrics_then_prometheus_independence",
        );
    }

    /// ## 测试过程 / ## 意义
    /// 验证“先 Prometheus 后 MetricsRegistry”两条路径互不影响（顺序对称）。
    #[test]
    fn test_supplemental_prometheus_then_metrics_independence_via_subprocess() {
        run_request_plane_subprocess(
            "metrics::request_plane::tests::subprocess_prometheus_then_metrics_independence",
        );
    }

    #[test]
    fn test_supplemental_prometheus_registration_error_is_cached_via_subprocess() {
        run_request_plane_subprocess(
            "metrics::request_plane::tests::subprocess_prometheus_registration_error_is_cached",
        );
    }

    #[test]
    #[ignore = "subprocess helper"]
    fn subprocess_metrics_registry_registration() {
        let first = MetricsRegistry::new();
        ensure_request_plane_metrics_registered(&first);
        emit_request_plane_metric_samples();
        assert_eq!(
            metric_family_names(&first.get_prometheus_registry().gather()),
            expected_metric_names()
        );

        let second = MetricsRegistry::new();
        ensure_request_plane_metrics_registered(&second);
        assert!(second.get_prometheus_registry().gather().is_empty());
    }

    #[test]
    #[ignore = "subprocess helper"]
    fn subprocess_prometheus_registration() {
        let first = prometheus::Registry::new();
        ensure_request_plane_metrics_registered_prometheus(&first).unwrap();
        emit_request_plane_metric_samples();
        assert_eq!(metric_family_names(&first.gather()), expected_metric_names());

        let second = prometheus::Registry::new();
        ensure_request_plane_metrics_registered_prometheus(&second).unwrap();
        assert!(second.gather().is_empty());
    }

    #[test]
    #[ignore = "subprocess helper"]
    fn subprocess_metrics_then_prometheus_independence() {
        let mr = MetricsRegistry::new();
        ensure_request_plane_metrics_registered(&mr);
        let pr = prometheus::Registry::new();
        ensure_request_plane_metrics_registered_prometheus(&pr).unwrap();
        emit_request_plane_metric_samples();
        assert_eq!(
            metric_family_names(&mr.get_prometheus_registry().gather()),
            expected_metric_names()
        );
        assert_eq!(metric_family_names(&pr.gather()), expected_metric_names());
    }

    #[test]
    #[ignore = "subprocess helper"]
    fn subprocess_prometheus_then_metrics_independence() {
        let pr = prometheus::Registry::new();
        ensure_request_plane_metrics_registered_prometheus(&pr).unwrap();
        let mr = MetricsRegistry::new();
        ensure_request_plane_metrics_registered(&mr);
        emit_request_plane_metric_samples();
        assert_eq!(metric_family_names(&pr.gather()), expected_metric_names());
        assert_eq!(
            metric_family_names(&mr.get_prometheus_registry().gather()),
            expected_metric_names()
        );
    }

    #[test]
    #[ignore = "subprocess helper"]
    fn subprocess_prometheus_registration_error_is_cached() {
        let conflicting = prometheus::Registry::new();
        let conflicting_hist = prometheus::Histogram::with_opts(HistogramOpts::new(
            request_plane_metric_name(request_plane::QUEUE_SECONDS),
            "conflicting request-plane queue histogram",
        ))
        .unwrap();
        conflicting.register(Box::new(conflicting_hist)).unwrap();

        let first =
            ensure_request_plane_metrics_registered_prometheus(&conflicting).unwrap_err().to_string();
        assert!(!first.is_empty());

        let healthy = prometheus::Registry::new();
        let cached =
            ensure_request_plane_metrics_registered_prometheus(&healthy).unwrap_err().to_string();
        assert_eq!(cached, first);
        assert!(healthy.gather().is_empty());
    }
}
