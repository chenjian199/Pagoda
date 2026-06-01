// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # Transport 层 Prometheus 计数器
//!
//! ## 设计意图
//!
//! TCP / NATS 客户端的发送热路径不应每次去查注册表或构造对象，因此本模块把
//! 4 个核心计数器声明为 `Lazy<Counter | IntCounterVec>` 单例，让发送侧的
//! `inc_by` 调用零分配、零锁竞争（Prometheus counter 内部 atomic）。
//!
//! ## 外部契约
//!
//! - 公开静态：`TCP_BYTES_SENT_TOTAL` / `TCP_BYTES_RECEIVED_TOTAL` /
//!   `TCP_ERRORS_TOTAL` / `NATS_ERRORS_TOTAL`；调用方直接 `.inc(_by)`；
//! - [`ensure_transport_metrics_registered_prometheus`]：把上述 4 个计数器
//!   注册进给定 `prometheus::Registry`，**幂等**——多次调用不重复注册，
//!   且首次失败会被缓存，后续所有调用都返回同一错误（避免静默部分注册）。
//!
//! ## 实现要点
//!
//! - 公共的 “拼前缀 + Counter::new + expect” 流程抽离到 [`tcp_counter`]，
//!   各计数器只需一行表达式即可声明；
//! - `OnceCell<Result<(), String>>` 既保证单次注册，又把 `prometheus::Error`
//!   降为 `String` 存储——`prometheus::Error` 不 `Clone`，无法直接缓存；
//! - 真正的注册流程抽到 [`register_all`]，与 `OnceCell` 解耦便于阅读。

use once_cell::sync::{Lazy, OnceCell};
use prometheus::{Counter, IntCounterVec, Opts};

use super::prometheus_names::{name_prefix, transport};

// === 公共命名 helper =========================================================

/// 拼接 `pagoda_transport_<suffix>` 形式的指标全名。
fn transport_metric_name(suffix: &str) -> String {
    format!("{}_{}", name_prefix::TRANSPORT, suffix)
}

/// 公共的 TCP `Counter` 构造模板：拼名 + 描述 + 失败即 panic。
fn tcp_counter(suffix: &str, help: &'static str) -> Counter {
    Counter::new(transport_metric_name(suffix), help)
        .unwrap_or_else(|e| panic!("failed to build transport counter {suffix}: {e}"))
}

// === TCP 计数器 ==============================================================

pub static TCP_BYTES_SENT_TOTAL: Lazy<Counter> = Lazy::new(|| {
    tcp_counter(
        transport::tcp::BYTES_SENT_TOTAL,
        "Total bytes sent by TCP request client",
    )
});

pub static TCP_BYTES_RECEIVED_TOTAL: Lazy<Counter> = Lazy::new(|| {
    tcp_counter(
        transport::tcp::BYTES_RECEIVED_TOTAL,
        "Total bytes received by TCP request client",
    )
});

pub static TCP_ERRORS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    tcp_counter(
        transport::tcp::ERRORS_TOTAL,
        "Total TCP request errors (send failure or timeout)",
    )
});

// === NATS 计数器 =============================================================

/// `error_type` 标签当前取值：`"request_failed"`。
pub static NATS_ERRORS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            transport_metric_name(transport::nats::ERRORS_TOTAL),
            "Total NATS request errors (label: error_type)",
        ),
        &["error_type"],
    )
    .expect("nats_errors_total counter vec")
});

// === 幂等注册逻辑 ============================================================

/// 缓存首次 `register` 的结果（包括错误），保证“注册一次即定型”。
static PROMETHEUS_REGISTERED: OnceCell<Result<(), String>> = OnceCell::new();

/// 把 transport 层的 4 个计数器注册进 `registry`。**幂等**。
pub fn ensure_transport_metrics_registered_prometheus(
    registry: &prometheus::Registry,
) -> Result<(), prometheus::Error> {
    let result = PROMETHEUS_REGISTERED.get_or_init(|| register_all(registry));
    match result {
        Ok(()) => Ok(()),
        Err(msg) => Err(prometheus::Error::Msg(msg.clone())),
    }
}

/// 真正执行注册的内部函数。
fn register_all(registry: &prometheus::Registry) -> Result<(), String> {
    let collectors: [Box<dyn prometheus::core::Collector>; 4] = [
        Box::new(TCP_BYTES_SENT_TOTAL.clone()),
        Box::new(TCP_BYTES_RECEIVED_TOTAL.clone()),
        Box::new(TCP_ERRORS_TOTAL.clone()),
        Box::new(NATS_ERRORS_TOTAL.clone()),
    ];
    collectors
        .into_iter()
        .try_for_each(|c| registry.register(c))
        .map_err(|e| e.to_string())
}

// === 单元测试 =================================================================
//
// 注意：`PROMETHEUS_REGISTERED` 是进程级 `OnceCell`，整个测试二进制只能初始化一次。
// 因此“注册路径”的两类测试通过 fork 当前测试二进制（`#[ignore]` 子进程）来隔离全局态。

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::core::Collector;
    use std::collections::BTreeSet;
    use std::process::Command;

    fn expected_metric_names() -> BTreeSet<String> {
        [
            transport_metric_name(transport::tcp::BYTES_SENT_TOTAL),
            transport_metric_name(transport::tcp::BYTES_RECEIVED_TOTAL),
            transport_metric_name(transport::tcp::ERRORS_TOTAL),
            transport_metric_name(transport::nats::ERRORS_TOTAL),
        ]
        .into_iter()
        .collect()
    }

    fn metric_family_names(families: &[prometheus::proto::MetricFamily]) -> BTreeSet<String> {
        families.iter().map(|f| f.name().to_string()).collect()
    }

    fn emit_transport_metric_samples() {
        TCP_BYTES_SENT_TOTAL.inc_by(128.0);
        TCP_BYTES_RECEIVED_TOTAL.inc_by(256.0);
        TCP_ERRORS_TOTAL.inc_by(1.0);
        NATS_ERRORS_TOTAL
            .with_label_values(&["request_failed"])
            .inc_by(2);
    }

    /// 在子进程中跑一个被 `#[ignore]` 标记的辅助 test，捕获 stdout/stderr。
    fn run_transport_subprocess(test_name: &str) {
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
    /// 直接调 `transport_metric_name` 验证前缀拼接；再读 `Lazy` 单例 desc
    /// 检查 fq_name 与 help 串一致。
    /// ## 意义
    /// 锁定对外指标全名与描述，防止重命名造成 Grafana / 告警规则脱钩。
    #[test]
    fn test_supplemental_transport_metric_name_and_descriptors() {
        assert_eq!(
            transport_metric_name(transport::tcp::BYTES_SENT_TOTAL),
            "pagoda_transport_tcp_bytes_sent_total"
        );
        assert_eq!(
            transport_metric_name(transport::tcp::BYTES_RECEIVED_TOTAL),
            "pagoda_transport_tcp_bytes_received_total"
        );
        assert_eq!(
            transport_metric_name(transport::tcp::ERRORS_TOTAL),
            "pagoda_transport_tcp_errors_total"
        );
        assert_eq!(
            transport_metric_name(transport::nats::ERRORS_TOTAL),
            "pagoda_transport_nats_errors_total"
        );
        assert_eq!(transport_metric_name(""), format!("{}_", name_prefix::TRANSPORT));
        assert_eq!(
            transport_metric_name("custom-suffix"),
            "pagoda_transport_custom-suffix"
        );

        let sent_desc = TCP_BYTES_SENT_TOTAL.desc();
        assert_eq!(sent_desc.len(), 1);
        assert_eq!(
            sent_desc[0].fq_name,
            transport_metric_name(transport::tcp::BYTES_SENT_TOTAL)
        );
        assert_eq!(sent_desc[0].help, "Total bytes sent by TCP request client");

        let recv_desc = TCP_BYTES_RECEIVED_TOTAL.desc();
        assert_eq!(recv_desc.len(), 1);
        assert_eq!(
            recv_desc[0].fq_name,
            transport_metric_name(transport::tcp::BYTES_RECEIVED_TOTAL)
        );
        assert_eq!(recv_desc[0].help, "Total bytes received by TCP request client");

        let err_desc = TCP_ERRORS_TOTAL.desc();
        assert_eq!(err_desc.len(), 1);
        assert_eq!(
            err_desc[0].fq_name,
            transport_metric_name(transport::tcp::ERRORS_TOTAL)
        );
        assert_eq!(
            err_desc[0].help,
            "Total TCP request errors (send failure or timeout)"
        );

        let nats_desc = NATS_ERRORS_TOTAL.desc();
        assert_eq!(nats_desc.len(), 1);
        assert_eq!(
            nats_desc[0].fq_name,
            transport_metric_name(transport::nats::ERRORS_TOTAL)
        );
        assert_eq!(
            nats_desc[0].help,
            "Total NATS request errors (label: error_type)"
        );
    }

    /// ## 测试过程
    /// 对 4 个计数器各自 `inc_by`，断言读取值的 delta 与传入完全相等
    /// （float 用 epsilon 容差）。
    /// ## 意义
    /// 防止某次 refactor 把 `Counter` 误换成无操作或带聚合的实现导致采样丢失。
    #[test]
    fn test_supplemental_transport_metric_observation_deltas() {
        let epsilon = 0.000_001;

        let sent_before = TCP_BYTES_SENT_TOTAL.get();
        TCP_BYTES_SENT_TOTAL.inc_by(10.0);
        assert!((TCP_BYTES_SENT_TOTAL.get() - (sent_before + 10.0)).abs() < epsilon);

        let recv_before = TCP_BYTES_RECEIVED_TOTAL.get();
        TCP_BYTES_RECEIVED_TOTAL.inc_by(20.0);
        assert!((TCP_BYTES_RECEIVED_TOTAL.get() - (recv_before + 20.0)).abs() < epsilon);

        let errors_before = TCP_ERRORS_TOTAL.get();
        TCP_ERRORS_TOTAL.inc_by(1.0);
        assert!((TCP_ERRORS_TOTAL.get() - (errors_before + 1.0)).abs() < epsilon);

        let nats_before = NATS_ERRORS_TOTAL
            .with_label_values(&["request_failed"])
            .get();
        NATS_ERRORS_TOTAL
            .with_label_values(&["request_failed"])
            .inc_by(3);
        assert_eq!(
            NATS_ERRORS_TOTAL
                .with_label_values(&["request_failed"])
                .get(),
            nats_before + 3
        );
    }

    /// ## 测试过程
    /// fork 子进程跑 `subprocess_prometheus_registration`，检查退出码 0。
    /// ## 意义
    /// 端到端验证 OnceCell 幂等：第一次注册成功并产出 4 个 family，
    /// 第二次注册到新 registry 时 gather() 为空（说明没有重复注册）。
    #[test]
    fn test_supplemental_prometheus_registration_via_subprocess() {
        run_transport_subprocess(
            "metrics::transport_metrics::tests::subprocess_prometheus_registration",
        );
    }

    /// ## 测试过程
    /// fork 子进程跑 `subprocess_prometheus_registration_error_is_cached`。
    /// ## 意义
    /// 验证首次失败后错误被缓存：第二次即使换到健康 registry 仍返回同样错误，
    /// 防止部分注册导致指标半残。
    #[test]
    fn test_supplemental_prometheus_registration_error_is_cached_via_subprocess() {
        run_transport_subprocess(
            "metrics::transport_metrics::tests::subprocess_prometheus_registration_error_is_cached",
        );
    }

    #[test]
    #[ignore = "subprocess helper"]
    fn subprocess_prometheus_registration() {
        let first = prometheus::Registry::new();
        ensure_transport_metrics_registered_prometheus(&first).unwrap();
        emit_transport_metric_samples();
        assert_eq!(metric_family_names(&first.gather()), expected_metric_names());

        let second = prometheus::Registry::new();
        ensure_transport_metrics_registered_prometheus(&second).unwrap();
        assert!(second.gather().is_empty());
    }

    #[test]
    #[ignore = "subprocess helper"]
    fn subprocess_prometheus_registration_error_is_cached() {
        let conflicting = prometheus::Registry::new();
        let conflicting_counter = prometheus::Counter::new(
            transport_metric_name(transport::tcp::BYTES_SENT_TOTAL),
            "conflicting tcp bytes sent",
        )
        .unwrap();
        conflicting.register(Box::new(conflicting_counter)).unwrap();

        let first_error =
            ensure_transport_metrics_registered_prometheus(&conflicting).unwrap_err().to_string();
        assert!(!first_error.is_empty());

        let healthy = prometheus::Registry::new();
        let cached_error =
            ensure_transport_metrics_registered_prometheus(&healthy).unwrap_err().to_string();
        assert_eq!(cached_error, first_error);
        assert!(healthy.gather().is_empty());
    }
}
