// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 工作处理器性能指标：`generate()` 延迟、并发度。
//!
//! 指标前缀：`pagoda_work_handler_*`

use prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge};

use crate::metrics::{MetricsHierarchy, MetricsRegistry};

/// 工作处理器性能指标集。
#[derive(Debug, Clone)]
pub struct WorkHandlerPerfMetrics {
    registry: MetricsRegistry,

    /// `generate()` 调用端到端延迟（秒）。
    pub generate_duration_seconds: Histogram,
    /// `generate()` 累计调用次数。
    pub generate_calls_total: IntCounter,
    /// `generate()` 累计错误次数。
    pub generate_errors_total: IntCounter,
    /// 当前并发 `generate()` 调用数。
    pub generate_concurrency: IntGauge,
}

impl WorkHandlerPerfMetrics {
    /// 使用给定的父注册表创建并注册所有指标。
    pub fn new(parent: &MetricsRegistry) -> Self {
        let registry = parent.create_child("work_handler_perf");

        let generate_duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                super::prometheus_names::WORK_HANDLER_GENERATE_DURATION_SECONDS,
                "End-to-end duration of generate() in seconds",
            )
            .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0]),
        )
        .expect("valid histogram");

        let generate_calls_total = IntCounter::new(
            super::prometheus_names::WORK_HANDLER_GENERATE_CALLS_TOTAL,
            "Total generate() invocations",
        )
        .expect("valid counter");

        let generate_errors_total = IntCounter::new(
            super::prometheus_names::WORK_HANDLER_GENERATE_ERRORS_TOTAL,
            "Total generate() errors",
        )
        .expect("valid counter");

        let generate_concurrency = IntGauge::new(
            super::prometheus_names::WORK_HANDLER_GENERATE_CONCURRENCY,
            "Current concurrent generate() calls",
        )
        .expect("valid gauge");

        let r = registry.prometheus_registry();
        r.register(Box::new(generate_duration_seconds.clone())).expect("register gen_dur");
        r.register(Box::new(generate_calls_total.clone())).expect("register gen_calls");
        r.register(Box::new(generate_errors_total.clone())).expect("register gen_err");
        r.register(Box::new(generate_concurrency.clone())).expect("register gen_conc");

        Self {
            registry,
            generate_duration_seconds,
            generate_calls_total,
            generate_errors_total,
            generate_concurrency,
        }
    }
}

impl MetricsHierarchy for WorkHandlerPerfMetrics {
    fn basename(&self) -> &str {
        "work_handler_perf"
    }

    fn parent_hierarchies(&self) -> Vec<&str> {
        vec!["pagoda"]
    }

    fn get_metrics_registry(&self) -> &MetricsRegistry {
        &self.registry
    }
}
