// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Tokio 运行时性能指标：轮询次数、调度延迟、线程利用率。
//!
//! 指标前缀：`pagoda_tokio_*`

use prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge};

use crate::metrics::{MetricsHierarchy, MetricsRegistry};

/// Tokio 运行时性能指标集。
#[derive(Debug, Clone)]
pub struct TokioPerfMetrics {
    registry: MetricsRegistry,

    /// 任务被轮询的累计次数。
    pub poll_count_total: IntCounter,
    /// 从任务就绪到实际被轮询的调度延迟（秒）。
    pub scheduling_delay_seconds: Histogram,
    /// 单次轮询执行耗时（秒）。
    pub poll_duration_seconds: Histogram,
    /// 活跃 worker 线程数。
    pub active_threads: IntGauge,
    /// 总 worker 线程数。
    pub total_threads: IntGauge,
    /// 线程利用率（0.0 ~ 1.0），由采样计算。
    pub thread_utilization: prometheus::Gauge,
}

impl TokioPerfMetrics {
    /// 使用给定的父注册表创建并注册所有指标。
    pub fn new(parent: &MetricsRegistry) -> Self {
        let registry = parent.create_child("tokio_perf");

        let poll_count_total = IntCounter::new(
            super::prometheus_names::TOKIO_POLL_COUNT_TOTAL,
            "Total number of task polls",
        )
        .expect("valid counter");

        let scheduling_delay_seconds = Histogram::with_opts(
            HistogramOpts::new(
                super::prometheus_names::TOKIO_SCHEDULING_DELAY_SECONDS,
                "Scheduling delay from ready to polled, in seconds",
            )
            .buckets(vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5]),
        )
        .expect("valid histogram");

        let poll_duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                super::prometheus_names::TOKIO_POLL_DURATION_SECONDS,
                "Single poll execution duration in seconds",
            )
            .buckets(vec![0.00001, 0.0001, 0.001, 0.01, 0.1, 1.0]),
        )
        .expect("valid histogram");

        let active_threads = IntGauge::new(
            super::prometheus_names::TOKIO_ACTIVE_THREADS,
            "Number of active Tokio worker threads",
        )
        .expect("valid gauge");

        let total_threads = IntGauge::new(
            super::prometheus_names::TOKIO_TOTAL_THREADS,
            "Total number of Tokio worker threads",
        )
        .expect("valid gauge");

        let thread_utilization = prometheus::Gauge::new(
            super::prometheus_names::TOKIO_THREAD_UTILIZATION,
            "Worker thread utilization ratio (0.0 - 1.0)",
        )
        .expect("valid gauge");

        let r = registry.prometheus_registry();
        r.register(Box::new(poll_count_total.clone())).expect("register poll_count");
        r.register(Box::new(scheduling_delay_seconds.clone())).expect("register sched_delay");
        r.register(Box::new(poll_duration_seconds.clone())).expect("register poll_dur");
        r.register(Box::new(active_threads.clone())).expect("register active_threads");
        r.register(Box::new(total_threads.clone())).expect("register total_threads");
        r.register(Box::new(thread_utilization.clone())).expect("register util");

        Self {
            registry,
            poll_count_total,
            scheduling_delay_seconds,
            poll_duration_seconds,
            active_threads,
            total_threads,
            thread_utilization,
        }
    }
}

impl MetricsHierarchy for TokioPerfMetrics {
    fn basename(&self) -> &str {
        "tokio_perf"
    }

    fn parent_hierarchies(&self) -> Vec<&str> {
        vec!["pagoda"]
    }

    fn get_metrics_registry(&self) -> &MetricsRegistry {
        &self.registry
    }
}
