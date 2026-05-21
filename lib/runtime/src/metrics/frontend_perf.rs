// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 前端请求性能指标：TTFT、TPOT、排队等待。
//!
//! 指标前缀：`pagoda_frontend_*`

use prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge};

use crate::metrics::{MetricsHierarchy, MetricsRegistry};

/// 前端请求性能指标集。
#[derive(Debug, Clone)]
pub struct FrontendPerfMetrics {
    registry: MetricsRegistry,

    /// Time To First Token（秒）直方图。
    pub ttft_seconds: Histogram,
    /// Time Per Output Token（秒）直方图。
    pub tpot_seconds: Histogram,
    /// 请求在前端队列中的等待时长（秒）。
    pub queue_wait_seconds: Histogram,

    /// 累计已接收请求数。
    pub requests_received_total: IntCounter,
    /// 累计已完成请求数。
    pub requests_completed_total: IntCounter,
    /// 当前排队请求数。
    pub queue_depth: IntGauge,
}

impl FrontendPerfMetrics {
    /// 使用给定的父注册表创建并注册所有指标。
    pub fn new(parent: &MetricsRegistry) -> Self {
        let registry = parent.create_child("frontend_perf");

        let ttft_seconds = Histogram::with_opts(
            HistogramOpts::new(
                super::prometheus_names::FRONTEND_TTFT_SECONDS,
                "Time to first token in seconds",
            )
            .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
        )
        .expect("valid histogram");

        let tpot_seconds = Histogram::with_opts(
            HistogramOpts::new(
                super::prometheus_names::FRONTEND_TPOT_SECONDS,
                "Time per output token in seconds",
            )
            .buckets(vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]),
        )
        .expect("valid histogram");

        let queue_wait_seconds = Histogram::with_opts(
            HistogramOpts::new(
                super::prometheus_names::FRONTEND_QUEUE_WAIT_SECONDS,
                "Queue wait duration in seconds",
            )
            .buckets(vec![0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0]),
        )
        .expect("valid histogram");

        let requests_received_total = IntCounter::new(
            super::prometheus_names::FRONTEND_REQUESTS_RECEIVED_TOTAL,
            "Total requests received by frontend",
        )
        .expect("valid counter");

        let requests_completed_total = IntCounter::new(
            super::prometheus_names::FRONTEND_REQUESTS_COMPLETED_TOTAL,
            "Total requests completed by frontend",
        )
        .expect("valid counter");

        let queue_depth = IntGauge::new(
            super::prometheus_names::FRONTEND_QUEUE_DEPTH,
            "Current number of requests queued at frontend",
        )
        .expect("valid gauge");

        let r = registry.prometheus_registry();
        r.register(Box::new(ttft_seconds.clone())).expect("register ttft");
        r.register(Box::new(tpot_seconds.clone())).expect("register tpot");
        r.register(Box::new(queue_wait_seconds.clone())).expect("register queue_wait");
        r.register(Box::new(requests_received_total.clone())).expect("register reqs_recv");
        r.register(Box::new(requests_completed_total.clone())).expect("register reqs_done");
        r.register(Box::new(queue_depth.clone())).expect("register queue_depth");

        Self {
            registry,
            ttft_seconds,
            tpot_seconds,
            queue_wait_seconds,
            requests_received_total,
            requests_completed_total,
            queue_depth,
        }
    }
}

impl MetricsHierarchy for FrontendPerfMetrics {
    fn basename(&self) -> &str {
        "frontend_perf"
    }

    fn parent_hierarchies(&self) -> Vec<&str> {
        vec!["pagoda"]
    }

    fn get_metrics_registry(&self) -> &MetricsRegistry {
        &self.registry
    }
}
