// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 请求平面指标：RPS、p50/p99 延迟、错误率。
//!
//! 指标前缀：`pagoda_request_plane_*`

use prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge};

use crate::metrics::{MetricsHierarchy, MetricsRegistry};

/// 请求平面指标集。
#[derive(Debug, Clone)]
pub struct RequestPlaneMetrics {
    registry: MetricsRegistry,

    /// 每秒接收请求数的直方图。
    pub request_duration_seconds: Histogram,
    /// 累计请求总数。
    pub requests_total: IntCounter,
    /// 累计请求错误数。
    pub request_errors_total: IntCounter,
    /// 当前正在处理的请求数（flight gauge）。
    pub requests_in_flight: IntGauge,
}

impl RequestPlaneMetrics {
    /// 使用给定的父注册表创建并注册所有指标。
    pub fn new(parent: &MetricsRegistry) -> Self {
        let registry = parent.create_child("request_plane");

        let request_duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                super::prometheus_names::REQUEST_PLANE_DURATION_SECONDS,
                "End-to-end request duration in seconds (use for p50/p99)",
            )
            .buckets(vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
        )
        .expect("valid histogram");

        let requests_total = IntCounter::new(
            super::prometheus_names::REQUEST_PLANE_REQUESTS_TOTAL,
            "Total requests processed by request plane",
        )
        .expect("valid counter");

        let request_errors_total = IntCounter::new(
            super::prometheus_names::REQUEST_PLANE_ERRORS_TOTAL,
            "Total request errors in request plane",
        )
        .expect("valid counter");

        let requests_in_flight = IntGauge::new(
            super::prometheus_names::REQUEST_PLANE_IN_FLIGHT,
            "Current number of in-flight requests",
        )
        .expect("valid gauge");

        let r = registry.prometheus_registry();
        r.register(Box::new(request_duration_seconds.clone())).expect("register req_dur");
        r.register(Box::new(requests_total.clone())).expect("register req_total");
        r.register(Box::new(request_errors_total.clone())).expect("register req_err");
        r.register(Box::new(requests_in_flight.clone())).expect("register in_flight");

        Self {
            registry,
            request_duration_seconds,
            requests_total,
            request_errors_total,
            requests_in_flight,
        }
    }
}

impl MetricsHierarchy for RequestPlaneMetrics {
    fn basename(&self) -> &str {
        "request_plane"
    }

    fn parent_hierarchies(&self) -> Vec<&str> {
        vec!["pagoda"]
    }

    fn get_metrics_registry(&self) -> &MetricsRegistry {
        &self.registry
    }
}
