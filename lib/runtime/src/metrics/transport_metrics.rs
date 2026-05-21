// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 传输层指标：字节吞吐、连接数、队列深度。
//!
//! 指标前缀：`pagoda_transport_*`

use prometheus::{IntCounter, IntGauge, Histogram, HistogramOpts};

use crate::metrics::{MetricsHierarchy, MetricsRegistry};

/// 传输层指标集。
#[derive(Debug, Clone)]
pub struct TransportMetrics {
    registry: MetricsRegistry,

    /// 发送字节累计。
    pub bytes_sent_total: IntCounter,
    /// 接收字节累计。
    pub bytes_received_total: IntCounter,
    /// 当前活跃连接数。
    pub active_connections: IntGauge,
    /// 累计建立的连接数。
    pub connections_established_total: IntCounter,
    /// 累计关闭/断开的连接数。
    pub connections_closed_total: IntCounter,
    /// 发送队列深度。
    pub send_queue_depth: IntGauge,
    /// 单次消息发送耗时（秒）。
    pub send_duration_seconds: Histogram,
}

impl TransportMetrics {
    /// 使用给定的父注册表创建并注册所有指标。
    pub fn new(parent: &MetricsRegistry) -> Self {
        let registry = parent.create_child("transport");

        let bytes_sent_total = IntCounter::new(
            super::prometheus_names::TRANSPORT_BYTES_SENT_TOTAL,
            "Total bytes sent over transport",
        )
        .expect("valid counter");

        let bytes_received_total = IntCounter::new(
            super::prometheus_names::TRANSPORT_BYTES_RECEIVED_TOTAL,
            "Total bytes received over transport",
        )
        .expect("valid counter");

        let active_connections = IntGauge::new(
            super::prometheus_names::TRANSPORT_ACTIVE_CONNECTIONS,
            "Current active transport connections",
        )
        .expect("valid gauge");

        let connections_established_total = IntCounter::new(
            super::prometheus_names::TRANSPORT_CONNECTIONS_ESTABLISHED_TOTAL,
            "Total connections established",
        )
        .expect("valid counter");

        let connections_closed_total = IntCounter::new(
            super::prometheus_names::TRANSPORT_CONNECTIONS_CLOSED_TOTAL,
            "Total connections closed",
        )
        .expect("valid counter");

        let send_queue_depth = IntGauge::new(
            super::prometheus_names::TRANSPORT_SEND_QUEUE_DEPTH,
            "Current depth of the transport send queue",
        )
        .expect("valid gauge");

        let send_duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                super::prometheus_names::TRANSPORT_SEND_DURATION_SECONDS,
                "Duration of a single message send in seconds",
            )
            .buckets(vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]),
        )
        .expect("valid histogram");

        let r = registry.prometheus_registry();
        r.register(Box::new(bytes_sent_total.clone())).expect("register bytes_sent");
        r.register(Box::new(bytes_received_total.clone())).expect("register bytes_recv");
        r.register(Box::new(active_connections.clone())).expect("register active_conns");
        r.register(Box::new(connections_established_total.clone())).expect("register conns_est");
        r.register(Box::new(connections_closed_total.clone())).expect("register conns_closed");
        r.register(Box::new(send_queue_depth.clone())).expect("register send_queue");
        r.register(Box::new(send_duration_seconds.clone())).expect("register send_dur");

        Self {
            registry,
            bytes_sent_total,
            bytes_received_total,
            active_connections,
            connections_established_total,
            connections_closed_total,
            send_queue_depth,
            send_duration_seconds,
        }
    }
}

impl MetricsHierarchy for TransportMetrics {
    fn basename(&self) -> &str {
        "transport"
    }

    fn parent_hierarchies(&self) -> Vec<&str> {
        vec!["pagoda"]
    }

    fn get_metrics_registry(&self) -> &MetricsRegistry {
        &self.registry
    }
}
