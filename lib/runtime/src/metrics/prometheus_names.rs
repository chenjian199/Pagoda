// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 所有 Prometheus 指标名称的字符串常量。
//!
//! 集中定义确保指标名称全局唯一且风格一致，
//! 遵循 `pagoda_{subsystem}_{metric}_{unit}` 命名约定。

// ── Frontend Performance ──

pub const FRONTEND_TTFT_SECONDS: &str = "pagoda_frontend_ttft_seconds";
pub const FRONTEND_TPOT_SECONDS: &str = "pagoda_frontend_tpot_seconds";
pub const FRONTEND_QUEUE_WAIT_SECONDS: &str = "pagoda_frontend_queue_wait_seconds";
pub const FRONTEND_REQUESTS_RECEIVED_TOTAL: &str = "pagoda_frontend_requests_received_total";
pub const FRONTEND_REQUESTS_COMPLETED_TOTAL: &str = "pagoda_frontend_requests_completed_total";
pub const FRONTEND_QUEUE_DEPTH: &str = "pagoda_frontend_queue_depth";

// ── Tokio Runtime Performance ──

pub const TOKIO_POLL_COUNT_TOTAL: &str = "pagoda_tokio_poll_count_total";
pub const TOKIO_SCHEDULING_DELAY_SECONDS: &str = "pagoda_tokio_scheduling_delay_seconds";
pub const TOKIO_POLL_DURATION_SECONDS: &str = "pagoda_tokio_poll_duration_seconds";
pub const TOKIO_ACTIVE_THREADS: &str = "pagoda_tokio_active_threads";
pub const TOKIO_TOTAL_THREADS: &str = "pagoda_tokio_total_threads";
pub const TOKIO_THREAD_UTILIZATION: &str = "pagoda_tokio_thread_utilization";

// ── Transport Layer ──

pub const TRANSPORT_BYTES_SENT_TOTAL: &str = "pagoda_transport_bytes_sent_total";
pub const TRANSPORT_BYTES_RECEIVED_TOTAL: &str = "pagoda_transport_bytes_received_total";
pub const TRANSPORT_ACTIVE_CONNECTIONS: &str = "pagoda_transport_active_connections";
pub const TRANSPORT_CONNECTIONS_ESTABLISHED_TOTAL: &str = "pagoda_transport_connections_established_total";
pub const TRANSPORT_CONNECTIONS_CLOSED_TOTAL: &str = "pagoda_transport_connections_closed_total";
pub const TRANSPORT_SEND_QUEUE_DEPTH: &str = "pagoda_transport_send_queue_depth";
pub const TRANSPORT_SEND_DURATION_SECONDS: &str = "pagoda_transport_send_duration_seconds";

// ── Request Plane ──

pub const REQUEST_PLANE_DURATION_SECONDS: &str = "pagoda_request_plane_duration_seconds";
pub const REQUEST_PLANE_REQUESTS_TOTAL: &str = "pagoda_request_plane_requests_total";
pub const REQUEST_PLANE_ERRORS_TOTAL: &str = "pagoda_request_plane_errors_total";
pub const REQUEST_PLANE_IN_FLIGHT: &str = "pagoda_request_plane_in_flight";

// ── Work Handler Performance ──

pub const WORK_HANDLER_GENERATE_DURATION_SECONDS: &str = "pagoda_work_handler_generate_duration_seconds";
pub const WORK_HANDLER_GENERATE_CALLS_TOTAL: &str = "pagoda_work_handler_generate_calls_total";
pub const WORK_HANDLER_GENERATE_ERRORS_TOTAL: &str = "pagoda_work_handler_generate_errors_total";
pub const WORK_HANDLER_GENERATE_CONCURRENCY: &str = "pagoda_work_handler_generate_concurrency";
