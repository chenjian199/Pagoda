// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 所有 `PGD_*` 环境变量名字符串常量，集中管理避免散落在代码中。

// ── Runtime ──
pub const PGD_RUNTIME_NUM_WORKER_THREADS: &str = "PGD_RUNTIME_NUM_WORKER_THREADS";
pub const PGD_RUNTIME_MAX_BLOCKING_THREADS: &str = "PGD_RUNTIME_MAX_BLOCKING_THREADS";
pub const PGD_RUNTIME_COMPUTE_THREADS: &str = "PGD_RUNTIME_COMPUTE_THREADS";
pub const PGD_ENABLE_POLL_HISTOGRAM: &str = "PGD_ENABLE_POLL_HISTOGRAM";

// ── Worker ──
pub const PGD_WORKER_GRACEFUL_SHUTDOWN_TIMEOUT: &str = "PGD_WORKER_GRACEFUL_SHUTDOWN_TIMEOUT";

// ── System Status Server ──
pub const PGD_SYSTEM_HOST: &str = "PGD_SYSTEM_HOST";
pub const PGD_SYSTEM_PORT: &str = "PGD_SYSTEM_PORT";
pub const PGD_SYSTEM_STARTING_HEALTH_STATUS: &str = "PGD_SYSTEM_STARTING_HEALTH_STATUS";
pub const PGD_SYSTEM_HEALTH_PATH: &str = "PGD_SYSTEM_HEALTH_PATH";
pub const PGD_SYSTEM_LIVE_PATH: &str = "PGD_SYSTEM_LIVE_PATH";

// ── Discovery ──
pub const PGD_DISCOVERY_BACKEND: &str = "PGD_DISCOVERY_BACKEND";

// ── Request Plane ──
pub const PGD_REQUEST_PLANE: &str = "PGD_REQUEST_PLANE";

// ── Event Plane ──
pub const PGD_EVENT_PLANE: &str = "PGD_EVENT_PLANE";
pub const PGD_EVENT_PLANE_CODEC: &str = "PGD_EVENT_PLANE_CODEC";

// ── NATS ──
pub const PGD_NATS_SERVER: &str = "PGD_NATS_SERVER";

// ── etcd ──
pub const PGD_ETCD_ENDPOINTS: &str = "PGD_ETCD_ENDPOINTS";

// ── TCP ──
pub const PGD_TCP_RPC_HOST: &str = "PGD_TCP_RPC_HOST";
pub const PGD_TCP_RPC_PORT: &str = "PGD_TCP_RPC_PORT";
pub const PGD_TCP_WORKER_POOL_SIZE: &str = "PGD_TCP_WORKER_POOL_SIZE";
pub const PGD_TCP_POOL_SIZE: &str = "PGD_TCP_POOL_SIZE";

// ── HTTP ──
pub const PGD_HTTP_RPC_HOST: &str = "PGD_HTTP_RPC_HOST";
pub const PGD_HTTP_RPC_PORT: &str = "PGD_HTTP_RPC_PORT";
pub const PGD_HTTP_RPC_ROOT_PATH: &str = "PGD_HTTP_RPC_ROOT_PATH";

// ── Health Check ──
pub const PGD_HEALTH_CHECK_ENABLED: &str = "PGD_HEALTH_CHECK_ENABLED";
pub const PGD_CANARY_WAIT_TIME_SECS: &str = "PGD_CANARY_WAIT_TIME_SECS";

// ── Tracing ──
pub const OTEL_EXPORT_ENABLED: &str = "OTEL_EXPORT_ENABLED";
