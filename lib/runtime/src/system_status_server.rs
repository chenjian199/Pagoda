// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 系统运维 HTTP 服务（基于 Axum）。
//!
//! 提供以下端点：
//! - `GET /health` — 综合健康检查（基于 `SystemHealth`）
//! - `GET /live` — 存活探针（始终 200）
//! - `GET /metrics` — Prometheus 指标导出
//! - `GET /engine/*` — 引擎级自定义端点代理
//!
//! 绑定端口由 `PGD_SYSTEM_PORT` 控制。

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use tokio_util::sync::CancellationToken;

use crate::metrics::MetricsRegistry;
use crate::system_health::SystemHealth;

// ──────────────────────── Server Info ─────────────────────────────

/// 已启动的系统状态服务器信息。
#[derive(Debug, Clone)]
pub struct SystemStatusServerInfo {
    /// 服务器实际绑定的地址（含端口）。
    pub bound_addr: SocketAddr,
}

// ──────────────────── Shared Application State ────────────────────

#[derive(Clone)]
struct AppState {
    health: Arc<parking_lot::Mutex<SystemHealth>>,
    metrics_registry: Arc<MetricsRegistry>,
}

// ───────────────────────── Endpoints ─────────────────────────────

/// `GET /health` — 返回 200（健康）或 503（不健康），并附带 JSON 状态明细。
async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    let (is_healthy, detail) = state.health.lock().get_health_status();
    let body = serde_json::json!({
        "status": if is_healthy { "ready" } else { "notready" },
        "portnames": detail,
    });
    let status = if is_healthy {
        axum::http::StatusCode::OK
    } else {
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    };
    (status, axum::Json(body)).into_response()
}

/// `GET /live` — Kubernetes liveness 探针，始终 200。
async fn liveness_handler() -> impl IntoResponse {
    (axum::http::StatusCode::OK, "alive")
}

/// `GET /metrics` — Prometheus text exposition 格式。
async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    match state.metrics_registry.encode_to_text() {
        Ok(body) => (
            axum::http::StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
            body,
        )
            .into_response(),
        Err(_) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "encode error").into_response(),
    }
}

/// `GET /engine/*path` — 引擎级自定义端点代理。
async fn engine_proxy_handler(
    axum::extract::Path(path): axum::extract::Path<String>,
) -> axum::response::Response {
    let _ = path;
    (axum::http::StatusCode::NOT_IMPLEMENTED, "engine proxy not yet implemented").into_response()
}

// ──────────────────────── Server Start ────────────────────────────

/// 启动系统状态 HTTP 服务器。
///
/// # 参数
/// - `health` — 全局健康状态聚合器。
/// - `metrics_registry` — 根指标注册表。
/// - `cancel` — 优雅关闭令牌。
///
/// # 环境变量
/// - `PGD_SYSTEM_PORT`：绑定端口。缺失或 `-1` 时不启动。
///
/// 返回 `None` 表示端口未配置、服务器未启动。
pub async fn start_system_status_server(
    health: Arc<parking_lot::Mutex<SystemHealth>>,
    metrics_registry: Arc<MetricsRegistry>,
    cancel: CancellationToken,
) -> Option<SystemStatusServerInfo> {
    let port: i16 = std::env::var("PGD_SYSTEM_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(-1);

    if port < 0 {
        tracing::info!("PGD_SYSTEM_PORT not set or negative; system status server disabled");
        return None;
    }

    let bind_addr: SocketAddr = ([0, 0, 0, 0], port as u16).into();

    let state = AppState {
        health,
        metrics_registry,
    };

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/live", get(liveness_handler))
        .route("/metrics", get(metrics_handler))
        .route("/engine/{*path}", get(engine_proxy_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind_addr)
        .await
        .expect("failed to bind system status server");

    let bound_addr = listener.local_addr().expect("listener has local addr");

    tracing::info!(%bound_addr, "system status server started");

    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(cancel.cancelled_owned())
            .await
            .expect("system status server error");
    });

    Some(SystemStatusServerInfo { bound_addr })
}
