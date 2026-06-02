// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 系统状态 / 管理 HTTP 服务（system_status_server）
//!
//! ## 设计意图
//! 为 `DistributedRuntime` 提供一条统一的进程内管理面：健康 / 存活探测、
//! Prometheus 指标输出、发现元数据询询、LoRA 带状态动态加载 / 卸载，
//! 以及面向只读迭代的元数据制品下发、动态推理引擎路由。服务在调用 [`spawn_system_status_server`]
//! 之后以 Axum＋Tokio 异步任务的形式运行，受外部 `CancellationToken` 控制优雅退出。
//!
//! TODO：(DEP-635) 后续该文件应更名为 `system_http_server.rs`，因为它已经超出
//! 纯“status / health”范畴，同时承载了 LoRA 管理、引擎路由等 HTTP 接口。
//!
//! ## 外部契约
//! - 公开类型：`SystemStatusServerInfo`（`Debug + Clone`）、`SystemStatusState`、
//!   `LoadLoraRequest` / `LoraSource` / `LoraResponse`（`Debug + Clone + Serialize + Deserialize`）。
//! - 公开函数 `spawn_system_status_server(host, port, cancel_token, drt, discovery_metadata)`。
//!   返回 `(SocketAddr, JoinHandle<()>)`；服务在 `cancel_token.cancelled()` 上优雅关闭。
//! - HTTP 路由集与历史实现严格一致：
//!   * `GET <health_path>` / `GET <live_path>` —— 返回聚合健康状态与 portname 明细；
//!   * `GET /metrics` —— Prometheus 文本输出；
//!   * `GET /metadata` —— `DiscoveryMetadata` JSON；
//!   * `ANY /engine/{*path}` —— 查询 `drt.engine_routes()` 并执行回调；
//!   * 当 `PGD_LORA_ENABLED=true` 时启用 `GET/POST /v1/loras` 与 `DELETE /v1/loras/{*lora_name}`；
//!   * `GET /v1/metadata/{model_slug}/{model_suffix}/{*filename}` —— 返回本地注册的制品原始字节。
//!   * Fallback —— `404 "Route not found"`，TraceLayer 负责 span 构造。
//! - LoRA 调用仅走 `drt.local_portname_registry()`；未注册时返回错误文本 “not found in local registry”。
//!
//! ## 实现要点
//! - `Router` 按现有 builder 结构依序拼接。考虑到 Axum 子路由的 fallback / state 传播
//!   与原始路由采集在某些边界不严格等价，本次重构**不**引入 `Router::merge` 拆分，
//!   全量保留历史路由拼接顺序以避免可观察行为漂移。
//! - 所有 handler 闭包都使用 `Arc::clone` 捕获状态，避免多份引用超出查请生命周期。
//! - 测试划分为“常规单元测试”与“需要 `integration` feature 的集成测试”两类，后者嵌套
//!   在主 `tests` 模块中作为 `mod integration_tests`（遵循“单一 `mod tests`”的本宝项目公约）。

use crate::config::HealthStatus;
use crate::config::environment_names::logging as env_logging;
use crate::config::environment_names::runtime::canary as env_canary;
use crate::config::environment_names::runtime::system as env_system;
use crate::logging::make_system_request_span;
use crate::metrics::MetricsHierarchy;
use crate::traits::DistributedRuntimeProvider;
use axum::{
    Router,
    body::Bytes,
    extract::{Json, Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{any, delete, get, post},
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Instant;
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;

// === SECTION: 服务信息与共享状态 ===

/// 系统状态服务信息，包含监听地址和后台任务句柄。
#[derive(Debug)]
pub struct SystemStatusServerInfo {
    pub socket_addr: std::net::SocketAddr,
    pub handle: Option<Arc<JoinHandle<()>>>,
}

impl SystemStatusServerInfo {
    /// 根据监听地址和可选任务句柄构造服务信息对象。
    pub fn new(socket_addr: std::net::SocketAddr, handle: Option<JoinHandle<()>>) -> Self {
        let handle = handle.map(Arc::new);
        Self { socket_addr, handle }
    }

    /// 返回 `host:port` 形式的监听地址字符串。
    pub fn address(&self) -> String {
        format!("{}", self.socket_addr)
    }

    /// 返回监听地址中的主机 IP 字符串。
    pub fn hostname(&self) -> String {
        format!("{}", self.socket_addr.ip())
    }

    /// 返回监听端口号。
    pub fn port(&self) -> u16 {
        let port = self.socket_addr.port();
        port
    }
}

impl Clone for SystemStatusServerInfo {
    /// 克隆服务信息，复用同一个后台任务句柄引用。
    fn clone(&self) -> Self {
        let socket_addr = self.socket_addr;
        let handle = self.handle.clone();

        Self { socket_addr, handle }
    }
}

/// 系统状态 HTTP 服务共享状态，持有分布式运行时及发现元数据引用。
pub struct SystemStatusState {
    // 根 DRT 用于对外输出完整 Prometheus 指标以及路由级系统能力。
    root_drt: Arc<crate::DistributedRuntime>,
    // 发现元数据，仅在 Kubernetes 后端场景中存在。
    discovery_metadata: Option<Arc<tokio::sync::RwLock<crate::discovery::DiscoveryMetadata>>>,
}

impl SystemStatusState {
    /// 用给定的 DRT 和可选发现元数据构造系统状态服务状态对象。
    pub fn new(
        drt: Arc<crate::DistributedRuntime>,
        discovery_metadata: Option<Arc<tokio::sync::RwLock<crate::discovery::DiscoveryMetadata>>>,
    ) -> anyhow::Result<Self> {
        let root_drt = drt;

        Ok(Self {
            root_drt,
            discovery_metadata,
        })
    }

    /// 返回内部持有的分布式运行时引用。
    pub fn drt(&self) -> &crate::DistributedRuntime {
        self.root_drt.as_ref()
    }

    /// 返回发现元数据引用；如果当前后端不支持，则返回 `None`。
    pub fn discovery_metadata(
        &self,
    ) -> Option<&Arc<tokio::sync::RwLock<crate::discovery::DiscoveryMetadata>>> {
        match &self.discovery_metadata {
            Some(metadata) => Some(metadata),
            None => None,
        }
    }
}

// === SECTION: LoRA 请求 / 响应 DTO ===

/// `POST /v1/loras` 的请求体。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LoadLoraRequest {
    pub lora_name: String,
    pub source: LoraSource,
}

/// LoRA 加载来源信息。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LoraSource {
    pub uri: String,
}

/// LoRA 相关 HTTP 操作的统一响应结构。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LoraResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lora_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lora_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub loras: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<usize>,
}

// === SECTION: 服务启动入口 ===

/// 启动系统状态 HTTP 服务，并注册健康检查、指标、元数据、LoRA 和引擎路由。
///
/// 处理流程为：读取健康检查路径配置、按开关组装路由、绑定监听地址，
/// 最后在取消令牌控制下异步启动 Axum 服务。
pub async fn spawn_system_status_server(
    host: &str,
    port: u16,
    cancel_token: CancellationToken,
    drt: Arc<crate::DistributedRuntime>,
    discovery_metadata: Option<Arc<tokio::sync::RwLock<crate::discovery::DiscoveryMetadata>>>,
) -> anyhow::Result<(std::net::SocketAddr, tokio::task::JoinHandle<()>)> {
    let server_state = Arc::new(SystemStatusState::new(drt, discovery_metadata)?);
    let (health_path, live_path) = {
        let system_health = server_state.drt().system_health();
        let system_health = system_health.lock();
        (
            system_health.health_path().to_string(),
            system_health.live_path().to_string(),
        )
    };

    let lora_enabled = std::env::var(crate::config::environment_names::llm::PGD_LORA_ENABLED)
        .map(|v| v.to_lowercase() == "true")
        .unwrap_or(false);

    let mut app = Router::new()
        .route(
            &health_path,
            get({
                let state = Arc::clone(&server_state);
                move || health_handler(state)
            }),
        )
        .route(
            &live_path,
            get({
                let state = Arc::clone(&server_state);
                move || health_handler(state)
            }),
        )
        .route(
            "/metrics",
            get({
                let state = Arc::clone(&server_state);
                move || metrics_handler(state)
            }),
        )
        .route(
            "/metadata",
            get({
                let state = Arc::clone(&server_state);
                move || metadata_handler(state)
            }),
        )
        .route(
            "/engine/{*path}",
            any({
                let state = Arc::clone(&server_state);
                move |path, body| engine_route_handler(state, path, body)
            }),
        );

    if lora_enabled {
        app = app
            .route(
                "/v1/loras",
                get({
                    let state = Arc::clone(&server_state);
                    move || list_loras_handler(State(state))
                })
                .post({
                    let state = Arc::clone(&server_state);
                    move |body| load_lora_handler(State(state), body)
                }),
            )
            .route(
                "/v1/loras/{*lora_name}",
                delete({
                    let state = Arc::clone(&server_state);
                    move |path| unload_lora_handler(State(state), path)
                }),
            );
    }

    app = app.route(
        "/v1/metadata/{model_slug}/{model_suffix}/{*filename}",
        get({
            let state = Arc::clone(&server_state);
            move |path| metadata_file_handler(State(state), path)
        }),
    );

    let app = app
        .fallback(|| async {
            tracing::info!("[fallback handler] called");
            (StatusCode::NOT_FOUND, "Route not found").into_response()
        })
        .layer(TraceLayer::new_for_http().make_span_with(make_system_request_span));

    let address = format!("{}:{}", host, port);
    tracing::info!("[spawn_system_status_server] binding to: {address}");

    let listener = TcpListener::bind(&address)
        .await
        .map_err(|err| {
            tracing::error!("Failed to bind to address {}: {}", address, err);
            anyhow::anyhow!("Failed to bind to address: {}", err)
        })?;
    let actual_address = listener.local_addr()?;
    tracing::info!(
        "[spawn_system_status_server] system status server bound to: {}",
        actual_address
    );

    let observer = cancel_token.child_token();
    let handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app)
            .with_graceful_shutdown(observer.cancelled_owned())
            .await
        {
            tracing::error!("System status server error: {e}");
        }
    });

    Ok((actual_address, handle))
}

// === SECTION: HTTP 处理器 ===

/// 健康检查处理器。
/// 它会读取 `SystemHealth` 的聚合状态，并返回对应 HTTP 状态码和 JSON 文本。
#[tracing::instrument(skip_all, level = "trace")]
async fn health_handler(state: Arc<SystemStatusState>) -> impl IntoResponse {
    let system_health = state.drt().system_health();
    let system_health_lock = system_health.lock();
    let (healthy, portnames) = system_health_lock.get_health_status();
    let uptime = Some(system_health_lock.uptime());
    drop(system_health_lock);

    let (status_code, healthy_string) = if healthy {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "notready")
    };

    let response = json!({
        "status": healthy_string,
        "uptime": uptime,
        "portnames": portnames,
    });

    tracing::trace!("Response {}", response.to_string());

    (status_code, response.to_string())
}

/// 指标处理器，返回包含运行时 uptime 在内的 Prometheus 文本格式输出。
#[tracing::instrument(skip_all, level = "trace")]
async fn metrics_handler(state: Arc<SystemStatusState>) -> impl IntoResponse {
    let response = if let Ok(metrics) = state.drt().metrics().prometheus_expfmt() {
        metrics
    } else {
        tracing::error!("Failed to get metrics from registry");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to get metrics".to_string(),
        );
    };

    (StatusCode::OK, response)
}

/// 元数据处理器，负责输出发现后端维护的模型元数据 JSON。
#[tracing::instrument(skip_all, level = "trace")]
async fn metadata_handler(state: Arc<SystemStatusState>) -> impl IntoResponse {
    let Some(metadata) = state.discovery_metadata() else {
        tracing::debug!("Metadata portname called but no discovery metadata available");
        return (
            StatusCode::NOT_FOUND,
            "Discovery metadata not available".to_string(),
        )
            .into_response();
    };

    let metadata_guard = metadata.read().await;

    match serde_json::to_string(&*metadata_guard) {
        Ok(json) => {
            tracing::trace!("Returning metadata: {} bytes", json.len());
            (StatusCode::OK, json).into_response()
        }
        Err(e) => {
            tracing::error!("Failed to serialize metadata: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to serialize metadata".to_string(),
            )
                .into_response()
        }
    }
}

/// 处理 `POST /v1/loras` 请求，执行 LoRA 加载。
#[tracing::instrument(skip_all, level = "debug")]
async fn load_lora_handler(
    State(state): State<Arc<SystemStatusState>>,
    Json(request): Json<LoadLoraRequest>,
) -> impl IntoResponse {
    tracing::info!("Loading LoRA: {}", request.lora_name);

    let portname_result = call_lora_endpoint(
        state.drt(),
        "load_lora",
        json!({
            "lora_name": request.lora_name,
            "source": {
                "uri": request.source.uri
            },
        }),
    )
    .await;

    match portname_result {
        Ok(response) => {
            if response.status == "error" {
                tracing::error!(
                    "Failed to load LoRA {}: {}",
                    request.lora_name,
                    response.message.as_deref().unwrap_or("Unknown error")
                );
                (StatusCode::INTERNAL_SERVER_ERROR, Json(response))
            } else {
                tracing::info!("LoRA loaded successfully: {}", request.lora_name);
                (StatusCode::OK, Json(response))
            }
        }
        Err(e) => {
            tracing::error!("Failed to load LoRA {}: {}", request.lora_name, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(LoraResponse {
                    status: "error".to_string(),
                    message: Some(e.to_string()),
                    lora_name: Some(request.lora_name),
                    lora_id: None,
                    loras: None,
                    count: None,
                }),
            )
        }
    }
}

/// 处理 `DELETE /v1/loras/*lora_name` 请求，执行 LoRA 卸载。
#[tracing::instrument(skip_all, level = "debug")]
async fn unload_lora_handler(
    State(state): State<Arc<SystemStatusState>>,
    Path(lora_name): Path<String>,
) -> impl IntoResponse {
    let lora_name = lora_name
        .strip_prefix('/')
        .unwrap_or(&lora_name)
        .to_string();
    tracing::info!("Unloading LoRA: {lora_name}");

    let portname_result = call_lora_endpoint(
        state.drt(),
        "unload_lora",
        json!({
            "lora_name": lora_name.clone(),
        }),
    )
    .await;

    match portname_result {
        Ok(response) => {
            if response.status == "error" {
                tracing::error!(
                    "Failed to unload LoRA {}: {}",
                    lora_name,
                    response.message.as_deref().unwrap_or("Unknown error")
                );
                (StatusCode::INTERNAL_SERVER_ERROR, Json(response))
            } else {
                tracing::info!("LoRA unloaded successfully: {lora_name}");
                (StatusCode::OK, Json(response))
            }
        }
        Err(e) => {
            tracing::error!("Failed to unload LoRA {}: {}", lora_name, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(LoraResponse {
                    status: "error".to_string(),
                    message: Some(e.to_string()),
                    lora_name: Some(lora_name),
                    lora_id: None,
                    loras: None,
                    count: None,
                }),
            )
        }
    }
}

/// 处理 `GET /v1/loras` 请求，列出当前已加载的 LoRA。
#[tracing::instrument(skip_all, level = "debug")]
async fn list_loras_handler(State(state): State<Arc<SystemStatusState>>) -> impl IntoResponse {
    tracing::info!("Listing all LoRAs");

    let result = call_lora_endpoint(state.drt(), "list_loras", json!({})).await;

    match result {
        Ok(response) => {
            tracing::info!("Successfully retrieved LoRA list");
            (StatusCode::OK, Json(response))
        }
        Err(e) => {
            tracing::error!("Failed to list LoRAs: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(LoraResponse {
                    status: "error".to_string(),
                    message: Some(e.to_string()),
                    lora_name: None,
                    lora_id: None,
                    loras: None,
                    count: None,
                }),
            )
        }
    }
}

/// 处理 `GET /v1/metadata/{slug}/{suffix}/{filename}` 请求。
/// 缺失时返回 404，读取失败返回 500，命中时直接返回原始字节数据，由调用方自行做 blake3 校验。
async fn metadata_file_handler(
    State(state): State<Arc<SystemStatusState>>,
    Path((model_slug, model_suffix, filename)): Path<(String, String, String)>,
) -> impl IntoResponse {
    let Some(path) = state
        .drt()
        .metadata_artifacts()
        .get(&model_slug, &model_suffix, &filename)
    else {
        tracing::debug!(
            model_slug,
            model_suffix,
            filename,
            "metadata artifact not registered for self-host"
        );
        return (StatusCode::NOT_FOUND, "Not found").into_response();
    };

    match tokio::fs::read(&path).await {
        Ok(bytes) => (StatusCode::OK, bytes).into_response(),
        Err(err) => {
            tracing::error!(
                model_slug,
                model_suffix,
                filename,
                path = %path.display(),
                %err,
                "failed to read self-hosted metadata file"
            );
            (StatusCode::INTERNAL_SERVER_ERROR, "Failed to read file").into_response()
        }
    }
}

// === SECTION: LoRA 本地调用辅助 ===

/// 通过进程内本地注册表直接调用 LoRA 管理端点。
///
/// 该函数只走本地注册表，不会在找不到端点时回退到网络发现路径。
async fn call_lora_endpoint(
    drt: &crate::DistributedRuntime,
    portname_name: &str,
    request_body: serde_json::Value,
) -> anyhow::Result<LoraResponse> {
    use crate::engine::AsyncEngine;

    tracing::debug!("Calling local portname: '{portname_name}'");

    let local_registry = drt.local_portname_registry();
    let engine = match local_registry.get(portname_name) {
        Some(engine) => engine,
        None => {
            anyhow::bail!(
                "PortName '{}' not found in local registry. Make sure it's registered with .register_local_engine()",
                portname_name
            );
        }
    };

    tracing::debug!(
        "Found portname '{}' in local registry, calling directly",
        portname_name
    );

    let request = crate::pipeline::SingleIn::new(request_body);
    let mut stream = engine.generate(request).await?;

    let Some(response) = stream.next().await else {
        anyhow::bail!("No response received from portname '{}'", portname_name)
    };

    let response_data = response.data.unwrap_or_default();
    let parsed = serde_json::from_value::<LoraResponse>(response_data.clone())
        .unwrap_or_else(|_| parse_lora_response(&response_data));

    Ok(parsed)
}

/// 将端点返回的通用 JSON 数据尽力解析成 `LoraResponse`。
fn parse_lora_response(response_data: &serde_json::Value) -> LoraResponse {
    let read_string = |key: &str| {
        response_data
            .get(key)
            .and_then(|value| value.as_str())
            .map(str::to_string)
    };

    LoraResponse {
        status: read_string("status").unwrap_or_else(|| "success".to_string()),
        message: read_string("message"),
        lora_name: read_string("lora_name"),
        lora_id: response_data.get("lora_id").and_then(|id| id.as_u64()),
        loras: response_data.get("loras").cloned(),
        count: response_data
            .get("count")
            .and_then(|c| c.as_u64())
            .map(|c| c as usize),
    }
}

/// 处理 `/engine/*` 路由。
///
/// 处理流程为：先把请求体解析成 JSON，再到引擎路由注册表查找回调，
/// 执行后把结果编码成 JSON 文本返回给客户端。
#[tracing::instrument(skip_all, level = "trace", fields(path = %path))]
async fn engine_route_handler(
    state: Arc<SystemStatusState>,
    Path(path): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    tracing::trace!("Engine route request to /engine/{path}");

    let body_json = match body.is_empty() {
        true => serde_json::json!({}),
        false => match serde_json::from_slice(&body) {
            Ok(json) => json,
            Err(err) => {
                tracing::warn!("Invalid JSON in request body: {err}");
                return (
                    StatusCode::BAD_REQUEST,
                    json!({
                        "error": "Invalid JSON",
                        "message": format!("{}", err)
                    })
                    .to_string(),
                )
                    .into_response();
            }
        },
    };

    let callback = if let Some(callback) = state.drt().engine_routes().get(&path) {
        callback
    } else {
        tracing::debug!("Route /engine/{path} not found");
        return (
            StatusCode::NOT_FOUND,
            json!({
                "error": "Route not found",
                "message": format!("Route /engine/{} not found", path)
            })
            .to_string(),
        )
            .into_response();
    };

    match callback(body_json).await {
        Ok(response) => {
            tracing::trace!("Engine route handler succeeded for /engine/{path}");
            (StatusCode::OK, response.to_string()).into_response()
        }
        Err(e) => {
            tracing::error!("Engine route handler error for /engine/{}: {}", path, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({
                    "error": "Handler error",
                    "message": format!("{}", e)
                })
                .to_string(),
            )
                .into_response()
        }
    }
}

// === SECTION: 单元测试（含嵌套集成测试）===

// 常规测试：cargo test system_status_server --lib
// 集成测试：cargo test system_status_server --lib --features integration
#[cfg(test)]
mod tests {
    //! ## 测试过程
    //! 单元层面：以 `MockLoraEngine` 驱动 `call_lora_endpoint` 与 `engine_route_handler`、
    //! 及 `SystemStatusServerInfo` / `SystemStatusState` 的访问器，并验证 HTTP 服务生命周期
    //! 能被 `CancellationToken` 优雅终止。集成层面（`feature = "integration"`）以真实
    //! `DistributedRuntime` 驱动完整服务，覆盖 uptime 推进、指标输出、健康状态变迁、
    //! tracing 请求头、多路径 / 多状态 health/live 接口，以及含 health-check payload 的端点交互。
    //!
    //! ## 意义
    //! 这些用例固定了 system_status_server 的全部对外可观察行为：路由集、各 handler
    //! 的 HTTP 状态码 / 响应体、LoRA 本地调用错误文本、以及健康状态与 Prometheus
    //! 指标输出。本次重构（中文文档、SECTION 划分、集成测试嵌套）仅限于结构 / 文档
    //! 层面调整，**不**变动任何运行时路由拼接顺序与 handler 实现。

    use super::*;
    use crate::engine::{AsyncEngine, AsyncEngineContextProvider, ResponseStream};
    use crate::local_portname_registry::LocalAsyncEngine;
    use crate::pipeline::{ManyOut, SingleIn};
    use crate::protocols::annotated::Annotated;
    use async_trait::async_trait;
    use axum::{body, response::IntoResponse};
    use futures::stream;
    use tokio::time::Duration;

    struct MockLoraEngine {
        chunks: Vec<Annotated<serde_json::Value>>,
    }

    /// 创建测试用分布式运行时，供系统状态服务测试复用。
    async fn create_test_drt() -> Arc<crate::DistributedRuntime> {
        let rt = crate::Runtime::from_current().unwrap();
        let config = crate::distributed::DistributedConfig::process_local();

        Arc::new(crate::DistributedRuntime::new(rt, config).await.unwrap())
    }

    #[async_trait]
    impl AsyncEngine<SingleIn<serde_json::Value>, ManyOut<Annotated<serde_json::Value>>, anyhow::Error>
        for MockLoraEngine
    {
        /// 把预设 chunk 序列包装为响应流，模拟本地 LoRA 管理端点行为。
        async fn generate(
            &self,
            input: SingleIn<serde_json::Value>,
        ) -> anyhow::Result<ManyOut<Annotated<serde_json::Value>>> {
            let (_payload, ctx) = input.into_parts();
            Ok(ResponseStream::new(
                Box::pin(stream::iter(self.chunks.clone())),
                ctx.context(),
            ))
        }
    }

    /// 把 Axum 响应体完整读取为字符串，便于测试断言。
    async fn body_to_string(response: impl IntoResponse) -> String {
        let response = response.into_response();
        let bytes = body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[test]
    /// 测试：系统状态服务信息对象的地址访问器和克隆行为正确。
    fn test_system_status_server_info_accessors_and_clone() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.spawn(async {});
        let info = SystemStatusServerInfo::new(
            "127.0.0.1:8080".parse().unwrap(),
            Some(handle),
        );
        let cloned = info.clone();

        assert_eq!(info.address(), "127.0.0.1:8080");
        assert_eq!(info.hostname(), "127.0.0.1");
        assert_eq!(info.port(), 8080);
        assert!(cloned.handle.is_some());
    }

    #[tokio::test]
    /// 测试：系统状态服务状态对象能正确暴露 DRT 和元数据引用。
    async fn test_system_status_state_accessors() {
        let drt = create_test_drt().await;
        let metadata = Arc::new(tokio::sync::RwLock::new(crate::discovery::DiscoveryMetadata::new()));
        let state = SystemStatusState::new(Arc::clone(&drt), Some(Arc::clone(&metadata))).unwrap();

        assert!(std::ptr::eq(state.drt(), drt.as_ref()));
        assert!(state.discovery_metadata().is_some());
    }

    #[test]
    /// 测试：`parse_lora_response` 能补齐默认值并提取关键字段。
    fn test_parse_lora_response_extracts_defaults_and_fields() {
        let parsed = parse_lora_response(&serde_json::json!({
            "message": "loaded",
            "lora_name": "adapter-a",
            "lora_id": 7,
            "count": 3
        }));

        assert_eq!(parsed.status, "success");
        assert_eq!(parsed.message.as_deref(), Some("loaded"));
        assert_eq!(parsed.lora_name.as_deref(), Some("adapter-a"));
        assert_eq!(parsed.lora_id, Some(7));
        assert_eq!(parsed.count, Some(3));
    }

    #[tokio::test]
    /// 测试：没有发现元数据时，元数据接口返回 404。
    async fn test_metadata_handler_without_metadata_returns_not_found() {
        let drt = create_test_drt().await;
        let state = Arc::new(SystemStatusState::new(drt, None).unwrap());

        let response = metadata_handler(state).await.into_response();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    /// 测试：本地 LoRA 端点调用可以返回结构化 `LoraResponse`。
    async fn test_call_lora_endpoint_returns_structured_response() {
        let drt = create_test_drt().await;
        let engine: LocalAsyncEngine = Arc::new(MockLoraEngine {
            chunks: vec![Annotated::from_data(serde_json::json!({
                "status": "ok",
                "message": "loaded",
                "lora_name": "adapter-a"
            }))],
        });
        drt.local_portname_registry().register(
            "load_lora".to_string(),
            engine,
        );

        let response = call_lora_endpoint(drt.as_ref(), "load_lora", serde_json::json!({})).await.unwrap();

        assert_eq!(response.status, "ok");
        assert_eq!(response.message.as_deref(), Some("loaded"));
        assert_eq!(response.lora_name.as_deref(), Some("adapter-a"));
    }

    #[tokio::test]
    /// 测试：当严格反序列化失败时，会回退到手工解析响应字段。
    async fn test_call_lora_endpoint_falls_back_to_manual_parse() {
        let drt = create_test_drt().await;
        let engine: LocalAsyncEngine = Arc::new(MockLoraEngine {
            chunks: vec![Annotated::from_data(serde_json::json!({
                "loras": ["a", "b"],
                "count": 2
            }))],
        });
        drt.local_portname_registry().register(
            "list_loras".to_string(),
            engine,
        );

        let response = call_lora_endpoint(drt.as_ref(), "list_loras", serde_json::json!({})).await.unwrap();

        assert_eq!(response.status, "success");
        assert_eq!(response.count, Some(2));
        assert_eq!(response.loras, Some(serde_json::json!(["a", "b"])));
    }

    #[tokio::test]
    /// 测试：缺失本地端点时，LoRA 调用会返回错误。
    async fn test_call_lora_endpoint_errors_for_missing_local_endpoint() {
        let drt = create_test_drt().await;

        let error = call_lora_endpoint(drt.as_ref(), "missing_route", serde_json::json!({}))
            .await
            .unwrap_err();

        assert!(error.to_string().contains("not found in local registry"));
    }

    #[tokio::test]
    /// 测试：`/engine/*` 路由会拒绝非法 JSON 请求体。
    async fn test_engine_route_handler_rejects_invalid_json() {
        let drt = create_test_drt().await;
        let state = Arc::new(SystemStatusState::new(drt, None).unwrap());
        let response = engine_route_handler(
            state,
            Path("echo".to_string()),
            Bytes::from_static(br#"{"#),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    /// 测试：未注册的 `/engine/*` 路由会返回 404。
    async fn test_engine_route_handler_returns_not_found_for_missing_route() {
        let drt = create_test_drt().await;
        let state = Arc::new(SystemStatusState::new(drt, None).unwrap());
        let response = engine_route_handler(state, Path("missing".to_string()), Bytes::new())
            .await
            .into_response();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    /// 测试：已注册的 `/engine/*` 回调会被正确执行并返回结果。
    async fn test_engine_route_handler_executes_registered_callback() {
        let drt = create_test_drt().await;
        drt.engine_routes().register(
            "echo",
            Arc::new(|body| Box::pin(async move { Ok(serde_json::json!({"echo": body})) })),
        );
        let state = Arc::new(SystemStatusState::new(Arc::clone(&drt), None).unwrap());
        let response = engine_route_handler(
            state,
            Path("echo".to_string()),
            Bytes::from_static(br#"{"input":"hello"}"#),
        )
        .await
        .into_response();
        let body = body_to_string(response).await;

        assert!(body.contains("hello"));
    }

    #[tokio::test]
    /// 测试：回调内部错误会透传为 500 响应。
    async fn test_engine_route_handler_surfaces_callback_errors() {
        let drt = create_test_drt().await;
        drt.engine_routes().register(
            "explode",
            Arc::new(|_| Box::pin(async move { Err(anyhow::anyhow!("boom")) })),
        );
        let state = Arc::new(SystemStatusState::new(Arc::clone(&drt), None).unwrap());
        let response = engine_route_handler(state, Path("explode".to_string()), Bytes::new())
            .await
            .into_response();
            let status = response.status();
        let body = body_to_string(response).await;

            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(body.contains("boom"));
    }

    // 这是一个基础测试，用来先确认 HTTP 服务生命周期正常，再进入更复杂的测试。
    #[tokio::test]
    /// 测试：取消令牌触发后，基础 HTTP 服务能够正常退出。
    async fn test_http_server_lifecycle() {
        let cancel_token = CancellationToken::new();
        let cancel_token_for_server = cancel_token.clone();

        // 不依赖 DistributedRuntime，先验证最基本的 HTTP 服务生命周期。
        let app = Router::new().route("/test", get(|| async { (StatusCode::OK, "test") }));

        // 启动 HTTP 服务。
        let server_handle = tokio::spawn(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(cancel_token_for_server.cancelled_owned())
                .await;
        });

        // 服务启动很快，这里不需要额外等待。

        // 触发取消令牌。
        cancel_token.cancel();

        // 等待服务退出。
        let result = tokio::time::timeout(Duration::from_secs(5), server_handle).await;
        assert!(
            result.is_ok(),
            "HTTP server should shut down when cancel token is cancelled"
        );
    }

    // === SUBSECTION: 集成测试（feature = "integration"）===

    #[cfg(feature = "integration")]
    mod integration_tests {
        use super::super::*;
        use crate::config::environment_names::logging as env_logging;
    use crate::config::environment_names::runtime::canary as env_canary;
    use crate::distributed::distributed_test_utils::create_test_drt_async;
    use crate::metrics::MetricsHierarchy;
    use anyhow::Result;
    use rstest::rstest;
    use std::sync::Arc;
    use tokio::time::Duration;

    #[tokio::test]
    /// 测试：`SystemHealth` 的 uptime 会随时间推进而增长。
    async fn test_uptime_from_system_health() {
        // 验证可以从 `SystemHealth` 读取 uptime，并且它会持续增长。
        temp_env::async_with_vars([(env_system::PGD_SYSTEM_PORT, None::<&str>)], async {
            let drt = create_test_drt_async().await;

            // 读取初始 uptime。
            let uptime = drt.system_health().lock().uptime();
            // 即使接近零，也应当存在有效 uptime 值。
            assert!(uptime.as_nanos() > 0 || uptime.is_zero());

            // 短暂休眠后，uptime 应继续增长。
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let uptime_after = drt.system_health().lock().uptime();
            assert!(uptime_after > uptime);
        })
        .await;
    }

    #[tokio::test]
    /// 测试：运行时指标输出中会包含预期命名空间下的 uptime 指标。
    async fn test_runtime_metrics_initialization_and_namespace() {
        // 验证指标命名空间和 uptime 指标注册是否正确。
        temp_env::async_with_vars([(env_system::PGD_SYSTEM_PORT, None::<&str>)], async {
            let drt = create_test_drt_async().await;
            // `SystemStatusState` 已在 distributed.rs 中创建，这里直接复用即可。

            // uptime_seconds 指标应当已经被注册并可导出。
            let response = drt.metrics().prometheus_expfmt().unwrap();
            println!("Full metrics response:\n{}", response);

            // 校验 uptime_seconds 指标及其命名空间前缀。
            assert!(
                response.contains("# HELP pagoda_servicegroup_uptime_seconds"),
                "Should contain uptime_seconds help text"
            );
            assert!(
                response.contains("# TYPE pagoda_servicegroup_uptime_seconds gauge"),
                "Should contain uptime_seconds type"
            );
            assert!(
                response.contains("pagoda_servicegroup_uptime_seconds"),
                "Should contain uptime_seconds metric with correct namespace"
            );
        })
        .await;
    }

    #[tokio::test]
    /// 测试：uptime gauge 更新后，其对应的时长值会持续增大。
    async fn test_uptime_gauge_updates() {
        // 验证 uptime gauge 会随着时间推进持续更新。
        temp_env::async_with_vars([(env_system::PGD_SYSTEM_PORT, None::<&str>)], async {
            let drt = create_test_drt_async().await;

            // 记录初始 uptime。
            let initial_uptime = drt.system_health().lock().uptime();

            // 先把初始值写入 gauge。
            drt.system_health().lock().update_uptime_gauge();

            // 等待一小段时间。
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;

            // 再次读取 uptime。
            let uptime_after_sleep = drt.system_health().lock().uptime();

            // 再次刷新 gauge。
            drt.system_health().lock().update_uptime_gauge();

            // 校验 uptime 至少增长了 100ms。
            let elapsed = uptime_after_sleep - initial_uptime;
            assert!(
                elapsed >= std::time::Duration::from_millis(100),
                "Uptime should have increased by at least 100ms after sleep, but only increased by {:?}",
                elapsed
            );
        })
        .await;
    }

    #[tokio::test]
    /// 测试：当系统状态服务未启用时，DRT 不会暴露服务信息。
    async fn test_http_requests_fail_when_system_disabled() {
        // 验证系统状态服务被禁用时不会启动 HTTP 服务。
        temp_env::async_with_vars([(env_system::PGD_SYSTEM_PORT, None::<&str>)], async {
            let drt = create_test_drt_async().await;

            // 期望禁用场景下 `system_status_server_info` 为空。
            let system_info = drt.system_status_server_info();
            assert!(
                system_info.is_none(),
                "System status server should not be running when disabled"
            );

            println!("✓ System status server correctly disabled when not enabled");
        })
        .await;
    }

    /// 测试系统状态服务的健康检查和存活检查端点。
    /// 它会根据初始健康状态和可选自定义路径，验证接口返回的状态码和响应体是否符合预期。
    /// 这里使用多个 `#[case]` 参数化场景，覆盖默认路径与自定义路径两类配置。
    #[rstest]
    #[case("ready", 200, "ready", None, None, 3)]
    #[case("notready", 503, "notready", None, None, 3)]
    #[case("ready", 200, "ready", Some("/custom/health"), Some("/custom/live"), 5)]
    #[case(
        "notready",
        503,
        "notready",
        Some("/custom/health"),
        Some("/custom/live"),
        5
    )]
    #[tokio::test]
    #[cfg(feature = "integration")]
    /// 测试：不同健康状态和路径配置下，健康/存活接口的响应符合预期。
    async fn test_health_portnames(
        #[case] starting_health_status: &'static str,
        #[case] expected_status: u16,
        #[case] expected_body: &'static str,
        #[case] custom_health_path: Option<&'static str>,
        #[case] custom_live_path: Option<&'static str>,
        #[case] expected_num_tests: usize,
    ) {
        use std::sync::Arc;
        // 这里需要显式调用闭包以适配 `async_with_vars`。

        crate::logging::init();

        #[allow(clippy::redundant_closure_call)]
        temp_env::async_with_vars(
            [
                (env_system::PGD_SYSTEM_PORT, Some("0")),
                (
                    env_system::PGD_SYSTEM_STARTING_HEALTH_STATUS,
                    Some(starting_health_status),
                ),
                (env_system::PGD_SYSTEM_HEALTH_PATH, custom_health_path),
                (env_system::PGD_SYSTEM_LIVE_PATH, custom_live_path),
            ],
            (async || {
                let drt = Arc::new(create_test_drt_async().await);

                // 从 DRT 中获取已经自动启动的系统状态服务信息。
                let system_info = drt
                    .system_status_server_info()
                    .expect("System status server should be started by DRT");
                let addr = system_info.socket_addr;

                let client = reqwest::Client::new();

                // 组装要验证的 HTTP 请求用例。
                let mut test_cases = vec![];
                match custom_health_path {
                    None => {
                        // 使用默认路径时，直接验证默认健康路径。
                        test_cases.push(("/health", expected_status, expected_body));
                    }
                    Some(chp) => {
                        // 使用自定义路径时，默认路径应返回 404。
                        test_cases.push(("/health", 404, "Route not found"));
                        test_cases.push((chp, expected_status, expected_body));
                    }
                }
                match custom_live_path {
                    None => {
                        // 使用默认路径时，直接验证默认存活路径。
                        test_cases.push(("/live", expected_status, expected_body));
                    }
                    Some(clp) => {
                        // 使用自定义路径时，默认路径应返回 404。
                        test_cases.push(("/live", 404, "Route not found"));
                        test_cases.push((clp, expected_status, expected_body));
                    }
                }
                test_cases.push(("/someRandomPathNotFoundHere", 404, "Route not found"));
                assert_eq!(test_cases.len(), expected_num_tests);

                for (path, expect_status, expect_body) in test_cases {
                    println!("[test] Sending request to {}", path);
                    let url = format!("http://{}{}", addr, path);
                    let response = client.get(&url).send().await.unwrap();
                    let status = response.status();
                    let body = response.text().await.unwrap();
                    println!(
                        "[test] Response for {}: status={}, body={:?}",
                        path, status, body
                    );
                    assert_eq!(
                        status, expect_status,
                        "Response: status={}, body={:?}",
                        status, body
                    );
                    assert!(
                        body.contains(expect_body),
                        "Response: status={}, body={:?}",
                        status,
                        body
                    );
                }
            })(),
        )
        .await;
    }

    #[tokio::test]
    /// 测试：健康检查接口在 tracing 请求头存在时仍能正常处理请求。
    async fn test_health_portname_tracing() -> Result<()> {
        use std::sync::Arc;

        // 这里需要显式调用闭包以适配 `async_with_vars`。

        #[allow(clippy::redundant_closure_call)]
        let _ = temp_env::async_with_vars(
            [
                (env_system::PGD_SYSTEM_PORT, Some("0")),
                (env_system::PGD_SYSTEM_STARTING_HEALTH_STATUS, Some("ready")),
                (env_logging::PGD_LOGGING_JSONL, Some("1")),
                (env_logging::PGD_LOG, Some("trace")),
            ],
            (async || {
                // TODO: 后续可继续补充对 trace id / parent id 的精确断言。

                crate::logging::init();

                let drt = Arc::new(create_test_drt_async().await);

                // 从 DRT 获取已经自动启动的系统状态服务信息。
                let system_info = drt
                    .system_status_server_info()
                    .expect("System status server should be started by DRT");
                let addr = system_info.socket_addr;
                let client = reqwest::Client::new();
                for path in [("/health"), ("/live"), ("/someRandomPathNotFoundHere")] {
                    let traceparent_value =
                        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
                    let tracestate_value = "vendor1=opaqueValue1,vendor2=opaqueValue2";
                    let mut headers = reqwest::header::HeaderMap::new();
                    headers.insert(
                        reqwest::header::HeaderName::from_static("traceparent"),
                        reqwest::header::HeaderValue::from_str(traceparent_value)?,
                    );
                    headers.insert(
                        reqwest::header::HeaderName::from_static("tracestate"),
                        reqwest::header::HeaderValue::from_str(tracestate_value)?,
                    );
                    let url = format!("http://{}{}", addr, path);
                    let response = client.get(&url).headers(headers).send().await.unwrap();
                    let status = response.status();
                    let body = response.text().await.unwrap();
                    tracing::info!(body = body, status = status.to_string());
                }

                Ok::<(), anyhow::Error>(())
            })(),
        )
        .await;
        Ok(())
    }

    #[tokio::test]
    /// 测试：当端点健康状态变化时，健康检查接口会从 notready 逐步变为 ready。
    async fn test_health_portname_with_changing_health_status() {
        // 验证健康接口会先返回 notready，随后在端点注册并变健康后切换为 ready。
        const ENDPOINT_NAME: &str = "generate";
        const ENDPOINT_HEALTH_CONFIG: &str = "[\"generate\"]";
        temp_env::async_with_vars(
            [
                (env_system::PGD_SYSTEM_PORT, Some("0")),
                (env_system::PGD_SYSTEM_STARTING_HEALTH_STATUS, Some("notready")),
                (env_system::PGD_SYSTEM_USE_ENDPOINT_HEALTH_STATUS, Some(ENDPOINT_HEALTH_CONFIG)),
            ],
            async {
                let drt = Arc::new(create_test_drt_async().await);

                // 确认系统状态服务已经由 DRT 自动启动。
                let system_info_opt = drt.system_status_server_info();

                // 如果这里为空，则说明启动流程没有按预期工作。
                assert!(
                    system_info_opt.is_some(),
                    "System status server was not spawned by DRT. Expected DRT to spawn server when PGD_SYSTEM_PORT is set to a positive value, but system_status_server_info() returned None. Environment: PGD_SYSTEM_PORT={:?}",
                    std::env::var(env_system::PGD_SYSTEM_PORT)
                );

                // 经过上面的断言后，这里可以安全取得服务信息。
                let system_info = system_info_opt.unwrap();
                let addr = system_info.socket_addr;

                // 初始检查应当是 notready。
                let client = reqwest::Client::new();
                let health_url = format!("http://{}/health", addr);

                let response = client.get(&health_url).send().await.unwrap();
                let status = response.status();
                let body = response.text().await.unwrap();

                // 初始状态应返回 503 和 notready。
                assert_eq!(status, 503, "Health should be 503 (not ready) initially, got: {}", status);
                assert!(body.contains("\"status\":\"notready\""), "Health should contain status notready");

                // 接着创建 namespace/servicegroup/portname，让系统进入健康状态。
                let namespace = drt.namespace("ns1234").unwrap();
                let servicegroup = namespace.servicegroup("comp1234").unwrap();

                // 构造一个简单测试 handler，作为端点处理逻辑。
                use crate::pipeline::{async_trait, network::Ingress, AsyncEngine, AsyncEngineContextProvider, Error, ManyOut, SingleIn};
                use crate::protocols::annotated::Annotated;

                struct TestHandler;

                #[async_trait]
                impl AsyncEngine<SingleIn<String>, ManyOut<Annotated<String>>, anyhow::Error> for TestHandler {
                    async fn generate(&self, input: SingleIn<String>) -> anyhow::Result<ManyOut<Annotated<String>>> {
                        let (data, ctx) = input.into_parts();
                        let response = Annotated::from_data(format!("You responded: {}", data));
                        Ok(crate::pipeline::ResponseStream::new(
                            Box::pin(crate::stream::iter(vec![response])),
                            ctx.context()
                        ))
                    }
                }

                // 创建 ingress 并启动端点服务。
                let ingress = Ingress::for_engine(std::sync::Arc::new(TestHandler)).unwrap();

                // 使用健康检查负载启动服务并注册端点名称。
                // 这样会自动把端点名称登记到健康监控中。
                tokio::spawn(async move {
                    let _ = servicegroup.portname(ENDPOINT_NAME)
                        .portname_builder()
                        .handler(ingress)
                        .health_check_payload(serde_json::json!({
                            "test": "health_check"
                        }))
                        .start()
                        .await;
                });

                // 连续请求健康端点 200 次以验证一致性。
                let mut success_count = 0;
                let mut failures = Vec::new();

                for i in 1..=200 {
                    let response = client.get(&health_url).send().await.unwrap();
                    let status = response.status();
                    let body = response.text().await.unwrap();

                    if status == 200 && body.contains("\"status\":\"ready\"") {
                        success_count += 1;
                    } else {
                        failures.push((i, status.as_u16(), body.clone()));
                        if failures.len() <= 5 {  // 只记录前 5 次失败。
                            tracing::warn!("Request {}: status={}, body={}", i, status, body);
                        }
                    }
                }

                tracing::info!("Health portname test results: {success_count}/200 requests succeeded");
                if !failures.is_empty() {
                    tracing::warn!("Failed requests: {}", failures.len());
                }

                // 期望 200 次请求里至少有 150 次成功。
                assert!(success_count >= 150, "Expected at least 150 out of 200 requests to succeed, but only {} succeeded", success_count);
            },
        )
        .await;
    }

    #[tokio::test]
    /// 测试：系统状态服务默认端点能够被正常访问并返回预期内容。
    async fn test_spawn_system_status_server_portnames() {
        // 使用 reqwest 发起 HTTP 请求验证服务端点。
        temp_env::async_with_vars(
            [
                (env_system::PGD_SYSTEM_PORT, Some("0")),
                (env_system::PGD_SYSTEM_STARTING_HEALTH_STATUS, Some("ready")),
            ],
            async {
                let drt = Arc::new(create_test_drt_async().await);

                // 从 DRT 获取自动启动的系统状态服务信息。
                let system_info = drt
                    .system_status_server_info()
                    .expect("System status server should be started by DRT");
                let addr = system_info.socket_addr;
                let client = reqwest::Client::new();
                for (path, expect_200, expect_body) in [
                    ("/health", true, "ready"),
                    ("/live", true, "ready"),
                    ("/someRandomPathNotFoundHere", false, "Route not found"),
                ] {
                    println!("[test] Sending request to {}", path);
                    let url = format!("http://{}{}", addr, path);
                    let response = client.get(&url).send().await.unwrap();
                    let status = response.status();
                    let body = response.text().await.unwrap();
                    println!(
                        "[test] Response for {}: status={}, body={:?}",
                        path, status, body
                    );
                    if expect_200 {
                        assert_eq!(status, 200, "Response: status={}, body={:?}", status, body);
                    } else {
                        assert_eq!(status, 404, "Response: status={}, body={:?}", status, body);
                    }
                    assert!(
                        body.contains(expect_body),
                        "Response: status={}, body={:?}",
                        status,
                        body
                    );
                }
                // DRT 会自动处理服务端清理。
            },
        )
        .await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test]
    /// 测试：带健康检查 payload 的端点在状态变化后，会正确体现在健康接口结果中。
    async fn test_health_check_with_payload_and_timeout() {
        // 验证新版基于 canary 的完整健康检查流程。
        crate::logging::init();

        temp_env::async_with_vars(
            [
                (env_system::PGD_SYSTEM_PORT, Some("0")),
                (
                    env_system::PGD_SYSTEM_STARTING_HEALTH_STATUS,
                    Some("notready"),
                ),
                (
                    env_system::PGD_SYSTEM_USE_ENDPOINT_HEALTH_STATUS,
                    Some("[\"test.portname\"]"),
                ),
                // 为测试缩短健康检查等待和超时时间。
                ("PGD_HEALTH_CHECK_ENABLED", Some("true")),
                (env_canary::PGD_CANARY_WAIT_TIME, Some("1")), // Send canary after 1 second of inactivity
                ("PGD_HEALTH_CHECK_REQUEST_TIMEOUT", Some("1")), // Immediately timeout to mimic unresponsiveness
                ("RUST_LOG", Some("info")),                      // Enable logging for test
            ],
            async {
                let drt = Arc::new(create_test_drt_async().await);

                // 获取系统状态服务信息。
                let system_info = drt
                    .system_status_server_info()
                    .expect("System status server should be started");
                let addr = system_info.socket_addr;

                let client = reqwest::Client::new();
                let health_url = format!("http://{}/health", addr);

                // 注册一个带健康检查 payload 的端点。
                let portname = "test.portname";
                let health_check_payload = serde_json::json!({
                    "prompt": "health check test",
                    "_health_check": true
                });

                // 将端点和健康检查 payload 写入 `SystemHealth`。
                {
                    let system_health = drt.system_health();
                    let system_health_lock = system_health.lock();
                    system_health_lock.register_health_check_target(
                        portname,
                        crate::servicegroup::Instance {
                            servicegroup: "test_servicegroup".to_string(),
                            portname: "health".to_string(),
                            namespace: "test_namespace".to_string(),
                            instance_id: 1,
                            transport: crate::servicegroup::TransportType::Nats(portname.to_string()),
                            device_type: None,
                        },
                        health_check_payload.clone(),
                    );
                }

                // 初始健康状态应为 notready。
                let response = client.get(&health_url).send().await.unwrap();
                let status = response.status();
                let body = response.text().await.unwrap();
                assert_eq!(status, 503, "Should be unhealthy initially (default state)");
                assert!(
                    body.contains("\"status\":\"notready\""),
                    "Should show notready status initially"
                );

                // 再把端点手动标记为 Ready。
                drt.system_health()
                    .lock()
                    .set_portname_health_status(portname, HealthStatus::Ready);

                // 再次访问健康接口，此时应返回 ready。
                let response = client.get(&health_url).send().await.unwrap();
                let status = response.status();
                let body = response.text().await.unwrap();

                assert_eq!(status, 200, "Should be healthy due to recent response");
                assert!(
                    body.contains("\"status\":\"ready\""),
                    "Should show ready status after response"
                );

                // 最后直接检查 `SystemHealth` 内部状态。
                let portname_status = drt
                    .system_health()
                    .lock()
                    .get_portname_health_status(portname);
                assert_eq!(
                    portname_status,
                    Some(HealthStatus::Ready),
                    "SystemHealth should show portname as Ready after response"
                );
            },
        )
        .await;
    }
    }
}
