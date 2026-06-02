// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::network::ingress::http_endpoint` —— HTTP/2 入站端点服务器
//!
//! ## 设计意图
//! 暴露一个基于 `axum` / `hyper` 的 HTTP/2 服务器，把每条 POST 请求体当作
//! `PushWorkHandler::handle_payload` 的输入，并以"立即 ACK + 后台流响应"模型与
//! TCP/NATS 服务器保持一致。
//!
//! ## 外部契约
//! - 公开 `HttpEndpointServer` 与 `RequestPlaneServer` 实现，签名一致；
//!   `address()` 返回 `http://host:port` 形式，`transport_name() -> "http"`、`is_healthy()` 行为均为契约。
//! - 路由路径与 header 名（`x-pagoda-request-id` 等）是跨语言契约。
//!
//! ## 实现要点
//! - 监听 socket 通过 `tokio::net::TcpListener` 创建后立即记录端口，便于 `address()` 拼接；
//!   监听任务持有 `CancellationToken`，关停时由它触发 graceful。
//! - 服务端不做 retry / rate-limit，这些策略放在更上层的 router。

//! 通过 Axum/HTTP/2 接收请求的 HTTP portname。

use super::*;
use crate::SystemHealth;
use crate::config::HealthStatus;
use crate::logging::TraceParent;
use anyhow::Result;
use axum::{
    Router,
    body::Bytes,
    extract::{Path, State as AxumState},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::post,
};
use dashmap::DashMap;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as Http2Builder;
use hyper_util::service::TowerToHyperService;
use parking_lot::Mutex;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Notify, RwLock};
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;
use tracing::Instrument;

/// pagoda RPC portname 的默认根路径。
const DEFAULT_RPC_ROOT_PATH: &str = "/v1/rpc";

// === SECTION: [1] 版本与常量 ===

/// crate 版本。
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

// === SECTION: [2] 共享 HTTP 服务器与处理器结构 ===

/// 共享 HTTP 服务端，在一个端口上处理多个 portname。
pub struct SharedHttpServer {
    handlers: Arc<DashMap<String, Arc<PortNameHandler>>>,
    bind_addr: SocketAddr,
    actual_addr: RwLock<Option<SocketAddr>>,
    cancellation_token: CancellationToken,
}

/// 某个特定 portname 的处理器。
struct PortNameHandler {
    service_handler: Arc<dyn PushWorkHandler>,
    instance_id: u64,
    namespace: Arc<String>,
    servicegroup_name: Arc<String>,
    portname_name: Arc<String>,
    system_health: Arc<Mutex<SystemHealth>>,
    inflight: Arc<AtomicU64>,
    notify: Arc<Notify>,
}

impl SharedHttpServer {
    pub fn new(bind_addr: SocketAddr, cancellation_token: CancellationToken) -> Arc<Self> {
        Arc::new(Self {
            handlers: Arc::new(DashMap::new()),
            bind_addr,
            actual_addr: RwLock::new(None),
            cancellation_token,
        })
    }

    /// 获取实际绑定地址（在 `bind_and_start` 完成后可用）。
    pub fn actual_address(&self) -> Option<SocketAddr> {
        self.actual_addr.try_read().ok().and_then(|g| *g)
    }

    /// 向这个服务端注册一个 portname 处理器。
    #[allow(clippy::too_many_arguments)]
    pub async fn register_portname(
        &self,
        subject: String,
        service_handler: Arc<dyn PushWorkHandler>,
        instance_id: u64,
        namespace: String,
        servicegroup_name: String,
        portname_name: String,
        system_health: Arc<Mutex<SystemHealth>>,
    ) -> Result<()> {
        let handler = Arc::new(PortNameHandler {
            service_handler,
            instance_id,
            namespace: Arc::new(namespace),
            servicegroup_name: Arc::new(servicegroup_name),
            portname_name: Arc::new(portname_name.clone()),
            system_health: system_health.clone(),
            inflight: Arc::new(AtomicU64::new(0)),
            notify: Arc::new(Notify::new()),
        });

        // 先插入处理器，确保它已经准备好接收请求。
        let subject_clone = subject.clone();
        self.handlers.insert(subject, handler);

        system_health.lock().set_portname_registered(&portname_name);

        tracing::debug!("Registered portname handler for subject: {subject_clone}");
        Ok(())
    }

    /// 从这个服务端注销一个 portname 处理器。
    pub async fn unregister_portname(&self, subject: &str, portname_name: &str) {
        if let Some((_, handler)) = self.handlers.remove(subject) {
            handler
                .system_health
                .lock()
                .set_portname_health_status(portname_name, HealthStatus::NotReady);
            tracing::debug!(
                portname_name = %portname_name,
                subject = %subject,
                "Unregistered HTTP portname handler"
            );

            let inflight_count = handler.inflight.load(Ordering::SeqCst);
            if inflight_count > 0 {
                tracing::info!(
                    portname_name = %portname_name,
                    inflight_count = inflight_count,
                    "Waiting for inflight HTTP requests to complete"
                );
                while handler.inflight.load(Ordering::SeqCst) > 0 {
                    handler.notify.notified().await;
                }
                tracing::info!(
                    portname_name = %portname_name,
                    "All inflight HTTP requests completed"
                );
            }
        }
    }

    /// 绑定 TCP 监听器并启动 accept 循环。
    ///
    /// 返回实际绑定的 `SocketAddr`（在绑定到 0 端口时尤其重要）。
    pub async fn bind_and_start(self: Arc<Self>) -> Result<SocketAddr> {
        let rpc_root_path = std::env::var("PGD_HTTP_RPC_ROOT_PATH")
            .unwrap_or_else(|_| DEFAULT_RPC_ROOT_PATH.to_string());
        let route_pattern = format!("{}/{{*portname}}", rpc_root_path);

        let app = Router::new()
            .route(&route_pattern, post(handle_shared_request))
            .layer(TraceLayer::new_for_http())
            .with_state(self.clone());

        let listener = tokio::net::TcpListener::bind(&self.bind_addr).await?;
        let actual_addr = listener.local_addr()?;

        tracing::info!(
            requested = %self.bind_addr,
            actual = %actual_addr,
            rpc_root = %rpc_root_path,
            "HTTP/2 portname server bound"
        );

        // 保存实际地址，这样 `address()` 就会返回真实端口。
        *self.actual_addr.write().await = Some(actual_addr);

        let cancellation_token = self.cancellation_token.clone();

        // 在后台启动 accept 循环。
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    accept_result = listener.accept() => {
                        match accept_result {
                            Ok((stream, _addr)) => {
                                let app_clone = app.clone();
                                let cancel_clone = cancellation_token.clone();

                                tokio::spawn(async move {
                                    let http2_builder = Http2Builder::new(TokioExecutor::new());

                                    let io = TokioIo::new(stream);
                                    let tower_service = app_clone.into_service();
                                    let hyper_service = TowerToHyperService::new(tower_service);

                                    tokio::select! {
                                        result = http2_builder.serve_connection(io, hyper_service) => {
                                            if let Err(e) = result {
                                                tracing::debug!("HTTP/2 connection error: {e}");
                                            }
                                        }
                                        _ = cancel_clone.cancelled() => {
                                            tracing::trace!("Connection cancelled");
                                        }
                                    }
                                });
                            }
                            Err(e) => {
                                tracing::error!("Failed to accept connection: {e}");
                            }
                        }
                    }
                    _ = cancellation_token.cancelled() => {
                        tracing::info!("SharedHttpServer received cancellation signal, shutting down");
                        return;
                    }
                }
            }
        });

        Ok(actual_addr)
    }

    /// 等待所有 portname 上的 in-flight 请求完成。
    pub async fn wait_for_inflight(&self) {
        for handler in self.handlers.iter() {
            while handler.value().inflight.load(Ordering::SeqCst) > 0 {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }
    }
}

/// 共享服务端的 HTTP 处理器。
async fn handle_shared_request(
    AxumState(server): AxumState<Arc<SharedHttpServer>>,
    Path(portname_path): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    // 查找这个 portname 对应的处理器（通过 DashMap 做无锁读取）。
    let handler = match server.handlers.get(&portname_path) {
        Some(h) => h.clone(),
        None => {
            tracing::warn!("No handler found for portname: {portname_path}");
            return (StatusCode::NOT_FOUND, "PortName not found");
        }
    };

    // 增加 in-flight 计数。
    handler.inflight.fetch_add(1, Ordering::SeqCst);

    // 提取 tracing 头部。
    let traceparent = TraceParent::from_axum_headers(&headers);

    // 启动异步处理任务。
    let service_handler = handler.service_handler.clone();
    let inflight = handler.inflight.clone();
    let notify = handler.notify.clone();
    let namespace = handler.namespace.clone();
    let servicegroup_name = handler.servicegroup_name.clone();
    let portname_name = handler.portname_name.clone();
    let instance_id = handler.instance_id;

    tokio::spawn(async move {
        tracing::trace!(instance_id, "handling new HTTP request");
        let result = service_handler
            .handle_payload(body, traceparent.request_id.clone())
            .instrument(tracing::info_span!(
                "handle_payload",
                servicegroup = servicegroup_name.as_ref(),
                portname = portname_name.as_ref(),
                namespace = namespace.as_ref(),
                instance_id = instance_id,
                trace_id = traceparent.trace_id,
                parent_id = traceparent.parent_id,
                x_request_id = traceparent.x_request_id,
                request_id = traceparent.request_id,
                tracestate = traceparent.tracestate
            ))
            .await;
        match result {
            Ok(_) => {
                tracing::trace!(instance_id, "request handled successfully");
            }
            Err(e) => {
                tracing::warn!("Failed to handle request: {}", e.to_string());
            }
        }

        // 减少 in-flight 计数。
        inflight.fetch_sub(1, Ordering::SeqCst);
        notify.notify_one();
    });

    // 立即返回 202 Accepted（类似 NATS ack）。
    (StatusCode::ACCEPTED, "")
}

// === SECTION: [3] TraceParent 头部解析 ===

/// 为 TraceParent 提供对 Axum 头部支持的扩展实现。
impl TraceParent {
    pub fn from_axum_headers(headers: &HeaderMap) -> Self {
        let mut traceparent = TraceParent::default();

        if let Some(value) = headers.get("traceparent")
            && let Ok(s) = value.to_str()
        {
            traceparent.trace_id = Some(s.to_string());
        }

        if let Some(value) = headers.get("tracestate")
            && let Ok(s) = value.to_str()
        {
            traceparent.tracestate = Some(s.to_string());
        }

        if let Some(value) = headers.get("x-request-id")
            && let Ok(s) = value.to_str()
        {
            traceparent.x_request_id = Some(s.to_string());
        }

        // 从内部头部读取 request-id，并在没有时回退到已弃用的 x-pagoda-request-id。
        if let Some(s) = headers
            .get("request-id")
            .and_then(|v| v.to_str().ok())
            .filter(|s| uuid::Uuid::parse_str(s).is_ok())
        {
            traceparent.request_id = Some(s.to_string());
        } else if let Some(s) = headers
            .get("x-pagoda-request-id")
            .and_then(|v| v.to_str().ok())
            .filter(|s| uuid::Uuid::parse_str(s).is_ok())
        {
            traceparent.request_id = Some(s.to_string());
        }

        traceparent
    }
}

// 为 SharedHttpServer 实现 RequestPlaneServer trait。
#[async_trait::async_trait]
impl super::unified_server::RequestPlaneServer for SharedHttpServer {
    async fn register_portname(
        &self,
        portname_name: String,
        service_handler: Arc<dyn PushWorkHandler>,
        instance_id: u64,
        namespace: String,
        servicegroup_name: String,
        system_health: Arc<Mutex<SystemHealth>>,
    ) -> Result<()> {
        // 对于 HTTP，我们把 portname_name 同时作为 subject（路由键）和 portname_name。
        self.register_portname(
            portname_name.clone(),
            service_handler,
            instance_id,
            namespace,
            servicegroup_name,
            portname_name,
            system_health,
        )
        .await
    }

    async fn unregister_portname(&self, portname_name: &str) -> Result<()> {
        self.unregister_portname(portname_name, portname_name).await;
        Ok(())
    }

    fn address(&self) -> String {
        let addr = self.actual_address().unwrap_or(self.bind_addr);
        format!("http://{}:{}", addr.ip(), addr.port())
    }

    fn transport_name(&self) -> &'static str {
        "http"
    }

    fn is_healthy(&self) -> bool {
        // 服务端只要已经创建，就认为是健康的。
        // TODO：补充更复杂的健康检查（例如检查监听器是否仍然活跃）。
        true
    }
}

// === SECTION: [4] 单元测试 ===

#[cfg(test)]
mod tests {
    //! ## 测试矩阵
    //!
    //! | 测试名 | 覆盖维度 |
    //! |---|---|
    //! | `test_traceparent_from_axum_headers` | 从 Axum HeaderMap 解析 W3C traceparent / tracestate / baggage |
    //! | `test_shared_http_server_creation` | 服务器构造器字段初值（绑定地址、actual_addr 等） |
    //! | `test_bind_and_start_assigns_os_port` | 绑定 0 端口后 `actual_addr` 由 OS 分配且 > 0 |
    //! | `test_two_servers_get_different_ports` | 两个独立实例各自分配到不同端口 |
    //! | `test_bind_and_start_with_explicit_port` | 指定端口时 `actual_addr` 与请求端口一致 |

    use super::*;

    #[test]
    fn test_traceparent_from_axum_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("traceparent", "test-trace-id".parse().unwrap());
        headers.insert("tracestate", "test-state".parse().unwrap());
        headers.insert("x-request-id", "req-123".parse().unwrap());
        headers.insert(
            "x-pagoda-request-id",
            "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
        );

        let traceparent = TraceParent::from_axum_headers(&headers);
        assert_eq!(traceparent.trace_id, Some("test-trace-id".to_string()));
        assert_eq!(traceparent.tracestate, Some("test-state".to_string()));
        assert_eq!(traceparent.x_request_id, Some("req-123".to_string()));
        assert_eq!(
            traceparent.request_id,
            Some("550e8400-e29b-41d4-a716-446655440000".to_string())
        );
    }

    #[test]
    fn test_shared_http_server_creation() {
        use std::net::{IpAddr, Ipv4Addr};
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
        let token = CancellationToken::new();

        let server = SharedHttpServer::new(bind_addr, token);
        assert!(server.handlers.is_empty());
    }

    #[tokio::test]
    async fn test_bind_and_start_assigns_os_port() {
        use std::net::{IpAddr, Ipv4Addr};
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
        let token = CancellationToken::new();

        let server = SharedHttpServer::new(bind_addr, token.clone());
        let actual_addr = server.clone().bind_and_start().await.unwrap();

        // OS 应该分配一个非 0 端口。
        assert_ne!(actual_addr.port(), 0);

        // `actual_address()` 应该返回真实绑定地址。
        assert_eq!(server.actual_address(), Some(actual_addr));

        // `address()` 应该包含真实端口。
        let addr_str =
            <SharedHttpServer as super::unified_server::RequestPlaneServer>::address(&*server);
        assert!(addr_str.contains(&actual_addr.port().to_string()));

        token.cancel();
    }

    #[tokio::test]
    async fn test_two_servers_get_different_ports() {
        use std::net::{IpAddr, Ipv4Addr};
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);

        let token1 = CancellationToken::new();
        let token2 = CancellationToken::new();

        let server1 = SharedHttpServer::new(addr, token1.clone());
        let server2 = SharedHttpServer::new(addr, token2.clone());

        let actual1 = server1.clone().bind_and_start().await.unwrap();
        let actual2 = server2.clone().bind_and_start().await.unwrap();

        // 两个绑定到 0 端口的服务端必须拿到不同端口。
        assert_ne!(actual1.port(), actual2.port());

        token1.cancel();
        token2.cancel();
    }

    #[tokio::test]
    async fn test_bind_and_start_with_explicit_port() {
        use std::net::{IpAddr, Ipv4Addr};

        // 先绑定到 0 端口以获取一个空闲端口。
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let free_port = listener.local_addr().unwrap().port();
        drop(listener); // 释放这个端口。

        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), free_port);
        let token = CancellationToken::new();

        let server = SharedHttpServer::new(bind_addr, token.clone());
        let actual_addr = server.clone().bind_and_start().await.unwrap();

        // 当绑定到显式端口时，实际端口应该一致。
        assert_eq!(actual_addr.port(), free_port);

        token.cancel();
    }
}
