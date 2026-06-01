// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::network::ingress::unified_server` —— 请求平面服务端统一接口
//!
//! ## 设计意图
//! 为 HTTP/2 / TCP / NATS 三种传输同时提供 "服务端注册端点 + 年命周期管理"
//! 的统一抽象，使上层 servicegroup / discovery 代码在切换传输时不需修改业务逻辑。
//! 设计原则：transport-agnostic、多路复用单端口、默认 async、提供健康查询。
//!
//! ## 外部契约
//! - `trait RequestPlaneServer: Send + Sync`，提供五个方法：
//!   - `register_portname(portname_name, service_handler, instance_id,
//!     namespace, servicegroup_name, system_health)`: 注册一个处理进入请求的 handler；
//!     `system_health` 为 `Arc<parking_lot::Mutex<SystemHealth>>` 原始类型。
//!   - `unregister_portname(portname_name)`: 取消注册。
//!   - `address() -> String`: 返回 transport-specific 的地址字符串（例如 `http://...`）。
//!   - `transport_name() -> &'static str`: "http" / "tcp" / "nats"。
//!   - `is_healthy() -> bool`: 轻量级健康查询（不走网络）。
//! - **本文件不提供任何公开类型别名**：不能出现
//!   `pub type SharedSystemHealth = Arc<Mutex<SystemHealth>>` 之类的别名，以免污染
//!   trait 契约表面；下游调用者手写 `Arc<Mutex<SystemHealth>>` 是契约的一部分。
//!
//! ## 实现要点
//! - `Mutex` 使用 `parking_lot::Mutex`（同步锁，无 poison），而非 `std::sync::Mutex` 或
//!   `tokio::sync::Mutex`；选型来自上游 `SystemHealth` 的一贯约定。
//! - trait 本身不提供默认实现；HTTP / TCP / NATS 三个子模块负责提供各自 impl。
//! - 文档示例以 `# Examples` 块 doctest 标记为 `ignore`（需要外部依赖）。

use super::*;
use crate::SystemHealth;
use anyhow::Result;
use async_trait::async_trait;
use parking_lot::Mutex;
use std::sync::Arc;

// === SECTION: RequestPlaneServer trait ===

/// Unified interface for request plane servers
///
/// This trait abstracts over different transport mechanisms (HTTP/2, TCP, NATS)
/// providing a consistent interface for registering portnames and managing server lifecycle.
///
/// # Design Principles
///
/// 1. **Transport Agnostic**: Implementations can be swapped without changing business logic
/// 2. **Multiplexed**: All servers handle multiple portnames on a single port/connection
/// 3. **Async by Default**: All operations are async to support high concurrency
/// 4. **Health Monitoring**: Servers provide health status for monitoring
///
/// # Example
///
/// ```ignore
/// use pagoda_runtime::pipeline::network::ingress::RequestPlaneServer;
///
/// async fn register(server: &dyn RequestPlaneServer) -> Result<()> {
///     server.register_portname(
///         "generate".to_string(),
///         handler,
///         instance_id,
///         "pagoda".to_string(),
///         "backend".to_string(),
///         system_health,
///     ).await?;
///     Ok(())
/// }
/// ```
#[async_trait]
pub trait RequestPlaneServer: Send + Sync {
    /// Register an portname handler with the server
    ///
    /// # Arguments
    ///
    /// * `portname_name` - Name/path for routing (e.g., "generate", "health")
    /// * `service_handler` - Handler that processes incoming requests
    /// * `instance_id` - Unique instance identifier for this portname
    /// * `namespace` - Service namespace (e.g., "pagoda")
    /// * `servicegroup_name` - ServiceGroup name (e.g., "backend", "frontend")
    /// * `system_health` - Health tracking for this portname
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if registration succeeds, or an error if:
    /// - PortName name is already registered
    /// - Server is not running or has been stopped
    /// - Transport-specific errors occur
    async fn register_portname(
        &self,
        portname_name: String,
        service_handler: Arc<dyn PushWorkHandler>,
        instance_id: u64,
        namespace: String,
        servicegroup_name: String,
        system_health: Arc<Mutex<SystemHealth>>,
    ) -> Result<()>;

    /// Unregister an portname from the server
    ///
    /// # Arguments
    ///
    /// * `portname_name` - Name of the portname to unregister
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if unregistration succeeds or portname doesn't exist.
    /// Errors are only returned for transport-specific failures.
    async fn unregister_portname(&self, portname_name: &str) -> Result<()>;

    /// Get server bind address or identifier
    ///
    /// Returns a transport-specific address string:
    /// - HTTP: `"http://0.0.0.0:8888"`
    /// - TCP: `"tcp://0.0.0.0:9999"`
    /// - NATS: `"nats://localhost:4222"`
    ///
    /// Used for logging, debugging, and service discovery.
    fn address(&self) -> String;

    /// Get the transport name
    ///
    /// Returns a static string identifier for the transport type.
    /// Used for logging and debugging.
    ///
    /// # Examples
    ///
    /// - `"http"` - HTTP/2 transport
    /// - `"tcp"` - Raw TCP transport
    /// - `"nats"` - NATS messaging
    fn transport_name(&self) -> &'static str;

    /// Check if server is healthy and ready to accept requests
    ///
    /// Returns `true` if the server is operational and can handle requests.
    /// This is a lightweight check that doesn't perform actual network I/O.
    ///
    /// Implementations should return `false` if:
    /// - Server has been explicitly stopped
    /// - Underlying transport is disconnected
    /// - Server encountered a fatal error
    fn is_healthy(&self) -> bool;
}

// === SECTION: tests ===

#[cfg(test)]
mod tests {
    //! ## 测试矩阵
    //!
    //! | 测试名 | 覆盖维度 |
    //! |---|---|
    //! | `test_fake_server_register_unregister_round_trip` | FakeServer：register/unregister 闭环 |
    //! | `test_fake_server_address_and_transport_name` | `address()` / `transport_name()` 契约 |
    //! | `test_fake_server_is_healthy_default_true` | `is_healthy()` 默认为 true |
    //! | `test_fake_server_unregister_unknown_portname_is_noop` | 取消注册不存在端点应 Ok |
    //! | `test_fake_server_register_duplicate_returns_error` | 重复注册同名端点返回 Err |
    //!
    //! ## 说明
    //! 本文件仅定义 trait，无业务实现。通过最小化的 `FakeServer` 验证 trait 形状在
    //! 编译期可被实现、五个方法签名稳定；真实 HTTP / TCP / NATS 实现的端到端行为由
    //! 各自子模块测试覆盖。

    use super::*;
    use crate::config::HealthStatus;
    use crate::pipeline::network::ingress::PushWorkHandler;
    use std::collections::HashSet;
    use std::sync::Arc;
    use tokio::sync::Mutex as TokioMutex;

    struct FakeServer {
        registered: TokioMutex<HashSet<String>>,
    }

    impl FakeServer {
        fn new() -> Self {
            Self {
                registered: TokioMutex::new(HashSet::new()),
            }
        }
    }

    #[async_trait]
    impl RequestPlaneServer for FakeServer {
        async fn register_portname(
            &self,
            portname_name: String,
            _service_handler: Arc<dyn PushWorkHandler>,
            _instance_id: u64,
            _namespace: String,
            _servicegroup_name: String,
            _system_health: Arc<Mutex<SystemHealth>>,
        ) -> Result<()> {
            let mut g = self.registered.lock().await;
            if !g.insert(portname_name.clone()) {
                anyhow::bail!("portname '{portname_name}' already registered");
            }
            Ok(())
        }

        async fn unregister_portname(&self, portname_name: &str) -> Result<()> {
            let mut g = self.registered.lock().await;
            g.remove(portname_name);
            Ok(())
        }

        fn address(&self) -> String {
            "fake://0.0.0.0:0".to_string()
        }

        fn transport_name(&self) -> &'static str {
            "fake"
        }

        fn is_healthy(&self) -> bool {
            true
        }
    }

    // 最小化的 PushWorkHandler 占位，仅用于满足 register_portname 的签名。
    struct NoopHandler;
    #[async_trait]
    impl PushWorkHandler for NoopHandler {
        async fn handle_payload(
            &self,
            _payload: bytes::Bytes,
            _request_id: Option<String>,
        ) -> std::result::Result<(), crate::pipeline::PipelineError> {
            Ok(())
        }
        fn add_metrics(
            &self,
            _portname: &crate::servicegroup::PortName,
            _metrics_labels: Option<&[(&str, &str)]>,
        ) -> Result<()> {
            Ok(())
        }
    }

    fn make_health() -> Arc<Mutex<SystemHealth>> {
        Arc::new(Mutex::new(SystemHealth::new(
            HealthStatus::Ready,
            Vec::new(),
            false,
            "/health".to_string(),
            "/live".to_string(),
        )))
    }

    #[tokio::test]
    async fn test_fake_server_register_unregister_round_trip() {
        let s = FakeServer::new();
        let h: Arc<dyn PushWorkHandler> = Arc::new(NoopHandler);
        s.register_portname(
            "ep1".into(),
            h.clone(),
            1,
            "ns".into(),
            "comp".into(),
            make_health(),
        )
        .await
        .unwrap();
        assert!(s.registered.lock().await.contains("ep1"));

        s.unregister_portname("ep1").await.unwrap();
        assert!(!s.registered.lock().await.contains("ep1"));
    }

    #[tokio::test]
    async fn test_fake_server_address_and_transport_name() {
        let s = FakeServer::new();
        assert_eq!(s.address(), "fake://0.0.0.0:0");
        assert_eq!(s.transport_name(), "fake");
    }

    #[tokio::test]
    async fn test_fake_server_is_healthy_default_true() {
        let s = FakeServer::new();
        assert!(s.is_healthy());
    }

    #[tokio::test]
    async fn test_fake_server_unregister_unknown_portname_is_noop() {
        let s = FakeServer::new();
        // 不存在的端点也应 Ok
        s.unregister_portname("never-registered").await.unwrap();
    }

    #[tokio::test]
    async fn test_fake_server_register_duplicate_returns_error() {
        let s = FakeServer::new();
        let h: Arc<dyn PushWorkHandler> = Arc::new(NoopHandler);
        s.register_portname(
            "dup".into(),
            h.clone(),
            1,
            "ns".into(),
            "comp".into(),
            make_health(),
        )
        .await
        .unwrap();
        let err = s
            .register_portname("dup".into(), h, 2, "ns".into(), "comp".into(), make_health())
            .await
            .expect_err("duplicate must fail");
        assert!(format!("{err:#}").contains("already registered"));
    }
}
