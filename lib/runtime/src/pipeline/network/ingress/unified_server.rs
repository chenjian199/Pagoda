// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::network::ingress::unified_server` —— 请求平面服务端统一接口
//!
//! ## 设计意图
//! 为 HTTP/2 / TCP / NATS 三种传输同时提供“服务端注册端点 + 生命周期管理”的统一抽象，
//! 让上层 servicegroup / discovery 代码在切换传输时不需要修改业务逻辑。
//! 设计原则：与传输无关、单端口多路复用、默认异步、提供健康查询。
//!
//! ## 外部契约
//! - `trait RequestPlaneServer: Send + Sync`，提供五个方法：
//!   - `register_portname(portname_name, service_handler, instance_id,
//!     namespace, servicegroup_name, system_health)`: 注册一个处理入站请求的 handler；
//!     `system_health` 的原始类型是 `Arc<parking_lot::Mutex<SystemHealth>>`。
//!   - `unregister_portname(portname_name)`: 取消注册。
//!   - `address() -> String`: 返回特定于传输的地址字符串（例如 `http://...`）。
//!   - `transport_name() -> &'static str`: "http" / "tcp" / "nats"。
//!   - `is_healthy() -> bool`: 轻量级健康查询（不走网络）。
//! - **本文件不提供任何公开类型别名**：不能出现
//!   `pub type SharedSystemHealth = Arc<Mutex<SystemHealth>>` 之类的别名，以免污染
//!   trait 契约表面；下游调用方手写 `Arc<Mutex<SystemHealth>>` 本身就是契约的一部分。
//!
//! ## 实现要点
//! - `Mutex` 使用 `parking_lot::Mutex`（同步锁、无 poison），而不是 `std::sync::Mutex` 或
//!   `tokio::sync::Mutex`；这个选型来自上游 `SystemHealth` 的一贯约定。
//! - trait 本身不提供默认实现；HTTP / TCP / NATS 三个子模块分别负责自己的 impl。
//! - 文档示例使用 `# Examples` 块并标记为 `ignore`（因为需要外部依赖）。

use super::*;
use crate::SystemHealth;
use anyhow::Result;
use async_trait::async_trait;
use parking_lot::Mutex;
use std::sync::Arc;

// === SECTION: RequestPlaneServer trait ===

/// request plane 服务端的统一接口。
///
/// 这个 trait 抽象了不同的传输机制（HTTP/2、TCP、NATS），
/// 为注册 portname 和管理服务端生命周期提供一致接口。
///
/// # 设计原则
///
/// 1. **与传输无关**：实现可以替换，而不会改变业务逻辑。
/// 2. **多路复用**：所有服务端都在单个端口/连接上处理多个 portname。
/// 3. **默认异步**：所有操作都是 async，以支持高并发。
/// 4. **健康监控**：服务端提供健康状态供监控使用。
///
/// # 示例
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
    /// 向服务端注册一个 portname 处理器。
    ///
    /// # 参数
    ///
    /// * `portname_name` - 路由名称/路径（例如 `generate`、`health`）
    /// * `service_handler` - 处理入站请求的 handler
    /// * `instance_id` - 该 portname 的唯一实例标识
    /// * `namespace` - 服务命名空间（例如 `pagoda`）
    /// * `servicegroup_name` - ServiceGroup 名称（例如 `backend`、`frontend`）
    /// * `system_health` - 该 portname 的健康状态跟踪器
    ///
    /// # 返回值
    ///
    /// 注册成功时返回 `Ok(())`；否则可能返回错误，常见情况有：
    /// - PortName 名称已经注册过；
    /// - 服务端未运行或已停止；
    /// - 出现了传输相关错误。
    async fn register_portname(
        &self,
        portname_name: String,
        service_handler: Arc<dyn PushWorkHandler>,
        instance_id: u64,
        namespace: String,
        servicegroup_name: String,
        system_health: Arc<Mutex<SystemHealth>>,
    ) -> Result<()>;

    /// 从服务端注销一个 portname。
    ///
    /// # 参数
    ///
    /// * `portname_name` - 要注销的 portname 名称
    ///
    /// # 返回值
    ///
    /// 注销成功或 portname 不存在时返回 `Ok(())`。
    /// 只有传输相关失败才会返回错误。
    async fn unregister_portname(&self, portname_name: &str) -> Result<()>;

    /// 获取服务端绑定地址或标识符。
    ///
    /// 返回一个特定于传输的地址字符串：
    /// - HTTP: `"http://0.0.0.0:8888"`
    /// - TCP: `"tcp://0.0.0.0:9999"`
    /// - NATS: `"nats://localhost:4222"`
    ///
    /// 用于日志、调试和服务发现。
    fn address(&self) -> String;

    /// 获取传输名称。
    ///
    /// 返回该传输类型的静态字符串标识符。
    /// 用于日志和调试。
    ///
    /// # Examples
    ///
    /// - `"http"` - HTTP/2 transport
    /// - `"tcp"` - 原生 TCP 传输
    /// - `"nats"` - NATS messaging
    fn transport_name(&self) -> &'static str;

    /// 检查服务端是否健康并准备好接收请求。
    ///
    /// 如果服务端可用且能够处理请求，则返回 `true`。
    /// 这是一个不会执行真实网络 I/O 的轻量级检查。
    ///
    /// 如果出现以下情况，实现应返回 `false`：
    /// - 服务端已被显式停止；
    /// - 底层传输已断开；
    /// - 服务端遇到致命错误。
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
    //! 本文件只定义 trait，不包含业务实现。通过最小化的 `FakeServer` 验证 trait 形状
    //! 能在编译期被实现、五个方法签名保持稳定；真实 HTTP / TCP / NATS 实现的端到端行为
    //! 由各自子模块测试覆盖。

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

    // 最小化的 PushWorkHandler 占位，只用于满足 `register_portname` 的签名。
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
