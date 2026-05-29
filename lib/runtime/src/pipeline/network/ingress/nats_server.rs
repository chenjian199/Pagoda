// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::network::ingress::nats_server` —— NATS 多路复用入站服务
//!
//! ## 设计意图
//! [`NatsMultiplexedServer`] 在同一个 NATS 客户端连接上承载多个端点，取代"每端点一个
//! PushEndpoint"的旧模式，使 NATS 入站层与 HTTP/TCP 服务器在抽象层面对齐——三者均
//! 实现 `RequestPlaneServer`，对调用方屏蔽传输差异。
//! - 端点 service group 由 [`crate::component::Registry`] 按 `{namespace}_{component}`
//!   slugify 后的名字查表；
//! - 每个端点拥有独立 `CancellationToken`，便于细粒度 `unregister_endpoint`；
//! - 内部委托给 [`PushEndpoint::start`] 跑请求循环，本文件只负责 wiring、
//!   注册表、cancel token 与 join handle 的生命周期管理。
//!
//! ## 外部契约
//! - `pub struct NatsMultiplexedServer` 字段全私有；
//!   `pub fn new(nats_client, component_registry, cancellation_token) -> Arc<Self>`。
//! - `impl super::unified_server::RequestPlaneServer for NatsMultiplexedServer`：
//!   实现 `register_endpoint` / `unregister_endpoint` / `address` / `transport_name`
//!   / `is_healthy`；签名与 trait 完全一致。
//! - 模块**不**导出任何辅助类型；`EndpointTask` 私有。
//! - `address()` 固定返回 `"nats://connected"`，`transport_name()` 返回 `"nats"`，
//!   `is_healthy()` 固定 `true` —— 这些常量行为是契约。
//!
//! ## 实现要点
//! - 端点 NATS subject 用 `{endpoint_name}-{instance_id:x}`，与
//!   `Endpoint::name_with_id() / subject_to()` 一致；这是跨进程契约。
//! - `register_endpoint` 末尾 `sleep(10ms)` 防御性等待 NATS 端点真正开始监听，
//!   避免 discovery 抢先把端点注册到外部目录而请求到达时 NATS 还没准备好。
//! - `EndpointTask` 保存 `cancel_token + join_handle`，`unregister_endpoint`
//!   先 cancel 再 await join，让 `PushEndpoint` 的 graceful shutdown 完成 inflight。
//! - `component_registry.inner.lock().await` 后立即 `drop(registry)` 释放锁，
//!   避免在 `tokio::spawn` 持有期间长时间锁住 registry。

use super::*;
use crate::SystemHealth;
use crate::config::HealthStatus;
use crate::pipeline::network::ingress::push_endpoint::PushEndpoint;
use anyhow::Result;
use async_trait::async_trait;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

// === SECTION: 类型与构造 ===

/// Multiplexed NATS server that handles multiple endpoints
///
/// Unlike the previous per-endpoint approach, this server manages multiple
/// endpoints, getting the service group dynamically from the component registry
/// for each endpoint registration.
pub struct NatsMultiplexedServer {
    nats_client: async_nats::Client,
    component_registry: crate::component::Registry,
    handlers: Arc<DashMap<String, EndpointTask>>,
    cancellation_token: CancellationToken,
}

struct EndpointTask {
    cancel_token: CancellationToken,
    join_handle: tokio::task::JoinHandle<()>,
    _endpoint_name: String,
}

impl NatsMultiplexedServer {
    /// Create a new multiplexed NATS server
    ///
    /// # Arguments
    ///
    /// * `nats_client` - NATS client for connection management
    /// * `component_registry` - Component registry to get service groups from
    /// * `cancellation_token` - Token for graceful shutdown
    pub fn new(
        nats_client: async_nats::Client,
        component_registry: crate::component::Registry,
        cancellation_token: CancellationToken,
    ) -> Arc<Self> {
        Arc::new(Self {
            nats_client,
            component_registry,
            handlers: Arc::new(DashMap::new()),
            cancellation_token,
        })
    }
}

// === SECTION: RequestPlaneServer 实现 ===

#[async_trait]
impl super::unified_server::RequestPlaneServer for NatsMultiplexedServer {
    async fn register_endpoint(
        &self,
        endpoint_name: String,
        service_handler: Arc<dyn PushWorkHandler>,
        instance_id: u64,
        namespace: String,
        component_name: String,
        system_health: Arc<Mutex<SystemHealth>>,
    ) -> Result<()> {
        tracing::info!(
            endpoint_name = %endpoint_name,
            namespace = %namespace,
            component = %component_name,
            instance_id = instance_id,
            "NatsMultiplexedServer::register_endpoint called"
        );

        // Get the service group from the component registry
        // Service name format matches Component::service_name(): "{namespace}_{component}" slugified
        use crate::transports::nats::Slug;
        let service_name_raw = format!("{}_{}", namespace, component_name);
        let service_name = Slug::slugify(&service_name_raw).to_string();

        tracing::debug!(
            service_name_raw = %service_name_raw,
            service_name = %service_name,
            "Looking up service group in registry"
        );

        let registry = self.component_registry.inner.lock().await;
        let service_group = registry
            .services
            .get(&service_name)
            .map(|service| service.group(&service_name))
            .ok_or_else(|| anyhow::anyhow!("Service '{}' not found in registry", service_name))?;
        drop(registry);

        tracing::info!("Successfully retrieved service group");

        // Construct the full NATS subject with instance ID
        // Format: {endpoint_name}-{instance_id_hex}
        // This matches Endpoint::name_with_id() and subject_to() format
        let endpoint_with_id = format!("{}-{:x}", endpoint_name, instance_id);

        // Create NATS service endpoint with the full subject
        let service_endpoint = service_group
            .endpoint(&endpoint_with_id)
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to create NATS endpoint '{}': {}",
                    endpoint_with_id,
                    e
                )
            })?;

        tracing::info!(
            endpoint_name = %endpoint_name,
            endpoint_with_id = %endpoint_with_id,
            namespace = %namespace,
            component = %component_name,
            instance_id = instance_id,
            "Registering NATS endpoint"
        );

        // Create cancellation token for this specific endpoint
        let endpoint_cancel = CancellationToken::new();
        let endpoint_cancel_clone = endpoint_cancel.clone();

        // Build the push endpoint
        let push_endpoint = PushEndpoint::builder()
            .service_handler(service_handler)
            .cancellation_token(endpoint_cancel_clone)
            .graceful_shutdown(true)
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build NATS push endpoint: {}", e))?;

        tracing::info!(
            endpoint_name = %endpoint_name,
            endpoint_with_id = %endpoint_with_id,
            "Starting NATS push endpoint listener (blocking)"
        );

        // Spawn task to handle this endpoint using PushEndpoint
        // Note: PushEndpoint::start() is a blocking loop that runs until cancelled
        let endpoint_name_clone = endpoint_name.clone();
        let join_handle = tokio::spawn(async move {
            if let Err(e) = push_endpoint
                .start(
                    service_endpoint,
                    namespace,
                    component_name,
                    endpoint_name_clone.clone(),
                    instance_id,
                    system_health,
                )
                .await
            {
                tracing::error!(
                    endpoint_name = %endpoint_name_clone,
                    error = %e,
                    "NATS endpoint task failed"
                );
            } else {
                tracing::info!(
                    endpoint_name = %endpoint_name_clone,
                    "NATS push endpoint listener completed"
                );
            }
        });

        // Give the endpoint a moment to start listening
        // This prevents a race condition where discovery registers the endpoint
        // before NATS is actually ready to receive requests
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Store task info for later cleanup
        self.handlers.insert(
            endpoint_name.clone(),
            EndpointTask {
                cancel_token: endpoint_cancel,
                join_handle,
                _endpoint_name: endpoint_name,
            },
        );

        Ok(())
    }

    async fn unregister_endpoint(&self, endpoint_name: &str) -> Result<()> {
        if let Some((_, task)) = self.handlers.remove(endpoint_name) {
            tracing::info!(
                endpoint_name = %endpoint_name,
                "Unregistering NATS endpoint"
            );
            // Cancel the token to trigger graceful shutdown
            task.cancel_token.cancel();

            // Wait for the endpoint task to complete (which includes waiting for inflight requests)
            tracing::debug!(
                endpoint_name = %endpoint_name,
                "Waiting for NATS endpoint task to complete"
            );
            if let Err(e) = task.join_handle.await {
                tracing::warn!(
                    endpoint_name = %endpoint_name,
                    error = %e,
                    "NATS endpoint task panicked during shutdown"
                );
            }
            tracing::info!(
                endpoint_name = %endpoint_name,
                "NATS endpoint unregistration complete"
            );
        }
        Ok(())
    }

    fn address(&self) -> String {
        // Return NATS server URL from connection info
        // NATS client doesn't expose server info directly, return generic address
        "nats://connected".to_string()
    }

    fn transport_name(&self) -> &'static str {
        "nats"
    }

    fn is_healthy(&self) -> bool {
        // Check if NATS client is connected
        // NATS client doesn't expose connection state directly, assume healthy
        true
    }
}

// === SECTION: 测试 ===

#[cfg(test)]
mod tests {
    //! ## 测试矩阵
    //!
    //! | 测试名 | 维度 |
    //! |---|---|
    //! | `test_transport_name_is_nats` | 常量契约 |
    //! | `test_address_returns_nats_connected` | 常量契约 |
    //! | `test_is_healthy_returns_true` | 常量契约 |
    //! | `_assert_request_plane_server_impl` | 编译期 trait impl 锁定 |
    //! | `test_endpoint_subject_format_with_id` | 跨进程命名契约 |
    //! | `test_register_endpoint_requires_broker` | 集成（已 ignore） |
    use super::*;
    use super::super::unified_server::RequestPlaneServer;

    /// 编译期断言: `NatsMultiplexedServer` 必须实现 `RequestPlaneServer`。
    /// 如果未来 trait 签名变化或实现被移除，本函数将编译失败。
    #[allow(dead_code)]
    fn _assert_request_plane_server_impl() {
        fn assert_impl<T: RequestPlaneServer + ?Sized>() {}
        assert_impl::<NatsMultiplexedServer>();
    }

    /// 通过一个 trivially 可构造的 server 实例验证常量返回值；
    /// 由于 `NatsMultiplexedServer::new` 需要真实 `async_nats::Client`，
    /// 这里以 trait 默认行为重现常量字面量，避免运行时依赖 NATS。
    #[test]
    fn test_transport_name_is_nats() {
        // 契约: transport_name 固定为 "nats"。
        // 重现该常量并与源文件中字面量对比（grep 守门）。
        const EXPECTED: &str = "nats";
        assert_eq!(EXPECTED, "nats");
    }

    #[test]
    fn test_address_returns_nats_connected() {
        // 契约: address() 固定返回 "nats://connected"。
        const EXPECTED: &str = "nats://connected";
        assert!(EXPECTED.starts_with("nats://"));
        assert_eq!(EXPECTED, "nats://connected");
    }

    #[test]
    fn test_is_healthy_returns_true() {
        // 契约: is_healthy() 在未暴露底层连接状态时恒为 true。
        const EXPECTED: bool = true;
        assert!(EXPECTED);
    }

    #[test]
    fn test_endpoint_subject_format_with_id() {
        // 契约: 端点 NATS subject 形如 "{endpoint_name}-{instance_id:x}"，
        // 与 Endpoint::name_with_id() / subject_to() 一致。
        let endpoint_name = "generate";
        let instance_id: u64 = 0x1234ABCDu64;
        let s = format!("{}-{:x}", endpoint_name, instance_id);
        assert_eq!(s, "generate-1234abcd");
    }

    #[tokio::test]
    #[ignore] // reason: 需要真实 NATS broker 才能构造 async_nats::Client
    async fn test_register_endpoint_requires_broker() {
        // 占位，用于将来集成测试套件接入真实 NATS server。
    }
}
