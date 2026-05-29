// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::network::ingress::push_endpoint` —— NATS Push 服务端点单体启动器
//!
//! ## 设计意图
//! [`PushEndpoint`] 把一个 `async_nats::service::endpoint::Endpoint` 流式请求源
//! 套接到 [`PushWorkHandler::handle_payload`] 上：
//! - 主循环 `tokio::select!` 在"下一条请求"与"取消令牌"之间竞争；
//! - 每条请求 fire-and-forget 地 `tokio::spawn` 进入 handler，主循环立刻回到 `next()`，
//!   实现单端点内部的全并发服务；
//! - `AtomicU64` 维护 inflight 计数，`Notify` 在每条任务完成时 `notify_one`，
//!   `graceful_shutdown=true` 时主循环在取消后会阻塞等待 inflight 归零。
//! - 与 `SystemHealth` 联动：启动时 `set_endpoint_registered`、退出时
//!   `set_endpoint_health_status(NotReady)`。
//!
//! ## 外部契约
//! - `pub struct PushEndpoint` 字段全部 `pub`：`service_handler`、`cancellation_token`、
//!   `graceful_shutdown: bool`（`#[builder(default = "true")]`）。
//! - `pub const VERSION = env!("CARGO_PKG_VERSION")`。
//! - `pub fn builder() -> PushEndpointBuilder`（由 `derive_builder` 生成的 builder 类型
//!   `PushEndpointBuilder` 同样 pub）。
//! - `pub async fn start(self, endpoint, namespace, component_name, endpoint_name,
//!   instance_id, system_health)` —— 形参签名与顺序为契约。
//! - **不**提供 `new` / `endpoint_name` / `request_span` / `enter` / `leave` /
//!   `count` / `wait_until_empty` / `stop_endpoint` / `mark_*` / `acknowledge_request`
//!   等私有抽取出来的辅助方法；inflight、Notify、span 逻辑全部内联在 `start` 中。
//!
//! ## 实现要点
//! - `parking_lot::Mutex` 用于 `SystemHealth` 锁；与 `std::sync::Mutex` 不同 API
//!   (`lock()` 不返回 `Result`)，这是契约的一部分（不可换为 std）。
//! - `Arc<String>`（不是 `Arc<str>`）三件套 `component_name_local / endpoint_name_local
//!   / namespace_local` 提前装箱，避免每次 spawn clone 字符串。
//! - request-id 优先从 `request-id`、回退到 `x-dynamo-request-id`，再传入 handler。
//! - `req.respond(Ok("".into()))` 是 NATS 服务即时 ACK：必须紧跟在 `next()` 之后、
//!   在 spawn 之前，让客户端尽快释放半同步等待。

use std::sync::atomic::{AtomicU64, Ordering};

use super::*;
use crate::SystemHealth;
use crate::config::HealthStatus;
use crate::logging::make_handle_payload_span;
use crate::protocols::LeaseId;
use anyhow::Result;
use async_nats::service::endpoint::Endpoint;
use derive_builder::Builder;
use parking_lot::Mutex;
use std::collections::HashMap;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

// === SECTION: PushEndpoint 结构与 builder ===

#[derive(Builder)]
pub struct PushEndpoint {
    pub service_handler: Arc<dyn PushWorkHandler>,
    pub cancellation_token: CancellationToken,
    #[builder(default = "true")]
    pub graceful_shutdown: bool,
}

/// version of crate
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

// === SECTION: start 主循环 ===

impl PushEndpoint {
    pub fn builder() -> PushEndpointBuilder {
        PushEndpointBuilder::default()
    }

    pub async fn start(
        self,
        endpoint: Endpoint,
        namespace: String,
        component_name: String,
        endpoint_name: String,
        instance_id: u64,
        system_health: Arc<Mutex<SystemHealth>>,
    ) -> Result<()> {
        let mut endpoint = endpoint;

        let inflight = Arc::new(AtomicU64::new(0));
        let notify = Arc::new(Notify::new());
        let component_name_local: Arc<String> = Arc::from(component_name);
        let endpoint_name_local: Arc<String> = Arc::from(endpoint_name);
        let namespace_local: Arc<String> = Arc::from(namespace);

        system_health
            .lock()
            .set_endpoint_registered(endpoint_name_local.as_str());

        loop {
            let req = tokio::select! {
                biased;

                // await on service request
                req = endpoint.next() => {
                    req
                }

                // process shutdown
                _ = self.cancellation_token.cancelled() => {
                    tracing::info!("PushEndpoint received cancellation signal, shutting down service");
                    if let Err(e) = endpoint.stop().await {
                        tracing::warn!("Failed to stop NATS service: {:?}", e);
                    }
                    break;
                }
            };

            if let Some(req) = req {
                let response = "".to_string();
                if let Err(e) = req.respond(Ok(response.into())).await {
                    tracing::warn!(
                        "Failed to respond to request; this may indicate the request has shutdown: {:?}",
                        e
                    );
                }

                let ingress = self.service_handler.clone();
                let endpoint_name: Arc<String> = Arc::clone(&endpoint_name_local);
                let component_name: Arc<String> = Arc::clone(&component_name_local);
                let namespace: Arc<String> = Arc::clone(&namespace_local);

                // increment the inflight counter
                inflight.fetch_add(1, Ordering::SeqCst);
                let inflight_clone = inflight.clone();
                let notify_clone = notify.clone();

                // Handle headers here for tracing
                let span = if let Some(headers) = req.message.headers.as_ref() {
                    make_handle_payload_span(
                        headers,
                        component_name.as_ref(),
                        endpoint_name.as_ref(),
                        namespace.as_ref(),
                        instance_id,
                    )
                } else {
                    tracing::info_span!(target: "request_span", "handle_payload")
                };

                // Extract request_id from headers before passing payload
                let request_id = req
                    .message
                    .headers
                    .as_ref()
                    .and_then(|h| h.get("request-id").map(|v| v.to_string()))
                    .or_else(|| {
                        req.message
                            .headers
                            .as_ref()
                            .and_then(|h| h.get("x-dynamo-request-id").map(|v| v.to_string()))
                    });

                tokio::spawn(async move {
                    tracing::trace!(instance_id, "handling new request");
                    let result = ingress
                        .handle_payload(req.message.payload, request_id)
                        .instrument(span)
                        .await;
                    match result {
                        Ok(_) => {
                            tracing::trace!(instance_id, "request handled successfully");
                        }
                        Err(e) => {
                            tracing::warn!("Failed to handle request: {}", e.to_string());
                        }
                    }

                    // decrease the inflight counter
                    inflight_clone.fetch_sub(1, Ordering::SeqCst);
                    notify_clone.notify_one();
                });
            } else {
                break;
            }
        }

        system_health
            .lock()
            .set_endpoint_health_status(endpoint_name_local.as_str(), HealthStatus::NotReady);

        // await for all inflight requests to complete if graceful shutdown
        if self.graceful_shutdown {
            let inflight_count = inflight.load(Ordering::SeqCst);
            if inflight_count > 0 {
                tracing::info!(
                    endpoint_name = endpoint_name_local.as_str(),
                    inflight_count = inflight_count,
                    "Waiting for inflight NATS requests to complete"
                );
                while inflight.load(Ordering::SeqCst) > 0 {
                    notify.notified().await;
                }
                tracing::info!(
                    endpoint_name = endpoint_name_local.as_str(),
                    "All inflight NATS requests completed"
                );
            }
        } else {
            tracing::info!(
                endpoint_name = endpoint_name_local.as_str(),
                "Skipping graceful shutdown, not waiting for inflight requests"
            );
        }

        Ok(())
    }
}

// === SECTION: 测试 ===

#[cfg(test)]
mod tests {
    //! ## 测试矩阵
    //!
    //! | 测试名 | 维度 |
    //! |---|---|
    //! | `test_version_const_matches_cargo_pkg_version` | 公开常量 |
    //! | `test_builder_defaults_graceful_shutdown_true` | builder 默认值 |
    //! | `test_builder_explicit_graceful_shutdown_false` | builder 覆盖 |
    //! | `test_builder_missing_handler_returns_error` | builder 校验缺字段 |
    //! | `test_builder_missing_cancel_token_returns_error` | builder 校验缺字段 |
    //! | `test_pushwork_handler_trait_object_compiles` | 编译期 trait object |
    use super::*;
    use crate::pipeline::PipelineError;
    use async_trait::async_trait;
    use bytes::Bytes;

    /// 仅满足 trait 的最小 handler；不在测试中真正运行。
    struct NoopHandler;

    #[async_trait]
    impl PushWorkHandler for NoopHandler {
        async fn handle_payload(
            &self,
            _payload: Bytes,
            _request_id: Option<String>,
        ) -> std::result::Result<(), PipelineError> {
            Ok(())
        }
        fn add_metrics(
            &self,
            _endpoint: &crate::component::Endpoint,
            _metrics_labels: Option<&[(&str, &str)]>,
        ) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_version_const_matches_cargo_pkg_version() {
        assert!(!VERSION.is_empty(), "VERSION must not be empty");
        assert_eq!(VERSION, env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn test_builder_defaults_graceful_shutdown_true() {
        let h: Arc<dyn PushWorkHandler> = Arc::new(NoopHandler);
        let pe = PushEndpoint::builder()
            .service_handler(h)
            .cancellation_token(CancellationToken::new())
            .build()
            .expect("builder should succeed with required fields");
        assert!(
            pe.graceful_shutdown,
            "graceful_shutdown 默认值必须为 true"
        );
    }

    #[test]
    fn test_builder_explicit_graceful_shutdown_false() {
        let h: Arc<dyn PushWorkHandler> = Arc::new(NoopHandler);
        let pe = PushEndpoint::builder()
            .service_handler(h)
            .cancellation_token(CancellationToken::new())
            .graceful_shutdown(false)
            .build()
            .expect("builder should succeed");
        assert!(!pe.graceful_shutdown);
    }

    #[test]
    fn test_builder_missing_handler_returns_error() {
        let r = PushEndpoint::builder()
            .cancellation_token(CancellationToken::new())
            .build();
        assert!(r.is_err(), "缺 service_handler 必须返回 Err");
    }

    #[test]
    fn test_builder_missing_cancel_token_returns_error() {
        let h: Arc<dyn PushWorkHandler> = Arc::new(NoopHandler);
        let r = PushEndpoint::builder().service_handler(h).build();
        assert!(r.is_err(), "缺 cancellation_token 必须返回 Err");
    }

    #[test]
    fn test_pushwork_handler_trait_object_compiles() {
        // 编译期断言: NoopHandler 满足 PushWorkHandler，且可装入 Arc<dyn ...>。
        fn _assert_trait_object<T: PushWorkHandler + ?Sized>() {}
        _assert_trait_object::<dyn PushWorkHandler>();
        let _h: Arc<dyn PushWorkHandler> = Arc::new(NoopHandler);
    }
}
