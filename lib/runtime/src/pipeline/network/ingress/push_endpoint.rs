// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::network::ingress::push_endpoint` —— NATS Push 服务端点单体启动器
//!
//! ## 设计意图
//! [`PushEndpoint`] 把一个 `async_nats::service::endpoint::Endpoint` 的流式请求源
//! 接到 [`PushWorkHandler::handle_payload`] 上：
//! - 主循环通过 `tokio::select!` 在“下一条请求”和“取消令牌”之间竞争；
//! - 每条请求都会 fire-and-forget 地 `tokio::spawn` 进入 handler，主循环立刻回到 `next()`，
//!   从而实现单端点内部的全并发服务；
//! - `AtomicU64` 维护 in-flight 计数，`Notify` 在每条任务完成时 `notify_one`，
//!   当 `graceful_shutdown=true` 时，主循环会在取消后阻塞等待 in-flight 归零。
//! - 与 `SystemHealth` 联动：启动时调用 `set_portname_registered`，退出时调用
//!   `set_portname_health_status(NotReady)`。
//!
//! ## 外部契约
//! - `pub struct PushEndpoint` 的字段全部是 `pub`：`service_handler`、`cancellation_token`、
//!   `graceful_shutdown: bool`（`#[builder(default = "true")]`）。
//! - `pub const VERSION = env!("CARGO_PKG_VERSION")`。
//! - `pub fn builder() -> PushEndpointBuilder`（由 `derive_builder` 生成的 builder 类型
//!   `PushEndpointBuilder` 同样 pub）。
//! - `pub async fn start(self, portname, namespace, servicegroup_name, portname_name,
//!   instance_id, system_health)` —— 形参签名与顺序本身就是契约。
//! - **不**提供 `new` / `portname_name` / `request_span` / `enter` / `leave` /
//!   `count` / `wait_until_empty` / `stop_portname` / `mark_*` / `acknowledge_request`
//!   这类从私有实现中抽出来的辅助方法；in-flight、Notify、span 逻辑都内联在 `start` 中。
//!
//! ## 实现要点
//! - `parking_lot::Mutex` 用于 `SystemHealth` 锁；它和 `std::sync::Mutex` 的 API 不同
//!   （`lock()` 不返回 `Result`），这是契约的一部分，不能换成 std。
//! - `Arc<String>`（不是 `Arc<str>`）的三件套 `servicegroup_name_local / portname_name_local
//!   / namespace_local` 先装箱，避免每次 spawn 都克隆字符串。
//! - request-id 优先读取 `request-id`，再回退到 `x-pagoda-request-id`，然后传给 handler。
//! - `req.respond(Ok("".into()))` 是 NATS 服务的即时 ACK：它必须紧跟在 `next()` 之后、
//!   且在 spawn 之前，这样客户端才能尽快释放半同步等待。

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

/// crate 版本。
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
        servicegroup_name: String,
        portname_name: String,
        instance_id: u64,
        system_health: Arc<Mutex<SystemHealth>>,
    ) -> Result<()> {
        let mut endpoint = endpoint;

        let inflight = Arc::new(AtomicU64::new(0));
        let notify = Arc::new(Notify::new());
        let servicegroup_name_local: Arc<String> = Arc::from(servicegroup_name);
        let portname_name_local: Arc<String> = Arc::from(portname_name);
        let namespace_local: Arc<String> = Arc::from(namespace);

        system_health
            .lock()
            .set_portname_registered(portname_name_local.as_str());

        loop {
            let req = tokio::select! {
                biased;

                // 等待服务请求。
                req = endpoint.next() => {
                    req
                }

                // 处理关闭流程。
                _ = self.cancellation_token.cancelled() => {
                    tracing::info!("PushEndpoint 收到取消信号，准备关闭服务。");
                    if let Err(e) = endpoint.stop().await {
                        tracing::warn!("停止 NATS 服务失败：{:?}", e);
                    }
                    break;
                }
            };

            if let Some(req) = req {
                let response = "".to_string();
                if let Err(e) = req.respond(Ok(response.into())).await {
                    tracing::warn!(
                        "响应请求失败；这可能表示请求已经关闭：{:?}",
                        e
                    );
                }

                let ingress = self.service_handler.clone();
                let portname_name: Arc<String> = Arc::clone(&portname_name_local);
                let servicegroup_name: Arc<String> = Arc::clone(&servicegroup_name_local);
                let namespace: Arc<String> = Arc::clone(&namespace_local);

                // 增加 in-flight 计数。
                inflight.fetch_add(1, Ordering::SeqCst);
                let inflight_clone = inflight.clone();
                let notify_clone = notify.clone();

                // 在这里处理头部以便打 tracing。
                let span = if let Some(headers) = req.message.headers.as_ref() {
                    make_handle_payload_span(
                        headers,
                        servicegroup_name.as_ref(),
                        portname_name.as_ref(),
                        namespace.as_ref(),
                        instance_id,
                    )
                } else {
                    tracing::info_span!(target: "request_span", "handle_payload")
                };

                // 在传递 payload 之前，从头部提取 request_id。
                let request_id = req
                    .message
                    .headers
                    .as_ref()
                    .and_then(|h| h.get("request-id").map(|v| v.to_string()))
                    .or_else(|| {
                        req.message
                            .headers
                            .as_ref()
                            .and_then(|h| h.get("x-pagoda-request-id").map(|v| v.to_string()))
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

                    // 减少 in-flight 计数。
                    inflight_clone.fetch_sub(1, Ordering::SeqCst);
                    notify_clone.notify_one();
                });
            } else {
                break;
            }
        }

        system_health
            .lock()
            .set_portname_health_status(portname_name_local.as_str(), HealthStatus::NotReady);

        // 如果启用优雅关闭，就等待所有 in-flight 请求完成。
        if self.graceful_shutdown {
            let inflight_count = inflight.load(Ordering::SeqCst);
            if inflight_count > 0 {
                tracing::info!(
                    portname_name = portname_name_local.as_str(),
                    inflight_count = inflight_count,
                    "等待 in-flight NATS 请求完成"
                );
                while inflight.load(Ordering::SeqCst) > 0 {
                    notify.notified().await;
                }
                tracing::info!(
                    portname_name = portname_name_local.as_str(),
                    "所有 in-flight NATS 请求已完成"
                );
            }
        } else {
            tracing::info!(
                portname_name = portname_name_local.as_str(),
                "跳过优雅关闭，不等待 in-flight 请求"
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

    /// 仅用于满足 trait 约束的最小 handler；测试中不会真正运行。
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
            _portname: &crate::servicegroup::PortName,
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
