// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::network::egress::nats_client` —— NATS 请求平面客户端适配器
//!
//! ## 设计意图
//! 把 `async_nats::Client` 包装成统一 `RequestPlaneClient` trait，使上层路由代码
//! 可以以 transport-agnostic 的方式发送请求：根据配置选择 HTTP / TCP / NATS
//! 三种客户端而不需修改调用点。本文件只负责“headers 转换 + request_with_headers
//! 调用 + 错误包装为 `PagodaError`”三步。
//!
//! ## 外部契约
//! - `pub struct NatsRequestClient { client: async_nats::Client }`：唯一公开构造点
//!   `NatsRequestClient::new(client)`。
//! - `impl RequestPlaneClient for NatsRequestClient`：
//!   - `send_request(address, payload, headers)`：成功返回 `response.payload`；
//!     失败时递增 `NATS_ERRORS_TOTAL{kind="request_failed"}` 并返回 `PagodaError`
//!     （`ErrorType::CannotConnect`，消息 "NATS request to {address} failed"）。
//!   - `transport_name() -> "nats"`。
//!   - `is_healthy() -> true`（NATS 客户端不暴露连接状态，默认乐观上报）。
//!   - `stats() -> ClientStats { active_connections: 0/1, 其余均 0 }`。
//!   - `close() -> Ok(())`（依赖客户端生命周期自动释放）。
//!
//! ## 实现要点
//! - 抽取三个私有 helper：
//!   - `headers_for_nats(headers) -> async_nats::HeaderMap`：集中 generic Headers
//!     到 NATS HeaderMap 的转换逻辑，避免 send_request 中嵌套循环。
//!   - `record_request_failure()`：集中递增计数器的调用，为后续可能加入额外
//!     指标预留唯一入口。
//!   - `request_failure_error(address, cause) -> anyhow::Error`：把 "计数器递增 +
//!     PagodaError 构造" 这对孪生动作原子化，避免遗漏任一侧。
//! - 上述 helper 均为模块私有（无 `pub`），不改变 `RequestPlaneClient` 契约。

use super::unified_client::{ClientStats, Headers, RequestPlaneClient};
use crate::error::{PagodaError, ErrorType};
use crate::metrics::transport_metrics::NATS_ERRORS_TOTAL;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;

// === SECTION: NatsRequestClient 结构与私有 helper ===

/// NATS implementation of RequestPlaneClient
///
/// This client wraps the async_nats::Client and adapts it to the
/// unified RequestPlaneClient interface.
pub struct NatsRequestClient {
    client: async_nats::Client,
}

impl NatsRequestClient {
    /// Create a new NATS request client
    ///
    /// # Arguments
    ///
    /// * `client` - The underlying NATS client
    pub fn new(client: async_nats::Client) -> Self {
        Self { client }
    }

    fn headers_for_nats(headers: Headers) -> async_nats::HeaderMap {
        let mut nats_headers = async_nats::HeaderMap::new();
        for (key, value) in headers {
            nats_headers.insert(key.as_str(), value.as_str());
        }
        nats_headers
    }

    fn record_request_failure() {
        NATS_ERRORS_TOTAL
            .with_label_values(&["request_failed"])
            .inc();
    }

    fn request_failure_error(
        address: &str,
        cause: impl std::error::Error + 'static,
    ) -> anyhow::Error {
        Self::record_request_failure();
        anyhow::anyhow!(
            PagodaError::builder()
                .error_type(ErrorType::CannotConnect)
                .message(format!("NATS request to {address} failed"))
                .cause(cause)
                .build()
        )
    }
}

// === SECTION: RequestPlaneClient impl ===

#[async_trait]
impl RequestPlaneClient for NatsRequestClient {
    async fn send_request(
        &self,
        address: String,
        payload: Bytes,
        headers: Headers,
    ) -> Result<Bytes> {
        let nats_headers = Self::headers_for_nats(headers);

        let response = self
            .client
            .request_with_headers(address.clone(), nats_headers, payload)
            .await
            .map_err(|e| Self::request_failure_error(&address, e))?;

        Ok(response.payload)
    }

    fn transport_name(&self) -> &'static str {
        "nats"
    }

    fn is_healthy(&self) -> bool {
        // Check if NATS client is connected
        // NATS client doesn't expose connection state directly, assume healthy
        true
    }

    fn stats(&self) -> ClientStats {
        ClientStats {
            requests_sent: 0,
            responses_received: 0,
            errors: 0,
            bytes_sent: 0,
            bytes_received: 0,
            active_connections: if self.is_healthy() { 1 } else { 0 },
            idle_connections: 0,
            avg_latency_us: 0,
        }
    }

    async fn close(&self) -> Result<()> {
        // NATS client doesn't have an explicit close method
        // Connection is managed by the client lifecycle
        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION: 测试
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    //! ## 测试矩阵
    //!
    //! | 测试名 | 覆盖维度 |
    //! |---|---|
    //! | `test_headers_for_nats_empty` | 空 Headers → 空 HeaderMap（边界） |
    //! | `test_headers_for_nats_single_kv` | 单 key/value 透传（happy path） |
    //! | `test_headers_for_nats_preserves_multiple_keys` | 多 key 全部保留（不变式） |
    //! | `test_record_request_failure_increments_counter` | 计数器 +1（指标契约） |
    //! | `test_request_failure_error_wraps_cause_and_increments` | 同步触发计数 + 构造 PagodaError（孪生原子化） |
    //! | `test_request_failure_error_message_contains_address` | 错误消息包含目标地址（可观测性） |
    //!
    //! 说明：`transport_name()` / `is_healthy()` / `stats()` / `close()` / `send_request()`
    //! 均依赖真实 `async_nats::Client`，本单元测试不引入 NATS server 依赖，留给集成测试覆盖。

    use super::*;
    use crate::metrics::transport_metrics::NATS_ERRORS_TOTAL;

    fn nats_failure_counter() -> u64 {
        NATS_ERRORS_TOTAL
            .with_label_values(&["request_failed"])
            .get()
    }

    #[test]
    fn test_headers_for_nats_empty() {
        let map = NatsRequestClient::headers_for_nats(Headers::new());
        assert_eq!(map.iter().count(), 0);
    }

    #[test]
    fn test_headers_for_nats_single_kv() {
        let mut h = Headers::new();
        h.insert("k".to_string(), "v".to_string());
        let map = NatsRequestClient::headers_for_nats(h);
        assert_eq!(map.iter().count(), 1);
        // async_nats::HeaderMap 通过 get 取首值；这里只断言条目数 + key 存在。
        let got = map.get("k").expect("key k present");
        assert_eq!(got.as_str(), "v");
    }

    #[test]
    fn test_headers_for_nats_preserves_multiple_keys() {
        let mut h = Headers::new();
        h.insert("a".to_string(), "1".to_string());
        h.insert("b".to_string(), "2".to_string());
        h.insert("c".to_string(), "3".to_string());
        let map = NatsRequestClient::headers_for_nats(h);
        for (k, v) in [("a", "1"), ("b", "2"), ("c", "3")] {
            assert_eq!(map.get(k).expect("key present").as_str(), v);
        }
    }

    #[test]
    fn test_record_request_failure_increments_counter() {
        let before = nats_failure_counter();
        NatsRequestClient::record_request_failure();
        let after = nats_failure_counter();
        assert_eq!(after, before + 1, "NATS_ERRORS_TOTAL 应 +1");
    }

    #[test]
    fn test_request_failure_error_wraps_cause_and_increments() {
        let before = nats_failure_counter();
        let cause = std::io::Error::other("boom-cause");
        let err = NatsRequestClient::request_failure_error("nats://x:4222", cause);
        let after = nats_failure_counter();
        assert_eq!(after, before + 1, "计数器与错误构造应原子化");

        // 错误链应包含 cause 文本
        let chain = format!("{err:#}");
        assert!(chain.contains("boom-cause"), "got: {chain}");
    }

    #[test]
    fn test_request_failure_error_message_contains_address() {
        let cause = std::io::Error::other("ignored");
        let err = NatsRequestClient::request_failure_error("nats://broker:4222", cause);
        let s = format!("{err:#}");
        assert!(s.contains("NATS request to nats://broker:4222 failed"), "got: {s}");
    }
}
