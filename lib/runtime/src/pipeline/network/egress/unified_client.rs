// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::network::egress::unified_client` —— 出站请求平面的传输无关抽象
//!
//! ## 设计意图
//! 不同传输（TCP/HTTP/NATS）的客户端要在 `egress` router 中可互换，必须暴露同一个
//! 抽象 trait：[`RequestPlaneClient`]。router 只依赖这一层 trait，不感知背后是 socket、
//! NATS 还是 HTTP 池。
//!
//! ## 外部契约
//! - `pub type Headers = HashMap<String, String>`；
//! - `pub trait RequestPlaneClient: Send + Sync`，方法集与默认实现：
//!   - `async fn send_request(address, payload, headers) -> Result<Bytes>`（无默认实现）；
//!   - `fn transport_name() -> &'static str`（无默认）；
//!   - `fn is_healthy() -> bool`（无默认）；
//!   - `fn stats() -> ClientStats { ClientStats::default() }`（默认空）；
//!   - `fn start_warmup(_instance_rx, _cancel_token)`（默认 no-op，仅 TCP 重写）；
//!   - `async fn close() -> Result<()> { Ok(()) }`（默认 no-op）。
//! - `pub struct ClientStats`（`Debug + Clone + Default`），所有字段 `pub`：
//!   `requests_sent / responses_received / errors / bytes_sent / bytes_received /
//!    active_connections / idle_connections / avg_latency_us`。
//! - `ClientStats::new()` 与 `ClientStats::is_available()` 公开方法。
//! - **不**抽出 `has_request_activity` / `has_connection_activity` 之类的私有辅助方法；
//!   `is_available()` 内联表达 `requests_sent > 0 || active_connections > 0`。
//!
//! ## 实现要点
//! - `start_warmup` 默认 no-op，是为了让 HTTP/NATS 客户端不必关心 TCP 才用得到的
//!   `instance_rx: watch::Receiver<Vec<Instance>>`；签名出现 `tokio` 与
//!   `crate::servicegroup::Instance` 是契约。
//! - `is_available` 选用"请求活动 OR 连接活动"作为可用性启发，让"还没发请求但已连上"
//!   的客户端也被视为 available，便于 dashboard 展示。

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;

// === SECTION: 类型别名 ===

/// Type alias for request headers
pub type Headers = HashMap<String, String>;

// === SECTION: RequestPlaneClient trait ===

/// Unified interface for request plane clients
///
/// This trait abstracts over different transport mechanisms (TCP, HTTP, NATS)
/// providing a consistent interface for sending requests and receiving acknowledgments.
///
/// # Design Principles
///
/// 1. **Transport Agnostic**: Implementations can be swapped without changing router logic
/// 2. **Async by Default**: All operations are async to support high concurrency
/// 3. **Headers Support**: All transports must support custom headers for tracing, etc.
/// 4. **Health Checks**: Implementations should provide connection health information
/// 5. **Error Handling**: All errors are wrapped in anyhow::Result for flexibility
///
/// # Example
///
/// ```ignore
/// use pagoda_runtime::pipeline::network::egress::RequestPlaneClient;
///
/// async fn send_request(client: &dyn RequestPlaneClient) -> Result<()> {
///     let mut headers = HashMap::new();
///     headers.insert("x-request-id".to_string(), "123".to_string());
///
///     let response = client.send_request(
///         "service-portname".to_string(),
///         Bytes::from("payload"),
///         headers,
///     ).await?;
///
///     Ok(())
/// }
/// ```
#[async_trait]
pub trait RequestPlaneClient: Send + Sync {
    /// Send a request to a specific address and wait for acknowledgment
    ///
    /// # Arguments
    ///
    /// * `address` - Transport-specific address:
    ///   - HTTP: `http://host:port/path`
    ///   - TCP: `host:port` or `tcp://host:port`
    ///   - NATS: `subject.name`
    /// * `payload` - Request payload (encoded as bytes)
    /// * `headers` - Custom headers for tracing, authentication, etc.
    ///
    /// # Returns
    ///
    /// Returns an acknowledgment response. Note that for streaming responses,
    /// the actual response data comes over the TCP response plane, not through
    /// this acknowledgment.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection to the portname fails
    /// - Request times out
    /// - Transport-specific errors occur (e.g., NATS server unavailable)
    async fn send_request(
        &self,
        address: String,
        payload: Bytes,
        headers: Headers,
    ) -> Result<Bytes>;

    /// Get the transport name
    ///
    /// Returns a static string identifier for the transport type.
    /// Used for logging and debugging.
    ///
    /// # Examples
    ///
    /// - `"tcp"` - Raw TCP transport
    /// - `"http"` or `"http2"` - HTTP/2 transport
    /// - `"nats"` - NATS messaging
    fn transport_name(&self) -> &'static str;

    /// Check connection health
    ///
    /// Returns `true` if the client is healthy and ready to send requests.
    /// This is a lightweight check that doesn't perform actual network I/O.
    ///
    /// Implementations should return `false` if:
    /// - Connection pool is exhausted
    /// - Underlying transport is disconnected
    /// - Client has been explicitly closed
    fn is_healthy(&self) -> bool;

    /// Get client statistics (optional)
    ///
    /// Returns runtime statistics about the client for monitoring and debugging.
    /// Default implementation returns empty statistics.
    fn stats(&self) -> ClientStats {
        ClientStats::default()
    }

    /// Start a background task that eagerly warms connections for newly-discovered backends.
    /// Only TCP overrides this; HTTP and NATS clients inherit the no-op.
    fn start_warmup(
        &self,
        _instance_rx: tokio::sync::watch::Receiver<Vec<crate::servicegroup::Instance>>,
        _cancel_token: tokio_util::sync::CancellationToken,
    ) {
        // No-op default
    }

    /// Close the client gracefully (optional)
    ///
    /// Implementations should:
    /// - Close all active connections
    /// - Wait for in-flight requests to complete (or timeout)
    /// - Release all resources
    ///
    /// Default implementation does nothing.
    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

// === SECTION: ClientStats ===

/// Client runtime statistics
///
/// Used for monitoring and debugging transport client performance.
#[derive(Debug, Clone, Default)]
pub struct ClientStats {
    /// Total number of requests sent
    pub requests_sent: u64,

    /// Total number of successful responses
    pub responses_received: u64,

    /// Total number of errors
    pub errors: u64,

    /// Total bytes sent
    pub bytes_sent: u64,

    /// Total bytes received
    pub bytes_received: u64,

    /// Number of active connections (for connection-pooled transports)
    pub active_connections: usize,

    /// Number of idle connections in pool
    pub idle_connections: usize,

    /// Average request latency in microseconds (0 if not available)
    pub avg_latency_us: u64,
}

impl ClientStats {
    /// Create new empty statistics
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if statistics are available (non-zero)
    pub fn is_available(&self) -> bool {
        self.requests_sent > 0 || self.active_connections > 0
    }
}

// === SECTION: tests ===

#[cfg(test)]
mod tests {
    //! ## 测试矩阵
    //!
    //! | 测试名 | 覆盖维度 |
    //! |---|---|
    //! | `test_client_stats_default` | `Default` 全零 + `is_available()=false` |
    //! | `test_client_stats_is_available` | `requests_sent` / `active_connections` 任一非零都为 true |
    //! | `test_client_stats_new_equals_default` | `new()` 与 `default()` 字段级等价 |
    //! | `test_client_stats_clone_and_debug` | `Clone + Debug` 衍生（契约面） |
    //! | `test_client_stats_other_fields_do_not_imply_available` | `errors`/`bytes_*`/`idle_connections` 单独非零仍为 false（OR 语义边界） |
    //! | `test_headers_alias_round_trip` | `Headers` 别名作为 `HashMap<String,String>` 行为 |
    //! | `test_default_trait_methods_via_fake_client` | trait 默认实现 `stats()` / `start_warmup()` / `close()` 不抛错 |
    //!
    //! ## 意义
    //! 锁定 `is_available()` 的 OR 语义；防止后续误改成 AND 或漏判 connection-only 场景；
    //! 同时通过 `FakeClient` 在编译期校验 `RequestPlaneClient` 的默认方法仍可被零样板继承。

    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_client_stats_default() {
        let stats = ClientStats::default();
        assert_eq!(stats.requests_sent, 0);
        assert_eq!(stats.responses_received, 0);
        assert!(!stats.is_available());
    }

    #[test]
    fn test_client_stats_is_available() {
        let mut stats = ClientStats::default();
        assert!(!stats.is_available());

        stats.requests_sent = 1;
        assert!(stats.is_available());

        let stats2 = ClientStats {
            active_connections: 1,
            ..Default::default()
        };
        assert!(stats2.is_available());
    }

    #[test]
    fn test_client_stats_new_equals_default() {
        let a = ClientStats::new();
        let b = ClientStats::default();
        assert_eq!(a.requests_sent, b.requests_sent);
        assert_eq!(a.responses_received, b.responses_received);
        assert_eq!(a.errors, b.errors);
        assert_eq!(a.bytes_sent, b.bytes_sent);
        assert_eq!(a.bytes_received, b.bytes_received);
        assert_eq!(a.active_connections, b.active_connections);
        assert_eq!(a.idle_connections, b.idle_connections);
        assert_eq!(a.avg_latency_us, b.avg_latency_us);
    }

    #[test]
    fn test_client_stats_clone_and_debug() {
        let s = ClientStats {
            requests_sent: 42,
            active_connections: 3,
            ..Default::default()
        };
        let c = s.clone();
        assert_eq!(c.requests_sent, 42);
        assert_eq!(c.active_connections, 3);
        let d = format!("{:?}", s);
        assert!(d.contains("ClientStats"));
        assert!(d.contains("42"));
    }

    #[test]
    fn test_client_stats_other_fields_do_not_imply_available() {
        // 单纯 errors / bytes_* / idle_connections / avg_latency_us 非零，
        // 不应被视为 available。
        for stats in [
            ClientStats { errors: 1, ..Default::default() },
            ClientStats { bytes_sent: 1, ..Default::default() },
            ClientStats { bytes_received: 1, ..Default::default() },
            ClientStats { idle_connections: 1, ..Default::default() },
            ClientStats { avg_latency_us: 1, ..Default::default() },
            ClientStats { responses_received: 1, ..Default::default() },
        ] {
            assert!(
                !stats.is_available(),
                "non-request/connection fields must not flip availability: {stats:?}"
            );
        }
    }

    #[test]
    fn test_headers_alias_round_trip() {
        let mut h: Headers = HashMap::new();
        h.insert("x-trace".to_string(), "abc".to_string());
        assert_eq!(h.get("x-trace").map(String::as_str), Some("abc"));
        assert_eq!(h.len(), 1);
    }

    // ── FakeClient：仅用于校验 trait 默认实现仍可被零样板继承 ──
    struct FakeClient {
        warmup_called: Arc<AtomicBool>,
        closed: Arc<AtomicBool>,
    }

    #[async_trait]
    impl RequestPlaneClient for FakeClient {
        async fn send_request(
            &self,
            _address: String,
            _payload: Bytes,
            _headers: Headers,
        ) -> Result<Bytes> {
            Ok(Bytes::from_static(b"ok"))
        }
        fn transport_name(&self) -> &'static str {
            "fake"
        }
        fn is_healthy(&self) -> bool {
            true
        }
        // 不重写 stats / start_warmup / close —— 测试默认实现
    }

    #[tokio::test]
    async fn test_default_trait_methods_via_fake_client() {
        let warmup_called = Arc::new(AtomicBool::new(false));
        let closed = Arc::new(AtomicBool::new(false));
        let c = FakeClient {
            warmup_called: warmup_called.clone(),
            closed: closed.clone(),
        };

        // 默认 stats() 应等价 ClientStats::default()
        let s = c.stats();
        assert!(!s.is_available());
        assert_eq!(s.requests_sent, 0);

        // 默认 close() 应返回 Ok
        assert!(c.close().await.is_ok());

        // 默认 start_warmup 是 no-op：调用不应 panic，也不会触碰任何标志位
        let (_tx, rx) = tokio::sync::watch::channel(Vec::<crate::servicegroup::Instance>::new());
        let cancel = tokio_util::sync::CancellationToken::new();
        c.start_warmup(rx, cancel);
        assert!(!warmup_called.load(Ordering::SeqCst));
        assert!(!closed.load(Ordering::SeqCst));

        // 顺手调 send_request 跑通
        let resp = c
            .send_request("addr".into(), Bytes::new(), Headers::new())
            .await
            .unwrap();
        assert_eq!(resp.as_ref(), b"ok");
        assert_eq!(c.transport_name(), "fake");
        assert!(c.is_healthy());
    }
}
