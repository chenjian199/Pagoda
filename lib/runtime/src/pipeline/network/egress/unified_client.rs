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
//!   `is_available()` 直接内联表达 `requests_sent > 0 || active_connections > 0`。
//!
//! ## 实现要点
//! - `start_warmup` 默认是 no-op，这样 HTTP/NATS 客户端就不必关心只有 TCP 才用得到的
//!   `instance_rx: watch::Receiver<Vec<Instance>>`；签名里出现 `tokio` 和
//!   `crate::servicegroup::Instance` 本身就是契约。
//! - `is_available` 选择“请求活动 OR 连接活动”作为可用性启发，让“还没发请求但已经连上”
//!   的客户端也被视为 available，便于 dashboard 展示。

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;

// === SECTION: 类型别名 ===

/// 请求 headers 的类型别名。
pub type Headers = HashMap<String, String>;

// === SECTION: RequestPlaneClient trait ===

/// 请求平面客户端的统一接口。
///
/// 该 trait 抽象了不同传输机制（TCP、HTTP、NATS），为“发送请求、
/// 接收确认”提供一致的接口。
///
/// # 设计原则
///
/// 1. **传输无关**：实现可互换，无需改动 router 逻辑；
/// 2. **默认异步**：所有操作都是 async，以支持高并发；
/// 3. **Headers 支持**：所有传输都必须支持自定义 headers（用于追踪等）；
/// 4. **健康检查**：实现应提供连接健康信息；
/// 5. **错误处理**：所有错误统一包装为 `anyhow::Result`，以保持灵活性。
///
/// # 示例
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
    /// 向指定地址发送请求并等待确认。
    ///
    /// # 参数
    ///
    /// * `address` —— 与传输相关的地址：
    ///   - HTTP：`http://host:port/path`
    ///   - TCP：`host:port` 或 `tcp://host:port`
    ///   - NATS：`subject.name`
    /// * `payload` —— 请求负载（字节形式）
    /// * `headers` —— 用于追踪、认证等的自定义 headers
    ///
    /// # 返回
    ///
    /// 返回一个确认响应。注意：对于流式响应，实际响应数据是通过 TCP 响应
    /// 平面传回的，而不是通过这次确认。
    ///
    /// # 错误
    ///
    /// 出现以下情况时返回错误：
    /// - 连接 portname 失败；
    /// - 请求超时；
    /// - 传输相关错误（例如 NATS 服务器不可用）。
    async fn send_request(
        &self,
        address: String,
        payload: Bytes,
        headers: Headers,
    ) -> Result<Bytes>;

    /// 获取传输名称。
    ///
    /// 返回传输类型的静态字符串标识，用于日志与调试。
    ///
    /// # 示例
    ///
    /// - `"tcp"` —— 原生 TCP 传输
    /// - `"http"` 或 `"http2"` —— HTTP/2 传输
    /// - `"nats"` —— NATS 消息
    fn transport_name(&self) -> &'static str;

    /// 检查连接健康状态。
    ///
    /// 如果客户端健康且可以发送请求，则返回 `true`。
    /// 这是一个不会执行实际网络 I/O 的轻量检查。
    ///
    /// 出现以下情况时实现应返回 `false`：
    /// - 连接池耗尽；
    /// - 底层传输已断开；
    /// - 客户端已被显式关闭。
    fn is_healthy(&self) -> bool;

    /// 获取客户端统计信息（可选）。
    ///
    /// 返回用于监控和调试的客户端运行时统计。
    /// 默认实现返回空统计。
    fn stats(&self) -> ClientStats {
        ClientStats::default()
    }

    /// 启动一个后台任务，为新发现的后端提前预热连接。
    /// 只有 TCP 会重写它；HTTP 与 NATS 客户端沿用这个 no-op 默认实现。
    fn start_warmup(
        &self,
        _instance_rx: tokio::sync::watch::Receiver<Vec<crate::servicegroup::Instance>>,
        _cancel_token: tokio_util::sync::CancellationToken,
    ) {
        // 默认 no-op
    }

    /// 优雅地关闭客户端（可选）。
    ///
    /// 实现应该：
    /// - 关闭所有活跃连接
    /// - 等待在飞请求完成（或超时）
    /// - 释放所有资源
    ///
    /// 默认实现什么都不做。
    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

// === SECTION: ClientStats ===

/// 客户端运行时统计信息。
///
/// 用于监控与调试传输客户端的性能。
#[derive(Debug, Clone, Default)]
pub struct ClientStats {
    /// 发送的请求总数
    pub requests_sent: u64,

    /// 成功响应总数
    pub responses_received: u64,

    /// 错误总数
    pub errors: u64,

    /// 发送的字节总数
    pub bytes_sent: u64,

    /// 接收的字节总数
    pub bytes_received: u64,

    /// 活跃连接数（针对使用连接池的传输）
    pub active_connections: usize,

    /// 池中空闲连接数
    pub idle_connections: usize,

    /// 平均请求延迟（微秒；不可用时为 0）
    pub avg_latency_us: u64,
}

impl ClientStats {
    /// 创建新的空统计。
    pub fn new() -> Self {
        Self::default()
    }

    /// 检查统计是否可用（非零）。
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
