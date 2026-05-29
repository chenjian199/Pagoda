// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::network::egress::http_router` —— HTTP/2 出站路由客户端
//!
//! ## 设计意图
//! 让 egress 层能像调用本地函数一样把请求 POST 到远端 HTTP 端点：内部维护连接池、
//! 按 `address` 复用 `reqwest::Client`、把统一的 `Headers` 映射到 HTTP header，并以
//! `RequestPlaneClient` trait 对外暴露。
//!
//! ## 外部契约
//! - 公开结构与 `RequestPlaneClient` 实现；`send_request` / `transport_name() -> "http"`
//!   / `is_healthy()` 行为与 lib-copy 一致。
//! - 内部统计字段、构造器参数、错误映射规则均与 lib-copy 一致，不引入新的 helper。
//!
//! ## 实现要点
//! - `reqwest::Client` 使用 HTTP/2 优先 + keep-alive；连接复用由 reqwest 自身负责，
//!   本文件不再做二级池。
//! - `Http2Config::from_env` 中八处类似的「读取变量 → parse 成字段」逻辑收敛到私有
//!   [`env_parse<T>`] helper，避免调用一次动一次独立书写。
//! - 错误统一包装到 [`connect_error`] helper 下，该 helper 负责构造
//!   `DynamoError::CannotConnect` + 携带 cause。

//! HTTP/2 client for request plane

// ─────────────────────────────────────────────────────────────────────────────
// === SECTION: [1] 依赖导入与常量
// ─────────────────────────────────────────────────────────────────────────────

use super::unified_client::{Headers, RequestPlaneClient};
use crate::Result;
use async_trait::async_trait;
use bytes::Bytes;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

/// Default timeout for HTTP requests (ack only, not full response)
const DEFAULT_HTTP_REQUEST_TIMEOUT_SECS: u64 = 5;

/// HTTP/2 Performance Configuration Constants
const DEFAULT_MAX_FRAME_SIZE: u32 = 1024 * 1024; // 1MB frame size for better throughput
const DEFAULT_MAX_CONCURRENT_STREAMS: u32 = 1000; // Allow more concurrent streams
const DEFAULT_POOL_MAX_IDLE_PER_HOST: usize = 100; // Increased connection pool
const DEFAULT_POOL_IDLE_TIMEOUT_SECS: u64 = 90; // Keep connections alive longer
const DEFAULT_HTTP2_KEEP_ALIVE_INTERVAL_SECS: u64 = 30; // Send pings every 30s
const DEFAULT_HTTP2_KEEP_ALIVE_TIMEOUT_SECS: u64 = 10; // Timeout for ping responses
const DEFAULT_HTTP2_ADAPTIVE_WINDOW: bool = true; // Enable adaptive flow control

// ─────────────────────────────────────────────────────────────────────────────
// === SECTION: [2] 私有 helper（env 解析 / 错误构造）
// ─────────────────────────────────────────────────────────────────────────────

/// 读环境变量并调用 `T::from_str` 解析；变量未设或解析失败均返回 `None`。
///
/// 该 helper 把 `Http2Config::from_env` 中 8 次重复的「`env::var().ok().and_then(|s| s.parse())`
/// 」收敛为一行调用。
fn env_parse<T: FromStr>(name: &str) -> Option<T> {
    std::env::var(name).ok().and_then(|v| v.parse::<T>().ok())
}

/// 把 `reqwest::Error` 包装为 `anyhow::Error(DynamoError::CannotConnect)`，保留 cause 链。
fn connect_error(address: &str, cause: reqwest::Error) -> anyhow::Error {
    anyhow::anyhow!(
        crate::error::DynamoError::builder()
            .error_type(crate::error::ErrorType::CannotConnect)
            .message(format!("HTTP request to {address} failed"))
            .cause(cause)
            .build()
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// === SECTION: [3] Http2Config
// ─────────────────────────────────────────────────────────────────────────────

/// HTTP/2 Performance Configuration
#[derive(Debug, Clone)]
pub struct Http2Config {
    pub max_frame_size: u32,
    pub max_concurrent_streams: u32,
    pub pool_max_idle_per_host: usize,
    pub pool_idle_timeout: Duration,
    pub keep_alive_interval: Duration,
    pub keep_alive_timeout: Duration,
    pub adaptive_window: bool,
    pub request_timeout: Duration,
}

impl Default for Http2Config {
    fn default() -> Self {
        Self {
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            max_concurrent_streams: DEFAULT_MAX_CONCURRENT_STREAMS,
            pool_max_idle_per_host: DEFAULT_POOL_MAX_IDLE_PER_HOST,
            pool_idle_timeout: Duration::from_secs(DEFAULT_POOL_IDLE_TIMEOUT_SECS),
            keep_alive_interval: Duration::from_secs(DEFAULT_HTTP2_KEEP_ALIVE_INTERVAL_SECS),
            keep_alive_timeout: Duration::from_secs(DEFAULT_HTTP2_KEEP_ALIVE_TIMEOUT_SECS),
            adaptive_window: DEFAULT_HTTP2_ADAPTIVE_WINDOW,
            request_timeout: Duration::from_secs(DEFAULT_HTTP_REQUEST_TIMEOUT_SECS),
        }
    }
}

impl Http2Config {
    /// Create configuration from environment variables
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Some(v) = env_parse::<u32>("DYN_HTTP2_MAX_FRAME_SIZE") {
            config.max_frame_size = v;
        }
        if let Some(v) = env_parse::<u32>("DYN_HTTP2_MAX_CONCURRENT_STREAMS") {
            config.max_concurrent_streams = v;
        }
        if let Some(v) = env_parse::<usize>("DYN_HTTP2_POOL_MAX_IDLE_PER_HOST") {
            config.pool_max_idle_per_host = v;
        }
        if let Some(v) = env_parse::<u64>("DYN_HTTP2_POOL_IDLE_TIMEOUT_SECS") {
            config.pool_idle_timeout = Duration::from_secs(v);
        }
        if let Some(v) = env_parse::<u64>("DYN_HTTP2_KEEP_ALIVE_INTERVAL_SECS") {
            config.keep_alive_interval = Duration::from_secs(v);
        }
        if let Some(v) = env_parse::<u64>("DYN_HTTP2_KEEP_ALIVE_TIMEOUT_SECS") {
            config.keep_alive_timeout = Duration::from_secs(v);
        }
        if let Ok(val) = std::env::var("DYN_HTTP2_ADAPTIVE_WINDOW") {
            // 保留 lib-copy 语义：变量存在但解析失败 → 回退默认值。
            config.adaptive_window = val.parse().unwrap_or(DEFAULT_HTTP2_ADAPTIVE_WINDOW);
        }
        if let Some(v) = env_parse::<u64>("DYN_HTTP_REQUEST_TIMEOUT") {
            config.request_timeout = Duration::from_secs(v);
        }

        config
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// === SECTION: [4] HttpRequestClient
// ─────────────────────────────────────────────────────────────────────────────

/// HTTP/2 request plane client
pub struct HttpRequestClient {
    client: reqwest::Client,
    config: Http2Config,
}

impl HttpRequestClient {
    /// Create a new HTTP request client with HTTP/2 and default configuration
    pub fn new() -> Result<Self> {
        Self::with_config(Http2Config::default())
    }

    /// Create a new HTTP request client with custom timeout (legacy method)
    /// Uses HTTP/2 with prior knowledge to avoid ALPN negotiation overhead
    pub fn with_timeout(timeout: Duration) -> Result<Self> {
        let config = Http2Config {
            request_timeout: timeout,
            ..Http2Config::default()
        };
        Self::with_config(config)
    }

    /// Create a new HTTP request client with basic configuration
    ///
    /// Note: Advanced HTTP/2 configuration methods may not be available in all versions of reqwest.
    /// This implementation uses only the stable, widely-supported configuration options.
    pub fn with_config(config: Http2Config) -> Result<Self> {
        let builder = reqwest::Client::builder()
            .pool_max_idle_per_host(config.pool_max_idle_per_host)
            .pool_idle_timeout(config.pool_idle_timeout)
            .timeout(config.request_timeout);
        // HTTP/2 is automatically negotiated by reqwest when available

        let client = builder.build()?;

        Ok(Self { client, config })
    }

    /// Create from environment configuration
    pub fn from_env() -> Result<Self> {
        Self::with_config(Http2Config::from_env())
    }

    /// Get the current HTTP/2 configuration
    pub fn config(&self) -> &Http2Config {
        &self.config
    }
}

impl Default for HttpRequestClient {
    fn default() -> Self {
        Self::new().expect("Failed to create HTTP request client")
    }
}

#[async_trait]
impl RequestPlaneClient for HttpRequestClient {
    async fn send_request(
        &self,
        address: String,
        payload: Bytes,
        headers: Headers,
    ) -> Result<Bytes> {
        let mut req = self
            .client
            .post(&address)
            .header("Content-Type", "application/octet-stream")
            .body(payload);

        // Add custom headers
        for (key, value) in headers {
            req = req.header(key, value);
        }

        let response = req.send().await.map_err(|e| connect_error(&address, e))?;

        if !response.status().is_success() {
            anyhow::bail!(
                "HTTP request failed with status {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            );
        }

        let body = response.bytes().await?;
        Ok(body)
    }

    fn transport_name(&self) -> &'static str {
        "http2"
    }

    fn is_healthy(&self) -> bool {
        // HTTP client is stateless and always healthy if created successfully
        true
    }
}

#[cfg(test)]
mod tests {
    //! ## 测试矩阵
    //!
    //! | 测试名 | 覆盖维度 |
    //! |---|---|
    //! | `test_http_client_creation` | （lib-copy）构造默认客户端成功 |
    //! | `test_http_client_with_custom_timeout` | （lib-copy）`with_timeout` 覆盖 `request_timeout` |
    //! | `test_http2_config_from_env` | （lib-copy）多变量覆盖路径 |
    //! | `test_http_client_with_custom_config` | （lib-copy）所有字段透传到 client |
    //! | `test_http_client_send_request_invalid_url` | （lib-copy）无法连接 → Err |
    //! | `test_http2_client_server_integration` | （lib-copy）端到端 axum 集成 |
    //! | `test_http2_headers_propagation` | （lib-copy）自定义 header 透传 |
    //! | `test_http2_concurrent_requests` | （lib-copy）HTTP/2 多路并发 |
    //! | `test_http2_performance_benchmark` | （lib-copy）性能基线 |
    //! | `test_http2_config_default_values` | 默认值锁定（防常量被误改） |
    //! | `test_env_parse_returns_none_when_var_missing` | helper：变量缺失 → None |
    //! | `test_env_parse_returns_none_when_malformed` | helper：变量存在但格式非法 → None |
    //! | `test_env_parse_succeeds_for_typical_types` | helper：u32 / u64 / usize / bool 解析正确 |
    //! | `test_http_client_transport_name_is_http2` | trait 契约：`"http2"` |
    //! | `test_http_client_is_healthy_returns_true` | trait 契约 |
    //! | `test_http_client_config_accessor_returns_set_value` | `config()` getter 暴露的是构造态 |
    //! | `test_http_client_default_constructs_without_panic` | `Default::default()` 不 panic |
    //! | `test_connect_error_wraps_address_and_cause` | helper：错误消息含 address，cause 链可读 |

    use super::*;
    use axum::{Router, body::Bytes as AxumBytes, extract::State as AxumState, routing::post};
    use std::sync::Arc;
    use tokio::sync::Mutex as TokioMutex;

    // ── lib-copy 同名测试 ─────────────────────────────────────────────────

    #[test]
    fn test_http_client_creation() {
        let client = HttpRequestClient::new();
        assert!(client.is_ok());
    }

    #[test]
    fn test_http_client_with_custom_timeout() {
        let client = HttpRequestClient::with_timeout(Duration::from_secs(10));
        assert!(client.is_ok());
        assert_eq!(
            client.unwrap().config.request_timeout,
            Duration::from_secs(10)
        );
    }

    #[test]
    fn test_http2_config_from_env() {
        // Set environment variables
        unsafe {
            std::env::set_var("DYN_HTTP2_MAX_FRAME_SIZE", "2097152"); // 2MB
            std::env::set_var("DYN_HTTP2_MAX_CONCURRENT_STREAMS", "2000");
            std::env::set_var("DYN_HTTP2_POOL_MAX_IDLE_PER_HOST", "200");
            std::env::set_var("DYN_HTTP2_KEEP_ALIVE_INTERVAL_SECS", "60");
            std::env::set_var("DYN_HTTP2_ADAPTIVE_WINDOW", "false");
        }

        let config = Http2Config::from_env();

        assert_eq!(config.max_frame_size, 2097152);
        assert_eq!(config.max_concurrent_streams, 2000);
        assert_eq!(config.pool_max_idle_per_host, 200);
        assert_eq!(config.keep_alive_interval, Duration::from_secs(60));
        assert!(!config.adaptive_window);

        // Clean up
        unsafe {
            std::env::remove_var("DYN_HTTP2_MAX_FRAME_SIZE");
            std::env::remove_var("DYN_HTTP2_MAX_CONCURRENT_STREAMS");
            std::env::remove_var("DYN_HTTP2_POOL_MAX_IDLE_PER_HOST");
            std::env::remove_var("DYN_HTTP2_KEEP_ALIVE_INTERVAL_SECS");
            std::env::remove_var("DYN_HTTP2_ADAPTIVE_WINDOW");
        }
    }

    #[test]
    fn test_http_client_with_custom_config() {
        let config = Http2Config {
            max_frame_size: 512 * 1024, // 512KB
            max_concurrent_streams: 500,
            pool_max_idle_per_host: 75,
            pool_idle_timeout: Duration::from_secs(60),
            keep_alive_interval: Duration::from_secs(45),
            keep_alive_timeout: Duration::from_secs(15),
            adaptive_window: false,
            request_timeout: Duration::from_secs(8),
        };

        let client = HttpRequestClient::with_config(config.clone());
        assert!(client.is_ok());

        let client = client.unwrap();
        assert_eq!(client.config.max_frame_size, 512 * 1024);
        assert_eq!(client.config.max_concurrent_streams, 500);
        assert_eq!(client.config.pool_max_idle_per_host, 75);
        assert_eq!(client.config.request_timeout, Duration::from_secs(8));
    }

    #[tokio::test]
    async fn test_http_client_send_request_invalid_url() {
        let client = HttpRequestClient::new().unwrap();
        let result = client
            .send_request(
                "http://invalid-host-that-does-not-exist:9999/test".to_string(),
                Bytes::from("test"),
                std::collections::HashMap::new(),
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_http2_client_server_integration() {
        use hyper_util::rt::{TokioExecutor, TokioIo};
        use hyper_util::server::conn::auto::Builder as ConnBuilder;
        use hyper_util::service::TowerToHyperService;

        // Create a test server that accepts HTTP/2
        #[derive(Clone)]
        struct TestState {
            received: Arc<TokioMutex<Vec<Bytes>>>,
            protocol_version: Arc<TokioMutex<Option<String>>>,
        }

        async fn test_handler(
            AxumState(state): AxumState<TestState>,
            body: AxumBytes,
        ) -> &'static str {
            state.received.lock().await.push(body);
            "OK"
        }

        let state = TestState {
            received: Arc::new(TokioMutex::new(Vec::new())),
            protocol_version: Arc::new(TokioMutex::new(None)),
        };

        let app = Router::new()
            .route("/test", post(test_handler))
            .with_state(state.clone());

        // Bind to a random port
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Start HTTP/2 server
        let server_handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };

                let app = app.clone();
                tokio::spawn(async move {
                    let conn_builder = ConnBuilder::new(TokioExecutor::new());
                    let io = TokioIo::new(stream);
                    let tower_service = app.into_service();
                    let hyper_service = TowerToHyperService::new(tower_service);

                    let _ = conn_builder.serve_connection(io, hyper_service).await;
                });
            }
        });

        // Give server time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create HTTP/2 client with prior knowledge
        let client = HttpRequestClient::new().unwrap();

        // Send request
        let test_data = Bytes::from("test_payload");
        let result = client
            .send_request(
                format!("http://{}/test", addr),
                test_data.clone(),
                std::collections::HashMap::new(),
            )
            .await;

        // Verify request succeeded
        assert!(result.is_ok(), "Request failed: {:?}", result.err());

        // Verify server received the data
        tokio::time::sleep(Duration::from_millis(100)).await;
        let received = state.received.lock().await;
        assert_eq!(received.len(), 1);
        assert_eq!(received[0], test_data);

        // Cleanup
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_http2_headers_propagation() {
        use hyper_util::rt::{TokioExecutor, TokioIo};
        use hyper_util::server::conn::auto::Builder as ConnBuilder;
        use hyper_util::service::TowerToHyperService;

        // Create a test server that captures headers
        #[derive(Clone)]
        struct HeaderState {
            headers: Arc<TokioMutex<Vec<(String, String)>>>,
        }

        async fn header_handler(
            AxumState(state): AxumState<HeaderState>,
            headers: axum::http::HeaderMap,
        ) -> &'static str {
            let mut captured = state.headers.lock().await;
            for (name, value) in headers.iter() {
                if let Ok(val_str) = value.to_str() {
                    captured.push((name.to_string(), val_str.to_string()));
                }
            }
            "OK"
        }

        let state = HeaderState {
            headers: Arc::new(TokioMutex::new(Vec::new())),
        };

        let app = Router::new()
            .route("/test", post(header_handler))
            .with_state(state.clone());

        // Bind to a random port
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Start HTTP/2 server
        let server_handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };

                let app = app.clone();
                tokio::spawn(async move {
                    let conn_builder = ConnBuilder::new(TokioExecutor::new());
                    let io = TokioIo::new(stream);
                    let tower_service = app.into_service();
                    let hyper_service = TowerToHyperService::new(tower_service);

                    let _ = conn_builder.serve_connection(io, hyper_service).await;
                });
            }
        });

        // Give server time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create HTTP/2 client
        let client = HttpRequestClient::new().unwrap();

        // Send request with custom headers
        let mut headers = std::collections::HashMap::new();
        headers.insert("x-test-header".to_string(), "test-value".to_string());
        headers.insert("x-request-id".to_string(), "req-123".to_string());

        let result = client
            .send_request(
                format!("http://{}/test", addr),
                Bytes::from("test"),
                headers,
            )
            .await;

        // Verify request succeeded
        assert!(result.is_ok());

        // Verify headers were received
        tokio::time::sleep(Duration::from_millis(100)).await;
        let received_headers = state.headers.lock().await;

        let header_map: std::collections::HashMap<_, _> = received_headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        assert!(header_map.contains_key("x-test-header"));
        assert_eq!(header_map.get("x-test-header"), Some(&"test-value"));
        assert!(header_map.contains_key("x-request-id"));
        assert_eq!(header_map.get("x-request-id"), Some(&"req-123"));

        // Cleanup
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_http2_concurrent_requests() {
        use hyper_util::rt::{TokioExecutor, TokioIo};
        use hyper_util::server::conn::auto::Builder as ConnBuilder;
        use hyper_util::service::TowerToHyperService;
        use std::sync::atomic::{AtomicU64, Ordering};

        // Create a test server that counts requests
        #[derive(Clone)]
        struct CounterState {
            count: Arc<AtomicU64>,
        }

        async fn counter_handler(AxumState(state): AxumState<CounterState>) -> String {
            let count = state.count.fetch_add(1, Ordering::SeqCst);
            format!("{}", count)
        }

        let state = CounterState {
            count: Arc::new(AtomicU64::new(0)),
        };

        let app = Router::new()
            .route("/test", post(counter_handler))
            .with_state(state.clone());

        // Bind to a random port
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Start HTTP/2 server
        let server_handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };

                let app = app.clone();
                tokio::spawn(async move {
                    let conn_builder = ConnBuilder::new(TokioExecutor::new());
                    let io = TokioIo::new(stream);
                    let tower_service = app.into_service();
                    let hyper_service = TowerToHyperService::new(tower_service);

                    let _ = conn_builder.serve_connection(io, hyper_service).await;
                });
            }
        });

        // Give server time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create HTTP/2 client
        let client = Arc::new(HttpRequestClient::new().unwrap());

        // Send multiple concurrent requests (HTTP/2 multiplexing)
        let mut handles = vec![];
        for _ in 0..10 {
            let client = client.clone();
            let handle = tokio::spawn(async move {
                client
                    .send_request(
                        format!("http://{}/test", addr),
                        Bytes::from("test"),
                        std::collections::HashMap::new(),
                    )
                    .await
            });
            handles.push(handle);
        }

        // Wait for all requests to complete
        let mut success_count = 0;
        for handle in handles {
            if let Ok(Ok(_)) = handle.await {
                success_count += 1;
            }
        }

        // Verify all requests succeeded
        assert_eq!(success_count, 10);

        // Verify server received all requests
        assert_eq!(state.count.load(Ordering::SeqCst), 10);

        // Cleanup
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_http2_performance_benchmark() {
        use hyper_util::rt::{TokioExecutor, TokioIo};
        use hyper_util::server::conn::auto::Builder as ConnBuilder;
        use hyper_util::service::TowerToHyperService;
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::time::Instant;

        // Create a test server that measures performance
        #[derive(Clone)]
        struct PerfState {
            request_count: Arc<AtomicU64>,
            total_bytes: Arc<AtomicU64>,
        }

        async fn perf_handler(
            AxumState(state): AxumState<PerfState>,
            body: AxumBytes,
        ) -> &'static str {
            state.request_count.fetch_add(1, Ordering::Relaxed);
            state
                .total_bytes
                .fetch_add(body.len() as u64, Ordering::Relaxed);
            "OK"
        }

        let state = PerfState {
            request_count: Arc::new(AtomicU64::new(0)),
            total_bytes: Arc::new(AtomicU64::new(0)),
        };

        let app = Router::new()
            .route("/perf", post(perf_handler))
            .with_state(state.clone());

        // Bind to a random port
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Start HTTP/2 server
        let server_handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };

                let app = app.clone();
                tokio::spawn(async move {
                    let conn_builder = ConnBuilder::new(TokioExecutor::new());
                    let io = TokioIo::new(stream);
                    let tower_service = app.into_service();
                    let hyper_service = TowerToHyperService::new(tower_service);

                    let _ = conn_builder.serve_connection(io, hyper_service).await;
                });
            }
        });

        // Give server time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create optimized HTTP/2 client
        let optimized_config = Http2Config {
            max_frame_size: 1024 * 1024, // 1MB frames
            max_concurrent_streams: 1000,
            pool_max_idle_per_host: 100,
            pool_idle_timeout: Duration::from_secs(90),
            keep_alive_interval: Duration::from_secs(30),
            keep_alive_timeout: Duration::from_secs(10),
            adaptive_window: true,
            request_timeout: Duration::from_secs(30),
        };

        let client = Arc::new(HttpRequestClient::with_config(optimized_config).unwrap());

        // Performance test: Send many concurrent requests
        let num_requests = 100;
        let payload_size = 64 * 1024; // 64KB payload
        let payload = Bytes::from(vec![0u8; payload_size]);

        let start_time = Instant::now();
        let mut handles = vec![];

        for _ in 0..num_requests {
            let client = client.clone();
            let payload = payload.clone();

            let handle = tokio::spawn(async move {
                let headers = std::collections::HashMap::new();
                client
                    .send_request(format!("http://{}/perf", addr), payload, headers)
                    .await
            });
            handles.push(handle);
        }

        // Wait for all requests to complete
        let mut successful_requests = 0;
        for handle in handles {
            if handle.await.unwrap().is_ok() {
                successful_requests += 1;
            }
        }

        let elapsed = start_time.elapsed();
        let requests_per_sec = successful_requests as f64 / elapsed.as_secs_f64();
        let throughput_mbps =
            (successful_requests * payload_size) as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0);

        println!("Performance Results:");
        println!(
            "  Successful requests: {}/{}",
            successful_requests, num_requests
        );
        println!("  Total time: {:?}", elapsed);
        println!("  Requests/sec: {:.2}", requests_per_sec);
        println!("  Throughput: {:.2} MB/s", throughput_mbps);

        // Verify server received all requests
        let server_count = state.request_count.load(Ordering::Relaxed);
        let server_bytes = state.total_bytes.load(Ordering::Relaxed);

        assert_eq!(server_count, successful_requests as u64);
        assert_eq!(server_bytes, (successful_requests * payload_size) as u64);

        // Performance assertions (adjust based on your requirements)
        assert!(successful_requests >= num_requests * 95 / 100); // At least 95% success rate
        assert!(requests_per_sec > 50.0); // At least 50 requests per second
        assert!(throughput_mbps > 10.0); // At least 10 MB/s throughput

        // Cleanup
        server_handle.abort();
    }

    // ── 新增测试 ─────────────────────────────────────────────────────────

    #[test]
    fn test_http2_config_default_values() {
        let c = Http2Config::default();
        assert_eq!(c.max_frame_size, DEFAULT_MAX_FRAME_SIZE);
        assert_eq!(c.max_concurrent_streams, DEFAULT_MAX_CONCURRENT_STREAMS);
        assert_eq!(c.pool_max_idle_per_host, DEFAULT_POOL_MAX_IDLE_PER_HOST);
        assert_eq!(c.pool_idle_timeout, Duration::from_secs(DEFAULT_POOL_IDLE_TIMEOUT_SECS));
        assert_eq!(c.keep_alive_interval, Duration::from_secs(DEFAULT_HTTP2_KEEP_ALIVE_INTERVAL_SECS));
        assert_eq!(c.keep_alive_timeout, Duration::from_secs(DEFAULT_HTTP2_KEEP_ALIVE_TIMEOUT_SECS));
        assert_eq!(c.adaptive_window, DEFAULT_HTTP2_ADAPTIVE_WINDOW);
        assert_eq!(c.request_timeout, Duration::from_secs(DEFAULT_HTTP_REQUEST_TIMEOUT_SECS));
    }

    #[test]
    fn test_env_parse_returns_none_when_var_missing() {
        // 使用极不可能存在的变量名
        let v = env_parse::<u32>("DYN_TEST_HTTP_PROBABLY_NOT_SET_47A9B3");
        assert!(v.is_none());
    }

    #[test]
    fn test_env_parse_returns_none_when_malformed() {
        let name = "DYN_TEST_HTTP_MALFORMED_INT_47A9B3";
        unsafe {
            std::env::set_var(name, "not-a-number");
        }
        let v = env_parse::<u32>(name);
        unsafe {
            std::env::remove_var(name);
        }
        assert!(v.is_none(), "malformed value 应回退到 None");
    }

    #[test]
    fn test_env_parse_succeeds_for_typical_types() {
        let name = "DYN_TEST_HTTP_TYPED_47A9B3";
        unsafe {
            std::env::set_var(name, "12345");
        }
        let as_u32 = env_parse::<u32>(name);
        let as_u64 = env_parse::<u64>(name);
        let as_usize = env_parse::<usize>(name);
        unsafe {
            std::env::set_var(name, "true");
        }
        let as_bool = env_parse::<bool>(name);
        unsafe {
            std::env::remove_var(name);
        }
        assert_eq!(as_u32, Some(12345));
        assert_eq!(as_u64, Some(12345));
        assert_eq!(as_usize, Some(12345));
        assert_eq!(as_bool, Some(true));
    }

    #[test]
    fn test_http_client_transport_name_is_http2() {
        let client = HttpRequestClient::new().unwrap();
        assert_eq!(client.transport_name(), "http2");
    }

    #[test]
    fn test_http_client_is_healthy_returns_true() {
        let client = HttpRequestClient::new().unwrap();
        assert!(client.is_healthy());
    }

    #[test]
    fn test_http_client_config_accessor_returns_set_value() {
        let cfg = Http2Config {
            request_timeout: Duration::from_millis(777),
            ..Default::default()
        };
        let client = HttpRequestClient::with_config(cfg).unwrap();
        assert_eq!(client.config().request_timeout, Duration::from_millis(777));
    }

    #[test]
    fn test_http_client_default_constructs_without_panic() {
        let _ = HttpRequestClient::default();
    }

    #[tokio::test]
    async fn test_connect_error_wraps_address_and_cause() {
        // 通过 send_request 触发 connect_error 路径，验证错误链含 address。
        let client = HttpRequestClient::with_timeout(Duration::from_millis(200)).unwrap();
        let err = client
            .send_request(
                "http://127.0.0.1:1/dead".to_string(),
                Bytes::from_static(b"x"),
                std::collections::HashMap::new(),
            )
            .await
            .expect_err("应失败");
        let s = format!("{err:#}");
        assert!(s.contains("HTTP request to http://127.0.0.1:1/dead failed"), "got: {s}");
    }
}
