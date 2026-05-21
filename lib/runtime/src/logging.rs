// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 分布式链路追踪与日志初始化。

use std::collections::HashMap;
use std::sync::Once;

use hyper::http;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

static INIT_ONCE: Once = Once::new();

struct LoggingConfig {
    log_level: String,
    log_filters: HashMap<String, String>,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        let mut filters = HashMap::new();
        for lib in &["h2","tower","hyper_util","async_nats","rustls","tokenizers","opentelemetry"] {
            filters.insert(lib.to_string(), "error".to_string());
        }
        Self { log_level: "info".to_string(), log_filters: filters }
    }
}

fn build_env_filter(config: &LoggingConfig) -> EnvFilter {
    let mut filter = EnvFilter::try_from_env("PGD_LOG")
        .unwrap_or_else(|_| EnvFilter::new(&config.log_level));
    for (module, level) in &config.log_filters {
        if let Ok(d) = format!("{module}={level}").parse() {
            filter = filter.add_directive(d);
        }
    }
    filter
}

fn setup_logging() -> anyhow::Result<()> {
    let config = LoggingConfig::default();
    let filter = build_env_filter(&config);
    let use_json = std::env::var("PGD_LOGGING_JSONL")
        .map(|v| v == "1" || v.to_lowercase() == "true")
        .unwrap_or(false);
    if use_json {
        tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().json().with_current_span(true))
            .try_init()
            .map_err(|e| anyhow::anyhow!("logging init failed: {e}"))?;
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().with_ansi(true).with_target(true))
            .try_init()
            .map_err(|e| anyhow::anyhow!("logging init failed: {e}"))?;
    }
    Ok(())
}

/// 初始化全局日志和追踪层（幂等）。
///
/// 环境变量：
/// - `PGD_LOG`：`RUST_LOG` 格式过滤字符串
/// - `PGD_LOGGING_JSONL`：`1` 启用 JSON 格式
pub fn init() {
    INIT_ONCE.call_once(|| {
        if let Err(e) = setup_logging() {
            eprintln!("Failed to initialize logging: {e}");
            std::process::exit(1);
        }
    });
}

// ── W3C Trace Context ──────────────────────────────────────────────

/// W3C `traceparent` 头解析结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceParent {
    pub version: u8,
    pub trace_id: String,
    pub parent_id: String,
    pub trace_flags: u8,
}

impl TraceParent {
    /// 解析 `{version}-{trace_id}-{parent_id}-{flags}` 格式。
    pub fn parse(header_value: &str) -> Option<Self> {
        let parts: Vec<&str> = header_value.trim().splitn(4, '-').collect();
        if parts.len() != 4 { return None; }
        let version = u8::from_str_radix(parts[0], 16).ok()?;
        let trace_id = parts[1];
        let parent_id = parts[2];
        let trace_flags = u8::from_str_radix(parts[3], 16).ok()?;
        if trace_id.len() != 32 || parent_id.len() != 16 { return None; }
        if !trace_id.chars().all(|c| c.is_ascii_hexdigit())
            || !parent_id.chars().all(|c| c.is_ascii_hexdigit())
        { return None; }
        Some(Self { version, trace_id: trace_id.to_string(), parent_id: parent_id.to_string(), trace_flags })
    }

    pub fn to_header_value(&self) -> String {
        format!("{:02x}-{}-{}-{:02x}", self.version, self.trace_id, self.parent_id, self.trace_flags)
    }
}

/// 完整分布式追踪上下文（traceparent + 可选 tracestate）。
#[derive(Debug, Clone)]
pub struct DistributedTraceContext {
    pub traceparent: TraceParent,
    pub tracestate: Option<String>,
}

impl DistributedTraceContext {
    pub fn extract_from<H: GenericHeaders>(headers: &H) -> Option<Self> {
        let traceparent = TraceParent::parse(headers.get("traceparent")?)?;
        let tracestate = headers.get("tracestate").map(|s| s.to_owned());
        Some(Self { traceparent, tracestate })
    }
}

// ── Generic Headers ────────────────────────────────────────────────

pub trait GenericHeaders {
    fn get(&self, key: &str) -> Option<&str>;
}

impl GenericHeaders for http::HeaderMap {
    fn get(&self, key: &str) -> Option<&str> {
        self.get(key).and_then(|v| v.to_str().ok())
    }
}

impl GenericHeaders for HashMap<String, String> {
    fn get(&self, key: &str) -> Option<&str> {
        HashMap::get(self, key).map(|v| v.as_str())
    }
}

// ── Span Helpers ───────────────────────────────────────────────────

pub fn make_request_span<H: GenericHeaders>(method: &str, path: &str, headers: &H) -> tracing::Span {
    let _ctx = DistributedTraceContext::extract_from(headers);
    tracing::info_span!("request", otel.kind = "server", http.method = method, http.route = path)
}

pub fn make_handle_payload_span(service_name: &str, port_name: &str) -> tracing::Span {
    tracing::info_span!("handle_payload", otel.kind = "internal", pagoda.service = service_name, pagoda.port = port_name)
}

// ── Python FFI ─────────────────────────────────────────────────────

pub fn log_message(level: &str, target: &str, message: &str) {
    match level {
        "trace" => tracing::trace!(target: "python", python_target = target, "{}", message),
        "debug" => tracing::debug!(target: "python", python_target = target, "{}", message),
        "info"  => tracing::info!(target: "python",  python_target = target, "{}", message),
        "warn"  => tracing::warn!(target: "python",  python_target = target, "{}", message),
        "error" => tracing::error!(target: "python", python_target = target, "{}", message),
        _       => tracing::info!(target: "python",  python_target = target, "{}", message),
    }
}
