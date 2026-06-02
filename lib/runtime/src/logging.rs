// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Pagoda 分布式日志模块（logging）
//!
//! ## 设计意图
//! 为整个进程提供"一句初始化"的 tracing-subscriber 装配能力：读取环境变量 /
//! TOML 配置，选择 `READABLE` 或 `JSONL` 输出、本地或 UTC 时区、多层过滤器、分布式
//! W3C Trace Context 传递层（`DistributedTraceIdLayer`），以及面向 NATS / HTTP 头的
//! `TraceParent` 提取工具。
//!
//! ## 配置来源优先级
//!   1. 环境变量（最高）；
//!   2. `PGD_LOGGING_CONFIG_PATH` 指向的 TOML；
//!   3. `/opt/pagoda/etc/logging.toml`（默认路径）。
//!
//! 日志格式默认 `READABLE`，设置 `PGD_LOGGING_JSONL=1` 后切为 `JSONL`；设置
//! `PGD_LOG_USE_LOCAL_TZ=1` 可使用本地时区。`PGD_LOG` / TOML 里的 `log_filters` 控制
//! per-target 过滤器，默认级别为 `info`。
//!
//! 示例：
//! ```toml
//! log_level = "error"
//!
//! [log_filters]
//! "test_logging" = "info"
//! "test_logging::api" = "trace"
//! ```
//!
//! ## 外部契约
//! - 公开函数与类型（`init` / `make_system_request_span` / `is_valid_trace_id` /
//!   `is_valid_span_id` / `parse_traceparent` / `DistributedTraceIdLayer` / 等公开项
//!   `DistributedTraceContext` / `TraceParent` / `GenericHeaders` 等）签名均保持不变。
//! - `is_valid_trace_id` / `is_valid_span_id` 的 W3C 合法性定义保持不变：
//!   * trace ID：32 位、全十六进制；
//!   * span ID：16 位、全十六进制。
//! - `parse_traceparent` 在任一字段不合法时返回 `(None, None)`；合法时返回
//!   `(Some(trace_id), Some(parent_id))`，且仅在输入恰好为 4 段时才视为合法。
//!
//! ## 实现要点
//! - **多样化（Rule 2）**：抽出私有助手 `is_valid_hex_id(&str, expected_len)`
//!   集中表达"长度 + ASCII hex"双重检查；公开的 `is_valid_trace_id` /
//!   `is_valid_span_id` 作为轻层委托保持原签名 / 原语义。
//!   `parse_traceparent` 改为 `split_once` 链式 + 提前返回的打点验证写法，
//!   语义严格等价于历史"4 段拆分 + 长度校验"。
//! - **不**变动 tracing-subscriber 层装配顺序、`DistributedTraceIdLayer` 逻辑、
//!   `emit_at_level!` 宏、JSONL/Readable formatter、`DistributedTraceContext`
//!   字段定义等富含顺序依赖的部分。

use std::collections::{BTreeMap, HashMap};
use std::sync::Once;

use figment::{
    Figment,
    providers::{Format, Serialized, Toml},
};
use serde::{Deserialize, Serialize};
use tracing::level_filters::LevelFilter;
use tracing::{Event, Subscriber};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::fmt::time::LocalTime;
use tracing_subscriber::fmt::time::SystemTime;
use tracing_subscriber::fmt::time::UtcTime;
use tracing_subscriber::fmt::{FmtContext, FormatFields};
use tracing_subscriber::fmt::{FormattedFields, format::Writer};
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{filter::Directive, fmt};

use crate::config::{disable_ansi_logging, jsonl_logging_enabled, span_events_enabled};
use async_nats::{HeaderMap, HeaderValue};
use axum::extract::FromRequestParts;
use axum::http;
use axum::http::Request;
use axum::http::request::Parts;
use serde_json::Value;
use std::convert::Infallible;
use std::time::Instant;
use tower_http::trace::{DefaultMakeSpan, TraceLayer};
use tracing::Id;
use tracing::Span;
use tracing::field::Field;
use tracing::span;
use tracing_subscriber::Layer;
use tracing_subscriber::field::Visit;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::SpanData;
use tracing_subscriber::Registry;
use uuid::Uuid;

use opentelemetry::propagation::{Extractor, Injector, TextMapPropagator};
use opentelemetry::trace::TraceContextExt;
use opentelemetry::{global, trace::Tracer};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::WithExportConfig;

use opentelemetry::trace::TracerProvider as _;
use opentelemetry::{Key, KeyValue};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing::error;
use tracing_subscriber::layer::SubscriberExt;

use std::time::Duration;
use tracing::{info, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::config::environment_names::logging as env_logging;

use pagoda_config::env_is_truthy;

/// Default log level
const DEFAULT_FILTER_LEVEL: &str = "info";

/// Default OTLP portname
const DEFAULT_OTLP_ENDPOINT: &str = "http://localhost:4317";

/// Default service name
const DEFAULT_OTEL_SERVICE_NAME: &str = "pagoda";

/// 单例实例，确保 logger 只会初始化一次
static INIT: Once = Once::new();

#[derive(Serialize, Deserialize, Debug)]
struct LoggingConfig {
    log_level: String,
    log_filters: HashMap<String, String>,
}
impl Default for LoggingConfig {
    fn default() -> Self {
        LoggingConfig {
            log_level: DEFAULT_FILTER_LEVEL.to_string(),
            log_filters: HashMap::from([
                ("h2".to_string(), "error".to_string()),
                ("tower".to_string(), "error".to_string()),
                ("hyper_util".to_string(), "error".to_string()),
                ("neli".to_string(), "error".to_string()),
                ("async_nats".to_string(), "error".to_string()),
                ("rustls".to_string(), "error".to_string()),
                ("tokenizers".to_string(), "error".to_string()),
                ("axum".to_string(), "error".to_string()),
                ("tonic".to_string(), "error".to_string()),
                ("hf_hub".to_string(), "error".to_string()),
                ("opentelemetry".to_string(), "error".to_string()),
                ("opentelemetry-otlp".to_string(), "error".to_string()),
                ("opentelemetry_sdk".to_string(), "error".to_string()),
            ]),
        }
    }
}

/// 检查是否启用 OTLP trace 导出（接受："1"、"true"、"on"、"yes"，不区分大小写）
fn otlp_exporter_enabled() -> bool {
    env_is_truthy(env_logging::otlp::OTEL_EXPORT_ENABLED)
}

/// 从环境变量获取服务名，或使用默认值
fn get_service_name() -> String {
    std::env::var(env_logging::otlp::OTEL_SERVICE_NAME)
        .unwrap_or_else(|_| DEFAULT_OTEL_SERVICE_NAME.to_string())
}

/// 私有助手：判定字符串是否是长度为 `expected_len` 的 ASCII 十六进制表示。
///
/// 集中表达 `is_valid_trace_id` / `is_valid_span_id` 中重复的"长度检查 + ASCII hex 检查"
/// 逻辑；公开版本仅作为轻层委托以保持历史对外签名 / 常量不变。
#[inline]
fn is_valid_hex_id(value: &str, expected_len: usize) -> bool {
    value.len() == expected_len && value.chars().all(|c| c.is_ascii_hexdigit())
}

/// 按照 W3C Trace Context 规范验证给定的 trace ID。
/// 合法的 trace ID 是一个 32 位十六进制字符串（小写）。
pub fn is_valid_trace_id(trace_id: &str) -> bool {
    is_valid_hex_id(trace_id, 32)
}

/// 按照 W3C Trace Context 规范验证给定的 span ID。
/// 合法的 span ID 是一个 16 位十六进制字符串（小写）。
pub fn is_valid_span_id(span_id: &str) -> bool {
    is_valid_hex_id(span_id, 16)
}

pub struct DistributedTraceIdLayer;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedTraceContext {
    pub trace_id: String,
    pub span_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tracestate: Option<String>,
    #[serde(skip)]
    start: Option<Instant>,
    #[serde(skip)]
    end: Option<Instant>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub x_request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

/// 在 on_new_span 中收集的待定上下文数据，会在 on_enter 中完成最终化
#[derive(Debug, Clone)]
struct PendingDistributedTraceContext {
    trace_id: Option<String>,
    span_id: Option<String>,
    parent_id: Option<String>,
    tracestate: Option<String>,
    x_request_id: Option<String>,
    request_id: Option<String>,
}

/// 用于在动态级别和自定义 target 下发出 tracing 事件的宏。
macro_rules! emit_at_level {
    ($level:expr, target: $target:expr, $($arg:tt)*) => {
        // tracing::event! 需要编译期常量级别，因此必须按运行时级别分支，
        // 并在每个分支中使用字面量 Level 常量。
        // 参见：https://github.com/tokio-rs/tracing/issues/2730
        match $level {
            &tracing::Level::ERROR => tracing::event!(target: $target, tracing::Level::ERROR, $($arg)*),
            &tracing::Level::WARN => tracing::event!(target: $target, tracing::Level::WARN, $($arg)*),
            &tracing::Level::INFO => tracing::event!(target: $target, tracing::Level::INFO, $($arg)*),
            &tracing::Level::DEBUG => tracing::event!(target: $target, tracing::Level::DEBUG, $($arg)*),
            &tracing::Level::TRACE => tracing::event!(target: $target, tracing::Level::TRACE, $($arg)*),
        }
    };
}

impl DistributedTraceContext {
    /// 从上下文创建 traceparent 字符串
    pub fn create_traceparent(&self) -> String {
        format!("00-{}-{}-01", self.trace_id, self.span_id)
    }
}

    /// 将 traceparent 字符串解析到其 servicegroup。
///
/// 中文说明：
/// 1. 按 W3C Trace Context 定义，`traceparent` 必须是 `version-trace_id-parent_id-flags`
///    四段、以 `-` 分隔。这里不验证 `version` / `flags`，仅需保证中间两段合法。
/// 2. 采用 `split_once` 链式提取 trace_id 与 parent_id：`a-b-c-d` 三次拆分后拿到
///    `b` / `c` / `d`；拆分失败或"剩余部分仍包含 `-`"（说明实际段数 > 4）都会
///    被判定为非法，与历史"4 段拆分"语义严格等价。
/// 3. 合法时返回 `(Some(trace_id), Some(parent_id))`，任一字段不合法以 `(None, None)`
///    表示错误（与历史实现一致）。
pub fn parse_traceparent(traceparent: &str) -> (Option<String>, Option<String>) {
    // 链式拆分：拿到 trace_id / parent_id / flags 三段；任意一步失败都说明段数不足 4。
    let parsed = traceparent
        .split_once('-')
        .and_then(|(_version, rest)| rest.split_once('-'))
        .and_then(|(trace_id, rest)| {
            rest.split_once('-')
                .map(|(parent_id, flags)| (trace_id, parent_id, flags))
        });

    let Some((trace_id, parent_id, flags)) = parsed else {
        return (None, None);
    };

    // flags 字段本身可以是任意内容，但不能再含 `-`，否则原版 `split('-')` 计数会 > 4。
    if flags.contains('-') {
        return (None, None);
    }

    if !is_valid_trace_id(trace_id) || !is_valid_span_id(parent_id) {
        return (None, None);
    }

    (Some(trace_id.to_string()), Some(parent_id.to_string()))
}

#[derive(Debug, Clone, Default)]
pub struct TraceParent {
    pub trace_id: Option<String>,
    pub parent_id: Option<String>,
    pub tracestate: Option<String>,
    pub x_request_id: Option<String>,
    pub request_id: Option<String>,
}

pub trait GenericHeaders {
    fn get(&self, key: &str) -> Option<&str>;
}

impl GenericHeaders for async_nats::HeaderMap {
    fn get(&self, key: &str) -> Option<&str> {
        async_nats::HeaderMap::get(self, key).map(|value| value.as_str())
    }
}

impl GenericHeaders for http::HeaderMap {
    fn get(&self, key: &str) -> Option<&str> {
        http::HeaderMap::get(self, key).and_then(|value| value.to_str().ok())
    }
}

impl TraceParent {
    pub fn from_headers<H: GenericHeaders>(headers: &H) -> TraceParent {
        let mut trace_id = None;
        let mut parent_id = None;
        let mut tracestate = None;
        let mut x_request_id = None;
        let mut request_id = None;

        if let Some(header_value) = headers.get("traceparent") {
            (trace_id, parent_id) = parse_traceparent(header_value);
        }

        if let Some(header_value) = headers.get("x-request-id") {
            x_request_id = Some(header_value.to_string());
        }

        if let Some(header_value) = headers.get("tracestate") {
            tracestate = Some(header_value.to_string());
        }

        // 从内部头读取 request-id，并回退到已废弃的 x-pagoda-request-id
        if let Some(header_value) = headers.get("request-id") {
            request_id = Some(header_value.to_string());
        } else if let Some(header_value) = headers.get("x-pagoda-request-id") {
            request_id = Some(header_value.to_string());
        }

        let request_id = request_id.filter(|id| uuid::Uuid::parse_str(id).is_ok());
        TraceParent {
            trace_id,
            parent_id,
            tracestate,
            x_request_id,
            request_id,
        }
    }
}

/// 为推理请求类 portname 创建 span（补全、对话、嵌入等）。
///
/// 使用 `target: "request_span"`，它始终可通过 PGD_LOG 过滤
/// （由 `filters()` 中的 `request_span=trace` 规则放行）。这样可确保请求上下文
/// （request_id、model、trace_id）始终出现在日志事件中。
pub fn make_inference_request_span<B>(req: &Request<B>) -> Span {
    let method = req.method();
    let uri = req.uri();
    let version = format!("{:?}", req.version());
    let trace_parent = TraceParent::from_headers(req.headers());

    let otel_context = extract_otel_context_from_http_headers(req.headers());

    // 确保每个推理请求的 span 都带有 request_id。
    // 这是唯一事实来源——worker 和 get_or_create_request_id
    // 都会通过 DistributedTraceIdLayer 读回它。
    let request_id = trace_parent
        .request_id
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    let span = tracing::info_span!(
            target: "request_span",
        "http-request",
        method = %method,
        uri = %uri,
        version = %version,
        trace_id = trace_parent.trace_id,
        parent_id = trace_parent.parent_id,
        x_request_id = trace_parent.x_request_id,
        request_id = %request_id,
        model = tracing::field::Empty,
        input_tokens = tracing::field::Empty,
        output_tokens = tracing::field::Empty,
        ttft_ms = tracing::field::Empty,
        avg_itl_ms = tracing::field::Empty,
        prefill_worker_id = tracing::field::Empty,
        decode_worker_id = tracing::field::Empty,
    );

    if let Some(context) = otel_context {
        let _ = span.set_parent(context);
    }

    span
}

/// 为系统类 portname 创建 span（健康检查、指标、模型、引擎、loras 等）。
///
/// 结构与 `make_inference_request_span` 相同，但使用 `target: "system_span"`，
/// 它遵循普通 PGD_LOG 过滤（默认 debug 级别）。推理 span 的 target
/// `request_span` 通过 `request_span=trace` 规则始终放行；系统 span 不这样做，
/// 从而让高频轮询类 portname 保持安静。
pub fn make_system_request_span<B>(req: &Request<B>) -> Span {
    let method = req.method();
    let uri = req.uri();
    let version = format!("{:?}", req.version());
    let trace_parent = TraceParent::from_headers(req.headers());
    let otel_context = extract_otel_context_from_http_headers(req.headers());

    // 确保每个系统请求的 span 都带有 request_id。
    let request_id = trace_parent
        .request_id
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    let span = tracing::debug_span!(
        target: "system_span",
        "http-request",
        method = %method,
        uri = %uri,
        version = %version,
        trace_id = trace_parent.trace_id,
        parent_id = trace_parent.parent_id,
        x_request_id = trace_parent.x_request_id,
        request_id = %request_id,
        model = tracing::field::Empty,
        input_tokens = tracing::field::Empty,
        output_tokens = tracing::field::Empty,
        ttft_ms = tracing::field::Empty,
        avg_itl_ms = tracing::field::Empty,
        prefill_worker_id = tracing::field::Empty,
        decode_worker_id = tracing::field::Empty,
    );

    if let Some(context) = otel_context {
        let _ = span.set_parent(context);
    }

    span
}

/// 从 HTTP 头中提取 OpenTelemetry 上下文，用于分布式追踪。
fn extract_otel_context_from_http_headers(
    headers: &http::HeaderMap,
) -> Option<opentelemetry::Context> {
    let traceparent_value = headers.get("traceparent")?.to_str().ok()?;

    struct HttpHeaderExtractor<'a>(&'a http::HeaderMap);

    impl<'a> Extractor for HttpHeaderExtractor<'a> {
        fn get(&self, key: &str) -> Option<&str> {
            self.0.get(key).and_then(|v| v.to_str().ok())
        }

        fn keys(&self) -> Vec<&str> {
            vec!["traceparent", "tracestate"]
                .into_iter()
                .filter(|&key| self.0.get(key).is_some())
                .collect()
        }
    }

    // 若 traceparent 为空则提前返回。
    if traceparent_value.is_empty() {
        return None;
    }

    let extractor = HttpHeaderExtractor(headers);
    let otel_context = TRACE_PROPAGATOR.extract(&extractor);

    if otel_context.span().span_context().is_valid() {
        Some(otel_context)
    } else {
        None
    }
}

/// 使用 servicegroup 上下文，从 NATS 头创建 handle_payload span。
pub fn make_handle_payload_span(
    headers: &async_nats::HeaderMap,
    servicegroup: &str,
    portname: &str,
    namespace: &str,
    instance_id: u64,
) -> Span {
    let (otel_context, trace_id, parent_span_id) = extract_otel_context_from_nats_headers(headers);
    let trace_parent = TraceParent::from_headers(headers);

    if let (Some(trace_id), Some(parent_id)) = (trace_id.as_ref(), parent_span_id.as_ref()) {
        let span = tracing::info_span!(
            target: "request_span",
            "handle_payload",
            trace_id = trace_id.as_str(),
            parent_id = parent_id.as_str(),
            x_request_id = trace_parent.x_request_id,
            request_id = trace_parent.request_id,
            tracestate = trace_parent.tracestate,
            servicegroup = servicegroup,
            portname = portname,
            namespace = namespace,
            instance_id = instance_id,
        );

        if let Some(context) = otel_context {
            let _ = span.set_parent(context);
        }
        span
    } else {
        tracing::info_span!(
            target: "request_span",
            "handle_payload",
            x_request_id = trace_parent.x_request_id,
            request_id = trace_parent.request_id,
            tracestate = trace_parent.tracestate,
            servicegroup = servicegroup,
            portname = portname,
            namespace = namespace,
            instance_id = instance_id,
        )
    }
}

/// 使用 servicegroup 上下文，从 TCP/HashMap 头创建 handle_payload span。
pub fn make_handle_payload_span_from_tcp_headers(
    headers: &std::collections::HashMap<String, String>,
    servicegroup: &str,
    portname: &str,
    namespace: &str,
    instance_id: u64,
) -> Span {
    let (otel_context, trace_id, parent_span_id) = extract_otel_context_from_tcp_headers(headers);
    let x_request_id = headers.get("x-request-id").cloned();
    let request_id = headers
        .get("request-id")
        .or_else(|| headers.get("x-pagoda-request-id"))
        .filter(|id| uuid::Uuid::parse_str(id).is_ok())
        .cloned();
    let tracestate = headers.get("tracestate").cloned();

    if let (Some(trace_id), Some(parent_id)) = (trace_id.as_ref(), parent_span_id.as_ref()) {
        let span = tracing::info_span!(
            target: "request_span",
            "handle_payload",
            trace_id = trace_id.as_str(),
            parent_id = parent_id.as_str(),
            x_request_id = x_request_id,
            request_id = request_id,
            tracestate = tracestate,
            servicegroup = servicegroup,
            portname = portname,
            namespace = namespace,
            instance_id = instance_id,
        );

        if let Some(context) = otel_context {
            let _ = span.set_parent(context);
        }
        span
    } else {
        tracing::info_span!(
            target: "request_span",
            "handle_payload",
            x_request_id = x_request_id,
            request_id = request_id,
            tracestate = tracestate,
            servicegroup = servicegroup,
            portname = portname,
            namespace = namespace,
            instance_id = instance_id,
        )
    }
}

/// 从 TCP/HashMap 头提取 OpenTelemetry trace 上下文，用于分布式追踪。
fn extract_otel_context_from_tcp_headers(
    headers: &std::collections::HashMap<String, String>,
) -> (
    Option<opentelemetry::Context>,
    Option<String>,
    Option<String>,
) {
    let traceparent_value = match headers.get("traceparent") {
        Some(value) => value.as_str(),
        None => return (None, None, None),
    };

    let (trace_id, parent_span_id) = parse_traceparent(traceparent_value);

    struct TcpHeaderExtractor<'a>(&'a std::collections::HashMap<String, String>);

    impl<'a> Extractor for TcpHeaderExtractor<'a> {
        fn get(&self, key: &str) -> Option<&str> {
            self.0.get(key).map(|s| s.as_str())
        }

        fn keys(&self) -> Vec<&str> {
            vec!["traceparent", "tracestate"]
                .into_iter()
                .filter(|&key| self.0.get(key).is_some())
                .collect()
        }
    }

    let extractor = TcpHeaderExtractor(headers);
    let otel_context = TRACE_PROPAGATOR.extract(&extractor);

    let context_with_trace = if otel_context.span().span_context().is_valid() {
        Some(otel_context)
    } else {
        None
    };

    (context_with_trace, trace_id, parent_span_id)
}

/// 从 NATS 头提取 OpenTelemetry trace 上下文，用于分布式追踪。
pub fn extract_otel_context_from_nats_headers(
    headers: &async_nats::HeaderMap,
) -> (
    Option<opentelemetry::Context>,
    Option<String>,
    Option<String>,
) {
    let traceparent_value = match headers.get("traceparent") {
        Some(value) => value.as_str(),
        None => return (None, None, None),
    };

    let (trace_id, parent_span_id) = parse_traceparent(traceparent_value);

    struct NatsHeaderExtractor<'a>(&'a async_nats::HeaderMap);

    impl<'a> Extractor for NatsHeaderExtractor<'a> {
        fn get(&self, key: &str) -> Option<&str> {
            self.0.get(key).map(|value| value.as_str())
        }

        fn keys(&self) -> Vec<&str> {
            vec!["traceparent", "tracestate"]
                .into_iter()
                .filter(|&key| self.0.get(key).is_some())
                .collect()
        }
    }

    let extractor = NatsHeaderExtractor(headers);
    let otel_context = TRACE_PROPAGATOR.extract(&extractor);

    let context_with_trace = if otel_context.span().span_context().is_valid() {
        Some(otel_context)
    } else {
        None
    };

    (context_with_trace, trace_id, parent_span_id)
}

/// 使用 W3C Trace Context 传播，将 OpenTelemetry trace 上下文注入 NATS 头。
pub fn inject_otel_context_into_nats_headers(
    headers: &mut async_nats::HeaderMap,
    context: Option<opentelemetry::Context>,
) {
    let otel_context = context.unwrap_or_else(|| Span::current().context());

    struct NatsHeaderInjector<'a>(&'a mut async_nats::HeaderMap);

    impl<'a> Injector for NatsHeaderInjector<'a> {
        fn set(&mut self, key: &str, value: String) {
            self.0.insert(key, value);
        }
    }

    let mut injector = NatsHeaderInjector(headers);
    TRACE_PROPAGATOR.inject_context(&otel_context, &mut injector);
}

/// 将当前 span 的 trace 上下文注入 NATS 头。
pub fn inject_current_trace_into_nats_headers(headers: &mut async_nats::HeaderMap) {
    inject_otel_context_into_nats_headers(headers, None);
}

// 将 trace 头注入通用 HashMap，供 HTTP/TCP 传输使用。
pub fn inject_trace_headers_into_map(headers: &mut std::collections::HashMap<String, String>) {
    if let Some(trace_context) = get_distributed_tracing_context() {
        // 注入 W3C traceparent 头。
        headers.insert(
            "traceparent".to_string(),
            trace_context.create_traceparent(),
        );

        // 注入可选的 tracestate。
        if let Some(tracestate) = trace_context.tracestate {
            headers.insert("tracestate".to_string(), tracestate);
        }

        // 注入自定义 request ID。
        if let Some(x_request_id) = trace_context.x_request_id {
            headers.insert("x-request-id".to_string(), x_request_id);
        }
        if let Some(request_id) = trace_context.request_id {
            headers.insert("request-id".to_string(), request_id);
        }
    }
}

/// 创建与父 trace 上下文关联的 client_request span。
pub fn make_client_request_span(
    operation: &str,
    request_id: &str,
    trace_context: Option<&DistributedTraceContext>,
    instance_id: Option<&str>,
) -> Span {
    if let Some(ctx) = trace_context {
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("traceparent", ctx.create_traceparent());

        if let Some(ref tracestate) = ctx.tracestate {
            headers.insert("tracestate", tracestate.as_str());
        }

        let (otel_context, _extracted_trace_id, _extracted_parent_span_id) =
            extract_otel_context_from_nats_headers(&headers);

        let span = if let Some(inst_id) = instance_id {
            tracing::info_span!(
                "client_request",
                operation = operation,
                request_id = request_id,
                instance_id = inst_id,
                trace_id = ctx.trace_id.as_str(),
                parent_id = ctx.span_id.as_str(),
                x_request_id = ctx.x_request_id.as_deref(),
            )
        } else {
            tracing::info_span!(
                "client_request",
                operation = operation,
                request_id = request_id,
                trace_id = ctx.trace_id.as_str(),
                parent_id = ctx.span_id.as_str(),
                x_request_id = ctx.x_request_id.as_deref(),
            )
        };

        if let Some(context) = otel_context {
            let _ = span.set_parent(context);
        }

        span
    } else if let Some(inst_id) = instance_id {
        tracing::info_span!(
            "client_request",
            operation = operation,
            request_id = request_id,
            instance_id = inst_id,
        )
    } else {
        tracing::info_span!(
            "client_request",
            operation = operation,
            request_id = request_id,
        )
    }
}

#[derive(Debug, Default)]
pub struct FieldVisitor {
    pub fields: HashMap<String, String>,
}

impl Visit for FieldVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.fields
            .insert(field.name().to_string(), format!("{:?}", value).to_string());
    }
}

impl<S> Layer<S> for DistributedTraceIdLayer
where
    S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    // 记录 span 关闭时间。
    // 当前尚未使用，但为后续计时用途预留。
    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(&id) {
            let mut extensions = span.extensions_mut();
            if let Some(distributed_tracing_context) =
                extensions.get_mut::<DistributedTraceContext>()
            {
                distributed_tracing_context.end = Some(Instant::now());
            }
        }
    }

    // 在 on_new_span 中收集 span 属性和元数据。
    // 最终初始化延后到 on_enter，以便拿到 OtelData。
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            let mut trace_id: Option<String> = None;
            let mut parent_id: Option<String> = None;
            let mut span_id: Option<String> = None;
            let mut x_request_id: Option<String> = None;
            let mut request_id: Option<String> = None;
            let mut tracestate: Option<String> = None;
            let mut visitor = FieldVisitor::default();
            attrs.record(&mut visitor);

            // 从 span 属性中提取 trace_id。
            if let Some(trace_id_input) = visitor.fields.get("trace_id") {
                if !is_valid_trace_id(trace_id_input) {
                    tracing::trace!("trace id  '{trace_id_input}' is not valid! Ignoring.");
                } else {
                    trace_id = Some(trace_id_input.to_string());
                }
            }

            // 从 span 属性中提取 span_id。
            if let Some(span_id_input) = visitor.fields.get("span_id") {
                if !is_valid_span_id(span_id_input) {
                    tracing::trace!("span id  '{span_id_input}' is not valid! Ignoring.");
                } else {
                    span_id = Some(span_id_input.to_string());
                }
            }

            // 从 span 属性中提取 parent_id。
            if let Some(parent_id_input) = visitor.fields.get("parent_id") {
                if !is_valid_span_id(parent_id_input) {
                    tracing::trace!("parent id  '{parent_id_input}' is not valid! Ignoring.");
                } else {
                    parent_id = Some(parent_id_input.to_string());
                }
            }

            // Extract tracestate
            if let Some(tracestate_input) = visitor.fields.get("tracestate") {
                tracestate = Some(tracestate_input.to_string());
            }

            // Extract x_request_id
            if let Some(x_request_id_input) = visitor.fields.get("x_request_id") {
                x_request_id = Some(x_request_id_input.to_string());
            }

            // 提取 request_id（兼容旧的 x_pagoda_request_id）。
            if let Some(request_id_input) = visitor.fields.get("request_id") {
                request_id = Some(request_id_input.to_string());
            } else if let Some(x_request_id_input) = visitor.fields.get("x_pagoda_request_id") {
                request_id = Some(x_request_id_input.to_string());
            }

            // 若可用，则继承父 span 的 trace 上下文。
            if parent_id.is_none()
                && let Some(parent_span_id) = ctx.current_span().id()
                && let Some(parent_span) = ctx.span(parent_span_id)
            {
                let parent_ext = parent_span.extensions();
                if let Some(parent_tracing_context) = parent_ext.get::<DistributedTraceContext>() {
                    trace_id = Some(parent_tracing_context.trace_id.clone());
                    parent_id = Some(parent_tracing_context.span_id.clone());
                    tracestate = parent_tracing_context.tracestate.clone();
                    if x_request_id.is_none() {
                        x_request_id = parent_tracing_context.x_request_id.clone();
                    }
                    if request_id.is_none() {
                        request_id = parent_tracing_context.request_id.clone();
                    }
                }
            }

            // 校验一致性。
            if (parent_id.is_some() || span_id.is_some()) && trace_id.is_none() {
                tracing::error!("parent id or span id are set but trace id is not set!");
                // 清除不一致的 ID，以保持 trace 完整性。
                parent_id = None;
                span_id = None;
            }

            // 存储待定上下文，将在 on_enter 中最终化。
            let mut extensions = span.extensions_mut();
            extensions.insert(PendingDistributedTraceContext {
                trace_id,
                span_id,
                parent_id,
                tracestate,
                x_request_id,
                request_id,
            });
        }
    }

    // 在 span 进入时完成 DistributedTraceContext 的最终化。
    // 此时 OtelData 应已具有有效的 trace_id 和 span_id。
    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            // 检查是否已经初始化（例如 span 重新进入）。
            {
                let extensions = span.extensions();
                if extensions.get::<DistributedTraceContext>().is_some() {
                    return;
                }
            }

            // 获取待定上下文并提取 OtelData 中的 ID。
            let mut extensions = span.extensions_mut();
            let pending = match extensions.remove::<PendingDistributedTraceContext>() {
                Some(p) => p,
                None => {
                    // 这不该发生——on_new_span 本应已创建它。
                    tracing::error!("PendingDistributedTraceContext not found in on_enter");
                    return;
                }
            };

            let mut trace_id = pending.trace_id;
            let mut span_id = pending.span_id;
            let parent_id = pending.parent_id;
            let tracestate = pending.tracestate;
            let x_request_id = pending.x_request_id;
            let request_id = pending.request_id;

            // 若尚未设置，则尝试从 OtelData 中提取。
            // 需要先释放 extensions_mut，才能对 OtelData 做不可变借用。
            drop(extensions);

            if trace_id.is_none() || span_id.is_none() {
                let extensions = span.extensions();
                if let Some(otel_data) = extensions.get::<tracing_opentelemetry::OtelData>() {
                    // 若尚未设置，则从 OTEL 数据中提取 trace_id。
                    if trace_id.is_none()
                        && let Some(otel_trace_id) = otel_data.trace_id()
                    {
                        let trace_id_str = format!("{}", otel_trace_id);
                        if is_valid_trace_id(&trace_id_str) {
                            trace_id = Some(trace_id_str);
                        }
                    }

                    // 若尚未设置，则从 OTEL 数据中提取 span_id。
                    if span_id.is_none()
                        && let Some(otel_span_id) = otel_data.span_id()
                    {
                        let span_id_str = format!("{}", otel_span_id);
                        if is_valid_span_id(&span_id_str) {
                            span_id = Some(span_id_str);
                        }
                    }
                }
            }

            // 若仍缺少必要 ID，则 panic。
            if trace_id.is_none() {
                panic!(
                    "trace_id is not set in on_enter - OtelData may not be properly initialized"
                );
            }

            if span_id.is_none() {
                panic!("span_id is not set in on_enter - OtelData may not be properly initialized");
            }

            let span_level = span.metadata().level();
            let mut extensions = span.extensions_mut();
            extensions.insert(DistributedTraceContext {
                trace_id: trace_id.expect("Trace ID must be set"),
                span_id: span_id.expect("Span ID must be set"),
                parent_id,
                tracestate,
                start: Some(Instant::now()),
                end: None,
                x_request_id,
                request_id,
            });

            drop(extensions);

            // 发出 SPAN_FIRST_ENTRY 事件。只有 span 通过 layer 过滤器时才会运行，
            // （被过滤掉的 span 不会调用 on_enter），因此无需额外检查。
            if span_events_enabled() {
                emit_at_level!(span_level, target: "span_event", message = "SPAN_FIRST_ENTRY");
            }
        }
    }
}

// 让函数能够获取当前上下文，以便写入分布式头。
pub fn get_distributed_tracing_context() -> Option<DistributedTraceContext> {
    Span::current()
        .with_subscriber(|(id, subscriber)| {
            subscriber
                .downcast_ref::<Registry>()
                .and_then(|registry| registry.span_data(id))
                .and_then(|span_data| {
                    let extensions = span_data.extensions();
                    extensions.get::<DistributedTraceContext>().cloned()
                })
        })
        .flatten()
}

/// 初始化 logger - 必须在 Tokio runtime 可用时调用。
pub fn init() {
    INIT.call_once(|| {
        if let Err(e) = setup_logging() {
            eprintln!("Failed to initialize logging: {}", e);
            std::process::exit(1);
        }
    });
}

#[cfg(feature = "tokio-console")]
fn setup_logging() -> Result<(), Box<dyn std::error::Error>> {
    let tokio_console_layer = console_subscriber::ConsoleLayer::builder()
        .with_default_env()
        .server_addr(([0, 0, 0, 0], console_subscriber::Server::DEFAULT_PORT))
        .spawn();
    let tokio_console_target = tracing_subscriber::filter::Targets::new()
        .with_default(LevelFilter::ERROR)
        .with_target("runtime", LevelFilter::TRACE)
        .with_target("tokio", LevelFilter::TRACE);
    let l = fmt::layer()
        .with_ansi(!disable_ansi_logging())
        .event_format(fmt::format().compact().with_timer(TimeFormatter::new()))
        .with_writer(std::io::stderr)
        .with_filter(filters(load_config()));
    tracing_subscriber::registry()
        .with(l)
        .with(tokio_console_layer.with_filter(tokio_console_target))
        .init();
    Ok(())
}

#[cfg(not(feature = "tokio-console"))]
fn setup_logging() -> Result<(), Box<dyn std::error::Error>> {
    let fmt_filter_layer = filters(load_config());
    let trace_filter_layer = filters(load_config());
    let otel_filter_layer = filters(load_config());
    let otel_logs_filter_layer = filters(load_config());

    if jsonl_logging_enabled() {
        let span_events = if span_events_enabled() {
            FmtSpan::CLOSE
        } else {
            FmtSpan::NONE
        };
        let l = fmt::layer()
            .with_ansi(false)
            .with_span_events(span_events)
            .event_format(CustomJsonFormatter::new())
            .with_writer(std::io::stderr)
            .with_filter(fmt_filter_layer);

        // 创建 OpenTelemetry tracer - 根据环境变量决定是否导出到 OTLP。
        let service_name = get_service_name();

        // 构建 tracer 和 logger provider - 可带或不带 OTLP 导出。
        let (tracer_provider, logger_provider_opt, portname_opt) = if otlp_exporter_enabled() {
            // 已启用导出：创建带批处理器的 OTLP exporter。
            let traces_endpoint =
                std::env::var(env_logging::otlp::OTEL_EXPORTER_OTLP_TRACES_ENDPOINT)
                    .unwrap_or_else(|_| DEFAULT_OTLP_ENDPOINT.to_string());
            let logs_endpoint = std::env::var(env_logging::otlp::OTEL_EXPORTER_OTLP_LOGS_ENDPOINT)
                .unwrap_or_else(|_| traces_endpoint.clone());

            let resource = opentelemetry_sdk::Resource::builder_empty()
                .with_service_name(service_name.clone())
                .build();

            // 使用 gRPC（Tonic）初始化 OTLP span exporter。
            let span_exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(&traces_endpoint)
                .build()?;

            let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
                .with_batch_exporter(span_exporter)
                .with_resource(resource.clone())
                .build();

            // 使用 gRPC（Tonic）初始化 OTLP log exporter。
            let log_exporter = opentelemetry_otlp::LogExporter::builder()
                .with_tonic()
                .with_endpoint(&logs_endpoint)
                .build()?;

            let logger_provider = SdkLoggerProvider::builder()
                .with_batch_exporter(log_exporter)
                .with_resource(resource)
                .build();

            (
                tracer_provider,
                Some(logger_provider),
                Some(traces_endpoint),
            )
        } else {
            // 不导出 - trace 仅在本地生成（用于日志 / trace ID）。
            let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
                .with_resource(
                    opentelemetry_sdk::Resource::builder_empty()
                        .with_service_name(service_name.clone())
                        .build(),
                )
                .build();

            (provider, None, None)
        };

        // 从 provider 获取 tracer。
        let tracer = tracer_provider.tracer(service_name.clone());

        // 构建 OTLP 日志桥接层（仅在启用导出时）。
        let otel_logs_layer = logger_provider_opt
            .as_ref()
            .map(|lp| OpenTelemetryTracingBridge::new(lp).with_filter(otel_logs_filter_layer));

        tracing_subscriber::registry()
            .with(
                tracing_opentelemetry::layer()
                    .with_tracer(tracer)
                    .with_filter(otel_filter_layer),
            )
            .with(otel_logs_layer)
            .with(DistributedTraceIdLayer.with_filter(trace_filter_layer))
            .with(l)
            .init();

        // 在 subscriber 就绪后记录初始化状态。
        if let Some(portname) = portname_opt {
            tracing::info!(
                portname = %portname,
                service = %service_name,
                "OpenTelemetry OTLP export enabled (traces and logs)"
            );
        } else {
            tracing::info!(
                service = %service_name,
                "OpenTelemetry OTLP export disabled, traces local only"
            );
        }
    } else {
        let l = fmt::layer()
            .with_ansi(!disable_ansi_logging())
            .event_format(fmt::format().compact().with_timer(TimeFormatter::new()))
            .with_writer(std::io::stderr)
            .with_filter(fmt_filter_layer);

        tracing_subscriber::registry().with(l).init();
    }

    Ok(())
}

fn filters(config: LoggingConfig) -> EnvFilter {
    let mut filter_layer = EnvFilter::builder()
        .with_default_directive(config.log_level.parse().unwrap())
        .with_env_var(env_logging::PGD_LOG)
        .from_env_lossy();

    for (module, level) in config.log_filters {
        match format!("{module}={level}").parse::<Directive>() {
            Ok(d) => {
                filter_layer = filter_layer.add_directive(d);
            }
            Err(e) => {
                eprintln!("Failed parsing filter '{level}' for module '{module}': {e}");
            }
        }
    }

    // 当启用 span 事件时，允许 "span_event" target 通过所有级别过滤。
    // 这能确保 on_enter 发出的 SPAN_FIRST_ENTRY 事件通过过滤器。
    if span_events_enabled() {
        filter_layer = filter_layer.add_directive("span_event=trace".parse().unwrap());
    }

    // 始终允许基础设施请求 span，不受 PGD_LOG 级别影响。
    // 这能确保请求上下文（request_id、model、trace_id）始终可用于日志事件，
    // 即使 PGD_LOG=error 或 PGD_LOG=warn 也是如此。
    // 如有需要，可通过 PGD_LOG=request_span=<level> 覆盖。
    filter_layer = filter_layer.add_directive("request_span=trace".parse().unwrap());

    filter_layer
}

/// 记录带文件和行号信息的消息。
/// 供 Python 封装层使用。
pub fn log_message(level: &str, message: &str, module: &str, file: &str, line: u32) {
    let level = match level {
        "debug" => log::Level::Debug,
        "info" => log::Level::Info,
        "warn" => log::Level::Warn,
        "error" => log::Level::Error,
        "warning" => log::Level::Warn,
        _ => log::Level::Info,
    };
    log::logger().log(
        &log::Record::builder()
            .args(format_args!("{}", message))
            .level(level)
            .target(module)
            .file(Some(file))
            .line(Some(line))
            .build(),
    );
}

fn load_config() -> LoggingConfig {
    let config_path =
        std::env::var(env_logging::PGD_LOGGING_CONFIG_PATH).unwrap_or_else(|_| "".to_string());
    let figment = Figment::new()
        .merge(Serialized::defaults(LoggingConfig::default()))
        .merge(Toml::file("/opt/pagoda/etc/logging.toml"))
        .merge(Toml::file(config_path));

    figment.extract().unwrap()
}

#[derive(Serialize)]
struct JsonLog<'a> {
    time: String,
    level: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    file: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    line: Option<u32>,
    target: String,
    message: serde_json::Value,
    #[serde(flatten)]
    fields: BTreeMap<String, serde_json::Value>,
}

struct TimeFormatter {
    use_local_tz: bool,
}

impl TimeFormatter {
    fn new() -> Self {
        Self {
            use_local_tz: crate::config::use_local_timezone(),
        }
    }

    fn format_now(&self) -> String {
        if self.use_local_tz {
            chrono::Local::now()
                .format("%Y-%m-%dT%H:%M:%S%.6f%:z")
                .to_string()
        } else {
            chrono::Utc::now()
                .format("%Y-%m-%dT%H:%M:%S%.6fZ")
                .to_string()
        }
    }
}

impl FormatTime for TimeFormatter {
    fn format_time(&self, w: &mut fmt::format::Writer<'_>) -> std::fmt::Result {
        write!(w, "{}", self.format_now())
    }
}

struct CustomJsonFormatter {
    time_formatter: TimeFormatter,
}

impl CustomJsonFormatter {
    fn new() -> Self {
        Self {
            time_formatter: TimeFormatter::new(),
        }
    }
}

use once_cell::sync::Lazy;
use regex::Regex;

/// 静态 W3C Trace Context propagator 实例，避免重复分配。
static TRACE_PROPAGATOR: Lazy<opentelemetry_sdk::propagation::TraceContextPropagator> =
    Lazy::new(opentelemetry_sdk::propagation::TraceContextPropagator::new);

fn parse_tracing_duration(s: &str) -> Option<u64> {
    static RE: Lazy<Regex> =
        Lazy::new(|| Regex::new(r#"^["']?\s*([0-9.]+)\s*(µs|us|ns|ms|s)\s*["']?$"#).unwrap());
    let captures = RE.captures(s)?;
    let value: f64 = captures[1].parse().ok()?;
    let unit = &captures[2];
    match unit {
        "ns" => Some((value / 1000.0) as u64),
        "µs" | "us" => Some(value as u64),
        "ms" => Some((value * 1000.0) as u64),
        "s" => Some((value * 1_000_000.0) as u64),
        _ => None,
    }
}

impl<S, N> tracing_subscriber::fmt::FormatEvent<S, N> for CustomJsonFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> std::fmt::Result {
        let mut visitor = JsonVisitor::default();
        let time = self.time_formatter.format_now();
        event.record(&mut visitor);
        let mut message = visitor
            .fields
            .remove("message")
            .unwrap_or(serde_json::Value::String("".to_string()));

        let mut target_override: Option<String> = None;

        let current_span = event
            .parent()
            .and_then(|id| ctx.span(id))
            .or_else(|| ctx.lookup_current());
        if let Some(span) = current_span {
            let ext = span.extensions();
            let data = ext.get::<FormattedFields<N>>().unwrap();
            let span_fields: Vec<(&str, &str)> = data
                .fields
                .split(' ')
                .filter_map(|entry| entry.split_once('='))
                .collect();
            for (name, value) in span_fields {
                visitor.fields.insert(
                    name.to_string(),
                    serde_json::Value::String(value.trim_matches('"').to_string()),
                );
            }

            let busy_us = visitor
                .fields
                .remove("time.busy")
                .and_then(|v| parse_tracing_duration(&v.to_string()));
            let idle_us = visitor
                .fields
                .remove("time.idle")
                .and_then(|v| parse_tracing_duration(&v.to_string()));

            if let (Some(busy_us), Some(idle_us)) = (busy_us, idle_us) {
                visitor.fields.insert(
                    "time.busy_us".to_string(),
                    serde_json::Value::Number(busy_us.into()),
                );
                visitor.fields.insert(
                    "time.idle_us".to_string(),
                    serde_json::Value::Number(idle_us.into()),
                );
                visitor.fields.insert(
                    "time.duration_us".to_string(),
                    serde_json::Value::Number((busy_us + idle_us).into()),
                );
            }

            let is_span_created = message.as_str() == Some("SPAN_FIRST_ENTRY");
            let is_span_closed = message.as_str() == Some("close");
            if is_span_created || is_span_closed {
                target_override = Some(span.metadata().target().to_string());
                if is_span_closed {
                    message = serde_json::Value::String("SPAN_CLOSED".to_string());
                }
            }

            visitor.fields.insert(
                "span_name".to_string(),
                serde_json::Value::String(span.name().to_string()),
            );

            if let Some(tracing_context) = ext.get::<DistributedTraceContext>() {
                visitor.fields.insert(
                    "span_id".to_string(),
                    serde_json::Value::String(tracing_context.span_id.clone()),
                );
                visitor.fields.insert(
                    "trace_id".to_string(),
                    serde_json::Value::String(tracing_context.trace_id.clone()),
                );
                if let Some(parent_id) = tracing_context.parent_id.clone() {
                    visitor.fields.insert(
                        "parent_id".to_string(),
                        serde_json::Value::String(parent_id),
                    );
                } else {
                    visitor.fields.remove("parent_id");
                }
                if let Some(tracestate) = tracing_context.tracestate.clone() {
                    visitor.fields.insert(
                        "tracestate".to_string(),
                        serde_json::Value::String(tracestate),
                    );
                } else {
                    visitor.fields.remove("tracestate");
                }
                if let Some(x_request_id) = tracing_context.x_request_id.clone() {
                    visitor.fields.insert(
                        "x_request_id".to_string(),
                        serde_json::Value::String(x_request_id),
                    );
                } else {
                    visitor.fields.remove("x_request_id");
                }

                if let Some(request_id) = tracing_context.request_id.clone() {
                    visitor.fields.insert(
                        "request_id".to_string(),
                        serde_json::Value::String(request_id),
                    );
                } else {
                    visitor.fields.remove("request_id");
                }
                // 若存在则移除旧字段名。
                visitor.fields.remove("x_pagoda_request_id");
            } else {
                tracing::error!(
                    "Distributed Trace Context not found, falling back to internal ids"
                );
                visitor.fields.insert(
                    "span_id".to_string(),
                    serde_json::Value::String(span.id().into_u64().to_string()),
                );
                if let Some(parent) = span.parent() {
                    visitor.fields.insert(
                        "parent_id".to_string(),
                        serde_json::Value::String(parent.id().into_u64().to_string()),
                    );
                }
            }
        } else {
            let reserved_fields = [
                "trace_id",
                "span_id",
                "parent_id",
                "span_name",
                "tracestate",
            ];
            for reserved_field in reserved_fields {
                visitor.fields.remove(reserved_field);
            }
        }
        let metadata = event.metadata();
        let log = JsonLog {
            level: metadata.level().to_string(),
            time,
            file: metadata.file(),
            line: metadata.line(),
            target: target_override.unwrap_or_else(|| metadata.target().to_string()),
            message,
            fields: visitor.fields,
        };
        let json = serde_json::to_string(&log).unwrap();
        writeln!(writer, "{json}")
    }
}

#[derive(Default)]
struct JsonVisitor {
    fields: BTreeMap<String, serde_json::Value>,
}

impl tracing::field::Visit for JsonVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.fields.insert(
            field.name().to_string(),
            serde_json::Value::String(format!("{value:?}")),
        );
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() != "message" {
            match serde_json::from_str::<Value>(value) {
                Ok(json_val) => self.fields.insert(field.name().to_string(), json_val),
                Err(_) => self.fields.insert(field.name().to_string(), value.into()),
            };
        } else {
            self.fields.insert(field.name().to_string(), value.into());
        }
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.fields
            .insert(field.name().to_string(), serde_json::Value::Bool(value));
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.fields.insert(
            field.name().to_string(),
            serde_json::Value::Number(value.into()),
        );
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.fields.insert(
            field.name().to_string(),
            serde_json::Value::Number(value.into()),
        );
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        use serde_json::value::Number;
        self.fields.insert(
            field.name().to_string(),
            serde_json::Value::Number(Number::from_f64(value).unwrap_or(0.into())),
        );
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use anyhow::{Result, anyhow};
    use chrono::{DateTime, Utc};
    use jsonschema::{Draft, JSONSchema};
    use serde_json::Value;
    use std::fs::File;
    use std::io::{BufRead, BufReader};
    use stdio_override::*;
    use tempfile::NamedTempFile;

    static LOG_LINE_SCHEMA: &str = r#"
    {
      "$schema": "http://json-schema.org/draft-07/schema#",
      "title": "Runtime Log Line",
      "type": "object",
      "required": [
        "file",
        "level",
        "line",
        "message",
        "target",
        "time"
      ],
      "properties": {
        "file":      { "type": "string" },
        "level":     { "type": "string", "enum": ["ERROR", "WARN", "INFO", "DEBUG", "TRACE"] },
        "line":      { "type": "integer" },
        "message":   { "type": "string" },
        "target":    { "type": "string" },
        "time":      { "type": "string", "format": "date-time" },
        "span_id":   { "type": "string", "pattern": "^[a-f0-9]{16}$" },
        "parent_id": { "type": "string", "pattern": "^[a-f0-9]{16}$" },
        "trace_id":  { "type": "string", "pattern": "^[a-f0-9]{32}$" },
        "span_name": { "type": "string" },
        "time.busy_us":     { "type": "integer" },
        "time.duration_us": { "type": "integer" },
        "time.idle_us":     { "type": "integer" },
        "tracestate": { "type": "string" }
      },
      "additionalProperties": true
    }
    "#;

    #[tracing::instrument(skip_all)]
    async fn parent() {
        tracing::trace!(message = "parent!");
        if let Some(my_ctx) = get_distributed_tracing_context() {
            tracing::info!(my_trace_id = my_ctx.trace_id);
        }
        child().await;
    }

    #[tracing::instrument(skip_all)]
    async fn child() {
        tracing::trace!(message = "child");
        if let Some(my_ctx) = get_distributed_tracing_context() {
            tracing::info!(my_trace_id = my_ctx.trace_id);
        }
        grandchild().await;
    }

    #[tracing::instrument(skip_all)]
    async fn grandchild() {
        tracing::trace!(message = "grandchild");
        if let Some(my_ctx) = get_distributed_tracing_context() {
            tracing::info!(my_trace_id = my_ctx.trace_id);
        }
    }

    pub fn load_log(file_name: &str) -> Result<Vec<serde_json::Value>> {
        let schema_json: Value =
            serde_json::from_str(LOG_LINE_SCHEMA).expect("schema parse failure");
        let compiled_schema = JSONSchema::options()
            .with_draft(Draft::Draft7)
            .compile(&schema_json)
            .expect("Invalid schema");

        let f = File::open(file_name)?;
        let reader = BufReader::new(f);
        let mut result = Vec::new();

        for (line_num, line) in reader.lines().enumerate() {
            let line = line?;
            let val: Value = serde_json::from_str(&line)
                .map_err(|e| anyhow!("Line {}: invalid JSON: {}", line_num + 1, e))?;

            if let Err(errors) = compiled_schema.validate(&val) {
                let errs = errors.map(|e| e.to_string()).collect::<Vec<_>>().join("; ");
                return Err(anyhow!(
                    "Line {}: JSON Schema Validation errors: {}",
                    line_num + 1,
                    errs
                ));
            }
            println!("{}", val);
            result.push(val);
        }
        Ok(result)
    }

    #[tokio::test]
    async fn test_json_log_capture() -> Result<()> {
        #[allow(clippy::redundant_closure_call)]
        let _ = temp_env::async_with_vars(
            [(env_logging::PGD_LOGGING_JSONL, Some("1"))],
            (async || {
                let tmp_file = NamedTempFile::new().unwrap();
                let file_name = tmp_file.path().to_str().unwrap();
                let guard = StderrOverride::from_file(file_name)?;
                init();
                parent().await;
                drop(guard);

                let lines = load_log(file_name)?;

                // 1. 提取动态生成的 trace ID 并验证一致性。
                // 由于所有日志都属于同一条 trace，因此应具有相同的 trace_id。
                // 跳过没有 trace_id 的初始化日志（例如 OTLP 设置消息）。
                //
                // 注意：如果 logging 已被其他并行测试初始化，该测试可能失败。
                // logging 初始化是全局性的（Once），每个进程只能发生一次。
                // 如果未找到 trace_id，则优雅跳过验证。
                let Some(trace_id) = lines
                    .iter()
                    .find_map(|log_line| log_line.get("trace_id").and_then(|v| v.as_str()))
                    .map(|s| s.to_string())
                else {
                    // 如果 logging 已经初始化，则跳过测试 - 我们无法控制输出格式。
                    return Ok(());
                };

                // 验证 trace_id 不是全零 / 无效值。
                assert_ne!(
                    trace_id, "00000000000000000000000000000000",
                    "trace_id should not be a zero/invalid ID"
                );
                assert!(
                    !trace_id.chars().all(|c| c == '0'),
                    "trace_id should not be all zeros"
                );

                // 验证所有日志都拥有相同的 trace_id。
                for log_line in &lines {
                    if let Some(line_trace_id) = log_line.get("trace_id") {
                        assert_eq!(
                            line_trace_id.as_str().unwrap(),
                            &trace_id,
                            "All logs should have the same trace_id"
                        );
                    }
                }

                // 验证 my_trace_id 与真实 trace ID 一致。
                for log_line in &lines {
                    if let Some(my_trace_id) = log_line.get("my_trace_id") {
                        assert_eq!(
                            my_trace_id,
                            &serde_json::Value::String(trace_id.clone()),
                            "my_trace_id should match the trace_id from distributed tracing context"
                        );
                    }
                }

                // 2. 验证 span ID 存在且格式正确。
                let mut span_ids_seen: std::collections::HashSet<String> = std::collections::HashSet::new();
                let mut span_timestamps: std::collections::HashMap<String, DateTime<Utc>> = std::collections::HashMap::new();

                for log_line in &lines {
                    if let Some(span_id) = log_line.get("span_id") {
                        let span_id_str = span_id.as_str().unwrap();
                        assert!(
                            is_valid_span_id(span_id_str),
                            "Invalid span_id format: {}",
                            span_id_str
                        );
                        span_ids_seen.insert(span_id_str.to_string());
                    }

                    // 验证时间戳格式并跟踪 span 时间戳。
                    if let Some(time_str) = log_line.get("time").and_then(|v| v.as_str()) {
                        let timestamp = DateTime::parse_from_rfc3339(time_str)
                            .expect("All timestamps should be valid RFC3339 format")
                            .with_timezone(&Utc);

                        // 为每个 span_name 记录时间戳。
                        if let Some(span_name) = log_line.get("span_name").and_then(|v| v.as_str()) {
                            span_timestamps.insert(span_name.to_string(), timestamp);
                        }
                    }
                }

                // 3. 验证父子 span 关系。
                // 通过查看日志消息提取每个 span 的 span ID。
                let parent_span_id = lines
                    .iter()
                    .find(|log_line| {
                        log_line.get("span_name")
                            .and_then(|v| v.as_str()) == Some("parent")
                    })
                    .and_then(|log_line| {
                        log_line.get("span_id")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                    })
                    .expect("Should find parent span with span_id");

                let child_span_id = lines
                    .iter()
                    .find(|log_line| {
                        log_line.get("span_name")
                            .and_then(|v| v.as_str()) == Some("child")
                    })
                    .and_then(|log_line| {
                        log_line.get("span_id")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                    })
                    .expect("Should find child span with span_id");

                let grandchild_span_id = lines
                    .iter()
                    .find(|log_line| {
                        log_line.get("span_name")
                            .and_then(|v| v.as_str()) == Some("grandchild")
                    })
                    .and_then(|log_line| {
                        log_line.get("span_id")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                    })
                    .expect("Should find grandchild span with span_id");

                // 验证 span ID 是唯一的。
                assert_ne!(parent_span_id, child_span_id, "Parent and child should have different span IDs");
                assert_ne!(child_span_id, grandchild_span_id, "Child and grandchild should have different span IDs");
                assert_ne!(parent_span_id, grandchild_span_id, "Parent and grandchild should have different span IDs");

                // 验证父 span 没有 parent_id。
                for log_line in &lines {
                    if let Some(span_name) = log_line.get("span_name")
                        && let Some(span_name_str) = span_name.as_str()
                        && span_name_str == "parent"
                    {
                        assert!(
                            log_line.get("parent_id").is_none(),
                            "Parent span should not have a parent_id"
                        );
                    }
                }

                // 验证子 span 的 parent_id 等于 parent_span_id。
                for log_line in &lines {
                    if let Some(span_name) = log_line.get("span_name")
                        && let Some(span_name_str) = span_name.as_str()
                        && span_name_str == "child"
                    {
                        let parent_id = log_line.get("parent_id")
                            .and_then(|v| v.as_str())
                            .expect("Child span should have a parent_id");
                        assert_eq!(
                            parent_id,
                            parent_span_id,
                            "Child's parent_id should match parent's span_id"
                        );
                    }
                }

                // 验证孙子 span 的 parent_id 等于 child_span_id。
                for log_line in &lines {
                    if let Some(span_name) = log_line.get("span_name")
                        && let Some(span_name_str) = span_name.as_str()
                        && span_name_str == "grandchild"
                    {
                        let parent_id = log_line.get("parent_id")
                            .and_then(|v| v.as_str())
                            .expect("Grandchild span should have a parent_id");
                        assert_eq!(
                            parent_id,
                            child_span_id,
                            "Grandchild's parent_id should match child's span_id"
                        );
                    }
                }

                // 4. 验证时间戳顺序 - span 应按执行顺序记录日志。
                let parent_time = span_timestamps.get("parent")
                    .expect("Should have timestamp for parent span");
                let child_time = span_timestamps.get("child")
                    .expect("Should have timestamp for child span");
                let grandchild_time = span_timestamps.get("grandchild")
                    .expect("Should have timestamp for grandchild span");

                // 父 span 先记录（或同时记录），然后是子 span，再然后是孙子 span。
                assert!(
                    parent_time <= child_time,
                    "Parent span should log before or at same time as child span (parent: {}, child: {})",
                    parent_time,
                    child_time
                );
                assert!(
                    child_time <= grandchild_time,
                    "Child span should log before or at same time as grandchild span (child: {}, grandchild: {})",
                    child_time,
                    grandchild_time
                );

                Ok::<(), anyhow::Error>(())
            })(),
        )
        .await;
        Ok(())
    }

    // 用于过滤测试的不同日志级别测试函数。
    #[tracing::instrument(level = "debug", skip_all)]
    async fn debug_level_span() {
        tracing::debug!("inside debug span");
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn info_level_span() {
        tracing::info!("inside info span");
    }

    #[tracing::instrument(level = "warn", skip_all)]
    async fn warn_level_span() {
        tracing::warn!("inside warn span");
    }

    // 来自不同 target 的 span - 在 info 级别下应被过滤掉。
    // 因为过滤器是 warn,pagoda_runtime::logging::tests=debug。
    #[tracing::instrument(level = "info", target = "other_module", skip_all)]
    async fn other_target_info_span() {
        tracing::info!(target: "other_module", "inside other target span");
    }

    /// 针对 span 事件的综合测试，覆盖：
    /// - SPAN_FIRST_ENTRY 和 SPAN_CLOSED 事件发出
    /// - span 事件中的 trace 上下文（trace_id、span_id）
    /// - SPAN_CLOSED 中的计时信息
    /// - 基于级别的过滤（正向：允许级别通过，反向：过滤掉受限级别）
    /// - 基于 target 的过滤（允许 target 的 span 即使级别更低也可通过）
    ///
    /// 该测试在子进程中运行，以确保 logging 使用我们指定的
    /// 过滤配置（PGD_LOG=warn,pagoda_runtime::logging::tests=debug），避免
    /// 被其他可能先初始化 logging 的测试干扰。
    #[test]
    fn test_span_events() {
        use std::process::Command;

        // 使用指定环境变量运行子进程测试的 cargo test。
        let output = Command::new("cargo")
            .args([
                "test",
                "-p",
                "pagoda-runtime",
                "test_span_events_subprocess",
                "--",
                "--exact",
                "--nocapture",
            ])
            .env("PGD_LOGGING_JSONL", "1")
            .env("PGD_LOGGING_SPAN_EVENTS", "1")
            .env("PGD_LOG", "warn,pagoda_runtime::logging::tests=debug")
            .output()
            .expect("Failed to execute subprocess test");

        // 打印输出，便于调试。
        if !output.status.success() {
            eprintln!(
                "=== STDOUT ===\n{}",
                String::from_utf8_lossy(&output.stdout)
            );
            eprintln!(
                "=== STDERR ===\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        assert!(
            output.status.success(),
            "Subprocess test failed with exit code: {:?}",
            output.status.code()
        );
    }

    /// 子进程测试：执行实际的 span 事件校验。
    /// 它由 test_span_events 在带受控环境变量的独立进程中调用。
    #[tokio::test]
    async fn test_span_events_subprocess() -> Result<()> {
        // 如果不是以子进程运行（未设置环境变量），则跳过。
        if std::env::var("PGD_LOGGING_SPAN_EVENTS").is_err() {
            return Ok(());
        }

        let tmp_file = NamedTempFile::new().unwrap();
        let file_name = tmp_file.path().to_str().unwrap();
        let guard = StderrOverride::from_file(file_name)?;
        init();

        // 运行 parent/child/grandchild span（默认都为 INFO 级别）。
        parent().await;

        // 运行本测试模块中显式指定级别的 span。
        debug_level_span().await;
        info_level_span().await;
        warn_level_span().await;

        // 运行来自不同 target 的 span（应被过滤）。
        other_target_info_span().await;

        drop(guard);

        let lines = load_log(file_name)?;

        // 检查某个 span 事件是否存在的辅助函数。
        let has_span_event = |msg: &str, span_name: &str| {
            lines.iter().any(|log| {
                log.get("message").and_then(|v| v.as_str()) == Some(msg)
                    && log.get("span_name").and_then(|v| v.as_str()) == Some(span_name)
            })
        };

        // 获取 span 事件的辅助函数。
        let get_span_events = |msg: &str| -> Vec<&serde_json::Value> {
            lines
                .iter()
                .filter(|log| log.get("message").and_then(|v| v.as_str()) == Some(msg))
                .collect()
        };

        // === 测试 1：SPAN_FIRST_ENTRY 事件具有必需字段 ===
        let span_created_events = get_span_events("SPAN_FIRST_ENTRY");
        for event in &span_created_events {
            // 必须有 span_name。
            assert!(
                event.get("span_name").is_some(),
                "SPAN_FIRST_ENTRY must have span_name"
            );
            // 必须有合法的 trace_id（格式检查）。
            let trace_id = event
                .get("trace_id")
                .and_then(|v| v.as_str())
                .expect("SPAN_FIRST_ENTRY must have trace_id");
            assert!(
                trace_id.len() == 32 && trace_id.chars().all(|c| c.is_ascii_hexdigit()),
                "SPAN_FIRST_ENTRY must have valid trace_id format"
            );
            // 必须有合法的 span_id。
            let span_id = event
                .get("span_id")
                .and_then(|v| v.as_str())
                .expect("SPAN_FIRST_ENTRY must have span_id");
            assert!(
                is_valid_span_id(span_id),
                "SPAN_FIRST_ENTRY must have valid span_id"
            );
        }

        // === 测试 2：SPAN_CLOSED 事件具有计时信息 ===
        let span_closed_events = get_span_events("SPAN_CLOSED");
        for event in &span_closed_events {
            assert!(
                event.get("span_name").is_some(),
                "SPAN_CLOSED must have span_name"
            );
            assert!(
                event.get("time.busy_us").is_some()
                    || event.get("time.idle_us").is_some()
                    || event.get("time.duration_us").is_some(),
                "SPAN_CLOSED must have timing information"
            );
            // 必须有合法的 trace_id。
            let trace_id = event
                .get("trace_id")
                .and_then(|v| v.as_str())
                .expect("SPAN_CLOSED must have trace_id");
            assert!(
                trace_id.len() == 32 && trace_id.chars().all(|c| c.is_ascii_hexdigit()),
                "SPAN_CLOSED must have valid trace_id format"
            );
        }

        // === 测试 3：基于 target 的过滤（正向）===
        // 来自 pagoda_runtime::logging::tests 的 span 应在所有级别通过。
        // 因为该 target 在 debug 级别被允许。
        assert!(
            has_span_event("SPAN_FIRST_ENTRY", "debug_level_span"),
            "DEBUG span from allowed target MUST pass (target=debug filter)"
        );
        assert!(
            has_span_event("SPAN_FIRST_ENTRY", "info_level_span"),
            "INFO span from allowed target MUST pass (target=debug filter)"
        );
        assert!(
            has_span_event("SPAN_FIRST_ENTRY", "warn_level_span"),
            "WARN span from allowed target MUST pass (target=debug filter)"
        );

        // parent/child/grandchild 是来自允许 target 的 INFO 级别 span - 应通过。
        assert!(
            has_span_event("SPAN_FIRST_ENTRY", "parent"),
            "parent span (INFO) from allowed target MUST pass"
        );
        assert!(
            has_span_event("SPAN_FIRST_ENTRY", "child"),
            "child span (INFO) from allowed target MUST pass"
        );
        assert!(
            has_span_event("SPAN_FIRST_ENTRY", "grandchild"),
            "grandchild span (INFO) from allowed target MUST pass"
        );

        // === 测试 4：基于级别的过滤（反向）===
        // 验证来自其他 target 的 debug/info 级别 span 会被过滤掉。
        assert!(
            !has_span_event("SPAN_FIRST_ENTRY", "other_target_info_span"),
            "INFO span from non-allowed target (other_module) MUST be filtered out"
        );

        // 同时验证其他 target 的 span 不会出现在 debug/info 级别。
        for event in &span_created_events {
            let target = event.get("target").and_then(|v| v.as_str()).unwrap_or("");
            let level = event.get("level").and_then(|v| v.as_str()).unwrap_or("");

            // 如果 level 是 DEBUG 或 INFO，target 必须是我们的测试模块。
            if level == "DEBUG" || level == "INFO" {
                assert!(
                    target.contains("pagoda_runtime::logging::tests"),
                    "DEBUG/INFO span must be from allowed target, got target={target}"
                );
            }
        }

        Ok(())
    }

    // ─── is_valid_trace_id 单元测试 ────────────────────────────────────────────

    #[test]
    fn valid_trace_id_accepts_32_hex_chars() {
        assert!(is_valid_trace_id("4bf92f3577b34da6a3ce929d0e0e4736"));
        assert!(is_valid_trace_id("00000000000000000000000000000000"));
        assert!(is_valid_trace_id("ffffffffffffffffffffffffffffffff"));
        assert!(is_valid_trace_id("abcdef0123456789abcdef0123456789"));
    }

    #[test]
    fn valid_trace_id_rejects_wrong_length() {
        // 太短
        assert!(!is_valid_trace_id("4bf92f3577b34da6a3ce929d0e0e473"));
        // 太长
        assert!(!is_valid_trace_id("4bf92f3577b34da6a3ce929d0e0e47360"));
        // 空串
        assert!(!is_valid_trace_id(""));
    }

    #[test]
    fn valid_trace_id_rejects_non_hex() {
        // is_ascii_hexdigit() 接受大写十六进制（A-F），因此大写是合法的
        assert!(is_valid_trace_id("4BF92F3577B34DA6A3CE929D0E0E4736"));
        // 含连字符（UUID 格式）→ 长度为 36，必须拒绝
        assert!(!is_valid_trace_id("4bf92f35-77b3-4da6-a3ce-929d0e0e4736"));
        // 含非法字符 g
        assert!(!is_valid_trace_id("4bf92f3577b34da6a3ce929d0e0e473g"));
        // 含空格
        assert!(!is_valid_trace_id("4bf92f3577b34da6a3ce929d0e0e473 "));
    }

    // ─── is_valid_span_id 单元测试 ─────────────────────────────────────────────

    #[test]
    fn valid_span_id_accepts_16_hex_chars() {
        assert!(is_valid_span_id("00f067aa0ba902b7"));
        assert!(is_valid_span_id("0000000000000000"));
        assert!(is_valid_span_id("ffffffffffffffff"));
        assert!(is_valid_span_id("abcdef0123456789"));
    }

    #[test]
    fn valid_span_id_rejects_wrong_length() {
        assert!(!is_valid_span_id("00f067aa0ba902b")); // 15 位
        assert!(!is_valid_span_id("00f067aa0ba902b70")); // 17 位
        assert!(!is_valid_span_id(""));
    }

    #[test]
    fn valid_span_id_rejects_non_hex() {
        // is_ascii_hexdigit() 接受大写十六进制
        assert!(is_valid_span_id("00F067AA0BA902B7"));
        // 含非法字符 g
        assert!(!is_valid_span_id("00f067aa0ba902g7"));
        // 含连字符
        assert!(!is_valid_span_id("00f067aa-ba902b7"));
    }

    // ─── parse_traceparent 单元测试 ────────────────────────────────────────────

    #[test]
    fn parse_traceparent_valid_returns_ids() {
        let tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let (trace, span) = parse_traceparent(tp);
        assert_eq!(trace.as_deref(), Some("4bf92f3577b34da6a3ce929d0e0e4736"));
        assert_eq!(span.as_deref(), Some("00f067aa0ba902b7"));
    }

    #[test]
    fn parse_traceparent_wrong_segment_count_returns_none() {
        // 只有三段
        let (t, s) = parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7");
        assert!(t.is_none());
        assert!(s.is_none());
        // 五段
        let (t, s) = parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra");
        assert!(t.is_none());
        assert!(s.is_none());
    }

    #[test]
    fn parse_traceparent_invalid_trace_id_returns_none() {
        // trace_id 段长度错误
        let (t, s) = parse_traceparent("00-shortid-00f067aa0ba902b7-01");
        assert!(t.is_none());
        assert!(s.is_none());
    }

    #[test]
    fn parse_traceparent_invalid_span_id_returns_none() {
        // span_id 含非法字符 g → 应拒绝
        let (t, s) = parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902g7-01");
        assert!(t.is_none());
        assert!(s.is_none());
        // span_id 长度错误（15位）→ 应拒绝
        let (t, s) = parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b-01");
        assert!(t.is_none());
        assert!(s.is_none());
    }

    #[test]
    fn parse_traceparent_empty_string_returns_none() {
        let (t, s) = parse_traceparent("");
        assert!(t.is_none());
        assert!(s.is_none());
    }

    // ─── create_traceparent 单元测试 ───────────────────────────────────────────

    #[test]
    fn create_traceparent_produces_w3c_format() {
        let ctx = DistributedTraceContext {
            trace_id: "4bf92f3577b34da6a3ce929d0e0e4736".to_string(),
            span_id: "00f067aa0ba902b7".to_string(),
            parent_id: None,
            tracestate: None,
            start: None,
            end: None,
            x_request_id: None,
            request_id: None,
        };
        assert_eq!(
            ctx.create_traceparent(),
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        );
    }

    #[test]
    fn create_traceparent_roundtrip_through_parse() {
        let ctx = DistributedTraceContext {
            trace_id: "abcdef0123456789abcdef0123456789".to_string(),
            span_id: "fedcba9876543210".to_string(),
            parent_id: None,
            tracestate: None,
            start: None,
            end: None,
            x_request_id: None,
            request_id: None,
        };
        let tp = ctx.create_traceparent();
        let (trace, span) = parse_traceparent(&tp);
        assert_eq!(trace.as_deref(), Some(ctx.trace_id.as_str()));
        assert_eq!(span.as_deref(), Some(ctx.span_id.as_str()));
    }

    // ─── TimeFormatter 单元测试 ────────────────────────────────────────────────

    #[test]
    fn time_formatter_utc_ends_with_z() {
        let fmt = TimeFormatter { use_local_tz: false };
        let s = fmt.format_now();
        assert!(s.ends_with('Z'), "UTC 时间戳应以 'Z' 结尾，实际: {s}");
    }

    #[test]
    fn time_formatter_utc_has_microsecond_precision() {
        let fmt = TimeFormatter { use_local_tz: false };
        let s = fmt.format_now();
        // 格式：2025-01-15T01:30:00.000000Z，小数点后应有 6 位
        let dot_pos = s.find('.').expect("时间戳应包含小数点");
        let frac = &s[dot_pos + 1..s.len() - 1]; // 去掉末尾 Z
        assert_eq!(frac.len(), 6, "微秒精度应为 6 位，实际: {s}");
    }

    #[test]
    fn time_formatter_utc_parses_as_rfc3339() {
        let fmt = TimeFormatter { use_local_tz: false };
        let s = fmt.format_now();
        DateTime::parse_from_rfc3339(&s)
            .unwrap_or_else(|e| panic!("UTC 时间戳 '{s}' 不是合法 RFC3339: {e}"));
    }

    #[test]
    fn time_formatter_local_contains_offset() {
        let fmt = TimeFormatter { use_local_tz: true };
        let s = fmt.format_now();
        // 本地时区格式含 +HH:MM 或 -HH:MM
        assert!(
            s.contains('+') || s.contains('-'),
            "本地时区时间戳应包含偏移量，实际: {s}"
        );
    }

    #[test]
    fn time_formatter_local_parses_as_rfc3339() {
        let fmt = TimeFormatter { use_local_tz: true };
        let s = fmt.format_now();
        DateTime::parse_from_rfc3339(&s)
            .unwrap_or_else(|e| panic!("本地时区时间戳 '{s}' 不是合法 RFC3339: {e}"));
    }

    // ─── parse_tracing_duration 单元测试 ──────────────────────────────────────

    #[test]
    fn parse_tracing_duration_nanoseconds() {
        // 1000 ns → 1 µs
        assert_eq!(parse_tracing_duration("1000ns"), Some(1));
        // 500 ns → 0 µs（截断）
        assert_eq!(parse_tracing_duration("500ns"), Some(0));
    }

    #[test]
    fn parse_tracing_duration_microseconds() {
        assert_eq!(parse_tracing_duration("42µs"), Some(42));
        assert_eq!(parse_tracing_duration("42us"), Some(42));
        assert_eq!(parse_tracing_duration("1.5us"), Some(1));
    }

    #[test]
    fn parse_tracing_duration_milliseconds() {
        // 2 ms → 2000 µs
        assert_eq!(parse_tracing_duration("2ms"), Some(2000));
        assert_eq!(parse_tracing_duration("0.5ms"), Some(500));
    }

    #[test]
    fn parse_tracing_duration_seconds() {
        // 1 s → 1_000_000 µs
        assert_eq!(parse_tracing_duration("1s"), Some(1_000_000));
        assert_eq!(parse_tracing_duration("0.001s"), Some(1000));
    }

    #[test]
    fn parse_tracing_duration_with_quotes_and_spaces() {
        assert_eq!(parse_tracing_duration("\"42ms\""), Some(42_000));
        assert_eq!(parse_tracing_duration("' 10 ms '"), Some(10_000));
    }

    #[test]
    fn parse_tracing_duration_invalid_returns_none() {
        assert_eq!(parse_tracing_duration(""), None);
        assert_eq!(parse_tracing_duration("fast"), None);
        assert_eq!(parse_tracing_duration("42"), None); // 缺少单位
        assert_eq!(parse_tracing_duration("42hrs"), None); // 不支持的单位
    }

    // ─── inject_trace_headers_into_map 单元测试 ────────────────────────────────

    #[test]
    fn inject_trace_headers_outside_span_does_not_panic() {
        // 在 span 外调用不应崩溃，且不应插入任何头部
        let mut headers = std::collections::HashMap::new();
        inject_trace_headers_into_map(&mut headers);
        // 无活跃 span → get_distributed_tracing_context() 返回 None → 无注入
        assert!(
            headers.is_empty(),
            "span 外调用不应向 headers 插入任何内容"
        );
    }

    // ─── DistributedTraceContext 字段访问单元测试 ──────────────────────────────

    #[test]
    fn distributed_trace_context_fields_round_trip() {
        let ctx = DistributedTraceContext {
            trace_id: "aabbccddeeff00112233445566778899".to_string(),
            span_id: "0011223344556677".to_string(),
            parent_id: Some("8899aabbccddeeff".to_string()),
            tracestate: Some("vendor=value".to_string()),
            start: None,
            end: None,
            x_request_id: Some("req-001".to_string()),
            request_id: Some("rid-abc".to_string()),
        };
        assert_eq!(ctx.trace_id, "aabbccddeeff00112233445566778899");
        assert_eq!(ctx.span_id, "0011223344556677");
        assert_eq!(ctx.parent_id.as_deref(), Some("8899aabbccddeeff"));
        assert_eq!(ctx.tracestate.as_deref(), Some("vendor=value"));
        assert_eq!(ctx.x_request_id.as_deref(), Some("req-001"));
        assert_eq!(ctx.request_id.as_deref(), Some("rid-abc"));
    }

    #[test]
    fn distributed_trace_context_serialization_roundtrip() {
        let ctx = DistributedTraceContext {
            trace_id: "4bf92f3577b34da6a3ce929d0e0e4736".to_string(),
            span_id: "00f067aa0ba902b7".to_string(),
            parent_id: None,
            tracestate: None,
            start: None,
            end: None,
            x_request_id: None,
            request_id: None,
        };
        let json = serde_json::to_string(&ctx).unwrap();
        let decoded: DistributedTraceContext = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.trace_id, ctx.trace_id);
        assert_eq!(decoded.span_id, ctx.span_id);
        assert!(decoded.parent_id.is_none());
    }

    #[test]
    fn distributed_trace_context_optional_fields_omitted_in_json() {
        let ctx = DistributedTraceContext {
            trace_id: "00000000000000000000000000000001".to_string(),
            span_id: "0000000000000001".to_string(),
            parent_id: None,
            tracestate: None,
            start: None,
            end: None,
            x_request_id: None,
            request_id: None,
        };
        let json = serde_json::to_string(&ctx).unwrap();
        // skip_serializing_if = "Option::is_none" — 可选字段不应出现在 JSON 中
        assert!(!json.contains("parent_id"), "parent_id 为 None 时不应序列化");
        assert!(!json.contains("tracestate"), "tracestate 为 None 时不应序列化");
        assert!(!json.contains("x_request_id"), "x_request_id 为 None 时不应序列化");
        assert!(!json.contains("request_id"), "request_id 为 None 时不应序列化");
    }
}
