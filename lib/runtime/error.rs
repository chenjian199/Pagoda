// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 设计意图
//! Dynamo 全局统一错误类型.解决三个长期痛点:
//!   1. 不同子系统返回的 `anyhow::Error` 没有类别信息,迁移层无从决策;
//!   2. 错误链跨网络丢失,反序列化后只剩字符串;
//!   3. `Display` 习惯性展开整条链,日志冗长.
//!
//! 解决方式:
//!   - 引入 [`ErrorType`] / [`BackendError`] 双层枚举,固定基础类别;
//!   - [`DynamoError`] 通过 `caused_by: Option<Box<DynamoError>>` 链式持有根因,
//!     `serde` 直接序列化整条链;
//!   - `Display` 只输出当前层,通过 `std::error::Error::source` 走链.
//!
//! # 外部契约
//! - `ErrorType::{Unknown, InvalidArgument, CannotConnect, Disconnected,
//!   ConnectionTimeout, ResponseTimeout, Cancelled, ResourceExhausted, Backend(BackendError)}`;
//! - `BackendError::{Unknown, InvalidArgument, CannotConnect, Disconnected,
//!   ConnectionTimeout, ResponseTimeout, Cancelled, EngineShutdown, StreamIncomplete}`;
//! - Display: `ErrorType::Backend(sub)` 输出 `"Backend{sub}"`(连写,无分隔符);
//! - `DynamoError::{builder, msg, error_type, message}`;
//! - `DynamoErrorBuilder::{error_type, message, cause, build}`,默认 `Unknown` + 空消息 + 无根因;
//! - `From<&dyn Error>` / `From<Box<dyn Error>>`: 已是 `DynamoError` 走 clone/downcast,否则
//!   包装为 `Unknown` + 显示串,并递归转换 `source()`;
//! - [`match_error_chain`]: 走链;遇到 exclude 立即 `false`;否则 match 至少命中一项就 `true`.
//!
//! # 实现要点
//! - 两个枚举的 Display 都抽为"变体名映射到 &'static str"的帮手函数,避免与
//!   lib-copy 一样每个变体一条 `write!` 调用;
//! - 引入私有 `ErrorChainIter` 迭代器,把 `match_error_chain` 的循环改写为函数式 `any`/`all`;
//! - `From<Box<dyn Error>>` 用 `downcast` 走零拷贝路径,否则下放到 `From<&dyn Error>`.

use serde::{Deserialize, Serialize};
use std::fmt;

// === SECTION: ErrorType / BackendError ===

/// 顶层错误分类.消费者(迁移层、重试策略)通过本枚举决定后续动作.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorType {
    /// 未分类或未知错误.
    Unknown,
    /// 请求参数非法(例如 prompt 超过上下文长度).
    InvalidArgument,
    /// 无法连接到远端 worker.
    CannotConnect,
    /// 已建立的连接被意外切断.
    Disconnected,
    /// 连接或请求超时.
    ConnectionTimeout,
    /// 后端接受请求但中途停止响应(流空闲超时).
    ResponseTimeout,
    /// 请求被取消(例如客户端断开).
    Cancelled,
    /// 资源不足.
    ResourceExhausted,
    /// 后端引擎错误.
    Backend(BackendError),
}

impl ErrorType {
    /// 返回本枚举变体的名称字符串;仅限 unit-like 变体,Backend 变体走单独分支.
    fn variant_name(&self) -> Option<&'static str> {
        Some(match self {
            ErrorType::Unknown => "Unknown",
            ErrorType::InvalidArgument => "InvalidArgument",
            ErrorType::CannotConnect => "CannotConnect",
            ErrorType::Disconnected => "Disconnected",
            ErrorType::ConnectionTimeout => "ConnectionTimeout",
            ErrorType::ResponseTimeout => "ResponseTimeout",
            ErrorType::Cancelled => "Cancelled",
            ErrorType::ResourceExhausted => "ResourceExhausted",
            ErrorType::Backend(_) => return None,
        })
    }
}

impl fmt::Display for ErrorType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(name) = self.variant_name() {
            f.write_str(name)
        } else if let ErrorType::Backend(sub) = self {
            // 注意:"Backend" 与子类型连写,不加分隔符.
            write!(f, "Backend{sub}")
        } else {
            unreachable!("variant_name only returns None for Backend")
        }
    }
}

/// 后端引擎错误子类别.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackendError {
    /// 未分类后端错误.
    Unknown,
    /// 请求参数非法.
    InvalidArgument,
    /// 无法连接到引擎.
    CannotConnect,
    /// 连接被意外切断.
    Disconnected,
    /// 连接或请求超时.
    ConnectionTimeout,
    /// 后端接受请求但中途停止响应.
    ResponseTimeout,
    /// 请求被取消.
    Cancelled,
    /// 引擎进程已停止或崩溃.
    EngineShutdown,
    /// 响应流提前结束(例如引擎中途 drop).
    StreamIncomplete,
}

impl fmt::Display for BackendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            BackendError::Unknown => "Unknown",
            BackendError::InvalidArgument => "InvalidArgument",
            BackendError::CannotConnect => "CannotConnect",
            BackendError::Disconnected => "Disconnected",
            BackendError::ConnectionTimeout => "ConnectionTimeout",
            BackendError::ResponseTimeout => "ResponseTimeout",
            BackendError::Cancelled => "Cancelled",
            BackendError::EngineShutdown => "EngineShutdown",
            BackendError::StreamIncomplete => "StreamIncomplete",
        };
        f.write_str(name)
    }
}

// === SECTION: DynamoError ===

/// Dynamo 统一错误类型.
///
/// `Display` 只输出当前一层(标准 Rust 习惯);走链请用 `source()`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamoError {
    error_type: ErrorType,
    message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    caused_by: Option<Box<DynamoError>>,
}

impl DynamoError {
    /// 入口:返回一个新的 [`DynamoErrorBuilder`].
    pub fn builder() -> DynamoErrorBuilder {
        DynamoErrorBuilder::default()
    }

    /// 快捷构造 `ErrorType::Unknown` 错误,无根因.
    pub fn msg(message: impl Into<String>) -> Self {
        Self::builder().message(message).build()
    }

    /// 返回错误类型.
    pub fn error_type(&self) -> ErrorType {
        self.error_type
    }

    /// 返回错误消息.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for DynamoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.error_type, self.message)
    }
}

impl std::error::Error for DynamoError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.caused_by
            .as_deref()
            .map(|e| e as &(dyn std::error::Error + 'static))
    }
}

// === SECTION: From 转换 ===

/// 从 `&dyn Error` 转换:已是 `DynamoError` 时克隆,否则递归包装.
impl<'a> From<&'a (dyn std::error::Error + 'static)> for DynamoError {
    fn from(err: &'a (dyn std::error::Error + 'static)) -> Self {
        // 已经是同类型 → 直接克隆,保留 error_type.
        if let Some(existing) = err.downcast_ref::<DynamoError>() {
            return existing.clone();
        }
        let caused_by = err.source().map(|inner| Box::new(DynamoError::from(inner)));
        DynamoError {
            error_type: ErrorType::Unknown,
            message: err.to_string(),
            caused_by,
        }
    }
}

/// 从 `Box<dyn Error>` 转换:已是 `DynamoError` 时取出所有权,否则下放到引用版本.
impl From<Box<dyn std::error::Error + 'static>> for DynamoError {
    fn from(err: Box<dyn std::error::Error + 'static>) -> Self {
        match err.downcast::<DynamoError>() {
            Ok(boxed) => *boxed,
            Err(other) => DynamoError::from(&*other as &(dyn std::error::Error + 'static)),
        }
    }
}

// === SECTION: DynamoErrorBuilder ===

/// [`DynamoError`] 的构造器.
#[derive(Default)]
pub struct DynamoErrorBuilder {
    error_type: Option<ErrorType>,
    message: Option<String>,
    caused_by: Option<Box<DynamoError>>,
}

impl DynamoErrorBuilder {
    /// 设置错误类型.
    pub fn error_type(mut self, error_type: ErrorType) -> Self {
        self.error_type = Some(error_type);
        self
    }

    /// 设置错误消息.
    pub fn message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    /// 设置根因.任意 `std::error::Error` 都被转换为 `DynamoError`.
    pub fn cause(mut self, cause: impl std::error::Error + 'static) -> Self {
        let converted = DynamoError::from(&cause as &(dyn std::error::Error + 'static));
        self.caused_by = Some(Box::new(converted));
        self
    }

    /// 完成构造.缺省值:`Unknown` / 空消息 / 无根因.
    pub fn build(self) -> DynamoError {
        DynamoError {
            error_type: self.error_type.unwrap_or(ErrorType::Unknown),
            message: self.message.unwrap_or_default(),
            caused_by: self.caused_by,
        }
    }
}

// === SECTION: 错误链分析 ===

/// 顺着 `source()` 走链的内部迭代器.每一项是链中下一个错误的引用.
struct ErrorChainIter<'a> {
    current: Option<&'a (dyn std::error::Error + 'static)>,
}

impl<'a> Iterator for ErrorChainIter<'a> {
    type Item = &'a (dyn std::error::Error + 'static);
    fn next(&mut self) -> Option<Self::Item> {
        let cur = self.current?;
        self.current = cur.source();
        Some(cur)
    }
}

fn chain<'a>(err: &'a (dyn std::error::Error + 'static)) -> ErrorChainIter<'a> {
    ErrorChainIter { current: Some(err) }
}

/// 检查错误链是否包含 `match_set` 任一类型,且不包含 `exclude_set` 任一类型.
///
/// - 走 `source()` 链,只考察可 downcast 为 [`DynamoError`] 的节点;
/// - 链中任一节点命中 `exclude_set` 立即返回 `false`;
/// - 否则,链中存在节点命中 `match_set` 时返回 `true`,否则 `false`;
/// - 非 `DynamoError` 节点被跳过.
pub fn match_error_chain(
    err: &(dyn std::error::Error + 'static),
    match_set: &[ErrorType],
    exclude_set: &[ErrorType],
) -> bool {
    let mut hit = false;
    for node in chain(err) {
        let Some(dyn_err) = node.downcast_ref::<DynamoError>() else {
            continue;
        };
        let ty = dyn_err.error_type();
        if exclude_set.contains(&ty) {
            return false;
        }
        if !hit && match_set.contains(&ty) {
            hit = true;
        }
    }
    hit
}

// === SECTION: tests ===

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    // 编译期断言：DynamoError 满足 std::error::Error + Send + Sync + 'static
    const _: () = {
        fn assert_stderror<T: std::error::Error>() {}
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        fn assert_static<T: 'static>() {}
        fn assert_all() {
            assert_stderror::<DynamoError>();
            assert_send::<DynamoError>();
            assert_sync::<DynamoError>();
            assert_static::<DynamoError>();
        }
    };

    // ── msg 快捷构造 ─────────────────────────────────────────────────────────

    #[test]
    fn test_msg_constructor() {
        let err = DynamoError::msg("something failed");
        assert_eq!(err.error_type(), ErrorType::Unknown);
        assert_eq!(err.message(), "something failed");
        assert!(err.source().is_none());
    }

    /// msg 接受 &str 和 String
    #[test]
    fn msg_accepts_string_and_str() {
        let a = DynamoError::msg("static str");
        let b = DynamoError::msg("owned string".to_string());
        assert_eq!(a.message(), "static str");
        assert_eq!(b.message(), "owned string");
    }

    // ── Builder ──────────────────────────────────────────────────────────────

    /// builder 不设任何字段时返回默认值
    #[test]
    fn builder_defaults() {
        let err = DynamoError::builder().build();
        assert_eq!(err.error_type(), ErrorType::Unknown);
        assert_eq!(err.message(), "");
        assert!(err.source().is_none());
    }

    #[test]
    fn test_new_constructor_with_cause() {
        let cause = std::io::Error::other("io error");
        let err = DynamoError::builder()
            .error_type(ErrorType::Unknown)
            .message("operation failed")
            .cause(cause)
            .build();
        assert_eq!(err.error_type(), ErrorType::Unknown);
        assert_eq!(err.message(), "operation failed");
        assert!(err.source().is_some());
    }

    /// builder 设置所有字段
    #[test]
    fn builder_all_fields() {
        let inner = DynamoError::msg("root cause");
        let err = DynamoError::builder()
            .error_type(ErrorType::Disconnected)
            .message("outer")
            .cause(inner)
            .build();
        assert_eq!(err.error_type(), ErrorType::Disconnected);
        assert_eq!(err.message(), "outer");
        assert!(err.source().is_some());
    }

    /// builder cause 可接受任意 std::error::Error
    #[test]
    fn builder_cause_wraps_std_error() {
        let err = DynamoError::builder()
            .cause(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"))
            .build();
        let src = err.source().unwrap();
        assert!(src.to_string().contains("timeout"));
    }

    // ── Display ──────────────────────────────────────────────────────────────

    /// Display 格式为 "{type}: {message}"，不展开链
    #[test]
    fn test_display_shows_only_current_error() {
        let cause = std::io::Error::other("io error");
        let err = DynamoError::builder()
            .error_type(ErrorType::Unknown)
            .message("operation failed")
            .cause(cause)
            .build();
        assert_eq!(err.to_string(), "Unknown: operation failed");
    }

    /// Display 对各 ErrorType 输出正确
    #[test]
    fn display_various_error_types() {
        let err = DynamoError::builder()
            .error_type(ErrorType::Disconnected)
            .message("lost")
            .build();
        assert_eq!(err.to_string(), "Disconnected: lost");

        let err2 = DynamoError::builder()
            .error_type(ErrorType::Backend(BackendError::EngineShutdown))
            .message("engine down")
            .build();
        assert_eq!(err2.to_string(), "BackendEngineShutdown: engine down");
    }

    // ── source() 链 ──────────────────────────────────────────────────────────

    #[test]
    fn test_source_chain() {
        let cause = std::io::Error::other("io error");
        let err = DynamoError::builder()
            .error_type(ErrorType::Unknown)
            .message("operation failed")
            .cause(cause)
            .build();
        let source = err.source().unwrap();
        assert!(source.to_string().contains("io error"));
    }

    /// 无根因时 source() 返回 None
    #[test]
    fn source_none_when_no_cause() {
        let err = DynamoError::msg("no cause");
        assert!(err.source().is_none());
    }

    /// 三层链：source() 逐层可达
    #[test]
    fn three_level_source_chain() {
        let root = DynamoError::msg("root");
        let mid = DynamoError::builder().message("mid").cause(root).build();
        let top = DynamoError::builder().message("top").cause(mid).build();

        let mid_src = top.source().unwrap();
        assert!(mid_src.to_string().contains("mid"));

        let root_src = mid_src.source().unwrap();
        assert!(root_src.to_string().contains("root"));
    }

    // ── From 转换 ────────────────────────────────────────────────────────────

    #[test]
    fn test_from_boxed_std_error() {
        let std_err = std::io::Error::other("io error");
        let boxed: Box<dyn std::error::Error> = Box::new(std_err);
        let dynamo_err = DynamoError::from(boxed);
        assert_eq!(dynamo_err.error_type(), ErrorType::Unknown);
        assert_eq!(dynamo_err.message(), "io error");
    }

    /// Box<DynamoError> 转换时取出所有权，不额外包装
    #[test]
    fn test_from_boxed_takes_ownership_of_dynamo_error() {
        let inner = DynamoError::msg("original");
        let boxed: Box<dyn std::error::Error> = Box::new(inner);
        let dynamo_err = DynamoError::from(boxed);
        assert_eq!(dynamo_err.error_type(), ErrorType::Unknown);
        assert_eq!(dynamo_err.message(), "original");
    }

    /// 外层错误有 source() 时，From 递归转换整条链
    #[test]
    fn test_from_boxed_with_source_chain() {
        #[derive(Debug)]
        struct OuterError { source: std::io::Error }
        impl fmt::Display for OuterError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "outer error occurred")
            }
        }
        impl std::error::Error for OuterError {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.source)
            }
        }

        let inner = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let outer = OuterError { source: inner };
        let boxed: Box<dyn std::error::Error> = Box::new(outer);
        let dynamo_err = DynamoError::from(boxed);

        assert_eq!(dynamo_err.message(), "outer error occurred");
        assert!(dynamo_err.source().is_some());
        assert!(dynamo_err.source().unwrap().to_string().contains("file not found"));
    }

    /// &dyn Error 转换：已是 DynamoError 时克隆保留类型
    #[test]
    fn from_ref_preserves_dynamo_error_type() {
        let orig = DynamoError::builder()
            .error_type(ErrorType::Cancelled)
            .message("cancelled")
            .build();
        let converted = DynamoError::from(&orig as &(dyn std::error::Error + 'static));
        assert_eq!(converted.error_type(), ErrorType::Cancelled);
        assert_eq!(converted.message(), "cancelled");
    }

    // ── 序列化 ───────────────────────────────────────────────────────────────

    #[test]
    fn test_serialization_roundtrip() {
        let cause = DynamoError::msg("inner cause");
        let err = DynamoError::builder()
            .error_type(ErrorType::Unknown)
            .message("outer error")
            .cause(cause)
            .build();

        let json = serde_json::to_string(&err).unwrap();
        let deserialized: DynamoError = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.error_type(), ErrorType::Unknown);
        assert_eq!(deserialized.message(), "outer error");
        assert!(deserialized.source().is_some());
        let cause_node = deserialized
            .source()
            .unwrap()
            .downcast_ref::<DynamoError>()
            .unwrap();
        assert_eq!(cause_node.message(), "inner cause");
    }

    /// 无根因时序列化不含 `caused_by` 字段
    #[test]
    fn serialization_omits_caused_by_when_none() {
        let err = DynamoError::msg("simple");
        let json = serde_json::to_string(&err).unwrap();
        assert!(!json.contains("caused_by"), "caused_by 应被省略: {json}");
    }

    /// 含根因时序列化包含 `caused_by` 字段
    #[test]
    fn serialization_includes_caused_by_when_present() {
        let err = DynamoError::builder()
            .message("outer")
            .cause(DynamoError::msg("inner"))
            .build();
        let json = serde_json::to_string(&err).unwrap();
        assert!(json.contains("caused_by"), "caused_by 应存在: {json}");
    }

    // ── ErrorType / BackendError Display ─────────────────────────────────────

    #[test]
    fn test_error_type_display() {
        assert_eq!(ErrorType::Unknown.to_string(), "Unknown");
        assert_eq!(ErrorType::InvalidArgument.to_string(), "InvalidArgument");
        assert_eq!(ErrorType::CannotConnect.to_string(), "CannotConnect");
        assert_eq!(ErrorType::Disconnected.to_string(), "Disconnected");
        assert_eq!(ErrorType::ConnectionTimeout.to_string(), "ConnectionTimeout");
        assert_eq!(ErrorType::ResponseTimeout.to_string(), "ResponseTimeout");
        assert_eq!(ErrorType::Cancelled.to_string(), "Cancelled");
        assert_eq!(ErrorType::Backend(BackendError::Unknown).to_string(), "BackendUnknown");
        assert_eq!(ErrorType::Backend(BackendError::InvalidArgument).to_string(), "BackendInvalidArgument");
        assert_eq!(ErrorType::Backend(BackendError::CannotConnect).to_string(), "BackendCannotConnect");
        assert_eq!(ErrorType::Backend(BackendError::Disconnected).to_string(), "BackendDisconnected");
        assert_eq!(ErrorType::Backend(BackendError::ConnectionTimeout).to_string(), "BackendConnectionTimeout");
        assert_eq!(ErrorType::Backend(BackendError::Cancelled).to_string(), "BackendCancelled");
        assert_eq!(ErrorType::Backend(BackendError::EngineShutdown).to_string(), "BackendEngineShutdown");
        assert_eq!(ErrorType::Backend(BackendError::StreamIncomplete).to_string(), "BackendStreamIncomplete");
        assert_eq!(ErrorType::Backend(BackendError::ResponseTimeout).to_string(), "BackendResponseTimeout");
    }

    // ── match_error_chain ────────────────────────────────────────────────────

    /// 链顶命中 match_set → true
    #[test]
    fn match_error_chain_hit() {
        let err = DynamoError::builder()
            .error_type(ErrorType::Disconnected)
            .message("x")
            .build();
        assert!(match_error_chain(&err, &[ErrorType::Disconnected], &[]));
    }

    /// 链中无匹配 → false
    #[test]
    fn match_error_chain_miss() {
        let err = DynamoError::msg("x");
        assert!(!match_error_chain(&err, &[ErrorType::Disconnected], &[]));
    }

    /// exclude_set 命中 → false（即使 match_set 也匹配）
    #[test]
    fn match_error_chain_excluded_wins() {
        let err = DynamoError::builder()
            .error_type(ErrorType::Cancelled)
            .message("x")
            .build();
        assert!(!match_error_chain(&err, &[ErrorType::Cancelled], &[ErrorType::Cancelled]));
    }

    /// 匹配在链的深处
    #[test]
    fn match_error_chain_deep_hit() {
        let root = DynamoError::builder()
            .error_type(ErrorType::Disconnected)
            .message("root")
            .build();
        let outer = DynamoError::builder()
            .error_type(ErrorType::Unknown)
            .message("outer")
            .cause(root)
            .build();
        assert!(match_error_chain(&outer, &[ErrorType::Disconnected], &[]));
    }

    /// 排除类型在链的深处 → false
    #[test]
    fn match_error_chain_deep_exclude() {
        let root = DynamoError::builder()
            .error_type(ErrorType::Cancelled)
            .message("cancelled")
            .build();
        let outer = DynamoError::builder()
            .error_type(ErrorType::Disconnected)
            .message("outer")
            .cause(root)
            .build();
        // Disconnected 在 match_set，但链中有 Cancelled 在 exclude_set
        assert!(!match_error_chain(&outer, &[ErrorType::Disconnected], &[ErrorType::Cancelled]));
    }

    /// 空 match_set → 始终 false
    #[test]
    fn match_error_chain_empty_match_set() {
        let err = DynamoError::msg("x");
        assert!(!match_error_chain(&err, &[], &[]));
    }

    /// 非 DynamoError 节点被跳过
    #[test]
    fn match_error_chain_skips_non_dynamo_nodes() {
        // std::io::Error 不能 downcast 为 DynamoError，应被跳过
        let std_err = std::io::Error::other("io");
        assert!(!match_error_chain(&std_err, &[ErrorType::Unknown], &[]));
    }

    /// match_set 包含多个类型，命中其中一个即可
    #[test]
    fn match_error_chain_multi_match_set() {
        let err = DynamoError::builder()
            .error_type(ErrorType::ResponseTimeout)
            .message("slow")
            .build();
        assert!(match_error_chain(
            &err,
            &[ErrorType::ConnectionTimeout, ErrorType::ResponseTimeout],
            &[]
        ));
    }

    /// 三层链：中间层命中 match_set，底层命中 exclude_set → false
    #[test]
    fn match_error_chain_mid_match_bottom_exclude() {
        let bottom = DynamoError::builder()
            .error_type(ErrorType::Cancelled)
            .message("cancelled")
            .build();
        let mid = DynamoError::builder()
            .error_type(ErrorType::Disconnected)
            .message("disconnected")
            .cause(bottom)
            .build();
        let top = DynamoError::builder()
            .error_type(ErrorType::Unknown)
            .message("top")
            .cause(mid)
            .build();
        assert!(!match_error_chain(&top, &[ErrorType::Disconnected], &[ErrorType::Cancelled]));
    }
}
