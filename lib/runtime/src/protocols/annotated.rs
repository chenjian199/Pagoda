// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `Annotated<R>`：带元信息的流式增量
//!
//! ## 设计意图
//!
//! LLM 流式接口在每一个增量上不只是“纯数据”：它可能携带 SSE 的
//! `id` / `event` / `comment`，可能是一条结构化错误，也可能是不带数据的
//! 元事件（如 metrics / trace 注解）。本模块用一个统一的 [`Annotated<R>`]
//! 容器把这五个维度（`data` / `id` / `event` / `comment` / `error`）打包，
//! 让管道里的每一节都能透明地搬运/转译它，**而不需要为成功路径与错误路径
//! 各写一套类型**。
//!
//! ## 外部契约
//!
//! - [`AnnotationsProvider`]：抽象“能枚举注解”的能力，默认提供
//!   `has_annotation()` 包含判定；
//! - [`Annotated<R>`]：5 个 `pub` 字段（保留以兼容直接结构体字面量构造）；
//!   构造器 `from_data` / `from_error` / `from_annotation`；状态查询
//!   `is_ok` / `is_err` / `is_error` / `is_event`；负载操作 `transfer` /
//!   `map_data`；转结果 `ok` / `into_result`；
//! - [`MaybeError`] for `Annotated<R>`：以错误事件 + `comment` 文本回退
//!   实现 `err()`。
//!
//! ## 实现要点
//!
//! - **错误判定单点**：[`ERROR_EVENT`] 常量集中保存事件名 `"error"`，所有
//!   方法都通过 [`Annotated::is_error_event`] 内部方法查询，避免散落的
//!   字面量。
//! - **错误文本抽取共享**：[`Annotated::error_message`] 把
//!   “优先 `DynamoError` → 回退 `comment.join(", ")` → 兜底 `unknown error`”
//!   这套逻辑抽成一个私有方法，被 `ok` / `into_result` 共用。
//! - **元数据保留**：`transfer` 与 `map_data` 都通过解构 + 命名字段重组的
//!   方式显式列出 5 个字段，保证未来新增字段时编译器立刻报错（结构性提醒）。
//! - **`map_data` 失败路径**：转换函数返回 `Err` 时不静默丢弃，而是构造
//!   错误注解返回，保持“流式错误传播”语义。

use super::maybe_error::MaybeError;
use crate::error::DynamoError;
use anyhow::{Result, anyhow as error};
use serde::{Deserialize, Serialize};

/// 当 `Annotated` 表示错误时统一使用的事件名。
const ERROR_EVENT: &str = "error";

// === TRAIT: AnnotationsProvider =============================================

/// 表示对象可以暴露一组字符串注解的能力。
///
/// 典型使用：请求/响应包装类型实现本 trait 后，下游可以用
/// `has_annotation("trace")` 之类的语义化查询代替手动操作 `Vec<String>`。
pub trait AnnotationsProvider {
    /// 返回注解列表；无注解时返回 `None`。
    fn annotations(&self) -> Option<Vec<String>>;

    /// 注解列表是否包含 `annotation`。
    ///
    /// 默认实现：先取 `annotations()`，再用迭代器 `any` 做线性匹配。
    /// 没有注解视为不包含。
    fn has_annotation(&self, annotation: &str) -> bool {
        self.annotations()
            .as_deref()
            .map(|list| list.iter().any(|candidate| candidate == annotation))
            .unwrap_or(false)
    }
}

// === STRUCT: Annotated ======================================================

/// 带元信息的流式增量。
///
/// 字段语义：
/// - `data`：实际负载（业务数据），`None` 表示本增量是元事件；
/// - `id`：SSE id，用来串联同一逻辑事件的多次推送；
/// - `event`：SSE event 名，特殊值 [`ERROR_EVENT`] 触发错误分支；
/// - `comment`：SSE comment 行；错误场景下作为人类可读消息的回退；
/// - `error`：结构化 [`DynamoError`]，与 `event = "error"` 同时使用。
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Annotated<R> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<R>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<DynamoError>,
}

// === IMPL: 构造器 ===========================================================

impl<R> Annotated<R> {
    /// 构造一条仅承载错误信息的注解。
    ///
    /// `event` 固定为 [`ERROR_EVENT`]，`error` 由 `DynamoError::msg(message)`
    /// 包装；其余字段留空。
    pub fn from_error(error: String) -> Self {
        Self {
            data: None,
            id: None,
            event: Some(ERROR_EVENT.to_string()),
            comment: None,
            error: Some(DynamoError::msg(error)),
        }
    }

    /// 构造一条只承载成功数据的注解（无元信息）。
    pub fn from_data(data: R) -> Self {
        Self {
            data: Some(data),
            id: None,
            event: None,
            comment: None,
            error: None,
        }
    }

    /// 构造一条 `event = name, comment = [serde_json(value)]` 的元事件注解。
    ///
    /// 失败仅可能来自 `serde_json::to_string` 序列化错误。
    pub fn from_annotation<S: Serialize>(
        name: impl Into<String>,
        value: &S,
    ) -> Result<Self, serde_json::Error> {
        let payload = serde_json::to_string(value)?;
        Ok(Self {
            data: None,
            id: None,
            event: Some(name.into()),
            comment: Some(vec![payload]),
            error: None,
        })
    }
}

// === IMPL: 状态查询 + 文本抽取 ==============================================

impl<R> Annotated<R> {
    /// 内部：事件名是否等于 [`ERROR_EVENT`]。
    fn is_error_event(&self) -> bool {
        self.event.as_deref() == Some(ERROR_EVENT)
    }

    /// 内部：抽取错误的可读文本，遵循“`DynamoError` 优先 → `comment` 拼接
    /// → 兜底 `unknown error`”三段优先级，被 `ok` / `into_result` 共用。
    fn error_message(&self, sep: &str) -> String {
        if let Some(err) = self.error.as_ref() {
            return err.to_string();
        }
        match self.comment.as_ref() {
            Some(list) if !list.is_empty() => list.join(sep),
            _ => "unknown error".to_string(),
        }
    }

    /// 事件名等于 [`ERROR_EVENT`] 视为错误。
    pub fn is_error(&self) -> bool {
        self.is_error_event()
    }

    /// `Annotated` 视角下的“成功”：非错误事件即可（包含纯元事件）。
    pub fn is_ok(&self) -> bool {
        !self.is_error_event()
    }

    /// `is_ok` 的否定。
    pub fn is_err(&self) -> bool {
        self.is_error_event()
    }

    /// 是否带 `event` 字段（无论 OK 还是 Error）。
    pub fn is_event(&self) -> bool {
        self.event.is_some()
    }
}

// === IMPL: 负载变换 =========================================================

impl<R> Annotated<R> {
    /// 把当前实例的元信息（id/event/comment/error）转给一个新类型 `U`，
    /// 负载替换为 `data`。
    ///
    /// 通过解构 + 命名字段重组确保新增字段时编译报错。
    pub fn transfer<U: Serialize>(self, data: Option<U>) -> Annotated<U> {
        let Self {
            data: _,
            id,
            event,
            comment,
            error,
        } = self;
        Annotated::<U> {
            data,
            id,
            event,
            comment,
            error,
        }
    }

    /// 对 `data` 字段做 `R -> Result<U, String>` 变换。
    ///
    /// - 原本 `data` 为 `None` → 直接换皮，保留所有元数据；
    /// - 变换成功 → 元数据原样保留，`data` 替换为新值；
    /// - 变换失败 → 整体被替换为 [`Annotated::from_error`]，**丢弃原元数据**
    ///   （等价于“一次失败让本条流增量退化为错误事件”）。
    pub fn map_data<U, F>(self, transform: F) -> Annotated<U>
    where
        F: FnOnce(R) -> Result<U, String>,
    {
        let Self {
            data,
            id,
            event,
            comment,
            error,
        } = self;

        let Some(value) = data else {
            return Annotated::<U> {
                data: None,
                id,
                event,
                comment,
                error,
            };
        };

        match transform(value) {
            Ok(mapped) => Annotated::<U> {
                data: Some(mapped),
                id,
                event,
                comment,
                error,
            },
            Err(err) => Annotated::from_error(err),
        }
    }
}

// === IMPL: 转结果 ===========================================================

impl<R> Annotated<R> {
    /// 把自身投影为 `Result<Self, String>`。
    ///
    /// 错误事件 → `Err(message)`；其它情况 → `Ok(self)`。
    pub fn ok(self) -> Result<Self, String> {
        if self.is_error_event() {
            return Err(self.error_message(", "));
        }
        Ok(self)
    }

    /// 投影为 `anyhow::Result<Option<R>>`：
    /// - 有 `data` → `Ok(Some(data))`；
    /// - 错误事件 → `Err(anyhow!)`；
    /// - 其它（无数据元事件）→ `Ok(None)`。
    pub fn into_result(self) -> Result<Option<R>> {
        if let Some(data) = self.data {
            return Ok(Some(data));
        }
        if self.is_error_event() {
            return Err(error!("{}", self.error_message(", ")));
        }
        Ok(None)
    }
}

// === IMPL: MaybeError ======================================================

impl<R> MaybeError for Annotated<R>
where
    R: for<'de> Deserialize<'de>,
{
    fn from_err(err: impl std::error::Error + 'static) -> Self {
        let boxed: Box<dyn std::error::Error + 'static> = Box::new(err);
        Self {
            data: None,
            id: None,
            event: Some(ERROR_EVENT.to_string()),
            comment: None,
            error: Some(DynamoError::from(boxed)),
        }
    }

    /// 注意：当 `event == "error"` 但 `error` 字段缺失时，回退到
    /// `comment.join("; ")` 文本并用 `DynamoError::msg` 包装。
    fn err(&self) -> Option<DynamoError> {
        if !self.is_error_event() {
            return None;
        }
        if let Some(err) = self.error.clone() {
            return Some(err);
        }
        Some(DynamoError::msg(self.error_message("; ")))
    }
}

// === 单元测试 ================================================================

#[cfg(test)]
mod tests {
    use super::*;

    struct TestAnnotationsProvider(Option<Vec<String>>);

    impl AnnotationsProvider for TestAnnotationsProvider {
        fn annotations(&self) -> Option<Vec<String>> {
            self.0.clone()
        }
    }

    /// ## 测试过程
    /// 用 `from_data` / `from_error` / `from_err` 三种构造路径分别造出
    /// 容器，验证 `MaybeError::err` / `is_ok` / `is_err` 的相互关系。
    /// ## 意义
    /// 守护 `Annotated` 与 `MaybeError` 的最小契约。
    #[test]
    fn test_maybe_error() {
        let annotated = Annotated::from_data("Test data".to_string());
        assert!(annotated.err().is_none());
        assert!(annotated.is_ok());

        let annotated = Annotated::<String>::from_error("Test error 2".to_string());
        assert!(annotated.err().is_some());
        assert!(annotated.is_err());

        let dynamo_err = DynamoError::msg("Test error 3");
        let annotated = Annotated::<String>::from_err(dynamo_err);
        assert!(annotated.is_err());
    }

    /// ## 测试过程
    /// 用 `DynamoError` 走 `from_err`，验证错误文本可被读回。
    /// ## 意义
    /// 防止 `from_err` 在装箱过程中把信息抹掉。
    #[test]
    fn test_from_err() {
        let err = DynamoError::msg("connection lost");
        let annotated = Annotated::<String>::from_err(err);

        assert!(annotated.is_err());
        let err = annotated.err().unwrap();
        assert!(err.to_string().contains("connection lost"));
    }

    /// ## 测试过程
    /// JSON 编码 + 解码后再调 `err()`，断言错误文本仍包含原始消息。
    /// ## 意义
    /// 保证 `serde` 序列化路径不丢错误内容。
    #[test]
    fn test_error_serialization() {
        let err = DynamoError::msg("test error");
        let annotated = Annotated::<String>::from_err(err);

        let json = serde_json::to_string(&annotated).unwrap();
        let deserialized: Annotated<String> = serde_json::from_str(&json).unwrap();

        assert!(deserialized.is_err());
        assert!(deserialized.err().unwrap().to_string().contains("test error"));
    }

    /// ## 测试过程
    /// 把错误注解通过 `transfer` 变换到新类型，验证错误信息不被丢弃。
    /// ## 意义
    /// `transfer` 在流式中转节点频繁使用，必须保留错误元数据。
    #[test]
    fn test_transfer_preserves_error() {
        let err = DynamoError::msg("request timed out");
        let annotated = Annotated::<String>::from_err(err);

        let transferred: Annotated<i32> = annotated.transfer(None);
        assert!(transferred.err().is_some());
    }

    /// ## 测试过程
    /// 错误注解走 `ok()`，断言进入 Err 分支且消息正确传递。
    /// ## 意义
    /// 验证 `Result` 投影对错误事件的处理符合“向调用方暴露原始消息”的约定。
    #[test]
    fn test_ok_method() {
        let err = DynamoError::msg("connection lost");
        let annotated = Annotated::<String>::from_err(err);

        let result = annotated.ok();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("connection lost"));
    }

    /// ## 测试过程
    /// 用包含 / 不包含目标注解的两种 provider，验证 `has_annotation` 行为。
    /// ## 意义
    /// 守护 `AnnotationsProvider` 默认实现的语义。
    #[test]
    fn test_annotations_provider_has_annotation() {
        let provider =
            TestAnnotationsProvider(Some(vec!["timing".to_string(), "trace".to_string()]));
        let empty_provider = TestAnnotationsProvider(None);

        assert!(provider.has_annotation("trace"));
        assert!(!provider.has_annotation("missing"));
        assert!(!empty_provider.has_annotation("trace"));
    }

    /// ## 测试过程
    /// 用一个 JSON 对象走 `from_annotation`，验证 `event` 名和 `comment`
    /// 的填充结果。
    /// ## 意义
    /// 保证元事件构造路径不引入额外字段或漏填字段。
    #[test]
    fn test_from_annotation_populates_event_and_comment() {
        let annotated = Annotated::<String>::from_annotation(
            "metrics",
            &serde_json::json!({"latency_ms": 12}),
        )
        .unwrap();

        assert!(annotated.data.is_none());
        assert_eq!(annotated.event.as_deref(), Some("metrics"));
        assert_eq!(
            annotated.comment,
            Some(vec![r#"{"latency_ms":12}"#.to_string()])
        );
        assert!(annotated.is_event());
        assert!(!annotated.is_error());
    }

    /// ## 测试过程
    /// 直接构造一个错误事件但不带 `error` 字段，断言 `ok()` 回退到
    /// `comment.join(", ")`。
    /// ## 意义
    /// 守护“`error` 缺失时由 `comment` 提供文本”的回退路径。
    #[test]
    fn test_ok_uses_comment_fallback_for_error_events() {
        let annotated = Annotated::<String> {
            data: None,
            id: Some("req-1".to_string()),
            event: Some("error".to_string()),
            comment: Some(vec!["first".to_string(), "second".to_string()]),
            error: None,
        };

        let result = annotated.ok();
        assert_eq!(result.unwrap_err(), "first, second");
    }

    /// ## 测试过程
    /// `map_data` 成功路径：把数字翻倍，断言所有元数据原样保留。
    /// ## 意义
    /// 保证成功路径不会“顺手”改动 id/event/comment。
    #[test]
    fn test_map_data_success_preserves_metadata() {
        let annotated = Annotated {
            data: Some(21_i32),
            id: Some("chunk-7".to_string()),
            event: Some("delta".to_string()),
            comment: Some(vec!["note".to_string()]),
            error: None,
        };

        let mapped = annotated.map_data(|value| Ok(value * 2));

        assert_eq!(mapped.data, Some(42));
        assert_eq!(mapped.id.as_deref(), Some("chunk-7"));
        assert_eq!(mapped.event.as_deref(), Some("delta"));
        assert_eq!(mapped.comment, Some(vec!["note".to_string()]));
    }

    /// ## 测试过程
    /// `map_data` 失败路径：返回 `Err`，验证整条注解被替换成错误事件。
    /// ## 意义
    /// 保证“一次转换失败 → 错误事件”这一退化路径的稳定性。
    #[test]
    fn test_map_data_failure_returns_error_annotation() {
        let annotated = Annotated::from_data("bad payload".to_string());

        let mapped: Annotated<usize> = annotated.map_data(|_| Err("decode failed".to_string()));

        assert!(mapped.is_error());
        assert!(mapped.data.is_none());
        assert_eq!(mapped.event.as_deref(), Some("error"));
        assert!(mapped.err().unwrap().to_string().contains("decode failed"));
    }

    /// ## 测试过程
    /// 覆盖 `into_result` 三条主要分支：有数据、空事件、错误事件含 comment。
    /// ## 意义
    /// 把 `into_result` 的状态机一次性验证完整。
    #[test]
    fn test_into_result_handles_data_comment_errors_and_empty_events() {
        let with_data = Annotated::from_data(7_u32);
        assert_eq!(with_data.into_result().unwrap(), Some(7));

        let empty_event = Annotated::<u32> {
            data: None,
            id: None,
            event: Some("progress".to_string()),
            comment: Some(vec!["halfway".to_string()]),
            error: None,
        };
        assert_eq!(empty_event.into_result().unwrap(), None);

        let comment_error = Annotated::<u32> {
            data: None,
            id: None,
            event: Some("error".to_string()),
            comment: Some(vec!["transient failure".to_string()]),
            error: None,
        };
        assert!(
            comment_error
                .into_result()
                .unwrap_err()
                .to_string()
                .contains("transient failure")
        );
    }

    /// ## 测试过程
    /// 错误事件 `comment` 非空与空两种子状态走 `err()`，验证 `;` 拼接
    /// 与 `unknown error` 兜底。
    /// ## 意义
    /// 守护 `MaybeError::err` 的回退顺序与 `ok` / `into_result` 一致。
    #[test]
    fn test_err_falls_back_to_comment_and_unknown_error() {
        let comment_error = Annotated::<String> {
            data: None,
            id: None,
            event: Some("error".to_string()),
            comment: Some(vec!["bad gateway".to_string(), "retry later".to_string()]),
            error: None,
        };
        let unknown_error = Annotated::<String> {
            data: None,
            id: None,
            event: Some("error".to_string()),
            comment: Some(vec![]),
            error: None,
        };

        assert_eq!(
            comment_error.err().unwrap().to_string(),
            "Unknown: bad gateway; retry later"
        );
        assert_eq!(
            unknown_error.err().unwrap().to_string(),
            "Unknown: unknown error"
        );
    }

    /// ## 测试过程
    /// 通过 `from_err` 注入结构化 `DynamoError` 后调 `into_result`，
    /// 验证 `anyhow::Error` 文本仍包含原始消息。
    /// ## 意义
    /// 守护结构化错误路径在 `into_result` 中的可观察性。
    #[test]
    fn test_into_result() {
        let err = DynamoError::msg("connection lost");
        let annotated = Annotated::<String>::from_err(err);

        let result = annotated.into_result();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("connection lost"));
    }
}
