// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 协议类型操作所用的错误类型。
//!
//! ## 设计意图
//! 为协议层的反序列化、参数校验以及上游 OpenAI API 返回的错误对象提供统一的错误表达，
//! 让调用方能用单一枚举处理三类失败来源。
//!
//! ## 外部契约
//! - 公开枚举 `OpenAIError`，含 `ApiError`、`JSONDeserialize`、`InvalidArgument` 三个变体。
//! - 公开结构体 `ApiError`（可序列化）及其 `Display` 实现。
//! - 公开结构体 `WrappedError`，用于解析嵌套在 `"error"` 键下的错误对象。
//!
//! ## 实现要点
//! `ApiError` 的 `Display` 采用「分段拼接」策略：把可选的 type/param/code 按存在性
//! 逐段压入向量，最后以空格连接，避免多分支的格式化字符串。

use serde::{Deserialize, Serialize};

// === SECTION: 顶层错误枚举 ===

/// 协议层统一错误类型。
#[derive(Debug, thiserror::Error)]
pub enum OpenAIError {
    /// OpenAI 在 API 调用失败时返回的错误对象，携带详细信息。
    #[error("{0}")]
    ApiError(ApiError),
    /// 无法把响应反序列化为 Rust 类型时产生的错误。
    #[error("failed to deserialize api response: {0}")]
    JSONDeserialize(serde_json::Error),
    /// 客户端侧校验错误，或在发起 API 调用前 builder 构建失败。
    #[error("invalid args: {0}")]
    InvalidArgument(String),
}

// === SECTION: API 错误对象 ===

/// OpenAI API 失败时返回的错误对象。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ApiError {
    /// 人类可读的错误描述。
    pub message: String,
    /// 错误分类（如 `invalid_request_error`）。
    pub r#type: Option<String>,
    /// 触发错误的参数名（若适用）。
    pub param: Option<String>,
    /// 机器可读的错误码（若适用）。
    pub code: Option<String>,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 逐段收集存在的字段，最后统一以空格连接，避免多分支格式化。
        let mut segments: Vec<String> = Vec::with_capacity(4);

        if let Some(kind) = self.r#type.as_deref() {
            segments.push(format!("{kind}:"));
        }

        segments.push(self.message.clone());

        if let Some(param) = self.param.as_deref() {
            segments.push(format!("(param: {param})"));
        }

        if let Some(code) = self.code.as_deref() {
            segments.push(format!("(code: {code})"));
        }

        f.write_str(&segments.join(" "))
    }
}

// === SECTION: 嵌套错误包装 ===

/// 用于反序列化嵌套在 JSON `"error"` 键下错误对象的包装类型。
#[derive(Debug, Deserialize, Serialize)]
pub struct WrappedError {
    /// 实际的错误对象。
    pub error: ApiError,
}
