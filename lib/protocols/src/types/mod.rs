// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
// 推理 API 请求与响应所用的类型。
//
// 基础 OpenAI 类型从上游 async-openai 重新导出。
// 推理服务扩展与 Anthropic 类型在本地定义。

//! 协议类型聚合模块。
//!
//! ## 设计意图
//! 把「本地自有类型」与「上游重新导出类型」汇聚到统一命名空间，对外屏蔽两者的来源差异。
//!
//! ## 外部契约
//! - 重新导出 `chat`、`completion` 的全部公开类型。
//! - 重新导出上游 `embeddings`、`images` 的全部类型。
//! - 公开 `anthropic`、`responses` 子模块。
//! - 提供 `UninitializedFieldError` 到 `OpenAIError` 的 `From` 转换。
//!
//! ## 实现要点
//! 本地模块优先于上游 glob 导入，从而在需要时遮蔽上游同名类型。

// === SECTION: 本地定义模块 ===
pub mod anthropic;
mod chat;
mod completion;
pub mod responses;

// === SECTION: 本地类型重新导出 ===
pub use chat::*;
pub use completion::*;

// === SECTION: 上游重新导出（仅类型，不含 HTTP 客户端） ===

// Embeddings（完整重新导出）
pub use async_openai::types::embeddings::*;

// Images
pub use async_openai::types::images::*;

// === SECTION: 本地类型的便捷 impl ===
mod impls;

use crate::error::OpenAIError;
use derive_builder::UninitializedFieldError;

impl From<UninitializedFieldError> for OpenAIError {
    fn from(value: UninitializedFieldError) -> Self {
        OpenAIError::InvalidArgument(value.to_string())
    }
}
