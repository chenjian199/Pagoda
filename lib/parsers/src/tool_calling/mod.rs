// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 工具调用解析器门面
//!
//! ## 设计意图
//! 汇集面向各类模型输出格式的工具调用（tool call）解析器，并把各子方言
//! （JSON / Pythonic / XML / DSML / Gemma4 / Harmony）的入口统一接入同一个门面。
//!
//! ## 外部契约
//! - 导出公共类型 [`ToolDefinition`]（字段 `name: String`、`parameters: Option<Value>`）。
//! - 重新导出各子模块的公开入口函数与配置/响应类型，名称与签名保持不变。
//!
//! ## 实现要点
//! - `tests` 模块仅在 `cfg(test)` 下可见。

use serde_json::Value;

pub mod config;
pub mod dsml;
pub mod gemma4;
pub mod harmony;
pub mod json;
pub mod parsers;
pub mod pythonic;
pub mod response;
#[cfg(test)]
pub mod tests;
pub mod tools;
pub mod xml;

/// 工具定义，携带函数名与可选的参数 schema。
#[derive(Debug, Clone)]
pub struct ToolDefinition {
    pub name: String,
    pub parameters: Option<Value>,
}

// === SECTION: 公开符号重新导出 ===
// 把各子模块的公开入口提升到 `tool_calling` 顶层，供调用方统一访问。
pub use config::{
    JsonParserConfig, KimiK2ParserConfig, ParserConfig, ToolCallConfig, XmlParserConfig,
};
pub use dsml::try_tool_call_parse_dsml;
pub use gemma4::try_tool_call_parse_gemma4;
pub use harmony::parse_tool_calls_harmony_complete;
pub use json::try_tool_call_parse_json;
pub use parsers::{
    detect_and_parse_tool_call, detect_tool_call_start, find_tool_call_end_position,
    try_tool_call_parse,
};
pub use pythonic::try_tool_call_parse_pythonic;
pub use response::{CalledFunction, ToolCallResponse, ToolCallType};
pub use tools::{
    try_tool_call_parse_aggregate, try_tool_call_parse_aggregate_finalize,
    try_tool_call_parse_stream,
};
pub use xml::try_tool_call_parse_kimi_k2;
pub use xml::try_tool_call_parse_xml;
