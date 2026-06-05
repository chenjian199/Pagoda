// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-License-Identifier: Apache-2.0
//
// API-compatible implementation based on the public interfaces and behavioral
// contracts of NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
// Implementation rewritten by PAGODA.

//! # tool_calling::tools
//!
//! ## 设计意图
//!
//! ## 外部契约
//!
//! ## 实现要点
//! - 三个入口的「解析 → 转换 → 打包」流程高度同构，故抽出私有助手集中表达类型转换，

use pagoda_protocols::types::{
    ChatCompletionMessageToolCall, ChatCompletionMessageToolCallChunk, FunctionCall,
    FunctionCallStream, FunctionType,
};

pub use super::config::ToolCallConfig;
pub use super::parsers::{detect_and_parse_tool_call, detect_and_parse_tool_call_with_recovery};
use super::response::ToolCallResponse;

/// 解析结果的标准载荷：工具调用列表 + 剩余普通文本。
type ParsedToolCalls = (Vec<ToolCallResponse>, Option<String>);

// === SECTION: 内部类型转换助手 ===

/// 把一个内部 [`ToolCallResponse`] 转换为对外的非流式工具调用类型。
fn to_aggregate_call(parsed: ToolCallResponse) -> ChatCompletionMessageToolCall {
    ChatCompletionMessageToolCall {
        id: parsed.id,
        r#type: FunctionType::Function,
        function: FunctionCall {
            name: parsed.function.name,
            arguments: parsed.function.arguments,
        },
    }
}

/// 把一个内部 [`ToolCallResponse`] 连同其序号转换为对外的流式分片类型。
fn to_stream_chunk(index: usize, parsed: ToolCallResponse) -> ChatCompletionMessageToolCallChunk {
    ChatCompletionMessageToolCallChunk {
        index: index as u32,
        id: Some(parsed.id),
        r#type: Some(FunctionType::Function),
        function: Some(FunctionCallStream {
            name: Some(parsed.function.name),
            arguments: Some(parsed.function.arguments),
        }),
    }
}

// === SECTION: 聚合解析入口 ===

/// 以聚合方式解析字符串中的结构化工具调用。
///
/// 解析成功时返回 `ChatCompletionMessageToolCall` 列表。
///
/// 流式 jail 调用方（`should_exit_jail_early`、流中途早退确认）必须继续使用本函数：
/// `allow_eof_recovery` 保持关闭，使解析器不会在结束 token 真正到达前就宣称工具调用完整。
pub async fn try_tool_call_parse_aggregate(
    message: &str,
    parser_str: Option<&str>,
    tools: Option<&[super::ToolDefinition]>,
) -> anyhow::Result<(Vec<ChatCompletionMessageToolCall>, Option<String>)> {
    if parser_str.is_none() {
        tracing::debug!("No tool parser provided. Trying parsing with default parser.");
    } else {
        tracing::debug!("Using tool parser: {:?}", parser_str);
    }
    let (parsed, content): ParsedToolCalls =
        detect_and_parse_tool_call(message, parser_str, tools).await?;
    let calls = parsed.into_iter().map(to_aggregate_call).collect();
    Ok((calls, content))
}

/// [`try_tool_call_parse_aggregate`] 的「收尾」变体，启用 EOF 恢复（缺失外层结束 token、
/// 截断的 JSON 参数）。仅供流结束 / 非流式聚合路径使用，切勿用于流式 jail 早退逻辑。
pub async fn try_tool_call_parse_aggregate_finalize(
    message: &str,
    parser_str: Option<&str>,
    tools: Option<&[super::ToolDefinition]>,
) -> anyhow::Result<(Vec<ChatCompletionMessageToolCall>, Option<String>)> {
    let (parsed, content): ParsedToolCalls =
        detect_and_parse_tool_call_with_recovery(message, parser_str, tools).await?;
    let calls = parsed.into_iter().map(to_aggregate_call).collect();
    Ok((calls, content))
}

// === SECTION: 流式解析入口 ===

/// 以流式（delta）方式解析字符串中的结构化工具调用。
///
/// 解析成功时返回 `ChatCompletionMessageToolCallChunk` 列表。
pub async fn try_tool_call_parse_stream(
    message: &str,
    parser_str: Option<&str>,
    tools: Option<&[super::ToolDefinition]>,
) -> anyhow::Result<(Vec<ChatCompletionMessageToolCallChunk>, Option<String>)> {
    let (parsed, content): ParsedToolCalls =
        detect_and_parse_tool_call(message, parser_str, tools).await?;
    let chunks = parsed
        .into_iter()
        .enumerate()
        .map(|(index, parsed)| to_stream_chunk(index, parsed))
        .collect();
    Ok((chunks, content))
}
