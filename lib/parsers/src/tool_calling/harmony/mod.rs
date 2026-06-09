// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # tool_calling::harmony
//!
//! ## 设计意图
//! 封装 Harmony 格式的工具调用解析，并提供结束位置定位的门面函数。
//!
//! ## 外部契约
//! - 重新导出 `detect_tool_call_start_harmony`、`parse_tool_calls_harmony_complete`。
//! - [`find_tool_call_end_position_harmony`] 返回结束 token 之后的偏移；未命中时返回 chunk 总长。

pub use super::config::JsonParserConfig;
pub use super::{config, response};

pub mod harmony_parser;

pub use harmony_parser::{detect_tool_call_start_harmony, parse_tool_calls_harmony_complete};

/// ## 实现要点
/// 从配置中取首个结束 token（默认 `<|call|>`），以「最后一次出现」位置为准，
/// 返回该 token 末尾的字节偏移；若未出现则视为尚未结束，返回整个 chunk 长度。
pub fn find_tool_call_end_position_harmony(chunk: &str, config: &JsonParserConfig) -> usize {
    let end_token = config
        .tool_call_end_tokens
        .first()
        .map_or("<|call|>", |token| token.as_str());
    match chunk.rfind(end_token) {
        Some(pos) => pos + end_token.len(),
        None => chunk.len(),
    }
}
