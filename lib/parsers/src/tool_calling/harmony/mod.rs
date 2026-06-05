// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-License-Identifier: Apache-2.0
//
// API-compatible implementation based on the public interfaces and behavioral
// contracts of NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
// Implementation rewritten by PAGODA.

//!
//! ## 设计意图
//!
//! ## 外部契约

pub use super::config::JsonParserConfig;
pub use super::{config, response};

pub mod harmony_parser;

pub use harmony_parser::{detect_tool_call_start_harmony, parse_tool_calls_harmony_complete};

/// ## 实现要点
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
