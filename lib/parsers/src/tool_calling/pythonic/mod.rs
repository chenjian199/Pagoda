// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # tool_calling::pythonic
//!
//! ## 设计意图
//! 封装 Pythonic（类 Python 函数调用字面）格式的工具调用解析。
//!
//! ## 外部契约
//! - 重新导出 `detect_tool_call_start_pythonic`、`try_tool_call_parse_pythonic`。
//! - [`find_tool_call_end_position_pythonic`] 始终返回整个 chunk 长度（该方言不依赖独立结束 token）。

pub use super::{config, response};

pub mod pythonic_parser;

pub use pythonic_parser::{detect_tool_call_start_pythonic, try_tool_call_parse_pythonic};

/// ## 实现要点
/// Pythonic 方言以闭合括号作为边界，无独立的结束 token，故结束位置恒为 chunk 末端。
pub fn find_tool_call_end_position_pythonic(chunk: &str) -> usize {
    chunk.len()
}
