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

pub use super::{config, response};

pub mod pythonic_parser;

pub use pythonic_parser::{detect_tool_call_start_pythonic, try_tool_call_parse_pythonic};

/// ## 实现要点
pub fn find_tool_call_end_position_pythonic(chunk: &str) -> usize {
    chunk.len()
}
