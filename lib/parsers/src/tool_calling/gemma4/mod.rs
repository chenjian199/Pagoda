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
//! - 重新导出「检测起始 / 定位结束 / 解析」三组函数。

mod parser;

pub(crate) use parser::{TOOL_CALL_END, TOOL_CALL_START};
pub use parser::{
    detect_tool_call_start_gemma4, find_tool_call_end_position_gemma4, try_tool_call_parse_gemma4,
};
