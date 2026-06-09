// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # tool_calling::gemma4
//!
//! ## 设计意图
//! 封装 Gemma4 格式的工具调用解析器。
//!
//! ## 外部契约
//! - 重新导出「检测起始 / 定位结束 / 解析」三组函数。
//! - crate 内部导出起止 token 常量 `TOOL_CALL_START` / `TOOL_CALL_END`。

mod parser;

pub(crate) use parser::{TOOL_CALL_END, TOOL_CALL_START};
pub use parser::{
    detect_tool_call_start_gemma4, find_tool_call_end_position_gemma4, try_tool_call_parse_gemma4,
};
