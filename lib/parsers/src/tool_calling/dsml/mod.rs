// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-License-Identifier: Apache-2.0

//! # tool_calling::dsml
//!
//! ## 设计意图
//! 封装 DSML 格式的工具调用解析器。
//!
//! ## 外部契约
//! - 重新导出「检测起始 / 定位结束 / 解析」三组函数。

pub use super::response;

mod parser;

pub use parser::{
    detect_tool_call_start_dsml, find_tool_call_end_position_dsml, try_tool_call_parse_dsml,
};
