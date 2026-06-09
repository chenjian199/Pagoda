// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-License-Identifier: Apache-2.0

//! # tool_calling::xml
//!
//! ## 设计意图
//! 汇集基于 XML 标签的工具调用解析器：通用 XML、GLM-4.7、Kimi-K2 三种方言。
//!
//! ## 外部契约
//! - 对外重新导出三种方言各自的「检测起始 / 定位结束 / 解析」三组函数。

pub use super::response;

mod glm47_parser;
mod kimi_k2_parser;
mod parser;

pub use glm47_parser::{
    detect_tool_call_start_glm47, find_tool_call_end_position_glm47, try_tool_call_parse_glm47,
};
pub use kimi_k2_parser::{
    detect_tool_call_start_kimi_k2, find_tool_call_end_position_kimi_k2,
    try_tool_call_parse_kimi_k2,
};
pub use parser::{
    detect_tool_call_start_xml, find_tool_call_end_position_xml, try_tool_call_parse_xml,
};
