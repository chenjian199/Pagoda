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

pub use super::response;

mod parser;

pub use parser::{
    detect_tool_call_start_dsml, find_tool_call_end_position_dsml, try_tool_call_parse_dsml,
};
