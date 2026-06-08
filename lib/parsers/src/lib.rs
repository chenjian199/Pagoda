// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-License-Identifier: Apache-2.0

//! # pagoda-parsers
//!
//! ## 设计意图
//! 本 crate 汇集两类后处理解析器：工具调用（`tool_calling`）与推理段（`reasoning`）。
//! crate 根仅承担装配与门面职责：声明子模块并将公开符号平铺到顶层。
//!
//! ## 外部契约
//! - 对外暴露两个公开模块：`reasoning` 与 `tool_calling`。
//! - 两个模块的公开项在 crate 顶层重新导出，调用方可直接访问。
//!
//! ## 实现要点
//! - 顶层不放置业务逻辑，避免门面层与实现层耦合。

pub mod reasoning;
pub mod tool_calling;

// 将两个子模块的公开符号平铺到 crate 顶层，保持扁平访问路径。
pub use reasoning::*;
pub use tool_calling::*;
