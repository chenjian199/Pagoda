// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # pagoda-parsers
//!
//! ## 设计意图
//! 本 crate 汇集两类后处理解析器：工具调用（`tool_calling`）与推理段（`reasoning`）。
//! crate 根仅承担「装配与门面」职责——声明子模块并把两个子模块的公开符号
//! 平铺到 crate 顶层，使下游可以通过 `pagoda_parsers::xxx` 直接访问，而无需
//! 关心符号归属于哪个子模块。
//!
//! ## 外部契约
//! - 对外暴露两个公开模块：`reasoning` 与 `tool_calling`。
//! - 这两个模块的全部公开项均在 crate 顶层重新导出（glob re-export）。
//!
//! ## 实现要点
//! - 顶层不放置任何业务逻辑，避免门面层与实现层耦合。

pub mod reasoning;
pub mod tool_calling;

// 将两个子模块的公开符号平铺到 crate 顶层，保持调用方的扁平访问路径。
pub use reasoning::*;
pub use tool_calling::*;
