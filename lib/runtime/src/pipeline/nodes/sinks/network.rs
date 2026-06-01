// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::nodes::sinks::network` —— 网络出口 sink 占位模块
//!
//! ## 设计意图
//! 为 “把响应通过网络反向回传” 的 sink 实现预留独立模块。当前仅保留
//! 文件骨架（无符号），让模块树与 `sinks::base` / `sinks::pipeline` /
//! `sinks::segment` 形成对称结构。
//!
//! ## 外部契约
//! - 不导出任何符号；外部不依赖本文件的具体内容。
//! - 文件存在性本身是契约的一部分：父模块 `sinks` 可在未来填充本文件
//!   而无需调整模块声明。
//!
//! ## 实现要点
//! - 保持空实现，避免引入未使用代码触发 lint。

// === SECTION: 空占位（保留模块文件以维持模块树对称） ===
