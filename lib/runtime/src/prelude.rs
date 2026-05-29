// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 预置导入（`prelude`）
//!
//! ## 设计意图
//! 为上层 crate 提供一个“一行 `use ::prelude::*;` 即可拿到常用 trait”的集中入口，
//! 避免调用方逐项从 [`crate::traits`] 导入 `RuntimeProvider` / `DistributedRuntimeProvider`。
//!
//! ## 外部契约
//! 本模块**只**重导出 [`crate::traits`] 下的公开符号（`pub use crate::traits::*;`），
//! 不新增、不掩盖、不别名，以保证调用方看到的符号集与 [`crate::traits`] 严格一致。
//!
//! ## 实现要点
//! 本文件遵守“prelude 零实现”原则：不引入额外依赖、不包含可执行代码，
//! 仅作为重导出联走点。如需调整抑制 / 增加符号，请直接修改 [`crate::traits`]。

pub use crate::traits::*;
