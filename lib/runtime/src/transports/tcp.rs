// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # TCP 直连 transport
//!
//! ## 设计意图
//! TCP 直连的"真身"住在 [`crate::pipeline::network::tcp`] 模块里 ——
//! 它是 pipeline 层的实现。但 transports 命名空间为了让"按 transport 找东西"
//! 的访问路径保持一致，**在此处再导出**一份 `client` / `server`。
//!
//! ## 外部契约
//! 公开符号：`pub use crate::pipeline::network::tcp::{client, server};`

pub use crate::pipeline::network::tcp::{client, server};
