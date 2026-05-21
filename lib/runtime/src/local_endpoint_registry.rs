// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 兼容层：重新导出 `local_portname_registry`。
//!
//! 新代码请使用 `crate::local_portname_registry::LocalPortNameRegistry`。

pub use crate::local_portname_registry::*;
