// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! TCP 传输层 re-export。
//!
//! 直接复用 pipeline::network::tcp 中的客户端和服务端实现。

pub use crate::pipeline::network::tcp::client;
pub use crate::pipeline::network::tcp::server;
