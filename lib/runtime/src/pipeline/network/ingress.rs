// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::network::ingress` —— 入口侧（服务端 / 接收端点）实现集合
//!
//! ## 设计意图
//! 集中声明 “在本进程响应外部请求” 一侧的实现：HTTP 端点、NATS 服务端、
//! Push portname / handler、共享 TCP portname、统一服务端入口。
//! 本文件本身不持有业务逻辑，只作命名空间聚合与重导出。
//!
//! ## 外部契约
//! - 子模块全部 `pub`，供 `network` 与上层 `pipeline` 模块按需重导出。
//! - `use super::*;` 引入 `network` 父模块的全部 pub 名称作隐式 prelude，
//!   子模块依赖此 prelude；改动会触发子模块编译错误。
//!
//! ## 实现要点
//! - 子模块顺序遵循 “协议/角色” 自然分组（HTTP → NATS → Push → TCP → 统一）。
//! - 不在此处放任何 helper / type alias，避免散播实现细节。

// === SECTION: 子模块声明 ===

pub mod http_endpoint;
pub mod nats_server;
pub mod push_endpoint;
pub mod push_handler;
pub mod shared_tcp_endpoint;
pub mod unified_server;

use super::*;
