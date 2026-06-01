// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::network::egress` —— 出口侧（客户端 / 路由）实现集合
//!
//! ## 设计意图
//! 集中声明 “从本进程发起请求” 一侧的实现：地址路由、HTTP 路由、Push 路由、
//! NATS 客户端、统一请求平面（TCP + 抽象客户端）。本文件本身不持有逻辑，
//! 只承担命名空间聚合与统一重导出 `super::*` 的作用。
//!
//! ## 外部契约
//! - 子模块全部 `pub`，下游（如 `pipeline.rs`）按 `network::egress::xxx` 路径
//!   重导出 `AddressedPushRouter` / `PushRouter` / `RouterMode` /
//!   `WorkerLoadMonitor` 等关键符号。
//! - `use super::*;` 引入父 `network` 模块的所有 pub 名称，子模块以此为隐式 prelude。
//!
//! ## 实现要点
//! - 模块顺序遵循 “已稳定 → 实验性” 的人类阅读顺序；
//!   `tcp_client` / `unified_client` 与 `addressed_router` 等属同一 request plane 抽象。
//! - 不在此处放任何 helper / type alias，避免被多处误用。

// === SECTION: 子模块声明 ===

pub mod addressed_router;
pub mod http_router;
pub mod nats_client;
pub mod push_router;

// 统一请求平面接口与实现
pub mod tcp_client;
pub mod unified_client;

use super::*;
