// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `dynamo_runtime` —— Crate 根
//!
//! ## 设计意图
//! 仅作为 crate 的"目录索引"：把分布式运行时所需的全部子系统（组件 / 计算 /
//! 发现 / 引擎 / 健康 / 日志 / 指标 / 管线 / 协议 / 传输 / 工作器等）以扁平
//! `pub mod` 方式暴露给外部 crate，并在顶层做少量"高频符号"的 re-export，
//! 让下游 `use dynamo_runtime::{Runtime, Worker, Result, Error}` 即可。
//!
//! ## 外部契约
//! - 顶层 `pub use` 集合（外部 API 表面，与 lib-copy 严格一致）：
//!   - `anyhow` 别名：`Context as ErrorContext` / `Error` / `Ok as OK` /
//!     `Result` / `anyhow as error` / `bail as raise`。
//!   - `config::RuntimeConfig`、`system_status_server::SystemStatusServerInfo`。
//!   - `distributed::{DistributedRuntime, distributed_test_utils}`、
//!     `futures::stream`、`metrics::MetricsRegistry`、`runtime::Runtime`、
//!     `system_health::{HealthCheckTarget, SystemHealth}`、
//!     `tokio_util::sync::CancellationToken`、`worker::Worker`。
//! - 顶层 `pub mod` 列表（不可增减、不可改名、不可改顺序语义）：
//!   `config / component / compute / discovery / engine / engine_routes /
//!    error / health_check / local_endpoint_registry / metadata_registry /
//!    system_status_server / distributed / instances / logging / metrics /
//!    nvtx / pipeline / prelude / protocols / runnable / runtime / service /
//!    slug / storage / system_health / traits / transports / utils / worker`。
//! - `#![allow(dead_code)]` 与 `#![allow(unused_imports)]` 两个 crate 级 lint
//!   抑制是契约的一部分（部分子模块在不同 feature 组合下会出现暂未使用的导入）。
//!
//! ## 实现要点
//! - 文件本身无业务逻辑，因此没有 `#[cfg(test)] mod tests`；测试矩阵规则
//!   对其**不适用**（vacuous）。
//! - crate 级 `use std::{...}` / `OnceCell` / `Endpoint` / `GracefulShutdownTracker`
//!   / `HealthStatus` 等 internal-only 导入保留原貌：它们供子模块通过
//!   `crate::...` 路径间接引用，并被 `#![allow(unused_imports)]` 兜底。
//! - `pub use` 中保留 `Ok as OK` 这种大写别名是与 lib-copy 一致的历史约定，
//!   不可换成 `Ok`（会和 prelude 冲突）。

#![allow(dead_code)]
#![allow(unused_imports)]

// === SECTION: crate 内部 use（仅供子模块通过 crate::... 间接引用）===

use std::{
    collections::HashMap,
    sync::{Arc, OnceLock, Weak},
};

// === SECTION: 顶层 pub use —— anyhow 错误处理别名 ===

pub use anyhow::{
    Context as ErrorContext, Error, Ok as OK, Result, anyhow as error, bail as raise,
};

use async_once_cell::OnceCell;

// === SECTION: 顶层 pub mod 与就近 re-export ===

pub mod config;
pub use config::RuntimeConfig;

pub mod component;
pub mod compute;
pub mod discovery;
pub mod engine;
pub mod engine_routes;
pub mod error;
pub mod health_check;
pub mod local_endpoint_registry;
pub mod metadata_registry;
pub mod system_status_server;
pub use system_status_server::SystemStatusServerInfo;
pub mod distributed;
pub mod instances;
pub mod logging;
pub mod metrics;
pub mod nvtx;
pub mod pipeline;
pub mod prelude;
pub mod protocols;
pub mod runnable;
pub mod runtime;
pub mod service;
pub mod slug;
pub mod storage;
pub mod system_health;
pub mod traits;
pub mod transports;
pub mod utils;
pub mod worker;

// === SECTION: 顶层 pub use —— 高频符号集中 re-export ===

pub use distributed::{DistributedRuntime, distributed_test_utils};
pub use futures::stream;
pub use metrics::MetricsRegistry;
pub use runtime::Runtime;
pub use system_health::{HealthCheckTarget, SystemHealth};
pub use tokio_util::sync::CancellationToken;
pub use worker::Worker;

// === SECTION: crate 内部别名导入（不对外暴露）===

use component::Endpoint;
use utils::GracefulShutdownTracker;

use config::HealthStatus;
