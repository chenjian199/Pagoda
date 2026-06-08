// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-License-Identifier: Apache-2.0
//
// API-compatible implementation based on the public interfaces and behavioral
// contracts of NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
// Implementation rewritten by PAGODA.

//! # 离线重放模块根
//!
//! ## 设计意图
//! 组织离线（离散事件驱动）重放的聚合/分离运行时、调度核心、组件与入口函数。
//!
//! ## 外部契约
//! 重导出 `simulate_trace`/`simulate_concurrency`/`generate_trace_worker_artifacts` 等离线入口，签名与 Dynamo 保持一致。

pub(crate) use crate::replay::normalize_trace_requests;

pub(crate) mod agg;
pub(crate) mod components;
pub(crate) mod core;
pub(crate) mod disagg;
mod entrypoints;
pub(crate) mod events;
mod progress;
pub(crate) mod runtime_utils;
pub(crate) mod single;
pub(crate) mod state;

pub(crate) use entrypoints::{
    generate_trace_worker_artifacts, simulate_concurrency, simulate_concurrency_disagg,
    simulate_concurrency_workload, simulate_concurrency_workload_disagg, simulate_trace,
    simulate_trace_disagg, simulate_trace_workload, simulate_trace_workload_disagg,
};
