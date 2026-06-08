// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-License-Identifier: Apache-2.0
//
// API-compatible implementation based on the public interfaces and behavioral
// contracts of NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
// Implementation rewritten by PAGODA.

//! # 在线重放模块根
//!
//! ## 设计意图
//! 组织在线（实时）重放的子模块，提供基于真实异步运行时的 trace 与并发重放能力。
//!
//! ## 外部契约
//! 导出 `simulate_trace_*`/`simulate_concurrency_*` 入口与 `ReplayRouter`，行为与 Dynamo 一致。

mod demux;
mod entrypoints;
mod live_runtime;
mod router;
mod state;
mod task;

#[cfg(test)]
mod tests;

pub(crate) use entrypoints::{
    simulate_concurrency_requests, simulate_concurrency_workload, simulate_trace_requests,
    simulate_trace_workload,
};
pub(crate) use router::ReplayRouter;
