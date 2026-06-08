// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-License-Identifier: Apache-2.0
//
// API-compatible implementation based on the public interfaces and behavioral
// contracts of NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
// Implementation rewritten by PAGODA.

//! # vLLM 调度模拟
//!
//! ## 设计意图
//! 围绕统一的「等待/运行」请求模型模拟 vLLM 调度。
//!
//! ## 外部契约
//! 对外导出 `Scheduler`、`MockerMetrics`，对 crate 内导出 `VllmCore`。
//!
//! 参考：vllm/vllm/v1/core/sched/scheduler.py

mod core;
mod live;

pub(crate) use core::VllmCore;
pub use live::{MockerMetrics, Scheduler};

#[cfg(test)]
mod tests;
