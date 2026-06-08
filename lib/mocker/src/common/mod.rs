// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-License-Identifier: Apache-2.0
//
// API-compatible implementation based on the public interfaces and behavioral
// contracts of NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
// Implementation rewritten by PAGODA.

//! # common：跨引擎共享构件
//!
//! ## 设计意图
//! 汇集被各引擎实现复用的基础构件：配置/协议、token 序列、性能模型、滑动均值、
//! KV trace 日志、bootstrap 握手与若干工具函数。
//!
//! ## 外部契约
//! 这些子模块的公开类型与函数被 scheduler / loadgen / replay 等上层直接依赖，
//! 其名称、签名与可观察行为必须保持稳定。

pub mod bootstrap;
pub mod kv_cache_trace;
pub mod perf_model;
pub mod protocols;
pub mod running_mean;
pub mod sequence;
pub mod utils;
