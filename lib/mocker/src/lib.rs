// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-License-Identifier: Apache-2.0
//
// API-compatible implementation based on the public interfaces and behavioral
// contracts of NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
// Implementation rewritten by PAGODA.

//! # pagoda-mocker：无 GPU 的 LLM 调度器与 KV cache 模拟
//!
//! ## 设计意图
//! 在不依赖真实 GPU 资源或完整分布式运行时的前提下，模拟 LLM 调度器的核心行为：
//! KV cache 管理、请求调度与 token 生成时序，用于测试与基准。
//!
//! ## 外部契约
//! 对外导出以下子模块；其公开类型与函数构成下游可依赖的稳定接口。

pub mod common;
pub mod engine;
pub mod kv_cache;
pub mod loadgen;
pub mod replay;
pub mod scheduler;
