// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-License-Identifier: Apache-2.0
//
// API-compatible implementation based on the public interfaces and behavioral
// contracts of NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
// Implementation rewritten by PAGODA.

//! # 重放产物（worker artifacts）
//!
//! ## 设计意图
//! 定义离线生成阶段产出的带时间戳请求、输出信号与 KV 事件结构，供后续回放消费。
//!
//! ## 外部契约
//! 公开 `ReplayTimedRequest`/`ReplayTimedOutputSignal`/`ReplayTimedKvEvent`/`ReplayWorkerArtifacts`，字段名与 Dynamo 保持一致。

use pagoda_kv_router::protocols::{KvCacheEvent, StorageTier};
use uuid::Uuid;

use crate::common::protocols::OutputSignal;
use crate::loadgen::ReplayRequestHashes;

#[derive(Debug, Clone)]
pub struct ReplayTimedRequest {
    pub uuid: Uuid,
    pub timestamp_us: u64,
    pub scheduled_ready_at_ms: f64,
    pub input_length: usize,
    pub output_length: usize,
    pub replay_hashes: ReplayRequestHashes,
}

#[derive(Debug, Clone)]
pub struct ReplayTimedOutputSignal {
    pub signal: OutputSignal,
    pub timestamp_us: u64,
}

#[derive(Debug, Clone)]
pub struct ReplayTimedKvEvent {
    pub event: KvCacheEvent,
    pub storage_tier: StorageTier,
    pub timestamp_us: u64,
}

#[derive(Debug, Clone, Default)]
pub struct ReplayWorkerArtifacts {
    pub requests: Vec<ReplayTimedRequest>,
    pub output_signals: Vec<ReplayTimedOutputSignal>,
    pub kv_events: Vec<ReplayTimedKvEvent>,
}
