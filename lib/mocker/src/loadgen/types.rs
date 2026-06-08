// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-License-Identifier: Apache-2.0
//
// API-compatible implementation based on the public interfaces and behavioral
// contracts of NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
// Implementation rewritten by PAGODA.

//! # 负载生成数据类型
//!
//! ## 设计意图
//! 集中定义负载驱动所需的轨迹、合成规格、到达/延迟分布以及就绪轮次等数据载体。
//!
//! ## 外部契约
//! 全部 `pub` 类型的名称、字段名与类型、enum 变体保持与 Dynamo 一致，供 driver/trace 复用。

use pagoda_kv_router::LocalBlockHash;
use pagoda_kv_router::protocols::{ExternalSequenceBlockHash, WorkerId};
use pagoda_tokens::SequenceHash;
use uuid::Uuid;

use crate::common::protocols::DirectRequest;

#[derive(Debug, Clone)]
pub struct Trace {
    pub block_size: usize,
    pub sessions: Vec<SessionTrace>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceFileFormat {
    Mooncake,
    AppliedComputeAgentic,
}

#[derive(Debug, Clone)]
pub struct SessionTrace {
    pub session_id: String,
    pub first_arrival_timestamp_ms: Option<f64>,
    pub turns: Vec<TurnTrace>,
}

#[derive(Debug, Clone)]
pub struct TurnTrace {
    pub input_length: usize,
    pub max_output_tokens: usize,
    pub hash_ids: Vec<u64>,
    pub delay_after_previous_ms: f64,
}

#[derive(Debug, Clone)]
pub struct LengthSpec {
    pub mean: usize,
    pub stddev: f64,
}

#[derive(Debug, Clone)]
pub enum ArrivalSpec {
    Burst,
    ConstantQps { qps: f64 },
    PoissonQps { qps: f64 },
    GammaQps { qps: f64, smoothness: f64 },
}

#[derive(Debug, Clone)]
pub enum DelaySpec {
    None,
    ConstantMs(f64),
    ExponentialMs { mean_ms: f64 },
}

#[derive(Debug, Clone)]
pub struct SyntheticTraceSpec {
    pub block_size: usize,
    pub num_sessions: usize,
    pub turns_per_session: usize,
    pub input_tokens: LengthSpec,
    pub output_tokens: LengthSpec,
    pub shared_prefix_ratio: f64,
    pub num_prefix_groups: usize,
    pub first_turn_arrivals: ArrivalSpec,
    pub inter_turn_delays: DelaySpec,
    pub seed: u64,
}

#[derive(Debug, Clone, Copy)]
pub enum SequenceHashMode {
    Raw,
    Cumulative,
}

#[derive(Debug, Clone, Copy)]
pub enum SessionPartitionSpec {
    Random { num_partitions: usize, seed: u64 },
    RoundRobin { num_partitions: usize },
}

#[derive(Debug, Clone)]
pub struct RouterSequence {
    pub worker_id: WorkerId,
    pub local_hashes: Vec<LocalBlockHash>,
    pub external_hashes: Vec<ExternalSequenceBlockHash>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayRequestHashes {
    pub local_block_hashes: Vec<LocalBlockHash>,
    pub sequence_hashes: Vec<SequenceHash>,
}

#[derive(Debug, Clone)]
pub struct ReadyTurn {
    pub request_uuid: Uuid,
    pub session_id: String,
    pub turn_index: usize,
    pub scheduled_ready_at_ms: f64,
    pub replay_hashes: Option<ReplayRequestHashes>,
    pub request: DirectRequest,
}
