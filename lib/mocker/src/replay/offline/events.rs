// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-License-Identifier: Apache-2.0
//
// API-compatible implementation based on the public interfaces and behavioral
// contracts of NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
// Implementation rewritten by PAGODA.

//! # 离线仿真事件
//!
//! ## 设计意图
//! 定义离线重放离散事件堆中的事件类型（worker 完成、decode 交接、worker 就绪）及其排序规则。
//!
//! ## 外部契约
//! 提供 `SimulationEvent`/`SimulationEventKind`/`SimulationWorkerStage`，其 `Ord` 实现保证最小堆按时间戳与序号出队，行为与 Dynamo 一致。

use std::cmp::Ordering;

use crate::common::protocols::OutputSignal;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SimulationWorkerStage {
    Aggregated,
    Prefill,
    Decode,
}

#[derive(Debug)]
pub(crate) enum SimulationEventKind {
    WorkerCompletion {
        stage: SimulationWorkerStage,
        worker_idx: usize,
        completed_requests: usize,
        output_signals: Vec<OutputSignal>,
        kv_events: Vec<pagoda_kv_router::protocols::RouterEvent>,
    },
    DecodeHandoff {
        uuid: Uuid,
    },
    WorkerReady {
        stage: SimulationWorkerStage,
        worker_id: usize,
    },
}

#[derive(Debug)]
pub(crate) struct SimulationEvent {
    pub(crate) at_ms: f64,
    pub(crate) seq_no: u64,
    pub(crate) kind: SimulationEventKind,
}

impl PartialEq for SimulationEvent {
    fn eq(&self, other: &Self) -> bool {
        self.at_ms.to_bits() == other.at_ms.to_bits() && self.seq_no == other.seq_no
    }
}

impl Eq for SimulationEvent {}

impl PartialOrd for SimulationEvent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SimulationEvent {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .at_ms
            .partial_cmp(&self.at_ms)
            .unwrap_or(Ordering::Equal)
            .then_with(|| other.seq_no.cmp(&self.seq_no))
    }
}
