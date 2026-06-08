// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-License-Identifier: Apache-2.0
//
// API-compatible implementation based on the public interfaces and behavioral
// contracts of NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
// Implementation rewritten by PAGODA.

//! # 重放路由共享组件
//!
//! ## 设计意图
//! 为离线/在线重放构造与 KV Router 对齐的调度器、worker 配置、活动序列槽与选择器等共享基础设施。
//!
//! ## 外部契约
//! 提供 `ReplayScheduler` 类型别名与 `replay_worker_config`/`replay_slots`/`replay_router_config` 等构造函数，
//! 字段语义与 Dynamo KV Router 保持一致。

use std::collections::HashMap;
use std::future;
use std::sync::Arc;

use crate::common::protocols::MockEngineArgs;
use pagoda_kv_router::config::KvRouterConfig;
use pagoda_kv_router::protocols::{
    ActiveLoad, ActiveSequenceEvent, WorkerConfigLike, WorkerId, WorkerWithDpRank,
};
use pagoda_kv_router::scheduling::queue::DEFAULT_MAX_BATCHED_TOKENS;
use pagoda_kv_router::{
    ActiveSequencesMultiWorker, DefaultWorkerSelector, LocalScheduler, RouterSchedulingPolicy,
    SequencePublisher,
};

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct ReplayNoopPublisher;

impl SequencePublisher for ReplayNoopPublisher {
    fn publish_event(
        &self,
        _event: &ActiveSequenceEvent,
    ) -> impl future::Future<Output = anyhow::Result<()>> + Send {
        future::ready(Ok(()))
    }

    fn publish_load(&self, _load: ActiveLoad) {}

    fn observe_load(&self, _: &WorkerWithDpRank, _: &str, _: usize, _: usize) {}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ReplayWorkerConfig {
    pub(super) max_num_batched_tokens: u64,
    pub(super) total_kv_blocks: u64,
}

impl WorkerConfigLike for ReplayWorkerConfig {
    fn data_parallel_start_rank(&self) -> u32 {
        0
    }

    fn data_parallel_size(&self) -> u32 {
        1
    }

    fn max_num_batched_tokens(&self) -> Option<u64> {
        Some(self.max_num_batched_tokens)
    }

    fn total_kv_blocks(&self) -> Option<u64> {
        Some(self.total_kv_blocks)
    }
}

pub(super) type ReplayScheduler = LocalScheduler<
    ReplayNoopPublisher,
    ReplayWorkerConfig,
    RouterSchedulingPolicy,
    DefaultWorkerSelector,
>;

pub(in crate::replay) fn replay_worker_config(args: &MockEngineArgs) -> ReplayWorkerConfig {
    ReplayWorkerConfig {
        max_num_batched_tokens: args
            .max_num_batched_tokens
            .map(|tokens| tokens as u64)
            .unwrap_or(DEFAULT_MAX_BATCHED_TOKENS),
        total_kv_blocks: args.num_gpu_blocks as u64,
    }
}

pub(super) fn replay_workers_with_configs(
    args: &MockEngineArgs,
    num_workers: usize,
) -> HashMap<WorkerId, ReplayWorkerConfig> {
    let worker_config = replay_worker_config(args);
    (0..num_workers)
        .map(|worker_idx| (worker_idx as WorkerId, worker_config.clone()))
        .collect()
}

pub(super) fn replay_slots(
    args: &MockEngineArgs,
    workers_with_configs: &HashMap<WorkerId, ReplayWorkerConfig>,
) -> Arc<ActiveSequencesMultiWorker<ReplayNoopPublisher>> {
    let dp_range = workers_with_configs
        .keys()
        .copied()
        .map(|worker_id| (worker_id, (0, 1)))
        .collect();
    Arc::new(ActiveSequencesMultiWorker::new(
        ReplayNoopPublisher,
        args.block_size,
        dp_range,
        false,
        0,
        "replay",
    ))
}

pub(super) fn replay_selector(config: &KvRouterConfig) -> DefaultWorkerSelector {
    DefaultWorkerSelector::new(Some(config.clone()), "replay")
}

pub(crate) fn replay_router_config(
    args: &MockEngineArgs,
    router_config: Option<KvRouterConfig>,
) -> KvRouterConfig {
    let mut config = router_config.unwrap_or_default();
    if let Some(policy) = args.router_queue_policy {
        config.router_queue_policy = policy;
    }
    config
}

pub(super) fn replay_policy(config: &KvRouterConfig) -> RouterSchedulingPolicy {
    RouterSchedulingPolicy::new(config.router_queue_policy)
}
