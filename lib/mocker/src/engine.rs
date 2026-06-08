// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-License-Identifier: Apache-2.0

//! # 引擎工厂
//!
//! ## 设计意图
//! 根据 [`EngineType`] 创建相应的调度器，并以 trait object 形式返回，使引擎包装层
//! 无需感知底层具体后端。
//!
//! ## 外部契约
//! [`create_engine`] 的参数面、返回类型 `Box<dyn SchedulerHandle>` 与调度行为保持稳定。
//! 当前 [`EngineType`] 仅含 `Vllm` 一个变体（保留扩展位）。

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::common::protocols::{
    EngineType, FpmPublisher, KvEventPublishers, MockEngineArgs, OutputSignal,
};
use crate::scheduler::{Scheduler, SchedulerHandle};

// === SECTION: 工厂入口 ===

/// 为配置的引擎类型创建调度器，返回装箱的 [`SchedulerHandle`]。
pub fn create_engine(
    args: MockEngineArgs,
    dp_rank: u32,
    output_tx: Option<mpsc::UnboundedSender<Vec<OutputSignal>>>,
    kv_event_publishers: KvEventPublishers,
    cancellation_token: Option<CancellationToken>,
    fpm_publisher: FpmPublisher,
) -> Box<dyn SchedulerHandle> {
    // 当前只有 vLLM 一种后端；保留 match 以便后续扩展。
    match args.engine_type {
        EngineType::Vllm => Box::new(Scheduler::new(
            args,
            dp_rank,
            output_tx,
            kv_event_publishers,
            cancellation_token,
            fpm_publisher,
        )),
    }
}
