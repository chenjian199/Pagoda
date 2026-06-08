// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-License-Identifier: Apache-2.0

//! # 离线重放组件模块根
//!
//! ## 设计意图
//! 汇聚离线重放的可复用组件：准入队列、引擎组件、离线路由与共享类型。
//!
//! ## 外部契约
//! 重导出 `AdmissionQueue`/`EngineComponent`/`OfflineReplayRouter`/`ReplayMode`/`TrafficStats` 等类型，可见性与字段语义与 Dynamo 保持一致。

mod admission;
mod engine;
mod router;
mod types;

pub(in crate::replay::offline) use admission::AdmissionQueue;
pub(in crate::replay::offline) use engine::EngineComponent;
pub(crate) use router::OfflineReplayRouter;
#[cfg(test)]
pub(crate) use router::OfflineRouterSnapshot;
pub(in crate::replay) use types::ReplayMode;
pub use types::TrafficStats;
pub(in crate::replay::offline) use types::{
    EngineEffects, EnginePassMode, ReadyArrival, ScheduledWorkerCompletion, TrafficAccumulator,
};
pub(crate) use types::{RouterEffects, WorkerAdmission};
