// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-License-Identifier: Apache-2.0

//! # 负载生成（loadgen）
//!
//! ## 外部契约
//! 导出 `WorkloadDriver` 及一组 trace/spec 类型，供构造与回放合成或文件负载使用。

mod driver;
mod trace;
mod types;

pub use driver::WorkloadDriver;
pub use types::{
    ArrivalSpec, DelaySpec, LengthSpec, ReadyTurn, ReplayRequestHashes, RouterSequence,
    SequenceHashMode, SessionPartitionSpec, SessionTrace, SyntheticTraceSpec, Trace,
    TraceFileFormat, TurnTrace,
};

#[cfg(test)]
mod tests;
