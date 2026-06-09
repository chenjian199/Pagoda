// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 后端注册与选择。
//!
//! 通过编译期 `#[cfg(feature = "...")]` 在互斥的后端中选定唯一一个，并以
//! `ActiveBackend` 别名对外（crate 内）暴露。`timeline` 模块的 `push_impl` /
//! `pop_impl` / `name_current_thread_impl` 以及 `TimelineRangeGuard` 都统一
//! 委托给 `ActiveBackend`。
//!
//! 新增后端步骤：
//! 1. 在本目录新建 `xxx.rs` 并实现 [`crate::timeline::TimelineBackend`]；
//! 2. 在 `Cargo.toml` 增加 `timeline-xxx` feature（连带可选依赖）；
//! 3. 在此处增加一组 `#[cfg(feature = "timeline-xxx")]` 分支。

#[cfg(feature = "timeline-nvtx")]
mod nvtx;
#[cfg(feature = "timeline-nvtx")]
pub(crate) use nvtx::NvtxBackend as ActiveBackend;

#[cfg(feature = "timeline-ascend")]
mod ascend;
#[cfg(feature = "timeline-ascend")]
pub(crate) use ascend::AscendBackend as ActiveBackend;

// 默认：启用了 `timeline` 总开关但未选择任何具体后端时，使用空后端占位。
#[cfg(not(any(feature = "timeline-nvtx", feature = "timeline-ascend")))]
mod noop;
#[cfg(not(any(feature = "timeline-nvtx", feature = "timeline-ascend")))]
pub(crate) use noop::NoopBackend as ActiveBackend;
