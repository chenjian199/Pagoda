// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::nodes::sources` —— 请求源端节点：Frontend / ServiceFrontend / SegmentSource
//!
//! ## 设计意图
//! "Source" 是图的请求入口：把外部 `generate(request)` 调用注入下游 `Sink<In>`,
//! 同时把"响应流"从下游 `Sink<Out>` 回填到调用方持有的 `oneshot` 接收端。
//! - 三类源共享同一个内部状态结构 [`Frontend`]：[`ServiceFrontend`] 是对外暴露的
//!   `AsyncEngine` 包装；[`SegmentSource`] 是图内段间的源端。
//!
//! ## 外部契约
//! - `Frontend<In, Out>` / `ServiceFrontend<In, Out>` / `SegmentSource<In, Out>` 均为
//!   `pub struct`；内部字段 `edge: OnceLock<Edge<In>>` 与
//!   `sinks: Arc<Mutex<HashMap<String, oneshot::Sender<Out>>>>` 私有。
//! - 子模块 [`base`]、[`common`] 私有，仅用于内部 trait 实现与构造器。
//! - `Mutex` 必须来自 `std::sync::Mutex`（经 `super::*` 引入），不可替换为
//!   `tokio::sync::Mutex`：请求注册与响应回填都是同步短临界区。
//!
//! ## 实现要点
//! - `use super::*` 将父模块 `nodes.rs` 顶部的 `Arc / Mutex / OnceLock / HashMap /
//!   oneshot / Edge / PipelineError / private / ...` 一并引入，让本文件保持极简。
//! - 仍 `use crate::pipeline::{AsyncEngine, PipelineIO}` 显式拉入两个常用名。

// === SECTION: 引入与子模块声明 ===

use super::*;
use crate::pipeline::{AsyncEngine, PipelineIO};

mod base;
mod common;

// === SECTION: 三类 Source 公开类型定义 ===

pub struct Frontend<In: PipelineIO, Out: PipelineIO> {
    edge: OnceLock<Edge<In>>,
    sinks: Arc<Mutex<HashMap<String, oneshot::Sender<Out>>>>,
}

/// A [`ServiceFrontend`] is the interface for an [`AsyncEngine<SingleIn<Context<In>>, ManyOut<Annotated<Out>>, Error>`]
pub struct ServiceFrontend<In: PipelineIO, Out: PipelineIO> {
    inner: Frontend<In, Out>,
}

pub struct SegmentSource<In: PipelineIO, Out: PipelineIO> {
    inner: Frontend<In, Out>,
}