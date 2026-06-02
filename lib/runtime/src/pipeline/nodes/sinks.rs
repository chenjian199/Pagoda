// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::nodes::sinks` —— sink 侧图节点（聚合层）
//!
//! ## 设计意图
//! 本文件作为 sink 子模块树的"门面"：① 集中声明 `SinkEdge` / `ServiceBackend` /
//! `SegmentSink` / `EgressPort` 四个数据结构本身；② 把 `base` / `pipeline` /
//! `segment` 三个子模块的具体 trait 实现拼接进来。把"结构定义"与"trait 实现"
//! 拆开是为了让一个结构体的不同 trait 实现可以分文件维护，避免单文件膨胀。
//!
//! ## 外部契约
//! - `SinkEdge<Resp>` 为 `pub(crate)`：仅 `pipeline::nodes` 模块树内可见
//!   （`PipelineOperator.upstream` 需要它），外部禁止依赖。
//! - `ServiceBackend<Req, Resp>` / `SegmentSink<Req, Resp>` 为 `pub`，并在
//!   父模块 `pipeline::nodes` 重导出为 `pipeline::nodes::{ServiceBackend, SegmentSink}`。
//! - `EgressPort<Req, Resp>` 为 `pub`（带 `#[allow(dead_code)]`），路径
//!   `pipeline::nodes::sinks::EgressPort` 对外可见，作为未来"基于 NATS / TCP 的
//!   出口端点"预留的形参占位；现阶段尚未连接到 trait 体系，但其类型存在性属于契约。
//! - 三个结构体字段均为模块内私有（无 `pub` / `pub(super)`），仅由本目录下的
//!   `base.rs` / `pipeline.rs` / `segment.rs` 通过 `use super::*;` 直接访问。
//! - 模块入口的 use 列表（`super::{Arc, Edge, OnceLock, PipelineError, Service,
//!   Sink, Source, async_trait, private::Token}` + `crate::pipeline::{PipelineIO,
//!   ServiceEngine}`）属于事实契约：三个子模块全部依赖 `use super::*;` 由本文件
//!   注入这些名称。
//!
//! ## 实现要点
//! - 不在此处放置任何 impl 块；所有 trait 实现下放到 `base` / `pipeline` /
//!   `segment` 三个子模块，便于按结构体职责定位。
//! - `// todo - use a once lock of a TransportEngine` 与 `EgressPort` 周围
//!   的大段注释代码块为历史设计草案，保留以传达"网络出口端点"的演进意图。
//! - 三个子模块均为 `mod`（非 `pub mod`）：它们只为本文件的结构体补 impl，
//!   不引入新的命名空间。

use super::{
    Arc, Edge, OnceLock, PipelineError, Service, Sink, Source, async_trait, private::Token,
};
use crate::pipeline::{PipelineIO, ServiceEngine};

// === SECTION: 子模块声明 ===

mod base;
mod pipeline;
mod segment;

// === SECTION: SinkEdge 响应回流的最小 Source ===

pub(crate) struct SinkEdge<Resp: PipelineIO> {
    edge: OnceLock<Edge<Resp>>,
}

// === SECTION: ServiceBackend 本地图尾（绑定引擎）===

pub struct ServiceBackend<Req: PipelineIO, Resp: PipelineIO> {
    engine: ServiceEngine<Req, Resp>,
    inner: SinkEdge<Resp>,
}

// === SECTION: SegmentSink 段图尾（延迟绑定引擎）===

// todo - use a once lock of a TransportEngine
pub struct SegmentSink<Req: PipelineIO, Resp: PipelineIO> {
    engine: OnceLock<ServiceEngine<Req, Resp>>,
    inner: SinkEdge<Resp>,
}

// === SECTION: EgressPort 网络出口端点（占位）===

#[allow(dead_code)]
pub struct EgressPort<Req: PipelineIO, Resp: PipelineIO> {
    engine: Service<Req, Resp>,
}

