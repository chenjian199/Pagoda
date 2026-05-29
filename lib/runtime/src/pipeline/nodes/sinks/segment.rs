// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::nodes::sinks::segment` —— SegmentSink 的构造、绑定与 trait 实现
//!
//! ## 设计意图
//! `SegmentSink<Req, Resp>` 是 `ServiceBackend` 的"延迟绑定"版本：在拓扑装配
//! 阶段先把节点连入图中、运行阶段再通过 [`SegmentSink::attach`] 把实际引擎
//! 注入进来。这一模式用于"图先于引擎可用"的场景（如跨段网络拓扑）。
//! 本文件提供构造（`new` / `Default`）、绑定（`attach`）以及 `Sink` /
//! `Source` 两条方向的 trait 实现。
//!
//! ## 外部契约
//! - `SegmentSink::new() -> Arc<Self>`：构造一个未绑定引擎的节点。
//! - `SegmentSink::attach(engine) -> Result<(), PipelineError>`：
//!   首次成功；二次调用返回 `PipelineError::EdgeAlreadySet`。
//! - `impl Default for SegmentSink`：等价于 `new` 的非 `Arc` 形式。
//! - `impl Sink<Req> for SegmentSink where Req: PipelineIO + Sync`：
//!   - 引擎未绑定 → `Err(PipelineError::NoNetworkEdge)`；
//!   - 已绑定 → `engine.generate(data).await?`，再 `self.on_next(stream, Token)`
//!     转发响应方向。
//! - `impl Source<Resp> for SegmentSink where Req: PipelineIO`：
//!   `on_next` / `set_edge` 透传到 `self.inner`。
//! - 泛型 bound 不对称（`Sink` 需 `Sync`，`Source` 不需要）与 `ServiceBackend`
//!   保持一致，属契约的一部分。
//!
//! ## 实现要点
//! - `use super::*;` 由 `sinks.rs` 注入所有需要的名称；本文件不重复 import。
//! - `attach` 内 `.set(engine).map_err(|_| EdgeAlreadySet)` 直接返回 `Result`，
//!   不画蛇添足地以 `?; Ok(())` 拆开，错误语义保持原子。
//! - `Sink::on_data` 中保留方法链 `.engine.get().ok_or(...)?.generate(...).await?`，
//!   让"未绑定/绑定后失败"两种错误路径在同一表达式内可读出。
//! - `Default` 实现独立列出（而非派生），因为字段类型 `OnceLock` /
//!   `SinkEdge` 都需要显式 `default()` 构造，且 `SinkEdge` 未派生 `Default`
//!   宏（手写的 impl），无法走 `#[derive(Default)]` 通道。

use super::*;
use anyhow::Error;

// === SECTION: 构造与绑定 ===

impl<Req: PipelineIO, Resp: PipelineIO> SegmentSink<Req, Resp> {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn attach(&self, engine: ServiceEngine<Req, Resp>) -> Result<(), PipelineError> {
        self.engine
            .set(engine)
            .map_err(|_| PipelineError::EdgeAlreadySet)
    }
}

impl<Req: PipelineIO, Resp: PipelineIO> Default for SegmentSink<Req, Resp> {
    fn default() -> Self {
        Self {
            engine: OnceLock::new(),
            inner: SinkEdge::default(),
        }
    }
}

// === SECTION: Sink<Req> ===

#[async_trait]
impl<Req: PipelineIO + Sync, Resp: PipelineIO> Sink<Req> for SegmentSink<Req, Resp> {
    async fn on_data(&self, data: Req, _: Token) -> Result<(), Error> {
        let stream = self
            .engine
            .get()
            .ok_or(PipelineError::NoNetworkEdge)?
            .generate(data)
            .await?;
        self.on_next(stream, Token).await
    }
}

// === SECTION: Source<Resp> ===

#[async_trait]
impl<Req: PipelineIO, Resp: PipelineIO> Source<Resp> for SegmentSink<Req, Resp> {
    async fn on_next(&self, data: Resp, _: Token) -> Result<(), Error> {
        self.inner.on_next(data, Token).await
    }

    fn set_edge(&self, edge: Edge<Resp>, _: Token) -> Result<(), PipelineError> {
        self.inner.set_edge(edge, Token)
    }
}
