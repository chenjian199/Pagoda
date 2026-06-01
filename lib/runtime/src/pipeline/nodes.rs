// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::nodes` —— 图节点核心原语：Source / Sink / Edge / Operator / PipelineNode
//!
//! ## 设计意图
//! `ServicePipeline` 是有向图：每个节点同时定义"请求方向"与"响应方向"的行为。
//! 节点要么是 [`Source`]（输出方向），要么是 [`Sink`]（输入方向），或两者兼具。
//! - `Frontend` 是图的起点：请求方向是 [`Source`]、响应方向是 [`Sink`]。
//! - `Backend` 是图的终点：请求方向是 [`Sink`]、响应方向是 [`Source`]。
//! - [`PipelineOperator`] 同时变换两条方向，由 [`Operator`] trait 提供变换逻辑；
//!   通过 [`PipelineOperator::forward_edge`] / [`PipelineOperator::backward_edge`]
//!   分别拿到请求路径与响应路径的"双面"句柄。
//! - [`PipelineNode`] 只变换一条方向（请求 *或* 响应），算是 [`Operator`] 的退化版。
//!
//! ## 外部契约
//! - 公开 trait 与结构：`Source<T>`、`Sink<T>`、`Edge<T>`、`Operator<UpIn, UpOut,
//!   DownIn, DownOut>`、`PipelineOperator`、`PipelineOperatorForwardEdge`、
//!   `PipelineOperatorBackwardEdge`、`PipelineNode<In, Out>`。
//! - 公开 trait bound 一律使用 `Data`（`Source<T>: Data` / `Sink<T>: Data` /
//!   `Operator<...>: Data`），**不可**替换为 `Send + Sync + 'static`，这是契约。
//! - `Source<T>::on_next` / `set_edge`、`Sink<T>::on_data` 形参中的 `private::Token`
//!   为模块私有类型：外部无法构造，因而这些方法事实上 sealed；
//!   外部唯一合法连接图的入口是 `Source::link`。
//! - `Edge::new` 与 `Edge::write` 为模块私有 fn（**不可** `pub`），仅供 `Source::link`
//!   与 `Edge` 自身的私有路径使用。
//! - `type NodeFn<In, Out> = Box<dyn Fn(In) -> Result<Out, Error> + Send + Sync>`：
//!   私有别名，仅供 `PipelineNode::new` 形参类型。
//! - 重导出：`pub use sinks::{SegmentSink, ServiceBackend}` /
//!   `pub use sources::{SegmentSource, ServiceFrontend}`。
//! - `type Service<In, Out> = Arc<ServiceFrontend<In, Out>>` 公开类型别名。
//!
//! ## 实现要点
//! - 顶部以单个嵌套 `use std::{collections::HashMap, sync::{Arc, Mutex, OnceLock}}`
//!   引入子模块通过 `use super::*;` 所需的全部 std 名称；这一组合是契约：
//!   sinks/base.rs、sources/base.rs 等子模块均假定 `super::*` 能命名出
//!   `Arc / Mutex / OnceLock / HashMap`。
//! - `tokio::sync::oneshot` 与 `Mutex`（std）同时纳入 `super::*`，让响应路径的
//!   one-shot channel 与同步互斥锁两者都可在 sources/base.rs 内直接使用。
//! - `private::Token` 单独 `mod private`，与 `pub trait Source / Sink` 同文件
//!   定义，便于 `link` 自构 Token、外部无法构造。
//! - `PipelineOperator` 的三个泛型字段拆出独立结构 `Forward/BackwardEdge`，
//!   让请求/响应两条路径可分别 Arc 引用，避免一条路径阻塞另一条。

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
};

use super::AsyncEngine;
use async_trait::async_trait;
use tokio::sync::oneshot;

use super::{Data, Error, PipelineError, PipelineIO};

// === SECTION: 子模块与重导出 ===

mod sinks;
mod sources;

pub use sinks::{SegmentSink, ServiceBackend};
pub use sources::{SegmentSource, ServiceFrontend};

pub type Service<In, Out> = Arc<ServiceFrontend<In, Out>>;

mod private {
    pub struct Token;
}

// === SECTION: Source<T> ===

// todo rename `ServicePipelineExt`
/// A [`Source`] trait defines how data is emitted from a source to a downstream sink.
#[async_trait]
pub trait Source<T: PipelineIO>: Data {
    async fn on_next(&self, data: T, _: private::Token) -> Result<(), Error>;

    fn set_edge(&self, edge: Edge<T>, _: private::Token) -> Result<(), PipelineError>;

    fn link<S: Sink<T> + 'static>(&self, sink: Arc<S>) -> Result<Arc<S>, PipelineError> {
        let edge = Edge::new(sink.clone());
        self.set_edge(edge, private::Token)?;
        Ok(sink)
    }
}

// === SECTION: Sink<T> ===

/// A [`Sink`] trait defines how data is received from a source and processed.
#[async_trait]
pub trait Sink<T: PipelineIO>: Data {
    async fn on_data(&self, data: T, _: private::Token) -> Result<(), Error>;
}

// === SECTION: Edge<T> ===

/// An [`Edge`] is a connection between a [`Source`] and a [`Sink`].
pub struct Edge<T: PipelineIO> {
    downstream: Arc<dyn Sink<T>>,
}

impl<T: PipelineIO> Edge<T> {
    fn new(downstream: Arc<dyn Sink<T>>) -> Self {
        Edge { downstream }
    }

    async fn write(&self, data: T) -> Result<(), Error> {
        self.downstream.on_data(data, private::Token).await
    }
}

type NodeFn<In, Out> = Box<dyn Fn(In) -> Result<Out, Error> + Send + Sync>;

// === SECTION: Operator trait ===

/// An [`Operator`] is a trait that defines the behavior of how two [`AsyncEngine`] can be chained together.
/// An [`Operator`] is not quite an [`AsyncEngine`] because its generate method requires both the upstream
/// request, but also the downstream [`AsyncEngine`] to which it will pass the transformed request.
/// The [`Operator`] logic must transform the upstream request `UpIn` to the downstream request `DownIn`,
/// then transform the downstream response `DownOut` to the upstream response `UpOut`.
///
/// A [`PipelineOperator`] accepts an [`Operator`] and presents itself as an [`AsyncEngine`] for the upstream
/// [`AsyncEngine<UpIn, UpOut, Error>`].
///
/// ### Example of type transformation and data flow
/// ```text
/// ... --> <UpIn> ---> [Operator] --> <DownIn> ---> ...
/// ... <-- <UpOut> --> [Operator] <-- <DownOut> <-- ...
/// ```
#[async_trait]
pub trait Operator<UpIn: PipelineIO, UpOut: PipelineIO, DownIn: PipelineIO, DownOut: PipelineIO>:
    Data
{
    /// This method is expected to transform the upstream request `UpIn` to the downstream request `DownIn`,
    /// call the next [`AsyncEngine`] with the transformed request, then transform the downstream response
    /// `DownOut` to the upstream response `UpOut`.
    async fn generate(
        &self,
        req: UpIn,
        next: Arc<dyn AsyncEngine<DownIn, DownOut, Error>>,
    ) -> Result<UpOut, Error>;

    fn into_operator(self: &Arc<Self>) -> Arc<PipelineOperator<UpIn, UpOut, DownIn, DownOut>>
    where
        Self: Sized,
    {
        PipelineOperator::new(self.clone())
    }
}

// === SECTION: PipelineOperator 与双向边句柄 ===

/// A [`PipelineOperatorForwardEdge`] is [`Sink`] for the upstream request type `UpIn` and a [`Source`] for the
/// downstream request type `DownIn`.
pub struct PipelineOperatorForwardEdge<
    UpIn: PipelineIO,
    UpOut: PipelineIO,
    DownIn: PipelineIO,
    DownOut: PipelineIO,
> {
    parent: Arc<PipelineOperator<UpIn, UpOut, DownIn, DownOut>>,
}

/// A [`PipelineOperatorBackwardEdge`] is [`Sink`] for the downstream response type `DownOut` and a [`Source`] for the
/// upstream response type `UpOut`.
pub struct PipelineOperatorBackwardEdge<
    UpIn: PipelineIO,
    UpOut: PipelineIO,
    DownIn: PipelineIO,
    DownOut: PipelineIO,
> {
    parent: Arc<PipelineOperator<UpIn, UpOut, DownIn, DownOut>>,
}

/// A [`PipelineOperator`] is a node that can transform both the forward and backward paths using the logic defined
/// by the implementation of an [`Operator`] trait.
pub struct PipelineOperator<
    UpIn: PipelineIO,
    UpOut: PipelineIO,
    DownIn: PipelineIO,
    DownOut: PipelineIO,
> {
    // core business logic of this object
    operator: Arc<dyn Operator<UpIn, UpOut, DownIn, DownOut>>,

    // this hold the downstream connections via the generic frontend
    // frontends provide both a source and a sink interfaces
    downstream: Arc<sources::Frontend<DownIn, DownOut>>,

    // this hold the connection to the previous/upstream response sink
    // we are a source to that upstream's response sink
    upstream: sinks::SinkEdge<UpOut>,
}

impl<UpIn, UpOut, DownIn, DownOut> PipelineOperator<UpIn, UpOut, DownIn, DownOut>
where
    UpIn: PipelineIO,
    UpOut: PipelineIO,
    DownIn: PipelineIO,
    DownOut: PipelineIO,
{
    /// Create a new [`PipelineOperator`] with the given [`Operator`] implementation.
    pub fn new(operator: Arc<dyn Operator<UpIn, UpOut, DownIn, DownOut>>) -> Arc<Self> {
        Arc::new(PipelineOperator {
            operator,
            downstream: Arc::new(sources::Frontend::default()),
            upstream: sinks::SinkEdge::default(),
        })
    }

    /// Access the forward edge of the [`PipelineOperator`] allowing the forward/requests paths to be linked.
    pub fn forward_edge(
        self: &Arc<Self>,
    ) -> Arc<PipelineOperatorForwardEdge<UpIn, UpOut, DownIn, DownOut>> {
        Arc::new(PipelineOperatorForwardEdge {
            parent: self.clone(),
        })
    }

    /// Access the backward edge of the [`PipelineOperator`] allowing the backward/responses paths to be linked.
    pub fn backward_edge(
        self: &Arc<Self>,
    ) -> Arc<PipelineOperatorBackwardEdge<UpIn, UpOut, DownIn, DownOut>> {
        Arc::new(PipelineOperatorBackwardEdge {
            parent: self.clone(),
        })
    }
}

// === SECTION: PipelineOperator 的 AsyncEngine / Sink / Source 实现 ===

/// A [`PipelineOperator`] is an [`AsyncEngine`] for the upstream [`AsyncEngine<UpIn, UpOut, Error>`].
#[async_trait]
impl<UpIn, UpOut, DownIn, DownOut> AsyncEngine<UpIn, UpOut, Error>
    for PipelineOperator<UpIn, UpOut, DownIn, DownOut>
where
    UpIn: PipelineIO + Sync,
    DownIn: PipelineIO + Sync,
    DownOut: PipelineIO,
    UpOut: PipelineIO,
{
    async fn generate(&self, req: UpIn) -> Result<UpOut, Error> {
        self.operator.generate(req, self.downstream.clone()).await
    }
}

#[async_trait]
impl<UpIn, UpOut, DownIn, DownOut> Sink<UpIn>
    for PipelineOperatorForwardEdge<UpIn, UpOut, DownIn, DownOut>
where
    UpIn: PipelineIO + Sync,
    DownIn: PipelineIO + Sync,
    DownOut: PipelineIO,
    UpOut: PipelineIO,
{
    async fn on_data(&self, data: UpIn, _token: private::Token) -> Result<(), Error> {
        let stream = self.parent.generate(data).await?;
        self.parent.upstream.on_next(stream, private::Token).await
    }
}

#[async_trait]
impl<UpIn, UpOut, DownIn, DownOut> Source<DownIn>
    for PipelineOperatorForwardEdge<UpIn, UpOut, DownIn, DownOut>
where
    UpIn: PipelineIO,
    DownIn: PipelineIO,
    DownOut: PipelineIO,
    UpOut: PipelineIO,
{
    async fn on_next(&self, data: DownIn, token: private::Token) -> Result<(), Error> {
        self.parent.downstream.on_next(data, token).await
    }

    fn set_edge(&self, edge: Edge<DownIn>, token: private::Token) -> Result<(), PipelineError> {
        self.parent.downstream.set_edge(edge, token)
    }
}

#[async_trait]
impl<UpIn, UpOut, DownIn, DownOut> Sink<DownOut>
    for PipelineOperatorBackwardEdge<UpIn, UpOut, DownIn, DownOut>
where
    UpIn: PipelineIO,
    DownIn: PipelineIO,
    DownOut: PipelineIO,
    UpOut: PipelineIO,
{
    async fn on_data(&self, data: DownOut, token: private::Token) -> Result<(), Error> {
        self.parent.downstream.on_data(data, token).await
    }
}

#[async_trait]
impl<UpIn, UpOut, DownIn, DownOut> Source<UpOut>
    for PipelineOperatorBackwardEdge<UpIn, UpOut, DownIn, DownOut>
where
    UpIn: PipelineIO,
    DownIn: PipelineIO,
    DownOut: PipelineIO,
    UpOut: PipelineIO,
{
    async fn on_next(&self, data: UpOut, token: private::Token) -> Result<(), Error> {
        self.parent.upstream.on_next(data, token).await
    }

    fn set_edge(&self, edge: Edge<UpOut>, token: private::Token) -> Result<(), PipelineError> {
        self.parent.upstream.set_edge(edge, token)
    }
}

// === SECTION: PipelineNode 边算子 ===

pub struct PipelineNode<In: PipelineIO, Out: PipelineIO> {
    edge: OnceLock<Edge<Out>>,
    map_fn: NodeFn<In, Out>,
}

impl<In: PipelineIO, Out: PipelineIO> PipelineNode<In, Out> {
    pub fn new(map_fn: NodeFn<In, Out>) -> Arc<Self> {
        Arc::new(PipelineNode::<In, Out> {
            edge: OnceLock::new(),
            map_fn,
        })
    }
}

#[async_trait]
impl<In: PipelineIO, Out: PipelineIO> Source<Out> for PipelineNode<In, Out> {
    async fn on_next(&self, data: Out, _: private::Token) -> Result<(), Error> {
        self.edge
            .get()
            .ok_or(PipelineError::NoEdge)?
            .write(data)
            .await
    }

    fn set_edge(&self, edge: Edge<Out>, _: private::Token) -> Result<(), PipelineError> {
        self.edge
            .set(edge)
            .map_err(|_| PipelineError::EdgeAlreadySet)?;

        Ok(())
    }
}

#[async_trait]
impl<In: PipelineIO, Out: PipelineIO> Sink<In> for PipelineNode<In, Out> {
    async fn on_data(&self, data: In, _: private::Token) -> Result<(), Error> {
        self.on_next((self.map_fn)(data)?, private::Token).await
    }
}

// === SECTION: tests ===

#[cfg(test)]
mod tests {
    //! ## 测试矩阵
    //!
    //! | 测试名 | 覆盖维度 |
    //! |---|---|
    //! | `test_pipeline_source_no_edge` | `ServiceFrontend::generate` 在未连接下游 (`Edge` 未设置) 时必须返回错误 |
    //!
    //! ## 测试过程
    //! 单一用例 `test_pipeline_source_no_edge`：构造未连接下游的 `ServiceFrontend`，
    //! 调用 `generate(().into())` 应得到错误。
    //!
    //! ## 意义
    //! 在 `nodes` 层一次性验证 `ServiceFrontend -> Frontend -> Source<In>::on_next`
    //! 这条转发链在边未设置时一定失败，避免请求被悄无声息地丢弃。

    use super::*;
    use crate::pipeline::*;

    #[tokio::test]
    async fn test_pipeline_source_no_edge() {
        let source = ServiceFrontend::<SingleIn<()>, ManyOut<()>>::new();
        let stream = source.generate(().into()).await;
        assert!(stream.is_err());
    }
}
