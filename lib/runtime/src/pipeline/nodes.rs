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

// TODO：后续重命名 `ServicePipelineExt`。
/// [`Source`] trait 定义了数据如何从源节点发向下游 sink。
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

/// [`Sink`] trait 定义了数据如何从源节点接收并处理。
#[async_trait]
pub trait Sink<T: PipelineIO>: Data {
    async fn on_data(&self, data: T, _: private::Token) -> Result<(), Error>;
}

// === SECTION: Edge<T> ===

/// [`Edge`] 是 [`Source`] 与 [`Sink`] 之间的连接。
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

/// [`Operator`] trait 定义两个 [`AsyncEngine`] 如何串联时的行为。
/// [`Operator`] 本身不完全等同于 [`AsyncEngine`]，因为它的 generate 方法既需要上游请求，
/// 也需要被传入变换后请求的下游 [`AsyncEngine`]。
/// [`Operator`] 的逻辑必须把上游请求 `UpIn` 变换为下游请求 `DownIn`，
/// 再把下游响应 `DownOut` 变换回上游响应 `UpOut`。
///
/// [`PipelineOperator`] 接收一个 [`Operator`]，并以面向上游的 [`AsyncEngine`]
/// [`AsyncEngine<UpIn, UpOut, Error>`] 形式呈现自己。
///
/// ### 类型变换与数据流示例
/// ```text
/// ... --> <UpIn> ---> [Operator] --> <DownIn> ---> ...
/// ... <-- <UpOut> --> [Operator] <-- <DownOut> <-- ...
/// ```
#[async_trait]
pub trait Operator<UpIn: PipelineIO, UpOut: PipelineIO, DownIn: PipelineIO, DownOut: PipelineIO>:
    Data
{
    /// 这个方法应当把上游请求 `UpIn` 变换为下游请求 `DownIn`，
    /// 用变换后的请求调用下一个 [`AsyncEngine`]，再把下游响应 `DownOut`
    /// 变换回上游响应 `UpOut`。
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

/// [`PipelineOperatorForwardEdge`] 对上游请求类型 `UpIn` 是 [`Sink`]，
/// 对下游请求类型 `DownIn` 是 [`Source`]。
pub struct PipelineOperatorForwardEdge<
    UpIn: PipelineIO,
    UpOut: PipelineIO,
    DownIn: PipelineIO,
    DownOut: PipelineIO,
> {
    parent: Arc<PipelineOperator<UpIn, UpOut, DownIn, DownOut>>,
}

/// [`PipelineOperatorBackwardEdge`] 对下游响应类型 `DownOut` 是 [`Sink`]，
/// 对上游响应类型 `UpOut` 是 [`Source`]。
pub struct PipelineOperatorBackwardEdge<
    UpIn: PipelineIO,
    UpOut: PipelineIO,
    DownIn: PipelineIO,
    DownOut: PipelineIO,
> {
    parent: Arc<PipelineOperator<UpIn, UpOut, DownIn, DownOut>>,
}

/// [`PipelineOperator`] 是一个节点，它可以使用 [`Operator`] trait 的逻辑同时变换
/// 前向和后向两条路径。
pub struct PipelineOperator<
    UpIn: PipelineIO,
    UpOut: PipelineIO,
    DownIn: PipelineIO,
    DownOut: PipelineIO,
> {
    // 这个对象的核心业务逻辑
    operator: Arc<dyn Operator<UpIn, UpOut, DownIn, DownOut>>,

    // 通过通用 frontend 持有下游连接
    // frontend 同时提供 source 和 sink 两种接口
    downstream: Arc<sources::Frontend<DownIn, DownOut>>,

    // 持有到前一个/上游响应 sink 的连接
    // 我们是上游响应 sink 的一个 source
    upstream: sinks::SinkEdge<UpOut>,
}

impl<UpIn, UpOut, DownIn, DownOut> PipelineOperator<UpIn, UpOut, DownIn, DownOut>
where
    UpIn: PipelineIO,
    UpOut: PipelineIO,
    DownIn: PipelineIO,
    DownOut: PipelineIO,
{
    /// 使用给定的 [`Operator`] 实现创建一个新的 [`PipelineOperator`]。
    pub fn new(operator: Arc<dyn Operator<UpIn, UpOut, DownIn, DownOut>>) -> Arc<Self> {
        Arc::new(PipelineOperator {
            operator,
            downstream: Arc::new(sources::Frontend::default()),
            upstream: sinks::SinkEdge::default(),
        })
    }

    /// 访问 [`PipelineOperator`] 的前向边，用于连接请求路径。
    pub fn forward_edge(
        self: &Arc<Self>,
    ) -> Arc<PipelineOperatorForwardEdge<UpIn, UpOut, DownIn, DownOut>> {
        Arc::new(PipelineOperatorForwardEdge {
            parent: self.clone(),
        })
    }

    /// 访问 [`PipelineOperator`] 的后向边，用于连接响应路径。
    pub fn backward_edge(
        self: &Arc<Self>,
    ) -> Arc<PipelineOperatorBackwardEdge<UpIn, UpOut, DownIn, DownOut>> {
        Arc::new(PipelineOperatorBackwardEdge {
            parent: self.clone(),
        })
    }
}

// === SECTION: PipelineOperator 的 AsyncEngine / Sink / Source 实现 ===

/// [`PipelineOperator`] 作为一个面向上游的 [`AsyncEngine`] 使用，
/// 对应 [`AsyncEngine<UpIn, UpOut, Error>`]。
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
