// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline` —— 流水线类型体系与 IO 契约
//!
//! ## 设计意图
//! 为分布式与本地共存的 `AsyncEngine` 调用模型提供一组统一的类型别名：
//! 把 “单值/流式” × “输入/输出” 四种组合压缩成 `SingleIn` / `ManyIn` /
//! `SingleOut` / `ManyOut` 四个核心别名，再派生出 Unary / ClientStreaming /
//! ServerStreaming / BidirectionalStreaming 四种 `ServiceEngine` 形态；并通过
//! `PipelineIO` trait 把 “携带 cancellation 上下文” 这一不变量抽离出来，使得
//! 节点之间能够以 trait object 的形式互连。
//!
//! ## 外部契约
//! - 公开类型别名（`SingleIn`/`SingleOut`/`ManyIn`/`ManyOut`/各 `*Engine`）签名严格保持。
//! - `PipelineIO::id(&self) -> String` 必须返回底层 `AsyncEngineContext.id()` 的克隆。
//! - `Event { id }` 结构体字段顺序与可见性保持。
//! - `AsyncTransportEngine` trait 仅为 marker（无额外方法），契约与基线一致。
//! - 子模块导出列表（`nodes::*`、`network::egress::*`、`context`、`error`、`registry`）保持。
//!
//! ## 实现要点
//! - `PipelineIO` 三个实现（`Context<T>` / `EngineUnary<T>` / `EngineStream<T>`）行为相同，
//!   故抽出私有 `context_id` 辅助函数 + `impl_pipeline_io!` 声明宏消除重复；
//!   `Context<T>` 仍显式手写一份（与宏展开等价）以保留主类型的可读性。
//! - `sealed::Connectable` + `sealed::Token` 仍是 “封装第三方扩展” 的私有 marker，
//!   外部无法构造 `Token`，因而 `PipelineIO` 实质上是 sealed trait。

use serde::{Deserialize, Serialize};

// === SECTION: 子模块声明与重导出 ===

mod nodes;
pub use nodes::{
    Operator, PipelineNode, PipelineOperator, SegmentSink, SegmentSource, Service, ServiceBackend,
    ServiceFrontend, Sink, Source,
};

pub mod context;
pub mod error;
pub mod network;
pub use network::egress::addressed_router::{AddressedPushRouter, AddressedRequest};
pub use network::egress::push_router::{PushRouter, RouterMode, WorkerLoadMonitor};
pub mod registry;

pub use crate::engine::{
    self as engine, AsyncEngine, AsyncEngineContext, AsyncEngineContextProvider, Data, DataStream,
    Engine, EngineStream, EngineUnary, RequestStream, ResponseStream, async_trait,
};
pub use anyhow::Error;
pub use context::Context;
pub use error::{PipelineError, PipelineErrorExt, TwoPartCodecError};

/// Pipeline inputs carry a [`Context`] which can be used to carry metadata or additional information
/// about the request. This information propagates through the stages, both local and distributed.
pub type SingleIn<T> = Context<T>;

/// Pipeline input for streaming-request engines: a trait-object alias around a
/// stream that carries its own cancellation context (via the
/// [`AsyncEngineContextProvider`] half of [`AsyncEngineStream`]).
///
/// This is the input-side mirror of [`ManyOut`]. Both aliases resolve to the
/// same underlying [`EngineStream<T>`] type; the directional names are
/// documentary. Earlier definitions wrapped the stream in a `Context<…>`
/// (`Context<DataStream<T>>`); that shape was uninstantiable because
/// `DataStream<T>` is `!Sync` while `Context<T: Data>` requires `Sync`. The
/// trait-object form solves that cleanly: the cancellation surface is part of
/// the trait contract rather than an outer wrapper.
pub type ManyIn<T> = EngineStream<T>;

/// Type alias for the output of pipeline that returns a single value
pub type SingleOut<T> = EngineUnary<T>;

/// Type alias for the output of pipeline that returns multiple values
pub type ManyOut<T> = EngineStream<T>;

pub type ServiceEngine<T, U> = Engine<T, U, Error>;

/// Unary Engine is a pipeline that takes a single input and returns a single output
pub type UnaryEngine<T, U> = ServiceEngine<SingleIn<T>, SingleOut<U>>;

/// `ClientStreaming` Engine is a pipeline that takes multiple inputs and returns a single output
/// Typically the engine will consume the entire input stream; however, it can also decided to exit
/// early and emit a response without consuming the entire input stream.
pub type ClientStreamingEngine<T, U> = ServiceEngine<ManyIn<T>, SingleOut<U>>;

/// `ServerStreaming` takes a single input and returns multiple outputs.
pub type ServerStreamingEngine<T, U> = ServiceEngine<SingleIn<T>, ManyOut<U>>;

/// `BidirectionalStreaming` takes multiple inputs and returns multiple outputs. Input and output values
/// are considered independent of each other; however, they could be constrained to be related.
pub type BidirectionalStreamingEngine<T, U> = ServiceEngine<ManyIn<T>, ManyOut<U>>;

pub trait AsyncTransportEngine<T: Data + PipelineIO, U: Data + PipelineIO>:
    AsyncEngine<T, U, Error> + Send + Sync + 'static
{
}

// pub type TransportEngine<T, U> = Arc<dyn AsyncTransportEngine<T, U>>;

mod sealed {
    use super::*;

    #[allow(dead_code)]
    pub struct Token;

    pub trait Connectable {
        type DataType: Data;
    }

    impl<T: Data> Connectable for Context<T> {
        type DataType = T;
    }
    impl<T: Data> Connectable for EngineUnary<T> {
        type DataType = T;
    }
    impl<T: Data> Connectable for EngineStream<T> {
        type DataType = T;
    }
}

// === SECTION: PipelineIO trait 与统一实现 ===

pub trait PipelineIO: sealed::Connectable + AsyncEngineContextProvider + 'static {
    fn id(&self) -> String;
}

fn context_id(value: &impl AsyncEngineContextProvider) -> String {
    value.context().id().to_string()
}

macro_rules! impl_pipeline_io {
    ($ty:ident) => {
        impl<T: Data> PipelineIO for $ty<T> {
            fn id(&self) -> String {
                context_id(self)
            }
        }
    };
}

impl<T: Data> PipelineIO for Context<T> {
    fn id(&self) -> String {
        context_id(self)
    }
}

impl_pipeline_io!(EngineUnary);
impl_pipeline_io!(EngineStream);

// === SECTION: 跨节点事件载荷 ===

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Event {
    pub id: String,
}
