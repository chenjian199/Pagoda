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
//! - `AsyncTransportEngine` trait 仅为 marker（无额外方法），契约保持稳定。
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

/// 流水线输入携带一个 [`Context`]，用于在请求中附带元数据或额外信息。
/// 这些信息会随着各处理阶段在本地与分布式两种场景下一路传播。
pub type SingleIn<T> = Context<T>;

/// 流式请求引擎的输入：一个 trait object 别名，包装一个自带取消上下文的流
///（取消上下文通过 [`AsyncEngineStream`] 的 [`AsyncEngineContextProvider`] 一面提供）。
///
/// 这是 [`ManyOut`] 在输入侧的镜像。两个别名都解析到同一个底层
/// [`EngineStream<T>`] 类型，方向性命名仅用于表意。之所以不采用把流包进
/// `Context<…>`（`Context<DataStream<T>>`）的形态，是因为该形态无法实例化：
/// `DataStream<T>` 是 `!Sync`，而 `Context<T: Data>` 要求 `Sync`。trait object
/// 形态干净地解决了这一点：取消能力成为 trait 契约的一部分，而非外层包装。
pub type ManyIn<T> = EngineStream<T>;

/// 返回单个值的流水线输出类型别名
pub type SingleOut<T> = EngineUnary<T>;

/// 返回多个值的流水线输出类型别名
pub type ManyOut<T> = EngineStream<T>;

pub type ServiceEngine<T, U> = Engine<T, U, Error>;

/// Unary 引擎是接收单个输入并返回单个输出的流水线
pub type UnaryEngine<T, U> = ServiceEngine<SingleIn<T>, SingleOut<U>>;

/// `ClientStreaming` 引擎是接收多个输入并返回单个输出的流水线。
/// 通常引擎会消费整个输入流；但它也可以提前退出，在未消费完整输入流的情况下发出响应。
pub type ClientStreamingEngine<T, U> = ServiceEngine<ManyIn<T>, SingleOut<U>>;

/// `ServerStreaming` 接收单个输入并返回多个输出。
pub type ServerStreamingEngine<T, U> = ServiceEngine<SingleIn<T>, ManyOut<U>>;

/// `BidirectionalStreaming` 接收多个输入并返回多个输出。输入值与输出值
/// 彼此独立；但也可以被约束为相互关联。
pub type BidirectionalStreamingEngine<T, U> = ServiceEngine<ManyIn<T>, ManyOut<U>>;

pub trait AsyncTransportEngine<T: Data + PipelineIO, U: Data + PipelineIO>:
    AsyncEngine<T, U, Error> + Send + Sync + 'static
{
}

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
