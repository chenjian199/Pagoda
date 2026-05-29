// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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
//! - 文件尾部保留 lib-copy 中"早期单体版"实现的大段注释，作为设计演进档案：
//!   说明为何最终拆出 base / common 子模块、以及 IngressPort/convert_stream 这类
//!   被放弃的替代方案 —— 这些注释**不能删除**，它们是契约的一部分。

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

// === SECTION: 设计演进档案（保留作为契约的一部分） ===

// impl<In: DataType, Out: PipelineIO> Frontend<In, Out> {
//     pub fn new() -> Arc<Self> {
//         Arc::new(Self {
//             edge: OnceLock::new(),
//             sinks: Arc::new(Mutex::new(HashMap::new())),
//         })
//     }
// }

// impl<In: DataType, Out: PipelineIO> SegmentSource<In, Out> {
//     pub fn new() -> Arc<Self> {
//         Arc::new(Self {
//             edge: OnceLock::new(),
//             sinks: Arc::new(Mutex::new(HashMap::new())),
//         })
//     }
// }

// #[async_trait]
// impl<In: DataType, Out: PipelineIO> Source<Context<In>> for Frontend<In, Out> {
//     async fn on_next(&self, data: Context<In>, _: private::Token) -> Result<(), PipelineError> {
//         self.edge
//             .get()
//             .ok_or(PipelineError::NoEdge)?
//             .write(data)
//             .await
//     }

//     fn set_edge(
//         &self,
//         edge: Edge<Context<In>>>,
//         _: private::Token,
//     ) -> Result<(), PipelineError> {
//         self.edge
//             .set(edge)
//             .map_err(|_| PipelineError::EdgeAlreadySet)?;
//         Ok(())
//     }
// }

// #[async_trait]
// impl<In: DataType, Out: PipelineIO> Sink<PipelineStream<Out>> for Frontend<In, Out> {
//     async fn on_data(
//         &self,
//         data: PipelineStream<Out>,
//         _: private::Token,
//     ) -> Result<(), PipelineError> {
//         let context = data.context();

//         let mut sinks = self.sinks.lock().unwrap();
//         let tx = sinks
//             .remove(context.id())
//             .ok_or(PipelineError::DetachedStreamReceiver)
//             .map_err(|e| {
//                 data.context().stop_generating();
//                 e
//             })?;
//         drop(sinks);

//         let ctx = data.context();
//         tx.send(data)
//             .map_err(|_| PipelineError::DetachedStreamReceiver)
//             .map_err(|e| {
//                 ctx.stop_generating();
//                 e
//             })
//     }
// }

// impl<In: DataType, Out: PipelineIO> Link<Context<In>> for Frontend<In, Out> {
//     fn link<S: Sink<Context<In>> + 'static>(&self, sink: Arc<S>) -> Result<Arc<S>, PipelineError> {
//         let edge = Edge::new(sink.clone());
//         self.set_edge(edge.into(), private::Token {})?;
//         Ok(sink)
//     }
// }

// #[async_trait]
// impl<In: DataType, Out: PipelineIO> AsyncEngine<Context<In>, Annotated<Out>, PipelineError>
//     for Frontend<In, Out>
// {
//     async fn generate(&self, request: Context<In>) -> Result<PipelineStream<Out>, PipelineError> {
//         let (tx, rx) = oneshot::channel::<PipelineStream<Out>>();
//         {
//             let mut sinks = self.sinks.lock().unwrap();
//             sinks.insert(request.id().to_string(), tx);
//         }
//         self.on_next(request, private::Token {}).await?;
//         rx.await.map_err(|_| PipelineError::DetachedStreamSender)
//     }
// }

// // SegmentSource

// #[async_trait]
// impl<In: DataType, Out: PipelineIO> Source<Context<In>> for SegmentSource<In, Out> {
//     async fn on_next(&self, data: Context<In>, _: private::Token) -> Result<(), PipelineError> {
//         self.edge
//             .get()
//             .ok_or(PipelineError::NoEdge)?
//             .write(data)
//             .await
//     }

//     fn set_edge(
//         &self,
//         edge: Edge<Context<In>>>,
//         _: private::Token,
//     ) -> Result<(), PipelineError> {
//         self.edge
//             .set(edge)
//             .map_err(|_| PipelineError::EdgeAlreadySet)?;
//         Ok(())
//     }
// }

// #[async_trait]
// impl<In: DataType, Out: PipelineIO> Sink<PipelineStream<Out>> for SegmentSource<In, Out> {
//     async fn on_data(
//         &self,
//         data: PipelineStream<Out>,
//         _: private::Token,
//     ) -> Result<(), PipelineError> {
//         let context = data.context();

//         let mut sinks = self.sinks.lock().unwrap();
//         let tx = sinks
//             .remove(context.id())
//             .ok_or(PipelineError::DetachedStreamReceiver)
//             .map_err(|e| {
//                 data.context().stop_generating();
//                 e
//             })?;
//         drop(sinks);

//         let ctx = data.context();
//         tx.send(data)
//             .map_err(|_| PipelineError::DetachedStreamReceiver)
//             .map_err(|e| {
//                 ctx.stop_generating();
//                 e
//             })
//     }
// }

// impl<In: DataType, Out: PipelineIO> Link<Context<In>> for SegmentSource<In, Out> {
//     fn link<S: Sink<Context<In>> + 'static>(&self, sink: Arc<S>) -> Result<Arc<S>, PipelineError> {
//         let edge = Edge::new(sink.clone());
//         self.set_edge(edge.into(), private::Token {})?;
//         Ok(sink)
//     }
// }

// #[async_trait]
// impl<In: DataType, Out: PipelineIO> AsyncEngine<Context<In>, Annotated<Out>, PipelineError>
//     for SegmentSource<In, Out>
// {
//     async fn generate(&self, request: Context<In>) -> Result<PipelineStream<Out>, PipelineError> {
//         let (tx, rx) = oneshot::channel::<PipelineStream<Out>>();
//         {
//             let mut sinks = self.sinks.lock().unwrap();
//             sinks.insert(request.id().to_string(), tx);
//         }
//         self.on_next(request, private::Token {}).await?;
//         rx.await.map_err(|_| PipelineError::DetachedStreamSender)
//     }
// }

// ## 测试矩阵（历史样本，已随实现演进注释保留）
//
// | 测试名 | 覆盖维度 |
// |---|---|
// | `test_pipeline_source_no_edge` | 未绑定下游 `Edge` 时 `generate()` 应返回 `PipelineError::NoEdge`；当前 Source 已拆分到 base/common 子模块，该单测作为设计档案保留，未在本文件启用 |

// #[cfg(test)]

// mod tests {
//     use super::*;

//     #[tokio::test]
//     async fn test_pipeline_source_no_edge() {
//         let source = Frontend::<(), ()>::new();
//         let stream = source.generate(().into()).await;
//         match stream {
//             Err(PipelineError::NoEdge) => (),
//             _ => panic!("Expected NoEdge error"),
//         }
//     }
// }

// pub struct IngressPort<In, Out: PipelineIO> {
//     edge: OnceLock<ServiceEngine<In, Out>>,
// }

// impl<In, Out> IngressPort<In, Out>
// where
//     In: for<'de> Deserialize<'de> + DataType,
//     Out: PipelineIO + Serialize,
// {
//     pub fn new() -> Arc<Self> {
//         Arc::new(IngressPort {
//             edge: OnceLock::new(),
//         })
//     }
// }

// #[async_trait]
// impl<In, Out> AsyncEngine<Context<Vec<u8>>, Vec<u8>> for IngressPort<In, Out>
// where
//     In: for<'de> Deserialize<'de> + DataType,
//     Out: PipelineIO + Serialize,
// {
//     async fn generate(
//         &self,
//         request: Context<Vec<u8>>,
//     ) -> Result<EngineStream<Vec<u8>>, PipelineError> {
//         // Deserialize request
//         let request = request.try_map(|bytes| {
//             bincode::deserialize::<In>(&bytes)
//                 .map_err(|err| PipelineError(format!("Failed to deserialize request: {}", err)))
//         })?;

//         // Forward request to edge
//         let stream = self
//             .edge
//             .get()
//             .ok_or(PipelineError("No engine to forward request to".to_string()))?
//             .generate(request)
//             .await?;

//         // Serialize response stream

//         let stream =
//             stream.map(|resp| bincode::serialize(&resp).expect("Failed to serialize response"));

//         Err(PipelineError(format!("Not implemented")))
//     }
// }

// fn convert_stream<T, U>(
//     stream: impl Stream<Item = ServerStream<T>> + Send + 'static,
//     ctx: Arc<dyn AsyncEngineContext>,
//     transform: Arc<dyn Fn(T) -> Result<U, StreamError> + Send + Sync>,
// ) -> Pin<Box<dyn Stream<Item = ServerStream<U>> + Send>>
// where
//     T: Send + 'static,
//     U: Send + 'static,
// {
//     Box::pin(stream.flat_map(move |item| {
//         let ctx = ctx.clone();
//         let transform = transform.clone();
//         match item {
//             ServerStream::Data(data) => match transform(data) {
//                 Ok(transformed) => futures::stream::iter(vec![ServerStream::Data(transformed)]),
//                 Err(e) => {
//                     // Trigger cancellation and propagate the error, followed by Sentinel
//                     ctx.stop_generating();
//                     futures::stream::iter(vec![ServerStream::Error(e), ServerStream::Sentinel])
//                 }
//             },
//             other => futures::stream::iter(vec![other]),
//         }
//     })
//     // Use take_while to stop processing when encountering the Sentinel
//     .take_while(|item| futures::future::ready(!matches!(item, ServerStream::Sentinel))))
// }
