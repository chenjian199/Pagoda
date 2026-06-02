// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::nodes::sinks::pipeline` —— ServiceBackend 的构造与 trait 实现
//!
//! ## 设计意图
//! 为 `ServiceBackend<Req, Resp>` 提供构造入口（`from_engine`），并把它接入
//! pipeline 抽象：作为 `Sink<Req>` 接收上游请求、调用绑定引擎、把响应再以
//! `Source<Resp>` 形式发往下游。响应方向的转发委托给 `SinkEdge<Resp>`，
//! 让请求路径与响应路径解耦。
//!
//! ## 外部契约
//! - `ServiceBackend::from_engine(engine) -> Arc<Self>`：唯一构造路径。
//!   返回 `Arc` 保证后续可以共享挂接到多条流水线上。
//! - `impl Sink<Req> for ServiceBackend<Req, Resp> where Req: PipelineIO + Sync`：
//!   `on_data` 调用 `engine.generate(data).await?`，把得到的响应流再以
//!   `self.on_next(stream, Token).await` 转发给响应方向。
//! - `impl Source<Resp> for ServiceBackend<Req, Resp> where Req: PipelineIO`：
//!   - `on_next` 透传到 `self.inner.on_next(...)`；
//!   - `set_edge` 透传到 `self.inner.set_edge(...)`。
//! - **泛型 bound 严格区分**：`Sink` 实现需 `Req: PipelineIO + Sync`，
//!   `Source` 实现仅需 `Req: PipelineIO`（**不**带 `Sync`）；这一不对称
//!   是契约的一部分，禁止统一加 `Sync`。
//!
//! ## 实现要点
//! - `use super::*;` 由 `sinks.rs` 注入 `Arc / Edge / Sink / Source / async_trait
//!   / Token / ServiceBackend / SinkEdge / ServiceEngine / PipelineIO /
//!   PipelineError` 等全部名称；本文件不重复 import。
//! - `from_engine` 直接 `Arc::new(Self { engine, inner: SinkEdge::default() })`，
//!   不暴露 `new` / `default`，避免外部错误构造一个无引擎的 `ServiceBackend`。
//! - `on_data` 中保留变量名 `stream`（而非 `resp`），便于
//!   读者识别"响应可能是单值或流"的事实语义。

use super::*;
use anyhow::Error;

// === SECTION: 构造入口 ===

impl<Req: PipelineIO, Resp: PipelineIO> ServiceBackend<Req, Resp> {
    pub fn from_engine(engine: ServiceEngine<Req, Resp>) -> Arc<Self> {
        Arc::new(Self {
            engine,
            inner: SinkEdge::default(),
        })
    }
}

// === SECTION: Sink<Req> ===

#[async_trait]
impl<Req: PipelineIO + Sync, Resp: PipelineIO> Sink<Req> for ServiceBackend<Req, Resp> {
    async fn on_data(&self, data: Req, _: Token) -> Result<(), Error> {
        let stream = self.engine.generate(data).await?;
        self.on_next(stream, Token).await
    }
}

// === SECTION: Source<Resp> ===

#[async_trait]
impl<Req: PipelineIO, Resp: PipelineIO> Source<Resp> for ServiceBackend<Req, Resp> {
    async fn on_next(&self, data: Resp, _: Token) -> Result<(), Error> {
        self.inner.on_next(data, Token).await
    }

    fn set_edge(&self, edge: Edge<Resp>, _: Token) -> Result<(), PipelineError> {
        self.inner.set_edge(edge, Token)
    }
}
