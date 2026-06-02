// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::nodes::sinks::base` —— SinkEdge 的 Default 与 Source 实现
//!
//! ## 设计意图
//! 把 `SinkEdge` 的 `Default` 构造与 `Source<Resp>` 行为剥离到独立文件，
//! 使 `sinks.rs` 只承担"结构体声明 + 模块拼接"职责，便于按结构 × trait
//! 二维定位代码。
//!
//! ## 外部契约
//! - `impl<Resp: PipelineIO> Default for SinkEdge<Resp>`：
//!   构造一个未连接下游的 `SinkEdge`（`edge: OnceLock::new()`）。
//! - `impl<Resp: PipelineIO> Source<Resp> for SinkEdge<Resp>`：
//!   - `on_next(data, _) -> Err(NoEdge)` 当下游未设置；否则委托 `Edge::write`。
//!   - `set_edge(edge, _) -> Err(EdgeAlreadySet)` 当 `OnceLock` 已被占用。
//! - 两个方法的 `Token` 形参属于 `super::private::Token`，外部无法构造，
//!   因而本 impl 实际上只允许 pipeline 框架内部调用。
//!
//! ## 实现要点
//! - `use super::*;` 由 `sinks.rs` 注入 `Arc / Edge / OnceLock / PipelineError /
//!   Sink / Source / async_trait / Token / SinkEdge / PipelineIO / ServiceEngine`
//!   等全部名称；此处不再单独 import，与 `pipeline.rs` / `segment.rs` 保持一致。
//! - `on_next` 故意使用方法链 `ok_or(...)?.write(data).await`，让"未连接 →
//!   返回 NoEdge"的失败路径与"已连接 → 委托写入"的成功路径在同一表达式内。
//! - `set_edge` 显式以 `Ok(())` 收尾而非 `map(|_| ())`，保持显式写法，
//!   保证错误链路与字节级语义统一。

use super::*;
use anyhow::Error;

// === SECTION: Default ===

impl<Resp: PipelineIO> Default for SinkEdge<Resp> {
    fn default() -> Self {
        Self {
            edge: OnceLock::new(),
        }
    }
}

// === SECTION: Source<Resp> ===

#[async_trait]
impl<Resp: PipelineIO> Source<Resp> for SinkEdge<Resp> {
    async fn on_next(&self, data: Resp, _: Token) -> Result<(), Error> {
        self.edge
            .get()
            .ok_or(PipelineError::NoEdge)?
            .write(data)
            .await
    }

    fn set_edge(&self, edge: Edge<Resp>, _: Token) -> Result<(), PipelineError> {
        self.edge
            .set(edge)
            .map_err(|_| PipelineError::EdgeAlreadySet)?;
        Ok(())
    }
}
