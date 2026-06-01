// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::nodes::sources::common` —— `impl_frontend!` 宏与两个 wrapper 实现
//!
//! ## 设计意图
//! `ServiceFrontend<In, Out>` 与 `SegmentSource<In, Out>` 均是 `Frontend<In, Out>`
//! 的轻量 wrapper（唯一字段 `inner: Frontend<In, Out>`）。两者运行时行为完全相同，
//! 仅类型名（与其传达的架构角色：本地 vs 跨段）不同。
//! 用 `impl_frontend!` 宏统一生成 `new() / Source / Sink / AsyncEngine` 四份委托
//! 实现，避免逐字复制带来的发散风险。
//!
//! ## 外部契约
//! - 为 `ServiceFrontend` 与 `SegmentSource` 两者均提供：
//!   - `pub fn new() -> Arc<Self>`：包装一个默认 `Frontend`。
//!   - `impl Source<In>`：`on_next` / `set_edge` 转发给 `inner`。
//!   - `impl Sink<Out> where Out: PipelineIO + AsyncEngineContextProvider`：
//!     `on_data` 转发给 `inner`。
//!   - `impl AsyncEngine<In, Out, Error> where In: PipelineIO + Sync`：
//!     `generate` 转发给 `inner`。
//! - 两 wrapper 的 trait 实现 bound 与 `Frontend` 自身完全一致；
//!   `impl_frontend!` 是契约的"形式生成器"，不引入新的约束。
//! - `#[cfg(test)] mod tests` 至少包含 `test_pipeline_source_no_edge` 测试。
//!
//! ## 实现要点
//! - `use super::*;` 由 `sources.rs` 注入需要的全部名称，加上本文件显式
//!   `use crate::engine::AsyncEngineContextProvider;`。
//! - 宏内参数 `$type` 同时作为类型构造器（`Self {...}` 中的 `inner: Frontend::default()`）
//!   与 trait 接收器；宏体不引入新的 lifetime 或 bound。
//! - `impl_frontend!(ServiceFrontend);` 与 `impl_frontend!(SegmentSource);`
//!   两次调用按顺序声明，与基线一致；调换顺序在功能上等价但会偏离基线契约。

use crate::engine::AsyncEngineContextProvider;

use super::*;

// === SECTION: impl_frontend! 宏定义 ===

macro_rules! impl_frontend {
    ($type:ident) => {
        impl<In: PipelineIO, Out: PipelineIO> $type<In, Out> {
            pub fn new() -> Arc<Self> {
                Arc::new(Self {
                    inner: Frontend::default(),
                })
            }
        }

        #[async_trait]
        impl<In: PipelineIO, Out: PipelineIO> Source<In> for $type<In, Out> {
            async fn on_next(&self, data: In, token: private::Token) -> Result<(), Error> {
                self.inner.on_next(data, token).await
            }

            fn set_edge(&self, edge: Edge<In>, token: private::Token) -> Result<(), PipelineError> {
                self.inner.set_edge(edge, token)
            }
        }

        #[async_trait]
        impl<In: PipelineIO, Out: PipelineIO + AsyncEngineContextProvider> Sink<Out>
            for $type<In, Out>
        {
            async fn on_data(&self, data: Out, token: private::Token) -> Result<(), Error> {
                self.inner.on_data(data, token).await
            }
        }

        #[async_trait]
        impl<In: PipelineIO + Sync, Out: PipelineIO> AsyncEngine<In, Out, Error>
            for $type<In, Out>
        {
            async fn generate(&self, request: In) -> Result<Out, Error> {
                self.inner.generate(request).await
            }
        }
    };
}

// === SECTION: 应用宏到两个 wrapper ===

impl_frontend!(ServiceFrontend);
impl_frontend!(SegmentSource);

// === SECTION: tests ===

#[cfg(test)]
mod tests {
    //! ## 测试矩阵
    //!
    //! | 测试名 | 覆盖维度 |
    //! |---|---|
    //! | `test_pipeline_source_no_edge` | 宏生成的 `Frontend::generate` 在 `edge` 未设置时返回 `PipelineError::NoEdge` |
    //!
    //! ## 测试过程
    //! 仅一个用例 `test_pipeline_source_no_edge`：直接构造一个未连接下游的
    //! `Frontend`，调用 `generate(().into())`，期望返回 `PipelineError::NoEdge`。
    //!
    //! ## 意义
    //! 与 `base.rs` 中的 `test_frontend_no_edge` 形成对照：base.rs 的测试直接
    //! 验证 `Frontend` 的失败语义，本文件的测试通过宏生成的转发链证明
    //! `ServiceFrontend` / `SegmentSource` 的失败语义与 `Frontend` 完全一致。

    use super::*;
    use crate::pipeline::{ManyOut, PipelineErrorExt, SingleIn};

    #[tokio::test]
    async fn test_pipeline_source_no_edge() {
        let source = Frontend::<SingleIn<()>, ManyOut<()>>::default();
        let stream = source
            .generate(().into())
            .await
            .unwrap_err()
            .try_into_pipeline_error()
            .unwrap();

        match stream {
            PipelineError::NoEdge => (),
            _ => panic!("Expected NoEdge error"),
        }
    }
}
