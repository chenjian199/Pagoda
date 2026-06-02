// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::nodes::sources::base` —— Frontend 的 Default/Source/Sink/AsyncEngine 四实现
//!
//! ## 设计意图
//! `Frontend<In, Out>` 是请求/响应桥接的最小内核：
//! 1. `generate(req)` 时给 `req.id()` 在 `sinks` 表中注册一个 `oneshot::Sender<Out>`；
//! 2. 通过 `Source<In>::on_next` 把请求经 `Edge<In>` 推给下游；
//! 3. `await` 同一个 `oneshot::Receiver<Out>`，直到响应方向 `Sink<Out>::on_data`
//!    用相同的 `id` 把响应回投。
//!
//! 本文件把这四种 trait 实现集中起来，便于把"id 注册 / 转发 / 等待 / 回投"
//! 这四步流程在一处看完。
//!
//! ## 外部契约
//! - `impl Default for Frontend<In, Out>`：零字段构造 `Frontend { edge: OnceLock::new(),
//!   sinks: Arc::new(Mutex::new(HashMap::new())) }`。
//! - `impl Source<In> for Frontend<In, Out>`：
//!   - `on_next` 未连接边时返回 `PipelineError::NoEdge`；
//!   - `set_edge` 重复设置时返回 `PipelineError::EdgeAlreadySet`。
//! - `impl Sink<Out> for Frontend<In, Out> where Out: PipelineIO + AsyncEngineContextProvider`：
//!   - `on_data` 通过 `data.context().id()` 找回对应的 `Sender`；找不到 → 取消上下文 +
//!     `PipelineError::DetachedStreamReceiver`；
//!   - 找到后 `tx.send(data)`；发送失败同样取消上下文 + `DetachedStreamReceiver`。
//! - `impl AsyncEngine<In, Out, Error> for Frontend<In, Out> where In: PipelineIO + Sync`：
//!   - 先注册 `(id, tx)`，再 `on_next(request, Token{})`，最后 `rx.await`；
//!   - `rx.await` 失败 → `PipelineError::DetachedStreamSender`。
//! - `sinks` 字段类型为 `Arc<std::sync::Mutex<HashMap<...>>>`（**同步锁**），
//!   通过 `lock().unwrap()` 持有；这是契约的一部分，不可改为 `tokio::sync::Mutex`。
//! - `#[cfg(test)] mod tests` 至少包含 `test_frontend_no_edge` 测试。
//!
//! ## 实现要点
//! - `use super::*;` 由 `sources.rs` 注入 `Frontend / Edge / private / Source / Sink /
//!   PipelineIO / PipelineError / async_trait / AsyncEngine / oneshot / Arc /
//!   Mutex / HashMap / OnceLock` 等全部名称。
//! - `AsyncEngineContextProvider` 不在 `super::*` 中，需要在此显式 `use`。
//! - `on_data` 中将 `sinks` 锁先 `remove` 后立即 `drop(sinks)`，再 `tx.send`，
//!   避免持有锁的同时跨 await。
//! - 错误链路均使用 `.inspect_err(|_| ctx.stop_generating())` 显式取消上下文，
//!   保证调用方的 cancellation token 一定收到信号。

use crate::engine::AsyncEngineContextProvider;

use super::*;

// === SECTION: Default ===

impl<In: PipelineIO, Out: PipelineIO> Default for Frontend<In, Out> {
    fn default() -> Self {
        Self {
            edge: OnceLock::new(),
            sinks: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }
}

// === SECTION: Source<In> ===

#[async_trait]
impl<In: PipelineIO, Out: PipelineIO> Source<In> for Frontend<In, Out> {
    async fn on_next(&self, data: In, _: private::Token) -> Result<(), Error> {
        self.edge
            .get()
            .ok_or(PipelineError::NoEdge)?
            .write(data)
            .await
    }

    fn set_edge(&self, edge: Edge<In>, _: private::Token) -> Result<(), PipelineError> {
        self.edge
            .set(edge)
            .map_err(|_| PipelineError::EdgeAlreadySet)?;
        Ok(())
    }
}

// === SECTION: Sink<Out> ===

#[async_trait]
impl<In: PipelineIO, Out: PipelineIO + AsyncEngineContextProvider> Sink<Out> for Frontend<In, Out> {
    async fn on_data(&self, data: Out, _: private::Token) -> Result<(), Error> {
        let ctx = data.context();

        let mut sinks = self.sinks.lock().unwrap();
        let tx = sinks
            .remove(ctx.id())
            .ok_or(PipelineError::DetachedStreamReceiver)
            .inspect_err(|_| {
                ctx.stop_generating();
            })?;
        drop(sinks);

        Ok(tx
            .send(data)
            .map_err(|_| PipelineError::DetachedStreamReceiver)
            .inspect_err(|_| {
                ctx.stop_generating();
            })?)
    }
}

// === SECTION: AsyncEngine<In, Out, Error> ===

#[async_trait]
impl<In: PipelineIO + Sync, Out: PipelineIO> AsyncEngine<In, Out, Error> for Frontend<In, Out> {
    async fn generate(&self, request: In) -> Result<Out, Error> {
        let (tx, rx) = oneshot::channel::<Out>();
        {
            let mut sinks = self.sinks.lock().unwrap();
            sinks.insert(request.id().to_string(), tx);
        }
        self.on_next(request, private::Token {}).await?;
        Ok(rx.await.map_err(|_| PipelineError::DetachedStreamSender)?)
    }
}

// === SECTION: tests ===

#[cfg(test)]
mod tests {
    //! ## 测试矩阵
    //!
    //! | 测试名 | 覆盖维度 |
    //! |---|---|
    //! | `test_frontend_no_edge` | `Frontend` 在 `edge` 未设置时，`generate` 与 `on_next` 均返回 `PipelineError::NoEdge` |
    //!
    //! ## 测试过程
    //! 仅一个用例 `test_frontend_no_edge`：构造未连接下游的 `Frontend`，
    //! ① 调用 `generate(().into())` 期望返回 `PipelineError::NoEdge`；
    //! ② 直接调用 `on_next(().into(), Token)` 同样期望 `NoEdge`。
    //!
    //! ## 意义
    //! 覆盖 `Source<In>::on_next` 与 `AsyncEngine::generate` 两条入口
    //! 在"边未设置"前的失败语义，确保未来重构不会让请求悄无声息地丢失。

    use super::*;
    use crate::pipeline::{ManyOut, SingleIn, error::PipelineErrorExt};

    #[tokio::test]
    async fn test_frontend_no_edge() {
        let source = Frontend::<SingleIn<()>, ManyOut<()>>::default();
        let error = source
            .generate(().into())
            .await
            .unwrap_err()
            .try_into_pipeline_error()
            .unwrap();

        match error {
            PipelineError::NoEdge => (),
            _ => panic!("Expected NoEdge error"),
        }

        let result = source
            .on_next(().into(), private::Token)
            .await
            .unwrap_err()
            .try_into_pipeline_error()
            .unwrap();

        match result {
            PipelineError::NoEdge => (),
            _ => panic!("Expected NoEdge error"),
        }
    }
}
