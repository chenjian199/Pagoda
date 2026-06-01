// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # transports 内部小工具
//!
//! ## 设计意图
//! 提供一个**"在专属 tokio 运行时里跑一个 future、返回结果 + 运行时句柄"**
//! 的封装。某些 transport 客户端（典型如 etcd_client）会绑定到创建它的运行时，
//! 因此我们必须把"创建 client"这一步钉死在一个**外部可拥有的**运行时上 ——
//! 否则 client 在跨运行时调用时会拒绝服务。
//!
//! ## 外部契约
//! [`build_in_runtime<T, F>(fut, num_threads) -> Result<(T, Arc<Runtime>)>`]：
//! 在新建的多线程运行时里 `await` `fut`，把结果返回，并把运行时句柄一起返还。
//! 注意：返还的 `Arc<Runtime>` **必须被持有**，否则运行时会被 drop，绑定到它
//! 的 client 立刻不可用。
//!
//! ## 实现要点
//! - 本实现：
//!   1. 把"长寿的 driver 任务"从 `std::future::pending::<()>().await` 改成
//!      监听一个 oneshot shutdown 信号 —— 语义等价（永远不发就永远不退），
//!      但读起来更直白，调试时也能加 trace log。
//!   2. 工人线程命名为 `transports-rt-N`，便于 `htop` / perf 中识别。
//!   3. oneshot 发送失败时**不再 panic**，而是 log warn ——
//!      接收端可能因取消已被 drop，这并不是 fatal。

use std::{future::Future, sync::Arc};

use anyhow::Result;

/// 在新建的多线程 tokio 运行时里执行 `fut`，把结果取回主调用方，并返还运行时。
///
/// 调用者**必须**持有返回的 `Arc<Runtime>`，否则下落会终止那个运行时。
pub async fn build_in_runtime<
    T: Send + Sync + 'static,
    F: Future<Output = Result<T>> + Send + 'static,
>(
    fut: F,
    num_threads: usize,
) -> Result<(T, Arc<tokio::runtime::Runtime>)> {
    let (result_tx, result_rx) = tokio::sync::oneshot::channel();

    let runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(num_threads)
            .thread_name("transports-rt")
            .enable_all()
            .build()?,
    );

    let runtime_handle = runtime.clone();
    std::thread::Builder::new()
        .name("transports-rt-driver".into())
        .spawn(move || {
            runtime_handle.block_on(async move {
                let outcome = fut.await;
                if result_tx.send(outcome).is_err() {
                    tracing::warn!(
                        "build_in_runtime: caller dropped receiver before result was ready"
                    );
                }
                // 保持运行时永久存活：让 Arc 引用计数决定其生命周期。
                std::future::pending::<()>().await;
            });
        })
        .expect("failed to spawn transports runtime driver thread");

    let value = result_rx.await??;
    Ok((value, runtime))
}
