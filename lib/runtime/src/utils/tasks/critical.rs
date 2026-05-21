// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 关键任务 spawn：任意 panic 或错误 → `std::process::exit(1)`。

use std::future::Future;

use tokio::task::JoinHandle;

/// Spawn 一个关键任务：如果 future 返回 Err 或 panic，进程立即退出。
///
/// 用于守护进程级不可恢复的后台任务（如 etcd 连接丢失后无法恢复）。
pub fn spawn_critical<F>(name: &'static str, future: F) -> JoinHandle<()>
where
    F: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    tokio::spawn(async move {
        match future.await {
            Ok(()) => {
                tracing::info!("Critical task '{name}' completed normally");
            }
            Err(err) => {
                tracing::error!("Critical task '{name}' failed: {err:#}");
                std::process::exit(1);
            }
        }
    })
}
