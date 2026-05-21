// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 单 task 工具函数。

use std::future::Future;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Spawn 一个可取消的任务。
pub fn spawn_cancellable<F>(
    token: CancellationToken,
    future: F,
) -> JoinHandle<Option<F::Output>>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(async move {
        tokio::select! {
            result = future => Some(result),
            () = token.cancelled() => None,
        }
    })
}

/// Spawn 一个任务并在完成后自动取消 token。
pub fn spawn_linked<F>(
    token: CancellationToken,
    future: F,
) -> JoinHandle<()>
where
    F: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    tokio::spawn(async move {
        tokio::select! {
            result = future => {
                if let Err(e) = result {
                    tracing::error!("Linked task failed: {e:#}");
                }
                token.cancel();
            }
            () = token.cancelled() => {}
        }
    })
}
