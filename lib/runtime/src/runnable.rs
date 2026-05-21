// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 任务执行句柄：封装 JoinHandle + CancellationToken。

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// 可取消任务的执行句柄。
pub struct ExecutionHandle {
    handle: JoinHandle<()>,
    token: CancellationToken,
}

impl ExecutionHandle {
    pub fn new(handle: JoinHandle<()>, token: CancellationToken) -> Self {
        Self { handle, token }
    }

    pub fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }

    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    pub fn cancel(&self) {
        self.token.cancel();
    }

    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.token
    }

    pub fn handle(&self) -> &JoinHandle<()> {
        &self.handle
    }

    pub fn into_handle(self) -> JoinHandle<()> {
        self.handle
    }
}
