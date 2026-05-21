// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 优雅关闭追踪器：用 AtomicUsize 计数活跃端点，Notify 通知归零。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::Notify;

/// 优雅关闭追踪器。
///
/// 每次 `register_portname()` 计数 +1，`unregister_portname()` 计数 -1；
/// 归零时通过 `Notify` 唤醒 `wait_for_completion()` 的等待者。
pub struct GracefulShutdownTracker {
    active_portnames: AtomicUsize,
    shutdown_complete: Notify,
}

impl GracefulShutdownTracker {
    pub(crate) fn new() -> Self {
        Self {
            active_portnames: AtomicUsize::new(0),
            shutdown_complete: Notify::new(),
        }
    }

    /// 端点注册时加一。
    pub(crate) fn register_portname(&self) {
        self.active_portnames.fetch_add(1, Ordering::SeqCst);
    }

    /// 端点结束时减一；若从 1 变为 0，则唤醒等待者。
    pub(crate) fn unregister_portname(&self) {
        let prev = self.active_portnames.fetch_sub(1, Ordering::SeqCst);
        if prev == 1 {
            self.shutdown_complete.notify_waiters();
        }
    }

    /// 获取当前活动端点数。
    pub(crate) fn get_count(&self) -> usize {
        self.active_portnames.load(Ordering::SeqCst)
    }

    /// 等待所有活跃端点结束（计数归零）。
    ///
    /// 先创建 `notified()` 再检查计数，避免最后一个端点恰好在检查之后完成的竞态。
    pub(crate) async fn wait_for_completion(self: &Arc<Self>) {
        loop {
            let notified = self.shutdown_complete.notified();
            if self.active_portnames.load(Ordering::SeqCst) == 0 {
                break;
            }
            notified.await;
        }
    }
}

impl Default for GracefulShutdownTracker {
    fn default() -> Self {
        Self::new()
    }
}
