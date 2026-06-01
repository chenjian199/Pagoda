// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 设计意图
//! 在 `Runtime::shutdown` 关停流程中，提供一个轻量级「活跃端点引用计数 +
//! 完成事件」组合，保证关停阶段能阻塞等待所有登记的任务自然收尾。
//!
//! # 外部契约
//! - `GracefulShutdownTracker`：活跃端点计数器，暴露
//!   `register` / `notify_one` / `wait_for_completion` 等核心方法；
//! - `GracefulTaskGuard`：登记句柄，Drop 时自动注销，方便配合 `?` 控制流；
//! - 关停语义：`active_portnames == 0` 时唤醒所有等待者，且重复唤醒幂等。
//!
//! # 实现要点
//! - 计数使用 `AtomicUsize` + `Ordering::AcqRel`，避免显式锁；
//! - 完成事件依赖 `tokio::sync::Notify::notify_waiters`，唤醒所有等待者；
//! - `GracefulTaskGuard` 持有 `Arc<...>` 副本，在 Drop 中调用 `notify_one`，
//!   不需要业务代码主动释放。

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

// === SECTION: GracefulShutdownTracker ===

/// 跟踪优雅关闭阶段仍在运行的端点数量。
pub struct GracefulShutdownTracker {
    active_portnames: AtomicUsize,
    shutdown_complete: Notify,
}

/// 保存一次优雅关闭登记的 RAII 句柄。
///
/// 释放该句柄时会自动注销登记。它主要用于长生命周期的关闭编排逻辑，
/// 确保 `Runtime::shutdown` 在进入下一阶段前，能够等待这些任务完成清理。
pub struct GracefulTaskGuard {
    tracker: Arc<GracefulShutdownTracker>,
}

impl Drop for GracefulTaskGuard {
    /// 在句柄销毁时自动注销一个活跃端点计数。
    fn drop(&mut self) {
        let tracker = Arc::clone(&self.tracker);
        tracker.unregister_portname();
    }
}

impl std::fmt::Debug for GracefulShutdownTracker {
    /// 输出当前活跃端点数量，便于排查关闭等待状态。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let active_portnames = self.active_portnames.load(Ordering::SeqCst);
        let mut debug = f.debug_struct("GracefulShutdownTracker");
        debug.field("active_portnames", &active_portnames);
        debug.finish()
    }
}

impl GracefulShutdownTracker {
    /// 创建新的优雅关闭跟踪器。
    ///
    /// 处理流程是初始化活跃计数为 0，并创建一个用于通知等待方的 `Notify`。
    pub(crate) fn new() -> Self {
        let active_portnames = AtomicUsize::new(0);
        let shutdown_complete = Notify::new();

        Self {
            active_portnames,
            shutdown_complete,
        }
    }

    /// 注册一个参与优雅关闭等待的任务，并返回对应守卫。
    ///
    /// 处理流程是先增加活跃计数，再返回 `GracefulTaskGuard`，后续通过 guard 的 drop 自动回收。
    pub fn register_task(self: &Arc<Self>) -> GracefulTaskGuard {
        self.register_portname();
        let tracker = Arc::clone(self);

        GracefulTaskGuard { tracker }
    }

    /// 手动登记一个活跃端点。
    pub(crate) fn register_portname(&self) {
        let previous = self.active_portnames.fetch_add(1, Ordering::SeqCst);
        tracing::debug!(
            "PortName registered, total active: {} -> {}",
            previous,
            previous + 1
        );
    }

    /// 手动注销一个活跃端点，并在最后一个端点退出时唤醒等待者。
    pub(crate) fn unregister_portname(&self) {
        let previous = self.active_portnames.fetch_sub(1, Ordering::SeqCst);
        tracing::debug!(
            "PortName unregistered, remaining active: {} -> {}",
            previous,
            previous - 1
        );

        if previous == 1 {
            tracing::info!("Last portname completed, notifying all waiters");
            self.shutdown_complete.notify_waiters();
        }
    }

    /// 读取当前活跃端点数量。
    pub(crate) fn get_count(&self) -> usize {
        let active = self.active_portnames.load(Ordering::Acquire);
        active
    }

    /// 等待所有已登记端点完成。
    ///
    /// 处理流程是循环检查计数；若仍有活跃端点则挂起等待通知，收到通知后再次检查。
    pub(crate) async fn wait_for_completion(&self) {
        loop {
            let notified = self.shutdown_complete.notified();
            let active = self.get_count();
            tracing::trace!("Checking completion status, active portnames: {active}");

            if active == 0 {
                tracing::debug!("All portnames completed");
                return;
            }

            tracing::debug!("Waiting for {active} portnames to complete");
            notified.await;
            tracing::trace!("Received notification, rechecking...");
        }
    }

    // 这里不再额外提供访问器，调用方可直接持有 tracker。
}

// === SECTION: tests ===

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration, timeout};

    #[test]
    fn test_register_task_guard_updates_count_and_unregisters_on_drop() {
        // 测试任务守卫在创建和释放时是否正确维护计数。
        let tracker = Arc::new(GracefulShutdownTracker::new());

        let guard = tracker.register_task();
        assert_eq!(tracker.get_count(), 1);
        assert_eq!(format!("{:?}", tracker), "GracefulShutdownTracker { active_portnames: 1 }");

        drop(guard);
        assert_eq!(tracker.get_count(), 0);
    }

    #[test]
    fn test_manual_register_and_unregister_portname() {
        // 测试手动注册与注销端点的计数变化。
        let tracker = GracefulShutdownTracker::new();

        tracker.register_portname();
        tracker.register_portname();
        assert_eq!(tracker.get_count(), 2);

        tracker.unregister_portname();
        assert_eq!(tracker.get_count(), 1);

        tracker.unregister_portname();
        assert_eq!(tracker.get_count(), 0);
    }

    #[tokio::test]
    async fn test_wait_for_completion_returns_immediately_when_empty() {
        // 测试无活跃端点时等待会立即返回。
        let tracker = GracefulShutdownTracker::new();

        timeout(Duration::from_millis(50), tracker.wait_for_completion())
            .await
            .expect("wait_for_completion should not block when there are no active portnames");
    }

    #[tokio::test]
    async fn test_wait_for_completion_waits_until_last_guard_dropped() {
        // 测试等待逻辑会阻塞直到最后一个守卫释放。
        let tracker = Arc::new(GracefulShutdownTracker::new());
        let guard_a = tracker.register_task();
        let guard_b = tracker.register_task();
        let tracker_for_wait = Arc::clone(&tracker);

        let wait_task = tokio::spawn(async move {
            tracker_for_wait.wait_for_completion().await;
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!wait_task.is_finished());

        drop(guard_a);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!wait_task.is_finished());

        drop(guard_b);

        timeout(Duration::from_millis(100), wait_task)
            .await
            .expect("waiter should complete after the final guard is dropped")
            .unwrap();
    }
}
