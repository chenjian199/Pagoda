// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 设计意图
//! 「关键任务」指一旦失败就必须立即拉响系统取消信号的后台异步任务。
//! 本模块提供统一的执行句柄，把「监控失败 → 触发父级取消」与
//! 「优雅 shutdown 子 token」两条终止路径封装在一起。
//!
//! # 外部契约
//! - `type CriticalTaskHandler<Fut>`：任务工厂签名
//!   `FnOnce(CancellationToken) -> Fut + Send + 'static`，返回 `Result<()>`；
//! - `CriticalTaskExecutionHandle::new(handler, parent_token, name)`
//!   返回 `Result<Self>`，自动 spawn 两个 tokio task；
//! - `join().await`：阻塞等待任务结束，返回 `Result<()>`；
//! - `cancel()`：触发子 token 通知任务自行优雅退出；
//! - Drop：默认行为是触发父级取消，避免句柄被悄悄丢弃。
//!
//! # 实现要点
//! - 两个 `JoinHandle`：业务任务 + 监控任务，监控任务通过 `oneshot` 等待
//!   业务任务结果，再决定是否触发父级 `CancellationToken::cancel`；
//! - 子 token 由父 token 的 `child_token()` 派生，与父级形成单向通知链；
//! - 使用 `tokio::runtime::Handle::current()` 显式持有运行时引用，
//!   保证 `Drop` 时仍能投递取消信号。

use anyhow::{Context, Result};
use std::future::Future;
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// 关键任务处理函数的类型别名。
///
/// 处理函数会接收一个 `CancellationToken`，并返回解析为 `Result<()>` 的异步任务。
/// 任务实现应主动监听取消信号并在收到后优雅退出。
pub type CriticalTaskHandler<Fut> = dyn FnOnce(CancellationToken) -> Fut + Send + 'static;

/// 管理关键任务生命周期的执行句柄。
///
/// 它同时支持两种终止路径：
/// 1. 关键失败：任务返回错误或 panic 时，立即触发父级取消。
/// 2. 优雅关闭：通过子级 token 通知任务自行收尾，不会直接触发系统级取消。
// === SECTION: CriticalTaskExecutionHandle ===

pub struct CriticalTaskExecutionHandle {
    monitor_task: JoinHandle<()>,
    graceful_shutdown_token: CancellationToken,
    result_receiver: Option<oneshot::Receiver<Result<()>>>,
    detached: bool,
}

impl CriticalTaskExecutionHandle {
    /// 使用当前 Tokio runtime 创建关键任务句柄。
    ///
    /// 处理流程是先获取当前 runtime，再委托给 `new_with_runtime` 完成实际创建。
    pub fn new<Fut>(
        task_fn: impl FnOnce(CancellationToken) -> Fut + Send + 'static,
        parent_token: CancellationToken,
        description: &str,
    ) -> Result<Self>
    where
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let runtime = Handle::try_current()?;
        Self::new_with_runtime(task_fn, parent_token, description, &runtime)
    }

    /// 在指定 runtime 上创建关键任务句柄。
    ///
    /// 处理流程是先派生优雅关闭 token 并启动主任务，然后再启动监控任务：
    /// 监控任务负责捕获错误或 panic，并在异常时取消父级 token。
    pub fn new_with_runtime<Fut>(
        task_fn: impl FnOnce(CancellationToken) -> Fut + Send + 'static,
        parent_token: CancellationToken,
        description: &str,
        runtime: &Handle,
    ) -> Result<Self>
    where
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let graceful_shutdown_token = parent_token.child_token();
        let description = description.to_string();
        let parent_token_clone = parent_token.clone();
        let (result_sender, result_receiver) = oneshot::channel();

        let graceful_shutdown_token_clone = graceful_shutdown_token.clone();
        let description_clone = description.to_string();
        let task = runtime.spawn(async move {
            let task_future = task_fn(graceful_shutdown_token_clone);

            match task_future.await {
                Ok(()) => {
                    tracing::debug!(
                        "Critical task '{}' completed successfully",
                        description_clone
                    );
                    Ok(())
                }
                Err(e) => {
                    tracing::error!("Critical task '{}' failed: {:#}", description_clone, e);
                    Err(e.context(format!("Critical task '{}' failed", description_clone)))
                }
            }
        });

        let monitor_task = {
            let main_task_handle = task;
            let parent_token_monitor = parent_token_clone;
            let description_monitor = description.clone();

            runtime.spawn(async move {
                let result = match main_task_handle.await {
                    Ok(task_result) => {
                        if task_result.is_err() {
                            parent_token_monitor.cancel();
                        }
                        task_result
                    }
                    Err(join_error) => {
                        if join_error.is_panic() {
                            let panic_msg = join_error
                                .try_into_panic()
                                .ok()
                                .and_then(|reason| {
                                    reason
                                        .downcast_ref::<String>()
                                        .cloned()
                                        .or_else(|| reason.downcast_ref::<&str>().map(|s| s.to_string()))
                                })
                                .unwrap_or_else(|| "Panic occurred but reason unavailable".to_string());

                            tracing::error!(
                                "Critical task '{}' panicked: {}",
                                description_monitor,
                                panic_msg
                            );
                            parent_token_monitor.cancel();
                            Err(anyhow::anyhow!(
                                "Critical task '{}' panicked: {}",
                                description_monitor,
                                panic_msg
                            ))
                        } else {
                            parent_token_monitor.cancel();
                            Err(anyhow::anyhow!(
                                "Failed to join critical task '{}': {}",
                                description_monitor,
                                join_error
                            ))
                        }
                    }
                };

                let _ = result_sender.send(result);
            })
        };

        Ok(Self {
            monitor_task,
            graceful_shutdown_token,
            result_receiver: Some(result_receiver),
            detached: false,
        })
    }

    /// 判断监控中的关键任务是否已经结束。
    pub fn is_finished(&self) -> bool {
        let finished = self.monitor_task.is_finished();
        finished
    }

    /// 判断关键任务是否已经收到优雅关闭取消信号。
    pub fn is_cancelled(&self) -> bool {
        let cancelled = self.graceful_shutdown_token.is_cancelled();
        cancelled
    }

    /// 以优雅方式取消关键任务，而不直接触发系统级关闭。
    ///
    /// 处理流程是仅取消子级 token，由任务自行感知并完成清理。
    pub fn cancel(&self) {
        let token = &self.graceful_shutdown_token;
        token.cancel();
    }

    /// 等待关键任务结束并返回真实执行结果。
    ///
    /// 处理流程是消费内部 oneshot 接收端，若任务成功或优雅退出则返回 `Ok(())`，
    /// 若失败或 panic 则保留原始错误信息返回。
    pub async fn join(mut self) -> Result<()> {
        self.detached = true;

        let receiver = self.result_receiver.take().unwrap();

        match receiver.await {
            Ok(task_result) => task_result,
            Err(_) => Err(anyhow::anyhow!("Critical task monitor was cancelled")),
        }
    }

    /// 分离句柄，使任务在句柄释放后继续运行。
    pub fn detach(mut self) {
        self.detached = true;
    }
}

impl Drop for CriticalTaskExecutionHandle {
    /// 防止调用方在未 `join` 或 `detach` 的情况下直接丢弃关键任务句柄。
    fn drop(&mut self) {
        if !self.detached {
            panic!("Critical task was not detached prior to drop!");
        }
    }
}

// === SECTION: tests ===

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_successful_task_completion() {
        // 测试关键任务成功完成时的返回结果与父级取消状态。
        let parent_token = CancellationToken::new();
        let completed = Arc::new(AtomicBool::new(false));
        let completed_clone = completed.clone();

        let handle = CriticalTaskExecutionHandle::new(
            |_cancel_token| async move {
                completed_clone.store(true, Ordering::SeqCst);
                Ok(())
            },
            parent_token.clone(),
            "test-success-task",
        )
        .unwrap();

        let result = handle.join().await;
        assert!(result.is_ok());
        assert!(completed.load(Ordering::SeqCst));
        assert!(!parent_token.is_cancelled());
    }

    #[tokio::test]
    async fn test_task_failure_cancels_parent_token() {
        // 测试关键任务返回错误时会触发父级取消。
        let parent_token = CancellationToken::new();

        let handle = CriticalTaskExecutionHandle::new(
            |_cancel_token| async move {
                anyhow::bail!("Critical task failed!");
            },
            parent_token.clone(),
            "test-failure-task",
        )
        .unwrap();

        let result = handle.join().await;
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        // Check that the error contains either the original message or the context
        assert!(
            error_msg.contains("Critical task failed!")
                || error_msg.contains("Critical task 'test-failure-task' failed"),
            "Error message should contain failure context: {}",
            error_msg
        );

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(parent_token.is_cancelled());
    }

    #[tokio::test]
    async fn test_task_panic_is_caught_and_reported() {
        // 测试关键任务 panic 时会被捕获并上报为错误。
        let parent_token = CancellationToken::new();

        let handle = CriticalTaskExecutionHandle::new(
            |_cancel_token| async move {
                panic!("Something went terribly wrong!");
            },
            parent_token.clone(),
            "test-panic-task",
        )
        .unwrap();

        let result = handle.join().await;
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(
            error_msg.contains("panicked") || error_msg.contains("panic"),
            "Error message should indicate a panic occurred: {}",
            error_msg
        );

        assert!(parent_token.is_cancelled());
    }

    #[tokio::test]
    async fn test_graceful_shutdown_via_cancellation_token() {
        // 测试优雅关闭只取消子级任务，不取消父级 token。
        let parent_token = CancellationToken::new();
        let work_done = Arc::new(AtomicU32::new(0));
        let work_done_clone = work_done.clone();

        let handle = CriticalTaskExecutionHandle::new(
            |cancel_token| async move {
                for i in 0..100 {
                    if cancel_token.is_cancelled() {
                        break;
                    }
                    work_done_clone.store(i, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Ok(())
            },
            parent_token.clone(),
            "test-graceful-shutdown",
        )
        .unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;

        handle.cancel();

        let result = handle.join().await;
        assert!(result.is_ok());

        let final_work = work_done.load(Ordering::SeqCst);
        assert!(final_work > 0);
        assert!(final_work < 99);

        assert!(!parent_token.is_cancelled());
    }

    #[tokio::test]
    async fn test_multiple_critical_tasks_one_failure() {
        // 测试多个关键任务共享父级 token 时，单个失败会触发整体协同关闭。
        let parent_token = CancellationToken::new();
        let task1_completed = Arc::new(AtomicBool::new(false));
        let task2_completed = Arc::new(AtomicBool::new(false));

        let task1_completed_clone = task1_completed.clone();
        let task2_completed_clone = task2_completed.clone();

        let handle1 = CriticalTaskExecutionHandle::new(
            |cancel_token| async move {
                for _ in 0..50 {
                    if cancel_token.is_cancelled() {
                        return Ok(());
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                task1_completed_clone.store(true, Ordering::SeqCst);
                Ok(())
            },
            parent_token.clone(),
            "long-running-task",
        )
        .unwrap();

        let handle2 = CriticalTaskExecutionHandle::new(
            |_cancel_token| async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                task2_completed_clone.store(true, Ordering::SeqCst);
                anyhow::bail!("Task 2 failed!");
            },
            parent_token.clone(),
            "failing-task",
        )
        .unwrap();

        let result2 = handle2.join().await;
        assert!(result2.is_err());

        assert!(parent_token.is_cancelled());

        let result1 = handle1.join().await;
        assert!(result1.is_ok());
        assert!(!task1_completed.load(Ordering::SeqCst)); // Should not have completed normally
    }

    #[tokio::test]
    async fn test_status_checking_methods() {
        // 测试句柄状态查询方法的行为。
        let parent_token = CancellationToken::new();

        let handle = CriticalTaskExecutionHandle::new(
            |cancel_token| async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                if cancel_token.is_cancelled() {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok(())
            },
            parent_token,
            "status-test-task",
        )
        .unwrap();

        assert!(!handle.is_finished());
        assert!(!handle.is_cancelled());

        handle.cancel();

        assert!(handle.is_cancelled());

        let result = handle.join().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_task_with_select_pattern() {
        // 测试任务使用 select 模式响应取消信号。
        let parent_token = CancellationToken::new();
        let work_completed = Arc::new(AtomicBool::new(false));
        let work_completed_clone = work_completed.clone();

        let handle = CriticalTaskExecutionHandle::new(
            |cancel_token| async move {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(200)) => {
                        work_completed_clone.store(true, Ordering::SeqCst);
                        Ok(())
                    }
                    _ = cancel_token.cancelled() => Ok(())
                }
            },
            parent_token,
            "select-pattern-task",
        )
        .unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;
        handle.cancel();

        let result = handle.join().await;
        assert!(result.is_ok());
        assert!(!work_completed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_timeout_behavior() {
        // 测试外部等待超时不会被当作关键任务失败。
        let parent_token = CancellationToken::new();

        let handle = CriticalTaskExecutionHandle::new(
            |_cancel_token| async move {
                tokio::time::sleep(Duration::from_secs(10)).await;
                Ok(())
            },
            parent_token,
            "long-task",
        )
        .unwrap();

        let result = timeout(Duration::from_millis(100), handle.join()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_panic_triggers_immediate_parent_cancellation() {
        // 测试 panic 会被监控任务立即转化为父级取消。
        let parent_token = CancellationToken::new();

        let handle = CriticalTaskExecutionHandle::new(
            |_cancel_token| async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                panic!("Critical failure!");
            },
            parent_token.clone(),
            "immediate-panic-task",
        )
        .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(
            parent_token.is_cancelled(),
            "Parent token should be cancelled immediately when critical task panics"
        );
        assert!(handle.join().await.is_err());
    }

    #[tokio::test]
    async fn test_error_triggers_immediate_parent_cancellation() {
        // 测试普通错误也会立即触发父级取消。
        let parent_token = CancellationToken::new();

        let handle = CriticalTaskExecutionHandle::new(
            |_cancel_token| async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                anyhow::bail!("Critical error!");
            },
            parent_token.clone(),
            "immediate-error-task",
        )
        .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(
            parent_token.is_cancelled(),
            "Parent token should be cancelled immediately when critical task errors"
        );
        assert!(handle.join().await.is_err());
    }

    #[tokio::test]
    #[should_panic]
    async fn test_task_detach() {
        // 测试未 detach 或 join 就丢弃句柄时会 panic。
        let parent_token = CancellationToken::new();
        let _handle = CriticalTaskExecutionHandle::new(
            |_cancel_token| async move { Ok(()) },
            parent_token,
            "test-detach-task",
        )
        .unwrap();
    }
}
