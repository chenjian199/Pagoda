// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 可执行任务句柄 trait。
//!
//! ## 设计意图
//! 提供一个面向底层任务句柄的最小 trait [`ExecutionHandle`]，用于抽象“可被取消、
//! 可被查询是否完成 / 取消、能返回底层 `JoinHandle` 的任务”，避免上层代码
//! 直接被 `tokio::task::JoinHandle` 与 `CancellationToken` 坐死。
//!
//! ## 外部契约
//! - 公开 trait `ExecutionHandle`。方法集、签名与返回类型保持不变：
//!   * `is_finished(&self) -> bool`
//!   * `is_cancelled(&self) -> bool`
//!   * `cancel(&self)`
//!   * `cancellation_token(&self) -> CancellationToken`
//!   * `handle(self) -> JoinHandle<Result<()>>`
//! - trait 仍是 `#[async_trait]` 修饰，且**不**增加默认方法，以保持对现有实现者不产生破坏性变更。
//! - 重导出 `anyhow::Error / Result`、`async_trait::async_trait`、`tokio::task::JoinHandle`、
//!   `tokio_util::sync::CancellationToken`。
//!
//! ## 实现要点
//! 本文件仅定义 trait 以及一组公开的重导出；单元测试通过一个 `MockHandle` 结构体
//! 验证 trait 的可对象安全性（`Box<dyn ExecutionHandle>`）与取消令牌语义在克隆间的共享。

use std::{
    pin::Pin,
    task::{Context, Poll},
};

pub use anyhow::{Error, Result};
pub use async_trait::async_trait;
pub use tokio::task::JoinHandle;
pub use tokio_util::sync::CancellationToken;

// === SECTION: trait 声明 ===

#[async_trait]
pub trait ExecutionHandle {
    fn is_finished(&self) -> bool;
    fn is_cancelled(&self) -> bool;
    fn cancel(&self);
    fn cancellation_token(&self) -> CancellationToken;
    fn handle(self) -> JoinHandle<Result<()>>;
}

// === SECTION: 单元测试 ===

#[cfg(test)]
mod tests {
    //! ## 测试过程
    //! 以 `MockHandle`（`AtomicBool` + 真实 `CancellationToken` + 已就绪 `JoinHandle`）验证：
    //! 初始状态、预先标记完成、`cancel()` 语义、`cancellation_token()` 克隆共享状态、
    //! 逆向从克隆令牌取消仅会同步到原始句柄、`handle()` 消费 self 后返回 Ok(())、
    //! `cancel` 与 `is_finished` 状态互不干扰、以及 trait 可以通过 `Box<dyn ExecutionHandle>` 使用。
    //!
    //! ## 意义
    //! 这组用例钉定了 `ExecutionHandle` 的对外可观察行为。未来若调整默认方法 / 附加
    //! `Send + Sync` 等约束，需同时更新本测试集。

    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    // ── 具体 Mock 实现 ――――――――――――――――――――――――――――――――――――――――――――――――――――――――――
    //
    // MockHandle 用 AtomicBool 模拟“已完成”标志，配合真实
    // CancellationToken 验证取消语义。

    struct MockHandle {
        /// 模拟的“任务已完成”标志。
        finished: Arc<AtomicBool>,
        /// 真实的取消令牌，用于 cancel() / is_cancelled() 验证。
        token: CancellationToken,
        /// 内部 join 句柄（包装一个已预和解析的 Future）。
        join: JoinHandle<Result<()>>,
    }

    impl MockHandle {
        /// 创建一个立即以 Ok(()) 解析的句柄。
        fn ok() -> Self {
            let finished = Arc::new(AtomicBool::new(false));
            let token = CancellationToken::new();
            let join = tokio::task::spawn(async { Ok(()) });
            Self {
                finished,
                token,
                join,
            }
        }

        /// 创建一个预先标记为已完成的句柄。
        fn already_finished() -> Self {
            let h = Self::ok();
            h.finished.store(true, Ordering::SeqCst);
            h
        }
    }

    #[async_trait]
    impl ExecutionHandle for MockHandle {
        fn is_finished(&self) -> bool {
            self.finished.load(Ordering::SeqCst)
        }

        fn is_cancelled(&self) -> bool {
            self.token.is_cancelled()
        }

        fn cancel(&self) {
            self.token.cancel();
        }

        fn cancellation_token(&self) -> CancellationToken {
            self.token.clone()
        }

        fn handle(self) -> JoinHandle<Result<()>> {
            self.join
        }
    }

    // ── 测试：初始句柄既未完成也未取消 ―――――――――――――――――――――――――――――
    #[tokio::test]
    async fn new_handle_is_neither_finished_nor_cancelled() {
        let h = MockHandle::ok();
        assert!(!h.is_finished(), "全新句柄不应为已完成状态");
        assert!(!h.is_cancelled(), "全新句柄不应为已取消状态");
    }

    // ── 测试：预先完成的句柄返回 is_finished() == true ――――――――――――――――
    #[tokio::test]
    async fn pre_finished_handle_reports_finished() {
        let h = MockHandle::already_finished();
        assert!(h.is_finished());
        assert!(!h.is_cancelled(), "已完成 ≠ 已取消");
    }

    // ── 测试：cancel() 把 is_cancelled() 置为 true ―――――――――――――――――――――
    #[tokio::test]
    async fn cancel_marks_token_as_cancelled() {
        let h = MockHandle::ok();
        assert!(!h.is_cancelled());
        h.cancel();
        assert!(h.is_cancelled(), "调用 cancel() 后令牌应已取消");
    }

    // ── 测试：cancellation_token() 返回的克隆共享取消状态 ―――――――――――
    #[tokio::test]
    async fn cancellation_token_clone_shares_cancelled_state() {
        let h = MockHandle::ok();
        let cloned = h.cancellation_token();
        assert!(!cloned.is_cancelled());
        h.cancel();
        // 克隆令牌应反映取消操作
        assert!(cloned.is_cancelled(), "克隆令牌应能观察到取消");
    }

    // ── 测试：取消克隆令牌同样在原始令牌上生效 ―――――――――――――――――
    #[tokio::test]
    async fn cancelling_clone_cancels_original() {
        let h = MockHandle::ok();
        let cloned = h.cancellation_token();
        cloned.cancel();
        assert!(
            h.is_cancelled(),
            "取消克隆令牌应同时取消原始句柄的令牌"
        );
    }

    // ── 测试：handle() 消耗 self 后 join 解析为 Ok(()) ――――――――――――――――
    #[tokio::test]
    async fn handle_resolves_to_ok() {
        let h = MockHandle::ok();
        let join = h.handle();
        let result = join.await.expect("join 句柄不应 panic");
        assert!(result.is_ok(), "任务结果应为 Ok(())");
    }

    // ── 测试：cancel() 不影响 is_finished() ――――――――――――――――――――――――
    #[tokio::test]
    async fn cancel_does_not_set_finished_flag() {
        let h = MockHandle::ok();
        h.cancel();
        assert!(
            !h.is_finished(),
            "cancel() 不应设置 is_finished()，两者状态独立"
        );
    }

    // ── 测试：多次调用 cancellation_token() 返回的克隆共享状态 ――――――――
    #[tokio::test]
    async fn multiple_token_clones_are_independent_copies() {
        let h = MockHandle::ok();
        let t1 = h.cancellation_token();
        let t2 = h.cancellation_token();
        // 原始、t1、t2 共享相同的底层状态
        h.cancel();
        assert!(t1.is_cancelled());
        assert!(t2.is_cancelled());
    }

    // ── 测试：trait 可以透过 Box<dyn ExecutionHandle> 使用 ――――――――――――
    #[tokio::test]
    async fn trait_is_object_safe_via_box() {
        let boxed: Box<dyn ExecutionHandle> = Box::new(MockHandle::ok());
        assert!(!boxed.is_finished());
        assert!(!boxed.is_cancelled());
        boxed.cancel();
    }
}

