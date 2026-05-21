// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Tokio 线程本地计算上下文。
//!
//! 每个 Tokio worker 线程持有一个 [`ComputeContext`]，存储 [`ComputePool`] 引用和
//! `block_in_place` 许可 [`tokio::sync::Semaphore`]。
//!
//! ## 初始化流程
//!
//! `Runtime::initialize_all_thread_locals()` 通过 Barrier + `spawn_blocking`
//! 确保所有 worker 线程在处理请求前已完成 [`initialize_context`] 调用。
//!
//! ## block_in_place 许可
//!
//! [`ComputeContext::block_in_place_permits`] 控制 `compute_medium!` 宏的并发
//! `block_in_place` 数量，默认由 `Runtime` 按 `tokio_worker_threads / 2` 初始化，
//! 避免全部 worker 被阻塞。

use std::cell::RefCell;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::pool::ComputePool;

// ── thread-local 存储 ─────────────────────────────────────────────────────────

thread_local! {
    /// 当前线程绑定的计算上下文（私有）。
    ///
    /// 所有 `compute_medium!` / `compute_large!` 宏的 thread-local 路径依赖此上下文。
    static COMPUTE_CONTEXT: RefCell<Option<ComputeContext>> = const { RefCell::new(None) };
}

// ── ComputeContext ────────────────────────────────────────────────────────────

/// 线程本地计算上下文，存储当前线程的计算资源句柄。
///
/// `Clone` 为轻量 `Arc` 拷贝，复制开销低。
#[derive(Clone)]
pub struct ComputeContext {
    /// 当前线程关联的 Rayon 计算池。
    pub pool: Arc<ComputePool>,
    /// `block_in_place` 并发许可，控制 `compute_medium!` 的并发阻塞数量。
    ///
    /// 由 `Runtime` 按 `tokio_worker_threads / 2` 初始化，避免全部 worker 被阻塞。
    pub block_in_place_permits: Arc<Semaphore>,
}

// ── 公开函数 ──────────────────────────────────────────────────────────────────

/// 在当前线程设置计算上下文。
///
/// 由 `crate::runtime::Runtime::initialize_thread_local()` 在每个 Tokio worker
/// 线程启动时调用，外部代码仅在测试或特殊场景中直接使用。
pub fn initialize_context(pool: Arc<ComputePool>, permits: Arc<Semaphore>) {
    COMPUTE_CONTEXT.with(|ctx| {
        *ctx.borrow_mut() = Some(ComputeContext {
            pool,
            block_in_place_permits: permits,
        });
    });
}

/// 安全访问当前线程的 [`ComputeContext`]。
///
/// 若当前线程未初始化上下文，返回 `None`。
pub fn with_context<F, R>(f: F) -> Option<R>
where
    F: FnOnce(&ComputeContext) -> R,
{
    COMPUTE_CONTEXT.with(|ctx| ctx.borrow().as_ref().map(f))
}

/// 尝试从当前线程的上下文中获取一个 `block_in_place` 许可。
///
/// - 成功 → `Ok(OwnedSemaphorePermit)`，持有期间占用一个许可槽
/// - 无上下文 → `Err("no compute context on this thread")`
/// - 无可用许可 → `Err("no block_in_place permits available")`
pub fn try_acquire_block_permit() -> Result<OwnedSemaphorePermit, &'static str> {
    match with_context(|ctx| ctx.block_in_place_permits.clone().try_acquire_owned()) {
        Some(Ok(permit)) => Ok(permit),
        Some(Err(_)) => Err("no block_in_place permits available"),
        None => Err("no compute context on this thread"),
    }
}

/// 获取当前线程关联的 [`ComputePool`] 引用。
///
/// 若当前线程未初始化，返回 `None`。
pub fn get_pool() -> Option<Arc<ComputePool>> {
    with_context(|ctx| Arc::clone(&ctx.pool))
}

/// 检查当前线程是否已初始化计算上下文。
pub fn has_compute_context() -> bool {
    COMPUTE_CONTEXT.with(|ctx| ctx.borrow().is_some())
}

/// 断言当前线程已初始化计算上下文，否则 panic。
///
/// 适用于只能在 Tokio worker 线程中调用的代码路径。
pub fn assert_compute_context() {
    assert!(
        has_compute_context(),
        "当前线程未初始化 ComputeContext，\
         请确保在 Runtime::initialize_all_thread_locals() 之后调用"
    );
}

// ── 单元测试 ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compute::{ComputeConfig, ComputePool};

    fn make_pool() -> Arc<ComputePool> {
        let mut cfg = ComputeConfig::default();
        cfg.num_threads = Some(2);
        Arc::new(ComputePool::new(cfg).unwrap())
    }

    #[test]
    fn test_uninitialized_context() {
        // 未初始化线程上各函数的退化行为
        // 注意：线程本地存储在测试线程间可能共享，此处仅验证 None 路径
        // 实际 worker 线程隔离由 Runtime 的 Barrier 保证
        let pool = get_pool();
        // 如果当前测试线程已被其他测试初始化则跳过
        if pool.is_none() {
            assert!(!has_compute_context());
            assert!(try_acquire_block_permit().is_err());
        }
    }

    #[test]
    fn test_initialize_and_get_pool() {
        let pool = make_pool();
        let permits = Arc::new(Semaphore::new(4));
        initialize_context(Arc::clone(&pool), Arc::clone(&permits));

        assert!(has_compute_context());
        assert!(get_pool().is_some());
        assert_eq!(
            get_pool().unwrap().num_threads(),
            pool.num_threads()
        );
    }

    #[test]
    fn test_block_permit_acquire() {
        let pool = make_pool();
        let permits = Arc::new(Semaphore::new(2));
        initialize_context(pool, Arc::clone(&permits));

        let p1 = try_acquire_block_permit();
        assert!(p1.is_ok());
        let p2 = try_acquire_block_permit();
        assert!(p2.is_ok());
        // 许可耗尽
        let p3 = try_acquire_block_permit();
        assert!(p3.is_err());

        // 释放后可重新获取
        drop(p1);
        let p4 = try_acquire_block_permit();
        assert!(p4.is_ok());
    }

    #[test]
    #[should_panic(expected = "未初始化 ComputeContext")]
    fn test_assert_compute_context_panics() {
        // 使用新线程确保没有已初始化的上下文
        std::thread::spawn(|| {
            assert_compute_context();
        })
        .join()
        .unwrap();
    }
}
