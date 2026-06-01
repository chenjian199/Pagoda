// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `compute::thread_local` —— 计算上下文的线程局部存储
//!
//! ## 设计意图
//!
//! `compute_medium!` / `compute_large!` 这两个宏在被调用时，并不知道
//! 当前所在的 Tokio worker 是否绑定了某个 `ComputePool`、也不知道有
//! 没有 `block_in_place` permit 可用。直接把 `&Runtime` 或
//! `&ComputePool` 作为参数透传给每个 async 函数代价过高，且会污染
//! 函数签名。
//!
//! 本模块在每个 Tokio worker 线程上挂一个 `RefCell<Option<ComputeContext>>`，
//! 让宏 / 上层代码可以**无参**取到当前线程绑定的计算资源——
//! 真正的上下文初始化由运行时启动时通过 `on_thread_start` 钩子完成。
//!
//! ## 外部契约
//!
//! - `pub struct ComputeContext { pub pool, pub block_in_place_permits }`；
//! - `pub fn initialize_context(pool, permits)`；
//! - `pub fn with_context<F, R>(f) -> Option<R>`；
//! - `pub fn try_acquire_block_permit() -> Result<OwnedSemaphorePermit, &'static str>`；
//! - `pub fn get_pool() -> Option<Arc<ComputePool>>`；
//! - `pub fn has_compute_context() -> bool`；
//! - `pub fn assert_compute_context()`（无上下文则 panic）。
//!
//! 上述项被 `compute/macros.rs` 与 runtime 启动路径直接引用，签名 /
//! 字段 / 错误字符串均保持不变。

use std::cell::RefCell;
use std::sync::Arc;

use tokio::sync::Semaphore;

use super::ComputePool;

// ============================================================================
// 线程局部状态
// ============================================================================

thread_local! {
    /// 当前线程上挂载的计算上下文。`const`-init 让首次访问无锁、零分配。
    ///
    /// 在非 Tokio worker 线程上访问该 cell 是合法的，只是值始终为
    /// `None`——`has_compute_context()` 会返回 false，宏会走 fallback
    /// 路径。
    static COMPUTE_CONTEXT: RefCell<Option<ComputeContext>> = const { RefCell::new(None) };
}

// ============================================================================
// 公开类型：ComputeContext
// ============================================================================

/// 单个 Tokio worker 线程能访问到的"计算资源套件"。
///
/// 两个字段都是 `Arc`，因为可能被宏 / 上层代码"借出去"再 drop——必
/// 须独立于 thread-local 生命周期之外。
#[derive(Clone)]
pub struct ComputeContext {
    /// 当前线程关联的 Rayon 池。
    pub pool: Arc<ComputePool>,
    /// `block_in_place` 用的 permit 信号量。当池已满时，宏会自动
    /// 退化为 offload 而不是阻塞。
    pub block_in_place_permits: Arc<Semaphore>,
}

// ============================================================================
// 公开 API：初始化 / 借用 / 查询
// ============================================================================

/// 把 `(pool, permits)` 安装到当前线程的 thread-local slot。
///
/// 调用时机：Tokio runtime 通过 `on_thread_start` 钩子在每个 worker
/// 线程启动时调用一次。重复调用会**覆盖**旧上下文（一般不应发生）。
pub fn initialize_context(pool: Arc<ComputePool>, permits: Arc<Semaphore>) {
    COMPUTE_CONTEXT.with(|cell| {
        *cell.borrow_mut() = Some(ComputeContext {
            pool,
            block_in_place_permits: permits,
        });
    });
}

/// 借出当前线程的上下文供闭包使用。
///
/// ## 出参
///
/// - `Some(R)`：当前线程已初始化上下文，`f` 已被调用；
/// - `None`：当前线程没有上下文（非 worker 线程或尚未初始化）。
///
/// `f` 拿到的是不可变借用，请尽量短时间持有，以免阻塞嵌套调用。
pub fn with_context<F, R>(f: F) -> Option<R>
where
    F: FnOnce(&ComputeContext) -> R,
{
    COMPUTE_CONTEXT.with(|cell| cell.borrow().as_ref().map(f))
}

/// 尝试从当前线程的上下文中拿一张 `block_in_place` permit。
///
/// 返回 `Ok(permit)` 时，调用方可以安全地用 `tokio::task::block_in_place`
/// 包裹耗时操作；permit 被 drop 时计数自动归还。
///
/// 失败原因有两种，**错误字符串保持原貌**——它们被 `compute_medium!`
/// 宏的判定路径间接依赖：
///
/// - `"No permits available"`：上下文存在但信号量已枯竭；
/// - `"No compute context on this thread"`：当前线程根本没有上下文。
pub fn try_acquire_block_permit() -> Result<tokio::sync::OwnedSemaphorePermit, &'static str> {
    let inner = with_context(|ctx| {
        ctx.block_in_place_permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| "No permits available")
    });
    match inner {
        Some(res) => res,
        None => Err("No compute context on this thread"),
    }
}

/// 拿到当前线程绑定的 `ComputePool` 的 Arc clone。
///
/// 返回 `None` 表示当前线程没有上下文。
pub fn get_pool() -> Option<Arc<ComputePool>> {
    with_context(|ctx| ctx.pool.clone())
}

/// 当前线程是否已经初始化计算上下文。
///
/// 等价于 `with_context(|_| ()).is_some()`，但可读性更好。宏的
/// fallback 决策会用到。
pub fn has_compute_context() -> bool {
    with_context(|_| ()).is_some()
}

/// 强制要求当前线程必须已经初始化计算上下文，否则 panic。
///
/// 用法：在那些"必须 offload，不允许 fallback inline"的关键路径开
/// 头调用一次。panic 信息中包含初始化方法名，便于排查。
pub fn assert_compute_context() {
    if !has_compute_context() {
        panic!(
            "Thread-local compute context not initialized! \
             Compute macros will fall back to inline execution. \
             Call Runtime::initialize_thread_local() on worker threads."
        );
    }
}

// ============================================================================
// 单元测试
//
// `thread_local!` 状态在不同测试线程间不共享，但 `cargo test` 默认会
// 复用同一线程跑多个测试——我们在每个测试结尾显式把 slot 置回 None，
// 避免污染同线程上的下一个测试。
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// 把当前线程的上下文清空，作为测试后置 / 前置守卫。
    fn clear_context() {
        COMPUTE_CONTEXT.with(|cell| {
            *cell.borrow_mut() = None;
        });
    }

    /// ## 测试过程
    /// 未初始化时调用 `get_pool` / `with_context` / `try_acquire_block_permit`，
    /// 应分别返回 `None` / `None` / `Err`。
    ///
    /// ## 意义
    /// 锁定"无上下文时一切查询都是无副作用的"这一安全契约。
    #[test]
    fn uninitialized_context_returns_none() {
        clear_context();
        assert!(!has_compute_context());
        assert!(get_pool().is_none());
        assert!(with_context(|_| ()).is_none());
        let err = try_acquire_block_permit().unwrap_err();
        assert_eq!(err, "No compute context on this thread");
    }

    /// ## 测试过程
    /// 未初始化时调 `assert_compute_context`，应 panic。
    #[test]
    #[should_panic(expected = "Thread-local compute context not initialized")]
    fn assert_compute_context_panics_when_uninitialized() {
        clear_context();
        assert_compute_context();
    }

    /// ## 测试过程
    /// 1. 调 `initialize_context` 注入 pool + 2 张 permit；
    /// 2. 断言 `has_compute_context` / `get_pool` / `with_context` 都返回有效值；
    /// 3. 清理上下文以免污染同线程后续测试。
    #[test]
    fn initialized_context_is_reachable() {
        let pool = Arc::new(ComputePool::with_defaults().unwrap());
        let permits = Arc::new(Semaphore::new(2));
        initialize_context(pool.clone(), permits.clone());

        assert!(has_compute_context());
        assert!(get_pool().is_some());
        assert!(with_context(|ctx| ctx.pool.num_threads()).is_some());

        clear_context();
    }

    /// ## 测试过程
    /// 上下文存在且 permit 充足时，连续两次 `try_acquire_block_permit`
    /// 都应 Ok。
    #[tokio::test(flavor = "current_thread")]
    async fn block_permit_can_be_acquired_when_available() {
        let pool = Arc::new(ComputePool::with_defaults().unwrap());
        let permits = Arc::new(Semaphore::new(2));
        initialize_context(pool, permits);

        let p1 = try_acquire_block_permit();
        let p2 = try_acquire_block_permit();
        assert!(p1.is_ok());
        assert!(p2.is_ok());

        clear_context();
    }

    /// ## 测试过程
    /// 信号量容量为 0 时，应返回 `Err("No permits available")`——这是
    /// 宏 fallback 路径依赖的字符串。
    #[tokio::test(flavor = "current_thread")]
    async fn block_permit_returns_specific_err_when_exhausted() {
        let pool = Arc::new(ComputePool::with_defaults().unwrap());
        let permits = Arc::new(Semaphore::new(0));
        initialize_context(pool, permits);

        let err = try_acquire_block_permit().unwrap_err();
        assert_eq!(err, "No permits available");

        clear_context();
    }

    /// ## 测试过程
    /// 先初始化，再清理，再查询：应回到"无上下文"状态。
    ///
    /// ## 意义
    /// 锁定"清理后线程恢复纯净"这一回归契约，避免悄悄遗留旧 Arc。
    #[test]
    fn clear_context_resets_state() {
        let pool = Arc::new(ComputePool::with_defaults().unwrap());
        let permits = Arc::new(Semaphore::new(1));
        initialize_context(pool, permits);
        assert!(has_compute_context());

        clear_context();
        assert!(!has_compute_context());
        assert!(get_pool().is_none());
    }

    // ------------------------------------------------------------------
    // === lib 标准契约测试 ============================
    // ------------------------------------------------------------------

    #[test]
    fn test_uninitialized_context() {
        clear_context();
        // Should return None when context not initialized
        assert!(get_pool().is_none());
        assert!(try_acquire_block_permit().is_err());
        assert!(!has_compute_context());
    }

    #[test]
    #[should_panic(expected = "Thread-local compute context not initialized")]
    fn test_assert_compute_context_panics() {
        clear_context();
        // Should panic when context not initialized
        assert_compute_context();
    }
}
