// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `compute` —— CPU 密集型工作的统一入口
//!
//! ## 设计意图
//!
//! 这是 Pagoda runtime "计算" 子系统的根模块。整个子系统围绕一个
//! Rayon 线程池构建，目标是给 async 代码提供一条**清晰、可观测、可
//! 验证**的快路径去跑 CPU 密集型工作，避免污染 Tokio worker。
//!
//! 子模块分工：
//!
//! | 子模块               | 角色                                           |
//! |----------------------|------------------------------------------------|
//! | `macros`             | `compute_small/medium/large!` 三档分流宏       |
//! | `metrics`            | 无锁原子计数 + 人类可读 Display                |
//! | `pool`               | `ComputePool` / `ComputeHandle` / `ComputePoolExt` |
//! | `thread_local`       | 当前线程的 `ComputeContext` 挂载点             |
//! | `validation` (feat.) | 运行时档位错配检测                              |
//!
//! 本文件主要承担：
//!
//! 1. **配置类型** [`ComputeConfig`]——`Default` 给出合理初值，
//!    `validate` 拦截荒唐输入，`build_pool` 把它实例化成 Rayon 池；
//! 2. **scope-trait** [`ScopeExecutor`]——为外部桥接实现保留扩展位；
//! 3. **`patterns`** 子模块——给常见组合模式（join / map）一个无 self
//!    的便利包装。
//!
//! ## 外部契约
//!
//! - `pub struct ComputeConfig { num_threads, stack_size, thread_prefix, pin_threads }`
//!   及其 `Default` / `validate` / `pub(crate) build_pool`；
//! - `pub trait ScopeExecutor`；
//! - `pub mod patterns` 内 `parallel_join` / `parallel_map`；
//! - mod 声明 `macros` / `metrics` / `pool` / `thread_local` 以及
//!   feature-gated `validation`；
//! - re-export `ComputeMetrics` / `ComputeHandle` / `ComputePool` /
//!   `ComputePoolExt`。

use anyhow::Result;
use rayon::ThreadPoolBuilder;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

// ============================================================================
// 子模块声明 + re-export
// ============================================================================

pub mod macros;
pub mod metrics;
pub mod pool;
pub mod thread_local;
#[cfg(feature = "compute-validation")]
pub mod validation;

pub use metrics::ComputeMetrics;
pub use pool::{ComputeHandle, ComputePool, ComputePoolExt};

// ============================================================================
// 内部常量：默认值集中放，便于审计
// ============================================================================

/// 默认线程栈大小（2 MiB），偏保守，留余量给递归式 Rayon 任务。
const DEFAULT_STACK_SIZE: usize = 2 * 1024 * 1024;

/// 栈大小下限（128 KiB）。低于此值会被 `validate` 拒绝。
const MIN_STACK_SIZE: usize = 128 * 1024;

/// 自动 num_threads 的下限。
const AUTO_THREADS_MIN: usize = 2;

/// 自动 num_threads 的上限。
const AUTO_THREADS_MAX: usize = 16;

/// 默认线程前缀。
const DEFAULT_THREAD_PREFIX: &str = "compute";

// ============================================================================
// ComputeConfig
// ============================================================================

/// `ComputePool` 的构造参数。
///
/// 所有字段都公开，方便上层用 struct-update 语法只覆盖关心的项。
#[derive(Debug, Clone)]
pub struct ComputeConfig {
    /// Rayon 池线程数；`None` 表示按机器自动推断。
    pub num_threads: Option<usize>,

    /// 每个工作线程的栈大小；`None` 表示沿用 Rayon 默认值。
    pub stack_size: Option<usize>,

    /// 线程名前缀，最终名形如 `"<prefix>-<id>"`。
    pub thread_prefix: String,

    /// 是否把工作线程绑定到 CPU core（当前留口未启用）。
    pub pin_threads: bool,
}

impl Default for ComputeConfig {
    fn default() -> Self {
        Self {
            num_threads: None,
            stack_size: Some(DEFAULT_STACK_SIZE),
            thread_prefix: DEFAULT_THREAD_PREFIX.to_string(),
            pin_threads: false,
        }
    }
}

impl ComputeConfig {
    /// 验证配置合法性。
    ///
    /// 当前两条规则：
    ///
    /// - `num_threads == Some(0)` 视为非法（"零线程的池"无意义，应改
    ///   用 `None`）；
    /// - `stack_size < 128 KiB` 视为非法（防止误填字节数当 KiB）。
    pub fn validate(&self) -> Result<()> {
        if matches!(self.num_threads, Some(0)) {
            return Err(anyhow::anyhow!(
                "Number of compute threads cannot be 0. Use None to disable compute pool entirely."
            ));
        }

        if let Some(stack_size) = self.stack_size
            && stack_size < MIN_STACK_SIZE
        {
            return Err(anyhow::anyhow!(
                "Stack size too small: {}KB. Minimum recommended: 128KB",
                stack_size / 1024
            ));
        }

        Ok(())
    }

    /// 把配置实例化成一个真正的 Rayon `ThreadPool`。
    ///
    /// 这是 `pub(crate)`——只有 `ComputePool::new` 应该调用它。
    pub(crate) fn build_pool(&self) -> Result<rayon::ThreadPool> {
        self.validate()?;

        let num_threads = resolve_num_threads(self.num_threads);
        let stack_size = self.stack_size;
        let thread_namer = make_thread_namer(self.thread_prefix.clone());

        let mut builder = ThreadPoolBuilder::new().num_threads(num_threads);
        if let Some(sz) = stack_size {
            builder = builder.stack_size(sz);
        }
        builder = builder.thread_name(thread_namer);

        // pin_threads 暂未实现；预留扩展位以便未来无破坏地接入。
        let _pin_requested = self.pin_threads;

        builder
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to create Rayon thread pool: {}", e))
    }
}

// ============================================================================
// build_pool 私有 helper
// ============================================================================

/// 根据 `Option<usize>` 决定最终线程数。
///
/// `None` → 从 `available_parallelism()` 推断，取一半，clamp 到
/// `[AUTO_THREADS_MIN, AUTO_THREADS_MAX]`；检测失败时回退到 2。
fn resolve_num_threads(opt: Option<usize>) -> usize {
    if let Some(n) = opt {
        return n;
    }
    match std::thread::available_parallelism() {
        Ok(n) => (n.get() / 2).clamp(AUTO_THREADS_MIN, AUTO_THREADS_MAX),
        Err(_) => AUTO_THREADS_MIN,
    }
}

/// 构造一个 thread-name 闭包，按递增 id 给线程命名。
///
/// 单独成函数，方便单测验证命名规则。
fn make_thread_namer(prefix: String) -> impl Fn(usize) -> String + Send + Sync + 'static {
    let counter = Arc::new(AtomicU64::new(0));
    move |_idx| {
        let id = counter.fetch_add(1, Ordering::SeqCst);
        format!("{prefix}-{id}")
    }
}

// ============================================================================
// ScopeExecutor
// ============================================================================

/// scope 风格执行器的抽象。
///
/// 当前 trait 主要用作"扩展接缝"：外部想给一个非 `ComputePool` 的
/// 桥（例如 mock、独立 Rayon 池）实现 scope 调度时可以单独实现该
/// trait 而不必动 `ComputePool` 本身。
pub trait ScopeExecutor {
    /// 在 Rayon scope 内同步执行 `f` 并返回其值。
    fn execute_in_scope<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&rayon::Scope) -> R + Send,
        R: Send;
}

// ============================================================================
// patterns：无 self 的便利包装
// ============================================================================

/// 常见组合并行模式的便利函数。
///
/// 这一层薄包装存在的意义只是让调用点更短：
///
/// ```ignore
/// patterns::parallel_join(&pool, a, b).await?
/// // 等价于
/// pool.execute(move || rayon::join(a, b)).await?
/// ```
pub mod patterns {
    use super::*;

    /// 并行执行两个独立闭包。
    pub async fn parallel_join<F1, F2, R1, R2>(
        pool: &ComputePool,
        f1: F1,
        f2: F2,
    ) -> Result<(R1, R2)>
    where
        F1: FnOnce() -> R1 + Send + 'static,
        F2: FnOnce() -> R2 + Send + 'static,
        R1: Send + 'static,
        R2: Send + 'static,
    {
        pool.execute(move || rayon::join(f1, f2)).await
    }

    /// 对 `items` 做并行 `map`，等价于 `pool.parallel_map` 但无需引入
    /// `ComputePoolExt` trait。
    pub async fn parallel_map<F, T, R>(pool: &ComputePool, items: Vec<T>, f: F) -> Result<Vec<R>>
    where
        F: Fn(T) -> R + Sync + Send + 'static,
        T: Send + 'static,
        R: Send + 'static,
    {
        use rayon::prelude::*;
        pool.execute(move || items.into_par_iter().map(f).collect())
            .await
    }
}

// ============================================================================
// 单元测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ------------------------------------------------------------------
    // Default / validate
    // ------------------------------------------------------------------

    #[test]
    fn default_config_has_expected_fields() {
        let c = ComputeConfig::default();
        assert_eq!(c.thread_prefix, "compute");
        assert_eq!(c.stack_size, Some(2 * 1024 * 1024));
        assert!(!c.pin_threads);
        assert!(c.num_threads.is_none());
    }

    #[test]
    fn validate_accepts_default() {
        assert!(ComputeConfig::default().validate().is_ok());
    }

    #[test]
    fn validate_rejects_zero_threads() {
        let c = ComputeConfig {
            num_threads: Some(0),
            ..Default::default()
        };
        let err = c.validate().unwrap_err().to_string();
        assert!(err.contains("cannot be 0"), "实际: {err}");
    }

    #[test]
    fn validate_rejects_tiny_stack() {
        let c = ComputeConfig {
            stack_size: Some(4 * 1024),
            ..Default::default()
        };
        let err = c.validate().unwrap_err().to_string();
        assert!(err.contains("Stack size too small"), "实际: {err}");
    }

    #[test]
    fn validate_accepts_minimum_stack() {
        let c = ComputeConfig {
            stack_size: Some(128 * 1024),
            ..Default::default()
        };
        assert!(c.validate().is_ok());
    }

    // ------------------------------------------------------------------
    // resolve_num_threads
    // ------------------------------------------------------------------

    #[test]
    fn resolve_threads_explicit_value_passthrough() {
        assert_eq!(resolve_num_threads(Some(7)), 7);
    }

    #[test]
    fn resolve_threads_auto_within_bounds() {
        let n = resolve_num_threads(None);
        assert!((AUTO_THREADS_MIN..=AUTO_THREADS_MAX).contains(&n));
    }

    // ------------------------------------------------------------------
    // make_thread_namer
    // ------------------------------------------------------------------

    #[test]
    fn thread_namer_uses_prefix_and_increments() {
        let namer = make_thread_namer("wrk".to_string());
        let a = namer(0);
        let b = namer(0);
        assert_eq!(a, "wrk-0");
        assert_eq!(b, "wrk-1");
    }

    // ------------------------------------------------------------------
    // build_pool
    // ------------------------------------------------------------------

    #[test]
    fn build_pool_with_explicit_threads() {
        let c = ComputeConfig {
            num_threads: Some(2),
            ..Default::default()
        };
        let pool = c.build_pool().unwrap();
        assert_eq!(pool.current_num_threads(), 2);
    }

    #[test]
    fn build_pool_with_defaults() {
        let pool = ComputeConfig::default().build_pool().unwrap();
        assert!(pool.current_num_threads() >= AUTO_THREADS_MIN);
    }

    #[test]
    fn build_pool_validates_before_building() {
        let c = ComputeConfig {
            num_threads: Some(0),
            ..Default::default()
        };
        assert!(c.build_pool().is_err());
    }

    // ------------------------------------------------------------------
    // patterns
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn patterns_parallel_join_returns_tuple() {
        let pool = ComputePool::with_defaults().unwrap();
        let (a, b) = patterns::parallel_join(&pool, || 10, || 20).await.unwrap();
        assert_eq!((a, b), (10, 20));
    }

    #[tokio::test]
    async fn patterns_parallel_map_applies_function() {
        let pool = ComputePool::with_defaults().unwrap();
        let v: Vec<i32> = patterns::parallel_map(&pool, (0..5).collect(), |x| x * x)
            .await
            .unwrap();
        assert_eq!(v, vec![0, 1, 4, 9, 16]);
    }

    // ------------------------------------------------------------------
    // re-export 烟囱测试：确保对外符号仍然可见
    // ------------------------------------------------------------------

    #[test]
    fn reexports_are_visible() {
        let _m: ComputeMetrics = ComputeMetrics::new();
        let _: fn() -> Result<ComputePool> = ComputePool::with_defaults;
    }

    #[test]
    fn test_build_pool() {
        let config = ComputeConfig {
            num_threads: Some(2),
            ..Default::default()
        };

        let pool = config.build_pool().unwrap();
        assert_eq!(pool.current_num_threads(), 2);
    }

    #[test]
    fn test_compute_config_default() {
        let config = ComputeConfig::default();
        assert_eq!(config.thread_prefix, "compute");
        assert_eq!(config.stack_size, Some(2 * 1024 * 1024));
        assert!(!config.pin_threads);
    }
}
