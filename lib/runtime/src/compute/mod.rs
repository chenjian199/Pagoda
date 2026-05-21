// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! CPU 计算隔离层。
//!
//! 提供独立于 Tokio 的 Rayon 线程池，专用于 CPU 密集型操作（token 处理、批量解码、
//! 向量计算等），通过 `tokio-rayon` 异步桥接与 Tokio 集成。
//!
//! ## 核心设计原则
//!
//! - **物理隔离**：Rayon 线程池与 Tokio worker 线程完全独立
//! - **工作窃取**：Rayon 内置 work-stealing 调度
//! - **异步桥接**：`tokio-rayon` 将 Rayon 操作包装为 Tokio Future
//! - **分级宏**：`compute_small!` / `compute_medium!` / `compute_large!` 按耗时选策略

pub mod pool;
pub mod thread_local;
pub mod macros;
pub mod metrics;
#[cfg(feature = "compute-validation")]
pub mod validation;

pub use metrics::ComputeMetrics;
pub use pool::{ComputeHandle, ComputePool, ComputePoolExt};

use std::sync::atomic::{AtomicU64, Ordering};

/// 全局线程命名计数器，保证同一池内线程名稳定（compute-0, compute-1…）。
pub(crate) static THREAD_COUNTER: AtomicU64 = AtomicU64::new(0);

// ── ComputeConfig ─────────────────────────────────────────────────────────────

/// Rayon 线程池配置。
///
/// 通过 [`ComputeConfig::build_pool`] 创建 `rayon::ThreadPool`，
/// 是 `ComputePool` 到 Rayon 的唯一构建收口点。
#[derive(Debug, Clone)]
pub struct ComputeConfig {
    /// 线程数。`None` → `clamp(cpu/2, 2, 16)`；检测失败默认 2。
    pub num_threads: Option<usize>,
    /// 线程栈大小。`None` 使用默认值；最小推荐 128 KiB。
    pub stack_size: Option<usize>,
    /// 线程命名前缀，默认 `"compute"`。
    pub thread_prefix: String,
    /// CPU 绑定（已建模，当前未实现）。
    pub pin_threads: bool,
}

impl Default for ComputeConfig {
    fn default() -> Self {
        Self {
            num_threads: None,
            stack_size: Some(2 * 1024 * 1024), // 2 MiB
            thread_prefix: "compute".to_string(),
            pin_threads: false,
        }
    }
}

impl ComputeConfig {
    /// 验证配置合法性。
    ///
    /// - `num_threads == Some(0)` → Err（0 线程无意义）
    /// - `stack_size < 128 KiB` → Err
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.num_threads == Some(0) {
            anyhow::bail!("ComputeConfig: num_threads must be > 0");
        }
        if let Some(size) = self.stack_size {
            if size < 128 * 1024 {
                anyhow::bail!(
                    "ComputeConfig: stack_size {} is below minimum 128 KiB",
                    size
                );
            }
        }
        Ok(())
    }

    /// 将配置转换为 Rayon ThreadPool 实例（唯一收口点）。
    pub(crate) fn build_pool(&self) -> anyhow::Result<rayon::ThreadPool> {
        self.validate()?;

        let num_threads = match self.num_threads {
            Some(n) => n,
            None => {
                let cpus = std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(4);
                (cpus / 2).clamp(2, 16)
            }
        };

        let prefix = self.thread_prefix.clone();
        let mut builder = rayon::ThreadPoolBuilder::new().num_threads(num_threads);

        if let Some(size) = self.stack_size {
            builder = builder.stack_size(size);
        }

        builder = builder.thread_name(move |_| {
            let id = THREAD_COUNTER.fetch_add(1, Ordering::SeqCst);
            format!("{prefix}-{id}")
        });

        // pin_threads 已建模但当前未实现 CPU 亲和性绑定
        builder
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to create Rayon thread pool: {}", e))
    }
}

// ── ScopeExecutor trait ───────────────────────────────────────────────────────

/// 支持 Rayon `Scope` 语义的执行器接口。
///
/// 把"在作用域内并行执行"的能力抽象出来，避免调用方直接耦合 `ComputePool`。
/// 当前仅 [`ComputePool`] 提供具体实现。
pub trait ScopeExecutor {
    /// 在池的作用域中执行闭包，闭包内可创建并行子任务。
    fn execute_in_scope<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&rayon::Scope<'_>) -> R + Send,
        R: Send;
}

// ── patterns 内联子模块 ────────────────────────────────────────────────────────

/// 常用并行模式辅助函数。
///
/// 以独立函数形式提供，接收 `&ComputePool` 参数；
/// 与 [`ComputePoolExt`] trait 方法的区别：
/// - `patterns` 函数适合作为轻量 helper 直接按函数调用
/// - [`ComputePoolExt`] 将常见模式挂到池对象上，面向对象调用风格
///
/// **注意**：`patterns::parallel_map` 对 `T` 的约束是 `Send`，
/// 而 [`ComputePoolExt::parallel_map`] 要求 `T: Send + Sync`。
pub mod patterns {
    use super::pool::ComputePool;

    /// 并行执行两个独立任务，返回二元组结果。
    ///
    /// 内部使用 `rayon::join`，两个任务在池内并行调度。
    pub async fn parallel_join<F1, F2, R1, R2>(
        pool: &ComputePool,
        f1: F1,
        f2: F2,
    ) -> anyhow::Result<(R1, R2)>
    where
        F1: FnOnce() -> R1 + Send + 'static,
        F2: FnOnce() -> R2 + Send + 'static,
        R1: Send + 'static,
        R2: Send + 'static,
    {
        pool.execute(move || rayon::join(f1, f2)).await
    }

    /// 并行 map：对每个元素执行 `f`，收集结果。
    ///
    /// 与 [`ComputePoolExt::parallel_map`] 的区别：此函数 `T: Send`（不要求 `Sync`）。
    pub async fn parallel_map<F, T, R>(
        pool: &ComputePool,
        items: Vec<T>,
        f: F,
    ) -> anyhow::Result<Vec<R>>
    where
        F: Fn(T) -> R + Sync + Send + 'static,
        T: Send + 'static,
        R: Send + 'static,
    {
        pool.execute(move || {
            use rayon::prelude::*;
            items.into_par_iter().map(f).collect()
        })
        .await
    }
}

// ── 单元测试 ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_config_default() {
        let cfg = ComputeConfig::default();
        assert!(cfg.num_threads.is_none());
        assert_eq!(cfg.stack_size, Some(2 * 1024 * 1024));
        assert_eq!(cfg.thread_prefix, "compute");
        assert!(!cfg.pin_threads);
    }

    #[test]
    fn test_compute_config_validate_zero_threads() {
        let mut cfg = ComputeConfig::default();
        cfg.num_threads = Some(0);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_compute_config_validate_small_stack() {
        let mut cfg = ComputeConfig::default();
        cfg.stack_size = Some(64 * 1024); // 64 KiB < 128 KiB
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_build_pool() {
        let mut cfg = ComputeConfig::default();
        cfg.num_threads = Some(2);
        let pool = cfg.build_pool().expect("build_pool failed");
        assert_eq!(pool.current_num_threads(), 2);
    }

    #[test]
    fn test_build_pool_default_threads() {
        let cfg = ComputeConfig::default();
        let pool = cfg.build_pool().expect("build_pool with default threads failed");
        // 线程数在 [2, 16] 范围内
        let n = pool.current_num_threads();
        assert!((2..=16).contains(&n), "num_threads={n} not in [2,16]");
    }
}
