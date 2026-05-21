// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! ComputePool — 基于 Rayon 的 CPU 计算线程池。
//!
//! ## Tokio-Rayon 异步桥接机制
//!
//! ```text
//! Tokio worker thread
//!   ↓ pool.execute(f)               ← async fn，在 Tokio 上下文调用
//!   ↓ tokio_rayon::spawn(move || {  ← oneshot channel 桥接
//!         pool.install(f)            ← Rayon 调度到某个 compute 线程
//!     })
//!   ↓ .await ──────────────────── Tokio 线程挂起，不阻塞事件循环
//!                                           ↓
//!                          compute-N 线程执行 f()，完成后发送结果
//!   ↓ 收到结果，Tokio 线程恢复，返回 Ok(result)
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use super::metrics::ComputeMetrics;
use super::{ComputeConfig, ScopeExecutor};

// ── ComputePool ───────────────────────────────────────────────────────────────

/// CPU 计算线程池。
///
/// 封装 `Arc<rayon::ThreadPool>`，通过 `tokio-rayon` 桥接与 Tokio 集成。
/// `Clone` 为轻量 `Arc` 拷贝，所有克隆共享同一底层池和指标。
#[derive(Clone)]
pub struct ComputePool {
    pool: Arc<rayon::ThreadPool>,
    metrics: Arc<ComputeMetrics>,
    config: ComputeConfig,
}

impl std::fmt::Debug for ComputePool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComputePool")
            .field("num_threads", &self.pool.current_num_threads())
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl ComputePool {
    /// 使用配置创建计算池。
    pub fn new(config: ComputeConfig) -> anyhow::Result<Self> {
        let pool = config.build_pool()?;
        Ok(Self {
            pool: Arc::new(pool),
            metrics: Arc::new(ComputeMetrics::new()),
            config,
        })
    }

    /// 使用默认配置创建计算池。
    pub fn with_defaults() -> anyhow::Result<Self> {
        Self::new(ComputeConfig::default())
    }

    /// 将闭包异步提交到计算池执行（通用入口）。
    ///
    /// `.await` 挂起 Tokio 线程，不阻塞事件循环；Rayon 线程执行完后恢复。
    pub async fn execute<F, R>(&self, f: F) -> anyhow::Result<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let start = Instant::now();
        self.metrics.record_task_start();
        let pool = Arc::clone(&self.pool);
        let result = tokio_rayon::spawn(move || pool.install(f)).await;
        self.metrics.record_task_completion(start.elapsed());
        Ok(result)
    }

    /// 与 `execute` 相同，但语义上专用于需要 Rayon 上下文的场景（`par_iter` 等）。
    pub async fn install<F, R>(&self, f: F) -> anyhow::Result<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let start = Instant::now();
        self.metrics.record_task_start();
        let pool = Arc::clone(&self.pool);
        let result = tokio_rayon::spawn(move || pool.install(f)).await;
        self.metrics.record_task_completion(start.elapsed());
        Ok(result)
    }

    /// 同步版本：直接 `pool.install(f)`，无 async 开销。
    ///
    /// 适用于从 `spawn_blocking` 或其他同步上下文调用。
    pub fn execute_sync<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R + Send,
        R: Send,
    {
        self.pool.install(f)
    }

    /// scope-based 并行（LIFO 调度）。
    ///
    /// 闭包内通过 `&rayon::Scope` 派生并行子任务，scope 结束时所有子任务已完成。
    pub async fn execute_scoped<F, R>(&self, f: F) -> anyhow::Result<R>
    where
        F: FnOnce(&rayon::Scope<'_>) -> R + Send + 'static,
        R: Send + 'static,
    {
        let start = Instant::now();
        self.metrics.record_task_start();
        let pool = Arc::clone(&self.pool);
        let result = tokio_rayon::spawn(move || pool.scope(f)).await;
        self.metrics.record_task_completion(start.elapsed());
        Ok(result)
    }

    /// scope-based 并行（FIFO 调度）。
    pub async fn execute_scoped_fifo<F, R>(&self, f: F) -> anyhow::Result<R>
    where
        F: FnOnce(&rayon::ScopeFifo<'_>) -> R + Send + 'static,
        R: Send + 'static,
    {
        let start = Instant::now();
        self.metrics.record_task_start();
        let pool = Arc::clone(&self.pool);
        let result = tokio_rayon::spawn(move || pool.scope_fifo(f)).await;
        self.metrics.record_task_completion(start.elapsed());
        Ok(result)
    }

    /// 并行执行两个独立任务，返回二元组结果（内部使用 `rayon::join`）。
    pub async fn join<F1, F2, R1, R2>(&self, f1: F1, f2: F2) -> anyhow::Result<(R1, R2)>
    where
        F1: FnOnce() -> R1 + Send + 'static,
        F2: FnOnce() -> R2 + Send + 'static,
        R1: Send + 'static,
        R2: Send + 'static,
    {
        self.execute(move || rayon::join(f1, f2)).await
    }

    /// 返回指标的引用（非 Arc 引用，避免调用方持有额外强引用）。
    pub fn metrics(&self) -> &ComputeMetrics {
        &self.metrics
    }

    /// 返回当前实际线程数。
    pub fn num_threads(&self) -> usize {
        self.pool.current_num_threads()
    }
}

impl ScopeExecutor for ComputePool {
    fn execute_in_scope<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&rayon::Scope<'_>) -> R + Send,
        R: Send,
    {
        // pool.scope 将作用域绑定到本池，而非全局 rayon 池
        self.pool.scope(f)
    }
}

// ── ComputeHandle<T> ──────────────────────────────────────────────────────────

/// 计算任务 Future 句柄，隐藏内部 Future 类型。
///
/// 通过 `ComputeHandle::new(future)` 包装任意 `Send` Future，
/// 调用方只需 `.await` 即可获取结果。
pub struct ComputeHandle<T> {
    inner: Pin<Box<dyn Future<Output = T> + Send>>,
}

impl<T> ComputeHandle<T> {
    pub(crate) fn new<F>(future: F) -> Self
    where
        F: Future<Output = T> + Send + 'static,
    {
        Self {
            inner: Box::pin(future),
        }
    }
}

impl<T> Future for ComputeHandle<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.as_mut().poll(cx)
    }
}

impl<T> std::fmt::Debug for ComputeHandle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComputeHandle").finish_non_exhaustive()
    }
}

// ── ComputePoolExt ────────────────────────────────────────────────────────────

/// `ComputePool` 扩展 trait，提供常用批量并行模式。
///
/// 仅为 [`ComputePool`] 提供具体实现（非 blanket impl）。
///
/// 与 [`crate::compute::patterns`] 模块的区别：
/// - `ComputePoolExt` 是面向对象的方法调用（`pool.parallel_map(...)`）
/// - `patterns` 是独立函数（`patterns::parallel_map(&pool, ...)`）
///
/// **注意**：两者对 `T` 的约束不同：
/// - `ComputePoolExt::parallel_map` 要求 `T: Send + Sync`（Rayon 并行迭代跨线程安全）
/// - `patterns::parallel_map` 仅要求 `T: Send`
pub trait ComputePoolExt {
    /// 分块并行处理：将 `items` 按 `batch_size` 分块，每块并行调用 `f`，结果摊平收集。
    fn parallel_batch<T, F, R>(
        &self,
        items: Vec<T>,
        batch_size: usize,
        f: F,
    ) -> impl Future<Output = anyhow::Result<Vec<R>>> + Send
    where
        T: Send + Sync + 'static,
        F: Fn(&[T]) -> Vec<R> + Send + Sync + 'static,
        R: Send + 'static;

    /// 并行 map：对每个元素执行 `f`，保序收集结果。
    fn parallel_map<T, F, R>(
        &self,
        items: Vec<T>,
        f: F,
    ) -> impl Future<Output = anyhow::Result<Vec<R>>> + Send
    where
        T: Send + Sync + 'static,
        F: Fn(T) -> R + Send + Sync + 'static,
        R: Send + 'static;
}

impl ComputePoolExt for ComputePool {
    fn parallel_batch<T, F, R>(
        &self,
        items: Vec<T>,
        batch_size: usize,
        f: F,
    ) -> impl Future<Output = anyhow::Result<Vec<R>>> + Send
    where
        T: Send + Sync + 'static,
        F: Fn(&[T]) -> Vec<R> + Send + Sync + 'static,
        R: Send + 'static,
    {
        self.install(move || {
            use rayon::prelude::*;
            items
                .par_chunks(batch_size)
                .flat_map(|chunk| f(chunk))
                .collect()
        })
    }

    fn parallel_map<T, F, R>(
        &self,
        items: Vec<T>,
        f: F,
    ) -> impl Future<Output = anyhow::Result<Vec<R>>> + Send
    where
        T: Send + Sync + 'static,
        F: Fn(T) -> R + Send + Sync + 'static,
        R: Send + 'static,
    {
        self.install(move || {
            use rayon::prelude::*;
            items.into_par_iter().map(f).collect()
        })
    }
}

// ── 单元测试 ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_pool() -> ComputePool {
        let mut cfg = ComputeConfig::default();
        cfg.num_threads = Some(2);
        ComputePool::new(cfg).expect("pool creation failed")
    }

    #[tokio::test]
    async fn test_compute_pool_execute() {
        let pool = make_pool();
        let result = pool.execute(|| 1 + 1).await.unwrap();
        assert_eq!(result, 2);
        assert_eq!(pool.metrics().tasks_total(), 1);
        assert_eq!(pool.metrics().tasks_active(), 0);
    }

    #[tokio::test]
    async fn test_compute_pool_join() {
        let pool = make_pool();
        let (a, b) = pool.join(|| 10_u32 * 10, || 20_u32 + 20).await.unwrap();
        assert_eq!(a, 100);
        assert_eq!(b, 40);
    }

    #[tokio::test]
    async fn test_compute_pool_execute_sync() {
        let pool = make_pool();
        let pool_clone = pool.clone();
        let result = tokio::task::spawn_blocking(move || pool_clone.execute_sync(|| 42_u32))
            .await
            .unwrap();
        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn test_compute_pool_scoped() {
        let pool = make_pool();
        let result = pool
            .execute_scoped(|_scope| {
                // scope 内返回计算结果
                let sum: u32 = (1..=100).sum();
                sum
            })
            .await
            .unwrap();
        assert_eq!(result, 5050);
    }

    #[tokio::test]
    async fn test_parallel_batch() {
        let pool = make_pool();
        let items: Vec<u32> = (0..10).collect();
        let result = pool
            .parallel_batch(items, 3, |chunk| chunk.iter().map(|x| x * 2).collect())
            .await
            .unwrap();
        assert_eq!(result.len(), 10);
        // 每个元素翻倍，总和为 2*(0+1+…+9) = 90
        assert_eq!(result.iter().sum::<u32>(), 90);
    }

    #[tokio::test]
    async fn test_parallel_map() {
        let pool = make_pool();
        let items: Vec<u32> = (1..=5).collect();
        let mut result = pool.parallel_map(items, |x| x * x).await.unwrap();
        result.sort();
        assert_eq!(result, vec![1, 4, 9, 16, 25]);
    }
}
