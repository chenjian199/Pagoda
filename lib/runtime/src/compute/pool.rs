// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `compute::pool` —— Rayon 计算池的 async 桥与扩展
//!
//! ## 设计意图
//!
//! [`ComputePool`] 是 Pagoda runtime 里所有"CPU 密集型工作"的统一入
//! 口。它在三件事上做了取舍：
//!
//! 1. **复用同一 Rayon 池**：多个 async 任务并发提交 `execute /
//!    install / execute_scoped*`，由 Rayon 的 work-stealing 调度器
//!    自动均衡；
//! 2. **桥接 Tokio**：所有 `async fn` 路径都走 [`tokio_rayon::spawn`]
//!    把闭包 dispatch 到 Rayon，绝不阻塞 Tokio worker；
//! 3. **强一致指标**：每一次提交都通过 [`record_around`] 一层薄包裹
//!    完成 `start → 跑 → completion` 的计数闭环，把 metrics 操作从
//!    各 fn 主体里抽出来，避免漏调或重复。
//!
//! ## 外部契约
//!
//! - `pub struct ComputePool`（Clone + 自定义 Debug）；
//! - `new(config)` / `with_defaults()` / `execute_sync` / `execute` /
//!   `execute_scoped` / `execute_scoped_fifo` / `join` / `install` /
//!   `metrics()` / `num_threads()`；
//! - `pub struct ComputeHandle<T>` 与 `pub(crate) fn new`；
//! - `pub trait ComputePoolExt`（`#[async_trait]`），含
//!   `parallel_batch` / `parallel_map`，并为 `ComputePool` 提供默认
//!   实现。
//!
//! 所有签名（参数类型、返回类型、生命周期 / Send 约束）均保持不变。

use super::{ComputeConfig, ComputeMetrics};
use anyhow::Result;
use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

// ============================================================================
// ComputePool
// ============================================================================

/// 共享的 Rayon 计算池 + 指标 + 配置。
///
/// 通过 `Arc` 共享 `pool` 与 `metrics`，`Clone` 是廉价的引用复制。
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
            .field("metrics", &self.metrics)
            .field("config", &self.config)
            .finish()
    }
}

impl ComputePool {
    /// 从 [`ComputeConfig`] 构造一个新池。
    pub fn new(config: ComputeConfig) -> Result<Self> {
        let pool = config.build_pool()?;
        Ok(Self {
            pool: Arc::new(pool),
            metrics: Arc::new(ComputeMetrics::new()),
            config,
        })
    }

    /// 使用默认配置构造。
    pub fn with_defaults() -> Result<Self> {
        Self::new(ComputeConfig::default())
    }

    // ------------------------------------------------------------------
    // 同步路径
    // ------------------------------------------------------------------

    /// 同步 `install`，仅供 `spawn_blocking` 或已知非 async 上下文使用。
    ///
    /// 不更新指标：调用方既然走了同步快路径，就应当自行决定是否计费。
    pub fn execute_sync<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R + Send,
        R: Send,
    {
        self.pool.install(f)
    }

    // ------------------------------------------------------------------
    // async 桥路径
    //
    // 这一组 fn 共享 record_around 公共骨架，保证：
    //   - start 计数一定与 completion 计数配对；
    //   - 失败也走 completion 路径（实际上 `tokio_rayon::spawn` 不会
    //     失败，但留口子方便未来引入超时 / 取消）。
    // ------------------------------------------------------------------

    /// 在 Rayon 池里执行一个闭包，并把结果通过 async 通道返回。
    pub async fn execute<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let pool = self.pool.clone();
        record_around(&self.metrics, async move {
            tokio_rayon::spawn(move || pool.install(f)).await
        })
        .await
    }

    /// 在 Rayon 池里执行一个使用 `&rayon::Scope` 的闭包。
    pub async fn execute_scoped<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&rayon::Scope) -> R + Send + 'static,
        R: Send + 'static,
    {
        let pool = self.pool.clone();
        record_around(&self.metrics, async move {
            tokio_rayon::spawn(move || pool.install(|| run_in_scope(f))).await
        })
        .await
    }

    /// FIFO 版本的 `execute_scoped`。
    pub async fn execute_scoped_fifo<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&rayon::ScopeFifo) -> R + Send + 'static,
        R: Send + 'static,
    {
        let pool = self.pool.clone();
        record_around(&self.metrics, async move {
            tokio_rayon::spawn(move || pool.install(|| run_in_scope_fifo(f))).await
        })
        .await
    }

    /// 在 Rayon 池里并行跑两个独立闭包，返回二元组。
    pub async fn join<F1, F2, R1, R2>(&self, f1: F1, f2: F2) -> Result<(R1, R2)>
    where
        F1: FnOnce() -> R1 + Send + 'static,
        F2: FnOnce() -> R2 + Send + 'static,
        R1: Send + 'static,
        R2: Send + 'static,
    {
        self.execute(move || rayon::join(f1, f2)).await
    }

    /// 在 Rayon 池里安装闭包以便使用 `par_iter` / `par_chunks`。
    pub async fn install<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let pool = self.pool.clone();
        record_around(&self.metrics, async move {
            tokio_rayon::spawn(move || pool.install(f)).await
        })
        .await
    }

    // ------------------------------------------------------------------
    // 元信息
    // ------------------------------------------------------------------

    /// 借出指标只读引用。
    pub fn metrics(&self) -> &ComputeMetrics {
        &self.metrics
    }

    /// 池内线程数。
    pub fn num_threads(&self) -> usize {
        self.pool.current_num_threads()
    }
}

// ============================================================================
// 私有 helper
// ============================================================================

/// 包裹一段 `async` 闭包，把"开始记账 → 跑 → 完成记账"封装成一个原子
/// 操作，避免在每个公开 fn 里手写四次同样的样板代码。
async fn record_around<Fut, R>(metrics: &ComputeMetrics, fut: Fut) -> Result<R>
where
    Fut: Future<Output = R>,
{
    metrics.record_task_start();
    let start = Instant::now();
    let result = fut.await;
    metrics.record_task_completion(start.elapsed());
    Ok(result)
}

/// 用 `rayon::scope` 跑一个闭包并把它的返回值带出来。
fn run_in_scope<F, R>(f: F) -> R
where
    F: FnOnce(&rayon::Scope) -> R + Send,
    R: Send,
{
    let mut slot: Option<R> = None;
    rayon::scope(|s| {
        slot = Some(f(s));
    });
    slot.expect("rayon::scope returned without invoking f")
}

/// FIFO scope 的同款 helper。
fn run_in_scope_fifo<F, R>(f: F) -> R
where
    F: FnOnce(&rayon::ScopeFifo) -> R + Send,
    R: Send,
{
    let mut slot: Option<R> = None;
    rayon::scope_fifo(|s| {
        slot = Some(f(s));
    });
    slot.expect("rayon::scope_fifo returned without invoking f")
}

// ============================================================================
// ComputeHandle
// ============================================================================

/// 任意 `Future<Output = T>` 的"擦类型"包装，用于把不同 fn 的返回类型
/// 收敛成同一个 `ComputeHandle<T>`。
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

// ============================================================================
// ComputePoolExt
// ============================================================================

/// 在 [`ComputePool`] 之外补充的、面向"集合数据"的并行模式。
#[async_trait]
pub trait ComputePoolExt {
    /// 把 `items` 切成大小为 `batch_size` 的块，对每块调用 `f`，把所有
    /// 输出展平成单个 `Vec<R>`。
    async fn parallel_batch<T, F, R>(
        &self,
        items: Vec<T>,
        batch_size: usize,
        f: F,
    ) -> Result<Vec<R>>
    where
        T: Send + Sync + 'static,
        F: Fn(&[T]) -> Vec<R> + Send + Sync + 'static,
        R: Send + 'static;

    /// 用 `par_iter` 对 `items` 做并行 map。
    async fn parallel_map<T, F, R>(&self, items: Vec<T>, f: F) -> Result<Vec<R>>
    where
        T: Send + Sync + 'static,
        F: Fn(T) -> R + Send + Sync + 'static,
        R: Send + 'static;
}

#[async_trait]
impl ComputePoolExt for ComputePool {
    async fn parallel_batch<T, F, R>(
        &self,
        items: Vec<T>,
        batch_size: usize,
        f: F,
    ) -> Result<Vec<R>>
    where
        T: Send + Sync + 'static,
        F: Fn(&[T]) -> Vec<R> + Send + Sync + 'static,
        R: Send + 'static,
    {
        use rayon::prelude::*;
        self.install(move || items.par_chunks(batch_size).flat_map(f).collect())
            .await
    }

    async fn parallel_map<T, F, R>(&self, items: Vec<T>, f: F) -> Result<Vec<R>>
    where
        T: Send + Sync + 'static,
        F: Fn(T) -> R + Send + Sync + 'static,
        R: Send + 'static,
    {
        use rayon::prelude::*;
        self.install(move || items.into_par_iter().map(f).collect())
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
    // 构造 / 元信息
    // ------------------------------------------------------------------

    #[test]
    fn pool_with_defaults_has_at_least_two_threads() {
        let pool = ComputePool::with_defaults().unwrap();
        assert!(pool.num_threads() >= 2);
        // metrics 初值为零
        assert_eq!(pool.metrics().tasks_total(), 0);
    }

    #[test]
    fn pool_debug_includes_thread_count() {
        let pool = ComputePool::with_defaults().unwrap();
        let s = format!("{pool:?}");
        assert!(s.contains("num_threads"));
        assert!(s.contains("ComputePool"));
    }

    #[test]
    fn pool_clone_shares_metrics() {
        let pool = ComputePool::with_defaults().unwrap();
        let pool2 = pool.clone();
        // 通过观察"在一个 clone 上记一次任务，另一个 clone 也能看见"
        // 来证明 metrics 是共享同一个 Arc。
        pool.metrics.record_task_start();
        pool.metrics.record_task_completion(std::time::Duration::from_micros(1));
        assert_eq!(pool2.metrics().tasks_total(), 1);
        assert!(Arc::ptr_eq(&pool.metrics, &pool2.metrics));
    }

    // ------------------------------------------------------------------
    // execute / execute_sync
    // ------------------------------------------------------------------

    /// ## 测试过程
    /// `execute` 在 async 上下文中把 `0..1000` 求和 dispatch 到 Rayon。
    ///
    /// ## 意义
    /// 锁定最常用入口的正确性，并验证调用后 `tasks_total == 1`。
    #[tokio::test]
    async fn execute_sums_range_and_records_one_task() {
        let pool = ComputePool::with_defaults().unwrap();
        let result = pool.execute(|| (0u64..1000).sum::<u64>()).await.unwrap();
        assert_eq!(result, 499500);
        assert_eq!(pool.metrics().tasks_total(), 1);
        assert_eq!(pool.metrics().tasks_active(), 0);
    }

    /// `execute_sync` 必须在 spawn_blocking 中工作且不记录指标。
    #[tokio::test]
    async fn execute_sync_works_under_spawn_blocking() {
        let pool = Arc::new(ComputePool::with_defaults().unwrap());
        let p = pool.clone();
        let r = tokio::task::spawn_blocking(move || {
            p.execute_sync(|| (0u64..100).sum::<u64>())
        })
        .await
        .unwrap();
        assert_eq!(r, 4950);
        // execute_sync 不计费
        assert_eq!(pool.metrics().tasks_total(), 0);
    }

    // ------------------------------------------------------------------
    // join / install
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn join_runs_two_closures_in_parallel() {
        let pool = ComputePool::with_defaults().unwrap();
        let (a, b) = pool.join(|| 2 + 2, || 3 * 3).await.unwrap();
        assert_eq!((a, b), (4, 9));
        assert_eq!(pool.metrics().tasks_total(), 1);
    }

    #[tokio::test]
    async fn install_runs_closure_on_pool() {
        let pool = ComputePool::with_defaults().unwrap();
        use rayon::prelude::*;
        let r: u64 = pool
            .install(|| (0u64..1000).into_par_iter().sum())
            .await
            .unwrap();
        assert_eq!(r, 499500);
    }

    // ------------------------------------------------------------------
    // scope
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn execute_scoped_spawns_multiple_children() {
        use std::sync::mpsc;
        let pool = ComputePool::with_defaults().unwrap();
        let mut out = pool
            .execute_scoped(|s| {
                let (tx, rx) = mpsc::channel();
                for i in 0..4 {
                    let tx = tx.clone();
                    s.spawn(move |_| tx.send(i * 2).unwrap());
                }
                drop(tx);
                let mut v: Vec<i32> = rx.iter().collect();
                v.sort();
                v
            })
            .await
            .unwrap();
        out.sort();
        assert_eq!(out, vec![0, 2, 4, 6]);
    }

    #[tokio::test]
    async fn execute_scoped_fifo_returns_result() {
        let pool = ComputePool::with_defaults().unwrap();
        let r: i32 = pool
            .execute_scoped_fifo(|_s| 42)
            .await
            .unwrap();
        assert_eq!(r, 42);
    }

    /// run_in_scope helper：F 一定会被调用且其返回值被传出。
    #[test]
    fn run_in_scope_helper_returns_value() {
        let v = run_in_scope(|_| 7u32);
        assert_eq!(v, 7);
    }

    // ------------------------------------------------------------------
    // ComputePoolExt
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn parallel_map_doubles_each_item() {
        let pool = ComputePool::with_defaults().unwrap();
        let v: Vec<i32> = pool
            .parallel_map((0..10i32).collect::<Vec<_>>(), |x| x * 2)
            .await
            .unwrap();
        assert_eq!(v, (0..10).map(|x| x * 2).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn parallel_batch_processes_chunks() {
        let pool = ComputePool::with_defaults().unwrap();
        let items: Vec<i32> = (0..20).collect();
        let v: Vec<i32> = pool
            .parallel_batch(items, 5, |chunk| chunk.iter().map(|x| x + 1).collect())
            .await
            .unwrap();
        assert_eq!(v.len(), 20);
        assert_eq!(v[0], 1);
        assert_eq!(v[19], 20);
    }

    // ------------------------------------------------------------------
    // 指标记账闭环
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn record_around_counts_each_submission() {
        let pool = ComputePool::with_defaults().unwrap();
        for _ in 0..5 {
            pool.execute(|| 1u32).await.unwrap();
        }
        assert_eq!(pool.metrics().tasks_total(), 5);
        assert_eq!(pool.metrics().tasks_active(), 0);
    }

    // ------------------------------------------------------------------
    // ComputeHandle
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn compute_handle_forwards_inner_future() {
        let h = ComputeHandle::new(async { 123u32 });
        assert_eq!(h.await, 123);
    }

    // ------------------------------------------------------------------
    // === 标准契约测试 ============================
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_compute_pool_execute() {
        let pool = ComputePool::with_defaults().unwrap();

        let result = pool
            .execute(|| {
                // 模拟一段 CPU 密集型工作
                let mut sum = 0u64;
                for i in 0..1000 {
                    sum += i;
                }
                sum
            })
            .await
            .unwrap();

        assert_eq!(result, 499500);
    }

    #[tokio::test]
    async fn test_compute_pool_execute_sync() {
        use std::sync::Arc;
        let pool = Arc::new(ComputePool::with_defaults().unwrap());

        // 在 spawn_blocking 中测试 execute_sync
        let pool_clone = pool.clone();
        let result = tokio::task::spawn_blocking(move || {
            pool_clone.execute_sync(|| {
                let mut sum = 0u64;
                for i in 0..1000 {
                    sum += i;
                }
                sum
            })
        })
        .await
        .unwrap();

        assert_eq!(result, 499500);
    }

    #[tokio::test]
    async fn test_compute_pool_join() {
        let pool = ComputePool::with_defaults().unwrap();

        let (a, b) = pool.join(|| 2 + 2, || 3 * 3).await.unwrap();

        assert_eq!(a, 4);
        assert_eq!(b, 9);
    }

    #[tokio::test]
    async fn test_compute_pool_scoped() {
        use std::sync::mpsc;

        let pool = ComputePool::with_defaults().unwrap();

        let mut result = pool
            .execute_scoped(|scope| {
                let (tx, rx) = mpsc::channel();

                for i in 0..4 {
                    let tx = tx.clone();
                    scope.spawn(move |_| {
                        tx.send((i, i * 2)).unwrap();
                    });
                }

                drop(tx); // 关闭发送端，接收端才能结束迭代

                let mut results = vec![0; 4];
                for (i, val) in rx {
                    results[i] = val;
                }
                results
            })
            .await
            .unwrap();

        // 并行执行导致结果顺序不确定，排序后再断言
        result.sort();
        assert_eq!(result, vec![0, 2, 4, 6]);
    }
}
