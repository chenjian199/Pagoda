// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `compute::macros` —— 三档计算任务执行宏
//!
//! ## 设计意图
//!
//! Tokio 是为 I/O 密集型设计的，把"耗时几十毫秒以上的纯 CPU 工作"
//! 直接放在 async 任务里会阻塞 worker，进而拖垮整个事件循环。Dynamo
//! 在 `compute::pool` 之上提供了一组 **零开销 / 上下文感知** 的执行
//! 宏，让调用方按预估耗时挑选合适的执行策略：
//!
//! | 宏 | 期望耗时 | 实际执行路径 |
//! | --- | --- | --- |
//! | [`compute_small!`] | < 100 μs | inline 直接跑（零开销） |
//! | [`compute_medium!`] | 100 μs ~ 1 ms | 优先 `block_in_place` permit；否则 offload |
//! | [`compute_large!`] | > 1 ms | 永远 offload 到 Rayon 池 |
//!
//! 当编译 feature `compute-validation` 启用时，每个宏会在执行前后取
//! 时间戳，并通过 [`crate::compute::validation`] 校验实际耗时是否落
//! 在该档位允许的区间内；越界会打 warn 日志并累加计数器。
//!
//! ## 外部契约
//!
//! - `#[macro_export]` 宏 `compute_small!` / `compute_medium!` /
//!   `compute_large!`；
//! - 入参形态：`compute_*!(expr)` 与 `compute_medium!/compute_large!`
//!   还支持 `compute_*!(pool, expr)` 显式 pool 形态；
//! - 当 `compute-validation` feature 开启时调用 `$crate::compute::validation::*`，
//!   关闭时这部分代码会被 `#[cfg]` 完全消除——宏体仍要保持"两种 cfg
//!   下都能编译"。
//!
//! ## 设计要点（与上一版的区别）
//!
//! 1. 把 `compute_medium!` 与 `compute_large!` 中"thread-local 探测 +
//!    fallback"逻辑统一到 `__compute_run_medium_async!` /
//!    `__compute_run_large_async!` 这两个**私有 helper 宏**里，使各分支
//!    可以被复用，main macro 只关心计时；
//! 2. 计时逻辑统一抽到 `__compute_timed_block!` 宏中，由它统一负责
//!    `Instant::now()` 与 `validate_*` 调用，避免每个 main macro 重复
//!    手写两遍；
//! 3. 三个宏的文档示例与可见性保持不变（`#[macro_export]`），且
//!    `$crate::compute::*` 路径完全一致，使外部使用方无感知。

// ============================================================================
// 私有 helper 宏：计时块
// ============================================================================

/// 在表达式两端打上 `Instant::now()` 时间戳，并把 `elapsed` 交给指
/// 定的 `validate_*` 函数（仅在启用 `compute-validation` 时生效）。
///
/// 这是给三个公开宏共用的"事后计时"内骨。设为 `#[doc(hidden)]` 以避
/// 免污染公开文档。
#[doc(hidden)]
#[macro_export]
macro_rules! __compute_timed_block {
    ($validator:path, $body:block) => {{
        #[cfg(feature = "compute-validation")]
        let __compute_validation_start = std::time::Instant::now();

        let __compute_validation_result = $body;

        #[cfg(feature = "compute-validation")]
        $validator(__compute_validation_start.elapsed());

        __compute_validation_result
    }};
}

/// 私有 helper：实现 `compute_medium!` 的"thread-local 优先，否则
/// fallback"决策。两个公开 main arm 共用这段逻辑。
///
/// - `$try_pool`：在没拿到 permit 时使用的 pool 表达式
///   （`None`/`Some(expr)`）。`None` 时走 `get_pool()` 回退；`Some(expr)`
///   时直接用该 pool。
#[doc(hidden)]
#[macro_export]
macro_rules! __compute_run_medium_async {
    (none, $expr:expr) => {
        async {
            if let Ok(_permit) =
                $crate::compute::thread_local::try_acquire_block_permit()
            {
                Ok(tokio::task::block_in_place(|| {
                    let r = $expr;
                    drop(_permit);
                    r
                }))
            } else if let Some(__compute_pool) =
                $crate::compute::thread_local::get_pool()
            {
                __compute_pool.execute(|| $expr).await
            } else {
                tracing::warn!(
                    "compute_medium: No thread-local context, executing inline (may block async runtime)",
                );
                Ok($expr)
            }
        }
    };
    (some, $pool:expr, $expr:expr) => {
        async {
            if let Ok(_permit) =
                $crate::compute::thread_local::try_acquire_block_permit()
            {
                Ok(tokio::task::block_in_place(|| {
                    let r = $expr;
                    drop(_permit);
                    r
                }))
            } else {
                $pool.execute(|| $expr).await
            }
        }
    };
}

/// 私有 helper：实现 `compute_large!` 的两条分支（thread-local /
/// 显式 pool）。
#[doc(hidden)]
#[macro_export]
macro_rules! __compute_run_large_async {
    (none, $expr:expr) => {
        async {
            if let Some(__compute_pool) =
                $crate::compute::thread_local::get_pool()
            {
                __compute_pool.execute(|| $expr).await
            } else {
                tracing::warn!(
                    "compute_large: No thread-local context, executing inline (will block async runtime!)",
                );
                Ok($expr)
            }
        }
    };
    (some, $pool:expr, $expr:expr) => {
        async { $pool.execute(|| $expr).await }
    };
}

// ============================================================================
// 公开宏：compute_small!
// ============================================================================

/// 执行一个**小任务**（< 100 μs），直接 inline 跑，零开销。
///
/// 当编译 feature `compute-validation` 启用时，会校验实际耗时是否
/// ≤ 100 μs；越界则打日志并累加 `SMALL_MISCLASSIFIED` 计数器。
///
/// # Example
/// ```
/// # use dynamo_runtime::compute_small;
/// let result = compute_small!(2 + 2);
/// assert_eq!(result, 4);
/// ```
#[macro_export]
macro_rules! compute_small {
    ($expr:expr) => {
        $crate::__compute_timed_block!(
            $crate::compute::validation::validate_small,
            { $expr }
        )
    };
}

// ============================================================================
// 公开宏：compute_medium!
// ============================================================================

/// 执行一个**中等任务**（100 μs ~ 1 ms）。
///
/// 决策顺序：
///
/// 1. 优先尝试从 thread-local context 拿一张 `block_in_place` permit。
///    拿到 → 使用 [`tokio::task::block_in_place`]，让该任务**就地**
///    执行而 Tokio 仍能从 worker 上把其它任务迁走；
/// 2. 否则若 thread-local 里有 `ComputePool`，offload 到 Rayon 池；
/// 3. 否则 fallback inline（同时打 warn）。
///
/// 也支持显式 pool 形态：`compute_medium!(my_pool, { ... })`。
///
/// # Example
/// ```ignore
/// # use dynamo_runtime::{compute_medium, compute::ComputePool};
/// # async fn example(pool: &ComputePool) {
/// // 使用 thread-local context
/// let s: i32 = compute_medium!({ (0..1000).map(|i| i * 2).sum() }).await;
///
/// // 或显式给定 pool
/// let s: i32 = compute_medium!(pool, { (0..1000).map(|i| i * 2).sum() }).await;
/// # }
/// ```
#[macro_export]
macro_rules! compute_medium {
    // thread-local 形态
    ($expr:expr) => {
        $crate::__compute_timed_block!(
            $crate::compute::validation::validate_medium,
            { $crate::__compute_run_medium_async!(none, $expr).await? }
        )
    };

    // 显式 pool 形态
    ($pool:expr, $expr:expr) => {
        $crate::__compute_timed_block!(
            $crate::compute::validation::validate_medium,
            { $crate::__compute_run_medium_async!(some, $pool, $expr).await? }
        )
    };
}

// ============================================================================
// 公开宏：compute_large!
// ============================================================================

/// 执行一个**大任务**（> 1 ms），永远 offload 到 Rayon 池。
///
/// 大任务即使在 thread-local 有 permit 也不应 `block_in_place`——因为
/// 那会长时间占用 Tokio worker。所以本宏直接选 pool。
///
/// # Example
/// ```ignore
/// # use dynamo_runtime::{compute_large, compute::ComputePool};
/// # async fn example(pool: &ComputePool) {
/// // thread-local context
/// let r: u64 = compute_large!({ heavy_work() }).await;
///
/// // 显式 pool
/// let r: u64 = compute_large!(pool, { heavy_work() }).await;
/// # }
/// ```
#[macro_export]
macro_rules! compute_large {
    // thread-local 形态
    ($expr:expr) => {
        $crate::__compute_timed_block!(
            $crate::compute::validation::validate_large,
            { $crate::__compute_run_large_async!(none, $expr).await? }
        )
    };

    // 显式 pool 形态
    ($pool:expr, $expr:expr) => {
        $crate::__compute_timed_block!(
            $crate::compute::validation::validate_large,
            { $crate::__compute_run_large_async!(some, $pool, $expr).await? }
        )
    };
}
