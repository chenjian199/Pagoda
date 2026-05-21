// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 分级计算宏：按任务预期耗时选择执行策略。
//!
//! ## 使用策略
//!
//! | 宏 | 预期耗时 | 执行策略 | 返回类型 |
//! |----|---------|---------|---------|
//! | `compute_small!` | < 100μs | 直接内联，零开销 | 同步返回值 |
//! | `compute_medium!` | 100μs–1ms | `block_in_place`（Semaphore 控制）或内联降级 | 同步返回值 |
//! | `compute_large!` | > 1ms | Rayon 线程池 | **`impl Future`，需 `.await`** |
//!
//! > `compute_small!` 和 `compute_medium!` 同步返回，`compute_large!` 返回 Future。
//! > 调用方需注意区分，对 `compute_large!` 使用 `.await`。
//!
//! ## 示例
//!
//! ```rust,ignore
//! // 小任务：直接内联
//! let sum = compute_small!(a + b);
//!
//! // 中型任务：block_in_place（在 async fn 内调用）
//! let result = compute_medium!(heavy_decode(&data));
//!
//! // 大型任务：卸载到 Rayon（需 await）
//! let embedding = compute_large!(compute_embedding(&tokens)).await.unwrap();
//!
//! // 显式指定池（大型任务）
//! let result = compute_large!(pool, matrix_multiply(&a, &b)).await.unwrap();
//! ```

// ── compute_small! ────────────────────────────────────────────────────────────

/// 小型 CPU 任务（< 100μs）：直接内联执行，零调度开销。
///
/// 开启 `compute-validation` feature 时，会验证实际耗时并在超出阈值时发出警告。
///
/// **返回**：同步返回表达式的值。
#[macro_export]
macro_rules! compute_small {
    ($expr:expr) => {{
        #[cfg(feature = "compute-validation")]
        let __start = ::std::time::Instant::now();

        let __result = $expr;

        #[cfg(feature = "compute-validation")]
        $crate::compute::validation::validate_small(__start.elapsed());

        __result
    }};
}

// ── compute_medium! ───────────────────────────────────────────────────────────

/// 中型 CPU 任务（100μs–1ms）：优先通过 `block_in_place` 执行，无许可时内联降级。
///
/// ## 执行路径
///
/// 1. 尝试从线程本地上下文获取 `block_in_place` 许可（[`Semaphore`] 控制并发数）
/// 2. 成功 → `tokio::task::block_in_place(|| expr)`：当前 Tokio worker 让出调度权
///    但保留线程执行计算，完成后恢复
/// 3. 失败（无上下文 / 许可耗尽）→ 内联执行并 warn
///
/// ## 注意
///
/// - 必须在多线程 Tokio runtime 中使用（`block_in_place` 要求 multi-thread）
/// - **返回**：同步返回表达式的值（无论走哪条路径）
///
/// [`Semaphore`]: tokio::sync::Semaphore
#[macro_export]
macro_rules! compute_medium {
    ($expr:expr) => {{
        #[cfg(feature = "compute-validation")]
        let __start = ::std::time::Instant::now();

        let __result = match $crate::compute::thread_local::try_acquire_block_permit() {
            Ok(_permit) => {
                // _permit 在此作用域内持有，block_in_place 完成后自动释放
                ::tokio::task::block_in_place(|| $expr)
            }
            Err(_reason) => {
                ::tracing::warn!(
                    "compute_medium: 无 block_in_place 许可（{}），降级为内联执行",
                    _reason
                );
                $expr
            }
        };

        #[cfg(feature = "compute-validation")]
        $crate::compute::validation::validate_medium(__start.elapsed());

        __result
    }};

    // 显式提供池的版本（池参数保留以备扩展，当前执行路径与无参版本相同）
    ($pool:expr, $expr:expr) => {{
        let _ = &$pool; // 抑制未使用警告，pool 参数预留给未来的显式池路径

        #[cfg(feature = "compute-validation")]
        let __start = ::std::time::Instant::now();

        let __result = match $crate::compute::thread_local::try_acquire_block_permit() {
            Ok(_permit) => {
                ::tokio::task::block_in_place(|| $expr)
            }
            Err(_reason) => {
                ::tracing::warn!(
                    "compute_medium: 无 block_in_place 许可（{}），降级为内联执行",
                    _reason
                );
                $expr
            }
        };

        #[cfg(feature = "compute-validation")]
        $crate::compute::validation::validate_medium(__start.elapsed());

        __result
    }};
}

// ── compute_large! ────────────────────────────────────────────────────────────

/// 大型 CPU 任务（> 1ms）：卸载到 Rayon 线程池异步执行。
///
/// ## 执行路径
///
/// **无参数版本**（使用线程本地 pool）：
/// 1. 尝试获取线程本地 [`ComputePool`]
/// 2. 成功 → `pool.execute(|| expr)`，在 Rayon 线程上执行
/// 3. 无 pool → `async { Ok(expr) }` 内联执行并 warn
///
/// **显式 pool 版本**：直接 `pool.execute(|| expr)`。
///
/// ## 注意
///
/// - **返回 `impl Future<Output = anyhow::Result<T>>`，必须 `.await`**
/// - 约 25μs 的 channel 开销对 > 1ms 的计算可忽略不计
///
/// [`ComputePool`]: crate::compute::ComputePool
#[macro_export]
macro_rules! compute_large {
    ($expr:expr) => {
        async move {
            #[cfg(feature = "compute-validation")]
            let __start = ::std::time::Instant::now();

            let __result = match $crate::compute::thread_local::get_pool() {
                Some(__pool) => {
                    __pool.execute(move || $expr).await
                }
                None => {
                    ::tracing::warn!(
                        "compute_large: 无 ComputePool 上下文，降级为内联执行"
                    );
                    Ok($expr)
                }
            };

            #[cfg(feature = "compute-validation")]
            if let Ok(_) = &__result {
                $crate::compute::validation::validate_large(__start.elapsed());
            }

            __result
        }
    };

    // 显式提供池的版本：直接使用指定池，无降级路径
    ($pool:expr, $expr:expr) => {
        $pool.execute(move || $expr)
    };
}
