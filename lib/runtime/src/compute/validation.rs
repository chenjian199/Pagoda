// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 计算任务分级验证模块（`compute-validation` feature）。
//!
//! 仅在开启 `compute-validation` feature 时编译，用于开发期检测任务是否被正确分级。
//! 若任务实际耗时与其使用的宏级别不符，则记录警告日志并累加误分类计数器。
//!
//! ## 使用场景
//!
//! 在测试环境开启此 feature 后，`compute_small!` / `compute_medium!` / `compute_large!`
//! 宏会自动调用对应的 `validate_*` 函数。通过 `get_misclassification_metrics()`
//! 可读取聚合统计，便于定位哪些任务使用了错误的宏级别。

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// `compute_small!` 的耗时阈值（100 μs）。
pub const SMALL_THRESHOLD_US: u64 = 100;
/// `compute_medium!` 的耗时上限阈值（1 ms）。
pub const MEDIUM_THRESHOLD_US: u64 = 1_000;

// 私有静态误分类计数器，外部只能通过函数接口访问
static SMALL_MISCLASSIFIED: AtomicU64 = AtomicU64::new(0);
static MEDIUM_MISCLASSIFIED: AtomicU64 = AtomicU64::new(0);
static LARGE_MISCLASSIFIED: AtomicU64 = AtomicU64::new(0);

/// 验证 `compute_small!` 任务的实际耗时。
///
/// 超过 100 μs 时打印警告并记录误分类。
pub fn validate_small(elapsed: Duration) {
    let us = elapsed.as_micros() as u64;
    if us > SMALL_THRESHOLD_US {
        SMALL_MISCLASSIFIED.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            elapsed_us = us,
            threshold_us = SMALL_THRESHOLD_US,
            "compute_small: task exceeded threshold, consider compute_medium! or compute_large!"
        );
    }
}

/// 验证 `compute_medium!` 任务的实际耗时。
///
/// - 耗时 < 100 μs → 建议改用 `compute_small!`
/// - 耗时 > 1 ms   → 建议改用 `compute_large!`
pub fn validate_medium(elapsed: Duration) {
    let us = elapsed.as_micros() as u64;
    if us < SMALL_THRESHOLD_US {
        MEDIUM_MISCLASSIFIED.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            elapsed_us = us,
            threshold_us = SMALL_THRESHOLD_US,
            "compute_medium: task is too fast, consider compute_small!"
        );
    } else if us > MEDIUM_THRESHOLD_US {
        MEDIUM_MISCLASSIFIED.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            elapsed_us = us,
            threshold_us = MEDIUM_THRESHOLD_US,
            "compute_medium: task is too slow, consider compute_large!"
        );
    }
}

/// 验证 `compute_large!` 任务的实际耗时。
///
/// 耗时 < 1 ms 时建议改用 `compute_medium!` 或 `compute_small!`。
pub fn validate_large(elapsed: Duration) {
    let us = elapsed.as_micros() as u64;
    if us < MEDIUM_THRESHOLD_US {
        LARGE_MISCLASSIFIED.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            elapsed_us = us,
            threshold_us = MEDIUM_THRESHOLD_US,
            "compute_large: task is too fast, consider compute_medium! or compute_small!"
        );
    }
}

/// 读取误分类聚合统计。
///
/// 返回 `(small_misclassified, medium_misclassified, large_misclassified)`。
pub fn get_misclassification_metrics() -> (u64, u64, u64) {
    (
        SMALL_MISCLASSIFIED.load(Ordering::Relaxed),
        MEDIUM_MISCLASSIFIED.load(Ordering::Relaxed),
        LARGE_MISCLASSIFIED.load(Ordering::Relaxed),
    )
}

/// 将所有误分类计数器归零。
pub fn reset_misclassification_metrics() {
    SMALL_MISCLASSIFIED.store(0, Ordering::Relaxed);
    MEDIUM_MISCLASSIFIED.store(0, Ordering::Relaxed);
    LARGE_MISCLASSIFIED.store(0, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_small_ok() {
        reset_misclassification_metrics();
        validate_small(Duration::from_micros(50)); // 在阈值内
        let (s, _, _) = get_misclassification_metrics();
        assert_eq!(s, 0);
    }

    #[test]
    fn test_validate_small_over() {
        reset_misclassification_metrics();
        validate_small(Duration::from_micros(200)); // 超过 100μs
        let (s, _, _) = get_misclassification_metrics();
        assert_eq!(s, 1);
    }

    #[test]
    fn test_validate_medium_too_fast() {
        reset_misclassification_metrics();
        validate_medium(Duration::from_micros(10)); // < 100μs
        let (_, m, _) = get_misclassification_metrics();
        assert_eq!(m, 1);
    }

    #[test]
    fn test_validate_medium_too_slow() {
        reset_misclassification_metrics();
        validate_medium(Duration::from_millis(5)); // > 1ms
        let (_, m, _) = get_misclassification_metrics();
        assert_eq!(m, 1);
    }

    #[test]
    fn test_validate_medium_ok() {
        reset_misclassification_metrics();
        validate_medium(Duration::from_micros(500)); // 在 [100μs, 1ms] 内
        let (_, m, _) = get_misclassification_metrics();
        assert_eq!(m, 0);
    }

    #[test]
    fn test_validate_large_too_fast() {
        reset_misclassification_metrics();
        validate_large(Duration::from_micros(200)); // < 1ms
        let (_, _, l) = get_misclassification_metrics();
        assert_eq!(l, 1);
    }

    #[test]
    fn test_reset() {
        SMALL_MISCLASSIFIED.store(5, Ordering::Relaxed);
        MEDIUM_MISCLASSIFIED.store(3, Ordering::Relaxed);
        LARGE_MISCLASSIFIED.store(7, Ordering::Relaxed);
        reset_misclassification_metrics();
        assert_eq!(get_misclassification_metrics(), (0, 0, 0));
    }
}
