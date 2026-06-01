// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `compute::validation` —— 计算任务大小分类的运行期校验
//!
//! ## 设计意图
//!
//! `compute_small!` / `compute_medium!` / `compute_large!` 三个宏依赖
//! 调用方对任务耗时**预先**正确分类：
//!
//! - small：< 100 μs，直接 inline 跑；
//! - medium：100 μs ~ 1 ms，走 `block_in_place` 或 offload；
//! - large：> 1 ms，offload 到 Rayon 池。
//!
//! 分类错误会破坏运行时调度（例如 medium 实际耗时 50 ms，会阻塞
//! Tokio worker）。本模块在 `compute-validation` feature 下提供"事后
//! 计时 + 越界计数 + 警告日志"的机制，帮助开发者发现误判。
//!
//! ## 外部契约
//!
//! 仅在 `#[cfg(feature = "compute-validation")]` 下编译：
//!
//! - 常量：`SMALL_THRESHOLD_US`、`MEDIUM_THRESHOLD_US`；
//! - 函数：`validate_small`、`validate_medium`、`validate_large`、
//!   `get_misclassification_metrics`、`reset_misclassification_metrics`。
//!
//! 这些项被 `compute/macros.rs` 内的宏直接引用，签名 / 阈值取值不得改动。

#![cfg(feature = "compute-validation")]

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::warn;

// ============================================================================
// 公开阈值（μs）
// ============================================================================

/// "小任务"上限：100 μs。超过则建议改用 `compute_medium!`。
pub const SMALL_THRESHOLD_US: u64 = 100;

/// "中任务"上限：1 000 μs（= 1 ms）。超过则建议改用 `compute_large!`。
pub const MEDIUM_THRESHOLD_US: u64 = 1000;

// ============================================================================
// 私有：分类越界计数器
//
// 这些计数器是进程级共享状态——所有宏调用最终汇聚到同一组 atomic，
// 由 `get_misclassification_metrics` 读出，便于在测试 / 诊断里观察。
// ============================================================================

static SMALL_MISCLASSIFIED: AtomicU64 = AtomicU64::new(0);
static MEDIUM_MISCLASSIFIED: AtomicU64 = AtomicU64::new(0);
static LARGE_MISCLASSIFIED: AtomicU64 = AtomicU64::new(0);

// ============================================================================
// 私有 helper：把"判断 + 计数 + 警告"合并到一处
// ============================================================================

/// 表示一次校验的判定结果。
enum ClassificationVerdict {
    /// 实测耗时落在期望区间内。
    Ok,
    /// 实测耗时超出预期；需要更换为更大档位的宏。
    TooSlow {
        threshold_us: u64,
        suggested_bucket: &'static str,
    },
    /// 实测耗时小于该档位的下限；调用方应改用更小档位的宏。
    TooFast {
        threshold_us: u64,
        suggested_bucket: &'static str,
    },
}

/// 根据判定结果累加对应计数器并打日志。`counter` 由调用方按档位传入。
fn record_verdict(
    bucket_label: &'static str,
    elapsed_us: u64,
    verdict: ClassificationVerdict,
    counter: &AtomicU64,
) {
    match verdict {
        ClassificationVerdict::Ok => {}
        ClassificationVerdict::TooSlow {
            threshold_us,
            suggested_bucket,
        } => {
            counter.fetch_add(1, Ordering::Relaxed);
            warn!(
                task_duration_us = elapsed_us,
                threshold_us = threshold_us,
                "{bucket_label} task exceeded threshold. Consider using {suggested_bucket}",
            );
        }
        ClassificationVerdict::TooFast {
            threshold_us,
            suggested_bucket,
        } => {
            counter.fetch_add(1, Ordering::Relaxed);
            warn!(
                task_duration_us = elapsed_us,
                threshold_us = threshold_us,
                "{bucket_label} task below threshold. Consider using {suggested_bucket}",
            );
        }
    }
}

// ============================================================================
// 公开 API：三档校验函数
// ============================================================================

/// 校验"标为 small 的任务"实际耗时是否 ≤ [`SMALL_THRESHOLD_US`]。
///
/// 越界 → 累加 SMALL 计数器并 warn 一条建议升档的日志。
pub fn validate_small(elapsed: Duration) {
    let micros = elapsed.as_micros().min(u64::MAX as u128) as u64;
    let verdict = if micros > SMALL_THRESHOLD_US {
        ClassificationVerdict::TooSlow {
            threshold_us: SMALL_THRESHOLD_US,
            suggested_bucket: "compute_medium!",
        }
    } else {
        ClassificationVerdict::Ok
    };
    record_verdict("compute_small!", micros, verdict, &SMALL_MISCLASSIFIED);
}

/// 校验"标为 medium 的任务"实际耗时落在
/// [`SMALL_THRESHOLD_US`, `MEDIUM_THRESHOLD_US`] 之间。
///
/// - 实测 < small 阈值 → 建议改 `compute_small!`；
/// - 实测 > medium 阈值 → 建议改 `compute_large!`；
/// - 区间内 → 不操作。
pub fn validate_medium(elapsed: Duration) {
    let micros = elapsed.as_micros().min(u64::MAX as u128) as u64;
    let verdict = if micros < SMALL_THRESHOLD_US {
        ClassificationVerdict::TooFast {
            threshold_us: SMALL_THRESHOLD_US,
            suggested_bucket: "compute_small!",
        }
    } else if micros > MEDIUM_THRESHOLD_US {
        ClassificationVerdict::TooSlow {
            threshold_us: MEDIUM_THRESHOLD_US,
            suggested_bucket: "compute_large!",
        }
    } else {
        ClassificationVerdict::Ok
    };
    record_verdict("compute_medium!", micros, verdict, &MEDIUM_MISCLASSIFIED);
}

/// 校验"标为 large 的任务"实际耗时是否 ≥ [`MEDIUM_THRESHOLD_US`]。
///
/// 实测过短意味着 offload 到 Rayon 的开销与任务本身相当，建议降档。
pub fn validate_large(elapsed: Duration) {
    let micros = elapsed.as_micros().min(u64::MAX as u128) as u64;
    let verdict = if micros < MEDIUM_THRESHOLD_US {
        ClassificationVerdict::TooFast {
            threshold_us: MEDIUM_THRESHOLD_US,
            suggested_bucket: "compute_medium! or compute_small!",
        }
    } else {
        ClassificationVerdict::Ok
    };
    record_verdict("compute_large!", micros, verdict, &LARGE_MISCLASSIFIED);
}

/// 一次性读出 `(small, medium, large)` 三个计数器的当前值。
pub fn get_misclassification_metrics() -> (u64, u64, u64) {
    (
        SMALL_MISCLASSIFIED.load(Ordering::Relaxed),
        MEDIUM_MISCLASSIFIED.load(Ordering::Relaxed),
        LARGE_MISCLASSIFIED.load(Ordering::Relaxed),
    )
}

/// 把三个计数器复位为 0。通常仅在测试或调试场景下调用。
pub fn reset_misclassification_metrics() {
    SMALL_MISCLASSIFIED.store(0, Ordering::Relaxed);
    MEDIUM_MISCLASSIFIED.store(0, Ordering::Relaxed);
    LARGE_MISCLASSIFIED.store(0, Ordering::Relaxed);
}

// ============================================================================
// 单元测试
//
// 注意：本模块的状态是进程级 atomic，所以并发跑测试会互相干扰。每个
// 测试都先用 `reset_misclassification_metrics()` 清零并通过 `env_lock`
// 进程内互斥。
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn env_lock() -> &'static std::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
    }

    /// ## 测试过程
    /// 1. 持锁 + 清零；
    /// 2. 用一个 50 μs 的 Duration 调 `validate_small`；
    /// 3. 断言三个计数器仍为 (0, 0, 0)。
    ///
    /// ## 意义
    /// 锁定 small 档在合法区间内不打日志、不累加计数的契约。
    #[test]
    fn validate_small_ok_when_under_threshold() {
        let _g = env_lock().lock().unwrap();
        reset_misclassification_metrics();
        validate_small(Duration::from_micros(50));
        assert_eq!(get_misclassification_metrics(), (0, 0, 0));
    }

    /// ## 测试过程
    /// 用 200 μs 调 `validate_small`，断言 SMALL 计数器 +1。
    #[test]
    fn validate_small_counts_when_over_threshold() {
        let _g = env_lock().lock().unwrap();
        reset_misclassification_metrics();
        validate_small(Duration::from_micros(200));
        assert_eq!(get_misclassification_metrics(), (1, 0, 0));
    }

    /// ## 测试过程
    /// 用 500 μs（落在 medium 区间）调 `validate_medium`，无计数。
    #[test]
    fn validate_medium_ok_in_band() {
        let _g = env_lock().lock().unwrap();
        reset_misclassification_metrics();
        validate_medium(Duration::from_micros(500));
        assert_eq!(get_misclassification_metrics(), (0, 0, 0));
    }

    /// ## 测试过程
    /// 用 10 μs（< small 阈值）调 `validate_medium`，断言 MEDIUM +1。
    #[test]
    fn validate_medium_counts_when_too_fast() {
        let _g = env_lock().lock().unwrap();
        reset_misclassification_metrics();
        validate_medium(Duration::from_micros(10));
        assert_eq!(get_misclassification_metrics(), (0, 1, 0));
    }

    /// ## 测试过程
    /// 用 5 ms（> medium 阈值）调 `validate_medium`，断言 MEDIUM +1。
    #[test]
    fn validate_medium_counts_when_too_slow() {
        let _g = env_lock().lock().unwrap();
        reset_misclassification_metrics();
        validate_medium(Duration::from_millis(5));
        assert_eq!(get_misclassification_metrics(), (0, 1, 0));
    }

    /// ## 测试过程
    /// 用 5 ms（≥ medium 阈值）调 `validate_large`，无计数。
    #[test]
    fn validate_large_ok_when_above_threshold() {
        let _g = env_lock().lock().unwrap();
        reset_misclassification_metrics();
        validate_large(Duration::from_millis(5));
        assert_eq!(get_misclassification_metrics(), (0, 0, 0));
    }

    /// ## 测试过程
    /// 用 200 μs（< medium 阈值）调 `validate_large`，断言 LARGE +1。
    #[test]
    fn validate_large_counts_when_too_fast() {
        let _g = env_lock().lock().unwrap();
        reset_misclassification_metrics();
        validate_large(Duration::from_micros(200));
        assert_eq!(get_misclassification_metrics(), (0, 0, 1));
    }

    /// ## 测试过程
    /// 连续多次越界调用同一档位，断言计数器累加。
    ///
    /// ## 意义
    /// 验证计数器是"自上次 reset 至今"的累计值，不会自动衰减。
    #[test]
    fn metrics_accumulate_across_calls() {
        let _g = env_lock().lock().unwrap();
        reset_misclassification_metrics();
        for _ in 0..5 {
            validate_small(Duration::from_micros(500));
        }
        assert_eq!(get_misclassification_metrics().0, 5);
    }

    /// ## 测试过程
    /// 累积一些计数后调 reset，断言全部归零。
    #[test]
    fn reset_clears_all_counters() {
        let _g = env_lock().lock().unwrap();
        reset_misclassification_metrics();
        validate_small(Duration::from_micros(500));
        validate_medium(Duration::from_micros(5));
        validate_large(Duration::from_micros(200));
        assert_ne!(get_misclassification_metrics(), (0, 0, 0));
        reset_misclassification_metrics();
        assert_eq!(get_misclassification_metrics(), (0, 0, 0));
    }

    /// ## 测试过程
    /// 阈值常量必须保持当前数值（100 / 1000 μs），用作回归保险。
    #[test]
    fn threshold_constants_have_documented_values() {
        assert_eq!(SMALL_THRESHOLD_US, 100);
        assert_eq!(MEDIUM_THRESHOLD_US, 1000);
    }
}
