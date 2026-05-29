// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `compute::metrics` —— 计算池运行时观测计数
//!
//! ## 设计意图
//!
//! [`super::ComputePool`] 上的每一次 `execute` / `install` /
//! `execute_scoped*` 调用都会在 [`ComputeMetrics`] 上留下若干原子级
//! 的痕迹——任务计数、当前在跑数、累计耗时、峰值耗时、慢任务计数。
//! 上层（运行时仪表盘、日志、健康检查）可以无锁地读出这些字段，做
//! 出"池是否过载"、"是否存在异常慢任务"等判断。
//!
//! 本文件的核心实现要点：
//!
//! - 所有计数字段都是 `AtomicU64` / `AtomicUsize`，写路径只用
//!   `Ordering::Relaxed`，开销极低；
//! - 峰值耗时的更新用 CAS 循环（`fetch_update`-style），保证多线程下
//!   不会丢更新；
//! - "慢任务"判定阈值统一在 [`SLOW_TASK_THRESHOLD_MS`] 常量里，便于
//!   未来调整；
//! - `Display` 用于人类可读输出，对外字段名固定，不可改动（被测试
//!   断言锁定）。
//!
//! ## 外部契约
//!
//! - `pub struct ComputeMetrics`（字段全部私有，对外只暴露 getter）；
//! - `pub fn new` / `pub fn default` / `pub fn record_task_start` /
//!   `pub fn record_task_completion(Duration)` / 各 getter / `reset`；
//! - `impl Display`：输出格式必须包含字段名 `tasks_total:` /
//!   `tasks_active:` / `avg_duration_ms:` / `max_duration_ms:` /
//!   `slow_tasks:`。

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

// ============================================================================
// 内部常量
// ============================================================================

/// 慢任务的判定阈值（毫秒）。**严格大于**该值才计为慢任务。
const SLOW_TASK_THRESHOLD_MS: u128 = 100;

// ============================================================================
// ComputeMetrics
// ============================================================================

/// 共享的计算池观测计数。
///
/// 所有字段都用原子类型，可被多个 `ComputePool::clone()` 安全并发更
/// 新。本结构体不实现 `Clone`，因为"共享同一份计数"语义只能通过
/// `Arc<ComputeMetrics>` 表达。
#[derive(Debug)]
pub struct ComputeMetrics {
    /// 累计完成的任务数。
    tasks_total: AtomicU64,

    /// 当前正在运行的任务数。
    tasks_active: AtomicUsize,

    /// 累计耗时（μs）。除以 `tasks_total` 得到平均耗时。
    total_compute_time_us: AtomicU64,

    /// 单次任务的峰值耗时（μs）。
    max_task_duration_us: AtomicU64,

    /// 慢任务数（> `SLOW_TASK_THRESHOLD_MS` ms）。
    slow_tasks: AtomicU64,
}

impl ComputeMetrics {
    /// 构造一份全零计数器。
    pub fn new() -> Self {
        Self {
            tasks_total: AtomicU64::new(0),
            tasks_active: AtomicUsize::new(0),
            total_compute_time_us: AtomicU64::new(0),
            max_task_duration_us: AtomicU64::new(0),
            slow_tasks: AtomicU64::new(0),
        }
    }

    // ------------------------------------------------------------------
    // 写路径
    // ------------------------------------------------------------------

    /// 标记一个任务**已开始执行**。
    ///
    /// 仅自增 `tasks_active`；`tasks_total` 在完成时才加，从而保证
    /// "active + total" 不会出现瞬时双计。
    pub fn record_task_start(&self) {
        self.tasks_active.fetch_add(1, Ordering::Relaxed);
    }

    /// 标记一个任务**已完成**，同时合入耗时统计。
    ///
    /// ## 实现细节
    ///
    /// 1. `tasks_active -= 1`；
    /// 2. `tasks_total += 1`；
    /// 3. 把 `duration` 限幅在 `u64::MAX μs` 之内累加到
    ///    `total_compute_time_us`，避免 `as u64` 溢出；
    /// 4. 用 CAS 循环更新 `max_task_duration_us`；
    /// 5. 若 `duration > 100 ms`，`slow_tasks += 1`。
    pub fn record_task_completion(&self, duration: Duration) {
        self.tasks_active.fetch_sub(1, Ordering::Relaxed);
        self.tasks_total.fetch_add(1, Ordering::Relaxed);

        let duration_us = clamp_duration_to_u64_us(duration);
        self.total_compute_time_us
            .fetch_add(duration_us, Ordering::Relaxed);

        update_max_atomic(&self.max_task_duration_us, duration_us);

        if duration.as_millis() > SLOW_TASK_THRESHOLD_MS {
            self.slow_tasks.fetch_add(1, Ordering::Relaxed);
        }
    }

    // ------------------------------------------------------------------
    // 读路径
    // ------------------------------------------------------------------

    /// 累计已完成任务数。
    pub fn tasks_total(&self) -> u64 {
        self.tasks_total.load(Ordering::Relaxed)
    }

    /// 当前正在跑的任务数。
    pub fn tasks_active(&self) -> usize {
        self.tasks_active.load(Ordering::Relaxed)
    }

    /// 平均任务耗时（μs）。零任务时返回 `0.0`。
    pub fn avg_task_duration_us(&self) -> f64 {
        let total = self.tasks_total();
        if total == 0 {
            return 0.0;
        }
        let sum = self.total_compute_time_us.load(Ordering::Relaxed);
        sum as f64 / total as f64
    }

    /// 峰值任务耗时（μs）。
    pub fn max_task_duration_us(&self) -> u64 {
        self.max_task_duration_us.load(Ordering::Relaxed)
    }

    /// 慢任务计数（耗时 > 100 ms 的任务数量）。
    pub fn slow_tasks(&self) -> u64 {
        self.slow_tasks.load(Ordering::Relaxed)
    }

    // ------------------------------------------------------------------
    // 维护
    // ------------------------------------------------------------------

    /// 把所有计数字段复位为 0。仅在测试 / 诊断场景使用。
    pub fn reset(&self) {
        self.tasks_total.store(0, Ordering::Relaxed);
        self.tasks_active.store(0, Ordering::Relaxed);
        self.total_compute_time_us.store(0, Ordering::Relaxed);
        self.max_task_duration_us.store(0, Ordering::Relaxed);
        self.slow_tasks.store(0, Ordering::Relaxed);
    }
}

impl Default for ComputeMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ComputeMetrics {
    /// 人类可读输出，字段名固定不变（被测试锁定）。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ComputeMetrics {{ tasks_total: {}, tasks_active: {}, avg_duration_ms: {:.2}, max_duration_ms: {:.2}, slow_tasks: {} }}",
            self.tasks_total(),
            self.tasks_active(),
            self.avg_task_duration_us() / 1000.0,
            self.max_task_duration_us() as f64 / 1000.0,
            self.slow_tasks(),
        )
    }
}

// ============================================================================
// 私有 helper
// ============================================================================

/// 把 `Duration` 限幅在 `u64::MAX μs` 之内转成 `u64`。
fn clamp_duration_to_u64_us(d: Duration) -> u64 {
    let micros = d.as_micros();
    micros.min(u64::MAX as u128) as u64
}

/// CAS 循环把 `target` 抬到至少 `candidate`。
///
/// 该 helper 抽出来主要为两点：(1) 可在测试中独立验证；(2) 让
/// `record_task_completion` 主体更易读。
fn update_max_atomic(target: &AtomicU64, candidate: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while candidate > current {
        match target.compare_exchange_weak(
            current,
            candidate,
            Ordering::SeqCst,
            Ordering::Relaxed,
        ) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

// ============================================================================
// 单元测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ------------------------------------------------------------------
    // 初值 / Default / Clone-style
    // ------------------------------------------------------------------

    /// 全零初值。
    #[test]
    fn new_metrics_are_all_zero() {
        let m = ComputeMetrics::new();
        assert_eq!(m.tasks_total(), 0);
        assert_eq!(m.tasks_active(), 0);
        assert_eq!(m.avg_task_duration_us(), 0.0);
        assert_eq!(m.max_task_duration_us(), 0);
        assert_eq!(m.slow_tasks(), 0);
    }

    /// `Default` 等价于 `new`。
    #[test]
    fn default_matches_new() {
        let a = ComputeMetrics::new();
        let b = ComputeMetrics::default();
        assert_eq!(a.tasks_total(), b.tasks_total());
        assert_eq!(a.tasks_active(), b.tasks_active());
    }

    // ------------------------------------------------------------------
    // 写路径
    // ------------------------------------------------------------------

    /// `record_task_start` 自增 active 计数；`record_task_completion`
    /// 配对自减 active 并自增 total。
    #[test]
    fn start_and_completion_balance_active_total() {
        let m = ComputeMetrics::new();
        m.record_task_start();
        m.record_task_start();
        assert_eq!(m.tasks_active(), 2);

        m.record_task_completion(Duration::from_millis(10));
        assert_eq!(m.tasks_active(), 1);
        assert_eq!(m.tasks_total(), 1);

        m.record_task_completion(Duration::from_millis(20));
        assert_eq!(m.tasks_active(), 0);
        assert_eq!(m.tasks_total(), 2);
    }

    // ------------------------------------------------------------------
    // 慢任务阈值（严格大于 100 ms）
    // ------------------------------------------------------------------

    /// 99 ms 不计入慢任务。
    #[test]
    fn fast_task_does_not_count_as_slow() {
        let m = ComputeMetrics::new();
        m.record_task_start();
        m.record_task_completion(Duration::from_millis(99));
        assert_eq!(m.slow_tasks(), 0);
    }

    /// 恰好 100 ms 不计入慢任务（阈值用严格大于）。
    #[test]
    fn boundary_100ms_is_not_slow() {
        let m = ComputeMetrics::new();
        m.record_task_start();
        m.record_task_completion(Duration::from_millis(100));
        assert_eq!(m.slow_tasks(), 0);
    }

    /// 101 ms 计入慢任务。
    #[test]
    fn task_over_100ms_counts_as_slow() {
        let m = ComputeMetrics::new();
        m.record_task_start();
        m.record_task_completion(Duration::from_millis(101));
        assert_eq!(m.slow_tasks(), 1);
    }

    /// 多个慢任务累加。
    #[test]
    fn slow_tasks_accumulate() {
        let m = ComputeMetrics::new();
        for _ in 0..5 {
            m.record_task_start();
            m.record_task_completion(Duration::from_secs(1));
        }
        assert_eq!(m.slow_tasks(), 5);
    }

    // ------------------------------------------------------------------
    // 峰值耗时
    // ------------------------------------------------------------------

    /// 峰值只朝上调，较小值不会替换。
    #[test]
    fn max_duration_tracks_running_max() {
        let m = ComputeMetrics::new();
        for &us in &[100u64, 50, 500, 200] {
            m.record_task_start();
            m.record_task_completion(Duration::from_micros(us));
        }
        assert_eq!(m.max_task_duration_us(), 500);
    }

    /// 直接验证 helper `update_max_atomic` 的行为。
    #[test]
    fn update_max_atomic_helper_behaviour() {
        let cell = AtomicU64::new(0);
        update_max_atomic(&cell, 10);
        assert_eq!(cell.load(Ordering::Relaxed), 10);
        update_max_atomic(&cell, 5);
        assert_eq!(cell.load(Ordering::Relaxed), 10);
        update_max_atomic(&cell, 100);
        assert_eq!(cell.load(Ordering::Relaxed), 100);
    }

    // ------------------------------------------------------------------
    // 平均耗时
    // ------------------------------------------------------------------

    /// 零任务时返回 0.0。
    #[test]
    fn avg_zero_when_no_tasks() {
        let m = ComputeMetrics::new();
        assert_eq!(m.avg_task_duration_us(), 0.0);
    }

    /// 平均值 = sum / count。
    #[test]
    fn avg_is_total_over_count() {
        let m = ComputeMetrics::new();
        m.record_task_start();
        m.record_task_completion(Duration::from_micros(100));
        m.record_task_start();
        m.record_task_completion(Duration::from_micros(300));
        let avg = m.avg_task_duration_us();
        assert!(
            (avg - 200.0).abs() < 0.1,
            "期望 ≈200，实际 {avg}"
        );
    }

    // ------------------------------------------------------------------
    // clamp_duration_to_u64_us
    // ------------------------------------------------------------------

    /// 小于 u64::MAX 时按 as u64 转换。
    #[test]
    fn clamp_duration_normal_value() {
        assert_eq!(
            clamp_duration_to_u64_us(Duration::from_micros(1_234_567)),
            1_234_567
        );
    }

    /// `Duration::MAX` 应被限幅到 `u64::MAX`，而不是溢出。
    #[test]
    fn clamp_duration_saturates_to_u64_max() {
        let saturated = clamp_duration_to_u64_us(Duration::MAX);
        assert_eq!(saturated, u64::MAX);
    }

    // ------------------------------------------------------------------
    // reset
    // ------------------------------------------------------------------

    /// 累积一些数据后 reset，所有字段回到 0。
    #[test]
    fn reset_clears_all_fields() {
        let m = ComputeMetrics::new();
        m.record_task_start();
        m.record_task_completion(Duration::from_secs(1));
        m.record_task_start();
        m.record_task_completion(Duration::from_millis(200));
        assert!(m.tasks_total() > 0 && m.slow_tasks() > 0 && m.max_task_duration_us() > 0);
        m.reset();
        assert_eq!(m.tasks_total(), 0);
        assert_eq!(m.tasks_active(), 0);
        assert_eq!(m.max_task_duration_us(), 0);
        assert_eq!(m.slow_tasks(), 0);
        assert_eq!(m.avg_task_duration_us(), 0.0);
    }

    // ------------------------------------------------------------------
    // Display
    // ------------------------------------------------------------------

    /// Display 输出必须含全部字段名（被运维 / 日志依赖）。
    #[test]
    fn display_contains_all_field_labels() {
        let m = ComputeMetrics::new();
        m.record_task_start();
        m.record_task_completion(Duration::from_millis(50));
        let s = format!("{m}");
        for tag in [
            "tasks_total:",
            "tasks_active:",
            "avg_duration_ms:",
            "max_duration_ms:",
            "slow_tasks:",
        ] {
            assert!(s.contains(tag), "Display 缺少 {tag}: {s}");
        }
    }

    /// 全零状态下 Display 也不能 panic。
    #[test]
    fn display_zero_state_does_not_panic() {
        let m = ComputeMetrics::new();
        let _ = format!("{m}");
    }

    // ------------------------------------------------------------------
    // 并发安全
    // ------------------------------------------------------------------

    /// 多线程并发更新后，`tasks_total` 等于线程数 × 每线程任务数。
    #[test]
    fn concurrent_updates_are_consistent() {
        use std::sync::Arc;
        use std::thread;

        let m = Arc::new(ComputeMetrics::new());
        let n_threads = 8usize;
        let tasks_per_thread = 100usize;

        let handles: Vec<_> = (0..n_threads)
            .map(|_| {
                let mc = m.clone();
                thread::spawn(move || {
                    for _ in 0..tasks_per_thread {
                        mc.record_task_start();
                        mc.record_task_completion(Duration::from_micros(10));
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(m.tasks_total(), (n_threads * tasks_per_thread) as u64);
        assert_eq!(m.tasks_active(), 0);
    }

    // ------------------------------------------------------------------
    // === lib-copy 标准契约测试（原样保留） ============================
    // ------------------------------------------------------------------

    #[test]
    fn test_metrics_recording() {
        let metrics = ComputeMetrics::new();

        assert_eq!(metrics.tasks_total(), 0);
        assert_eq!(metrics.tasks_active(), 0);

        metrics.record_task_start();
        assert_eq!(metrics.tasks_active(), 1);

        metrics.record_task_completion(Duration::from_millis(50));
        assert_eq!(metrics.tasks_active(), 0);
        assert_eq!(metrics.tasks_total(), 1);
        assert_eq!(metrics.slow_tasks(), 0);

        metrics.record_task_start();
        metrics.record_task_completion(Duration::from_millis(150));
        assert_eq!(metrics.tasks_total(), 2);
        assert_eq!(metrics.slow_tasks(), 1);
    }

    #[test]
    fn test_metrics_reset() {
        let metrics = ComputeMetrics::new();

        metrics.record_task_start();
        metrics.record_task_completion(Duration::from_millis(50));
        assert_eq!(metrics.tasks_total(), 1);

        metrics.reset();
        assert_eq!(metrics.tasks_total(), 0);
        assert_eq!(metrics.tasks_active(), 0);
    }
}
