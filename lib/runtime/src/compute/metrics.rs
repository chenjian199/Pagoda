// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! ComputePool 性能指标。
//!
//! 全部字段使用原子操作，`max_task_duration_us` 通过 CAS 循环无锁更新。
//! 高频计算场景下原子操作比 Mutex 的吞吐量高一个数量级。

use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

/// ComputePool 性能指标。
///
/// 统计 Rayon 线程池上任务的执行情况，包含慢任务检测（> 100ms）。
#[derive(Debug)]
pub struct ComputeMetrics {
    /// 累计完成的任务数
    tasks_total: AtomicU64,
    /// 当前正在运行的任务数
    tasks_active: AtomicUsize,
    /// 累计计算时间（微秒）
    total_compute_time_us: AtomicU64,
    /// 单次任务最大耗时（微秒），CAS 无锁更新
    max_task_duration_us: AtomicU64,
    /// 耗时 > 100ms 的任务数
    slow_tasks: AtomicU64,
}

impl ComputeMetrics {
    /// 创建所有计数器归零的指标实例。
    pub fn new() -> Self {
        Self {
            tasks_total: AtomicU64::new(0),
            tasks_active: AtomicUsize::new(0),
            total_compute_time_us: AtomicU64::new(0),
            max_task_duration_us: AtomicU64::new(0),
            slow_tasks: AtomicU64::new(0),
        }
    }

    /// 任务开始时调用：`tasks_active` 加一。
    pub fn record_task_start(&self) {
        self.tasks_active.fetch_add(1, Ordering::Relaxed);
    }

    /// 任务完成时调用：更新全部统计字段。
    ///
    /// - `tasks_active` 减一，`tasks_total` 加一
    /// - 累加 `total_compute_time_us`（saturating 转换防溢出）
    /// - 通过 CAS 循环更新 `max_task_duration_us`
    /// - 耗时超过 100ms 时 `slow_tasks` 加一
    pub fn record_task_completion(&self, duration: Duration) {
        self.tasks_active.fetch_sub(1, Ordering::Relaxed);
        self.tasks_total.fetch_add(1, Ordering::Relaxed);

        let duration_us = u64::try_from(duration.as_micros()).unwrap_or(u64::MAX);
        self.total_compute_time_us
            .fetch_add(duration_us, Ordering::Relaxed);

        // 无锁 CAS 循环更新最大值
        let mut current = self.max_task_duration_us.load(Ordering::Relaxed);
        loop {
            if duration_us <= current {
                break;
            }
            match self.max_task_duration_us.compare_exchange_weak(
                current,
                duration_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }

        if duration > Duration::from_millis(100) {
            self.slow_tasks.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// 累计完成的任务数。
    pub fn tasks_total(&self) -> u64 {
        self.tasks_total.load(Ordering::Relaxed)
    }

    /// 当前正在运行的任务数。
    pub fn tasks_active(&self) -> usize {
        self.tasks_active.load(Ordering::Relaxed)
    }

    /// 平均任务执行时间（微秒）。任务数为零时返回 0.0。
    pub fn avg_task_duration_us(&self) -> f64 {
        let total = self.tasks_total.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        self.total_compute_time_us.load(Ordering::Relaxed) as f64 / total as f64
    }

    /// 单次任务最大耗时（微秒）。
    pub fn max_task_duration_us(&self) -> u64 {
        self.max_task_duration_us.load(Ordering::Relaxed)
    }

    /// 耗时超过 100ms 的任务计数。
    pub fn slow_tasks(&self) -> u64 {
        self.slow_tasks.load(Ordering::Relaxed)
    }

    /// 将所有计数器归零。
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

impl fmt::Display for ComputeMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ComputeMetrics {{ total={}, active={}, avg_us={:.1}, max_us={}, slow={} }}",
            self.tasks_total(),
            self.tasks_active(),
            self.avg_task_duration_us(),
            self.max_task_duration_us(),
            self.slow_tasks(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_metrics_recording() {
        let m = ComputeMetrics::new();
        assert_eq!(m.tasks_total(), 0);
        assert_eq!(m.tasks_active(), 0);

        m.record_task_start();
        assert_eq!(m.tasks_active(), 1);

        m.record_task_start();
        assert_eq!(m.tasks_active(), 2);

        m.record_task_completion(Duration::from_micros(500));
        assert_eq!(m.tasks_active(), 1);
        assert_eq!(m.tasks_total(), 1);
        assert_eq!(m.max_task_duration_us(), 500);
        assert_eq!(m.slow_tasks(), 0);

        // 慢任务（200ms > 100ms）
        m.record_task_completion(Duration::from_millis(200));
        assert_eq!(m.tasks_total(), 2);
        assert_eq!(m.slow_tasks(), 1);
        assert!(m.max_task_duration_us() >= 200_000);
    }

    #[test]
    fn test_metrics_reset() {
        let m = ComputeMetrics::new();
        m.record_task_start();
        m.record_task_completion(Duration::from_millis(50));
        assert_eq!(m.tasks_total(), 1);

        m.reset();
        assert_eq!(m.tasks_total(), 0);
        assert_eq!(m.tasks_active(), 0);
        assert_eq!(m.max_task_duration_us(), 0);
        assert_eq!(m.slow_tasks(), 0);
    }

    #[test]
    fn test_max_duration_cas() {
        let m = ComputeMetrics::new();
        m.record_task_start();
        m.record_task_completion(Duration::from_micros(100));
        m.record_task_start();
        m.record_task_completion(Duration::from_micros(50));
        // 最大值应保持 100μs
        assert_eq!(m.max_task_duration_us(), 100);

        m.record_task_start();
        m.record_task_completion(Duration::from_micros(200));
        assert_eq!(m.max_task_duration_us(), 200);
    }
}
