// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-License-Identifier: Apache-2.0
//
// API-compatible implementation based on the public interfaces and behavioral
// contracts of NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
// Implementation rewritten by PAGODA.

//! # 滑动窗口均值（RunningMean）
//!
//! ## 设计意图
//! 在固定长度的滑动窗口上以常数时间维护算术平均值。典型用途是对一连串度量
//! （时延、吞吐等）做平滑，只关心最近 `max_size` 个样本。
//!
//! ## 外部契约
//! 暴露泛型类型 [`RunningMean`]，类型参数 `T` 的 trait bound 与方法面必须与上游一致：
//! - `new(max_size: u16) -> Self`
//! - `push(&mut self, value: T)`：窗口已满时挤出最旧样本
//! - `mean(&self) -> T`：窗口为空时返回 `T::default()`，否则为 `sum / count`
//! - `len(&self) -> usize` / `is_empty(&self) -> bool`
//! - `clear(&mut self)`
//!
//! ## 实现要点
//! 用一个增量累加器 `accumulator` 跟踪窗口内样本之和，配合 [`VecDeque`] 充当
//! 先进先出的环形缓冲。入队即把样本并入累加器；窗口越界时先把队首样本从累加器中
//! 扣除再丢弃。如此 `push` 与 `mean` 均为 O(1)，不需要每次遍历窗口求和。

use std::collections::VecDeque;
use std::ops::{Add, Div, Sub};

// === SECTION: 类型定义 ===

/// 样本类型需要满足的运算约束：可复制、支持加 / 减 / 除、有默认值，且能由窗口长度
/// （`u16`）转换而来以充当除数。
pub trait Sample:
    Copy + Add<Output = Self> + Sub<Output = Self> + Div<Output = Self> + Default + From<u16>
{
}

impl<T> Sample for T where
    T: Copy + Add<Output = T> + Sub<Output = T> + Div<Output = T> + Default + From<u16>
{
}

/// ## 外部契约
/// 固定窗口的滑动均值计算器。窗口容量在构造时确定，超出后最旧样本被挤出。
#[derive(Debug, Clone)]
pub struct RunningMean<T>
where
    T: Sample,
{
    /// 窗口容量上限。
    capacity: u16,
    /// 当前窗口内样本之和（增量维护，避免重复求和）。
    accumulator: T,
    /// 按到达顺序保存的窗口样本，队首为最旧。
    window: VecDeque<T>,
}

// === SECTION: 构造与变更 ===

impl<T> RunningMean<T>
where
    T: Sample,
{
    /// 以给定窗口容量构造一个空的滑动均值。
    pub fn new(max_size: u16) -> Self {
        Self {
            capacity: max_size,
            accumulator: T::default(),
            window: VecDeque::with_capacity(max_size as usize),
        }
    }

    /// 追加一个样本。窗口已达容量时，先移除并扣除最旧样本，再纳入新样本。
    pub fn push(&mut self, value: T) {
        if self.window.len() >= self.capacity as usize {
            if let Some(oldest) = self.window.pop_front() {
                self.accumulator = self.accumulator - oldest;
            }
        }
        self.window.push_back(value);
        self.accumulator = self.accumulator + value;
    }

    /// 清空窗口，累加器归零。
    pub fn clear(&mut self) {
        self.window.clear();
        self.accumulator = T::default();
    }

    // === SECTION: 查询 ===

    /// 返回当前窗口的算术平均值；窗口为空时返回 `T::default()`。
    pub fn mean(&self) -> T {
        let count = self.window.len();
        if count == 0 {
            T::default()
        } else {
            self.accumulator / T::from(count as u16)
        }
    }

    /// 当前窗口内样本个数。
    pub fn len(&self) -> usize {
        self.window.len()
    }

    /// 窗口是否为空。
    pub fn is_empty(&self) -> bool {
        self.window.is_empty()
    }
}

// === SECTION: 测试 ===

#[cfg(test)]
mod tests {
    use super::RunningMean;

    #[test]
    fn empty_window_reports_default_mean() {
        // ## 测试过程
        // 新建一个容量为 4 的窗口，不放任何样本，立即查询。
        // ## 意义
        // 验证空窗口契约：mean 返回 T::default()（这里 f64 为 0.0），len 为 0、is_empty 为真。
        let rm: RunningMean<f64> = RunningMean::new(4);
        assert_eq!(rm.mean(), 0.0);
        assert_eq!(rm.len(), 0);
        assert!(rm.is_empty());
    }

    #[test]
    fn mean_tracks_samples_within_capacity() {
        // ## 测试过程
        // 容量 4 的窗口依次放入 2、4、6 三个样本（未越界）。
        // ## 意义
        // 验证未触发挤出时，均值等于全部样本的算术平均。
        let mut rm: RunningMean<f64> = RunningMean::new(4);
        for v in [2.0, 4.0, 6.0] {
            rm.push(v);
        }
        assert_eq!(rm.len(), 3);
        assert_eq!(rm.mean(), 4.0);
    }

    #[test]
    fn oldest_sample_is_evicted_when_full() {
        // ## 测试过程
        // 容量 3 的窗口放入 1、2、3 后再放 4，触发最旧样本被挤出。
        // ## 意义
        // 验证滑动行为：窗口稳定在容量上限，均值只统计最近 capacity 个样本（2+3+4）/3。
        let mut rm: RunningMean<f64> = RunningMean::new(3);
        for v in [1.0, 2.0, 3.0, 4.0] {
            rm.push(v);
        }
        assert_eq!(rm.len(), 3);
        assert_eq!(rm.mean(), 3.0);
    }

    #[test]
    fn clear_resets_window_and_accumulator() {
        // ## 测试过程
        // 放入若干样本后调用 clear，再查询，并继续放入新样本。
        // ## 意义
        // 验证 clear 既清空窗口也把累加器归零，清空后能从干净状态重新累计均值。
        let mut rm: RunningMean<i64> = RunningMean::new(2);
        rm.push(10);
        rm.push(20);
        rm.clear();
        assert!(rm.is_empty());
        assert_eq!(rm.mean(), 0);
        rm.push(8);
        assert_eq!(rm.mean(), 8);
    }

    #[test]
    fn integer_mean_truncates_toward_zero() {
        // ## 测试过程
        // 用整型样本 1、2 求均值（整数除法）。
        // ## 意义
        // 验证泛型对整型同样适用，且均值沿用 T 的除法语义（3 / 2 截断为 1）。
        let mut rm: RunningMean<i64> = RunningMean::new(4);
        rm.push(1);
        rm.push(2);
        assert_eq!(rm.mean(), 1);
    }
}
