// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! [`TimelineRangeGuard`] —— RAII 区间守护，确保 push / pop 配对。

#[cfg(feature = "timeline")]
use std::sync::atomic::Ordering;

/// RAII 守护：构造时压入区间，`Drop` 时弹出。
///
/// 通过 [`pagoda_timeline_range!`](crate::pagoda_timeline_range) 构造。手工
/// `push_impl` / `pop_impl` 在提前 `return` 或 `?` 传播错误时容易漏掉 pop，
/// 导致线程局部 range 栈失衡；用本守护可以让区间随作用域自动闭合。
#[cfg(feature = "timeline")]
pub struct TimelineRangeGuard {
    active: bool,
}

/// 未启用 `timeline` feature 时的零大小空守护。
#[cfg(not(feature = "timeline"))]
pub struct TimelineRangeGuard;

impl TimelineRangeGuard {
    #[doc(hidden)]
    pub fn new(name: &str) -> Self {
        #[cfg(feature = "timeline")]
        {
            use crate::timeline::TimelineBackend;
            // 仅在构造时读取一次开关，并记录下来，保证 Drop 行为与构造时一致：
            // 避免在区间存活期间开关被翻转而出现 push/pop 不配对。
            let active = super::TIMELINE_ENABLED.load(Ordering::Relaxed);
            if active {
                super::backends::ActiveBackend::range_push(name);
            }
            return TimelineRangeGuard { active };
        }
        #[cfg(not(feature = "timeline"))]
        {
            let _ = name;
            TimelineRangeGuard
        }
    }
}

#[cfg(feature = "timeline")]
impl Drop for TimelineRangeGuard {
    fn drop(&mut self) {
        use crate::timeline::TimelineBackend;
        if self.active {
            super::backends::ActiveBackend::range_pop();
        }
    }
}

#[cfg(not(feature = "timeline"))]
impl Drop for TimelineRangeGuard {
    fn drop(&mut self) {}
}
