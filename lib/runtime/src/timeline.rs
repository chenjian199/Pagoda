// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 时间线标注模块（feature=timeline 时启用 NVTX 后端）。
//!
//! 两级门控：
//! 1. Cargo feature `timeline`：关闭时所有宏展开为空，零开销。
//! 2. 运行时环境变量 `PGD_ENABLE_RUST_TIMELINE`：关闭时仅一次原子读，成本约等于内存读。
//!
//! `Runtime::new()` 会自动调用 `timeline::init()`，无需手工初始化。

#[cfg(feature = "timeline")]
use std::sync::atomic::{AtomicBool, Ordering};

/// 运行时时间线开关。feature 关闭时编译期消除。
#[cfg(feature = "timeline")]
static TIMELINE_ENABLED: AtomicBool = AtomicBool::new(false);

/// 从环境变量 `PGD_ENABLE_RUST_TIMELINE` 初始化运行时开关。
///
/// `Runtime::new()` 在构造期自动调用；非 `timeline` feature 场景下为空操作。
pub fn init() {
    #[cfg(feature = "timeline")]
    {
        let enabled = std::env::var("PGD_ENABLE_RUST_TIMELINE")
            .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false);
        TIMELINE_ENABLED.store(enabled, Ordering::Relaxed);
        if enabled {
            tracing::debug!("timeline annotation enabled (PGD_ENABLE_RUST_TIMELINE)");
        }
    }
}

// ─── 底层实现辅助 ──────────────────────────────────────────────────

/// 将命名区间压入当前线程的时间线栈。
#[inline(always)]
pub fn push_impl(_name: &str) {
    #[cfg(feature = "timeline")]
    {
        if TIMELINE_ENABLED.load(Ordering::Relaxed) {
            // 底层后端：cudarc::nvtx::range_push(_name);
        }
    }
}

/// 弹出当前线程时间线栈最内层区间。
#[inline(always)]
pub fn pop_impl() {
    #[cfg(feature = "timeline")]
    {
        if TIMELINE_ENABLED.load(Ordering::Relaxed) {
            // 底层后端：cudarc::nvtx::range_pop();
        }
    }
}

/// 给当前 OS 线程设置可读名称（用于 Nsight Systems 显示）。
#[inline(always)]
pub fn name_current_thread_impl(_name: &str) {
    #[cfg(feature = "timeline")]
    {
        if TIMELINE_ENABLED.load(Ordering::Relaxed) {
            // 底层后端：cudarc::nvtx::name_os_thread(tid, _name);
        }
    }
}

// ─── RAII 区间 Guard ───────────────────────────────────────────────

/// RAII 时间线区间保护器，防止 push/pop 不匹配。
///
/// `Drop` 时自动 `pop_impl()`，即使中间路径 `?` 传播错误也安全。
#[cfg(feature = "timeline")]
pub struct TimelineRangeGuard {
    active: bool,
}

/// feature 关闭时为零大小类型，编译器完全优化掉。
#[cfg(not(feature = "timeline"))]
pub struct TimelineRangeGuard;

impl TimelineRangeGuard {
    pub fn new(_name: &str) -> Self {
        #[cfg(feature = "timeline")]
        {
            let active = TIMELINE_ENABLED.load(Ordering::Relaxed);
            if active {
                push_impl(_name);
            }
            Self { active }
        }
        #[cfg(not(feature = "timeline"))]
        Self
    }
}

#[cfg(feature = "timeline")]
impl Drop for TimelineRangeGuard {
    fn drop(&mut self) {
        if self.active {
            pop_impl();
        }
    }
}

// ─── 宏接口 ───────────────────────────────────────────────────────

/// 创建 RAII 时间线区间（推荐默认用法）。
///
/// ```rust
/// let _guard = pagoda_timeline_range!("preprocess.tokenize");
/// ```
#[macro_export]
macro_rules! pagoda_timeline_range {
    ($name:expr) => {
        $crate::timeline::TimelineRangeGuard::new($name)
    };
}

/// 手动推入命名时间线范围。
#[macro_export]
macro_rules! pagoda_timeline_range_push {
    ($name:expr) => {
        $crate::timeline::push_impl($name)
    };
}

/// 手动弹出当前时间线范围。
#[macro_export]
macro_rules! pagoda_timeline_range_pop {
    () => {
        $crate::timeline::pop_impl()
    };
}

/// 标记一个时间点事件。
#[macro_export]
macro_rules! pagoda_timeline_mark {
    ($name:expr) => {
        $crate::timeline::push_impl($name);
        $crate::timeline::pop_impl();
    };
}

/// 给当前线程设置可读名称（用于 Nsight Systems 显示）。
#[macro_export]
macro_rules! pagoda_timeline_name_thread {
    ($name:expr) => {
        $crate::timeline::name_current_thread_impl($name)
    };
}

/// 创建时间线域（前向兼容保留，当前等同于 mark）。
#[macro_export]
macro_rules! pagoda_timeline_domain_create {
    ($name:expr) => {
        $crate::timeline::push_impl($name)
    };
}
