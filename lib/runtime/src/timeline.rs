// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! NVTX Timeline 标注辅助层（Nsight Systems profiling）
//!
//! ## 设计意图
//! 为 Rust 侧提供与 [`cudarc::nvtx`] 适配的零依赖、零运行时开销的标注入口，
//! 让上层代码可以用统一的宏 / 函数接口描述 NVTX range 与线程名，而是否真正
//! 向 NVTX runtime 发生调用取决于两级门限：
//!
//! | Cargo feature `nvtx` | `PGD_ENABLE_RUST_TIMELINE` env | 效果                                  |
//! |----------------------|----------------------------|-------------------------------------------|
//! | off (default)        | any                        | macros compile to nothing; zero overhead  |
//! | on                   | unset                      | one `Relaxed` load per site (~1 ns)       |
//! | on                   | `1` / `true` / `yes`       | cudarc NVTX calls (~50 ns/annotation)     |
//!
//! ## 外部契约
//! - 公开函数：`init()` / `enabled()` / `push_impl(&str)` / `pop_impl()` /
//!   `name_current_thread_impl(&str)` 与公开类型 `TimelineRangeGuard` 的签名、位置与语义保持不变。
//! - 宏 `pagoda_timeline_push!` / `pagoda_timeline_pop!` / `pagoda_timeline_range!` /
//!   `pagoda_timeline_name_thread!` 的 `macro_rules!` 展开与调用路径保持不变。
//! - 在未启用 `timeline` feature 时，所有调用点都必须被编译为空操作（零开销）。
//!
//! ## 使用示例
//!
//! ```rust,ignore
//! let _r = pagoda_timeline_range!("preprocess.tokenize"); // RAII — 作用域结束时自动 pop
//! pagoda_timeline_push!("codec.encode");
//! pagoda_timeline_pop!();
//! pagoda_timeline_name_thread!("tokio-worker-0");
//! ```
//!
//! ## 构建
//!
//! ```bash
//! cargo build --profile profiling --features nvtx
//! ```
//! 运行时需要 `libnvToolsExt.so`（随 CUDA Toolkit 或 NVHPC 一同提供）。

#[cfg(feature = "timeline")]
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(feature = "timeline")]
static TIMELINE_ENABLED: AtomicBool = AtomicBool::new(false);

// === SECTION: 公开 API ===───────────────────

/// Initialise the NVTX subsystem from the `PGD_ENABLE_RUST_TIMELINE` environment variable.
/// Must be called once at runtime startup before any annotation macros fire.
/// No-op when the `timeline` Cargo feature is off.
pub fn init() {
    #[cfg(feature = "timeline")]
    {
        let enabled = std::env::var("PGD_ENABLE_RUST_TIMELINE")
            .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false);
        TIMELINE_ENABLED.store(enabled, Ordering::Relaxed);
        if enabled {
            tracing::info!("NVTX annotations enabled (PGD_ENABLE_RUST_TIMELINE)");
        }
    }
}

/// Returns `true` when the `timeline` feature is compiled in **and** `PGD_ENABLE_RUST_TIMELINE` is set.
#[inline(always)]
pub fn enabled() -> bool {
    #[cfg(feature = "timeline")]
    {
        return TIMELINE_ENABLED.load(Ordering::Relaxed);
    }
    #[allow(unreachable_code)]
    false
}

/// Push an NVTX range onto the calling thread's stack.
/// No-op (compiled out) when the `timeline` feature is off.
#[inline(always)]
pub fn push_impl(name: &str) {
    #[cfg(feature = "timeline")]
    {
        if TIMELINE_ENABLED.load(Ordering::Relaxed) {
            cudarc::nvtx::result::range_push(name);
        }
    }
    let _ = name;
}

/// Pop the innermost NVTX range from the calling thread's stack.
/// No-op (compiled out) when the `timeline` feature is off.
#[inline(always)]
pub fn pop_impl() {
    #[cfg(feature = "timeline")]
    {
        if TIMELINE_ENABLED.load(Ordering::Relaxed) {
            cudarc::nvtx::result::range_pop();
        }
    }
}

/// Name the current OS thread in the Nsight Systems timeline.
/// No-op (compiled out) when the `timeline` feature is off.
#[inline(always)]
pub fn name_current_thread_impl(name: &str) {
    #[cfg(feature = "timeline")]
    {
        if TIMELINE_ENABLED.load(Ordering::Relaxed) {
            #[cfg(target_os = "linux")]
            let tid = unsafe { libc::syscall(libc::SYS_gettid) as u32 };
            #[cfg(not(target_os = "linux"))]
            let tid = 0u32;
            cudarc::nvtx::result::name_os_thread(tid, name);
        }
    }
    let _ = name;
}

// === SECTION: RAII guard ===──────────────────

/// RAII guard that pops an NVTX range when dropped.
/// Construct with [`pagoda_timeline_range!`].
#[cfg(feature = "timeline")]
pub struct TimelineRangeGuard {
    active: bool,
}

/// Zero-sized no-op guard used when the `timeline` feature is off.
#[cfg(not(feature = "timeline"))]
pub struct TimelineRangeGuard;

impl TimelineRangeGuard {
    #[doc(hidden)]
    pub fn new(name: &str) -> Self {
        #[cfg(feature = "timeline")]
        {
            let active = TIMELINE_ENABLED.load(Ordering::Relaxed);
            if active {
                cudarc::nvtx::result::range_push(name);
            }
            return TimelineRangeGuard { active };
        }
        #[cfg(not(feature = "timeline"))]
        {
            let _ = name;
            TimelineRangeGuard {}
        }
    }
}

#[cfg(feature = "timeline")]
impl Drop for TimelineRangeGuard {
    fn drop(&mut self) {
        if self.active {
            cudarc::nvtx::result::range_pop();
        }
    }
}

#[cfg(not(feature = "timeline"))]
impl Drop for TimelineRangeGuard {
    fn drop(&mut self) {}
}

// === SECTION: 宏定义 ===───────────────────

/// Push a named NVTX range onto the calling thread's stack.
/// Zero-cost when the `timeline` Cargo feature is off.
#[macro_export]
macro_rules! pagoda_timeline_push {
    ($name:expr) => {
        $crate::timeline::push_impl($name)
    };
}

/// Pop the innermost NVTX range from the calling thread's stack.
/// Zero-cost when the `timeline` Cargo feature is off.
#[macro_export]
macro_rules! pagoda_timeline_pop {
    () => {
        $crate::timeline::pop_impl()
    };
}

/// Open a named NVTX range that closes automatically at end of scope.
///
/// ```rust,ignore
/// let _r = pagoda_timeline_range!("preprocess.tokenize");
/// // range closes here
/// ```
/// Zero-cost when the `timeline` Cargo feature is off.
#[macro_export]
macro_rules! pagoda_timeline_range {
    ($name:expr) => {
        $crate::timeline::TimelineRangeGuard::new($name)
    };
}

/// Annotate the current OS thread in the Nsight Systems timeline.
/// Zero-cost when the `timeline` Cargo feature is off.
#[macro_export]
macro_rules! pagoda_timeline_name_thread {
    ($name:expr) => {
        $crate::timeline::name_current_thread_impl($name)
    };
}
