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

/// 从 `PGD_ENABLE_RUST_TIMELINE` 环境变量初始化 NVTX 子系统。
/// 必须在运行时启动阶段、任何标注宏触发之前调用一次。
/// 当 `timeline` Cargo feature 关闭时，该函数无操作。
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

/// 当编译启用了 `timeline` feature 且设置了 `PGD_ENABLE_RUST_TIMELINE` 时返回 `true`。
#[inline(always)]
pub fn enabled() -> bool {
    #[cfg(feature = "timeline")]
    {
        return TIMELINE_ENABLED.load(Ordering::Relaxed);
    }
    #[allow(unreachable_code)]
    false
}

/// 将一个 NVTX range 压入当前线程的栈。
/// 当 `timeline` feature 关闭时，该函数为无操作（会被编译掉）。
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

/// 从当前线程的栈中弹出最内层的 NVTX range。
/// 当 `timeline` feature 关闭时，该函数为无操作（会被编译掉）。
#[inline(always)]
pub fn pop_impl() {
    #[cfg(feature = "timeline")]
    {
        if TIMELINE_ENABLED.load(Ordering::Relaxed) {
            cudarc::nvtx::result::range_pop();
        }
    }
}

/// 在 Nsight Systems 的 timeline 中为当前 OS 线程命名。
/// 当 `timeline` feature 关闭时，该函数为无操作（会被编译掉）。
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

/// 在被 drop 时弹出一个 NVTX range 的 RAII 守卫。
/// 使用 [`pagoda_timeline_range!`] 构造。
#[cfg(feature = "timeline")]
pub struct TimelineRangeGuard {
    active: bool,
}

/// 当 `timeline` feature 关闭时使用的零大小无操作守卫。
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

/// 将一个命名的 NVTX range 压入当前线程的栈。
/// 当 `timeline` Cargo feature 关闭时，零开销。
#[macro_export]
macro_rules! pagoda_timeline_push {
    ($name:expr) => {
        $crate::timeline::push_impl($name)
    };
}

/// 从当前线程的栈中弹出最内层的 NVTX range。
/// 当 `timeline` Cargo feature 关闭时，零开销。
#[macro_export]
macro_rules! pagoda_timeline_pop {
    () => {
        $crate::timeline::pop_impl()
    };
}

/// 打开一个命名 NVTX range，并在作用域结束时自动关闭。
///
/// ```rust,ignore
/// let _r = pagoda_timeline_range!("preprocess.tokenize");
/// // range 在此关闭
/// ```
/// 当 `timeline` Cargo feature 关闭时，零开销。
#[macro_export]
macro_rules! pagoda_timeline_range {
    ($name:expr) => {
        $crate::timeline::TimelineRangeGuard::new($name)
    };
}

/// 在 Nsight Systems 的 timeline 中标注当前 OS 线程。
/// 当 `timeline` Cargo feature 关闭时，零开销。
#[macro_export]
macro_rules! pagoda_timeline_name_thread {
    ($name:expr) => {
        $crate::timeline::name_current_thread_impl($name)
    };
}
