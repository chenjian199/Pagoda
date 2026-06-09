// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Timeline 标注模块 —— 统一的时间线标注抽象层
//!
//! ## 设计意图
//! 为 Rust 侧提供一层**与具体分析器解耦**的时间线标注入口。上层业务代码只依赖
//! 稳定的 timeline 语义（push / pop / range / name_thread），底层则可以在编译期挂载
//! 不同厂商的分析器后端（NVIDIA NVTX、华为 Ascend，以及其它后端），做到上层零改动
//! 切换分析后端。
//!
//! ## 两级门控
//!
//! | Cargo feature `timeline` | `PGD_ENABLE_RUST_TIMELINE` env | 效果                                       |
//! |--------------------------|--------------------------------|--------------------------------------------|
//! | off (default)            | any                            | 宏编译为空操作；零开销                       |
//! | on                       | unset                          | 每个调用点一次 `Relaxed` 原子读 (~1 ns)     |
//! | on                       | `1` / `true` / `yes` / `on`    | 真正调用后端标注 API                        |
//!
//! ## 后端选择（编译期，互斥）
//!
//! - `timeline`（仅总开关）           → [`backends::noop`] 空后端
//! - `timeline-nvtx`                  → [`backends::nvtx`] NVIDIA Nsight Systems
//! - `timeline-ascend`               → [`backends::ascend`] 华为 Ascend Insight / msprof
//!
//! ## 外部契约
//! - 公开函数：`init()` / `enabled()` / `push_impl(&str)` / `pop_impl()` /
//!   `name_current_thread_impl(&str)` 与公开类型 [`TimelineRangeGuard`]。
//! - 宏 `pagoda_timeline_push!` / `pagoda_timeline_pop!` / `pagoda_timeline_range!` /
//!   `pagoda_timeline_name_thread!`。
//! - 在未启用 `timeline` feature 时，所有调用点都被编译为空操作（零开销）。
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
//! cargo build --profile profiling --features timeline-nvtx     # NVTX 后端
//! cargo build --profile profiling --features timeline-ascend   # Ascend 后端
//! cargo build --features timeline                              # 仅框架（空后端）
//! ```

#[cfg(feature = "timeline")]
use std::sync::atomic::{AtomicBool, Ordering};

mod guard;
pub use guard::TimelineRangeGuard;

#[cfg(feature = "timeline")]
mod backends;

// === SECTION: 后端契约 ===───────────────────

/// 时间线分析后端的统一抽象。
///
/// 每个后端实现者只需提供三个关联函数：
/// - [`range_push`](TimelineBackend::range_push) / [`range_pop`](TimelineBackend::range_pop)：
///   在调用线程的局部栈上压入 / 弹出命名区间；
/// - [`name_os_thread`](TimelineBackend::name_os_thread)：给 OS 线程赋予可读名称。
///
/// 这些函数假设调用方已经完成了"是否启用"的门控判断（即 `TIMELINE_ENABLED` 为真），
/// 因此后端实现内部不需要再检查全局开关。
///
/// 方法均为关联函数（无 `&self`），因为底层 profiler C API（NVTX、msprof 等）都是基于
/// 线程局部状态的全局调用，不需要实例状态。
pub trait TimelineBackend: Send + Sync + 'static {
    /// 压入一个命名区间，返回一个不透明 id（部分后端用于匹配 push/pop；不需要的后端返回 0）。
    fn range_push(name: &str) -> u64;

    /// 弹出最内层区间。
    fn range_pop();

    /// 给指定 OS 线程赋予可读名称。
    fn name_os_thread(tid: u32, name: &str);
}

#[cfg(feature = "timeline")]
static TIMELINE_ENABLED: AtomicBool = AtomicBool::new(false);

// === SECTION: 公开 API ===───────────────────

/// 从 `PGD_ENABLE_RUST_TIMELINE` 环境变量初始化 timeline 子系统。
///
/// 应在运行时启动阶段、任何标注宏触发之前调用一次。`Runtime::new()` 会自动调用。
/// 未启用 `timeline` feature 时为空函数。
pub fn init() {
    #[cfg(feature = "timeline")]
    {
        let enabled = std::env::var("PGD_ENABLE_RUST_TIMELINE")
            .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false);
        TIMELINE_ENABLED.store(enabled, Ordering::Relaxed);
        if enabled {
            tracing::info!("Timeline annotations enabled (PGD_ENABLE_RUST_TIMELINE)");
        }
    }
}

/// 当 `timeline` feature 编译进二进制 **且** `PGD_ENABLE_RUST_TIMELINE` 被设置时返回 `true`。
#[inline(always)]
pub fn enabled() -> bool {
    #[cfg(feature = "timeline")]
    {
        return TIMELINE_ENABLED.load(Ordering::Relaxed);
    }
    #[allow(unreachable_code)]
    false
}

/// 在调用线程的栈上压入一个命名区间。
/// 未启用 `timeline` feature 时编译为空操作。
#[inline(always)]
pub fn push_impl(name: &str) {
    #[cfg(feature = "timeline")]
    {
        if TIMELINE_ENABLED.load(Ordering::Relaxed) {
            backends::ActiveBackend::range_push(name);
        }
    }
    let _ = name;
}

/// 弹出调用线程栈上最内层的区间。
/// 未启用 `timeline` feature 时编译为空操作。
#[inline(always)]
pub fn pop_impl() {
    #[cfg(feature = "timeline")]
    {
        if TIMELINE_ENABLED.load(Ordering::Relaxed) {
            backends::ActiveBackend::range_pop();
        }
    }
}

/// 在分析器时间线上给当前 OS 线程命名。
/// 未启用 `timeline` feature 时编译为空操作。
#[inline(always)]
pub fn name_current_thread_impl(name: &str) {
    #[cfg(feature = "timeline")]
    {
        if TIMELINE_ENABLED.load(Ordering::Relaxed) {
            #[cfg(target_os = "linux")]
            let tid = unsafe { libc::syscall(libc::SYS_gettid) as u32 };
            #[cfg(not(target_os = "linux"))]
            let tid = 0u32;
            backends::ActiveBackend::name_os_thread(tid, name);
        }
    }
    let _ = name;
}

// === SECTION: 宏定义 ===───────────────────

/// 在调用线程栈上压入一个命名区间。`timeline` feature 关闭时零开销。
#[macro_export]
macro_rules! pagoda_timeline_push {
    ($name:expr) => {
        $crate::timeline::push_impl($name)
    };
}

/// 弹出调用线程栈上最内层的区间。`timeline` feature 关闭时零开销。
#[macro_export]
macro_rules! pagoda_timeline_pop {
    () => {
        $crate::timeline::pop_impl()
    };
}

/// 打开一个在作用域结束时自动闭合的命名区间。
///
/// ```rust,ignore
/// let _r = pagoda_timeline_range!("preprocess.tokenize");
/// // 区间在此自动关闭
/// ```
/// `timeline` feature 关闭时零开销。
#[macro_export]
macro_rules! pagoda_timeline_range {
    ($name:expr) => {
        $crate::timeline::TimelineRangeGuard::new($name)
    };
}

/// 在分析器时间线上给当前 OS 线程命名。`timeline` feature 关闭时零开销。
#[macro_export]
macro_rules! pagoda_timeline_name_thread {
    ($name:expr) => {
        $crate::timeline::name_current_thread_impl($name)
    };
}
