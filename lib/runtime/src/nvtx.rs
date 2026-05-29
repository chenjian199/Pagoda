// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! NVTX Timeline 标注辅助层（Nsight Systems profiling）
//!
//! ## 设计意图
//! 为 Rust 侧提供与 [`cudarc::nvtx`] 适配的零依赖、零运行时开销的标注入口，
//! 让上层代码可以用统一的宏 / 函数接口描述 NVTX range 与线程名，而是否真正
//! 向 NVTX runtime 发生调用取决于两级门限：
//!
//! | Cargo feature `nvtx` | `DYN_ENABLE_RUST_NVTX` env | 效果                                  |
//! |----------------------|----------------------------|-------------------------------------------|
//! | off (default)        | any                        | macros compile to nothing; zero overhead  |
//! | on                   | unset                      | one `Relaxed` load per site (~1 ns)       |
//! | on                   | `1` / `true` / `yes`       | cudarc NVTX calls (~50 ns/annotation)     |
//!
//! ## 外部契约
//! - 公开函数：`init()` / `enabled()` / `push_impl(&str)` / `pop_impl()` /
//!   `name_current_thread_impl(&str)` 与公开类型 `NvtxRangeGuard` 的签名、位置与语义保持不变。
//! - 宏 `dynamo_nvtx_push!` / `dynamo_nvtx_pop!` / `dynamo_nvtx_range!` /
//!   `dynamo_nvtx_name_thread!` 的 `macro_rules!` 展开与调用路径保持不变。
//! - 在未启用 `nvtx` feature 时，所有调用点都必须被编译为空操作（零开销）。
//!
//! ## 使用示例
//!
//! ```rust,ignore
//! let _r = dynamo_nvtx_range!("preprocess.tokenize"); // RAII — 作用域结束时自动 pop
//! dynamo_nvtx_push!("codec.encode");
//! dynamo_nvtx_pop!();
//! dynamo_nvtx_name_thread!("tokio-worker-0");
//! ```
//!
//! ## 构建
//!
//! ```bash
//! cargo build --profile profiling --features nvtx
//! ```
//! 运行时需要 `libnvToolsExt.so`（随 CUDA Toolkit 或 NVHPC 一同提供）。

#[cfg(feature = "nvtx")]
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(feature = "nvtx")]
static NVTX_ENABLED: AtomicBool = AtomicBool::new(false);

// === SECTION: 公开 API ===───────────────────

/// Initialise the NVTX subsystem from the `DYN_ENABLE_RUST_NVTX` environment variable.
/// Must be called once at runtime startup before any annotation macros fire.
/// No-op when the `nvtx` Cargo feature is off.
pub fn init() {
    #[cfg(feature = "nvtx")]
    {
        let enabled = std::env::var("DYN_ENABLE_RUST_NVTX")
            .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false);
        NVTX_ENABLED.store(enabled, Ordering::Relaxed);
        if enabled {
            tracing::info!("NVTX annotations enabled (DYN_ENABLE_RUST_NVTX)");
        }
    }
}

/// Returns `true` when the `nvtx` feature is compiled in **and** `DYN_ENABLE_RUST_NVTX` is set.
#[inline(always)]
pub fn enabled() -> bool {
    #[cfg(feature = "nvtx")]
    {
        return NVTX_ENABLED.load(Ordering::Relaxed);
    }
    #[allow(unreachable_code)]
    false
}

/// Push an NVTX range onto the calling thread's stack.
/// No-op (compiled out) when the `nvtx` feature is off.
#[inline(always)]
pub fn push_impl(name: &str) {
    #[cfg(feature = "nvtx")]
    {
        if NVTX_ENABLED.load(Ordering::Relaxed) {
            cudarc::nvtx::result::range_push(name);
        }
    }
    let _ = name;
}

/// Pop the innermost NVTX range from the calling thread's stack.
/// No-op (compiled out) when the `nvtx` feature is off.
#[inline(always)]
pub fn pop_impl() {
    #[cfg(feature = "nvtx")]
    {
        if NVTX_ENABLED.load(Ordering::Relaxed) {
            cudarc::nvtx::result::range_pop();
        }
    }
}

/// Name the current OS thread in the Nsight Systems timeline.
/// No-op (compiled out) when the `nvtx` feature is off.
#[inline(always)]
pub fn name_current_thread_impl(name: &str) {
    #[cfg(feature = "nvtx")]
    {
        if NVTX_ENABLED.load(Ordering::Relaxed) {
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
/// Construct with [`dynamo_nvtx_range!`].
#[cfg(feature = "nvtx")]
pub struct NvtxRangeGuard {
    active: bool,
}

/// Zero-sized no-op guard used when the `nvtx` feature is off.
#[cfg(not(feature = "nvtx"))]
pub struct NvtxRangeGuard;

impl NvtxRangeGuard {
    #[doc(hidden)]
    pub fn new(name: &str) -> Self {
        #[cfg(feature = "nvtx")]
        {
            let active = NVTX_ENABLED.load(Ordering::Relaxed);
            if active {
                cudarc::nvtx::result::range_push(name);
            }
            return NvtxRangeGuard { active };
        }
        #[cfg(not(feature = "nvtx"))]
        {
            let _ = name;
            NvtxRangeGuard {}
        }
    }
}

#[cfg(feature = "nvtx")]
impl Drop for NvtxRangeGuard {
    fn drop(&mut self) {
        if self.active {
            cudarc::nvtx::result::range_pop();
        }
    }
}

#[cfg(not(feature = "nvtx"))]
impl Drop for NvtxRangeGuard {
    fn drop(&mut self) {}
}

// === SECTION: 宏定义 ===───────────────────

/// Push a named NVTX range onto the calling thread's stack.
/// Zero-cost when the `nvtx` Cargo feature is off.
#[macro_export]
macro_rules! dynamo_nvtx_push {
    ($name:expr) => {
        $crate::nvtx::push_impl($name)
    };
}

/// Pop the innermost NVTX range from the calling thread's stack.
/// Zero-cost when the `nvtx` Cargo feature is off.
#[macro_export]
macro_rules! dynamo_nvtx_pop {
    () => {
        $crate::nvtx::pop_impl()
    };
}

/// Open a named NVTX range that closes automatically at end of scope.
///
/// ```rust,ignore
/// let _r = dynamo_nvtx_range!("preprocess.tokenize");
/// // range closes here
/// ```
/// Zero-cost when the `nvtx` Cargo feature is off.
#[macro_export]
macro_rules! dynamo_nvtx_range {
    ($name:expr) => {
        $crate::nvtx::NvtxRangeGuard::new($name)
    };
}

/// Annotate the current OS thread in the Nsight Systems timeline.
/// Zero-cost when the `nvtx` Cargo feature is off.
#[macro_export]
macro_rules! dynamo_nvtx_name_thread {
    ($name:expr) => {
        $crate::nvtx::name_current_thread_impl($name)
    };
}
