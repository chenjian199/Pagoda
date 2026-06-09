// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 华为 Ascend 后端 —— 面向 Ascend Insight / msprof（昇腾性能分析工具链）。
//!
//! 通过 `libloading` 在首次使用时**延迟加载** `libmsprof.so`，避免在没有 Ascend
//! 工具链的节点上因缺少 `.so` 而无法启动。任意一步加载/取符号失败都会静默降级为
//! 空操作（因为 timeline 本身是可选的辅助能力）。通过 `--features timeline-ascend`
//! 启用，运行时需要 `libmsprof.so`（随 Ascend Toolkit 提供）。
//!
//! > 注意：下面的 C 符号名（`MsprofRangePush` 等）为占位约定，集成时应对照所用
//! > Ascend Toolkit 版本的 msprof 头文件确认实际导出符号与签名。
//!
//! ### 集成参考（以 Ascend CANN 8.0.RC1 为例）
//!
//! 在 `/usr/local/Ascend/ascend-toolkit/latest/tools/profiler/include/msprof.h`
//! 中查找实际导出的标记函数，确认后替换 `get_fns()` 内的三处 `lib.get(b"xxx\0")`：
//!
//! ```c
//! // msprof.h 中的常见标记入口（示例，以实际头文件为准）：
//! uint64_t MsprofRangePush(const char *name);    // 压入命名区间，返回区间 id
//! void     MsprofRangePop();                     // 弹出最内层区间
//! void     MsprofNameOsThread(uint32_t tid, const char *name); // 命名线程
//! ```
//!
//! 如果实际头文件中符号名不同（如 `msprofRangePush` / `aclprofMark` 等），
//! 只需修改本文件 `get_fns()` 中对应的 `lib.get(b"...\0")` 字符串，以及上述
//! `type RangePushFn` / `RangePopFn` / `NameThreadFn` 的函数签名即可。

use std::ffi::{c_char, CString};
use std::sync::OnceLock;

use crate::timeline::TimelineBackend;

type RangePushFn = unsafe extern "C" fn(*const c_char) -> u64;
type RangePopFn = unsafe extern "C" fn();
type NameThreadFn = unsafe extern "C" fn(u32, *const c_char);

/// 已解析的 msprof 标注函数表；持有 `Library` 句柄以保证函数指针在进程生命周期内有效。
struct MsprofFnTable {
    // 保持动态库加载状态，不可丢弃（drop 会卸载 .so 使函数指针悬空）。
    _lib: libloading::Library,
    range_push: RangePushFn,
    range_pop: RangePopFn,
    name_thread: NameThreadFn,
}

// Safety: msprof 的标注入口是线程安全的全局 C 函数，函数指针本身可跨线程共享。
unsafe impl Send for MsprofFnTable {}
unsafe impl Sync for MsprofFnTable {}

static MSPROF_FNS: OnceLock<Option<MsprofFnTable>> = OnceLock::new();

/// 首次调用时尝试加载 `libmsprof.so` 并解析符号；任何失败返回 `None`（静默降级）。
fn get_fns() -> Option<&'static MsprofFnTable> {
    MSPROF_FNS
        .get_or_init(|| unsafe {
            let lib = match libloading::Library::new("libmsprof.so") {
                Ok(lib) => lib,
                Err(e) => {
                    tracing::debug!("timeline ascend: failed to load libmsprof.so: {e}");
                    return None;
                }
            };

            let range_push: libloading::Symbol<RangePushFn> =
                match lib.get(b"MsprofRangePush\0") {
                    Ok(sym) => sym,
                    Err(e) => {
                        tracing::debug!("timeline ascend: missing MsprofRangePush: {e}");
                        return None;
                    }
                };
            let range_pop: libloading::Symbol<RangePopFn> = match lib.get(b"MsprofRangePop\0") {
                Ok(sym) => sym,
                Err(e) => {
                    tracing::debug!("timeline ascend: missing MsprofRangePop: {e}");
                    return None;
                }
            };
            let name_thread: libloading::Symbol<NameThreadFn> =
                match lib.get(b"MsprofNameOsThread\0") {
                    Ok(sym) => sym,
                    Err(e) => {
                        tracing::debug!("timeline ascend: missing MsprofNameOsThread: {e}");
                        return None;
                    }
                };

            // 将符号解引用为裸函数指针，与 Library 一起打包持有。
            let range_push = *range_push;
            let range_pop = *range_pop;
            let name_thread = *name_thread;

            Some(MsprofFnTable {
                _lib: lib,
                range_push,
                range_pop,
                name_thread,
            })
        })
        .as_ref()
}

pub(crate) struct AscendBackend;

impl TimelineBackend for AscendBackend {
    fn range_push(name: &str) -> u64 {
        if let Some(fns) = get_fns() {
            if let Ok(c_name) = CString::new(name) {
                return unsafe { (fns.range_push)(c_name.as_ptr()) };
            }
        }
        0
    }

    fn range_pop() {
        if let Some(fns) = get_fns() {
            unsafe { (fns.range_pop)() }
        }
    }

    fn name_os_thread(tid: u32, name: &str) {
        if let Some(fns) = get_fns() {
            if let Ok(c_name) = CString::new(name) {
                unsafe { (fns.name_thread)(tid, c_name.as_ptr()) }
            }
        }
    }
}
