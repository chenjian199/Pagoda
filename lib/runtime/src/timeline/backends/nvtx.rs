// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! NVIDIA NVTX 后端 —— 面向 Nsight Systems。
//!
//! 直接委托 [`cudarc::nvtx`]，运行时需要 `libnvToolsExt.so`（随 CUDA Toolkit 或
//! NVHPC 提供）。通过 `--features timeline-nvtx` 启用。

use crate::timeline::TimelineBackend;

pub(crate) struct NvtxBackend;

impl TimelineBackend for NvtxBackend {
    fn range_push(name: &str) -> u64 {
        // cudarc 不同版本的返回类型可能为 `()` 或整型 id；这里统一丢弃后返回 0，
        // 以保持对 cudarc 版本的鲁棒性（timeline 不依赖该 id 做配对校验）。
        let _ = cudarc::nvtx::result::range_push(name);
        0
    }

    fn range_pop() {
        cudarc::nvtx::result::range_pop();
    }

    fn name_os_thread(tid: u32, name: &str) {
        cudarc::nvtx::result::name_os_thread(tid, name);
    }
}
