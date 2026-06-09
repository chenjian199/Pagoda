// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 空后端 —— 编译占位。
//!
//! 当只启用了 `timeline` 总开关、但未选择任何具体厂商后端时使用。所有方法均为
//! 空操作，使得 timeline 框架可以独立编译与链路验证而不引入任何 profiler 依赖。

use crate::timeline::TimelineBackend;

pub(crate) struct NoopBackend;

impl TimelineBackend for NoopBackend {
    fn range_push(_name: &str) -> u64 {
        0
    }

    fn range_pop() {}

    fn name_os_thread(_tid: u32, _name: &str) {}
}
