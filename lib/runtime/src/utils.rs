// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 通用工具模块。

pub mod graceful_shutdown;
pub mod pool;
pub mod stream;
pub mod ip_resolver;
pub mod task;
pub mod tasks;
pub mod typed_prefix_watcher;

// NOTE: utils/tasks.rs (multi-task coordination) 和 utils/tasks/ (目录) 同名冲突，
// 多任务协调函数合并在 tasks/ 目录内。
