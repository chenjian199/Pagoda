// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 设计意图
//! `tasks` 是任务体系的命名空间，统一暴露两个子模块：
//! - [`critical`]：关键任务执行句柄（失败即触发父级取消）；
//! - [`tracker`]：层次化任务跟踪器（调度策略、错误策略、度量上报）。
//!
//! # 外部契约
//! - `pub mod critical` / `pub mod tracker` 必须保持稳定路径；
//! - 不在本模块定义任何业务类型，仅做命名空间聚合；
//! - 子模块物理路径通过 `#[path = ...]` 显式声明，方便目录扁平化。
//!
//! # 实现要点
//! - 自带一个 smoke 测试，确保两个子模块的核心类型在此路径可访问；
//! - 与 [`super::task`] 共同维护「新路径 + 旧路径」并存的过渡兼容性。

// 暴露 critical 与 tracker 两个任务子模块。
#[path = "tasks/critical.rs"]
pub mod critical;
#[path = "tasks/tracker.rs"]
pub mod tracker;

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_submodules_expose_expected_types() {
		// 测试任务子模块是否正确暴露核心类型。
		let _scheduler = tracker::UnlimitedScheduler::new();
		let _policy = tracker::LogOnlyPolicy::new();
		let _critical_handle_size = std::mem::size_of::<critical::CriticalTaskExecutionHandle>();
	}
}
