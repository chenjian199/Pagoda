// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 设计意图
//! 仅做兼容层：把 `utils::tasks::critical::*` 重新导出到旧的
//! `utils::task` 路径，避免破坏既有调用方。
//!
//! # 外部契约
//! - 路径 `utils::task::CriticalTaskExecutionHandle` 与新路径
//!   `utils::tasks::critical::CriticalTaskExecutionHandle` 必须可互换；
//! - 不引入额外类型，不改变任何 `pub` 项的签名与文档。
//!
//! # 实现要点
//! - `pub use super::tasks::critical::*;` 一行完成转发；
//! - 自带一个回归 smoke 测试，确保关键类型可经此路径构造。

// 重新导出 critical 任务工具，供外部通过更短路径使用。
pub use super::tasks::critical::*;

#[cfg(test)]
mod tests {
	use super::*;
	use tokio_util::sync::CancellationToken;

	#[tokio::test]
	async fn test_critical_task_reexport_is_usable() {
		// 测试 critical 任务相关类型可通过该模块重导出使用。
		let parent = CancellationToken::new();
		let handle = CriticalTaskExecutionHandle::new(
			|_cancel_token| async move { Ok(()) },
			parent,
			"reexport-smoke-test",
		)
		.unwrap();

		handle.join().await.unwrap();
	}
}
