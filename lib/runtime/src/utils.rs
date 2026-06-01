// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 设计意图
//! `utils` 是运行时一组通用工具的聚合命名空间。模块本身不承载业务逻辑，
//! 只负责把 `graceful_shutdown` / `ip_resolver` / `pool` / `stream` /
//! `task` / `tasks` / `typed_prefix_watcher` 等子模块和高频常用类型
//! 统一暴露给运行时其它子系统使用。
//!
//! # 外部契约
//! - 重新导出 `tokio::time::{Duration, Instant}`，使调用方无需直接依赖
//!   `tokio::time` 的路径；
//! - `pub mod graceful_shutdown / ip_resolver / pool / stream / task /
//!   tasks / typed_prefix_watcher` 必须保持稳定路径；
//! - 顶层 re-export：`GracefulShutdownTracker` / `GracefulTaskGuard` /
//!   `get_http_rpc_host_from_env` / `get_tcp_rpc_host_from_env`，
//!   下游通过 `utils::*` 直接访问，路径不得变动。
//!
//! # 实现要点
//! - 子模块统一使用 `#[path = "utils/<name>.rs"]` 声明物理位置，
//!   保持文件树扁平化，并明示模块归属；
//! - 不在本文件定义任何类型或函数，仅做命名空间聚合；
//! - `mod tests` 提供一个 smoke 用例，断言 re-export 项可被解析，
//!   防止后续重构悄悄破坏对外路径。

// === SECTION: 子模块声明 ===

#[path = "utils/graceful_shutdown.rs"]
pub mod graceful_shutdown;
#[path = "utils/ip_resolver.rs"]
pub mod ip_resolver;
#[path = "utils/pool.rs"]
pub mod pool;
#[path = "utils/stream.rs"]
pub mod stream;
#[path = "utils/task.rs"]
pub mod task;
#[path = "utils/tasks.rs"]
pub mod tasks;
#[path = "utils/typed_prefix_watcher.rs"]
pub mod typed_prefix_watcher;

// === SECTION: 顶层重导出 ===

pub use tokio::time::{Duration, Instant};

pub use graceful_shutdown::{GracefulShutdownTracker, GracefulTaskGuard};
pub use ip_resolver::{get_http_rpc_host_from_env, get_tcp_rpc_host_from_env};

// === SECTION: tests ===

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	/// ## 测试过程
	/// 1. 引用 `Duration::from_millis` 与 `Instant::now` 触发 `tokio::time` 重导出；
	/// 2. 取 `GracefulShutdownTracker::new` 的函数指针确认类型可达；
	/// 3. 把两个 host 解析函数赋值给 `fn() -> String`，验证签名稳定。
	///
	/// ## 意义
	/// 该用例守护 `utils` 命名空间对外暴露的 5 个 re-export 路径，
	/// 一旦后续重构改动这些公开符号，编译期立即失败。
	fn test_reexports_are_accessible() {
		let _duration = Duration::from_millis(1);
		let _instant = Instant::now();
		let _tracker_ctor = GracefulShutdownTracker::new;
		let _http_host_fn: fn() -> String = get_http_rpc_host_from_env;
		let _tcp_host_fn: fn() -> String = get_tcp_rpc_host_from_env;
	}
}
