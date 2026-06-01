// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # etcd KV 占位
//!
//! ## 设计意图
//! etcd 的 KV 抽象当前实现在 [`crate::storage::kv::etcd`] 下，本文件保留为
//! **占位符**：保持 `crate::transports::etcd::kv` 路径可达，便于未来把 etcd
//! 专属、与 `storage` 解耦的 KV 工具放在这里（例如低层 lease + KV 联合事务）。
//!
//! ## 外部契约
//! 当前不导出任何符号。
