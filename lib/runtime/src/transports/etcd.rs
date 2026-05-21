// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! etcd 客户端封装层。
//!
//! 提供连接管理、租约、分布式锁和 KV 操作。

pub mod connector;
pub mod lease;
pub mod lock;
pub mod kv;

pub use connector::EtcdConnector;
pub use lease::Lease;
pub use lock::DistributedRWLock;
pub use kv::{PrefixWatcher, KvCache, TypedPrefixWatcher};
