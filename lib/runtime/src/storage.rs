// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 持久化与共享状态
//!
//! ## 设计意图
//! 把"跨进程共享的小块结构化状态"集中收口在 `storage` 命名空间下。当前唯一
//! 子模块 [`kv`] 提供传统键值存储抽象（etcd / NATS JetStream KV / 本地文件 /
//! 内存四种 backend 的统一接口）。后续若新增 blob、流式日志等其他持久化原语，
//! 都将在此 namespace 下并列展开。
//!
//! ## 外部契约
//! 重新导出子模块 [`kv`]；不引入新公开符号。

pub mod kv;
