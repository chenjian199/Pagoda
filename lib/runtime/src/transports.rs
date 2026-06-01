// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 网络通信栈集合
//!
//! ## 设计意图
//! `transports` 是 pagoda 分布式系统**最底层的字节搬运层**：所有"跟外部服务说话、
//! 跨进程把字节挪到对端"的代码都汇聚在这里。再往上才是 `pipeline` 这种语义层。
//!
//! 子模块按"角色"切：
//! - [`etcd`]：服务发现 / 分布式锁 / 租约元数据
//! - [`nats`]：通用消息总线 + JetStream KV
//! - [`zmq`]：高性能点到点 / pub-sub
//! - [`tcp`]：直连流式（从 `pipeline::network::tcp` 再导出）
//! - [`event_plane`]：在 nats/zmq 之上的事件平面抽象（统一 pub/sub API）
//! - `utils`：跨 transport 共享的小工具（独立运行时构建等），保持私有
//!
//! ## 外部契约
//! `pub mod` 暴露：`etcd / event_plane / nats / tcp / zmq`。`utils` 私有。

pub mod etcd;
pub mod event_plane;
pub mod nats;
pub mod tcp;
mod utils;
pub mod zmq;
