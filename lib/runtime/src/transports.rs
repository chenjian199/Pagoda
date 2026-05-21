// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 传输协议层统一入口。
//!
//! 聚合 etcd / NATS / TCP / ZMQ 等底层传输实现，
//! 以及基于事件平面的 pub/sub 抽象。

pub mod etcd;
pub mod nats;
pub mod tcp;
pub mod zmq;
pub mod utils;
pub mod event_plane;

// Re-exports for convenience
pub use etcd::EtcdConnector;
pub use nats::{Client as NatsClient, ClientOptions as NatsClientOptions};
pub use zmq::{ZmqPublisher, ZmqSubscriber};
pub use event_plane::{EventPublisher, EventSubscriber};
