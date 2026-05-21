// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Egress layer — outbound request routing and sending.

pub mod addressed_router;
pub mod http_router;
pub mod nats_client;
pub mod push_router;
pub mod tcp_client;
pub mod unified_client;

pub use addressed_router::{AddressedPushRouter, AddressedRequest};
pub use http_router::HttpRequestClient;
pub use nats_client::NatsRequestClient;
pub use push_router::{PushRouter, RouterMode, WorkerLoadMonitor};
pub use tcp_client::TcpRequestClient;
pub use unified_client::RequestPlaneClient;
