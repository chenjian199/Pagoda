// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Low-level TCP transport — server, client, and connection pooling.

pub mod client;
pub mod server;
pub mod test_utils;

pub use client::{ConnectionPool, TcpClient};
pub use server::{SharedTcpServer, TcpStreamServer};
