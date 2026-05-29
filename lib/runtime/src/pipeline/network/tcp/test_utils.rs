// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::network::tcp::test_utils` —— TCP 传输测试公共工具
//!
//! ## 设计意图
//! 为 TCP 传输层（client / server / shared_tcp_endpoint）的单元测试提供
//! 一个零外网依赖、可重复的本地连接对工厂，避免每个测试自己重复
//! “绑 127.0.0.1:0 → 取端口 → connect/accept” 的样板。
//!
//! ## 外部契约
//! - 公开函数 `create_tcp_pair() -> (TcpStream, TcpStream)`：返回
//!   一对已建立连接的本地 TcpStream（client, server）；失败立即 panic
//!   （仅限测试场景）。
//! - 监听器在函数返回前必须已 accept 一次，确保返回的 server 流可用。
//!
//! ## 实现要点
//! - 使用 `tokio::join!` 并行驱动 `connect` 与 `accept`，避免顺序写法可能
//!   在某些平台触发 accept 排队抖动；行为与顺序版本可观察等价。
//! - 监听器作用域结束时立即关闭，不影响已建立连接的两端继续通信。

use tokio::net::TcpListener;

// === SECTION: TCP pair factory for tests ===
/// Creates a connected TCP pair for testing.
///
/// Returns a tuple of (client, server) TcpStream instances that are connected to each other.
/// This is useful for testing functions that operate on TCP streams without needing
/// actual network communication.
pub async fn create_tcp_pair() -> (tokio::net::TcpStream, tokio::net::TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let (client, server) = tokio::join!(tokio::net::TcpStream::connect(addr), listener.accept());
    let client = client.unwrap();
    let (server, _) = server.unwrap();

    (client, server)
}
