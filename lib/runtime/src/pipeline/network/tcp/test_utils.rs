// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::network::tcp::test_utils` —— TCP 传输测试公共工具
//!
//! ## 设计意图
//! 为 TCP 传输层（client / server / shared_tcp_endpoint）的单元测试提供
//! 一个不依赖外网、可重复使用的本地连接对工厂，避免每个测试自己重复
//! “绑定 127.0.0.1:0 → 获取端口 → connect/accept” 这类样板代码。
//!
//! ## 外部契约
//! - 公开函数 `create_tcp_pair() -> (TcpStream, TcpStream)`：返回
//!   一对已建立连接的本地 TcpStream（client, server）；失败立即 panic
//!   （仅限测试场景）。
//! - 监听器在函数返回前必须至少 accept 一次，确保返回的 server 流可用。
//!
//! ## 实现要点
//! - 使用 `tokio::join!` 并行驱动 `connect` 与 `accept`，避免顺序写法可能
//!   在某些平台触发 accept 排队抖动；行为与顺序版本在可观察上等价。
//! - 监听器在作用域结束时立即关闭，不影响已建立连接的两端继续通信。

use tokio::net::TcpListener;

// === SECTION: 测试用 TCP 连接对工厂 ===
/// 创建一个可用于测试的已连接 TCP 连接对。
///
/// 返回一对彼此已连接的 TcpStream 实例（client, server）。
/// 这适合在不需要真实网络通信的情况下测试 TCP 流处理函数。
pub async fn create_tcp_pair() -> (tokio::net::TcpStream, tokio::net::TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let (client, server) = tokio::join!(tokio::net::TcpStream::connect(addr), listener.accept());
    let client = client.unwrap();
    let (server, _) = server.unwrap();

    (client, server)
}
