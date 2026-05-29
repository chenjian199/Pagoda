// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::network::tcp` —— TCP 传输入口与 CallHome 握手协议
//!
//! ## 设计意图
//! TCP 传输由 client/server 两块组成：client 是下游节点，负责反向连回上游
//! server。为支持 "服务端在连接建立后才能知道哪个响应流对应哪个学阶"，
//! 本模块定义了 CallHome 类型的握手消息 `CallHomeHandshake`。本文件本身负责：
//! ¹ 声明子模块 `client` / `server` / `test_utils`；² 定义与 `ConnectionInfo` 互转的
//! `TcpStreamConnectionInfo`；³ 提供贯穿本模块的 `TCP_TRANSPORT` 起名。
//!
//! ## 外部契约
//! - 子模块 `client` / `server` / `test_utils` 均为 `pub mod`。
//! - `TcpStreamConnectionInfo` 与 `ConnectionInfo` 之间的 `From` / `TryFrom` 互转及其
//!   错误消息是契约的一部分；进入 `ConnectionInfo` 后 `transport = "tcp_server"`。
//! - `CallHomeHandshake` 为模块私有类型（仅在 `client` / `server` 之间传递）。
//! - **不重导出 `codec::TwoPartCodec`**：下游请走原完整路径
//!   `pipeline::network::codec::TwoPartCodec`；本文件仅为内部编译需要而 `use` 它。
//!
//! ## 实现要点
//! - `#[allow(unused_imports)]` 覆盖整个 `use super::{...}` 列表。保留这个允许是因为
//!   该列表中部分名称仅在某些 feature 下被消费。
//! - `TryFrom<ConnectionInfo>` 中严格检查 `info.transport != TCP_TRANSPORT`，反向以
//!   明确错误消息起始记号（“Invalid transport...”）避免上层调用者误使用。

// === SECTION: 子模块声明 ===

pub mod client;
pub mod server;

pub mod test_utils;

use super::ControlMessage;
use serde::{Deserialize, Serialize};

#[allow(unused_imports)]
use super::{
    ConnectionInfo, PendingConnections, RegisteredStream, ResponseService, StreamOptions,
    StreamReceiver, StreamSender, StreamType, codec::TwoPartCodec,
};

const TCP_TRANSPORT: &str = "tcp_server";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TcpStreamConnectionInfo {
    pub address: String,
    pub subject: String,
    pub context: String,
    pub stream_type: StreamType,
}

impl From<TcpStreamConnectionInfo> for ConnectionInfo {
    fn from(info: TcpStreamConnectionInfo) -> Self {
        // Need to consider the below. If failure should be fatal, keep the below with .expect()
        // But if there is a default value, we can use:
        // unwrap_or_else(|e| {
        //     eprintln!("Failed to serialize TcpStreamConnectionInfo: {:?}", e);
        //     "{}".to_string() // Provide a fallback empty JSON string or default value
        ConnectionInfo {
            transport: TCP_TRANSPORT.to_string(),
            info: serde_json::to_string(&info)
                .expect("Failed to serialize TcpStreamConnectionInfo"),
        }
    }
}

impl TryFrom<ConnectionInfo> for TcpStreamConnectionInfo {
    type Error = anyhow::Error;

    fn try_from(info: ConnectionInfo) -> Result<Self, Self::Error> {
        if info.transport != TCP_TRANSPORT {
            return Err(anyhow::anyhow!(
                "Invalid transport; TcpClient requires the transport to be `tcp_server`; however {} was passed",
                info.transport
            ));
        }

        serde_json::from_str(&info.info)
            .map_err(|e| anyhow::anyhow!("Failed parse ConnectionInfo: {:?}", e))
    }
}

/// First message sent over a CallHome stream which will map the newly created socket to a specific
/// response data stream which was registered with the same subject.
///
/// This is a transport specific message as part of forming/completing a CallHome TcpStream.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CallHomeHandshake {
    subject: String,
    stream_type: StreamType,
}

#[cfg(test)]
mod tests {
    //! ## 测试矩阵
    //!
    //! | 测试名 | 覆盖维度 |
    //! |---|---|
    //! | `test_tcp_stream_client_server` | TCP server/client 端到端：注册流 + CallHomeHandshake + 双向收发 + 多 rank 互不串扰 |
    use crate::engine::AsyncEngineContextProvider;

    use super::*;
    use crate::pipeline::Context;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestMessage {
        foo: String,
    }

    #[tokio::test]
    async fn test_tcp_stream_client_server() {
        println!("Test Started");
        let options = server::ServerOptions::builder().port(9124).build().unwrap();
        println!("Test Started");
        let server = server::TcpStreamServer::new(options).await.unwrap();
        println!("Server created");

        let context_rank0 = Context::new(());

        let options = StreamOptions::builder()
            .context(context_rank0.context())
            .enable_request_stream(false)
            .enable_response_stream(true)
            .build()
            .unwrap();

        let pending_connection = server.register(options).await;

        let connection_info = pending_connection
            .recv_stream
            .as_ref()
            .unwrap()
            .connection_info
            .clone();

        // set up the other rank
        let context_rank1 = Context::with_id((), context_rank0.id().to_string());

        // connect to the server socket
        let mut send_stream = client::TcpClient::create_response_stream(
            context_rank1.context(),
            connection_info,
            None,
        )
        .await
        .unwrap();
        println!("Client connected");

        // the client can now setup it's end of the stream and if it errors, it can send a message
        // to the server to stop the stream
        //
        // this step must be done before the next step on the server can complete, i.e.
        // the server's stream is now blocked on receiving the prologue message
        //
        // let's improve this and use an enum like Ok/Err; currently, None means good-to-go, and
        // Some(String) means an error happened on this downstream node and we need to alert the
        // upstream node that an error occurred
        send_stream.send_prologue(None).await.unwrap();

        // [server] next - now pending connections should be connected
        let (_conn_info, stream_provider) = pending_connection.recv_stream.unwrap().into_parts();
        let recv_stream = stream_provider.await.unwrap();

        println!("Server paired");

        let msg = TestMessage {
            foo: "bar".to_string(),
        };

        let payload = serde_json::to_vec(&msg).unwrap();

        send_stream.send(payload.into()).await.unwrap();

        println!("Client sent message");

        let data = recv_stream.unwrap().rx.recv().await.unwrap();

        println!("Server received message");

        let recv_msg = serde_json::from_slice::<TestMessage>(&data).unwrap();

        assert_eq!(msg.foo, recv_msg.foo);
        println!("message match");

        drop(send_stream);

        // let data = recv_stream.rx.recv().await;

        // assert!(data.is_none());
    }
}
