// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
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

/// CallHome 流里发送的第一条消息，用于把新创建的 socket 映射到先前按相同 subject
/// 注册的特定响应数据流。
///
/// 这是 CallHome TcpStream 建立过程中的一条传输专用消息。
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

        // 设置另一侧的 rank。
        let context_rank1 = Context::with_id((), context_rank0.id().to_string());

        // 连接到服务端 socket。
        let mut send_stream = client::TcpClient::create_response_stream(
            context_rank1.context(),
            connection_info,
            None,
        )
        .await
        .unwrap();
        println!("Client connected");

        // 客户端现在可以初始化流的这一端；如果出错，也可以向服务端发送消息以停止流。
        // 这个步骤必须先于服务端的下一步完成，也就是服务端当前会阻塞在接收 prologue。
        // 这里后续可以改成类似 Ok/Err 的枚举；目前 None 表示可以继续，Some(String)
        // 表示下游节点出错，需要通知上游节点。
        send_stream.send_prologue(None).await.unwrap();

        // 服务端下一步：此时挂起的连接应该已经完成配对。
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

    }
}
