// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::network::tcp::client` —— 响应平面 TCP 客户端（反向连接）
//!
//! ## 设计意图
//! 与 [`super::server`] 成对：本文件是 egress 侧在响应平面上以“反向连接”形式
//! 主动拨号到 ingress 侧响应 server 的客户端实现，负责在连接上复用 `TwoPartCodec`
//! 推送响应流。
//!
//! ## 外部契约
//! - 公开类型 / 方法是稳定契约；`SinkExt` / `StreamExt` 是连接上 `Framed` 必需。
//! - 不额外暴露任何 helper；连接超时 / retry 由上层 router 处理。
//!
//! ## 实现要点
//! - 使用 `tokio::net::TcpStream` + `Framed<_, TwoPartCodec>` 组装帧读写；
//! - 关联 cancellation 以便上层在响应流未结束时强制断开连接。

use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, ReadHalf, WriteHalf};
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    time::{self, Duration, Instant},
};
use tokio_util::codec::{FramedRead, FramedWrite};

use prometheus::IntCounter;

use super::{CallHomeHandshake, ControlMessage, TcpStreamConnectionInfo};
use crate::engine::AsyncEngineContext;
use crate::pipeline::network::{
    ConnectionInfo, ResponseStreamPrologue, StreamSender,
    codec::{TwoPartCodec, TwoPartMessage},
    tcp::StreamType,
};
use anyhow::{Context, Result, anyhow as error}; // Import SinkExt to use the `send` method

// === SECTION: TcpClient 公开类型 ===
#[allow(dead_code)]
pub struct TcpClient {
    worker_id: String,
}

impl Default for TcpClient {
    fn default() -> Self {
        TcpClient {
            worker_id: uuid::Uuid::new_v4().to_string(),
        }
    }
}

impl TcpClient {
    pub fn new(worker_id: String) -> Self {
        TcpClient { worker_id }
    }

    async fn connect(address: &str) -> std::io::Result<TcpStream> {
        // 尝试连接到目标地址；如果出现 AddrNotAvailable，则按线性退避重试。
        let backoff = std::time::Duration::from_millis(200);
        loop {
            match TcpStream::connect(address).await {
                Ok(socket) => {
                    socket.set_nodelay(true)?;
                    return Ok(socket);
                }
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::AddrNotAvailable {
                        tracing::warn!("retry warning: failed to connect: {:?}", e);
                        tokio::time::sleep(backoff).await;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
    }

    pub async fn create_response_stream(
        context: Arc<dyn AsyncEngineContext>,
        info: ConnectionInfo,
        cancellation_counter: Option<IntCounter>,
    ) -> Result<StreamSender> {
        let info =
            TcpStreamConnectionInfo::try_from(info).context("tcp-stream-connection-info-error")?;
        tracing::trace!("Creating response stream for {:?}", info);

        if info.stream_type != StreamType::Response {
            return Err(error!(
                "Invalid stream type; TcpClient requires the stream type to be `response`; however {:?} was passed",
                info.stream_type
            ));
        }

        if info.context != context.id() {
            return Err(error!(
                "Invalid context; TcpClient requires the context to be {:?}; however {:?} was passed",
                context.id(),
                info.context
            ));
        }

        let stream = TcpClient::connect(&info.address).await?;
        let peer_port = stream.peer_addr().ok().map(|addr| addr.port());
        let (read_half, write_half) = tokio::io::split(stream);

        let framed_reader = FramedRead::new(read_half, TwoPartCodec::default());
        let mut framed_writer = FramedWrite::new(write_half, TwoPartCodec::default());

        // 这是一个 oneshot 通道，用来在流关闭时发出信号。
        // 当流发送端被 drop 时，bytes_rx 会关闭，转发任务也会退出。
        // 转发任务会持有 oneshot 通道的 alive_rx 半边，这会关闭 alive 通道，
        // 从而通知 alive_tx 的持有者流已经关闭；alive_tx 随后会被监控任务持有。
        let (alive_tx, alive_rx) = tokio::sync::oneshot::channel::<()>();

        let reader_task = tokio::spawn(handle_reader(
            framed_reader,
            context.clone(),
            alive_tx,
            cancellation_counter,
        ));

        // 传输专用的握手消息。
        let handshake = CallHomeHandshake {
            subject: info.subject.clone(),
            stream_type: StreamType::Response,
        };

        let handshake_bytes = match serde_json::to_vec(&handshake) {
            Ok(hb) => hb,
            Err(err) => {
                return Err(error!(
                    "create_response_stream: Error converting CallHomeHandshake to JSON array: {err:#}"
                ));
            }
        };
        let msg = TwoPartMessage::from_header(handshake_bytes.into());

        // 发送第一条 TCP 握手消息。
        framed_writer
            .send(msg)
            .await
            .map_err(|e| error!("failed to send handshake: {:?}", e))?;

        // 建立向传输层发送字节的通道。
        let (bytes_tx, bytes_rx) = tokio::sync::mpsc::channel(64);

        // 把这个流发送出的字节转发到传输层；同时持有 oneshot 通道的 alive_rx 半边。
        let writer_context = context.clone();
        let writer_task = tokio::spawn(handle_writer(
            framed_writer,
            bytes_rx,
            alive_rx,
            writer_context,
        ));

        let subject = info.subject.clone();
        let monitor_context = context;
        // 启动连接监控任务；错误已经在 wait_for_connection_tasks 内部记录，
        // 所以这里故意丢弃 Result。
        tokio::spawn(async move {
            let _ = wait_for_connection_tasks(
                reader_task,
                writer_task,
                monitor_context,
                peer_port,
                subject,
            )
            .await;
        });

        // 为流设置 prologue。
        // 未来这里可能会加入传输专用元数据。
        let prologue = Some(ResponseStreamPrologue { error: None });

        // 创建流发送端。
        let stream_sender = StreamSender {
            tx: bytes_tx,
            prologue,
        };

        Ok(stream_sender)
    }
}

// === SECTION: 连接 / reader / writer 任务辅助函数 ===
async fn wait_for_connection_tasks(
    reader_task: tokio::task::JoinHandle<FramedRead<ReadHalf<TcpStream>, TwoPartCodec>>,
    writer_task: tokio::task::JoinHandle<Result<FramedWrite<WriteHalf<TcpStream>, TwoPartCodec>>>,
    context: Arc<dyn AsyncEngineContext>,
    peer_port: Option<u16>,
    subject: String,
) -> Result<()> {
    let (reader, writer) = tokio::join!(reader_task, writer_task);

    match (reader, writer) {
        (Ok(reader), Ok(writer)) => {
            let reader = reader.into_inner();

            let writer = match writer {
                Ok(writer) => writer.into_inner(),
                Err(e) => {
                    tracing::error!(
                        subject = %subject,
                        peer_port = ?peer_port,
                        err = ?e,
                        "writer task returned error"
                    );
                    return Err(e);
                }
            };

            let stream = reader.unsplit(writer);
            wait_for_server_shutdown(stream, context).await
        }
        (Err(reader_err), Ok(_)) => {
            tracing::error!(
                subject = %subject,
                peer_port = ?peer_port,
                err = ?reader_err,
                "reader task failed to join"
            );
            Err(reader_err.into())
        }
        (Ok(_), Err(writer_err)) => {
            tracing::error!(
                subject = %subject,
                peer_port = ?peer_port,
                err = ?writer_err,
                "writer task failed to join"
            );
            Err(writer_err.into())
        }
        (Err(reader_err), Err(writer_err)) => {
            tracing::error!(
                subject = %subject,
                peer_port = ?peer_port,
                reader_err = ?reader_err,
                writer_err = ?writer_err,
                "both reader and writer tasks failed to join"
            );
            // 直接暴露 reader 错误；writer 错误已经在上面记录。
            Err(reader_err.into())
        }
    }
}

async fn wait_for_server_shutdown(
    mut stream: TcpStream,
    context: Arc<dyn AsyncEngineContext>,
) -> Result<()> {
    // `handle_writer` 在 `killed` 和 `stopped` 两种情况下都会跳过关闭哨兵，
    // 所以服务端在这两种情况下都没有可响应的内容；如果一直停在读循环里等到
    // 10 秒超时，就只是在浪费时间。
    if context.is_killed() || context.is_stopped() {
        tracing::debug!("stream context killed or stopped; skipping server FIN wait");
        return Ok(());
    }

    // 等待 TCP 服务端关闭 socket 连接，并用超时包起来，避免正常的哨兵关闭
    // 逻辑无限挂起。
    let mut buf = [0u8; 1024];
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let n = time::timeout_at(deadline, stream.read(&mut buf))
            .await
            .inspect_err(|_| {
                tracing::debug!("server did not close socket within the deadline");
            })?
            .inspect_err(|e| {
                tracing::debug!(err = ?e, "failed to read from stream");
            })?;
        if n == 0 {
            // 服务端已经关闭（FIN）。
            break;
        }
    }

    Ok(())
}

async fn handle_reader(
    framed_reader: FramedRead<tokio::io::ReadHalf<tokio::net::TcpStream>, TwoPartCodec>,
    context: Arc<dyn AsyncEngineContext>,
    alive_tx: tokio::sync::oneshot::Sender<()>,
    cancellation_counter: Option<IntCounter>,
) -> FramedRead<tokio::io::ReadHalf<tokio::net::TcpStream>, TwoPartCodec> {
    let mut framed_reader = framed_reader;
    let mut alive_tx = alive_tx;
    // 在每个取消分支都会置位；循环结束后只统计一次。
    let mut cancellation_seen = false;
    loop {
        tokio::select! {
            msg = framed_reader.next() => {
                match msg {
                    Some(Ok(two_part_msg)) => {
                        match two_part_msg.optional_parts() {
                           (Some(bytes), None) => {
                                let msg = match serde_json::from_slice::<ControlMessage>(bytes) {
                                    Ok(msg) => msg,
                                    Err(e) => {
                                        tracing::warn!(
                                            err = ?e,
                                            "invalid control message, closing connection"
                                        );
                                        cancellation_seen = true;
                                        context.kill();
                                        break;
                                    }
                                };

                                // Stop / Kill 故意不 `break`：reader 会继续运行，
                                // 这样后来的 Kill 可以升级之前的 Stop（反之亦然）。
                                // 一旦 `handle_writer` 对 `context.stop()` / `context.kill()`
                                // 做出反应，循环仍会通过 `alive_tx.closed()` 分支及时退出。
                                match msg {
                                    ControlMessage::Stop => {
                                        cancellation_seen = true;
                                        context.stop();
                                    }
                                    ControlMessage::Kill => {
                                        cancellation_seen = true;
                                        context.kill();
                                    }
                                    ControlMessage::Sentinel => {
                                        tracing::warn!(
                                            "unexpected sentinel on client reader, closing connection"
                                        );
                                        cancellation_seen = true;
                                        context.kill();
                                        break;
                                    }
                                }
                           }
                           _ => {
                                tracing::warn!(
                                    "unexpected non-control message on client reader, closing connection"
                                );
                                cancellation_seen = true;
                                context.kill();
                                break;
                           }
                        }
                    }
                    Some(Err(e)) => {
                        // 关闭 engine context，让 producer 停止生成已经无法送达的响应。
                        tracing::warn!(err = ?e, "tcp stream read error, closing connection");
                        cancellation_seen = true;
                        context.kill();
                        break;
                    }
                    None => {
                        tracing::debug!("tcp stream closed by server");
                        cancellation_seen = true;
                        break;
                    }
                }
            }
            _ = alive_tx.closed() => {
                break;
            }
        }
    }
    if cancellation_seen && let Some(counter) = &cancellation_counter {
        counter.inc();
    }
    framed_reader
}

async fn handle_writer(
    mut framed_writer: FramedWrite<tokio::io::WriteHalf<tokio::net::TcpStream>, TwoPartCodec>,
    mut bytes_rx: tokio::sync::mpsc::Receiver<TwoPartMessage>,
    alive_rx: tokio::sync::oneshot::Receiver<()>,
    context: Arc<dyn AsyncEngineContext>,
) -> Result<FramedWrite<tokio::io::WriteHalf<tokio::net::TcpStream>, TwoPartCodec>> {
    // 只在正常通道关闭时发送 sentinel。
    let mut send_sentinel = true;

    loop {
        let msg = tokio::select! {
            biased;

            _ = context.killed() => {
                tracing::trace!("context kill signal received; shutting down");
                send_sentinel = false;
                break;
            }

            _ = context.stopped() => {
                tracing::trace!("context stop signal received; shutting down");
                send_sentinel = false;
                break;
            }

            msg = bytes_rx.recv() => {
                match msg {
                    Some(msg) => msg,
                    None => {
                        tracing::trace!("response channel closed; shutting down");
                        break;
                    }
                }
            }
        };

        if let Err(e) = framed_writer.send(msg).await {
            tracing::trace!(
                "failed to send message to network; possible disconnect: {:?}",
                e
            );
            send_sentinel = false;
            break;
        }
    }

    // 仅在正常关闭时发送 sentinel。
    if send_sentinel {
        let message = serde_json::to_vec(&ControlMessage::Sentinel)?;
        let msg = TwoPartMessage::from_header(message.into());
        framed_writer.send(msg).await?;
    }

    drop(alive_rx);
    Ok(framed_writer)
}

// === SECTION: 测试 - writer/reader 行为、控制消息、取消 ===
#[cfg(test)]
mod tests {
    //! ## 测试矩阵
    //!
    //! | 测试名 | 覆盖维度 |
    //! |---|---|
    //! | `test_handle_writer_forwards_messages` | writer：channel → framed_writer 数据转发 |
    //! | `test_handle_writer_sends_sentinel_on_normal_closure` | writer：正常关闭时发送 Sentinel header |
    //! | `test_handle_writer_no_sentinel_on_context_killed` | writer：context killed 不发 Sentinel |
    //! | `test_handle_writer_no_sentinel_on_context_stopped` | writer：context stopped 不发 Sentinel |
    //! | `test_handle_writer_multiple_messages` | writer：连续多消息顺序保持 |
    //! | `test_handle_writer_drops_alive_rx` | writer：alive_rx 被 drop 时退出循环 |
    //! | `test_handle_writer_header_only_messages` | writer：纯 header 消息编码正确 |
    //! | `test_handle_writer_mixed_messages` | writer：header-only / data-only / 双段混合 |
    //! | `test_wait_for_server_shutdown_skips_terminal_context` | shutdown：context 已 terminal 时立即返回 |
    //! | `test_connection_monitor_skips_fin_wait_after_read_error_kills_context` | reader：read 错误后跳过 FIN-wait 并 kill context |
    //! | `test_handle_reader_stop_control_message` | reader：Stop 控制消息触发 context stop |
    //! | `test_handle_reader_kill_control_message` | reader：Kill 控制消息触发 context kill |
    //! | `test_handle_reader_exits_on_alive_channel_closed` | reader：alive 通道关闭时退出 |
    //! | `test_handle_reader_exits_on_stream_closed` | reader：底层 stream 关闭时退出 |
    //! | `test_handle_reader_multiple_control_messages` | reader：连续多控制消息处理 |
    //! | `test_handle_reader_stop_then_kill` | reader：先 Stop 后 Kill 的状态机转换 |
    //! | `test_handle_reader_increments_cancellation_counter_on_read_error` | reader：read 错误时取消计数器递增 |
    //! | `test_handle_reader_kills_on_protocol_violations` | reader：协议违规帧触发 kill |
    use super::*;
    use crate::pipeline::context::Controller;
    use crate::pipeline::network::tcp::test_utils::create_tcp_pair;
    use bytes::Bytes;
    use futures::StreamExt;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::sync::{mpsc, oneshot};
    use tokio_util::codec::FramedRead;

    struct WriterHarness {
        server: tokio::net::TcpStream,
        framed_writer: FramedWrite<tokio::io::WriteHalf<tokio::net::TcpStream>, TwoPartCodec>,
        bytes_tx: mpsc::Sender<TwoPartMessage>,
        bytes_rx: mpsc::Receiver<TwoPartMessage>,
        alive_tx: oneshot::Sender<()>,
        alive_rx: oneshot::Receiver<()>,
        controller: Arc<Controller>,
    }

    /// 创建一个可复用的 writer 测试支架，包含成对的 TCP 流和测试通道。
    async fn writer_harness() -> WriterHarness {
        let (client, server) = create_tcp_pair().await;
        let (_, write_half) = tokio::io::split(client);
        let framed_writer = FramedWrite::new(write_half, TwoPartCodec::default());

        let (bytes_tx, bytes_rx) = mpsc::channel(64);
        let (alive_tx, alive_rx) = oneshot::channel::<()>();
        let controller = Arc::new(Controller::default());

        WriterHarness {
            server,
            framed_writer,
            bytes_tx,
            bytes_rx,
            alive_tx,
            alive_rx,
            controller,
        }
    }

    async fn recv_msg(reader: &mut FramedRead<TcpStream, TwoPartCodec>) -> TwoPartMessage {
        reader
            .next()
            .await
            .expect("expected message")
            .expect("failed to decode message")
    }

    fn assert_data_only_message(msg: TwoPartMessage, expected: &[u8]) {
        let (header, data) = msg.optional_parts();
        assert!(header.is_none(), "data-only message should not have header");
        assert_eq!(
            data.expect("data payload missing").as_ref(),
            expected,
            "data payload should match"
        );
    }

    fn assert_header_only_message(msg: TwoPartMessage, expected: &[u8]) {
        let (header, data) = msg.optional_parts();
        assert!(data.is_none(), "header-only message should not carry data");
        assert_eq!(
            header.expect("header missing").as_ref(),
            expected,
            "header payload should match"
        );
    }

    fn assert_header_and_data_message(
        msg: TwoPartMessage,
        expected_header: &[u8],
        expected_data: &[u8],
    ) {
        let (header, data) = msg.optional_parts();
        assert_eq!(
            header.expect("header missing").as_ref(),
            expected_header,
            "header payload should match"
        );
        assert_eq!(
            data.expect("data missing").as_ref(),
            expected_data,
            "data payload should match"
        );
    }

    fn assert_sentinel_message(msg: TwoPartMessage) {
        let (header, data) = msg.optional_parts();
        assert!(data.is_none(), "sentinel should not include a data section");
        let expected_sentinel = serde_json::to_vec(&ControlMessage::Sentinel).unwrap();
        assert_eq!(
            header.expect("sentinel header missing").as_ref(),
            expected_sentinel.as_slice(),
            "sentinel header should match serialized ControlMessage::Sentinel"
        );
    }

    /// 测试 handle_writer 会把通道里的消息转发给 framed writer。
    #[tokio::test]
    async fn test_handle_writer_forwards_messages() {
        let WriterHarness {
            server,
            framed_writer,
            bytes_tx,
            bytes_rx,
            alive_rx,
            controller,
            ..
        } = writer_harness().await;

        // 发送测试消息。
        let test_msg = TwoPartMessage::from_data(Bytes::from("test data"));
        bytes_tx.send(test_msg).await.unwrap();

        // 关闭发送端以触发正常终止。
        drop(bytes_tx);

        let result = handle_writer(framed_writer, bytes_rx, alive_rx, controller).await;

        assert!(result.is_ok());

        // 从服务端侧解码，验证数据和 sentinel 都已发送。
        let mut reader = FramedRead::new(server, TwoPartCodec::default());

        let msg = recv_msg(&mut reader).await;
        assert_data_only_message(msg, b"test data");

        let sentinel = recv_msg(&mut reader).await;
        assert_sentinel_message(sentinel);
    }

    /// 测试 handle_writer 会在通道正常关闭时发送 sentinel。
    #[tokio::test]
    async fn test_handle_writer_sends_sentinel_on_normal_closure() {
        let WriterHarness {
            mut server,
            framed_writer,
            bytes_tx,
            bytes_rx,
            alive_rx,
            controller,
            ..
        } = writer_harness().await;

        // 立即关闭发送端，以触发正常终止。
        drop(bytes_tx);

        let result = handle_writer(framed_writer, bytes_rx, alive_rx, controller).await;

        assert!(result.is_ok());

        // 从服务端侧读取，验证 sentinel 已发送。
        let mut buffer = vec![0u8; 1024];
        let n = server.read(&mut buffer).await.unwrap();

        // 缓冲区中应该包含 sentinel 消息。
        assert!(n > 0, "Expected sentinel to be written to the TCP stream");

        // 通过检查 JSON 片段来确认其中包含 sentinel 消息。
        let sentinel_json = serde_json::to_vec(&ControlMessage::Sentinel).unwrap();
        assert!(
            buffer[..n]
                .windows(sentinel_json.len())
                .any(|w| w == sentinel_json.as_slice()),
            "Buffer should contain sentinel message. Buffer: {:?}",
            String::from_utf8_lossy(&buffer[..n])
        );
    }

    /// 测试在 context 被 kill 时，handle_writer 不会发送 sentinel。
    #[tokio::test]
    async fn test_handle_writer_no_sentinel_on_context_killed() {
        let WriterHarness {
            mut server,
            framed_writer,
            bytes_rx,
            alive_rx,
            controller,
            ..
        } = writer_harness().await;

        // kill 掉 context。
        controller.kill();

        let result = handle_writer(framed_writer, bytes_rx, alive_rx, controller).await;

        assert!(result.is_ok());

        // 先 drop writer 关闭连接，再尝试读取，否则测试会卡在 `server.read()`。
        drop(result);

        // 从服务端侧读取，应该拿不到 sentinel。
        let mut buffer = vec![0u8; 1024];
        let n = server.read(&mut buffer).await.unwrap();

        // 缓冲区应该为空（没有发送 sentinel）。
        let sentinel_json = serde_json::to_vec(&ControlMessage::Sentinel).unwrap();
        assert!(
            n == 0
                || !buffer[..n]
                    .windows(sentinel_json.len())
                    .any(|w| w == sentinel_json.as_slice()),
            "Buffer should NOT contain sentinel message when context is killed"
        );
    }

    /// 测试在 context 被 stop 时，handle_writer 不会发送 sentinel。
    #[tokio::test]
    async fn test_handle_writer_no_sentinel_on_context_stopped() {
        let WriterHarness {
            mut server,
            framed_writer,
            bytes_rx,
            alive_rx,
            controller,
            ..
        } = writer_harness().await;

        // stop 掉 context。
        controller.stop();

        let result = handle_writer(framed_writer, bytes_rx, alive_rx, controller).await;

        assert!(result.is_ok());

        // 先 drop writer 关闭连接，再尝试读取，否则测试会卡在 `server.read()`。
        drop(result);

        // 从服务端侧读取，应该拿不到 sentinel。
        let mut buffer = vec![0u8; 1024];
        let n = server.read(&mut buffer).await.unwrap();

        // 缓冲区应该为空（没有发送 sentinel）。
        let sentinel_json = serde_json::to_vec(&ControlMessage::Sentinel).unwrap();
        assert!(
            n == 0
                || !buffer[..n]
                    .windows(sentinel_json.len())
                    .any(|w| w == sentinel_json.as_slice()),
            "Buffer should NOT contain sentinel message when context is stopped"
        );
    }

    /// 测试 handle_writer 能正确处理多条消息。
    #[tokio::test]
    async fn test_handle_writer_multiple_messages() {
        let WriterHarness {
            server,
            framed_writer,
            bytes_tx,
            bytes_rx,
            alive_rx,
            controller,
            ..
        } = writer_harness().await;

        // 发送多条消息。
        for i in 0..5 {
            let test_msg = TwoPartMessage::from_data(Bytes::from(format!("message {}", i)));
            bytes_tx.send(test_msg).await.unwrap();
        }

        // 关闭发送端以触发正常终止。
        drop(bytes_tx);

        let result = handle_writer(framed_writer, bytes_rx, alive_rx, controller).await;

        assert!(result.is_ok());

        // 从服务端侧解码，验证所有消息以及 sentinel 都已发送。
        let mut reader = FramedRead::new(server, TwoPartCodec::default());
        for i in 0..5 {
            let msg = recv_msg(&mut reader).await;
            assert_data_only_message(msg, format!("message {}", i).as_bytes());
        }

        let sentinel = recv_msg(&mut reader).await;
        assert_sentinel_message(sentinel);
    }

    /// 测试 handle_writer 完成后 alive_rx 会被 drop。
    #[tokio::test]
    async fn test_handle_writer_drops_alive_rx() {
        let WriterHarness {
            framed_writer,
            bytes_tx,
            bytes_rx,
            alive_tx,
            alive_rx,
            controller,
            ..
        } = writer_harness().await;

        // 关闭发送端以触发正常终止。
        drop(bytes_tx);

        let result = handle_writer(framed_writer, bytes_rx, alive_rx, controller).await;

        assert!(result.is_ok());

        // 由于 alive_rx 已被 drop，alive_tx 现在应该已关闭。
        assert!(alive_tx.is_closed());
    }

    /// 测试仅头部消息（控制消息）的 handle_writer 行为。
    #[tokio::test]
    async fn test_handle_writer_header_only_messages() {
        let WriterHarness {
            server,
            framed_writer,
            bytes_tx,
            bytes_rx,
            alive_rx,
            controller,
            ..
        } = writer_harness().await;

        // 发送一条仅头部消息。
        let header_msg = TwoPartMessage::from_header(Bytes::from("header content"));
        bytes_tx.send(header_msg).await.unwrap();

        // 关闭发送端。
        drop(bytes_tx);

        let result = handle_writer(framed_writer, bytes_rx, alive_rx, controller).await;

        assert!(result.is_ok());

        let mut reader = FramedRead::new(server, TwoPartCodec::default());

        let header_msg = recv_msg(&mut reader).await;
        assert_header_only_message(header_msg, b"header content");

        let sentinel = recv_msg(&mut reader).await;
        assert_sentinel_message(sentinel);
    }

    /// 测试 handle_writer 处理头部消息和数据消息混合输入。
    #[tokio::test]
    async fn test_handle_writer_mixed_messages() {
        let WriterHarness {
            server,
            framed_writer,
            bytes_tx,
            bytes_rx,
            alive_rx,
            controller,
            ..
        } = writer_harness().await;

        // 发送混合消息。
        bytes_tx
            .send(TwoPartMessage::from_header(Bytes::from("header1")))
            .await
            .unwrap();
        bytes_tx
            .send(TwoPartMessage::from_data(Bytes::from("data1")))
            .await
            .unwrap();
        bytes_tx
            .send(TwoPartMessage::from_parts(
                Bytes::from("header2"),
                Bytes::from("data2"),
            ))
            .await
            .unwrap();

        // 关闭发送端。
        drop(bytes_tx);

        let result = handle_writer(framed_writer, bytes_rx, alive_rx, controller).await;

        assert!(result.is_ok());

        let mut reader = FramedRead::new(server, TwoPartCodec::default());

        let first = recv_msg(&mut reader).await;
        assert_header_only_message(first, b"header1");

        let second = recv_msg(&mut reader).await;
        assert_data_only_message(second, b"data1");

        let third = recv_msg(&mut reader).await;
        assert_header_and_data_message(third, b"header2", b"data2");

        let sentinel = recv_msg(&mut reader).await;
        assert_sentinel_message(sentinel);
    }

    /// 被 kill 或 stop 的 context 会跳过服务端 FIN 等待时限。
    #[tokio::test]
    async fn test_wait_for_server_shutdown_skips_terminal_context() {
        for action in [Controller::kill as fn(&Controller), Controller::stop] {
            let (client, _server) = create_tcp_pair().await;
            let controller = Arc::new(Controller::default());
            action(&controller);

            let context: Arc<dyn AsyncEngineContext> = controller;
            let result = tokio::time::timeout(
                std::time::Duration::from_millis(50),
                wait_for_server_shutdown(client, context),
            )
            .await;

            assert!(result.is_ok(), "terminal context should not wait for FIN");
            assert!(
                result.unwrap().is_ok(),
                "terminal context shutdown should succeed"
            );
        }
    }

    /// connection monitor 中的读错误会 kill context，并跳过 FIN 等待。
    #[tokio::test]
    async fn test_connection_monitor_skips_fin_wait_after_read_error_kills_context() {
        let (client, mut server) = create_tcp_pair().await;
        let (read_half, write_half) = tokio::io::split(client);
        let framed_reader = FramedRead::new(read_half, TwoPartCodec::default());
        let framed_writer = FramedWrite::new(write_half, TwoPartCodec::default());
        let (_bytes_tx, bytes_rx) = mpsc::channel(64);
        let (alive_tx, alive_rx) = oneshot::channel::<()>();
        let controller = Arc::new(Controller::default());

        let reader_context = controller.clone();
        let reader_task = tokio::spawn(async move {
            handle_reader(framed_reader, reader_context, alive_tx, None).await
        });
        let writer_context = controller.clone();
        let writer_task = tokio::spawn(async move {
            handle_writer(framed_writer, bytes_rx, alive_rx, writer_context).await
        });

        // 绕过 codec，写入一个完整但无效的 TwoPartCodec 头部。
        // 这样可以让 client reader 进入 Some(Err(_))，同时不关闭服务端 socket。
        server.write_all(&[0xFF; 24]).await.unwrap();

        let monitor_context: Arc<dyn AsyncEngineContext> = controller.clone();
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(250),
            wait_for_connection_tasks(
                reader_task,
                writer_task,
                monitor_context,
                None,
                "test-subject".to_string(),
            ),
        )
        .await;

        assert!(
            result.is_ok(),
            "connection monitor should not wait for the FIN deadline after read error"
        );
        assert!(result.unwrap().is_ok(), "connection monitor should succeed");
        assert!(
            controller.is_killed(),
            "read error should kill the stream context"
        );
    }

    // ==================== handle_reader tests ====================

    struct ReaderHarness {
        framed_server: FramedWrite<tokio::io::WriteHalf<tokio::net::TcpStream>, TwoPartCodec>,
        framed_reader: FramedRead<tokio::io::ReadHalf<tokio::net::TcpStream>, TwoPartCodec>,
        alive_tx: oneshot::Sender<()>,
        alive_rx: oneshot::Receiver<()>,
        controller: Arc<Controller>,
    }

    /// 创建一个可复用的 reader 测试支架，包含成对的 TCP 流和测试通道。
    async fn reader_harness() -> ReaderHarness {
        let (client, server) = create_tcp_pair().await;
        let (read_half, _write_half) = tokio::io::split(client);
        let (_server_read, server_write) = tokio::io::split(server);

        let framed_reader = FramedRead::new(read_half, TwoPartCodec::default());
        let framed_server = FramedWrite::new(server_write, TwoPartCodec::default());
        let (alive_tx, alive_rx) = oneshot::channel::<()>();
        let controller = Arc::new(Controller::default());

        ReaderHarness {
            framed_server,
            framed_reader,
            alive_tx,
            alive_rx,
            controller,
        }
    }

    fn control_message(msg: &ControlMessage) -> TwoPartMessage {
        let msg_bytes = serde_json::to_vec(msg).unwrap();
        TwoPartMessage::from_header(Bytes::from(msg_bytes))
    }

    /// 测试 handle_reader 会在收到 Stop 控制消息时调用 context.stop()。
    #[tokio::test]
    async fn test_handle_reader_stop_control_message() {
        let ReaderHarness {
            mut framed_server,
            framed_reader,
            alive_tx,
            alive_rx: _alive_rx,
            controller,
        } = reader_harness().await;

        // 启动 reader 任务。
        let controller_clone = controller.clone();
        let reader_handle = tokio::spawn(async move {
            handle_reader(framed_reader, controller_clone, alive_tx, None).await
        });

        // 从服务端发送 Stop 控制消息。
        framed_server
            .send(control_message(&ControlMessage::Stop))
            .await
            .unwrap();

        // 关闭 framed server，向 client 发送 EOF 信号。
        framed_server.close().await.unwrap();

        // 等待 reader 结束。
        let _ = reader_handle.await.unwrap();

        // 验证 controller 上的 stop 已被调用。
        assert!(
            controller.is_stopped(),
            "Controller should be stopped after receiving Stop message"
        );
    }

    /// 测试 handle_reader 会在收到 Kill 控制消息时调用 context.kill()。
    #[tokio::test]
    async fn test_handle_reader_kill_control_message() {
        let ReaderHarness {
            mut framed_server,
            framed_reader,
            alive_tx,
            alive_rx: _alive_rx,
            controller,
        } = reader_harness().await;

        // 启动 reader 任务。
        let controller_clone = controller.clone();
        let reader_handle = tokio::spawn(async move {
            handle_reader(framed_reader, controller_clone, alive_tx, None).await
        });

        // 从服务端发送 Kill 控制消息。
        framed_server
            .send(control_message(&ControlMessage::Kill))
            .await
            .unwrap();

        // 关闭 framed server，向 client 发送 EOF 信号。
        framed_server.close().await.unwrap();

        // 等待 reader 结束。
        let _ = reader_handle.await.unwrap();

        // 验证 controller 上的 kill 已被调用。
        assert!(
            controller.is_killed(),
            "Controller should be killed after receiving Kill message"
        );
    }

    /// 测试当 alive 通道关闭时，handle_reader 会退出。
    #[tokio::test]
    async fn test_handle_reader_exits_on_alive_channel_closed() {
        let ReaderHarness {
            framed_reader,
            alive_tx,
            alive_rx,
            controller,
            ..
        } = reader_harness().await;

        // 启动 reader 任务。
        let reader_handle =
            tokio::spawn(
                async move { handle_reader(framed_reader, controller, alive_tx, None).await },
            );

        // drop alive_rx 以关闭通道（模拟 writer 结束）。
        drop(alive_rx);

        // reader 应该因为 alive 通道关闭而退出。
        let result = reader_handle.await;

        assert!(
            result.is_ok(),
            "handle_reader should exit when alive channel is closed"
        );
    }

    /// 测试当 TCP 流关闭时，handle_reader 会退出。
    #[tokio::test]
    async fn test_handle_reader_exits_on_stream_closed() {
        let ReaderHarness {
            mut framed_server,
            framed_reader,
            alive_tx,
            alive_rx: _alive_rx,
            controller,
        } = reader_harness().await;

        // 启动 reader 任务。
        let reader_handle =
            tokio::spawn(
                async move { handle_reader(framed_reader, controller, alive_tx, None).await },
            );

        // 关闭 framed server，向 client 发送 EOF 信号。
        framed_server.close().await.unwrap();

        // reader 应该因为流关闭而退出。
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), reader_handle).await;

        assert!(
            result.is_ok(),
            "handle_reader should exit when stream is closed"
        );
    }

    /// 测试 handle_reader 能按顺序处理多条控制消息。
    #[tokio::test]
    async fn test_handle_reader_multiple_control_messages() {
        let ReaderHarness {
            mut framed_server,
            framed_reader,
            alive_tx,
            alive_rx: _alive_rx,
            controller,
        } = reader_harness().await;

        // 启动 reader 任务。
        let controller_clone = controller.clone();
        let reader_handle = tokio::spawn(async move {
            handle_reader(framed_reader, controller_clone, alive_tx, None).await
        });

        // 发送多条 Stop 消息（第一条会 stop，后续消息都是 no-op）。
        framed_server
            .send(control_message(&ControlMessage::Stop))
            .await
            .unwrap();
        framed_server
            .send(control_message(&ControlMessage::Stop))
            .await
            .unwrap();

        // 关闭 framed server，向 client 发送 EOF 信号。
        framed_server.close().await.unwrap();

        // 等待 reader 结束。
        let _ = reader_handle.await.unwrap();

        // 验证 stop 已被调用。
        assert!(
            controller.is_stopped(),
            "Controller should be stopped after receiving Stop messages"
        );
    }

    /// 测试 handle_reader 中 Stop 后接 Kill 的处理。
    #[tokio::test]
    async fn test_handle_reader_stop_then_kill() {
        let ReaderHarness {
            mut framed_server,
            framed_reader,
            alive_tx,
            alive_rx: _alive_rx,
            controller,
        } = reader_harness().await;

        // 启动 reader 任务。
        let controller_clone = controller.clone();
        let reader_handle = tokio::spawn(async move {
            handle_reader(framed_reader, controller_clone, alive_tx, None).await
        });

        // 先发送 Stop，再发送 Kill。
        framed_server
            .send(control_message(&ControlMessage::Stop))
            .await
            .unwrap();
        framed_server
            .send(control_message(&ControlMessage::Kill))
            .await
            .unwrap();

        // 关闭 framed server，向 client 发送 EOF 信号。
        framed_server.close().await.unwrap();

        // 等待 reader 结束。
        let _ = reader_handle.await.unwrap();

        // 验证 kill 已被调用（它会把状态设为 killed）。
        assert!(
            controller.is_killed(),
            "Controller should be killed after receiving Kill message"
        );
    }

    /// 读错误会 kill context，并计入取消次数。
    #[tokio::test]
    async fn test_handle_reader_increments_cancellation_counter_on_read_error() {
        let ReaderHarness {
            framed_server,
            framed_reader,
            alive_tx,
            alive_rx: _alive_rx,
            controller,
        } = reader_harness().await;
        let cancellation_counter = IntCounter::new(
            "tcp_client_reader_read_error_cancellations_test",
            "test cancellation counter",
        )
        .unwrap();

        let counter_clone = cancellation_counter.clone();
        let controller_clone = controller.clone();
        let reader_handle = tokio::spawn(async move {
            handle_reader(
                framed_reader,
                controller_clone,
                alive_tx,
                Some(counter_clone),
            )
            .await
        });

        let mut raw_writer = framed_server.into_inner();
        raw_writer.write_all(&[0u8; 8]).await.unwrap();
        raw_writer.shutdown().await.unwrap();

        let _ = reader_handle.await.unwrap();

        assert!(
            controller.is_killed(),
            "Controller should be killed after TCP stream read error"
        );
        assert_eq!(
            cancellation_counter.get(),
            1,
            "read-error close should increment cancellation metric once"
        );
    }

    /// 用单条消息驱动 `handle_reader`，并返回 controller 与取消计数器供断言使用。
    async fn run_reader_with(
        msg: TwoPartMessage,
        counter_name: &str,
    ) -> (Arc<Controller>, IntCounter) {
        let ReaderHarness {
            mut framed_server,
            framed_reader,
            alive_tx,
            alive_rx: _alive_rx,
            controller,
        } = reader_harness().await;
        let counter = IntCounter::new(counter_name, "test counter").unwrap();

        let counter_clone = counter.clone();
        let controller_clone = controller.clone();
        let reader_handle = tokio::spawn(async move {
            handle_reader(
                framed_reader,
                controller_clone,
                alive_tx,
                Some(counter_clone),
            )
            .await
        });

        framed_server.send(msg).await.unwrap();
        let _ = reader_handle.await.unwrap();

        (controller, counter)
    }

    /// 每一种违反协议的消息都只能杀掉当前流（controller 进入 killed，
    /// 取消计数只加一次），不能让 worker panic。覆盖 `handle_reader` 中
    /// 三种非读错误的 panic 分支：无法解码的控制字节、服务端发送的 Sentinel，
    /// 以及非控制类消息。
    /// (data-only) messages.
    #[tokio::test]
    async fn test_handle_reader_kills_on_protocol_violations() {
        let cases: Vec<(&str, TwoPartMessage)> = vec![
            (
                "invalid control bytes",
                TwoPartMessage::from_header(Bytes::from_static(b"not a valid control message")),
            ),
            (
                "sentinel from server",
                control_message(&ControlMessage::Sentinel),
            ),
            (
                "non-control (data-only)",
                TwoPartMessage::from_data(Bytes::from_static(b"unexpected payload")),
            ),
        ];

        for (i, (label, msg)) in cases.into_iter().enumerate() {
            let counter_name = format!("tcp_client_reader_protocol_violation_test_{i}");
            let (controller, counter) = run_reader_with(msg, &counter_name).await;
            assert!(
                controller.is_killed(),
                "{label}: should kill stream context"
            );
            assert_eq!(counter.get(), 1, "{label}: should be counted once");
        }
    }
}
