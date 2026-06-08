// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-License-Identifier: Apache-2.0
//
// API-compatible implementation based on the public interfaces and behavioral
// contracts of NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
// Implementation rewritten by PAGODA.

//! # 分离式部署的 bootstrap 会合（rendezvous）
//!
//! ## 设计意图
//! 为分离式（disaggregated）serving 模拟 prefill 与 decode 之间的 KV 搬运握手。
//! prefill 与 decode 谁先到都可以；当两者都就绪时会合完成。
//!
//! - prefill：产出首 token（KV cache 就绪）后调用 [`BootstrapServer::complete_room`]。
//! - decode：连接 prefill 的 bootstrap server，阻塞直到 prefill 完成。
//!
//! ## 外部契约
//! 线路协议（wire protocol）必须保持不变：
//! - decode → prefill：`room_id`（8 字节，小端 `u64`）
//! - prefill → decode：prefill 完成后回 ACK（1 字节，`0x01`）
//!
//! 公开面同样保持稳定：[`BootstrapServer::start`] / [`BootstrapServer::complete_room`]
//! / [`BootstrapServer::port`] 与 [`connect_to_prefill`]。会合超时为 30 秒。
//!
//! ## 实现要点
//! 每个 `room_id` 对应一份 [`RoomState`]，用 [`DashMap`] 并发管理。decode 等待时通过
//! [`oneshot`] 通道挂起，prefill 完成时唤醒。两种到达顺序：
//! - prefill 先到：标记 `prefill_ready`，decode 连上即立即 ACK；
//! - decode 先到：登记等待通道，prefill 完成时发送唤醒信号并清理房间。

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

// === SECTION: 常量与状态 ===

/// 会合操作的超时时长。
const RENDEZVOUS_TIMEOUT: Duration = Duration::from_secs(30);

/// prefill 完成后服务端发给 decode 的 ACK 字节。
const ACK_BYTE: u8 = 0x01;

/// 单个房间的会合状态。
struct RoomState {
    /// prefill 是否已完成（KV cache 就绪）。
    prefill_ready: bool,
    /// 当 decode 正在等待时，用于唤醒它的发送端。
    waiter: Option<oneshot::Sender<()>>,
}

// === SECTION: BootstrapServer ===

/// prefill 端的 bootstrap server，负责 prefill 与 decode 间的 KV 搬运会合。
pub struct BootstrapServer {
    port: u16,
    rooms: Arc<DashMap<u64, RoomState>>,
}

impl BootstrapServer {
    /// 在指定端口启动 bootstrap server。`port` 为 0 时由系统分配空闲端口。
    pub async fn start(port: u16, cancel_token: CancellationToken) -> Result<Arc<Self>> {
        let listener = TcpListener::bind(format!("0.0.0.0:{port}")).await?;
        let actual_port = listener.local_addr()?.port();

        tracing::info!("Bootstrap server started on port {actual_port}");

        let rooms: Arc<DashMap<u64, RoomState>> = Arc::new(DashMap::new());
        let server = Arc::new(Self {
            port: actual_port,
            rooms: Arc::clone(&rooms),
        });

        // 接收循环：每个连接交给独立任务处理；收到取消信号即退出。
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    accepted = listener.accept() => match accepted {
                        Ok((stream, addr)) => {
                            tracing::debug!("Bootstrap: accepted connection from {addr}");
                            let rooms = Arc::clone(&rooms);
                            tokio::spawn(async move {
                                if let Err(e) = Self::serve_decode(stream, rooms).await {
                                    tracing::warn!("Bootstrap: connection error: {e}");
                                }
                            });
                        }
                        Err(e) => tracing::warn!("Bootstrap: accept failed: {e}"),
                    },
                    _ = cancel_token.cancelled() => {
                        tracing::debug!("Bootstrap server shutting down");
                        break;
                    }
                }
            }
        });

        Ok(server)
    }

    /// 处理来自 decode 的一条连接：读取 `room_id`，阻塞至该房间 prefill 完成后回 ACK。
    async fn serve_decode(
        mut stream: TcpStream,
        rooms: Arc<DashMap<u64, RoomState>>,
    ) -> Result<()> {
        // 读取 room_id（8 字节小端）。
        let mut room_id_bytes = [0u8; 8];
        stream.read_exact(&mut room_id_bytes).await?;
        let room_id = u64::from_le_bytes(room_id_bytes);

        tracing::debug!("Bootstrap: decode connected for room {room_id}");

        // 决定是否需要等待，必要时登记唤醒通道。
        let pending = match rooms.entry(room_id) {
            Entry::Occupied(mut occupied) => {
                if occupied.get().prefill_ready {
                    // prefill 已完成：立即 ACK，并清理房间。
                    occupied.remove();
                    tracing::debug!(
                        "Bootstrap: room {room_id} already completed, immediate ACK"
                    );
                    None
                } else {
                    // prefill 已登记但未完成：挂起等待。
                    let (tx, rx) = oneshot::channel();
                    occupied.get_mut().waiter = Some(tx);
                    tracing::debug!(
                        "Bootstrap: room {room_id} waiting for prefill to complete"
                    );
                    Some(rx)
                }
            }
            Entry::Vacant(vacant) => {
                // decode 先到：创建房间并等待。
                let (tx, rx) = oneshot::channel();
                vacant.insert(RoomState {
                    prefill_ready: false,
                    waiter: Some(tx),
                });
                tracing::debug!("Bootstrap: room {room_id} decode arrived first, waiting");
                Some(rx)
            }
        };

        // 如需等待，则阻塞至 prefill 完成或超时。
        if let Some(rx) = pending {
            match tokio::time::timeout(RENDEZVOUS_TIMEOUT, rx).await {
                Ok(Ok(())) => {
                    tracing::debug!(
                        "Bootstrap: room {room_id} prefill completed, sending ACK"
                    );
                }
                Ok(Err(_)) => bail!("Bootstrap: room {room_id} sender dropped"),
                Err(_) => {
                    rooms.remove(&room_id);
                    bail!("Bootstrap: room {room_id} timeout waiting for prefill");
                }
            }
        }

        // 回 ACK。
        stream.write_all(&[ACK_BYTE]).await?;
        Ok(())
    }

    /// 标记某房间已完成（prefill 收尾、KV cache 就绪）。若 decode 已在等待则唤醒它。
    pub fn complete_room(&self, room_id: u64) {
        match self.rooms.entry(room_id) {
            Entry::Occupied(mut occupied) => {
                if let Some(waiter) = occupied.get_mut().waiter.take() {
                    // decode 正在等待：唤醒并清理房间。
                    let _ = waiter.send(());
                    occupied.remove();
                    tracing::debug!("Bootstrap: room {room_id} completed, decode unblocked");
                } else {
                    // decode 尚未连接：标记完成，等待其到来。
                    occupied.get_mut().prefill_ready = true;
                    tracing::debug!("Bootstrap: room {room_id} completed, awaiting decode");
                }
            }
            Entry::Vacant(vacant) => {
                // decode 尚未连接：先建好已完成的房间。
                vacant.insert(RoomState {
                    prefill_ready: true,
                    waiter: None,
                });
                tracing::debug!("Bootstrap: room {room_id} completed (no decode yet)");
            }
        }
    }

    /// 返回 server 实际监听的端口。
    pub fn port(&self) -> u16 {
        self.port
    }
}

// === SECTION: decode 侧连接 ===

/// 连接 prefill worker 的 bootstrap server，等待其 KV 就绪。
pub async fn connect_to_prefill(host: &str, port: u16, room_id: u64) -> Result<()> {
    // 去掉 IPv6 字面量的方括号。
    let host = host.trim_matches(|c| c == '[' || c == ']');
    let addr = format!("{host}:{port}");

    tracing::debug!("Bootstrap: decode connecting to {addr} for room {room_id}");

    let mut stream = tokio::time::timeout(RENDEZVOUS_TIMEOUT, TcpStream::connect(&addr))
        .await
        .map_err(|_| anyhow::anyhow!("Bootstrap: connect timeout to {addr}"))?
        .map_err(|e| anyhow::anyhow!("Bootstrap: connect failed to {addr}: {e}"))?;

    // 发送 room_id（小端）。
    stream.write_all(&room_id.to_le_bytes()).await?;

    // 阻塞读取 ACK（直到 prefill 完成）。
    let mut ack = [0u8; 1];
    tokio::time::timeout(RENDEZVOUS_TIMEOUT, stream.read_exact(&mut ack))
        .await
        .map_err(|_| anyhow::anyhow!("Bootstrap: ACK timeout for room {room_id}"))?
        .map_err(|e| anyhow::anyhow!("Bootstrap: read ACK failed: {e}"))?;

    if ack[0] != ACK_BYTE {
        bail!(
            "Bootstrap: invalid ACK byte {:02x} for room {room_id}",
            ack[0]
        );
    }

    tracing::debug!("Bootstrap: decode received ACK for room {room_id}");
    Ok(())
}

// === SECTION: 测试 ===

#[cfg(test)]
mod tests {
    use super::*;

    /// 启动一个随机端口的 server，返回 `(server, port, cancel_token)`。
    async fn spawn_server() -> (Arc<BootstrapServer>, u16, CancellationToken) {
        let cancel = CancellationToken::new();
        let server = BootstrapServer::start(0, cancel.clone()).await.unwrap();
        let port = server.port();
        (server, port, cancel)
    }

    #[tokio::test]
    async fn prefill_ready_before_decode_gets_immediate_ack() {
        // ## 测试过程
        // prefill 先 complete_room，随后 decode 才连接。
        // ## 意义
        // 验证 prefill 先到时 decode 立即收到 ACK 成功返回。
        let (server, port, cancel) = spawn_server().await;
        server.complete_room(1001);
        assert!(connect_to_prefill("127.0.0.1", port, 1001).await.is_ok());
        cancel.cancel();
    }

    #[tokio::test]
    async fn decode_waits_until_prefill_completes() {
        // ## 测试过程
        // decode 先连接并阻塞，稍后 prefill 才 complete_room。
        // ## 意义
        // 验证 decode 先到时会挂起，prefill 完成后被唤醒并成功收到 ACK。
        let (server, port, cancel) = spawn_server().await;
        let decode = tokio::spawn(async move {
            connect_to_prefill("127.0.0.1", port, 1002).await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        server.complete_room(1002);
        assert!(decode.await.unwrap().is_ok());
        cancel.cancel();
    }

    #[tokio::test]
    async fn concurrent_rooms_resolve_independently() {
        // ## 测试过程
        // 并发处理三个房间：分别为 prefill 先到、decode 先到、近乎同时三种顺序。
        // ## 意义
        // 验证多房间互不干扰，三种到达顺序都能完成会合。
        let (server, port, cancel) = spawn_server().await;

        // 房间 A：prefill 先到。
        let sa = server.clone();
        let a = tokio::spawn(async move {
            sa.complete_room(2001);
            tokio::time::sleep(Duration::from_millis(10)).await;
            connect_to_prefill("127.0.0.1", port, 2001).await
        });
        // 房间 B：decode 先到。
        let sb = server.clone();
        let b = tokio::spawn(async move {
            let decode = tokio::spawn(connect_to_prefill("127.0.0.1", port, 2002));
            tokio::time::sleep(Duration::from_millis(50)).await;
            sb.complete_room(2002);
            decode.await.unwrap()
        });
        // 房间 C：近乎同时。
        let sc = server.clone();
        let c = tokio::spawn(async move {
            let decode = tokio::spawn(connect_to_prefill("127.0.0.1", port, 2003));
            sc.complete_room(2003);
            decode.await.unwrap()
        });

        for handle in [a, b, c] {
            assert!(handle.await.unwrap().is_ok());
        }
        cancel.cancel();
    }

    #[tokio::test]
    async fn decode_times_out_without_prefill() {
        // ## 测试过程
        // decode 连接一个 prefill 永不完成的房间，用一个较短的外层超时包裹。
        // ## 意义
        // 验证无 prefill 时 decode 会一直等待（被外层短超时打断），不会错误地提前成功。
        let (_server, port, cancel) = spawn_server().await;
        let outcome = tokio::time::timeout(
            Duration::from_millis(100),
            connect_to_prefill("127.0.0.1", port, 9999),
        )
        .await;
        assert!(outcome.is_err(), "should still be waiting for prefill");
        cancel.cancel();
    }
}
