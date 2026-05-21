// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! TCP stream server — accepts inbound connections and dispatches frames.
//!
//! 帧格式（`TwoPartCodec`）：
//! ```text
//! [header_len: u32][body_len: u32][header bytes][body bytes]
//! ```
//! header 为 UTF-8 路由路径（如 `"ns.sg.portname"`），body 为请求负载。

use std::net::SocketAddr;
use std::sync::Arc;

use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;

use crate::pipeline::error::PipelineError;
use crate::pipeline::network::codec::two_part::{TwoPartCodec, TwoPartFrame};

/// Type-erased frame handler：接受 body bytes，返回响应 bytes。
pub type BoxedFrameHandler =
    Arc<dyn Fn(bytes::Bytes) -> futures::future::BoxFuture<'static, Result<bytes::Bytes, PipelineError>> + Send + Sync>;

/// 单端口 TCP 服务器，通过帧头中的路径多路复用多个 endpoint。
pub struct TcpStreamServer {
    /// listener 包在 Mutex 中以支持通过 Arc 调用 take()。
    pub listener: Mutex<Option<TcpListener>>,
    pub local_addr: SocketAddr,
    pub cancel: CancellationToken,
}

impl TcpStreamServer {
    /// 绑定到指定地址。
    pub async fn bind(addr: SocketAddr, cancel: CancellationToken) -> Result<Self, PipelineError> {
        let listener = TcpListener::bind(addr).await
            .map_err(|e| PipelineError::transport(format!("TCP bind {addr}: {e}")))?;
        let local_addr = listener.local_addr()
            .map_err(|e| PipelineError::transport(format!("local_addr: {e}")))?;
        Ok(Self {
            listener: Mutex::new(Some(listener)),
            local_addr,
            cancel,
        })
    }

    /// 返回本地绑定地址。
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// 简单的接受循环（不路由帧），用于测试或作为 SharedTcpServer 的底层。
    /// 实际帧路由请使用 `SharedTcpServer::serve()`。
    pub async fn serve(&mut self) -> Result<(), PipelineError> {
        let listener_opt = self.listener.get_mut().take();
        let listener = listener_opt
            .ok_or_else(|| PipelineError::transport("TcpStreamServer: already serving"))?;
        let cancel = self.cancel.clone();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                result = listener.accept() => {
                    match result {
                        Ok((_stream, _peer)) => {}
                        Err(e) => {
                            tracing::warn!("TCP accept error: {e}");
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// 共享 TCP 服务器：同一监听端口支持多个 endpoint 注册 handler。
pub struct SharedTcpServer {
    pub inner: Arc<TcpStreamServer>,
    /// 路径 → 处理函数映射表。
    pub handlers: Arc<DashMap<String, BoxedFrameHandler>>,
}

impl SharedTcpServer {
    pub fn new(inner: TcpStreamServer) -> Self {
        Self {
            inner: Arc::new(inner),
            handlers: Arc::new(DashMap::new()),
        }
    }

    /// 注册 endpoint handler。
    pub fn register_endpoint(&self, path: impl Into<String>, handler: BoxedFrameHandler) {
        self.handlers.insert(path.into(), handler);
    }

    /// 注销 endpoint handler。
    pub fn unregister_endpoint(&self, path: &str) {
        self.handlers.remove(path);
    }

    /// 启动服务循环，接受连接并按帧头路径分发到对应 handler。
    ///
    /// 每个连接在独立 task 中处理，task 内串行读取帧、查找 handler、写回响应。
    pub async fn serve(&self) -> Result<(), PipelineError> {
        let listener = self.inner.listener.lock().await.take()
            .ok_or_else(|| PipelineError::transport("SharedTcpServer: already serving"))?;
        let cancel = self.inner.cancel.clone();
        let handlers = self.handlers.clone();

        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                result = listener.accept() => {
                    match result {
                        Ok((stream, peer)) => {
                            let handlers = handlers.clone();
                            let cancel = cancel.clone();
                            tokio::spawn(async move {
                                if let Err(e) = handle_connection(stream, peer, handlers, cancel).await {
                                    tracing::debug!("TCP connection {peer} error: {e}");
                                }
                            });
                        }
                        Err(e) => {
                            tracing::warn!("TCP accept error: {e}");
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// 处理单个 TCP 连接：循环读取帧并路由到对应 handler。
async fn handle_connection(
    stream: TcpStream,
    peer: SocketAddr,
    handlers: Arc<DashMap<String, BoxedFrameHandler>>,
    cancel: CancellationToken,
) -> Result<(), PipelineError> {
    let mut framed = Framed::new(stream, TwoPartCodec::default());

    loop {
        let frame = tokio::select! {
            _ = cancel.cancelled() => break,
            item = framed.next() => {
                match item {
                    Some(Ok(f)) => f,
                    Some(Err(e)) => {
                        tracing::debug!("TCP frame decode error from {peer}: {e}");
                        break;
                    }
                    None => break,  // connection closed
                }
            }
        };

        // 从帧头解析路径
        let path = match std::str::from_utf8(&frame.header) {
            Ok(p) => p.to_string(),
            Err(e) => {
                tracing::warn!("TCP frame header not valid UTF-8 from {peer}: {e}");
                break;
            }
        };

        // 查找 handler
        let handler = match handlers.get(&path) {
            Some(h) => h.clone(),
            None => {
                tracing::warn!("TCP: no handler for path '{path}' from {peer}");
                // 发送空响应（避免客户端挂起），然后继续
                let resp = TwoPartFrame {
                    header: bytes::Bytes::new(),
                    body: bytes::Bytes::new(),
                };
                if let Err(e) = framed.send(resp).await {
                    tracing::debug!("TCP send error-response to {peer}: {e}");
                    break;
                }
                continue;
            }
        };

        // 调用 handler
        match handler(frame.body).await {
            Ok(resp_body) => {
                let resp = TwoPartFrame {
                    header: bytes::Bytes::new(),
                    body: resp_body,
                };
                if let Err(e) = framed.send(resp).await {
                    tracing::debug!("TCP send response to {peer}: {e}");
                    break;
                }
            }
            Err(e) => {
                tracing::warn!("TCP handler error for path '{path}' from {peer}: {e}");
                // 发送错误消息体
                let err_bytes = e.to_string().into_bytes();
                let resp = TwoPartFrame {
                    header: bytes::Bytes::from_static(b"error"),
                    body: bytes::Bytes::from(err_bytes),
                };
                if let Err(send_err) = framed.send(resp).await {
                    tracing::debug!("TCP send error to {peer}: {send_err}");
                    break;
                }
            }
        }
    }

    Ok(())
}
