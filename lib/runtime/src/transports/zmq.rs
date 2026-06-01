// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 设计意图
//! 本模块基于 [`tmq`] 提供一个 *点对点* 的 ZMQ 传输层：服务端使用 ROUTER 套接字
//! 维护"按 request_id 分流"的多路复用，客户端使用 DEALER 套接字与之通信。
//! 这是 ZeroMQ Harmony 模式的简化实现,目标是替代直接 TCP 流——以一个连接池
//! 承载多个上行调用,代价是每个请求多一次内部路由步骤.
//!
//! # 外部契约
//! - 帧契约(wire-level,必须稳定):ROUTER 在 strip identity 后看到的有效消息恰好
//!   *3 个* 帧:`[identity, request_id_utf8, message_bytes]`;违反契约直接中止 server
//!   并取消父 token(supplemental 测试 `server_new_propagates_fatal_broken_contract_error...`
//!   依赖此行为).
//! - 公共 API:
//!     - `Server::new(ctx, addr, cancel_token) -> Result<(Self, ServerExecutionHandle)>`
//!     - `ServerExecutionHandle::{is_finished, is_cancelled, cancel, join}`
//!     - `Client::new` + `Client::dealer()`(同模块可见即可)
//!     - 类型别名 `pub type MultipartMessage = Vec<Vec<u8>>;`
//! - 内部但测试可见:`RouterState::{new, register_stream, remove_stream}` 与
//!   `Server::state` 字段——不能改名/改可见性.
//!
//! # 实现要点
//! - 服务端事件循环用 `tokio::select! biased` 优先处理 ROUTER 的下一帧,
//!   其次响应 cancellation;ROUTER 流终止则正常退出循环.
//! - 数据派发分三态:`SendEager` / `SendDelayed` / `Close`,分别对应 try_send 成功,
//!   遇到 `Full` 时回退到 await `send` 等待,以及通道被关闭立即移除条目.
//! - server task 的失败由一个 watchdog 包装任务捕获,在任一层失败时取消父 token,
//!   确保业务侧能及时感知不可恢复错误.

use anyhow::Result;
use bytes::Bytes;
use derive_getters::Dissolve;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tmq::{AsZmqSocket, Context as TmqContext, dealer, router};
use tokio::{
    sync::{Mutex, mpsc},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

// === SECTION: wire types ===

/// 多部分消息的别名(每个内层 `Vec<u8>` 对应一帧).
pub type MultipartMessage = Vec<Vec<u8>>;

/// 控制消息——目前未在 wire 上启用,保留以便后续扩展取消/错误/完成信号.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum ControlMessage {
    Cancel { request_id: String },
    CancelAck { request_id: String },
    Error { request_id: String, error: String },
    Complete { request_id: String },
}

/// 顶层消息类型——保留供未来扩展使用.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum MessageType {
    Data(Vec<u8>),
    Control(ControlMessage),
}

/// 单帧数据派发后产生的动作.
enum StreamAction {
    SendEager(usize),
    SendDelayed(usize),
    Close,
}

// === SECTION: RouterState ===

/// 服务端路由表:把 request_id 映射到数据/控制通道的发送端.
///
/// 字段对同模块测试可见,保持私有即可.
struct RouterState {
    active_streams: HashMap<String, mpsc::Sender<Bytes>>,
    control_channels: HashMap<String, mpsc::Sender<ControlMessage>>,
}

impl RouterState {
    fn new() -> Self {
        Self {
            active_streams: HashMap::new(),
            control_channels: HashMap::new(),
        }
    }

    fn register_stream(
        &mut self,
        request_id: String,
        data_tx: mpsc::Sender<Bytes>,
        control_tx: mpsc::Sender<ControlMessage>,
    ) {
        self.active_streams.insert(request_id.clone(), data_tx);
        self.control_channels.insert(request_id, control_tx);
    }

    fn remove_stream(&mut self, request_id: &str) {
        self.active_streams.remove(request_id);
        self.control_channels.remove(request_id);
    }
}

// === SECTION: Server ===

/// ZMQ ROUTER 服务端句柄;通过 [`Server::new`] 创建并与 [`ServerExecutionHandle`] 配对.
#[derive(Clone, Dissolve)]
pub struct Server {
    state: Arc<Mutex<RouterState>>,
    cancel_token: CancellationToken,
    fd: i32,
}

impl Server {
    /// 在给定 ZMQ 上下文上绑定 ROUTER 到 `address`,并启动事件循环.
    ///
    /// 若事件循环失败(如帧契约被破坏),错误会通过包装任务传播到 `cancel_token`.
    pub async fn new(
        context: &TmqContext,
        address: &str,
        cancel_token: CancellationToken,
    ) -> Result<(Self, ServerExecutionHandle)> {
        let router = router(context).bind(address)?;
        let fd = router.get_socket().get_fd()?;
        let state = Arc::new(Mutex::new(RouterState::new()));

        // child_token 让 watch_task 既能独立取消事件循环,又能在 server 失败时
        // 反向取消父 token,触发业务侧的关停流程.
        let child = cancel_token.child_token();
        let primary_task = tokio::spawn(Self::run(router, state.clone(), child.child_token()));

        let watch_task = tokio::spawn(async move {
            let result = primary_task.await.inspect_err(|e| {
                tracing::error!("zmq server/router task failed: {e}");
                cancel_token.cancel();
            })?;
            result.inspect_err(|e| {
                tracing::error!("zmq server/router task failed: {e}");
                cancel_token.cancel();
            })
        });

        let handle = ServerExecutionHandle {
            task: watch_task,
            cancel_token: child.clone(),
        };

        Ok((
            Self {
                state,
                cancel_token: child,
                fd,
            },
            handle,
        ))
    }

    /// 事件循环主体;遵循 *3 帧契约*:`[identity, request_id, message]`.
    async fn run(
        mut router: tmq::router::Router,
        state: Arc<Mutex<RouterState>>,
        token: CancellationToken,
    ) -> Result<()> {
        loop {
            // 优先处理 ROUTER 的下一帧;其次响应取消.
            let frames = tokio::select! {
                biased;

                next = router.next() => match next {
                    Some(Ok(frames)) => frames,
                    Some(Err(e)) => {
                        tracing::warn!("Error receiving message: {e}");
                        continue;
                    }
                    None => break,
                },

                _ = token.cancelled() => {
                    tracing::info!("Server shutting down");
                    break;
                }
            };

            // 帧契约校验——破坏即致命.
            if frames.len() != 3 {
                anyhow::bail!(
                    "Fatal Error -- Broken contract -- Expected 3 frames, got {}",
                    frames.len()
                );
            }

            let request_id = String::from_utf8_lossy(&frames[1]).to_string();
            let payload = frames[2].to_vec();
            Self::dispatch_frame(&state, request_id, payload).await;
        }

        Ok(())
    }

    /// 按 request_id 查找目标流并尝试派发:eager → delayed → close.
    async fn dispatch_frame(
        state: &Arc<Mutex<RouterState>>,
        request_id: String,
        payload: Vec<u8>,
    ) {
        let message_size = payload.len();
        // 持锁时间尽量短:取出 sender 的克隆后立即释放锁.
        let sender_opt = state.lock().await.active_streams.get(&request_id).cloned();

        let Some(tx) = sender_opt else {
            tracing::trace!(request_id, "no active stream for request_id");
            return;
        };

        let action = match tx.try_send(payload.into()) {
            Ok(()) => {
                tracing::trace!(
                    request_id,
                    "response data sent eagerly to stream: {} bytes",
                    message_size
                );
                StreamAction::SendEager(message_size)
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::info!(request_id, "response stream was closed");
                StreamAction::Close
            }
            Err(mpsc::error::TrySendError::Full(data)) => {
                tracing::warn!(request_id, "response stream is full; backpressure alert");
                // TODO: 加入超时,避免单流卡住其他流.
                if tx.send(data).await.is_err() {
                    StreamAction::Close
                } else {
                    StreamAction::SendDelayed(message_size)
                }
            }
        };

        match action {
            StreamAction::SendEager(_) | StreamAction::SendDelayed(_) => {
                // 指标点位预留——后续可接入 metrics::counter.
            }
            StreamAction::Close => {
                state.lock().await.active_streams.remove(&request_id);
            }
        }
    }
}

// === SECTION: ServerExecutionHandle ===

/// 服务端后台任务的远程句柄:可查询/取消/join.
pub struct ServerExecutionHandle {
    task: JoinHandle<Result<()>>,
    cancel_token: CancellationToken,
}

impl ServerExecutionHandle {
    /// 后台任务是否已结束(成功或失败均返回 true).
    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    /// 后台任务的 cancellation 是否已被触发.
    pub fn is_cancelled(&self) -> bool {
        self.cancel_token.is_cancelled()
    }

    /// 触发后台任务的取消(立即返回,不等待 join).
    pub fn cancel(&self) {
        self.cancel_token.cancel();
    }

    /// 等待后台任务结束并返回其结果.
    pub async fn join(self) -> Result<()> {
        self.task.await?
    }
}

// === SECTION: Client ===

/// ZMQ DEALER 客户端,直接连接服务端 ROUTER.
pub struct Client {
    dealer: tmq::dealer::Dealer,
}

impl Client {
    fn new(context: &TmqContext, address: &str) -> Result<Self> {
        let dealer = dealer(context).connect(address)?;
        Ok(Self { dealer })
    }

    fn dealer(&mut self) -> &mut tmq::dealer::Dealer {
        &mut self.dealer
    }
}

// === SECTION: tests ===

#[cfg(test)]
mod tests {
    use super::*;
    use futures::SinkExt;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_basic_communication() -> Result<()> {
        let context = TmqContext::new();
        let address = "tcp://127.0.0.1:1337";
        let token = CancellationToken::new();

        // Start server
        let (server, handle) = Server::new(&context, address, token.clone()).await?;
        let state = server.state.clone();

        let id = "test-request".to_string();
        let (tx, mut rx) = tokio::sync::mpsc::channel(512);
        state.lock().await.active_streams.insert(id.clone(), tx);

        // Create client
        let mut client = Client::new(&context, address)?;

        client
            .dealer()
            .send(tmq::Multipart::from(vec![
                id.as_bytes().to_vec(),
                id.as_bytes().to_vec(),
            ]))
            .await?;

        let receive_result = timeout(std::time::Duration::from_secs(2), rx.recv()).await?;
        let received = receive_result.unwrap();

        let received_str = String::from_utf8_lossy(&received).to_string();
        assert_eq!(received_str, "test-request");

        drop(client);

        handle.cancel();
        handle.join().await?;

        println!("done");
        Ok(())
    }

    // === SECTION: 合并自原 mod supplemental_tests ===
    // ## 测试过程
    // 覆盖 7 类路径:RouterState 注册/移除,Client 基本发送,ServerExecutionHandle
    // 的生命周期方法,未知 request_id 不致命,SendDelayed 路径,3 帧契约违反时
    // 取消父 token,以及 Client 拒绝非法地址.
    //
    // ## 意义
    // 这些路径是 ZMQ 传输的可观察行为契约,覆盖错误传播,背压,契约校验三大要点.

    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{Duration, sleep};

    static PORT_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn unique_portname() -> String {
        // Reserve a best-effort unique local TCP port for test bindings.
        let probe = std::net::TcpListener::bind("127.0.0.1:0")
            .expect("should bind an ephemeral probe port");
        let port = probe
            .local_addr()
            .expect("probe should have local addr")
            .port();
        drop(probe);

        // Add a tiny deterministic jitter to reduce race risk between tests.
        let offset = (PORT_COUNTER.fetch_add(1, Ordering::SeqCst) % 7) as u16;
        format!("tcp://127.0.0.1:{}", port.saturating_add(offset))
    }

    #[test]
    fn router_state_register_and_remove_streams() {
        let mut state = RouterState::new();
        assert!(state.active_streams.is_empty());
        assert!(state.control_channels.is_empty());

        let (data_tx, _data_rx) = mpsc::channel(2);
        let (ctrl_tx, _ctrl_rx) = mpsc::channel(2);
        state.register_stream("req-1".to_string(), data_tx, ctrl_tx);

        assert!(state.active_streams.contains_key("req-1"));
        assert!(state.control_channels.contains_key("req-1"));

        state.remove_stream("req-1");
        assert!(!state.active_streams.contains_key("req-1"));
        assert!(!state.control_channels.contains_key("req-1"));
    }

    #[tokio::test]
    async fn client_new_and_dealer_send_message() -> Result<()> {
        let context = TmqContext::new();
        let portname = unique_portname();
        let token = CancellationToken::new();

        let (server, handle) = Server::new(&context, &portname, token.clone()).await?;

        let id = "req-client".to_string();
        let (tx, mut rx) = mpsc::channel(8);
        server
            .state
            .lock()
            .await
            .register_stream(id.clone(), tx, mpsc::channel(1).0);

        let mut client = Client::new(&context, &portname)?;
        client
            .dealer()
            .send(tmq::Multipart::from(vec![
                id.as_bytes().to_vec(),
                b"hello".to_vec(),
            ]))
            .await?;

        let received = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("server should route message")
            .expect("receiver should get bytes");
        assert_eq!(received, Bytes::from_static(b"hello"));

        handle.cancel();
        handle.join().await?;
        Ok(())
    }

    #[tokio::test]
    async fn server_execution_handle_methods_cover_lifecycle() -> Result<()> {
        let context = TmqContext::new();
        let portname = unique_portname();
        let token = CancellationToken::new();

        let (_server, handle) = Server::new(&context, &portname, token.clone()).await?;
        let _ = handle.is_finished();
        assert!(!handle.is_cancelled());

        handle.cancel();
        assert!(handle.is_cancelled());

        handle.join().await?;
        Ok(())
    }

    #[tokio::test]
    async fn server_run_ignores_unknown_stream_request_ids() -> Result<()> {
        let context = TmqContext::new();
        let portname = unique_portname();
        let token = CancellationToken::new();
        let (_server, handle) = Server::new(&context, &portname, token.clone()).await?;

        let mut client = Client::new(&context, &portname)?;
        client
            .dealer()
            .send(tmq::Multipart::from(vec![
                b"unknown-request".to_vec(),
                b"payload".to_vec(),
            ]))
            .await?;

        // Server should keep running because unknown request_id is non-fatal.
        sleep(Duration::from_millis(100)).await;
        assert!(!handle.is_finished());

        handle.cancel();
        handle.join().await?;
        Ok(())
    }

    #[tokio::test]
    async fn server_full_channel_triggers_delayed_send_path() -> Result<()> {
        let context = TmqContext::new();
        let portname = unique_portname();
        let token = CancellationToken::new();
        let (server, handle) = Server::new(&context, &portname, token.clone()).await?;

        let id = "req-full".to_string();
        let (tx, mut rx) = mpsc::channel::<Bytes>(1);
        server
            .state
            .lock()
            .await
            .register_stream(id.clone(), tx, mpsc::channel(1).0);

        let mut client = Client::new(&context, &portname)?;

        // First message fills the channel via eager send.
        client
            .dealer()
            .send(tmq::Multipart::from(vec![
                id.as_bytes().to_vec(),
                b"first".to_vec(),
            ]))
            .await?;

        // Second message should hit TrySendError::Full and follow delayed send path.
        client
            .dealer()
            .send(tmq::Multipart::from(vec![
                id.as_bytes().to_vec(),
                b"second".to_vec(),
            ]))
            .await?;

        // Drain first then second; this unblocks delayed send.
        let first = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("first receive should complete")
            .expect("first message should exist");
        assert_eq!(first, Bytes::from_static(b"first"));

        let second = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("second receive should complete")
            .expect("second message should exist");
        assert_eq!(second, Bytes::from_static(b"second"));

        handle.cancel();
        handle.join().await?;
        Ok(())
    }

    #[tokio::test]
    async fn server_new_propagates_fatal_broken_contract_error_and_cancels_parent_token()
    -> Result<()> {
        let context = TmqContext::new();
        let portname = unique_portname();
        let parent = CancellationToken::new();

        let (_server, handle) = Server::new(&context, &portname, parent.clone()).await?;
        let mut client = Client::new(&context, &portname)?;

        // Sending a single frame results in identity+payload (2 frames) at router,
        // which violates the 3-frame contract and should terminate the server.
        client
            .dealer()
            .send(tmq::Multipart::from(vec![b"only-one-frame".to_vec()]))
            .await?;

        let join_err = handle
            .join()
            .await
            .err()
            .expect("fatal contract violation should surface as error");
        assert!(
            join_err
                .to_string()
                .contains("Broken contract -- Expected 3 frames")
        );

        assert!(parent.is_cancelled());
        Ok(())
    }

    #[test]
    fn client_new_rejects_invalid_address() {
        let context = TmqContext::new();
        let result = Client::new(&context, "invalid-address");
        assert!(result.is_err());
    }
}

