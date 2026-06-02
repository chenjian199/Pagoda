// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::network::tcp::server` —— 响应平面 TCP 服务器
//!
//! ## 设计意图
//! 运行在 ingress 侧的 TCP 底层服务器：接受客户端连接、在连接上跡调 `TwoPartCodec`
//! 帧、根据 header 中的 stream id 路由到对应响应接收者。与 `tcp_client` 成对，是
//! pipeline 响应回流的传输层基础。
//!
//! ## 外部契约
//! - 公开类型 / 方法一致；`socket2` 创建 monitor socket 的 keepalive /
//!   nodelay / reuseport 参数是契约。
//! - 帧格式严格遵循 `codec::TwoPartCodec`；不接受额外 magic / version 变体。
//!
//! ## 实现要点
//! - 接受循环使用 `loop { accept().await }` 模式，任何错误只警告、不退出，防止
//!   瞬间错误冲击全量服务；
//! - 每个连接一个 `tokio::spawn`，持有 `CancellationToken` 克隆，便于全局关停。

use socket2::{Domain, SockAddr, Socket, Type};
use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, SocketAddr, TcpListener},
    os::fd::{AsFd, FromRawFd},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Mutex;
use tokio::time::Instant;

/// 墓碑（Tombstone）生命周期。用于弥合 `register()` → `associate_instance()`
/// 之间的窗口（实践中为亚毫秒级）；5s 使该集合受近期 worker 变动而非进程
/// 生命周期约束，因为 etcd lease ID 每次重启唯一，且不会被同一身份的
/// `Added` 事件清除。
const TOMBSTONE_TTL: Duration = Duration::from_secs(5);

use bytes::Bytes;
use derive_builder::Builder;
use futures::{SinkExt, StreamExt};
use local_ip_address::{Error, list_afinet_netifas, local_ip, local_ipv6};

use serde::{Deserialize, Serialize};
use tokio::{
    io::AsyncWriteExt,
    sync::{mpsc, oneshot},
    time,
};
use tokio_util::codec::{FramedRead, FramedWrite};

use super::{
    CallHomeHandshake, ControlMessage, PendingConnections, RegisteredStream, StreamOptions,
    StreamReceiver, StreamSender, TcpStreamConnectionInfo, TwoPartCodec,
};
use crate::discovery::PortNameInstanceId;
use crate::engine::AsyncEngineContext;
use crate::pipeline::{
    PipelineError,
    network::{
        ResponseService, ResponseStreamPrologue,
        codec::{TwoPartMessage, TwoPartMessageType},
        tcp::StreamType,
    },
};
use anyhow::{Context, Result, anyhow as error};

// === SECTION: IpResolver + ServerOptions（公开类型）===
// IP 地址解析 trait —— 允许在测试中注入依赖
pub trait IpResolver {
    fn local_ip(&self) -> Result<std::net::IpAddr, Error>;
    fn local_ipv6(&self) -> Result<std::net::IpAddr, Error>;
}

// 使用真实 local_ip_address crate 的默认实现
pub struct DefaultIpResolver;

impl IpResolver for DefaultIpResolver {
    fn local_ip(&self) -> Result<std::net::IpAddr, Error> {
        local_ip()
    }

    fn local_ipv6(&self) -> Result<std::net::IpAddr, Error> {
        local_ipv6()
    }
}

#[allow(dead_code)]
type ResponseType = TwoPartMessage;

#[derive(Debug, Serialize, Deserialize, Clone, Builder, Default)]
pub struct ServerOptions {
    #[builder(default = "0")]
    pub port: u16,

    #[builder(default)]
    pub interface: Option<String>,
}

impl ServerOptions {
    pub fn builder() -> ServerOptionsBuilder {
        ServerOptionsBuilder::default()
    }
}

/// [`TcpStreamServer`] 是一个在端口上监听传入响应连接的 TCP 服务。
/// 响应连接是由客户端建立、用于将特定数据回传给服务器的连接。
// === SECTION: TcpStreamServer + 内部状态类型 ===
pub struct TcpStreamServer {
    local_ip: String,
    local_port: u16,
    state: Arc<Mutex<State>>,
}

#[allow(dead_code)]
struct RequestedSendConnection {
    context: Arc<dyn AsyncEngineContext>,
    connection: oneshot::Sender<Result<StreamSender, String>>,
}

struct RequestedRecvConnection {
    context: Arc<dyn AsyncEngineContext>,
    connection: oneshot::Sender<Result<StreamReceiver, String>>,
}

#[derive(Default)]
struct State {
    tx_subjects: HashMap<String, RequestedSendConnection>,
    rx_subjects: HashMap<String, RequestedRecvConnection>,
    /// subject UUID -> PortNameInstanceId。完整的 4 字段键可隔离跨
    /// namespace/servicegroup 共享同一 portname 名称的服务。
    subject_instance: HashMap<String, PortNameInstanceId>,
    /// PortNameInstanceId -> subject UUID 集合，用于移除时批量取消。
    instance_subjects: HashMap<PortNameInstanceId, HashSet<String>>,
    /// 墓碑（instance -> 插入时间）用于消解 `cancel_instance_streams`
    /// 与 `associate_instance` 之间的竞争；条目在 [`TOMBSTONE_TTL`] 后过期。
    removed_instances: HashMap<PortNameInstanceId, Instant>,
    handle: Option<tokio::task::JoinHandle<Result<()>>>,
}

/// 丢弃早于 [`TOMBSTONE_TTL`] 的墓碑。在每次 `associate_instance` /
/// `cancel_instance_streams` 时惰性调用，以限制集合大小。
fn prune_tombstones(tombstones: &mut HashMap<PortNameInstanceId, Instant>, now: Instant) {
    tombstones.retain(|_, ts| now.saturating_duration_since(*ts) < TOMBSTONE_TTL);
}

// === SECTION: TcpStreamServer 实现（构造函数 + ResponseService）===
impl TcpStreamServer {
    pub fn options_builder() -> ServerOptionsBuilder {
        ServerOptionsBuilder::default()
    }

    pub async fn new(options: ServerOptions) -> Result<Arc<Self>, PipelineError> {
        Self::new_with_resolver(options, DefaultIpResolver).await
    }

    pub async fn new_with_resolver<R: IpResolver>(
        options: ServerOptions,
        resolver: R,
    ) -> Result<Arc<Self>, PipelineError> {
        let local_ip = match options.interface {
            Some(interface) => {
                let interfaces: HashMap<String, std::net::IpAddr> =
                    list_afinet_netifas()?.into_iter().collect();

                interfaces
                    .get(&interface)
                    .ok_or(PipelineError::Generic(format!(
                        "Interface not found: {}",
                        interface
                    )))?
                    .to_string()
            }
            None => {
                let resolved_ip = resolver.local_ip().or_else(|err| match err {
                    Error::LocalIpAddressNotFound => resolver.local_ipv6(),
                    _ => Err(err),
                });

                match resolved_ip {
                    Ok(addr) => addr,
                    // 仅在根本不存在可路由 IP 时才回退到回环地址；
                    // 传播其他解析器错误（I/O、平台），使配置错误的
                    // 主机快速失败，而不是静默地绑定到 127.0.0.1。
                    Err(Error::LocalIpAddressNotFound) => {
                        tracing::warn!(
                            "No routable local IP address found; falling back to 127.0.0.1"
                        );
                        IpAddr::from([127, 0, 0, 1])
                    }
                    Err(err) => {
                        return Err(PipelineError::Generic(format!(
                            "Failed to resolve local IP address: {err}"
                        )));
                    }
                }
                .to_string()
            }
        };

        let state = Arc::new(Mutex::new(State::default()));

        let local_port = Self::start(local_ip.clone(), options.port, state.clone())
            .await
            .map_err(|e| {
                PipelineError::Generic(format!("Failed to start TcpStreamServer: {}", e))
            })?;

        tracing::debug!("tcp transport service on {local_ip}:{local_port}");

        Ok(Arc::new(Self {
            local_ip,
            local_port,
            state,
        }))
    }

    /// 将已注册的 subject 与后端实例关联。
    ///
    /// 若实例已被墓碑化则返回 `false`，此时 subject 会被立即取消，
    /// 调用方应跳过 `send_request` 并以可迁移的 `Disconnected` 错误失败。
    pub async fn associate_instance(&self, subject: &str, id: &PortNameInstanceId) -> bool {
        let mut state = self.state.lock().await;
        let now = Instant::now();
        prune_tombstones(&mut state.removed_instances, now);
        if state.removed_instances.contains_key(id) {
            // 实例已被移除 —— 立即取消。
            tracing::warn!(
                subject,
                namespace = %id.namespace,
                servicegroup = %id.servicegroup,
                portname = %id.portname,
                instance_id = id.instance_id,
                "Cancelling subject immediately: instance already removed (tombstoned)"
            );
            state.rx_subjects.remove(subject);
            return false;
        }
        state
            .subject_instance
            .insert(subject.to_string(), id.clone());
        state
            .instance_subjects
            .entry(id.clone())
            .or_default()
            .insert(subject.to_string());
        true
    }

    /// 取消一个待处理的响应流注册。丢弃 `oneshot::Sender`，
    /// 使等待中的接收者以 `RecvError` 解析。
    pub async fn cancel_recv_stream(&self, subject: &str) {
        let mut state = self.state.lock().await;
        state.rx_subjects.remove(subject);
        if let Some(key) = state.subject_instance.remove(subject)
            && let Some(subjects) = state.instance_subjects.get_mut(&key)
        {
            subjects.remove(subject);
            if subjects.is_empty() {
                state.instance_subjects.remove(&key);
            }
        }
    }

    /// 取消某实例的所有待处理响应流并将其墓碑化，
    /// 使任何针对同一 id 的竞争性 `associate_instance()` 也被取消。
    /// 返回被取消的流数量。
    pub async fn cancel_instance_streams(&self, id: &PortNameInstanceId) -> usize {
        let mut state = self.state.lock().await;
        let now = Instant::now();
        prune_tombstones(&mut state.removed_instances, now);
        state.removed_instances.insert(id.clone(), now);
        let subjects = match state.instance_subjects.remove(id) {
            Some(subjects) => subjects,
            None => return 0,
        };
        let count = subjects.len();
        for subject in &subjects {
            state.rx_subjects.remove(subject);
            state.subject_instance.remove(subject);
        }
        count
    }

    /// 为在发现中重新出现的实例丢弃墓碑，
    /// 使该身份未来的 subject 被正常跟踪。
    pub async fn clear_instance_tombstone(&self, id: &PortNameInstanceId) {
        let mut state = self.state.lock().await;
        state.removed_instances.remove(id);
    }

    #[allow(clippy::await_holding_lock)]
    async fn start(local_ip: String, local_port: u16, state: Arc<Mutex<State>>) -> Result<u16> {
        let addr = format!("{}:{}", local_ip, local_port);
        let state_clone = state.clone();
        let mut guard = state.lock().await;
        if guard.handle.is_some() {
            panic!("TcpStreamServer already started");
        }
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<Result<u16>>();
        let handle = tokio::spawn(tcp_listener(addr, state_clone, ready_tx));
        guard.handle = Some(handle);
        drop(guard);
        let local_port = ready_rx.await??;
        Ok(local_port)
    }
}

// todo - 可能将 ResponseService 重命名为 ResponseServer
#[async_trait::async_trait]
impl ResponseService for TcpStreamServer {
    /// 向响应订阅者注册一个新的 subject 与 sender。
    /// 生成一个 RAII 对象，在 drop 时注销该 subject。
    ///
    /// 需同时注册 data-in 与 data-out 两个条目：
    /// 可能有前向 pipeline 想消费 data-out 流，
    /// 也可能有响应流想消费 data-in 流。
    /// 注册时需指定需要 data-in、data-out 还是两者，
    /// 这将映射到运行的服务类型，即单/多输入与单/多输出。
    ///
    /// todo(ryan) - 返回一个可 await 的连接对象；连接成功后
    /// 可请求 sender 和 receiver。
    ///
    /// 或者
    ///
    /// 拆分为 register sender 和 register receiver，两者都返回连接对象，
    /// 连接建立后得到各自的 sender 或 receiver。
    ///
    /// 注册可能需要一次性完成，所以应使用 builder 对象来
    /// 请求 receiver 与可选 sender。
    async fn register(&self, options: StreamOptions) -> PendingConnections {
        // 用 oneshot channel 回传 sender 和 receiver 对象

        let address = format!("{}:{}", self.local_ip, self.local_port);
        tracing::debug!("Registering new TcpStream on {address}");

        let send_stream = if options.enable_request_stream {
            let sender_subject = uuid::Uuid::new_v4().to_string();

            let (pending_sender_tx, pending_sender_rx) = oneshot::channel();

            let connection_info = RequestedSendConnection {
                context: options.context.clone(),
                connection: pending_sender_tx,
            };

            let mut state = self.state.lock().await;
            state
                .tx_subjects
                .insert(sender_subject.clone(), connection_info);

            let cleanup_subject = sender_subject.clone();
            let cleanup_state = self.state.clone();
            let registered_stream = RegisteredStream::new(
                TcpStreamConnectionInfo {
                    address: address.clone(),
                    subject: sender_subject,
                    context: options.context.id().to_string(),
                    stream_type: StreamType::Request,
                }
                .into(),
                pending_sender_rx,
            )
            .with_cleanup(move || {
                // Drop 是同步的；发起后不管的锁获取。
                tokio::spawn(async move {
                    let mut state = cleanup_state.lock().await;
                    state.tx_subjects.remove(&cleanup_subject);
                });
            });

            Some(registered_stream)
        } else {
            None
        };

        let recv_stream = if options.enable_response_stream {
            let (pending_recver_tx, pending_recver_rx) = oneshot::channel();
            let receiver_subject = uuid::Uuid::new_v4().to_string();

            let connection_info = RequestedRecvConnection {
                context: options.context.clone(),
                connection: pending_recver_tx,
            };

            let mut state = self.state.lock().await;
            state
                .rx_subjects
                .insert(receiver_subject.clone(), connection_info);

            let cleanup_subject = receiver_subject.clone();
            let cleanup_state = self.state.clone();
            let registered_stream = RegisteredStream::new(
                TcpStreamConnectionInfo {
                    address: address.clone(),
                    subject: receiver_subject,
                    context: options.context.id().to_string(),
                    stream_type: StreamType::Response,
                }
                .into(),
                pending_recver_rx,
            )
            .with_cleanup(move || {
                // Drop 是同步的；发起后不管的锁获取。
                tokio::spawn(async move {
                    let mut state = cleanup_state.lock().await;
                    state.rx_subjects.remove(&cleanup_subject);
                    if let Some(key) = state.subject_instance.remove(&cleanup_subject)
                        && let Some(subjects) = state.instance_subjects.get_mut(&key)
                    {
                        subjects.remove(&cleanup_subject);
                        if subjects.is_empty() {
                            state.instance_subjects.remove(&key);
                        }
                    }
                });
            });

            Some(registered_stream)
        } else {
            None
        };

        PendingConnections {
            send_stream,
            recv_stream,
        }
    }
}

// 此方法在一个 tcp 端口上监听传入连接。
// 新连接预期会发送协议特定的握手，
// 供我们确定其关注的 subject；本例中
// 首条消息应为 [`FirstMessage`]，据此找到 sender，
// 然后生成一个任务将 tcp 流的所有字节转发给 sender。
// === SECTION: 接受循环 + 逐连接任务 + 控制消息解码器 ===
async fn tcp_listener(
    addr: String,
    state: Arc<Mutex<State>>,
    read_tx: tokio::sync::oneshot::Sender<Result<u16>>,
) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to start TcpListender on {}: {}", addr, e));

    let listener = match listener {
        Ok(listener) => {
            let addr = listener
                .local_addr()
                .map_err(|e| anyhow::anyhow!("Failed get SocketAddr: {:?}", e))
                .unwrap();

            read_tx
                .send(Ok(addr.port()))
                .expect("Failed to send ready signal");

            listener
        }
        Err(e) => {
            read_tx.send(Err(e)).expect("Failed to send ready signal");
            return Err(anyhow::anyhow!("Failed to start TcpListender on {}", addr));
        }
    };

    loop {
        // todo - 添加探针插点
        // todo - 添加所有已接受连接的计数器
        // todo - 添加所有在飞连接的仪表
        // todo - 添加传入字节的计数器
        // todo - 添加传出字节的计数器
        let (stream, _addr) = match listener.accept().await {
            Ok((stream, _addr)) => (stream, _addr),
            Err(e) => {
                // 客户端应重试，因此无需中止
                tracing::warn!("failed to accept tcp connection: {e}");
                eprintln!("failed to accept tcp connection: {}", e);
                continue;
            }
        };

        match stream.set_nodelay(true) {
            Ok(_) => (),
            Err(e) => {
                tracing::warn!("failed to set tcp stream to nodelay: {e}");
            }
        }

        match stream.set_linger(Some(std::time::Duration::from_secs(0))) {
            Ok(_) => (),
            Err(e) => {
                tracing::warn!("failed to set tcp stream to linger: {e}");
            }
        }

        tokio::spawn(handle_connection(stream, state.clone()));
    }

    // todo - 在 spawn 前克隆并跟踪 process_stream
    async fn handle_connection(stream: tokio::net::TcpStream, state: Arc<Mutex<State>>) {
        let result = process_stream(stream, state).await;
        match result {
            Ok(_) => tracing::trace!("successfully processed tcp connection"),
            Err(e) => {
                tracing::warn!("failed to handle tcp connection: {e}");
                #[cfg(debug_assertions)]
                eprintln!("failed to handle tcp connection: {}", e);
            }
        }
    }

    /// 此方法负责内部 tcp 流握手。
    /// 握手会将流特化为 request/sender 或 response/receiver 流。
    async fn process_stream(stream: tokio::net::TcpStream, state: Arc<Mutex<State>>) -> Result<()> {
        // 将 socket 拆分为 reader 和 writer
        let (read_half, write_half) = tokio::io::split(stream);

        // 将 codec 附加到 reader 和 writer 以获得帧化的读写器
        let mut framed_reader = FramedRead::new(read_half, TwoPartCodec::default());
        let framed_writer = FramedWrite::new(write_half, TwoPartCodec::default());

        // 内部 tcp [`CallHomeHandshake`] 将 socket 连接到请求方；
        // 这里我们以原始字节两部分消息的形式等待首条消息。
        let first_message = framed_reader
            .next()
            .await
            .ok_or(error!("Connection closed without a ControlMessage"))??;

        // 我们等待的原始字节应以仅包含 header 的消息形式到达
        // todo - 改进错误处理 - 检查无数据情形
        let handshake: CallHomeHandshake = match first_message.header() {
            Some(header) => serde_json::from_slice(header).map_err(|e| {
                error!(
                    "Failed to deserialize the first message as a valid `CallHomeHandshake`: {e}",
                )
            })?,
            None => {
                return Err(error!("Expected ControlMessage, got DataMessage"));
            }
        };

        // 在此分支以处理 sender 流或 receiver 流
        match handshake.stream_type {
            StreamType::Request => process_request_stream().await,
            StreamType::Response => {
                process_response_stream(handshake.subject, state, framed_reader, framed_writer)
                    .await
            }
        }
    }

    async fn process_request_stream() -> Result<()> {
        Ok(())
    }

    async fn process_response_stream(
        subject: String,
        state: Arc<Mutex<State>>,
        mut reader: FramedRead<tokio::io::ReadHalf<tokio::net::TcpStream>, TwoPartCodec>,
        writer: FramedWrite<tokio::io::WriteHalf<tokio::net::TcpStream>, TwoPartCodec>,
    ) -> Result<()> {
        let response_stream = {
            let mut guard = state.lock().await;
            let conn = guard
                .rx_subjects
                .remove(&subject)
                .ok_or(error!("Subject not found: {}; upstream publisher specified a subject unknown to the downsteam subscriber", subject))?;
            if let Some(key) = guard.subject_instance.remove(&subject)
                && let Some(subjects) = guard.instance_subjects.get_mut(&key)
            {
                subjects.remove(&subject);
                if subjects.is_empty() {
                    guard.instance_subjects.remove(&key);
                }
            }
            conn
        };

        // 解包 response_stream
        let RequestedRecvConnection {
            context,
            connection,
        } = response_stream;

        // [`Prologue`]
        // 必须有第二条控制消息，表明另一段的 generate 方法成功
        let prologue = reader
            .next()
            .await
            .ok_or(error!("Connection closed without a ControlMessge"))??;

        // 反序列化 prologue
        let prologue = match prologue.into_message_type() {
            TwoPartMessageType::HeaderOnly(header) => {
                let prologue: ResponseStreamPrologue = serde_json::from_slice(&header)
                    .map_err(|e| error!("Failed to deserialize ControlMessage: {}", e))?;
                prologue
            }
            _ => {
                // Worker 在 prologue 位置发送了非 HeaderOnly 帧
                // （协议违反、版本偏差、损坏）。通知请求方，
                // 使 generate 调用链干净失败，然后返回 Err，
                // 使连接任务结束而不 panic。
                let msg = "malformed prologue: expected HeaderOnly ControlMessage";
                let _ = connection.send(Err(msg.to_string()));
                return Err(error!(msg));
            }
        };

        // 等待 GTG 或 Error 控制消息；若为 error 则 connection.send(Err(String))，
        // 该调用应使 generate 调用链失败。
        //
        // 注：这条第二控制消息可能延迟，但建立连接的昂贵部分已
        // 完成且准备好数据流动；在此等待不会带来性能损失或问题，
        // 且允许我们跟踪初始建立时间与到达 prologue 的时间。
        if let Some(error) = &prologue.error {
            let _ = connection.send(Err(error.clone()));
            return Err(error!("Received error prologue: {}", error));
        }

        // 需要从注册选项中获知缓冲区大小；将其加到 RequestRecvConnection 对象中
        let (response_tx, response_rx) = mpsc::channel(64);

        if connection
            .send(Ok(crate::pipeline::network::StreamReceiver {
                rx: response_rx,
            }))
            .is_err()
        {
            return Err(error!(
                "The requester of the stream has been dropped before the connection was established"
            ));
        }

        let (control_tx, control_rx) = mpsc::channel::<ControlMessage>(1);

        // sender 任务
        // 向 sender 发出控制消息，完成后关闭 socket。
        // 这应是最后完成的任务，且必须如此。
        let send_task = tokio::spawn(network_send_handler(writer, control_rx));

        // 转发任务
        let recv_task = tokio::spawn(network_receive_handler(
            reader,
            response_tx,
            control_tx,
            context.clone(),
        ));

        // 检查每个任务的结果
        let (monitor_result, forward_result) = tokio::join!(send_task, recv_task);

        monitor_result?;
        forward_result?;

        Ok(())
    }

    async fn network_receive_handler(
        mut framed_reader: FramedRead<tokio::io::ReadHalf<tokio::net::TcpStream>, TwoPartCodec>,
        response_tx: mpsc::Sender<Bytes>,
        control_tx: mpsc::Sender<ControlMessage>,
        context: Arc<dyn AsyncEngineContext>,
    ) {
        // 循环读取 tcp 流并检查 writer 是否已关闭
        let mut can_stop = true;
        loop {
            tokio::select! {
                biased;

                _ = response_tx.closed() => {
                    tracing::trace!("response channel closed before the client finished writing data");
                    let _ = control_tx.send(ControlMessage::Kill).await;
                    break;
                }

                _ = context.killed() => {
                    tracing::trace!("context kill signal received; shutting down");
                    let _ = control_tx.send(ControlMessage::Kill).await;
                    break;
                }

                _ = context.stopped(), if can_stop => {
                    tracing::trace!("context stop signal received; shutting down");
                    can_stop = false;
                    let _ = control_tx.send(ControlMessage::Stop).await;
                }

                msg = framed_reader.next() => {
                    match msg {
                        Some(Ok(msg)) => {
                            let (header, data) = msg.into_parts();

                            // 收到控制消息
                            if !header.is_empty() {
                                match process_control_message(header) {
                                    Ok(ControlAction::Continue) => {}
                                    Ok(ControlAction::Shutdown) => {
                                        if !data.is_empty() {
                                            // 哨兵带数据属于协议违反；
                                            // 杀掉此流，不要 assert!() 拖垮进程。
                                            tracing::warn!(
                                                data_len = data.len(),
                                                "client sent Sentinel with data (protocol violation); killing stream"
                                            );
                                            let _ = control_tx.send(ControlMessage::Kill).await;
                                            break;
                                        }
                                        tracing::trace!("received sentinel message; shutting down");
                                        break;
                                    }
                                    Err(e) => {
                                        // 控制消息格式错误 —— 仅杀掉此流。
                                        tracing::warn!(err = ?e, "malformed control message, closing connection");
                                        let _ = control_tx.send(ControlMessage::Kill).await;
                                        break;
                                    }
                                }
                            }

                            if !data.is_empty()
                                && let Err(err) = response_tx.send(data).await {
                                    tracing::debug!(?err, "forwarding body/data to response channel failed");
                                    let _ = control_tx.send(ControlMessage::Kill).await;
                                    break;
                                };
                        }
                        Some(Err(e)) => {
                            // 来自 worker 的 TCP RST 或解码错误 —— 仅杀掉此流。
                            tracing::warn!(err = ?e, "tcp stream read error from worker, closing connection");
                            let _ = control_tx.send(ControlMessage::Kill).await;
                            break;
                        }
                        None => {
                            // 这是允许的但我们尽量避免：
                            // 逻辑上客户端会告知我们它已完成，服务器在收到
                            // 哨兵消息时自然关闭连接。客户端提前关闭表示
                            // 传输层库控制之外的传输错误。
                            tracing::trace!("tcp stream was closed by client");
                            break;
                        }
                    }
                }

            }
        }
    }

    async fn network_send_handler(
        socket_tx: FramedWrite<tokio::io::WriteHalf<tokio::net::TcpStream>, TwoPartCodec>,
        control_rx: mpsc::Receiver<ControlMessage>,
    ) {
        let mut socket_tx = socket_tx;
        let mut control_rx = control_rx;

        while let Some(control_msg) = control_rx.recv().await {
            // Sentinel 是 worker→frontend 消息；在此处收到说明
            // 生产者存在缺陷。跳过而不是断言 —— 流级别的
            // 缺陷不得使 worker panic。
            if matches!(control_msg, ControlMessage::Sentinel) {
                tracing::warn!("received sentinel on send-side control channel; dropping");
                continue;
            }
            let bytes = match serde_json::to_vec(&control_msg) {
                Ok(b) => b,
                Err(e) => {
                    // 小型变体的封闭枚举；序列化不应失败。
                    // 如果真发生，记录并跳过而非 panic。
                    tracing::warn!(err = ?e, ?control_msg, "failed to serialize control message");
                    continue;
                }
            };
            let message = TwoPartMessage::from_header(bytes.into());
            match socket_tx.send(message).await {
                Ok(_) => tracing::debug!(?control_msg, "issued control message"),
                Err(e) => {
                    tracing::debug!(err = ?e, ?control_msg, "failed to send control message")
                }
            }
        }

        let mut inner = socket_tx.into_inner();
        if let Err(e) = inner.flush().await {
            tracing::debug!("failed to flush socket: {e}");
        }
        if let Err(e) = inner.shutdown().await {
            tracing::debug!("failed to shutdown socket: {e}");
        }
    }
}

enum ControlAction {
    Continue,
    Shutdown,
}

fn process_control_message(message: Bytes) -> Result<ControlAction> {
    match serde_json::from_slice::<ControlMessage>(&message)? {
        ControlMessage::Sentinel => {
            // 客户端发出了哨兵消息：
            // 它已完成写数据，正等待服务器关闭连接。
            tracing::trace!("sentinel received; shutting down");
            Ok(ControlAction::Shutdown)
        }
        ControlMessage::Kill | ControlMessage::Stop => {
            // Worker→frontend 控制方向仅携带 Sentinel。此处的 Kill/Stop
            // 是协议违反；调用方将该 Err 转为流局部的 Kill
            // 而非进程致命事件。
            anyhow::bail!("unexpected control message on response stream");
        }
    }
}

// === SECTION: 测试 —— server 绑定、注册、墓碑、kill 路径 ===
#[cfg(test)]
mod tests {
    //! ## 测试矩阵
    //!
    //! | 测试名 | 覆盖维度 |
    //! |---|---|
    //! | `test_tcp_stream_server_default_behavior` | server: 默认 IP 解析路径并能启动 |
    //! | `test_tcp_stream_server_fallback_to_loopback` | server: IP 解析失败时回退 loopback |
    //! | `test_server` | 辅助构造函数：以标准选项启动 server |
    //! | `test_cancel_instance_streams_unblocks_receiver` | cancel: 取消 instance 后接收端必须解阻 |
    //! | `test_cancel_instance_streams_multiple_subjects` | cancel: 批量取消同一 instance 下多 subject |
    //! | `test_cancel_instance_streams_nonexistent_instance` | cancel: 不存在的 instance 不报错 |
    //! | `test_cancel_recv_stream_cleans_up_instance_tracking` | cancel: recv 流取消需同步清理 instance 跟踪 |
    //! | `test_registered_stream_drop_runs_cleanup` | RegisteredStream: drop 触发 cleanup |
    //! | `test_registered_stream_into_parts_disarms_cleanup` | RegisteredStream: into_parts 解除 cleanup |
    //! | `test_associate_after_cancel_is_immediately_cancelled` | tombstone: cancel 后 associate 立即被 cancel |
    //! | `test_clear_tombstone_allows_new_associations` | tombstone: 清除后允许新 association |
    //! | `test_cancel_does_not_affect_sibling_portname` | 隔离: 取消不影响同组下别的 portname |
    //! | `test_tombstone_is_portname_scoped` | tombstone: 以 portname 身份作为作用域 |
    //! | `test_cancel_does_not_affect_different_servicegroup` | 隔离: 跨 servicegroup 不受影响 |
    //! | `test_tombstone_expires_after_ttl` | tombstone: TTL 过期后失效 |
    //! | `test_tombstone_within_ttl_blocks_associate` | tombstone: TTL 内继续拦截 associate |
    //! | `test_tombstone_lazy_prune_on_cancel` | tombstone: cancel 路径上懒惰剖除 |
    //! | `test_clear_tombstone_only_affects_named_identity` | tombstone: clear 只影响指定身份 |
    //! | `test_tombstone_scoped_to_full_identity` | tombstone: 以完整 4-tuple 身份为键 |
    //! | `test_tcp_stream_server_sends_kill_on_unexpected_control_message` | kill: 遇不期望控制消息发 Kill |
    //! | `test_tcp_stream_server_sends_kill_on_read_error` | kill: 读错误发 Kill |
    //! | `test_tcp_stream_server_sends_kill_on_sentinel_with_data` | kill: Sentinel 伴随 data 视为违规 |
    //! | `test_tcp_stream_server_returns_error_on_invalid_prologue` | prologue: 无效序言返回错误 |
    use super::*;
    use crate::engine::AsyncEngineContextProvider;
    use crate::pipeline::Context;
    use tokio::io::{AsyncWriteExt, ReadHalf, WriteHalf};
    use tokio::net::TcpStream;

    // 总是失败的模拟解析器，用于模拟回退场景
    struct FailingIpResolver;

    impl IpResolver for FailingIpResolver {
        fn local_ip(&self) -> Result<std::net::IpAddr, Error> {
            Err(Error::LocalIpAddressNotFound)
        }

        fn local_ipv6(&self) -> Result<std::net::IpAddr, Error> {
            Err(Error::LocalIpAddressNotFound)
        }
    }

    #[tokio::test]
    async fn test_tcp_stream_server_default_behavior() {
        // 测试 TcpStreamServer::new 在默认选项下可用
        // 验证 IP 检测成功时的正常运作
        let options = ServerOptions::default();
        let result = TcpStreamServer::new(options).await;

        assert!(
            result.is_ok(),
            "TcpStreamServer::new should succeed with default options"
        );

        let server = result.unwrap();

        // 通过注册一个流验证服务器可用
        let context = Context::new(());
        let stream_options = StreamOptions::builder()
            .context(context.context())
            .enable_request_stream(false)
            .enable_response_stream(true)
            .build()
            .unwrap();

        let pending_connection = server.register(stream_options).await;

        // 验证连接信息可用且有效
        let connection_info = pending_connection
            .recv_stream
            .as_ref()
            .unwrap()
            .connection_info
            .clone();

        let tcp_info: TcpStreamConnectionInfo = connection_info.try_into().unwrap();
        let socket_addr = tcp_info.address.parse::<std::net::SocketAddr>().unwrap();

        // 应分配到一个有效端口
        assert!(
            socket_addr.port() > 0,
            "Server should be assigned a valid port number"
        );

        println!(
            "Server created successfully with address: {}",
            tcp_info.address
        );
    }

    #[tokio::test]
    async fn test_tcp_stream_server_fallback_to_loopback() {
        // 使用总是失败的模拟解析器测试回退行为
        // 这保证回退逻辑被触发

        let options = ServerOptions::builder().port(0).build().unwrap();

        // 使用失败解析器迫使回退
        let result = TcpStreamServer::new_with_resolver(options, FailingIpResolver).await;
        assert!(
            result.is_ok(),
            "Server creation should succeed with fallback even when IP detection fails"
        );

        let server = result.unwrap();

        // 通过注册一个流获取实际绑定地址
        let context = Context::new(());
        let stream_options = StreamOptions::builder()
            .context(context.context())
            .enable_request_stream(false)
            .enable_response_stream(true)
            .build()
            .unwrap();

        let pending_connection = server.register(stream_options).await;
        let connection_info = pending_connection
            .recv_stream
            .as_ref()
            .unwrap()
            .connection_info
            .clone();

        let tcp_info: TcpStreamConnectionInfo = connection_info.try_into().unwrap();
        let socket_addr = tcp_info.address.parse::<std::net::SocketAddr>().unwrap();

        // 使用失败解析器时，应始终使用回退
        let ip = socket_addr.ip();
        assert!(
            ip.is_loopback(),
            "Should use loopback when IP detection fails"
        );

        // 验证其具体为 127.0.0.1（补丁中的回退值）
        assert_eq!(
            ip,
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
            "Fallback should use exactly 127.0.0.1, got: {}",
            ip
        );

        println!("SUCCESS: Fallback to 127.0.0.1 was confirmed: {}", ip);

        // 服务器应能使用回退 IP 正常工作
        assert!(socket_addr.port() > 0, "Server should have a valid port");
    }

    /// 使用失败 IP 解析器创建测试服务器（回退到 loopback）。
    async fn test_server() -> Arc<TcpStreamServer> {
        TcpStreamServer::new_with_resolver(
            ServerOptions::builder().port(0).build().unwrap(),
            FailingIpResolver,
        )
        .await
        .unwrap()
    }

    /// 辅助函数：注册一个响应流并提取其 subject 字符串。
    async fn register_and_get_subject(
        server: &TcpStreamServer,
    ) -> (
        String,
        tokio::sync::oneshot::Receiver<Result<super::StreamReceiver, String>>,
    ) {
        let context = Context::new(());
        let options = StreamOptions::builder()
            .context(context.context())
            .enable_request_stream(false)
            .enable_response_stream(true)
            .build()
            .unwrap();

        let pending = server.register(options).await;
        let recv_stream = pending.recv_stream.unwrap();
        let (conn_info, provider) = recv_stream.into_parts();
        let tcp_info: TcpStreamConnectionInfo = conn_info.try_into().unwrap();
        (tcp_info.subject, provider)
    }

    /// 便捷构造函数，避免测试重复结构体字面量。
    fn make_eid(
        namespace: &str,
        servicegroup: &str,
        portname: &str,
        instance_id: u64,
    ) -> PortNameInstanceId {
        PortNameInstanceId {
            namespace: namespace.to_string(),
            servicegroup: servicegroup.to_string(),
            portname: portname.to_string(),
            instance_id,
        }
    }

    #[tokio::test]
    async fn test_cancel_instance_streams_unblocks_receiver() {
        let server = test_server().await;

        let (subject, provider) = register_and_get_subject(&server).await;

        let id = make_eid("ns", "sg", "generate", 42);
        assert!(server.associate_instance(&subject, &id).await);

        let cancelled = server.cancel_instance_streams(&id).await;
        assert_eq!(cancelled, 1);

        // oneshot 接收端现在应以错误解析（sender 已 drop）
        let result = provider.await;
        assert!(result.is_err(), "Expected RecvError after cancellation");
    }

    #[tokio::test]
    async fn test_cancel_instance_streams_multiple_subjects() {
        let server = test_server().await;

        let (subj1, prov1) = register_and_get_subject(&server).await;
        let (subj2, prov2) = register_and_get_subject(&server).await;
        let (subj3, prov3) = register_and_get_subject(&server).await;

        let id10 = make_eid("ns", "sg", "generate", 10);
        let id20 = make_eid("ns", "sg", "generate", 20);

        // 前两个关联到实例 10，第三个关联到实例 20
        assert!(server.associate_instance(&subj1, &id10).await);
        assert!(server.associate_instance(&subj2, &id10).await);
        assert!(server.associate_instance(&subj3, &id20).await);

        // 取消实例 10 —— 应取消 2 个 subject
        let cancelled = server.cancel_instance_streams(&id10).await;
        assert_eq!(cancelled, 2);

        assert!(prov1.await.is_err());
        assert!(prov2.await.is_err());

        // 实例 20 应不受影响 —— 单独取消
        let cancelled = server.cancel_instance_streams(&id20).await;
        assert_eq!(cancelled, 1);
        assert!(prov3.await.is_err());
    }

    #[tokio::test]
    async fn test_cancel_instance_streams_nonexistent_instance() {
        let server = test_server().await;

        let id = make_eid("ns", "sg", "generate", 999);
        let cancelled = server.cancel_instance_streams(&id).await;
        assert_eq!(cancelled, 0);
    }

    #[tokio::test]
    async fn test_cancel_recv_stream_cleans_up_instance_tracking() {
        let server = test_server().await;

        let (subject, _provider) = register_and_get_subject(&server).await;
        let id = make_eid("ns", "sg", "generate", 42);
        assert!(server.associate_instance(&subject, &id).await);

        // 取消单个 subject
        server.cancel_recv_stream(&subject).await;

        // 实例应无剩余 subject
        let cancelled = server.cancel_instance_streams(&id).await;
        assert_eq!(
            cancelled, 0,
            "Instance tracking should have been cleaned up"
        );
    }

    #[tokio::test]
    async fn test_registered_stream_drop_runs_cleanup() {
        let server = test_server().await;

        // 注册一个响应流但不调用 into_parts —— 直接 drop
        let context = Context::new(());
        let options = StreamOptions::builder()
            .context(context.context())
            .enable_request_stream(false)
            .enable_response_stream(true)
            .build()
            .unwrap();

        let pending = server.register(options).await;
        let recv_stream = pending.recv_stream.unwrap();

        // drop 前获取 subject
        let tcp_info: TcpStreamConnectionInfo =
            recv_stream.connection_info.clone().try_into().unwrap();
        let subject = tcp_info.subject.clone();

        // 验证其在 rx_subjects 中
        {
            let state = server.state.lock().await;
            assert!(state.rx_subjects.contains_key(&subject));
        }

        // drop RegisteredStream —— RAII cleanup 应触发
        drop(recv_stream);

        // 给生成的 cleanup 任务片刻运行
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // 验证其已从 rx_subjects 移除
        {
            let state = server.state.lock().await;
            assert!(
                !state.rx_subjects.contains_key(&subject),
                "RAII cleanup should have removed the rx_subjects entry"
            );
        }
    }

    #[tokio::test]
    async fn test_registered_stream_into_parts_disarms_cleanup() {
        let server = test_server().await;

        let context = Context::new(());
        let options = StreamOptions::builder()
            .context(context.context())
            .enable_request_stream(false)
            .enable_response_stream(true)
            .build()
            .unwrap();

        let pending = server.register(options).await;
        let recv_stream = pending.recv_stream.unwrap();

        let tcp_info: TcpStreamConnectionInfo =
            recv_stream.connection_info.clone().try_into().unwrap();
        let subject = tcp_info.subject.clone();

        // 调用 into_parts 以解除 cleanup
        let (_conn_info, _provider) = recv_stream.into_parts();

        // 给任何潜在的 cleanup 片刻运行
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // 该条目应仍在 rx_subjects 中（cleanup 已被解除）
        {
            let state = server.state.lock().await;
            assert!(
                state.rx_subjects.contains_key(&subject),
                "into_parts() should disarm the RAII cleanup"
            );
        }
    }

    #[tokio::test]
    async fn test_associate_after_cancel_is_immediately_cancelled() {
        // 模拟竞争：cancel_instance_streams 在 associate_instance 之前触发。
        let server = test_server().await;

        let id = make_eid("ns", "sg", "generate", 42);

        // 在任何 subject 注册前取消（墓碑）。
        let cancelled = server.cancel_instance_streams(&id).await;
        assert_eq!(cancelled, 0);

        // 现在注册一个 subject 并尝试将其关联到已墓碑化的实例。
        let (subject, provider) = register_and_get_subject(&server).await;
        let associated = server.associate_instance(&subject, &id).await;

        // 实例已墓碑化时 associate_instance 应返回 false。
        assert!(
            !associated,
            "associate_instance on a tombstoned instance should return false"
        );

        // provider 应以错误解析，因为 associate_instance 发现墓碑
        // 并立即取消了 subject。
        let result = provider.await;
        assert!(
            result.is_err(),
            "Late associate_instance on a tombstoned instance should immediately cancel"
        );
    }

    #[tokio::test]
    async fn test_clear_tombstone_allows_new_associations() {
        let server = test_server().await;

        let id = make_eid("ns", "sg", "generate", 42);

        server.cancel_instance_streams(&id).await;
        server.clear_instance_tombstone(&id).await;

        // 现在 associate 应正常工作（subject 未被取消）。
        let (subject, _provider) = register_and_get_subject(&server).await;
        assert!(server.associate_instance(&subject, &id).await);

        // subject 应被跟踪，未被取消。
        let cancelled = server.cancel_instance_streams(&id).await;
        assert_eq!(
            cancelled, 1,
            "After clearing tombstone, subjects should be tracked normally"
        );
    }

    #[tokio::test]
    async fn test_cancel_does_not_affect_sibling_portname() {
        // 回归：取消 "generate" 不得取消共享同一 instance_id
        // （同一后端运行时）的 "prefill" subject。
        let server = test_server().await;

        let (gen_subj, gen_prov) = register_and_get_subject(&server).await;
        let (pre_subj, pre_prov) = register_and_get_subject(&server).await;

        let gen_id = make_eid("ns", "sg", "generate", 42);
        let pre_id = make_eid("ns", "sg", "prefill", 42);

        assert!(server.associate_instance(&gen_subj, &gen_id).await);
        assert!(server.associate_instance(&pre_subj, &pre_id).await);

        // 仅取消 "generate" portname 的 subject。
        let cancelled = server.cancel_instance_streams(&gen_id).await;
        assert_eq!(
            cancelled, 1,
            "Only the generate subject should be cancelled"
        );
        assert!(gen_prov.await.is_err());

        // prefill 必须仍被跟踪。
        let still_pending = server.cancel_instance_streams(&pre_id).await;
        assert_eq!(still_pending, 1, "prefill subject should still be tracked");
        assert!(pre_prov.await.is_err());
    }

    #[tokio::test]
    async fn test_tombstone_is_portname_scoped() {
        // 墓碑化 "generate" 不得阻止同一 instance_id 下 "prefill"
        // 的新关联。
        let server = test_server().await;

        let gen_id = make_eid("ns", "sg", "generate", 42);
        let pre_id = make_eid("ns", "sg", "prefill", 42);

        server.cancel_instance_streams(&gen_id).await;

        // "generate" 的新 subject 应被拒绝。
        let (gen_subj, gen_prov) = register_and_get_subject(&server).await;
        assert!(
            !server.associate_instance(&gen_subj, &gen_id).await,
            "generate should be tombstoned"
        );
        assert!(gen_prov.await.is_err());

        // 相同 instance_id 下 "prefill" 的新 subject 应被接受。
        let (pre_subj, _pre_prov) = register_and_get_subject(&server).await;
        assert!(
            server.associate_instance(&pre_subj, &pre_id).await,
            "prefill tombstone is independent; subject should be tracked"
        );
        let count = server.cancel_instance_streams(&pre_id).await;
        assert_eq!(count, 1, "prefill subject should be tracked normally");
    }

    #[tokio::test]
    async fn test_cancel_does_not_affect_different_servicegroup() {
        // 回归：两个 (namespace, servicegroup) 不同但 portname 名称相同、
        // 且同一 pod 支撑 instance_id 的服务不得相互干扰，
        // 即使它们共享同一个 TcpStreamServer 运行时。
        let server = test_server().await;

        let (subj_a, prov_a) = register_and_get_subject(&server).await;
        let (subj_b, prov_b) = register_and_get_subject(&server).await;

        // portname 名称 + instance_id 相同，namespace/servicegroup 不同。
        let id_a = make_eid("ns-a", "sg-a", "generate", 42);
        let id_b = make_eid("ns-b", "sg-b", "generate", 42);

        assert!(server.associate_instance(&subj_a, &id_a).await);
        assert!(server.associate_instance(&subj_b, &id_b).await);

        // 取消服务 A —— 仅 subj_a 应受影响。
        let cancelled = server.cancel_instance_streams(&id_a).await;
        assert_eq!(cancelled, 1, "Only service-A subject should be cancelled");
        assert!(prov_a.await.is_err());

        // 服务 B 的 subject 必须仍处于待处理。
        let still_tracked = server.cancel_instance_streams(&id_b).await;
        assert_eq!(still_tracked, 1, "Service-B subject should be unaffected");
        assert!(prov_b.await.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn test_tombstone_expires_after_ttl() {
        // TOMBSTONE_TTL 过去后，之前已墓碑化的身份必须再次接受
        // 新关联，且该条目必须从 `removed_instances` 中物理剔除，
        // 使集合保持有界。
        let server = test_server().await;

        let id = make_eid("ns", "sg", "generate", 42);

        // 墓碑化该身份。
        server.cancel_instance_streams(&id).await;
        {
            let state = server.state.lock().await;
            assert!(state.removed_instances.contains_key(&id));
        }

        // 推进超过 TTL。
        tokio::time::advance(TOMBSTONE_TTL + Duration::from_secs(1)).await;

        // 同一身份的 associate_instance 现在应成功（不再被墓碑化）。
        // 任何新 subject 必须被正常跟踪。
        let (subject, _provider) = register_and_get_subject(&server).await;
        assert!(
            server.associate_instance(&subject, &id).await,
            "tombstone older than TTL should not block association"
        );

        // 过期墓碑必须已被剔除（每次 associate_instance/
        // cancel_instance_streams 调用都会触发懒惰剔除）。
        {
            let state = server.state.lock().await;
            assert!(
                !state.removed_instances.contains_key(&id),
                "expired tombstone should be pruned, not retained"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_tombstone_within_ttl_blocks_associate() {
        // 原墓碑修复的回归保障：年龄小于 TTL 的墓碑
        // 必须仍取消迟到的 associate_instance() 调用。
        let server = test_server().await;

        let id = make_eid("ns", "sg", "generate", 42);
        server.cancel_instance_streams(&id).await;

        // 仅推进 TTL 的一小部分。
        tokio::time::advance(Duration::from_secs(1)).await;

        let (subject, provider) = register_and_get_subject(&server).await;
        assert!(
            !server.associate_instance(&subject, &id).await,
            "tombstone within TTL must still block association"
        );
        assert!(provider.await.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn test_tombstone_lazy_prune_on_cancel() {
        // 旧墓碑必须在下一次 cancel_instance_streams 调用时被剔除，
        // 无论正在墓碑化哪个身份。
        let server = test_server().await;

        let id_old = make_eid("ns", "sg", "generate", 1);
        let id_new = make_eid("ns", "sg", "generate", 2);

        server.cancel_instance_streams(&id_old).await;
        tokio::time::advance(TOMBSTONE_TTL + Duration::from_secs(1)).await;
        server.cancel_instance_streams(&id_new).await;

        let state = server.state.lock().await;
        assert!(
            !state.removed_instances.contains_key(&id_old),
            "old tombstone should be pruned by the next cancel_instance_streams call"
        );
        assert!(
            state.removed_instances.contains_key(&id_new),
            "fresh tombstone should be retained"
        );
        assert_eq!(state.removed_instances.len(), 1);
    }

    #[tokio::test]
    async fn test_clear_tombstone_only_affects_named_identity() {
        // 记录单调租约不变量：对某个 PortNameInstanceId 的
        // `clear_instance_tombstone` 不得触及同级条目。使用 etcd
        // 租约 ID 时这段防御性代码很少触发（新租约 = 新
        // PortNameInstanceId），但按键作用域必须成立。
        let server = test_server().await;

        let id_a = make_eid("ns", "sg", "generate", 1);
        let id_b = make_eid("ns", "sg", "generate", 2);

        server.cancel_instance_streams(&id_a).await;
        server.clear_instance_tombstone(&id_b).await;

        let state = server.state.lock().await;
        assert!(
            state.removed_instances.contains_key(&id_a),
            "clearing a different identity must not remove id_a's tombstone"
        );
    }

    #[tokio::test]
    async fn test_tombstone_scoped_to_full_identity() {
        // 对 (ns-a, sg-a, generate, 42) 的墓碑不得阻止
        // (ns-b, sg-b, generate, 42) 上的关联。
        let server = test_server().await;

        let id_a = make_eid("ns-a", "sg-a", "generate", 42);
        let id_b = make_eid("ns-b", "sg-b", "generate", 42);

        // 仅墓碑化服务 A。
        server.cancel_instance_streams(&id_a).await;

        // 服务 A 已墓碑化 —— 新关联被拒绝。
        let (subj_a, prov_a) = register_and_get_subject(&server).await;
        assert!(!server.associate_instance(&subj_a, &id_a).await);
        assert!(prov_a.await.is_err());

        // 服务 B 使用相同 portname 名称 + instance_id 必须被接受。
        let (subj_b, _prov_b) = register_and_get_subject(&server).await;
        assert!(
            server.associate_instance(&subj_b, &id_b).await,
            "Different namespace/servicegroup must not be tombstoned"
        );
        assert_eq!(server.cancel_instance_streams(&id_b).await, 1);
    }

    type TestFramedRead = FramedRead<ReadHalf<TcpStream>, TwoPartCodec>;
    type TestFramedWrite = FramedWrite<WriteHalf<TcpStream>, TwoPartCodec>;
    type TestResponseStream = (TestFramedRead, TestFramedWrite, StreamReceiver);

    /// 启动一个 TcpStreamServer，注册一个响应流，连接一个
    /// 客户端，驱动握手 + 序幕，并返回客户端的帧化
    /// 读/写器以及接收器。
    async fn open_registered_response_stream() -> TestResponseStream {
        let options = ServerOptions::builder().port(0).build().unwrap();
        let server = TcpStreamServer::new_with_resolver(options, FailingIpResolver)
            .await
            .unwrap();
        let context = Context::new(());
        let stream_options = StreamOptions::builder()
            .context(context.context())
            .enable_request_stream(false)
            .enable_response_stream(true)
            .build()
            .unwrap();
        let pending_connection = server.register(stream_options).await;
        let registered_stream = pending_connection.recv_stream.unwrap();
        let (connection_info, stream_provider) = registered_stream.into_parts();
        let tcp_info: TcpStreamConnectionInfo = connection_info.try_into().unwrap();

        let stream = TcpStream::connect(&tcp_info.address).await.unwrap();
        let (read_half, write_half) = tokio::io::split(stream);
        let framed_reader = FramedRead::new(read_half, TwoPartCodec::default());
        let mut framed_writer = FramedWrite::new(write_half, TwoPartCodec::default());

        let handshake = CallHomeHandshake {
            subject: tcp_info.subject,
            stream_type: StreamType::Response,
        };
        framed_writer
            .send(TwoPartMessage::from_header(
                serde_json::to_vec(&handshake).unwrap().into(),
            ))
            .await
            .unwrap();
        framed_writer
            .send(TwoPartMessage::from_header(
                serde_json::to_vec(&ResponseStreamPrologue { error: None })
                    .unwrap()
                    .into(),
            ))
            .await
            .unwrap();

        // SAFETY（仅测试）：健康的 localhost 握手总能解析出全部
        // 三层；此处 panic 意味着测试脚手架已损坏。
        let receiver = tokio::time::timeout(std::time::Duration::from_secs(1), stream_provider)
            .await
            .expect("server should establish response stream within timeout")
            .expect("stream provider should not be dropped")
            .expect("response stream should be accepted");

        (framed_reader, framed_writer, receiver)
    }

    async fn recv_control_message(framed_reader: &mut TestFramedRead) -> ControlMessage {
        // SAFETY（仅测试）：这些层中任一层出现行为异常的服务器
        // 正是我们希望以测试 panic 形式暴露的测试脚手架故障。
        let message = tokio::time::timeout(std::time::Duration::from_secs(1), framed_reader.next())
            .await
            .expect("server should send a control message within timeout")
            .expect("server should not close before sending control")
            .expect("control message should decode");
        let (header, data) = message.optional_parts();
        assert!(data.is_none(), "control message should not contain data");
        serde_json::from_slice(header.expect("control header missing").as_ref()).unwrap()
    }

    /// 发送意外的控制消息（来自数据方向的 Stop 或 Kill）
    /// 是协议违规。服务器的 network_receive_handler 必须仅在
    /// 该流上以 ControlMessage::Kill 回复，而不是 panic。
    #[tokio::test]
    async fn test_tcp_stream_server_sends_kill_on_unexpected_control_message() {
        let (mut framed_reader, mut framed_writer, _receiver) =
            open_registered_response_stream().await;

        framed_writer
            .send(TwoPartMessage::from_header(
                serde_json::to_vec(&ControlMessage::Stop).unwrap().into(),
            ))
            .await
            .unwrap();

        assert_eq!(
            recv_control_message(&mut framed_reader).await,
            ControlMessage::Kill,
            "unexpected control message should kill only this stream"
        );
    }

    /// 来自 worker 侧的帧化/解码错误对该流不可恢复，
    /// 但不得使 worker panic。服务器应发送 Kill 并仅
    /// 拆除该连接。
    #[tokio::test]
    async fn test_tcp_stream_server_sends_kill_on_read_error() {
        let (mut framed_reader, framed_writer, _receiver) = open_registered_response_stream().await;

        let mut raw_writer = framed_writer.into_inner();
        raw_writer.write_all(&[0u8; 8]).await.unwrap();
        raw_writer.shutdown().await.unwrap();

        assert_eq!(
            recv_control_message(&mut framed_reader).await,
            ControlMessage::Kill,
            "framing read error should kill only this stream"
        );
    }

    /// Sentinel 应为仅含报头。附加数据负载的行为异常客户端
    /// 不得通过 assert!() 使 worker panic。
    #[tokio::test]
    async fn test_tcp_stream_server_sends_kill_on_sentinel_with_data() {
        let (mut framed_reader, mut framed_writer, _receiver) =
            open_registered_response_stream().await;

        let header = serde_json::to_vec(&ControlMessage::Sentinel)
            .unwrap()
            .into();
        framed_writer
            .send(TwoPartMessage::from_parts(
                header,
                Bytes::from_static(b"unexpected payload"),
            ))
            .await
            .unwrap();

        assert_eq!(
            recv_control_message(&mut framed_reader).await,
            ControlMessage::Kill,
            "Sentinel with data should kill only this stream"
        );
    }

    /// 序幕必须是 HeaderOnly 帧。非 HeaderOnly 的序幕
    /// （仅数据或混合）必须以 Err 向请求方暴露，
    /// 而不是使 worker panic。
    #[tokio::test]
    async fn test_tcp_stream_server_returns_error_on_invalid_prologue() {
        let options = ServerOptions::builder().port(0).build().unwrap();
        let server = TcpStreamServer::new_with_resolver(options, FailingIpResolver)
            .await
            .unwrap();
        let context = Context::new(());
        let stream_options = StreamOptions::builder()
            .context(context.context())
            .enable_request_stream(false)
            .enable_response_stream(true)
            .build()
            .unwrap();
        let pending_connection = server.register(stream_options).await;
        let registered_stream = pending_connection.recv_stream.unwrap();
        let (connection_info, stream_provider) = registered_stream.into_parts();
        let tcp_info: TcpStreamConnectionInfo = connection_info.try_into().unwrap();

        let stream = TcpStream::connect(&tcp_info.address).await.unwrap();
        let (_read_half, write_half) = tokio::io::split(stream);
        let mut framed_writer = FramedWrite::new(write_half, TwoPartCodec::default());

        let handshake = CallHomeHandshake {
            subject: tcp_info.subject,
            stream_type: StreamType::Response,
        };
        framed_writer
            .send(TwoPartMessage::from_header(
                serde_json::to_vec(&handshake).unwrap().into(),
            ))
            .await
            .unwrap();

        // 在序幕位置发送一个仅数据帧。
        framed_writer
            .send(TwoPartMessage::from_data(Bytes::from_static(
                b"not a prologue",
            )))
            .await
            .unwrap();

        let outcome = tokio::time::timeout(std::time::Duration::from_secs(1), stream_provider)
            .await
            .expect("stream provider should resolve quickly")
            .expect("stream provider channel should not be dropped");
        // StreamReceiver 未实现 Debug，因此无法使用 `.expect_err`。
        match outcome {
            Err(err) => assert!(
                err.contains("malformed prologue"),
                "expected malformed-prologue error, got: {err}"
            ),
            Ok(_) => panic!("invalid prologue should produce an error, but got Ok"),
        }
    }
}
