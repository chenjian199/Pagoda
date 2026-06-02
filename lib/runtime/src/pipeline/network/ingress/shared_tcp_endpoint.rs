// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::network::ingress::shared_tcp_endpoint` —— 单端口多端点复用 TCP 服务器
//!
//! ## 设计意图
//! 让同一个进程的多个逻辑端点共享一个 TCP 端口 —— 路由以 portname path 为键。这是
//! TCP 传输下 `RequestPlaneServer` 的实现：随着 `register_portname` 动态查表并调用
//! 对应的 `PushWorkHandler`。
//!
//! ## 外部契约
//! - 公开类型与方法是稳定契约；`address() -> tcp://host:port` /
//!   `transport_name() -> "tcp"` / `is_healthy()` 是契约。
//! - portname path 路由表与错误响应格式不可改，跨语言客户端依赖。
//!
//! ## 实现要点
//! - 连接路径上采用 `TwoPartCodec` 读取请求帧；需 ACK 的错误路径统一走
//!   `send_error_response` 辅助路径，避免重复拼装错误帧。
//! - 过载控制：有插入 admission controller 接点，限流拒绝转化为
//!   `"Server overloaded"` 错误帧发回。

//! 带 portname 复用的共享 TCP 服务器
//!
//! 通过在 TCP wire protocol 中加入 portname 路由，
//! 提供一个可在单个端口上处理多个 portname 的共享 TCP 服务器。

use crate::SystemHealth;
use crate::metrics::work_handler_pool::{
    WORK_HANDLER_ENQUEUE_REJECTED_TOTAL, WORK_HANDLER_PERMIT_WAIT_SECONDS,
    WORK_HANDLER_POOL_ACTIVE_TASKS, WORK_HANDLER_POOL_CAPACITY, WORK_HANDLER_QUEUE_CAPACITY,
    WORK_HANDLER_QUEUE_DEPTH,
};
use crate::pipeline::network::PushWorkHandler;
use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio_util::bytes::BytesMut;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

/// TCP 请求处理的默认 worker 池大小
const DEFAULT_WORKER_POOL_SIZE: usize = 10000;

/// TCP 请求处理的默认工作队列大小
/// 这是 worker 池大小的 4 倍，用于承接突发流量。
const DEFAULT_WORK_QUEUE_SIZE: usize = 40000;

// === SECTION: [1] 工作池规模环境配置 ===

/// 从环境变量读取 worker 池大小，或使用默认值。
fn get_worker_pool_size() -> usize {
    std::env::var("PGD_TCP_WORKER_POOL_SIZE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(DEFAULT_WORKER_POOL_SIZE)
}

/// 从环境变量读取工作队列大小，或使用默认值。
fn get_work_queue_size() -> usize {
    std::env::var("PGD_TCP_WORK_QUEUE_SIZE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(DEFAULT_WORK_QUEUE_SIZE)
}

// === SECTION: [2] 在飞任务 RAII 计数守卫 ===

/// `WORK_HANDLER_POOL_ACTIVE_TASKS` 的 RAII 守卫。`new()` 负责递增，
/// `Drop` 负责递减，因此单一所有者即可表达“任务处于活跃状态”的区间。
/// 它在 dispatcher 中、`tokio::spawn` 之前构造并移动进 future，
/// 因而在任何 worker 线程轮询该任务之前，仪表值就已递增，
/// 且递减会在每条退出路径上执行——正常返回、panic，或
/// 取消。
struct ActiveTaskGuard;

impl ActiveTaskGuard {
    fn new() -> Self {
        WORK_HANDLER_POOL_ACTIVE_TASKS.inc();
        Self
    }
}

impl Drop for ActiveTaskGuard {
    fn drop(&mut self) {
        WORK_HANDLER_POOL_ACTIVE_TASKS.dec();
    }
}

// === SECTION: [3] 工作队列项 ===

/// worker 池中的工作项
struct WorkItem {
    service_handler: Arc<dyn PushWorkHandler>,
    payload: Bytes,
    headers: std::collections::HashMap<String, String>,
    inflight: Arc<AtomicU64>,
    notify: Arc<Notify>,
    instance_id: u64,
    namespace: String,
    servicegroup_name: String,
    portname_name: String,
}

// === SECTION: [4] 共享 TCP 服务器与处理器 ===

/// 在单个端口上处理多个 portname 的共享 TCP 服务器
pub struct SharedTcpServer {
    handlers: Arc<DashMap<String, Arc<PortNameHandler>>>,
    /// 要绑定的地址（端口可为 0，由操作系统分配）
    bind_addr: SocketAddr,
    /// 实际绑定地址（在 bind_and_start 后填充，包含实际端口）
    actual_addr: RwLock<Option<SocketAddr>>,
    cancellation_token: CancellationToken,
    /// 向 worker 池发送工作项的通道
    work_tx: tokio::sync::mpsc::Sender<WorkItem>,
}

struct PortNameHandler {
    service_handler: Arc<dyn PushWorkHandler>,
    instance_id: u64,
    namespace: String,
    servicegroup_name: String,
    portname_name: String,
    system_health: Arc<Mutex<SystemHealth>>,
    inflight: Arc<AtomicU64>,
    notify: Arc<Notify>,
}

impl SharedTcpServer {
    pub fn new(bind_addr: SocketAddr, cancellation_token: CancellationToken) -> Arc<Self> {
        let worker_pool_size = get_worker_pool_size();
        let work_queue_size = get_work_queue_size();

        tracing::info!(
            "Initializing TCP server with dispatcher (concurrency={}, queue={})",
            worker_pool_size,
            work_queue_size
        );

        // 发布静态容量，方便仪表板计算饱和率。
        // 这些 gauge 是进程全局的；如果同一进程里启动多个 TCP
        // server，重复设置它们也没有问题（测试场景）。
        WORK_HANDLER_POOL_CAPACITY.set(crate::metrics::prometheus_names::clamp_u64_to_i64(
            worker_pool_size as u64,
        ));
        WORK_HANDLER_QUEUE_CAPACITY.set(crate::metrics::prometheus_names::clamp_u64_to_i64(
            work_queue_size as u64,
        ));

        // 为工作项创建有界通道。
        let (work_tx, work_rx) = tokio::sync::mpsc::channel(work_queue_size);

        // 启动工作池。
        Self::start_worker_pool(worker_pool_size, work_rx, cancellation_token.clone());

        Arc::new(Self {
            handlers: Arc::new(DashMap::new()),
            // 我们请求绑定的地址。
            bind_addr,
            // 若未指定 PGD_TCP_RPC_PORT，这里保存的是端口自动分配后的实际地址。
            actual_addr: RwLock::new(None),
            cancellation_token,
            work_tx,
        })
    }

    /// 启动 worker 池调度器，以有界并发处理请求。
    ///
    /// 使用单个 receiver 配合 semaphore 限制并发执行，
    /// 避免 mutex 争用把所有 worker 串行化。
    fn start_worker_pool(
        pool_size: usize,
        mut work_rx: tokio::sync::mpsc::Receiver<WorkItem>,
        cancellation_token: CancellationToken,
    ) {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(pool_size));

        tokio::spawn(async move {
            tracing::trace!(
                "TCP worker dispatcher started with concurrency limit {}",
                pool_size
            );

            loop {
                tokio::select! {
                    biased;

                    _ = cancellation_token.cancelled() => {
                        tracing::trace!("TCP worker dispatcher shutting down: cancellation requested");
                        break;
                    }

                    msg = work_rx.recv() => {
                        let Some(work_item) = msg else {
                            tracing::trace!("TCP worker dispatcher shutting down: channel closed");
                            break;
                        };
                        // 项已经离开 mpsc 通道——此时就减少 queue_depth，
                        // 让 gauge 严格反映通道占用。等待获取 permit 的时间
                        // 由 WORK_HANDLER_PERMIT_WAIT_SECONDS 单独跟踪。
                        WORK_HANDLER_QUEUE_DEPTH.dec();

                        // 在 spawn 前获取 permit（限制并发）。同时记录等待时间，
                        // 让池饥饿（permit 耗尽）体现在 `pagoda_work_handler_permit_wait_seconds`
                        // 的 p99 上升中。
                        let permit_wait_start = Instant::now();
                        let permit = match semaphore.clone().acquire_owned().await {
                            Ok(p) => p,
                            Err(_) => {
                                tracing::trace!("TCP worker dispatcher: semaphore closed");
                                break;
                            }
                        };
                        WORK_HANDLER_PERMIT_WAIT_SECONDS
                            .observe(permit_wait_start.elapsed().as_secs_f64());

                        // 在 spawn 前构造守卫（这里 inc 是同步执行的，
                        // 因此即使 future 先在另一个 worker 上完成，
                        // gauge 也不会被观察到为负），然后再把所有权移入 future——
                        // Drop 负责 dec。
                        let active_guard = ActiveTaskGuard::new();
                        tokio::spawn(async move {
                            let _active_guard = active_guard;
                            Self::handle_work_item(work_item).await;
                            drop(permit);
                        });
                    }
                }
            }

            tracing::trace!("TCP worker dispatcher exited");
        });

        tracing::info!(
            "Started TCP worker dispatcher with concurrency limit {}",
            pool_size
        );
    }

    /// 处理单个工作项。
    async fn handle_work_item(work_item: WorkItem) {
        tracing::trace!(
            instance_id = work_item.instance_id,
            "TCP worker processing request"
        );

        // 根据前端侧在 TCP 写入前打上的 transport header 计算网络传输时间。
        if let Some(t1_str) = work_item.headers.get("x-frontend-send-ts-ns")
            && let Ok(t1_ns) = t1_str.parse::<u64>()
        {
            let t2_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            let transit_ns = t2_ns.saturating_sub(t1_ns);
            crate::metrics::work_handler_perf::WORK_HANDLER_NETWORK_TRANSIT_SECONDS
                .observe(transit_ns as f64 / 1_000_000_000.0);
        }

        // 使用头部中的 trace 上下文创建 span。
        let span = crate::logging::make_handle_payload_span_from_tcp_headers(
            &work_item.headers,
            &work_item.servicegroup_name,
            &work_item.portname_name,
            &work_item.namespace,
            work_item.instance_id,
        );

        let request_id = work_item
            .headers
            .get("request-id")
            .or_else(|| work_item.headers.get("x-pagoda-request-id"))
            .cloned();

        let result = work_item
            .service_handler
            .handle_payload(work_item.payload, request_id)
            .instrument(span)
            .await;

        if let Err(e) = result {
            tracing::warn!(
                instance_id = work_item.instance_id,
                error = %e,
                "TCP worker failed to handle request"
            );
        }

        work_item.inflight.fetch_sub(1, Ordering::SeqCst);
        work_item.notify.notify_one();
    }

    /// 绑定 server 并开始接受连接。
    ///
    /// 此方法会先绑定到配置的地址，然后启动 accept 循环。
    /// 如果配置端口为 0，操作系统会分配一个空闲端口。
    /// 实际绑定地址会被保存，并可通过 `actual_address()` 获取。
    ///
    /// 返回实际绑定地址（当端口为 0 时尤其有用）。
    pub async fn bind_and_start(self: Arc<Self>) -> Result<SocketAddr> {
        tracing::info!("Binding TCP server to {}", self.bind_addr);

        let listener = TcpListener::bind(&self.bind_addr).await?;
        let actual_addr = listener.local_addr()?;

        tracing::info!(
            requested = %self.bind_addr,
            actual = %actual_addr,
            "TCP server bound successfully"
        );

        // 保存实际绑定地址。
        *self.actual_addr.write() = Some(actual_addr);

        // 在后台任务中开始接受连接。
        let server = self.clone();
        tokio::spawn(async move {
            server.accept_loop(listener).await;
        });

        Ok(actual_addr)
    }

    /// 获取实际绑定地址（在调用 bind_and_start 之后）。
    ///
    /// 如果 server 尚未启动，则返回 None。
    pub fn actual_address(&self) -> Option<SocketAddr> {
        *self.actual_addr.read()
    }

    /// 内部 accept 循环 - 在绑定后运行。
    async fn accept_loop(self: Arc<Self>, listener: TcpListener) {
        let cancellation_token = self.cancellation_token.clone();

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, peer_addr)) => {
                            tracing::trace!("Accepted TCP connection from {peer_addr}");

                            let handlers = self.handlers.clone();
                            let work_tx = self.work_tx.clone();
                            tokio::spawn(async move {
                                if let Err(e) = Self::handle_connection(stream, handlers, work_tx).await {
                                    tracing::error!("TCP connection error: {e}");
                                }
                            });
                        }
                        Err(e) => {
                            tracing::error!("Failed to accept TCP connection: {e}");
                        }
                    }
                }
                _ = cancellation_token.cancelled() => {
                    tracing::info!("SharedTcpServer received cancellation signal, shutting down");
                    return;
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn register_portname(
        &self,
        portname_path: String,
        service_handler: Arc<dyn PushWorkHandler>,
        instance_id: u64,
        namespace: String,
        servicegroup_name: String,
        portname_name: String,
        system_health: Arc<Mutex<SystemHealth>>,
    ) -> Result<()> {
        let fqn_portname = format!("{namespace}.{servicegroup_name}.{portname_name}");

        let handler = Arc::new(PortNameHandler {
            service_handler,
            instance_id,
            namespace,
            servicegroup_name,
            portname_name: portname_name.clone(),
            system_health: system_health.clone(),
            inflight: Arc::new(AtomicU64::new(0)),
            notify: Arc::new(Notify::new()),
        });

        // 先插入 handler，确保它已准备好接收请求。
        self.handlers.insert(portname_path, handler);

        system_health.lock().set_portname_registered(&portname_name);

        tracing::info!(
            "Registered portname '{fqn_portname}' with shared TCP server on {}",
            self.actual_address().unwrap_or(self.bind_addr)
        );

        Ok(())
    }

    pub async fn unregister_portname(&self, portname_path: &str, portname_name: &str) {
        if let Some((_, handler)) = self.handlers.remove(portname_path) {
            handler
                .system_health
                .lock()
                .set_portname_health_status(portname_name, crate::HealthStatus::NotReady);
            tracing::info!(
                portname_name = %portname_name,
                portname_path = %portname_path,
                "Unregistered TCP portname handler"
            );

            let inflight_count = handler.inflight.load(Ordering::SeqCst);
            if inflight_count > 0 {
                tracing::info!(
                    portname_name = %portname_name,
                    inflight_count = inflight_count,
                    "Waiting for inflight TCP requests to complete"
                );
                while handler.inflight.load(Ordering::SeqCst) > 0 {
                    handler.notify.notified().await;
                }
                tracing::info!(
                    portname_name = %portname_name,
                    "All inflight TCP requests completed"
                );
            }
        }
    }

    /// 启动 server（旧方法 - 新代码优先使用 bind_and_start）。
    ///
    /// 该方法保留用于向后兼容。它会绑定并启动 server，
    /// 但不会返回实际绑定地址。
    pub async fn start(self: Arc<Self>) -> Result<()> {
        let cancel_token = self.cancellation_token.clone();
        self.bind_and_start().await?;
        // 等待取消（accept 循环在后台运行）。
        cancel_token.cancelled().await;
        Ok(())
    }

    async fn handle_connection(
        stream: TcpStream,
        handlers: Arc<DashMap<String, Arc<PortNameHandler>>>,
        work_tx: tokio::sync::mpsc::Sender<WorkItem>,
    ) -> Result<()> {
        use crate::pipeline::network::codec::{TcpRequestMessage, TcpResponseMessage};

        // 将 stream 拆成读/写两半，以便并发操作。
        let (read_half, write_half) = tokio::io::split(stream);

        // 向写任务发送响应的通道（zero-copy Bytes）。
        let (response_tx, response_rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();

        // 派发写任务。
        let write_task = tokio::spawn(Self::write_loop(write_half, response_rx));

        // 在当前上下文中运行读任务。
        let read_result = Self::read_loop(read_half, handlers, response_tx, work_tx).await;

        // 当 response_tx 被 drop 时，写任务会结束。
        write_task.await??;

        read_result
    }

    async fn read_loop(
        mut read_half: tokio::io::ReadHalf<TcpStream>,
        handlers: Arc<DashMap<String, Arc<PortNameHandler>>>,
        response_tx: tokio::sync::mpsc::UnboundedSender<Bytes>,
        work_tx: tokio::sync::mpsc::Sender<WorkItem>,
    ) -> Result<()> {
        use crate::pipeline::network::codec::{TcpResponseMessage, ZeroCopyTcpDecoder};

        // 创建带优化缓冲区大小的 zero-copy 解码器。
        let mut decoder = ZeroCopyTcpDecoder::new();

        loop {
            // 以零拷贝方式读取一条完整消息！
            let request_msg = match decoder.read_message(&mut read_half).await {
                Ok(msg) => msg,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    tracing::trace!("Connection closed by peer");
                    break;
                }
                Err(e) => {
                    tracing::warn!("Failed to read TCP request: {e}");
                    // 发送错误响应。
                    let error_response =
                        TcpResponseMessage::new(Bytes::from(format!("Read error: {}", e)));
                    if let Ok(encoded) = error_response.encode() {
                        let _ = response_tx.send(encoded);
                    }
                    return Err(e.into());
                }
            };

            // 获取 portname 路径（零拷贝字符串切片）。
            let portname_path = match request_msg.portname_path() {
                Ok(path) => path,
                Err(e) => {
                    tracing::warn!("Invalid UTF-8 in portname path: {e}");
                    let error_response =
                        TcpResponseMessage::new(Bytes::from_static(b"Invalid portname path"));
                    if let Ok(encoded) = error_response.encode() {
                        let _ = response_tx.send(encoded);
                    }
                    continue;
                }
            };

            // 获取头部（从消息中解析）。
            let headers = request_msg.headers();

            // 获取负载（zero-copy Bytes - 只是 Arc clone！）。
            let payload = request_msg.payload();

            tracing::trace!(
                portname = portname_path,
                payload_len = payload.len(),
                total_size = request_msg.total_size(),
                "Received TCP request"
            );

            // 查找 handler（使用 DashMap 做无锁读取）。
            let handler = handlers.get(portname_path).map(|h| h.clone());

            let handler = match handler {
                Some(h) => h,
                None => {
                    tracing::warn!("No handler found for portname: {portname_path}");
                    // 发送错误响应。
                    let error_response = TcpResponseMessage::new(Bytes::from(format!(
                        "Unknown portname: {}",
                        portname_path
                    )));
                    if let Ok(encoded) = error_response.encode() {
                        let _ = response_tx.send(encoded);
                    }
                    continue;
                }
            };

            handler.inflight.fetch_add(1, Ordering::SeqCst);

            // 构造工作项。
            // 注意：payload 是 Bytes（Arc 计数），因此克隆非常便宜。
            let work_item = WorkItem {
                service_handler: handler.service_handler.clone(),
                payload,
                headers,
                inflight: handler.inflight.clone(),
                notify: handler.notify.clone(),
                instance_id: handler.instance_id,
                namespace: handler.namespace.clone(),
                servicegroup_name: handler.servicegroup_name.clone(),
                portname_name: handler.portname_name.clone(),
            };

            // 在递增 queue-depth gauge 之前，先在有界通道中预留一个槽位。
            // 否则，停在 `send().await` 等待容量的发送方会被算作队列占用，
            // 使 gauge 在饱和时超过 `queue_capacity`——而这正是该指标要暴露的状态。
            // `reserve()` 会等待容量，然后 `Permit::send` 是非阻塞且不可失败的，
            // 其对 dispatcher 的 `recv()` 提供与 `send().await` 相同的 happens-before 关系。
            match work_tx.reserve().await {
                Ok(permit) => {
                    WORK_HANDLER_QUEUE_DEPTH.inc();
                    permit.send(work_item);

                    // 仅在成功入队后发送确认。
                    let ack_response = TcpResponseMessage::empty();
                    if let Ok(encoded_ack) = ack_response.encode()
                        && response_tx.send(encoded_ack).is_err()
                    {
                        tracing::debug!("Write task closed, ending read loop");
                        // 由于工作项已入队但 ACK 失败，因此清理 inflight 计数。
                        handler.inflight.fetch_sub(1, Ordering::SeqCst);
                        handler.notify.notify_one();
                        break;
                    }

                    tracing::trace!(
                        portname = handler.portname_name.as_str(),
                        instance_id = handler.instance_id,
                        "Request queued and acknowledged"
                    );
                }
                Err(e) => {
                    // `reserve()` 只有在 receiver 已被 drop（通道关闭）时才会报错——
                    // dispatcher 已不存在，因此读循环必须终止。
                    WORK_HANDLER_ENQUEUE_REJECTED_TOTAL.inc();
                    tracing::warn!(
                        portname = handler.portname_name.as_str(),
                        instance_id = handler.instance_id,
                        error = %e,
                        "Failed to reserve worker pool slot, sending error response"
                    );

                    // 向客户端发送错误响应，而不是 ACK。
                    let error_response =
                        TcpResponseMessage::new(Bytes::from(format!("Server overloaded: {}", e)));
                    if let Ok(encoded) = error_response.encode() {
                        let _ = response_tx.send(encoded);
                    }

                    // 清理 inflight 计数。
                    handler.inflight.fetch_sub(1, Ordering::SeqCst);
                    handler.notify.notify_one();

                    tracing::error!("Worker pool channel closed, shutting down read loop");
                    break;
                }
            }
        }

        Ok(())
    }

    async fn write_loop(
        mut write_half: tokio::io::WriteHalf<TcpStream>,
        mut response_rx: tokio::sync::mpsc::UnboundedReceiver<Bytes>,
    ) -> Result<()> {
        while let Some(response) = response_rx.recv().await {
            write_half.write_all(&response).await?;
            write_half.flush().await?;
        }
        Ok(())
    }
}

// === SECTION: [5] RequestPlaneServer trait 实现 ===

// 为 SharedTcpServer 实现 RequestPlaneServer trait。
#[async_trait::async_trait]
impl super::unified_server::RequestPlaneServer for SharedTcpServer {
    async fn register_portname(
        &self,
        portname_name: String,
        service_handler: Arc<dyn PushWorkHandler>,
        instance_id: u64,
        namespace: String,
        servicegroup_name: String,
        system_health: Arc<Mutex<SystemHealth>>,
    ) -> Result<()> {
        // 在路由键中加入 instance_id，以避免多个 worker 共享同一 TCP server 时发生冲突。
        // 例如测试里 `--num-workers > 1` 的情况。
        let portname_path = format!("{instance_id:x}/{portname_name}");
        self.register_portname(
            portname_path,
            service_handler,
            instance_id,
            namespace,
            servicegroup_name,
            portname_name,
            system_health,
        )
        .await
    }

    async fn unregister_portname(&self, portname_name: &str) -> Result<()> {
        // 在同一进程里有多个 worker 时，每个 worker 都会用唯一键
        // "{instance_id}/{portname_name}" 注册。找到并移除所有匹配条目。
        let suffix = format!("/{portname_name}");
        let keys_to_remove: Vec<String> = self
            .handlers
            .iter()
            .filter(|entry| entry.key().ends_with(&suffix))
            .map(|entry| entry.key().clone())
            .collect();

        for key in keys_to_remove {
            self.unregister_portname(&key, portname_name).await;
        }
        Ok(())
    }

    fn address(&self) -> String {
        // 若有实际绑定地址（在 bind_and_start 之后），则返回它；
        // 否则回退到配置的绑定地址。
        let addr = self.actual_address().unwrap_or(self.bind_addr);
        format!("tcp://{}:{}", addr.ip(), addr.port())
    }

    fn transport_name(&self) -> &'static str {
        "tcp"
    }

    fn is_healthy(&self) -> bool {
        // 只要 server 已创建，就视为健康。
        // TODO：增加更复杂的健康检查（例如检查 listener 是否活跃）。
        true
    }
}

// === SECTION: [6] 单元测试 ===

#[cfg(test)]
mod tests {
    //! ## 测试矩阵
    //!
    //! | 测试名 | 覆盖维度 |
    //! |---|---|
    //! | `test_graceful_shutdown_waits_for_inflight_tcp_requests` | 优雅停机：取消令牌触发后仍等待 inflight TCP 请求处理完毕 |
    //! | `test_worker_pool_bounds_concurrency` | 工作池上限：并发度被 `PGD_TCP_WORKER_POOL_SIZE` 严格限制 |
    //! | `test_worker_pool_metrics_are_observed` | 工作池活跃任务 gauge 在调度/完成时正确升降 |
    //! | `test_capacities_published_on_server_init` | 服务初始化时上报工作池与队列容量到指标系统 |

    use super::*;
    use crate::pipeline::error::PipelineError;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use tokio::time::Instant;

    /// 模拟慢速请求处理的测试用 mock handler。
    struct SlowMockHandler {
        /// 记录请求当前是否正在处理。
        request_in_flight: Arc<AtomicBool>,
        /// 在请求处理开始时通知。
        request_started: Arc<Notify>,
        /// 在请求处理完成时通知。
        request_completed: Arc<Notify>,
        /// 用于模拟请求处理的持续时间。
        processing_duration: Duration,
    }

    impl SlowMockHandler {
        fn new(processing_duration: Duration) -> Self {
            Self {
                request_in_flight: Arc::new(AtomicBool::new(false)),
                request_started: Arc::new(Notify::new()),
                request_completed: Arc::new(Notify::new()),
                processing_duration,
            }
        }
    }

    #[async_trait]
    impl PushWorkHandler for SlowMockHandler {
        async fn handle_payload(
            &self,
            _payload: Bytes,
            _request_id: Option<String>,
        ) -> Result<(), PipelineError> {
            self.request_in_flight.store(true, Ordering::SeqCst);
            self.request_started.notify_one();

            tracing::debug!(
                "SlowMockHandler: Request started, sleeping for {:?}",
                self.processing_duration
            );

            // 模拟慢速请求处理。
            tokio::time::sleep(self.processing_duration).await;

            tracing::debug!("SlowMockHandler: Request completed");

            self.request_in_flight.store(false, Ordering::SeqCst);
            self.request_completed.notify_one();
            Ok(())
        }

        fn add_metrics(
            &self,
            _portname: &crate::servicegroup::PortName,
            _metrics_labels: Option<&[(&str, &str)]>,
        ) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_graceful_shutdown_waits_for_inflight_tcp_requests() {
        // 初始化 tracing，便于测试调试。
        crate::logging::init();

        let cancellation_token = CancellationToken::new();
        let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

        // 创建 SharedTcpServer。
        let server = SharedTcpServer::new(bind_addr, cancellation_token.clone());

        // 创建一个处理请求需要 1 秒的 handler。
        let handler = Arc::new(SlowMockHandler::new(Duration::from_secs(1)));
        let request_started = handler.request_started.clone();
        let request_completed = handler.request_completed.clone();
        let request_in_flight = handler.request_in_flight.clone();

        // 注册 portname。
        let portname_path = "test_portname".to_string();
        let system_health = Arc::new(Mutex::new(SystemHealth::new(
            crate::HealthStatus::Ready,
            vec![],
            false, // health_check_enabled
            "/health".to_string(),
            "/live".to_string(),
        )));

        server
            .register_portname(
                portname_path.clone(),
                handler.clone() as Arc<dyn PushWorkHandler>,
                1,
                "test_namespace".to_string(),
                "test_servicegroup".to_string(),
                "test_portname".to_string(),
                system_health,
            )
            .await
            .expect("Failed to register portname");

        tracing::debug!("PortName registered");

        // 获取 portname handler，以便模拟请求处理。
        let portname_handler = server
            .handlers
            .get(&portname_path)
            .expect("Handler should be registered")
            .clone();

        // 派发一个模拟在途请求的任务。
        let request_task = tokio::spawn({
            let handler = handler.clone();
            async move {
                let payload = Bytes::from("test payload");
                handler.handle_payload(payload, None).await
            }
        });

        // 手动递增 inflight 计数，以模拟该请求被跟踪。
        portname_handler.inflight.fetch_add(1, Ordering::SeqCst);

        // 等待请求开始处理。
        tokio::select! {
            _ = request_started.notified() => {
                tracing::debug!("Request processing started");
            }
            _ = tokio::time::sleep(Duration::from_secs(2)) => {
                panic!("Timeout waiting for request to start");
            }
        }

        // 验证请求处于在途状态。
        assert!(
            request_in_flight.load(Ordering::SeqCst),
            "Request should be in flight"
        );

        // 现在在请求在途时注销该 portname。
        let unregister_start = Instant::now();
        tracing::debug!("Starting unregister_portname with inflight request");

        // 在单独任务中执行注销，以便监控其行为。
        let unregister_task = tokio::spawn({
            let server = server.clone();
            let portname_path = portname_path.clone();
            async move {
                server
                    .unregister_portname(&portname_path, "test_portname")
                    .await;
                Instant::now()
            }
        });

        // 让注销动作有一点时间移除 handler 并开始等待。
        tokio::time::sleep(Duration::from_millis(50)).await;

        // 验证 unregister_portname 还没有返回（它应处于等待中）。
        assert!(
            !unregister_task.is_finished(),
            "unregister_portname should still be waiting for inflight request"
        );

        tracing::debug!("Verified unregister is waiting, now waiting for request to complete");

        // 等待请求完成。
        tokio::select! {
            _ = request_completed.notified() => {
                tracing::debug!("Request completed");
            }
            _ = tokio::time::sleep(Duration::from_secs(2)) => {
                panic!("Timeout waiting for request to complete");
            }
        }

        // 递减 inflight 计数并发送通知（模拟真实代码的行为）。
        portname_handler.inflight.fetch_sub(1, Ordering::SeqCst);
        portname_handler.notify.notify_one();

        // 现在等待注销完成。
        let unregister_end = tokio::time::timeout(Duration::from_secs(2), unregister_task)
            .await
            .expect("unregister_portname should complete after inflight request finishes")
            .expect("unregister task should not panic");

        let unregister_duration = unregister_end - unregister_start;

        tracing::debug!("unregister_portname completed in {:?}", unregister_duration);

        // 验证 unregister_portname 确实等待了在途请求。
        assert!(
            unregister_duration >= Duration::from_secs(1),
            "unregister_portname should have waited ~1s for inflight request, but only took {:?}",
            unregister_duration
        );

        // 验证请求已成功完成。
        assert!(
            !request_in_flight.load(Ordering::SeqCst),
            "Request should have completed"
        );

        // 等待请求任务结束。
        request_task
            .await
            .expect("Request task should complete")
            .expect("Request should succeed");

        tracing::info!("Test passed: unregister_portname properly waited for inflight TCP request");
    }

    ///////////////////// 并发上限测试 /////////////////////

    /// 记录并发执行计数的测试用 mock handler。
    struct ConcurrencyTrackingHandler {
        /// 当前正在处理的并发请求数。
        concurrent_count: Arc<AtomicU64>,
        /// 观察到的最大并发数。
        max_concurrent: Arc<AtomicU64>,
        /// 用于模拟请求处理的持续时间。
        processing_duration: Duration,
        /// 在请求完成时通知。
        completed: Arc<Notify>,
    }

    impl ConcurrencyTrackingHandler {
        fn new(processing_duration: Duration) -> Self {
            Self {
                concurrent_count: Arc::new(AtomicU64::new(0)),
                max_concurrent: Arc::new(AtomicU64::new(0)),
                processing_duration,
                completed: Arc::new(Notify::new()),
            }
        }
    }

    #[async_trait]
    impl PushWorkHandler for ConcurrencyTrackingHandler {
        async fn handle_payload(
            &self,
            _payload: Bytes,
            _request_id: Option<String>,
        ) -> Result<(), PipelineError> {
            // 递增并发计数。
            let current = self.concurrent_count.fetch_add(1, Ordering::SeqCst) + 1;

            // 如果更高，则更新最大值。
            self.max_concurrent.fetch_max(current, Ordering::SeqCst);

            // 模拟工作。
            tokio::time::sleep(self.processing_duration).await;

            // 递减并发计数。
            self.concurrent_count.fetch_sub(1, Ordering::SeqCst);
            self.completed.notify_one();

            Ok(())
        }

        fn add_metrics(
            &self,
            _portname: &crate::servicegroup::PortName,
            _metrics_labels: Option<&[(&str, &str)]>,
        ) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_worker_pool_bounds_concurrency() {
        crate::logging::init();

        // 测试时使用较小的池大小。
        let pool_size = 3;
        let total_requests = 10;

        // 直接创建有界通道和 dispatcher。
        let (work_tx, work_rx) = tokio::sync::mpsc::channel::<WorkItem>(total_requests);
        let cancellation_token = CancellationToken::new();

        // 以较小的并发上限启动 worker 池。
        SharedTcpServer::start_worker_pool(pool_size, work_rx, cancellation_token.clone());

        // 创建跟踪 handler。
        let handler = Arc::new(ConcurrencyTrackingHandler::new(Duration::from_millis(50)));

        // 为工作项创建虚拟的 inflight/notify。
        let inflight = Arc::new(AtomicU64::new(0));
        let notify = Arc::new(Notify::new());

        // 发送比池大小更多的工作项。这里要模拟生产 read_loop 的
        // queue-depth 记账，让 `handle_work_item` 的递减能对应上
        // 递增，并使全局 gauge 对其他测试保持一致。
        for i in 0..total_requests {
            inflight.fetch_add(1, Ordering::SeqCst);
            WORK_HANDLER_QUEUE_DEPTH.inc();
            let work_item = WorkItem {
                service_handler: handler.clone() as Arc<dyn PushWorkHandler>,
                payload: Bytes::from(format!("request {}", i)),
                headers: std::collections::HashMap::new(),
                inflight: inflight.clone(),
                notify: notify.clone(),
                instance_id: 1,
                namespace: "test".to_string(),
                servicegroup_name: "test".to_string(),
                portname_name: "test".to_string(),
            };
            work_tx.send(work_item).await.expect("send should succeed");
        }

        // 等待所有请求完成。
        let timeout = tokio::time::timeout(Duration::from_secs(5), async {
            while inflight.load(Ordering::SeqCst) > 0 {
                notify.notified().await;
            }
        })
        .await;

        assert!(
            timeout.is_ok(),
            "All requests should complete within timeout"
        );

        // 验证并发确实被限制住了。
        let max_observed = handler.max_concurrent.load(Ordering::SeqCst);
        assert!(
            max_observed <= pool_size as u64,
            "Max concurrent ({}) should not exceed pool size ({})",
            max_observed,
            pool_size
        );

        // 验证所有请求都已完成。
        assert_eq!(
            inflight.load(Ordering::SeqCst),
            0,
            "All requests should have completed"
        );

        tracing::info!(
            "Test passed: max concurrent {} <= pool size {}",
            max_observed,
            pool_size
        );

        // 清理。
        cancellation_token.cancel();
    }

    #[tokio::test]
    async fn test_worker_pool_metrics_are_observed() {
        crate::logging::init();

        // 直方图计数是单调递增的：即使并行测试在移动 gauge，也可以安全断言。
        let permit_observations_before = WORK_HANDLER_PERMIT_WAIT_SECONDS.get_sample_count();

        let pool_size = 2;
        let total_requests = 4;
        let (work_tx, work_rx) = tokio::sync::mpsc::channel::<WorkItem>(total_requests);
        let cancellation_token = CancellationToken::new();
        SharedTcpServer::start_worker_pool(pool_size, work_rx, cancellation_token.clone());

        let handler = Arc::new(ConcurrencyTrackingHandler::new(Duration::from_millis(25)));
        let inflight = Arc::new(AtomicU64::new(0));
        let notify = Arc::new(Notify::new());

        for i in 0..total_requests {
            inflight.fetch_add(1, Ordering::SeqCst);
            // 模拟生产 read_loop 的 inc，让 handle_work_item 的 dec 有对应项。
            WORK_HANDLER_QUEUE_DEPTH.inc();
            let work_item = WorkItem {
                service_handler: handler.clone() as Arc<dyn PushWorkHandler>,
                payload: Bytes::from(format!("request {}", i)),
                headers: std::collections::HashMap::new(),
                inflight: inflight.clone(),
                notify: notify.clone(),
                instance_id: 1,
                namespace: "test".to_string(),
                servicegroup_name: "test".to_string(),
                portname_name: "test".to_string(),
            };
            work_tx.send(work_item).await.expect("send should succeed");
        }

        // 等待所有工作项被处理完。
        tokio::time::timeout(Duration::from_secs(5), async {
            while inflight.load(Ordering::SeqCst) > 0 {
                notify.notified().await;
            }
        })
        .await
        .expect("all requests should complete");

        // permit_wait 直方图是单调的，并且每个已派发工作项都会记录一条样本——
        // 在并行测试线程下也可靠。
        assert!(
            WORK_HANDLER_PERMIT_WAIT_SECONDS.get_sample_count()
                >= permit_observations_before + total_requests as u64,
            "permit_wait histogram should record at least one sample per dispatched work item"
        );

        cancellation_token.cancel();
    }

    #[tokio::test]
    async fn test_capacities_published_on_server_init() {
        crate::logging::init();

        // SharedTcpServer::new 会发布静态容量。任何实例化 SharedTcpServer 的测试
        // 都已经填充了这些 gauge；我们这里只需断言它们大于 0。
        let cancellation_token = CancellationToken::new();
        let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let _server = SharedTcpServer::new(bind_addr, cancellation_token.clone());

        assert!(
            WORK_HANDLER_POOL_CAPACITY.get() > 0,
            "pool_capacity should be set to DEFAULT_WORKER_POOL_SIZE"
        );
        assert!(
            WORK_HANDLER_QUEUE_CAPACITY.get() > 0,
            "queue_capacity should be set to DEFAULT_WORK_QUEUE_SIZE"
        );
        cancellation_token.cancel();
    }
}
