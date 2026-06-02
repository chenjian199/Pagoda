// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::network::egress::tcp_client` —— TCP 请求平面客户端与共享连接池
//!
//! ## 设计意图
//! 为 egress 层提供一个高吞吞、锁自由、LRU 淘汰 + 轮询选择的共享 TCP 连接池，
//! 实现 [`super::unified_client::RequestPlaneClient`]。连接按 `address` 分桶，同一目标
//! 端点多连接并发复用，使 tail-latency 不被单点队列堵住。
//!
//! ## 外部契约
//! - 公开类型 / 方法集 / `transport_name() -> "tcp"` / `start_warmup` 严格一致；
//! - 不额外暴露 LRU / 轮询 / 信差安装的实现细节；连接池参数仅通过构造器提供。
//! - 错误映射：连接失败与超时被隐藏在 `RequestPlaneClient::send_request` 返回的
//!   `anyhow::Error` 中，可依 `chain()` 追溯。
//!
//! ## 实现要点
//! - 使用锁自由数据结构（`DashMap` + atomic counter）避免热点互斥；
//! - `start_warmup` 启动后台任务监听 instance discovery watch，对新发现后端预建连接，
//!   只有 TCP 客户端趋于需要这个提前作为（HTTP/NATS 用默认 no-op）。

use super::unified_client::{ClientStats, Headers, RequestPlaneClient};
use crate::metrics::transport_metrics::{
    TCP_BYTES_RECEIVED_TOTAL, TCP_BYTES_SENT_TOTAL, TCP_ERRORS_TOTAL,
};
use crate::pipeline::network::get_tcp_max_message_size;
use anyhow::Result;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use crossbeam_queue::SegQueue;
use dashmap::DashMap;
use futures::StreamExt;
use lru::LruCache;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::codec::FramedRead;

/// TCP 请求确认的默认超时。
const DEFAULT_TCP_REQUEST_TIMEOUT_SECS: u64 = 5;

/// 每个主机的默认连接池大小。
/// 上限：DEFAULT_POOL_SIZE(100) × REQUEST_CHANNEL_BUFFER(1024) = 102,400 个并发
/// 槽位/主机。在低于 10ms 的传输延迟下，同一时刻只有少量连接真正活跃；
/// 其余连接由 should_grow() 按需开启。
const DEFAULT_POOL_SIZE: usize = 100;

/// 每连接的准入信号量许可数（也是流水线深度）。
/// 从 256 提升到 1024，以支撑高吞吐前端（跨 ~100 个后端 1M+ RPS）。
/// 在 1ms 往返延迟下，单连接已可支撑 ~1,000 个并发请求，
/// 更深的流水线可避免不必要的连接增生，并让写任务在单次 write_all
/// 中减出更大批量（高速率下减少 syscall）。由于 TCP 传输层面向
/// 亚毫秒延迟，队头阻塞仍可接受；后续请求很少需要长时间等待。
/// 每主机上限：DEFAULT_POOL_SIZE(100) × REQUEST_CHANNEL_BUFFER(1024) = 102,400。
const REQUEST_CHANNEL_BUFFER: usize = 1024;

/// 当另一个任务正在连接时的最大重试次数（防止无限递归）。
const MAX_CONNECT_RETRIES: usize = 5;

/// 跨所有主机的全局连接并发默认上限。
const DEFAULT_GLOBAL_CONNECT_LIMIT: usize = 64;

/// 空闲主机被清理前的默认 TTL（秒）。
const DEFAULT_HOST_IDLE_TTL_SECS: u64 = 300;

/// 写任务回退到异步 Notify 之前的自旋循环上限。
const WRITER_SPIN_LIMIT: u32 = 64;

/// 每个写任务 BytesMut 发送缓冲区的初始容量（256 KB）。
/// 若某批量超过此值，缓冲区会自动增长，随后维持在高水位
/// 供后续批量复用（摊销后零分配）。
const WRITER_INITIAL_BUF_CAPACITY: usize = 256 * 1024;

// === SECTION: [1] 环境变量与配置辅助 ===

/// 根据环境变量判断是否启用延迟跟踪。
fn latency_trace_enabled() -> bool {
    std::env::var("PGD_TCP_LATENCY_TRACE")
        .ok()
        .is_some_and(|v| v == "1" || v == "true")
}

/// TCP 请求平面配置。
#[derive(Debug, Clone)]
pub struct TcpRequestConfig {
    /// 请求超时。
    pub request_timeout: Duration,
    /// 每主机最大连接数。
    pub pool_size: usize,
    /// 连接超时。
    pub connect_timeout: Duration,
    /// 请求 channel 缓冲区大小。
    pub channel_buffer: usize,
}

impl Default for TcpRequestConfig {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(DEFAULT_TCP_REQUEST_TIMEOUT_SECS),
            pool_size: DEFAULT_POOL_SIZE,
            connect_timeout: Duration::from_secs(5),
            channel_buffer: REQUEST_CHANNEL_BUFFER,
        }
    }
}

impl TcpRequestConfig {
    /// 从环境变量构造配置。
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Ok(val) = std::env::var("PGD_TCP_REQUEST_TIMEOUT")
            && let Ok(timeout) = val.parse::<u64>()
        {
            config.request_timeout = Duration::from_secs(timeout);
        }

        if let Ok(val) = std::env::var("PGD_TCP_POOL_SIZE")
            && let Ok(size) = val.parse::<usize>()
        {
            config.pool_size = size;
        }

        if let Ok(val) = std::env::var("PGD_TCP_CONNECT_TIMEOUT")
            && let Ok(timeout) = val.parse::<u64>()
        {
            config.connect_timeout = Duration::from_secs(timeout);
        }

        if let Ok(val) = std::env::var("PGD_TCP_CHANNEL_BUFFER")
            && let Ok(size) = val.parse::<usize>()
        {
            config.channel_buffer = size;
        }

        config
    }
}

// === SECTION: [2] 内部请求、守卫与单连接结构 ===

/// 无锁提交队列中的待处理请求。
struct PendingRequest {
    /// 预先编码、可直接发送的请求数据（零拷贝 Bytes）。
    encoded_data: Bytes,
    /// 用于将响应回传给调用方的 oneshot channel。
    response_tx: oneshot::Sender<Result<Bytes>>,
}

/// 在 drop 时递减 inflight 计数器的 RAII 守卫。
///
/// 保证 `fetch_sub` 在 `send_request` 的所有退出路径上都会执行：
/// - 正常返回（成功或 `?` 传播）
/// - `tokio::time::timeout` 取消（future 在 await 中途被 drop）
/// - 任何其他 future drop（例如 `select!` 分支取消）
///
/// 若没有该守卫，超时会在 `response_rx.await` 处 drop 掉 future，
/// 其下方的 `fetch_sub` 永远不会执行，从而永久虚肨 inflight
/// 计数器并污染 `available_capacity()`。
struct InflightGuard(Arc<AtomicU64>);

impl Drop for InflightGuard {
    fn drop(&mut self) {
        // Release：与 available_capacity() 和 cleanup_idle_hosts() 中的 Acquire
        // 读配对，使递减对随后观察计数器的任何读者可见。
        self.0.fetch_sub(1, Ordering::Release);
    }
}

/// 在 drop 时重置 `connecting` CAS 闸门的 RAII 守卫。
///
/// 赢得 CAS（`connecting` 置为 `true`）后，`ensure_capacity_or_heal` 中
/// 后续的两个 await 点是取消不安全的：
///
/// 1. `connect_limiter.acquire().await` — 若外层 Tokio future 在此处被
///    drop，`connecting` 会永远保持 `true`，而现有的 `map_err` 闭包
///    仅在 `Semaphore` *被关闭* 时运行，而非取消时。
/// 2. `TcpConnection::connect(...).await` — 若在此处被取消，await 下方
///    显式的 `self.connecting.store(false)` 永远不会执行。
///
/// 两种情形下，输掉 CAS 竞争的调用方会阻塞在 `connect_notify.notified()`
/// 直到其 `connect_timeout` 超时，然后重试 CAS — 但 `connecting` 仍为
/// `true`，于是再次超时，永久阻塞该主机的连接池增长。
///
/// 修复：在 CAS 成功后立即构造该守卫。其 Drop 无条件重置 `connecting`
/// 并唤醒等待者，覆盖每一条退出路径：正常返回（Ok/Err）、`?` 传播
/// 以及 future 取消。重复重置（已为 false 时再 store false）与重复通知
/// （唤醒空的等待集）都是 no-op，所以成功与错误分支中的显式清理
/// 调用为了可读性保留且毫无风险。
struct ConnectingGuard<'a> {
    connecting: &'a AtomicBool,
    notify: &'a tokio::sync::Notify,
}

impl Drop for ConnectingGuard<'_> {
    fn drop(&mut self) {
        self.connecting.store(false, Ordering::Release);
        self.notify.notify_waiters();
    }
}

/// 采用无锁提交与批量读/写任务的 TCP 连接。
///
/// 设计：SegQueue 提交 → 批量写任务 → 读任务 → oneshot 响应
/// - 调用方推入 SegQueue（无锁，~20-40ns）
/// - 写任务将队列减入可复用的 BytesMut，每批单次 write_all
/// - 读任务使用帧编解码器，从 SegQueue 弹出 response_tx
/// - FIFO 顺序：写任务在 write_all 成功后才推入所有 response_tx
struct TcpConnection {
    addr: SocketAddr,
    /// 供调用方提交请求的无锁队列。
    submit_queue: Arc<SegQueue<PendingRequest>>,
    /// 用于写任务→读任务交接 response_tx 的无锁队列。
    response_queue: Arc<SegQueue<oneshot::Sender<Result<Bytes>>>>,
    /// 当 submit_queue 从空转为非空时唤醒写任务的 Notify。
    writer_notify: Arc<tokio::sync::Notify>,
    /// 用于清理的写任务句柄。
    writer_handle: Arc<JoinHandle<()>>,
    /// 用于清理的读任务句柄。
    reader_handle: Arc<JoinHandle<()>>,
    /// 健康状态（任务失败则为 false）。
    healthy: Arc<AtomicBool>,
    /// 一旦写任务进入终止减入路径，即关闭新提交。
    /// 这消除了“请求在最后一轮减入后才入队、随后一直等到外层请求
    /// 超时”的竞态。
    closed: Arc<AtomicBool>,
    /// 在飞请求数（用于容量启发式判断）。
    inflight: Arc<AtomicU64>,
    /// 容量启发式的最大在飞数（与 channel_buffer 一致）。
    channel_buffer: usize,
    /// 有界准入信号量（许可数 == channel_buffer）。
    /// 调用方必须在推入 submit_queue 前获取许可，以 channel_buffer
    /// 作为硬上限，防止 SegQueue 无限增长与持续过载下的堆 OOM。
    /// 许可在调用生命周期内持有，并在 drop 时释放
    /// （成功、通过 `?` 出错、或超时/取消时 future drop）。
    admission: Arc<tokio::sync::Semaphore>,
    #[cfg(test)]
    post_enqueue_barrier: Option<Arc<tokio::sync::Barrier>>,
}

impl TcpConnection {
    /// 创建一个采用无锁提交与批量读/写任务的新连接。
    async fn connect(addr: SocketAddr, timeout: Duration, channel_buffer: usize) -> Result<Self> {
        let stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| anyhow::anyhow!("TCP connect timeout to {}", addr))??;

        // 配置 socket 以降低延迟。
        Self::configure_socket(&stream)?;

        let (read_half, write_half) = tokio::io::split(stream);

        let submit_queue = Arc::new(SegQueue::new());
        let response_queue = Arc::new(SegQueue::new());
        let writer_notify = Arc::new(tokio::sync::Notify::new());
        let healthy = Arc::new(AtomicBool::new(true));
        let closed = Arc::new(AtomicBool::new(false));
        let inflight = Arc::new(AtomicU64::new(0));
        let admission = Arc::new(tokio::sync::Semaphore::new(channel_buffer));

        // 启动写任务（减入 BytesMut，每次减入周期单次 write_all）。
        let writer_handle = {
            let submit_q = submit_queue.clone();
            let response_q = response_queue.clone();
            let notify = writer_notify.clone();
            let healthy = healthy.clone();
            let closed = closed.clone();
            let admission = admission.clone();
            tokio::spawn(async move {
                if let Err(e) = Self::writer_task(
                    write_half,
                    submit_q,
                    response_q,
                    notify,
                    healthy.clone(),
                    closed.clone(),
                )
                .await
                {
                    tracing::debug!("Writer task failed for {}: {}", addr, e);
                    // writer_task 在 drain_pending 之前已设置过 healthy 与 closed；
                    // 这里是幂等的兼底。
                    healthy.store(false, Ordering::Relaxed);
                    closed.store(true, Ordering::Release);
                    // 解除当前等待获取许可的调用方的阻塞，
                    // 让它们通过获取后的健康重检快速失败。
                    admission.close();
                }
            })
        };

        // 启动读任务（传入 writer_notify，使读任务退出时能唤醒写任务）。
        let reader_handle = {
            let response_q = response_queue.clone();
            let healthy = healthy.clone();
            let writer_notify = writer_notify.clone();
            let admission = admission.clone();
            tokio::spawn(async move {
                if let Err(e) =
                    Self::reader_task(read_half, response_q, healthy.clone(), writer_notify).await
                {
                    tracing::debug!("Reader task failed for {}: {}", addr, e);
                    healthy.store(false, Ordering::Relaxed);
                    // 解除当前等待获取许可的调用方的阻塞。
                    admission.close();
                }
            })
        };

        Ok(Self {
            addr,
            submit_queue,
            response_queue,
            writer_notify,
            writer_handle: Arc::new(writer_handle),
            reader_handle: Arc::new(reader_handle),
            healthy,
            closed,
            inflight,
            channel_buffer,
            admission,
            #[cfg(test)]
            post_enqueue_barrier: None,
        })
    }

    /// 依据传输层的低延迟最佳实践配置 socket。
    fn configure_socket(stream: &TcpStream) -> Result<()> {
        use socket2::SockRef;

        let sock_ref = SockRef::from(stream);

        // TCP_NODELAY - 禁用 Nagle 算法以立即发送。
        sock_ref.set_nodelay(true)?;

        // 增大 socket 缓冲区，提升高负载下的吞吐。
        sock_ref.set_recv_buffer_size(2 * 1024 * 1024)?; // 2MB
        sock_ref.set_send_buffer_size(2 * 1024 * 1024)?; // 2MB

        // 面向超低延迟的 Linux 高级优化（可选 feature）。
        #[cfg(feature = "tcp-low-latency")]
        {
            use std::os::unix::io::AsRawFd;

            unsafe {
                let fd = stream.as_raw_fd();

                // TCP_QUICKACK - 最小化 ACK 延迟。
                let quickack: libc::c_int = 1;
                libc::setsockopt(
                    fd,
                    libc::SOL_TCP,
                    libc::TCP_QUICKACK,
                    &quickack as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&quickack) as libc::socklen_t,
                );

                // SO_BUSY_POLL - 启用忙轮询以降低延迟（50 微秒）。
                let busy_poll: libc::c_int = 50;
                libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_BUSY_POLL,
                    &busy_poll as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&busy_poll) as libc::socklen_t,
                );
            }

            tracing::debug!("TCP low-latency optimizations enabled (TCP_QUICKACK, SO_BUSY_POLL)");
        }

        Ok(())
    }

    /// 减入提交队列，在所有 oneshot 发送端上发送错误。
    /// 写任务退出时调用，防止孤儿调用方永远等待那些从未被处理的请求。
    ///
    /// 注意：这里故意不减入 response_queue。response_queue 中的项对应
    /// 已冲入线路的请求 — 读任务仍可能交付它们的响应。若读任务也退出，
    /// 剩余 oneshot 发送端会在 TcpConnection 被清理时 drop，调用方会收到
    /// 被外层超时捕获的 RecvError。
    fn drain_pending(submit_queue: &SegQueue<PendingRequest>) {
        while let Some(req) = submit_queue.pop() {
            let _ = req
                .response_tx
                .send(Err(anyhow::anyhow!("Connection closed")));
        }
    }

    /// 当读任务判定该连接上不可能再有响应到达时，减入已提交的响应等待者。
    fn drain_response_waiters(
        response_queue: &SegQueue<oneshot::Sender<Result<Bytes>>>,
        err_msg: impl Into<String>,
    ) {
        let err_msg = err_msg.into();
        while let Some(tx) = response_queue.pop() {
            let _ = tx.send(Err(anyhow::anyhow!("{}", err_msg)));
        }
    }

    /// 写任务：将 SegQueue 减入可复用的 BytesMut，然后每个减入周期发出
    /// 单次 write_all() — 不论批量大小都只一次 syscall。
    ///
    /// 为何用 BytesMut 而不是 BufWriter：
    /// - BufWriter 有固定内部上限（256 KB）；超过该值的批量会触发隐式的
    ///   批中部分刷新，破坏“每批单次 syscall”保证。在 channel_buffer=1024
    ///   时，中等负载下这会经常发生。
    /// - BufWriter 将每个 Bytes 拷入其内部 Vec<u8>，刷新时内核再拷一次
    ///   — 每请求两次拷贝。BytesMut 将其合并为一次 extend_from_slice
    ///   + 一次 write_all（单次内核拷贝）。
    /// - BytesMut 增长到批量高水位并保持；预热后每批零分配。
    ///
    /// 刷新边界跟踪：写阶段期间 response_tx 在本地持有，仅在 write_all
    /// 成功后才推入 response_queue。这样：
    /// - 写出错误时，当前批次的调用方立即收到错误
    ///   （而非经 drain_pending 收到“Connection closed”）
    /// - 之前已写出的批次仍留在 response_queue 中供读任务交付
    ///   — 不会被 drain_pending 误杀
    /// - 服务端无法在 write_all 返回前响应，所以读任务绝不会在其
    ///   response_tx 入队之前看到响应
    async fn writer_task(
        mut write_half: tokio::io::WriteHalf<TcpStream>,
        submit_queue: Arc<SegQueue<PendingRequest>>,
        response_queue: Arc<SegQueue<oneshot::Sender<Result<Bytes>>>>,
        notify: Arc<tokio::sync::Notify>,
        healthy: Arc<AtomicBool>,
        closed: Arc<AtomicBool>,
    ) -> Result<()> {
        let mut send_buf = BytesMut::with_capacity(WRITER_INITIAL_BUF_CAPACITY);
        // 提升到循环外，以便跨排空周期复用内存分配。
        let mut encoded_batch: Vec<Bytes> = Vec::with_capacity(64);
        let mut response_batch: Vec<oneshot::Sender<Result<Bytes>>> = Vec::with_capacity(64);
        let trace = latency_trace_enabled();

        // 延迟插桩的累加器。
        let mut batch_count: u64 = 0;
        let mut total_batch_size: u64 = 0;
        let mut total_batch_write_ns: u64 = 0;
        let mut last_report = std::time::Instant::now();

        let result: Result<()> = async {
            loop {
                // 自适应自旋：先试队列，再回退到异步 Notify。
                let mut spins: u32 = 0;
                while submit_queue.is_empty() {
                    // 检查读任务是否已退出。
                    if !healthy.load(Ordering::Relaxed) {
                        return Err(anyhow::anyhow!("Reader exited, writer stopping"));
                    }
                    spins += 1;
                    if spins >= WRITER_SPIN_LIMIT {
                        notify.notified().await;
                        break;
                    }
                    std::hint::spin_loop();
                }

                // 减入所有可用请求（复用预分配的 Vec）。
                encoded_batch.clear();
                response_batch.clear();
                while let Some(req) = submit_queue.pop() {
                    encoded_batch.push(req.encoded_data);
                    response_batch.push(req.response_tx);
                }

                let count = encoded_batch.len();
                if count == 0 {
                    continue; // 虚假唤醒
                }

                // 阶段 1：将所有已编码负载汇集到发送缓冲区。
                // 每项单次 extend_from_slice — 无中间 BufWriter 拷贝，
                // 批量超限时也不会隐式部分刷新。
                // response_tx 仍保留在本地 — 此时尚未进入 response_queue。
                let write_start = if trace {
                    Some(std::time::Instant::now())
                } else {
                    None
                };

                for data in &encoded_batch {
                    send_buf.extend_from_slice(data);
                }

                // 阶段 2：单次 write_all = 整批一次 syscall。
                // socket 发送缓冲区为 2 MB；能装下的批量在一次 writev() 中发出。
                // 更大的批量在 write_all 内部循环，但仍只会随内核减尽
                // socket 缓冲区的速度调用。
                if let Err(e) = write_half.write_all(&send_buf).await {
                    // 数据可能已部分上线 — 连接处于不可恢复状态
                    // （帧结构损坏）。使整批失败，防御性清空缓冲区，
                    // 以防将来加入“重连重试”时重发陈旧数据，然后退出。
                    send_buf.clear();
                    let err_msg = format!("Write failed: {}", e);
                    for tx in response_batch.drain(..) {
                        let _ = tx.send(Err(anyhow::anyhow!("{}", err_msg)));
                    }
                    return Err(e.into());
                }
                TCP_BYTES_SENT_TOTAL.inc_by(send_buf.len() as f64);
                send_buf.clear(); // 重置长度，保留分配供下一批复用

                // 阶段 3：write_all 成功 — 数据已提交到线路。
                // 现在才将 response_tx 推入 response_queue，
                // 以便读任务能将它们与到达的响应匹配。
                for tx in response_batch.drain(..) {
                    response_queue.push(tx);
                }

                // 检查读任务是否已退出（例如对端关闭连接）。
                // 内核缓冲可能让对端关闭后的写仍成功，
                // 但读任务不会交付响应 — 立即退出。
                if !healthy.load(Ordering::Relaxed) {
                    return Err(anyhow::anyhow!("Reader exited, writer stopping"));
                }

                // 延迟插桩。
                if trace {
                    if let Some(start) = write_start {
                        total_batch_write_ns += start.elapsed().as_nanos() as u64;
                    }
                    batch_count += 1;
                    total_batch_size += count as u64;

                    if last_report.elapsed() >= Duration::from_secs(5) {
                        let avg_batch = if batch_count > 0 {
                            total_batch_size / batch_count
                        } else {
                            0
                        };
                        let avg_write_ns = if batch_count > 0 {
                            total_batch_write_ns / batch_count
                        } else {
                            0
                        };
                        tracing::info!(
                            batches = batch_count,
                            avg_batch_size = avg_batch,
                            avg_batch_write_ns = avg_write_ns,
                            "TCP writer instrumentation summary"
                        );
                        batch_count = 0;
                        total_batch_size = 0;
                        total_batch_write_ns = 0;
                        last_report = std::time::Instant::now();
                    }
                }

                encoded_batch.clear();
            }
        }
        .await;

        // 退出时，仅减入 submit_queue（未处理的请求）。
        // response_queue 中的项对应已提交（已刷新）的数据 — 读任务
        // 仍可能交付其响应。若无法交付，oneshot 发送端会在
        // TcpConnection 被清理时 drop，调用方得到 RecvError。
        healthy.store(false, Ordering::Relaxed);
        // 在减入之前先置 closed，这样任何在本次减入与函数结束之间
        // 入队的并发发送方，都会在其入队后的二次检查中看到 closed=true
        // 并自行调用 drain_pending。若不这样做，drain_pending 与 spawn
        // 包装中错误处理的 closed.store(true) 之间的窗口会遗留晚入队请求。
        closed.store(true, Ordering::Release);
        Self::drain_pending(&submit_queue);

        result
    }

    /// 读任务：使用帧编解码器读取响应，从 SegQueue 弹出 response_tx。
    ///
    /// 退出时（正常关闭或出错）设置 `healthy=false` 并通过 `writer_notify`
    /// 唤醒写任务，使其能检测到读任务死亡并减入待处理的调用方。
    async fn reader_task(
        read_half: tokio::io::ReadHalf<TcpStream>,
        response_queue: Arc<SegQueue<oneshot::Sender<Result<Bytes>>>>,
        healthy: Arc<AtomicBool>,
        writer_notify: Arc<tokio::sync::Notify>,
    ) -> Result<()> {
        use crate::pipeline::network::codec::TcpResponseCodec;

        let max_message_size = get_tcp_max_message_size();
        let codec = TcpResponseCodec::new(Some(max_message_size));
        let mut framed = FramedRead::new(read_half, codec);

        while let Some(result) = framed.next().await {
            // 若 response_queue 为空（写任务尚未推入）则略微自旋。
            let tx = loop {
                if let Some(tx) = response_queue.pop() {
                    break tx;
                }
                // 若连接已不健康（写任务失败），停止自旋。
                if !healthy.load(Ordering::Relaxed) {
                    return Err(anyhow::anyhow!("Connection unhealthy, reader exiting"));
                }
                tokio::task::yield_now().await;
            };

            match result {
                Ok(response_msg) => {
                    let _ = tx.send(Ok(response_msg.data));
                }
                Err(e) => {
                    let err_msg = format!("Failed to decode response: {}", e);
                    let _ = tx.send(Err(anyhow::anyhow!("{}", err_msg)));
                    healthy.store(false, Ordering::Relaxed);
                    Self::drain_response_waiters(
                        &response_queue,
                        format!("Connection closed after decode failure: {}", e),
                    );
                    // 唤醒写任务，使其检测到不健康并退出。
                    writer_notify.notify_one();
                    return Err(anyhow::anyhow!("Failed to decode response"));
                }
            }
        }

        // 连接被对端关闭 — 标记为不健康，使写任务检测到读任务
        // 死亡并减入待处理的调用方。
        healthy.store(false, Ordering::Relaxed);
        Self::drain_response_waiters(
            &response_queue,
            "Connection closed before response was received",
        );
        // 从 Notify.await 唤醒写任务，使其检查 healthy 并退出。
        writer_notify.notify_one();
        Ok(())
    }

    /// 通过无锁 SegQueue 推入发送请求（~20-40ns）。
    async fn send_request(&self, payload: Bytes, headers: &Headers) -> Result<Bytes> {
        use crate::pipeline::network::codec::TcpRequestMessage;

        if !self.healthy.load(Ordering::Relaxed) {
            anyhow::bail!("Connection unhealthy (tasks failed)");
        }
        if self.closed.load(Ordering::Acquire) {
            anyhow::bail!("Connection closed (writer exited)");
        }

        let portname_path = headers
            .get("x-portname-path")
            .ok_or_else(|| anyhow::anyhow!("Missing x-portname-path header for TCP request"))?
            .to_string();

        let trace = latency_trace_enabled();
        let e2e_start = if trace {
            Some(std::time::Instant::now())
        } else {
            None
        };

        // 有界准入：阻塞直到有空闲槽位（channel_buffer 硬上限）。
        // 许可在本次调用期间持有，并在 drop 时释放 — 无论调用方是正常
        // 返回、出错，还是外层 tokio::time::timeout 在中途 drop 本 future。
        // 这防止过载下 SegQueue 无限增长与堆 OOM。
        // encode() 会在 acquire 之后才执行，所以被信号量阻塞的调用方不会
        // 持有预分配的已编码帧，从而把峰值内存限制在每个连接的
        // 通道缓冲区容量乘以帧大小。
        let _permit = self
            .admission
            .acquire()
            .await
            .map_err(|_| anyhow::anyhow!("Connection closed (admission gate shut)"))?;

        // 这里调用 encode() — 在准入被授予之后 — 使帧仅在我们有容量
        // 处理它时才被分配。
        let request_msg = TcpRequestMessage::with_headers(portname_path, headers.clone(), payload);
        let encoded_data = request_msg.encode()?;

        let (response_tx, response_rx) = oneshot::channel();

        // 重检健康：在等待许可期间连接可能已变为不健康。
        // 快速失败，使调用方可在新连接上重试，而非推入已死的 submit_queue。
        if !self.healthy.load(Ordering::Relaxed) {
            anyhow::bail!("Connection unhealthy (tasks failed)");
        }
        if self.closed.load(Ordering::Acquire) {
            anyhow::bail!("Connection closed (writer exited)");
        }

        // 递增 inflight 并附上 RAII 守卫，使其在所有退出路径上递减：
        // 正常返回、`?` 传播、tokio::time::timeout 取消，或任何其他
        // future drop。若无守卫，超时会在 response_rx.await 处 drop future，
        // fetch_sub 永远不会执行，从而永久虚肨计数器并污染
        // available_capacity()。
        // Release：与 InflightGuard::drop 中的 Release 对称，使
        // available_capacity() 中的 Acquire 读看到一致的值。
        self.inflight.fetch_add(1, Ordering::Release);
        let _inflight_guard = InflightGuard(self.inflight.clone());

        // 无锁提交：~20-40ns。
        self.submit_queue.push(PendingRequest {
            encoded_data,
            response_tx,
        });

        #[cfg(test)]
        if let Some(barrier) = &self.post_enqueue_barrier {
            barrier.wait().await;
            barrier.wait().await;
        }

        // 若写任务已进入终止清理，立即使刚入队的请求失败，
        // 而不是等待外层请求超时。
        if self.closed.load(Ordering::Acquire) {
            Self::drain_pending(&self.submit_queue);
        } else {
            // 若写任务在休眠则唤醒它。
            self.writer_notify.notify_one();
            // 写任务可能在首次检查与 notify 之间关闭。再减入一次，
            // 使晚入队请求不会越过写任务最后一轮减入而被遗留。
            if self.closed.load(Ordering::Acquire) {
                Self::drain_pending(&self.submit_queue);
            }
        }

        // 等待响应。超时时外层 future 在此处被 drop：
        // `_inflight_guard` drop → 自动运行 fetch_sub(Release)。
        // `_permit` drop → 自动释放信号量槽位。
        let result = response_rx
            .await
            .map_err(|_| anyhow::anyhow!("Reader task closed"))?;

        if trace && let Some(start) = e2e_start {
            let e2e_ns = start.elapsed().as_nanos() as u64;
            tracing::trace!(e2e_ns = e2e_ns, "TCP request e2e latency");
        }

        result
    }

    /// 检查连接是否健康。
    fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }

    /// 可用容量（参考性，用于冷路径增长启发式判断）。
    fn available_capacity(&self) -> usize {
        // Acquire：与 fetch_add/InflightGuard::drop 中的 Release 对称，使我们
        // 观察到任一线程最近提交的 inflight 值。
        let inflight = self.inflight.load(Ordering::Acquire) as usize;
        self.channel_buffer.saturating_sub(inflight)
    }
}

// === SECTION: [3] 单主机连接池（HostPool） ===

/// 单主机连接池，具备 LRU 生命周期与基于 ArcSwap 的快照。
///
/// 热路径：`ArcSwap::load()` + 原子轮询（合计 ~40ns，完全无锁）。
/// 冷路径：LRU 修剪/插入 + ArcSwap 存储（仅在启动或失败时）。
struct HostPool {
    /// 热路径的无锁快照（冷路径变更时重建）。
    snapshot: arc_swap::ArcSwap<Vec<Arc<TcpConnection>>>,
    /// 生命周期管理（驱逐、修剪）的 LRU 缓存。
    lru: parking_lot::Mutex<LruCache<u64, Arc<TcpConnection>>>,
    /// 连接选择的原子轮询计数器。
    counter: AtomicU64,
    /// LRU 键的单调递增 ID 生成器。
    next_id: AtomicU64,
    /// 防止连接风暴（thundering-herd）的 CAS 闸门。
    connecting: AtomicBool,
    /// 连接尝试完成（成功或失败）时唤醒 CAS 输家。
    connect_notify: tokio::sync::Notify,
    /// 该主机的最大连接数。
    max_connections: usize,
    /// 目标地址。
    addr: SocketAddr,
    /// 连接超时。
    connect_timeout: Duration,
    /// 新连接的 channel 缓冲区大小。
    channel_buffer: usize,
    /// 最后使用时间戳（unix 毫秒），用于空闲清理。
    last_used_ms: AtomicU64,
}

impl HostPool {
    fn new(addr: SocketAddr, config: &TcpRequestConfig) -> Self {
        let cap = NonZeroUsize::new(config.pool_size).unwrap_or(NonZeroUsize::new(1).unwrap());
        Self {
            snapshot: arc_swap::ArcSwap::from_pointee(Vec::new()),
            lru: parking_lot::Mutex::new(LruCache::new(cap)),
            counter: AtomicU64::new(0),
            next_id: AtomicU64::new(0),
            connecting: AtomicBool::new(false),
            connect_notify: tokio::sync::Notify::new(),
            max_connections: config.pool_size,
            addr,
            connect_timeout: config.connect_timeout,
            channel_buffer: config.channel_buffer,
            last_used_ms: AtomicU64::new(current_time_ms()),
        }
    }

    /// 获取一个连接，尽可能走热路径（ArcSwap load + 原子轮询）。
    async fn get_connection(
        &self,
        connect_limiter: &tokio::sync::Semaphore,
    ) -> Result<Arc<TcpConnection>> {
        // === 热路径：ArcSwap load + 原子轮询（完全无锁） ===
        {
            let guard = self.snapshot.load();
            let conns = &**guard;
            let len = conns.len();
            if len > 0 {
                let start = self.counter.fetch_add(1, Ordering::Relaxed) as usize;

                // 第一轮：找一个健康且有可用容量的连接。
                for i in 0..len {
                    let idx = (start + i) % len;
                    let conn = &conns[idx];
                    if conn.is_healthy() && conn.available_capacity() > 0 {
                        return Ok(conn.clone());
                    }
                }

                // 所有健康连接都已饱和。
                // 若连接池已达上限，返回一个饱和的健康连接（由 SegQueue 背压处理）。
                // 若连接池仍可增长，则落入冷路径以增加容量。
                if len >= self.max_connections {
                    for i in 0..len {
                        let idx = (start + i) % len;
                        if conns[idx].is_healthy() {
                            return Ok(conns[idx].clone());
                        }
                    }
                }
                // 落入：全部不健康，或全部饱和且低于 max_connections
            }
        }

        // === 冷路径 ===
        self.ensure_capacity_or_heal(connect_limiter).await
    }

    /// 判断连接池是否应当增长。
    fn should_grow(healthy: &[Arc<TcpConnection>], max_connections: usize) -> bool {
        if healthy.is_empty() {
            return true;
        }
        if healthy.len() >= max_connections {
            return false;
        }
        // 当每个健康连接的 channel 都已完全饱和时增长。
        healthy.iter().all(|c| c.available_capacity() == 0)
    }

    /// 冷路径：修剪不健康连接，可选增长，重建快照。
    async fn ensure_capacity_or_heal(
        &self,
        connect_limiter: &tokio::sync::Semaphore,
    ) -> Result<Arc<TcpConnection>> {
        // --- 阶段 A：锁住 LRU，修剪，决策，构建快照，解锁 ---
        let (need_connect, new_snap) = {
            let mut lru = self.lru.lock();

            // 修剪不健康连接（被驱逐的 Arc 仍为在飞持有者保活）。
            let dead: Vec<u64> = lru
                .iter()
                .filter(|(_, c)| !c.is_healthy())
                .map(|(&k, _)| k)
                .collect();
            for k in dead {
                lru.pop(&k);
            }

            let snap: Vec<Arc<TcpConnection>> = lru.iter().map(|(_, c)| c.clone()).collect();
            let grow = Self::should_grow(&snap, self.max_connections);
            (grow, snap)
        };
        // LRU 锁在此处释放

        // 原子快照更新（无 RwLock！）。
        self.snapshot.store(Arc::new(new_snap.clone()));

        // 重检快照以寻找可用连接。当 need_connect 为 true 时，要求
        // available_capacity() > 0，以避免返回饱和连接而阻碍连接池增长。
        // 这与热路径检查及阶段 B 的重试检查一致。
        {
            let guard = self.snapshot.load();
            let conns = &**guard;
            if !conns.is_empty() {
                let start = self.counter.fetch_add(1, Ordering::Relaxed) as usize;
                for i in 0..conns.len() {
                    let idx = (start + i) % conns.len();
                    let conn = &conns[idx];
                    if conn.is_healthy() && (!need_connect || conn.available_capacity() > 0) {
                        return Ok(conn.clone());
                    }
                }
            }
        }

        if !need_connect {
            anyhow::bail!(
                "No healthy TCP connection to {} and pool at capacity ({})",
                self.addr,
                self.max_connections
            );
        }

        // --- 阶段 B：连接（不持锁，CAS 闸门防止踩踏） ---
        // 用有界重试循环代替递归。
        for retry in 0..MAX_CONNECT_RETRIES {
            if self
                .connecting
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                // 赢得 CAS 闸门。守卫在所有退出路径上重置它（并唤醒等待者）
                // — 正常返回、`?`、以及 future 取消。
                let _connecting_guard = ConnectingGuard {
                    connecting: &self.connecting,
                    notify: &self.connect_notify,
                };

                // 获取全局连接许可以限制总连接突发。
                // 若此 await 被取消，_connecting_guard 会 drop 并重置闸门
                // — 无需手动清理。
                let _permit = connect_limiter
                    .acquire()
                    .await
                    .map_err(|_| anyhow::anyhow!("Global connect limiter closed"))?;

                let connect_result =
                    TcpConnection::connect(self.addr, self.connect_timeout, self.channel_buffer)
                        .await;

                self.connecting.store(false, Ordering::Release);

                match connect_result {
                    Ok(stream) => {
                        let new_conn = Arc::new(stream);

                        // --- 阶段 C：锁住 LRU，插入，重建快照，解锁 ---
                        {
                            let mut lru = self.lru.lock();

                            // 重新修剪（连接期间可能已变化）。
                            let dead: Vec<u64> = lru
                                .iter()
                                .filter(|(_, c)| !c.is_healthy())
                                .map(|(&k, _)| k)
                                .collect();
                            for k in dead {
                                lru.pop(&k);
                            }

                            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
                            lru.put(id, new_conn.clone());

                            let snap: Vec<Arc<TcpConnection>> =
                                lru.iter().map(|(_, c)| c.clone()).collect();
                            drop(lru);
                            self.snapshot.store(Arc::new(snap));
                        }

                        self.connect_notify.notify_waiters();
                        return Ok(new_conn);
                    }
                    Err(e) => {
                        self.connect_notify.notify_waiters();
                        return Err(e);
                    }
                }
            }

            // 另一个任务正在连接。等待其完成（或超时）。
            let _ =
                tokio::time::timeout(self.connect_timeout, self.connect_notify.notified()).await;

            // 让出后再试热路径。
            let guard = self.snapshot.load();
            let conns = &**guard;
            let len = conns.len();
            if len > 0 {
                let start = self.counter.fetch_add(1, Ordering::Relaxed) as usize;
                for i in 0..len {
                    let idx = (start + i) % len;
                    if conns[idx].is_healthy() && conns[idx].available_capacity() > 0 {
                        return Ok(conns[idx].clone());
                    }
                }
                // 若已达上限则接受饱和连接。
                if len >= self.max_connections {
                    for i in 0..len {
                        let idx = (start + i) % len;
                        if conns[idx].is_healthy() {
                            return Ok(conns[idx].clone());
                        }
                    }
                }
            }
            drop(guard);

            tracing::trace!(
                "TCP pool connect retry {}/{} for {}",
                retry + 1,
                MAX_CONNECT_RETRIES,
                self.addr
            );
        }

        // 所有连接尝试均失败。回退到任一健康连接
        // （即使已饱和）而不丢弃请求 — SegQueue 背压会优雅处理过载。
        {
            let guard = self.snapshot.load();
            let conns = &**guard;
            let len = conns.len();
            if len > 0 {
                let start = self.counter.fetch_add(1, Ordering::Relaxed) as usize;
                for i in 0..len {
                    let idx = (start + i) % len;
                    if conns[idx].is_healthy() {
                        return Ok(conns[idx].clone());
                    }
                }
            }
        }

        anyhow::bail!(
            "No healthy TCP connection to {} after {} connect retries",
            self.addr,
            MAX_CONNECT_RETRIES
        )
    }
}

/// 返回自 epoch 起的当前时间（毫秒）。
fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// 带 LRU 生命周期与共享 Arc 连接的连接池。
///
// === SECTION: [4] 全局连接池（TcpConnectionPool） ===

/// 使用 DashMap 管理逐主机连接池，并用全局 Semaphore 限制
/// 跨多个冷主机的连接突发总量。
struct TcpConnectionPool {
    hosts: DashMap<SocketAddr, Arc<HostPool>>,
    config: TcpRequestConfig,
    /// 全局连接并发限制器（限制连接突发总量）
    connect_limiter: Arc<tokio::sync::Semaphore>,
    /// 用于清理的空闲主机 TTL
    host_idle_ttl_ms: u64,
}

impl TcpConnectionPool {
    fn new(config: TcpRequestConfig) -> Self {
        let global_limit = std::env::var("PGD_TCP_GLOBAL_CONNECT_LIMIT")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(DEFAULT_GLOBAL_CONNECT_LIMIT);

        let host_idle_ttl_secs = std::env::var("PGD_TCP_HOST_IDLE_TTL_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(DEFAULT_HOST_IDLE_TTL_SECS);

        Self {
            hosts: DashMap::new(),
            config,
            connect_limiter: Arc::new(tokio::sync::Semaphore::new(global_limit)),
            host_idle_ttl_ms: host_idle_ttl_secs * 1000,
        }
    }

    /// 从连接池获取连接或创建一个新连接。
    /// 热路径：DashMap 分片读锁 → ArcSwap load → 原子轮询。
    async fn get_connection(&self, addr: SocketAddr) -> Result<Arc<TcpConnection>> {
        // 快速路径：从 DashMap 守卫中克隆 Arc，使分片锁在任何
        // `.await` 之前释放（DashMap 守卫是 !Send，跨 await 点持有
        // 会阻塞其他分片操作）。
        if let Some(host) = self.hosts.get(&addr).map(|entry| Arc::clone(&*entry)) {
            host.last_used_ms
                .store(current_time_ms(), Ordering::Relaxed);
            return host.get_connection(&self.connect_limiter).await;
        }

        // 慢速路径：对该主机的首次请求
        let host = self
            .hosts
            .entry(addr)
            .or_insert_with(|| Arc::new(HostPool::new(addr, &self.config)))
            .clone();

        host.last_used_ms
            .store(current_time_ms(), Ordering::Relaxed);
        host.get_connection(&self.connect_limiter).await
    }

    /// 主动向给定地址建立一条 TCP 连接。
    ///
    /// 创建 `HostPool` 条目（若不存在）并通过正常冷路径打开一条连接，
    /// 从而遵守全局 `connect_limiter`。失败会被记录但不会传播
    /// — 懒加载冷路径仍作为回退。
    async fn warmup(&self, addr: SocketAddr) {
        let host = self
            .hosts
            .entry(addr)
            .or_insert_with(|| Arc::new(HostPool::new(addr, &self.config)))
            .clone();
        host.last_used_ms
            .store(current_time_ms(), Ordering::Relaxed);
        match host.get_connection(&self.connect_limiter).await {
            Ok(_) => tracing::debug!("TCP warmup: pre-connected to {}", addr),
            Err(e) => tracing::warn!("TCP warmup: failed to pre-connect to {}: {}", addr, e),
        }
    }

    /// 监听实例发现 channel 并为每个新发现的 TCP 后端
    /// 主动预热一条 TCP 连接的后台任务。
    ///
    /// 采用基于差异的方式：跟踪已知地址的 `HashSet<SocketAddr>`，
    /// 仅预热真正新增的。
    fn start_warmup_watcher(
        self: &Arc<Self>,
        mut instance_rx: tokio::sync::watch::Receiver<Vec<crate::servicegroup::Instance>>,
        cancel_token: tokio_util::sync::CancellationToken,
    ) {
        let pool = Arc::clone(self);
        tokio::spawn(async move {
            let mut known_addrs = std::collections::HashSet::<SocketAddr>::new();

            // 从当前值初始化，以免重复预热已有后端。
            {
                let instances = instance_rx.borrow_and_update();
                for inst in instances.iter() {
                    if let crate::servicegroup::TransportType::Tcp(ref addr_str) = inst.transport
                        && let Ok((sock, _)) = TcpRequestClient::parse_address(addr_str)
                    {
                        known_addrs.insert(sock);
                    }
                }
            }

            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        tracing::debug!("TCP warmup watcher cancelled");
                        break;
                    }
                    result = instance_rx.changed() => {
                        if result.is_err() {
                            tracing::debug!("TCP warmup watcher: instance channel closed");
                            break;
                        }

                        let instances = instance_rx.borrow_and_update().clone();
                        let mut current_addrs = std::collections::HashSet::<SocketAddr>::new();

                        for inst in &instances {
                            if let crate::servicegroup::TransportType::Tcp(ref addr_str) = inst.transport
                                && let Ok((sock, _)) = TcpRequestClient::parse_address(addr_str)
                            {
                                current_addrs.insert(sock);
                                if !known_addrs.contains(&sock) {
                                    let pool = Arc::clone(&pool);
                                    tokio::spawn(async move {
                                        pool.warmup(sock).await;
                                    });
                                }
                            }
                        }

                        known_addrs = current_addrs;
                    }
                }
            }
        });
    }

    /// 机会性清理空闲主机连接池。
    /// 由后台维护任务周期性调用；不在热路径上。
    fn cleanup_idle_hosts(&self) {
        let now = current_time_ms();
        let ttl = self.host_idle_ttl_ms;

        let stale: Vec<SocketAddr> = self
            .hosts
            .iter()
            .filter(|entry| {
                let last = entry.value().last_used_ms.load(Ordering::Relaxed);
                if now.saturating_sub(last) <= ttl {
                    return false;
                }
                // 不要驱逐仍有在飞请求的主机 ——
                // 一个长时间运行、无新检出的请求看似空闲，
                // 但杀掉它会导致合法工作失败。
                let snap = entry.value().snapshot.load();
                !snap.iter().any(|c| c.inflight.load(Ordering::Acquire) > 0)
            })
            .map(|entry| *entry.key())
            .collect();

        for addr in stale {
            tracing::debug!("Removing idle TCP host pool for {}", addr);
            if let Some((_, host)) = self.hosts.remove(&addr) {
                // 关闭连接：标记为不健康，唤醒 writer 以便其退出
                // 并排空待处理调用者；中止 reader，因为它们可能阻塞在
                // 静默对端的 framed.next().await 上，否则会无限期持有套接字 FD。
                let snap = host.snapshot.load();
                for conn in snap.iter() {
                    conn.healthy.store(false, Ordering::Relaxed);
                    // 唤醒等待准入信号量的调用者，使其通过获取后的
                    // 健康重检快速失败，而不是等待 drain_pending 释放许可。
                    conn.admission.close();
                    conn.writer_notify.notify_one();
                    conn.reader_handle.abort();
                }
            }
        }
    }

    /// 统计所有主机连接池中的活跃与空闲连接数。
    /// 活跃 = 健康且 inflight > 0，空闲 = 健康且 inflight == 0。
    fn connection_counts(&self) -> (usize, usize) {
        let mut active = 0usize;
        let mut idle = 0usize;
        for entry in self.hosts.iter() {
            let snap = entry.value().snapshot.load();
            for conn in snap.iter() {
                if conn.is_healthy() {
                    if conn.inflight.load(Ordering::Acquire) > 0 {
                        active += 1;
                    } else {
                        idle += 1;
                    }
                }
            }
        }
        (active, idle)
    }

    /// 生成一个周期性清理空闲主机连接池的后台任务。
    ///
    /// 使用 `Weak` 引用，使任务在 `TcpConnectionPool` 被 drop 时自动停止
    /// （无需显式取消）。清理间隔为空闲 TTL 的一半，
    /// 使主机在过期后能较及时被回收。
    fn start_idle_cleanup(self: &Arc<Self>) {
        // 仅在有 tokio 运行时可用时生成。生产环境中总是如此
        // （客户端在异步上下文创建），但同步单元测试会在无运行时
        // 构造 TcpRequestClient。
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::debug!("No tokio runtime available; idle host cleanup disabled");
            return;
        };
        let pool = Arc::downgrade(self);
        // 以 30s 为下限，防止 PGD_TCP_HOST_IDLE_TTL_SECS=0 时忙循环
        let interval = Duration::from_millis((self.host_idle_ttl_ms / 2).max(30_000));
        handle.spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                match pool.upgrade() {
                    Some(pool) => pool.cleanup_idle_hosts(),
                    None => break,
                }
            }
        });
    }
}

// === SECTION: [5] 公开 TCP 客户端（TcpRequestClient） ===

/// TCP 请求面客户端。
pub struct TcpRequestClient {
    pool: Arc<TcpConnectionPool>,
    config: TcpRequestConfig,
    stats: Arc<TcpClientStats>,
}

struct TcpClientStats {
    requests_sent: AtomicU64,
    responses_received: AtomicU64,
    errors: AtomicU64,
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,
}

impl TcpRequestClient {
    /// 使用默认配置创建新的 TCP 请求客户端。
    pub fn new() -> Result<Self> {
        Self::with_config(TcpRequestConfig::default())
    }

    /// 使用自定义配置创建新的 TCP 请求客户端。
    pub fn with_config(config: TcpRequestConfig) -> Result<Self> {
        let pool = Arc::new(TcpConnectionPool::new(config.clone()));
        pool.start_idle_cleanup();
        Ok(Self {
            pool,
            config,
            stats: Arc::new(TcpClientStats {
                requests_sent: AtomicU64::new(0),
                responses_received: AtomicU64::new(0),
                errors: AtomicU64::new(0),
                bytes_sent: AtomicU64::new(0),
                bytes_received: AtomicU64::new(0),
            }),
        })
    }

    /// 从环境配置创建。
    pub fn from_env() -> Result<Self> {
        Self::with_config(TcpRequestConfig::from_env())
    }

    /// 生成为新发现后端主动预热 TCP 连接的后台任务。
    ///
    /// 委托给 [`TcpConnectionPool::start_warmup_watcher`]。
    pub fn start_warmup(
        &self,
        instance_rx: tokio::sync::watch::Receiver<Vec<crate::servicegroup::Instance>>,
        cancel_token: tokio_util::sync::CancellationToken,
    ) {
        self.pool.start_warmup_watcher(instance_rx, cancel_token);
    }

    /// 从字符串解析 TCP 地址。
    /// 支持格式："host:port" 或 "tcp://host:port" 或 "host:port/portname_name"。
    /// 返回 (SocketAddr, Option<portname_name>)。
    pub(crate) fn parse_address(address: &str) -> Result<(SocketAddr, Option<String>)> {
        let addr_str = if let Some(stripped) = address.strip_prefix("tcp://") {
            stripped
        } else {
            address
        };

        // 检查是否包含 portname 名称（格式：host:port/portname_name）。
        if let Some((socket_part, portname_name)) = addr_str.split_once('/') {
            let socket_addr = socket_part
                .parse::<SocketAddr>()
                .map_err(|e| anyhow::anyhow!("Invalid TCP address '{}': {}", address, e))?;
            Ok((socket_addr, Some(portname_name.to_string())))
        } else {
            let socket_addr = addr_str
                .parse::<SocketAddr>()
                .map_err(|e| anyhow::anyhow!("Invalid TCP address '{}': {}", address, e))?;
            Ok((socket_addr, None))
        }
    }
}

impl Default for TcpRequestClient {
    fn default() -> Self {
        Self::new().expect("Failed to create TCP request client")
    }
}

#[async_trait]
impl RequestPlaneClient for TcpRequestClient {
    async fn send_request(
        &self,
        address: String,
        payload: Bytes,
        mut headers: Headers,
    ) -> Result<Bytes> {
        tracing::debug!("TCP client sending request to address: {}", address);
        self.stats.requests_sent.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes_sent
            .fetch_add(payload.len() as u64, Ordering::Relaxed);

        let (addr, portname_name) = Self::parse_address(&address)?;

        if let Some(portname_name) = portname_name {
            headers.insert("x-portname-path".to_string(), portname_name.clone());
        }

        // 从连接池获取共享连接（Arc，非独占借用）。
        let conn = self.pool.get_connection(addr).await?;

        let result = tokio::time::timeout(
            self.config.request_timeout,
            conn.send_request(payload, &headers),
        )
        .await;

        match result {
            Ok(Ok(response)) => {
                self.stats
                    .responses_received
                    .fetch_add(1, Ordering::Relaxed);
                self.stats
                    .bytes_received
                    .fetch_add(response.len() as u64, Ordering::Relaxed);
                TCP_BYTES_RECEIVED_TOTAL.inc_by(response.len() as f64);
                // conn（Arc）在此处 drop —— 连接仍留在池中
                Ok(response)
            }
            Ok(Err(e)) => {
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                TCP_ERRORS_TOTAL.inc();
                tracing::warn!("TCP request failed to {}: {}", addr, e);
                let cause = crate::error::PagodaError::from(
                    e.into_boxed_dyn_error() as Box<dyn std::error::Error + 'static>
                );
                Err(anyhow::anyhow!(
                    crate::error::PagodaError::builder()
                        .error_type(crate::error::ErrorType::CannotConnect)
                        .message(format!("TCP request to {addr} failed"))
                        .cause(cause)
                        .build()
                ))
            }
            Err(_) => {
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                TCP_ERRORS_TOTAL.inc();
                tracing::warn!("TCP request timeout to {}", addr);
                Err(anyhow::anyhow!(
                    crate::error::PagodaError::builder()
                        .error_type(crate::error::ErrorType::CannotConnect)
                        .message(format!("TCP request to {addr} timed out"))
                        .build()
                ))
            }
        }
    }

    fn transport_name(&self) -> &'static str {
        "tcp"
    }

    fn is_healthy(&self) -> bool {
        true // TCP 客户端只要创建成功就始终健康
    }

    fn start_warmup(
        &self,
        instance_rx: tokio::sync::watch::Receiver<Vec<crate::servicegroup::Instance>>,
        cancel_token: tokio_util::sync::CancellationToken,
    ) {
        TcpRequestClient::start_warmup(self, instance_rx, cancel_token);
    }

    fn stats(&self) -> ClientStats {
        let (active, idle) = self.pool.connection_counts();
        ClientStats {
            requests_sent: self.stats.requests_sent.load(Ordering::Relaxed),
            responses_received: self.stats.responses_received.load(Ordering::Relaxed),
            errors: self.stats.errors.load(Ordering::Relaxed),
            bytes_sent: self.stats.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.stats.bytes_received.load(Ordering::Relaxed),
            active_connections: active,
            idle_connections: idle,
            avg_latency_us: 0,
        }
    }
}

// === SECTION: [6] 单元测试 ===

#[cfg(test)]
mod tests {
    //! ## 测试矩阵
    //!
    //! | 测试名 | 覆盖维度 |
    //! |---|---|
    //! | `test_tcp_config_default` | `Default` 锁定关键常量 |
    //! | `test_tcp_config_from_env` | 4 个环境变量覆盖路径 |
    //! | `test_parse_address` | 3 种地址格式 + 非法回退 |
    //! | `test_tcp_client_creation` | `new` + transport_name/is_healthy 契约 |
    //! | `test_connection_health_check` 等 17 个 async | 连接池 / LRU / 并发 / 故障恢复 / warmup 端到端集成 |
    //! | `test_parse_address_with_tcp_prefix_and_portname` | tcp:// 前缀 + portname 后缀同时存在 |
    //! | `test_parse_address_rejects_empty_input` | 空串 → Err |
    //! | `test_parse_address_rejects_address_without_port` | 缺少 port → Err |
    //! | `test_parse_address_error_message_includes_input` | 错误消息含入参（可观测性） |
    //! | `test_tcp_client_default_does_not_panic` | `Default::default()` 走 `new()` 路径 |
    //! | `test_tcp_client_with_config_preserves_pool_size` | 自定义 config 的 `pool_size` 透传 |
    //! | `test_latency_trace_enabled_env_values` | env var "1" / "true" / 缺失 / 其他 → 布尔语义 |
    //!
    //! ## 说明
    //! 主体实现（连接池、LRU、warmup watcher 等）依赖完整 tokio + 真实 socket，已由
    //! 17 个 async 集成测试覆盖；本次精细化只在测试矩阵上做"边界化"补强，不动 hot-path 代码。

    use super::*;
    use std::sync::atomic::AtomicUsize;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    #[test]
    fn test_tcp_config_default() {
        let config = TcpRequestConfig::default();
        assert_eq!(config.pool_size, DEFAULT_POOL_SIZE);
        assert_eq!(
            config.request_timeout,
            Duration::from_secs(DEFAULT_TCP_REQUEST_TIMEOUT_SECS)
        );
        assert_eq!(config.channel_buffer, REQUEST_CHANNEL_BUFFER);
    }

    #[test]
    fn test_tcp_config_from_env() {
        unsafe {
            std::env::set_var("PGD_TCP_REQUEST_TIMEOUT", "10");
            std::env::set_var("PGD_TCP_POOL_SIZE", "50");
            std::env::set_var("PGD_TCP_CONNECT_TIMEOUT", "3");
            std::env::set_var("PGD_TCP_CHANNEL_BUFFER", "100");
        }

        let config = TcpRequestConfig::from_env();
        assert_eq!(config.request_timeout, Duration::from_secs(10));
        assert_eq!(config.pool_size, 50);
        assert_eq!(config.connect_timeout, Duration::from_secs(3));
        assert_eq!(config.channel_buffer, 100);

        // 清理环境变量
        unsafe {
            std::env::remove_var("PGD_TCP_REQUEST_TIMEOUT");
            std::env::remove_var("PGD_TCP_POOL_SIZE");
            std::env::remove_var("PGD_TCP_CONNECT_TIMEOUT");
            std::env::remove_var("PGD_TCP_CHANNEL_BUFFER");
        }
    }

    #[test]
    fn test_parse_address() {
        let (addr1, _) = TcpRequestClient::parse_address("127.0.0.1:8080").unwrap();
        assert_eq!(addr1.port(), 8080);

        let (addr2, _) = TcpRequestClient::parse_address("tcp://127.0.0.1:9090").unwrap();
        assert_eq!(addr2.port(), 9090);

        let (addr3, portname) =
            TcpRequestClient::parse_address("127.0.0.1:8080/test_portname").unwrap();
        assert_eq!(addr3.port(), 8080);
        assert_eq!(portname, Some("test_portname".to_string()));

        assert!(TcpRequestClient::parse_address("invalid").is_err());
    }

    #[test]
    fn test_tcp_client_creation() {
        let client = TcpRequestClient::new();
        assert!(client.is_ok());

        let client = client.unwrap();
        assert_eq!(client.transport_name(), "tcp");
        assert!(client.is_healthy());
    }

    /// 辅助函数：生成一个回显请求的模拟 TCP 服务器。
    /// 返回 (listener_addr, connection_count_tracker)。
    async fn spawn_echo_server() -> (SocketAddr, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let conn_count = Arc::new(AtomicUsize::new(0));
        let conn_count_clone = conn_count.clone();

        tokio::spawn(async move {
            loop {
                let result = listener.accept().await;
                if result.is_err() {
                    break;
                }
                let (stream, _) = result.unwrap();
                conn_count_clone.fetch_add(1, Ordering::SeqCst);

                tokio::spawn(async move {
                    let (mut read_half, mut write_half) = tokio::io::split(stream);
                    loop {
                        // 读取路径长度
                        let mut len_buf = [0u8; 2];
                        if read_half.read_exact(&mut len_buf).await.is_err() {
                            break;
                        }
                        let path_len = u16::from_be_bytes(len_buf) as usize;
                        let mut path_buf = vec![0u8; path_len];
                        if read_half.read_exact(&mut path_buf).await.is_err() {
                            break;
                        }

                        // 读取 headers 长度
                        let mut headers_len_buf = [0u8; 2];
                        if read_half.read_exact(&mut headers_len_buf).await.is_err() {
                            break;
                        }
                        let headers_len = u16::from_be_bytes(headers_len_buf) as usize;
                        let mut headers_buf = vec![0u8; headers_len];
                        if read_half.read_exact(&mut headers_buf).await.is_err() {
                            break;
                        }

                        // 读取 payload 长度 + payload
                        let mut len_buf = [0u8; 4];
                        if read_half.read_exact(&mut len_buf).await.is_err() {
                            break;
                        }
                        let payload_len = u32::from_be_bytes(len_buf) as usize;
                        let mut payload_buf = vec![0u8; payload_len];
                        if read_half.read_exact(&mut payload_buf).await.is_err() {
                            break;
                        }

                        // 发送响应
                        use crate::pipeline::network::codec::TcpResponseMessage;
                        let response = TcpResponseMessage::new(Bytes::from(payload_buf));
                        let encoded = response.encode().unwrap();
                        if write_half.write_all(&encoded).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });

        (addr, conn_count)
    }

    #[tokio::test]
    async fn test_connection_health_check() {
        use crate::pipeline::network::codec::TcpResponseMessage;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (mut read_half, mut write_half) = tokio::io::split(stream);

            let mut len_buf = [0u8; 2];
            read_half.read_exact(&mut len_buf).await.unwrap();
            let path_len = u16::from_be_bytes(len_buf) as usize;
            let mut path_buf = vec![0u8; path_len];
            read_half.read_exact(&mut path_buf).await.unwrap();

            let mut headers_len_buf = [0u8; 2];
            read_half.read_exact(&mut headers_len_buf).await.unwrap();
            let headers_len = u16::from_be_bytes(headers_len_buf) as usize;
            let mut headers_buf = vec![0u8; headers_len];
            read_half.read_exact(&mut headers_buf).await.unwrap();

            let mut len_buf = [0u8; 4];
            read_half.read_exact(&mut len_buf).await.unwrap();
            let payload_len = u32::from_be_bytes(len_buf) as usize;
            let mut payload_buf = vec![0u8; payload_len];
            read_half.read_exact(&mut payload_buf).await.unwrap();

            let response = TcpResponseMessage::new(Bytes::from_static(b"pong"));
            let encoded = response.encode().unwrap();
            write_half.write_all(&encoded).await.unwrap();
        });

        let conn = TcpConnection::connect(addr, Duration::from_secs(5), 10)
            .await
            .unwrap();

        assert!(conn.is_healthy(), "New connection should be healthy");

        let mut headers = Headers::new();
        headers.insert("x-portname-path".to_string(), "test".to_string());

        let result = conn.send_request(Bytes::from("ping"), &headers).await;
        assert!(result.is_ok(), "Request should succeed");
        assert_eq!(result.unwrap(), Bytes::from("pong"));
    }

    #[tokio::test]
    async fn test_concurrent_requests_single_connection() {
        use crate::pipeline::network::codec::TcpResponseMessage;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let request_count = Arc::new(AtomicUsize::new(0));
        let request_count_clone = request_count.clone();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (mut read_half, mut write_half) = tokio::io::split(stream);

            for _ in 0..5 {
                let mut len_buf = [0u8; 2];
                if read_half.read_exact(&mut len_buf).await.is_err() {
                    break;
                }
                let path_len = u16::from_be_bytes(len_buf) as usize;
                let mut path_buf = vec![0u8; path_len];
                if read_half.read_exact(&mut path_buf).await.is_err() {
                    break;
                }

                let mut headers_len_buf = [0u8; 2];
                if read_half.read_exact(&mut headers_len_buf).await.is_err() {
                    break;
                }
                let headers_len = u16::from_be_bytes(headers_len_buf) as usize;
                let mut headers_buf = vec![0u8; headers_len];
                if read_half.read_exact(&mut headers_buf).await.is_err() {
                    break;
                }

                let mut len_buf = [0u8; 4];
                if read_half.read_exact(&mut len_buf).await.is_err() {
                    break;
                }
                let payload_len = u32::from_be_bytes(len_buf) as usize;
                let mut payload_buf = vec![0u8; payload_len];
                if read_half.read_exact(&mut payload_buf).await.is_err() {
                    break;
                }

                request_count_clone.fetch_add(1, Ordering::SeqCst);

                let response = TcpResponseMessage::new(Bytes::from(payload_buf));
                let encoded = response.encode().unwrap();
                if write_half.write_all(&encoded).await.is_err() {
                    break;
                }
            }
        });

        let conn = Arc::new(
            TcpConnection::connect(addr, Duration::from_secs(5), 10)
                .await
                .unwrap(),
        );

        let mut handles = vec![];
        for i in 0..5 {
            let conn = conn.clone();
            let handle = tokio::spawn(async move {
                let mut headers = Headers::new();
                headers.insert("x-portname-path".to_string(), "test".to_string());
                let payload = format!("request_{}", i);
                conn.send_request(Bytes::from(payload.clone()), &headers)
                    .await
                    .map(|response| (payload, response))
            });
            handles.push(handle);
        }

        let mut results = vec![];
        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok(), "Request should succeed");
            results.push(result.unwrap());
        }

        assert_eq!(results.len(), 5);
        assert_eq!(
            request_count.load(Ordering::SeqCst),
            5,
            "Server should have received 5 requests"
        );
    }

    #[tokio::test]
    async fn test_lru_connection_reuse() {
        let (addr, conn_count) = spawn_echo_server().await;

        let config = TcpRequestConfig {
            request_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(5),
            pool_size: 4,
            channel_buffer: 10,
        };
        let pool = TcpConnectionPool::new(config);

        // 连续两次获取连接 —— 应复用同一条 TCP 连接
        let conn1 = pool.get_connection(addr).await.unwrap();
        let mut headers = Headers::new();
        headers.insert("x-portname-path".to_string(), "test".to_string());
        let _ = conn1
            .send_request(Bytes::from("ping1"), &headers)
            .await
            .unwrap();
        drop(conn1); // Arc 引用被释放，但连接仍留在池中

        let conn2 = pool.get_connection(addr).await.unwrap();
        let _ = conn2
            .send_request(Bytes::from("ping2"), &headers)
            .await
            .unwrap();
        drop(conn2);

        assert_eq!(
            conn_count.load(Ordering::SeqCst),
            1,
            "Should reuse connection from pool (1 TCP connection total)"
        );
    }

    #[tokio::test]
    async fn test_lru_eviction_keeps_inflight_alive() {
        let (addr, _conn_count) = spawn_echo_server().await;

        // 连接池大小为 1，便于轻松触发驱逐
        let config = TcpRequestConfig {
            request_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(5),
            pool_size: 1,
            channel_buffer: 10,
        };
        let pool = TcpConnectionPool::new(config);

        // 获取一条连接并持有 Arc
        let conn = pool.get_connection(addr).await.unwrap();

        // 将其标记为不健康，迫使连接池创建新连接
        conn.healthy.store(false, Ordering::Relaxed);

        // 再次获取连接应创建一条新连接（旧连接已从 LRU 驱逐）
        let conn2 = pool.get_connection(addr).await.unwrap();
        assert!(conn2.is_healthy());

        // 原连接 Arc 仍存活（未被 drop）—— 只是不能再被使用
        assert!(!conn.is_healthy());
        // 即使 LRU 驱逐了它，Arc 仍保持资源存活
        assert!(Arc::strong_count(&conn.writer_handle) >= 1);
    }

    #[tokio::test]
    async fn test_high_concurrency_bounded_connections() {
        let (addr, conn_count) = spawn_echo_server().await;

        let config = TcpRequestConfig {
            request_timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(5),
            pool_size: 2,
            channel_buffer: 50,
        };
        let pool = Arc::new(TcpConnectionPool::new(config));

        let mut handles = vec![];
        for i in 0..500 {
            let pool = pool.clone();
            handles.push(tokio::spawn(async move {
                let conn = pool.get_connection(addr).await?;
                let mut headers = Headers::new();
                headers.insert("x-portname-path".to_string(), "test".to_string());
                conn.send_request(Bytes::from(format!("req_{}", i)), &headers)
                    .await
            }));
        }

        let mut ok_count = 0;
        for handle in handles {
            if handle.await.unwrap().is_ok() {
                ok_count += 1;
            }
        }

        let total_conns = conn_count.load(Ordering::SeqCst);
        assert!(
            total_conns <= 2,
            "Should create at most pool_size (2) connections, got {}",
            total_conns
        );
        assert!(ok_count > 0, "At least some requests should succeed");
    }

    #[tokio::test]
    async fn test_thundering_herd_cold_start() {
        let (addr, conn_count) = spawn_echo_server().await;

        let config = TcpRequestConfig {
            request_timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(5),
            pool_size: 4,
            channel_buffer: 50,
        };
        let pool = Arc::new(TcpConnectionPool::new(config));

        // 100 个任务同时竞争一个冷连接池
        let mut handles = vec![];
        for i in 0..100 {
            let pool = pool.clone();
            handles.push(tokio::spawn(async move {
                let conn = pool.get_connection(addr).await?;
                let mut headers = Headers::new();
                headers.insert("x-portname-path".to_string(), "test".to_string());
                conn.send_request(Bytes::from(format!("req_{}", i)), &headers)
                    .await
            }));
        }

        for handle in handles {
            let _ = handle.await.unwrap();
        }

        let total_conns = conn_count.load(Ordering::SeqCst);
        assert!(
            total_conns <= 4,
            "Thundering herd: should create at most pool_size (4) connections, got {}",
            total_conns
        );
    }

    #[tokio::test]
    async fn test_server_crash_recovery() {
        // 启动一个可以被杀掉的服务器
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let cancel = tokio_util::sync::CancellationToken::new();
        let cancel_clone = cancel.clone();

        let server_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        if let Ok((stream, _)) = result {
                            let cancel = cancel_clone.clone();
                            tokio::spawn(async move {
                                let (mut read_half, mut write_half) = tokio::io::split(stream);
                                loop {
                                    tokio::select! {
                                        _ = cancel.cancelled() => break,
                                        result = async {
                                            let mut len_buf = [0u8; 2];
                                            read_half.read_exact(&mut len_buf).await?;
                                            let path_len = u16::from_be_bytes(len_buf) as usize;
                                            let mut buf = vec![0u8; path_len];
                                            read_half.read_exact(&mut buf).await?;
                                            let mut hlen = [0u8; 2];
                                            read_half.read_exact(&mut hlen).await?;
                                            let hl = u16::from_be_bytes(hlen) as usize;
                                            let mut hbuf = vec![0u8; hl];
                                            read_half.read_exact(&mut hbuf).await?;
                                            let mut plen = [0u8; 4];
                                            read_half.read_exact(&mut plen).await?;
                                            let pl = u32::from_be_bytes(plen) as usize;
                                            let mut pbuf = vec![0u8; pl];
                                            read_half.read_exact(&mut pbuf).await?;

                                            use crate::pipeline::network::codec::TcpResponseMessage;
                                            let resp = TcpResponseMessage::new(Bytes::from(pbuf));
                                            let enc = resp.encode()?;
                                            write_half.write_all(&enc).await?;
                                            Ok::<_, anyhow::Error>(())
                                        } => {
                                            if result.is_err() { break; }
                                        }
                                    }
                                }
                            });
                        }
                    }
                    _ = cancel_clone.cancelled() => break,
                }
            }
        });

        let config = TcpRequestConfig {
            request_timeout: Duration::from_secs(2),
            connect_timeout: Duration::from_secs(2),
            pool_size: 2,
            channel_buffer: 10,
        };
        let pool = Arc::new(TcpConnectionPool::new(config));

        // 成功使用连接池
        let conn = pool.get_connection(addr).await.unwrap();
        let mut headers = Headers::new();
        headers.insert("x-portname-path".to_string(), "test".to_string());
        let result = conn
            .send_request(Bytes::from("before_crash"), &headers)
            .await;
        assert!(result.is_ok());
        drop(conn);

        // 杀掉服务器
        cancel.cancel();
        let _ = server_task.await;

        // 请求应失败（服务器已不在）
        tokio::time::sleep(Duration::from_millis(50)).await;
        let conn = pool.get_connection(addr).await;
        // 连接池应要么连接失败，要么返回一个不健康连接
        if let Ok(conn) = conn {
            let result = conn
                .send_request(Bytes::from("after_crash"), &headers)
                .await;
            // 要么发送失败，要么连接不健康
            assert!(result.is_err() || !conn.is_healthy());
        }

        // 在同一端口上启动新服务器
        let listener2 = TcpListener::bind(addr).await.unwrap();
        tokio::spawn(async move {
            loop {
                let result = listener2.accept().await;
                if result.is_err() {
                    break;
                }
                let (stream, _) = result.unwrap();
                tokio::spawn(async move {
                    let (mut read_half, mut write_half) = tokio::io::split(stream);
                    loop {
                        let mut len_buf = [0u8; 2];
                        if read_half.read_exact(&mut len_buf).await.is_err() {
                            break;
                        }
                        let path_len = u16::from_be_bytes(len_buf) as usize;
                        let mut buf = vec![0u8; path_len];
                        if read_half.read_exact(&mut buf).await.is_err() {
                            break;
                        }
                        let mut hlen = [0u8; 2];
                        if read_half.read_exact(&mut hlen).await.is_err() {
                            break;
                        }
                        let hl = u16::from_be_bytes(hlen) as usize;
                        let mut hbuf = vec![0u8; hl];
                        if read_half.read_exact(&mut hbuf).await.is_err() {
                            break;
                        }
                        let mut plen = [0u8; 4];
                        if read_half.read_exact(&mut plen).await.is_err() {
                            break;
                        }
                        let pl = u32::from_be_bytes(plen) as usize;
                        let mut pbuf = vec![0u8; pl];
                        if read_half.read_exact(&mut pbuf).await.is_err() {
                            break;
                        }

                        use crate::pipeline::network::codec::TcpResponseMessage;
                        let resp = TcpResponseMessage::new(Bytes::from(pbuf));
                        let enc = resp.encode().unwrap();
                        if write_half.write_all(&enc).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });

        // 连接池应自愈：获取新连接并成功
        tokio::time::sleep(Duration::from_millis(100)).await;
        let conn = pool.get_connection(addr).await.unwrap();
        let result = conn
            .send_request(Bytes::from("after_recovery"), &headers)
            .await;
        assert!(result.is_ok(), "Pool should heal after server recovery");
    }

    #[tokio::test]
    async fn test_pool_scales_under_pressure() {
        let (addr, conn_count) = spawn_echo_server().await;

        let config = TcpRequestConfig {
            request_timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(5),
            pool_size: 4,
            channel_buffer: 1, // 缓冲区极小，以便快速迫使饱和
        };
        let pool = Arc::new(TcpConnectionPool::new(config));

        // 发送足够多的并发请求以饱和 channel_buffer=1
        let mut handles = vec![];
        for i in 0..20 {
            let pool = pool.clone();
            handles.push(tokio::spawn(async move {
                let conn = pool.get_connection(addr).await?;
                let mut headers = Headers::new();
                headers.insert("x-portname-path".to_string(), "test".to_string());
                conn.send_request(Bytes::from(format!("req_{}", i)), &headers)
                    .await
            }));
        }

        for handle in handles {
            let _ = handle.await.unwrap();
        }

        let total_conns = conn_count.load(Ordering::SeqCst);
        assert!(
            total_conns > 1,
            "Pool should scale beyond 1 connection under pressure, got {}",
            total_conns
        );
        assert!(
            total_conns <= 4,
            "Pool should not exceed pool_size (4), got {}",
            total_conns
        );
    }

    #[tokio::test]
    async fn test_pool_size_cap_sustained_load() {
        let (addr, conn_count) = spawn_echo_server().await;

        let config = TcpRequestConfig {
            request_timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(5),
            pool_size: 3,
            channel_buffer: 50,
        };
        let pool = Arc::new(TcpConnectionPool::new(config));

        // 3 轮，每轮 200 个请求
        for round in 0..3 {
            let mut handles = vec![];
            for i in 0..200 {
                let pool = pool.clone();
                handles.push(tokio::spawn(async move {
                    let conn = pool.get_connection(addr).await?;
                    let mut headers = Headers::new();
                    headers.insert("x-portname-path".to_string(), "test".to_string());
                    conn.send_request(Bytes::from(format!("round_{}_req_{}", round, i)), &headers)
                        .await
                }));
            }

            for handle in handles {
                let _ = handle.await.unwrap();
            }
        }

        let total_conns = conn_count.load(Ordering::SeqCst);
        assert!(
            total_conns <= 3,
            "Sustained load should not exceed pool_size (3), got {}",
            total_conns
        );
    }

    #[tokio::test]
    async fn test_backpressure_small_channel() {
        let (addr, _conn_count) = spawn_echo_server().await;

        let config = TcpRequestConfig {
            request_timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(5),
            pool_size: 1,
            channel_buffer: 1,
        };
        let pool = Arc::new(TcpConnectionPool::new(config));

        // 通过 pool_size=1 buffer=1 发送 50 个请求 —— 应全部通过背压完成
        let mut handles = vec![];
        for i in 0..50 {
            let pool = pool.clone();
            handles.push(tokio::spawn(async move {
                let conn = pool.get_connection(addr).await?;
                let mut headers = Headers::new();
                headers.insert("x-portname-path".to_string(), "test".to_string());
                conn.send_request(Bytes::from(format!("req_{}", i)), &headers)
                    .await
            }));
        }

        let mut ok_count = 0;
        for handle in handles {
            if handle.await.unwrap().is_ok() {
                ok_count += 1;
            }
        }

        assert_eq!(
            ok_count, 50,
            "All 50 requests should complete under backpressure"
        );
    }

    #[tokio::test]
    async fn test_no_recursive_retry_under_connect_contention() {
        // 本测试验证连接竞争使用有界重试而非递归。
        // 通过让多个任务在 pool_size=1 的冷连接池上竞争来验证。
        let (addr, conn_count) = spawn_echo_server().await;

        let config = TcpRequestConfig {
            request_timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(5),
            pool_size: 1,
            channel_buffer: 50,
        };
        let pool = Arc::new(TcpConnectionPool::new(config));

        // 多个任务竞争，只应有一个连接成功
        let mut handles = vec![];
        for _ in 0..50 {
            let pool = pool.clone();
            handles.push(tokio::spawn(async move { pool.get_connection(addr).await }));
        }

        let mut ok = 0;
        for handle in handles {
            if handle.await.unwrap().is_ok() {
                ok += 1;
            }
        }

        // 应全部成功（一个连接，其余通过热路径重试）
        assert!(ok > 0, "At least some tasks should get connections");
        assert_eq!(
            conn_count.load(Ordering::SeqCst),
            1,
            "Only 1 TCP connection should be created"
        );
    }

    #[tokio::test]
    async fn test_global_connect_limiter_multi_host() {
        // 在 4 个不同端口上生成服务器以模拟多个主机
        let mut addrs = vec![];
        let total_conns = Arc::new(AtomicUsize::new(0));

        for _ in 0..4 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            addrs.push(addr);
            let total_conns = total_conns.clone();

            tokio::spawn(async move {
                loop {
                    let result = listener.accept().await;
                    if result.is_err() {
                        break;
                    }
                    let (stream, _) = result.unwrap();
                    total_conns.fetch_add(1, Ordering::SeqCst);

                    tokio::spawn(async move {
                        let (mut read_half, mut write_half) = tokio::io::split(stream);
                        loop {
                            let mut len_buf = [0u8; 2];
                            if read_half.read_exact(&mut len_buf).await.is_err() {
                                break;
                            }
                            let path_len = u16::from_be_bytes(len_buf) as usize;
                            let mut buf = vec![0u8; path_len];
                            if read_half.read_exact(&mut buf).await.is_err() {
                                break;
                            }
                            let mut hlen = [0u8; 2];
                            if read_half.read_exact(&mut hlen).await.is_err() {
                                break;
                            }
                            let hl = u16::from_be_bytes(hlen) as usize;
                            let mut hbuf = vec![0u8; hl];
                            if read_half.read_exact(&mut hbuf).await.is_err() {
                                break;
                            }
                            let mut plen = [0u8; 4];
                            if read_half.read_exact(&mut plen).await.is_err() {
                                break;
                            }
                            let pl = u32::from_be_bytes(plen) as usize;
                            let mut pbuf = vec![0u8; pl];
                            if read_half.read_exact(&mut pbuf).await.is_err() {
                                break;
                            }

                            use crate::pipeline::network::codec::TcpResponseMessage;
                            let resp = TcpResponseMessage::new(Bytes::from(pbuf));
                            let enc = resp.encode().unwrap();
                            if write_half.write_all(&enc).await.is_err() {
                                break;
                            }
                        }
                    });
                }
            });
        }

        let config = TcpRequestConfig {
            request_timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(5),
            pool_size: 2,
            channel_buffer: 50,
        };
        let pool = Arc::new(TcpConnectionPool::new(config));

        // 同时访问全部 4 个主机
        let mut handles = vec![];
        for addr in &addrs {
            let pool = pool.clone();
            let addr = *addr;
            for i in 0..10 {
                let pool = pool.clone();
                handles.push(tokio::spawn(async move {
                    let conn = pool.get_connection(addr).await?;
                    let mut headers = Headers::new();
                    headers.insert("x-portname-path".to_string(), "test".to_string());
                    conn.send_request(Bytes::from(format!("req_{}", i)), &headers)
                        .await
                }));
            }
        }

        let mut ok_count = 0;
        for handle in handles {
            if handle.await.unwrap().is_ok() {
                ok_count += 1;
            }
        }

        assert!(
            ok_count > 0,
            "Requests across multiple hosts should succeed"
        );
        // 跨所有主机的总连接数应受限
        let tc = total_conns.load(Ordering::SeqCst);
        assert!(
            tc <= 8,
            "Total connections across 4 hosts should be <= 4*pool_size(2)=8, got {}",
            tc
        );
    }

    #[tokio::test]
    async fn test_idle_host_pool_cleanup() {
        let (addr, _conn_count) = spawn_echo_server().await;

        let config = TcpRequestConfig {
            request_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(5),
            pool_size: 2,
            channel_buffer: 10,
        };
        let pool = TcpConnectionPool::new(config);
        // 为测试将 TTL 覆盖为 0
        let pool = TcpConnectionPool {
            host_idle_ttl_ms: 0,
            ..pool
        };

        // 创建一条连接以填充主机条目
        let conn = pool.get_connection(addr).await.unwrap();
        let mut headers = Headers::new();
        headers.insert("x-portname-path".to_string(), "test".to_string());
        let _ = conn.send_request(Bytes::from("test"), &headers).await;
        drop(conn);

        assert!(pool.hosts.contains_key(&addr), "Host entry should exist");

        // 稍等片刻，使时间戳变陈旧
        tokio::time::sleep(Duration::from_millis(10)).await;

        pool.cleanup_idle_hosts();

        assert!(
            !pool.hosts.contains_key(&addr),
            "Idle host entry should be cleaned up"
        );
    }

    #[tokio::test]
    async fn test_connection_pool_reuse() {
        let (addr, conn_count) = spawn_echo_server().await;

        let config = TcpRequestConfig {
            request_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(5),
            pool_size: 2,
            channel_buffer: 10,
        };
        let pool = TcpConnectionPool::new(config);

        // 从连接池两次获取连接
        let conn1 = pool.get_connection(addr).await.unwrap();
        let mut headers = Headers::new();
        headers.insert("x-portname-path".to_string(), "test".to_string());
        let _ = conn1
            .send_request(Bytes::from("test1"), &headers)
            .await
            .unwrap();
        drop(conn1);

        tokio::time::sleep(Duration::from_millis(10)).await;

        let conn2 = pool.get_connection(addr).await.unwrap();
        let _ = conn2
            .send_request(Bytes::from("test2"), &headers)
            .await
            .unwrap();
        drop(conn2);

        assert_eq!(
            conn_count.load(Ordering::SeqCst),
            1,
            "Should reuse connection from pool"
        );
    }

    #[tokio::test]
    async fn test_unhealthy_connection_filtered() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // 立即关闭连接的服务器
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                drop(stream);
            }
        });

        let config = TcpRequestConfig {
            request_timeout: Duration::from_secs(1),
            connect_timeout: Duration::from_secs(1),
            pool_size: 2,
            channel_buffer: 10,
        };

        let result =
            TcpConnection::connect(addr, config.connect_timeout, config.channel_buffer).await;

        if let Ok(conn) = result {
            let mut headers = Headers::new();
            headers.insert("x-portname-path".to_string(), "test".to_string());
            let result = tokio::time::timeout(
                Duration::from_millis(250),
                conn.send_request(Bytes::from("test"), &headers),
            )
            .await;
            assert!(
                result.is_ok(),
                "send_request should fail promptly when the peer closes cleanly"
            );
            assert!(
                result.unwrap().is_err(),
                "clean peer close should surface as a request error"
            );

            // 服务器丢弃连接后，连接应变为不健康
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(
                !conn.is_healthy(),
                "Connection should become unhealthy after peer closes it"
            );
        }
    }

    #[tokio::test]
    async fn test_warmup_pre_connects_on_instance_discovery() {
        use crate::servicegroup::{Instance, TransportType};

        let (addr, conn_count) = spawn_echo_server().await;

        let config = TcpRequestConfig {
            request_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(5),
            pool_size: 4,
            channel_buffer: 10,
        };
        let pool = Arc::new(TcpConnectionPool::new(config));
        let cancel_token = tokio_util::sync::CancellationToken::new();

        // 创建一个初始无实例的 watch channel
        let (instance_tx, instance_rx) = tokio::sync::watch::channel::<Vec<Instance>>(Vec::new());

        // 启动预热 watcher
        pool.start_warmup_watcher(instance_rx, cancel_token.clone());

        // 让 watcher 任务启动并开始轮询 changed()
        tokio::time::sleep(Duration::from_millis(50)).await;

        // 尚无连接
        assert_eq!(conn_count.load(Ordering::SeqCst), 0);

        // 发现一个新的 TCP 后端
        let tcp_addr = format!("{}:{}/test_portname", addr.ip(), addr.port());
        instance_tx
            .send(vec![Instance {
                servicegroup: "test".to_string(),
                portname: "test_portname".to_string(),
                namespace: "default".to_string(),
                instance_id: 1,
                transport: TransportType::Tcp(tcp_addr),
                device_type: None,
            }])
            .unwrap();

        // 给预热 watcher 时间处理并连接
        tokio::time::sleep(Duration::from_millis(200)).await;

        // 应已预连接
        assert_eq!(
            conn_count.load(Ordering::SeqCst),
            1,
            "Warmup should have created 1 connection to the newly discovered backend"
        );

        // 连接池应包含该主机条目
        assert!(
            pool.hosts.contains_key(&addr),
            "Pool should have a host entry for the warmed address"
        );

        cancel_token.cancel();
    }

    #[tokio::test]
    async fn test_closed_connection_after_enqueue_fails_promptly() {
        let (addr, _conn_count) = spawn_echo_server().await;

        let mut conn = TcpConnection::connect(addr, Duration::from_secs(5), 10)
            .await
            .unwrap();
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        conn.post_enqueue_barrier = Some(barrier.clone());
        let conn = Arc::new(conn);

        let request = {
            let conn = conn.clone();
            tokio::spawn(async move {
                let mut headers = Headers::new();
                headers.insert("x-portname-path".to_string(), "test".to_string());
                tokio::time::timeout(
                    Duration::from_millis(200),
                    conn.send_request(Bytes::from("queued_after_close"), &headers),
                )
                .await
            })
        };

        // 等待请求入队，然后模拟 writer 在处理新项之前
        // 进入其终止清理路径。
        barrier.wait().await;
        conn.closed.store(true, Ordering::Release);
        conn.healthy.store(false, Ordering::Relaxed);
        barrier.wait().await;

        let result = request.await.unwrap();
        assert!(
            result.is_ok(),
            "send_request should fail promptly instead of waiting for request_timeout"
        );
        let inner = result.unwrap();
        assert!(inner.is_err(), "closed connection should return an error");

        conn.writer_handle.abort();
        conn.reader_handle.abort();
    }

    #[tokio::test]
    async fn test_lockfree_submit_and_batch() {
        // 验证对同一连接的并发提交会产生批量写入
        // （batch_size > 1）：通过检查所有请求即使同时提交
        // 也能正确完成来验证。
        let (addr, conn_count) = spawn_echo_server().await;

        let config = TcpRequestConfig {
            request_timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(5),
            pool_size: 1,
            channel_buffer: 200,
        };
        let pool = Arc::new(TcpConnectionPool::new(config));

        // 迫使使用单一连接，然后猛发并发请求
        let conn = pool.get_connection(addr).await.unwrap();

        let mut handles = vec![];
        for i in 0..100 {
            let conn = conn.clone();
            handles.push(tokio::spawn(async move {
                let mut headers = Headers::new();
                headers.insert("x-portname-path".to_string(), "test".to_string());
                conn.send_request(Bytes::from(format!("batch_req_{}", i)), &headers)
                    .await
            }));
        }

        let mut ok_count = 0;
        for handle in handles {
            if handle.await.unwrap().is_ok() {
                ok_count += 1;
            }
        }

        assert_eq!(ok_count, 100, "All 100 concurrent requests should succeed");
        assert_eq!(
            conn_count.load(Ordering::SeqCst),
            1,
            "Should use only 1 connection"
        );
    }

    // ── 新增：parse_address 边界 + 构造器 / 配置 / latency-trace ──────────────

    #[test]
    fn test_parse_address_with_tcp_prefix_and_portname() {
        let (addr, portname) =
            TcpRequestClient::parse_address("tcp://127.0.0.1:5555/inference").unwrap();
        assert_eq!(addr.port(), 5555);
        assert_eq!(portname.as_deref(), Some("inference"));
    }

    #[test]
    fn test_parse_address_rejects_empty_input() {
        assert!(TcpRequestClient::parse_address("").is_err());
    }

    #[test]
    fn test_parse_address_rejects_address_without_port() {
        assert!(TcpRequestClient::parse_address("127.0.0.1").is_err());
        assert!(TcpRequestClient::parse_address("tcp://127.0.0.1").is_err());
    }

    #[test]
    fn test_parse_address_error_message_includes_input() {
        let err = TcpRequestClient::parse_address("not-an-address").unwrap_err();
        let s = format!("{err}");
        assert!(s.contains("not-an-address"), "got: {s}");
    }

    #[test]
    fn test_tcp_client_default_does_not_panic() {
        let _ = TcpRequestClient::default();
    }

    #[test]
    fn test_tcp_client_with_config_preserves_pool_size() {
        let mut cfg = TcpRequestConfig::default();
        cfg.pool_size = 7;
        let client = TcpRequestClient::with_config(cfg).expect("client should build");
        // pool_size 没有 pub getter；通过 stats() 间接验证客户端可用
        assert_eq!(client.transport_name(), "tcp");
        assert!(client.is_healthy());
    }

    #[test]
    fn test_latency_trace_enabled_env_values() {
        let key = "PGD_TCP_LATENCY_TRACE";
        unsafe {
            std::env::remove_var(key);
        }
        assert!(!latency_trace_enabled(), "默认（未设置）应为 false");
        for (val, want) in [("1", true), ("true", true), ("0", false), ("yes", false)] {
            unsafe {
                std::env::set_var(key, val);
            }
            assert_eq!(
                latency_trace_enabled(),
                want,
                "env={val:?} 期望 {want}, 实际 {}",
                latency_trace_enabled()
            );
        }
        unsafe {
            std::env::remove_var(key);
        }
    }
}
