// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::network::egress::addressed_router` —— 按显式 address 路由的出站客户端
//!
//! ## 设计意图
//! 在 `PushRouter` 之外提供一条"调用方已知目标地址"的快路径：跳过 instance 选择，
//! 直接基于 `RequestPlaneClient::send_request(address, payload, headers)` 发出请求。
//! 常用于 health check / 控制面消息 / 单元测试。
//!
//! ## 外部契约
//! - 公开类型一致；构造器签名、`Drop` 时是否取消 inflight 都是契约。
//! - 不引入新的 builder pattern，不重命名公有方法。
//!
//! ## 实现要点
//! - 内部不维护连接池——完全委托给注入的 `Arc<dyn RequestPlaneClient>`；
//!   `addressed_router` 的职责仅是：组装 headers、塞入 request_id、发出调用。
//! - 抽取 3 个私有 helper 以消除 `generate()` 内的重复：
//!   - [`AddressedPushRouter::cancel_recv_stream_if_present`] —— 把 "4 处 `if Err: cancel + return`"
//!     的取消动作集中表达，避免遗漏；
//!   - [`build_request_headers`] —— 把 trace 注入、request-id、发送时间戳三步打包；
//!   - [`current_unix_nanos`] —— 把 `SystemTime` 取时间戳的不可测细节包起来便于单测。

// ─────────────────────────────────────────────────────────────────────────────
// === SECTION: [1] 依赖导入
// ─────────────────────────────────────────────────────────────────────────────

use std::sync::Arc;
use std::time::Instant;

use super::unified_client::RequestPlaneClient;
use super::*;
use crate::servicegroup::Instance;
use crate::discovery::PortNameInstanceId;
use crate::pagoda_timeline_range;
use crate::engine::{AsyncEngine, AsyncEngineContextProvider, Data};
use crate::error::{PagodaError, ErrorType};
use crate::logging::inject_trace_headers_into_map;
use crate::metrics::frontend_perf::STAGE_DURATION_SECONDS;
use crate::metrics::request_plane::{
    REQUEST_PLANE_INFLIGHT, REQUEST_PLANE_QUEUE_SECONDS, REQUEST_PLANE_ROUNDTRIP_TTFT_SECONDS,
    REQUEST_PLANE_SEND_SECONDS,
};
use crate::pipeline::network::ConnectionInfo;
use crate::pipeline::network::NetworkStreamWrapper;
use crate::pipeline::network::PendingConnections;
use crate::pipeline::network::StreamOptions;
use crate::pipeline::network::TwoPartCodec;
use crate::pipeline::network::codec::TwoPartMessage;
use crate::pipeline::network::tcp;
use crate::pipeline::{ManyOut, PipelineError, ResponseStream, SingleIn};
use crate::protocols::maybe_error::MaybeError;

use anyhow::{Error, Result};
use futures::stream::Stream;
use serde::Deserialize;
use serde::Serialize;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_stream::{StreamExt, StreamNotifyClose, wrappers::ReceiverStream};
use tracing::Instrument;

// ─────────────────────────────────────────────────────────────────────────────
// === SECTION: [2] wire 协议类型（RequestType / ResponseType / RequestControlMessage）
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RequestType {
    SingleIn,
    ManyIn,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ResponseType {
    SingleOut,
    ManyOut,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RequestControlMessage {
    id: String,
    request_type: RequestType,
    response_type: ResponseType,
    connection_info: ConnectionInfo,
    /// wall-clock 发送时间戳（自 UNIX epoch 起的纳秒数），用于拆解传输层延迟。
    /// 使用 `SystemTime`，精度取决于前端与后端主机间的 NTP 同步情况。
    /// 单机性能分析时可靠；跨主机的数值应视为近似值。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    frontend_send_ts_ns: Option<u64>,
}

/// RAII guard：drop 时递减 `REQUEST_PLANE_INFLIGHT`，除非已被解除（disarm）。
/// 防止在递增计数与构造 `InflightDecStream` 之间因 `?` 提前返回而导致计数泄漏。
struct InflightGuard {
    armed: bool,
}

impl InflightGuard {
    fn new() -> Self {
        Self { armed: true }
    }

    /// 消费该 guard 但不递减计数。当 `InflightDecStream` 接管递减职责时调用。
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        if self.armed {
            REQUEST_PLANE_INFLIGHT.dec();
        }
    }
}

/// 包装器：当 stream 被 drop 时递减 request-plane 的 inflight 计量器。
struct InflightDecStream<S> {
    inner: S,
}

impl<S, T> Stream for InflightDecStream<S>
where
    S: Stream<Item = T> + Unpin,
{
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl<S> Drop for InflightDecStream<S> {
    fn drop(&mut self) {
        REQUEST_PLANE_INFLIGHT.dec();
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// === SECTION: [3] 私有 helper（headers builder / unix-nanos / cancel-on-error）
// ─────────────────────────────────────────────────────────────────────────────

/// 当前 wall-clock 自 UNIX epoch 以来的纳秒数；若系统时钟在 epoch 之前则回退到 0。
///
/// 单独抽出便于在测试中验证 "非零 + 比一秒前要大"，不直接 mock `SystemTime::now`。
fn current_unix_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// 构造发往 request plane 的 HTTP-style headers：
/// 1. `inject_trace_headers_into_map` —— W3C tracecontext 注入；
/// 2. `request-id` —— 调用方上下文 id；
/// 3. `x-frontend-send-ts-ns` —— 临近写 socket 时刻打的时间戳，用于网络段耗时拆分。
fn build_request_headers(request_id: &str) -> std::collections::HashMap<String, String> {
    let mut headers = std::collections::HashMap::new();
    inject_trace_headers_into_map(&mut headers);
    headers.insert("request-id".to_string(), request_id.to_string());
    headers.insert(
        "x-frontend-send-ts-ns".to_string(),
        current_unix_nanos().to_string(),
    );
    headers
}

// ─────────────────────────────────────────────────────────────────────────────
// === SECTION: [4] AddressedRequest 与 AddressedPushRouter
// ─────────────────────────────────────────────────────────────────────────────

pub struct AddressedRequest<T> {
    request: T,
    address: String,
    /// 携带 portname 名称与 instance_id，使取消操作精确作用于
    /// (portname, instance) 这一对，而非同一 runtime 上的所有 portname。
    instance: Option<Instance>,
}

impl<T> AddressedRequest<T> {
    pub fn new(request: T, address: String) -> Self {
        Self {
            request,
            address,
            instance: None,
        }
    }

    pub fn with_instance(request: T, address: String, instance: Instance) -> Self {
        Self {
            request,
            address,
            instance: Some(instance),
        }
    }

    pub(crate) fn into_parts(self) -> (T, String, Option<Instance>) {
        (self.request, self.address, self.instance)
    }
}

pub struct AddressedPushRouter {
    // 请求传输（统一 trait object——适用于所有传输）
    req_client: Arc<dyn RequestPlaneClient>,

    // 响应传输（TCP 流式——保持不变）
    resp_transport: Arc<tcp::server::TcpStreamServer>,
}

impl AddressedPushRouter {
    /// 用一个 request plane 客户端创建新的 router。
    ///
    /// 这是适用于任意传输类型的统一构造器。
    /// 客户端以 trait object 形式注入，隐藏具体实现。
    pub fn new(
        req_client: Arc<dyn RequestPlaneClient>,
        resp_transport: Arc<tcp::server::TcpStreamServer>,
    ) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            req_client,
            resp_transport,
        }))
    }

    /// 取消某个 instance 上所有待处理的响应流注册。
    pub async fn cancel_instance_streams(&self, instance_id: &PortNameInstanceId) -> usize {
        self.resp_transport
            .cancel_instance_streams(instance_id)
            .await
    }

    /// 当 instance 在发现中重新出现后，清除其 tombstone 标记。
    pub async fn clear_instance_tombstone(&self, instance_id: &PortNameInstanceId) {
        self.resp_transport
            .clear_instance_tombstone(instance_id)
            .await
    }

    /// 若已注册了响应流，则取消之；否则 no-op。
    ///
    /// 集中 `generate()` 内 4 处 `if let Some(subject) = &recv_subject { ... }` 的清理
    /// 动作，避免任一分支遗漏导致响应流 leak。
    async fn cancel_recv_stream_if_present(&self, recv_subject: &Option<String>) {
        if let Some(subject) = recv_subject {
            self.resp_transport.cancel_recv_stream(subject).await;
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// === SECTION: [5] AsyncEngine 实现
// ─────────────────────────────────────────────────────────────────────────────

#[async_trait::async_trait]
impl<T, U> AsyncEngine<SingleIn<AddressedRequest<T>>, ManyOut<U>, Error> for AddressedPushRouter
where
    T: Data + Serialize,
    U: Data + for<'de> Deserialize<'de> + MaybeError,
{
    async fn generate(&self, request: SingleIn<AddressedRequest<T>>) -> Result<ManyOut<U>, Error> {
        let queue_start = Instant::now();
        REQUEST_PLANE_INFLIGHT.inc();
        let inflight_guard = InflightGuard::new();

        let request_id = request.context().id().to_string();
        let (addressed_request, context) = request.transfer(());
        let (request, address, instance_info) = addressed_request.into_parts();
        let engine_ctx = context.context();
        let engine_ctx_ = engine_ctx.clone();

        // 单入多出的 data plane 注册选项。
        let options = StreamOptions::builder()
            .context(engine_ctx.clone())
            .enable_request_stream(false)
            .enable_response_stream(true)
            .build()
            .unwrap();

        // 向 data plane 注册我们的需求。
        // TODO：将其泛化为通用 data plane 对象，以隐藏具体传输实现。
        let pending_connections: PendingConnections = self.resp_transport.register(options).await;

        // 校验并解包 RegisteredStream 对象。
        let pending_response_stream = match pending_connections.into_parts() {
            (None, Some(recv_stream)) => recv_stream,
            _ => {
                panic!("Invalid data plane registration for a SingleIn/ManyOut transport");
            }
        };

        // 从注册流中拆出 connection info 和 stream provider。
        let (connection_info, response_stream_provider) = pending_response_stream.into_parts();

        // 在移动 connection_info 之前先快照 subject，供清理时使用。
        let recv_subject: Option<String> =
            serde_json::from_str::<tcp::TcpStreamConnectionInfo>(&connection_info.info)
                .ok()
                .map(|ci| ci.subject);

        // 如果 instance 已经被 tombstone，就快速失败并返回可迁移的错误，
        // 而不是继续写入 request plane。
        if let (Some(subject), Some(inst)) = (&recv_subject, &instance_info) {
            let portname_instance_id = inst.portname_instance_id();
            if !self
                .resp_transport
                .associate_instance(subject, &portname_instance_id)
                .await
            {
                return Err(anyhow::anyhow!(
                    PagodaError::builder()
                        .error_type(ErrorType::Disconnected)
                        .message(
                            "Worker removed before request could be sent (tombstoned instance)"
                        )
                        .build()
                ));
            }
        }

        // 将 connection info 打包成 two-part message 的“header” servicegroup 的一部分，
        // 用来发起请求。
        // TODO：这个对象应该由 register 调用自动创建，通过两个 into_parts() 调用达成。
        // 这里的所有信息都由 [`StreamOptions`] 对象和/或 data plane 对象提供。
        let control_message = RequestControlMessage {
            id: engine_ctx.id().to_string(),
            request_type: RequestType::SingleIn,
            response_type: ResponseType::ManyOut,
            connection_info,
            frontend_send_ts_ns: None,
        };

        // 接下来构建 two-part message，把 connection info 和 request 打包成一个
        // 可以通过 wire 发送的 `Vec<u8>`。
        // --- 把这部分封装进 WorkQueuePublisher ---
        let ctrl = match serde_json::to_vec(&control_message) {
            Ok(v) => v,
            Err(e) => {
                self.cancel_recv_stream_if_present(&recv_subject).await;
                return Err(e.into());
            }
        };
        let data = match serde_json::to_vec(&request) {
            Ok(v) => v,
            Err(e) => {
                self.cancel_recv_stream_if_present(&recv_subject).await;
                return Err(e.into());
            }
        };

        tracing::trace!(
            request_id,
            "packaging two-part message; ctrl: {} bytes, data: {} bytes",
            ctrl.len(),
            data.len()
        );

        let msg = TwoPartMessage::from_parts(ctrl.into(), data.into());

        // request plane / work queue 应该提供一个可复用的 two-part message codec，
        // 或者直接接收 two-part message。
        // TODO：更新这里。
        let codec = TwoPartCodec::default();
        let buffer = match codec.encode_message(msg) {
            Ok(v) => v,
            Err(e) => {
                self.cancel_recv_stream_if_present(&recv_subject).await;
                return Err(e.into());
            }
        };

        REQUEST_PLANE_QUEUE_SECONDS.observe(queue_start.elapsed().as_secs_f64());
        let tx_start = Instant::now();

        // 到这里就已经完成了传输抽象所需的工作。

        // 通过统一的 client 接口发送请求。
        tracing::trace!(
            request_id,
            transport = self.req_client.transport_name(),
            address = %address,
            "Sending request via request plane client"
        );

        // 把 trace headers / request-id / 发送时间戳的注入封装到 helper，
        // 让时间戳在接近 socket 写入时才生成，这样网络段耗时不包含序列化/编码开销。
        let headers = build_request_headers(&request_id);

        // 阶段 A：前端 → 后端（network + queue + ack）。
        let _nvtx_send = pagoda_timeline_range!("transport.tcp.send");
        let send_result = self.req_client.send_request(address, buffer, headers).await;
        drop(_nvtx_send);

        if let Err(e) = send_result {
            self.cancel_recv_stream_if_present(&recv_subject).await;
            return Err(e);
        }
        REQUEST_PLANE_SEND_SECONDS.observe(tx_start.elapsed().as_secs_f64());

        let _nvtx_wait = pagoda_timeline_range!("transport.tcp.wait_backend");
        tracing::trace!(request_id, "awaiting transport handshake");

        // RecvError → 可迁移的 Disconnected（watcher 取消了 subject，
        // 或者 worker 在建立 response stream 之前就死了）。
        let response_stream = match response_stream_provider.await {
            Ok(Ok(stream)) => stream,
            Ok(Err(e)) => {
                // generate() 在任何 response 字节到来之前就失败了；通过
                // CannotConnect 迁移，因为主因通常是 worker 本地的
                // 配置/版本问题。当前 wire prologue 只携带一个
                // 不透明字符串，所以应用层拒绝也会重试——这很安全，
                // 因为此时还没有可见副作用。后续可考虑结构化 prologue 错误类型，
                // 以便更精细地路由。
                self.cancel_recv_stream_if_present(&recv_subject).await;
                return Err(anyhow::anyhow!(
                    PagodaError::builder()
                        .error_type(ErrorType::CannotConnect)
                        .message(format!(
                            "Worker generate() failed before response stream: {e}"
                        ))
                        .build()
                ));
            }
            Err(_recv_err) => {
                // oneshot 被丢弃：要么是 discovery watcher 取消了
                // 这个 subject，要么是 worker 在握手中途死掉了。
                self.cancel_recv_stream_if_present(&recv_subject).await;
                return Err(anyhow::anyhow!(
                    PagodaError::builder()
                        .error_type(ErrorType::Disconnected)
                        .message("Worker disconnected before response stream was established")
                        .build()
                ));
            }
        };
        drop(_nvtx_wait);

        // TODO：改用 Server-Sent Events（SSE）检测流结束。
        let mut is_complete_final = false;
        let mut first_response = true;
        let stream = tokio_stream::StreamNotifyClose::new(
            tokio_stream::wrappers::ReceiverStream::new(response_stream.rx),
        )
        .filter_map(move |res| {
            if let Some(res_bytes) = res {
                if first_response {
                    first_response = false;
                    let roundtrip_ttft = tx_start.elapsed().as_secs_f64();
                    REQUEST_PLANE_ROUNDTRIP_TTFT_SECONDS.observe(roundtrip_ttft);
                    STAGE_DURATION_SECONDS
                        .with_label_values(&["transport_roundtrip"])
                        .observe(queue_start.elapsed().as_secs_f64());
                }
                if is_complete_final {
                    let err = PagodaError::msg(
                        "Response received after generation ended - this should never happen",
                    );
                    return Some(U::from_err(err));
                }
                match serde_json::from_slice::<NetworkStreamWrapper<U>>(&res_bytes) {
                    Ok(item) => {
                        is_complete_final = item.complete_final;
                        if let Some(data) = item.data {
                            Some(data)
                        } else if is_complete_final {
                            None
                        } else {
                            let err = PagodaError::msg(
                                "Empty response received - this should never happen",
                            );
                            Some(U::from_err(err))
                        }
                    }
                    Err(err) => {
                        // legacy log print
                        let json_str = String::from_utf8_lossy(&res_bytes);
                        tracing::warn!(%err, %json_str, "Failed deserializing JSON to response");

                        Some(U::from_err(PagodaError::msg(err.to_string())))
                    }
                }
            } else if is_complete_final {
                // end of stream
                None
            } else if engine_ctx_.is_stopped() {
                // 如果调用了 `stop_generating()`，就优雅地结束流。这里不要检查
                // `is_killed()`，因为它意味着流是异常结束的，应该交给下面的错误分支处理。
                tracing::debug!("Request cancelled and then trying to read a response");
                None
            } else {
                // stream ended unexpectedly
                let err = PagodaError::builder()
                    .error_type(ErrorType::Disconnected)
                    .message("Stream ended before generation completed")
                    .build();
                tracing::debug!("{err}");
                Some(U::from_err(err))
            }
        });

        inflight_guard.disarm();
        let stream = InflightDecStream { inner: stream };
        Ok(ResponseStream::new(Box::pin(stream), engine_ctx))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// === SECTION: [6] 测试
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    //! ## 测试矩阵
    //!
    //! | 测试名 | 覆盖维度 |
    //! |---|---|
    //! | `test_addressed_request_new_no_instance` | `new()` 构造后 `into_parts` 三段还原（instance=None） |
    //! | `test_addressed_request_with_instance_round_trip` | `with_instance()` 构造后 `into_parts` 携带 Instance |
    //! | `test_current_unix_nanos_is_positive_and_recent` | 时间戳 helper 非零且与系统时钟一致 |
    //! | `test_build_request_headers_contains_required_fields` | headers 至少含 `request-id` 与 `x-frontend-send-ts-ns` |
    //! | `test_build_request_headers_request_id_preserved` | `request-id` 与入参字符串相等 |
    //! | `test_build_request_headers_ts_is_parsable_u64` | `x-frontend-send-ts-ns` 可被解析为 u64 |
    //! | `test_inflight_guard_and_stream_drop_semantics_serial` | 三合一：armed/disarmed Guard + InflightDecStream drop 语义（串行以避免并行 gauge 干扰） |
    //! | `test_request_type_serde_snake_case` | `RequestType` JSON 序列化为 snake_case 字符串 |
    //! | `test_response_type_serde_snake_case` | `ResponseType` 同上 |
    //! | `test_request_control_message_round_trip` | `RequestControlMessage` 完整 round-trip + 可选字段省略 |
    //!
    //! ## 说明
    //! `generate()` 的端到端行为依赖 `TcpStreamServer` 和真实的 RequestPlaneClient 实现，
    //! 由集成测试覆盖；这里的单元测试只锁定可独立验证的契约面：headers 协议、wire 枚举
    //! 序列化、in-flight 计数器孪生不变式、构造器透传。
    use super::*;
    use crate::servicegroup::Instance;
    use crate::metrics::request_plane::REQUEST_PLANE_INFLIGHT;
    use futures::stream;
    use serde_json::Value;

    fn dummy_instance() -> Instance {
        // 仅用于 round-trip 测试：字段值不参与任何路由逻辑。
        Instance {
            servicegroup: "comp".into(),
            portname: "ep".into(),
            namespace: "ns".into(),
            instance_id: 42,
            transport: crate::servicegroup::TransportType::Nats("nats://127.0.0.1:4222".into()),
            device_type: None,
        }
    }

    #[test]
    fn test_addressed_request_new_no_instance() {
        let req = AddressedRequest::new(123u32, "addr-a".to_string());
        let (r, a, i) = req.into_parts();
        assert_eq!(r, 123);
        assert_eq!(a, "addr-a");
        assert!(i.is_none());
    }

    #[test]
    fn test_addressed_request_with_instance_round_trip() {
        let inst = dummy_instance();
        let inst_clone = inst.clone();
        let req = AddressedRequest::with_instance("payload".to_string(), "addr-b".to_string(), inst);
        let (r, a, i) = req.into_parts();
        assert_eq!(r, "payload");
        assert_eq!(a, "addr-b");
        let i = i.expect("instance should be present");
        assert_eq!(i.instance_id, inst_clone.instance_id);
        assert_eq!(i.portname, inst_clone.portname);
    }

    #[test]
    fn test_current_unix_nanos_is_positive_and_recent() {
        let t1 = current_unix_nanos();
        assert!(t1 > 0, "wall-clock 应为正");
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        // 与系统时钟的差距应不超过 1 秒。
        let diff = now_ns.abs_diff(t1);
        assert!(diff < 1_000_000_000, "diff={}ns", diff);
    }

    #[test]
    fn test_build_request_headers_contains_required_fields() {
        let h = build_request_headers("rid-xyz");
        assert!(h.contains_key("request-id"));
        assert!(h.contains_key("x-frontend-send-ts-ns"));
    }

    #[test]
    fn test_build_request_headers_request_id_preserved() {
        let h = build_request_headers("trace-007");
        assert_eq!(h.get("request-id").map(String::as_str), Some("trace-007"));
    }

    #[test]
    fn test_build_request_headers_ts_is_parsable_u64() {
        let h = build_request_headers("x");
        let ts = h
            .get("x-frontend-send-ts-ns")
            .expect("present")
            .parse::<u64>()
            .expect("parsable as u64");
        assert!(ts > 0);
    }

    #[test]
    fn test_inflight_guard_and_stream_drop_semantics_serial() {
        // 把三项与全局 REQUEST_PLANE_INFLIGHT 相关的断言合并起来，避免并行测试
        // 互相干扰；delta 语义在单个函数内严格等于“相对起点 0”。

        // 1) armed 默认 → drop 后该抵消一次 inc
        let start = REQUEST_PLANE_INFLIGHT.get();
        REQUEST_PLANE_INFLIGHT.inc();
        {
            let _g = InflightGuard::new();
        }
        assert!(
            (REQUEST_PLANE_INFLIGHT.get() - start).abs() < f64::EPSILON,
            "armed guard drop 未净减 1"
        );

        // 2) disarm() 后 drop 不减计
        let start = REQUEST_PLANE_INFLIGHT.get();
        REQUEST_PLANE_INFLIGHT.inc();
        {
            let g = InflightGuard::new();
            g.disarm();
        }
        assert!(
            (REQUEST_PLANE_INFLIGHT.get() - start - 1.0).abs() < f64::EPSILON,
            "disarmed guard drop 不该减计"
        );
        REQUEST_PLANE_INFLIGHT.dec(); // 恢复 gauge

        // 3) InflightDecStream drop 减计一次
        let start = REQUEST_PLANE_INFLIGHT.get();
        REQUEST_PLANE_INFLIGHT.inc();
        {
            let inner = stream::iter::<Vec<i32>>(vec![]);
            let _s = InflightDecStream { inner };
        }
        assert!(
            (REQUEST_PLANE_INFLIGHT.get() - start).abs() < f64::EPSILON,
            "InflightDecStream drop 未净减 1"
        );
    }

    #[test]
    fn test_request_type_serde_snake_case() {
        let s_single = serde_json::to_string(&RequestType::SingleIn).unwrap();
        let s_many = serde_json::to_string(&RequestType::ManyIn).unwrap();
        assert_eq!(s_single, "\"single_in\"");
        assert_eq!(s_many, "\"many_in\"");
        // round-trip
        let r: RequestType = serde_json::from_str("\"single_in\"").unwrap();
        assert!(matches!(r, RequestType::SingleIn));
    }

    #[test]
    fn test_response_type_serde_snake_case() {
        let s_single = serde_json::to_string(&ResponseType::SingleOut).unwrap();
        let s_many = serde_json::to_string(&ResponseType::ManyOut).unwrap();
        assert_eq!(s_single, "\"single_out\"");
        assert_eq!(s_many, "\"many_out\"");
        let r: ResponseType = serde_json::from_str("\"many_out\"").unwrap();
        assert!(matches!(r, ResponseType::ManyOut));
    }

    #[test]
    fn test_request_control_message_round_trip() {
        let m = RequestControlMessage {
            id: "id-1".into(),
            request_type: RequestType::SingleIn,
            response_type: ResponseType::ManyOut,
            connection_info: ConnectionInfo {
                transport: "tcp".into(),
                info: r#"{"subject":"abc"}"#.into(),
            },
            frontend_send_ts_ns: None,
        };
        let s = serde_json::to_string(&m).unwrap();
        // frontend_send_ts_ns 为 None 时应被跳过
        let v: Value = serde_json::from_str(&s).unwrap();
        assert!(v.get("frontend_send_ts_ns").is_none());
        assert_eq!(v["id"], "id-1");
        assert_eq!(v["request_type"], "single_in");
        assert_eq!(v["response_type"], "many_out");
        // 反序列化回原结构
        let back: RequestControlMessage = serde_json::from_str(&s).unwrap();
        assert_eq!(back.id, "id-1");
        assert!(matches!(back.request_type, RequestType::SingleIn));
        assert!(matches!(back.response_type, ResponseType::ManyOut));
        assert!(back.frontend_send_ts_ns.is_none());
    }
}
