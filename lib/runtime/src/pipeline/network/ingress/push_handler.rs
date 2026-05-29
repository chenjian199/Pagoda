// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::network::ingress::push_handler` —— PushWorkHandler 处理器实现
//!
//! ## 设计意图
//! ingress 侧端点收到一条请求后，需要把"反序列化 → 调用本地 AsyncEngine → 把响应流通过
//! response plane 推回"这一连串动作封装成可复用对象。本文件提供该实现，让
//! NATS / TCP / HTTP server 都能共用同一份请求处理逻辑。
//!
//! ## 外部契约
//! - `pub trait PushWorkHandler` 由 ingress.rs 定义；本文件提供其默认实现结构体，
//!   与 lib-copy 完全一致；不引入额外的 helper 方法或类型别名。
//! - `handle_payload(payload, request_id)` 的错误语义、tracing span / metric 增量
//!   都属于契约。
//!
//! ## 实现要点
//! - 反序列化失败、响应流断开等错误统一返回 `anyhow::Error`，由调用方决定是否降级；
//!   不在本文件内做 retry。
//! - response plane 的 `mpsc::Sender` 在响应流耗尽后立即 drop，触发下游 close。

use super::*;

use crate::metrics::prometheus_names::work_handler;
use crate::metrics::work_handler_perf::{
    WORK_HANDLER_NETWORK_TRANSIT_SECONDS, WORK_HANDLER_TIME_TO_FIRST_RESPONSE_SECONDS,
};
use crate::protocols::maybe_error::MaybeError;
use prometheus::{Histogram, IntCounter, IntCounterVec, IntGauge};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Instant;
use tracing::Instrument;
use tracing::info_span;

/// Metrics configuration for profiling work handlers
#[derive(Clone, Debug)]
pub struct WorkHandlerMetrics {
    pub request_counter: IntCounter,
    pub request_duration: Histogram,
    pub inflight_requests: IntGauge,
    pub request_bytes: IntCounter,
    pub response_bytes: IntCounter,
    pub error_counter: IntCounterVec,
    pub cancellation_total: IntCounter,
}

impl WorkHandlerMetrics {
    pub fn new(
        request_counter: IntCounter,
        request_duration: Histogram,
        inflight_requests: IntGauge,
        request_bytes: IntCounter,
        response_bytes: IntCounter,
        error_counter: IntCounterVec,
        cancellation_total: IntCounter,
    ) -> Self {
        Self {
            request_counter,
            request_duration,
            inflight_requests,
            request_bytes,
            response_bytes,
            error_counter,
            cancellation_total,
        }
    }

    /// Create WorkHandlerMetrics from an endpoint using its built-in labeling
    pub fn from_endpoint(
        endpoint: &crate::component::Endpoint,
        metrics_labels: Option<&[(&str, &str)]>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let metrics_labels = metrics_labels.unwrap_or(&[]);
        let metrics = endpoint.metrics();
        let request_counter = metrics.create_intcounter(
            work_handler::REQUESTS_TOTAL,
            "Total number of requests processed by work handler",
            metrics_labels,
        )?;

        // Custom buckets for inference workloads: retain sub-second resolution for
        // fast operations, extend well beyond the default 10s ceiling to capture
        // long-running generation requests that can last minutes.
        let request_duration_buckets = vec![
            0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0, 30.0, 60.0, 120.0,
            300.0, 600.0,
        ];
        let request_duration = metrics.create_histogram(
            work_handler::REQUEST_DURATION_SECONDS,
            "Time spent processing requests by work handler",
            metrics_labels,
            Some(request_duration_buckets),
        )?;

        let inflight_requests = metrics.create_intgauge(
            work_handler::INFLIGHT_REQUESTS,
            "Number of requests currently being processed by work handler",
            metrics_labels,
        )?;

        let request_bytes = metrics.create_intcounter(
            work_handler::REQUEST_BYTES_TOTAL,
            "Total number of bytes received in requests by work handler",
            metrics_labels,
        )?;

        let response_bytes = metrics.create_intcounter(
            work_handler::RESPONSE_BYTES_TOTAL,
            "Total number of bytes sent in responses by work handler",
            metrics_labels,
        )?;

        let error_counter = metrics.create_intcountervec(
            work_handler::ERRORS_TOTAL,
            "Total number of errors in work handler processing",
            &[work_handler::ERROR_TYPE_LABEL],
            metrics_labels,
        )?;

        let cancellation_total = metrics.create_intcounter(
            work_handler::CANCELLATION_TOTAL,
            "Total number of requests cancelled by work handler",
            metrics_labels,
        )?;

        Ok(Self::new(
            request_counter,
            request_duration,
            inflight_requests,
            request_bytes,
            response_bytes,
            error_counter,
            cancellation_total,
        ))
    }
}

// RAII guard to ensure inflight gauge is decremented, request duration is observed,
// and lifecycle logs are emitted on all code paths.
struct RequestMetricsGuard {
    inflight_requests: prometheus::IntGauge,
    request_duration: prometheus::Histogram,
    start_time: Instant,
    request_id: Option<String>,
}

impl Drop for RequestMetricsGuard {
    fn drop(&mut self) {
        self.inflight_requests.dec();
        self.request_duration
            .observe(self.start_time.elapsed().as_secs_f64());
        if let Some(request_id) = &self.request_id {
            tracing::info!(request_id = %request_id, "request completed");
        }
    }
}

#[async_trait]
impl<T: Data, U: Data> PushWorkHandler for Ingress<SingleIn<T>, ManyOut<U>>
where
    T: Data + for<'de> Deserialize<'de> + std::fmt::Debug,
    U: Data + Serialize + MaybeError + std::fmt::Debug,
{
    fn add_metrics(
        &self,
        endpoint: &crate::component::Endpoint,
        metrics_labels: Option<&[(&str, &str)]>,
    ) -> Result<()> {
        // Call the Ingress-specific add_metrics implementation
        use crate::pipeline::network::Ingress;
        Ingress::add_metrics(self, endpoint, metrics_labels)
    }

    fn set_endpoint_health_check_notifier(&self, notifier: Arc<tokio::sync::Notify>) -> Result<()> {
        use crate::pipeline::network::Ingress;
        self.endpoint_health_check_notifier
            .set(notifier)
            .map_err(|_| anyhow::anyhow!("Endpoint health check notifier already set"))?;
        Ok(())
    }

    async fn handle_payload(
        &self,
        payload: Bytes,
        request_id: Option<String>,
    ) -> Result<(), PipelineError> {
        let t2_wallclock_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let start_time = std::time::Instant::now();

        // Increment inflight and ensure it's decremented on all exits via RAII guard
        let _inflight_guard = self.metrics().map(|m| {
            m.request_counter.inc();
            m.inflight_requests.inc();
            m.request_bytes.inc_by(payload.len() as u64);
            if let Some(rid) = &request_id {
                tracing::info!(request_id = %rid, "request received");
            }
            RequestMetricsGuard {
                inflight_requests: m.inflight_requests.clone(),
                request_duration: m.request_duration.clone(),
                start_time,
                request_id: request_id.clone(),
            }
        });

        // decode the control message and the request
        let msg = TwoPartCodec::default()
            .decode_message(payload)?
            .into_message_type();

        // we must have a header and a body
        // it will be held by this closure as a Some(permit)
        let (control_msg, request) = match msg {
            TwoPartMessageType::HeaderAndData(header, data) => {
                tracing::trace!(
                    "received two part message with ctrl: {} bytes, data: {} bytes",
                    header.len(),
                    data.len()
                );
                let control_msg: RequestControlMessage = match serde_json::from_slice(&header) {
                    Ok(cm) => cm,
                    Err(err) => {
                        let json_str = String::from_utf8_lossy(&header);
                        if let Some(m) = self.metrics() {
                            m.error_counter
                                .with_label_values(&[work_handler::error_types::DESERIALIZATION])
                                .inc();
                        }
                        return Err(PipelineError::DeserializationError(format!(
                            "Failed deserializing to RequestControlMessage. err={err}, json_str={json_str}"
                        )));
                    }
                };
                let request: T = serde_json::from_slice(&data)?;
                (control_msg, request)
            }
            _ => {
                if let Some(m) = self.metrics() {
                    m.error_counter
                        .with_label_values(&[work_handler::error_types::INVALID_MESSAGE])
                        .inc();
                }
                return Err(PipelineError::Generic(String::from(
                    "Unexpected message from work queue; unable extract a TwoPartMessage with a header and data",
                )));
            }
        };

        // Compute network transit time (T2 - T1) using cross-process wall-clock timestamps
        if let Some(t1_ns) = control_msg.frontend_send_ts_ns {
            let transit_ns = t2_wallclock_ns.saturating_sub(t1_ns);
            WORK_HANDLER_NETWORK_TRANSIT_SECONDS.observe(transit_ns as f64 / 1_000_000_000.0);
        }

        // extend request with context
        tracing::trace!("received control message: {:?}", control_msg);
        tracing::trace!("received request: {:?}", request);
        let request: context::Context<T> = Context::with_id(request, control_msg.id);

        // todo - eventually have a handler class which will returned an abstracted object, but for now,
        // we only support tcp here, so we can just unwrap the connection info
        tracing::trace!("creating tcp response stream");
        let mut publisher = tcp::client::TcpClient::create_response_stream(
            request.context(),
            control_msg.connection_info,
            self.metrics().map(|m| m.cancellation_total.clone()),
        )
        .await
        .map_err(|e| {
            if let Some(m) = self.metrics() {
                m.error_counter
                    .with_label_values(&[work_handler::error_types::RESPONSE_STREAM])
                    .inc();
            }
            PipelineError::Generic(format!("Failed to create response stream: {:?}", e,))
        })?;

        tracing::trace!("calling generate");
        let stream = self
            .segment
            .get()
            .expect("segment not set")
            .generate(request)
            .await
            .map_err(|e| {
                if let Some(m) = self.metrics() {
                    m.error_counter
                        .with_label_values(&[work_handler::error_types::GENERATE])
                        .inc();
                }
                PipelineError::GenerateError(e)
            });

        // the prolouge is sent to the client to indicate that the stream is ready to receive data
        // or if the generate call failed, the error is sent to the client
        let mut stream = match stream {
            Ok(stream) => {
                tracing::trace!("Successfully generated response stream; sending prologue");
                let _result = publisher.send_prologue(None).await;
                WORK_HANDLER_TIME_TO_FIRST_RESPONSE_SECONDS
                    .observe(start_time.elapsed().as_secs_f64());
                stream
            }
            Err(e) => {
                let error_string = e.to_string();

                #[cfg(debug_assertions)]
                {
                    tracing::debug!(
                        "Failed to generate response stream (with debug backtrace): {:?}",
                        e
                    );
                }
                #[cfg(not(debug_assertions))]
                {
                    tracing::error!("Failed to generate response stream: {error_string}");
                }

                let _result = publisher.send_prologue(Some(error_string)).await;
                Err(e)?
            }
        };

        let context = stream.context();

        // TODO: Detect end-of-stream using Server-Sent Events (SSE)
        let mut send_complete_final = true;
        let mut saw_error_response = false;
        while let Some(resp) = stream.next().await {
            tracing::trace!("Sending response: {:?}", resp);
            let is_error = resp.err().is_some();
            if is_error {
                saw_error_response = true;
            }
            let resp_wrapper = NetworkStreamWrapper {
                data: Some(resp),
                complete_final: false,
            };
            let resp_bytes = serde_json::to_vec(&resp_wrapper)
                .expect("fatal error: invalid response object - this should never happen");
            if let Some(m) = self.metrics() {
                m.response_bytes.inc_by(resp_bytes.len() as u64);
            }
            if (publisher.send(resp_bytes.into()).await).is_err() {
                send_complete_final = false;
                if context.is_stopped() {
                    // Say there are 2 threads accessing `context`, the sequence can be either:
                    // 1. context.stop_generating (other) -> publisher.send failure (this)
                    //    -> context.is_stopped (this)
                    // 2. publisher.send failure (this) -> context.stop_generating (other)
                    //    -> context.is_stopped (this)
                    // Case 1 can happen when client closed the connection after receiving the
                    // complete response from frontend. Hence, send failure can be expected in this
                    // case.
                    tracing::warn!("Failed to publish response for stream {}", context.id());
                } else {
                    // Otherwise, this is an error.
                    tracing::error!("Failed to publish response for stream {}", context.id());
                    context.stop_generating();
                }
                // Account errors in all cases, including cancellation. Therefore this metric can be
                // inflated.
                if let Some(m) = self.metrics() {
                    m.error_counter
                        .with_label_values(&[work_handler::error_types::PUBLISH_RESPONSE])
                        .inc();
                }
                break;
            } else if !is_error {
                // Only notify on non-error chunks — error responses don't prove
                // the engine is healthy and should not reset the canary timer.
                if let Some(notifier) = self.endpoint_health_check_notifier.get() {
                    notifier.notify_one();
                }
            }
        }
        if send_complete_final {
            let resp_wrapper = NetworkStreamWrapper::<U> {
                data: None,
                complete_final: true,
            };
            let resp_bytes = serde_json::to_vec(&resp_wrapper)
                .expect("fatal error: invalid response object - this should never happen");
            if let Some(m) = self.metrics() {
                m.response_bytes.inc_by(resp_bytes.len() as u64);
            }
            if (publisher.send(resp_bytes.into()).await).is_err() {
                tracing::error!(
                    "Failed to publish complete final for stream {}",
                    context.id()
                );
                if let Some(m) = self.metrics() {
                    m.error_counter
                        .with_label_values(&[work_handler::error_types::PUBLISH_FINAL])
                        .inc();
                }
            }
            // Only notify on stream completion if no error responses were seen
            if let (false, Some(notifier)) = (
                saw_error_response,
                self.endpoint_health_check_notifier.get(),
            ) {
                notifier.notify_one();
            }
        }

        // Ensure the metrics guard is not dropped until the end of the function.
        // Drop fires "request completed" log via RAII.
        drop(_inflight_guard);

        Ok(())
    }
}

// === SECTION: 测试 ===

#[cfg(test)]
mod tests {
    //! ## 测试矩阵
    //!
    //! | 测试名 | 维度 |
    //! |---|---|
    //! | `test_work_handler_metrics_new_round_trip` | 公开构造函数 |
    //! | `test_request_metrics_guard_decrements_inflight_on_drop` | RAII drop 行为 |
    //! | `test_request_metrics_guard_observes_duration` | histogram 样本计数 |
    //! | `test_error_type_label_constant_present` | 常量契约 |
    //! | `_assert_metrics_clone_send_sync` | 编译期 trait 约束 |
    use super::*;
    use prometheus::{Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, Opts};

    fn mk_metrics() -> WorkHandlerMetrics {
        WorkHandlerMetrics::new(
            IntCounter::new("t_requests_total", "help").unwrap(),
            Histogram::with_opts(HistogramOpts::new(
                "t_request_duration_seconds",
                "help",
            ))
            .unwrap(),
            IntGauge::new("t_inflight_requests", "help").unwrap(),
            IntCounter::new("t_request_bytes_total", "help").unwrap(),
            IntCounter::new("t_response_bytes_total", "help").unwrap(),
            IntCounterVec::new(
                Opts::new("t_errors_total", "help"),
                &[work_handler::ERROR_TYPE_LABEL],
            )
            .unwrap(),
            IntCounter::new("t_cancellation_total", "help").unwrap(),
        )
    }

    #[test]
    fn test_work_handler_metrics_new_round_trip() {
        let m = mk_metrics();
        m.request_counter.inc();
        m.request_bytes.inc_by(42);
        m.response_bytes.inc_by(7);
        m.cancellation_total.inc();
        assert_eq!(m.request_counter.get(), 1);
        assert_eq!(m.request_bytes.get(), 42);
        assert_eq!(m.response_bytes.get(), 7);
        assert_eq!(m.cancellation_total.get(), 1);
    }

    #[test]
    fn test_request_metrics_guard_decrements_inflight_on_drop() {
        let m = mk_metrics();
        m.inflight_requests.inc();
        assert_eq!(m.inflight_requests.get(), 1);
        {
            let _g = RequestMetricsGuard {
                inflight_requests: m.inflight_requests.clone(),
                request_duration: m.request_duration.clone(),
                start_time: Instant::now(),
                request_id: None,
            };
        }
        assert_eq!(m.inflight_requests.get(), 0, "guard drop 必须递减 inflight");
    }

    #[test]
    fn test_request_metrics_guard_observes_duration() {
        let m = mk_metrics();
        let before = m.request_duration.get_sample_count();
        {
            let _g = RequestMetricsGuard {
                inflight_requests: m.inflight_requests.clone(),
                request_duration: m.request_duration.clone(),
                start_time: Instant::now(),
                request_id: Some("rid-x".into()),
            };
        }
        let after = m.request_duration.get_sample_count();
        assert_eq!(after, before + 1, "drop 必须向 histogram 写入恰好一个样本");
    }

    #[test]
    fn test_error_type_label_constant_present() {
        // 公开常量契约: 错误类型枚举名稳定。
        assert_eq!(work_handler::ERROR_TYPE_LABEL, "error_type");
        assert_eq!(work_handler::error_types::DESERIALIZATION, "deserialization");
        assert_eq!(work_handler::error_types::GENERATE, "generate");
    }

    #[allow(dead_code)]
    fn _assert_metrics_clone_send_sync() {
        fn assert_traits<T: Clone + Send + Sync + 'static>() {}
        assert_traits::<WorkHandlerMetrics>();
    }
}
