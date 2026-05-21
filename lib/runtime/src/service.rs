// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! NATS micro service stats 抓取与聚合工具层。
//!
//! 提供 `ServiceClient` 封装对 `$SRV.STATS.<service_name>` 的广播收集，
//! 将各 Worker 节点上报的统计信息聚合为 `ServiceSet`，支持：
//! - 单播 request-reply（`unary`）
//! - 广播收集（`collect_services`）
//!
//! 与 `servicegroup/service.rs` 的职责划分：
//! - `src/service.rs`（本文件）：**抓取端** — 向 `$SRV.STATS.*` 广播、收集、聚合；
//! - `src/servicegroup/service.rs`：**注册端** — 将 servicegroup 启动为 NATS service。

use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::StreamExt;
use serde::{Deserialize, Serialize};

use crate::transports::nats;
use crate::utils::stream::until_deadline;

// ─── 核心类型 ───────────────────────────────────────────────────────────────

/// 服务发现与单播请求的统一入口。
#[derive(Clone)]
pub struct ServiceClient {
    nats_client: nats::Client,
}

/// 一次服务统计广播收集得到的所有实例集合。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ServiceSet {
    services: Vec<ServiceInfo>,
}

/// 单个 Worker 节点的服务信息（映射 NATS `$SRV.STATS` 响应）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceInfo {
    /// 服务名称，如 `pagoda_backend`。
    pub name: String,
    /// 服务实例唯一 ID。
    pub id: String,
    /// 服务版本号。
    pub version: String,
    /// 服务启动时间（字符串，不做时间运算，只用于展示）。
    pub started: String,
    /// 该服务实例暴露的端点列表。
    pub portnames: Vec<EndpointInfo>,
}

/// 单个端点信息（原 `PortnameInfo`，与 NATS stats 字段对应）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointInfo {
    /// 端点名称，如 `generate`。
    pub name: String,
    /// NATS subject，如 `ns.sg.generate-0deadbeef`。
    pub subject: String,
    /// 端点统计数据（刚启动时可能为 None）。
    #[serde(flatten)]
    pub data: Option<NatsStatsMetrics>,
}

/// NATS service 端点统计指标（直接来自 NATS `$SRV.STATS` 响应）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NatsStatsMetrics {
    /// 平均处理时长（纳秒）。
    pub average_processing_time: u64,
    /// 最后一次错误字符串（空字符串表示无错误）。
    pub last_error: String,
    /// 累计错误次数。
    pub num_errors: u64,
    /// 累计请求次数。
    pub num_requests: u64,
    /// 累计处理时长（纳秒）。
    pub processing_time: u64,
    /// NATS queue group 名称。
    pub queue_group: String,
    /// 自定义业务指标（引擎上报的 JSON 数据）。
    pub data: serde_json::Value,
}

// ─── ServiceClient ───────────────────────────────────────────────────────────

impl ServiceClient {
    /// 创建 `ServiceClient`，持有一个 NATS 客户端。
    pub fn new(nats_client: nats::Client) -> Self {
        Self { nats_client }
    }

    /// 单播 request-reply：向单个 subject 发送请求，等待一个回复。
    ///
    /// 适用于定向操作（如调用特定 Worker 的管理接口）。
    pub async fn unary(
        &self,
        subject: impl Into<String>,
        payload: impl Into<Bytes>,
    ) -> anyhow::Result<nats::Message> {
        let subject = subject.into();
        let payload_bytes: Bytes = payload.into();
        let msg = self
            .nats_client
            .request(&subject, &payload_bytes, Duration::from_secs(5))
            .await
            .map_err(|e| anyhow::anyhow!("unary request to {subject} failed: {e}"))?;
        Ok(msg)
    }

    /// 广播收集：向 `$SRV.STATS.<service_name>` 广播，
    /// 在 `timeout` 时间窗口内尽可能多地收集 Worker 响应。
    ///
    /// 设计原则：
    /// - `timeout == 0` 或 `timeout > 10s` 只记 warning，不报错；
    /// - 空 payload 静默跳过（Worker 启动早期 metrics 可能未就绪）；
    /// - 单条响应解析失败只记 debug，不中断整个收集；
    /// - 使用 `until_deadline()` 按截止时间自然结束，而非按响应条数。
    pub async fn collect_services(
        &self,
        service_name: &str,
        timeout: Duration,
    ) -> anyhow::Result<ServiceSet> {
        if timeout.is_zero() {
            tracing::warn!("collect_services called with zero timeout for service '{service_name}'");
        } else if timeout > Duration::from_secs(10) {
            tracing::warn!(
                "collect_services timeout ({timeout:?}) > 10s for service '{service_name}', \
                 this may block callers for a long time"
            );
        }

        let inner = self.nats_client.inner();
        let inbox = inner.new_inbox();
        let mut sub = inner
            .subscribe(inbox.clone())
            .await
            .map_err(|e| anyhow::anyhow!("subscribe to inbox failed: {e}"))?;

        inner
            .publish_with_reply(
                format!("$SRV.STATS.{service_name}"),
                inbox,
                Bytes::new(),
            )
            .await
            .map_err(|e| anyhow::anyhow!("publish to $SRV.STATS.{service_name} failed: {e}"))?;

        inner
            .flush()
            .await
            .map_err(|e| anyhow::anyhow!("NATS flush failed: {e}"))?;

        let deadline = Instant::now() + timeout;

        // 将 async_nats::Subscriber 转为 boxed stream
        let raw_stream = Box::pin(async_stream::stream! {
            while let Some(msg) = sub.next().await {
                yield msg;
            }
        });

        let mut timed_stream = until_deadline(raw_stream, deadline);
        let mut services = Vec::new();

        while let Some(msg) = timed_stream.next().await {
            let payload = msg.payload.as_ref();
            if payload.is_empty() {
                tracing::trace!("Skipping empty payload from $SRV.STATS.{service_name}");
                continue;
            }
            match serde_json::from_slice::<ServiceInfo>(payload) {
                Ok(info) => services.push(info),
                Err(e) => {
                    tracing::debug!(
                        "Failed to deserialize ServiceInfo from $SRV.STATS.{service_name}: {e}"
                    );
                }
            }
        }

        Ok(ServiceSet { services })
    }
}

// ─── ServiceSet ──────────────────────────────────────────────────────────────

impl ServiceSet {
    /// 创建空 ServiceSet。
    pub fn empty() -> Self {
        Self { services: Vec::new() }
    }

    /// 返回内部服务数组切片（只读访问）。
    pub fn services(&self) -> &[ServiceInfo] {
        &self.services
    }

    /// 消费 ServiceSet，将所有服务的端点列表扁平化为单个迭代器。
    ///
    /// 适合"只关心所有活跃端点"的调用方。
    pub fn into_portnames(self) -> impl Iterator<Item = EndpointInfo> {
        self.services.into_iter().flat_map(|s| s.portnames.into_iter())
    }
}

// ─── EndpointInfo ────────────────────────────────────────────────────────────

impl EndpointInfo {
    /// 从 `subject` 的最后一个 `-` 分段中提取十六进制实例 ID，转换为 `i64`。
    ///
    /// 例如：`"ns.sg.generate-deadbeef"` → `Ok(3_735_928_559)`
    pub fn id(&self) -> anyhow::Result<i64> {
        let hex_part = self
            .subject
            .rsplit('-')
            .next()
            .ok_or_else(|| anyhow::anyhow!("no '-' segment in subject '{}'", self.subject))?;
        i64::from_str_radix(hex_part, 16)
            .map_err(|e| anyhow::anyhow!("invalid hex instance id '{}': {e}", hex_part))
    }
}

// ─── NatsStatsMetrics ────────────────────────────────────────────────────────

impl NatsStatsMetrics {
    /// 将 `data` 字段的自定义 JSON 反序列化为具体类型 `T`。
    ///
    /// 标准字段保留在 `NatsStatsMetrics`，业务自定义指标通过此方法转成强类型。
    pub fn decode<T: for<'de> Deserialize<'de>>(self) -> anyhow::Result<T> {
        serde_json::from_value(self.data)
            .map_err(|e| anyhow::anyhow!("NatsStatsMetrics::decode failed: {e}"))
    }
}
