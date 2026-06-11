// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! NATS 微服务发现与状态集（service 模块）
//!
//! ## 设计意图
//! 为调用方封装三件事：
//! 1. [`ServiceClient`] 提供基于 NATS 的请求 / 应答与服务发现拉取；
//! 2. [`ServiceSet`] / [`ServiceInfo`] / [`PortNameInfo`] / [`NatsStatsMetrics`]
//!    描述一次“$SRV.STATS.<service>” 采集的原始结果集；
//! 3. 辅助函数（为 [`PortNameInfo::id`] 提供的 hex 后缀解析、为 `collect_services`
//!    提供的消息反序列化）集中为私有助手，让主路径保持函数式、上下文纯净。
//!
//! TODO：本模块后续仍需重构以明确“组件 live / ready 状态”与取消令牌间的关联。
//!
//! ## 外部契约
//! - 公开结构体：`ServiceClient`、`ServiceSet`、`ServiceInfo`、`PortNameInfo`、`NatsStatsMetrics`；
//!   后三者均为 `Debug + Clone + Serialize + Deserialize`，`PortNameInfo` / `NatsStatsMetrics`
//!   额外提供 `derive_getters::Dissolve`。字段与序列化格式保持不变（NATS 线上格式契约）。
//! - 公开方法集合 `ServiceClient::new` / `unary` / `collect_services`、
//!   `PortNameInfo::id`、`NatsStatsMetrics::decode`、`ServiceSet::into_portnames` / `services` 签名不变。
//! - `PortNameInfo::id` 语义：从 subject 末尾 `-` 后的部分按十六进制解析为 `i64`；
//!   缺失 / 空 / 非十六进制均返回 `anyhow::Error`，错误文本与历史一致。
//! - `collect_services` 语义：在 `timeout` 截止前收集所有消息，忽略空 payload，
//!   反序列化失败记 `debug` 日志但继续，timeout 为零 / 超过 10s 都会额外发 `warn`。
//!
//! ## 实现要点
//! - **多样化（Rule 2）**：
//!   * `PortNameInfo::id` 抽出私有助手 `extract_hex_suffix`，把“取后缀”与
//!     “hex 解析”拆成两步，使主函数只负责错误包装；
//!   * `collect_services` 主环改为 `while let Some(message) = s.next().await`，并抽出
//!     `try_parse_service_info` 助手负责“空 payload 跳过 + 解析失败记日志 + 返回 Option”；
//!     主环只需 `if let Some(info) = ...` 推入 `Vec`；
//!   * `into_portnames` 由 `.map(...).flatten()` 改为 `.flat_map(...)`，仅为习惯性表达。
//! - **不**变动任何错误文本、`try_stream!` 框架、NATS 调用路径与日志级别，
//!   以保证对外可观察行为保持稳定。

// TODO: 整个模块后续仍需重构。
//
// 这里希望保留组件 `live` 与 `ready` 两种状态的语义，
// 并把组件的取消令牌与其服务状态关联起来。

use crate::{
    DistributedRuntime,
    servicegroup::ServiceGroup,
    metrics::{MetricsHierarchy, prometheus_names},
    traits::*,
    transports::nats,
    utils::stream,
};

use anyhow::Result;
use anyhow::anyhow as error;
use async_nats::Message;
use async_stream::try_stream;
use bytes::Bytes;
use derive_getters::Dissolve;
use futures::stream::{StreamExt, TryStreamExt};
use prometheus;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::time::Duration;

// === SECTION: ServiceClient 与顶层结构 ===

pub struct ServiceClient {
    nats_client: nats::Client,
}

impl ServiceClient {
    /// 创建一个基于 NATS 客户端的服务查询客户端。
    pub fn new(nats_client: nats::Client) -> Self {
        Self { nats_client }
    }
}

/// `ServiceSet` 表示一组服务及其端点和指标信息的集合。
///
/// 树状结构如下：
/// - ServiceSet
///   - services: Vec<ServiceInfo>
///     - name: String
///     - id: String
///     - version: String
///     - started: String
///     - portnames: Vec<PortNameInfo>
///       - name: String
///       - subject: String
///       - data: Option<NatsStatsMetrics>
///         - average_processing_time: f64
///         - last_error: String
///         - num_errors: u64
///         - num_requests: u64
///         - processing_time: u64
///         - queue_group: String
///         - data: serde_json::Value (custom stats)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceSet {
    services: Vec<ServiceInfo>,
}

/// 下面是 `nats req '$SRV.STATS.pagoda_backend'` 返回 JSON 的示例：
/// {
///   "type": "io.nats.micro.v1.stats_response",
///   "name": "pagoda_backend",
///   "id": "bdu7nA8tbhy9mEkxIWlkBA",
///   "version": "0.0.1",
///   "started": "2025-08-08T05:07:17.720783523Z",
///   "portnames": [
///     {
///       "name": "pagoda_backend-generate-694d988806b92e39",
///       "subject": "pagoda_backend.generate-694d988806b92e39",
///       "num_requests": 0,
///       "num_errors": 0,
///       "processing_time": 0,
///       "average_processing_time": 0,
///       "last_error": "",
///       "data": {
///         "val": 10
///       },
///       "queue_group": "q"
///     }
///   ]
/// }
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceInfo {
    pub name: String,
    pub id: String,
    pub version: String,
    pub started: String,
    /// NATS `$SRV.STATS` wire format uses `"endpoints"`; keep Rust field name aligned with Pagoda terminology.
    #[serde(alias = "endpoints")]
    pub portnames: Vec<PortNameInfo>,
}

/// 每个端点都包含名称、subject、请求统计以及扩展数据等字段。
#[derive(Debug, Clone, Serialize, Deserialize, Dissolve)]
pub struct PortNameInfo {
    pub name: String,
    pub subject: String,

    /// 不属于 `PortNameInfo` 固定字段的额外内容，会被展平到指标结构中。
    #[serde(flatten)]
    pub data: Option<NatsStatsMetrics>,
}

// === SECTION: PortNameInfo / NatsStatsMetrics 语义补充 ===

/// 私有助手：从 subject 中提取“最后一个 `-` 后的 hex 后缀”并按 16 进制解析为 `i64`。
///
/// 这个助手把“取后缀”与“hex 解析”拆成两步，令 [`PortNameInfo::id`] 主体只需
/// 负责错误消息包装。错误文本与历史实现严格保持一致：
/// * 缺失 / 空后缀 → `"No id found in subject"`
/// * 非十六进制 → `"Invalid id format: <ParseIntError>"`
fn extract_hex_suffix(subject: &str) -> Result<i64> {
    let suffix = subject
        .rsplit_once('-')
        .map(|(_, value)| value)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| error!("No id found in subject"))?;

    i64::from_str_radix(suffix, 16).map_err(|err| error!("Invalid id format: {}", err))
}

impl PortNameInfo {
    /// 从端点 subject 的十六进制后缀中提取实例 ID。
    ///
    /// 处理流程与历史实现等价：先按最后一个 `-` 分割出后缀，再按十六进制解析为 `i64`。
    /// 实现上委托给私有助手 [`extract_hex_suffix`]，使错误路径集中、便于独立复用。
    pub fn id(&self) -> Result<i64> {
        extract_hex_suffix(&self.subject)
    }
}

// TODO: 这个结构与 `async_nats::service::Stats` 已经非常接近，
// 但仍缺少如 `name` 等字段，因此暂时保留一个本地结构用于反序列化。
// 理想情况下，这个类型应由上游库直接暴露。
/// NATS Service API 返回的统计结构。
/// https://github.com/nats-io/nats.rs/blob/main/async-nats/src/service/portname.rs
#[derive(Debug, Clone, Serialize, Deserialize, Dissolve)]
pub struct NatsStatsMetrics {
    // 这些字段来自 `$SRV.STATS.<service_name>` 请求返回的标准 NATS 统计信息。
    pub average_processing_time: u64, // 按 nats-io 约定，单位为纳秒。
    pub last_error: String,
    pub num_errors: u64,
    pub num_requests: u64,
    pub processing_time: u64, // 按 nats-io 约定，单位为纳秒。
    pub queue_group: String,
    // 自定义统计处理器返回的数据负载。
    pub data: serde_json::Value,
}

impl NatsStatsMetrics {
    /// 将自定义 `data` 字段反序列化为目标类型，便于业务端直接读取结构化指标。
    pub fn decode<T: for<'de> Deserialize<'de>>(self) -> Result<T> {
        let payload = self.data;
        Ok(serde_json::from_value(payload)?)
    }
}

// === SECTION: ServiceClient 调用与服务发现 ===

/// 私有助手：安全反序列化单条 NATS 服务发现消息。
///
/// 语义与历史实现严格一致：
/// * `payload` 为空 → 输出 `trace` 日志后返回 `None`；
/// * 解析失败 → 输出 `debug` 日志（携带错误、服务名与原始负载）后返回 `None`；
/// * 解析成功 → 返回 `Some(ServiceInfo)`。
fn try_parse_service_info(payload: &[u8], service_name: &str) -> Option<ServiceInfo> {
    if payload.is_empty() {
        tracing::trace!(service_name, "collect_services: empty payload from nats");
        return None;
    }

    match serde_json::from_slice::<ServiceInfo>(payload) {
        Ok(info) => Some(info),
        Err(err) => {
            let payload_text = String::from_utf8_lossy(payload);
            tracing::debug!(%err, service_name, %payload_text, "error decoding service info");
            None
        }
    }
}

impl ServiceClient {
    /// 发送一次 NATS request-reply 调用，并返回远端消息响应。
    pub async fn unary(
        &self,
        subject: impl Into<String>,
        payload: impl Into<Bytes>,
    ) -> Result<Message> {
        let target = subject.into();
        let body = payload.into();

        self.nats_client
            .client()
            .request(target, body)
            .await
            .map_err(Into::into)
    }

    /// 从 NATS 服务发现接口拉取指定服务在超时时间内返回的所有实例信息。
    ///
    /// 处理流程（与历史语义等价）：订阅服务统计流、按截止时间消费消息、
    /// 跳过空 payload、把可解析的条目反序列化为 `ServiceInfo`，最后组装成 `ServiceSet` 返回。
    ///
    /// 实现上：主环使用 `while let Some(message) = s.next().await` 表达“消费到流结束”；
    /// 单条消息的“空 payload 跳过 + 解析失败记日志”逻辑下沉到私有助手
    /// [`try_parse_service_info`]，主高交流程保持函数式、无多重嵌套。
    pub async fn collect_services(
        &self,
        service_name: &str,
        timeout: Duration,
    ) -> Result<ServiceSet> {
        let sub = self.nats_client.scrape_service(service_name).await?;
        if timeout.is_zero() {
            tracing::warn!("collect_services: timeout is zero");
        }
        if timeout > Duration::from_secs(10) {
            tracing::warn!("collect_services: timeout is greater than 10 seconds");
        }
        let deadline = tokio::time::Instant::now() + timeout;

        let mut services = Vec::new();
        let mut s = stream::until_deadline(sub, deadline);
        while let Some(message) = s.next().await {
            if let Some(info) = try_parse_service_info(&message.payload, service_name) {
                services.push(info);
            }
        }

        Ok(ServiceSet { services })
    }
}

// === SECTION: ServiceSet 访问器 ===

impl ServiceSet {
    /// 展平所有服务下的端点列表，返回一个按需迭代器。
    ///
    /// 实现上以 `flat_map` 代替历史的 `.map(...).flatten()` 两步拼接，
    /// 语义等价、迭代顺序与动态行为一致。
    pub fn into_portnames(self) -> impl Iterator<Item = PortNameInfo> {
        self.services
            .into_iter()
            .flat_map(|service| service.portnames)
    }

    /// 返回当前 `ServiceSet` 内部保存的服务切片引用。
    pub fn services(&self) -> &[ServiceInfo] {
        self.services.as_slice()
    }
}

// === SECTION: 单元测试 ===

#[cfg(test)]
mod tests {
    //! ## 测试过程
    //! 不依赖 NATS：手动构造 `ServiceSet` / `PortNameInfo` / `NatsStatsMetrics` 样本，
    //! 验证 `into_portnames` 的展平、`PortNameInfo::id` 在合法 / 非十六进制后缀上的双路径、
    //! `NatsStatsMetrics::decode` 的泛型解码，以及 `services()` 访问器的引用返回。
    //!
    //! ## 意义
    //! 这些用例固定了 service 模块面向上层的几个关键可观察行为：subject 后缀解析、
    //! NATS 拓扑展平、自定义 stats payload 解码。本次重构（中文文档、
    //! `extract_hex_suffix` / `try_parse_service_info` 助手抽取、`flat_map` 习惯性）必须保证
    //! 上述断言全部保持为真。

    use super::*;

    #[derive(Debug, Deserialize, PartialEq)]
    struct MetricsPayload {
        key: String,
    }

    /// 构造测试用指标数据，便于验证解码与聚合逻辑。
    fn sample_metrics(value: &str) -> NatsStatsMetrics {
        NatsStatsMetrics {
            average_processing_time: 100_000,
            last_error: "none".to_string(),
            num_errors: 0,
            num_requests: 10,
            processing_time: 100,
            queue_group: "group1".to_string(),
            data: serde_json::json!({"key": value}),
        }
    }

    #[test]
    /// 测试：`ServiceSet::into_portnames` 会把不同服务中的端点展平输出。
    fn test_service_set() {
        let services = vec![
            ServiceInfo {
                name: "service1".to_string(),
                id: "1".to_string(),
                version: "1.0".to_string(),
                started: "2021-01-01".to_string(),
                portnames: vec![
                    PortNameInfo {
                        name: "portname1".to_string(),
                        subject: "subject1".to_string(),
                        data: Some(sample_metrics("value1")),
                    },
                    PortNameInfo {
                        name: "portname2-foo".to_string(),
                        subject: "subject2".to_string(),
                        data: Some(sample_metrics("value1")),
                    },
                ],
            },
            ServiceInfo {
                name: "service1".to_string(),
                id: "2".to_string(),
                version: "1.0".to_string(),
                started: "2021-01-01".to_string(),
                portnames: vec![
                    PortNameInfo {
                        name: "portname1".to_string(),
                        subject: "subject1".to_string(),
                        data: Some(sample_metrics("value1")),
                    },
                    PortNameInfo {
                        name: "portname2-bar".to_string(),
                        subject: "subject2".to_string(),
                        data: Some(sample_metrics("value2")),
                    },
                ],
            },
        ];

        let service_set = ServiceSet { services };

        let portnames: Vec<_> = service_set
            .into_portnames()
            .filter(|e| e.name.starts_with("portname2"))
            .collect();

        assert_eq!(portnames.len(), 2);
    }

    #[test]
    /// 测试：`PortNameInfo::id` 能正确解析十六进制后缀。
    fn test_portname_info_id_parses_hex_suffix() {
        let portname = PortNameInfo {
            name: "portname".to_string(),
            subject: "service.generate-deadbeef".to_string(),
            data: None,
        };

        assert_eq!(portname.id().unwrap(), 0xdeadbeef);
    }

    #[test]
    /// 测试：`PortNameInfo::id` 在后缀不是十六进制时返回错误。
    fn test_portname_info_id_rejects_invalid_hex_suffix() {
        let portname = PortNameInfo {
            name: "portname".to_string(),
            subject: "service.generate-not-hex".to_string(),
            data: None,
        };

        assert!(portname.id().is_err());
    }

    #[test]
    /// 测试：`NatsStatsMetrics::decode` 可以把自定义数据解码成目标结构。
    fn test_nats_stats_metrics_decode_typed_payload() {
        let decoded: MetricsPayload = sample_metrics("decoded").decode().unwrap();

        assert_eq!(decoded, MetricsPayload { key: "decoded".to_string() });
    }

    #[test]
    /// 测试：NATS `$SRV.STATS` wire JSON 使用 `endpoints` 字段名时仍能反序列化到 `portnames`。
    fn test_service_info_deserializes_nats_endpoints_field() {
        let payload = r#"{
            "type": "io.nats.micro.v1.stats_response",
            "name": "pagoda_backend",
            "id": "svc-id",
            "version": "0.0.1",
            "started": "2025-08-08T05:07:17.720783523Z",
            "endpoints": [
                {
                    "name": "pagoda_backend-generate-694d988806b92e39",
                    "subject": "pagoda_backend.generate-694d988806b92e39",
                    "num_requests": 0,
                    "num_errors": 0,
                    "processing_time": 0,
                    "average_processing_time": 0,
                    "last_error": "",
                    "queue_group": "q"
                }
            ]
        }"#;
        let info: ServiceInfo = serde_json::from_str(payload).unwrap();
        assert_eq!(info.portnames.len(), 1);
        assert_eq!(info.portnames[0].name, "pagoda_backend-generate-694d988806b92e39");
    }

    #[test]
    /// 测试：`services()` 访问器会返回原始服务切片内容。
    fn test_services_accessor_returns_original_slice() {
        let service_set = ServiceSet {
            services: vec![ServiceInfo {
                name: "service-a".to_string(),
                id: "1".to_string(),
                version: "1.0".to_string(),
                started: "2021-01-01".to_string(),
                portnames: vec![],
            }],
        };

        assert_eq!(service_set.services().len(), 1);
        assert_eq!(service_set.services()[0].name, "service-a");
    }
}
