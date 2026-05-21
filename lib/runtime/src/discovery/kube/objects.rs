// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Kubernetes 资源对象 ↔ DiscoveryInstance 映射。
//!
//! 读方向：从 K8s API 返回的 Service / EndpointSlice / ConfigMap / Lease
//! 转换为统一的 `DiscoveryInstance`。
//!
//! 写方向：从 `DiscoveryMetadata` 中的注册信息生成 K8s 资源并 apply。

use k8s_openapi::api::coordination::v1::Lease;
use k8s_openapi::api::core::v1::{ConfigMap, Service};
use k8s_openapi::api::discovery::v1::EndpointSlice;

use crate::discovery::{DiscoveryInstance, EventTransport};

// ──────────────────────────────────────────────
// Read direction: K8s objects → DiscoveryInstance
// ──────────────────────────────────────────────

/// 从 headless Service + EndpointSlice 提取 PortName 实例列表。
///
/// 一个 EndpointSlice 可包含多个 endpoint address，每个对应一个 Pod 实例。
pub fn endpoint_instance_from_service_and_slice(
    _service: &Service,
    _slice: &EndpointSlice,
) -> Vec<DiscoveryInstance> {
    // K8s 集成存根：需 EndpointSlice API 解析，暂返回空集合。
    vec![]
}

/// 从 ConfigMap 提取 Model card 实例。
///
/// ConfigMap data 中包含 `card.json` 和 `topo.json` 字段。
pub fn model_card_instance_from_config_map(
    _config_map: &ConfigMap,
) -> Option<DiscoveryInstance> {
    // K8s 集成存根：需解析 ConfigMap data[card.json]/[topo.json]，暂返回 None。
    None
}

/// 从 Lease 提取 EventChannel 实例。
///
/// Lease annotations 包含 transport 类型和地址信息。
pub fn event_instance_from_lease(
    _lease: &Lease,
) -> Option<DiscoveryInstance> {
    // K8s 集成存根：需解析 Lease annotations，暂返回 None。
    None
}

// ──────────────────────────────────────────────
// Write direction: DiscoveryMetadata → K8s objects
// ──────────────────────────────────────────────

/// 构建并 server-side apply Model ConfigMap。
///
/// ConfigMap 名称格式：`pagoda-model-{servicegroup}-{portname}-{instance_id_hex}`
pub async fn apply_model_config_map(
    _client: &kube::Client,
    _namespace: &str,
    _servicegroup: &str,
    _portname: &str,
    _instance_id: u64,
    _card_json: &serde_json::Value,
    _model_suffix: Option<&str>,
    _topo_json: &serde_json::Value,
) -> anyhow::Result<()> {
    // K8s 集成存根：SSA patch 实现待补充。
    Ok(())
}

/// 构建并 server-side apply EventChannel Lease。
///
/// Lease 名称格式：`pagoda-event-{servicegroup}-{topic}-{instance_id_hex}`
pub async fn apply_event_lease(
    _client: &kube::Client,
    _namespace: &str,
    _servicegroup: &str,
    _topic: &str,
    _instance_id: u64,
    _transport: &EventTransport,
) -> anyhow::Result<()> {
    // K8s 集成存根：SSA patch 实现待补充。
    Ok(())
}
