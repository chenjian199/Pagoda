// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Kubernetes 发现层工具：Pod 身份解析与哈希。

use k8s_openapi::api::discovery::v1::EndpointSlice;
use serde::{Deserialize, Serialize};

// ══════════════════════════════════════════════════════════════════════════════
// PodInfo
// ══════════════════════════════════════════════════════════════════════════════

/// 当前 Pod 的身份信息，用于构建 K8s 资源的 OwnerReference 和计算 instance_id。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PodInfo {
    pub pod_name: String,
    pub pod_namespace: String,
    pub pod_uid: String,
    pub pod_ip: String,
    /// 系统管理端口（metrics / health），从 `PGD_SYSTEM_PORT` 读取，`-1` 表示未启用。
    pub system_port: i16,
}

impl PodInfo {
    /// 从 Downward API 挂载文件或环境变量读取 Pod 身份。
    ///
    /// **文件优先于环境变量**：支持 CRIU（checkpoint/restore in userspace）场景——
    /// 被 CRIU 还原的进程环境变量中仍是源 pod 的名称，但挂载文件由 kubelet 刷新为目标 pod。
    ///
    /// 文件路径（Downward API volume）：
    /// - `/etc/podinfo/pod_name`
    /// - `/etc/podinfo/pod_uid`
    /// - `/etc/podinfo/pod_namespace`
    /// - `/etc/podinfo/pod_ip`
    ///
    /// 回退到环境变量：`POD_NAME` / `HOSTNAME`、`POD_UID`、`POD_NAMESPACE`、
    /// `POD_IP` / `PGD_TCP_RPC_HOST`。
    pub fn from_env() -> Self {
        let pod_name = read_podinfo_file("pod_name")
            .or_else(|| std::env::var("POD_NAME").ok())
            .or_else(|| std::env::var("HOSTNAME").ok())
            .unwrap_or_else(|| "unknown-pod".to_string());

        let pod_namespace = read_podinfo_file("pod_namespace")
            .or_else(|| std::env::var("POD_NAMESPACE").ok())
            .unwrap_or_else(|| "default".to_string());

        let pod_uid = read_podinfo_file("pod_uid")
            .or_else(|| std::env::var("POD_UID").ok())
            .unwrap_or_default();

        let pod_ip = read_podinfo_file("pod_ip")
            .or_else(|| std::env::var("POD_IP").ok())
            .or_else(|| std::env::var("PGD_TCP_RPC_HOST").ok())
            .unwrap_or_else(|| "0.0.0.0".to_string());

        let system_port: i16 = std::env::var("PGD_SYSTEM_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(-1);

        Self { pod_name, pod_namespace, pod_uid, pod_ip, system_port }
    }
}

/// 尝试从 `/etc/podinfo/{key}` 读取单行内容，失败时返回 `None`。
fn read_podinfo_file(key: &str) -> Option<String> {
    std::fs::read_to_string(format!("/etc/podinfo/{key}"))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

// ══════════════════════════════════════════════════════════════════════════════
// hash_pod_name
// ══════════════════════════════════════════════════════════════════════════════

/// 将 Pod 名称哈希为 53-bit 无符号整数，作为稳定的 `instance_id`。
///
/// 清除高 11 位的原因：`instance_id` 有时写入 JSON Number（ConfigMap 注解等）。
/// IEEE-754 double 仅有 53 位尾数，超过此范围的 `u64` 在 JSON roundtrip 后会丢失精度。
/// 掩码 `0x001F_FFFF_FFFF_FFFF` 确保 JSON 精度安全。
pub fn hash_pod_name(pod_name: &str) -> u64 {
    const INSTANCE_ID_MASK: u64 = 0x001F_FFFF_FFFF_FFFFu64;
    xxhash_rust::xxh64::xxh64(pod_name.as_bytes(), 0) & INSTANCE_ID_MASK
}

// ══════════════════════════════════════════════════════════════════════════════
// EndpointInfo / extract_endpoint_info
// ══════════════════════════════════════════════════════════════════════════════

/// 从 `EndpointSlice` 提取的单个 endpoint 信息。
#[derive(Debug, Clone)]
pub struct EndpointInfo {
    pub pod_ip: String,
    pub port: i32,
    pub instance_id: u64,
    pub pod_name: Option<String>,
}

/// 从 `EndpointSlice` 提取所有 `ready = true` 端点的信息。
///
/// 通过 `endpoint.conditions.ready` 确认就绪状态；
/// 通过 `endpoint.target_ref.name` 获取 pod_name，调用 `hash_pod_name` 计算 instance_id；
/// pod_name 为空或无 `target_ref` 的端点被跳过。
pub(super) fn extract_endpoint_info(slice: &EndpointSlice) -> Vec<EndpointInfo> {
    let port = slice
        .ports
        .as_ref()
        .and_then(|ports| ports.first())
        .and_then(|p| p.port);

    let mut results = Vec::new();

    for ep in &slice.endpoints {
        // 只处理 ready 端点
        let ready = ep
            .conditions
            .as_ref()
            .and_then(|c| c.ready)
            .unwrap_or(false);
        if !ready {
            continue;
        }

        let pod_name = ep.target_ref.as_ref().and_then(|tr| tr.name.clone());
        if pod_name.is_none() {
            continue;
        }

        let instance_id = hash_pod_name(pod_name.as_deref().unwrap_or(""));

        for addr in &ep.addresses {
            results.push(EndpointInfo {
                pod_ip: addr.clone(),
                port: port.unwrap_or(0),
                instance_id,
                pod_name: pod_name.clone(),
            });
        }
    }

    results
}

// ══════════════════════════════════════════════════════════════════════════════
// Tests
// ══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_pod_name_fits_53_bits() {
        let id = hash_pod_name("my-worker-pod-0");
        assert!(id <= 0x001F_FFFF_FFFF_FFFFu64, "instance_id must fit in 53 bits");
        assert_ne!(id, 0, "hash should be non-zero for non-empty input");
    }

    #[test]
    fn hash_pod_name_deterministic() {
        assert_eq!(hash_pod_name("pod-abc-123"), hash_pod_name("pod-abc-123"));
    }

    #[test]
    fn hash_pod_name_different_names_different_ids() {
        assert_ne!(hash_pod_name("pod-a"), hash_pod_name("pod-b"));
    }

    #[test]
    fn extract_endpoint_info_skips_not_ready() {
        let mut slice = EndpointSlice {
            address_type: "IPv4".into(),
            endpoints: vec![k8s_openapi::api::discovery::v1::Endpoint {
                addresses: vec!["10.0.0.1".into()],
                conditions: Some(k8s_openapi::api::discovery::v1::EndpointConditions {
                    ready: Some(false),
                    ..Default::default()
                }),
                target_ref: Some(k8s_openapi::api::core::v1::ObjectReference {
                    name: Some("pod-1".into()),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(extract_endpoint_info(&slice).is_empty());

        // mark ready
        slice.endpoints[0].conditions.as_mut().unwrap().ready = Some(true);
        assert_eq!(extract_endpoint_info(&slice).len(), 1);
    }
}
