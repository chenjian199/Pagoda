// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Kubernetes Service / EndpointSlice 注册构建器。
//!
//! 将 PortName 注册信息转换为 K8s 原生资源对象，供 daemon apply 到集群。

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::Service;
use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::api::ObjectMeta;

use super::utils::PodInfo;
use crate::servicegroup::TransportType;

/// PortName 注册描述：daemon 据此生成 Service + EndpointSlice。
#[derive(Debug, Clone)]
pub struct ServiceRegistration {
    pub namespace: String,
    pub servicegroup: String,
    pub portname: String,
    pub instance_id: u64,
    pub transport: TransportType,
    pub pod_info: PodInfo,
}

impl ServiceRegistration {
    /// 生成 K8s Service 资源名称。
    ///
    /// 格式：`pagoda-{servicegroup}-{portname}`，截断到 63 字符。
    pub fn service_name(&self) -> String {
        let raw = format!("pagoda-{}-{}", self.servicegroup, self.portname);
        if raw.len() > 63 {
            raw[..63].to_string()
        } else {
            raw
        }
    }

    /// 生成 EndpointSlice 资源名称。
    ///
    /// 格式：`{service_name}-{pod_name_hash_hex}`
    pub fn endpoint_slice_name(&self) -> String {
        let svc = self.service_name();
        let hash = super::utils::hash_pod_name(&self.pod_info.pod_name);
        format!("{svc}-{hash:x}")
    }
}

/// 构建 headless Service 对象（ClusterIP=None）。
///
/// Labels:
/// - `pagoda.io/namespace`: 业务命名空间
/// - `pagoda.io/servicegroup`: 服务组
/// - `pagoda.io/portname`: 端点名
pub fn build_service(reg: &ServiceRegistration) -> Service {
    let labels = pagoda_labels(&reg.namespace, &reg.servicegroup, &reg.portname);

    Service {
        metadata: ObjectMeta {
            name: Some(reg.service_name()),
            namespace: Some(reg.pod_info.pod_namespace.clone()),
            labels: Some(labels.clone()),
            ..Default::default()
        },
        spec: Some(k8s_openapi::api::core::v1::ServiceSpec {
            cluster_ip: Some("None".to_string()),
            selector: Some(labels),
            ports: Some(vec![k8s_openapi::api::core::v1::ServicePort {
                name: Some(reg.portname.clone()),
                port: transport_port(&reg.transport),
                protocol: Some("TCP".to_string()),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// 构建 EndpointSlice 对象（addressType=IPv4）。
pub fn build_endpoint_slice(reg: &ServiceRegistration) -> EndpointSlice {
    let labels = pagoda_labels(&reg.namespace, &reg.servicegroup, &reg.portname);

    let mut all_labels = labels;
    all_labels.insert(
        "kubernetes.io/service-name".to_string(),
        reg.service_name(),
    );

    EndpointSlice {
        metadata: ObjectMeta {
            name: Some(reg.endpoint_slice_name()),
            namespace: Some(reg.pod_info.pod_namespace.clone()),
            labels: Some(all_labels),
            owner_references: Some(vec![]),
            ..Default::default()
        },
        address_type: "IPv4".to_string(),
        endpoints: vec![k8s_openapi::api::discovery::v1::Endpoint {
            addresses: vec![reg.pod_info.pod_ip.clone()],
            conditions: Some(k8s_openapi::api::discovery::v1::EndpointConditions {
                ready: Some(true),
                serving: Some(true),
                terminating: Some(false),
            }),
            target_ref: Some(k8s_openapi::api::core::v1::ObjectReference {
                kind: Some("Pod".to_string()),
                name: Some(reg.pod_info.pod_name.clone()),
                namespace: Some(reg.pod_info.pod_namespace.clone()),
                uid: Some(reg.pod_info.pod_uid.clone()),
                ..Default::default()
            }),
            ..Default::default()
        }],
        ports: Some(vec![k8s_openapi::api::discovery::v1::EndpointPort {
            name: Some(reg.portname.clone()),
            port: Some(transport_port(&reg.transport)),
            protocol: Some("TCP".to_string()),
            ..Default::default()
        }]),
    }
}

fn pagoda_labels(namespace: &str, servicegroup: &str, portname: &str) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    m.insert("pagoda.io/namespace".to_string(), namespace.to_string());
    m.insert("pagoda.io/servicegroup".to_string(), servicegroup.to_string());
    m.insert("pagoda.io/portname".to_string(), portname.to_string());
    m
}

fn transport_port(transport: &TransportType) -> i32 {
    // 从地址字符串提取端口，如 "host:8080" → 8080
    let addr = match transport {
        TransportType::Tcp(address) => address.as_str(),
        TransportType::Nats(_) | TransportType::Http(_) => return 0,
    };
    addr.rsplit(':')
        .next()
        .and_then(|p| p.parse::<i32>().ok())
        .unwrap_or(0)
}

// ══════════════════════════════════════════════════════════════════════════════
// Server-Side Apply 辅助
// ══════════════════════════════════════════════════════════════════════════════

const FIELD_MANAGER: &str = "pagoda-worker";

/// Server-Side Apply：create-or-update headless Service。
pub async fn apply_service(
    kube_client: &kube::Client,
    namespace: &str,
    service: &k8s_openapi::api::core::v1::Service,
) -> anyhow::Result<()> {
    use kube::api::{Api, Patch, PatchParams};
    let api: Api<k8s_openapi::api::core::v1::Service> =
        Api::namespaced(kube_client.clone(), namespace);
    let name = service
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("Service missing metadata.name"))?;
    api.patch(
        name,
        &PatchParams::apply(FIELD_MANAGER).force(),
        &Patch::Apply(service),
    )
    .await
    .map_err(|e| anyhow::anyhow!("failed to apply Service {name}: {e}"))?;
    Ok(())
}

/// Server-Side Apply：create-or-update pod-owned EndpointSlice。
pub async fn apply_endpoint_slice(
    kube_client: &kube::Client,
    namespace: &str,
    slice: &k8s_openapi::api::discovery::v1::EndpointSlice,
) -> anyhow::Result<()> {
    use kube::api::{Api, Patch, PatchParams};
    let api: Api<k8s_openapi::api::discovery::v1::EndpointSlice> =
        Api::namespaced(kube_client.clone(), namespace);
    let name = slice
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("EndpointSlice missing metadata.name"))?;
    api.patch(
        name,
        &PatchParams::apply(FIELD_MANAGER).force(),
        &Patch::Apply(slice),
    )
    .await
    .map_err(|e| anyhow::anyhow!("failed to apply EndpointSlice {name}: {e}"))?;
    Ok(())
}
