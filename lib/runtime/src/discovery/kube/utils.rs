// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # Kube 发现子系统的工具函数集合
//!
//! ## 设计意图
//!
//! 本模块是 `discovery::kube` 命名空间下的**纯函数层**，集中收纳所有不依赖
//! 业务上下文、不持有 K8s 客户端、不产生副作用的小工具。把这些片段集中在一处
//! 而不是散布到 `objects.rs` / `service_registry.rs` 中，有两个直接好处：
//!
//! 1. **去重**：`sanitize` / `short_hash` 这类被多处调用的函数只需定义一次；
//! 2. **可移植**：升级 K8s 原生注册路径时，对象层只需 `use super::utils::{...}`
//!    即可获得统一的命名规则，避免不同模块各自实现差异化版本。
//!
//! ## 外部契约
//!
//! - [`hash_pod_name`]：把 Pod 名称压缩到 53 位 instance_id（C bindings/EPP 使用）。
//! - [`sanitize`]：把任意字符串规范化为 K8s DNS label 兼容的小写串，并按长度截断。
//! - [`short_hash`]：把任意字符串映射为 8 位十六进制串，用于在 DNS 名称中
//!   引入“不可逆但确定性”的去重后缀。
//! - [`KubeDiscoveryMode`] / [`KubeDiscoveryTarget`] / [`PodInfo`]：仅在
//!   `kube` 子模块内部使用，供 `daemon` / 入口客户端读取 Pod 自身身份。
//! - 工具函数 [`extract_portname_info`] / [`extract_ready_containers`]：从
//!   `EndpointSlice` / `Pod` 抽取就绪条目，daemon 用其聚合快照。
//!
//! ## 实现要点
//!
//! - 所有公开 API 版本完全一致；新增的 [`sanitize`] / [`short_hash`]
//!   以及 [`PodInfo::pod_ip`] 字段是为了支撑 K8s 原生注册路径
//!   （`Service` + `EndpointSlice`）所需的命名 / 地址写入。
//! - 53 位掩码 [`INSTANCE_ID_MASK`] 来源于 JavaScript Number 的安全整数范围，
//!   保证 JSON 序列化后下游 (TS / JS) 不会发生整数精度丢失。
//! - 字符规范化沿用 K8s DNS-1123 label 规则：只允许 `[a-z0-9-]`，首尾必须是
//!   字母数字字符，长度 ≤ 63。

use anyhow::Result;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::api::discovery::v1::EndpointSlice;
use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::Path;

use crate::config::environment_names::discovery;

// === 常量 ====================================================================

/// 把 `u64` 哈希压缩到 JavaScript 安全整数（53 位）范围的掩码。
///
/// 下游路由器、UI 仪表板等组件会以 JSON `number` 形式读取 `instance_id`，
/// 而 JSON `number` 在 JS 端被解析为 IEEE-754 `f64`，安全整数上限是 `2^53-1`。
/// 超出该范围会发生精度丢失，导致两个不同的 instance 看起来 ID 相同。
const INSTANCE_ID_MASK: u64 = 0x001F_FFFF_FFFF_FFFFu64;

/// 容器模式下识别“主容器”的固定名称（与 helm chart 中 main container 命名一致）。
///
/// 当 mode=Container 且容器名等于 `main` 时，CR 名称退化为 pod 名称，
/// 与 mode=Pod 行为保持兼容，便于平滑切换两种模式。
const MAIN_CONTAINER_NAME: &str = "main";

/// 默认从 Downward API 卷挂载读取 pod 身份的目录。
const DEFAULT_PODINFO_PATH: &str = "/etc/podinfo";

// === 公共工具函数 ============================================================

/// 用 [`DefaultHasher`] 把 Pod 名映射到 53 位的 instance_id。
///
/// ## 为什么需要这个公开函数？
///
/// C bindings（EPP，PortName Picker Plugin）会跨 FFI 边界访问 Pod 到 worker
/// 的映射，必须与 Rust 端使用**完全相同的算法**，否则前后端无法对齐。
/// 抽出为单独函数确保两侧只有一处可信定义。
pub fn hash_pod_name(pod_name: &str) -> u64 {
    let mut h = DefaultHasher::new();
    pod_name.hash(&mut h);
    h.finish() & INSTANCE_ID_MASK
}

/// 把任意字符串规范化为 DNS-1123 label 兼容片段，并截断到 `max_len`。
///
/// ## 规则
///
/// 1. 全部小写。
/// 2. 非 `[a-z0-9-]` 字符替换为 `-`。
/// 3. 截断到 `max_len` 字符（K8s 单个 label 段上限 63，调用方按需选择）。
/// 4. 去掉前导 / 尾部的 `-`，确保首尾为字母数字（K8s 强制要求）。
/// 5. 截断后若为空字符串，回退为 `"x"`，避免上层构造出非法的 K8s 资源名。
///
/// ## 实现要点
///
/// 没有用正则，而是直接遍历字符以避免在热路径中引入正则编译开销；
/// 同时一遍循环既完成大小写转换又完成非法字符替换。
pub(super) fn sanitize(input: &str, max_len: usize) -> String {
    let mut out = String::with_capacity(input.len().min(max_len));
    for c in input.chars() {
        if out.len() == max_len {
            break;
        }
        let mapped = match c {
            'A'..='Z' => c.to_ascii_lowercase(),
            'a'..='z' | '0'..='9' | '-' => c,
            _ => '-',
        };
        out.push(mapped);
    }
    // 去掉首尾连字符
    let trimmed: &str = out.trim_matches('-');
    if trimmed.is_empty() {
        "x".to_owned()
    } else {
        trimmed.to_owned()
    }
}

/// 把任意字符串映射为 8 位十六进制串，作为 K8s 名称中的“去重后缀”。
///
/// ## 用途
///
/// 同一 Pod 上的多个 `(servicegroup, portname)` 二元组共享 Service，但每个
/// 三元组 `(service, port, pod)` 都需要唯一的 EndpointSlice 名称。
/// 在 sanitize 后的可读片段之后追加这 8 位 hash，可以在不增加显著长度的
/// 前提下避免不同三元组的名称发生碰撞。
///
/// ## 实现要点
///
/// 使用 [`DefaultHasher`] 取低 32 位再格式化为 8 位 hex，可在保证可读性的
/// 同时给出 2^32 量级的去重空间，足以应对单 namespace 内成千上万的 Pod。
pub(super) fn short_hash(input: &str) -> String {
    let mut h = DefaultHasher::new();
    input.hash(&mut h);
    format!("{:08x}", (h.finish() as u32))
}

// === 发现模式与目标 ===========================================================

/// `KubeDiscoveryClient` 的发现粒度。
///
/// - [`KubeDiscoveryMode::Pod`]：默认模式，一个 Pod 对应一个发现实体。
/// - [`KubeDiscoveryMode::Container`]：每个 Sidecar 容器独立注册，适用于
///   单 Pod 多副本场景。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum KubeDiscoveryMode {
    Pod,
    Container,
}

impl KubeDiscoveryMode {
    /// 从环境变量 `PGD_KUBE_DISCOVERY_MODE` 解析模式。
    ///
    /// 缺省或非法值会回退到 `Pod`，避免在未配置环境变量的开发环境直接 panic。
    pub fn from_env() -> Result<Self> {
        match std::env::var(discovery::PGD_KUBE_DISCOVERY_MODE).as_deref() {
            Ok("container") => Ok(Self::Container),
            Ok("pod") | Err(_) => Ok(Self::Pod),
            Ok(other) => anyhow::bail!(
                "Invalid PGD_KUBE_DISCOVERY_MODE value '{}'. Valid values: 'pod', 'container'",
                other
            ),
        }
    }
}

/// 资源命名所依据的 Pod / Container 身份。
///
/// 这是 daemon 聚合阶段把“原始 K8s 对象”映射回“Pagoda 实例”的核心键。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum KubeDiscoveryTarget {
    Pod(String),
    Container(String, String),
}

impl KubeDiscoveryTarget {
    /// 历史上 CR 名称，现在仍作为 `(instance_id, key)` 配对中的字符串键。
    pub fn cr_name(&self) -> String {
        match self {
            Self::Pod(pod) => pod.clone(),
            Self::Container(pod, cname) if cname == MAIN_CONTAINER_NAME => pod.clone(),
            Self::Container(pod, cname) => format!("{}-{}", pod, cname),
        }
    }

    /// 由 `cr_name` 哈希产出的稳定 instance_id。
    pub fn instance_id(&self) -> u64 {
        hash_pod_name(&self.cr_name())
    }

    /// 持有方 Pod 的名称（供 owner reference / 日志使用）。
    pub fn pod_name(&self) -> &str {
        match self {
            Self::Pod(p) | Self::Container(p, _) => p.as_str(),
        }
    }
}

// === EndpointSlice / Pod -> 就绪条目 ==========================================

/// 从一个 EndpointSlice 抽取 `(instance_id, key)` 二元组，仅保留 `ready=true`。
pub(super) fn extract_portname_info(slice: &EndpointSlice) -> Vec<(u64, String)> {
    slice
        .endpoints
        .iter()
        .filter_map(|ep| {
            // 只考虑明确 ready 的条目；缺省视为未就绪以稳妥处理
            let ready = ep.conditions.as_ref().and_then(|c| c.ready).unwrap_or(false);
            if !ready {
                return None;
            }
            // target_ref 缺失或 name 为空都视为无效条目
            let pod = ep.target_ref.as_ref().and_then(|r| r.name.as_deref())?;
            if pod.is_empty() {
                return None;
            }
            let target = KubeDiscoveryTarget::Pod(pod.to_owned());
            Some((target.instance_id(), target.cr_name()))
        })
        .collect()
}

/// 从一个 Pod 抽取**所有就绪容器**对应的 `(instance_id, key)` 二元组。
pub(super) fn extract_ready_containers(pod: &Pod) -> Vec<(u64, String)> {
    let Some(pod_name) = pod.metadata.name.as_deref() else {
        return Vec::new();
    };
    let Some(statuses) = pod.status.as_ref().and_then(|s| s.container_statuses.as_ref()) else {
        return Vec::new();
    };
    statuses
        .iter()
        .filter(|cs| cs.ready)
        .map(|cs| {
            let t = KubeDiscoveryTarget::Container(pod_name.to_owned(), cs.name.clone());
            (t.instance_id(), t.cr_name())
        })
        .collect()
}

// === PodInfo =================================================================

/// 单个进程启动时从 Downward API / 环境变量加载到的 Pod 身份。
///
/// 字段 `pod_ip` 是 K8s 原生注册路径新增的必需输入——`EndpointSlice` 的
/// `portname.addresses[]` 不能为空，否则 kube-proxy 无法把流量路由到本 Pod。
#[derive(Debug, Clone)]
pub(super) struct PodInfo {
    pub pod_name: String,
    pub pod_namespace: String,
    pub pod_uid: String,
    /// Pod 的可路由 IP，写入 `EndpointSlice.endpoints[].addresses`。
    pub pod_ip: String,
    pub system_port: u16,
    pub mode: KubeDiscoveryMode,
    pub target: KubeDiscoveryTarget,
}

impl PodInfo {
    /// 优先读 Downward API 文件，回退到环境变量；都没有时返回 `None`。
    fn read_field(file: &Path, env_var: &str) -> Option<String> {
        if let Ok(content) = fs::read_to_string(file) {
            let v = content.trim();
            if !v.is_empty() {
                return Some(v.to_owned());
            }
        }
        std::env::var(env_var).ok()
    }

    /// 一次性从环境/卷读取所有 Pod 身份字段。
    ///
    /// 缺少 `pod_name` / `pod_uid` / `pod_ip` 会直接返回 `Err`，因为这三者
    /// 是 K8s 原生注册路径的硬性输入；`pod_namespace` 缺失时回退到 `"default"`。
    pub fn from_env() -> Result<Self> {
        let root = Path::new(DEFAULT_PODINFO_PATH);

        let pod_name = Self::read_field(&root.join("pod_name"), "POD_NAME")
            .ok_or_else(|| anyhow::anyhow!("POD_NAME not available from file or environment"))?;
        let pod_uid = Self::read_field(&root.join("pod_uid"), "POD_UID")
            .ok_or_else(|| anyhow::anyhow!("POD_UID not available from file or environment"))?;
        let pod_namespace = Self::read_field(&root.join("pod_namespace"), "POD_NAMESPACE")
            .unwrap_or_else(|| {
                tracing::warn!("POD_NAMESPACE not set, defaulting to 'default'");
                "default".to_owned()
            });
        let pod_ip = Self::read_field(&root.join("pod_ip"), "POD_IP").ok_or_else(|| {
            anyhow::anyhow!("POD_IP not available; K8s native discovery requires a pod IP")
        })?;

        let mode = KubeDiscoveryMode::from_env()?;
        let target = match mode {
            KubeDiscoveryMode::Pod => KubeDiscoveryTarget::Pod(pod_name.clone()),
            KubeDiscoveryMode::Container => {
                let cname = std::env::var("CONTAINER_NAME").map_err(|_| {
                    anyhow::anyhow!(
                        "CONTAINER_NAME is required when PGD_KUBE_DISCOVERY_MODE=container"
                    )
                })?;
                KubeDiscoveryTarget::Container(pod_name.clone(), cname)
            }
        };

        if root.join("pod_name").exists() {
            tracing::info!("Pod identity loaded from Downward API at {DEFAULT_PODINFO_PATH}");
        } else {
            tracing::info!("Pod identity loaded from environment variables");
        }

        let cfg = crate::config::RuntimeConfig::from_settings().unwrap_or_default();
        Ok(Self {
            pod_name,
            pod_namespace,
            pod_uid,
            pod_ip,
            system_port: cfg.system_port as u16,
            mode,
            target,
        })
    }
}

// === 单元测试 =================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::ObjectReference;
    use k8s_openapi::api::discovery::v1::{Endpoint, EndpointConditions};

    /// ## 测试过程
    /// 对同一字符串两次调用 `hash_pod_name`，比较返回值。
    /// ## 意义
    /// 跨进程、跨 FFI 的一致性依赖此哈希的确定性，必须严格保证。
    #[test]
    fn hash_pod_name_is_deterministic() {
        assert_eq!(hash_pod_name("worker-0"), hash_pod_name("worker-0"));
    }

    /// ## 测试过程
    /// 对多种长度的 Pod 名调用 `hash_pod_name`，断言结果 ≤ 53 位掩码。
    /// ## 意义
    /// 防止超出 JS Number 安全整数范围导致前端 ID 精度丢失。
    #[test]
    fn hash_pod_name_within_53_bits() {
        for name in &["a", "pod", "very-long-pod-name-xyz-0987654321"] {
            assert!(hash_pod_name(name) <= INSTANCE_ID_MASK);
        }
    }

    /// ## 测试过程
    /// 对 5 个不同名称求哈希，放入 `HashSet` 检查无碰撞。
    /// ## 意义
    /// 基本碰撞检测——`DefaultHasher` 在低基数下不应给出相同结果。
    #[test]
    fn hash_pod_name_no_collision_on_small_sample() {
        let names = ["w-0", "w-1", "w-2", "w-3", "w-4"];
        let set: std::collections::HashSet<_> = names.iter().map(|n| hash_pod_name(n)).collect();
        assert_eq!(set.len(), names.len());
    }

    /// ## 测试过程
    /// 调用 `sanitize` 输入大写字母+非法字符+中文，比较输出。
    /// ## 意义
    /// 保证 DNS-1123 规则被严格遵守，否则 K8s 写入会返回 400。
    #[test]
    fn sanitize_replaces_illegal_chars_and_lowercases() {
        assert_eq!(sanitize("Hello_World!", 20), "hello-world");
        assert_eq!(sanitize("ABC.def", 20), "abc-def");
    }

    /// ## 测试过程
    /// 输入长度大于 `max_len` 的字符串，比较返回值长度。
    /// ## 意义
    /// 验证截断逻辑，避免上层拼接后超出 K8s 63 字符上限。
    #[test]
    fn sanitize_truncates_to_max_len() {
        let out = sanitize("0123456789abcdef", 5);
        assert_eq!(out, "01234");
    }

    /// ## 测试过程
    /// 输入完全非法字符串使其规范化后为空。
    /// ## 意义
    /// 兜底回退到 `"x"`，避免上层构造非法资源名。
    #[test]
    fn sanitize_empty_fallback() {
        assert_eq!(sanitize("___", 5), "x");
        assert_eq!(sanitize("", 5), "x");
    }

    /// ## 测试过程
    /// 验证首尾连字符被剥离。
    /// ## 意义
    /// DNS-1123 要求首尾为字母数字，否则资源创建失败。
    #[test]
    fn sanitize_trims_leading_trailing_dashes() {
        // 截断后剩余 `--ab`，trim 后 `ab`
        assert_eq!(sanitize("__ab__cd", 4), "ab");
    }

    /// ## 测试过程
    /// 对同一字符串两次调用 `short_hash`，比较返回值。
    /// ## 意义
    /// 用作 K8s 资源名的去重后缀，必须确定性。
    #[test]
    fn short_hash_is_deterministic_and_8_hex() {
        let a = short_hash("svc/grpc/pod-0");
        let b = short_hash("svc/grpc/pod-0");
        assert_eq!(a, b);
        assert_eq!(a.len(), 8);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
    }

    /// ## 测试过程
    /// 构造 EndpointSlice，包含 ready / not-ready / no-target_ref 三类条目。
    /// ## 意义
    /// daemon 聚合阶段只能纳入 ready 条目，过滤逻辑必须正确。
    #[test]
    fn extract_portname_info_filters_unready() {
        let mk_ep = |name: &str, ready: bool, with_ref: bool| Endpoint {
            addresses: vec!["10.0.0.1".into()],
            conditions: Some(EndpointConditions {
                ready: Some(ready),
                ..Default::default()
            }),
            target_ref: if with_ref {
                Some(ObjectReference {
                    kind: Some("Pod".into()),
                    name: Some(name.into()),
                    ..Default::default()
                })
            } else {
                None
            },
            ..Default::default()
        };
        let slice = EndpointSlice {
            address_type: "IPv4".into(),
            endpoints: vec![
                mk_ep("ready-pod", true, true),
                mk_ep("not-ready", false, true),
                mk_ep("no-ref", true, false),
            ],
            ..Default::default()
        };
        let infos = extract_portname_info(&slice);
        assert_eq!(infos.len(), 1);
        assert_eq!(infos[0].1, "ready-pod");
    }

    /// ## 测试过程
    /// 验证 `KubeDiscoveryTarget::cr_name` 在 main 容器名下退化为 Pod 名。
    /// ## 意义
    /// 让 Pod 模式和默认 Container 模式产出兼容的 key，便于上层平滑切换。
    #[test]
    fn target_main_container_collapses_to_pod_name() {
        let t = KubeDiscoveryTarget::Container("pod-0".to_owned(), MAIN_CONTAINER_NAME.to_owned());
        assert_eq!(t.cr_name(), "pod-0");

        let t2 = KubeDiscoveryTarget::Container("pod-0".to_owned(), "sidecar".to_owned());
        assert_eq!(t2.cr_name(), "pod-0-sidecar");
    }
}
