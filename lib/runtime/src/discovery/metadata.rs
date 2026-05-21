// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 发现层元数据管理。
//!
//! - [`DiscoveryMetadata`]：单个 Pod 的注册元数据状态，三个 HashMap 分别存储三类实例。
//! - [`MetadataSnapshot`]：集群全局快照，由 `DiscoveryDaemon` 聚合后通过 `watch` channel 广播。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use serde::{Deserialize, Deserializer, Serialize};

use super::{DiscoveryInstance, DiscoveryQuery};

// ══════════════════════════════════════════════════════════════════════════════
// DiscoveryMetadata
// ══════════════════════════════════════════════════════════════════════════════

/// 单个 Pod 当前注册的所有实例元数据。
///
/// 三个 HashMap 以 `InstanceId::to_path()` 字符串作为键，值为完整 `DiscoveryInstance`，
/// 同一实例重复注册时覆盖写入（幂等）。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiscoveryMetadata {
    #[serde(default, deserialize_with = "deserialize_null_default")]
    portnames: HashMap<String, DiscoveryInstance>,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    model_cards: HashMap<String, DiscoveryInstance>,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    event_channels: HashMap<String, DiscoveryInstance>,
}

impl DiscoveryMetadata {
    pub fn new() -> Self {
        Self::default()
    }

    // ── 注册 ──────────────────────────────────────────────────────────────────

    /// 注册 PortName 实例（仅接受 `DiscoveryInstance::PortName` 变体）。
    pub fn register_portname(&mut self, instance: DiscoveryInstance) -> anyhow::Result<()> {
        anyhow::ensure!(
            matches!(instance, DiscoveryInstance::PortName(_)),
            "register_portname received non-PortName instance"
        );
        let key = instance.id().to_string();
        self.portnames.insert(key, instance);
        Ok(())
    }

    /// 注册 ModelCard 实例（仅接受 `DiscoveryInstance::Model` 变体）。
    pub fn register_model_card(&mut self, instance: DiscoveryInstance) -> anyhow::Result<()> {
        anyhow::ensure!(
            matches!(instance, DiscoveryInstance::Model { .. }),
            "register_model_card received non-Model instance"
        );
        let key = instance.id().to_string();
        self.model_cards.insert(key, instance);
        Ok(())
    }

    /// 注册 EventChannel 实例（仅接受 `DiscoveryInstance::EventChannel` 变体）。
    pub fn register_event_channel(&mut self, instance: DiscoveryInstance) -> anyhow::Result<()> {
        anyhow::ensure!(
            matches!(instance, DiscoveryInstance::EventChannel { .. }),
            "register_event_channel received non-EventChannel instance"
        );
        let key = instance.id().to_string();
        self.event_channels.insert(key, instance);
        Ok(())
    }

    // ── 注销 ──────────────────────────────────────────────────────────────────

    pub fn unregister_portname(&mut self, instance: &DiscoveryInstance) -> anyhow::Result<()> {
        anyhow::ensure!(
            matches!(instance, DiscoveryInstance::PortName(_)),
            "unregister_portname received non-PortName instance"
        );
        self.portnames.remove(&instance.id().to_string());
        Ok(())
    }

    pub fn unregister_model_card(&mut self, instance: &DiscoveryInstance) -> anyhow::Result<()> {
        anyhow::ensure!(
            matches!(instance, DiscoveryInstance::Model { .. }),
            "unregister_model_card received non-Model instance"
        );
        self.model_cards.remove(&instance.id().to_string());
        Ok(())
    }

    pub fn unregister_event_channel(
        &mut self,
        instance: &DiscoveryInstance,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            matches!(instance, DiscoveryInstance::EventChannel { .. }),
            "unregister_event_channel received non-EventChannel instance"
        );
        self.event_channels.remove(&instance.id().to_string());
        Ok(())
    }

    // ── 读取 ──────────────────────────────────────────────────────────────────

    pub fn get_all_portnames(&self) -> Vec<DiscoveryInstance> {
        self.portnames.values().cloned().collect()
    }

    pub fn get_all_model_cards(&self) -> Vec<DiscoveryInstance> {
        self.model_cards.values().cloned().collect()
    }

    pub fn get_all_event_channels(&self) -> Vec<DiscoveryInstance> {
        self.event_channels.values().cloned().collect()
    }

    /// 三个 HashMap 合并输出。
    pub fn get_all(&self) -> Vec<DiscoveryInstance> {
        self.portnames
            .values()
            .chain(self.model_cards.values())
            .chain(self.event_channels.values())
            .cloned()
            .collect()
    }

    /// 按查询条件过滤，先选 HashMap 再做精确字段匹配。
    pub fn filter(&self, query: &DiscoveryQuery) -> Vec<DiscoveryInstance> {
        match query {
            // ── PortName queries ──
            DiscoveryQuery::AllPortNames => self.get_all_portnames(),
            DiscoveryQuery::NamespacedPortNames { namespace } => self
                .portnames
                .values()
                .filter(|i| portname_inst_ns(i) == Some(namespace.as_str()))
                .cloned()
                .collect(),
            DiscoveryQuery::ServiceGroupPortNames { namespace, servicegroup } => self
                .portnames
                .values()
                .filter(|i| {
                    portname_inst_ns(i) == Some(namespace.as_str())
                        && portname_inst_sg(i) == Some(servicegroup.as_str())
                })
                .cloned()
                .collect(),
            DiscoveryQuery::PortName { namespace, servicegroup, portname } => self
                .portnames
                .values()
                .filter(|i| {
                    portname_inst_ns(i) == Some(namespace.as_str())
                        && portname_inst_sg(i) == Some(servicegroup.as_str())
                        && portname_inst_pn(i) == Some(portname.as_str())
                })
                .cloned()
                .collect(),

            // ── Model queries ──
            DiscoveryQuery::AllModels => self.get_all_model_cards(),
            DiscoveryQuery::NamespacedModels { namespace } => self
                .model_cards
                .values()
                .filter(|i| model_field(i, |ns, _, _| ns == namespace.as_str()))
                .cloned()
                .collect(),
            DiscoveryQuery::ServiceGroupModels { namespace, servicegroup } => self
                .model_cards
                .values()
                .filter(|i| {
                    model_field(i, |ns, sg, _| ns == namespace.as_str() && sg == servicegroup.as_str())
                })
                .cloned()
                .collect(),
            DiscoveryQuery::PortNameModels { namespace, servicegroup, portname } => self
                .model_cards
                .values()
                .filter(|i| {
                    model_field(i, |ns, sg, pn| {
                        ns == namespace.as_str()
                            && sg == servicegroup.as_str()
                            && pn == portname.as_str()
                    })
                })
                .cloned()
                .collect(),

            // ── EventChannel queries ──
            DiscoveryQuery::EventChannels(eq) => self
                .event_channels
                .values()
                .filter(|i| {
                    if let DiscoveryInstance::EventChannel {
                        namespace: ns,
                        servicegroup: sg,
                        topic: t,
                        ..
                    } = i
                    {
                        eq.namespace.as_ref().map_or(true, |n| n == ns)
                            && eq.servicegroup.as_ref().map_or(true, |s| s == sg)
                            && eq.topic.as_ref().map_or(true, |tp| tp == t)
                    } else {
                        false
                    }
                })
                .cloned()
                .collect(),
        }
    }
}

// ── DiscoveryMetadata 字段提取辅助（避免重复 match）─────────────────────────

fn portname_inst_ns(i: &DiscoveryInstance) -> Option<&str> {
    if let DiscoveryInstance::PortName(inst) = i {
        Some(&inst.namespace)
    } else {
        None
    }
}

fn portname_inst_sg(i: &DiscoveryInstance) -> Option<&str> {
    if let DiscoveryInstance::PortName(inst) = i {
        Some(&inst.servicegroup)
    } else {
        None
    }
}

fn portname_inst_pn(i: &DiscoveryInstance) -> Option<&str> {
    if let DiscoveryInstance::PortName(inst) = i {
        Some(&inst.portname)
    } else {
        None
    }
}

fn model_field<F>(i: &DiscoveryInstance, f: F) -> bool
where
    F: Fn(&str, &str, &str) -> bool,
{
    if let DiscoveryInstance::Model { namespace, servicegroup, portname, .. } = i {
        f(namespace, servicegroup, portname)
    } else {
        false
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// MetadataSnapshot
// ══════════════════════════════════════════════════════════════════════════════

/// 集群全局实例快照，由 `DiscoveryDaemon` 聚合后通过 `watch` channel 广播。
///
/// `instances` 与 `generations` 的键集合严格相同（`DiscoveryDaemon` 聚合时同时写入两个 HashMap）。
#[derive(Clone, Debug)]
pub struct MetadataSnapshot {
    /// instance_id → 该 pod 的注册元数据
    pub instances: HashMap<u64, Arc<DiscoveryMetadata>>,
    /// instance_id → 对象聚合版本（变更检测用，来自 k8s 资源 generation/resourceVersion）
    pub generations: HashMap<u64, i64>,
    /// 快照序列号，用于 debug 日志追踪
    pub sequence: u64,
    /// 快照生成时间，供可观测性使用
    pub timestamp: Instant,
}

impl MetadataSnapshot {
    /// 初始空快照，作为 watch channel 的初始值。
    pub fn empty() -> Self {
        Self {
            instances: HashMap::new(),
            generations: HashMap::new(),
            sequence: 0,
            timestamp: Instant::now(),
        }
    }

    /// 比较两个快照的 `generations` map 是否相同。
    ///
    /// 相同返回 `false`（跳过广播）；不同返回 `true` 并打印 info 日志。
    pub fn has_changes_from(&self, prev: &MetadataSnapshot) -> bool {
        if self.generations == prev.generations {
            return false;
        }

        let prev_keys: std::collections::HashSet<_> = prev.generations.keys().collect();
        let curr_keys: std::collections::HashSet<_> = self.generations.keys().collect();

        let added: Vec<_> = curr_keys.difference(&prev_keys).collect();
        let removed: Vec<_> = prev_keys.difference(&curr_keys).collect();
        let updated: Vec<_> = curr_keys
            .intersection(&prev_keys)
            .filter(|k| self.generations[*k] != prev.generations[*k])
            .collect();

        tracing::info!(
            sequence = self.sequence,
            added = ?added,
            removed = ?removed,
            updated = ?updated,
            "MetadataSnapshot changed"
        );
        true
    }

    /// 遍历所有 pod 的 `DiscoveryMetadata`，汇总符合 query 的实例。
    pub fn filter(&self, query: &DiscoveryQuery) -> Vec<DiscoveryInstance> {
        self.instances
            .values()
            .flat_map(|meta| meta.filter(query))
            .collect()
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// 反序列化工具
// ══════════════════════════════════════════════════════════════════════════════

/// 将 JSON `null` 反序列化为 `T::default()`，而不是报错。
///
/// 修复 k8s API 返回对象集合为 null 时 `DiscoveryMetadata` 字段的反序列化故障。
pub fn deserialize_null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    let opt = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}

// ══════════════════════════════════════════════════════════════════════════════
// Tests
// ══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discovery::{DiscoveryInstance, EventTransport};
    use crate::servicegroup;

    fn make_portname_inst(ns: &str, sg: &str, pn: &str, id: u64) -> DiscoveryInstance {
        DiscoveryInstance::PortName(servicegroup::Instance {
            namespace: ns.into(),
            servicegroup: sg.into(),
            portname: pn.into(),
            instance_id: id,
            transport: servicegroup::TransportType::Nats(format!("{ns}.{sg}.{pn}")),
        })
    }

    fn make_event_inst(ns: &str, sg: &str, topic: &str, id: u64) -> DiscoveryInstance {
        DiscoveryInstance::EventChannel {
            namespace: ns.into(),
            servicegroup: sg.into(),
            topic: topic.into(),
            instance_id: id,
            transport: EventTransport::nats(format!("{ns}.{sg}.{topic}")),
        }
    }

    #[test]
    fn register_and_filter_portname() {
        let mut meta = DiscoveryMetadata::new();
        let inst = make_portname_inst("ns", "sg", "pn", 1);
        meta.register_portname(inst.clone()).unwrap();

        let results = meta.filter(&DiscoveryQuery::AllPortNames);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], inst);
    }

    #[test]
    fn register_wrong_variant_returns_err() {
        let mut meta = DiscoveryMetadata::new();
        let wrong = make_event_inst("ns", "sg", "topic", 1);
        assert!(meta.register_portname(wrong).is_err());
    }

    #[test]
    fn idempotent_register() {
        let mut meta = DiscoveryMetadata::new();
        let inst = make_portname_inst("ns", "sg", "pn", 1);
        meta.register_portname(inst.clone()).unwrap();
        meta.register_portname(inst.clone()).unwrap();
        assert_eq!(meta.get_all_portnames().len(), 1);
    }

    #[test]
    fn unregister_removes_instance() {
        let mut meta = DiscoveryMetadata::new();
        let inst = make_portname_inst("ns", "sg", "pn", 1);
        meta.register_portname(inst.clone()).unwrap();
        meta.unregister_portname(&inst).unwrap();
        assert!(meta.get_all_portnames().is_empty());
    }

    #[test]
    fn filter_by_servicegroup() {
        let mut meta = DiscoveryMetadata::new();
        meta.register_portname(make_portname_inst("ns", "sgA", "pn", 1)).unwrap();
        meta.register_portname(make_portname_inst("ns", "sgB", "pn", 2)).unwrap();

        let results = meta.filter(&DiscoveryQuery::ServiceGroupPortNames {
            namespace: "ns".into(),
            servicegroup: "sgA".into(),
        });
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn snapshot_empty_has_no_changes_from_itself() {
        let a = MetadataSnapshot::empty();
        let b = MetadataSnapshot::empty();
        assert!(!a.has_changes_from(&b));
    }

    #[test]
    fn snapshot_detects_generation_change() {
        let mut a = MetadataSnapshot::empty();
        a.generations.insert(1, 0);
        let mut b = MetadataSnapshot::empty();
        b.generations.insert(1, 1);
        assert!(b.has_changes_from(&a));
    }
}
