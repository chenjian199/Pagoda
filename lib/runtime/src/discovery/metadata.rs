// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # Worker 元数据容器与快照
//!
//! ## 设计意图
//!
//! 每个 worker 把自己注册的 portname / model card / event channel 整体维护在
//! 一个 [`DiscoveryMetadata`] 中，作为可序列化的“自描述”单元被发布到 K8s
//! ConfigMap / Service annotation / KV store。daemon 把全集群的多份元数据
//! 收敛成一个 [`MetadataSnapshot`]，供上层在内存中按 [`DiscoveryQuery`] 过滤。
//!
//! ## 外部契约
//!
//! - [`DiscoveryMetadata`] 三个集合分别只接受同类型实例，类型不符返回错误；
//! - [`DiscoveryMetadata::filter`] 在调用者无需关心实例所属集合时按查询自动分派；
//! - [`MetadataSnapshot::has_changes_from`] 用 generation 比较判定差异，
//!   并在 `info!` 中输出 added / removed / updated 的 instance id（hex）；
//! - [`MetadataSnapshot::filter`] 把过滤透传到每个 worker 的 metadata 上。
//!
//! ## 实现要点
//!
//! - 三个 HashMap 的 key 一律使用各自 InstanceId 的 `to_path()` 结果，
//!   保证“同一实例多次注册”的幂等覆盖语义；
//! - 在 K8s SSA `schema=disabled` 场景下，空对象会被写回 `null`，
//!   因此所有字段都保留 [`deserialize_null_default`] 以容忍 `null`，
//!   即使切换到原生 K8s 路径后仍作为防御性兜底；
//! - [`MetadataSnapshot::has_changes_from`] 比较 generation 而非全字段：
//!   底层 watcher 已保证 generation 变化等价于内容变化，避免 O(n) 深比较。

use anyhow::Result;
use serde::Deserialize as _;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::{DiscoveryInstance, DiscoveryInstanceId, DiscoveryQuery};

// === serde 兜底：null → Default =============================================

/// 把 JSON `null` 或缺失字段反序列化为 `T::default()`。
///
/// **设计动机**：K8s Server-Side Apply 在 `schema = "disabled"` 下，空对象 `{}`
/// 经常被回写为 `null`。若不容忍，整条记录反序列化失败 → worker 从快照中消失
/// → 上层 `list` 返回 0 条 → 流量全 404。典型触发场景是 vLLM elastic EP
/// 缩容时 `unregister_event_channel` 把 `event_channels` 清空。
///
/// 即使当前主路径已切换到 K8s 原生对象（Service / ConfigMap / Lease），此兜底
/// 也作为**反序列化健壮性**的一部分保留：任何后端只要复用 `DiscoveryMetadata`
/// 作为线协议，就免疫 `null` 字段问题。
fn deserialize_null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Default + serde::Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

// === DiscoveryMetadata =======================================================

/// 单个 worker 的注册元数据集合（按类型分桶存储）。
///
/// 每个桶以 `InstanceId::to_path()` 作为 key，从而：
/// - 重复注册自动覆盖（最后一次写入胜出）；
/// - 跨进程序列化后仍可正确还原原始实例。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DiscoveryMetadata {
    /// 已注册的 portname 实例（key = [`PortNameInstanceId::to_path`]）
    #[serde(default, deserialize_with = "deserialize_null_default")]
    portnames: HashMap<String, DiscoveryInstance>,
    /// 已注册的 model card 实例（key = [`ModelCardInstanceId::to_path`]）
    #[serde(default, deserialize_with = "deserialize_null_default")]
    model_cards: HashMap<String, DiscoveryInstance>,
    /// 已注册的 event channel 实例（key = [`EventChannelInstanceId::to_path`]）
    #[serde(default, deserialize_with = "deserialize_null_default")]
    event_channels: HashMap<String, DiscoveryInstance>,
}

impl Default for DiscoveryMetadata {
    fn default() -> Self {
        Self::new()
    }
}

impl DiscoveryMetadata {
    /// 构造一个空 metadata。
    pub fn new() -> Self {
        Self {
            portnames: HashMap::new(),
            model_cards: HashMap::new(),
            event_channels: HashMap::new(),
        }
    }

    /// 注册一个 PortName 实例。类型不符时返回错误。
    pub fn register_portname(&mut self, instance: DiscoveryInstance) -> Result<()> {
        insert_typed(
            &mut self.portnames,
            instance,
            DiscoveryInstanceId::extract_portname_id,
            |k| k.to_path(),
            "portname",
        )
    }

    /// 注册一个 Model card 实例。类型不符时返回错误。
    pub fn register_model_card(&mut self, instance: DiscoveryInstance) -> Result<()> {
        insert_typed(
            &mut self.model_cards,
            instance,
            DiscoveryInstanceId::extract_model_id,
            |k| k.to_path(),
            "model card",
        )
    }

    /// 注册一个 Event channel 实例。类型不符时返回错误。
    pub fn register_event_channel(&mut self, instance: DiscoveryInstance) -> Result<()> {
        insert_typed(
            &mut self.event_channels,
            instance,
            DiscoveryInstanceId::extract_event_channel_id,
            |k| k.to_path(),
            "event channel",
        )
    }

    /// 注销 PortName 实例。幂等（不存在时也返回 Ok）。
    pub fn unregister_portname(&mut self, instance: &DiscoveryInstance) -> Result<()> {
        remove_typed(
            &mut self.portnames,
            instance,
            DiscoveryInstanceId::extract_portname_id,
            |k| k.to_path(),
            "portname",
        )
    }

    /// 注销 Model card 实例。幂等。
    pub fn unregister_model_card(&mut self, instance: &DiscoveryInstance) -> Result<()> {
        remove_typed(
            &mut self.model_cards,
            instance,
            DiscoveryInstanceId::extract_model_id,
            |k| k.to_path(),
            "model card",
        )
    }

    /// 注销 Event channel 实例。幂等。
    pub fn unregister_event_channel(&mut self, instance: &DiscoveryInstance) -> Result<()> {
        remove_typed(
            &mut self.event_channels,
            instance,
            DiscoveryInstanceId::extract_event_channel_id,
            |k| k.to_path(),
            "event channel",
        )
    }

    pub fn get_all_portnames(&self) -> Vec<DiscoveryInstance> {
        self.portnames.values().cloned().collect()
    }
    pub fn get_all_model_cards(&self) -> Vec<DiscoveryInstance> {
        self.model_cards.values().cloned().collect()
    }
    pub fn get_all_event_channels(&self) -> Vec<DiscoveryInstance> {
        self.event_channels.values().cloned().collect()
    }

    /// 返回三类集合的并集（按 portnames → model_cards → event_channels 顺序）。
    pub fn get_all(&self) -> Vec<DiscoveryInstance> {
        self.portnames
            .values()
            .chain(self.model_cards.values())
            .chain(self.event_channels.values())
            .cloned()
            .collect()
    }

    /// 按查询过滤实例：
    ///
    /// 1. 根据查询变体决定从哪个桶取候选（避免跨桶扫描）；
    /// 2. 复用 [`filter_instances`] 做细粒度字段比较。
    pub fn filter(&self, query: &DiscoveryQuery) -> Vec<DiscoveryInstance> {
        let candidates = match query {
            DiscoveryQuery::AllPortNames
            | DiscoveryQuery::NamespacedPortNames { .. }
            | DiscoveryQuery::ServiceGroupPortNames { .. }
            | DiscoveryQuery::PortName { .. } => self.get_all_portnames(),

            DiscoveryQuery::AllModels
            | DiscoveryQuery::NamespacedModels { .. }
            | DiscoveryQuery::ServiceGroupModels { .. }
            | DiscoveryQuery::PortNameModels { .. } => self.get_all_model_cards(),

            DiscoveryQuery::EventChannels(_) => self.get_all_event_channels(),
        };
        filter_instances(candidates, query)
    }
}

// === 私有类型分派 helpers ====================================================

/// 抽出“按 InstanceId 类型校验后写入对应 HashMap”的通用流程。
fn insert_typed<K, F, P>(
    bucket: &mut HashMap<String, DiscoveryInstance>,
    instance: DiscoveryInstance,
    extract: F,
    to_path: P,
    label: &str,
) -> Result<()>
where
    F: for<'a> Fn(&'a DiscoveryInstanceId) -> Result<&'a K>,
    P: Fn(&K) -> String,
{
    let id = instance.id();
    let key = extract(&id)
        .map_err(|_| anyhow::anyhow!("Cannot register non-{label} instance as {label}"))?;
    let path = to_path(key);
    bucket.insert(path, instance);
    Ok(())
}

/// 抽出“按 InstanceId 类型校验后从对应 HashMap 删除”的通用流程（幂等）。
fn remove_typed<K, F, P>(
    bucket: &mut HashMap<String, DiscoveryInstance>,
    instance: &DiscoveryInstance,
    extract: F,
    to_path: P,
    label: &str,
) -> Result<()>
where
    F: for<'a> Fn(&'a DiscoveryInstanceId) -> Result<&'a K>,
    P: Fn(&K) -> String,
{
    let id = instance.id();
    let key = extract(&id)
        .map_err(|_| anyhow::anyhow!("Cannot unregister non-{label} instance as {label}"))?;
    bucket.remove(&to_path(key));
    Ok(())
}

// === 实例 → 字段过滤 =========================================================

/// 在“同类候选集”内部按查询字段做细粒度过滤。
fn filter_instances(
    instances: Vec<DiscoveryInstance>,
    query: &DiscoveryQuery,
) -> Vec<DiscoveryInstance> {
    instances
        .into_iter()
        .filter(|inst| instance_matches_query(inst, query))
        .collect()
    }

/// 单实例与查询的字段级匹配判定。
fn instance_matches_query(inst: &DiscoveryInstance, query: &DiscoveryQuery) -> bool {
    match (inst, query) {
        (_, DiscoveryQuery::AllPortNames) | (_, DiscoveryQuery::AllModels) => true,

        (DiscoveryInstance::PortName(i), DiscoveryQuery::NamespacedPortNames { namespace }) => {
            &i.namespace == namespace
        }
        (
            DiscoveryInstance::PortName(i),
            DiscoveryQuery::ServiceGroupPortNames { namespace, servicegroup },
        ) => &i.namespace == namespace && &i.servicegroup == servicegroup,
        (
            DiscoveryInstance::PortName(i),
            DiscoveryQuery::PortName { namespace, servicegroup, portname },
        ) => &i.namespace == namespace && &i.servicegroup == servicegroup && &i.portname == portname,

        (
            DiscoveryInstance::Model { namespace: ns, .. },
            DiscoveryQuery::NamespacedModels { namespace },
        ) => ns == namespace,
        (
            DiscoveryInstance::Model {
                namespace: ns,
                servicegroup: comp,
                ..
            },
            DiscoveryQuery::ServiceGroupModels { namespace, servicegroup },
        ) => ns == namespace && comp == servicegroup,
        (
            DiscoveryInstance::Model {
                namespace: ns,
                servicegroup: comp,
                portname: ep,
                ..
            },
            DiscoveryQuery::PortNameModels {
                namespace,
                servicegroup,
                portname,
            },
        ) => ns == namespace && comp == servicegroup && ep == portname,

        (
            DiscoveryInstance::EventChannel {
                namespace: ns,
                servicegroup: comp,
                topic: t,
                ..
            },
            DiscoveryQuery::EventChannels(q),
        ) => {
            q.namespace.as_ref().is_none_or(|x| x == ns)
                && q.servicegroup.as_ref().is_none_or(|x| x == comp)
                && q.topic.as_ref().is_none_or(|x| x == t)
        }

        _ => false,
    }
}

// === MetadataSnapshot ========================================================

/// 全集群所有 worker 的 metadata 聚合视图。
#[derive(Clone, Debug)]
pub struct MetadataSnapshot {
    /// instance_id → metadata 的映射
    pub instances: HashMap<u64, Arc<DiscoveryMetadata>>,
    /// instance_id → 关联 K8s 对象 generation，仅包含 ready 的 worker
    pub generations: HashMap<u64, i64>,
    /// 调试用单调递增序号
    pub sequence: u64,
    /// 可观测性时间戳
    pub timestamp: std::time::Instant,
}

impl MetadataSnapshot {
    /// 空快照。
    pub fn empty() -> Self {
        Self {
            instances: HashMap::new(),
            generations: HashMap::new(),
            sequence: 0,
            timestamp: std::time::Instant::now(),
        }
    }

    /// 与上一版本快照比较；变化则返回 `true` 并打印 diff。
    ///
    /// 实现细节：
    /// - 直接比较 `generations` map（PartialEq 实现已含 key + value 比较）；
    /// - 不等时进一步分类为 added / removed / updated 三组用于日志，
    ///   有助于运维直接看出是“新 pod 上线”、“pod 退场”还是“同 pod 数据更新”。
    pub fn has_changes_from(&self, prev: &MetadataSnapshot) -> bool {
        if self.generations == prev.generations {
            tracing::trace!(
                "Snapshot (seq={}): no changes, {} instances",
                self.sequence,
                self.instances.len()
            );
            return false;
        }

        let (added, removed, updated) = classify_generation_diff(&self.generations, &prev.generations);
        tracing::info!(
            "Snapshot (seq={}): {} instances, added={:?}, removed={:?}, updated={:?}",
            self.sequence,
            self.instances.len(),
            added,
            removed,
            updated
        );
        true
    }

    /// 把过滤透传到每个 worker 的 metadata 上并合并结果。
    pub fn filter(&self, query: &DiscoveryQuery) -> Vec<DiscoveryInstance> {
        self.instances
            .values()
            .flat_map(|m| m.filter(query))
            .collect()
    }
}

/// 将两个 generation map 的差异分类为 added / removed / updated 三组（hex 字符串）。
fn classify_generation_diff(
    curr: &HashMap<u64, i64>,
    prev: &HashMap<u64, i64>,
) -> (Vec<String>, Vec<String>, Vec<String>) {
    let curr_ids: HashSet<u64> = curr.keys().copied().collect();
    let prev_ids: HashSet<u64> = prev.keys().copied().collect();

    let added: Vec<_> = curr_ids
        .difference(&prev_ids)
        .map(|id| format!("{id:x}"))
        .collect();
    let removed: Vec<_> = prev_ids
        .difference(&curr_ids)
        .map(|id| format!("{id:x}"))
        .collect();
    let updated: Vec<_> = curr
        .iter()
        .filter(|(k, v)| prev.get(*k).is_some_and(|pv| pv != *v))
        .map(|(k, _)| format!("{k:x}"))
        .collect();
    (added, removed, updated)
}

// === 单元测试 =================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::servicegroup::{Instance, TransportType};
    use crate::discovery::{EventChannelQuery, EventTransport};

    fn ep(ns: &str, c: &str, e: &str, id: u64) -> DiscoveryInstance {
        DiscoveryInstance::PortName(Instance {
            namespace: ns.into(),
            servicegroup: c.into(),
            portname: e.into(),
            instance_id: id,
            transport: TransportType::Nats("nats://x".into()),
            device_type: None,
        })
    }

    fn model(ns: &str, c: &str, e: &str, id: u64) -> DiscoveryInstance {
        DiscoveryInstance::Model {
            namespace: ns.into(),
            servicegroup: c.into(),
            portname: e.into(),
            instance_id: id,
            card_json: serde_json::json!({ "display_name": "m" }),
            model_suffix: None,
        }
    }

    fn evc(ns: &str, c: &str, t: &str, id: u64) -> DiscoveryInstance {
        DiscoveryInstance::EventChannel {
            namespace: ns.into(),
            servicegroup: c.into(),
            topic: t.into(),
            instance_id: id,
            transport: EventTransport::zmq("tcp://x:1"),
        }
    }

    /// ## 测试过程
    /// 注册 PortName 后序列化再反序列化，三个桶大小应保持 1/0/0。
    /// ## 意义
    /// 验证 serde 往返不丢字段、不串桶。
    #[test]
    fn metadata_serde_roundtrip() {
        let mut m = DiscoveryMetadata::new();
        m.register_portname(ep("t", "c", "e", 1)).unwrap();
        let json = serde_json::to_string(&m).unwrap();
        let back: DiscoveryMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(back.portnames.len(), 1);
        assert_eq!(back.model_cards.len(), 0);
        assert_eq!(back.event_channels.len(), 0);
    }

    /// ## 测试过程
    /// 构造一个三字段均为 `null` 的 JSON，调用 from_str。
    /// ## 意义
    /// 回归保护 [`deserialize_null_default`] —— 没有它整条解析会失败。
    #[test]
    fn metadata_tolerates_null_buckets() {
        let json = r#"{ "portnames": null, "model_cards": null, "event_channels": null }"#;
        let m: DiscoveryMetadata = serde_json::from_str(json).unwrap();
        assert!(m.get_all().is_empty());
    }

    /// ## 测试过程
    /// 把 Model 实例传给 register_portname。
    /// ## 意义
    /// 类型分桶必须严格，否则查询结果会跨桶污染。
    #[test]
    fn register_rejects_wrong_type() {
        let mut m = DiscoveryMetadata::new();
        assert!(m.register_portname(model("n", "c", "e", 1)).is_err());
    }

    /// ## 测试过程
    /// 对从未注册过的实例调用 unregister；返回 Ok。
    /// ## 意义
    /// fan-out 注销路径上需要幂等，避免“先 unregister 再 shutdown”竞态报错。
    #[test]
    fn unregister_is_idempotent() {
        let mut m = DiscoveryMetadata::new();
        m.unregister_portname(&ep("n", "c", "e", 9)).unwrap();
    }

    /// ## 测试过程
    /// 同一 InstanceId 连续注册两次，集合大小仍为 1。
    /// ## 意义
    /// 验证“路径去重”语义，防止重复条目导致 list 数量虚高。
    #[test]
    fn duplicate_register_overwrites() {
        let mut m = DiscoveryMetadata::new();
        m.register_portname(ep("n", "c", "e", 1)).unwrap();
        m.register_portname(ep("n", "c", "e", 1)).unwrap();
        assert_eq!(m.get_all_portnames().len(), 1);
    }

    /// ## 测试过程
    /// 注册 portname/model/event_channel 各 1 个，分别用对应 All 查询。
    /// ## 意义
    /// 验证 filter 的桶分派正确，杜绝跨类型穿透。
    #[test]
    fn filter_dispatches_to_correct_bucket() {
        let mut m = DiscoveryMetadata::new();
        m.register_portname(ep("n", "c", "e", 1)).unwrap();
        m.register_model_card(model("n", "c", "e", 2)).unwrap();
        m.register_event_channel(evc("n", "c", "kv", 3)).unwrap();
        assert_eq!(m.filter(&DiscoveryQuery::AllPortNames).len(), 1);
        assert_eq!(m.filter(&DiscoveryQuery::AllModels).len(), 1);
        assert_eq!(
            m.filter(&DiscoveryQuery::EventChannels(EventChannelQuery::all())).len(),
            1
        );
    }

    /// ## 测试过程
    /// 两个 snapshot 的 generations 完全一致，has_changes_from 返回 false。
    /// ## 意义
    /// generation 相等 → 内容相等，节省深比较开销。
    #[test]
    fn snapshot_unchanged() {
        let mut a = MetadataSnapshot::empty();
        a.generations.insert(1, 1);
        let mut b = MetadataSnapshot::empty();
        b.generations.insert(1, 1);
        assert!(!b.has_changes_from(&a));
    }

    /// ## 测试过程
    /// 构造 prev 含 {1:1, 2:1}，curr 含 {2:2, 3:1}；
    /// 调用 classify_generation_diff 检查 added=[3]、removed=[1]、updated=[2]。
    /// ## 意义
    /// 直接验证 diff 分类的核心算法，保证日志输出正确。
    #[test]
    fn classify_diff_categories() {
        let prev = HashMap::from([(1u64, 1i64), (2, 1)]);
        let curr = HashMap::from([(2u64, 2i64), (3, 1)]);
        let (mut added, mut removed, mut updated) = classify_generation_diff(&curr, &prev);
        added.sort();
        removed.sort();
        updated.sort();
        assert_eq!(added, vec!["3"]);
        assert_eq!(removed, vec!["1"]);
        assert_eq!(updated, vec!["2"]);
    }

    /// ## 测试过程
    /// 构造 snapshot 含 2 个 worker，各注册 1 个 portname；调用 filter。
    /// ## 意义
    /// 验证 MetadataSnapshot::filter 跨 worker 聚合正确。
    #[test]
    fn snapshot_filter_aggregates_workers() {
        let mut a = DiscoveryMetadata::new();
        a.register_portname(ep("n", "c", "e1", 1)).unwrap();
        let mut b = DiscoveryMetadata::new();
        b.register_portname(ep("n", "c", "e2", 2)).unwrap();
        let snap = MetadataSnapshot {
            instances: HashMap::from([(1u64, Arc::new(a)), (2, Arc::new(b))]),
            generations: HashMap::from([(1u64, 1i64), (2, 1)]),
            sequence: 1,
            timestamp: std::time::Instant::now(),
        };
        assert_eq!(snap.filter(&DiscoveryQuery::AllPortNames).len(), 2);
    }
}
