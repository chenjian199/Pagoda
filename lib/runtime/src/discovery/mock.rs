// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 用于测试的 In-Memory Discovery 实现
//!
//! ## 设计意图
//!
//! `MockDiscovery` 在测试 / 演示 / 本地工具中提供一个无外部依赖的 `Discovery`
//! 后端：所有实例存放在共享 `Vec<DiscoveryInstance>` 中，注册 / 注销 / 查询
//! 全部走内存。它**不模拟**真实后端的 TTL、CAS、批量推送等语义，目的只是
//! 让上层组件能够脱离 etcd / K8s 完成单元测试。
//!
//! ## 外部契约
//!
//! - `pub SharedMockRegistry`：跨多个 `MockDiscovery` 实例共享存储；
//! - `pub MockDiscovery::new(Option<u64> instance_id, SharedMockRegistry)`：
//!   缺省 `instance_id` 由单调原子计数器分配；
//! - 实现 `Discovery` trait 的全部方法；`list_and_watch` 通过 10ms 轮询
//!   差分模拟事件流。
//!
//! ## 实现要点
//!
//! - 查询匹配抽离到 [`QueryMatcher`]：把 `(Instance, Query)` 的所有跨类型 /
//!   嵌套字段组合用方法链表达，避免历史版本上百行 `match` 嵌套；
//! - `list_and_watch` 内部用 [`diff_snapshots`] 计算 `Added` / `Removed`，
//!   使主循环只剩“轮询 + emit”两步骨架；
//! - 共享存储采用 `Arc<Mutex<Vec<...>>>`：`Mutex` 提供短临界区写保护，
//!   对内存 mock 性能完全够用。

use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use super::{
    Discovery, DiscoveryEvent, DiscoveryInstance, DiscoveryInstanceId, DiscoveryQuery,
    DiscoverySpec, DiscoveryStream,
};

// === SharedMockRegistry ======================================================

/// 多个 `MockDiscovery` 共享的内存注册表。
///
/// 通过 `Clone` 可以让“多个 worker”在测试中看到同一份实例集合，模拟
/// etcd 后端的跨节点可见性。
#[derive(Clone, Default)]
pub struct SharedMockRegistry {
    instances: Arc<Mutex<Vec<DiscoveryInstance>>>,
}

impl SharedMockRegistry {
    pub fn new() -> Self {
        Self::default()
    }
}

// === MockDiscovery ===========================================================

/// Mock 后端的 `Discovery` 实现，所有数据存放在共享内存。
pub struct MockDiscovery {
    instance_id: u64,
    registry: SharedMockRegistry,
}

impl MockDiscovery {
    /// 构造一个新的 mock 客户端。
    ///
    /// - `instance_id = Some(n)`：使用显式 ID（便于测试断言）；
    /// - `instance_id = None`：从内部原子计数器分配一个自增 ID。
    pub fn new(instance_id: Option<u64>, registry: SharedMockRegistry) -> Self {
        Self {
            instance_id: instance_id.unwrap_or_else(allocate_mock_instance_id),
            registry,
        }
    }
}

/// 单调递增的全局计数器，用作未指定 instance_id 时的回退分配源。
fn allocate_mock_instance_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    COUNTER.fetch_add(1, Ordering::SeqCst)
}

// === QueryMatcher ============================================================

/// 把 `(实例, 查询)` 的匹配判定收敛到一个结构，避免在主路径里重复 match。
///
/// 通过分别为三种 `DiscoveryInstance` 变体提供专用方法（`matches_portname`、
/// `matches_model`、`matches_event_channel`），让顶层 `match` 只起“分派”作用，
/// 实际过滤条件下沉到方法体，可读性更好。
struct QueryMatcher<'q> {
    query: &'q DiscoveryQuery,
}

impl<'q> QueryMatcher<'q> {
    fn new(query: &'q DiscoveryQuery) -> Self {
        Self { query }
    }

    /// 入口：根据 instance 变体调用对应的匹配方法。
    fn matches(&self, inst: &DiscoveryInstance) -> bool {
        match inst {
            DiscoveryInstance::PortName(_) => self.matches_portname(inst),
            DiscoveryInstance::Model { .. } => self.matches_model(inst),
            DiscoveryInstance::EventChannel { .. } => self.matches_event_channel(inst),
        }
    }

    fn matches_portname(&self, inst: &DiscoveryInstance) -> bool {
        let DiscoveryInstance::PortName(e) = inst else {
            return false;
        };
        match self.query {
            DiscoveryQuery::AllPortNames => true,
            DiscoveryQuery::NamespacedPortNames { namespace } => &e.namespace == namespace,
            DiscoveryQuery::ServiceGroupPortNames { namespace, servicegroup } => {
                &e.namespace == namespace && &e.servicegroup == servicegroup
            }
            DiscoveryQuery::PortName {
                namespace,
                servicegroup,
                portname,
            } => {
                &e.namespace == namespace
                    && &e.servicegroup == servicegroup
                    && &e.portname == portname
            }
            // 任何针对 Model / EventChannel 的查询都不应命中 PortName
            _ => false,
        }
    }

    fn matches_model(&self, inst: &DiscoveryInstance) -> bool {
        let DiscoveryInstance::Model {
            namespace: ns,
            servicegroup: comp,
            portname: ep,
            ..
        } = inst
        else {
            return false;
        };
        match self.query {
            DiscoveryQuery::AllModels => true,
            DiscoveryQuery::NamespacedModels { namespace } => ns == namespace,
            DiscoveryQuery::ServiceGroupModels { namespace, servicegroup } => {
                ns == namespace && comp == servicegroup
            }
            DiscoveryQuery::PortNameModels {
                namespace,
                servicegroup,
                portname,
            } => ns == namespace && comp == servicegroup && ep == portname,
            _ => false,
        }
    }

    fn matches_event_channel(&self, inst: &DiscoveryInstance) -> bool {
        let DiscoveryInstance::EventChannel {
            namespace: ns,
            servicegroup: comp,
            topic: t,
            ..
        } = inst
        else {
            return false;
        };
        match self.query {
            DiscoveryQuery::EventChannels(q) => {
                q.namespace.as_ref().is_none_or(|x| x == ns)
                    && q.servicegroup.as_ref().is_none_or(|x| x == comp)
                    && q.topic.as_ref().is_none_or(|x| x == t)
            }
            _ => false,
        }
    }
}

// === watch 流差分 ============================================================

/// 计算两次轮询之间的 Added / Removed 集合。
///
/// 抽离为独立函数是为了：
/// 1. 让 `list_and_watch` 主循环结构更平坦；
/// 2. 可独立测试差分逻辑（无需 spin up async 流）。
fn diff_snapshots(
    current: &[DiscoveryInstance],
    known: &std::collections::HashSet<DiscoveryInstanceId>,
) -> (Vec<DiscoveryInstance>, Vec<DiscoveryInstanceId>) {
    use std::collections::HashSet;
    let current_ids: HashSet<DiscoveryInstanceId> = current.iter().map(|i| i.id()).collect();

    let added: Vec<_> = current
        .iter()
        .filter(|i| !known.contains(&i.id()))
        .cloned()
        .collect();
    let removed: Vec<_> = known.difference(&current_ids).cloned().collect();
    (added, removed)
}

// === Discovery trait 实现 =====================================================

#[async_trait]
impl Discovery for MockDiscovery {
    fn instance_id(&self) -> u64 {
        self.instance_id
    }

    async fn register_internal(&self, spec: DiscoverySpec) -> Result<DiscoveryInstance> {
        let instance = spec.with_instance_id(self.instance_id);
        self.registry
            .instances
            .lock()
            .unwrap()
            .push(instance.clone());
        Ok(instance)
    }

    async fn unregister(&self, instance: DiscoveryInstance) -> Result<()> {
        let target = instance.id();
        self.registry
            .instances
            .lock()
            .unwrap()
            .retain(|i| i.id() != target);
        Ok(())
    }

    async fn list(&self, query: DiscoveryQuery) -> Result<Vec<DiscoveryInstance>> {
        let matcher = QueryMatcher::new(&query);
        let guard = self.registry.instances.lock().unwrap();
        Ok(guard
            .iter()
            .filter(|inst| matcher.matches(inst))
            .cloned()
            .collect())
    }

    async fn list_and_watch(
        &self,
        query: DiscoveryQuery,
        _cancel_token: Option<CancellationToken>,
    ) -> Result<DiscoveryStream> {
        use std::collections::HashSet;
        let registry = self.registry.clone();

        let stream = async_stream::stream! {
            let mut known: HashSet<DiscoveryInstanceId> = HashSet::new();

            loop {
                // 拿当前匹配快照（持锁极短）
                let current: Vec<DiscoveryInstance> = {
                    let matcher = QueryMatcher::new(&query);
                    let guard = registry.instances.lock().unwrap();
                    guard.iter().filter(|i| matcher.matches(i)).cloned().collect()
                };

                let (added, removed) = diff_snapshots(&current, &known);

                for inst in added {
                    let id = inst.id();
                    known.insert(id);
                    yield Ok(DiscoveryEvent::Added(inst));
                }
                for id in removed {
                    known.remove(&id);
                    yield Ok(DiscoveryEvent::Removed(id));
                }

                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        };
        Ok(Box::pin(stream))
    }
}

// === 单元测试 =================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::servicegroup::{Instance, TransportType};
    use crate::discovery::{DiscoveryQuery, DiscoverySpec, EventChannelQuery, EventTransport};
    use futures::StreamExt as _;
    use std::collections::HashSet;

    fn portname_spec(ns: &str, comp: &str, ep: &str) -> DiscoverySpec {
        DiscoverySpec::PortName {
            namespace: ns.into(),
            servicegroup: comp.into(),
            portname: ep.into(),
            transport: TransportType::Tcp(format!("localhost:5550/1/{ep}")),
            device_type: None,
        }
    }

    fn model_spec(ns: &str, comp: &str, ep: &str, name: &str) -> DiscoverySpec {
        DiscoverySpec::Model {
            namespace: ns.into(),
            servicegroup: comp.into(),
            portname: ep.into(),
            card_json: serde_json::json!({ "display_name": name }),
            model_suffix: None,
        }
    }

    fn portname_instance(ns: &str, comp: &str, ep: &str, id: u64) -> DiscoveryInstance {
        DiscoveryInstance::PortName(Instance {
            namespace: ns.into(),
            servicegroup: comp.into(),
            portname: ep.into(),
            instance_id: id,
            transport: TransportType::Nats(format!("nats://{ep}/{id}")),
            device_type: None,
        })
    }

    fn event_channel_instance(ns: &str, comp: &str, topic: &str, id: u64) -> DiscoveryInstance {
        DiscoveryInstance::EventChannel {
            namespace: ns.into(),
            servicegroup: comp.into(),
            topic: topic.into(),
            instance_id: id,
            transport: EventTransport::zmq(format!("tcp://localhost:{}", 6000 + id)),
        }
    }

    // ── QueryMatcher ─────────────────────────────────────────────────────────

    /// ## 测试过程
    /// 用 PortName 实例 + 不匹配的 Model 查询，断言不命中。
    /// ## 意义
    /// 跨类型查询绝不能误命中，否则 list 会泄露其他类型的实例。
    #[test]
    fn matcher_rejects_cross_type() {
        let inst = portname_instance("ns", "c", "e", 1);
        let m = QueryMatcher::new(&DiscoveryQuery::AllModels);
        assert!(!m.matches(&inst));
    }

    /// ## 测试过程
    /// 用 EventChannel 查询的 topic 不匹配 / 匹配两种情况调用 matches。
    /// ## 意义
    /// 验证 EventChannel 的多级可选过滤（namespace/servicegroup/topic）逻辑。
    #[test]
    fn matcher_event_channel_optional_filters() {
        let inst = event_channel_instance("ns", "c", "kv-events", 1);
        let q_match = DiscoveryQuery::EventChannels(EventChannelQuery::topic(
            "ns",
            "c",
            "kv-events",
        ));
        let q_miss = DiscoveryQuery::EventChannels(EventChannelQuery::topic(
            "ns",
            "c",
            "other",
        ));
        assert!(QueryMatcher::new(&q_match).matches(&inst));
        assert!(!QueryMatcher::new(&q_miss).matches(&inst));
    }

    // ── diff_snapshots ───────────────────────────────────────────────────────

    /// ## 测试过程
    /// 已知集合空、当前快照有 2 条；调用 diff，断言 added=2/removed=0。
    /// ## 意义
    /// 验证“首次同步”路径产生正确的 Added 列表。
    #[test]
    fn diff_added_only_when_known_empty() {
        let current = vec![
            portname_instance("n", "c", "e1", 1),
            portname_instance("n", "c", "e2", 2),
        ];
        let (added, removed) = diff_snapshots(&current, &HashSet::new());
        assert_eq!(added.len(), 2);
        assert!(removed.is_empty());
    }

    /// ## 测试过程
    /// 已知集合含 1 个 id，当前快照空；调用 diff，断言 removed=1/added=0。
    /// ## 意义
    /// 验证“被注销”路径能正确产生 Removed 事件。
    #[test]
    fn diff_removed_only_when_current_empty() {
        let mut known = HashSet::new();
        known.insert(portname_instance("n", "c", "e", 7).id());
        let (added, removed) = diff_snapshots(&[], &known);
        assert!(added.is_empty());
        assert_eq!(removed.len(), 1);
    }

    // ── register / unregister / list ─────────────────────────────────────────

    /// ## 测试过程
    /// 注册一个 PortName，list 应返回 1 条且类型正确。
    /// ## 意义
    /// 验证最基础的 register→list 链路。
    #[tokio::test]
    async fn register_then_list() {
        let reg = SharedMockRegistry::new();
        let d = MockDiscovery::new(Some(1), reg);
        d.register(portname_spec("n", "c", "e")).await.unwrap();
        let all = d.list(DiscoveryQuery::AllPortNames).await.unwrap();
        assert_eq!(all.len(), 1);
        assert!(matches!(all[0], DiscoveryInstance::PortName(_)));
    }

    /// ## 测试过程
    /// 注册后注销，list 应返回空。
    /// ## 意义
    /// 验证 unregister 真正从 registry 中移除。
    #[tokio::test]
    async fn unregister_removes_instance() {
        let d = MockDiscovery::new(Some(1), SharedMockRegistry::new());
        let inst = d.register(portname_spec("n", "c", "e")).await.unwrap();
        d.unregister(inst).await.unwrap();
        assert!(d.list(DiscoveryQuery::AllPortNames).await.unwrap().is_empty());
    }

    /// ## 测试过程
    /// 同一实例连续注销两次，第二次也应返回 Ok。
    /// ## 意义
    /// 注销必须幂等，否则上层 fan-out 注销时会因竞态报错。
    #[tokio::test]
    async fn unregister_idempotent() {
        let d = MockDiscovery::new(Some(1), SharedMockRegistry::new());
        let inst = d.register(portname_spec("n", "c", "e")).await.unwrap();
        d.unregister(inst.clone()).await.unwrap();
        d.unregister(inst).await.unwrap();
    }

    /// ## 测试过程
    /// 注册 prod / dev 两个 namespace 的实例，用 namespace=prod 过滤。
    /// ## 意义
    /// 验证 namespace 过滤准确，避免跨命名空间数据泄漏。
    #[tokio::test]
    async fn list_namespaced_filter() {
        let d = MockDiscovery::new(Some(42), SharedMockRegistry::new());
        d.register(portname_spec("prod", "c", "e")).await.unwrap();
        d.register(portname_spec("dev", "c", "e")).await.unwrap();
        let prod = d
            .list(DiscoveryQuery::NamespacedPortNames { namespace: "prod".into() })
            .await
            .unwrap();
        assert_eq!(prod.len(), 1);
    }

    /// ## 测试过程
    /// 注册一个 Model，用 PortName 查询；list 应为空。
    /// ## 意义
    /// 跨类型查询零泄漏，回归保护 [QueryMatcher::matches] 的分派分支。
    #[tokio::test]
    async fn list_portname_query_does_not_match_model() {
        let d = MockDiscovery::new(Some(1), SharedMockRegistry::new());
        d.register(model_spec("n", "c", "e", "m")).await.unwrap();
        let all = d.list(DiscoveryQuery::AllPortNames).await.unwrap();
        assert!(all.is_empty());
    }

    /// ## 测试过程
    /// 两个 MockDiscovery 共享 registry，分别注册不同 portname；
    /// 任一一方都能在 list 中看到对方写入的实例。
    /// ## 意义
    /// 共享 registry 的语义验证（用于模拟跨 worker 可见性）。
    #[tokio::test]
    async fn shared_registry_cross_visibility() {
        let reg = SharedMockRegistry::new();
        let a = MockDiscovery::new(Some(1), reg.clone());
        let b = MockDiscovery::new(Some(2), reg.clone());
        a.register(portname_spec("n", "c", "e1")).await.unwrap();
        b.register(portname_spec("n", "c", "e2")).await.unwrap();
        let seen = a.list(DiscoveryQuery::AllPortNames).await.unwrap();
        assert_eq!(seen.len(), 2);
    }

    // ── list_and_watch ───────────────────────────────────────────────────────

    /// ## 测试过程
    /// 启动 watch 后异步 register，等待第一个事件。
    /// ## 意义
    /// 端到端验证 mock 的 watch 流能在 ≤100ms 内反映出 register 结果。
    #[tokio::test]
    async fn list_and_watch_emits_added_on_new_registration() {
        let reg = SharedMockRegistry::new();
        let d = MockDiscovery::new(Some(1), reg);
        let cancel = CancellationToken::new();
        let mut stream = d
            .list_and_watch(DiscoveryQuery::AllPortNames, Some(cancel.clone()))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
        d.register(portname_spec("n", "c", "e")).await.unwrap();
        let ev = tokio::time::timeout(std::time::Duration::from_millis(150), stream.next()).await;
        cancel.cancel();
        assert!(matches!(ev, Ok(Some(Ok(DiscoveryEvent::Added(_))))));
    }
}
