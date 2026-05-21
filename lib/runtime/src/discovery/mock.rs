// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 测试用发现后端：进程内共享注册表 + 10ms 轮询事件流。
//!
//! `MockDiscovery` 将注册状态存储在 `Arc<Mutex<Vec<DiscoveryInstance>>>` 中，
//! 多个实例可共享同一 [`SharedMockRegistry`]，模拟同一集群中不同 worker 的注册场景。

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use super::{
    Discovery, DiscoveryEvent, DiscoveryInstance, DiscoveryInstanceId, DiscoveryQuery,
    DiscoverySpec, DiscoveryStream,
};

const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// 全局单调递增 instance_id 计数器（`new(None, ...)` 时使用）。
static MOCK_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

// ══════════════════════════════════════════════════════════════════════════════
// SharedMockRegistry
// ══════════════════════════════════════════════════════════════════════════════

/// 进程内共享的发现实例注册表。
///
/// 多个 `MockDiscovery` 实例可共享同一注册表，模拟多节点场景。
#[derive(Debug, Clone, Default)]
pub struct SharedMockRegistry {
    instances: Arc<Mutex<Vec<DiscoveryInstance>>>,
}

impl SharedMockRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn snapshot(&self) -> Vec<DiscoveryInstance> {
        self.instances.lock().await.clone()
    }

    async fn push(&self, instance: DiscoveryInstance) {
        self.instances.lock().await.push(instance);
    }

    async fn remove_by_id(&self, instance_id: u64) {
        self.instances
            .lock()
            .await
            .retain(|i| i.instance_id() != instance_id);
    }

    pub async fn clear(&self) {
        self.instances.lock().await.clear();
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// MockDiscovery
// ══════════════════════════════════════════════════════════════════════════════

/// 测试用发现客户端。
///
/// `list_and_watch` 每 10ms 轮询一次注册表，通过 `HashSet<DiscoveryInstanceId>` diff
/// 生成 `Added` / `Removed` 事件。
pub struct MockDiscovery {
    instance_id: u64,
    registry: SharedMockRegistry,
}

impl MockDiscovery {
    /// 构造 MockDiscovery。
    ///
    /// - `instance_id = Some(id)`：使用指定值（测试中固定 ID 便于断言）
    /// - `instance_id = None`：从全局原子计数器自增分配（保证唯一，从 1 开始）
    pub fn new(instance_id: Option<u64>, registry: SharedMockRegistry) -> Self {
        let id = instance_id
            .unwrap_or_else(|| MOCK_ID_COUNTER.fetch_add(1, Ordering::SeqCst));
        Self { instance_id: id, registry }
    }

    /// 创建带独立注册表的 MockDiscovery（单测便捷方法）。
    pub fn standalone(instance_id: Option<u64>) -> Self {
        Self::new(instance_id, SharedMockRegistry::new())
    }

    pub fn registry(&self) -> &SharedMockRegistry {
        &self.registry
    }
}

impl std::fmt::Debug for MockDiscovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockDiscovery")
            .field("instance_id", &self.instance_id)
            .finish()
    }
}

#[async_trait]
impl Discovery for MockDiscovery {
    fn instance_id(&self) -> u64 {
        self.instance_id
    }

    async fn register_internal(
        &self,
        spec: DiscoverySpec,
    ) -> anyhow::Result<DiscoveryInstance> {
        let instance = spec.with_instance_id(self.instance_id);
        self.registry.push(instance.clone()).await;
        Ok(instance)
    }

    async fn unregister(&self, instance: DiscoveryInstance) -> anyhow::Result<()> {
        self.registry.remove_by_id(instance.instance_id()).await;
        Ok(())
    }

    async fn list(&self, query: DiscoveryQuery) -> anyhow::Result<Vec<DiscoveryInstance>> {
        let all = self.registry.snapshot().await;
        Ok(all.into_iter().filter(|i| matches_query(i, &query)).collect())
    }

    async fn list_and_watch(
        &self,
        query: DiscoveryQuery,
        cancel_token: Option<CancellationToken>,
    ) -> anyhow::Result<DiscoveryStream> {
        let registry = self.registry.clone();
        let cancel = cancel_token.unwrap_or_else(CancellationToken::new);

        let stream = async_stream::try_stream! {
            let mut known: HashSet<DiscoveryInstanceId> = HashSet::new();

            // 初始全量快照：发出所有匹配实例的 Added 事件
            {
                let initial = registry.snapshot().await;
                for inst in initial.into_iter().filter(|i| matches_query(i, &query)) {
                    known.insert(inst.id());
                    yield DiscoveryEvent::Added(inst);
                }
            }

            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(POLL_INTERVAL) => {}
                }

                let current: Vec<DiscoveryInstance> = registry
                    .snapshot()
                    .await
                    .into_iter()
                    .filter(|i| matches_query(i, &query))
                    .collect();

                let current_ids: HashSet<DiscoveryInstanceId> =
                    current.iter().map(|i| i.id()).collect();

                // Added：当前有但 known 中没有的
                for inst in &current {
                    if !known.contains(&inst.id()) {
                        yield DiscoveryEvent::Added(inst.clone());
                    }
                }

                // Removed：known 中有但当前没有的
                for id in &known {
                    if !current_ids.contains(id) {
                        yield DiscoveryEvent::Removed(id.clone());
                    }
                }

                known = current_ids;
            }
        };

        Ok(Box::pin(stream))
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// 内部辅助：穷举匹配所有合法/非法组合
// ══════════════════════════════════════════════════════════════════════════════

fn matches_query(instance: &DiscoveryInstance, query: &DiscoveryQuery) -> bool {
    use DiscoveryInstance as I;
    use DiscoveryQuery as Q;
    match (instance, query) {
        // ── PortName
        (I::PortName(_), Q::AllPortNames) => true,
        (I::PortName(i), Q::NamespacedPortNames { namespace }) => i.namespace == *namespace,
        (I::PortName(i), Q::ServiceGroupPortNames { namespace, servicegroup }) => {
            i.namespace == *namespace && i.servicegroup == *servicegroup
        }
        (I::PortName(i), Q::PortName { namespace, servicegroup, portname }) => {
            i.namespace == *namespace
                && i.servicegroup == *servicegroup
                && i.portname == *portname
        }
        // ── Model
        (I::Model { .. }, Q::AllModels) => true,
        (I::Model { namespace: ns, .. }, Q::NamespacedModels { namespace }) => ns == namespace,
        (I::Model { namespace: ns, servicegroup: sg, .. }, Q::ServiceGroupModels { namespace, servicegroup }) => {
            ns == namespace && sg == servicegroup
        }
        (I::Model { namespace: ns, servicegroup: sg, portname: pn, .. }, Q::PortNameModels { namespace, servicegroup, portname }) => {
            ns == namespace && sg == servicegroup && pn == portname
        }
        // ── EventChannel
        (I::EventChannel { namespace: ns, servicegroup: sg, topic: t, .. }, Q::EventChannels(eq)) => {
            eq.namespace.as_ref().map_or(true, |n| n == ns)
                && eq.servicegroup.as_ref().map_or(true, |s| s == sg)
                && eq.topic.as_ref().map_or(true, |tp| tp == t)
        }
        // 跨类型不匹配，明确返回 false
        _ => false,
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// Tests
// ══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discovery::{EventChannelQuery, EventTransport};
    use crate::servicegroup;
    use futures::StreamExt;

    fn make_portname_spec(ns: &str, sg: &str, pn: &str) -> DiscoverySpec {
        DiscoverySpec::PortName {
            namespace: ns.into(),
            servicegroup: sg.into(),
            portname: pn.into(),
            transport: servicegroup::TransportType::Nats(format!("{ns}.{sg}.{pn}")),
        }
    }

    #[tokio::test]
    async fn register_and_list() {
        let disco = MockDiscovery::standalone(Some(1));
        let inst = disco
            .register_internal(make_portname_spec("ns", "sg", "pn"))
            .await
            .unwrap();
        assert_eq!(inst.instance_id(), 1);

        let results = disco.list(DiscoveryQuery::AllPortNames).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], inst);
    }

    #[tokio::test]
    async fn unregister_removes() {
        let disco = MockDiscovery::standalone(Some(2));
        let inst = disco
            .register_internal(make_portname_spec("ns", "sg", "pn"))
            .await
            .unwrap();
        disco.unregister(inst).await.unwrap();
        assert!(disco.list(DiscoveryQuery::AllPortNames).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn shared_registry_multi_node() {
        let registry = SharedMockRegistry::new();
        let d1 = MockDiscovery::new(Some(1), registry.clone());
        let d2 = MockDiscovery::new(Some(2), registry.clone());

        d1.register_internal(make_portname_spec("ns", "sg", "pn"))
            .await
            .unwrap();
        d2.register_internal(make_portname_spec("ns", "sg", "pn"))
            .await
            .unwrap();

        let all = d1.list(DiscoveryQuery::AllPortNames).await.unwrap();
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn auto_increment_instance_id() {
        let a = MockDiscovery::standalone(None);
        let b = MockDiscovery::standalone(None);
        assert_ne!(a.instance_id(), b.instance_id());
        assert!(a.instance_id() > 0);
    }

    #[tokio::test]
    async fn list_and_watch_produces_initial_added_events() {
        let disco = MockDiscovery::standalone(Some(10));
        disco
            .register_internal(make_portname_spec("ns", "sg", "pn"))
            .await
            .unwrap();

        let cancel = CancellationToken::new();
        let mut stream = disco
            .list_and_watch(DiscoveryQuery::AllPortNames, Some(cancel.clone()))
            .await
            .unwrap();

        let event = stream.next().await.unwrap().unwrap();
        assert!(matches!(event, DiscoveryEvent::Added(_)));

        cancel.cancel();
    }

    #[tokio::test]
    async fn list_and_watch_detects_added() {
        let registry = SharedMockRegistry::new();
        let disco = MockDiscovery::new(Some(20), registry.clone());

        let cancel = CancellationToken::new();
        let mut stream = disco
            .list_and_watch(DiscoveryQuery::AllPortNames, Some(cancel.clone()))
            .await
            .unwrap();

        // 注册后等待下一个 poll 周期
        disco
            .register_internal(make_portname_spec("ns", "sg", "pn"))
            .await
            .unwrap();

        let event = tokio::time::timeout(Duration::from_millis(200), stream.next())
            .await
            .expect("timed out waiting for event")
            .unwrap()
            .unwrap();
        assert!(matches!(event, DiscoveryEvent::Added(_)));

        cancel.cancel();
    }

    #[tokio::test]
    async fn list_and_watch_detects_removed() {
        let registry = SharedMockRegistry::new();
        let disco = MockDiscovery::new(Some(30), registry.clone());

        let inst = disco
            .register_internal(make_portname_spec("ns", "sg", "pn"))
            .await
            .unwrap();

        let cancel = CancellationToken::new();
        let mut stream = disco
            .list_and_watch(DiscoveryQuery::AllPortNames, Some(cancel.clone()))
            .await
            .unwrap();

        // 消耗初始 Added 事件
        stream.next().await.unwrap().unwrap();

        disco.unregister(inst).await.unwrap();

        let event = tokio::time::timeout(Duration::from_millis(200), stream.next())
            .await
            .expect("timed out waiting for removed event")
            .unwrap()
            .unwrap();
        assert!(matches!(event, DiscoveryEvent::Removed(_)));

        cancel.cancel();
    }

    #[tokio::test]
    async fn event_channel_query() {
        let disco = MockDiscovery::standalone(Some(40));
        disco
            .register_internal(DiscoverySpec::EventChannel {
                namespace: "ns".into(),
                servicegroup: "sg".into(),
                topic: "kv-events".into(),
                transport: EventTransport::nats("ns.sg.kv-events"),
            })
            .await
            .unwrap();

        let results = disco
            .list(DiscoveryQuery::EventChannels(EventChannelQuery::all()))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);

        let empty = disco
            .list(DiscoveryQuery::EventChannels(EventChannelQuery::namespace("other")))
            .await
            .unwrap();
        assert!(empty.is_empty());
    }
}
