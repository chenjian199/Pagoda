// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 基于通用 KV 存储的 Discovery 实现
//!
//! ## 设计意图
//!
//! `KVStoreDiscovery` 是 `Discovery` trait 在 etcd / 内存 KV 后端上的具体实现，
//! 它把 Dynamo 的三类发现对象分桶序列化为 JSON 字节串：
//!
//! | 类别              | 桶名              | key 模式                                    |
//! |------------------|------------------|--------------------------------------------|
//! | `Endpoint`       | `v1/instances`   | `{ns}/{comp}/{ep}/{instance_id:x}`         |
//! | `Model` (基础)    | `v1/mdc`         | `{ns}/{comp}/{ep}/{instance_id:x}`         |
//! | `Model` (LoRA)    | `v1/mdc`         | `{ns}/{comp}/{ep}/{instance_id:x}/{suffix}`|
//! | `EventChannel`   | `v1/event_channels` | `{ns}/{comp}/{topic}/{instance_id:x}`   |
//!
//! ## 外部契约
//!
//! - 公开类型 [`KVStoreDiscovery`] 与历史版本签名一致：
//!   `KVStoreDiscovery::new(kv::Manager, CancellationToken)`。
//! - `Discovery` trait 的 `register / unregister / list / list_and_watch /
//!   shutdown` 全部实现并保持原有语义。
//!
//! ## 实现要点
//!
//! - **键路径计算**集中到 [`KeySchema`] 静态方法，避免 register/unregister/list
//!   三个地方重复实现 `format!`；
//! - **桶名选择**通过 [`BucketRoute`] 枚举与 [`BucketRoute::from_query`]
//!   集中决策，原版散落多处的 `starts_with` 分支被收敛；
//! - **删除事件 key 反序列化**抽离为 [`parse_deleted_key`] 单一函数，
//!   单元测试可以独立覆盖其边界条件（如长度不足、hex 解析失败、缺 suffix）；
//! - `list_and_watch` 不再就地内联 `match (Put / Delete)` 大块代码，而是
//!   委托给 [`watch_event_into_discovery_event`]，让 stream 主体保持简洁。

use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use futures::Stream;
use tokio_util::sync::CancellationToken;

use super::{
    Discovery, DiscoveryEvent, DiscoveryInstance, DiscoveryInstanceId, DiscoveryQuery,
    DiscoverySpec, DiscoveryStream, EndpointInstanceId, EventChannelInstanceId,
    ModelCardInstanceId,
};
use crate::storage::kv;

// === 桶名常量 =================================================================

const INSTANCES_BUCKET: &str = "v1/instances";
const MODELS_BUCKET: &str = "v1/mdc";
const EVENT_CHANNELS_BUCKET: &str = "v1/event_channels";

// === BucketRoute：桶选择枚举 ==================================================

/// 把 `DiscoveryQuery` 与具体桶名建立映射，集中决策点。
///
/// 原版在 register / list / watch 中都各写一份 “if prefix.starts_with(...)”
/// 的判断；提炼出 enum 后只需一个静态方法即可在三处复用。
#[derive(Clone, Copy)]
enum BucketRoute {
    Endpoints,
    Models,
    EventChannels,
}

impl BucketRoute {
    /// 桶在底层 KV store 中的名称。
    fn name(self) -> &'static str {
        match self {
            Self::Endpoints => INSTANCES_BUCKET,
            Self::Models => MODELS_BUCKET,
            Self::EventChannels => EVENT_CHANNELS_BUCKET,
        }
    }

    /// 由 `DiscoveryQuery` 选择对应桶。
    fn from_query(query: &DiscoveryQuery) -> Self {
        match query {
            DiscoveryQuery::AllEndpoints
            | DiscoveryQuery::NamespacedEndpoints { .. }
            | DiscoveryQuery::ComponentEndpoints { .. }
            | DiscoveryQuery::Endpoint { .. } => Self::Endpoints,

            DiscoveryQuery::AllModels
            | DiscoveryQuery::NamespacedModels { .. }
            | DiscoveryQuery::ComponentModels { .. }
            | DiscoveryQuery::EndpointModels { .. } => Self::Models,

            DiscoveryQuery::EventChannels(_) => Self::EventChannels,
        }
    }

    /// 由 `DiscoveryInstance` 选择对应桶。
    fn from_instance(inst: &DiscoveryInstance) -> Self {
        match inst {
            DiscoveryInstance::Endpoint(_) => Self::Endpoints,
            DiscoveryInstance::Model { .. } => Self::Models,
            DiscoveryInstance::EventChannel { .. } => Self::EventChannels,
        }
    }
}

// === KeySchema：键路径计算 ====================================================

/// 集中管理所有 key 路径生成逻辑。
///
/// 注意：返回值都是**相对**于桶名的路径（不带 `bucket/` 前缀），
/// 这样可以同时兼容把桶前缀视作 etcd key namespace 的实现与不带前缀的内存实现。
struct KeySchema;

impl KeySchema {
    /// `{ns}/{comp}/{ep}/{id:x}` — Endpoint / 基础 Model 通用前 4 段。
    fn quad(a: &str, b: &str, c: &str, id: u64) -> String {
        format!("{a}/{b}/{c}/{id:x}")
    }

    /// 为 Model 实例追加 LoRA suffix（若有且非空）。
    fn model_key(ns: &str, comp: &str, ep: &str, id: u64, suffix: Option<&str>) -> String {
        let base = Self::quad(ns, comp, ep, id);
        match suffix {
            Some(s) if !s.is_empty() => format!("{base}/{s}"),
            _ => base,
        }
    }

    /// 把 query 转换为相对 / 绝对 prefix（绝对时带桶前缀，便于打印）。
    fn query_prefix(query: &DiscoveryQuery) -> String {
        let bucket = BucketRoute::from_query(query).name();
        match query {
            DiscoveryQuery::AllEndpoints | DiscoveryQuery::AllModels => bucket.to_owned(),

            DiscoveryQuery::NamespacedEndpoints { namespace }
            | DiscoveryQuery::NamespacedModels { namespace } => format!("{bucket}/{namespace}"),

            DiscoveryQuery::ComponentEndpoints { namespace, component }
            | DiscoveryQuery::ComponentModels { namespace, component } => {
                format!("{bucket}/{namespace}/{component}")
            }

            DiscoveryQuery::Endpoint { namespace, component, endpoint }
            | DiscoveryQuery::EndpointModels { namespace, component, endpoint } => {
                format!("{bucket}/{namespace}/{component}/{endpoint}")
            }

            DiscoveryQuery::EventChannels(q) => {
                let mut path = bucket.to_owned();
                if let Some(ns) = &q.namespace {
                    path.push('/');
                    path.push_str(ns);
                    if let Some(c) = &q.component {
                        path.push('/');
                        path.push_str(c);
                        if let Some(t) = &q.topic {
                            path.push('/');
                            path.push_str(t);
                        }
                    }
                }
                path
            }
        }
    }

    /// 把可能带 `bucket/` 前缀的 key 转换为相对路径。
    fn strip_bucket<'a>(key: &'a str, bucket: &str) -> &'a str {
        key.strip_prefix(bucket)
            .map(|rest| rest.strip_prefix('/').unwrap_or(rest))
            .unwrap_or(key)
    }

    /// 比较 key 与 prefix（两者都不假定带桶前缀，由本函数统一规范）。
    fn matches_prefix(key: &str, prefix: &str, bucket: &str) -> bool {
        let rel_key = Self::strip_bucket(key, bucket);
        let rel_pref = Self::strip_bucket(prefix, bucket);
        rel_pref.is_empty() || rel_key.starts_with(rel_pref)
    }
}

// === Delete 事件 key 解析 =====================================================

/// 把 KV 删除事件的裸 key 解析为 [`DiscoveryInstanceId`]。
///
/// 删除事件在底层 store 中通常没有 value（只剩 key），必须能从 key 反推
/// 出三类 ID。本函数处理三种格式：
///
/// | 桶            | 段数                           | ID 类型             |
/// |---------------|-------------------------------|--------------------|
/// | `instances`   | 4 (`ns/comp/ep/id`)            | `EndpointInstanceId`|
/// | `mdc`         | 4 或 5 (`.../id[/suffix]`)     | `ModelCardInstanceId`|
/// | `event_channels` | 4 (`ns/comp/topic/id`)      | `EventChannelInstanceId`|
fn parse_deleted_key(key: &str, bucket: BucketRoute) -> Option<DiscoveryInstanceId> {
    let rel = KeySchema::strip_bucket(key, bucket.name());
    let parts: Vec<&str> = rel.split('/').collect();
    if parts.len() < 4 {
        return None;
    }
    let id = u64::from_str_radix(parts[3], 16).ok()?;
    let ns = parts[0].to_owned();
    let comp = parts[1].to_owned();
    let third = parts[2].to_owned();

    Some(match bucket {
        BucketRoute::Endpoints => DiscoveryInstanceId::Endpoint(EndpointInstanceId {
            namespace: ns,
            component: comp,
            endpoint: third,
            instance_id: id,
        }),
        BucketRoute::Models => DiscoveryInstanceId::Model(ModelCardInstanceId {
            namespace: ns,
            component: comp,
            endpoint: third,
            instance_id: id,
            model_suffix: parts.get(4).map(|s| (*s).to_owned()),
        }),
        BucketRoute::EventChannels => DiscoveryInstanceId::EventChannel(EventChannelInstanceId {
            namespace: ns,
            component: comp,
            topic: third,
            instance_id: id,
        }),
    })
}

// === KVStoreDiscovery 类型 ====================================================

/// 由 `kv::Manager` 提供后端的 Discovery 实现。
pub struct KVStoreDiscovery {
    store: Arc<kv::Manager>,
    cancel_token: CancellationToken,
}

impl KVStoreDiscovery {
    pub fn new(store: kv::Manager, cancel_token: CancellationToken) -> Self {
        Self {
            store: Arc::new(store),
            cancel_token,
        }
    }

    /// 由 `DiscoveryInstance` 计算 `(桶名, key)`，集中调用 `KeySchema`。
    fn instance_to_route(instance: &DiscoveryInstance) -> (BucketRoute, String) {
        let bucket = BucketRoute::from_instance(instance);
        let key = match instance {
            DiscoveryInstance::Endpoint(inst) => KeySchema::quad(
                &inst.namespace,
                &inst.component,
                &inst.endpoint,
                inst.instance_id,
            ),
            DiscoveryInstance::Model {
                namespace,
                component,
                endpoint,
                instance_id,
                model_suffix,
                ..
            } => KeySchema::model_key(
                namespace,
                component,
                endpoint,
                *instance_id,
                model_suffix.as_deref(),
            ),
            DiscoveryInstance::EventChannel {
                namespace,
                component,
                topic,
                instance_id,
                ..
            } => KeySchema::quad(namespace, component, topic, *instance_id),
        };
        (bucket, key)
    }

    /// 把 watch 事件转换为 `DiscoveryEvent`，过滤掉与 prefix 不匹配的更新。
    fn watch_event_into_discovery_event(
        event: kv::WatchEvent,
        prefix: &str,
        bucket: BucketRoute,
    ) -> Option<DiscoveryEvent> {
        match event {
            kv::WatchEvent::Put(kv) => {
                if !KeySchema::matches_prefix(kv.key_str(), prefix, bucket.name()) {
                    return None;
                }
                match serde_json::from_slice::<DiscoveryInstance>(kv.value()) {
                    Ok(instance) => Some(DiscoveryEvent::Added(instance)),
                    Err(e) => {
                        tracing::warn!(key = %kv.key_str(), error = %e, "parse Put failed");
                        None
                    }
                }
            }
            kv::WatchEvent::Delete(kv) => {
                let key_str = kv.as_ref();
                if !KeySchema::matches_prefix(key_str, prefix, bucket.name()) {
                    return None;
                }
                parse_deleted_key(key_str, bucket).map(DiscoveryEvent::Removed)
            }
        }
    }
}

// === Discovery trait 实现 =====================================================

#[async_trait]
impl Discovery for KVStoreDiscovery {
    fn instance_id(&self) -> u64 {
        self.store.connection_id()
    }

    async fn register_internal(&self, spec: DiscoverySpec) -> Result<DiscoveryInstance> {
        let instance = spec.with_instance_id(self.instance_id());
        let (bucket, key_path) = Self::instance_to_route(&instance);
        let payload = serde_json::to_vec(&instance)?;

        tracing::debug!(
            bucket = bucket.name(),
            key = %key_path,
            bytes = payload.len(),
            "register_internal write"
        );

        let store = self.store.get_or_create_bucket(bucket.name(), None).await?;
        let key = kv::Key::new(key_path);
        // revision=0：首次写入，etcd / 内存后端都以此约定表示无 CAS 期望
        store.insert(&key, payload.into(), 0).await?;
        Ok(instance)
    }

    async fn unregister(&self, instance: DiscoveryInstance) -> Result<()> {
        let (bucket, key_path) = Self::instance_to_route(&instance);
        let Some(store) = self.store.get_bucket(bucket.name()).await? else {
            // 桶不存在 → 本就没注册过，幂等返回
            tracing::warn!(bucket = bucket.name(), "bucket missing on unregister; ignored");
            return Ok(());
        };
        store.delete(&kv::Key::new(key_path)).await?;
        Ok(())
    }

    async fn list(&self, query: DiscoveryQuery) -> Result<Vec<DiscoveryInstance>> {
        let prefix = KeySchema::query_prefix(&query);
        let bucket = BucketRoute::from_query(&query);

        let Some(store) = self.store.get_bucket(bucket.name()).await? else {
            tracing::debug!(bucket = bucket.name(), %prefix, "list: bucket missing");
            return Ok(Vec::new());
        };

        let mut out = Vec::new();
        for (key, value) in store.entries().await? {
            if KeySchema::matches_prefix(key.as_ref(), &prefix, bucket.name()) {
                match serde_json::from_slice::<DiscoveryInstance>(&value) {
                    Ok(inst) => out.push(inst),
                    Err(e) => tracing::warn!(%key, error = %e, "skip bad entry"),
                }
            }
        }
        Ok(out)
    }

    async fn list_and_watch(
        &self,
        query: DiscoveryQuery,
        cancel_token: Option<CancellationToken>,
    ) -> Result<DiscoveryStream> {
        let prefix = KeySchema::query_prefix(&query);
        let bucket = BucketRoute::from_query(&query);

        // 缺省取本 client 的 cancel token；保持上层调用语义不变
        let cancel = cancel_token.unwrap_or_else(|| self.cancel_token.clone());
        let (_, mut rx) = self.store.clone().watch(bucket.name(), None, cancel);

        let stream = async_stream::stream! {
            while let Some(event) = rx.recv().await {
                if let Some(ev) = Self::watch_event_into_discovery_event(event, &prefix, bucket) {
                    yield Ok(ev);
                }
            }
        };
        let pinned: Pin<Box<dyn Stream<Item = Result<DiscoveryEvent>> + Send>> = Box::pin(stream);
        Ok(pinned)
    }

    fn shutdown(&self) {
        self.store.shutdown();
    }
}

// === 单元测试 =================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::TransportType;
    use crate::discovery::EventChannelQuery;
    use futures::StreamExt as _;

    fn endpoint_spec(ns: &str, comp: &str, ep: &str) -> DiscoverySpec {
        DiscoverySpec::Endpoint {
            namespace: ns.into(),
            component: comp.into(),
            endpoint: ep.into(),
            transport: TransportType::Nats("nats://localhost:4222".into()),
            device_type: None,
        }
    }

    // ── KeySchema ────────────────────────────────────────────────────────────

    /// ## 测试过程
    /// 调用 `KeySchema::quad`，断言结果格式为 `a/b/c/{id:x}`。
    /// ## 意义
    /// 防止键格式被无意中修改，破坏旧版数据的兼容性。
    #[test]
    fn key_schema_quad_format() {
        assert_eq!(KeySchema::quad("ns", "c", "e", 0xff), "ns/c/e/ff");
    }

    /// ## 测试过程
    /// 对 `model_key` 分别传入 `None`、`Some("")`、`Some("v1")`，比较结果。
    /// ## 意义
    /// 验证空 suffix 退化为基础键，避免出现尾部 `/` 引发的两条不同键指向
    /// “同一个”模型。
    #[test]
    fn key_schema_model_suffix_empty_collapses() {
        let base = KeySchema::model_key("n", "c", "e", 1, None);
        let empty = KeySchema::model_key("n", "c", "e", 1, Some(""));
        let with = KeySchema::model_key("n", "c", "e", 1, Some("v1"));
        assert_eq!(base, empty);
        assert_eq!(with, "n/c/e/1/v1");
    }

    /// ## 测试过程
    /// 对 `query_prefix` 输入 `EventChannels` 不同填充程度，比较拼接结果。
    /// ## 意义
    /// EventChannel 的 prefix 是动态拼接，必须按段累加，否则 watch 过滤异常。
    #[test]
    fn key_schema_event_channel_prefix_partial_fields() {
        let p_all = KeySchema::query_prefix(&DiscoveryQuery::EventChannels(
            EventChannelQuery::all(),
        ));
        let p_ns = KeySchema::query_prefix(&DiscoveryQuery::EventChannels(
            EventChannelQuery::namespace("ns"),
        ));
        let p_comp = KeySchema::query_prefix(&DiscoveryQuery::EventChannels(
            EventChannelQuery::component("ns", "comp"),
        ));
        assert_eq!(p_all, EVENT_CHANNELS_BUCKET);
        assert!(p_ns.ends_with("/ns"));
        assert!(p_comp.ends_with("/ns/comp"));
    }

    /// ## 测试过程
    /// 用带前缀的 key 与裸 key 调用 `matches_prefix`，验证两者都通过。
    /// ## 意义
    /// 兼容 etcd（key 含桶前缀）与内存（不含）两种后端。
    #[test]
    fn key_schema_matches_prefix_handles_both_formats() {
        let bucket = INSTANCES_BUCKET;
        assert!(KeySchema::matches_prefix("ns/c/e/1", "ns/c", bucket));
        assert!(KeySchema::matches_prefix(
            &format!("{bucket}/ns/c/e/1"),
            "ns/c",
            bucket
        ));
        assert!(!KeySchema::matches_prefix("other/x", "ns/c", bucket));
    }

    // ── parse_deleted_key ────────────────────────────────────────────────────

    /// ## 测试过程
    /// 用合法的 Endpoint key 调用 `parse_deleted_key`，匹配返回的枚举类型与字段。
    /// ## 意义
    /// 删除事件没有 value，只能从 key 还原 ID；此路径必须严格正确。
    #[test]
    fn parse_deleted_key_endpoint() {
        let id = parse_deleted_key("ns/c/e/2a", BucketRoute::Endpoints).unwrap();
        match id {
            DiscoveryInstanceId::Endpoint(e) => {
                assert_eq!(e.namespace, "ns");
                assert_eq!(e.component, "c");
                assert_eq!(e.endpoint, "e");
                assert_eq!(e.instance_id, 0x2a);
            }
            _ => panic!("期望 Endpoint 变体"),
        }
    }

    /// ## 测试过程
    /// 用带 LoRA suffix 的 key 调用 `parse_deleted_key`。
    /// ## 意义
    /// 第 5 段是 LoRA 标识，必须能被独立解出。
    #[test]
    fn parse_deleted_key_model_with_suffix() {
        let id = parse_deleted_key("ns/c/e/1/lora-a", BucketRoute::Models).unwrap();
        match id {
            DiscoveryInstanceId::Model(m) => {
                assert_eq!(m.model_suffix.as_deref(), Some("lora-a"));
            }
            _ => panic!("期望 Model 变体"),
        }
    }

    /// ## 测试过程
    /// 用段数不足或 hex 非法的 key 调用 `parse_deleted_key`。
    /// ## 意义
    /// 边缘失败必须返回 `None`，避免 watch 流向上层抛出错乱事件。
    #[test]
    fn parse_deleted_key_invalid_returns_none() {
        assert!(parse_deleted_key("too/short", BucketRoute::Endpoints).is_none());
        assert!(parse_deleted_key("ns/c/e/zz", BucketRoute::Endpoints).is_none());
    }

    // ── 集成：内存后端端到端 ───────────────────────────────────────────────────

    /// ## 测试过程
    /// 用内存后端注册一个 Endpoint，然后 list 查询。
    /// ## 意义
    /// 验证 register/list 完整链路，确保桶选择与 key 计算一致。
    #[tokio::test]
    async fn register_then_list_endpoint() {
        let store = kv::Manager::memory();
        let client = KVStoreDiscovery::new(store, CancellationToken::new());
        let inst = client.register(endpoint_spec("t", "c", "e")).await.unwrap();
        match inst {
            DiscoveryInstance::Endpoint(_) => {}
            _ => panic!("期望 Endpoint"),
        }
        let all = client.list(DiscoveryQuery::AllEndpoints).await.unwrap();
        assert_eq!(all.len(), 1);
    }

    /// ## 测试过程
    /// 注册 ns1/c1/e1、ns1/c1/e2、ns2/c2/e1，然后分别按 All / Namespaced /
    /// Component 三种粒度 list，断言数量。
    /// ## 意义
    /// 验证 prefix 匹配在多种粒度下都正确。
    #[tokio::test]
    async fn list_filters_by_query_granularity() {
        let client = KVStoreDiscovery::new(kv::Manager::memory(), CancellationToken::new());
        for (ns, comp, ep) in [("ns1", "c1", "e1"), ("ns1", "c1", "e2"), ("ns2", "c2", "e1")] {
            client.register(endpoint_spec(ns, comp, ep)).await.unwrap();
        }
        assert_eq!(client.list(DiscoveryQuery::AllEndpoints).await.unwrap().len(), 3);
        assert_eq!(
            client
                .list(DiscoveryQuery::NamespacedEndpoints { namespace: "ns1".into() })
                .await
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            client
                .list(DiscoveryQuery::ComponentEndpoints {
                    namespace: "ns1".into(),
                    component: "c1".into(),
                })
                .await
                .unwrap()
                .len(),
            2
        );
    }

    /// ## 测试过程
    /// 启动 watch 后异步 register，等待第一个事件。
    /// ## 意义
    /// 验证 list_and_watch 能正确把 KV Put 转换为 Added 事件。
    #[tokio::test]
    async fn watch_emits_added_event_on_register() {
        let cancel = CancellationToken::new();
        let client = Arc::new(KVStoreDiscovery::new(kv::Manager::memory(), cancel.clone()));
        let mut stream = client
            .list_and_watch(DiscoveryQuery::AllEndpoints, None)
            .await
            .unwrap();

        let cli = client.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(40)).await;
            cli.register(endpoint_spec("t", "c", "e")).await.unwrap();
        });

        let event = stream.next().await.unwrap().unwrap();
        match event {
            DiscoveryEvent::Added(DiscoveryInstance::Endpoint(_)) => {}
            other => panic!("期望 Added(Endpoint)，得到 {other:?}"),
        }
        cancel.cancel();
    }
}
