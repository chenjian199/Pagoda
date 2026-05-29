// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 设计意图
//! 提供"跨命名空间/组件"的全局实例聚合视图,作为 `component.rs` 中按组件粒度
//! 列表函数的补集.调用方拿一份 `Vec<Instance>` 即可遍历整个发现平面里所有
//! 服务端点,无需手动迭代 namespace/component.
//!
//! # 外部契约
//! - `pub async fn list_all_instances(discovery_client: Arc<dyn Discovery>) -> anyhow::Result<Vec<Instance>>`
//!   - 仅返回 `DiscoveryInstance::Endpoint` 变体;其它变体(Model/EventChannel/...)被静默丢弃;
//!   - 返回的 `Vec<Instance>` **必须**已按 `Instance` 的 `Ord` 顺序排好;
//!   - 任何 `Discovery::list` 失败直接 `?` 透传.
//!
//! # 实现要点
//! - 用 `into_iter().fold` 把过滤与提取合并为单次遍历,避免临时迭代器链.
//! - 排序复用 `Vec::sort`(稳定排序),`Instance::Ord` 已经按
//!   `(namespace, component, endpoint, instance_id)` 字段顺序定义.

use std::sync::Arc;

use crate::component::Instance;
use crate::discovery::{Discovery, DiscoveryInstance, DiscoveryQuery};

// === SECTION: 公开 API ===

/// 列出整个发现平面里所有 `Endpoint` 类型的实例,按字典序返回.
pub async fn list_all_instances(
    discovery_client: Arc<dyn Discovery>,
) -> anyhow::Result<Vec<Instance>> {
    let raw = discovery_client.list(DiscoveryQuery::AllEndpoints).await?;
    let mut endpoints = collect_endpoints(raw);
    endpoints.sort();
    Ok(endpoints)
}

// === SECTION: 内部辅助 ===

/// 从原始 `DiscoveryInstance` 序列中抽取所有 `Endpoint` 变体.
///
/// 用一次 `fold` 完成过滤+解包,避免 `filter_map + match` 的双层语义.
fn collect_endpoints(items: Vec<DiscoveryInstance>) -> Vec<Instance> {
    items.into_iter().fold(Vec::new(), |mut acc, item| {
        if let DiscoveryInstance::Endpoint(instance) = item {
            acc.push(instance);
        }
        acc
    })
}

// === SECTION: tests ===

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::TransportType;
    use crate::discovery::{
        DiscoveryInstance, DiscoveryQuery, DiscoverySpec, DiscoveryStream,
    };
    use anyhow::Result;
    use async_trait::async_trait;
    use std::sync::Arc;

    // ── 最小化 Discovery Mock ────────────────────────────────────────────────
    //
    // 测试仅需实现 instance_id() 和 list() 两个方法，
    // 其余 trait 方法以 unimplemented!() 占位。

    struct MockDiscovery {
        /// list() 每次调用固定返回的实例集合。
        instances: Vec<DiscoveryInstance>,
        /// 为 true 时 list() 模拟传输层错误。
        fail: bool,
    }

    impl MockDiscovery {
        /// 创建一个每次 list() 都返回指定实例的 mock。
        fn with_instances(instances: Vec<DiscoveryInstance>) -> Arc<Self> {
            Arc::new(Self {
                instances,
                fail: false,
            })
        }

        /// 创建一个 list() 始终返回错误的 mock。
        fn failing() -> Arc<Self> {
            Arc::new(Self {
                instances: vec![],
                fail: true,
            })
        }
    }

    #[async_trait]
    impl Discovery for MockDiscovery {
        fn instance_id(&self) -> u64 {
            0
        }

        async fn list(&self, _query: DiscoveryQuery) -> Result<Vec<DiscoveryInstance>> {
            if self.fail {
                anyhow::bail!("simulated discovery error");
            }
            Ok(self.instances.clone())
        }

        async fn register_internal(&self, _spec: DiscoverySpec) -> Result<DiscoveryInstance> {
            unimplemented!("单元测试不需要此方法")
        }

        async fn unregister(&self, _instance: DiscoveryInstance) -> Result<()> {
            unimplemented!("单元测试不需要此方法")
        }

        async fn list_and_watch(
            &self,
            _query: DiscoveryQuery,
            _cancel_token: Option<crate::CancellationToken>,
        ) -> Result<DiscoveryStream> {
            unimplemented!("单元测试不需要此方法")
        }
    }

    // ── 辅助函数 ─────────────────────────────────────────────────────────────

    /// 构造一个使用内存 NATS 传输的最小化 Instance。
    fn make_instance(namespace: &str, component: &str, endpoint: &str, id: u64) -> Instance {
        Instance {
            namespace: namespace.to_string(),
            component: component.to_string(),
            endpoint: endpoint.to_string(),
            instance_id: id,
            transport: TransportType::Nats(format!("{namespace}.{component}.{endpoint}.{id:x}")),
            device_type: None,
        }
    }

    // ── 测试：发现平面为空 → 返回空 Vec ──────────────────────────────────────
    #[tokio::test]
    async fn returns_empty_vec_when_no_instances_registered() {
        let client = MockDiscovery::with_instances(vec![]);
        let result = list_all_instances(client).await.unwrap();
        assert!(result.is_empty(), "期望空 Vec，实际 {:?}", result);
    }

    // ── 测试：单个端点实例被正确返回 ──────────────────────────────────────────
    #[tokio::test]
    async fn returns_single_endpoint_instance() {
        let inst = make_instance("ns", "comp", "ep", 1);
        let client = MockDiscovery::with_instances(vec![DiscoveryInstance::Endpoint(inst.clone())]);
        let result = list_all_instances(client).await.unwrap();
        assert_eq!(result, vec![inst]);
    }

    // ── 测试：Model 变体被过滤掉 ─────────────────────────────────────────────
    #[tokio::test]
    async fn filters_out_model_card_variants() {
        let inst = make_instance("ns", "comp", "ep", 42);
        let model = DiscoveryInstance::Model {
            namespace: "ns".to_string(),
            component: "comp".to_string(),
            endpoint: "ep".to_string(),
            instance_id: 99,
            card_json: serde_json::json!({"display_name": "test-model"}),
            model_suffix: None,
        };
        let client = MockDiscovery::with_instances(vec![
            model,
            DiscoveryInstance::Endpoint(inst.clone()),
        ]);
        let result = list_all_instances(client).await.unwrap();
        assert_eq!(result, vec![inst], "Model 变体应被过滤掉");
    }

    // ── 测试：EventChannel 变体被过滤掉 ──────────────────────────────────────
    #[tokio::test]
    async fn filters_out_event_channel_variants() {
        let inst = make_instance("ns", "comp", "ep", 7);
        let channel = DiscoveryInstance::EventChannel {
            namespace: "ns".to_string(),
            component: "comp".to_string(),
            topic: "kv-events".to_string(),
            instance_id: 8,
            transport: crate::discovery::EventTransport::Nats {
                subject_prefix: "ns.comp.kv-events".to_string(),
            },
        };
        let client = MockDiscovery::with_instances(vec![
            channel,
            DiscoveryInstance::Endpoint(inst.clone()),
        ]);
        let result = list_all_instances(client).await.unwrap();
        assert_eq!(result, vec![inst], "EventChannel 变体应被过滤掉");
    }

    // ── 测试：发现平面只有模型卡时返回空 Vec ─────────────────────────────────
    #[tokio::test]
    async fn returns_empty_when_only_model_cards_present() {
        let model = DiscoveryInstance::Model {
            namespace: "ns".to_string(),
            component: "comp".to_string(),
            endpoint: "ep".to_string(),
            instance_id: 1,
            card_json: serde_json::json!({"display_name": "llama"}),
            model_suffix: None,
        };
        let client = MockDiscovery::with_instances(vec![model]);
        let result = list_all_instances(client).await.unwrap();
        assert!(result.is_empty());
    }

    // ── 测试：多个实例按 Instance Ord 实现排序 ──────────────────────────────
    #[tokio::test]
    async fn result_is_sorted() {
        // 以逆序插入，验证输出为正序
        let a = make_instance("ns", "comp", "ep", 3);
        let b = make_instance("ns", "comp", "ep", 1);
        let c = make_instance("ns", "comp", "ep", 2);
        let client = MockDiscovery::with_instances(vec![
            DiscoveryInstance::Endpoint(a.clone()),
            DiscoveryInstance::Endpoint(b.clone()),
            DiscoveryInstance::Endpoint(c.clone()),
        ]);
        let result = list_all_instances(client).await.unwrap();
        // Instance Ord 按 (namespace, component, endpoint, instance_id) 排序
        assert_eq!(result, vec![b, c, a], "结果必须已排序");
    }

    // ── 测试：跨命名空间实例全部返回且排序正确 ──────────────────────────────
    #[tokio::test]
    async fn multi_namespace_instances_are_returned_and_sorted() {
        let x = make_instance("alpha", "svc", "infer", 1);
        let y = make_instance("beta", "svc", "infer", 1);
        let z = make_instance("alpha", "svc", "infer", 2);
        let client = MockDiscovery::with_instances(vec![
            DiscoveryInstance::Endpoint(y.clone()),
            DiscoveryInstance::Endpoint(z.clone()),
            DiscoveryInstance::Endpoint(x.clone()),
        ]);
        let result = list_all_instances(client).await.unwrap();
        assert_eq!(result, vec![x, z, y]);
    }

    // ── 测试：发现错误被向上传播 ─────────────────────────────────────────────
    #[tokio::test]
    async fn propagates_discovery_error() {
        let client = MockDiscovery::failing();
        let result = list_all_instances(client).await;
        assert!(result.is_err(), "期望错误被传播");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("simulated discovery error"),
            "错误消息不符合预期: {msg}"
        );
    }

    // ── 测试：三种变体混合时只有 Endpoint 通过 ──────────────────────────────
    #[tokio::test]
    async fn mixed_variants_only_endpoints_returned() {
        let ep1 = make_instance("ns", "a", "ep", 1);
        let ep2 = make_instance("ns", "b", "ep", 2);
        let model = DiscoveryInstance::Model {
            namespace: "ns".to_string(),
            component: "a".to_string(),
            endpoint: "ep".to_string(),
            instance_id: 10,
            card_json: serde_json::json!({"display_name": "m"}),
            model_suffix: None,
        };
        let channel = DiscoveryInstance::EventChannel {
            namespace: "ns".to_string(),
            component: "a".to_string(),
            topic: "t".to_string(),
            instance_id: 20,
            transport: crate::discovery::EventTransport::Nats {
                subject_prefix: "ns.a.t".to_string(),
            },
        };
        let client = MockDiscovery::with_instances(vec![
            DiscoveryInstance::Endpoint(ep2.clone()),
            model,
            channel,
            DiscoveryInstance::Endpoint(ep1.clone()),
        ]);
        let result = list_all_instances(client).await.unwrap();
        assert_eq!(result, vec![ep1, ep2]);
    }

    // ── 测试：大量实例全部返回且排序正确 ────────────────────────────────────
    #[tokio::test]
    async fn handles_large_number_of_instances() {
        let n = 200u64;
        let raw: Vec<DiscoveryInstance> = (0..n)
            .map(|i| DiscoveryInstance::Endpoint(make_instance("ns", "comp", "ep", i)))
            .collect();
        let client = MockDiscovery::with_instances(raw);
        let result = list_all_instances(client).await.unwrap();
        assert_eq!(result.len(), n as usize);
        // 验证相邻元素已排序
        for pair in result.windows(2) {
            assert!(pair[0] <= pair[1], "结果必须已排序");
        }
    }

    // ── 测试：collect_endpoints 助手在零拷贝路径下行为一致 ───────────────────
    #[test]
    fn collect_endpoints_preserves_input_order() {
        let a = make_instance("z", "c", "e", 9);
        let b = make_instance("a", "c", "e", 1);
        let items = vec![
            DiscoveryInstance::Endpoint(a.clone()),
            DiscoveryInstance::Endpoint(b.clone()),
        ];
        // 助手本身不排序;排序是 list_all_instances 的额外步骤.
        let out = collect_endpoints(items);
        assert_eq!(out, vec![a, b]);
    }
}
