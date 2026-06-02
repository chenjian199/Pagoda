// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # Discovery 流的下游适配工具
//!
//! ## 设计意图
//!
//! 上层（KV 路由器、UI、控制面）大都不直接处理 `DiscoveryEvent` 流，而是
//! 关心“某个 worker 当前对应哪个字段值”。本模块提供 [`watch_and_extract_field`]
//! 这一**单一公开入口**，把：
//!
//! 1. 模型卡的反序列化（JSON → `T`）；
//! 2. 自定义提取闭包 `F: Fn(T) -> V`；
//! 3. 多 LoRA / 多 portname 同 `instance_id` 时的“折叠”策略；
//! 4. 通过 `watch::Receiver<HashMap<u64, V>>` 推送给下游；
//!
//! 这四件事打包到一个后台 task 中完成，避免每个调用方各自实现。
//!
//! ## 外部契约
//!
//! - `pub fn watch_and_extract_field<T, V, F>(stream, extractor) ->
//!   watch::Receiver<HashMap<u64, V>>` 作为稳定的公开签名；
//! - 折叠策略：当同一 `instance_id` 既有基础模型 (`suffix=None`) 又有 LoRA
//!   时，优先保留基础模型；若只有 LoRA 则任选其一。
//!
//! ## 实现要点
//!
//! - 内部状态键采用完整的 `DiscoveryInstanceId`（含 namespace / servicegroup /
//!   portname / 后缀），避免不同对象因低 53 位哈希冲突而互相覆盖；
//! - 折叠后的 `HashMap<u64, V>` 与上次广播值比较，仅在**内容真实变化**时
//!   `send`，避免下游被空唤醒；
//! - 折叠逻辑被抽离为 [`collapse_to_instance_view`]，可单独测试。

use std::collections::HashMap;

use futures::StreamExt;
use serde::Deserialize;
use tokio::sync::watch;

use super::{DiscoveryEvent, DiscoveryInstance, DiscoveryInstanceId, DiscoveryStream};

// === 内部辅助：状态折叠 =======================================================

/// 把 `HashMap<DiscoveryInstanceId, V>` 折叠成 `HashMap<u64, V>`。
///
/// ## 折叠规则
///
/// 同一 `instance_id` 可能对应：
/// - 一个基础模型条目（`Model{suffix=None}`）
/// - 任意数量的 LoRA 条目（`Model{suffix=Some}`）
/// - 不同 namespace/servicegroup/portname 上偶尔重复的条目
///
/// 选择策略：**先到的基础模型优先**；若桶里已经有任何条目且新条目是 LoRA，
/// 则保持原值不变。直观地说，下游通常只关心“这个 worker 在跑哪个基础模型”，
/// LoRA 仅作为附加 adapter 不应替换基础视图。
fn collapse_to_instance_view<V: Clone>(
    state: &HashMap<DiscoveryInstanceId, V>,
) -> HashMap<u64, V> {
    let mut out = HashMap::with_capacity(state.len());
    for (key, value) in state {
        let iid = key.instance_id();
        let is_lora_suffix = matches!(
            key,
            DiscoveryInstanceId::Model(m) if m.model_suffix.is_some()
        );
        // 基础模型 / PortName / EventChannel 一律允许插入或覆盖；
        // LoRA 只有当桶为空时才占位
        if !is_lora_suffix || !out.contains_key(&iid) {
            out.insert(iid, value.clone());
        }
    }
    out
}

// === 核心 API ================================================================

/// 监听一个 `DiscoveryStream` 并把每个模型卡的指定字段汇聚到 watch channel。
///
/// 调用方提供 `extractor: Fn(T) -> V`，由内部 task 自动完成：
/// 反序列化 → 提取字段 → 折叠 → 仅在变化时广播。
///
/// # 类型参数
/// - `T`：从 `DiscoveryInstance` 反序列化得到的中间类型（一般是 `ModelDeploymentCard`）；
/// - `V`：被提取的字段类型，必须 `PartialEq + Clone + Send + Sync + 'static`；
/// - `F`：闭包类型，必须 `Send + 'static`。
///
/// # 返回
/// `watch::Receiver<HashMap<u64, V>>`：调用方可随时 `.borrow()` 读取当前快照。
///
/// # 示例
/// ```ignore
/// let stream = discovery
///     .list_and_watch(DiscoveryQuery::ServiceGroupModels { .. }, None)
///     .await?;
/// let rx = watch_and_extract_field(stream, |card: ModelDeploymentCard| {
///     card.runtime_config
/// });
/// if let Some(cfg) = rx.borrow().get(&worker_id) {
///     // 使用 cfg ...
/// }
/// ```
pub fn watch_and_extract_field<T, V, F>(
    stream: DiscoveryStream,
    extractor: F,
) -> watch::Receiver<HashMap<u64, V>>
where
    T: for<'de> Deserialize<'de> + 'static,
    V: Clone + PartialEq + Send + Sync + 'static,
    F: Fn(T) -> V + Send + 'static,
{
    let (tx, rx) = watch::channel(HashMap::new());
    tokio::spawn(run_extractor_loop(stream, extractor, tx));
    rx
}

/// 后台任务主循环：消费 stream，维护 state，把折叠视图广播给 tx。
///
/// 抽离为独立 async fn 的目的：
/// 1. 让外部 `watch_and_extract_field` 主体仅有 `spawn` 一行；
/// 2. 把 Added / Removed 两条分支的相似代码（折叠 → 广播）集中到 [`publish_if_changed`]。
async fn run_extractor_loop<T, V, F>(
    mut stream: DiscoveryStream,
    extractor: F,
    tx: watch::Sender<HashMap<u64, V>>,
) where
    T: for<'de> Deserialize<'de> + 'static,
    V: Clone + PartialEq + Send + Sync + 'static,
    F: Fn(T) -> V + Send + 'static,
{
    let mut state: HashMap<DiscoveryInstanceId, V> = HashMap::new();

    while let Some(event) = stream.next().await {
        match event {
            Ok(DiscoveryEvent::Added(instance)) => {
                if !apply_added(&mut state, &instance, &extractor) {
                    continue;
                }
            }
            Ok(DiscoveryEvent::Removed(id)) => {
                state.remove(&id);
            }
            Err(e) => {
                tracing::error!(error = %e, "watch_and_extract_field stream error; ignored");
                continue;
            }
        }

        if !publish_if_changed(&state, &tx) {
            // 接收端已 drop，循环退出
            break;
        }
    }
    tracing::debug!("watch_and_extract_field background task exiting");
}

/// 处理一次 Added：反序列化 + 提取，写入 state。失败则跳过且返回 false。
fn apply_added<T, V, F>(
    state: &mut HashMap<DiscoveryInstanceId, V>,
    instance: &DiscoveryInstance,
    extractor: &F,
) -> bool
where
    T: for<'de> Deserialize<'de>,
    V: Clone,
    F: Fn(T) -> V,
{
    let deserialized: T = match instance.deserialize_model() {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(
                instance_id = instance.instance_id(),
                error = %e,
                "deserialize_model failed; entry skipped"
            );
            return false;
        }
    };
    state.insert(instance.id(), extractor(deserialized));
    true
}

/// 折叠当前 state 并与 tx 当前值比较；只在变化时 send。返回是否成功 send（true）
/// 或没必要 send（true）；仅在接收端已断开时返回 false。
fn publish_if_changed<V>(
    state: &HashMap<DiscoveryInstanceId, V>,
    tx: &watch::Sender<HashMap<u64, V>>,
) -> bool
where
    V: Clone + PartialEq,
{
    let collapsed = collapse_to_instance_view(state);
    if *tx.borrow() == collapsed {
        return true; // 无需广播但任务继续
    }
    tx.send(collapsed).is_ok()
}

// === 单元测试 =================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discovery::mock::{MockDiscovery, SharedMockRegistry};
    use crate::discovery::{
        Discovery, DiscoveryQuery, DiscoverySpec, PortNameInstanceId, ModelCardInstanceId,
    };

    #[derive(serde::Deserialize, Clone, Debug)]
    struct FakeCard {
        display_name: String,
    }

    fn base_model_spec(name: &str) -> DiscoverySpec {
        DiscoverySpec::Model {
            namespace: "ns".into(),
            servicegroup: "comp".into(),
            portname: "generate".into(),
            card_json: serde_json::json!({ "display_name": name }),
            model_suffix: None,
        }
    }

    fn mk_model_id(iid: u64, suffix: Option<&str>) -> DiscoveryInstanceId {
        DiscoveryInstanceId::Model(ModelCardInstanceId {
            namespace: "n".into(),
            servicegroup: "c".into(),
            portname: "e".into(),
            instance_id: iid,
            model_suffix: suffix.map(str::to_owned),
        })
    }

    fn mk_portname_id(iid: u64) -> DiscoveryInstanceId {
        DiscoveryInstanceId::PortName(PortNameInstanceId {
            namespace: "n".into(),
            servicegroup: "c".into(),
            portname: "e".into(),
            instance_id: iid,
        })
    }

    // ── collapse_to_instance_view ────────────────────────────────────────────

    /// ## 测试过程
    /// 构造同一 `instance_id` 既有基础模型又有 LoRA 的 state，折叠后断言
    /// 桶值为基础模型对应的 value。
    /// ## 意义
    /// LoRA 不应在已有基础模型时挤掉它，否则下游看到的“此 worker 在跑什么”
    /// 会被 adapter 名误导。
    #[test]
    fn collapse_prefers_base_over_lora_when_base_inserted_first() {
        let mut state: HashMap<DiscoveryInstanceId, &'static str> = HashMap::new();
        state.insert(mk_model_id(1, None), "base");
        state.insert(mk_model_id(1, Some("lora-a")), "lora");
        let view = collapse_to_instance_view(&state);
        assert_eq!(view.get(&1), Some(&"base"));
    }

    /// ## 测试过程
    /// 只插入 LoRA 条目，折叠后断言桶非空（占位）。
    /// ## 意义
    /// 即使只有 LoRA，下游仍要能看到“该 worker 存在”的信号。
    #[test]
    fn collapse_falls_back_to_lora_when_no_base() {
        let mut state: HashMap<DiscoveryInstanceId, &'static str> = HashMap::new();
        state.insert(mk_model_id(2, Some("only-lora")), "lora-v");
        let view = collapse_to_instance_view(&state);
        assert_eq!(view.get(&2), Some(&"lora-v"));
    }

    /// ## 测试过程
    /// 同一 `instance_id` 来自两条独立的 PortName key，折叠为同一桶。
    /// ## 意义
    /// 验证折叠键是 `instance_id` 而非完整 ID；这是上游想要的语义。
    #[test]
    fn collapse_groups_distinct_keys_by_instance_id() {
        let mut state: HashMap<DiscoveryInstanceId, &'static str> = HashMap::new();
        state.insert(mk_portname_id(7), "a");
        state.insert(mk_model_id(7, None), "b");
        let view = collapse_to_instance_view(&state);
        assert_eq!(view.len(), 1);
        assert!(matches!(view.get(&7), Some(&"a") | Some(&"b")));
    }

    // ── publish_if_changed ───────────────────────────────────────────────────

    /// ## 测试过程
    /// 让 state 与 tx 当前快照保持一致，调用 publish_if_changed；
    /// 接收端不应收到新版本。
    /// ## 意义
    /// 验证“无变化不广播”，避免下游被空唤醒。
    #[test]
    fn publish_skips_when_view_unchanged() {
        let (tx, rx) = watch::channel::<HashMap<u64, &'static str>>(HashMap::new());
        let state: HashMap<DiscoveryInstanceId, &'static str> = HashMap::new();
        assert!(publish_if_changed(&state, &tx));
        // borrow 当前值仍然是初始空 map，且未触发 changed
        assert!(rx.borrow().is_empty());
    }

    // ── 端到端：watch_and_extract_field ──────────────────────────────────────

    /// ## 测试过程
    /// 把 watch_and_extract_field 接到一个空 stream 上，立即读取 rx。
    /// ## 意义
    /// 验证初始值是空 map，调用方在 daemon 还未填充时不会拿到陈旧数据。
    #[tokio::test]
    async fn empty_stream_initial_value_is_empty_map() {
        let discovery = MockDiscovery::new(Some(1), SharedMockRegistry::new());
        let stream = discovery
            .list_and_watch(DiscoveryQuery::AllModels, None)
            .await
            .unwrap();
        let rx = watch_and_extract_field(stream, |c: FakeCard| c.display_name);
        assert!(rx.borrow().is_empty(), "初始视图应为空");
    }

    /// ## 测试过程
    /// 启动 watch_and_extract_field 后立即 drop rx，再触发一次 register；
    /// 期望 50ms 后无 panic。
    /// ## 意义
    /// 验证 watch::Sender 失败时 task 优雅退出，不留下泄露 task。
    #[tokio::test]
    async fn background_task_exits_when_receiver_dropped() {
        let discovery = MockDiscovery::new(Some(1), SharedMockRegistry::new());
        let stream = discovery
            .list_and_watch(DiscoveryQuery::AllModels, None)
            .await
            .unwrap();
        let rx = watch_and_extract_field(stream, |c: FakeCard| c.display_name);
        drop(rx);
        discovery.register(base_model_spec("t")).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        // 不 panic 即通过
    }

    /// ## 测试过程
    /// 注册一个基础模型，等待 rx 可见；断言映射 `{instance_id → display_name}` 正确。
    /// ## 意义
    /// 验证 stream → 反序列化 → 折叠 → 广播这条主路径端到端可用。
    #[tokio::test]
    async fn receives_model_after_registration() {
        let discovery = MockDiscovery::new(Some(7), SharedMockRegistry::new());
        let stream = discovery
            .list_and_watch(DiscoveryQuery::AllModels, None)
            .await
            .unwrap();
        let mut rx = watch_and_extract_field(stream, |c: FakeCard| c.display_name);
        discovery.register(base_model_spec("llama3")).await.unwrap();

        // 最多等 500ms（mock 内部以 10ms 轮询）
        for _ in 0..50 {
            if rx.borrow().get(&7).map(String::as_str) == Some("llama3") {
                return;
            }
            // changed() 可能因初始空状态广播立刻返回，所以用 sleep 而非纯 await
            let _ = tokio::time::timeout(
                tokio::time::Duration::from_millis(10),
                rx.changed(),
            )
            .await;
        }
        panic!("超时未收到 llama3：state={:?}", *rx.borrow());
    }
}
