// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 发现层通用工具函数。

use std::collections::HashMap;

use futures::StreamExt;
use serde::Deserialize;
use tokio::sync::watch;

use super::{DiscoveryEvent, DiscoveryStream};

/// 订阅发现事件流，实时维护 `instance_id → V` 的 HashMap，并通过 `watch` channel 广播。
///
/// - `Added` 事件：反序列化为 `T`，调用 `extractor` 提取字段 `V`，插入 HashMap；
/// - `Removed` 事件：按 `instance_id` 从 HashMap 中删除；
/// - 反序列化/提取失败：仅打印 warn 并继续，不中止流（保证部分故障不影响整体）。
///
/// 返回的 `Receiver` 可通过 `.borrow()` 无阻塞读取最新状态，适合请求处理热路径。
///
/// # 类型参数
/// - `T`：反序列化目标类型（如 `ModelDeploymentCard`）
/// - `V`：从 `T` 提取的字段类型（如 `ModelRuntimeConfig`）
/// - `F`：`T → V` 的提取函数
pub fn watch_and_extract_field<T, V, F>(
    stream: DiscoveryStream,
    extractor: F,
) -> watch::Receiver<HashMap<u64, V>>
where
    T: for<'de> Deserialize<'de> + 'static,
    V: Clone + Send + Sync + 'static,
    F: Fn(T) -> V + Send + 'static,
{
    let (tx, rx) = watch::channel(HashMap::new());

    tokio::spawn(async move {
        let mut state: HashMap<u64, V> = HashMap::new();
        let mut stream = stream;

        while let Some(result) = stream.next().await {
            match result {
                Ok(DiscoveryEvent::Added(instance)) => {
                    match instance.deserialize_model::<T>() {
                        Ok(typed) => {
                            let value = extractor(typed);
                            let id = instance.instance_id();
                            state.insert(id, value);
                            if tx.send(state.clone()).is_err() {
                                // 接收方已 drop，退出后台任务
                                break;
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                instance_id = instance.instance_id(),
                                error = %e,
                                "watch_and_extract_field: failed to deserialize model instance, skipping"
                            );
                        }
                    }
                }
                Ok(DiscoveryEvent::Removed(id)) => {
                    state.remove(&id.instance_id());
                    if tx.send(state.clone()).is_err() {
                        break;
                    }
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        "watch_and_extract_field: stream error, continuing"
                    );
                }
            }
        }
    });

    rx
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discovery::{Discovery, DiscoveryQuery, DiscoverySpec, MockDiscovery};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct FakeCard {
        display_name: String,
        runtime_cfg: String,
    }

    #[tokio::test]
    async fn watch_and_extract_tracks_added_removed() {
        let disco = MockDiscovery::standalone(Some(99));
        let card = FakeCard {
            display_name: "test-model".into(),
            runtime_cfg: "cfg-v1".into(),
        };
        let spec = DiscoverySpec::from_model("ns", "sg", "pn", &card).unwrap();
        let inst = disco.register_internal(spec).await.unwrap();
        let instance_id = inst.instance_id();

        let stream = disco
            .list_and_watch(DiscoveryQuery::AllModels, None)
            .await
            .unwrap();

        let rx = watch_and_extract_field::<FakeCard, String, _>(
            stream,
            |card| card.runtime_cfg,
        );

        // 等待 watch 更新
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        {
            let map = rx.borrow();
            assert_eq!(map.get(&instance_id).map(String::as_str), Some("cfg-v1"));
        }

        disco.unregister(inst).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        {
            let map = rx.borrow();
            assert!(!map.contains_key(&instance_id));
        }
    }
}
