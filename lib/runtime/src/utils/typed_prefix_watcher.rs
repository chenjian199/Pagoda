// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 设计意图
//! 把「监听 etcd 前缀 + 反序列化 + 聚合到 `HashMap<K, V>`」这套常见模式
//! 抽成可复用组件，避免各业务子系统重写 watch 循环。
//!
//! # 外部契约
//! - `TypedPrefixWatcher<K, V>`：对外暴露 `subscribe()` 返回 `watch::Receiver`，
//!   并提供 `borrow()` / `snapshot()` 等快照访问；
//! - 构造方法接受一个 `key_extractor: fn(&KeyValue) -> Option<K>`，
//!   把 etcd 原始 key 转为业务键；
//! - `pub mod key_extractors`：内置常用键提取器（按 `/` 取末段等）。
//!
//! # 实现要点
//! - 使用 `tokio::sync::watch` 暴露状态，订阅者天然支持 `borrow` + `changed`；
//! - 监听任务接受 `CancellationToken`，能与父级 runtime 联动停机；
//! - 反序列化失败的事件被记录并丢弃，避免污染状态映射。

use crate::transports::etcd::{Client as EtcdClient, WatchEvent};
use anyhow::Result;
use etcd_client::KeyValue;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::fmt::Debug;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

// === SECTION: TypedPrefixWatcher ===

/// 监听 etcd 前缀并维护反序列化后状态映射的通用 watcher。
///
/// 它会持续监听指定前缀，并把 `KeyValue` 提取出的键和值保存在 `HashMap` 中。
pub struct TypedPrefixWatcher<K, V>
where
    K: Clone + Eq + std::hash::Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    rx: watch::Receiver<HashMap<K, V>>,
}

impl<K, V> TypedPrefixWatcher<K, V>
where
    K: Clone + Eq + std::hash::Hash + Send + Sync + Debug + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// 获取当前状态的 watch 接收端。
    pub fn receiver(&self) -> watch::Receiver<HashMap<K, V>> {
        let receiver = self.rx.clone();
        receiver
    }

    /// 获取当前状态快照。
    pub fn current(&self) -> HashMap<K, V> {
        let snapshot = self.rx.borrow();
        snapshot.clone()
    }
}

/// 监听 etcd 前缀，并通过提取器维护一个键值映射。
///
/// 处理流程是先全量获取并订阅前缀，再在后台任务中根据 `Put/Delete` 事件更新内存状态，
/// 每次更新后通过 `watch` 广播最新快照。
///
/// # 示例
/// ```ignore
/// // Watch for ModelDeploymentCard objects and extract runtime_config field
/// let watcher = watch_prefix_with_extraction(
///     etcd_client,
///     "v1/mdc/",
///     |kv| Some(kv.lease()),  // Use lease_id as key
///     |card: ModelDeploymentCard| card.runtime_config,  // Extract runtime_config field
///     cancellation_token,
/// ).await?;
/// ```
pub async fn watch_prefix_with_extraction<K, V, T>(
    client: EtcdClient,
    prefix: impl Into<String>,
    key_extractor: impl Fn(&KeyValue) -> Option<K> + Send + 'static,
    value_extractor: impl Fn(T) -> Option<V> + Send + 'static,
    cancellation_token: CancellationToken,
) -> Result<TypedPrefixWatcher<K, V>>
where
    K: Clone + Eq + std::hash::Hash + Send + Sync + Debug + 'static,
    V: Clone + Send + Sync + 'static,
    T: DeserializeOwned + Send + 'static,
{
    let (watch_tx, watch_rx) = watch::channel(HashMap::new());
    let prefix = prefix.into();

    let prefix_watcher = client.kv_get_and_watch_prefix(&prefix).await?;
    let (prefix_str, mut events_rx) = prefix_watcher.dissolve();

    tokio::spawn(async move {
        let mut state: HashMap<K, V> = HashMap::new();

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    tracing::debug!("TypedPrefixWatcher for prefix '{prefix_str}' cancelled");
                    break;
                }
                event = events_rx.recv() => {
                    let Some(event) = event else {
                        tracing::debug!("TypedPrefixWatcher watch stream closed for prefix '{prefix_str}'");
                        break;
                    };

                    match event {
                        WatchEvent::Put(kv) => {
                            let Some(key) = key_extractor(&kv) else {
                                tracing::trace!("Skipping entry - key extractor returned None");
                                continue;
                            };

                            let deserialized = match serde_json::from_slice::<T>(kv.value()) {
                                Ok(val) => val,
                                Err(e) => {
                                    tracing::warn!(
                                        "Failed to deserialize value from etcd. Key: {}, Error: {}",
                                        kv.key_str().unwrap_or("<invalid>"),
                                        e
                                    );
                                    continue;
                                }
                            };

                            let next_value = value_extractor(deserialized);
                            match next_value {
                                Some(v) => {
                                    state.insert(key.clone(), v);
                                    tracing::trace!("Updated entry for key {:?}", key);
                                }
                                None => {
                                    state.remove(&key);
                                    tracing::trace!("Removed entry for key {:?} (extractor returned None)", key);
                                }
                            }

                            if watch_tx.send(state.clone()).is_err() {
                                tracing::error!("Failed to send update; receiver dropped");
                                break;
                            }
                        }
                        WatchEvent::Delete(kv) => {
                            let Some(key) = key_extractor(&kv) else {
                                continue;
                            };

                            state.remove(&key);
                            tracing::trace!("Removed entry for deleted key {:?}", key);

                            if watch_tx.send(state.clone()).is_err() {
                                tracing::error!("Failed to send update; receiver dropped");
                                break;
                            }
                        }
                    }
                }
            }
        }

        tracing::debug!("TypedPrefixWatcher for prefix '{prefix_str}' stopped");
    });

    Ok(TypedPrefixWatcher { rx: watch_rx })
}

/// 监听 etcd 前缀，并直接保存完整反序列化值。
///
/// 这是 `watch_prefix_with_extraction` 的简化版本，适合不需要二次提取字段的场景。
///
/// # 示例
/// ```ignore
/// // Watch for TestConfig objects directly
/// let watcher = watch_prefix(
///     etcd_client,
///     "configs/",
///     |kv| Some(kv.lease()),  // Use lease_id as key
///     cancellation_token,
/// ).await?;
/// ```
pub async fn watch_prefix<K, V>(
    client: EtcdClient,
    prefix: impl Into<String>,
    key_extractor: impl Fn(&KeyValue) -> Option<K> + Send + 'static,
    cancellation_token: CancellationToken,
) -> Result<TypedPrefixWatcher<K, V>>
where
    K: Clone + Eq + std::hash::Hash + Send + Sync + Debug + 'static,
    V: Clone + DeserializeOwned + Send + Sync + 'static,
{
    let watcher = watch_prefix_with_extraction(
        client,
        prefix,
        key_extractor,
        |v: V| Some(v), // 恒等映射，直接返回完整值。
        cancellation_token,
    )
    .await?;

    Ok(watcher)
}

/// 常用键提取器集合。
// === SECTION: key_extractors ===

pub mod key_extractors {
    use etcd_client::KeyValue;

    /// 提取租约 ID 作为键。
    pub fn lease_id(kv: &KeyValue) -> Option<u64> {
        let lease_id = kv.lease() as u64;
        Some(lease_id)
    }

    /// 提取去掉前缀后的键字符串。
    pub fn key_string(prefix: &str) -> impl Fn(&KeyValue) -> Option<String> {
        let prefix = prefix.to_string();
        move |kv: &KeyValue| {
            let key = kv.key_str().ok()?;
            let stripped = key.strip_prefix(&prefix).unwrap_or(key);
            Some(stripped.to_string())
        }
    }

    /// 提取完整键字符串。
    pub fn full_key_string(kv: &KeyValue) -> Option<String> {
        let key = kv.key_str().ok()?;
        Some(key.to_string())
    }
}

// === SECTION: tests ===

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_receiver_and_current_clone_state() {
        // 测试 receiver 与 current 都能返回当前状态副本。
        let mut state = HashMap::new();
        state.insert("alpha".to_string(), 1_u32);
        let (_tx, rx) = watch::channel(state.clone());
        let watcher = TypedPrefixWatcher { rx };

        assert_eq!(watcher.current(), state);
        assert_eq!(watcher.receiver().borrow().get("alpha"), Some(&1));
    }
}
