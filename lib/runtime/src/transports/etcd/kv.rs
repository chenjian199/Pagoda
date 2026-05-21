// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! etcd KV 操作：前缀监听、本地缓存和类型化前缀监听器。

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{mpsc, RwLock};
use tokio_util::sync::CancellationToken;

use crate::error::PagodaError;

// ─────────────────────── KvClient ────────────────────────────────

#[derive(Clone)]
pub struct KvClient {
    inner: etcd_client::Client,
}

impl KvClient {
    pub(crate) fn new(client: etcd_client::Client) -> Self {
        Self { inner: client }
    }

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, PagodaError> {
        let mut kv = self.inner.kv_client();
        let resp = kv.get(key, None).await
            .map_err(|e| PagodaError::cannot_connect(format!("etcd get {key}: {e}")))?;
        Ok(resp.kvs().first().map(|kv| kv.value().to_vec()))
    }

    pub async fn put(
        &self,
        key: &str,
        value: &[u8],
        lease_id: Option<super::lease::LeaseId>,
    ) -> Result<(), PagodaError> {
        let mut kv = self.inner.kv_client();
        let opts = lease_id.map(|id| etcd_client::PutOptions::new().with_lease(id));
        kv.put(key, value, opts).await
            .map_err(|e| PagodaError::cannot_connect(format!("etcd put {key}: {e}")))?;
        Ok(())
    }

    pub async fn delete(&self, key: &str) -> Result<bool, PagodaError> {
        let mut kv = self.inner.kv_client();
        let resp = kv.delete(key, None).await
            .map_err(|e| PagodaError::cannot_connect(format!("etcd delete {key}: {e}")))?;
        Ok(resp.deleted() > 0)
    }

    pub async fn get_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, PagodaError> {
        let mut kv = self.inner.kv_client();
        let opts = etcd_client::GetOptions::new().with_prefix();
        let resp = kv.get(prefix, Some(opts)).await
            .map_err(|e| PagodaError::cannot_connect(format!("etcd get_prefix {prefix}: {e}")))?;
        Ok(resp.kvs().iter().map(|kv| {
            (String::from_utf8_lossy(kv.key()).to_string(), kv.value().to_vec())
        }).collect())
    }
}

// ─────────────────────── WatchEvent ──────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchEventType {
    Put,
    Delete,
}

#[derive(Debug, Clone)]
pub struct WatchEvent {
    pub event_type: WatchEventType,
    pub key: String,
    pub value: Option<Vec<u8>>,
    pub revision: i64,
}

// ─────────────────────── PrefixWatcher ───────────────────────────

pub struct PrefixWatcher {
    pub(crate) prefix: String,
    rx: mpsc::Receiver<WatchEvent>,
    cancel: CancellationToken,
}

impl PrefixWatcher {
    pub async fn new(
        client: &super::connector::EtcdClient,
        prefix: String,
    ) -> Result<Self, PagodaError> {
        let cancel = CancellationToken::new();
        let (tx, rx) = mpsc::channel::<WatchEvent>(256);

        let mut watcher_client = client.raw();
        let prefix_clone = prefix.clone();
        let cancel_clone = cancel.clone();

        tokio::spawn(async move {
            let opts = etcd_client::WatchOptions::new().with_prefix();
            let (mut watcher, mut stream) =
                match watcher_client.watch(prefix_clone.as_str(), Some(opts)).await {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::error!("etcd watch failed for prefix {prefix_clone}: {e}");
                        return;
                    }
                };

            loop {
                tokio::select! {
                    _ = cancel_clone.cancelled() => {
                        let _ = watcher.cancel().await;
                        break;
                    }
                    msg = stream.message() => {
                        match msg {
                            Ok(Some(resp)) => {
                                let revision = resp.header().map(|h| h.revision()).unwrap_or(0);
                                for event in resp.events() {
                                    let event_type = match event.event_type() {
                                        etcd_client::EventType::Put => WatchEventType::Put,
                                        etcd_client::EventType::Delete => WatchEventType::Delete,
                                    };
                                    if let Some(kv) = event.kv() {
                                        let we = WatchEvent {
                                            value: if event_type == WatchEventType::Put {
                                                Some(kv.value().to_vec())
                                            } else {
                                                None
                                            },
                                            event_type,
                                            key: String::from_utf8_lossy(kv.key()).to_string(),
                                            revision,
                                        };
                                        let _ = tx.send(we).await;
                                    }
                                }
                            }
                            Ok(None) | Err(_) => break,
                        }
                    }
                }
            }
        });

        Ok(Self { prefix, rx, cancel })
    }

    pub async fn next_event(&mut self) -> Option<WatchEvent> {
        self.rx.recv().await
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn cancel(&self) {
        self.cancel.cancel();
    }
}

// ─────────────────────── KvCache ─────────────────────────────────

pub struct KvCache {
    prefix: String,
    data: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    cancel: CancellationToken,
}

impl KvCache {
    pub async fn new(
        client: &super::connector::EtcdClient,
        prefix: String,
    ) -> Result<Self, PagodaError> {
        let cancel = CancellationToken::new();
        let data: Arc<RwLock<HashMap<String, Vec<u8>>>> = Arc::new(RwLock::new(HashMap::new()));

        // 全量加载
        let kv_client = client.kv_client();
        let initial = kv_client.get_prefix(&prefix).await?;
        {
            let mut map = data.write().await;
            for (k, v) in initial {
                map.insert(k, v);
            }
        }

        // 启动增量监听
        let mut watcher = PrefixWatcher::new(client, prefix.clone()).await?;
        let data_clone = Arc::clone(&data);
        let cancel_clone = cancel.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_clone.cancelled() => break,
                    event = watcher.next_event() => {
                        match event {
                            Some(we) => {
                                let mut map = data_clone.write().await;
                                match we.event_type {
                                    WatchEventType::Put => {
                                        if let Some(v) = we.value {
                                            map.insert(we.key, v);
                                        }
                                    }
                                    WatchEventType::Delete => {
                                        map.remove(&we.key);
                                    }
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
        });

        Ok(Self { prefix, data, cancel })
    }

    pub async fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.data.read().await.get(key).cloned()
    }

    pub async fn snapshot(&self) -> HashMap<String, Vec<u8>> {
        self.data.read().await.clone()
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn stop(&self) {
        self.cancel.cancel();
    }
}

// ─────────────────────── TypedPrefixWatcher ──────────────────────

pub struct TypedPrefixWatcher<T> {
    inner: PrefixWatcher,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> TypedPrefixWatcher<T>
where
    T: serde::de::DeserializeOwned + Send + 'static,
{
    pub async fn new(
        client: &super::connector::EtcdClient,
        prefix: String,
    ) -> Result<Self, PagodaError> {
        let inner = PrefixWatcher::new(client, prefix).await?;
        Ok(Self { inner, _phantom: std::marker::PhantomData })
    }

    pub async fn next_event(&mut self) -> Option<TypedWatchEvent<T>> {
        let we = self.inner.next_event().await?;
        let value = match &we.event_type {
            WatchEventType::Put => {
                if let Some(bytes) = &we.value {
                    match serde_json::from_slice::<T>(bytes) {
                        Ok(v) => Some(v),
                        Err(e) => {
                            tracing::warn!(key = %we.key, error = %e, "failed to deserialize watch event value");
                            None
                        }
                    }
                } else {
                    None
                }
            }
            WatchEventType::Delete => None,
        };
        Some(TypedWatchEvent {
            event_type: we.event_type,
            key: we.key,
            value,
            revision: we.revision,
        })
    }
}

#[derive(Debug)]
pub struct TypedWatchEvent<T> {
    pub event_type: WatchEventType,
    pub key: String,
    pub value: Option<T>,
    pub revision: i64,
}
