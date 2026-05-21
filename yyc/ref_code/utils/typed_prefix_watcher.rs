use super::tasks::critical::CancellationToken;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
    pub lease_id: u64,
}

impl KeyValue {
    pub fn new(key: impl Into<String>, value: impl Into<String>, lease_id: u64) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
            lease_id,
        }
    }
}

pub trait EtcdClient: Clone + Send + 'static {
    fn list_prefix(&self, prefix: &str) -> Vec<KeyValue>;
    fn watch_prefix(&self, prefix: &str) -> Vec<Vec<KeyValue>>;
}

struct WatchState<T: Clone> {
    data: Mutex<(T, u64)>,
    condvar: Condvar,
}

#[derive(Clone)]
pub struct WatchReceiver<T: Clone> {
    inner: Arc<WatchState<T>>,
    version: u64,
}

impl<T: Clone> WatchReceiver<T> {
    pub fn current(&self) -> T {
        self.inner
            .data
            .lock()
            .expect("watch receiver poisoned")
            .0
            .clone()
    }

    pub fn wait_for_update(mut self, timeout: Duration) -> Option<T> {
        let state = self.inner.data.lock().expect("watch receiver poisoned");
        let (state, _) = self
            .inner
            .condvar
            .wait_timeout_while(state, timeout, |(_, version)| *version == self.version)
            .expect("watch receiver poisoned");
        if state.1 == self.version {
            return None;
        }
        self.version = state.1;
        Some(state.0.clone())
    }
}

pub struct TypedPrefixWatcher<K, V>
where
    K: Clone + Eq + Hash,
    V: Clone,
{
    receiver: WatchReceiver<HashMap<K, V>>,
}

impl<K, V> TypedPrefixWatcher<K, V>
where
    K: Clone + Eq + Hash,
    V: Clone,
{
    pub fn receiver(&self) -> WatchReceiver<HashMap<K, V>> {
        self.receiver.clone()
    }

    pub fn current(&self) -> HashMap<K, V> {
        self.receiver.current()
    }
}

pub fn watch_prefix_with_extraction<K, V, C>(
    client: C,
    prefix: impl Into<String>,
    key_extractor: impl Fn(&KeyValue) -> Option<K> + Send + Sync + 'static,
    value_extractor: impl Fn(String) -> Option<V> + Send + Sync + 'static,
    cancellation_token: CancellationToken,
) -> TypedPrefixWatcher<K, V>
where
    K: Clone + Eq + Hash + Send + 'static,
    V: Clone + Send + 'static,
    C: EtcdClient,
{
    let prefix = prefix.into();
    let mut initial_snapshot = HashMap::new();
    for kv in client.list_prefix(&prefix) {
        if let (Some(key), Some(value)) = (key_extractor(&kv), value_extractor(kv.value.clone())) {
            initial_snapshot.insert(key, value);
        }
    }

    let state = Arc::new(WatchState {
        data: Mutex::new((initial_snapshot, 0)),
        condvar: Condvar::new(),
    });

    let receiver = WatchReceiver {
        inner: state.clone(),
        version: 0,
    };

    let key_extractor = Arc::new(key_extractor);
    let value_extractor = Arc::new(value_extractor);

    thread::spawn(move || {
        for batch in client.watch_prefix(&prefix) {
            if cancellation_token.is_cancelled() {
                break;
            }

            let mut snapshot = state.data.lock().expect("watch state poisoned");
            for kv in batch {
                if let (Some(key), Some(value)) =
                    (key_extractor(&kv), value_extractor(kv.value.clone()))
                {
                    snapshot.0.insert(key, value);
                }
            }
            snapshot.1 += 1;
            state.condvar.notify_all();
        }
    });

    TypedPrefixWatcher { receiver }
}

pub fn watch_prefix<K, V, C>(
    client: C,
    prefix: impl Into<String>,
    key_extractor: impl Fn(&KeyValue) -> Option<K> + Send + Sync + 'static,
    cancellation_token: CancellationToken,
) -> TypedPrefixWatcher<K, V>
where
    K: Clone + Eq + Hash + Send + 'static,
    V: Clone + From<String> + Send + 'static,
    C: EtcdClient,
{
    watch_prefix_with_extraction(
        client,
        prefix,
        key_extractor,
        |value| Some(V::from(value)),
        cancellation_token,
    )
}

pub mod key_extractors {
    use super::KeyValue;

    pub fn lease_id(kv: &KeyValue) -> Option<u64> {
        Some(kv.lease_id)
    }

    pub fn key_string(
        prefix: &str,
    ) -> impl Fn(&KeyValue) -> Option<String> + Send + Sync + 'static {
        let prefix = prefix.to_string();
        move |kv: &KeyValue| {
            kv.key
                .strip_prefix(&prefix)
                .map(|suffix| suffix.trim_start_matches('/').to_string())
        }
    }

    pub fn full_key_string(kv: &KeyValue) -> Option<String> {
        Some(kv.key.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct MockEtcdClient {
        initial: Vec<KeyValue>,
        updates: Vec<Vec<KeyValue>>,
    }

    impl EtcdClient for MockEtcdClient {
        fn list_prefix(&self, _prefix: &str) -> Vec<KeyValue> {
            self.initial.clone()
        }

        fn watch_prefix(&self, _prefix: &str) -> Vec<Vec<KeyValue>> {
            self.updates.clone()
        }
    }

    #[test]
    fn receives_snapshot_updates() {
        let watcher = watch_prefix::<String, String, _>(
            MockEtcdClient {
                initial: vec![KeyValue::new("/workers/a", "alpha", 1)],
                updates: vec![vec![KeyValue::new("/workers/b", "beta", 2)]],
            },
            "/workers",
            key_extractors::key_string("/workers"),
            CancellationToken::new(),
        );

        assert_eq!(watcher.current().get("a"), Some(&"alpha".to_string()));

        let updated = watcher
            .receiver()
            .wait_for_update(Duration::from_millis(100))
            .expect("watcher should receive update");
        assert_eq!(updated.get("b"), Some(&"beta".to_string()));
    }
}