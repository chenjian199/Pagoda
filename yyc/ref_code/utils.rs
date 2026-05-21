#[path = "utils/graceful_shutdown.rs"]
pub mod graceful_shutdown;
#[path = "utils/ip_resolver.rs"]
pub mod ip_resolver;
#[path = "utils/pool.rs"]
pub mod pool;
#[path = "utils/stream.rs"]
pub mod stream;
#[path = "utils/task.rs"]
pub mod task;
#[path = "utils/tasks.rs"]
pub mod tasks;
#[path = "utils/typed_prefix_watcher.rs"]
pub mod typed_prefix_watcher;

pub use graceful_shutdown::GracefulShutdownTracker;
pub use ip_resolver::{
    format_ip_for_url, get_http_rpc_host, get_http_rpc_host_from_env,
    get_http_rpc_host_with_resolver, get_local_ip_for_advertise,
    get_local_ip_for_advertise_with_resolver, get_tcp_rpc_host_from_env, IpResolver,
    LocalIpResolutionError, SystemIpResolver,
};
pub use pool::{
    Pool, PoolExt, PoolItem, PoolValue, ReturnHandle, Returnable, SharedPoolItem, SyncPool,
    SyncPoolItem,
};
pub use stream::{until_deadline, DeadlineStream};
pub use typed_prefix_watcher::{
    key_extractors, watch_prefix, watch_prefix_with_extraction, EtcdClient, KeyValue,
    TypedPrefixWatcher, WatchReceiver,
};

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};

    #[derive(Clone)]
    struct MockResolver {
        ipv4: Result<IpAddr, LocalIpResolutionError>,
        ipv6: Result<IpAddr, LocalIpResolutionError>,
    }

    impl IpResolver for MockResolver {
        fn local_ip(&self) -> Result<IpAddr, LocalIpResolutionError> {
            self.ipv4.clone()
        }

        fn local_ipv6(&self) -> Result<IpAddr, LocalIpResolutionError> {
            self.ipv6.clone()
        }
    }

    #[derive(Default)]
    struct ResettableItem {
        value: usize,
    }

    impl Returnable for ResettableItem {
        fn on_return(&mut self) {
            self.value = 0;
        }
    }

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
    fn ip_resolver_falls_back_to_ipv6_and_formats_addresses() {
        let resolver = MockResolver {
            ipv4: Err(LocalIpResolutionError::AddressNotFound),
            ipv6: Ok(IpAddr::V6(Ipv6Addr::LOCALHOST)),
        };
        assert_eq!(get_local_ip_for_advertise_with_resolver(resolver), "[::1]");
        assert_eq!(format_ip_for_url(IpAddr::V4(Ipv4Addr::LOCALHOST)), "127.0.0.1");
    }

    #[test]
    fn pool_and_stream_work_through_root_exports() {
        let pool = SyncPool::new_direct(vec![ResettableItem { value: 9 }]);
        let item = pool.try_acquire().expect("pool item should exist");
        assert_eq!(item.get().value, 9);
        drop(item);

        let mut stream = until_deadline(0..10, Instant::now() + Duration::from_millis(20));
        let mut count = 0;
        while stream.next_item().is_some() {
            count += 1;
            thread::sleep(Duration::from_millis(5));
        }
        assert!(count < 10);
    }

    #[test]
    fn typed_prefix_watcher_receives_updates() {
        let watcher = watch_prefix::<String, String, _>(
            MockEtcdClient {
                initial: vec![KeyValue::new("/workers/a", "alpha", 1)],
                updates: vec![vec![KeyValue::new("/workers/b", "beta", 2)]],
            },
            "/workers",
            key_extractors::key_string("/workers"),
            tasks::critical::CancellationToken::new(),
        );

        let snapshot = watcher.current();
        assert_eq!(snapshot.get("a"), Some(&"alpha".to_string()));

        let rx = watcher.receiver();
        let updated = rx.wait_for_update(Duration::from_millis(100));
        assert!(updated.is_some());
        let values = updated.expect("updated snapshot should exist");
        assert_eq!(values.get("b"), Some(&"beta".to_string()));
    }

    #[test]
    fn tracker_can_spawn_and_join_task() {
        let tracker = tasks::tracker::TaskTracker::new(
            tasks::tracker::UnlimitedScheduler::new(),
            tasks::tracker::LogOnlyPolicy::new(),
        )
        .expect("tracker should build");
        let handle = tracker.spawn(|| Ok::<usize, String>(7));
        assert_eq!(handle.join().expect("task should succeed"), 7);
    }

    #[test]
    fn critical_task_supports_cancel_and_join() {
        let handle = tasks::critical::CriticalTaskExecutionHandle::new(
            |token| {
                let finished = Arc::new(Mutex::new(false));
                let watcher = finished.clone();
                while !token.is_cancelled() {
                    thread::sleep(Duration::from_millis(5));
                }
                *watcher.lock().expect("watcher poisoned") = true;
                Ok(())
            },
            tasks::critical::CancellationToken::new(),
            "test-critical",
        )
        .expect("critical task should start");
        handle.cancel();
        handle.join().expect("critical task should stop cleanly");
    }

    #[test]
    fn graceful_shutdown_tracker_is_reexported() {
        let tracker = Arc::new(GracefulShutdownTracker::new());
        tracker.register_portname();
        let cloned = tracker.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            cloned.unregister_portname();
        });
        tracker.wait_for_completion();
        assert_eq!(tracker.get_count(), 0);
    }
}