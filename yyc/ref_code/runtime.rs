use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread;
use std::time::Duration;

#[derive(Debug)]
struct CancellationState {
    cancelled: AtomicBool,
    condvar: Condvar,
    lock: Mutex<()>,
    children: Mutex<Vec<Weak<CancellationState>>>,
}

#[derive(Clone, Debug)]
pub struct CancellationToken {
    inner: Arc<CancellationState>,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(CancellationState {
                cancelled: AtomicBool::new(false),
                condvar: Condvar::new(),
                lock: Mutex::new(()),
                children: Mutex::new(Vec::new()),
            }),
        }
    }

    pub fn child_token(&self) -> Self {
        let child = Self::new();
        if self.is_cancelled() {
            child.cancel();
        } else {
            self.inner
                .children
                .lock()
                .expect("cancel token children poisoned")
                .push(Arc::downgrade(&child.inner));
        }
        child
    }

    pub fn cancel(&self) {
        cancel_state(&self.inner);
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::SeqCst)
    }

    pub fn wait_cancelled(&self) {
        let mut guard = self.inner.lock.lock().expect("cancel token poisoned");
        while !self.is_cancelled() {
            guard = self
                .inner
                .condvar
                .wait(guard)
                .expect("cancel token poisoned");
        }
    }
}

fn cancel_state(state: &Arc<CancellationState>) {
    if state.cancelled.swap(true, Ordering::SeqCst) {
        return;
    }
    state.condvar.notify_all();
    let children = state
        .children
        .lock()
        .expect("cancel token children poisoned")
        .clone();
    for child in children {
        if let Some(child_state) = child.upgrade() {
            cancel_state(&child_state);
        }
    }
}

#[derive(Debug)]
pub struct GracefulShutdownTracker {
    active_portnames: AtomicUsize,
    condvar: Condvar,
    lock: Mutex<()>,
}

impl GracefulShutdownTracker {
    pub fn new() -> Self {
        Self {
            active_portnames: AtomicUsize::new(0),
            condvar: Condvar::new(),
            lock: Mutex::new(()),
        }
    }

    pub fn register_portname(&self) {
        self.active_portnames.fetch_add(1, Ordering::SeqCst);
    }

    pub fn unregister_portname(&self) {
        let previous = self.active_portnames.fetch_sub(1, Ordering::SeqCst);
        if previous == 1 {
            self.condvar.notify_all();
        }
    }

    pub fn get_count(&self) -> usize {
        self.active_portnames.load(Ordering::SeqCst)
    }

    pub fn wait_for_completion(&self) {
        let mut guard = self.lock.lock().expect("shutdown tracker poisoned");
        while self.get_count() > 0 {
            guard = self.condvar.wait(guard).expect("shutdown tracker poisoned");
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeHandle {
    name: &'static str,
}

impl RuntimeHandle {
    fn new(name: &'static str) -> Self {
        Self { name }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }
}

#[derive(Clone, Debug)]
pub struct Runtime {
    id: Arc<String>,
    primary: RuntimeHandle,
    secondary: RuntimeHandle,
    cancellation_token: CancellationToken,
    portname_shutdown_token: CancellationToken,
    graceful_shutdown_tracker: Arc<GracefulShutdownTracker>,
    compute_threads: Option<usize>,
    block_in_place_permits: Option<usize>,
}

#[derive(Clone, Debug, Default)]
pub struct RuntimeConfig {
    pub num_worker_threads: Option<usize>,
    pub compute_threads: Option<usize>,
}

impl Runtime {
    fn new(primary: RuntimeHandle, secondary: Option<RuntimeHandle>) -> Self {
        let cancellation_token = CancellationToken::new();
        let portname_shutdown_token = cancellation_token.child_token();
        Self {
            id: Arc::new(format!("runtime-{:?}", thread::current().id())),
            primary,
            secondary: secondary.unwrap_or_else(|| RuntimeHandle::new("secondary")),
            cancellation_token,
            portname_shutdown_token,
            graceful_shutdown_tracker: Arc::new(GracefulShutdownTracker::new()),
            compute_threads: None,
            block_in_place_permits: None,
        }
    }

    fn new_with_config(
        primary: RuntimeHandle,
        secondary: Option<RuntimeHandle>,
        config: &RuntimeConfig,
    ) -> Self {
        let mut runtime = Self::new(primary, secondary);
        runtime.compute_threads = config.compute_threads.filter(|threads| *threads > 0);
        let workers = config.num_worker_threads.unwrap_or(1);
        runtime.block_in_place_permits = Some(workers.saturating_sub(1).max(1));
        runtime
    }

    pub fn from_current() -> Self {
        Self::from_handle(RuntimeHandle::new("external"))
    }

    pub fn from_handle(handle: RuntimeHandle) -> Self {
        Self::new(handle.clone(), Some(handle))
    }

    pub fn from_settings(config: RuntimeConfig) -> Self {
        let primary = RuntimeHandle::new("primary");
        Self::new_with_config(primary.clone(), Some(primary), &config)
    }

    pub fn single_threaded() -> Self {
        Self::new(RuntimeHandle::new("single-primary"), None)
    }

    pub fn id(&self) -> &str {
        self.id.as_str()
    }

    pub fn primary(&self) -> RuntimeHandle {
        self.primary.clone()
    }

    pub fn secondary(&self) -> RuntimeHandle {
        self.secondary.clone()
    }

    pub fn primary_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    pub fn child_token(&self) -> CancellationToken {
        self.portname_shutdown_token.child_token()
    }

    pub fn graceful_shutdown_tracker(&self) -> Arc<GracefulShutdownTracker> {
        self.graceful_shutdown_tracker.clone()
    }

    pub fn compute_pool(&self) -> Option<usize> {
        self.compute_threads
    }

    pub fn block_in_place_permits(&self) -> Option<usize> {
        self.block_in_place_permits
    }

    pub fn initialize_thread_local(&self) -> bool {
        self.compute_threads.is_some() && self.block_in_place_permits.is_some()
    }

    pub fn initialize_all_thread_locals(&self) -> usize {
        self.block_in_place_permits.unwrap_or(1)
    }

    pub fn shutdown(&self) {
        self.portname_shutdown_token.cancel();
        self.graceful_shutdown_tracker.wait_for_completion();
        self.cancellation_token.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_from_settings_uses_configured_resources() {
        let runtime = Runtime::from_settings(RuntimeConfig {
            num_worker_threads: Some(4),
            compute_threads: Some(2),
        });
        assert_eq!(runtime.compute_pool(), Some(2));
        assert_eq!(runtime.block_in_place_permits(), Some(3));
        assert!(runtime.initialize_thread_local());
    }

    #[test]
    fn shutdown_is_two_phase() {
        let runtime = Runtime::single_threaded();
        let tracker = runtime.graceful_shutdown_tracker();
        tracker.register_portname();
        let cloned = tracker.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(25));
            cloned.unregister_portname();
        });
        runtime.shutdown();
        assert!(runtime.primary_token().is_cancelled());
    }

    #[test]
    fn child_token_observes_parent_cancellation() {
        let runtime = Runtime::from_current();
        let child = runtime.child_token();
        runtime.primary_token().cancel();
        assert!(child.is_cancelled());
    }

    #[test]
    fn child_cancellation_does_not_cancel_parent() {
        let runtime = Runtime::from_current();
        let child = runtime.child_token();
        child.cancel();
        assert!(!runtime.primary_token().is_cancelled());
    }
}