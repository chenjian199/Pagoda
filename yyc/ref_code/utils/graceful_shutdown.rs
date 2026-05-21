use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};

#[derive(Debug)]
pub struct GracefulShutdownTracker {
    active_portnames: AtomicUsize,
    shutdown_complete: Condvar,
    lock: Mutex<()>,
}

impl GracefulShutdownTracker {
    pub fn new() -> Self {
        Self {
            active_portnames: AtomicUsize::new(0),
            shutdown_complete: Condvar::new(),
            lock: Mutex::new(()),
        }
    }

    pub fn register_portname(&self) {
        self.active_portnames.fetch_add(1, Ordering::SeqCst);
    }

    pub fn unregister_portname(&self) {
        let previous = self.active_portnames.fetch_sub(1, Ordering::SeqCst);
        if previous == 1 {
            self.shutdown_complete.notify_all();
        }
    }

    pub fn get_count(&self) -> usize {
        self.active_portnames.load(Ordering::SeqCst)
    }

    pub fn wait_for_completion(&self) {
        let mut guard = self.lock.lock().expect("shutdown tracker poisoned");
        while self.get_count() != 0 {
            guard = self
                .shutdown_complete
                .wait(guard)
                .expect("shutdown tracker poisoned");
        }
    }
}

impl Default for GracefulShutdownTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn waits_until_all_portnames_finish() {
        let tracker = Arc::new(GracefulShutdownTracker::new());
        tracker.register_portname();
        tracker.register_portname();

        let cloned = tracker.clone();
        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            cloned.unregister_portname();
            cloned.unregister_portname();
        });

        tracker.wait_for_completion();
        assert_eq!(tracker.get_count(), 0);
        handle.join().expect("waiter thread should stop");
    }
}