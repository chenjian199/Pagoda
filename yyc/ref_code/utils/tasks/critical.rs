use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

#[derive(Clone, Debug)]
pub struct CancellationToken {
    state: Arc<CancellationState>,
}

#[derive(Debug)]
struct CancellationState {
    cancelled: AtomicBool,
    condvar: Condvar,
    lock: Mutex<()>,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self {
            state: Arc::new(CancellationState {
                cancelled: AtomicBool::new(false),
                condvar: Condvar::new(),
                lock: Mutex::new(()),
            }),
        }
    }

    pub fn child_token(&self) -> Self {
        self.clone()
    }

    pub fn cancel(&self) {
        self.state.cancelled.store(true, Ordering::SeqCst);
        self.state.condvar.notify_all();
    }

    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::SeqCst)
    }

    pub fn wait_cancelled(&self) {
        let mut guard = self.state.lock.lock().expect("cancellation token poisoned");
        while !self.is_cancelled() {
            guard = self.state.condvar.wait(guard).expect("cancellation token poisoned");
        }
    }
}

#[derive(Clone, Debug)]
pub struct Handle;

pub struct CriticalTaskExecutionHandle {
    monitor_task: Option<JoinHandle<()>>,
    graceful_shutdown_token: CancellationToken,
    result_receiver: Option<mpsc::Receiver<Result<(), String>>>,
    finished: Arc<AtomicBool>,
    detached: bool,
}

impl CriticalTaskExecutionHandle {
    pub fn new<F>(
        task_fn: F,
        parent_token: CancellationToken,
        description: &str,
    ) -> Result<Self, String>
    where
        F: FnOnce(CancellationToken) -> Result<(), String> + Send + 'static,
    {
        Self::new_with_runtime(task_fn, parent_token, description, &Handle)
    }

    pub fn new_with_runtime<F>(
        task_fn: F,
        parent_token: CancellationToken,
        _description: &str,
        _runtime: &Handle,
    ) -> Result<Self, String>
    where
        F: FnOnce(CancellationToken) -> Result<(), String> + Send + 'static,
    {
        let token = parent_token.child_token();
        let (tx, rx) = mpsc::channel();
        let finished = Arc::new(AtomicBool::new(false));
        let finished_flag = finished.clone();
        let child = token.clone();
        let handle = thread::spawn(move || {
            let result = task_fn(child);
            let _ = tx.send(result);
            finished_flag.store(true, Ordering::SeqCst);
        });
        Ok(Self {
            monitor_task: Some(handle),
            graceful_shutdown_token: token,
            result_receiver: Some(rx),
            finished,
            detached: false,
        })
    }

    pub fn is_finished(&self) -> bool {
        self.finished.load(Ordering::SeqCst)
    }

    pub fn is_cancelled(&self) -> bool {
        self.graceful_shutdown_token.is_cancelled()
    }

    pub fn cancel(&self) {
        self.graceful_shutdown_token.cancel();
    }

    pub fn join(mut self) -> Result<(), String> {
        let result = self
            .result_receiver
            .take()
            .ok_or_else(|| "result receiver already consumed".to_string())?
            .recv()
            .map_err(|err| err.to_string())?;
        if let Some(handle) = self.monitor_task.take() {
            let _ = handle.join();
        }
        self.detached = true;
        result
    }

    pub fn detach(mut self) {
        self.detached = true;
        let _ = self.monitor_task.take();
        let _ = self.result_receiver.take();
    }
}

impl Drop for CriticalTaskExecutionHandle {
    fn drop(&mut self) {
        assert!(
            self.detached || self.monitor_task.is_none(),
            "critical task handle dropped without join() or detach()"
        );
    }
}