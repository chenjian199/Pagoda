use super::critical::CancellationToken;
use std::any::Any;
use std::collections::VecDeque;
use std::fmt::{self, Debug, Display};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

#[derive(Debug)]
pub enum TaskError {
    Cancelled,
    Failed(String),
    TrackerClosed,
}

impl TaskError {
    pub fn is_cancellation(&self) -> bool {
        matches!(self, Self::Cancelled)
    }

    pub fn is_failure(&self) -> bool {
        matches!(self, Self::Failed(_))
    }

    pub fn into_anyhow(self) -> String {
        match self {
            Self::Cancelled => "task cancelled".to_string(),
            Self::Failed(err) => err,
            Self::TrackerClosed => "tracker closed".to_string(),
        }
    }
}

pub struct TaskHandle<T> {
    join_handle: Option<JoinHandle<Result<T, TaskError>>>,
    cancel_token: CancellationToken,
}

impl<T> TaskHandle<T> {
    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.cancel_token
    }

    pub fn abort(&self) {
        self.cancel_token.cancel();
    }

    pub fn is_finished(&self) -> bool {
        self.join_handle.as_ref().map(JoinHandle::is_finished).unwrap_or(true)
    }

    pub fn join(mut self) -> Result<T, TaskError> {
        self.join_handle
            .take()
            .ok_or(TaskError::TrackerClosed)?
            .join()
            .unwrap_or_else(|_| Err(TaskError::Failed("task panicked".to_string())))
    }
}

pub trait Continuation: Send + Sync + Debug + Any {
    fn execute(
        &self,
        cancel_token: CancellationToken,
    ) -> TaskExecutionResult<Box<dyn Any + Send + 'static>>;
}

#[derive(Debug)]
pub struct FailedWithContinuation {
    pub source: String,
    pub continuation: Arc<dyn Continuation + Send + Sync + 'static>,
}

impl FailedWithContinuation {
    pub fn new(
        source: String,
        continuation: Arc<dyn Continuation + Send + Sync + 'static>,
    ) -> Self {
        Self { source, continuation }
    }

    pub fn into_anyhow(
        source: String,
        continuation: Arc<dyn Continuation + Send + Sync + 'static>,
    ) -> String {
        let _ = continuation;
        source
    }

    pub fn from_fn<F>(source: String, _f: F) -> String
    where
        F: Fn() + Send + Sync + 'static,
    {
        source
    }

    pub fn from_cancellable<F>(source: String, _f: F) -> String
    where
        F: Fn(CancellationToken) + Send + Sync + 'static,
    {
        source
    }
}

pub trait FailedWithContinuationExt {
    fn extract_continuation(&self) -> Option<Arc<dyn Continuation + Send + Sync + 'static>>;
    fn has_continuation(&self) -> bool;
}

impl FailedWithContinuationExt for FailedWithContinuation {
    fn extract_continuation(&self) -> Option<Arc<dyn Continuation + Send + Sync + 'static>> {
        Some(self.continuation.clone())
    }

    fn has_continuation(&self) -> bool {
        true
    }
}

pub enum SchedulingPolicy {
    Unlimited,
    Semaphore(usize),
}

pub struct OnErrorContext {
    pub attempt_count: u32,
    pub task_id: TaskId,
    pub execution_context: TaskExecutionContext,
    pub state: Option<Box<dyn Any + Send + 'static>>,
}

pub trait OnErrorPolicy: Send + Sync + Debug {
    fn create_child(&self) -> Arc<dyn OnErrorPolicy>;
    fn create_context(&self) -> Option<Box<dyn Any + Send + 'static>> {
        None
    }
    fn on_error(&self, error: &str, context: &mut OnErrorContext) -> ErrorResponse;
    fn allow_continuation(&self, _error: &str, _context: &OnErrorContext) -> bool {
        false
    }
    fn should_reschedule(&self, _error: &str, _context: &OnErrorContext) -> bool {
        false
    }
}

pub enum ErrorPolicy {
    LogOnly,
    CancelOnError,
    CancelOnPatterns(Vec<String>),
    CancelOnThreshold { max_failures: usize },
    CancelOnRate { max_failure_rate: f32, window_secs: u64 },
}

pub enum ErrorResponse {
    Fail,
    Shutdown,
    Custom(Box<dyn OnErrorAction>),
}

pub trait OnErrorAction: Send + Sync + Debug {
    fn execute(
        &self,
        error: &str,
        task_id: TaskId,
        attempt_count: u32,
        context: &TaskExecutionContext,
    ) -> ActionResult;
}

pub enum ActionResult {
    Fail,
    Continue { continuation: Arc<dyn Continuation + Send + Sync + 'static> },
    Shutdown,
}

#[derive(Clone)]
pub struct TaskExecutionContext {
    pub scheduler: Arc<dyn TaskScheduler>,
    pub metrics: Arc<dyn HierarchicalTaskMetrics>,
}

pub enum TaskExecutionResult<T> {
    Success(T),
    Cancelled,
    Error(String),
}

pub trait ArcPolicy: Sized + Send + Sync + 'static {
    fn new_arc(self) -> Arc<Self> {
        Arc::new(self)
    }
}

impl<T> ArcPolicy for T where T: Sized + Send + Sync + 'static {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TaskId(u64);

impl TaskId {
    fn new() -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        Self(NEXT_ID.fetch_add(1, Ordering::SeqCst))
    }
}

impl Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "task-{}", self.0)
    }
}

pub enum CompletionStatus {
    Ok,
    Cancelled,
    Failed(String),
}

pub enum CancellableTaskResult<T> {
    Ok(T),
    Cancelled,
    Err(String),
}

pub enum SchedulingResult<T> {
    Execute(T),
    Cancelled,
    Rejected(String),
}

pub trait ResourceGuard: Send + 'static {}

pub trait TaskScheduler: Send + Sync + Debug {
    fn acquire_execution_slot(
        &self,
        cancel_token: CancellationToken,
    ) -> SchedulingResult<Box<dyn ResourceGuard>>;
}

pub trait HierarchicalTaskMetrics: Send + Sync + Debug {
    fn increment_issued(&self);
    fn increment_started(&self);
    fn increment_success(&self);
    fn increment_cancelled(&self);
    fn increment_failed(&self);
    fn increment_rejected(&self);
    fn issued(&self) -> u64;
    fn started(&self) -> u64;
    fn success(&self) -> u64;
    fn cancelled(&self) -> u64;
    fn failed(&self) -> u64;
    fn rejected(&self) -> u64;
}

#[derive(Debug)]
pub struct TaskMetrics {
    issued: AtomicU64,
    started: AtomicU64,
    success: AtomicU64,
    cancelled: AtomicU64,
    failed: AtomicU64,
    rejected: AtomicU64,
}

impl TaskMetrics {
    pub fn new() -> Self {
        Self {
            issued: AtomicU64::new(0),
            started: AtomicU64::new(0),
            success: AtomicU64::new(0),
            cancelled: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
        }
    }
}

impl HierarchicalTaskMetrics for TaskMetrics {
    fn increment_issued(&self) { self.issued.fetch_add(1, Ordering::SeqCst); }
    fn increment_started(&self) { self.started.fetch_add(1, Ordering::SeqCst); }
    fn increment_success(&self) { self.success.fetch_add(1, Ordering::SeqCst); }
    fn increment_cancelled(&self) { self.cancelled.fetch_add(1, Ordering::SeqCst); }
    fn increment_failed(&self) { self.failed.fetch_add(1, Ordering::SeqCst); }
    fn increment_rejected(&self) { self.rejected.fetch_add(1, Ordering::SeqCst); }
    fn issued(&self) -> u64 { self.issued.load(Ordering::SeqCst) }
    fn started(&self) -> u64 { self.started.load(Ordering::SeqCst) }
    fn success(&self) -> u64 { self.success.load(Ordering::SeqCst) }
    fn cancelled(&self) -> u64 { self.cancelled.load(Ordering::SeqCst) }
    fn failed(&self) -> u64 { self.failed.load(Ordering::SeqCst) }
    fn rejected(&self) -> u64 { self.rejected.load(Ordering::SeqCst) }
}

#[derive(Debug)]
pub struct PrometheusTaskMetrics {
    inner: TaskMetrics,
}

impl PrometheusTaskMetrics {
    pub fn new() -> Self {
        Self { inner: TaskMetrics::new() }
    }
}

impl HierarchicalTaskMetrics for PrometheusTaskMetrics {
    fn increment_issued(&self) { self.inner.increment_issued(); }
    fn increment_started(&self) { self.inner.increment_started(); }
    fn increment_success(&self) { self.inner.increment_success(); }
    fn increment_cancelled(&self) { self.inner.increment_cancelled(); }
    fn increment_failed(&self) { self.inner.increment_failed(); }
    fn increment_rejected(&self) { self.inner.increment_rejected(); }
    fn issued(&self) -> u64 { self.inner.issued() }
    fn started(&self) -> u64 { self.inner.started() }
    fn success(&self) -> u64 { self.inner.success() }
    fn cancelled(&self) -> u64 { self.inner.cancelled() }
    fn failed(&self) -> u64 { self.inner.failed() }
    fn rejected(&self) -> u64 { self.inner.rejected() }
}

#[derive(Debug)]
pub struct UnlimitedGuard;
impl ResourceGuard for UnlimitedGuard {}

#[derive(Debug)]
pub struct UnlimitedScheduler;

impl UnlimitedScheduler {
    pub fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

impl TaskScheduler for UnlimitedScheduler {
    fn acquire_execution_slot(
        &self,
        cancel_token: CancellationToken,
    ) -> SchedulingResult<Box<dyn ResourceGuard>> {
        if cancel_token.is_cancelled() {
            return SchedulingResult::Cancelled;
        }
        SchedulingResult::Execute(Box::new(UnlimitedGuard))
    }
}

#[derive(Debug)]
struct SemaphoreState {
    permits: Mutex<usize>,
    condvar: Condvar,
}

#[derive(Debug)]
pub struct SemaphoreGuard {
    state: Arc<SemaphoreState>,
}

impl Drop for SemaphoreGuard {
    fn drop(&mut self) {
        let mut permits = self.state.permits.lock().expect("semaphore poisoned");
        *permits += 1;
        self.state.condvar.notify_one();
    }
}

impl ResourceGuard for SemaphoreGuard {}

#[derive(Debug)]
pub struct SemaphoreScheduler {
    state: Arc<SemaphoreState>,
}

impl SemaphoreScheduler {
    pub fn new(permits: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Arc::new(SemaphoreState {
                permits: Mutex::new(permits),
                condvar: Condvar::new(),
            }),
        })
    }

    pub fn with_permits(permits: usize) -> Arc<Self> {
        Self::new(permits)
    }

    pub fn available_permits(&self) -> usize {
        *self.state.permits.lock().expect("semaphore poisoned")
    }
}

impl TaskScheduler for SemaphoreScheduler {
    fn acquire_execution_slot(
        &self,
        cancel_token: CancellationToken,
    ) -> SchedulingResult<Box<dyn ResourceGuard>> {
        let mut permits = self.state.permits.lock().expect("semaphore poisoned");
        loop {
            if cancel_token.is_cancelled() {
                return SchedulingResult::Cancelled;
            }
            if *permits > 0 {
                *permits -= 1;
                return SchedulingResult::Execute(Box::new(SemaphoreGuard {
                    state: self.state.clone(),
                }));
            }
            permits = self.state.condvar.wait(permits).expect("semaphore poisoned");
        }
    }
}

#[derive(Debug)]
pub struct LogOnlyPolicy;

impl LogOnlyPolicy {
    pub fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

impl OnErrorPolicy for LogOnlyPolicy {
    fn create_child(&self) -> Arc<dyn OnErrorPolicy> {
        Self::new()
    }

    fn on_error(&self, _error: &str, _context: &mut OnErrorContext) -> ErrorResponse {
        ErrorResponse::Fail
    }
}

#[derive(Debug)]
pub struct CancelOnError {
    cancel_token: CancellationToken,
    patterns: Vec<String>,
}

impl CancelOnError {
    pub fn new() -> (Arc<Self>, CancellationToken) {
        let token = CancellationToken::new();
        (
            Arc::new(Self {
                cancel_token: token.clone(),
                patterns: Vec::new(),
            }),
            token,
        )
    }

    pub fn with_patterns(error_patterns: Vec<String>) -> (Arc<Self>, CancellationToken) {
        let token = CancellationToken::new();
        (
            Arc::new(Self {
                cancel_token: token.clone(),
                patterns: error_patterns,
            }),
            token,
        )
    }
}

impl OnErrorPolicy for CancelOnError {
    fn create_child(&self) -> Arc<dyn OnErrorPolicy> {
        Arc::new(Self {
            cancel_token: self.cancel_token.clone(),
            patterns: self.patterns.clone(),
        })
    }

    fn on_error(&self, error: &str, _context: &mut OnErrorContext) -> ErrorResponse {
        if self.patterns.is_empty() || self.patterns.iter().any(|pattern| error.contains(pattern)) {
            self.cancel_token.cancel();
            return ErrorResponse::Shutdown;
        }
        ErrorResponse::Fail
    }
}

#[derive(Debug)]
pub struct ThresholdCancelPolicy {
    max_failures: usize,
    failure_count: AtomicU64,
}

impl ThresholdCancelPolicy {
    pub fn with_threshold(max_failures: usize) -> Arc<Self> {
        Arc::new(Self {
            max_failures,
            failure_count: AtomicU64::new(0),
        })
    }

    pub fn failure_count(&self) -> u64 {
        self.failure_count.load(Ordering::SeqCst)
    }

    pub fn reset_failure_count(&self) {
        self.failure_count.store(0, Ordering::SeqCst);
    }
}

impl OnErrorPolicy for ThresholdCancelPolicy {
    fn create_child(&self) -> Arc<dyn OnErrorPolicy> {
        Self::with_threshold(self.max_failures)
    }

    fn on_error(&self, _error: &str, _context: &mut OnErrorContext) -> ErrorResponse {
        let failures = self.failure_count.fetch_add(1, Ordering::SeqCst) + 1;
        if failures as usize >= self.max_failures {
            ErrorResponse::Shutdown
        } else {
            ErrorResponse::Fail
        }
    }
}

#[derive(Debug)]
pub struct RateCancelPolicy {
    max_failure_rate: f32,
    window_secs: u64,
    history: Mutex<VecDeque<Instant>>,
    cancel_token: CancellationToken,
}

impl RateCancelPolicy {
    pub fn builder() -> RateCancelPolicyBuilder {
        RateCancelPolicyBuilder {
            max_failure_rate: 1.0,
            window_secs: 60,
        }
    }
}

impl OnErrorPolicy for RateCancelPolicy {
    fn create_child(&self) -> Arc<dyn OnErrorPolicy> {
        Arc::new(Self {
            max_failure_rate: self.max_failure_rate,
            window_secs: self.window_secs,
            history: Mutex::new(VecDeque::new()),
            cancel_token: self.cancel_token.clone(),
        })
    }

    fn on_error(&self, _error: &str, _context: &mut OnErrorContext) -> ErrorResponse {
        let mut history = self.history.lock().expect("rate policy poisoned");
        let now = Instant::now();
        history.push_back(now);
        while history
            .front()
            .map(|time| now.duration_since(*time).as_secs() > self.window_secs)
            .unwrap_or(false)
        {
            history.pop_front();
        }
        let rate = history.len() as f32 / self.window_secs.max(1) as f32;
        if rate >= self.max_failure_rate {
            self.cancel_token.cancel();
            ErrorResponse::Shutdown
        } else {
            ErrorResponse::Fail
        }
    }
}

pub struct RateCancelPolicyBuilder {
    max_failure_rate: f32,
    window_secs: u64,
}

impl RateCancelPolicyBuilder {
    pub fn rate(mut self, max_failure_rate: f32) -> Self {
        self.max_failure_rate = max_failure_rate;
        self
    }

    pub fn window_secs(mut self, window_secs: u64) -> Self {
        self.window_secs = window_secs;
        self
    }

    pub fn build(self) -> (Arc<RateCancelPolicy>, CancellationToken) {
        let token = CancellationToken::new();
        (
            Arc::new(RateCancelPolicy {
                max_failure_rate: self.max_failure_rate,
                window_secs: self.window_secs,
                history: Mutex::new(VecDeque::new()),
                cancel_token: token.clone(),
            }),
            token,
        )
    }
}

#[derive(Debug)]
pub struct TriggerCancellationTokenAction {
    cancel_token: CancellationToken,
}

impl TriggerCancellationTokenAction {
    pub fn new(cancel_token: CancellationToken) -> Self {
        Self { cancel_token }
    }
}

impl OnErrorAction for TriggerCancellationTokenAction {
    fn execute(
        &self,
        _error: &str,
        _task_id: TaskId,
        _attempt_count: u32,
        _context: &TaskExecutionContext,
    ) -> ActionResult {
        self.cancel_token.cancel();
        ActionResult::Shutdown
    }
}

#[derive(Debug)]
pub struct TriggerCancellationTokenOnError {
    cancel_token: CancellationToken,
}

impl TriggerCancellationTokenOnError {
    pub fn new(cancel_token: CancellationToken) -> Arc<Self> {
        Arc::new(Self { cancel_token })
    }
}

impl OnErrorPolicy for TriggerCancellationTokenOnError {
    fn create_child(&self) -> Arc<dyn OnErrorPolicy> {
        Self::new(self.cancel_token.clone())
    }

    fn on_error(&self, _error: &str, _context: &mut OnErrorContext) -> ErrorResponse {
        self.cancel_token.cancel();
        ErrorResponse::Shutdown
    }
}

struct TaskTrackerInner {
    scheduler: Arc<dyn TaskScheduler>,
    error_policy: Arc<dyn OnErrorPolicy>,
    metrics: Arc<dyn HierarchicalTaskMetrics>,
    cancel_token: CancellationToken,
    closed: AtomicBool,
    children: RwLock<Vec<Weak<TaskTrackerInner>>>,
}

pub struct ChildTrackerBuilder<'parent> {
    parent: &'parent TaskTracker,
    scheduler: Option<Arc<dyn TaskScheduler>>,
    error_policy: Option<Arc<dyn OnErrorPolicy>>,
}

impl<'parent> ChildTrackerBuilder<'parent> {
    pub fn new(parent: &'parent TaskTracker) -> Self {
        Self {
            parent,
            scheduler: None,
            error_policy: None,
        }
    }

    pub fn scheduler(mut self, scheduler: Arc<dyn TaskScheduler>) -> Self {
        self.scheduler = Some(scheduler);
        self
    }

    pub fn error_policy(mut self, error_policy: Arc<dyn OnErrorPolicy>) -> Self {
        self.error_policy = Some(error_policy);
        self
    }

    pub fn build(self) -> Result<TaskTracker, String> {
        let scheduler = self
            .scheduler
            .clone()
            .unwrap_or_else(|| self.parent.0.scheduler.clone());
        let error_policy = self
            .error_policy
            .clone()
            .unwrap_or_else(|| self.parent.0.error_policy.create_child());
        let tracker = TaskTracker::new_with_parts(
            scheduler,
            error_policy,
            self.parent.0.metrics.clone(),
            self.parent.0.cancel_token.child_token(),
        );
        self.parent
            .0
            .children
            .write()
            .expect("children poisoned")
            .push(Arc::downgrade(&tracker.0));
        Ok(tracker)
    }
}

pub struct TaskTracker(Arc<TaskTrackerInner>);

pub struct TaskTrackerBuilder {
    scheduler: Option<Arc<dyn TaskScheduler>>,
    error_policy: Option<Arc<dyn OnErrorPolicy>>,
    metrics: Option<Arc<dyn HierarchicalTaskMetrics>>,
    cancel_token: Option<CancellationToken>,
}

impl TaskTrackerBuilder {
    pub fn scheduler(mut self, scheduler: Arc<dyn TaskScheduler>) -> Self {
        self.scheduler = Some(scheduler);
        self
    }

    pub fn error_policy(mut self, error_policy: Arc<dyn OnErrorPolicy>) -> Self {
        self.error_policy = Some(error_policy);
        self
    }

    pub fn metrics(mut self, metrics: Arc<dyn HierarchicalTaskMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn cancel_token(mut self, cancel_token: CancellationToken) -> Self {
        self.cancel_token = Some(cancel_token);
        self
    }

    pub fn build(self) -> Result<TaskTracker, String> {
        TaskTracker::new_with_parts(
            self.scheduler.ok_or_else(|| "missing scheduler".to_string())?,
            self.error_policy.ok_or_else(|| "missing error policy".to_string())?,
            self.metrics.unwrap_or_else(|| Arc::new(TaskMetrics::new())),
            self.cancel_token.unwrap_or_else(CancellationToken::new),
        )
        .pipe(Ok)
    }
}

trait Pipe: Sized {
    fn pipe<T>(self, f: impl FnOnce(Self) -> T) -> T {
        f(self)
    }
}

impl<T> Pipe for T {}

impl TaskTracker {
    pub fn builder() -> TaskTrackerBuilder {
        TaskTrackerBuilder {
            scheduler: None,
            error_policy: None,
            metrics: None,
            cancel_token: None,
        }
    }

    pub fn new(
        scheduler: Arc<dyn TaskScheduler>,
        error_policy: Arc<dyn OnErrorPolicy>,
    ) -> Result<Self, String> {
        Ok(Self::new_with_parts(
            scheduler,
            error_policy,
            Arc::new(TaskMetrics::new()),
            CancellationToken::new(),
        ))
    }

    pub fn new_with_prometheus(
        scheduler: Arc<dyn TaskScheduler>,
        error_policy: Arc<dyn OnErrorPolicy>,
    ) -> Result<Self, String> {
        Ok(Self::new_with_parts(
            scheduler,
            error_policy,
            Arc::new(PrometheusTaskMetrics::new()),
            CancellationToken::new(),
        ))
    }

    fn new_with_parts(
        scheduler: Arc<dyn TaskScheduler>,
        error_policy: Arc<dyn OnErrorPolicy>,
        metrics: Arc<dyn HierarchicalTaskMetrics>,
        cancel_token: CancellationToken,
    ) -> Self {
        Self(Arc::new(TaskTrackerInner {
            scheduler,
            error_policy,
            metrics,
            cancel_token,
            closed: AtomicBool::new(false),
            children: RwLock::new(Vec::new()),
        }))
    }

    pub fn child_tracker(&self) -> Result<TaskTracker, String> {
        self.child_tracker_builder().build()
    }

    pub fn spawn<F, T>(&self, future: F) -> TaskHandle<T>
    where
        F: FnOnce() -> Result<T, String> + Send + 'static,
        T: Send + 'static,
    {
        let cancel_token = self.0.cancel_token.child_token();
        let task_token = cancel_token.clone();
        let scheduler = self.0.scheduler.clone();
        let metrics = self.0.metrics.clone();
        let error_policy = self.0.error_policy.clone();
        let closed = self.0.closed.load(Ordering::SeqCst);
        metrics.increment_issued();

        let join_handle = thread::spawn(move || {
            if closed {
                metrics.increment_rejected();
                return Err(TaskError::TrackerClosed);
            }

            let guard = match scheduler.acquire_execution_slot(task_token.clone()) {
                SchedulingResult::Execute(guard) => guard,
                SchedulingResult::Cancelled => {
                    metrics.increment_cancelled();
                    return Err(TaskError::Cancelled);
                }
                SchedulingResult::Rejected(reason) => {
                    metrics.increment_rejected();
                    return Err(TaskError::Failed(reason));
                }
            };

            let _guard = guard;
            metrics.increment_started();

            if task_token.is_cancelled() {
                metrics.increment_cancelled();
                return Err(TaskError::Cancelled);
            }

            match future() {
                Ok(value) => {
                    metrics.increment_success();
                    Ok(value)
                }
                Err(error) => {
                    metrics.increment_failed();
                    let mut context = OnErrorContext {
                        attempt_count: 1,
                        task_id: TaskId::new(),
                        execution_context: TaskExecutionContext {
                            scheduler: scheduler.clone(),
                            metrics: metrics.clone(),
                        },
                        state: error_policy.create_context(),
                    };
                    let response = error_policy.on_error(&error, &mut context);
                    if matches!(response, ErrorResponse::Shutdown) {
                        task_token.cancel();
                    }
                    Err(TaskError::Failed(error))
                }
            }
        });

        TaskHandle {
            join_handle: Some(join_handle),
            cancel_token,
        }
    }

    pub fn spawn_cancellable<F, T>(&self, task_fn: F) -> TaskHandle<T>
    where
        F: FnOnce(CancellationToken) -> Result<T, String> + Send + 'static,
        T: Send + 'static,
    {
        let token = self.0.cancel_token.child_token();
        self.spawn(move || task_fn(token))
    }

    pub fn metrics(&self) -> &dyn HierarchicalTaskMetrics {
        self.0.metrics.as_ref()
    }

    pub fn cancel(&self) {
        self.0.cancel_token.cancel();
    }

    pub fn is_closed(&self) -> bool {
        self.0.closed.load(Ordering::SeqCst)
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.0.cancel_token.clone()
    }

    pub fn child_count(&self) -> usize {
        self.0
            .children
            .read()
            .expect("children poisoned")
            .iter()
            .filter(|child| child.upgrade().is_some())
            .count()
    }

    pub fn child_tracker_builder(&self) -> ChildTrackerBuilder<'_> {
        ChildTrackerBuilder::new(self)
    }

    pub fn join(&self) {
        self.0.closed.store(true, Ordering::SeqCst);
        loop {
            if self.child_count() == 0 {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}