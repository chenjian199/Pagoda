use std::any::Any;
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use tokio::sync::watch::{self, Receiver, Sender};

use super::registry::Registry;
use crate::engine::{Data, AsyncEngineContext, AsyncEngineContextProvider};
use crate::engine::AsyncEngineController;

/// Generate a lightweight unique request ID using a process-local sequence
/// number and a nanosecond timestamp.
fn generate_id() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:032x}-{:016x}", ns, seq)
}

// ── IntoContext<U> ────────────────────────────────────────────────────────────

/// Converts `Context<T>` into `Context<U>` when `T: Into<U>`, preserving all
/// request identity and metadata.
pub trait IntoContext<U: Data> {
    fn into_context(self) -> Context<U>;
}

// ── State ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum State {
    Live,
    Stopped,
    Killed,
}

// ── Controller ────────────────────────────────────────────────────────────────

/// The canonical lifecycle state-machine for a single request.
///
/// Holds a [`tokio::sync::watch`] channel so that any number of async tasks can
/// observe the `Live → Stopped → Killed` transition without competing for a
/// single message.  Parent–child links allow cancellation to cascade through the
/// entire sub-graph of a request.
// #[derive(Debug)]
pub struct Controller {
    id: String,
    tx: Sender<State>,
    rx: Receiver<State>,
    child_context: Mutex<Vec<Arc<dyn AsyncEngineContext>>>,
}

impl Controller {
    pub fn new(id: impl Into<String>) -> Self {
        let (tx, rx) = watch::channel(State::Live);
        Self {
            id: id.into(),
            tx,
            rx,
            child_context: Mutex::new(Vec::new()),
        }
    }
    
    pub fn id(&self) -> &str {
        &self.id
    }

}

impl Default for Controller {
    fn default() -> Self {
        Self::new(generate_id())
    }
}

impl AsyncEngineController for Controller {}

#[async_trait]
impl AsyncEngineContext for Controller {
    fn id(&self) -> &str {
        &self.id
    }

    fn stop(&self) {
        // Snapshot children while holding the lock, then release before
        // recursing to avoid potential deadlocks.
        let children: Vec<Arc<dyn AsyncEngineContext>> =
            self.child_context.lock().unwrap().clone();
        for child in children {
            child.stop();
        }
        let _ = self.tx.send(State::Stopped);
    }

    fn kill(&self) {
        let children: Vec<Arc<dyn AsyncEngineContext>> =
            self.child_context.lock().unwrap().clone();
        for child in children {
            child.kill();
        }
        let _ = self.tx.send(State::Killed);
    }

    fn stop_generating(&self) {
        let children: Vec<Arc<dyn AsyncEngineContext>> =
            self.child_context.lock().unwrap().clone();
        for child in children {
            child.stop_generating();
        }
        let _ = self.tx.send(State::Stopped);
    }

    fn is_stopped(&self) -> bool {
        *self.rx.borrow() != State::Live
    }

    fn is_killed(&self) -> bool {
        *self.rx.borrow() == State::Killed
    }

    fn link_child(&self, child: Arc<dyn AsyncEngineContext>) {
        self.child_context.lock().unwrap().push(child);
    }

    // Async wait: resolves as soon as the state leaves `Live`.
    async fn stopped(&self) {
        let mut rx = self.rx.clone();
        loop {
            if *rx.borrow_and_update() != State::Live || rx.changed().await.is_err() {
                return;
            }
        }
    }

    /// Async wait: resolves as soon as the state reaches `Killed`.
    async fn killed(&self) {
        let mut rx = self.rx.clone();
        loop {
            if *rx.borrow_and_update() == State::Killed || rx.changed().await.is_err() {
                return;
            }
        }
    }
}

// ── Context<T> ────────────────────────────────────────────────────────────────

/// The primary request carrier in the pipeline.
///
/// Binds a business payload `T` together with:
/// - a shared [`Controller`] for lifecycle / cancellation,
/// - a per-request [`Registry`] for arbitrary side-channel metadata,
/// - a stage trace `Vec<String>` for observability.
///
/// `T` changes as the request moves through operators; the identity shell
/// (controller, registry, stages) is preserved across every transformation.
pub struct Context<T: Data> {
    current: T,
    controller: Arc<Controller>,
    registry: Registry,
    stages: Vec<String>,
}

impl<T: Data> Context<T> {
    // ── Constructors ──────────────────────────────────────────────────────────

    /// Create a brand-new request context with a freshly generated UUID.
    pub fn new(current: T) -> Self {
        Self::with_id(current, generate_id())
    }

    /// Create a request context with an explicit ID.  Useful when restoring a
    /// request identity after a network hop.
    pub fn with_id(current: T, id: impl Into<String>) -> Self {
        Self {
            current,
            controller: Arc::new(Controller::new(id)),
            registry: Registry::new(),
            stages: Vec::new(),
        }
    }

    /// Bind an existing controller to a new payload.  Useful when the calling
    /// code already controls the lifecycle and wants to attach it to a
    /// newly-constructed payload.
    pub fn with_controller(current: T, controller: Controller) -> Self {
        Self {
            current,
            controller: Arc::new(controller),
            registry: Registry::new(),
            stages: Vec::new(),
        }
    }

    /// Carry over the controller and stage trace from an existing context while
    /// replacing the payload.  The registry starts fresh.
    pub fn rejoin<U: Data>(current: T, context: Context<U>) -> Self {
        Self {
            current,
            controller: context.controller,
            registry: context.registry,
            stages: context.stages,
        }
    }

    /// Get the id of the context
    pub fn id(&self) -> &str {
        self.controller.id()
    }

    /// Get the content of the context
    pub fn content(&self) -> &T {
        &self.current
    }

    pub fn controller(&self) -> &Controller {
        &self.controller
    }

    // ── Payload transformation ────────────────────────────────────────────────

    /// Core migration primitive: swap the payload for `new_current` and return
    /// the old payload together with the new context.  All metadata is moved
    /// intact; nothing is cloned.
    pub fn transfer<U: Data>(self, new_current: U) -> (T, Context<U>) {
        let old_current = self.current;
        let new_ctx = Context {
            current: new_current,
            controller: self.controller,
            registry: self.registry,
            stages: self.stages,
        };
        (old_current, new_ctx)
    }

    /// Separate the payload from the context shell, returning `(T, Context<()>)`.
    pub fn into_parts(self) -> (T, Context<()>) {
        self.transfer(())
    }

    /// Apply `f` to the payload, preserving all metadata.
    pub fn map<U: Data, F: FnOnce(T) -> U>(self, f: F) -> Context<U> {
        let (current, ctx) = self.transfer(());
        let new_current = f(current);
        let (_, new_ctx) = ctx.transfer(new_current);
        new_ctx
    }

    /// Fallible variant of [`map`](Self::map).
    pub fn try_map<U: Data, E, F: FnOnce(T) -> Result<U, E>>(
        self,
        f: F,
    ) -> Result<Context<U>, E> {
        let (current, ctx) = self.transfer(());
        let new_current = f(current)?;
        let (_, new_ctx) = ctx.transfer(new_current);
        Ok(new_ctx)
    }

    // ── Stage tracking ────────────────────────────────────────────────────────

    pub fn stages(&self) -> &[String] {
        &self.stages
    }

    pub fn add_stage(&mut self, stage: impl Into<String>) {
        self.stages.push(stage.into());
    }

    // ── Registry delegation — shared ──────────────────────────────────────────

    /// Store a value in shared-read storage.
    pub fn insert<V: Any + Send + Sync + 'static>(&mut self, key: impl Into<String>, value: V) {
        self.registry.insert_shared(key, value);
    }

    /// Retrieve a shared value by key. Returns `Err(String)` on missing key or
    /// type mismatch.
    pub fn get<V: Any + Send + Sync + 'static>(&self, key: &str) -> Result<Arc<V>, String> {
        self.registry.get_shared(key)
    }

    // ── Registry delegation — unique ──────────────────────────────────────────

    /// Store a value in one-shot (unique) storage.
    pub fn insert_unique<V: Any + Send + Sync + 'static>(
        &mut self,
        key: impl Into<String>,
        value: V,
    ) {
        self.registry.insert_unique(key, value);
    }

    /// Move a value out of unique storage; the key is removed.
    pub fn take_unique<V: Any + Send + Sync + 'static>(
        &mut self,
        key: &str,
    ) -> Result<V, String> {
        self.registry.take_unique(key)
    }

    /// Clone a value from unique storage without removing it.
    pub fn clone_unique<V: Any + Send + Sync + Clone + 'static>(
        &self,
        key: &str,
    ) -> Result<V, String> {
        self.registry.clone_unique(key)
    }
}

// ── Standard trait impls for Context<T> ──────────────────────────────────────

/// Only the request ID is printed to avoid requiring `T: Debug` and to prevent
/// sensitive payload data from leaking into logs.
impl<T: Data> fmt::Debug for Context<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Context")
            .field("id", &self.controller.id())
            .finish()
    }
}

impl<T: Data> Deref for Context<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.current
    }
}

impl<T: Data> DerefMut for Context<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.current
    }
}

/// Lifting a bare value into the pipeline creates a fresh request context.
impl<T: Data> From<T> for Context<T> {
    fn from(value: T) -> Self {
        Context::new(value)
    }
}

impl<T: Data + Into<U>, U: Data> IntoContext<U> for Context<T> {
    fn into_context(self) -> Context<U> {
        self.map(Into::into)
    }
}

impl<T: Data> AsyncEngineContextProvider for Context<T> {
    fn context(&self) -> Arc<dyn AsyncEngineContext> {
        self.controller.clone()
    }
}

// ── StreamContext ─────────────────────────────────────────────────────────────

/// A shared, payload-free view of a request context for streaming responses.
///
/// When a pipeline transitions from processing a single `Context<T>` to
/// emitting a `ResponseStream`, every response item needs access to the same
/// controller and registry but no longer has exclusive ownership of a payload.
/// `StreamContext` wraps these shared pieces in `Arc` so that multiple
/// concurrent tasks can hold a clone of the same session state.
pub struct StreamContext {
    controller: Arc<Controller>,
    registry: Arc<Registry>,
    stages: Vec<String>,
}

impl StreamContext {
    pub fn new(controller: Arc<Controller>, registry: Registry) -> Self {
        Self {
            controller,
            registry: Arc::new(registry),
            stages: Vec::new(),
        }
    }

    // ── Registry access (read-only; registry is shared via Arc) ──────────────

    pub fn get<V: Any + Send + Sync + 'static>(&self, key: &str) -> Result<Arc<V>, String> {
        self.registry.get_shared(key)
    }

    pub fn clone_unique<V: Any + Send + Sync + Clone + 'static>(
        &self,
        key: &str,
    ) -> Result<V, String> {
        self.registry.clone_unique(key)
    }

    /// Expose the shared registry handle for complex consumers.
    pub fn registry(&self) -> Arc<Registry> {
        Arc::clone(&self.registry)
    }

    // ── Stage tracking ────────────────────────────────────────────────────────

    pub fn stages(&self) -> &[String] {
        &self.stages
    }

    pub fn add_stage(&mut self, stage: impl Into<String>) {
        self.stages.push(stage.into());
    }
}

#[async_trait]
impl AsyncEngineContext for StreamContext {
    fn id(&self) -> &str {
        self.controller.id()
    }

    fn stop(&self) {
        self.controller.stop();
    }

    fn kill(&self) {
        self.controller.kill();
    }

    fn stop_generating(&self) {
        self.controller.stop_generating();
    }

    fn is_stopped(&self) -> bool {
        self.controller.is_stopped()
    }

    fn is_killed(&self) -> bool {
        self.controller.is_killed()
    }

    async fn stopped(&self) {
        self.controller.stopped().await;
    }

    async fn killed(&self) {
        self.controller.killed().await;
    }

    fn link_child(&self, child: Arc<dyn AsyncEngineContext>) {
        self.controller.link_child(child);
    }
}

impl AsyncEngineContextProvider for StreamContext {
    fn context(&self) -> Arc<dyn AsyncEngineContext> {
        self.controller.clone()
    }
}

/// Convert a `Context<T>` into its shared stream view.  The payload is dropped;
/// the controller and registry are carried forward.
impl<T: Data> From<Context<T>> for StreamContext {
    fn from(value: Context<T>) -> Self {
        StreamContext::new(value.controller, value.registry)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone)]
    struct Input {
        value: String,
    }

    #[derive(Debug, Clone)]
    struct Processed {
        length: usize,
    }

    #[derive(Debug, Clone)]
    struct Final {
        message: String,
    }

    impl From<Input> for Processed {
        fn from(input: Input) -> Self {
            Processed {
                length: input.value.len(),
            }
        }
    }

    impl From<Processed> for Final {
        fn from(processed: Processed) -> Self {
            Final {
                message: format!("Processed length: {}", processed.length),
            }
        }
    }

    #[test]
    fn test_insert_and_get() {
        let mut ctx = Context::new(Input {
            value: "Hello".to_string(),
        });

        ctx.insert("key1", 42);
        ctx.insert("key2", "some data".to_string());

        assert_eq!(*ctx.get::<i32>("key1").unwrap(), 42);
        assert_eq!(*ctx.get::<String>("key2").unwrap(), "some data");
        assert!(ctx.get::<f64>("key1").is_err()); // Testing a downcast failure
    }

    #[test]
    fn test_transfer() {
        let ctx = Context::new(Input {
            value: "Hello".to_string(),
        });

        let (input, ctx) = ctx.transfer(Processed { length: 5 });

        assert_eq!(input.value, "Hello");
        assert_eq!(ctx.length, 5);
    }

    #[test]
    fn test_map() {
        let ctx = Context::new(Input {
            value: "Hello".to_string(),
        });

        let ctx: Context<Processed> = ctx.map(|input| input.into());
        let ctx: Context<Final> = ctx.map(|processed| processed.into());

        assert_eq!(ctx.current.message, "Processed length: 5");
    }

    #[test]
    fn test_into_context() {
        let ctx = Context::new(Input {
            value: "Hello".to_string(),
        });

        let ctx: Context<Processed> = ctx.into_context();
        let ctx: Context<Final> = ctx.into_context();

        assert_eq!(ctx.current.message, "Processed length: 5");
    }
}