//! Engine traits, type aliases, response stream wrapper, and the type-erasure
//! system used to store heterogeneous engines in a single collection.
//!
//! Design reference: `docs/modules/engine.md`.
//!
//! ## Layer overview
//!
//! ```text
//! AsyncEngine<Req, Resp, E>           ← core engine trait (one method)
//!     │
//!     ├─ Engine<Req, Resp, E>         ← Arc<dyn AsyncEngine<…>> alias
//!     │
//!     ├─ ResponseStream<R>            ← re-attaches context after stream transforms
//!     │
//!     └─ type-erasure system
//!          AsAnyAsyncEngine           ← erase into Arc<dyn AnyAsyncEngine>
//!          DowncastAnyAsyncEngine     ← recover Arc<dyn AsyncEngine<…>>
//! ```

use std::any::{Any, TypeId};
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};

use async_trait::async_trait;
use futures::Stream;

// ── Shared trait bounds ─────────────────────────────────────────────────────

/// Blanket marker for types that can flow through the pipeline.
pub trait Data: Send + Sync + 'static {}
impl<T: Send + Sync + 'static> Data for T {}

pub trait AsyncEngineController: Send + Sync {}
// ── AsyncEngineContext ────────────────────────────────────────────────────────

/// Uniform control interface that every request context exposes to pipeline
/// nodes and engine implementations.  Implementors: `Controller`,
/// `StreamContext`.
#[async_trait]
pub trait AsyncEngineContext: Send + Sync + 'static {
    /// Stable request identifier (usually a UUID string).
    fn id(&self) -> &str;

    /// Broadcast `Stopped` to self and all linked children.
    fn stop(&self);

    /// Broadcast `Killed` to self and all linked children.
    fn kill(&self);

    /// Propagate `Stopped` to children without stopping self immediately.
    fn stop_generating(&self);

    /// Returns `true` if the state is no longer `Live`.
    fn is_stopped(&self) -> bool;

    /// Returns `true` if the state has reached `Killed`.
    fn is_killed(&self) -> bool;

    /// Register a child context so that stop/kill signals cascade to it.
    fn link_child(&self, child: Arc<dyn AsyncEngineContext>);

    /// Async wait: resolves as soon as the state leaves `Live`.
    async fn stopped(&self);

    /// Async wait: resolves as soon as the state reaches `Killed`.
    async fn killed(&self);
}

// ── AsyncEngineContextProvider ────────────────────────────────────────────────

/// Implemented by objects that carry a request context (e.g. `Context<T>`,
/// `StreamContext`).  Lets downstream code retrieve the control handle without
/// knowing the concrete wrapper type.
pub trait AsyncEngineContextProvider: Send + fmt::Debug {
    fn context(&self) -> Arc<dyn AsyncEngineContext>;
}

// ── Type aliases ──────────────────────────────────────────────────────────────

/// A boxed, pinned, single-value async computation (no context embedded).
pub type DataUnary<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// A boxed, pinned async item stream (no context embedded).
pub type DataStream<T> = Pin<Box<dyn Stream<Item = T> + Send>>;

/// A shared-ownership handle to any [`AsyncEngine`] implementation.
pub type Engine<Req, Resp, E> = Arc<dyn AsyncEngine<Req, Resp, E>>;

/// A boxed, pinned single-value async computation that also promises a
/// cancellable context via [`AsyncEngineContextProvider`].
pub type EngineUnary<Resp> = Pin<Box<dyn AsyncEngineUnary<Resp>>>;

/// A boxed, pinned async item stream that also promises a cancellable context
/// via [`AsyncEngineContextProvider`].
pub type EngineStream<Resp> = Pin<Box<dyn AsyncEngineStream<Resp>>>;

/// Convenience alias for a shared request-context handle.
pub type Context = Arc<dyn AsyncEngineContext>;

// ── AsyncEngineUnary ──────────────────────────────────────────────────────────

/// A one-shot async operation that resolves to `Resp` and carries a request
/// context so it can be cancelled by the framework.
pub trait AsyncEngineUnary<Resp: Data>:
    Future<Output = Resp> + AsyncEngineContextProvider + Send
{
}

// ── AsyncEngineStream ─────────────────────────────────────────────────────────

/// An async item stream that yields `Resp` values and carries a request context
/// so it can be cancelled by the framework.
pub trait AsyncEngineStream<Resp: Data>:
    Stream<Item = Resp> + AsyncEngineContextProvider + Send
{
}

impl<Resp, T> AsyncEngineStream<Resp> for T
where
    Resp: Data,
    T: Stream<Item = Resp> + AsyncEngineContextProvider + Send,
{
}

// ── Conversion: EngineStream → DataStream ─────────────────────────────────────

/// Strip the context wrapper, producing a plain [`DataStream`] suitable for
/// use with `futures::StreamExt` combinators.  After transforms, wrap back
/// with [`ResponseStream::new`].
impl<T: Data> From<EngineStream<T>> for DataStream<T> {
    fn from(stream: EngineStream<T>) -> Self {
        Box::pin(stream)
    }
}

// ── AsyncEngine ───────────────────────────────────────────────────────────────

/// The core engine interface.  All backend implementations (TensorRT-LLM,
/// vLLM, mock, proxy) implement this trait so that the router can dispatch
/// to them uniformly.
///
/// # Design constraints
///
/// * `Resp: AsyncEngineContextProvider` — every response must carry a
///   cancellable context; the framework guarantees it can always cancel.
/// * Single method `generate` — routing, health-checking, and cancellation are
///   all mediated through the `Context` embedded in `Resp`, so no additional
///   methods are needed on the trait itself.
#[async_trait]
pub trait AsyncEngine<Req, Resp, E>: Send + Sync + 'static
where
    Req: Data,
    Resp: Data + AsyncEngineContextProvider,
    E: Data,
{
    async fn generate(&self, request: Req) -> Result<Resp, E>;
}

// ── ResponseStream ────────────────────────────────────────────────────────────

/// Re-attaches a [`AsyncEngineContext`] to a [`DataStream`] produced by stream
/// combinators (which strip the context).
///
/// # Typical usage
///
/// ```ignore
/// let raw: DataStream<Token> = engine_stream.map(transform).take_while(…);
/// let resp: EngineStream<Token> = ResponseStream::new(raw, ctx);
/// ```
pub struct ResponseStream<R: Data> {
    stream: DataStream<R>,
    ctx: Arc<dyn AsyncEngineContext>,
}

impl<R: Data> ResponseStream<R> {
    /// Wrap `stream` and its associated `ctx` into a pinned [`ResponseStream`].
    pub fn new(stream: DataStream<R>, ctx: Arc<dyn AsyncEngineContext>) -> Pin<Box<Self>> {
        Box::pin(Self { stream, ctx })
    }
}

impl<R: Data> Stream for ResponseStream<R> {
    type Item = R;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<R>> {
        // `self.stream` is `Pin<Box<…>>` — already pinned, safe to poll.
        self.stream.as_mut().poll_next(cx)
    }
}

impl<R: Data> AsyncEngineContextProvider for ResponseStream<R> {
    fn context(&self) -> Arc<dyn AsyncEngineContext> {
        Arc::clone(&self.ctx)
    }
}

impl<R: Data> fmt::Debug for ResponseStream<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResponseStream")
            .field("ctx_id", &self.ctx.id())
            // todo: add debug for stream
            .finish()
    }
}

impl<T: Data> AsyncEngineContextProvider for Pin<Box<dyn AsyncEngineUnary<T>>> {
    fn context(&self) -> Arc<dyn AsyncEngineContext> {
        AsyncEngineContextProvider::context(&**self)
    }
}

impl<T: Data> AsyncEngineContextProvider for Pin<Box<dyn AsyncEngineStream<T>>> {
    fn context(&self) -> Arc<dyn AsyncEngineContext> {
        AsyncEngineContextProvider::context(&**self)
    }
}
// ── Type-erasure system ───────────────────────────────────────────────────────
//
// `AsyncEngine<Req, Resp, E>` has three type parameters; different
// instantiations are incompatible Rust types and cannot be stored in the same
// collection directly.  The three-step erasure system below enables a runtime
// `HashMap<String, Arc<dyn AnyAsyncEngine>>` that can hold any engine, with a
// safe downcast path back to the concrete typed variant.
//
// Flow:
//   typed engine
//       │  .into_any_engine()          (AsAnyAsyncEngine)
//       ▼
//   Arc<dyn AnyAsyncEngine>  ──store──►  HashMap<…>
//       │  .downcast::<Req, Resp, E>()  (DowncastAnyAsyncEngine)
//       ▼
//   Arc<dyn AsyncEngine<Req, Resp, E>>

// ── AnyAsyncEngine ────────────────────────────────────────────────────────────

/// Type-erased engine trait object.
///
/// Stores the [`TypeId`]s of the original `Req`, `Resp`, and `E` so that
/// [`DowncastAnyAsyncEngine::downcast`] can verify type compatibility before
/// performing a safe `downcast_ref` — without callers knowing the internal
/// wrapper type.
pub trait AnyAsyncEngine: Send + Sync {
    fn request_type_id(&self) -> TypeId;
    fn response_type_id(&self) -> TypeId;
    fn error_type_id(&self) -> TypeId;

    /// Downgrade to `&dyn Any` to enable `downcast_ref` on the inner engine.
    fn as_any(&self) -> &dyn Any;
}

// ── AnyEngineWrapper ──────────────────────────────────────────────────────────

struct AnyEngineWrapper<Req, Resp, E>
where
    Req: Data,
    Resp: Data + AsyncEngineContextProvider,
    E: Data,
{
    engine: Arc<dyn AsyncEngine<Req, Resp, E>>,
    // `fn(Req, Resp, E)` PhantomData: avoids over-constraining variance and
    // Drop; the idiomatic choice when a struct "knows about" but does not own
    // the type parameters.
    _phantom: PhantomData<fn(Req, Resp, E)>,
}

impl<Req, Resp, E> AnyAsyncEngine for AnyEngineWrapper<Req, Resp, E>
where
    Req: Data,
    Resp: Data + AsyncEngineContextProvider,
    E: Data,
{
    fn request_type_id(&self) -> TypeId {
        TypeId::of::<Req>()
    }
    fn response_type_id(&self) -> TypeId {
        TypeId::of::<Resp>()
    }
    fn error_type_id(&self) -> TypeId {
        TypeId::of::<E>()
    }

    fn as_any(&self) -> &dyn Any {
        // Return a reference to the inner `Arc` — not to `self` — so that
        // `downcast_ref::<Arc<dyn AsyncEngine<Req, Resp, E>>>()` works without
        // callers ever knowing about `AnyEngineWrapper`.
        &self.engine
    }
}

// ── AsAnyAsyncEngine — type-erasure entrance ──────────────────────────────────

/// Extension trait that converts a typed engine into an [`AnyAsyncEngine`]
/// trait object.
///
/// Implemented as an extension trait (not a method on `AsyncEngine`) to keep
/// `AsyncEngine` focused on the business interface and avoid a circular
/// dependency between `AsyncEngine` and the erasure machinery.
pub trait AsAnyAsyncEngine {
    fn into_any_engine(self) -> Arc<dyn AnyAsyncEngine>;
}

impl<Req, Resp, E> AsAnyAsyncEngine for Arc<dyn AsyncEngine<Req, Resp, E>>
where
    Req: Data,
    Resp: Data + AsyncEngineContextProvider,
    E: Data,
{
    fn into_any_engine(self) -> Arc<dyn AnyAsyncEngine> {
        Arc::new(AnyEngineWrapper {
            engine: self,
            _phantom: PhantomData,
        })
    }
}

// ── DowncastAnyAsyncEngine — type-erasure exit ────────────────────────────────

/// Extension trait that recovers a typed engine from an [`AnyAsyncEngine`]
/// trait object.
pub trait DowncastAnyAsyncEngine {
    /// Returns `Some(engine)` when the stored engine's type parameters exactly
    /// match `<Req, Resp, E>`; returns `None` on mismatch.  Never panics.
    ///
    /// The two-phase check (three `TypeId` comparisons, then `downcast_ref`)
    /// makes the intent explicit while remaining fully safe — no `unsafe` code.
    fn downcast<Req, Resp, E>(&self) -> Option<Arc<dyn AsyncEngine<Req, Resp, E>>>
    where
        Req: Data,
        Resp: Data + AsyncEngineContextProvider,
        E: Data;
}

impl DowncastAnyAsyncEngine for Arc<dyn AnyAsyncEngine> {
    fn downcast<Req, Resp, E>(&self) -> Option<Arc<dyn AsyncEngine<Req, Resp, E>>>
    where
        Req: Data,
        Resp: Data + AsyncEngineContextProvider,
        E: Data,
    {
        if self.request_type_id() == TypeId::of::<Req>()
            && self.response_type_id() == TypeId::of::<Resp>()
            && self.error_type_id() == TypeId::of::<E>()
        {
            // `as_any()` returns `&self.engine` (an `Arc<dyn AsyncEngine<…>>`),
            // so `downcast_ref` checks that exact type and clones the Arc.
            self.as_any()
                .downcast_ref::<Arc<dyn AsyncEngine<Req, Resp, E>>>()
                .cloned()
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // 1. Define mock data structures
    #[derive(Debug, PartialEq)]
    struct Req1(String);

    #[derive(Debug, PartialEq)]
    struct Resp1(String);

    // Dummy context provider implementation for the response
    impl AsyncEngineContextProvider for Resp1 {
        fn context(&self) -> Arc<dyn AsyncEngineContext> {
            // For this test, we don't need a real context.
            unimplemented!()
        }
    }

    #[derive(Debug)]
    struct Err1;

    // A different set of types for testing failure cases
    #[derive(Debug)]
    struct Req2;
    #[derive(Debug)]
    struct Resp2;
    impl AsyncEngineContextProvider for Resp2 {
        fn context(&self) -> Arc<dyn AsyncEngineContext> {
            unimplemented!()
        }
    }

    // 2. Define a mock engine
    struct MockEngine;

    #[async_trait]
    impl AsyncEngine<Req1, Resp1, Err1> for MockEngine {
        async fn generate(&self, request: Req1) -> Result<Resp1, Err1> {
            Ok(Resp1(format!("response to {}", request.0)))
        }
    }

    #[tokio::test]
    async fn test_engine_type_erasure_and_downcast() {
        // 3. Create a typed engine
        let typed_engine: Arc<dyn AsyncEngine<Req1, Resp1, Err1>> = Arc::new(MockEngine);

        // 4. Use the extension trait to erase the type
        let any_engine = typed_engine.into_any_engine();

        // Check type IDs are preserved
        assert_eq!(any_engine.request_type_id(), TypeId::of::<Req1>());
        assert_eq!(any_engine.response_type_id(), TypeId::of::<Resp1>());
        assert_eq!(any_engine.error_type_id(), TypeId::of::<Err1>());

        // 5. Use the new downcast method on the Arc
        let downcasted_engine = any_engine.downcast::<Req1, Resp1, Err1>();

        // 6. Assert success
        assert!(downcasted_engine.is_some());

        // We can even use the downcasted engine
        let response = downcasted_engine
            .unwrap()
            .generate(Req1("hello".to_string()))
            .await;
        assert_eq!(response.unwrap(), Resp1("response to hello".to_string()));

        // 7. Assert failure for wrong types
        let failed_downcast = any_engine.downcast::<Req2, Resp2, Err1>();
        assert!(failed_downcast.is_none());

        // 8. HashMap usage test
        let mut engine_map: HashMap<String, Arc<dyn AnyAsyncEngine>> = HashMap::new();
        engine_map.insert("mock".to_string(), any_engine);

        let retrieved_engine = engine_map.get("mock").unwrap();
        let final_engine = retrieved_engine.downcast::<Req1, Resp1, Err1>().unwrap();
        let final_response = final_engine.generate(Req1("world".to_string())).await;
        assert_eq!(
            final_response.unwrap(),
            Resp1("response to world".to_string())
        );
    }
}
