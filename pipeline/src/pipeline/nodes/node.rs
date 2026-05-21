//! Core graph primitives: [`Source`], [`Sink`], [`Edge`], [`PipelineNode`],
//! [`Operator`], [`PipelineOperator`] and their forward/backward edge views.
//!
//! Design axiom: **who can call what is dictated by the graph topology, not by
//! arbitrary code holding a reference**.  The [`private::Token`] zero-sized
//! type enforces this at compile time — only code inside this module can
//! construct a `Token`, so `on_next` / `on_data` cannot be driven from outside
//! the pipeline machinery.

use std::sync::{Arc, OnceLock};

use anyhow::Error;
use async_trait::async_trait;
use tokio::sync::Mutex;

pub use crate::pipeline::error::PipelineError;

use super::sources::Frontend;

// ── Private token ─────────────────────────────────────────────────────────────

// pub(super) makes Token visible within the `nodes` module tree, so sibling
// modules (e.g. sources/base.rs) can implement Source/Sink without leaking the
// capability to drive on_next / on_data from outside `nodes`.
pub(super) mod private {
    /// Zero-sized "internal call permit".  Only constructible inside this
    /// module, making `on_next` / `on_data` effectively module-private even
    /// though they appear in public trait signatures.
    #[derive(Debug)]
    pub struct Token;
}

// ── PipelineIO ────────────────────────────────────────────────────────────────

/// Marker bound for types that may flow through the pipeline graph.
pub trait PipelineIO: Send + Sync + 'static {}
impl<T: Send + Sync + 'static> PipelineIO for T {}

// ── AsyncEngine ───────────────────────────────────────────────────────────────

/// The uniform async engine interface.  Both local graph nodes and remote
/// transport adapters implement this trait, so callers never need to know which
/// side they are talking to.
#[async_trait]
pub trait AsyncEngine<In, Out, E>: Send + Sync + 'static
where
    In: PipelineIO,
    Out: PipelineIO,
    E: Send + Sync + 'static,
{
    async fn generate(&self, input: In) -> Result<Out, E>;
}

// ── Source<T> ─────────────────────────────────────────────────────────────────

/// Output port of a graph node for data type `T`.
///
/// A node that produces `T` implements `Source<T>`.  The only public surface
/// is [`Source::link`] — which is the sole sanctioned way to wire up the graph
/// topology.  `on_next` and `set_edge` are sealed behind [`private::Token`].
#[async_trait]
pub trait Source<T: PipelineIO>: Send + Sync + 'static {
    /// Push `data` to the downstream edge.  May only be called from within
    /// this module (requires `private::Token`).
    async fn on_next(&self, data: T, _: private::Token) -> Result<(), Error>;

    /// Register the single downstream edge.  May only be called from within
    /// this module (requires `private::Token`).
    fn set_edge(&self, edge: Edge<T>, _: private::Token) -> Result<(), PipelineError>;

    /// Public graph-building entry point.  Creates an [`Edge`] to `sink`,
    /// calls `set_edge`, and returns `sink` for chaining.
    fn link<S>(&self, sink: Arc<S>) -> Result<Arc<S>, PipelineError>
    where
        S: Sink<T> + 'static,
    {
        let edge = Edge::new(sink.clone());
        self.set_edge(edge, private::Token)?;
        Ok(sink)
    }
}

// ── Sink<T> ───────────────────────────────────────────────────────────────────

/// Input port of a graph node for data type `T`.
///
/// A node that consumes `T` implements `Sink<T>`.  `on_data` is sealed behind
/// [`private::Token`] to prevent arbitrary external invocation.
#[async_trait]
pub trait Sink<T: PipelineIO>: Send + Sync + 'static {
    /// Process `data` that has arrived from an upstream node.  May only be
    /// called from within this module (requires `private::Token`).
    async fn on_data(&self, data: T, _: private::Token) -> Result<(), Error>;
}

// ── Edge<T> ───────────────────────────────────────────────────────────────────

/// A directed edge between a [`Source`] and a [`Sink`] for data type `T`.
///
/// Intentionally thin: it stores only a reference to the downstream sink and
/// forwards data to it.  All business logic lives in the nodes; the edge is a
/// pure topology object.
pub struct Edge<T: PipelineIO> {
    downstream: Arc<dyn Sink<T>>,
}

impl<T: PipelineIO> Edge<T> {
    pub fn new(downstream: Arc<dyn Sink<T>>) -> Self {
        Self { downstream }
    }

    /// Forward `data` to the downstream sink.
    pub async fn write(&self, data: T) -> Result<(), Error> {
        self.downstream.on_data(data, private::Token).await
    }
}

// ── Operator<UpIn, UpOut, DownIn, DownOut> ────────────────────────────────────

/// A **bidirectional** transformation node.
///
/// Unlike [`PipelineNode`] (which only maps `In → Out`), an `Operator` is
/// aware of both the forward request path and the reverse response path.  Its
/// `generate` receives the upstream request *and* a handle to the downstream
/// sub-graph engine, so it can:
///
/// 1. Transform `UpIn → DownIn` and forward the request.
/// 2. Receive `DownOut` from the downstream engine.
/// 3. Transform `DownOut → UpOut` and return the result.
///
/// Four type parameters make the upstream and downstream protocols explicit:
/// * `UpIn`  — request coming **from** upstream
/// * `UpOut` — response going **back to** upstream
/// * `DownIn`  — request going **to** downstream
/// * `DownOut` — response coming **from** downstream
#[async_trait]
pub trait Operator<UpIn, UpOut, DownIn, DownOut>: Send + Sync + 'static
where
    UpIn: PipelineIO,
    UpOut: PipelineIO,
    DownIn: PipelineIO,
    DownOut: PipelineIO,
{
    async fn generate(
        &self,
        req: UpIn,
        next: Arc<dyn AsyncEngine<DownIn, DownOut, Error>>,
    ) -> Result<UpOut, Error>;

    /// Convenience: wrap `self` in [`PipelineOperator`].
    fn into_operator(self: &Arc<Self>) -> Arc<PipelineOperator<UpIn, UpOut, DownIn, DownOut>>
    where
        Self: Sized,
    {
        PipelineOperator::new(self.clone())
    }
}

// ── SinkEdge<T> ───────────────────────────────────────────────────────────────

/// A lightweight [`Source`] that simply forwards data to a registered
/// downstream edge.  Used as the upstream response port of
/// [`PipelineOperator`].
pub struct SinkEdge<T: PipelineIO> {
    edge: OnceLock<Edge<T>>,
}

impl<T: PipelineIO> Default for SinkEdge<T> {
    fn default() -> Self {
        Self {
            edge: OnceLock::new(),
        }
    }
}

#[async_trait]
impl<T: PipelineIO> Source<T> for SinkEdge<T> {
    async fn on_next(&self, data: T, _: private::Token) -> Result<(), Error> {
        match self.edge.get() {
            Some(edge) => edge.write(data).await,
            None => Err(PipelineError::NoEdge.into()),
        }
    }

    fn set_edge(&self, edge: Edge<T>, _: private::Token) -> Result<(), PipelineError> {
        self.edge
            .set(edge)
            .map_err(|_| PipelineError::EdgeAlreadySet)
    }
}

// ── PipelineOperator ──────────────────────────────────────────────────────────

/// Wraps an [`Operator`] into a graph-connectable node with explicit forward
/// and backward edge views.
///
/// Internal layout:
/// * `operator`   — the bidirectional business logic
/// * `downstream` — inner [`InnerFrontend`] that bridges `operator → sub-graph`
/// * `upstream`   — [`SinkEdge`] that routes `UpOut` back to the upstream node
///
/// Use [`PipelineOperator::forward_edge`] to obtain the request-path handle
/// and [`PipelineOperator::backward_edge`] for the response-path handle.
pub struct PipelineOperator<UpIn, UpOut, DownIn, DownOut>
where
    UpIn: PipelineIO,
    UpOut: PipelineIO,
    DownIn: PipelineIO,
    DownOut: PipelineIO,
{
    operator: Arc<dyn Operator<UpIn, UpOut, DownIn, DownOut>>,
    downstream: Arc<Frontend<DownIn, DownOut>>,
    upstream: SinkEdge<UpOut>,
}

impl<UpIn, UpOut, DownIn, DownOut> PipelineOperator<UpIn, UpOut, DownIn, DownOut>
where
    UpIn: PipelineIO,
    UpOut: PipelineIO,
    DownIn: PipelineIO,
    DownOut: PipelineIO,
{
    pub fn new(operator: Arc<dyn Operator<UpIn, UpOut, DownIn, DownOut>>) -> Arc<Self> {
        Arc::new(Self {
            operator,
            downstream: Arc::new(Frontend::default()),
            upstream: SinkEdge::default(),
        })
    }

    /// Returns the **request-path** view of this operator.
    ///
    /// Connect upstream nodes here: the returned handle implements
    /// `Sink<UpIn>` (receives upstream requests) and `Source<DownIn>`
    /// (exposes the downstream connection point).
    pub fn forward_edge(self: &Arc<Self>) -> Arc<PipelineOperatorForwardEdge<UpIn, UpOut, DownIn, DownOut>> {
        Arc::new(PipelineOperatorForwardEdge {
            parent: Arc::clone(self),
        })
    }

    /// Returns the **response-path** view of this operator.
    ///
    /// Connect downstream nodes here: the returned handle implements
    /// `Sink<DownOut>` (receives downstream responses) and `Source<UpOut>`
    /// (exposes the upstream response connection point).
    pub fn backward_edge(self: &Arc<Self>) -> Arc<PipelineOperatorBackwardEdge<UpIn, UpOut, DownIn, DownOut>> {
        Arc::new(PipelineOperatorBackwardEdge {
            parent: Arc::clone(self),
        })
    }
}

#[async_trait]
impl<UpIn, UpOut, DownIn, DownOut> AsyncEngine<UpIn, UpOut, Error>
    for PipelineOperator<UpIn, UpOut, DownIn, DownOut>
where
    UpIn: PipelineIO,
    UpOut: PipelineIO,
    DownIn: PipelineIO,
    DownOut: PipelineIO,
{
    async fn generate(&self, input: UpIn) -> Result<UpOut, Error> {
        let engine: Arc<dyn AsyncEngine<DownIn, DownOut, Error>> =
            self.downstream.clone();
        self.operator.generate(input, engine).await
    }
}

// ── PipelineOperatorForwardEdge ───────────────────────────────────────────────

/// The **request-path** face of a [`PipelineOperator`].
///
/// * As `Sink<UpIn>`: receives upstream requests, drives the full
///   `operator.generate(UpIn, downstream)` call, then forwards `UpOut` via
///   `parent.upstream`.
/// * As `Source<DownIn>`: exposes the downstream sub-graph connection point
///   (delegates to the inner `InnerFrontend`).
pub struct PipelineOperatorForwardEdge<UpIn, UpOut, DownIn, DownOut>
where
    UpIn: PipelineIO,
    UpOut: PipelineIO,
    DownIn: PipelineIO,
    DownOut: PipelineIO,
{
    parent: Arc<PipelineOperator<UpIn, UpOut, DownIn, DownOut>>,
}

#[async_trait]
impl<UpIn, UpOut, DownIn, DownOut> Sink<UpIn>
    for PipelineOperatorForwardEdge<UpIn, UpOut, DownIn, DownOut>
where
    UpIn: PipelineIO,
    UpOut: PipelineIO,
    DownIn: PipelineIO,
    DownOut: PipelineIO,
{
    async fn on_data(&self, data: UpIn, _: private::Token) -> Result<(), Error> {
        let up_out = self.parent.generate(data).await?;
        self.parent.upstream.on_next(up_out, private::Token).await
    }
}

#[async_trait]
impl<UpIn, UpOut, DownIn, DownOut> Source<DownIn>
    for PipelineOperatorForwardEdge<UpIn, UpOut, DownIn, DownOut>
where
    UpIn: PipelineIO,
    UpOut: PipelineIO,
    DownIn: PipelineIO,
    DownOut: PipelineIO,
{
    async fn on_next(&self, data: DownIn, token: private::Token) -> Result<(), Error> {
        self.parent.downstream.on_next(data, token).await
    }

    fn set_edge(&self, edge: Edge<DownIn>, token: private::Token) -> Result<(), PipelineError> {
        self.parent.downstream.set_edge(edge, token)
    }
}

// ── PipelineOperatorBackwardEdge ──────────────────────────────────────────────

/// The **response-path** face of a [`PipelineOperator`].
///
/// * As `Sink<DownOut>`: receives downstream responses and delivers them to
///   the inner `InnerFrontend` so that the awaiting `operator.generate` call
///   is resumed.
/// * As `Source<UpOut>`: exposes the upstream response connection point
///   (delegates to `parent.upstream`).
pub struct PipelineOperatorBackwardEdge<UpIn, UpOut, DownIn, DownOut>
where
    UpIn: PipelineIO,
    UpOut: PipelineIO,
    DownIn: PipelineIO,
    DownOut: PipelineIO,
{
    parent: Arc<PipelineOperator<UpIn, UpOut, DownIn, DownOut>>,
}

#[async_trait]
impl<UpIn, UpOut, DownIn, DownOut> Sink<DownOut>
    for PipelineOperatorBackwardEdge<UpIn, UpOut, DownIn, DownOut>
where
    UpIn: PipelineIO,
    UpOut: PipelineIO,
    DownIn: PipelineIO,
    DownOut: PipelineIO,
{
    async fn on_data(&self, data: DownOut, token: private::Token) -> Result<(), Error> {
        self.parent.downstream.on_data(data, token).await
    }
}

#[async_trait]
impl<UpIn, UpOut, DownIn, DownOut> Source<UpOut>
    for PipelineOperatorBackwardEdge<UpIn, UpOut, DownIn, DownOut>
where
    UpIn: PipelineIO,
    UpOut: PipelineIO,
    DownIn: PipelineIO,
    DownOut: PipelineIO,
{
    async fn on_next(&self, data: UpOut, token: private::Token) -> Result<(), Error> {
        self.parent.upstream.on_next(data, token).await
    }

    fn set_edge(&self, edge: Edge<UpOut>, token: private::Token) -> Result<(), PipelineError> {
        self.parent.upstream.set_edge(edge, token)
    }
}

// ── NodeFn ────────────────────────────────────────────────────────────────────

/// The mapping function type used by [`PipelineNode`].
pub type NodeFn<In, Out> = Arc<dyn Fn(In) -> Out + Send + Sync + 'static>;

// ── PipelineNode<In, Out> ─────────────────────────────────────────────────────

/// The simplest graph node: a pure **unidirectional** `In → Out` transformer.
///
/// Use this when a node only needs to map one data type to another and does
/// not need to observe — let alone modify — the downstream response.  If you
/// need to wrap the downstream call or change the response, use
/// [`PipelineOperator`] instead.
///
/// Internally it implements both `Sink<In>` (to receive upstream data) and
/// `Source<Out>` (to push transformed data downstream).
pub struct PipelineNode<In: PipelineIO, Out: PipelineIO> {
    edge: OnceLock<Edge<Out>>,
    map_fn: NodeFn<In, Out>,
}

impl<In: PipelineIO, Out: PipelineIO> PipelineNode<In, Out> {
    pub fn new(map_fn: impl Fn(In) -> Out + Send + Sync + 'static) -> Arc<Self> {
        Arc::new(Self {
            edge: OnceLock::new(),
            map_fn: Arc::new(map_fn),
        })
    }
}

#[async_trait]
impl<In: PipelineIO, Out: PipelineIO> Source<Out> for PipelineNode<In, Out> {
    async fn on_next(&self, data: Out, _: private::Token) -> Result<(), Error> {
        match self.edge.get() {
            Some(edge) => edge.write(data).await,
            None => Err(PipelineError::NoEdge.into()),
        }
    }

    fn set_edge(&self, edge: Edge<Out>, _: private::Token) -> Result<(), PipelineError> {
        self.edge
            .set(edge)
            .map_err(|_| PipelineError::EdgeAlreadySet)
    }
}

#[async_trait]
impl<In: PipelineIO, Out: PipelineIO> Sink<In> for PipelineNode<In, Out> {
    async fn on_data(&self, data: In, _: private::Token) -> Result<(), Error> {
        let out = (self.map_fn)(data);
        self.on_next(out, private::Token).await
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomOrd};

    // ── helpers ───────────────────────────────────────────────────────────────

    /// A simple sink that collects received items.
    struct CollectSink<T: PipelineIO + Clone> {
        items: Mutex<Vec<T>>,
    }

    impl<T: PipelineIO + Clone> CollectSink<T> {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                items: Mutex::new(Vec::new()),
            })
        }

        async fn collected(&self) -> Vec<T> {
            self.items.lock().await.clone()
        }
    }

    #[async_trait]
    impl<T: PipelineIO + Clone> Sink<T> for CollectSink<T> {
        async fn on_data(&self, data: T, _: private::Token) -> Result<(), Error> {
            self.items.lock().await.push(data);
            Ok(())
        }
    }

    // ── Edge ──────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn edge_forwards_to_sink() {
        let sink = CollectSink::<i32>::new();
        let edge = Edge::new(Arc::clone(&sink));
        edge.write(42).await.unwrap();
        assert_eq!(sink.collected().await, vec![42]);
    }

    // ── PipelineNode ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn pipeline_node_maps_and_forwards() {
        let node = PipelineNode::new(|x: i32| x * 2);
        let sink = CollectSink::<i32>::new();

        node.link(Arc::clone(&sink)).unwrap();
        node.on_data(5, private::Token).await.unwrap();
        node.on_data(7, private::Token).await.unwrap();

        assert_eq!(sink.collected().await, vec![10, 14]);
    }

    #[tokio::test]
    async fn pipeline_node_edge_already_set_error() {
        let node = PipelineNode::new(|x: i32| x);
        let sink1 = CollectSink::<i32>::new();
        let sink2 = CollectSink::<i32>::new();
        node.link(sink1).unwrap();
        let err = node.link(sink2).unwrap_err();
        assert!(matches!(err, PipelineError::EdgeAlreadySet));
    }

    #[tokio::test]
    async fn pipeline_node_no_edge_error() {
        let node = PipelineNode::new(|x: i32| x);
        let err = node.on_data(1, private::Token).await.unwrap_err();
        assert!(err.to_string().contains("no downstream edge"));
    }

    // ── Source::link chain ────────────────────────────────────────────────────

    #[tokio::test]
    async fn link_chain_two_nodes() {
        // node_a: i32 -> String, node_b: String -> usize
        let node_a = PipelineNode::new(|x: i32| x.to_string());
        let node_b = PipelineNode::new(|s: String| s.len());
        let sink = CollectSink::<usize>::new();

        node_a.link(Arc::clone(&node_b)).unwrap();
        node_b.link(Arc::clone(&sink)).unwrap();

        node_a.on_data(12345, private::Token).await.unwrap();

        assert_eq!(sink.collected().await, vec![5]);
    }

    // ── PipelineOperator ──────────────────────────────────────────────────────

    /// A trivial operator: multiplies the request by `factor`, calls the
    /// downstream engine, then wraps the response in a string.
    struct MulOperator {
        factor: i32,
    }

    #[async_trait]
    impl Operator<i32, String, i32, i32> for MulOperator {
        async fn generate(
            &self,
            req: i32,
            next: Arc<dyn AsyncEngine<i32, i32, Error>>,
        ) -> Result<String, Error> {
            let down_in = req * self.factor;
            let down_out = next.generate(down_in).await?;
            Ok(format!("result:{down_out}"))
        }
    }

    /// A leaf engine that doubles its input.
    struct DoubleEngine;

    #[async_trait]
    impl AsyncEngine<i32, i32, Error> for DoubleEngine {
        async fn generate(&self, input: i32) -> Result<i32, Error> {
            Ok(input * 2)
        }
    }

    /// Downstream backend sink: receives `i32`, runs `DoubleEngine`, and
    /// delivers the result back via the backward edge's `InnerFrontend`.
    struct BackendSink {
        backward: Arc<PipelineOperatorBackwardEdge<i32, String, i32, i32>>,
    }

    #[async_trait]
    impl Sink<i32> for BackendSink {
        async fn on_data(&self, data: i32, _: private::Token) -> Result<(), Error> {
            let engine = DoubleEngine;
            let out = engine.generate(data).await?;
            self.backward.on_data(out, private::Token).await
        }
    }

    #[tokio::test]
    async fn pipeline_operator_bidirectional_transform() {
        // Operator: req * 3 -> downstream, then "result:{downstream_out}"
        let op = PipelineOperator::new(Arc::new(MulOperator { factor: 3 }));

        let fwd = op.forward_edge();
        let bwd = op.backward_edge();

        // Downstream: receives DownIn, runs DoubleEngine, sends DownOut back
        let backend = Arc::new(BackendSink {
            backward: Arc::clone(&bwd),
        });

        // Wire: forward_edge (Source<DownIn>) -> backend (Sink<DownIn>)
        fwd.link(Arc::clone(&backend)).unwrap();

        // Wire: backward_edge (Source<UpOut>) -> collect_sink
        let result_sink = CollectSink::<String>::new();
        bwd.link(Arc::clone(&result_sink)).unwrap();

        // Drive: upstream sends UpIn=4
        // Expected:  DownIn = 4*3 = 12,  DoubleEngine: 12*2 = 24,  UpOut = "result:24"
        fwd.on_data(4, private::Token).await.unwrap();

        assert_eq!(result_sink.collected().await, vec!["result:24"]);
    }

    // ── operator call-count sanity ────────────────────────────────────────────

    struct CountOperator {
        count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Operator<(), (), (), ()> for CountOperator {
        async fn generate(
            &self,
            _: (),
            next: Arc<dyn AsyncEngine<(), (), Error>>,
        ) -> Result<(), Error> {
            self.count.fetch_add(1, AtomOrd::Relaxed);
            next.generate(()).await
        }
    }

    struct NoopBackend {
        bwd: Arc<PipelineOperatorBackwardEdge<(), (), (), ()>>,
    }

    #[async_trait]
    impl Sink<()> for NoopBackend {
        async fn on_data(&self, data: (), _: private::Token) -> Result<(), Error> {
            self.bwd.on_data(data, private::Token).await
        }
    }

    #[tokio::test]
    async fn operator_called_once_per_request() {
        let count = Arc::new(AtomicUsize::new(0));
        let op = PipelineOperator::new(Arc::new(CountOperator {
            count: Arc::clone(&count),
        }));

        let fwd = op.forward_edge();
        let bwd = op.backward_edge();

        let backend = Arc::new(NoopBackend {
            bwd: Arc::clone(&bwd),
        });
        fwd.link(Arc::clone(&backend)).unwrap();

        let sink = CollectSink::<()>::new();
        bwd.link(Arc::clone(&sink)).unwrap();

        fwd.on_data((), private::Token).await.unwrap();
        fwd.on_data((), private::Token).await.unwrap();
        fwd.on_data((), private::Token).await.unwrap();

        assert_eq!(count.load(AtomOrd::Relaxed), 3);
        assert_eq!(sink.collected().await.len(), 3);
    }
}
