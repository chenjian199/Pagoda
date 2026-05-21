// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Core engine trait system for Pagoda's composable streaming inference.

use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

// ─── Data trait ───

/// Marker trait for types that can flow through engine pipelines.
pub trait Data: Send + Sync + 'static {}
impl<T: Send + Sync + 'static> Data for T {}

// ─── Core type aliases ───

/// A unary (single-value) data container.
pub type DataUnary<T> = Pin<Box<dyn Future<Output = Result<T, EngineError>> + Send>>;

/// A streaming data container.
pub type DataStream<T> = Pin<Box<dyn tokio_stream::Stream<Item = Result<T, EngineError>> + Send>>;

/// Shorthand for a boxed async engine.
pub type Engine<Req, Resp, E> = Box<dyn AsyncEngine<Req, Resp, E> + Send + Sync>;

/// A unary engine returning a single response.
pub type EngineUnary<Resp> = Engine<Resp, Resp, EngineError>;

/// A streaming engine returning a response stream.
pub type EngineStream<Resp> = Engine<Resp, Resp, EngineError>;

/// Type-erased engine context.
pub type Context = Arc<dyn AsyncEngineContext>;

// ─── Engine error ───

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("engine cancelled")]
    Cancelled,
    #[error("engine stopped")]
    Stopped,
    #[error("engine killed")]
    Killed,
    #[error("internal error: {0}")]
    Internal(#[from] anyhow::Error),
}

// ─── AsyncEngineContext ───

/// Provides lifecycle control for a single engine invocation.
#[async_trait]
pub trait AsyncEngineContext: Send + Sync + 'static {
    /// Unique identifier for this context.
    fn id(&self) -> &str;

    /// Whether the context has been stopped (gracefully).
    fn is_stopped(&self) -> bool;

    /// Whether the context has been killed (immediately).
    fn is_killed(&self) -> bool;

    /// Future that resolves when context is stopped.
    async fn stopped(&self);

    /// Future that resolves when context is killed.
    async fn killed(&self);

    /// Signal to stop generating new tokens but finish current work.
    fn stop_generating(&self);

    /// Gracefully stop the engine invocation.
    fn stop(&self);

    /// Immediately kill the engine invocation.
    fn kill(&self);

    /// Link a child context whose lifecycle is bound to this parent.
    fn link_child(&self, child: Arc<dyn AsyncEngineContext>);
}

// ─── AsyncEngineContextProvider ───

/// Trait for types that can provide an engine context.
pub trait AsyncEngineContextProvider: Send + Sync {
    fn context(&self) -> Arc<dyn AsyncEngineContext>;
}

// ─── AsyncEngine ───

/// Core async engine trait — transforms a request into a streaming response.
#[async_trait]
pub trait AsyncEngine<Req, Resp, E>: Send + Sync + 'static
where
    Req: Data,
    Resp: Data,
    E: Send + Sync + 'static,
{
    /// Generate a response stream from a request and context.
    async fn generate(
        &self,
        context: Arc<dyn AsyncEngineContext>,
        request: Req,
    ) -> Result<ResponseStream<Resp>, E>;
}

// ─── ResponseStream ───

/// Wraps a `DataStream` with its associated engine context.
pub struct ResponseStream<R: Data> {
    pub stream: DataStream<R>,
    pub context: Arc<dyn AsyncEngineContext>,
}

impl<R: Data> ResponseStream<R> {
    pub fn new(stream: DataStream<R>, context: Arc<dyn AsyncEngineContext>) -> Self {
        Self { stream, context }
    }

    pub fn from_channel(
        rx: mpsc::Receiver<Result<R, EngineError>>,
        context: Arc<dyn AsyncEngineContext>,
    ) -> Self {
        let stream = Box::pin(ReceiverStream::new(rx));
        Self { stream, context }
    }
}

// ─── Type erasure system ───

/// Type-erased async engine.
pub type AnyAsyncEngine = dyn Any + Send + Sync;

/// Wrapper that holds a type-erased engine.
pub struct AnyEngineWrapper {
    inner: Box<AnyAsyncEngine>,
}

impl AnyEngineWrapper {
    pub fn new<T: Send + Sync + 'static>(engine: T) -> Self {
        Self {
            inner: Box::new(engine),
        }
    }

    pub fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        self.inner.downcast_ref::<T>()
    }

    pub fn downcast<T: 'static>(self) -> Result<Box<T>, Self> {
        match self.inner.downcast::<T>() {
            Ok(inner) => Ok(inner),
            Err(inner) => Err(Self { inner }),
        }
    }
}

/// Trait to erase engine to `Any`.
pub trait AsAnyAsyncEngine: Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
}

impl<T: Send + Sync + 'static> AsAnyAsyncEngine for T {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
}

/// Trait to downcast from a type-erased engine back to a concrete type.
pub trait DowncastAnyAsyncEngine {
    fn downcast_ref<T: 'static>(&self) -> Option<&T>;
}

impl DowncastAnyAsyncEngine for dyn Any + Send + Sync {
    fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        (self as &dyn Any).downcast_ref::<T>()
    }
}

// ─── Service engine type aliases ───

/// A single input value.
pub type SingleIn<T> = T;

/// Multiple input values (streamed).
pub type ManyIn<T> = DataStream<T>;

/// A single output value.
pub type SingleOut<U> = DataUnary<U>;

/// Multiple output values (streamed).
pub type ManyOut<U> = DataStream<U>;

/// A service engine: single request in, streaming response out.
pub type ServiceEngine<T, U> =
    Box<dyn AsyncEngine<SingleIn<T>, ManyOut<U>, EngineError> + Send + Sync>;

/// A unary engine: single request in, single response out.
pub type UnaryEngine<T, U> =
    Box<dyn AsyncEngine<SingleIn<T>, SingleOut<U>, EngineError> + Send + Sync>;
