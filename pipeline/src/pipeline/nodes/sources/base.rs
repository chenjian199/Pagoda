//! Trait implementations for [`Frontend`]: [`Default`], [`Source`], [`Sink`],
//! and [`AsyncEngine`].
//!
//! `Frontend` acts as the minimal request/response bridge between an upstream
//! caller and the downstream sub-graph.  When `generate` is called it:
//! 1. Registers a `oneshot` sender keyed by a monotonically generated ID.
//! 2. Pushes the request downstream through its `Edge<In>`.
//! 3. Awaits the `oneshot` receiver until `on_data` resolves it.

use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Error;
use async_trait::async_trait;
use tokio::sync::oneshot;

use crate::pipeline::error::PipelineError;
use crate::pipeline::nodes::node::{private, AsyncEngine, Edge, PipelineIO, Sink, Source};
use crate::engine::AsyncEngineContextProvider;
use super::Frontend;

// Global sequence counter shared across all Frontend instances.
static NEXT_ID: AtomicU64 = AtomicU64::new(0);

// ── Default ───────────────────────────────────────────────────────────────────

impl<In: PipelineIO, Out: PipelineIO> Default for Frontend<In, Out> {
    fn default() -> Self {
        Self {
            edge: std::sync::OnceLock::new(),
            sinks: std::sync::Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
        }
    }
}

// ── Source<In> ────────────────────────────────────────────────────────────────

#[async_trait]
impl<In: PipelineIO, Out: PipelineIO> Source<In> for Frontend<In, Out> {
    async fn on_next(&self, data: In, _: private::Token) -> Result<(), Error> {
        let edge = self.edge.get().ok_or(PipelineError::NoEdge)?;
        edge.write(data).await
    }

    fn set_edge(&self, edge: Edge<In>, _: private::Token) -> Result<(), PipelineError> {
        self.edge
            .set(edge)
            .map_err(|_| PipelineError::EdgeAlreadySet)
    }
}

// ── Sink<Out> ─────────────────────────────────────────────────────────────────

#[async_trait]
impl<In: PipelineIO, Out: PipelineIO + AsyncEngineContextProvider> Sink<Out> for Frontend<In, Out> {
    async fn on_data(&self, data: Out, _: private::Token) -> Result<(), Error> {
        let stream_ctx = data.context();
        let target_key = stream_ctx.id();

        let mut sink_map = self.sinks.lock().await;

        let sender = sink_map.remove(target_key)
            .map_err(|_| PipelineError::DetachedStreamReceiver)
            .inspect_err(|_| stream_ctx.stop_generating())?;

        drop(sink_map);

        sender.send(data)
            .map_err(|_| PipelineError::DetachedStreamReceiver)
            .inspect_err(|_| stream_ctx.stop_generating())?;

        Ok(())
    }
}


// ── AsyncEngine<In, Out, Error> ───────────────────────────────────────────────

#[async_trait]
impl<In: PipelineIO + Sync, Out: PipelineIO> AsyncEngine<In, Out, Error> for Frontend<In, Out> {
    async fn generate(&self, input: In) -> Result<Out, Error> {
        let (sender, receiver) = oneshot::channel();

        {
            let mut sink_guard = self.sinks.lock().await;
            sink_guard.insert(input.id().to_string(), sender);
        }

        self.on_next(input, private::Token {}).await?;

        receiver
            .await
            .map_err(|_| PipelineError::DetachedStreamSender.into())
    }
}

