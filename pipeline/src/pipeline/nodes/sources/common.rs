//! `impl_frontend!` macro — uniform `new()`, `Source`, `Sink`, `AsyncEngine`
//! for [`ServiceFrontend`] and [`SegmentSource`].
//!
//! Both structs are thin wrappers around [`Frontend`] with a single field:
//!
//! ```text
//! inner: Frontend<In, Out>
//! ```
//!
//! They share identical runtime behaviour; only the type name (and thus the
//! architectural role it conveys) differs.  The macro below generates all four
//! required impls by delegating directly to `self.inner`.

use std::sync::Arc;

use anyhow::Error;
use async_trait::async_trait;

use crate::engine::AsyncEngineContextProvider;
use crate::pipeline::error::PipelineError;
use crate::pipeline::nodes::node::{private, AsyncEngine, Edge, PipelineIO, Sink, Source};

// ── impl_frontend! ────────────────────────────────────────────────────────────

/// Generate the standard four impls for any `$T<In, Out>` that wraps a
/// `Frontend<In, Out>` in a field named `inner`.
macro_rules! impl_frontend {
    ($T:ident) => {
        // ── Constructor ───────────────────────────────────────────────────────

        impl<In: PipelineIO, Out: PipelineIO> super::$T<In, Out> {
            pub fn new() -> Arc<super::$T<In, Out>> {
                Arc::new(super::$T {
                    inner: super::Frontend::default(),
                })
            }
        }

        // ── Source<In> ────────────────────────────────────────────────────────

        #[async_trait]
        impl<In: PipelineIO, Out: PipelineIO> Source<In> for super::$T<In, Out> {
            async fn on_next(&self, data: In, t: private::Token) -> Result<(), Error> {
                self.inner.on_next(data, t).await
            }

            fn set_edge(&self, edge: Edge<In>, t: private::Token) -> Result<(), PipelineError> {
                self.inner.set_edge(edge, t)
            }
        }

        // ── Sink<Out> ─────────────────────────────────────────────────────────

        #[async_trait]
        impl<In: PipelineIO, Out: PipelineIO + AsyncEngineContextProvider> Sink<Out>
            for super::$T<In, Out>
        {
            async fn on_data(&self, data: Out, t: private::Token) -> Result<(), Error> {
                self.inner.on_data(data, t).await
            }
        }

        // ── AsyncEngine<In, Out, Error> ───────────────────────────────────────

        #[async_trait]
        impl<In: PipelineIO + Sync, Out: PipelineIO> AsyncEngine<In, Out, Error>
            for super::$T<In, Out>
        {
            async fn generate(&self, input: In) -> Result<Out, Error> {
                self.inner.generate(input).await
            }
        }
    };
}

// ── Apply to both wrapper types ───────────────────────────────────────────────

impl_frontend!(ServiceFrontend);
impl_frontend!(SegmentSource);
