use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use tokio::sync::{oneshot, Mutex};

use super::*;
use crate::pipeline::{AsyncEngine, PipelineIO};

mod base;
mod common;

pub struct Frontend<In: PipelineIO, Out: PipelineIO> {
    edge: OnceLock<Edge<In>>,
    sinks: Arc<Mutex<HashMap<String, oneshot::Sender<Out>>>>,
}

/// A [`ServiceFrontend`] is the interface for an [`AsyncEngine<SingleIn<Context<In>>, ManyOut<Annotated<Out>>, Error>`]
pub struct ServiceFrontend<In: PipelineIO, Out: PipelineIO> {
    inner: Frontend<In, Out>,
}

pub struct SegmentSource<In: PipelineIO, Out: PipelineIO> {
    inner: Frontend<In, Out>,
}