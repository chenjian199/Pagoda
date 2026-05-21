// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Pipeline context types — carry metadata and control signals through pipeline stages.

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::engine::{AsyncEngineContext, Data, DataStream};

/// Pipeline I/O envelope wrapping a typed payload with routing metadata.
#[derive(Debug, Clone)]
pub struct PipelineIO<T: Data> {
    /// The payload being transported.
    pub payload: T,
    /// Logical path of the originating endpoint.
    pub path: String,
    /// Unique request identifier.
    pub request_id: String,
}

impl<T: Data> PipelineIO<T> {
    pub fn new(payload: T, path: impl Into<String>, request_id: impl Into<String>) -> Self {
        Self {
            payload,
            path: path.into(),
            request_id: request_id.into(),
        }
    }
}

/// A context carrying a single request value through the pipeline.
pub struct Context<T: Data> {
    /// The wrapped value.
    pub value: T,
    /// Engine context for lifecycle control.
    pub engine_context: Arc<dyn AsyncEngineContext>,
    /// Cancellation token for cooperative shutdown.
    pub cancel: CancellationToken,
}

impl<T: Data> Context<T> {
    pub fn new(value: T, engine_context: Arc<dyn AsyncEngineContext>) -> Self {
        Self {
            value,
            engine_context,
            cancel: CancellationToken::new(),
        }
    }

    /// Map the inner value to a new type.
    pub fn map<U: Data>(self, f: impl FnOnce(T) -> U) -> Context<U> {
        Context {
            value: f(self.value),
            engine_context: self.engine_context,
            cancel: self.cancel,
        }
    }
}

/// A context carrying a stream of values through the pipeline.
pub struct StreamContext<T: Data> {
    /// The response stream.
    pub stream: DataStream<T>,
    /// Engine context for lifecycle control.
    pub engine_context: Arc<dyn AsyncEngineContext>,
    /// Cancellation token for cooperative shutdown.
    pub cancel: CancellationToken,
}

impl<T: Data> StreamContext<T> {
    pub fn new(stream: DataStream<T>, engine_context: Arc<dyn AsyncEngineContext>) -> Self {
        Self {
            stream,
            engine_context,
            cancel: CancellationToken::new(),
        }
    }
}

/// Controller for pipeline flow management.
pub struct Controller {
    cancel: CancellationToken,
    _tx: mpsc::Sender<ControlMessage>,
}

/// Internal control messages.
#[derive(Debug)]
enum ControlMessage {
    Stop,
    Kill,
}

impl Controller {
    pub fn new(cancel: CancellationToken, tx: mpsc::Sender<ControlMessage>) -> Self {
        Self { cancel, _tx: tx }
    }

    /// Stop the pipeline gracefully.
    pub fn stop(&self) {
        self.cancel.cancel();
    }

    /// Check if the pipeline is cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }
}

/// Trait for types that can be converted into a pipeline `Context`.
pub trait IntoContext<T: Data> {
    fn into_context(self, engine_context: Arc<dyn AsyncEngineContext>) -> Context<T>;
}

impl<T: Data> IntoContext<T> for T {
    fn into_context(self, engine_context: Arc<dyn AsyncEngineContext>) -> Context<T> {
        Context::new(self, engine_context)
    }
}
