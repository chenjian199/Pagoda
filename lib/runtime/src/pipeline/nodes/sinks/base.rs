// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Base sink trait.

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::engine::Data;
use crate::pipeline::context::Context;
use crate::pipeline::error::PipelineError;

/// Describes how a sink connects to the next stage.
#[derive(Debug, Clone)]
pub enum SinkEdge {
    /// Direct in-process channel to the next segment.
    Local { segment_name: String },
    /// Remote network connection.
    Network { target_path: String },
}

/// A sink consumes items from a pipeline stage.
#[async_trait]
pub trait Sink<T: Data>: Send + Sync + 'static {
    /// Start consuming items from `rx` until `cancel` fires.
    async fn run(
        &self,
        rx: tokio::sync::mpsc::Receiver<Context<T>>,
        cancel: CancellationToken,
    ) -> Result<(), PipelineError>;

    /// The edge description for this sink.
    fn edge(&self) -> &SinkEdge;

    /// Human-readable name for diagnostics.
    fn name(&self) -> &str;
}
