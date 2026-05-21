// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Base source trait.

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::engine::Data;
use crate::pipeline::context::Context;
use crate::pipeline::error::PipelineError;

/// A source produces items that feed into a pipeline.
#[async_trait]
pub trait Source<T: Data>: Send + Sync + 'static {
    /// Start producing items. The source should send items until `cancel` fires.
    async fn run(
        &self,
        tx: tokio::sync::mpsc::Sender<Context<T>>,
        cancel: CancellationToken,
    ) -> Result<(), PipelineError>;

    /// Human-readable name for diagnostics.
    fn name(&self) -> &str;
}
