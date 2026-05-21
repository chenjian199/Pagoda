// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Unified request-plane server trait.

use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::pipeline::error::PipelineError;

/// Trait unifying all inbound request-plane server implementations
/// (TCP, NATS, HTTP, etc.).
#[async_trait]
pub trait RequestPlaneServer: Send + Sync + 'static {
    /// Start serving incoming requests. Blocks until `cancel` fires.
    async fn serve(&self, cancel: CancellationToken) -> Result<(), PipelineError>;

    /// Register a handler for a logical endpoint path.
    fn register_endpoint(
        &self,
        path: &str,
        handler: Arc<dyn PushWorkHandlerDyn>,
    ) -> Result<(), PipelineError>;

    /// Unregister an endpoint.
    fn unregister_endpoint(&self, path: &str) -> Result<(), PipelineError>;

    /// The transport name (e.g. "tcp", "nats", "http").
    fn transport_name(&self) -> &str;
}

/// Type-erased push work handler for the unified server trait.
#[async_trait]
pub trait PushWorkHandlerDyn: Send + Sync + 'static {
    async fn handle(&self, request: bytes::Bytes) -> Result<bytes::Bytes, PipelineError>;
}
