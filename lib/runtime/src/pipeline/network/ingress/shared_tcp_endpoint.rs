// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Shared TCP endpoint — registers on a `SharedTcpServer` for multiplexed TCP serving.

use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::pipeline::error::PipelineError;
use crate::pipeline::network::tcp::server::SharedTcpServer;

use super::unified_server::{PushWorkHandlerDyn, RequestPlaneServer};

/// An endpoint that registers itself on a shared TCP server.
pub struct SharedTcpEndpoint {
    pub server: Arc<SharedTcpServer>,
    pub path: String,
    pub cancel: CancellationToken,
}

impl SharedTcpEndpoint {
    pub fn new(
        server: Arc<SharedTcpServer>,
        path: impl Into<String>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            server,
            path: path.into(),
            cancel,
        }
    }

    /// Register a handler on the underlying shared server.
    pub fn register(
        &self,
        handler: Arc<dyn PushWorkHandlerDyn>,
    ) -> Result<(), PipelineError> {
        let boxed: crate::pipeline::network::tcp::server::BoxedFrameHandler = Arc::new(move |bytes| {
            let h = handler.clone();
            Box::pin(async move { h.handle(bytes).await })
        });
        self.server.register_endpoint(&self.path, boxed);
        Ok(())
    }

    /// Unregister from the shared server.
    pub fn unregister(&self) -> Result<(), PipelineError> {
        self.server.unregister_endpoint(&self.path);
        Ok(())
    }
}

#[async_trait]
impl RequestPlaneServer for SharedTcpEndpoint {
    async fn serve(&self, cancel: CancellationToken) -> Result<(), PipelineError> {
        // Serving is handled by SharedTcpServer; wait for cancellation.
        cancel.cancelled().await;
        Ok(())
    }

    fn register_endpoint(
        &self,
        path: &str,
        handler: Arc<dyn PushWorkHandlerDyn>,
    ) -> Result<(), PipelineError> {
        let boxed: crate::pipeline::network::tcp::server::BoxedFrameHandler = Arc::new(move |bytes| {
            let h = handler.clone();
            Box::pin(async move { h.handle(bytes).await })
        });
        self.server.register_endpoint(path, boxed);
        Ok(())
    }

    fn unregister_endpoint(&self, path: &str) -> Result<(), PipelineError> {
        self.server.unregister_endpoint(path);
        Ok(())
    }

    fn transport_name(&self) -> &str {
        "tcp"
    }
}
