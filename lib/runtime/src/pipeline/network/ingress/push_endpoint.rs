// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Push endpoint — a generic inbound endpoint that delegates to a `PushWorkHandler`.

use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::engine::Data;
use crate::pipeline::error::PipelineError;

use super::push_handler::PushWorkHandler;
use super::unified_server::PushWorkHandlerDyn;

/// A concrete push endpoint that binds a typed handler to a logical path.
pub struct PushEndpoint<Req: Data, Resp: Data> {
    pub path: String,
    pub handler: Arc<dyn PushWorkHandler<Req, Resp>>,
    pub cancel: CancellationToken,
}

impl<Req: Data, Resp: Data> PushEndpoint<Req, Resp> {
    pub fn new(
        path: impl Into<String>,
        handler: Arc<dyn PushWorkHandler<Req, Resp>>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            path: path.into(),
            handler,
            cancel,
        }
    }

    /// Start processing on this endpoint.
    pub async fn serve(&self) -> Result<(), PipelineError> {
        self.cancel.cancelled().await;
        Ok(())
    }
}

#[async_trait]
impl<Req: Data, Resp: Data> PushWorkHandlerDyn for PushEndpoint<Req, Resp> {
    async fn handle(&self, request: bytes::Bytes) -> Result<bytes::Bytes, PipelineError> {
        self.handler.handle_raw(request).await
    }
}
