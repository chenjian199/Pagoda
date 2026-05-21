// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Push work handler trait — processes inbound push requests.

use async_trait::async_trait;
use bytes::Bytes;

use crate::engine::Data;
use crate::pipeline::error::PipelineError;

/// Handler for inbound "push" style requests (fire-and-process).
#[async_trait]
pub trait PushWorkHandler<Req: Data, Resp: Data>: Send + Sync + 'static {
    /// Decode and handle a raw request, returning a raw response.
    async fn handle_raw(&self, request: Bytes) -> Result<Bytes, PipelineError>;

    /// Handle a typed request, returning a typed response.
    async fn handle(&self, request: Req) -> Result<Resp, PipelineError>;
}
