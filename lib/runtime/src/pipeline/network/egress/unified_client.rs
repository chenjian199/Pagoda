// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Unified request-plane client trait.

use std::net::SocketAddr;

use async_trait::async_trait;
use bytes::Bytes;

use crate::pipeline::error::PipelineError;

/// Trait unifying all outbound request-plane client implementations.
#[async_trait]
pub trait RequestPlaneClient: Send + Sync + 'static {
    /// Send a request to a target and await the response.
    async fn send_request(
        &self,
        target: SocketAddr,
        path: &str,
        payload: Bytes,
    ) -> Result<Bytes, PipelineError>;

    /// Send a request that expects a streaming response.
    async fn send_streaming_request(
        &self,
        target: SocketAddr,
        path: &str,
        payload: Bytes,
    ) -> Result<tokio::sync::mpsc::Receiver<Result<Bytes, PipelineError>>, PipelineError>;

    /// The transport name (e.g. "tcp", "nats", "http").
    fn transport_name(&self) -> &str;
}
