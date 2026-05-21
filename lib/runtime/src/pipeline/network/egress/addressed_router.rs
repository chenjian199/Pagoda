// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Addressed push router — sends requests to a pre-determined target address.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::engine::{AsyncEngineContext, Data, ResponseStream};
use crate::pipeline::error::PipelineError;

use super::unified_client::RequestPlaneClient;

/// A request pre-addressed to a specific target.
#[derive(Debug, Clone)]
pub struct AddressedRequest<T: Data> {
    /// The target worker address.
    pub target: SocketAddr,
    /// Logical endpoint path on the target.
    pub path: String,
    /// The request payload.
    pub payload: T,
}

impl<T: Data> AddressedRequest<T> {
    pub fn new(target: SocketAddr, path: impl Into<String>, payload: T) -> Self {
        Self {
            target,
            path: path.into(),
            payload,
        }
    }
}

/// Router that sends requests to their pre-specified target address.
pub struct AddressedPushRouter {
    pub client: Arc<dyn RequestPlaneClient>,
    pub cancel: CancellationToken,
}

impl AddressedPushRouter {
    pub fn new(client: Arc<dyn RequestPlaneClient>, cancel: CancellationToken) -> Self {
        Self { client, cancel }
    }

    /// Send a raw addressed request.
    pub async fn send(
        &self,
        target: SocketAddr,
        path: &str,
        payload: Bytes,
    ) -> Result<Bytes, PipelineError> {
        self.client.send_request(target, path, payload).await
    }

    /// Send and receive a streaming response.
    pub async fn send_streaming(
        &self,
        target: SocketAddr,
        path: &str,
        payload: Bytes,
    ) -> Result<tokio::sync::mpsc::Receiver<Result<Bytes, PipelineError>>, PipelineError> {
        self.client
            .send_streaming_request(target, path, payload)
            .await
    }

    /// Generate a response stream (AsyncEngine-compatible interface).
    pub async fn generate(
        &self,
        target: SocketAddr,
        path: &str,
        payload: Bytes,
        context: Arc<dyn AsyncEngineContext>,
    ) -> Result<ResponseStream<Bytes>, PipelineError> {
        let mut rx = self.send_streaming(target, path, payload).await?;
        let stream = async_stream::stream! {
            while let Some(item) = rx.recv().await {
                yield item.map_err(|e| crate::engine::EngineError::Internal(anyhow::anyhow!("{e}")));
            }
        };
        Ok(ResponseStream::new(Box::pin(stream), context))
    }
}
