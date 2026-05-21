// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! NATS request client — implements `RequestPlaneClient` over NATS.

use std::net::SocketAddr;

use async_trait::async_trait;
use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::pipeline::error::PipelineError;

use super::unified_client::RequestPlaneClient;

/// A `RequestPlaneClient` implementation backed by NATS request-reply.
pub struct NatsRequestClient {
    pub nats_url: String,
    pub cancel: CancellationToken,
}

impl NatsRequestClient {
    pub fn new(nats_url: impl Into<String>, cancel: CancellationToken) -> Self {
        Self {
            nats_url: nats_url.into(),
            cancel,
        }
    }
}

#[async_trait]
impl RequestPlaneClient for NatsRequestClient {
    async fn send_request(
        &self,
        _target: SocketAddr,
        path: &str,
        payload: Bytes,
    ) -> Result<Bytes, PipelineError> {
        use crate::transports::nats::Client as NatsClient;
        let client = NatsClient::from_env().await
            .map_err(|e| PipelineError::transport(format!("NATS connect: {e}")))? ;
        let resp = client.request(path, &payload, std::time::Duration::from_secs(30)).await
            .map_err(|e| PipelineError::transport(format!("NATS request: {e}")))? ;
        Ok(Bytes::from(resp.payload))
    }

    async fn send_streaming_request(
        &self,
        target: SocketAddr,
        path: &str,
        payload: Bytes,
    ) -> Result<tokio::sync::mpsc::Receiver<Result<Bytes, PipelineError>>, PipelineError> {
        let body = self.send_request(target, path, payload).await?;
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let _ = tx.send(Ok(body)).await;
        Ok(rx)
    }

    fn transport_name(&self) -> &str {
        "nats"
    }
}
