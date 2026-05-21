// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! TCP request client — implements `RequestPlaneClient` over raw TCP.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::pipeline::error::PipelineError;
use crate::pipeline::network::tcp::client::ConnectionPool;

use super::unified_client::RequestPlaneClient;

/// A `RequestPlaneClient` implementation that communicates over TCP
/// using the `TwoPartCodec` framing protocol.
pub struct TcpRequestClient {
    pub pool: Arc<ConnectionPool>,
    pub cancel: CancellationToken,
}

impl TcpRequestClient {
    pub fn new(pool: Arc<ConnectionPool>, cancel: CancellationToken) -> Self {
        Self { pool, cancel }
    }
}

#[async_trait]
impl RequestPlaneClient for TcpRequestClient {
    async fn send_request(
        &self,
        target: SocketAddr,
        path: &str,
        payload: Bytes,
    ) -> Result<Bytes, PipelineError> {
        use crate::pipeline::network::codec::two_part::TwoPartFrame;
        let header = Bytes::copy_from_slice(path.as_bytes());
        let frame = TwoPartFrame { header, body: payload };
        let mut client = self.pool.get(target).await?;
        let resp = client.send(frame).await?;
        self.pool.release(client);
        Ok(resp.body)
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
        "tcp"
    }
}
