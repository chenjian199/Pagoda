// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! HTTP request client — implements `RequestPlaneClient` over HTTP.

use std::net::SocketAddr;

use async_trait::async_trait;
use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::pipeline::error::PipelineError;

use super::unified_client::RequestPlaneClient;

/// A `RequestPlaneClient` implementation backed by HTTP (hyper client).
pub struct HttpRequestClient {
    pub cancel: CancellationToken,
}

impl HttpRequestClient {
    pub fn new(cancel: CancellationToken) -> Self {
        Self { cancel }
    }
}

#[async_trait]
impl RequestPlaneClient for HttpRequestClient {
    async fn send_request(
        &self,
        target: SocketAddr,
        path: &str,
        payload: Bytes,
    ) -> Result<Bytes, PipelineError> {
        use tokio::io::{AsyncWriteExt, AsyncReadExt};
        let mut stream = tokio::net::TcpStream::connect(target).await
            .map_err(|e| PipelineError::transport(format!("HTTP connect {target}: {e}")))? ;
        let req = format!(
            "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nContent-Type: application/octet-stream\r\n\r\n",
            path, target, payload.len()
        );
        stream.write_all(req.as_bytes()).await
            .map_err(|e| PipelineError::transport(format!("HTTP send headers: {e}")))?;
        stream.write_all(&payload).await
            .map_err(|e| PipelineError::transport(format!("HTTP send body: {e}")))?;
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await
            .map_err(|e| PipelineError::transport(format!("HTTP read response: {e}")))?;
        // Skip HTTP headers, return body
        if let Some(pos) = resp.windows(4).position(|w| w == b"\r\n\r\n") {
            Ok(Bytes::copy_from_slice(&resp[pos + 4..]))
        } else {
            Ok(Bytes::from(resp))
        }
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
        "http"
    }
}
