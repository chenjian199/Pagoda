// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! TCP client with connection pooling.

use std::net::SocketAddr;
use std::time::Duration;

use dashmap::DashMap;
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

use crate::pipeline::error::PipelineError;
use crate::pipeline::network::codec::two_part::TwoPartFrame;

/// A single TCP client connection wrapper.
pub struct TcpClient {
    pub target: SocketAddr,
    pub stream: Option<TcpStream>,
    pub cancel: CancellationToken,
}

impl TcpClient {
    /// Connect to the target address.
    pub async fn connect(target: SocketAddr, cancel: CancellationToken) -> Result<Self, PipelineError> {
        let stream = tokio::time::timeout(
            Duration::from_secs(10),
            TcpStream::connect(target),
        ).await
            .map_err(|_| PipelineError::transport(format!("TCP connect timeout to {target}")))
            .and_then(|r| r.map_err(|e| PipelineError::transport(format!("TCP connect to {target}: {e}"))))?;
        Ok(Self { target, stream: Some(stream), cancel })
    }

    /// Send a frame and await the response.
    pub async fn send(&mut self, frame: TwoPartFrame) -> Result<TwoPartFrame, PipelineError> {
        use crate::pipeline::network::codec::two_part::TwoPartCodec;
        let stream = self.stream.as_mut()
            .ok_or_else(|| PipelineError::transport("TcpClient: stream not connected"))?;
        let (reader, writer) = stream.split();
        let mut framed_w = tokio_util::codec::FramedWrite::new(writer, TwoPartCodec::default());
        let mut framed_r = tokio_util::codec::FramedRead::new(reader, TwoPartCodec::default());
        use futures::SinkExt;
        framed_w.send(frame).await
            .map_err(|e| PipelineError::transport(format!("TCP send: {e}")))?;
        framed_w.flush().await
            .map_err(|e| PipelineError::transport(format!("TCP flush: {e}")))?;
        use futures::StreamExt;
        let resp = framed_r.next().await
            .ok_or_else(|| PipelineError::transport("TCP: connection closed before response"))?
            .map_err(|e| PipelineError::transport(format!("TCP recv: {e}")))?;
        Ok(resp)
    }

    /// Close the connection gracefully.
    pub async fn close(mut self) -> Result<(), PipelineError> {
        if let Some(mut stream) = self.stream.take() {
            use tokio::io::AsyncWriteExt;
            let _ = stream.shutdown().await;
        }
        Ok(())
    }
}

/// A pool of TCP connections to multiple remote addresses.
pub struct ConnectionPool {
    /// Maximum connections per target.
    pub max_connections_per_target: usize,
    /// Idle timeout before a connection is evicted.
    pub idle_timeout: Duration,
    /// Active connections keyed by target address.
    connections: DashMap<SocketAddr, Vec<TcpClient>>,
    pub cancel: CancellationToken,
}

impl ConnectionPool {
    pub fn new(
        max_connections_per_target: usize,
        idle_timeout: Duration,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            max_connections_per_target,
            idle_timeout,
            connections: DashMap::new(),
            cancel,
        }
    }

    /// Get or create a connection to the given target.
    pub async fn get(&self, target: SocketAddr) -> Result<TcpClient, PipelineError> {
        // Try to pop an existing connection from the pool
        if let Some(mut entry) = self.connections.get_mut(&target) {
            if let Some(client) = entry.value_mut().pop() {
                return Ok(client);
            }
        }
        // Create a new connection
        TcpClient::connect(target, self.cancel.clone()).await
    }

    /// Return a connection to the pool.
    pub fn release(&self, client: TcpClient) {
        let mut entry = self.connections.entry(client.target).or_insert_with(Vec::new);
        if entry.len() < self.max_connections_per_target {
            entry.push(client);
        }
        // else: drop the connection
    }

    /// Evict all idle connections.
    pub fn evict_idle(&self) {
        // Clear all pooled connections (idle eviction policy: drop all)
        self.connections.clear();
    }
}
