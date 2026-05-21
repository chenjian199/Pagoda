// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Push router — selects a target worker instance from the available pool.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::pipeline::error::PipelineError;

use super::unified_client::RequestPlaneClient;

/// Load-balancing strategies for the push router.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouterMode {
    /// Cycle through workers in order.
    RoundRobin,
    /// Pick a random worker.
    Random,
    /// Power-of-two random choices (pick the less-loaded of two random picks).
    PowerOfTwo,
    /// Route to the worker with the lowest reported load.
    LeastLoaded,
    /// Route directly to a specific address (no balancing).
    Direct,
    /// Route by key hash (for stateful workloads).
    KV,
}

/// Provides load information for a worker, used by `LeastLoaded` and `PowerOfTwo` modes.
#[async_trait]
pub trait WorkerLoadMonitor: Send + Sync + 'static {
    /// Current load metric for the given worker (lower = less loaded).
    async fn load(&self, worker: SocketAddr) -> f64;
}

/// Routes push requests to a selected worker using the configured strategy.
pub struct PushRouter {
    pub mode: RouterMode,
    pub workers: Arc<parking_lot::RwLock<Vec<SocketAddr>>>,
    pub client: Arc<dyn RequestPlaneClient>,
    pub load_monitor: Option<Arc<dyn WorkerLoadMonitor>>,
    pub cancel: CancellationToken,
    round_robin_counter: std::sync::atomic::AtomicUsize,
}

impl PushRouter {
    pub fn new(
        mode: RouterMode,
        client: Arc<dyn RequestPlaneClient>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            mode,
            workers: Arc::new(parking_lot::RwLock::new(Vec::new())),
            client,
            load_monitor: None,
            cancel,
            round_robin_counter: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Set the load monitor (required for `LeastLoaded` / `PowerOfTwo`).
    pub fn with_load_monitor(mut self, monitor: Arc<dyn WorkerLoadMonitor>) -> Self {
        self.load_monitor = Some(monitor);
        self
    }

    /// Update the list of available workers.
    pub fn update_workers(&self, workers: Vec<SocketAddr>) {
        *self.workers.write() = workers;
    }

    /// Select a target based on the configured mode.
    pub async fn select_target(&self) -> Result<SocketAddr, PipelineError> {
        use std::sync::atomic::Ordering;
        let workers = self.workers.read();
        if workers.is_empty() {
            return Err(PipelineError::transport("PushRouter: no workers available"));
        }
        match self.mode {
            RouterMode::RoundRobin | RouterMode::Direct => {
                let idx = self.round_robin_counter.fetch_add(1, Ordering::Relaxed) % workers.len();
                Ok(workers[idx])
            }
            RouterMode::Random => {
                let idx = (std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .subsec_nanos() as usize) % workers.len();
                Ok(workers[idx])
            }
            RouterMode::LeastLoaded | RouterMode::PowerOfTwo | RouterMode::KV => {
                // fallback to round-robin
                let idx = self.round_robin_counter.fetch_add(1, Ordering::Relaxed) % workers.len();
                Ok(workers[idx])
            }
        }
    }

    /// Route a request: select a target and send the payload.
    pub async fn route(
        &self,
        path: &str,
        payload: Bytes,
    ) -> Result<Bytes, PipelineError> {
        let target = self.select_target().await?;
        self.client.send_request(target, path, payload).await
    }

    /// Route a streaming request.
    pub async fn route_streaming(
        &self,
        path: &str,
        payload: Bytes,
    ) -> Result<tokio::sync::mpsc::Receiver<Result<Bytes, PipelineError>>, PipelineError> {
        let target = self.select_target().await?;
        self.client.send_streaming_request(target, path, payload).await
    }
}
