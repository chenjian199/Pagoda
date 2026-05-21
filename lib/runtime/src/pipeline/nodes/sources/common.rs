// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Common source node implementations.

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::engine::Data;
use crate::pipeline::context::Context;
use crate::pipeline::error::PipelineError;

use super::base::Source;

/// Source that receives items from a downstream segment (intra-pipeline).
pub struct SegmentSource<T: Data> {
    pub name: String,
    pub rx: tokio::sync::Mutex<Option<mpsc::Receiver<Context<T>>>>,
}

impl<T: Data> SegmentSource<T> {
    pub fn new(name: impl Into<String>, rx: mpsc::Receiver<Context<T>>) -> Self {
        Self {
            name: name.into(),
            rx: tokio::sync::Mutex::new(Some(rx)),
        }
    }
}

#[async_trait]
impl<T: Data> Source<T> for SegmentSource<T> {
    async fn run(
        &self,
        tx: mpsc::Sender<Context<T>>,
        cancel: CancellationToken,
    ) -> Result<(), PipelineError> {
        let mut rx_guard = self.rx.lock().await;
        let mut rx = rx_guard.take().ok_or_else(|| PipelineError::Internal(anyhow::anyhow!("SegmentSource already consumed")))?;
        drop(rx_guard);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                item = rx.recv() => {
                    match item {
                        Some(ctx) => {
                            if tx.send(ctx).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    }
                }
            }
        }
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

pub struct NetworkSourceNode<T: Data> {
    pub name: String,
    pub path: String,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Data> NetworkSourceNode<T> {
    pub fn new(name: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            path: path.into(),
            _marker: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T: Data> Source<T> for NetworkSourceNode<T> {
    async fn run(
        &self,
        _tx: mpsc::Sender<Context<T>>,
        cancel: CancellationToken,
    ) -> Result<(), PipelineError> {
        // NetworkSourceNode delegates to the network ingress layer.
        // Wait for cancellation — actual frame forwarding is done by the
        // ingress server calling the registered handler.
        cancel.cancelled().await;
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// A frontend source — the outermost entry point for user requests.
pub struct Frontend<T: Data> {
    pub name: String,
    pub path: String,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Data> Frontend<T> {
    pub fn new(name: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            path: path.into(),
            _marker: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T: Data> Source<T> for Frontend<T> {
    async fn run(
        &self,
        _tx: mpsc::Sender<Context<T>>,
        cancel: CancellationToken,
    ) -> Result<(), PipelineError> {
        cancel.cancelled().await;
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// A service frontend — binds to an engine route and acts as a source.
pub struct ServiceFrontend<T: Data> {
    pub name: String,
    pub endpoint_path: String,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Data> ServiceFrontend<T> {
    pub fn new(name: impl Into<String>, endpoint_path: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            endpoint_path: endpoint_path.into(),
            _marker: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T: Data> Source<T> for ServiceFrontend<T> {
    async fn run(
        &self,
        _tx: mpsc::Sender<Context<T>>,
        cancel: CancellationToken,
    ) -> Result<(), PipelineError> {
        cancel.cancelled().await;
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}
