// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Service backend sink — forwards requests to a remote service endpoint.

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::engine::Data;
use crate::pipeline::context::Context;
use crate::pipeline::error::PipelineError;

use super::base::{Sink, SinkEdge};

/// A sink that forwards pipeline output to a remote service via the network egress layer.
pub struct ServiceBackend<T: Data> {
    pub name: String,
    pub target_path: String,
    pub edge: SinkEdge,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Data> ServiceBackend<T> {
    pub fn new(name: impl Into<String>, target_path: impl Into<String>) -> Self {
        let target_path = target_path.into();
        Self {
            name: name.into(),
            edge: SinkEdge::Network {
                target_path: target_path.clone(),
            },
            target_path,
            _marker: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T: Data> Sink<T> for ServiceBackend<T> {
    async fn run(
        &self,
        mut rx: mpsc::Receiver<Context<T>>,
        cancel: CancellationToken,
    ) -> Result<(), PipelineError> {
        // ServiceBackend drains the channel and discards items.
        // Actual forwarding to the network egress layer is done by the
        // egress client registered in the network manager.
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                item = rx.recv() => {
                    if item.is_none() { break; }
                    // TODO: forward via network egress
                }
            }
        }
        Ok(())
    }

    fn edge(&self) -> &SinkEdge {
        &self.edge
    }

    fn name(&self) -> &str {
        &self.name
    }
}
