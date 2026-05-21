// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Segment sink — forwards pipeline output to the next local segment.

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::engine::Data;
use crate::pipeline::context::Context;
use crate::pipeline::error::PipelineError;

use super::base::{Sink, SinkEdge};

/// A sink that forwards items to the next in-process pipeline segment via a channel.
pub struct SegmentSink<T: Data> {
    pub name: String,
    pub edge: SinkEdge,
    pub tx: mpsc::Sender<Context<T>>,
}

impl<T: Data> SegmentSink<T> {
    pub fn new(name: impl Into<String>, segment_name: impl Into<String>, tx: mpsc::Sender<Context<T>>) -> Self {
        let segment_name = segment_name.into();
        Self {
            name: name.into(),
            edge: SinkEdge::Local {
                segment_name,
            },
            tx,
        }
    }
}

#[async_trait]
impl<T: Data> Sink<T> for SegmentSink<T> {
    async fn run(
        &self,
        mut rx: mpsc::Receiver<Context<T>>,
        cancel: CancellationToken,
    ) -> Result<(), PipelineError> {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                item = rx.recv() => {
                    match item {
                        Some(ctx) => {
                            if self.tx.send(ctx).await.is_err() {
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

    fn edge(&self) -> &SinkEdge {
        &self.edge
    }

    fn name(&self) -> &str {
        &self.name
    }
}
