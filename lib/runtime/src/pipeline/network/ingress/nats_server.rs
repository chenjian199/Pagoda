// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! NATS-based request-plane server.

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use tokio_util::sync::CancellationToken;

use crate::pipeline::error::PipelineError;

use super::unified_server::{PushWorkHandlerDyn, RequestPlaneServer};

/// A NATS-backed request-plane server that subscribes to subjects for each endpoint.
pub struct NatsServer {
    /// NATS client connection.
    pub nats_url: String,
    /// Registered handlers by path/subject.
    pub handlers: DashMap<String, Arc<dyn PushWorkHandlerDyn>>,
    pub cancel: CancellationToken,
}

impl NatsServer {
    pub fn new(nats_url: impl Into<String>, cancel: CancellationToken) -> Self {
        Self {
            nats_url: nats_url.into(),
            handlers: DashMap::new(),
            cancel,
        }
    }
}

#[async_trait]
impl RequestPlaneServer for NatsServer {
    async fn serve(&self, cancel: CancellationToken) -> Result<(), PipelineError> {
        // NatsServer subscribe loop: connect and subscribe all registered handlers
        use crate::transports::nats::{Client as NatsClient, ClientOptions};
        let opts = ClientOptions { url: self.nats_url.clone(), ..Default::default() };
        let client = NatsClient::connect(opts).await
            .map_err(|e| PipelineError::transport(format!("NATS server connect: {e}")))? ;
        let mut tasks = tokio::task::JoinSet::new();
        for entry in self.handlers.iter() {
            let subject = entry.key().clone();
            let handler = entry.value().clone();
            let sub_client = client.clone();
            let sub_cancel = cancel.clone();
            tasks.spawn(async move {
                let mut sub = sub_client.subscribe(&subject).await
                    .map_err(|e| PipelineError::transport(format!("NATS subscribe {subject}: {e}")))? ;
                loop {
                    tokio::select! {
                        _ = sub_cancel.cancelled() => break,
                        msg = sub.next_message() => {
                            match msg {
                                Some(m) => {
                                    let _ = handler.handle(bytes::Bytes::from(m.payload)).await;
                                }
                                None => break,
                            }
                        }
                    }
                }
                Ok::<_, PipelineError>(())
            });
        }
        cancel.cancelled().await;
        tasks.abort_all();
        Ok(())
    }

    fn register_endpoint(
        &self,
        path: &str,
        handler: Arc<dyn PushWorkHandlerDyn>,
    ) -> Result<(), PipelineError> {
        self.handlers.insert(path.to_string(), handler);
        Ok(())
    }

    fn unregister_endpoint(&self, path: &str) -> Result<(), PipelineError> {
        self.handlers.remove(path);
        Ok(())
    }

    fn transport_name(&self) -> &str {
        "nats"
    }
}
