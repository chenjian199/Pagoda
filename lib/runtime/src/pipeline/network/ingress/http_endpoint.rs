// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! HTTP endpoint — serves requests over HTTP/1.1 or HTTP/2 via axum.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use tokio_util::sync::CancellationToken;

use crate::pipeline::error::PipelineError;

use super::unified_server::{PushWorkHandlerDyn, RequestPlaneServer};

/// An HTTP-based request-plane server using axum.
pub struct HttpEndpoint {
    pub listen_addr: SocketAddr,
    pub handlers: DashMap<String, Arc<dyn PushWorkHandlerDyn>>,
    pub cancel: CancellationToken,
}

impl HttpEndpoint {
    pub fn new(listen_addr: SocketAddr, cancel: CancellationToken) -> Self {
        Self {
            listen_addr,
            handlers: DashMap::new(),
            cancel,
        }
    }

    /// Build the axum router from registered handlers.
    fn build_router(&self) -> axum::Router {
        use axum::{response::IntoResponse, routing::post};
        let mut router = axum::Router::new();
        for entry in self.handlers.iter() {
            let path = entry.key().clone();
            let handler = entry.value().clone();
            let route_path = if path.starts_with('/') { path.clone() } else { format!("/{path}") };
            router = router.route(
                &route_path,
                post(move |body: bytes::Bytes| {
                    let h = handler.clone();
                    async move {
                        match h.handle(body).await {
                            Ok(resp) => (axum::http::StatusCode::OK, resp).into_response(),
                            Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                                       format!("{e}")).into_response(),
                        }
                    }
                }),
            );
        }
        router
    }
}

#[async_trait]
impl RequestPlaneServer for HttpEndpoint {
    async fn serve(&self, cancel: CancellationToken) -> Result<(), PipelineError> {
        use axum::serve;
        let router = self.build_router();
        let listener = tokio::net::TcpListener::bind(self.listen_addr).await
            .map_err(|e| PipelineError::transport(format!("HTTP bind {}: {e}", self.listen_addr)))? ;
        serve(listener, router)
            .with_graceful_shutdown(async move { cancel.cancelled().await })
            .await
            .map_err(|e| PipelineError::transport(format!("HTTP serve: {e}")))
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
        "http"
    }
}
