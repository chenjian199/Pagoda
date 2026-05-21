// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Network manager — factory for request-plane servers and clients.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::pipeline::error::PipelineError;
use crate::pipeline::network::ingress::unified_server::RequestPlaneServer;
use crate::pipeline::network::egress::unified_client::RequestPlaneClient;

/// Central manager for all network resources in the runtime.
///
/// Owns shared TCP listeners, connection pools, and codec configuration.
pub struct NetworkManager {
    /// Address this node listens on for the request plane.
    pub listen_addr: SocketAddr,
    /// Cancellation token for shutting down all network resources.
    pub cancel: CancellationToken,
}

impl NetworkManager {
    /// Create a new `NetworkManager` bound to the given address.
    pub fn new(listen_addr: SocketAddr, cancel: CancellationToken) -> Self {
        Self {
            listen_addr,
            cancel,
        }
    }

    /// Obtain a request-plane server (ingress) for the given path.
    pub async fn server(
        &self,
        path: &str,
    ) -> Result<Arc<dyn RequestPlaneServer>, PipelineError> {
        use crate::pipeline::network::tcp::server::{TcpStreamServer, SharedTcpServer};
        use crate::pipeline::network::ingress::shared_tcp_endpoint::SharedTcpEndpoint;
        let tcp_server = TcpStreamServer::bind(self.listen_addr, self.cancel.clone()).await?;
        let shared = Arc::new(SharedTcpServer::new(tcp_server));
        Ok(Arc::new(SharedTcpEndpoint::new(shared, path, self.cancel.clone())))
    }

    /// Obtain a request-plane client (egress) for a remote address.
    pub async fn client(
        &self,
        _target: SocketAddr,
    ) -> Result<Arc<dyn RequestPlaneClient>, PipelineError> {
        use crate::pipeline::network::tcp::client::ConnectionPool;
        use crate::pipeline::network::egress::tcp_client::TcpRequestClient;
        use std::time::Duration;
        let pool = Arc::new(ConnectionPool::new(4, Duration::from_secs(60), self.cancel.clone()));
        Ok(Arc::new(TcpRequestClient::new(pool, self.cancel.clone())))
    }

    /// Gracefully shut down all network resources.
    pub async fn shutdown(&self) -> Result<(), PipelineError> {
        self.cancel.cancel();
        Ok(())
    }
}
