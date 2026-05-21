// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Test utilities for the TCP transport layer.

use std::net::SocketAddr;

use tokio_util::sync::CancellationToken;

use super::server::TcpStreamServer;
use super::client::TcpClient;

/// Spawn a local TCP echo server for testing. Returns the bound address.
pub async fn spawn_echo_server(cancel: CancellationToken) -> SocketAddr {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let mut server = TcpStreamServer::bind(addr, cancel.clone()).await
        .expect("echo server bind failed");
    let bound = server.local_addr;
    tokio::spawn(async move {
        let _ = server.serve().await;
    });
    bound
}

/// Create a connected client-server pair on localhost for unit tests.
pub async fn connected_pair(cancel: CancellationToken) -> (TcpClient, TcpStreamServer) {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = TcpStreamServer::bind(addr, cancel.clone()).await
        .expect("server bind failed");
    let bound = server.local_addr;
    let client = TcpClient::connect(bound, cancel.clone()).await
        .expect("client connect failed");
    (client, server)
}

/// A mock frame handler that records all received frames.
#[derive(Default, Clone)]
pub struct RecordingHandler {
    pub frames: std::sync::Arc<tokio::sync::Mutex<Vec<bytes::Bytes>>>,
}

impl RecordingHandler {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn received(&self) -> Vec<bytes::Bytes> {
        self.frames.lock().await.clone()
    }
}
