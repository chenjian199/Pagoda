// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! PortName 服务端注册配置与传输地址构建。

use std::sync::Arc;

use crate::discovery::DiscoverySpec;
use crate::distributed::RequestPlaneMode;
use crate::pipeline::network::ingress::push_handler::PushWorkHandler;
use crate::pipeline::network::ingress::unified_server::PushWorkHandlerDyn;
use crate::servicegroup::{PortName, PortNameId, TransportType};
use crate::traits::DistributedRuntimeProvider;

/// PortName 服务端注册配置。
#[allow(dead_code)]
pub struct PortNameConfig {
    portname: PortName,
    /// 类型擦除的 handler，实际为 `Arc<dyn PushWorkHandlerDyn>`。
    handler: Arc<dyn PushWorkHandlerDyn>,
    metrics_labels: Option<Vec<(String, String)>>,
    graceful_shutdown: bool,
    health_check_payload: Option<serde_json::Value>,
}

/// PortName 注册配置 Builder。
pub struct PortNameConfigBuilder {
    portname: Option<PortName>,
    handler: Option<Arc<dyn PushWorkHandlerDyn>>,
    metrics_labels: Option<Vec<(String, String)>>,
    graceful_shutdown: bool,
    health_check_payload: Option<serde_json::Value>,
}

impl PortNameConfigBuilder {
    pub(crate) fn from_portname(portname: PortName) -> Self {
        Self {
            portname: Some(portname),
            handler: None,
            metrics_labels: None,
            graceful_shutdown: true,
            health_check_payload: None,
        }
    }

    /// 设置类型化 handler。
    ///
    /// `PushEndpoint<Req, Resp>` 已实现 `PushWorkHandlerDyn`，可直接传入。
    pub fn handler<Req, Resp>(
        mut self,
        handler: Arc<dyn PushWorkHandler<Req, Resp>>,
    ) -> Self
    where
        Req: crate::engine::Data + 'static,
        Resp: crate::engine::Data + 'static,
    {
        use crate::pipeline::network::ingress::push_endpoint::PushEndpoint;
        // 用 PushEndpoint 适配为 PushWorkHandlerDyn
        let portname = self.portname.as_ref()
            .expect("portname must be set before handler");
        let cancel = portname.drt().rt().portname_shutdown_token();
        let path = format!("{}", portname.id());
        let endpoint: Arc<dyn PushWorkHandlerDyn> =
            Arc::new(PushEndpoint::new(path, handler, cancel.clone()));
        self.handler = Some(endpoint);
        self
    }

    pub fn metrics_labels(mut self, labels: Vec<(String, String)>) -> Self {
        self.metrics_labels = Some(labels);
        self
    }

    pub fn graceful_shutdown(mut self, enabled: bool) -> Self {
        self.graceful_shutdown = enabled;
        self
    }

    pub fn health_check_payload(mut self, payload: serde_json::Value) -> Self {
        self.health_check_payload = Some(payload);
        self
    }

    /// 注册本地引擎（进程内直连优化）。
    ///
    /// 将引擎写入 `DistributedRuntime` 的 `LocalPortNameRegistry`，
    /// 使同进程内的调用方可以直接调用，完全绕过 TCP/NATS 网络栈。
    pub fn register_local_engine(
        self,
        engine: crate::local_portname_registry::LocalAsyncEngine,
    ) -> anyhow::Result<Self> {
        let portname = self.portname.as_ref()
            .ok_or_else(|| anyhow::anyhow!("portname must be set before register_local_engine"))?;
        let id = portname.id().clone();
        portname.drt().local_portname_registry().register(id, engine);
        Ok(self)
    }

    /// 启动 PortName 服务端注册。
    ///
    /// 注册顺序（CONSTRAINTS 3.4）：
    /// 1. 解构 builder，验证必要字段
    /// 2. 确定传输类型（build_transport_type）
    /// 3. 注册 health check target（在请求平面之前）
    /// 4. 向 RequestPlaneServer 注册 endpoint
    /// 5. 创建 cleanup task（在 Discovery 注册之前）
    /// 6. 向 Discovery 注册实例
    /// 7. 等待 port_shutdown_token 触发（cleanup task 负责清理）
    pub async fn start(self) -> anyhow::Result<()> {
        let portname = self.portname
            .ok_or_else(|| anyhow::anyhow!("portname not set"))?;
        let handler = self.handler
            .ok_or_else(|| anyhow::anyhow!("handler not set"))?;

        let drt = portname.drt().clone();
        let id = portname.id();
        let connection_id = drt.connection_id();
        let cancel = drt.rt().portname_shutdown_token().clone();
        let path = format!("{}", id);

        // 步骤 2：构建传输类型
        let transport = build_transport_type(&portname, &id, connection_id).await?;

        // 步骤 3：注册 health check target（必须先于请求平面注册）
        {
            drt.system_health().lock().register_endpoint(path.clone(), crate::HealthStatus::Starting);
            tracing::debug!(path=%path, "health check target registered");
        }

        // 步骤 4：向 RequestPlaneServer 注册 handler
        let server = drt.network_manager().server(&path).await?;
        server.register_endpoint(&path, handler)?;

        // 步骤 5：创建 cleanup task（在 Discovery 注册之前 spawn）
        let cleanup_drt = drt.clone();
        let cleanup_path = path.clone();
        let cleanup_cancel = cancel.clone();
        let cleanup_server = server.clone();
        let cleanup_task = tokio::spawn(async move {
            cleanup_cancel.cancelled().await;
            // Phase 1：注销请求平面（停止接受新请求）
            if let Err(e) = cleanup_server.unregister_endpoint(&cleanup_path) {
                tracing::warn!(path=%cleanup_path, "failed to unregister endpoint: {e}");
            }
            // Phase 2：从健康检查移除
            cleanup_drt.system_health().lock().unregister_endpoint(&cleanup_path);
            tracing::info!(path=%cleanup_path, "portname request plane unregistered");
        });

        // 步骤 6：向 Discovery 注册实例（在 cleanup task 就位后）
        let spec = DiscoverySpec::PortName {
            namespace: id.namespace.clone(),
            servicegroup: id.servicegroup.clone(),
            portname: id.portname.clone(),
            transport: transport.clone(),
        };
        let di = match drt.discovery().register(spec).await {
            Ok(di) => di,
            Err(e) => {
                // 注册失败时，触发 cancel 让 cleanup task 执行反注册
                cancel.cancel();
                let _ = cleanup_task.await;
                return Err(e);
            }
        };
        tracing::info!(path=%path, transport=?transport, "portname registered");

        // 步骤 7：等待 shutdown token
        cancel.cancelled().await;
        // cleanup task 已在 token 触发时自动执行
        let _ = cleanup_task.await;
        // 从 Discovery 注销
        if let Err(e) = drt.discovery().unregister(di).await {
            tracing::warn!(path=%path, "failed to unregister portname from discovery: {e}");
        }
        tracing::info!(path=%path, "portname unregistered from discovery");
        Ok(())
    }
}

/// 构建传输地址描述。
///
/// async 是因为 HTTP/TCP 动态端口模式需要 await server bind。
pub async fn build_transport_type(
    portname: &PortName,
    portname_id: &PortNameId,
    connection_id: u64,
) -> anyhow::Result<TransportType> {
    let drt = portname.drt();
    let addr = drt.network_manager().listen_addr;
    let path = format!("{}", portname_id);

    match drt.request_plane() {
        RequestPlaneMode::Tcp => {
            // TCP 格式: "{host}:{port}/{instance_id_hex}/{portname}"
            let transport_str = format!(
                "{}/{:016x}/{}",
                addr, connection_id, portname_id.portname
            );
            Ok(TransportType::Tcp(transport_str))
        }
        RequestPlaneMode::Http => {
            // HTTP 格式: "http://{host}:{port}/v1/rpc/{portname}"
            let transport_str = format!("http://{}/v1/rpc/{}", addr, path);
            Ok(TransportType::Http(transport_str))
        }
        RequestPlaneMode::Nats => {
            // NATS subject 格式: "{namespace}.{servicegroup}.{portname}.{instance_id}"
            let subject = format!(
                "{}.{}.{}.{}",
                portname_id.namespace,
                portname_id.servicegroup,
                portname_id.portname,
                connection_id,
            );
            Ok(TransportType::Nats(subject))
        }
    }
}
