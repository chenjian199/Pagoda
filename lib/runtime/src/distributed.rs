// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 分布式运行时：在 `Runtime` 之上叠加集群感知能力的单一入口。
//!
//! 聚合 Discovery / NetworkManager / NATS / SystemHealth / Metrics 等所有
//! 分布式基础设施，通过 `.namespace()` 入口对外暴露服务模型 API。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};

use tokio::sync::{watch, Mutex};

use crate::discovery;
use crate::discovery::KubeDiscoveryClient;
use crate::engine_routes::EngineRouteRegistry;
use crate::local_portname_registry::LocalPortNameRegistry;
use crate::metrics::MetricsRegistry;
use crate::pipeline::network::manager::NetworkManager;
use crate::runtime::Runtime;
use crate::servicegroup::{self, Instance, Namespace, PortName};
use crate::system_health::{HealthStatus, SystemHealth};
use crate::transports;

// ── 类型别名 ──────────────────────────────────────────────────────

/// 实例列表 Watch 的进程内共享表（Weak 避免阻止回收）。
type InstanceMap = HashMap<PortName, std::sync::Weak<watch::Receiver<Vec<Instance>>>>;

/// 路由占用状态的进程内共享表（Weak 避免阻止回收）。
type RoutingOccupancyMap =
    HashMap<PortName, std::sync::Weak<servicegroup::client::RoutingOccupancyState>>;

/// `0.0.0.0:0` 占位地址，NetworkManager 懒绑定时使用。
const DEFAULT_LISTEN_ADDR: SocketAddr =
    SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 0);

// ── 配置类型 ──────────────────────────────────────────────────────

/// 请求平面模式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestPlaneMode {
    /// TCP（默认，低延迟）
    Tcp,
    /// HTTP（标准化，适合多语言互操作）
    Http,
    /// NATS（deprecated）
    Nats,
}

impl Default for RequestPlaneMode {
    fn default() -> Self {
        Self::Tcp
    }
}

impl RequestPlaneMode {
    fn from_env() -> Self {
        match std::env::var("PGD_REQUEST_PLANE")
            .unwrap_or_default()
            .to_lowercase()
            .as_str()
        {
            "http" => Self::Http,
            "nats" => Self::Nats,
            _ => Self::Tcp,
        }
    }
}

/// 服务发现后端选择。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiscoveryBackend {
    /// Kubernetes 原生 CRD（生产环境）
    Kubernetes,
    /// etcd（`"etcd://host:port"`）/ file（`"file:///path"`）/ mem（`"mem://"`）
    KvStore(String),
}

impl Default for DiscoveryBackend {
    fn default() -> Self {
        Self::Kubernetes
    }
}

impl DiscoveryBackend {
    fn from_env() -> anyhow::Result<Self> {
        let val = std::env::var("PGD_DISCOVERY_BACKEND")
            .unwrap_or_else(|_| "kubernetes".to_string())
            .to_lowercase();
        match val.as_str() {
            "kubernetes" | "k8s" => Ok(Self::Kubernetes),
            s if s.starts_with("etcd://") || s.starts_with("file://") || s.starts_with("mem://") => {
                Ok(Self::KvStore(val))
            }
            other => anyhow::bail!(
                "invalid PGD_DISCOVERY_BACKEND={other:?}, valid: kubernetes, etcd://<host>, file://<path>, mem://"
            ),
        }
    }
}

/// 分布式运行时配置。
#[derive(Debug, Clone)]
pub struct DistributedConfig {
    pub discovery_backend: DiscoveryBackend,
    pub nats_config: Option<transports::nats::ClientOptions>,
    pub request_plane: RequestPlaneMode,
}

impl DistributedConfig {
    /// 从环境变量读取配置。
    ///
    /// - `PGD_DISCOVERY_BACKEND`：发现后端（默认 `kubernetes`）
    /// - `PGD_REQUEST_PLANE`：请求平面（默认 `tcp`）
    /// - `PGD_NATS_SERVER`：NATS 地址（若设置则启用 NATS，与请求平面选择无关）
    pub fn from_settings() -> Self {
        let discovery_backend = DiscoveryBackend::from_env().unwrap_or_else(|e| {
            tracing::warn!("{e}, using default Kubernetes discovery");
            DiscoveryBackend::Kubernetes
        });
        let request_plane = RequestPlaneMode::from_env();
        let nats_config = std::env::var("PGD_NATS_SERVER").ok().map(|url| {
            transports::nats::ClientOptions {
                url,
                ..Default::default()
            }
        });
        Self {
            discovery_backend,
            nats_config,
            request_plane,
        }
    }
}

// ── DistributedRuntime ────────────────────────────────────────────

/// Pagoda 分布式能力的单一入口。
///
/// Clone 只增加 Arc 引用计数，进程内可自由共享。
#[derive(Clone)]
pub struct DistributedRuntime {
    runtime: Runtime,
    nats_client: Option<transports::nats::Client>,
    network_manager: Arc<NetworkManager>,
    /// TCP 服务器懒初始化容器（Worker 角色才真正创建）。
    tcp_server: Arc<OnceLock<Arc<crate::transports::tcp::server::TcpStreamServer>>>,
    /// HTTP 系统状态服务器信息（`PGD_SYSTEM_PORT >= 0` 时写入）。
    system_status_server: Arc<OnceLock<Arc<crate::system_status_server::SystemStatusServerInfo>>>,
    request_plane: RequestPlaneMode,
    discovery_client: Arc<dyn discovery::Discovery>,
    discovery_metadata: Option<Arc<tokio::sync::RwLock<discovery::DiscoveryMetadata>>>,
    servicegroup_registry: servicegroup::Registry,
    instance_sources: Arc<Mutex<InstanceMap>>,
    routing_occupancy_states: Arc<Mutex<RoutingOccupancyMap>>,
    /// 全局健康状态（外层 parking_lot::Mutex 供低频系统级写入，内部 RwLock 供高频端点级并发）。
    system_health: Arc<parking_lot::Mutex<SystemHealth>>,
    local_portname_registry: LocalPortNameRegistry,
    metrics_registry: MetricsRegistry,
    engine_routes: EngineRouteRegistry,
}

impl DistributedRuntime {
    // ── 初始化 ──────────────────────────────────────────────────────

    /// 从环境变量配置初始化 DistributedRuntime（生产入口）。
    pub async fn from_settings(runtime: Runtime) -> anyhow::Result<Self> {
        let config = DistributedConfig::from_settings();
        Self::new(runtime, config).await
    }

    /// 完整初始化（顺序不可颠倒）：
    ///
    /// 1. 连接 NATS（如配置）
    /// 2. 初始化 Discovery 后端
    /// 3. 构造 NetworkManager
    /// 4. 组装结构体
    /// 5. 启动 SystemStatusServer（`PGD_SYSTEM_PORT >= 0` 时）
    /// 6. 启动 HealthCheckManager（`health_check_enabled` 时）
    pub async fn new(runtime: Runtime, config: DistributedConfig) -> anyhow::Result<Self> {
        let DistributedConfig {
            discovery_backend,
            nats_config,
            request_plane,
        } = config;

        // 步骤 1：建立 NATS 连接（仅当 PGD_NATS_SERVER 配置时）
        let nats_client = match nats_config {
            Some(opts) => {
                match transports::nats::Client::connect(opts).await {
                    Ok(c) => Some(c),
                    Err(e) => {
                        anyhow::bail!("Failed to connect to NATS: {e}. Check PGD_NATS_SERVER.");
                    }
                }
            }
            None => None,
        };

        // 步骤 2：读取 RuntimeConfig（system_port 等）
        let rt_config = crate::config::RuntimeConfig::from_settings().unwrap_or_default();

        // IMPORTANT: 在 runtime 被 move 进结构体之前提取 cancel_token
        let system_server_cancel = if rt_config.system_port >= 0 {
            Some(runtime.child_token())
        } else {
            None
        };

        // 步骤 3：构建健康状态
        let system_health = Arc::new(parking_lot::Mutex::new(SystemHealth::new(
            HealthStatus::Starting,
            Vec::new(),
            "/health".to_string(),
            "/live".to_string(),
        )));

        // 步骤 4：初始化 Discovery 后端
        let (discovery_client, discovery_metadata): (
            Arc<dyn discovery::Discovery>,
            Option<Arc<tokio::sync::RwLock<discovery::DiscoveryMetadata>>>,
        ) = match discovery_backend {
            DiscoveryBackend::Kubernetes => {
                // new() 内部自动从 Downward API 读取 Pod 身份，返回 Arc<Self>
                let arc_client = KubeDiscoveryClient::new()
                    .await
                    .map_err(|e| {
                        tracing::error!(%e, "Failed to initialize Kubernetes discovery client");
                        e
                    })?;
                // KubeDiscoveryClient: Clone + Discovery; 从 Arc 中 clone 出裸值
                let client: KubeDiscoveryClient = (*arc_client).clone();
                let discovery: Arc<dyn discovery::Discovery> = Arc::new(client);
                // 暂不对外暴露 metadata，K8s 元数据由内部 DiscoveryDaemon 管理
                (discovery, None)
            }
            DiscoveryBackend::KvStore(selector) => {
                // KvStore 后端（etcd/file/mem）尚未实现，暂用 MockDiscovery 兜底
                tracing::warn!(
                    "KvStore backend ({selector:?}) not yet implemented; \
                     falling back to MockDiscovery. Pass PGD_DISCOVERY_BACKEND=kubernetes for production."
                );
                let client = discovery::MockDiscovery::new(
                    None,
                    discovery::mock::SharedMockRegistry::new(),
                );
                (Arc::new(client) as Arc<dyn discovery::Discovery>, None)
            }
        };

        // 步骤 5：构造 NetworkManager
        let servicegroup_registry = servicegroup::Registry::new();
        let network_manager = NetworkManager::new(
            DEFAULT_LISTEN_ADDR,
            runtime.child_token(),
        );

        // 步骤 6：组装结构体
        let drt = Self {
            runtime,
            nats_client,
            network_manager: Arc::new(network_manager),
            tcp_server: Arc::new(OnceLock::new()),
            system_status_server: Arc::new(OnceLock::new()),
            request_plane,
            discovery_client,
            discovery_metadata,
            servicegroup_registry,
            instance_sources: Arc::new(Mutex::new(HashMap::new())),
            routing_occupancy_states: Arc::new(Mutex::new(HashMap::new())),
            system_health,
            local_portname_registry: LocalPortNameRegistry::new(),
            metrics_registry: MetricsRegistry::new("drt"),
            engine_routes: EngineRouteRegistry::new(),
        };

        // 步骤 7：初始化 uptime Prometheus gauge
        drt.system_health.lock().initialize_uptime_gauge(&drt.metrics_registry);

        // 步骤 7b：注册 Prometheus 抓取更新回调（每次 /metrics 请求时刷新 uptime gauge）
        {
            let system_health = drt.system_health.clone();
            drt.metrics_registry.add_update_callback(Arc::new(move || {
                system_health.lock().update_uptime_gauge();
                Ok(())
            }));
        }

        // 步骤 7c：启动 SystemStatusServer（条件启动，失败不中断初始化）
        if let Some(cancel) = system_server_cancel {
            let info = crate::system_status_server::start_system_status_server(
                drt.system_health.clone(),
                Arc::new(drt.metrics_registry.clone()),
                cancel,
            )
            .await;
            if let Some(info) = info {
                tracing::info!(bound_addr = %info.bound_addr, "System status server started");
                let _ = drt.system_status_server.set(Arc::new(info));
            }
        }

        // 步骤 8：启动 HealthCheckManager（有 health-check target 时按需启动）
        // HealthCheckManager 启动后，如果 SystemHealth 中尚无注册 target，monitor 也在等待
        // 引擎通过 PortNameConfigBuilder::health_check_payload() 注册 target 后自动触发
        {
            let drt_for_hc = drt.clone();
            tokio::spawn(async move {
                if let Err(e) = crate::health_check::start_health_check_manager(
                    drt_for_hc, None,
                ).await {
                    tracing::warn!("HealthCheckManager start error: {e}");
                }
            });
        }

        Ok(drt)
    }

    // ── 服务模型入口 ─────────────────────────────────────────────────

    /// 创建或获取命名空间。
    pub fn namespace(&self, name: impl Into<String>) -> anyhow::Result<Namespace> {
        Namespace::new(self.clone(), name.into())
    }

    // ── 关闭 ─────────────────────────────────────────────────────────

    /// 优雅关闭：先停运行时任务，再注销服务发现。
    pub fn shutdown(&self) {
        self.runtime.shutdown();
        // discovery shutdown 由 cancel_token 触发各后台任务退出
        // K8s informer daemon 等监听 primary_token，自动退出
    }

    // ── 访问器 ──────────────────────────────────────────────────────

    pub fn rt(&self) -> &Runtime {
        &self.runtime
    }

    /// 兼容旧接口名称。
    pub fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    pub fn primary_token(&self) -> tokio_util::sync::CancellationToken {
        self.runtime.primary_token()
    }

    pub fn child_token(&self) -> tokio_util::sync::CancellationToken {
        self.runtime.child_token()
    }

    pub fn discovery(&self) -> &Arc<dyn discovery::Discovery> {
        &self.discovery_client
    }

    pub fn nats_client(&self) -> Option<&transports::nats::Client> {
        self.nats_client.as_ref()
    }

    pub fn network_manager(&self) -> &Arc<NetworkManager> {
        &self.network_manager
    }

    pub fn request_plane(&self) -> RequestPlaneMode {
        self.request_plane
    }

    pub fn system_health(&self) -> &Arc<parking_lot::Mutex<SystemHealth>> {
        &self.system_health
    }

    pub fn metrics_registry(&self) -> &MetricsRegistry {
        &self.metrics_registry
    }

    pub fn local_portname_registry(&self) -> &LocalPortNameRegistry {
        &self.local_portname_registry
    }

    pub fn engine_routes(&self) -> &EngineRouteRegistry {
        &self.engine_routes
    }

    pub fn servicegroup_registry(&self) -> &servicegroup::Registry {
        &self.servicegroup_registry
    }

    pub fn discovery_metadata(
        &self,
    ) -> Option<&Arc<tokio::sync::RwLock<discovery::DiscoveryMetadata>>> {
        self.discovery_metadata.as_ref()
    }

    pub(crate) fn instance_sources(&self) -> &Arc<Mutex<InstanceMap>> {
        &self.instance_sources
    }

    pub(crate) fn routing_occupancy_states(&self) -> &Arc<Mutex<RoutingOccupancyMap>> {
        &self.routing_occupancy_states
    }

    /// 当前进程的 connection_id（来自 discovery 后端）。
    pub fn connection_id(&self) -> u64 {
        self.discovery_client.instance_id()
    }

    /// 获取已启动的系统状态服务器信息（如未启动则返回 None）。
    pub fn system_status_server_info(
        &self,
    ) -> Option<Arc<crate::system_status_server::SystemStatusServerInfo>> {
        self.system_status_server.get().cloned()
    }

    /// 获取（或懒创建）TCP 流服务器，供 Worker 角色使用。
    ///
    /// 首次调用时绑定 TCP 端口；后续调用返回同一实例（通过 `OnceLock`）。
    pub async fn get_or_create_tcp_server(
        &self,
    ) -> anyhow::Result<Arc<crate::pipeline::network::tcp::server::TcpStreamServer>> {
        use crate::pipeline::network::tcp::server::TcpStreamServer;

        if let Some(server) = self.tcp_server.get() {
            return Ok(server.clone());
        }
        let cancel = self.runtime.child_token();
        let listen_addr = self.network_manager.listen_addr;
        let server = TcpStreamServer::bind(listen_addr, cancel)
            .await
            .map(Arc::new)
            .map_err(|e| anyhow::anyhow!("failed to bind TCP server: {e}"))?;
        // 若并发初始化，以 OnceLock 中已有的为准
        let _ = self.tcp_server.set(server);
        Ok(self.tcp_server.get().cloned().expect("just set above"))
    }

    /// 获取请求平面服务器（抽象接口，供路由层使用）。
    pub async fn request_plane_server(
        &self,
    ) -> anyhow::Result<Arc<dyn crate::pipeline::network::ingress::RequestPlaneServer>> {
        self.network_manager
            .server("/")
            .await
            .map_err(|e| anyhow::anyhow!("request_plane_server error: {e}"))
    }
}

impl std::fmt::Debug for DistributedRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributedRuntime")
            .field("request_plane", &self.request_plane)
            .finish_non_exhaustive()
    }
}
