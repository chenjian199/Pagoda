// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 设计意图
//! `DistributedRuntime` 是 Pagoda 分布式层的根节点:在本地 [`Runtime`] 之上叠加
//! 「服务发现(etcd) + 事件面(NATS) + 请求面(TCP/HTTP/NATS) + 系统状态服务」等
//! 交叉关关心,并以单一句柄统一生命周期。设计重点:
//! 1. 「能力分层」:仅需本地运行时的场景可用 `process_local()` 跳过远程依赖;
//! 2. 「处处可能关闭」:`shutdown()` / `child_token()` 将取消信号双向贯穿;
//! 3. 「热状态可观测」:`MetricsRegistry` + `system_health()` 上报运行时指标。
//!
//! # 外部契约
//! - `pub struct DistributedRuntime { ... }`:使用者主要接口与返回类型;
//! - `DistributedRuntime::new(rt, cfg)` 同步构造(内部需 tokio 运行时);
//! - `DistributedRuntime::from_settings(rt)` / `from_settings_with_discovery`:
//!   从环境变量初始化,避免调用方手拼 `DistributedConfig`;
//! - `enum DiscoveryBackend { Etcd, Process }`,
//!   `struct DistributedConfig { discovery_backend, .. }`,
//!   `enum RequestPlaneMode { Nats, Http, Tcp }` + `FromStr` / `Display` (全小写);
//! - `pub mod distributed_test_utils`:对外暴露给 integration 测试的辅助构造器。
//!
//! # 实现要点
//! - 后端接入全部点起后才初始化:`etcd_client` / `nats_client` / `tcp_server`
//!   均为 `OnceCell`,被动延迟并只实例化一次;
//! - 「代理同货」模式:大量访问器方法转发到内部状态,使调用者无需调 `Arc<...>`;
//! - `RequestPlaneMode::from_env` 使用 `PGD_REQUEST_PLANE`,未设或非法值默认 `Tcp`;
//! - `register_graceful_task` 使 `GracefulShutdownTracker` 为后台任务、`PortName` 实例、
//!   服务器任务提供统一的优雅终止信号。

use crate::servicegroup::{
    self, ServiceGroup, ServiceGroupBuilder, PortName, PortNameDiscoverySource, Instance, Namespace,
    RoutingOccupancyState,
};
use crate::config::environment_names::tcp_response_stream;
use crate::pipeline::PipelineError;
use crate::pipeline::network::manager::NetworkManager;
use crate::service::{ServiceClient, ServiceSet};
use crate::storage::kv;
use crate::{discovery, system_status_server, transports};
use crate::{
    discovery::Discovery,
    metrics::PrometheusUpdateCallback,
    metrics::{MetricsHierarchy, MetricsRegistry},
    transports::{etcd, nats, tcp},
};

use super::utils::GracefulShutdownTracker;
use crate::SystemHealth;
use crate::runtime::Runtime;

// Used instead of std::cell::OnceCell because get_or_try_init there is nightly
use async_once_cell::OnceCell;

use std::fmt;
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;
use tokio::sync::watch::Receiver;

use anyhow::Result;
use derive_getters::Dissolve;
use figment::error;
use std::collections::HashMap;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

type PortNameDiscoverySourceMap = HashMap<PortName, Weak<PortNameDiscoverySource>>;
type RoutingOccupancyMap = HashMap<PortName, Weak<RoutingOccupancyState>>;

// === SECTION: DistributedRuntime ===

/// Distributed [Runtime] which provides access to shared resources across the cluster, this includes
/// communication protocols and transports.
#[derive(Clone)]
pub struct DistributedRuntime {
    // local runtime
    runtime: Runtime,

    nats_client: Option<transports::nats::Client>,
    network_manager: Arc<NetworkManager>,
    tcp_server: Arc<OnceCell<Arc<transports::tcp::server::TcpStreamServer>>>,
    system_status_server: Arc<OnceLock<Arc<system_status_server::SystemStatusServerInfo>>>,
    request_plane: RequestPlaneMode,

    // Service discovery client
    discovery_client: Arc<dyn discovery::Discovery>,

    // Discovery metadata (only used for Kubernetes backend)
    // Shared with system status server to expose via /metadata portname
    discovery_metadata: Option<Arc<tokio::sync::RwLock<discovery::DiscoveryMetadata>>>,

    // local registry for servicegroups
    // the registry allows us to use share runtime resources across instances of the same servicegroup object.
    // take for example two instances of a client to the same remote servicegroup. The registry allows us to use
    // a single portname watcher for both clients, this keeps the number background tasking watching specific
    // paths in etcd to a minimum.
    servicegroup_registry: servicegroup::Registry,

    portname_discovery_sources: Arc<tokio::sync::Mutex<PortNameDiscoverySourceMap>>,
    routing_occupancy_states: Arc<tokio::sync::Mutex<RoutingOccupancyMap>>,

    // Health Status
    system_health: Arc<parking_lot::Mutex<SystemHealth>>,

    // Local portname registry for in-process calls
    local_portname_registry: crate::local_portname_registry::LocalPortNameRegistry,

    // This hierarchy's own metrics registry
    metrics_registry: MetricsRegistry,

    // Registry for /engine/* route callbacks
    engine_routes: crate::engine_routes::EngineRouteRegistry,

    // Backs `/v1/metadata/{model_slug}/{model_suffix}/{filename}`.
    metadata_artifacts: crate::metadata_registry::MetadataArtifactRegistry,

    // Resolved event transport kind — set once at construction time from
    // PGD_EVENT_PLANE + discovery backend; returned by default_event_transport_kind().
    event_transport_kind: crate::discovery::EventTransportKind,
}

impl MetricsHierarchy for DistributedRuntime {
    // 中文说明：
    // 1. 这个函数用于告诉指标层级系统，当前 DistributedRuntime 节点自己的基础名称是什么。
    // 2. DistributedRuntime 在这里被当作整棵指标树的根节点处理，因此不额外追加任何名字片段。
    // 3. 直接返回空字符串，可以让后续真正参与拼接的名称从 Namespace 等更具体的层级开始。
    fn basename(&self) -> String {
        String::new() // drt has no basename. Basename only begins with the Namespace.
    }

    // 中文说明：
    // 1. 这个函数返回当前节点在指标树中的父层级列表。
    // 2. 由于 DistributedRuntime 本身就是根节点，所以它上面不存在更高一层的指标容器。
    // 3. 因此这里构造并返回一个空数组，明确表达“没有父层级”这个事实。
    fn parent_hierarchies(&self) -> Vec<&dyn MetricsHierarchy> {
        Vec::new() // drt is the root, so no parent hierarchies
    }

    // 中文说明：
    // 1. 这个函数负责把当前运行时持有的指标注册表暴露给指标系统。
    // 2. 代码先把字段引用绑定到局部变量，便于后续阅读时明确“要返回的是哪个注册表实例”。
    // 3. 最终返回对同一个 MetricsRegistry 的不可变引用，不会创建副本，也不会改变内部状态。
    fn get_metrics_registry(&self) -> &MetricsRegistry {
        let registry = &self.metrics_registry;
        registry
    }

    // 中文说明：
    // 1. 这个函数对外提供当前运行时对应的连接标识，用于指标层级接口的统一访问。
    // 2. 它先向 discovery 子系统查询当前实例的 instance_id。
    // 3. 因为 trait 接口要求返回 Option<u64>，所以这里再把实际拿到的 id 包装进 Some 中返回。
    fn connection_id(&self) -> Option<u64> {
        let connection_id = self.discovery().instance_id();
        Some(connection_id)
    }
}

impl std::fmt::Debug for DistributedRuntime {
    // 中文说明：
    // 1. 这个函数定义 DistributedRuntime 在调试输出中的文本表现形式。
    // 2. 这里不展开所有内部字段，避免日志里出现过多噪声或复杂结构。
    // 3. 因此只向 formatter 写入固定字符串 "DistributedRuntime"，用来表明对象类型。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DistributedRuntime")
    }
}

impl DistributedRuntime {
    // 中文说明：
    // 1. 这个构造函数先拆开传入配置，得到 discovery 后端、NATS 配置、请求平面模式和事件平面类型。
    // 2. 然后根据 NATS 配置决定是否建立 NATS 客户端连接，没有配置时保留为 None。
    // 3. 接着读取运行时配置，准备系统状态服务的取消令牌，并构建 SystemHealth 初始状态。
    // 4. 随后按 discovery backend 的不同分支初始化 Kubernetes 或 KVStore discovery 客户端，同时准备可能的元数据对象。
    // 5. discovery 客户端就绪后，再创建组件注册表、网络管理器，以及 DistributedRuntime 自身的所有字段。
    // 6. 运行时对象创建完后，会初始化 uptime gauge，并注册 Prometheus 更新回调，确保每次抓取前都刷新运行时长。
    // 7. 如果系统状态服务被启用，就启动 HTTP 服务并把返回的服务信息存入 OnceLock；如果失败则记录错误日志。
    // 8. 如果健康检查功能开启，还会继续拉起健康检查管理器，让每个端点的探活逻辑进入工作状态。
    // 9. 所有初始化步骤都成功后，最终返回构建完成的 DistributedRuntime。
    pub async fn new(runtime: Runtime, config: DistributedConfig) -> Result<Self> {
        let (discovery_backend, nats_config, request_plane, event_transport_kind) =
            config.dissolve();

        let nats_client = match nats_config {
            Some(client_options) => Some(client_options.connect().await?),
            None => None,
        };

        // Start system status server for health and metrics if enabled in configuration
        let runtime_config = crate::config::RuntimeConfig::from_settings().unwrap_or_default();
        // IMPORTANT: We must extract cancel_token from runtime BEFORE moving runtime into the struct below.
        // This is because after moving, runtime is no longer accessible in this scope (ownership rules).
        let cancel_token = runtime_config
            .system_server_enabled()
            .then(|| runtime.clone().child_token());
        let system_health = {
            let starting_health_status = runtime_config.starting_health_status.clone();
            let use_portname_health_status = runtime_config.use_portname_health_status.clone();
            let health_endpoint_path = runtime_config.system_health_path.clone();
            let live_endpoint_path = runtime_config.system_live_path.clone();

            Arc::new(parking_lot::Mutex::new(SystemHealth::new(
                starting_health_status,
                use_portname_health_status,
                runtime_config.health_check_enabled,
                health_endpoint_path,
                live_endpoint_path,
            )))
        };
        let primary_token = runtime.primary_token();
        let child_token = runtime.child_token();

        // Initialize discovery client based on backend configuration
        let discovery_setup = match discovery_backend {
            DiscoveryBackend::Kubernetes => {
                tracing::info!("Initializing Kubernetes discovery backend");
                let metadata = Arc::new(tokio::sync::RwLock::new(
                    crate::discovery::DiscoveryMetadata::new(),
                ));
                let client = crate::discovery::KubeDiscoveryClient::new(
                    Arc::clone(&metadata),
                    primary_token.clone(),
                )
                .await
                .inspect_err(
                    |err| tracing::error!(%err, "Failed to initialize Kubernetes discovery client"),
                )?;

                let discovery_client: Arc<dyn Discovery> = Arc::new(client);
                (discovery_client, Some(metadata))
            }
            DiscoveryBackend::KvStore(kv_selector) => {
                tracing::info!("Initializing KV store discovery backend: {kv_selector}");
                let runtime_clone = runtime.clone();
                let discovery_token = primary_token.clone();
                let store = match kv_selector {
                    kv::Selector::Etcd(etcd_config) => {
                        let etcd_client = etcd::Client::new(*etcd_config, runtime_clone).await.inspect_err(|err|
                            tracing::error!(%err, "Could not connect to etcd. Pass `--discovery-backend ..` to use a different backend or start etcd."))?;
                        kv::Manager::etcd(etcd_client)
                    }
                    kv::Selector::File(root) => kv::Manager::file(discovery_token.clone(), root),
                    kv::Selector::Memory => kv::Manager::memory(),
                };
                use crate::discovery::KVStoreDiscovery;

                let discovery_client: Arc<dyn Discovery> =
                    Arc::new(KVStoreDiscovery::new(store, discovery_token));
                (discovery_client, None)
            }
        };
        let (discovery_client, discovery_metadata) = discovery_setup;

        let servicegroup_registry = servicegroup::Registry::new();

        // NetworkManager for request plane
        let network_manager = {
            let request_plane_client = nats_client.as_ref().map(|client| client.client().clone());
            Arc::new(NetworkManager::new(
                child_token,
                request_plane_client,
                servicegroup_registry.clone(),
                request_plane,
            ))
        };

        let distributed_runtime = Self {
            runtime,
            network_manager,
            nats_client,
            tcp_server: Arc::new(OnceCell::new()),
            system_status_server: Arc::new(OnceLock::new()),
            discovery_client,
            discovery_metadata,
            servicegroup_registry,
            portname_discovery_sources: Arc::new(Mutex::new(HashMap::new())),
            routing_occupancy_states: Arc::new(Mutex::new(HashMap::new())),
            metrics_registry: crate::MetricsRegistry::new(),
            system_health,
            request_plane,
            local_portname_registry: crate::local_portname_registry::LocalPortNameRegistry::new(),
            engine_routes: crate::engine_routes::EngineRouteRegistry::new(),
            metadata_artifacts: crate::metadata_registry::MetadataArtifactRegistry::new(),
            event_transport_kind,
        };

        // Initialize the uptime gauge in SystemHealth
        {
            let system_health = distributed_runtime.system_health.lock();
            system_health.initialize_uptime_gauge(&distributed_runtime)?;
        }

        // Register an update callback so the uptime gauge is refreshed before
        // every Prometheus scrape (both system status server and frontend).
        distributed_runtime.metrics_registry.add_update_callback({
            let system_health = Arc::clone(&distributed_runtime.system_health);
            let callback: PrometheusUpdateCallback = Arc::new(move || {
                system_health.lock().update_uptime_gauge();
                Ok(())
            });
            callback
        });

        // Handle system status server initialization
        match cancel_token {
            Some(cancel_token) => {
                let host = runtime_config.system_host.clone();
                let port = runtime_config.system_port as u16;
                let runtime_handle = Arc::new(distributed_runtime.clone());
                let discovery_metadata = distributed_runtime.discovery_metadata.clone();

                match crate::system_status_server::spawn_system_status_server(
                    &host,
                    port,
                    cancel_token,
                    runtime_handle,
                    discovery_metadata,
                )
                .await
                {
                    Ok((addr, handle)) => {
                        tracing::info!("System status server started successfully on {addr}");

                        let server_info = Arc::new(
                            crate::system_status_server::SystemStatusServerInfo::new(
                                addr,
                                Some(handle),
                            ),
                        );

                        distributed_runtime
                            .system_status_server
                            .set(server_info)
                            .expect("System status server info should only be set once");
                    }
                    Err(error) => {
                        tracing::error!("System status server startup failed: {error}");
                    }
                }
            }
            None => {
                tracing::debug!(
                    "System status server HTTP portnames disabled, but uptime metrics are being tracked"
                );
            }
        }

        // Start health check manager if enabled
        if runtime_config.health_check_enabled {
            let health_check_config = crate::health_check::HealthCheckConfig {
                canary_wait_time: Duration::from_secs(runtime_config.canary_wait_time_secs),
                request_timeout: Duration::from_secs(
                    runtime_config.health_check_request_timeout_secs,
                ),
            };

            // Start the health check manager (spawns per-portname monitoring tasks)
            match crate::health_check::start_health_check_manager(
                distributed_runtime.clone(),
                Some(health_check_config),
            )
            .await
            {
                Ok(()) => {
                    tracing::info!(
                        "Health check manager started (canary_wait_time: {}s, request_timeout: {}s)",
                        runtime_config.canary_wait_time_secs,
                        runtime_config.health_check_request_timeout_secs
                    )
                }
                Err(error) => tracing::error!("Health check manager failed to start: {error}"),
            }
        }

        Ok(distributed_runtime)
    }

    // 中文说明：
    // 1. 这个函数是一个便捷入口，用于从当前环境配置直接创建 DistributedRuntime。
    // 2. 它先调用 DistributedConfig::from_settings 读取环境变量和默认值，生成完整配置。
    // 3. 然后把生成出的配置连同传入的 Runtime 一起交给 new，复用主构造逻辑完成初始化。
    pub async fn from_settings(runtime: Runtime) -> Result<Self> {
        Self::new(runtime, DistributedConfig::from_settings()).await
    }

    // 中文说明：
    // 1. 这个函数返回当前 DistributedRuntime 内部持有的 Runtime 引用。
    // 2. 代码通过模式匹配显式取出 runtime 字段，让返回来源一眼可见。
    // 3. 返回的是借用而不是克隆，因此不会产生额外的运行时实例。
    pub fn runtime(&self) -> &Runtime {
        match self {
            Self { runtime, .. } => runtime,
        }
    }

    // 中文说明：
    // 1. 这个函数用于取得顶层取消令牌，供外部监听全局关闭信号。
    // 2. 它先复用 runtime() 拿到底层 Runtime 引用，保持访问路径统一。
    // 3. 随后把 primary_token 从 Runtime 中取出并返回，调用方拿到的是可独立使用的 CancellationToken。
    pub fn primary_token(&self) -> CancellationToken {
        let runtime = self.runtime();
        runtime.primary_token()
    }

    // TODO: Don't hand out pointers, instead have methods to use the registry in friendly ways
    // (without being aware of async locks and so on)
    // 中文说明：
    // 1. 这个函数把组件注册表的引用暴露出去，供其它模块读取或操作已注册组件。
    // 2. 这里使用 match 只是为了把返回对象写得更显式，强调返回值就是当前结构里的 servicegroup_registry 字段。
    // 3. 返回的是共享借用，因此不会移动注册表，也不会改变内部存储内容。
    pub fn servicegroup_registry(&self) -> &servicegroup::Registry {
        match &self.servicegroup_registry {
            registry => registry,
        }
    }

    // TODO: Don't hand out pointers, instead provide system health related services.
    // 中文说明：
    // 1. 这个函数提供系统健康状态对象的共享访问入口。
    // 2. 因为内部字段是 Arc 包裹的互斥对象，所以这里通过 Arc::clone 增加一个共享引用计数。
    // 3. 调用方拿到克隆后的 Arc 后，可以在不影响所有权的前提下继续读取或更新健康状态。
    pub fn system_health(&self) -> Arc<parking_lot::Mutex<SystemHealth>> {
        Arc::clone(&self.system_health)
    }

    // 中文说明：
    // 1. 这个函数返回本地端点注册表，供进程内直连调用场景使用。
    // 2. 代码通过 match 显式取出 local_portname_registry 字段，表达“这里只是转交现有注册表”。
    // 3. 返回引用而不是复制对象，保证所有调用方看到的是同一个本地端点注册中心。
    /// Get the local portname registry for in-process portname calls
    pub fn local_portname_registry(
        &self,
    ) -> &crate::local_portname_registry::LocalPortNameRegistry {
        match &self.local_portname_registry {
            registry => registry,
        }
    }

    // 中文说明：
    // 1. 这个函数把 /engine/* 路由注册表暴露给上层，以便外部注册自定义引擎路由。
    // 2. 它通过 match 明确返回当前运行时内部保存的 engine_routes 字段。
    // 3. 由于只是返回不可变引用，所以不会触发任何路由注册行为，也不会修改现有状态。
    /// Get the engine route registry for registering custom /engine/* routes
    pub fn engine_routes(&self) -> &crate::engine_routes::EngineRouteRegistry {
        match &self.engine_routes {
            routes => routes,
        }
    }

    // 中文说明：
    // 1. 这个函数返回元数据制品注册表，用于支撑 /v1/metadata 相关资源查询。
    // 2. 代码先把字段引用绑定到局部变量，突出返回值来源，便于后续阅读时定位。
    // 3. 最终返回的是同一个注册表实例的借用，不会复制任何元数据内容。
    pub fn metadata_artifacts(&self) -> &crate::metadata_registry::MetadataArtifactRegistry {
        let metadata_artifacts = &self.metadata_artifacts;
        metadata_artifacts
    }

    // 中文说明：
    // 1. 这个函数返回当前运行时实例在 discovery 系统中的连接 id。
    // 2. 它直接委托给 discovery() 返回的 discovery 客户端去查询 instance_id。
    // 3. 因为这里的公开接口需要直接给出 u64，所以拿到值后原样返回即可。
    pub fn connection_id(&self) -> u64 {
        self.discovery().instance_id()
    }

    // 中文说明：
    // 1. 这个函数负责关闭当前 DistributedRuntime 关联的关键子系统。
    // 2. 它先取出底层 Runtime 和 discovery_client 的引用，明确后续关闭动作分别作用在哪两个对象上。
    // 3. 随后先触发 runtime.shutdown，再调用 discovery_client.shutdown，使本地运行时与发现系统一起进入收尾流程。
    pub fn shutdown(&self) {
        let runtime = &self.runtime;
        let discovery_client = &self.discovery_client;

        runtime.shutdown();
        discovery_client.shutdown();
    }

    // 中文说明：
    // 1. 这个函数根据调用方提供的名称创建一个新的 Namespace 对象。
    // 2. 首先把传入的泛型参数统一转换成 String，避免后续构造函数再处理多种输入类型。
    // 3. 然后把当前 DistributedRuntime 的克隆和名字一起交给 Namespace::new，得到与当前运行时绑定的命名空间实例。
    /// Create a [`Namespace`]
    pub fn namespace(&self, name: impl Into<String>) -> Result<Namespace> {
        let namespace_name = name.into();
        Namespace::new(self.clone(), namespace_name)
    }

    // 中文说明：
    // 1. 这个函数向外返回 discovery 接口，供服务注册、发现和实例查询等逻辑使用。
    // 2. 内部字段是 Arc<dyn Discovery>，因此这里使用 Arc::clone 复制共享指针而不是复制底层对象。
    // 3. 调用方拿到新的 Arc 后，可以独立持有 discovery 客户端，而不会影响当前运行时的所有权结构。
    /// Returns the discovery interface for service registration and discovery
    pub fn discovery(&self) -> Arc<dyn Discovery> {
        Arc::clone(&self.discovery_client)
    }

    // 中文说明：
    // 1. 这个函数懒加载并返回 TCP response stream 服务端实例，避免在未使用时提前占用端口和资源。
    // 2. 如果 OnceCell 里还没有服务端，它会读取端口和主机环境变量，解析端口是否合法，并决定是否绑定固定主机地址。
    // 3. 之后代码会根据端口配置打印不同日志，明确是使用固定端口还是让操作系统自动分配端口。
    // 4. 最后用解析出的参数创建 TcpStreamServer，把结果缓存进 OnceCell，并把缓存中的 Arc 克隆一份返回给调用方。
    pub async fn tcp_server(&self) -> Result<Arc<tcp::server::TcpStreamServer>> {
        let server = self
            .tcp_server
            .get_or_try_init(async {
                let port = if let Ok(raw_port) =
                    std::env::var(tcp_response_stream::PGD_TCP_RESPONSE_STREAM_PORT)
                {
                    raw_port.parse::<u16>().map_err(|_| {
                        PipelineError::Generic(format!(
                            "invalid {}: '{}' is not a valid port number",
                            tcp_response_stream::PGD_TCP_RESPONSE_STREAM_PORT,
                            raw_port
                        ))
                    })?
                } else {
                    0
                };

                let interface = match std::env::var(tcp_response_stream::PGD_TCP_RESPONSE_STREAM_HOST)
                {
                    Ok(host) if !host.is_empty() => Some(host),
                    _ => None,
                };

                let host_suffix = interface
                    .as_deref()
                    .map(|host| format!(" on host {host}"))
                    .unwrap_or_default();

                match port {
                    0 => {
                        tracing::info!(
                            "TCP response stream server using OS-assigned port{host_suffix}"
                        );
                    }
                    port => {
                        tracing::info!(
                            "TCP response stream server using fixed port {port}{host_suffix}"
                        );
                    }
                }

                let options = tcp::server::ServerOptions { port, interface };
                tcp::server::TcpStreamServer::new(options)
                    .await
                    .map_err(PipelineError::from)
            })
            .await?;

        Ok(Arc::clone(server))
    }

    // 中文说明：
    // 1. 这个函数提供网络管理器的共享访问入口。
    // 2. 因为 network_manager 被 Arc 包裹，所以这里通过 Arc::clone 返回一个新的共享句柄。
    // 3. 这样调用方可以安全地继续使用网络管理器，而不需要关心底层对象的生命周期管理。
    /// Get the network manager
    ///
    /// The network manager consolidates all network configuration and provides
    /// unified access to request plane servers and clients.
    pub fn network_manager(&self) -> Arc<NetworkManager> {
        Arc::clone(&self.network_manager)
    }

    // 中文说明：
    // 1. 这个函数是 request plane server 的便捷访问入口，避免外部重复写拿 manager 再取 server 的流程。
    // 2. 它先通过 network_manager() 获取网络管理器的共享句柄。
    // 3. 然后调用 server().await 创建或获取统一请求平面服务端，并把结果直接返回给调用方。
    /// Get the request plane server (convenience method)
    ///
    /// This is a shortcut for `network_manager().await?.server().await`.
    pub async fn request_plane_server(
        &self,
    ) -> Result<Arc<dyn crate::pipeline::network::ingress::unified_server::RequestPlaneServer>>
    {
        let network_manager = self.network_manager();
        network_manager.server().await
    }

    // 中文说明：
    // 1. 这个函数查询系统状态服务是否已经启动，并返回对应的服务信息对象。
    // 2. 它先从 OnceLock 中尝试读取已经保存的 SystemStatusServerInfo。
    // 3. 如果服务存在，就克隆一份 Arc 返回；如果尚未启动或初始化失败，则返回 None 表示没有可用信息。
    /// Get system status server information if available
    pub fn system_status_server_info(
        &self,
    ) -> Option<Arc<crate::system_status_server::SystemStatusServerInfo>> {
        match self.system_status_server.get() {
            Some(server_info) => Some(Arc::clone(server_info)),
            None => None,
        }
    }

    // 中文说明：
    // 1. 这个函数返回当前运行时配置好的 request plane 模式。
    // 2. 代码使用 match 显式取出枚举值，便于表达“这里只是返回字段本身”。
    // 3. 由于 RequestPlaneMode 是可复制的小型枚举，返回时不会带来额外的资源管理成本。
    /// How the frontend should talk to the backend.
    pub fn request_plane(&self) -> RequestPlaneMode {
        match self.request_plane {
            mode => mode,
        }
    }

    // 中文说明：
    // 1. 这个函数返回在构造 DistributedRuntime 时就已经解析好的事件传输类型。
    // 2. 它不会重新读取环境变量，也不会再次根据后端类型重新推导，保证整个运行期答案一致。
    // 3. 代码先把字段赋给局部变量，再原样返回这个缓存值，强调返回的是启动时确定下来的配置结果。
    /// Returns the event transport kind this runtime was configured with.
    ///
    /// The value is resolved once at construction time by `DiscoveryBackend::resolve_event_transport_kind`:
    /// if `PGD_EVENT_PLANE` is set explicitly that value wins; otherwise the discovery
    /// backend drives the default (ZMQ for `file`/`mem`, NATS for `etcd`/`kubernetes`).
    ///
    /// Use this instead of [`EventTransportKind::from_env_or_default`] wherever you have
    /// access to a `DistributedRuntime`, so that local-only workflows work without
    /// setting `PGD_EVENT_PLANE` explicitly.
    pub fn default_event_transport_kind(&self) -> crate::discovery::EventTransportKind {
        let event_transport_kind = self.event_transport_kind;
        event_transport_kind
    }

    // 中文说明：
    // 1. 这个函数返回当前运行时派生出的子取消令牌。
    // 2. 它直接复用 runtime() 获取到底层 Runtime。
    // 3. 然后调用 Runtime::child_token，让调用方可以订阅更细粒度的关闭信号。
    pub fn child_token(&self) -> CancellationToken {
        self.runtime().child_token()
    }

    // 中文说明：
    // 1. 这个函数把优雅关闭跟踪器暴露给内部模块使用。
    // 2. 它不自己维护额外状态，而是直接委托到底层 Runtime。
    // 3. 返回的是 Arc 包裹的共享跟踪器，调用方可以继续注册或观察长生命周期关闭任务。
    pub(crate) fn graceful_shutdown_tracker(&self) -> Arc<GracefulShutdownTracker> {
        self.runtime().graceful_shutdown_tracker()
    }

    // 中文说明：
    // 1. 这个函数返回端点发现来源映射表的共享句柄。
    // 2. 内部字段本身是 Arc<Mutex<...>>，所以这里通过 Arc::clone 保持共享语义。
    // 3. 调用方随后可以锁住这张表，追踪每个 PortName 对应的发现来源对象。
    pub(crate) fn portname_discovery_sources(&self) -> Arc<Mutex<PortNameDiscoverySourceMap>> {
        Arc::clone(&self.portname_discovery_sources)
    }

    // 中文说明：
    // 1. 这个函数用于把外部长时间运行的清理任务注册到优雅关闭流程中。
    // 2. 它先从 Runtime 中拿到统一的 graceful shutdown tracker。
    // 3. 然后调用 register_task 生成守卫对象，只要守卫还活着，关闭流程就会继续等待该任务完成。
    /// Register an external long-running shutdown task with this runtime's
    /// graceful-shutdown tracker. While the returned guard is alive,
    /// `Runtime::shutdown` will keep waiting in Phase 2 (rather than
    /// advancing to Phase 3 / NATS+etcd teardown). Drop the guard once
    /// the task has finished.
    pub fn register_graceful_task(&self) -> crate::utils::GracefulTaskGuard {
        let graceful_shutdown_tracker = self.runtime.graceful_shutdown_tracker();
        graceful_shutdown_tracker.register_task()
    }

    // 中文说明：
    // 1. 这个函数返回路由占用状态映射表的共享句柄。
    // 2. 这里通过 Arc::clone 把内部共享状态安全地暴露给其它模块。
    // 3. 调用方可以在之后加锁读取或更新每个 PortName 对应的 RoutingOccupancyState。
    pub(crate) fn routing_occupancy_states(&self) -> Arc<Mutex<RoutingOccupancyMap>> {
        Arc::clone(&self.routing_occupancy_states)
    }

    // 中文说明：
    // 1. 这个函数负责把 KV Router 产生的事件消息发布到 NATS 指定主题。
    // 2. 代码先检查当前运行时是否已经配置 NATS 客户端；如果有，就真正执行 publish 并等待异步发送完成。
    // 3. 如果没有 NATS，则把这次发布视为可选行为，只记录一条 trace 日志后返回 Ok(())，避免近似模式下报错。
    /// TODO: This is a temporary KV router measure for servicegroup/servicegroup.rs EventPublisher impl for
    /// ServiceGroup, to allow it to publish to NATS. KV Router is the only user.
    ///
    /// When NATS is not available (e.g., running in approximate mode with --no-kv-events),
    /// this function returns Ok(()) silently since publishing is optional in that mode.
    pub async fn kv_router_nats_publish(
        &self,
        subject: String,
        payload: bytes::Bytes,
    ) -> anyhow::Result<()> {
        match self.nats_client.as_ref() {
            Some(nats_client) => {
                nats_client.client().publish(subject, payload).await?;
                Ok(())
            }
            None => {
                tracing::trace!("Skipping NATS publish (NATS not configured): {subject}");
                Ok(())
            }
        }
    }

    // 中文说明：
    // 1. 这个函数为 KV Router 建立一个 NATS 订阅者，用来接收指定主题上的消息。
    // 2. 它先判断当前运行时里是否存在 NATS 客户端；有客户端时就向 NATS 发起 subscribe 请求并返回订阅者。
    // 3. 如果运行时根本没有启用 NATS，则立即返回错误，明确告诉上层这个能力依赖 NATS 支持。
    /// TODO: This is a temporary KV router measure for servicegroup/servicegroup.rs EventSubscriber impl for
    /// ServiceGroup, to allow it to subscribe to NATS. KV Router is the only user.
    pub(crate) async fn kv_router_nats_subscribe(
        &self,
        subject: String,
    ) -> Result<async_nats::Subscriber> {
        if let Some(nats_client) = self.nats_client.as_ref() {
            return Ok(nats_client.client().subscribe(subject).await?);
        }

        anyhow::bail!("KV router's EventSubscriber requires NATS")
    }

    // 中文说明：
    // 1. 这个函数让 KV Router 通过 NATS 执行一次 request/reply 交互，并等待远端回复。
    // 2. 首先它会检查 NATS 客户端是否存在，不存在时立即返回错误，避免继续发请求。
    // 3. 有客户端后，代码把 request 调用包装成 future，再用 tokio::time::timeout 为它增加超时控制。
    // 4. 最后把 timeout 结果和内部 request 结果两层错误都展开，成功时返回收到的 async_nats::Message。
    /// TODO (karenc): This is a temporary KV router measure for worker query requests.
    /// Allows KV Router to perform request/reply with workers. (versus the pub/sub pattern above)
    /// KV Router is the only user, made public for use in pagoda-llm crate
    pub async fn kv_router_nats_request(
        &self,
        subject: String,
        payload: bytes::Bytes,
        timeout: std::time::Duration,
    ) -> anyhow::Result<async_nats::Message> {
        let nats_client = if let Some(nats_client) = self.nats_client.as_ref() {
            nats_client
        } else {
            anyhow::bail!("KV router's request requires NATS");
        };

        let request_future = nats_client.client().request(subject, payload);
        let response = tokio::time::timeout(timeout, request_future)
            .await
            .map_err(|_| anyhow::anyhow!("Request timed out after {:?}", timeout))??;

        Ok(response)
    }

    // 中文说明：
    // 1. 这个函数负责为某个组件异步注册 NATS service，并通过返回的 channel 把结果通知给调用方。
    // 2. 入口处先创建容量为 1 的 mpsc 通道，然后把真正的注册逻辑丢到 secondary runtime 的异步任务里执行。
    // 3. 后台任务会先计算 service_name，并先查一遍注册表；如果服务已存在，就直接回传成功并结束，避免重复创建。
    // 4. 若当前运行时没有 NATS 客户端，就记录错误并通过 channel 返回失败原因。
    // 5. 有 NATS 时，代码继续调用 build_nats_service 构建服务对象，构建失败同样会记录日志并把错误发回去。
    // 6. 服务构建成功后，再次加锁组件注册表，用 entry API 判断最终应插入还是放弃重复服务。
    // 7. 若插入成功就记录新增日志；若发现已有占位，则停止刚创建的重复服务，避免泄漏多余后台资源。
    // 8. 整个流程结束后，后台任务会通过 tx 发送 Ok(()), 调用方可从 rx 中等待最终注册结果。
    /// DEPRECATED: This method exists only for NATS request plane support.
    /// Once everything uses the TCP request plane, this can be removed along with
    /// the NATS service registration infrastructure.
    ///
    /// Returns a receiver that signals when the NATS service registration is complete.
    /// The caller should use `blocking_recv()` to wait for completion.
    pub fn register_nats_service(
        &self,
        servicegroup: ServiceGroup,
    ) -> tokio::sync::mpsc::Receiver<Result<(), String>> {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<(), String>>(1);

        let drt = self.clone();
        let secondary_runtime = self.runtime().secondary();
        secondary_runtime.spawn(async move {
            let service_name = servicegroup.service_name();
            let missing_nats_error = "Cannot create NATS service without NATS";

            let service_exists = {
                let guard = drt.servicegroup_registry().inner.lock().await;
                guard.services.contains_key(&service_name)
            };

            if service_exists {
                tracing::trace!("Service {service_name} already exists");
                let _ = tx.send(Ok(())).await;
                return;
            }

            let nats_client = if let Some(nats_client) = drt.nats_client.as_ref() {
                nats_client
            } else {
                tracing::error!("{missing_nats_error}.");
                let _ = tx.send(Err(missing_nats_error.to_string())).await;
                return;
            };

            let nats_service = match crate::servicegroup::service::build_nats_service(
                nats_client,
                &servicegroup,
                None,
            )
            .await
            {
                Ok(service) => service,
                Err(err) => {
                    tracing::error!(error = %err, servicegroup = service_name, "Failed to build NATS service");
                    let _ = tx.send(Err(format!("Failed to build NATS service: {err}"))).await;
                    return;
                }
            };

            let mut guard = drt.servicegroup_registry().inner.lock().await;
            match guard.services.entry(service_name.clone()) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(nats_service);
                    tracing::info!("Added NATS service {service_name}");
                    drop(guard);
                }
                std::collections::hash_map::Entry::Occupied(_) => {
                    drop(guard);
                    let _ = nats_service.stop().await;
                }
            }

            let _ = tx.send(Ok(())).await;
        });

        rx
    }
}

// === SECTION: DiscoveryBackend ===

/// Selects which discovery backend to use and, for KV store backends, which KV store.
#[derive(Clone, Debug)]
pub enum DiscoveryBackend {
    /// Use Kubernetes API for service discovery (no KV store needed)
    Kubernetes,
    /// Use a KV store (etcd, file, or memory) for service discovery
    KvStore(kv::Selector),
}

impl DiscoveryBackend {
    // 中文说明：
    // 1. 这个函数用于判断当前 discovery backend 是否属于“本地模式”后端。
    // 2. 对 file 和 memory 这两类 KVStore 选择器，函数返回 true，因为它们不依赖外部基础设施。
    // 3. 对 kubernetes 或其它需要远端依赖的 KVStore 类型，则返回 false，供默认配置逻辑继续分支处理。
    /// Returns true if this backend requires no external services (file or in-memory).
    ///
    /// Local backends do not need etcd, NATS, or any other infrastructure daemon.
    /// This is used to drive smart defaults: for example, the event plane defaults to
    /// ZMQ (not NATS) when a local backend is in use and `PGD_EVENT_PLANE` is not set.
    pub fn is_local(&self) -> bool {
        match self {
            DiscoveryBackend::KvStore(kv::Selector::File(_))
            | DiscoveryBackend::KvStore(kv::Selector::Memory) => true,
            DiscoveryBackend::Kubernetes | DiscoveryBackend::KvStore(_) => false,
        }
    }

    // 中文说明：
    // 1. 这个函数根据 discovery backend 和环境变量 PGD_EVENT_PLANE 共同决定最终采用哪种事件传输层。
    // 2. 它先定义一个默认策略闭包：本地后端默认用 ZMQ，分布式后端默认用 NATS。
    // 3. 然后读取环境变量；如果显式写了 nats 或 zmq，就直接采用该值。
    // 4. 如果环境变量为空或根本未设置，就回退到前面定义的默认策略。
    // 5. 如果环境变量是非法值，则记录 warning 日志，并返回根据后端推导出的兜底默认值。
    /// Resolve the event transport kind for this backend.
    ///
    /// This is the single authoritative mapping of `(PGD_EVENT_PLANE, backend)` →
    /// `EventTransportKind`. When `PGD_EVENT_PLANE` is unset or empty the backend
    /// drives the default: local backends (`file`/`mem`) → ZMQ, distributed backends
    /// (`etcd`/`kubernetes`) → NATS.
    ///
    /// Call this once at startup and store the result; do not call it repeatedly.
    pub fn resolve_event_transport_kind(&self) -> crate::discovery::EventTransportKind {
        use crate::config::environment_names::event_plane::PGD_EVENT_PLANE;
        use crate::discovery::EventTransportKind;
        let default_kind = || {
            if self.is_local() {
                EventTransportKind::Zmq
            } else {
                EventTransportKind::Nats
            }
        };

        match std::env::var(PGD_EVENT_PLANE) {
            Ok(value) if value == "nats" => EventTransportKind::Nats,
            Ok(value) if value == "zmq" => EventTransportKind::Zmq,
            Ok(value) if value.is_empty() => default_kind(),
            Err(_) => default_kind(),
            Ok(other) => {
                let fallback_kind = default_kind();
                tracing::warn!(
                    "Invalid PGD_EVENT_PLANE value '{}'. Valid values: 'nats', 'zmq'. \
                     Defaulting to {:?}.",
                    other,
                    fallback_kind
                );
                fallback_kind
            }
        }
    }
}

// === SECTION: DistributedConfig ===

#[derive(Dissolve)]
pub struct DistributedConfig {
    pub discovery_backend: DiscoveryBackend,
    pub nats_config: Option<nats::ClientOptions>,
    pub request_plane: RequestPlaneMode,
    /// Resolved event transport kind — computed once at config time from
    /// `PGD_EVENT_PLANE` and the discovery backend, then stored on the runtime
    /// so callers always get the same answer regardless of which other services
    /// happen to be reachable.
    pub event_transport_kind: crate::discovery::EventTransportKind,
}

impl DistributedConfig {
    // 中文说明：
    // 1. 这个函数从环境变量读取分布式运行时所需配置，并组装出一份完整的 DistributedConfig。
    // 2. 它先解析 request plane 模式，再读取 PGD_DISCOVERY_BACKEND 来确定 discovery backend 使用哪一类实现。
    // 3. backend 确定后，函数立即解析 event transport kind，保证事件平面选择与 discovery 逻辑保持一致。
    // 4. 随后代码检查用户是否显式配置了 NATS_SERVER，并结合 request plane 和 event transport 共同判断是否需要启用 NATS 客户端。
    // 5. 最后把解析出的 discovery_backend、nats_config、request_plane 和 event_transport_kind 一起打包返回。
    pub fn from_settings() -> DistributedConfig {
        let request_plane = RequestPlaneMode::from_env();

        // Determine the discovery backend first — we need it to compute the NATS default below.
        // Valid values for PGD_DISCOVERY_BACKEND: "kubernetes", "etcd" (default), "file", "mem"
        let backend_value =
            std::env::var("PGD_DISCOVERY_BACKEND").unwrap_or_else(|_| String::from("etcd"));

        let discovery_backend = if backend_value == "kubernetes" {
            tracing::info!("Using Kubernetes discovery backend");
            DiscoveryBackend::Kubernetes
        } else {
            let selector: kv::Selector = backend_value.parse().unwrap_or_else(|_| {
                panic!(
                    "Unknown PGD_DISCOVERY_BACKEND value: '{backend_value}'. \
                     Valid options: kubernetes, etcd, file, mem"
                )
            });
            DiscoveryBackend::KvStore(selector)
        };

        // Resolve event transport kind once — the single source of truth used both to
        // decide whether to open a NATS connection and to answer
        // `DistributedRuntime::default_event_transport_kind()` later.
        let event_transport_kind = discovery_backend.resolve_event_transport_kind();
        let has_explicit_nats_server =
            std::env::var(crate::config::environment_names::nats::NATS_SERVER).is_ok();

        // NATS is used for more than just NATS request-plane RPC:
        // - KV router events (JetStream or NATS core + local indexer)
        // - inter-router replica sync (NATS core)
        //
        // Enable the NATS client when any of these hold:
        // 1. Request plane is NATS
        // 2. NATS_SERVER is explicitly configured by the user
        // 3. The resolved event transport kind is NATS
        let nats_enabled = request_plane.is_nats()
            || has_explicit_nats_server
            || matches!(event_transport_kind, crate::discovery::EventTransportKind::Nats);

        let nats_config = if nats_enabled {
            Some(nats::ClientOptions::default())
        } else {
            None
        };

        Self {
            discovery_backend,
            nats_config,
            request_plane,
            event_transport_kind,
        }
    }

    // 中文说明：
    // 1. 这个函数构造命令行工具场景下使用的 DistributedConfig。
    // 2. 它先固定创建一个 attach_lease 为 false 的 etcd 客户端配置，再据此生成 Etcd 类型的 discovery backend。
    // 3. 接着沿用与普通配置相同的思路，解析 request plane、event transport，以及是否需要开启 NATS 客户端。
    // 4. 最终返回一份明确以 etcd 为 discovery 基础、同时保留事件平面和请求平面选择结果的配置对象。
    pub fn for_cli() -> DistributedConfig {
        let etcd_config = etcd::ClientOptions {
            attach_lease: false,
            ..Default::default()
        };
        let request_plane = RequestPlaneMode::from_env();
        let discovery_backend =
            DiscoveryBackend::KvStore(kv::Selector::Etcd(Box::new(etcd_config)));
        let event_transport_kind = discovery_backend.resolve_event_transport_kind();
        let has_explicit_nats_server =
            std::env::var(crate::config::environment_names::nats::NATS_SERVER).is_ok();
        let nats_enabled = request_plane.is_nats()
            || has_explicit_nats_server
            || matches!(event_transport_kind, crate::discovery::EventTransportKind::Nats);
        let nats_config = if nats_enabled {
            Some(nats::ClientOptions::default())
        } else {
            None
        };

        Self {
            discovery_backend,
            nats_config,
            request_plane,
            event_transport_kind,
        }
    }

    // 中文说明：
    // 1. 这个函数构造一个“进程内本地运行”的分布式配置，用于前后端同进程的场景。
    // 2. 它把 discovery backend 固定为内存型 KVStore，不依赖外部 etcd 或 Kubernetes。
    // 3. 同时把 request plane 固定为 Tcp、event transport 固定为 Zmq，并明确关闭 NATS 配置。
    // 4. 这样返回的配置可以表达一个完全本地、无需额外网络依赖的执行环境。
    /// A DistributedConfig that isn't distributed, for when the frontend and backend are in the
    /// same process.
    pub fn process_local() -> DistributedConfig {
        let discovery_backend = DiscoveryBackend::KvStore(kv::Selector::Memory);
        let request_plane = RequestPlaneMode::Tcp;
        let event_transport_kind = crate::discovery::EventTransportKind::Zmq;

        Self {
            discovery_backend,
            nats_config: None,
            // This won't be used in process local, so we likely need a "none" option to
            // communicate that and avoid opening the ports.
            request_plane,
            event_transport_kind,
        }
    }
}

// === SECTION: RequestPlaneMode ===

/// Request plane transport mode configuration
///
/// This determines how requests are distributed from routers to workers:
/// - `Nats`: Use NATS for request distribution (legacy)
/// - `Http`: Use HTTP/2 for request distribution
/// - `Tcp`: Use raw TCP for request distribution with msgpack support (default)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RequestPlaneMode {
    /// Use NATS for request plane
    Nats,
    /// Use HTTP/2 for request plane
    Http,
    /// Use raw TCP for request plane with msgpack support
    #[default]
    Tcp,
}

impl fmt::Display for RequestPlaneMode {
    // 中文说明：
    // 1. 这个函数定义 RequestPlaneMode 在字符串输出时应该呈现什么文本。
    // 2. 它先根据枚举分支把当前模式映射成对应的小写字符串常量。
    // 3. 然后把这个字符串写入 formatter，使日志、配置展示和 to_string 结果都保持统一格式。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let request_plane = match self {
            Self::Nats => "nats",
            Self::Http => "http",
            Self::Tcp => "tcp",
        };

        f.write_str(request_plane)
    }
}

impl std::str::FromStr for RequestPlaneMode {
    type Err = anyhow::Error;

    // 中文说明：
    // 1. 这个函数负责把外部传入的字符串解析成 RequestPlaneMode 枚举值。
    // 2. 它先把输入统一转成 ASCII 小写，避免大小写不同导致同义配置解析失败。
    // 3. 随后依次判断是否为 nats、http 或 tcp，并返回对应枚举。
    // 4. 如果字符串不属于这三个合法值，就构造一条带有可选值提示的 anyhow 错误返回给调用方。
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let normalized = s.to_ascii_lowercase();

        if normalized == "nats" {
            Ok(Self::Nats)
        } else if normalized == "http" {
            Ok(Self::Http)
        } else if normalized == "tcp" {
            Ok(Self::Tcp)
        } else {
            Err(anyhow::anyhow!(
                "Invalid request plane mode: '{}'. Valid options are: 'nats', 'http', 'tcp'",
                s
            ))
        }
    }
}

impl RequestPlaneMode {
    // 中文说明：
    // 1. 这个函数从环境变量 PGD_REQUEST_PLANE 中读取请求平面模式。
    // 2. 如果环境变量存在，就尝试把它解析成 RequestPlaneMode；解析失败时退回默认值，避免非法配置导致启动崩溃。
    // 3. 如果环境变量根本不存在，也同样直接返回默认模式，保证系统总能得到一个可用配置。
    /// Get the request plane mode from environment variable (uncached)
    /// Reads from `PGD_REQUEST_PLANE` environment variable.
    fn from_env() -> Self {
        match std::env::var("PGD_REQUEST_PLANE") {
            Ok(value) => value.parse().unwrap_or_default(),
            Err(_) => Self::default(),
        }
    }

    // 中文说明：
    // 1. 这个函数判断当前 request plane 是否为 NATS 模式。
    // 2. 它把 self 与 RequestPlaneMode::Nats 做直接相等比较。
    // 3. 比较结果会以布尔值形式返回，供上层决定是否启用与 NATS 相关的逻辑。
    pub fn is_nats(&self) -> bool {
        *self == RequestPlaneMode::Nats
    }
}

// === SECTION: distributed_test_utils ===

pub mod distributed_test_utils {
    //! Common test helper functions for DistributedRuntime tests

    /// Helper function to create a DRT instance for integration-only tests.
    /// Uses from_current to leverage existing tokio runtime
    /// Note: Settings are read from environment variables inside DistributedRuntime::from_settings
    #[cfg(feature = "integration")]
    // 中文说明：
    // 1. 这个测试辅助函数用于在集成测试里快速创建一个可用的 DistributedRuntime。
    // 2. 它先复用当前 Tokio 运行时句柄，避免测试里重复新建 runtime。
    // 3. 接着手工组装一份基于内存 KVStore、默认 request plane、启用 NATS 的测试配置。
    // 4. 最后调用 DistributedRuntime::new 完成真正初始化，并在测试场景下直接 unwrap 确保失败会立刻暴露出来。
    pub async fn create_test_drt_async() -> super::DistributedRuntime {
        use crate::transports::nats;

        let rt = crate::Runtime::from_current().unwrap();
        let discovery_backend =
            super::DiscoveryBackend::KvStore(crate::storage::kv::Selector::Memory);
        let request_plane = crate::distributed::RequestPlaneMode::default();
        let event_transport_kind = crate::discovery::EventTransportKind::Nats;
        let config = super::DistributedConfig {
            discovery_backend,
            nats_config: Some(nats::ClientOptions::default()),
            request_plane,
            event_transport_kind,
        };

        super::DistributedRuntime::new(rt, config).await.unwrap()
    }

    /// Helper function to create a DRT instance which points at
    /// a (shared) file-backed KV store and ephemeral NATS transport so that
    /// multiple DRT instances may observe the same registration state.
    /// NOTE: This gets around the fact that create_test_drt_async() is
    /// hardcoded to spin up a memory-backed discovery store
    /// which means we can't share discovery state across runtimes.
    // 中文说明：
    // 1. 这个测试辅助函数用于创建多个测试运行时可共享发现状态的 DRT 实例。
    // 2. 与内存版辅助函数不同，这里会把 discovery backend 配成文件型 KVStore，并使用传入路径作为共享存储位置。
    // 3. 其它配置仍然保持默认 request plane 和启用 NATS，从而尽量贴近真实分布式场景。
    // 4. 最后同样调用 DistributedRuntime::new 构建实例，并通过 unwrap 让测试在初始化失败时立即中断。
    pub async fn create_test_shared_drt_async(
        store_path: &std::path::Path,
    ) -> super::DistributedRuntime {
        use crate::transports::nats;

        let rt = crate::Runtime::from_current().unwrap();
        let discovery_backend = super::DiscoveryBackend::KvStore(
            crate::storage::kv::Selector::File(store_path.to_path_buf()),
        );
        let request_plane = crate::distributed::RequestPlaneMode::default();
        let event_transport_kind = crate::discovery::EventTransportKind::Nats;
        let config = super::DistributedConfig {
            discovery_backend,
            nats_config: Some(nats::ClientOptions::default()),
            request_plane,
            event_transport_kind,
        };

        super::DistributedRuntime::new(rt, config).await.unwrap()
    }
}

// === SECTION: tests ===

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;

    use super::{DiscoveryBackend, DistributedConfig, DistributedRuntime, RequestPlaneMode};
    use crate::servicegroup::get_or_create_routing_occupancy_state;
    use crate::config::environment_names::{
        event_plane as env_event_plane, nats as env_nats, runtime::system as env_system,
        tcp_response_stream as env_tcp_response_stream,
    };
    use crate::discovery::EventTransportKind;
    use crate::metadata_registry::BASE_SUFFIX;
    use crate::metrics::MetricsHierarchy;
    use crate::runtime::Runtime;
    use crate::storage::kv;
    use crate::transports::etcd;

    async fn create_process_local_drt() -> DistributedRuntime {
        let runtime = Runtime::from_current().unwrap();
        DistributedRuntime::new(runtime, DistributedConfig::process_local())
            .await
            .unwrap()
    }

    async fn create_local_drt_with_event_transport(
        event_transport_kind: EventTransportKind,
    ) -> DistributedRuntime {
        let runtime = Runtime::from_current().unwrap();
        DistributedRuntime::new(
            runtime,
            DistributedConfig {
                discovery_backend: DiscoveryBackend::KvStore(kv::Selector::Memory),
                nats_config: None,
                request_plane: RequestPlaneMode::Tcp,
                event_transport_kind,
            },
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_distributed_runtime_accessors_metadata_and_shutdown() {
        temp_env::async_with_vars(
            vec![
                (env_system::PGD_SYSTEM_PORT, Some("-1")),
                (env_tcp_response_stream::PGD_TCP_RESPONSE_STREAM_PORT, None::<&str>),
                (env_tcp_response_stream::PGD_TCP_RESPONSE_STREAM_HOST, None::<&str>),
            ],
            async {
                let drt = create_process_local_drt().await;

                assert_eq!(format!("{drt:?}"), "DistributedRuntime");
                assert_eq!(drt.basename(), "");
                assert!(drt.parent_hierarchies().is_empty());
                assert_eq!(
                    <DistributedRuntime as MetricsHierarchy>::connection_id(&drt),
                    Some(drt.connection_id())
                );
                assert!(std::ptr::eq(drt.runtime(), &drt.runtime));
                assert!(std::ptr::eq(drt.servicegroup_registry(), &drt.servicegroup_registry));
                assert!(Arc::ptr_eq(&drt.system_health(), &drt.system_health));
                assert!(std::ptr::eq(
                    drt.local_portname_registry(),
                    &drt.local_portname_registry,
                ));
                assert!(std::ptr::eq(drt.engine_routes(), &drt.engine_routes));
                assert!(std::ptr::eq(drt.metadata_artifacts(), &drt.metadata_artifacts));
                assert!(std::ptr::eq(drt.get_metrics_registry(), &drt.metrics_registry));
                assert_eq!(drt.default_event_transport_kind(), EventTransportKind::Zmq);

                let discovery = drt.discovery();
                assert!(Arc::ptr_eq(&discovery, &drt.discovery_client));
                assert_eq!(drt.connection_id(), discovery.instance_id());

                let network_manager = drt.network_manager();
                assert!(Arc::ptr_eq(&network_manager, &drt.network_manager));
                assert_eq!(network_manager.mode(), RequestPlaneMode::Tcp);
                assert_eq!(drt.request_plane(), RequestPlaneMode::Tcp);
                assert!(drt.system_status_server_info().is_none());

                let tracker = drt.graceful_shutdown_tracker();
                assert!(Arc::ptr_eq(&tracker, &drt.runtime.graceful_shutdown_tracker()));
                assert_eq!(tracker.get_count(), 0);

                let artifact_path = PathBuf::from("/tmp/runtime-metadata.json");
                drt.metadata_artifacts().register(
                    "model-slug",
                    BASE_SUFFIX,
                    "metadata.json",
                    artifact_path.clone(),
                );
                assert_eq!(
                    drt.metadata_artifacts()
                        .get("model-slug", BASE_SUFFIX, "metadata.json"),
                    Some(artifact_path)
                );
                drt.metadata_artifacts().unregister("model-slug", BASE_SUFFIX);
                assert!(drt.metadata_artifacts().is_empty());

                let primary_token = drt.primary_token();
                let child_token = drt.child_token();
                assert!(!primary_token.is_cancelled());
                assert!(!child_token.is_cancelled());

                drt.shutdown();

                tokio::time::timeout(Duration::from_secs(1), primary_token.cancelled())
                    .await
                    .unwrap();
                tokio::time::timeout(Duration::from_secs(1), child_token.cancelled())
                    .await
                    .unwrap();
            },
        )
        .await;
    }

    #[tokio::test]
    async fn test_system_status_server_info_present_when_enabled() {
        temp_env::async_with_vars(vec![(env_system::PGD_SYSTEM_PORT, Some("0"))], async {
            let drt = create_process_local_drt().await;

            let info1 = drt
                .system_status_server_info()
                .expect("system status server should be available");
            let info2 = drt
                .system_status_server_info()
                .expect("system status server should be cached");

            assert!(Arc::ptr_eq(&info1, &info2));
            assert!(!info1.address().is_empty());
            assert!(!info1.hostname().is_empty());
            assert!(info1.port() > 0);

            drt.shutdown();
        })
        .await;
    }

    #[tokio::test]
    async fn test_namespace_nats_helpers_and_shared_state_without_nats() {
        temp_env::async_with_vars(vec![(env_system::PGD_SYSTEM_PORT, Some("-1"))], async {
            let drt = create_process_local_drt().await;

            let namespace = drt.namespace("valid-name_123").unwrap();
            assert_eq!(namespace.name(), "valid-name_123");

            let servicegroup = namespace.servicegroup("servicegroup").unwrap();
            let portname = servicegroup.portname("portname");

            drt.kv_router_nats_publish("subject".to_string(), Bytes::from_static(b"payload"))
                .await
                .unwrap();

            let request_err = drt
                .kv_router_nats_request(
                    "subject".to_string(),
                    Bytes::from_static(b"payload"),
                    Duration::from_millis(10),
                )
                .await
                .unwrap_err();
            assert!(request_err.to_string().contains("requires NATS"));

            let mut registration_rx = drt.register_nats_service(servicegroup.clone());
            let result = tokio::time::timeout(Duration::from_secs(1), registration_rx.recv())
                .await
                .unwrap();
            assert_eq!(
                result.unwrap(),
                Err("Cannot create NATS service without NATS".to_string())
            );
            assert!(drt.servicegroup_registry().inner.lock().await.services.is_empty());

            let _client1 = portname.client().await.unwrap();
            let sources = drt.portname_discovery_sources();
            let guard = sources.lock().await;
            assert_eq!(guard.len(), 1);
            let source1 = guard.get(&portname).unwrap().upgrade().unwrap();
            drop(guard);

            let _client2 = portname.client().await.unwrap();
            let sources = drt.portname_discovery_sources();
            let guard = sources.lock().await;
            let source2 = guard.get(&portname).unwrap().upgrade().unwrap();
            assert!(Arc::ptr_eq(&source1, &source2));
            drop(guard);

            let state1 = get_or_create_routing_occupancy_state(&portname).await;
            let state2 = get_or_create_routing_occupancy_state(&portname).await;
            assert!(Arc::ptr_eq(&state1, &state2));

            let states = drt.routing_occupancy_states();
            let guard = states.lock().await;
            assert_eq!(guard.len(), 1);
            let state_from_map = guard.get(&portname).unwrap().upgrade().unwrap();
            assert!(Arc::ptr_eq(&state1, &state_from_map));

            drt.shutdown();
        })
        .await;
    }

    #[tokio::test]
    async fn test_register_graceful_task_updates_tracker() {
        temp_env::async_with_vars(vec![(env_system::PGD_SYSTEM_PORT, Some("-1"))], async {
            let drt = create_process_local_drt().await;
            let tracker = drt.graceful_shutdown_tracker();

            assert_eq!(tracker.get_count(), 0);
            let guard = drt.register_graceful_task();
            assert_eq!(tracker.get_count(), 1);
            drop(guard);
            assert_eq!(tracker.get_count(), 0);

            drt.shutdown();
        })
        .await;
    }

    #[tokio::test]
    async fn test_request_plane_server_delegates_and_is_cached() {
        temp_env::async_with_vars(vec![(env_system::PGD_SYSTEM_PORT, Some("-1"))], async {
            let drt = create_process_local_drt().await;

            let server1 = drt.request_plane_server().await.unwrap();
            let server2 = drt.request_plane_server().await.unwrap();
            let manager_server = drt.network_manager().server().await.unwrap();

            assert!(Arc::ptr_eq(&server1, &server2));
            assert!(Arc::ptr_eq(&server1, &manager_server));

            drt.shutdown();
        })
        .await;
    }

    #[tokio::test]
    async fn test_tcp_server_is_cached() {
        temp_env::async_with_vars(
            vec![
                (env_system::PGD_SYSTEM_PORT, Some("-1")),
                (env_tcp_response_stream::PGD_TCP_RESPONSE_STREAM_PORT, Some("0")),
                (env_tcp_response_stream::PGD_TCP_RESPONSE_STREAM_HOST, None::<&str>),
            ],
            async {
                let drt = create_process_local_drt().await;

                let server1 = drt.tcp_server().await.unwrap();
                let server2 = drt.tcp_server().await.unwrap();

                assert!(Arc::ptr_eq(&server1, &server2));

                drt.shutdown();
            },
        )
        .await;
    }

    #[tokio::test]
    async fn test_tcp_server_rejects_invalid_port_env() {
        temp_env::async_with_vars(
            vec![
                (env_system::PGD_SYSTEM_PORT, Some("-1")),
                (env_tcp_response_stream::PGD_TCP_RESPONSE_STREAM_PORT, Some("invalid")),
                (env_tcp_response_stream::PGD_TCP_RESPONSE_STREAM_HOST, None::<&str>),
            ],
            async {
                let drt = create_process_local_drt().await;
                let err = match drt.tcp_server().await {
                    Ok(_) => panic!("tcp_server should reject an invalid port env"),
                    Err(err) => err,
                };
                assert!(err.to_string().contains(env_tcp_response_stream::PGD_TCP_RESPONSE_STREAM_PORT));
                drt.shutdown();
            },
        )
        .await;
    }

    #[tokio::test]
    async fn test_metrics_scrape_refreshes_uptime_gauge() {
        fn uptime_value(metrics: &str) -> f64 {
            metrics
                .lines()
                .find(|line| {
                    line.starts_with("pagoda_servicegroup_uptime_seconds") && !line.starts_with('#')
                })
                .and_then(|line| line.split_whitespace().last())
                .unwrap()
                .parse::<f64>()
                .unwrap()
        }

        temp_env::async_with_vars(vec![(env_system::PGD_SYSTEM_PORT, Some("-1"))], async {
            let drt = create_process_local_drt().await;

            let metrics1 = drt.metrics().prometheus_expfmt().unwrap();
            assert!(metrics1.contains("# HELP pagoda_servicegroup_uptime_seconds"));
            assert!(metrics1.contains("# TYPE pagoda_servicegroup_uptime_seconds gauge"));

            tokio::time::sleep(Duration::from_millis(20)).await;

            let metrics2 = drt.metrics().prometheus_expfmt().unwrap();
            let uptime1 = uptime_value(&metrics1);
            let uptime2 = uptime_value(&metrics2);

            assert!(uptime2 >= uptime1);
            assert!(uptime2 > 0.0);

            drt.shutdown();
        })
        .await;
    }

    #[tokio::test]
    async fn test_distributed_runtime_from_settings_uses_environment() {
        temp_env::async_with_vars(
            vec![
                ("PGD_DISCOVERY_BACKEND", Some("mem")),
                ("PGD_REQUEST_PLANE", Some("http")),
                (env_event_plane::PGD_EVENT_PLANE, None::<&str>),
                (env_nats::NATS_SERVER, None::<&str>),
                (env_system::PGD_SYSTEM_PORT, Some("-1")),
            ],
            async {
                let runtime = Runtime::from_current().unwrap();
                let drt = DistributedRuntime::from_settings(runtime).await.unwrap();

                assert_eq!(drt.request_plane(), RequestPlaneMode::Http);
                assert_eq!(drt.network_manager().mode(), RequestPlaneMode::Http);
                assert!(drt.system_status_server_info().is_none());
                assert_eq!(drt.default_event_transport_kind(), EventTransportKind::Zmq);

                drt.shutdown();
            },
        )
        .await;
    }

    #[tokio::test]
    async fn test_new_preserves_explicit_event_transport_kind() {
        temp_env::async_with_vars(vec![(env_system::PGD_SYSTEM_PORT, Some("-1"))], async {
            let drt = create_local_drt_with_event_transport(EventTransportKind::Nats).await;
            assert_eq!(drt.default_event_transport_kind(), EventTransportKind::Nats);
            drt.shutdown();
        })
        .await;
    }

    #[test]
    fn test_discovery_backend_locality_and_event_transport_resolution() {
        let file_backend = DiscoveryBackend::KvStore(kv::Selector::File(PathBuf::from("/tmp")));
        let memory_backend = DiscoveryBackend::KvStore(kv::Selector::Memory);
        let etcd_backend = DiscoveryBackend::KvStore(kv::Selector::Etcd(Box::new(
            etcd::ClientOptions {
                attach_lease: false,
                ..Default::default()
            },
        )));

        assert!(file_backend.is_local());
        assert!(memory_backend.is_local());
        assert!(!etcd_backend.is_local());
        assert!(!DiscoveryBackend::Kubernetes.is_local());

        temp_env::with_vars(vec![(env_event_plane::PGD_EVENT_PLANE, None::<&str>)], || {
            assert_eq!(file_backend.resolve_event_transport_kind(), EventTransportKind::Zmq);
            assert_eq!(memory_backend.resolve_event_transport_kind(), EventTransportKind::Zmq);
            assert_eq!(etcd_backend.resolve_event_transport_kind(), EventTransportKind::Nats);
            assert_eq!(
                DiscoveryBackend::Kubernetes.resolve_event_transport_kind(),
                EventTransportKind::Nats
            );
        });

        temp_env::with_vars(vec![(env_event_plane::PGD_EVENT_PLANE, Some("nats"))], || {
            assert_eq!(memory_backend.resolve_event_transport_kind(), EventTransportKind::Nats);
        });

        temp_env::with_vars(vec![(env_event_plane::PGD_EVENT_PLANE, Some("zmq"))], || {
            assert_eq!(etcd_backend.resolve_event_transport_kind(), EventTransportKind::Zmq);
        });

        temp_env::with_vars(vec![(env_event_plane::PGD_EVENT_PLANE, Some(""))], || {
            assert_eq!(memory_backend.resolve_event_transport_kind(), EventTransportKind::Zmq);
            assert_eq!(etcd_backend.resolve_event_transport_kind(), EventTransportKind::Nats);
        });

        temp_env::with_vars(vec![(env_event_plane::PGD_EVENT_PLANE, Some("invalid"))], || {
            assert_eq!(memory_backend.resolve_event_transport_kind(), EventTransportKind::Zmq);
            assert_eq!(etcd_backend.resolve_event_transport_kind(), EventTransportKind::Nats);
        });
    }

    #[test]
    fn test_distributed_config_from_settings_processes_env() {
        temp_env::with_vars(
            vec![
                ("PGD_DISCOVERY_BACKEND", Some("mem")),
                ("PGD_REQUEST_PLANE", Some("tcp")),
                (env_event_plane::PGD_EVENT_PLANE, None::<&str>),
                (env_nats::NATS_SERVER, None::<&str>),
            ],
            || {
                let config = DistributedConfig::from_settings();
                assert!(matches!(
                    config.discovery_backend,
                    DiscoveryBackend::KvStore(kv::Selector::Memory)
                ));
                assert!(config.nats_config.is_none());
                assert_eq!(config.request_plane, RequestPlaneMode::Tcp);
                assert_eq!(config.event_transport_kind, EventTransportKind::Zmq);
            },
        );

        temp_env::with_vars(
            vec![
                ("PGD_DISCOVERY_BACKEND", Some("kubernetes")),
                ("PGD_REQUEST_PLANE", Some("http")),
                (env_event_plane::PGD_EVENT_PLANE, None::<&str>),
                (env_nats::NATS_SERVER, Some("nats://example:4222")),
            ],
            || {
                let config = DistributedConfig::from_settings();
                assert!(matches!(config.discovery_backend, DiscoveryBackend::Kubernetes));
                assert!(config.nats_config.is_some());
                assert_eq!(config.request_plane, RequestPlaneMode::Http);
                assert_eq!(config.event_transport_kind, EventTransportKind::Nats);
            },
        );

        temp_env::with_vars(
            vec![
                ("PGD_DISCOVERY_BACKEND", Some("mem")),
                ("PGD_REQUEST_PLANE", Some("tcp")),
                (env_event_plane::PGD_EVENT_PLANE, Some("nats")),
                (env_nats::NATS_SERVER, None::<&str>),
            ],
            || {
                let config = DistributedConfig::from_settings();
                assert!(matches!(
                    config.discovery_backend,
                    DiscoveryBackend::KvStore(kv::Selector::Memory)
                ));
                assert!(config.nats_config.is_some());
                assert_eq!(config.request_plane, RequestPlaneMode::Tcp);
                assert_eq!(config.event_transport_kind, EventTransportKind::Nats);
            },
        );

        temp_env::with_vars(
            vec![
                ("PGD_DISCOVERY_BACKEND", Some("mem")),
                ("PGD_REQUEST_PLANE", Some("tcp")),
                (env_event_plane::PGD_EVENT_PLANE, Some("invalid")),
                (env_nats::NATS_SERVER, None::<&str>),
            ],
            || {
                let config = DistributedConfig::from_settings();
                assert!(config.nats_config.is_none());
                assert_eq!(config.event_transport_kind, EventTransportKind::Zmq);
            },
        );
    }

    #[test]
    fn test_distributed_config_convenience_constructors() {
        let process_local = DistributedConfig::process_local();
        assert!(matches!(
            process_local.discovery_backend,
            DiscoveryBackend::KvStore(kv::Selector::Memory)
        ));
        assert!(process_local.nats_config.is_none());
        assert_eq!(process_local.request_plane, RequestPlaneMode::Tcp);
        assert_eq!(process_local.event_transport_kind, EventTransportKind::Zmq);

        temp_env::with_vars(
            vec![
                ("PGD_REQUEST_PLANE", Some("tcp")),
                (env_event_plane::PGD_EVENT_PLANE, None::<&str>),
                (env_nats::NATS_SERVER, None::<&str>),
            ],
            || {
                let config = DistributedConfig::for_cli();
                match config.discovery_backend {
                    DiscoveryBackend::KvStore(kv::Selector::Etcd(etcd_config)) => {
                        assert!(!etcd_config.attach_lease)
                    }
                    _ => panic!("expected etcd discovery backend"),
                }
                assert!(config.nats_config.is_some());
                assert_eq!(config.request_plane, RequestPlaneMode::Tcp);
                assert_eq!(config.event_transport_kind, EventTransportKind::Nats);
            },
        );

        temp_env::with_vars(
            vec![
                ("PGD_REQUEST_PLANE", Some("tcp")),
                (env_event_plane::PGD_EVENT_PLANE, Some("zmq")),
                (env_nats::NATS_SERVER, None::<&str>),
            ],
            || {
                let config = DistributedConfig::for_cli();
                assert!(config.nats_config.is_none());
                assert_eq!(config.request_plane, RequestPlaneMode::Tcp);
                assert_eq!(config.event_transport_kind, EventTransportKind::Zmq);
            },
        );
    }

    #[test]
    fn test_distributed_config_from_settings_rejects_unknown_backend() {
        temp_env::with_vars(
            vec![
                ("PGD_DISCOVERY_BACKEND", Some("unknown")),
                ("PGD_REQUEST_PLANE", Some("tcp")),
                (env_event_plane::PGD_EVENT_PLANE, None::<&str>),
                (env_nats::NATS_SERVER, None::<&str>),
            ],
            || {
                let panic = std::panic::catch_unwind(DistributedConfig::from_settings);
                assert!(panic.is_err());
            },
        );
    }

    #[test]
    fn test_request_plane_mode_parsing_display_and_env() {
        assert_eq!(RequestPlaneMode::default(), RequestPlaneMode::Tcp);
        assert_eq!("nats".parse::<RequestPlaneMode>().unwrap(), RequestPlaneMode::Nats);
        assert_eq!("http".parse::<RequestPlaneMode>().unwrap(), RequestPlaneMode::Http);
        assert_eq!("tcp".parse::<RequestPlaneMode>().unwrap(), RequestPlaneMode::Tcp);
        assert_eq!("NATS".parse::<RequestPlaneMode>().unwrap(), RequestPlaneMode::Nats);
        assert_eq!("HTTP".parse::<RequestPlaneMode>().unwrap(), RequestPlaneMode::Http);
        assert_eq!("TCP".parse::<RequestPlaneMode>().unwrap(), RequestPlaneMode::Tcp);
        assert!("invalid".parse::<RequestPlaneMode>().is_err());

        assert_eq!(RequestPlaneMode::Nats.to_string(), "nats");
        assert_eq!(RequestPlaneMode::Http.to_string(), "http");
        assert_eq!(RequestPlaneMode::Tcp.to_string(), "tcp");
        assert!(RequestPlaneMode::Nats.is_nats());
        assert!(!RequestPlaneMode::Http.is_nats());
        assert!(!RequestPlaneMode::Tcp.is_nats());

        temp_env::with_vars(vec![("PGD_REQUEST_PLANE", None::<&str>)], || {
            assert_eq!(RequestPlaneMode::from_env(), RequestPlaneMode::Tcp);
        });

        temp_env::with_vars(vec![("PGD_REQUEST_PLANE", Some("NATS"))], || {
            assert_eq!(RequestPlaneMode::from_env(), RequestPlaneMode::Nats);
        });

        temp_env::with_vars(vec![("PGD_REQUEST_PLANE", Some("invalid"))], || {
            assert_eq!(RequestPlaneMode::from_env(), RequestPlaneMode::Tcp);
        });
    }

    // === SECTION: 合并自原 mod tests（integration feature gated）===
    #[cfg(feature = "integration")]
    mod request_plane_integration {
        use super::super::RequestPlaneMode;
        use super::super::distributed_test_utils::create_test_drt_async;

        #[tokio::test]
        // 中文说明：
        // 1. 这个测试验证在系统状态 HTTP 服务关闭时，DistributedRuntime 的 uptime 统计仍然会正常增长。
        // 2. 测试先把 PGD_SYSTEM_PORT 临时清空，确保系统状态服务不会启动。
        // 3. 然后创建一个测试用 DRT，等待 50 毫秒，让 uptime 有足够时间累积。
        // 4. 接着从 system_health 中读取当前 uptime，并断言它至少不小于等待时长。
        // 5. 如果断言通过，再打印一条成功日志，帮助定位测试运行时的实际 uptime 数值。
        async fn test_drt_uptime_after_delay_system_disabled() {
            use crate::config::environment_names::runtime::system as env_system;
            let wait_duration = tokio::time::Duration::from_millis(50);

            temp_env::async_with_vars([(env_system::PGD_SYSTEM_PORT, None::<&str>)], async {
                let drt = create_test_drt_async().await;

                tokio::time::sleep(wait_duration).await;

                let uptime = drt.system_health.lock().uptime();
                assert!(
                    uptime >= wait_duration,
                    "Expected uptime to be at least 50ms, but got {:?}",
                    uptime
                );

                println!(
                    "✓ DRT uptime test passed (system disabled): uptime = {:?}",
                    uptime
                );
            })
            .await;
        }

        #[tokio::test]
        // 中文说明：
        // 1. 这个测试验证在系统状态 HTTP 服务开启时，DistributedRuntime 的 uptime 统计也会正常增长。
        // 2. 测试先把 PGD_SYSTEM_PORT 临时设置成 8081，让系统状态服务进入启用路径。
        // 3. 随后创建测试 DRT，等待 50 毫秒，使 uptime 累积出可观测值。
        // 4. 然后读取 system_health 中的 uptime，并断言它至少达到等待时间，证明启动额外服务没有破坏 uptime 统计。
        // 5. 最后打印成功信息，方便在测试日志里区分“启用系统服务”这一分支的执行结果。
        async fn test_drt_uptime_after_delay_system_enabled() {
            use crate::config::environment_names::runtime::system as env_system;
            let wait_duration = tokio::time::Duration::from_millis(50);

            temp_env::async_with_vars([(env_system::PGD_SYSTEM_PORT, Some("8081"))], async {
                let drt = create_test_drt_async().await;

                tokio::time::sleep(wait_duration).await;

                let uptime = drt.system_health.lock().uptime();
                assert!(
                    uptime >= wait_duration,
                    "Expected uptime to be at least 50ms, but got {:?}",
                    uptime
                );

                println!(
                    "✓ DRT uptime test passed (system enabled): uptime = {:?}",
                    uptime
                );
            })
            .await;
        }

        #[test]
        // 中文说明：
        // 1. 这个测试验证 RequestPlaneMode::from_str 对合法字符串和非法字符串的解析行为。
        // 2. 测试先准备一组大小写混合的合法输入及期望枚举值，覆盖 nats、http、tcp 三种模式。
        // 3. 然后逐个遍历这些用例，调用 parse 并断言解析结果与期望一致，确认大小写不影响行为。
        // 4. 最后再对一个非法字符串执行 parse，并断言结果为错误，确保无效配置不会被静默接受。
        fn test_request_plane_mode_from_str() {
            let valid_cases = [
                ("nats", RequestPlaneMode::Nats),
                ("http", RequestPlaneMode::Http),
                ("tcp", RequestPlaneMode::Tcp),
                ("NATS", RequestPlaneMode::Nats),
                ("HTTP", RequestPlaneMode::Http),
                ("TCP", RequestPlaneMode::Tcp),
            ];

            for (input, expected) in valid_cases {
                assert_eq!(input.parse::<RequestPlaneMode>().unwrap(), expected);
            }

            assert!("invalid".parse::<RequestPlaneMode>().is_err());
        }

        #[test]
        // 中文说明：
        // 1. 这个测试验证 RequestPlaneMode 的 Display 实现是否会输出预期的小写字符串。
        // 2. 测试先准备每个枚举值与期望字符串之间的对应关系。
        // 3. 然后逐个遍历这些用例，调用 to_string 并断言输出值完全匹配。
        // 4. 如果全部通过，就说明日志、配置回显和其它依赖 Display 的场景都会得到稳定一致的文本结果。
        fn test_request_plane_mode_display() {
            let render_cases = [
                (RequestPlaneMode::Nats, "nats"),
                (RequestPlaneMode::Http, "http"),
                (RequestPlaneMode::Tcp, "tcp"),
            ];

            for (mode, expected) in render_cases {
                assert_eq!(mode.to_string(), expected);
            }
        }
    }
}


// cargo test -p pagoda-runtime distributed --lib --features integration