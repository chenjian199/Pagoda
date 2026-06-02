// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::network::manager` —— 出站连接管理与多传输协调器
//!
//! ## 设计意图
//! `EgressConnectionManager` 在 egress 侧持有多个传输客户端（TCP/HTTP/NATS）与一个统一的
//! `ResponseService`，根据被调用端点声明的 transport 选择正确的 `RequestPlaneClient`，
//! 并把响应回流挂载到本进程的 response service 上。
//!
//! ## 外部契约
//! - 公开类型 / 方法集合严格一致；构造器 `new` 与 transport selection
//!   逻辑是契约面，不可重写为 enum match。
//! - `Drop` 实现里 `cancellation_token.cancel()` + log 是契约：上层依赖该日志做
//!   优雅退出时序判断。
//!
//! ## 实现要点
//! - 本文件只承载"管理"职责，不做协议序列化；序列化在 `codec.rs` / `egress::*` / `ingress::*` 中。

//! 网络管理器 - 网络配置与创建逻辑的唯一来源
//!
//! 这个模块汇总了所有与网络相关的配置和创建逻辑。
//! 它是代码库里唯一负责以下事情的地方：
//! - 读取网络配置所需的环境变量；
//! - 了解各类传输专用类型（SharedHttpServer、TcpRequestClient 等）；
//! - 根据 RequestPlaneMode 选择工作模式；
//! - 创建服务端和客户端。
//!
//! 代码库其余部分只与 trait object 交互，不直接访问任何传输实现或配置。

use super::egress::unified_client::RequestPlaneClient;
use super::ingress::shared_tcp_endpoint::SharedTcpServer;
use super::ingress::unified_server::RequestPlaneServer;
use crate::distributed::RequestPlaneMode;
use anyhow::Result;
use async_once_cell::OnceCell;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio_util::sync::CancellationToken;

// === SECTION: 进程范围全局状态（端口、共享服务端、取消令牌）===
/// 绑定后实际 TCP RPC 端口的全局存储。
/// 使用 OnceLock，因为服务端绑定后端口只会设置一次，之后不会再变。
static ACTUAL_TCP_RPC_PORT: OnceLock<u16> = OnceLock::new();

/// 绑定后实际 HTTP RPC 端口的全局存储。
/// 使用 OnceLock，因为服务端绑定后端口只会设置一次，之后不会再变。
static ACTUAL_HTTP_RPC_PORT: OnceLock<u16> = OnceLock::new();

/// 共享 TCP 服务端实例的全局存储。
///
/// 当多个 worker 运行在同一进程中时，它们必须共享同一个 TCP 服务端，
/// 才能确保所有 portname 都注册到同一台服务端上。否则每个 worker 都会
/// 在不同端口上创建自己的服务端，但 discovery 里发布的却仍然是同一个端口
/// （来自 ACTUAL_TCP_RPC_PORT），最终会导致 "No handler found" 错误。
///
/// 使用 `tokio::sync::OnceCell` 以支持异步初始化（绑定 TCP socket）。
static GLOBAL_TCP_SERVER: tokio::sync::OnceCell<Arc<SharedTcpServer>> =
    tokio::sync::OnceCell::const_new();

/// 共享 HTTP 服务端实例的全局存储。
///
/// 原因与 GLOBAL_TCP_SERVER 相同：同一进程中的多个 worker 必须共享同一个 HTTP 服务端，
/// 才能让所有 portname 注册到同一个端口上。
static GLOBAL_HTTP_SERVER: tokio::sync::OnceCell<
    Arc<super::ingress::http_endpoint::SharedHttpServer>,
> = tokio::sync::OnceCell::const_new();

/// 全局 TCP 服务端使用的进程范围取消令牌。
///
/// 这个令牌独立于任何单个 runtime 的取消令牌，这样 servicegroup 的 Drop 实现
/// （例如 KvRouter::drop → cancel）就不会在 OnceCell 仍向后续 runtime 返回
/// （已经失效的）服务端时，把共享的 accept 循环提前杀掉。
static GLOBAL_TCP_SERVER_TOKEN: std::sync::LazyLock<CancellationToken> =
    std::sync::LazyLock::new(CancellationToken::new);

/// 全局 HTTP 服务端使用的进程范围取消令牌。
static GLOBAL_HTTP_SERVER_TOKEN: std::sync::LazyLock<CancellationToken> =
    std::sync::LazyLock::new(CancellationToken::new);

// === SECTION: 公开 RPC 端口访问器 ===
/// 获取服务端当前监听的真实 TCP RPC 端口。
pub fn get_actual_tcp_rpc_port() -> anyhow::Result<u16> {
    ACTUAL_TCP_RPC_PORT.get().copied().ok_or_else(|| {
        tracing::error!(
            "TCP RPC port not set - request_plane_server() must be called before get_actual_tcp_rpc_port()"
        );
        anyhow::anyhow!(
            "TCP RPC port not initialized. This is not expected."
        )
    })
}

/// 设置真实 TCP RPC 端口（在服务端绑定后内部调用）。
fn set_actual_tcp_rpc_port(port: u16) {
    if let Err(existing) = ACTUAL_TCP_RPC_PORT.set(port) {
        tracing::warn!(
            existing_port = existing,
            new_port = port,
            "TCP RPC port already set, ignoring new value"
        );
    }
}

/// 获取服务端当前监听的真实 HTTP RPC 端口。
pub fn get_actual_http_rpc_port() -> anyhow::Result<u16> {
    ACTUAL_HTTP_RPC_PORT.get().copied().ok_or_else(|| {
        tracing::error!(
            "HTTP RPC port not set - request_plane_server() must be called before get_actual_http_rpc_port()"
        );
        anyhow::anyhow!(
            "HTTP RPC port not initialized. This is not expected."
        )
    })
}

/// 设置真实 HTTP RPC 端口（在服务端绑定后内部调用）。
fn set_actual_http_rpc_port(port: u16) {
    if let Err(existing) = ACTUAL_HTTP_RPC_PORT.set(port) {
        tracing::warn!(
            existing_port = existing,
            new_port = port,
            "HTTP RPC port already set, ignoring new value"
        );
    }
}

// === SECTION: 网络配置（从环境变量加载）===
/// 从环境变量加载的网络配置。
#[derive(Clone)]
struct NetworkConfig {
    // HTTP 服务端配置
    http_host: String,
    /// 要绑定的 HTTP 端口。如果为 None，则由操作系统分配空闲端口。
    http_port: Option<u16>,
    http_rpc_root: String,

    // TCP 服务端配置
    tcp_host: String,
    /// 要绑定的 TCP 端口。如果为 None，则由操作系统分配空闲端口。
    tcp_port: Option<u16>,

    // HTTP 客户端配置
    http_client_config: super::egress::http_router::Http2Config,

    // TCP 客户端配置
    tcp_client_config: super::egress::tcp_client::TcpRequestConfig,

    // NATS 配置（由外部提供，不从环境变量读取）
    nats_client: Option<async_nats::Client>,
}

impl NetworkConfig {
    /// 从环境变量加载配置。
    ///
    /// 这里是唯一读取网络相关环境变量的地方。
    fn from_env(nats_client: Option<async_nats::Client>) -> Self {
        Self {
            // HTTP 服务端配置
            // 如果设置了 PGD_HTTP_RPC_PORT，就使用该端口；否则 None 表示由操作系统分配空闲端口。
            http_host: std::env::var("PGD_HTTP_RPC_HOST")
                .unwrap_or_else(|_| crate::utils::get_http_rpc_host_from_env()),
            http_port: std::env::var("PGD_HTTP_RPC_PORT")
                .ok()
                .and_then(|p| p.parse().ok()),
            http_rpc_root: std::env::var("PGD_HTTP_RPC_ROOT_PATH")
                .unwrap_or_else(|_| "/v1/rpc".to_string()),

            // TCP 服务端配置
            // 如果设置了 PGD_TCP_RPC_PORT，就使用该端口；否则 None 表示由操作系统分配空闲端口。
            tcp_host: std::env::var("PGD_TCP_RPC_HOST")
                .unwrap_or_else(|_| crate::utils::get_tcp_rpc_host_from_env()),
            tcp_port: std::env::var("PGD_TCP_RPC_PORT")
                .ok()
                .and_then(|p| p.parse().ok()),

            // HTTP 客户端配置（读取 PGD_HTTP2_* 环境变量）
            http_client_config: super::egress::http_router::Http2Config::from_env(),

            // TCP 客户端配置（读取 PGD_TCP_* 环境变量）
            tcp_client_config: super::egress::tcp_client::TcpRequestConfig::from_env(),

            // NATS（外部传入）
            nats_client,
        }
    }
}

/// 网络管理器 - 所有网络资源的中心协调器
///
/// # 职责
///
/// 1. **配置管理**：读取并管理所有网络相关环境变量
/// 2. **服务端创建**：根据模式创建并启动 request plane 服务端
/// 3. **客户端创建**：按需创建 request plane 客户端
/// 4. **抽象隔离**：对代码库其余部分隐藏所有传输专用细节
///
/// # 设计原则
///
/// - **唯一事实来源**：所有网络配置和创建逻辑都放在这里
/// - **懒加载初始化**：服务端只在首次访问时创建
/// - **与传输无关的接口**：只向调用方暴露 trait object
/// - **不泄漏抽象**：传输类型不会逃出这个模块
///
/// # 示例
///
/// ```ignore
/// // 创建管理器（通常只在 DistributedRuntime 中做一次）
/// let manager = NetworkManager::new(cancel_token, nats_client, servicegroup_registry, request_plane_mode);
///
/// // 获取服务端（懒加载并缓存）
/// let server = manager.server().await?;
/// server.register_portname(...).await?;
///
/// // 创建客户端（不缓存，开销较轻）
/// let client = manager.create_client()?;
/// client.send_request(...).await?;
/// ```
// === SECTION: 网络管理器（公开协调器）===
pub struct NetworkManager {
    mode: RequestPlaneMode,
    config: NetworkConfig,
    server: Arc<OnceCell<Arc<dyn RequestPlaneServer>>>,
    cancellation_token: CancellationToken,
    servicegroup_registry: crate::servicegroup::Registry,
}

impl NetworkManager {
    /// 创建一个新的网络管理器。
    ///
    /// 这是 NetworkManager 的唯一构造入口。所有配置都在内部从环境变量加载。
    ///
    /// # 参数
    ///
    /// * `cancellation_token` - 用于服务端优雅关闭的令牌
    /// * `nats_client` - 可选的 NATS 客户端（仅在 NATS 模式下需要）
    /// * `servicegroup_registry` - 用来获取 NATS servicegroup 的 ServiceGroup 注册表
    ///
    /// # 返回值
    ///
    /// 返回一个可用于创建服务端和客户端的 Arc 包裹 NetworkManager。
    pub fn new(
        cancellation_token: CancellationToken,
        nats_client: Option<async_nats::Client>,
        servicegroup_registry: crate::servicegroup::Registry,
        mode: RequestPlaneMode,
    ) -> Self {
        let config = NetworkConfig::from_env(nats_client);

        match mode {
            RequestPlaneMode::Http => {
                let port_display = config
                    .http_port
                    .map(|p| p.to_string())
                    .unwrap_or_else(|| "OS-assigned".to_string());
                tracing::info!(
                    %mode,
                    host = %config.http_host,
                    port = %port_display,
                    rpc_root = %config.http_rpc_root,
                    "Initializing NetworkManager with HTTP request plane"
                );
            }
            RequestPlaneMode::Tcp => {
                let port_display = config
                    .tcp_port
                    .map(|p| p.to_string())
                    .unwrap_or_else(|| "OS-assigned".to_string());
                tracing::info!(
                    %mode,
                    host = %config.tcp_host,
                    port = %port_display,
                    "Initializing NetworkManager with TCP request plane"
                );
            }
            RequestPlaneMode::Nats => {
                tracing::info!(
                    %mode,
                    "Initializing NetworkManager with NATS request plane"
                );
            }
        }

        Self {
            mode,
            config,
            server: Arc::new(OnceCell::new()),
            cancellation_token,
            servicegroup_registry,
        }
    }

    /// 获取或创建 request plane 服务端。
    ///
    /// 服务端会在首次访问时懒加载创建，并缓存供后续调用使用。
    /// 服务端会自动在后台启动。
    ///
    /// # 返回值
    ///
    /// 返回一个抽象 HTTP/TCP/NATS 实现的 trait object。
    ///
    /// # 错误
    ///
    /// 可能返回错误的情况：
    /// - 服务端创建失败（例如端口已被占用）
    /// - 选择了 NATS 模式但没有可用的 NATS 客户端
    /// - 配置无效（例如绑定地址格式错误）
    pub async fn server(&self) -> Result<Arc<dyn RequestPlaneServer>> {
        let server = self
            .server
            .get_or_try_init(async { self.create_server().await })
            .await?;

        Ok(server.clone())
    }

    /// 创建一个新的 request plane 客户端。
    ///
    /// 客户端开销较轻且不会缓存，每次调用都会创建一个新的客户端实例。
    ///
    /// # 返回值
    ///
    /// 返回一个抽象 HTTP/TCP/NATS 实现的 trait object。
    ///
    /// # 错误
    ///
    /// 可能返回错误的情况：
    /// - 客户端创建失败（例如配置无效）
    /// - 选择了 NATS 模式但没有可用的 NATS 客户端
    pub fn create_client(&self) -> Result<Arc<dyn RequestPlaneClient>> {
        match self.mode {
            RequestPlaneMode::Http => self.create_http_client(),
            RequestPlaneMode::Tcp => self.create_tcp_client(),
            RequestPlaneMode::Nats => self.create_nats_client(),
        }
    }

    /// 获取当前 request plane 模式。
    ///
    /// 这个接口主要用于日志和调试。
    /// 业务逻辑不应基于模式分支处理，而应使用 trait object。
    pub fn mode(&self) -> RequestPlaneMode {
        self.mode
    }

    // ============================================================================
    // PRIVATE: Server Creation
    // ============================================================================

    async fn create_server(&self) -> Result<Arc<dyn RequestPlaneServer>> {
        match self.mode {
            RequestPlaneMode::Http => self.create_http_server().await,
            RequestPlaneMode::Tcp => self.create_tcp_server().await,
            RequestPlaneMode::Nats => self.create_nats_server().await,
        }
    }

    async fn create_http_server(&self) -> Result<Arc<dyn RequestPlaneServer>> {
        use super::ingress::http_endpoint::SharedHttpServer;

        // 使用全局 HTTP 服务端，确保同一进程中的所有 worker 共享同一个服务端。
        // 这对正确的 portname 路由至关重要。
        let server = GLOBAL_HTTP_SERVER
            .get_or_try_init(|| async {
                // 如果指定了配置端口就使用它，否则使用 0（由操作系统分配空闲端口）。
                let port = self.config.http_port.unwrap_or(0);
                let bind_addr = format!("{}:{}", self.config.http_host, port)
                    .parse()
                    .map_err(|e| anyhow::anyhow!("Invalid HTTP bind address: {}", e))?;

                tracing::info!(
                    bind_addr = %bind_addr,
                    port_source = if self.config.http_port.is_some() { "PGD_HTTP_RPC_PORT" } else { "OS-assigned" },
                    rpc_root = %self.config.http_rpc_root,
                    "Creating HTTP request plane server"
                );

                let server = SharedHttpServer::new(bind_addr, GLOBAL_HTTP_SERVER_TOKEN.clone());

                // 绑定并启动服务端，获取真实绑定地址。
                let actual_addr = server.clone().bind_and_start().await?;

                // 将真实绑定端口存到全局，方便 build_transport_type() 读取。
                set_actual_http_rpc_port(actual_addr.port());

                tracing::info!(
                    actual_addr = %actual_addr,
                    actual_port = actual_addr.port(),
                    "HTTP request plane server started"
                );

                Ok::<_, anyhow::Error>(server)
            })
            .await?;

        Ok(server.clone() as Arc<dyn RequestPlaneServer>)
    }

    async fn create_tcp_server(&self) -> Result<Arc<dyn RequestPlaneServer>> {
        // 使用全局 TCP 服务端，确保同一进程中的所有 worker 共享同一个服务端。
        // 这对正确的 portname 路由至关重要。
        let server = GLOBAL_TCP_SERVER
            .get_or_try_init(|| async {
                // 如果指定了配置端口就使用它，否则使用 0（由操作系统分配空闲端口）。
                let port = self.config.tcp_port.unwrap_or(0);
                let bind_addr = format!("{}:{}", self.config.tcp_host, port)
                    .parse()
                    .map_err(|e| anyhow::anyhow!("Invalid TCP bind address: {}", e))?;

                tracing::info!(
                    bind_addr = %bind_addr,
                    port_source = if self.config.tcp_port.is_some() { "PGD_TCP_RPC_PORT" } else { "OS-assigned" },
                    "Creating TCP request plane server"
                );

                let server = SharedTcpServer::new(bind_addr, GLOBAL_TCP_SERVER_TOKEN.clone());

                // 绑定并启动服务端，获取真实绑定地址。
                let actual_addr = server.clone().bind_and_start().await?;

                // 将真实绑定端口存到全局，方便 build_transport_type() 读取。
                set_actual_tcp_rpc_port(actual_addr.port());

                tracing::info!(
                    actual_addr = %actual_addr,
                    actual_port = actual_addr.port(),
                    "TCP request plane server started"
                );

                Ok::<_, anyhow::Error>(server)
            })
            .await?;

        Ok(server.clone() as Arc<dyn RequestPlaneServer>)
    }

    async fn create_nats_server(&self) -> Result<Arc<dyn RequestPlaneServer>> {
        use super::ingress::nats_server::NatsMultiplexedServer;

        let nats_client = self
            .config
            .nats_client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("NATS client required for NATS mode"))?;

        tracing::info!("Creating NATS request plane server");

        Ok(NatsMultiplexedServer::new(
            nats_client.clone(),
            self.servicegroup_registry.clone(),
            self.cancellation_token.clone(),
        ) as Arc<dyn RequestPlaneServer>)
    }

    // ============================================================================
    // PRIVATE: Client Creation
    // ============================================================================

    fn create_http_client(&self) -> Result<Arc<dyn RequestPlaneClient>> {
        use super::egress::http_router::HttpRequestClient;

        tracing::debug!("Creating HTTP request plane client with config from NetworkManager");
        Ok(Arc::new(HttpRequestClient::with_config(
            self.config.http_client_config.clone(),
        )?))
    }

    fn create_tcp_client(&self) -> Result<Arc<dyn RequestPlaneClient>> {
        use super::egress::tcp_client::TcpRequestClient;

        tracing::debug!("Creating TCP request plane client with config from NetworkManager");
        Ok(Arc::new(TcpRequestClient::with_config(
            self.config.tcp_client_config.clone(),
        )?))
    }

    fn create_nats_client(&self) -> Result<Arc<dyn RequestPlaneClient>> {
        use super::egress::nats_client::NatsRequestClient;

        let nats_client = self
            .config
            .nats_client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("NATS client required for NATS mode"))?;

        tracing::debug!("Creating NATS request plane client");
        Ok(Arc::new(NatsRequestClient::new(nats_client.clone())))
    }
}
