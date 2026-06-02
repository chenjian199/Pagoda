// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `servicegroup::portname` —— 端点生命周期与注册编排
//!
//! ## 设计意图
//!
//! 一个 [`PortName`] 在 Pagoda 中只是"某 namespace.某 servicegroup 下的一
//! 个可调用入口"。要让它真正对外可见，必须把若干并不显眼的事情按正确
//! 顺序串起来。本文件即承担这一编排职责。
//!
//! 一次 `PortNameConfigBuilder::start()` 需要做到的事情大致是：
//!
//! 1. 给请求 handler 绑定 Prometheus 指标；
//! 2. 申请一个端点级 cancellation 子令牌，纳入运行时三阶段优雅关闭体系；
//! 3. 若 `graceful_shutdown` 为真，通知 `GracefulShutdownTracker` 多了一
//!    个端点；
//! 4. 获取统一 request-plane server（TCP / HTTP / NATS），向其注册当前
//!    handler；
//! 5. 计算面向发现平面的传输地址（见 [`build_transport_type`]）；
//! 6. 写入发现平面，让外部路由器能寻址命中；
//! 7. 启动一个后台 task 监听 cancel；cancel 触发后做反向清理；
//! 8. 若配置了 `health_check_payload`，还需挂上 SystemHealth 上的健康检
//!    查 target，并校验 canary 场景下必须存在 local engine。
//!
//! 这些步骤具备顺序依赖。本文件把它拆成一组短小的 helper，让 `start()`
//! 的主体看起来更像"按顺序点名"——既便于阅读，也便于在 `tests` 中独立
//! 验证可纯函数化的环境推断逻辑。
//!
//! ## 外部契约（必须严格保持）
//!
//! - `pub struct PortNameConfig { ... }`（含 derive_builder / derive_getters /
//!   educe 全部属性）；
//! - `PortNameConfigBuilder::from_portname(...)`；
//! - `PortNameConfigBuilder::register_local_engine(...)`；
//! - `PortNameConfigBuilder::start()`；
//! - `pub async fn build_transport_type(portname, portname_id, conn_id)`；
//! - `impl PortName { register_portname_instance, unregister_portname_instance }`。
//!
//! 上述所有项目的签名、字段名、属性宏均**不得变更**。本文件内部的私
//! 有 helper 与控制流细节可自由调整。

use std::sync::Arc;

use anyhow::Result;
use derive_builder::Builder;
use derive_getters::Dissolve;
use educe::Educe;
use tokio_util::sync::CancellationToken;

use crate::{
    servicegroup::{DeviceType, PortName, Instance, TransportType},
    distributed::RequestPlaneMode,
    pipeline::network::{PushWorkHandler, ingress::push_endpoint::PushEndpoint},
    protocols::PortNameId,
    traits::DistributedRuntimeProvider,
    transports::nats,
};

// ============================================================================
// 环境推断：设备类型
// ============================================================================

/// 表示某个 CUDA / NVIDIA 相关环境变量是否"显式禁用了 GPU 可见性"。
enum DeviceVisibility {
    /// 环境变量未设置或语义上**不**禁用 GPU。
    GpuAllowed,
    /// 环境变量被显式设置为禁用 GPU 的取值。
    GpuForciblyDisabled,
}

/// 解析单个环境变量的 GPU 可见性语义。
///
/// ## 入参
///
/// - `var_name`：环境变量名（例如 `CUDA_VISIBLE_DEVICES`）。
/// - `treat_empty_as_disabled`：当变量值为空字符串时是否视为禁用 GPU。
///   `CUDA_VISIBLE_DEVICES=""` 在 CUDA 语义里等同于关闭 GPU；
///   `NVIDIA_VISIBLE_DEVICES=""` 则**不**视为关闭。该开关把两种语义差
///   异收敛到这一处。
///
/// ## 返回
///
/// - 变量不存在 → `GpuAllowed`；
/// - 变量存在，且小写化、trim 后落在拒绝集合（空 / `-1` / `none` / `void`）
///   且对应开关允许时 → `GpuForciblyDisabled`；
/// - 其它情况 → `GpuAllowed`。
fn read_visibility_env(var_name: &str, treat_empty_as_disabled: bool) -> DeviceVisibility {
    let raw = match std::env::var(var_name) {
        Ok(v) => v,
        Err(_) => return DeviceVisibility::GpuAllowed,
    };
    let lower = raw.trim().to_ascii_lowercase();
    let disabled = match lower.as_str() {
        "" => treat_empty_as_disabled,
        "-1" => treat_empty_as_disabled,
        "none" => true,
        "void" => true,
        _ => false,
    };
    if disabled {
        DeviceVisibility::GpuForciblyDisabled
    } else {
        DeviceVisibility::GpuAllowed
    }
}

/// 推断当前端点应该上报为 CPU 还是 CUDA 设备。
///
/// ## 出参
///
/// 实际上始终返回 `Some(...)`；保留 `Option` 包装仅是为了与
/// `Instance::device_type` 字段类型对齐。
///
/// ## 推断规则
///
/// 1. 若 `CUDA_VISIBLE_DEVICES` 取值显式禁用（空串 / `-1` / `none` /
///    `void`），返回 `DeviceType::Cpu`；
/// 2. 否则若 `NVIDIA_VISIBLE_DEVICES` 取值为 `none` / `void`（空串不算），
///    同样返回 `DeviceType::Cpu`；
/// 3. 其它情况一律返回 `DeviceType::Cuda`。
fn portname_device_type() -> Option<DeviceType> {
    if matches!(
        read_visibility_env("CUDA_VISIBLE_DEVICES", /*empty=*/ true),
        DeviceVisibility::GpuForciblyDisabled
    ) {
        return Some(DeviceType::Cpu);
    }
    if matches!(
        read_visibility_env("NVIDIA_VISIBLE_DEVICES", /*empty=*/ false),
        DeviceVisibility::GpuForciblyDisabled
    ) {
        return Some(DeviceType::Cpu);
    }
    Some(DeviceType::Cuda)
}

// ============================================================================
// 公开类型：PortNameConfig（签名 / 属性宏严格保持原貌）
// ============================================================================

#[derive(Educe, Builder, Dissolve)]
#[educe(Debug)]
#[builder(pattern = "owned", build_fn(private, name = "build_internal"))]
pub struct PortNameConfig {
    #[builder(private)]
    portname: PortName,

    /// portname 的请求 handler
    #[educe(Debug(ignore))]
    handler: Arc<dyn PushWorkHandler>,

    /// 指标用的附加标签
    #[builder(default, setter(into))]
    metrics_labels: Option<Vec<(String, String)>>,

    /// 关闭过程中是否等待在途请求完成
    #[builder(default = "true")]
    graceful_shutdown: bool,

    /// 本 portname 的健康检查负载。
    /// 健康检查时会把该负载发送给 portname，
    /// 以验证其能够正常响应。
    #[educe(Debug(ignore))]
    #[builder(default, setter(into, strip_option))]
    health_check_payload: Option<serde_json::Value>,
}

// ============================================================================
// 公开 API：PortNameConfigBuilder
// ============================================================================

impl PortNameConfigBuilder {
    /// 由 [`PortName::portname_builder`] 间接调用，预填 `portname` 字段，
    /// 让外部使用方无需重复指定自身。
    pub(crate) fn from_portname(portname: PortName) -> Self {
        Self::default().portname(portname)
    }

    /// 把本地 async engine 注册到进程内 `LocalPortNameRegistry`，
    /// 允许同进程调用绕过网络直接命中本地实现。
    ///
    /// ## 设计动机
    ///
    /// 主要服务两类需求：
    ///
    /// 1. canary 健康检查：health-check payload 需要真正驱动一次 `generate`
    ///    才能验证 worker 可用；若无本地 engine、仅能走网络路径，会陷
    ///    入"端点尚未注册到发现平面 → canary 失败 → 端点永远注册不上"
    ///    的死锁。
    /// 2. 单进程 / 嵌入式：可以不起完整 request-plane server。
    ///
    /// ## 入参
    ///
    /// - `engine`：实现 `LocalAsyncEngine` 的引擎实例。
    ///
    /// ## 行为
    ///
    /// 仅当 builder 已持有 `portname` 时执行注册；否则静默跳过。
    pub fn register_local_engine(
        self,
        engine: crate::local_portname_registry::LocalAsyncEngine,
    ) -> Result<Self> {
        if let Some(portname) = self.portname.as_ref() {
            let registry = portname.drt().local_portname_registry();
            registry.register(portname.name.clone(), engine);
            tracing::debug!(
                "Registered engine for portname '{}' in local registry",
                portname.name,
            );
        }
        Ok(self)
    }

    /// 真正启动端点：装指标、绑请求平面、写入发现平面、接入优雅关闭。
    ///
    /// ## 失败后的副作用
    ///
    /// - 发现平面注册失败时主动 `cancel()` 端点 shutdown token，使后台
    ///   清理 task 解除 request-plane 注册，避免"已注册 RPC server 但
    ///   外部找不到"的半挂态；
    /// - 其它启动期错误不会强制 cancel（因为尚未注册到任何外部系统）。
    pub async fn start(self) -> Result<()> {
        // === 第 1 步：拆解 builder ===
        let (portname, handler, metrics_labels, graceful_shutdown, health_check_payload) =
            self.build_internal()?.dissolve();
        let connection_id = portname.drt().connection_id();
        let portname_id = portname.id();

        tracing::debug!("Starting portname: {portname_id}");

        // === 第 2 步：给 handler 装指标 ===
        attach_handler_metrics(handler.as_ref(), &portname, metrics_labels.as_ref())?;

        // === 第 3 步：申请端点级 cancellation token ===
        // 它是 DRT portname_shutdown_token 的子令牌；在优雅关闭阶段 1 被
        // 主动 cancel，从而触发后续清理任务。
        let portname_shutdown_token = portname.drt().child_token();

        let system_health = portname.drt().system_health();

        // === 第 4 步：登记到 GracefulShutdownTracker ===
        let tracker_clone = register_with_graceful_tracker(&portname, graceful_shutdown);

        // === 第 5 步：拿请求平面 server ===
        let server = portname.drt().request_plane_server().await?;

        // === 第 6 步：health check 接入（可选） ===
        wire_health_check_target(
            &portname,
            &portname_id,
            connection_id,
            handler.as_ref(),
            &system_health,
            health_check_payload.as_ref(),
        )
        .await?;

        // === 第 7 步：把 handler 注册到 request-plane server ===
        tracing::debug!(
            portname = %portname_id.name,
            transport = server.transport_name(),
            "Registering portname with request plane server",
        );
        server
            .register_portname(
                portname_id.name.clone(),
                handler,
                connection_id,
                portname_id.namespace.clone(),
                portname_id.servicegroup.clone(),
                system_health.clone(),
            )
            .await?;

        // === 第 8 步：启动清理 task，等待 cancel ===
        let cleanup_task = spawn_cleanup_task(
            portname_id.name.clone(),
            server.clone(),
            portname_shutdown_token.clone(),
            tracker_clone,
        );

        // === 第 9 步：注册到发现平面；失败则 cancel 清理 task 并 bail ===
        if let Err(e) = register_in_discovery(&portname, &portname_id, connection_id).await {
            tracing::error!(
                %portname_id,
                error = %e,
                "Unable to register service for discovery",
            );
            portname_shutdown_token.cancel();
            anyhow::bail!(
                "Unable to register service for discovery. Check discovery service status"
            );
        }

        // === 第 10 步：等待清理 task 结束（实际就是等 cancel） ===
        cleanup_task.await??;
        Ok(())
    }
}

// ============================================================================
// 私有 helper：start() 的拆分步骤
// ============================================================================

/// 给请求 handler 挂指标。
///
/// 把 owned 形式的 `(String, String)` 标签按引用转成 `(&str, &str)` 切片
/// 后传给 `PushWorkHandler::add_metrics`，避免上游 trait 签名改动，也
/// 不必在循环中反复 `.as_str()`。
fn attach_handler_metrics(
    handler: &dyn PushWorkHandler,
    portname: &PortName,
    labels: Option<&Vec<(String, String)>>,
) -> Result<()> {
    let borrowed: Option<Vec<(&str, &str)>> =
        labels.map(|v| v.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect());
    handler.add_metrics(portname, borrowed.as_deref())?;
    Ok(())
}

/// 当 `graceful_shutdown == true` 时通知 tracker 多了一个端点，并把
/// tracker 的 `Arc` 引用回传给清理 task；否则返回 `None`。
fn register_with_graceful_tracker(
    portname: &PortName,
    graceful_shutdown: bool,
) -> Option<Arc<crate::utils::GracefulShutdownTracker>> {
    if graceful_shutdown {
        tracing::debug!(
            "Registering portname '{}' with graceful shutdown tracker",
            portname.name,
        );
        let tracker = portname.drt().graceful_shutdown_tracker();
        tracker.register_portname();
        Some(tracker)
    } else {
        tracing::debug!("PortName '{}' has graceful_shutdown=false", portname.name);
        None
    }
}

/// 把端点接入 SystemHealth 的健康检查目标。
///
/// ## 行为
///
/// - 若未配置 `health_check_payload`，直接返回 `Ok(())`；
/// - 若 canary 启用但没有 local engine，立即 `bail!` 以避免后续死锁；
/// - 否则计算传输地址，构造 `Instance`，调用
///   `SystemHealth::register_health_check_target` 完成挂载，并把回调
///   notifier 绑定到 handler。
async fn wire_health_check_target(
    portname: &PortName,
    portname_id: &PortNameId,
    connection_id: u64,
    handler: &dyn PushWorkHandler,
    system_health: &Arc<parking_lot::Mutex<crate::system_health::SystemHealth>>,
    payload: Option<&serde_json::Value>,
) -> Result<()> {
    let Some(payload) = payload else {
        return Ok(());
    };

    let canary_enabled = system_health.lock().health_check_enabled();
    let has_local_engine = portname
        .drt()
        .local_portname_registry()
        .get(&portname.name)
        .is_some();
    if canary_enabled && !has_local_engine {
        anyhow::bail!(
            "PortName '{}' has a health_check_payload and canary is enabled, \
             but no local engine is registered. Call .register_local_engine() \
             before .start() so the canary health check can function.",
            portname.name
        );
    }

    let transport = build_transport_type(portname, portname_id, connection_id).await?;
    let instance = Instance {
        servicegroup: portname_id.servicegroup.clone(),
        portname: portname_id.name.clone(),
        namespace: portname_id.namespace.clone(),
        instance_id: connection_id,
        transport,
        device_type: portname_device_type(),
    };

    tracing::debug!(
        portname_name = %portname.name,
        "Registering portname health check target",
    );
    let guard = system_health.lock();
    guard.register_health_check_target(&portname.name, instance, payload.clone());
    if let Some(notifier) = guard.get_portname_health_check_notifier(&portname.name) {
        handler.set_portname_health_check_notifier(notifier)?;
    }
    Ok(())
}

/// 启动一个后台 task，等 `cancel` 被触发后反向清理 request-plane 注册
/// 与 graceful shutdown tracker 计数。
fn spawn_cleanup_task(
    portname_name: String,
    server: Arc<dyn crate::pipeline::network::ingress::unified_server::RequestPlaneServer>,
    cancel: CancellationToken,
    tracker: Option<Arc<crate::utils::GracefulShutdownTracker>>,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    tokio::spawn(async move {
        cancel.cancelled().await;

        tracing::debug!(
            portname = %portname_name,
            "Unregistering portname from request plane server",
        );

        if let Err(e) = server.unregister_portname(&portname_name).await {
            tracing::warn!(
                portname = %portname_name,
                error = %e,
                "Failed to unregister portname",
            );
        }

        if let Some(tracker) = tracker {
            tracing::debug!("Unregister portname from graceful shutdown tracker");
            tracker.unregister_portname();
        }

        anyhow::Ok(())
    })
}

/// 在发现平面写入一条 `DiscoverySpec::PortName`。
async fn register_in_discovery(
    portname: &PortName,
    portname_id: &PortNameId,
    connection_id: u64,
) -> Result<()> {
    let discovery = portname.drt().discovery();
    let transport = build_transport_type(portname, portname_id, connection_id).await?;
    let spec = crate::discovery::DiscoverySpec::PortName {
        namespace: portname_id.namespace.clone(),
        servicegroup: portname_id.servicegroup.clone(),
        portname: portname_id.name.clone(),
        transport,
        device_type: portname_device_type(),
    };
    discovery.register(spec).await.map(|_| ())
}

// ============================================================================
// 私有 helper：端口与地址解析
// ============================================================================

/// 读取"显式固定端口"环境变量。
///
/// ## 入参
///
/// - `var`：形如 `PGD_TCP_RPC_PORT` 的变量名。
///
/// ## 返回
///
/// - 变量未设置 / 解析失败 / 值为 0 → `None`；
/// - 否则 → `Some(port)`。
///
/// 该 helper 让 `Tcp` 与 `Http` 模式共用同一条解析路径。
fn fixed_port_from_env(var: &str) -> Option<u16> {
    std::env::var(var)
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .filter(|&p| p != 0)
}

/// 解析 TCP 端口：固定端口优先；未设置则取已绑定端口。
fn resolved_tcp_port() -> Result<u16> {
    match fixed_port_from_env("PGD_TCP_RPC_PORT") {
        Some(p) => Ok(p),
        None => crate::pipeline::network::manager::get_actual_tcp_rpc_port(),
    }
}

/// 解析 HTTP 端口：固定端口优先；未设置则取已绑定端口。
fn resolved_http_port() -> Result<u16> {
    match fixed_port_from_env("PGD_HTTP_RPC_PORT") {
        Some(p) => Ok(p),
        None => crate::pipeline::network::manager::get_actual_http_rpc_port(),
    }
}

/// HTTP 模式下拼装 URL。
///
/// 格式：`http://{host}:{port}{root_path}/{portname_name}`，其中
/// `root_path` 由 `PGD_HTTP_RPC_ROOT_PATH` 控制，缺省 `/v1/rpc`。
fn compose_http_transport(portname_name: &str) -> Result<TransportType> {
    let host = crate::utils::get_http_rpc_host_from_env();
    let port = resolved_http_port()?;
    let root = std::env::var("PGD_HTTP_RPC_ROOT_PATH").unwrap_or_else(|_| "/v1/rpc".to_string());
    Ok(TransportType::Http(format!(
        "http://{host}:{port}{root}/{portname_name}"
    )))
}

/// TCP 模式下拼装地址。
///
/// 格式：`{host}:{port}/{instance_id:x}/{portname_name}`。
/// 把 `instance_id` 编入路径，使同一进程下多个 worker 共享同一 TCP
/// server 时仍能区分路由 key。
fn compose_tcp_transport(portname_name: &str, instance_id: u64) -> Result<TransportType> {
    let host = crate::utils::get_tcp_rpc_host_from_env();
    let port = resolved_tcp_port()?;
    Ok(TransportType::Tcp(format!(
        "{host}:{port}/{instance_id:x}/{portname_name}"
    )))
}

/// NATS 模式下委托给 `nats::instance_subject` 拼出 subject。
fn compose_nats_transport(portname_id: &PortNameId, instance_id: u64) -> TransportType {
    TransportType::Nats(nats::instance_subject(portname_id, instance_id))
}

/// 判断当前 mode 下是否"无需先绑定 server 就能确定地址"。
///
/// - HTTP / TCP：仅当 `PGD_*_RPC_PORT` 显式设为非零时为真；
/// - NATS：恒为真（subject 命名约定不依赖端口）。
fn transport_mode_has_fixed_addressing(mode: RequestPlaneMode) -> bool {
    match mode {
        RequestPlaneMode::Tcp => fixed_port_from_env("PGD_TCP_RPC_PORT").is_some(),
        RequestPlaneMode::Http => fixed_port_from_env("PGD_HTTP_RPC_PORT").is_some(),
        RequestPlaneMode::Nats => true,
    }
}

/// 按 mode 派发到具体的 `compose_*_transport`。
fn compose_transport_for_mode(
    mode: RequestPlaneMode,
    portname_id: &PortNameId,
    instance_id: u64,
) -> Result<TransportType> {
    match mode {
        RequestPlaneMode::Http => compose_http_transport(&portname_id.name),
        RequestPlaneMode::Tcp => compose_tcp_transport(&portname_id.name, instance_id),
        RequestPlaneMode::Nats => Ok(compose_nats_transport(portname_id, instance_id)),
    }
}

// ============================================================================
// 公开 API：传输地址构造
// ============================================================================

/// 计算面向发现平面 / 健康检查的传输地址。
///
/// ## 入参
///
/// - `portname`：关联的 [`PortName`]，用于反查 request-plane 模式与触发
///   server 初始化；
/// - `portname_id`：端点三元组，作为 HTTP URL path / TCP 路由 key /
///   NATS subject 的输入；
/// - `connection_id`：当前连接 / 实例 ID。
///
/// ## 返回
///
/// 一个 `TransportType`：
///
/// - HTTP → `http://host:port{root}/{portname_name}`；
/// - TCP  → `host:port/{instance_id:x}/{portname_name}`；
/// - NATS → 由 `nats::instance_subject` 决定的 subject 字符串。
///
/// ## 错误
///
/// 主要错误来自"端口尚未绑定"——OS 自动分配端口时调用方必须先让
/// server 绑定才能拿到正确地址。本函数在这种情况下会主动 `await` 一次
/// `request_plane_server()` 以触发绑定。
pub async fn build_transport_type(
    portname: &PortName,
    portname_id: &PortNameId,
    connection_id: u64,
) -> Result<TransportType> {
    let mode = portname.drt().request_plane();
    if !transport_mode_has_fixed_addressing(mode) {
        let _ = portname.drt().request_plane_server().await?;
    }
    compose_transport_for_mode(mode, portname_id, connection_id)
}

// ============================================================================
// 公开 API：PortName 上的下线 / 重新上线
// ============================================================================

impl PortName {
    /// 让当前进程的这个端点从发现平面**主动下线**。
    ///
    /// ## 行为
    ///
    /// 1. 通过 [`build_transport_type`] 重新计算当前端点的传输地址；
    /// 2. 用同样的实例 ID / 端点三元组拼出
    ///    `DiscoveryInstance::PortName`；
    /// 3. 调 `discovery.unregister(...)`；失败时打 error + `bail!`。
    ///
    /// ## 典型场景
    ///
    /// - worker 进入 sleep 状态，不应再被路由命中；
    /// - 灰度发布期间临时把某实例移出 routing pool。
    pub async fn unregister_portname_instance(&self) -> anyhow::Result<()> {
        let drt = self.drt();
        let instance_id = drt.connection_id();
        let portname_id = self.id();

        let transport = build_transport_type(self, &portname_id, instance_id).await?;
        let instance = crate::discovery::DiscoveryInstance::PortName(Instance {
            namespace: portname_id.namespace,
            servicegroup: portname_id.servicegroup,
            portname: portname_id.name,
            instance_id,
            transport,
            device_type: portname_device_type(),
        });

        if let Err(e) = drt.discovery().unregister(instance).await {
            let portname_id = self.id();
            tracing::error!(
                %portname_id,
                error = %e,
                "Unable to unregister portname instance from discovery",
            );
            anyhow::bail!(
                "Unable to unregister portname instance from discovery. Check discovery service status"
            );
        }

        tracing::info!(
            instance_id = instance_id,
            "Successfully unregistered portname instance from discovery - worker removed from routing pool",
        );
        Ok(())
    }

    /// 把当前端点重新登记到发现平面。
    ///
    /// 与 [`Self::unregister_portname_instance`] 形成一对，常用于
    /// worker 从 sleep 状态恢复后重新加入 routing pool。
    pub async fn register_portname_instance(&self) -> anyhow::Result<()> {
        let drt = self.drt();
        let instance_id = drt.connection_id();
        let portname_id = self.id();

        let transport = build_transport_type(self, &portname_id, instance_id).await?;
        let spec = crate::discovery::DiscoverySpec::PortName {
            namespace: portname_id.namespace,
            servicegroup: portname_id.servicegroup,
            portname: portname_id.name,
            transport,
            device_type: portname_device_type(),
        };
        if let Err(e) = drt.discovery().register(spec).await {
            let portname_id = self.id();
            tracing::error!(
                %portname_id,
                error = %e,
                "Unable to re-register portname instance to discovery",
            );
            anyhow::bail!(
                "Unable to re-register portname instance to discovery. Check discovery service status"
            );
        }

        tracing::info!(
            instance_id = instance_id,
            "Successfully re-registered portname instance to discovery - worker added back to routing pool",
        );
        Ok(())
    }
}

// ============================================================================
// 单元测试
//
// 只覆盖**不依赖真实 DRT 初始化**的纯函数行为：
//
// - `portname_device_type` 与底层 `read_visibility_env` 的环境变量解析
// - `fixed_port_from_env` 的取值规则
// - `transport_mode_has_fixed_addressing` 的判定
// - `compose_*_transport` 的字符串格式
//
// 端点 `start()` / discovery / request-plane server 等依赖真实
// `DistributedRuntime` 的集成场景由 `lib/runtime/tests/` 与 `examples/`
// 下的集成测试负责验证。
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ------------------------------------------------------------------
    // 环境变量测试 helper：进程级互斥 + RAII 恢复
    // ------------------------------------------------------------------

    /// 进程级互斥锁。`std::env` 是进程共享状态，多个测试并发改动它会相
    /// 互干扰，因此每个涉及环境变量的测试都需要先持有此锁。
    fn env_lock() -> &'static std::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
    }

    /// RAII 句柄：捕获指定环境变量的初始值；drop 时恢复原状。
    /// 测试无论是否 panic 都不会污染其他用例。
    struct EnvGuard {
        keys: Vec<(&'static str, Option<String>)>,
    }
    impl EnvGuard {
        fn capture(keys: &[&'static str]) -> Self {
            let snapshot = keys
                .iter()
                .map(|k| (*k, std::env::var(k).ok()))
                .collect::<Vec<_>>();
            Self { keys: snapshot }
        }
        fn set(&self, k: &str, v: &str) {
            // SAFETY: 测试持有 env_lock()，且在 Drop 中恢复原值。
            unsafe { std::env::set_var(k, v) };
        }
        fn unset(&self, k: &str) {
            // SAFETY: 测试持有 env_lock()，且在 Drop 中恢复原值。
            unsafe { std::env::remove_var(k) };
        }
    }
    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (k, v) in &self.keys {
                match v {
                    Some(val) => unsafe { std::env::set_var(k, val) },
                    None => unsafe { std::env::remove_var(k) },
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // portname_device_type
    // ------------------------------------------------------------------

    /// ## 测试过程
    /// 1. 清除两个 GPU 可见性环境变量；
    /// 2. 调用 `portname_device_type()`；
    /// 3. 断言结果为 `Some(DeviceType::Cuda)`。
    ///
    /// ## 意义
    /// 锁住"默认 CUDA-capable"这条契约，防止默认分支被改成 CPU。
    #[test]
    fn portname_device_type_defaults_to_cuda_when_env_unset() {
        let _g = env_lock().lock().unwrap();
        let guard = EnvGuard::capture(&["CUDA_VISIBLE_DEVICES", "NVIDIA_VISIBLE_DEVICES"]);
        guard.unset("CUDA_VISIBLE_DEVICES");
        guard.unset("NVIDIA_VISIBLE_DEVICES");
        assert_eq!(portname_device_type(), Some(DeviceType::Cuda));
    }

    /// ## 测试过程
    /// 将 `CUDA_VISIBLE_DEVICES` 依次设为空串 / `-1` / `none` / `void`
    /// 及其大小写变体，验证全部判定为 CPU。
    ///
    /// ## 意义
    /// 覆盖 CUDA 语义下所有"禁用 GPU"的常见写法，避免漏判。
    #[test]
    fn portname_device_type_cuda_disabled_values_map_to_cpu() {
        let _g = env_lock().lock().unwrap();
        let guard = EnvGuard::capture(&["CUDA_VISIBLE_DEVICES", "NVIDIA_VISIBLE_DEVICES"]);
        guard.unset("NVIDIA_VISIBLE_DEVICES");
        for v in ["", "-1", "none", "void", "NONE", "Void", "  -1  "] {
            guard.set("CUDA_VISIBLE_DEVICES", v);
            assert_eq!(
                portname_device_type(),
                Some(DeviceType::Cpu),
                "CUDA_VISIBLE_DEVICES={v:?} 应判定为 CPU",
            );
        }
    }

    /// ## 测试过程
    /// 将 `CUDA_VISIBLE_DEVICES` 设为正常的设备列表（如 `0,1`）；
    /// 断言结果为 CUDA。
    ///
    /// ## 意义
    /// 防止把正常列表误判成 CPU。
    #[test]
    fn portname_device_type_keeps_cuda_when_devices_listed() {
        let _g = env_lock().lock().unwrap();
        let guard = EnvGuard::capture(&["CUDA_VISIBLE_DEVICES", "NVIDIA_VISIBLE_DEVICES"]);
        guard.unset("NVIDIA_VISIBLE_DEVICES");
        guard.set("CUDA_VISIBLE_DEVICES", "0,1");
        assert_eq!(portname_device_type(), Some(DeviceType::Cuda));
    }

    /// ## 测试过程
    /// 在 `CUDA_VISIBLE_DEVICES` 未设置时，依次测 `NVIDIA_VISIBLE_DEVICES`
    /// 的不同取值；验证：
    /// - `none` / `void` → CPU；
    /// - 空串 → 仍 CUDA（与 CUDA 变量语义不同）；
    /// - `all` 或具体设备列表 → CUDA。
    ///
    /// ## 意义
    /// 锁定 CUDA 与 NVIDIA 两套变量"空串语义不同"这一微妙差异。
    #[test]
    fn portname_device_type_handles_nvidia_visible_devices() {
        let _g = env_lock().lock().unwrap();
        let guard = EnvGuard::capture(&["CUDA_VISIBLE_DEVICES", "NVIDIA_VISIBLE_DEVICES"]);
        guard.unset("CUDA_VISIBLE_DEVICES");

        guard.set("NVIDIA_VISIBLE_DEVICES", "none");
        assert_eq!(portname_device_type(), Some(DeviceType::Cpu));

        guard.set("NVIDIA_VISIBLE_DEVICES", "void");
        assert_eq!(portname_device_type(), Some(DeviceType::Cpu));

        guard.set("NVIDIA_VISIBLE_DEVICES", "");
        assert_eq!(portname_device_type(), Some(DeviceType::Cuda));

        guard.set("NVIDIA_VISIBLE_DEVICES", "all");
        assert_eq!(portname_device_type(), Some(DeviceType::Cuda));
    }

    /// ## 测试过程
    /// 让 CUDA 与 NVIDIA 同时禁用，验证仍为 CPU；以及 CUDA 启用
    /// 但 NVIDIA 禁用时——CUDA 的判定先发生，故应为 CPU。
    ///
    /// ## 意义
    /// 验证两个变量的优先级与短路逻辑。
    #[test]
    fn portname_device_type_short_circuits_on_cuda() {
        let _g = env_lock().lock().unwrap();
        let guard = EnvGuard::capture(&["CUDA_VISIBLE_DEVICES", "NVIDIA_VISIBLE_DEVICES"]);

        // 两个都禁用
        guard.set("CUDA_VISIBLE_DEVICES", "-1");
        guard.set("NVIDIA_VISIBLE_DEVICES", "none");
        assert_eq!(portname_device_type(), Some(DeviceType::Cpu));

        // CUDA 正常 + NVIDIA 禁用 → NVIDIA 起作用 → CPU
        guard.set("CUDA_VISIBLE_DEVICES", "0");
        guard.set("NVIDIA_VISIBLE_DEVICES", "none");
        assert_eq!(portname_device_type(), Some(DeviceType::Cpu));

        // CUDA 禁用 + NVIDIA 正常 → CUDA 起作用 → CPU
        guard.set("CUDA_VISIBLE_DEVICES", "-1");
        guard.set("NVIDIA_VISIBLE_DEVICES", "0");
        assert_eq!(portname_device_type(), Some(DeviceType::Cpu));
    }

    // ------------------------------------------------------------------
    // fixed_port_from_env
    // ------------------------------------------------------------------

    /// ## 测试过程
    /// 变量未设置时调用，应返回 `None`。
    #[test]
    fn fixed_port_returns_none_when_unset() {
        let _g = env_lock().lock().unwrap();
        let guard = EnvGuard::capture(&["PGD_TEST_PORT_VAR_A"]);
        guard.unset("PGD_TEST_PORT_VAR_A");
        assert_eq!(fixed_port_from_env("PGD_TEST_PORT_VAR_A"), None);
    }

    /// ## 测试过程
    /// 非数字 / 端口 0 / 越界 u16 都应返回 `None`。
    ///
    /// ## 意义
    /// 防止把 "0" 当成有效端口；保护后续 `format!` 不会拼出 `:0/...`。
    #[test]
    fn fixed_port_returns_none_for_invalid_values() {
        let _g = env_lock().lock().unwrap();
        let guard = EnvGuard::capture(&["PGD_TEST_PORT_VAR_B"]);

        guard.set("PGD_TEST_PORT_VAR_B", "abc");
        assert_eq!(fixed_port_from_env("PGD_TEST_PORT_VAR_B"), None);

        guard.set("PGD_TEST_PORT_VAR_B", "0");
        assert_eq!(fixed_port_from_env("PGD_TEST_PORT_VAR_B"), None);

        guard.set("PGD_TEST_PORT_VAR_B", "99999");
        assert_eq!(fixed_port_from_env("PGD_TEST_PORT_VAR_B"), None);
    }

    /// ## 测试过程
    /// 合法端口被原样解析为 `Some(u16)`。
    #[test]
    fn fixed_port_returns_some_for_valid_port() {
        let _g = env_lock().lock().unwrap();
        let guard = EnvGuard::capture(&["PGD_TEST_PORT_VAR_C"]);
        guard.set("PGD_TEST_PORT_VAR_C", "8443");
        assert_eq!(fixed_port_from_env("PGD_TEST_PORT_VAR_C"), Some(8443));
    }

    // ------------------------------------------------------------------
    // transport_mode_has_fixed_addressing
    // ------------------------------------------------------------------

    /// ## 测试过程
    /// NATS 模式恒视为已就绪，与环境变量无关。
    ///
    /// ## 意义
    /// 防止 NATS 模式的 `build_transport_type` 走到 `request_plane_server`
    /// 初始化路径。
    #[test]
    fn transport_mode_nats_is_always_fixed() {
        let _g = env_lock().lock().unwrap();
        assert!(transport_mode_has_fixed_addressing(RequestPlaneMode::Nats));
    }

    /// ## 测试过程
    /// 清除 / 设置 TCP / HTTP 端口环境变量，验证返回值随之翻转。
    #[test]
    fn transport_mode_tcp_http_depends_on_env() {
        let _g = env_lock().lock().unwrap();
        let guard = EnvGuard::capture(&["PGD_TCP_RPC_PORT", "PGD_HTTP_RPC_PORT"]);

        guard.unset("PGD_TCP_RPC_PORT");
        guard.unset("PGD_HTTP_RPC_PORT");
        assert!(!transport_mode_has_fixed_addressing(RequestPlaneMode::Tcp));
        assert!(!transport_mode_has_fixed_addressing(RequestPlaneMode::Http));

        guard.set("PGD_TCP_RPC_PORT", "9001");
        guard.set("PGD_HTTP_RPC_PORT", "9002");
        assert!(transport_mode_has_fixed_addressing(RequestPlaneMode::Tcp));
        assert!(transport_mode_has_fixed_addressing(RequestPlaneMode::Http));

        // 端口 0 不算固定
        guard.set("PGD_TCP_RPC_PORT", "0");
        assert!(!transport_mode_has_fixed_addressing(RequestPlaneMode::Tcp));
    }

    // ------------------------------------------------------------------
    // compose_*_transport
    // ------------------------------------------------------------------

    /// ## 测试过程
    /// 给定固定 TCP 端口，验证拼出的字符串以 `:9090/cafe/gen` 结尾。
    #[test]
    fn compose_tcp_transport_format() {
        let _g = env_lock().lock().unwrap();
        let guard = EnvGuard::capture(&["PGD_TCP_RPC_PORT"]);
        guard.set("PGD_TCP_RPC_PORT", "9090");
        let res = compose_tcp_transport("gen", 0xCAFE).expect("port set");
        match res {
            TransportType::Tcp(s) => {
                assert!(s.ends_with(":9090/cafe/gen"), "got: {s}");
            }
            other => panic!("expected Tcp variant, got {other:?}"),
        }
    }

    /// ## 测试过程
    /// 默认 `PGD_HTTP_RPC_ROOT_PATH` 未设置时，URL 应以 `:8080/v1/rpc/gen`
    /// 结尾且以 `http://` 起头。
    #[test]
    fn compose_http_transport_default_root_path() {
        let _g = env_lock().lock().unwrap();
        let guard = EnvGuard::capture(&["PGD_HTTP_RPC_PORT", "PGD_HTTP_RPC_ROOT_PATH"]);
        guard.set("PGD_HTTP_RPC_PORT", "8080");
        guard.unset("PGD_HTTP_RPC_ROOT_PATH");
        let res = compose_http_transport("gen").expect("port set");
        match res {
            TransportType::Http(s) => {
                assert!(s.starts_with("http://"), "got: {s}");
                assert!(s.ends_with(":8080/v1/rpc/gen"), "got: {s}");
            }
            other => panic!("expected Http variant, got {other:?}"),
        }
    }

    /// ## 测试过程
    /// 自定义 root path 时，应反映到最终 URL 中。
    #[test]
    fn compose_http_transport_custom_root_path() {
        let _g = env_lock().lock().unwrap();
        let guard = EnvGuard::capture(&["PGD_HTTP_RPC_PORT", "PGD_HTTP_RPC_ROOT_PATH"]);
        guard.set("PGD_HTTP_RPC_PORT", "8080");
        guard.set("PGD_HTTP_RPC_ROOT_PATH", "/custom/path");
        let res = compose_http_transport("gen").expect("port set");
        match res {
            TransportType::Http(s) => {
                assert!(s.ends_with(":8080/custom/path/gen"), "got: {s}");
            }
            other => panic!("expected Http variant, got {other:?}"),
        }
    }

    /// ## 测试过程
    /// `compose_nats_transport` 输出应与 `nats::instance_subject` 完全
    /// 一致——验证派发关系正确。
    #[test]
    fn compose_nats_transport_delegates_to_subject() {
        let id = PortNameId {
            namespace: "ns".to_string(),
            servicegroup: "comp".to_string(),
            name: "gen".to_string(),
        };
        let expected = nats::instance_subject(&id, 42);
        let actual = compose_nats_transport(&id, 42);
        assert_eq!(actual, TransportType::Nats(expected));
    }

    // ------------------------------------------------------------------
    // 仅为消除 unused import 警告（PushEndpoint 仅用于跨模块类型可见性）
    // ------------------------------------------------------------------

    /// 该测试不真正运行任何逻辑，只是确保从父模块沿用的 `PushEndpoint`
    /// 类型仍被本文件引用，避免 dead-import 警告并保留原始 use 行不变。
    #[test]
    fn push_endpoint_symbol_is_in_scope() {
        fn _assert_type_visible<T>() {}
        _assert_type_visible::<PushEndpoint>();
    }
}
