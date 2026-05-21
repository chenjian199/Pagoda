// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 主动健康检查管理器——canary 请求探测 + Notify 驱动的按需探测策略。
//!
//! 设计要点：
//! - 每个 PortName 一个独立的 Tokio 任务（互不干扰）
//! - `canary_wait_time`：一段时间无真实业务流量才触发探测（金丝雀策略）
//! - `Notify`：真实业务请求到来时重置计时器，避免与正常流量抢资源
//! - 动态发现：`spawn_new_portname_monitor` 监听 `SystemHealth` 的新端点注册事件

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::task::JoinHandle;

use crate::distributed::DistributedRuntime;
use crate::servicegroup::TransportType;
use crate::system_health::HealthStatus;

// ──────────────────── HealthCheckConfig ───────────────────────────

/// 健康检查配置参数。
#[derive(Debug, Clone)]
pub struct HealthCheckConfig {
    /// 空闲多久后才触发一次主动探测（canary 策略）。
    pub canary_wait_time: Duration,
    /// 单次探测请求超时。
    pub request_timeout: Duration,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            canary_wait_time: if cfg!(debug_assertions) {
                Duration::from_secs(5)
            } else {
                Duration::from_secs(30)
            },
            request_timeout: Duration::from_secs(10),
        }
    }
}

// ──────────────────── RouterCache ────────────────────────────────

/// portname_subject → 目标地址缓存（首次探测时创建，后续复用）。
type RouterCache = Arc<Mutex<HashMap<String, String>>>;

// ──────────────────── HealthCheckManager ─────────────────────────

/// 管理每个 PortName 的健康检查任务生命周期。
pub struct HealthCheckManager {
    drt: DistributedRuntime,
    config: HealthCheckConfig,
    /// portname → 目标传输地址缓存
    router_cache: RouterCache,
    /// portname → 正在运行的探测任务句柄
    portname_tasks: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
}

impl HealthCheckManager {
    /// 构造管理器（不启动任何任务）。
    pub fn new(drt: DistributedRuntime, config: HealthCheckConfig) -> Self {
        Self {
            drt,
            config,
            router_cache: Arc::new(Mutex::new(HashMap::new())),
            portname_tasks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    // ── 启动流程 ──────────────────────────────────────────────────

    /// 启动：为已注册端点开始探测，并监听后续动态注册。
    ///
    /// 必须以 `Arc<Self>` 调用，因为探测任务持有 `manager.clone()`。
    pub async fn start(self: Arc<Self>) -> anyhow::Result<()> {
        // 1. 读取已注册端点并为每个端点启动任务
        let existing_portnames = {
            self.drt.system_health().lock().get_health_check_portnames()
        };
        for portname_subject in existing_portnames {
            self.spawn_portname_health_check_task(portname_subject);
        }

        // 2. 监听后续动态注册的端点
        self.clone().spawn_new_portname_monitor().await?;
        Ok(())
    }

    // ── 单端点探测任务 ────────────────────────────────────────────

    /// 为指定端点启动独立的 canary 探测任务。
    fn spawn_portname_health_check_task(self: &Arc<Self>, portname_subject: String) {
        // 重复检测
        {
            let tasks = self.portname_tasks.lock();
            if tasks.contains_key(&portname_subject) {
                tracing::warn!(
                    portname = %portname_subject,
                    "health check task already exists for this portname"
                );
                return;
            }
        }

        let notifier = {
            self.drt
                .system_health()
                .lock()
                .get_portname_health_check_notifier(&portname_subject)
                .expect("notifier must exist when task is spawned")
        };

        let manager = Arc::clone(self);
        let canary_wait = self.config.canary_wait_time;
        let pn = portname_subject.clone();
        let cancel = self.drt.rt().portname_shutdown_token().clone();

        let handle = tokio::spawn(async move {
            tracing::debug!(portname = %pn, "health check task started");
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        tracing::debug!(portname = %pn, "health check task cancelled");
                        break;
                    }
                    _ = tokio::time::sleep(canary_wait) => {
                        // 空闲超时：触发一次主动探测
                        manager.send_health_check_request(&pn).await;
                    }
                    _ = notifier.notified() => {
                        // 有真实业务流量：重置计时器（循环重来，sleep 从头计时）
                        tracing::trace!(portname = %pn, "notified: real traffic received, resetting canary timer");
                        continue;
                    }
                }
            }
        });

        self.portname_tasks.lock().insert(portname_subject, handle);
    }

    // ── 动态端点发现监控 ──────────────────────────────────────────

    /// 监听 `SystemHealth` 新端点注册事件，及时为其启动探测任务。
    async fn spawn_new_portname_monitor(self: Arc<Self>) -> anyhow::Result<()> {
        let mut rx = self
            .drt
            .system_health()
            .lock()
            .take_new_portname_receiver()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "new portname receiver already taken; is HealthCheckManager started twice?"
                )
            })?;

        let manager = Arc::clone(&self);
        tokio::spawn(async move {
            while let Some(portname_subject) = rx.recv().await {
                // 重复注册检测
                {
                    let tasks = manager.portname_tasks.lock();
                    if tasks.contains_key(&portname_subject) {
                        tracing::error!(
                            portname = %portname_subject,
                            "CRITICAL: received duplicate portname registration in health check monitor"
                        );
                        break;
                    }
                }
                manager.spawn_portname_health_check_task(portname_subject);
            }
        });
        Ok(())
    }

    // ── 探测请求发送 ──────────────────────────────────────────────

    /// 向目标端点发送一次 canary 探测请求，并把结果回写到 SystemHealth。
    async fn send_health_check_request(&self, portname_subject: &str) {
        // 获取探测目标
        let target = match self.drt.system_health().lock().get_health_check_target(portname_subject) {
            Some(t) => t,
            None => {
                tracing::warn!(portname = %portname_subject, "health check target not found");
                return;
            }
        };

        let timeout = self.config.request_timeout;
        let drt = self.drt.clone();
        let pn_subject = portname_subject.to_owned();

        // 获取或创建传输地址（懒缓存）
        let transport_addr = {
            let cached = self.router_cache.lock().get(portname_subject).cloned();
            if let Some(addr) = cached {
                addr
            } else {
                // 从发现层查找目标实例的传输地址
                match self.resolve_transport_addr(&target.instance).await {
                    Some(addr) => {
                        self.router_cache.lock().insert(portname_subject.to_owned(), addr.clone());
                        addr
                    }
                    None => {
                        tracing::debug!(
                            portname = %portname_subject,
                            "no instances available for health check probe, marking NotReady"
                        );
                        drt.system_health().lock().set_portname_health_status(
                            &pn_subject,
                            HealthStatus::NotReady,
                        );
                        return;
                    }
                }
            }
        };

        // 在独立任务中发送探测（不阻塞 canary 循环）
        let payload = target.payload.clone();
        tokio::spawn(async move {
            let result = tokio::time::timeout(
                timeout,
                Self::probe_via_transport(&transport_addr, &payload),
            )
            .await;

            let status = match result {
                Ok(Ok(true)) => HealthStatus::Ready,
                Ok(Ok(false)) => {
                    tracing::warn!(portname = %pn_subject, "canary probe returned unhealthy response");
                    HealthStatus::NotReady
                }
                Ok(Err(e)) => {
                    tracing::warn!(portname = %pn_subject, error = %e, "canary probe error");
                    HealthStatus::NotReady
                }
                Err(_elapsed) => {
                    tracing::warn!(portname = %pn_subject, ?timeout, "canary probe timed out");
                    HealthStatus::NotReady
                }
            };

            drt.system_health().lock().set_portname_health_status(&pn_subject, status);
        });
    }

    /// 从发现层解析目标实例的传输地址字符串。
    async fn resolve_transport_addr(
        &self,
        instance: &crate::servicegroup::Instance,
    ) -> Option<String> {
        use crate::discovery::{DiscoveryInstance, DiscoveryQuery};

        let query = DiscoveryQuery::PortName {
            namespace: instance.namespace.clone(),
            servicegroup: instance.servicegroup.clone(),
            portname: instance.portname.clone(),
        };

        let instances = match self.drt.discovery().list(query).await {
            Ok(list) => list,
            Err(e) => {
                tracing::warn!("discovery list failed during health check: {e}");
                return None;
            }
        };

        // 优先找目标 instance_id，fallback 到任意实例
        let target_transport = instances
            .iter()
            .filter_map(|di| match di {
                DiscoveryInstance::PortName(inst)
                    if inst.instance_id == instance.instance_id =>
                {
                    Some(inst.transport.clone())
                }
                _ => None,
            })
            .next()
            .or_else(|| {
                instances.iter().find_map(|di| match di {
                    DiscoveryInstance::PortName(inst) => Some(inst.transport.clone()),
                    _ => None,
                })
            });

        target_transport.map(|t| match t {
            TransportType::Tcp(addr) => addr,
            TransportType::Http(url) => url,
            TransportType::Nats(subject) => subject,
        })
    }

    /// 通过传输地址发送探测请求。
    ///
    /// - TCP 地址：尝试 TCP 连接（三次握手成功即视为存活）
    /// - HTTP URL：发送 HTTP GET
    /// - NATS subject：通过 NATS pub/sub 探测（暂不实现，返回 true）
    async fn probe_via_transport(
        addr: &str,
        _payload: &serde_json::Value,
    ) -> anyhow::Result<bool> {
        if addr.starts_with("http://") || addr.starts_with("https://") {
            Self::probe_http(addr).await
        } else if addr.contains('/') {
            // NATS subject 格式通常含 '.'，不含 '/'；TCP 地址含 '/'；简单判断
            // TCP 格式: "{host}:{port}/{instance_id_hex}/{portname}"
            let tcp_addr = addr.split('/').next().unwrap_or(addr);
            Self::probe_tcp(tcp_addr).await
        } else {
            // 纯 host:port 格式
            Self::probe_tcp(addr).await
        }
    }

    /// TCP 连接探测（三次握手成功 = 服务存活）。
    async fn probe_tcp(addr: &str) -> anyhow::Result<bool> {
        use tokio::net::TcpStream;
        match TcpStream::connect(addr).await {
            Ok(_) => Ok(true),
            Err(e) => {
                tracing::debug!("TCP probe failed for {addr}: {e}");
                Ok(false)
            }
        }
    }

    /// HTTP GET 探测（2xx = 健康）。
    async fn probe_http(url: &str) -> anyhow::Result<bool> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let uri: hyper::Uri = url.parse()?;
        let host = uri.host().ok_or_else(|| anyhow::anyhow!("missing host in {url}"))?;
        let port = uri.port_u16().unwrap_or(if uri.scheme_str() == Some("https") { 443 } else { 80 });
        let path = if uri.path().is_empty() { "/" } else { uri.path() };
        let addr = format!("{host}:{port}");

        let mut stream = TcpStream::connect(&addr).await?;
        let req = format!(
            "GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n"
        );
        stream.write_all(req.as_bytes()).await?;
        let mut buf = [0u8; 64];
        let n = stream.read(&mut buf).await?;
        let header = std::str::from_utf8(&buf[..n]).unwrap_or("");
        let ok = header
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse::<u16>().ok())
            .map(|code| (200..300).contains(&code))
            .unwrap_or(false);
        Ok(ok)
    }
}

impl std::fmt::Debug for HealthCheckManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HealthCheckManager")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

// ──────────────────── 公开入口函数 ────────────────────────────────

/// 创建并启动健康检查管理器（DRT 初始化时由步骤 8 调用）。
///
/// `config` 为 `None` 时使用默认配置。
pub async fn start_health_check_manager(
    drt: DistributedRuntime,
    config: Option<HealthCheckConfig>,
) -> anyhow::Result<()> {
    let config = config.unwrap_or_default();
    let manager = Arc::new(HealthCheckManager::new(drt, config));
    manager.start().await
}

/// 获取健康检查状态汇总（供 `/health` HTTP 端点使用）。
///
/// 返回 JSON：
/// ```json
/// { "status": "ready", "portnames_checked": 2,
///   "portname_statuses": { "ns/sg/pn": { "healthy": true, "status": "ready" } } }
/// ```
pub async fn get_health_check_status(drt: &DistributedRuntime) -> anyhow::Result<serde_json::Value> {
    let (portname_subjects, health_guard) = {
        let guard = drt.system_health().lock();
        let subjects = guard.get_health_check_portnames();
        // We need to read statuses while holding the lock, then drop
        let mut statuses = serde_json::Map::new();
        for pn in &subjects {
            let status = guard
                .get_portname_health_status(pn)
                .unwrap_or(HealthStatus::NotReady);
            let is_healthy = status == HealthStatus::Ready;
            statuses.insert(
                pn.clone(),
                serde_json::json!({ "healthy": is_healthy, "status": status.to_string() }),
            );
        }
        (subjects, statuses)
    };

    let portnames_checked = portname_subjects.len();
    let overall_healthy = health_guard
        .values()
        .all(|v| v["healthy"].as_bool().unwrap_or(false));

    Ok(serde_json::json!({
        "status": if overall_healthy { "ready" } else { "notready" },
        "portnames_checked": portnames_checked,
        "portname_statuses": health_guard,
    }))
}
