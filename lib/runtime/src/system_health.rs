// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 进程级健康状态聚合——SystemHealth 是健康子系统的被动状态中心。
//!
//! 职责：
//! 1. 保存健康状态（系统级 + 端点级）
//! 2. 管理主动健康检查注册表（target + notifier）
//! 3. 提供 uptime、路径元数据等可观测性基础数据
//!
//! 不发送探测请求，不提供 HTTP 服务；由 `health_check` 和 `system_status_server` 消费。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::servicegroup;

// ───────────────────────── HealthStatus ───────────────────────────

/// 端点健康状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    /// 端点正在启动（health check 尚未通过）。
    Starting,
    /// 端点已就绪，可接受流量。
    Ready,
    /// 端点尚未就绪或已失联。
    NotReady,
}

impl std::fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HealthStatus::Starting => write!(f, "starting"),
            HealthStatus::Ready => write!(f, "ready"),
            HealthStatus::NotReady => write!(f, "notready"),
        }
    }
}

// ───────────────────────── HealthCheckTarget ──────────────────────

/// 健康检查目标：要检查哪个实例 + 探测请求体。
#[derive(Debug, Clone)]
pub struct HealthCheckTarget {
    /// 目标实例（含 namespace / servicegroup / portname / transport / instance_id）。
    pub instance: servicegroup::Instance,
    /// 探测请求 JSON 体（通常是最小 prompt 或 health-check 标识字段）。
    pub payload: serde_json::Value,
}

// ───────────────────────── SystemHealth ───────────────────────────

/// 进程级健康状态聚合器（被动状态中心）。
///
/// 外层由 `Arc<parking_lot::Mutex<SystemHealth>>` 保护，
/// 内部高频访问字段使用 `Arc<RwLock<...>>` 降低锁粒度。
pub struct SystemHealth {
    /// 系统级状态（低频、生命周期切换驱动，直接字段存储）。
    system_health: HealthStatus,
    /// 端点级状态表（高频、并发任务驱动）。
    portname_health: Arc<RwLock<HashMap<String, HealthStatus>>>,
    /// 健康检查目标表（端点 → 目标描述）。
    health_check_targets: Arc<RwLock<HashMap<String, HealthCheckTarget>>>,
    /// 活跃流量通知器（端点 → Notify）：真实业务请求到来时触发，重置 canary 计时器。
    health_check_notifiers: Arc<RwLock<HashMap<String, Arc<tokio::sync::Notify>>>>,
    /// 新端点注册事件发送端（注册时发送端点名称，供 HealthCheckManager 监听）。
    new_portname_tx: mpsc::UnboundedSender<String>,
    /// 新端点注册事件接收端（只能被取走一次，防止多消费者竞态）。
    new_portname_rx: Arc<parking_lot::Mutex<Option<mpsc::UnboundedReceiver<String>>>>,
    /// 参与聚合健康判定的端点名称列表（空 = 由 health-check target 或 system_health 决定）。
    use_portname_health_status: Vec<String>,
    /// HTTP `/health` 路径（供 system_status_server 使用）。
    health_path: String,
    /// HTTP `/live` 路径（供 system_status_server 使用）。
    live_path: String,
    /// 进程启动时间（用于 uptime 计算）。
    start_time: Instant,
    /// Prometheus uptime gauge（初始化后存入，用 OnceLock 保证至多一次注册）。
    uptime_gauge: std::sync::OnceLock<prometheus::Gauge>,
}

impl SystemHealth {
    // ── 构造 ──────────────────────────────────────────────────────

    /// 创建并初始化 SystemHealth。
    ///
    /// `use_portname_health_status` 若非空，则 `get_health_status()` 按这些端点聚合；
    /// 传入空 vec 时退化为"有 health-check target 就按 target 聚合，否则用 system_health"。
    pub fn new(
        starting_health_status: HealthStatus,
        use_portname_health_status: Vec<String>,
        health_path: String,
        live_path: String,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();

        // 预填充 portname_health，使声明的端点先占位为初始状态（避免 /health 误判）
        let portname_health: HashMap<String, HealthStatus> = use_portname_health_status
            .iter()
            .map(|name| (name.clone(), starting_health_status))
            .collect();

        Self {
            system_health: starting_health_status,
            portname_health: Arc::new(RwLock::new(portname_health)),
            health_check_targets: Arc::new(RwLock::new(HashMap::new())),
            health_check_notifiers: Arc::new(RwLock::new(HashMap::new())),
            new_portname_tx: tx,
            new_portname_rx: Arc::new(parking_lot::Mutex::new(Some(rx))),
            use_portname_health_status,
            health_path,
            live_path,
            start_time: Instant::now(),
            uptime_gauge: std::sync::OnceLock::new(),
        }
    }

    /// 使用默认路径（`/health`、`/live`）和默认初始状态创建简化版。
    pub fn with_defaults() -> Self {
        Self::new(
            HealthStatus::Starting,
            Vec::new(),
            "/health".to_string(),
            "/live".to_string(),
        )
    }

    // ── 系统级状态 ────────────────────────────────────────────────

    /// 设置系统级健康状态（低频，生命周期切换调用）。
    pub fn set_health_status(&mut self, status: HealthStatus) {
        self.system_health = status;
    }

    /// 获取系统级状态。
    pub fn get_system_health_status(&self) -> HealthStatus {
        self.system_health
    }

    // ── 端点级状态 ────────────────────────────────────────────────

    /// 设置指定端点的健康状态（高频，并发安全）。
    pub fn set_portname_health_status(&self, portname: &str, status: HealthStatus) {
        self.portname_health.write().insert(portname.to_owned(), status);
    }

    /// 获取指定端点的健康状态。
    pub fn get_portname_health_status(&self, portname: &str) -> Option<HealthStatus> {
        self.portname_health.read().get(portname).copied()
    }

    // ── 兼容接口（与旧版 set_endpoint_health 兼容）──────────────

    /// 设置端点健康状态（兼容接口，内部委托 portname_health）。
    pub fn set_endpoint_health(&self, endpoint: &str, status: HealthStatus) {
        self.portname_health.write().insert(endpoint.to_owned(), status);
    }

    /// 注册端点并设置初始状态（用于 PortName 启动流程）。
    pub fn register_endpoint(&self, endpoint: String, initial_status: HealthStatus) {
        self.portname_health.write().entry(endpoint).or_insert(initial_status);
    }

    /// 从健康检查注销端点（用于 PortName cleanup）。
    pub fn unregister_endpoint(&self, endpoint: &str) {
        self.portname_health.write().remove(endpoint);
        self.health_check_targets.write().remove(endpoint);
        self.health_check_notifiers.write().remove(endpoint);
    }

    /// 获取端点健康状态（兼容接口）。
    pub fn get_endpoint_health(&self, endpoint: &str) -> Option<HealthStatus> {
        self.portname_health.read().get(endpoint).copied()
    }

    // ── 健康注册表 ────────────────────────────────────────────────

    /// 注册健康检查目标。
    ///
    /// 1. 原子化检查并插入 target（重复注册记录 warn 并返回）
    /// 2. 幂等创建 notifier（活跃流量通知器）
    /// 3. portname 状态保守初始化为 NotReady
    /// 4. 发送注册事件给 HealthCheckManager
    pub fn register_health_check_target(
        &self,
        portname_subject: &str,
        instance: servicegroup::Instance,
        payload: serde_json::Value,
    ) {
        // Step 1: 原子化检查并插入
        {
            let mut targets = self.health_check_targets.write();
            if targets.contains_key(portname_subject) {
                tracing::warn!(
                    portname = %portname_subject,
                    "duplicate health check target registration (ignored)"
                );
                return;
            }
            targets.insert(
                portname_subject.to_owned(),
                HealthCheckTarget { instance, payload },
            );
        }
        // Step 2: 幂等创建 notifier
        {
            let mut notifiers = self.health_check_notifiers.write();
            notifiers
                .entry(portname_subject.to_owned())
                .or_insert_with(|| Arc::new(tokio::sync::Notify::new()));
        }
        // Step 3: 保守初始化端点状态
        {
            let mut portnames = self.portname_health.write();
            portnames
                .entry(portname_subject.to_owned())
                .or_insert(HealthStatus::NotReady);
        }
        // Step 4: 通知 HealthCheckManager
        let _ = self.new_portname_tx.send(portname_subject.to_owned());
        tracing::debug!(portname = %portname_subject, "health check target registered");
    }

    /// 获取所有健康检查目标（克隆返回，供 health_check::start() 使用）。
    pub fn get_health_check_targets(&self) -> Vec<HealthCheckTarget> {
        self.health_check_targets.read().values().cloned().collect()
    }

    /// 获取所有已注册的端点名称（克隆返回）。
    pub fn get_health_check_portnames(&self) -> Vec<String> {
        self.health_check_targets.read().keys().cloned().collect()
    }

    /// 获取单个端点的健康检查目标。
    pub fn get_health_check_target(&self, portname: &str) -> Option<HealthCheckTarget> {
        self.health_check_targets.read().get(portname).cloned()
    }

    /// 是否已注册任何健康检查目标。
    pub fn has_health_check_targets(&self) -> bool {
        !self.health_check_targets.read().is_empty()
    }

    /// 获取活跃流量通知器（canary 计时器重置用）。
    pub fn get_portname_health_check_notifier(
        &self,
        portname: &str,
    ) -> Option<Arc<tokio::sync::Notify>> {
        self.health_check_notifiers.read().get(portname).cloned()
    }

    /// 取走新端点注册事件接收端（只能被取走一次）。
    pub fn take_new_portname_receiver(
        &self,
    ) -> Option<mpsc::UnboundedReceiver<String>> {
        self.new_portname_rx.lock().take()
    }

    // ── 三层健康判定 ──────────────────────────────────────────────

    /// 计算整体健康状态（三层回退逻辑）。
    ///
    /// 返回 `(is_healthy: bool, portname_detail: HashMap<portname, status_str>)`
    ///
    /// 优先级：
    /// 1. 显式指定的 `use_portname_health_status` 端点集合
    /// 2. 已注册健康检查目标集合
    /// 3. 系统级 `system_health`
    pub fn get_health_status(&self) -> (bool, HashMap<String, String>) {
        let portname_health = self.portname_health.read();

        // 构建明细
        let all_portnames: Vec<String> = {
            let targets = self.health_check_targets.read();
            targets.keys().cloned().collect()
        };

        let mut detail: HashMap<String, String> = all_portnames
            .iter()
            .map(|pn| {
                let status = portname_health
                    .get(pn.as_str())
                    .copied()
                    .unwrap_or(HealthStatus::NotReady);
                (pn.clone(), status.to_string())
            })
            .collect();

        // 层 1：use_portname_health_status 显式指定
        if !self.use_portname_health_status.is_empty() {
            let is_healthy = self.use_portname_health_status.iter().all(|pn| {
                portname_health
                    .get(pn.as_str())
                    .copied()
                    .unwrap_or(HealthStatus::NotReady)
                    == HealthStatus::Ready
            });
            // 补全 detail
            for pn in &self.use_portname_health_status {
                detail.entry(pn.clone()).or_insert_with(|| HealthStatus::NotReady.to_string());
            }
            return (is_healthy, detail);
        }

        // 层 2：按已注册目标聚合
        if self.has_health_check_targets() {
            let is_healthy = all_portnames.iter().all(|pn| {
                portname_health
                    .get(pn.as_str())
                    .copied()
                    .unwrap_or(HealthStatus::NotReady)
                    == HealthStatus::Ready
            });
            return (is_healthy, detail);
        }

        // 层 3：退化为系统级状态
        (self.system_health == HealthStatus::Ready, detail)
    }

    /// 简单判断整体是否健康。
    pub fn is_healthy(&self) -> bool {
        self.get_health_status().0
    }

    /// 是否有任何端点仍处于 Starting 状态。
    pub fn is_starting(&self) -> bool {
        self.portname_health.read().values().any(|s| *s == HealthStatus::Starting)
    }

    /// 返回所有端点的状态快照。
    pub fn snapshot(&self) -> HashMap<String, HealthStatus> {
        self.portname_health.read().clone()
    }

    /// 清除所有已注册端点。
    pub fn clear(&self) {
        self.portname_health.write().clear();
    }

    // ── Uptime 与 Prometheus ──────────────────────────────────────

    /// 进程运行时长。
    pub fn uptime(&self) -> std::time::Duration {
        self.start_time.elapsed()
    }

    /// 向指标注册表注册 uptime gauge（至多一次）。
    pub fn initialize_uptime_gauge(&self, registry: &crate::metrics::MetricsRegistry) {
        let result = self.uptime_gauge.get_or_init(|| {
            let gauge = prometheus::Gauge::new(
                "pagoda_uptime_seconds",
                "Process uptime in seconds",
            )
            .expect("uptime gauge creation should not fail");
            registry
                .prometheus_registry()
                .register(Box::new(gauge.clone()))
                .ok();
            gauge
        });
        let _ = result;
    }

    /// 将当前 uptime 写入 Prometheus gauge（在 scrape 前或定时调用）。
    pub fn update_uptime_gauge(&self) {
        if let Some(gauge) = self.uptime_gauge.get() {
            gauge.set(self.start_time.elapsed().as_secs_f64());
        }
    }

    // ── HTTP 路径元数据 ───────────────────────────────────────────

    /// `/health` 路径。
    pub fn health_path(&self) -> &str {
        &self.health_path
    }

    /// `/live` 路径。
    pub fn live_path(&self) -> &str {
        &self.live_path
    }
}

impl Default for SystemHealth {
    fn default() -> Self {
        Self::with_defaults()
    }
}

impl std::fmt::Debug for SystemHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SystemHealth")
            .field("system_health", &self.system_health)
            .field("health_path", &self.health_path)
            .field("live_path", &self.live_path)
            .field("uptime_secs", &self.start_time.elapsed().as_secs())
            .finish()
    }
}

// ─── 兼容占位（HealthCheckManager 从 health_check.rs 移走后保留空壳类型名）

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_system_is_healthy() {
        let health = SystemHealth::with_defaults();
        assert!(health.is_healthy());
    }

    #[test]
    fn system_level_health_defaults_to_starting() {
        let health = SystemHealth::with_defaults();
        assert_eq!(health.get_system_health_status(), HealthStatus::Starting);
    }

    #[test]
    fn single_ready_endpoint_is_healthy() {
        let health = SystemHealth::with_defaults();
        health.set_endpoint_health("engine", HealthStatus::Ready);
        assert!(health.is_healthy());
    }

    #[test]
    fn not_ready_endpoint_after_hc_target_makes_system_unhealthy() {
        let health = SystemHealth::with_defaults();
        let inst = servicegroup::Instance {
            namespace: "ns".into(),
            servicegroup: "sg".into(),
            portname: "generate".into(),
            instance_id: 1,
            transport: servicegroup::TransportType::Http("http://localhost:8080".into()),
            topo_json: serde_json::Value::Null,
        };
        health.register_health_check_target("ns/sg/generate", inst, serde_json::json!({}));
        // 刚注册时为 NotReady
        assert!(!health.is_healthy());
    }

    #[test]
    fn use_portname_health_status_layer_takes_priority() {
        let health = SystemHealth::new(
            HealthStatus::Ready,
            vec!["engine".to_string()],
            "/health".into(),
            "/live".into(),
        );
        // engine 预填充为 Ready（来自 starting_health_status）
        assert!(health.is_healthy());
        health.set_portname_health_status("engine", HealthStatus::NotReady);
        assert!(!health.is_healthy());
    }

    #[test]
    fn take_new_portname_receiver_once() {
        let health = SystemHealth::with_defaults();
        assert!(health.take_new_portname_receiver().is_some());
        assert!(health.take_new_portname_receiver().is_none());
    }

    #[test]
    fn uptime_increases_over_time() {
        let health = SystemHealth::with_defaults();
        let d1 = health.uptime();
        std::thread::sleep(std::time::Duration::from_millis(5));
        let d2 = health.uptime();
        assert!(d2 >= d1);
    }

    #[test]
    fn health_path_and_live_path() {
        let health = SystemHealth::new(
            HealthStatus::Ready,
            vec![],
            "/custom/health".into(),
            "/custom/live".into(),
        );
        assert_eq!(health.health_path(), "/custom/health");
        assert_eq!(health.live_path(), "/custom/live");
    }
}
