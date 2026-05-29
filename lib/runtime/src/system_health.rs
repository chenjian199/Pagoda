// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! 系统健康状态监控与健康检查目标管理
//!
//! ## 设计意图
//! 为 `DistributedRuntime` 提供一份“进程全局健康视图”：
//! * 维护系统整体健康状态与每个端点的健康状态表；
//! * 管理“健康检查目标”的注册 / 查询 / 发送新端点事件；
//! * 为每个端点提供一个 `tokio::sync::Notify` 供 canary 任务推动；
//! * 跟踪进程 uptime 并更新到 Prometheus gauge。
//!
//! ## 外部契约
//! - 公开结构体 `HealthCheckTarget`（`Clone + Debug`）与 `SystemHealth`（`Clone`）；
//!   字段 `instance` / `payload` 公开可访问。
//! - `SystemHealth` 的公开方法集合（`new` / `health_check_enabled` / `set_*` / `get_*` /
//!   `register_health_check_target` / `take_new_endpoint_receiver` /
//!   `initialize_uptime_gauge` / `uptime` / `update_uptime_gauge` / `health_path` / `live_path`）签名保持不变。
//! - `take_new_endpoint_receiver` 继续返回 `Option<mpsc::UnboundedReceiver<String>>`，
//!   **不**改为 broadcast 、**不**改变只能被消费一次的语义。
//! - `get_health_status` 的判定优先级按“`use_endpoint_health_status` → 已注册目标表 →
//!   `system_health`” 三档依次回退，结果与历史实现逐字段等价。
//!
//! ## 实现要点
//! - **多样化（Rule 2）**：
//!   * 抽出私有助手 `with_endpoint_write` 封装 `endpoint_health.write().unwrap()` 访问模式，
//!     将多处重复的“取写锁 + 操作 HashMap”压平为单一闭包用例；
//!   * `get_health_status` 的嵌套 `if/else { if/else { ... } }` 展平为 `if/else if/else`
//!     级联链，语义严格等价；
//!   * `has_health_check_targets` 由 `.iter().next().is_some()` 改为 `!read().is_empty()`，
//!     成本更低、意图更明确。
//! - **不**动锼粒度（`endpoint_health` / `health_check_targets` / `health_check_notifiers`
//!   仍是独立 `RwLock`），**不**改变锁获取顺序，避免潜在死锁特征变迁。

use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
    time::Instant,
};
use tokio::sync::mpsc;

use crate::component;
use crate::config::HealthStatus;
use crate::metrics::{MetricsHierarchy, prometheus_names::distributed_runtime};

/// 健康检查目标，包含实例信息以及发起检查时要发送的 payload。
#[derive(Clone, Debug)]
pub struct HealthCheckTarget {
    pub instance: component::Instance,
    pub payload: serde_json::Value,
}

/// 系统健康状态管理器。
/// 如果配置了 `use_endpoint_health_status`，初始化时会为这些端点预填充健康状态表。
#[derive(Clone)]
pub struct SystemHealth {
    system_health: HealthStatus,
    endpoint_health: Arc<std::sync::RwLock<HashMap<String, HealthStatus>>>,
    /// 将端点 subject 映射到健康检查目标（实例信息 + payload）。
    health_check_targets: Arc<std::sync::RwLock<HashMap<String, HealthCheckTarget>>>,
    /// 将端点 subject 映射到其专属健康检查通知器。
    health_check_notifiers: Arc<std::sync::RwLock<HashMap<String, Arc<tokio::sync::Notify>>>>,
    /// 新端点注册通知通道。
    /// 这样可以避免 `HealthCheckManager` 先启动、端点后注册时出现竞态并漏掉事件。
    new_endpoint_tx: mpsc::UnboundedSender<String>,
    new_endpoint_rx: Arc<parking_lot::Mutex<Option<mpsc::UnboundedReceiver<String>>>>,
    use_endpoint_health_status: Vec<String>,
    health_check_enabled: bool,
    health_path: String,
    live_path: String,
    start_time: Instant,
    uptime_gauge: OnceLock<prometheus::Gauge>,
}

// === SECTION: SystemHealth 构造与健康状态访问 ===

impl SystemHealth {
    /// 创建系统健康状态对象，并初始化端点状态表、通知器表和注册通道。
    pub fn new(
        starting_health_status: HealthStatus,
        use_endpoint_health_status: Vec<String>,
        health_check_enabled: bool,
        health_path: String,
        live_path: String,
    ) -> Self {
        let seeded_status = match health_check_enabled {
            true => HealthStatus::NotReady,
            false => starting_health_status.clone(),
        };
        let endpoint_health = use_endpoint_health_status
            .iter()
            .cloned()
            .map(|endpoint| (endpoint, seeded_status.clone()))
            .collect::<HashMap<_, _>>();
        let (new_endpoint_tx, new_endpoint_rx) = mpsc::unbounded_channel();

        Self {
            system_health: starting_health_status,
            endpoint_health: Arc::new(std::sync::RwLock::new(endpoint_health)),
            health_check_targets: Arc::new(std::sync::RwLock::new(HashMap::new())),
            health_check_notifiers: Arc::new(std::sync::RwLock::new(HashMap::new())),
            new_endpoint_tx,
            new_endpoint_rx: Arc::new(parking_lot::Mutex::new(Some(new_endpoint_rx))),
            use_endpoint_health_status,
            health_check_enabled,
            health_path,
            live_path,
            start_time: Instant::now(),
            uptime_gauge: OnceLock::new(),
        }
    }

    /// 返回当前是否启用了健康检查机制。
    pub fn health_check_enabled(&self) -> bool {
        self.health_check_enabled
    }

    /// 私有助手：在 `endpoint_health` 写锁保护下执行给定闭包。
    ///
    /// 将“获取写锁 → `unwrap()` → 操作 `HashMap` → 释放”的重复模式集中为一个助手，
    /// 使调用点只关心业务逻辑（写什么）。锁获取顺序与历史实现严格一致。
    fn with_endpoint_write<R>(
        &self,
        op: impl FnOnce(&mut HashMap<String, HealthStatus>) -> R,
    ) -> R {
        let mut endpoint_health = self.endpoint_health.write().unwrap();
        op(&mut endpoint_health)
    }

    /// 记录端点传输层已注册。
    /// 如果未启用 canary，则直接把端点标记为 `Ready`；否则保持不变，等待 canary 验证。
    pub fn set_endpoint_registered(&self, endpoint: &str) {
        if !self.health_check_enabled {
            self.set_endpoint_health_status(endpoint, HealthStatus::Ready);
        }
    }

    /// 更新系统整体健康状态，不直接修改单个端点状态。
    pub fn set_health_status(&mut self, status: HealthStatus) {
        self.system_health = status;
    }

    /// 设置指定端点的健康状态。实现上委托给私有助手 [`Self::with_endpoint_write`]。
    pub fn set_endpoint_health_status(&self, endpoint: &str, status: HealthStatus) {
        self.with_endpoint_write(|endpoint_health| {
            endpoint_health.insert(endpoint.to_string(), status);
        });
    }

    /// 返回系统整体健康状态以及所有端点的当前状态快照。
    ///
    /// 判断流程为：优先使用显式配置的端点列表；如果没有该列表，则退化为已注册健康检查目标；
    /// 若二者都不存在，则回退到系统整体状态字段。
    pub fn get_health_status(&self) -> (bool, HashMap<String, String>) {
        let health_check_targets = self.health_check_targets.read().unwrap();
        let endpoint_health = self.endpoint_health.read().unwrap();
        let endpoints = endpoint_health
            .iter()
            .map(|(endpoint, status)| {
                let label = if *status == HealthStatus::Ready {
                    "ready"
                } else {
                    "notready"
                };
                (endpoint.clone(), label.to_string())
            })
            .collect::<HashMap<_, _>>();

        let healthy = if !self.use_endpoint_health_status.is_empty() {
            self.use_endpoint_health_status.iter().all(|endpoint| {
                endpoint_health
                    .get(endpoint)
                    .is_some_and(|status| *status == HealthStatus::Ready)
            })
        } else if !health_check_targets.is_empty() {
            // 如果已经注册了健康检查目标，则以这些目标的状态来判定整体健康性。
            health_check_targets
                .iter()
                .all(|(endpoint_subject, _target)| {
                    endpoint_health
                        .get(endpoint_subject)
                        .is_some_and(|status| *status == HealthStatus::Ready)
                })
        } else {
            // 如果没有任何健康检查目标，则退回到系统整体状态字段。
            self.system_health == HealthStatus::Ready
        };

        (healthy, endpoints)
    }

    /// 为端点注册一个健康检查目标。
    ///
    /// 处理流程为：原子写入目标表、创建通知器、初始化端点状态，并通过通道通知管理器有新端点加入。
    pub fn register_health_check_target(
        &self,
        endpoint_subject: &str,
        instance: component::Instance,
        payload: serde_json::Value,
    ) {
        let key = endpoint_subject.to_owned();

        // 在单次写锁中完成检查和插入，避免重复注册时产生竞态。
        let inserted = {
            let mut targets = self.health_check_targets.write().unwrap();
            match targets.entry(key.clone()) {
                std::collections::hash_map::Entry::Occupied(_) => false,
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert(HealthCheckTarget { instance, payload });
                    true
                }
            }
        };

        if !inserted {
            tracing::warn!(
                "Attempted to re-register health check for endpoint '{}'; ignoring.",
                key
            );
            return;
        }

        // 为该端点创建并保存一个唯一 notifier；重复执行也应保持幂等。
        {
            let mut notifiers = self.health_check_notifiers.write().unwrap();
            notifiers
                .entry(key.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Notify::new()));
        }

        // 端点初始状态保守地标记为 NotReady，等待真实探测结果覆盖。
        self.with_endpoint_write(|endpoint_health| {
            endpoint_health
                .entry(key.clone())
                .or_insert(HealthStatus::NotReady);
        });

        if let Err(e) = self.new_endpoint_tx.send(key.clone()) {
            tracing::error!(
                "Failed to send endpoint '{}' registration to health check manager: {}. \
                 Health checks will not be performed for this endpoint.",
                key,
                e
            );
        }
    }

    /// 获取全部健康检查目标的快照副本。
    pub fn get_health_check_targets(&self) -> Vec<(String, HealthCheckTarget)> {
        self.health_check_targets
            .read()
            .unwrap()
            .iter()
            .map(|(endpoint, target)| (endpoint.clone(), target.clone()))
            .collect()
    }

    /// 判断当前是否至少注册了一个健康检查目标。
    pub fn has_health_check_targets(&self) -> bool {
        !self.health_check_targets.read().unwrap().is_empty()
    }

    /// 获取已注册健康检查目标的端点列表。
    pub fn get_health_check_endpoints(&self) -> Vec<String> {
        self.health_check_targets
            .read()
            .unwrap()
            .keys()
            .cloned()
            .collect()
    }

    /// 获取指定端点对应的健康检查目标。
    pub fn get_health_check_target(&self, endpoint: &str) -> Option<HealthCheckTarget> {
        self.health_check_targets
            .read()
            .unwrap()
            .get(endpoint)
            .cloned()
    }

    /// 获取指定端点当前的健康状态。
    pub fn get_endpoint_health_status(&self, endpoint: &str) -> Option<HealthStatus> {
        self.endpoint_health.read().unwrap().get(endpoint).cloned()
    }

    /// 获取指定端点的专属健康检查 notifier。
    pub fn get_endpoint_health_check_notifier(
        &self,
        endpoint_subject: &str,
    ) -> Option<Arc<tokio::sync::Notify>> {
        self.health_check_notifiers
            .read()
            .unwrap()
            .get(endpoint_subject)
            .cloned()
    }

    /// 取走新端点注册事件接收端。
    /// 该方法只能成功一次，供 `HealthCheckManager` 独占消费注册事件。
    pub fn take_new_endpoint_receiver(&self) -> Option<mpsc::UnboundedReceiver<String>> {
        let mut receiver = self.new_endpoint_rx.lock();
        receiver.take()
    }

    /// 使用传入的指标注册器初始化 uptime gauge。
    pub fn initialize_uptime_gauge<T: MetricsHierarchy>(&self, registry: &T) -> anyhow::Result<()> {
        let gauge = registry.metrics().create_gauge(
            distributed_runtime::UPTIME_SECONDS,
            "Total uptime of the DistributedRuntime in seconds",
            &[],
        )?;
        if self.uptime_gauge.set(gauge).is_err() {
            return Err(anyhow::anyhow!("uptime_gauge already initialized"));
        }
        Ok(())
    }

    /// 计算自对象创建以来的当前运行时长。
    pub fn uptime(&self) -> std::time::Duration {
        Instant::elapsed(&self.start_time)
    }

    /// 用当前 uptime 刷新指标 gauge 的值。
    pub fn update_uptime_gauge(&self) {
        let seconds = self.uptime().as_secs_f64();
        if let Some(gauge) = self.uptime_gauge.get() {
            gauge.set(seconds);
        }
    }

    /// 返回健康检查 HTTP 路径。
    pub fn health_path(&self) -> &str {
        self.health_path.as_str()
    }

    /// 返回存活检查 HTTP 路径。
    pub fn live_path(&self) -> &str {
        self.live_path.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::{Instance, TransportType};
    use crate::metrics::MetricsRegistry;

    struct TestMetricsHierarchy {
        registry: MetricsRegistry,
    }

    impl TestMetricsHierarchy {
        /// 构造一个带独立指标注册器的测试层级对象。
        fn new() -> Self {
            Self {
                registry: MetricsRegistry::new(),
            }
        }
    }

    impl MetricsHierarchy for TestMetricsHierarchy {
        fn basename(&self) -> String {
            "test".to_string()
        }

        fn parent_hierarchies(&self) -> Vec<&dyn MetricsHierarchy> {
            vec![]
        }

        fn get_metrics_registry(&self) -> &MetricsRegistry {
            &self.registry
        }
    }

    /// 构造一个最小可用的测试实例，便于注册健康检查目标。
    fn sample_instance(endpoint: &str) -> Instance {
        Instance {
            component: "component".to_string(),
            endpoint: endpoint.to_string(),
            namespace: "namespace".to_string(),
            instance_id: 1,
            transport: TransportType::Nats(endpoint.to_string()),
            device_type: None,
        }
    }

    #[test]
    /// 测试：初始化时会根据健康检查开关为端点设置不同的初始状态。
    fn test_new_initializes_endpoint_statuses_from_health_check_flag() {
        let without_canary = SystemHealth::new(
            HealthStatus::Ready,
            vec!["ep-a".to_string()],
            false,
            "/health".to_string(),
            "/live".to_string(),
        );
        let with_canary = SystemHealth::new(
            HealthStatus::Ready,
            vec!["ep-a".to_string()],
            true,
            "/health".to_string(),
            "/live".to_string(),
        );

        assert_eq!(
            without_canary.get_endpoint_health_status("ep-a"),
            Some(HealthStatus::Ready)
        );
        assert_eq!(
            with_canary.get_endpoint_health_status("ep-a"),
            Some(HealthStatus::NotReady)
        );
    }

    #[test]
    /// 测试：端点注册信号在 canary 开关不同的情况下会产生不同状态更新行为。
    fn test_set_endpoint_registered_respects_health_check_flag() {
        let disabled = SystemHealth::new(
            HealthStatus::NotReady,
            vec!["ep-a".to_string()],
            false,
            "/health".to_string(),
            "/live".to_string(),
        );
        let enabled = SystemHealth::new(
            HealthStatus::NotReady,
            vec!["ep-a".to_string()],
            true,
            "/health".to_string(),
            "/live".to_string(),
        );

        disabled.set_endpoint_registered("ep-a");
        enabled.set_endpoint_registered("ep-a");

        assert_eq!(disabled.get_endpoint_health_status("ep-a"), Some(HealthStatus::Ready));
        assert_eq!(enabled.get_endpoint_health_status("ep-a"), Some(HealthStatus::NotReady));
    }

    #[test]
    /// 测试：存在显式端点列表时，整体健康性由该列表中的端点状态决定。
    fn test_get_health_status_uses_explicit_endpoint_list() {
        let system_health = SystemHealth::new(
            HealthStatus::NotReady,
            vec!["ep-a".to_string(), "ep-b".to_string()],
            false,
            "/health".to_string(),
            "/live".to_string(),
        );

        system_health.set_endpoint_health_status("ep-a", HealthStatus::Ready);
        system_health.set_endpoint_health_status("ep-b", HealthStatus::NotReady);

        let (healthy, endpoints) = system_health.get_health_status();

        assert!(!healthy);
        assert_eq!(endpoints.get("ep-a").map(String::as_str), Some("ready"));
        assert_eq!(endpoints.get("ep-b").map(String::as_str), Some("notready"));
    }

    #[test]
    /// 测试：重复注册同一健康检查目标时保持幂等，不会覆盖首次 payload。
    fn test_register_health_check_target_is_idempotent() {
        let system_health = SystemHealth::new(
            HealthStatus::Ready,
            vec![],
            true,
            "/health".to_string(),
            "/live".to_string(),
        );

        system_health.register_health_check_target(
            "ep-a",
            sample_instance("ep-a"),
            serde_json::json!({"version": 1}),
        );
        system_health.register_health_check_target(
            "ep-a",
            sample_instance("ep-a"),
            serde_json::json!({"version": 2}),
        );

        let targets = system_health.get_health_check_targets();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].0, "ep-a");
        assert_eq!(targets[0].1.payload, serde_json::json!({"version": 1}));
        assert!(system_health.get_endpoint_health_check_notifier("ep-a").is_some());
    }

    #[tokio::test]
    /// 测试：新端点注册事件接收端只能被取走一次，并能收到后续注册通知。
    async fn test_take_new_endpoint_receiver_only_once_and_receives_registrations() {
        let system_health = SystemHealth::new(
            HealthStatus::Ready,
            vec![],
            true,
            "/health".to_string(),
            "/live".to_string(),
        );

        let mut receiver = system_health.take_new_endpoint_receiver().unwrap();
        assert!(system_health.take_new_endpoint_receiver().is_none());

        system_health.register_health_check_target(
            "ep-a",
            sample_instance("ep-a"),
            serde_json::json!({"prompt": "health"}),
        );

        let endpoint = tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
            .await
            .unwrap();
        assert_eq!(endpoint.as_deref(), Some("ep-a"));
    }

    #[test]
    /// 测试：没有显式端点列表时，整体健康性会退化为已注册健康检查目标的聚合结果。
    fn test_get_health_status_uses_health_check_targets_when_no_explicit_list() {
        let system_health = SystemHealth::new(
            HealthStatus::NotReady,
            vec![],
            true,
            "/health".to_string(),
            "/live".to_string(),
        );

        system_health.register_health_check_target(
            "ep-a",
            sample_instance("ep-a"),
            serde_json::json!({"prompt": "health"}),
        );
        let (healthy_before, _) = system_health.get_health_status();
        system_health.set_endpoint_health_status("ep-a", HealthStatus::Ready);
        let (healthy_after, _) = system_health.get_health_status();

        assert!(!healthy_before);
        assert!(healthy_after);
    }

    #[test]
    /// 测试：uptime gauge 可初始化、更新，并能返回健康检查与存活检查路径。
    fn test_initialize_uptime_gauge_and_update_it() {
        let system_health = SystemHealth::new(
            HealthStatus::Ready,
            vec![],
            false,
            "/healthz".to_string(),
            "/livez".to_string(),
        );
        let hierarchy = TestMetricsHierarchy::new();

        system_health.initialize_uptime_gauge(&hierarchy).unwrap();
        assert!(system_health.initialize_uptime_gauge(&hierarchy).is_err());

        system_health.update_uptime_gauge();
        let metrics = hierarchy.metrics().prometheus_expfmt().unwrap();

        assert!(metrics.contains(distributed_runtime::UPTIME_SECONDS));
        assert_eq!(system_health.health_path(), "/healthz");
        assert_eq!(system_health.live_path(), "/livez");
        assert!(system_health.uptime() >= std::time::Duration::ZERO);
    }
}
