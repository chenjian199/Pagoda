// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Prometheus 指标树——分层注册表与子模块指标集。
//!
//! `MetricsRegistry` 包装 `prometheus::Registry`，支持树形父子聚合；
//! `MetricsHierarchy` trait 使各子系统以统一接口暴露指标。

use std::sync::Arc;

use parking_lot;

pub mod frontend_perf;
pub mod tokio_perf;
pub mod transport_metrics;
pub mod request_plane;
pub mod work_handler_perf;
pub mod prometheus_names;

// ──────────────────────── MetricsRegistry ─────────────────────────

/// 分层 Prometheus 注册表。
///
/// 每个子系统持有自身的 `MetricsRegistry`，根注册表通过 `children` 聚合全部指标。
#[derive(Clone, Debug)]
pub struct MetricsRegistry {
    inner: Arc<MetricsRegistryInner>,
}

struct MetricsRegistryInner {
    /// 底层 Prometheus Registry。
    registry: prometheus::Registry,
    /// 子注册表列表。
    children: std::sync::RwLock<Vec<MetricsRegistry>>,
    /// 该注册表的逻辑名称（用于层级展示）。
    name: String,
    /// Prometheus 抓取前的更新回调（如 uptime gauge 刷新）。
    update_callbacks: parking_lot::Mutex<Vec<Arc<dyn Fn() -> anyhow::Result<()> + Send + Sync>>>,
}

impl std::fmt::Debug for MetricsRegistryInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsRegistryInner")
            .field("name", &self.name)
            .field("update_callbacks_count", &self.update_callbacks.lock().len())
            .finish_non_exhaustive()
    }
}

impl MetricsRegistry {
    /// 创建根注册表。
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(MetricsRegistryInner {
                registry: prometheus::Registry::new(),
                children: std::sync::RwLock::new(Vec::new()),
                name: name.into(),
                update_callbacks: parking_lot::Mutex::new(Vec::new()),
            }),
        }
    }

    /// 注册 Prometheus 抓取前回调（如 uptime gauge 更新）。
    ///
    /// 每次 `encode_to_text()` 或 `gather_all()` 调用前触发所有已注册回调。
    pub fn add_update_callback(
        &self,
        callback: Arc<dyn Fn() -> anyhow::Result<()> + Send + Sync>,
    ) {
        self.inner.update_callbacks.lock().push(callback);
    }

    /// 触发所有已注册的更新回调。
    fn run_update_callbacks(&self) {
        for cb in self.inner.update_callbacks.lock().iter() {
            if let Err(e) = cb() {
                tracing::warn!("metrics update callback error: {e}");
            }
        }
    }

    /// 创建一个命名子注册表并自动注册为 child。
    pub fn create_child(&self, name: impl Into<String>) -> Self {
        let child = Self::new(name);
        self.inner
            .children
            .write()
            .expect("lock poisoned")
            .push(child.clone());
        child
    }

    /// 注册表逻辑名称。
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// 获取底层 Prometheus Registry 引用。
    pub fn prometheus_registry(&self) -> &prometheus::Registry {
        &self.inner.registry
    }

    /// 收集自身及所有子注册表的指标族。
    pub fn gather_all(&self) -> Vec<prometheus::proto::MetricFamily> {
        let mut families = self.inner.registry.gather();
        let children = self.inner.children.read().expect("lock poisoned");
        for child in children.iter() {
            families.extend(child.gather_all());
        }
        families
    }

    /// 将指标族编码为 Prometheus text exposition 格式。
    ///
    /// 编码前自动触发所有已注册的更新回调（如 uptime gauge 刷新）。
    pub fn encode_to_text(&self) -> Result<String, std::fmt::Error> {
        use prometheus::Encoder;
        self.run_update_callbacks();
        let families = self.gather_all();
        let encoder = prometheus::TextEncoder::new();
        let mut buf = Vec::new();
        encoder.encode(&families, &mut buf).map_err(|_| std::fmt::Error)?;
        String::from_utf8(buf).map_err(|_| std::fmt::Error)
    }
}

// ───────────────────── MetricsHierarchy Trait ─────────────────────

/// 可组合的指标层级接口。
///
/// 各子系统（前端、传输层、引擎等）实现该 trait 以统一暴露指标。
pub trait MetricsHierarchy: Send + Sync {
    /// 该层级的基础名称（如 `"frontend"`, `"transport"`）。
    fn basename(&self) -> &str;

    /// 父层级列表，用于构造全限定指标前缀。
    fn parent_hierarchies(&self) -> Vec<&str> {
        Vec::new()
    }

    /// 该层级使用的 `MetricsRegistry`。
    fn get_metrics_registry(&self) -> &MetricsRegistry;

    /// 收集该层级的所有指标族。
    fn metrics(&self) -> Vec<prometheus::proto::MetricFamily> {
        self.get_metrics_registry().gather_all()
    }

    /// 构造全限定前缀：`parent1_parent2_basename`。
    fn qualified_prefix(&self) -> String {
        let mut parts: Vec<&str> = self.parent_hierarchies();
        parts.push(self.basename());
        parts.join("_")
    }
}
