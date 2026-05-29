// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # Metrics 注册中心与层级化 Prometheus 接入
//!
//! ## 设计意图
//!
//! 本模块是整个 dynamo runtime 指标体系的“总入口”：它把 Prometheus 原生的多种
//! 指标类型（Counter / Gauge / Histogram / *Vec）藏在统一的
//! [`PrometheusMetric`] trait 与 [`create_metric`] 函数后面，再借助
//! [`MetricsHierarchy`]（DRT → Namespace → Component → Endpoint）实现
//! **自动标签注入**和**多 registry 合并抓取**。子模块按域拆分：
//! `frontend_perf` / `request_plane` / `tokio_perf` / `transport_metrics` /
//! `work_handler_perf` / `work_handler_pool`，以及命名常量 `prometheus_names`。
//!
//! ## 外部契约
//!
//! - [`MetricsHierarchy`]：所有可观测对象实现的层级 trait，要求暴露
//!   `basename()` / `parent_hierarchies()` / `get_metrics_registry()`，并自动
//!   获得 `.metrics()` 方法返回 [`Metrics<&Self>`] 工厂。
//! - [`Metrics`]：暴露 `create_counter` / `create_gauge` / `create_histogram` /
//!   `create_*vec` 与 `prometheus_expfmt()`，签名稳定，是绑定层（Python）
//!   依赖的对外 API。
//! - [`MetricsRegistry`]：内部持有 `prometheus::Registry` 与子 registry 列表，
//!   提供 `add_metric` / `add_metric_or_warn` / `prometheus_expfmt_combined` /
//!   `add_child_registry` / 回调（update + expfmt）等接口。
//!
//! ## 实现要点
//!
//! - **自动标签注入**：[`create_metric`] 先从 `parent_hierarchies()` 取出
//!   namespace / component / endpoint 三段名字，再加上 `worker_id`（来自
//!   `connection_id()`），作为 **const_labels** 拼到 Prometheus opts 上。
//!   用户传入的 `labels` 若撞上这四个保留名直接返回错误。
//! - **泛型分派**：用 `TypeId` 比对决定具体走 `with_opts` / `with_opts_and_label_names` /
//!   `with_histogram_opts_and_buckets`，避免为每种指标类型重复写 9 套 `create_*`。
//! - **合并抓取**：[`MetricsRegistry::prometheus_expfmt_combined`] 在汇总
//!   父 registry + 子 registry 时调用 `text` 编码并去重 `# HELP` / `# TYPE`，
//!   保证多源同名 family 不会被 prometheus 解析器拒绝。
//! - **回调机制**：`add_update_callback` 在抓取前同步收集动态状态（如 KV cache 用量），
//!   `add_expfmt_callback` 直接拼追加文本（用于桥接外部 Python 指标）。
//!
//! ## 子模块
//!
//! 见各子模块文件级文档；它们都使用 `Lazy<静态指标>` + 双 OnceCell 模式独立
//! 实现自己的注册入口，与本主模块只通过 [`MetricsRegistry::add_metric`] 交互。

pub mod frontend_perf;
pub mod prometheus_names;
pub mod request_plane;
pub mod tokio_perf;
pub mod transport_metrics;
pub mod work_handler_perf;
pub mod work_handler_pool;

use parking_lot::Mutex;
use std::collections::HashSet;
use std::sync::Arc;

use crate::component::ComponentBuilder;
use anyhow;
use once_cell::sync::Lazy;
use regex::Regex;
use std::any::Any;
use std::collections::HashMap;

// Import commonly used items to avoid verbose prefixes
use prometheus_names::{
    build_component_metric_name, labels, name_prefix, sanitize_prometheus_label,
    sanitize_prometheus_name, work_handler,
};

// Pipeline imports for endpoint creation
use crate::pipeline::{
    AsyncEngine, AsyncEngineContextProvider, Error, ManyOut, ResponseStream, SingleIn, async_trait,
    network::Ingress,
};
use crate::protocols::annotated::Annotated;
use crate::stream;
use crate::stream::StreamExt;

// Prometheus imports
use prometheus::Encoder;

/// Validate that a label slice has no duplicate keys.
/// Returns Ok(()) when all keys are unique; otherwise returns an error naming the duplicate key.
// 中文说明：
// 1. 这个辅助函数会遍历传入的标签列表，检查每个标签键名是否只出现一次。
// 2. 一旦发现重复键，就立即返回错误，避免后续指标注册时出现歧义或冲突。
fn validate_no_duplicate_label_keys(labels: &[(&str, &str)]) -> anyhow::Result<()> {
    let mut seen_keys = HashSet::with_capacity(labels.len());
    if let Some(duplicate_key) = labels
        .iter()
        .map(|(key, _)| *key)
        .find(|key| !seen_keys.insert(*key))
    {
        return Err(anyhow::anyhow!(
            "Duplicate label key '{}' found in labels",
            duplicate_key
        ));
    }

    Ok(())
}

// === SECTION: PrometheusMetric trait ===

/// 给所有 Prometheus 指标类型加上的统一构造门面。`create_metric` 据此泛型分派。
pub trait PrometheusMetric: prometheus::core::Collector + Clone + Send + Sync + 'static {
    /// Create a new metric with the given options
    fn with_opts(opts: prometheus::Opts) -> Result<Self, prometheus::Error>
    where
        Self: Sized;

    /// Create a new metric with histogram options and custom buckets
    /// This is a default implementation that will panic for non-histogram metrics
    // 中文说明：
    // 1. 这是给不支持 Histogram 的指标类型准备的默认实现。
    // 2. 如果调用方误把这套接口用于非 Histogram 指标，这里会直接 panic，尽早暴露使用错误。
    fn with_histogram_opts_and_buckets(
        _opts: prometheus::HistogramOpts,
        _buckets: Option<Vec<f64>>,
    ) -> Result<Self, prometheus::Error>
    where
        Self: Sized,
    {
        let message = "with_histogram_opts_and_buckets is not implemented for this metric type";
        panic!("{message}");
    }

    /// Create a new metric with counter options and label names (for CounterVec)
    /// This is a default implementation that will panic for non-countervec metrics
    // 中文说明：
    // 1. 这是给不支持带标签名构造的指标类型准备的默认实现。
    // 2. 如果错误地在不兼容的指标类型上调用它，会直接 panic，避免静默产生错误结果。
    fn with_opts_and_label_names(
        _opts: prometheus::Opts,
        _label_names: &[&str],
    ) -> Result<Self, prometheus::Error>
    where
        Self: Sized,
    {
        let message = "with_opts_and_label_names is not implemented for this metric type";
        panic!("{message}");
    }
}

// Implement the trait for Counter, IntCounter, and Gauge
impl PrometheusMetric for prometheus::Counter {
    // 中文说明：把统一的 trait 构造入口转发给 Counter 自己的 with_opts，并原样返回构造结果。
    fn with_opts(opts: prometheus::Opts) -> Result<Self, prometheus::Error> {
        let metric = prometheus::Counter::with_opts(opts)?;
        Ok(metric)
    }
}

impl PrometheusMetric for prometheus::IntCounter {
    // 中文说明：把统一的 trait 构造入口转发给 IntCounter 的原生构造函数。
    fn with_opts(opts: prometheus::Opts) -> Result<Self, prometheus::Error> {
        let metric = prometheus::IntCounter::with_opts(opts)?;
        Ok(metric)
    }
}

impl PrometheusMetric for prometheus::Gauge {
    // 中文说明：使用 Gauge 自带的 with_opts 完成真实指标对象创建。
    fn with_opts(opts: prometheus::Opts) -> Result<Self, prometheus::Error> {
        let metric = prometheus::Gauge::with_opts(opts)?;
        Ok(metric)
    }
}

impl PrometheusMetric for prometheus::IntGauge {
    // 中文说明：通过 IntGauge 的原生接口创建指标，并保持错误传播行为不变。
    fn with_opts(opts: prometheus::Opts) -> Result<Self, prometheus::Error> {
        let metric = prometheus::IntGauge::with_opts(opts)?;
        Ok(metric)
    }
}

impl PrometheusMetric for prometheus::GaugeVec {
    // 中文说明：GaugeVec 必须显式提供动态标签名，因此这里直接返回说明性错误而不是继续构造。
    fn with_opts(_opts: prometheus::Opts) -> Result<Self, prometheus::Error> {
        let message =
            "GaugeVec requires label names, use with_opts_and_label_names instead".to_string();
        Err(prometheus::Error::Msg(message))
    }

    // 中文说明：根据传入的指标选项和标签名列表真正创建 GaugeVec。
    fn with_opts_and_label_names(
        opts: prometheus::Opts,
        label_names: &[&str],
    ) -> Result<Self, prometheus::Error> {
        let metric = prometheus::GaugeVec::new(opts, label_names)?;
        Ok(metric)
    }
}

impl PrometheusMetric for prometheus::IntGaugeVec {
    // 中文说明：IntGaugeVec 也依赖动态标签名，所以未提供标签名时直接返回错误。
    fn with_opts(_opts: prometheus::Opts) -> Result<Self, prometheus::Error> {
        let message = "IntGaugeVec requires label names, use with_opts_and_label_names instead"
            .to_string();
        Err(prometheus::Error::Msg(message))
    }

    // 中文说明：使用传入的标签名集合创建 IntGaugeVec 实例。
    fn with_opts_and_label_names(
        opts: prometheus::Opts,
        label_names: &[&str],
    ) -> Result<Self, prometheus::Error> {
        let metric = prometheus::IntGaugeVec::new(opts, label_names)?;
        Ok(metric)
    }
}

impl PrometheusMetric for prometheus::IntCounterVec {
    // 中文说明：IntCounterVec 没有标签名时无法正常构造，这里明确返回错误信息。
    fn with_opts(_opts: prometheus::Opts) -> Result<Self, prometheus::Error> {
        let message = "IntCounterVec requires label names, use with_opts_and_label_names instead"
            .to_string();
        Err(prometheus::Error::Msg(message))
    }

    // 中文说明：基于统一 opts 和外部提供的标签名列表创建 IntCounterVec。
    fn with_opts_and_label_names(
        opts: prometheus::Opts,
        label_names: &[&str],
    ) -> Result<Self, prometheus::Error> {
        let metric = prometheus::IntCounterVec::new(opts, label_names)?;
        Ok(metric)
    }
}

// Implement the trait for Histogram
impl PrometheusMetric for prometheus::Histogram {
    // 中文说明：先把通用 Opts 转成 HistogramOpts，再构造 Histogram 指标。
    fn with_opts(opts: prometheus::Opts) -> Result<Self, prometheus::Error> {
        let histogram_opts = prometheus::HistogramOpts::new(opts.name, opts.help);
        let histogram = prometheus::Histogram::with_opts(histogram_opts)?;
        Ok(histogram)
    }

    // 中文说明：如果调用方提供了自定义桶，就先覆盖默认桶配置，然后再创建 Histogram。
    fn with_histogram_opts_and_buckets(
        opts: prometheus::HistogramOpts,
        buckets: Option<Vec<f64>>,
    ) -> Result<Self, prometheus::Error> {
        let opts = match buckets {
            Some(custom_buckets) => opts.buckets(custom_buckets),
            None => opts,
        };
        let histogram = prometheus::Histogram::with_opts(opts)?;
        Ok(histogram)
    }
}

// Implement the trait for CounterVec
impl PrometheusMetric for prometheus::CounterVec {
    // 中文说明：CounterVec 必须伴随标签名一起创建，因此误用此入口时直接 panic。
    fn with_opts(_opts: prometheus::Opts) -> Result<Self, prometheus::Error> {
        let message = "CounterVec requires label names, use with_opts_and_label_names instead";
        panic!("{message}");
    }

    // 中文说明：使用传入的 opts 和标签名列表创建 CounterVec 指标。
    fn with_opts_and_label_names(
        opts: prometheus::Opts,
        label_names: &[&str],
    ) -> Result<Self, prometheus::Error> {
        let metric = prometheus::CounterVec::new(opts, label_names)?;
        Ok(metric)
    }
}

// === SECTION: 指标工厂（create_metric）===

/// 统一的指标创建入口（Python 绑定也复用本函数）。
// 中文说明：
// 1. 这是整个文件里统一的指标创建入口，负责拼装层级名称、校验标签并根据具体指标类型走不同构造分支。
// 2. 它会先检查用户标签是否重复、是否和自动注入标签冲突，再从层级信息中补齐 namespace、component、endpoint、worker_id 等常量标签。
// 3. 随后根据泛型 T 的实际类型决定是创建普通指标、Vec 指标还是 Histogram，并对 buckets、const_labels 这些参数做对应约束检查。
// 4. 指标创建成功后，还会把 collector 注册进当前层级的 MetricsRegistry，确保后续抓取时能真正暴露出来。
pub fn create_metric<T: PrometheusMetric, H: MetricsHierarchy + ?Sized>(
    hierarchy: &H,
    metric_name: &str,
    metric_desc: &str,
    labels: &[(&str, &str)],
    buckets: Option<Vec<f64>>,
    const_labels: Option<&[&str]>,
) -> anyhow::Result<T> {
    validate_no_duplicate_label_keys(labels)?;

    let parent_hierarchies = hierarchy.parent_hierarchies();
    let mut hierarchy_names = Vec::with_capacity(parent_hierarchies.len() + 1);
    hierarchy_names.extend(parent_hierarchies.iter().map(|parent| parent.basename()));
    hierarchy_names.push(hierarchy.basename());

    let metric_name = build_component_metric_name(metric_name);

    let reserved_label_names = [
        labels::NAMESPACE,
        labels::COMPONENT,
        labels::ENDPOINT,
        labels::WORKER_ID,
    ];

    if let Some((key, _)) = labels
        .iter()
        .find(|(key, _)| reserved_label_names.contains(key))
    {
        return Err(anyhow::anyhow!(
            "Label '{}' is automatically added by auto-label injection and cannot be manually set",
            key
        ));
    }

    if let Some(conflicting_name) = const_labels.and_then(|label_names| {
        label_names
            .iter()
            .copied()
            .find(|name| reserved_label_names.contains(name))
    }) {
        return Err(anyhow::anyhow!(
            "Variable label name '{}' conflicts with auto-injected const label and cannot be used",
            conflicting_name
        ));
    }

    let mut updated_labels: Vec<(String, String)> = Vec::with_capacity(labels.len() + 4);
    for (index, label_name) in [
        (1usize, labels::NAMESPACE),
        (2usize, labels::COMPONENT),
        (3usize, labels::ENDPOINT),
    ] {
        let Some(raw_value) = hierarchy_names.get(index) else {
            continue;
        };

        if raw_value.is_empty() {
            continue;
        }

        let sanitized_value = sanitize_prometheus_label(raw_value)?;
        if sanitized_value.is_empty() {
            continue;
        }

        updated_labels.push((label_name.to_string(), sanitized_value));
    }

    if let Some(conn_id) = hierarchy.connection_id() {
        updated_labels.push((labels::WORKER_ID.to_string(), format!("{:x}", conn_id)));
    }

    updated_labels.extend(
        labels
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string())),
    );

    let build_opts = || {
        updated_labels.iter().fold(
            prometheus::Opts::new(&metric_name, metric_desc),
            |opts, (key, value)| opts.const_label(key.clone(), value.clone()),
        )
    };
    let build_histogram_opts = || {
        updated_labels.iter().fold(
            prometheus::HistogramOpts::new(&metric_name, metric_desc),
            |opts, (key, value)| opts.const_label(key.clone(), value.clone()),
        )
    };

    let metric_type = std::any::TypeId::of::<T>();
    let prometheus_metric = if metric_type == std::any::TypeId::of::<prometheus::CounterVec>() {
        if buckets.is_some() {
            return Err(anyhow::anyhow!(
                "buckets parameter is not valid for CounterVec"
            ));
        }
        let label_names = const_labels
            .ok_or_else(|| anyhow::anyhow!("CounterVec requires const_labels parameter"))?;
        T::with_opts_and_label_names(build_opts(), label_names)?
    } else if metric_type == std::any::TypeId::of::<prometheus::GaugeVec>() {
        if buckets.is_some() {
            return Err(anyhow::anyhow!(
                "buckets parameter is not valid for GaugeVec"
            ));
        }
        let label_names = const_labels
            .ok_or_else(|| anyhow::anyhow!("GaugeVec requires const_labels parameter"))?;
        T::with_opts_and_label_names(build_opts(), label_names)?
    } else if metric_type == std::any::TypeId::of::<prometheus::Histogram>() {
        if const_labels.is_some() {
            return Err(anyhow::anyhow!(
                "const_labels parameter is not valid for Histogram"
            ));
        }
        T::with_histogram_opts_and_buckets(build_histogram_opts(), buckets)?
    } else if metric_type == std::any::TypeId::of::<prometheus::IntCounterVec>() {
        if buckets.is_some() {
            return Err(anyhow::anyhow!(
                "buckets parameter is not valid for IntCounterVec"
            ));
        }
        let label_names = const_labels
            .ok_or_else(|| anyhow::anyhow!("IntCounterVec requires const_labels parameter"))?;
        T::with_opts_and_label_names(build_opts(), label_names)?
    } else if metric_type == std::any::TypeId::of::<prometheus::IntGaugeVec>() {
        if buckets.is_some() {
            return Err(anyhow::anyhow!(
                "buckets parameter is not valid for IntGaugeVec"
            ));
        }
        let label_names = const_labels
            .ok_or_else(|| anyhow::anyhow!("IntGaugeVec requires const_labels parameter"))?;
        T::with_opts_and_label_names(build_opts(), label_names)?
    } else {
        if buckets.is_some() {
            return Err(anyhow::anyhow!(
                "buckets parameter is not valid for Counter, IntCounter, Gauge, or IntGauge"
            ));
        }
        if const_labels.is_some() {
            return Err(anyhow::anyhow!(
                "const_labels parameter is not valid for Counter, IntCounter, Gauge, or IntGauge"
            ));
        }
        T::with_opts(build_opts())?
    };

    let collector: Box<dyn prometheus::core::Collector> = Box::new(prometheus_metric.clone());
    hierarchy.get_metrics_registry().add_metric(collector)?;

    Ok(prometheus_metric)
}

/// Wrapper struct that provides access to metrics functionality
/// This struct is accessed via the `.metrics()` method on DistributedRuntime, Namespace, Component, and Endpoint
pub struct Metrics<H: MetricsHierarchy> {
    hierarchy: H,
}

impl<H: MetricsHierarchy> Metrics<H> {
    // 中文说明：保存传入的层级对象，后续所有 create_* 方法都会通过它定位对应的 metrics registry。
    pub fn new(hierarchy: H) -> Self {
        let metrics = Self { hierarchy };
        metrics
    }

    // TODO: Add support for additional Prometheus metric types:
    // - Counter: ✅ IMPLEMENTED - create_counter()
    // - CounterVec: ✅ IMPLEMENTED - create_countervec()
    // - Gauge: ✅ IMPLEMENTED - create_gauge()
    // - GaugeVec: ✅ IMPLEMENTED - create_gaugevec()
    // - GaugeHistogram: create_gauge_histogram() - for gauge histograms
    // - Histogram: ✅ IMPLEMENTED - create_histogram()
    // - HistogramVec with custom buckets: create_histogram_with_buckets()
    // - Info: create_info() - for info metrics with labels
    // - IntCounter: ✅ IMPLEMENTED - create_intcounter()
    // - IntCounterVec: ✅ IMPLEMENTED - create_intcountervec()
    // - IntGauge: ✅ IMPLEMENTED - create_intgauge()
    // - IntGaugeVec: ✅ IMPLEMENTED - create_intgaugevec()
    // - Stateset: create_stateset() - for state-based metrics
    // - Summary: create_summary() - for quantiles and sum/count metrics
    // - SummaryVec: create_summary_vec() - for labeled summaries
    // - Untyped: create_untyped() - for untyped metrics
    //
    // NOTE: The order of create_* methods below is mirrored in lib/bindings/python/rust/lib.rs::Metrics
    // Keep them synchronized when adding new metric types

    /// Create a Counter metric
    // 中文说明：创建一个无动态标签的 Counter 指标，并把请求转发给统一的 create_metric 入口。
    pub fn create_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> anyhow::Result<prometheus::Counter> {
        let counter = create_metric(&self.hierarchy, name, description, labels, None, None)?;
        Ok(counter)
    }

    /// Create a CounterVec metric with label names (for dynamic labels)
    // 中文说明：创建 CounterVec，并把动态标签名和值分别传给统一创建逻辑。
    pub fn create_countervec(
        &self,
        name: &str,
        description: &str,
        const_labels: &[&str],
        const_label_values: &[(&str, &str)],
    ) -> anyhow::Result<prometheus::CounterVec> {
        let counter_vec = create_metric(
            &self.hierarchy,
            name,
            description,
            const_label_values,
            None,
            Some(const_labels),
        )?;
        Ok(counter_vec)
    }

    /// Create a Gauge metric
    // 中文说明：创建一个普通 Gauge 指标，不带额外的动态标签名定义。
    pub fn create_gauge(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> anyhow::Result<prometheus::Gauge> {
        let gauge = create_metric(&self.hierarchy, name, description, labels, None, None)?;
        Ok(gauge)
    }

    /// Create a GaugeVec metric with label names (for dynamic labels)
    // 中文说明：创建 GaugeVec，把动态标签名和对应常量标签值一起交给通用入口处理。
    pub fn create_gaugevec(
        &self,
        name: &str,
        description: &str,
        const_labels: &[&str],
        const_label_values: &[(&str, &str)],
    ) -> anyhow::Result<prometheus::GaugeVec> {
        let gauge_vec = create_metric(
            &self.hierarchy,
            name,
            description,
            const_label_values,
            None,
            Some(const_labels),
        )?;
        Ok(gauge_vec)
    }

    /// Create a Histogram metric with custom buckets
    // 中文说明：创建 Histogram，并允许调用方传入自定义桶配置覆盖默认桶。
    pub fn create_histogram(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
        buckets: Option<Vec<f64>>,
    ) -> anyhow::Result<prometheus::Histogram> {
        let histogram = create_metric(&self.hierarchy, name, description, labels, buckets, None)?;
        Ok(histogram)
    }

    /// Create an IntCounter metric
    // 中文说明：创建 IntCounter，适用于以整数形式单调递增统计的场景。
    pub fn create_intcounter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> anyhow::Result<prometheus::IntCounter> {
        let int_counter = create_metric(&self.hierarchy, name, description, labels, None, None)?;
        Ok(int_counter)
    }

    /// Create an IntCounterVec metric with label names (for dynamic labels)
    // 中文说明：创建 IntCounterVec，并显式传入动态标签名集合。
    pub fn create_intcountervec(
        &self,
        name: &str,
        description: &str,
        const_labels: &[&str],
        const_label_values: &[(&str, &str)],
    ) -> anyhow::Result<prometheus::IntCounterVec> {
        let int_counter_vec = create_metric(
            &self.hierarchy,
            name,
            description,
            const_label_values,
            None,
            Some(const_labels),
        )?;
        Ok(int_counter_vec)
    }

    /// Create an IntGauge metric
    // 中文说明：创建 IntGauge，用于整数型可增可减指标。
    pub fn create_intgauge(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> anyhow::Result<prometheus::IntGauge> {
        let int_gauge = create_metric(&self.hierarchy, name, description, labels, None, None)?;
        Ok(int_gauge)
    }

    /// Create an IntGaugeVec metric with label names (for dynamic labels)
    // 中文说明：创建 IntGaugeVec，并把动态标签名与常量标签值传给统一实现。
    pub fn create_intgaugevec(
        &self,
        name: &str,
        description: &str,
        const_labels: &[&str],
        const_label_values: &[(&str, &str)],
    ) -> anyhow::Result<prometheus::IntGaugeVec> {
        let int_gauge_vec = create_metric(
            &self.hierarchy,
            name,
            description,
            const_label_values,
            None,
            Some(const_labels),
        )?;
        Ok(int_gauge_vec)
    }

    /// Get metrics in Prometheus text format
    // 中文说明：从当前层级对应的 registry 中导出 Prometheus 文本格式结果，供抓取接口直接使用。
    pub fn prometheus_expfmt(&self) -> anyhow::Result<String> {
        let registry = self.hierarchy.get_metrics_registry();
        registry.prometheus_expfmt_combined()
    }
}

/// This trait should be implemented by all metric registries, including Prometheus, Envy, OpenTelemetry, and others.
/// It offers a unified interface for creating and managing metrics, organizing sub-registries, and
/// generating output in Prometheus text format.
use crate::traits::DistributedRuntimeProvider;

pub trait MetricsHierarchy: Send + Sync {
    // ========================================================================
    // Required methods - must be implemented by all types
    // ========================================================================

    /// Get the name of this hierarchy (without any hierarchy prefix)
    fn basename(&self) -> String;

    /// Get the parent hierarchies as actual objects (not strings)
    /// Returns a vector of hierarchy references, ordered from root to immediate parent.
    /// For example, an Endpoint would return [DRT, Namespace, Component].
    fn parent_hierarchies(&self) -> Vec<&dyn MetricsHierarchy>;

    /// Get a reference to this hierarchy's metrics registry
    fn get_metrics_registry(&self) -> &MetricsRegistry;

    // ========================================================================
    // Provided methods - have default implementations
    // ========================================================================

    /// Get the connection ID (discovery instance ID) for this hierarchy level.
    ///
    /// Returns `Some(id)` when the hierarchy has access to the DistributedRuntime
    /// (e.g. Namespace, Component, Endpoint). Used by `create_metric()` to auto-inject
    /// the `worker_id` label. Returns `None` by default.
    // 中文说明：默认情况下层级对象不提供连接 ID，因此返回 None，具体类型需要时再自行覆写。
    fn connection_id(&self) -> Option<u64> {
        Option::<u64>::None
    }

    /// Access the metrics interface for this hierarchy
    /// This is a provided method that works for any type implementing MetricsHierarchy
    // 中文说明：为任意实现了 MetricsHierarchy 的对象生成一个轻量级 Metrics 包装器，便于继续调用 create_* 系列方法。
    fn metrics(&self) -> Metrics<&Self>
    where
        Self: Sized,
    {
        let metrics = Metrics::new(self);
        metrics
    }
}

// Blanket implementation for references to types that implement MetricsHierarchy
impl<T: MetricsHierarchy + ?Sized> MetricsHierarchy for &T {
    // 中文说明：把引用类型的 basename 调用继续转发给底层真实对象。
    fn basename(&self) -> String {
        let inner = *self;
        inner.basename()
    }

    // 中文说明：让引用类型也能复用底层对象的父层级查询逻辑。
    fn parent_hierarchies(&self) -> Vec<&dyn MetricsHierarchy> {
        let inner = *self;
        inner.parent_hierarchies()
    }

    // 中文说明：对引用类型透明暴露底层对象持有的 MetricsRegistry。
    fn get_metrics_registry(&self) -> &MetricsRegistry {
        let inner = *self;
        inner.get_metrics_registry()
    }

    // 中文说明：把连接 ID 查询继续委托给底层实现，避免引用包装层改变行为。
    fn connection_id(&self) -> Option<u64> {
        let inner = *self;
        inner.connection_id()
    }
}

/// Type alias for runtime callback functions to reduce complexity
///
/// This type represents an Arc-wrapped callback function that can be:
/// - Shared efficiently across multiple threads and contexts
/// - Cloned without duplicating the underlying closure
/// - Used in generic contexts requiring 'static lifetime
///
/// The Arc wrapper is included in the type to make sharing explicit.
pub type PrometheusUpdateCallback = Arc<dyn Fn() -> anyhow::Result<()> + Send + Sync + 'static>;

/// Type alias for exposition text callback functions that return Prometheus text
pub type PrometheusExpositionFormatCallback =
    Arc<dyn Fn() -> anyhow::Result<String> + Send + Sync + 'static>;

/// Structure to hold Prometheus registries and associated callbacks for a given hierarchy.
///
/// All fields are Arc-wrapped, so cloning shares state. This ensures metrics registered
/// on cloned instances (e.g., cloned Client/Endpoint) are visible to the original.
#[derive(Clone)]
pub struct MetricsRegistry {
    /// The Prometheus registry for this hierarchy.
    /// Arc-wrapped so clones share the same registry (metrics registered on clones are visible everywhere).
    pub prometheus_registry: Arc<std::sync::RwLock<prometheus::Registry>>,

    /// Child registries included when emitting combined `/metrics` output.
    ///
    /// Why this exists:
    /// - Previously, `create_metric()` registered every collector into *all* parent registries
    ///   (Endpoint → Component → Namespace → DRT) so scraping the root registry included everything.
    /// - That fan-out caused Prometheus collisions when different endpoints tried to register the
    ///   same metric name with different const-labels (descriptor mismatch).
    ///
    /// We now register metrics only into the local hierarchy registry to avoid collisions.
    /// `child_registries` rebuilds “what to scrape” as a tree of registries so `/metrics` can:
    /// - traverse registries recursively,
    /// - merge metric families into one exposition payload,
    /// - warn/drop exact duplicate series, while allowing same metric name with different labels.
    child_registries: Arc<std::sync::RwLock<Vec<MetricsRegistry>>>,

    /// Update callbacks invoked before metrics are scraped.
    /// Wrapped in Arc to preserve callbacks across clones (prevents callback loss when MetricsRegistry is cloned).
    pub prometheus_update_callbacks: Arc<std::sync::RwLock<Vec<PrometheusUpdateCallback>>>,

    /// Callbacks that return Prometheus exposition text appended to metrics output.
    /// Wrapped in Arc to preserve callbacks across clones (e.g., vLLM callbacks registered at Endpoint remain accessible at DRT).
    pub prometheus_expfmt_callbacks:
        Arc<std::sync::RwLock<Vec<PrometheusExpositionFormatCallback>>>,
}

impl std::fmt::Debug for MetricsRegistry {
    // 中文说明：自定义 Debug 输出时不展开真正的 Registry 内容，只展示 callback 数量等摘要信息，避免输出过于庞大。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let update_callback_count = self.prometheus_update_callbacks.read().unwrap().len();
        let expfmt_callback_count = self.prometheus_expfmt_callbacks.read().unwrap().len();

        f.debug_struct("MetricsRegistry")
            .field("prometheus_registry", &"<RwLock<Registry>>")
            .field(
                "prometheus_update_callbacks",
                &format!("<RwLock<Vec<Callback>>> with {} callbacks", update_callback_count),
            )
            .field(
                "prometheus_expfmt_callbacks",
                &format!("<RwLock<Vec<Callback>>> with {} callbacks", expfmt_callback_count),
            )
            .finish()
    }
}

impl MetricsRegistry {
    /// Create a new metrics registry with an empty Prometheus registry and callback lists
    // 中文说明：初始化一个全新的 MetricsRegistry，并为 registry、子节点列表以及两类回调列表分别创建共享容器。
    pub fn new() -> Self {
        let prometheus_registry = Arc::new(std::sync::RwLock::new(prometheus::Registry::new()));
        let child_registries = Arc::new(std::sync::RwLock::new(Vec::new()));
        let prometheus_update_callbacks = Arc::new(std::sync::RwLock::new(Vec::new()));
        let prometheus_expfmt_callbacks = Arc::new(std::sync::RwLock::new(Vec::new()));

        Self {
            prometheus_registry,
            child_registries,
            prometheus_update_callbacks,
            prometheus_expfmt_callbacks,
        }
    }

    /// Add a child registry to be included in combined /metrics output.
    ///
    /// Dedup is by underlying Prometheus registry pointer, so repeated registration via clones is safe.
    // 中文说明：把子 registry 挂到当前节点下，并通过底层 registry 指针去重，避免同一个子节点被重复收集。
    pub fn add_child_registry(&self, child: &MetricsRegistry) {
        let child_ptr = Arc::as_ptr(&child.prometheus_registry);
        let mut guard = self.child_registries.write().unwrap();
        let already_present = guard
            .iter()
            .any(|r| Arc::as_ptr(&r.prometheus_registry) == child_ptr)
            ;

        if !already_present {
            guard.push(child.clone());
        }
    }

    // 中文说明：递归收集当前 registry 以及所有子 registry，生成一次完整抓取时需要遍历的 registry 列表。
    fn registries_for_combined_scrape(&self) -> Vec<MetricsRegistry> {
        // Traverse child registries recursively so `prometheus_expfmt()` on any hierarchy
        // (DRT/namespace/component/endpoint) includes metrics from its descendants.
        //
        // Dedup by underlying Prometheus registry pointer so multiple paths (e.g. also registering
        // directly on the root) won't duplicate output.
        // 中文说明：深度优先遍历 registry 树，把尚未见过的底层 registry 依次加入输出列表。
        fn visit(
            registry: &MetricsRegistry,
            out: &mut Vec<MetricsRegistry>,
            seen: &mut HashSet<*const std::sync::RwLock<prometheus::Registry>>,
        ) {
            let ptr = Arc::as_ptr(&registry.prometheus_registry);
            let inserted = seen.insert(ptr);
            if !inserted {
                return;
            }

            out.push(registry.clone());

            let children = registry.child_registries.read().unwrap().clone();
            for child in children.into_iter() {
                visit(&child, out, seen);
            }
        }

        let mut registries = Vec::new();
        let mut seen_registries: HashSet<*const std::sync::RwLock<prometheus::Registry>> =
            HashSet::new();
        visit(self, &mut registries, &mut seen_registries);
        registries
    }

    /// 把当前节点与所有子节点的指标合并为一份 Prometheus exposition 文本。
    ///
    /// - 同名 family 之间合并；HELP / TYPE 必须一致。
    /// - 同 family 下允许多条 series（按 label 区分）。
    /// - 完全重复的 series（name + labels 完全一致）保留首次出现样本，
    ///   后续重复样本发出 `warn` 日志后丢弃。
    ///
    /// 中文说明（实现要点）：
    /// 1. 先递归收集当前节点 + 所有子节点的 registry 列表，并触发所有更新回调，
    ///    确保抓取前每个 registry 的内部状态已经刷新。
    /// 2. 合并阶段使用 `BTreeMap<String, MetricFamily>` 作为聚合容器：
    ///    `BTreeMap` 的遍历顺序天然按 family 名升序，最终编码时无需再做显式 sort，
    ///    输出顺序与历史实现严格一致。
    /// 3. 使用私有 `series_dedup_key` 把"family 名 + 排序后的 label 对"压成一个
    ///    可哈希字符串，集中表达"同 series"的判定逻辑，避免哈希键构造代码散落。
    /// 4. 把合并后的 family 编码为文本，再依次追加每个 registry 的 exposition
    ///    callback 输出（与历史实现完全相同的换行规则）。
    pub fn prometheus_expfmt_combined(&self) -> anyhow::Result<String> {
        use std::collections::BTreeMap;

        let registries = self.registries_for_combined_scrape();

        for registry in &registries {
            let callback_results = registry.execute_update_callbacks();
            for result in callback_results {
                if let Err(e) = result {
                    tracing::error!("Error executing metrics callback: {e}");
                }
            }
        }

        // 私有助手：把 (family 名, 排序后的 label 对) 压成一个用于 series 去重的字符串键。
        fn series_dedup_key(name: &str, labels: &[(String, String)]) -> String {
            let label_part = labels
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join(",");
            format!("{}|{}", name, label_part)
        }

        // BTreeMap 保证按 family 名升序遍历，省去最终 sort 步骤。
        let mut by_name: BTreeMap<String, prometheus::proto::MetricFamily> = BTreeMap::new();
        let mut seen_series: HashSet<String> = HashSet::new();

        for (registry_idx, registry) in registries.iter().enumerate() {
            let families = registry.get_prometheus_registry().gather();
            for mut family in families {
                let name = family.name().to_string();

                let entry = by_name.entry(name.clone()).or_insert_with(|| {
                    let mut out = prometheus::proto::MetricFamily::new();
                    out.set_name(name.clone());
                    out.set_help(family.help().to_string());
                    out.set_field_type(family.get_field_type());
                    out
                });

                if entry.help() != family.help()
                    || entry.get_field_type() != family.get_field_type()
                {
                    return Err(anyhow::anyhow!(
                        "Metric family '{}' has inconsistent help/type across registries (idx={})",
                        name,
                        registry_idx
                    ));
                }

                let mut metrics = family.take_metric();
                for metric in metrics.drain(..) {
                    let mut labels: Vec<(String, String)> = metric
                        .get_label()
                        .iter()
                        .map(|lp| (lp.name().to_string(), lp.value().to_string()))
                        .collect();
                    labels.sort_by(|(ka, va), (kb, vb)| (ka, va).cmp(&(kb, vb)));

                    let key = series_dedup_key(&name, &labels);

                    let inserted = seen_series.insert(key);
                    if !inserted {
                        tracing::warn!(
                            metric_name = %name,
                            labels = ?labels,
                            registry_idx,
                            "Duplicate Prometheus series while merging registries; dropping later sample"
                        );
                        continue;
                    }

                    entry.mut_metric().push(metric);
                }
            }
        }

        // BTreeMap 已经按 family 名升序，直接 collect 即可，无需额外 sort。
        let merged: Vec<prometheus::proto::MetricFamily> = by_name.into_values().collect();

        let encoder = prometheus::TextEncoder::new();
        let mut buffer = Vec::new();
        encoder.encode(&merged, &mut buffer)?;
        let mut result = String::from_utf8(buffer)?;

        let mut expfmt = String::new();
        for registry in registries {
            let exposition_text = registry.execute_expfmt_callbacks();
            if !exposition_text.is_empty() {
                if !expfmt.is_empty() && !expfmt.ends_with('\n') {
                    expfmt.push('\n');
                }
                expfmt.push_str(&exposition_text);
            }
        }

        if !expfmt.is_empty() {
            if !result.ends_with('\n') {
                result.push('\n');
            }
            result.push_str(&expfmt);
        }

        Ok(result)
    }

    /// Add a callback function that receives a reference to any MetricsHierarchy
    // 中文说明：注册一个抓取前执行的更新回调，后续 scrape 时会按顺序触发这些回调。
    pub fn add_update_callback(&self, callback: PrometheusUpdateCallback) {
        let mut callbacks = self.prometheus_update_callbacks.write().unwrap();
        callbacks.push(callback);
    }

    /// Add an exposition text callback that returns Prometheus text
    // 中文说明：注册一个额外文本回调，用于在标准指标文本后面追加自定义 exposition 内容。
    pub fn add_expfmt_callback(&self, callback: PrometheusExpositionFormatCallback) {
        let mut callbacks = self.prometheus_expfmt_callbacks.write().unwrap();
        callbacks.push(callback);
    }

    /// Execute all update callbacks and return their results
    // 中文说明：顺序执行所有更新回调，并把每个回调的结果完整收集返回给调用方处理。
    pub fn execute_update_callbacks(&self) -> Vec<anyhow::Result<()>> {
        let callbacks = self.prometheus_update_callbacks.read().unwrap();
        let results = callbacks.iter().map(|callback| callback()).collect();
        results
    }

    /// Execute all exposition text callbacks and return their concatenated text
    // 中文说明：依次执行所有 exposition 文本回调，把非空文本按换行规则拼接成一个最终字符串。
    pub fn execute_expfmt_callbacks(&self) -> String {
        let callbacks = self.prometheus_expfmt_callbacks.read().unwrap();
        let mut output = String::new();
        for callback in callbacks.iter() {
            match callback() {
                Ok(text) => {
                    if text.is_empty() {
                        continue;
                    }

                    if !output.is_empty() && !output.ends_with('\n') {
                        output.push('\n');
                    }
                    output.push_str(&text);
                }
                Err(e) => {
                    tracing::error!("Error executing exposition text callback: {e}");
                }
            }
        }
        output
    }

    /// Add a Prometheus metric collector to this registry
    // 中文说明：把新的 collector 注册到当前 Prometheus registry 中，并把底层注册错误包装成 anyhow 错误返回。
    pub fn add_metric(
        &self,
        collector: Box<dyn prometheus::core::Collector>,
    ) -> anyhow::Result<()> {
        let registry = self.prometheus_registry.write().unwrap();
        registry
            .register(collector)
            .map_err(|e| anyhow::anyhow!("Failed to register metric: {}", e))
    }

    /// Add a Prometheus metric collector, logging a warning on failure instead of returning an error.
    // 中文说明：尝试注册 collector；如果失败则仅记录 warning，不把错误继续向上传播。
    pub fn add_metric_or_warn(&self, collector: Box<dyn prometheus::core::Collector>, name: &str) {
        match self.add_metric(collector) {
            Ok(()) => {}
            Err(error) => {
                tracing::warn!(error = %error, metric = name, "Failed to register metric");
            }
        }
    }

    /// Get a read guard to the Prometheus registry for scraping
    // 中文说明：返回底层 Prometheus registry 的只读锁，供外部直接执行 gather 或其他只读操作。
    pub fn get_prometheus_registry(&self) -> std::sync::RwLockReadGuard<'_, prometheus::Registry> {
        let registry = self.prometheus_registry.read().unwrap();
        registry
    }

    /// Returns true if a metric with the given name already exists in the Prometheus registry
    // 中文说明：抓取当前 registry 中的所有 metric family，并判断是否已经存在指定名称的指标。
    pub fn has_metric_named(&self, metric_name: &str) -> bool {
        let registry = self.prometheus_registry.read().unwrap();
        let families = registry.gather();
        families.iter().any(|family| family.name() == metric_name)
    }
}

impl Default for MetricsRegistry {
    // 中文说明：默认构造逻辑直接复用 new，保证默认值和显式初始化行为完全一致。
    fn default() -> Self {
        let registry = Self::new();
        registry
    }
}

#[cfg(test)]
mod test_helpers {
    use super::prometheus_names::name_prefix;
    use super::*;

    /// Base function to filter Prometheus output lines based on a predicate.
    /// Returns lines that match the predicate, converted to String.
    fn filter_prometheus_lines<F>(input: &str, mut predicate: F) -> Vec<String>
    where
        F: FnMut(&str) -> bool,
    {
        input
            .lines()
            .filter(|line| predicate(line))
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
    }

    /// Extracts all component metrics (excluding help text and type definitions).
    /// Returns only the actual metric lines with values.
    pub fn extract_metrics(input: &str) -> Vec<String> {
        filter_prometheus_lines(input, |line| {
            line.starts_with(&format!("{}_", name_prefix::COMPONENT))
                && !line.starts_with("#")
                && !line.trim().is_empty()
        })
    }

    /// Parses a Prometheus metric line and extracts the name, labels, and value.
    /// Used instead of fetching metrics directly to test end-to-end results, not intermediate state.
    ///
    /// # Example
    /// ```
    /// let line = "http_requests_total{method=\"GET\"} 1234";
    /// let (name, labels, value) = parse_prometheus_metric(line).unwrap();
    /// assert_eq!(name, "http_requests_total");
    /// assert_eq!(labels.get("method"), Some(&"GET".to_string()));
    /// assert_eq!(value, 1234.0);
    /// ```
    pub fn parse_prometheus_metric(
        line: &str,
    ) -> Option<(String, std::collections::HashMap<String, String>, f64)> {
        if line.trim().is_empty() || line.starts_with('#') {
            return None;
        }

        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 2 {
            return None;
        }

        let metric_part = parts[0];
        let value: f64 = parts[1].parse().ok()?;

        let (name, labels) = if metric_part.contains('{') {
            let brace_start = metric_part.find('{').unwrap();
            let brace_end = metric_part.rfind('}').unwrap_or(metric_part.len());
            let name = &metric_part[..brace_start];
            let labels_str = &metric_part[brace_start + 1..brace_end];

            let mut labels = std::collections::HashMap::new();
            for pair in labels_str.split(',') {
                if let Some((k, v)) = pair.split_once('=') {
                    let v = v.trim_matches('"');
                    labels.insert(k.trim().to_string(), v.to_string());
                }
            }
            (name.to_string(), labels)
        } else {
            (metric_part.to_string(), std::collections::HashMap::new())
        };

        Some((name, labels, value))
    }

    /// Injects a `worker_id` label into Prometheus metric data lines.
    /// Prometheus places const labels (like worker_id) before special labels
    /// (like histogram `le`), so for histogram bucket lines we insert before
    /// `,le=`. For all other metric lines, we insert before the closing `}`.
    /// Comment lines and lines without labels are left unchanged.
    pub fn inject_worker_id(expected: &str, wid: &str) -> String {
        let wid_label = format!(",worker_id=\"{}\"", wid);
        expected
            .lines()
            .map(|line| {
                if line.starts_with('#') || line.trim().is_empty() || !line.contains('{') {
                    line.to_string()
                } else if let Some(le_pos) = line.find(",le=") {
                    // Histogram bucket lines: worker_id is a const label, `le` is special,
                    // so worker_id sorts before `le` in Prometheus output.
                    let mut s = line.to_string();
                    s.insert_str(le_pos, &wid_label);
                    s
                } else {
                    line.replacen("}", &format!("{}}}", wid_label), 1)
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

#[cfg(test)]
mod test_metricsregistry_units {
    use super::*;

    #[test]
    fn test_build_component_metric_name_with_prefix() {
        // Test that build_component_metric_name correctly prepends the dynamo_component prefix
        let result = build_component_metric_name("requests");
        assert_eq!(result, "dynamo_component_requests");

        let result = build_component_metric_name("counter");
        assert_eq!(result, "dynamo_component_counter");
    }

    #[test]
    fn test_parse_prometheus_metric() {
        use super::test_helpers::parse_prometheus_metric;
        use std::collections::HashMap;

        // Test parsing a metric with labels
        let line = "http_requests_total{method=\"GET\",status=\"200\"} 1234";
        let parsed = parse_prometheus_metric(line);
        assert!(parsed.is_some());

        let (name, labels, value) = parsed.unwrap();
        assert_eq!(name, "http_requests_total");

        let mut expected_labels = HashMap::new();
        expected_labels.insert("method".to_string(), "GET".to_string());
        expected_labels.insert("status".to_string(), "200".to_string());
        assert_eq!(labels, expected_labels);

        assert_eq!(value, 1234.0);

        // Test parsing a metric without labels
        let line = "cpu_usage 98.5";
        let parsed = parse_prometheus_metric(line);
        assert!(parsed.is_some());

        let (name, labels, value) = parsed.unwrap();
        assert_eq!(name, "cpu_usage");
        assert!(labels.is_empty());
        assert_eq!(value, 98.5);

        // Test parsing a metric with float value
        let line = "response_time{service=\"api\"} 0.123";
        let parsed = parse_prometheus_metric(line);
        assert!(parsed.is_some());

        let (name, labels, value) = parsed.unwrap();
        assert_eq!(name, "response_time");

        let mut expected_labels = HashMap::new();
        expected_labels.insert("service".to_string(), "api".to_string());
        assert_eq!(labels, expected_labels);

        assert_eq!(value, 0.123);

        // Test parsing invalid lines
        assert!(parse_prometheus_metric("").is_none()); // Empty line
        assert!(parse_prometheus_metric("# HELP metric description").is_none()); // Help text
        assert!(parse_prometheus_metric("# TYPE metric counter").is_none()); // Type definition
        assert!(parse_prometheus_metric("metric_name").is_none()); // No value

        println!("✓ Prometheus metric parsing works correctly!");
    }

    #[test]
    fn test_metrics_registry_entry_callbacks() {
        use crate::MetricsRegistry;
        use std::sync::atomic::{AtomicUsize, Ordering};

        // Test 1: Basic callback execution with counter increments
        {
            let registry = MetricsRegistry::new();
            let counter = Arc::new(AtomicUsize::new(0));

            // Add callbacks with different increment values
            for increment in [1, 10, 100] {
                let counter_clone = counter.clone();
                registry.add_update_callback(Arc::new(move || {
                    counter_clone.fetch_add(increment, Ordering::SeqCst);
                    Ok(())
                }));
            }

            // Verify counter starts at 0
            assert_eq!(counter.load(Ordering::SeqCst), 0);

            // First execution
            let results = registry.execute_update_callbacks();
            assert_eq!(results.len(), 3);
            assert!(results.iter().all(|r| r.is_ok()));
            assert_eq!(counter.load(Ordering::SeqCst), 111); // 1 + 10 + 100

            // Second execution - callbacks should be reusable
            let results = registry.execute_update_callbacks();
            assert_eq!(results.len(), 3);
            assert_eq!(counter.load(Ordering::SeqCst), 222); // 111 + 111

            // Test cloning - cloned entry shares callbacks (callbacks are Arc-wrapped)
            let cloned = registry.clone();
            assert_eq!(cloned.execute_update_callbacks().len(), 3);
            assert_eq!(counter.load(Ordering::SeqCst), 333); // 222 + 111

            // Original still has callbacks and shares the same Arc
            registry.execute_update_callbacks();
            assert_eq!(counter.load(Ordering::SeqCst), 444); // 333 + 111
        }

        // Test 2: Mixed success and error callbacks
        {
            let registry = MetricsRegistry::new();
            let counter = Arc::new(AtomicUsize::new(0));

            // Successful callback
            let counter_clone = counter.clone();
            registry.add_update_callback(Arc::new(move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }));

            // Error callback
            registry.add_update_callback(Arc::new(|| Err(anyhow::anyhow!("Simulated error"))));

            // Another successful callback
            let counter_clone = counter.clone();
            registry.add_update_callback(Arc::new(move || {
                counter_clone.fetch_add(10, Ordering::SeqCst);
                Ok(())
            }));

            // Execute and verify mixed results
            let results = registry.execute_update_callbacks();
            assert_eq!(results.len(), 3);
            assert!(results[0].is_ok());
            assert!(results[1].is_err());
            assert!(results[2].is_ok());

            // Verify error message
            assert_eq!(
                results[1].as_ref().unwrap_err().to_string(),
                "Simulated error"
            );

            // Verify successful callbacks still executed
            assert_eq!(counter.load(Ordering::SeqCst), 11); // 1 + 10

            // Execute again - errors should be consistent
            let results = registry.execute_update_callbacks();
            assert!(results[1].is_err());
            assert_eq!(counter.load(Ordering::SeqCst), 22); // 11 + 11
        }

        // Test 3: Empty registry
        {
            let registry = MetricsRegistry::new();
            let results = registry.execute_update_callbacks();
            assert_eq!(results.len(), 0);
        }
    }
}

#[cfg(feature = "integration")]
#[cfg(test)]
mod test_metricsregistry_prefixes {
    use super::*;
    use crate::distributed::distributed_test_utils::create_test_drt_async;
    use prometheus::core::Collector;

    #[tokio::test]
    async fn test_hierarchical_prefixes_and_parent_hierarchies() {
        let drt = create_test_drt_async().await;

        const DRT_NAME: &str = "";
        const NAMESPACE_NAME: &str = "ns901";
        const COMPONENT_NAME: &str = "comp901";
        const ENDPOINT_NAME: &str = "ep901";
        let namespace = drt.namespace(NAMESPACE_NAME).unwrap();
        let component = namespace.component(COMPONENT_NAME).unwrap();
        let endpoint = component.endpoint(ENDPOINT_NAME);

        // DRT
        assert_eq!(drt.basename(), DRT_NAME);
        assert_eq!(drt.parent_hierarchies().len(), 0);
        // DRT hierarchy is just its basename (empty string)

        // Namespace
        assert_eq!(namespace.basename(), NAMESPACE_NAME);
        assert_eq!(namespace.parent_hierarchies().len(), 1);
        assert_eq!(namespace.parent_hierarchies()[0].basename(), DRT_NAME);
        // Namespace hierarchy is just its basename since parent is empty

        // Component
        assert_eq!(component.basename(), COMPONENT_NAME);
        assert_eq!(component.parent_hierarchies().len(), 2);
        assert_eq!(component.parent_hierarchies()[0].basename(), DRT_NAME);
        assert_eq!(component.parent_hierarchies()[1].basename(), NAMESPACE_NAME);
        // Component hierarchy structure is validated by the individual assertions above

        // Endpoint
        assert_eq!(endpoint.basename(), ENDPOINT_NAME);
        assert_eq!(endpoint.parent_hierarchies().len(), 3);
        assert_eq!(endpoint.parent_hierarchies()[0].basename(), DRT_NAME);
        assert_eq!(endpoint.parent_hierarchies()[1].basename(), NAMESPACE_NAME);
        assert_eq!(endpoint.parent_hierarchies()[2].basename(), COMPONENT_NAME);
        // Endpoint hierarchy structure is validated by the individual assertions above

        // Relationships
        assert!(
            namespace
                .parent_hierarchies()
                .iter()
                .any(|h| h.basename() == drt.basename())
        );
        assert!(
            component
                .parent_hierarchies()
                .iter()
                .any(|h| h.basename() == namespace.basename())
        );
        assert!(
            endpoint
                .parent_hierarchies()
                .iter()
                .any(|h| h.basename() == component.basename())
        );

        // Depth
        assert_eq!(drt.parent_hierarchies().len(), 0);
        assert_eq!(namespace.parent_hierarchies().len(), 1);
        assert_eq!(component.parent_hierarchies().len(), 2);
        assert_eq!(endpoint.parent_hierarchies().len(), 3);

        // Invalid namespace behavior - sanitizes to "_123" and succeeds
        // @ryanolson intended to enable validation (see TODO comment in component.rs) but didn't turn it on,
        // so invalid characters are sanitized in MetricsRegistry rather than rejected.
        let invalid_namespace = drt.namespace("@@123").unwrap();
        let result =
            invalid_namespace
                .metrics()
                .create_counter("test_counter", "A test counter", &[]);
        assert!(result.is_ok());
        if let Ok(counter) = &result {
            // Verify the namespace was sanitized to "_123" in the label
            let desc = counter.desc();
            let namespace_label = desc[0]
                .const_label_pairs
                .iter()
                .find(|l| l.name() == "dynamo_namespace")
                .expect("Should have dynamo_namespace label");
            assert_eq!(namespace_label.value(), "_123");
        }

        // Valid namespace works
        let valid_namespace = drt.namespace("ns567").unwrap();
        assert!(
            valid_namespace
                .metrics()
                .create_counter("test_counter", "A test counter", &[])
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_expfmt_callback_only_registered_on_endpoint_is_included_once() {
        // Sanity test: if an expfmt callback is registered only on the endpoint registry,
        // scraping from the root (DRT) should still include it exactly once via the
        // child-registry traversal.
        let drt = create_test_drt_async().await;
        let namespace = drt.namespace("ns_expfmt_ep_only").unwrap();
        let component = namespace.component("comp_expfmt_ep_only").unwrap();
        let endpoint = component.endpoint("ep_expfmt_ep_only");

        let metric_line = "dynamo_component_active_decode_blocks{dp_rank=\"0\"} 0\n";
        let callback: PrometheusExpositionFormatCallback =
            Arc::new(move || Ok(metric_line.to_string()));

        endpoint
            .get_metrics_registry()
            .add_expfmt_callback(callback);

        let output = drt.metrics().prometheus_expfmt().unwrap();
        let occurrences = output
            .lines()
            .filter(|line| line == &metric_line.trim_end_matches('\n'))
            .count();

        assert_eq!(
            occurrences, 1,
            "endpoint-registered exposition callback should appear once, got {} occurrences\n\n{}",
            occurrences, output
        );
    }

    #[tokio::test]
    async fn test_recursive_namespace() {
        // Create a distributed runtime for testing
        let drt = create_test_drt_async().await;

        // Create a deeply chained namespace: ns1.ns2.ns3
        let ns1 = drt.namespace("ns1").unwrap();
        let ns2 = ns1.namespace("ns2").unwrap();
        let ns3 = ns2.namespace("ns3").unwrap();

        // Create a component in the deepest namespace
        let component = ns3.component("test-component").unwrap();

        // Verify the hierarchy structure
        assert_eq!(ns1.basename(), "ns1");
        assert_eq!(ns1.parent_hierarchies().len(), 1);
        assert_eq!(ns1.parent_hierarchies()[0].basename(), "");
        // ns1 hierarchy is just its basename since parent is empty

        assert_eq!(ns2.basename(), "ns2");
        assert_eq!(ns2.parent_hierarchies().len(), 2);
        assert_eq!(ns2.parent_hierarchies()[0].basename(), "");
        assert_eq!(ns2.parent_hierarchies()[1].basename(), "ns1");
        // ns2 hierarchy structure validated by parent assertions above

        assert_eq!(ns3.basename(), "ns3");
        assert_eq!(ns3.parent_hierarchies().len(), 3);
        assert_eq!(ns3.parent_hierarchies()[0].basename(), "");
        assert_eq!(ns3.parent_hierarchies()[1].basename(), "ns1");
        assert_eq!(ns3.parent_hierarchies()[2].basename(), "ns2");
        // ns3 hierarchy structure validated by parent assertions above

        assert_eq!(component.basename(), "test-component");
        assert_eq!(component.parent_hierarchies().len(), 4);
        assert_eq!(component.parent_hierarchies()[0].basename(), "");
        assert_eq!(component.parent_hierarchies()[1].basename(), "ns1");
        assert_eq!(component.parent_hierarchies()[2].basename(), "ns2");
        assert_eq!(component.parent_hierarchies()[3].basename(), "ns3");
        // component hierarchy structure validated by parent assertions above

        println!("✓ Chained namespace test passed - all prefixes correct");
    }
}

#[cfg(feature = "integration")]
#[cfg(test)]
mod test_metricsregistry_prometheus_fmt_outputs {
    use super::prometheus_names::name_prefix;
    use super::*;
    use crate::distributed::distributed_test_utils::create_test_drt_async;
    use prometheus::Counter;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_prometheusfactory_using_metrics_registry_trait() {
        // Setup real DRT and registry using the test-friendly constructor
        let drt = create_test_drt_async().await;

        // Use a simple constant namespace name
        let namespace_name = "ns345";

        let namespace = drt.namespace(namespace_name).unwrap();
        let component = namespace.component("comp345").unwrap();
        let endpoint = component.endpoint("ep345");

        // Test Counter creation
        let counter = endpoint
            .metrics()
            .create_counter("testcounter", "A test counter", &[])
            .unwrap();
        counter.inc_by(123.456789);
        let epsilon = 0.01;
        assert!((counter.get() - 123.456789).abs() < epsilon);

        let endpoint_output_raw = endpoint.metrics().prometheus_expfmt().unwrap();
        println!("Endpoint output:");
        println!("{}", endpoint_output_raw);

        // worker_id is runtime-generated (etcd lease ID), so we grab it from the DRT
        // and inject it into expected strings via the inject_worker_id helper.
        let wid = format!("{:x}", drt.connection_id());
        use super::test_helpers::inject_worker_id;

        let expected_endpoint_output = inject_worker_id(
            r#"# HELP dynamo_component_testcounter A test counter
# TYPE dynamo_component_testcounter counter
dynamo_component_testcounter{dynamo_component="comp345",dynamo_endpoint="ep345",dynamo_namespace="ns345"} 123.456789"#,
            &wid,
        );

        assert_eq!(
            endpoint_output_raw.trim_end_matches('\n'),
            expected_endpoint_output.trim_end_matches('\n'),
            "\n=== ENDPOINT COMPARISON FAILED ===\n\
             Actual:\n{}\n\
             Expected:\n{}\n\
             ==============================",
            endpoint_output_raw,
            expected_endpoint_output
        );

        // Test Gauge creation
        let gauge = component
            .metrics()
            .create_gauge("testgauge", "A test gauge", &[])
            .unwrap();
        gauge.set(50000.0);
        assert_eq!(gauge.get(), 50000.0);

        // Test Prometheus format output for Component (gauge + histogram)
        let component_output_raw = component.metrics().prometheus_expfmt().unwrap();
        println!("Component output:");
        println!("{}", component_output_raw);

        let expected_component_output = inject_worker_id(
            r#"# HELP dynamo_component_testcounter A test counter
# TYPE dynamo_component_testcounter counter
dynamo_component_testcounter{dynamo_component="comp345",dynamo_endpoint="ep345",dynamo_namespace="ns345"} 123.456789
# HELP dynamo_component_testgauge A test gauge
# TYPE dynamo_component_testgauge gauge
dynamo_component_testgauge{dynamo_component="comp345",dynamo_namespace="ns345"} 50000"#,
            &wid,
        );

        assert_eq!(
            component_output_raw.trim_end_matches('\n'),
            expected_component_output.trim_end_matches('\n'),
            "\n=== COMPONENT COMPARISON FAILED ===\n\
             Actual:\n{}\n\
             Expected:\n{}\n\
             ==============================",
            component_output_raw,
            expected_component_output
        );

        let intcounter = namespace
            .metrics()
            .create_intcounter("testintcounter", "A test int counter", &[])
            .unwrap();
        intcounter.inc_by(12345);
        assert_eq!(intcounter.get(), 12345);

        // Test Prometheus format output for Namespace (int_counter + gauge + histogram)
        let namespace_output_raw = namespace.metrics().prometheus_expfmt().unwrap();
        println!("Namespace output:");
        println!("{}", namespace_output_raw);

        let expected_namespace_output = inject_worker_id(
            r#"# HELP dynamo_component_testcounter A test counter
# TYPE dynamo_component_testcounter counter
dynamo_component_testcounter{dynamo_component="comp345",dynamo_endpoint="ep345",dynamo_namespace="ns345"} 123.456789
# HELP dynamo_component_testgauge A test gauge
# TYPE dynamo_component_testgauge gauge
dynamo_component_testgauge{dynamo_component="comp345",dynamo_namespace="ns345"} 50000
# HELP dynamo_component_testintcounter A test int counter
# TYPE dynamo_component_testintcounter counter
dynamo_component_testintcounter{dynamo_namespace="ns345"} 12345"#,
            &wid,
        );

        assert_eq!(
            namespace_output_raw.trim_end_matches('\n'),
            expected_namespace_output.trim_end_matches('\n'),
            "\n=== NAMESPACE COMPARISON FAILED ===\n\
             Actual:\n{}\n\
             Expected:\n{}\n\
             ==============================",
            namespace_output_raw,
            expected_namespace_output
        );

        // Test IntGauge creation
        let intgauge = namespace
            .metrics()
            .create_intgauge("testintgauge", "A test int gauge", &[])
            .unwrap();
        intgauge.set(42);
        assert_eq!(intgauge.get(), 42);

        // Test IntGaugeVec creation
        let intgaugevec = namespace
            .metrics()
            .create_intgaugevec(
                "testintgaugevec",
                "A test int gauge vector",
                &["instance", "status"],
                &[("service", "api")],
            )
            .unwrap();
        intgaugevec
            .with_label_values(&["server1", "active"])
            .set(10);
        intgaugevec
            .with_label_values(&["server2", "inactive"])
            .set(0);

        // Test CounterVec creation
        let countervec = endpoint
            .metrics()
            .create_countervec(
                "testcountervec",
                "A test counter vector",
                &["method", "status"],
                &[("service", "api")],
            )
            .unwrap();
        countervec.with_label_values(&["GET", "200"]).inc_by(10.0);
        countervec.with_label_values(&["POST", "201"]).inc_by(5.0);

        // Test Histogram creation
        let histogram = component
            .metrics()
            .create_histogram("testhistogram", "A test histogram", &[], None)
            .unwrap();
        histogram.observe(1.0);
        histogram.observe(2.5);
        histogram.observe(4.0);

        // Test Prometheus format output for DRT (all metrics combined)
        let drt_output_raw = drt.metrics().prometheus_expfmt().unwrap();
        println!("DRT output:");
        println!("{}", drt_output_raw);

        // The uptime_seconds value is dynamic (depends on elapsed wall-clock time),
        // so we check all other lines exactly and validate uptime separately.
        let expected_drt_output_without_uptime = inject_worker_id(
            r#"# HELP dynamo_component_testcounter A test counter
# TYPE dynamo_component_testcounter counter
dynamo_component_testcounter{dynamo_component="comp345",dynamo_endpoint="ep345",dynamo_namespace="ns345"} 123.456789
# HELP dynamo_component_testcountervec A test counter vector
# TYPE dynamo_component_testcountervec counter
dynamo_component_testcountervec{dynamo_component="comp345",dynamo_endpoint="ep345",dynamo_namespace="ns345",method="GET",service="api",status="200"} 10
dynamo_component_testcountervec{dynamo_component="comp345",dynamo_endpoint="ep345",dynamo_namespace="ns345",method="POST",service="api",status="201"} 5
# HELP dynamo_component_testgauge A test gauge
# TYPE dynamo_component_testgauge gauge
dynamo_component_testgauge{dynamo_component="comp345",dynamo_namespace="ns345"} 50000
# HELP dynamo_component_testhistogram A test histogram
# TYPE dynamo_component_testhistogram histogram
dynamo_component_testhistogram_bucket{dynamo_component="comp345",dynamo_namespace="ns345",le="0.005"} 0
dynamo_component_testhistogram_bucket{dynamo_component="comp345",dynamo_namespace="ns345",le="0.01"} 0
dynamo_component_testhistogram_bucket{dynamo_component="comp345",dynamo_namespace="ns345",le="0.025"} 0
dynamo_component_testhistogram_bucket{dynamo_component="comp345",dynamo_namespace="ns345",le="0.05"} 0
dynamo_component_testhistogram_bucket{dynamo_component="comp345",dynamo_namespace="ns345",le="0.1"} 0
dynamo_component_testhistogram_bucket{dynamo_component="comp345",dynamo_namespace="ns345",le="0.25"} 0
dynamo_component_testhistogram_bucket{dynamo_component="comp345",dynamo_namespace="ns345",le="0.5"} 0
dynamo_component_testhistogram_bucket{dynamo_component="comp345",dynamo_namespace="ns345",le="1"} 1
dynamo_component_testhistogram_bucket{dynamo_component="comp345",dynamo_namespace="ns345",le="2.5"} 2
dynamo_component_testhistogram_bucket{dynamo_component="comp345",dynamo_namespace="ns345",le="5"} 3
dynamo_component_testhistogram_bucket{dynamo_component="comp345",dynamo_namespace="ns345",le="10"} 3
dynamo_component_testhistogram_bucket{dynamo_component="comp345",dynamo_namespace="ns345",le="+Inf"} 3
dynamo_component_testhistogram_sum{dynamo_component="comp345",dynamo_namespace="ns345"} 7.5
dynamo_component_testhistogram_count{dynamo_component="comp345",dynamo_namespace="ns345"} 3
# HELP dynamo_component_testintcounter A test int counter
# TYPE dynamo_component_testintcounter counter
dynamo_component_testintcounter{dynamo_namespace="ns345"} 12345
# HELP dynamo_component_testintgauge A test int gauge
# TYPE dynamo_component_testintgauge gauge
dynamo_component_testintgauge{dynamo_namespace="ns345"} 42
# HELP dynamo_component_testintgaugevec A test int gauge vector
# TYPE dynamo_component_testintgaugevec gauge
dynamo_component_testintgaugevec{dynamo_namespace="ns345",instance="server1",service="api",status="active"} 10
dynamo_component_testintgaugevec{dynamo_namespace="ns345",instance="server2",service="api",status="inactive"} 0"#,
            &wid,
        );

        // Split actual output into non-uptime lines and validate the uptime value line.
        // The uptime metric now carries a worker_id label, so we match on the metric name
        // prefix and extract the value as the last whitespace-delimited token.
        let mut non_uptime_lines = Vec::new();
        let mut saw_uptime_value = false;
        for line in drt_output_raw.trim_end_matches('\n').lines() {
            if line.starts_with("dynamo_component_uptime_seconds") && !line.starts_with('#') {
                let val_str = line.split_whitespace().last().unwrap();
                val_str.parse::<f64>().expect("uptime should be a float");
                saw_uptime_value = true;
            } else if line.starts_with("# HELP dynamo_component_uptime_seconds")
                || line.starts_with("# TYPE dynamo_component_uptime_seconds")
            {
                // Skip HELP/TYPE lines for uptime (we just verify it exists via the value)
            } else {
                non_uptime_lines.push(line);
            }
        }
        assert!(
            saw_uptime_value,
            "uptime_seconds metric should be present in initial scrape"
        );

        let actual_without_uptime = non_uptime_lines.join("\n");
        assert_eq!(
            actual_without_uptime,
            expected_drt_output_without_uptime.trim_end_matches('\n'),
            "\n=== DRT COMPARISON FAILED (excluding uptime) ===\n\
             Expected:\n{}\n\
             Actual:\n{}\n\
             ==============================",
            expected_drt_output_without_uptime,
            actual_without_uptime
        );

        // Wait briefly so the uptime gauge is clearly positive on the next scrape.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let drt_output_after = drt.metrics().prometheus_expfmt().unwrap();
        let uptime_line = drt_output_after
            .lines()
            .find(|l| l.starts_with("dynamo_component_uptime_seconds") && !l.starts_with('#'))
            .expect("uptime_seconds metric should be present after sleep");
        let uptime_after: f64 = uptime_line
            .split_whitespace()
            .last()
            .unwrap()
            .parse()
            .expect("uptime should be a float");
        assert!(
            uptime_after > 0.0,
            "uptime_seconds should be > 0 after 10ms sleep, got {}",
            uptime_after
        );

        println!("✓ All Prometheus format outputs verified successfully!");
    }

    #[test]
    fn test_refactored_filter_functions() {
        // Test data with component metrics
        let test_input = r#"# HELP dynamo_component_requests Total requests
# TYPE dynamo_component_requests counter
dynamo_component_requests 42
# HELP dynamo_component_latency Response latency
# TYPE dynamo_component_latency histogram
dynamo_component_latency_bucket{le="0.1"} 10
dynamo_component_latency_bucket{le="0.5"} 25
dynamo_component_errors_total 5"#;

        // Test extract_metrics (only actual metric lines, excluding help/type)
        let metrics_only = super::test_helpers::extract_metrics(test_input);
        assert_eq!(metrics_only.len(), 4); // 4 actual metric lines (excluding help/type)
        assert!(
            metrics_only
                .iter()
                .all(|line| line.starts_with("dynamo_component") && !line.starts_with("#"))
        );

        println!("✓ All refactored filter functions work correctly!");
    }

    #[tokio::test]
    async fn test_same_metric_name_different_endpoints() {
        // Test that the same metric name can exist in different endpoints without collision.
        // This validates the multi-registry approach: each endpoint has its own registry,
        // and metrics are merged at scrape time with distinct labels.
        let drt = create_test_drt_async().await;
        let namespace = drt.namespace("ns_test").unwrap();
        let component = namespace.component("comp_test").unwrap();

        // Create two endpoints with the same metric name
        let ep1 = component.endpoint("ep1");
        let ep2 = component.endpoint("ep2");

        let counter1 = ep1
            .metrics()
            .create_counter("requests_total", "Total requests", &[])
            .unwrap();
        counter1.inc_by(100.0);

        let counter2 = ep2
            .metrics()
            .create_counter("requests_total", "Total requests", &[])
            .unwrap();
        counter2.inc_by(200.0);

        // Get merged Prometheus output from component level
        let output = component.metrics().prometheus_expfmt().unwrap();

        let wid = format!("{:x}", drt.connection_id());
        use super::test_helpers::inject_worker_id;

        let expected_output = inject_worker_id(
            r#"# HELP dynamo_component_requests_total Total requests
# TYPE dynamo_component_requests_total counter
dynamo_component_requests_total{dynamo_component="comp_test",dynamo_endpoint="ep1",dynamo_namespace="ns_test"} 100
dynamo_component_requests_total{dynamo_component="comp_test",dynamo_endpoint="ep2",dynamo_namespace="ns_test"} 200"#,
            &wid,
        );

        assert_eq!(
            output.trim_end_matches('\n'),
            expected_output.trim_end_matches('\n'),
            "\n=== MULTI-REGISTRY COMPARISON FAILED ===\n\
             Actual:\n{}\n\
             Expected:\n{}\n\
             ==============================",
            output,
            expected_output
        );

        println!("✓ Multi-registry prevents Prometheus collisions!");
    }

    #[tokio::test]
    async fn test_duplicate_series_warning() {
        // Test that duplicate series (same metric name + same labels) are detected and deduplicated.
        // This should log a warning and keep only one of the duplicate series.
        let drt = create_test_drt_async().await;
        let namespace = drt.namespace("ns_dup").unwrap();
        let component = namespace.component("comp_dup").unwrap();

        // Create two endpoints with counters that will have identical labels when scraped
        let ep1 = component.endpoint("ep_same");
        let ep2 = component.endpoint("ep_same"); // Same endpoint name = duplicate labels

        let counter1 = ep1
            .metrics()
            .create_counter("dup_metric", "Duplicate metric test", &[])
            .unwrap();
        counter1.inc_by(50.0);

        let counter2 = ep2
            .metrics()
            .create_counter("dup_metric", "Duplicate metric test", &[])
            .unwrap();
        counter2.inc_by(75.0);

        // Get merged output - duplicates should be deduplicated
        let output = component.metrics().prometheus_expfmt().unwrap();

        let wid = format!("{:x}", drt.connection_id());
        use super::test_helpers::inject_worker_id;

        let expected_output = inject_worker_id(
            r#"# HELP dynamo_component_dup_metric Duplicate metric test
# TYPE dynamo_component_dup_metric counter
dynamo_component_dup_metric{dynamo_component="comp_dup",dynamo_endpoint="ep_same",dynamo_namespace="ns_dup"} 50"#,
            &wid,
        );

        assert_eq!(
            output.trim_end_matches('\n'),
            expected_output.trim_end_matches('\n'),
            "\n=== DEDUPLICATION COMPARISON FAILED ===\n\
             Actual:\n{}\n\
             Expected:\n{}\n\
             ==============================",
            output,
            expected_output
        );

        println!("✓ Duplicate series detection and deduplication works!");
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use super::test_helpers::{extract_metrics, inject_worker_id, parse_prometheus_metric};
    use prometheus::core::Collector;
    use std::panic;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct StaticHierarchy {
        name: &'static str,
        registry: MetricsRegistry,
        parents: Vec<&'static dyn MetricsHierarchy>,
        connection_id: Option<u64>,
    }

    impl StaticHierarchy {
        fn new(
            name: &'static str,
            parents: Vec<&'static dyn MetricsHierarchy>,
            connection_id: Option<u64>,
        ) -> Self {
            Self {
                name,
                registry: MetricsRegistry::new(),
                parents,
                connection_id,
            }
        }
    }

    impl MetricsHierarchy for StaticHierarchy {
        fn basename(&self) -> String {
            self.name.to_string()
        }

        fn parent_hierarchies(&self) -> Vec<&dyn MetricsHierarchy> {
            self.parents.clone()
        }

        fn get_metrics_registry(&self) -> &MetricsRegistry {
            &self.registry
        }

        fn connection_id(&self) -> Option<u64> {
            self.connection_id
        }
    }

    #[derive(Debug)]
    struct DefaultConnectionHierarchy {
        registry: MetricsRegistry,
    }

    impl DefaultConnectionHierarchy {
        fn new() -> Self {
            Self {
                registry: MetricsRegistry::new(),
            }
        }
    }

    impl MetricsHierarchy for DefaultConnectionHierarchy {
        fn basename(&self) -> String {
            "default".to_string()
        }

        fn parent_hierarchies(&self) -> Vec<&dyn MetricsHierarchy> {
            vec![]
        }

        fn get_metrics_registry(&self) -> &MetricsRegistry {
            &self.registry
        }
    }

    fn build_hierarchy_tree() -> (
        &'static StaticHierarchy,
        &'static StaticHierarchy,
        &'static StaticHierarchy,
        &'static StaticHierarchy,
    ) {
        let root = Box::leak(Box::new(StaticHierarchy::new("", vec![], Some(0xfeedbeef))));
        let namespace = Box::leak(Box::new(StaticHierarchy::new(
            "ns",
            vec![root as &dyn MetricsHierarchy],
            Some(0xfeedbeef),
        )));
        let component = Box::leak(Box::new(StaticHierarchy::new(
            "comp",
            vec![root as &dyn MetricsHierarchy, namespace as &dyn MetricsHierarchy],
            Some(0xfeedbeef),
        )));
        let endpoint = Box::leak(Box::new(StaticHierarchy::new(
            "ep",
            vec![
                root as &dyn MetricsHierarchy,
                namespace as &dyn MetricsHierarchy,
                component as &dyn MetricsHierarchy,
            ],
            Some(0xfeedbeef),
        )));

        root.registry.add_child_registry(&namespace.registry);
        namespace.registry.add_child_registry(&component.registry);
        component.registry.add_child_registry(&endpoint.registry);

        (root, namespace, component, endpoint)
    }

    #[test]
    fn test_supplemental_validate_labels_and_prometheus_metric_impl_edges() {
        assert!(validate_no_duplicate_label_keys(&[("a", "1"), ("b", "2")]).is_ok());

        let duplicate_err = validate_no_duplicate_label_keys(&[("a", "1"), ("a", "2")])
            .unwrap_err()
            .to_string();
        assert!(duplicate_err.contains("Duplicate label key 'a'"));

        let counter = <prometheus::Counter as PrometheusMetric>::with_opts(
            prometheus::Opts::new("supplemental_counter_metric_impl", "counter help"),
        )
        .unwrap();
        counter.inc();
        assert_eq!(counter.get(), 1.0);

        let int_counter = <prometheus::IntCounter as PrometheusMetric>::with_opts(
            prometheus::Opts::new("supplemental_intcounter_metric_impl", "int counter help"),
        )
        .unwrap();
        int_counter.inc();
        assert_eq!(int_counter.get(), 1);

        let gauge = <prometheus::Gauge as PrometheusMetric>::with_opts(prometheus::Opts::new(
            "supplemental_gauge_metric_impl",
            "gauge help",
        ))
        .unwrap();
        gauge.set(3.5);
        assert_eq!(gauge.get(), 3.5);

        let int_gauge = <prometheus::IntGauge as PrometheusMetric>::with_opts(
            prometheus::Opts::new("supplemental_intgauge_metric_impl", "int gauge help"),
        )
        .unwrap();
        int_gauge.set(7);
        assert_eq!(int_gauge.get(), 7);

        let histogram = <prometheus::Histogram as PrometheusMetric>::with_histogram_opts_and_buckets(
            prometheus::HistogramOpts::new(
                "supplemental_histogram_metric_impl",
                "histogram help",
            ),
            Some(vec![0.5, 1.0, 2.0]),
        )
        .unwrap();
        histogram.observe(0.75);
        assert_eq!(histogram.get_sample_count(), 1);

        let counter_vec = <prometheus::CounterVec as PrometheusMetric>::with_opts_and_label_names(
            prometheus::Opts::new("supplemental_countervec_metric_impl", "countervec help"),
            &["status"],
        )
        .unwrap();
        counter_vec.with_label_values(&["ok"]).inc_by(2.0);

        let gauge_vec = <prometheus::GaugeVec as PrometheusMetric>::with_opts_and_label_names(
            prometheus::Opts::new("supplemental_gaugevec_metric_impl", "gaugevec help"),
            &["status"],
        )
        .unwrap();
        gauge_vec.with_label_values(&["ready"]).set(4.0);

        let int_counter_vec =
            <prometheus::IntCounterVec as PrometheusMetric>::with_opts_and_label_names(
                prometheus::Opts::new(
                    "supplemental_intcountervec_metric_impl",
                    "intcountervec help",
                ),
                &["status"],
            )
            .unwrap();
        int_counter_vec.with_label_values(&["done"]).inc_by(3);

        let int_gauge_vec =
            <prometheus::IntGaugeVec as PrometheusMetric>::with_opts_and_label_names(
                prometheus::Opts::new(
                    "supplemental_intgaugevec_metric_impl",
                    "intgaugevec help",
                ),
                &["status"],
            )
            .unwrap();
        int_gauge_vec.with_label_values(&["busy"]).set(5);

        assert!(panic::catch_unwind(|| {
            <prometheus::Counter as PrometheusMetric>::with_histogram_opts_and_buckets(
                prometheus::HistogramOpts::new("supplemental_default_histogram_panic", "panic"),
                None,
            )
        })
        .is_err());

        assert!(panic::catch_unwind(|| {
            <prometheus::CounterVec as PrometheusMetric>::with_opts(prometheus::Opts::new(
                "supplemental_countervec_with_opts_panic",
                "panic",
            ))
        })
        .is_err());

        assert!(<prometheus::GaugeVec as PrometheusMetric>::with_opts(prometheus::Opts::new(
            "supplemental_gaugevec_with_opts_error",
            "error",
        ))
        .unwrap_err()
        .to_string()
        .contains("GaugeVec requires label names"));

        assert!(<prometheus::IntCounterVec as PrometheusMetric>::with_opts(prometheus::Opts::new(
            "supplemental_intcountervec_with_opts_error",
            "error",
        ))
        .unwrap_err()
        .to_string()
        .contains("IntCounterVec requires label names"));

        assert!(<prometheus::IntGaugeVec as PrometheusMetric>::with_opts(prometheus::Opts::new(
            "supplemental_intgaugevec_with_opts_error",
            "error",
        ))
        .unwrap_err()
        .to_string()
        .contains("IntGaugeVec requires label names"));
    }

    #[test]
    fn test_supplemental_create_metric_validation_and_auto_labels() {
        let (_root, _namespace, component, endpoint) = build_hierarchy_tree();

        let metric = create_metric::<prometheus::Counter, _>(
            endpoint,
            "requests_total",
            "Request count",
            &[("custom", "value")],
            None,
            None,
        )
        .unwrap();
        metric.inc_by(2.0);

        let output = endpoint.metrics().prometheus_expfmt().unwrap();
        let line = output
            .lines()
            .find(|line| line.starts_with("dynamo_component_requests_total") && !line.starts_with('#'))
            .unwrap();
        let (name, labels, value) = parse_prometheus_metric(line).unwrap();

        assert_eq!(name, "dynamo_component_requests_total");
        assert_eq!(value, 2.0);
        assert_eq!(labels.get(labels::NAMESPACE), Some(&"ns".to_string()));
        assert_eq!(labels.get(labels::COMPONENT), Some(&"comp".to_string()));
        assert_eq!(labels.get(labels::ENDPOINT), Some(&"ep".to_string()));
        assert_eq!(labels.get(labels::WORKER_ID), Some(&"feedbeef".to_string()));
        assert_eq!(labels.get("custom"), Some(&"value".to_string()));

        let duplicate_labels = create_metric::<prometheus::Counter, _>(
            endpoint,
            "duplicate_labels",
            "Duplicate labels",
            &[("dup", "1"), ("dup", "2")],
            None,
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(duplicate_labels.contains("Duplicate label key 'dup'"));

        let auto_label_conflict = create_metric::<prometheus::Counter, _>(
            endpoint,
            "auto_label_conflict",
            "Auto label conflict",
            &[(labels::NAMESPACE, "manual")],
            None,
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(auto_label_conflict.contains("automatically added"));

        let const_label_conflict = create_metric::<prometheus::CounterVec, _>(
            endpoint,
            "const_label_conflict",
            "Const label conflict",
            &[],
            None,
            Some(&[labels::WORKER_ID]),
        )
        .unwrap_err()
        .to_string();
        assert!(const_label_conflict.contains("conflicts with auto-injected const label"));

        let counter_vec_missing_labels = create_metric::<prometheus::CounterVec, _>(
            endpoint,
            "counter_vec_missing_labels",
            "CounterVec missing labels",
            &[],
            None,
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(counter_vec_missing_labels.contains("CounterVec requires const_labels"));

        let counter_vec_with_buckets = create_metric::<prometheus::CounterVec, _>(
            endpoint,
            "counter_vec_with_buckets",
            "CounterVec buckets",
            &[],
            Some(vec![1.0]),
            Some(&["status"]),
        )
        .unwrap_err()
        .to_string();
        assert!(counter_vec_with_buckets.contains("buckets parameter is not valid for CounterVec"));

        let gauge_vec_missing_labels = create_metric::<prometheus::GaugeVec, _>(
            endpoint,
            "gauge_vec_missing_labels",
            "GaugeVec missing labels",
            &[],
            None,
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(gauge_vec_missing_labels.contains("GaugeVec requires const_labels"));

        let gauge_vec_with_buckets = create_metric::<prometheus::GaugeVec, _>(
            endpoint,
            "gauge_vec_with_buckets",
            "GaugeVec buckets",
            &[],
            Some(vec![1.0]),
            Some(&["status"]),
        )
        .unwrap_err()
        .to_string();
        assert!(gauge_vec_with_buckets.contains("buckets parameter is not valid for GaugeVec"));

        let int_counter_vec_missing_labels = create_metric::<prometheus::IntCounterVec, _>(
            endpoint,
            "int_counter_vec_missing_labels",
            "IntCounterVec missing labels",
            &[],
            None,
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(int_counter_vec_missing_labels.contains("IntCounterVec requires const_labels"));

        let int_counter_vec_with_buckets = create_metric::<prometheus::IntCounterVec, _>(
            endpoint,
            "int_counter_vec_with_buckets",
            "IntCounterVec buckets",
            &[],
            Some(vec![1.0]),
            Some(&["status"]),
        )
        .unwrap_err()
        .to_string();
        assert!(int_counter_vec_with_buckets.contains("buckets parameter is not valid for IntCounterVec"));

        let int_gauge_vec_missing_labels = create_metric::<prometheus::IntGaugeVec, _>(
            endpoint,
            "int_gauge_vec_missing_labels",
            "IntGaugeVec missing labels",
            &[],
            None,
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(int_gauge_vec_missing_labels.contains("IntGaugeVec requires const_labels"));

        let int_gauge_vec_with_buckets = create_metric::<prometheus::IntGaugeVec, _>(
            endpoint,
            "int_gauge_vec_with_buckets",
            "IntGaugeVec buckets",
            &[],
            Some(vec![1.0]),
            Some(&["status"]),
        )
        .unwrap_err()
        .to_string();
        assert!(int_gauge_vec_with_buckets.contains("buckets parameter is not valid for IntGaugeVec"));

        let histogram_const_labels = create_metric::<prometheus::Histogram, _>(
            component,
            "histogram_const_labels",
            "Histogram const labels",
            &[],
            Some(vec![0.5, 1.0]),
            Some(&["status"]),
        )
        .unwrap_err()
        .to_string();
        assert!(histogram_const_labels.contains("const_labels parameter is not valid for Histogram"));

        let standard_metric_buckets = create_metric::<prometheus::Counter, _>(
            endpoint,
            "counter_with_buckets",
            "Counter buckets",
            &[],
            Some(vec![1.0]),
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(standard_metric_buckets.contains("buckets parameter is not valid for Counter, IntCounter, Gauge, or IntGauge"));

        let standard_metric_const_labels = create_metric::<prometheus::Gauge, _>(
            endpoint,
            "gauge_with_const_labels",
            "Gauge const labels",
            &[],
            None,
            Some(&["status"]),
        )
        .unwrap_err()
        .to_string();
        assert!(standard_metric_const_labels.contains("const_labels parameter is not valid for Counter, IntCounter, Gauge, or IntGauge"));
    }

    #[test]
    fn test_supplemental_metrics_wrapper_and_hierarchy_defaults() {
        let hierarchy = DefaultConnectionHierarchy::new();
        assert_eq!(MetricsHierarchy::connection_id(&hierarchy), None);

        let borrowed = &hierarchy;
        assert_eq!(borrowed.basename(), "default".to_string());
        assert!(borrowed.parent_hierarchies().is_empty());
        assert_eq!(borrowed.connection_id(), None);

        let metrics = Metrics::new(borrowed);
        let gauge_vec = metrics
            .create_gaugevec(
                "queue_depth",
                "Queue depth",
                &["state"],
                &[("service", "api")],
            )
            .unwrap();
        gauge_vec.with_label_values(&["ready"]).set(4.5);

        let int_counter_vec = hierarchy
            .metrics()
            .create_intcountervec(
                "jobs_total",
                "Jobs processed",
                &["status"],
                &[("service", "api")],
            )
            .unwrap();
        int_counter_vec.with_label_values(&["done"]).inc_by(6);

        let int_gauge = hierarchy
            .metrics()
            .create_intgauge("workers", "Worker count", &[])
            .unwrap();
        int_gauge.set(3);

        let output = hierarchy.metrics().prometheus_expfmt().unwrap();
        assert!(output.contains("dynamo_component_queue_depth"));
        assert!(output.contains("service=\"api\""));
        assert!(output.contains("state=\"ready\""));
        assert!(output.contains("dynamo_component_jobs_total"));
        assert!(output.contains("status=\"done\""));
        assert!(output.contains("dynamo_component_workers"));
        assert!(!output.contains(labels::WORKER_ID));
    }

    #[test]
    fn test_supplemental_metrics_registry_helpers_and_combined_output() {
        let root = MetricsRegistry::new();
        let child = MetricsRegistry::new();
        let grandchild = MetricsRegistry::default();

        root.add_child_registry(&child);
        root.add_child_registry(&child.clone());
        child.add_child_registry(&grandchild);
        root.add_child_registry(&grandchild);

        let registries = root.registries_for_combined_scrape();
        assert_eq!(registries.len(), 3);

        let update_count = Arc::new(AtomicUsize::new(0));
        let update_count_root = update_count.clone();
        root.add_update_callback(Arc::new(move || {
            update_count_root.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }));
        let update_count_child = update_count.clone();
        child.add_update_callback(Arc::new(move || {
            update_count_child.fetch_add(10, Ordering::SeqCst);
            Ok(())
        }));

        root.add_expfmt_callback(Arc::new(|| Ok("root_extra 1".to_string())));
        root.add_expfmt_callback(Arc::new(|| Ok(String::new())));
        root.add_expfmt_callback(Arc::new(|| Err(anyhow::anyhow!("ignored expfmt error"))));
        child.add_expfmt_callback(Arc::new(|| Ok("child_extra 2\n".to_string())));

        let root_counter = prometheus::Counter::with_opts(prometheus::Opts::new(
            "supplemental_root_counter",
            "Root counter",
        ))
        .unwrap();
        root_counter.inc_by(2.0);
        assert!(!root.has_metric_named("supplemental_root_counter"));
        root.add_metric(Box::new(root_counter.clone())).unwrap();
        assert!(root.has_metric_named("supplemental_root_counter"));

        let child_counter = prometheus::IntCounter::with_opts(prometheus::Opts::new(
            "supplemental_child_counter",
            "Child counter",
        ))
        .unwrap();
        child_counter.inc_by(4);
        child.add_metric(Box::new(child_counter.clone())).unwrap();

        let duplicate_metric = prometheus::Counter::with_opts(prometheus::Opts::new(
            "supplemental_root_counter",
            "Root counter",
        ))
        .unwrap();
        let duplicate_error = root.add_metric(Box::new(duplicate_metric)).unwrap_err().to_string();
        assert!(duplicate_error.contains("Failed to register metric"));

        let duplicate_metric_warn = prometheus::Counter::with_opts(prometheus::Opts::new(
            "supplemental_root_counter",
            "Root counter",
        ))
        .unwrap();
        root.add_metric_or_warn(Box::new(duplicate_metric_warn), "supplemental_root_counter");

        let gathered = root.get_prometheus_registry().gather();
        assert!(gathered.iter().any(|mf| mf.name() == "supplemental_root_counter"));

        let debug_repr = format!("{:?}", root);
        assert!(debug_repr.contains("MetricsRegistry"));
        assert!(debug_repr.contains("1 callbacks") || debug_repr.contains("3 callbacks"));

        let callback_text = root.execute_expfmt_callbacks();
        assert_eq!(callback_text, "root_extra 1");

        let combined = root.prometheus_expfmt_combined().unwrap();
        assert_eq!(update_count.load(Ordering::SeqCst), 11);
        assert!(combined.contains("# HELP supplemental_root_counter Root counter"));
        assert!(combined.contains("supplemental_root_counter 2"));
        assert!(combined.contains("supplemental_child_counter 4"));
        assert!(combined.contains("root_extra 1"));
        assert!(combined.contains("child_extra 2"));
    }

    #[test]
    fn test_supplemental_prometheus_expfmt_combined_rejects_inconsistent_families() {
        let root = MetricsRegistry::new();
        let child = MetricsRegistry::new();
        root.add_child_registry(&child);

        let counter = prometheus::Counter::with_opts(prometheus::Opts::new(
            "supplemental_inconsistent_metric",
            "same name",
        ))
        .unwrap();
        counter.inc();
        root.add_metric(Box::new(counter)).unwrap();

        let gauge = prometheus::Gauge::with_opts(prometheus::Opts::new(
            "supplemental_inconsistent_metric",
            "same name",
        ))
        .unwrap();
        gauge.set(1.0);
        child.add_metric(Box::new(gauge)).unwrap();

        let error = root.prometheus_expfmt_combined().unwrap_err().to_string();
        assert!(error.contains("inconsistent help/type across registries"));
    }

    #[test]
    fn test_supplemental_test_helpers_extract_and_inject_worker_id() {
        let input = r#"# HELP dynamo_component_requests Total requests
# TYPE dynamo_component_requests counter
dynamo_component_requests{service="api"} 42

dynamo_component_latency_bucket{service="api",le="0.5"} 25
plain_metric 1"#;

        let metrics = extract_metrics(input);
        assert_eq!(metrics.len(), 2);
        assert_eq!(metrics[0], "dynamo_component_requests{service=\"api\"} 42");
        assert_eq!(
            metrics[1],
            "dynamo_component_latency_bucket{service=\"api\",le=\"0.5\"} 25"
        );

        let injected = inject_worker_id(input, "abcd1234");
        assert!(injected.contains(
            "dynamo_component_requests{service=\"api\",worker_id=\"abcd1234\"} 42"
        ));
        assert!(injected.contains(
            "dynamo_component_latency_bucket{service=\"api\",worker_id=\"abcd1234\",le=\"0.5\"} 25"
        ));
        assert!(injected.contains("plain_metric 1"));

        let parsed = parse_prometheus_metric(
            "dynamo_component_requests{service=\"api\",worker_id=\"abcd1234\"} 42",
        )
        .unwrap();
        assert_eq!(parsed.0, "dynamo_component_requests");
        assert_eq!(parsed.1.get("worker_id"), Some(&"abcd1234".to_string()));
        assert_eq!(parsed.2, 42.0);
    }
}
