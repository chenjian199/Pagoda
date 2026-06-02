// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # Metrics 注册中心与层级化 Prometheus 接入
//!
//! ## 设计意图
//!
//! 本模块是整个 pagoda runtime 指标体系的“总入口”：它把 Prometheus 原生的多种
//! 指标类型（Counter / Gauge / Histogram / *Vec）藏在统一的
//! [`PrometheusMetric`] trait 与 [`create_metric`] 函数后面，再借助
//! [`MetricsHierarchy`]（DRT → Namespace → ServiceGroup → PortName）实现
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
//!   namespace / servicegroup / portname 三段名字，再加上 `worker_id`（来自
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

use crate::servicegroup::ServiceGroupBuilder;
use anyhow;
use once_cell::sync::Lazy;
use regex::Regex;
use std::any::Any;
use std::collections::HashMap;

// 导入常用项，避免在代码中反复写出冗长的路径前缀
use prometheus_names::{
    build_servicegroup_metric_name, labels, name_prefix, sanitize_prometheus_label,
    sanitize_prometheus_name, work_handler,
};

// 创建 portname 所需的 pipeline 相关导入
use crate::pipeline::{
    AsyncEngine, AsyncEngineContextProvider, Error, ManyOut, ResponseStream, SingleIn, async_trait,
    network::Ingress,
};
use crate::protocols::annotated::Annotated;
use crate::stream;
use crate::stream::StreamExt;

// Prometheus 相关导入
use prometheus::Encoder;

/// 校验标签切片中没有重复的键名。
/// 当所有键名唯一时返回 Ok(())；否则返回错误并指出重复的键名。
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

// === 分节：PrometheusMetric trait ===

/// 给所有 Prometheus 指标类型加上的统一构造门面。`create_metric` 据此泛型分派。
pub trait PrometheusMetric: prometheus::core::Collector + Clone + Send + Sync + 'static {
    /// 根据给定的指标选项创建一个新指标。
    fn with_opts(opts: prometheus::Opts) -> Result<Self, prometheus::Error>
    where
        Self: Sized;

    /// 根据 Histogram 选项与自定义桶创建一个新指标。
    /// 这是一个默认实现，用于非 Histogram 指标时会直接 panic。
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

    /// 根据 Counter 选项与标签名创建一个新指标（用于 CounterVec）。
    /// 这是一个默认实现，用于非 CounterVec 指标时会直接 panic。
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

// 为 Counter、IntCounter、Gauge 等基础类型实现该 trait
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

// 为 Histogram 类型实现该 trait
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

// 为 CounterVec 类型实现该 trait
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
// 2. 它会先检查用户标签是否重复、是否和自动注入标签冲突，再从层级信息中补齐 namespace、servicegroup、portname、worker_id 等常量标签。
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

    let metric_name = build_servicegroup_metric_name(metric_name);

    let reserved_label_names = [
        labels::NAMESPACE,
        labels::SERVICEGROUP,
        labels::PORTNAME,
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
        (2usize, labels::SERVICEGROUP),
        (3usize, labels::PORTNAME),
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

/// 提供指标功能访问入口的包装结构。
/// 该结构通过 DistributedRuntime、Namespace、ServiceGroup、PortName 上的 `.metrics()` 方法访问。
pub struct Metrics<H: MetricsHierarchy> {
    hierarchy: H,
}

impl<H: MetricsHierarchy> Metrics<H> {
    // 中文说明：保存传入的层级对象，后续所有 create_* 方法都会通过它定位对应的 metrics registry。
    pub fn new(hierarchy: H) -> Self {
        let metrics = Self { hierarchy };
        metrics
    }

    // 待办：补充更多 Prometheus 指标类型的支持：
    // - Counter：已实现 - create_counter()
    // - CounterVec：已实现 - create_countervec()
    // - Gauge：已实现 - create_gauge()
    // - GaugeVec：已实现 - create_gaugevec()
    // - GaugeHistogram：create_gauge_histogram() - 用于 gauge 直方图
    // - Histogram：已实现 - create_histogram()
    // - 带自定义桶的 HistogramVec：create_histogram_with_buckets()
    // - Info：create_info() - 用于带标签的 info 指标
    // - IntCounter：已实现 - create_intcounter()
    // - IntCounterVec：已实现 - create_intcountervec()
    // - IntGauge：已实现 - create_intgauge()
    // - IntGaugeVec：已实现 - create_intgaugevec()
    // - Stateset：create_stateset() - 用于状态类指标
    // - Summary：create_summary() - 用于分位数与 sum/count 指标
    // - SummaryVec：create_summary_vec() - 用于带标签的 summary
    // - Untyped：create_untyped() - 用于无类型指标
    //
    // 注意：下面 create_* 方法的顺序与 lib/bindings/python/rust/lib.rs::Metrics 中保持一致，
    // 新增指标类型时请同步两处。

    /// 创建一个 Counter 指标
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

    /// 创建一个带标签名的 CounterVec 指标（用于动态标签）
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

    /// 创建一个 Gauge 指标
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

    /// 创建一个带标签名的 GaugeVec 指标（用于动态标签）
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

    /// 创建一个带自定义桶的 Histogram 指标
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

    /// 创建一个 IntCounter 指标
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

    /// 创建一个带标签名的 IntCounterVec 指标（用于动态标签）
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

    /// 创建一个 IntGauge 指标
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

    /// 创建一个带标签名的 IntGaugeVec 指标（用于动态标签）
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

    /// 以 Prometheus 文本格式获取指标
    // 中文说明：从当前层级对应的 registry 中导出 Prometheus 文本格式结果，供抓取接口直接使用。
    pub fn prometheus_expfmt(&self) -> anyhow::Result<String> {
        let registry = self.hierarchy.get_metrics_registry();
        registry.prometheus_expfmt_combined()
    }
}

/// 该 trait 应由所有指标注册表实现，包括 Prometheus、Envy、OpenTelemetry 等。
/// 它提供了创建与管理指标、组织子注册表、以及生成 Prometheus 文本格式输出的统一接口。
use crate::traits::DistributedRuntimeProvider;

pub trait MetricsHierarchy: Send + Sync {
    // ========================================================================
    // 必需方法 — 所有类型都必须实现
    // ========================================================================

    /// 获取该层级的名称（不含任何层级前缀）
    fn basename(&self) -> String;

    /// 以实际对象（而非字符串）的形式获取父层级。
    /// 返回一个层级引用列表，从根到直接父层依次排列。
    /// 例如，PortName 会返回 [DRT, Namespace, ServiceGroup]。
    fn parent_hierarchies(&self) -> Vec<&dyn MetricsHierarchy>;

    /// 获取该层级所持有的指标注册表引用
    fn get_metrics_registry(&self) -> &MetricsRegistry;

    // ========================================================================
    // 提供方法 — 已有默认实现
    // ========================================================================

    /// 获取该层级的连接 ID（discovery 实例 ID）。
    ///
    /// 当层级可以访问 DistributedRuntime（例如 Namespace、ServiceGroup、PortName）时
    /// 返回 `Some(id)`。`create_metric()` 据此自动注入 `worker_id` 标签。默认返回 `None`。
    // 中文说明：默认情况下层级对象不提供连接 ID，因此返回 None，具体类型需要时再自行覆写。
    fn connection_id(&self) -> Option<u64> {
        Option::<u64>::None
    }

    /// 访问该层级的指标接口
    /// 这是一个提供方法，适用于任何实现了 MetricsHierarchy 的类型
    // 中文说明：为任意实现了 MetricsHierarchy 的对象生成一个轻量级 Metrics 包装器，便于继续调用 create_* 系列方法。
    fn metrics(&self) -> Metrics<&Self>
    where
        Self: Sized,
    {
        let metrics = Metrics::new(self);
        metrics
    }
}

// 为实现了 MetricsHierarchy 的类型的引用提供通用（blanket）实现
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

/// 运行时回调函数的类型别名，用于降低类型复杂度
///
/// 该类型表示一个 Arc 包装的回调函数，具备以下特性：
/// - 可在多个线程与上下文间高效共享
/// - 可克隆而不复制底层闭包
/// - 可用于需要 'static 生命周期的泛型上下文
///
/// 类型中显式包含 Arc 包装以明确表达共享语义。
pub type PrometheusUpdateCallback = Arc<dyn Fn() -> anyhow::Result<()> + Send + Sync + 'static>;

/// 返回 Prometheus 文本的曝光文本回调函数的类型别名
pub type PrometheusExpositionFormatCallback =
    Arc<dyn Fn() -> anyhow::Result<String> + Send + Sync + 'static>;

/// 为某个层级保存 Prometheus 注册表及其关联回调的结构。
///
/// 所有字段都是 Arc 包装的，因此克隆时会共享状态。这保证在克隆实例
/// （例如克隆的 Client/PortName）上注册的指标对原始对象可见。
#[derive(Clone)]
pub struct MetricsRegistry {
    /// 该层级的 Prometheus 注册表。
    /// 采用 Arc 包装，使克隆体共享同一注册表（在克隆体上注册的指标处处可见）。
    pub prometheus_registry: Arc<std::sync::RwLock<prometheus::Registry>>,

    /// 在输出合并的 `/metrics` 时需要纳入的子注册表。
    ///
    /// 设计原因：
    /// - 指标只注册到当前层级的本地注册表，避免不同 portname 用不同常量标签
    ///   注册同名指标时出现 Prometheus 描述符冲突。
    /// - `child_registries` 把“需要采集的内容”重建为一棵注册表树，使 `/metrics` 可以：
    ///   - 递归遍历注册表；
    ///   - 把各指标族合并为一份曝光负载；
    ///   - 对完全重复的序列告警/丢弃，同时允许同名指标携带不同标签。
    child_registries: Arc<std::sync::RwLock<Vec<MetricsRegistry>>>,

    /// 在采集指标前调用的更新回调。
    /// 使用 Arc 包装以在克隆间保留回调（防止 MetricsRegistry 被克隆时丢失回调）。
    pub prometheus_update_callbacks: Arc<std::sync::RwLock<Vec<PrometheusUpdateCallback>>>,

    /// 返回 Prometheus 曝光文本、追加到指标输出末尾的回调。
    /// 使用 Arc 包装以在克隆间保留回调（例如在 PortName 注册的 vLLM 回调在 DRT 仍可访问）。
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
    /// 创建一个新的指标注册表，包含空的 Prometheus 注册表与回调列表
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

    /// 添加一个子注册表，使其被纳入合并的 /metrics 输出中。
    ///
    /// 按底层 Prometheus 注册表指针去重，因此通过克隆体重复注册是安全的。
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
        // 递归遍历子 registry，使任意层级（DRT/namespace/servicegroup/portname）上调用
        // `prometheus_expfmt()` 都能包含其后代的指标。
        //
        // 按底层 Prometheus 注册表指针去重，使多条路径（例如同时直接注册到根节点）
        // 不会重复输出。
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
    ///    输出顺序与注册顺序无关、始终保持稳定。
    /// 3. 使用私有 `series_dedup_key` 把"family 名 + 排序后的 label 对"压成一个
    ///    可哈希字符串，集中表达"同 series"的判定逻辑，避免哈希键构造代码散落。
    /// 4. 把合并后的 family 编码为文本，再依次追加每个 registry 的 exposition
    ///    callback 输出（使用统一的换行拼接规则）。
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

    /// 添加一个接收 MetricsHierarchy 引用的回调函数
    // 中文说明：注册一个抓取前执行的更新回调，后续 scrape 时会按顺序触发这些回调。
    pub fn add_update_callback(&self, callback: PrometheusUpdateCallback) {
        let mut callbacks = self.prometheus_update_callbacks.write().unwrap();
        callbacks.push(callback);
    }

    /// 添加一个返回 Prometheus 文本的曝光文本回调
    // 中文说明：注册一个额外文本回调，用于在标准指标文本后面追加自定义 exposition 内容。
    pub fn add_expfmt_callback(&self, callback: PrometheusExpositionFormatCallback) {
        let mut callbacks = self.prometheus_expfmt_callbacks.write().unwrap();
        callbacks.push(callback);
    }

    /// 执行所有更新回调并返回它们的结果
    // 中文说明：顺序执行所有更新回调，并把每个回调的结果完整收集返回给调用方处理。
    pub fn execute_update_callbacks(&self) -> Vec<anyhow::Result<()>> {
        let callbacks = self.prometheus_update_callbacks.read().unwrap();
        let results = callbacks.iter().map(|callback| callback()).collect();
        results
    }

    /// 执行所有曝光文本回调并返回拼接后的文本
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

    /// 把一个 Prometheus 指标 collector 注册到当前 registry
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

    /// 把一个 Prometheus 指标 collector 注册到当前 registry；失败时记录 warning 而不返回错误。
    // 中文说明：尝试注册 collector；如果失败则仅记录 warning，不把错误继续向上传播。
    pub fn add_metric_or_warn(&self, collector: Box<dyn prometheus::core::Collector>, name: &str) {
        match self.add_metric(collector) {
            Ok(()) => {}
            Err(error) => {
                tracing::warn!(error = %error, metric = name, "Failed to register metric");
            }
        }
    }

    /// 获取 Prometheus registry 的只读锁，用于抓取
    // 中文说明：返回底层 Prometheus registry 的只读锁，供外部直接执行 gather 或其他只读操作。
    pub fn get_prometheus_registry(&self) -> std::sync::RwLockReadGuard<'_, prometheus::Registry> {
        let registry = self.prometheus_registry.read().unwrap();
        registry
    }

    /// 如果 Prometheus registry 中已经存在指定名称的指标，则返回 true
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

    /// 基础函数：根据谓词过滤 Prometheus 输出行。
    /// 返回所有匹配谓词的行，并转换为 String。
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

    /// 提取所有 servicegroup 指标（不包含 help 文本与类型定义）。
    /// 仅返回带值的实际指标行。
    pub fn extract_metrics(input: &str) -> Vec<String> {
        filter_prometheus_lines(input, |line| {
            line.starts_with(&format!("{}_", name_prefix::SERVICEGROUP))
                && !line.starts_with("#")
                && !line.trim().is_empty()
        })
    }

    /// 解析一行 Prometheus 指标文本，提取其名称、标签与值。
    /// 用于测试端到端结果而非中间状态，因此不直接读取指标对象。
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

    /// 向 Prometheus 指标数据行中注入一个 `worker_id` 标签。
    /// Prometheus 会把常量标签（如 worker_id）排在特殊标签（如直方图的 `le`）之前，
    /// 因此对于直方图桶行，我们在 `,le=` 之前插入；对其他指标行则在闭合 `}` 之前插入。
    /// 注释行与不含标签的行保持不变。
    pub fn inject_worker_id(expected: &str, wid: &str) -> String {
        let wid_label = format!(",worker_id=\"{}\"", wid);
        expected
            .lines()
            .map(|line| {
                if line.starts_with('#') || line.trim().is_empty() || !line.contains('{') {
                    line.to_string()
                } else if let Some(le_pos) = line.find(",le=") {
                    // 直方图桶行：worker_id 是常量标签，`le` 是特殊标签，
                    // 所以在 Prometheus 输出中 worker_id 排在 `le` 之前。
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
    fn test_build_servicegroup_metric_name_with_prefix() {
        // 验证 build_servicegroup_metric_name 能正确加上 pagoda_servicegroup 前缀
        let result = build_servicegroup_metric_name("requests");
        assert_eq!(result, "pagoda_servicegroup_requests");

        let result = build_servicegroup_metric_name("counter");
        assert_eq!(result, "pagoda_servicegroup_counter");
    }

    #[test]
    fn test_parse_prometheus_metric() {
        use super::test_helpers::parse_prometheus_metric;
        use std::collections::HashMap;

        // 解析一个带标签的指标
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

        // 解析一个不带标签的指标
        let line = "cpu_usage 98.5";
        let parsed = parse_prometheus_metric(line);
        assert!(parsed.is_some());

        let (name, labels, value) = parsed.unwrap();
        assert_eq!(name, "cpu_usage");
        assert!(labels.is_empty());
        assert_eq!(value, 98.5);

        // 解析一个带浮点值的指标
        let line = "response_time{service=\"api\"} 0.123";
        let parsed = parse_prometheus_metric(line);
        assert!(parsed.is_some());

        let (name, labels, value) = parsed.unwrap();
        assert_eq!(name, "response_time");

        let mut expected_labels = HashMap::new();
        expected_labels.insert("service".to_string(), "api".to_string());
        assert_eq!(labels, expected_labels);

        assert_eq!(value, 0.123);

        // 解析无效行
        assert!(parse_prometheus_metric("").is_none()); // 空行
        assert!(parse_prometheus_metric("# HELP metric description").is_none()); // help 文本
        assert!(parse_prometheus_metric("# TYPE metric counter").is_none()); // 类型定义
        assert!(parse_prometheus_metric("metric_name").is_none()); // 缺少值

        println!("✓ Prometheus metric parsing works correctly!");
    }

    #[test]
    fn test_metrics_registry_entry_callbacks() {
        use crate::MetricsRegistry;
        use std::sync::atomic::{AtomicUsize, Ordering};

        // 测试 1：基本回调执行，伴随计数器递增
        {
            let registry = MetricsRegistry::new();
            let counter = Arc::new(AtomicUsize::new(0));

            // 添加多个具有不同递增值的回调
            for increment in [1, 10, 100] {
                let counter_clone = counter.clone();
                registry.add_update_callback(Arc::new(move || {
                    counter_clone.fetch_add(increment, Ordering::SeqCst);
                    Ok(())
                }));
            }

            // 验证计数器从 0 开始
            assert_eq!(counter.load(Ordering::SeqCst), 0);

            // 首次执行
            let results = registry.execute_update_callbacks();
            assert_eq!(results.len(), 3);
            assert!(results.iter().all(|r| r.is_ok()));
            assert_eq!(counter.load(Ordering::SeqCst), 111); // 1 + 10 + 100

            // 第二次执行 — 回调应可复用
            let results = registry.execute_update_callbacks();
            assert_eq!(results.len(), 3);
            assert_eq!(counter.load(Ordering::SeqCst), 222); // 111 + 111

            // 测试克隆 — 克隆体共享回调（回调是 Arc 包装的）
            let cloned = registry.clone();
            assert_eq!(cloned.execute_update_callbacks().len(), 3);
            assert_eq!(counter.load(Ordering::SeqCst), 333); // 222 + 111

            // 原始对象仍持有回调并共享同一个 Arc
            registry.execute_update_callbacks();
            assert_eq!(counter.load(Ordering::SeqCst), 444); // 333 + 111
        }

        // 测试 2：成功与错误回调混合
        {
            let registry = MetricsRegistry::new();
            let counter = Arc::new(AtomicUsize::new(0));

            // 成功的回调
            let counter_clone = counter.clone();
            registry.add_update_callback(Arc::new(move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }));

            // 错误回调
            registry.add_update_callback(Arc::new(|| Err(anyhow::anyhow!("Simulated error"))));

            // 另一个成功的回调
            let counter_clone = counter.clone();
            registry.add_update_callback(Arc::new(move || {
                counter_clone.fetch_add(10, Ordering::SeqCst);
                Ok(())
            }));

            // 执行并验证混合结果
            let results = registry.execute_update_callbacks();
            assert_eq!(results.len(), 3);
            assert!(results[0].is_ok());
            assert!(results[1].is_err());
            assert!(results[2].is_ok());

            // 验证错误消息
            assert_eq!(
                results[1].as_ref().unwrap_err().to_string(),
                "Simulated error"
            );

            // 验证成功的回调仍然执行
            assert_eq!(counter.load(Ordering::SeqCst), 11); // 1 + 10

            // 再次执行 — 错误应保持一致
            let results = registry.execute_update_callbacks();
            assert!(results[1].is_err());
            assert_eq!(counter.load(Ordering::SeqCst), 22); // 11 + 11
        }

        // 测试 3：空注册表
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
        let servicegroup = namespace.servicegroup(COMPONENT_NAME).unwrap();
        let portname = servicegroup.portname(ENDPOINT_NAME);

        // DRT
        assert_eq!(drt.basename(), DRT_NAME);
        assert_eq!(drt.parent_hierarchies().len(), 0);
        // DRT 层级名称即其 basename（空字符串）

        // Namespace
        assert_eq!(namespace.basename(), NAMESPACE_NAME);
        assert_eq!(namespace.parent_hierarchies().len(), 1);
        assert_eq!(namespace.parent_hierarchies()[0].basename(), DRT_NAME);
        // 由于父层为空，Namespace 层级名称即其 basename

        // ServiceGroup
        assert_eq!(servicegroup.basename(), COMPONENT_NAME);
        assert_eq!(servicegroup.parent_hierarchies().len(), 2);
        assert_eq!(servicegroup.parent_hierarchies()[0].basename(), DRT_NAME);
        assert_eq!(servicegroup.parent_hierarchies()[1].basename(), NAMESPACE_NAME);
        // ServiceGroup 层级结构由上面的逐条断言验证

        // PortName
        assert_eq!(portname.basename(), ENDPOINT_NAME);
        assert_eq!(portname.parent_hierarchies().len(), 3);
        assert_eq!(portname.parent_hierarchies()[0].basename(), DRT_NAME);
        assert_eq!(portname.parent_hierarchies()[1].basename(), NAMESPACE_NAME);
        assert_eq!(portname.parent_hierarchies()[2].basename(), COMPONENT_NAME);
        // PortName 层级结构由上面的逐条断言验证

        // 层级间的父子关系
        assert!(
            namespace
                .parent_hierarchies()
                .iter()
                .any(|h| h.basename() == drt.basename())
        );
        assert!(
            servicegroup
                .parent_hierarchies()
                .iter()
                .any(|h| h.basename() == namespace.basename())
        );
        assert!(
            portname
                .parent_hierarchies()
                .iter()
                .any(|h| h.basename() == servicegroup.basename())
        );

        // 层级深度
        assert_eq!(drt.parent_hierarchies().len(), 0);
        assert_eq!(namespace.parent_hierarchies().len(), 1);
        assert_eq!(servicegroup.parent_hierarchies().len(), 2);
        assert_eq!(portname.parent_hierarchies().len(), 3);

        // 非法 namespace 的行为 — 被清洗为 "_123" 并成功
        // 当前未启用名称校验（参见 servicegroup.rs 中的 TODO 注释），
        // 因此非法字符会在 MetricsRegistry 中被清洗而非被拒绝。
        let invalid_namespace = drt.namespace("@@123").unwrap();
        let result =
            invalid_namespace
                .metrics()
                .create_counter("test_counter", "A test counter", &[]);
        assert!(result.is_ok());
        if let Ok(counter) = &result {
            // 验证 namespace 已在标签中被清洗为 "_123"
            let desc = counter.desc();
            let namespace_label = desc[0]
                .const_label_pairs
                .iter()
                .find(|l| l.name() == "pagoda_namespace")
                .expect("Should have pagoda_namespace label");
            assert_eq!(namespace_label.value(), "_123");
        }

        // 有效 namespace 正常工作
        let valid_namespace = drt.namespace("ns567").unwrap();
        assert!(
            valid_namespace
                .metrics()
                .create_counter("test_counter", "A test counter", &[])
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_expfmt_callback_only_registered_on_portname_is_included_once() {
        // 决定性测试：如果一个 expfmt 回调仅注册在 portname 的 registry 上，
        // 从根（DRT）抓取时应通过子 registry 遍历恰好包含它一次。
        let drt = create_test_drt_async().await;
        let namespace = drt.namespace("ns_expfmt_ep_only").unwrap();
        let servicegroup = namespace.servicegroup("comp_expfmt_ep_only").unwrap();
        let portname = servicegroup.portname("ep_expfmt_ep_only");

        let metric_line = "pagoda_servicegroup_active_decode_blocks{dp_rank=\"0\"} 0\n";
        let callback: PrometheusExpositionFormatCallback =
            Arc::new(move || Ok(metric_line.to_string()));

        portname
            .get_metrics_registry()
            .add_expfmt_callback(callback);

        let output = drt.metrics().prometheus_expfmt().unwrap();
        let occurrences = output
            .lines()
            .filter(|line| line == &metric_line.trim_end_matches('\n'))
            .count();

        assert_eq!(
            occurrences, 1,
            "portname-registered exposition callback should appear once, got {} occurrences\n\n{}",
            occurrences, output
        );
    }

    #[tokio::test]
    async fn test_recursive_namespace() {
        // 创建一个用于测试的分布式运行时
        let drt = create_test_drt_async().await;

        // 创建一个深度链式的 namespace：ns1.ns2.ns3
        let ns1 = drt.namespace("ns1").unwrap();
        let ns2 = ns1.namespace("ns2").unwrap();
        let ns3 = ns2.namespace("ns3").unwrap();

        // 在最深的 namespace 中创建一个 servicegroup
        let servicegroup = ns3.servicegroup("test-servicegroup").unwrap();

        // 验证层级结构
        assert_eq!(ns1.basename(), "ns1");
        assert_eq!(ns1.parent_hierarchies().len(), 1);
        assert_eq!(ns1.parent_hierarchies()[0].basename(), "");
        // 由于父层为空，ns1 层级名称即其 basename

        assert_eq!(ns2.basename(), "ns2");
        assert_eq!(ns2.parent_hierarchies().len(), 2);
        assert_eq!(ns2.parent_hierarchies()[0].basename(), "");
        assert_eq!(ns2.parent_hierarchies()[1].basename(), "ns1");
        // ns2 层级结构由上面的父层断言验证

        assert_eq!(ns3.basename(), "ns3");
        assert_eq!(ns3.parent_hierarchies().len(), 3);
        assert_eq!(ns3.parent_hierarchies()[0].basename(), "");
        assert_eq!(ns3.parent_hierarchies()[1].basename(), "ns1");
        assert_eq!(ns3.parent_hierarchies()[2].basename(), "ns2");
        // ns3 层级结构由上面的父层断言验证

        assert_eq!(servicegroup.basename(), "test-servicegroup");
        assert_eq!(servicegroup.parent_hierarchies().len(), 4);
        assert_eq!(servicegroup.parent_hierarchies()[0].basename(), "");
        assert_eq!(servicegroup.parent_hierarchies()[1].basename(), "ns1");
        assert_eq!(servicegroup.parent_hierarchies()[2].basename(), "ns2");
        assert_eq!(servicegroup.parent_hierarchies()[3].basename(), "ns3");
        // servicegroup 层级结构由上面的父层断言验证

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
        // 使用便于测试的构造函数创建真实的 DRT 与 registry
        let drt = create_test_drt_async().await;

        // 使用一个简单的常量 namespace 名称
        let namespace_name = "ns345";

        let namespace = drt.namespace(namespace_name).unwrap();
        let servicegroup = namespace.servicegroup("comp345").unwrap();
        let portname = servicegroup.portname("ep345");

        // 测试 Counter 创建
        let counter = portname
            .metrics()
            .create_counter("testcounter", "A test counter", &[])
            .unwrap();
        counter.inc_by(123.456789);
        let epsilon = 0.01;
        assert!((counter.get() - 123.456789).abs() < epsilon);

        let portname_output_raw = portname.metrics().prometheus_expfmt().unwrap();
        println!("PortName output:");
        println!("{}", portname_output_raw);

        // worker_id 是运行时生成的（etcd lease ID），因此从 DRT 获取它，
        // 并通过 inject_worker_id 助手注入到预期字符串中。
        let wid = format!("{:x}", drt.connection_id());
        use super::test_helpers::inject_worker_id;

        let expected_portname_output = inject_worker_id(
            r#"# HELP pagoda_servicegroup_testcounter A test counter
# TYPE pagoda_servicegroup_testcounter counter
pagoda_servicegroup_testcounter{pagoda_servicegroup="comp345",pagoda_portname="ep345",pagoda_namespace="ns345"} 123.456789"#,
            &wid,
        );

        assert_eq!(
            portname_output_raw.trim_end_matches('\n'),
            expected_portname_output.trim_end_matches('\n'),
            "\n=== ENDPOINT COMPARISON FAILED ===\n\
             Actual:\n{}\n\
             Expected:\n{}\n\
             ==============================",
            portname_output_raw,
            expected_portname_output
        );

        // 测试 Gauge 创建
        let gauge = servicegroup
            .metrics()
            .create_gauge("testgauge", "A test gauge", &[])
            .unwrap();
        gauge.set(50000.0);
        assert_eq!(gauge.get(), 50000.0);

        // 测试 ServiceGroup 的 Prometheus 格式输出（gauge + histogram）
        let servicegroup_output_raw = servicegroup.metrics().prometheus_expfmt().unwrap();
        println!("ServiceGroup output:");
        println!("{}", servicegroup_output_raw);

        let expected_servicegroup_output = inject_worker_id(
            r#"# HELP pagoda_servicegroup_testcounter A test counter
# TYPE pagoda_servicegroup_testcounter counter
pagoda_servicegroup_testcounter{pagoda_servicegroup="comp345",pagoda_portname="ep345",pagoda_namespace="ns345"} 123.456789
# HELP pagoda_servicegroup_testgauge A test gauge
# TYPE pagoda_servicegroup_testgauge gauge
pagoda_servicegroup_testgauge{pagoda_servicegroup="comp345",pagoda_namespace="ns345"} 50000"#,
            &wid,
        );

        assert_eq!(
            servicegroup_output_raw.trim_end_matches('\n'),
            expected_servicegroup_output.trim_end_matches('\n'),
            "\n=== COMPONENT COMPARISON FAILED ===\n\
             Actual:\n{}\n\
             Expected:\n{}\n\
             ==============================",
            servicegroup_output_raw,
            expected_servicegroup_output
        );

        let intcounter = namespace
            .metrics()
            .create_intcounter("testintcounter", "A test int counter", &[])
            .unwrap();
        intcounter.inc_by(12345);
        assert_eq!(intcounter.get(), 12345);

        // 测试 Namespace 的 Prometheus 格式输出（int_counter + gauge + histogram）
        let namespace_output_raw = namespace.metrics().prometheus_expfmt().unwrap();
        println!("Namespace output:");
        println!("{}", namespace_output_raw);

        let expected_namespace_output = inject_worker_id(
            r#"# HELP pagoda_servicegroup_testcounter A test counter
# TYPE pagoda_servicegroup_testcounter counter
pagoda_servicegroup_testcounter{pagoda_servicegroup="comp345",pagoda_portname="ep345",pagoda_namespace="ns345"} 123.456789
# HELP pagoda_servicegroup_testgauge A test gauge
# TYPE pagoda_servicegroup_testgauge gauge
pagoda_servicegroup_testgauge{pagoda_servicegroup="comp345",pagoda_namespace="ns345"} 50000
# HELP pagoda_servicegroup_testintcounter A test int counter
# TYPE pagoda_servicegroup_testintcounter counter
pagoda_servicegroup_testintcounter{pagoda_namespace="ns345"} 12345"#,
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

        // 测试 IntGauge 创建
        let intgauge = namespace
            .metrics()
            .create_intgauge("testintgauge", "A test int gauge", &[])
            .unwrap();
        intgauge.set(42);
        assert_eq!(intgauge.get(), 42);

        // 测试 IntGaugeVec 创建
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

        // 测试 CounterVec 创建
        let countervec = portname
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

        // 测试 Histogram 创建
        let histogram = servicegroup
            .metrics()
            .create_histogram("testhistogram", "A test histogram", &[], None)
            .unwrap();
        histogram.observe(1.0);
        histogram.observe(2.5);
        histogram.observe(4.0);

        // 测试 DRT 的 Prometheus 格式输出（所有指标合并）
        let drt_output_raw = drt.metrics().prometheus_expfmt().unwrap();
        println!("DRT output:");
        println!("{}", drt_output_raw);

        // uptime_seconds 的值是动态的（取决于已经过的壁钟时间），
        // 因此我们精确检查其他所有行，并单独验证 uptime。
        let expected_drt_output_without_uptime = inject_worker_id(
            r#"# HELP pagoda_servicegroup_testcounter A test counter
# TYPE pagoda_servicegroup_testcounter counter
pagoda_servicegroup_testcounter{pagoda_servicegroup="comp345",pagoda_portname="ep345",pagoda_namespace="ns345"} 123.456789
# HELP pagoda_servicegroup_testcountervec A test counter vector
# TYPE pagoda_servicegroup_testcountervec counter
pagoda_servicegroup_testcountervec{pagoda_servicegroup="comp345",pagoda_portname="ep345",pagoda_namespace="ns345",method="GET",service="api",status="200"} 10
pagoda_servicegroup_testcountervec{pagoda_servicegroup="comp345",pagoda_portname="ep345",pagoda_namespace="ns345",method="POST",service="api",status="201"} 5
# HELP pagoda_servicegroup_testgauge A test gauge
# TYPE pagoda_servicegroup_testgauge gauge
pagoda_servicegroup_testgauge{pagoda_servicegroup="comp345",pagoda_namespace="ns345"} 50000
# HELP pagoda_servicegroup_testhistogram A test histogram
# TYPE pagoda_servicegroup_testhistogram histogram
pagoda_servicegroup_testhistogram_bucket{pagoda_servicegroup="comp345",pagoda_namespace="ns345",le="0.005"} 0
pagoda_servicegroup_testhistogram_bucket{pagoda_servicegroup="comp345",pagoda_namespace="ns345",le="0.01"} 0
pagoda_servicegroup_testhistogram_bucket{pagoda_servicegroup="comp345",pagoda_namespace="ns345",le="0.025"} 0
pagoda_servicegroup_testhistogram_bucket{pagoda_servicegroup="comp345",pagoda_namespace="ns345",le="0.05"} 0
pagoda_servicegroup_testhistogram_bucket{pagoda_servicegroup="comp345",pagoda_namespace="ns345",le="0.1"} 0
pagoda_servicegroup_testhistogram_bucket{pagoda_servicegroup="comp345",pagoda_namespace="ns345",le="0.25"} 0
pagoda_servicegroup_testhistogram_bucket{pagoda_servicegroup="comp345",pagoda_namespace="ns345",le="0.5"} 0
pagoda_servicegroup_testhistogram_bucket{pagoda_servicegroup="comp345",pagoda_namespace="ns345",le="1"} 1
pagoda_servicegroup_testhistogram_bucket{pagoda_servicegroup="comp345",pagoda_namespace="ns345",le="2.5"} 2
pagoda_servicegroup_testhistogram_bucket{pagoda_servicegroup="comp345",pagoda_namespace="ns345",le="5"} 3
pagoda_servicegroup_testhistogram_bucket{pagoda_servicegroup="comp345",pagoda_namespace="ns345",le="10"} 3
pagoda_servicegroup_testhistogram_bucket{pagoda_servicegroup="comp345",pagoda_namespace="ns345",le="+Inf"} 3
pagoda_servicegroup_testhistogram_sum{pagoda_servicegroup="comp345",pagoda_namespace="ns345"} 7.5
pagoda_servicegroup_testhistogram_count{pagoda_servicegroup="comp345",pagoda_namespace="ns345"} 3
# HELP pagoda_servicegroup_testintcounter A test int counter
# TYPE pagoda_servicegroup_testintcounter counter
pagoda_servicegroup_testintcounter{pagoda_namespace="ns345"} 12345
# HELP pagoda_servicegroup_testintgauge A test int gauge
# TYPE pagoda_servicegroup_testintgauge gauge
pagoda_servicegroup_testintgauge{pagoda_namespace="ns345"} 42
# HELP pagoda_servicegroup_testintgaugevec A test int gauge vector
# TYPE pagoda_servicegroup_testintgaugevec gauge
pagoda_servicegroup_testintgaugevec{pagoda_namespace="ns345",instance="server1",service="api",status="active"} 10
pagoda_servicegroup_testintgaugevec{pagoda_namespace="ns345",instance="server2",service="api",status="inactive"} 0"#,
            &wid,
        );

        // 把实际输出拆分为非 uptime 行，并验证 uptime 值行。
        // uptime 指标现在携带 worker_id 标签，因此我们按指标名前缀匹配，
        // 并把值提取为最后一个以空白分隔的 token。
        let mut non_uptime_lines = Vec::new();
        let mut saw_uptime_value = false;
        for line in drt_output_raw.trim_end_matches('\n').lines() {
            if line.starts_with("pagoda_servicegroup_uptime_seconds") && !line.starts_with('#') {
                let val_str = line.split_whitespace().last().unwrap();
                val_str.parse::<f64>().expect("uptime should be a float");
                saw_uptime_value = true;
            } else if line.starts_with("# HELP pagoda_servicegroup_uptime_seconds")
                || line.starts_with("# TYPE pagoda_servicegroup_uptime_seconds")
            {
                // 跳过 uptime 的 HELP/TYPE 行（我们只通过值验证它存在）
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

        // 稍作等待，使下一次抓取时 uptime gauge 明显为正值。
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let drt_output_after = drt.metrics().prometheus_expfmt().unwrap();
        let uptime_line = drt_output_after
            .lines()
            .find(|l| l.starts_with("pagoda_servicegroup_uptime_seconds") && !l.starts_with('#'))
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
        // 带 servicegroup 指标的测试数据
        let test_input = r#"# HELP pagoda_servicegroup_requests Total requests
# TYPE pagoda_servicegroup_requests counter
pagoda_servicegroup_requests 42
# HELP pagoda_servicegroup_latency Response latency
# TYPE pagoda_servicegroup_latency histogram
pagoda_servicegroup_latency_bucket{le="0.1"} 10
pagoda_servicegroup_latency_bucket{le="0.5"} 25
pagoda_servicegroup_errors_total 5"#;

        // 测试 extract_metrics（仅保留实际指标行，排除 help/type）
        let metrics_only = super::test_helpers::extract_metrics(test_input);
        assert_eq!(metrics_only.len(), 4); // 4 条实际指标行（排除 help/type）
        assert!(
            metrics_only
                .iter()
                .all(|line| line.starts_with("pagoda_servicegroup") && !line.starts_with("#"))
        );

        println!("✓ All refactored filter functions work correctly!");
    }

    #[tokio::test]
    async fn test_same_metric_name_different_portnames() {
        // 验证同一指标名可以存在于不同 portname 中而不冲突。
        // 这验证了多 registry 方案：每个 portname 有自己的 registry，
        // 指标在抓取时按不同标签合并。
        let drt = create_test_drt_async().await;
        let namespace = drt.namespace("ns_test").unwrap();
        let servicegroup = namespace.servicegroup("comp_test").unwrap();

        // 创建两个使用相同指标名的 portname
        let ep1 = servicegroup.portname("ep1");
        let ep2 = servicegroup.portname("ep2");

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

        // 从 servicegroup 层级获取合并后的 Prometheus 输出
        let output = servicegroup.metrics().prometheus_expfmt().unwrap();

        let wid = format!("{:x}", drt.connection_id());
        use super::test_helpers::inject_worker_id;

        let expected_output = inject_worker_id(
            r#"# HELP pagoda_servicegroup_requests_total Total requests
# TYPE pagoda_servicegroup_requests_total counter
pagoda_servicegroup_requests_total{pagoda_servicegroup="comp_test",pagoda_portname="ep1",pagoda_namespace="ns_test"} 100
pagoda_servicegroup_requests_total{pagoda_servicegroup="comp_test",pagoda_portname="ep2",pagoda_namespace="ns_test"} 200"#,
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
        // 验证重复 series（同名指标 + 同标签）能被检测并去重。
        // 这应该记录一条 warning 并只保留重复 series 中的一条。
        let drt = create_test_drt_async().await;
        let namespace = drt.namespace("ns_dup").unwrap();
        let servicegroup = namespace.servicegroup("comp_dup").unwrap();

        // 创建两个 portname，其 counter 在抓取时会具有相同标签
        let ep1 = servicegroup.portname("ep_same");
        let ep2 = servicegroup.portname("ep_same"); // 相同的 portname 名 = 重复标签

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

        // 获取合并后的输出 — 重复项应被去重
        let output = servicegroup.metrics().prometheus_expfmt().unwrap();

        let wid = format!("{:x}", drt.connection_id());
        use super::test_helpers::inject_worker_id;

        let expected_output = inject_worker_id(
            r#"# HELP pagoda_servicegroup_dup_metric Duplicate metric test
# TYPE pagoda_servicegroup_dup_metric counter
pagoda_servicegroup_dup_metric{pagoda_servicegroup="comp_dup",pagoda_portname="ep_same",pagoda_namespace="ns_dup"} 50"#,
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
        let servicegroup = Box::leak(Box::new(StaticHierarchy::new(
            "comp",
            vec![root as &dyn MetricsHierarchy, namespace as &dyn MetricsHierarchy],
            Some(0xfeedbeef),
        )));
        let portname = Box::leak(Box::new(StaticHierarchy::new(
            "ep",
            vec![
                root as &dyn MetricsHierarchy,
                namespace as &dyn MetricsHierarchy,
                servicegroup as &dyn MetricsHierarchy,
            ],
            Some(0xfeedbeef),
        )));

        root.registry.add_child_registry(&namespace.registry);
        namespace.registry.add_child_registry(&servicegroup.registry);
        servicegroup.registry.add_child_registry(&portname.registry);

        (root, namespace, servicegroup, portname)
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
        let (_root, _namespace, servicegroup, portname) = build_hierarchy_tree();

        let metric = create_metric::<prometheus::Counter, _>(
            portname,
            "requests_total",
            "Request count",
            &[("custom", "value")],
            None,
            None,
        )
        .unwrap();
        metric.inc_by(2.0);

        let output = portname.metrics().prometheus_expfmt().unwrap();
        let line = output
            .lines()
            .find(|line| line.starts_with("pagoda_servicegroup_requests_total") && !line.starts_with('#'))
            .unwrap();
        let (name, labels, value) = parse_prometheus_metric(line).unwrap();

        assert_eq!(name, "pagoda_servicegroup_requests_total");
        assert_eq!(value, 2.0);
        assert_eq!(labels.get(labels::NAMESPACE), Some(&"ns".to_string()));
        assert_eq!(labels.get(labels::SERVICEGROUP), Some(&"comp".to_string()));
        assert_eq!(labels.get(labels::PORTNAME), Some(&"ep".to_string()));
        assert_eq!(labels.get(labels::WORKER_ID), Some(&"feedbeef".to_string()));
        assert_eq!(labels.get("custom"), Some(&"value".to_string()));

        let duplicate_labels = create_metric::<prometheus::Counter, _>(
            portname,
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
            portname,
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
            portname,
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
            portname,
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
            portname,
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
            portname,
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
            portname,
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
            portname,
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
            portname,
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
            portname,
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
            portname,
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
            servicegroup,
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
            portname,
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
            portname,
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
        assert!(output.contains("pagoda_servicegroup_queue_depth"));
        assert!(output.contains("service=\"api\""));
        assert!(output.contains("state=\"ready\""));
        assert!(output.contains("pagoda_servicegroup_jobs_total"));
        assert!(output.contains("status=\"done\""));
        assert!(output.contains("pagoda_servicegroup_workers"));
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
        let input = r#"# HELP pagoda_servicegroup_requests Total requests
# TYPE pagoda_servicegroup_requests counter
pagoda_servicegroup_requests{service="api"} 42

pagoda_servicegroup_latency_bucket{service="api",le="0.5"} 25
plain_metric 1"#;

        let metrics = extract_metrics(input);
        assert_eq!(metrics.len(), 2);
        assert_eq!(metrics[0], "pagoda_servicegroup_requests{service=\"api\"} 42");
        assert_eq!(
            metrics[1],
            "pagoda_servicegroup_latency_bucket{service=\"api\",le=\"0.5\"} 25"
        );

        let injected = inject_worker_id(input, "abcd1234");
        assert!(injected.contains(
            "pagoda_servicegroup_requests{service=\"api\",worker_id=\"abcd1234\"} 42"
        ));
        assert!(injected.contains(
            "pagoda_servicegroup_latency_bucket{service=\"api\",worker_id=\"abcd1234\",le=\"0.5\"} 25"
        ));
        assert!(injected.contains("plain_metric 1"));

        let parsed = parse_prometheus_metric(
            "pagoda_servicegroup_requests{service=\"api\",worker_id=\"abcd1234\"} 42",
        )
        .unwrap();
        assert_eq!(parsed.0, "pagoda_servicegroup_requests");
        assert_eq!(parsed.1.get("worker_id"), Some(&"abcd1234".to_string()));
        assert_eq!(parsed.2, 42.0);
    }
}
