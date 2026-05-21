# `metrics` 模块设计

**源码**：`src/metrics.rs` · `src/metrics/prometheus_names.rs` · `src/metrics/tokio_perf.rs` · `src/metrics/frontend_perf.rs` · `src/metrics/request_plane.rs` · `src/metrics/transport_metrics.rs` · `src/metrics/work_handler_perf.rs`

---

## 一、模块定位

`metrics` 模块是 Pagoda 的 **Prometheus 指标管理系统**，承担三个核心职责：

1. **层级化指标命名**：通过 `MetricsHierarchy` trait 链自动拼接 `pagoda_<namespace>_<servicegroup>_<portname>_<metric>` 前缀，并自动注入 `pagoda_namespace` / `pagoda_servicegroup` / `pagoda_portname` const-label，使每条指标天然具备多维度过滤能力；
2. **Prometheus 注册表管理**：`MetricsRegistry` 以树形 `child_registries` 组织子层级，合并时按指标族名去重、按 label 集合去重，避免多 portname 注册同名指标时的 descriptor collision；
3. **预定义全局指标**：六个子模块以 `once_cell::sync::Lazy` 静态变量定义运行时、前端、传输、请求平面等 20+ 个指标，并通过 `ensure_*_registered` 家族函数保证幂等注册。

模块的两条注册路径相互独立，对应两类消费方：

| 注册路径 | 函数签名 | 消费方 |
| --- | --- | --- |
| `MetricsRegistry` 路径 | `ensure_*_registered(registry: &MetricsRegistry)` | Pagoda Runtime System Status Server（`/metrics`） |
| Raw Prometheus 路径 | `ensure_*_registered_prometheus(registry: &prometheus::Registry)` | LLM HTTP 服务（自有 `/metrics`，不经过 Runtime） |

---

## 二、文件结构与可见性

```
src/metrics.rs                — pub: PrometheusMetric / MetricsHierarchy / MetricsRegistry
                                      Metrics<H> / create_metric / 两类回调类型别名
                                      pub(crate): validate_no_duplicate_label_keys
src/metrics/
    ├── prometheus_names.rs     — pub(crate): name_prefix / labels / 指标名常量子模块
    │                              pub fn: sanitize_prometheus_label / sanitize_prometheus_name
    │                              pub fn: build_servicegroup_metric_name
  ├── tokio_perf.rs           — pub: 14 个 Lazy 静态指标（Gauge / Counter / IntGaugeVec / IntCounterVec / Histogram）
  │                              pub fn: ensure_tokio_perf_metrics_registered
  │                              pub fn: ensure_tokio_perf_metrics_registered_prometheus
  │                              pub async fn: tokio_metrics_and_canary_loop（主循环）
  ├── frontend_perf.rs        — pub: 5 个 Lazy 静态指标（HistogramVec / Histogram / Counter）
  │                              pub fn: ensure_frontend_perf_metrics_registered
  │                              pub fn: ensure_frontend_perf_metrics_registered_prometheus
  ├── request_plane.rs        — pub: 4 个 Lazy 静态指标（Histogram × 3 / Gauge × 1）
  │                              pub fn: ensure_request_plane_metrics_registered
  │                              pub fn: ensure_request_plane_metrics_registered_prometheus
  ├── transport_metrics.rs    — pub: 4 个 Lazy 静态指标（Counter × 3 / IntCounterVec × 1）
  │                              pub fn: ensure_transport_metrics_registered_prometheus
    └── work_handler_perf.rs    — pub: 8 个 Lazy 静态指标（Histogram × 3 / IntGauge × 4 / IntCounter × 1）
                                                                 pub fn: ensure_work_handler_perf_metrics_registered
                                                                 pub fn: ensure_work_handler_perf_metrics_registered_prometheus
                                                                 pub fn: ensure_work_handler_pool_metrics_registered
```

---

## 三、类型详解

---

### 3.1 `validate_no_duplicate_label_keys` — 标签键重复性检查

**来源**：`src/metrics.rs`

**设计意图**：`create_metric` 接受用户传入的 `labels: &[(&str, &str)]`，允许零到多个自定义 const-label。如果用户误传了两个相同 key（如 `&[("env", "prod"), ("env", "staging")]`），Prometheus 会在创建 `Opts` 时接受（不报错），但后续 `registry.register()` 或 `gather()` 时行为未定义。该函数在进入构造流程之前提前捕获这类错误，产生明确的错误信息，而非依赖 Prometheus 库的隐式行为。

```rust
fn validate_no_duplicate_label_keys(labels: &[(&str, &str)]) -> anyhow::Result<()>
// 遍历 labels，以 HashSet 跟踪已见 key；
// 发现重复时立即返回 Err，错误信息包含重复的 key 名称。
// 仅检查用户传入的 labels，不与自动注入的 pagoda_* 标签比较
//（自动注入标签的冲突由后续的保留键检查单独处理）
```

---

### 3.2 `PrometheusMetric` trait — Prometheus 指标统一创建接口

**来源**：`src/metrics.rs`

**设计意图**：Prometheus Rust 库的各指标类型（`Counter`、`Gauge`、`Histogram`、`*Vec`）创建 API 各不相同——`Counter::with_opts` 接受 `Opts`，`Histogram::with_opts` 接受 `HistogramOpts`，`GaugeVec::new` 额外接受 `label_names`。`create_metric` 需要一个泛型参数 `T`，但无法在一个地方用同一接口调用所有构造函数。`PrometheusMetric` 解决了这个问题：它定义三个带默认 panic 实现的构造方法，每种具体指标类型只实现自己所需的那个，其余留默认。这使 `create_metric` 可以用 `TypeId` 分派到正确路径，而不需要在泛型边界上引入更复杂的类型层次。

```rust
pub trait PrometheusMetric:
    prometheus::core::Collector + Clone + Send + Sync + 'static
{
    fn with_opts(opts: prometheus::Opts) -> Result<Self, prometheus::Error>
    where Self: Sized;
    // 用于 Counter / IntCounter / Gauge / IntGauge
    // Vec 类型调用此方法会返回 Err（有明确错误信息），防止误用

    fn with_histogram_opts_and_buckets(
        opts: prometheus::HistogramOpts,
        buckets: Option<Vec<f64>>,
    ) -> Result<Self, prometheus::Error>
    where Self: Sized
    { panic!("...") }
    // 用于 Histogram；其余类型保留 panic 默认实现

    fn with_opts_and_label_names(
        opts: prometheus::Opts,
        label_names: &[&str],
    ) -> Result<Self, prometheus::Error>
    where Self: Sized
    { panic!("...") }
    // 用于 *Vec 类型（CounterVec / GaugeVec / IntCounterVec / IntGaugeVec）
}
```

**实现的类型**：

| 类型 | 实现的方法 | 备注 |
| --- | --- | --- |
| `Counter` / `IntCounter` / `Gauge` / `IntGauge` | `with_opts` | 标量，无 label_names |
| `Histogram` | `with_histogram_opts_and_buckets` | 支持自定义 bucket 边界；`with_opts` 将 `Opts` 转成 `HistogramOpts`（丢弃 const_labels，仅保留 name/help），作为无 bucket 的兜底路径 |
| `CounterVec` / `GaugeVec` / `IntCounterVec` / `IntGaugeVec` | `with_opts_and_label_names` | 向量，需要 label_names；`with_opts` 返回明确 Err（`CounterVec` 的 `with_opts` 会 panic） |

---

### 3.3 `create_metric` — 层级前缀自动拼接 + 自动 label 注入

**来源**：`src/metrics.rs`

**设计意图**：业务代码只应关心"这个指标叫什么、描述是什么"，而不应关心它挂在哪个 namespace / servicegroup / portname 下。`create_metric` 从 `MetricsHierarchy` 中自动提取四层路径（DRT → namespace → servicegroup → portname），注入三个 const-label（`pagoda_namespace` / `pagoda_servicegroup` / `pagoda_portname`），并将指标自动注册进层级的 `MetricsRegistry`。这样，同一个 `create_metric` 调用，在不同的 portname 上调用会生成带有不同 label 值的独立指标序列，而不需要调用方手动拼接名称或管理注册。

```rust
pub fn create_metric<T: PrometheusMetric, H: MetricsHierarchy + ?Sized>(
    hierarchy: &H,            // 调用方的层级上下文（PortName / ServiceGroup / Namespace / DRT）
    metric_name: &str,        // 指标基础名（不含前缀），如 "requests_total"
    metric_desc: &str,        // 帮助文本
    labels: &[(&str, &str)],  // 用户自定义 const-label，不得包含保留 label 键
    buckets: Option<Vec<f64>>,        // 仅 Histogram 有效；其余传 None
    const_labels: Option<&[&str]>,    // 仅 *Vec 类型有效；指定可变 label 的键名
) -> anyhow::Result<T>
```

**执行步骤**：

1. `validate_no_duplicate_label_keys(labels)` — 检查用户 label 无重复键
2. 从 hierarchy 的 `parent_hierarchies()` + `basename()` 拼接层级路径 `[drt, ns, comp, ep]`
3. `build_servicegroup_metric_name(metric_name)` — 生成完整指标名（加 `pagoda_servicegroup_` 前缀）
4. 拒绝用户 label 中含有 `pagoda_namespace` / `pagoda_servicegroup` / `pagoda_portname` 键（保留键保护）
5. 从层级路径自动提取并 sanitize namespace（`hierarchy_names[1]`）、servicegroup（`[2]`）、portname（`[3]`），依次追加到 `updated_labels`
6. 追加用户 label
7. `TypeId::of::<T>()` 分派到对应 Prometheus 构造路径（Vec 类型 / Histogram / 标量），使用正确的 `with_*` 方法创建指标，并进行参数有效性验证：
   - Vec 类型：要求 `const_labels` 非空，`buckets` 必须为 `None`
   - Histogram：要求 `const_labels` 必须为 `None`，`buckets` 可选
   - 标量类型：要求 `buckets` 和 `const_labels` 均为 `None`
8. `hierarchy.get_metrics_registry().add_metric(Box::new(metric.clone()))` — 注册进当前层级的 registry
9. 返回 `T`（调用方持有，可直接用于 `.inc()` / `.observe()` 等操作）

---

### 3.4 `MetricsHierarchy` trait — 层级化指标命名抽象

**来源**：`src/metrics.rs`

**设计意图**：Pagoda 的 `DistributedRuntime` → `Namespace` → `ServiceGroup` → `PortName` 是一个自然的四层树形结构，每层都需要创建指标并暴露 `/metrics` 端点。为了使各层创建指标时不重复传递前缀字符串，且使 `create_metric` 能以统一方式处理任意层级，引入 `MetricsHierarchy` trait。每个层级只需实现 `basename()`（当前层名称）、`parent_hierarchies()`（父层链）和 `get_metrics_registry()`（本层注册表），`create_metric` 即可自动沿链向上提取完整路径。`&T` 的 blanket impl 使引用类型同样满足约束，避免大量 `&*` 解引用。

```rust
pub trait MetricsHierarchy: Send + Sync {
    fn basename(&self) -> String;
    // 当前层的短名称，如 "my_ns" / "my_servicegroup" / "generate"
    // DistributedRuntime 的 basename 为空字符串 ""

    fn parent_hierarchies(&self) -> Vec<&dyn MetricsHierarchy>;
    // 有序列表，从根到直接父层：[DRT, Namespace, ServiceGroup] for PortName
    // create_metric 遍历此列表 + self 的 basename 构造完整四段路径

    fn get_metrics_registry(&self) -> &MetricsRegistry;
    // 本层独立的 MetricsRegistry；指标仅注册进此 registry，不扇出到父层
    // （历史上曾扇出至所有父层，导致多 portname 注册同名指标时的 descriptor mismatch）

    fn metrics(&self) -> Metrics<&Self> where Self: Sized {
        Metrics::new(self)
    }
    // 便捷方法，返回 Metrics<&Self>，调用方可以写 portname.metrics().create_counter(...)
}

// &T 的 blanket impl（所有 &T: MetricsHierarchy 均委托 **self）
impl<T: MetricsHierarchy + ?Sized> MetricsHierarchy for &T { ... }
```

**实现者**：

| 类型 | `basename()` 返回值 | `parent_hierarchies()` 返回值 |
| --- | --- | --- |
| `DistributedRuntime` | `""` | `[]` |
| `Namespace` | namespace 名 | `[&drt]` |
| `ServiceGroup` | servicegroup 名 | `[&drt, &ns]` |
| `PortName` | portname 名 | `[&drt, &ns, &comp]` |

---

### 3.5 `MetricsRegistry` — Prometheus 注册表与树形合并

**来源**：`src/metrics.rs`

**设计意图**：每个层级（PortName、ServiceGroup、Namespace、DRT）持有各自独立的 `MetricsRegistry`，指标只注册在创建该指标的层级的 registry 中（不扇出）。这解决了历史版本中"多 PortName 注册同名指标"引发的 Prometheus descriptor collision 问题——同名但不同 const-label 的指标序列现在各自存放在独立 registry，合并时由 `prometheus_expfmt_combined()` 统一处理。

`child_registries` 字段维护父→子 registry 引用树，使 DRT 层调用 `prometheus_expfmt_combined()` 时能递归收集所有下游注册表的指标，产生完整的 `/metrics` 响应。`Arc<RwLock<...>>` 使 `MetricsRegistry` 在 clone 时共享底层状态——在 PortName 上注册的指标对克隆的 registry 引用同样可见，无需重新注册。

```rust
#[derive(Clone)]
pub struct MetricsRegistry {
    pub prometheus_registry: Arc<std::sync::RwLock<prometheus::Registry>>,
    // 本层的 Prometheus 注册表；Arc 保证 clone 共享同一注册表

    child_registries: Arc<std::sync::RwLock<Vec<MetricsRegistry>>>,
    // 子层级注册表列表；DRT 包含所有 Namespace，Namespace 包含所有 ServiceGroup，以此类推
    // 合并时递归遍历，以注册表指针去重（防止重复路径）

    pub prometheus_update_callbacks: Arc<std::sync::RwLock<Vec<PrometheusUpdateCallback>>>,
    // 在 Prometheus 抓取前回调：用于刷新 uptime gauge、vLLM KV cache 利用率等"外部状态"指标

    pub prometheus_expfmt_callbacks: Arc<std::sync::RwLock<Vec<PrometheusExpositionFormatCallback>>>,
    // 返回 Prometheus 文本格式字符串：用于注入来自外部系统（Python vLLM）的指标文本
    // Arc 保证在 PortName 上注册的回调对 DRT 层的 expfmt 调用同样可见
}

pub type PrometheusUpdateCallback =
    Arc<dyn Fn() -> anyhow::Result<()> + Send + Sync + 'static>;
pub type PrometheusExpositionFormatCallback =
    Arc<dyn Fn() -> anyhow::Result<String> + Send + Sync + 'static>;
```

**实现的 trait**：

| Trait | 来源 | 实现细节 |
| --- | --- | --- |
| `Clone` | derive | `Arc` 引用计数增量，所有 clone 共享底层注册表和回调列表 |
| `Default` | 手写 | 等同于 `Self::new()`，三个空 Arc 包装的数据结构 |
| `Debug` | 手写 | 打印回调数量（不暴露回调内容），避免调试输出过长 |

**`Default` 实现**：

```rust
impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}
```

`Default` 直接委托给 `Self::new()`，保证 `MetricsRegistry::default()` 和 `MetricsRegistry::new()` 两条构造路径返回完全相同的空注册表状态。这样既满足泛型上下文里对 `T: Default` 的要求，也避免维护两份独立的初始化逻辑。

**自身方法（完整列表）**：

```rust
impl MetricsRegistry {

    // ── 构造 ──────────────────────────────────────────────────────────────

    pub fn new() -> Self
    // 创建空注册表：prometheus::Registry::new() + 空 child_registries + 空回调列表

    // ── 子注册表管理 ──────────────────────────────────────────────────────

    pub fn add_child_registry(&self, child: &MetricsRegistry)
    // 以底层 prometheus_registry 的 Arc 指针去重后追加子 registry；
    // 重复注册（通过 clone 的引用）是安全的，不产生重复条目。
    // 父层级在创建子层级（Namespace 创建 ServiceGroup）时调用此方法，
    // 使父层的 prometheus_expfmt_combined() 能递归覆盖子层的指标。

    fn registries_for_combined_scrape(&self) -> Vec<MetricsRegistry>
    // 私有辅助：深度优先遍历以 self 为根的 child_registries 树，
    // 收集所有层级（含 self）的 MetricsRegistry 克隆到一个扁平 Vec。
    // 以 Arc 原始指针（*const RwLock<prometheus::Registry>）为 key 的 HashSet 去重，
    // 保证同一物理注册表不被重复采集（当 DRT 和某个 Namespace 同时持有对同一
    // MetricsRegistry 的 Arc 引用时，只收集一次）。
    // prometheus_expfmt_combined() 以此列表为迭代基础；
    // execute_update_callbacks 和 execute_expfmt_callbacks 均在此列表上按顺序执行。

    // ── 指标注册 ──────────────────────────────────────────────────────────

    pub fn add_metric(&self, collector: Box<dyn prometheus::core::Collector>) -> anyhow::Result<()>
    // 向本层 prometheus_registry 注册一个 Collector；
    // 内部调用 prometheus::Registry::register()，重复注册（相同描述符）返回 Err。
    // create_metric() 的最后一步调用此方法，将新建的指标纳入本层注册表。

    pub fn add_metric_or_warn(&self, collector: Box<dyn prometheus::core::Collector>, name: &str)
    // 注册失败时只打印 warn 日志而非返回 Err；
    // 用于预定义指标的"尽力注册"场景（ensure_*_registered 家族）——
    // 若同名指标已存在，忽略错误而非 panic，保证幂等性。

    pub fn get_prometheus_registry(&self) -> std::sync::RwLockReadGuard<'_, prometheus::Registry>
    // 返回本层 prometheus::Registry 的读锁守卫；
    // 供需要直接操作注册表的场景使用（如 tokio_perf、ensure_*_registered_prometheus）。
    // 注意：返回的守卫持有读锁，调用方不得在持锁期间再获取写锁，否则死锁。

    pub fn has_metric_named(&self, metric_name: &str) -> bool
    // 通过 gather() 扫描本层注册表中所有 MetricFamily 的名称；
    // 用于条件性注册场景：若指标已存在则跳过创建，避免 add_metric 返回错误。
    // 注意：仅查询本层注册表，不遍历 child_registries。

    // ── 回调注册 ──────────────────────────────────────────────────────────

    pub fn add_update_callback(&self, callback: PrometheusUpdateCallback)
    // 追加一个更新回调到 prometheus_update_callbacks；
    // 回调签名：Fn() -> anyhow::Result<()>，在每次 /metrics 抓取前
    //（prometheus_expfmt_combined 内、gather 之前）被调用，
    // 用于刷新外部状态（uptime Gauge、KV cache 利用率等）。
    // Arc 包装保证同一回调在 clone 间共享，注册一次、所有 clone 可见。

    pub fn add_expfmt_callback(&self, callback: PrometheusExpositionFormatCallback)
    // 追加一个 exposition 格式回调到 prometheus_expfmt_callbacks；
    // 回调签名：Fn() -> anyhow::Result<String>，返回完整 Prometheus 文本格式字符串，
    // 追加在 prometheus_expfmt_combined 的 encoder 输出之后。
    // 用于将 Python vLLM 进程的指标文本注入 Pagoda 的 /metrics 响应，
    // 无需将 Python 指标反序列化成 Rust 对象再序列化。

    // ── 回调执行 ──────────────────────────────────────────────────────────

    pub fn execute_update_callbacks(&self) -> Vec<anyhow::Result<()>>
    // 顺序执行本层 prometheus_update_callbacks 中的所有回调，
    // 收集每个回调的 Result 到 Vec 并返回；不短路，所有回调均会被调用。
    // 调用方（prometheus_expfmt_combined）负责检查并记录错误日志，但不中断流程。
    // 注意：仅执行本层回调；子层回调由 prometheus_expfmt_combined 在遍历
    // registries_for_combined_scrape 返回的列表时单独调用。

    pub fn execute_expfmt_callbacks(&self) -> String
    // 顺序执行本层 prometheus_expfmt_callbacks 中的所有回调，
    // 将各回调返回的非空字符串拼接（中间插入换行符分隔），返回合并后的文本。
    // 回调失败时只打印 error 日志，不中断，保证 /metrics 端点的高可用性。
    // 注意：仅执行本层回调；子层回调由 prometheus_expfmt_combined 在遍历列表时单独调用。

    // ── 合并输出 ──────────────────────────────────────────────────────────

    pub fn prometheus_expfmt_combined(&self) -> anyhow::Result<String>
    // 【核心方法】递归收集本层 + 所有子层的指标，合并为一个 Prometheus 文本响应
    // 详见下文"合并逻辑"说明
}
```

**`prometheus_expfmt_combined()` 合并逻辑**：

1. 调用 `registries_for_combined_scrape()` 递归（DFS）收集本层及所有子层 registry，以 `Arc` 指针去重，得到扁平 `Vec<MetricsRegistry>`
2. 对 Vec 中每个 registry 依次调用 `execute_update_callbacks()`（刷新外部状态），错误只打印不中断
3. 对每个 registry 调用 `get_prometheus_registry().gather()`，收集 `MetricFamily` 列表；按指标族名（`name`）合并入 `HashMap<String, MetricFamily>`：
   - 同名指标族：HELP 文本和 TYPE 必须一致，不一致时返回 `Err`（阻止格式不一致的注册）
   - 同族内：以 `name + 排序后的 label=value 对` 构成去重键（存入 `seen_series: HashSet<String>`），重复序列打印 `warn` 并丢弃后者（避免重复计数）
4. 将 `HashMap` 中所有族按名称排序后，通过 `prometheus::TextEncoder` 序列化为文本
5. 对 Vec 中每个 registry 依次调用 `execute_expfmt_callbacks()`，将返回的文本追加到编码输出之后（顺序与 registry 遍历顺序一致，保证确定性）

---

### 3.6 `Metrics<H>` — 层级持有者便捷包装

**来源**：`src/metrics.rs`

**设计意图**：`create_metric` 是自由函数，每次调用都需要显式传入 hierarchy 引用，略显冗长。`Metrics<H>` 持有一个 `H: MetricsHierarchy`，将 `create_metric` 包装为方法，使调用方可以写 `portname.metrics().create_counter(...)` 而非 `create_metric(&portname, ...)`。`H = &Self` 的参数化使 `Metrics` 不持有所有权，不增加额外 clone，生命周期与调用方绑定。

```rust
pub struct Metrics<H: MetricsHierarchy> {
    hierarchy: H,   // 通常为 &DistributedRuntime / &PortName 等引用，由 metrics() 方法构造
}

impl<H: MetricsHierarchy> Metrics<H> {

    // ── 标量指标创建 ───────────────────────────────────────────────────────
    pub fn new(hierarchy: H) -> Self {
        Self { hierarchy }
    }
    
    pub fn create_counter(
        &self, name: &str, description: &str, labels: &[(&str, &str)],
    ) -> anyhow::Result<prometheus::Counter>
    // 等价于 create_metric::<Counter>(hierarchy, name, description, labels, None, None)

    pub fn create_intcounter(
        &self, name: &str, description: &str, labels: &[(&str, &str)],
    ) -> anyhow::Result<prometheus::IntCounter>
    // 整型计数器；与 Counter 语义相同，内部使用 u64 而非 f64

    pub fn create_gauge(
        &self, name: &str, description: &str, labels: &[(&str, &str)],
    ) -> anyhow::Result<prometheus::Gauge>
    // 可增减的浮点型仪表盘

    pub fn create_intgauge(
        &self, name: &str, description: &str, labels: &[(&str, &str)],
    ) -> anyhow::Result<prometheus::IntGauge>
    // 整型仪表盘；适用于任务数、连接数等整数值

    pub fn create_histogram(
        &self, name: &str, description: &str,
        labels: &[(&str, &str)], buckets: Option<Vec<f64>>,
    ) -> anyhow::Result<prometheus::Histogram>
    // 直方图；buckets 为 None 时使用 Prometheus 默认 bucket 边界

    // ── 向量指标创建（动态 label）─────────────────────────────────────────

    pub fn create_countervec(
        &self, name: &str, description: &str,
        const_labels: &[&str],               // 动态 label 的键名列表，如 &["method", "status"]
        const_label_values: &[(&str, &str)],  // 额外的 const-label（键值对）
    ) -> anyhow::Result<prometheus::CounterVec>
    // 等价于 create_metric::<CounterVec>(hierarchy, name, description,
    //   const_label_values, None, Some(const_labels))

    pub fn create_intcountervec(
        &self, name: &str, description: &str,
        const_labels: &[&str], const_label_values: &[(&str, &str)],
    ) -> anyhow::Result<prometheus::IntCounterVec>

    pub fn create_gaugevec(
        &self, name: &str, description: &str,
        const_labels: &[&str], const_label_values: &[(&str, &str)],
    ) -> anyhow::Result<prometheus::GaugeVec>

    pub fn create_intgaugevec(
        &self, name: &str, description: &str,
        const_labels: &[&str], const_label_values: &[(&str, &str)],
    ) -> anyhow::Result<prometheus::IntGaugeVec>

    // ── 导出 ──────────────────────────────────────────────────────────────

    pub fn prometheus_expfmt(&self) -> anyhow::Result<String>
    // 透传 hierarchy.get_metrics_registry().prometheus_expfmt_combined()
    // 在本层及所有子层采集、合并并序列化为 Prometheus 文本格式
}
```

Python 绑定（`lib/bindings/python/rust/lib.rs` 中的 `Metrics` 类）与此处方法列表保持镜像顺序，新增方法时需同步更新。

---

### 3.7 `prometheus_names` — 指标命名常量与校验工具

**来源**：`src/metrics/prometheus_names.rs`

**设计意图**：Prometheus 指标命名有严格规范（蛇形命名、单位后缀、`_total` 表 counter 等），且 Pagoda 的指标名在 Rust 运行时和 Python 组件（vLLM）两侧均需保持一致。将所有名称常量集中在一个文件，配合自动代码生成（`cargo run -p pagoda-codegen --bin gen-python-prometheus-names`），可以保证 Python 侧的 `prometheus_names.py` 始终与 Rust 侧同步，无需手动双维护。校验函数 `sanitize_prometheus_label` / `sanitize_prometheus_name` 在 `create_metric` 调用链上对用户输入做统一净化，将非法字符替换为 `_`，防止运行时创建出 Prometheus 拒绝接受的指标名。

```rust
pub mod name_prefix {
    pub const SERVICEGROUP:     &str = "pagoda_servicegroup";     // ServiceGroup 层级指标前缀
    pub const FRONTEND:         &str = "pagoda_frontend";         // 前端 HTTP 服务指标
    pub const REQUEST_PLANE:    &str = "pagoda_request_plane";    // 请求平面（传输无关延迟）
    pub const TRANSPORT:        &str = "pagoda_transport";        // 传输层（TCP/NATS 字节/错误）
    pub const WORK_HANDLER:     &str = "pagoda_work_handler";     // 后端侧传输分解
    pub const TOKIO:            &str = "pagoda_tokio";            // Tokio 运行时内部状态
    pub const ROUTER:           &str = "pagoda_router";           // KV 路由器实例指标
    pub const KVINDEXER:        &str = "pagoda_kvindexer";        // KV 索引器指标
    pub const ROUTING_OVERHEAD: &str = "pagoda_routing_overhead"; // 路由阶段开销
}

pub mod labels {
    pub const NAMESPACE:    &str = "pagoda_namespace";    // 由 create_metric 自动注入
    pub const SERVICEGROUP: &str = "pagoda_servicegroup"; // 由 create_metric 自动注入
    pub const PORTNAME:     &str = "pagoda_portname";     // 由 create_metric 自动注入
    // ... 其余标签（WORKER_DP_RANK、WORKER_PP_RANK 等）
    pub const DP_RANK: &str = "dp_rank";
    /// Label for worker instance ID (etcd lease ID).
    pub const WORKER_ID: &str = "worker_id";
    pub const MODEL: &str = "model";
    pub const MODEL_NAME: &str = "model_name";
    pub const WORKER_TYPE: &str = "worker_type";
    /// Label for router instance (discovery.instance_id() of the frontend)
    pub const ROUTER_ID: &str = "router_id";
}

// 指标名与辅助命名常量子模块（按功能分组）
pub mod servicegroup_names {
    pub const ROUTER: &str = "router";
    // TODO: PREFILL / DECODE
}

pub mod frontend_service {
    pub const METRICS_PREFIX_ENV: &str = "PGD_METRICS_PREFIX";
    pub const REQUESTS_TOTAL: &str = "requests_total";
    pub const REQUESTS_STARTED_TOTAL: &str = "requests_started_total";
    pub const QUEUED_REQUESTS: &str = "queued_requests";
    pub const INFLIGHT_REQUESTS: &str = "inflight_requests";
    pub const ACTIVE_REQUESTS: &str = "active_requests";
    pub const DISCONNECTED_CLIENTS: &str = "disconnected_clients";
    pub const REQUEST_DURATION_SECONDS: &str = "request_duration_seconds";
    pub const INPUT_SEQUENCE_TOKENS: &str = "input_sequence_tokens";
    pub const OUTPUT_SEQUENCE_TOKENS: &str = "output_sequence_tokens";
    pub const KV_HIT_RATE: &str = "kv_hit_rate";
    pub const KV_TRANSFER_ESTIMATED_LATENCY_SECONDS: &str = "kv_transfer_estimated_latency_seconds";
    pub const SHARED_CACHE_HIT_RATE: &str = "shared_cache_hit_rate";
    pub const SHARED_CACHE_BEYOND_BLOCKS: &str = "shared_cache_beyond_blocks";
    pub const CACHED_TOKENS: &str = "cached_tokens";
    pub const TOKENIZER_LATENCY_MS: &str = "tokenizer_latency_ms";
    pub const OUTPUT_TOKENS_TOTAL: &str = "output_tokens_total";
    pub const TIME_TO_FIRST_TOKEN_SECONDS: &str = "time_to_first_token_seconds";
    pub const INTER_TOKEN_LATENCY_SECONDS: &str = "inter_token_latency_seconds";
    pub const MODEL_TOTAL_KV_BLOCKS: &str = "model_total_kv_blocks";
    pub const MODEL_MAX_NUM_SEQS: &str = "model_max_num_seqs";
    pub const MODEL_MAX_NUM_BATCHED_TOKENS: &str = "model_max_num_batched_tokens";
    pub const MODEL_CONTEXT_LENGTH: &str = "model_context_length";
    pub const MODEL_KV_CACHE_BLOCK_SIZE: &str = "model_kv_cache_block_size";
    pub const MODEL_MIGRATION_LIMIT: &str = "model_migration_limit";
    pub const MODEL_MIGRATION_TOTAL: &str = "model_migration_total";
    pub const MODEL_MIGRATION_MAX_SEQ_LEN_EXCEEDED_TOTAL: &str = "model_migration_max_seq_len_exceeded_total";
    pub const MODEL_CANCELLATION_TOTAL: &str = "model_cancellation_total";
    pub const MODEL_REJECTION_TOTAL: &str = "model_rejection_total";
    pub const WORKER_ACTIVE_DECODE_BLOCKS: &str = "worker_active_decode_blocks";
    pub const WORKER_ACTIVE_PREFILL_TOKENS: &str = "worker_active_prefill_tokens";
    pub const WORKER_LAST_TIME_TO_FIRST_TOKEN_SECONDS: &str = "worker_last_time_to_first_token_seconds";
    pub const WORKER_LAST_INPUT_SEQUENCE_TOKENS: &str = "worker_last_input_sequence_tokens";
    pub const WORKER_LAST_INTER_TOKEN_LATENCY_SECONDS: &str = "worker_last_inter_token_latency_seconds";
    pub const ROUTER_QUEUE_PENDING_REQUESTS: &str = "router_queue_pending_requests";
    pub const LORA_REPLICA_FACTOR: &str = "lora_replica_factor";
    pub const LORA_IS_ACTIVE: &str = "lora_is_active";
    pub const LORA_ESTIMATED_LOAD: &str = "lora_estimated_load";
    pub const LORA_RAW_ARRIVAL_COUNT: &str = "lora_raw_arrival_count";
    pub const LORA_ACTIVE_REQUESTS: &str = "lora_active_requests";
    pub const MIGRATION_TYPE_LABEL: &str = "migration_type";
    pub const OPERATION_LABEL: &str = "operation";
    pub mod operation {
        pub const TOKENIZE: &str = "tokenize";
        pub const DETOKENIZE: &str = "detokenize";
    }
    pub mod migration_type {
        pub const NEW_REQUEST: &str = "new_request";
        pub const ONGOING_REQUEST: &str = "ongoing_request";
    }
    pub mod status {
        pub const SUCCESS: &str = "success";
        pub const ERROR: &str = "error";
    }
    pub mod request_type {
        pub const STREAM: &str = "stream";
        pub const UNARY: &str = "unary";
    }
    pub mod error_type {
        pub const NONE: &str = "";
        pub const VALIDATION: &str = "validation";
        pub const NOT_FOUND: &str = "not_found";
        pub const OVERLOAD: &str = "overload";
        pub const CANCELLED: &str = "cancelled";
        pub const RESPONSE_TIMEOUT: &str = "response_timeout";
        pub const INTERNAL: &str = "internal";
        pub const NOT_IMPLEMENTED: &str = "not_implemented";
    }
}

pub mod work_handler {
    pub const REQUESTS_TOTAL: &str = "requests_total";
    pub const REQUEST_BYTES_TOTAL: &str = "request_bytes_total";
    pub const RESPONSE_BYTES_TOTAL: &str = "response_bytes_total";
    pub const INFLIGHT_REQUESTS: &str = "inflight_requests";
    pub const REQUEST_DURATION_SECONDS: &str = "request_duration_seconds";
    pub const ERRORS_TOTAL: &str = "errors_total";
    pub const CANCELLATION_TOTAL: &str = "cancellation_total";
    pub const NETWORK_TRANSIT_SECONDS: &str = "network_transit_seconds";
    pub const TIME_TO_FIRST_RESPONSE_SECONDS: &str = "time_to_first_response_seconds";
    pub const QUEUE_DEPTH: &str = "queue_depth";
    pub const QUEUE_CAPACITY: &str = "queue_capacity";
    pub const ENQUEUE_REJECTED_TOTAL: &str = "enqueue_rejected_total";
    pub const PERMIT_WAIT_SECONDS: &str = "permit_wait_seconds";
    pub const POOL_ACTIVE_TASKS: &str = "pool_active_tasks";
    pub const POOL_CAPACITY: &str = "pool_capacity";
    pub const ERROR_TYPE_LABEL: &str = "error_type";
    pub mod error_types {
        pub const DESERIALIZATION: &str = "deserialization";
        pub const INVALID_MESSAGE: &str = "invalid_message";
        pub const RESPONSE_STREAM: &str = "response_stream";
        pub const GENERATE: &str = "generate";
        pub const PUBLISH_RESPONSE: &str = "publish_response";
        pub const PUBLISH_FINAL: &str = "publish_final";
    }
}

pub mod task_tracker {
    pub const TASKS_ISSUED_TOTAL: &str = "tasks_issued_total";
    pub const TASKS_STARTED_TOTAL: &str = "tasks_started_total";
    pub const TASKS_SUCCESS_TOTAL: &str = "tasks_success_total";
    pub const TASKS_CANCELLED_TOTAL: &str = "tasks_cancelled_total";
    pub const TASKS_FAILED_TOTAL: &str = "tasks_failed_total";
    pub const TASKS_REJECTED_TOTAL: &str = "tasks_rejected_total";
}

pub mod distributed_runtime {
    pub const UPTIME_SECONDS: &str = "uptime_seconds";
}

pub mod kvbm {
    pub const OFFLOAD_BLOCKS_D2H: &str = "offload_blocks_d2h";
    pub const OFFLOAD_BLOCKS_H2D: &str = "offload_blocks_h2d";
    pub const OFFLOAD_BLOCKS_D2D: &str = "offload_blocks_d2d";
    pub const ONBOARD_BLOCKS_H2D: &str = "onboard_blocks_h2d";
    pub const ONBOARD_BLOCKS_D2D: &str = "onboard_blocks_d2d";
    pub const MATCHED_TOKENS: &str = "matched_tokens";
    pub const HOST_CACHE_HIT_RATE: &str = "host_cache_hit_rate";
    pub const DISK_CACHE_HIT_RATE: &str = "disk_cache_hit_rate";
    pub const OBJECT_CACHE_HIT_RATE: &str = "object_cache_hit_rate";
    pub const OFFLOAD_BLOCKS_D2O: &str = "offload_blocks_d2o";
    pub const ONBOARD_BLOCKS_O2D: &str = "onboard_blocks_o2d";
    pub const OFFLOAD_BYTES_OBJECT: &str = "offload_bytes_object";
    pub const ONBOARD_BYTES_OBJECT: &str = "onboard_bytes_object";
    pub const OBJECT_READ_FAILURES: &str = "object_read_failures";
    pub const OBJECT_WRITE_FAILURES: &str = "object_write_failures";
}

pub mod router_request {
    pub const METRIC_PREFIX: &str = "router_";
}

pub mod routing_overhead {
    pub const BLOCK_HASHING_MS: &str = "overhead_block_hashing_ms";
    pub const INDEXER_FIND_MATCHES_MS: &str = "overhead_indexer_find_matches_ms";
    pub const SEQ_HASHING_MS: &str = "overhead_seq_hashing_ms";
    pub const SCHEDULING_MS: &str = "overhead_scheduling_ms";
    pub const TOTAL_MS: &str = "overhead_total_ms";
    pub const SHARED_CACHE_QUERY_MS: &str = "overhead_shared_cache_query_ms";
    pub const SHARED_CACHE_ERRORS_TOTAL: &str = "shared_cache_errors_total";
}

pub mod router {
    pub const REQUESTS_TOTAL: &str = "router_requests_total";
    pub const REMOTE_INDEXER_QUERY_FAILURES_TOTAL: &str = "router_remote_indexer_query_failures_total";
    pub const REMOTE_INDEXER_WRITE_FAILURES_TOTAL: &str = "router_remote_indexer_write_failures_total";
    pub const TIME_TO_FIRST_TOKEN_SECONDS: &str = "router_time_to_first_token_seconds";
    pub const INTER_TOKEN_LATENCY_SECONDS: &str = "router_inter_token_latency_seconds";
    pub const INPUT_SEQUENCE_TOKENS: &str = "router_input_sequence_tokens";
    pub const OUTPUT_SEQUENCE_TOKENS: &str = "router_output_sequence_tokens";
    pub const KV_HIT_RATE: &str = "router_kv_hit_rate";
    pub const SHARED_CACHE_HIT_RATE: &str = "router_shared_cache_hit_rate";
    pub const SHARED_CACHE_BEYOND_BLOCKS: &str = "router_shared_cache_beyond_blocks";
}

pub mod frontend_perf {
    pub const STAGE_DURATION_SECONDS: &str = "stage_duration_seconds";
    pub const STAGE_REQUESTS: &str = "stage_requests";
    pub const STAGE_PREPROCESS: &str = "preprocess";
    pub const STAGE_ROUTE: &str = "route";
    pub const STAGE_DISPATCH: &str = "dispatch";
    pub const TOKENIZE_SECONDS: &str = "tokenize_seconds";
    pub const TEMPLATE_SECONDS: &str = "template_seconds";
    pub const DETOKENIZE_TOTAL_US: &str = "detokenize_total_us";
    pub const DETOKENIZE_TOKEN_COUNT: &str = "detokenize_token_count";
    pub const EVENT_LOOP_DELAY_SECONDS: &str = "event_loop_delay_seconds";
    pub const EVENT_LOOP_STALL_TOTAL: &str = "event_loop_stall_total";
}

pub mod tokio_perf {
    pub const WORKER_MEAN_POLL_TIME_NS: &str = "worker_mean_poll_time_ns";
    pub const GLOBAL_QUEUE_DEPTH: &str = "global_queue_depth";
    pub const BUDGET_FORCED_YIELD_TOTAL: &str = "budget_forced_yield_total";
    pub const WORKER_BUSY_RATIO: &str = "worker_busy_ratio";
    pub const WORKER_PARK_COUNT_TOTAL: &str = "worker_park_count_total";
    pub const WORKER_LOCAL_QUEUE_DEPTH: &str = "worker_local_queue_depth";
    pub const WORKER_STEAL_COUNT_TOTAL: &str = "worker_steal_count_total";
    pub const WORKER_OVERFLOW_COUNT_TOTAL: &str = "worker_overflow_count_total";
    pub const BLOCKING_THREADS: &str = "blocking_threads";
    pub const BLOCKING_IDLE_THREADS: &str = "blocking_idle_threads";
    pub const BLOCKING_QUEUE_DEPTH: &str = "blocking_queue_depth";
    pub const ALIVE_TASKS: &str = "alive_tasks";
}

pub mod kvindexer {
    pub const REQUEST_DURATION_SECONDS: &str = "request_duration_seconds";
    pub const REQUESTS_TOTAL: &str = "requests_total";
    pub const ERRORS_TOTAL: &str = "errors_total";
    pub const MODELS: &str = "models";
    pub const WORKERS: &str = "workers";
}

pub mod request_plane {
    pub const QUEUE_SECONDS: &str = "queue_seconds";
    pub const SEND_SECONDS: &str = "send_seconds";
    pub const ROUNDTRIP_TTFT_SECONDS: &str = "roundtrip_ttft_seconds";
    pub const INFLIGHT_REQUESTS: &str = "inflight_requests";
}

pub mod transport {
    pub mod tcp {
        pub const POOL_ACTIVE: &str = "tcp_pool_active";
        pub const POOL_IDLE: &str = "tcp_pool_idle";
        pub const BYTES_SENT_TOTAL: &str = "tcp_bytes_sent_total";
        pub const BYTES_RECEIVED_TOTAL: &str = "tcp_bytes_received_total";
        pub const ERRORS_TOTAL: &str = "tcp_errors_total";
        pub const SERVER_QUEUE_DEPTH: &str = "tcp_server_queue_depth";
    }
    pub mod nats {
        pub const ERRORS_TOTAL: &str = "nats_errors_total";
    }
}

pub mod kvrouter {
    pub const KV_CACHE_EVENTS_APPLIED: &str = "kv_cache_events_applied";
}

pub mod kv_publisher {
    pub const ENGINES_DROPPED_EVENTS_TOTAL: &str = "kv_publisher_engines_dropped_events_total";
    pub const ZMQ_EVENTS_TOTAL: &str = "kv_publisher_zmq_events_total";
    pub const ZMQ_FILTERED_EVENTS_TOTAL: &str = "kv_publisher_zmq_filtered_events_total";
    pub const ZMQ_CONVERSION_ISSUES_TOTAL: &str = "kv_publisher_zmq_conversion_issues_total";
    pub const ZMQ_SUSPICIOUS_EVENTS_TOTAL: &str = "kv_publisher_zmq_suspicious_events_total";
}

pub mod trtllm_additional {
    pub const NUM_ABORTED_REQUESTS_TOTAL: &str = "trtllm_num_aborted_requests_total";
    pub const REQUEST_TYPE_IMAGE_TOTAL: &str = "trtllm_request_type_image_total";
    pub const REQUEST_TYPE_STRUCTURED_OUTPUT_TOTAL: &str = "trtllm_request_type_structured_output_total";
    pub const KV_TRANSFER_SUCCESS_TOTAL: &str = "trtllm_kv_transfer_success_total";
    pub const KV_TRANSFER_LATENCY_SECONDS: &str = "trtllm_kv_transfer_latency_seconds";
    pub const KV_TRANSFER_BYTES: &str = "trtllm_kv_transfer_bytes";
    pub const KV_TRANSFER_SPEED_GB_S: &str = "trtllm_kv_transfer_speed_gb_s";
}

pub mod kvstats {
    pub const TOTAL_BLOCKS: &str = "total_blocks";
    pub const GPU_CACHE_USAGE_PERCENT: &str = "gpu_cache_usage_percent";
}

pub mod model_info {
    pub const LOAD_TIME_SECONDS: &str = "model_load_time_seconds";
}

pub fn sanitize_prometheus_label(s: &str) -> anyhow::Result<String>
// 将非 [a-zA-Z0-9_] 字符替换为 '_'，保证 label value 合法；空字符串返回 Err

pub fn sanitize_prometheus_name(s: &str) -> anyhow::Result<String>
// 与 sanitize_prometheus_label 类似，用于指标名中的动态部分

pub fn sanitize_frontend_prometheus_prefix(raw: &str) -> String
// 前端指标前缀专用净化函数；空字符串或净化失败时回退到 `name_prefix::FRONTEND`

pub fn build_servicegroup_metric_name(metric_name: &str) -> String
// 为 ServiceGroup 层级指标名添加 "pagoda_servicegroup_" 前缀

pub fn clamp_u64_to_i64(value: u64) -> i64 {
    if value > i64::MAX as u64 {
        i64::MAX
    } else {
        value as i64
    }
}
```

⚠️ 修改此文件中的常量后必须执行 `cargo run -p pagoda-codegen --bin gen-python-prometheus-names` 重新生成 `lib/bindings/python/src/pagoda/prometheus_names.py`，否则 Python 侧的指标名会与 Rust 侧不一致，导致 Grafana Dashboard 面板数据丢失。

---

### 3.8 `tokio_perf` — Tokio 运行时指标与事件循环金丝雀

**来源**：`src/metrics/tokio_perf.rs`

**设计意图**：Tokio 异步运行时的内部状态（worker poll 时间、任务队列深度、steal 次数）对诊断 Pagoda 推理服务的 tail latency 至关重要，但 Tokio 本身不暴露 Prometheus 指标。`tokio_perf` 通过 `tokio::runtime::Handle::current().metrics()` API 采集运行时内部 telemetry，每秒更新一次全局 Lazy 静态变量。

事件循环金丝雀（canary）独立于采集循环——每 10ms sleep 后测量实际睡眠时长，超出 10ms 部分即为事件循环延迟。延迟超过 5ms 时递增 `EVENT_LOOP_STALL_TOTAL` 计数器。该金丝雀提供了一个与 Tokio 内部实现无关的黑盒视角：无论延迟来自 GC 停顿、系统调用阻塞还是 CPU 饥饿，均能捕捉。

```rust
fn tokio_metric_name(suffix: &str) -> String {
    format!("{}_{}", name_prefix::TOKIO, suffix)
}
// --- 运行时级别指标（每 1s 采集）---
pub static TOKIO_GLOBAL_QUEUE_DEPTH: Lazy<Gauge>
// 全局任务队列深度；持续 > 0 说明所有 worker 满载

pub static TOKIO_BUDGET_FORCED_YIELD_TOTAL: Lazy<Counter>
// 任务因消耗预算被强制让步的累计次数；增速高说明存在长时间不 await 的任务

pub static TOKIO_BLOCKING_THREADS: Lazy<Gauge>
pub static TOKIO_BLOCKING_IDLE_THREADS: Lazy<Gauge>
pub static TOKIO_BLOCKING_QUEUE_DEPTH: Lazy<Gauge>
// 阻塞线程池状态；spawn_blocking 密集时可能排队

pub static TOKIO_ALIVE_TASKS: Lazy<Gauge>
// 当前存活任务数；异常增长可能表明 Future 泄漏

// --- 每 worker 指标（IntGaugeVec / IntCounterVec，label = "worker"）---
pub static TOKIO_WORKER_MEAN_POLL_TIME_NS: Lazy<IntGaugeVec>
// 平均单次 poll 耗时（纳秒）；大值说明存在耗时 Future 未及时 .await

pub static TOKIO_WORKER_BUSY_RATIO_VEC: Lazy<IntGaugeVec>
// 忙碌比（0–1000，千分比）；> 950 视为该 worker 饱和

pub static TOKIO_WORKER_LOCAL_QUEUE_DEPTH: Lazy<IntGaugeVec>
// 本地队列深度；持续高说明该 worker 任务积压

pub static TOKIO_WORKER_PARK_COUNT_TOTAL: Lazy<IntCounterVec>
pub static TOKIO_WORKER_STEAL_COUNT_TOTAL: Lazy<IntCounterVec>
pub static TOKIO_WORKER_OVERFLOW_COUNT_TOTAL: Lazy<IntCounterVec>
// 累计 park / steal / overflow 次数（单调递增，通过 delta 更新 Counter）

// --- 事件循环金丝雀（每 10ms）---
pub static EVENT_LOOP_DELAY_SECONDS: Lazy<Histogram>
// 超出 10ms sleep 目标的延迟量（秒）；bucket [0, 0.001, 0.005, 0.01, ...]

pub static EVENT_LOOP_STALL_TOTAL: Lazy<Counter>
// 延迟 > 5ms 的停滞次数累计
```

**主函数**：

```rust
pub async fn tokio_metrics_and_canary_loop(cancel: CancellationToken)
// 在目标 tokio runtime 上 spawn；每 10ms 检查金丝雀延迟，每 1s 调用 sample_tokio_metrics()
// 通过 tokio::select! 在 cancel 触发时优雅退出，不泄漏后台任务
```

**注册函数**（两条路径，均幂等）：

- `ensure_tokio_perf_metrics_registered(registry: &MetricsRegistry)` — Runtime 路径，通过 `OnceCell<()>` 保证全进程只执行一次
- `ensure_tokio_perf_metrics_registered_prometheus(registry: &prometheus::Registry)` — Raw 路径，使用独立的 `PROMETHEUS_REGISTERED: OnceCell<()>` 守卫，与 Runtime 路径相互独立（调用顺序不影响结果）

**增量采样辅助状态**：

```rust
static PREV_BUDGET_FORCED_YIELD: AtomicU64
// 保存上一轮 budget_forced_yield_count，用于把 Tokio 的累计值转换成 Counter 增量

struct PrevWorkerCounters {
    park: Vec<u64>,
    steal: Vec<u64>,
    overflow: Vec<u64>,
}

impl PrevWorkerCounters {
    fn new() -> Self
    fn ensure_capacity(&mut self, num_workers: usize)
}
// 维护每个 worker 的上一轮 park / steal / overflow 计数快照；
// 仅由 tokio_metrics_and_canary_loop 单任务持有，因此不需要加锁
```

`PrevWorkerCounters` 的作用不是缓存当前 Gauge，而是保留 Tokio 暴露的单调递增计数器上一轮采样值。`worker_park_count()`、`worker_steal_count()`、`worker_overflow_count()` 以及 `budget_forced_yield_count()` 都是累计值，直接写入 Prometheus `*_TOTAL` Counter 会造成重复累加；因此采样循环必须先记住上一轮快照，再用 `saturating_sub` 计算本轮增量，既避免重复统计，也能在 runtime 重建或计数回退时防止出现负值。

**采样函数**：

```rust
fn sample_tokio_metrics(prev: &mut PrevWorkerCounters)
// 每 1s 从 Handle::current().metrics() 读取 Tokio runtime 快照；
// 刷新 global_queue_depth、blocking_threads、blocking_idle_threads、blocking_queue_depth、alive_tasks 等运行时级 Gauge；
// 对 budget_forced_yield 与每个 worker 的 park / steal / overflow 计数采用“当前累计值 - 上一轮快照”的 delta 更新方式推进 *_TOTAL Counter；
// 按 worker 标签写入 mean_poll_time_ns、local_queue_depth 和 busy_ratio，其中 busy_ratio 以 mean_poll_time 作为忙碌度代理值并换算成 0-1000 千分比
```

这段采样逻辑把 Tokio telemetry 分成两类处理：一类是可以直接覆盖的瞬时状态，如全局队列深度、阻塞线程池规模、本地队列深度；另一类是只能追加的累计计数，必须经由 `PrevWorkerCounters` 做差分后再调用 `.inc_by(...)`。`ensure_capacity()` 则负责在 worker 数量变化时补齐快照数组，避免新 worker 首次采样时访问越界。


---

### 3.9 `frontend_perf` — 前端流水线阶段性能指标

**来源**：`src/metrics/frontend_perf.rs`

**设计意图**：LLM 推理请求在前端流水线中经历多个串行阶段（preprocess → route → transport_roundtrip → postprocess），每阶段的耗时对定位用户感知延迟来源（是 tokenize 慢、还是路由慢、还是模型慢）不可或缺。`frontend_perf` 将这些阶段时延和 token 处理量以全局静态指标的形式暴露，同时被 runtime 层（route / transport_roundtrip）和 llm 层（preprocess / postprocess）共同使用。使用 `HistogramVec`（而非多个独立 Histogram）记录阶段耗时，允许通过 `stage` label 值在 Grafana 中用单条 PromQL 获取所有阶段的分布，减少指标数量的同时增强可比较性。

```rust
fn frontend_metric_name(suffix: &str) -> String {
    format!("{}_{}", name_prefix::FRONTEND, suffix)
}
pub static STAGE_REQUESTS: Lazy<IntGaugeVec> 
pub static STAGE_DURATION_SECONDS: Lazy<HistogramVec>
// label: "stage"，值如 "preprocess" / "route" / "transport_roundtrip" / "postprocess"
// bucket: [0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0]

pub static TOKENIZE_SECONDS: Lazy<Histogram>
// tokenize 耗时；bucket 上界 1.0s（超出说明 tokenizer 异常慢）

pub static TEMPLATE_SECONDS: Lazy<Histogram>
// chat template 渲染耗时；bucket 上界 0.05s（通常亚毫秒）

pub static DETOKENIZE_TOTAL_US: Lazy<Counter>
// 去 token 化累计耗时（微秒）；与 DETOKENIZE_TOKEN_COUNT 配合计算 per-token 平均


static REGISTERED: OnceCell<()> = OnceCell::new();

static PROMETHEUS_REGISTERED: OnceCell<()> = OnceCell::new();
pub static DETOKENIZE_TOKEN_COUNT: Lazy<Counter>
// 去 token 化累计 token 数；rate(total_us) / rate(count) = 平均每 token 耗时

pub struct StageGuard {
    gauge: prometheus::IntGauge,
}

impl StageGuard {
    /// Increment the stage gauge and return a guard that decrements on drop.
    ///
    /// * `stage` — pipeline stage name; use `frontend_perf::STAGE_{PREPROCESS,ROUTE,DISPATCH}`
    ///   constants from [`crate::metrics::prometheus_names`].
    /// * `phase` — request phase; use [`RequestPhase::to_string`] output
    ///   (`"prefill"|"decode"|"aggregated"`), or `""` for stages without a phase.
    pub fn new(stage: &str, phase: &str) -> Self {
        let gauge = STAGE_REQUESTS.with_label_values(&[stage, phase]);
        gauge.inc();
        Self { gauge }
    }
}

impl Drop for StageGuard {
    fn drop(&mut self) {
        self.gauge.dec();
    }
}
```




**注册函数**（两条路径，均幂等）：`ensure_frontend_perf_metrics_registered` / `ensure_frontend_perf_metrics_registered_prometheus`。

---

### 3.10 `request_plane` — 请求平面生命周期指标

**来源**：`src/metrics/request_plane.rs`

**设计意图**：`AddressedPushRouter` 是 Pagoda 前端向后端发送推理请求的核心路径，延迟分解（序列化 + 编码 + 控制消息时间 vs 纯网络往返时间）是确定性能瓶颈的必要手段。`REQUEST_PLANE_QUEUE_SECONDS`（从 `generate()` 入口到 `send_request()`）、`REQUEST_PLANE_SEND_SECONDS`（`send_request()` 完成时间）和 `REQUEST_PLANE_ROUNDTRIP_TTFT_SECONDS`（`send_request()` 到首个响应）三段时间相加约等于前端视角的首 token 时间（TTFT），可以直接在 Prometheus 中通过 label 查询。`REQUEST_PLANE_INFLIGHT` 是 Gauge 而非 Counter，因为它表示瞬时状态（当前并发数）而非累计量。

```rust
fn request_plane_metric_name(suffix: &str) -> String {
    format!("{}_{}", name_prefix::REQUEST_PLANE, suffix)
}
pub static REQUEST_PLANE_QUEUE_SECONDS: Lazy<Histogram>
// generate() 入口 → send_request()：包含序列化、编码、控制消息
// bucket: [0.0001 ... 1.0]

pub static REQUEST_PLANE_SEND_SECONDS: Lazy<Histogram>
// send_request() 完成耗时（前端视角：网络 + 后端队列 + ack）
// bucket: [0.0001 ... 1.0]

pub static REQUEST_PLANE_ROUNDTRIP_TTFT_SECONDS: Lazy<Histogram>
// send_request() 完成 → 收到首个响应（含后端处理时间）
// bucket: [0.001, 0.005, 0.01, 0.025, ... 5.0]（比前两者更粗粒度，因为跨进程）
static METRICS_REGISTERED: OnceCell<()> = OnceCell::new();


static PROMETHEUS_REGISTERED: OnceCell<Result<(), String>> = OnceCell::new();

pub static REQUEST_PLANE_INFLIGHT: Lazy<Gauge>
// 当前在途请求数；在 generate() 入口 +1，响应流完成 -1
// 持续高值且无对应吞吐量增加 → 后端过载
```

**注册函数**：`ensure_request_plane_metrics_registered` / `ensure_request_plane_metrics_registered_prometheus`，均使用独立的 `OnceCell` 守卫。

---

### 3.11 `transport_metrics` — 传输层协议字节与错误指标

**来源**：`src/metrics/transport_metrics.rs`

**设计意图**：`request_plane` 指标关注"请求做了什么"（延迟、并发），而 `transport_metrics` 关注"协议层的线路状态"（字节量、错误计数）。两者分离是 Prometheus 最佳实践——前者是业务指标，后者是基础设施指标，在告警规则和 Dashboard 中职责不同。`NATS_ERRORS_TOTAL` 使用 `IntCounterVec` 而非 `Counter` 正是为了预留未来细化错误分类的空间。

```rust
fn transport_metric_name(suffix: &str) -> String {
    format!("{}_{}", name_prefix::TRANSPORT, suffix)
}
// --- TCP 计数器（无 label，直接 .inc_by(n) 或 .inc()）---
pub static TCP_BYTES_SENT_TOTAL: Lazy<Counter>
pub static TCP_BYTES_RECEIVED_TOTAL: Lazy<Counter>
pub static TCP_ERRORS_TOTAL: Lazy<Counter>

// --- NATS 计数器（带 error_type label）---
pub static NATS_ERRORS_TOTAL: Lazy<IntCounterVec>
// .with_label_values(&["request_failed"]).inc()
static PROMETHEUS_REGISTERED: OnceCell<Result<(), String>> = OnceCell::new();
```

`transport_metrics` **仅提供 Raw Prometheus 路径**（`ensure_transport_metrics_registered_prometheus`），没有 `MetricsRegistry` 路径。原因：传输层指标是全局的、与任何层级无关的状态，不需要挂载到具体的 ServiceGroup / PortName registry 树中，直接注册到 LLM HTTP 服务的 raw registry 即可。

---

### 3.12 `work_handler_perf` — 工作处理器传输分解指标

**来源**：`src/metrics/work_handler_perf.rs`

**设计意图**：`request_plane` 指标是**前端视角**的延迟，包含了网络传输时间但无法单独量化它。`work_handler_perf` 提供**后端视角**的补充：T1（send 完成时间戳，嵌入控制消息）、T2（`handle_payload` 入口）、T3（发出首个响应）。`WORK_HANDLER_NETWORK_TRANSIT_SECONDS`（T2-T1）是纯网络时间，`WORK_HANDLER_TIME_TO_FIRST_RESPONSE_SECONDS`（T3-T2）是后端首 token 处理时间。注意 T1 是前端时钟，T2 是后端时钟，两者存在时钟差，因此 `NETWORK_TRANSIT_SECONDS` 反映的是"壁钟差"而非严格的单向传播时间。

```rust
fn work_handler_metric_name(suffix: &str) -> String {
    format!("{}_{}", name_prefix::WORK_HANDLER, suffix)
}
pub static WORK_HANDLER_NETWORK_TRANSIT_SECONDS: Lazy<Histogram>
// 前端 send_request() 完成 → 后端 handle_payload() 入口（跨进程壁钟差）
// bucket: [0.0001 ... 1.0]（通常亚毫秒，局域网场景）

pub static WORK_HANDLER_TIME_TO_FIRST_RESPONSE_SECONDS: Lazy<Histogram>
// handle_payload() 入口 → 发出首个 prologue 响应（纯后端处理时间）
// bucket: [0.001, 0.005, 0.01, 0.025, ... 10.0]（模型推理时间跨度大）
static METRICS_REGISTERED: OnceCell<()> = OnceCell::new();

static PROMETHEUS_REGISTERED: OnceCell<Result<(), String>> = OnceCell::new();

```

**注册函数**（两条路径，均幂等）：`ensure_work_handler_perf_metrics_registered` / `ensure_work_handler_perf_metrics_registered_prometheus`，使用独立 `OnceCell` 守卫，两路径互不影响。

---

### 3.13 `work_handler_pool` — 工作队列与 Worker Pool 饱和指标

**来源**：`src/metrics/work_handler_perf.rs`

**设计意图**：`work_handler_perf` 前半部分解决的是“消息到达后端后，网络传输和首响应生成分别花了多久”；但后端吞吐是否稳定，还取决于 dispatcher 前面的有界 `mpsc` 队列是否积压、dispatcher 拿到任务后是否长期卡在 permit 获取上，以及 worker pool 当前是否已逼近并发上限。`work_handler_pool` 这一组指标正是为此补上的容量治理视角：它把“排队”“等 permit”“permit 已占满”三个阶段拆开观测，便于区分是纯流量高峰、线程池饱和，还是 dispatcher 已经退出。

```rust
fn work_handler_metric_name(suffix: &str) -> String {
    format!("{}_{}", name_prefix::WORK_HANDLER, suffix)
}

pub static WORK_HANDLER_QUEUE_DEPTH: Lazy<IntGauge>
// bounded mpsc work queue 中等待 dispatcher 领取的当前项目数；
// 成功 `work_tx.send()` 后 +1，`work_rx.recv()` 取走后立即 -1；
// 不包含 permit 获取等待，因此与 `WORK_HANDLER_PERMIT_WAIT_SECONDS` 互补

pub static WORK_HANDLER_QUEUE_CAPACITY: Lazy<IntGauge>
// work queue 配置容量；服务初始化时写入一次，作为 `QUEUE_DEPTH` 的对照基线

pub static WORK_HANDLER_ENQUEUE_REJECTED_TOTAL: Lazy<IntCounter>
// `work_tx.send().await` 失败累计次数；
// 对 tokio bounded mpsc 而言，这表示接收端 dispatcher 已关闭，
// 而不是“队列已满”（满队列会表现为背压等待，而非返回错误）

pub static WORK_HANDLER_PERMIT_WAIT_SECONDS: Lazy<Histogram>
// dispatcher 取到任务后等待 worker-pool permit 的耗时；
// p99 拉长通常说明并发额度耗尽而非网络问题；
// bucket: [0.0001, 0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0]

pub static WORK_HANDLER_POOL_ACTIVE_TASKS: Lazy<IntGauge>
// 当前已占用 permit 的活跃任务数；接近容量上限说明 worker pool 接近饱和

pub static WORK_HANDLER_POOL_CAPACITY: Lazy<IntGauge>
// worker pool permit 总量；服务初始化时写入一次，作为 `ACTIVE_TASKS` 的容量参照
```

这组指标把工作处理链路切成两个相邻但不同的瓶颈面：`WORK_HANDLER_QUEUE_DEPTH` 反映 dispatcher 之前的积压，`WORK_HANDLER_PERMIT_WAIT_SECONDS` 和 `WORK_HANDLER_POOL_ACTIVE_TASKS` 反映 dispatcher 之后的执行资源竞争。两者一起看时，可以快速判断“请求卡在进队列前”还是“请求已经出队、但拿不到执行配额”。`WORK_HANDLER_ENQUEUE_REJECTED_TOTAL` 则应被视为通道生命周期异常信号，而不是单纯的容量告警，因为 tokio 的有界 `mpsc` 在队列满时不会直接报错。

**注册函数**：

```rust
static METRICS_REGISTERED: OnceCell<()>
// `MetricsRegistry` 路径的幂等守卫

pub fn ensure_work_handler_pool_metrics_registered(registry: &MetricsRegistry)
// 将 queue_depth、queue_capacity、enqueue_rejected_total、permit_wait_seconds、
// pool_active_tasks、pool_capacity 注册到给定 registry；
// 内部统一使用 `add_metric_or_warn`，重复调用时只告警不报错，保持“尽力注册”语义
```

当前源码中这组指标只暴露 `ensure_work_handler_pool_metrics_registered(registry: &MetricsRegistry)` 这一条 Runtime 注册路径，说明它们主要面向 Runtime System Status Server 汇总导出，而不是像 `transport_metrics` 那样走独立的 Raw Prometheus 注册表。



---

## 四、设计决策与演进方向

### D-01：`MetricsRegistry` 树形子注册表（取代父层扇出）

历史版本的 `create_metric` 在注册指标时会把 collector 同时注册到当前层级及所有父层级（PortName → ServiceGroup → Namespace → DRT）。这使 DRT 层的 `/metrics` 包含全部指标，但带来了致命问题：当两个不同的 portname 注册同名指标时（如 `pagoda_servicegroup_requests_total`），同一 `prometheus::Registry` 中出现了相同指标名但 const-label 集合不同的两个描述符。Prometheus 在 `register()` 时检测到 descriptor mismatch，拒绝第二次注册，导致第二个 portname 的指标静默丢失。

当前方案（每层独立 registry + `child_registries` 树）将指标隔离在各自 registry 中，`prometheus_expfmt_combined()` 在合并时按"族名 + label 集合"去重，允许不同 portname 有相同指标名但不同 label 值，完全解决了 descriptor mismatch 问题。代价是合并时需要 O(N) 遍历所有子注册表，但实际部署中层级树不超过 4 层，开销可忽略。

### D-02：Update Callback vs Exposition Callback 的职责分离

两类回调服务于不同场景：

- **`update_callback`**（`execute_update_callbacks`）：在 `/metrics` 请求到来时同步刷新指标值，用于"外部状态"——如 Gauge 类型的 uptime、KV cache 利用率等需要在采集时点更新的值。回调返回 `Result<()>`，失败只打印日志，不中断采集，保证 `/metrics` 端点的高可用性。
- **`expfmt_callback`**（`execute_expfmt_callbacks`）：返回完整的 Prometheus 文本格式字符串，追加在 Prometheus encoder 输出之后。用于将 Python vLLM 进程的指标文本（通过 IPC 或 HTTP 获取）注入 Pagoda 的 `/metrics` 响应，无需将 Python 指标反序列化成 Rust 对象再序列化，降低跨语言集成复杂度。

两类回调均通过 `Arc<RwLock<Vec<...>>>` 存储，同一回调注册一次即可在所有 `MetricsRegistry` clone 上可见，解决了跨层级调用（如 PortName 上注册的回调在 DRT 层 expfmt 时被执行）的问题。

### D-03：`once_cell::sync::Lazy` + 双重 `OnceCell` 幂等守卫

每个预定义指标子模块中有两个 `OnceCell` 守卫（`REGISTERED` 和 `PROMETHEUS_REGISTERED`），分别保护 `MetricsRegistry` 路径和 Raw Prometheus 路径的注册幂等性。两个守卫相互独立，确保以任意顺序调用两条路径时均只注册一次，调用顺序不影响结果。`Lazy` 本身保证静态指标变量在首次访问时创建，此后所有访问复用同一实例，永不重复构造。

### D-04：`TypeId` 分派替代 trait 方法多态

`create_metric` 用 `std::any::TypeId::of::<T>()` 判断具体指标类型，分派到不同的构造路径。这是因为 `Histogram` 需要 `HistogramOpts`（而非 `Opts`），`*Vec` 类型需要额外的 `label_names` 参数，无法用统一的单一方法签名覆盖。`TypeId` 分派在编译期无法检验（运行时 if/else），但实现简单，且指标类型集合固定（Prometheus 库不会频繁增加类型），维护成本可控。替代方案（为每类指标提供独立的 `create_*` 函数）更类型安全，已通过 `Metrics<H>` 的具名方法实现，`create_metric` 自由函数作为底层统一路径保留用于 Python 绑定。

### D-05：`prometheus_names.rs` 双语言同步与代码生成

Pagoda 的 Python 组件（vLLM、前端 HTTP 服务）和 Rust 运行时共享同一组 Prometheus 指标名。如果各自维护字符串常量，名称漂移（如 Python 侧错误地把 `_total` 去掉）会导致 Grafana Dashboard 悄然失效。`prometheus_names.rs` 是**单一事实来源**（single source of truth），通过代码生成器同步到 Python，确保双侧始终一致。

演进方向：若 Python 侧指标进一步丰富，可以将代码生成纳入 CI 检查（校验生成的 Python 文件是否与 Rust 源码一致），防止修改 Rust 常量后忘记重新生成。

---

## 五、模块依赖

**`metrics` 使用**：

```
once_cell           — Lazy / OnceCell（替代 lazy_static!，避免宏依赖）
prometheus          — Registry / Counter / Gauge / Histogram / *Vec / TextEncoder / Encoder
parking_lot         — Mutex（metrics.rs 中的辅助结构）
regex               — Lazy<Regex>（sanitize_prometheus_label 中的非法字符过滤）
tokio               — runtime::Handle::current().metrics()（tokio_perf 采集 API）
tokio_util          — CancellationToken（tokio_metrics_and_canary_loop 优雅退出）
anyhow              — anyhow::Result / anyhow::anyhow!（统一错误类型）
```

**`metrics` 被使用**：

```
distributed.rs                — DistributedRuntime 实现 MetricsHierarchy；创建根 MetricsRegistry
servicegroup/namespace.rs     — Namespace 实现 MetricsHierarchy；调用 add_child_registry 挂载到 DRT
servicegroup/servicegroup.rs  — ServiceGroup 实现 MetricsHierarchy；挂载到 Namespace
servicegroup/portname.rs      — PortName 实现 MetricsHierarchy；挂载到 ServiceGroup
system_status_server.rs  — 调用 MetricsRegistry::prometheus_expfmt_combined() 暴露 /metrics HTTP 端点
pipeline/addressed_push_router.rs — 使用 REQUEST_PLANE_* 静态指标记录请求生命周期
pipeline/work_handler.rs — 使用 WORK_HANDLER_* 静态指标记录后端传输分解
pipeline/tcp/client.rs   — 使用 TCP_BYTES_* / TCP_ERRORS_TOTAL 记录传输层状态
pipeline/nats/client.rs  — 使用 NATS_ERRORS_TOTAL 记录 NATS 错误
llm/frontend.rs          — 使用 STAGE_DURATION_SECONDS / TOKENIZE_SECONDS / DETOKENIZE_* 记录前端阶段
llm/http_service.rs      — 调用 ensure_*_registered_prometheus() 注册指标到 LLM 服务自有 registry
bindings/python/rust/lib.rs — 将 Metrics<H> 暴露为 Python 类型（create_counter / create_gauge 等）
```
