# `logging` 模块设计文档

**源码位置**：`lib/runtime/src/logging.rs`（约 1913 行，单文件大型模块）

---

## 一、设计背景与模块职责

Pagoda 的推理链路跨越多个进程和机器：HTTP 前端 → NATS/TCP 传输层 → Worker 进程 → 推理引擎。一个用户请求产生的日志分散在不同进程的日志流中，若不进行关联，故障排查需要手动比对时间戳，效率极低。

`logging` 模块的核心目标是**让整条链路上的所有日志都携带同一个 `trace_id`**，使运维人员通过 `grep trace_id=xxx` 就能找到某个请求从接收到完成的全部日志，无需在多个进程间逐条比对。

为实现这个目标，模块需要解决三个子问题：

1. **Trace ID 的生成与传播**：前端收到 HTTP 请求时从 `traceparent` 头部提取 Trace ID（或由 OpenTelemetry 自动生成），通过 NATS/TCP 消息头传播到 Worker，Worker 在处理请求时将 Trace ID 附加到所有日志中；
2. **日志格式的可配置性**：生产环境需要 JSONL 格式（机器可解析，便于日志聚合系统导入），开发环境需要 READABLE 格式（人类可直读，带颜色高亮）；
3. **OpenTelemetry 集成**：企业用户使用 Jaeger/Zipkin/Tempo 等分布式追踪系统时，需要 Pagoda 以标准 W3C Trace Context 格式输出 Span 数据并导出到 OTLP 端点。

---

## 二、配置体系

### 配置加载顺序（优先级由低到高）

```
默认值（LoggingConfig::default()）
    ↓
/opt/pagoda/etc/logging.toml（系统全局配置文件）
    ↓
$PGD_LOGGING_CONFIG_PATH 指向的 TOML 文件（用户自定义配置）
    ↓
环境变量（PGD_LOG、PGD_LOGGING_JSONL 等，最高优先级）
```

使用 `figment` crate 实现多层次配置合并。`figment` 的设计是每层配置覆盖低优先级层的同名字段，未设置的字段保持低优先级层的值——这使得用户可以只覆盖关心的字段，其他字段使用系统默认值。

### `LoggingConfig` 结构体

```rust
#[derive(Serialize, Deserialize, Debug)]
struct LoggingConfig {
    log_level: String,
    log_filters: HashMap<String, String>,
}
```

`**log_level**`：全局默认日志级别，默认 `"info"`。对应 tracing 的 `LevelFilter`，可以设置为 `"trace"`、`"debug"`、`"info"`、`"warn"`、`"error"`。

`**log_filters**`：模块/crate 级别的精细过滤规则，格式 `{"module_name": "level"}`。

默认过滤规则将若干底层库的日志级别设为 `"error"`：

```rust
HashMap::from([
    ("h2".to_string(), "error".to_string()),
    ("tower".to_string(), "error".to_string()),
    ("hyper_util".to_string(), "error".to_string()),
    ("async_nats".to_string(), "error".to_string()),
    ("rustls".to_string(), "error".to_string()),
    ("tokenizers".to_string(), "error".to_string()),
    ("opentelemetry".to_string(), "error".to_string()),
    // ...
])
```

**为什么默认静默这些库**：这些库在正常运行时会产生大量 debug/info 级别的输出（HTTP/2 帧、TLS 握手、NATS 心跳等），对 Pagoda 业务逻辑的调试没有帮助，但会淹没真正有用的日志。设为 `"error"` 使它们只在出错时才输出，大幅减少日志噪音。开发者若需要调试特定库可以通过 `PGD_LOG=async_nats=debug` 临时覆盖。

### 相关辅助函数：`otlp_exporter_enabled()` / `get_service_name()` / `load_config()`

这三个函数虽然都很短，但它们共同构成了 `setup_logging()` 的配置前置层：

```rust
fn otlp_exporter_enabled() -> bool
fn get_service_name() -> String
fn load_config() -> LoggingConfig
```

- `otlp_exporter_enabled()`：统一解析 `OTEL_EXPORT_ENABLED` 一类布尔环境变量，决定后续是否创建 OTLP traces/logs exporter；
- `get_service_name()`：读取 OTEL service name 环境变量，若未设置则退回默认服务名；
- `load_config()`：使用 `Figment` 按“默认值 → 系统 TOML → 用户 TOML”顺序合并配置，得到 `LoggingConfig`。

其中 `load_config()` 并不解析环境变量 `PGD_LOG` 本身；环境变量覆盖是在后续 `filters(config)` 里通过 `EnvFilter::from_env_lossy()` 完成的。

---

## 三、`init()` 与 `setup_logging()`：日志系统初始化

### 为什么使用 `Once`

```rust
static INIT: Once = Once::new();

pub fn init() {
    INIT.call_once(|| {
        if let Err(e) = setup_logging() {
            eprintln!("Failed to initialize logging: {}", e);
            std::process::exit(1);
        }
    });
}
```

`tracing` 的 subscriber 只能全局初始化一次（`SubscriberInitExt::init()` 若重复调用会 panic）。`Once` 保证无论多少代码路径调用 `logging::init()`，实际初始化只发生一次。失败时 `std::process::exit(1)` 而非 panic，原因：日志系统初始化失败说明环境配置严重错误（OTLP 端点无法连接等），继续运行没有意义，且 panic 的错误信息可能因日志系统未就绪而丢失。`eprintln!` 确保错误信息到达 stderr。

### `setup_logging()` 的分支逻辑

```
PGD_LOGGING_JSONL=1
├── 是：构建 CustomJsonFormatter（JSONL 输出）
│       ├── OTEL_EXPORT_ENABLED=true
│       │   ├── 创建 OTLP Span 导出器（gRPC/Tonic）
│       │   ├── 创建 OTLP Log 导出器
│       │   └── 注册 opentelemetry layer + otel_logs 桥接 layer
│       └── OTEL_EXPORT_ENABLED=false
│           └── 仅本地生成 Trace ID，不导出
│
└── 否：构建 fmt::layer（READABLE 输出，带 ANSI 颜色）
        └── 注册 tracing_subscriber::registry().with(l).init()
```

**为什么 OTLP 导出只在 JSONL 模式下可用**：OTLP 导出面向生产环境的日志聚合系统（Grafana Loki、Elastic 等），这些系统消费 JSONL 格式的日志。在开发模式（READABLE 格式）下不需要 OTLP 导出，且 OTLP 连接的建立会增加进程启动时间，影响开发迭代效率。

**Layer 注册顺序**（JSONL + OTLP 模式）：

```rust
tracing_subscriber::registry()
    .with(tracing_opentelemetry::layer().with_tracer(tracer)...)  // OTEL 跟踪层
    .with(otel_logs_layer)                                         // OTEL 日志桥接
    .with(DistributedTraceIdLayer...)                              // 分布式 Trace ID 注入
    .with(fmt_layer)                                               // 日志格式化输出
    .init();
```

层的执行顺序是从外到内（最后注册的最先执行）。`DistributedTraceIdLayer` 在 `fmt_layer` 之前，确保格式化输出时 `DistributedTraceContext` 已经注入到 Span 扩展中，`CustomJsonFormatter` 可以读取并包含在 JSON 输出中。

### `filters(config) -> EnvFilter`

```rust
fn filters(config: LoggingConfig) -> EnvFilter
```

这个函数负责把 `LoggingConfig` 变成 tracing 真正可执行的过滤器：

1. 用 `config.log_level` 创建默认 directive；
2. 再读取环境变量 `PGD_LOG`，允许运行时覆盖；
3. 遍历 `config.log_filters`，逐项拼成 `module=level` directive 并加入 `EnvFilter`；
4. 若开启 `span_events_enabled()`，额外强制放行 `span_event=trace`，否则 `SPAN_FIRST_ENTRY` 事件可能被过滤掉。

失败路径主要是单条 directive 解析失败；源码对这种情况不会中止初始化，而是 `eprintln!` 提示该模块过滤规则无效并跳过该项。

### `log_message(...)` — Python/外部包装桥接

```rust
pub fn log_message(level: &str, message: &str, module: &str, file: &str, line: u32)
```

这个函数的职责不是参与 tracing span 管线，而是给 Python 包装层或其它非 Rust 原生日志调用者提供一个统一入口：

- 把字符串级别（`debug/info/warn/error/warning`）映射成 `log::Level`；
- 构造带 `target/file/line` 元信息的 `log::Record`；
- 交给当前全局 logger 输出。

因此它更像“跨语言日志适配层”，而不是 `logging` 模块主 trace 传播机制的一部分。

---

## 四、`DistributedTraceContext`：分布式追踪上下文

### 为什么需要自定义上下文而非直接用 OpenTelemetry

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedTraceContext {
    pub trace_id: String,
    pub span_id: String,
    pub parent_id: Option<String>,
    pub tracestate: Option<String>,
    #[serde(skip)]
    start: Option<Instant>,
    #[serde(skip)]
    end: Option<Instant>,
    pub x_request_id: Option<String>,
    pub x_pagoda_request_id: Option<String>,
}
```

OpenTelemetry 的 `SpanContext` 在 span 关闭前存储在 `OtelData` 扩展中，但 OTEL 的 Trace ID 格式是 `TraceId` 类型，需要 `.to_hex()` 转换才能用于日志格式化，且每次日志事件都需要此转换，开销较大。

`DistributedTraceContext` 在 span 首次进入（`on_enter`）时将 Trace ID 和 Span ID 转换为已格式化的 `String`，之后日志格式化直接读取字符串，无需重复转换。

**Pagoda 专属字段**：

- `x_request_id`：来自 HTTP 请求头 `X-Request-ID`，通常由 API 网关或客户端生成，用于端到端请求追踪（跨越 Pagoda 系统边界）；
- `x_pagoda_request_id`：Pagoda 内部生成的 UUID，仅在 Pagoda 系统内部流转，用于区分来自不同系统的请求；
- `start` / `end`（`#[serde(skip)]`）：记录 span 的开始和结束时间，用于计算 duration。不序列化，因为 `Instant` 跨进程无意义。

`**create_traceparent() -> String`**：

```rust
pub fn create_traceparent(&self) -> String {
    format!("00-{}-{}-01", self.trace_id, self.span_id)
}
```

生成 W3C Trace Context 规范的 `traceparent` 头部值，格式为 `{version}-{trace_id}-{span_id}-{flags}`。`"00"` 是当前版本，`"01"` 表示 sampled（采样）。此方法用于在 NATS/TCP 消息头中注入 trace 上下文，传播到 Worker 进程。

---

## 五、`DistributedTraceIdLayer`：自定义 tracing-subscriber 层

### 为什么需要自定义 Layer

OpenTelemetry 的 `tracing_opentelemetry::layer()` 会将 OTEL Span 数据注入到 `OtelData` 扩展，但这个数据：

1. **在 `on_new_span` 时不可用**：OTEL Trace ID 在 span 首次 **进入**（`on_enter`）时才确定（因为要等 OTEL Sampler 决定是否采样），`on_new_span` 时还没有；
2. **不携带 Pagoda 专属字段**：`x_request_id`、`x_pagoda_request_id` 是 Pagoda 特有的，OpenTelemetry 不知道它们；
3. **格式不适合 JSON 日志直接读取**：需要从 `OtelData` 提取并转换为已格式化的字符串。

`DistributedTraceIdLayer` 实现 `Layer<S> for DistributedTraceIdLayer` 来自定义 span 的生命周期处理：

### `FieldVisitor` — span 字段抓取器

```rust
#[derive(Debug, Default)]
pub struct FieldVisitor {
    pub fields: HashMap<String, String>,
}
```

`FieldVisitor` 是 `on_new_span()` 的辅助访问器，实现 `Visit` trait，用来把 tracing span attributes 收集成 `HashMap<String, String>`。

它的作用非常具体：

- 读取 `trace_id` / `span_id` / `parent_id` / `tracestate` / `x_request_id` / `x_dynamo_request_id` 这类初始字段；
- 让 `DistributedTraceIdLayer` 可以在真正进入 span 前，先提取并暂存“候选上下文”。

如果没有它，`on_new_span()` 就需要直接手写一套属性解析逻辑，可读性会明显变差。

### `on_new_span`：收集 Span 属性

```rust
fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &Id, ctx: Context<'_, S>)
```

此钩子在 span 创建时触发（`tracing::info_span!(...)` 调用时），此时 OTEL Trace ID **尚未确定**。`on_new_span` 只做一件事：从 span 的字段（如 `trace_id = "..."`, `x_request_id = "..."` 等 tracing field）提取 Pagoda 专属信息，存储到 `PendingDistributedTraceContext` 扩展中，等待 `on_enter` 完成初始化。

**父 span 上下文继承**：

```rust
if parent_id.is_none()
    && let Some(parent_span_id) = ctx.current_span().id()
    && let Some(parent_span) = ctx.span(parent_span_id)
{
    let parent_ext = parent_span.extensions();
    if let Some(parent_tracing_context) = parent_ext.get::<DistributedTraceContext>() {
        trace_id = Some(parent_tracing_context.trace_id.clone());
        parent_id = Some(parent_tracing_context.span_id.clone());
        tracestate = parent_tracing_context.tracestate.clone();
    }
}
```

若当前 span 没有显式指定 `trace_id`，从父 span 的 `DistributedTraceContext` 继承——这是 trace 传播的核心机制。父 span 已经确定了 `trace_id`（在 `on_enter` 时），子 span 继承它，使同一个请求的所有 span 都有相同的 `trace_id`，无论嵌套多深。

**一致性校验**：

```rust
if (parent_id.is_some() || span_id.is_some()) && trace_id.is_none() {
    tracing::error!("parent id or span id are set but trace id is not set!");
    parent_id = None;
    span_id = None;
}
```

若 span 有 `parent_id` 或 `span_id` 但没有 `trace_id`，说明 span 创建代码有误（trace ID 是必须的）。清空不一致的字段，并记录错误日志，防止产生格式不合法的 JSON 输出。

### `on_enter`：完成 `DistributedTraceContext` 初始化

```rust
fn on_enter(&self, id: &Id, ctx: Context<'_, S>)
```

span 首次被 `.await` 进入时触发，此时 OTEL 的 `OtelData` 已经包含有效的 Trace ID 和 Span ID（Sampler 已做决定）。

**两阶段设计（pending → final）的原因**：

- `on_new_span` 时 OTEL Trace ID 未确定（Sampler 未运行）→ 只能收集 Pagoda 字段，存为 `PendingDistributedTraceContext`；
- `on_enter` 时 OTEL Trace ID 已确定（`OtelData` 已就绪）→ 将 Pending 中的 Pagoda 字段与 OTEL 的 Trace/Span ID 合并，构造最终的 `DistributedTraceContext`。

**Panic on 未设置的 Trace ID**：

```rust
if trace_id.is_none() {
    panic!("trace_id is not set in on_enter - OtelData may not be properly initialized");
}
```

若 `on_enter` 时 Trace ID 仍然为空，说明 OpenTelemetry 层没有正确初始化（层注册顺序错误，或 OTEL 库版本不兼容）。Panic 比输出格式不完整的日志（缺少 trace_id 的 JSON 行会通不过 JSON Schema 验证）更好——立即暴露问题，而非产生难以追查的数据质量问题。

`**span_events_enabled()` 的 `SPAN_FIRST_ENTRY` 事件**：

```rust
if span_events_enabled() {
    emit_at_level!(span_level, target: "span_event", message = "SPAN_FIRST_ENTRY");
}
```

当 `PGD_SPAN_EVENTS=1` 时，每个 span 首次进入时发出一个"span 开始"事件。此事件由 `CustomJsonFormatter` 格式化为带 `span_name`、`trace_id`、`span_id` 的 JSON 行，使 span 的开始时间点在日志流中可见（默认 `tracing_subscriber` 只在 span 关闭时输出）。`emit_at_level!` 宏是必要的，因为 `tracing::event!` 要求编译期常量 level，使用 match 在运行时选择正确的 level 常量。

### `on_close`：记录 span 结束时间

```rust
fn on_close(&self, id: Id, ctx: Context<'_, S>) {
    if let Some(distributed_tracing_context) = extensions.get_mut::<DistributedTraceContext>() {
        distributed_tracing_context.end = Some(Instant::now());
    }
}
```

目前仅记录结束时间（注释说"Currently not used but added for future use in timing"），未来用于计算 span duration 并输出到 JSON 日志中。

---

## 六、Trace Context 传播工具

### `TraceParent` 结构体与 `GenericHeaders` trait

```rust
pub struct TraceParent {
    pub trace_id: Option<String>,
    pub parent_id: Option<String>,
    pub tracestate: Option<String>,
    pub x_request_id: Option<String>,
    pub x_pagoda_request_id: Option<String>,
}

pub trait GenericHeaders {
    fn get(&self, key: &str) -> Option<&str>;
}
```

**为什么需要 `GenericHeaders` trait**：Pagoda 使用两种消息头类型：

- HTTP 请求：`http::HeaderMap`（axum/hyper）；
- NATS 消息：`async_nats::HeaderMap`。

两者都有 `.get(key)` 方法但签名不同，不能用统一的函数处理。`GenericHeaders` trait 为两者提供统一的 `get(key) -> Option<&str>` 接口，使 `TraceParent::from_headers` 可以统一处理任意类型的消息头，无需重复实现。

### `parse_traceparent(traceparent) -> (Option<String>, Option<String>)`

```rust
pub fn parse_traceparent(traceparent: &str) -> (Option<String>, Option<String>) {
    let pieces: Vec<_> = traceparent.split('-').collect();
    if pieces.len() != 4 { return (None, None); }
    let trace_id = pieces[1];
    let parent_id = pieces[2];
    if !is_valid_trace_id(trace_id) || !is_valid_span_id(parent_id) {
        return (None, None);
    }
    (Some(trace_id.to_string()), Some(parent_id.to_string()))
}
```

严格解析 W3C Trace Context `traceparent` 头部。解析失败时返回 `(None, None)` 而非 `Err`，原因：上游系统可能传来格式不规范的 `traceparent`，不应因此中断请求处理，只是无法关联到上游 trace，生成一个新的 Trace ID 继续即可。`is_valid_trace_id`（32 位 hex）和 `is_valid_span_id`（16 位 hex）提供额外的格式验证，防止格式合法但内容无效的 trace ID（如全零 ID）污染日志。

### Inject/Extract 函数族

**从各种消息头中提取 OTEL 上下文**：

```rust
pub fn extract_otel_context_from_nats_headers(headers: &async_nats::HeaderMap)
    -> (Option<opentelemetry::Context>, Option<String>, Option<String>)

fn extract_otel_context_from_http_headers(headers: &http::HeaderMap)
    -> Option<opentelemetry::Context>

fn extract_otel_context_from_tcp_headers(headers: &HashMap<String, String>)
    -> (Option<opentelemetry::Context>, Option<String>, Option<String>)
```

三个函数结构相同，差异仅在于消息头类型（NATS、HTTP、TCP HashMap）。**为什么不共用**：三种消息头的 Extractor 实现不同（`Extractor trait` 的 `get()` 方法需要适配各自的 API），Rust 的零成本抽象需要在泛型或 trait 对象层面处理，但三者的出现场景各自独立，代码复用带来的复杂度不值得。

每个 extract 函数：

1. 检查是否有 `traceparent` 头（无则直接返回 `None`）；
2. 实现本地的 `Extractor`（实现 `opentelemetry::propagation::Extractor`）；
3. 调用 `TRACE_PROPAGATOR.extract(&extractor)` 得到 `opentelemetry::Context`；
4. 验证 Context 中的 `span_context().is_valid()`（防止空的 Context 被用于 span 的父级设置）。

**注入 OTEL 上下文到消息头**：

```rust
pub fn inject_otel_context_into_nats_headers(headers: &mut HeaderMap, context: Option<Context>)
pub fn inject_current_trace_into_nats_headers(headers: &mut HeaderMap)
pub fn inject_trace_headers_into_map(headers: &mut HashMap<String, String>)
```

当前 span 的 trace context 写入消息头，使 NATS/TCP 消息接收方可以在相同的 trace 下创建子 span。

`inject_trace_headers_into_map` 同时注入 `traceparent`、`tracestate`、`x-request-id`、`x-pagoda-request-id`，使 TCP 消息完整携带所有追踪信息。

`**TRACE_PROPAGATOR` 静态实例**：

```rust
static TRACE_PROPAGATOR: Lazy<opentelemetry_sdk::propagation::TraceContextPropagator> =
    Lazy::new(opentelemetry_sdk::propagation::TraceContextPropagator::new);
```

W3C Trace Context propagator 是无状态的（只有 parse/format 逻辑），复用单个实例避免每次传播时重新构造。`once_cell::sync::Lazy` 确保首次使用时才初始化（懒初始化），避免在 `setup_logging()` 之前访问 OTEL 全局状态。

---

## 七、`get_distributed_tracing_context()`：获取当前 Span 的追踪上下文

```rust
pub fn get_distributed_tracing_context() -> Option<DistributedTraceContext> {
    Span::current()
        .with_subscriber(|(id, subscriber)| {
            subscriber
                .downcast_ref::<Registry>()
                .and_then(|registry| registry.span_data(id))
                .and_then(|span_data| {
                    let extensions = span_data.extensions();
                    extensions.get::<DistributedTraceContext>().cloned()
                })
        })
        .flatten()
}
```

从当前 tracing Span 的扩展中提取 `DistributedTraceContext`。

**为什么需要这个函数**：当代码在某个 span 的 async 上下文中运行，需要将当前的 trace context 注入到发出的消息头（如向 NATS 发消息时）。Span 的上下文存储在 tracing 的 thread-local 状态中，通过 `Span::current().with_subscriber(...)` 访问。若当前没有 span（在 span 外调用）返回 `None`，调用方不注入追踪头（不传播 trace context）。

---

## 八、`CustomJsonFormatter`：JSONL 输出格式化器

### 为什么需要自定义 formatter

`tracing_subscriber` 的内置 JSON formatter（`fmt::Layer::json()`）输出的字段名和结构不符合 Pagoda 的日志格式规范（例如 Pagoda 要求 `trace_id` 是顶层字段，但 OTEL 的 span 数据在内置 formatter 中是嵌套的）。自定义 formatter 完全控制 JSON 结构。

### `JsonLog` 结构体

```rust
#[derive(Serialize)]
struct JsonLog<'a> {
    time: String,      // ISO 8601 时间戳（UTC 或本地时区）
    level: String,     // ERROR/WARN/INFO/DEBUG/TRACE
    file: Option<&'a str>,   // 源文件路径
    line: Option<u32>,       // 行号
    target: String,    // tracing target（通常是模块路径）
    message: serde_json::Value,
    #[serde(flatten)]
    fields: BTreeMap<String, serde_json::Value>,  // 所有其他字段（trace_id 等）
}
```

`**#[serde(flatten)] fields**`：将所有动态字段（来自 span 的 `trace_id`、`span_id`、`x_request_id` 等）直接展开到 JSON 顶层，而非嵌套在 `"fields": {...}` 下。日志聚合系统（Elasticsearch、Loki）能直接在顶层查询 `trace_id=xxx`，无需嵌套路径。

`**BTreeMap` 而非 `HashMap**`：`BTreeMap` 按键名字母排序，保证同一种日志事件的 JSON 输出字段顺序固定。日志可读性更好（字段位置可预期），diff 比较时（如调试期间比对两条日志）不会因字段顺序不同而产生虚假差异。

### `TimeFormatter` / `CustomJsonFormatter` / `JsonVisitor`

这三者共同组成 JSONL 输出的格式化流水线：

```rust
struct TimeFormatter { use_local_tz: bool }
struct CustomJsonFormatter { time_formatter: TimeFormatter }
struct JsonVisitor { fields: BTreeMap<String, serde_json::Value> }
```

- `TimeFormatter`：负责输出当前时间字符串，并根据配置决定使用本地时区还是 UTC；
- `CustomJsonFormatter`：实现 `FormatEvent`，把 event + span 扩展 + timing 字段装配成 `JsonLog`；
- `JsonVisitor`：遍历 event 上的动态字段，把 `str/bool/i64/u64/f64/debug` 等不同类型统一写进 `BTreeMap<String, Value>`。

`JsonVisitor` 的一个关键细节是：

- 对普通字符串字段，会尝试 `serde_json::from_str::<Value>(value)`，如果字符串本身就是合法 JSON，就直接保留为 JSON 值；
- 只有解析失败时才退回普通字符串。

这样可以让调用方通过日志字段直接写入结构化 JSON，而不是所有值都被硬编码成字符串。

### `parse_tracing_duration(s) -> Option<u64>`

```rust
fn parse_tracing_duration(s: &str) -> Option<u64>
```

这个函数把 `FmtSpan::CLOSE` 产生的持续时间字符串（如 `12.3µs`、`45.6ms`、`1.2s`）统一解析成**微秒整数**。支持单位：`ns`、`us/µs`、`ms`、`s`。

它是 `time.busy` / `time.idle` 数值化的核心步骤；若没有这一层，日志里就只能保留字符串形式的耗时，后续查询和聚合能力会差很多。

### 时间格式化（`TimeFormatter`）

```rust
struct TimeFormatter {
    use_local_tz: bool,
}
```

**为什么支持本地时区**：容器化部署（Docker、Kubernetes）中进程的时区通常是 UTC，日志时间戳为 UTC 便于全球分布式团队统一分析。但本地开发时，UTC 时间不直观（需要换算）。`PGD_LOG_USE_LOCAL_TZ=1` 使开发者可以看到本地时间，无需心算时区换算。

时间格式精确到微秒（`%S%.6f`），LLM 推理的某些关键阶段（KV cache 命中判断、token 调度）在毫秒量级，微秒精度便于分析这些细节。

### Span 字段提取与 Duration 计算

`format_event` 中，通过 `ctx.lookup_current()` 或 `event.parent()` 找到当前事件所属的 span，从 span 的 `FormattedFields` 扩展中读取所有已格式化的字段：

```rust
let span_fields: Vec<(&str, &str)> = data.fields.split(' ')
    .filter_map(|entry| entry.split_once('='))
    .collect();
for (name, value) in span_fields {
    visitor.fields.insert(name.to_string(), ...);
}
```

`**time.busy` / `time.idle` → 微秒数值的转换**：`tracing_subscriber` 的 `FmtSpan::CLOSE` 在 span 关闭时输出 `time.busy = "12.3µs"` 和 `time.idle = "45.6ms"` 字符串。`parse_tracing_duration` 将这些字符串解析为微秒整数（`time.busy_us`、`time.idle_us`、`time.duration_us`），使日志聚合系统可以对 duration 做数值查询（如 `time.duration_us > 1000000` 查找超过 1 秒的请求），而非字符串比较。

---

## 九、Span 辅助函数

### 创建 Span 的工厂函数族

`**make_request_span(req)`**：为 axum HTTP 请求创建顶层 span，提取 `traceparent`、`x-request-id` 等头部，并通过 `span.set_parent(otel_context)` 与上游 trace 关联：

```rust
pub fn make_request_span<B>(req: &Request<B>) -> Span
```

用于 `tower_http::trace::TraceLayer` 的 `make_span_with` 配置，每个进入 axum 的 HTTP 请求自动创建带 trace 上下文的 span。

`**make_handle_payload_span(headers, servicegroup, portname, namespace, instance_id)**`：为 NATS 消息处理创建 span，从 NATS 消息头提取 trace context：

```rust
pub fn make_handle_payload_span(
    headers: &async_nats::HeaderMap,
    servicegroup: &str,
    portname: &str,
    namespace: &str,
    instance_id: u64,
) -> Span
```

Worker 收到 NATS 消息时调用此函数，创建的 span 自动成为上游（Router）span 的子 span，实现跨进程的 trace 链路连接。

`**make_handle_payload_span_from_tcp_headers**`：TCP 请求处理的等价版本，消息头从 `HashMap<String, String>` 中读取（TCP 传输层将头部序列化为字符串 Map）。

`**make_client_request_span(operation, request_id, trace_context, instance_id)**`：客户端发起请求时创建 span，用于追踪"Router 向某个 Worker 发送请求"这一操作的延迟：

```rust
pub fn make_client_request_span(
    operation: &str,
    request_id: &str,
    trace_context: Option<&DistributedTraceContext>,
    instance_id: Option<&str>,
) -> Span
```

这个函数的关键点是：当调用方显式传入 `trace_context` 时，它会先构造临时 headers，再复用 `extract_otel_context_from_nats_headers()` 把上下文恢复成 OTEL parent context，随后 `span.set_parent(context)`。因此它不是“手工拼字段的假父子关系”，而是真正参与 OTEL trace 树链接。

---

## 十、验证体系

### JSON Schema 验证（测试工具）

```rust
static LOG_LINE_SCHEMA: &str = r#"
{
    "required": ["file", "level", "line", "message", "target", "time"],
    "properties": {
        "level":     {"enum": ["ERROR", "WARN", "INFO", "DEBUG", "TRACE"]},
        "span_id":   {"pattern": "^[a-f0-9]{16}$"},
        "trace_id":  {"pattern": "^[a-f0-9]{32}$"},
        ...
    }
}
"#;
```

**为什么在测试中引入 JSON Schema 验证**：JSONL 格式的日志是对外承诺的接口（日志聚合系统依赖固定的字段格式），若某次代码修改改变了字段名称或格式，必须在测试阶段发现，而非等到生产环境日志聚合 pipeline 报错。Schema 验证使日志格式契约显式化，每条日志行都必须满足 schema 约束。

### `test_json_log_capture` 集成测试

通过 `stdio_override::StderrOverride` 重定向 stderr 到临时文件，调用真实的日志输出函数，读取输出文件并验证：

1. 所有日志行通过 JSON Schema 验证；
2. 同一 trace 内所有日志的 `trace_id` 相同；
3. `get_distributed_tracing_context()` 返回的 `trace_id` 与日志中的 `trace_id` 一致；
4. Span ID 格式合法（16 位 hex）；
5. 时间戳有效的 ISO 8601 格式。

`**#[tracing::instrument]` 测试函数**：`parent()` → `child()` → `grandchild()` 形成三层嵌套，验证 trace context 能正确地沿调用链继承，而非每层都生成新的 `trace_id`。

### `load_log(file_name) -> Result<Vec<Value>>`

```rust
pub fn load_log(file_name: &str) -> Result<Vec<serde_json::Value>>
```

这是测试模块里的辅助函数，用于：

1. 读取捕获到的 stderr 日志文件；
2. 逐行做 JSON 反序列化；
3. 用 `LOG_LINE_SCHEMA` 做 Schema 校验；
4. 返回结构化日志行数组供测试断言。

它本质上是“测试里的日志回读器 + Schema 守门人”。

### `test_span_events` / `test_span_events_subprocess`

源码里除了 `test_json_log_capture`，还有一组更细的 span 事件测试：

```rust
#[test]
fn test_span_events()

#[tokio::test]
async fn test_span_events_subprocess() -> Result<()>
```

它们验证的不是普通 JSONL 日志，而是 `PGD_LOGGING_SPAN_EVENTS=1` 时的 span 事件行为：

- `SPAN_FIRST_ENTRY` 是否正确生成；
- `SPAN_CLOSED` 是否带有 timing 字段；
- `trace_id` / `span_id` 格式是否正确；
- 目标过滤（target-based filtering）和级别过滤（level-based filtering）是否按配置工作。

之所以拆成“父测试 + 子进程测试”两层，是因为 logging 初始化被 `Once` 全局保护；若和其它测试共进程执行，很容易因为 logger 已初始化而失去对格式/过滤器的控制。子进程模式可以确保在受控环境变量下重新初始化 logging。

### 完备性结论

`logging.md` 在主链路设计上已经比较完整，但在这次补充前，存在几类“源码有、文档弱覆盖”的缺口：

- 配置辅助函数：`otlp_exporter_enabled`、`get_service_name`、`load_config`、`filters`
- 跨语言桥接：`log_message`
- 事件/字段访问器：`FieldVisitor`、`JsonVisitor`
- JSON 格式化辅助：`TimeFormatter`、`parse_tracing_duration`
- 测试辅助与测试矩阵：`load_log`、`test_span_events`、`test_span_events_subprocess`

补完这些后，这份文档才更接近“可据此还原实现”的粒度，而不只是体系结构说明。