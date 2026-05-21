# `config` 模块设计

**源码**：`src/config.rs` · `src/config/environment_names.rs`

---

## 一、模块定位

`config` 模块负责 Pagoda 运行时的**全量配置管理**，使用 `figment` 框架实现多层配置合并：默认值 → TOML 文件 → 环境变量（优先级递增）。覆盖 Tokio 线程参数、系统状态服务器、计算线程池、健康检查等所有运行时可调参数。

核心特性：

- **多层合并**：`Default` → `/opt/pagoda/defaults/runtime.toml` → `/opt/pagoda/etc/runtime.toml` → `PGD_*` 环境变量
- **空值过滤**：空环境变量被忽略，不会覆盖有效默认值
- **验证**：基于 `validator` crate 的声明式校验（如 `num_worker_threads >= 1`）
- **Builder 模式**：通过 `derive_builder` 提供可选的编程式配置

---

## 二、文件结构与可见性

```
src/config.rs                    — RuntimeConfig / WorkerConfig / 工具函数
src/config/environment_names.rs  — 所有环境变量名常量集中定义
```

---

## 三、类型详解

---

### 3.1 `RuntimeConfig` — 运行时配置

```rust
#[derive(Serialize, Deserialize, Validate, Debug, Builder, Clone)]
pub struct RuntimeConfig {
    // === Tokio 线程配置 ===
    pub num_worker_threads:     Option<usize>,  // PGD_RUNTIME_NUM_WORKER_THREADS, 默认 num_cores
    pub max_blocking_threads:   usize,          // PGD_RUNTIME_MAX_BLOCKING_THREADS, 默认 512(builder)/num_cores(default)

    // === 系统状态服务器 ===
    pub system_host:            String,         // PGD_SYSTEM_HOST, 默认 "0.0.0.0"
    pub system_port:            i16,            // PGD_SYSTEM_PORT, 默认 -1 (禁用)
    pub system_enabled:         bool,           // PGD_SYSTEM_ENABLED (DEPRECATED)
    pub starting_health_status: HealthStatus,   // PGD_SYSTEM_STARTING_HEALTH_STATUS, 默认 NotReady
    pub use_portname_health_status: Vec<String>, // PGD_SYSTEM_USE_PORTNAME_HEALTH_STATUS (DEPRECATED)
    pub system_health_path:     String,         // PGD_SYSTEM_HEALTH_PATH, 默认 "/health"
    pub system_live_path:       String,         // PGD_SYSTEM_LIVE_PATH, 默认 "/live"

    // === 计算线程池 ===
    pub compute_threads:        Option<usize>,  // PGD_COMPUTE_THREADS, 默认 None → cpu/2
    pub compute_stack_size:     Option<usize>,  // PGD_COMPUTE_STACK_SIZE, 默认 2MB
    pub compute_thread_prefix:  String,         // PGD_COMPUTE_THREAD_PREFIX, 默认 "compute"

    // === 健康检查 ===
    pub health_check_enabled:              bool, // PGD_HEALTH_CHECK_ENABLED, 默认 false
    pub canary_wait_time_secs:             u64,  // PGD_CANARY_WAIT_TIME, 默认 10
    pub health_check_request_timeout_secs: u64,  // PGD_HEALTH_CHECK_REQUEST_TIMEOUT, 默认 3
}
```

**实现的 trait**：

| Trait | 来源 | 说明 |
|-------|------|------|
| `Serialize` / `Deserialize` | serde | 序列化/反序列化 |
| `Validate` | validator | 声明式校验 |
| `Debug` / `Clone` | derive | 标准 |
| `Builder` | derive_builder | 生成 `RuntimeConfigBuilder` |
| `Display` | 手写 | 人类可读的配置摘要 |
| `Default` | 手写 | `num_worker_threads = num_cores, compute_threads = None` 等 |

**`Display` 实现**：

```rust
impl fmt::Display for RuntimeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // If None, it defaults to "number of cores", so we indicate that.
        match self.num_worker_threads {
            Some(val) => write!(f, "num_worker_threads={val}, ")?,
            None => write!(f, "num_worker_threads=default (num_cores), ")?,
        }

        write!(f, "max_blocking_threads={}, ", self.max_blocking_threads)?;
        write!(f, "system_host={}, ", self.system_host)?;
        write!(f, "system_port={}, ", self.system_port)?;
        write!(
            f,
            "use_portname_health_status={:?}",
            self.use_portname_health_status
        )?;
        write!(
            f,
            "starting_health_status={:?}",
            self.starting_health_status
        )?;
        write!(f, ", system_health_path={}", self.system_health_path)?;
        write!(f, ", system_live_path={}", self.system_live_path)?;
        write!(f, ", health_check_enabled={}", self.health_check_enabled)?;
        write!(f, ", canary_wait_time_secs={}", self.canary_wait_time_secs)?;
        write!(
            f,
            ", health_check_request_timeout_secs={}",
            self.health_check_request_timeout_secs
        )?;

        Ok(())
    }
}
```

这个实现把 `RuntimeConfig` 格式化成一行人类可读摘要，适合启动日志和调试输出。特殊点在于 `num_worker_threads` 为 `None` 时不会打印 `None`，而是明确写成 `default (num_cores)`，直接表达“未显式配置，将回退到按 CPU 核心数推导”的真实语义。

**构造方法**：

```rust
impl RuntimeConfig {
    pub fn builder() -> RuntimeConfigBuilder
    // Builder 模式构造

    pub fn from_settings() -> Result<RuntimeConfig>
    // figment 多层合并 → validate → Ok(config)

    pub fn single_threaded() -> Self
    // 测试用：1 worker, 1 blocking, 1 compute, 禁用系统服务器
}
```

**`single_threaded()` 的实际效果**：这个辅助构造并不只是把 Tokio worker 线程数设为 `1`。它会同时将 `max_blocking_threads` 设为 `1`、`compute_threads` 设为 `Some(1)`、系统状态服务器保持默认禁用端口 `-1`、起始健康状态设为 `NotReady`、`system_health_path` / `system_live_path` 回退到默认值，并关闭主动健康检查。它的目标是构造一个最小、确定、适合测试的运行时配置，而不是通用生产配置。

**核心方法**：

```rust
impl RuntimeConfig {
    pub fn system_server_enabled(&self) -> bool
    // system_port >= 0

    pub(crate) fn create_runtime(&self) -> io::Result<tokio::runtime::Runtime>
    // Builder::new_multi_thread()
    //   .worker_threads(num_worker_threads 或 available_parallelism)
    //   .max_blocking_threads(max_blocking_threads)
    //   .enable_all()
    //   .enable_metrics_poll_time_histogram()  // 仅当 PGD_ENABLE_POLL_HISTOGRAM=true
    //   .build()

    pub(crate) fn figment() -> Figment
    // 配置合并链（见下方详解）
}
```

**`create_runtime()` 的额外行为**：除了按配置设置 worker 线程数和 blocking 线程上限，它还会在 `PGD_ENABLE_POLL_HISTOGRAM` 为 truthy 值时打印一条 info 日志，并调用 Tokio 的 `enable_metrics_poll_time_histogram()` 开启 poll-time 统计。这个开关默认关闭，因为每次 task `poll()` 都会多一次时间采样开销。

---

### 3.2 `RuntimeConfigBuilder`

由 `derive_builder` 自动生成。

```rust
impl RuntimeConfigBuilder {
    pub fn build(&self) -> Result<RuntimeConfig>
    // build_internal() → validate() → Ok(config)
}
```

---

### 3.3 `WorkerConfig` — Worker 配置

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    pub graceful_shutdown_timeout: u64,  // debug=1, release=30
}
```

**构造**：

```rust
impl WorkerConfig {
    pub fn from_settings() -> Self
    // Figment::new().merge(defaults).merge(Env::prefixed("PGD_WORKER_")).extract()
}
```

**默认值实现**：

```rust
impl Default for WorkerConfig {
    fn default() -> Self {
        WorkerConfig {
            graceful_shutdown_timeout: if cfg!(debug_assertions) {
                1 // Debug build: 1 second
            } else {
                30 // Release build: 30 seconds
            },
        }
    }
}
```

`WorkerConfig::from_settings()` 会先通过 `Serialized::defaults(Self::default())` 注入默认值，再由 `PGD_WORKER_*` 环境变量覆盖，所以这个 `Default` 实现定义的就是 Worker 生命周期配置的基线。

**为什么区分 debug / release**：调试构建强调快速迭代，收到退出信号后只等待 1 秒，避免开发时频繁重启被优雅退出拖慢；发布构建则默认等待 30 秒，给在途请求和清理逻辑足够完成时间，降低强制退出导致请求中断的概率。

---

### 3.4 `HealthStatus` 枚举

```rust
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    Ready,     // "ready"
    NotReady,  // "notready"
}
```

---

### 3.5 Figment 配置合并链

```
优先级（高 → 低）：
  1. PGD_RUNTIME_*     环境变量（NUM_WORKER_THREADS, MAX_BLOCKING_THREADS）
  2. PGD_SYSTEM_*      环境变量（HOST, PORT, HEALTH_PATH, LIVE_PATH, ...）
  3. PGD_COMPUTE_*     环境变量（THREADS, STACK_SIZE, THREAD_PREFIX）
  4. PGD_HEALTH_CHECK_* 环境变量（ENABLED, REQUEST_TIMEOUT）
  5. PGD_CANARY_*      环境变量（WAIT_TIME）
    6. /opt/pagoda/etc/runtime.toml    — 用户自定义覆盖
    7. /opt/pagoda/defaults/runtime.toml — 发行版默认值
  8. RuntimeConfig::default()        — 代码默认值
```

每个 `Env::prefixed()` 都带 `filter_map`：

- 过滤空环境变量（`!v.is_empty()`）
- 映射环境变量名到字段名（如 `PGD_SYSTEM_HOST` → `system_host`，`PGD_COMPUTE_THREADS` → `compute_threads`）

源码里的实际重映射是按前缀分组显式处理的，不是简单地把后缀直接小写后写入字段：

- `PGD_SYSTEM_HOST` → `system_host`
- `PGD_SYSTEM_PORT` → `system_port`
- `PGD_SYSTEM_ENABLED` → `system_enabled`
- `PGD_SYSTEM_USE_PORTNAME_HEALTH_STATUS` → `use_portname_health_status`
- `PGD_SYSTEM_STARTING_HEALTH_STATUS` → `starting_health_status`
- `PGD_SYSTEM_HEALTH_PATH` → `system_health_path`
- `PGD_SYSTEM_LIVE_PATH` → `system_live_path`
- `PGD_COMPUTE_THREADS` → `compute_threads`
- `PGD_COMPUTE_STACK_SIZE` → `compute_stack_size`
- `PGD_COMPUTE_THREAD_PREFIX` → `compute_thread_prefix`
- `PGD_HEALTH_CHECK_ENABLED` → `health_check_enabled`
- `PGD_HEALTH_CHECK_REQUEST_TIMEOUT` → `health_check_request_timeout_secs`
- `PGD_CANARY_WAIT_TIME` → `canary_wait_time_secs`

**`from_settings()` 的额外行为**：真正执行 `extract()` 和 `validate()` 之前，代码还会显式检查已废弃环境变量是否存在，并为 `PGD_SYSTEM_USE_PORTNAME_HEALTH_STATUS` 和 `PGD_SYSTEM_ENABLED` 打印兼容性 warning。这一步不参与字段赋值，只用于迁移期提示。

---

### 3.6 工具函数

```rust
pub fn is_truthy(val: &str) -> bool         // "1" | "true" | "on" | "yes"
pub fn is_falsey(val: &str) -> bool         // "0" | "false" | "off" | "no"
pub fn parse_bool(val: &str) -> Result<bool>
pub fn env_is_truthy(env: &str) -> bool     // 读环境变量 + is_truthy
pub fn env_is_falsey(env: &str) -> bool

pub fn jsonl_logging_enabled() -> bool      // PGD_LOGGING_JSONL
pub fn disable_ansi_logging() -> bool       // PGD_SDK_DISABLE_ANSI_LOGGING
pub fn use_local_timezone() -> bool         // PGD_LOG_USE_LOCAL_TZ
pub fn span_events_enabled() -> bool        // PGD_LOGGING_SPAN_EVENTS
```

**行为语义补充**：

- `parse_bool()` 复用 `is_truthy()` / `is_falsey()` 的判定集合；若输入既不属于 truthy，也不属于 falsey，就返回 `Err`，错误消息中会列出合法写法（`true/false`、`1/0`、`on/off`、`yes/no`）。
- `env_is_truthy()` 和 `env_is_falsey()` 都是“读环境变量再判定”的薄包装；当环境变量不存在时，两者都返回 `false`，不会报错。
- `jsonl_logging_enabled()`、`disable_ansi_logging()`、`use_local_timezone()`、`span_events_enabled()` 本质上都只是对特定环境变量调用一次 `env_is_truthy()`，没有额外缓存或复杂逻辑；每次调用都会重新读取当前进程环境。

---

### 3.7 `environment_names` 子模块

**这个模块和 figment 没有直接关系。**

figment 的工作方式是"前缀扫描 + 映射"：`Env::prefixed("PGD_RUNTIME_")` 会扫描所有以该前缀开头的环境变量，再通过 `filter_map` 的映射表把它们转换成 `RuntimeConfig` 的字段名写进去。这个过程是自动的，figment 从不需要知道某个具体变量叫什么名字。

`environment_names` 解决的是另一个问题：**在 figment 之外，代码里需要手动读取环境变量的地方。** 

如果这些地方都写字面字符串，同一个变量名就会散落在多个文件的多处；常量把所有引用收拢到一个 `const`，改一处同步全部，且拼写错误在编译期就会报错。

按命名空间分组如下：

#### `runtime` — Tokio 主线程池

| 环境变量 | 作用 |
|----------|------|
| `PGD_RUNTIME_NUM_WORKER_THREADS` | Tokio 异步工作线程数。不设则自动取 CPU 核心数；设为 `1` 可切换到单线程模式（通常用于调试或资源受限容器）。 |
| `PGD_RUNTIME_MAX_BLOCKING_THREADS` | Tokio 阻塞线程池上限，默认与核心数相同。文件 I/O、同步 FFI 等 `spawn_blocking` 任务均消耗该池，过小会导致任务排队超时。 |
| `PGD_ENABLE_POLL_HISTOGRAM` | 设为 truthy 值后，Tokio 会在内部记录每个任务每次 `poll()` 的耗时分布（通过 `enable_metrics_poll_time_histogram()`），可用 `TOKIO_WORKER_MEAN_POLL_TIME_NS` 等指标读取。开启后每次 poll 多一次 `Instant::now()`，约 2× 时钟开销，生产环境按需开启。 |

#### `runtime::system` — 系统状态 HTTP 服务器

| 环境变量 | 作用 |
|----------|------|
| `PGD_SYSTEM_HOST` | HTTP 服务器监听地址，默认 `0.0.0.0`（所有网卡）。容器内通常保持默认；若需限制只监听本地，设为 `127.0.0.1`。 |
| `PGD_SYSTEM_PORT` | 端口号，控制服务器的启用与端口：`-1`（默认）完全禁用，`0` 绑定随机可用端口（测试时用），正整数绑定到指定端口（如 `8081`）。 |
| `PGD_SYSTEM_ENABLED` | **已废弃**。旧版本用布尔值控制服务器启用，现已被 `PGD_SYSTEM_PORT` 取代，设置此变量会打印兼容性警告。 |
| `PGD_SYSTEM_STARTING_HEALTH_STATUS` | 进程启动时的初始健康状态，接受 `ready` 或 `notready`（默认）。绝大多数服务应保持 `notready`，等模型/端点完全就绪后再通过 API 切换为 ready，避免流量过早进入。 |
| `PGD_SYSTEM_USE_PORTNAME_HEALTH_STATUS` | **已废弃**。旧机制：指定一批端点名，系统健康为这些端点健康状态的逻辑 AND。现已改为由端点主动注册健康检查 payload，不再需要在此声明端点列表。设置此变量会打印兼容性警告。 |
| `PGD_SYSTEM_HEALTH_PATH` | 健康检查 HTTP 路径，默认 `/health`。K8s `readinessProbe` 应指向此路径，返回 200 表示服务就绪。 |
| `PGD_SYSTEM_LIVE_PATH` | 存活检查 HTTP 路径，默认 `/live`。K8s `livenessProbe` 应指向此路径，仅检查进程是否存活（不依赖后端是否就绪）。 |

#### `worker` — Worker 进程生命周期

| 环境变量 | 作用 |
|----------|------|
| `PGD_WORKER_GRACEFUL_SHUTDOWN_TIMEOUT` | 收到 SIGINT/SIGTERM 后等待任务自然结束的最长秒数。Debug 构建默认 1 秒（快速迭代），Release 构建默认 30 秒（给在途请求足够时间完成）。超时后强制退出，退出码为 911。 |

#### `nats` / `etcd` — 外部服务连接

| 环境变量 | 作用 |
|----------|------|
| `NATS_SERVER` | NATS 服务器地址，如 `nats://localhost:4222`。用于服务发现、请求平面和事件总线。 |
| `ETCD_PORTNAMES` | etcd 端点列表（逗号分隔），如 `http://localhost:2379`。用于 KV 存储和分布式锁；若不设则退化到内存/文件后端。 |

#### `logging` — 日志格式与行为

| 环境变量 | 作用 |
|----------|------|
| `PGD_LOGGING_JSONL` | 设为 truthy 后输出 JSON Lines 格式日志，便于 Fluentd/Loki 等采集系统解析。默认关闭（人类可读格式）。 |
| `PGD_SDK_DISABLE_ANSI_LOGGING` | 设为 truthy 后禁用 ANSI 颜色转义码。在不支持颜色的 CI 环境或日志采集器中应开启，避免日志中出现乱码控制字符。 |
| `PGD_LOG_USE_LOCAL_TZ` | 设为 truthy 后日志时间戳使用本地时区，默认 UTC。分布式系统建议保持 UTC 以便跨节点对比日志。 |
| `PGD_LOGGING_SPAN_EVENTS` | 设为 truthy 后为每个 tracing span 的 enter/exit 事件输出日志行，用于细粒度性能追踪，正常运行时关闭以减少日志量。 |

#### `compute` — Rayon 计算线程池

| 环境变量 | 作用 |
|----------|------|
| `PGD_COMPUTE_THREADS` | Rayon 线程池大小。不设则默认为 CPU 核心数的一半，以避免与 Tokio 线程争抢 CPU。GPU 密集型场景可酌情增大。 |
| `PGD_COMPUTE_STACK_SIZE` | 计算线程的栈大小（字节），默认 2MB（`2097152`）。递归深度较大的算法（如树遍历、解析器）需要增大此值，否则会 stack overflow。 |
| `PGD_COMPUTE_THREAD_PREFIX` | 线程名称前缀，默认 `compute`。`top`/`htop`/Nsight 等工具中可按此前缀识别计算线程，便于区分 Tokio 工作线程和计算线程。 |

---

## 四、设计决策

### D-01：figment 分层配置而非单一来源

支持多环境部署：开发用默认值，容器用 TOML 文件，Kubernetes 用环境变量。环境变量优先级最高，允许运维无需修改镜像即可调参。

### D-02：空环境变量过滤

`PGD_RUNTIME_NUM_WORKER_THREADS=""` 不应覆盖默认值。所有 `Env::prefixed` 的 `filter_map` 都检查 `!v.is_empty()`，只有非空值才参与合并。

### D-03：system_port 替代 system_enabled

`system_enabled` 布尔标志被标记 `#[deprecated]`。新设计用 `system_port` 语义：`-1` 禁用，`0` 随机端口，正值绑定指定端口。减少配置项数量且语义更清晰。

### D-04：环境变量名常量集中管理

所有 `PGD_*` 变量名在 `environment_names.rs` 中定义为 `const &str`。好处：编译期检查拼写、IDE 跳转、grep 友好。

---

## 五、模块依赖

```
config 使用：
  figment          — Figment / Env / Toml / Serialized 配置合并
  serde            — Serialize / Deserialize
  validator        — Validate 校验
  derive_builder   — Builder 宏
  tokio            — runtime::Builder (create_runtime)
  std::thread      — available_parallelism

config 被使用：
  runtime.rs       — RuntimeConfig::from_settings() / create_runtime()
  worker.rs        — WorkerConfig / RuntimeConfig
  distributed.rs   — 读取 system 配置
  logging.rs       — jsonl_logging_enabled 等工具函数
  所有模块         — environment_names 中的常量
```
