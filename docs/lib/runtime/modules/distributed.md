# `distributed` 模块设计文档

**源码位置**：`lib/runtime/src/distributed.rs`（820 行，单文件模块）

---

## 一、为什么需要这个模块

Pagoda 是一个分布式推理框架。一个推理请求从前端路由器出发，需要经历服务发现（找到哪些 Worker 可用）、请求传输（把请求发送到某个 Worker）、健康监控（淘汰不健康的 Worker）三个分布式协调步骤。这三件事的技术选型在不同部署环境下各不相同：

- **服务发现**：生产用 etcd 或 Kubernetes API，开发用文件或内存；
- **请求传输**：默认 TCP（低延迟无 broker），支持 HTTP/2，历史遗留 NATS；
- **事件通道**：KV Router 的路由事件和 replica sync 依赖 NATS pub/sub，与请求平面无关。

如果没有 `distributed.rs` 这个文件，上层的 `Namespace`、`ServiceGroup`、`PortName` 等对象就需要各自持有 etcd 客户端、NATS 客户端、TCP 服务器等引用——依赖关系爆炸，任何配置切换都需要修改多处代码。

`distributed.rs` 的核心价值是**编排（orchestration）**：按正确顺序初始化所有子系统，整合成 `DistributedRuntime` 这一个对象，让上层业务代码只持有一个 `drt.clone()` 就能访问所有分布式能力，彻底屏蔽底层选型差异。

---

## 二、模块级类型别名

### 为什么需要这两个类型别名

```rust
type InstanceMap        = HashMap<PortName, Weak<Receiver<Vec<Instance>>>>;
type RoutingOccupancyMap = HashMap<PortName, Weak<RoutingOccupancyState>>;
```

`DistributedRuntime` 需要在多个 `PortName` 客户端之间共享两类状态：
1. **实例列表 Watch**（`InstanceMap`）：多个客户端监控同一 PortName 的可用实例列表，若各自创建独立 Watch 则会产生多倍的 etcd 连接和后台任务。共享同一个 `Receiver` 使一个后台 Watch 任务服务多个客户端。
2. **路由占用状态**（`RoutingOccupancyMap`）：追踪每个 PortName 当前有多少请求在途，用于 LLM 推理场景的负载感知路由（KV cache 命中率优化）。

两者都用 `Weak<T>` 而非 `Arc<T>`：注册表不持有这些状态的所有权。当所有调用方都释放了 `Arc<T>` 后，`Weak` 自动失效，下次有新调用方时重建，无需手动清理——避免了注册表因持有 `Arc` 而阻止状态被回收，消除内存泄漏风险。

`HashMap<PortName, ...>` 以 `PortName`（命名空间 + 组件名 + 端点名）为键，精确定位到某一个具体端点的共享状态。

---

## 三、`DistributedRuntime` 结构体

### 为什么需要这个结构体

`DistributedRuntime` 是 Pagoda 分布式能力的**单一入口**。设计目标：

1. **一次初始化**：所有子系统（etcd、NATS、TCP 服务器、健康监控）在 `new()` 中统一初始化，调用方无需关心顺序和依赖关系。
2. **廉价克隆**：所有字段都是 `Arc<T>` 或 `Copy` 类型，`clone()` 只递增引用计数，可以随意传入 `tokio::spawn` 而无需 `Arc<DistributedRuntime>`。
3. **后端无关**：上层代码通过 `Arc<dyn Discovery>` 等 trait 对象使用服务，切换 etcd/K8s/内存后端无需修改上层代码。

```rust
#[derive(Clone)]
pub struct DistributedRuntime {
    runtime: Runtime,
    nats_client: Option<transports::nats::Client>,
    network_manager: Arc<NetworkManager>,
    tcp_server: Arc<OnceCell<Arc<transports::tcp::server::TcpStreamServer>>>,
    system_status_server: Arc<OnceLock<Arc<system_status_server::SystemStatusServerInfo>>>,
    request_plane: RequestPlaneMode,
    discovery_client: Arc<dyn discovery::Discovery>,
    discovery_metadata: Option<Arc<tokio::sync::RwLock<discovery::DiscoveryMetadata>>>,
    servicegroup_registry: servicegroup::Registry,
    instance_sources: Arc<tokio::sync::Mutex<InstanceMap>>,
    routing_occupancy_states: Arc<tokio::sync::Mutex<RoutingOccupancyMap>>,
    system_health: Arc<parking_lot::Mutex<SystemHealth>>,
    local_portname_registry: crate::local_portname_registry::LocalPortNameRegistry,
    metrics_registry: MetricsRegistry,
    engine_routes: crate::engine_routes::EngineRouteRegistry,
}
```

### 字段详解

---

**`runtime: Runtime`**

本地运行时包装，内部含 Tokio 线程池（primary 高优先级 + secondary 低优先级）、主取消令牌（`primary_token`）和 `GracefulShutdownTracker`。`DistributedRuntime` 是 `Runtime` 的扩展，所有分布式能力建立在这个本地基础之上。`Runtime` 自身的字段已是 `Arc`，直接 Clone 不涉及数据拷贝。

---

**`nats_client: Option<transports::nats::Client>`**

NATS 客户端，`Option` 表示非必须。

为什么是Option：不是所有部署模式都需要 NATS。`process_local()`（前后端同进程）完全不需要 NATS；`PGD_REQUEST_PLANE=tcp` 使用 TCP 请求平面时不需要 NATS 做请求分发。

NATS启用条件的演进：历史上只有 `request_plane == Nats` 时才连接 NATS。后来发现 KV Router 的事件发布/订阅（路由决策广播）和 replica sync 依赖 NATS pub/sub，但不需要 NATS 请求平面——旧逻辑导致 `PGD_REQUEST_PLANE=tcp` 时 KV 路由功能完全失效。修复方案：只要 `NATS_SERVER` 环境变量存在就启用 NATS，使请求平面选择和事件通道选择彻底解耦（详见 `DistributedConfig::from_settings()` 的注释）。

---

**`network_manager: Arc<NetworkManager>`**

请求平面的统一管理器，封装 NATS/HTTP/TCP 三种请求路由模式的连接管理、客户端工厂和服务器工厂。

**为什么用 `Arc`**：多个 `DistributedRuntime` 克隆共享同一个 `NetworkManager`，避免因克隆而重复创建连接。`NetworkManager` 内部含连接池状态，必须是单例。

---

**`tcp_server: Arc<OnceCell<Arc<transports::tcp::server::TcpStreamServer>>>`**

TCP 请求平面服务器，懒初始化。

**为什么懒初始化**：不是所有角色都需要监听 TCP 端口。纯客户端角色（只发送请求、不接收请求）无需 TCP 服务器；Worker 角色才需要监听。若在 `new()` 中就创建，客户端角色会白白占用端口。`tcp_server()` 方法首次调用时才真正创建并绑定端口，之后复用。

**为什么用 `async_once_cell::OnceCell` 而非标准库**：标准库 `OnceCell::get_or_try_init` 在稳定版 Rust 中是 nightly-only API。`async_once_cell` 提供稳定实现且支持异步初始化闭包（TCP 服务器创建是 async 操作）。代码顶部注释明确说明：`// Used instead of std::cell::OnceCell because get_or_try_init there is nightly`。

---

**`system_status_server: Arc<OnceLock<Arc<system_status_server::SystemStatusServerInfo>>>`**

HTTP 系统状态服务器（`/health`、`/live`、`/metrics`、`/metadata` 端点）的信息（监听地址 + JoinHandle）。

**为什么用 `OnceLock`**：服务器在 `new()` 中写入一次，之后多线程并发读取其监听地址（用于测试断言、运维监控）。`OnceLock` 的语义是"一次写、无限读"，读路径完全无锁，比 `Mutex` 更高效。服务器未启用时 `get()` 返回 `None`，调用方可以安全判断。

**为什么服务器启动失败不中断初始化**：系统状态服务器提供的是运维可观测性，不是推理服务本身的依赖。端口被占用等原因导致启动失败时，推理服务应该继续运行，只是暂时无法通过 HTTP 查询健康状态。

---

**`request_plane: RequestPlaneMode`**

请求平面模式，`Copy` 枚举，直接按值存储。各 `Namespace`/`ServiceGroup`/`PortName` 创建时读取此值，决定与 Worker 通信时用哪种协议。因为是 `Copy` 类型，无需 `Arc` 包裹，Clone 时直接复制枚举值。

---

**`discovery_client: Arc<dyn discovery::Discovery>`**

服务发现 trait 对象，统一屏蔽 Kubernetes API、etcd、文件、内存四种后端。

**为什么用 `Arc<dyn>` 而非泛型参数**：若用泛型 `DistributedRuntime<D: Discovery>`，则 `DistributedRuntime<KubeDiscovery>` 和 `DistributedRuntime<EtcdDiscovery>` 是两个不同类型，无法互换存储和传递——整个调用链（`Namespace<D>`、`ServiceGroup<D>`、`PortName<D>`）都需要携带泛型参数，代码复杂度爆炸。`Arc<dyn Discovery>` 在运行时选择后端，调用链无需感知具体类型。

---

**`discovery_metadata: Option<Arc<tokio::sync::RwLock<discovery::DiscoveryMetadata>>>`**

仅 Kubernetes 后端使用（`Some`），其他后端为 `None`。

**为什么需要**：Kubernetes 发现后端在本地维护一份 K8s 集群状态缓存（Pod IP、注解元数据等），系统状态服务器的 `/metadata` HTTP 端点需要读取这份数据供运维查询。两者共享同一个 `Arc<RwLock<...>>`：`KubeDiscoveryClient` 写入更新，`SystemStatusServer` 读取暴露。若各自维护独立副本，会产生数据不一致。`tokio::sync::RwLock` 支持多读单写，适合频繁读取、偶尔更新的缓存场景。非 Kubernetes 后端不需要此缓存，用 `Option::None` 明确表示"不适用"。

---

**`servicegroup_registry: servicegroup::Registry`**

组件注册表，允许指向同一远程 ServiceGroup 的多个客户端对象共享同一个后台 Watch 任务和 NATS 服务注册。

**为什么需要**：考虑一个 Worker 同时暴露 `generate` 和 `clear_kv_blocks` 两个端点——这是同一 ServiceGroup 的两个 PortName。若每个 PortName 各自创建 etcd Watch 和 NATS 服务注册，就会产生冗余的网络连接和后台任务。Registry 内部对相同 ServiceGroup 的请求做去重，只保留一套后台基础设施。

---

**`instance_sources: Arc<tokio::sync::Mutex<InstanceMap>>`**

以 `PortName` 为键存储实例 Watch 接收器弱引用的 Map。

**为什么需要**：当多个调用方同时关注同一个 PortName 的可用实例列表时（例如多个并发推理请求都在查询 `generate` 的 Worker 列表），若每个调用方各自创建 etcd Watch，会导致 etcd 连接数与请求数成正比——这在高并发场景下完全不可接受。通过此 Map 共享一个 `Receiver`，所有调用方订阅同一个广播，只有一个 etcd Watch 在后台运行。使用 `Weak` 引用：当所有订阅者都释放了 `Arc<Receiver>` 后，下次调用会发现 `Weak::upgrade()` 返回 `None`，重新创建 Watch，自然生命周期管理。

**为什么用 `tokio::sync::Mutex`**：Map 的读写发生在 async 上下文（`await` 期间持有），必须使用 async 锁而非同步锁（持有同步锁时不能 `await`，会导致 Tokio 线程池饥饿）。

---

**`routing_occupancy_states: Arc<tokio::sync::Mutex<RoutingOccupancyMap>>`**

以 `PortName` 为键存储路由占用状态弱引用的 Map，设计模式与 `instance_sources` 完全相同。

**为什么需要**：LLM 推理的 KV cache 感知路由需要知道每个 Worker 当前有多少请求在途（占用率），以便将新请求路由到 KV cache 命中概率更高的 Worker。多个路由决策可能同时读写同一 PortName 的占用状态，此 Map 确保它们共享同一份状态对象而非各自维护副本。

---

**`system_health: Arc<parking_lot::Mutex<SystemHealth>>`**

系统健康状态机，维护进程存活状态、uptime 计时器，为 `/health`/`/live` 端点提供响应，并维护 Prometheus uptime gauge。

**为什么用 `parking_lot::Mutex` 而非 `tokio::sync::Mutex`**：健康状态的读写是纯内存操作（更新计时器、切换枚举值），持锁时间在微秒级且代码中不含任何 `await`。在这种场景下同步锁比异步锁开销更低（asynco 锁需要额外的 waker 机制）。`parking_lot` 的实现性能又优于标准库 `Mutex`。此外，Prometheus 更新回调是同步闭包（`Fn() -> Result<()>`，不是 async），必须使用同步锁才能在回调中访问 `system_health`。

---

**`local_portname_registry: crate::local_portname_registry::LocalPortNameRegistry`**

同进程直调注册表，用于前后端在同一进程运行的场景（`process_local()` 配置）。

**为什么需要**：前后端同进程时，通过 TCP/HTTP 网络栈的开销（序列化、syscall、loopback）是完全不必要的。`LocalPortNameRegistry` 允许 PortName 直接注册 Rust 函数指针，前端发起"请求"时直接调用该函数，延迟从 ~100μs 降低到 ~1μs。常用于嵌入式部署和测试场景。

---

**`metrics_registry: MetricsRegistry`**

Prometheus 指标注册表，是整个指标层级树的根节点。

**为什么 `basename()` 返回空字符串**：`DistributedRuntime` 实现了 `MetricsHierarchy` trait。指标的完整名称由层级路径拼接而成：`{namespace}_{servicegroup}_{portname}_{metric_name}`。DRT 是根节点，本身不贡献前缀——前缀从 Namespace 层开始。`basename()` 返回 `""` 使拼接逻辑不产生多余的 `_` 分隔符。

**为什么在 DRT 层注册 uptime gauge**：uptime 是进程级别的指标，不属于任何 Namespace 或 ServiceGroup，应该在最顶层注册。DRT 的 `metrics_registry` 是最顶层，所有 Prometheus 抓取路径（系统状态服务器 `/metrics`、前端 `/metrics`）都会包含此注册表的内容。

---

**`engine_routes: crate::engine_routes::EngineRouteRegistry`**

`/engine/*` 自定义 HTTP 路由回调注册表。

**为什么需要**：不同 Worker 可能需要暴露自定义的 HTTP 端点（如 `/engine/stats` 查询 GPU 利用率、`/engine/config` 读取模型配置），这些端点与框架无关，是业务层的需求。`EngineRouteRegistry` 允许上层代码在 DRT 上注册处理函数，系统状态服务器在启动时将这些路由一并挂载，无需业务代码直接操作 HTTP 服务器。

---

### `MetricsHierarchy for DistributedRuntime` 实现

```rust
impl MetricsHierarchy for DistributedRuntime {
    fn basename(&self) -> String { "".to_string() }
    fn parent_hierarchies(&self) -> Vec<&dyn MetricsHierarchy> { vec![] }
    fn get_metrics_registry(&self) -> &MetricsRegistry { &self.metrics_registry }
    fn connection_id(&self) -> Option<u64> {
        Some(self.discovery_client.instance_id())
    }
}
```

**为什么需要这个 impl**：`SystemHealth::initialize_uptime_gauge(&dyn MetricsHierarchy)` 需要通过此接口在正确的命名空间路径注册 Prometheus gauge。DRT 作为根节点，`parent_hierarchies()` 返回空（无父节点），`basename()` 返回空字符串（不贡献前缀）。这个 impl 使 DRT 可以作为 `&dyn MetricsHierarchy` 传入，解耦了 `SystemHealth` 和 `DistributedRuntime` 的直接依赖。

**`impl std::fmt::Debug for DistributedRuntime`**：手动实现，仅输出 `"DistributedRuntime"` 字符串。原因：DRT 包含多个不可派生 Debug 的字段（如 `Arc<dyn Discovery>`），无法使用 `#[derive(Debug)]`；手动实现保证 `{:?}` 格式化不 panic，满足 trait bound 要求（某些 trait 要求 `Debug`）。

---

## 四、`DistributedRuntime::new()` — 初始化流程

```rust
pub async fn new(runtime: Runtime, config: DistributedConfig) -> Result<Self>
```

**为什么需要这个函数**：`DistributedRuntime` 有 14 个字段，它们之间存在严格的初始化顺序依赖（NATS 客户端必须在 NetworkManager 之前创建，`cancel_token` 必须在 `runtime` 被 move 之前提取等）。将所有初始化逻辑集中在 `new()` 中，调用方只需一行 `DistributedRuntime::new(runtime, config).await?` 即可得到一个完全就绪的分布式运行时。

### 步骤 1：解构配置

```rust
let (discovery_backend, nats_config, request_plane) = config.dissolve();
```

`config.dissolve()` 由 `#[derive(Dissolve)]` 自动生成，将 `DistributedConfig` 消费（move）并展开为三个独立变量。之后各步骤直接使用这三个变量，不再持有 `config`——避免了字段访问的命名前缀，代码更清晰。

### 步骤 2：建立 NATS 连接

```rust
let nats_client = match nats_config {
    Some(nc) => Some(nc.connect().await?),
    None => None,
};
```

**为什么在此步骤就连接**：NATS 连接是后续所有依赖 NATS 的子系统（NetworkManager、KV Router 接口、NATS 服务注册）的前提。提前连接确保后续步骤可以直接使用已就绪的客户端。

**为什么失败时立即返回 `Err`**：若 `NATS_SERVER` 配置了地址但 NATS 不可达，说明环境配置有误，继续初始化只会让错误更难定位（后续依赖 NATS 的组件会产生更隐晦的错误）。快速失败（fail-fast）使问题立即暴露。

### 步骤 3：读取 RuntimeConfig 并提取取消令牌

```rust
let config = crate::config::RuntimeConfig::from_settings().unwrap_or_default();
// IMPORTANT: 必须在 runtime 被 move 进结构体之前提取 cancel_token
let cancel_token = if config.system_server_enabled() {
    Some(runtime.clone().child_token())
} else {
    None
};
```

**为什么必须在 move 之前提取**：Rust 的所有权规则：`runtime` 被 move 进结构体后，此函数作用域内的 `runtime` 变量即失效，无法再访问。代码注释专门标注了 `IMPORTANT`，提醒未来维护者不能调整此代码顺序。`child_token()` 创建主令牌的子令牌，用于控制系统状态服务器的生命周期（主令牌取消时子令牌也取消，但子令牌取消不影响主令牌）。

**为什么 `unwrap_or_default()`**：`RuntimeConfig::from_settings()` 读取环境变量，任何环境变量缺失都应使用默认值而非 panic——DRT 的初始化不应因为可选配置缺失而失败。

### 步骤 4：提取健康检查配置并构造 `SystemHealth`

```rust
let starting_health_status = config.starting_health_status.clone();
// ... 其他字段克隆 ...
let system_health = Arc::new(parking_lot::Mutex::new(SystemHealth::new(
    starting_health_status,
    use_portname_health_status,
    health_portname_path,
    live_portname_path,
)));
```

**为什么在 `runtime` move 前克隆配置字段**：与步骤 3 相同的所有权约束——`config` 在后续步骤中被消费，必须提前提取需要的字段。

`SystemHealth::new()` 初始化健康状态机，uptime 计时器在步骤 8 才真正启动（保证 uptime 从结构体完全就绪后开始计时，不包含初始化耗时）。

### 步骤 5：初始化服务发现后端

```rust
let (discovery_client, discovery_metadata) = match discovery_backend {
    DiscoveryBackend::Kubernetes => { ... }
    DiscoveryBackend::KvStore(kv_selector) => { ... }
};
```

这是 `new()` 中**最复杂**的步骤，原因：服务发现后端初始化涉及网络连接（etcd）或 K8s API 认证，可能失败；Kubernetes 后端需要额外创建 `discovery_metadata` 共享引用供系统状态服务器使用；KvStore 后端内部还有三条路径（etcd/file/memory）。

**Kubernetes 路径**：

```rust
DiscoveryBackend::Kubernetes => {
    let metadata = Arc::new(tokio::sync::RwLock::new(DiscoveryMetadata::new()));
    let client = KubeDiscoveryClient::new(metadata.clone(), runtime.primary_token())
        .await
        .inspect_err(|err| tracing::error!(%err, "Failed to initialize Kubernetes discovery client"))?;
    (Arc::new(client) as Arc<dyn Discovery>, Some(metadata))
}
```

先创建 `metadata` 共享引用，再传给 `KubeDiscoveryClient`——客户端内部 Watch K8s API 并写入 `metadata`，系统状态服务器稍后通过同一个 `Arc` 读取。`inspect_err` 在失败时打印错误日志，然后 `?` 传播错误，不吞没错误细节。

**KvStore 路径（etcd 分支）**：

```rust
kv::Selector::Etcd(etcd_config) => {
    etcd::Client::new(*etcd_config, runtime_clone)
        .await
        .inspect_err(|err|
            tracing::error!(%err, "Could not connect to etcd. Pass `--discovery-backend ..` to use a different backend or start etcd.")
        )?;
}
```

错误提示中明确建议用 `--discovery-backend` 切换后端，因为最常见的错误场景是开发者本地没有运行 etcd——一个有帮助的错误消息能节省大量调试时间。

**KvStore 路径（file/memory 分支）**：同步构造，不需要 `await`，不会失败，直接返回 `Ok`。

### 步骤 6：构造组件注册表和网络管理器

```rust
let servicegroup_registry = servicegroup::Registry::new();
let network_manager = NetworkManager::new(
    runtime.child_token(),
    nats_client.clone().map(|c| c.client().clone()),  // Option<async_nats::Client>
    servicegroup_registry.clone(),
    request_plane,
);
```

`NetworkManager` 需要底层的 `async_nats::Client`（而非 Pagoda 包装的 `transports::nats::Client`），因此通过 `.client().clone()` 提取。`nats_client.clone().map(...)` 处理 `Option`——NATS 不可用时传 `None`，NetworkManager 内部据此跳过 NATS 相关初始化。

**为什么 `servicegroup_registry.clone()` 传给 NetworkManager**：NetworkManager 需要访问组件注册表来管理服务路由（知道哪些 ServiceGroup 已注册了哪些 PortName），共享同一个注册表实例保证两者视图一致。

### 步骤 7：组装结构体

```rust
let distributed_runtime = Self {
    runtime,
    network_manager: Arc::new(network_manager),
    nats_client,
    tcp_server: Arc::new(OnceCell::new()),       // 空，懒初始化
    system_status_server: Arc::new(OnceLock::new()), // 空，步骤 10 写入
    discovery_client,
    discovery_metadata,
    servicegroup_registry,
    instance_sources: Arc::new(Mutex::new(HashMap::new())),
    routing_occupancy_states: Arc::new(Mutex::new(HashMap::new())),
    metrics_registry: crate::MetricsRegistry::new(),
    system_health,
    request_plane,
    local_portname_registry: LocalPortNameRegistry::new(),
    engine_routes: EngineRouteRegistry::new(),
};
```

此时结构体已完整构造，但部分字段（`tcp_server`、`system_status_server`）尚未真正初始化——它们是懒初始化容器，等待后续步骤或首次使用时填充。

### 步骤 8：初始化 uptime gauge

```rust
distributed_runtime
    .system_health
    .lock()
    .initialize_uptime_gauge(&distributed_runtime)?;
```

**为什么在结构体组装后才初始化**：`initialize_uptime_gauge` 需要 `&dyn MetricsHierarchy`，而 `DistributedRuntime` 实现了此 trait。在结构体完整组装后才能拿到 `&distributed_runtime` 作为 trait 对象传入。此步骤将 uptime Prometheus gauge 注册到 `metrics_registry`，**从这一刻起 uptime 开始计时**——不包含 etcd 连接等初始化耗时，反映的是"服务就绪后的运行时长"。

### 步骤 9：注册 Prometheus 更新回调

```rust
{
    let system_health = distributed_runtime.system_health.clone();
    distributed_runtime
        .metrics_registry
        .add_update_callback(Arc::new(move || {
            system_health.lock().update_uptime_gauge();
            Ok(())
        }));
}
```

**为什么用回调而非定时器**：Prometheus 抓取是按需触发的（运维工具定期 GET `/metrics`），定时刷新 gauge 会在无人抓取时产生无效的计算和内存写入。回调模式：每次 Prometheus 抓取前，注册表调用所有更新回调，然后再序列化 gauge 值——保证抓取到的值是最新的，同时不产生无用的后台计算。

**为什么用独立代码块 `{}`**：将 `system_health` 的克隆作用域限制在回调注册内，避免在后续代码中意外使用这个额外的克隆变量。清晰表达意图："这个克隆只用于这个回调"。

### 步骤 10：启动 HTTP 系统状态服务器（条件启动）

```rust
if let Some(cancel_token) = cancel_token {
    match spawn_system_status_server(&host, port, cancel_token,
        Arc::new(distributed_runtime.clone()),
        distributed_runtime.discovery_metadata.clone(),
    ).await {
        Ok((addr, handle)) => {
            let info = SystemStatusServerInfo::new(addr, Some(handle));
            distributed_runtime.system_status_server
                .set(Arc::new(info))
                .expect("System status server info should only be set once");
        }
        Err(e) => {
            tracing::error!("System status server startup failed: {e}");
            // 继续初始化，不返回 Err
        }
    }
}
```

**为什么失败不中断初始化**：状态服务器提供运维可观测性，不是推理核心功能的依赖。端口冲突等原因导致启动失败时，推理服务应继续运行，只是暂时无法查询健康状态和指标。

**`.expect("should only be set once")`**：`OnceLock::set()` 若被调用两次会返回 `Err`。`expect` 将其转为 panic——这是一个"不应该发生的编程错误"（`new()` 中只有一处 `set`），用 panic 而非忽略错误，确保如果将来代码变更引入了 bug 能立即发现。

**`Arc::new(distributed_runtime.clone())`**：系统状态服务器需要 `Arc<dyn MetricsHierarchy>` 来定期抓取指标。`distributed_runtime` 实现了 `MetricsHierarchy`，`Arc::new(...)` 包裹使其满足 `'static` 约束（trait 对象需要 `'static`）。

### 步骤 11：启动健康检查管理器（条件启动）

```rust
if config.health_check_enabled {
    let health_check_config = HealthCheckConfig {
        canary_wait_time: Duration::from_secs(config.canary_wait_time_secs),
        request_timeout: Duration::from_secs(config.health_check_request_timeout_secs),
    };
    match start_health_check_manager(distributed_runtime.clone(), Some(health_check_config)).await {
        Ok(()) => tracing::info!("Health check manager started ..."),
        Err(e) => tracing::error!("Health check manager failed to start: {e}"),
    }
}
```

**为什么需要健康检查管理器**：Worker 可能因 OOM、GPU 故障等原因变得不健康但不主动注销。健康检查管理器定期向每个 Worker 发送探测请求，超时或失败则将其从路由表中摘除，防止请求被分发到不健康的 Worker。

**`canary_wait_time`**：Worker 首次注册后，等待一段时间再开始健康检查（给 Worker 加载模型的时间），避免在 Worker 初始化期间就将其标记为不健康。

失败同样不中断初始化——健康检查是保护性功能，缺失只影响对失败 Worker 的自动摘除，不影响基本推理功能。

---

## 五、`DistributedRuntime` 公开方法

### `from_settings(runtime) -> Result<Self>`

```rust
pub async fn from_settings(runtime: Runtime) -> Result<Self> {
    let config = DistributedConfig::from_settings();
    Self::new(runtime, config).await
}
```

**为什么需要**：生产环境中配置完全来自环境变量，调用方不需要手动构造 `DistributedConfig`。此方法提供"零配置"的便捷入口。与 `new()` 分离的原因：`new()` 接受显式配置，方便测试时注入特定配置；`from_settings()` 适合生产代码，测试代码使用 `distributed_test_utils` 中的辅助函数。

---

### `runtime() -> &Runtime`

返回内部 `Runtime` 引用。调用方可通过此方法访问 Tokio 线程池（`runtime.primary().spawn(...)`）。返回引用而非克隆，避免不必要的 Arc 引用计数递增（调用方若需要克隆自己 `.clone()`）。

---

### `primary_token() -> CancellationToken`

委托给 `self.runtime.primary_token()`，返回主取消令牌的克隆。**为什么需要**：后台任务需要监听此令牌以便在 DRT 关闭时优雅退出。`CancellationToken` 的 `clone()` 是廉价操作（Arc 内部），可以自由分发给各后台任务。

---

### `servicegroup_registry() -> &servicegroup::Registry`

返回组件注册表引用。代码注释标注 `TODO: Don't hand out pointers, instead have methods to use the registry in friendly ways`——现有接口暴露了内部实现细节（调用方需要了解 Registry 的 API），未来应收归为更高层次的业务方法（如 `register_servicegroup()`、`find_servicegroup()`）。

---

### `system_health() -> Arc<parking_lot::Mutex<SystemHealth>>`

返回系统健康对象的 `Arc` 克隆。代码注释同样标注 `TODO`：未来应提供健康状态服务（如 `mark_healthy()`、`is_live()`）而非直接暴露内部锁。

---

### `connection_id() -> u64`

```rust
pub fn connection_id(&self) -> u64 {
    self.discovery_client.instance_id()
}
```

委托给 `self.discovery_client.instance_id()`。**为什么需要**：服务发现 Watch 需要检测后端重连（etcd lease_id 变化、文件 connection_id 变化等），连接 ID 变化表明后端发生了重启，此时已有的 Watch 流可能已失效，需要重新建立。

---

### `local_portname_registry() -> &crate::local_portname_registry::LocalPortNameRegistry`

```rust
pub fn local_portname_registry(
    &self,
) -> &crate::local_portname_registry::LocalPortNameRegistry {
    &self.local_portname_registry
}
```

返回同进程直调注册表引用。**为什么需要**：`process_local()` 场景下，上层需要把 `PortName` 处理函数注册到这张本地表里，前端才能按名字直接查找并调用，而不经过 TCP/HTTP/NATS 协议栈。返回引用而非复制，保证注册和查询操作作用于同一份注册表状态。

---

### `engine_routes() -> &crate::engine_routes::EngineRouteRegistry`

```rust
/// Get the engine route registry for registering custom /engine/* routes
pub fn engine_routes(&self) -> &crate::engine_routes::EngineRouteRegistry {
    &self.engine_routes
}
```

返回 `/engine/*` 路由注册表引用。**为什么需要**：业务层可以通过这个入口注册自定义 HTTP 路由，而不必直接操作底层 HTTP 服务器。这样自定义路由和系统状态路由可以由同一套服务器统一挂载，职责边界更清晰。

---

### `metadata_artifacts() -> &crate::metadata_registry::MetadataArtifactRegistry`

```rust
pub fn metadata_artifacts(&self) -> &crate::metadata_registry::MetadataArtifactRegistry {
    &self.metadata_artifacts
}
```

返回元数据产物注册表引用。**为什么需要**：上层代码可以把框架级元数据产物统一登记到这里，后续由运行时集中查询、组织和对外暴露。返回引用而非克隆，避免出现多个彼此不一致的注册表副本。

---

### `shutdown()`

```rust
pub fn shutdown(&self) {
    self.runtime.shutdown();
    self.discovery_client.shutdown();
}
```

**为什么顺序是"先停 runtime，再注销发现"**：

若顺序颠倒——先注销服务发现，再停 Tokio 任务：其他节点会立即感知本节点下线（移除路由），但本节点的 Tokio 任务还在处理请求，出现"已从路由移除但还在处理"的窗口期，可能导致请求被转发到正在关闭的节点。

正确顺序：先取消所有 Tokio 任务（`runtime.shutdown()` 触发 `primary_token.cancel()`，所有监听令牌的任务开始退出），**等待任务退出后**再注销服务发现，确保注销时节点真的已停止接受新请求。

---

### `namespace(name) -> Result<Namespace>`

```rust
pub fn namespace(&self, name: impl Into<String>) -> Result<Namespace> {
    Namespace::new(self.clone(), name.into())
}
```

创建 `Namespace` 对象，是整个命名层级树（`Namespace → ServiceGroup → PortName`）的入口。传入 `self.clone()`（廉价），使 `Namespace` 持有 DRT 的完整能力（发现、传输、指标等）。`impl Into<String>` 接受 `&str`、`String` 等多种类型，使调用方无需手动 `.to_string()`。

---

### `discovery() -> Arc<dyn Discovery>`

返回发现客户端的 `Arc` 克隆。**为什么单独暴露**：某些场景（如运维工具查询已注册节点）需要直接调用服务发现接口，不经过 Namespace/ServiceGroup 层级。

---

### `tcp_server() -> Result<Arc<TcpStreamServer>>`

```rust
pub async fn tcp_server(&self) -> Result<Arc<tcp::server::TcpStreamServer>> {
    Ok(self.tcp_server
        .get_or_try_init(async move {
            let options = tcp::server::ServerOptions::default();
            let server = tcp::server::TcpStreamServer::new(options).await?;
            Ok::<_, PipelineError>(server)
        })
        .await?
        .clone())
}
```

**懒初始化的意义**：Worker 角色（接收推理请求）需要 TCP 服务器监听端口；Router 角色（转发请求）不需要。在 `new()` 时统一创建会让所有角色都占用端口，即使不需要。首次调用此方法时创建并绑定端口，之后多次调用返回同一个实例（`OnceCell` 语义）。

---

### `network_manager() -> Arc<NetworkManager>`

返回网络管理器的 `Arc` 克隆，供上层（`PortName::serve()` 等）创建请求平面客户端和服务器。

---

### `request_plane_server() -> Result<Arc<dyn RequestPlaneServer>>`

```rust
pub async fn request_plane_server(&self) -> Result<Arc<dyn RequestPlaneServer>> {
    self.network_manager().server().await
}
```

便捷方法，将 `network_manager().server().await` 封装成一步调用，减少调用方的模板代码。返回 `Arc<dyn RequestPlaneServer>` trait 对象，上层不需要知道底层是 TCP、HTTP 还是 NATS 服务器。

---

### `system_status_server_info() -> Option<Arc<SystemStatusServerInfo>>`

从 `OnceLock` 中读取（`get().cloned()`）。**为什么返回 Option**：服务器可能未启用（`system_server_enabled() == false`）或启动失败，调用方需要安全处理两种情况。常用于测试中获取监听地址（`info.addr`）来验证服务器正常启动。

---

### `request_plane() -> RequestPlaneMode`

返回 `Copy` 枚举值，调用方据此决定如何构造请求（选择 TCP/HTTP/NATS 连接器）。

---

### `default_event_transport_kind() -> crate::discovery::EventTransportKind`

```rust
pub fn default_event_transport_kind(&self) -> crate::discovery::EventTransportKind {
    self.event_transport_kind
}
```

返回默认事件通道传输类型。**为什么需要**：事件平面和请求平面是两套独立的通道选择，调用方在创建事件发布器、订阅器或事件通道元数据时，需要知道 DRT 当前默认采用哪种事件传输后端（如 NATS 或 ZMQ），而不必再次读取环境变量或重复执行配置解析。返回 `Copy` 枚举值而非引用，调用方可以零成本按值传递。



### `child_token() -> CancellationToken`

创建主令牌的子令牌，传给需要独立取消控制的后台任务。子令牌被取消时不影响主令牌（及其他子令牌），允许选择性地停止某个子系统而不关闭整个 DRT。

---

### `graceful_shutdown_tracker() -> Arc<GracefulShutdownTracker>` (`pub(crate)`)

委托给 `runtime.graceful_shutdown_tracker()`，供内部组件（如 `PortName`）注册关闭回调，确保在 shutdown 时等待所有在途请求处理完毕后再退出（优雅关闭，不丢弃进行中的推理）。`pub(crate)` 限制：这是框架内部机制，外部业务代码不应直接调用。

---

### `instance_sources() -> Arc<Mutex<InstanceMap>>`

返回 `Arc` 克隆，供 `servicegroup.rs` 管理 PortName 实例 Watch 的生命周期（查询已有 Watch、注册新 Watch、清理过期 Watch）。

---

### `routing_occupancy_states() -> Arc<Mutex<RoutingOccupancyMap>>` (`pub(crate)`)

返回 `Arc` 克隆，供路由层（`kv_router` 等）查询和更新在途请求计数。`pub(crate)` 限制：路由占用状态是框架内部的调度信息，不对外暴露。

---

## 六、NATS 临时接口

**为什么这三个方法以 `kv_router_` 为前缀且标注 `TODO`**：它们是为 KV Router 特定场景临时打开的 NATS 直接访问口，绕过了正常的请求平面抽象层。注释说明等 KV Router 迁移到更干净的抽象后这些方法将被移除。用前缀命名明确标识"这是临时的、有限的、专用的接口"，防止其他代码随意调用。

### `kv_router_nats_publish(subject, payload) -> anyhow::Result<()>`

```rust
pub async fn kv_router_nats_publish(&self, subject: String, payload: bytes::Bytes) -> anyhow::Result<()> {
    let Some(nats_client) = self.nats_client.as_ref() else {
        tracing::trace!("Skipping NATS publish (NATS not configured): {subject}");
        return Ok(());  // 静默成功，不报错
    };
    Ok(nats_client.client().publish(subject, payload).await?)
}
```

**为什么 NATS 不可用时静默返回 `Ok(())`**：KV Router 在"近似模式"（`--no-kv-events`，不使用 NATS 事件）下仍然工作，只是路由决策质量略低（不能实时感知 KV cache 状态）。此时 NATS 不可用是预期状态，publish 是可选的优化路径，不应导致推理请求失败。使用 `tracing::trace!`（而非 warn/error）记录，避免在近似模式下产生大量噪音日志。

---

### `kv_router_nats_subscribe(subject) -> Result<async_nats::Subscriber>` (`pub(crate)`)

```rust
pub(crate) async fn kv_router_nats_subscribe(&self, subject: String) -> Result<async_nats::Subscriber> {
    let Some(nats_client) = self.nats_client.as_ref() else {
        anyhow::bail!("KV router's EventSubscriber requires NATS");
    };
    Ok(nats_client.client().subscribe(subject).await?)
}
```

**为什么与 publish 行为不同（bail 而非静默成功）**：订阅是建立持续连接的操作，调用方会将订阅的 `Receiver` 用于后续的事件处理循环。若 NATS 不可用而静默返回"假成功"，调用方会得到一个永远不产生数据的假 Subscriber，导致事件处理完全失效且难以排查。`bail!` 使调用方在订阅阶段就感知到 NATS 不可用，而非在运行时才发现没有数据。

`pub(crate)` 限制：订阅接口比发布接口更危险（持续占用资源），限制在 crate 内部使用。

---

### `kv_router_nats_request(subject, payload, timeout) -> anyhow::Result<async_nats::Message>`

```rust
pub async fn kv_router_nats_request(&self, subject: String, payload: bytes::Bytes, timeout: Duration) -> anyhow::Result<async_nats::Message> {
    let Some(nats_client) = self.nats_client.as_ref() else {
        anyhow::bail!("KV router's request requires NATS");
    };
    let response = tokio::time::timeout(timeout, nats_client.client().request(subject, payload))
        .await
        .map_err(|_| anyhow::anyhow!("Request timed out after {:?}", timeout))??;
    Ok(response)
}
```

**为什么需要显式超时参数**：NATS request-reply 是点对点查询（KV Router 查询 Worker 的 KV cache 状态），若目标 Worker 没有响应（宕机、网络故障），不能无限等待。调用方传入超时时间，使不同场景（快速探测 vs 等待重负载 Worker）可以使用不同的超时策略。

**`anyhow!("Request timed out after {:?}", timeout)`**：将 `tokio::time::Elapsed` 错误转换为包含超时时间的自定义消息，使日志中的错误更具诊断价值（能直接看到"超时了多久"而非只看到"超时"）。

**双 `??`**：`tokio::time::timeout` 返回 `Result<Result<Message, NatsErr>, Elapsed>`，外层 `?` 处理超时（此时已被 `map_err` 转换为 `anyhow::Error`），内层 `?` 处理 NATS 请求本身的错误。

---

## 七、`register_nats_service()` — 已废弃方法

```rust
/// DEPRECATED: This method exists only for NATS request plane support.
pub fn register_nats_service(&self, servicegroup: ServiceGroup) -> tokio::sync::mpsc::Receiver<Result<(), String>>
```

**为什么已废弃但未删除**：Pagoda 仍有少量流量通过 NATS 请求平面路由（历史遗留）。此方法将在所有流量迁移到 TCP 请求平面后删除。用 `DEPRECATED` 注释标注，防止新代码依赖此接口。

**为什么返回 `mpsc::Receiver` 而非 `async fn`**：此方法是同步的（`pub fn`，非 `async fn`），但内部需要 spawn 一个异步任务来完成 NATS 服务注册（异步网络操作）。`Receiver` 作为完成信号：调用方（通常是 `serve_portname`）通过 `blocking_recv()` 等待注册完成，然后才开始处理请求。若直接返回 `async fn`，调用方在 `await` 期间就占用了调用栈；通过 `Receiver` 解耦，调用方可以选择何时等待。使用容量 1 的 `mpsc` 而非 `oneshot`：`oneshot::Sender::send` 消费 sender，在异步任务中处理错误路径（多个 early return）时需要多次 send，`mpsc` 避免了 owner 转移的复杂性。

**双重检查逻辑**：

```rust
// 预检查（无锁）
if registry.lock().await.services.contains_key(&service_name) {
    tx.send(Ok(())).await;
    return;
}
// ... 构建 NATS 服务 ...
// 正式检查（有锁）
let mut guard = registry.lock().await;
if !guard.services.contains_key(&service_name) {
    guard.services.insert(service_name, nats_service);
} else {
    nats_service.stop().await;  // 重复创建的服务立即停止
}
```

**为什么需要双重检查**：同一 ServiceGroup 的多个 PortName（`generate`、`clear_kv_blocks`）会并发调用 `register_nats_service`。预检查（无锁）减少了加锁频率——已注册的情况直接短路返回。正式检查（有锁）防止两个并发调用都通过预检查后各自构建了 NATS 服务的竞态：后到的那个发现服务已存在，立即 `stop()` 清理自己构建的重复服务。这是经典的"双重检查锁定（double-checked locking）"模式在 async 上下文的应用。

---

## 八、`DiscoveryBackend` 枚举

### 为什么需要这个枚举

```rust
#[derive(Clone, Debug)]
pub enum DiscoveryBackend {
    Kubernetes,
    KvStore(kv::Selector),
}
```

服务发现有两种截然不同的机制：

1. **Kubernetes 原生发现**：通过 K8s API（CRD、PortNameSlice）发现 Pod，完全不依赖 KV 存储。这种方式与 K8s 基础设施深度集成，适合生产 K8s 集群。

2. **KV 存储发现**：将节点信息写入 KV 存储（etcd/文件/内存），其他节点 Watch 变化。适合裸机部署、开发环境和测试。

**为什么 `Kubernetes` 不作为 `kv::Selector` 的一个变体**：`kv::Selector` 描述的是"用哪种 KV 存储"，而 Kubernetes 后端根本不使用 KV 存储——将 `Kubernetes` 放入 `kv::Selector` 会产生语义矛盾（KV 选择器中出现"不使用 KV"的选项）。用独立的 `DiscoveryBackend` 枚举，清晰表达"发现机制的两大类"。

### `impl DiscoveryBackend`

这个 `impl` 只承载两类与“后端类型推导默认值”相关的辅助逻辑：一类是判断当前发现后端是否属于本地模式，另一类是据此推导事件平面的默认传输协议。把这些逻辑集中收口在 `DiscoveryBackend` 上，而不是散落在 `DistributedConfig::from_settings()` 或其他初始化路径里，可以保证默认值规则只有一份权威实现，避免不同调用点出现分叉。

---

### `is_local() -> bool`

```rust
pub fn is_local(&self) -> bool {
    matches!(
        self,
        DiscoveryBackend::KvStore(kv::Selector::File(_))
            | DiscoveryBackend::KvStore(kv::Selector::Memory)
    )
}
```

**为什么需要这个方法**：文件后端和内存后端都属于“本地发现”模式，不依赖 etcd、NATS 或 Kubernetes API 这类外部基础设施。运行时初始化时，经常需要先回答“当前是不是本地模式”，再决定后续的默认配置，例如事件平面在本地模式下默认走 ZMQ，而不是 NATS。把这个判断封装成具名方法，比在多个地方重复写 `match` 更清晰，也更不容易漏掉某个本地后端。

---

### `resolve_event_transport_kind() -> crate::discovery::EventTransportKind`

```rust
pub fn resolve_event_transport_kind(&self) -> crate::discovery::EventTransportKind {
    use crate::config::environment_names::event_plane::PGD_EVENT_PLANE;
    use crate::discovery::EventTransportKind;
    match std::env::var(PGD_EVENT_PLANE).as_deref() {
        Ok("nats") => EventTransportKind::Nats,
        Ok("zmq") => EventTransportKind::Zmq,
        Ok("") | Err(_) => {
            if self.is_local() {
                EventTransportKind::Zmq
            } else {
                EventTransportKind::Nats
            }
        }
        Ok(other) => {
            let default_kind = if self.is_local() {
                EventTransportKind::Zmq
            } else {
                EventTransportKind::Nats
            };
            tracing::warn!(
                "Invalid PGD_EVENT_PLANE value '{}'. Valid values: 'nats', 'zmq'. \
                 Defaulting to {:?}.",
                other,
                default_kind
            );
            default_kind
        }
    }
}
```

**为什么需要这个方法**：事件平面和请求平面是两套独立的通道选择，`DistributedRuntime` 在启动时必须决定“事件默认走哪种传输协议”。这个方法就是 `(PGD_EVENT_PLANE, discovery_backend) -> EventTransportKind` 的唯一权威映射：若环境变量显式写了 `nats` 或 `zmq`，则尊重用户配置；若环境变量未设置或为空，则由后端类型驱动默认值，本地后端（`file`/`mem`）默认选 `Zmq`，分布式后端（`etcd`/`kubernetes`）默认选 `Nats`。

**为什么非法值只告警、不 panic**：事件平面有一套合理的后备默认值，配置写错时最好的行为不是让整个进程启动失败，而是记录一条足够清晰的 warning，然后退回到按后端推导出的默认协议。这种处理既保留了可诊断性，又避免把一个可恢复的配置错误升级成完全不可用。

**为什么强调“启动时调用一次并缓存结果”**：这个方法会读取环境变量并执行一套带默认分支的判定逻辑，语义上属于“启动配置决议”，而不是运行时热路径逻辑。初始化阶段算一次、保存到 `DistributedConfig.event_transport_kind`，后续统一复用，能保证整个进程内事件平面的默认选择保持一致。

---

## 九、`DistributedConfig` 结构体

### 为什么需要独立的配置类型

```rust
#[derive(Dissolve)]
pub struct DistributedConfig {
    pub discovery_backend: DiscoveryBackend,
    pub nats_config: Option<nats::ClientOptions>,
    pub request_plane: RequestPlaneMode,
    pub event_transport_kind: crate::discovery::EventTransportKind,
}
```

若将所有配置参数直接传入 `DistributedRuntime::new(backend, nats, plane, ...)`，函数签名冗长，且调用方无法部分覆盖默认配置（必须提供所有参数）。独立的 `DistributedConfig` 类型：
- 允许 `from_settings()` 从环境变量读取完整配置
- 允许测试代码构造特定配置（覆盖某些字段，其余保持默认）
- `#[derive(Dissolve)]` 让 `new()` 一次性解包所有配置，代码简洁

---

### `DistributedConfig::from_settings()` — 生产环境标准入口

**NATS 启用条件的演进历史**（重要设计决策，代码注释有详细说明）：

最初逻辑：`nats_enabled = request_plane.is_nats()`——只有 NATS 请求平面才需要 NATS 连接。这在纯 NATS 架构下是对的。

后来 KV Router 引入了 NATS pub/sub 用于路由事件广播（`kv_router_nats_publish/subscribe`）。这些功能独立于请求平面，在 `PGD_REQUEST_PLANE=tcp` 时也需要 NATS。旧逻辑导致：当用户设置 `PGD_REQUEST_PLANE=tcp`（用 TCP 传输推理请求）但同时想使用 KV 路由事件时，NATS 客户端不会初始化，KV Router 静默失败（`kv_router_nats_publish` 收到 `nats_client=None` 时静默返回 `Ok`，没有任何错误提示）。

修复：

```rust
let nats_enabled = request_plane.is_nats()
    || std::env::var(crate::config::environment_names::nats::NATS_SERVER).is_ok();
```

只要用户配置了 `NATS_SERVER` 环境变量（明确表达"我有 NATS 服务器"），就启用 NATS 客户端，无论请求平面选择什么。使请求平面和事件通道的选择完全解耦。

**`PGD_DISCOVERY_BACKEND` 解析**：

- `"kubernetes"` → 特殊处理，映射到 `DiscoveryBackend::Kubernetes`
- 其他字符串 → 委托给 `kv::Selector::from_str()`（解析 `"etcd"`/`"file"`/`"mem"`）
- 非法值 → `panic!`（配置错误应在启动时暴露，而非在运行时某个不相关的地方报错）
- 默认值 → `"etcd"`（不设置此环境变量时使用 etcd）

---

### `DistributedConfig::for_cli()` — CLI 工具专用配置

```rust
pub fn for_cli() -> DistributedConfig {
    let etcd_config = etcd::ClientOptions {
        attach_lease: false,
        ..Default::default()
    };
    DistributedConfig {
        discovery_backend: DiscoveryBackend::KvStore(kv::Selector::Etcd(Box::new(etcd_config))),
        ...
    }
}
```

**为什么 `attach_lease: false` 是关键差异**：CLI 工具（如 `pagoda-cli list`）只读取 etcd 中的注册信息，不注册自身，因此**不应该申请 etcd 租约**。

如果申请了租约，etcd 会认为 CLI 进程是一个 Worker，为它维护租约续约。CLI 退出后不续约，租约在 TTL 内（通常 10 秒）仍然活跃，etcd 中留下孤立的"幽灵 Worker"记录，影响其他节点的服务发现（它们可能尝试向这个不存在的 Worker 发送请求）。

`attach_lease: false` 明确告知 etcd 客户端"只读，不注册"，etcd 连接断开后不会留下任何痕迹。

---

### `DistributedConfig::process_local()` — 同进程前后端配置

```rust
pub fn process_local() -> DistributedConfig {
    DistributedConfig {
        discovery_backend: DiscoveryBackend::KvStore(kv::Selector::Memory),
        nats_config: None,
        request_plane: RequestPlaneMode::Tcp,  // 占位值，实际通过 LocalPortNameRegistry 直调
    }
}
```

**使用场景**：前端（Router/调度器）和后端（Worker/推理引擎）在同一 Rust 进程内运行，无需网络通信。常见于：嵌入式部署（单机全栈）、集成测试（省去启动外部服务的开销）。

**为什么 `kv::Selector::Memory`**：同进程内的发现不需要持久化或跨进程共享，内存存储足够且零配置。

**`RequestPlaneMode::Tcp` 是占位值**：代码注释明确指出"This won't be used in process local, so we likely need a 'none' option"——同进程场景下请求通过 `LocalPortNameRegistry` 直接调用函数，不走任何网络协议。`Tcp` 是个合理的默认值，实际代码路径会在判断 `LocalPortNameRegistry` 命中后短路，不会真正建立 TCP 连接。这是已知的改进点。

---

## 十、`RequestPlaneMode` 枚举

### 为什么需要这个枚举

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RequestPlaneMode {
    Nats,
    Http,
    #[default]
    Tcp,
}
```

Pagoda 支持多种网络传输协议，选择哪种取决于部署环境和性能需求。将模式抽象为枚举使各处代码可以通过 `match request_plane { ... }` 做分支处理，无需到处散落 `if/else` 字符串比较。

**各变体的历史演进**：

- `Nats`（最早）：最初实现，依赖 NATS broker 做请求分发。优点：天然支持 pub/sub 和 load balancing；缺点：增加了 NATS broker 这个额外依赖，延迟比直连更高。
- `Http`：为支持 gRPC 风格接口（HTTP/2 + protobuf）而添加。
- `Tcp`（当前默认，`#[default]`）：raw TCP + msgpack，直连 Worker，无需 broker，延迟最低，支持流式传输（对 LLM token streaming 友好）。是目前推荐的生产模式。

**`Copy` 的重要性**：`RequestPlaneMode` 在 `DistributedRuntime` 字段、`NetworkManager`、各 PortName 创建逻辑中广泛传递。`Copy` 使其可以直接按值传递，无需引用和生命周期标注。

**`#[default]` 标注 `Tcp`**：`Tcp` 是推荐模式，`RequestPlaneMode::default()` 应该返回最合理的默认值。`#[default]` 使 `Default::default()` 和 `#[derive(Default)]` 自动得到 `Tcp`，无需手动 `impl Default`。

---

### `impl fmt::Display for RequestPlaneMode`

输出小写字符串：`"nats"` / `"http"` / `"tcp"`，与 `PGD_REQUEST_PLANE` 环境变量的合法值完全一致。**设计意图**：`Display` 输出应与 `FromStr` 输入是互逆的（`s.parse::<RequestPlaneMode>()?.to_string() == s`），保证配置的序列化/反序列化一致性，方便配置文件生成和日志解析。

---

### `impl FromStr for RequestPlaneMode`

```rust
fn from_str(s: &str) -> Result<Self, Self::Err> {
    match s.to_lowercase().as_str() {
        "nats" => Ok(Self::Nats),
        "http" => Ok(Self::Http),
        "tcp"  => Ok(Self::Tcp),
        _      => Err(anyhow::anyhow!("Invalid request plane mode: '{}'. Valid options are: 'nats', 'http', 'tcp'", s)),
    }
}
```

**为什么大小写不敏感**：环境变量的值通常由运维配置，不同人可能写 `"TCP"`、`"tcp"` 或 `"Tcp"`。大小写不敏感减少了因大小写不匹配导致的配置错误，是命令行和环境变量解析的最佳实践。

**错误消息中列出合法值**：`"Valid options are: 'nats', 'http', 'tcp'"`——当用户写错时（如 `PGD_REQUEST_PLANE=grpc`），错误消息直接告知正确写法，无需查阅文档。

---

### `RequestPlaneMode::from_env() -> Self`（私有方法）

```rust
fn from_env() -> Self {
    std::env::var("PGD_REQUEST_PLANE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_default()
}
```

**为什么是私有方法**：读取环境变量是 `DistributedConfig` 构造时的内部行为，外部不应直接调用（外部应通过 `DistributedConfig::from_settings()` 获取完整配置）。

**`unwrap_or_default()` 的语义**：环境变量不存在（最常见情况）→ 使用默认值 `Tcp`；环境变量存在但值非法 → 也使用默认值 `Tcp`（`.and_then(|s| s.parse().ok())` 解析失败返回 `None`）。选择静默回退而非 panic：`from_env()` 可能在进程早期调用，此时 panic 会产生不友好的错误消息。注释说明"Reads from `PGD_REQUEST_PLANE` environment variable"（uncached，每次调用都重新读取）。

---

### `is_nats() -> bool`

```rust
pub fn is_nats(&self) -> bool {
    matches!(self, RequestPlaneMode::Nats)
}
```

**为什么需要单独的方法**：`DistributedConfig::from_settings()` 和 `for_cli()` 都需要判断"是否为 NATS 请求平面"来决定是否初始化 NATS 客户端。提供具名方法比 `mode == RequestPlaneMode::Nats` 更语义清晰，且如果未来 NATS 相关逻辑变化（如新增 `NatsV2` 变体），只需修改 `is_nats()` 的实现，不需要修改所有调用处。

---

## 十一、`distributed_test_utils` 子模块

### 为什么需要测试工具子模块

```rust
pub mod distributed_test_utils {
    //! Common test helper functions for DistributedRuntime tests
}
```

DRT 初始化需要 Runtime、配置等多个依赖，测试代码重复构造这些依赖既繁琐又易错。将常用的 DRT 构造逻辑集中在此模块，所有需要 DRT 的测试统一调用，降低测试代码的维护负担。两个辅助函数对应两种不同的测试场景，满足不同的测试需求。

---

### `create_test_drt_async() -> DistributedRuntime` (`#[cfg(feature = "integration")]`)

```rust
#[cfg(feature = "integration")]
pub async fn create_test_drt_async() -> super::DistributedRuntime {
    let rt = crate::Runtime::from_current().unwrap();
    let config = super::DistributedConfig {
        discovery_backend: super::DiscoveryBackend::KvStore(kv::Selector::Memory),
        nats_config: Some(nats::ClientOptions::default()),
        request_plane: crate::distributed::RequestPlaneMode::default(),
    };
    super::DistributedRuntime::new(rt, config).await.unwrap()
}
```

**设计选择分析**：

- `Runtime::from_current()`：从当前 Tokio 运行时创建 Runtime 包装，适用于 `#[tokio::test]` 环境（测试框架已创建好 Tokio 运行时）。
- `kv::Selector::Memory`：内存后端，无外部依赖，测试间完全隔离，测试结束自动释放。
- `nats_config: Some(nats::ClientOptions::default())`：需要测试环境有 NATS server（集成测试的隐含前提）。
- `unwrap()` 而非 `?`：测试辅助函数中 panic 比返回 `Result` 更简洁——测试失败时 panic 的 backtrace 比 `?` 传播更容易定位问题。

**`#[cfg(feature = "integration")]` 的意义**：集成测试标志确保此函数只在显式开启集成测试时编译，防止普通单元测试（`cargo test` 无 feature 标志）意外依赖 NATS server，使 CI 环境中无 NATS 也能运行单元测试。

---

### `create_test_shared_drt_async(store_path: &Path) -> DistributedRuntime`

```rust
pub async fn create_test_shared_drt_async(store_path: &std::path::Path) -> super::DistributedRuntime {
    let rt = crate::Runtime::from_current().unwrap();
    let config = super::DistributedConfig {
        discovery_backend: super::DiscoveryBackend::KvStore(
            crate::storage::kv::Selector::File(store_path.to_path_buf()),
        ),
        nats_config: Some(nats::ClientOptions::default()),
        request_plane: crate::distributed::RequestPlaneMode::default(),
    };
    super::DistributedRuntime::new(rt, config).await.unwrap()
}
```

**为什么需要与 `create_test_drt_async` 不同的版本**：内存后端的 `MemoryStore` 是进程内隔离的——每个 `DistributedRuntime` 实例有独立的 `HashMap`，不同实例之间无法共享发现状态。但某些测试场景需要模拟多节点：

- 测试 Leader 选举：需要两个 DRT 实例相互发现对方
- 测试实例下线检测：DRT-A 注册，DRT-B 监听注销事件
- 测试健康检查跨实例传播

这些场景要求多个 DRT 实例共享同一份发现状态。文件后端通过共享文件系统目录实现跨实例共享：`create_test_shared_drt_async(path)` 的多次调用传入同一个 `store_path`，所有实例通过文件 Watch（inotify/kqueue）相互发现。

注意：此函数**不带** `#[cfg(feature = "integration")]`，但实际上仍然需要 NATS（因为 `nats_config: Some(...)`）——这可能是个遗漏，是已知的改进点。

---

## 十二、集成测试

所有测试在 `#[cfg(all(test, feature = "integration"))]` 块中。

### `test_drt_uptime_after_delay_system_disabled` / `test_drt_uptime_after_delay_system_enabled`

**测试目标**：验证 uptime 追踪与 HTTP 系统状态服务器解耦——无论 HTTP 服务器是否启用，uptime 都应正常计时。这是两个子系统的独立性保证测试。

```rust
temp_env::async_with_vars(
    [(env_system::PGD_SYSTEM_PORT, None::<&str>)],  // 禁用系统状态服务器
    async {
        let drt = create_test_drt_async().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let uptime = drt.system_health.lock().uptime();
        assert!(uptime >= Duration::from_millis(50), "Expected uptime >= 50ms, got {:?}", uptime);
    }
).await;
```

**`temp_env::async_with_vars`**：在 async 上下文中临时设置/删除环境变量，测试结束后自动恢复。这是测试环境隔离的关键工具——防止测试之间通过环境变量相互影响，使测试可以并行运行。

**两个测试的必要性**：`_system_disabled` 验证 uptime 不依赖 HTTP 服务器；`_system_enabled` 验证 HTTP 服务器启动不影响 uptime 计时。合在一起才能完整证明两个子系统真正解耦，而不是某一方依赖另一方。

---

### `test_request_plane_mode_from_str` / `test_request_plane_mode_display`

**测试目标**：验证 `FromStr` 和 `Display` 的往返一致性（`from_str(mode.to_string()) == Ok(mode)`），以及大小写不敏感（`"TCP"` / `"tcp"` 都合法），以及非法值正确报错（`"invalid".parse().is_err()`）。

**为什么是 `#[test]`（同步测试）而非 `#[tokio::test]`**：`RequestPlaneMode` 的 `FromStr` 和 `Display` 是纯同步操作，不涉及 IO 或 `await`。使用同步测试避免了 Tokio 运行时的启动开销，也明确表达"这是纯逻辑测试，无需异步"。

---

## 十三、关键设计决策汇总

| 决策 | 选择 | 设计原因 |
|------|------|---------|
| DRT 实现 `Clone` | 所有字段 `Arc`/`Copy` | 无需 `Arc<DRT>`，随意传入 spawn，使用体验自然；避免了 `Arc<Arc<T>>` 的双重包裹 |
| `tcp_server` 懒初始化 | `async_once_cell::OnceCell` | 不是所有角色都需要 TCP 服务器；标准库稳定 Rust 无 `get_or_try_init` |
| `system_status_server` 只写一次 | `std::sync::OnceLock` | 写一次读多次语义，读路径无锁；`expect("only set once")` 作为编程错误检测 |
| `system_health` 同步锁 | `parking_lot::Mutex` | 健康状态操作无 IO、无 `await`、持锁时间短；Prometheus 回调是同步 `Fn` |
| `discovery_client` trait 对象 | `Arc<dyn Discovery>` | 避免泛型参数传染整个调用链；运行时切换后端无需重新编译 |
| `discovery_metadata` 可选 | `Option<Arc<RwLock<...>>>` | 只有 K8s 后端需要此元数据；`Option` 明确表达"不适用"，比默认值更语义清晰 |
| 弱引用注册表 | `Weak<Receiver>/Weak<OccupancyState>` | 注册表不持有所有权，使用方释放后自动失效，无需手动清理 |
| NATS 连接条件 | `is_nats() OR NATS_SERVER env exists` | 修复 KV Router 在 TCP 请求平面下无法使用 NATS 事件的历史 bug |
| shutdown 顺序 | 先 runtime 后 discovery | 防止"已注销但还在处理请求"的窗口期，保证关闭的原子语义 |
| `for_cli` 禁用租约 | `attach_lease: false` | CLI 只读操作不应在 etcd 留下幽灵注册记录 |
| `process_local` TCP 占位 | `RequestPlaneMode::Tcp`（未来需 `None`） | 同进程走 LocalPortNameRegistry 直调，`Tcp` 是占位，已知改进点 |
| publish 静默成功 | NATS 不可用时 `Ok(())` | 近似模式下 KV 事件是可选的，不应导致推理请求失败 |
| subscribe/request 快速失败 | NATS 不可用时 `bail!` | 订阅/请求是建立持续依赖的操作，假成功比失败更难排查 |
| NATS 服务注册双重检查 | 预检(无锁) + 正式检(有锁) | 防止同 ServiceGroup 多 PortName 并发注册产生竞态，兼顾性能和正确性 |
