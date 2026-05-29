# `servicegroup` 模块设计

**源码**：`src/servicegroup.rs` · `src/servicegroup/client.rs` · `src/servicegroup/portname.rs` · `src/servicegroup/namespace.rs` · `src/servicegroup/registry.rs` · `src/servicegroup/service.rs`

---

## 一、模块定位

`servicegroup` 模块定义 Pagoda 的服务身份模型、实例寻址模型与客户端发现模型，是 Pagoda 新三段式服务抽象的核心入口。

Pagoda 的新三段式如下：

- **Namespace**：租户 / 环境 / 业务边界
- **ServiceGroup**：一组共享职责、共享实例池、共享协议身份的服务集合
- **PortName**：ServiceGroup 下对外暴露的具体入口或 RPC 语义端口

模块要解决三个核心问题：

- **我是谁**：运行时通过 `Namespace → ServiceGroup → PortName` 声明服务身份
- **我在哪**：通过 `Instance + TransportType + topo_json` 描述一个活跃实例的网络位置与拓扑属性
- **我如何被访问**：通过 `Client` 封装发现订阅、实例列表维护和负载均衡路由

设计原则：**调用链全同步、网络动作全懒加载**。`drt.namespace("x")?.service_group("y")?.portname("z")` 只是内存对象构造，不产生网络访问。只有在以下路径上才会触发真实网络动作：

- `PortNameConfigBuilder::start()`：向发现系统注册实例
- `PortName::client()`：订阅发现系统并维护动态实例视图

---

## 二、文件结构与可见性

```
src/servicegroup.rs        — pub 入口：TransportType / Instance / Namespace / ServiceGroup / PortName / Registry
src/servicegroup/
    ├── client.rs            — pub Client；pub(crate) RoutingOccupancyState, get_or_create_routing_occupancy_state
    ├── portname.rs          — pub(crate) PortNameConfig/Builder；pub build_transport_type
    ├── namespace.rs         — Namespace 的 MetricsHierarchy impl
    ├── registry.rs          — Registry::new() / impl Default for Registry
    ├── servicegroup.rs      — ServiceGroup 的辅助实现
    └── service.rs           — build_nats_service（兼容层）
```

这套结构与新三段式一一对应：

- `namespace.rs`：第一段 Namespace
- `servicegroup.rs`：第二段 ServiceGroup
- `portname.rs`：第三段 PortName

其中 `client.rs`、`registry.rs`、`service.rs` 属于配套支撑模块，而不是三段式本身的一部分。


## 三、类型详解


### 3.1 `TransportType` — 服务可达地址枚举

**来源**：`src/servicegroup.rs`

**设计意图**：Pagoda 允许同一 `PortName` 通过不同请求平面暴露服务能力，因此需要统一枚举表达“如何访问这个实例”。

```rust
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TransportType {
    Nats(String),   // "{namespace}.{servicegroup}.{portname}.{instance_id}"
    Http(String),   // "http://{host}:{port}/v1/rpc/{portname}"
    Tcp(String),    // "{host}:{port}/{instance_id_hex}/{portname}"
}
```

**实现的 trait**：

| Trait | 用途 |
|-------|------|
| `Debug` | 调试打印 |
| `Clone` | 值复制 |
| `Serialize` / `Deserialize` | 序列化写入 / 读取发现系统 |
| `Eq` / `PartialEq` | 相等比较 |
| `Hash` | 可作为 `HashMap` key |


### 3.2 `RegistryInner` / `Registry` — 协议层 service 注册表

**来源**：`src/servicegroup.rs`（结构体定义）+ `src/servicegroup/registry.rs`（方法实现）

**设计意图**：在 NATS 请求平面下，每个 `ServiceGroup` 需要一个唯一协议级 `Service` 对象。`Registry` 用于进程内统一保存这些协议对象，避免重复注册，并在运行时克隆之间共享状态。

```rust
#[derive(Default)]
pub struct RegistryInner {
    pub(crate) services: HashMap<String, async_nats::service::Service>,
    // key：servicegroup.service_name()（slugified）
}

#[derive(Clone)]
pub struct Registry {
    pub(crate) inner: Arc<tokio::sync::Mutex<RegistryInner>>,
    // Arc：允许多处共享（DRT clone 时同步）
    // tokio::sync::Mutex：注册 NATS service 时需要 await，必须用异步锁
}
```

**实现的 trait**：

| 类型 | Trait | 来源 | 说明 |
|------|-------|------|------|
| `RegistryInner` | `Default` | derive | `services: HashMap::new()` |
| `Registry` | `Clone` | derive | clone 只增加 Arc 引用计数，不复制数据 |
| `Registry` | `Default` | `registry.rs` 手写 | 调用 `Registry::new()` |

**自身方法**（`registry.rs`）：

```rust
impl Default for Registry {
    fn default() -> Self {
        Self::new()   // 委托给 new()
    }
}

impl Registry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(RegistryInner::default())),
        }
    }
    // 由 DRT 初始化时调用，创建空的 NATS service 存储容器
}
```

`Default` 手写而非 derive 的原因：`async_nats::service::Service` 不实现 `Default`，所以 `RegistryInner` 无法整体 derive `Default`。实际上 `RegistryInner` 的 `Default` 来自 `services: HashMap::new()`（HashMap 实现了 Default），可以 derive。`Registry` 的 `Default` 则手写委托 `new()`，这是 Rust 惯用模式：`new()` 是语义构造入口，`Default` 委托它，两者保持一致。


### 3.3 `Instance` — 一个活跃 PortName 实例的完整描述

**来源**：`src/servicegroup.rs`

**设计意图**：发现系统返回的不应只是地址字符串，而应是一个可被路由层、拓扑调度层和观测层直接消费的结构化对象。

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Instance {
    pub namespace:     String,
    pub servicegroup:  String,
    pub portname:     String,
    pub instance_id:   u64,
    pub transport:     TransportType,
}
```

**实现的 trait**：

| Trait | 说明 |
|-------|------|
| `Debug` | 标准格式输出 |
| `Clone` | 全字段拷贝 |
| `Serialize` / `Deserialize` | JSON 序列化 |
| `PartialEq` / `Eq` | 全字段比较 |
| `fmt::Display` | 输出 `"namespace/servicegroup/portname/instance_id"` |
| `Ord` / `PartialOrd` | 提供稳定排序 |

`topo_json` 是新设计里的关键增强点。它不是附属注释字段，而是实例身份的一部分：后续可以直接用于 NUMA-aware、rack-aware、super-node-aware 路由。

**自身方法**：

```rust
impl Instance {
    pub fn id(&self) -> u64
    pub fn portname_id(&self) -> PortNameId
}
```


### 3.4 `Namespace` — 命名空间

**来源**：`src/servicegroup.rs`（主体）+ `src/servicegroup/namespace.rs`（MetricsHierarchy impl）

**设计意图**：`Namespace` 是 Pagoda 三段式的第一段，用来表达租户 / 环境 / 业务边界，同时持有运行时引用，作为下游对象访问基础设施的入口。

```rust
#[derive(Builder, Clone, Validate)]
#[builder(pattern = "owned")]
pub struct Namespace {
    #[builder(private)]
    runtime: Arc<DistributedRuntime>,               // 基础设施入口；private setter 保证只能通过 new() 设置

    #[validate(custom(function = "validate_allowed_chars"))]
    name: String,                               // 单段名称，合法字符：[a-z0-9-_]

    #[builder(default = "None")]
    parent: Option<Arc<Namespace>>,             // 嵌套命名空间，如 "prod" 下的 "llm"

    #[builder(default = "Vec::new()")]
    labels: Vec<(String, String)>,              // Prometheus 额外标签

    #[builder(default = "crate::MetricsRegistry::new()")]
    metrics_registry: crate::MetricsRegistry,   // 本命名空间的指标节点

    #[builder(default = "Arc::new(DashMap::new())")]
    servicegroup_cache: Arc<DashMap<String, ServiceGroup>>,
}
```

**实现的 trait**：

| Trait | 来源 | 实现细节 |
|-------|------|---------|
| `Clone` | derive | Arc 引用计数增量，不复制底层数据 |
| `Validate` | derive（validator crate）| 校验 `name` 字符集，`build()` 时自动调用 |
| `fmt::Debug` | 手写（不 derive）| 输出 `"Namespace { name: x; parent: y }"`，规避 DRT 循环打印 |
| `fmt::Display` | 手写 | 输出 `self.name`（单段，不含 parent 前缀） |
| `DistributedRuntimeProvider` | 手写 | `fn drt() -> &DistributedRuntime { &self.drt }` |
| `RuntimeProvider` | 手写 | `fn rt() -> &Runtime { self.drt.rt() }` |
| `MetricsHierarchy` | 手写（namespace.rs）| `basename()` = `self.name`；`parent_hierarchies()` 向上遍历 parent 链，根节点为 DRT |

**自身方法**：

```rust
impl Namespace {
    pub(crate) fn new(drt: DistributedRuntime, name: String) -> anyhow::Result<Self>
    // 唯一构造入口：内部调用 NamespaceBuilder（含私有 setter），构造后将指标节点挂载到 DRT 指标树

    pub fn servicegroup(&self, name: impl Into<String>) -> anyhow::Result<ServiceGroup>
    // 幂等获取组件：先查 servicegroup_cache（DashMap 无锁读）；未命中则构造并缓存
    // NATS 模式下构造会 block_in_place 等待 NATS service 注册（见决策 D-02）

    pub fn namespace(&self, name: impl Into<String>) -> anyhow::Result<Namespace>
    // 创建子命名空间，parent 指向 self

    pub fn name(&self) -> String
    // 有 parent 时返回全路径 "parent.name"，无 parent 时返回 self.name
    // 用于 k8s 原生发现资源命名与 NATS subject 前缀
}
```

`servicegroup()` 保持同步，是整个服务身份链条的基础设计要求：上层业务只应在真正启动服务或真正访问远端实例时才进入异步路径。


### 3.5 `ServiceGroup` — 服务组

**来源**：`src/servicegroup.rs`（主体）+ `src/servicegroup/servicegroup.rs`（辅助实现）

**设计意图**：`ServiceGroup` 是 Pagoda 三段式的第二段，表达“一组共享职责、共享实例池、共享协议身份”的服务单元。它负责组织其下的 `PortName`，并在 NATS 模式下承担协议 service 注册职责。

```rust
#[derive(Educe, Builder, Clone, Validate)]
#[educe(Debug)]
#[builder(pattern = "owned", build_fn(private, name = "build_internal"))]
pub struct ServiceGroup {
    #[builder(private)]
    #[educe(Debug(ignore))]              // Debug 时跳过，避免冗长/循环输出
    drt: Arc<DistributedRuntime>,

    #[builder(setter(into))]
    #[validate(custom(function = "validate_allowed_chars"))]
    name: String,                        // 合法字符：[a-z0-9-_]

    #[builder(default = "Vec::new()")]
    labels: Vec<(String, String)>,

    #[builder(setter(into))]
    namespace: Namespace,

    #[builder(default = "crate::MetricsRegistry::new()")]
    metrics_registry: crate::MetricsRegistry,
}
```

**实现的 trait**：

| Trait | 来源 | 实现细节 |
|-------|------|---------|
| `Debug`（条件）| Educe derive | `drt` 字段被 `#[educe(Debug(ignore))]` 排除 |
| `Clone` | derive | Arc 引用计数，轻量 |
| `Validate` | derive | 校验 `name` 字符集 |
| `Hash` | 手写 | 仅用 `(namespace.name(), name)`，忽略 drt / metrics 等引用 |
| `PartialEq` | 手写 | 仅用 `(namespace.name(), name)` 比较 |
| `Eq` | 手写 | 依赖 PartialEq |
| `fmt::Display` | 手写 | 输出 `"ns.sg"`，如 `"llm.service1"` |
| `DistributedRuntimeProvider` | 手写 | `fn drt() -> &DistributedRuntime { &self.drt }` |
| `RuntimeProvider` | 手写 | `fn rt() -> &Runtime { self.drt.rt() }` |
| `MetricsHierarchy` | 手写 | `basename()` = `self.name`；`parent_hierarchies()` = `[DRT, ...ancestor namespaces, namespace]` |

**自身方法**：

```rust
impl ServiceGroup {
    pub fn service_name(&self) -> String
    // 生成合法的 NATS service 名称："ns.sg" 经 Slug::slugify 处理

    pub fn namespace(&self) -> &Namespace
    pub fn name(&self) -> &str
    pub fn labels(&self) -> &[(String, String)]

    pub fn portname(&self, name: impl Into<String>) -> PortName
    pub async fn list_instances(&self) -> anyhow::Result<Vec<Instance>>
    // 查询此 ServiceGroup 下所有活跃 Instance（调用 discovery.list()），过滤非 Portname 类型，排序返回
}

impl ServiceGroupBuilder {
    pub fn from_runtime(drt: Arc<DistributedRuntime>) -> Self
    // 构建起点，注入私有的 drt 字段（外部唯一合法路径）

    pub fn build(self) -> Result<ServiceGroup, anyhow::Error>
    // 覆盖 derive_builder 生成的 build()：
    // 1. 调用私有 build_internal() 构造 
    // 2. 若请求平面为 NATS：tokio::task::block_in_place(|| rx.blocking_recv()) 等待 NATS service 注册完成
    // 3. 返回 ServiceGroup
}
```

---

### 3.6 `PortName` — 端点

**来源**：`src/servicegroup.rs`（主体 + 基础方法）+ `src/servicegroup/portname.rs`（注册/注销方法）

**设计意图**：`PortName` 是 Pagoda 三段式的第三段，是服务树叶子节点，对应一个具体访问入口或 RPC 语义端口。它既是服务端注册入口，也是客户端构造入口。

```rust
#[derive(Debug, Clone)]
pub struct PortName {
    servicegroup:     ServiceGroup,
    name:             String,
    labels:           Vec<(String, String)>,
    metrics_registry: crate::MetricsRegistry,
}
```

**实现的 trait**：

| Trait | 说明 |
|-------|------|
| `Debug` | 标准格式 |
| `Clone` | 轻量克隆 |
| `Hash` / `PartialEq` / `Eq` | 按 `(servicegroup, name)` 标识 |
| `DistributedRuntimeProvider` | 穿透到运行时 |
| `RuntimeProvider` | 访问执行运行时 |
| `MetricsHierarchy` | 指标树叶子节点 |

**自身方法**：

```rust
impl PortName {
    pub fn id(&self) -> PortNameId
    pub fn name(&self) -> &str
    pub fn servicegroup(&self) -> &ServiceGroup

    pub async fn client(&self) -> anyhow::Result<client::Client>
    pub fn portname_builder(&self) -> portname::PortNameConfigBuilder
}
```

**运行时动态注销 / 重注册方法**：

```rust
impl PortName {
    pub async fn unregister_port_instance(&self) -> anyhow::Result<()>
    pub async fn register_port_instance(&self) -> anyhow::Result<()>
}
```

这两个方法不是“删除/新增 `PortName` 对象”，而是**只操作当前进程对应实例在发现系统中的上下线状态**：

- `unregister_port_instance()`：把当前 worker 对应实例从发现平面移除，使路由层不再把新请求发往该实例；
- `register_port_instance()`：把当前 worker 对应实例重新写回发现平面，使其重新进入路由池。

两者都遵循同一模式：

1. 读取 `drt.connection_id()` 与 `self.id()`；
2. 调用 `build_transport_type(...)` 生成当前实例的 `TransportType`；
3. 组装发现层对象（`DiscoveryInstance::PortName(...)` 或 `DiscoverySpec::PortName { ... }`）；
4. 调用 `discovery.unregister(...)` / `discovery.register(...)`；
5. 失败时记录 error 日志并返回带“检查发现服务状态”的 `anyhow` 错误。

因此，这两个方法更适合“临时摘流 / 恢复流量”的运行时控制，而不是服务创建/销毁。

---

### 3.7 `PortNameConfig` / `PortNameConfigBuilder` — 服务端注册配置

**来源**：`src/servicegroup/portname.rs`

**设计意图**：`PortName` 的服务端注册不是单点动作，而是请求平面、发现系统、健康系统与优雅关闭系统的多步协调，因此使用 Builder 聚合可选配置，并用 `.start().await` 作为唯一执行入口。

```rust
#[derive(Educe, Builder, Dissolve)]
#[educe(Debug)]
#[builder(pattern = "owned", build_fn(private, name = "build_internal"))]
pub struct PortNameConfig {
    #[builder(private)]
    portname: PortName,

    #[educe(Debug(ignore))]
    handler: Arc<dyn PushWorkHandler>,

    #[builder(default, setter(into))]
    metrics_labels: Option<Vec<(String, String)>>,

    #[builder(default = "true")]
    graceful_shutdown: bool,

    #[educe(Debug(ignore))]
    #[builder(default, setter(into, strip_option))]
    health_check_payload: Option<serde_json::Value>,
}
```

字段职责要点：

- `portname`：Builder 绑定的目标服务入口，决定后续所有注册动作的身份三元组；
- `handler`：真正处理请求的执行体，`start()` 的本质就是把它接入请求平面；
- `metrics_labels`：写入 handler 指标上下文的附加标签，只影响观测，不影响路由与发现；
- `graceful_shutdown`：控制是否把当前 PortName 纳入优雅关闭跟踪；
- `health_check_payload`：若配置，则在 `start()` 期间同步把该实例注册到 `SystemHealth`，并把 notifier 回接给 handler。

**Builder 方法**：

```rust
impl PortNameConfigBuilder {
    pub(crate) fn from_portname(portname: PortName) -> Self
    pub fn register_local_engine(self, engine: LocalAsyncEngine) -> Result<Self>
    pub async fn start(self) -> Result<()>
}
```

#### `from_portname(portname) -> Self`

这是 Builder 的最小构造入口，等价于“先 `Default::default()`，再把私有 `portname` 字段写进去”。

它的作用不是执行任何注册动作，而是保证后续链式配置总是绑定到同一个 `PortName`：

```rust
pub(crate) fn from_portname(portname: PortName) -> Self {
    Self::default().portname(portname)
}
```

#### `register_local_engine(self, engine) -> Result<Self>`

这个方法服务于**进程内直连优化**：如果当前 `PortName` 对应的处理引擎也运行在本进程内，就先把 `LocalAsyncEngine` 注册到本地注册表，使调用方后续可以绕过网络平面，直接走 in-process 路径。

其源码语义是：

1. 检查 builder 内部是否已经持有 `portname`；
2. 若有，则通过 `portname.drt().local_portname_registry()`（目标设计）拿到本地注册表；
3. 以 `portname.name.clone()` 为 key 注册 `engine`；
4. 记录 debug 日志；
5. 无论是否注册成功走到实际分支，最终都返回 `Ok(self)`，便于继续链式调用。

因此它是**附加优化步骤**，不是 `start()` 的必要前提；未调用它时，远端请求仍然可以通过请求平面正常访问实例。

返回值语义：

- `Ok(self)`：允许继续链式配置，例如 `from_portname(...).register_local_engine(...)? .handler(...).start().await`；
- 该方法当前几乎不产生业务失败路径，`Result<Self>` 主要是为了与 Builder 链式 API 风格保持一致，并为未来注册失败场景预留接口形态。

**`start()` 执行顺序及理由**：

```
① `build_internal()?.dissolve()`
    → 取出 `portname`、`handler`、`metrics_labels`、`graceful_shutdown`、`health_check_payload`

② 读取 `connection_id` 与 `portname.id()`
    → 这是后续请求平面注册、健康检查注册、发现注册的统一实例身份来源

③ `handler.add_metrics(&portname, labels)`
    → 先把 PortName 维度的指标上下文挂到 handler 上

④ `port_shutdown_token = portname.drt().child_token()`
    → 创建当前 PortName 生命周期专属关闭令牌，后续 cleanup task 依赖它退出

⑤ 若 `graceful_shutdown = true`，则 `graceful_shutdown_tracker.register_portname()`
    → 运行时关闭时需要等待该 PortName 清空在途请求；否则只打 debug 日志

⑥ `server = portname.drt().request_plane_server().await?`
    → 懒初始化统一请求平面 server；HTTP/TCP/NATS 都从这里收口

⑦ 若配置了 `health_check_payload`：
    → 先 `build_transport_type(...)`
    → 构造当前实例 `Instance`
    → 写入 `SystemHealth` 的 health-check target
    → 若系统返回 notifier，则 `handler.set_portname_health_check_notifier(...)`

⑧ `server.register_portname(...)`
    → 把 handler 写入本地请求平面路由表；只有完成这一步，该实例才真正“可服务”

⑨ `tokio::spawn(cleanup_task)`
    → cleanup task 持有 `port_shutdown_token`、`server.clone()` 与可选 tracker
    → 等待 token 取消后执行：
       - `server.unregister_portname(...)`
       - 若启用了优雅关闭，则 `tracker.unregister_portname()`

⑩ `discovery.register(DiscoverySpec::PortName { ... })`
    → 把当前实例写入发现平面；失败时：
       - 记录 error
       - 主动取消 `port_shutdown_token`
       - 返回 “检查发现服务状态” 的错误

⑪ `task.await??`
    → 当前 `start()` future 不会在“注册完毕”时立即返回，而是持续等待 cleanup task 完成，
      即把 PortName 生命周期绑定到关闭令牌上
```

`start()` 的返回语义也要特别说明：它不是“启动完成即返回”的短生命周期初始化函数，而是一个**绑定服务生命周期的长活 async 过程**。只有当关闭令牌触发、cleanup task 执行完毕后，这个 future 才会结束。

更准确地说，源码里的关键顺序约束是：

- **请求平面注册必须先于发现注册**，否则会出现“发现得到、但本地尚不可服务”的假活实例；
- **health check target 注册发生在请求平面注册前**，这样 handler 一旦进入服务态，就已经具备健康检查联动能力；
- **cleanup task 必须在发现注册之前创建好**，确保后续任何失败路径都能通过取消 token 走同一套清理逻辑。

主要失败路径如下：

- `build_internal()` 失败：Builder 参数不完整或校验失败，`start()` 直接返回错误；
- `handler.add_metrics(...)` 失败：说明 handler 无法接受指标上下文，启动中止；
- `request_plane_server().await` 失败：请求平面未能初始化，启动中止；
- `build_transport_type(...)` 失败：动态端口尚不可用或地址构建失败，健康检查注册 / 发现注册中止；
- `server.register_portname(...)` 失败：实例未进入本地服务态，后续不会执行发现注册；
- `discovery.register(...)` 失败：会主动取消 `port_shutdown_token` 并返回错误，确保已创建的 cleanup task 负责反注册本地状态；
- `task.await??` 失败：说明 cleanup task 自身 panic 或返回错误，PortName 生命周期以异常结束。

---

### 3.8 `build_transport_type` — 传输地址构建函数

**来源**：`src/servicegroup/portname.rs`

**设计意图**：在将实例写入发现系统之前，统一构造 TCP / HTTP / NATS 地址描述；若端口由 OS 动态分配，则必须先保证请求平面 server 已经 bind 完成。

```rust
pub async fn build_transport_type(
    portname: &PortName,
    portname_id: &PortNameId,
    connection_id: u64,
) -> Result<TransportType>
```

参数语义：

- `portname`：不是为了读名字本身，而是为了拿到 `drt().request_plane()` 与 `request_plane_server()`；
- `portname_id`：提供 `namespace / servicegroup / portname` 三段身份，用于拼接最终地址；
- `connection_id`：实例级唯一标识，尤其在 TCP / NATS 路径中决定最终路由键。

这个函数在实现上可以分成两层：

1. `build_transport_type_inner(mode, portname_id, connection_id)`
     - 纯根据请求平面模式拼接地址；
2. `build_transport_type(...)`
     - 先判断当前模式是否需要等待 server 完成 bind，再调用 inner。

具体规则如下：

- **HTTP 模式**：
    - 读取 `PGD_HTTP_RPC_HOST` / `PGD_HTTP_RPC_PORT` / `PGD_HTTP_RPC_ROOT_PATH`（目标设计下对应 Pagoda 前缀环境变量）；
    - 若端口是固定非 0 值，直接使用；
    - 若端口未配置或为 `0`，则读取运行时记录的实际绑定端口；
    - 最终拼出 `http://host:port/root/portname` 形式的地址。

- **TCP 模式**：
    - 读取 `PGD_TCP_RPC_HOST` / `PGD_TCP_RPC_PORT`；
    - 若端口未固定，则同样读取运行时记录的实际绑定端口；
    - 地址格式包含 `connection_id` 的十六进制形式，保证多个 worker 共享一个 TCP server 时仍能正确路由；
    - 最终格式为 `host:port/{instance_id_hex}/{portname}`。

- **NATS 模式**：
    - 不依赖端口是否已 bind；
    - 直接构造实例级 subject，得到 `TransportType::Nats(...)`。

`build_transport_type(...)` 之所以是 `async fn`，不是因为字符串拼接本身异步，而是因为在 HTTP/TCP 使用 OS 动态分配端口时，它可能需要先 `await portname.drt().request_plane_server()`，确保 server 已完成绑定，随后才能得到正确的实际地址。

失败路径：

- HTTP/TCP 动态端口模式下，如果实际绑定端口尚不可读，会返回错误；
- 若 `request_plane_server().await` 本身失败，也会直接中止地址构建；
- NATS 路径通常最稳定，因为它不依赖本地监听 socket 先 bind 成功。

---

### 3.9 `build_nats_service` — NATS service 构建函数

**来源**：`src/servicegroup/service.rs`

**状态**：兼容层。Pagoda 的长期方向是弱化对 NATS service 的显式暴露，但当前仍保留这条路径兼容既有协议生态。

```rust
pub const PROJECT_NAME: &str = "Pagoda";
const SERVICE_VERSION: &str = env!("CARGO_PKG_VERSION");

pub async fn build_nats_service(
    nats_client: &crate::transports::nats::Client,
    servicegroup: &ServiceGroup,
    description: Option<String>,
) -> anyhow::Result<NatsService>
```

**执行流程**：

```
① servicegroup.service_name()
② description.unwrap_or("Pagoda servicegroup {name} in namespace {ns}")
③ nats_client.client().service_builder().start(...)
④ 返回 NatsService
```

---

### 3.10 `RoutingOccupancyState` — 路由占用状态（`pub(crate)`）

**来源**：`src/servicegroup/client.rs`

**设计意图**：`PowerOfTwo` 和 `LeastLoaded` 路由需要实时知道实例的 in-flight 请求数，这个状态必须在同一个 `PortName` 的全部 `Client` 与路由器实例之间共享。

```rust
#[derive(Debug, Default)]
pub(crate) struct RoutingOccupancyState {
    counts: DashMap<u64, AtomicU64>,
    exact_selection_lock: tokio::sync::Mutex<()>,
}
```

**自身方法**（均 `pub(crate)`）：

```rust
impl RoutingOccupancyState {
    fn increment(&self, instance_id: u64)
    // 请求发出时 +1；AtomicU64::fetch_add(Relaxed)，无锁

    async fn select_exact_min_and_increment(&self, instance_ids: &[u64]) -> Option<u64>
    // LeastLoaded 专用：加 exact_selection_lock → 找最小负载实例 → increment → 解锁
    // Mutex 串行化保证不会两个并发路由同选同一实例

    fn decrement(&self, instance_id: u64)
    // 请求完成时 -1（floor 0）；fetch_update(saturating_sub(1))

    fn load(&self, instance_id: u64) -> u64
    // 读取当前 in-flight 数；AtomicU64::load(Relaxed)，无锁

    fn retain(&self, instance_ids: &[u64])
    // 清除已下线实例的计数（monitor_instance_source 调用），防止内存泄漏
}
```

---

### 3.11 `get_or_create_routing_occupancy_state` — 进程内共享状态工厂

**来源**：`src/servicegroup/client.rs`

```rust
pub(crate) async fn get_or_create_routing_occupancy_state(
    portname: &PortName,
) -> Arc<RoutingOccupancyState>
```

设计要点：

- DRT 持有 `HashMap<PortName, Weak<RoutingOccupancyState>>`
- 优先升级已有 `Weak`
- 不存在时新建 `Arc` 并回填 `Weak`
- 所有消费者释放后自动 GC

---

### 3.12 `Client` — 服务发现与负载均衡客户端

**来源**：`src/servicegroup/client.rs`

**设计意图**：`Client` 负责维护某个 `Namespace / ServiceGroup / PortName` 下的动态实例视图，并基于这些视图支撑不同路由策略。

内部维护三套实例视图：

内部维护三套实例视图，并通过 `PortNameDiscoverySource` 提供逐事件广播：

| 视图字段 | 类型 | 内容 | 用途 |
|---------|------|------|------|
| `instance_source` | `watch::Receiver<Vec<Instance>>` | 权威实例列表 | `wait_for_instances()` / 地址解析 |
| `instance_avail` | `ArcSwap<Vec<u64>>` | 当前可路由实例 ID | 路由选择 |
| `instance_free` | `ArcSwap<Vec<u64>>` | 排除过载后的空闲实例 ID | 负载感知路由 |

`PortNameDiscoverySource` 包装了 `watch::Receiver<Vec<Instance>>` 并附带一个事件订阅列表，
使得上层消费者可通过 `subscribe_discovery_events()` 获得逐事件（Added/Removed）的无损广播，
而不仅仅是合并后的快照变化。这与标准实现的 `EndpointDiscoverySource` 模式完全一致。

```rust
#[derive(Clone, Debug)]
pub struct Client {
    pub portname:                PortName,
    portname_discovery_source:   Arc<PortNameDiscoverySource>,
    pub instance_source:         watch::Receiver<Vec<Instance>>,
    instance_avail:              Arc<ArcSwap<Vec<u64>>>,
    instance_free:               Arc<ArcSwap<Vec<u64>>>,
    instance_avail_tx:           Arc<tokio::sync::watch::Sender<Vec<u64>>>,
    instance_avail_rx:           tokio::sync::watch::Receiver<Vec<u64>>,
    reconcile_interval:          Duration,
}
```

**自身方法**：

```rust
impl Client {
    pub(crate) async fn new(portname: PortName) -> Result<Self>
    pub(crate) async fn with_reconcile_interval(portname: PortName, interval: Duration) -> Result<Self>

    pub fn instances(&self) -> Vec<Instance>
    pub fn instance_ids(&self) -> Vec<u64>
    pub fn instance_ids_avail(&self) -> arc_swap::Guard<Arc<Vec<u64>>>
    pub fn instance_ids_free(&self) -> arc_swap::Guard<Arc<Vec<u64>>>
    pub fn instance_avail_watcher(&self) -> tokio::sync::watch::Receiver<Vec<u64>>
    pub(crate) fn subscribe_discovery_events(&self)
        -> tokio::sync::mpsc::UnboundedReceiver<DiscoveryEvent>

    pub async fn wait_for_instances(&self) -> Result<Vec<Instance>>
    pub fn report_instance_down(&self, instance_id: u64)
    pub fn update_free_instances(&self, busy_instance_ids: &[u64])

    fn monitor_instance_source(&self)
    async fn get_or_create_dynamic_discovery_source(portname: &PortName)
        -> Result<Arc<PortNameDiscoverySource>>
}
```

#### `new(portname) -> Result<Self>`

这是默认构造入口，内部并不直接重复构造逻辑，而是委托给：

```rust
Self::with_reconcile_interval(portname, DEFAULT_RECONCILE_INTERVAL).await
```

也就是说，`new()` 的语义是“使用系统默认 reconcile 周期创建动态发现客户端”。它的职责只有两件事：

- 统一默认参数入口；
- 避免调用方手工关心 `DEFAULT_RECONCILE_INTERVAL` 常量。

#### `with_reconcile_interval(portname, interval) -> Result<Self>`

这是 `Client` 的真实构造核心。它完成的不是简单字段填充，而是四个关键动作：

1. 通过 `get_or_create_dynamic_discovery_source(&portname).await` 获取共享的 `Arc<PortNameDiscoverySource>`，同时从中派生出 `instance_source`（`discovery_source.instance_receiver()`）；
2. 从当前 `instance_source.borrow()` 快照里提取 `initial_ids`，用它初始化 `instance_avail` / `instance_free`；
3. 创建 `instance_avail_tx/rx` 这一条“可路由实例 ID 广播通道”；
4. 调用 `monitor_instance_source()` 启动后台同步任务。

这里最容易漏掉的设计点是**初始快照播种（seed）**：

- `wait_for_instances()` 读的是 `instance_source`；
- 实际路由热路径常读的是 `instance_avail`；
- 如果构造时不先把当前快照同步给 `instance_avail`，就会出现“调用方刚等到实例出现，但路由层仍看到空列表”的瞬时不一致。

因此 `with_reconcile_interval()` 的本质是：**把发现层 watch 源、路由热路径缓存、watch 广播通道和后台同步任务一次性接好**。

#### `instances() -> Vec<Instance>`

返回当前 `instance_source` 中保存的权威实例快照。这里直接克隆 `watch::Receiver<Vec<Instance>>` 里最新的一份 `Vec<Instance>`，因此：

- 它看到的是“发现系统当前认为存在的实例”；
- 不会反映 `report_instance_down()` 对 `instance_avail` 的临时抑制；
- 适合做地址解析、调试展示和控制面查询，而不是直接作为“当前可路由实例”集合。

#### `instance_ids() -> Vec<u64>`

这是 `instances()` 的轻量派生函数：只抽取当前权威实例列表中的 `instance_id`。

用途主要有两个：

- 在 `update_free_instances()` / `monitor_instance_source()` 中做集合计算；
- 让上层不必重复写 `instances().into_iter().map(|x| x.id()).collect()` 模板代码。

#### `instance_ids_avail() -> arc_swap::Guard<Arc<Vec<u64>>>`

返回当前**可路由实例 ID 列表**的无锁快照。这个列表：

- 以 `instance_source` 为基础；
- 会被 `report_instance_down()` 临时剔除某些实例；
- 会被 `monitor_instance_source()` 的更新/周期 reconcile 重置回权威快照。

它使用 `ArcSwap` 的意义是：路由热路径只做无锁读，不需要为每次选路去拿 `Mutex`。

#### `instance_ids_free() -> arc_swap::Guard<Arc<Vec<u64>>>`

返回当前“未过载 / 未标记 busy”的实例 ID 快照。与 `instance_ids_avail()` 的区别是：

- `instance_avail` 解决的是“这个实例是否应该继续被选路”；
- `instance_free` 解决的是“在已知实例里，哪些暂时没被 busy 阈值排除”。

这使客户端可以同时支撑：

- down/unhealthy 抑制；
- busy/overloaded 抑制；
- 两套抑制逻辑互不覆盖。

#### `instance_avail_watcher() -> watch::Receiver<Vec<u64>>`

返回 `instance_avail_rx.clone()`，供外部订阅“可路由实例 ID 列表”的变化。

注意这里暴露的是**watch receiver 的克隆**，不是内部 `ArcSwap` 本身。这样上层可以：

- 等待实例集合变化事件；
- 在变化后再读取新列表；
- 而不需要轮询 `instance_ids_avail()`。

#### `wait_for_instances() -> Result<Vec<Instance>>`

这是面向调用方的“阻塞直到至少有一个实例可见”接口。其行为是：

1. clone 一份 `instance_source` receiver；
2. 循环读取 `borrow_and_update()` 当前快照；
3. 如果为空，则 `rx.changed().await?` 挂起；
4. 如果非空，立刻返回这批实例。

这个函数读的是**权威实例源**，不是 `instance_avail`。这样设计是为了确保：只要发现层已经出现实例，调用方就能解除等待，而不会被本地 down/busy 缓存逻辑影响。

失败路径主要来自 `rx.changed().await?`：如果对应 sender 已关闭，说明底层发现源或 watch 管道已经终止，这时函数会直接返回错误。

#### `report_instance_down(instance_id)`

这个函数代表客户端本地观察到“某实例暂时不可用”。它不会改 discovery，只做**本地抑制**：

1. 从当前 `instance_ids_avail()` 中过滤掉指定 `instance_id`；
2. 把新列表写回 `instance_avail`；
3. 通过 `instance_avail_tx.send(filtered)` 通知订阅者；
4. 记录 debug 日志。

这意味着它的效果是**临时的**：后续只要 `monitor_instance_source()` 收到新发现快照，或者周期 reconcile 到点，就可能把该实例重新加入 `instance_avail`。

#### `update_free_instances(busy_instance_ids)`

这个函数负责更新“忙碌过滤”视图：

1. 读取全部权威实例 ID（`instance_ids()`）；
2. 过滤掉 `busy_instance_ids` 中的项；
3. 把剩余 ID 写入 `instance_free`。

它不触碰 `instance_avail`，因此 busy 抑制与 down 抑制分层管理。

#### `monitor_instance_source()`

这是 `Client` 构造后自动启动的后台同步任务。它负责把**发现层权威状态**持续投射到本地三类状态：

- `instance_avail`
- `instance_free`
- `RoutingOccupancyState`

其循环逻辑可以概括为：

1. 从 `instance_source` 当前快照提取最新 `instance_ids`；
2. 用这份快照重置 `instance_avail` 与 `instance_free`；
3. 若能拿到共享的 `RoutingOccupancyState`，则调用 `retain(&instance_ids)` 清理已消失实例的占用计数；
4. 用 `instance_avail_tx.send(instance_ids)` 向订阅者广播新快照；
5. `tokio::select!` 等待（**三路**）：
    - `rx.changed()`：说明发现源有新事件；
    - `sleep(reconcile_interval)`：说明该做周期 reconcile 了；
    - `cancel_token.cancelled()`：runtime 关闭，立即退出，无需等待下一次 changed/sleep 触发。

这里的 reconcile 设计非常关键：即使 discovery 一段时间没有任何新事件，被 `report_instance_down()` 临时剔除的实例也会在超时后重新回到 `instance_avail`，避免“本地永久误杀”。

退出条件：

- `cancel_token.cancelled()` arm 触发（runtime 关闭），直接 `break`；
- 或 `rx.changed()` 返回错误，说明 sender 已消失，此时会记录 error 并主动取消 runtime token，再 `break`。

#### `get_or_create_dynamic_discovery_source(portname) -> Result<Arc<PortNameDiscoverySource>>`

这是整个客户端共享发现 watch 的工厂函数。它的目标不是“每次创建一个新 watcher”，而是：**同一个 PortName 在同一进程内只维护一条 discovery watch 管道**。

完整流程如下：

1. 通过 `drt.instance_sources()` 拿到 `HashMap<PortName, Weak<PortNameDiscoverySource>>`；
2. 先查缓存：
    - 若 key 存在且 `Weak` 可升级，直接复用现有 `Arc<PortNameDiscoverySource>`；
    - 若 key 存在但已失效，则移除旧项；
3. 若缓存未命中，则构造新的发现源：
    - 组装 `DiscoveryQuery::PortName { namespace, servicegroup, portname }`；
    - `discovery.list_and_watch(...).await?` 获取事件流；
    - 创建 `watch_tx/watch_rx`，用 `watch_rx` 构造 `Arc<PortNameDiscoverySource>`；
    - 在次级 runtime 上 spawn `port_watcher` 后台任务；
4. `port_watcher` 维护 `HashMap<u64, Instance>`：
    - `DiscoveryEvent::Added(PortName(inst))` → 先调 `src.broadcast_event(&event)` 广播原始事件，再用 `send_modify` 就地更新快照（去重后追加）；
    - `DiscoveryEvent::Removed(did)` → 先广播，再 `send_modify` 移除对应 `instance_id`；
    - 若发现流报错、结束或 `watch_tx.closed()`，则退出并发送空列表；
5. 最后把 `Arc<PortNameDiscoverySource>` 以 `Weak` 形式回填缓存，再返回给调用方。

这一层缓存复用极其重要：如果每个 `Client` 都自己去 `list_and_watch()`，同一个 PortName 会在进程内制造重复控制面连接、重复事件反序列化和重复状态维护。

---

## 四、后台 Task 总览

| Task 名 | 创建位置 | 运行在 | 退出条件 |
|---------|---------|-------|---------|
| `port_watcher` | `get_or_create_dynamic_discovery_source` | 次级 runtime | `watch_tx.closed()` 或发现流结束 |
| `monitor_instance_source` | `Client::monitor_instance_source()` | 主 runtime | `cancel_token.cancelled()` 触发或 sender 丢失 |
| `cleanup_task` | `PortNameConfigBuilder::start()` | 主 runtime | `port_shutdown_token` 取消后执行一次清理 |

把 `port_watcher` 放到次级 runtime 的原因是：发现系统 watch 是长连接控制面任务，不应与业务请求 I/O 共用同一执行资源池。

---

## 五、并发数据结构选型

| 场景 | 数据结构 | 理由 |
|------|---------|------|
| `servicegroup_cache`（Namespace） | `Arc<DashMap<String, ServiceGroup>>` | 同步接口 + 幂等缓存 |
| `instance_avail` / `instance_free`（Client） | `Arc<ArcSwap<Vec<u64>>>` | 路由热路径无锁读 |
| `instance_source`（Client） | `tokio::sync::watch::Receiver` | 多消费者订阅最新实例视图 |
| `instance_avail_tx/rx`（Client） | `tokio::sync::watch::channel` | 广播实例变化 |
| `counts`（RoutingOccupancyState） | `DashMap<u64, AtomicU64>` | per-instance 并发计数 |
| `exact_selection_lock`（RoutingOccupancyState） | `tokio::sync::Mutex<()>` | 原子化最小负载选择 |
| `routing_occupancy_states`（DRT） | `Arc<Mutex<HashMap<PortName, Weak<RoutingOccupancyState>>>>` | 共享状态工厂 |
| `inner`（Registry） | `Arc<tokio::sync::Mutex<RegistryInner>>` | 协议 service 注册需要 await |

---

## 六、设计决策

### D-01：`servicegroup()` 保持同步，用 `DashMap` 做幂等缓存

**问题**：同名 `ServiceGroup` 必须在进程内唯一，否则同名指标会重复注册，导致注册表冲突。

**决策**：使用 `servicegroup_cache: Arc<DashMap<String, ServiceGroup>>`。读路径无锁，写路径幂等，接口保持同步。

---

### D-02：协议 service 注册在同步构造流程中等待完成

**问题**：`servicegroup()` 是同步接口，而 NATS service 注册是异步的。若不等待注册完成，后续 `PortName` 启动可能在协议层尚未就绪时失败。

**决策**：在次级 runtime 上 spawn 注册任务，通过单元素 channel 回传结果，再用 `block_in_place` 同步等待。

---

### D-03：`instance_avail` 使用 `ArcSwap` 而不是 `RwLock`

**问题**：路由热路径每个请求都要读取当前可用实例集，必须尽量做到无锁读。

**决策**：使用 `ArcSwap<Vec<u64>>` 保存当前实例快照。读路径是原子指针 load，写路径低频替换新的 `Arc<Vec<u64>>`。

---

### D-04：发现观察任务运行在次级 runtime

**问题**：发现系统 watch 是长期 I/O 任务，会和业务请求竞争主 runtime 的执行资源。

**决策**：把 `port_watcher` 放到次级 runtime，只把轻量快照同步保留在主 runtime。

---

### D-05：`RoutingOccupancyState` 通过 DRT WeakMap 进程内共享

**问题**：同一个 `PortName` 的多个 `Client` / 路由器实例若各自维护占用状态，会看到割裂的负载视图。

**决策**：DRT 持有 `HashMap<PortName, Weak<RoutingOccupancyState>>`，保证同一 `PortName` 下共享同一份状态，并在无人使用时自动回收。

---

### D-06：本地处理器注册必须先于发现注册

**问题**：如果实例先进入发现系统，而本地 `PortName` handler 尚未写入路由表，远端请求会打到一个“可发现但不可服务”的实例上。

**决策**：严格保证 `server.register_portname()` 先于 `discovery.register()`。

---

## 七、辅助函数

```rust
fn validate_allowed_chars(input: &str) -> Result<(), ValidationError>
// 私有，定义在 servicegroup.rs 中
// 每次调用时构造正则：^[a-z0-9-_]+$
// regex.is_match(input) 为 true → Ok(())
// 否则 → Err(ValidationError::new("invalid_characters"))
// 用于 Namespace.name / ServiceGroup.name / PortName.name 的字符集校验
```

---

## 八、模块依赖

```
servicegroup 使用：
  crate::distributed::DistributedRuntime   — 基础设施入口
  crate::discovery::{Discovery, DiscoveryEvent, DiscoveryInstance, DiscoveryQuery, DiscoverySpec}
  crate::pipeline::{AsyncEngine, PushRouter, AddressedPushRouter, RouterMode, PushWorkHandler}
  crate::metrics::{MetricsHierarchy, MetricsRegistry}
    crate::protocols::PortNameId
  crate::traits::{DistributedRuntimeProvider, RuntimeProvider}
  arc_swap::ArcSwap
  dashmap::DashMap
  derive_builder::Builder
  validator::Validate
  educe::Educe
  derive_getters::Dissolve

servicegroup 被使用：
    distributed.rs                  — 提供 namespace() 入口；持有 instance_sources / routing_occupancy_states / registry
  pipeline/egress/push_router.rs  — 使用 Client、RoutingOccupancyState、get_or_create_routing_occupancy_state
    pipeline/ingress/               — 通过 portname_builder() 注册 PortName 处理器
    所有业务 crate                   — 通过 namespace→servicegroup→portname 链声明和调用服务
```
