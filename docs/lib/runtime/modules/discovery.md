# `discovery` 模块设计

**源码**：`src/discovery/mod.rs` · `src/discovery/metadata.rs` · `src/discovery/mock.rs` · `src/discovery/utils.rs` · `src/discovery/kube.rs` · `src/discovery/kube/service_registry.rs` · `src/discovery/kube/objects.rs` · `src/discovery/kube/daemon.rs` · `src/discovery/kube/utils.rs`

---

## 一、模块定位

`discovery` 模块解决分布式系统中最根本的问题：**服务实例如何相互找到彼此**。具体来说，它解决三类信息的发布与订阅：

- **PortName 服务实例**（PortName）：某个工作进程对外暴露的可达入口，供调用方建立连接
- **模型部署卡**（ModelCard）：某个 `PortName` 上当前加载的模型信息，供路由层做模型感知调度；模型实例同时携带 `topo_json`
- **事件平面通道**（EventChannel）：某个 `ServiceGroup` 发布事件的 `NATS` subject 或 `ZMQ` endpoint，供订阅方接入

发现系统由三个核心概念构成：

**注册（Registration）**：工作进程在启动时向发现后端写入自己的信息，并在关闭时删除。注册成功后返回 `DiscoveryInstance`，持有者可用它执行注销。

**查询（Query/List）**：调用方向发现后端查询符合 `DiscoveryQuery` 范围的所有当前实例，得到快照列表。

**监听（Watch）**：调用方建立一个持续的 `DiscoveryStream`，接收 `Added` / `Removed` 事件，始终维护最新视图。

---

### 后端策略

`discovery` 通过 `Discovery` trait 定义统一抽象，但最终正式版本只保留 **k8s 原生资源存储路径**。也就是说，发现状态只写入 `Service` / `EndpointSlice` / `ConfigMap` / `Lease`；`MockDiscovery` 仅作为测试替身，不属于正式存储后端：


| 实现                    | 环境         | 存储                                       | 适用场景                   |
| --------------------- | ---------- | ---------------------------------------- | ---------------------- |
| `KubeDiscoveryClient` | k8s 环境 | `Service` / `EndpointSlice` / `ConfigMap` / `Lease` | 唯一正式发现后端 |
| `MockDiscovery`       | 测试     | 进程内 `Vec<DiscoveryInstance>`             | 单元测试、集成测试 |


---

## 二、文件结构与可见性

```
src/discovery/
  ├── mod.rs           — pub 入口；所有公共类型；Discovery trait；模型名冲突检测逻辑
  ├── metadata.rs      — pub DiscoveryMetadata / MetadataSnapshot；null-default 反序列化修复
  ├── mock.rs          — pub MockDiscovery / SharedMockRegistry；轮询式 watch 实现
  ├── utils.rs         — pub watch_and_extract_field；通用流转化工具
        ├── kube.rs          — pub KubeDiscoveryClient；Discovery trait 的 k8s 实现
    └── kube/
            ├── service_registry.rs — pub ServiceRegistration；Service / EndpointSlice 注册工具
            ├── objects.rs          — 对象映射：PortName → Service/EndpointSlice，Model（承载 ModelCard）→ ConfigMap，EventChannel → Lease
            ├── daemon.rs           — pub(super) DiscoveryDaemon；多 reflector 聚合守护进程
            └── utils.rs            — pub hash_pod_name；pub(super) PodInfo / extract_endpoint_info
```

---

## 三、类型详解

---

### 3.1 `EventTransportKind` — 事件平面传输类型标识

**来源**：`src/discovery/mod.rs`

**设计意图**：事件平面与请求平面物理上相互独立——前者是 pub/sub 广播模型（NATS subject 或 ZMQ socket），后者是点对点 RPC 模型。`EventTransportKind` 是一个轻量枚举，仅表示"用哪种传输协议"，不携带连接地址，因此可以被廉价地 `Copy`、用作 `HashMap` 键、写入环境变量配置。当需要连接地址时则使用 `EventTransport`（见 3.3 节）。

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EventTransportKind {
    #[default]
    Nats,   // NATS Core pub/sub，默认值；JSON 友好，集群共享 broker
    Zmq,    // ZMQ pub/sub；低延迟，适合高频 KV 事件广播
}
```

**实现的 trait**：


| Trait                       | 来源                    | 实现细节                                                    |
| --------------------------- | --------------------- | ------------------------------------------------------- |
| `Clone` / `Copy`            | derive                | 枚举无堆数据，可直接按值传递                                          |
| `PartialEq` / `Eq`          | derive                | 变体名相等即相等，用于条件判断                                         |
| `Hash`                      | derive                | 可作为 `HashMap` / `HashSet` 的键                            |
| `Serialize` / `Deserialize` | derive                | serde `rename_all = "snake_case"` 输出 `"nats"` / `"zmq"` |
| `Default`                   | derive + `#[default]` | 默认值为 `Nats`，与环境变量未设置时行为一致                               |


**自身方法**：

```rust
impl EventTransportKind {
    pub fn from_env() -> Result<Self>
    pub fn from_env_or_default() -> Self
    pub fn default_codec(&self) -> EventCodecKind
}
```

- `**from_env()**`：读取环境变量 `PGD_EVENT_PLANE`。值为 `"nats"` / `""` / 未设置时返回 `Nats`；值为 `"zmq"` 返回 `Zmq`；其他值返回 `Err`，错误消息中列出合法值。
- `**from_env_or_default()**`：调用 `from_env()`，出错时打印 warn 日志并返回 `Nats`，供不需要显式错误处理的初始化路径使用。
- `**default_codec()**`：为每种传输类型返回合理的默认序列化格式——`Nats` → `Json`（便于调试和与 NATS tooling 集成），`Zmq` → `Msgpack`（ZMQ 场景更关注性能，二进制格式节省带宽）。

---

### 3.2 `EventCodecKind` — 事件平面序列化格式

**来源**：`src/discovery/mod.rs`

**设计意图**：事件的序列化格式与传输协议正交——理论上 NATS 也可以用 Msgpack，ZMQ 也可以用 JSON。`EventCodecKind` 独立于 `EventTransportKind` 存在，允许运维人员通过环境变量覆盖默认值，在调试模式下将 ZMQ 事件切换为可读的 JSON，或在 NATS 场景下追求性能切换为 Msgpack。

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventCodecKind {
    Json,     // 人可读；适合调试和与 NATS CLI 工具集成
    Msgpack,  // 紧凑二进制；ZMQ 高频场景的默认选择
}
```

**实现的 trait**：同 `EventTransportKind`（`Clone/Copy/PartialEq/Eq/Hash/Serialize/Deserialize`）。此枚举无 `Default`，因为"无默认"本身就是它的默认——调用 `from_env()` 返回 `Option<Self>`，表示未配置，由调用者按传输类型决定。

**自身方法**：

```rust
impl EventCodecKind {
    pub fn from_env() -> Result<Option<Self>>
    pub fn from_env_or_transport_default(transport: EventTransportKind) -> Self
}
```

- `**from_env()**`：读取 `PGD_EVENT_PLANE_CODEC`。未设置 / 空值返回 `Ok(None)`（None 表示未明确配置，由传输类型决定默认值）；`"json"` → `Some(Json)`；`"msgpack"` → `Some(Msgpack)`；无效值返回 `Err`。
- `**from_env_or_transport_default(transport)**`：将 `from_env()` 的 `Option` 结果与 `transport.default_codec()` 组合。出错时用 transport 默认值并打印 warn；`None` 时取 transport 默认值；`Some(v)` 时直接返回 `v`。

---

### 3.3 `EventTransport` — 事件平面完整传输配置

**来源**：`src/discovery/mod.rs`

**设计意图**：调用方不仅需要知道"用哪种传输"，还需要知道"连接地址是什么"。`EventTransport` 将传输类型与连接参数封装在同一个结构里，可被序列化写入发现后端，供订阅方反序列化后直接建立连接，不需要额外查询。注意它有意区别于 `servicegroup::TransportType`（请求平面），二者语义不同：请求平面是点对点 RPC，事件平面是 pub/sub 广播。

`ZmqBroker` 变体支持 broker 模式部署——此时发布方连接 XSUB，订阅方连接 XPUB，中间 broker 做消息转发，实现发布方与订阅方的完全解耦。

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", content = "config")]
pub enum EventTransport {
    Nats {
        subject_prefix: String, // NATS subject 前缀，如 "mynamespace.pagoda.myservicegroup.backend"
    },
    Zmq {
        endpoint: String,       // ZMQ 直连地址，如 "tcp://host:5555"（直连模式）
    },
    ZmqBroker {
        xsub_endpoints: Vec<String>, // 发布方连接的 XSUB 端点（broker 暴露给 publisher）
        xpub_endpoints: Vec<String>, // 订阅方连接的 XPUB 端点（broker 暴露给 subscriber）
    },
}
```

**实现的 trait**：


| Trait                       | 来源     | 实现细节                                                                                             |
| --------------------------- | ------ | ------------------------------------------------------------------------------------------------ |
| `Clone`                     | derive | 克隆字符串内容，NATS subject 通常很短                                                                        |
| `PartialEq` / `Eq` / `Hash` | derive | 地址相等则相等；可作 HashMap 键进行去重                                                                         |
| `Serialize` / `Deserialize` | derive | `serde(tag = "kind", content = "config")` 输出 `{"kind":"Nats","config":{"subject_prefix":"..."}}` |


**自身方法**：

```rust
impl EventTransport {
    pub fn kind(&self) -> EventTransportKind     // 提取变体对应的 Kind，避免二次 match
    pub fn nats(subject_prefix: impl Into<String>) -> Self  // 便利构造 Nats 变体
    pub fn zmq(endpoint: impl Into<String>) -> Self         // 便利构造 Zmq 变体
    pub fn address(&self) -> &str
    // 返回主要地址字符串：Nats → subject_prefix；Zmq → endpoint；
    // ZmqBroker → 第一个 xsub 端点（无端点时返回 ""）
}
```

---

### 3.4 `DiscoveryQuery` — 层级范围查询键

**来源**：`src/discovery/mod.rs`

**设计意图**：服务发现的查询往往有层级性——有时需要"全部 `PortName`"，有时只需要"某命名空间的模型"，有时精确到"某个 `PortName` 下所有实例"。`DiscoveryQuery` 将这种层级关系编码为枚举，每个变体携带的字段精确描述查询范围。后端实现可将其转换为 KV 前缀字符串（`v1/instances/ns/sg/pn`），高效利用兼容 KV 扫描或 k8s 原生 watch filter。

`PortName` 查询与模型查询的层级结构完全对称（`All / Namespaced / ServiceGroup / PortName`），但由于二者存储在不同后端对象中，需要分别定义。事件通道查询独立为 `EventChannels(EventChannelQuery)` 变体，通过内嵌 `EventChannelQuery` 实现 `Option<String>` 形式的可选过滤，比静态枚举更灵活。

```rust
pub enum DiscoveryQuery {
    // ── PortName（RPC 服务可达地址）────────────────────────────────────
    AllPortNames,
    NamespacedPortNames   { namespace: String },
    ServiceGroupPortNames { namespace: String, servicegroup: String },
    PortName              { namespace: String, servicegroup: String, portname: String },

    // ── 模型部署卡（模型加载状态）──────────────────────────────────────
    AllModels,
    NamespacedModels    { namespace: String },
    ServiceGroupModels  { namespace: String, servicegroup: String },
    PortNameModels      { namespace: String, servicegroup: String, portname: String },

    // ── 事件通道（pub/sub 地址）─────────────────────────────────────────
    EventChannels(EventChannelQuery), // 通过内嵌结构支持可选层级过滤
}
```

---

### 3.5 `EventChannelQuery` — 事件通道可选层级过滤

**来源**：`src/discovery/mod.rs`

**设计意图**：与端点/模型的静态层级枚举不同，事件通道查询需要在"全部"到"精确 topic"之间平滑过渡，且字段可选性更高。用 `Option<String>` 表示每一层是否指定，比定义 `AllEventChannels / NamespacedEventChannels / ...` 四个变体更简洁，也方便未来扩展新的过滤维度。

```rust
pub struct EventChannelQuery {
    pub namespace: Option<String>,  // None = 不限命名空间；Some = 精确匹配
    pub servicegroup: Option<String>,  // None = 不限服务组；Some = 精确匹配（namespace 需有意义）
    pub topic:     Option<String>,  // None = 不限 topic；Some = 精确匹配（前两者需有意义）
}
```

**自身方法**：

```rust
impl EventChannelQuery {
    pub fn all() -> Self                                                              // 无过滤
    pub fn namespace(ns: impl Into<String>) -> Self                                   // 限命名空间
    pub fn servicegroup(ns: impl Into<String>, sg: impl Into<String>) -> Self         // 限服务组
    pub fn topic(ns, sg, topic: impl Into<String>) -> Self                            // 精确 topic
    pub fn scope_level(&self) -> u8    // 返回 0-3，表示当前有效过滤层数
}
```

`scope_level()` 是一个诊断辅助方法，让调用方无需逐字段检查即可知道查询的"精度"：0 = 全局，1 = 命名空间，2 = 组件，3 = topic。

---

### 3.6 `DiscoverySpec` — 注册意图描述（`register` 的输入）

**来源**：`src/discovery/mod.rs`

**设计意图**：注册操作的入参需要根据注册类型携带不同字段，但又要通过同一个 `register()` 接口传递。`DiscoverySpec` 是一个类型安全的枚举，每个变体只包含对应类型所需的字段，避免了传入通用 map 时的运行时字段缺失问题。

`Model` 变体承载的是 `ModelCard` 语义。其 `card_json: serde_json::Value` 设计尤其值得关注：`lib/runtime` 本身不依赖 `lib/llm`（避免循环依赖），因此无法直接持有 `ModelDeploymentCard` 类型。通过存储已序列化的 JSON 值，runtime 层可以透明地传递和存储 LLM 层的领域对象，消费方按需反序列化（`DiscoveryInstance::deserialize_model::<T>()`）。`model_suffix` 支持 LoRA adapter 注册——同一个 `instance_id` 下可注册多个 LoRA，每个 LoRA 的路径追加唯一后缀；同时模型实例额外携带 `topo_json`，为拓扑感知路由提供统一输入。

```rust
pub enum DiscoverySpec {
    PortName {
        namespace:  String,
        servicegroup: String,
        portname:   String,
        transport:  TransportType,  // 消费方如何连接（NATS / HTTP / TCP）
    },
    Model {
        namespace:     String,
        servicegroup:  String,
        portname:      String,
        card_json:     serde_json::Value,  // 已序列化的 ModelDeploymentCard，解耦 lib/llm
        model_suffix:  Option<String>,     // None = 基础模型；Some(slug) = LoRA adapter
        topo_json:     serde_json::Value,  // 模型实例拓扑属性，供 NUMA/rack 感知路由
    },
    EventChannel {
        namespace:  String,
        servicegroup: String,
        topic:      String,         // 通道名，如 "kv-events"、"kv-metrics"
        transport:  EventTransport, // 消费方如何订阅（NATS subject 或 ZMQ 地址）
    },
}
```

**自身方法**：

```rust
impl DiscoverySpec {
    pub fn from_model<T: Serialize>(ns, comp, ep, card: &T) -> Result<Self>
    pub fn from_model_with_suffix<T: Serialize>(ns, comp, ep, card: &T, suffix: Option<String>) -> Result<Self>
    pub fn with_instance_id(self, instance_id: u64) -> DiscoveryInstance
}
```

- `**from_model()**` / `**from_model_with_suffix()**`：调用 `serde_json::to_value(card)` 将任意可序列化类型转为 `serde_json::Value`，再构造 `Model` 变体；这里的 `Model` 实际承载 `ModelCard` 数据。这是跨越 lib/runtime ↔ lib/llm 边界的序列化桥接点。
- `**with_instance_id()**`：将 `DiscoverySpec`（意图）转化为 `DiscoveryInstance`（已注册实例），附加 `instance_id`，由各后端的 `register_internal()` 在决定 ID 后调用。

---

### 3.7 `DiscoveryInstance` — 已注册实例（`register` 的输出 / watch 事件携带的数据）

**来源**：`src/discovery/mod.rs`

**设计意图**：注册成功后需要返回一个"句柄"，持有者可以用它执行注销，也可以在 watch 流中作为 `Added` 事件的载荷传递给消费方。`DiscoveryInstance` 和 `DiscoverySpec` 的变体一一对应，区别在于 `instance_id` 被嵌入，并且实现了 `Serialize/Deserialize`（spec 不需要序列化，instance 需要存储）。

`PortName` 变体直接包裹 `crate::servicegroup::Instance`，而不是展开字段，是因为 `Instance` 在 client 路由层被直接使用，保持同一类型减少转换层。`Model` 和 `EventChannel` 则直接展开字段，其中 `Model` 实际表示一个 `ModelCard` 实例，并同样保留 `topo_json`。

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum DiscoveryInstance {
    PortName(crate::servicegroup::Instance),   // 直接复用 servicegroup::Instance 类型
    Model {
        namespace:    String,
        servicegroup: String,
        portname:     String,
        instance_id:  u64,
        card_json:    serde_json::Value,     // ModelDeploymentCard 的 JSON 表示
        topo_json:    serde_json::Value,     // 模型实例拓扑属性

        #[serde(default, skip_serializing_if = "Option::is_none")]
        model_suffix: Option<String>,        // LoRA adapter 路径后缀
    },
    EventChannel {
        namespace:    String,
        servicegroup: String,
        topic:        String,
        instance_id:  u64,
        transport:    EventTransport,        // 订阅方需要的连接信息
    },
}
```

**实现的 trait**：


| Trait                       | 来源     | 实现细节                                                               |
| --------------------------- | ------ | ------------------------------------------------------------------ |
| `Clone`                     | derive | `card_json`（`serde_json::Value`）内部用 Arc 表示，克隆代价较低                  |
| `PartialEq` / `Eq`          | derive | 全字段比较，用于测试断言和 diff 去重                                              |
| `Serialize` / `Deserialize` | derive | `serde(tag = "type")` 输出 `{"type":"PortName",...}` 等，KV store 按此存取 |


**自身方法**：

```rust
impl DiscoveryInstance {
    pub fn instance_id(&self) -> u64
    pub fn deserialize_model<T: for<'de> Deserialize<'de>>(&self) -> Result<T>
    pub fn id(&self) -> DiscoveryInstanceId
}
```

- `**instance_id()**`：从任意变体提取 `instance_id`，路由层用此字段做负载均衡键。
- `**deserialize_model::<T>()**`：仅对 `Model` 变体有效，将 `card_json` 反序列化为调用方指定的类型（通常是 `lib/llm` 的 `ModelDeploymentCard`）。对其他变体返回 `Err`，防止误用。
- `**id()**`：将 instance 转为 `DiscoveryInstanceId`，提取出所有标识字段但不携带数据部分（`card_json` / `transport`），用于 diff 计算和 `Removed` 事件构造。

---

### 3.8 `PortNameInstanceId` / `ModelCardInstanceId` / `EventChannelInstanceId` — 实例唯一标识

**来源**：`src/discovery/mod.rs`

**设计意图**：watch 流的 `Removed` 事件只需要知道"谁被删除了"，不需要删除时的全部数据（transport 地址、card_json 等）。将标识信息独立成这三个轻量结构体，一方面减少 `Removed` 事件的内存占用，另一方面允许从 KV delete 事件的键路径重建 ID（etcd 删除事件不携带 value，只有 key）。

`to_path()` 方法定义了每类对象在兼容 KV 存储中的相对路径，路径格式统一为 `{namespace}/{servicegroup}/{name}/{instance_id:x}`（十六进制 instance_id），LoRA 模型追加 `/{model_suffix}`。

```rust
pub struct PortNameInstanceId {
    pub namespace:   String,
    pub servicegroup:String,
    pub portname:    String,
    pub instance_id: u64,   // 十六进制写入路径，如 "a1b2c3d4"
}

pub struct ModelCardInstanceId {
    pub namespace:    String,
    pub servicegroup: String,
    pub portname:     String,
    pub instance_id:  u64,
    pub model_suffix: Option<String>,  // None = 基础模型；Some(slug) = LoRA adapter
}

pub struct EventChannelInstanceId {
    pub namespace:   String,
    pub servicegroup:String,
    pub topic:       String,
    pub instance_id: u64,
}
```

**实现的 trait**（三个结构体相同）：


| Trait                       | 来源     | 实现细节                                       |
| --------------------------- | ------ | ------------------------------------------ |
| `Clone`                     | derive | 字段均为 `String` + `u64`，深拷贝                  |
| `PartialEq` / `Eq` / `Hash` | derive | 全字段相等；作为 `HashSet<DiscoveryInstanceId>` 成员 |
| `Serialize` / `Deserialize` | derive | 支持跨进程序列化，在协议层表示 Removed 事件                 |


**自身方法**（以 `PortNameInstanceId` 为例，其他类型对称）：

```rust
impl PortNameInstanceId {
    pub fn to_path(&self) -> String
    // → "{namespace}/{servicegroup}/{portname}/{instance_id:x}"（instance_id 16 进制）

    pub fn from_path(path: &str) -> Result<Self>
    // path 按 '/' 分割为 4 段；segments[3] 用 from_str_radix(16) 解析 instance_id
    // 段数不为 4 时返回 Err（保护 KV delete 事件的键解析路径）
}
```

`ModelCardInstanceId::to_path()` 在 `model_suffix` 存在时追加第 5 段，因此 `from_path()` 接受 4 或 5 段路径。LoRA 删除事件的键路径带有后缀，反序列化时自动填充 `model_suffix`。

---

### 3.9 `DiscoveryInstanceId` — 三类实例标识的联合枚举

**来源**：`src/discovery/mod.rs`

**设计意图**：`DiscoveryEvent::Removed` 需要一个统一类型携带被删除实例的 ID，但三类实例的 ID 字段不同（`portname` vs `topic` vs `model_suffix`），无法共用同一个结构体。`DiscoveryInstanceId` 是三类 ID 的 sum type，watch 流的消费方通过 match 或 `extract_*_id()` 方法取出具体变体。

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DiscoveryInstanceId {
    PortName(PortNameInstanceId),
    Model(ModelCardInstanceId),
    EventChannel(EventChannelInstanceId),
}
```

**自身方法**：

```rust
impl DiscoveryInstanceId {
    pub fn instance_id(&self) -> u64
    // 跨变体提取数值 ID，路由层可在不 match 变体的情况下获取数字 ID

    pub fn extract_portname_id(&self) -> Result<&PortNameInstanceId>
    pub fn extract_model_id(&self) -> Result<&ModelCardInstanceId>
    pub fn extract_event_channel_id(&self) -> Result<&EventChannelInstanceId>
    // 类型安全提取：变体不匹配时返回包含实际变体名的错误消息，便于调试
}
```

---

### 3.10 `DiscoveryEvent` / `DiscoveryStream` — watch 流的事件与流类型

**来源**：`src/discovery/mod.rs`

**设计意图**：watch 操作返回一个 `Stream`，而非回调或 channel，是因为 Stream 可以直接被 tokio 的 `StreamExt::next().await` 消费，与 Rust 异步生态无缝集成，且背压（backpressure）由调用方的消费速度自然控制。`DiscoveryEvent` 只有 `Added` 和 `Removed` 两种，没有 `Updated`——更新语义由"删除旧实例 + 添加新实例"表达，简化了消费方的状态机。

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiscoveryEvent {
    Added(DiscoveryInstance),       // 新实例出现；携带完整数据（供消费方立即使用）
    Removed(DiscoveryInstanceId),   // 实例消失；只携带 ID（数据已不存在）
}

pub type DiscoveryStream = Pin<Box<dyn Stream<Item = Result<DiscoveryEvent>> + Send>>;
// Pin<Box<...>>：类型擦除，三种后端实现都返回同一类型
// Stream<Item = Result<...>>：允许流中间产生错误（网络故障等），消费方可决定是否中止
// + Send：可跨 tokio 任务边界传递
```

---

### 3.11 `ModelRegistrationIdentity` / `extract_model_registration_identity` / `find_conflicting_model_name` — 模型卡注册名冲突检测工具

**来源**：`src/discovery/mod.rs`（私有，仅供 `Discovery::register()` 默认实现调用）

**设计意图**：`Discovery::register()` 的默认实现需要判断"即将注册的模型卡"与"同 `PortName` 已有模型卡"是否兼容。这个判断不能简单地比较 `display_name` 字符串，因为 LoRA adapter 的 `display_name` 是 adapter 自身的名字（不同 adapter 名字各异），不能以它作为冲突判断依据；而基础模型卡才以 `display_name` 作为唯一标识。实现命名上这里仍叫 `ModelRegistrationIdentity`，但它处理的是 `ModelCard` 数据。三个私有工具共同完成"从 JSON 提取身份 → 与已有实例对比 → 找出冲突名"的完整流程，从 trait 默认实现中分离出这段逻辑，使各后端无需重复实现。

`**ModelRegistrationIdentity`**：

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
struct ModelRegistrationIdentity {
    display_name: String,         // 模型显示名；基础模型的兼容键
    source_path:  Option<String>, // 基底模型路径；LoRA adapter 的兼容键（来自 card_json["source_path"]）
    is_lora:      bool,           // true = 此注册为 LoRA adapter（model_suffix 非空 或 card_json["lora"] 非 null）
}
```

`base_identity()` 辅助方法返回 `source_path.as_deref().unwrap_or(&display_name)`——有 `source_path` 时以基底模型路径作为兼容键，没有时退回 `display_name`。

`is_compatible_with()` 实现两套规则：

- **LoRA 场景**（`self.is_lora || other.is_lora`）：以 `base_identity()` 为兼容键。不同 LoRA adapter（名字不同）可以共存于同一 `PortName`，只要它们依附于同一基底模型（`source_path` 相同）。若其中一方无 `source_path`，则退回 `display_name` 比较，兜底防止同名冲突。
- **基础模型场景**（双方均非 LoRA）：以 `display_name` 为兼容键。同名模型允许多实例（横向扩容场景）；不同名模型禁止共存（路由层无法确定应路由到哪个模型，必须报错）。

`**extract_model_registration_identity(card_json, model_suffix)`**：

从 `DiscoverySpec::Model` 的 `card_json` 中提取 `ModelRegistrationIdentity`。

- `display_name` 是必需字段：从 `card_json["display_name"]` 读取；字段缺失时返回 `Err`，保护注册流程拒绝格式不完整的模型卡，而不是写入一个无法被正确比较的条目。
- `source_path` 从 `card_json["source_path"]` 读取，可选，不存在则为 `None`。
- `is_lora` 判断条件：`model_suffix` 参数非 `None` 且非空字符串，**或** `card_json["lora"]` 字段存在且非 `null`——两个条件任一满足即视为 LoRA 注册。

`**find_conflicting_model_name(instances, requested_identity)`**：

遍历 `instances`（同 `PortName` 已有的 `DiscoveryInstance` 列表），对每个 `Model` 变体提取其 `ModelRegistrationIdentity`（提取失败则以 `?` 向上传播错误，保护调用方不接受损坏数据），随后用 `!requested_identity.is_compatible_with(&existing)` 判断是否冲突。返回**第一个**冲突实例的 `display_name`（`Ok(Some(name))`），全部兼容时返回 `Ok(None)`。调用方（`register()` 默认实现）在收到 `Some(name)` 时构造包含冲突模型卡名和 `PortName` 路径的错误消息，便于运维人员定位问题。

---

### 3.12 `Discovery` trait — 发现后端的统一抽象契约

**来源**：`src/discovery/mod.rs`

**设计意图**：正式发现路径只有 k8s 实现，但 `Discovery` trait 仍然有价值：它把生产实现与测试替身统一到同一接口下，使上层代码（`DistributedRuntime`、`servicegroup` 对外服务入口、路由层）无需感知当前拿到的是生产发现后端还是测试替身。

trait 的关键设计是 `register()` 与 `register_internal()` 的分离：`register()` 是带模型名冲突检测的**默认实现**，`register_internal()` 是后端必须实现的**钩子**。这样每个后端只需实现原子写入逻辑，冲突检测逻辑在 trait 层统一维护，避免三个后端各自实现相同的检测逻辑。

```rust
#[async_trait]
pub trait Discovery: Send + Sync {
    fn instance_id(&self) -> u64;
    // 当前后端分配给本进程的唯一 ID（pod name 哈希 / 测试计数器）

    async fn register(&self, spec: DiscoverySpec) -> Result<DiscoveryInstance>;
    // 默认实现：含 Model 注册名冲突检测（见下文）；非 Model 直接转发 register_internal

    async fn register_internal(&self, spec: DiscoverySpec) -> Result<DiscoveryInstance>;
    // 后端必须实现：原子写入存储，不含冲突检测

    async fn unregister(&self, instance: DiscoveryInstance) -> Result<()>;
    // 从存储中删除；k8s 后端删除/更新对应对象，Mock 从 Vec 中 retain

    async fn list(&self, query: DiscoveryQuery) -> Result<Vec<DiscoveryInstance>>;
    // 一次性快照查询；返回所有当前匹配的实例

    async fn list_and_watch(
        &self,
        query: DiscoveryQuery,
        cancel_token: Option<CancellationToken>,
    ) -> Result<DiscoveryStream>;
    // 流式订阅：先发出当前所有实例的 Added 事件，再持续推送增量变化

    fn shutdown(&self) {}
    // 可选：k8s 后端在关闭时主动撤销自身注册，测试替身可忽略
}
```

**模型卡名冲突检测逻辑**（`register()` 默认实现）：

非 `Model` 类型的注册直接透传 `register_internal()`。`Model` 类型注册流程如下：

1. 从 `card_json` 中提取 `ModelRegistrationIdentity`（`display_name` + `source_path` + `is_lora`）
2. 调用 `list(PortNameModels { ... })` 查询同 `PortName` 已有的 `Model` 实例
3. 用 `find_conflicting_model_name()` 检查兼容性——若已有同名模型卡则视为兼容（允许多实例同名），若已有不同名模型卡则视为冲突（禁止同 `PortName` 混用不同模型卡）
4. 检查通过后调用 `register_internal()` 执行写入
5. 写入完成后**再次**查询 `list()`，检测写入期间是否发生竞态冲突
6. 若竞态检测发现冲突，调用 `unregister()` 回滚，返回 `Err`

LoRA adapter 的兼容性规则：LoRA 注册时以 `source_path`（基底模型路径）为兼容键，而非 `display_name`（adapter 名），允许同 `PortName` 注册同一基底模型的多个不同 LoRA adapter。基础模型卡（non-LoRA）以 `display_name` 为兼容键，不同名则冲突。

**实现者**：


| 类型                    | 注册存储                | `instance_id()` 来源             |
| --------------------- | ------------------- | ------------------------------ |
| `KubeDiscoveryClient` | k8s 原生对象（SSA） | `hash_pod_name(pod_name)` |
| `MockDiscovery`       | 进程内 `Vec`        | 构造时传入或原子自增计数器 |


---

### 3.13 `DiscoveryMetadata` — 单个 Pod 的注册元数据状态

**来源**：`src/discovery/metadata.rs`

**设计意图**：k8s 原生发现后端需要一个进程内的"中间状态"：每个 pod 维护一份 `DiscoveryMetadata`，记录该 pod 当前注册的所有 `PortName`、`Model`（承载 `ModelCard` 数据）、EventChannel。注册/注销操作先更新本地 `DiscoveryMetadata`，再分别落到原生 k8s 对象：`PortName` → `Service/EndpointSlice`，`Model` / `ModelCard` 数据 → `ConfigMap`，EventChannel → `Lease`。这种"本地状态 + 原生对象映射"的模式避免了把所有发现语义塞进单个 CRD。

三个 `HashMap` 以 `InstanceId::to_path()` 字符串作为键，值为完整 `DiscoveryInstance`，因此同一实例的注册幂等（重复调用 `register_portname` 以相同 path 覆盖写，不产生重复条目）。

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryMetadata {
    #[serde(default, deserialize_with = "deserialize_null_default")]
    portnames:      HashMap<String, DiscoveryInstance>,  // key: PortNameInstanceId::to_path()
    #[serde(default, deserialize_with = "deserialize_null_default")]
    model_cards:    HashMap<String, DiscoveryInstance>,  // key: ModelCardInstanceId::to_path()
    #[serde(default, deserialize_with = "deserialize_null_default")]
    event_channels: HashMap<String, DiscoveryInstance>,  // key: EventChannelInstanceId::to_path()
}
```

`**deserialize_null_default` 反序列化修复**：

- **根因**：原生对象聚合时，某个对象集合可能临时为空，而来自 k8s API 的反序列化结果仍需稳定映射为 `DiscoveryMetadata`。
- **影响**：若把空对象集合误当成反序列化失败，会导致该 pod 对聚合守护进程不可见，进而所有发向该 pod 的请求返回 404。
- **修复**：`deserialize_null_default` 将 `null` 等同于 `T::default()`（即空 `HashMap`），消除此类临时空集合导致的可见性故障。

**实现的 trait**：


| Trait                       | 来源           | 实现细节                                     |
| --------------------------- | ------------ | ---------------------------------------- |
| `Clone`                     | derive       | 由原生 k8s discovery client 在写对象失败时克隆用于回滚 |
| `Serialize` / `Deserialize` | derive + 自定义 | 用于本地状态持久化/对象映射；反序列化时修复 `null` → `{}` |
| `Default`                   | 手写           | `Self::new()`，三个空 HashMap                |


**自身方法**：

```rust
// 注册（写入对应 HashMap，key = InstanceId::to_path()）
pub fn register_portname(&mut self, instance: DiscoveryInstance) -> Result<()>
pub fn register_model_card(&mut self, instance: DiscoveryInstance) -> Result<()>
pub fn register_event_channel(&mut self, instance: DiscoveryInstance) -> Result<()>

// 注销（从对应 HashMap 删除 key）
pub fn unregister_portname(&mut self, instance: &DiscoveryInstance) -> Result<()>
pub fn unregister_model_card(&mut self, instance: &DiscoveryInstance) -> Result<()>
pub fn unregister_event_channel(&mut self, instance: &DiscoveryInstance) -> Result<()>

// 读取
pub fn get_all_portnames(&self) -> Vec<DiscoveryInstance>
pub fn get_all_model_cards(&self) -> Vec<DiscoveryInstance>
pub fn get_all_event_channels(&self) -> Vec<DiscoveryInstance>
pub fn get_all(&self) -> Vec<DiscoveryInstance>   // 三个 HashMap 合并输出
pub fn filter(&self, query: &DiscoveryQuery) -> Vec<DiscoveryInstance>
```

所有 `register_*()` 和 `unregister_*()` 方法都包含变体类型校验：传入错误变体（如把 `EventChannel` instance 传给 `register_portname`）会返回 `Err`，而不是静默写入错误桶。

`filter()` 先根据 `query` 类型选择读取哪个 HashMap，再调用内部 `filter_instances()` 做精确字段匹配，两步过滤确保跨类型查询（如用 `AllPortNames` 查 EventChannel）返回空集而不是错误数据。

---

### 3.14 `MetadataSnapshot` — 集群全局实例快照

**来源**：`src/discovery/metadata.rs`

**设计意图**：`KubeDiscoveryClient` 的 `list()` 和 `list_and_watch()` 需要从当前时刻的集群状态提取实例列表，而不是实时查询 k8s API（避免每次 list 都发起 HTTP 调用）。`MetadataSnapshot` 是 `DiscoveryDaemon` 定期聚合的不可变快照，通过 `tokio::sync::watch` channel 广播给所有 `KubeDiscoveryClient` 实例。这个设计将"聚合"与"查询"解耦：聚合由守护进程在后台以 debounce 节奏执行，查询由客户端直接读取最新快照（无锁竞争）。

```rust
#[derive(Clone, Debug)]
pub struct MetadataSnapshot {
    pub instances:   HashMap<u64, Arc<DiscoveryMetadata>>,  // instance_id → 该 pod 的注册元数据
    pub generations: HashMap<u64, i64>,                      // instance_id → 对象聚合版本（变更检测用）
    pub sequence:    u64,                                    // 快照序列号，用于 debug 日志追踪
    pub timestamp:   std::time::Instant,                     // 快照生成时间，供可观测性使用
}
```

`**instances` 与 `generations` 的键集合严格相同**：`DiscoveryDaemon` 在聚合时以"EndpointSlice 中 ready 且有对应原生发现对象"为条件同时写入两个 HashMap，不存在键不对齐的情况。

**自身方法**：

```rust
pub fn empty() -> Self
// 初始空快照，作为 watch channel 的初始值，避免消费方读到未初始化状态

pub fn has_changes_from(&self, prev: &MetadataSnapshot) -> bool
// 比较两个快照的 generations map 是否相同
// 相同 → false（守护进程跳过广播，避免无意义 watch 触发）
// 不同 → true，并打印 info 日志记录 added/removed/updated 的 instance_id 集合

pub fn filter(&self, query: &DiscoveryQuery) -> Vec<DiscoveryInstance>
// 遍历 instances.values()，对每个 DiscoveryMetadata 调用 metadata.filter(query)，汇总结果
```

`has_changes_from()` 的比较基准是 **对象聚合版本**（来自 k8s 原生对象的 generation / resourceVersion 折叠结果）而非 instance 数据内容本身。版本未变意味着相关对象内容未变，无需重新聚合实例列表。这比逐字段比较 `DiscoveryMetadata` 更高效。

---

### 3.15 `KubeDiscoveryClient` — k8s 发现后端

**来源**：模块根为 `src/discovery/kube.rs`；配套子模块在 `src/discovery/kube/` 目录（`service_registry.rs`、`objects.rs`、`daemon.rs`、`utils.rs`）。

**设计意图**：在 k8s 环境中，原生的 pod 生命周期管理（健康检查、滚动更新、CRIU 快照恢复）与服务发现深度集成。`KubeDiscoveryClient` 直接利用原生资源承载发现状态：`PortName` 用 `Service + EndpointSlice`，`Model`（其负载是 `ModelCard`）用 `ConfigMap`，EventChannel 用 `Lease`，通过 Server-Side Apply 实现幂等写入，并依赖 `OwnerReference` 让 k8s 在 pod 删除时自动 GC 相关对象。

```rust
#[derive(Clone)]
pub struct KubeDiscoveryClient {
    instance_id:    u64,                                                // hash_pod_name(pod_name)
    metadata:       Arc<RwLock<DiscoveryMetadata>>,                     // 本进程的注册状态，与系统服务器共享
    metadata_watch: tokio::sync::watch::Receiver<Arc<MetadataSnapshot>>,// 从 DiscoveryDaemon 接收快照更新
    kube_client:    KubeClient,                                         // Kubernetes API 客户端
    pod_info:       PodInfo,                                            // 本 pod 的身份信息
}


```

`**new()` 初始化过程**：

1. `PodInfo::from_env()` 从 Downward API 文件或环境变量读取 pod 身份（CRIU 恢复场景优先读文件，见 3.19 节）
2. `hash_pod_name(&pod_info.pod_name)` 计算稳定的 `instance_id`
3. `KubeClient::try_default()` 创建 K8s 客户端（in-cluster kubeconfig 或 `~/.kube/config`）
4. `tokio::sync::watch::channel(Arc::new(MetadataSnapshot::empty()))` 创建快照广播 channel
5. 构造 `DiscoveryDaemon` 并 `tokio::spawn` 在后台运行守护进程

```rust
#[async_trait]
impl Discovery for KubeDiscoveryClient 
```

`**register_internal()` 过程**：

1. 通过 `self.metadata.write().await` 获取写锁（注释明确：跨越对象写操作持有锁，防止并发竞争）
2. 克隆当前 `metadata` 作为回滚快照（`original_state`）
3. match `instance` 变体调用对应的 `register_portname / register_model_card / register_event_channel`
4. 调用原生对象映射逻辑：`PortName` → `register_endpoint_instance()`，`Model` → `apply_model_config_map()`，EventChannel → `apply_event_lease()`
5. 通过 Server-Side Apply 写入对应 k8s 原生资源
6. 若对象 apply 失败，`*metadata = original_state` 回滚本地状态并返回 `Err`

写锁跨越整个对象写操作，代价是注册操作会串行化，但 `register` 调用通常只在启动时发生一次，不影响运行时性能。

`**unregister()` 过程**：

同上，反向注销。

`**list()` 过程**：

直接调用 `self.metadata_watch.borrow()` 读取最新快照（不触发 k8s API 调用），再调用 `snapshot.filter(&query)` 过滤。快照由 `DiscoveryDaemon` 持续更新，`list()` 的语义是"截至最近一次 debounce 窗口的状态"，而非严格实时。

`**list_and_watch()` 过程**：

1. clone `metadata_watch` 得到本次 watch 专用的 receiver
2. 创建 `mpsc::unbounded_channel` 作为输出
3. `tokio::spawn` 一个后台任务，该任务：
  - 读取初始快照并 `borrow_and_update()`（标记已读，防止 `changed()` 重复触发）
  - 将初始快照中所有匹配的实例发出 `Added` 事件（"list"部分）
  - 用 `HashSet<DiscoveryInstanceId>` 维护已知实例集合（`known`）
  - 循环 `watch_rx.changed().await` 等待快照更新
  - 收到新快照后与 `known` 集合 diff：新增 key 发 `Added`，消失 key 发 `Removed`
  - 更新 `known` 集合为当前快照的 key 集合
  - 若 `cancel_token` 触发，`break` 退出
4. 将 `mpsc::UnboundedReceiver` 包装为 stream 返回

**实现的 trait**：


| Trait       | 来源              | 实现细节                                                    |
| ----------- | --------------- | ------------------------------------------------------- |
| `Clone`     | derive          | `Arc<RwLock<...>>` 和 `watch::Receiver` 均支持 clone，共享底层状态 |
| `Discovery` | 手写（async_trait） | 见上文各方法说明                                                |


---

### 3.16 `ServiceRegistration` / 对象映射 — k8s 资源注册模型

**来源**：`src/discovery/kube/service_registry.rs` + `src/discovery/kube/objects.rs`

**设计意图**：Pagoda 的发现不再把全部发现状态压缩到单个 CRD，而是直接使用原生资源表达各类发现对象：`PortName` 用 `Service + EndpointSlice`，`Model`（承载 `ModelCard`）用 `ConfigMap`，EventChannel 用 `Lease`。`ServiceRegistration` 统一封装 `Service/EndpointSlice` 所需的 triplet 注册信息，`objects.rs` 负责对象级 apply / delete / 反向映射。

`OwnerReference` 的设计让 k8s GC 自动管理这些原生对象的生命周期：pod 被删除时，k8s garbage collector 会自动删除 `owner_references` 中指向该 pod 的 `Service` / `EndpointSlice` / `ConfigMap` / `Lease` 等对象，无需额外 TTL 清理。

```rust
pub struct ServiceRegistration {
    pub service_name: String,
    pub port_name: String,
    pub port: i32,
    pub pod_name: String,
    pub pod_uid: String,
    pub pod_ip: String,
    pub hostname: Option<String>,
    pub protocol: String,
    pub app_protocol: Option<String>,
    pub headless: bool,
}
```

`service_registry.rs` 负责根据 `ServiceRegistration` 构造和 apply 原生 `Service` / `EndpointSlice`；`objects.rs` 负责将 `DiscoveryInstance` 与 `ConfigMap` / `Lease` / `EndpointSlice` 之间做双向映射。

**工具函数**：

```rust
pub fn build_service(registration: &ServiceRegistration) -> Result<Service>
pub fn build_endpoint_slice(registration: &ServiceRegistration) -> Result<EndpointSlice>
```

1. `build_service()` 生成 headless `Service`，表达 `ServiceGroup` 级协议入口
2. `build_endpoint_slice()` 生成 pod-owned `EndpointSlice`，表达单个 `PortName` 实例的 ready 地址
3. `apply_model_config_map()` / `apply_event_lease()` 分别写入模型卡与事件平面发现对象
4. 所有对象都设置 `owner_references` 指向 Pod，允许 k8s 自动回收

```rust
pub async fn apply_service(kube_client: &KubeClient, namespace: &str, service: &Service) -> Result<()>
pub async fn apply_endpoint_slice(kube_client: &KubeClient, namespace: &str, slice: &EndpointSlice) -> Result<()>
```

使用 `PatchParams::apply(FIELD_MANAGER).force()` 执行 Server-Side Apply（SSA）。Pagoda 目标设计中，`FIELD_MANAGER` 使用 `pagoda-worker` 一类对象管理器标识本客户端，使 k8s 追踪字段所有权。SSA 语义是 create-or-update，无需先判断对象是否存在。

---

### 3.17 `DiscoveryDaemon` — 原生资源聚合守护进程

**来源**：`src/discovery/kube/daemon.rs`

**设计意图**：`KubeDiscoveryClient` 的 `list` / `list_and_watch` 需要一个始终最新的集群视图，而不是在每次调用时都向 k8s API 发起 HTTP 请求（成本高、延迟不确定）。`DiscoveryDaemon` 以 `kube-runtime` 的 `reflector` 机制（本地缓存 + list/watch）构建多个本地状态存储，并在状态变化时聚合出新快照，通过 `watch::Sender` 广播给所有 `KubeDiscoveryClient`。

**为何需要多 reflector 聚合**：单靠 `EndpointSlice` 只能知道 pod IP 是否 ready，不含模型卡与事件通道；单靠 `ConfigMap` / `Lease` 又无法确认 pod 是否 ready。只有 `EndpointSlice + Service + ConfigMap + Lease` 这些原生对象在同一 pod 身上形成交集时，才能得到真正可用的完整实例视图，这是 `aggregate_snapshot()` 的核心逻辑。

`**DEBOUNCE_DURATION = 500ms`**：K8s 控制面事件在滚动更新等场景下会高频密集触发，若每次事件都立即重新聚合会造成 CPU 浪费和快照抖动。500ms debounce 将连续事件批次合并为单次快照更新，在响应及时性与聚合开销之间取得平衡。

```rust
#[derive(Clone)]
pub(super) struct DiscoveryDaemon {
    kube_client:  KubeClient,
    pod_info:     PodInfo,          // 本 pod 信息，用于确定 watch 的 namespace
    cancel_token: CancellationToken,
}


```

`**run()` 主循环过程**：

1. 创建 `Arc<Notify>` 作为"有更新"信号，多个 reflector 共享
2. 启动 EndpointSlice reflector：
    - label filter：监听 discovery 管理的 `EndpointSlice`
  - 每收到更新或错误调用 `notify.notify_one()`
  - `tokio::spawn` 后台运行（`for_each` 返回 Future，内部调 `future::ready(())`）
3. 启动 `Service` reflector：
    - 监听当前命名空间下由 discovery 注册器管理的 `Service`
4. 启动模型卡 `ConfigMap` reflector：
    - 监听模型卡注册 `ConfigMap`
5. 启动事件 `Lease` reflector：
    - 监听事件平面 `Lease`
  - 每收到更新调用 `notify.notify_one()`
  - `tokio::spawn` 后台运行
6. 事件驱动主循环（`tokio::select!`）：
  - `notify.notified()` → `sleep(DEBOUNCE_DURATION)` → `timeout(Duration::ZERO, notify.notified())` 排空额外 notify → `aggregate_snapshot()` → `has_changes_from` → 有变化则 `watch_tx.send(Arc::new(snapshot))`；`send` 返回 `Err` 时（无消费方）退出循环
  - `cancel_token.cancelled()` → break，守护进程优雅退出

`**aggregate_snapshot()` 聚合过程**：

1. 从 EndpointSlice reflector store 取所有快照
2. 调用 `extract_endpoint_info()` 提取 ready 端点，收集 `(instance_id, pod_name)` 列表
3. 从 `Service` / `ConfigMap` / `Lease` reflector store 取所有原生对象
4. 通过 `endpoint_instance_from_service_and_slice()`、`model_card_instance_from_config_map()`、`event_instance_from_lease()` 分别重建 `DiscoveryInstance`
5. 遍历 ready pod 列表：若该 pod 同时具备对应原生对象信息，则加入 `instances` 和 `generations` map；否则跳过（pod ready 但注册对象未齐备，可能是注册延迟，下次 notify 再重试）
6. 返回带有 sequence 编号和 timestamp 的 `MetadataSnapshot`

---

### 3.18 `PodInfo` 与 `hash_pod_name` — Pod 身份解析

**来源**：`src/discovery/kube/utils.rs`

**设计意图**：`KubeDiscoveryClient` 需要知道本 pod 的身份（名称、命名空间、UID、IP）来构建原生对象的所有者引用，并计算稳定的 `instance_id`。Pod 身份有两种获取方式：k8s Downward API volume 挂载文件（`/etc/podinfo/{pod_name,pod_uid,pod_namespace,pod_ip}`）和环境变量（`POD_NAME / POD_UID / POD_NAMESPACE / POD_IP`）。

Downward API 文件优先于环境变量，是为了支持 CRIU（checkpoint/restore in userspace）场景：被 CRIU 还原的 pod 进程，其环境变量中保存的是**源 pod** 的名称，而挂载的 podinfo 文件由 kubelet 在还原 pod 时刷新为**目标 pod** 的名称，因此读文件能得到正确的当前 pod 身份，避免注册到错误的 CR 下。

```rust
pub(super) struct PodInfo {
    pub pod_name:      String,
    pub pod_namespace: String,
    pub pod_uid:       String,
    pub pod_ip:        String,
    pub system_port:   u16,   // 从 RuntimeConfig 读取，用于 system server 端口配置
}
```

`**hash_pod_name()` 设计**：

```rust
pub fn hash_pod_name(pod_name: &str) -> u64 {
    const INSTANCE_ID_MASK: u64 = 0x001F_FFFF_FFFF_FFFFu64;  // 保留低 53 位
    let mut hasher = DefaultHasher::new();
    pod_name.hash(&mut hasher);
    hasher.finish() & INSTANCE_ID_MASK
}
```

清除高 11 位的原因：`instance_id` 有时会被序列化为 JSON number（如写入 `ConfigMap` 注解或对象映射字段时）。IEEE-754 double 精度仅有 53 位尾数，超过 53 位的 `u64` 值在 JSON 序列化/反序列化后会丢失精度，导致 ID 变化。掩码 `0x001F_FFFF_FFFF_FFFF` 确保结果始终在 53 位有效数字范围内，JSON roundtrip 精度安全。

`**extract_endpoint_info()**`：从一个 `EndpointSlice` 提取所有 `ready = true` 的端点的 `(instance_id, pod_name)` 对：

- 通过 `endpoint.conditions.ready` 确认就绪状态
- 通过 `endpoint.target_ref.name` 获取 pod_name
- 调用 `hash_pod_name` 计算 instance_id
- pod_name 为空或无 `target_ref` 的端点被跳过

---

### 3.19 `MockDiscovery` / `SharedMockRegistry` — 测试用内存发现后端

**来源**：`src/discovery/mock.rs`

**设计意图**：单元测试和集成测试需要一个不依赖 k8s API 的发现替身，能够即时注册、快速响应、确定性行为。`MockDiscovery` 将注册状态存储在 `Arc<Mutex<Vec<DiscoveryInstance>>>` 中，多个 `MockDiscovery` 实例可共享同一个 `SharedMockRegistry`，模拟同一集群中不同 worker 的注册场景。

```rust
#[derive(Clone, Default)]
pub struct SharedMockRegistry {
    instances: Arc<Mutex<Vec<DiscoveryInstance>>>,  // Arc：多实例共享；Mutex：同步访问
}

pub struct MockDiscovery {
    instance_id: u64,               // 构造时指定（测试可控）或原子自增计数器（不指定时）
    registry:    SharedMockRegistry, // 对共享注册表的引用
}
```

`**MockDiscovery::new()` 的 instance_id 策略**：

- 传入 `Some(id)`：使用指定值（测试中固定 ID 便于断言）
- 传入 `None`：使用 `static AtomicU64::fetch_add(1, SeqCst)`，保证不同实例 ID 唯一，从 1 开始递增

`**list_and_watch()` 实现**：不使用 channel 推送，而是在 `async_stream::stream!` 宏内**每 10ms 轮询**一次 `registry.instances`，与上次已知集合（`HashSet<DiscoveryInstanceId>`）diff，发出 `Added` / `Removed` 事件。这个"polling watch"模式实现简单，不需要 notify 机制，10ms 粒度对单元测试足够；但不适合生产场景（生产后端使用 k8s watch，延迟更低且不占 CPU）。

`**matches_query()`**：private 辅助函数，实现 `(instance, query)` 组合的匹配逻辑。使用**穷举 match** 列出所有合法组合和所有跨类型不匹配组合，确保 `PortName` instance 对 `AllModels` query 明确返回 `false`，不会有默认行为导致数据类型混淆。

**实现的 trait**：


| Trait       | 来源                           | 实现细节                                                                                                          |
| ----------- | ---------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `Clone`     | derive（`SharedMockRegistry`） | Arc 引用计数增量，多实例共享同一 Vec                                                                                        |
| `Default`   | derive（`SharedMockRegistry`） | `instances: Arc::new(Mutex::new(Vec::new()))`                                                                 |
| `Discovery` | 手写（async_trait）              | `register_internal` 直接 push Vec；`unregister` 按 instance_id retain；`list` 过滤 Vec；`list_and_watch` 10ms 轮询 diff |


---

### 3.20 `watch_and_extract_field` — 发现流到字段 HashMap 的通用转化器

**来源**：`src/discovery/utils.rs`

**设计意图**：上层代码（如路由层）常见的模式是：watch 一组 `Model` 实例，并把它们反序列化成 `ModelCard`，实时维护一个 `instance_id → 某字段` 的 HashMap，供请求处理时快速查找。这个模式重复出现，但每次提取的字段类型不同（`ModelRuntimeConfig`、`TokenizerConfig` 等）。`watch_and_extract_field` 是这个模式的泛型实现，通过 `extractor: F` 参数注入字段提取逻辑，封装 stream 消费、state 维护、watch channel 广播三个步骤。

```rust
pub fn watch_and_extract_field<T, V, F>(
    stream:    DiscoveryStream,   // 来自 discovery.list_and_watch(...) 的事件流
    extractor: F,                 // T → V：从反序列化后的类型中提取目标字段
) -> tokio::sync::watch::Receiver<HashMap<u64, V>>
where
    T: for<'de> Deserialize<'de> + 'static,  // 反序列化目标类型（如 ModelDeploymentCard）
    V: Clone + Send + Sync + 'static,          // 提取出的字段类型（如 ModelRuntimeConfig）
    F: Fn(T) -> V + Send + 'static,
```

**内部工作过程**：

1. 创建 `watch::channel(HashMap::new())` 作为对外接口（消费方调用 `rx.borrow()` 读取最新状态，无需 await）
2. `tokio::spawn` 后台任务，持有 `stream` 和状态 `HashMap<u64, V>`
3. 循环 `stream.next().await`：
    - `Added(instance)` → `instance.deserialize_model::<T>()` 反序列化为 T → `extractor(t)` 提取字段 V → 插入 `state` → `tx.send(state.clone())`
  - `Removed(id)` → `state.remove(&id.instance_id())` → `tx.send(state.clone())`
  - `Err(e)` → 打印 error 日志，**继续**（不中止 stream，允许临时错误后恢复）
4. `tx.send()` 返回 `Err` 时（接收方已 drop）退出任务，避免泄漏后台任务
5. `stream` 关闭时（上游 Discovery 停止）自然退出循环

反序列化失败时仅打印 warn 并 `continue`（不 panic、不中止），使单个坏 instance 不影响整个 watch 流。这要求 LLM 层在更新 `ModelDeploymentCard` 格式时保持向后兼容。

---

## 四、后端选择与集成路径

`DistributedRuntime` 在初始化时构造 `KubeDiscoveryClient`，之后将其包装为 `Arc<dyn Discovery>`，所有需要发现能力的组件通过 `drt.discovery()` 获取此引用。测试环境则显式注入 `MockDiscovery`。

```
k8s 部署环境        → KubeDiscoveryClient（Service + EndpointSlice + ConfigMap + Lease）
测试 / 单测          → MockDiscovery
```

---

## 五、设计约束与演进方向

1. `**lib/runtime` 不依赖 `lib/llm**`：`card_json: serde_json::Value` 是解耦手段，代价是在边界处多了一次 serialize/deserialize。若未来 `lib/runtime` 与 `lib/llm` 合并，可以将 `DiscoverySpec::Model` 改为泛型参数，消除 JSON 中间层。
2. **MockDiscovery 的轮询模式**：10ms 轮询在单元测试中可接受，但如果需要亚毫秒精度的 watch 响应（如性能测试），应改为 notify-driven 模式，增加 `Notify` 信号配合 `Arc<Mutex<Vec>>` 实现推送。
3. **KubeDiscoveryClient 的对象写入串行化**：写锁跨越 k8s API 调用保证了本地状态与原生对象状态的一致性，但代价是注册/注销时其他注册操作会阻塞。若未来需要并发注册（如 batch 启动场景），可以考虑将对象写入移出锁范围，改用乐观并发控制。
4. **原生对象 label/filter 约定**：发现系统依赖 `Service` / `EndpointSlice` / `ConfigMap` / `Lease` 的统一标签与 owner 关系。如果对象缺少这些标签，`DiscoveryDaemon` 不会聚合到对应 pod，调试时需优先检查对象标签配置。
5. **模型名冲突检测的 TOCTOU 问题**：`register()` 的双重检查（注册前 + 注册后）是一种乐观并发控制，在极端竞态下仍有可能出现两个不同名模型同时通过前检查后并发写入。当前设计在发现竞态后回滚并报错，依赖调用方重试，是"尽力而为"语义而非强一致保证。

