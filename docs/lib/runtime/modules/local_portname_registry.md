# `local_portname_registry` 模块设计文档

**源码位置**：`lib/runtime/src/local_portname_registry.rs`（约 65 行，单文件模块）

---

## 一、设计背景与模块职责

Pagoda 的正常部署模式是前端（Router/Scheduler）和后端（Worker/推理引擎）运行在不同进程甚至不同机器上，通过 TCP 或 NATS 通信。但存在若干场景需要前后端在**同一进程**内运行：

1. **嵌入式单机部署**：将整个推理服务打包为单一可执行文件，省去进程管理和网络开销；
2. **集成测试**：在单进程测试中验证前后端的完整交互，避免启动外部服务的复杂性；
3. **低延迟场景**：某些延迟极度敏感的应用不能承受任何网络栈开销（即使是 loopback）。

若在同进程模式下仍走 TCP 栈，一次请求的开销包括：Rust 对象序列化 → 写入内核 socket 缓冲区 → loopback 回环 → 读出缓冲区 → 反序列化。这一路径延迟约 100-200μs，而直接调用 Rust 函数的延迟在 1μs 以内，差距两个数量级。

`local_portname_registry` 模块提供了同进程直调的机制：后端将自己的处理函数注册为"本地端点"，前端在发起请求时先查询本地注册表，若命中则直接调用函数，完全绕过网络传输层。

---

## 二、`LocalAsyncEngine` 类型别名

### 为什么需要固定的类型签名

```rust
pub type LocalAsyncEngine = Arc<
    dyn AsyncEngine<
            crate::pipeline::SingleIn<serde_json::Value>,
            crate::pipeline::ManyOut<crate::protocols::annotated::Annotated<serde_json::Value>>,
            anyhow::Error,
        > + Send
        + Sync,
>;
```

网络传输层（TCP/NATS）在传输推理请求时将请求序列化为 JSON（`serde_json::Value`）再反序列化——这是网络边界上数据格式的必然选择。

本地直调虽然不走网络，但仍然需要与网络路径保持相同的接口，原因：

**统一的路由逻辑**：路由层在决定"走本地路径还是网络路径"时，需要知道两条路径使用的是相同的请求/响应类型，否则需要在路由前做类型判断，逻辑复杂且容易出错。固定使用 `SingleIn<serde_json::Value>` 和 `ManyOut<Annotated<serde_json::Value>>`，使本地路径和网络路径对路由层完全透明——路由层只需查询注册表是否有命中，有就调用 `LocalAsyncEngine`，没有就走网络。

**类型参数详解**：

- `SingleIn<serde_json::Value>`：单一 JSON 请求（pipeline 的 `SingleIn` 包装，携带 `AsyncEngineContext` 以支持取消）；
- `ManyOut<Annotated<serde_json::Value>>`：流式 JSON 响应，`Annotated` 表示响应可以携带错误信息（`MaybeError` 语义），`ManyOut` 是流输出的 pipeline 类型；
- `anyhow::Error`：引擎层面的错误类型，足够灵活（不同引擎可以产生不同的错误，统一用 `anyhow::Error` 包裹）。

**`Arc<dyn AsyncEngine<...> + Send + Sync>`**：

- `Arc`：同一个引擎实现可能被多个请求并发调用，需要引用计数共享所有权；
- `dyn AsyncEngine<...>`：注册表存储 trait 对象，不需要知道具体引擎类型；
- `+ Send + Sync`：引擎实例在多线程 Tokio 运行时上被并发调用，必须满足线程安全约束。

---

## 三、`LocalPortNameRegistry` 结构体

### 为什么需要这个结构体

```rust
#[derive(Clone, Default)]
pub struct LocalPortNameRegistry {
    engines: Arc<DashMap<String, LocalAsyncEngine>>,
}
```

`LocalPortNameRegistry` 作为 `DistributedRuntime` 的一个字段存在，随 DRT 一起被克隆和传递。它需要满足与 DRT 字段相同的约束：

**廉价克隆**：`Arc<DashMap<...>>` 克隆只递增引用计数，所有 DRT 克隆共享同一个注册表实例。后端注册本地端点后，所有持有 DRT 克隆的前端代码都能立即看到这个注册——注册是全局可见的，不会出现"注册在 DRT-A 上，DRT-B 查不到"的情况。

**并发安全的无锁读写**：使用 `DashMap`（分片 ConcurrentHashMap），而非 `Mutex<HashMap>`。

**为什么选 `DashMap` 而非 `Mutex<HashMap>` 或 `RwLock<HashMap>`**：

- `Mutex<HashMap>`：每次读写都独占整个 Map，并发读也会互相阻塞；在高并发推理请求下，每个请求都需要查询注册表，`Mutex` 会成为序列化瓶颈。
- `RwLock<HashMap>`：多读单写，读不阻塞读，但 `std::sync::RwLock` 在高读并发下仍有锁竞争开销；且跨 await 持有 `RwLock` guard 会有 Send 问题（同步锁不能跨 await 持有）。
- `DashMap`：内部将 Map 分成多个分片（默认 shard 数 = CPU 核数 × 4），每个 shard 有独立的读写锁。不同 shard 的读写完全并行，同一 shard 的多读也并行。对于端点数量通常在个位数的注册表，访问时几乎不存在 shard 竞争，查询路径接近无锁。

`DashMap` 的 `get()` 和 `insert()` 都是同步方法，不涉及 `await`，可以在 async 上下文中安全调用而无需担心持锁跨 await 的问题。

**`#[derive(Default)]`**：生成空注册表的构造，与 `new()` 等价，使 `DistributedRuntime::new()` 中可以用 `LocalPortNameRegistry::default()` 或 `::new()` 初始化。

---

### 字段详解

**`engines: Arc<DashMap<String, LocalAsyncEngine>>`**

键：端点名称（如 `"generate"`、`"load_lora"`），与 Pagoda 的 PortName 命名一致，不含命名空间和组件名前缀。

路由层在查询时传入端点名，若命中则使用对应的 `LocalAsyncEngine` 直接处理请求。

只存储端点名而非完整路径（`{namespace}.{servicegroup}.{portname}`）的原因：同进程部署场景下通常只有一套命名空间和组件，完整路径反而增加了注册和查询时的字符串拼接开销。若未来需要支持多组件同进程部署，键的设计可以扩展为完整路径。

---

## 四、`impl LocalPortNameRegistry` 方法

### `new() -> Self`

```rust
pub fn new() -> Self {
    Self {
        engines: Arc::new(DashMap::new()),
    }
}
```

创建空注册表，`DashMap::new()` 使用默认分片数。不在构造时注册任何引擎，注册由后端初始化代码按需调用。

---

### `register(portname, engine)`

```rust
pub fn register(&self, portname: String, engine: LocalAsyncEngine) {
    tracing::debug!("Registering local portname: {portname}");
    self.engines.insert(portname, engine);
}
```

将引擎注册到注册表。`DashMap::insert()` 若键已存在则覆盖（与 `HashMap::insert` 语义相同），允许后端在重新初始化时替换旧的引擎实现（如模型热重载后注册新的引擎实例）。

`tracing::debug!` 记录注册事件，用于调试同进程部署时的初始化顺序问题（确认端点在请求到来前已注册）。

**`portname: String`（而非 `&str`）**：`DashMap` 需要所有的 Key，`String` 避免了调用方在栈上分配字符串后再 clone 一次。调用方通常已经持有 `String`（从配置或 portname 定义中获取），直接 move 传入无额外开销。

---

### `get(portname) -> Option<LocalAsyncEngine>`

```rust
pub fn get(&self, portname: &str) -> Option<LocalAsyncEngine> {
    self.engines.get(portname).map(|e| e.clone())
}
```

查找并返回引擎的 `Arc` 克隆。

**`self.engines.get(portname)`** 返回 `Option<dashmap::mapref::one::Ref<'_, String, LocalAsyncEngine>>`——一个持有 DashMap 分片读锁的 RAII guard。

**`.map(|e| e.clone())`** 克隆内部的 `Arc<dyn AsyncEngine<...>>`（即 `LocalAsyncEngine`），然后 guard 被 drop，分片读锁释放。返回的是一个独立的 `Arc` 强引用，调用方可以持有它并在后续的 `.await` 中使用，无需担心锁的生命周期问题。

若在 guard 存活期间 `await`，会持有 DashMap 的分片读锁跨越 `await` 点——虽然 `Ref` 类型本身是 `Send`，但持锁跨 await 会阻止该分片的写操作（注册新引擎时需要写锁），造成不必要的阻塞。提前 `.clone()` 释放 guard 是正确的实践。

返回 `Option`：端点未注册时返回 `None`，路由层据此决定走网络路径（fallback 到 TCP/NATS）。这使本地注册表对路由层是"透明可选"的——有注册就用本地路径，没有就走网络，无需提前知道是否有本地端点。
