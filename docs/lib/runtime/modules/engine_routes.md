# `engine_routes` 模块设计文档

**源码位置**：`lib/runtime/src/engine_routes.rs`（单文件，约 100 行）

---

## 一、设计背景与模块职责

Pagoda 的系统状态服务器（`SystemStatusServer`）提供了一组固定的 HTTP 端点：`/health`、`/live`、`/metrics`、`/metadata`。这些端点由框架统一实现，覆盖运维可观测性需求。

但是，不同的推理引擎需要暴露各自独特的运维接口。例如：

- TensorRT-LLM 引擎可能需要 `/engine/start_profile`、`/engine/stop_profile` 来启动和停止 GPU 性能分析；
- vLLM 后端可能需要 `/engine/stats` 查询 KV cache 利用率；
- 自定义后端可能需要 `/engine/reload_lora` 动态加载 LoRA 权重。

若将这些端点硬编码到框架的 HTTP 服务器中，框架就会依赖业务逻辑，违反分层原则。若让每个引擎自行启动独立的 HTTP 服务器，则会产生多个监听端口、增加运维复杂度。

`engine_routes` 模块的解决方案是**注册表模式（Registry Pattern）**：框架的 HTTP 服务器保留 `/engine/*` 路径的路由槽，引擎在初始化时将自己的处理函数注册进来，服务器启动时将这些动态路由一并挂载。框架与引擎之间通过注册表解耦——框架不需要知道具体有哪些引擎路由，引擎不需要关心 HTTP 服务器的实现细节。

---

## 二、`EngineRouteCallback` 类型别名

### 为什么需要这个类型别名

```rust
pub type EngineRouteCallback = Arc<
    dyn Fn(
            serde_json::Value,
        ) -> Pin<Box<dyn Future<Output = anyhow::Result<serde_json::Value>> + Send>>
        + Send
        + Sync,
>;
```

这是一个复合类型，拆开看清楚每层的用意：

**函数签名 `Fn(serde_json::Value) -> Pin<Box<dyn Future<...> + Send>>`**

入参和出参都选用 `serde_json::Value`（即 JSON 对象），而非强类型 Rust 结构体。设计原因：

1. **跨语言边界**：Pagoda 的引擎通常由 Python 注册（通过 PyO3 绑定），Python 与 Rust 之间最自然的数据交换格式是 JSON。若用强类型，每种引擎都需要定义 Rust 类型并实现序列化，大幅增加绑定层代码量。
2. **前向兼容**：不同引擎的请求/响应结构各不相同，用 `serde_json::Value` 使框架完全不依赖引擎的数据结构，引擎升级时框架无需重新编译。
3. **HTTP 天然语义**：HTTP 请求体和响应体本来就是字节流，框架只需在进出时做一次 JSON 解析/序列化，回调函数收到的就是已解析的 Value，无需重复处理。

回调是异步的（返回 `Future`），因为引擎操作（如启动 profiling、等待模型加载）本质上是异步 IO 操作。`Pin<Box<...>>` 是异步 trait 方法的标准返回形式：`Pin` 确保 Future 在 poll 期间内存地址不变（await 语义要求），`Box` 使 Future 有确定的大小（trait 对象无法在栈上直接存储）。

**`+ Send` 约束（两处）**：HTTP 服务器在 Tokio 多线程运行时上处理请求，不同请求可能在不同线程上执行。`Fn + Send` 保证回调函数可以跨线程调用；`Future + Send` 保证异步执行期间不持有线程本地数据。

**`Arc<dyn Fn(...)>`**：将 `Fn` 包裹在 `Arc` 中，使同一个回调可以被注册表克隆并分发给多个 HTTP 处理器。`EngineRouteRegistry` 实现了 `Clone`，当 drt（`DistributedRuntime`）克隆自身时，所有已注册的路由回调会随之复制（引用计数加一，无数据复制）。

**为什么需要类型别名**：展开的类型过于冗长，在结构体定义、函数参数、方法返回值中重复书写易出错且难以阅读。类型别名将其收归一处，修改回调签名时只需改一处，所有引用处自动更新。

---

## 三、`EngineRouteRegistry` 结构体

### 为什么需要这个结构体

```rust
#[derive(Clone, Default)]
pub struct EngineRouteRegistry {
    routes: Arc<RwLock<HashMap<String, EngineRouteCallback>>>,
}
```

`EngineRouteRegistry` 在 `DistributedRuntime` 中作为一个字段存在（见 `distributed.rs`）。它需要满足两个约束：

1. **`DistributedRuntime` 是廉价克隆的**：DRT 的所有字段都是 `Arc<T>` 或 `Copy` 类型，`clone()` 只递增引用计数。`EngineRouteRegistry` 通过 `Arc<RwLock<HashMap<...>>>` 实现同等语义：外层 `Arc` 使克隆廉价，内层共享 Map 确保所有克隆持有同一份路由表。
2. **线程安全的动态注册**：引擎在运行时（非构造期）随时可能注册新路由，HTTP 服务器在另一个线程上并发读取路由表。`RwLock` 允许多个 HTTP 处理线程同时持读锁查找路由，写锁只在注册时短暂占用（注册操作仅在进程初始化阶段发生，不是热路径）。

**为什么用 `std::sync::RwLock` 而非 `tokio::sync::RwLock`**：路由的注册（`register()`）和查找（`get()`）都是同步操作——`register` 只是 `HashMap::insert`，`get` 只是 `HashMap::get` 加一个 `Arc::clone`，两者持锁时间在微秒级且没有任何 `await`。使用同步锁避免了 async 锁的 waker 机制开销，性能更好。HTTP 服务器在 await 回调执行结果之前已释放锁，不存在"持锁跨 await 点"的问题。

**`#[derive(Default)]`**：生成 `new()` 等价的无参构造，使 `DistributedRuntime::new()` 中可以用 `EngineRouteRegistry::new()` 或 `Default::default()` 构造空注册表，不需要额外的参数。

---

### 字段详解

**`routes: Arc<RwLock<HashMap<String, EngineRouteCallback>>>`**

路由名称到回调的 Map。键是路由名称（如 `"start_profile"`，不含 `/engine/` 前缀），HTTP 服务器在匹配 `/engine/{route}` 时提取 `{route}` 部分作为查找键。

不含前缀的原因：注册时只关心"这个操作叫什么名字"，前缀 `/engine/` 是 HTTP 服务器的挂载决策，不应该渗透到回调注册逻辑中。

Map 的容量通常在个位数（一个引擎暴露 2-5 个自定义端点），`HashMap` 的查找开销完全可以忽略，无需特殊优化。

---

## 四、`EngineRouteRegistry` 方法

### `new() -> Self`

```rust
pub fn new() -> Self {
    Self {
        routes: Arc::new(RwLock::new(HashMap::new())),
    }
}
```

创建空注册表。构造时不注册任何路由——路由由引擎在其初始化阶段按需注册，框架不预设任何引擎特定的端点。

---

### `register(route, callback)`

```rust
pub fn register(&self, route: &str, callback: EngineRouteCallback) {
    let mut routes = self.routes.write().unwrap();
    routes.insert(route.to_string(), callback);
    tracing::debug!("Registered engine route: /engine/{route}");
}
```

将回调插入 Map，后注册的同名路由会覆盖旧的（`HashMap::insert` 语义）。这允许引擎在初始化失败后重新注册同名路由（例如模型重载时替换旧的处理函数）。

`write().unwrap()`：`RwLock` 在持锁的线程 panic 时会毒化（poisoning），`unwrap()` 会传播这种 panic。路由注册只发生在进程初始化的早期阶段，此时如果发生 panic 说明有严重的编程错误，传播 panic 比静默忽略更符合 fail-fast 原则。

`tracing::debug!` 记录注册事件，方便调试时确认路由是否成功注册。

---

### `get(route) -> Option<EngineRouteCallback>`

```rust
pub fn get(&self, route: &str) -> Option<EngineRouteCallback> {
    let routes = self.routes.read().unwrap();
    routes.get(route).cloned()
}
```

查找并返回回调的 `Arc` 克隆（`EngineRouteCallback` 本身就是 `Arc<dyn Fn(...)>`，`.cloned()` 只递增引用计数）。返回克隆而非引用的原因：若返回引用，调用方需要持有读锁的生命周期，而读锁的 guard 不能跨 `await` 持有——HTTP 处理器在 `await` 回调结果期间若仍持读锁，会阻止注册操作写入新路由。返回克隆释放读锁后，调用方持有一个独立的 `Arc` 引用，可以安全地 `await` 回调。

返回 `Option`：路由不存在时返回 `None`，HTTP 服务器据此响应 `404 Not Found`。

---

### `routes() -> Vec<String>`

```rust
pub fn routes(&self) -> Vec<String> {
    let routes = self.routes.read().unwrap();
    routes.keys().cloned().collect()
}
```

返回所有已注册路由名称的列表。主要用途：

1. **系统状态端点的路由枚举**：HTTP 服务器启动时可以列出所有可用的引擎路由，用于 `/metadata` 端点的响应内容或启动日志。
2. **测试验证**：测试代码通过此方法断言期望的路由已注册。

---

## 五、测试设计

源码包含三个单元测试，覆盖三种关键行为：

**`test_registry_basic`**：验证注册、查找、不存在路由返回 `None`、`routes()` 列表正确。这是最基础的 CRUD 语义测试。

**`test_callback_execution`**：验证注册的回调能正确被 `await` 并返回期望值。测试点：`get()` 返回的 `Arc<Fn>` 可以被调用，且异步执行结果符合预期。这确保回调的类型签名在实际执行路径中是正确的，而非仅在编译期满足约束。

**`test_clone_shares_routes`**：验证 `Clone` 后两个注册表实例共享同一份路由数据。这是最重要的行为保证——`DistributedRuntime` 会被克隆并传入多个子系统，所有克隆必须看到相同的路由注册状态。测试通过在克隆后注册新路由，验证原始实例也能看到这个路由，确认共享语义（`Arc<RwLock<...>>`）的正确性。
