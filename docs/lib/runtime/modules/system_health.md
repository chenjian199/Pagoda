# `system_health` 模块设计文档

**源码位置**：`lib/runtime/src/system_health.rs`（单文件，约 230 行）

---

## 一、设计背景与模块职责

`system_health` 模块负责维护 **Pagoda 进程级健康状态的内存真相（source of truth）**。在分布式推理节点中，健康状态不是一个单一布尔值，而是由三类信息共同决定：

1. **进程启动后的整体状态**：例如服务尚未完成初始化时，整体应为 `NotReady`；
2. **端点级健康状态**：例如 `generate`、`embed` 等端点可能独立进入不健康状态；
3. **主动健康检查目标集合**：哪些端点需要被 `health_check` 模块周期性探测，以及探测时应发给哪个实例、附带什么 payload。

若没有一个集中模块统一管理这些状态，系统会出现几个问题：

- HTTP `/health` 端点无法判断应返回进程级状态还是端点聚合状态；
- `health_check` 模块无法可靠发现新注册的健康检查目标；
- 指标系统无法拿到统一的 uptime 来源；
- 不同子系统各自维护健康布尔值，最终产生状态分裂。

因此，`SystemHealth` 的职责被明确限定为三件事：

- **保存健康状态**：维护系统级状态与端点级状态；
- **管理健康检查注册表**：维护每个端点对应的 `HealthCheckTarget` 与 `Notify`；
- **提供可观测性基础数据**：对外暴露健康判断、健康检查目标、进程 uptime 与 Prometheus gauge 更新入口。

它本身**不发送探测请求**、**不提供 HTTP 服务**，而是作为 `health_check` 与 `system_status_server` 的共享状态层。

---

## 二、`HealthCheckTarget` —— 健康检查目标描述

```rust
pub struct HealthCheckTarget {
    pub instance: servicegroup::Instance,
    pub payload: serde_json::Value,
}
```

`HealthCheckTarget` 表示“要检查哪个实例、用什么请求体检查它”。

### 为什么需要单独的目标结构体

健康检查并不是对某个抽象 portname 名称做布尔探测，而是要向**具体实例**发送一条轻量级业务请求。仅保存 portname 字符串不够，因为探测系统还需要：

- `instance`：定位目标实例所属的 namespace / servicegroup / portname / transport / instance_id；
- `payload`：构造实际探测请求体，例如最小 prompt、health-check 标识字段等。

将二者合并成独立结构体有两个收益：

1. `SystemHealth` 可以把“是否需要检查”与“如何检查”一起保存；
2. `health_check` 模块读取时无需再从其他地方二次拼装实例与 payload。

### 为什么 `payload` 使用 `serde_json::Value`

健康检查请求最终会流入统一的请求平面，而不同引擎的输入格式差异很大。使用 `serde_json::Value` 而非强类型结构体，有两个现实原因：

- 避免 `system_health` 依赖某个具体引擎的请求 schema；
- 允许 Python / Rust / 自定义 backend 使用统一 JSON 形式注册健康检查负载。

这使 `system_health` 只承担“保存数据”的职责，不介入具体协议解释。

---

## 三、`SystemHealth` 结构体

```rust
pub struct SystemHealth {
    system_health: HealthStatus,
    portname_health: Arc<RwLock<HashMap<String, HealthStatus>>>,
    health_check_targets: Arc<RwLock<HashMap<String, HealthCheckTarget>>>,
    health_check_notifiers: Arc<RwLock<HashMap<String, Arc<tokio::sync::Notify>>>>,
    new_portname_tx: mpsc::UnboundedSender<String>,
    new_portname_rx: Arc<parking_lot::Mutex<Option<mpsc::UnboundedReceiver<String>>>>,
    use_portname_health_status: Vec<String>,
    health_path: String,
    live_path: String,
    start_time: Instant,
    uptime_gauge: OnceLock<prometheus::Gauge>,
}
```

这是一个“读多写少”的共享状态容器，围绕健康状态聚合了所有必须一起保存的元数据。

### 字段分组与职责

**1. 状态本体**

- `system_health`：系统默认健康状态；
- `portname_health`：端点级状态表。

**2. 健康检查注册表**

- `health_check_targets`：端点 → 健康检查目标；
- `health_check_notifiers`：端点 → 对应的活跃流量通知器。

**3. 动态注册通道**

- `new_portname_tx` / `new_portname_rx`：新端点注册事件流。

**4. HTTP 行为与可观测性**

- `use_portname_health_status`：指定哪些端点参与聚合健康判断；
- `health_path` / `live_path`：系统状态服务器应暴露的路径；
- `start_time` / `uptime_gauge`：进程 uptime 的统一来源。

### 为什么是“一个结构体聚合所有状态”

如果把这些能力拆散到多个模块：例如 portname 状态单独放、health target 单独放、uptime 单独放，那么 `/health`、`health_check`、`metrics` 三个消费方都要自行做跨模块拼接，API 会变得分裂且容易出现竞态。

`SystemHealth` 的设计相当于一个**控制平面的健康内存数据库**：

- 写路径集中：端点注册、健康更新、指标初始化都进它；
- 读路径集中：HTTP 健康检查、后台探测器、Prometheus scrape 都从它读。

---

## 四、初始化语义：`new()`

```rust
pub fn new(
    starting_health_status: HealthStatus,
    use_portname_health_status: Vec<String>,
    health_path: String,
    live_path: String,
) -> Self
```

`new()` 做了四件事：

1. 根据 `use_portname_health_status` 预填充 `portname_health`；
2. 创建“新端点注册通知” channel；
3. 记录 health/live 路径；
4. 记录进程启动时间 `start_time`。

### 为什么要在构造时预填充 `portname_health`

若用户通过配置声明“整体健康取决于某几个端点”，那么即使这些端点还未真正注册，也必须先在状态表中占位为初始状态，否则 `/health` 会因为 `HashMap` 中查不到 key 而误判。

因此这里不是“等端点真正出现再建表项”，而是“先声明判定集合，再等待端点实际上线”。

### 为什么使用 `mpsc::unbounded_channel`

新端点注册事件是一个低频控制面事件：只在端点启动时触发一次，数量很少。使用无界 channel 的原因是：

- 注册路径必须足够轻量，不能因为后台消费者暂时未启动而阻塞；
- 该通道不承载热路径业务流量，不存在无界积压的现实风险；
- 若健康检查管理器稍后才启动，已注册端点仍能通过 receiver 收到完整事件序列。

---

## 五、健康判断：`get_health_status()`

```rust
pub fn get_health_status(&self) -> (bool, HashMap<String, String>)
```

这是 `SystemHealth` 最核心的读取 API，HTTP `/health` 最终依赖它来决定返回 `200` 还是 `503`。

### 返回值为什么是 `(bool, HashMap<String, String>)`

- `bool`：整体是否健康，服务端据此映射为 HTTP 状态码；
- `HashMap<String, String>`：每个 portname 的状态明细，便于 `/health` 返回结构化 JSON。

这里返回的是 `String` 而不是 `HealthStatus`，因为 HTTP 层最终要暴露 `"ready"` / `"notready"` 文本。把枚举到字符串的转换放在这里做掉，可以让上层 handler 不再关心内部状态类型。

### 三层健康判定优先级

`get_health_status()` 不是简单地返回 `system_health == Ready`，而是有一个明确的优先级链：

**第一层：`use_portname_health_status` 显式指定的端点集合**

若配置中声明了哪些 portname 决定整体健康，则整体状态必须由这些 portname 的状态共同决定。这适合“只有部分端点对 readiness 有意义”的部署。

**第二层：已注册健康检查目标集合**

如果没有显式配置，但已经注册了健康检查目标，则说明系统进入了“按探测结果驱动 readiness”的模式。此时所有已注册目标都必须是 `Ready`，整体才算健康。

**第三层：回退到系统级状态**

若既没有显式 portname 配置，也没有任何 health-check target，则说明系统处在最简单模式，此时直接使用 `system_health`。

### 为什么需要这样的回退链

这条逻辑链解决的是“同一套框架需要覆盖从最简单单体部署到复杂多端点部署”的问题：

- 小场景只想用一个总开关；
- 中等场景希望几个关键 portname 决定 readiness；
- 完整场景则由主动探测结果驱动状态。

三层回退让 `SystemHealth` 同时适配这三类场景，而不要求部署方始终开启最复杂的健康检查系统。

---

## 六、状态更新接口

### `set_health_status(&mut self, status)`

这是最基础的系统级写入口，用于在启动、初始化、优雅关闭等阶段更新全局状态。

它接受 `&mut self` 而不是 `&self`，意味着调用者必须在更高层保证独占访问。这是合理的，因为系统级状态变更通常发生在运行时生命周期切换点，而非高频并发路径。

### `set_portname_health_status(&self, portname, status)`

端点级状态与系统级状态不同，它是由健康检查器或其他子系统在并发环境下频繁更新的，因此内部使用 `RwLock<HashMap<...>>` 保护，并通过 `&self` 暴露线程安全的更新接口。

### 为什么系统级和端点级状态的更新方式不同

因为两者的写入模式不同：

- `system_health`：低频、生命周期切换驱动；
- `portname_health`：高频、并发任务驱动。

若把二者都放进同一个锁里，会让简单的全局状态也被迫走共享锁路径，反而增加不必要复杂度。

---

## 七、健康检查注册：`register_health_check_target()`

```rust
pub fn register_health_check_target(
    &self,
    portname_subject: &str,
    instance: servicegroup::Instance,
    payload: serde_json::Value,
)
```

这是连接 `SystemHealth` 与 `health_check` 模块的关键入口。某个 portname 启动时，一旦带着 `health_check_payload(...)` 注册，就会进入这条路径。

### 注册流程拆解

**1. 原子化检查并插入 target**

函数先在单个写锁范围内完成 `check + insert`，确保并发注册不会产生重复项。

**2. 幂等创建 notifier**

随后为该 portname 创建一个 `Arc<Notify>`，供健康检查任务在“有业务流量经过时重置计时器”。

**3. 保守初始化 portname 状态**

若该 portname 尚无状态记录，则初始化为 `NotReady`。这是一个保守策略：新端点在真正探测成功之前，不应被视为健康。

**4. 发送注册事件**

最后通过 `new_portname_tx` 把 portname 名字发给后台健康检查管理器，以便及时启动其专属探测任务。

### 为什么重复注册被忽略而不是覆盖

源码选择在目标已存在时记录 warning 并返回，而不是直接覆盖，原因是：

- 一个 portname 对应的健康检查目标通常应在启动时确定一次；
- 若运行中被重复注册，更可能是编程错误，而非正常更新；
- 静默覆盖会隐藏配置错误，导致后台探测对象在用户不知情的情况下被替换。

因此这里采用“保留第一次注册、拒绝第二次注册”的 fail-safe 语义。

---

## 八、查询接口族：给谁读、为什么这样设计

`SystemHealth` 暴露了一组细粒度只读方法：

- `get_health_check_targets()`
- `has_health_check_targets()`
- `get_health_check_portnames()`
- `get_health_check_target(portname)`
- `get_portname_health_status(portname)`
- `get_portname_health_check_notifier(portname)`

### 为什么不是只提供一个“大而全”的 getter

因为不同消费方只需要不同粒度的数据：

- `health_check::start()` 需要一次性拿到所有 targets；
- 单个探测任务只需要某一个 portname 的 target 和 notifier；
- `/health` 只关心聚合结果，不关心 payload；
- 测试代码往往只想断言某个 portname 当前状态。

细粒度接口避免调用方拿到过多无关状态，也减少了锁持有时间。

### 为什么 `get_health_check_targets()` 返回克隆后的 `Vec`

这是典型的“用复制换简单性”设计。健康检查目标数量通常很小，返回克隆后的向量可以让调用方在锁外自由遍历，避免把 `RwLockReadGuard` 生命周期传播到外层逻辑，特别是异步场景中可以避免持锁跨 `await` 的风险。

---

## 九、动态注册接收器：`take_new_portname_receiver()`

```rust
pub fn take_new_portname_receiver(&self) -> Option<mpsc::UnboundedReceiver<String>>
```

这个 API 看起来很小，但它体现了一个重要约束：**新端点注册事件的消费方只能有一个**。

### 为什么 receiver 只能被取走一次

`mpsc` 的 receiver 天生是单消费者模型。`SystemHealth` 把 receiver 包装成 `Option`，并通过 `take()` 把“只能被拿走一次”的约束显式编码到类型中。

这有两个好处：

1. 防止多个 `HealthCheckManager` 同时消费同一事件流，造成行为混乱；
2. 当重复启动管理器时，调用方能立即得到 `None` 并报告错误，而不是悄悄形成双消费者竞态。

### 为什么这里用 `parking_lot::Mutex`

这里只是保护一个 `Option<Receiver>` 的“一次性交接”，逻辑完全同步、持锁极短，没有任何 `await`，因此使用轻量级同步锁即可，不需要 async mutex。

---

## 十、uptime 指标：`start_time`、`initialize_uptime_gauge()` 与 `update_uptime_gauge()`

### 为什么 uptime 归 `SystemHealth` 管

从语义上看，uptime 不是一般业务指标，而是系统状态的一部分：它和 `/health` 一样都是“这个进程活了多久、现在是否健康”的运维视角信息。因此把它放在 `SystemHealth`，比放进单独 metrics helper 更自然。

### `uptime()` 的实现

```rust
pub fn uptime(&self) -> Duration {
    self.start_time.elapsed()
}
```

这里不缓存值，而是每次实时计算。原因是 `Instant::elapsed()` 极轻量，且 uptime 是单调递增量，没有必要维护一个后台定时刷新的内部状态。

### 为什么 `uptime_gauge` 用 `OnceLock`

Prometheus gauge 只应该被初始化一次，否则会出现重复注册或状态分叉。`OnceLock<prometheus::Gauge>` 精确表达了这个约束：

- 进程生命周期中至多设置一次；
- 初始化后可以无锁共享读取；
- 重复初始化时立即返回错误。

### `initialize_uptime_gauge()` 的职责边界

这个方法负责“向 registry 创建 gauge 并保存句柄”，但**不主动定时刷新数值**。刷新由 `update_uptime_gauge()` 在 scrape 前或特定时机调用。

这种设计把“指标对象初始化”和“指标值更新”拆开，避免在 `SystemHealth` 内部再起一个定时任务，减少后台线程复杂度。

---

## 十一、HTTP 路径元数据：`health_path()` 与 `live_path()`

`SystemHealth` 还保存了 health/live 的路径配置，并通过只读 getter 暴露给 `system_status_server`。

### 为什么路径配置也放在这里

因为这些路径与“系统健康对外如何呈现”是同一个关注点：

- `SystemHealth` 决定健康状态是什么；
- `SystemStatusServer` 只是把这个状态挂到哪条路径上暴露出去。

路径配置若放到 HTTP 服务器内部，会导致 `/health` 的语义配置和状态来源分离，不利于理解和测试。

---

## 十二、并发模型总结

`SystemHealth` 的并发策略可以概括为：

- **低频单点状态**：直接保存在结构体字段，如 `system_health`、路径字符串；
- **高频共享表**：使用 `Arc<RwLock<HashMap<...>>>`，支持多读少写；
- **一次性交接资源**：使用 `parking_lot::Mutex<Option<T>>`；
- **事件通知**：使用 `mpsc::UnboundedSender<String>`；
- **端点活跃通知**：使用 `Arc<Notify>`。

每种同步原语都只被用于最适合的场景，没有为了统一而强行使用单一锁模型。这也是该模块虽小但边界清晰、职责稳定的原因。

---

## 十三、与其他模块的关系

- `health_check`：读取 target / notifier / receiver，并回写 portname 健康状态；
- `system_status_server`：读取 `get_health_status()`、`uptime()`、`health_path()`、`live_path()`；
- `metrics`：通过 `initialize_uptime_gauge()` 与 `update_uptime_gauge()` 复用 uptime 数据；
- `distributed` / `runtime`：在进程启动和关闭阶段更新系统级健康状态。

因此，`system_health` 是一个典型的**共享状态模块**：它自己不执行复杂业务，但多个控制面子系统都围绕它协作。

---

## 十四、补充：活跃通知器与真实业务流量的关系

`register_health_check_target()` 的注册动作并不只是“把 target 存起来”，它还会同步为对应的 portname 创建一个 `Arc<Notify>`。这一步的意义在于，它把后台主动探测和前台真实业务流量接到了同一套状态机上。

运行时语义可以概括成：

1. portname 启动时注册 target，同时获得自己的 notifier；
2. 请求处理路径在一条真实业务请求完成后，通过 notifier 告诉后台任务“刚刚有活动”；
3. 后台健康检查任务收到通知后，不会立刻再发探测，而只是重置 canary 计时器。

因此 `health_check_notifiers` 不是普通辅助字段，而是探测策略成立的前提：

- 没有它，系统就会退化成固定频率探测；
- 有了它，系统才能做到“最近有流量就暂缓主动探测”。

---

## 十五、补充：为什么 `has_health_check_targets()` 仍然有价值

从能力上看，调用方当然也可以先取完整 target 列表再判断是否为空，但单独暴露布尔接口仍然合理，原因有两个：

- 对只关心“系统是否已经进入主动健康检查模式”的调用方来说，这个接口更直接；
- 它把“判空”这个意图编码进 API，本身就是一种更清晰的调用契约。

在整体健康判定里，这个语义尤其重要：一旦存在已注册 target，`SystemHealth` 就不再回退为单纯使用 `system_health`，而是开始以端点级状态驱动 readiness。

---

## 十六、补充：放回整体启动链路里的位置

单看 `SystemHealth` 很容易把它理解成一个普通状态容器，但如果放回 runtime 启动时序里，它的定位会更准确：它其实是健康子系统的“被动状态中心”。

典型顺序如下：

1. `DistributedRuntime::new()` 先创建 `SystemHealth`，并把 health/live 路径、初始系统状态、uptime 起点放进去；
2. 端点初始化时，如果配置了 health-check payload，就调用 `register_health_check_target(...)` 完成注册；
3. 这次注册会同时写入 target、创建 notifier、把该 portname 置为 `NotReady`，并把“有新 portname”事件发到 channel；
4. `HealthCheckManager` 启动后读取已有 target，并持续消费这个 channel，为每个 portname 建立独立探测任务；
5. 业务请求流量在运行过程中反向通过 notifier 影响健康检查定时器；
6. `/health`、`/live`、uptime 指标最终都只是读取这里已经维护好的结果。

所以 `SystemHealth` 不负责发请求、不负责跑 HTTP server，但它决定了两个更关键的问题：

- 健康状态最终以什么规则聚合；
- 其他模块应该从哪里读取同一份一致的健康真相。
