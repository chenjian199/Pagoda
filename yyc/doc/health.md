# `health_check` 模块设计文档

**源码位置**：`lib/runtime/src/health_check.rs`（约 590 行）

---

## 一、设计背景与模块职责

Pagoda 的分布式部署中，Worker 节点可能在不宣告的情况下进入不健康状态——GPU OOM 导致推理引擎卡死、依赖服务超时使 Worker 不响应、或者 Python 进程崩溃但 etcd 租约尚未过期。在租约过期（通常 10 秒）之前，这些 Worker 仍然出现在服务发现结果中，路由层会持续将请求发送给它们，造成超时积压。

被动等待租约过期是不可接受的方案：10 秒内所有路由到故障 Worker 的请求都会失败，用户体验严重受损。

`health_check` 模块实现的是**主动探测（probing）方案**：框架定期向每个 Worker 发送轻量级探测请求，根据响应结果更新 `SystemHealth` 中的端点健康状态。`/health` 端点综合所有端点的健康状态决定整体健康。一旦某端点连续探测失败，路由层可以在下次 `/health` 查询时获知该端点不健康，提前将其从路由表摘除，无需等待 etcd 租约过期。

模块职责：管理每个 Endpoint 的健康检查任务生命周期，发送探测请求，更新健康状态。

---

## 二、`HealthCheckConfig` 结构体

### 为什么需要配置结构体

```rust
pub struct HealthCheckConfig {
    pub canary_wait_time: Duration,
    pub request_timeout: Duration,
}
```

健康检查有两个关键时间参数，不同部署场景的需求差异很大：

**`canary_wait_time`（探测间隔）**

含义：当某个端点一段时间内没有正常业务流量时，等待多长时间后发送一次探测请求。

设计原理："canary"（金丝雀）隐喻：探测请求是一只金丝雀，在矿坑（Worker）中感知有毒气体（不健康状态）。若 Worker 有正常业务请求在处理，说明它是健康的，不需要额外的探测（`Notify` 机制会重置计时器）；只有在空闲一段时间后，才需要主动探测确认 Worker 还活着。

典型值：生产环境 30 秒（频繁探测增加 Worker 负载），测试环境 5 秒（快速感知状态变化）。

**`request_timeout`（单次探测超时）**

含义：向 Worker 发送一次探测请求，等待响应的最长时间。超过此时间视为探测失败（Worker 不健康）。

典型值：需要大于 Worker 的 normal tail latency，但远小于 `canary_wait_time`。若设置过小，繁忙 Worker 的正常延迟也会超时；若设置过大，发现故障的延迟增加。

**`impl Default`**：源码中并非 `#[derive(Default)]`，而是手写 `impl Default for HealthCheckConfig`，将 `crate::config` 里的默认秒数常量转换为 `Duration`。这样既提供了合理的生产默认值，也保留了后续扩展默认构造逻辑的空间。

---

## 三、`RouterCache` 类型别名

```rust
type RouterCache =
    Arc<Mutex<HashMap<String, Arc<PushRouter<serde_json::Value, Annotated<serde_json::Value>>>>>>;
```

**为什么需要 Router 缓存**

每次发送健康检查请求都需要一个 `PushRouter`，而 `PushRouter` 的创建涉及网络连接（建立到 Worker 的传输层连接）和服务发现（查找 Worker 的 Instance 列表）。这些操作：
1. 是耗时的异步操作；
2. 创建的连接应该复用（避免频繁建立/断开 TCP 连接的开销）。

`RouterCache` 将 `portname_subject`（端点的唯一字符串标识，如 `"default.generate.v1"`）映射到已创建的 `PushRouter`，首次探测时创建，后续探测复用同一个 Router。

**为什么用 `portname_subject` 而非 `Endpoint` 对象做 Key**：`Endpoint` 是富类型（含 DRT 引用），不适合作为 HashMap Key（需要实现 Hash 等）。`portname_subject` 是 `String`，直接使用 `HashMap<String, ...>` 即可。

**`Arc<Mutex<...>>`**：`RouterCache` 在多个异步任务间共享（每个 Endpoint 有独立的健康检查任务），且访问发生在 async 上下文中（路由器创建涉及 await），必须使用 async 锁。`parking_lot::Mutex`（同步锁）仅适用于持锁时无 await 的场景。

注意此处实际使用的是 `parking_lot::Mutex`（源码 `use parking_lot::Mutex`），这是因为路由器查找（缓存命中路径）是纯内存操作，创建新路由器时先释放锁再创建（持锁期间无 await）——见 `get_or_create_router` 中两段独立的 `lock()` 块。

---

## 四、`HealthCheckManager` 结构体

### 为什么需要这个结构体

```rust
pub struct HealthCheckManager {
    drt: DistributedRuntime,
    config: HealthCheckConfig,
    router_cache: RouterCache,
    portname_tasks: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
}
```

健康检查是一个持续运行的后台系统，涉及多个并发任务（每个 Endpoint 一个任务）的生命周期管理。将状态集中在 `HealthCheckManager` 中，调用方只需创建一个 `Arc<HealthCheckManager>` 并调用 `start()`，内部的任务管理、缓存维护、状态更新都由 Manager 负责。

**`drt: DistributedRuntime`**：访问 `SystemHealth`（获取注册的探测目标、更新健康状态）和 `Namespace/Component/Endpoint` 对象（构造发送探测请求所需的路由路径）。DRT 是廉价克隆的，直接持有一份克隆。

**`config: HealthCheckConfig`**：保存探测间隔和超时时间，在任务内部可访问。

**`router_cache: RouterCache`**：见上节，所有探测任务共享同一个路由器缓存，避免重复创建连接。

**`portname_tasks: Arc<Mutex<HashMap<String, JoinHandle<()>>>>`**：追踪每个端点的后台探测任务句柄（`JoinHandle`）。用途：
1. **重复检测**：新端点注册时检查是否已有任务（防止同一端点被注册两次产生重复探测）；
2. **未来扩展**：持有 `JoinHandle` 使未来可以取消特定端点的探测任务（端点下线时停止相关探测）。

---

## 五、`HealthCheckManager::new()` — 管理器构造函数

```rust
pub fn new(drt: DistributedRuntime, config: HealthCheckConfig) -> Self
```

这是 `HealthCheckManager` 的唯一公开构造函数，职责很直接：接收运行时句柄与配置，初始化内部共享状态，但**不启动任何后台任务**。

对应实现：

```rust
Self {
    drt,
    config,
    router_cache: Arc::new(Mutex::new(HashMap::new())),
    portname_tasks: Arc::new(Mutex::new(HashMap::new())),
}
```

设计上将“构造”和“启动”分离有两个好处：

1. 调用方可以先创建 Manager，再决定何时 `start()`；
2. 单元测试可以只验证初始状态，无需真的拉起异步任务。

其中两个 `HashMap` 都在构造阶段初始化为空：

- `router_cache` 初始为空，表示尚未对任何端点建立探测路由；
- `portname_tasks` 初始为空，表示尚未为任何端点 spawn 探测任务。

---

## 六、`get_or_create_router()` — 路由缓存与懒创建

```rust
async fn get_or_create_router(
    &self,
    cache_key: &str,
    portname: Endpoint,
) -> anyhow::Result<Arc<PushRouter<serde_json::Value, Annotated<serde_json::Value>>>>
```

这个函数负责实现 RouterCache 的“读穿透”逻辑：缓存命中就直接复用，缓存未命中才为该端点创建新的 `Client` 和 `PushRouter`。

核心流程：

1. 将 `cache_key` 转为 `String`，便于后续存入 `HashMap`；
2. 第一段加锁：查询 `router_cache`，若已存在则直接返回；
3. 缓存未命中时释放锁，异步执行 `Client::new(portname).await?`；
4. 基于该 Client 构造 `PushRouter::from_client(...).await?`；
5. 第二段加锁：将新建 router 放入缓存；
6. 返回 `Arc<PushRouter<...>>`，供调用方复用。

对应实现骨架：

```rust
{
    let cache = self.router_cache.lock();
    if let Some(router) = cache.get(&cache_key) {
        return Ok(router.clone());
    }
}

let client = Client::new(portname).await?;
let router = Arc::new(
    PushRouter::from_client(client, crate::pipeline::RouterMode::RoundRobin).await?,
);

self.router_cache.lock().insert(cache_key, router.clone());
Ok(router)
```

这里有三个值得单独说明的点：

**1. 为什么缓存的是 `Arc<PushRouter<...>>`**

健康检查请求是持续发生的，而底层 `Client` / `PushRouter` 会维护服务发现与传输层状态。把 Router 放进 `Arc` 里缓存后，多次健康检查可以共享同一个路由对象，避免反复建连。

**2. 为什么构造时使用 `RouterMode::RoundRobin`，发送时却调用 `direct()`**

`PushRouter` 需要一个默认路由模式完成初始化；这里选 `RoundRobin` 只是满足对象构造要求。真正发送健康检查请求时，代码调用的是 `router.direct(request, instance_id)`，因此实际行为仍然是“定向发给指定实例”，不会被默认模式覆盖。

**3. 为什么要分成两段 `lock()`**

因为 `Client::new(...).await` 和 `PushRouter::from_client(...).await` 都是异步操作，不能在持有 `parking_lot::Mutex` 时跨 `await`。因此实现上先查缓存、释放锁、异步创建、再重新加锁写回。

这也带来一个实现细节：如果多个任务同时对同一端点首次探测，理论上可能出现“并发 miss，各自各建一个 router”的情况，最后由最后一次 `insert` 留在缓存里。该行为不会破坏正确性，只是可能在极低概率下产生一次重复初始化；当前实现选择的是更简单、且不会持锁跨 await 的方案。

---

## 七、`HealthCheckManager::start()` — 主启动流程

```rust
pub async fn start(self: Arc<Self>) -> anyhow::Result<()>
```

**为什么接受 `Arc<Self>` 而非 `&self`**：`start()` 内部调用 `spawn_portname_health_check_task()`，后者 spawn 了一个异步任务，任务中需要 `manager.clone()`。Tokio 的 `spawn` 要求 Future `'static`，所以任务不能持有 `&HealthCheckManager`（生命周期受限），必须持有 `Arc<HealthCheckManager>`。调用方传入 `Arc<Self>` 而非在 `start()` 内部 clone，明确表达调用方需要持有一个 Arc。

**启动流程**：

1. **读取已注册端点**：`drt.system_health().lock().get_health_check_targets()` 获取在 DRT 创建后、健康检查管理器启动前已注册的所有端点。这些端点由引擎在初始化阶段通过 `register_health_check_target()` 注册。
2. **为每个端点启动专属任务**：调用 `spawn_portname_health_check_task(portname_subject)`，每个端点有独立的 Tokio 任务，互不干扰（一个端点的探测失败不影响其他端点的探测计时）。
3. **启动动态发现监控**：调用 `spawn_new_portname_monitor().await?`——建立一个 channel receiver，监听管理器启动**之后**新注册的端点，为其及时启动探测任务。

---

## 八、`spawn_portname_health_check_task()` — 单端点探测任务

### 为什么每个端点一个独立任务

**设计对比**：

- **方案 A（单任务轮询所有端点）**：一个任务维护一个定时器列表，每次定时器触发向对应端点发探测。问题：一个端点的探测响应阻塞了其他端点的定时器检查；所有端点共享一个计时器重置逻辑，实现复杂。
- **方案 B（每端点独立任务，当前采用）**：每个端点有自己的 `canary_wait_time` 计时器和 `Notify` 通知器，完全独立。某端点的探测超时不影响其他端点；每个任务的逻辑简单清晰。

代价：任务数量等于端点数量（通常为个位数），Tokio 任务的开销极低（约 64 字节内存，无额外线程），此代价完全可接受。

**任务内部循环**：

```rust
loop {
    tokio::select! {
        _ = tokio::time::sleep(canary_wait) => {
            // 超时：发送探测请求
        }
        _ = notifier.notified() => {
            // 有业务流量：重置计时器，继续循环
        }
    }
}
```

`tokio::select!` 同时等待两个事件：
- 计时器超时（`canary_wait` 时间内无活动）→ 发送探测；
- `Notify::notified()`（有业务请求通过了这个端点）→ 重置计时器（循环重新开始，`sleep` 从头计时）。

**为什么用 `Notify` 而非 channel**：`Notify` 是"通知有事件发生过"的信号，不关心发生了多少次——5 个并发请求都调用 `notify_one()`，计时器只需重置一次。Channel 会积压所有通知，任务需要消费所有消息后才能继续，增加了不必要的复杂度。

**获取 Notifier**：

```rust
let notifier = self
    .drt
    .system_health()
    .lock()
    .get_portname_health_check_notifier(&portname_subject)
    .expect("Notifier should exist for registered portname");
```

`SystemHealth` 在 `register_health_check_target()` 时同步创建 `Arc<Notify>` 并存储。此处取出用于 select。`expect` 合理：若 notifier 不存在说明端点未正确注册，这是编程错误，panic 优于继续运行。

---

## 九、`spawn_new_portname_monitor()` — 动态端点发现

### 为什么需要这个机制

`start()` 在启动时读取已注册端点并启动任务。但推理框架通常是先启动基础设施，再逐步初始化各组件（模型加载完成后才注册端点）。若在 `HealthCheckManager::start()` 调用后才注册某个端点，该端点永远不会有探测任务——健康检查系统对其失明。

`spawn_new_portname_monitor()` 通过 `mpsc::channel` 解决这个问题：

- `SystemHealth::register_health_check_target()` 在注册时将端点名称发送到 channel；
- `spawn_new_portname_monitor()` 持有 channel 的 receiver，收到新端点时为其立即启动探测任务。

**为什么选 `mpsc::channel` 而非 `Notify` 或 `broadcast`**：需要传递具体的端点名称（不只是"有新端点"的信号），`Notify` 不携带数据。`mpsc` 保证每个注册事件都被接收（不丢失），顺序保证新端点都能得到处理。

**`take_new_portname_receiver()` 只能调用一次**：

```rust
let mut rx = manager
    .drt
    .system_health()
    .lock()
    .take_new_portname_receiver()
    .ok_or_else(|| anyhow::anyhow!("Endpoint receiver already taken"))?;
```

`SystemHealth` 中的 receiver 是 `Option<mpsc::Receiver<...>>`，`take()` 后变为 `None`。若重复调用 `start()` 或重复创建 `HealthCheckManager`，`take_new_portname_receiver()` 返回 `None`，`ok_or_else` 将其转为错误，使调用方立即感知到系统状态异常（重复启动是编程错误）。

**重复端点检测**：

```rust
if already_exists {
    error!(
        "CRITICAL: Received registration for portname '{}' that already has a health check task!",
        portname_subject
    );
    break;  // 退出监控任务
}
```

若同一端点被注册两次（编程错误），监控任务记录 CRITICAL 错误并退出。注释说明退出后不会有新端点被监控，这是一个需要人工介入的严重状态，用 `error!` + `break` 确保问题可见。

---

## 十、`send_health_check_request()` — 探测请求发送

### 完整发送流程

```rust
async fn send_health_check_request(
    &self,
    portname_subject: &str,
    payload: &serde_json::Value,
) -> anyhow::Result<()>
```

**步骤 1：获取探测目标信息**

```rust
let target = self.drt.system_health().lock().get_health_check_target(portname_subject)
    .ok_or_else(|| anyhow::anyhow!("No health check target found for {}", portname_subject))?;
```

从 `SystemHealth` 取出 `HealthCheckTarget`（包含 Instance 信息和探测 Payload）。Instance 信息指定了要探测哪个具体实例（`instance_id`），Payload 是预先准备好的探测请求体（通常是一个极小的推理请求，如最短的 prompt）。

**步骤 2：构建 Endpoint 对象**

```rust
let namespace = self.drt.namespace(&target.instance.namespace)?;
let servicegroup = namespace.servicegroup(&target.instance.servicegroup)?;
let portname = servicegroup.portname(&target.instance.portname);
```

从 DRT 出发，按命名层级构建 `Endpoint` 对象。此 Endpoint 对象被传给 `get_or_create_router` 用于创建或查找 PushRouter。

**步骤 3：等待实例发现完成**

```rust
match tokio::time::timeout(
    Duration::from_secs(10),
    router.client.wait_for_instances(),
).await {
    Ok(Ok(instances)) => { /* 继续 */ }
    Ok(Err(e)) => { return Err(...) }
    Err(_) => { return Err(...) }  // 10s 超时
}
```

这一步解决了竞态条件：健康检查任务可能在 Worker 刚注册后的很短时间内就触发（canary 计时器从头开始，而 etcd Watch 的实例列表还没有收到第一个更新）。若此时直接发送请求，PushRouter 没有可用实例，请求会失败。`wait_for_instances()` 等待 Watch 流完成初始同步，确保实例列表非空后再发送探测。10 秒超时防止无限等待。

**步骤 4：发送请求并在独立任务中处理响应**

```rust
tokio::spawn(async move {
    let result = tokio::time::timeout(timeout, async {
        match router.direct(request, instance_id).await {
            Ok(mut response_stream) => {
                let is_healthy = if let Some(response) = response_stream.next().await {
                    !response.err().is_some()  // 有响应且不是错误
                } else {
                    false  // 无响应
                };
                // 消费剩余流（避免前端 warning）
                tokio::spawn(async move {
                    response_stream.for_each(|_| async {}).await;
                });
                // 更新健康状态
                system_health.lock().set_portname_health_status(...)
            }
            Err(e) => { /* 请求失败，标记 NotReady */ }
        }
    }).await;
    if result.is_err() { /* 超时，标记 NotReady */ }
});
```

**为什么在 spawn 的子任务中处理响应**：探测请求的发送和响应处理是"触发即忘"的——发出请求后健康检查任务继续监听下一个事件（计时器或通知），不需要等待这次探测的结果。等待响应的逻辑在独立任务中运行，不阻塞探测任务的事件循环。

**`router.direct(request, instance_id)`**：使用 Direct routing（直接指定 instance_id），而非 RoundRobin。原因：健康检查是针对特定 Worker 实例的，不应路由到其他实例（那样就检测不到目标实例的状态了）。

**消费剩余流的原因**：LLM 推理返回的是流式响应（多个 token）。健康检查只需第一个 token 来确认引擎活跃，不需要完整响应。但不消费剩余流会在框架层产生 "stream dropped prematurely" 的 warning，对运维者造成困惑。因此 spawn 另一个任务静默消费剩余内容。

---

## 十一、公开函数

### `start_health_check_manager(drt, config)`

```rust
pub async fn start_health_check_manager(
    drt: DistributedRuntime,
    config: Option<HealthCheckConfig>,
) -> anyhow::Result<()>
```

模块对外暴露的主入口，由 `DistributedRuntime::new()` 在步骤 11 调用（条件启动）。接受 `Option<HealthCheckConfig>`，为 `None` 时使用默认配置，减少调用方的配置负担。

内部创建 `Arc<HealthCheckManager>` 并调用 `start()`，对调用方完全隐藏 Manager 的生命周期管理细节——调用方不需要持有 Manager 的引用，Manager 通过任务句柄和 Arc 自身维持存活。

### `get_health_check_status(drt) -> anyhow::Result<serde_json::Value>`

```rust
pub async fn get_health_check_status(
    drt: &DistributedRuntime,
) -> anyhow::Result<serde_json::Value>
```

为 `/health` HTTP 端点提供状态汇总的工具函数。返回格式：

```json
{
    "status": "ready",
    "portnames_checked": 3,
    "portname_statuses": {
        "default.generate.v1": {"healthy": true, "status": "Ready"},
        "default.prefill.v1": {"healthy": false, "status": "NotReady"}
    }
}
```

完整逻辑分为三步：

1. 调用 `get_health_check_portnames()` 取得所有已注册端点名；
2. 遍历每个端点，调用 `get_portname_health_status()` 读取状态；若状态缺失则保守地按 `HealthStatus::NotReady` 处理；
3. 组装 `portname_statuses` JSON，并使用 `all(...)` 判断是否所有端点都健康。

对应实现骨架：

```rust
let portname_subjects: Vec<String> = drt.system_health().lock().get_health_check_portnames();

let mut portname_statuses = HashMap::new();
{
    let system_health = drt.system_health();
    let system_health_lock = system_health.lock();
    for portname_subject in &portname_subjects {
        let health_status = system_health_lock
            .get_portname_health_status(portname_subject)
            .unwrap_or(HealthStatus::NotReady);

        let is_healthy = matches!(health_status, HealthStatus::Ready);
        portname_statuses.insert(...);
    }
}

let overall_healthy = portname_statuses
    .values()
    .all(|v| v["healthy"].as_bool().unwrap_or(false));
```

这里的设计意图有两点：

**1. 缺失状态默认视为 `NotReady`**

健康检查系统面向的是故障场景，保守判定比乐观判定安全。如果某个端点已经注册，但状态尚未成功写入，`/health` 不应把它误报为健康。

**2. 整体状态采用 AND 逻辑**

所有端点都 ready 才返回 `"ready"`。任何一个端点 `NotReady`，整体就变成 `"notready"`，使路由层可以通过一次 GET `/health` 就获知是否可以安全地继续向该 Worker 发送流量。

从实现上看，这个函数本身不发送探测请求，也不修改状态；它只是读取 `SystemHealth` 当前快照并转换为 HTTP 层友好的 JSON 结构，因此非常适合直接挂在探针或运维接口上。

返回结果在语义上通常接近：

```json
{
    "status": "ready",
    "portnames_checked": 2,
    "portname_statuses": {
        "default.generate.v1": {
            "healthy": true,
            "status": "Ready"
        }
    }
}
```

这里最值得注意的是：它输出的是**状态汇总快照**，不是一次即时探测的返回值。也就是说，调用这个函数不会触发新的 health check，请求侧读到的是后台任务此前已经回写进 `SystemHealth` 的结果。

---

## 十二、补充：单端点任务的事件循环语义

虽然 `send_health_check_request()` 负责真正发请求，但整个模块的调度核心其实在每个端点各自的后台任务里。其行为可以概括成下面这个事件循环：

```rust
loop {
        tokio::select! {
                _ = tokio::time::sleep(canary_wait) => {
                        // 空闲超时后才触发一次主动探测
                }
                _ = notifier.notified() => {
                        // 最近有真实业务流量，重置计时器
                }
        }
}
```

这里最关键的设计点不是“定期探测”，而是“**空闲后探测**”：

- 如果某个 portname 最近持续有真实业务请求流过，那么 `Notify` 会不断重置定时器，后台任务不会频繁额外发 canary；
- 只有在一段时间没有活动时，才通过主动请求确认目标实例仍然能沿正常业务路径处理请求；
- 这让健康检查更接近“补充验证”，而不是与真实流量抢资源的固定频率轮询器。

从系统行为看，这意味着当前实现验证的不是“进程还活着”这种粗粒度状态，而是“该实例现在还能不能收下一个真实请求并返回首个响应”。

---

## 十三、补充：与 `system_health` / `system_status_server` 的协作链路

如果把健康检查放回整个 runtime 启动流程里看，这个模块的职责边界会更清楚：

1. `SystemHealth` 先保存 health-check target、portname 状态和 activity notifier；
2. 端点启动时调用 `register_health_check_target(...)` 完成注册，并把自己的 notifier 注入请求处理路径；
3. `start_health_check_manager()` 创建 `HealthCheckManager`，先为已注册 portname 启动任务，再监听后续动态注册事件；
4. 真实业务请求完成后，handler 调用 `notify_one()`，对应 portname 的 canary 计时器被重置；
5. 当某个 portname 长时间无流量时，health-check task 才会发送一次 canary 请求，并把结果回写到 `SystemHealth`；
6. `system_status_server` 的 `/health` 与 `/live` 再从 `SystemHealth` 读取聚合后的结果并对外返回。

因此 `health_check` 在三个模块里扮演的是“主动执行器”角色：

- `system_health` 负责保存状态；
- `health_check` 负责制造和更新状态；
- `system_status_server` 负责把状态暴露成 HTTP 接口。
