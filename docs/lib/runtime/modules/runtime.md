# `runtime` 模块设计文档

**源码位置**：`lib/runtime/src/runtime.rs`（393 行）

---

## 一、设计背景与模块职责

`Runtime` 是 Pagoda 节点级共享资源的统一访问入口。Pagoda 的组件系统中，每个 `ServiceGroup`、`Namespace`、`Portname` 都需要访问以下资源：

- **Tokio 线程池**：执行异步任务（请求处理、网络 I/O）
- **计算线程池**：执行 CPU 密集型操作（token 编解码、请求预处理），避免阻塞 Tokio worker 线程
- **取消令牌**：协调各组件的生命周期和优雅关闭
- **优雅关闭追踪器**：确保关闭时正在处理的请求能完成

若每个组件各自持有 Tokio Handle，线程池无法共享，资源利用率低；若直接传递各种资源的引用，API 会变得复杂。`Runtime` 将这些资源聚合为一个轻量克隆对象，组件只需持有一个 `Runtime` 即可访问所有共享资源。

---

## 二、核心类型定义

### `RuntimeType` 枚举

```rust
enum RuntimeType {
    Shared(Arc<ManuallyDrop<tokio::runtime::Runtime>>),
    External(tokio::runtime::Handle),
}
```

- `Shared`：表示运行时由 `Runtime` 自己创建并拥有。
- `External`：表示复用外部传入的 Tokio 运行时句柄，`Runtime` 不负责底层 runtime 的所有权释放。

### `Runtime` 结构体

```rust
pub struct Runtime {
    id: Arc<String>,
    primary: RuntimeType,
    secondary: RuntimeType,
    cancellation_token: CancellationToken,
    portname_shutdown_token: CancellationToken,
    graceful_shutdown_tracker: Arc<GracefulShutdownTracker>,
    compute_pool: Option<Arc<compute::ComputePool>>,
    block_in_place_permits: Option<Arc<tokio::sync::Semaphore>>,
}
```

字段职责如下：

- `id`：运行时实例唯一标识，构造时用 UUID 生成。
- `primary`：主 Tokio 运行时，承载请求处理、推理执行、API 服务等前台任务。
- `secondary`：后台 Tokio 运行时，承载 etcd watch、NATS 心跳、服务发现等后台任务。
- `cancellation_token`：主取消令牌，用于最终关闭所有附着组件与后端连接。
- `portname_shutdown_token`：主令牌的子令牌，用于先停止 Portname 接受新请求。
- `graceful_shutdown_tracker`：跟踪优雅关闭期间仍在执行的端点任务数量。
- `compute_pool`：可选的专用 CPU 计算线程池。
- `block_in_place_permits`：用于限制 `block_in_place` 并发数量的信号量。

先集中列出 `Runtime` 的完整定义，比把字段拆散到多个章节里更清楚；后续章节再分别解释它的不同设计面向。

---

## 三、构造函数与初始化路径

### 基础构造函数 `new`

```rust
fn new(runtime: RuntimeType, secondary: Option<RuntimeType>) -> anyhow::Result<Runtime>
```

`new()` 是 `Runtime` 的基础内部构造函数，负责完成以下初始化：

- 调用 `crate::nvtx::init()`，按环境变量初始化 NVTX 功能开关。
- 生成 `id`。
- 创建主取消令牌 `cancellation_token`。
- 基于主令牌派生 `portname_shutdown_token`。
- 决定 `secondary` 运行时来源：
  - 若调用方显式传入 `secondary`，直接使用。
  - 若未传入，则自动创建一个单线程 Tokio runtime 作为后台运行时。
- 初始化 `graceful_shutdown_tracker`。
- 将 `compute_pool` 与 `block_in_place_permits` 初始化为 `None`。

也就是说，`new()` 只负责构造最基本的共享运行时资源，不负责根据配置创建计算池。

### 配置增强构造函数 `new_with_config`

```rust
fn new_with_config(
    runtime: RuntimeType,
    secondary: Option<RuntimeType>,
    config: &RuntimeConfig,
) -> anyhow::Result<Runtime>
```

`new_with_config()` 在 `new()` 的基础上增加“按配置初始化计算资源”的逻辑：

- 先调用 `Self::new(runtime, secondary)` 构造基础 `Runtime`。
- 根据 `RuntimeConfig` 组装 `crate::compute::ComputeConfig`。
- 若 `config.compute_threads == Some(0)`，则显式禁用计算池。
- 否则尝试创建 `ComputePool`：
  - 创建成功：写入 `rt.compute_pool`。
  - 创建失败：仅记录 warning，后续 CPU 密集任务退回 `spawn_blocking`。
- 根据 `num_worker_threads` 或系统可用并行度计算 `block_in_place_permits`。
- 许可数采用 `num_workers.saturating_sub(1).max(1)`，确保至少保留一个 Tokio worker 线程处理 async 任务。

因此，`new_with_config()` 是“基础运行时 + 计算资源配置”的完整初始化入口。

### 对外构造接口

```rust
pub fn from_current() -> anyhow::Result<Runtime>
pub fn from_handle(handle: tokio::runtime::Handle) -> anyhow::Result<Runtime>
pub fn from_settings() -> anyhow::Result<Runtime>
pub fn single_threaded() -> anyhow::Result<Runtime>
```

它们分别对应不同构造路径：

- `from_current()`：从当前 Tokio 上下文获取 `Handle`，再转交给 `from_handle()`。
- `from_handle()`：将同一个外部 `Handle` 同时作为 `primary` 与 `secondary`，再调用 `new()`。
- `from_settings()`：读取 `RuntimeConfig`，创建受 `Runtime` 拥有的 primary runtime，并调用 `new_with_config()`。
- `single_threaded()`：构造测试用单线程运行时，并在未显式提供 `secondary` 时让 `new()` 自动创建单线程后台运行时。

### 为什么需要区分 `new()` 和 `new_with_config()`

- 嵌入式场景通常只能复用外部已有 runtime，不适合额外创建和管理 compute pool，此时走 `new()`。
- 独立部署场景需要从配置完整初始化运行时与计算资源，此时走 `new_with_config()`。

---

## 四、双运行时架构

`Runtime` 采用主/辅两个 Tokio 运行时：

- `primary`：处理推理请求、组件执行、API 服务等前台业务。
- `secondary`：处理 etcd watch、NATS 心跳、服务发现等后台任务。

这样设计的原因是：后台连接在网络故障与重连风暴期间会产生大量短时阻塞 I/O。若这些任务与请求处理共用同一线程池，可能拖慢前台推理请求。将它们隔离到 `secondary` 后，即使后台运行时忙于重连，`primary` 仍能保持前台吞吐。

当通过 `from_current()` / `from_handle()` 从外部运行时构造时，`primary` 与 `secondary` 会指向同一个 Tokio runtime；这是为了嵌入场景下避免重复创建线程池。

### 运行时句柄访问

```rust
pub fn primary(&self) -> tokio::runtime::Handle
pub fn secondary(&self) -> tokio::runtime::Handle
```

- `primary()` 返回主运行时句柄。
- `secondary()` 返回后台运行时句柄。

此外，`RuntimeType` 还提供一个统一的句柄提取方法：

```rust
pub fn handle(&self) -> tokio::runtime::Handle
```

它屏蔽了 `Shared` 和 `External` 两种内部存储形式的差异。

---

## 五、分层取消令牌与优雅关闭

### 取消令牌层级

```text
cancellation_token（主）
  └── portname_shutdown_token（子）
        └── child_token()（孙，每个 Portname 一个）
```

对应接口如下：

```rust
pub fn primary_token(&self) -> CancellationToken
pub fn child_token(&self) -> CancellationToken
```

- `primary_token()` 返回主取消令牌克隆，供 NATS/etcd 等后台连接监听。
- `child_token()` 返回 `portname_shutdown_token` 的子令牌，供每个 Portname 控制自身生命周期。

这种分层设计使“停止接受新请求”和“最终断开后端连接”成为两个独立阶段。

### 三阶段关闭流程

```rust
pub fn shutdown(&self)
```

`shutdown()` 并不直接同步关闭资源，而是把关闭协调任务投递到 `primary()`：

1. 取消 `portname_shutdown_token`，让各 Portname 停止接收新请求。
2. 通过 `GracefulShutdownTracker` 等待正在处理的请求全部完成。
3. 最后取消 `cancellation_token`，断开 NATS/etcd 等后端连接。

这样可以保证请求处理完毕后再关闭后端依赖，符合优雅关闭语义。

---

## 六、计算线程池与线程本地初始化

### 计算池访问器

```rust
pub fn compute_pool(&self) -> Option<&Arc<crate::compute::ComputePool>>
```

该方法返回当前 `Runtime` 是否持有专用 `ComputePool`。返回 `Option` 的原因是：

- 某些构造路径（如 `from_current()` / `from_handle()`）不会初始化计算池。
- `new_with_config()` 尝试创建计算池失败时，会降级到 `spawn_blocking`。

### 当前线程初始化

```rust
pub fn initialize_thread_local(&self)
```

若 `compute_pool` 和 `block_in_place_permits` 已配置，该方法会在当前线程上初始化 compute thread-local 上下文，并为当前线程设置 NVTX 名称，便于 Nsight Systems 时间线分析。

该方法适用于调用方已明确知道自己正处于某个 Tokio worker 线程上的场景。

### 全 worker 线程初始化

```rust
pub async fn initialize_all_thread_locals(&self) -> anyhow::Result<()>
```

该方法会：

- 先探测实际 worker 线程数量。
- 创建 `Barrier`，确保所有参与初始化的线程同时进入同步点。
- 为每个 worker 线程提交一个 `spawn_blocking` 任务。
- 在各线程内写入 compute thread-local 上下文。

### worker 数量探测辅助函数

```rust
async fn detect_worker_thread_count(&self) -> usize
```

该内部辅助函数通过多次 `spawn_blocking` 收集实际执行线程的 `ThreadId` 集合，推断运行时真实 worker 数量，而不是依赖静态配置值。这对于 `from_current()` / `from_handle()` 这类外部 runtime 场景尤其重要。

---

## 七、运行时标识与销毁语义

### 实例标识访问器

```rust
pub fn id(&self) -> &str
```

返回运行时实例的唯一字符串 ID，用于日志关联、调试与运行时实例区分。

### 为什么使用 `ManuallyDrop`

`RuntimeType::Shared` 使用 `Arc<ManuallyDrop<tokio::runtime::Runtime>>` 包裹 runtime，是为了避免在异步上下文中直接 drop Tokio runtime 导致 panic：

> Cannot drop a runtime in a context where blocking is not allowed.

源码通过 `impl Drop for RuntimeType` 区分两种情况：

- 若当前处于异步上下文，则取出 runtime 并调用 `shutdown_background()`。
- 若当前不在异步上下文，则执行正常 drop。

这样可以兼容 PyO3 / Python asyncio 等嵌入式场景下的销毁行为。

---

## 八、补充：当前实现里的 primary / secondary 不是总是物理分离

前文把 `primary` / `secondary` 解释成前台与后台双运行时，这个设计意图本身没有问题；但结合当前源码实现，还需要补充一个更精确的事实：这种分离并不是所有构造路径下都严格成立。

当前几条构造路径的行为分别是：

- `from_current()` 只是转调 `from_handle(Handle::current())`；
- `from_handle()` 会把同一个外部 handle 同时包成 `primary` 和 `secondary`；
- `from_settings()` 虽然会创建一个 owned runtime，但 `secondary` 仍然只是 `runtime.handle().clone()` 的 `External` 包装；
- 只有 `secondary == None` 时，`Runtime::new()` 才会额外补一个单线程 secondary runtime。

因此，当前实现更准确的说法是：

- API 层保留了 primary / secondary 两级执行上下文抽象；
- 是否真的是两个独立 Tokio runtime，要看具体构造路径。

---

## 九、补充：`block_in_place_permits` 的真实设计意图

`new_with_config()` 除了创建 compute pool，还会根据 worker 线程数创建一个 `block_in_place` 许可信号量：

```rust
let permits = num_workers.saturating_sub(1).max(1);
```

这个公式的核心语义是：**始终至少留一个 Tokio worker 线程给 async 调度**。

例如：

- 如果 runtime 有 8 个 worker，那么只发 7 个 permit；
- 这样同一时刻最多只有 7 个线程能进入 `block_in_place`；
- 至少还会剩 1 个线程继续处理定时器、网络 I/O 和其他 async task。

它要防的不是普通性能下降，而是更严重的“所有 worker 全被同步阻塞拖死，事件循环完全停转”。

---

## 十、补充：线程本地初始化的实际边界

`initialize_thread_local()` 和 `initialize_all_thread_locals()` 这组 API 的目标，是把 compute pool 和 `block_in_place_permits` 写进线程本地上下文，以减少后续 CPU 密集路径首次懒初始化时的抖动。

但当前实现还有一个很重要的边界需要写清楚：

- `initialize_all_thread_locals()` 通过 `spawn_blocking` + `Barrier` 批量初始化；
- `detect_worker_thread_count()` 也是通过提交多个 `spawn_blocking` 任务，统计实际落到哪些线程；
- 所以这里观测到的更接近“当前 blocking 执行线程集合”，而不是 Tokio async worker 线程数的严格官方 API。

换句话说，这套实现更准确地说是在**预热 blocking 执行线程上的 thread-local 上下文**，而不是对所有 async worker 线程做绝对保证。

---

## 十一、补充：三阶段关闭的顺序语义

`shutdown()` 当前不会同步阻塞地直接把所有资源关掉，而是先在 primary runtime 上投递一个协调任务，然后按三阶段执行：

1. 取消 `portname_shutdown_token`，停止 portname 接受新请求；
2. 等待 `GracefulShutdownTracker` 中仍在执行的 portname 工作完成；
3. 最后再取消主 `cancellation_token`，让后端连接与后台任务退出。

这条顺序的目的，是把“停止接流量”和“断开底层连接”分开处理：

- Phase 1 先阻止新请求继续进入；
- Phase 2 给 in-flight 请求留出完成窗口；
- Phase 3 才真正让 etcd / NATS 等依赖开始退出。

因此这个关闭过程更像一个非阻塞入口，而不是一次同步销毁动作。

---

## 十二、补充：`RuntimeType::Drop` 为什么要特殊处理

`RuntimeType::Shared` 用 `Arc<ManuallyDrop<tokio::runtime::Runtime>>` 包 runtime，不只是为了“手动控制 drop”，更直接的动机是：**Tokio runtime 在 async 上下文里被直接 drop 会 panic**。

当前 `Drop for RuntimeType` 的语义是：

- `External(handle)` 不归自己所有，因此不负责销毁；
- `Shared(runtime)` 只有在自己是最后一个 owner 时才会实际进入 drop；
- 若检测到当前线程已经在 async runtime 内，则调用 `shutdown_background()`；
- 否则才走正常 drop。

源码注释里还明确点名了一个实际触发场景：Python / pyo3 绑定把 runtime 从异步上下文里释放时，如果没有这层处理，就会直接撞上 Tokio 的 panic 保护。
