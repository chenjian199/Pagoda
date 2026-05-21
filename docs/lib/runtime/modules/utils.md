# `utils` 模块设计文档

**源码位置**：`lib/runtime/src/utils/`（子目录，多文件）

---

## 一、模块概览

`utils` 是运行时层的基础设施工具箱。当前目录下的实际文件结构为：

```text
src/utils/
  ├── graceful_shutdown.rs     — 优雅关闭追踪器
  ├── ip_resolver.rs           — 本地 IP 解析与环境变量回退
  ├── pool.rs                  — 通用对象池与 RAII 借出令牌
  ├── stream.rs                — 截止时间驱动的 Stream 包装器
  ├── task.rs                  — 旧入口，对 critical 任务 API 的兼容性 re-export
  ├── tasks.rs                 — tasks 子模块声明入口
  ├── typed_prefix_watcher.rs  — 类型化 etcd 前缀监听器
  └── tasks/
      ├── critical.rs          — 关键任务执行句柄
      └── tracker.rs           — 层级化任务调度与错误处理系统
```

`task.rs` 和 `tasks.rs` 这两个入口文件虽然不承载复杂逻辑，但它们属于目录结构的一部分，也影响模块导出路径，因此在本节一并说明。

---

## 二、`graceful_shutdown.rs` — 优雅关闭追踪器

### 核心结构

```rust
pub struct GracefulShutdownTracker {
    active_portnames: AtomicUsize,
    shutdown_complete: Notify,
}
```

这里需要注意其可见性分层：结构体本身是 `pub`，但其构造与主要方法仍然是 `pub(crate)`，也就是说它对 crate 内可实例化、可操作，对 crate 外并不暴露完整控制面。

### crate 内方法定义

```rust
pub(crate) fn new() -> Self
pub(crate) fn register_portname(&self)
pub(crate) fn unregister_portname(&self)
pub(crate) fn get_count(&self) -> usize
pub(crate) async fn wait_for_completion(&self)
```

这些方法分别承担：

- `new()`：初始化计数器和 `Notify`
- `register_portname()`：端点注册时加一
- `unregister_portname()`：端点结束时减一；若从 1 变为 0，则唤醒等待者
- `get_count()`：获取当前活动端点数
- `wait_for_completion()`：循环等待直到活动计数归零

### 双重检查等待模式

```rust
pub(crate) async fn wait_for_completion(&self) {
    loop {
        let notified = self.shutdown_complete.notified();
        let count = self.active_portnames.load(Ordering::SeqCst);
        if count == 0 {
            break;
        }
        notified.await;
    }
}
```

先创建 `notified()` 再检查计数，是为了避免“最后一个端点恰好在检查之后完成，导致错过通知”的竞态问题。

---

## 三、`ip_resolver.rs` — IP 解析与环境变量回退

### 核心私有辅助函数定义

```rust
fn resolve_local_ip_with_resolver<R: IpResolver>(resolver: R) -> IpAddr
fn format_ip_for_url(addr: IpAddr) -> String
```

这两个函数虽然不是公开 API，但它们实际上承载了本模块最核心的行为：

- `resolve_local_ip_with_resolver<R: IpResolver>(resolver: R)`：真正执行 IPv4 -> IPv6 -> `127.0.0.1` 的解析与回退逻辑。上层几个公开函数本质上都是围绕它做包装。
- `format_ip_for_url(addr: IpAddr)`：把解析得到的 `IpAddr` 转成可直接用于 URL/host:port 拼接的字符串；若是 IPv6，则自动补 `[]`。

这两个内部定义构成了 `ip_resolver.rs` 的核心实现层次，因此在设计文档中一并列出。

### 公开函数定义

```rust
pub fn get_local_ip_for_advertise_with_resolver<R: IpResolver>(resolver: R) -> String
pub fn get_local_ip_for_advertise() -> String
pub fn get_http_rpc_host_with_resolver<R: IpResolver>(resolver: R) -> String
pub fn get_http_rpc_host() -> String
pub fn get_http_rpc_host_from_env() -> String
pub fn get_tcp_rpc_host_from_env() -> String
```

其中有两个公开的测试友好入口：

- `get_local_ip_for_advertise_with_resolver`
- `get_http_rpc_host_with_resolver`

这两个函数允许调用方显式传入 `IpResolver` 实现，因此是可测试性设计的一部分，而不只是内部辅助函数。

### 解析逻辑

内部回退链为：

1. 先尝试 `resolver.local_ip()`
2. 若返回 `LocalIpAddressNotFound`，再尝试 `resolver.local_ipv6()`
3. 若仍失败，则回退到 `127.0.0.1`

### IPv6 URL 格式化

私有辅助函数 `format_ip_for_url` 会为 IPv6 地址自动加方括号，使最终返回值可安全拼接 `:{port}` 组成 URL 或 socket 地址字符串。

### 环境变量覆盖

- `get_http_rpc_host_from_env()` 优先读取 `PGD_HTTP_RPC_HOST`
- `get_tcp_rpc_host_from_env()` 优先读取 `PGD_TCP_RPC_HOST`

若环境变量不存在，则两者都会回退到本地 IP 解析逻辑。

---

## 四、`pool.rs` — 对象池与借出令牌

这一文件包含多个公开 trait 与便捷构造方法，是对象池抽象的主体部分。

### 核心 trait

```rust
pub trait Returnable: Send + Sync + 'static {
    fn on_return(&mut self) {}
}

pub trait ReturnHandle<T: Returnable>: Send + Sync + 'static {
    fn return_to_pool(&self, value: PoolValue<T>);
}

pub trait PoolExt<T: Returnable>: Send + Sync + 'static {
    fn create_pool_item(
        &self,
        value: PoolValue<T>,
        handle: Arc<dyn ReturnHandle<T>>,
    ) -> PoolItem<T>
}
```

- `Returnable`：定义对象归还时的清理钩子
- `ReturnHandle`：定义把对象送回池中的动作
- `PoolExt`：为池实现方提供统一的 `PoolItem` 构造入口

### `PoolValue<T>`

```rust
pub enum PoolValue<T: Returnable> {
    Boxed(Box<T>),
    Direct(T),
}
```

除了类型定义，当前源码还公开了以下方法：

```rust
pub fn from_boxed(value: Box<T>) -> Self
pub fn from_direct(value: T) -> Self
pub fn get(&self) -> &T
pub fn get_mut(&mut self) -> &mut T
pub fn on_return(&mut self)
```

这些方法负责在“盒装对象”和“直接对象”两种存储形式之间提供统一访问面。

### `PoolItem<T>` 与 `SharedPoolItem<T>`

```rust
pub struct PoolItem<T: Returnable> {
    value: Option<PoolValue<T>>,
    handle: Arc<dyn ReturnHandle<T>>,
    _token: private::PoolItemToken,
}

pub struct SharedPoolItem<T: Returnable> {
    inner: Arc<PoolItem<T>>,
}
```

公开方法为：

```rust
pub fn into_shared(self) -> SharedPoolItem<T>
pub fn has_value(&self) -> bool

pub fn get(&self) -> &T
pub fn strong_count(&self) -> usize
```

除 `into_shared()` 外，`has_value()`、`SharedPoolItem::get()` 和 `SharedPoolItem::strong_count()` 也共同组成了共享借出句柄的使用面。

### `Pool<T>` 与 `SyncPool<T>`

```rust
pub struct Pool<T: Returnable> {
    state: Arc<PoolState<T>>,
    capacity: usize,
}

pub struct SyncPool<T: Returnable> {
    state: Arc<SyncPoolState<T>>,
    capacity: usize,
}

pub struct SyncPoolItem<T: Returnable> {
    value: Option<PoolValue<T>>,
    state: Arc<SyncPoolState<T>>,
}
```

当前源码里，`Pool<T>` 的公开方法只有：

```rust
pub fn new(initial_elements: Vec<PoolValue<T>>) -> Self
pub fn new_boxed(initial_elements: Vec<Box<T>>) -> Self
pub fn new_direct(initial_elements: Vec<T>) -> Self
```

注意：异步 `Pool<T>` 对外公开的接口集中在构造函数；`Pool::try_acquire()`、`Pool::acquire()`、`Pool::capacity()` 都是私有方法，不属于对外公开定义。

而 `SyncPool<T>` 的公开接口则是：

```rust
pub fn new(initial_elements: Vec<PoolValue<T>>) -> Self
pub fn new_direct(initial_elements: Vec<T>) -> Self
pub fn try_acquire(&self) -> Option<SyncPoolItem<T>>
pub fn acquire_blocking(&self) -> SyncPoolItem<T>
pub fn capacity(&self) -> usize
```

因此，这里应当区分两类池的公开使用面：同步池公开 API 更完整，而异步池当前对外只公开构造接口。

### 关键私有结构定义

`pool.rs` 里还有几类不会对外导出，但对实现机制很关键的结构：

```rust
struct PoolState<T: Returnable> {
    pool: Arc<Mutex<VecDeque<PoolValue<T>>>>,
    available: Arc<Notify>,
}

struct SyncPoolState<T: Returnable> {
    pool: Mutex<VecDeque<PoolValue<T>>>,
    available: Condvar,
}

mod private {
    pub struct PoolItemToken(());
}
```

- `PoolState<T>`：异步池的真实共享状态，维护对象队列和 `Notify`
- `SyncPoolState<T>`：同步池的真实共享状态，维护对象队列和 `Condvar`
- `PoolItemToken`：阻止外部直接伪造 `PoolItem<T>` 的防伪标记

### 主要函数/方法功能概括

- `PoolValue::from_boxed`：把 `Box<T>` 包装为池值
- `PoolValue::from_direct`：把直接值 `T` 包装为池值
- `PoolValue::get/get_mut`：统一读取底层对象引用
- `PoolValue::on_return`：在归还前触发 `Returnable::on_return`
- `PoolItem::into_shared`：把独占借出令牌转为共享借出句柄
- `PoolItem::has_value`：检查当前借出令牌内部是否仍持有对象
- `SharedPoolItem::get`：读取共享借出对象
- `SharedPoolItem::strong_count`：查看共享引用计数
- `Pool::new/new_boxed/new_direct`：构造异步对象池
- `SyncPool::new/new_direct`：构造同步对象池
- `SyncPool::try_acquire`：尝试无阻塞地借出对象
- `SyncPool::acquire_blocking`：阻塞等待直到有对象可借出
- `SyncPool::capacity`：返回池容量

---

## 五、`stream.rs` — 截止时间流包装器

### 公开定义

```rust
pub struct DeadlineStream<S> {
    stream: S,
    sleep: Pin<Box<Sleep>>,
}

pub fn until_deadline<S: Stream + Unpin>(stream: S, deadline: Instant) -> DeadlineStream<S>
```

`DeadlineStream` 的外部构造入口是 `until_deadline<S: Stream + Unpin>(stream: S, deadline: Instant)`，使用时通常通过该函数创建包装器。

### 语义

`DeadlineStream` 通过在 `poll_next` 里同时检查：

- 截止时间睡眠 future 是否已完成
- 底层 stream 是否产生下一项

来实现“截止时间到就自然终止 stream”的语义。这个模式适合 `while let Some(item) = stream.next().await` 这种自然消费循环。

---

## 六、`typed_prefix_watcher.rs` — 类型化 etcd 前缀监听器

### 核心类型

```rust
pub struct TypedPrefixWatcher<K, V> {
    rx: watch::Receiver<HashMap<K, V>>,
}
```

### 结构体公开方法

```rust
pub fn receiver(&self) -> watch::Receiver<HashMap<K, V>>
pub fn current(&self) -> HashMap<K, V>
```

该类型提供两个重要的读取接口：

- `receiver()`：返回 watch receiver clone，供调用方订阅后续变化
- `current()`：直接返回当前快照的克隆

### 工厂函数

```rust
pub async fn watch_prefix_with_extraction<K, V, T>(
    client: EtcdClient,
    prefix: impl Into<String>,
    key_extractor: impl Fn(&KeyValue) -> Option<K> + Send + 'static,
    value_extractor: impl Fn(T) -> Option<V> + Send + 'static,
    cancellation_token: CancellationToken,
) -> Result<TypedPrefixWatcher<K, V>>

pub async fn watch_prefix<K, V>(
    client: EtcdClient,
    prefix: impl Into<String>,
    key_extractor: impl Fn(&KeyValue) -> Option<K> + Send + 'static,
    cancellation_token: CancellationToken,
) -> Result<TypedPrefixWatcher<K, V>>
```

两者的职责分别如下：

- `watch_prefix_with_extraction`：先反序列化为 `T`，再经 `value_extractor` 投影为 `V`
- `watch_prefix`：直接把反序列化值本身当作 `V`

### 主要函数/方法功能概括

- `TypedPrefixWatcher::receiver`：返回一个 `watch::Receiver` 克隆，供外部持续监听状态变化
- `TypedPrefixWatcher::current`：立即返回当前完整状态快照的克隆
- `watch_prefix_with_extraction`：监听前缀、反序列化值并提取目标字段，持续维护 `HashMap<K, V>`
- `watch_prefix`：监听前缀并直接缓存完整反序列化值
- `key_extractors::lease_id`：用 etcd lease ID 作为键
- `key_extractors::key_string`：生成“去除前缀后的 key 字符串”提取器
- `key_extractors::full_key_string`：保留 etcd 原始 key 字符串

### `key_extractors` 子模块

该模块还提供了一个公开辅助子模块：

```rust
pub mod key_extractors {
    pub fn lease_id(kv: &KeyValue) -> Option<u64>
    pub fn key_string(prefix: &str) -> impl Fn(&KeyValue) -> Option<String>
    pub fn full_key_string(kv: &KeyValue) -> Option<String>
}
```

这些函数分别提供：

- `lease_id`：用 etcd lease 作为键
- `key_string`：去掉给定前缀后的 key 字符串
- `full_key_string`：保留原始完整 key 字符串

---

## 七、`task.rs` 与 `tasks.rs` — 模块入口与兼容 re-export

### `tasks.rs`

```rust
pub mod critical;
pub mod tracker;
```

这是 `utils::tasks` 命名空间的模块入口。

功能概括：该文件本身不承载业务逻辑，只负责把 `critical` 和 `tracker` 两个子模块挂到 `utils::tasks::*` 命名空间下。

### `task.rs`

```rust
pub use super::tasks::critical::*;
```

这是一个兼容性 re-export 文件，使旧路径仍可通过 `utils::task` 访问 critical task API。

功能概括：该文件用于兼容旧导入路径，避免调用方必须立即从 `utils::task::*` 迁移到 `utils::tasks::critical::*`。

---

## 八、`tasks/critical.rs` — 关键任务执行句柄

### 类型别名与核心结构

```rust
pub type CriticalTaskHandler<Fut> = dyn FnOnce(CancellationToken) -> Fut + Send + 'static;

pub struct CriticalTaskExecutionHandle {
    monitor_task: JoinHandle<()>,
    graceful_shutdown_token: CancellationToken,
    result_receiver: Option<oneshot::Receiver<Result<()>>>,
    detached: bool,
}
```

除设计思路外，这里也列出 `CriticalTaskExecutionHandle` 的公开方法定义清单。

### 公开方法定义

```rust
pub fn new<Fut>(
    task_fn: impl FnOnce(CancellationToken) -> Fut + Send + 'static,
    parent_token: CancellationToken,
    description: &str,
) -> Result<Self>

pub fn new_with_runtime<Fut>(
    task_fn: impl FnOnce(CancellationToken) -> Fut + Send + 'static,
    parent_token: CancellationToken,
    description: &str,
    runtime: &Handle,
) -> Result<Self>

pub fn is_finished(&self) -> bool
pub fn is_cancelled(&self) -> bool
pub fn cancel(&self)
pub async fn join(self) -> Result<()>
pub fn detach(self)
```

职责分别是：

- `new`：在当前 Tokio runtime 上创建 critical task
- `new_with_runtime`：显式指定运行时句柄
- `is_finished`：检查 monitor task 是否结束
- `is_cancelled`：检查 graceful shutdown token 是否已取消
- `cancel`：只触发子 token 的优雅取消，不直接取消父 token
- `join`：等待监控任务返回最终结果，并传播原始错误
- `detach`：允许句柄 drop 后任务继续运行

源码还通过 `Drop` 实现强制要求：若句柄在未 `detach` 且未被 `join` 消费时直接 drop，会 panic。这是为了避免 critical task 被无意遗失。

### 主要函数/方法功能概括

- `new`：在当前运行时创建一个关键任务及其监控任务
- `new_with_runtime`：在指定 Tokio `Handle` 上创建关键任务
- `is_finished`：检查监控任务是否已结束
- `is_cancelled`：检查优雅关闭子令牌是否已取消
- `cancel`：请求关键任务自行优雅退出
- `join`：等待关键任务最终结果，并返回成功、失败或 panic 包装后的错误
- `detach`：放弃句柄所有权，但允许任务继续运行

---

## 九、`tasks/tracker.rs` — 层级化任务调度与错误处理系统

这是 `utils` 目录里最复杂的子模块之一，对外公开了一整套“调度策略 + 错误策略 + continuation + metrics + builder”框架。

### 核心错误与句柄类型

```rust
pub enum TaskError {
    Cancelled,
    Failed(anyhow::Error),
    TrackerClosed,
}

pub struct TaskHandle<T> {
    join_handle: JoinHandle<Result<T, TaskError>>,
    cancel_token: CancellationToken,
}
```

公开方法包括：

```rust
pub fn is_cancellation(&self) -> bool
pub fn is_failure(&self) -> bool
pub fn into_anyhow(self) -> anyhow::Error

pub fn cancellation_token(&self) -> &CancellationToken
pub fn abort(&self)
pub fn is_finished(&self) -> bool
```

其中 `TaskHandle<T>` 本身还实现了 `Future`，因此可直接 `.await`。

### continuation 机制

```rust
pub trait Continuation: Send + Sync + Debug + Any {
    async fn execute(
        &self,
        cancel_token: CancellationToken,
    ) -> TaskExecutionResult<Box<dyn Any + Send + 'static>>;
}

pub struct FailedWithContinuation {
    pub source: anyhow::Error,
    pub continuation: Arc<dyn Continuation + Send + Sync + 'static>,
}

pub trait FailedWithContinuationExt {
    fn extract_continuation(&self) -> Option<Arc<dyn Continuation + Send + Sync + 'static>>;
    fn has_continuation(&self) -> bool;
}
```

`FailedWithContinuation` 允许任务在失败时携带“下一步继续执行什么”的 continuation，当前源码公开了以下便捷构造：

```rust
pub fn new(
    source: anyhow::Error,
    continuation: Arc<dyn Continuation + Send + Sync + 'static>,
) -> Self

pub fn into_anyhow(
    source: anyhow::Error,
    continuation: Arc<dyn Continuation + Send + Sync + 'static>,
) -> anyhow::Error

pub fn from_fn<F, Fut, T>(source: anyhow::Error, f: F) -> anyhow::Error
pub fn from_cancellable<F, Fut, T>(source: anyhow::Error, f: F) -> anyhow::Error
```

这部分能力共同组成了失败后续执行与重试编排的基础机制。

### 当前还应显式列出的关键私有辅助定义

除公开类型外，`tracker.rs` 还有几类对理解内部执行流程很关键的私有定义：

```rust
enum GuardState {
    Keep,
    Reschedule,
}

trait TaskExecutor<T>: Send {
    async fn execute(&mut self, cancel_token: CancellationToken) -> TaskExecutionResult<T>;
}
```

- `GuardState`：控制 continuation 重试时，是复用当前调度许可，还是重新走调度器获取执行槽位。
- `TaskExecutor<T>`：统一抽象“普通任务 future”和“可取消任务闭包”的内部执行接口，是 `execute_with_retry_loop` 的直接依赖。

这两者虽然是私有定义，但它们是 `TaskTracker` 内部执行状态机的重要组成部分。

### 调度与错误策略抽象

```rust
pub enum SchedulingPolicy {
    Unlimited,
    Semaphore(usize),
}

pub struct OnErrorContext {
    pub attempt_count: u32,
    pub task_id: TaskId,
    pub execution_context: TaskExecutionContext,
    pub state: Option<Box<dyn Any + Send + 'static>>,
}

pub trait OnErrorPolicy: Send + Sync + Debug {
    fn create_child(&self) -> Arc<dyn OnErrorPolicy>;
    fn create_context(&self) -> Option<Box<dyn Any + Send + 'static>>;
    fn on_error(&self, error: &anyhow::Error, context: &mut OnErrorContext) -> ErrorResponse;
    fn allow_continuation(&self, error: &anyhow::Error, context: &OnErrorContext) -> bool;
    fn should_reschedule(&self, error: &anyhow::Error, context: &OnErrorContext) -> bool;
}

pub enum ErrorPolicy {
    LogOnly,
    CancelOnError,
    CancelOnPatterns(Vec<String>),
    CancelOnThreshold { max_failures: usize },
    CancelOnRate { max_failure_rate: f32, window_secs: u64 },
}

pub enum ErrorResponse {
    Fail,
    Shutdown,
    Custom(Box<dyn OnErrorAction>),
}

pub trait OnErrorAction: Send + Sync + Debug {
    async fn execute(
        &self,
        error: &anyhow::Error,
        task_id: TaskId,
        attempt_count: u32,
        context: &TaskExecutionContext,
    ) -> ActionResult;
}

pub enum ActionResult {
    Fail,
    Continue { continuation: Arc<dyn Continuation + Send + Sync + 'static> },
    Shutdown,
}

pub struct TaskExecutionContext {
    pub scheduler: Arc<dyn TaskScheduler>,
    pub metrics: Arc<dyn HierarchicalTaskMetrics>,
}

pub enum TaskExecutionResult<T> {
    Success(T),
    Cancelled,
    Error(anyhow::Error),
}

pub trait ArcPolicy: Sized + Send + Sync + 'static {
    fn new_arc(self) -> Arc<Self>;
}
pub struct TaskId(Uuid);
pub enum CompletionStatus {
    Ok,
    Cancelled,
    Failed(String),
}

pub enum CancellableTaskResult<T> {
    Ok(T),
    Cancelled,
    Err(anyhow::Error),
}

pub enum SchedulingResult<T> {
    Execute(T),
    Cancelled,
    Rejected(String),
}

pub trait ResourceGuard: Send + 'static {}

pub trait TaskScheduler: Send + Sync + Debug {
    async fn acquire_execution_slot(
        &self,
        cancel_token: CancellationToken,
    ) -> SchedulingResult<Box<dyn ResourceGuard>>;
}

pub trait HierarchicalTaskMetrics: Send + Sync + Debug {
    fn increment_issued(&self);
    fn increment_started(&self);
    fn increment_success(&self);
    fn increment_cancelled(&self);
    fn increment_failed(&self);
    fn increment_rejected(&self);
    fn issued(&self) -> u64;
    fn started(&self) -> u64;
    fn success(&self) -> u64;
    fn cancelled(&self) -> u64;
    fn failed(&self) -> u64;
    fn rejected(&self) -> u64;
}
```

这些抽象共同构成了 `TaskTracker` 的设计骨架。

其中 `TaskId` 还包含两个相关定义：

```rust
impl TaskId {
    fn new() -> Self
}

impl std::fmt::Display for TaskId
```

- `TaskId::new()` 是私有构造函数，由 tracker 内部生成全局唯一任务 ID。
- `Display for TaskId` 则把它格式化成 `task-<uuid>`，用于日志和 tracing 字段输出。

### 关键私有结构定义

除 `GuardState` 和 `TaskExecutor<T>` 外，还有三类对执行路径很关键的私有结构：

```rust
struct RegularTaskExecutor<F, T> {
    future: Option<F>,
    _phantom: PhantomData<T>,
}

struct CancellableTaskExecutor<F, Fut, T> {
    task_fn: F,
}

struct TaskTrackerInner {
    tokio_tracker: TokioTaskTracker,
    parent: Option<Arc<TaskTrackerInner>>,
    scheduler: Arc<dyn TaskScheduler>,
    error_policy: Arc<dyn OnErrorPolicy>,
    metrics: Arc<dyn HierarchicalTaskMetrics>,
    cancel_token: CancellationToken,
    children: RwLock<Vec<Weak<TaskTrackerInner>>>,
}
```

- `RegularTaskExecutor`：把普通 `Future<Output = Result<T>>` 适配进统一执行管线
- `CancellableTaskExecutor`：把接收 `CancellationToken` 的任务闭包适配进统一执行管线
- `TaskTrackerInner`：`TaskTracker` 背后的真实状态载体，维护调度策略、错误策略、指标和子 tracker 树

### 主要函数/方法功能概括

- `TaskError::is_cancellation`：判断错误是否属于取消分支
- `TaskError::is_failure`：判断错误是否属于任务失败分支
- `TaskError::into_anyhow`：把 `TaskError` 转成兼容旧接口的 `anyhow::Error`
- `TaskHandle::cancellation_token`：取出当前任务独立取消令牌
- `TaskHandle::abort`：直接中止底层 Tokio 任务
- `TaskHandle::is_finished`：检查任务是否完成
- `FailedWithContinuation::new`：构造带 continuation 的失败对象
- `FailedWithContinuation::into_anyhow`：把 continuation 失败包装成 `anyhow::Error`
- `FailedWithContinuation::from_fn`：用普通异步闭包构造 continuation 失败
- `FailedWithContinuation::from_cancellable`：用可取消异步闭包构造 continuation 失败
- `TaskMetrics::new`：创建纯内存指标收集器
- `PrometheusTaskMetrics::new`：创建带 Prometheus 注册的指标收集器
- `ChildTrackerBuilder::new`：从父 tracker 开始构造子 tracker
- `ChildTrackerBuilder::scheduler`：覆写子 tracker 的调度策略
- `ChildTrackerBuilder::error_policy`：覆写子 tracker 的错误策略
- `ChildTrackerBuilder::build`：生成子 tracker 并挂入父 tracker 层级
- `TaskTrackerBuilder::scheduler/error_policy/metrics/cancel_token`：配置 root tracker 的构建参数
- `TaskTrackerBuilder::build`：生成 root tracker
- `TaskTracker::builder`：返回 builder 风格入口
- `TaskTracker::new`：以简化参数创建 root tracker
- `TaskTracker::new_with_prometheus`：创建带 Prometheus 指标的 root tracker
- `TaskTracker::child_tracker`：生成继承策略的子 tracker
- `TaskTracker::spawn`：提交普通异步任务
- `TaskTracker::spawn_cancellable`：提交接收取消令牌的任务
- `TaskTracker::metrics`：返回本 tracker 指标接口
- `TaskTracker::cancel`：取消当前 tracker 及其任务
- `TaskTracker::is_closed`：检查是否已关闭，不再接受新任务
- `TaskTracker::cancellation_token`：获取 tracker 级取消令牌
- `TaskTracker::child_count`：统计当前存活子 tracker 数量
- `TaskTracker::child_tracker_builder`：创建子 tracker builder
- `TaskTracker::join`：关闭并等待整个 tracker 层级完成退出
- `UnlimitedScheduler::new`：创建无限并发调度器
- `SemaphoreScheduler::new`：基于现有信号量创建并发限制调度器
- `SemaphoreScheduler::with_permits`：按 permit 数直接创建调度器
- `SemaphoreScheduler::available_permits`：查询当前剩余 permit 数
- `CancelOnError::new`：创建“任意错误即 shutdown”的策略
- `CancelOnError::with_patterns`：创建“匹配错误模式才 shutdown”的策略
- `LogOnlyPolicy::new`：创建只记录错误不级联关闭的策略
- `ThresholdCancelPolicy::with_threshold`：创建失败次数阈值策略
- `ThresholdCancelPolicy::failure_count`：读取累计失败次数
- `ThresholdCancelPolicy::reset_failure_count`：重置失败次数计数
- `RateCancelPolicy::builder`：返回失败率策略 builder
- `RateCancelPolicyBuilder::rate`：设置最大允许失败率
- `RateCancelPolicyBuilder::window_secs`：设置失败率统计窗口
- `RateCancelPolicyBuilder::build`：生成失败率策略与其取消令牌
- `TriggerCancellationTokenAction::new`：构造一个执行时触发取消令牌的自定义动作
- `TriggerCancellationTokenOnError::new`：构造一个“出错即触发外部取消令牌”的策略

### metrics 类型

```rust
pub struct TaskMetrics
pub struct PrometheusTaskMetrics
```

公开构造函数：

```rust
pub fn new() -> Self
pub fn new<R: MetricsHierarchy>(registry: &R, servicegroup_name: &str) -> anyhow::Result<Self>
```

分别对应：

- 纯内存 metrics
- Prometheus 集成 metrics

### tracker builder 与主类型

```rust
pub struct ChildTrackerBuilder<'parent>
pub struct TaskTracker(Arc<TaskTrackerInner>)
pub struct TaskTrackerBuilder
```

公开方法包括：

```rust
pub fn new(parent: &'parent TaskTracker) -> Self
pub fn scheduler(mut self, scheduler: Arc<dyn TaskScheduler>) -> Self
pub fn error_policy(mut self, error_policy: Arc<dyn OnErrorPolicy>) -> Self
pub fn build(self) -> anyhow::Result<TaskTracker>

pub fn scheduler(mut self, scheduler: Arc<dyn TaskScheduler>) -> Self
pub fn error_policy(mut self, error_policy: Arc<dyn OnErrorPolicy>) -> Self
pub fn metrics(mut self, metrics: Arc<dyn HierarchicalTaskMetrics>) -> Self
pub fn cancel_token(mut self, cancel_token: CancellationToken) -> Self
pub fn build(self) -> anyhow::Result<TaskTracker>

pub fn builder() -> TaskTrackerBuilder
pub fn new(scheduler: Arc<dyn TaskScheduler>, error_policy: Arc<dyn OnErrorPolicy>) -> anyhow::Result<Self>
pub fn new_with_prometheus<R: MetricsHierarchy>(
    scheduler: Arc<dyn TaskScheduler>,
    error_policy: Arc<dyn OnErrorPolicy>,
    registry: &R,
    servicegroup_name: &str,
) -> anyhow::Result<Self>

pub fn child_tracker(&self) -> anyhow::Result<TaskTracker>
pub fn spawn<F, T>(&self, future: F) -> TaskHandle<T>
pub fn spawn_cancellable<F, Fut, T>(&self, task_fn: F) -> TaskHandle<T>
pub fn metrics(&self) -> &dyn HierarchicalTaskMetrics
pub fn cancel(&self)
pub fn is_closed(&self) -> bool
pub fn cancellation_token(&self) -> CancellationToken
pub fn child_count(&self) -> usize
pub fn child_tracker_builder(&self) -> ChildTrackerBuilder<'_>
pub async fn join(&self)
```

这部分构成了 `tracker.rs` 的主要公开使用面，也是外部集成该模块时最常使用的入口集合。

### 内置调度器与错误策略

该模块提供的内置实现包括：

```rust
pub struct UnlimitedGuard;
pub struct UnlimitedScheduler;
pub struct SemaphoreGuard;
pub struct SemaphoreScheduler;
pub struct CancelOnError;
pub struct LogOnlyPolicy;
pub struct ThresholdCancelPolicy;
pub struct RateCancelPolicy;
pub struct RateCancelPolicyBuilder;
pub struct TriggerCancellationTokenAction;
pub struct TriggerCancellationTokenOnError;
```

对应的重要构造方法有：

```rust
pub fn new() -> Arc<Self>
pub fn new(semaphore: Arc<Semaphore>) -> Self
pub fn with_permits(permits: usize) -> Arc<Self>
pub fn available_permits(&self) -> usize
pub fn with_patterns(error_patterns: Vec<String>) -> (Arc<Self>, CancellationToken)
pub fn with_threshold(max_failures: usize) -> Arc<Self>
pub fn failure_count(&self) -> u64
pub fn reset_failure_count(&self)
pub fn builder() -> RateCancelPolicyBuilder
pub fn rate(mut self, max_failure_rate: f32) -> Self
pub fn window_secs(mut self, window_secs: u64) -> Self
pub fn build(self) -> (Arc<RateCancelPolicy>, CancellationToken)
pub fn new(cancel_token: CancellationToken) -> Self
pub fn new(cancel_token: CancellationToken) -> Arc<Self>
```

这些策略类型决定了 `TaskTracker` 在并发控制、失败统计和级联取消中的实际行为。

---

