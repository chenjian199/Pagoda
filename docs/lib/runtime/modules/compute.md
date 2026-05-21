# `compute` 模块设计

**源码**：`src/compute/mod.rs` · `src/compute/pool.rs` · `src/compute/metrics.rs` · `src/compute/thread_local.rs` · `src/compute/macros.rs` · `src/compute/validation.rs`

---

## 一、模块定位

`compute` 模块提供一个**独立于 Tokio 的 Rayon 线程池**，专用于 CPU 密集型操作（LLM token 处理、批量解码、向量计算等），并通过 `tokio-rayon` 异步桥接机制与 Tokio 集成。

核心设计原则：

- **物理隔离**：Rayon 线程池与 Tokio worker 线程池完全独立，CPU 计算不抢占 I/O 调度线程
- **工作窃取**：Rayon 内置 work-stealing 调度，多任务并发提交时自动负载均衡
- **异步桥接**：通过 `tokio-rayon` 将同步 Rayon 操作包装为 Tokio Future，外部用 `await` 等待
- **分级宏**：提供 `compute_small!` / `compute_medium!` / `compute_large!` 三级宏按任务耗时选择执行策略

---

## 二、文件结构与可见性

```
src/compute/
    ├── mod.rs            — pub 入口：ComputeConfig / ScopeExecutor trait / 内联 patterns 子模块
    ├── pool.rs           — pub ComputePool / ComputeHandle<T> / ComputePoolExt trait
    ├── metrics.rs        — pub ComputeMetrics
    ├── thread_local.rs   — pub ComputeContext / initialize_context / with_context / get_pool / …
    ├── macros.rs         — #[macro_export] compute_small! / compute_medium! / compute_large!
    └── validation.rs     — pub（feature-gated）validate_small / validate_medium / validate_large
```

注意：`patterns` 不是单独的 `patterns.rs` 文件，而是定义在 `mod.rs` 内部的**内联子模块**。

**re-export**（`mod.rs`）：

```rust
pub use metrics::ComputeMetrics;
pub use pool::{ComputeHandle, ComputePool, ComputePoolExt};
```

---

## 三、类型详解

---

### 3.1 `ComputeConfig` — 线程池配置

**来源**：`src/compute/mod.rs`

```rust
#[derive(Debug, Clone)]
pub struct ComputeConfig {
    pub num_threads:   Option<usize>,  // None → clamp(cpu/2, 2, 16)
    pub stack_size:    Option<usize>,  // 默认 Some(2MB)
    pub thread_prefix: String,         // 线程命名前缀，默认 "compute"
    pub pin_threads:   bool,           // CPU 绑定（预留，未实现）
}
```

**实现的 trait**：


| Trait             | 来源     | 说明                                                                                   |
| ----------------- | ------ | ------------------------------------------------------------------------------------ |
| `Debug` / `Clone` | derive | 标准                                                                                   |
| `Default`         | 手写     | `num_threads=None, stack_size=Some(2MB), thread_prefix="compute", pin_threads=false` |


**自身方法**：

```rust
impl ComputeConfig {
    pub fn validate(&self) -> Result<()>
    // num_threads == Some(0) → Err（0 线程无意义）
    // stack_size < 128KB → Err（最小推荐 128KB）

    pub(crate) fn build_pool(&self) -> Result<rayon::ThreadPool>
    // 1. validate()
    // 2. ThreadPoolBuilder::new()
    // 3. num_threads 计算：
    //    - Some(n) → n 线程
    //    - None → available_parallelism / 2，clamp(2, 16)，检测失败默认 2
    // 4. 若 stack_size 为 Some，则 builder.stack_size(size)
    // 5. 用 AtomicU64 线程计数器生成 thread_name：{prefix}-{id}
    // 6. pin_threads 目前仅保留配置位，源码里尚未接 start_handler 绑定 CPU
    // 7. builder.build()，构建失败时 map_err 为 anyhow 错误
}
```

`build_pool()` 不是简单的 builder 包装，而是 `ComputeConfig` 到 Rayon 线程池实例的**唯一收口点**：

- `validate()` 先行，保证非法配置不会流入 Rayon；
- 默认线程数策略直接编码在这里，而不是分散在调用方；
- 线程命名使用 `AtomicU64` + `Ordering::SeqCst` 递增编号，确保同一池内线程名稳定为 `compute-0`、`compute-1`…；
- `pin_threads` 当前只是目标设计预留位，文档应明确“已建模、未实现”，避免误以为当前已有 CPU 亲和性逻辑；
- `builder.build()` 的底层错误被统一包装成 `anyhow!("Failed to create Rayon thread pool: ...")`，所以上层拿到的是运行时统一错误风格，而非 Rayon 原始错误类型。

---

### 3.2 `ComputePool` — 计算线程池

**来源**：`src/compute/pool.rs`

```rust
#[derive(Clone)]
pub struct ComputePool {
    pool:    Arc<rayon::ThreadPool>,
    metrics: Arc<ComputeMetrics>,
    config:  ComputeConfig,
}
```

**实现的 trait**：


| Trait   | 来源     | 说明                              |
| ------- | ------ | ------------------------------- |
| `Clone` | derive | Arc 包装，轻量                       |
| `Debug` | 手写     | 打印 num_threads, metrics, config |


**自身方法**：

```rust
impl ComputePool {
    pub fn new(config: ComputeConfig) -> Result<Self>
    // config.build_pool() → Arc<rayon::ThreadPool>，ComputeMetrics::new()

    pub fn with_defaults() -> Result<Self>
    // ComputeConfig::default() → Self::new()

    pub async fn execute<F, R>(&self, f: F) -> Result<R>
    where F: FnOnce() -> R + Send + 'static, R: Send + 'static
    // 核心异步桥接：
    // 1. metrics.record_task_start()
    // 2. tokio_rayon::spawn(move || pool.install(f)).await
    // 3. metrics.record_task_completion(elapsed)
    // 4. Ok(result)

    pub async fn install<F, R>(&self, f: F) -> Result<R>
    where F: FnOnce() -> R + Send + 'static, R: Send + 'static
    // 与 execute 实现相同：tokio_rayon::spawn(move || pool.install(f)).await
    // 语义区别：install 用于 par_iter 等需要 Rayon 线程池上下文的场景
    // 也记录 metrics

    pub fn execute_sync<F, R>(&self, f: F) -> R
    where F: FnOnce() -> R + Send, R: Send
    // 同步版本：直接 pool.install(f)，无 async 开销
    // 适用场景：从 spawn_blocking 或其他同步上下文调用

    pub async fn execute_scoped<F, R>(&self, f: F) -> Result<R>
    where F: FnOnce(&rayon::Scope) -> R + Send + 'static, R: Send + 'static
    // scope-based 并行：
    // tokio_rayon::spawn(move || pool.install(|| { rayon::scope(|s| f(s)) })).await
    // 记录 metrics

    pub async fn execute_scoped_fifo<F, R>(&self, f: F) -> Result<R>
    where F: FnOnce(&rayon::ScopeFifo) -> R + Send + 'static, R: Send + 'static
    // 同 execute_scoped，但使用 rayon::scope_fifo（FIFO 调度而非默认 LIFO）

    pub async fn join<F1, F2, R1, R2>(&self, f1: F1, f2: F2) -> Result<(R1, R2)>
    where ...
    // self.execute(move || rayon::join(f1, f2)).await

    pub fn metrics(&self) -> &ComputeMetrics
    // 返回 &ComputeMetrics（非 Arc）

    pub fn num_threads(&self) -> usize
    // pool.current_num_threads()
}
```

---

### 3.3 Tokio-Rayon 异步桥接机制

```
Tokio worker thread
  ↓ pool.execute(f)               ← async fn，在 Tokio 上下文调用
  ↓ tokio_rayon::spawn(move || {  ← 内部使用 oneshot channel 桥接
        pool.install(f)            ← Rayon 调度到某个 compute 线程
    })
  ↓ .await ────────────────────── Tokio 线程挂起，等待 Rayon 完成
                                              ↓
                             compute-0 线程执行 f()
                             完成后通过 oneshot 发送结果
  ↓ 收到结果，Tokio 线程恢复
  ↓ 返回 Ok(result)
```

关键点：`.await` 挂起 Tokio 线程（不阻塞），Tokio 可以调度其他 task；Rayon 线程纯 CPU 计算，不参与 Tokio 事件循环。`execute` 和 `install` 都经过 metrics 记录。

---

### 3.4 `ComputeHandle<T>` — 计算任务 Future 包装

**来源**：`src/compute/pool.rs`

```rust
pub struct ComputeHandle<T> {
    inner: Pin<Box<dyn Future<Output = T> + Send>>,
}
```

**设计意图**：将一个异步计算任务包装为可 await 的 Future，隐藏内部实现细节。

**自身方法**：

```rust
impl<T> ComputeHandle<T> {
    pub(crate) fn new<F>(future: F) -> Self
    where F: Future<Output = T> + Send + 'static
    // 将 future Box::pin 包装
}
```

**实现的 trait**：


| Trait                | 说明                                        |
| -------------------- | ----------------------------------------- |
| `Future<Output = T>` | 手写 impl：委托 `self.inner.as_mut().poll(cx)` |


---

### 3.5 `ComputePoolExt` trait — 扩展并行模式

**来源**：`src/compute/pool.rs`

```rust
#[async_trait]
pub trait ComputePoolExt {
    async fn parallel_batch<T, F, R>(
        &self, items: Vec<T>, batch_size: usize, f: F
    ) -> Result<Vec<R>>
    where T: Send + Sync + 'static,
          F: Fn(&[T]) -> Vec<R> + Send + Sync + 'static,
          R: Send + 'static;
    // pool.install(move || items.par_chunks(batch_size).flat_map(f).collect()).await

    async fn parallel_map<T, F, R>(&self, items: Vec<T>, f: F) -> Result<Vec<R>>
    where T: Send + Sync + 'static,
          F: Fn(T) -> R + Send + Sync + 'static,
          R: Send + 'static;
    // pool.install(move || items.into_par_iter().map(f).collect()).await
}

#[async_trait]
impl ComputePoolExt for ComputePool {
    async fn parallel_batch<T, F, R>(...) -> Result<Vec<R>>
    where T: Send + Sync + 'static,
          F: Fn(&[T]) -> Vec<R> + Send + Sync + 'static,
          R: Send + 'static;
    // use rayon::prelude::*;
    // self.install(move || items.par_chunks(batch_size).flat_map(f).collect()).await

    async fn parallel_map<T, F, R>(...) -> Result<Vec<R>>
    where T: Send + Sync + 'static,
          F: Fn(T) -> R + Send + Sync + 'static,
          R: Send + 'static;
    // use rayon::prelude::*;
    // self.install(move || items.into_par_iter().map(f).collect()).await
}
```

这里要特别注意：源码里不是“对所有满足条件类型的 blanket impl”，而是**只为 `ComputePool` 提供了一个具体实现**。

`parallel_batch()` 的具体实现路径是：

- 函数体内部先 `use rayon::prelude::*`；
- 对 `items` 使用 `par_chunks(batch_size)` 做并行分块；
- 对每个分块调用 `f(&[T]) -> Vec<R>`；
- 再通过 `flat_map(f)` 把每个批次返回的 `Vec<R>` 摊平并收集；
- 整个闭包最终通过 `self.install(...)` 放到当前 `ComputePool` 对应的 Rayon 池里执行。

`parallel_map()` 的具体实现路径则更直接：

- 函数体内部 `use rayon::prelude::*`；
- 对 `items` 调用 `into_par_iter()`；
- 对每个元素执行 `map(f)`；
- 最终 `collect()` 成 `Vec<R>`；
- 同样通过 `self.install(...)` 保证并行迭代发生在当前池上。

这也解释了为什么这两个扩展方法都要求 `T: Send + Sync`：

- `parallel_batch()` 中分块切片 `&[T]` 会在线程间并行读取，因此 `T` 需要 `Sync`；
- `parallel_map()` 虽然表面上按值消费 `T`，但 Rayon 的并行迭代实现仍要求元素类型满足跨线程安全共享/调度语义，因此源码保持 `Send + Sync` 约束。

---

### 3.6 `ScopeExecutor` trait

**来源**：`src/compute/mod.rs`

```rust
pub trait ScopeExecutor {
    fn execute_in_scope<F, R>(&self, f: F) -> R
    where F: FnOnce(&rayon::Scope) -> R + Send, R: Send;
}
```

目前仅定义 trait，无 blanket impl，也没有在本模块内直接提供默认实现。它更像一个**能力边界接口**：只要某个执行器能够提供 Rayon `Scope` 语义，就可以选择实现它。

这意味着 `ScopeExecutor` 的价值不在当前实现数量，而在于它把“支持 scope 并行执行”的能力单独抽象出来，避免外部调用方直接耦合 `ComputePool` 具体类型。

---

### 3.7 `patterns` 子模块 — 常用并行模式

**来源**：`src/compute/mod.rs`

```rust
pub mod patterns {
    pub async fn parallel_join<F1, F2, R1, R2>(
        pool: &ComputePool, f1: F1, f2: F2
    ) -> Result<(R1, R2)>
    where F1: FnOnce() -> R1 + Send + 'static,
          F2: FnOnce() -> R2 + Send + 'static,
          R1: Send + 'static,
          R2: Send + 'static;
    // pool.execute(move || rayon::join(f1, f2)).await

    pub async fn parallel_map<F, T, R>(
        pool: &ComputePool, items: Vec<T>, f: F
    ) -> Result<Vec<R>>
    where F: Fn(T) -> R + Sync + Send + 'static,
          T: Send + 'static,
          R: Send + 'static;
    // pool.execute(move || items.into_par_iter().map(f).collect()).await
}
```

`patterns` 内部通过 `use super::*;` 直接复用 `Result`、`ComputePool` 等上层导出；其中 `parallel_map()` 还会在函数体内部局部引入 `rayon::prelude::*`，以启用 `into_par_iter()`。

与 `ComputePoolExt` 的区别：`patterns` 是独立函数，接收 `&ComputePool` 参数；`ComputePoolExt` 是 trait 方法，可直接 `pool.parallel_map(...)` 调用。

两者的语义差异也要注意：

- `patterns::parallel_join()` / `parallel_map()` 适合作为轻量 helper，被外部直接按函数调用；
- `ComputePoolExt` 更像“把常见模式挂到池对象上”的面向对象封装；
- `patterns::parallel_map()` 对 `T` 的约束是 `Send`，而 `ComputePoolExt::parallel_map()` 对 `T` 的约束是 `Send + Sync`，文档里应保留这种源码层面的真实差异，而不是把两者写成完全相同。

由于 `patterns` 是内联定义在 `mod.rs` 中的子模块，因此它本质上属于 `compute` 对外 API 的一部分，而不是额外文件。

---

### 3.8 `ComputeMetrics` — 计算池指标

**来源**：`src/compute/metrics.rs`

```rust
#[derive(Debug)]
pub struct ComputeMetrics {
    tasks_total:           AtomicU64,    // 累计完成的任务数
    tasks_active:          AtomicUsize,  // 当前正在运行的任务数
    total_compute_time_us: AtomicU64,    // 累计计算时间（微秒）
    max_task_duration_us:  AtomicU64,    // 单次任务最大耗时（微秒）
    slow_tasks:            AtomicU64,    // 耗时 > 100ms 的任务数
}
```

**实现的 trait**：


| Trait     | 来源     | 说明               |
| --------- | ------ | ---------------- |
| `Debug`   | derive | 标准               |
| `Default` | 手写     | 委托 `Self::new()` |
| `Display` | 手写     | 输出人类可读的指标摘要字符串   |


**自身方法**：

```rust
impl ComputeMetrics {
    pub fn new() -> Self                      // 全部原子量初始化为 0

    pub fn record_task_start(&self)           // tasks_active += 1
    pub fn record_task_completion(&self, duration: Duration)
    // tasks_active -= 1
    // tasks_total += 1
    // total_compute_time_us += duration_us（saturating 转换防溢出）
    // max_task_duration_us = max(current, duration_us)  ← CAS 循环更新
    // slow_tasks += 1  （当 duration > 100ms）

    pub fn tasks_total(&self) -> u64          // 读 tasks_total
    pub fn tasks_active(&self) -> usize       // 读 tasks_active
    pub fn avg_task_duration_us(&self) -> f64 // total_compute_time_us / tasks_total
    pub fn max_task_duration_us(&self) -> u64 // 读 max_task_duration_us
    pub fn slow_tasks(&self) -> u64           // 读 slow_tasks
    pub fn reset(&self)                       // 全部原子量重置为 0
}
```

**RAII 设计**：`record_task_start()` 在 `ComputePool` 的 `execute` / `install` / `execute_scoped` / `execute_scoped_fifo` 方法开头调用，`record_task_completion()` 在方法返回前调用。不使用 Guard 模式，而是直接在方法体内配对调用。

---

### 3.9 `thread_local` 模块 — Tokio 线程本地存储

**来源**：`src/compute/thread_local.rs`

```rust
thread_local! {
    static COMPUTE_CONTEXT: RefCell<Option<ComputeContext>> = const { RefCell::new(None) };
}

#[derive(Clone)]
pub struct ComputeContext {
    pub pool:                  Arc<ComputePool>,
    pub block_in_place_permits: Arc<Semaphore>,
}
```

**实现的 trait**：

| Trait   | 来源     | 说明                     |
| ------- | -------- | ------------------------ |
| `Clone` | derive   | 共享 `Arc` 句柄，复制开销低 |


`COMPUTE_CONTEXT` 是本模块的关键私有实体：它不是公开 API，但所有 `compute_medium!` / `compute_large!` 宏的 thread-local 路径都依赖它提供 `ComputePool` 与 `Semaphore` 上下文。

**公开函数**：

```rust
pub fn initialize_context(pool: Arc<ComputePool>, permits: Arc<Semaphore>)
// 在当前线程设置 COMPUTE_CONTEXT
// 由本地运行时 `crate::runtime::Runtime::initialize_thread_local()` 调用

pub fn with_context<F, R>(f: F) -> Option<R>
where F: FnOnce(&ComputeContext) -> R
// 安全访问当前线程的 ComputeContext，若未初始化返回 None

pub fn try_acquire_block_permit() -> Result<OwnedSemaphorePermit, &'static str>
// 尝试从 thread-local context 获取 block_in_place 许可
// 成功 → Ok(permit)；无 context 或无可用许可 → Err

pub fn get_pool() -> Option<Arc<ComputePool>>
// 获取当前线程的 ComputePool 引用

pub fn has_compute_context() -> bool
// 检查当前线程是否已初始化 compute context

pub fn assert_compute_context()
// 断言当前线程已初始化，否则 panic
```

本地运行时 `crate::runtime::Runtime::initialize_all_thread_locals()` 通过 Barrier + `spawn_blocking` 确保所有 Tokio worker 线程都完成此初始化。

---

### 3.10 `macros` 模块 — 分级计算宏

**来源**：`src/compute/macros.rs`

提供三个分级宏，按任务预期耗时选择不同执行策略：

#### `compute_small!` — 小型任务（< 100μs）

```rust
#[macro_export]
macro_rules! compute_small {
    ($expr:expr) => {{
        // 直接内联执行，零开销
        let result = $expr;
        // feature-gated：验证实际耗时 < 100μs
        result
    }};
}
```

#### `compute_medium!` — 中型任务（100μs - 1ms）

```rust
#[macro_export]
macro_rules! compute_medium {
    // 无参数版本（使用 thread-local context）
    ($expr:expr) => {{
        // 1. 尝试 try_acquire_block_permit() → block_in_place
        // 2. 失败则尝试 get_pool() → pool.execute()
        // 3. 都无则 inline 执行并 warn
    }};

    // 显式 pool 版本
    ($pool:expr, $expr:expr) => {{
        // 1. 尝试 try_acquire_block_permit() → block_in_place
        // 2. 失败则使用提供的 pool.execute()
    }};
}
```

#### `compute_large!` — 大型任务（> 1ms）

```rust
#[macro_export]
macro_rules! compute_large {
    // 无参数版本
    ($expr:expr) => {{
        // 1. 尝试 get_pool() → pool.execute()
        // 2. 无 pool 则 inline 执行并 warn
    }};

    // 显式 pool 版本
    ($pool:expr, $expr:expr) => {{
        // 直接 pool.execute()
    }};
}
```

**选择策略**：


| 宏                 | 预期耗时        | 执行策略                        | 开销                         |
| ----------------- | ----------- | --------------------------- | -------------------------- |
| `compute_small!`  | < 100μs     | 直接内联                        | 零                          |
| `compute_medium!` | 100μs - 1ms | `block_in_place`（优先）或 Rayon | ~几μs（semaphore acquire）    |
| `compute_large!`  | > 1ms       | Rayon 线程池                   | ~25μs（tokio-rayon channel） |


---

### 3.11 `validation` 模块 — 任务分类验证（feature-gated）

**来源**：`src/compute/validation.rs`

仅在 `compute-validation` feature 开启时编译。用于开发期间检测任务是否被正确分类。

```rust
pub const SMALL_THRESHOLD_US:  u64 = 100;   // 100μs
pub const MEDIUM_THRESHOLD_US: u64 = 1000;  // 1ms

static SMALL_MISCLASSIFIED:  AtomicU64;
static MEDIUM_MISCLASSIFIED: AtomicU64;
static LARGE_MISCLASSIFIED:  AtomicU64;

pub fn validate_small(elapsed: Duration)
// elapsed > 100μs → warn + SMALL_MISCLASSIFIED += 1

pub fn validate_medium(elapsed: Duration)
// elapsed < 100μs → warn "考虑用 compute_small!"
// elapsed > 1ms → warn "考虑用 compute_large!"

pub fn validate_large(elapsed: Duration)
// elapsed < 1ms → warn "考虑用 compute_medium! 或 compute_small!"

pub fn get_misclassification_metrics() -> (u64, u64, u64)
// 返回 (small_misclassified, medium_misclassified, large_misclassified)

pub fn reset_misclassification_metrics()
// 全部计数器归零
```

这里的三个 `*_MISCLASSIFIED` 计数器是**私有静态状态**，不直接暴露给调用方；外部只能通过 `get_misclassification_metrics()` 读取聚合结果，通过 `reset_misclassification_metrics()` 清零。

---

## 四、设计决策

### D-01：Rayon 独立线程池，而非 `spawn_blocking`

`spawn_blocking` 使用 Tokio 的 blocking 线程池（默认 512 线程），没有 work-stealing，也没有 fork-join 语义。Rayon 的 work-stealing 调度和 `par_iter` / `scope` 原语使 CPU 密集型工作的并行化更简单、更高效。

### D-02：线程数上限 clamp(cpu/2, 2, 16)

不使用全部 CPU 核心：保留一半给 Tokio worker（I/O 调度）和 OS 开销。上限 16 防止在高核机器（如 128 核 A100 节点）上创建过多线程，超过 Rayon 高效 work-stealing 的有效范围。下限 2 保证基本并行性。

### D-03：三级宏分级执行

- `compute_small!`：零开销，直接内联。适用于几十微秒的小计算，避免 channel/semaphore 开销。
- `compute_medium!`：优先尝试 `block_in_place`（通过 semaphore 控制并发数），失败则降级到 Rayon。适用于不值得跨线程调度的中等计算。
- `compute_large!`：始终卸载到 Rayon，~25μs 的 channel 开销对于 >1ms 的计算可忽略不计。

### D-04：thread-local ComputeContext 避免参数传递

每个 Tokio worker 线程有自己的 `COMPUTE_CONTEXT`，存储 pool 和 permits 引用。宏可直接 `get_pool()` / `try_acquire_block_permit()` 获取，不需要从函数签名层层传递。初始化通过 Barrier 保证所有线程在开始处理请求前就已就绪。

### D-05：ComputeMetrics 使用原子操作而非锁

所有指标字段使用 `Atomic*` 类型，`Relaxed` 顺序读写。`max_task_duration_us` 更新使用 CAS 循环（`compare_exchange_weak`），保证无锁且最终一致。在高频计算场景下，原子操作比 Mutex 的吞吐量高一个数量级。

### D-06：将关键上下文保留为私有实体

`COMPUTE_CONTEXT` 与 validation 模块里的误分类计数器都没有直接暴露为 public static，而是通过函数访问。这保证了：

- 调用方只能通过受控接口读写状态；
- 后续内部实现可以从 `thread_local!` 或原子变量切换到其他实现，而不破坏外部 API；
- 宏与运行时之间仍然可以共享必要上下文，但不会把底层状态模型泄漏到业务层。

---

## 五、模块依赖

```
compute 使用：
  rayon                    — ThreadPool / ThreadPoolBuilder / scope / scope_fifo / join
  tokio_rayon              — spawn（async-sync 桥接）
  tokio::sync::Semaphore   — block_in_place permits
  async_trait              — ComputePoolExt trait
  std::sync::atomic        — ComputeMetrics 无锁计数

compute 被使用：
    runtime.rs               — 本地运行时创建 ComputePool、初始化 thread-local context
    config.rs                — 通过 `RuntimeConfig` 配置 compute_threads / compute_stack_size

---

## 六、测试覆盖与验证点

`compute` 模块不仅有 API 定义，也在源码里附带了多组单元测试/异步测试；这些测试是完备性检查中不能遗漏的一部分。

### 6.1 `mod.rs` 测试

```rust
test_compute_config_default()
test_build_pool()
```

- 验证 `ComputeConfig::default()` 的默认字段值；
- 验证 `build_pool()` 能按指定线程数创建 Rayon 池。

### 6.2 `pool.rs` 测试

```rust
test_compute_pool_execute()
test_compute_pool_join()
test_compute_pool_execute_sync()
test_compute_pool_scoped()
```

- 验证 `execute()` 异步桥接结果正确；
- 验证 `join()` 的并行组合语义；
- 验证 `execute_sync()` 可在 `spawn_blocking` 中工作；
- 验证 `execute_scoped()` 的 scope 并行任务回收逻辑。

### 6.3 `metrics.rs` 测试

```rust
test_metrics_recording()
test_metrics_reset()
```

- 验证 `record_task_start()` / `record_task_completion()` 的计数更新；
- 验证 `slow_tasks` 与 `reset()` 行为。

### 6.4 `thread_local.rs` 测试

```rust
test_uninitialized_context()
test_assert_compute_context_panics()
```

- 验证未初始化线程上的 `get_pool()` / `try_acquire_block_permit()` 退化行为；
- 验证 `assert_compute_context()` 在缺失上下文时会 panic。

### 6.5 当前未覆盖项

以下实体当前没有在本模块内看到直接测试：

- `compute_small!` / `compute_medium!` / `compute_large!` 宏的各条分支；
- `validation.rs` 中的误分类计数聚合函数；
- `ComputePoolExt::parallel_batch()` / `parallel_map()`；
- `patterns::parallel_join()` / `patterns::parallel_map()`。

这不代表实现错误，但说明它们目前主要依赖间接使用路径或后续集成测试覆盖。做后续完备性建设时，可优先补这些分支。

---

## 七、完备性检查结论

对照 `src/compute/mod.rs`、`pool.rs`、`metrics.rs`、`thread_local.rs`、`macros.rs`、`validation.rs`，当前文档已覆盖该模块的主要公开实体：

- `ComputeConfig`
- `ComputePool`
- `ComputeHandle<T>`
- `ComputePoolExt`
- `ScopeExecutor`
- `patterns::{parallel_join, parallel_map}`
- `ComputeMetrics`
- `ComputeContext`
- `initialize_context` / `with_context` / `try_acquire_block_permit` / `get_pool` / `has_compute_context` / `assert_compute_context`
- `compute_small!` / `compute_medium!` / `compute_large!`
- `validate_small` / `validate_medium` / `validate_large`
- `get_misclassification_metrics` / `reset_misclassification_metrics`

本次补充的重点不是新增大量 API 说明，而是把先前未单列的**私有关键状态、测试覆盖、内联子模块归属**补齐，使文档与源码实体列表更一致。
    worker.rs                — 间接通过本地运行时使用（非 drt 路径）
  pipeline/*               — CPU 密集型数据处理
  所有业务 crate           — 通过宏 compute_small! / compute_medium! / compute_large! 使用
```

