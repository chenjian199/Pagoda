# `worker` 模块设计文档

**源码位置**：`lib/runtime/src/worker.rs`（238 行）

---

## 一、设计背景

Pagoda 应用程序的 `main()` 函数需要完成以下工作才能安全地运行用户的推理服务：创建 Tokio 运行时、初始化 `Runtime`、注册 OS 信号处理器（SIGINT/SIGTERM）、运行用户应用、在应用退出时执行优雅关闭、超时后强制退出。

这些步骤如果让每个应用自己实现，既繁琐又容易出错（遗漏信号处理、优雅关闭超时不统一）。`Worker` 将这些样板代码封装起来，使用户的 `main()` 只需：

```rust
fn main() -> anyhow::Result<()> {
    Worker::from_settings()?.execute(|runtime| async move {
        // 用户应用代码
        my_service(runtime).await
    })
}
```

`Worker` 的注释说它未来可能演化为 `#[pagoda::main]` 过程宏，与 `#[tokio::main]` 的设计思路一致。

---

## 二、`Worker` 结构与初始化

### 2.1 结构体字段

```rust
#[derive(Debug, Clone)]
pub struct Worker {
    runtime: Runtime,
    config: RuntimeConfig,
}
```

**`runtime: Runtime`**

这是 `Worker` 真正持有的运行时抽象，里面封装了主/辅 Tokio 运行时句柄、取消令牌和关闭逻辑。`execute()`、`execute_async()`、`runtime()` 等方法最终都围绕这个字段展开，因此 `Worker` 本质上可以看作是“带有统一生命周期管理策略的 Runtime 包装器”。

**`config: RuntimeConfig`**

保存构造 `Worker` 时所使用的运行时配置。即使后续执行路径主要依赖 `runtime` 字段，这个配置对象仍然有价值，因为它把“当前实例是按什么设置创建或接管的”这件事显式保存在对象内部，而不是要求后续逻辑再回头重新读取环境变量。对 `from_current()` 这样的路径尤其如此：虽然运行时不是由 `Worker` 新建的，但实例依然保留了一份与当前进程设置一致的 `RuntimeConfig`。

### 2.2 构造路径

**`from_settings() -> Result<Worker>`**

这是最常规的入口：先用 `RuntimeConfig::from_settings()` 从环境变量构造配置，再统一委托给 `from_config(config)`。它的价值不在于增加一层薄包装，而在于把“读取环境”和“按配置初始化 Worker”这两步固定成默认启动路径，从而让上层 `main()` 保持极简。

**`from_config(config) -> Result<Worker>`**

这是真正负责初始化进程级 Worker 的核心构造函数。它先检查 `RT` 和 `RTHANDLE` 是否已经被填充，防止同一进程重复创建或重复接管运行时；随后调用 `config.create_runtime()?` 生成新的 Tokio 运行时，并通过 `RT.try_insert(...)` 放入全局 `OnceCell`。

这里有一个细节很重要：源码里同时保留了“先检查一次是否已初始化”和“最终以 `try_insert` 为准”两层保护。前者用于快速失败，后者用于兜住两个线程几乎同时通过前置检查的竞争场景。也就是说，真正保证全局唯一性的不是那个 `if RT.get().is_some()` 判断本身，而是 `OnceCell` 的单次插入语义。

在成功插入后，`from_config()` 不直接把 `tokio::runtime::Runtime` 暴露出去，而是基于其 handle 构造 `Runtime::from_handle(...)`，再与传入的 `config` 一起打包成 `Worker`。这说明 `Worker` 对外强调的不是底层 Tokio 运行时本体，而是 Pagoda Runtime 语义下的统一运行时抽象。

**`from_current() -> Result<Worker>`**

这条路径用于“当前线程或宿主框架已经有 Tokio 运行时，我只想把它包装成 `Worker`”的场景。它同样先拒绝任何已经发布过 `RT`/`RTHANDLE` 的进程状态，避免把“接管现有运行时”和“创建/发布全局运行时”两类模式混用；随后调用 `Runtime::from_current()?` 借用当前上下文，并读取一份 `RuntimeConfig::from_settings()?` 填入 `config` 字段。

与 `from_config()` 不同，`from_current()` 的职责是包装和接入，而不是向 `RT` 里注册一个新的进程级主运行时。因此它更适合嵌入式集成或宿主已经决定 Tokio 生命周期的场景。

### 2.3 全局唯一性保证

```rust
static RT:     OnceCell<tokio::runtime::Runtime>
static RTHANDLE: OnceCell<tokio::runtime::Handle>
static INIT:   OnceCell<Mutex<Option<JoinHandle<anyhow::Result<()>>>>>
```

**为什么用 `OnceCell` 全局存储运行时**：一个进程只应有一个"主" Tokio 运行时（`Worker` 创建的那个）。若允许创建多个 `Worker`，则有多个独立的 Tokio 运行时，各自管理自己的线程池，总线程数翻倍而没有收益，且信号处理会被注册多次。`OnceCell` 的 `try_insert` 在竞争时只有一个成功，其他调用返回 `Err`，`from_config` 将其转化为明确的错误：

```rust
if RT.get().is_some() || RTHANDLE.get().is_some() {
    return Err(anyhow::anyhow!("Worker already initialized"));
}
```

**`RT` 与 `RTHANDLE` 的分工**：`RT` 表示“完整的、由 Worker 管理生命周期的 Tokio 运行时实例”，而 `RTHANDLE` 表示“进程里已经存在一个可复用的 Tokio 运行时 handle，但不一定是通过 `Worker::from_config()` 这条路径创建出来的”。把这两个状态拆开保存，可以让代码区分“我拥有这个运行时本体”与“我至少知道这个进程里已经有一个运行时”这两种语义。

**`INIT` 的意义**：它保存真正应用任务的 `JoinHandle`，并额外套了一层 `Mutex<Option<...>>`。`OnceCell` 负责“这个句柄只能被发布一次”，`Option::take()` 则负责“这个句柄只能被消费一次”。因此 `Worker::execute()` / `execute_async()` 在设计上都不是可重入、可多次调用的控制接口，而是单次启动、单次等待的生命周期入口。

**`&'static tokio::runtime::Runtime`**：`OnceCell` 内的 `Runtime` 具有 `'static` 生命周期（与进程同寿）。`tokio_runtime()` 方法返回 `&'static` 引用，允许用户在不持有 `Worker` 的情况下访问运行时（如在 Python 绑定中）。

---

## 三、执行入口与 `execute_internal`

`Worker` 对外暴露两条执行入口：`execute()` 面向同步 `main()`，`execute_async()` 面向已经处于 async 上下文中的调用方。两者都会启动同一个 `execute_internal()` 流程，并在应用完成后调用 `runtime.shutdown()` 收尾；区别只在于等待方式不同：

- `execute()` 通过 `runtime.secondary().block_on(...)` 在同步上下文里阻塞等待；
- `execute_async()` 直接 `await` 内部任务，适合已经运行在 Tokio 环境中的场景。

这层拆分的关键价值是：用户代码不需要为了迁就 `Worker` 去改变 `main()` 形态，而 `Worker` 内部又仍然只有一套统一的执行、关闭和日志路径。

```rust
fn execute_internal<F, Fut>(self, f: F) -> JoinHandle<anyhow::Result<()>>
where F: FnOnce(Runtime) -> Fut + Send + 'static,
      Fut: Future<Output = anyhow::Result<()>> + Send + 'static
```

这是 `Worker` 最核心的方法，以下是其执行流程：

```
secondary.spawn(async move {
    tokio::spawn(signal_handler(primary_token))   ← 独立任务，监听信号
    
    let (mut tx, rx) = oneshot::channel::<()>();
    let task = primary.spawn(async move {
        let _rx = rx;                              ← rx 存活期间 tx.closed() 不触发
        f(runtime).await                           ← 用户应用在 primary 上运行
    });
    
    select! {
        _ = cancel_token.cancelled() => { /* 收到关闭信号 */ }
        _ = tx.closed() => { /* 应用自行退出 */ }
    }
    
    select! {
        result = task => { return result?; }
        _ = sleep(timeout) => { exit(911); }      ← 超时强制退出
    }
})
```

**为什么 `execute_internal` 运行在 `secondary` 上**：编排逻辑（等待信号、等待应用退出、关闭协调）是轻量的异步操作，不应占用 `primary` 的 worker 线程。更重要的是，若 `primary` 线程因应用负载全部繁忙，运行在 `secondary` 上的编排逻辑可以继续响应信号，启动关闭流程，而不会被饿死。

**应用运行在 `primary` 上**：`primary.spawn(f(runtime))` 将用户应用提交到 `primary` 线程池。推理引擎、HTTP 服务等计算密集型工作需要充分利用 `primary` 的多线程，不能被 `secondary` 的单线程限制。

**`oneshot::channel` 检测应用退出**：`_rx` 持有 oneshot 接收端，存活于 primary spawn 的 async 块中。当 primary 任务完成时（无论成功还是失败），该 async 块 drop，`_rx` drop，`tx.closed()` 立即 ready。这是一种无需额外同步原语的"任务完成检测"方式——比 `Arc<AtomicBool>` 更优雅，比 `task.is_finished()` 轮询更高效。

**`INIT` 只允许一次真正执行**：`execute_internal()` 会把 `secondary.spawn(...)` 得到的应用任务句柄放进 `INIT`，随后立刻通过 `.lock().take()` 取出并返回。这意味着第一个调用者会独占这次执行机会，而后续若还有其他线程试图再次 `execute()` / `execute_async()`，就会在 `expect("Worker.execute() can only be called once")` 这一层被视为编程错误。对 `Worker` 来说，这种“单次启动器”语义是刻意设计，而不是实现细节。

---

## 四、信号处理

```rust
async fn signal_handler(cancel_token: CancellationToken) -> anyhow::Result<()> {
    tokio::select! {
        _ = ctrl_c   => { /* Ctrl+C */ }
        _ = sigterm  => { /* SIGTERM（Kubernetes 发送此信号终止 Pod） */ }
        _ = cancel_token.cancelled() => { /* 程序内部触发关闭 */ }
    }
    cancel_token.cancel();
    Ok(())
}
```

`signal_handler` 独立运行在 `secondary` 上（通过 `tokio::spawn`）。三路 `select!` 确保无论哪种关闭触发源（用户 Ctrl+C、Kubernetes SIGTERM、程序内部取消），都统一调用 `cancel_token.cancel()`，触发 `execute_internal` 中的关闭流程。函数签名返回 `anyhow::Result<()>` 而非 `()`，是为了允许 `ctrl_c.await?` 和 `signal().recv()` 的 `?` 传播，错误会被 JoinHandle 捕获并记录。

---

## 五、关闭超时与 `exit(911)`

```rust
select! {
    result = task => { result }
    _ = sleep(Duration::from_secs(timeout)) => {
        tracing::debug!("Application did not shutdown in time; terminating");
        std::process::exit(911);
    }
}
```

**为什么是 `exit(911)` 而非 panic**：`panic!` 会触发展开（unwinding），在某些情况下可能因 drop 顺序问题导致二次 panic 或死锁，反而看不到最后的日志。`exit(911)` 直接终止进程，退出码 911 在运维监控中易于识别（grep `exit code 911` 即可找到超时关闭的记录）。

**超时值的调试/发布差异**：
- Debug 构建：5 秒（`DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_DEBUG`）——开发迭代时不需要长时间等待
- Release 构建：30 秒（`DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_RELEASE`）——生产场景的推理请求可能需要较长时间完成

`PGD_WORKER_GRACEFUL_SHUTDOWN_TIMEOUT` 环境变量可以覆盖默认值，适用于需要更长关闭窗口的大型模型场景。

---

## 六、静态访问与运行时查询

```rust
pub fn runtime_from_existing() -> anyhow::Result<Runtime> {
    if let Some(rt) = RT.get() {
        Ok(Runtime::from_handle(rt.handle().clone())?)
    } else if let Some(rt) = RTHANDLE.get() {
        Ok(Runtime::from_handle(rt.clone())?)
    } else {
        let runtime = Runtime::from_settings()?;
        let _ = RTHANDLE.set(runtime.primary());
        Ok(runtime)
    }
}
```

此静态方法允许代码在不持有 `Worker` 实例的情况下获取 `Runtime`。这主要用于 Python 绑定（PyO3 函数无法持有 `Worker`，但需要访问 `Runtime`）和某些全局初始化代码。它的查找顺序是：优先复用 `RT` 中完整的运行时，再退回到 `RTHANDLE` 中已经发布的 handle；只有两者都为空时，才走一次 `Runtime::from_settings()` 的兜底构造。

这里相比旧实现多了一个关键动作：在兜底新建 `Runtime` 之后，会把 `runtime.primary()` 回填到 `RTHANDLE`。这样后续 `has_existing_runtime()`、外部后端绑定代码或其他静态访问者，就能正确观察到“这个进程里现在已经存在一个运行时”这件事，而不会误以为自己仍然拥有创建首个 Worker 的资格，从而意外构造第二个运行时。

**`has_existing_runtime() -> bool`**

这是一个纯查询接口，只检查 `RT` 或 `RTHANDLE` 是否已被填充，不会像 `runtime_from_existing()` 那样触发兜底创建。它适合用作无副作用的守卫条件，例如外部绑定先探测当前进程是否已经拥有 Runtime，再决定是复用还是报错。

**`tokio_runtime() -> Result<&'static tokio::runtime::Runtime>`** / **`runtime() -> &Runtime`**

这两个访问器分别服务于不同层级的调用方。`runtime()` 返回 `Worker` 内部持有的 Pagoda `Runtime` 抽象，是常规 Rust 代码最应该使用的入口；`tokio_runtime()` 则尝试直接取出 `RT` 里的全局 Tokio 运行时引用，因此只有在运行时确实通过 `Worker::from_config()` 这类会填充 `RT` 的路径创建时才会成功。换句话说，前者面向正常业务调用，后者面向需要直接接触底层 Tokio runtime 的低层集成代码。
