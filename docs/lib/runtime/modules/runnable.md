# `runnable` 模块设计文档

**源码位置**：`lib/runtime/src/runnable.rs`（当前实现约 26 行）

---

## 一、设计背景

Pagoda 运行时中存在多种可执行的后台任务：ServiceGroup PortName 的请求处理循环、健康检查任务、后台监控任务、传输层监听任务等。这些任务从调用方视角看都需要同样的一组生命周期操作：查询是否结束、触发取消、获取取消令牌、等待退出结果。

若没有统一接口，调用方就必须了解具体实现类型（例如健康检查 handle、传输层 server handle 等），既无法编写通用的任务管理逻辑，也无法把异构后台任务放入统一容器中处理。`runnable` 模块的职责就是用最小抽象提供这个统一约束：定义 `ExecutionHandle` trait，并把 trait 签名依赖的常用异步运行时类型重导出到同一命名空间。

---

## 二、模块内容总览

当前 `runnable.rs` 很小，只包含两类内容：

1. `ExecutionHandle` trait：统一描述“可取消、可等待、可查询状态”的后台任务句柄；
2. 类型重导出：把 `Result`、`Error`、`JoinHandle`、`CancellationToken`、`async_trait` 暴露到同一入口，降低实现方的样板代码成本。

该模块本身不负责启动任务，也不持有具体任务状态；它只定义约定，让其他模块以一致方式暴露任务生命周期控制能力。

## 三、`ExecutionHandle` trait

```rust
#[async_trait]
pub trait ExecutionHandle {
    fn is_finished(&self) -> bool;
    fn is_cancelled(&self) -> bool;
    fn cancel(&self);
    fn cancellation_token(&self) -> CancellationToken;
    fn handle(self) -> JoinHandle<Result<()>>;
}
```

**`is_finished()`**：非阻塞查询任务是否已完成（无论成功、失败还是取消）。调用方可以轮询此方法，无需 `.await`。

**`is_cancelled()`**：区分"任务已完成"和"任务因取消而完成"。某些场景（如优雅关闭过程中的清理逻辑）需要知道任务是正常结束还是被外部取消，以决定是否需要记录警告或执行补偿操作。

**`cancel()`**：触发取消但不等待。调用方在发出取消信号后可以继续执行其他工作，之后再通过 `handle()` 等待完成。相比直接 `abort()` JoinHandle，`cancel()` 通过 `CancellationToken` 触发，任务有机会执行清理代码（关闭连接、刷新缓冲区等）。

**`cancellation_token()`**：暴露任务内部的 `CancellationToken`，允许外部代码将自己的等待逻辑绑定到任务的取消信号。例如，另一个任务可以在此 token 和自己的工作之间 `select!`，实现联动取消。

**`handle(self) -> JoinHandle<Result<()>>`**：消费 `ExecutionHandle`，返回底层 Tokio `JoinHandle`。设计为消费 `self` 而非返回引用，是因为 `JoinHandle` 本身只能被 await 一次——多次 await 同一 JoinHandle 会 panic。通过 `self` 消费语义，Rust 类型系统在编译期阻止重复 await。

**`#[async_trait]`**：尽管目前 trait 中没有 `async fn`，加上此宏是为了未来扩展（如 `async fn wait(&self)`）时不需要破坏性修改。


## 四、重导出的意义

```rust
pub use anyhow::{Error, Result};
pub use async_trait::async_trait;
pub use tokio::task::JoinHandle;
pub use tokio_util::sync::CancellationToken;
```

实现 `ExecutionHandle` 的模块需要这几个类型。若不重导出，每个实现方都要分别引入多个 crate，样板代码会分散在各个模块里。重导出后，实现方只需 `use pagoda_runtime::runnable::*` 即可获得构建 trait 实现所需的大部分基础类型，同时确保 trait 定义和其签名中出现的类型来自统一导出路径，减少因依赖版本差异导致的类型不匹配问题。

---

## 五、典型实现方与使用关系

`runnable` 模块本身不提供具体实现，但它服务于多个“后台任务句柄”类型。例如：

- 传输层 server 的执行句柄可以实现 `ExecutionHandle`，让上层统一停止监听任务；
- 关键后台任务的监控句柄可以复用这一抽象，把取消与等待语义暴露给运行时；
- 健康检查、观测、清理类任务也可以沿用相同接口，避免每个模块重复定义一套 `stop/join/is_done` API。

这种设计让 Pagoda 在 `drt（DistributedRuntime）` 内部可以把“任务生命周期管理”抽象成统一操作，而不需要为每种任务引入一套专有管理代码。

---

## 六、测试与验证关系

`runnable.rs` 当前本身没有独立测试；这是因为它主要承载 trait 定义和类型重导出，逻辑几乎都在具体实现方中。

因此，这个模块的正确性主要通过两类方式被间接验证：

- **编译期约束**：实现方必须满足 `ExecutionHandle` 的完整 trait 签名，否则无法通过编译；
- **实现方测试**：具体 handle 类型所在模块（如关键任务管理、传输层 server 生命周期管理）会在它们自己的测试里覆盖取消、退出、等待等行为。

阅读或修订本模块文档时，应把它视为“任务生命周期协议层”，而不是带业务逻辑的执行模块。
