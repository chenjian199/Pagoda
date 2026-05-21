# pagoda-runtime 对外接口文档

面向上层模块开发者（如 `pagoda-llm`、`kvbm`、router、mocker 等）的公共接口说明。

本文目标不是罗列全部源码，而是回答两个问题：

1. 上层模块**应该从哪里接入** `pagoda-runtime`；
2. 哪些 API 是**推荐直接依赖**的，哪些虽然是 `pub` 但更偏内部实现或临时兼容接口。

---

## 1. 总入口

`pagoda-runtime` 的 crate 门面定义在 [../src/lib.rs](../src/lib.rs)。

最常用的顶层导出如下：

- `Worker`：进程启动与生命周期管理，定义在 [../src/worker.rs](../src/worker.rs)
- `Runtime`：本地运行时上下文，定义在 [../src/runtime.rs](../src/runtime.rs)
- `DistributedRuntime`：分布式运行时上下文，定义在 [../src/distributed.rs](../src/distributed.rs)
- `RuntimeConfig`：运行时配置，定义在 [../src/config.rs](../src/config.rs)
- `MetricsRegistry`：指标注册表，定义在 [../src/metrics.rs](../src/metrics.rs)
- `SystemHealth` / `HealthCheckTarget`：健康状态相关类型，定义在 [../src/system_health.rs](../src/system_health.rs)
- `CancellationToken`：取消令牌，从 `tokio_util` 重新导出

如果你只是写业务模块，通常从下面这一组开始就够了：

```rust
use pagoda_runtime::{
    Worker,
    Runtime,
    DistributedRuntime,
    RuntimeConfig,
    MetricsRegistry,
    CancellationToken,
};
```

---

## 2. 推荐的对外使用层次

建议把 `pagoda-runtime` 的公共接口分为四层理解：

### 第一层：进程与运行时入口

- `Worker`
- `Runtime`
- `DistributedRuntime`

这一层负责：

- 创建 Tokio runtime
- 初始化 discovery / request plane / system health / metrics
- 承载整个进程生命周期

### 第二层：服务模型 API

定义在 [../src/servicegroup.rs](../src/servicegroup.rs)：

- `Namespace`
- `ServiceGroup`
- `PortName`
- `Client`
- `Instance`
- `TransportType`

这一层是上层业务模块最常直接使用的 API。

### 第三层：引擎抽象 API

定义在 [../src/engine.rs](../src/engine.rs)：

- `AsyncEngine`
- `AsyncEngineContext`
- `AsyncEngineContextProvider`
- `ResponseStream`
- 类型擦除接口 `AnyAsyncEngine` / `AsAnyAsyncEngine` / `DowncastAnyAsyncEngine`

这一层适合实现模型推理引擎、代理引擎、mock 引擎等。

### 第四层：辅助与横切能力

- `traits`：运行时访问 trait，定义在 [../src/traits.rs](../src/traits.rs)
- `protocols`：协议公共类型，定义在 [../src/protocols.rs](../src/protocols.rs)
- `metrics`：指标能力，定义在 [../src/metrics.rs](../src/metrics.rs)
- `logging`：日志与 trace 工具，定义在 [../src/logging.rs](../src/logging.rs)
- `system_health` / `health_check`：健康检查能力

---

## 3. 最常见的使用场景

### 3.1 启动一个 runtime

### 推荐入口

使用 `Worker` 启动进程。

参考实现： [../src/worker.rs](../src/worker.rs)

核心接口：

- `Worker::from_settings()`
- `Worker::from_config(config)`
- `Worker::execute(app_fn)`
- `Worker::execute_async(app_fn)`
- `Worker::runtime()`

典型模式：

```rust
use pagoda_runtime::{DistributedRuntime, Worker};

fn main() -> anyhow::Result<()> {
    let worker = Worker::from_settings()?;
    worker.execute(|runtime| async move {
        let drt = DistributedRuntime::from_settings(runtime).await?;
        let ns = drt.namespace("llm")?;
        let component = ns.service_group("backend")?;
        let portname = servicegroup.portname("generate");

        // 在这里继续注册服务或创建客户端
        let _ = endpoint;
        Ok(())
    })
}
```

### 什么时候直接用 `Runtime`

当你已经在现有 Tokio runtime 中运行时，可以直接使用：

- `Runtime::from_current()`
- `Runtime::from_handle(handle)`
- `Runtime::from_settings()`

定义见 [../src/runtime.rs](../src/runtime.rs)。

这适合：

- Python binding 嵌入式场景
- 单元测试或集成测试
- 上层模块已经自行管理 Tokio runtime 的场景

---

### 3.2 创建分布式上下文

推荐入口：`DistributedRuntime`

定义见 [../src/distributed.rs](../src/distributed.rs)

核心接口：

- `DistributedRuntime::new(runtime, config)`
- `DistributedRuntime::from_settings(runtime)`
- `DistributedRuntime::runtime()`
- `DistributedRuntime::namespace(name)`
- `DistributedRuntime::discovery()`
- `DistributedRuntime::request_plane_server().await`
- `DistributedRuntime::network_manager()`
- `DistributedRuntime::system_health()`
- `DistributedRuntime::engine_routes()`
- `DistributedRuntime::local_portname_registry()`
- `DistributedRuntime::shutdown()`

### 推荐理解方式

`DistributedRuntime` 是上层模块最重要的共享上下文：

- 业务命名模型从它开始：`namespace() -> component() -> endpoint()`
- discovery 和 request plane 都挂在它下面
- health / metrics / local registry 也集中在这里

### 配置相关类型

同文件还定义了：

- `DistributedConfig`
- `DiscoveryBackend`
- `RequestPlaneMode`

这三者是上层模块在测试、自定义部署、特殊后端接入时最常碰到的配置入口。

---

### 3.3 注册一个服务端 PortName

推荐入口链路：

- `DistributedRuntime::namespace(name)`
- `Namespace::component(name)`
- `ServiceGroup::endpoint(name)`
- `PortName::portname_builder()`
- `PortNameConfigBuilder::start().await`

定义见：

- [../src/servicegroup.rs](../src/servicegroup.rs)
- [../src/servicegroup/portname.rs](../src/servicegroup/portname.rs)

关键类型：

- `Namespace`
- `ServiceGroup`
- `PortName`
- `PortNameConfig`
- 由 `PortName::portname_builder()` 返回的 builder

该 builder 常用接口：

- `register_local_engine(...)`
- `start().await`

`start()` 内部会做这些事情：

1. 组装 transport 地址；
2. 注册 request plane server；
3. 注册 discovery；
4. 接入 graceful shutdown；
5. 可选注册 health check target。

### 适合谁使用

- `pagoda-llm` 中的模型后端 endpoint
- `kvbm` 中暴露网络服务的 worker
- mocker / benchmark 组件

---

### 3.4 调用远端 PortName

推荐入口：`PortName::client().await`

定义见 [../src/servicegroup.rs](../src/servicegroup.rs) 和 [../src/servicegroup/client.rs](../src/servicegroup/client.rs)

核心接口：

- `PortName::client().await`
- `Client::wait_for_instances().await`
- `Client::instances()`
- `Client::instance_ids()`
- `Client::report_instance_down(id)`
- `Client::update_free_instances(ids)`

### 建议使用方式

如果你只需要“根据 namespace/servicegroup/endpoint 找到目标并调用”，优先依赖：

- `Namespace`
- `ServiceGroup`
- `PortName`
- `Client`

而不是直接操作 discovery path、instance watch 或 request plane client。

这样可以避免把上层模块和底层路由、发现实现耦合起来。

---

### 3.5 实现一个引擎

推荐入口：`engine` 模块

定义见 [../src/engine.rs](../src/engine.rs)

核心接口：

- `Data`
- `AsyncEngine<Req, Resp, E>`
- `AsyncEngineContext`
- `AsyncEngineContextProvider`
- `AsyncEngineUnary`
- `AsyncEngineStream`
- `ResponseStream`

### 适合谁使用

- `pagoda-llm`：实现 LLM 推理引擎
- mock / benchmark 模块：实现假引擎或代理引擎
- 需要异构引擎集合管理的模块：使用类型擦除接口

### 类型擦除接口

当你需要在同一集合里存不同泛型参数的引擎时，用：

- `AnyAsyncEngine`
- `AsAnyAsyncEngine`
- `DowncastAnyAsyncEngine`

这对上层做“运行时按名字取引擎”非常有用。

---

### 3.6 使用协议公共类型

推荐入口：`protocols` 模块

定义见 [../src/protocols.rs](../src/protocols.rs)

最常用类型：

- `PortNameId`
- `ServiceGroup`
- `LeaseId`
- `annotated::Annotated`
- `maybe_error::MaybeError`

其中 `PortNameId` 是最值得上层直接依赖的协议类型，用于统一标识：

- namespace
- component
- endpoint

它支持从字符串解析、格式化和 URL 形式转换。

---

### 3.7 获取运行时上下文 trait

推荐入口：`prelude` 或 `traits`

定义见：

- [../src/prelude.rs](../src/prelude.rs)
- [../src/traits.rs](../src/traits.rs)

关键 trait：

- `RuntimeProvider`
- `DistributedRuntimeProvider`

### 使用建议

如果你写的是通用组件、辅助函数、中间层，不想把函数签名绑死在某个具体类型上，可以依赖这两个 trait：

```rust
use pagoda_runtime::prelude::*;

fn use_runtime<T: RuntimeProvider>(target: &T) {
    let _rt = target.rt();
}
```

这比直接要求 `&DistributedRuntime` 或 `&PortName` 更灵活。

---

## 4. 建议上层模块优先依赖的接口

对 `pagoda-llm`、`kvbm`、router 等上层模块，推荐优先依赖下面这些稳定入口：

### 进程与运行时

- `Worker`
- `Runtime`
- `RuntimeConfig`
- `DistributedRuntime`

### 服务模型

- `Namespace`
- `ServiceGroup`
- `PortName`
- `Client`
- `Instance`
- `TransportType`

### 引擎抽象

- `AsyncEngine`
- `AsyncEngineContext`
- `AsyncEngineContextProvider`
- `ResponseStream`

### 协议与 trait

- `PortNameId`
- `RuntimeProvider`
- `DistributedRuntimeProvider`

### 横切能力

- `MetricsRegistry`
- `SystemHealth`
- `HealthCheckTarget`
- `CancellationToken`

---

## 5. 虽然是 `pub`，但不建议上层直接强依赖的接口

下面这些接口目前虽然公开，但更偏实现细节、兼容逻辑或临时措施；如果不是明确知道自己在做什么，不建议上层模块直接耦合：

### `DistributedRuntime` 上的临时接口

定义见 [../src/distributed.rs](../src/distributed.rs)

- `component_registry()`
- `instance_sources()`
- `kv_router_nats_publish(...)`
- `kv_router_nats_request(...)`
- `register_nats_service(...)`

原因：

- 这些接口有的在注释里已明确标注为 `TODO` 或临时措施；
- 有些直接暴露内部 registry / cache / transport 细节；
- 后续 request plane 或 NATS 使用方式变化时，它们更容易调整。

### 低层 request plane / transport 实现

除非你在写 runtime 基础设施本身，否则尽量不要直接依赖：

- `pipeline::network::*` 下的具体 client/server 类型
- `transports::*` 下的具体后端实现
- `discovery::*` 下的某个具体 backend 类型实现

上层更推荐依赖：

- `PortName` / `Client`
- `DistributedRuntime::discovery()`
- `DistributedRuntime::request_plane_server()`
- `AsyncEngine`

---

## 6. 按角色给出的推荐用法

### 6.1 对 `pagoda-llm` 同事

优先依赖：

- `Worker`
- `DistributedRuntime`
- `Namespace` / `ServiceGroup` / `PortName`
- `AsyncEngine` / `ResponseStream`
- 通过 `PortName::portname_builder()` 获取的服务端注册 builder
- `MetricsRegistry`
- `SystemHealth`

典型动作：

- 启动 worker
- 创建 LLM backend component
- 注册 `generate` / `health` / `metadata` 等 endpoint
- 把模型推理逻辑包装成 `AsyncEngine`

### 6.2 对 `kvbm` / router 同事

优先依赖：

- `DistributedRuntime`
- `Client`
- `PortNameId`
- `Instance`
- `RequestPlaneMode`
- discovery 查询接口（通过 `DistributedRuntime::discovery()`）

典型动作：

- 找 worker 实例
- 跟踪实例上下线
- 按 endpoint 发请求
- 维护路由层健康/负载策略

### 6.3 对 mock / benchmark 同事

优先依赖：

- `Worker`
- `DistributedRuntime`
- `AsyncEngine`
- 通过 `PortName::portname_builder()` 获取的服务端注册 builder
- `Runtime::from_current()` / `DistributedConfig::process_local()`

典型动作：

- 在单进程内快速构建一个本地 runtime
- 注册 fake endpoint
- 复用同样的调用链测试路由和发现

---

## 7. 最小可用调用路径

如果上层模块只想快速知道“从哪里开始”，可以直接记住下面两条路径。

### 服务端注册路径

`Worker`
→ `DistributedRuntime`
→ `Namespace`
→ `ServiceGroup`
→ `PortName`
→ `PortNameConfigBuilder::start()`

### 客户端调用路径

`DistributedRuntime`
→ `Namespace`
→ `ServiceGroup`
→ `PortName`
→ `PortName::client().await`
→ `Client`

### 引擎实现路径

`AsyncEngine`
→ `AsyncEngineContext`
→ `ResponseStream`

---

## 8. 建议的 import 方式

### 常规业务模块

```rust
use pagoda_runtime::{DistributedRuntime, Worker};
use pagoda_runtime::component::{Client, PortName, Instance, Namespace};
use pagoda_runtime::engine::{AsyncEngine, AsyncEngineContext, ResponseStream};
use pagoda_runtime::protocols::PortNameId;
```

### 写通用工具函数

```rust
use pagoda_runtime::prelude::*;
```

`prelude` 当前主要重导出 [../src/traits.rs](../src/traits.rs) 中的 trait，适合写需要 `rt()` / `drt()` 访问能力的通用代码。

---

## 9. 文档边界说明

本文描述的是“建议上层直接使用的公共接口”，不是稳定性承诺文档，也不是全部 `pub` API 的逐项索引。

如果你要做的是：

- 新增一个业务组件；
- 新增一个 worker endpoint；
- 实现一个引擎；
- 写一个 router / kvbm / benchmark 客户端；

优先查本文列出的入口即可。

如果你要改的是 runtime 基础设施本身，再进一步看：

- [runtime-architecture.md](runtime-architecture.md)
- [pipeline-flow.md](pipeline-flow.md)
- [modules/](modules/)

---

## 10. 用 `cargo doc` / `rustdoc` 生成 API 文档

可以，而且如果你需要“更具体到函数、trait、方法签名级别”的接口说明，`cargo doc` 生成的 Rust API 文档比手写总览文档更合适。

### 适合解决什么问题

- 查看某个公开类型到底有哪些 `pub fn` / `pub async fn`
- 查看 trait 的完整方法签名和泛型约束
- 查看类型之间的跳转关系（点击模块、类型、方法）
- 基于源码注释生成更精确的 API 参考文档

### 不适合替代什么

`cargo doc` 生成的是“按源码项组织”的 API 文档，不会自动给出：

- 上层模块应该从哪里接入
- 哪些接口是推荐依赖、哪些是临时兼容接口
- 一个完整业务场景的调用顺序

所以比较推荐的方式是：

- 用本文档做“入口说明”和“使用建议”
- 用 `cargo doc` 做“具体 API 索引”

### 本仓库生成命令

在仓库根目录执行：

```bash
cargo doc -p pagoda-runtime --no-deps
```

如果只想看本 crate 的公开项，这个命令通常就够了。

生成后的入口页面通常在：

- `target/doc/pagoda_runtime/index.html`


先看本文知道从哪里接入，再点进 rustdoc 看具体方法签名，会比只看源码或只看总览文档都更高效。
