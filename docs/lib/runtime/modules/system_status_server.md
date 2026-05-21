# `system_status_server` 模块设计文档

**源码位置**：`lib/runtime/src/system_status_server.rs`（单文件，约 800 行）

---

## 一、设计背景与模块职责

`system_status_server` 模块为 Pagoda 节点提供一个轻量级的内建 HTTP 控制面入口，统一承载以下几类系统级接口：

- 健康检查：`/health`、`/live`
- 指标导出：`/metrics`
- 发现元数据查看：`/metadata`
- 引擎自定义控制路由：`/engine/*`
- 可选的 LoRA 管理接口：`/v1/loras`

如果没有这样一个统一入口，运维与调试会迅速失控：

- health、metrics、metadata 可能分散在不同端口；
- 引擎自定义控制面只能各自再启动一套 HTTP server；
- LoRA 管理若走业务请求平面，会和普通推理流量混在一起；
- 运行时无法通过单个地址完成 readiness / liveness / observability 暴露。

因此，这个模块的职责不是“做业务推理”，而是做 **系统 HTTP 门面（system-facing HTTP facade）**：

- 将分布式运行时的内部状态翻译成标准 HTTP 接口；
- 为引擎与运维功能预留统一挂载点；
- 把控制面流量与普通推理流量分离。

它是一个典型的“窄控制面 server”，本身几乎不保存业务状态，核心逻辑都围绕 `DistributedRuntime` 展开。

---

## 二、整体结构：状态极薄，路由集中

该模块的结构可以概括为三层：

1. **状态承载层**：`SystemStatusServerInfo`、`SystemStatusState`
2. **启动装配层**：`spawn_system_status_server()`
3. **请求处理层**：`health_handler`、`metrics_handler`、`metadata_handler`、`engine_route_handler`、LoRA handlers

这种划分背后的原则是：

- 启动逻辑只负责装配 router 与监听 socket；
- handler 只负责将 HTTP 请求翻译为对 runtime 的读取或调用；
- 真正的系统状态统一存放在 `DistributedRuntime` / `SystemHealth` / `DiscoveryMetadata` 中。

这样可以避免 HTTP 模块本身变成第二个控制平面状态中心。

---

## 三、`SystemStatusServerInfo` —— 服务器句柄与地址信息

```rust
pub struct SystemStatusServerInfo {
    pub socket_addr: std::net::SocketAddr,
    pub handle: Option<Arc<JoinHandle<()>>>,
}
```

它的作用不是“控制服务器行为”，而是为外部提供一个可查询、可测试、可观测的 server 句柄。

### 为什么要显式保存 `socket_addr`

系统状态服务器经常绑定到 `0` 端口，让操作系统分配随机可用端口，特别适合测试场景。因此，调用方在启动后必须拿到“实际绑定到哪一个端口”。

把 `socket_addr` 单独保存下来，可以让：

- 测试代码直接拼接 URL 发请求；
- 运行时在日志里输出真实监听地址；
- 上层通过 `hostname()` / `port()` 进一步拆分使用。

### 为什么 `handle` 是 `Option<Arc<JoinHandle<()>>>`

`JoinHandle` 不可克隆，而 `SystemStatusServerInfo` 需要实现 `Clone`，因此必须包一层 `Arc`。同时它又是 `Option`，因为并不是所有场景都需要或能够暴露后台任务句柄；某些路径只需要地址信息即可。

这是一种常见的“元数据一定有、控制句柄可选”的设计。

### `SystemStatusServerInfo::new()`

```rust
pub fn new(socket_addr: std::net::SocketAddr, handle: Option<JoinHandle<()>>) -> Self
```

这是该结构体的标准构造函数。它做的事情很少，但有一个实现细节值得写清楚：传入的 `Option<JoinHandle<()>>` 会被转换成 `Option<Arc<JoinHandle<()>>>`。

也就是：

```rust
Self {
    socket_addr,
    handle: handle.map(Arc::new),
}
```

这样设计的目的不是为了共享可变状态，而是为了让 `SystemStatusServerInfo` 后续可以安全实现 `Clone`。如果直接把裸 `JoinHandle<()>` 存在结构体里，那么这个信息对象就无法被轻量复制给测试、运行时状态对象或其他调用方。

### `SystemStatusServerInfo::address()` / `hostname()` / `port()`

```rust
pub fn address(&self) -> String
pub fn hostname(&self) -> String
pub fn port(&self) -> u16
```

这三个函数都是围绕 `socket_addr` 的便捷访问器：

- `address()` 返回完整的 `ip:port` 字符串；
- `hostname()` 返回 IP 部分；
- `port()` 返回端口号。

虽然它们都很简单，但保留这些辅助函数仍然有意义，因为上层调用方不必每次都手动拆解 `SocketAddr`。尤其在测试代码里，`address()` 和 `port()` 能直接减少 URL 拼接或日志输出时的样板代码。

### 为什么手写 `Clone`

源码没有 `#[derive(Clone)]`，而是手写：

```rust
impl Clone for SystemStatusServerInfo {
    fn clone(&self) -> Self {
        Self {
            socket_addr: self.socket_addr,
            handle: self.handle.clone(),
        }
    }
}
```

这本质上依赖了前面 `handle: Option<Arc<JoinHandle<()>>>` 的设计。如果没有 `Arc`，这里就无法安全复制后台任务句柄的引用。

---

## 四、`SystemStatusState` —— handler 的共享只读上下文

```rust
pub struct SystemStatusState {
    root_drt: Arc<crate::DistributedRuntime>,
    discovery_metadata: Option<Arc<tokio::sync::RwLock<crate::discovery::DiscoveryMetadata>>>,
}
```

### 为什么状态对象只保留这两个字段

因为所有 handler 本质上只需要两类数据源：

- `DistributedRuntime`：健康状态、指标、引擎路由、本地 portname registry 等；
- `DiscoveryMetadata`：Kubernetes 等后端的发现元数据快照。

没有必要在 HTTP 层复制更多状态。HTTP 层越薄，越不容易形成“状态在 runtime 和 server 各存一份”的双写问题。

### 为什么 `discovery_metadata` 是 `Option`

不是所有部署模式都有 discovery metadata：

- 本地模式下可能不需要；
- 某些后端不提供 metadata；
- 测试场景可能只验证 health/metrics。

因此这里不强制要求 metadata 一定存在，而是在 `/metadata` handler 中按需判断，缺失时返回 `404`。

这比在启动阶段就因为 metadata 缺失而拒绝启动更合理，因为 metadata 只是附加观测接口，不是 server 存活的必要前提。

### `SystemStatusState::new()`

```rust
pub fn new(
    drt: Arc<crate::DistributedRuntime>,
    discovery_metadata: Option<Arc<tokio::sync::RwLock<crate::discovery::DiscoveryMetadata>>>,
) -> anyhow::Result<Self>
```

这是 `SystemStatusState` 的构造函数。它当前的实现非常薄：只是把传入参数原样包进结构体并返回 `Ok(Self { ... })`。

```rust
Ok(Self {
    root_drt: drt,
    discovery_metadata,
})
```

这里返回 `anyhow::Result<Self>` 而不是直接返回 `Self`，主要体现的是一种接口留白：当前构造过程没有失败路径，但未来如果需要在状态创建时增加校验、预处理或资源初始化，就不必改动函数签名和调用方结构。

### `SystemStatusState::drt()`

```rust
pub fn drt(&self) -> &crate::DistributedRuntime
```

这个函数返回对底层 `DistributedRuntime` 的只读引用：

```rust
&self.root_drt
```

看起来只是一个简单 getter，但它是整个模块里最重要的状态访问入口之一。几乎所有 handler 都通过它拿到运行时能力：

- `health_handler()` 通过它访问 `system_health()`；
- `metrics_handler()` 通过它访问 `metrics()`；
- `engine_route_handler()` 通过它访问 `engine_routes()`；
- LoRA handlers 通过它访问 `local_portname_registry()`。

它返回的是 `&DistributedRuntime` 而不是 `Arc<DistributedRuntime>`，这也很合理：`SystemStatusState` 自己已经持有 `Arc`，对外只暴露借用即可，避免无意义地增加引用计数操作。

### `SystemStatusState::discovery_metadata()`

```rust
pub fn discovery_metadata(
    &self,
) -> Option<&Arc<tokio::sync::RwLock<crate::discovery::DiscoveryMetadata>>>
```

这个访问器返回的是一个可选借用，而不是克隆后的 `Arc`：

```rust
self.discovery_metadata.as_ref()
```

这么设计有两个原因：

1. 它明确表达 metadata 本身就是可选资源，调用方必须显式处理 `None`；
2. 它只暴露引用，不在 getter 层额外制造 `Arc` clone，把“是否需要持有一份所有权”这个决定留给真正的调用方。

`metadata_handler()` 就是典型使用点：先判断 `Option`，有值再进入 `read().await`，没有值直接返回 `404`。

---

## 五、启动入口：`spawn_system_status_server()`

```rust
pub async fn spawn_system_status_server(
    host: &str,
    port: u16,
    cancel_token: CancellationToken,
    drt: Arc<crate::DistributedRuntime>,
    discovery_metadata: Option<Arc<tokio::sync::RwLock<crate::discovery::DiscoveryMetadata>>>,
) -> anyhow::Result<(SocketAddr, JoinHandle<()>)>
```

这是整个模块的装配中心。

### 启动流程

**1. 构造共享状态**

先把 `drt` 与 `discovery_metadata` 包成 `Arc<SystemStatusState>`，供后续每条路由共享。

**2. 从 `SystemHealth` 动态读取 health/live 路径**

health 与 live 路径不是写死在代码里，而是由 `SystemHealth` 提供。这保证路径配置与健康状态来源保持一致。

**3. 根据 `PGD_LORA_ENABLED` 决定是否挂载 LoRA 路由**

LoRA 管理是可选能力，不应让所有运行时都暴露这组接口。通过环境变量在启动时做一次开关判断，可以让 Router 保持静态结构，避免请求时再做条件分支。

**4. 组装 Axum Router**

挂载固定系统路由、可选 LoRA 路由、fallback 与 tracing layer。

**5. 绑定 socket 并获取实际地址**

绑定 `host:port`，如果 `port = 0` 则让 OS 分配；随后通过 `listener.local_addr()` 拿到实际地址。

**6. 用 `cancel_token` 驱动优雅退出**

将 server 放进后台 Tokio 任务，并用 `.with_graceful_shutdown(observer.cancelled_owned())` 把它接入运行时的生命周期系统。

### 为什么 server 在后台 `tokio::spawn`

HTTP server 本质是长期运行任务。如果在当前 async 调用链里直接 `await axum::serve(...)`，启动函数就不会返回，调用方也无法继续完成其余初始化。把它放到后台执行后，启动函数只需返回地址与句柄即可。

---

## 六、Router 设计：固定系统路由 + 可扩展业务控制路由

默认挂载的路由包括：

- `health_path` → `health_handler`
- `live_path` → `health_handler`
- `/metrics` → `metrics_handler`
- `/metadata` → `metadata_handler`
- `/engine/{*path}` → `engine_route_handler`

可选挂载：

- `GET /v1/loras` → `list_loras_handler`
- `POST /v1/loras` → `load_lora_handler`
- `DELETE /v1/loras/{*lora_name}` → `unload_lora_handler`

### 为什么 `/health` 和 `/live` 共用同一个 handler

当前实现中两者都映射到 `health_handler`，说明系统目前把 readiness 与 liveness 视作同一状态源。这样做的好处是：

- 避免两套不一致的判定逻辑；
- 对部署者而言更直观；
- 与 `SystemHealth` 当前只维护一套状态模型相匹配。

未来若需要区分 liveness / readiness，只需在 handler 层拆分，不影响 server 启动骨架。

### 为什么 `/engine/*` 用通配动态路由

因为引擎暴露的控制接口数量、名称、参数都不固定。框架只保留统一前缀 `/engine/`，具体 path 交给 `EngineRouteRegistry` 决定。这延续了 `engine_routes` 模块的注册表设计，使 HTTP server 不需要知道任何引擎特定 API。

---

## 七、`health_handler()` —— 健康状态到 HTTP 的翻译层

`health_handler()` 的职责很单纯：

1. 从 `SystemHealth` 读取 `(healthy, portnames)`；
2. 读取当前 uptime；
3. 组装 JSON：`status + uptime + portnames`；
4. 将 `healthy` 映射成 `200 OK` 或 `503 SERVICE_UNAVAILABLE`。

### 为什么 handler 不自己参与健康判定

HTTP 层只负责对外协议，不负责业务语义。若健康判定逻辑散落在 handler 中：

- 测试会被迫通过 HTTP 间接验证内部逻辑；
- 其他调用方无法复用同一健康判定；
- 任何状态语义变更都要改 server 层。

因此它只调用 `SystemHealth::get_health_status()`，自己不做额外判断。

### 为什么返回 JSON 字符串而不是 `Json<T>`

这里直接返回 `(StatusCode, response.to_string())`，体现出一个实用主义选择：响应体结构简单、字段固定，手工序列化足够直接，而且能和其他直接返回字符串的 handler 保持一致。

---

## 八、`metrics_handler()` —— 多注册表指标聚合出口

`metrics_handler()` 调用 `state.drt().metrics().prometheus_expfmt()`，返回最终的 Prometheus exposition 文本。

### 为什么 metrics 由 runtime 聚合，而不是 server 自己维护 registry

因为指标树本来就属于 runtime / namespace / servicegroup / portname 的层级结构。server 若再维护一份 registry，会复制一套本不属于 HTTP 层的状态。

当前设计是：

- 指标注册发生在各层级对象内；
- 聚合逻辑也由 `metrics` 模块负责；
- HTTP server 只负责把结果暴露到 `/metrics`。

这样职责边界最清晰。

---

## 九、`metadata_handler()` —— 可选发现元数据查看接口

`metadata_handler()` 只在 `discovery_metadata` 存在时工作：

- 有 metadata：读锁读取并序列化为 JSON，返回 `200`；
- 无 metadata：返回 `404 Discovery metadata not available`。

### 为什么这里使用 `tokio::sync::RwLock`

metadata 是一个异步系统不断刷新的共享快照，HTTP 请求读取它时位于 async 上下文中，因此使用 async 读写锁更自然，能够安全地在 `.await` 场景中等待锁。

### 为什么 metadata 缺失返回 `404` 而不是 `500`

因为这不是服务器内部错误，而是“当前运行模式没有提供这类资源”。`404` 更准确表达“该能力不存在”，也避免把非必需附加能力误报成系统异常。

---

## 十、LoRA 管理接口：为什么走系统状态服务器

`load_lora_handler`、`unload_lora_handler`、`list_loras_handler` 构成一组控制面接口，用于动态管理 LoRA 适配器。

### 为什么它们挂在 system status server，而不是推理主入口

因为 LoRA 管理是运维/控制操作，而不是普通推理业务流量：

- 需要单独权限与访问路径；
- 不应与普通推理请求共享业务协议；
- 调用目标其实是本地 runtime 已注册的控制 portname，而非对外公开推理 API。

### `call_lora_portname()` 的关键设计

这个辅助函数明确规定：**只走本地 portname registry，不走网络发现回退**。

这条约束非常重要，原因是 LoRA 管理是节点内控制操作：

- 它要求调用本进程已经注册的控制 portname；
- 若本地不存在，说明当前节点没有启用该能力；
- 不应该偷偷退化为跨网络发现调用，否则控制面语义会变得模糊。

### 为什么从响应流中只取第一条消息

LoRA 控制接口本质是命令式操作，其响应是单结果而非 token stream。虽然底层复用了 `AsyncEngine` 的流式接口，但这里的协议语义是“一次操作，一次结果”，因此取第一条响应即可。

### `parse_lora_response()` —— 非严格响应的兜底解析

```rust
fn parse_lora_response(response_data: &serde_json::Value) -> LoraResponse
```

`call_lora_portname()` 在拿到本地 portname 的响应后，会优先尝试：

```rust
serde_json::from_value::<LoraResponse>(response_data.clone())
```

如果失败，才退回到 `parse_lora_response()` 做手工字段提取。这个辅助函数的意义是：允许底层控制 portname 返回“结构近似但不完全严格匹配 `LoraResponse`”的 JSON，而 HTTP 层仍能尽量解析出稳定结果。

它逐个字段读取：

- `status` 缺失时默认为 `"success"`；
- `message`、`lora_name`、`loras` 缺失时为 `None`；
- `lora_id` 读取为 `u64`；
- `count` 先读成 `u64`，再转换成 `usize`。

因此这个函数本质上是 LoRA 控制接口与外部 HTTP 协议之间的一个“宽容层”，用来降低内部响应格式轻微变化带来的脆弱性。

---

## 十一、`engine_route_handler()` —— 动态引擎控制面的 HTTP 适配器

这个 handler 做了三件事：

1. 将 request body 解析为 `serde_json::Value`；空 body 视为 `{}`；
2. 用 path 去 `engine_routes()` 注册表查找回调；
3. `await` 回调并把结果转换为 HTTP JSON 响应。

### 为什么空 body 要映射为 `{}`

这样可以让 GET 或无 body 的请求也统一走 JSON 回调接口，避免注册回调时还要区分“这个 path 是否一定有请求体”。这使 engine route callback 的签名始终保持一致。

### 为什么查不到路由返回 `404`

因为 `/engine/*` 是一块预留路径空间，是否存在具体子路由取决于运行时注册结果。没注册就是资源不存在，不是 server 出错。

### 为什么回调错误返回 `500`

一旦回调本身已找到但执行失败，说明是引擎处理逻辑错误或内部异常，这才属于标准 `500` 语义。

---

## 十二、fallback 与 tracing

### fallback

未命中的路径统一落到 fallback，返回 `404 Route not found`。这让测试用例和运维调用都能拿到稳定、可预期的错误文本，而不是默认 HTML 页面或空响应。

### `TraceLayer::new_for_http().make_span_with(make_request_span)`

系统状态服务器也接入统一 tracing span 工厂，意味着 health、metrics、engine control 等控制面请求都能进入同一套日志/链路体系。这样即使是 `/health` 请求，也能保留 traceparent / tracestate 等信息，便于排查控制面调用链。

---

## 十三、优雅关闭与生命周期接入

`spawn_system_status_server()` 从传入的 `CancellationToken` 派生一个 `child_token()`，再将其作为 Axum server 的 graceful shutdown 信号。

### 为什么使用子 token 而不是直接消费父 token

子 token 允许上层 runtime 继续统一持有主取消令牌，同时把 system status server 视为某个子系统。这样生命周期树是分层的，而不是所有后台任务都直接挂在同一个根令牌上。

### 为什么 graceful shutdown 很重要

即使这是控制面 server，也不能在进程退出时粗暴中断：

- 测试需要可预测地等待其退出；
- 运维请求可能正在进行；
- 日志与 tracing span 需要有机会正常收尾。

---

## 十四、测试覆盖说明

源码中的测试大致分为四类：

1. **基础生命周期测试**：验证 HTTP server 能启动并在 cancel 后退出；
2. **health/live 行为测试**：覆盖默认路径、自定义路径、ready/notready 状态切换；
3. **uptime / metrics 测试**：验证 uptime 来源与 Prometheus 暴露一致；
4. **LoRA / portname health / tracing 集成测试**：验证与真实 runtime 子系统的集成语义。

这些测试说明该模块虽然是 HTTP facade，但并不是一个“薄到无需验证”的壳层；它承担了控制面协议正确性的最后一跳。

---

## 十五、与其他模块的关系

- `system_health`：提供 health/live 路径、整体健康状态与 uptime；
- `metrics`：提供 `/metrics` 实际内容；
- `engine_routes`：提供 `/engine/*` 的动态回调注册表；
- `local_portname_registry`：LoRA 管理接口通过它做本地 in-process 调用；
- `discovery`：`/metadata` 读取 discovery metadata 快照；
- `logging`：通过 `make_request_span` 将控制面请求接入 tracing 体系。

因此，`system_status_server` 可以理解为运行时多个控制面能力的统一 HTTP 出口，而不是一个独立子系统。

---

## 十六、补充：路由表与 `/health` 返回语义

把 `spawn_system_status_server()` 里真正挂出来的入口摊平来看，当前行为可以总结成下面这张表：

| 路径 | 方法 | 处理逻辑 |
|------|------|---------|
| `health_path` | GET | 调 `health_handler()`，读取聚合健康状态并返回 200 或 503 |
| `live_path` | GET | 当前与 `health_path` 共用同一个 handler，不是独立 liveness 语义 |
| `/metrics` | GET | 调 `metrics_handler()`，输出 Prometheus exposition text |
| `/metadata` | GET | discovery metadata 存在则返回 JSON，否则 404 |
| `/engine/{*path}` | ALL | 调 `engine_route_handler()`，按注册表把请求桥接到 runtime callback |
| `/v1/loras` | GET / POST | 仅在 `PGD_LORA_ENABLED=true` 时注册，分别对应 list / load |
| `/v1/loras/{*lora_name}` | DELETE | 仅在 `PGD_LORA_ENABLED=true` 时注册，对应 unload |
| `fallback` | ALL | 返回 `404 Route not found` |

其中最容易被忽略的一点是：`/live` 当前并不是传统意义上“只表示进程活着”的独立接口，而是直接复用了 readiness 的状态来源。

`health_handler()` 返回体的语义大致如下：

```json
{
    "status": "ready",
    "uptime": "...",
    "portnames": {
        "default.generate.v1": "ready",
        "default.embed.v1": "notready"
    }
}
```

这说明 handler 本身并不参与健康计算，它只是把 `SystemHealth::get_health_status()` 的结果和 `uptime()` 一起翻译成 HTTP 响应。

---

## 十七、补充：LoRA helper 的复用链路

LoRA 相关的三个 handler 虽然对外是不同 HTTP 路由，但内部其实共用同一套处理步骤：

1. HTTP handler 先把请求转换成 JSON；
2. 再调用本地控制入口 helper；
3. helper 只查本进程 registry，不走远端发现；
4. 拿到首条响应后优先尝试结构化反序列化；
5. 若结构不完全匹配，再用兜底解析逻辑补出统一响应结构。

这样设计的直接收益是：每个 LoRA handler 不需要重复写“查本地 portname、调用 engine、解析首条结果”的样板代码，控制面协议也因此更稳定。

---

## 十八、补充：`engine_route_handler()` 的请求边界

除了“按路径查 callback、执行 callback”之外，这个 handler 还有两个值得单独强调的边界：

- 空 body 会被规范化成 `{}`，因此同一套 callback 可以同时服务 GET 和无 body 的控制请求；
- 非空 body 必须能解析成 JSON，否则会直接返回 `400 Invalid JSON`，不会把坏请求继续传进 runtime。

因此它本质上是一个很薄但边界清楚的 HTTP 适配层：

- 路由存在性由 `engine_routes()` 决定；
- 请求体合法性在这里收口；
- 真正的控制逻辑仍由 runtime 内部注册的 callback 执行。

---

## 十九、补充：在三模块协作中的角色

如果把 `system_health`、`health_check`、`system_status_server` 串起来看，`system_status_server` 的职责边界会更清楚：

1. `SystemHealth` 先负责保存系统与端点健康状态；
2. `HealthCheckManager` 在后台根据空闲超时和探测结果持续更新这些状态；
3. `system_status_server` 不自己做探测，也不自己决定健康规则；
4. 它只是在收到 HTTP 请求时，把 runtime 里已经存在的状态翻译成 `/health`、`/live`、`/metrics`、`/metadata` 和 `/engine/*` 这些对外接口。

所以它更像“统一出口层”而不是“状态决策层”：

- 状态真相在 `SystemHealth`；
- 主动执行在 `health_check`；
- 对外协议适配在 `system_status_server`。
