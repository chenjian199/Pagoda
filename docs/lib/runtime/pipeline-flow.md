# pagoda-runtime 请求流水线阅读指南

> 这个文件旨在展示runtime模块的功能，流程分为rust用户端入口配置，到各服务线程的初始化，以及请求流程的建立和传输。具体展示了各个功能的入口，以及使用方式。
---

## 1. 全局过程，流水线总览

从运行时角度看，一次请求会经过两条主线：

1. **服务端主线**：Worker 启动 → 构建 `DistributedRuntime` → 注册 endpoint → 暴露请求平面 → 注册到 discovery。
2. **客户端主线**：拿到 `PortName` 客户端 → 从 discovery 订阅实例变化 → 选一个实例 → 通过请求平面发请求 → 通过响应平面接收流式返回。

主干图：

```text
业务入口
  ↓
crate::worker::Worker::execute()                       (lib/runtime/src/worker.rs)
  ↓
crate::distributed::DistributedRuntime::new()         (lib/runtime/src/distributed.rs)
  ↓
crate::component::Namespace / ServiceGroup / PortName    (lib/runtime/src/servicegroup.rs)
  ↓
crate::component::endpoint::EndpointConfigBuilder::start()
                                                     (lib/runtime/src/servicegroup/portname.rs)
  ├─ 注册到请求平面 server
  │    └─ crate::pipeline::network::manager::NetworkManager::server()
  │       (lib/runtime/src/pipeline/network/manager.rs)
  │    └─ crate::pipeline::network::ingress::shared_tcp_endpoint::SharedTcpServer::register_endpoint()
  │       (lib/runtime/src/pipeline/network/ingress/shared_tcp_portname.rs)
  └─ 注册到 discovery
       └─ crate::discovery::Discovery::register()     (lib/runtime/src/discovery/mod.rs)

客户端请求
  ↓
crate::component::PortName::client()                  (lib/runtime/src/servicegroup.rs)
  ↓
crate::component::client::Client::new()               (lib/runtime/src/servicegroup/client.rs)
  ↓
crate::pipeline::network::egress::push_router::PushRouter::from_client()
                                                     (lib/runtime/src/pipeline/network/egress/push_router.rs)
  ↓
crate::pipeline::network::egress::addressed_router::AddressedPushRouter::generate()
                                                     (lib/runtime/src/pipeline/network/egress/addressed_router.rs)
  ↓
crate::pipeline::network::egress::tcp_client::TcpRequestClient::send_request()
                                                     (lib/runtime/src/pipeline/network/egress/tcp_client.rs)
  ↓
worker 侧 SharedTcpServer 收到请求并执行 handler
  ↓
crate::pipeline::network::tcp::server::TcpStreamServer 反向接收响应流
                                                     (lib/runtime/src/pipeline/network/tcp/server.rs)
```

---

## 2. 快捷跳转方式

链路调用代码：

| 顺序 | 文件 | 主要内容 |
| --- | --- | --- |
| 1 | [lib/runtime/src/worker.rs#L49](../src/worker.rs#L49) | 进程入口，负责启动 Runtime 和应用生命周期 |
| 2 | [lib/runtime/src/distributed.rs#L44](../src/distributed.rs#L44) | 组装 `DistributedRuntime`，把 discovery、network、health、metrics 串起来 |
| 3 | [lib/runtime/src/servicegroup.rs#L144](../src/servicegroup.rs#L144) | `Namespace`、`ServiceGroup`、`PortName` 三层抽象 |
| 4 | [lib/runtime/src/servicegroup/portname.rs#L69](../src/servicegroup/portname.rs#L69) | endpoint 启动、注册、注销的核心入口 |
| 5 | [lib/runtime/src/pipeline/network/manager.rs#L201](../src/pipeline/network/manager.rs#L201) | 请求平面的 server/client 工厂 |
| 6 | [lib/runtime/src/pipeline/network/ingress/shared_tcp_portname.rs#L73](../src/pipeline/network/ingress/shared_tcp_portname.rs#L73) | TCP 请求平面服务端 |
| 7 | [lib/runtime/src/servicegroup/client.rs#L97](../src/servicegroup/client.rs#L97) | discovery watch、实例缓存、实例可用性维护 |
| 8 | [lib/runtime/src/pipeline/network/egress/push_router.rs#L97](../src/pipeline/network/egress/push_router.rs#L97) | 实例选路逻辑 |
| 9 | [lib/runtime/src/pipeline/network/egress/addressed_router.rs#L128](../src/pipeline/network/egress/addressed_router.rs#L128) | 把“已选定地址的请求”发出去 |
| 10 | [lib/runtime/src/pipeline/network/tcp/server.rs#L72](../src/pipeline/network/tcp/server.rs#L72) | 客户端侧响应平面，接收流式返回 |

---

## 3. 阶段一：Worker 启动并构建 `DistributedRuntime`

### 3.1 功能说明

这一阶段只做一件事：**把一个普通进程变成一个可以承载分布式请求的 Runtime 进程**。

它主要完成以下事情：

- 创建 Tokio runtime 和 Pagoda `Runtime`
- 准备取消令牌、优雅退出控制、计算线程池等本地运行时能力
- 创建 `DistributedRuntime`
- 在 `DistributedRuntime` 中初始化 discovery、network manager、健康检查、metrics、system status server 等共享设施

换句话说：这一阶段还没有真正开始处理业务请求，但后面所有的请求链路都会依赖这里创建的对象。

### 3.2 关键类与函数

| 类型 / 函数 | 定义位置 | 作用 |
| --- | --- | --- |
| `crate::worker::Worker` | [lib/runtime/src/worker.rs#L49](../src/worker.rs#L49) | 业务进程入口包装器 |
| `crate::worker::Worker::from_settings()` | [lib/runtime/src/worker.rs#L56](../src/worker.rs#L56) | 从配置创建 Worker |
| `crate::worker::Worker::execute()` | [lib/runtime/src/worker.rs#L98](../src/worker.rs#L98) | 运行应用闭包并管理生命周期 |
| `crate::runtime::Runtime` | [lib/runtime/src/runtime.rs#L41](../src/runtime.rs#L41) | Pagoda 自己的 runtime 封装 |
| `crate::distributed::DistributedRuntime` | [lib/runtime/src/distributed.rs#L44](../src/distributed.rs#L44) | 分布式运行时根对象 |
| `crate::distributed::DistributedRuntime::new()` | [lib/runtime/src/distributed.rs#L105](../src/distributed.rs#L105) | 初始化 discovery / network / metrics / health |
| `crate::pipeline::network::manager::NetworkManager` | [lib/runtime/src/pipeline/network/manager.rs#L201](../src/pipeline/network/manager.rs#L201) | 请求平面统一工厂 |
| `crate::discovery::KVStoreDiscovery` | [lib/runtime/src/discovery/kv_store.rs](../src/discovery/kv_store.rs) | KV / etcd 发现后端 |
| `crate::discovery::KubeDiscoveryClient` | [lib/runtime/src/discovery/kube.rs](../src/discovery/kube.rs) | Kubernetes 发现后端 |

### 3.3 调用代码

#### 业务侧调用

```rust
use dynamo_runtime::worker::Worker;
use dynamo_runtime::distributed::DistributedRuntime;

Worker::from_settings()?.execute(|runtime| async move {
    let drt = DistributedRuntime::from_settings(runtime).await?;
    // 后续所有 namespace/servicegroup/endpoint 都从 drt 开始
    Ok(())
})?;
```

#### 内部调用链

```text
crate::worker::Worker::from_settings()                      (lib/runtime/src/worker.rs)
  → crate::config::RuntimeConfig::from_settings()          (lib/runtime/src/config.rs)
  → crate::worker::Worker::from_config()                   (lib/runtime/src/worker.rs)
  → crate::runtime::Runtime::from_handle()                 (lib/runtime/src/runtime.rs)

crate::worker::Worker::execute()                           (lib/runtime/src/worker.rs)
  → crate::worker::Worker::execute_internal()              (lib/runtime/src/worker.rs)
  → 用户闭包
  → crate::distributed::DistributedRuntime::from_settings()(lib/runtime/src/distributed.rs)
  → crate::distributed::DistributedRuntime::new()          (lib/runtime/src/distributed.rs)
```

### 3.4 这一阶段的重点

- [lib/runtime/src/worker.rs](../src/worker.rs) 解决的是**进程如何活起来、如何退出**。
- [lib/runtime/src/distributed.rs](../src/distributed.rs) 解决的是**这个进程需要哪些全局共享资源**。
- `DistributedRuntime` 是后续所有对象的根：`Namespace`、`ServiceGroup`、`PortName`、discovery、network manager 都从这里拿。

---

## 4. 阶段二：从 `Namespace` 到 `PortName`，把业务对象挂到 Runtime 上

### 4.1 功能说明

这一层不是网络层，而是**业务命名层**。

这里把一个 endpoint 的身份拆成三层：

- `Namespace`：业务域
- `ServiceGroup`：该域下的服务或组件
- `PortName`：组件下的具体请求入口

它的作用是：

- 给 endpoint 提供唯一身份（如 `llm/worker/generate`）
- 给 metrics、discovery、routing 提供统一命名
- 把“业务上的 endpoint”转换成“后面可以注册到请求平面和发现平面的 endpoint”

### 4.2 关键类与函数

| 类型 / 函数 | 定义位置 | 作用 |
| --- | --- | --- |
| `crate::component::Namespace` | [lib/runtime/src/servicegroup.rs#L414](../src/servicegroup.rs#L414) | 命名空间对象 |
| `crate::component::Namespace::component()` | [lib/runtime/src/servicegroup.rs#L486](../src/servicegroup.rs#L486) | 创建或复用 ServiceGroup |
| `crate::component::ServiceGroup` | [lib/runtime/src/servicegroup.rs#L144](../src/servicegroup.rs#L144) | 组件对象 |
| `crate::component::ServiceGroup::endpoint()` | [lib/runtime/src/servicegroup.rs#L241](../src/servicegroup.rs#L241) | 创建 PortName |
| `crate::component::PortName` | [lib/runtime/src/servicegroup.rs#L323](../src/servicegroup.rs#L323) | endpoint 元信息 |
| `crate::component::PortName::id()` | [lib/runtime/src/servicegroup.rs#L387](../src/servicegroup.rs#L387) | 构造 `EndpointId` |
| `crate::distributed::DistributedRuntime::namespace()` | [lib/runtime/src/distributed.rs#L334](../src/distributed.rs#L334) | 从 `DistributedRuntime` 创建顶层 Namespace |

### 4.3 调用代码

#### 业务侧调用

```rust
let ns = drt.namespace("llm")?;
let comp = ns.service_group("worker")?;
let ep = comp.portname("generate");
```

#### 内部调用链

```text
crate::distributed::DistributedRuntime::namespace()        (lib/runtime/src/distributed.rs)
  → crate::component::Namespace::new()                     (lib/runtime/src/servicegroup.rs)

crate::component::Namespace::component()                   (lib/runtime/src/servicegroup.rs)
  → crate::component::ComponentBuilder::build()            (lib/runtime/src/servicegroup.rs)

crate::component::ServiceGroup::endpoint()                    (lib/runtime/src/servicegroup.rs)
  → 生成 crate::component::PortName                        (lib/runtime/src/servicegroup.rs)
```

### 4.4 这一阶段的重点

- 这层对象本身不负责收发网络包。
- 它负责定义“我是谁”，后面的 discovery 和 routing 都依赖这个身份。
- `PortName::id()` 产生的 `crate::protocols::EndpointId`（定义在 [lib/runtime/src/protocols.rs](../src/protocols.rs)）是后面拼 transport、拼 discovery key 的基础。

---

## 5. 阶段三：服务端注册 —— `.start().await` 到底做了什么

### 5.1 功能说明

这是服务端最关键的一步。  
`crate::component::endpoint::EndpointConfigBuilder::start()`（文件：[lib/runtime/src/servicegroup/portname.rs#L69](../src/servicegroup/portname.rs#L69)）会把一个“业务 handler”真正变成可访问的远程 endpoint。

它实际完成了四件事：

1. 给 handler 绑定 metrics
2. 向请求平面 server 注册 endpoint
3. 向 discovery 注册实例信息
4. 安装关闭清理逻辑

也就是说，**服务端是否真正“上线”**，是由这一步决定的。

### 5.2 关键类与函数

| 类型 / 函数 | 定义位置 | 作用 |
| --- | --- | --- |
| `crate::component::endpoint::EndpointConfigBuilder` | [lib/runtime/src/servicegroup/portname.rs](../src/servicegroup/portname.rs) | endpoint 启动配置 Builder |
| `crate::component::endpoint::EndpointConfigBuilder::start()` | [lib/runtime/src/servicegroup/portname.rs#L69](../src/servicegroup/portname.rs#L69) | 启动 endpoint |
| `crate::pipeline::network::Ingress` | [lib/runtime/src/pipeline/network.rs#L288](../src/pipeline/network.rs#L288) | 把业务 engine 包装成 `PushWorkHandler` |
| `crate::pipeline::network::Ingress::add_metrics()` | [lib/runtime/src/pipeline/network.rs#L307](../src/pipeline/network.rs#L307) | 创建 endpoint 级 metrics |
| `crate::distributed::DistributedRuntime::request_plane_server()` | [lib/runtime/src/distributed.rs#L366](../src/distributed.rs#L366) | 获取请求平面 server |
| `crate::pipeline::network::manager::NetworkManager::server()` | [lib/runtime/src/pipeline/network/manager.rs#L290](../src/pipeline/network/manager.rs#L290) | 按模式懒初始化 server |
| `crate::pipeline::network::ingress::shared_tcp_endpoint::SharedTcpServer` | [lib/runtime/src/pipeline/network/ingress/shared_tcp_portname.rs#L73](../src/pipeline/network/ingress/shared_tcp_portname.rs#L73) | TCP 请求平面服务端 |
| `crate::pipeline::network::ingress::shared_tcp_endpoint::SharedTcpServer::register_endpoint()` | [lib/runtime/src/pipeline/network/ingress/shared_tcp_portname.rs#L301](../src/pipeline/network/ingress/shared_tcp_portname.rs#L301) | 将 endpoint handler 注册进路由表 |
| `crate::discovery::Discovery::register()` | [lib/runtime/src/discovery/mod.rs#L762](../src/discovery/mod.rs#L762) | 注册到 discovery |

### 5.3 调用代码

#### 业务侧调用

```rust
use dynamo_runtime::pipeline::network::Ingress;

let ingress = Ingress::for_engine(my_engine)?;

comp.portname("generate")
    .portname_builder()
    .handler(ingress)
    .start()
    .await?;
```

#### 内部调用链

```text
crate::component::PortName::endpoint_builder()                 (lib/runtime/src/servicegroup.rs)
  → crate::component::endpoint::EndpointConfigBuilder::from_endpoint()
                                                             (lib/runtime/src/servicegroup/portname.rs)

crate::component::endpoint::EndpointConfigBuilder::start()    (lib/runtime/src/servicegroup/portname.rs)
  → crate::pipeline::network::Ingress::add_metrics()          (lib/runtime/src/pipeline/network.rs)
  → crate::distributed::DistributedRuntime::request_plane_server()
                                                             (lib/runtime/src/distributed.rs)
  → crate::pipeline::network::manager::NetworkManager::server()
                                                             (lib/runtime/src/pipeline/network/manager.rs)
  → crate::pipeline::network::ingress::shared_tcp_endpoint::SharedTcpServer::register_endpoint()
                                                             (lib/runtime/src/pipeline/network/ingress/shared_tcp_portname.rs)
  → crate::component::endpoint::build_transport_type()        (lib/runtime/src/servicegroup/portname.rs)
  → crate::discovery::Discovery::register()                   (lib/runtime/src/discovery/mod.rs)
```

### 5.4 TCP 请求平面这一步到底注册了什么

当请求平面模式是 TCP 时，`NetworkManager::server()` 最终会走到：

- `crate::pipeline::network::manager::NetworkManager::create_tcp_server()`  
  文件：[lib/runtime/src/pipeline/network/manager.rs#L290](../src/pipeline/network/manager.rs#L290)
- `crate::pipeline::network::ingress::shared_tcp_endpoint::SharedTcpServer::new()`  
  文件：[lib/runtime/src/pipeline/network/ingress/shared_tcp_portname.rs#L73](../src/pipeline/network/ingress/shared_tcp_portname.rs#L73)
- `crate::pipeline::network::ingress::shared_tcp_endpoint::SharedTcpServer::bind_and_start()`  
  文件：[lib/runtime/src/pipeline/network/ingress/shared_tcp_portname.rs#L237](../src/pipeline/network/ingress/shared_tcp_portname.rs#L237)

这几步会创建以下长期存在的后台实体：

- 一个共享的 TCP listener
- 一个 `accept_loop`
- 一个 worker dispatcher（带并发上限和队列）
- 一个 `DashMap<String, Arc<EndpointHandler>>` 路由表

其中 `EndpointHandler` 定义在：

- [lib/runtime/src/pipeline/network/ingress/shared_tcp_portname.rs](../src/pipeline/network/ingress/shared_tcp_portname.rs)

它持有：

- `service_handler: Arc<dyn PushWorkHandler>`
- `instance_id`
- endpoint 的 namespace/servicegroup/endpoint 名称
- inflight 计数器
- 优雅退出用 `Notify`

### 5.5 discovery 注册这一步到底注册了什么

`EndpointConfigBuilder::start()` 在请求平面注册成功后，还会调用：

- `crate::discovery::Discovery::register()`  
  文件：[lib/runtime/src/discovery/mod.rs#L762](../src/discovery/mod.rs#L762)

它接收：

```rust
crate::discovery::DiscoverySpec::PortName {
    namespace,
    component,
    endpoint,
    transport,
}
```

然后根据后端不同，转到不同实现：

- `crate::discovery::KVStoreDiscovery::register_internal()`  
  文件：[lib/runtime/src/discovery/kv_store.rs](../src/discovery/kv_store.rs)
- `crate::discovery::KubeDiscoveryClient::register_internal()`  
  文件：[lib/runtime/src/discovery/kube.rs](../src/discovery/kube.rs)

这里要抓住一个核心点：

- **请求平面**负责“请求如何发到这个实例”
- **discovery 平面**负责“别人如何知道这个实例存在”

这两层是配合关系，不是同一层。

---

## 6. 阶段四：客户端如何感知实例变化

### 6.1 功能说明

客户端不是每次请求都去现查 discovery，而是先建立一个**持续更新的实例视图**。

这一层由 `crate::component::client::Client`（文件：[lib/runtime/src/servicegroup/client.rs#L97](../src/servicegroup/client.rs#L97)）负责，它做三件事：

1. 建立对 discovery 的 `list_and_watch`
2. 维护当前 endpoint 对应的实例集合
3. 维护“可用实例”和“空闲实例”两套缓存，供路由器选路

### 6.2 关键类与函数

| 类型 / 函数 | 定义位置 | 作用 |
| --- | --- | --- |
| `crate::component::PortName::client()` | [lib/runtime/src/servicegroup.rs#L403](../src/servicegroup.rs#L403) | 创建 endpoint 客户端 |
| `crate::component::client::Client` | [lib/runtime/src/servicegroup/client.rs#L97](../src/servicegroup/client.rs#L97) | discovery 感知客户端 |
| `crate::component::client::Client::new()` | [lib/runtime/src/servicegroup/client.rs#L117](../src/servicegroup/client.rs#L117) | 创建动态实例客户端 |
| `crate::component::client::Client::get_or_create_dynamic_instance_source()` | [lib/runtime/src/servicegroup/client.rs#L284](../src/servicegroup/client.rs#L284) | 建立 discovery watch |
| `crate::component::client::Client::monitor_instance_source()` | [lib/runtime/src/servicegroup/client.rs#L235](../src/servicegroup/client.rs#L235) | 同步实例状态到本地缓存 |
| `crate::discovery::Discovery::list_and_watch()` | [lib/runtime/src/discovery/mod.rs](../src/discovery/mod.rs) | 发现层事件流接口 |

### 6.3 调用代码

#### 业务侧调用

```rust
let client = endpoint.client().await?;
let instances = client.wait_for_instances().await?;
```

#### 内部调用链

```text
crate::component::PortName::client()                           (lib/runtime/src/servicegroup.rs)
  → crate::component::client::Client::new()                   (lib/runtime/src/servicegroup/client.rs)
  → crate::component::client::Client::with_reconcile_interval()
                                                             (lib/runtime/src/servicegroup/client.rs)
  → crate::component::client::Client::get_or_create_dynamic_instance_source()
                                                             (lib/runtime/src/servicegroup/client.rs)
  → crate::discovery::Discovery::list_and_watch()            (lib/runtime/src/discovery/mod.rs)
  → crate::component::client::Client::monitor_instance_source()
                                                             (lib/runtime/src/servicegroup/client.rs)
```

### 6.4 这一层维护了哪些状态

`crate::component::client::Client`（文件：[lib/runtime/src/servicegroup/client.rs#L97](../src/servicegroup/client.rs#L97)）内部长期维护：

- `instance_source`：从 discovery watch 得到的原始实例集合
- `instance_avail`：当前可用实例 ID 列表
- `instance_free`：当前未被判定过载的实例 ID 列表
- 一个后台 monitor task：把 watch 更新同步到本地缓存

所以，后面的 router 选路时，通常不会直接读 discovery，而是读 `Client` 的本地视图。

---

## 7. 阶段五：客户端如何选一个实例来发请求

### 7.1 功能说明

这一层的核心类是：

- `crate::pipeline::network::egress::push_router::PushRouter<T, U>`  
  文件：[lib/runtime/src/pipeline/network/egress/push_router.rs#L97](../src/pipeline/network/egress/push_router.rs#L97)

它的职责非常明确：

1. 根据路由策略选一个实例 ID
2. 从 `Client` 当前持有的实例视图里取出目标实例的 transport 地址
3. 把原始请求包装成 `AddressedRequest`
4. 交给下一层 `AddressedPushRouter`

也就是说，`PushRouter` 负责的是**“选谁”**，不是**“怎么发”**。

### 7.2 关键类与函数

| 类型 / 函数 | 定义位置 | 作用 |
| --- | --- | --- |
| `crate::pipeline::network::egress::push_router::PushRouter` | [lib/runtime/src/pipeline/network/egress/push_router.rs#L97](../src/pipeline/network/egress/push_router.rs#L97) | 选路器 |
| `crate::pipeline::network::egress::push_router::PushRouter::from_client()` | [lib/runtime/src/pipeline/network/egress/push_router.rs#L207](../src/pipeline/network/egress/push_router.rs#L207) | 从 `Client` 构建路由器 |
| `crate::pipeline::network::egress::push_router::PushRouter::round_robin()` | [lib/runtime/src/pipeline/network/egress/push_router.rs#L281](../src/pipeline/network/egress/push_router.rs#L281) | 轮询选路 |
| `crate::pipeline::network::egress::push_router::PushRouter::random()` | [lib/runtime/src/pipeline/network/egress/push_router.rs#L302](../src/pipeline/network/egress/push_router.rs#L302) | 随机选路 |
| `crate::pipeline::network::egress::push_router::PushRouter::power_of_two_choices()` | [lib/runtime/src/pipeline/network/egress/push_router.rs#L321](../src/pipeline/network/egress/push_router.rs#L321) | P2C 选路 |
| `crate::pipeline::network::egress::push_router::PushRouter::direct()` | [lib/runtime/src/pipeline/network/egress/push_router.rs#L353](../src/pipeline/network/egress/push_router.rs#L353) | 指定实例直连 |
| `crate::pipeline::network::egress::addressed_router::AddressedRequest` | [lib/runtime/src/pipeline/network/egress/addressed_router.rs#L103](../src/pipeline/network/egress/addressed_router.rs#L103) | 已带目标地址的请求 |

### 7.3 调用代码

#### 构造路由器

```rust
use dynamo_runtime::pipeline::network::egress::push_router::{PushRouter, RouterMode};

let client = endpoint.client().await?;
let router = PushRouter::<Req, Resp>::from_client(client, RouterMode::RoundRobin).await?;
```

#### 内部调用链

```text
crate::pipeline::network::egress::push_router::PushRouter::from_client()
                                                             (lib/runtime/src/pipeline/network/egress/push_router.rs)
  → addressed_router(endpoint)                               (lib/runtime/src/pipeline/network/egress/push_router.rs)
  → crate::pipeline::network::manager::NetworkManager::create_client()
                                                             (lib/runtime/src/pipeline/network/manager.rs)
  → crate::distributed::DistributedRuntime::tcp_server()     (lib/runtime/src/distributed.rs)
  → crate::pipeline::network::egress::addressed_router::AddressedPushRouter::new()
                                                             (lib/runtime/src/pipeline/network/egress/addressed_router.rs)
```

#### 发起一次路由请求

```text
crate::pipeline::network::egress::push_router::PushRouter::round_robin()
                                                             (lib/runtime/src/pipeline/network/egress/push_router.rs)
  → 选择 instance_id
  → 从 client.instances() 中拿到 transport 地址
  → crate::pipeline::network::egress::addressed_router::AddressedRequest::new()
                                                             (lib/runtime/src/pipeline/network/egress/addressed_router.rs)
  → crate::pipeline::network::egress::addressed_router::AddressedPushRouter::generate()
                                                             (lib/runtime/src/pipeline/network/egress/addressed_router.rs)
```

### 7.4 这层最重要的理解点

- `PushRouter` 关心的是“**选哪个实例**”。
- `AddressedPushRouter` 关心的是“**已经选好了实例，怎么把请求送过去**”。
- 这两层分开以后，选路策略和传输策略就不会混在一起。

---

## 8. 阶段六：请求是如何真正发到 Worker 的

### 8.1 功能说明

一旦实例已经选好，请求就会交给：

- `crate::pipeline::network::egress::addressed_router::AddressedPushRouter::generate()`  
  文件：[lib/runtime/src/pipeline/network/egress/addressed_router.rs#L128](../src/pipeline/network/egress/addressed_router.rs#L128)

它负责完成下面这些事情：

1. 在响应平面上注册一个“等待返回流”的槽位
2. 构造控制消息 + 请求消息
3. 编码成二进制 buffer
4. 调用请求平面 client 的 `send_request()`
5. 等待 worker 反向连接响应平面并开始推送返回流

### 8.2 关键类与函数

| 类型 / 函数 | 定义位置 | 作用 |
| --- | --- | --- |
| `crate::pipeline::network::egress::addressed_router::AddressedPushRouter` | [lib/runtime/src/pipeline/network/egress/addressed_router.rs#L128](../src/pipeline/network/egress/addressed_router.rs#L128) | 已带地址的请求发送器 |
| `crate::pipeline::network::egress::addressed_router::AddressedPushRouter::generate()` | [lib/runtime/src/pipeline/network/egress/addressed_router.rs#L158](../src/pipeline/network/egress/addressed_router.rs#L158) | 发送一次请求并返回流式响应 |
| `crate::pipeline::network::egress::unified_client::RequestPlaneClient` | [lib/runtime/src/pipeline/network/egress/unified_client.rs](../src/pipeline/network/egress/unified_client.rs) | 请求平面 client trait |
| `crate::pipeline::network::egress::tcp_client::TcpRequestClient` | [lib/runtime/src/pipeline/network/egress/tcp_client.rs#L431](../src/pipeline/network/egress/tcp_client.rs#L431) | TCP 请求平面 client |
| `crate::pipeline::network::egress::tcp_client::TcpRequestClient::send_request()` | [lib/runtime/src/pipeline/network/egress/tcp_client.rs#L504](../src/pipeline/network/egress/tcp_client.rs#L504) | 通过 TCP 发请求 |
| `crate::pipeline::network::tcp::server::TcpStreamServer` | [lib/runtime/src/pipeline/network/tcp/server.rs#L72](../src/pipeline/network/tcp/server.rs#L72) | 响应平面服务端 |
| `crate::pipeline::network::tcp::server::TcpStreamServer::register()` | [lib/runtime/src/pipeline/network/tcp/server.rs#L239](../src/pipeline/network/tcp/server.rs#L239) | 注册一个等待返回流的连接槽位 |

### 8.3 调用代码

#### 关键调用链

```text
crate::pipeline::network::egress::addressed_router::AddressedPushRouter::generate()
                                                             (lib/runtime/src/pipeline/network/egress/addressed_router.rs)
  → crate::pipeline::network::tcp::server::TcpStreamServer::register()
                                                             (lib/runtime/src/pipeline/network/tcp/server.rs)
  → 构造 RequestControlMessage + request payload
  → TwoPartCodec::encode_message()                           (codec 实现在 lib/runtime/src/pipeline/network/codec/*.rs)
  → crate::pipeline::network::egress::unified_client::RequestPlaneClient::send_request()
  → crate::pipeline::network::egress::tcp_client::TcpRequestClient::send_request()
                                                             (lib/runtime/src/pipeline/network/egress/tcp_client.rs)
```

### 8.4 TCP 请求平面 client 实际干了什么

当模式是 TCP 时，`RequestPlaneClient` 的具体实现是：

- `crate::pipeline::network::egress::tcp_client::TcpRequestClient`  
  文件：[lib/runtime/src/pipeline/network/egress/tcp_client.rs#L431](../src/pipeline/network/egress/tcp_client.rs#L431)

它的 `send_request()` 会做这些事：

1. 解析地址字符串（如 `host:port/instance_id_hex/endpoint_name`）
2. 从连接池里取一个连接
3. 写入请求 payload 和 headers
4. 等 ACK
5. 成功则把连接放回池里

所以它解决的是：**如何把已经编码好的请求可靠地塞到目标 worker 的请求平面里**。

---

## 9. 阶段七：Worker 收到请求后如何执行 handler

### 9.1 功能说明

worker 收到 TCP 请求后，真正接住请求的是：

- `crate::pipeline::network::ingress::shared_tcp_endpoint::SharedTcpServer`  
  文件：[lib/runtime/src/pipeline/network/ingress/shared_tcp_portname.rs#L73](../src/pipeline/network/ingress/shared_tcp_portname.rs#L73)

它在运行时维护一个 endpoint 路由表，并在收到请求后：

1. 根据 endpoint path 找到对应的 `EndpointHandler`
2. 把请求包装成 `WorkItem`
3. 投递到 worker pool dispatcher
4. dispatcher 再异步调用 `PushWorkHandler::handle_payload()`

### 9.2 关键类与函数

| 类型 / 函数 | 定义位置 | 作用 |
| --- | --- | --- |
| `crate::pipeline::network::ingress::shared_tcp_endpoint::SharedTcpServer::bind_and_start()` | [lib/runtime/src/pipeline/network/ingress/shared_tcp_portname.rs#L237](../src/pipeline/network/ingress/shared_tcp_portname.rs#L237) | 绑定监听端口并启动 accept loop |
| `crate::pipeline::network::ingress::shared_tcp_endpoint::SharedTcpServer::accept_loop()` | [lib/runtime/src/pipeline/network/ingress/shared_tcp_portname.rs#L269](../src/pipeline/network/ingress/shared_tcp_portname.rs#L269) | 接收 TCP 连接 |
| `crate::pipeline::network::ingress::shared_tcp_endpoint::SharedTcpServer::handle_connection()` | [lib/runtime/src/pipeline/network/ingress/shared_tcp_portname.rs#L382](../src/pipeline/network/ingress/shared_tcp_portname.rs#L382) | 处理单连接 |
| `crate::pipeline::network::ingress::shared_tcp_endpoint::SharedTcpServer::read_loop()` | [lib/runtime/src/pipeline/network/ingress/shared_tcp_portname.rs#L407](../src/pipeline/network/ingress/shared_tcp_portname.rs#L407) | 读请求、查 handler、投递工作 |
| `crate::pipeline::network::ingress::shared_tcp_endpoint::SharedTcpServer::start_worker_pool()` | [lib/runtime/src/pipeline/network/ingress/shared_tcp_portname.rs#L127](../src/pipeline/network/ingress/shared_tcp_portname.rs#L127) | 创建工作分发循环 |
| `crate::pipeline::network::ingress::shared_tcp_endpoint::SharedTcpServer::handle_work_item()` | [lib/runtime/src/pipeline/network/ingress/shared_tcp_portname.rs#L183](../src/pipeline/network/ingress/shared_tcp_portname.rs#L183) | 执行真正的 handler |
| `crate::pipeline::network::PushWorkHandler::handle_payload()` | [lib/runtime/src/pipeline/network.rs](../src/pipeline/network.rs) | 业务 handler trait |

### 9.3 调用代码

```text
TCP 连接进入 SharedTcpServer.accept_loop()                   (lib/runtime/src/pipeline/network/ingress/shared_tcp_portname.rs)
  → SharedTcpServer.handle_connection()                      (同文件)
  → SharedTcpServer.read_loop()                              (同文件)
  → handlers.get(endpoint_path)
  → 构造 WorkItem
  → work_tx.send(work_item)

worker dispatcher
  → SharedTcpServer.handle_work_item()                       (同文件)
  → service_handler.handle_payload(payload)                  (trait 定义在 lib/runtime/src/pipeline/network.rs)
```

### 9.4 这一层最重要的理解点

- `SharedTcpServer` 并不直接懂业务类型，它只认 `Bytes` 和 `PushWorkHandler`。
- 业务类型的反序列化、真正的 engine 调用，发生在 `Ingress` / 业务 handler 这一侧。
- 这层的重点是**高并发收包、路由、排队、调度**。

---

## 10. 阶段八：响应为什么是“反向流”返回给客户端的

### 10.1 功能说明

客户端在发请求之前，会先在本地创建一个响应平面槽位：

- `crate::pipeline::network::tcp::server::TcpStreamServer::register()`  
  文件：[lib/runtime/src/pipeline/network/tcp/server.rs#L72](../src/pipeline/network/tcp/server.rs#L72)

这个槽位的作用是：**先告诉系统“我准备好接收这个 request_id 的返回流了”**。

因此，完整的思路是：

1. 前端先在本地响应平面注册一个等待槽
2. 前端再把请求发到 worker
3. worker 执行业务逻辑后，把输出流发回前端响应平面
4. 前端把这个流包装成 `ManyOut<U>` 返回给业务代码

### 10.2 关键类与函数

| 类型 / 函数 | 定义位置 | 作用 |
| --- | --- | --- |
| `crate::pipeline::network::tcp::server::TcpStreamServer` | [lib/runtime/src/pipeline/network/tcp/server.rs#L72](../src/pipeline/network/tcp/server.rs#L72) | 响应平面服务端 |
| `crate::pipeline::network::tcp::server::TcpStreamServer::register()` | [lib/runtime/src/pipeline/network/tcp/server.rs#L239](../src/pipeline/network/tcp/server.rs#L239) | 注册待接收连接 |
| `crate::pipeline::network::ResponseService` | [lib/runtime/src/pipeline/network.rs](../src/pipeline/network.rs) | 响应平面 trait |
| `crate::pipeline::ResponseStream` | [lib/runtime/src/engine.rs](../src/engine.rs) | 封装最终返回给业务侧的流 |

### 10.3 调用代码

```text
crate::pipeline::network::egress::addressed_router::AddressedPushRouter::generate()
                                                             (lib/runtime/src/pipeline/network/egress/addressed_router.rs)
  → crate::pipeline::network::tcp::server::TcpStreamServer::register()
                                                             (lib/runtime/src/pipeline/network/tcp/server.rs)
  → 得到 response_stream_provider
  → 等待 worker 回连并拿到 response_stream
  → 包装成 crate::pipeline::ResponseStream
```

### 10.4 业务侧看到的结果

业务侧最终看到的只是：

```rust
let stream = router.generate(request).await?;
while let Some(item) = stream.next().await {
    // 持续消费流式响应
}
```

但在内部，它已经跨过了：

- discovery
- routing
- request plane
- worker handler
- response plane

这也是为什么这条链路要拆成这么多层：每一层都只做自己的一件事。

---

## 11. discovery 在整条链路里的位置

### 11.1 功能说明

discovery 不是传输层，而是**实例目录服务**。

它回答的问题是：

- 当前某个 `namespace/servicegroup/endpoint` 下有哪些实例？
- 每个实例对应的 transport 地址是什么？
- 实例上线、下线时，客户端如何及时感知？

### 11.2 关键接口

| 类型 / 函数 | 定义位置 | 作用 |
| --- | --- | --- |
| `crate::discovery::Discovery` | [lib/runtime/src/discovery/mod.rs#L754](../src/discovery/mod.rs#L754) | 发现层 trait |
| `crate::discovery::Discovery::register()` | [lib/runtime/src/discovery/mod.rs#L762](../src/discovery/mod.rs#L762) | 注册实例 |
| `crate::discovery::Discovery::unregister()` | [lib/runtime/src/discovery/mod.rs#L823](../src/discovery/mod.rs#L823) | 注销实例 |
| `crate::discovery::Discovery::list()` | [lib/runtime/src/discovery/mod.rs#L827](../src/discovery/mod.rs#L827) | 快照查询 |
| `crate::discovery::Discovery::list_and_watch()` | [lib/runtime/src/discovery/mod.rs#L831](../src/discovery/mod.rs#L831) | 持续订阅 |
| `crate::discovery::KVStoreDiscovery` | [lib/runtime/src/discovery/kv_store.rs](../src/discovery/kv_store.rs) | etcd / file / mem discovery |
| `crate::discovery::KubeDiscoveryClient` | [lib/runtime/src/discovery/kube.rs](../src/discovery/kube.rs) | Kubernetes discovery |

### 11.3 在服务端和客户端中的作用差异

#### 服务端视角

- `EndpointConfigBuilder::start()` 负责调用 `Discovery::register()`
- endpoint 下线或清理时，调用 `Discovery::unregister()`

#### 客户端视角

- `component::client::Client` 调用 `Discovery::list_and_watch()`
- 后续路由只读 `Client` 本地实例缓存，不直接读 discovery 后端

这使得 discovery 成为了整条链路的“共享目录”，而不是“请求转发器”。

---

## 12. 把“功能说明”和“调用代码”对应起来看

各阶段概览对照表：

| 阶段 | 功能说明 | 关键调用代码 |
| --- | --- | --- |
| 启动 | 让进程拥有分布式运行能力 | `Worker::execute()` → `DistributedRuntime::new()` |
| 命名 | 确定 endpoint 身份 | `DistributedRuntime::namespace()` → `Namespace::component()` → `ServiceGroup::endpoint()` |
| 服务端注册 | 把 endpoint 真正上线 | `EndpointConfigBuilder::start()` |
| 请求平面注册 | 让 worker 可以收请求 | `NetworkManager::server()` → `SharedTcpServer::register_endpoint()` |
| discovery 注册 | 让别人知道这个实例存在 | `Discovery::register()` |
| 客户端订阅 | 持续感知实例变化 | `PortName::client()` → `Client::new()` → `Discovery::list_and_watch()` |
| 选路 | 选一个实例来请求 | `PushRouter::round_robin()` / `random()` / `direct()` |
| 发送请求 | 发到目标实例 | `AddressedPushRouter::generate()` → `TcpRequestClient::send_request()` |
| 接收响应 | 收到流式返回 | `TcpStreamServer::register()` → `ResponseStream` |

---

## 13. 实际调用顺序

推荐源码顺序：

1. [lib/runtime/src/worker.rs#L49](../src/worker.rs#L49)
2. [lib/runtime/src/distributed.rs#L44](../src/distributed.rs#L44)
3. [lib/runtime/src/servicegroup.rs#L144](../src/servicegroup.rs#L144)
4. [lib/runtime/src/servicegroup/portname.rs#L69](../src/servicegroup/portname.rs#L69)
5. [lib/runtime/src/pipeline/network/manager.rs#L201](../src/pipeline/network/manager.rs#L201)
6. [lib/runtime/src/pipeline/network/ingress/shared_tcp_portname.rs#L73](../src/pipeline/network/ingress/shared_tcp_portname.rs#L73)
7. [lib/runtime/src/discovery/mod.rs#L754](../src/discovery/mod.rs#L754)
8. [lib/runtime/src/servicegroup/client.rs#L97](../src/servicegroup/client.rs#L97)
9. [lib/runtime/src/pipeline/network/egress/push_router.rs#L97](../src/pipeline/network/egress/push_router.rs#L97)
10. [lib/runtime/src/pipeline/network/egress/addressed_router.rs#L128](../src/pipeline/network/egress/addressed_router.rs#L128)
11. [lib/runtime/src/pipeline/network/egress/tcp_client.rs#L431](../src/pipeline/network/egress/tcp_client.rs#L431)
12. [lib/runtime/src/pipeline/network/tcp/server.rs#L72](../src/pipeline/network/tcp/server.rs#L72)

---

## 14. 总结

请求流水线可以概括如下：

> `Worker` 和 `DistributedRuntime` 负责把进程搭起来，`EndpointConfigBuilder::start()` 负责把服务注册出去，`Client` + `PushRouter` 负责发现和选路，`AddressedPushRouter` + 请求平面负责把请求送到 worker，`TcpStreamServer` 负责把流式响应带回来。

