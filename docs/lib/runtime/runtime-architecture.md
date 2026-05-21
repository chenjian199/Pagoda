# pagoda-runtime 架构文档

> **对应代码**：`lib/runtime/`（crate `pagoda-runtime`）

---

## 1. 系统定位

`pagoda-runtime` 是 Pagoda 推理系统的**核心基础设施层**，为上层业务组件提供：

- 本地与分布式异步执行上下文（Tokio Runtime + Rayon ComputePool）
- 跨节点服务注册与发现（etcd / Kubernetes / file / mem）
- 统一请求平面传输抽象（TCP / NATS / HTTP）
- 可组合的流式引擎框架（AsyncEngine / Pipeline）
- 可观测性接入点（Prometheus 指标树、W3C 链路追踪、健康检查）
- 进程生命周期管理（信号处理、三阶段优雅关闭）

---

## 2. 整体分层架构

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              业务层（上层 crate）                                 │
│          pagoda-llm  ·  pagoda-kv-router  ·  pagoda-mocker  ·  用户 workers     │
└──────────────────────────────────────┬──────────────────────────────────────────┘
                                       │ pub API
┌──────────────────────────────────────▼──────────────────────────────────────────┐
│  ENTRY LAYER — 进程入口层                                                        │
│                                                                                 │
│  Worker ──► RuntimeConfig ──► create_runtime() ──► Runtime                      │
│    └── execute(app_fn)                                                          │
│          ├── secondary: signal_handler + graceful shutdown watchdog             │
│          └── primary:   app_fn(runtime).await  ←── 用户代码从这里开始              │
└──────────────────────────────────────┬──────────────────────────────────────────┘
                                       │ app_fn 内构造
┌──────────────────────────────────────▼──────────────────────────────────────────┐
│  RUNTIME LAYER — 本机运行时层                                                     │
│                                                                                 │
│  Runtime                                                                        │
│  ├── primary:   Tokio multi-thread handle  ── 业务 I/O 任务                      │
│  ├── secondary: Tokio multi-thread handle  ── 框架控制任务                        │
│  ├── cancellation_token                    ── 全局根令牌                          │
│  ├── endpoint_shutdown_token               ── 端点关闭子令牌                       │
│  ├── graceful_shutdown_tracker             ── 等待 in-flight 请求完成             │
│  └── compute_pool: Option<ComputePool>     ── Rayon CPU 计算池                   │
└──────────────────────────────────────┬──────────────────────────────────────────┘
                                       │ from_settings(runtime)
┌──────────────────────────────────────▼──────────────────────────────────────────┐
│  DISTRIBUTED LAYER — 分布式运行时层                                               │
│                                                                                 │
│  DistributedRuntime                                                             │
│  ├── runtime: Runtime                                                           │
│  ├── discovery_client: Arc<dyn Discovery>   ── etcd / K8s / file / mem          │
│  ├── network_manager: NetworkManager        ── 请求平面 server 统一入口            │
│  ├── nats_client: Option<NatsClient>        ── NATS 连接（可选）                  │
│  ├── system_health: SystemHealth            ── 健康状态聚合                       │
│  ├── metrics_registry: MetricsRegistry      ── Prometheus 指标根节点              │
│  ├── local_portname_registry               ── 进程内直连表（绕过网络）              │
│  └── engine_routes: EngineRoutes           ── /engine/* HTTP 路由回调            │
└──────────────────────────────────────┬──────────────────────────────────────────┘
                                       │ .namespace().service_group().portname()
┌──────────────────────────────────────▼──────────────────────────────────────────┐
│  SERVICE MODEL LAYER — 服务模型层                                                 │
│                                                                                 │
│  Namespace ──► ServiceGroup ──► PortName                                           │
│                                  ├── .client().await                            │
│                                  │       └── Client (订阅发现 + PushRouter)       │
│                                  └── .portname_builder().handler(engine).start()│
│                                          └── Discovery.register()               │
│                                          └── RequestPlaneServer.register()      │
└──────────────────────────────────────┬──────────────────────────────────────────┘
                                       │
┌──────────────────────────────────────▼──────────────────────────────────────────┐
│  PIPELINE / ENGINE LAYER — 引擎与管道层                                           │
│                                                                                 │
│  AsyncEngine<Req, Resp, E>  (核心 trait)                                         │
│    async fn generate(&self, Req) -> Result<Resp, E>                              │
│                                                                                  │
│  ┌─────────────────────────────┐    ┌──────────────────────────────────────────┐ │
│  │  Client (客户端路径)          │    │  Ingress (服务端路径)                     │ │
│  │                             │    │                                          │ │
│  │  PushRouter                 │    │  RequestPlaneServer                      │ │
│  │  ├── RoundRobin             │    │  ├── TcpStreamServer                     │ │
│  │  ├── Random                 │    │  ├── NatsServer                          │ │
│  │  ├── PowerOfTwoChoices      │    │  └── HttpServer                          │ │
│  │  ├── KV (感知前缀)           │    │          │                               │ │
│  │  ├── Direct                 │    │          │ decode → PushWorkHandler      │ │
│  │  └── LeastLoaded            │    │          └── engine.generate()           │ │
│  │          │                  │    │                    │                      │ │
│  │  AddressedPushRouter        │    │               encode response stream      │ │
│  │  └── RequestPlaneClient ───────────────────────────► TCP / NATS response    │ │
│  └─────────────────────────────┘    └──────────────────────────────────────────┘ │
│                                                                                   │
│  Pipeline Nodes                                                                   │
│  ├── sources/   SourceNode, NetworkSourceNode                                    │
│  └── sinks/     SinkNode, NetworkSinkNode, SegmentSink                          │
└──────────────────────────────────────┬──────────────────────────────────────────┘
                                       │
┌──────────────────────────────────────▼──────────────────────────────────────────┐
│  TRANSPORT / STORAGE LAYER — 传输与存储层                                         │
│                                                                                 │
│  Request Plane (请求面)          Event Plane (事件面)    KV Store (KV 存储)        │
│  ├── TCP (default)               ├── ZMQ (default)       ├── etcd (default)     │
│  ├── NATS (deprecated)           └── NATS JetStream      ├── file (本地开发)      │
│  └── HTTP                                                 ├── mem (单进程测试)    │
│                                                           └── NATS JetStream    |
│                                                                                 │
│  Discovery Backend (发现后端)                                                     │
│  ├── KvStoreDiscovery   ── etcd prefix watch + lease                             │
│  └── KubeDiscoveryClient── K8s API + CRD watch                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────────────────────┐
│  CROSS-CUTTING CONCERNS — 横切关注点                                              │
│                                                                                  
│  logging     W3C TraceParent 注入/提取，OTLP 导出，日志初始化                        │
│  metrics     MetricsHierarchy 树（DRT → Namespace → ServiceGroup → PortName）       │
│  compute     Rayon 计算池，tokio-rayon 桥接，thread-local 初始化                    │
│  utils       GracefulShutdownTracker · TaskTracker · Pool · Stream 工具          │
│  protocols   Annotated<T>（流式 token）· MaybeError<T,E>（流中错误传播）             │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. 模块层级结构

```
src/
│
├── lib.rs          pub 门面。re-export 全部公开 API：Worker / Runtime / DistributedRuntime /
│                   RuntimeConfig / CancellationToken / logging / pipeline::* / protocols::* /
│                   stream / MetricsRegistry / SystemHealth / HealthCheckTarget
│
├── prelude.rs      常用类型别名集合，供 use pagoda_runtime::prelude::* 一次性引入
│
│
├── ── ENTRY  进程入口层 ────────────────────────────────────────────────────────────
│
├── worker.rs       Worker struct。
│                   • 职责：从配置构建 Tokio Runtime，驱动用户 app_fn 到完成，管理整个进程生命周期。
│                   • 进程唯一性：通过三个 static OnceCell（RT / RTHANDLE / INIT）保证进程内只存在
│                     一个 Tokio Runtime，重复调用 from_config 返回 Err。
│                   • execute(f)：main 线程在 secondary.block_on 处阻塞；secondary pool 运行
│                     signal_handler 与 graceful shutdown watchdog；primary pool 运行用户 app_fn。
│                   • 优雅关闭：等待 app_fn 完成或 SIGINT/SIGTERM 触发；超时（PGD_WORKER_GRACEFUL_
│                     SHUTDOWN_TIMEOUT，debug=5s / release=30s）后 std::process::exit(911)。
│                   • execute_async：供已在异步上下文中使用（Python bindings）。
│                   • from_current：复用调用方已有的 Tokio runtime（嵌入式场景）。
│
├── runtime.rs      Runtime struct（Clone = Arc 引用计数增量）。
│                   • 职责：封装 Tokio 运行时 handle，提供统一的 primary / secondary 线程池访问、
│                     取消令牌树和计算池访问接口。
│                   • RuntimeType 枚举：Shared（Worker 自建，ManuallyDrop 避免 async drop panic）
│                     / External（借用外部 Handle，供 Python PyO3 绑定使用）。
│                   • 双线程池：primary 跑业务 I/O 任务；secondary 跑框架控制任务（信号、watchdog）。
│                     二者当前共用同一 Tokio multi-thread pool，但 API 已隔离，未来可独立配置。
│                   • CancellationToken 树：根令牌 cancellation_token → 子令牌
│                     endpoint_shutdown_token。Phase 1 取消子令牌（停止接受新请求），Phase 2 等待
│                     in-flight 完成，Phase 3 取消根令牌（断开 NATS/etcd 等后端连接）。
│                   • compute_pool：可选 Rayon 池，通过 initialize_all_thread_locals() 在每个
│                     Tokio worker 线程上预热 thread-local 引用，避免热路径上的锁竞争。
│
├── config.rs       RuntimeConfig struct（figment Builder + Validate）。
│                   • 职责：通过 figment 框架按优先级合并环境变量 > TOML 配置文件 > 代码默认值，
│                     提供强类型配置结构。
│                   • 关键字段：num_worker_threads / max_blocking_threads / system_port /
│                     starting_health_status / compute_threads / health_check_enabled /
│                     canary_wait_time_secs。
│                   • create_runtime()：使用 tokio::runtime::Builder::new_multi_thread()
│                     按配置创建 Tokio Runtime，支持 PGD_ENABLE_POLL_HISTOGRAM 开关。
│   └── config/
│       └── environment_names.rs
│                   所有 PGD_* 环境变量名字符串常量，集中管理避免散落在代码中。
│                   分组：runtime（PGD_RUNTIME_*）/ worker（PGD_WORKER_*）/ system（PGD_SYSTEM_*）
│
│
├── ── DISTRIBUTED  分布式运行时层 ─────────────────────────────────────────────────
│
├── distributed.rs  DistributedRuntime struct（Clone = Arc 引用计数增量，进程内可自由共享）。
│                   • 职责：在 Runtime 之上叠加集群感知能力，是业务组件直接持有的上下文对象。
│                     聚合 Discovery / NetworkManager / NATS / SystemHealth / Metrics 等所有
│                     分布式基础设施，通过 .namespace() 入口对外暴露服务模型 API。
│                   • 初始化顺序（new() 内，顺序不可颠倒）：
│                     1. 连接 NATS（如配置）
│                     2. 读取 RuntimeConfig（system_port 等）
│                     3. 初始化 Discovery（KvStore 或 K8s）
│                     4. NetworkManager::new()（绑定请求平面 server）
│                     5. 启动 SystemStatusServer（system_port >= 0 时）
│                     6. 启动 HealthCheckManager（health_check_enabled 时）
│                   • DistributedConfig：携带 DiscoveryBackend / NatsConfig / RequestPlaneMode，
│                     通过 from_settings() 从 PGD_DISCOVERY_BACKEND / PGD_REQUEST_PLANE 读取。
│                   • RequestPlaneMode：Tcp（默认，低延迟）/ Http（标准化）/ Nats（deprecated）。
│                   • DiscoveryBackend：KvStore("etcd://..." | "file://..." | "mem://") / Kubernetes。
│                   • NATS 操作代理：kv_router_nats_publish / subscribe / request /
│                     register_nats_service，集中封装 NATS client 调用。
│
├── traits.rs       两个基础 Provider trait：
│                   • RuntimeProvider：fn rt(&self) -> &Runtime
│                     — 供 ServiceGroup / PortName / Namespace 等类型暴露底层 Runtime 访问。
│                   • DistributedRuntimeProvider：fn drt(&self) -> &DistributedRuntime
│                     — 供 ServiceGroup / PortName 等暴露 DistributedRuntime 访问。
│                   二者均由 ServiceGroup / PortName / Namespace 实现，使通用工具函数可接受任意
│                   持有运行时引用的类型，而不必泛型写死具体结构。
│
│
├── ── SERVICE MODEL  服务模型层 ──────────────────────────────────────────────────
│
├── servicegroup.rs (门面，re-export servicegroup/*)    门面模块，re-export servicegroup/* 的所有公开类型。
│   └── servicegroup/
│       ├── namespace.rs
│                   Namespace struct（Builder + Clone）。
│                   • 职责：服务寻址的顶层命名空间，对应 etcd key 前缀 "/{name}/"。
│                   • servicegroup_cache：Arc<DashMap<String, ServiceGroup>>，同名 ServiceGroup 只创建一次，
│                     保证 MetricsRegistry 不重复注册，同时减少 NATS 服务注册次数。
│                   • 支持子命名空间：.namespace(child_name) 创建 parent.child 层级。
│                   • 实现 DistributedRuntimeProvider / RuntimeProvider / MetricsHierarchy。
│
│       ├── servicegroup.rs (门面，re-export servicegroup/*)
│                   ServiceGroup struct（Builder + Clone）。
│                   • 职责：可独立部署的服务单元，拥有多个 PortName，对应 etcd 前缀
│                     "/{namespace}/{servicegroup}/"。
│                   • service_name()：返回 "namespace.servicegroup" 用于 NATS service 注册。
│                   • endpoint(name) → PortName：工厂方法，创建 PortName 并挂载指标子树。
│                   • list_instances()：通过 discovery.list() 查询当前所有活跃实例。
│                   • build() 时若请求面为 NATS，自动在 ServiceGroupRegistry 中注册 NATS micro service，
│                     防止 serve_endpoint 时 service 尚未注册的竞态。
│
│       ├── portname.rs
│                   PortName struct + PortNameConfig + PortNameConfigBuilder。
│                   • PortName：轻量描述符（持有 ServiceGroup 引用 + endpoint name），不含网络状态。
│                     id() 返回三元组 PortNameId { namespace, component, name }。
│                   • client().await：异步构建 Client，订阅发现层并初始化路由器。
│                   • portname_builder()：返回 PortNameConfigBuilder，供服务端注册使用。
│                   • PortNameConfig：服务端配置，含 handler（PushWorkHandler）、
│                     health_check_payload、graceful_shutdown 标志。
│                   • PortNameConfigBuilder::start()：
│                     1. build_transport_type() — 按请求面模式生成 Instance 地址
│                     2. discovery.register(instance) — 写入 etcd/K8s
│                     3. request_plane_server.register_portname(handler) — 绑定路由
│                     4. local_portname_registry.register() — 进程内直连注册
│                     5. 启动健康检查（如配置）
│
│       ├── client.rs
│                   Client struct + RoutingOccupancyState。
│                   • 职责：对远端 PortName 的调用抽象，自动负载均衡至健康实例。
│                   • 实例列表维护：
│                     instance_avail / instance_free 均为 Arc<ArcSwap<Vec<u64>>>，
│                     读路径完全无锁（ArcSwap::load），写路径 CAS（ArcSwap::rcu）。
│                   • reconcile_loop：消费 DiscoveryStream，实时更新实例列表，
│                     通过 watch::Sender 通知 wait_for_instances() 等待方。
│                   • report_instance_down(id)：故障检测触发，将实例从 avail 列表移除。
│                   • update_free_instances(ids)：供 KV 路由器等外部策略更新空闲实例列表。
│                   • RoutingOccupancyState：DashMap<instance_id, AtomicU64> 追踪各实例
│                     in-flight 请求数，供 PowerOfTwo / LeastLoaded 路由使用。
│
│       ├── registry.rs
│                   ServiceGroupRegistry（进程内 NATS service 注册表）。
│                   • 职责：为 NATS 请求面模式维护 ServiceGroup 级的 micro service 注册，
│                     合并同一 ServiceGroup 内多个 PortName 到同一 NATS service，减少服务数量。
│                   • 内部用 HashMap<service_name, Service>，由 parking_lot::Mutex 保护。
│
│       └── service.rs
│                   NATS micro service 封装，将 async-nats Service API 适配为
│                   pagoda-runtime 的内部接口。
│
├── instances.rs    InstanceSource。
│                   • 职责：维护某个 PortName 下所有活跃实例的规范化列表，作为 Client
│                     的数据源。一个 PortName 对应唯一一个 InstanceSource（通过
│                     DRT.instance_sources DashMap 缓存），避免多个 Client 对同一 PortName
│                     发起多份 etcd watch，减少后端压力。
│                   • 内部持有 DiscoveryStream，spawn reconcile task 维护
│                     Arc<ArcSwap<Vec<Instance>>> 实例快照。
│
├── local_portname_registry.rs
│                   LocalPortNameRegistry（进程内 engine 直连表）。
│                   • 职责：将 AsyncEngine 注册为可本地寻址的端点，供同进程内的调用方
│                     绕过 TCP/NATS 直接调用，适用于前端与 worker 合并部署的单进程模式。
│                   • 内部：DashMap<PortNameId, Arc<dyn AnyAsyncEngine>>。
│                   • 通过类型擦除（AnyAsyncEngine）存储任意泛型参数的 engine，
│                     取出时通过 DowncastAnyAsyncEngine::downcast() 恢复具体类型。
│
│
├── ── ENGINE / PIPELINE  引擎与管道层 ─────────────────────────────────────────────
│
├── engine.rs       核心引擎 trait 体系。
│                   • AsyncEngine<Req, Resp, E>：所有推理/处理逻辑的统一接口，
│                     async fn generate(&self, Req) -> Result<Resp, E>。
│                     被 Pipeline ingress、PushRouter、LocalPortNameRegistry 统一消费。
│                   • Data trait：blanket impl，所有 Send + Sync + 'static 类型自动满足，
│                     是 Req / Resp 的类型约束。
│                   • AsyncEngineContext：请求级控制接口，持有 id / is_stopped / is_killed /
│                     stop_generating / stop / kill / link_child。link_child 构建父子取消链，
│                     父 cancel 自动传播到所有子 context。
│                   • AsyncEngineContextProvider：fn context() -> Arc<dyn AsyncEngineContext>，
│                     由 SingleIn<T>（即 EngineUnary<T>）实现，让 engine 从请求中取出 context。
│                   • 类型擦除三件套：AnyAsyncEngine（存储）/ AsAnyAsyncEngine（转换）/
│                     DowncastAnyAsyncEngine（恢复）— 支持将不同泛型的 engine 存入同一 HashMap。
│                   • ResponseStream<R>：流式响应的标准包装，impl Stream<Item = R>。
│                   • 类型别名：SingleIn<T> / ManyIn<T> / SingleOut<U> / ManyOut<U> /
│                     Context / ServiceEngine<T,U> / UnaryEngine<T,U> 等，简化业务代码签名。
│
├── engine_routes.rs
│                   EngineRouteRegistry。
│                   • 职责：维护 "/engine/*" HTTP 路由回调表，允许引擎向 SystemStatusServer
│                     注册自定义 HTTP 端点（如暴露引擎内部状态、调试接口）。
│                   • 内部：DashMap<path, BoxedHandler>，在 Axum router 构建时注入。
│
├── pipeline.rs     门面，re-export pipeline/* 所有公开类型。
│   └── pipeline/
│       ├── context.rs
│                   AsyncEngineContextProvider trait 及 IntoContext 转换辅助 trait。
│                   定义请求如何从业务数据类型转换为带 context 的 SingleIn<T>。
│
│       ├── error.rs
│                   PipelineError（全局错误枚举）+ PipelineErrorExt（错误链扩展）。
│                   区分 transport 错误 / engine 错误 / 取消 / 超时等，
│                   供 egress/ingress 路径统一处理和上报。
│
│       ├── registry.rs
│                   PipelineRegistry：进程内流水线实例注册表，
│                   用于多流水线场景下的命名查找与生命周期管理。
│
│       ├── nodes.rs  门面，re-export nodes/*
│       │   └── nodes/
│       │       ├── sources/
│       │       │           SourceNode：从网络或本地读取请求，产生 SingleIn<T> 流。
│       │       │           NetworkSourceNode：从 ingress 接收字节流并反序列化为 T。
│       │       └── sinks/
│       │                   SinkNode：消费 ManyOut<U> 流，写回网络或下游流水线。
│       │                   NetworkSinkNode：序列化后通过 egress 发回客户端。
│       │                   SegmentSink：将响应写入同进程内另一流水线的输入端。
│       │
│       └── network.rs  门面，re-export network/*
│           └── network/
│               ├── manager.rs
│               │           NetworkManager。
│               │           • 职责：统一管理所有网络端点的生命周期，根据 RequestPlaneMode
│               │             创建对应的 server（TcpStreamServer / NatsServer / HttpServer）
│               │             和 client（TcpRequestClient / NatsRequestClient / HttpClient）。
│               │           • 作为 DistributedRuntime 的组成部分，在 DRT::new() 时初始化。
│               │           • 通过 request_plane_server() / request_plane_client() 暴露统一
│               │             接口，上层代码无需感知底层传输协议。
│               │
│               ├── codec/
│               │   ├── two_part.rs
│               │   │           TwoPartCodec：pagoda 私有帧格式编解码器。
│               │   │           格式：[varint: ctrl_len][ctrl_bytes][data_bytes]
│               │   │           ctrl_bytes = msgpack(RequestControlMessage)，含请求 ID、
│               │   │           响应回拨地址（Call-Home 模式）、时间戳等控制字段。
│               │   │           data_bytes = msgpack(T)，业务请求体。
│               │   └── zero_copy_decoder.rs
│               │               ZeroCopyTcpDecoder：TCP 流的零拷贝帧解析器，
│               │               直接在 DMA 缓冲区上操作，避免业务负载的内存拷贝。
│               │
│               ├── tcp/
│               │   ├── server.rs
│               │   │           TcpStreamServer / SharedTcpServer。
│               │   │           • SharedTcpServer：进程级单例（OnceCell），所有 Worker 的
│               │   │             PortName 注册到同一 port，通过 DashMap<endpoint_path, handler>
│               │   │             路由区分（见 ADR-003）。
│               │   │           • accept_loop：永久运行，每个连接 spawn handle_connection task。
│               │   │           • handle_connection：每连接 2 个 task（read_loop + write_loop），
│               │   │             read_loop 解帧 → 路由 → 入队，write_loop 串行写 ACK。
│               │   │           • worker_dispatcher：全局唯一 task，从 mpsc::Receiver 消费
│               │   │             WorkItem，Semaphore 控制并发上限（PGD_TCP_WORKER_POOL_SIZE）。
│               │   │
│               │   ├── client.rs
│               │   │           TcpClient + ConnectionPool。
│               │   │           • 每个 host:port 维护独立连接池（最大 PGD_TCP_POOL_SIZE 条）。
│               │   │           • Call-Home 模式：发请求时附带本地 TcpStreamServer 地址，
│               │   │             Worker 收到后反向连接，流式回传响应（双连接模式）。
│               │   └── test_utils.rs  测试辅助：快速创建本地 TCP server/client 对。
│               │
│               ├── ingress/   服务端接入，将网络请求路由到 AsyncEngine。
│               │   ├── unified_server.rs
│               │   │           RequestPlaneServer trait（服务端抽象接口）：
│               │   │           register_portname(path, handler) / unregister_portname /
│               │   │           address() / transport_name() / is_healthy()
│               │   │
│               │   ├── push_handler.rs
│               │   │           PushWorkHandler trait（请求处理器抽象）：
│               │   │           handle_payload(bytes, headers) — 解码并调用 engine.generate()；
│               │   │           add_metrics(labels) — 注入指标标签；
│               │   │           set_endpoint_health_check_notifier — 注册健康检查通知器。
│               │   │           Ingress::for_engine(engine) 会自动生成实现此 trait 的处理器。
│               │   │
│               │   ├── push_portname.rs   PushEndpoint：将 handler 绑定到具体传输端点。
│               │   ├── shared_tcp_portname.rs  TCP 端点的进程内共享封装。
│               │   ├── nats_server.rs     NatsServer：NATS queue subscribe 实现。
│               │   └── http_portname.rs   HttpEndpoint：Axum route handler 实现。
│               │
│               └── egress/   客户端出口，将请求发往目标 Worker。
│                   ├── unified_client.rs
│                   │           RequestPlaneClient trait（客户端抽象接口）：
│                   │           send_request(target, request_bytes, resp_server) → DataStream<Bytes>
│                   │
│                   ├── push_router.rs
│                   │           PushRouter<T, U> + RouterMode + WorkerLoadMonitor。
│                   │           • 实现 AsyncEngine<SingleIn<T>, ManyOut<U>, Error>，
│                   │             是 Client 的核心，将 generate() 调用路由到具体实例。
│                   │           • RouterMode 六种策略：
│                   │             RoundRobin — counter % len，适合同质 worker
│                   │             Random      — rng.choose，无共享状态，最低开销
│                   │             PowerOfTwo  — 随机取2个实例，选 in-flight 较少者
│                   │             LeastLoaded — 全局最低 in-flight，Mutex 保证原子选取
│                   │             Direct      — 从请求 headers 中读取指定 instance_id
│                   │             KV          — 委托给 WorkerLoadMonitor（KV 前缀感知路由）
│                   │           • WorkerLoadMonitor trait：供 KV 路由器注入外部实例评分逻辑。
│                   │           • fault_detection：请求失败后 report_instance_down，
│                   │             从 avail 列表移除该实例，等待 discovery 重新发现。
│                   │
│                   ├── addressed_router.rs
│                   │           AddressedPushRouter + AddressedRequest<T>。
│                   │           • 职责：在已知 instance_id 的情况下，直接调用
│                   │             RequestPlaneClient 发送请求，处理 Call-Home 响应回传。
│                   │           • AddressedRequest<T>：{ request: T, address: u64(instance_id) }
│                   │
│                   ├── tcp_client.rs   TcpRequestClient impl RequestPlaneClient。
│                   ├── nats_client.rs  NatsRequestClient impl RequestPlaneClient。
│                   └── http_router.rs  HttpRequestClient impl RequestPlaneClient。
│
│
├── ── DISCOVERY  服务发现层 ──────────────────────────────────────────────────────
│
├── discovery/
│   ├── mod.rs      Discovery trait + 所有公共类型。
│                   • Discovery trait（核心抽象）：
│                     instance_id() — 本实例唯一 ID（xxhash 生成）
│                     register(spec) — 写入发现层（etcd PUT with lease / K8s CRD create）
│                     unregister(id) — 删除注册（etcd DELETE / K8s CRD delete）
│                     list(query) — 查询当前活跃实例列表
│                     list_and_watch(query) — 返回 DiscoveryStream（初始 list + 持续 watch 事件）
│                   • DiscoveryQuery：ComponentEndpoints / NamespaceComponents，
│                     控制 watch 的 etcd prefix 范围。
│                   • DiscoveryEvent：Added(DiscoveryInstance) / Removed(DiscoveryInstanceId)，
│                     由 reconcile_loop 消费，驱动 Client 的实例列表更新。
│                   • DiscoverySpec：register 的入参，携带 Instance 的完整描述（含 transport 地址）。
│
│   ├── kv_store.rs KVStoreDiscovery。
│                   • 职责：基于 KV 存储（etcd/file/mem/NATS KV）实现 Discovery trait。
│                   • register：PUT key="/ns/comp/ep/{uuid}" value=JSON{transport, instance_id}
│                     附带 etcd lease（TTL），进程存活期间 keep_alive() 续约。
│                   • list_and_watch：etcd.watch(prefix) → 转换为 DiscoveryStream。
│                   • unregister：DELETE key + revoke lease。
│                   • 通过 kv::Manager（storage/kv.rs）抽象底层存储，无需感知 etcd/file/mem 差异。
│
│   ├── metadata.rs DiscoveryMetadata（K8s 模式专用）。
│                   Arc<RwLock<>> 保护的元数据缓存，存储从 K8s API 读取的 Pod / Node 信息，
│                   供 /metadata HTTP 端点暴露给运维工具。
│
│   ├── kube.rs     KubeDiscoveryClient。
│                   • 职责：通过 Kubernetes API + DynamoEndpoint CRD 实现 Discovery trait。
│                   • 创建时连接 K8s API server，启动 daemon task 持续 watch CRD 变更。
│                   • CRD spec 包含 transport 地址，通过 CRD annotation 传递 instance_id 等元数据。
│   │   └── kube/
│   │       ├── crd.rs      DynamoEndpoint CRD 的 Rust 类型定义（k8s-openapi 生成）。
│   │       ├── daemon.rs   K8s informer daemon：监听 CRD Added/Modified/Deleted 事件，
│   │       │               维护进程内实例快照，触发 DiscoveryEvent 发布。
│   │       └── utils.rs    K8s 工具函数：namespace 解析、label selector 构建等。
│
│   ├── mock.rs     MockDiscovery（测试用）。
│                   纯内存实现，register/unregister 直接操作 DashMap，
│                   list_and_watch 通过 broadcast channel 推送事件。
│                   支持 inject_event() 手动触发特定 DiscoveryEvent，便于测试路由逻辑。
│
│   └── utils.rs    etcd key 编解码工具。
│                   format_key(ns, comp, ep, uuid) → "/ns/comp/ep/uuid"
│                   parse_key(key) → (ns, comp, ep, uuid)，用于 watch 事件解析。
│
│
├── ── STORAGE  KV 存储抽象层 ──────────────────────────────────────────────────────
│
├── storage.rs      门面，re-export storage/kv。
│   └── storage/
│       └── kv.rs   Store trait + Bucket trait + Manager。
│                   • Store trait：KV 存储的统一抽象：
│                     get_or_create_bucket(name) → Bucket
│                     get_bucket(name) → Option<Bucket>
│                     connection_id() → u64（用于调试与链路追踪）
│                     shutdown()
│                   • Bucket trait：具体 KV 桶操作：
│                     get(key) / put(key, value) / delete(key) / list(prefix) /
│                     watch(key) → Stream<WatchEvent> / put_with_lease(key, value, lease_id)
│                   • Versioned<T>：带版本号的值封装，用于 CAS 操作。
│                   • Manager：根据 Selector 字符串（"etcd://..." / "file://..." / "mem://"）
│                     创建对应后端实例，是 KVStoreDiscovery 的依赖。
│           └── kv/
│               ├── etcd.rs     EtcdKv impl Store + Bucket。
│                               基于 etcd-client，支持 lease、watch、事务操作。
│                               生产环境首选，提供强一致性和 TTL 租约机制。
│               ├── file.rs     FileKv impl Store + Bucket。
│                               基于本地文件系统，使用 notify crate watch 文件变更。
│                               适用于本地开发和 CI 测试，无需外部依赖。
│               ├── mem.rs      MemKv impl Store + Bucket。
│                               纯内存实现，使用 DashMap + broadcast channel。
│                               适用于单进程集成测试，启动最快，无副作用。
│               └── nats.rs     NatsKv impl Store + Bucket。
│                               基于 NATS JetStream KV API，适用于纯 NATS 部署场景。
│
│
├── ── TRANSPORTS  传输协议层 ──────────────────────────────────────────────────────
│
├── transports.rs   门面，re-export transports/*。
│   └── transports/
│       ├── etcd.rs     etcd 客户端封装（对外暴露 pagoda 友好的 API）。
│                       • Client：包装 etcd-client::Client，提供重试与超时配置。
│                       • PrefixWatcher：订阅 etcd prefix，返回 Stream<WatchEvent>，
│                         供 KVStoreDiscovery 和 TypedPrefixWatcher 使用。
│                       • KvCache：带本地缓存的 KV 读取，减少 etcd 读压力。
│                       • DistributedRWLock：基于 etcd 事务的分布式读写锁。
│                       • Lease：etcd lease 封装，keep_alive() 在后台 spawn task 自动续约，
│                         drop 时自动 revoke，RAII 保证注册条目随进程消亡。
│       │   └── etcd/
│       │       ├── connector.rs    连接建立、TLS 配置、重试策略。
│       │       ├── lease.rs        Lease struct + keep_alive background task。
│       │       ├── lock.rs         DistributedRWLock 实现（基于 etcd compare-and-swap）。
│       │       └── kv.rs           etcd KV 操作的 pagoda 封装，处理序列化/反序列化。
│       │
│       ├── nats.rs     NATS 客户端封装。
│                       • Client：包装 async-nats::Client，提供 publish / subscribe /
│                         request / jetstream 访问。
│                       • ClientOptions：连接配置（URL / NatsAuth / TLS / 重连策略）。
│                       • NatsAuth：None / Token / UserPassword，对应 NATS 认证方式。
│                       • NatsQueue：queue group 名称封装，用于 NATS 请求面负载均衡。
│                       • url_to_bucket_and_key() / instance_subject()：NATS subject 格式化工具。
│
│       ├── tcp.rs      re-export pipeline::network::tcp::{client, server}，
│                       使 transports 模块提供统一的传输 API 入口。
│
│       ├── zmq.rs      ZMQ 传输（事件面备选）。
│                       • ZmqPublisher：PUB socket，async fn send(bytes)。
│                       • ZmqSubscriber：SUB/PULL socket，返回 Stream<Bytes>。
│                       用于 KV 事件从 vLLM 进程传输到 Pagoda 事件面的本地 IPC 场景。
│
│       ├── utils.rs    传输层共用工具：地址解析、socket 选项（TCP_QUICKACK / SO_BUSY_POLL）等。
│
│       └── event_plane/   事件面传输抽象（KV events / forward-pass metrics 等）。
│           ├── mod.rs      EventTransportTx / EventTransportRx trait：
│                           async fn send(Frame) / async fn recv() → Option<Frame>
│                           EventPublisher<T> / EventSubscriber<T>：
│                           泛型包装，自动序列化/反序列化业务类型 T。
│           ├── frame.rs    Frame { subject: String, payload: Bytes }，事件的基本传输单元。
│           ├── codec.rs    Codec：Frame 的 msgpack 编解码。
│           ├── transport.rs    基础类型与 trait 定义。
│           ├── nats_transport.rs   NatsTransport impl EventTransportTx/Rx，
│                               基于 NATS JetStream publish/subscribe，提供持久化和重放。
│           ├── zmq_transport.rs    ZmqTransport impl EventTransportTx/Rx，
│                               基于 ZMQ PUB/SUB，高吞吐低延迟，适合本地节点间通信。
│           └── dynamic_subscriber.rs
│                           DynamicSubscriber：运行时动态订阅/取消订阅 subject，
│                           供需要按需监听特定 worker 事件的路由器使用。
│
│
├── ── COMPUTE  CPU 计算隔离层 ──────────────────────────────────────────────────────
│
├── compute/
│   ├── mod.rs      ComputePool + ScopeExecutor trait + ComputePoolExt trait。
│                   • ComputePool：Rayon ThreadPool 的 pagoda 封装，与 Tokio 完全隔离。
│                     专为 >1ms 的 CPU 密集型同步计算设计（前缀哈希、张量算法等）。
│                   • spawn(f)：提交闭包到 Rayon pool，返回 tokio-rayon JoinHandle，
│                     可在 Tokio async 上下文中 .await。
│                   • ScopeExecutor：fork-join 并行接口，fn execute(f: FnOnce)，
│                     适合对 Rayon scope 的结构化并行。
│                   • ComputePoolExt：扩展方法，提供 scope_executor() 便利接口。
│   ├── pool.rs     ComputePool 内部：Rayon ThreadPool 的创建与配置（线程数 / 栈大小 / 线程命名）。
│   ├── thread_local.rs
│                   COMPUTE_CONTEXT：每个 Tokio worker 线程上的 thread-local，
│                   存储 Arc<ComputePool> 引用，热路径上无需全局 Arc clone。
│                   通过 Runtime::initialize_all_thread_locals() 在启动时预热。
│   ├── macros.rs   compute!() 宏：简化 ComputePool::spawn() 调用的语法糖。
│   ├── metrics.rs  ComputePool 性能指标：队列深度、任务执行时间分布等。
│   └── validation.rs
│                   compute-validation feature：在 debug 模式下验证 compute!() 宏参数，
│                   检测意外的跨线程数据捕获，防止在 compute 线程上执行 async 代码。
│
│
├── ── OBSERVABILITY  可观测性层 ────────────────────────────────────────────────────
│
├── logging.rs      分布式追踪与日志初始化。
│                   • init()：Once 保证进程内只初始化一次；根据配置选择 text/JSON 格式；
│                     OTEL_EXPORT_ENABLED 时注册 OTLP exporter（gRPC）到 tracing-subscriber。
│                   • TraceParent / DistributedTraceContext：W3C traceparent 标准解析与构造。
│                   • GenericHeaders trait：fn get(&self, key) → Option<&str>，
│                     由 HttpHeaders / NatsHeaders 实现，统一 HTTP header map 与 NATS header map
│                     的访问接口，使追踪上下文注入/提取代码不依赖具体传输类型。
│                   • make_request_span / make_handle_payload_span：
│                     在请求进入和处理时创建 tracing Span，自动从 headers 提取父 trace context。
│                   • log_message()：供 Python bindings 调用，将 Python 端日志注入 Rust tracing。
│
├── metrics.rs      Prometheus 指标树。
│                   • MetricsRegistry：包装 prometheus::Registry，支持子注册表树形挂载。
│                     add_child_registry(child) 构建层级；gather() 递归收集所有子树指标。
│                   • MetricsHierarchy trait：
│                     basename() — 当前层级名称（ServiceGroup 返回组件名，PortName 返回端点名）
│                     parent_hierarchies() — 父级列表（用于拼接全限定指标前缀）
│                     get_metrics_registry() — 返回本层注册表
│                     metrics() — 默认实现：返回拼接后的全限定名
│                   实现者：DistributedRuntime（根）/ Namespace / ServiceGroup / PortName（叶）。
│                   所有指标按层级自动打 label，无需手动维护 namespace 前缀。
│   └── metrics/
│       ├── frontend_perf.rs    前端请求性能指标（TTFT / TPOT / 队列等待时间）。
│       ├── tokio_perf.rs       Tokio 运行时指标（需 tokio-console feature）：
│                               任务轮询次数、调度延迟、线程利用率。
│       ├── transport_metrics.rs 传输层指标：字节吞吐、连接数、队列深度。
│       ├── request_plane.rs    请求平面指标：RPS、p50/p99 延迟、错误率。
│       ├── work_handler_perf.rs Handler 性能：generate() 耗时、并发数。
│       └── prometheus_names.rs  所有指标名字符串常量（统一管理，避免名称冲突）。
│
├── system_status_server.rs
│                   运维 HTTP 服务（基于 Axum）。
│                   • 职责：提供 K8s liveness/readiness probe、Prometheus scrape、
│                     自定义 engine 路由的统一 HTTP 入口。
│                   • 端点：GET /health（就绪检查）/ GET /live（存活检查）/
│                     GET /metrics（Prometheus text）/ GET /engine/*（自定义路由）。
│                   • 由 PGD_SYSTEM_PORT 控制（-1=禁用，0=OS 随机端口，>0=指定端口）。
│                   • start_system_status_server() 异步启动，返回 SystemStatusServerInfo
│                     含实际绑定地址，供服务注册时上报。
│
├── health_check.rs HealthCheckManager + HealthCheckTarget。
│                   • HealthCheckManager：每个注册了 health_check_payload 的 PortName 对应
│                     一个实例，定期发送 canary 请求（间隔 canary_wait_time_secs），
│                     根据响应更新 SystemHealth 中该 endpoint 的状态。
│                   • HealthCheckTarget：描述要检查的 endpoint 及预期响应内容。
│                   • 超时由 health_check_request_timeout_secs 控制。
│
├── system_health.rs SystemHealth + HealthStatus。
│                   • SystemHealth：聚合进程整体健康状态，由所有注册 endpoint 的
│                     health_check 结果决定（use_endpoint_health_status 配置项指定哪些
│                     endpoint 参与聚合）。
│                   • HealthStatus：Ready / NotReady，初始状态由 starting_health_status 配置。
│                   • 写入：HealthCheckManager 通过 set_endpoint_health() 更新单个 endpoint 状态。
│                   • 读取：SystemStatusServer 的 /health 端点调用 is_healthy() 决定返回 200/503。
│
│
├── ── PROTOCOLS  协议辅助类型 ─────────────────────────────────────────────────────
│
├── protocols.rs    门面，re-export protocols/*。
│   └── protocols/
│       ├── annotated.rs
│                   Annotated<T>：带注解的流式响应 token 包装。
│                   • 字段：data: Option<T> / event: Option<String> / id / comment。
│                   • 用于在 ManyOut<Annotated<T>> 流中携带 SSE 事件类型、序列 ID 等元数据，
│                     同时标记流结束（is_final() 检查 data.is_none() && event == "done"）。
│                   • 典型用法：AsyncEngine 的 Resp 类型为 Annotated<String>，
│                     每个 token 为 Annotated::from_data(token_str)，
│                     流末尾为 Annotated { data: None, event: Some("done"), ... }。
│       └── maybe_error.rs
│                   MaybeError<T, E>：可能包含错误的流 item。
│                   • 在流式响应中传播非致命错误，而不中断整个 stream。
│                   • Value(T) — 正常 token；Error(E) — 该 item 出错，但流可继续。
│                   • 与 Result<T,E> 区别：MaybeError 不中断流，允许后续 item 继续发送。
│
│
└── ── UTILITIES  通用工具 ──────────────────────────────────────────────────────────
    ├── error.rs    PipelineError（全局错误枚举）。
                    区分 Transport / Encoding / Timeout / Cancelled / Engine / Internal 等错误类型，
                    附带 source chain，供 egress/ingress 统一处理和指标上报。

    ├── service.rs  Service（NATS micro service 封装）。
                    将 async-nats service API 适配为 pagoda 内部接口，
                    管理 NATS service 的 endpoint group、stats、info 端点。

    ├── runnable.rs ExecutionHandle（任务执行句柄）。
                    封装 JoinHandle + CancellationToken，提供：
                    is_finished() / is_cancelled() / cancel() / cancellation_token() / handle()。
                    供需要管理后台任务生命周期的模块使用。

    ├── slug.rs     slug 生成工具。生成 URL/文件名安全的短标识符，用于临时路径、日志标签等。

    ├── timeline.rs 时间线标注（feature=timeline）。
            底层可包装 cudarc::nvtx，提供 pagoda_timeline_* 宏，
            在 NVIDIA Nsight Systems 中可视化 Rust 代码的执行时间线。
            feature 未开启时所有调用编译为空（零开销）。

    └── utils.rs    门面，re-export utils/*。
        └── utils/
            ├── graceful_shutdown.rs
                        GracefulShutdownTracker。
                        职责：追踪进程内所有活跃请求处理器，实现三阶段优雅关闭的 Phase 2。
                        机制：每个活跃请求持有一个 Arc<GracefulShutdownTracker> clone；
                        wait_for_completion() 将自身降为弱引用，轮询等待 strong count 归 1
                        （只剩 DistributedRuntime 自己持有）。无需显式注册每个请求。

            ├── tasks/
            │   ├── tracker.rs
            │               TaskTracker（JoinHandle 追踪池）。
            │               DashMap<u64, JoinHandle<()>> 管理所有后台任务句柄，
            │               join_all() 等待所有任务退出；abort_all() 强制取消。
            │               防止后台任务泄漏（进程退出时未等待 → 资源未释放）。
            │   └── critical.rs
            │               spawn_critical(future)。
            │               spawn 一个关键任务：任意 panic 或错误 → std::process::exit(1)。
            │               用于守护进程级不可恢复的后台任务（etcd 连接丢失后无法恢复等）。
            │
            ├── stream.rs   异步 Stream 工具函数集合。
                            into_single(stream) — 从流取首个元素（用于 SingleOut 场景）；
                            timeout_stream(stream, dur) — 为流添加元素级超时；
                            flatten_stream — 将 Stream<Stream<T>> 展平为 Stream<T>。

            ├── pool.rs     通用对象池（RAII 归还模式）。
                            Pool<T>：Arc<Mutex<Vec<T>>> 存储空闲对象；
                            PoolGuard<T>：RAII 借出句柄，drop 时自动归还对象；
                            trait Returnable + ReturnHandle：对象池接口抽象，
                            供 TcpClient 的连接池等复用。

            ├── ip_resolver.rs
                            主机名/网络接口名/IP 字符串解析为 IpAddr。
                            支持：直接 IP 字符串 / hostname DNS 解析 / 网络接口名（"eth0"）。
                            用于 TcpStreamServer 绑定地址解析（PGD_TCP_RPC_HOST）。

            ├── task.rs     单 task 工具函数。
            ├── tasks.rs    多 task 协调工具函数（fan-out、race 等）。
            └── typed_prefix_watcher.rs
                        TypedPrefixWatcher<T>。
                        在 etcd PrefixWatcher 基础上增加自动反序列化层：
                        watch(prefix) → Stream<(key, Option<T>)>（None 表示 key 被删除）。
                        供需要 watch 结构化数据的模块（如配置 watch）使用。
```

---

## 4. 服务模型寻址层级

```
DistributedRuntime
  │
  └── .namespace("llm")                            ← Namespace
        │   key prefix: /llm/
        │
        └── .service_group("worker")                   ← ServiceGroup
              │   key prefix: /llm/worker/
              │
              └── .portname("generate")             ← PortName
                    │   key prefix: /llm/worker/generate/
                    │
                    ├── 服务端：.portname_builder().handler(engine).start()
                    │       ├── discovery.register()   → etcd key: /llm/worker/generate/{uuid}
                    │       │                              value: tcp://10.0.0.1:PORT
                    │       └── request_plane.register_portname(handler)
                    │                                 → TCP DashMap 路由 / NATS subscribe
                    │
                    └── 客户端：.client().await            ← Client
                              └── discovery.list_and_watch()
                                  └── PushRouter (负载均衡)
                                        └── AddressedPushRouter
                                              └── RequestPlaneClient → Instance
```

---

## 5. 请求完整数据流

```
外部调用方
    │  SingleIn<Request>
    ▼
Client.generate(req)
    │
    ├── PushRouter.select_next_worker(instance_ids)
    │       ├── RoundRobin / Random / PowerOfTwo / LeastLoaded / KV / Direct
    │       └── → instance_id
    │
    └── AddressedPushRouter.send_request(req, instance_id)
              │
              │  TCP / NATS / HTTP  →  目标 Worker 进程
              │
              ▼
        RequestPlaneServer (TcpStreamServer / NatsServer / HttpServer)
              │
              ├── ZeroCopyTcpDecoder / decode
              ├── DashMap route → PushWorkHandler
              └── PushWorkHandler.handle_payload(bytes)
                        │
                        └── AsyncEngine.generate(SingleIn<Request>)
                                  │  用户实现
                                  └── yield Annotated<Response> × N
                                            │
                                            └── encode → TCP response stream
                                                    │
                                                    ▼
                                            Client 收到 ManyOut<Response>
```

---

## 6. 模块依赖关系

```
worker
  └─► runtime
        └─► compute
              └─► (Rayon + tokio-rayon)

distributed
  ├─► runtime
  ├─► discovery
  │     └─► storage/kv
  │           ├─► transports/etcd
  │           └─► transports/nats (JetStream KV)
  ├─► pipeline/network
  │     ├─► transports/tcp
  │     ├─► transports/nats
  │     └─► pipeline/codec
  ├─► servicegroup
  │     ├─► discovery (watch/register)
  │     └─► protocols (PortNameId)
  ├─► metrics
  │     └─► system_status_server (Prometheus scrape)
  ├─► system_health
  ├─► logging (tracing/OTLP)
  └─► engine
        └─► pipeline (流水线节点消费引擎)
```

---

## 7. 进程内线程模型

```
OS Thread: main
  └── secondary.block_on(execute_internal)        ← Worker::execute 阻塞于此
          │
          ├── [secondary pool task] signal_handler   SIGINT/SIGTERM → cancel()
          ├── [secondary pool task] watchdog          超时 → exit(911)
          └── [primary pool task]  app_fn(runtime)   ← 用户代码
                  └── DistributedRuntime::from_settings(runtime).await
                        ├── [primary task] discovery::list_and_watch loop
                        ├── [primary task] Client::reconcile_loop × N
                        ├── [primary task] TcpStreamServer::accept_loop
                        ├── [primary task] SystemStatusServer::serve
                        ├── [primary task] HealthCheckManager::run
                        └── [primary task] per-request handler × concurrent

Rayon ComputePool (独立 OS threads "compute-0..N")
  └── CPU 密集型同步计算，通过 tokio-rayon 从 Tokio tasks 提交

Tokio blocking pool (OS threads，max = max_blocking_threads)
  └── tokio::spawn_blocking()，同步 I/O 封装
```

---

## 8. 服务发现与实例生命周期

```
Worker 启动                         Client 侧
    │                                   │
    │  PortNameConfigBuilder::start()   │  Client::new()
    │  → discovery.register(instance)  │  → discovery.list_and_watch()
    │    etcd: PUT key TTL=lease        │    → 初始 list + 持续 watch stream
    │    K8s:  create CRD              │
    │                                   │
    │  lease.keep_alive() 心跳          │  DiscoveryEvent::Added(instance)
    │  ────────────────────────────────►│  → instance_avail.push(id)  [ArcSwap]
    │                                   │
    │  Worker 下线 / lease 过期         │  DiscoveryEvent::Removed(id)
    │  etcd 自动删除 key               │  → instance_avail.remove(id) [ArcSwap]
    │  ────────────────────────────────►│
    │                                   │  client.wait_for_instances() 解除阻塞
```

---

## 9. 健康状态管理

```
SystemHealth
  ├── HealthStatus: NotReady (启动默认)
  │       │
  │       │  canary_wait_time_secs 后
  │       │  或所有 use_endpoint_health_status 端点就绪
  │       ▼
  ├── HealthStatus: Ready
  │       │
  │       │  某 endpoint 报告异常
  │       ▼
  └── HealthStatus: NotReady (局部异常)

SystemStatusServer (PGD_SYSTEM_PORT 启用)
  ├── GET /health   → 200 (Ready) / 503 (NotReady)
  ├── GET /live     → 200 (进程存活)
  ├── GET /metrics  → Prometheus text format
  └── GET /engine/* → EngineRouteRegistry 自定义回调

MetricsHierarchy 树 (Prometheus 自动层级标签):
  DistributedRuntime (root)
    └── Namespace
          └── ServiceGroup
                └── PortName (叶节点)
```

---

## 10. 配置体系与关键环境变量

**配置优先级**：`环境变量 > TOML 配置文件 > 代码默认值`（通过 `figment` 合并）


| 分类     | 环境变量                                   | 默认值                     | 说明                                     |
| ------ | -------------------------------------- | ----------------------- | -------------------------------------- |
| 发现     | `PGD_DISCOVERY_BACKEND`                | `etcd`                  | `etcd` / `kubernetes` / `file` / `mem` |
| 传输     | `PGD_REQUEST_PLANE`                    | `tcp`                   | `tcp` / `nats` / `http`                |
| 传输     | `PGD_EVENT_PLANE`                      | `zmq`                   | `zmq` / `nats`                         |
| 运行时    | `PGD_RUNTIME_NUM_WORKER_THREADS`       | CPU 核数                  | Tokio worker 线程数                       |
| 运行时    | `PGD_RUNTIME_COMPUTE_THREADS`          | 禁用                      | Rayon 计算线程数                            |
| Worker | `PGD_WORKER_GRACEFUL_SHUTDOWN_TIMEOUT` | debug=5s / release=30s  | 超时后 `exit(911)`                        |
| 运维     | `PGD_SYSTEM_PORT`                      | -1（禁用）                  | 运维 HTTP 端口                             |
| 连接     | `PGD_NATS_SERVER`                      | `nats://localhost:4222` | NATS 地址                                |
| 连接     | `PGD_ETCD_ENDPOINTS`                   | `http://localhost:2379` | etcd 地址                                |
| TCP    | `PGD_TCP_WORKER_POOL_SIZE`             | 1500                    | 请求并发上限                                 |
| 追踪     | `OTEL_EXPORT_ENABLED`                  | 未设置                     | 启用 OTLP 导出                             |


---

## 11. Feature Flags


| Feature              | 说明                                          |
| -------------------- | ------------------------------------------- |
| `integration`        | 需要真实 etcd/NATS 的集成测试                        |
| `testing-etcd`       | etcd 测试工具函数                                 |
| `tokio-console`      | tokio-console 运行时诊断                         |
| `compute-validation` | 计算任务参数验证（开发调试）                              |
| `tcp-low-latency`    | Linux TCP 低延迟优化（TCP_QUICKACK, SO_BUSY_POLL） |
| `timeline`           | NVIDIA Nsight Systems 时间线标注（需 CUDA）      |


---

*本文档基于 `lib/runtime/src/` 源代码分析生成，如源码有更新请同步修订。*