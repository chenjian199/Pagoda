# pagoda-runtime 模块地图

> 每个源码文件的职责概述，供开发者快速定位。

---

## 进程入口层 (Entry Layer)

| 文件 | 核心类型 | 职责 |
|------|---------|------|
| `lib.rs` | — | pub 门面，re-export 全部公开 API |
| `prelude.rs` | — | 常用类型别名集合 |
| `worker.rs` | `Worker` | 进程入口、信号处理、优雅关闭 |
| `runtime.rs` | `Runtime` | 双 Tokio 线程池 + 取消令牌树 + Rayon 池 |
| `config.rs` | `RuntimeConfig` | figment 多层配置合并 |
| `config/environment_names.rs` | — | `PGD_*` 环境变量名常量 |

## 分布式运行时层 (Distributed Layer)

| 文件 | 核心类型 | 职责 |
|------|---------|------|
| `distributed.rs` | `DistributedRuntime` | 集群感知能力单一入口 |
| `traits.rs` | `RuntimeProvider`, `DistributedRuntimeProvider` | 通用 Provider trait |

## 服务模型层 (Service Model Layer)

| 文件 | 核心类型 | 职责 |
|------|---------|------|
| `servicegroup.rs` | `Namespace`, `ServiceGroup`, `PortName`, `Instance`, `TransportType` | 新三段式入口 |
| `servicegroup/namespace.rs` | — | Namespace 的 MetricsHierarchy 实现 |
| `servicegroup/servicegroup_impl.rs` | — | ServiceGroup 的 MetricsHierarchy 实现 |
| `servicegroup/portname.rs` | `PortNameConfig`, `PortNameConfigBuilder` | 服务端注册配置与传输地址构建 |
| `servicegroup/client.rs` | `Client`, `RoutingOccupancyState` | 发现订阅 + 实例视图 + 负载均衡 |
| `servicegroup/registry.rs` | `Registry` | NATS service 注册表 |
| `servicegroup/service.rs` | — | NATS micro service 构建（兼容层） |

## 引擎与管道层 (Engine / Pipeline Layer)

| 文件 | 核心类型 | 职责 |
|------|---------|------|
| `engine.rs` | `AsyncEngine`, `AsyncEngineContext`, `ResponseStream`, 类型擦除 | 核心引擎 trait |
| `engine_routes.rs` | `EngineRouteRegistry` | /engine/* HTTP 路由回调 |
| `pipeline.rs` | — | 管道模块门面 |
| `pipeline/context.rs` | `Context<T>`, `StreamContext`, `Controller` | 请求上下文 |
| `pipeline/error.rs` | `PipelineError` | 管道子系统内部错误（不可序列化，独立于 PagodaError） |
| `pipeline/registry.rs` | `PipelineRegistry` | 管道实例注册表 |
| `pipeline/nodes/sources/*` | `Source<T>`, `Frontend`, `ServiceFrontend` | 图入口节点 |
| `pipeline/nodes/sinks/*` | `Sink<T>`, `ServiceBackend`, `SegmentSink` | 图出口节点 |
| `pipeline/network/manager.rs` | `NetworkManager` | 请求平面统一管理 |
| `pipeline/network/codec/*` | `TwoPartCodec`, `ZeroCopyTcpDecoder` | 帧编解码 |
| `pipeline/network/tcp/*` | `TcpStreamServer`, `TcpClient`, `ConnectionPool` | TCP 传输 |
| `pipeline/network/ingress/*` | `RequestPlaneServer`, `PushWorkHandler` | 服务端接入 |
| `pipeline/network/egress/*` | `RequestPlaneClient`, `PushRouter`, `RouterMode` | 客户端出口 |

## 服务发现层 (Discovery Layer)

| 文件 | 核心类型 | 职责 |
|------|---------|------|
| `discovery/mod.rs` | `Discovery` trait, `DiscoveryQuery`, `DiscoveryEvent` | 统一发现抽象 |
| `discovery/metadata.rs` | `DiscoveryMetadata`, `MetadataSnapshot` | 注册元数据 |
| `discovery/mock.rs` | `MockDiscovery`, `SharedMockRegistry` | 测试替身 |
| `discovery/utils.rs` | `watch_and_extract_field` | 通用流转化 |
| `discovery/kube.rs` | `KubeDiscoveryClient` | K8s 原生发现后端 |
| `discovery/kube/service_registry.rs` | `ServiceRegistration` | Service/EndpointSlice 注册 |
| `discovery/kube/objects.rs` | — | K8s 对象 ↔ DiscoveryInstance 映射 |
| `discovery/kube/daemon.rs` | `DiscoveryDaemon` | 多 reflector 聚合守护进程 |
| `discovery/kube/utils.rs` | `PodInfo`, `hash_pod_name` | Pod 身份解析 |

## 传输协议层 (Transports Layer)

| 文件 | 核心类型 | 职责 |
|------|---------|------|
| `transports/etcd.rs` | — | etcd 客户端封装门面 |
| `transports/etcd/connector.rs` | `EtcdConnector` | 连接建立、TLS、重试 |
| `transports/etcd/lease.rs` | `Lease` | RAII lease + keep_alive |
| `transports/etcd/lock.rs` | `DistributedRWLock` | 基于 etcd CAS 的分布式锁 |
| `transports/etcd/kv.rs` | `PrefixWatcher`, `KvCache` | etcd KV 操作 |
| `transports/nats.rs` | `Client`, `ClientOptions`, `NatsAuth` | NATS 封装 |
| `transports/tcp.rs` | — | re-export TCP server/client |
| `transports/zmq.rs` | `ZmqPublisher`, `ZmqSubscriber` | ZMQ 传输 |
| `transports/event_plane/*` | `EventTransportTx/Rx`, `EventPublisher/Subscriber` | 事件面 |

## CPU 计算隔离层 (Compute Layer)

| 文件 | 核心类型 | 职责 |
|------|---------|------|
| `compute/mod.rs` | `ComputePool`, `ScopeExecutor` | Rayon 计算池 |
| `compute/pool.rs` | — | ThreadPool 创建配置 |
| `compute/thread_local.rs` | — | thread-local 预热 |
| `compute/macros.rs` | `compute!()` | 便利宏 |
| `compute/metrics.rs` | `ComputeMetrics` | 计算池指标 |
| `compute/validation.rs` | — | 参数验证（debug） |

## 可观测性层 (Observability Layer)

| 文件 | 核心类型 | 职责 |
|------|---------|------|
| `logging.rs` | `TraceParent`, `GenericHeaders` | 分布式追踪与日志 |
| `metrics.rs` | `MetricsRegistry`, `MetricsHierarchy` | Prometheus 指标树 |
| `metrics/frontend_perf.rs` | `FrontendPerfMetrics` | TTFT/TPOT 指标 |
| `metrics/tokio_perf.rs` | `TokioPerfMetrics` | Tokio 运行时指标 |
| `metrics/transport_metrics.rs` | `TransportMetrics` | 传输层指标 |
| `metrics/request_plane.rs` | `RequestPlaneMetrics` | 请求平面指标 |
| `metrics/work_handler_perf.rs` | `WorkHandlerPerfMetrics` | Handler 性能 |
| `metrics/prometheus_names.rs` | — | 指标名常量 |
| `system_status_server.rs` | `SystemStatusServerInfo` | 运维 HTTP 服务 |
| `health_check.rs` | `HealthCheckManager`, `HealthCheckTarget` | 健康检查 |
| `system_health.rs` | `SystemHealth`, `HealthStatus` | 健康状态聚合 |

## 协议辅助类型 (Protocols)

| 文件 | 核心类型 | 职责 |
|------|---------|------|
| `protocols/annotated.rs` | `Annotated<T>` | 流式 token 注解 |
| `protocols/maybe_error.rs` | `MaybeError<T, E>` | 流内非致命错误 |

## 通用工具 (Utilities)

| 文件 | 核心类型 | 职责 |
|------|---------|------|
| `error.rs` | `PagodaError`, `ErrorType`, `BackendError` | 框架级跨网络错误（可序列化） |
| `service.rs` | `Service` | NATS micro service 封装 |
| `local_endpoint_registry.rs` | `LocalEndpointRegistry` | 进程内直连表 |
| `runnable.rs` | `ExecutionHandle` | 可取消任务句柄 |
| `slug.rs` | `slugify()` | URL 安全标识符 |
| `timeline.rs` | `pagoda_timeline_*` 宏 | NVIDIA 时间线标注 |
| `utils/graceful_shutdown.rs` | `GracefulShutdownTracker` | Phase 2 关闭追踪 |
| `utils/tasks/tracker.rs` | `TaskTracker` | 后台任务追踪池 |
| `utils/tasks/critical.rs` | `spawn_critical()` | 关键任务 spawn |
| `utils/stream.rs` | `into_single()`, `timeout_stream()` | Stream 工具 |
| `utils/pool.rs` | `Pool<T>`, `PoolGuard<T>` | 通用对象池 |
| `utils/ip_resolver.rs` | `resolve_address()` | 地址解析 |
| `utils/task.rs` | `spawn_cancellable()`, `spawn_linked()` | 单任务工具 |
| `utils/typed_prefix_watcher.rs` | `TypedPrefixWatcher<T>` | etcd 自动反序列化 watch |

---

## 统计

- **源码文件总数**：114 个 `.rs` 文件
- **模块层级**：7 层（Entry / Distributed / Service Model / Engine-Pipeline / Discovery / Transport / Observability）
- **Cargo features**：6 个可选 feature
