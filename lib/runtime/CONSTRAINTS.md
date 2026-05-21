# pagoda-runtime 约束文档

> 本文件定义 pagoda-runtime crate 的设计约束、命名规则和模块边界。
> 所有开发者在贡献代码前必须阅读并遵守。

---

## 一、命名约束

### 1.1 三段式命名（强制）

| 层级 | 旧版 | 新版 | 说明 |
|------|------|------|------|
| 第一段 | Namespace | **Namespace** | 保持不变 |
| 第二段 | Component | **ServiceGroup** | 共享职责的服务集合 |
| 第三段 | Endpoint | **PortName** | 具体 RPC 语义端口 |

### 1.2 前缀命名（强制）

- 所有涉及 `dynamo` 的常量、变量、字符串、函数名均改为 `pagoda`（注意大小写）
- 环境变量前缀：`DYN_*` → `PGD_*`
- Crate 名：`dynamo-runtime` → `pagoda-runtime`
- 项目名常量：`"Pagoda"`

### 1.3 时间线宏（强制）

- `nvtx` 模块更改为 `timeline` 模块
- 四个宏前缀：`pagoda_timeline_range_push`、`pagoda_timeline_range_pop`、`pagoda_timeline_mark`、`pagoda_timeline_domain_create`

### 1.4 名称字符集（强制）

- Namespace / ServiceGroup / PortName 的 `name` 字段仅允许 `[a-z0-9\-_]`
- 通过 `validate_allowed_chars()` 在构造时校验

---

## 二、模块边界约束

### 2.1 已删除模块

| 模块 | 状态 | 原因 |
|------|------|------|
| `storage/` | **完全删除** | 新版不再需要 KV 存储抽象层 |
| `storage/kv.rs` | **完全删除** | Store/Bucket trait 不再需要 |
| `storage/kv/etcd.rs` | **完全删除** | etcd 连接保留在 transports/etcd |
| `storage/kv/file.rs` | **完全删除** | 无本地文件发现需求 |
| `storage/kv/mem.rs` | **完全删除** | MockDiscovery 替代 |
| `storage/kv/nats.rs` | **完全删除** | NATS KV 不再作为发现后端 |

### 2.2 已替换模块

| 旧模块 | 新模块 | 说明 |
|--------|--------|------|
| `component/` | `servicegroup/` | 新三段式服务模型 |
| `component/namespace.rs` | `servicegroup/namespace.rs` | |
| `component/component.rs` | `servicegroup/servicegroup_impl.rs` | |
| `component/endpoint.rs` | `servicegroup/portname.rs` | |
| `component/client.rs` | `servicegroup/client.rs` | 合并了 instances.rs |
| `component/registry.rs` | `servicegroup/registry.rs` | |
| `component/service.rs` | `servicegroup/service.rs` | |

### 2.3 发现后端约束

| 后端 | 状态 | 存储 |
|------|------|------|
| `KubeDiscoveryClient` | **唯一生产后端** | K8s 原生：Service/EndpointSlice/ConfigMap/Lease |
| `MockDiscovery` | **测试专用** | 进程内 Vec |
| `KvStoreDiscovery` | **已删除** | 随 storage 模块删除 |

### 2.4 `instances.rs` 归属

`instances.rs` 的 `InstanceSource` 功能已完全合并到 `servicegroup/client.rs` 中的 `get_or_create_dynamic_instance_source()`。不再保留独立的 `instances.rs` 文件。

### 2.5 `local_endpoint_registry.rs` 寻址

使用 `PortNameId`（不是旧版的 `EndpointId`）作为 `DashMap` 键：
```rust
DashMap<PortNameId, Arc<dyn AnyAsyncEngine>>
```

---

## 三、设计约束

### 3.1 调用链同步原则

`drt.namespace("x")?.service_group("y")?.portname("z")` 只是内存对象构造，不产生网络访问。仅以下路径触发真实网络动作：
- `PortNameConfigBuilder::start()`：向发现系统注册实例
- `PortName::client()`：订阅发现系统并维护动态实例视图

### 3.2 双运行时隔离

- `primary`：业务 I/O 任务（请求处理、推理）
- `secondary`：框架控制任务（信号处理、watchdog、发现 watch）
- 发现观察任务（`port_watcher`）运行在 secondary runtime

### 3.3 取消令牌层级

```
cancellation_token（根）
  └── endpoint_shutdown_token（子）
        └── per-portname port_shutdown_token（孙）
```

Phase 1：取消 endpoint_shutdown_token → 停止接受新请求
Phase 2：等待 in-flight 请求完成（GracefulShutdownTracker）
Phase 3：取消 cancellation_token → 断开后端连接

### 3.4 注册顺序约束

`PortNameConfigBuilder::start()` 中：
1. 请求平面注册**必须先于**发现注册
2. health check target 注册**在**请求平面注册**前**
3. cleanup task**在**发现注册**前**创建

### 3.5 模型实例拓扑

所有模型实例（DiscoverySpec::Model / DiscoveryInstance::Model）携带 `topo_json: serde_json::Value`，供 NUMA-aware、rack-aware 路由使用。

### 3.6 错误类型（双错误体系）

框架包含两个独立的错误类型，职责不同、不可互相替代：

| 错误类型 | 文件 | 特征 | 职责 |
|---------|------|------|------|
| `PagodaError` | `src/error.rs` | `serde` 可序列化、`Clone`、跨网络 | 框架级跨网络错误传输，路由层重试/熔断决策 |
| `PipelineError` | `src/pipeline/error.rs` | `thiserror` 派生、含 `source` 链、不可序列化 | 管道子系统内部，ingress/egress 路径处理与指标上报 |

转换关系：
- `PipelineError` → `PagodaError`：通过 `From<PipelineError> for PagodaError` 在 egress 边界转换
- `PagodaError` 不可转回 `PipelineError`（跨网络传输后丢失了 source 错误链）

`PagodaError` 支持：
- 可分类（`ErrorType` 枚举：Unknown / InvalidArgument / CannotConnect / Disconnected / ConnectionTimeout / Cancelled / Backend）
- 可序列化（跨 TCP/NATS/HTTP 网络传输）
- 可链式追踪（`caused_by` 字段）

`PipelineError` 覆盖：
- Transport（网络/传输故障）
- Encoding（编解码失败）
- Timeout（请求超时，含 elapsed 和 limit）
- Cancelled（请求取消）
- Engine（下游引擎错误，包装 `EngineError`）
- Internal（内部兜底，包装 `anyhow::Error`）

---

## 四、环境变量清单

所有环境变量集中定义在 `src/config/environment_names.rs`，使用 `PGD_` 前缀。

| 分类 | 环境变量 | 默认值 | 说明 |
|------|---------|--------|------|
| 发现 | `PGD_DISCOVERY_BACKEND` | `kubernetes` | 唯一生产后端 |
| 传输 | `PGD_REQUEST_PLANE` | `tcp` | tcp/nats/http |
| 传输 | `PGD_EVENT_PLANE` | `nats` | nats/zmq |
| 运行时 | `PGD_RUNTIME_NUM_WORKER_THREADS` | CPU 核数 | Tokio 线程数 |
| 运行时 | `PGD_RUNTIME_COMPUTE_THREADS` | 禁用 | Rayon 线程数 |
| Worker | `PGD_WORKER_GRACEFUL_SHUTDOWN_TIMEOUT` | debug=5s/release=30s | 超时后 exit(911) |
| 运维 | `PGD_SYSTEM_PORT` | -1（禁用） | HTTP 端口 |
| 连接 | `PGD_NATS_SERVER` | `nats://localhost:4222` | NATS 地址 |
| 连接 | `PGD_ETCD_ENDPOINTS` | `http://localhost:2379` | etcd 地址 |
| TCP | `PGD_TCP_WORKER_POOL_SIZE` | 1500 | 请求并发上限 |
| 追踪 | `OTEL_EXPORT_ENABLED` | 未设置 | 启用 OTLP 导出 |

---

## 五、依赖约束

- `lib/runtime` **不依赖** `lib/llm`（通过 `card_json: serde_json::Value` 解耦）
- etcd 连接保留在 `transports/etcd.rs`，但不再有 `storage/kv` 抽象层
- NATS 客户端通过 `Option<Client>` 保持可选（非所有部署需要）

---

*基于新版设计文档生成，版本 v0.1.0*
