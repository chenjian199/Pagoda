# Pagoda Copilot Instructions

## 项目概览

Pagoda 是一个分布式推理框架。当前工作区为 Cargo workspace，仅含 `lib/runtime` crate（`pagoda-runtime`），后续将追加 `lib/llm`、`lib/kv-router` 等。

**架构分层（从上至下）：**
```
Worker → RuntimeConfig → Runtime（primary/secondary 双 Tokio runtime）
  → DistributedRuntime（发现、网络、NATS、健康、指标）
    → Namespace → ServiceGroup → PortName（三段式服务模型）
      → AsyncEngine<Req, Resp, E> / Pipeline
        → Transports（TCP / NATS / HTTP / etcd）
```

## 强制命名约束（违反即错误）

- **三段式**：`Namespace` → `ServiceGroup` → `PortName`（旧版 Component/Endpoint **已废弃**）
- **前缀**：所有 `dynamo` 前缀 → `pagoda`；环境变量 `DYN_*` → `PGD_*`
- **timeline 宏**：`nvtx` 模块已改名为 `timeline`，四个宏前缀为 `pagoda_timeline_range_push`、`pagoda_timeline_range_pop`、`pagoda_timeline_mark`、`pagoda_timeline_domain_create`
- **名称字符集**：Namespace/ServiceGroup/PortName 的 `name` 仅允许 `[a-z0-9\-_]`，由 `validate_allowed_chars()` 在构造时校验
- **已删除**：`storage/` 模块、`KvStoreDiscovery`、`instances.rs`、`EndpointId`

## 文件版权头（每个 .rs 文件必须包含）

```rust
// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
```

## 关键设计规则

- **网络懒触发**：`drt.namespace("x")?.service_group("y")?.portname("z")` 仅构造内存对象，**不产生网络访问**。仅 `PortNameConfigBuilder::start()`（注册）和 `PortName::client()`（订阅）触发真实网络动作。
- **双运行时隔离**：`primary` 运行业务 I/O 任务，`secondary` 运行框架控制任务（信号、watchdog、发现 watch）。
- **发现后端**：生产用 `KubeDiscoveryClient`（K8s Service/EndpointSlice/ConfigMap/Lease），测试用 `MockDiscovery`（进程内内存）。
- **模型实例**：`Instance` 结构体必须包含 `topo_json: serde_json::Value` 字段。
- **LocalPortNameRegistry**：键为 `PortNameId`（不是旧版 `EndpointId`），存储 `DashMap<PortNameId, Arc<dyn AnyAsyncEngine>>`。
- `local_endpoint_registry.rs` 保留为兼容层，内部重定向至 `local_portname_registry`。

## 典型入口模式

```rust
use pagoda_runtime::prelude::*;

fn main() -> anyhow::Result<()> {
    Worker::from_settings()?.execute(|runtime| async move {
        let drt = DistributedRuntime::from_settings(runtime).await?;
        let portname = drt.namespace("llm")?.service_group("worker")?.portname("generate");
        portname.portname_builder().handler(my_engine).start().await
    })
}
```

## 构建与测试

```bash
# 构建
cargo build -p pagoda-runtime

# 单元测试（无外部依赖）
cargo test -p pagoda-runtime

# 集成测试（需真实 K8s 环境）
cargo test -p pagoda-runtime --features integration

# 带 etcd 工具的测试
cargo test -p pagoda-runtime --features testing-etcd
```

## 关键文件索引

| 文件 | 职责 |
|------|------|
| [lib/runtime/src/lib.rs](lib/runtime/src/lib.rs) | 模块树与顶层 re-export |
| [lib/runtime/CONSTRAINTS.md](lib/runtime/CONSTRAINTS.md) | **所有设计约束权威文档** |
| [lib/runtime/src/worker.rs](lib/runtime/src/worker.rs) | 进程入口与生命周期（`Worker`） |
| [lib/runtime/src/distributed.rs](lib/runtime/src/distributed.rs) | `DistributedRuntime`，分布式层核心 |
| [lib/runtime/src/servicegroup/](lib/runtime/src/servicegroup/) | 三段式服务模型实现 |
| [lib/runtime/src/engine.rs](lib/runtime/src/engine.rs) | `AsyncEngine` trait 与流式引擎抽象 |
| [info/name-rules.md](info/name-rules.md) | 命名规约速查 |
