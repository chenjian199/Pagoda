# pagoda-runtime

Pagoda 分布式推理框架的核心基础设施层。

## 系统定位

`pagoda-runtime` 是 Pagoda 推理系统的**核心基础设施层**，为上层业务组件提供：

- 本地与分布式异步执行上下文（Tokio Runtime + Rayon ComputePool）
- 跨节点服务注册与发现（Kubernetes 原生资源）
- 统一请求平面传输抽象（TCP / NATS / HTTP）
- 可组合的流式引擎框架（AsyncEngine / Pipeline）
- 可观测性接入点（Prometheus 指标树、W3C 链路追踪、健康检查）
- 进程生命周期管理（信号处理、三阶段优雅关闭）

## 整体分层架构

```
┌─────────────────────────────────────────────────────────────────┐
│                     业务层（上层 crate）                          │
│      pagoda-llm · pagoda-kv-router · 用户 workers               │
└───────────────────────────┬─────────────────────────────────────┘
                            │ pub API
┌───────────────────────────▼─────────────────────────────────────┐
│  ENTRY LAYER — 进程入口层                                        │
│  Worker ──► RuntimeConfig ──► create_runtime() ──► Runtime       │
└───────────────────────────┬─────────────────────────────────────┘
┌───────────────────────────▼─────────────────────────────────────┐
│  RUNTIME LAYER — 本机运行时层                                     │
│  Runtime { primary, secondary, cancellation_token, compute_pool }│
└───────────────────────────┬─────────────────────────────────────┘
┌───────────────────────────▼─────────────────────────────────────┐
│  DISTRIBUTED LAYER — 分布式运行时层                               │
│  DistributedRuntime { discovery, network, nats, health, metrics }│
└───────────────────────────┬─────────────────────────────────────┘
┌───────────────────────────▼─────────────────────────────────────┐
│  SERVICE MODEL LAYER — 服务模型层（新三段式）                      │
│  Namespace ──► ServiceGroup ──► PortName                         │
└───────────────────────────┬─────────────────────────────────────┘
┌───────────────────────────▼─────────────────────────────────────┐
│  PIPELINE / ENGINE LAYER — 引擎与管道层                           │
│  AsyncEngine<Req, Resp, E> · PushRouter · Pipeline Nodes         │
└───────────────────────────┬─────────────────────────────────────┘
┌───────────────────────────▼─────────────────────────────────────┐
│  TRANSPORT LAYER — 传输层                                        │
│  TCP · NATS · HTTP · ZMQ(event) · etcd                           │
└─────────────────────────────────────────────────────────────────┘
```

## 新版三段式命名

| 层级 | 含义 | 示例 |
|------|------|------|
| Namespace | 租户/环境/业务边界 | `llm` |
| ServiceGroup | 共享职责的服务集合 | `worker` |
| PortName | 具体 RPC 语义端口 | `generate` |

## 命名约束

- 所有 `dynamo` 前缀 → `pagoda`
- 环境变量 `DYN_*` → `PGD_*`
- Crate 名：`pagoda-runtime`
- 模型实例新增 `topo_json: serde_json::Value`
- `nvtx` 模块 → `timeline`，宏前缀 `pagoda_timeline_`

## 发现后端

仅保留两个后端：
- **KubeDiscoveryClient**：生产环境，使用 K8s 原生资源（Service/EndpointSlice/ConfigMap/Lease）
- **MockDiscovery**：测试环境，进程内内存实现

## Feature Flags

| Feature | 说明 |
|---------|------|
| `integration` | 需要真实 K8s 环境的集成测试 |
| `testing-etcd` | etcd 测试工具函数 |
| `tokio-console` | tokio-console 运行时诊断 |
| `compute-validation` | 计算任务参数验证（开发调试） |
| `tcp-low-latency` | Linux TCP 低延迟优化 |
| `timeline` | NVIDIA Nsight Systems 时间线标注 |

## 快速开始

```rust
use pagoda_runtime::prelude::*;

fn main() -> Result<()> {
    Worker::from_settings()?.execute(|runtime| async move {
        let drt = DistributedRuntime::from_settings(runtime).await?;
        let ns = drt.namespace("llm")?;
        let sg = ns.service_group("worker")?;
        let portname = sg.portname("generate");

        // 注册服务端
        portname
            .portname_builder()
            .handler(my_engine)
            .start()
            .await?;

        Ok(())
    })
}
```

## 许可证

Apache-2.0
