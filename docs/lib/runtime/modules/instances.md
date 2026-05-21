# `instances` 模块设计文档

**源码位置**：`lib/runtime/src/instances.rs`（约 35 行，单文件极简模块）

---

## 一、设计背景与模块职责

Pagoda 的服务发现体系中，`Instance` 是一个 Worker `PortName` 的具体运行实例——它包含了命名空间、`ServiceGroup` 名、`PortName` 名、实例 ID 和传输地址（TCP 地址或 NATS Subject）等信息。

两种不同粒度的实例查询需求存在于系统中：

1. **特定 `PortName` 的实例列表**：某个 `PortName` 对象想知道"有哪些 Worker 在处理 `generate` 请求？"——这由 `servicegroup::Client` 负责，通过 Watch 机制维护实时更新的实例列表。

2. **全局所有实例的快照**：运维工具（CLI 中的 `pagoda ps`、监控仪表盘）想知道"整个集群中当前有哪些 `PortName` 的哪些实例在运行？"——这需要一次性列出发现后端中已注册的 `PortName` 类型实例。

`servicegroup::Client` 的设计是增量 Watch 而非一次性列表，适合实时路由但不适合批量查询。`instances` 模块提供全局一次性快照查询，是对 `servicegroup.rs` 功能的补充，而非重复。

---

## 二、`list_all_instances` 函数

### 为什么需要独立的模块

```rust
pub async fn list_all_instances(
    discovery_client: Arc<dyn Discovery>,
) -> anyhow::Result<Vec<Instance>>
```

这个函数只有 ~20 行，但独立成模块有以下原因：

**职责分离**：`Discovery` trait（定义在 `discovery/` 目录）只负责"从发现后端读取数据"，不知道也不应该知道 `Instance` 这个领域类型。`instances.rs` 是连接两个层次的桥梁——它知道 `DiscoveryInstance` 的变体（`PortName` 还是 `Model` / `ModelCard` 语义），也知道 `Instance` 类型，在这里做过滤转换。

**测试和复用的独立单元**：将此逻辑放在 `servicegroup.rs` 会让 `ServiceGroup` 对象承担"全局查询"的职责（但 `ServiceGroup` 只知道自己的命名空间和服务组名，不是"全局"视角）；放在 `distributed.rs` 会使 drt（`DistributedRuntime`）的方法过于细碎。独立模块使调用方可以只传入 `Arc<dyn Discovery>`，无需构造完整的 drt 对象，更易于测试。

---

### 函数详解

```rust
pub async fn list_all_instances(
    discovery_client: Arc<dyn Discovery>,
) -> anyhow::Result<Vec<Instance>> {
    let discovery_instances = discovery_client
        .list(DiscoveryQuery::AllPortNames)
        .await?;

    let mut instances: Vec<Instance> = discovery_instances
        .into_iter()
        .filter_map(|di| match di {
            crate::discovery::DiscoveryInstance::PortName(instance) => Some(instance),
            _ => None,  // 忽略 Model（承载 ModelCard）等其他变体
        })
        .collect();

    instances.sort();

    Ok(instances)
}
```

**`DiscoveryQuery::AllPortNames`**

向发现后端发起"列出所有 `PortName` 类型实例"的查询。`DiscoveryQuery` 是一个枚举，`AllPortNames` 变体对应的语义是"不过滤命名空间、不过滤 `ServiceGroup` 名，返回所有已注册的 `PortName` 类型记录"。

在 Pagoda 的最终设计中，这对应一次对发现快照的全局读取：k8s 原生发现后端会从 `Service` / `EndpointSlice` 聚合出的 `PortName` 视图中返回所有实例；测试替身则从内存注册表中返回所有实例。

**`filter_map` 过滤 `DiscoveryInstance` 变体**

发现后端返回的 `Vec<DiscoveryInstance>` 是混合类型——`DiscoveryInstance` 枚举有多个变体，包括 `PortName`（Worker 实例）和 `Model`（其负载是 `ModelCard`，用于模型注册表功能）。本函数只关心 Worker 实例，`filter_map` 的 `_ => None` 静默忽略所有非 `PortName` 变体。

这种设计的健壮性体现在：未来若发现后端增加了新的 `DiscoveryInstance` 变体（如控制面元数据卡），`list_all_instances` 无需修改即可自动忽略，不会因为 `match` 缺少分支而编译报错（通配符 `_` 处理新变体）。

**`instances.sort()`**

为什么对结果排序：

1. **确定性输出**：`Discovery::list()` 的底层实现（k8s 对象聚合、测试替身内存遍历）不保证返回顺序。若直接返回乱序列表，CLI 每次执行 `pagoda ps` 的输出顺序不稳定，用户难以阅读和比较。
2. **测试友好**：测试中断言 `Vec<Instance>` 的内容时，排序后可以直接比较 `vec == expected_vec`，无需复杂的集合相等检查。

`Instance` 实现了 `Ord`（或 `PartialOrd`），排序键通常包含命名空间 + `ServiceGroup` 名 + `PortName` 名 + 实例 ID，使输出按层级结构有序排列，便于阅读。

**`async` 和 `?` 的使用**

`discovery_client.list()` 是异步操作（可能涉及 k8s API 或异步快照读取），必须 `await`。错误直接用 `?` 传播，不做额外包装——这是工具函数的常见模式，错误处理留给调用方（CLI 会格式化错误消息，drt 方法会添加上下文）。

**参数类型 `Arc<dyn Discovery>` 而非 `&dyn Discovery`**

`Discovery::list()` 是 async trait 方法，async trait 方法通常要求 `&self: Send`，而 async fn 的 Future 若持有 `&self` 引用则其生命周期受限。传入 `Arc<dyn Discovery>` 使函数可以在 Future 内部安全地持有发现客户端的引用（通过 Arc clone），满足 `'static` 约束。调用方通常已经持有 `Arc<dyn Discovery>`（如从 drt 的 `discovery()` 方法取得），直接传入无需额外包装。
