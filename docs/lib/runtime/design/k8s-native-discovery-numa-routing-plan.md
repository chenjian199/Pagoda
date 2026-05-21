# 基于新版 K8s 原生发现的 NUMA 感知接入方案

## 1. 目标

在**不破坏现有 Discovery 上层 API** 的前提下，让新版原生 K8s 发现链路具备：

1. worker 在注册时感知本机 NUMA / 拓扑信息；
2. 这些信息能随 K8s 原生对象一起暴露给集群中的其他副本；
3. KV Router 在选 worker 时把 NUMA 作为一个额外因子参与决策；
4. 改动尽量局部化，避免先把 `DiscoveryInstance` / `component::Instance` 这类公共结构做大范围扩容。

---

## 2. 当前代码现状

### 2.1 发现层公共结构没有 NUMA 扩展槽

当前 endpoint 发现最终暴露的是：

- `dynamo_runtime::component::Instance`
- `dynamo_runtime::discovery::DiscoveryInstance::Endpoint(Instance)`
- `dynamo_runtime::discovery::DiscoveryMetadata`

这些结构只承载：

- `namespace`
- `component`
- `endpoint`
- `instance_id`
- `transport`

没有 `numa_node`、`topology_domain`、`socket_id` 一类字段。

这意味着如果直接把 NUMA 塞进公共发现结构，会波及：

- 发现层序列化/反序列化；
- watch/list 兼容；
- `Client` 的实例缓存；
- 所有使用 `Instance` 的通用路由逻辑。

### 2.2 原生 K8s 发现层已经有“对象注解”可作为静态元数据落点

新版并行实现里，endpoint 注册已经会把信息写入：

- `Service` 注解
- `EndpointSlice` 注解

关键位置：

- `lib/runtime/src/discovery/kube/native_objects.rs`
- `register_endpoint_instance()`
- `endpoint_instance_from_service_and_slice()`

也就是说，**K8s 原生对象层已经具备 NUMA 元数据的落盘位置**。

### 2.3 路由层已经有“按 worker 传播静态配置”的稳定入口

LLM 路由侧并不是直接消费 `DiscoveryInstance::Endpoint` 的附加字段，而是主要通过：

- `lib/llm/src/discovery/runtime_configs.rs`
- `runtime_config_watch()`

把两路信息 join 起来：

1. endpoint availability（实例是否存在）
2. model runtime config（按 `instance_id` 对齐的 worker 静态配置）

之后 KV Scheduler / WorkerSelector 消费的是：

- `ModelRuntimeConfig`
- `WorkerConfigLike`
- `WorkerSelector<C>`

其中：

- `ModelRuntimeConfig` 已经是 per-worker 静态配置承载体；
- `WorkerSelector<C>` 已经是可插拔的，不必修改所有通用 router。

### 2.4 代码库里已经有 NUMA 探测能力

现有可复用能力主要在：

- `lib/memory/src/numa/mod.rs`
- `dynamo_memory::numa::get_current_cpu_numa_node()`
- `dynamo_memory::numa::get_device_numa_node(device_id)`

所以“NUMA 怎么探测”本身不是难点，难点是：

- 把探测结果放到哪一层；
- 如何低侵入地送到路由器；
- 让它成为**路由的一个因子**，而不是唯一因子。

---

## 3. 推荐的最小改动原则

### 原则 A：发现层负责“探测并传播静态拓扑事实”

发现层只负责回答：

- 这个 worker 属于哪个 NUMA node / topology domain？
- 这个信息是否可用？
- 这份信息和哪个 `instance_id` 对应？

不在发现层做复杂路由策略。

### 原则 B：路由层负责“把 NUMA 作为打分项之一”

最终路由决策仍然由 KV overlap、decode/prefill load 等主因子主导。
NUMA 只是在候选 worker 之间提供一个**偏置项 / penalty / boost**。

### 原则 C：先不要扩容 `DiscoveryInstance::Endpoint`

第一版不建议直接改：

- `component::Instance`
- `DiscoveryInstance::Endpoint`
- `DiscoveryMetadata`

因为这会让影响面过大。

更合适的路径是：

**K8s 原生对象注解 -> ModelRuntimeConfig 承载 -> 自定义 WorkerSelector 消费**

---

## 4. 推荐的数据流

推荐把 NUMA 数据流拆成两条：

### 4.1 注册/发现链路

worker 启动后本地探测 NUMA：

- CPU 所在 NUMA：`get_current_cpu_numa_node()`
- GPU 邻近 NUMA：`get_device_numa_node(device_id)`（如果该 worker 绑定 GPU）

然后在注册时写入原生对象注解：

- `EndpointSlice` 注解：适合放 **pod / worker 粒度** 的 NUMA 信息
- `Service` 注解：不建议放 worker-specific NUMA，因为 Service 是 component 共享的

建议只把 NUMA 写入 `EndpointSlice`，因为它本来就是 `component.endpoint.pod` 粒度。

### 4.2 路由消费链路

在 model card / runtime config 注册时，把同一份 NUMA 信息写入 `ModelRuntimeConfig`，例如：

- 新增显式字段 `numa_node: Option<u32>`
- 或先放到 `runtime_data["topology"]`

然后：

- `runtime_config_watch()` 继续按 `instance_id` 做 join；
- `KvScheduler` 继续拿 `HashMap<WorkerId, ModelRuntimeConfig>`；
- 自定义 `WorkerSelector<ModelRuntimeConfig>` 在现有 score 基础上叠加 NUMA bias。

这样改动最小，因为：

- `Client` 侧不需要知道 NUMA；
- `DiscoveryMetadata` 公共形状不需要变；
- 只有 native K8s 注册逻辑、runtime config 构造逻辑、KV selector 需要改。

---

## 5. 文件级改动建议

## 5.1 新增一个统一的拓扑描述结构

建议新增一个轻量结构，放在 LLM 可访问、又不强绑定 K8s 的位置，例如：

- `lib/llm/src/local_model/runtime_config.rs`
- 或者新增 `lib/llm/src/topology.rs`

建议结构：

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct WorkerTopology {
    pub numa_node: Option<u32>,
    pub gpu_numa_node: Option<u32>,
    pub topology_domain: Option<String>,
}
```

然后在 `ModelRuntimeConfig` 里新增：

```rust
#[serde(default, skip_serializing_if = "Option::is_none")]
pub topology: Option<WorkerTopology>
```

这里显式字段优于直接塞 `runtime_data`，原因是：

- 类型清晰；
- selector 读取简单；
- 后续可继续扩展 socket / super-node / HCCS domain。

## 5.2 新增本地 NUMA 探测函数

建议新增一个小工具模块，例如：

- `lib/runtime/src/discovery/kube/native_topology.rs`

职责：

1. 本地探测 worker NUMA 信息；
2. 统一生成 K8s 注解键值；
3. 在需要时反序列化回结构体。

建议注解键：

- `nvidia.com/dynamo-numa-node`
- `nvidia.com/dynamo-gpu-numa-node`
- `nvidia.com/dynamo-topology-domain`

其中 `topology_domain` 建议作为未来兼容“华为超节点”的抽象字段。它可以不是单纯 NUMA node，后面也可以编码成：

- `supernode-a/numa-0`
- `rack-2/supernode-1/numa-3`
- `nodepool-x/socket-1`

## 5.3 修改 native endpoint 注册逻辑，把 NUMA 写入 EndpointSlice 注解

改动文件：

- `lib/runtime/src/discovery/kube/native_objects.rs`

在 `register_endpoint_instance()` 中，当前已经构造：

- `registration.endpoint_slice_annotations`

这里直接追加拓扑注解即可。

注意：

- 不建议写入 `Service` 注解，因为 `Service` 是 component 级共享对象；
- NUMA 是 pod/worker 级属性，应写在 `EndpointSlice`。

## 5.4 修改 model ConfigMap 注册逻辑，把 NUMA 同步进 runtime config

改动位置通常不在 native discovery 文件本身，而在**构造 `ModelDeploymentCard` / `ModelRuntimeConfig` 的那一侧**。

目标是让每个 worker 的 runtime config 带上 topology 字段。

原因：

- `runtime_config_watch()` 本来就是路由侧的静态配置入口；
- 它天然按 `instance_id` 关联；
- 不需要新增一条额外 watch。

如果当前 model card 是在 worker 本地生成的，那么这里直接调用同一个 topology probe 即可；
如果你坚持“所有静态事实都以 K8s 原生对象为准”，也可以在 native daemon 端反向读取 `EndpointSlice` 注解并回填到 model config，但那样路径更绕，不推荐第一版这么做。

## 5.5 扩展 `WorkerConfigLike`（可选，但推荐）

改动文件：

- `lib/kv-router/src/protocols.rs`

当前 trait 只有：

- `data_parallel_start_rank()`
- `data_parallel_size()`
- `max_num_batched_tokens()`
- `total_kv_blocks()`

推荐新增一个默认方法，保持向后兼容：

```rust
fn topology_domain(&self) -> Option<&str> {
    None
}
```

以及可选：

```rust
fn numa_node(&self) -> Option<u32> {
    None
}
```

然后让 `ModelRuntimeConfig` 实现它。

这样可以做到：

- 默认 selector 不受影响；
- 新 selector 可以直接通过 trait 读到拓扑信息；
- `SimpleWorkerConfig` 等测试类型只需依赖默认实现，不会大面积炸。

## 5.6 新增一个 NUMA-aware WorkerSelector

改动位置建议：

- `lib/llm/src/kv_router/scheduler.rs` 负责接线
- 新文件 `lib/llm/src/kv_router/numa_selector.rs`

不要直接把大量 NUMA 逻辑硬塞进 `DefaultWorkerSelector`。

更好的做法是：

1. 保留 `DefaultWorkerSelector` 现有语义；
2. 新增 `NumaAwareWorkerSelector`；
3. 内部复用当前公式，再叠加一个 topology bias。

### 推荐打分方式

当前默认公式本质上是越小越好：

```text
score = overlap_weight * potential_prefill_blocks + decode_blocks
```

可以把 NUMA 作为附加 penalty：

```text
score = base_score + numa_penalty
```

其中 `numa_penalty` 取值建议：

- 同 topology domain：`0.0`
- 同 supernode 不同 NUMA：`0.25`
- 跨 supernode：`1.0`
- 未知：`0.1` 或 `0.0`（取决于你想保守还是宽松）

为什么用 penalty 而不是 hard filter：

- overlap / load 仍然是主决策因子；
- NUMA 不会在 worker 稍忙时把候选集合卡死；
- 在华为超节点场景下，能优先选近邻，又保留退化路径。

## 5.7 路由请求侧需要一个“期望拓扑域”来源

NUMA-aware 路由不仅需要知道 **worker 在哪**，还需要知道 **请求更希望去哪**。

这个“请求侧拓扑偏好”建议优先从以下来源之一给出：

1. **本路由副本本地拓扑**
   - 如果 router 与 decode worker 同机/同超节点部署，可直接用 router 本机拓扑作为偏好；
2. **prefill 已选 worker 的 topology**
   - 在 disaggregated 场景里更合理，decode 尽量跟 prefill 就近；
3. **调用侧显式 hint**
   - 后续可扩展 request metadata / agent hint。

第一版最小化建议：

- 先支持“从 router 本地探测 topology_domain 作为默认偏好”；
- 如果请求上下文里已有 prefill worker，则优先用 prefill worker 的 topology。

这部分不一定要先做进 `SchedulingRequest` 公共结构。第一版甚至可以：

- selector 在本地持有一个 `preferred_topology_domain: Option<String>`；
- 先实现静态偏好版本；
- 之后再升级成 per-request dynamic hint。

---

## 6. 为什么不推荐第一版直接改 Discovery 公共结构

如果你第一版就把 NUMA 放进：

- `component::Instance`
- `DiscoveryInstance::Endpoint`
- `DiscoveryMetadata`
- `MetadataSnapshot`
- `Client.instance_source`

虽然概念上“更纯”，但代价很大：

1. 所有 discovery backend 都要跟着补；
2. memory / etcd / kube 多后端兼容都要重新考虑；
3. 现有 watch/list 相关序列化都要调整；
4. 通用 runtime router 也会被迫理解 NUMA，即使它根本不用。

所以更合理的策略是：

- **第一阶段**：NUMA 只走 K8s native + runtime config + KV selector；
- **第二阶段**：如果确认 NUMA / topology 会成为 runtime 通用概念，再抽象进 `DiscoveryInstance`。

---

## 7. 推荐实施顺序

### 阶段 1：打通静态链路

1. 增加 topology probe 模块；
2. endpoint 注册时把 topology 写到 `EndpointSlice` 注解；
3. model runtime config 增加 `topology` 字段并在 worker 注册时填充；
4. 保持现有 selector 不变，先让 topology 跟着 watch 流动起来。

### 阶段 2：接入路由策略

5. 扩展 `WorkerConfigLike` 默认方法；
6. 新增 `NumaAwareWorkerSelector`；
7. 在 decode/prefill scheduler 初始化时按配置切换 selector。

### 阶段 3：细化华为超节点策略

8. 把 `topology_domain` 从简单 NUMA node 扩展成“超节点 / socket / NUMA”层级编码；
9. 支持 per-request topology hint；
10. 把同域优先、跨域惩罚调成配置项。

---

## 8. 我建议的第一版最小 patch 组合

如果现在就开始落代码，我建议第一批只改这几处：

1. `lib/runtime/src/discovery/kube/` 下新增 topology probe/helper；
2. `lib/runtime/src/discovery/kube/native_objects.rs`
   - 给 `EndpointSlice` 注解增加 topology 字段；
3. `lib/llm/src/local_model/runtime_config.rs`
   - 增加 `WorkerTopology` / `topology` 字段；
4. 生成 `ModelRuntimeConfig` 的 worker 注册路径
   - 填充 topology；
5. `lib/kv-router/src/protocols.rs`
   - 给 `WorkerConfigLike` 增加带默认实现的 topology 访问器；
6. `lib/llm/src/kv_router/`
   - 新增 `numa_selector.rs`
   - 配置启用后替换默认 selector。

这样改完后，你就能得到：

- K8s 原生发现对象里能看到 NUMA；
- 路由侧能拿到每个 worker 的 topology；
- 选择 worker 时可把 NUMA 作为 bias；
- 旧 discovery 公共接口几乎不动。

---

## 9. 一句话结论

**最合适的做法不是先把 NUMA 塞进 `DiscoveryInstance::Endpoint`，而是让新版 K8s native discovery 在 `EndpointSlice` 注解里记录 NUMA/拓扑事实，同时把同一份信息写进 `ModelRuntimeConfig`，最后通过一个新的 `NumaAwareWorkerSelector` 把它变成 KV 路由打分的一部分。**

这条路径最符合当前代码结构，也最小侵入。
