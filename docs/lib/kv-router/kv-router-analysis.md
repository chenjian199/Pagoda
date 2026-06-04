# Pagoda 通用 KV Router 模块设计分析文档

## 1. 文档目的

本文档用于定义 Pagoda 项目中通用 `kv-router` 模块的设计方向、模块边界、第一阶段实现范围以及后续扩展路径。

当前设计基于以下前提：

- Pagoda 第一阶段暂不实现 KVBM；
- Pagoda 第一阶段不保留 Dynamo/KVBM 的完整 `memory` 模块；
- Pagoda 第一阶段只接入 LMCache 作为外部 KV cache 后端；
- KV Router 作为独立的控制面路由模块保留；
- Router 不直接搬运 KV tensor，只基于 KV 元数据、worker 负载和拓扑代价做路由决策；
- 未来保留接入 KVBM、HiCache、Mooncake、AscendStore 等能力；
- 当前优先适配 NVIDIA 与 Huawei Ascend 机器；
- 拓扑建模必须通用，保留适配其它硬件平台的能力。

本文档既是技术分析，也是 Pagoda `kv-router` 模块的设计依据。

---

## 2. 总体结论

Pagoda 第一阶段应保留 KV Router，但应去除对 KVBM / memory 的硬依赖。

推荐结论如下：

```text
保留：
  - 通用 KV Router
  - worker 注册与发现
  - token prefix hash / block hash
  - KV event 消费与 indexer
  - active load tracking
  - topology-aware cost model
  - LMCache connector 接入配置
  - NVIDIA / Huawei 拓扑适配
  - 未来 KVBM / HiCache / Mooncake / AscendStore 扩展点

暂不保留：
  - Dynamo KVBM block manager
  - memory allocator
  - KVBM block lifecycle state machine
  - KVBM CPU / disk / remote tier manager
  - KVBM NIXL layer abstraction
  - Pagoda 自研多级 KV 内存管理
```

核心设计原则：

```text
Router 管控制面；
Connector 管数据面；
Indexer 管路由视图；
Cache backend 管真实 KV tensor；
Topology provider 管硬件和网络拓扑；
Cost model 统一计算路由分数。
```

---

## 3. 背景：KV Router 为什么仍然需要保留

即使不做 KVBM，KV Router 仍然有价值。

LLM 推理中，prompt prefill 代价很高。对于相同或相似 prefix 的请求，如果能路由到已有 KV cache 的 worker，或者路由到能较低成本从共享缓存取回 KV 的 worker，就可以减少重复 prefill。

因此 KV Router 的职责不是保存 KV，而是回答：

```text
当前请求应该发给哪个 worker，综合成本最低？
```

综合成本包括：

- 当前 worker 的 decode 负载；
- 当前 worker 是否已有 device-local KV；
- 外部 LMCache / shared pool 中是否已有 KV；
- 取回 KV 的传输代价；
- worker 所在拓扑位置；
- NVIDIA / Huawei 机器上的不同通信层级代价；
- aggregated / disaggregated serving 模式下的角色差异。

即使第一阶段只使用 LMCache，也仍然需要一个轻量但可扩展的 KV Router。

---

## 4. 总体架构

### 4.1 模块关系

```text
Client Request
  ↓
Frontend / Protocol Layer
  ↓
Tokenizer
  ↓
KV Router
  ├── Worker Registry
  ├── KV Indexer
  ├── Active Load Tracker
  ├── Shared Cache Metadata Client
  ├── Topology Provider
  └── Cost Model
  ↓
Selected Worker
  ↓
vLLM / SGLang / Other Backend
  ↓
KV Connector
  ↓
LMCache / KVBM / HiCache / Mooncake / AscendStore
```

### 4.2 控制面与数据面

```text
控制面：
  - Router
  - Indexer
  - Event Plane
  - Worker Registry
  - Topology Provider
  - Cost Model

数据面：
  - vLLM GPU paged KV buffer
  - connector load/save
  - LMCache / Mooncake / KVBM / AscendStore
  - GPU / CPU / Disk / Remote storage
```

Router 不应直接访问真实 KV tensor。  
Router 只消费元数据并计算路由分数。

---

## 5. 第一阶段范围：LMCache-only

### 5.1 第一阶段目标

第一阶段目标是：

```text
Pagoda Runtime
  ↓
KV Router
  ↓
vLLM worker
  ↓
LMCache connector
  ↓
LMCache L1 / L2 backend
```

第一阶段支持：

- LMCache connector 配置生成；
- LMCache sidecar / server 接入；
- vLLM `kv_transfer_config` 管理；
- aggregated 模式；
- disaggregated 模式配置预留；
- KV events 消费接口预留；
- LMCache shared metadata 查询预留；
- NVIDIA / Huawei 拓扑打分框架；
- KVBM / HiCache / Mooncake / AscendStore 扩展点。

### 5.2 第一阶段不做

第一阶段不做：

- KVBM；
- 自研 memory manager；
- 多级内存分配器；
- KVBM block table；
- KVBM lifecycle state；
- KVBM offload/onboard；
- NIXL 内部抽象；
- 直接接管 vLLM GPU KV block；
- 复制 LMCache 内部缓存算法；
- Router 直接读取 KV tensor。

---

## 6. 为什么去掉 KVBM / memory 后仍保留 KV Router

KVBM 是 KV block manager。  
KV Router 是路由决策器。

二者职责不同。

```text
KVBM:
  管真实 KV block 的分配、迁移、生命周期、offload/onboard。

KV Router:
  根据元数据判断请求发给哪个 worker 更划算。
```

去掉 KVBM 后，Router 仍然可以做：

- active load routing；
- prefix overlap routing；
- approximate prefix affinity；
- LMCache shared-cache-aware routing；
- topology-aware routing；
- NVIDIA / Huawei 硬件层级代价感知；
- future backend connector scoring。

但需要删除或降级：

- KVBM host tier 精确状态；
- KVBM disk tier 精确状态；
- KVBM remote tier 精确状态；
- KVBM block lifecycle；
- KVBM memory allocator 依赖；
- KVBM NIXL layer 依赖。

---

## 7. Router 与 Connector 的能力边界

### 7.1 Router 负责什么

Router 负责控制面决策：

```text
请求应该发给哪个 worker？
哪个 worker 复用 KV 的收益最大？
哪个 worker 当前负载最低？
哪个 worker 从共享缓存取 KV 的代价最低？
当前拓扑下哪个 worker 综合成本最低？
```

Router 维护或查询：

- worker registry；
- token block hash；
- prefix index；
- KV event view；
- active load state；
- shared cache metadata；
- topology information；
- cost model。

Router 不负责：

- 真实 KV tensor 读取；
- KV tensor 搬运；
- GPU/CPU/Disk transfer；
- connector load/save；
- block 物理生命周期；
- cache backend 内部索引。

### 7.2 Connector 负责什么

Connector 负责数据面操作：

```text
外部 KV 和 vLLM GPU paged KV buffer 之间如何 load/save？
KV 如何 offload？
KV 如何 onboard？
异步传输何时完成？
请求结束后哪些 block 不能立即释放？
```

Connector 管理：

- `start_load_kv()`；
- `wait_for_layer_load()`；
- `save_kv_layer()`；
- `wait_for_save()`；
- `request_finished()`；
- 外部 cache backend 交互；
- GPU/CPU/存储之间的数据搬运。

Connector 不负责：

- worker 选择；
- router 分数计算；
- 全局负载均衡；
- prefix indexer；
- topology-aware worker 选择。

### 7.3 Event Publisher / Relay 负责什么

Event Publisher / Relay 负责把 worker 内部 KV 状态变化发布到 Pagoda event plane：

```text
worker / backend cache manager
  ↓
native KV event 或 callback
  ↓
Pagoda KvEventPublisher / ZMQ relay
  ↓
Pagoda event plane
  ↓
Router KV Indexer
```

它不负责真实 KV 搬运，也不负责路由打分。

---

## 8. KV Event 机制设计

### 8.1 KV Event 的作用

KV Event 是路由元数据，不是真实 KV 数据。

它用于告诉 Router：

```text
哪个 worker / tier / pool 上存在或移除了某个 KV block。
```

KV Event 不应该包含：

- KV tensor；
- 真实文件路径；
- RDMA address；
- 对象存储 offset；
- connector 内部私有位置；
- LMCache / Mooncake 内部完整索引。

KV Event 应包含：

- block hash；
- parent hash；
- token block metadata；
- worker id；
- rank / device id；
- action；
- medium / tier；
- topology scope；
- optional model / lora 信息。

### 8.2 推荐事件类型

```rust
enum KvEventKind {
    BlockStored,
    BlockRemoved,
}

enum KvMedium {
    LocalDevice,
    LocalHost,
    LocalDisk,
    ExternalShared,
    RemoteHost,
    RemoteStorage,
    Unknown,
}

struct KvEvent {
    kind: KvEventKind,
    block_hash: String,
    parent_hash: Option<String>,
    token_count: usize,
    worker_id: String,
    model_id: String,
    rank_id: Option<u32>,
    device_id: Option<String>,
    medium: KvMedium,
    topology_scope: Option<TopologyScope>,
}
```

### 8.3 事件粒度

第一阶段建议只保留粗粒度层级：

```text
LocalDevice
ExternalShared
Unknown
```

后续可扩展：

```text
LocalHost
LocalDisk
RemoteHost
RemoteStorage
SameSupernode
CrossSupernode
```

### 8.4 事件发布者

事件的事实来源是实际管理 KV 的后端：

- vLLM；
- SGLang；
- HiCache；
- LMCache；
- KVBM；
- AscendStore；
- Mooncake adapter；
- 自研 backend cache manager。

但它们不一定直接持有 Pagoda publisher。

实际发布方式可以是：

```text
后端 native event
  ↓
ZMQ raw event
  ↓
Pagoda relay
  ↓
Pagoda event plane
```

或者：

```text
backend callback
  ↓
Pagoda KvEventPublisher
  ↓
Pagoda event plane
```

### 8.5 事件消费者

消费者是 Router 内部的 KV Indexer。

```text
Pagoda event plane
  ↓
KV Indexer
  ↓
radix tree / prefix tree
  ↓
Router cost model
```

KV Indexer 维护的是路由视图，不是真实存储视图。

---

## 9. ZMQ 转发机制设计

### 9.1 为什么需要 ZMQ relay

vLLM / SGLang 等后端可能已经有自己的 raw KV event system。  
这套事件系统通常通过 ZMQ 发布。

Pagoda 不应强制后端直接接入 Pagoda event plane，而应提供 relay：

```text
Backend native ZMQ publisher
  ↓
Pagoda ZMQ relay
  ↓
Pagoda event plane
  ↓
Router indexer
```

### 9.2 转发的含义

“转发”不是重新计算事件，也不是 Router 主动查询后端。

转发含义是：

```text
1. 后端已经发布 raw KV events；
2. Pagoda relay 订阅后端 ZMQ endpoint；
3. relay 将 raw event 转换为 Pagoda 标准 KvEvent；
4. relay 将 KvEvent 发布到 Pagoda event plane；
5. Router indexer 消费事件并更新视图。
```

### 9.3 推荐结构

```text
kv_events/
  mod.rs
  event.rs
  publisher.rs
  relay/
    mod.rs
    zmq.rs
    native.rs
  decoder/
    vllm.rs
    sglang.rs
    lmcache.rs
  sink/
    event_plane.rs
```

### 9.4 ZMQ relay 配置

```toml
[kv_events.zmq]
enabled = true
endpoint = "tcp://worker-0:20080"
topic = "kv-events"
source = "vllm"
```

### 9.5 多来源事件

不同后端事件格式可能不同，Pagoda 应做标准化：

```text
vLLM raw event
  ↓
VllmKvEventDecoder
  ↓
Pagoda KvEvent

SGLang raw event
  ↓
SglangKvEventDecoder
  ↓
Pagoda KvEvent

LMCache storage event
  ↓
LmcacheKvEventDecoder
  ↓
Pagoda KvEvent
```

---

## 10. LMCache 与 KV Events

### 10.1 LMCache 的事件定位

LMCache 可以生成 storage KV cache events，但 LMCache 不一定直接发布到 Pagoda event plane。

常见链路是：

```text
LMCache 完成 save/remove
  ↓
LMCache 生成 storage event
  ↓
传给 vLLM / SGLang
  ↓
vLLM / SGLang 用自己的 messaging system 发布
  ↓
Pagoda ZMQ relay
  ↓
Pagoda event plane
  ↓
Router indexer
```

### 10.2 第一阶段策略

第一阶段 LMCache-only 不强制要求完整 storage event 集成。

优先支持：

```text
vLLM native KV events
+
router approximate mode
+
LMCache shared metadata query 预留
```

后续再增强：

```text
LMCache enable_kv_events
+
vLLM/SGLang kv-events-config
+
Pagoda ZMQ relay
+
Router external shared cache scoring
```

### 10.3 没有事件时怎么办

如果没有真实 KV events，则 Router 可以使用 approximate mode：

```text
Router 把请求路由到 worker A
  ↓
Router 假设 worker A 在短时间内缓存了该 prefix
  ↓
后续相似请求优先发 worker A
  ↓
TTL 过期后清理预测状态
```

approximate mode 简单，但不精确。  
生产模式应逐步切换到 event-driven mode。

---

## 11. KV Indexer 设计

### 11.1 Indexer 职责

KV Indexer 负责把 KV events 转换成可查询的 prefix 视图。

它回答：

```text
给定请求的 token blocks，
哪些 worker / tier / topology scope 上已有对应 prefix？
```

### 11.2 推荐数据结构

```text
radix tree / prefix tree
  key: block_hash sequence
  value:
    - worker_id
    - medium
    - rank_id
    - topology_scope
    - timestamp
```

### 11.3 查询结果

```rust
struct PrefixHit {
    worker_id: String,
    matched_blocks: usize,
    medium: KvMedium,
    topology_scope: Option<TopologyScope>,
}
```

### 11.4 多层 residency

同一个 block 可能同时存在于多个 tier：

```text
block A:
  worker-1 LocalDevice
  worker-1 LocalHost
  shared-pool ExternalShared
```

Indexer 应允许同一 block 多 residency。

### 11.5 删除事件

当收到 `BlockRemoved`：

```text
block_hash + worker_id + medium
```

Indexer 只删除对应 residency，不应直接删除该 block 的所有位置。

---

## 12. Shared Cache 查询设计

### 12.1 为什么需要 shared query

KV events 主要维护 worker-local 视图。  
而 LMCache / Mooncake / AscendStore 这类 shared pool 可能是集群级共享缓存。

Router 如果只靠 worker events，可能不知道：

```text
某个 block 虽然不在当前 worker 本地，
但在 shared pool 中可以较低成本取回。
```

因此需要 shared cache metadata query。

### 12.2 查询内容

Router 查询 shared pool 时，只查询元数据：

```text
这些 block_hash 是否存在？
存在于哪个 pool？
相对候选 worker 的 locality 是什么？
取回代价大概是多少？
```

不读取真实 KV tensor。

### 12.3 推荐接口

```rust
trait SharedCacheMetadataClient {
    fn batch_lookup(&self, blocks: &[BlockHash]) -> Vec<SharedCacheHit>;
}

struct SharedCacheHit {
    block_hash: String,
    pool_id: String,
    locality: KvLocalityTier,
    estimated_cost: Option<f64>,
}
```

### 12.4 LMCache 第一阶段

第一阶段可先不实现 shared query，或只预留接口。

如果 LMCache backend 能提供 shared metadata，则接入：

```text
LMCache shared metadata query
  ↓
SharedCacheHit
  ↓
Router cost model
```

如果不能提供，则：

```text
shared_cache_hits = 0
```

---

## 13. Router Cost Model 设计

### 13.1 基础公式

Dynamo 风格的基础公式可以抽象为：

```text
score =
  prefill_load_scale * adjusted_prefill_blocks
  + decode_load
```

其中：

```text
adjusted_prefill_blocks =
  max(
    raw_prefill_blocks
    - cache_credits,
    0
  )
```

### 13.2 第一阶段公式

第一阶段只接 LMCache，且不做 KVBM memory tier。

推荐公式：

```text
cache_credits =
  local_device_hits * weight_local_device
  + external_shared_hits * weight_external_shared

score =
  prefill_load_scale * max(raw_prefill_blocks - cache_credits, 0)
  + decode_load
  + topology_penalty
```

### 13.3 后续扩展公式

后续可扩展：

```text
cache_credits =
  local_device_hits * weight_local_device
  + local_host_hits * weight_local_host
  + local_disk_hits * weight_local_disk
  + same_supernode_hits * weight_same_supernode
  + cross_supernode_hits * weight_cross_supernode
  + external_shared_hits * weight_external_shared
```

最终：

```text
score =
  prefill_load_scale * max(raw_prefill_blocks - cache_credits, 0)
  + decode_load
  + queue_penalty
  + topology_transfer_penalty
```

### 13.4 命中收益与传输惩罚拆分

更准确的模型应将命中收益和传输成本拆开：

```text
effective_credit[tier] =
  recompute_cost_per_block - transfer_cost[tier]
```

如果：

```text
transfer_cost[tier] >= recompute_cost_per_block
```

则该 tier 的命中不应加分，甚至应加惩罚。

### 13.5 推荐配置

```toml
[router.cost]
prefill_load_scale = 1.0
decode_load_scale = 1.0
queue_penalty_scale = 1.0
topology_penalty_scale = 1.0

[router.cache_credit]
local_device = 1.00
local_host = 0.70
local_disk = 0.25
external_shared = 0.50

[router.cache_credit.nvidia]
same_host_device = 0.90
same_node_nvlink = 0.85
cross_node_rdma = 0.35

[router.cache_credit.huawei]
same_device = 1.00
same_host = 0.90
same_supernode_fast = 0.75
same_supernode_slow = 0.55
cross_supernode = 0.30
shared_storage = 0.15
```

所有权重必须可配置，不应写死。

---

## 14. Huawei 384 超节点适配

### 14.1 问题背景

Huawei 384 超节点不是普通多机多卡结构。  
在这种机器上，不同层级之间的通信代价明显不同。

因此，KV 命中不能只分：

```text
device / host / disk / shared
```

还需要考虑：

- 同 NPU；
- 同 host；
- 同超节点高速域；
- 同超节点慢速域；
- 跨超节点；
- 本地 CPU；
- 远端 CPU；
- 本地 SSD；
- 共享存储；
- 对象存储。

### 14.2 拓扑层级

推荐定义通用拓扑层级：

```rust
enum KvLocalityTier {
    LocalDevice,
    SameHostDevice,
    SameSupernodeFast,
    SameSupernodeSlow,
    CrossSupernode,
    LocalHostMemory,
    RemoteHostMemory,
    LocalDisk,
    SharedStorage,
    Unknown,
}
```

### 14.3 Huawei 映射

在 Huawei 384 超节点中，可映射为：

```text
LocalDevice:
  当前 worker 所在 NPU HBM

SameHostDevice:
  同主机 / 同板卡域内其它 NPU

SameSupernodeFast:
  同 384 超节点内低代价高速互联域

SameSupernodeSlow:
  同超节点内跨更高层通信域

CrossSupernode:
  跨超节点 RoCE / RDMA / fabric

SharedStorage:
  LMCache / Mooncake / AscendStore / 对象存储
```

### 14.4 拓扑来源

Router 不应写死硬件拓扑，应通过 TopologyProvider 获取：

```text
worker_id
node_id
host_id
device_id
supernode_id
hccs_domain_id
roce_domain_id
fabric_domain_id
rack_id
availability_zone
```

来源可以是：

- worker registration metadata；
- Kubernetes labels；
- 静态配置文件；
- CMDB；
- 机器启动探测；
- 平台侧调度系统。

### 14.5 Worker 注册信息

```json
{
  "worker_id": "worker-17",
  "backend": "vllm-ascend",
  "topology": {
    "vendor": "huawei",
    "supernode_id": "sn-0",
    "host_id": "host-23",
    "device_id": "npu-5",
    "hccs_domain": "hccs-2",
    "roce_domain": "roce-0",
    "fabric_domain": "fabric-a"
  }
}
```

---

## 15. NVIDIA 适配

### 15.1 NVIDIA 拓扑

NVIDIA 场景中应支持：

- local GPU HBM；
- same host GPU；
- NVLink domain；
- NVSwitch domain；
- cross-node RDMA；
- GPUDirect Storage；
- object store；
- LMCache shared backend；
- future NIXL backend。

### 15.2 NVIDIA 映射

```text
LocalDevice:
  当前 GPU HBM

SameHostDevice:
  同主机其它 GPU

SameSupernodeFast:
  NVLink / NVSwitch 域

CrossSupernode:
  跨节点 RDMA / InfiniBand

LocalDisk:
  本地 NVMe / GDS

SharedStorage:
  LMCache / Mooncake / Object Store
```

### 15.3 NIXL 预留

第一阶段不做 NIXL 抽象，但预留：

```rust
enum KvTransferBackend {
    Native,
    Nixl,
    Mooncake,
    Hixl,
    Hccl,
    Rdma,
    Custom(String),
}
```

---

## 16. 通用硬件适配能力

Pagoda 不应把 Router 写死为 NVIDIA 或 Huawei。

推荐：

```rust
trait TopologyProvider {
    fn location_of_worker(&self, worker_id: &str) -> Option<WorkerLocation>;

    fn locality_between(
        &self,
        candidate_worker: &WorkerLocation,
        cache_location: &CacheLocation,
    ) -> KvLocalityTier;
}
```

```rust
struct WorkerLocation {
    vendor: HardwareVendor,
    supernode_id: Option<String>,
    host_id: Option<String>,
    device_id: Option<String>,
    fabric_domain_id: Option<String>,
    rack_id: Option<String>,
    zone_id: Option<String>,
}
```

```rust
enum HardwareVendor {
    Nvidia,
    Huawei,
    Amd,
    Intel,
    Custom(String),
}
```

这样未来可以适配：

- AMD GPU；
- Intel GPU；
- 自研 NPU；
- CPU-only；
- 混合异构集群；
- 云厂商专用超节点。

---

## 17. Aggregated 与 Disaggregated 模式

### 17.1 Aggregated 模式

Aggregated 模式中，同一个 worker 执行 prefill 和 decode。

```text
Frontend
  ↓
KV Router
  ↓
vLLM worker
  ├── prefill
  ├── decode
  └── LMCache connector
```

第一阶段优先实现 aggregated。

Router 主要考虑：

- worker active decode load；
- local device prefix overlap；
- LMCache shared hit；
- topology penalty。

### 17.2 Disaggregated 模式

Disaggregated 模式中，prefill worker 和 decode worker 分离。

```text
Frontend / Router
  ↓
Prefill worker
  ↓ transfer metadata
Decode worker
```

第一阶段只预留结构。

后续支持：

- prefill router；
- decode router；
- transfer metadata；
- LMCache + NIXL；
- LMCache + Mooncake；
- AscendStore；
- KVBM；
- topology-aware P/D placement。

---

## 18. 模块设计

### 18.1 推荐目录结构

```text
lib/runtime/src/kv_router/
  mod.rs
  config.rs
  router.rs
  worker.rs
  request.rs
  scoring/
    mod.rs
    cost_model.rs
    cache_credit.rs
    active_load.rs
    topology_penalty.rs
  indexer/
    mod.rs
    radix.rs
    prefix.rs
    residency.rs
  events/
    mod.rs
    event.rs
    publisher.rs
    consumer.rs
    relay/
      mod.rs
      zmq.rs
      native.rs
    decoder/
      mod.rs
      vllm.rs
      sglang.rs
      lmcache.rs
  topology/
    mod.rs
    provider.rs
    model.rs
    static_provider.rs
    kubernetes_provider.rs
    nvidia.rs
    huawei.rs
  shared_cache/
    mod.rs
    client.rs
    lmcache.rs
    mooncake.rs
    noop.rs
  connectors/
    mod.rs
    lmcache.rs
    kvbm.rs
    hicache.rs
    ascend_store.rs
```

### 18.2 第一阶段实际实现

第一阶段实际实现：

```text
kv_router/
  config.rs
  router.rs
  worker.rs
  scoring/
  indexer/
  topology/
  shared_cache/noop.rs
  connectors/lmcache.rs
```

暂不实现：

```text
connectors/kvbm.rs
connectors/hicache.rs
connectors/ascend_store.rs
shared_cache/mooncake.rs
memory/
```

保留空 trait / enum 扩展点即可。

---

## 19. 配置设计

### 19.1 Router 基础配置

```toml
[kv_router]
enabled = true
mode = "kv"
serving_mode = "aggregated"
event_mode = "approximate"
```

### 19.2 LMCache 配置

```toml
[kv_router.connector.lmcache]
enabled = true
connector_name = "LMCacheMPConnector"
kv_role = "kv_both"
server_endpoint = "127.0.0.1:65432"
l1_size_gb = 100
eviction_policy = "LRU"
```

### 19.3 KV Event 配置

```toml
[kv_router.events]
enabled = false
source = "vllm"
transport = "zmq"
topic = "kv-events"
endpoint = "tcp://127.0.0.1:20080"
```

### 19.4 Topology 配置

```toml
[kv_router.topology]
enabled = true
provider = "static"
vendor = "huawei"

[kv_router.topology.static.workers.worker_0]
supernode_id = "sn0"
host_id = "host0"
device_id = "npu0"
fabric_domain_id = "hccs0"
```

### 19.5 Cost 配置

```toml
[kv_router.cost]
prefill_load_scale = 1.0
decode_load_scale = 1.0
topology_penalty_scale = 1.0

[kv_router.cost.cache_credit]
local_device = 1.0
external_shared = 0.5
same_supernode_fast = 0.75
same_supernode_slow = 0.55
cross_supernode = 0.30
shared_storage = 0.15
```

---

## 20. 路由流程

### 20.1 Aggregated LMCache-only 流程

```text
1. 请求进入 frontend
2. tokenizer 生成 token_ids
3. router 将 token_ids 切成 block
4. 计算 block hash / prefix hash
5. 查询 KV indexer
6. 可选查询 LMCache shared metadata
7. 查询 worker active load
8. 查询 worker topology
9. cost model 计算每个 worker 分数
10. 选择分数最低 worker
11. 请求发送给 vLLM worker
12. worker 使用 LMCache connector load/save KV
13. 如果启用 KV events，则 worker 发布事件
14. router indexer 更新视图
```

### 20.2 Disaggregated 预留流程

```text
1. 请求进入 router
2. router 选择 prefill worker
3. prefill worker 计算 KV
4. connector / transfer backend 生成 transfer metadata
5. router 将 metadata 注入 decode 请求
6. router 选择 decode worker
7. decode worker 取回 KV 并继续 decode
```

第一阶段不实现完整 P/D 分离，仅保留数据结构。

---

## 21. 评分示例

假设请求有：

```text
raw_prefill_blocks = 8
```

候选 worker 状态：

```text
local_device_hits = 3
external_shared_hits = 2
decode_load = 4
topology_penalty = 0.5
```

权重：

```text
local_device = 1.0
external_shared = 0.5
prefill_load_scale = 1.0
```

计算：

```text
cache_credits =
  3 * 1.0
  + 2 * 0.5
= 4.0

adjusted_prefill_blocks =
  max(8 - 4.0, 0)
= 4.0

score =
  1.0 * 4.0
  + 4
  + 0.5
= 8.5
```

分数越低，worker 越优先。

---

## 22. KVBM / HiCache / Mooncake 后续接入预留

### 22.1 KVBM 预留

未来接入 KVBM 时新增：

```text
connectors/kvbm.rs
shared_cache/kvbm.rs
KvMedium::LocalHost
KvMedium::LocalDisk
KvMedium::RemoteStorage
```

但必须保证：

```text
KVBM 是可选 connector；
不污染 LMCache-only 路径；
不强制引入 memory 模块。
```

### 22.2 HiCache 预留

未来接入 HiCache 时新增：

```text
events/decoder/sglang.rs
connectors/hicache.rs
KvMedium::CpuPinned
```

HiCache tier transition events 可映射为：

```text
store(GPU)         -> BlockStored(LocalDevice)
store(CPU_PINNED)  -> BlockStored(LocalHost)
remove(GPU)        -> BlockRemoved(LocalDevice)
remove(CPU_PINNED) -> BlockRemoved(LocalHost)
```

### 22.3 Mooncake 预留

未来接入 Mooncake 时新增：

```text
shared_cache/mooncake.rs
KvTransferBackend::Mooncake
KvMedium::ExternalShared
```

Router 可并行查询：

```text
local radix tree
+
Mooncake shared pool metadata
```

### 22.4 AscendStore 预留

未来接入 AscendStore 时新增：

```text
connectors/ascend_store.rs
KvTransferBackend::Hixl
KvTransferBackend::Hccl
HardwareVendor::Huawei
```

---

## 23. 设计约束

### 23.1 Router 不读取真实 KV

Router 只处理元数据。

禁止：

```text
Router 直接读 KV tensor
Router 直接访问 LMCache 文件路径
Router 直接依赖对象存储 offset
Router 直接操作 RDMA address
```

### 23.2 Connector 不计算最终路由分数

Connector 可以提供事实：

```text
某个 KV block 可命中
某个 tier 可访问
某次 load/save 完成
```

但不能决定：

```text
请求应该发给哪个 worker
```

### 23.3 Event 不携带物理位置

Event 只携带路由元数据。  
真实位置由 cache backend 内部索引管理。

### 23.4 拓扑权重必须可配置

NVIDIA、Huawei、其它平台代价不同。  
权重不允许写死在代码里。

### 23.5 第一阶段保持简单

第一阶段只做：

```text
LMCache connector
KV Router
Topology-aware score framework
Event relay 预留
Shared query 预留
```

不做 KVBM/memory。

---

## 24. 验收标准

第一阶段完成后，应满足：

- 可以在不引入 KVBM/memory 的情况下编译；
- 可以生成 LMCache vLLM connector 配置；
- 可以启用或关闭 KV Router；
- 可以使用 least-loaded / kv / approximate 模式；
- 可以维护 worker active load；
- 可以维护基础 prefix index；
- 可以配置 NVIDIA / Huawei topology provider；
- 可以根据不同 topology tier 调整 router score；
- 可以预留 ZMQ event relay；
- 可以预留 LMCache shared metadata query；
- 不直接读取真实 KV tensor；
- 不依赖 KVBM block table；
- 不依赖 KVBM memory lifecycle。

---

## 25. 总结

Pagoda 的 KV Router 应设计成通用控制面模块，而不是 KVBM 的附属模块。

当前阶段的正确取舍是：

```text
不做 KVBM；
不做 memory；
保留通用 KV Router；
第一阶段只接 LMCache；
支持 NVIDIA / Huawei 拓扑打分；
预留其它硬件和后端适配能力。
```

最终架构目标是：

```text
Pagoda KV Router
  ↓
统一路由元数据视图
  ↓
统一 topology-aware cost model
  ↓
LMCache / KVBM / HiCache / Mooncake / AscendStore 可插拔
  ↓
NVIDIA / Huawei / 其它硬件平台可适配
```

这样既能满足当前 LMCache-only 的简化目标，也不会封死后续做 KVBM、HiCache、Mooncake、AscendStore 或其它机器适配的空间。
