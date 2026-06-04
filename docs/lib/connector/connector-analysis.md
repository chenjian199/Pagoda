# Pagoda KV Connector 技术分析文档：vLLM 接入、LMCache、KVBM 与适配取舍

## 1. 文档目的

本文档用于梳理 Pagoda 在接入 vLLM 后端时，围绕 KV cache connector 的整体设计取舍。

重点回答以下问题：

- vLLM 的 KV connector 机制是怎样工作的；
- LMCache connector 与 KVBM connector 分别承担什么职责；
- aggregated serving 和 disaggregated serving 下 KV 如何流转；
- vLLM、LMCache、KVBM 各自管理哪些内存层级；
- 请求结束后 `request_finished()` 为什么会影响 KV block 释放；
- 如果 Pagoda 当前只保留 LMCache 接入机制，是否还需要保留 Dynamo/KVBM 的 memory 模块；
- 面向 NVIDIA 和 Huawei Ascend 环境时，抽象层应如何设计。

本文档不是实现代码，而是架构分析与 Pagoda 第一阶段适配计划。

---

## 2. 背景：为什么需要 KV connector

LLM 推理过程中，每个 token 在每一层 attention 中都会产生 Key / Value：

```text
token
  ↓
attention layer
  ↓
K/V cache
```

后续生成 token 时，模型不需要重新计算历史 token 的 K/V，而是复用已经生成的 KV cache。

因此，KV cache 直接影响：

- 首 token 延迟；
- 长上下文吞吐；
- GPU 显存占用；
- prefix cache 命中；
- prefill / decode 分离；
- 跨请求、跨 worker、跨机器复用能力。

vLLM 原生使用 GPU paged KV cache 管理当前请求的 KV block。  
但当上下文变长、请求变多、需要跨请求复用或 P/D 分离时，仅依赖 GPU 显存会不够。因此需要 connector 把 vLLM 的 KV block 与外部 KV 系统连接起来。

整体关系是：

```text
vLLM GPU paged KV buffer
  ↑↓ load / save
KV connector
  ↑↓
外部 KV 系统
```

外部 KV 系统可以是：

- LMCache；
- Dynamo KVBM；
- FlexKV；
- Ascend Store / Mooncake；
- 其它 KV cache pool。

---

## 3. vLLM KV connector 机制

vLLM 的 connector 不是传统数据库 CRUD 接口，而是一组围绕 KV block 生命周期的回调接口。

它分为两侧：

```text
Scheduler-side
Worker-side
```

### 3.1 Scheduler-side

Scheduler-side 运行在 vLLM 调度侧，负责决策和元数据绑定。

它主要回答：

```text
这个请求有多少 token 可以命中外部 KV？
vLLM 分配了哪些本地 block？
这些 block 与外部 KV block 如何对应？
请求结束后这些 block 能否释放？
```

典型接口包括：

```text
get_num_new_matched_tokens()
update_state_after_alloc()
update_connector_output()
request_finished()
take_events()
```

含义：

| 接口 | 作用 |
| --- | --- |
| `get_num_new_matched_tokens()` | 查询远端或外部 KV cache 中可复用的新 token 数量 |
| `update_state_after_alloc()` | vLLM CacheManager 分配 block 后，更新 connector 状态 |
| `update_connector_output()` | 接收 worker-side 返回的 connector 结果并更新调度状态 |
| `request_finished()` | 请求结束时决定 KV block 是否可立即释放 |
| `take_events()` | 取出 connector 收集到的 KV cache 事件 |

### 3.2 Worker-side

Worker-side 运行在真正执行模型 forward 的 worker 中，负责实际搬运 KV。

典型接口包括：

```text
start_load_kv()
wait_for_layer_load()
save_kv_layer()
wait_for_save()
get_finished()
```

含义：

| 接口 | 作用 |
| --- | --- |
| `start_load_kv()` | 在 forward 前开始把外部 KV 加载到 vLLM paged KV buffer |
| `wait_for_layer_load()` | 等待某一层 KV 加载完成 |
| `save_kv_layer()` | 把当前层 KV 从 vLLM paged KV buffer 保存到 connector |
| `wait_for_save()` | 等待所有异步保存完成 |
| `get_finished()` | 通知哪些异步传输已经完成，可以释放相关资源 |

---

## 4. 请求生命周期中的 KV connector 流程

一次请求在启用 connector 后，大致经历以下阶段。

### 4.1 请求进入调度器

```text
request
  ↓
prompt token ids
  ↓
vLLM scheduler
```

调度器询问 connector：

```text
这些 prompt token 有多少已经存在于外部 KV cache？
```

如果命中：

```text
外部 cache 中已有部分 KV
  ↓
可以减少 prefill 计算
```

如果未命中：

```text
正常 prefill
```

### 4.2 vLLM 分配本地 GPU KV block

vLLM 自己仍然负责分配 GPU paged KV block。

```text
vLLM CacheManager
  ↓
分配本地 GPU KV block
```

connector 不替代 vLLM 的 GPU paged KV buffer。  
connector 只记录：

```text
请求 token 范围
本地 block id
外部 cache key
需要 load 的 block
需要 save 的 block
```

### 4.3 forward 前加载已有 KV

如果外部 KV 命中，worker-side connector 会执行：

```text
外部 KV 系统
  ↓ start_load_kv
vLLM GPU paged KV buffer
```

每层 attention 使用 KV 前，可以等待该层 KV 已经加载完成：

```text
wait_for_layer_load(layer)
```

### 4.4 模型计算缺失 KV

未命中的 token 正常走 vLLM prefill：

```text
token ids
  ↓
model forward
  ↓
生成 K/V
  ↓
写入 vLLM GPU paged KV buffer
```

### 4.5 保存新生成 KV

新生成的 KV 可以被 connector 保存到外部 KV 系统：

```text
vLLM GPU paged KV buffer
  ↓ save_kv_layer
KV connector
  ↓
外部 KV 系统
```

保存可能是异步执行。  
因此 forward 结束前或请求结束前，需要通过 `wait_for_save()` 或 `get_finished()` 确认异步保存已完成。

---

## 5. `request_finished()` 的意义

`request_finished()` 是请求结束时的资源所有权确认点。

请求结束后，vLLM 会认为：

```text
这个请求用过的 KV block 可以释放了
```

但如果 connector 正在异步保存、迁移、注册这些 block，就不能立即释放。

原因是：

```text
vLLM GPU block 中的数据还没被外部系统完整拷走
  ↓
如果 vLLM 立即释放 block
  ↓
新请求可能复用并覆盖这个 block
  ↓
connector 读到的就会是脏数据
```

所以 `request_finished()` 的核心问题是：

```text
vLLM:
这些 block 我能不能马上释放？

connector:
可以，立即释放。
或者：
不行，我还在异步处理，等我完成后再释放。
```

可以理解为：

```text
request_finished()
= 请求结束时的 KV block 释放协商接口
```

它不是普通的 delete 操作，而是：

```text
释放前的所有权交接
```

---

## 6. 用 CRUD 类比 connector

虽然 connector 不是数据库 CRUD，但可以类比：

| CRUD | connector 行为 | 含义 |
| --- | --- | --- |
| Create | `save_kv_layer()` | 新生成 KV 写入外部 cache |
| Read | `get_num_new_matched_tokens()` + `start_load_kv()` | 查询并加载已有 KV |
| Update | `update_state_after_alloc()` / `update_connector_output()` | 更新请求、block、传输状态 |
| Delete | `request_finished()` / eviction / cleanup | 请求结束、释放、淘汰或延迟释放 |

注意：真实系统中它们是 KV block 生命周期回调，不是面向记录的 CRUD API。

---

## 7. vLLM 自身管理的内存层级

vLLM 原生最核心管理的是：

```text
GPU HBM / VRAM
  └── paged KV buffer
```

也就是 PagedAttention 使用的 GPU KV block。

vLLM 管理：

- GPU KV block 分配；
- 请求到 block 的映射；
- block table；
- 当前 attention 需要读取的 K/V；
- 请求调度和本地 block 生命周期。

简化结构：

```text
vLLM
  ↓
GPU HBM
  ↓
paged KV cache
  ├── key blocks
  └── value blocks
```

重要结论：

> attention kernel 真正读取的是 vLLM GPU paged KV buffer。  
> LMCache 或 KVBM 中的 KV 都必须先 load / onboard 回 vLLM GPU KV buffer，才能参与当前推理。

---

## 8. LMCache 的层级与职责

LMCache 是外部 KV cache 系统。  
它负责保存、查询、复用和持久化 KV cache。

在 Dynamo 集成中，LMCache 可以作为 sidecar 运行：

```text
vLLM worker
  ↓
LMCacheMPConnector
  ↓
lmcache server
```

LMCache 的典型层级是：

```text
vLLM GPU paged KV buffer
  ↑↓ connector load/save
LMCache L1 memory cache
  ↑↓ optional
LMCache L2 persistent backend
```

### 8.1 LMCache L1 memory cache

L1 是内存缓存层。

作用：

- 保存常用 KV；
- 快速命中；
- 减少 prefill 重算；
- 支持跨请求复用。

示例：

```bash
lmcache server --l1-size-gb 100 --eviction-policy LRU
```

含义：

```text
LMCache 启动 100GB L1 内存缓存
使用 LRU 淘汰策略
```

### 8.2 LMCache L2 persistent backend

L2 是可选持久化后端。

可用于：

- 容量扩展；
- 跨进程共享；
- 跨机器共享；
- 长期缓存；
- 冷 KV 存储。

常见 L2 后端：

| 后端 | 含义 |
| --- | --- |
| POSIX filesystem | 普通文件系统或共享挂载路径 |
| GDS / GDS_MT | NVIDIA GPUDirect Storage 路径 |
| HF3FS / 3FS | 分布式文件系统 |
| Object Store / S3 | 对象存储 |
| Azure Blob | Azure 云对象存储 |
| Redis / Valkey | KV 服务型后端 |
| Mooncake | 分布式 KV cache storage 系统 |
| NIXL | 传输和存储适配层，而非具体磁盘 |

### 8.3 NIXL 作为 LMCache storage backend 的含义

“NIXL 作为 storage backend”不表示 NIXL 自己是磁盘。

更准确地说：

```text
LMCache
  ↓
NIXL storage backend
  ↓
nixl_backend
  ↓
POSIX / GDS / OBJ / Azure Blob / ...
```

NIXL 在这里是高性能传输和后端适配层。  
真正保存数据的位置由 `nixl_backend` 决定。

例如：

```yaml
extra_config:
  enable_nixl_storage: true
  nixl_backend: POSIX
  nixl_path: /mnt/nixl/cache/
```

含义：

```text
LMCache 使用 NIXL 存储通道
NIXL 使用 POSIX 后端
KV 最终写入 /mnt/nixl/cache/
```

---

## 9. KVBM 的层级与职责

KVBM 是 Dynamo 内建的 KV Block Manager。  
它不是普通外部 cache，而是一个多级 KV block 管理系统。

KVBM 关注：

- KV block table；
- block allocation；
- block layout；
- lifecycle state；
- block reuse；
- eviction；
- offload / onboard；
- 远程共享；
- NIXL 传输。

KVBM 可抽象为三层：

```text
LLM Inference Runtime Layer
  ↓
KVBM Logic Layer
  ↓
NIXL Layer
```

### 9.1 Runtime Layer

Runtime Layer 负责接入 vLLM、TensorRT-LLM 等推理后端。

在 vLLM 中典型配置是：

```bash
--kv-transfer-config '{
  "kv_connector": "DynamoConnector",
  "kv_role": "kv_both",
  "kv_connector_module_path": "kvbm.vllm_integration.connector"
}'
```

它把推理后端事件转成 KVBM 可理解的 block-oriented memory 操作。

### 9.2 KVBM Logic Layer

KVBM Logic Layer 负责：

```text
block lookup
memory allocation
block layout management
lifecycle state transition
block reuse
eviction
metadata management
```

这是 KVBM 和 LMCache 最大的区别之一。  
KVBM 更像内存管理器，LMCache 更像外部缓存系统。

### 9.3 NIXL Layer

NIXL Layer 负责数据搬运和远程访问。

它处理：

- GPU 到 GPU；
- GPU 到 CPU；
- CPU 到 GPU；
- CPU 到 disk；
- disk 到 GPU；
- 本机到远端；
- RDMA / NVLink / UCX 等传输；
- 存储后端插件。

---

## 10. KVBM 的 G1 / G2 / G3 / G4

KVBM 常被理解为多级 KV block 管理：

```text
G1: GPU HBM
G2: Host DRAM / pinned CPU memory
G3: Local SSD / disk
G4: Remote storage / remote memory / object store / cloud storage
```

### 10.1 G1：GPU HBM

G1 是 GPU 高带宽显存。

在 vLLM 场景下，G1 主要对应：

```text
vLLM GPU paged KV buffer
```

attention kernel 真正从这里读 KV。

### 10.2 G2：Host DRAM / pinned CPU memory

G2 是主机内存，尤其是 pinned CPU memory。

用途：

- GPU KV 的短期 offload；
- 热数据缓存；
- GPU 和更慢存储之间的缓冲层；
- 低于 GPU、高于磁盘的中间层。

### 10.3 G3：Local SSD / disk

G3 是本地 SSD 或磁盘。

用途：

- 更大容量；
- 保存冷 KV；
- 减少 GPU / CPU 内存压力；
- 支持长上下文场景。

### 10.4 G4：Remote storage / remote memory / cloud storage

G4 是远端资源：

- 远端 RDMA 内存；
- 分布式文件系统；
- 对象存储；
- 云存储；
- 跨节点 KV pool。

用途：

- 集群级共享；
- 大容量持久化；
- 跨机器复用；
- 远程 offload。

---

## 11. KVBM 是否替换 vLLM GPU paged KV buffer

不替换。

更准确地说：

```text
vLLM 继续管理执行期 GPU paged KV buffer
KVBM 管理外部 KV block 的 offload / onboard / lifecycle
```

也就是：

```text
vLLM GPU paged KV buffer
  ↓ offload
KVBM G2 / G3 / G4

KVBM G2 / G3 / G4
  ↓ onboard
vLLM GPU paged KV buffer
```

vLLM attention 仍然需要从自己的 GPU paged KV buffer 读取 KV。  
KVBM 不能让 vLLM attention 直接从磁盘或远端对象存储里算 attention。

因此：

> KVBM 接管的是外部 KV block 管理，不是替换 vLLM 的 GPU 执行层。

---

## 12. LMCache connector 设计

LMCache connector 的定位是：

```text
vLLM
  ↓
LMCache connector
  ↓
LMCache cache engine
```

它负责：

- 查询可复用 KV；
- 从 LMCache 加载 KV 到 vLLM；
- 把 vLLM 新生成 KV 保存到 LMCache；
- 支持 L1 / L2 多级缓存；
- 支持跨实例共享；
- 支持 P/D 分离中的 KV 复用和 offload。

### 12.1 Aggregated 模式

Aggregated 模式中，同一个 worker 同时执行 prefill 和 decode。

结构：

```text
Frontend
  ↓
vLLM worker
  ├── prefill
  ├── decode
  └── LMCacheMPConnector
        ↓
      lmcache server
        ↓
      L1 / L2 cache
```

典型配置：

```bash
lmcache server --l1-size-gb 100 --eviction-policy LRU

python -m dynamo.vllm \
  --model <model_name> \
  --disable-hybrid-kv-cache-manager \
  --kv-transfer-config '{"kv_connector":"LMCacheMPConnector","kv_role":"kv_both"}'
```

`kv_both` 表示：

```text
该 worker 既可以加载 KV，也可以保存 KV
```

### 12.2 Disaggregated 模式

Disaggregated 模式中，prefill worker 和 decode worker 分离。

LMCache 通常和 NIXL 配合：

```text
Prefill worker
  ├── LMCacheConnectorV1：保存 / 复用 / offload KV
  └── NixlConnector：把 KV 传给 decode worker

Decode worker
  └── 根据 transfer metadata 接收 / 加载 KV
```

职责区分：

```text
LMCache:
  负责缓存复用、offload、L1/L2 存储

NIXL:
  负责 P -> D 的高速 KV transfer
```

### 12.3 LMCache 跨机复用

LMCache 能实现跨实例、跨机器复用，但方式有多种：

```text
方式一：多个 worker 连接同一个 LMCache server
方式二：多个 worker 共享 L2 后端
方式三：使用对象存储、共享文件系统、Mooncake 等分布式后端
方式四：P/D 分离中通过 NIXL 做点对点 KV transfer
```

需要注意：

> LMCache 跨机复用不一定依赖 NIXL。  
> 共享 L2 后端也可以实现跨机复用。  
> P/D 分离中的低延迟 KV 传输更适合使用 NIXL。

---

## 13. KVBM connector 设计

KVBM connector 的定位是：

```text
vLLM / TensorRT-LLM
  ↓
DynamoConnector
  ↓
KVBM
  ↓
NIXL + CPU / Disk / Remote tiers
```

它负责：

- 将推理后端 KV 事件转为 KVBM block 操作；
- 管理 block metadata；
- 管理多级内存；
- 执行 offload / onboard；
- 配合 NIXL 做跨设备、跨机器传输；
- 支持 Dynamo 原生 KV-aware routing；
- 支持 aggregated 和 disaggregated serving。

### 13.1 Aggregated 模式

结构：

```text
vLLM worker
  ├── prefill
  ├── decode
  └── DynamoConnector
        ↓
      KVBM
        ↓
      CPU / Disk / Remote tiers
```

典型配置：

```bash
--kv-transfer-config '{
  "kv_connector":"DynamoConnector",
  "kv_role":"kv_both",
  "kv_connector_module_path":"kvbm.vllm_integration.connector"
}'
```

### 13.2 Disaggregated 模式

结构：

```text
Router
  ↓
Prefill worker
  ├── 计算 prompt KV
  ├── KVBM：offload / register / cache KV blocks
  └── NIXL：把 decode 所需 KV 传给 decode worker

Decode worker
  ├── 接收 / onboard KV
  └── 继续 decode
```

KVBM 的重点不是只保存 KV，而是管理：

```text
block 是否存在
block 在哪一层
block 是否可复用
block 是否正在迁移
block 是否可释放
block 是否被远端引用
```

---

## 14. LMCache 与 KVBM 对比

| 维度 | LMCache | KVBM |
| --- | --- | --- |
| 定位 | 外部 KV cache 系统 | Dynamo 内建 KV Block Manager |
| 接入方式 | LMCache connector / LMCacheMPConnector | DynamoConnector + kvbm connector module |
| 主要目标 | KV 复用、offload、跨实例共享 | 多级 block 管理、生命周期、offload/onboard、远程共享 |
| 内存模型 | L1 memory + L2 backend | G1/G2/G3/G4 多级内存/存储 |
| 是否替换 vLLM GPU KV | 否 | 否 |
| 是否需要自建 block manager | 否，LMCache 自己负责 | 是，KVBM 就是 block manager |
| 是否依赖 NIXL | 可选 | 深度绑定 NIXL 层 |
| 跨机复用 | 可通过共享服务、L2、对象存储、Mooncake、NIXL 等实现 | 通过 KVBM + NIXL + 远程内存/存储实现 |
| 适合场景 | 想快速接入成熟外部 KV cache | 想实现 Dynamo 原生多级 KV block 管理 |
| Pagoda 第一阶段建议 | 保留 | 暂不保留完整 KVBM |

---

## 15. Aggregated 与 Disaggregated 的区别

### 15.1 Aggregated serving

Aggregated 模式：

```text
同一个 worker 同时做 prefill 和 decode
```

请求流：

```text
Frontend
  ↓
Worker
  ├── prefill
  └── decode
```

特点：

- 架构简单；
- 不需要 P/D 跨机 KV transfer；
- connector 主要用于跨请求复用和 offload；
- LMCache / KVBM 都可在该模式下使用。

### 15.2 Disaggregated serving

Disaggregated 模式：

```text
prefill worker 和 decode worker 分离
```

请求流：

```text
Frontend / Router
  ↓
Prefill worker
  ↓ 生成 KV + transfer metadata
Router
  ↓ 注入 metadata
Decode worker
  ↓
继续 decode
```

特点：

- prefill 和 decode 可独立扩缩容；
- 需要跨 worker 传输 KV；
- 需要 router 管理 transfer metadata；
- 需要 NIXL / Mooncake / Ascend Store 等高速传输能力；
- 系统复杂度明显更高。

---

## 16. 跨机集群级管理

跨机管理分为控制面和数据面。

### 16.1 控制面

控制面负责：

- worker 注册；
- worker 发现；
- prefill worker 选择；
- decode worker 选择；
- transfer metadata 传递；
- KV-aware routing；
- 请求状态管理。

典型组件：

```text
Frontend
Router
PrefillRouter
Discovery
Event plane
Metadata
```

### 16.2 数据面

数据面负责真正传输 KV：

```text
Prefill GPU / CPU memory
  ↓
传输层
  ↓
Decode GPU / CPU memory
```

可选传输层包括：

- NIXL；
- RDMA / UCX；
- NVLink；
- Mooncake；
- Ascend Store；
- HIXL / ascend_direct；
- 共享 L2 后端；
- 对象存储或分布式文件系统。

### 16.3 LMCache 跨机方式

LMCache 跨机可通过：

```text
共享 LMCache server
共享 L2 后端
对象存储
共享文件系统
Mooncake
NIXL storage backend
NIXL P2P transfer
```

### 16.4 KVBM 跨机方式

KVBM 跨机依赖：

```text
KVBM block metadata
NIXL remote registration
NIXL transfer
远程内存或远程存储
Dynamo discovery / routing
```

---

## 17. Huawei Ascend 环境下的对应关系

NIXL 是 NVIDIA Inference Xfer Library，属于 NVIDIA 生态。

Huawei Ascend 环境中没有一个与 NIXL 完全一一对应的组件。  
更接近的是一组能力组合：

```text
HCCL
HCCN / RoCE
Fabric Memory
HIXL / ascend_direct
Ascend Store
Mooncake
Memcache
Yuanrong
```

在 vLLM Ascend 路线中，更常见的 KV pool 方案是：

```text
vLLM Ascend
  ↓
AscendStoreConnector
  ↓
KV Pool backend
  ├── mooncake
  ├── memcache
  └── yuanrong
```

如果 Pagoda 未来要适配 Ascend，不建议把抽象写死为 NIXL。  
建议抽象成：

```text
KvTransferBackend
  ├── NIXL
  ├── HIXL / ascend_direct
  ├── HCCL / RoCE
  ├── Mooncake
  ├── POSIX
  ├── Object Store
  └── Native
```

---

## 18. Pagoda 当前适配计划

### 18.1 第一阶段目标

当前 Pagoda 第一阶段只保留 LMCache 接入机制，不实现完整 KVBM。

目标：

```text
vLLM
  ↓
Pagoda KV connector 配置层
  ↓
LMCache connector / LMCache server
  ↓
LMCache L1 / L2 backend
```

### 18.2 不保留完整 KVBM memory 模块

如果不使用 KVBM，则不需要保留 Dynamo/KVBM 的完整 memory 模块。

不需要实现：

- KVBM block table；
- KVBM allocator；
- KVBM lifecycle state machine；
- CPU cache tier manager；
- disk cache tier manager；
- remote tier manager；
- NIXL abstraction layer；
- Pagoda 自己的 offload/onboard 调度器。

### 18.3 需要保留的能力

仍然需要保留或新建轻量 KV connector 层：

```text
kv_connector/
  mod.rs
  config.rs
  traits.rs
  lmcache.rs
  vllm.rs
```

职责：

- 表达 connector 类型；
- 生成 vLLM `kv_transfer_config`；
- 管理 LMCache server / sidecar 配置；
- 记录 cache backend 配置；
- 表达 aggregated / disaggregated 模式；
- 管理 metrics 和错误信息；
- 保留后续扩展 KVBM / AscendStore 的接口位置。

### 18.4 推荐模块边界

不建议把第一阶段模块命名为 `memory`，因为这会暗示 Pagoda 自己管理 KV block 内存。

推荐命名：

```text
kv_connector
kv_cache
lmcache_integration
```

其中：

```text
kv_connector:
  负责连接外部 KV 系统

kv_cache:
  负责描述 KV cache 配置与元数据

lmcache_integration:
  负责 LMCache 具体配置和启动方式
```

### 18.5 第一阶段不做的事情

第一阶段明确不做：

- 自研 KVBM；
- 自研多级 memory manager；
- 自研 NIXL 抽象；
- 自研 block 生命周期状态机；
- 自研 KV-aware block allocator；
- 直接接管 vLLM GPU KV block；
- 在 Pagoda 内部实现 LMCache 的缓存算法。

---

## 19. Pagoda 推荐抽象

推荐定义高层概念，而不是绑定某个后端。

### 19.1 Connector 类型

```text
KvConnectorKind
  ├── None
  ├── LMCache
  ├── KVBM
  ├── AscendStore
  └── Custom
```

### 19.2 Serving 模式

```text
ServingMode
  ├── Aggregated
  └── Disaggregated
```

### 19.3 Storage backend

```text
KvStorageBackend
  ├── Memory
  ├── LocalDisk
  ├── POSIX
  ├── GDS
  ├── ObjectStore
  ├── AzureBlob
  ├── Mooncake
  ├── Redis
  ├── NIXL
  └── Custom
```

### 19.4 Transfer backend

```text
KvTransferBackend
  ├── Native
  ├── NIXL
  ├── Mooncake
  ├── HIXL
  ├── HCCL
  ├── RDMA
  └── Custom
```

### 19.5 vLLM 配置生成

Pagoda 不需要直接实现 vLLM connector 内部逻辑。  
第一阶段只需要生成正确配置，并管理部署关系。

例如 LMCache aggregated：

```json
{
  "kv_connector": "LMCacheMPConnector",
  "kv_role": "kv_both"
}
```

KVBM 预留：

```json
{
  "kv_connector": "DynamoConnector",
  "kv_role": "kv_both",
  "kv_connector_module_path": "kvbm.vllm_integration.connector"
}
```

AscendStore 预留：

```json
{
  "kv_connector": "AscendStoreConnector",
  "kv_role": "kv_both",
  "kv_connector_extra_config": {
    "backend": "mooncake"
  }
}
```

---

## 20. 实施建议

### 20.1 第一阶段：LMCache-only

实现范围：

- 支持 LMCache connector 配置；
- 支持 LMCache server / sidecar 启动参数；
- 支持 L1 cache 配置；
- 支持 L2 backend 配置；
- 支持 aggregated 模式；
- 预留 disaggregated 配置结构；
- 文档中明确不实现 KVBM memory manager。

### 20.2 第二阶段：Disaggregated serving

扩展范围：

- prefill worker / decode worker 角色建模；
- transfer metadata 传递；
- NIXL / Mooncake / AscendStore 传输后端抽象；
- 路由器支持 P/D worker 选择；
- LMCache + NIXL 组合配置。

### 20.3 第三阶段：多后端 KV connector

扩展范围：

- KVBM 可选适配；
- AscendStore 可选适配；
- Mooncake backend；
- Object store backend；
- KV cache metrics；
- KV-aware routing。

---

## 21. 风险与注意事项

### 21.1 不要混淆执行层和缓存层

vLLM GPU paged KV buffer 是执行层。  
LMCache / KVBM 是外部缓存或管理层。  
外部 KV 必须 load 回 vLLM GPU buffer 后才能参与 attention。

### 21.2 不要在 LMCache-only 阶段保留 KVBM memory 假象

如果不实现 KVBM，就不要保留大量 KVBM memory 模块。  
否则会造成职责混乱。

### 21.3 不要把 NIXL 作为唯一抽象

NIXL 属于 NVIDIA 生态。  
为了未来适配 Ascend，应把传输层抽象为 `KvTransferBackend`，而不是直接写死 `NIXL`。

### 21.4 不要自己重复实现 LMCache

Pagoda 只需要接入 LMCache，不应复制 LMCache 的缓存管理能力。

### 21.5 注意 request_finished 资源释放

如果 connector 异步保存或传输未完成，不得提前释放 vLLM KV block，否则可能导致保存错误或脏数据。

---

## 22. 总结

vLLM connector 机制的本质是：

```text
vLLM 负责 GPU paged KV 执行层
connector 负责外部 KV 系统的 load/save
外部系统负责缓存、offload、复用或多级管理
```

LMCache 与 KVBM 的核心区别是：

```text
LMCache:
  外部 KV cache 系统
  强调缓存复用、L1/L2、多后端存储、跨实例共享

KVBM:
  Dynamo 原生 KV Block Manager
  强调 block table、多级内存、生命周期、offload/onboard、NIXL 传输
```

Pagoda 当前适配建议是：

```text
第一阶段只保留 LMCache 接入机制
不保留完整 KVBM memory 模块
建立轻量 kv_connector 抽象
预留未来 KVBM / AscendStore / Mooncake 扩展
```

最终目标是让 Pagoda 的 KV connector 层保持清晰边界：

```text
Pagoda Runtime / Router
  ↓
KvConnector abstraction
  ↓
LMCache / KVBM / AscendStore / Custom backend
  ↓
vLLM or other inference backend
```

---

## 23. 参考资料

- NVIDIA Dynamo：KV Cache Offloading  
  https://docs.nvidia.com/dynamo/backends/v-llm/kv-cache-offloading

- NVIDIA Dynamo：KVBM Guide  
  https://docs.nvidia.com/dynamo/v-0-9-1/components/kvbm/kvbm-guide

- vLLM：KVConnectorBase_V1  
  https://docs.vllm.ai/en/v0.9.1/api/vllm/distributed/kv_transfer/kv_connector/v1/base.html

- LMCache：Storage Backends  
  https://docs.lmcache.ai/kv_cache/storage_backends/index.html

- LMCache：NIXL Storage Backend  
  https://docs.lmcache.ai/kv_cache/storage_backends/nixl.html

- vLLM Ascend：Ascend Store / KV Pool  
  https://docs.vllm.ai/projects/ascend/en/main/user_guide/feature_guide/kv_pool.html

- LMCache：Mooncake Backend  
  https://docs.lmcache.ai/kv_cache/storage_backends/mooncake.html
