---
# SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
# SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-License-Identifier: Apache-2.0
#
# Implementation guide based on the public interfaces and behavioral contracts
# of NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
# Pagoda adaptation guide written by PAGODA.
title: Pagoda KV Router Adaptation Guide
---

# Pagoda KV Router 适配操作文档

## 1. 文档定位

本文档不是替代《Pagoda 通用 KV Router 模块设计分析文档》，而是把其中的目标设计落到当前 Dynamo `lib/kv-router` 代码上的操作手册。

目标是基于 Dynamo 现有 `dynamo-kv-router` crate 改造成 Pagoda 的 `pagoda-kv-router`：

- 保留 Dynamo 已经成熟的 hash、indexer、scheduler、ZMQ event、standalone indexer 能力；
- 按 Pagoda 目标补齐 LMCache shared metadata、通用拓扑建模、NVIDIA/Huawei 代价模型、未来后端扩展点；
- 第一阶段不引入 KVBM block manager、KVBM memory allocator 或 NIXL 硬依赖；
- 让所有扩展都落在现有 `lib/kv-router` 的协议、调度、索引和事件边界内。

本文档回答的问题是：按照 Pagoda 的设计目标，在 Dynamo 的 `lib/kv-router` 里具体应该改哪些文件、加哪些类型、保留哪些接口、每阶段如何验收。

## 2. 当前 Dynamo `lib/kv-router` 基线

当前 crate 路径：

```text
lib/kv-router
```

当前核心模块：

```text
lib/kv-router/src/
  lib.rs
  protocols.rs
  scheduling/
    config.rs
    local.rs
    queue.rs
    selector.rs
    types.rs
    policy.rs
    prefill_load.rs
  indexer/
    kv_indexer.rs
    radix_tree.rs
    concurrent_radix_tree.rs
    concurrent_radix_tree_compressed/
    lower_tier.rs
    lower_tier_indexers.rs
    types.rs
    traits.rs
  sequences/
    single.rs
    multi_worker.rs
    block_tracker.rs
    prefill_tracker.rs
    topology.rs
  zmq_wire/
    types.rs
    deserialize.rs
    convert.rs
    filter.rs
  standalone_indexer/
    server.rs
    registry.rs
    listener.rs
    indexer.rs
    zmq.rs
  standalone_shared_cache/
  recovery/
```

当前已经具备的关键类型：

```rust
// protocols.rs
pub type WorkerId = u64;
pub type DpRank = u32;

pub struct WorkerWithDpRank {
    pub worker_id: WorkerId,
    pub dp_rank: DpRank,
}

pub enum StorageTier {
    Device,
    HostPinned,
    Disk,
    External,
}

pub struct RouterEvent {
    pub worker_id: WorkerId,
    pub storage_tier: StorageTier,
    pub event: KvCacheEvent,
}

pub struct KvCacheEvent {
    pub event_id: u64,
    pub data: KvCacheEventData,
    pub dp_rank: DpRank,
}

pub enum KvCacheEventData {
    Stored(KvCacheStoreData),
    Removed(KvCacheRemoveData),
    Cleared,
}

pub struct SharedCacheHits { ... }
```

当前已经具备的调度配置：

```rust
// scheduling/config.rs
pub struct KvRouterConfig {
    pub overlap_score_credit: f64,
    pub prefill_load_scale: f64,
    pub host_cache_hit_weight: f64,
    pub disk_cache_hit_weight: f64,
    pub router_temperature: f64,
    pub use_kv_events: bool,
    pub durable_kv_events: bool,
    pub router_replica_sync: bool,
    pub router_track_active_blocks: bool,
    pub router_track_output_blocks: bool,
    pub router_assume_kv_reuse: bool,
    pub router_track_prefill_tokens: bool,
    pub router_prefill_load_model: RouterPrefillLoadModel,
    pub router_snapshot_threshold: Option<u32>,
    pub router_reset_states: bool,
    pub router_ttl_secs: f64,
    pub router_queue_threshold: Option<f64>,
    pub router_event_threads: u32,
    pub skip_initial_worker_wait: bool,
    pub router_queue_policy: RouterQueuePolicy,
    pub use_remote_indexer: bool,
    pub serve_indexer: bool,
    pub shared_cache_multiplier: f64,
    pub shared_cache_type: SharedCacheType,
}
```

当前 `DefaultWorkerSelector` 的打分公式：

```text
logit = prefill_load_scale * adjusted_prefill_blocks + decode_blocks

adjusted_prefill_blocks = max(raw_prefill_blocks - overlap_credit_blocks, 0)

overlap_credit_blocks =
  overlap_score_credit * device_overlap_blocks
  + host_cache_hit_weight * host_pinned_overlap_blocks
  + disk_cache_hit_weight * disk_overlap_blocks
  + shared_cache_multiplier * shared_cache_hits_beyond_device
```

这说明 Pagoda 不需要从零实现 router。正确策略是：保留现有 crate，把 Pagoda 的目标能力接到现有协议、索引和选择器扩展点上。

## 3. Pagoda 设计目标到 Dynamo 代码的映射

| Pagoda 设计目标 | Dynamo 当前落点 | 需要做的适配 |
| --- | --- | --- |
| 通用 KV Router | `lib/kv-router` crate | 保留 crate，必要时改 package description / re-export 名称，不新建并行 router |
| token prefix hash / block hash | `protocols.rs` 的 `compute_block_hash_for_seq`、`compute_seq_hash_for_block` | 保持兼容，扩展 salt / tenant / topology metadata 时不要破坏 hash 兼容 |
| KV event 消费 | `zmq_wire/*`、`standalone_indexer/*`、`RouterEvent` | 增加 LMCache / SGLang / Pagoda event source adapter |
| worker 注册与发现 | `standalone_indexer/registry.rs`、`server.rs` | 增加 Pagoda runtime 注册字段和拓扑字段，不破坏现有 HTTP shape |
| active load tracking | `sequences/*`、`scheduling/local.rs` | 保留，补充 aggregated/disaggregated worker role |
| LMCache shared metadata | 当前仅有 `SharedCacheHits` 输入；`SharedCacheType` 只有 `None`/`Hicache` | 新增 `SharedCacheType::Lmcache` 与 metadata client adapter |
| topology-aware cost model | 当前 selector 没有通用 topology provider | 新增 `topology/` 模块与 `TopologyAwareWorkerSelector` 或扩展 `DefaultWorkerSelector` |
| NVIDIA/Huawei 拓扑适配 | 当前无平台 provider | 新增 `topology/nvidia.rs`、`topology/huawei.rs`、`topology/static_provider.rs` |
| KVBM/HiCache/Mooncake/AscendStore 扩展 | 当前有 tier、placement、shared hits 的基础抽象 | 通过 adapter / feature gate 扩展，不作为 LMCache-only 硬依赖 |

## 4. 总体改造原则

1. `protocols.rs` 只放路由可见的稳定协议类型，不放具体后端 client。
2. `scheduling/` 只做选择和排队，不直接访问 LMCache、KVBM、Mooncake 或 AscendStore。
3. `indexer/` 只维护 prefix 到 worker/tier 的视图，不读取真实 KV tensor。
4. `zmq_wire/` 只处理 wire event decode 和转换，不承担调度策略。
5. shared cache 查询通过 adapter 产出 `SharedCacheHits`，再交给 scheduler。
6. topology provider 只产出 locality / penalty，不修改 KV event 的基础语义。
7. 第一阶段所有新增能力都应 feature-gated 或默认关闭，避免影响现有 Dynamo 行为。

## 5. 阶段 0：重命名和边界整理

### 5.1 操作目标

把 Dynamo `dynamo-kv-router` 明确作为 Pagoda fork 的基础模块，同时保持对上层调用方的迁移路径。

### 5.2 修改文件

```text
lib/kv-router/Cargo.toml
lib/kv-router/src/lib.rs
```

### 5.3 具体操作

1. 在 `Cargo.toml` 中按项目策略决定是否改 crate 名：

```toml
[package]
name = "pagoda-kv-router"
description = "Pagoda KV Router based on Dynamo KV routing interfaces"
```

如果短期仍需兼容 workspace 内部引用，可暂时保留 `name = "dynamo-kv-router"`，只改 description，并在后续统一替换依赖名。

2. 在 `lib.rs` 中保持现有 re-export：

```rust
pub use config::{KvRouterConfig, RouterConfigOverride, RouterPrefillLoadModel, RouterQueuePolicy, SharedCacheType};
pub use protocols::{RouterEvent, SharedCacheHits, WorkerId, WorkerWithDpRank};
pub use scheduling::{LocalScheduler, SchedulingRequest, SchedulingResponse};
pub use selector::{DefaultWorkerSelector, WorkerSelector};
```

3. 新增 Pagoda 专用模块时，应从 `lib.rs` 显式导出：

```rust
pub mod shared_cache;
pub mod topology;
```

4. 保留原有 public API，新增 API 不应替换原类型名。这样上层可以逐步从 Dynamo 行为迁移到 Pagoda 行为。

### 5.4 验收

```bash
cargo check -p dynamo-kv-router
cargo test -p dynamo-kv-router
```

若 crate 改名，需要同步 workspace、依赖项和 package 名后再运行：

```bash
cargo check -p pagoda-kv-router
cargo test -p pagoda-kv-router
```

## 6. 阶段 1：LMCache-only 基线路由

### 6.1 操作目标

实现 Pagoda 第一阶段：不依赖 KVBM/memory，只用 vLLM worker + LMCache connector，并让 router 基于当前 device overlap 和 active load 做选择。

### 6.2 保留文件

```text
lib/kv-router/src/protocols.rs
lib/kv-router/src/scheduling/config.rs
lib/kv-router/src/scheduling/local.rs
lib/kv-router/src/scheduling/selector.rs
lib/kv-router/src/scheduling/types.rs
lib/kv-router/src/indexer/*
lib/kv-router/src/sequences/*
```

### 6.3 配置适配

修改 `scheduling/config.rs`，新增 Pagoda runtime 场景配置，但不要删除原字段。

建议新增：

```rust
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServingMode {
    #[default]
    Aggregated,
    Disaggregated,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KvEventMode {
    #[default]
    Approximate,
    EventDriven,
    Hybrid,
}
```

在 `KvRouterConfig` 中新增：

```rust
pub serving_mode: ServingMode,
pub kv_event_mode: KvEventMode,
pub lmcache_connector_enabled: bool,
```

默认值：

```rust
serving_mode = ServingMode::Aggregated
kv_event_mode = KvEventMode::Approximate
lmcache_connector_enabled = true
```

### 6.4 调度适配

当前 `SchedulingRequest` 已有必要信号：

```rust
pub tier_overlap_blocks: TierOverlapBlocks,
pub effective_overlap_blocks: HashMap<WorkerWithDpRank, f64>,
pub effective_cached_tokens: HashMap<WorkerWithDpRank, usize>,
pub shared_cache_hits: Option<SharedCacheHits>,
pub decode_blocks: FxHashMap<WorkerWithDpRank, usize>,
pub prefill_tokens: FxHashMap<WorkerWithDpRank, usize>,
```

第一阶段无需新增请求结构。只需要确保上层构造请求时：

- `effective_overlap_blocks` 来自本地 indexer 或 approximate tracking；
- `decode_blocks` 来自 active sequence tracking；
- `shared_cache_hits = None`；
- `TierOverlapBlocks` 至少填充 `device`，没有 lower-tier 时保持空 map。

### 6.5 代码改动点

1. `scheduling/config.rs`：新增 `ServingMode`、`KvEventMode`、字段和校验。
2. `scheduling/local.rs`：确认构造 `SchedulingRequest` 时按 `kv_event_mode` 选择 event-driven 或 approximate overlap。
3. `scheduling/selector.rs`：保持原公式，不引入 topology penalty。
4. `indexer/*`：保留现有 radix tree 和 lower tier 结构，不接 KVBM。

### 6.6 验收

- `shared_cache_type = none` 时，路由行为与当前 Dynamo KV router 保持一致；
- `lmcache_connector_enabled = true` 不会让 router 直接访问 LMCache tensor；
- 禁止新增对 `lib/memory`、`lib/kvbm-*` 的编译依赖；
- `cargo test -p dynamo-kv-router` 通过。

## 7. 阶段 2：LMCache shared metadata adapter

### 7.1 操作目标

把 Pagoda 设计中的 `Shared Cache Metadata Client` 落到 Dynamo 的 `SharedCacheHits` 输入上，让 LMCache 只提供元数据命中，不参与最终路由决策。

### 7.2 修改文件

```text
lib/kv-router/src/scheduling/config.rs
lib/kv-router/src/protocols.rs
lib/kv-router/src/shared_cache/mod.rs          # 新增
lib/kv-router/src/shared_cache/lmcache.rs      # 新增
lib/kv-router/src/shared_cache/noop.rs         # 新增
lib/kv-router/src/lib.rs
```

### 7.3 配置改动

当前 `SharedCacheType`：

```rust
pub enum SharedCacheType {
    None,
    Hicache,
}
```

扩展为：

```rust
pub enum SharedCacheType {
    None,
    Hicache,
    Lmcache,
}
```

同步修改：

- `Display`；
- `FromStr`；
- serde 测试；
- 错误信息：`expected 'none', 'hicache', or 'lmcache'`。

新增配置字段：

```rust
pub lmcache_metadata_endpoint: Option<String>,
pub lmcache_metadata_timeout_ms: u64,
pub lmcache_shared_cache_scope: SharedCacheScope,
```

建议新增：

```rust
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SharedCacheScope {
    #[default]
    Global,
    Tenant,
    Model,
    WorkerGroup,
}
```

### 7.4 新增 trait

在 `shared_cache/mod.rs`：

```rust
use async_trait::async_trait;
use crate::protocols::{LocalBlockHash, SharedCacheHits};

#[async_trait]
pub trait SharedCacheMetadataClient: Send + Sync {
    async fn lookup(&self, request: SharedCacheLookupRequest) -> anyhow::Result<SharedCacheHits>;
}

pub struct SharedCacheLookupRequest {
    pub model_name: String,
    pub tenant_id: String,
    pub block_hashes: Vec<LocalBlockHash>,
    pub candidate_workers: Vec<crate::protocols::WorkerWithDpRank>,
}
```

`lmcache.rs` 实现 HTTP/gRPC/client adapter。第一版可以先实现 `NoopSharedCacheMetadataClient`，保证接口打通。

### 7.5 接入调度

`SchedulingRequest` 已有：

```rust
pub shared_cache_hits: Option<SharedCacheHits>,
```

因此接入点不在 `DefaultWorkerSelector`，而在构造 `SchedulingRequest` 之前：

```text
token ids
  ↓
compute_block_hash_for_seq
  ↓
indexer local overlap query
  ↓
SharedCacheMetadataClient::lookup
  ↓
SchedulingRequest { shared_cache_hits: Some(...) }
  ↓
DefaultWorkerSelector
```

### 7.6 验收

- `shared_cache_type = lmcache` 时会调用 LMCache metadata adapter；
- adapter 失败时可按配置 fallback 到 `shared_cache_hits = None`；
- `shared_cache_multiplier = 0.0` 时 LMCache shared hit 不影响路由；
- `shared_cache_multiplier > 0.0` 时 shared hits 只作为低于 device-local 的 credit；
- selector 测试覆盖 shared hits beyond device 的场景。

## 8. 阶段 3：通用 topology provider

### 8.1 操作目标

实现 Pagoda 设计中的 topology-aware cost model，但不把拓扑信息塞进 `StorageTier`。`StorageTier` 仍表示 KV 所在介质，topology provider 表示 candidate worker 到 cache placement 的代价。

### 8.2 修改文件

```text
lib/kv-router/src/topology/mod.rs              # 新增
lib/kv-router/src/topology/types.rs            # 新增
lib/kv-router/src/topology/static_provider.rs  # 新增
lib/kv-router/src/topology/nvidia.rs           # 新增
lib/kv-router/src/topology/huawei.rs           # 新增
lib/kv-router/src/scheduling/config.rs
lib/kv-router/src/scheduling/types.rs
lib/kv-router/src/scheduling/selector.rs
lib/kv-router/src/lib.rs
```

### 8.3 新增类型

```rust
pub enum HardwareVendor {
    Nvidia,
    Huawei,
    Amd,
    Intel,
    CpuOnly,
    Custom(String),
}

pub struct WorkerLocation {
    pub worker_id: WorkerId,
    pub dp_rank: Option<DpRank>,
    pub vendor: HardwareVendor,
    pub zone_id: Option<String>,
    pub rack_id: Option<String>,
    pub supernode_id: Option<String>,
    pub host_id: Option<String>,
    pub device_id: Option<String>,
    pub fabric_domain_id: Option<String>,
}

pub enum KvLocalityTier {
    LocalDevice,
    SameHostDevice,
    SameNvlinkDomain,
    SameSupernodeFast,
    SameSupernodeSlow,
    CrossSupernode,
    LocalHostMemory,
    RemoteHostMemory,
    LocalDisk,
    SharedStorage,
    Unknown,
}

pub struct TopologyPenalty {
    pub locality: KvLocalityTier,
    pub penalty_blocks: f64,
}
```

### 8.4 新增 trait

```rust
pub trait TopologyProvider: Send + Sync {
    fn location_of_worker(&self, worker: WorkerWithDpRank) -> Option<WorkerLocation>;

    fn locality_between(
        &self,
        candidate_worker: WorkerWithDpRank,
        cache_owner: WorkerWithDpRank,
        storage_tier: StorageTier,
    ) -> KvLocalityTier;

    fn penalty_blocks(
        &self,
        locality: KvLocalityTier,
        storage_tier: StorageTier,
    ) -> f64;
}
```

### 8.5 配置字段

在 `KvRouterConfig` 中新增：

```rust
pub topology_enabled: bool,
pub topology_provider: TopologyProviderType,
pub topology_penalty_scale: f64,
```

```rust
pub enum TopologyProviderType {
    None,
    Static,
    Kubernetes,
    Nvidia,
    Huawei,
}
```

默认：

```rust
topology_enabled = false
topology_provider = TopologyProviderType::None
topology_penalty_scale = 0.0
```

### 8.6 调度接入方式

不要直接把 topology penalty 写死进 `DefaultWorkerSelector`。推荐新增 wrapper：

```rust
pub struct TopologyAwareWorkerSelector<S, T> {
    inner: S,
    topology_provider: T,
    config: KvRouterConfig,
}
```

或者在 `DefaultWorkerSelector` 内以可选 provider 注入，但默认必须保持无 provider 行为。

最终公式：

```text
logit =
  prefill_load_scale * adjusted_prefill_blocks
  + decode_blocks
  + topology_penalty_scale * topology_penalty_blocks
```

### 8.7 验收

- `topology_enabled = false` 时，所有 selector 测试与当前 Dynamo 一致；
- `topology_enabled = true` 时，相同 KV overlap 下优先选择拓扑代价更低的 worker；
- NVIDIA provider 测试覆盖 same host、same NVLink/NVSwitch domain、cross node RDMA；
- Huawei provider 测试覆盖 same host、same supernode fast、same supernode slow、cross supernode；
- topology penalty 不改变 `StorageTier` 的 wire format。

## 9. 阶段 4：KV event source 扩展

### 9.1 操作目标

把 Pagoda 的 event relay / decoder 设计落到现有 `zmq_wire` 和 `standalone_indexer`，支持 vLLM 原生事件、LMCache storage event、SGLang/HiCache event 的统一转换。

### 9.2 当前基础

当前 vLLM raw event：

```rust
pub enum RawKvEvent {
    BlockStored { block_hashes, parent_block_hash, token_ids, block_size, medium, ... },
    BlockRemoved { block_hashes, medium, ... },
    AllBlocksCleared,
    Ignored,
}
```

当前 `medium` 会映射到：

```rust
StorageTier::from_kv_medium(...)
```

### 9.3 修改文件

```text
lib/kv-router/src/zmq_wire/types.rs
lib/kv-router/src/zmq_wire/convert.rs
lib/kv-router/src/zmq_wire/filter.rs
lib/kv-router/src/standalone_indexer/listener.rs
lib/kv-router/src/standalone_indexer/registry.rs
lib/kv-router/src/protocols.rs
```

### 9.4 新增 source 类型

```rust
pub enum KvEventSource {
    Vllm,
    Sglang,
    Lmcache,
    Pagoda,
}
```

如果 source 只影响 decode 逻辑，放在 `zmq_wire/types.rs`；如果会进入公共协议，放在 `protocols.rs`。

### 9.5 event metadata 扩展

需要支持 Pagoda 目标中的字段，但保持可选：

```rust
pub struct EventOriginMetadata {
    pub source: KvEventSource,
    pub model_name: Option<String>,
    pub tenant_id: Option<String>,
    pub worker_group: Option<String>,
    pub topology_scope: Option<String>,
}
```

第一版不要把这些字段强行加入 `RouterEvent` 必填项。优先通过 wrapper 或 registry context 传递，避免破坏当前 event producer。

### 9.6 验收

- vLLM 原始 ZMQ event 仍可反序列化；
- `medium = GPU/CPU_PINNED/DISK/EXTERNAL` 仍映射到正确 `StorageTier`；
- LMCache 或 Pagoda event 可以转换为 `RouterEvent`；
- unknown event 不导致 listener 崩溃，应转换为 `Ignored` 或返回可观测错误。

## 10. 阶段 5：standalone indexer API 适配

### 10.1 操作目标

让 standalone indexer 支持 Pagoda runtime 注册、查询和分层命中返回，同时保持现有 HTTP API 兼容。

### 10.2 当前 API

`standalone_indexer/server.rs` 当前请求：

```rust
pub struct RegisterRequest {
    pub instance_id: WorkerId,
    pub endpoint: String,
    pub model_name: String,
    pub tenant_id: String,
    pub block_size: u32,
    pub dp_rank: Option<u32>,
    pub replay_endpoint: Option<String>,
    pub additional_salt: Option<String>,
}

pub struct QueryRequest {
    pub token_ids: Vec<u32>,
    pub model_name: String,
    pub tenant_id: String,
    pub lora_name: Option<String>,
    pub cache_salt: Option<String>,
}
```

当前 response 已有：

```text
scores / frequencies       # 兼容旧客户端
instances                  # per instance, per tier breakdown
```

### 10.3 推荐新增字段

在 `RegisterRequest` 中增加可选字段：

```rust
pub worker_group: Option<String>,
pub serving_role: Option<ServingRole>,
pub hardware_vendor: Option<HardwareVendor>,
pub topology: Option<WorkerLocation>,
pub lmcache_endpoint: Option<String>,
pub shared_cache_scope: Option<String>,
```

在 `QueryRequest` 中增加可选字段：

```rust
pub request_id: Option<String>,
pub expected_output_tokens: Option<u32>,
pub allowed_worker_ids: Option<Vec<WorkerId>>,
pub router_config_override: Option<RouterConfigOverride>,
```

所有新增字段必须 `#[serde(default)]`，避免破坏现有客户端。

### 10.4 验收

- 老客户端只发送原字段时仍成功；
- 新客户端发送 topology / LMCache endpoint 时 registry 能保存；
- `/query` 仍返回 `scores`、`frequencies`、`instances`；
- `instances` 中 device/host/disk/external 的 token 数能与 indexer tier 查询一致。

## 11. 阶段 6：aggregated 和 disaggregated serving

### 11.1 操作目标

让 Pagoda router 支持 aggregated 优先，同时为 prefill/decode 分离预留字段和选择路径。

### 11.2 新增类型

```rust
pub enum ServingRole {
    Both,
    Prefill,
    Decode,
}
```

落点：

```text
lib/kv-router/src/protocols.rs      # 如果需要跨模块 public
lib/kv-router/src/scheduling/types.rs
lib/kv-router/src/standalone_indexer/server.rs
```

### 11.3 调度逻辑

在 `SchedulingRequest` 中新增可选字段：

```rust
pub target_role: Option<ServingRole>,
pub disagg_transfer_required: bool,
```

选择 worker 时：

- aggregated：允许 `Both` worker；
- prefill routing：允许 `Both` 或 `Prefill`；
- decode routing：允许 `Both` 或 `Decode`；
- disaggregated 场景下，后续 transfer metadata 由 connector/transfer backend 管理，router 只选择端点和传递元数据引用。

### 11.4 验收

- 默认 aggregated 行为不变；
- prefill-only worker 不会被选作 decode worker；
- decode-only worker 不会被选作 prefill worker；
- transfer backend 不成为 `lib/kv-router` 的必选依赖。

## 12. 阶段 7：后端扩展点

### 12.1 操作目标

为 KVBM、HiCache、Mooncake、AscendStore 预留适配，但不污染 LMCache-only 路径。

### 12.2 adapter 目录

建议新增：

```text
lib/kv-router/src/backend_adapters/
  mod.rs
  lmcache.rs
  hicache.rs
  mooncake.rs
  ascend_store.rs
  kvbm.rs
```

如果团队希望更严格地保持 router 纯控制面，则把这些 adapter 放在上层 runtime crate，只在 `lib/kv-router` 保留 trait：

```text
lib/kv-router/src/shared_cache/
lib/kv-router/src/topology/
lib/kv-router/src/zmq_wire/
```

推荐优先选择第二种：router crate 定义 trait 和通用数据结构，具体 backend client 放到 runtime / components 层。

### 12.3 KVBM

KVBM 接入只允许作为可选 adapter：

```text
KVBM metadata/event adapter
  ↓
RouterEvent / SharedCacheHits / StorageTier
  ↓
indexer + scheduler
```

禁止：

```text
lib/kv-router 直接依赖 lib/memory
lib/kv-router 直接依赖 kvbm allocator
lib/kv-router 直接操作 KVBM block table
```

### 12.4 HiCache

HiCache 优先复用现有 `SharedCacheType::Hicache`，补齐：

- SGLang / HiCache event decoder；
- HiCache metadata query adapter；
- CPU pinned tier 到 `StorageTier::HostPinned` 的转换测试。

### 12.5 Mooncake

Mooncake 适配优先走 `standalone_indexer` 的 `instances` response shape 和 shared metadata adapter。

需要注意：Mooncake RFC 风格字段可以体现在 HTTP API，但内部仍应转换为：

```text
TieredMatchDetails
SharedCacheHits
StorageTier::External
```

### 12.6 AscendStore

AscendStore 适配应分两层：

```text
Huawei topology provider
AscendStore shared metadata adapter
```

不要把 HCCL/HIXL/AscendStore 传输细节写进 `DefaultWorkerSelector`。

## 13. 阶段 8：配置示例

### 13.1 LMCache-only aggregated

```toml
[kv_router]
serving_mode = "aggregated"
kv_event_mode = "approximate"
use_kv_events = false
router_track_active_blocks = true
router_assume_kv_reuse = true
shared_cache_type = "none"
shared_cache_multiplier = 0.0
lmcache_connector_enabled = true
```

### 13.2 LMCache shared metadata

```toml
[kv_router]
serving_mode = "aggregated"
kv_event_mode = "hybrid"
use_kv_events = true
shared_cache_type = "lmcache"
shared_cache_multiplier = 0.5
lmcache_metadata_endpoint = "http://lmcache-metadata:8080"
lmcache_metadata_timeout_ms = 50
```

### 13.3 Huawei topology-aware

```toml
[kv_router]
topology_enabled = true
topology_provider = "huawei"
topology_penalty_scale = 1.0

[kv_router.topology.huawei]
same_host = 0.05
same_supernode_fast = 0.20
same_supernode_slow = 0.45
cross_supernode = 1.50
shared_storage = 2.00
```

### 13.4 NVIDIA topology-aware

```toml
[kv_router]
topology_enabled = true
topology_provider = "nvidia"
topology_penalty_scale = 1.0

[kv_router.topology.nvidia]
same_gpu = 0.0
same_host_nvlink = 0.10
same_host_pcie = 0.25
cross_node_rdma = 0.80
cross_node_tcp = 2.00
```

## 14. 测试计划

### 14.1 单元测试

新增或扩展：

```text
lib/kv-router/src/scheduling/config.rs
  - SharedCacheType::Lmcache parse/display
  - topology config default disabled
  - invalid topology config validation

lib/kv-router/src/scheduling/selector.rs
  - default behavior unchanged
  - shared cache hits affect score only when multiplier > 0
  - topology disabled means no penalty
  - topology enabled changes tie-break / score

lib/kv-router/src/topology/*
  - static provider lookup
  - NVIDIA locality classification
  - Huawei supernode locality classification

lib/kv-router/src/shared_cache/*
  - noop client returns empty hits
  - LMCache adapter maps metadata to SharedCacheHits

lib/kv-router/src/zmq_wire/*
  - vLLM event compatibility
  - LMCache/Pagoda event conversion
  - unknown medium fallback behavior
```

### 14.2 集成测试

```text
standalone_indexer register/query
  - old request shape
  - new Pagoda worker metadata shape
  - tiered response shape

scheduler local flow
  - approximate mode
  - event-driven mode
  - shared metadata mode
  - topology-aware mode
```

### 14.3 编译约束测试

必须持续验证：

```bash
cargo check -p dynamo-kv-router
cargo test -p dynamo-kv-router
cargo test -p dynamo-kv-router --features standalone-indexer
```

如果 crate 改名，则替换为 `pagoda-kv-router`。

还需要检查依赖边界：

```bash
cargo tree -p dynamo-kv-router | rg 'kvbm|memory|nixl'
```

第一阶段该命令不应显示 KVBM/memory/NIXL 硬依赖。

## 15. 分阶段完成标准

### 阶段 1 完成标准

- LMCache-only aggregated 能通过当前 scheduler 路由；
- router 不读取真实 KV tensor；
- 不依赖 KVBM/memory；
- 默认行为与 Dynamo 当前 `DefaultWorkerSelector` 一致。

### 阶段 2 完成标准

- `shared_cache_type = lmcache` 可启用；
- LMCache metadata adapter 能产出 `SharedCacheHits`；
- adapter 异常不会导致 router 不可用；
- shared hit 分数受 `shared_cache_multiplier` 控制。

### 阶段 3 完成标准

- `topology_enabled = false` 时行为完全兼容；
- `topology_enabled = true` 时支持 static/NVIDIA/Huawei provider；
- topology penalty 可配置、可测试、可观测。

### 阶段 4 完成标准

- vLLM 原事件兼容；
- Pagoda/LMCache/SGLang event 能映射到 `RouterEvent`；
- unknown event 可观测但不崩溃。

### 阶段 5 完成标准

- standalone indexer 支持 Pagoda worker metadata；
- 旧 API 兼容；
- `instances` 返回能表达 device/host/disk/external 分层命中。

### 阶段 6 完成标准

- aggregated 路径稳定；
- disaggregated role 字段可表达；
- prefill/decode worker 选择不混淆；
- transfer backend 仍在 connector/data plane，不进入 router core。

### 阶段 7 完成标准

- KVBM、HiCache、Mooncake、AscendStore 都有清晰 adapter 落点；
- LMCache-only 路径没有被可选后端污染；
- 新后端接入只需实现 metadata/event/topology adapter，不需要重写 scheduler。

## 16. 推荐实施顺序

```text
1. 保留 Dynamo lib/kv-router，整理 crate 边界和版权头
2. 增加 Pagoda runtime 配置字段，但默认关闭新增行为
3. 加 SharedCacheType::Lmcache 和 shared_cache trait/noop client
4. 接 LMCache metadata adapter 到 SharedCacheHits
5. 增加 topology types/provider，但 selector 默认不使用
6. 增加 topology-aware selector wrapper
7. 扩展 standalone indexer 注册字段
8. 扩展 event source decoder
9. 预留 disaggregated serving role
10. 后续接 HiCache/Mooncake/AscendStore/KVBM adapter
```

## 17. 不建议做的事

- 不建议把 Pagoda router 重写到 `lib/runtime/src/kv_router`，这会绕开 Dynamo 已有 `lib/kv-router` 成熟能力。
- 不建议把 `StorageTier` 改成硬件拓扑枚举；storage tier 和 topology locality 是两个维度。
- 不建议让 `DefaultWorkerSelector` 直接依赖 LMCache client 或 AscendStore client。
- 不建议第一阶段引入 `lib/memory`、`lib/kvbm-*` 或 NIXL 硬依赖。
- 不建议删除旧 HTTP API 字段；新增字段应全部 optional + serde default。
- 不建议让 router 读取真实 KV tensor、文件 offset、RDMA address 或对象存储地址。

## 18. 最终形态

完成所有阶段后，`lib/kv-router` 应演进为 Pagoda 的通用 KV Router 控制面：

```text
Pagoda KV Router
  ├── protocol layer: RouterEvent / StorageTier / Placement / SharedCacheHits
  ├── index layer: device / host / disk / external prefix view
  ├── load layer: active sequence / prefill / decode tracking
  ├── shared cache layer: LMCache / HiCache / Mooncake / AscendStore metadata adapter
  ├── topology layer: static / NVIDIA / Huawei / future hardware provider
  ├── scheduler layer: default selector + topology-aware selector
  └── event layer: vLLM / SGLang / LMCache / Pagoda event adapter
```

这样既能继承 Dynamo 的 KV router 基础，又能实现 Pagoda 的 LMCache-first、拓扑感知、跨硬件、跨后端的长期目标。
