# Pagoda Mocker KV 缓存与 LMCache 模拟设计

## 1. 模块目标

本文件定义 Pagoda Mocker 中 KV cache 模拟层。

该层替代原 `kv_manager` 的 KVBM 依赖，目标是：

```text
模拟 vLLM 本地 paged KV cache；
模拟 prefix cache；
模拟 block active / inactive / eviction；
模拟 LMCache L1/L2 外部缓存命中；
模拟 KV event 发布；
向 scheduler 提供 prefill cost；
向 router/replay 提供 cache metadata。
```

第一阶段不实现真实内存管理，不接真实 LMCache server。

---

## 2. 与原模块对齐关系

原模块中 `kv_manager/kvbm_backend.rs` 已经实现了大量 vLLM 风格 block 生命周期逻辑：

```text
Use
Deref
Promote
active partial block
active full block
inactive prefix cache
prefix hit 扫描
Stored / Removed event
local_hash 与 sequence_hash
parent_hash chaining
preemption 时释放 active blocks
```

Pagoda 应继承这些**语义**，但不继承 KVBM 作为公开设计依赖。

也就是说：

```text
保留语义：
  block 分配、复用、释放、晋升、事件发布、prefix cost 计算。

替换实现：
  kvbm-logical BlockManager
    → Pagoda LocalVllmKvCache 内部数据结构。

  kvbm G1/G2 offload
    → 删除。

  HostPinned / G2 router event
    → 第一阶段改为 LMCache ExternalShared 事件或 shared query。
```

---

## 3. 推荐模块结构

```text
kv_cache/
  mod.rs
  local_vllm_cache.rs
  lmcache_adapter.rs
  events.rs
  tests.rs
```

### 3.1 `local_vllm_cache.rs`

模拟 vLLM worker 本地 GPU paged KV buffer。

### 3.2 `lmcache_adapter.rs`

模拟 LMCache 外部缓存系统。

### 3.3 `events.rs`

封装 Stored / Removed event 构造逻辑，避免散落在 cache 操作中。

---

## 4. ActiveSequence 继承设计

`ActiveSequence` 继续作为请求 token 与 block 状态的核心结构。

它模拟：

```text
输入 token 序列；
生成 token 序列；
block_size 切分；
full block hash；
partial block；
prefix caching 开关；
allocation progress；
generated_tokens；
max_output_tokens。
```

### 4.1 初始化

```text
ActiveSequence::new(tokens, max_output_tokens, block_size, enable_prefix_caching, emit_token_ids)
```

流程：

```text
1. 将 tokens 切为 TokenBlockSequence。
2. 对每个完整 block 计算：
   - block_hash
   - sequence_hash
   - positional lineage hash 或等价 lineage key
3. 如果 token 数不是 block_size 整数倍，追加 partial block。
4. 记录 num_input_tokens。
5. num_allocated_tokens = 0。
6. generated_tokens = 0。
```

### 4.2 Prefix caching 关闭时

如果 `enable_prefix_caching = false`：

```text
相同 token 序列不应共享 block；
每个 full block 使用随机 identity；
prefix hit 永远为 0。
```

该行为应继承原模块设计。

---

## 5. MoveBlock 语义

### 5.1 Use

`Use` 表示请求需要使用一个或多个 block。

在 Pagoda 中处理流程：

```text
for block in requested_blocks:
  if full block:
    1. 检查 active full 是否存在；
    2. 检查 inactive local cache 是否存在；
    3. 检查 LMCache 是否存在；
    4. 如果都不存在，分配新的 local block。
  if partial block:
    1. 如果 active partial 已存在，复用；
    2. 否则分配新 local block。
```

返回值：

```text
成功处理的 block 数。
```

如果容量不足：

```text
返回部分成功数量；
scheduler 可触发 preemption。
```

### 5.2 Deref

`Deref` 表示请求释放 block 引用。

处理流程：

```text
PartialBlock:
  释放 active partial。

FullBlock:
  降低 active full refcount；
  如果 refcount 归零，则转入 inactive local prefix cache。
```

如果 inactive cache 容量超限，触发本地淘汰并发布 Removed 事件。

### 5.3 Promote

`Promote` 表示 partial block 填满，晋升为 full block。

处理流程：

```text
1. 从 active partial 中移除 partial block。
2. 生成 full block identity。
3. 如果 active/inactive 中已有相同 full block，则复用。
4. 否则注册新 full block。
5. 发布 Stored event。
6. 可选保存到 LMCache。
```

---

## 6. LocalVllmKvCache 设计

建议结构：

```rust
struct LocalVllmKvCache {
    max_capacity: usize,
    block_size: usize,
    active_partial: HashMap<Uuid, LocalBlockHandle>,
    active_full: HashMap<SequenceHash, Vec<LocalBlockHandle>>,
    inactive_full: LocalEvictionCache,
    registered_blocks: HashMap<BlockIdentity, RegisteredBlockInfo>,
    lmcache: Option<LmCacheMockAdapter>,
    kv_event_publishers: KvEventPublishers,
    dp_rank: u32,
    next_event_id: u64,
}
```

### 6.1 active_partial

模拟正在写入但还没形成完整 block 的 decode partial block。

特点：

```text
只属于一个请求；
不能用于 prefix cache；
完成后 Promote。
```

### 6.2 active_full

模拟当前被请求持有的完整 KV block。

特点：

```text
可被多个请求共享；
通过 Vec/计数模拟 refcount；
请求 Deref 后 refcount 减少；
refcount 归零后转 inactive。
```

### 6.3 inactive_full

模拟本地 GPU prefix cache 中可复用但当前没有请求持有的 block。

特点：

```text
可被 prefix hit 扫描命中；
容量不足时可被淘汰；
淘汰时发布 Removed event。
```

### 6.4 registered_blocks

保存 router event 构造所需 metadata：

```text
sequence_hash
parent_hash
local_hash
token_ids
```

用于：

```text
Stored event；
Removed event；
LMCache save；
shared metadata query。
```

---

## 7. Prefix cost 计算

接口：

```rust
fn get_prefill_cost(&self, sequence: &ActiveSequence) -> PrefillCost
```

继承原语义：

```text
从 sequence 的第一个 full block 开始；
逐个判断是否命中；
遇到第一个 miss 停止；
partial block 不参与 prefix hit；
命中 token 数 = overlap_blocks * block_size，并且不能超过 input token 数。
```

返回：

```rust
struct PrefillCost {
    new_blocks: usize,
    new_tokens: usize,
    cached_tokens: usize,
}
```

### 7.1 本地命中来源

```text
active_full 命中；
inactive_full 命中。
```

### 7.2 LMCache 命中来源

如果启用 LMCache：

```text
Local miss 后，可查 LMCacheAdapter；
如果 LMCache 命中连续 block，则计入 cached_tokens；
同时可模拟 load latency。
```

注意：

```text
只有连续前缀命中才计入 cached_tokens；
不能跳过中间 miss。
```

---

## 8. LMCacheAdapter 设计

### 8.1 目标

LMCacheAdapter 是 mock，不是真实 LMCache client。

它模拟：

```text
L1 是否命中；
L2 是否命中；
保存 block；
删除 block；
shared metadata lookup；
hit latency；
save latency；
external shared KV events。
```

### 8.2 推荐结构

```rust
struct LmCacheMockAdapter {
    config: LmCacheMockConfig,
    l1_blocks: HashMap<SequenceHash, LmCacheBlockMeta>,
    l2_blocks: HashMap<SequenceHash, LmCacheBlockMeta>,
    stats: LmCacheMockStats,
}
```

```rust
struct LmCacheBlockMeta {
    sequence_hash: SequenceHash,
    local_hash: BlockHash,
    parent_hash: Option<SequenceHash>,
    token_ids: Option<Vec<u32>>,
    stored_at_ms: Option<f64>,
}
```

### 8.3 查询

```rust
fn lookup_prefix(&self, sequence: &ActiveSequence) -> LmCachePrefixHit
```

返回：

```rust
struct LmCachePrefixHit {
    matched_blocks: usize,
    matched_tokens: usize,
    tier: LmCacheTier,
    latency_ms: f64,
}
```

`LmCacheTier`：

```rust
enum LmCacheTier {
    L1,
    L2,
    Miss,
}
```

### 8.4 保存

```rust
fn store_block(&mut self, meta: LmCacheBlockMeta)
```

保存策略：

```text
如果 enable_l1:
  写入 l1_blocks。

如果 enable_l2:
  写入 l2_blocks。

如果 publish_external_events:
  发布 ExternalShared Stored event。
```

第一阶段可以简化为：

```text
store 同时写 L1/L2；
不模拟复杂异步保存；
save latency 只影响 pass end_ms 或 replay timing。
```

### 8.5 删除

```rust
fn remove_block(&mut self, sequence_hash: SequenceHash)
```

用于模拟 LMCache eviction。

第一阶段可只在测试中调用，不接真实 eviction。

### 8.6 Shared metadata lookup

Router 查询 LMCache shared metadata 时，不拉取真实 KV。

接口：

```rust
fn batch_lookup(&self, block_hashes: &[SequenceHash]) -> Vec<LmCacheSharedHit>
```

返回：

```rust
struct LmCacheSharedHit {
    sequence_hash: SequenceHash,
    tier: LmCacheTier,
    estimated_latency_ms: f64,
}
```

用于 router score，而不是 worker load。

---

## 9. KV event 发布

LocalVllmKvCache 负责发布：

```text
Device Stored
Device Removed
ExternalShared Stored
ExternalShared Removed
```

### 9.1 Device Stored

在新 full block 形成时发布：

```text
event = Stored {
  parent_hash,
  blocks: [
    {
      block_hash: sequence_hash,
      tokens_hash: local_hash
    }
  ]
}
storage_tier = Device
```

### 9.2 Device Removed

本地 inactive block 被淘汰时发布：

```text
event = Removed {
  block_hashes: [...]
}
storage_tier = Device
```

### 9.3 ExternalShared Stored

LMCache 保存成功时可发布：

```text
event = Stored { ... }
storage_tier = ExternalShared
```

如果 Pagoda 的 `StorageTier` 还没有 `ExternalShared`，需要新增或映射为 `StorageTier::Disk` / `StorageTier::HostPinned` 的兼容值。推荐新增明确枚举，避免误导。

### 9.4 Parent hash 规则

Stored event 中的 `parent_hash` 表示本批新 block 之前的最后一个已知 prefix block。

规则：

```text
如果从序列第一个 block 开始保存:
  parent_hash = None

如果前面已有 block A:
  parent_hash = A.sequence_hash
```

同一个 Stored event 内连续 blocks 通过顺序隐含链式关系，不需要每个 block 都带 parent。

---

## 10. 淘汰策略

第一阶段本地 cache 可实现简单 LRU。

后续可扩展：

```text
Lineage-aware eviction
Multi-LRU
frequency-aware eviction
LMCache L1/L2 独立 eviction
```

但文档与第一阶段代码必须只声明已实现策略。

推荐第一阶段：

```rust
enum LocalEvictionPolicy {
    Lru,
}
```

---

## 11. 与 Scheduler 的接口

LocalVllmKvCache 对 vLLM scheduler 暴露：

```rust
fn process(&mut self, event: &MoveBlock) -> usize;

fn get_prefill_cost(&self, sequence: &ActiveSequence) -> PrefillCost;

fn num_active_blocks(&self) -> usize;

fn num_active_block_refs(&self) -> usize;

fn num_inactive_blocks(&self) -> usize;

fn max_capacity(&self) -> usize;

fn block_size(&self) -> usize;
```

可选：

```rust
fn lmcache_stats(&self) -> Option<LmCacheMockStats>;

fn shared_metadata_client(&self) -> Option<Arc<dyn SharedCacheMetadataClient>>;
```

---

## 12. 需要模拟的功能场景

### 12.1 Cold prompt

```text
请求 prefix 不在 local cache，也不在 LMCache；
需要新分配全部 prompt blocks；
prefill cost = input_length；
生成 Stored events；
保存到 LMCache。
```

### 12.2 Local device prefix hit

```text
请求 prefix 已在 active/inactive local cache；
prefill cost 减少；
不重新发布 Stored event；
只增加 active refcount。
```

### 12.3 LMCache external hit

```text
请求 prefix 不在 local cache；
LMCache 中存在连续 prefix；
模拟 load latency；
prefill cost 减少；
可将命中的 block materialize 到 local inactive/active；
可发布 Device Stored 或不发布，需固定策略。
```

推荐策略：

```text
LMCache 命中并被当前 worker 使用后，视为加载到 Device；
如果之前 Device 没有该 block，应发布 Device Stored。
```

### 12.4 Decode partial block

```text
decode 生成 token；
如果进入新的 partial block，分配 partial；
partial 不发布 Stored；
partial 满后 Promote 成 full；
Promote 后发布 Stored 并保存到 LMCache。
```

### 12.5 Request complete

```text
请求生成到 max_output_tokens；
释放所有 active blocks；
full block 进入 inactive；
partial block 释放；
如果 inactive 超容量，淘汰并发布 Removed。
```

### 12.6 Preemption

```text
容量不足；
scheduler 选择 victim；
victim 释放 active blocks；
victim 回到 waiting；
后续可通过 local inactive 或 LMCache 复用。
```

### 12.7 Prefix cache disabled

```text
即使 token 相同，也不共享 block；
sequence hash 使用随机值；
get_prefill_cost 永远 cached_tokens=0；
仍然模拟本请求内部 block 分配与释放。
```

---

## 13. 测试要求

```text
1. cold prompt 分配全部 block；
2. 相同 prefix 第二次请求 local 命中；
3. prefix caching disabled 时不命中；
4. partial block promote 产生 Stored；
5. request complete 后 full block 转 inactive；
6. inactive 超容量产生 Removed；
7. preemption 释放 active blocks；
8. LMCache miss 后保存；
9. LMCache hit 降低 prefill cost；
10. LMCache L1 与 L2 命中 latency 不同；
11. LMCache external event 可开关；
12. shared metadata lookup 只返回元数据；
13. Stored event parent_hash 正确；
14. Removed event 只删除对应 block；
15. token_ids 仅在 raw/ZMQ 需要时携带。
```
