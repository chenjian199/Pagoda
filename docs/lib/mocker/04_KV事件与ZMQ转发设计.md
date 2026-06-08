# Pagoda Mocker KV 事件与 ZMQ 转发设计

## 1. 模块目标

本文件定义 Pagoda Mocker 中 KV event 发布、缓冲、捕获、ZMQ raw event 转发的设计。

该部分继承当前模块的核心设计：

```text
KvCacheEventSink
RawKvEventSink
KvEventPublishers
CapturedRouterEventBuffer
DeferredKvPublishBuffer
RouterEventCaptureSink
DeferredKvEventSink
DeferredRawKvEventSink
publish_deferred_kv_events
```

Pagoda 第一阶段只支持 vLLM mock engine，因此事件格式以 vLLM-style KV event 为主。

---

## 2. 事件设计原则

### 2.1 KV event 是路由元数据

KV event 不携带真实 KV tensor，也不携带真实存储物理位置。

它表达：

```text
某个 worker / dp_rank / storage_tier 上，
某些 KV block 被 Stored 或 Removed。
```

### 2.2 Router 不通过 event 取 KV

Router 只用 event 更新 prefix indexer。

真正取回 KV 的行为由 worker 侧 connector 或 LMCache adapter 完成。

### 2.3 Mock event 必须接近真实后端

Pagoda Mocker 发布的事件应尽量接近 vLLM native KV event 的语义：

```text
Stored:
  block_hash
  tokens_hash
  parent_hash
  dp_rank

Removed:
  block_hashes
  dp_rank
```

这样可以测试 router / indexer / replay 的真实路径。

---

## 3. 事件发布接口

### 3.1 KvCacheEventSink

```rust
trait KvCacheEventSink {
    fn publish(&self, event: KvCacheEvent) -> anyhow::Result<()>;

    fn publish_with_storage_tier(
        &self,
        event: KvCacheEvent,
        storage_tier: StorageTier,
    ) -> anyhow::Result<()>;
}
```

用途：

```text
发布标准化 KV event；
用于 RouterEvent / event plane；
适合 offline replay 与直接测试。
```

### 3.2 RawKvEventSink

```rust
trait RawKvEventSink {
    fn publish(&self, event: RawKvEvent) -> anyhow::Result<()>;
}
```

用途：

```text
发布更接近 vLLM ZMQ wire format 的 raw event；
保留 block_token_ids；
保留 storage_tier；
适合 live 模式模拟 ZMQ publisher。
```

### 3.3 KvEventPublishers

```rust
struct KvEventPublishers {
    event_sink: Option<Arc<dyn KvCacheEventSink>>,
    raw_sink: Option<Arc<dyn RawKvEventSink>>,
}
```

调用逻辑：

```text
publish_with_storage_tier(event, block_token_ids, storage_tier):
  如果 event_sink 存在：
    发布标准 event。

  如果 raw_sink 存在：
    发布 RawKvEvent。
```

---

## 4. Stored 事件

Stored 事件在以下场景产生：

```text
新 full block 被本地 Device cache 注册；
partial block promote 成 full block；
LMCache external shared block 保存成功；
LMCache hit 后 materialize 到 Device。
```

事件结构：

```text
KvCacheEvent {
  event_id,
  dp_rank,
  data: Stored {
    parent_hash,
    start_position,
    blocks: [
      {
        block_hash,
        tokens_hash,
        mm_extra_info
      }
    ]
  }
}
```

### 4.1 parent_hash

`parent_hash` 用于 router radix tree 建链。

规则：

```text
如果本批 stored blocks 从序列开头开始：
  parent_hash = None

如果本批 stored blocks 前面已有 prefix block：
  parent_hash = previous full block sequence_hash
```

### 4.2 tokens_hash

`tokens_hash` 是当前 block 的 local token hash，用于 router 区分相同 sequence hash 与 token 内容关系。

### 4.3 block_token_ids

Raw event 可携带完整 token ids：

```text
block_token_ids: Option<Vec<Vec<u32>>>
```

用途：

```text
vLLM native ZMQ event；
debug；
replay；
下游需要按 token 重建 block 的场景。
```

---

## 5. Removed 事件

Removed 事件在以下场景产生：

```text
本地 inactive prefix cache 淘汰；
LMCache external block 被删除；
测试主动模拟 eviction；
请求关闭导致某些非复用 partial block 释放时，如果该 block 曾暴露给 router，则需要 removed。
```

事件结构：

```text
KvCacheEvent {
  event_id,
  dp_rank,
  data: Removed {
    block_hashes: [...]
  }
}
```

注意：

```text
请求 Deref 并不一定发布 Removed；
只有 router 视图中应该删除 residency 时才发布 Removed。
```

例如：

```text
active full refcount 归零进入 inactive：
  不发布 Removed，因为 router 仍可认为 worker 有该 prefix。

inactive 被真正淘汰：
  发布 Removed。
```

---

## 6. StorageTier

Pagoda 第一阶段建议至少支持：

```rust
enum StorageTier {
    Device,
    ExternalShared,
}
```

如果暂时沿用已有 enum，可映射：

```text
Device:
  vLLM 本地 GPU paged KV buffer / local prefix cache。

ExternalShared:
  LMCache L1/L2 共享缓存。
```

后续扩展：

```text
HostPinned
LocalDisk
RemoteStorage
Mooncake
AscendStore
```

---

## 7. Offline replay 事件捕获

Offline 模式中不需要真实 event plane。

使用：

```text
CapturedRouterEventBuffer
RouterEventCaptureSink
```

流程：

```text
LocalVllmKvCache 发布 KvCacheEvent
  ↓
RouterEventCaptureSink
  ↓
RouterEvent::new / with_storage_tier
  ↓
CapturedRouterEventBuffer
  ↓
EnginePassResult.kv_events
  ↓
OfflineReplayRouter.apply_events(...)
```

特点：

```text
同步；
无网络；
可重复；
适合 CI 和离线 replay；
不保留 raw token ids。
```

---

## 8. Live 模式事件延迟发布

Live 模式中，core 内部事件不会立即发送到真实 sink，而是先缓冲。

组件：

```text
DeferredKvPublishBuffer
DeferredKvEventSink
DeferredRawKvEventSink
```

流程：

```text
VllmCore 内部发布 event
  ↓
Deferred sink 捕获
  ↓
execute_pass_internal 返回 pass
  ↓
根据 RouterEventVisibility 判断发布时间
  ↓
publish_deferred_kv_events(...)
  ↓
真实 KvEventPublishers
```

目的：

```text
让事件在模拟时间轴上的可见时间与 pass 行为一致；
避免事件在模拟 prefill/decode sleep 前过早暴露。
```

---

## 9. RouterEventVisibility

保留两种：

```rust
enum RouterEventVisibility {
    PassStart,
    PassEnd,
}
```

语义：

```text
PassStart:
  pass 开始时对 router 可见。
  当前 vLLM Device Stored 通常使用该模式。

PassEnd:
  pass 完成后对 router 可见。
  适合某些异步保存或传输完成后才可见的事件。
```

LMCache external shared event 建议：

```text
如果 save_latency_ms = 0:
  可在 PassStart 或当前 pass 内立即可见。

如果 save_latency_ms > 0:
  应延迟到保存完成后可见。
  第一阶段可简化为 PassEnd。
```

---

## 10. ZMQ raw event 模拟

### 10.1 目标

模拟 vLLM native KV event publisher。

链路：

```text
Pagoda Mocker
  ↓
RawKvEventSink
  ↓
ZMQ PUB socket
  ↓
Pagoda / Dynamo-style relay
  ↓
event plane
  ↓
router indexer
```

### 10.2 配置

```rust
zmq_kv_events_port: Option<u16>
zmq_replay_port: Option<u16>
```

语义：

```text
zmq_kv_events_port:
  开启 ZMQ PUB 事件输出。

zmq_replay_port:
  开启 buffered batch replay。
```

### 10.3 Raw event 内容

```rust
struct RawKvEvent {
    event: KvCacheEvent,
    block_token_ids: Option<Vec<Vec<u32>>>,
    storage_tier: StorageTier,
}
```

### 10.4 replay port

`zmq_replay_port` 用于请求特定 sequence number 的 buffered event batch。

该能力保留现有设计，但第一阶段可以作为可选功能。

---

## 11. LMCache event 适配

LMCache 模拟层可能产生：

```text
ExternalShared Stored
ExternalShared Removed
```

需要满足：

```text
不暴露真实 LMCache 存储路径；
只暴露 block hash / parent hash / tokens hash；
storage_tier = ExternalShared；
可选携带 block_token_ids。
```

### 11.1 LMCache hit 后是否发布 Device Stored

推荐策略：

```text
当 LMCache hit 的 block 被加载并在本地 Device cache materialize 时：
  发布 Device Stored。

仅 LMCache 中存在但未加载到当前 worker：
  不发布 Device Stored。
```

这样 router 能区分：

```text
worker 本地已有；
shared cache 可取。
```

---

## 12. Forward Pass Metrics 发布

保留 `FpmPublisher`：

```rust
trait FpmSink {
    fn publish(&self, snapshot: ForwardPassSnapshot) -> anyhow::Result<()>;
}
```

Live 模式下与 KV event 一样先 deferred，再在合适时间发布。

---

## 13. 测试要求

```text
1. Stored event event_id 递增；
2. Stored event parent_hash 正确；
3. Removed event block_hashes 正确；
4. Device event storage_tier 正确；
5. ExternalShared event storage_tier 正确；
6. event_sink 和 raw_sink 同时存在时都能收到；
7. raw_sink 保留 block_token_ids；
8. deferred buffer 合并同 event_id 的 raw token ids；
9. offline capture 生成 RouterEvent；
10. live deferred publish 按 visibility flush；
11. LMCache save 可发布 ExternalShared Stored；
12. LMCache eviction 可发布 ExternalShared Removed；
13. request Deref 到 inactive 不误发 Removed；
14. inactive eviction 才发 Removed。
```
