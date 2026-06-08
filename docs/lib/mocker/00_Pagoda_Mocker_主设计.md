# Pagoda Mocker 模块主设计文档

## 1. 文档目的

本文档定义 Pagoda 项目中的 `pagoda-mocker` 模块设计。

该模块继承 Dynamo `lib/mocker` 已经实现的核心模拟思路，但根据 Pagoda 当前阶段的工程取舍进行裁剪：

- **只保留 vLLM 引擎模拟路径**；
- **不保留 SGLang 引擎模拟路径**；
- **不保留 KVBM / memory 模块依赖**；
- **不保留 KVBM G1↔G2 offload 模拟模块**；
- **将 KV cache 外部复用能力改为 LMCache 接入模拟**；
- **保留 Router、KV event、loadgen、replay、性能模型、调度行为模拟等通用能力**；
- **尽量继承原模块中与 vLLM、调度、回放、指标、事件相关的已实现设计**；
- **文档必须与目标代码对齐：未实现或暂不实现的能力必须显式标记为预留或移除**。

本文档不是通用 LLM serving 介绍，而是 Pagoda 对 `mocker` 模块的项目设计约束。

---

## 2. 模块定位

`pagoda-mocker` 是 Pagoda 的 **无真实 GPU 推理后端模拟器**。

它用于在不启动真实 vLLM、不加载真实模型、不占用 GPU 的情况下，模拟以下行为：

```text
请求进入 worker
  ↓
vLLM 风格调度器接收请求
  ↓
模拟 prefix cache 命中
  ↓
模拟 prefill / decode 调度
  ↓
模拟 KV block 分配、复用、释放、淘汰
  ↓
模拟 LMCache 外部缓存命中与保存
  ↓
模拟 KV events / raw events / ZMQ relay
  ↓
模拟输出 token 与请求完成
  ↓
产生 replay / benchmark / router / planner 所需指标
```

该模块主要服务于：

- KV Router 测试；
- LMCache 接入策略验证；
- prefix cache 命中率验证；
- vLLM 风格调度行为验证；
- trace replay；
- synthetic workload 压测；
- planner 策略验证；
- CI 中的无 GPU 行为回归测试。

它不用于验证：

- 模型生成质量；
- CUDA / NPU kernel 性能；
- 真实 vLLM connector 的字节级正确性；
- 真实 LMCache L1/L2 存储实现；
- 真实 GPU HBM / CPU pinned memory / disk 的数据搬运；
- KVBM 多级 block manager 行为；
- SGLang radix cache 行为。

---

## 3. 与上游 Dynamo `lib/mocker` 的关系

Pagoda 版本直接继承以下已经实现的设计思想：

```text
MockEngineArgs 配置驱动模拟引擎
DirectRequest 作为直接请求输入
OutputSignal 表示 token 输出与完成状态
vLLM-style waiting/running 调度队列
ActiveSequence 表示请求内 token 序列与 block 序列
block_size / num_gpu_blocks 模拟 KV cache 容量
prefix caching 通过 block hash / sequence hash 复用
chunked prefill 模拟长 prompt 分块
decode step 模拟逐 token 输出
PerfModel 预测 prefill/decode 时间
speedup_ratio / decode_speedup_ratio 调整模拟速度
KvEventPublishers 抽象 KV event 输出
ForwardPassSnapshot 抽象 forward pass metrics
loadgen 生成合成请求与 trace 请求
replay 支持 offline / online 行为模拟
TraceCollector 汇总请求延迟、吞吐、TTFT、TPOT 等报告
```

Pagoda 版本明确移除或替换以下能力：

```text
移除：
  EngineType::Sglang
  SglangArgs
  scheduler/sglang/*
  cache/radix_cache.rs 的 SGLang 专用路径
  kv_manager/sglang_backend.rs
  kvbm_offload/*
  KVBM G2 offload 相关配置项
  kvbm-engine / kvbm-physical / kvbm-common / kvbm-logical 强依赖
  KVBM host/disk/remote tier 精确模拟

替换：
  kv_manager/kvbm_backend.rs
    → Pagoda vLLM local KV cache simulator
    → LMCache mock adapter

保留但收敛：
  replay/offline/disagg.rs
    → 只保留 vLLM prefill/decode 分离模拟；
    → 不保留 SGLang 分支；
    → LMCache transfer/handoff 只模拟时延与元数据，不模拟真实 tensor。
```

---

## 4. 目标模块结构

建议 Pagoda 版本目录结构如下：

```text
lib/runtime/src/mocker/
  mod.rs
  README.md

  common/
    mod.rs
    protocols.rs
    sequence.rs
    perf_model.rs
    running_mean.rs
    utils.rs
    kv_cache_trace.rs
    bootstrap.rs

  engine.rs

  scheduler/
    mod.rs
    kv_event_sink.rs
    vllm/
      mod.rs
      core.rs
      live.rs
      tests.rs

  kv_cache/
    mod.rs
    local_vllm_cache.rs
    lmcache_adapter.rs
    events.rs
    tests.rs

  loadgen/
    mod.rs
    types.rs
    trace.rs
    driver.rs
    tests.rs

  replay/
    mod.rs
    entrypoints.rs
    loader.rs
    collector.rs
    artifacts.rs
    router_shared.rs
    validate.rs
    planner_handle.rs
    offline/
      mod.rs
      single.rs
      agg.rs
      disagg.rs
      state.rs
      events.rs
      core.rs
      progress.rs
      runtime_utils.rs
      components/
        mod.rs
        admission.rs
        engine.rs
        router.rs
        types.rs
    online/
      mod.rs
      entrypoints.rs
      live_runtime.rs
      router.rs
      state.rs
      task.rs
      demux.rs
      tests.rs
```

与原模块相比：

```text
kv_manager/
  不再作为 KVBM 语义模块保留；
  改名为 kv_cache/，表达 Pagoda 自己的 vLLM 本地 KV cache 模拟与 LMCache 适配模拟。

kvbm_offload/
  删除。

scheduler/sglang/
  删除。

cache/radix_cache.rs
  如果仅用于 SGLang，则删除；
  如果后续为 router/indexer 复用，可迁移到 kv_router 模块，不放在 mocker 中。
```

---

## 5. 总体数据流

### 5.1 Live 模式

```text
外部测试或 Python CLI
  ↓
SchedulerHandle::receive(DirectRequest)
  ↓
vLLM Scheduler request channel
  ↓
VllmCore::receive()
  ↓
waiting queue
  ↓
execute_pass_internal()
  ↓
prefill scheduling + decode scheduling
  ↓
LocalVllmKvCache / LmcacheAdapter
  ↓
KvEventPublishers / FpmPublisher
  ↓
OutputSignal
```

Live 模式使用真实 wall-clock sleep，模拟请求在时间上如何推进。

### 5.2 Offline replay 模式

```text
Trace / Synthetic workload
  ↓
WorkloadDriver / loader
  ↓
Offline runtime
  ↓
logical clock now_ms
  ↓
VllmCore::execute_pass(...)
  ↓
SimulationEvent heap
  ↓
TraceCollector
  ↓
TraceSimulationReport
```

Offline 模式不 sleep，而是用逻辑时钟推进，可重复、速度快，适合基准测试和 CI。

---

## 6. 核心模拟行为

Pagoda Mocker 需要模拟以下行为。

### 6.1 vLLM 风格请求调度

继承原 vLLM core 的核心状态机：

```text
Waiting:
  新请求进入等待队列。

Running:
  已被调度并拥有当前活跃 KV footprint。

Preempted:
  因 KV 容量不足被抢占，释放当前 active KV 后回到等待队列。
```

调度器每个 pass 执行：

```text
1. compact running queue
2. 按 token budget 调度 running requests
3. 从 waiting queue admit 新请求
4. 计算 prefill 时间
5. 对 ready requests 执行 decode token emission
6. 生成 OutputSignal
7. 生成 ForwardPassSnapshot
8. 输出 KV events 和 metrics
```

### 6.2 Prefix cache 命中

继承最长连续前缀命中规则：

```text
从请求第一个 full block 开始扫描；
命中则继续；
遇到第一个 miss 后停止；
miss 后面的 block 即使单独存在也不能算 prefix reuse。
```

原因是 KV 依赖顺序上下文，不能跳过中间 block。

### 6.3 KV block 分配与释放

继承原 `MoveBlock` 思路，但在 Pagoda 中不再绑定 KVBM。

推荐保留语义：

```text
Use:
  请求需要使用一个或多个 block；
  可能命中 active / inactive / LMCache；
  也可能新分配本地 vLLM KV block。

Promote:
  decode 过程中 partial block 满后转为 full block；
  可发布 Stored 事件；
  可保存到 LMCache mock adapter。

Deref:
  请求完成或被抢占时释放当前 request-owned block reference；
  block 可以转入 inactive prefix cache；
  或因容量被淘汰并发布 Removed 事件。
```

### 6.4 LMCache 外部缓存模拟

Pagoda 第一阶段不做 KVBM / memory，因此 LMCache 模拟只承担外部缓存语义：

```text
LMCacheAdapter:
  记录哪些 block hash 已被保存；
  模拟 L1 / L2 命中；
  模拟从外部缓存加载 prefix 的收益；
  模拟保存新 full block 到外部缓存；
  可选发布 ExternalShared tier 的 KV events；
  可选返回 shared-cache metadata 给 router。
```

第一阶段不模拟真实 tensor，也不模拟真实 LMCache 内部存储路径。

### 6.5 KV events

Pagoda Mocker 需要模拟两类事件路径：

```text
RouterEvent path:
  用于 offline replay / 测试；
  直接把 KvCacheEvent 转为 RouterEvent 并交给同步 indexer。

Raw/ZMQ path:
  用于 live 模式；
  保留 block token ids 与 storage tier；
  延迟到 pass 的可见点再发布；
  可模拟 vLLM 原生 ZMQ KV event stream。
```

### 6.6 Forward pass metrics

继承 `ForwardPassSnapshot` 语义，记录：

```text
本 pass 中调度了多少 prefill request；
prefill token 总量；
prefill length 方差；
decode request 数量；
decode KV token 总量；
queued prefill / decode 状态；
pass wall time。
```

该指标用于 planner / monitor / replay report。

### 6.7 Timing simulation

继承三类性能模型：

```text
Polynomial:
  默认多项式模型。

Interpolated:
  从 NPZ profile 加载插值模型。

Aiconfigurator:
  通过外部 callback 调用 AIC 预测模型。
```

Pagoda 版本只要求 vLLM 路径可用；SGLang backend name 可删除或作为未来扩展保留。

---

## 7. 与 LMCache-only 目标的边界

Pagoda Mocker 中的 LMCache 模拟不是完整 LMCache 实现。

它只模拟：

```text
哪些 block 被保存；
哪些 block 可被查到；
L1 / L2 命中产生的 prefill 减免；
保存 / 删除事件；
shared metadata 查询；
加载或保存时延。
```

它不模拟：

```text
真实 KV tensor；
真实网络 RPC；
真实文件系统；
真实对象存储；
真实 eviction policy 的所有细节；
真实 LMCache chunk layout；
真实 NIXL / GDS / Mooncake 后端。
```

如需测试真实 LMCache，需要另起集成测试连接真实 LMCache server。

---

## 8. 需要保持与现有代码对齐的能力

从当前模块中应直接继承或轻改的能力：

```text
MockEngineArgs 的核心字段：
  num_gpu_blocks
  block_size
  max_num_seqs
  max_num_batched_tokens
  enable_prefix_caching
  enable_chunked_prefill
  speedup_ratio
  decode_speedup_ratio
  dp_size
  startup_time
  worker_type
  perf_model
  enable_local_indexer
  bootstrap_port
  kv_bytes_per_token
  kv_transfer_bandwidth
  reasoning
  zmq_kv_events_port
  zmq_replay_port
  preemption_mode
  router_queue_policy

DirectRequest:
  tokens
  max_output_tokens
  uuid
  dp_rank
  arrival_timestamp_ms

OutputSignal:
  uuid
  completed
  handoff_delay_ms

ActiveSequence:
  token sequence
  block hashes
  sequence hashes
  partial/full block transition
  generate()
  free_signal()
  reset_with_signal()

VllmCore:
  receive()
  execute_pass()
  execute_hidden_pass()
  schedule_request()
  emit_ready_tokens()
  compute_fpm()

loadgen:
  Mooncake trace loader
  AppliedComputeAgentic trace loader
  synthetic trace
  session partition
  WorkloadDriver

replay:
  offline single
  offline aggregated
  offline disaggregated
  online replay
  TraceCollector
```

---

## 9. 必须删除或改名的能力

以下内容不应出现在 Pagoda LMCache-only vLLM 版本的公开设计中：

```text
EngineType::Sglang
SglangScheduler
SglangCore
SglangArgs
RadixCache 作为 SGLang cache
SGLang schedule policy
SGLang chunked prefill policy
kv_manager::sglang_backend
kvbm_offload
num_g2_blocks
offload_batch_size
bandwidth_g1_to_g2_gbps
bandwidth_g2_to_g1_gbps
MockOffloadEngine
G1/G2 KVBM vocabulary
kvbm-logical as public design dependency
```

如果底层短期为了兼容暂时复用某些代码，也不应在 Pagoda 文档中作为目标设计暴露。

---

## 10. 后续扩展预留

虽然第一阶段只做 vLLM + LMCache，但设计应保留扩展点：

```text
未来可选：
  HiCache mock adapter
  Mooncake shared pool mock adapter
  AscendStore mock adapter
  KVBM mock adapter
  NIXL transfer delay model
  Huawei supernode topology-aware transfer model
  NVIDIA NVLink / RDMA topology-aware transfer model
```

这些扩展必须通过 trait / adapter 接入，不能污染 vLLM + LMCache 基础路径。

---

## 11. 验收标准

Pagoda Mocker 第一阶段完成后，应满足：

```text
功能：
  可以创建 vLLM mock engine；
  可以接收 DirectRequest；
  可以模拟 waiting/running/preempted 状态；
  可以模拟 prefix cache；
  可以模拟 chunked prefill；
  可以模拟 decode token emission；
  可以模拟 preemption；
  可以模拟 LMCache external shared hit；
  可以发布 KV Stored / Removed events；
  可以输出 ForwardPassSnapshot；
  可以运行 loadgen synthetic workload；
  可以运行 trace replay；
  可以输出 TraceSimulationReport。

边界：
  不依赖 KVBM；
  不依赖 memory 模块；
  不暴露 SGLang 引擎；
  不真实读写 KV tensor；
  不要求真实 LMCache server；
  不要求 GPU。

测试：
  vLLM scheduler 单元测试通过；
  sequence block 转换测试通过；
  KV event 发布测试通过；
  replay 单 worker / 多 worker / kv_router 测试通过；
  LMCache adapter mock hit/miss 测试通过；
  trace loader 测试通过。
```

---

## 12. 文档拆分

本设计拆分为以下文档：

```text
00_Pagoda_Mocker_主设计.md
01_配置与协议设计.md
02_vLLM调度器模拟设计.md
03_KV缓存与LMCache模拟设计.md
04_KV事件与ZMQ转发设计.md
05_性能模型与指标设计.md
06_Loadgen与Replay设计.md
07_迁移裁剪与继承清单.md
```

主文档负责边界与总览；子文档负责具体模块行为。
