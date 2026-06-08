# Pagoda Mocker Loadgen 与 Replay 设计

## 1. 模块目标

本文件定义 Pagoda Mocker 的 workload 生成、trace 加载、offline replay、online replay 与 report 输出设计。

该部分继承原模块中：

```text
loadgen/types.rs
loadgen/trace.rs
loadgen/driver.rs
replay/*
replay/offline/*
replay/online/*
replay/collector.rs
```

但按 Pagoda 当前范围裁剪：

```text
只保留 vLLM engine；
不保留 SGLang engine 分支；
不保留 KVBM offload replay；
LMCache 只作为 external shared cache 模拟层参与 replay。
```

---

## 2. Loadgen 数据模型

### 2.1 Trace

```rust
struct Trace {
    block_size: usize,
    sessions: Vec<SessionTrace>,
}
```

语义：

```text
整个 workload；
包含多个 session；
每个 session 包含多个 turn。
```

### 2.2 SessionTrace

```rust
struct SessionTrace {
    session_id: String,
    first_arrival_timestamp_ms: Option<f64>,
    turns: Vec<TurnTrace>,
}
```

语义：

```text
一个会话；
多轮请求共享上下文；
后续 turn 的释放时间受上一 turn 完成时间和 delay 控制。
```

### 2.3 TurnTrace

```rust
struct TurnTrace {
    input_length: usize,
    max_output_tokens: usize,
    hash_ids: Vec<u64>,
    delay_after_previous_ms: f64,
}
```

语义：

```text
单轮请求；
hash_ids 用于合成具有相同 prefix 的 token；
input_length 表示输入 token 数；
max_output_tokens 表示输出长度。
```

---

## 3. Trace 文件格式

保留两种格式：

```rust
enum TraceFileFormat {
    Mooncake,
    AppliedComputeAgentic,
}
```

### 3.1 Mooncake trace

字段支持：

```text
session_id
timestamp / created_time
input_length / input_tokens
output_length / output_tokens
hash_ids
delay / delay_ms
```

处理规则：

```text
hash_ids 是 prefix block 的来源；
如果 input_length 超过 hash_ids * trace_block_size，则截断到可合成容量；
同一 session 的多条记录会组成多 turn；
delay_after_previous_ms 可由 timestamp 差或显式 delay 得到。
```

### 3.2 AppliedComputeAgentic trace

用于 agentic 多轮 workload。

字段：

```text
num_turns
input_prompt_length
assistant_response_length
tool_call_output_length
tool_call_latency
final_assistant_response_length
```

处理目标：

```text
将 agentic row 展开成多 turn；
模拟 assistant 输出、tool call 输出、tool latency、final response。
```

---

## 4. Synthetic workload

保留 `SyntheticTraceSpec`：

```rust
struct SyntheticTraceSpec {
    block_size,
    num_sessions,
    turns_per_session,
    input_tokens,
    output_tokens,
    shared_prefix_ratio,
    num_prefix_groups,
    first_turn_arrivals,
    inter_turn_delays,
    seed,
}
```

用途：

```text
生成可控 workload；
测试 prefix sharing；
测试 router 行为；
测试多 session / 多 turn；
测试 arrival pattern。
```

### 4.1 ArrivalSpec

```rust
enum ArrivalSpec {
    Burst,
    ConstantQps { qps },
    PoissonQps { qps },
    GammaQps { qps, smoothness },
}
```

### 4.2 DelaySpec

```rust
enum DelaySpec {
    None,
    ConstantMs(f64),
    ExponentialMs { mean_ms },
}
```

### 4.3 shared_prefix_ratio

模拟不同 session 或 prefix group 之间共享前缀。

```text
ratio 越高，相同 prefix 越多；
KV router / LMCache 命中越明显。
```

---

## 5. WorkloadDriver

`WorkloadDriver` 负责多 turn session 的 ready 管理。

核心语义：

```text
1. 第一轮请求按 arrival spec 就绪。
2. turn 完成后，后续 turn 需要等待 delay_after_previous_ms。
3. concurrency 模式下，也必须保证同一 session 的后续 turn 不早于前一 turn 完成。
4. next_ready_time 用于 runtime 决定下一次醒来时间。
```

该设计应保留，因为它能模拟真实对话式多轮流量。

---

## 6. Replay 模式

保留两类 replay：

```rust
enum ReplayArgsMode {
    Trace,
    Concurrency,
}
```

### 6.1 Trace 模式

特点：

```text
尊重原始 arrival timestamp；
第一个请求归一化到 0ms；
arrival_speedup_ratio 可压缩或拉伸到达间隔；
workload 模式尊重 first-turn timestamp 和 inter-turn delay。
```

### 6.2 Concurrency 模式

特点：

```text
忽略原始 first-turn spacing；
保持最多 max_in_flight 个请求；
适合测稳态吞吐；
多 turn 仍受 completion + delay 约束。
```

---

## 7. Offline replay

Offline replay 不 sleep，使用逻辑时钟。

### 7.1 Single-worker fast path

适用：

```text
num_workers == 1
engine = vLLM
```

流程：

```text
pending requests
  ↓
按 trace 或 concurrency admission
  ↓
ReplayWorkerCore::execute_pass
  ↓
current_time_ms = pass.end_ms
  ↓
TraceCollector
  ↓
finish
```

优点：

```text
最简单；
最快；
不需要 router；
适合单 worker 单元测试和基准。
```

### 7.2 Aggregated multi-worker

模拟多个 aggregated vLLM worker。

核心组件：

```text
logical clock now_ms
pending request queue
OfflineWorkerState per worker
SimulationEvent heap
OfflineReplayRouter optional
TraceCollector
```

主循环：

```text
1. 选择下一个有意义时间：
   - next arrival
   - next worker completion

2. 推进 now_ms。

3. 应用已完成 worker pass。

4. admit 新请求。

5. 对 ready worker 启动 pass。

6. 将新的 WorkerCompletion 放回 event heap。
```

### 7.3 Offline router

支持：

```text
round_robin
kv_router
```

`round_robin`：

```text
按 worker 顺序分配。
```

`kv_router`：

```text
使用本地同步 indexer；
使用 active load state；
使用 KV events；
可选使用 LMCache shared metadata；
计算路由分数。
```

### 7.4 Disaggregated replay

保留 vLLM P/D 分离模拟：

```text
prefill router + prefill worker pool
decode router + decode worker pool
```

流程：

```text
1. 请求进入 prefill router。
2. prefill worker 模拟 prompt KV 生成。
3. prefill 完成后产生 DecodeHandoff。
4. 根据 handoff_delay_ms 安排 decode admission。
5. decode worker 继续生成输出。
6. TraceCollector 对外报告 decode-visible 完成，但 TTFT 包含 prefill 排队与计算。
```

Pagoda 第一阶段可保留结构，但实际测试重点是 aggregated LMCache-only。

---

## 8. Online replay

Online replay 使用 live scheduler 与真实 wall-clock。

组件：

```text
LiveRuntime
ReplayRouter
task
demux
state
```

用途：

```text
更接近真实 runtime；
测试 async request channel；
测试 output signal demux；
测试 metrics watch；
测试 cancellation。
```

第一阶段保留 vLLM 路径，删除 SGLang 相关分支。

---

## 9. Router integration

Replay router 需要消费：

```text
KV events
active load
admission events
OutputSignal
```

Offline 模式：

```text
worker pass 返回 kv_events；
runtime 同步 apply 到 router indexer。
```

Online 模式：

```text
通过 event sink / replay router sink 更新 indexer。
```

LMCache-only 增强：

```text
如果 LmCacheMockAdapter 支持 shared metadata lookup，
router 在 score 时可查询 external shared hits。
```

---

## 10. TraceCollector

记录：

```text
arrival
admission
token emission
completion
reused_input_tokens
```

输出：

```text
TraceSimulationReport
```

报告包含：

```text
请求数；
完成数；
吞吐；
TTFT；
TPOT / ITL；
端到端延迟；
分布统计；
percentile；
复用统计。
```

---

## 11. Artifacts

保留：

```rust
ReplayTimedRequest
ReplayTimedOutputSignal
ReplayTimedKvEvent
ReplayWorkerArtifacts
```

用途：

```text
保存 replay 中每个 worker 的时间序列；
回放分析；
debug router 行为；
对比不同策略。
```

---

## 12. Validation

Replay 启动前必须检查：

```text
num_workers > 0
block_size > 0
trace_block_size > 0
max_in_flight 合法
router mode 与 worker 数兼容
disagg 参数合法
engine 只允许 vLLM
SGLang 参数不允许
KVBM offload 参数不允许
```

---

## 13. LMCache 场景测试

新增或改造 replay 测试：

```text
1. 相同 prefix trace 第二次请求命中 LMCache；
2. round_robin 下 LMCache shared hit 仍可降低 prefill；
3. kv_router 下 local hit 优先于 external shared hit；
4. LMCache shared metadata 可影响 worker 选择；
5. LMCache hit latency 影响 TTFT；
6. save_latency 不导致请求丢失；
7. ExternalShared event 被 indexer 消费；
8. concurrency 模式保持 max_in_flight；
9. multi-turn session 不提前发后续 turn；
10. disagg prefill handoff delay 计入 TTFT。
```
