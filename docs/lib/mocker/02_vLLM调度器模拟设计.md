# Pagoda Mocker vLLM 调度器模拟设计

## 1. 模块目标

本文件定义 Pagoda Mocker 中 vLLM 风格调度器的模拟设计。

该模块继承当前 `scheduler/vllm/core.rs` 与 `scheduler/vllm/live.rs` 已经实现的主要行为：

```text
waiting / running 队列
request state map
continuous batching
token budget
max_num_seqs
chunked prefill
prefix cache reuse
decode token emission
memory pressure preemption
ForwardPassSnapshot
MockerMetrics
OutputSignal
```

同时移除：

```text
SGLang scheduler 分支
KVBM offload parking / swap-in 逻辑
G1/G2 offload 相关路径
```

---

## 2. 核心状态

### 2.1 RequestStatus

保留三种状态：

```rust
enum RequestStatus {
    Waiting,
    Running,
    Preempted,
}
```

语义：

```text
Waiting:
  新请求或被抢占请求正在等待 admission。

Running:
  请求已经被调度，持有当前 active KV footprint。

Preempted:
  请求曾经进入 running，但因容量不足释放 KV 后回到等待队列。
```

### 2.2 VllmRequestState

```rust
struct VllmRequestState {
    sequence: ActiveSequence,
    status: RequestStatus,
    num_computed_tokens: usize,
    num_preemptions: usize,
}
```

字段说明：

```text
sequence:
  请求对应的 token 序列、block 序列和生成状态。

status:
  当前调度状态。

num_computed_tokens:
  已完成 prefill/decode 计算的 token 数。
  prefix cache 命中时，逻辑上会把 cached_tokens 纳入 computed 进度。

num_preemptions:
  被抢占次数，用于测试和调试。
```

### 2.3 SchedulerState

```rust
struct SchedulerState {
    waiting: VecDeque<Uuid>,
    waiting_members: FxHashSet<Uuid>,
    running: VecDeque<Uuid>,
    running_members: FxHashSet<Uuid>,
    requests: FxHashMap<Uuid, VllmRequestState>,
}
```

设计要点：

```text
VecDeque:
  保留队列顺序。

members set:
  防止同一个 uuid 重复进入队列。

requests map:
  保存所有未完成请求的状态。
```

`waiting` 和 `running` 可能包含已经过期的 uuid，因此每次调度前需要 compact。

---

## 3. 请求接收流程

入口：

```rust
VllmCore::receive(request: DirectRequest) -> Uuid
```

流程：

```text
1. 如果 request.uuid 存在，使用该 uuid；
   否则生成新的 uuid。

2. 创建 ActiveSequence：
   - tokens
   - max_output_tokens
   - block_size
   - enable_prefix_caching
   - emit_token_ids

3. 插入 state.requests。

4. 将 uuid 放入 waiting queue。

5. 返回 uuid。
```

设计约束：

```text
receive 只入队，不立即执行；
receive 不做真实模型推理；
receive 不直接发布输出 token；
receive 不直接调用 LMCache load/save。
```

---

## 4. 单个调度 pass

核心入口：

```rust
execute_pass_internal(...)
```

一个 pass 模拟一次 vLLM scheduler iteration。

流程：

```text
1. 记录本 pass 开始时请求数量。

2. 清理 running queue 中的无效项。

3. 初始化 token_budget。
   token_budget 来自 max_num_batched_tokens。

4. 先调度已有 running 请求。
   这些通常是 decode 或尚未完成 prefill 的请求。

5. 在没有发生 preemption 且 running 数未超过 max_num_seqs 时，
   从 waiting queue admit 新请求。

6. 根据本 pass 调度到的 prefill work 预测 prefill_time。

7. 对 ready requests 执行 decode token emission。

8. 根据 prefill_time + decode_time 得到 pass end_ms。

9. 计算 ForwardPassSnapshot。

10. 返回 EnginePassResult。
```

---

## 5. Running 请求调度

调度已有 running 请求时：

```text
for uuid in running:
  if token_budget > 0:
    schedule_request(uuid, from_waiting = false)
```

特点：

```text
running 请求优先于 waiting 请求；
running 请求可能继续 decode；
running 请求也可能继续 chunked prefill；
如果容量不足，可能触发 preemption。
```

---

## 6. Waiting 请求 admission

admission 条件：

```text
running.len() < max_num_seqs
token_budget > 0
没有刚刚发生 preemption
```

流程：

```text
1. 从 waiting 队首取 uuid。
2. 查询 prefix cache cost。
3. 尝试 schedule_request(uuid, from_waiting = true)。
4. 成功后 transition_to_running。
5. 生成 AdmissionEvent。
```

AdmissionEvent 包含：

```rust
struct AdmissionEvent {
    uuid: Uuid,
    reused_input_tokens: usize,
}
```

`reused_input_tokens` 用于 replay collector 统计 prefix reuse。

---

## 7. Prefix cache 与 computed progress

调度请求时，会先计算：

```text
cached_prefix_tokens =
  如果 num_computed_tokens == 0:
    kv_cache.get_prefill_cost(sequence).cached_tokens
  否则:
    0
```

然后：

```text
effective_computed_before =
  request.num_computed_tokens + cached_prefix_tokens
```

语义：

```text
如果请求第一次进入 prefill，
prefix cache 已经命中的 token 被视为已计算。

如果请求已经计算过一部分，
不重复把 cached prefix 加进去。
```

这与 vLLM 的 prefix reuse 语义一致：命中的 prefix 不再重新 prefill。

---

## 8. Chunked prefill

如果 prompt_remaining 大于当前 token_budget：

```text
enable_chunked_prefill = true:
  允许只计算一部分 prompt tokens；
  下一个 pass 继续 prefill。

enable_chunked_prefill = false:
  如果 token_budget 不足以容纳剩余 prompt，则阻塞。
```

这模拟 vLLM 长 prompt 分块 prefill。

---

## 9. Token budget

`max_num_batched_tokens` 限制一个 pass 最多计算多少 token。

使用方式：

```text
desired_tokens =
  min(remaining_known_tokens, token_budget)
```

调度成功后：

```text
token_budget -= tokens_used
```

作用：

```text
避免单个 pass 中无限处理所有请求；
模拟真实 vLLM batch token budget；
让长 prompt 与其它请求共享调度机会。
```

---

## 10. KV block 分配

调度请求需要推进到 `desired_computed_after` 时，会调用：

```text
sequence.prepare_allocation(allocation_target)
```

产生 `MoveBlock::Use`。

Pagoda 版本中由：

```text
LocalVllmKvCache.process_use(...)
```

处理。

可能结果：

```text
全部分配成功：
  commit_allocation(allocation_target)

部分分配成功：
  commit 到已经分配的 block 边界；
  触发 preemption 尝试释放容量。

完全分配失败：
  阻塞或 preempt。
```

---

## 11. Preemption

当 KV block 分配失败时，调度器会尝试抢占 running 请求。

保留两种策略：

```rust
PreemptionMode::Lifo
PreemptionMode::Fifo
```

语义：

```text
Lifo:
  抢占最新进入 running 的请求。

Fifo:
  抢占最早进入 running 的请求。
```

抢占流程：

```text
1. 从 running queue 选择 victim。
2. victim.status = Preempted。
3. victim.num_computed_tokens = 0。
4. victim.num_preemptions += 1。
5. victim.sequence.reset_with_signal() 生成 Deref signals。
6. KV cache 释放 victim 当前 active blocks。
7. victim 被放回 waiting 队首。
```

注意：

```text
抢占释放 active KV footprint；
请求 token 历史仍保留；
重新 admission 时可通过 prefix cache / LMCache 复用已保存 block。
```

---

## 12. Prefill 时间预测

本 pass 中所有调度到的 prefill work 会累计：

```text
batch_count
batch_total_isl
batch_total_prefix
```

然后：

```rust
predict_prefill_duration(batch_count, batch_total_isl, batch_total_prefix, args)
```

逻辑：

```text
如果 batch_count == 0:
  prefill time = 0

如果 worker_type == Decode:
  prefill time = 0

否则：
  mean_isl = batch_total_isl / batch_count
  mean_prefix = batch_total_prefix / batch_count
  perf_model.predict_prefill_time(batch_count, mean_isl, mean_prefix)
  再除以 speedup_ratio
```

作用：

```text
模拟 prefix cache 命中会降低实际新计算 token 数；
模拟 batch size 和 input length 对 TTFT 的影响。
```

---

## 13. Decode token emission

当请求满足：

```text
num_computed_tokens >= sequence.len()
generated_tokens < max_output_tokens
```

则该请求 ready to decode。

decode 时间预测输入：

```text
batch_size = ready.len()
active_kv_tokens = kv_cache.num_active_blocks() * block_size
total_kv_tokens = num_gpu_blocks * block_size
context_length = ready requests 的平均 sequence length
```

然后：

```text
perf_model.predict_decode_time(...)
decode_time /= speedup_ratio * decode_speedup_ratio
```

每个 ready request 执行：

```text
sequence.generate()
  ↓
可能产生 MoveBlock::Promote
  ↓
可能产生 MoveBlock::Use 新 partial block
  ↓
可能产生 MoveBlock::Deref 完成释放
```

如果 decode 期间新 block 分配失败，仍可能触发 preemption。

---

## 14. Prefill worker 特殊行为

在 P/D 分离中，如果：

```text
worker_type == Prefill
```

则 ready request 的第一个 decode token 被视为 prefill forward pass 的一部分，不额外增加 decode iteration：

```text
decode_time = 0
decode_end_ms = decode_start_ms
```

同时 `OutputSignal` 可带：

```text
handoff_delay_ms
```

用于模拟 prefill KV 传给 decode worker 的延迟。

---

## 15. OutputSignal 生成

每个成功 emit token 的请求生成：

```rust
OutputSignal {
    uuid,
    completed,
    handoff_delay_ms,
}
```

如果：

```text
generated_tokens >= max_output_tokens
```

则：

```text
completed = true
state.complete(uuid)
```

否则请求继续留在 running。

---

## 16. ForwardPassSnapshot

每个 pass 结束后计算：

```text
scheduled_prefills
scheduled_decodes
queued_prefills
queued_decodes
wall_time_secs
```

输出：

```rust
ForwardPassSnapshot
```

用于：

```text
planner
monitoring
replay report
debugging
```

---

## 17. Live Scheduler

`Scheduler` 是 live 模式包装器。

职责：

```text
1. 创建 request channel；
2. 创建 metrics watch channel；
3. spawn tokio task；
4. 循环接收请求；
5. 调用 VllmCore::execute_pass_internal；
6. 按 pass duration sleep；
7. flush KV events / FPM；
8. flush OutputSignal；
9. 更新 MockerMetrics。
```

### 17.1 Deferred KV event

Live 模式不立即发布 core 内部事件，而是先缓冲，再根据 pass 可见性发布。

作用：

```text
保证事件在模拟时间轴上的可见点与 pass 行为一致；
避免在 prefill/decode sleep 之前或之后暴露错误状态。
```

### 17.2 Metrics

`MockerMetrics` 包含：

```rust
dp_rank
active_decode_blocks
total_blocks
gpu_cache_usage_perc
```

用于 Router / Planner 观察 worker 当前负载。

---

## 18. 删除的 vLLM 调度路径

Pagoda 版本不保留以下 KVBM offload 代码路径：

```text
requests_awaiting_swap_in
tick_and_promote_swap_ins
complete_swap_in
try_park_for_swap_in
init_offload_live
init_offload_offline
BlockedOnG1Offload
G2→G1 swap-in
G1→G2 offload
```

LMCache 相关模拟应通过 `LmCacheMockAdapter` 的 hit/save latency 完成，不模拟 KVBM swap-in。

---

## 19. 测试要求

vLLM scheduler 至少覆盖：

```text
1. 单请求完成；
2. 多请求 continuous batching；
3. max_num_seqs 限制；
4. max_num_batched_tokens 限制；
5. chunked prefill 开关；
6. prefix cache hit；
7. prefix cache disabled；
8. decode 生成到 max_output_tokens；
9. preemption LIFO；
10. preemption FIFO；
11. request completed 后释放 KV；
12. output_tx 关闭后 drop request；
13. ForwardPassSnapshot 统计；
14. MockerMetrics 更新；
15. LMCache hit 对 prefill cost 的影响；
16. LMCache miss 后保存新 block；
17. KV event Stored / Removed 发布。
```
