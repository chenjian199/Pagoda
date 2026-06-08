# pagoda-mocker

`pagoda-mocker` 是一个不需要 GPU 的模拟 crate，用于模拟 LLM 推理中的调度行为和 KV cache 行为。

它主要用于测试、回放和基准测试场景。当你希望获得接近真实推理引擎的调度与缓存行为，但又不想启动真实推理引擎时，可以使用这个模块。

## 这个 crate 提供什么

- `MockEngineArgs`：用于配置模拟引擎。
- `engine::create_engine`：用于创建 vLLM 风格的模拟调度器。
- `KvEventPublishers`：用于发布 Router 可感知的 KV cache 事件。
- `loadgen` 和 `replay` 模块：用于合成 workload，以及基于 trace 的实验回放。

## 基础 Rust 用法

```rust
use pagoda_mocker::common::protocols::{
    DirectRequest, KvEventPublishers, MockEngineArgs,
};
use pagoda_mocker::engine::create_engine;

let args = MockEngineArgs::builder()
    .block_size(16)
    .num_gpu_blocks(1024)
    .max_num_seqs(Some(32))
    .max_num_batched_tokens(Some(4096))
    .build()
    .unwrap();

let engine = create_engine(args, 0, None, KvEventPublishers::default(), None);

engine.receive(DirectRequest {
    tokens: vec![1, 2, 3, 4],
    max_output_tokens: 16,
    uuid: None,
    dp_rank: 0,
    arrival_timestamp_ms: None,
});
```

这个 crate 也是 Pagoda 上层 mocker CLI 和 replay 工具的基础。在很多部署场景中，你通常不会把它作为一个独立的 Rust 依赖直接嵌入使用，而是会通过 Python 入口间接使用它。

