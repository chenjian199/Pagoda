# 推理内容解析器边界用例

本文档是 `src/reasoning/` 下 **推理内容解析器** 单元测试的中文分类标准，适用于 Granite、GPT-OSS、Gemma、Qwen3 think-tag、Minimax、DeepSeek V3 think-tag 等 parser。

相邻阶段的测试分类包括：

- **工具调用解析器**（`src/tool_calling/`）：见 `PARSER_CASES.md`。
- **前端门控逻辑**：见 `FRONTEND_CASES.md`。
- **流水线边界**：见 `PIPELINE_CASES.md`。

推理内容测试分类沿用 parser 测试分类的阶段、模式、格式三个维度：

- **阶段**：本文档使用 `REASONING.*`。
- **模式**：`batch` 表示一次性输入完整模型输出字符串；`stream` 表示增量输入 `delta_text`。
- **格式**：当 reasoning parser 消费了 `PARSER.fmt|xml|harmony.*` 对应格式时，也应附加这些格式标签。格式标签描述语法，不描述 parser 阶段。

每个 `#[test]` 应带一个或多个 `// REASONING.<tag>` 注释；适用时也应带格式标签。`N/A` 需要明确写出。

---

## 快速参考

### Reasoning，batch 模式

编号尽量与 `PARSER.batch.{N}` 对齐，方便说明工具调用侧与 reasoning 侧测试的是相似形态。有些编号在 reasoning 侧没有直接对应，例如 reasoning block 没有“空参数”概念，因此应明确标记 `N/A`。

- **`REASONING.batch.1`** 单个推理块，正常路径：存在 `<think>...</think>` 或等价形式，没有工具调用。解析器填充 `reasoning_text`，`tool_calls` 为空。
- **`REASONING.batch.2`** 推理内容 + 下游工具调用：模型输出 `<think>...</think>` 后接工具调用 token。reasoning parser 必须提取 think 内容，并把工具调用标记完整保留在 `normal_text` 中，供下游 tool-call parser 消费。重点防止“未闭合 think-tag 吞掉 tool call”的 bug。
- **`REASONING.batch.3`** 推理内容 + 普通文本，无工具调用：`<think>...</think>` 与用户可见叙述交错。推理内容进入 `reasoning_text`，叙述进入 `normal_text`。
- **`REASONING.batch.4`** 异常推理内容：孤立结束标记、只有开始标记没有结束标记、推理块内部语法非法等。行为由实现定义：可以回退到 `normal_text`，可以部分提取，也可以显式报错。
- **`REASONING.batch.5`** 缺失结束标记恢复：引擎在 think 中途命中 `max_tokens`，需要固定行为：恢复部分 reasoning、标记为截断，或全部作为 `normal_text`。
- **`REASONING.batch.6`** 空推理内容：`<think></think>` 或等价零字节内容。必须仍然登记为 reasoning span，不能静默丢弃。
- **`REASONING.batch.7`** 复杂推理内容：大块、多段落、特殊字符、Unicode、换行等。验证内容不会截断，也不会出现转义 bug。
- **`REASONING.batch.8`** 空/null 内容变体：空输入、只有空白的输入、上游 chunk 中为 null。parser 不能崩溃，必须产生一致输出。
- **`REASONING.batch.9`** 一个响应中有多个 reasoning span：例如两个 `<think>...</think>` 块背靠背出现。行为由实现定义：可以拼接、只暴露第一个，或暴露为列表。

### Reasoning，stream 模式

- **`REASONING.stream.1`** 单个 reasoning block 跨 N 个 chunk：以任意 chunk 大小增量组装 `<think>...</think>` 内容。
- **`REASONING.stream.2`** 起始标记被 chunk 边界切开：例如 `<think>` 或等价标记跨 chunk。部分 token 匹配必须继续缓冲，不能作为普通文本 flush。
- **`REASONING.stream.3`** 结束标记被 chunk 边界切开：例如 `</think>` 或 `<|end|>` 跨 chunk。缓冲契约与 `REASONING.stream.2` 相同。
- **`REASONING.stream.4`** 累积文本偏离起始标记：累积文本始终无法匹配 reasoning 起始标记，即模型从一开始就输出普通文本。parser 必须干净放弃，把整个流作为 `normal_text`，不能无限缓冲。

### 格式条件标签（跨阶段）

`PARSER_CASES.md` 中的 `PARSER.fmt|xml|harmony.*` 标签也适用于 reasoning parser，只要它消费了对应格式。例如 GPT-OSS reasoning parser 处理 Harmony 的：

```text
<|channel|>analysis<|message|>...<|end|>
```

因此相关测试应同时带 `REASONING.batch.1` 与 `PARSER.harmony.1`。

### reasoning parser 中不适用的分类

- `PARSER.batch.2`：工具侧的“多个工具调用”在 reasoning 侧对应 `REASONING.batch.9`，不能直接套用工具调用形态标签。
- `PARSER.batch.8`：工具调用标记与普通文本交错；reasoning 的对应形态是 `REASONING.batch.3`。
- `FRONTEND.tool_choice`：请求时门控逻辑，见 `FRONTEND_CASES.md`。
- `PIPELINE.finish_reason`：流水线边界契约，见 `PIPELINE_CASES.md`。

---

## `REASONING.batch.1` — 只有推理内容

存在 `<think>...</think>` 或等价形式，例如 `<seed:think>`、Harmony 的 `<|channel|>analysis`，没有工具调用。

- 适用于每个 reasoning parser。
- parser 应把标记之间的内容写入 `reasoning_text`。
- `tool_calls` 为空。
- `normal_text` 可以为空，也可以包含 reasoning block 外部文本；后一种情况见 `REASONING.batch.3`。

## `REASONING.batch.2` — 推理内容 + 下游工具调用

模型先输出 reasoning 内容，再输出工具调用 token，例如：

```text
<think>...</think><|tool_call_begin|>...
```

两者都必须被正确处理：

- `reasoning_text` 必须被填充；
- 工具调用标记必须保留在 `normal_text` 中，供下游 tool-call parser 继续消费。

适用于每一对 reasoning parser 与 tool-call parser。

典型失败模式是：贪婪 reasoning parser 吞掉后续 tool-call 内容。边界必须用测试明确固定。

## `REASONING.batch.3` — 推理内容 + 普通文本

`<think>...</think>` 前后带用户可见叙述，没有工具调用。推理内容进入 `reasoning_text`，叙述进入 `normal_text`。

- 适用于每个 reasoning parser。

---

## `REASONING.stream.1` — 单个 reasoning block 跨 N 个 chunk

一个完整 reasoning block 被多个 SSE chunk 拆开，chunk 大小任意。parser 应增量重构 `reasoning_text`。

- 适用于每个 reasoning parser。
- 这是生产中的主要路径。

## `REASONING.stream.2` — 起始标记被 chunk 边界切开

起始 reasoning 标记，例如 `<think>`、`<|channel|>analysis` 等，跨 chunk 边界。部分匹配必须返回“继续缓冲”，而不是把部分字节作为 `normal_text` 输出；等下一个 chunk 到达后再完成匹配。

- 适用于每个 reasoning parser。

## `REASONING.stream.3` — 结束标记被 chunk 边界切开

结束 reasoning 标记，例如 `</think>`、`<|end|>` 等，跨 chunk 边界。缓冲契约与 `REASONING.stream.2` 相同。

- 适用于每个 reasoning parser。

## `REASONING.stream.4` — 累积文本偏离起始标记

累积文本始终不能匹配起始标记，例如模型从一开始就输出普通文本，没有 reasoning block。parser 必须干净放弃，把整个流作为 `normal_text`，不能无限缓冲。

- 适用于每个 reasoning parser。

---

## 客户事故回归测试

约定与 `PARSER_CASES.md` 相同：在 `#[test]` 注释里带上来源引用。

```rust
#[test] // REASONING.batch.2 (PR #1234)
fn test_unclosed_think_tag_no_longer_swallows_tool_call() { ... }
```

---

## 新增 reasoning parser 时必须包含的测试

最低可接受集合：

1. `REASONING.batch.{1, 3}`：基础 reasoning 提取，以及 reasoning 与用户可见叙述的拆分。
2. `REASONING.batch.2`：与下游 tool-call parser 的边界契约。任何与工具调用同时使用的 reasoning parser 都必须覆盖。
3. `REASONING.batch.{4, 5}`：异常输入与缺失结束标记恢复。必须固定行为；静默丢弃是典型失败模式。
4. `REASONING.batch.{6, 7, 8}`：空内容、复杂内容、null 内容。
5. `REASONING.batch.9`：多个 reasoning span。必须记录契约。
6. `REASONING.stream.{1, 2, 3, 4}`：流式解析。任何位于 streaming frontend 后的 parser 基本都必须覆盖。
7. 适用时附加格式标签，例如消费 Harmony 格式的 parser 应带 `PARSER.harmony.1` 等。
