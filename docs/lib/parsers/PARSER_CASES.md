# 工具调用解析器边界用例

本文档是 `src/tool_calling/` 下 **工具调用解析器** 单元测试的分类标准。它对应原文件 `PARSER_CASES.md`，用于说明每个 parser 至少需要覆盖哪些 batch、stream、format、XML、Harmony 相关边界情况。

相邻阶段的测试分类包括：

- **推理内容解析器**：`src/reasoning/`，见 `REASONING_CASES.md`。
- **前端门控逻辑**：例如请求时的 `tool_choice`，见 `FRONTEND_CASES.md`。
- **流水线边界**：例如 `finish_reason` 独立性，见 `PIPELINE_CASES.md`。

## 分类维度

本测试分类按三个互相独立的维度组织：

1. **阶段 Stage**：本文档使用 `PARSER.*`。其它阶段分别使用 `REASONING.*`、`FRONTEND.*`、`PIPELINE.*`。
2. **模式 Mode**：`batch` 表示一次性输入完整模型输出字符串；`stream` 表示增量输入 `delta_text` 和 `delta_token_ids`。batch 与 stream 的编号互相独立，例如 `PARSER.batch.1` 和 `PARSER.stream.1` 都可以表示“单个工具调用”，但它们属于不同测试入口。
3. **格式范围 Format scope**：适用范围比“所有 parser”更窄，例如 `fmt`、`xml`、`harmony`。

每个 `#[test]` 应携带一个或多个 `// PARSER.<tag>` 注释，说明该测试覆盖哪些分类。不适用的分类要明确写 `N/A`，不要静默省略。

如果某个测试源自客户工单、PR 或 GitHub issue，应在注释中保留来源，例如：

```rust
#[test] // PARSER.batch.5 (PR #8208)
fn test_parse_malformed_no_section_end() { ... }
```

分类标签说明测试类型，括号内容提供审计线索。这样可以通过 `grep -r 'PARSER.batch.5'` 找到分类测试，也可以通过 `grep -r '#8208'` 找到事故相关测试。

辅助函数白盒测试，例如 `detect_tool_call_start_*` / `find_tool_call_end_position_*`，可以标记为 `// helper`。这些测试只固定内部 Rust 函数行为，没有跨实现的编号分类。

---

## 快速参考

### Parser，batch 模式

这是通用行为契约，适用于每个工具调用 parser，输入为完整模型输出字符串。

- **`PARSER.batch.1`** 单个工具调用：正常路径，一个完整且格式正确的调用。
- **`PARSER.batch.2`** 多个工具调用：一个响应中包含两个或更多调用，可以顺序出现，也可以并行出现。
- **`PARSER.batch.3`** 无工具调用：响应只有普通文本。
- **`PARSER.batch.4`** 异常或不完整 JSON 参数：参数被截断、缺少右括号或语法非法。恢复行为由实现定义，应记录差异，而不是强制只有一种正确行为。
- **`PARSER.batch.5`** 缺失结束 token 的恢复：由于 `max_tokens` 或 EOS，外层结束栅栏缺失时仍能恢复调用，或显式报错。
- **`PARSER.batch.6`** 空参数：`arguments={}` 或无参数调用。
- **`PARSER.batch.7`** 复杂参数类型：嵌套对象、数组、布尔值、数字、Unicode、参数值中的换行等。
- **`PARSER.batch.8`** 普通文本与工具调用交错。
- **`PARSER.batch.9`** 空内容、空 `tool_calls` 数组或 null 响应。
- **`PARSER.batch.10`** 重复工具调用：同一函数名出现多次，参数可能相同也可能不同。

### Parser，stream 模式

stream 模式从流式引擎逐 token 或逐 chunk 组装工具调用。逻辑用例与 batch 类似，但通过 `parse_streaming_increment(delta)` 这类入口驱动，需要独立测试框架。

- **`PARSER.stream.1`** 单个工具调用跨 N 个 chunk。
- **`PARSER.stream.2`** 多个工具调用，每个调用都跨多个 chunk。
- **`PARSER.stream.3`** 部分 token chunking：chunk 边界切开语法 token。部分匹配必须继续缓冲，不能先作为普通文本 flush。
- **`PARSER.stream.4`** 流式终止：最终 chunk 带 `finish_reason=tool_calls` 或 EOS，parser 需要 flush 正在进行中的调用。

### 格式条件标签

`PARSER.fmt|xml|harmony` 标签也可以出现在 reasoning parser 测试中。该标签描述语法格式，不描述 parser 阶段。

#### `PARSER.fmt.*` — 格式变体

- **`PARSER.fmt.1`** 函数名约定：允许的标识符字符、`functions.NAME` 与裸 `NAME` 等前缀变体、非法函数 ID 的拒绝。
- **`PARSER.fmt.2`** 空白与格式容忍度：语法 token 内部或之间出现空白时是否仍能解析。
- **`PARSER.fmt.3`** token / wire-format 变体：同一语义的多种合法拼写。例如 Kimi K2 单复数 section token、Mistral pre-v11 与 v11+ 格式、Llama 3 有无 `<|python_tag|>`、Hermes 的 `qwen25` alias 等。parser 必须接受当前配置中登记的变体，并拒绝未登记的变体。
- **`PARSER.fmt.4`** 空 section / 无内容 wrapper：只有 start+end fences，中间没有内容。
- **`PARSER.fmt.5`** 参数形状约定：调用体内部 JSON envelope 的布局。包含原生 call ID 保留、JSON 字段顺序容忍、`arguments` 与 `parameters` key 别名。

#### `PARSER.xml.*` — 仅 XML 家族

适用于 `hermes`、`glm47`、`qwen3_coder`、`minimax_m2`、`kimi_k2`。

- **`PARSER.xml.1`** XML entity / HTML 反转义处理，例如 `&lt;`、`&amp;`、`&quot;`。
- **`PARSER.xml.2`** 基于 schema 的类型转换，例如 string → number/bool/array。

#### `PARSER.harmony.*` — 仅 Harmony

适用于 gpt-oss。

- **`PARSER.harmony.1`** channel / recipient 解析：处理 `analysis`、`commentary`、`final` channel，以及 `to=functions.X` recipient。
- **`PARSER.harmony.2`** envelope tag 语法：解析 `<|channel|>commentary to=functions.X <|constrain|>json<|message|>{...}<|call|>` 及其合法变体。

---

## 当前没有覆盖的通用缺口

- 函数名中的 Unicode，例如非 ASCII 工具名或 emoji。
- 参数数值溢出，例如超大整数或超出 JSON 规范范围的浮点数。
- 空函数名：`"name": ""`。
- 并发并行请求导致的解析竞争。
- guided decoding 与 tool-call 的交互，例如受约束生成产生 malformed args。
- 极长输出，例如单个工具调用 JSON 超过 10 KB。
- 流中错误注入或中断，例如 worker 被杀、网络中途断开。
- schema 参数数量不匹配，例如模型输出额外参数或漏参数。
- 正则超时、防灾难性模式保护、parser 异常隔离、长普通文本快速路径。

---

## 已知生产缺口

- **Mistral v11+ wire format**：`[TOOL_CALLS]name{...args}` 的 name-then-object 形式。当前 Dynamo 的 `ToolCallConfig::mistral()` 与底层 `base_json_parser.rs` 只处理 pre-v11 的 JSON 数组体：`[TOOL_CALLS][{name, arguments}]`。v11 是当前 Mistral-Small / Mistral-Large 的生产路径，应归入 `PARSER.fmt.3`。

---

## `PARSER.batch.1` — 单个工具调用，正常路径

响应中包含一个完整且格式正确的调用。

- 适用于每个工具调用 parser。
- 这是基础正确性检查；如果它失败，后续测试都没有意义。
- 如果语法携带模型原生 call ID，例如 Kimi K2 的 `functions.NAME:N`，正常路径测试必须额外断言该 ID 被原样保留在 `ToolCall.id` 上。

## `PARSER.batch.2` — 多个工具调用

一个响应中有两个或更多调用，可以在同一个 block 中，也可以背靠背出现。

- 适用于每个工具调用 parser。
- 有些语法在一个 block 中发出并行调用，例如 DSML、XML；有些语法发出顺序顶层 sentinel，例如 JSON 方言。无论哪种形式，都必须全部提取。

## `PARSER.batch.3` — 无工具调用

响应是普通文本，没有 tool-call 语法。

- 适用于每个工具调用 parser。
- 必须返回空 `Vec<ToolCall>`，并将输入作为 `normal_text` 返回。
- 不允许误报工具调用。

## `PARSER.batch.4` — 异常或不完整 JSON 参数

参数 payload 中的 JSON 被截断、缺少右括号或语法非法。

- 适用于每个工具调用 parser。如果某个语法完全不嵌入 JSON，应明确标记 `N/A`。
- 行为由实现定义：可以降级为字符串、错误时丢弃，或显式报错。跨实现一致性测试应记录差异，而不是断言唯一真值。

## `PARSER.batch.5` — 缺失结束 token 的恢复

模型输出在外层结束栅栏到达前被截断，常见原因是 `max_tokens` 或 EOS。

- 适用于所有使用成对 start/end fences 的工具调用 parser。
- 这是客户侧常见 bug：正在进行中的调用被静默丢弃，看起来像成功响应但没有 `tool_calls`，也没有错误。
- 可接受处理有两种：恢复已经完整的 invoke，或返回显式错误。无论哪种，都必须用测试固定行为。

## `PARSER.batch.6` — 空参数

工具调用带 `arguments={}`，或者是无参数调用。

- 适用于每个工具调用 parser。
- 必须仍然返回该调用。空参数是合法调用，不表示缺失。

## `PARSER.batch.7` — 复杂参数类型

参数包含嵌套对象、数组、布尔值、数字、Unicode、换行等。

- 适用于每个工具调用 parser。
- 对带类型提示的语法，例如 DSML 的 `string="true|false"`，应验证 JSON round-trip。
- 对没有类型提示的 XML 语法，类型转换部分属于 `PARSER.xml.2`；这里只验证复杂值不会被截断或转义错误。

## `PARSER.batch.8` — 普通文本与工具调用交错

模型在工具调用前、后或多个工具调用之间输出叙述文本。parser 必须正确拆分：文本进入 `normal_text`，调用进入 `tool_calls`。

- 适用于每个工具调用 parser。
- 如果叙述文本是 reasoning 内容，例如 `<think>...</think>`，该测试还会验证 reasoning parser 的交接；必要时同时标注 `REASONING.batch.2`。

### 子用例

- **`PARSER.batch.8.a`** 只有工具调用前有叙述：text → call。
- **`PARSER.batch.8.b`** 只有工具调用后有叙述：call → text。
- **`PARSER.batch.8.c`** 工具调用前后都有叙述：text → call → text。
- **`PARSER.batch.8.d`** 多个工具调用之间有叙述：text → call → text → call → text。

四个子用例共享同一契约：`tool_calls` 被提取，`normal_text` 按位置保留。

## `PARSER.batch.9` — 空内容、空 `tool_calls` 数组或 null 响应

引擎输出 `delta.content = ""`，最终响应中 `tool_calls: []`，或参数中包含 null。

- 适用于每个工具调用 parser。
- 参数中的 null 属于 parser 层处理；空 choices 或空 stream 通常属于端到端集成层。

## `PARSER.batch.10` — 重复工具调用

同一个函数名在一个响应中被调用两次，参数可能相同也可能不同。

- 适用于每个工具调用 parser。
- 两个调用都必须出现在 `tool_calls` 中，并带不同 ID。是否执行重复调用由 runtime 或客户端决定。

---

## `PARSER.stream.1` — 单个工具调用跨 N 个 chunk

一个完整调用被多个 SSE chunk 拆开，chunk 大小任意。parser 需要增量重构该调用。

- 适用于每个工具调用 parser。
- 这是生产中的主要路径。

## `PARSER.stream.2` — 多个工具调用分别跨 N 个 chunk

响应中有两个或更多调用，每个调用都跨多个 chunk。parser 必须在每个完整调用到达时分别输出，不能混淆参数。

## `PARSER.stream.3` — 部分 token chunking

chunk 边界切开语法 token，例如 start fence、end fence、参数名或参数值。部分匹配必须继续缓冲，不能作为普通文本 flush。

## `PARSER.stream.4` — 流式终止

最终 chunk 带 `finish_reason=tool_calls`、`length` 或 `stop`。parser 需要 flush in-flight 调用，或按 `PARSER.batch.5` 显式处理截断。

---

## 新增工具调用 parser 时必须覆盖的测试

最低集合：

1. `PARSER.batch.{1, 2, 3}`：基础正确性。
2. `PARSER.batch.4` 或明确 `N/A`：处理或拒绝 malformed input。
3. `PARSER.batch.5`：固定外层 fence 缺失时的行为；静默丢弃是潜在回归。
4. `PARSER.batch.{6, 7}`：空参数与复杂参数。
5. `PARSER.stream.{1, 2, 3, 4}`：流式测试。任何位于 streaming frontend 后的 parser 都基本必须覆盖。
6. `PARSER.batch.8`：普通文本交错。
7. `PARSER.batch.{9, 10}`：空/null 与重复调用。
8. 适用的格式变体：`PARSER.fmt.{1..5}`。
9. 适用的家族专属分类：XML 语法覆盖 `PARSER.xml.{1, 2}`，Harmony 覆盖 `PARSER.harmony.{1, 2}`。

Reasoning parser 见 `REASONING_CASES.md`。
