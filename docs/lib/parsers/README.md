# pagoda-parsers

用于从原始 LLM 输出中解析 **工具调用** 和 **推理内容** 的 Rust crate。

这个 crate 关注具体输出格式，优先支持流式解析，并按模型家族组织解析逻辑。它属于 Pagoda chat-completions pipeline 中的 **模型输出后处理侧**：给定来自 vLLM 或 SGLang 的 token stream，解析出给客户端使用的结构化 `Vec<ToolCall>` 和 `reasoning_content`。

模型输入前的 prompt formatting 不在这里处理，而是在：

```text
lib/llm/src/preprocessor/prompt/
```

---

## crate 中包含什么

这个 crate 有两个顶层模块，每个模块都有自己的 parser registry：

```text
lib/parsers/
└── src/
    ├── tool_calling/        ← 工具调用提取，当前注册了 18 个 parser
    │   ├── parsers.rs       — parser 注册表与分发入口，例如 detect_and_parse_tool_call
    │   ├── config.rs        — 每个 parser 对应的 ToolCallConfig
    │   ├── response.rs      — ToolCallResponse 的 wire shape
    │   ├── dsml/            — DeepSeek V3.2 / V4 的 DSML 语法
    │   ├── gemma4/          — Google Gemma 4 的自定义非 JSON 语法，使用 <|"|> 分隔字符串
    │   ├── xml/             — hermes、glm47、kimi_k2、minimax_m2、qwen3_coder
    │   ├── json/            — deepseek_v3、deepseek_v3_1、nemotron_deci/nano、jamba、mistral、phi4、llama3_json
    │   ├── harmony/         — OpenAI gpt-oss，Harmony token stream，使用 openai_harmony crate
    │   └── pythonic/        — Python 函数调用语法，一些 Llama 变体会使用
    │
    └── reasoning/           ← 推理内容提取，当前注册了 15 个 parser
        ├── mod.rs           — parser 注册表与分发入口
        ├── base_parser.rs   — BasicReasoningParser，用于 <think>...</think> 这类格式
        ├── gemma4_parser.rs — Gemma 4，格式类似 <|channel>thought\n...
        ├── gpt_oss_parser.rs — Harmony channel 解析
        ├── granite_parser.rs — Granite 风格解析
        └── minimax_append_think_parser.rs — MiniMax inline-reasoning
```

---

## 一个请求在这个 crate 中如何流动

```text
token stream from engine
        │
        ▼
┌─────────────────────────────────┐
│ reasoning parser                │
│                                 │
│ 通过 reasoning::mod.rs 中的       │
│ get_reasoning_parser_map()      │
│ 按名称注册和查找                 │
│                                 │
│ 例如 basic / gpt_oss / ...       │
│                                 │
│ 返回：                           │
│ (reasoning_content,             │
│  non_reasoning_tail)             │
└─────────────────────────────────┘
        │
        ▼
(non-reasoning tail)
        │
        ▼
┌─────────────────────────────────┐
│ tool-call parser                │
│                                 │
│ 通过 tool_calling::parsers::     │
│ get_tool_parser_map()            │
│ 按 parser name 分发              │
│                                 │
│ 分发后选择一个 ParserConfig：     │
│                                 │
│ - Dsml(DsmlParserConfig)         │
│     → try_tool_call_parse_dsml   │
│ - Json(JsonParserConfig)         │
│     → try_tool_call_parse_json   │
│ - Xml(XmlParserConfig)           │
│     → try_tool_call_parse_xml    │
│ - KimiK2(KimiK2ParserConfig)     │
│     → try_tool_call_parse_kimi_k2│
│ - Pythonic / Harmony             │
└─────────────────────────────────┘
        │
        ▼
Vec<ToolCall> + normal_text
```

`tool_calling/parsers.rs` 中主要的公开入口包括：

```rust
detect_and_parse_tool_call(input, parser_name, schema)
    -> (calls, normal_text)
```

高层入口。根据 `parser_name` 从注册表中选择 parser，并结合工具 schema 解析输出。

```rust
try_tool_call_parse(input, config)
    -> (calls, normal_text)
```

低层入口。直接使用 `ToolCallConfig`，绕过 registry。

```rust
detect_tool_call_start(chunk, parser_name)
```

流式解析入口：判断当前 chunk 是否正在开始一个 tool-call block。

```rust
find_tool_call_end_position(chunk, parser_name)
```

流式解析入口：判断 tool-call block 在当前 chunk 中的结束位置。

---

## Parser family 速查表

新增模型时，通常应该先判断它属于下面哪一种 parser family。

### 工具调用 parser

| Family | 语法 | 共享解析引擎 | 示例 |
| -- | -- | -- | -- |
| **DSML** | `<｜DSML｜tool_calls>...`，参数中带 `string="true|false"` 这类类型信息 | `dsml/parser.rs` | DeepSeek V3.2、V4 |
| **XML** | XML 风格工具调用，包含嵌套的函数名与参数标签 | `xml/parser.rs` 通用实现，或为特殊变体单独实现 | hermes、qwen3_coder、minimax_m2、glm47、kimi_k2 |
| **JSON** | 起始 sentinel 后接裸 JSON 数组，数组元素形如 `{name, arguments}` | `json/base_json_parser.rs` | deepseek_v3、deepseek_v3_1、nemotron_deci/nano |
| **Harmony** | OpenAI Harmony token stream，包含 `<|channel|>`、`<|message|>`、`<|call|>` | `harmony/harmony_parser.rs`，封装外部 `openai_harmony` crate | gpt-oss-20B / 120B |
| **Pythonic** | `[func_name(arg=value, ...)]` 这种 Python 函数调用语法 | `pythonic/pythonic_parser.rs` | 一些 Llama 变体 |
| **Gemma 4** | 自定义格式：`<|tool_call>call:name{key:<|"|>val<|"|>}`，允许 bare keys，并使用自定义字符串分隔符 | `gemma4/parser.rs`，递归下降解析到 `serde_json::Value` | Google Gemma 4 thinking models |

### Reasoning parsers

| Family | 语法 | 共享解析引擎 | 示例 |
| -- | -- | -- | -- |
| **Basic，也就是 think-tag** | `<think>...</think>` | `reasoning/base_parser.rs`，即 BasicReasoningParser | Qwen3、Nemotron、Kimi K2.5、DeepSeek R1 / V4、GLM-4.5+ |
| **Append-think** | `<think>...</think>` 保留在行内文本中，并在第一个 chunk 前加 `<think>` 前缀 | `reasoning/minimax_append_think_parser.rs` | MiniMax M2 |
| **Harmony channel** | 隐藏的 `analysis` channel | `reasoning/gpt_oss_parser.rs`，封装外部 `openai_harmony` | gpt-oss-20B / 120B |
| **Granite** | 自定义 start / end token | `reasoning/granite_parser.rs` | IBM Granite |
| **Gemma 4 channel** | `<|channel>thought\n...`，并去掉 role-label prefix | `reasoning/gemma4_parser.rs` | Google Gemma 4 thinking models |

---

## 添加新的 parser

### 1. 先从上面的速查表中选择 family

如果现有的 config-driven family 能覆盖新模型，只需要：

1. 在 `tool_calling/config.rs` 中添加一个 `ToolCallConfig::<model>()` 构造函数；
2. 在 `tool_calling/parsers.rs` 中注册它。

这样就完成了接入，并且可以复用已有的共享 parser 和测试。

---

### 2. 如果语法确实是全新的，再新增模块

如果新模型的工具调用语法不属于已有 family，就在 `tool_calling/` 下新增一个模块，并在 `config.rs` 中增加一个新的 `ParserConfig` variant。

新模块的组织方式应参考现有 parser 模块。

---

### 3. Reasoning parser 优先复用 BasicReasoningParser

对于 reasoning，除非语法确实不同，否则优先 alias 到 `BasicReasoningParser`。

大多数新模型都使用普通的：

```text
<think>...</think>
```

这类格式可以共享 BasicReasoningParser。

只有真正不同的格式才需要新增 reasoning parser，例如：

- append-think；
- Harmony channels；
- Gemma thought channel；
- 自定义 start / end token。

---

### 4. 编写测试

最小可接受测试集合在：

```text
PARSER_CASES.md
```

也就是 `PARSER.*` 测试分类。

至少应覆盖：

```text
PARSER.1 / PARSER.2 / PARSER.3
```

用于基础正确性。

```text
PARSER.5
```

用于截断行为。

```text
PARSER.8 / PARSER.9
```

用于 reasoning 场景下的 streaming 行为。

```text
PARSER.13
```

用于普通文本与工具调用交错的场景。

如果某些分类对当前 parser 不适用，应在注释中明确标注 `N/A`，不要静默跳过。

---

## 相关文档

- [`PARSER_CASES.md`](./PARSER_CASES.md)  
  corner-case taxonomy。说明每个 parser 应该覆盖哪些边界用例、哪些用例对某个 family 是 N/A，以及目前有哪些通用缺口。

- `lib/llm/tests/data/`  
  按 engine × model 捕获的 streaming fixtures，用于驱动 `test_streaming_tool_parsers.rs`。这是测试体系中的 replay 部分。

---

## 与 Pagoda 其它部分的集成关系

- `lib/llm/src/preprocessor/prompt/`  
  模型输入前处理侧。这里负责编写 prompt，模型之后输出的内容最终会回到本 crate 中解析。

- `lib/llm/src/preprocessor.rs`  
  顶层 request / response pipeline。它根据 `is_reasoning_disabled_by_request` 判断是否运行 reasoning parser，然后把去掉 reasoning 之后的 tail 交给 tool-call parser。

- `components/src/pagoda/frontend/`  
  Python frontend。它把解析后的输出作为 OpenAI-compatible SSE chunks 暴露给客户端。
