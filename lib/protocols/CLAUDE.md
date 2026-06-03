# lib/protocols

用于 Pagoda HTTP 接口面的 OpenAI 兼容请求/响应类型。本模块构建在 `async-openai` crate 之上，并在上游不接受、或尚未合并但我们需要的行为上，选择性提供 Pagoda 自有覆盖类型。

如果你要扩展或调试这里的类型，请在编辑前完整阅读本文档。每一次修改都围绕一个核心问题展开：**这个类型应该重新导出 upstream，还是应该由我们自己拥有？** 本文档存在的目的，就是让这个判断保持一致。

## 核心矛盾

`async-openai` 维护良好，但对“输入宽松性”相关 PR 的合并速度较慢。维护者通常希望变更严格匹配 OpenAPI spec，即使 OpenAI 的**托管 API** 在实际输入上接受比 spec 更宽松的结构。参见 `64bit/async-openai#535`（`ReasoningItem.id 为可选字段`），以及此前关于 （`OutputMessage.id` / `status` 为可选字段）的工作——这些都来自真实的 Agents-SDK / Codex 流量，尽管从 spec 角度看这些输入技术上并不合法。

我们不能让 Pagoda 阻塞在 upstream 合并上。我们也不能 fork 整个 crate——它非常庞大，而且更新频繁。因此我们确定的规则是：**默认重新导出 upstream；只有在需要修复我们所需行为时，才拥有最窄的类型子树。**

## 类型归属判定规则

默认使用 upstream。只有满足以下至少一个条件时，才自己拥有某个类型：

1. **Upstream 拒绝真实客户端会发送的结构。** 这是主要触发场景。例如：`OutputMessage.id` / `status` / `annotations` 在 upstream 中被标记为必填，但 Codex / Agents SDK 的输入中经常省略这些字段。
2. **我们需要扩展 schema，加入不适合 upstream 的 Pagoda 专属字段。** 例如：`CreateChatCompletionRequest.mm_processor_kwargs`（vLLM 多模态）、`ChatCompletionRequestAssistantMessage.reasoning_content`（R1 / QwQ）、`ChatCompletionStreamOptions.continuous_usage_stats`。
3. **Upstream 的类型强制了一种会破坏下游后端的结构。** 例如：upstream 中 `FunctionCall.arguments` 是 `String`，但 LangChain 等客户端会把它作为 object 发送。我们拥有 `FunctionCall`，通过自定义反序列化同时接受两种形式，并规范化为 `String`。
4. **Upstream 存在已知 bug。** 例如：`ChatCompletionMessageToolCall.type` 并不总是被序列化；我们拥有该类型，并使用 `#[serde(default = "default_function_type")]` 保持 wire compatibility。

**不要仅仅因为相邻类型被我们拥有，就顺手拥有某个类型。** 要尽量缩小影响范围。如果拥有 `OutputMessage` 会级联导致必须拥有 `Response`、`OutputItem`、streaming events，以及半个 crate，那么就停下来，寻找更窄的修复方式（见下文“命名：避免输入/输出双侧冲突”）。

## 目录布局

- `src/types/chat.rs` — Chat Completions（请求、响应、流、消息）。Pagoda 拥有的范围较多：多模态内容、reasoning、continuous usage stats、灵活的 `arguments`。
- `src/types/responses/mod.rs` — Responses API（Codex、Agents SDK）。输入链条由我们拥有；输出链条完全使用 upstream。
- `src/types/completion.rs` — 旧版 Completions。大部分使用 upstream。
- `src/types/anthropic.rs` — Anthropic Messages API。完全由我们拥有（`async-openai` 中没有对应 upstream 类型）。
- `src/types/embeddings`、`src/types/images` — 完全重新导出 upstream（没有 Pagoda 扩展）。

## 重新导出约定

当需要选择性 shadow 某些类型时，优先使用**显式重新导出**：

```rust
pub use foo::{A, B, C};
```

不要优先使用 glob。glob：

```rust
pub use foo::*;
```

可以放在模块顶部——Rust 允许本地 `pub struct Foo` shadow glob 导入的 `Foo`（glob 只会产生 `unused_imports` 警告）。但显式列表能让读者更清楚地看到哪些类型来自 upstream，哪些类型由我们拥有；当 upstream 重命名或删除类型时，也能在编译期暴露错误。

`src/types/responses/mod.rs` 使用 glob 重新导出，是因为该接口面非常庞大（200+ 类型）。`src/types/chat.rs` 使用显式列表，是因为接口面更可控，并且 Pagoda 拥有的类型更多。两种模式都可以接受；应根据需要枚举多少类型、以及要排除多少自有类型来选择。

## 命名：避免输入/输出双侧冲突

**陷阱：** Upstream 有时会在请求输入侧和响应输出侧复用同一个类型。`OutputMessage` 是典型例子：它既出现在 `MessageItem::Output(...)` 中（输入侧——客户端回传的上一轮 assistant 消息），也出现在 `OutputItem::Message(...)` 中（输出侧——我们刚生成的 assistant 消息）。

如果我们放宽 `OutputMessage`（例如让 `id` / `status` 可选）并 shadow upstream 的同名类型，那么所有在输出侧构造 `OutputItem::Message(OutputMessage { ... })` 的地方都会出问题：`OutputItem::Message` 这个 variant 持有的是 upstream 的类型，而不是我们的宽松 struct；我们的宽松 struct 与它并不匹配。

天然的修复方式是继续拥有 `OutputItem`。但这会级联到必须拥有 `Response`、streaming events，以及一长串相关子类型。正确的修复方式应该更小：

**规则：** 如果某个 upstream 类型同时被输入侧和输出侧复用，那么 Pagoda 自有的输入侧变体必须使用**不同名称**。输出侧继续通过 glob 或显式重新导出使用 upstream 的原名。

当前 `responses/mod.rs` 中的命名如下：

- `InputOutputMessage` — Pagoda 自有，宽松类型；用于输入侧 `MessageItem::Output(...)`。
- `OutputMessage` — upstream，不变；用于输出侧 `OutputItem::Message(...)`。
- `InputOutputMessageContent`（输入侧）与 upstream 的 `OutputMessageContent`（输出侧）遵循同样模式；`InputOutputTextContent`（输入侧）与 upstream 的 `OutputTextContent`（输出侧）也遵循同样模式。

只在输入侧使用的类型可以用同名方式 shadow upstream，不会冲突。当前 shadow 的类型包括：`MessageItem`、`Item`、`InputItem`、`InputParam`、`CreateResponse`。

## Responses 输入链条的具体情况

截至目前，自有输入链条如下：

```text
CreateResponse
└── input: InputParam            (shadow)
    └── InputItem                (shadow)
        ├── ItemReference        (upstream)
        ├── EasyInputMessage     (upstream)
        └── Item                 (shadow, mirrors upstream variant-for-variant)
            ├── Message(MessageItem)  (shadow)
            │   ├── Input(InputMessage)  (upstream)
            │   └── Output(InputOutputMessage)  (NEW NAME — relaxed)
            │       └── content: Vec<InputOutputMessageContent>  (NEW NAME)
            │           └── OutputText(InputOutputTextContent)   (NEW NAME — relaxed)
            └── ... 19 other upstream variants (FunctionCall, Reasoning, etc.)
```

`Item` 需要逐 variant 镜像复制 upstream，因为它是一个 `#[serde(tag = "type")]` enum——我们无法继承 variants。如果 upstream 给它的 `Item` 增加了新 variant，我们这里也必须同步增加；否则携带该新类型的 payload 会反序列化失败。这是拥有该链条必须承担的一个 upstream drift 成本。

输出链条（`Response`、`OutputItem`、`OutputMessage`、streaming events 等）完全使用 upstream。我们在输出时会生成合法的 `id` / `status`，因此不需要宽松处理，也没有理由拥有输出链条。

## 当 upstream 最终合并放宽变更时

如果 upstream PR 最终合并，使某个字段变为可选，并且该行为与我们本地放宽一致，处理清单如下：

1. 在 `Cargo.toml` 中升级 `async-openai`。
2. 如果本地 owned override 已经与 upstream 完全一致，则删除该覆盖；如果 upstream 只部分放宽，则收窄本地覆盖范围。
3. 更新消费方代码（例如，如果 upstream 仍然保留字段但不是可选字段，则需要把 `Option<T>` 转回 `T` 等）。
4. 运行完整测试套件；serialization-shape 测试应能捕获任何回归。

不要“以防万一”保留冗余的 Pagoda 自有类型。无效的 owned 类型就是后期维护的技术债务。

## 当 upstream 重命名或重构我们重新导出的类型时

Glob 重新导出会静默接收重命名后的变化。显式重新导出会编译失败——这正是它的意义。此时需要更新显式列表和所有消费方代码，确认没有语义漂移，然后运行测试。

## 测试模式

- 序列化形状测试（`lib/llm` 中的 `test_response_wire_format_shape`）用于验证我们序列化出的 JSON 是否匹配 API spec。当你修改 owned 类型时，应重点依赖这些测试。
- owned 类型的反序列化测试应同时覆盖宽松结构（也就是我们拥有它的原因）和严格结构（证明我们没有破坏符合 spec 的客户端）。
- 当你给某个 owned 类型新增 Pagoda 字段时，应增加一个省略该字段的测试，并断言默认行为正确。

## 明确不属于本 crate 职责的事情

- HTTP 传输（请求执行、重试、streaming frame 解析）——这些属于 `lib/llm/src/http/`。
- API 类型之间的语义转换（Responses → Chat、Anthropic → Chat 等）——这些属于 `lib/llm/src/protocols/`，并使用这里定义的类型。
- 模型特定的 tokenization 或 prompt templating。

保持本 crate 的声明式定位：类型、serde derives、builders、基于 `From` 的转换。业务逻辑属于下游。

## 常见错误

- 因为某个 bug 附近有一个类型，就拥有该类型；而不是因为 bug 本身确实需要拥有它。请收窄修复范围。
- 在没有检查输出侧构造点的情况下 shadow 双侧共用类型。重命名前请先在整个 workspace 中 `grep` 构造调用。
- 试图通过本地 wrapper struct 上的 `#[serde(default)]` 给 upstream 重新导出的类型添加字段。这不可行——serde 无法向 foreign type 注入默认值，除非使用 `#[serde(remote)]`；但这要求逐字段复制，也无法解决 optional-vs-required 不匹配的问题。
- 添加 variant 后忘记更新 `From` impl。编译器能捕获 exhaustive match 问题，但当 enum 是 non-exhaustive 时，它不会自动捕获 `From<Ours> for Upstream` 中的 variant 数量问题。
