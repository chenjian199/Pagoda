# lib/protocols 设计文档

## 1. 目的

`pagoda-protocols` 为 Pagoda 的 HTTP 推理层提供**兼容 OpenAI 的请求/响应类型定义**。它是一个纯声明式的类型 crate：只包含类型、serde 派生、builder 以及 `From` 转换，不包含任何业务逻辑或 HTTP 传输代码。

支持的协议：

| 协议 | 来源策略 |
|------|----------|
| OpenAI Chat Completions & Completions | 上游 `async-openai` 重新导出 + Pagoda 扩展 |
| OpenAI Responses API（Codex、Agents SDK） | 输入链自有，输出链走上游 |
| Anthropic Messages API | 完全自有（上游无对应类型） |
| Embeddings / Images | 完全从上游重新导出 |

## 2. 核心设计原则

整个 crate 围绕一个问题展开：**某个类型是从上游重新导出，还是自己拥有？**

- `async-openai` 维护良好但体量巨大、更新频繁，无法整体 fork。
- 上游在「放宽输入校验」类改动上推进缓慢，而 Pagoda 不能被上游合并节奏阻塞。

由此确立的规则：**默认重新导出上游；只拥有能修复所需行为的、最小的那棵类型子树。**

### 所有权判定准则

仅当满足以下至少一条时才自有某类型：

1. 上游拒绝了真实客户端会发送的形态（如 `OutputMessage.id` 被强制必填，但客户端常省略）。
2. 需要用 Pagoda 专属字段扩展 schema（如 `mm_processor_kwargs`、`reasoning_content`）。
3. 上游类型强制了破坏下游后端的形态（如 `FunctionCall.arguments` 需同时接受 String 与对象）。
4. 上游存在已知 bug（如 `ToolCall.type` 序列化缺失）。

**关键约束**：把影响半径控制到最小，绝不因「相邻类型被拥有」而连锁拥有一整片类型树。

## 3. 模块结构

```
src/
├── lib.rs                    # crate 入口，声明 error / types 模块
├── error.rs                  # OpenAIError 错误类型
└── types/
    ├── mod.rs                # 模块聚合 + 上游重新导出
    ├── chat.rs               # Chat Completions（大量自有）
    ├── completion.rs         # 旧版 Completions（基本走上游）
    ├── anthropic.rs          # Anthropic Messages API（完全自有）
    ├── impls.rs              # 自有类型的便捷 impl
    └── responses/
        └── mod.rs            # Responses API（输入链自有）
```

依赖核心：`async-openai`（仅类型，无 HTTP 客户端）、`serde`/`serde_json`、`derive_builder`、`thiserror`。

## 4. 关键设计模式

### 4.1 重新导出约定

- **显式重新导出**（`pub use foo::{A, B}`）：当需要选择性 shadow 时使用，能在上游重命名/移除时于编译期报错。`chat.rs` 采用此方式。
- **glob 重新导出**（`pub use foo::*`）：类型面巨大时使用（`responses/mod.rs` 有 200+ 类型）。本地 `pub struct` 可 shadow  glob 导入的同名类型。

### 4.2 双侧（dual-side）命名规则

上游会在请求-输入侧与响应-输出侧复用同一类型（典型如 `OutputMessage`）。若直接 shadow 放宽，会导致输出侧构造点全部崩溃，进而连锁拥有半个 crate。

**规则**：被双侧复用的类型，给 Pagoda 自有的*输入侧*变体起*不同的名字*，输出侧继续用上游名字。

| 输入侧（自有，已放宽） | 输出侧（上游，未改动） |
|------|------|
| `InputOutputMessage` | `OutputMessage` |
| `InputOutputMessageContent` | `OutputMessageContent` |
| `InputOutputTextContent` | `OutputTextContent` |

仅输入类型可同名 shadow （无冲突）：`MessageItem`、`Item`、`InputItem`、`InputParam`、`CreateResponse`。

### 4.3 Responses 输入链

```
CreateResponse
└── input: InputParam            ( shadow )
    └── InputItem                ( shadow )
        └── Item                 ( shadow ，逐变体镜像上游)
            └── Message(MessageItem)
                └── Output(InputOutputMessage)   (新名字，已放宽)
```

`Item` 是 `#[serde(tag = "type")]` 枚举，无法继承变体，必须逐变体镜像上游——上游新增变体时需同步添加，这是拥有该链的代价。输出链（`Response`、`OutputItem` 等）完全走上游，因为输出时由 Pagoda 铸造合法的 id/status，无需放宽。

## 5. 边界（明确不属于本 crate）

- HTTP 传输（执行、重试、流式帧解析）→ `lib/llm/src/http/`
- API 类型间语义转换（Responses→Chat 等）→ `lib/llm/src/protocols/`
- 模型相关分词与提示模板

## 6. 维护要点

- **上游合并放宽后**：升级 `async-openai`，删除已等价的自有覆盖，更新消费方，跑序列化形态测试。失效的所有权即技术债。
- **上游重命名**：glob 会静默跟随，显式导出会编译失败（这正是其价值）。
- **测试**：序列化形态测试（`lib/llm` 的 `test_response_wire_format_shape`）保证 wire 格式合规；自有类型需同时测试放宽形态与严格形态。

---
> 关于所有权判定的完整论证与常见陷阱，详见同目录 [CLAUDE.md](CLAUDE.md) / [CLAUDE.zh-CN.md](CLAUDE.zh-CN.md)。
