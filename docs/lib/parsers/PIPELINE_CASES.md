# 流水线边界用例

本文档是测试 **parser 与流水线其它部分之间边界契约** 的分类标准。这里的输出格式既不是 parser 内部细节，也不是请求时门控逻辑。

相邻文件：

- **工具调用解析器**：`PARSER_CASES.md`
- **推理内容解析器**：`REASONING_CASES.md`
- **前端门控逻辑**：`FRONTEND_CASES.md`

---

## 快速参考

- **`PIPELINE.finish_reason`**：parser 输出必须独立于上游 `finish_reason`，例如 `stop`、`tool_calls`、`length`。相同输入文本必须产生相同解析结果，不应因为引擎报告了不同的流结束原因而改变。

---

## `PIPELINE.finish_reason` — parser 输出独立于上游 stream-end 原因

当引擎因为工具调用落地而报告 `finish_reason=tool_calls` 时，parser 不能“信任”这个信号；它必须只根据文本内容提取调用。

反过来，当引擎报告 `finish_reason=length`，表示输出被截断时，parser 仍然必须恢复截断点之前已经完整出现的调用。相关工具调用截断恢复见 `PARSER.batch.5`。

Parser 的职责是：

```text
text → (calls, normal_text)
```

而将：

```text
(calls, finish_reason)
```

映射成最终响应 wire format，例如：

```text
finish_reason: tool_calls
```

是 **frontend** 的职责，不是 parser 的职责。

- 适用于每个工具调用 parser。
- 测试约定：使用同一段文本，分别配上不同的上游 `finish_reason` 值输入两次，断言 parser 输出逐字节一致。
- 与之配套的前端断言，即当调用落地时传播 `finish_reason=tool_calls`，应放在 `FRONTEND_CASES.md` 中。
