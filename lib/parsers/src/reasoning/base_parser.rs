// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 推理与工具调用的交互
//!
//! ## 设计意图
//! GLM-4.5/4.7、Qwen3 等模型会把推理块与工具调用交错输出：
//!
//! ```text
//! <think>reasoning about what tool to call</think>
//! <tool_call>get_weather<arg_key>city</arg_key><arg_value>Beijing</arg_value></tool_call>
//! <think>reasoning about the result</think>
//! <tool_call>summarize<arg_key>text</arg_key><arg_value>...</arg_value></tool_call>
//! ```
//!
//! 推理解析器与工具调用解析器是**相互独立、串行**的两个阶段：
//!
//! 1. **推理解析器**（`BasicReasoningParser`）把流切分为：
//!    - `reasoning_content`：`<think>...</think>` 块内的全部内容
//!    - `normal_text`：块外的全部内容（含工具调用标签）
//! 2. **工具调用解析器**（`glm47` 等）再处理 `normal_text`，抽取
//!    `<tool_call>...</tool_call>` 块。
//!
//! 这意味着工具调用**必须**出现在 `<think>` 块之外才能被检测到。若模型错误地在
//! `<think>` 块内发出工具调用（GLM-4.7 在超长上下文下曾出现），工具调用解析器将看不到它。
//!
//! ## 外部契约
//! - `BasicReasoningParser`（`new` 四参构造 + `with_tool_start_token` 链式）实现
//!   `ReasoningParser`：`set_in_reasoning`、`detect_and_parse_reasoning`、
//!   `parse_reasoning_streaming_incremental`。
//! - 批式抽取结果对 `normal_text`/`reasoning_text` 做 `trim`；流式则原样累积不 trim。
//!
//! ## `force_reasoning` 与 tokenizer 行为
//!
//! 某些模型（如经 ZAI 提供的 GLM-5-FP8）把 `<think>` 当作特殊 tokenizer token 消费，
//! 从不以字面文本发出。此时用 `force_reasoning=true`（`deepseek_r1` 解析器），它在见到
//! `</think>` 前把全部输出视为推理。会以文本形式发出 `<think>` 的模型（标准部署、Qwen3、
//! GLM-4.5）应使用 `force_reasoning=false`（`glm45`、`nemotron_deci`、`qwen3` 解析器）。

use crate::{ParserResult, ReasoningParser};

/// 返回 `s` 的最长后缀且同时是 `delim` 前缀的长度。
///
/// 移植自 ollama 的 `thinking/parser.go::overlap()`。用于检测跨流式块边界切断的部分
/// 标签（例如 `"Hello world <th"`，其中 `<th` 是 `<think>` 的前缀）。
fn overlap(s: &str, delim: &str) -> usize {
    let max = delim.len().min(s.len());
    // 跳过位于多字节码点中间的位置（如 Kimi 标签中的多字节 `◁`）
    (1..=max)
        .rev()
        .filter(|&i| delim.is_char_boundary(i))
        .find(|&i| s.ends_with(&delim[..i]))
        .unwrap_or(0)
}

#[derive(Default, Debug, Clone)]
pub struct BasicReasoningParser {
    think_start_token: String,
    think_end_token: String,
    _in_reasoning: bool,
    stream_reasoning: bool,
    _buffer: String,
    stripped_think_start: bool,
    /// 可选标记：在推理块内遇到时强制退出推理模式（例如 Kimi-K2/K2.5 模型有时会发出
    /// `<|tool_calls_section_begin|>` 却未先闭合 `</think>`）。
    tool_start_token: Option<String>,
}

impl BasicReasoningParser {
    pub fn new(
        think_start_token: String,
        think_end_token: String,
        force_reasoning: bool,
        stream_reasoning: bool,
    ) -> Self {
        Self {
            think_start_token,
            think_end_token,
            _in_reasoning: force_reasoning,
            stream_reasoning,
            _buffer: String::new(),
            stripped_think_start: false,
            tool_start_token: None,
        }
    }

    /// 当 `token` 出现在已打开的推理块内时启用强制退出推理。
    pub fn with_tool_start_token(mut self, token: impl Into<String>) -> Self {
        self.tool_start_token = Some(token.into());
        self
    }
}

impl ReasoningParser for BasicReasoningParser {
    fn set_in_reasoning(&mut self, in_reasoning: bool) {
        self._in_reasoning = in_reasoning;
        if in_reasoning {
            // 标记起始 token 已被剥除，使解析器不再在流中寻找它——模板已注入它。
            self.stripped_think_start = true;
        }
    }

    fn detect_and_parse_reasoning(&mut self, text: &str, _token_ids: &[u32]) -> ParserResult {
        let has_think_tag = text.contains(&self.think_start_token);
        let in_reasoning = self._in_reasoning || has_think_tag;
        if !in_reasoning {
            return ParserResult {
                normal_text: text.to_string(),
                reasoning_text: String::new(),
            };
        }

        // force_reasoning 且无起始标记、无结束标记、无工具起始标记时，整段视为推理。
        let has_tool_start = self
            .tool_start_token
            .as_deref()
            .is_some_and(|tok| text.contains(tok));
        if self._in_reasoning
            && !has_think_tag
            && !text.contains(&self.think_end_token)
            && !has_tool_start
        {
            return ParserResult {
                normal_text: String::new(),
                reasoning_text: text.to_string(),
            };
        }

        // 用游标迭代抽取所有 <think>...</think> 对
        let mut reasoning_parts = Vec::new();
        let mut normal_parts = Vec::new();
        let mut cursor = 0;
        let mut currently_reasoning = self._in_reasoning;

        while cursor < text.len() {
            if currently_reasoning {
                // 若前导存在起始 token 则跳过（处理 force_reasoning + 显式 <think>）
                if text[cursor..].starts_with(&self.think_start_token) {
                    cursor += self.think_start_token.len();
                }
                // 寻找最早的推理退出点：</think> 或可选的 tool_start_token（强制退出情形）。
                let end_offset = text[cursor..].find(&self.think_end_token);
                let tool_offset = self
                    .tool_start_token
                    .as_deref()
                    .and_then(|tok| text[cursor..].find(tok));

                match (end_offset, tool_offset) {
                    (Some(e), Some(t)) if t < e => {
                        // tool_start 在 </think> 之前到达——强制退出。
                        reasoning_parts.push(&text[cursor..cursor + t]);
                        normal_parts.push(&text[cursor + t..]);
                        cursor = text.len();
                        currently_reasoning = false;
                    }
                    (Some(e), _) => {
                        reasoning_parts.push(&text[cursor..cursor + e]);
                        cursor += e + self.think_end_token.len();
                        currently_reasoning = false;
                    }
                    (None, Some(t)) => {
                        // 无 </think> 但存在 tool_start——强制退出。
                        reasoning_parts.push(&text[cursor..cursor + t]);
                        normal_parts.push(&text[cursor + t..]);
                        cursor = text.len();
                        currently_reasoning = false;
                    }
                    (None, None) => {
                        // 无结束 token——其余为推理（被截断）
                        reasoning_parts.push(&text[cursor..]);
                        cursor = text.len();
                    }
                }
            } else {
                // 处于普通文本——寻找起始 token
                if let Some(start_offset) = text[cursor..].find(&self.think_start_token) {
                    normal_parts.push(&text[cursor..cursor + start_offset]);
                    cursor += start_offset + self.think_start_token.len();
                    currently_reasoning = true;
                } else {
                    // 不再有 think 块——其余为普通文本
                    normal_parts.push(&text[cursor..]);
                    cursor = text.len();
                }
            }
        }

        let reasoning_text = reasoning_parts.join("").trim().to_string();
        let normal_text = normal_parts.join("").trim().to_string();

        // 注意：此处刻意不更新 self._in_reasoning。本方法被文档规定为「重置或忽略内部流式状态」
        // （见 trait 文档）。调用方不应在同一解析器实例上混用 detect_and_parse_reasoning 与
        // parse_reasoning_streaming_incremental。

        ParserResult {
            normal_text,
            reasoning_text,
        }
    }

    fn parse_reasoning_streaming_incremental(
        &mut self,
        text: &str,
        _token_ids: &[u32],
    ) -> ParserResult {
        self._buffer.push_str(text);

        let mut accumulated_normal = String::new();
        let mut accumulated_reasoning = String::new();

        // 循环耗尽单块内的所有状态转移。否则若一个块含两个完整 <think>...</think> 块，
        // 将只处理首个转移并缓冲其余，在流结束时有内容丢失风险。
        loop {
            let current_text = self._buffer.clone();

            // 若尚未剥除则剥除前导 <think> 标签。处理两种情形：
            // 1. force_reasoning=true 且模型也以文本形式发出 <think>
            // 2. 首次调用且 <think> 出现在缓冲位置 0
            // 文本中部的 <think>（位置 > 0）落入下方 find() 分支。
            if !self.stripped_think_start
                && current_text.starts_with(self.think_start_token.as_str())
            {
                self._buffer = current_text[self.think_start_token.len()..].to_string();
                self.stripped_think_start = true;
                self._in_reasoning = true;
                continue;
            }

            // 缓冲是起始 token 的前缀（如 "<think>" 的 "<thi"）——等待更多数据再决定剥除还是
            // 当作推理发出。仅当 force_reasoning=true 且尚未剥除标签时适用。
            if !self.stripped_think_start
                && self._in_reasoning
                && !current_text.is_empty()
                && self.think_start_token.starts_with(current_text.as_str())
            {
                break;
            }

            if self._in_reasoning {
                let end_idx = current_text.find(self.think_end_token.as_str());
                let tool_idx = self
                    .tool_start_token
                    .as_deref()
                    .and_then(|tok| current_text.find(tok));

                // 取最先出现的标记。若只存在其一则用之。
                let force_exit_idx = match (end_idx, tool_idx) {
                    (Some(e), Some(t)) if t < e => Some(t),
                    (None, Some(t)) => Some(t),
                    _ => None,
                };

                if let Some(tool_at) = force_exit_idx {
                    accumulated_reasoning.push_str(&current_text[..tool_at]);
                    accumulated_normal.push_str(&current_text[tool_at..]);
                    self._buffer.clear();
                    self._in_reasoning = false;
                    self.stripped_think_start = false;
                    break;
                }

                if let Some(end_idx) = end_idx {
                    // 推理块结束：累积内容并转出。
                    accumulated_reasoning.push_str(&current_text[..end_idx]);
                    let after_end = end_idx + self.think_end_token.len();
                    self._buffer = current_text[after_end..].to_string();
                    self._in_reasoning = false;
                    self.stripped_think_start = false; // 允许检测下一个 <think> 块
                    continue; // 处理其余——可能含更多块
                } else {
                    // 无完整结束 token——检查缓冲末尾的部分前缀
                    // （如 "reasoning content</th"，其中 "</th" 是 "</think>" 的前缀）。
                    // tool_start_token 的部分前缀也须缓冲，使强制退出标记不被切入推理文本。
                    if self.stream_reasoning {
                        let ol_end = overlap(&current_text, &self.think_end_token);
                        let ol_tool = self
                            .tool_start_token
                            .as_deref()
                            .map(|tok| overlap(&current_text, tok))
                            .unwrap_or(0);
                        let ol = ol_end.max(ol_tool);
                        if ol >= 2 {
                            let safe_end = current_text.len() - ol;
                            if safe_end > 0 {
                                accumulated_reasoning.push_str(&current_text[..safe_end]);
                            }
                            self._buffer = current_text[safe_end..].to_string();
                        } else {
                            accumulated_reasoning.push_str(&current_text);
                            self._buffer.clear();
                        }
                    }
                    // 当 stream_reasoning=false 时，缓冲保留全部内容直到 </think> 到达——
                    // 无需重叠检查。
                    break;
                }
            } else {
                // 不在推理中——寻找下一个 <think> 块。
                if let Some(think_pos) = current_text.find(self.think_start_token.as_str()) {
                    accumulated_normal.push_str(&current_text[..think_pos]);
                    let after_start = think_pos + self.think_start_token.len();
                    self._buffer = current_text[after_start..].to_string();
                    self._in_reasoning = true;
                    self.stripped_think_start = true;
                    continue; // 处理推理内容
                } else {
                    // 无完整起始 token——检查缓冲末尾的部分前缀
                    // （如 "Hello world <th"，其中 "<th" 是 "<think>" 的前缀）。
                    // 要求 overlap >= 2，使单独的 `<` 能透传给工具调用 XML 标签
                    // （如 `<invoke>` 或 `<minimax:tool_call>`）。
                    let ol = overlap(&current_text, &self.think_start_token);
                    if ol >= 2 {
                        let safe_end = current_text.len() - ol;
                        if safe_end > 0 {
                            accumulated_normal.push_str(&current_text[..safe_end]);
                        }
                        self._buffer = current_text[safe_end..].to_string();
                    } else {
                        accumulated_normal.push_str(&current_text);
                        self._buffer.clear();
                    }
                    break;
                }
            }
        }

        ParserResult {
            normal_text: accumulated_normal,
            reasoning_text: accumulated_reasoning,
        }
    }
}

#[cfg(test)]
mod tests {
    //! ## 测试过程
    //! 覆盖 `BasicReasoningParser` 在批式与流式下的行为：单/多 `<think>` 块抽取、无推理透传、
    //! 截断推理、`force_reasoning`、`stream_reasoning` 开关、部分标签跨块切分、
    //! `tool_start_token` 强制退出等。
    //!
    //! ## 意义
    //! 锁定推理解析的可观察契约，确保与工具调用解析器串行协作时不丢失或错分内容。
    use super::*;
    use rstest::rstest;

    #[test] // REASONING.batch.1
    fn test_detect_and_parse_reasoning_reasoning() {
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);
        let result =
            parser.detect_and_parse_reasoning("<think>with reasoning</think> and more text.", &[]);
        assert_eq!(result.normal_text, "and more text.");
        assert_eq!(result.reasoning_text, "with reasoning");
    }
    #[test] // REASONING.batch.3 — no reasoning content
    fn test_detect_and_parse_reasoning_reasoning_no_reasoning() {
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);
        let result = parser.detect_and_parse_reasoning("This is a test without reasoning.", &[]);
        assert_eq!(result.normal_text, "This is a test without reasoning.");
        assert_eq!(result.reasoning_text, "");
    }
    #[test] // REASONING.batch.4, REASONING.batch.1
    fn test_detect_and_parse_reasoning_reasoning_truncated_reasoning() {
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);
        let result = parser.detect_and_parse_reasoning("<think>with truncated reasoning", &[]);
        assert_eq!(result.normal_text, "");
        assert_eq!(result.reasoning_text, "with truncated reasoning");
    }

    #[test] // REASONING.stream.3, REASONING.batch.1
    fn test_parse_reasoning_streaming_incremental() {
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);
        let result = parser.parse_reasoning_streaming_incremental("<thi", &[]);
        assert_eq!(result.normal_text, "");
        assert_eq!(result.reasoning_text, "");
    }

    #[test] // REASONING.stream.3, REASONING.batch.1
    fn test_parse_reasoning_streaming_incremental_complete() {
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);
        let result = parser.parse_reasoning_streaming_incremental(
            "<think>with reasoning</think> and more text.",
            &[],
        );
        assert_eq!(result.normal_text, " and more text.");
        assert_eq!(result.reasoning_text, "with reasoning");
    }

    #[test] // REASONING.batch.4, REASONING.batch.5, REASONING.stream.3, REASONING.batch.1
    fn test_parse_reasoning_streaming_incremental_no_end_token() {
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), true, true);
        let result = parser.parse_reasoning_streaming_incremental("<think>with reasoning", &[]);
        assert_eq!(result.normal_text, "");
        assert_eq!(result.reasoning_text, "with reasoning");
    }

    #[test] // REASONING.batch.2, REASONING.batch.1 — multi-block
    fn test_detect_and_parse_reasoning_multiple_reasoning_blocks() {
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);
        let result = parser.detect_and_parse_reasoning(
            "<think>first reasoning</think> middle <think>second reasoning</think> end",
            &[],
        );
        assert_eq!(result.normal_text, "middle  end");
        assert_eq!(result.reasoning_text, "first reasoningsecond reasoning");
    }

    #[test] // REASONING.batch.2, REASONING.stream.3, REASONING.batch.1
    fn test_streaming_multiple_reasoning_blocks() {
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, false);
        let result1 = parser
            .parse_reasoning_streaming_incremental("<think>first reasoning</think> middle", &[]);
        assert_eq!(result1.normal_text, " middle");
        assert_eq!(result1.reasoning_text, "first reasoning");

        // 第二个推理块：<think> 前的空格是普通前缀，推理内容被提取
        let result2 = parser
            .parse_reasoning_streaming_incremental(" <think>second reasoning</think> end", &[]);
        assert_eq!(result2.reasoning_text, "second reasoning");
        assert_eq!(result2.normal_text, "  end"); // " " prefix + " end" suffix
    }

    #[test] // REASONING.stream.3, helper
    fn test_partial_token_matching_opening_tag() {
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);

        // 传入部分起始标签
        let result1 = parser.parse_reasoning_streaming_incremental("<th", &[]);
        assert_eq!(result1.normal_text, "");
        assert_eq!(result1.reasoning_text, "");

        // 补全起始标签并添加内容
        let result2 = parser.parse_reasoning_streaming_incremental(
            "ink>reasoning content</think> normal text",
            &[],
        );
        assert_eq!(result2.normal_text, " normal text");
        assert_eq!(result2.reasoning_text, "reasoning content");
    }

    #[test] // REASONING.stream.3, helper
    fn test_partial_token_matching_closing_tag() {
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, false);

        // 以完整起始标签和部分内容开始
        let result1 =
            parser.parse_reasoning_streaming_incremental("<think>reasoning content</th", &[]);
        assert_eq!(result1.normal_text, "");
        assert_eq!(result1.reasoning_text, "");

        // 补全闭合标签
        let result2 = parser.parse_reasoning_streaming_incremental("ink> normal text", &[]);
        assert_eq!(result2.normal_text, " normal text");
        assert_eq!(result2.reasoning_text, "reasoning content");
    }

    #[test] // REASONING.stream.3
    fn test_buffer_state_persistence_across_calls() {
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, false);

        // 首次调用 — 部分起始标签
        let result1 = parser.parse_reasoning_streaming_incremental("<th", &[]);
        assert_eq!(result1.normal_text, "");
        assert_eq!(result1.reasoning_text, "");

        // 第二次调用 — 完整起始标签，开始推理
        let result2 = parser.parse_reasoning_streaming_incremental("ink>part1 ", &[]);
        assert_eq!(result2.normal_text, "");
        assert_eq!(result2.reasoning_text, "");

        // 第三次调用 — 更多推理内容
        let result3 = parser.parse_reasoning_streaming_incremental("part2 ", &[]);
        assert_eq!(result3.normal_text, "");
        assert_eq!(result3.reasoning_text, "");

        // 第四次调用 — 结束推理并转到普通文本
        let result4 = parser.parse_reasoning_streaming_incremental("part3</think> normal", &[]);
        assert_eq!(result4.normal_text, " normal");
        assert_eq!(result4.reasoning_text, "part1 part2 part3");
    }

    #[test] // REASONING.stream.3, REASONING.batch.1
    fn test_streaming_with_stream_reasoning_enabled() {
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);

        // 开始推理块
        let result1 = parser.parse_reasoning_streaming_incremental("<think>reasoning ", &[]);
        assert_eq!(result1.normal_text, "");
        assert_eq!(result1.reasoning_text, "reasoning ");

        // 继续流式推理
        let result2 = parser.parse_reasoning_streaming_incremental("content ", &[]);
        assert_eq!(result2.normal_text, "");
        assert_eq!(result2.reasoning_text, "content ");

        // 结束推理块
        let result3 = parser.parse_reasoning_streaming_incremental("more</think> normal", &[]);
        assert_eq!(result3.normal_text, " normal");
        assert_eq!(result3.reasoning_text, "more");
    }

    #[test] // REASONING.batch.1 — nested
    fn test_nested_reasoning_blocks() {
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);
        let result = parser.detect_and_parse_reasoning(
            "<think>outer <think>inner</think> reasoning</think> normal",
            &[],
        );
        // 基于游标的解析：首个 <think> 开始推理，首个 </think> 终止。
        // "outer <think>inner" 为推理（内层 <think> 只是推理内的普通文本）。
        // " reasoning</think> normal" 为普通文本（游离的 </think> 穿透）。
        assert_eq!(result.reasoning_text, "outer <think>inner");
        assert_eq!(result.normal_text, "reasoning</think> normal");
    }

    #[test] // REASONING.batch.4, REASONING.batch.5
    fn test_malformed_missing_closing_tag() {
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);
        let result = parser.detect_and_parse_reasoning("<think>reasoning without closing tag", &[]);
        assert_eq!(result.normal_text, "");
        assert_eq!(result.reasoning_text, "reasoning without closing tag");
    }

    #[test] // REASONING.batch.4
    fn test_malformed_stray_closing_tag() {
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);
        let result = parser.detect_and_parse_reasoning("normal text</think> more normal", &[]);
        assert_eq!(result.normal_text, "normal text</think> more normal");
        assert_eq!(result.reasoning_text, "");
    }

    #[test] // REASONING.batch.4
    fn test_malformed_multiple_opening_tags() {
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);
        let result = parser
            .detect_and_parse_reasoning("<think>first <think>second reasoning</think> normal", &[]);
        // 基于游标：首个 <think> 打开推理，找到首个 </think>。
        // 内层 <think> 只是推理块内的普通文本。
        assert_eq!(result.reasoning_text, "first <think>second reasoning");
        assert_eq!(result.normal_text, "normal");
    }

    #[test] // REASONING.batch.6, REASONING.batch.1
    fn test_empty_reasoning_block() {
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);
        let result = parser.detect_and_parse_reasoning("<think></think> normal text", &[]);
        assert_eq!(result.normal_text, "normal text");
        assert_eq!(result.reasoning_text, "");
    }

    #[test] // REASONING.batch.1, PARSER.fmt.2
    fn test_whitespace_only_reasoning_block() {
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);
        let result = parser.detect_and_parse_reasoning("<think>   \n\t  </think> normal text", &[]);
        assert_eq!(result.normal_text, "normal text");
        assert_eq!(result.reasoning_text, ""); // Should be empty after trim
    }

    #[test] // REASONING.batch.1 — force-mode
    fn test_force_reasoning_mode() {
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), true, true);
        let result = parser.detect_and_parse_reasoning("no think tags here", &[]);
        assert_eq!(result.normal_text, "");
        assert_eq!(result.reasoning_text, "no think tags here");
    }

    #[test] // REASONING.stream.3, REASONING.batch.1
    fn test_streaming_reset_state_after_complete_block() {
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);

        // 处理完整推理块
        let result1 =
            parser.parse_reasoning_streaming_incremental("<think>reasoning</think> normal", &[]);
        assert_eq!(result1.normal_text, " normal");
        assert_eq!(result1.reasoning_text, "reasoning");

        // 处理普通文本 — 不应受先前状态影响
        let result2 = parser.parse_reasoning_streaming_incremental(" more normal text", &[]);
        assert_eq!(result2.normal_text, " more normal text");
        assert_eq!(result2.reasoning_text, "");

        // 后续推理块应被正常解析（交错思考）
        // 前导 " " 在 <think> 之前是普通文本前缀；" final" 是后缀。
        let result3 = parser
            .parse_reasoning_streaming_incremental(" <think>new reasoning</think> final", &[]);
        assert_eq!(result3.reasoning_text, "new reasoning");
        assert_eq!(result3.normal_text, "  final"); // " " prefix + " final" suffix

        // 以分块方式重复相同测试以更清晰
        let mut parser2 =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);

        let r1 = parser2.parse_reasoning_streaming_incremental("<think>first</think> normal", &[]);
        assert_eq!(r1.reasoning_text, "first");
        assert_eq!(r1.normal_text, " normal");

        let r2 = parser2.parse_reasoning_streaming_incremental(" between", &[]);
        assert_eq!(r2.normal_text, " between");
        assert_eq!(r2.reasoning_text, "");

        let r3 = parser2.parse_reasoning_streaming_incremental("<think>second</think> final", &[]);
        assert_eq!(r3.reasoning_text, "second");
        assert_eq!(r3.normal_text, " final");
    }

    #[test] // REASONING.batch.2, REASONING.batch.3
    fn test_post_reasoning_angle_bracket_not_buffered() {
        // 推理结束后，单独的 `<` 应立即作为普通文本透传。
        // 不得将其作为 <think> 或 </think> 的潜在前缀进行缓冲，
        // 否则下游工具调用 jail 将丢失 `<`（如 `<invoke` 变成 `invoke`）。
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);

        // 处理一个完整推理块
        let r1 =
            parser.parse_reasoning_streaming_incremental("<think>reasoning content</think>", &[]);
        assert_eq!(r1.reasoning_text, "reasoning content");
        assert_eq!(r1.normal_text, "");

        // 推理结束后，孤立的 `<` 必须作为普通文本透传
        let r2 = parser.parse_reasoning_streaming_incremental("<", &[]);
        assert_eq!(r2.normal_text, "<");
        assert_eq!(r2.reasoning_text, "");

        // 下一个 token 应独立到达（不与缓冲的 `<` 合并）
        let r3 = parser.parse_reasoning_streaming_incremental("invoke name=\"get_weather\">", &[]);
        assert_eq!(r3.normal_text, "invoke name=\"get_weather\">");
        assert_eq!(r3.reasoning_text, "");
    }

    #[test] // REASONING.batch.2
    fn test_post_reasoning_tool_call_xml_preserved() {
        // 模拟 MiniMax 工具调用场景：推理后接 XML 工具调用。
        // `<invoke` 中的 `<` 不得被推理解析器消费。
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);

        let r1 = parser.parse_reasoning_streaming_incremental("<think>let me check", &[]);
        assert_eq!(r1.reasoning_text, "let me check");

        let r2 = parser.parse_reasoning_streaming_incremental("</think>", &[]);
        assert_eq!(r2.normal_text, "");
        assert_eq!(r2.reasoning_text, "");

        // 工具调用标记应完整透传
        let r3 = parser.parse_reasoning_streaming_incremental("<minimax:tool_call>", &[]);
        assert_eq!(r3.normal_text, "<minimax:tool_call>");

        let r4 = parser.parse_reasoning_streaming_incremental("\n", &[]);
        assert_eq!(r4.normal_text, "\n");

        // 推理结束后作为单独 token 到达的 `<` 不得被缓冲
        let r5 = parser.parse_reasoning_streaming_incremental("<", &[]);
        assert_eq!(r5.normal_text, "<");

        let r6 = parser.parse_reasoning_streaming_incremental("invoke name=\"get_weather\">", &[]);
        assert_eq!(r6.normal_text, "invoke name=\"get_weather\">");
    }

    #[test] // REASONING.stream.3, REASONING.batch.3
    fn test_interleaved_streaming_across_chunks() {
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);

        let r1 = parser.parse_reasoning_streaming_incremental("<think>thought 1</think>", &[]);
        assert_eq!(r1.reasoning_text, "thought 1");
        assert_eq!(r1.normal_text, "");

        let r2 = parser.parse_reasoning_streaming_incremental(" answer 1 ", &[]);
        assert_eq!(r2.normal_text, " answer 1 ");
        assert_eq!(r2.reasoning_text, "");

        let r3 = parser.parse_reasoning_streaming_incremental("<think>thought 2</think>", &[]);
        assert_eq!(r3.reasoning_text, "thought 2");
        assert_eq!(r3.normal_text, "");

        let r4 = parser.parse_reasoning_streaming_incremental(" answer 2", &[]);
        assert_eq!(r4.normal_text, " answer 2");
        assert_eq!(r4.reasoning_text, "");

        let r5 = parser.parse_reasoning_streaming_incremental("<think>thought 3</think>", &[]);
        assert_eq!(r5.reasoning_text, "thought 3");
        assert_eq!(r5.normal_text, "");

        let r6 = parser.parse_reasoning_streaming_incremental(" final answer", &[]);
        assert_eq!(r6.normal_text, " final answer");
        assert_eq!(r6.reasoning_text, "");
    }

    #[test] // REASONING.batch.2, REASONING.batch.1
    fn test_three_reasoning_blocks_non_streaming() {
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);
        let result = parser.detect_and_parse_reasoning(
            "<think>A</think> one <think>B</think> two <think>C</think> three",
            &[],
        );
        assert_eq!(result.reasoning_text, "ABC");
        assert_eq!(result.normal_text, "one  two  three");
    }

    #[test] // REASONING.stream.3
    fn test_streaming_transition_chunk() {
        // </think> 和 <think> 在同一个 chunk 中到达。
        // 借助循环处理，第二个块的起始内容被立即发出
        //（stream_reasoning=true），而非缓冲至下次调用。
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);

        let r1 = parser.parse_reasoning_streaming_incremental("<think>first", &[]);
        assert_eq!(r1.reasoning_text, "first");

        // 块内过渡：</think> 之后是普通文本，然后是带更多内容的 <think>。
        // 循环过渡出推理，发出 " middle " 作为普通文本，进入
        // 下一个推理块，并立即流式发出 "second"。
        let r2 = parser.parse_reasoning_streaming_incremental("</think> middle <think>second", &[]);
        assert_eq!(r2.reasoning_text, "second");
        assert_eq!(r2.normal_text, " middle ");

        // 第二个推理块的延续
        let r3 = parser.parse_reasoning_streaming_incremental(" more</think> end", &[]);
        assert_eq!(r3.reasoning_text, " more");
        assert_eq!(r3.normal_text, " end");
    }

    #[test] // REASONING.batch.1, REASONING.batch.3
    fn test_interleaved_with_force_reasoning() {
        // deepseek_r1 模式：force_reasoning=true，首个 token 无 <think> 也视为推理
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), true, true);

        // 无 <think> 标签 — 因 force_reasoning=true 视为推理
        let r1 = parser.parse_reasoning_streaming_incremental("initial reasoning", &[]);
        assert_eq!(r1.reasoning_text, "initial reasoning");
        assert_eq!(r1.normal_text, "");

        // 强制推理块结束
        let r2 = parser.parse_reasoning_streaming_incremental("</think> answer", &[]);
        assert_eq!(r2.reasoning_text, "");
        assert_eq!(r2.normal_text, " answer");

        // 带显式 <think> 的第二个推理块
        let r3 =
            parser.parse_reasoning_streaming_incremental("<think>second thought</think> done", &[]);
        assert_eq!(r3.reasoning_text, "second thought");
        assert_eq!(r3.normal_text, " done");
    }

    #[test] // REASONING.stream.3, REASONING.batch.3
    fn test_interleaved_partial_think_tag_between_blocks() {
        // 首个推理块之后，部分 <think> 标签跨块到达
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);

        let r1 = parser.parse_reasoning_streaming_incremental("<think>first</think> normal", &[]);
        assert_eq!(r1.reasoning_text, "first");
        assert_eq!(r1.normal_text, " normal");

        // 部分 <think> 前缀："<th"（2 字符，满足阈值）
        let r2 = parser.parse_reasoning_streaming_incremental("<th", &[]);
        assert_eq!(r2.normal_text, "");
        assert_eq!(r2.reasoning_text, "");

        // 补全标签
        let r3 = parser.parse_reasoning_streaming_incremental("ink>second</think> end", &[]);
        assert_eq!(r3.reasoning_text, "second");
        assert_eq!(r3.normal_text, " end");
    }

    #[test] // REASONING.batch.3, helper
    fn test_lone_angle_bracket_between_reasoning_blocks() {
        // 推理块之间的孤立 `<` 应透传（不缓冲）
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);

        let r1 = parser.parse_reasoning_streaming_incremental("<think>thought</think>", &[]);
        assert_eq!(r1.reasoning_text, "thought");

        // 孤立 `<` 不得被缓冲 — 可能是工具调用
        let r2 = parser.parse_reasoning_streaming_incremental("<", &[]);
        assert_eq!(r2.normal_text, "<");
        assert_eq!(r2.reasoning_text, "");

        let r3 = parser.parse_reasoning_streaming_incremental("tool_call>", &[]);
        assert_eq!(r3.normal_text, "tool_call>");
        assert_eq!(r3.reasoning_text, "");

        // 但在此之后真实的 <think> 仍应正常运作
        let r4 =
            parser.parse_reasoning_streaming_incremental("<think>more thought</think> done", &[]);
        assert_eq!(r4.reasoning_text, "more thought");
        assert_eq!(r4.normal_text, " done");
    }

    #[test] // REASONING.stream.3, REASONING.batch.1
    fn test_force_reasoning_stream_false_buffers_until_end_token() {
        // force_reasoning=true, stream_reasoning=false：内容被缓冲直到 </think>
        // 到达，然后作为单个 chunk 返回。这是预期行为。
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), true, false);

        // 无 <think> — 强制进入推理，stream_reasoning=false 表示静默缓冲
        let r1 = parser.parse_reasoning_streaming_incremental("chunk one", &[]);
        assert_eq!(r1.reasoning_text, "");
        assert_eq!(r1.normal_text, "");

        let r2 = parser.parse_reasoning_streaming_incremental(" chunk two", &[]);
        assert_eq!(r2.reasoning_text, "");
        assert_eq!(r2.normal_text, "");

        // </think> 到达 — 全部缓冲的推理被刷新
        let r3 = parser.parse_reasoning_streaming_incremental("</think> answer", &[]);
        assert_eq!(r3.reasoning_text, "chunk one chunk two");
        assert_eq!(r3.normal_text, " answer");
    }

    #[test] // REASONING.batch.2, REASONING.stream.3, REASONING.batch.1
    fn test_multiple_full_blocks_in_single_streaming_chunk() {
        // 两个完整 <think>...</think> 块在一个 chunk 中到达。
        // 循环在单次调用中耗尽所有转移 — 两个块被完全处理，
        // 无需后续调用来刷新缓冲内容。
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);

        let r1 = parser.parse_reasoning_streaming_incremental(
            "<think>A</think> mid <think>B</think> end",
            &[],
        );
        assert_eq!(r1.reasoning_text, "AB");
        assert_eq!(r1.normal_text, " mid  end");

        // 缓冲已完全排空；后续空调用返回空
        let r2 = parser.parse_reasoning_streaming_incremental("", &[]);
        assert_eq!(r2.reasoning_text, "");
        assert_eq!(r2.normal_text, "");
    }

    #[test] // REASONING.stream.3, helper
    fn test_partial_end_token_stream_reasoning_true() {
        // 部分 </think> 跨 chunk 切分，stream_reasoning=true。
        // 部分结束 token 缓冲检查仅在解析器已处于推理模式
        // （来自先前调用）时执行。若 <think> 和 </th 在同一 chunk 中到达，
        // stream_reasoning=true 会立即发出推理内容（含 </th）。
        // 因此 <think> 必须先作为独立 chunk 到达。
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);

        let r1 = parser.parse_reasoning_streaming_incremental("<think>reasoning", &[]);
        assert_eq!(r1.reasoning_text, "reasoning");
        assert_eq!(r1.normal_text, "");

        // 处于推理中时的部分结束 token — 被缓冲，无输出
        let r2 = parser.parse_reasoning_streaming_incremental("</th", &[]);
        assert_eq!(r2.reasoning_text, "");
        assert_eq!(r2.normal_text, "");

        // 补全结束 token
        let r3 = parser.parse_reasoning_streaming_incremental("ink> normal", &[]);
        assert_eq!(r3.reasoning_text, "");
        assert_eq!(r3.normal_text, " normal");
    }

    #[test] // REASONING.batch.3, REASONING.batch.8
    fn test_empty_string_input_various_states() {
        // 空字符串输入在各种状态下应始终返回空结果而不改变状态
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);

        // 状态：空闲
        let r1 = parser.parse_reasoning_streaming_incremental("", &[]);
        assert_eq!(r1.reasoning_text, "");
        assert_eq!(r1.normal_text, "");

        // 进入推理
        parser.parse_reasoning_streaming_incremental("<think>content", &[]);

        // 状态：推理中
        let r2 = parser.parse_reasoning_streaming_incremental("", &[]);
        assert_eq!(r2.reasoning_text, "");
        assert_eq!(r2.normal_text, "");

        // 完成并退出推理
        parser.parse_reasoning_streaming_incremental("</think>", &[]);

        // 状态：推理后（普通文本）
        let r3 = parser.parse_reasoning_streaming_incremental("", &[]);
        assert_eq!(r3.reasoning_text, "");
        assert_eq!(r3.normal_text, "");
    }

    #[test] // REASONING.batch.2, REASONING.stream.3, REASONING.batch.1
    fn test_force_reasoning_stream_false_multiple_blocks() {
        // force_reasoning=true（deepseek_r1 模式），stream_reasoning=false。
        // 首个块使用强制推理（无显式 <think>）；后续块使用显式标签。
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), true, false);

        // 无起始标签的强制推理，遇到 </think> 时刷新
        let r1 =
            parser.parse_reasoning_streaming_incremental("initial reasoning</think> normal1 ", &[]);
        assert_eq!(r1.reasoning_text, "initial reasoning");
        assert_eq!(r1.normal_text, " normal1 ");

        // 后续显式 <think> 块正常运作
        let r2 = parser
            .parse_reasoning_streaming_incremental("<think>second block</think> normal2", &[]);
        assert_eq!(r2.reasoning_text, "second block");
        assert_eq!(r2.normal_text, " normal2");
    }

    #[test] // REASONING.stream.3 — GLM-5 burst pattern
    fn test_glm5_pattern_a_burst_single_chunk() {
        // GLM-5 模式 A：整个补全在一个 SSE 事件中到达。
        // 格式：<think>T1</think><tool_call>A</tool_call><think>T2</think><tool_call>B</tool_call>
        //
        // 两个推理块都必须被提取到 reasoning_text；两个工具调用
        // 必须落入 normal_text 供下游工具调用解析器处理。无需后续
        // 调用 — 循环在单次调用中完全排空缓冲。
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);

        let r1 = parser.parse_reasoning_streaming_incremental(
            "<think>T1</think><tool_call>A</tool_call><think>T2</think><tool_call>B</tool_call>",
            &[],
        );
        assert_eq!(r1.reasoning_text, "T1T2");
        assert_eq!(
            r1.normal_text,
            "<tool_call>A</tool_call><tool_call>B</tool_call>"
        );

        // 缓冲已完全排空；流可在此结束而无内容丢失
        let r2 = parser.parse_reasoning_streaming_incremental("", &[]);
        assert_eq!(r2.reasoning_text, "");
        assert_eq!(r2.normal_text, "");
    }

    #[test] // REASONING.stream.3, REASONING.batch.2, REASONING.batch.3
    fn test_tool_call_xml_between_reasoning_blocks_streaming() {
        // GLM-5 模式 A 逐块验证：推理块之间的工具调用 XML 块
        // 经由不同 SSE 事件落入 normal_text 而非 reasoning_text。
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);

        let r1 = parser.parse_reasoning_streaming_incremental("<think>T1</think>", &[]);
        assert_eq!(r1.reasoning_text, "T1");
        assert_eq!(r1.normal_text, "");

        let r2 = parser.parse_reasoning_streaming_incremental("<tool_call>A</tool_call>", &[]);
        assert_eq!(r2.normal_text, "<tool_call>A</tool_call>");
        assert_eq!(r2.reasoning_text, "");

        let r3 = parser.parse_reasoning_streaming_incremental("<think>T2</think>", &[]);
        assert_eq!(r3.reasoning_text, "T2");
        assert_eq!(r3.normal_text, "");

        let r4 = parser.parse_reasoning_streaming_incremental("<tool_call>B</tool_call>", &[]);
        assert_eq!(r4.normal_text, "<tool_call>B</tool_call>");
        assert_eq!(r4.reasoning_text, "");
    }

    // =========================================================================
    // 中字符串部分标签测试（基于重叠的缓冲）
    //
    // 以下测试覆盖 <think> 或 </think> 标签在中字符串处被切分的场景
    // （不在缓冲起始位置）。将多个前向 token 批量合并到单个分块响应中的后端
    // 可能产生这些模式。
    //
    // 从 PR #6448（ryanolson）移植，含额外伪匹配测试。
    // =========================================================================

    #[test] // REASONING.stream.3, helper
    fn test_mid_string_partial_opening_tag_batched() {
        // 后端批量合并 token："Hello world <th" 作为一个 chunk 到达
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);

        let r1 = parser.parse_reasoning_streaming_incremental("Hello world <th", &[]);
        // "Hello world " emitted as normal, "<th" held in buffer
        assert_eq!(r1.normal_text, "Hello world ");
        assert_eq!(r1.reasoning_text, "");

        let r2 = parser
            .parse_reasoning_streaming_incremental("ink>reasoning content</think> answer", &[]);
        assert_eq!(r2.reasoning_text, "reasoning content");
        assert_eq!(r2.normal_text, " answer");
    }

    #[test] // REASONING.stream.3, helper
    fn test_batched_tag_boundary_split() {
        // 激进批量合并：<think> 标签与普通文本前缀切分
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);

        let r1 = parser.parse_reasoning_streaming_incremental("The answer is <thi", &[]);
        assert_eq!(r1.normal_text, "The answer is ");
        assert_eq!(r1.reasoning_text, "");

        let r2 = parser.parse_reasoning_streaming_incremental("nk>let me think</think>42", &[]);
        assert_eq!(r2.reasoning_text, "let me think");
        assert_eq!(r2.normal_text, "42");
    }

    #[test] // REASONING.stream.3, helper
    fn test_mid_string_partial_closing_tag_stream_reasoning_false() {
        // stream_reasoning=false 时，内容保持缓冲直到 </think>。
        // 部分 </think> 在推理模式中跨中字符串切分。
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, false);

        let r1 =
            parser.parse_reasoning_streaming_incremental("<think>reasoning content and </th", &[]);
        assert_eq!(r1.normal_text, "");
        assert_eq!(r1.reasoning_text, "");

        let r2 = parser.parse_reasoning_streaming_incremental("ink> normal text", &[]);
        assert_eq!(r2.reasoning_text, "reasoning content and ");
        assert_eq!(r2.normal_text, " normal text");
    }

    #[test] // REASONING.stream.3, helper
    fn test_mid_string_partial_closing_tag_stream_reasoning_true() {
        // stream_reasoning=true 时，推理内容被增量发出。
        // 末尾的部分 "</th" 不得作为推理文本发出。
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);

        let r1 =
            parser.parse_reasoning_streaming_incremental("<think>reasoning content and </th", &[]);
        // "reasoning content and " emitted as reasoning, "</th" held
        assert_eq!(r1.reasoning_text, "reasoning content and ");
        assert_eq!(r1.normal_text, "");

        let r2 = parser.parse_reasoning_streaming_incremental("ink> normal text", &[]);
        assert_eq!(r2.reasoning_text, "");
        assert_eq!(r2.normal_text, " normal text");
    }

    #[test] // REASONING.stream.3, REASONING.batch.3
    fn test_batched_interleaved_with_mid_string_partial() {
        // 首个块在 chunk 1 完成，第二个块的 <think> 在边界处切分
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);

        let r1 =
            parser.parse_reasoning_streaming_incremental("<think>thought1</think>answer1<thi", &[]);
        assert_eq!(r1.reasoning_text, "thought1");
        assert_eq!(r1.normal_text, "answer1");

        let r2 = parser.parse_reasoning_streaming_incremental("nk>thought2</think>answer2", &[]);
        assert_eq!(r2.reasoning_text, "thought2");
        assert_eq!(r2.normal_text, "answer2");
    }

    #[test] // helper
    fn test_partial_tag_false_positive() {
        // "<th" 看起来像部分 <think> 但 "thesis" 不是 <think>
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);

        let r1 = parser.parse_reasoning_streaming_incremental("value <thesis on", &[]);
        // "value <thesis on" 的后缀中没有任何部分匹配 "<think>" 的前缀 — 全部发出
        let r2 = parser.parse_reasoning_streaming_incremental(" AI> is great", &[]);

        let combined_normal = format!("{}{}", r1.normal_text, r2.normal_text);
        assert_eq!(combined_normal, "value <thesis on AI> is great");
        assert_eq!(r1.reasoning_text, "");
        assert_eq!(r2.reasoning_text, "");
    }

    #[test] // helper
    fn test_partial_closing_tag_fakeout() {
        // Ollama 风格伪匹配："</th" 被缓冲，但 "ing>" 补全为 "</thing>" 而非 "</think>"
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), false, true);

        let r1 = parser.parse_reasoning_streaming_incremental("<think>abc</th", &[]);
        assert_eq!(r1.reasoning_text, "abc");
        assert_eq!(r1.normal_text, "");

        // "ing>def" 补全为 "</thing>def" — 不是闭合标签
        let r2 = parser.parse_reasoning_streaming_incremental("ing>def", &[]);
        assert_eq!(r2.reasoning_text, "</thing>def");
        assert_eq!(r2.normal_text, "");

        // 真正的闭合标签到达
        let r3 = parser.parse_reasoning_streaming_incremental("</think>done", &[]);
        assert_eq!(r3.reasoning_text, "");
        assert_eq!(r3.normal_text, "done");
    }

    #[test] // internal helper
    fn test_overlap_helper_function() {
        // overlap 工具函数的直接测试
        assert_eq!(overlap("abc</th", "</think>"), 4);
        assert_eq!(overlap("abc</thing>def", "</think>"), 0);
        assert_eq!(overlap("<", "<think>"), 1);
        assert_eq!(overlap("<th", "<think>"), 3);
        assert_eq!(overlap("<think>", "<think>"), 7); // full match
        assert_eq!(overlap("no match", "<think>"), 0);
        assert_eq!(overlap("", "<think>"), 0);
        assert_eq!(overlap("Hello world <thi", "<think>"), 4);
        // 多字节分隔符（Kimi 解析器使用 ◁think▷ / ◁/think▷）
        assert_eq!(overlap("text◁", "◁think▷"), 3); // ◁ 占 3 字节
        assert_eq!(overlap("text◁th", "◁think▷"), 5);
        assert_eq!(overlap("text◁/thi", "◁/think▷"), 7);
        assert_eq!(overlap("no match", "◁think▷"), 0);
    }

    fn kimi_k2_parser() -> BasicReasoningParser {
        // 对应 reasoning/mod.rs 中的 `kimi_k25` 注册。
        BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), true, true)
            .with_tool_start_token(crate::reasoning::KIMI_K2_TOOL_SECTION_BEGIN)
    }

    #[rstest] // REASONING.stream.3 — Kimi K2 split
    #[case(
        "thinking text <|tool_calls_section_begin|><|tool_call_begin|>functions.foo:0<|tool_call_argument_begin|>{}<|tool_call_end|><|tool_calls_section_end|>",
        "thinking text",
        "<|tool_calls_section_begin|><|tool_call_begin|>functions.foo:0<|tool_call_argument_begin|>{}<|tool_call_end|><|tool_calls_section_end|>"
    )]
    #[case("r</think>a", "r", "a")]
    #[case(
        "reasoning</think>answer <|tool_calls_section_begin|>tc",
        "reasoning",
        "answer <|tool_calls_section_begin|>tc"
    )]
    fn test_kimi_k2_one_shot_split(
        #[case] input: &str,
        #[case] expected_reasoning: &str,
        #[case] expected_normal: &str,
    ) {
        let mut parser = kimi_k2_parser();
        let r = parser.detect_and_parse_reasoning(input, &[]);
        assert_eq!(r.reasoning_text, expected_reasoning);
        assert_eq!(r.normal_text, expected_normal);
    }

    #[test] // REASONING.stream.3, REASONING.batch.1
    fn test_force_exit_streaming_single_chunk() {
        let mut parser = kimi_k2_parser();
        let r = parser.parse_reasoning_streaming_incremental(
            "thinking text <|tool_calls_section_begin|><|tool_call_begin|>functions.foo:0<|tool_call_argument_begin|>{}<|tool_call_end|><|tool_calls_section_end|>",
            &[],
        );
        assert_eq!(r.reasoning_text, "thinking text ");
        assert_eq!(
            r.normal_text,
            "<|tool_calls_section_begin|><|tool_call_begin|>functions.foo:0<|tool_call_argument_begin|>{}<|tool_call_end|><|tool_calls_section_end|>"
        );
    }

    #[test] // REASONING.stream.3, REASONING.batch.1
    fn test_force_exit_streaming_split_across_chunks() {
        let mut parser = kimi_k2_parser();

        let r1 = parser.parse_reasoning_streaming_incremental("thinking ", &[]);
        assert_eq!(r1.reasoning_text, "thinking ");
        assert_eq!(r1.normal_text, "");

        // 第二个 chunk 以工具标记的前缀结尾 — 该后缀必须被缓冲。
        let r2 = parser.parse_reasoning_streaming_incremental("text <|tool_cal", &[]);
        assert_eq!(r2.reasoning_text, "text ");
        assert_eq!(r2.normal_text, "");

        let r3 = parser.parse_reasoning_streaming_incremental("ls_section_begin|>rest", &[]);
        assert_eq!(r3.reasoning_text, "");
        assert_eq!(r3.normal_text, "<|tool_calls_section_begin|>rest");
    }

    #[test] // REASONING.stream.3, helper
    fn test_force_exit_partial_marker_resolves_as_non_marker() {
        // 首个 chunk 以 "<|tool_ca" 结尾（标记前缀）——必须被缓冲。
        // 第二个 chunk "xxx" 使合并后的 "<|tool_caxxx" 不是标记。
        // 由于 force_reasoning=true，内容随后以推理形式刷新。
        let mut parser = kimi_k2_parser();

        let r1 = parser.parse_reasoning_streaming_incremental("abc <|tool_ca", &[]);
        assert_eq!(r1.reasoning_text, "abc ");
        assert_eq!(r1.normal_text, "");

        let r2 = parser.parse_reasoning_streaming_incremental("xxx", &[]);
        assert_eq!(r2.reasoning_text, "<|tool_caxxx");
        assert_eq!(r2.normal_text, "");
    }

    #[test] // REASONING.batch.1
    fn test_no_tool_start_token_behaves_as_before() {
        // 未设置 tool_start_token 时，BasicReasoningParser 行为与补丁前逐字节一致——
        // 标记只是推理内容。
        let mut parser =
            BasicReasoningParser::new("<think>".to_string(), "</think>".to_string(), true, true);
        let r =
            parser.detect_and_parse_reasoning("thinking <|tool_calls_section_begin|>stuff", &[]);
        assert_eq!(
            r.reasoning_text,
            "thinking <|tool_calls_section_begin|>stuff"
        );
        assert_eq!(r.normal_text, "");
    }
}
