// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::{ParserResult, ReasoningParser};

/// MiniMax Append-Think Reasoning Parser.
///
/// ## 设计意图
/// MiniMax 模型直接开始生成推理内容，输出中并不发出 `<think>` 起始标记。
/// SGLang 的 `MiniMaxAppendThinkDetector` 与 vLLM 的 `MiniMaxM2AppendThinkReasoningParser`
/// 都只是给文本前置一个 `<think>` 并把整段流归类为 `normal_text`/content——
/// 二者都不依据 `</think>` 标记抽取推理。该标记原样内联保留，供下游渲染或后处理。
///
/// ## 外部契约
/// 与上述上游实现逐字一致：透传 + 首个流式块一次性前置 `<think>`，
/// `reasoning_text` 永不填充。
///
/// 参考：
/// - SGLang MiniMaxAppendThinkDetector:
///   <https://github.com/sgl-project/sglang/blob/main/python/sglang/srt/parser/reasoning_parser.py>
/// - vLLM MiniMaxM2AppendThinkReasoningParser:
///   <https://github.com/vllm-project/vllm/blob/main/vllm/reasoning/minimax_m2_reasoning_parser.py>
#[derive(Debug, Default)]
pub struct MiniMaxAppendThinkParser {
    /// 首个流式块加上 `<think>` 前缀后翻为 true，后续块原样透传。
    prefix_emitted: bool,
}

impl MiniMaxAppendThinkParser {
    pub fn new() -> Self {
        Self::default()
    }
}

const THINK_START_TOKEN: &str = "<think>";

impl ReasoningParser for MiniMaxAppendThinkParser {
    fn detect_and_parse_reasoning(&mut self, text: &str, _token_ids: &[u32]) -> ParserResult {
        // 非流式：返回带单个 `<think>` 前缀的完整文本，全部作为 normal_text。
        // 推理抽取刻意为空操作。
        ParserResult {
            normal_text: format!("{THINK_START_TOKEN}{text}"),
            reasoning_text: String::new(),
        }
    }

    fn parse_reasoning_streaming_incremental(
        &mut self,
        text: &str,
        _token_ids: &[u32],
    ) -> ParserResult {
        // 仅首块前置 `<think>`，之后透传
        let normal_text = if self.prefix_emitted {
            text.to_string()
        } else {
            self.prefix_emitted = true;
            format!("{THINK_START_TOKEN}{text}")
        };
        ParserResult {
            normal_text,
            reasoning_text: String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    //! ## 测试过程
    //! 围绕 `MiniMaxAppendThinkParser` 的非流式与流式增量接口，验证：整段加 `<think>`
    //! 前缀且全归 normal_text、`</think>` 不触发拆分、流式仅首块加前缀、bare-JSON 工具调用
    //! 与 minimax 工具调用标记均原样透传。
    //!
    //! ## 意义
    //! 锁定与 SGLang / vLLM 一致的 append-think 透传语义（reasoning_text 永远为空）。
    use super::*;

    #[test] // REASONING.batch.1 — minimax inline-reasoning
    fn test_detect_and_parse_prepends_think_all_as_normal_text() {
        let mut parser = MiniMaxAppendThinkParser::new();
        let result = parser.detect_and_parse_reasoning("reasoning content here", &[]);
        // 与 SGLang 一致：全部内容为 normal_text，加 `<think>` 前缀。
        assert_eq!(result.normal_text, "<think>reasoning content here");
        assert_eq!(result.reasoning_text, "");
    }

    #[test] // REASONING.batch.1 — minimax inline-reasoning
    fn test_detect_and_parse_with_end_token_is_still_normal_text() {
        let mut parser = MiniMaxAppendThinkParser::new();
        let result =
            parser.detect_and_parse_reasoning("reasoning content</think>normal response", &[]);
        // SGLang 不按 `</think>` 切分——整个字符串（带前置 `<think>`）以 normal_text 透传。
        assert_eq!(
            result.normal_text,
            "<think>reasoning content</think>normal response"
        );
        assert_eq!(result.reasoning_text, "");
    }

    #[test] // REASONING.stream.3, REASONING.batch.1
    fn test_streaming_first_chunk_gets_prefix_rest_pass_through() {
        let mut parser = MiniMaxAppendThinkParser::new();

        let r1 = parser.parse_reasoning_streaming_incremental("I need to ", &[]);
        assert_eq!(r1.normal_text, "<think>I need to ");
        assert_eq!(r1.reasoning_text, "");

        let r2 = parser.parse_reasoning_streaming_incremental("check the weather", &[]);
        assert_eq!(r2.normal_text, "check the weather");
        assert_eq!(r2.reasoning_text, "");

        let r3 = parser.parse_reasoning_streaming_incremental("</think>The weather is sunny.", &[]);
        // 不拆分——`</think>` 原样出现在 normal_text 中。
        assert_eq!(r3.normal_text, "</think>The weather is sunny.");
        assert_eq!(r3.reasoning_text, "");
    }

    #[test] // REASONING.batch.3 — minimax leaves tool-call shape inline
    fn test_streaming_bare_json_tool_call_is_normal_text() {
        // 回归测试：在 SGLang 引导解码下，模型发出不带 `</think>` 的裸 JSON 数组。
        // 解析器不得将其捕获为推理——必须透传以使工具调用 jail 能将其
        // 提取为结构化 tool_calls。
        let mut parser = MiniMaxAppendThinkParser::new();
        let r = parser.parse_reasoning_streaming_incremental(
            r#"[{"name":"get_weather","parameters":{"location":"San Francisco"}}]"#,
            &[],
        );
        assert_eq!(
            r.normal_text,
            r#"<think>[{"name":"get_weather","parameters":{"location":"San Francisco"}}]"#
        );
        assert_eq!(r.reasoning_text, "");
    }

    #[test] // REASONING.batch.2, REASONING.batch.3 — minimax inline-reasoning
    fn test_streaming_tool_call_after_reasoning_is_all_normal_text() {
        let mut parser = MiniMaxAppendThinkParser::new();

        let r1 = parser.parse_reasoning_streaming_incremental("let me call a tool", &[]);
        assert_eq!(r1.normal_text, "<think>let me call a tool");

        let r2 = parser.parse_reasoning_streaming_incremental(
            "</think><minimax:tool_call><invoke name=\"get_weather\">",
            &[],
        );
        // 整个 chunk 为 normal_text——`</think>` 不被消费。
        assert_eq!(
            r2.normal_text,
            "</think><minimax:tool_call><invoke name=\"get_weather\">"
        );
        assert_eq!(r2.reasoning_text, "");
    }
}
