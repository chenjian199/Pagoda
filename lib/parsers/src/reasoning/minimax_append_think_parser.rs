// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-License-Identifier: Apache-2.0
//
// API-compatible implementation based on the public interfaces and behavioral
// contracts of NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
// Implementation rewritten by PAGODA.

use crate::{ParserResult, ReasoningParser};

///
/// ## 设计意图
///
/// ## 外部契约
///
/// 参考：
#[derive(Debug, Default)]
pub struct MiniMaxAppendThinkParser {
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
    //!
    //! ## 意义
    use super::*;

    #[test] // REASONING.batch.1 — minimax inline-reasoning
    fn test_detect_and_parse_prepends_think_all_as_normal_text() {
        let mut parser = MiniMaxAppendThinkParser::new();
        let result = parser.detect_and_parse_reasoning("reasoning content here", &[]);
        assert_eq!(result.normal_text, "<think>reasoning content here");
        assert_eq!(result.reasoning_text, "");
    }

    #[test] // REASONING.batch.1 — minimax inline-reasoning
    fn test_detect_and_parse_with_end_token_is_still_normal_text() {
        let mut parser = MiniMaxAppendThinkParser::new();
        let result =
            parser.detect_and_parse_reasoning("reasoning content</think>normal response", &[]);
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
        assert_eq!(r3.normal_text, "</think>The weather is sunny.");
        assert_eq!(r3.reasoning_text, "");
    }

    #[test] // REASONING.batch.3 — minimax leaves tool-call shape inline
    fn test_streaming_bare_json_tool_call_is_normal_text() {
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
        assert_eq!(
            r2.normal_text,
            "</think><minimax:tool_call><invoke name=\"get_weather\">"
        );
        assert_eq!(r2.reasoning_text, "");
    }
}
