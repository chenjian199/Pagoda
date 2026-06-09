// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Granite 推理解析器。
//!
//! ## 设计意图
//! 解析 IBM Granite 风格的推理输出：用自然语言短语
//! `Here's my thought process:` / `Here's my response:`（及 `Here is ...` 变体）
//! 而非 `<think>` 标签来界定推理段与回答段。
//!
//! ## 外部契约
//! - `GraniteReasoningParser`（`new`/`default`）实现 `ReasoningParser` 的
//!   `detect_and_parse_reasoning`（非流式）与 `parse_reasoning_streaming_incremental`（流式）。
//! - 起止短语大小写敏感；末尾短语缺失时视为推理被截断；空段产出空串。
//!
//! ## 实现要点
//! - 流式按 buffer 累积，遇到任一起止短语的真前缀则继续缓冲。
//! - 首次遇到起始短语后剥除并进入推理态，遇结束短语则切出推理与正常文本。

use crate::ParserResult;
use crate::ReasoningParser;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GraniteReasoningParser {
    think_start_tokens: Vec<String>,
    think_end_tokens: Vec<String>,
    buffer: String,
    stripped_think_start: bool,
    in_reasoning: bool,
}

impl GraniteReasoningParser {
    pub fn new() -> Self {
        let to_vec = |arr: [&str; 2]| arr.iter().map(|s| s.to_string()).collect();
        Self {
            think_start_tokens: to_vec([
                "Here's my thought process:",
                "Here is my thought process:",
            ]),
            think_end_tokens: to_vec(["Here's my response:", "Here is my response:"]),
            buffer: String::new(),
            stripped_think_start: false,
            in_reasoning: false,
        }
    }

    /// 返回 `tokens` 中首个出现在 `text` 内的短语；都不出现时退回首个短语。
    fn first_present_or_default<'a>(tokens: &'a [String], text: &str) -> &'a String {
        tokens
            .iter()
            .find(|token| text.contains(token.as_str()))
            .unwrap_or_else(|| tokens.first().unwrap())
    }

    /// `current_text` 是否恰为某短语的真前缀（相等不算）。
    fn is_strict_prefix_of_any(tokens: &[String], current_text: &str) -> bool {
        tokens
            .iter()
            .any(|token| token.starts_with(current_text) && token.as_str() != current_text)
    }
}

impl Default for GraniteReasoningParser {
    fn default() -> Self {
        Self::new()
    }
}

impl ReasoningParser for GraniteReasoningParser {
    fn detect_and_parse_reasoning(&mut self, text: &str, _: &[u32]) -> ParserResult {
        let think_start_token = Self::first_present_or_default(&self.think_start_tokens, text);
        let think_end_token = Self::first_present_or_default(&self.think_end_tokens, text);

        // 处于推理态，或文本含任一起始短语，才进入推理处理
        let in_reasoning = self.in_reasoning
            || self
                .think_start_tokens
                .iter()
                .any(|token| text.contains(token.as_str()));
        if !in_reasoning {
            return ParserResult {
                normal_text: text.to_string(),
                reasoning_text: String::new(),
            };
        }

        // 视为处于推理块：剥除起始短语
        let processed_text = text.replacen(think_start_token, "", 1).trim().to_string();

        // 无结束短语：推理在 think_end_token 前被截断
        let Some(end_idx) = processed_text.find(think_end_token.as_str()) else {
            return ParserResult {
                normal_text: String::new(),
                reasoning_text: processed_text,
            };
        };

        // 切出推理与其后的正常文本
        let reasoning_text = processed_text[..end_idx].to_string();
        let normal_text = processed_text[end_idx + think_end_token.len()..]
            .trim()
            .to_string();

        ParserResult {
            normal_text,
            reasoning_text,
        }
    }

    fn parse_reasoning_streaming_incremental(&mut self, text: &str, _: &[u32]) -> ParserResult {
        // 增量累积到 buffer
        self.buffer.push_str(text);
        let mut current_text = self.buffer.to_string();

        let empty_result = || ParserResult {
            normal_text: String::new(),
            reasoning_text: String::new(),
        };

        // 当前文本若是某起止短语的真前缀，则继续缓冲
        if Self::is_strict_prefix_of_any(&self.think_start_tokens, &current_text)
            || Self::is_strict_prefix_of_any(&self.think_end_tokens, &current_text)
        {
            return empty_result();
        }

        let think_start_token =
            Self::first_present_or_default(&self.think_start_tokens, &current_text).clone();
        let think_end_token =
            Self::first_present_or_default(&self.think_end_tokens, &current_text).clone();

        // 首次命中起始短语：剥除并进入推理态
        if !self.stripped_think_start && current_text.contains(&think_start_token) {
            current_text = current_text.replacen(&think_start_token, "", 1);
            self.buffer = current_text.to_string();
            self.stripped_think_start = true;
            self.in_reasoning = true;
        }

        // 推理块内查找结束短语
        let think_end_idx = if self.in_reasoning {
            current_text
                .find(&think_end_token)
                .unwrap_or(current_text.len())
        } else {
            current_text.len()
        };

        if self.in_reasoning && think_end_idx < current_text.len() {
            // 结束短语已到：切出推理与其后正常文本
            let reasoning_text = current_text[..think_end_idx].to_string();
            self.buffer.clear();
            self.in_reasoning = false;
            let start_idx = think_end_idx + think_end_token.len();
            let normal_text = current_text.get(start_idx..).unwrap_or("").to_string();
            return ParserResult {
                normal_text,
                reasoning_text,
            };
        }

        // 推理进行中：立即流出推理内容
        if self.in_reasoning {
            self.buffer.clear();
            ParserResult {
                normal_text: String::new(),
                reasoning_text: current_text,
            }
        } else {
            // 非推理块：作为正常文本返回
            self.buffer.clear();
            ParserResult {
                normal_text: current_text,
                reasoning_text: String::new(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! ## 测试过程
    //! 围绕 `GraniteReasoningParser` 的非流式与流式接口，验证：基本/备选短语识别、
    //! 流式部分 token 缓冲、无短语透传、仅起始无结束的截断、空段、空白保留、
    //! 大小写敏感、嵌套/重复短语、多结束短语取首个等。
    //!
    //! ## 意义
    //! 锁定基于自然语言短语的 Granite 推理切分在批式与流式两路下的可观察行为。
    use super::*;

    #[test] // helper
    fn test_basic_reasoning_detection() {
        let mut parser = GraniteReasoningParser::new();
        let text = "Here's my thought process: I need to think about this. Here's my response: The answer is 42.";
        let result = parser.parse_reasoning_streaming_incremental(text, &[]);

        assert_eq!(result.reasoning_text, " I need to think about this. ");
        assert_eq!(result.normal_text, " The answer is 42.");
    }

    #[test] // helper, PARSER.fmt.3
    fn test_alternative_start_token() {
        let mut parser = GraniteReasoningParser::new();
        let text = "Here is my thought process: Different thinking here. Here is my response: Final answer.";
        let result = parser.parse_reasoning_streaming_incremental(text, &[]);

        assert_eq!(result.reasoning_text, " Different thinking here. ");
        assert_eq!(result.normal_text, " Final answer.");
    }

    #[test] // REASONING.stream.3, helper
    fn test_streaming_partial_tokens() {
        let mut parser = GraniteReasoningParser::new();

        // Test partial start token
        let result1 = parser.parse_reasoning_streaming_incremental("Here's", &[]);
        assert_eq!(result1.normal_text, "");
        assert_eq!(result1.reasoning_text, "");

        // 补全起始 token 并添加推理内容
        let result2 = parser
            .parse_reasoning_streaming_incremental(" my thought process: This is reasoning", &[]);
        assert_eq!(result2.reasoning_text, " This is reasoning");
        assert_eq!(result2.normal_text, "");
    }

    #[test] // REASONING.stream.3, helper
    fn test_streaming_partial_end_tokens() {
        let mut parser = GraniteReasoningParser::new();

        // Start reasoning
        parser
            .parse_reasoning_streaming_incremental("Here's my thought process: Thinking... ", &[]);

        parser.parse_reasoning_streaming_incremental("Here", &[]);

        // 部分结束 token 应被缓冲
        let result = parser.parse_reasoning_streaming_incremental("'s my", &[]);
        assert_eq!(result.normal_text, "");
        assert_eq!(result.reasoning_text, "");

        // Complete end token
        let result2 = parser.parse_reasoning_streaming_incremental(" response: Done!", &[]);
        assert_eq!(result2.reasoning_text, "");
        assert_eq!(result2.normal_text, " Done!");
    }

    #[test] // REASONING.batch.3, helper
    fn test_no_reasoning_tokens() {
        let mut parser = GraniteReasoningParser::new();
        let text = "This is just normal text without any special tokens.";
        let result = parser.parse_reasoning_streaming_incremental(text, &[]);

        assert_eq!(result.normal_text, text);
        assert_eq!(result.reasoning_text, "");
    }

    #[test] // REASONING.batch.4, REASONING.batch.5, helper
    fn test_only_start_token_no_end() {
        let mut parser = GraniteReasoningParser::new();

        let result1 = parser.parse_reasoning_streaming_incremental(
            "Here's my thought process: This is reasoning content",
            &[],
        );
        assert_eq!(result1.reasoning_text, " This is reasoning content");
        assert_eq!(result1.normal_text, "");

        // More reasoning content without end token
        let result2 = parser.parse_reasoning_streaming_incremental(" and more thinking", &[]);
        assert_eq!(result2.reasoning_text, " and more thinking");
        assert_eq!(result2.normal_text, "");
    }

    #[test] // REASONING.batch.6, REASONING.batch.1
    fn test_empty_reasoning_block() {
        let mut parser = GraniteReasoningParser::new();
        let text = "Here's my thought process:Here's my response: Direct answer.";
        let result = parser.parse_reasoning_streaming_incremental(text, &[]);

        assert_eq!(result.reasoning_text, "");
        assert_eq!(result.normal_text, " Direct answer.");
    }

    #[test] // REASONING.batch.1, PARSER.fmt.2
    fn test_reasoning_with_whitespace() {
        let mut parser = GraniteReasoningParser::new();
        let text = "Here's my thought process:   \n  Indented reasoning  \n  Here's my response:   Final result  ";
        let result = parser.parse_reasoning_streaming_incremental(text, &[]);

        assert_eq!(result.reasoning_text, "   \n  Indented reasoning  \n  ");
        assert_eq!(result.normal_text, "   Final result  ");
    }

    #[test] // PARSER.fmt.1 — token case sensitivity
    fn test_case_sensitive_tokens() {
        let mut parser = GraniteReasoningParser::new();
        let text = "here's my thought process: lowercase. here's my response: answer.";
        let result = parser.parse_reasoning_streaming_incremental(text, &[]);

        // 不应检测小写 token
        assert_eq!(result.normal_text, text);
        assert_eq!(result.reasoning_text, "");
    }

    #[test] // REASONING.batch.1
    fn test_nested_or_repeated_tokens() {
        let mut parser = GraniteReasoningParser::new();
        let text = "Here's my thought process: I think Here's my thought process: is confusing. Here's my response: Done.";
        let result = parser.parse_reasoning_streaming_incremental(text, &[]);

        assert_eq!(
            result.reasoning_text,
            " I think Here's my thought process: is confusing. "
        );
        assert_eq!(result.normal_text, " Done.");
    }

    #[test] // REASONING.batch.1
    fn test_detect_and_parse_reasoning_basic() {
        let mut parser = GraniteReasoningParser::new();
        let text = "Here's my thought process: I need to analyze this problem. Here's my response: The solution is clear.";
        let result = parser.detect_and_parse_reasoning(text, &[]);

        assert_eq!(result.reasoning_text, "I need to analyze this problem. ");
        assert_eq!(result.normal_text, "The solution is clear.");
    }

    #[test] // REASONING.batch.1, PARSER.fmt.3
    fn test_detect_and_parse_reasoning_alternative_tokens() {
        let mut parser = GraniteReasoningParser::new();
        let text = "Here is my thought process: Different reasoning approach. Here is my response: Final conclusion.";
        let result = parser.detect_and_parse_reasoning(text, &[]);

        assert_eq!(result.reasoning_text, "Different reasoning approach. ");
        assert_eq!(result.normal_text, "Final conclusion.");
    }

    #[test] // REASONING.batch.3
    fn test_detect_and_parse_reasoning_no_tokens() {
        let mut parser = GraniteReasoningParser::new();
        let text = "This is just normal text without special markers.";
        let result = parser.detect_and_parse_reasoning(text, &[]);

        assert_eq!(result.normal_text, text);
        assert_eq!(result.reasoning_text, "");
    }

    #[test] // REASONING.batch.4, REASONING.batch.5, REASONING.batch.1
    fn test_detect_and_parse_reasoning_only_start_token() {
        let mut parser = GraniteReasoningParser::new();
        let text = "Here's my thought process: This reasoning has no end marker.";
        let result = parser.detect_and_parse_reasoning(text, &[]);

        assert_eq!(result.reasoning_text, "This reasoning has no end marker.");
        assert_eq!(result.normal_text, "");
    }

    #[test] // REASONING.batch.6, REASONING.batch.1
    fn test_detect_and_parse_reasoning_empty_sections() {
        let mut parser = GraniteReasoningParser::new();
        let text = "Here's my thought process:Here's my response:";
        let result = parser.detect_and_parse_reasoning(text, &[]);

        assert_eq!(result.reasoning_text, "");
        assert_eq!(result.normal_text, "");
    }

    #[test] // REASONING.batch.1, PARSER.fmt.2
    fn test_detect_and_parse_reasoning_whitespace_handling() {
        let mut parser = GraniteReasoningParser::new();
        let text = "Here's my thought process:   \n\tSpaced reasoning\n   Here's my response:  \n  Spaced response\n";
        let result = parser.detect_and_parse_reasoning(text, &[]);

        assert_eq!(result.reasoning_text, "Spaced reasoning\n   ");
        assert_eq!(result.normal_text, "Spaced response");
    }

    #[test] // REASONING.batch.1, PARSER.fmt.3
    fn test_detect_and_parse_reasoning_multiple_end_tokens() {
        let mut parser = GraniteReasoningParser::new();
        let text = "Here's my thought process: Thinking about Here's my response: in the middle. Here's my response: Real end.";
        let result = parser.detect_and_parse_reasoning(text, &[]);

        assert_eq!(result.reasoning_text, "Thinking about ");
        assert_eq!(
            result.normal_text,
            "in the middle. Here's my response: Real end."
        );
    }

    #[test] // PARSER.fmt.1
    fn test_detect_and_parse_reasoning_case_sensitivity() {
        let mut parser = GraniteReasoningParser::new();
        let text =
            "here's my thought process: lowercase tokens. here's my response: should not work.";
        let result = parser.detect_and_parse_reasoning(text, &[]);

        assert_eq!(result.normal_text, text);
        assert_eq!(result.reasoning_text, "");
    }

    #[test] // REASONING.batch.1, PARSER.fmt.3
    fn test_detect_and_parse_reasoning_mixed_tokens() {
        let mut parser = GraniteReasoningParser::new();
        let text = "Here's my thought process: First reasoning. Here is my response: Mixed token response.";
        let result = parser.detect_and_parse_reasoning(text, &[]);

        assert_eq!(result.reasoning_text, "First reasoning. ");
        assert_eq!(result.normal_text, "Mixed token response.");
    }

    #[test] // REASONING.batch.1
    fn test_detect_and_parse_reasoning_long_content() {
        let mut parser = GraniteReasoningParser::new();
        let text = "Here's my thought process: This is a very long reasoning section that spans multiple sentences. I need to consider various factors. The analysis requires careful thought. Here's my response: After all that thinking, here is the comprehensive answer with multiple parts and detailed explanation.";
        let result = parser.detect_and_parse_reasoning(text, &[]);

        assert_eq!(
            result.reasoning_text,
            "This is a very long reasoning section that spans multiple sentences. I need to consider various factors. The analysis requires careful thought. "
        );
        assert_eq!(
            result.normal_text,
            "After all that thinking, here is the comprehensive answer with multiple parts and detailed explanation."
        );
    }
}
