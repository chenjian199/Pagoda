// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-License-Identifier: Apache-2.0
//
// API-compatible implementation based on the public interfaces and behavioral
// contracts of NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
// Implementation rewritten by PAGODA.

//! ## 设计意图
//! 推理解析子模块：把模型输出按推理块/普通文本切分。本文件汇聚各具体解析器、维护
//!
//! ## 外部契约
//!   以及各具体解析器的重导出。

use std::collections::HashMap;
use std::sync::OnceLock;

mod base_parser;
mod gemma4_parser;
mod gpt_oss_parser;
mod granite_parser;
mod minimax_append_think_parser;

pub use base_parser::BasicReasoningParser;
pub use gemma4_parser::Gemma4ReasoningParser;
pub use gpt_oss_parser::GptOssReasoningParser;
pub use granite_parser::GraniteReasoningParser;
pub use minimax_append_think_parser::MiniMaxAppendThinkParser;

pub(crate) const KIMI_K2_TOOL_SECTION_BEGIN: &str = "<|tool_calls_section_begin|>";

static REASONING_PARSER_MAP: OnceLock<HashMap<&'static str, ReasoningParserType>> = OnceLock::new();

/// 初始化全局推理解析器映射
fn get_reasoning_parser_map() -> &'static HashMap<&'static str, ReasoningParserType> {
    //
    //
    REASONING_PARSER_MAP.get_or_init(|| {
        use ReasoningParserType::*;
        [
            ("deepseek_r1", DeepseekR1),
            ("basic", Basic),
            ("gpt_oss", GptOss),
            ("qwen3", Qwen),
            ("deepseek_v4", DeepSeekV4),
            ("deepseek-v4", DeepSeekV4),
            ("deepseekv4", DeepSeekV4),
            ("nemotron_deci", NemotronDeci),
            ("kimi", Kimi),
            ("kimi_k25", KimiK25),
            ("step3", Step3),
            ("mistral", Mistral),
            ("granite", Granite),
            ("nemotron_nano", DeepseekR1), // nemotron nano is ...</think>
            ("nemotron3", DeepseekR1),
            ("nemotron_v3", DeepseekR1),
            ("glm45", NemotronDeci), // GLM-4.5/5 is <think>...</think>, no force_reasoning
            ("minimax_append_think", MiniMaxAppendThink),
            ("gemma4", Gemma4),
            ("gemma-4", Gemma4),
        ]
        .into_iter()
        .collect()
    })
}

pub fn get_available_reasoning_parsers() -> Vec<&'static str> {
    get_reasoning_parser_map().keys().copied().collect()
}

#[derive(Debug, Clone, Default)]
pub struct ParserResult {
    pub normal_text: String,

    pub reasoning_text: String,
}

impl ParserResult {
    pub fn get_some_reasoning(&self) -> Option<String> {
        (!self.reasoning_text.is_empty()).then(|| self.reasoning_text.clone())
    }

    pub fn get_some_normal_text(&self) -> Option<String> {
        (!self.normal_text.is_empty()).then(|| self.normal_text.clone())
    }
}

pub trait ReasoningParser: Send + std::fmt::Debug {
    fn detect_and_parse_reasoning(&mut self, text: &str, token_ids: &[u32]) -> ParserResult;

    fn parse_reasoning_streaming_incremental(
        &mut self,
        text: &str,
        token_ids: &[u32],
    ) -> ParserResult;

    fn set_in_reasoning(&mut self, _in_reasoning: bool) {
        // 默认空操作：用于不支持按请求覆盖的解析器。
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReasoningParserType {
    DeepseekR1,
    Step3,
    Basic,
    GptOss,
    Qwen,
    DeepSeekV4,
    NemotronDeci,
    Kimi,
    KimiK25,
    Mistral,
    Granite,
    MiniMaxAppendThink,
    Gemma4,
}

#[derive(std::fmt::Debug)]
pub struct ReasoningParserWrapper {
    parser: Box<dyn ReasoningParser>,
}

impl ReasoningParser for ReasoningParserWrapper {
    fn detect_and_parse_reasoning(&mut self, text: &str, token_ids: &[u32]) -> ParserResult {
        self.parser.detect_and_parse_reasoning(text, token_ids)
    }

    fn parse_reasoning_streaming_incremental(
        &mut self,
        text: &str,
        token_ids: &[u32],
    ) -> ParserResult {
        self.parser
            .parse_reasoning_streaming_incremental(text, token_ids)
    }

    fn set_in_reasoning(&mut self, in_reasoning: bool) {
        self.parser.set_in_reasoning(in_reasoning)
    }
}

impl ReasoningParserType {
    pub fn get_reasoning_parser(self) -> ReasoningParserWrapper {
        fn wrap(parser: impl ReasoningParser + 'static) -> ReasoningParserWrapper {
            ReasoningParserWrapper {
                parser: Box::new(parser),
            }
        }
        let basic = || BasicReasoningParser::new("<think>".into(), "</think>".into(), false, true);
        let force_basic =
            || BasicReasoningParser::new("<think>".into(), "</think>".into(), true, true);

        match self {
            ReasoningParserType::DeepseekR1 => wrap(force_basic()),
            ReasoningParserType::Step3 => wrap(force_basic()),
            ReasoningParserType::Basic => wrap(basic()),
            ReasoningParserType::Qwen => wrap(basic()),
            ReasoningParserType::DeepSeekV4 => wrap(basic()),
            ReasoningParserType::NemotronDeci => wrap(basic()),
            ReasoningParserType::Kimi => wrap(BasicReasoningParser::new(
                "◁think▷".into(),
                "◁/think▷".into(),
                false,
                true,
            )),
            ReasoningParserType::KimiK25 => wrap(
                BasicReasoningParser::new("<think>".into(), "</think>".into(), true, true)
                    .with_tool_start_token(KIMI_K2_TOOL_SECTION_BEGIN),
            ),
            ReasoningParserType::Mistral => wrap(BasicReasoningParser::new(
                "[THINK]".into(),
                "[/THINK]".into(),
                true,
                true,
            )),
            ReasoningParserType::GptOss => match GptOssReasoningParser::new() {
                Ok(parser) => wrap(parser),
                Err(e) => {
                    tracing::warn!(
                        "GptOssReasoningParser could not be initialized, falling back to Basic Reasoning Parser: {e}"
                    );
                    wrap(BasicReasoningParser::new(
                        "<think>".into(),
                        "</think>".into(),
                        false,
                        true,
                    ))
                }
            },
            ReasoningParserType::Granite => wrap(GraniteReasoningParser::new()),
            ReasoningParserType::MiniMaxAppendThink => wrap(MiniMaxAppendThinkParser::new()),
            ReasoningParserType::Gemma4 => wrap(Gemma4ReasoningParser::new()),
        }
    }

    pub fn get_reasoning_parser_from_name(name: &str) -> ReasoningParserWrapper {
        tracing::debug!("Selected reasoning parser: {}", name);

        let parser_map = get_reasoning_parser_map();
        let normalized_name = name.to_lowercase();

        match parser_map.get(normalized_name.as_str()) {
            Some(parser_type) => parser_type.get_reasoning_parser(),
            None => {
                tracing::warn!(
                    parser_name = name,
                    "Unknown reasoning parser type, falling back to Basic Reasoning Parser",
                );
                Self::Basic.get_reasoning_parser()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! ## 测试过程
    //! 解析器的批式/流式可观察行为。
    //!
    //! ## 意义
    //! 锁定推理解析器选择与分发的外部契约，防止新增/改名解析器时回归。
    use super::*;

    #[test] // registry helper
    fn test_get_available_reasoning_parsers() {
        let parsers = get_available_reasoning_parsers();
        assert!(!parsers.is_empty());
        let available_parsers = [
            "deepseek_r1",
            "basic",
            "gpt_oss",
            "qwen3",
            "deepseek_v4",
            "deepseek-v4",
            "deepseekv4",
            "nemotron_deci",
            "kimi",
            "kimi_k25",
            "step3",
            "mistral",
            "granite",
            "nemotron_nano",
            "nemotron3",
            "nemotron_v3",
            "glm45",
            "minimax_append_think",
            "gemma4",
            "gemma-4",
        ];
        for parser in available_parsers {
            assert!(parsers.contains(&parser));
        }
    }

    #[test] // REASONING.batch.1
    fn test_deepseek_v4_detect_and_parse() {
        for parser_name in ["deepseek_v4", "deepseek-v4", "deepseekv4"] {
            let mut parser = ReasoningParserType::get_reasoning_parser_from_name(parser_name);
            let result = parser.detect_and_parse_reasoning("<think>thinking</think>answer", &[]);
            assert_eq!(result.reasoning_text, "thinking");
            assert_eq!(result.normal_text, "answer");
        }
    }

    #[test] // REASONING.batch.3, REASONING.batch.1
    fn test_deepseek_v4_no_forced_reasoning_without_tags() {
        let mut parser = ReasoningParserType::get_reasoning_parser_from_name("deepseek_v4");
        let result = parser.detect_and_parse_reasoning("answer only", &[]);
        assert_eq!(result.reasoning_text, "");
        assert_eq!(result.normal_text, "answer only");
    }

    #[test] // REASONING.stream.3, REASONING.batch.1
    fn test_deepseek_v4_streaming() {
        let mut parser = ReasoningParserType::get_reasoning_parser_from_name("deepseek_v4");

        let chunks = ["<think>rea", "son</think>answer"];
        let mut reasoning = String::new();
        let mut normal = String::new();

        for chunk in chunks {
            let result = parser.parse_reasoning_streaming_incremental(chunk, &[]);
            reasoning.push_str(&result.reasoning_text);
            normal.push_str(&result.normal_text);
        }

        assert_eq!(reasoning, "reason");
        assert_eq!(normal, "answer");
    }

    #[test] // REASONING.batch.1
    fn test_kimi_k25_detect_and_parse() {
        let cases = [
            (
                "force reasoning: no think tags",
                "no think tags here",
                "no think tags here",
                "",
            ),
            (
                "standard think tags",
                "<think>Let me reason about this.</think>Hello!",
                "Let me reason about this.",
                "Hello!",
            ),
            (
                "empty think block (instant mode)",
                "<think></think>Hello from instant mode!",
                "",
                "Hello from instant mode!",
            ),
            (
                "empty think block with newline",
                "<think>\n</think>Hello from instant mode!",
                "",
                "Hello from instant mode!",
            ),
        ];

        for (desc, input, expected_reasoning, expected_normal) in cases {
            let mut parser = ReasoningParserType::KimiK25.get_reasoning_parser();
            let result = parser.detect_and_parse_reasoning(input, &[]);
            assert_eq!(
                result.reasoning_text, expected_reasoning,
                "FAILED reasoning: {desc}"
            );
            assert_eq!(result.normal_text, expected_normal, "FAILED normal: {desc}");
        }
    }

    #[test] // REASONING.stream.3, REASONING.batch.1
    fn test_kimi_k25_streaming_force_reasoning() {
        let mut parser = ReasoningParserType::KimiK25.get_reasoning_parser();

        let r1 = parser.parse_reasoning_streaming_incremental("<thi", &[]);
        assert_eq!(r1.reasoning_text, "");
        assert_eq!(r1.normal_text, "");

        let r2 = parser.parse_reasoning_streaming_incremental("nk>reasoning here", &[]);
        assert_eq!(r2.reasoning_text, "reasoning here");
        assert_eq!(r2.normal_text, "");

        let r3 = parser.parse_reasoning_streaming_incremental("</think>Hello!", &[]);
        assert_eq!(r3.reasoning_text, "");
        assert_eq!(r3.normal_text, "Hello!");
    }

    #[test] // REASONING.stream.3, REASONING.batch.1
    fn test_kimi_k25_streaming() {
        let cases: Vec<(&str, &[&str], &str, &str)> = vec![
            (
                "complete response",
                &[
                    "<think>",
                    "I need to",
                    " think about",
                    " this carefully.",
                    "</think>",
                    "Bonjour",
                    "!",
                ],
                "I need to think about this carefully.",
                "Bonjour!",
            ),
            (
                "empty think (instant mode)",
                &["<think>", "</think>", "Direct answer."],
                "",
                "Direct answer.",
            ),
        ];

        for (desc, tokens, expected_reasoning, expected_content) in cases {
            let mut parser = ReasoningParserType::KimiK25.get_reasoning_parser();
            let mut all_reasoning = String::new();
            let mut all_content = String::new();
            for token in tokens {
                let r = parser.parse_reasoning_streaming_incremental(token, &[]);
                all_reasoning.push_str(&r.reasoning_text);
                all_content.push_str(&r.normal_text);
            }
            assert_eq!(
                all_reasoning, expected_reasoning,
                "FAILED reasoning: {desc}"
            );
            assert_eq!(all_content, expected_content, "FAILED content: {desc}");
        }
    }

    #[test] // registry lookup
    fn test_kimi_k25_parser_lookup_by_name() {
        let mut parser = ReasoningParserType::get_reasoning_parser_from_name("kimi_k25");
        let result = parser.detect_and_parse_reasoning("<think>thinking</think>answer", &[]);
        assert_eq!(result.reasoning_text, "thinking");
        assert_eq!(result.normal_text, "answer");
    }

    #[test] // PARSER.fmt.3 — token-spelling differences across model variants
    fn test_kimi_vs_kimi_k25_different_tags() {
        let mut kimi = ReasoningParserType::Kimi.get_reasoning_parser();
        let mut kimi_k25 = ReasoningParserType::KimiK25.get_reasoning_parser();

        let r_kimi = kimi.detect_and_parse_reasoning("<think>reasoning</think>answer", &[]);
        assert_eq!(r_kimi.normal_text, "<think>reasoning</think>answer");
        assert_eq!(r_kimi.reasoning_text, "");

        let r_k25 = kimi_k25.detect_and_parse_reasoning("<think>reasoning</think>answer", &[]);
        assert_eq!(r_k25.reasoning_text, "reasoning");
        assert_eq!(r_k25.normal_text, "answer");
    }

    #[test] // REASONING.stream.3, REASONING.batch.1
    fn test_nemotron_streaming_with_set_in_reasoning() {
        let mut parser = ReasoningParserType::DeepseekR1.get_reasoning_parser();
        parser.set_in_reasoning(true); // OpenAI path calls this

        let tokens = &["Think", "ing about", " this", ".\n\n", "</think>", "Four"];

        let mut all_reasoning = String::new();
        let mut all_content = String::new();
        for token in tokens {
            let r = parser.parse_reasoning_streaming_incremental(token, &[]);
            all_reasoning.push_str(&r.reasoning_text);
            all_content.push_str(&r.normal_text);
        }
        assert_eq!(all_reasoning, "Thinking about this.\n\n");
        assert_eq!(all_content, "Four");
    }

    #[test] // REASONING.stream.3, REASONING.batch.1
    fn test_nemotron_streaming_force_reasoning_without_set_in_reasoning() {
        let mut parser = ReasoningParserType::DeepseekR1.get_reasoning_parser();

        let tokens = &["Think", "ing about", " this", ".\n\n", "</think>", "Four"];

        let mut all_reasoning = String::new();
        let mut all_content = String::new();
        for token in tokens {
            let r = parser.parse_reasoning_streaming_incremental(token, &[]);
            all_reasoning.push_str(&r.reasoning_text);
            all_content.push_str(&r.normal_text);
        }
        assert_eq!(all_reasoning, "Thinking about this.\n\n");
        assert_eq!(all_content, "Four");
    }

    #[test] // REASONING.stream.3, helper
    fn test_nemotron_streaming_split_end_think_tokens() {
        let mut parser = ReasoningParserType::DeepseekR1.get_reasoning_parser();
        parser.set_in_reasoning(true);

        let tokens = &[
            "reason", "ing", " done", ".", "</", "think", ">", "Hello", " world",
        ];

        let mut all_reasoning = String::new();
        let mut all_content = String::new();
        for token in tokens {
            let r = parser.parse_reasoning_streaming_incremental(token, &[]);
            all_reasoning.push_str(&r.reasoning_text);
            all_content.push_str(&r.normal_text);
        }
        assert_eq!(all_reasoning, "reasoning done.");
        assert_eq!(all_content, "Hello world");
    }

    #[test] // CASE.10 — vLLM nemotron_v3 parity
    fn test_nemotron_v3_detect_and_parse_vllm_cases() {
        let cases = [
            (
                "without start token",
                "This is a reasoning section</think>This is the rest",
                "This is a reasoning section",
                "This is the rest",
            ),
            (
                "with start token",
                "<think>This is a reasoning section</think>This is the rest",
                "This is a reasoning section",
                "This is the rest",
            ),
        ];

        for (desc, input, expected_reasoning, expected_content) in cases {
            let mut parser = ReasoningParserType::get_reasoning_parser_from_name("nemotron_v3");
            let result = parser.detect_and_parse_reasoning(input, &[]);
            assert_eq!(
                result.reasoning_text, expected_reasoning,
                "FAILED reasoning: {desc}"
            );
            assert_eq!(
                result.normal_text, expected_content,
                "FAILED content: {desc}"
            );
        }
    }

    #[test] // CASE.8, CASE.10 — vLLM nemotron_v3 parity
    fn test_nemotron_v3_streaming_vllm_cases() {
        let cases: Vec<(&str, &[&str], &str, &str)> = vec![
            (
                "without start token",
                &[
                    "This is a reasoning section",
                    "</think>",
                    "This is the rest",
                ],
                "This is a reasoning section",
                "This is the rest",
            ),
            (
                "with start token",
                &[
                    "<think>",
                    "This is a reasoning section",
                    "</think>",
                    "This is the rest",
                ],
                "This is a reasoning section",
                "This is the rest",
            ),
        ];

        for (desc, tokens, expected_reasoning, expected_content) in cases {
            let mut parser = ReasoningParserType::get_reasoning_parser_from_name("nemotron_v3");
            let mut all_reasoning = String::new();
            let mut all_content = String::new();
            for token in tokens {
                let result = parser.parse_reasoning_streaming_incremental(token, &[]);
                all_reasoning.push_str(&result.reasoning_text);
                all_content.push_str(&result.normal_text);
            }
            assert_eq!(
                all_reasoning, expected_reasoning,
                "FAILED reasoning: {desc}"
            );
            assert_eq!(all_content, expected_content, "FAILED content: {desc}");
        }
    }

    #[test]
    fn test_deepseek_v4_streaming_with_set_in_reasoning() {
        let mut parser = ReasoningParserType::get_reasoning_parser_from_name("deepseek_v4");
        parser.set_in_reasoning(true);

        let tokens = &[
            "Wei", "gh", "ing ", "options", ".", "</think>", "Bei", "jing", " is", " sunny.",
        ];

        let mut all_reasoning = String::new();
        let mut all_content = String::new();
        for token in tokens {
            let r = parser.parse_reasoning_streaming_incremental(token, &[]);
            all_reasoning.push_str(&r.reasoning_text);
            all_content.push_str(&r.normal_text);
        }
        assert_eq!(all_reasoning, "Weighing options.");
        assert_eq!(all_content, "Beijing is sunny.");
    }
}
