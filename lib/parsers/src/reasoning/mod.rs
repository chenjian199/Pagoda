// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-License-Identifier: Apache-2.0

//! ## 设计意图
//! 推理解析子模块：把模型输出按推理块/普通文本切分。本文件汇聚各具体解析器、维护
//! 「解析器名 → 类型」注册表，并提供统一的 `ReasoningParser` trait 与包装器。
//!
//! ## 外部契约
//! - 公开类型：`ParserResult`、`ReasoningParser`、`ReasoningParserType`、`ReasoningParserWrapper`，
//!   以及各具体解析器的重导出。
//! - 公开函数：`get_available_reasoning_parsers()`；`ReasoningParserType::get_reasoning_parser`、
//!   `::get_reasoning_parser_from_name`。
//! - 注册表键集合、回退到 `Basic` 的行为、各类型映射的 `BasicReasoningParser` 配置均须保持不变。

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
    // DeepSeek-V4 与 Qwen 使用相同的 `<think>` / `</think>` 分隔符（已对照
    // deepseek-ai/DeepSeek-V4-Pro 的 encoding_dsv4.py 确认），故今天委托给同一
    // `BasicReasoningParser` 配置。仍通过专用 `DeepSeekV4` 变体路由而非硬别名到 `Qwen`，
    // 使未来分歧（不同特殊 token、max-thinking 模式等）有落点而不波及 Qwen 自身配置。
    //
    // 三个名称别名的存在是因为调用方通过 `--dyn-reasoning-parser` / `--reasoning-parser`
    // 传入 HF 模型 / vLLM 配方 / chat-template 作者所选的任意字符串。我们接受全部三种分隔
    // 约定（snake / kebab / concat），而非强制用户使用单一规范形式。
    //
    // Gemma 4 thinking 模型：推理被 `<|channel>...<channel|>` 包裹，带 `thought\n` 角色标签
    // 由解析器剥除。与 `--dyn-tool-call-parser gemma4` 搭配以实现端到端 Gemma 4 支持。
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
    /// DeepSeek-V4-Pro / V4-Flash。当前与 Qwen 使用相同的 `<think>` / `</think>`
    /// `BasicReasoningParser` 配置（V4 从不在补全中追加 `<think>`——chat template 总是预注入它，
    /// 故解析器经 `set_in_reasoning(true)` 而非 `force_reasoning` 启动）。专用变体使未来 V4 特定
    /// 分歧（不同分隔符、thinking-effort 模式）不致泄漏进 Qwen 的行为。
    DeepSeekV4,
    NemotronDeci,
    Kimi,
    KimiK25,
    Mistral,
    Granite,
    MiniMaxAppendThink,
    /// Google Gemma 4 thinking 模型。自定义 `<|channel>...<channel|>` 分隔符，
    /// 带由解析器剥除的 `thought\n` 角色标签前缀。
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
        // `<think>` / `</think>` 的两种常用配置。
        let basic = || BasicReasoningParser::new("<think>".into(), "</think>".into(), false, true);
        let force_basic =
            || BasicReasoningParser::new("<think>".into(), "</think>".into(), true, true);

        match self {
            ReasoningParserType::DeepseekR1 => wrap(force_basic()),
            ReasoningParserType::Step3 => wrap(force_basic()),
            ReasoningParserType::Basic => wrap(basic()),
            ReasoningParserType::Qwen => wrap(basic()),
            // 今天与 Qwen 同为 `<think>` / `</think>` 配置；保留独立变体以便 V4 特定分歧落地。
            // 理由见 `ReasoningParserType::DeepSeekV4` 文档注释。
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
    //! 验证注册表键集合完整、各别名路由到正确解析器、未知名回退到 `Basic`，以及若干代表性
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
