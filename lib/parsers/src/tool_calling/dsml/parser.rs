// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # tool_calling::dsml::parser
//!
//! ## 设计意图
//! 解析 DeepSeek V3.2 / V4 的 DSML（DeepSeek Markup Language）工具调用格式。
//! V3.2 用 `<｜DSML｜function_calls>` 包裹，V4 用 `<｜DSML｜tool_calls>` 包裹，
//! 内层 invoke / parameter 文法一致：
//!
//! ```text
//! <｜DSML｜function_calls>
//! <｜DSML｜invoke name="function_name">
//! <｜DSML｜parameter name="param_name" string="true|false">value</｜DSML｜parameter>
//! ...
//! </｜DSML｜invoke>
//! </｜DSML｜function_calls>
//! ```
//! 参考实现：
//! - V3.2: https://huggingface.co/deepseek-ai/DeepSeek-V3.2/tree/main/encoding/encoding_dsv32.py
//! - V4:   https://huggingface.co/deepseek-ai/DeepSeek-V4-Pro/tree/main/encoding/encoding_dsv4.py
//!
//! ## 外部契约
//! - `detect_tool_call_start_dsml(chunk, config)`、`find_tool_call_end_position_dsml(chunk, config)`、
//!   `try_tool_call_parse_dsml(message, config)`，签名与返回形态不变。
//! - 调用 id 形如 `call_` + 24 位小写十六进制（OpenAI 风格）。
//!
//! ## 实现要点
//! - block / invoke / parameter 三层均用正则匹配；外层结束 token 缺失时无匹配（沿用既有保守契约）。
//! - parameter 的 `string="true|false"` 可缺省，缺省时尽力 JSON 解析、失败退化为字符串。

use regex::Regex;
use uuid::Uuid;

use super::super::config::DsmlParserConfig;
use super::super::response::{CalledFunction, ToolCallResponse, ToolCallType};

// === SECTION: 流式探测与边界定位 ===

/// 判断 chunk 是否包含（或部分包含，用于流式）DSML 工具调用起始。
pub fn detect_tool_call_start_dsml(chunk: &str, config: &DsmlParserConfig) -> bool {
    let start_token = config.block_start.as_str();

    if chunk.contains(start_token) {
        return true;
    }

    // 按字符（非字节）枚举前缀，兼容多字节标记
    let start_chars: Vec<char> = start_token.chars().collect();
    (1..start_chars.len()).any(|i| {
        let partial: String = start_chars[..i].iter().collect();
        chunk.ends_with(&partial)
    })
}

/// 返回 DSML 区块结束 token 之后的位置；缺失则返回 chunk 长度。
pub fn find_tool_call_end_position_dsml(chunk: &str, config: &DsmlParserConfig) -> usize {
    let end_token = config.block_end.as_str();

    match chunk.find(end_token) {
        Some(pos) => pos + end_token.len(),
        None => chunk.len(),
    }
}

// === SECTION: 区块正则与顶层解析 ===

/// 构造匹配完整 DSML tool_calls / function_calls 区块的正则。
/// 由 `extract_tool_calls_with_regex` 与 `try_tool_call_parse_dsml` 共用，确保两者识别一致。
fn build_block_regex(config: &DsmlParserConfig) -> anyhow::Result<Regex> {
    // 匹配 <｜DSML｜function_calls> ... </｜DSML｜function_calls>
    // (?s) 使 . 匹配换行；\s*(.*?)\s* 非贪婪捕获首尾标记之间的内容
    let block_pattern = format!(
        r"(?s){}\s*(.*?)\s*{}",
        regex::escape(&config.block_start),
        regex::escape(&config.block_end)
    );
    Ok(Regex::new(&block_pattern)?)
}

/// 解析消息中的 DSML 工具调用，返回 `(parsed_tool_calls, normal_text_content)`。
pub fn try_tool_call_parse_dsml(
    message: &str,
    config: &DsmlParserConfig,
) -> anyhow::Result<(Vec<ToolCallResponse>, Option<String>)> {
    let trimmed = message.trim();

    // 空内容直接返回
    if trimmed.is_empty() {
        return Ok((vec![], Some(String::new())));
    }

    // 无区块起始：整体作为普通文本
    let Some(start_idx) = trimmed.find(&config.block_start) else {
        return Ok((vec![], Some(trimmed.to_string())));
    };

    // 抽取区块内的工具调用
    let block_regex = build_block_regex(config)?;
    let tool_calls = extract_tool_calls_with_regex(trimmed, &block_regex, config)?;

    if tool_calls.is_empty() {
        // 检测到区块起始但未解析出有效 invoke。不把 DSML 标记泄回客户端：
        // 仅返回区块前文本，并打印失败块前缀的诊断。
        //
        // 注意：此处未闭合的区块起始会使 block_regex 完全无匹配，因此其*之后*的
        // 任何有效区块都会丢失。此为既有的保守 P1-3 契约。
        let failed = &trimmed[start_idx..];
        let prefix: String = failed.chars().take(120).collect();
        tracing::warn!(
            "DSML tool_calls block parsed no invokes; suppressing markup. prefix={:?}",
            prefix
        );
        return Ok((vec![], Some(trimmed[..start_idx].to_string())));
    }

    // 保留区块之间与之后的文本：从输入中剥除每个完整区块跨度，
    // 而非切到首个起始 token，否则会丢失多区块之间/之后的文本。
    let normal_text = block_regex.replace_all(trimmed, "").to_string();

    Ok((tool_calls, Some(normal_text)))
}

/// 抽取 `block_regex` 在 DSML 文本中匹配到的全部工具调用。
fn extract_tool_calls_with_regex(
    text: &str,
    block_regex: &Regex,
    config: &DsmlParserConfig,
) -> anyhow::Result<Vec<ToolCallResponse>> {
    let mut tool_calls = Vec::new();

    for block_match in block_regex.captures_iter(text) {
        if let Some(block_content) = block_match.get(1) {
            // 从该区块抽取各个 invoke
            tool_calls.extend(extract_invokes(block_content.as_str(), config)?);
        }
    }

    Ok(tool_calls)
}

/// 从 function_calls 内容中抽取各个 invoke 块。
fn extract_invokes(block: &str, config: &DsmlParserConfig) -> anyhow::Result<Vec<ToolCallResponse>> {
    let mut invokes = Vec::new();

    // 匹配 <｜DSML｜invoke name="function_name">..content..</｜DSML｜invoke>
    // 注意：invoke_start_prefix 为 "<｜DSML｜invoke name="（不含引号，引号在模式中补上）
    let invoke_pattern = format!(
        r#"(?s){}\"([^"]+)\"\s*>(.*?){}"#,
        regex::escape(&config.invoke_start_prefix),
        regex::escape(&config.invoke_end)
    );
    let invoke_regex = Regex::new(&invoke_pattern)?;

    for invoke_match in invoke_regex.captures_iter(block) {
        let (Some(name_match), Some(content_match)) = (invoke_match.get(1), invoke_match.get(2))
        else {
            continue;
        };
        let function_name = name_match.as_str().trim().to_string();

        // 解析 invoke 内容中的参数
        let parameters = parse_parameters(content_match.as_str(), config)?;

        // OpenAI 风格 id："call_" + 24 位小写十六进制。
        // 取 v4 UUID 的简单形式（32 位十六进制、无连字符）并截断。
        let uuid_simple = Uuid::new_v4().simple().to_string();
        let id = format!("call_{}", &uuid_simple[..24]);

        invokes.push(ToolCallResponse {
            id,
            tp: ToolCallType::Function,
            function: CalledFunction {
                name: function_name,
                arguments: serde_json::to_string(&parameters)?,
            },
        });
    }

    Ok(invokes)
}

/// 从 invoke 内容中解析参数。
fn parse_parameters(
    content: &str,
    config: &DsmlParserConfig,
) -> anyhow::Result<serde_json::Map<String, serde_json::Value>> {
    let mut parameters = serde_json::Map::new();

    // 匹配 <｜DSML｜parameter name="param_name" string="true|false">value</｜DSML｜parameter>
    // 注意：parameter_prefix 为 "<｜DSML｜parameter name="（不含引号）。
    // `string="true|false"` 属性可选——部分模型输出省略；省略时尽力解析（JSON → 字符串退化）。
    let param_pattern = format!(
        r#"(?s){}\"([^"]+)\"(?:\s+string=\"(true|false)\")?\s*>(.*?){}"#,
        regex::escape(&config.parameter_prefix),
        regex::escape(&config.parameter_end)
    );
    let param_regex = Regex::new(&param_pattern)?;

    for param_match in param_regex.captures_iter(content) {
        let (Some(name_match), Some(value_match)) = (param_match.get(1), param_match.get(3)) else {
            continue;
        };
        let param_name = name_match.as_str().trim();
        let param_value = value_match.as_str().trim();

        // 依据 string 属性（若存在）解析值：
        // `string="true"` 强制走 String 分支；其余情形（`string="false"` 或省略）
        // 先尝试 JSON，失败退化为 String。
        let value = if param_match.get(2).map(|m| m.as_str()) == Some("true") {
            serde_json::Value::String(param_value.to_string())
        } else {
            serde_json::from_str(param_value)
                .unwrap_or_else(|_| serde_json::Value::String(param_value.to_string()))
        };

        parameters.insert(param_name.to_string(), value);
    }

    Ok(parameters)
}

#[cfg(test)]
mod tests {
    //! ## 测试过程
    //! 围绕 DSML 公开 API（`detect_tool_call_start_dsml`、`find_tool_call_end_position_dsml`、
    //! `try_tool_call_parse_dsml`）覆盖 V3.2 与 V4 两种区块名下的：起始/边界探测、单/多调用、
    //! 参数类型（string 属性与 JSON 退化）、推理文本混合、截断行为（按既有契约钉死）等。
    //!
    //! ## 意义
    //! 锁定 DSML 引擎在两套 token、多区块文本保留与截断边界下的可观察行为。
    use super::*;

    fn extract_name_and_args(call: ToolCallResponse) -> (String, serde_json::Value) {
        let args: serde_json::Value = serde_json::from_str(&call.function.arguments).unwrap();
        (call.function.name, args)
    }

    fn get_test_config() -> DsmlParserConfig {
        DsmlParserConfig::default()
    }

    fn get_v4_test_config() -> DsmlParserConfig {
        DsmlParserConfig {
            block_start: "<｜DSML｜tool_calls>".to_string(),
            block_end: "</｜DSML｜tool_calls>".to_string(),
            ..Default::default()
        }
    }

    #[test] // helper
    fn test_detect_tool_call_start() {
        let config = get_test_config();
        assert!(detect_tool_call_start_dsml(
            "<｜DSML｜function_calls>",
            &config
        ));
        assert!(detect_tool_call_start_dsml(
            "text <｜DSML｜function_calls>",
            &config
        ));
        assert!(detect_tool_call_start_dsml("<｜DSML｜function_c", &config)); // 部分匹配
        assert!(!detect_tool_call_start_dsml("no tool call here", &config));
    }

    // -------------------------------------------------------------------
    // DeepSeek V4 覆盖清单（见 lib/parsers/PARSER_CASES.md 的 PARSER.* 分类）。
    //
    // 由下方的 V4 测试（或共享的 DSML 通用测试）覆盖：
    //   - PARSER.batch.1   single-call            (parsers.rs :: test_deepseek_v4_single_tool_call)
    //   - PARSER.batch.2   multi-calls            (test_parse_deepseek_v4_multiple_tool_calls)
    //   - PARSER.batch.3   no-call                (shared: test_parse_no_tool_calls)
    //   - PARSER.batch.4   malformed-args         (test_parse_deepseek_v4_malformed_json_value_falls_back_to_string,
    //                                      test_parse_deepseek_v4_missing_invoke_close_drops_call)
    //   - PARSER.batch.5   missing-end-token      (test_parse_deepseek_v4_missing_end_token{,_multiple_calls})
    //                                      — 标记为已知缺陷：解析器丢弃调用。参见下方 TODO。
    //   - PARSER.batch.6   empty-args             (test_parse_deepseek_v4_no_parameters)
    //   - PARSER.batch.7   complex-args           (shared: test_parse_mixed_types_realistic, test_parse_nested_object_parameter,
    //                                      lib/llm/tests/test_streaming_tool_parsers :: ..._mixed_param_types_vllm,
    //                                      ..._special_chars_vllm)
    //   - PARSER.stream.3   streaming              (test_detect_tool_call_start_v4, test_find_tool_call_end_position_v4,
    //                                      test_streaming_chunk_boundary_split_v4,
    //                                      lib/llm/tests/test_streaming_tool_parsers :: ..._fragmented_tokens_vllm)
    //   - PARSER.batch.8   reasoning-plus-tool    (test_parse_reasoning_plus_tool_v4;
    //                                      lib/llm/tests/test_streaming_tool_parsers :: ..._with_tools_vllm)
    //   - PARSER.batch.3  reasoning-only         (test_parse_reasoning_only_no_tool_v4;
    //                                      reasoning/mod.rs :: test_deepseek_v4_detect_and_parse etc.)
    //   - FRONTEND.tool_choice  tool_choice            (lib/llm/tests/tool_choice.rs ::
    //                                      test_deepseek_v4_tool_choice_{auto,required_pins_current_behavior,
    //                                      named_correct_tool_passes,named_wrong_tool_filtered};
    //                                      parser-level invariant in
    //                                      test_parser_does_not_filter_by_tool_choice_v4;
    //                                      cross-parser tool_choice parametrisation work-item (tracked separately) covers full cross-parser parametrisation)
    //   - PIPELINE.finish_reason  finish-reason          (parser-level invariant in
    //                                      test_parser_output_independent_of_upstream_finish_v4;
    //                                      cross-parser stop/tool_calls/length mapping is
    //                                      cross-parser finish_reason mapping work-item (tracked separately); lib/llm/tests/test_streaming_tool_parsers
    //                                      covers ToolCalls / Stop on E2E fixtures)
    //   - PARSER.batch.8  interleaved-text       (test_parse_deepseek_v4_multiple_tool_calls prefix text;
    //                                      lib/llm/tests/test_streaming_tool_parsers :: ..._content_before_tool_vllm)
    //   - PARSER.batch.9  empty/null             (test_parse_empty_and_whitespace_inputs_v4)
    //   - PARSER.batch.10  duplicate-calls        (test_parse_duplicate_invokes_same_name_v4)
    //
    //   - PARSER.xml.1 / PARSER.xml.2  不适用 — DSML 自带每条参数的
    //                  string="true|false" 类型提示，因此 XML 实体解码
    //                  和基于 schema 的类型强转不适用。
    //   - PARSER.harmony.1 / PARSER.harmony.2 不适用 — 仅 Harmony。
    //
    // TODO — 已钉死的 bug，解析器仍需修复：
    //   - PARSER.batch.5  缺陷：当 </｜DSML｜tool_calls>
    //             缺失（max_tokens / EOS 在闭合前到达）。同类问题。
    //             Kimi K2 在 PR #8208 之前的同类问题。修复：扫描完整的
    //             <｜DSML｜invoke>...</｜DSML｜invoke> 对，即使无外层闭合围栏
    //             （参见 kimi_k2_parser.rs 的先例）。
    //             锁定测试在下方断言当前静默丢弃行为；
    //             修复解析器后翻转它们。
    //   - （PARSER.batch.4 缺失参数闭合标签与中间 invoke 截断现已
    //     钉死：见 test_parse_deepseek_v4_missing_parameter_close_loses_param、
    //     test_parse_deepseek_v4_middle_invoke_truncation_corrupts_next。）
    // 尚无客户事故回归测试——V4 才数小时（2026-04-24），
    // 暂无针对它的 bug 报告。
    // -------------------------------------------------------------------

    /// `PARSER.stream.3` — 流式起始 token 检测（V4 variant）。
    #[test] // helper, PARSER.fmt.3 — V4 token variant
    fn test_detect_tool_call_start_v4() {
        let config = get_v4_test_config();
        assert!(detect_tool_call_start_dsml("<｜DSML｜tool_calls>", &config));
        assert!(detect_tool_call_start_dsml(
            "text <｜DSML｜tool_calls>",
            &config
        ));
        assert!(detect_tool_call_start_dsml("<｜DSML｜tool_c", &config));
        assert!(!detect_tool_call_start_dsml(
            "<｜DSML｜function_calls>",
            &config
        ));
        assert!(!detect_tool_call_start_dsml("no tool call here", &config));
    }

    #[test] // helper
    fn test_find_tool_call_end_position() {
        let config = get_test_config();
        let text = "<｜DSML｜function_calls><｜DSML｜invoke name=\"test\"></｜DSML｜invoke></｜DSML｜function_calls>more";
        let pos = find_tool_call_end_position_dsml(text, &config);
        assert_eq!(&text[pos..], "more");
    }

    /// `PARSER.stream.3` — 流式结束位置查找（V4 variant）。
    #[test] // helper, PARSER.fmt.3 — V4 token variant
    fn test_find_tool_call_end_position_v4() {
        let config = get_v4_test_config();
        let text = "<｜DSML｜tool_calls><｜DSML｜invoke name=\"test\"></｜DSML｜invoke></｜DSML｜tool_calls>more";
        let pos = find_tool_call_end_position_dsml(text, &config);
        assert_eq!(&text[pos..], "more");
    }

    #[test] // PARSER.batch.1
    fn test_parse_single_tool_call_string_param() {
        let input = r#"<｜DSML｜function_calls>
<｜DSML｜invoke name="get_weather">
<｜DSML｜parameter name="location" string="true">San Francisco</｜DSML｜parameter>
</｜DSML｜invoke>
</｜DSML｜function_calls>"#;

        let config = get_test_config();
        let result = try_tool_call_parse_dsml(input, &config);
        if let Err(e) = &result {
            eprintln!("Parse error: {:?}", e);
        }
        let (calls, normal) = result.unwrap();

        if calls.is_empty() {
            eprintln!("Input: {}", input);
            eprintln!("No calls parsed!");
        }

        assert_eq!(calls.len(), 1, "Expected 1 tool call, got {}", calls.len());
        assert_eq!(normal, Some("".to_string()));

        let (name, args) = extract_name_and_args(calls[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco");
    }

    #[test] // PARSER.batch.1, PARSER.batch.7
    fn test_parse_single_tool_call_mixed_params() {
        let input = r#"<｜DSML｜function_calls>
<｜DSML｜invoke name="search">
<｜DSML｜parameter name="query" string="true">test query</｜DSML｜parameter>
<｜DSML｜parameter name="topn" string="false">10</｜DSML｜parameter>
</｜DSML｜invoke>
</｜DSML｜function_calls>"#;

        let config = get_test_config();
        let (calls, _) = try_tool_call_parse_dsml(input, &config).unwrap();
        assert_eq!(calls.len(), 1);

        let (name, args) = extract_name_and_args(calls[0].clone());
        assert_eq!(name, "search");
        assert_eq!(args["query"], "test query");
        assert_eq!(args["topn"], 10);
    }

    #[test] // PARSER.batch.2
    fn test_parse_multiple_tool_calls() {
        let input = r#"<｜DSML｜function_calls>
<｜DSML｜invoke name="get_weather">
<｜DSML｜parameter name="location" string="true">Beijing</｜DSML｜parameter>
<｜DSML｜parameter name="date" string="true">2024-01-16</｜DSML｜parameter>
</｜DSML｜invoke>
<｜DSML｜invoke name="get_weather">
<｜DSML｜parameter name="location" string="true">Hangzhou</｜DSML｜parameter>
<｜DSML｜parameter name="date" string="true">2024-01-16</｜DSML｜parameter>
</｜DSML｜invoke>
</｜DSML｜function_calls>"#;

        let config = get_test_config();
        let (calls, _) = try_tool_call_parse_dsml(input, &config).unwrap();
        assert_eq!(calls.len(), 2);

        let (name1, args1) = extract_name_and_args(calls[0].clone());
        assert_eq!(name1, "get_weather");
        assert_eq!(args1["location"], "Beijing");

        let (name2, args2) = extract_name_and_args(calls[1].clone());
        assert_eq!(name2, "get_weather");
        assert_eq!(args2["location"], "Hangzhou");
    }

    /// `PARSER.batch.2` 多调用 + `PARSER.batch.8` 交错文本（块前有前缀文本）。
    #[test] // PARSER.batch.2, PARSER.fmt.3 — V4 variant
    fn test_parse_deepseek_v4_multiple_tool_calls() {
        let input = r#"Let's check this. <｜DSML｜tool_calls>
<｜DSML｜invoke name="get_favorite_tourist_spot">
<｜DSML｜parameter name="city" string="true">Beijing</｜DSML｜parameter>
</｜DSML｜invoke>
<｜DSML｜invoke name="search">
<｜DSML｜parameter name="query" string="true">search agent benchmark 2024</｜DSML｜parameter>
<｜DSML｜parameter name="topn" string="false">10</｜DSML｜parameter>
<｜DSML｜parameter name="source" string="true">web</｜DSML｜parameter>
</｜DSML｜invoke>
</｜DSML｜tool_calls>"#;

        let config = get_v4_test_config();
        let (calls, normal) = try_tool_call_parse_dsml(input, &config).unwrap();
        assert_eq!(calls.len(), 2);
        // 宽松匹配：前导文本必须包含散文内容；空白为实现定义。
        let normal = normal.unwrap();
        assert_eq!(normal.trim(), "Let's check this.");

        let (name1, args1) = extract_name_and_args(calls[0].clone());
        assert_eq!(name1, "get_favorite_tourist_spot");
        assert_eq!(args1["city"], "Beijing");

        let (name2, args2) = extract_name_and_args(calls[1].clone());
        assert_eq!(name2, "search");
        assert_eq!(args2["query"], "search agent benchmark 2024");
        assert_eq!(args2["topn"], 10);
        assert_eq!(args2["source"], "web");
    }

    /// `PARSER.batch.6` — 空参数（无参数 invoke）。
    #[test] // PARSER.batch.6, PARSER.fmt.3 — V4 variant
    fn test_parse_deepseek_v4_no_parameters() {
        let input = r#"<｜DSML｜tool_calls>
<｜DSML｜invoke name="get_current_time">
</｜DSML｜invoke>
</｜DSML｜tool_calls>"#;

        let config = get_v4_test_config();
        let (calls, normal) = try_tool_call_parse_dsml(input, &config).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(normal, Some("".to_string()));

        let (name, args) = extract_name_and_args(calls[0].clone());
        assert_eq!(name, "get_current_time");
        assert_eq!(args, serde_json::json!({}));
    }

    #[test] // PARSER.batch.8
    fn test_parse_with_normal_text() {
        let input = r#"Here's the result: <｜DSML｜function_calls>
<｜DSML｜invoke name="test">
<｜DSML｜parameter name="value" string="true">test</｜DSML｜parameter>
</｜DSML｜invoke>
</｜DSML｜function_calls>"#;

        let config = get_test_config();
        let (calls, normal) = try_tool_call_parse_dsml(input, &config).unwrap();
        assert_eq!(calls.len(), 1);
        // 宽松空白匹配（实现定义）。
        let normal = normal.unwrap();
        assert_eq!(normal.trim(), "Here's the result:");
    }

    #[test]
    fn test_parse_preserves_whitespace_before_dsml_block() {
        // vLLM 逐字保留 DSML 块前的空白；解析器
        // 必须同样处理以使客户端在各服务器间看到相同 prompt。
        let input = "Let me check the forecast.\n\n<｜DSML｜tool_calls>
<｜DSML｜invoke name=\"get_weather\">
<｜DSML｜parameter name=\"city\" string=\"true\">SF</｜DSML｜parameter>
</｜DSML｜invoke>
</｜DSML｜tool_calls>";

        let config = get_v4_test_config();
        let (calls, normal) = try_tool_call_parse_dsml(input, &config).unwrap();
        assert_eq!(calls.len(), 1);
        let normal = normal.unwrap();
        assert!(
            normal.ends_with("\n\n"),
            "Expected trailing \\n\\n preserved, got {:?}",
            normal
        );
        assert_eq!(normal, "Let me check the forecast.\n\n");
    }

    #[test] // PARSER.batch.3
    fn test_parse_no_tool_calls() {
        let input = "This is just normal text without any tool calls.";
        let config = get_test_config();
        let (calls, normal) = try_tool_call_parse_dsml(input, &config).unwrap();
        assert_eq!(calls.len(), 0);
        assert_eq!(normal, Some(input.to_string()));
    }

    #[test] // PARSER.batch.7
    fn test_parse_json_parameter_value() {
        let input = r#"<｜DSML｜function_calls>
<｜DSML｜invoke name="process">
<｜DSML｜parameter name="config" string="false">{"key": "value", "count": 42}</｜DSML｜parameter>
</｜DSML｜invoke>
</｜DSML｜function_calls>"#;

        let config = get_test_config();
        let (calls, _) = try_tool_call_parse_dsml(input, &config).unwrap();
        assert_eq!(calls.len(), 1);

        let (_, args) = extract_name_and_args(calls[0].clone());
        assert!(args["config"].is_object());
        assert_eq!(args["config"]["key"], "value");
        assert_eq!(args["config"]["count"], 42);
    }

    #[test] // PARSER.batch.7
    fn test_parse_array_parameter_value() {
        let input = r#"<｜DSML｜function_calls>
<｜DSML｜invoke name="process">
<｜DSML｜parameter name="items" string="false">[1, 2, 3]</｜DSML｜parameter>
</｜DSML｜invoke>
</｜DSML｜function_calls>"#;

        let config = get_test_config();
        let (calls, _) = try_tool_call_parse_dsml(input, &config).unwrap();
        assert_eq!(calls.len(), 1);

        let (_, args) = extract_name_and_args(calls[0].clone());
        assert!(args["items"].is_array());
        assert_eq!(args["items"][0], 1);
        assert_eq!(args["items"][2], 3);
    }

    #[test] // PARSER.batch.7
    fn test_parse_boolean_parameters() {
        let input = r#"<｜DSML｜function_calls>
<｜DSML｜invoke name="config">
<｜DSML｜parameter name="enabled" string="false">true</｜DSML｜parameter>
<｜DSML｜parameter name="disabled" string="false">false</｜DSML｜parameter>
</｜DSML｜invoke>
</｜DSML｜function_calls>"#;

        let config = get_test_config();
        let (calls, _) = try_tool_call_parse_dsml(input, &config).unwrap();
        assert_eq!(calls.len(), 1);

        let (_, args) = extract_name_and_args(calls[0].clone());
        assert_eq!(args["enabled"], true);
        assert_eq!(args["disabled"], false);
    }

    #[test] // PARSER.batch.7
    fn test_parse_number_parameters() {
        let input = r#"<｜DSML｜function_calls>
<｜DSML｜invoke name="calculate">
<｜DSML｜parameter name="integer" string="false">42</｜DSML｜parameter>
<｜DSML｜parameter name="float" string="false">2.7</｜DSML｜parameter>
<｜DSML｜parameter name="negative" string="false">-100</｜DSML｜parameter>
</｜DSML｜invoke>
</｜DSML｜function_calls>"#;

        let config = get_test_config();
        let (calls, _) = try_tool_call_parse_dsml(input, &config).unwrap();
        assert_eq!(calls.len(), 1);

        let (_, args) = extract_name_and_args(calls[0].clone());
        assert_eq!(args["integer"], 42);
        assert_eq!(args["float"], 2.7);
        assert_eq!(args["negative"], -100);
    }

    #[test] // PARSER.batch.7
    fn test_parse_mixed_types_realistic() {
        // 基于真实测试数据的示例
        let input = r#"<｜DSML｜function_calls>
<｜DSML｜invoke name="search">
<｜DSML｜parameter name="query" string="true">search agent benchmark 2024</｜DSML｜parameter>
<｜DSML｜parameter name="topn" string="false">10</｜DSML｜parameter>
<｜DSML｜parameter name="source" string="true">web</｜DSML｜parameter>
</｜DSML｜invoke>
</｜DSML｜function_calls>"#;

        let config = get_test_config();
        let (calls, _) = try_tool_call_parse_dsml(input, &config).unwrap();
        assert_eq!(calls.len(), 1);

        let (name, args) = extract_name_and_args(calls[0].clone());
        assert_eq!(name, "search");
        assert_eq!(args["query"], "search agent benchmark 2024");
        assert_eq!(args["topn"], 10); // 应为数字，非字符串
        assert_eq!(args["source"], "web");
    }

    #[test] // PARSER.batch.7
    fn test_parse_nested_object_parameter() {
        let input = r#"<｜DSML｜function_calls>
<｜DSML｜invoke name="configure">
<｜DSML｜parameter name="settings" string="false">{"timeout": 30, "retry": true, "endpoints": ["a", "b"]}</｜DSML｜parameter>
</｜DSML｜invoke>
</｜DSML｜function_calls>"#;

        let config = get_test_config();
        let (calls, _) = try_tool_call_parse_dsml(input, &config).unwrap();
        assert_eq!(calls.len(), 1);

        let (_, args) = extract_name_and_args(calls[0].clone());
        assert!(args["settings"].is_object());
        assert_eq!(args["settings"]["timeout"], 30);
        assert_eq!(args["settings"]["retry"], true);
        assert!(args["settings"]["endpoints"].is_array());
        assert_eq!(args["settings"]["endpoints"][0], "a");
    }

    #[test] // PARSER.batch.7
    fn test_parse_empty_string_parameter() {
        let input = r#"<｜DSML｜function_calls>
<｜DSML｜invoke name="test">
<｜DSML｜parameter name="empty" string="true"></｜DSML｜parameter>
</｜DSML｜invoke>
</｜DSML｜function_calls>"#;

        let config = get_test_config();
        let (calls, _) = try_tool_call_parse_dsml(input, &config).unwrap();
        assert_eq!(calls.len(), 1);

        let (_, args) = extract_name_and_args(calls[0].clone());
        assert_eq!(args["empty"], "");
    }

    #[test]
    fn test_empty_invokes_does_not_leak_dsml_markup() {
        // 合法的块起始 + 损坏内容（有 invoke 标签但无闭合/参数），
        // 后接块结束。extract_tool_calls 返回空；我们不得
        // 将 DSML 标记泄漏到 normal_content 中。
        let input = "Let me check. <｜DSML｜tool_calls>\n<｜DSML｜invoke name=\"broken\">\n</｜DSML｜tool_calls>";

        let config = get_v4_test_config();
        let (calls, normal) = try_tool_call_parse_dsml(input, &config).unwrap();

        assert!(
            calls.is_empty(),
            "Expected no tool calls, got {}",
            calls.len()
        );
        let normal = normal.unwrap();
        assert!(
            normal.contains("Let me check."),
            "Expected preamble in normal_content, got {:?}",
            normal
        );
        assert!(
            !normal.contains("<｜DSML｜"),
            "normal_content leaked DSML markup: {:?}",
            normal
        );
    }

    #[test]
    fn test_parse_parameter_missing_string_attribute() {
        // 模型发出不带 `string="..."` 属性的参数。
        // 解析器应尽力解析：先 JSON，失败则回退为字符串。
        let input = r#"<｜DSML｜function_calls>
<｜DSML｜invoke name="greet">
<｜DSML｜parameter name="name">Alice</｜DSML｜parameter>
</｜DSML｜invoke>
</｜DSML｜function_calls>"#;

        let config = get_test_config();
        let (calls, _) = try_tool_call_parse_dsml(input, &config).unwrap();
        assert_eq!(calls.len(), 1);

        let (name, args) = extract_name_and_args(calls[0].clone());
        assert_eq!(name, "greet");
        assert_eq!(args["name"], "Alice");
    }

    #[test]
    fn test_parse_string_false_with_bare_word_value() {
        // `string="false"` 的非 JSON 裸词仍应出现在参数中（作为字符串回退值）。
        let input = r#"<｜DSML｜function_calls>
<｜DSML｜invoke name="run">
<｜DSML｜parameter name="mode" string="false">quickly</｜DSML｜parameter>
</｜DSML｜invoke>
</｜DSML｜function_calls>"#;

        let config = get_test_config();
        let (calls, _) = try_tool_call_parse_dsml(input, &config).unwrap();
        assert_eq!(calls.len(), 1);

        let (_, args) = extract_name_and_args(calls[0].clone());
        assert_eq!(args["mode"], "quickly");
    }

    #[test]
    fn test_tool_call_id_format_openai_style() {
        let input = r#"<｜DSML｜function_calls>
<｜DSML｜invoke name="get_weather">
<｜DSML｜parameter name="location" string="true">San Francisco</｜DSML｜parameter>
</｜DSML｜invoke>
</｜DSML｜function_calls>"#;

        let config = get_test_config();
        let (calls, _) = try_tool_call_parse_dsml(input, &config).unwrap();
        assert_eq!(calls.len(), 1);

        // 仅验证形状的断言：OpenAI 风格 `call_` 前缀 + 至少 20 位
        // 小写字母数字字符。故意不锁定
        // 精确长度/字符集，使 id 生成器可演进而不影响本测试。
        let id = &calls[0].id;
        assert!(
            id.starts_with("call_"),
            "id should start with call_: {}",
            id
        );
        let suffix = &id["call_".len()..];
        assert!(
            suffix.len() >= 20,
            "suffix must be at least 20 chars: {}",
            suffix
        );
        assert!(
            suffix
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit()),
            "suffix must match [a-z0-9]+: {}",
            suffix
        );
    }

    #[test]
    fn test_multi_block_preserves_inter_and_trailing_text() {
        // 两个完整 DSML 块，前后及中间均有文本。
        // 两个块均须被解析，且块间/尾部文本必须保留。
        // 保留在 normal_text 中。
        let input = "pre <｜DSML｜tool_calls>\n<｜DSML｜invoke name=\"a\">\n</｜DSML｜invoke>\n</｜DSML｜tool_calls> middle <｜DSML｜tool_calls>\n<｜DSML｜invoke name=\"b\">\n</｜DSML｜invoke>\n</｜DSML｜tool_calls> tail";

        let config = get_v4_test_config();
        let (calls, normal) = try_tool_call_parse_dsml(input, &config).unwrap();
        assert_eq!(calls.len(), 2, "expected both blocks parsed");
        assert_eq!(calls[0].function.name, "a");
        assert_eq!(calls[1].function.name, "b");

        let normal = normal.unwrap();
        assert!(
            normal.contains(" middle "),
            "inter-block text lost: {:?}",
            normal
        );
        assert!(normal.contains(" tail"), "trailing text lost: {:?}", normal);
        assert!(
            !normal.contains("<｜DSML｜"),
            "normal_content leaked DSML markup: {:?}",
            normal
        );
    }

    #[test]
    fn test_unterminated_block_followed_by_valid_block() {
        // 一个未闭合的 DSML 起始出现在完整块之前。非贪婪块正则从首个 block_start
        // 跨越到唯一的 block_end，将嵌套的第二个 block_start 吞为块内容的一部分。
        //
        // 在此捕获区间内，非贪婪 invoke 正则将首个 `<invoke name="broken">`
        // 与首个 `</invoke>` 配对——因此仅恢复一个名为 "broken" 的工具调用。
        // 这是当前观察到的契约；本测试将其锁定，使任何未来行为变更必须显式而非静默。
        let input = "pre <｜DSML｜tool_calls>\n<｜DSML｜invoke name=\"broken\">\n mid <｜DSML｜tool_calls>\n<｜DSML｜invoke name=\"ok\">\n</｜DSML｜invoke>\n</｜DSML｜tool_calls> tail";

        let config = get_v4_test_config();
        let (calls, normal) = try_tool_call_parse_dsml(input, &config).unwrap();

        assert_eq!(calls.len(), 1, "exactly one invoke recovered");
        assert_eq!(
            calls[0].function.name, "broken",
            "outer invoke name is matched first (non-greedy)"
        );

        let normal = normal.unwrap();
        assert!(
            normal.starts_with("pre"),
            "pre-block text must survive: {:?}",
            normal
        );
        assert!(
            normal.contains(" tail"),
            "trailing text must survive: {:?}",
            normal
        );
        assert!(
            !normal.contains("<｜DSML｜tool_calls>"),
            "normal_content leaked block_start: {:?}",
            normal
        );
        assert!(
            !normal.contains("</｜DSML｜tool_calls>"),
            "normal_content leaked block_end: {:?}",
            normal
        );
    }

    #[test] // PARSER.batch.7, PARSER.batch.9
    fn test_parse_null_parameter() {
        let input = r#"<｜DSML｜function_calls>
<｜DSML｜invoke name="test">
<｜DSML｜parameter name="value" string="false">null</｜DSML｜parameter>
</｜DSML｜invoke>
</｜DSML｜function_calls>"#;

        let config = get_test_config();
        let (calls, _) = try_tool_call_parse_dsml(input, &config).unwrap();
        assert_eq!(calls.len(), 1);

        let (_, args) = extract_name_and_args(calls[0].clone());
        assert!(args["value"].is_null());
    }

    // 边界情况锁定测试。V4 覆盖清单（上方）含完整的 PARSER.* → test 映射。
    // 每个测试的文档注释标注了
    // 标记了它锁定的具体 CASE。

    /// `PARSER.batch.5` — 缺失结束 token 恢复。
    /// **标记为已知缺陷**——解析器丢弃调用；参见上方 TODO 块。
    ///
    /// 如果 DeepSeek V4 流在 `</｜DSML｜tool_calls>` 到达前被截断
    /// （max_tokens 截断、EOS 中途、连接断开），块正则要求两侧围栏均存在，
    /// 匹配零次。整个 DSML 形态的载荷以原始 `normal_text` 形式穿透；无工具调用被恢复。
    ///
    /// 这与 Kimi K2 在解析器获得结束 token 恢复能力之前的相同结构性故障模式一致；
    /// 参见 `kimi_k2_parser.rs::test_parse_malformed_no_section_end` 的修复后恢复模式。
    ///
    /// 注：加固后，当块起始出现但无 invoke 被解析时，解析器不再将原始 DSML 标记泄漏到
    /// `normal_text` 中——仅返回块前文本（此处为空，因为输入始于块起始围栏）。调用仍被丢弃。
    #[test] // PARSER.batch.5, PARSER.fmt.3 — V4 variant
    fn test_parse_deepseek_v4_missing_end_token() {
        // 起始围栏 + 完整 invoke，但无 </｜DSML｜tool_calls>。
        let input = "<｜DSML｜tool_calls>\n\
<｜DSML｜invoke name=\"get_weather\">\n\
<｜DSML｜parameter name=\"city\" string=\"true\">NYC</｜DSML｜parameter>\n\
</｜DSML｜invoke>";

        let config = get_v4_test_config();
        let (calls, normal_text) = try_tool_call_parse_dsml(input, &config).unwrap();

        assert!(
            calls.is_empty(),
            "V4 DSML parser currently drops tool calls when \
             </｜DSML｜tool_calls> is missing. \
             If recovery is added, flip this assertion."
        );
        assert_eq!(
            normal_text.as_deref(),
            Some(""),
            "Pre-block text is empty here; raw DSML markup must not leak \
             into normal_text (post-hardening behavior)."
        );
    }

    /// `PARSER.batch.5` — 多个完整 invoke，缺失结束围栏。
    ///
    /// 即使在起始围栏内有多条完整构造的 invoke，缺失闭合围栏也会阻止块正则匹配。
    /// 所有调用均被丢弃。若解析器未来获得部分块恢复能力，本测试将失败并强制有意识地更新。
    #[test] // PARSER.batch.2, PARSER.batch.5, PARSER.fmt.3
    fn test_parse_deepseek_v4_missing_end_token_multiple_calls() {
        let input = "<｜DSML｜tool_calls>\n\
<｜DSML｜invoke name=\"a\">\n\
<｜DSML｜parameter name=\"x\" string=\"true\">1</｜DSML｜parameter>\n\
</｜DSML｜invoke>\n\
<｜DSML｜invoke name=\"b\">\n\
<｜DSML｜parameter name=\"y\" string=\"true\">2</｜DSML｜parameter>\n\
</｜DSML｜invoke>";

        let config = get_v4_test_config();
        let (calls, _) = try_tool_call_parse_dsml(input, &config).unwrap();

        assert!(
            calls.is_empty(),
            "Even two fully-formed invokes are dropped when the outer \
             </｜DSML｜tool_calls> is missing."
        );
    }

    /// `PARSER.batch.4` — `string="false"` 参数中的畸形 JSON 值回退
    /// 为字符串。`parse_parameters` 显式吞掉 serde 错误
    /// (unwrap_or_else → Value::String)。锁定此回退，移除它
    /// （会导致粗糙 JSON 上整个调用 500）属于意向性变更。
    #[test] // PARSER.batch.4, PARSER.fmt.3
    fn test_parse_deepseek_v4_malformed_json_value_falls_back_to_string() {
        let input = "<｜DSML｜tool_calls>\n\
<｜DSML｜invoke name=\"test\">\n\
<｜DSML｜parameter name=\"payload\" string=\"false\">{this is not valid json</｜DSML｜parameter>\n\
</｜DSML｜invoke>\n\
</｜DSML｜tool_calls>";

        let config = get_v4_test_config();
        let (calls, _) = try_tool_call_parse_dsml(input, &config).unwrap();
        assert_eq!(calls.len(), 1);

        let (name, args) = extract_name_and_args(calls[0].clone());
        assert_eq!(name, "test");
        assert_eq!(
            args["payload"], "{this is not valid json",
            "Malformed JSON should fall back to the raw string, not drop \
             the parameter or the call."
        );
    }

    /// `PARSER.batch.4` — 畸形的 invoke（缺失 `</｜DSML｜invoke>` 但块围栏完整）。
    /// invoke 正则要求自身的闭合标签，因此调用被静默丢弃。锁定此行为。
    #[test] // PARSER.batch.4, PARSER.fmt.3
    fn test_parse_deepseek_v4_missing_invoke_close_drops_call() {
        let input = "<｜DSML｜tool_calls>\n\
<｜DSML｜invoke name=\"test\">\n\
<｜DSML｜parameter name=\"x\" string=\"true\">value</｜DSML｜parameter>\n\
</｜DSML｜tool_calls>";

        let config = get_v4_test_config();
        let (calls, _) = try_tool_call_parse_dsml(input, &config).unwrap();
        assert!(
            calls.is_empty(),
            "Malformed invoke (missing </｜DSML｜invoke>) is dropped today. \
             If recovery is added, flip this assertion."
        );
    }

    /// `PARSER.batch.4` — 畸形的 invoke（缺失 `</｜DSML｜parameter>` 闭合标签）。
    /// 参数正则需要自身的闭合标签；若某个参数在 `</｜DSML｜invoke>` 之前从未闭合，
    /// 该参数被静默丢失而调用本身仍被解析。锁定此部分行为。
    ///
    /// TODO(PARSER.batch.4) — BUG，需修复：解析器静默丢失参数并将一个规格不足的调用
    /// 交付给用户。修复方案应在 `</｜DSML｜invoke>` 之前保留原始值。修复后翻转本测试。
    #[test] // PARSER.batch.4, PARSER.fmt.3
    fn test_parse_deepseek_v4_missing_parameter_close_loses_param() {
        let input = "<｜DSML｜tool_calls>\n\
<｜DSML｜invoke name=\"test\">\n\
<｜DSML｜parameter name=\"x\" string=\"true\">value\n\
</｜DSML｜invoke>\n\
</｜DSML｜tool_calls>";

        let config = get_v4_test_config();
        let (calls, _) = try_tool_call_parse_dsml(input, &config).unwrap();
        // PIN_ME：首次运行后替换为观察到的行为。
        assert_eq!(calls.len(), 1);
        let (name, args) = extract_name_and_args(calls[0].clone());
        assert_eq!(name, "test");
        assert!(
            args.get("x").is_none(),
            "Expected 'x' to be dropped because </｜DSML｜parameter> is missing; \
             got args={args}"
        );
    }

    /// `PARSER.batch.4` — 中间 invoke 截断。如果 invoke A 缺失其
    /// `</｜DSML｜invoke>` 且 invoke B 紧随在同一外层块内，
    /// A 的内容渗入 B（正则非贪婪匹配消费了 B 的标记）。锁定此损坏行为。
    ///
    /// TODO(PARSER.batch.4) — 缺陷，需修复：A 吞掉 B 的参数而 B 被
    /// 静默丢弃——调用方收到 A 的错误参数且完全看不到 B。
    /// 修复：锚定到 `<｜DSML｜invoke name=` 以在多个 invoke 之间重新同步。修复后翻转本测试。
    #[test] // PARSER.batch.4, PARSER.fmt.3
    fn test_parse_deepseek_v4_middle_invoke_truncation_corrupts_next() {
        let input = "<｜DSML｜tool_calls>\n\
<｜DSML｜invoke name=\"a\">\n\
<｜DSML｜parameter name=\"x\" string=\"true\">1</｜DSML｜parameter>\n\
<｜DSML｜invoke name=\"b\">\n\
<｜DSML｜parameter name=\"y\" string=\"true\">2</｜DSML｜parameter>\n\
</｜DSML｜invoke>\n\
</｜DSML｜tool_calls>";

        let config = get_v4_test_config();
        let (calls, _) = try_tool_call_parse_dsml(input, &config).unwrap();
        // 当前：invoke A 吸收了 invoke B 的参数（正则渗漏），B 被丢弃。
        // 被完整丢弃。错误但稳定；钉死以便修复时明确。
        assert_eq!(calls.len(), 1, "B is dropped; A is the lone survivor");
        let (name, args) = extract_name_and_args(calls[0].clone());
        assert_eq!(name, "a");
        assert_eq!(args["x"], "1", "A's own parameter still parses correctly");
        assert_eq!(
            args["y"], "2",
            "BUG: B's parameter bleeds into A because A's body match runs \
             past the missing </｜DSML｜invoke> until B's close tag"
        );
    }

    /// `PARSER.stream.3` — 流式块边界切分。逐 token 组装：
    /// 起始 token 检测器与结束位置查找均需容忍
    /// 块边界落在多字节围栏中间的情况。
    #[test] // PARSER.stream.3
    fn test_streaming_chunk_boundary_split_v4() {
        let config = get_v4_test_config();
        // 检测器应在部分起始围栏上触发（差一个字符）。
        assert!(detect_tool_call_start_dsml("<｜DSML｜tool_call", &config));
        // 以及在以围栏首个字符结尾的空缓冲上触发。
        assert!(detect_tool_call_start_dsml("<", &config));
        // 当结束围栏未到达时结束位置查找须返回 chunk.len()——调用方应继续缓冲。
        // 尚未到达——调用方应继续缓冲。
        let partial = "<｜DSML｜tool_calls>\n<｜DSML｜invoke name=\"a\">\n";
        assert_eq!(
            find_tool_call_end_position_dsml(partial, &config),
            partial.len(),
            "Partial chunk without close fence must report end=len so caller buffers more"
        );
    }

    /// `PARSER.batch.8` — 同一响应中推理 + 工具调用配对。DSv4 发出
    /// `<think>...</think>` 在 DSML 块之前；工具解析器只关心 DSML，
    /// 但 normal_text 必须逐字保留推理标记使推理解析器可继续处理。
    #[test] // PARSER.batch.8
    fn test_parse_reasoning_plus_tool_v4() {
        let input = "<think>Let me check the weather.</think>\
<｜DSML｜tool_calls>\n\
<｜DSML｜invoke name=\"get_weather\">\n\
<｜DSML｜parameter name=\"city\" string=\"true\">NYC</｜DSML｜parameter>\n\
</｜DSML｜invoke>\n\
</｜DSML｜tool_calls>";
        let config = get_v4_test_config();
        let (calls, normal_text) = try_tool_call_parse_dsml(input, &config).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "get_weather");
        let normal = normal_text.unwrap_or_default();
        assert!(
            normal.contains("<think>") && normal.contains("</think>"),
            "Reasoning markup must be preserved in normal_text for the \
             downstream reasoning parser; got {:?}",
            normal
        );
    }

    /// `PARSER.batch.3` — 仅推理（仅 think 标签，无工具调用）。解析器必须
    /// 返回零调用并将整个输入以 normal_text 透传。
    #[test] // PARSER.batch.3
    fn test_parse_reasoning_only_no_tool_v4() {
        let input = "<think>Just thinking out loud, no tools needed.</think>";
        let config = get_v4_test_config();
        let (calls, normal_text) = try_tool_call_parse_dsml(input, &config).unwrap();
        assert!(calls.is_empty());
        assert_eq!(normal_text.as_deref(), Some(input));
    }

    /// 解析器级不变量：dsml 解析器不过滤 `tool_choice`——它返回每个格式正确的 invoke，
    /// jail / response builder 层负责按 `tool_choice=named`/`required`/`none` 过滤。
    /// 真正的 FRONTEND.tool_choice 覆盖位于集成层（`lib/llm/tests/tool_choice.rs`）。
    #[test]
    fn test_parser_does_not_filter_by_tool_choice_v4() {
        let input = "<｜DSML｜tool_calls>\n\
<｜DSML｜invoke name=\"get_weather\">\n\
<｜DSML｜parameter name=\"city\" string=\"true\">NYC</｜DSML｜parameter>\n\
</｜DSML｜invoke>\n\
<｜DSML｜invoke name=\"get_time\">\n\
<｜DSML｜parameter name=\"tz\" string=\"true\">EST</｜DSML｜parameter>\n\
</｜DSML｜invoke>\n\
</｜DSML｜tool_calls>";
        let config = get_v4_test_config();
        let (calls, _) = try_tool_call_parse_dsml(input, &config).unwrap();
        assert_eq!(calls.len(), 2);
    }

    /// 解析器级不变量：dsml 解析器是字节稳定的——不看 `finish_reason`，
    /// 无论上游流结束原因如何输出均相同。真正的 PIPELINE.finish_reason 覆盖
    /// （stop / tool_calls / length 映射）在 `lib/llm/tests/test_streaming_tool_parsers.rs`
    /// 且属于跨解析器 finish_reason 映射工作项（单独跟踪）。
    #[test]
    fn test_parser_output_independent_of_upstream_finish_v4() {
        let input = "<｜DSML｜tool_calls>\n\
<｜DSML｜invoke name=\"get_weather\">\n\
<｜DSML｜parameter name=\"city\" string=\"true\">NYC</｜DSML｜parameter>\n\
</｜DSML｜invoke>\n\
</｜DSML｜tool_calls>";
        let config = get_v4_test_config();
        let (calls, _) = try_tool_call_parse_dsml(input, &config).unwrap();
        assert_eq!(calls.len(), 1);
    }

    /// `PARSER.batch.9` — 空/null 内容变体。锁定真正空字节与纯空白输入的行为。
    #[test] // PARSER.batch.9
    fn test_parse_empty_and_whitespace_inputs_v4() {
        let config = get_v4_test_config();
        for input in &["", " ", "\n", "\t\n  \t"] {
            let (calls, normal) = try_tool_call_parse_dsml(input, &config).unwrap();
            assert!(
                calls.is_empty(),
                "Empty/whitespace input must yield no calls (input={:?})",
                input
            );
            // 空输入快速路径返回 Some("")；其他空白字符在搜索前被 trim，
            // 无块分支返回 trim 后（同样为空）的字符串。
            assert_eq!(
                normal.as_deref(),
                Some(""),
                "Empty/whitespace input collapses to empty normal_text"
            );
        }
    }

    /// `PARSER.batch.10` — 重复调用（同一 invoke 名称在一个块内出现两次）。
    /// 测试分类法中的通用缺口；首次 DSML 覆盖。
    #[test] // PARSER.batch.10
    fn test_parse_duplicate_invokes_same_name_v4() {
        let input = "<｜DSML｜tool_calls>\n\
<｜DSML｜invoke name=\"get_weather\">\n\
<｜DSML｜parameter name=\"city\" string=\"true\">NYC</｜DSML｜parameter>\n\
</｜DSML｜invoke>\n\
<｜DSML｜invoke name=\"get_weather\">\n\
<｜DSML｜parameter name=\"city\" string=\"true\">LA</｜DSML｜parameter>\n\
</｜DSML｜invoke>\n\
</｜DSML｜tool_calls>";
        let config = get_v4_test_config();
        let (calls, _) = try_tool_call_parse_dsml(input, &config).unwrap();
        assert_eq!(
            calls.len(),
            2,
            "Both duplicate-name invokes must be returned"
        );
        assert_eq!(calls[0].function.name, "get_weather");
        assert_eq!(calls[1].function.name, "get_weather");
        assert_ne!(
            calls[0].id, calls[1].id,
            "Duplicate calls must have distinct ids"
        );
        let (_, args0) = extract_name_and_args(calls[0].clone());
        let (_, args1) = extract_name_and_args(calls[1].clone());
        assert_eq!(args0["city"], "NYC");
        assert_eq!(args1["city"], "LA");
    }
}
