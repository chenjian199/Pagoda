// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-License-Identifier: Apache-2.0

//! # tool_calling::xml::kimi_k2_parser
//!
//! ## 设计意图
//! 解析 Kimi K2 风格的工具调用区段：
//! ```text
//! <|tool_calls_section_begin|>
//! <|tool_call_begin|>functions.get_weather:0<|tool_call_argument_begin|>{"location":"NYC"}<|tool_call_end|>
//! <|tool_calls_section_end|>
//! ```
//! 参考实现：
//! - https://github.com/sgl-project/sglang/blob/main/python/sglang/srt/function_call/kimik2_detector.py
//! - https://github.com/vllm-project/vllm/blob/main/vllm/tool_parsers/kimi_k2_tool_parser.py
//!
//! ## 外部契约
//! - `detect_tool_call_start_kimi_k2(chunk, config)`：完整或部分区段起始 token 命中即 true。
//! - `find_tool_call_end_position_kimi_k2(chunk, config)`：返回区段结束之后位置；缺失返回 None。
//! - `try_tool_call_parse_kimi_k2(message, config, tools)`：返回 `(calls, normal_text)`。
//!
//! ## 实现要点
//! - 调用 id 保留模型原生格式（如 `functions.bash:0`），与 vllm/sglang 对齐。
//! - section_end 缺失时把剩余文本视为区段体，以便从截断输出中恢复完整调用。

use std::sync::OnceLock;

use regex::Regex;

use super::super::ToolDefinition;
use super::super::config::KimiK2ParserConfig;
use super::response::{CalledFunction, ToolCallResponse, ToolCallType};

static ID_REGEX: OnceLock<Regex> = OnceLock::new();

static TOOL_CALL_REGEX: OnceLock<Regex> = OnceLock::new();

// === SECTION: 正则缓存 ===

/// 返回缓存的正则，捕获 `function_id`（如 `functions.get_weather:0`）与 `arguments`（JSON 对象），
/// 三者夹在 `call_start`、`argument_begin`、`call_end` token 之间。
///
/// `function_id` 模式 `[\w.\-]+:\d+` 匹配 Kimi K2 的 `functions.name:index` 形态，与 sglang
/// 参考实现一致。包含连字符以支持带横线的函数名（如 MCP 工具 `mcp__portal__search-documents`）。
fn get_tool_call_regex(config: &KimiK2ParserConfig) -> &'static Regex {
    TOOL_CALL_REGEX.get_or_init(|| {
        // arguments 故意用宽松的 `.*?` 而非 `\{...\}`，使截断 JSON（如 max_tokens / EOS
        // 造成的 `{"location":"NYC`）仍可命中。下游 serde_json::from_str 充当校验器：
        // 合法负载被解析，非法/截断负载退化为原始字符串参数路径。
        let pattern = format!(
            r"(?s){}\s*(?P<function_id>[\w.\-]+:\d+)\s*{}\s*(?P<arguments>.*?)\s*{}",
            regex::escape(&config.call_start),
            regex::escape(&config.argument_begin),
            regex::escape(&config.call_end),
        );
        Regex::new(&pattern).expect("Failed to compile kimi k2 tool call regex")
    })
}

fn get_id_regex() -> &'static Regex {
    ID_REGEX.get_or_init(|| {
        Regex::new(r"^(?:functions\.)?(?P<name>[\w.\-]+):(?P<index>\d+)$")
            .expect("Failed to compile kimi k2 id regex")
    })
}

// === SECTION: 流式探测 ===

/// 判断 chunk 是否包含（或部分包含，用于流式）Kimi K2 区段起始。
pub fn detect_tool_call_start_kimi_k2(chunk: &str, config: &KimiK2ParserConfig) -> bool {
    config.section_start_variants.iter().any(|start_token| {
        debug_assert!(
            start_token.is_ascii(),
            "Kimi K2 section tokens must be ASCII for safe byte slicing, got: {start_token:?}"
        );

        // 完整命中，或结尾恰好是起始 token 的某前缀（流式）
        chunk.contains(start_token.as_str())
            || (1..start_token.len()).any(|i| chunk.ends_with(&start_token[..i]))
    })
}

/// 返回区段结束 token 之后的位置（取最早出现的变体）；缺失返回 `None`，
/// 用于告知流式 jail 区段尚未正确闭合、应继续累积。
pub fn find_tool_call_end_position_kimi_k2(
    chunk: &str,
    config: &KimiK2ParserConfig,
) -> Option<usize> {
    config
        .section_end_variants
        .iter()
        .filter_map(|end_token| chunk.find(end_token.as_str()).map(|pos| pos + end_token.len()))
        .min()
}

// === SECTION: 顶层解析入口 ===

pub fn try_tool_call_parse_kimi_k2(
    message: &str,
    config: &KimiK2ParserConfig,
    tools: Option<&[ToolDefinition]>,
) -> anyhow::Result<(Vec<ToolCallResponse>, Option<String>)> {
    let (normal_text, tool_calls) = extract_tool_calls(message, config, tools)?;
    Ok((tool_calls, Some(normal_text)))
}

/// 在 `text[cursor..]` 中找首个区段起始变体，返回 `(相对位置, 命中 token 长度)`。
fn find_section_start(
    text: &str,
    cursor: usize,
    config: &KimiK2ParserConfig,
) -> Option<(usize, usize)> {
    config
        .section_start_variants
        .iter()
        .filter_map(|variant| {
            text[cursor..]
                .find(variant.as_str())
                .map(|pos| (pos, variant.len()))
        })
        .min_by_key(|&(pos, _)| pos)
}

/// 在 `text[from..]` 中找首个区段结束变体，返回 `(相对位置, 命中 token 长度)`。
fn find_section_end(text: &str, from: usize, config: &KimiK2ParserConfig) -> Option<(usize, usize)> {
    config
        .section_end_variants
        .iter()
        .filter_map(|variant| {
            text[from..]
                .find(variant.as_str())
                .map(|pos| (pos, variant.len()))
        })
        .min_by_key(|&(pos, _)| pos)
}

/// 从消息中分离工具调用区段与普通文本。
///
/// ## 与 Moonshot 参考实现的差异
///
/// 参考实现要求存在 `section_end` 才能抽取任何调用：
///
/// ```python
/// pattern = r"<\|tool_calls_section_begin\|>(.*?)<\|tool_calls_section_end\|>"
/// tool_calls_sections = re.findall(pattern, tool_call_rsp, re.DOTALL)
/// ```
///
/// 当 `section_end` 缺失（max_tokens / EOS / stop）时，`re.findall` 返回 `[]`，
/// 完整的单个调用也被静默丢弃。本实现把缺失的 `section_end` 视为「区段延伸至串尾」，
/// 等价于：
///
/// ```python
/// pattern = r"<\|tool_calls_section_begin\|>(.*?)(?:<\|tool_calls_section_end\|>|$)"
/// ```
fn extract_tool_calls(
    text: &str,
    config: &KimiK2ParserConfig,
    tools: Option<&[ToolDefinition]>,
) -> anyhow::Result<(String, Vec<ToolCallResponse>)> {
    let mut normal_parts: Vec<&str> = Vec::new();
    let mut calls: Vec<ToolCallResponse> = Vec::new();
    let mut cursor = 0;

    while cursor < text.len() {
        let Some((start_pos, _start_len)) = find_section_start(text, cursor, config) else {
            // 没有更多区段
            normal_parts.push(&text[cursor..]);
            break;
        };
        let abs_start = cursor + start_pos;
        normal_parts.push(&text[cursor..abs_start]);

        // section_end 缺失时把剩余文本作为区段体（截断恢复）
        let (block, next_cursor) = match find_section_end(text, abs_start, config) {
            Some((end_pos, end_len)) => {
                let abs_end = abs_start + end_pos + end_len;
                (&text[abs_start..abs_end], abs_end)
            }
            None => (&text[abs_start..], text.len()),
        };

        if let Ok(parsed_calls) = parse_section_block(block, config, tools) {
            calls.extend(parsed_calls);
        }
        cursor = next_cursor;
    }

    let normal_text = normal_parts.join("").trim().to_string();
    Ok((normal_text, calls))
}

/// 解析单个区段块，抽取其中各个工具调用。
///
/// 块位于 `<|tool_calls_section_begin|>` 与 `<|tool_calls_section_end|>` 之间，
/// 每个调用位于 `<|tool_call_begin|>` 与 `<|tool_call_end|>` 之间。
fn parse_section_block(
    block: &str,
    config: &KimiK2ParserConfig,
    tools: Option<&[ToolDefinition]>,
) -> anyhow::Result<Vec<ToolCallResponse>> {
    let tool_call_regex = get_tool_call_regex(config);
    let id_regex = get_id_regex();

    let mut results = Vec::new();

    for cap in tool_call_regex.captures_iter(block) {
        let function_id = cap
            .name("function_id")
            .map(|m| m.as_str().trim())
            .unwrap_or("");
        let arguments_raw = cap
            .name("arguments")
            .map(|m| m.as_str().trim())
            .unwrap_or("{}");

        // 解析 function id：优先取捕获组 name，否则整体作为函数名
        let function_name = match id_regex.captures(function_id) {
            Some(id_cap) => id_cap
                .name("name")
                .map(|m| m.as_str().to_string())
                .unwrap_or_default(),
            None => {
                tracing::warn!(
                    "Unexpected tool_call_id format: '{}', using as-is",
                    function_id
                );
                function_id.to_string()
            }
        };

        if function_name.is_empty() {
            continue;
        }

        // 若提供 tools，仅告警而不拦截未知函数
        if let Some(tools) = tools
            && !tools.iter().any(|t| t.name == function_name)
        {
            tracing::warn!("Tool '{}' is not defined in the tools list.", function_name);
        }

        // 校验 JSON 参数；失败时退化为原始字符串
        let arguments_json = match serde_json::from_str::<serde_json::Value>(arguments_raw) {
            Ok(val) => serde_json::to_string(&val)?,
            Err(e) => {
                tracing::warn!(
                    "Failed to parse JSON arguments for tool '{}': {}. Using raw string.",
                    function_name,
                    e,
                );
                arguments_raw.to_string()
            }
        };

        results.push(ToolCallResponse {
            id: function_id.to_string(),
            tp: ToolCallType::Function,
            function: CalledFunction {
                name: function_name,
                arguments: arguments_json,
            },
        });
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    //! ## 测试过程
    //! 围绕 Kimi K2 公开 API（`detect_tool_call_start_kimi_k2`、
    //! `find_tool_call_end_position_kimi_k2`、`try_tool_call_parse_kimi_k2`）覆盖：
    //! 起始/结束探测、单/多调用、区段截断恢复、id 保留、非法 JSON 退化等。
    //!
    //! ## 意义
    //! 锁定 Kimi K2 区段在截断与多调用场景下的可观察行为，保证与 vllm/sglang 的兼容性。
    use super::*;
    use rstest::rstest;

    fn default_config() -> KimiK2ParserConfig {
        KimiK2ParserConfig::default()
    }

    #[test] // detection helper
    fn test_detect_tool_call_start() {
        let config = default_config();
        assert!(detect_tool_call_start_kimi_k2(
            "<|tool_calls_section_begin|>",
            &config
        ));
        assert!(detect_tool_call_start_kimi_k2(
            "text <|tool_calls_section_begin|>",
            &config
        ));
        assert!(detect_tool_call_start_kimi_k2("<|tool_calls_sec", &config));
        assert!(detect_tool_call_start_kimi_k2("<|", &config));
        assert!(!detect_tool_call_start_kimi_k2(
            "no tool call here",
            &config
        ));
        assert!(!detect_tool_call_start_kimi_k2("toolcall", &config));
    }

    #[test] // detection helper
    fn test_find_tool_call_end_position() {
        let config = default_config();
        let text = "<|tool_calls_section_begin|><|tool_call_begin|>functions.test:0<|tool_call_argument_begin|>{}<|tool_call_end|><|tool_calls_section_end|>more text";
        let pos = find_tool_call_end_position_kimi_k2(text, &config);
        assert_eq!(pos, Some(text.len() - "more text".len()));
        assert_eq!(&text[pos.unwrap()..], "more text");

        let text_no_end = "<|tool_calls_section_begin|><|tool_call_begin|>functions.test:0";
        let pos = find_tool_call_end_position_kimi_k2(text_no_end, &config);
        assert_eq!(pos, None, "should return None when section_end is missing");
    }

    #[test] // PARSER.batch.1
    fn test_parse_simple_tool_call() {
        let config = default_config();
        let input = r#"<|tool_calls_section_begin|><|tool_call_begin|>functions.get_weather:0<|tool_call_argument_begin|>{"location":"NYC"}<|tool_call_end|><|tool_calls_section_end|>"#;

        let (calls, normal) = try_tool_call_parse_kimi_k2(input, &config, None).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "get_weather");
        assert_eq!(normal, Some("".to_string()));

        let args: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
        assert_eq!(args["location"], "NYC");
    }

    #[test] // PARSER.batch.1, PARSER.batch.7
    fn test_parse_multiple_args() {
        let config = default_config();
        let input = r#"<|tool_calls_section_begin|><|tool_call_begin|>functions.get_weather:0<|tool_call_argument_begin|>{"location":"San Francisco, CA","unit":"fahrenheit"}<|tool_call_end|><|tool_calls_section_end|>"#;

        let (calls, _) = try_tool_call_parse_kimi_k2(input, &config, None).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "get_weather");

        let args: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[test] // PARSER.batch.2
    fn test_parse_multiple_tool_calls() {
        let config = default_config();
        let input = r#"<|tool_calls_section_begin|><|tool_call_begin|>functions.get_weather:0<|tool_call_argument_begin|>{"location":"NYC"}<|tool_call_end|><|tool_call_begin|>functions.get_time:1<|tool_call_argument_begin|>{"timezone":"EST"}<|tool_call_end|><|tool_calls_section_end|>"#;

        let (calls, normal) = try_tool_call_parse_kimi_k2(input, &config, None).unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].function.name, "get_weather");
        assert_eq!(calls[1].function.name, "get_time");
        assert_eq!(normal, Some("".to_string()));

        let args0: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
        let args1: serde_json::Value = serde_json::from_str(&calls[1].function.arguments).unwrap();
        assert_eq!(args0["location"], "NYC");
        assert_eq!(args1["timezone"], "EST");
    }

    #[test] // PARSER.batch.8
    fn test_parse_with_normal_text() {
        let config = default_config();
        let input = r#"I'll help you with that. <|tool_calls_section_begin|><|tool_call_begin|>functions.get_weather:0<|tool_call_argument_begin|>{"location":"Dallas"}<|tool_call_end|><|tool_calls_section_end|> Let me check."#;

        let (calls, normal) = try_tool_call_parse_kimi_k2(input, &config, None).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "get_weather");
        assert_eq!(
            normal,
            Some("I'll help you with that.  Let me check.".to_string())
        );
    }

    #[test] // PARSER.batch.6
    fn test_parse_no_arg_call() {
        let config = default_config();
        let input = r#"<|tool_calls_section_begin|><|tool_call_begin|>functions.get_current_time:0<|tool_call_argument_begin|>{}<|tool_call_end|><|tool_calls_section_end|>"#;

        let (calls, _) = try_tool_call_parse_kimi_k2(input, &config, None).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "get_current_time");

        let args: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
        assert!(args.as_object().unwrap().is_empty());
    }

    #[test] // PARSER.batch.3
    fn test_parse_no_tool_calls() {
        let config = default_config();
        let input = "This is just normal text without any tool calls.";

        let (calls, normal) = try_tool_call_parse_kimi_k2(input, &config, None).unwrap();
        assert_eq!(calls.len(), 0);
        assert_eq!(normal, Some(input.to_string()));
    }

    #[test] // PARSER.fmt.1 — function-name conventions (`functions.X` vs bare `X`)
    fn test_parse_without_functions_prefix() {
        let config = default_config();
        let input = r#"<|tool_calls_section_begin|><|tool_call_begin|>get_weather:0<|tool_call_argument_begin|>{"location":"NYC"}<|tool_call_end|><|tool_calls_section_end|>"#;

        let (calls, _) = try_tool_call_parse_kimi_k2(input, &config, None).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "get_weather");
    }

    #[test] // PARSER.batch.1 (with declared `ToolDefinition` tools provided)
    fn test_parse_with_tool_validation() {
        let config = default_config();
        let tools = vec![ToolDefinition {
            name: "get_weather".to_string(),
            parameters: Some(serde_json::json!({
                "type": "object",
                "properties": {
                    "location": {"type": "string"}
                }
            })),
        }];

        let input = r#"<|tool_calls_section_begin|><|tool_call_begin|>functions.get_weather:0<|tool_call_argument_begin|>{"location":"NYC"}<|tool_call_end|><|tool_calls_section_end|>"#;

        let (calls, _) = try_tool_call_parse_kimi_k2(input, &config, Some(&tools)).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "get_weather");
    }

    #[test] // PARSER.batch.4
    fn test_parse_truncated_json_inside_complete_fences_recovers() {
        let config = default_config();
        let input = r#"<|tool_calls_section_begin|><|tool_call_begin|>functions.get_weather:0<|tool_call_argument_begin|>{"location":"NYC<|tool_call_end|><|tool_calls_section_end|>"#;

        let (calls, _) = try_tool_call_parse_kimi_k2(input, &config, None).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "get_weather");
        assert_eq!(calls[0].function.arguments, r#"{"location":"NYC"#);
    }

    #[test] // PARSER.batch.5 (PR #8208)
    fn test_parse_malformed_no_section_end() {
        let config = default_config();
        let input = r#"<|tool_calls_section_begin|><|tool_call_begin|>functions.get_weather:0<|tool_call_argument_begin|>{"location":"NYC"}<|tool_call_end|>"#;

        let (calls, _normal) = try_tool_call_parse_kimi_k2(input, &config, None).unwrap();
        assert_eq!(
            calls.len(),
            1,
            "Should parse complete tool calls even without section_end (max_tokens truncation)"
        );
        assert_eq!(calls[0].function.name, "get_weather");
    }

    #[test] // PARSER.batch.4, PARSER.batch.5
    fn test_parse_truncated_mid_argument_no_section_end() {
        let config = default_config();
        let input = r#"<|tool_calls_section_begin|><|tool_call_begin|>functions.get_weather:0<|tool_call_argument_begin|>{"location":"NY"#;

        let (calls, normal) = try_tool_call_parse_kimi_k2(input, &config, None).unwrap();
        assert_eq!(
            calls.len(),
            0,
            "Truly truncated call (no call_end) should return 0 tool calls"
        );
        assert_eq!(normal, Some("".to_string()));
    }

    #[test] // PARSER.batch.2, PARSER.batch.5
    fn test_parse_multiple_calls_no_section_end() {
        let config = default_config();
        let input = r#"<|tool_calls_section_begin|><|tool_call_begin|>functions.get_weather:0<|tool_call_argument_begin|>{"location":"NYC"}<|tool_call_end|><|tool_call_begin|>functions.get_time:1<|tool_call_argument_begin|>{"timezone":"EST"}<|tool_call_end|>"#;

        let (calls, _) = try_tool_call_parse_kimi_k2(input, &config, None).unwrap();
        assert_eq!(
            calls.len(),
            2,
            "Should parse both complete tool calls even without section_end"
        );
        assert_eq!(calls[0].function.name, "get_weather");
        assert_eq!(calls[1].function.name, "get_time");
    }

    #[test] // PARSER.batch.2, PARSER.batch.4, PARSER.batch.5
    fn test_parse_complete_plus_truncated_no_section_end() {
        let config = default_config();
        let input = r#"<|tool_calls_section_begin|><|tool_call_begin|>functions.get_weather:0<|tool_call_argument_begin|>{"location":"NYC"}<|tool_call_end|><|tool_call_begin|>functions.get_time:1<|tool_call_argument_begin|>{"tz"#;

        let (calls, _) = try_tool_call_parse_kimi_k2(input, &config, None).unwrap();
        assert_eq!(
            calls.len(),
            1,
            "Should parse the one complete tool call, ignoring the truncated second"
        );
        assert_eq!(calls[0].function.name, "get_weather");
    }

    #[test] // PARSER.fmt.2 — whitespace tolerance
    fn test_parse_with_whitespace() {
        let config = default_config();
        let input = "<|tool_calls_section_begin|>\n<|tool_call_begin|> functions.search:0 <|tool_call_argument_begin|> {\"query\":\"rust programming\"} <|tool_call_end|>\n<|tool_calls_section_end|>";

        let (calls, _) = try_tool_call_parse_kimi_k2(input, &config, None).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "search");

        let args: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
        assert_eq!(args["query"], "rust programming");
    }

    #[test] // PARSER.batch.7
    fn test_parse_complex_json_arguments() {
        let config = default_config();
        let input = r#"<|tool_calls_section_begin|><|tool_call_begin|>functions.process_data:0<|tool_call_argument_begin|>{"items":[1,2,3],"config":{"nested":true}}<|tool_call_end|><|tool_calls_section_end|>"#;

        let (calls, _) = try_tool_call_parse_kimi_k2(input, &config, None).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "process_data");

        let args: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
        assert_eq!(args["items"], serde_json::json!([1, 2, 3]));
        assert_eq!(args["config"]["nested"], true);
    }

    #[test] // PARSER.batch.2, PARSER.batch.7
    fn test_parse_deeply_nested_json_multiple_calls() {
        let config = default_config();
        let input = r#"<|tool_calls_section_begin|><|tool_call_begin|>functions.create_config:0<|tool_call_argument_begin|>{"database":{"primary":{"host":"db1.example.com","port":5432,"options":{"ssl":true,"pool":{"min":5,"max":20}}},"replica":{"host":"db2.example.com","port":5432}},"features":["auth","logging"]}<|tool_call_end|><|tool_call_begin|>functions.deploy:1<|tool_call_argument_begin|>{"env":"production","services":[{"name":"api","replicas":3,"config":{"memory":"2Gi","cpu":"1000m"}},{"name":"worker","replicas":2,"config":{"memory":"4Gi","cpu":"2000m"}}]}<|tool_call_end|><|tool_call_begin|>functions.notify:2<|tool_call_argument_begin|>{"channels":["slack","email"],"message":"Deployment started"}<|tool_call_end|><|tool_calls_section_end|>"#;

        let (calls, normal) = try_tool_call_parse_kimi_k2(input, &config, None).unwrap();
        assert_eq!(calls.len(), 3);

        assert_eq!(calls[0].function.name, "create_config");
        assert_eq!(calls[0].id, "functions.create_config:0");
        let args0: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
        assert_eq!(args0["database"]["primary"]["options"]["pool"]["max"], 20);

        assert_eq!(calls[1].function.name, "deploy");
        assert_eq!(calls[1].id, "functions.deploy:1");
        let args1: serde_json::Value = serde_json::from_str(&calls[1].function.arguments).unwrap();
        assert_eq!(args1["services"][0]["config"]["memory"], "2Gi");

        assert_eq!(calls[2].function.name, "notify");
        assert_eq!(calls[2].id, "functions.notify:2");
        let args2: serde_json::Value = serde_json::from_str(&calls[2].function.arguments).unwrap();
        assert_eq!(args2["channels"], serde_json::json!(["slack", "email"]));

        assert_eq!(normal, Some("".to_string()));
    }

    #[test] // helper, PARSER.fmt.3 — detection helper, singular section-token variant
    fn test_detect_singular_section_start() {
        let config = default_config();
        assert!(detect_tool_call_start_kimi_k2(
            "<|tool_call_section_begin|>",
            &config
        ));
        assert!(detect_tool_call_start_kimi_k2(
            "text <|tool_call_section_b",
            &config
        ));
    }

    #[test] // PARSER.fmt.3 — singular section-token variant
    fn test_parse_with_singular_section_tokens() {
        let config = default_config();
        let input = r#"<|tool_call_section_begin|><|tool_call_begin|>functions.get_weather:0<|tool_call_argument_begin|>{"location":"NYC"}<|tool_call_end|><|tool_call_section_end|>"#;

        let (calls, normal) = try_tool_call_parse_kimi_k2(input, &config, None).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "get_weather");
        assert_eq!(normal, Some("".to_string()));
    }

    #[test] // helper, PARSER.fmt.3 — detection helper, singular section-token variant
    fn test_find_end_position_singular_variant() {
        let config = default_config();
        let text = "<|tool_call_section_begin|><|tool_call_begin|>functions.test:0<|tool_call_argument_begin|>{}<|tool_call_end|><|tool_call_section_end|>more text";
        let pos = find_tool_call_end_position_kimi_k2(text, &config);
        assert_eq!(&text[pos.unwrap()..], "more text");
    }


    #[test] // PARSER.batch.4
    fn test_parse_invalid_json_falls_back_to_raw_string() {
        let config = default_config();
        let input = r#"<|tool_calls_section_begin|><|tool_call_begin|>functions.get_weather:0<|tool_call_argument_begin|>{invalid json here}<|tool_call_end|><|tool_calls_section_end|>"#;

        let (calls, _) = try_tool_call_parse_kimi_k2(input, &config, None).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "get_weather");
        assert_eq!(calls[0].function.arguments, "{invalid json here}");
    }

    #[test] // PARSER.fmt.1 — function-name conventions (ID regex validation)
    fn test_parse_invalid_function_id_rejected_by_regex() {
        let config = default_config();

        let input1 = r#"<|tool_calls_section_begin|><|tool_call_begin|>just_a_name<|tool_call_argument_begin|>{"key":"val"}<|tool_call_end|><|tool_calls_section_end|>"#;
        let (calls, _) = try_tool_call_parse_kimi_k2(input1, &config, None).unwrap();
        assert_eq!(calls.len(), 0, "ID without :digit should be rejected");

        let input2 = r#"<|tool_calls_section_begin|><|tool_call_begin|>functions.get_weather:abc<|tool_call_argument_begin|>{"key":"val"}<|tool_call_end|><|tool_calls_section_end|>"#;
        let (calls, _) = try_tool_call_parse_kimi_k2(input2, &config, None).unwrap();
        assert_eq!(calls.len(), 0, "ID with :non-digit should be rejected");

        let input3 = r#"<|tool_calls_section_begin|><|tool_call_begin|>:::0<|tool_call_argument_begin|>{"key":"val"}<|tool_call_end|><|tool_calls_section_end|>"#;
        let (calls, _) = try_tool_call_parse_kimi_k2(input3, &config, None).unwrap();
        assert_eq!(calls.len(), 0, "Garbage ID should be rejected");

        let input4 = r#"<|tool_calls_section_begin|><|tool_call_begin|>no_colon<|tool_call_argument_begin|>{"a":"b"}<|tool_call_end|><|tool_call_begin|>functions.valid:0<|tool_call_argument_begin|>{"x":"y"}<|tool_call_end|><|tool_calls_section_end|>"#;
        let (calls, _) = try_tool_call_parse_kimi_k2(input4, &config, None).unwrap();
        assert_eq!(calls.len(), 1, "Only valid call should be extracted");
        assert_eq!(calls[0].function.name, "valid");
    }

    #[test] // PARSER.batch.7 — special characters in arg values
    fn test_parse_angle_brackets_in_json_arguments() {
        let config = default_config();
        let input = r#"<|tool_calls_section_begin|><|tool_call_begin|>functions.render_html:0<|tool_call_argument_begin|>{"template":"<div class=\"main\"><h1>Title</h1><p>Content</p></div>","format":"html"}<|tool_call_end|><|tool_calls_section_end|>"#;

        let (calls, _) = try_tool_call_parse_kimi_k2(input, &config, None).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "render_html");

        let args: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
        assert!(args["template"].as_str().unwrap().contains("<div"));
        assert!(args["template"].as_str().unwrap().contains("</div>"));
        assert_eq!(args["format"], "html");
    }

    #[test] // PARSER.batch.2 — parallel calls, zero-spacing edge case
    fn test_parse_three_concatenated_calls_no_spacing() {
        let config = default_config();
        let input = "<|tool_calls_section_begin|>\
            <|tool_call_begin|>functions.search:0<|tool_call_argument_begin|>{\"q\":\"rust\"}<|tool_call_end|>\
            <|tool_call_begin|>functions.search:1<|tool_call_argument_begin|>{\"q\":\"python\"}<|tool_call_end|>\
            <|tool_call_begin|>functions.search:2<|tool_call_argument_begin|>{\"q\":\"go\"}<|tool_call_end|>\
            <|tool_calls_section_end|>";

        let (calls, normal) = try_tool_call_parse_kimi_k2(input, &config, None).unwrap();
        assert_eq!(calls.len(), 3);
        assert_eq!(calls[0].function.name, "search");
        assert_eq!(calls[0].id, "functions.search:0");
        assert_eq!(calls[1].function.name, "search");
        assert_eq!(calls[1].id, "functions.search:1");
        assert_eq!(calls[2].function.name, "search");
        assert_eq!(calls[2].id, "functions.search:2");

        let a0: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
        let a1: serde_json::Value = serde_json::from_str(&calls[1].function.arguments).unwrap();
        let a2: serde_json::Value = serde_json::from_str(&calls[2].function.arguments).unwrap();
        assert_eq!(a0["q"], "rust");
        assert_eq!(a1["q"], "python");
        assert_eq!(a2["q"], "go");
        assert_eq!(normal, Some("".to_string()));
    }

    #[test] // PARSER.batch.7 — newlines in arg values
    fn test_parse_newlines_in_json_arguments() {
        let config = default_config();
        let input = "<|tool_calls_section_begin|><|tool_call_begin|>functions.create_user:0<|tool_call_argument_begin|>{\n  \"name\": \"John Doe\",\n  \"address\": {\n    \"street\": \"123 Main St\",\n    \"city\": \"Springfield\"\n  },\n  \"tags\": [\n    \"admin\",\n    \"active\"\n  ]\n}<|tool_call_end|><|tool_calls_section_end|>";

        let (calls, _) = try_tool_call_parse_kimi_k2(input, &config, None).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "create_user");

        let args: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
        assert_eq!(args["name"], "John Doe");
        assert_eq!(args["address"]["city"], "Springfield");
        assert_eq!(args["tags"], serde_json::json!(["admin", "active"]));
    }

    #[test] // PARSER.fmt.4 — empty wrapper (start+end with no calls between)
    fn test_parse_empty_tool_section() {
        let config = default_config();
        let input = "Here is my answer. <|tool_calls_section_begin|><|tool_calls_section_end|> And more text.";

        let (calls, normal) = try_tool_call_parse_kimi_k2(input, &config, None).unwrap();
        assert_eq!(calls.len(), 0, "Empty section should produce no tool calls");
        assert_eq!(
            normal,
            Some("Here is my answer.  And more text.".to_string()),
            "Text around empty section should be preserved"
        );
    }

    #[rstest] // PARSER.fmt.1 — function-name conventions (hyphens, double-underscores)
    #[case(
        r#"<|tool_calls_section_begin|><|tool_call_begin|>functions.list-tasklists:0<|tool_call_argument_begin|>{}<|tool_call_end|><|tool_calls_section_end|>"#,
        "list-tasklists",
        "functions.list-tasklists:0"
    )]
    #[case(
        r#"<|tool_calls_section_begin|><|tool_call_begin|>functions.mcp__portal__search-documents:3<|tool_call_argument_begin|>{}<|tool_call_end|><|tool_calls_section_end|>"#,
        "mcp__portal__search-documents",
        "functions.mcp__portal__search-documents:3"
    )]
    #[case(
        r#"<|tool_calls_section_begin|><|tool_call_begin|>functions.gtasks_list-tasklists:0<|tool_call_argument_begin|>{}<|tool_call_end|><|tool_calls_section_end|>"#,
        "gtasks_list-tasklists",
        "functions.gtasks_list-tasklists:0"
    )]
    fn test_parse_names_with_hyphens(#[case] input: &str, #[case] name: &str, #[case] id: &str) {
        let config = default_config();
        let (calls, _normal) = try_tool_call_parse_kimi_k2(input, &config, None).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, name);
        assert_eq!(calls[0].id, id);
    }

    ///
    #[test] // PARSER.batch.4
    fn test_parse_missing_call_end_inside_complete_section_silent_drop() {
        let config = default_config();
        let input = r#"<|tool_calls_section_begin|><|tool_call_begin|>functions.get_weather:0<|tool_call_argument_begin|>{"location":"NYC"}<|tool_calls_section_end|>"#;

        let (calls, _) = try_tool_call_parse_kimi_k2(input, &config, None).unwrap();
        assert_eq!(
            calls.len(),
            0,
            "Missing per-call <|tool_call_end|> drops the call even when \
             section fences are complete"
        );
    }

    ///
    #[test] // PARSER.batch.2, PARSER.batch.4
    fn test_parse_middle_call_missing_end_corrupts_next() {
        let config = default_config();
        let input = r#"<|tool_calls_section_begin|><|tool_call_begin|>functions.a:0<|tool_call_argument_begin|>{"x":"1"}<|tool_call_end|><|tool_call_begin|>functions.b:1<|tool_call_argument_begin|>{"y":"2"}<|tool_call_begin|>functions.c:2<|tool_call_argument_begin|>{"z":"3"}<|tool_call_end|><|tool_calls_section_end|>"#;

        let (calls, _) = try_tool_call_parse_kimi_k2(input, &config, None).unwrap();
        assert_eq!(calls.len(), 2, "A and corrupted-B; C is consumed by B");
        assert_eq!(calls[0].function.name, "a");
        assert_eq!(calls[0].function.arguments, r#"{"x":"1"}"#);
        assert_eq!(calls[1].function.name, "b");
        assert!(
            calls[1].function.arguments.contains("functions.c:2"),
            "BUG: B's args swallowed C's markup verbatim; got {}",
            calls[1].function.arguments
        );
    }

    #[test]
    fn test_parser_does_not_filter_by_tool_choice() {
        let config = default_config();
        let input = r#"<|tool_calls_section_begin|><|tool_call_begin|>functions.get_weather:0<|tool_call_argument_begin|>{"location":"NYC"}<|tool_call_end|><|tool_call_begin|>functions.get_time:1<|tool_call_argument_begin|>{"timezone":"EST"}<|tool_call_end|><|tool_calls_section_end|>"#;
        let (calls, _) = try_tool_call_parse_kimi_k2(input, &config, None).unwrap();
        assert_eq!(calls.len(), 2);
    }

    #[test]
    fn test_parser_output_independent_of_upstream_finish() {
        let config = default_config();
        let stop_input = r#"<|tool_calls_section_begin|><|tool_call_begin|>functions.get_weather:0<|tool_call_argument_begin|>{"location":"NYC"}<|tool_call_end|><|tool_calls_section_end|>"#;
        let (calls_stop, _) = try_tool_call_parse_kimi_k2(stop_input, &config, None).unwrap();
        assert_eq!(calls_stop.len(), 1);
    }

    #[test] // PARSER.batch.9
    fn test_parse_empty_and_whitespace_inputs() {
        let config = default_config();
        for input in &["", " ", "\n", "\t\n  \t"] {
            let (calls, normal) = try_tool_call_parse_kimi_k2(input, &config, None).unwrap();
            assert!(
                calls.is_empty(),
                "Empty/whitespace input must yield no calls (input={:?})",
                input
            );
            assert_eq!(
                normal.as_deref(),
                Some(""),
                "Empty/whitespace input collapses to empty normal_text"
            );
        }
    }

    #[test] // PARSER.batch.10
    fn test_parse_duplicate_calls_same_name() {
        let config = default_config();
        let input = r#"<|tool_calls_section_begin|><|tool_call_begin|>functions.get_weather:0<|tool_call_argument_begin|>{"location":"NYC"}<|tool_call_end|><|tool_call_begin|>functions.get_weather:1<|tool_call_argument_begin|>{"location":"LA"}<|tool_call_end|><|tool_calls_section_end|>"#;
        let (calls, _) = try_tool_call_parse_kimi_k2(input, &config, None).unwrap();
        assert_eq!(calls.len(), 2, "Both duplicate-name calls must be returned");
        assert_eq!(calls[0].function.name, "get_weather");
        assert_eq!(calls[1].function.name, "get_weather");
        assert_ne!(
            calls[0].id, calls[1].id,
            "Duplicate calls must have distinct ids"
        );
        let args0: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
        let args1: serde_json::Value = serde_json::from_str(&calls[1].function.arguments).unwrap();
        assert_eq!(args0["location"], "NYC");
        assert_eq!(args1["location"], "LA");
    }
}
