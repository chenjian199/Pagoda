// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-License-Identifier: Apache-2.0
//
// API-compatible implementation based on the public interfaces and behavioral
// contracts of NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
// Implementation rewritten by PAGODA.

//!
//! ## 设计意图
//!
//! ## 外部契约
//!
//! ## 实现要点

use std::collections::HashMap;

use regex::RegexBuilder;
use serde_json::Value;
use uuid::Uuid;

use super::super::ToolDefinition;
use super::config::JsonParserConfig;
use super::response::{CalledFunction, ToolCallResponse, ToolCallType};


#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CalledFunctionParameters {
    pub name: String,
    pub parameters: HashMap<String, Value>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CalledFunctionArguments {
    pub name: String,
    pub arguments: HashMap<String, Value>,
}


fn extract_tool_call_content(input: &str, start_token: &str, end_token: &str) -> Option<String> {
    let pattern = format!(
        r"{}(.*?){}",
        regex::escape(start_token),
        regex::escape(end_token)
    );
    let regex = RegexBuilder::new(&pattern)
        .dot_matches_new_line(true)
        .build()
        .ok()?;

    let matches: Vec<String> = regex
        .captures_iter(input)
        .filter_map(|captures| captures.get(1))
        .map(|m| m.as_str().trim().to_string())
        .collect();

    match matches.as_slice() {
        [] => None,
        [single] => Some(single.clone()),
        many => Some(format!("[{}]", many.join(","))),
    }
}

fn extract_tool_call_content_eof_recovery(input: &str, start_token: &str) -> Option<String> {
    let start_pos = input.find(start_token)?;
    let tail = input[start_pos + start_token.len()..].trim();
    (tail.starts_with('{') || tail.starts_with('[')).then(|| tail.to_string())
}

fn handle_single_token_tool_calls(input: &str, start_token: &str) -> Option<String> {
    if !input.contains(start_token) {
        return None;
    }

    let mut items: Vec<String> = Vec::new();
    for segment in input.split(start_token) {
        let seg = segment.trim();
        if seg.is_empty() {
            continue;
        }

        if seg.starts_with('{') {
            // 单对象：截到最后一个 '}'，校验后保留
            if let Some(pos) = seg.rfind('}') {
                let candidate = seg[..=pos].trim();
                if serde_json::from_str::<Value>(candidate).is_ok() {
                    items.push(candidate.to_string());
                }
            }
        } else if seg.starts_with('[') {
            if let Some(pos) = seg.rfind(']') {
                let candidate = seg[..=pos].trim();
                if let Ok(Value::Array(arr)) = serde_json::from_str::<Value>(candidate) {
                    for item in arr {
                        if let Ok(item_str) = serde_json::to_string(&item) {
                            items.push(item_str);
                        }
                    }
                }
            }
        }
    }

    if items.is_empty() {
        return Some(String::new());
    }
    Some(format!("[{}]", items.join(",")))
}

pub(crate) fn try_repair_truncated_json(s: &str) -> Option<String> {
    let mut closers: Vec<char> = Vec::new();
    let mut in_string = false;
    let mut escape = false;

    for c in s.chars() {
        if escape {
            // 上一字符是反斜杠，本字符被转义，跳过
            escape = false;
            continue;
        }
        if in_string {
            match c {
                '\\' => escape = true,
                '"' => in_string = false,
                _ => {}
            }
            continue;
        }
        match c {
            '"' => in_string = true,
            '{' => closers.push('}'),
            '[' => closers.push(']'),
            '}' | ']' => {
                closers.pop();
            }
            _ => {}
        }
    }

    // 已经平衡且无悬挂字符串/转义，无需修复
    if !escape && !in_string && closers.is_empty() {
        return None;
    }

    let mut repaired = s.to_string();
    if escape {
        repaired.push('\\');
    }
    if in_string {
        repaired.push('"');
    }
    while let Some(closer) = closers.pop() {
        repaired.push(closer);
    }
    Some(repaired)
}

fn try_parse_normal_text(input: &str, start_token: &str) -> String {
    match input.find(start_token) {
        Some(idx) => input[..idx].trim().to_string(),
        None => String::new(),
    }
}


fn make_tool_call(name: String, arguments: HashMap<String, Value>) -> anyhow::Result<ToolCallResponse> {
    Ok(ToolCallResponse {
        id: format!("call-{}", Uuid::new_v4()),
        tp: ToolCallType::Function,
        function: CalledFunction {
            name,
            arguments: serde_json::to_string(&arguments)?,
        },
    })
}

fn build_calls_from_json(json: &str) -> anyhow::Result<Option<Vec<ToolCallResponse>>> {
    if let Ok(single) = serde_json::from_str::<CalledFunctionParameters>(json) {
        return Ok(Some(vec![make_tool_call(single.name, single.parameters)?]));
    }
    if let Ok(single) = serde_json::from_str::<CalledFunctionArguments>(json) {
        return Ok(Some(vec![make_tool_call(single.name, single.arguments)?]));
    }
    if let Ok(array) = serde_json::from_str::<Vec<Value>>(json) {
        let mut calls = Vec::new();
        for item in array {
            if let Ok(func) = serde_json::from_value::<CalledFunctionArguments>(item.clone()) {
                calls.push(make_tool_call(func.name, func.arguments)?);
            } else if let Ok(func) = serde_json::from_value::<CalledFunctionParameters>(item) {
                calls.push(make_tool_call(func.name, func.parameters)?);
            }
            // 非法条目静默跳过
        }
        return Ok(Some(calls));
    }
    Ok(None)
}
/// 解析成功时给出工具调用列表与剥离后的普通文本。
pub fn try_tool_call_parse_basic_json(
    message: &str,
    config: &JsonParserConfig,
    _tools: Option<&[ToolDefinition]>,
) -> anyhow::Result<(Vec<ToolCallResponse>, Option<String>)> {
    tracing::debug!("Using JSON parser config: {:?}", config);
    let trimmed = message.trim();

    // 空输入直接返回
    if trimmed.is_empty() {
        return Ok((vec![], Some(String::new())));
    }

    let tool_call_start_tokens = &config.tool_call_start_tokens;
    let tool_call_end_tokens = &config.tool_call_end_tokens;

    if tool_call_start_tokens.is_empty() && !config.bare_json_mode {
        return Ok((vec![], Some(trimmed.to_string())));
    }

    // 假设单条消息只使用一种标签；遍历标签组合是为了默认支持多模型
    let mut json = trimmed.to_string();
    let mut normal_text = trimmed.to_string();
    let mut found_start_token_with_no_valid_json = false;

    let has_start_token = !config.bare_json_mode
        && tool_call_start_tokens
            .iter()
            .any(|token| !token.is_empty() && normal_text.contains(token));

    if !has_start_token {
        if let Some(idx) = normal_text.find(['{', '[']) {
            let extracted_normal = normal_text[..idx].trim().to_string();
            let extracted_json = normal_text[idx..].trim().to_string();
            if !extracted_json.is_empty() {
                normal_text = extracted_normal;
                json = extracted_json;
            }
        }
    } else {
        'outer: for start_token in tool_call_start_tokens.iter() {
            for end_token in tool_call_end_tokens.iter() {
                let new_normal_text = try_parse_normal_text(&normal_text, start_token);

                let result = match (start_token.is_empty(), end_token.is_empty()) {
                    (false, true) => handle_single_token_tool_calls(&json, start_token),
                    (false, false) => {
                        let mut content =
                            extract_tool_call_content(&json, start_token, end_token);
                        if content.is_none()
                            && config.allow_eof_recovery
                            && json.contains(start_token.as_str())
                        {
                            content =
                                extract_tool_call_content_eof_recovery(&json, start_token);
                        }
                        content
                    }
                    // 其余组合跳过
                    _ => None,
                };

                if let Some(content) = result {
                    if content.is_empty() {
                        found_start_token_with_no_valid_json = true;
                    }
                    json = content;
                    normal_text = new_normal_text;
                    break 'outer;
                }
            }
        }
    }

    let json = json.as_str();

    if let Some(calls) = build_calls_from_json(json)? {
        return Ok((calls, Some(normal_text)));
    }

    if config.allow_eof_recovery
        && let Some(repaired) = try_repair_truncated_json(json)
        && let Some(calls) = build_calls_from_json(&repaired)?
        && !calls.is_empty()
    {
        return Ok((calls, Some(normal_text)));
    }

    if found_start_token_with_no_valid_json {
        Ok((vec![], Some(String::new())))
    } else {
        Ok((vec![], Some(trimmed.to_string())))
    }
}


pub fn detect_tool_call_start_basic_json(chunk: &str, config: &JsonParserConfig) -> bool {
    let trimmed = chunk.trim();
    if trimmed.is_empty() {
        return false;
    }

    let contains_complete_token = config
        .tool_call_start_tokens
        .iter()
        .any(|token| !token.is_empty() && trimmed.contains(token));

    if contains_complete_token {
        return true;
    }

    let has_partial_token = config.tool_call_start_tokens.iter().any(|token| {
        if token.is_empty() {
            return false;
        }
        token.char_indices().any(|(byte_idx, ch)| {
            let prefix_str = &token[..byte_idx + ch.len_utf8()];
            // 完整等于某前缀
            if trimmed == prefix_str {
                return true;
            }
            // 长前缀（>=3 字节）允许出现在任意位置
            if prefix_str.len() >= 3 && trimmed.contains(prefix_str) {
                return true;
            }
            // 短前缀仅在结尾匹配（流式场景）
            if prefix_str.len() < 3 && trimmed.ends_with(prefix_str) {
                return true;
            }
            false
        })
    });

    has_partial_token || trimmed.contains('{') || trimmed.contains('[')
}

#[cfg(test)]
mod tests {
    //! ## 测试过程
    //!
    //! ## 意义
    //! 验证解析器在批量与流式两种场景下对各家模型工具调用格式的兼容性，并确保
    //! 截断恢复与误报容忍策略符合外部契约。

    use super::*;

    fn config_with(start: &[&str], end: &[&str]) -> JsonParserConfig {
        JsonParserConfig {
            tool_call_start_tokens: start.iter().map(|s| s.to_string()).collect(),
            tool_call_end_tokens: end.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        }
    }

    // === 截断修复 ===

    #[test]
    fn repair_eof_after_backslash_yields_valid_json() {
        // 追加的 `"` 会被反斜杠转义，修复后仍非法。
        let repaired = try_repair_truncated_json(r#"{"k":"a\"#).expect("must repair");
        assert!(
            serde_json::from_str::<Value>(&repaired).is_ok(),
            "repaired must parse: {:?}",
            repaired
        );
    }

    #[test]
    fn repair_returns_none_for_balanced_json() {
        assert!(try_repair_truncated_json(r#"{"k":"v"}"#).is_none());
    }

    #[test]
    fn repair_closes_open_braces_and_brackets() {
        let repaired = try_repair_truncated_json(r#"[{"name":"a","arguments":{"x":1"#)
            .expect("must repair");
        assert!(serde_json::from_str::<Value>(&repaired).is_ok());
    }

    // === 起始探测 ===

    #[test]
    fn detect_complete_and_partial_start_tokens() {
        let cases: &[(&str, &str, &str, bool)] = &[
            (
                r#"<tool_call>{"name": "search"}</tool_call>"#,
                "<tool_call>",
                "</tool_call>",
                true,
            ),
            (
                r#"<TOOLCALL>[{"name": "search"}]</TOOLCALL>"#,
                "<TOOLCALL>",
                "</TOOLCALL>",
                true,
            ),
            (r#"<|python_tag|>{ "name": }"#, "<|python_tag|>", "", true),
            (
                r#"Hello Yo ! [TOOL_CALLS]{"name": "search", "#,
                "[TOOL_CALLS]",
                "",
                true,
            ),
            (r#"functools{"name": "search", "#, "functools", "", true),
            (r#"{"name": "search"}"#, "<tool_call>", "</tool_call>", true),
            (r#"Here it is {"name": "#, "<tool_call>", "</tool_call>", true),
            (
                r#"Here it is [{"name": "search","#,
                "<tool_call>",
                "</tool_call>",
                true,
            ),
            // 流式误报可接受
            (r#"Here it is { Whats up"#, "<tool_call>", "</tool_call>", true),
            (r#"fun"#, "functools", "", true),
            (r#"func"#, "functools", "", true),
            (r#"f"#, "functools", "", true),
            (r#"Hello fun"#, "functools", "", true),
            (r#"funny joke"#, "functools", "", true),
            // 无关文本不应命中
            (r#"hello world"#, "functools", "", false),
        ];

        for (text, start, end, expected) in cases {
            let config = config_with(&[start], &[end]);
            assert_eq!(
                detect_tool_call_start_basic_json(text, &config),
                *expected,
                "detect mismatch for input {:?} with token {:?}",
                text,
                start
            );
        }
    }

    // === 顶层解析 ===

    #[test]
    fn parse_single_object_with_parameters() {
        let config = config_with(&["<TOOLCALL>"], &["</TOOLCALL>"]);
        let input = r#"<TOOLCALL>{"name": "search", "parameters": {"query": "rust"}}</TOOLCALL>"#;
        let (calls, _normal) = try_tool_call_parse_basic_json(input, &config, None).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "search");
        assert!(calls[0].id.starts_with("call-"));
        assert!(calls[0].function.arguments.contains("rust"));
    }

    #[test]
    fn parse_array_with_arguments() {
        let config = config_with(&["<TOOLCALL>"], &["</TOOLCALL>"]);
        let input = r#"<TOOLCALL>[{"name": "a", "arguments": {}}, {"name": "b", "arguments": {}}]</TOOLCALL>"#;
        let (calls, _normal) = try_tool_call_parse_basic_json(input, &config, None).unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].function.name, "a");
        assert_eq!(calls[1].function.name, "b");
    }

    #[test]
    fn parse_empty_input_returns_no_calls() {
        let config = config_with(&["<TOOLCALL>"], &["</TOOLCALL>"]);
        let (calls, normal) = try_tool_call_parse_basic_json("   ", &config, None).unwrap();
        assert!(calls.is_empty());
        assert_eq!(normal.as_deref(), Some(""));
    }

    #[test]
    fn parse_eof_recovery_repairs_truncated_object() {
        let mut config = config_with(&["<TOOLCALL>"], &["</TOOLCALL>"]);
        config.allow_eof_recovery = true;
        let input = r#"<TOOLCALL>{"name": "search", "arguments": {"query": "rust""#;
        let (calls, _normal) = try_tool_call_parse_basic_json(input, &config, None).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "search");
    }

    #[test]
    fn parse_start_token_without_valid_json_is_suppressed() {
        let config = config_with(&["functools"], &[""]);
        let input = r#"functools not-json-here"#;
        let (calls, normal) = try_tool_call_parse_basic_json(input, &config, None).unwrap();
        assert!(calls.is_empty());
        assert_eq!(normal.as_deref(), Some(""));
    }
}

