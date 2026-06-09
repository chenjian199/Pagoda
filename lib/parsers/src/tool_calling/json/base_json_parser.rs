// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # tool_calling::json::base_json_parser
//!
//! ## 设计意图
//! 通用 JSON 工具调用解析器：从模型输出中抽取被起止 token 包裹（或裸露）的 JSON 载荷，
//! 兼容 `parameters` 与 `arguments` 两种字段命名，并把单对象 / 对象数组统一规整为
//! [`ToolCallResponse`] 列表。
//!
//! ## 外部契约
//! - `try_tool_call_parse_basic_json`：返回 `(Vec<ToolCallResponse>, Option<String>)`，
//!   id 形如 `call-<uuid>`，arguments 为 JSON 字符串。
//! - `detect_tool_call_start_basic_json`：在完整或部分起始 token、裸 JSON 前缀场景下返回 true。
//! - `try_repair_truncated_json` 为 crate 内部可见的截断修复助手。
//! - `CalledFunctionParameters` / `CalledFunctionArguments` 为公共 serde 结构。
//!
//! ## 实现要点
//! - 抽取阶段与反序列化阶段解耦：先得到候选 JSON 字符串，再统一走三形态解析助手。
//! - EOF 恢复仅在 `allow_eof_recovery` 显式开启时启用，避免流式 jail 过早判定完成。

use std::collections::HashMap;

use regex::RegexBuilder;
use serde_json::Value;
use uuid::Uuid;

use super::super::ToolDefinition;
use super::config::JsonParserConfig;
use super::response::{CalledFunction, ToolCallResponse, ToolCallType};

// === SECTION: 公共 serde 载荷结构 ===

// 与 CalledFunction 同形，使用 parameters 字段名
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CalledFunctionParameters {
    pub name: String,
    pub parameters: HashMap<String, Value>,
}

// 与 CalledFunction 同形，使用 arguments 字段名
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CalledFunctionArguments {
    pub name: String,
    pub arguments: HashMap<String, Value>,
}

// === SECTION: 候选 JSON 抽取 ===

/// 用正则抽取起止 token 之间的内容。单个匹配直接返回；多个匹配拼成 JSON 数组字符串。
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

    // 取出全部捕获组并去除首尾空白。TODO: Handle multiple tool calls
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

/// EOF 作结束 token 恢复——仅限收尾路径使用。在外部结束 token 从未到达时，
/// 返回 `start_token` 之后的 JSON 形态尾部。受 `JsonParserConfig::allow_eof_recovery` 控制，
/// 使流式早退不会在结束 token 出现前中途触发。
fn extract_tool_call_content_eof_recovery(input: &str, start_token: &str) -> Option<String> {
    let start_pos = input.find(start_token)?;
    let tail = input[start_pos + start_token.len()..].trim();
    (tail.starts_with('{') || tail.starts_with('[')).then(|| tail.to_string())
}

// 特例：`<|python_tag|>`。正则模式对此标记效果不佳（它无结束 token）。
// 处理 `<|python_tag|>` 这类单一起始 token 的单次与多次工具调用。
fn handle_single_token_tool_calls(input: &str, start_token: &str) -> Option<String> {
    // 不含起始 token 直接放弃
    if !input.contains(start_token) {
        return None;
    }

    let mut items: Vec<String> = Vec::new();
    // 按起始 token 切分，逐段挑出形似 JSON 的片段
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
            // 数组形态（如 phi4: functools[{...}]）：校验后拆出每个元素
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
        // 找到了起始 token 却无有效 JSON：返回空串，避免把非法内容（phi4 等）泄漏出去
        return Some(String::new());
    }
    Some(format!("[{}]", items.join(",")))
}

/// 尝试修复因 max_tokens / EOS 截断的 JSON。遍历输入并跟踪字符串状态与
/// 花括号/方括号嵌套层级；在 EOF 处闭合所有打开的字符串并补齐未闭合的配对符号。
/// 仅在至少需要追加一个闭合符时才返回 `Some(repaired)`（避免对已合法的 JSON 做无意义处理）。
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
    // EOF 中遇到转义序列：将尾部的 `\` 与另一个 `\` 配对，
    // 使后续追加的闭合引号不会被错误转义。
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

/// 抽取起始 token 之前的普通文本；未发现起始 token 则返回空串。
fn try_parse_normal_text(input: &str, start_token: &str) -> String {
    match input.find(start_token) {
        Some(idx) => input[..idx].trim().to_string(),
        None => String::new(),
    }
}

// === SECTION: 三形态反序列化 ===

/// 构造单个 [`ToolCallResponse`]，arguments 序列化为 JSON 字符串。
fn make_tool_call(name: String, arguments: HashMap<String, Value>) -> anyhow::Result<ToolCallResponse> {
    Ok(ToolCallResponse {
        id: format!("call-{}", Uuid::new_v4()),
        tp: ToolCallType::Function,
        function: CalledFunction {
            name,
            // 保留内嵌 JSON 字符串原样；不进行双重转义。
            arguments: serde_json::to_string(&arguments)?,
        },
    })
}

/// 依次尝试把候选 JSON 解析为：单 `{name, parameters}`、单 `{name, arguments}`、
/// 或对象数组（逐项兼容两种字段名）。三者皆不匹配返回 `Ok(None)`。
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

// === SECTION: 顶层解析入口 ===

/// 把一段原始 LLM 文本解析成统一的 [`ToolCallResponse`] 列表。
///
/// 兼容多种包裹格式（`<TOOLCALL>[...]</TOOLCALL>`、`<|python_tag|>...`）以及裸 JSON，
/// 字段名支持 `parameters` 或 `arguments`。
///
/// 返回值为 `(tool_calls, normal_text)`：
/// - 解析成功时给出工具调用列表与剥离后的普通文本；
/// - 找到起始 token 却无有效 JSON 时返回空列表与空文本，避免泄漏非法内容；
/// - 仅当内部 `serde_json::to_string(...)` 失败时返回 `Err`。
///
/// # Examples
///
/// ```ignore
/// let input = r#"<TOOLCALL>[{ "name": "search", "parameters": { "query": "rust" } }]</TOOLCALL>"#;
/// let result = try_tool_call_parse_json(input)?;
/// assert!(result.is_some());
/// ```
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

    // 未配置任何 token 且非 bare_json_mode：原样返回为普通文本
    if tool_call_start_tokens.is_empty() && !config.bare_json_mode {
        return Ok((vec![], Some(trimmed.to_string())));
    }

    // 假设单条消息只使用一种标签；遍历标签组合是为了默认支持多模型
    let mut json = trimmed.to_string();
    let mut normal_text = trimmed.to_string();
    let mut found_start_token_with_no_valid_json = false;

    // bare_json_mode 强制走无标记分支
    let has_start_token = !config.bare_json_mode
        && tool_call_start_tokens
            .iter()
            .any(|token| !token.is_empty() && normal_text.contains(token));

    if !has_start_token {
        // 无起始 token：把首个 '{' 或 '[' 之后的内容视为潜在 JSON
        if let Some(idx) = normal_text.find(['{', '[']) {
            let extracted_normal = normal_text[..idx].trim().to_string();
            let extracted_json = normal_text[idx..].trim().to_string();
            if !extracted_json.is_empty() {
                normal_text = extracted_normal;
                json = extracted_json;
            }
        }
    } else {
        // 有起始 token：遍历起止 token 组合做正则抽取
        'outer: for start_token in tool_call_start_tokens.iter() {
            for end_token in tool_call_end_tokens.iter() {
                let new_normal_text = try_parse_normal_text(&normal_text, start_token);

                let result = match (start_token.is_empty(), end_token.is_empty()) {
                    // 单 token 形态（如 <|python_tag|>）
                    (false, true) => handle_single_token_tool_calls(&json, start_token),
                    // 起止 token 成对形态
                    (false, false) => {
                        let mut content =
                            extract_tool_call_content(&json, start_token, end_token);
                        // EOF 恢复仅在 finalize 路径（allow_eof_recovery）启用，
                        // 流式 jail 不会在 end-token 到达前判定完成
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
                    // 找到起始 token 却得到空 JSON，记录标记
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

    // 第一轮：直接对候选 JSON 做三形态解析
    if let Some(calls) = build_calls_from_json(json)? {
        return Ok((calls, Some(normal_text)));
    }

    // 截断恢复：补齐未闭合的字符串/括号（常见 max_tokens / EOS 截断），再重试一次。
    // 仅在 allow_eof_recovery 开启时生效，避免流式 jail 在模型仍在输出时误判完成。
    if config.allow_eof_recovery
        && let Some(repaired) = try_repair_truncated_json(json)
        && let Some(calls) = build_calls_from_json(&repaired)?
        && !calls.is_empty()
    {
        return Ok((calls, Some(normal_text)));
    }

    // 找到起始 token 但无有效 JSON：返回空内容，避免泄漏 token 与非法内容
    if found_start_token_with_no_valid_json {
        Ok((vec![], Some(String::new())))
    } else {
        Ok((vec![], Some(trimmed.to_string())))
    }
}

// === SECTION: 起始探测（流式） ===

pub fn detect_tool_call_start_basic_json(chunk: &str, config: &JsonParserConfig) -> bool {
    let trimmed = chunk.trim();
    if trimmed.is_empty() {
        return false;
    }

    // 命中任意完整起始 token 直接判定
    let contains_complete_token = config
        .tool_call_start_tokens
        .iter()
        .any(|token| !token.is_empty() && trimmed.contains(token));

    if contains_complete_token {
        return true;
    }

    // 部分起始 token（流式分块）：起始 token 可能被拆分到多个 chunk
    let has_partial_token = config.tool_call_start_tokens.iter().any(|token| {
        if token.is_empty() {
            return false;
        }
        // 逐字符长度构造前缀，正确处理 Unicode 边界
        token.char_indices().any(|(byte_idx, ch)| {
            // 取前缀 token[..byte_idx + ch.len()]
            let prefix_str = &token[..byte_idx + ch.len_utf8()];
            // 完整等于某前缀
            if trimmed == prefix_str {
                return true;
            }
            // 长前缀（>=3 字节）允许出现在任意位置
            // 让 "funny joke" 经由 "fun" 命中 "functools"
            // 但阻止 "<tool_call>" 经由单字符 "<" 命中 "<TOOLCALL>"
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
    //! 围绕三个公共行为构建：截断 JSON 修复、起始探测（完整/部分 token、裸 JSON、误报）、
    //! 以及顶层解析对 parameters/arguments、单对象/数组、EOF 恢复的处理。配置经 helper 构造，
    //! 探测用例以表驱动覆盖各模型的 token 形态。
    //!
    //! ## 意义
    //! 验证解析器在批量与流式两种场景下对各家模型工具调用格式的兼容性，并确保
    //! 截断恢复与误报容忍策略符合外部契约。

    use super::*;

    /// 构造一份带起止 token 的 JSON 解析配置。
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
        // EOF 出现在转义序列中（`{"k":"a\` → `{"k":"a\\"}`）。若没有 escape 守卫，
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
        // (输入, 起始 token, 结束 token, 期望)
        let cases: &[(&str, &str, &str, bool)] = &[
            // 完整 token：hermes / nemotron / python_tag / mistral / phi4
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
            // 无 token 但有裸 JSON
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
            // phi4 部分 token：fun / func / f / 结尾 fun
            (r#"fun"#, "functools", "", true),
            (r#"func"#, "functools", "", true),
            (r#"f"#, "functools", "", true),
            (r#"Hello fun"#, "functools", "", true),
            // funny joke 经由前缀 "fun" 命中（可接受的误报）
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

