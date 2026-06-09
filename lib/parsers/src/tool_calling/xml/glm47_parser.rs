// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-License-Identifier: Apache-2.0

//! # tool_calling::xml::glm47_parser
//!
//! ## 设计意图
//! 解析 GLM-4.7 的工具调用格式：
//! `<tool_call>function_name<arg_key>param1</arg_key><arg_value>value1</arg_value></tool_call>`。
//! 参考：https://huggingface.co/zai-org/GLM-4.7/blob/main/chat_template.jinja
//!
//! ## 外部契约
//! - `detect_tool_call_start_glm47(chunk, config)`：完整或部分起始 token 命中即返回 true。
//! - `find_tool_call_end_position_glm47(chunk, config)`：返回 `</tool_call>` 之后的位置，缺失则返回长度。
//! - `try_tool_call_parse_glm47(message, config, tools)`：返回 `(calls, normal_text)`。
//!
//! ## 实现要点
//! - 无法解析的块保留为普通文本，绝不静默丢弃模型输出。
//! - 支持在 `</tool_call>` 截断（max_tokens / EOS）时的 EOF 恢复（受 allow_eof_recovery 控制）。
//! - 借助 tools 的参数 schema 进行类型强制转换，并解码 XML 实体。

use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use tracing::warn;
use uuid::Uuid;

use super::super::ToolDefinition;
use super::super::config::Glm47ParserConfig;
use super::response::{CalledFunction, ToolCallResponse, ToolCallType};

// === SECTION: 流式探测与定位 ===

/// 判断 chunk 是否包含（或部分包含，用于流式）GLM-4.7 工具调用起始。
pub fn detect_tool_call_start_glm47(chunk: &str, config: &Glm47ParserConfig) -> bool {
    let start_token = config.tool_call_start.as_str();

    // 完整起始 token 命中
    if chunk.contains(start_token) {
        return true;
    }

    // 流式场景：chunk 结尾恰好是起始 token 的某个前缀
    (1..start_token.len()).any(|i| chunk.ends_with(&start_token[..i]))
}

/// 返回 `</tool_call>` 结束 token 之后的位置；若缺失则返回 chunk 长度。
pub fn find_tool_call_end_position_glm47(chunk: &str, config: &Glm47ParserConfig) -> usize {
    let end_token = config.tool_call_end.as_str();

    match chunk.find(end_token) {
        Some(pos) => pos + end_token.len(),
        None => chunk.len(),
    }
}

// === SECTION: 顶层解析入口 ===

/// 解析消息中的 GLM-4.7 工具调用，返回 `(parsed_tool_calls, normal_text_content)`。
pub fn try_tool_call_parse_glm47(
    message: &str,
    config: &Glm47ParserConfig,
    tools: Option<&[ToolDefinition]>,
) -> anyhow::Result<(Vec<ToolCallResponse>, Option<String>)> {
    let (normal_text, tool_calls) = extract_tool_calls(message, config, tools)?;

    // 即使为空也回传 Some("")，保持外部契约
    Ok((tool_calls, Some(normal_text)))
}

/// 从消息中分离工具调用块与普通文本。
fn extract_tool_calls(
    text: &str,
    config: &Glm47ParserConfig,
    tools: Option<&[ToolDefinition]>,
) -> anyhow::Result<(String, Vec<ToolCallResponse>)> {
    let start_token = config.tool_call_start.as_str();
    let end_token = config.tool_call_end.as_str();
    let arg_key_start = config.arg_key_start.as_str();

    let mut normal_parts: Vec<&str> = Vec::new();
    let mut calls: Vec<ToolCallResponse> = Vec::new();
    let mut cursor = 0;

    while cursor < text.len() {
        // 定位下一个起始 token；找不到则余下全部为普通文本
        let Some(start_pos) = text[cursor..].find(start_token) else {
            normal_parts.push(&text[cursor..]);
            break;
        };
        let abs_start = cursor + start_pos;
        normal_parts.push(&text[cursor..abs_start]);

        match text[abs_start..].find(end_token) {
            Some(end_pos) => {
                // 完整块：[abs_start, abs_end)
                let abs_end = abs_start + end_pos + end_token.len();
                let block = &text[abs_start..abs_end];

                // 解析失败的块保留为普通文本，避免丢失模型输出
                match parse_tool_call_block(block, config, tools) {
                    Ok(parsed_call) => calls.push(parsed_call),
                    Err(e) => {
                        warn!("Failed to parse GLM-4.7 tool call block: {e}");
                        normal_parts.push(block);
                    }
                }
                cursor = abs_end;
            }
            None => {
                // 外层 </tool_call> 缺失（max_tokens / EOS 截断）。
                // 仅在 allow_eof_recovery 开启且尾段含 <arg_key> 结构信号时尝试恢复，
                // 避免流式中途误判。
                let block = &text[abs_start..];
                if config.allow_eof_recovery
                    && block.contains(arg_key_start)
                    && let Ok(parsed_call) = parse_tool_call_block(block, config, tools)
                        .inspect_err(|e| {
                            warn!("Failed to parse GLM-4.7 tool call block (no end token): {e}")
                        })
                {
                    calls.push(parsed_call);
                    break;
                }
                normal_parts.push(block);
                break;
            }
        }
    }

    let normal_text = normal_parts.join("").trim().to_string();
    Ok((normal_text, calls))
}

// === SECTION: 值解码与类型强制 ===

/// 解码 XML 预定义实体：&lt; &gt; &amp; &quot; &apos;。
fn decode_xml_entities(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
}

/// 根据参数 schema 类型将原始字符串强制为 JSON 值；无 schema 或无法识别时退化为字符串。
fn coerce_value(raw: &str, schema_type: Option<&str>) -> Value {
    let trimmed = raw.trim();

    // 看起来已经是 JSON（对象/数组/带引号字符串）则直接解析
    if matches!(trimmed.chars().next(), Some('{') | Some('[') | Some('"'))
        && let Ok(v) = serde_json::from_str(trimmed)
    {
        return v;
    }

    // 利用 schema 类型提示进行转换
    match schema_type {
        Some("integer" | "int") => {
            if let Ok(n) = trimmed.parse::<i64>() {
                return Value::Number(n.into());
            }
        }
        Some("number" | "float" | "double") => {
            if let Ok(n) = trimmed.parse::<f64>()
                && let Some(num) = serde_json::Number::from_f64(n)
            {
                return Value::Number(num);
            }
        }
        Some("boolean" | "bool") => match trimmed.to_lowercase().as_str() {
            "true" | "1" | "yes" => return Value::Bool(true),
            "false" | "0" | "no" => return Value::Bool(false),
            _ => {}
        },
        Some("array") => {
            // 先试 JSON 解析，不成则按逗号拆分
            if let Ok(v) = serde_json::from_str::<Value>(trimmed)
                && v.is_array()
            {
                return v;
            }
            let items = trimmed
                .split(',')
                .map(|s| Value::String(s.trim().to_string()))
                .collect();
            return Value::Array(items);
        }
        Some("null") => {
            if trimmed == "null" || trimmed == "None" || trimmed.is_empty() {
                return Value::Null;
            }
        }
        _ => {}
    }

    Value::String(raw.to_string())
}

/// 从某工具的参数 schema 中查找指定参数名的 JSON Schema 类型。
fn get_param_schema_type<'a>(
    tools: Option<&'a [ToolDefinition]>,
    function_name: &str,
    param_name: &str,
) -> Option<&'a str> {
    tools?
        .iter()
        .find(|t| t.name == function_name)?
        .parameters
        .as_ref()?
        .get("properties")?
        .get(param_name)?
        .get("type")?
        .as_str()
}

// === SECTION: 单块解析 ===

/// 解析单个 GLM-4.7 工具调用块：
/// `<tool_call>function_name<arg_key>key1</arg_key><arg_value>value1</arg_value>...</tool_call>`
fn parse_tool_call_block(
    block: &str,
    config: &Glm47ParserConfig,
    tools: Option<&[ToolDefinition]>,
) -> anyhow::Result<ToolCallResponse> {
    let start_token = config.tool_call_start.as_str();
    let end_token = config.tool_call_end.as_str();
    let arg_key_start = config.arg_key_start.as_str();

    // 剖开外层起始 token；结束 token 可缺失（用于 EOF 恢复）
    let after_start = block
        .strip_prefix(start_token)
        .ok_or_else(|| anyhow::anyhow!("Invalid tool call block format"))?;
    let content = after_start.strip_suffix(end_token).unwrap_or(after_start);

    // 函数名 = 首个 <arg_key> 之前的全部（或整个 content）
    let function_name = match content.find(arg_key_start) {
        Some(pos) => content[..pos].trim(),
        None => content.trim(),
    }
    .to_string();

    if function_name.is_empty() {
        anyhow::bail!("Empty function name in tool call");
    }

    // 构造匹配 <arg_key>key</arg_key><arg_value>value</arg_value> 的正则
    // (?s) 开启 dotall，使 (.*?) 能跨行匹配（arg 值常含多行内容）
    let pattern = format!(
        r"(?s){}([^<]+){}{}(.*?){}",
        regex::escape(&config.arg_key_start),
        regex::escape(&config.arg_key_end),
        regex::escape(&config.arg_value_start),
        regex::escape(&config.arg_value_end),
    );
    let regex = Regex::new(&pattern)?;

    // 解析键值对
    let mut arguments = HashMap::new();
    let args_section = &content[function_name.len()..];
    for cap in regex.captures_iter(args_section) {
        let key = cap.get(1).map(|m| m.as_str().trim()).unwrap_or("");
        if key.is_empty() {
            continue;
        }
        let raw_value = cap.get(2).map(|m| m.as_str()).unwrap_or("");

        // 先解码 XML 实体，再按 schema 类型强制转换
        let decoded = decode_xml_entities(raw_value);
        let schema_type = get_param_schema_type(tools, &function_name, key);
        arguments.insert(key.to_string(), coerce_value(&decoded, schema_type));
    }

    // 若提供了 tools，验证函数是否存在
    if let Some(tools_list) = tools
        && !tools_list.iter().any(|t| t.name == function_name)
    {
        anyhow::bail!("Function '{}' not found in available tools", function_name);
    }

    Ok(ToolCallResponse {
        id: Uuid::new_v4().to_string(),
        tp: ToolCallType::Function,
        function: CalledFunction {
            name: function_name,
            arguments: serde_json::to_string(&arguments)?,
        },
    })
}

#[cfg(test)]
mod tests {
    //! ## 测试过程
    //! 围绕 GLM-4.7 公开 API（`detect_tool_call_start_glm47`、`find_tool_call_end_position_glm47`、
    //! `try_tool_call_parse_glm47`）覆盖：起始探测、单/多调用、JSON 参数、无参、多行参数值、
    //! XML 实体解码、schema 类型强制、EOF 截断恢复、不可解析块保留为普通文本、空白输入、重名调用。
    //!
    //! ## 意义
    //! 锁定该解析器在截断、噪声与类型转换边界下的可观察行为，确保模型输出不被静默丢弃。
    use super::*;

    fn get_test_config() -> Glm47ParserConfig {
        Glm47ParserConfig::default()
    }

    #[test] // helper
    fn test_detect_tool_call_start() {
        let config = get_test_config();

        assert!(detect_tool_call_start_glm47(
            "<tool_call>get_weather",
            &config
        ));

        assert!(detect_tool_call_start_glm47("Some text <tool", &config));
        assert!(detect_tool_call_start_glm47("Some text <tool_c", &config));

        assert!(!detect_tool_call_start_glm47("Just normal text", &config));
    }

    #[test] // PARSER.batch.1
    fn test_parse_simple_tool_call() {
        let config = get_test_config();
        let message = "<tool_call>get_weather<arg_key>location</arg_key><arg_value>San Francisco</arg_value></tool_call>";

        let (calls, normal_text) = try_tool_call_parse_glm47(message, &config, None).unwrap();

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "get_weather");

        let args: HashMap<String, Value> =
            serde_json::from_str(&calls[0].function.arguments).unwrap();
        assert_eq!(
            args.get("location").unwrap().as_str().unwrap(),
            "San Francisco"
        );
        assert_eq!(normal_text, Some("".to_string()));
    }

    #[test] // PARSER.batch.1, PARSER.batch.7
    fn test_parse_tool_call_with_multiple_args() {
        let config = get_test_config();
        let message = "<tool_call>book_flight<arg_key>from</arg_key><arg_value>NYC</arg_value><arg_key>to</arg_key><arg_value>LAX</arg_value><arg_key>date</arg_key><arg_value>2026-03-15</arg_value></tool_call>";

        let (calls, _) = try_tool_call_parse_glm47(message, &config, None).unwrap();

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "book_flight");

        let args: HashMap<String, Value> =
            serde_json::from_str(&calls[0].function.arguments).unwrap();
        assert_eq!(args.get("from").unwrap().as_str().unwrap(), "NYC");
        assert_eq!(args.get("to").unwrap().as_str().unwrap(), "LAX");
        assert_eq!(args.get("date").unwrap().as_str().unwrap(), "2026-03-15");
    }

    #[test] // PARSER.batch.7
    fn test_parse_tool_call_with_json_value() {
        let config = get_test_config();
        let message = r#"<tool_call>search<arg_key>filters</arg_key><arg_value>{"category": "books", "price_max": 50}</arg_value></tool_call>"#;

        let (calls, _) = try_tool_call_parse_glm47(message, &config, None).unwrap();

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "search");

        let args: HashMap<String, Value> =
            serde_json::from_str(&calls[0].function.arguments).unwrap();
        let filters = args.get("filters").unwrap();
        assert!(filters.is_object());
    }

    #[test] // PARSER.batch.2
    fn test_parse_multiple_tool_calls() {
        let config = get_test_config();
        let message = "<tool_call>get_weather<arg_key>location</arg_key><arg_value>NYC</arg_value></tool_call><tool_call>get_time<arg_key>timezone</arg_key><arg_value>EST</arg_value></tool_call>";

        let (calls, _) = try_tool_call_parse_glm47(message, &config, None).unwrap();

        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].function.name, "get_weather");
        assert_eq!(calls[1].function.name, "get_time");
    }

    #[test] // PARSER.batch.8
    fn test_parse_with_normal_text() {
        let config = get_test_config();
        let message = "I'll check the weather for you. <tool_call>get_weather<arg_key>location</arg_key><arg_value>Paris</arg_value></tool_call>";

        let (calls, normal_text) = try_tool_call_parse_glm47(message, &config, None).unwrap();

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "get_weather");
        assert_eq!(
            normal_text,
            Some("I'll check the weather for you.".to_string())
        );
    }

    #[test] // PARSER.batch.6
    fn test_parse_tool_call_no_args() {
        let config = get_test_config();
        let message = "<tool_call>get_current_time</tool_call>";

        let (calls, _) = try_tool_call_parse_glm47(message, &config, None).unwrap();

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "get_current_time");

        let args: HashMap<String, Value> =
            serde_json::from_str(&calls[0].function.arguments).unwrap();
        assert!(args.is_empty());
    }

    #[test] // helper
    fn test_find_tool_call_end_position() {
        let config = get_test_config();
        let chunk =
            "<tool_call>func<arg_key>k</arg_key><arg_value>v</arg_value></tool_call>more text";

        let end_pos = find_tool_call_end_position_glm47(chunk, &config);
        assert_eq!(
            &chunk[..end_pos],
            "<tool_call>func<arg_key>k</arg_key><arg_value>v</arg_value></tool_call>"
        );
    }

    #[test] // PARSER.batch.7, PARSER.fmt.2
    fn test_parse_multiline_arg_value() {
        let config = get_test_config();
        let message = "<tool_call>write_file<arg_key>path</arg_key><arg_value>/tmp/hello.py</arg_value><arg_key>content</arg_key><arg_value>#!/usr/bin/env python3\nprint(\"Hello, World!\")\n</arg_value></tool_call>";

        let (calls, _) = try_tool_call_parse_glm47(message, &config, None).unwrap();

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "write_file");

        let args: HashMap<String, Value> =
            serde_json::from_str(&calls[0].function.arguments).unwrap();
        assert_eq!(args.get("path").unwrap().as_str().unwrap(), "/tmp/hello.py");
        assert!(
            args.contains_key("content"),
            "content argument must be parsed even when it contains newlines"
        );
        let content = args.get("content").unwrap().as_str().unwrap();
        assert!(content.contains("print(\"Hello, World!\")"));
    }

    #[test] // PARSER.batch.4
    fn test_malformed_tool_call() {
        let config = get_test_config();

        let message = "<tool_call>get_weather";
        let result = try_tool_call_parse_glm47(message, &config, None);
        assert!(result.is_ok()); // Should handle gracefully, no calls extracted

        let (calls, _) = result.unwrap();
        assert_eq!(calls.len(), 0);
    }

    // 针对缺失外层 `</tool_call>` 的恢复逻辑，也就是因 max_tokens 或 EOS 导致的截断：
    // 当内部参数键值对本身格式完整时，将 EOF 视为结束 token，
    // 并提取该工具调用。这里用 arg_key 的起始标记作为恢复门槛，
    // 这样即使普通文本碰巧以 `<tool_call>` 开头，也仍会被原样保留。
    #[test] // PARSER.batch.5
    fn test_parse_no_end_tag_complete_args_recovers() {
        let config = Glm47ParserConfig {
            allow_eof_recovery: true,
            ..get_test_config()
        };
        let message = "<tool_call>get_weather<arg_key>location</arg_key><arg_value>NYC</arg_value>";

        let (calls, _) = try_tool_call_parse_glm47(message, &config, None).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "get_weather");
        let args: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
        assert_eq!(args["location"], "NYC");
    }

    #[test] // PARSER.batch.5
    fn test_parse_no_end_tag_multiple_calls_recovers() {
        let config = Glm47ParserConfig {
            allow_eof_recovery: true,
            ..get_test_config()
        };
        let message = "<tool_call>get_weather<arg_key>city</arg_key><arg_value>NYC</arg_value></tool_call><tool_call>get_time<arg_key>tz</arg_key><arg_value>EST</arg_value>";

        let (calls, _) = try_tool_call_parse_glm47(message, &config, None).unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].function.name, "get_weather");
        assert_eq!(calls[1].function.name, "get_time");
    }

    #[test] // PARSER.batch.4, PARSER.batch.8
    fn test_unparseable_block_preserved_as_normal_text() {
        let config = get_test_config();
        let tools = vec![ToolDefinition {
            name: "get_weather".to_string(),
            parameters: None,
        }];

        let message = "Here is the result: <tool_call>unknown_func<arg_key>x</arg_key><arg_value>1</arg_value></tool_call> done";
        let (calls, normal_text) =
            try_tool_call_parse_glm47(message, &config, Some(&tools)).unwrap();

        assert_eq!(calls.len(), 0);
        let text = normal_text.unwrap();
        assert!(
            text.contains("unknown_func"),
            "Unparseable block should be in normal text, got: {text}"
        );
    }

    #[test] // helper
    fn test_xml_entity_decoding() {
        let config = get_test_config();
        let message = r#"<tool_call>write_file<arg_key>content</arg_key><arg_value>x &lt; y &amp;&amp; y &gt; z</arg_value></tool_call>"#;

        let (calls, _) = try_tool_call_parse_glm47(message, &config, None).unwrap();

        assert_eq!(calls.len(), 1);
        let args: HashMap<String, Value> =
            serde_json::from_str(&calls[0].function.arguments).unwrap();
        assert_eq!(
            args.get("content").unwrap().as_str().unwrap(),
            "x < y && y > z"
        );
    }

    #[test] // helper
    fn test_type_coercion_with_schema() {
        let config = get_test_config();
        let tools = vec![ToolDefinition {
            name: "set_temperature".to_string(),
            parameters: Some(serde_json::json!({
                "type": "object",
                "properties": {
                    "degrees": {"type": "number"},
                    "enabled": {"type": "boolean"},
                    "count": {"type": "integer"},
                    "label": {"type": "string"}
                }
            })),
        }];

        let message = "<tool_call>set_temperature<arg_key>degrees</arg_key><arg_value>72.5</arg_value><arg_key>enabled</arg_key><arg_value>true</arg_value><arg_key>count</arg_key><arg_value>3</arg_value><arg_key>label</arg_key><arg_value>warm</arg_value></tool_call>";

        let (calls, _) = try_tool_call_parse_glm47(message, &config, Some(&tools)).unwrap();
        assert_eq!(calls.len(), 1);

        let args: HashMap<String, Value> =
            serde_json::from_str(&calls[0].function.arguments).unwrap();

        assert_eq!(args.get("degrees").unwrap().as_f64().unwrap(), 72.5);
        assert!(args.get("enabled").unwrap().as_bool().unwrap());
        assert_eq!(args.get("count").unwrap().as_i64().unwrap(), 3);
        assert_eq!(args.get("label").unwrap().as_str().unwrap(), "warm");
    }

    #[test] // helper
    fn test_type_coercion_array_comma_separated() {
        let config = get_test_config();
        let tools = vec![ToolDefinition {
            name: "tag_item".to_string(),
            parameters: Some(serde_json::json!({
                "type": "object",
                "properties": {
                    "tags": {"type": "array"}
                }
            })),
        }];

        let message = "<tool_call>tag_item<arg_key>tags</arg_key><arg_value>rust, python, go</arg_value></tool_call>";
        let (calls, _) = try_tool_call_parse_glm47(message, &config, Some(&tools)).unwrap();

        let args: HashMap<String, Value> =
            serde_json::from_str(&calls[0].function.arguments).unwrap();
        let tags = args.get("tags").unwrap().as_array().unwrap();
        assert_eq!(tags.len(), 3);
        assert_eq!(tags[0].as_str().unwrap(), "rust");
        assert_eq!(tags[1].as_str().unwrap(), "python");
        assert_eq!(tags[2].as_str().unwrap(), "go");
    }

    #[test] // helper
    fn test_type_coercion_array_json() {
        let config = get_test_config();
        let tools = vec![ToolDefinition {
            name: "tag_item".to_string(),
            parameters: Some(serde_json::json!({
                "type": "object",
                "properties": {
                    "ids": {"type": "array"}
                }
            })),
        }];

        let message = r#"<tool_call>tag_item<arg_key>ids</arg_key><arg_value>[1, 2, 3]</arg_value></tool_call>"#;
        let (calls, _) = try_tool_call_parse_glm47(message, &config, Some(&tools)).unwrap();

        let args: HashMap<String, Value> =
            serde_json::from_str(&calls[0].function.arguments).unwrap();
        let ids = args.get("ids").unwrap().as_array().unwrap();
        assert_eq!(ids.len(), 3);
        assert_eq!(ids[0].as_i64().unwrap(), 1);
    }

    #[test] // helper
    fn test_type_coercion_falls_back_to_string() {
        let config = get_test_config();
        let tools = vec![ToolDefinition {
            name: "test_func".to_string(),
            parameters: Some(serde_json::json!({
                "type": "object",
                "properties": {
                    "count": {"type": "integer"}
                }
            })),
        }];

        let message = "<tool_call>test_func<arg_key>count</arg_key><arg_value>not_a_number</arg_value></tool_call>";
        let (calls, _) = try_tool_call_parse_glm47(message, &config, Some(&tools)).unwrap();

        let args: HashMap<String, Value> =
            serde_json::from_str(&calls[0].function.arguments).unwrap();
        assert!(
            args.get("count").unwrap().is_string(),
            "Should fall back to string when coercion fails"
        );
    }

    /// Parser 级别的不变式：glm47 parser 是字节稳定的。
    /// 它不会感知 `finish_reason`，并且无论上游流结束原因是什么，
    /// 都会产生相同的输出。
    ///
    /// 真正的 PIPELINE.finish_reason 覆盖，也就是 stop / tool_calls / length
    /// 的映射测试，位于 `lib/llm/tests/test_streaming_tool_parsers.rs`，
    /// 并且属于跨 parser 的 finish_reason 映射工作项，已单独跟踪。
    #[test]
    fn test_glm47_parser_output_independent_of_upstream_finish() {
        let config = get_test_config();
        let input = "<tool_call>get_weather<arg_key>location</arg_key><arg_value>NYC</arg_value></tool_call>";
        let (calls, _) = try_tool_call_parse_glm47(input, &config, None).unwrap();
        assert_eq!(calls.len(), 1);
    }

    #[test] // PARSER.batch.9
    fn test_parse_glm47_empty_and_whitespace_inputs() {
        let config = get_test_config();
        for input in &["", " ", "\n", "\t\n  \t"] {
            let (calls, normal) = try_tool_call_parse_glm47(input, &config, None).unwrap();
            assert!(
                calls.is_empty(),
                "Empty/whitespace input must yield no calls (input={:?})",
                input
            );
            assert_eq!(
                normal.as_deref(),
                Some(""),
                "Empty/whitespace input collapses to empty normal_text (input={:?})",
                input
            );
        }
    }

    #[test] // PARSER.batch.10
    fn test_parse_glm47_duplicate_calls_same_name() {
        let config = get_test_config();
        let input = "<tool_call>get_weather<arg_key>location</arg_key><arg_value>NYC</arg_value></tool_call><tool_call>get_weather<arg_key>location</arg_key><arg_value>LA</arg_value></tool_call>";
        let (calls, _) = try_tool_call_parse_glm47(input, &config, None).unwrap();
        assert_eq!(calls.len(), 2, "Both duplicate-name calls must be returned");
        assert_eq!(calls[0].function.name, "get_weather");
        assert_eq!(calls[1].function.name, "get_weather");
        assert_ne!(
            calls[0].id, calls[1].id,
            "Duplicate calls must have distinct ids"
        );
        let args0: HashMap<String, Value> =
            serde_json::from_str(&calls[0].function.arguments).unwrap();
        let args1: HashMap<String, Value> =
            serde_json::from_str(&calls[1].function.arguments).unwrap();
        assert_eq!(args0.get("location").unwrap().as_str().unwrap(), "NYC");
        assert_eq!(args1.get("location").unwrap().as_str().unwrap(), "LA");
    }
}
