// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-License-Identifier: Apache-2.0

//! ## 设计意图
//! 参考实现：
//!
//! ## 外部契约
//!
//! ## 实现要点
//! - 普通文本只取首个解析成功调用之前的内容；调用之后的文本不计入响应内容。

use std::collections::HashMap;

use regex::Regex;
use serde_json::Value;
use uuid::Uuid;

use super::super::ToolDefinition;
use super::super::config::XmlParserConfig;
use super::response::{CalledFunction, ToolCallResponse, ToolCallType};


/// 去除字符串首尾成对的引号（单或双）。
fn strip_quotes(s: &str) -> &str {
    let trimmed = s.trim();
    let bytes = trimmed.as_bytes();
    let quoted = bytes.len() >= 2
        && ((bytes[0] == b'"' && bytes[bytes.len() - 1] == b'"')
            || (bytes[0] == b'\'' && bytes[bytes.len() - 1] == b'\''));
    if quoted {
        &trimmed[1..trimmed.len() - 1]
    } else {
        trimmed
    }
}


pub fn detect_tool_call_start_xml(chunk: &str, config: &XmlParserConfig) -> bool {
    let start_token = config.tool_call_start_token.as_str();

    chunk.contains(start_token)
        || (1..start_token.len()).any(|i| chunk.ends_with(&start_token[..i]))
}

///
pub fn find_tool_call_end_position_xml(chunk: &str, config: &XmlParserConfig) -> usize {
    let start_token = config.tool_call_start_token.as_str();
    let end_token = config.tool_call_end_token.as_str();

    let Some(first_end) = chunk.find(end_token) else {
        return chunk.len();
    };

    let mut cursor = first_end + end_token.len();

    loop {
        let rest = &chunk[cursor..];
        let trimmed = rest.trim_start();
        if !trimmed.starts_with(start_token) {
            break;
        }
        let trim_offset = rest.len() - trimmed.len();
        let search_from = cursor + trim_offset + start_token.len();
        match chunk[search_from..].find(end_token) {
            Some(end_pos) => cursor = search_from + end_pos + end_token.len(),
            // 下一块不完整——停止，等待更多数据
            None => break,
        }
    }

    cursor
}


pub fn try_tool_call_parse_xml(
    message: &str,
    config: &XmlParserConfig,
    tools: Option<&[ToolDefinition]>,
) -> anyhow::Result<(Vec<ToolCallResponse>, Option<String>)> {
    let (normal_text, tool_calls) = extract_tool_calls(message, config, tools)?;
    Ok((tool_calls, Some(normal_text)))
}

/// 从消息中分离工具调用块与普通文本。
fn extract_tool_calls(
    text: &str,
    config: &XmlParserConfig,
    tools: Option<&[ToolDefinition]>,
) -> anyhow::Result<(String, Vec<ToolCallResponse>)> {
    let start_token = config.tool_call_start_token.as_str();
    let end_token = config.tool_call_end_token.as_str();
    let function_start = config.function_start_token.as_str();

    let mut normal_parts: Vec<&str> = Vec::new();
    let mut calls: Vec<ToolCallResponse> = Vec::new();
    let mut cursor = 0;

    while cursor < text.len() {
        // 定位下一个调用起始；找不到则余下全为普通文本（仅在尚无调用时收集）
        let Some(start_pos) = text[cursor..].find(start_token) else {
            if calls.is_empty() {
                normal_parts.push(&text[cursor..]);
            }
            break;
        };
        let abs_start = cursor + start_pos;

        // 继续扫描后续调用，但只暴露首个被解析调用之前的普通文本。
        if calls.is_empty() {
            normal_parts.push(&text[cursor..abs_start]);
        }

        match text[abs_start..].find(end_token) {
            Some(end_pos) => {
                let abs_end = abs_start + end_pos + end_token.len();
                let block = &text[abs_start..abs_end];
                if let Ok(parsed_calls) = parse_tool_call_block(block, config, tools) {
                    calls.extend(parsed_calls);
                }
                cursor = abs_end;
            }
            None => {
                let block = &text[abs_start..];
                if config.allow_eof_recovery
                    && block.contains(function_start)
                    && let Ok(parsed_calls) = parse_tool_call_block(block, config, tools)
                    && !parsed_calls.is_empty()
                {
                    calls.extend(parsed_calls);
                    break;
                }
                if calls.is_empty() {
                    normal_parts.push(block);
                }
                break;
            }
        }
    }

    let joined = normal_parts.join("");
    let normal_text = if calls.is_empty() {
        joined.trim().to_string()
    } else {
        joined
    };
    Ok((normal_text, calls))
}

/// 解析单个工具调用块。
fn parse_tool_call_block(
    block: &str,
    config: &XmlParserConfig,
    tools: Option<&[ToolDefinition]>,
) -> anyhow::Result<Vec<ToolCallResponse>> {
    let function_pattern = format!(
        r"(?s){}([^>]+)>(.*?)(?:{}|$)",
        regex::escape(&config.function_start_token),
        regex::escape(&config.function_end_token),
    );
    let parameter_pattern = format!(
        r"(?s){}([^>]+)>(.*?)(?:{}|$)",
        regex::escape(&config.parameter_start_token),
        regex::escape(&config.parameter_end_token),
    );

    let function_regex = Regex::new(&function_pattern)?;
    let parameter_regex = Regex::new(&parameter_pattern)?;

    let mut results = Vec::new();

    for func_cap in function_regex.captures_iter(block) {
        let function_name_raw = func_cap.get(1).map(|m| m.as_str().trim()).unwrap_or("");
        let function_name = strip_quotes(function_name_raw);
        let function_body = func_cap.get(2).map(|m| m.as_str()).unwrap_or("");

        if function_name.is_empty() {
            continue;
        }

        let param_config = get_arguments_config(function_name, tools);

        // 从函数体解析参数
        let mut parameters: HashMap<String, serde_json::Value> = HashMap::new();
        for param_cap in parameter_regex.captures_iter(function_body) {
            let param_name_raw = param_cap.get(1).map(|m| m.as_str().trim()).unwrap_or("");
            let param_name = strip_quotes(param_name_raw);
            if param_name.is_empty() {
                continue;
            }
            let param_value = param_cap.get(2).map(|m| m.as_str()).unwrap_or("");
            let parsed_value =
                convert_param_value(param_value, param_name, &param_config, function_name);
            parameters.insert(param_name.to_string(), parsed_value);
        }

        results.push(ToolCallResponse {
            id: format!("call-{}", Uuid::new_v4()),
            tp: ToolCallType::Function,
            function: CalledFunction {
                name: function_name.to_string(),
                arguments: serde_json::to_string(&parameters)?,
            },
        });
    }

    Ok(results)
}


fn get_arguments_config(
    func_name: &str,
    tools: Option<&[ToolDefinition]>,
) -> HashMap<String, Value> {
    let Some(tools) = tools else {
        return HashMap::new();
    };

    let Some(tool) = tools.iter().find(|t| t.name == func_name) else {
        tracing::warn!("Tool '{}' is not defined in the tools list.", func_name);
        return HashMap::new();
    };

    let Some(params) = &tool.parameters else {
        return HashMap::new();
    };

    let source = params.get("properties").or(Some(params));
    source
        .and_then(|v| v.as_object())
        .map(|obj| obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default()
}

///
/// 识别的类型别名：
///
fn convert_param_value(
    param_value: &str,
    param_name: &str,
    param_config: &HashMap<String, Value>,
    func_name: &str,
) -> Value {
    let param_value = html_unescape(param_value.trim());

    if param_value.eq_ignore_ascii_case("null") {
        return Value::Null;
    }

    // 参数未在配置中声明：直接按字符串返回
    let Some(param_schema) = param_config.get(param_name) else {
        tracing::debug!(
            "Parsed parameter '{}' is not defined in the tool parameters for tool '{}', directly returning the string value.",
            param_name,
            func_name
        );
        return Value::String(param_value);
    };

    let param_type = param_schema
        .get("type")
        .and_then(|t| t.as_str())
        .map(|t| t.to_lowercase())
        .unwrap_or_else(|| {
            if param_schema.get("anyOf").is_some() || param_schema.get("oneOf").is_some() {
                "object".to_string()
            } else {
                "string".to_string()
            }
        });

    match param_type.as_str() {
        "string" | "str" | "text" | "varchar" | "char" | "enum" => Value::String(param_value),

        // 整数类：解析为 i64，失败退化为字符串
        t if t.starts_with("int")
            || t.starts_with("uint")
            || t.starts_with("long")
            || t.starts_with("short")
            || t.starts_with("unsigned") =>
        {
            match param_value.parse::<i64>() {
                Ok(int_val) => Value::Number(int_val.into()),
                Err(_) => {
                    tracing::warn!(
                        "Parsed value '{}' of parameter '{}' is not an integer in tool '{}', degenerating to string.",
                        param_value,
                        param_name,
                        func_name
                    );
                    Value::String(param_value)
                }
            }
        }

        t if t.starts_with("num") || t.starts_with("float") => match param_value.parse::<f64>() {
            Ok(float_val) if float_val.fract() == 0.0 && float_val.is_finite() => {
                Value::Number((float_val as i64).into())
            }
            Ok(float_val) => match serde_json::Number::from_f64(float_val) {
                Some(num) => Value::Number(num),
                None => {
                    tracing::warn!(
                        "Parsed value '{}' of parameter '{}' is not a valid float in tool '{}', degenerating to string.",
                        param_value,
                        param_name,
                        func_name
                    );
                    Value::String(param_value)
                }
            },
            Err(_) => {
                tracing::warn!(
                    "Parsed value '{}' of parameter '{}' is not a float in tool '{}', degenerating to string.",
                    param_value,
                    param_name,
                    func_name
                );
                Value::String(param_value)
            }
        },

        "boolean" | "bool" | "binary" => {
            let lower_val = param_value.to_lowercase();
            if lower_val != "true" && lower_val != "false" {
                tracing::warn!(
                    "Parsed value '{}' of parameter '{}' is not a boolean (`true` or `false`) in tool '{}', degenerating to false.",
                    param_value,
                    param_name,
                    func_name
                );
            }
            Value::Bool(lower_val == "true")
        }

        t if t == "object"
            || t == "array"
            || t == "arr"
            || t.starts_with("dict")
            || t.starts_with("list") =>
        {
            if let Ok(json_val) = serde_json::from_str::<Value>(&param_value) {
                return json_val;
            }
            tracing::warn!(
                "Parsed value '{}' of parameter '{}' cannot be parsed with json.loads in tool '{}', will try other methods to parse it.",
                param_value,
                param_name,
                func_name
            );
            if let Ok(json_val) = try_literal_eval(&param_value) {
                return json_val;
            }
            tracing::warn!(
                "Parsed value '{}' of parameter '{}' cannot be converted via Python `ast.literal_eval()` in tool '{}', degenerating to string.",
                param_value,
                param_name,
                func_name
            );
            Value::String(param_value)
        }

        _ => {
            if let Ok(json_val) = try_literal_eval(&param_value) {
                return json_val;
            }
            tracing::warn!(
                "Parsed value '{}' of parameter '{}' cannot be converted via Python `ast.literal_eval()` in tool '{}', degenerating to string.",
                param_value,
                param_name,
                func_name
            );
            Value::String(param_value)
        }
    }
}

fn try_literal_eval(s: &str) -> Result<Value, ()> {
    if let Ok(val) = serde_json::from_str::<Value>(s) {
        return Ok(val);
    }

    let normalized = s
        .replace('\'', "\"")
        .replace("True", "true")
        .replace("False", "false")
        .replace("None", "null");

    serde_json::from_str::<Value>(&normalized).map_err(|_| ())
}

#[allow(dead_code)]
fn safe_parse_value(raw: &str) -> serde_json::Value {
    let unescaped = html_unescape(raw.trim());

    if let Ok(value) = serde_json::from_str::<serde_json::Value>(&unescaped) {
        return value;
    }
    if let Ok(num) = unescaped.parse::<i64>() {
        return serde_json::Value::Number(num.into());
    }
    if let Ok(num) = unescaped.parse::<f64>()
        && let Some(num_val) = serde_json::Number::from_f64(num)
    {
        return serde_json::Value::Number(num_val);
    }
    match unescaped.to_lowercase().as_str() {
        "true" => return serde_json::Value::Bool(true),
        "false" => return serde_json::Value::Bool(false),
        "null" | "none" => return serde_json::Value::Null,
        _ => {}
    }

    // 默认返回字符串，剥离首尾换行
    serde_json::Value::String(unescaped.trim_matches('\n').to_string())
}

fn html_unescape(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&#x27;", "'")
        .replace("&#39;", "'")
}

#[cfg(test)]
mod tests {
    //! ## 测试过程
    //!
    //! ## 意义
    use super::*;
    use rstest::rstest;

    #[test] // helper
    fn test_detect_tool_call_start() {
        let config = XmlParserConfig::default();
        assert!(detect_tool_call_start_xml("<tool_call>", &config));
        assert!(detect_tool_call_start_xml("text <tool_call>", &config));
        assert!(detect_tool_call_start_xml("<tool_c", &config)); // Partial match
        assert!(detect_tool_call_start_xml("<", &config)); // Partial match
        assert!(!detect_tool_call_start_xml("no tool call here", &config));
        assert!(!detect_tool_call_start_xml("toolcall", &config));
    }

    #[test] // helper
    fn test_find_tool_call_end_position() {
        let config = XmlParserConfig::default();
        let text = "<tool_call><function=test></function></tool_call>more text";
        let pos = find_tool_call_end_position_xml(text, &config);
        assert_eq!(pos, 49); // Position after </tool_call>
        assert_eq!(&text[pos..], "more text");

        let text_no_end = "<tool_call><function=test>";
        let pos = find_tool_call_end_position_xml(text_no_end, &config);
        assert_eq!(pos, text_no_end.len());
    }

    #[test] // PARSER.batch.2, helper
    fn test_find_tool_call_end_position_parallel_calls() {
        let config = XmlParserConfig::default();

        let two_calls = "<tool_call><function=foo><parameter=x>1</parameter></function></tool_call>\
                         <tool_call><function=bar><parameter=y>2</parameter></function></tool_call>\
                         trailing";
        let pos = find_tool_call_end_position_xml(two_calls, &config);
        assert!(
            &two_calls[..pos].ends_with("</tool_call>"),
            "should end at last </tool_call>, got: {:?}",
            &two_calls[..pos]
        );
        assert_eq!(&two_calls[pos..], "trailing");

        let three_calls = "<tool_call><function=a></function></tool_call>\n\
                           <tool_call><function=b></function></tool_call>\n\
                           <tool_call><function=c></function></tool_call> done";
        let pos3 = find_tool_call_end_position_xml(three_calls, &config);
        assert!(
            &three_calls[..pos3].ends_with("</tool_call>"),
            "should end at last </tool_call>, got: {:?}",
            &three_calls[..pos3]
        );
        assert_eq!(three_calls[pos3..].trim(), "done");

        let incomplete = "<tool_call><function=a></function></tool_call>\
                          <tool_call><function=b>"; // no </tool_call>
        let pos_inc = find_tool_call_end_position_xml(incomplete, &config);
        let first_end = "<tool_call><function=a></function></tool_call>".len();
        assert_eq!(
            pos_inc, first_end,
            "should stop at end of first complete call when second is incomplete"
        );
    }

    #[rstest] // helper
    #[case(r#"{"key": "value"}"#, serde_json::json!({"key": "value"}), "JSON object")]
    #[case(r#"[1, 2, 3]"#, serde_json::json!([1, 2, 3]), "JSON array")]
    #[case("42", serde_json::json!(42), "integer")]
    #[case("3.15", serde_json::json!(3.15), "float")]
    #[case("true", serde_json::json!(true), "boolean true")]
    #[case("false", serde_json::json!(false), "boolean false")]
    #[case("null", serde_json::json!(null), "null")]
    #[case("hello", serde_json::json!("hello"), "unquoted string")]
    #[case("  text  ", serde_json::json!("text"), "trimmed string")]
    fn test_safe_parse_value(
        #[case] input: &str,
        #[case] expected: serde_json::Value,
        #[case] _description: &str,
    ) {
        assert_eq!(safe_parse_value(input), expected);
    }

    #[rstest] // helper
    #[case("&lt;div&gt;", "<div>", "HTML tags")]
    #[case("a &amp; b", "a & b", "ampersand")]
    #[case("&quot;quoted&quot;", "\"quoted\"", "quotes")]
    fn test_html_unescape(#[case] input: &str, #[case] expected: &str, #[case] _description: &str) {
        assert_eq!(html_unescape(input), expected);
    }

    #[test] // PARSER.batch.1
    fn test_parse_simple_tool_call() {
        let input = r#"<tool_call>
<function=execute_bash>
<parameter=command>
pwd && ls
</parameter>
</function>
</tool_call>"#;

        let (calls, normal) =
            try_tool_call_parse_xml(input, &XmlParserConfig::default(), None).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "execute_bash");
        assert_eq!(normal, Some("".to_string()));

        let args: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
        assert_eq!(args["command"], "pwd && ls");
    }

    #[test] // PARSER.batch.1, PARSER.batch.7
    fn test_parse_multiple_parameters() {
        let input = r#"<tool_call>
<function=get_weather>
<parameter=city>
San Francisco
</parameter>
<parameter=state>
CA
</parameter>
<parameter=unit>
fahrenheit
</parameter>
</function>
</tool_call>"#;

        let (calls, _) = try_tool_call_parse_xml(input, &XmlParserConfig::default(), None).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "get_weather");

        let args: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
        assert_eq!(args["city"], "San Francisco");
        assert_eq!(args["state"], "CA");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[test] // PARSER.batch.8
    fn test_parse_with_normal_text() {
        let input = r#"I'll help you with that. <tool_call>
<function=get_weather>
<parameter=city>
Dallas
</parameter>
</function>
</tool_call> Let me check that for you."#;

        let (calls, normal) =
            try_tool_call_parse_xml(input, &XmlParserConfig::default(), None).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "get_weather");
        assert_eq!(normal, Some("I'll help you with that. ".to_string()));
    }

    #[test] // PARSER.batch.2
    fn test_parse_multiple_tool_calls() {
        let input = r#"<tool_call>
<function=get_weather>
<parameter=city>
Dallas
</parameter>
</function>
</tool_call>
<tool_call>
<function=get_weather>
<parameter=city>
Orlando
</parameter>
</function>
</tool_call>"#;

        let (calls, _) = try_tool_call_parse_xml(input, &XmlParserConfig::default(), None).unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].function.name, "get_weather");
        assert_eq!(calls[1].function.name, "get_weather");

        let args0: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
        let args1: serde_json::Value = serde_json::from_str(&calls[1].function.arguments).unwrap();
        assert_eq!(args0["city"], "Dallas");
        assert_eq!(args1["city"], "Orlando");
    }

    #[test] // PARSER.batch.7
    fn test_parse_json_parameter_value() {
        let tools = vec![ToolDefinition {
            name: "process_data".to_string(),
            parameters: Some(serde_json::json!({
                "type": "object",
                "properties": {
                    "config": {"type": "object"}
                }
            })),
        }];

        let input = r#"<tool_call>
<function=process_data>
<parameter=config>
{"setting": "value", "count": 42}
</parameter>
</function>
</tool_call>"#;

        let (calls, _) =
            try_tool_call_parse_xml(input, &XmlParserConfig::default(), Some(&tools)).unwrap();
        assert_eq!(calls.len(), 1);

        let args: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
        assert!(args["config"].is_object());
        assert_eq!(args["config"]["setting"], "value");
        assert_eq!(args["config"]["count"], 42);
    }

    #[test] // PARSER.batch.3
    fn test_parse_no_tool_calls() {
        let input = "This is just normal text without any tool calls.";
        let (calls, normal) =
            try_tool_call_parse_xml(input, &XmlParserConfig::default(), None).unwrap();
        assert_eq!(calls.len(), 0);
        assert_eq!(normal, Some(input.to_string()));
    }

    #[test] // PARSER.batch.4
    fn test_parse_malformed_tool_call() {
        let input = r#"<tool_call>
<function=incomplete>
<parameter=test>
value
</tool_call>"#;

        let result = try_tool_call_parse_xml(input, &XmlParserConfig::default(), None);
        assert!(result.is_ok());
    }

    #[test] // PARSER.batch.4
    fn test_parse_missing_parameter_closing_tag() {
        let input = r#"<tool_call>
<function=execute_bash>
<parameter=command>
ls -la
</function>
</tool_call>"#;

        let (calls, _) = try_tool_call_parse_xml(input, &XmlParserConfig::default(), None).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "execute_bash");

        let args: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
        assert_eq!(args["command"], "ls -la");
    }

    #[test] // PARSER.batch.4
    fn test_parse_missing_function_closing_tag() {
        let input = r#"<tool_call>
<function=get_weather>
<parameter=city>
Boston
</parameter>
</tool_call>"#;

        let (calls, _) = try_tool_call_parse_xml(input, &XmlParserConfig::default(), None).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "get_weather");

        let args: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
        assert_eq!(args["city"], "Boston");
    }

    #[test] // PARSER.batch.4
    fn test_parse_missing_both_closing_tags() {
        let input = r#"<tool_call>
<function=run_query>
<parameter=sql>
SELECT * FROM users
</tool_call>"#;

        let (calls, _) = try_tool_call_parse_xml(input, &XmlParserConfig::default(), None).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "run_query");

        let args: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
        assert_eq!(args["sql"], "SELECT * FROM users\n</tool_call>");
    }

    #[test] // PARSER.batch.4
    fn test_parse_multiple_parameters_missing_closing_tags() {
        let input = r#"<tool_call>
<function=search>
<parameter=query>
rust programming
<parameter=limit>
10
</function>
</tool_call>"#;

        let (calls, _) = try_tool_call_parse_xml(input, &XmlParserConfig::default(), None).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "search");

        let args: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
        assert_eq!(args["query"], "rust programming\n<parameter=limit>\n10");
    }

    #[test] // PARSER.batch.5 — qwen3_coder
    fn test_parse_qwen3_no_outer_close_recovers() {
        let input = r#"<tool_call>
<function=get_weather>
<parameter=city>
NYC
</parameter>
</function>"#;

        let config = XmlParserConfig {
            allow_eof_recovery: true,
            ..XmlParserConfig::default()
        };
        let (calls, _) = try_tool_call_parse_xml(input, &config, None).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "get_weather");
        let args: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
        assert_eq!(args["city"], "NYC");
    }

    #[test]
    fn test_parse_qwen3_no_outer_close_drops_suffix() {
        let input = "<tool_call>\n<function=get_weather>\n<parameter=city>\nNYC\n</parameter>\n</function>\nTRAILING NOTE";

        let config = XmlParserConfig {
            allow_eof_recovery: true,
            ..XmlParserConfig::default()
        };
        let (calls, normal) = try_tool_call_parse_xml(input, &config, None).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(normal, Some("".to_string()));
    }

    #[test] // PARSER.batch.5 — minimax_m2
    fn test_parse_minimax_m2_no_outer_close_recovers() {
        let config = XmlParserConfig {
            tool_call_start_token: "<minimax:tool_call>".to_string(),
            tool_call_end_token: "</minimax:tool_call>".to_string(),
            function_start_token: "<invoke name=".to_string(),
            function_end_token: "</invoke>".to_string(),
            parameter_start_token: "<parameter name=".to_string(),
            parameter_end_token: "</parameter>".to_string(),
            allow_eof_recovery: true,
        };
        let input = r#"<minimax:tool_call><invoke name="get_weather"><parameter name="city">NYC</parameter></invoke>"#;

        let (calls, _) = try_tool_call_parse_xml(input, &config, None).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "get_weather");
        let args: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
        assert_eq!(args["city"], "NYC");
    }

    #[test] // helper
    fn test_schema_aware_type_conversion() {
        let tools = vec![ToolDefinition {
            name: "multi_param_func".to_string(),
            parameters: Some(serde_json::json!({
                "type": "object",
                "properties": {
                    "param1": {"type": "string"},
                    "param2": {"type": "float"},
                    "param3": {"type": "integer"},
                    "param4": {"type": "boolean"},
                    "param5": {"type": "object"},
                    "param6": {"type": "array"},
                    "param7": {"type": "null"},
                    "param8": {"type": "other_type"}
                },
                "required": ["param1", "param2", "param3", "param4", "param5", "param6", "param7", "param8"]
            })),
        }];

        let input = r#"<tool_call>
<function=multi_param_func>
<parameter=param1>42</parameter>
<parameter=param2>41.9</parameter>
<parameter=param3>42</parameter>
<parameter=param4>true</parameter>
<parameter=param5>{"key": "value"}</parameter>
<parameter=param6>[1, 2, 3]</parameter>
<parameter=param7>null</parameter>
<parameter=param8>{'arg1': 3, 'arg2': [1, 2]}</parameter>
</function>
</tool_call>"#;

        let (calls, _) =
            try_tool_call_parse_xml(input, &XmlParserConfig::default(), Some(&tools)).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "multi_param_func");

        let args: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();

        assert_eq!(args["param1"], "42");

        assert_eq!(args["param2"], 41.9);

        assert_eq!(args["param3"], 42);

        assert_eq!(args["param4"], true);

        assert_eq!(args["param5"], serde_json::json!({"key": "value"}));

        assert_eq!(args["param6"], serde_json::json!([1, 2, 3]));

        assert_eq!(args["param7"], serde_json::Value::Null);

        assert_eq!(
            args["param8"],
            serde_json::json!({"arg1": 3, "arg2": [1, 2]})
        );
    }

    #[test] // helper
    fn test_schema_aware_type_conversion_fallback() {
        let tools = vec![ToolDefinition {
            name: "test_func".to_string(),
            parameters: Some(serde_json::json!({
                "type": "object",
                "properties": {
                    "int_param": {"type": "integer"},
                    "float_param": {"type": "float"},
                    "bool_param": {"type": "boolean"}
                }
            })),
        }];

        let input = r#"<tool_call>
<function=test_func>
<parameter=int_param>not_an_int</parameter>
<parameter=float_param>not_a_float</parameter>
<parameter=bool_param>not_a_bool</parameter>
</function>
</tool_call>"#;

        let (calls, _) =
            try_tool_call_parse_xml(input, &XmlParserConfig::default(), Some(&tools)).unwrap();
        assert_eq!(calls.len(), 1);

        let args: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();

        assert_eq!(args["int_param"], "not_an_int");
        assert_eq!(args["float_param"], "not_a_float");
        assert_eq!(args["bool_param"], false);
    }

    #[test] // helper
    fn test_anyof_param_parsed_as_object_not_string() {
        let tools = vec![ToolDefinition {
            name: "get_weather".to_string(),
            parameters: Some(serde_json::json!({
                "type": "object",
                "required": ["location"],
                "properties": {
                    "location": {
                        "anyOf": [
                            {
                                "type": "object",
                                "properties": {"city": {"type": "string"}},
                                "required": ["city"]
                            },
                            {
                                "type": "object",
                                "properties": {
                                    "lat": {"type": "number"},
                                    "lon": {"type": "number"}
                                },
                                "required": ["lat", "lon"]
                            }
                        ]
                    }
                }
            })),
        }];

        let input = r#"<tool_call>
<function=get_weather>
<parameter=location>
{"city": "Paris"}
</parameter>
</function>
</tool_call>"#;

        let (calls, _) =
            try_tool_call_parse_xml(input, &XmlParserConfig::default(), Some(&tools)).unwrap();
        assert_eq!(calls.len(), 1);

        let args: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
        assert!(
            args["location"].is_object(),
            "Expected location to be an object, got: {}",
            args["location"]
        );
        assert_eq!(args["location"]["city"], "Paris");
    }

    #[test] // helper
    fn test_no_schema_fallback_behavior() {
        let input = r#"<tool_call>
<function=unknown_func>
<parameter=param1>42</parameter>
<parameter=param2>true</parameter>
<parameter=param3>hello</parameter>
</function>
</tool_call>"#;

        let (calls, _) = try_tool_call_parse_xml(input, &XmlParserConfig::default(), None).unwrap();
        assert_eq!(calls.len(), 1);

        let args: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();

        assert_eq!(args["param1"], "42");
        assert_eq!(args["param2"], "true");
        assert_eq!(args["param3"], "hello");
    }

    fn minimax_m2_config() -> XmlParserConfig {
        XmlParserConfig {
            tool_call_start_token: "<minimax:tool_call>".to_string(),
            tool_call_end_token: "</minimax:tool_call>".to_string(),
            function_start_token: "<invoke name=".to_string(),
            function_end_token: "</invoke>".to_string(),
            parameter_start_token: "<parameter name=".to_string(),
            parameter_end_token: "</parameter>".to_string(),
            allow_eof_recovery: false,
        }
    }

    #[test] // PARSER.batch.6 — qwen3_coder
    fn test_parse_qwen3_empty_args() {
        let input = r#"<tool_call>
<function=current_time>
</function>
</tool_call>"#;
        let (calls, _) = try_tool_call_parse_xml(input, &XmlParserConfig::default(), None).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "current_time");
        let args: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
        assert_eq!(args, serde_json::json!({}));
    }

    #[test] // PARSER.batch.6 — minimax_m2
    fn test_parse_minimax_m2_empty_args() {
        let config = minimax_m2_config();
        let input =
            r#"<minimax:tool_call><invoke name="current_time"></invoke></minimax:tool_call>"#;
        let (calls, _) = try_tool_call_parse_xml(input, &config, None).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "current_time");
        let args: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
        assert_eq!(args, serde_json::json!({}));
    }

    #[test]
    fn test_xml_qwen3_parser_output_independent_of_upstream_finish() {
        let input = r#"<tool_call>
<function=get_weather>
<parameter=city>
NYC
</parameter>
</function>
</tool_call>"#;
        let (calls, _) = try_tool_call_parse_xml(input, &XmlParserConfig::default(), None).unwrap();
        assert_eq!(calls.len(), 1);
    }

    #[test]
    fn test_xml_minimax_m2_parser_output_independent_of_upstream_finish() {
        let config = minimax_m2_config();
        let input = r#"<minimax:tool_call><invoke name="get_weather"><parameter name="city">NYC</parameter></invoke></minimax:tool_call>"#;
        let (calls, _) = try_tool_call_parse_xml(input, &config, None).unwrap();
        assert_eq!(calls.len(), 1);
    }

    #[test] // PARSER.batch.9 — qwen3_coder
    fn test_parse_qwen3_empty_and_whitespace_inputs() {
        for input in &["", " ", "\n", "\t\n  \t"] {
            let (calls, normal) =
                try_tool_call_parse_xml(input, &XmlParserConfig::default(), None).unwrap();
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

    #[test] // PARSER.batch.9 — minimax_m2
    fn test_parse_minimax_m2_empty_and_whitespace_inputs() {
        let config = minimax_m2_config();
        for input in &["", " ", "\n", "\t\n  \t"] {
            let (calls, normal) = try_tool_call_parse_xml(input, &config, None).unwrap();
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

    #[test] // PARSER.batch.10 — qwen3_coder
    fn test_parse_qwen3_duplicate_calls_same_name() {
        let input = r#"<tool_call>
<function=get_weather>
<parameter=city>
NYC
</parameter>
</function>
<function=get_weather>
<parameter=city>
LA
</parameter>
</function>
</tool_call>"#;
        let (calls, _) = try_tool_call_parse_xml(input, &XmlParserConfig::default(), None).unwrap();
        assert_eq!(calls.len(), 2, "Both duplicate-name calls must be returned");
        assert_eq!(calls[0].function.name, "get_weather");
        assert_eq!(calls[1].function.name, "get_weather");
        assert_ne!(
            calls[0].id, calls[1].id,
            "Duplicate calls must have distinct ids"
        );
        let args0: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
        let args1: serde_json::Value = serde_json::from_str(&calls[1].function.arguments).unwrap();
        assert_eq!(args0["city"], "NYC");
        assert_eq!(args1["city"], "LA");
    }

    #[test] // PARSER.batch.10 — minimax_m2
    fn test_parse_minimax_m2_duplicate_calls_same_name() {
        let config = minimax_m2_config();
        let input = r#"<minimax:tool_call><invoke name="get_weather"><parameter name="city">NYC</parameter></invoke><invoke name="get_weather"><parameter name="city">LA</parameter></invoke></minimax:tool_call>"#;
        let (calls, _) = try_tool_call_parse_xml(input, &config, None).unwrap();
        assert_eq!(calls.len(), 2, "Both duplicate-name calls must be returned");
        assert_eq!(calls[0].function.name, "get_weather");
        assert_eq!(calls[1].function.name, "get_weather");
        assert_ne!(
            calls[0].id, calls[1].id,
            "Duplicate calls must have distinct ids"
        );
        let args0: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
        let args1: serde_json::Value = serde_json::from_str(&calls[1].function.arguments).unwrap();
        assert_eq!(args0["city"], "NYC");
        assert_eq!(args1["city"], "LA");
    }
}
