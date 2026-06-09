// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # tool_calling::xml::parser
//!
//! ## 设计意图
//! 解析 Qwen3-Coder 风格的 XML 工具调用：
//! `<tool_call><function=name><parameter=key>value</parameter></function></tool_call>`。
//! 参考实现：
//! https://github.com/sgl-project/sglang/blob/44da737770e4bcd9bfa27751f0a0751c9b5c06e1/python/sglang/srt/function_call/qwen3_coder_detector.py
//!
//! ## 外部契约
//! - `detect_tool_call_start_xml(chunk, config)`：完整或部分起始 token 命中即 true。
//! - `find_tool_call_end_position_xml(chunk, config)`：跨连续并行调用前进至最后一个 `</tool_call>` 之后。
//! - `try_tool_call_parse_xml(message, config, tools)`：返回 `(calls, normal_text)`。
//!
//! ## 实现要点
//! - 普通文本只取首个解析成功调用之前的内容；调用之后的文本不计入响应内容。
//! - 借助 tools 的参数 schema 进行类型转换，并兼容 Python 字面量（单引号/True/False/None）。

use std::collections::HashMap;

use regex::Regex;
use serde_json::Value;
use uuid::Uuid;

use super::super::ToolDefinition;
use super::super::config::XmlParserConfig;
use super::response::{CalledFunction, ToolCallResponse, ToolCallType};

// === SECTION: 文本辅助 ===

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

// === SECTION: 流式探测与边界定位 ===

/// 判断 chunk 是否包含（或部分包含，用于流式）XML 风格工具调用的起始。
/// 格式：`<tool_call><function=name><parameter=foo>...</parameter></function></tool_call>`
pub fn detect_tool_call_start_xml(chunk: &str, config: &XmlParserConfig) -> bool {
    let start_token = config.tool_call_start_token.as_str();

    chunk.contains(start_token)
        || (1..start_token.len()).any(|i| chunk.ends_with(&start_token[..i]))
}

/// 找出所有连续 XML 工具调用的结束位置。
///
/// 当模型在一个 chunk 内发出多个并行调用
/// （如 `<tool_call>...</tool_call><tool_call>...</tool_call>`）时，本函数会跨越每一对
/// 连续的 start→end，从而把整组捕获为单一 jail 区域。返回最后一个 `</tool_call>` 之后的位置；
/// 无结束 token 时返回 chunk 长度。
pub fn find_tool_call_end_position_xml(chunk: &str, config: &XmlParserConfig) -> usize {
    let start_token = config.tool_call_start_token.as_str();
    let end_token = config.tool_call_end_token.as_str();

    // 首个结束 token：没有则说明调用不完整
    let Some(first_end) = chunk.find(end_token) else {
        return chunk.len();
    };

    let mut cursor = first_end + end_token.len();

    // 继续吞掉紧随其后的连续 <tool_call>…</tool_call> 块（允许中间有空白）
    loop {
        let rest = &chunk[cursor..];
        let trimmed = rest.trim_start();
        if !trimmed.starts_with(start_token) {
            break;
        }
        // 计算 trimmed 在原 chunk 中的起点
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

// === SECTION: 顶层解析入口 ===

/// 解析 Qwen3Coder 格式工具调用，返回 `(parsed_tool_calls, normal_text_content)`。
/// 格式：`<tool_call><function=name><parameter=key>value</parameter></function></tool_call>`
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

        // Qwen3-Coder 模板允许调用前出现自然语言，但调用块之后的文本不属于响应内容。
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
                // 外层结束 token 缺失（max_tokens / EOS 截断）。
                // 仅在 allow_eof_recovery 开启、且尾段含 function-start 结构信号时尝试恢复，
                // 从而保证以 `<tool_call>` 开头的纯文本被原样保留。
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
    // 无调用时整体 trim；有调用时保留调用前文本（含其原始空白边界）
    let normal_text = if calls.is_empty() {
        joined.trim().to_string()
    } else {
        joined
    };
    Ok((normal_text, calls))
}

/// 解析单个工具调用块。
/// 格式：`<tool_call><function=name><parameter=key>value</parameter>...</function></tool_call>`
fn parse_tool_call_block(
    block: &str,
    config: &XmlParserConfig,
    tools: Option<&[ToolDefinition]>,
) -> anyhow::Result<Vec<ToolCallResponse>> {
    // 依据 config 构造正则
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

    // 遍历所有 function 块
    for func_cap in function_regex.captures_iter(block) {
        let function_name_raw = func_cap.get(1).map(|m| m.as_str().trim()).unwrap_or("");
        let function_name = strip_quotes(function_name_raw);
        let function_body = func_cap.get(2).map(|m| m.as_str()).unwrap_or("");

        if function_name.is_empty() {
            continue;
        }

        // 取该函数的参数 schema
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

// === SECTION: 参数 schema 与类型转换 ===

/// 从工具定义中取出某函数的参数配置，返回 参数名 → schema 定义 的映射。
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

    // 优先取 "properties"；若无则把整个对象当作配置
    let source = params.get("properties").or(Some(params));
    source
        .and_then(|v| v.as_object())
        .map(|obj| obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default()
}

/// 依据 schema 中的类型把 XML 字符串参数转换为带类型的 JSON Value，
/// 行为对齐 Python 参考实现。
///
/// 识别的类型别名：
/// - string: "string"/"str"/"text"/"varchar"/"char"/"enum"
/// - integer: "int"/"integer"/"int32"/"int64"/"uint"/"long"/"short"/"unsigned"
/// - number: "number"/"num"/"float"/"float32"/"float64"/"double"
/// - boolean: "boolean"/"bool"/"binary"
/// - object: "object"/"dict"/"dictionary"
/// - array: "array"/"arr"/"list"
///
/// 特殊情形：值为 "null" 直接返回 Null；HTML 实体会被反转义；未在 schema 中声明的参数按字符串返回。
fn convert_param_value(
    param_value: &str,
    param_name: &str,
    param_config: &HashMap<String, Value>,
    func_name: &str,
) -> Value {
    // HTML 反转义并裁剪空白
    let param_value = html_unescape(param_value.trim());

    // 处理 null
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

    // 取类型；anyOf/oneOf 无直接 "type" 时当作 object，使值走 JSON 解析而非二次编码字符串
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

    // 下面的 match 对每类类型：匹配别名 → 解析 → 失败则告警并退化为字符串
    match param_type.as_str() {
        // 字符串类：原样返回（上面已做 HTML 反转义）
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

        // 浮点/数值类：解析为 f64；整数值（如 42.0）以整数存储以提升 JSON 兼容性
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

        // 布尔类：仅 "true"/"false"（忽略大小写）有效，其余默认 false 并告警
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

        // 复杂类（对象/数组）：先 JSON 解析，再退回 Python 字面量解析（单引号等）
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

        // 未知/自定义类型：尽力用 literal_eval 解析结构化数据
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

/// 模拟 Python `ast.literal_eval` 的简化版本，处理常见情形。
fn try_literal_eval(s: &str) -> Result<Value, ()> {
    // 先试标准 JSON
    if let Ok(val) = serde_json::from_str::<Value>(s) {
        return Ok(val);
    }

    // 再处理 Python 风格字面量（单引号、True/False/None）
    let normalized = s
        .replace('\'', "\"")
        .replace("True", "true")
        .replace("False", "false")
        .replace("None", "null");

    serde_json::from_str::<Value>(&normalized).map_err(|_| ())
}

/// 安全解析值——先 JSON，再退化为字符串。语义上模仿 SGLang 的 `_safe_val`。
/// 注：此函数已弃用，仅保留作参考。请用 convert_param_value 替代。
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

/// 针对常见实体的简单 HTML 反转义。
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
    //! 围绕 XML 公开 API（`detect_tool_call_start_xml`、`find_tool_call_end_position_xml`、
    //! `try_tool_call_parse_xml`）与若干内部辅助（`safe_parse_value`/`html_unescape`）展开，
    //! 覆盖起始/边界探测、并行调用、参数类型转换、Python 字面量、截断恢复等。
    //!
    //! ## 意义
    //! 锁定 Qwen3-Coder 风格 XML 调用在多调用、类型强制与截断边界下的可观察行为。
    use super::*;
    use rstest::rstest;

    #[test] // helper
    fn test_detect_tool_call_start() {
        let config = XmlParserConfig::default();
        assert!(detect_tool_call_start_xml("<tool_call>", &config));
        assert!(detect_tool_call_start_xml("text <tool_call>", &config));
        assert!(detect_tool_call_start_xml("<tool_c", &config)); // 部分匹配
        assert!(detect_tool_call_start_xml("<", &config)); // 部分匹配
        assert!(!detect_tool_call_start_xml("no tool call here", &config));
        assert!(!detect_tool_call_start_xml("toolcall", &config));
    }

    #[test] // helper
    fn test_find_tool_call_end_position() {
        let config = XmlParserConfig::default();
        let text = "<tool_call><function=test></function></tool_call>more text";
        let pos = find_tool_call_end_position_xml(text, &config);
        assert_eq!(pos, 49); // </tool_call> 之后的位置
        assert_eq!(&text[pos..], "more text");

        let text_no_end = "<tool_call><function=test>";
        let pos = find_tool_call_end_position_xml(text_no_end, &config);
        assert_eq!(pos, text_no_end.len());
    }

    /// 针对 issue #6822 的回归测试：单个 chunk 中的并行工具调用必须全部被
    /// `find_tool_call_end_position_xml` 捕获，使 jail 将整组传入 `extract_tool_calls`
    /// 而非将第二个（及后续）调用当作原始尾部文本发出。
    #[test] // PARSER.batch.2, helper
    fn test_find_tool_call_end_position_parallel_calls() {
        let config = XmlParserConfig::default();

        // 两个并行调用，之间无空白。
        let two_calls = "<tool_call><function=foo><parameter=x>1</parameter></function></tool_call>\
                         <tool_call><function=bar><parameter=y>2</parameter></function></tool_call>\
                         trailing";
        let pos = find_tool_call_end_position_xml(two_calls, &config);
        // "trailing" 之前（不含 "trailing"）的所有内容均应被捕获。
        assert!(
            &two_calls[..pos].ends_with("</tool_call>"),
            "should end at last </tool_call>, got: {:?}",
            &two_calls[..pos]
        );
        assert_eq!(&two_calls[pos..], "trailing");

        // 三个并行调用，以空白/换行分隔。
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

        // 第二个调用不完整——应在第一个完整调用后停止。
        let incomplete = "<tool_call><function=a></function></tool_call>\
                          <tool_call><function=b>"; // no </tool_call>
        let pos_inc = find_tool_call_end_position_xml(incomplete, &config);
        // 第一个完整调用在首个块的长度位置处结束。
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
        // 使用 schema 感知解析，需提供 schema 来解析 JSON 对象
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

        // 应优雅处理——可能解析或返回空
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
        // 与原始 SGLang Python 实现一致。
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
        // 与原始 SGLang Python 实现一致。
        assert_eq!(args["query"], "rust programming\n<parameter=limit>\n10");
    }

    // 针对缺失外层 </tool_call>（max_tokens / EOS 截断）的恢复：
    // 当内部函数块格式正确时，将 EOF 视为结束 token 并提取调用。
    // 恢复由尾部切片中的函数起始标记控制入口，使恰好以 `<tool_call>` 开头的
    // 纯文本仍被原样保留。
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

    // Qwen3-Coder 风格 XML 将首个已解析工具调用之后的文本视为
    // 非内容，含 EOF 恢复后也是如此。
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
        // 本测试对应 Python test_parse_streaming_increment_multiple_parameters，
        // 来自 diff，展示了模式感知的类型转换。
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

        // param1 类型为 string，"42" 保持为字符串
        assert_eq!(args["param1"], "42");

        // param2 类型为 float，41.9 解析为浮点数
        assert_eq!(args["param2"], 41.9);

        // param3 类型为 integer，42 解析为整数
        assert_eq!(args["param3"], 42);

        // param4 类型为 boolean，"true" 解析为布尔
        assert_eq!(args["param4"], true);

        // param5 类型为 object，JSON 被解析
        assert_eq!(args["param5"], serde_json::json!({"key": "value"}));

        // param6 类型为 array，JSON 数组被解析
        assert_eq!(args["param6"], serde_json::json!([1, 2, 3]));

        // param7 类型为 null，"null" 解析为 null
        assert_eq!(args["param7"], serde_json::Value::Null);

        // param8 为 other_type，用 literal_eval 转换 Python 风格 dict
        assert_eq!(
            args["param8"],
            serde_json::json!({"arg1": 3, "arg2": [1, 2]})
        );
    }

    #[test] // helper
    fn test_schema_aware_type_conversion_fallback() {
        // 测试无效值带警告回退为字符串
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

        // 全部应回退为字符串
        assert_eq!(args["int_param"], "not_an_int");
        assert_eq!(args["float_param"], "not_a_float");
        // 无效值的 bool_param 默认为 false
        assert_eq!(args["bool_param"], false);
    }

    #[test] // helper
    fn test_anyof_param_parsed_as_object_not_string() {
        // 当工具参数使用 "anyOf" 而非直接 "type" 时，值
        // 应被 JSON 解析（视为对象），而非双重编码为字符串。
        // 回归测试，针对：https://github.com/vllm-project/vllm/pull/36032
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
        // 必须是正确的对象，而非双重编码的字符串如 "{\"city\": \"Paris\"}"
        assert!(
            args["location"].is_object(),
            "Expected location to be an object, got: {}",
            args["location"]
        );
        assert_eq!(args["location"]["city"], "Paris");
    }

    #[test] // helper
    fn test_no_schema_fallback_behavior() {
        // 无 schema 时应与旧的 safe_parse_value 逻辑行为一致
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

        // 无 schema 时，所有值均作为字符串返回（无类型推断）
        assert_eq!(args["param1"], "42");
        assert_eq!(args["param2"], "true");
        assert_eq!(args["param3"], "hello");
    }

    /// 以下新增边界情况测试（PARSER.batch.6 / PIPELINE.finish_reason / PARSER.batch.9
    /// / PARSER.batch.10）的辅助函数——`allow_eof_recovery: false`，因为这些测试均
    /// 不依赖 EOF 恢复。`test_parse_minimax_m2_no_outer_close_recovers` 中的内联配置
    /// 保持该标志为 `true`，因为该测试专门练习恢复路径。
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

    /// PARSER.batch.6 — 空参数。无参调用（无 `<parameter=...>` 块）
    /// 仍应暴露函数名（带空参数）。
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

    /// PARSER.batch.6 — empty args, minimax_m2 format.
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

    /// 解析器级不变量：xml 解析器是字节稳定的——不看 `finish_reason`，
    /// 无论上游流结束原因如何输出均相同。实际的 PIPELINE.finish_reason 覆盖
    /// （stop / tool_calls / length 映射）在 `lib/llm/tests/test_streaming_tool_parsers.rs`
    /// 且属于跨解析器 finish_reason 映射工作项（单独跟踪）。
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

    /// 解析器级不变量——minimax_m2 变体。原理参见 qwen3 对应项。
    #[test]
    fn test_xml_minimax_m2_parser_output_independent_of_upstream_finish() {
        let config = minimax_m2_config();
        let input = r#"<minimax:tool_call><invoke name="get_weather"><parameter name="city">NYC</parameter></invoke></minimax:tool_call>"#;
        let (calls, _) = try_tool_call_parse_xml(input, &config, None).unwrap();
        assert_eq!(calls.len(), 1);
    }

    /// PARSER.batch.9 — empty / null content variants. Truly-empty (zero bytes)
    /// 空白输入不得产生工具调用；normal_text 折叠为空字符串。在 qwen3_coder 和
    /// minimax_m2 两种配置下均验证。
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

    /// PARSER.batch.10 — 重复调用（同一函数名两次）。qwen3_coder 格式；
    /// 锁定解析器级行为——两次调用均返回且 ID 不同。
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

    /// PARSER.batch.10 — 重复调用（同一函数名两次）。minimax_m2 格式；
    /// 锁定解析器级行为——两次调用均返回且 ID 不同。
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
