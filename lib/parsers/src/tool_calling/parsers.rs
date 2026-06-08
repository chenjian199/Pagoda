// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-License-Identifier: Apache-2.0

//! ## 设计意图
//! 结束定位三组入口。
//!
//! ## 外部契约
//!
//! ## 实现要点

use super::ToolDefinition;
use super::config::{ParserConfig, ToolCallConfig};
use super::dsml::{
    detect_tool_call_start_dsml, find_tool_call_end_position_dsml, try_tool_call_parse_dsml,
};
use super::gemma4::{
    detect_tool_call_start_gemma4, find_tool_call_end_position_gemma4, try_tool_call_parse_gemma4,
};
use super::harmony::{
    detect_tool_call_start_harmony, find_tool_call_end_position_harmony,
    parse_tool_calls_harmony_complete,
};
use super::json::{
    detect_tool_call_start_json, find_tool_call_end_position_json, try_tool_call_parse_json,
};
use super::pythonic::{
    detect_tool_call_start_pythonic, find_tool_call_end_position_pythonic,
    try_tool_call_parse_pythonic,
};
use super::response::ToolCallResponse;
use super::xml::{
    detect_tool_call_start_glm47, detect_tool_call_start_kimi_k2, detect_tool_call_start_xml,
    find_tool_call_end_position_glm47, find_tool_call_end_position_kimi_k2,
    find_tool_call_end_position_xml, try_tool_call_parse_glm47, try_tool_call_parse_kimi_k2,
    try_tool_call_parse_xml,
};
use std::collections::HashMap;
use std::sync::OnceLock;


static PARSER_MAP: OnceLock<HashMap<&'static str, ToolCallConfig>> = OnceLock::new();

pub fn get_tool_parser_map() -> &'static HashMap<&'static str, ToolCallConfig> {
    PARSER_MAP.get_or_init(|| {
        // 别名表：(名称, 配置构造器)；含历史别名与同格式复用
        let entries: [(&'static str, ToolCallConfig); 23] = [
            ("hermes", ToolCallConfig::hermes()),
            ("nemotron_deci", ToolCallConfig::nemotron_deci()),
            ("llama3_json", ToolCallConfig::llama3_json()),
            ("mistral", ToolCallConfig::mistral()),
            ("phi4", ToolCallConfig::phi4()),
            ("pythonic", ToolCallConfig::pythonic()),
            ("harmony", ToolCallConfig::harmony()),
            ("deepseek_v3", ToolCallConfig::deepseek_v3()),
            ("deepseek_v3_1", ToolCallConfig::deepseek_v3_1()),
            ("deepseek_v3_2", ToolCallConfig::deepseek_v3_2()),
            ("deepseek_v4", ToolCallConfig::deepseek_v4()),
            ("deepseek-v4", ToolCallConfig::deepseek_v4()),
            ("deepseekv4", ToolCallConfig::deepseek_v4()),
            ("qwen3_coder", ToolCallConfig::qwen3_coder()),
            ("jamba", ToolCallConfig::jamba()),
            ("minimax_m2", ToolCallConfig::minimax_m2()),
            ("glm47", ToolCallConfig::glm47()),
            ("kimi_k2", ToolCallConfig::kimi_k2()),
            ("gemma4", ToolCallConfig::gemma4()),
            ("gemma-4", ToolCallConfig::gemma4()),
            ("default", ToolCallConfig::default()),
            ("nemotron_nano", ToolCallConfig::qwen3_coder()),
            ("qwen25", ToolCallConfig::hermes()),
        ];
        entries.into_iter().collect()
    })
}

pub fn get_available_tool_parsers() -> Vec<&'static str> {
    get_tool_parser_map().keys().copied().collect()
}


pub async fn try_tool_call_parse(
    message: &str,
    config: &ToolCallConfig,
    tools: Option<&[ToolDefinition]>,
) -> anyhow::Result<(Vec<ToolCallResponse>, Option<String>)> {
    match &config.parser_config {
        ParserConfig::Json(json_config) => try_tool_call_parse_json(message, json_config, tools),
        ParserConfig::Harmony(json_config) => {
            parse_tool_calls_harmony_complete(message, json_config, tools).await
        }
        ParserConfig::Pythonic => try_tool_call_parse_pythonic(message, tools),
        ParserConfig::Typescript => anyhow::bail!("Typescript parser not implemented"),
        ParserConfig::Xml(xml_config) => try_tool_call_parse_xml(message, xml_config, tools),
        ParserConfig::Dsml(dsml_config) => try_tool_call_parse_dsml(message, dsml_config),
        ParserConfig::Glm47(glm47_config) => try_tool_call_parse_glm47(message, glm47_config, tools),
        ParserConfig::KimiK2(kimi_config) => try_tool_call_parse_kimi_k2(message, kimi_config, tools),
        ParserConfig::Gemma4 => try_tool_call_parse_gemma4(message, tools),
    }
}


fn resolve_parser_key(parser_str: Option<&str>) -> &str {
    match parser_str {
        Some(s) if !s.is_empty() => s,
        _ => "default",
    }
}

/// 按名称查找配置，未知名称返回带可用列表的错误。
fn lookup_config(parser_key: &str) -> anyhow::Result<&'static ToolCallConfig> {
    get_tool_parser_map().get(parser_key).ok_or_else(|| {
        anyhow::anyhow!(
            "Parser '{}' is not implemented. Available parsers: {:?}",
            parser_key,
            get_available_tool_parsers()
        )
    })
}

pub async fn detect_and_parse_tool_call_with_recovery(
    message: &str,
    parser_str: Option<&str>,
    tools: Option<&[ToolDefinition]>,
) -> anyhow::Result<(Vec<ToolCallResponse>, Option<String>)> {
    let base = lookup_config(resolve_parser_key(parser_str))?;

    let recovery_config = match &base.parser_config {
        ParserConfig::Json(c) => {
            let mut c = c.clone();
            c.allow_eof_recovery = true;
            ParserConfig::Json(c)
        }
        ParserConfig::Xml(c) => {
            let mut c = c.clone();
            c.allow_eof_recovery = true;
            ParserConfig::Xml(c)
        }
        ParserConfig::Glm47(c) => {
            let mut c = c.clone();
            c.allow_eof_recovery = true;
            ParserConfig::Glm47(c)
        }
        other => other.clone(),
    };
    let cfg = ToolCallConfig {
        parser_config: recovery_config,
    };
    try_tool_call_parse(message, &cfg, tools).await
}

/// 所有工具解析的基础入口。
pub async fn detect_and_parse_tool_call(
    message: &str,
    parser_str: Option<&str>,
    tools: Option<&[ToolDefinition]>,
) -> anyhow::Result<(Vec<ToolCallResponse>, Option<String>)> {
    let config = lookup_config(resolve_parser_key(parser_str))?;
    try_tool_call_parse(message, config, tools).await
}

pub fn detect_tool_call_start(chunk: &str, parser_str: Option<&str>) -> anyhow::Result<bool> {
    let config = lookup_config(resolve_parser_key(parser_str))?;
    match &config.parser_config {
        ParserConfig::Json(json_config) => Ok(detect_tool_call_start_json(chunk, json_config)),
        ParserConfig::Harmony(json_config) => {
            Ok(detect_tool_call_start_harmony(chunk, json_config, false))
        }
        ParserConfig::Pythonic => Ok(detect_tool_call_start_pythonic(chunk)),
        ParserConfig::Typescript => anyhow::bail!("Typescript parser not implemented"),
        ParserConfig::Xml(xml_config) => Ok(detect_tool_call_start_xml(chunk, xml_config)),
        ParserConfig::Dsml(dsml_config) => Ok(detect_tool_call_start_dsml(chunk, dsml_config)),
        ParserConfig::Glm47(glm47_config) => Ok(detect_tool_call_start_glm47(chunk, glm47_config)),
        ParserConfig::KimiK2(kimi_config) => Ok(detect_tool_call_start_kimi_k2(chunk, kimi_config)),
        ParserConfig::Gemma4 => Ok(detect_tool_call_start_gemma4(chunk)),
    }
}

pub fn find_tool_call_end_position(chunk: &str, parser_str: Option<&str>) -> Option<usize> {
    let parser_key = resolve_parser_key(parser_str);
    // 未知解析器名时保守返回整段长度（与既有行为一致）
    let Ok(config) = lookup_config(parser_key) else {
        return Some(chunk.len());
    };

    match &config.parser_config {
        ParserConfig::Json(json_config) => {
            let effective_parser = if parser_key == "default" {
                "nemotron_deci"
            } else {
                parser_key
            };
            Some(find_tool_call_end_position_json(
                chunk,
                effective_parser,
                json_config,
            ))
        }
        ParserConfig::Harmony(json_config) => {
            Some(find_tool_call_end_position_harmony(chunk, json_config))
        }
        ParserConfig::Pythonic => Some(find_tool_call_end_position_pythonic(chunk)),
        ParserConfig::Typescript => Some(chunk.len()),
        ParserConfig::Xml(xml_config) => Some(find_tool_call_end_position_xml(chunk, xml_config)),
        ParserConfig::Dsml(dsml_config) => {
            Some(find_tool_call_end_position_dsml(chunk, dsml_config))
        }
        ParserConfig::Glm47(glm47_config) => {
            Some(find_tool_call_end_position_glm47(chunk, glm47_config))
        }
        ParserConfig::KimiK2(kimi_config) => find_tool_call_end_position_kimi_k2(chunk, kimi_config),
        ParserConfig::Gemma4 => find_tool_call_end_position_gemma4(chunk),
    }
}

#[cfg(test)]
mod tests {
    //! ## 测试过程
    //!
    //! ## 意义
    //! 锁定总分发层在全部已注册解析器名下的路由正确性与可观察行为。
    use super::super::config::JsonParserConfig;
    use super::*;

    fn extract_name_and_args(call: ToolCallResponse) -> (String, serde_json::Value) {
        let args: serde_json::Value = serde_json::from_str(&call.function.arguments).unwrap();
        (call.function.name, args)
    }

    #[test]
    fn test_get_available_tool_parsers() {
        let parsers = get_available_tool_parsers();
        assert!(!parsers.is_empty());
        let available_parsers = [
            "hermes",
            "llama3_json",
            "harmony",
            "nemotron_deci",
            "mistral",
            "phi4",
            "default",
            "pythonic",
            "deepseek_v3",
            "deepseek_v3_1",
            "deepseek_v3_2",
            "deepseek_v4",
            "deepseek-v4",
            "deepseekv4",
            "qwen3_coder",
            "jamba",
            "nemotron_nano",
            "minimax_m2",
            "glm47",
            "kimi_k2",
            "gemma4",
            "gemma-4",
            "qwen25",
        ];
        for parser in available_parsers {
            assert!(parsers.contains(&parser));
        }
    }

    #[tokio::test]
    async fn parses_single_parameters_object() {
        let input = r#"{ "name": "hello", "parameters": { "x": 1, "y": 2 } }"#;
        let (result, content) = try_tool_call_parse(input, &ToolCallConfig::default(), None)
            .await
            .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "hello");
        assert_eq!(args["x"], 1);
        assert_eq!(args["y"], 2);
    }

    #[tokio::test]
    async fn parses_single_arguments_object() {
        let input = r#"{ "name": "world", "arguments": { "a": "abc", "b": 42 } }"#;
        let (result, content) = try_tool_call_parse(input, &ToolCallConfig::default(), None)
            .await
            .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "world");
        assert_eq!(args["a"], "abc");
        assert_eq!(args["b"], 42);
    }

    #[tokio::test]
    async fn parses_vec_of_parameters() {
        let input = r#"[{ "name": "first", "parameters": { "a": 1 } }, { "name": "second", "parameters": { "b": 2 } }]"#;
        let (result, content) = try_tool_call_parse(input, &ToolCallConfig::default(), None)
            .await
            .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 2);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "first");
        assert_eq!(args["a"], 1);
        let (name, args) = extract_name_and_args(result[1].clone());
        assert_eq!(name, "second");
        assert_eq!(args["b"], 2);
    }

    #[tokio::test]
    async fn parses_vec_of_arguments() {
        let input = r#"[{ "name": "alpha", "arguments": { "a": "x" } }, { "name": "omega", "arguments": { "z": "y" } }]"#;
        let (result, content) = try_tool_call_parse(input, &ToolCallConfig::default(), None)
            .await
            .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 2);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "alpha");
        assert_eq!(args["a"], "x");
        let (name, args) = extract_name_and_args(result[1].clone());
        assert_eq!(name, "omega");
        assert_eq!(args["z"], "y");
    }

    #[tokio::test]
    async fn parses_toolcall_wrapped_payload() {
        let input =
            r#"<TOOLCALL>[{ "name": "wrapped", "parameters": { "foo": "bar" } }]</TOOLCALL>"#;
        let (result, content) = try_tool_call_parse(input, &ToolCallConfig::default(), None)
            .await
            .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "wrapped");
        assert_eq!(args["foo"], "bar");
    }

    #[tokio::test]
    async fn parses_python_tag_prefixed_payload() {
        let input = r#"<|python_tag|>{ "name": "pyfunc", "arguments": { "k": "v" } }"#;
        let (result, content) = try_tool_call_parse(
            input,
            &ToolCallConfig {
                parser_config: ParserConfig::Json(JsonParserConfig {
                    tool_call_start_tokens: vec!["<|python_tag|>".to_string()],
                    tool_call_end_tokens: vec!["".to_string()],
                    ..Default::default()
                }),
            },
            None,
        )
        .await
        .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "pyfunc");
        assert_eq!(args["k"], "v");
    }

    #[tokio::test]
    async fn returns_none_on_invalid_input() {
        let input = r#"not even json"#;
        let (result, content) = try_tool_call_parse(input, &ToolCallConfig::default(), None)
            .await
            .unwrap();
        assert_eq!(content, Some("not even json".to_string()));
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn returns_none_on_valid_json_wrong_shape() {
        let input = r#"{ "foo": "bar" }"#;
        let (result, content) = try_tool_call_parse(input, &ToolCallConfig::default(), None)
            .await
            .unwrap();
        assert_eq!(content, Some("{ \"foo\": \"bar\" }".to_string()));
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_nvidia_llama3_nemotron_super_49b_simple() {
        let input = r#"<think>
Okay, the user is asking for the weather in San Francisco in Fahrenheit. Let me check the tools available.
</think>

<TOOLCALL>[{"name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit"}}]</TOOLCALL>"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("nemotron_deci"), None)
            .await
            .unwrap();
        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        assert_eq!(content, Some("<think>\nOkay, the user is asking for the weather in San Francisco in Fahrenheit. Let me check the tools available.\n</think>".to_string()));
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_nvidia_llama3_nemotron_super_49b_simple_with_no_think() {
        let input = r#"<TOOLCALL>[{"name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit"}}]</TOOLCALL>"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("nemotron_deci"), None)
            .await
            .unwrap();
        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        assert_eq!(content, Some("".to_string()));
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_nvidia_llama3_nemotron_super_49b_with_function_array() {
        let input = r#"<think>
Okay, the user is asking for the weather in San Francisco in Fahrenheit. Let me check the tools available.
</think>

<TOOLCALL>[{"name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit"}}, {"name": "get_weather", "arguments": {"location": "New York, NY", "unit": "fahrenheit"}}]</TOOLCALL>"#;
        let config = ToolCallConfig::nemotron_deci();
        let (result, content) = try_tool_call_parse(input, &config, None).await.unwrap();
        assert_eq!(content, Some("<think>\nOkay, the user is asking for the weather in San Francisco in Fahrenheit. Let me check the tools available.\n</think>".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 2);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
        let (name, args) = extract_name_and_args(result[1].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "New York, NY");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_nvidia_llama3_nemotron_super_49b_with_function_array_with_new_lines() {
        let input = r#"<think>
Okay, the user is asking for the weather in San Francisco in Fahrenheit. Let me check the tools available.
</think>

<TOOLCALL>
[{"name": "get_weather",
 "arguments": {"location": "San Francisco, CA",
  "unit": "fahrenheit"}},
  {"name": "get_weather",
   "arguments":
  {"location": "New York, NY",
  "unit": "fahrenheit"}}]
  </TOOLCALL>
  "#;
        let config = ToolCallConfig::nemotron_deci();
        let (result, content) = try_tool_call_parse(input, &config, None).await.unwrap();
        assert_eq!(content, Some("<think>\nOkay, the user is asking for the weather in San Francisco in Fahrenheit. Let me check the tools available.\n</think>".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 2);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
        let (name, args) = extract_name_and_args(result[1].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "New York, NY");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_qwen_qwq_32b_simple() {
        let input = r#"<tool_call>
{"name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit"}}
</tool_call>"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("hermes"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_qwen_qwq_32b_simple_with_normal_text() {
        let input = r#"Hey How are you? <tool_call>
{"name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit"}}
</tool_call>"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("hermes"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("Hey How are you?".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn test_nousresearch_hermes3_llama31_8b_simple() {
        let input = r#"<tool_call>
{"name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit"}}
</tool_call>"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("hermes"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_qwen_qwq_32b_multiple_tool_calls() {
        let input = r#"<tool_call>
{"name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit"}}
</tool_call>
<tool_call>
{"name": "get_weather", "arguments": {"location": "New York, NY", "unit": "fahrenheit"}}
</tool_call>
"#;
        let config = ToolCallConfig::hermes();
        let (result, content) = try_tool_call_parse(input, &config, None).await.unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 2);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
        let (name, args) = extract_name_and_args(result[1].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "New York, NY");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_qwen_qwq_32b_multiple_tool_calls_with_normal_text() {
        let input = r#"Hey How are you? <tool_call>
{"name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit"}}
</tool_call>
<tool_call>
{"name": "get_weather", "arguments": {"location": "New York, NY", "unit": "fahrenheit"}}
</tool_call>
"#;
        let config = ToolCallConfig::hermes();
        let (result, content) = try_tool_call_parse(input, &config, None).await.unwrap();
        assert_eq!(content, Some("Hey How are you?".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 2);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
        let (name, args) = extract_name_and_args(result[1].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "New York, NY");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_qwen_qwq_32b_multiple_tool_calls_with_new_lines() {
        let input = r#"<tool_call>
{"name": "get_weather",
"arguments": {"location": "San Francisco, CA",
"unit": "fahrenheit"}}
</tool_call>
<tool_call>
{"name": "get_weather", "arguments":
{"location": "New York, NY", "unit":
"fahrenheit"}}
</tool_call>
"#;
        let config = ToolCallConfig::hermes();
        let (result, content) = try_tool_call_parse(input, &config, None).await.unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 2);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
        let (name, args) = extract_name_and_args(result[1].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "New York, NY");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    #[ignore]
    async fn test_ibm_granite_40_tiny_preview_simple() {
        let input = r#"[{"arguments": {"location": "San Francisco, CA", "unit": "fahrenheit"}, "name": "get_weather"}]"#;
        let config = ToolCallConfig {
            parser_config: ParserConfig::Json(JsonParserConfig {
                tool_call_start_tokens: vec![],
                tool_call_end_tokens: vec![],
                arguments_keys: vec!["arguments".to_string()],
                ..Default::default()
            }),
        };
        let (result, content) = try_tool_call_parse(input, &config, None).await.unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_mistralai_mistral_7b_instruct_v03_simple() {
        let input = r#" [{"name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit"}}]"#;
        let config = ToolCallConfig::mistral();
        let (result, content) = try_tool_call_parse(input, &config, None).await.unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_mistralai_mistral_7b_instruct_v03_simple_with_normal_text() {
        let input = r#"Hey How are you? [{"name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit"}}]"#;
        let config = ToolCallConfig::mistral();
        let (result, content) = try_tool_call_parse(input, &config, None).await.unwrap();
        assert_eq!(content, Some("Hey How are you?".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_mistralai_mistral_7b_instruct_v03_simple_with_new_lines() {
        let input = r#"
        [{"name": "get_weather",
        "arguments": {"location":
        "San Francisco, CA",
        "unit": "fahrenheit"}}]
        "#;
        let config = ToolCallConfig::mistral();
        let (result, content) = try_tool_call_parse(input, &config, None).await.unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_mistralai_mistral_7b_instruct_v03_multiple() {
        let input = r#" [{"name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit"}}, {"name": "get_weather", "arguments": {"location": "New York, NY", "unit": "fahrenheit"}}]"#;
        let config = ToolCallConfig::mistral();
        let (result, content) = try_tool_call_parse(input, &config, None).await.unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 2);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
        let (name, args) = extract_name_and_args(result[1].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "New York, NY");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_mistralai_mistral_7b_instruct_v03_multiple_with_normal_text() {
        let input = r#"Hey How are you? [{"name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit"}}, {"name": "get_weather", "arguments": {"location": "New York, NY", "unit": "fahrenheit"}}]"#;
        let config = ToolCallConfig::mistral();
        let (result, content) = try_tool_call_parse(input, &config, None).await.unwrap();
        assert_eq!(content, Some("Hey How are you?".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 2);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
        let (name, args) = extract_name_and_args(result[1].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "New York, NY");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_mistralai_mistral_7b_instruct_v03_multiple_with_new_lines() {
        let input = r#"
        [{"name": "get_weather",
        "arguments": {"location":
        "San Francisco, CA",
        "unit": "fahrenheit"}},
        {"name": "get_weather", "arguments":
        {"location": "New York, NY", "unit":
        "fahrenheit"}}]
        "#;
        let config = ToolCallConfig::mistral();
        let (result, content) = try_tool_call_parse(input, &config, None).await.unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 2);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
        let (name, args) = extract_name_and_args(result[1].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "New York, NY");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_mistralai_mistral_7b_instruct_v03_single_with_start_token() {
        let input = r#"[TOOL_CALLS] [{"name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit"}}]"#;
        let config = ToolCallConfig::mistral();
        let (result, content) = try_tool_call_parse(input, &config, None).await.unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_mistralai_mistral_7b_instruct_v03_single_with_start_token_with_normal_text() {
        let input = r#"Hey How are you? [TOOL_CALLS] [{"name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit"}}]"#;
        let config = ToolCallConfig::mistral();
        let (result, content) = try_tool_call_parse(input, &config, None).await.unwrap();
        assert_eq!(content, Some("Hey How are you?".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_mistralai_mistral_7b_instruct_v03_single_with_start_tokenwith_new_lines() {
        let input = r#"
        [TOOL_CALLS]
        [{"name": "get_weather",
        "arguments": {"location":
        "San Francisco, CA",
        "unit": "fahrenheit"}}]
        "#;
        let config = ToolCallConfig::mistral();
        let (result, content) = try_tool_call_parse(input, &config, None).await.unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_mistralai_mistral_7b_instruct_v03_single_with_start_token_multiple() {
        let input = r#"[TOOL_CALLS] [{"name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit"}}, {"name": "get_weather", "arguments": {"location": "New York, NY", "unit": "fahrenheit"}}]"#;
        let config = ToolCallConfig::mistral();
        let (result, content) = try_tool_call_parse(input, &config, None).await.unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 2);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
        let (name, args) = extract_name_and_args(result[1].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "New York, NY");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_mistralai_mistral_7b_instruct_v03_single_with_start_token_multiple_with_normal_text()
     {
        let input = r#"Hey How are you? [TOOL_CALLS] [{"name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit"}}, {"name": "get_weather", "arguments": {"location": "New York, NY", "unit": "fahrenheit"}}]"#;
        let config = ToolCallConfig::mistral();
        let (result, content) = try_tool_call_parse(input, &config, None).await.unwrap();
        assert_eq!(content, Some("Hey How are you?".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 2);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
        let (name, args) = extract_name_and_args(result[1].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "New York, NY");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_mistralai_mistral_7b_instruct_v03_single_with_start_token_multiple_with_new_lines()
     {
        let input = r#"
        [TOOL_CALLS]
        [{"name": "get_weather",
        "arguments": {"location":
        "San Francisco, CA",
        "unit": "fahrenheit"}},
        {"name": "get_weather", "arguments":
        {"location": "New York, NY", "unit":
        "fahrenheit"}}]
        "#;
        let config = ToolCallConfig::mistral();
        let (result, content) = try_tool_call_parse(input, &config, None).await.unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 2);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
        let (name, args) = extract_name_and_args(result[1].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "New York, NY");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_meta_llama_llama31_8b_instruct_simple() {
        let input = r#"{"name": "get_weather", "parameters": {"location": "San Francisco, CA", "unit": "fahrenheit"}}"#;
        let (result, content) = try_tool_call_parse(input, &ToolCallConfig::mistral(), None)
            .await
            .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_meta_llama_llama31_8b_instruct_simple_with_normal_text() {
        let input = r#"Hey How are you? {"name": "get_weather", "parameters": {"location": "San Francisco, CA", "unit": "fahrenheit"}}"#;
        let (result, content) = try_tool_call_parse(input, &ToolCallConfig::mistral(), None)
            .await
            .unwrap();
        assert_eq!(content, Some("Hey How are you?".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_meta_llama_llama31_8b_instruct_with_new_lines() {
        let input = r#"
        {"name": "get_weather",
        "parameters": {"location": "San Francisco, CA", "unit": "fahrenheit"}}
        "#;
        let (result, content) = detect_and_parse_tool_call(input, Some("llama3_json"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_meta_llama_llama31_8b_instruct_with_python_tag() {
        let input = r#"<|python_tag|>{ "name": "get_weather", "parameters": {"location": "San Francisco, CA", "unit": "fahrenheit" } }"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("llama3_json"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_meta_llama_llama31_8b_instruct_with_python_tag_with_normal_text() {
        let input = r#"Hey How are you? <|python_tag|>{ "name": "get_weather", "parameters": {"location": "San Francisco, CA", "unit": "fahrenheit" } }"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("llama3_json"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("Hey How are you?".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_meta_llama_llama31_8b_instruct_with_python_tag_with_new_lines() {
        let input = r#"
        <|python_tag|>
        {"name": "get_weather", "parameters": {"location": "San Francisco, CA", "unit": "fahrenheit"}}
        "#;
        let (result, content) = detect_and_parse_tool_call(input, Some("llama3_json"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_meta_llama_llama31_8b_instruct_with_python_tag_multiple_with_new_lines() {
        let input = r#"
        <|python_tag|>
        {"name": "get_weather", "parameters": {"location": "San Francisco, CA", "unit": "fahrenheit" }}
        <|python_tag|>
        {"name": "get_weather", "parameters": {"location": "New York, NY", "unit": "fahrenheit" }}
        "#;
        let (result, content) = detect_and_parse_tool_call(input, Some("llama3_json"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 2);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
        let (name, args) = extract_name_and_args(result[1].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "New York, NY");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_detect_and_parse_tool_call_error_handling() {
        let input = r#"{"name": "get_weather", "arguments": {"location": "San Francisco, CA"}}"#;
        let result = detect_and_parse_tool_call(input, Some("unknown_parser"), None).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("is not implemented"),
            "Unexpected error message: {}",
            err
        );

        let input = "not a json";
        let (result, content) = detect_and_parse_tool_call(input, Some("hermes"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("not a json".to_string()));
        assert!(result.is_empty());

        let input = r#"{"foo": "bar"}"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("hermes"), None)
            .await
            .unwrap();
        assert_eq!(content, Some(r#"{"foo": "bar"}"#.to_string()));
        assert!(result.is_empty());
    }

    #[tokio::test]
    #[ignore]
    async fn test_internlm_internlm2_5_7b_chat_simple() {
        let input = r#"San Francisco's weather is known for its mild climate with plenty of fog, especially along the coast. Here's an overview of the weather in Fahrenheit:

- **Summer (June to August)**: Average highs range from the mid-60s to low 70s Fahrenheit, with cooler mornings and evenings. Coastal areas may be cooler than inland spots.

Remember, San Francisco weather can be quite unpredictable, particularly with its famous fog, which can significantly lower temperatures. Always check a local weather forecast for the most accurate and up-to-date information."#;
        let (result, content) = try_tool_call_parse(input, &ToolCallConfig::default(), None)
            .await
            .unwrap();
        assert_eq!(content, Some(input.to_string()));
        assert!(result.is_empty()); // This model doesn't produce tool calls
    }

    #[tokio::test]
    async fn test_ai21labs_ai21_jamba_15_mini_simple() {
        let input = r#"<tool_calls>[
{"name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit"}}
]</tool_calls>"#;
        let config = ToolCallConfig::jamba();
        let (result, content) = try_tool_call_parse(input, &config, None).await.unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_ai21labs_ai21_jamba_15_mini_multiple() {
        let input = r#"<tool_calls>[
{"name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit"}},
{"name": "get_weather", "arguments": {"location": "New York, NY", "unit": "celsius"}}
]</tool_calls>"#;
        let config = ToolCallConfig::jamba();
        let (result, content) = try_tool_call_parse(input, &config, None).await.unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 2);

        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");

        let (name, args) = extract_name_and_args(result[1].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "New York, NY");
        assert_eq!(args["unit"], "celsius");
    }

    #[tokio::test]
    #[ignore]
    async fn test_salesforce_llama_xlam_2_8b_fc_r_simple() {
        let input = r#"[{"name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit"}}]"#;
        let config = ToolCallConfig {
            parser_config: ParserConfig::Json(JsonParserConfig {
                tool_call_start_tokens: vec![],
                tool_call_end_tokens: vec![],
                arguments_keys: vec!["arguments".to_string()],
                ..Default::default()
            }),
        };
        let (result, content) = try_tool_call_parse(input, &config, None).await.unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_detect_and_parse_tool_call_default_parser_nemotron_deci() {
        let input = r#"<TOOLCALL>[{"name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit"}}]</TOOLCALL>"#;
        let (result, content) = detect_and_parse_tool_call(input, None, None).await.unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_detect_and_parse_tool_call_default_parser_nemotron_deci_multiple() {
        let input = r#"<TOOLCALL>[{"name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit"}}, {"name": "get_weather", "arguments": {"location": "New York, NY", "unit": "fahrenheit"}}]</TOOLCALL>"#;
        let (result, content) = detect_and_parse_tool_call(input, None, None).await.unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 2);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
        let (name, args) = extract_name_and_args(result[1].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "New York, NY");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_detect_and_parse_tool_call_default_parser_nemotron_deci_multiple_with_normal_text()
     {
        let input = r#"Hey How are you? <TOOLCALL>[{"name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit"}}, {"name": "get_weather", "arguments": {"location": "New York, NY", "unit": "fahrenheit"}}]</TOOLCALL>"#;
        let (result, content) = detect_and_parse_tool_call(input, None, None).await.unwrap();
        assert_eq!(content, Some("Hey How are you?".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 2);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
        let (name, args) = extract_name_and_args(result[1].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "New York, NY");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_detect_and_parse_tool_call_default_parser_llama3_json_with_python_tag() {
        let input = r#"<|python_tag|>{ "name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit" } }"#;
        let (result, content) = detect_and_parse_tool_call(input, None, None).await.unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_detect_and_parse_tool_call_default_parser_llama3_json_with_python_tag_with_normal_text()
     {
        let input = r#"Hey How are you? <|python_tag|>{ "name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit" } }"#;
        let (result, content) = detect_and_parse_tool_call(input, None, None).await.unwrap();
        assert_eq!(content, Some("Hey How are you?".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_detect_and_parse_tool_call_default_parser_llama3_json_with_python_tag_with_new_lines()
     {
        let input = r#"
        <|python_tag|>
        {"name":
        "get_weather",
         "arguments":
          {"location": "San Francisco, CA",
          "unit": "fahrenheit" }}
        "#;
        let (result, content) = detect_and_parse_tool_call(input, None, None).await.unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_detect_and_parse_tool_call_default_parser_llama3_json_without_python_tag_multiple_with_new_lines()
     {
        let input = r#"
        {"name": "get_weather", "arguments":
         {"location": "San Francisco, CA",
          "unit": "fahrenheit" }}
        "#;
        let (result, content) = detect_and_parse_tool_call(input, None, None).await.unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_detect_and_parse_tool_call_default_parser_llama3_json_without_python_tag() {
        let input = r#"{ "name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit" } }"#;
        let (result, content) = try_tool_call_parse(input, &ToolCallConfig::mistral(), None)
            .await
            .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_detect_and_parse_tool_call_default_parser_llama3_json_without_python_tag_with_normal_text()
     {
        let input = r#"Hey How are you? { "name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit" } }"#;
        let (result, content) = try_tool_call_parse(input, &ToolCallConfig::mistral(), None)
            .await
            .unwrap();
        assert_eq!(content, Some("Hey How are you?".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_phi4_single_function_call() {
        let input =
            r#"functools[{"name": "get_country_capital", "arguments": {"country": "Poland"}}]"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("phi4"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_country_capital");
        assert_eq!(args["country"], "Poland");
    }

    #[tokio::test]
    async fn test_phi4_single_function_call_with_normal_text() {
        let input = r#"Hey How are you? functools[{"name": "get_country_capital", "arguments": {"country": "Poland"}}]"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("phi4"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("Hey How are you?".to_string()));
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_country_capital");
        assert_eq!(args["country"], "Poland");
    }

    #[tokio::test]
    async fn test_phi4_multiple_function_calls_simple_arguments() {
        let input = r#"functools[
  {"name": "get_country_capital", "arguments": {"country": "Poland"}},
  {"name": "get_population", "arguments": {"city": "Warsaw"}}
]"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("phi4"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert_eq!(result.len(), 2);

        let (name1, args1) = extract_name_and_args(result[0].clone());
        assert_eq!(name1, "get_country_capital");
        assert_eq!(args1["country"], "Poland");

        let (name2, args2) = extract_name_and_args(result[1].clone());
        assert_eq!(name2, "get_population");
        assert_eq!(args2["city"], "Warsaw");
    }

    #[tokio::test]
    async fn test_phi4_multiple_function_calls_simple_arguments_with_normal_text() {
        let input = r#"Hey How are you? functools[
  {"name": "get_country_capital", "arguments": {"country": "Poland"}},
  {"name": "get_population", "arguments": {"city": "Warsaw"}}
]"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("phi4"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("Hey How are you?".to_string()));
        assert_eq!(result.len(), 2);

        let (name1, args1) = extract_name_and_args(result[0].clone());
        assert_eq!(name1, "get_country_capital");
        assert_eq!(args1["country"], "Poland");

        let (name2, args2) = extract_name_and_args(result[1].clone());
        assert_eq!(name2, "get_population");
        assert_eq!(args2["city"], "Warsaw");
    }

    #[tokio::test]
    async fn test_phi4_single_function_call_nested_json_arguments() {
        let input = r#"functools[{"name": "get_weather_forecast", "arguments":
        {"location": {"city": "San Francisco",
        "state": "CA"}, "date": "2023-10-05"}}]"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("phi4"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather_forecast");
        assert_eq!(args["date"], "2023-10-05");
        assert_eq!(args["location"]["city"], "San Francisco");
        assert_eq!(args["location"]["state"], "CA");
    }

    #[tokio::test]
    async fn test_phi4_single_function_call_nested_json_arguments_with_normal_text() {
        let input = r#"Hey How are you? functools[{"name": "get_weather_forecast", "arguments":
        {"location": {"city": "San Francisco",
        "state": "CA"}, "date": "2023-10-05"}}]"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("phi4"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("Hey How are you?".to_string()));
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather_forecast");
        assert_eq!(args["date"], "2023-10-05");
        assert_eq!(args["location"]["city"], "San Francisco");
        assert_eq!(args["location"]["state"], "CA");
    }

    #[tokio::test]
    async fn test_phi4_function_call_with_parameters_instead_of_arguments() {
        let input = r#"functools[{"name": "calculate_distance",
         "parameters": {"from": "New York", "to": "Los Angeles"}}]"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("phi4"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "calculate_distance");
        assert_eq!(args["from"], "New York");
        assert_eq!(args["to"], "Los Angeles");
    }

    #[tokio::test]
    async fn test_phi4_function_call_with_parameters_instead_of_arguments_with_normal_text() {
        let input = r#"Hey How are you? functools[{"name": "calculate_distance",
         "parameters": {"from": "New York", "to": "Los Angeles"}}]"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("phi4"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("Hey How are you?".to_string()));
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "calculate_distance");
        assert_eq!(args["from"], "New York");
        assert_eq!(args["to"], "Los Angeles");
    }

    #[tokio::test]
    async fn test_phi4_token_leak_reproduction() {
        let input = r#"functools{"name": "get_weather","arguments":{"location":"San Francisco"}}"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("phi4"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco");
    }

    #[tokio::test]
    async fn test_phi4_token_leak_edge_case() {
        let input = r#"functools"#;
        let (result, _content) = detect_and_parse_tool_call(input, Some("phi4"), None)
            .await
            .unwrap();
        assert_eq!(result.len(), 0); // No tool calls found
    }

    #[tokio::test]
    async fn test_phi4_token_with_invalid_json() {
        let input = r#"functools{invalid json}"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("phi4"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert_eq!(result.len(), 0); // No tool calls found due to invalid JSON
    }

    #[tokio::test]
    async fn test_phi4_streaming_partial_tokens() {

        let config = super::get_tool_parser_map().get("phi4").unwrap();
        let json_config = match &config.parser_config {
            super::super::config::ParserConfig::Json(cfg) => cfg,
            _ => panic!("Expected JSON parser config"),
        };

        use super::super::json::detect_tool_call_start_json;
        assert!(
            detect_tool_call_start_json("fun", json_config),
            "'fun' should be detected as potential start"
        );
        assert!(
            detect_tool_call_start_json("f", json_config),
            "'f' should be detected as potential start"
        );
        assert!(
            detect_tool_call_start_json("func", json_config),
            "'func' should be detected as potential start"
        );
        assert!(
            detect_tool_call_start_json("functo", json_config),
            "'functo' should be detected as potential start"
        );

        assert!(
            !detect_tool_call_start_json("hello", json_config),
            "'hello' should not be detected"
        );
        assert!(
            !detect_tool_call_start_json("xyz", json_config),
            "'xyz' should not be detected"
        );
    }

    #[tokio::test]
    async fn test_phi4_false_positive_words() {

        let input = r#"funk music is great"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("phi4"), None)
            .await
            .unwrap();
        assert_eq!(
            result.len(),
            0,
            "No tool calls should be found in 'funk music is great'"
        );
        assert_eq!(
            content,
            Some("funk music is great".to_string()),
            "Content should contain the original text"
        );
    }

    #[tokio::test]
    async fn test_phi4_partial_but_complete_words() {

        let input = r#"The function works well"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("phi4"), None)
            .await
            .unwrap();
        assert_eq!(
            result.len(),
            0,
            "No tool calls should be found in 'The function works well'"
        );
        assert_eq!(content, Some("The function works well".to_string()));

        let input = r#"functional programming"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("phi4"), None)
            .await
            .unwrap();
        assert_eq!(
            result.len(),
            0,
            "No tool calls should be found in 'functional programming'"
        );
        assert_eq!(content, Some("functional programming".to_string()));
    }

    #[tokio::test]
    async fn test_phi4_funk_variations() {

        let test_cases = vec![
            "funk",
            "funky",
            "funktion", // German word for function
            "funked",
            "I love funk music",
            "This is funky stuff",
        ];

        for test_input in test_cases {
            let (result, content) = detect_and_parse_tool_call(test_input, Some("phi4"), None)
                .await
                .unwrap();
            assert_eq!(
                result.len(),
                0,
                "No tool calls should be found in '{}'",
                test_input
            );
            assert_eq!(
                content,
                Some(test_input.to_string()),
                "Content should match input for '{}'",
                test_input
            );
        }
    }

    #[tokio::test]
    async fn test_phi4_func_but_not_functools() {

        let test_cases = vec![
            "func()",  // Programming syntax
            "funcdef", // Python keyword variant
            "functions are useful",
            "functionally speaking",
        ];

        for test_input in test_cases {
            let (result, content) = detect_and_parse_tool_call(test_input, Some("phi4"), None)
                .await
                .unwrap();
            assert_eq!(
                result.len(),
                0,
                "No tool calls should be found in '{}'",
                test_input
            );
            assert_eq!(
                content,
                Some(test_input.to_string()),
                "Content should match input for '{}'",
                test_input
            );
        }
    }

    #[tokio::test]
    async fn test_pythonic_parser_basic_with_constants() {
        let input = r#"[get_weather(location="San Francisco", unit="fahrenheit"), get_weather(location="New York", unit="fahrenheit")]"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("pythonic"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert_eq!(result.len(), 2);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco");
        assert_eq!(args["unit"], "fahrenheit");
        let (name, args) = extract_name_and_args(result[1].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "New York");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    #[ignore]
    async fn test_pythonic_parser_with_constants_and_normal_text() {
        let input = r#"Hey How are you? [get_weather(location="San Francisco", unit="fahrenheit"), get_weather(location="New York", unit="fahrenheit")]"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("pythonic"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("Hey How are you?".to_string()));
        assert_eq!(result.len(), 2);

        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco");
        assert_eq!(args["unit"], "fahrenheit");
        let (name, args) = extract_name_and_args(result[1].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "New York");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_harmony_parser_basic() {
        let input = r#"
        <|channel|>analysis<|message|>Need to use function get_current_weather.<|end|><|start|>assistant<|channel|>commentary to=functions.get_current_weather <|constrain|>json<|message|>{"location":"San Francisco", "unit":"fahrenheit"}"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("harmony"), None)
            .await
            .unwrap();
        assert_eq!(
            content,
            Some("Need to use function get_current_weather.".to_string())
        );
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_current_weather");
        assert_eq!(args["location"], "San Francisco");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_deepseek_v3_parser_basic() {
        let input = r#"<｜tool▁calls▁begin｜><｜tool▁call▁begin｜>function<｜tool▁sep｜>get_current_weather
```json
{"location": "Tokyo"}
```<｜tool▁call▁end｜><｜tool▁call▁begin｜>function<｜tool▁sep｜>get_current_weather
```json
{"location": "Paris"}
```<｜tool▁call▁end｜><｜tool▁calls▁end｜><｜end▁of▁sentence｜>"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("deepseek_v3"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert_eq!(result.len(), 2);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_current_weather");
        assert_eq!(args["location"], "Tokyo");
        let (name, args) = extract_name_and_args(result[1].clone());
        assert_eq!(name, "get_current_weather");
        assert_eq!(args["location"], "Paris");
    }

    #[tokio::test]
    async fn test_deepseek_v3_1_parser_basic() {
        let input = r#"<｜tool▁calls▁begin｜><｜tool▁call▁begin｜>get_current_weather<｜tool▁sep｜>{"location": "Tokyo"}<｜tool▁call▁end｜><｜tool▁call▁begin｜>get_current_weather<｜tool▁sep｜>{"location": "Paris"}<｜tool▁call▁end｜><｜tool▁calls▁end｜><｜end▁of▁sentence｜>"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("deepseek_v3_1"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert_eq!(result.len(), 2);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_current_weather");
        assert_eq!(args["location"], "Tokyo");
        let (name, args) = extract_name_and_args(result[1].clone());
        assert_eq!(name, "get_current_weather");
        assert_eq!(args["location"], "Paris");
    }

    #[tokio::test]
    async fn test_deepseek_v3_2_single_tool_call() {
        let input = r#"<｜DSML｜function_calls>
<｜DSML｜invoke name="get_datetime">
<｜DSML｜parameter name="timezone" string="true">Asia/Shanghai</｜DSML｜parameter>
</｜DSML｜invoke>
</｜DSML｜function_calls>"#;

        let (tool_calls, normal_text) =
            detect_and_parse_tool_call(input, Some("deepseek_v3_2"), None)
                .await
                .expect("Failed to parse");

        assert_eq!(tool_calls.len(), 1);
        assert_eq!(tool_calls[0].function.name, "get_datetime");
        assert_eq!(normal_text, Some("".to_string()));

        let args: serde_json::Value =
            serde_json::from_str(&tool_calls[0].function.arguments).unwrap();
        assert_eq!(args["timezone"], "Asia/Shanghai");
    }

    #[tokio::test]
    async fn test_deepseek_v3_2_multiple_tool_calls() {
        let input = r#"<｜DSML｜function_calls>
<｜DSML｜invoke name="get_weather">
<｜DSML｜parameter name="location" string="true">Hangzhou</｜DSML｜parameter>
<｜DSML｜parameter name="date" string="true">2024-01-16</｜DSML｜parameter>
</｜DSML｜invoke>
<｜DSML｜invoke name="get_weather">
<｜DSML｜parameter name="location" string="true">Beijing</｜DSML｜parameter>
<｜DSML｜parameter name="date" string="true">2024-01-16</｜DSML｜parameter>
</｜DSML｜invoke>
</｜DSML｜function_calls>"#;

        let (tool_calls, _) = detect_and_parse_tool_call(input, Some("deepseek_v3_2"), None)
            .await
            .expect("Failed to parse");

        assert_eq!(tool_calls.len(), 2);
        assert_eq!(tool_calls[0].function.name, "get_weather");
        assert_eq!(tool_calls[1].function.name, "get_weather");

        let args0: serde_json::Value =
            serde_json::from_str(&tool_calls[0].function.arguments).unwrap();
        assert_eq!(args0["location"], "Hangzhou");
        assert_eq!(args0["date"], "2024-01-16");

        let args1: serde_json::Value =
            serde_json::from_str(&tool_calls[1].function.arguments).unwrap();
        assert_eq!(args1["location"], "Beijing");
    }

    #[tokio::test]
    async fn test_deepseek_v3_2_mixed_parameter_types() {
        let input = r#"<｜DSML｜function_calls>
<｜DSML｜invoke name="search">
<｜DSML｜parameter name="query" string="true">search agent benchmark 2024</｜DSML｜parameter>
<｜DSML｜parameter name="topn" string="false">10</｜DSML｜parameter>
<｜DSML｜parameter name="source" string="true">web</｜DSML｜parameter>
</｜DSML｜invoke>
</｜DSML｜function_calls>"#;

        let (tool_calls, _) = detect_and_parse_tool_call(input, Some("deepseek_v3_2"), None)
            .await
            .expect("Failed to parse");

        assert_eq!(tool_calls.len(), 1);
        assert_eq!(tool_calls[0].function.name, "search");

        let args: serde_json::Value =
            serde_json::from_str(&tool_calls[0].function.arguments).unwrap();
        assert_eq!(args["query"], "search agent benchmark 2024");
        assert_eq!(args["topn"], 10); // Should be number, not string
        assert_eq!(args["source"], "web");
    }

    #[tokio::test]
    async fn test_deepseek_v4_single_tool_call() {
        let input = r#"<｜DSML｜tool_calls>
<｜DSML｜invoke name="get_datetime">
<｜DSML｜parameter name="timezone" string="true">Asia/Shanghai</｜DSML｜parameter>
</｜DSML｜invoke>
</｜DSML｜tool_calls>"#;

        let (tool_calls, normal_text) =
            detect_and_parse_tool_call(input, Some("deepseek_v4"), None)
                .await
                .expect("Failed to parse");

        assert_eq!(tool_calls.len(), 1);
        assert_eq!(tool_calls[0].function.name, "get_datetime");
        assert_eq!(normal_text, Some("".to_string()));

        let args: serde_json::Value =
            serde_json::from_str(&tool_calls[0].function.arguments).unwrap();
        assert_eq!(args["timezone"], "Asia/Shanghai");
    }
    #[tokio::test]
    async fn test_deepseek_v4_compatibility_aliases() {
        let input = r#"<｜DSML｜tool_calls>
<｜DSML｜invoke name="search">
<｜DSML｜parameter name="query" string="true">search agent benchmark 2024</｜DSML｜parameter>
<｜DSML｜parameter name="topn" string="false">10</｜DSML｜parameter>
<｜DSML｜parameter name="source" string="true">web</｜DSML｜parameter>
</｜DSML｜invoke>
</｜DSML｜tool_calls>"#;

        for parser_name in ["deepseek_v4", "deepseek-v4", "deepseekv4"] {
            let (tool_calls, _) = detect_and_parse_tool_call(input, Some(parser_name), None)
                .await
                .expect("Failed to parse");

            assert_eq!(tool_calls.len(), 1);
            assert_eq!(tool_calls[0].function.name, "search");

            let args: serde_json::Value =
                serde_json::from_str(&tool_calls[0].function.arguments).unwrap();
            assert_eq!(args["query"], "search agent benchmark 2024");
            assert_eq!(args["topn"], 10);
            assert_eq!(args["source"], "web");
        }
    }

    #[tokio::test]
    async fn test_hermes_parser_without_new_line() {
        let input = r#"<tool_call>{"name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "celsius"}}</tool_call>"
        "#;
        let (result, content) = detect_and_parse_tool_call(input, Some("hermes"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "celsius");
    }

    #[tokio::test]
    async fn test_qwen25_simple() {
        let input = r#"<tool_call>
{"name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit"}}
</tool_call>"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("qwen25"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_qwen25_with_normal_text() {
        let input = r#"I'll check the weather for you. <tool_call>
{"name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit"}}
</tool_call>"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("qwen25"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("I'll check the weather for you.".to_string()));
        assert_eq!(result.len(), 1);
        let (name, _args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
    }

    #[tokio::test]
    async fn test_qwen25_multiple_tool_calls() {
        let input = r#"<tool_call>
{"name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit"}}
</tool_call>
<tool_call>
{"name": "get_weather", "arguments": {"location": "New York, NY", "unit": "fahrenheit"}}
</tool_call>"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("qwen25"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert_eq!(result.len(), 2);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        let (name, args) = extract_name_and_args(result[1].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "New York, NY");
    }

    #[tokio::test]
    async fn test_qwen25_plain_text_only() {
        let input = "Hello, how can I help you today?";
        let (result, content) = detect_and_parse_tool_call(input, Some("qwen25"), None)
            .await
            .unwrap();
        assert!(result.is_empty());
        assert_eq!(
            content,
            Some("Hello, how can I help you today?".to_string())
        );
    }

    // === 并行工具调用测试（基于所给示例的综合用例） ===

    fn validate_weather_tool_calls(result: &[ToolCallResponse], expected_cities: &[(&str, &str)]) {
        assert_eq!(
            result.len(),
            expected_cities.len(),
            "Expected {} tool calls, got {}",
            expected_cities.len(),
            result.len()
        );

        for (i, (expected_city, expected_state)) in expected_cities.iter().enumerate() {
            let (name, args) = extract_name_and_args(result[i].clone());
            assert_eq!(
                name, "get_current_weather",
                "Tool call {} should be get_current_weather",
                i
            );
            assert_eq!(
                args["city"], *expected_city,
                "Tool call {} city should be {}",
                i, expected_city
            );
            assert_eq!(
                args["state"], *expected_state,
                "Tool call {} state should be {}",
                i, expected_state
            );
            assert_eq!(
                args["unit"], "fahrenheit",
                "Tool call {} unit should be fahrenheit",
                i
            );

            assert!(
                result[i].id.len() >= 9,
                "Tool call {} ID should be at least 9 characters",
                i
            );

            assert_eq!(
                result[i].tp,
                crate::tool_calling::response::ToolCallType::Function,
                "Tool call {} type should be 'function'",
                i
            );
        }
    }

    // =============================================================================
    // =============================================================================

    #[tokio::test]
    async fn test_parallel_nemotron_format_two_cities() {
        let input = r#" <TOOLCALL>[
    {"name": "get_current_weather", "arguments": {"city": "Dallas", "state": "TX", "unit": "fahrenheit"}},
    {"name": "get_current_weather", "arguments": {"city": "Orlando", "state": "FL", "unit": "fahrenheit"}}
]</TOOLCALL>"#;

        let (result, content) = detect_and_parse_tool_call(input, Some("nemotron_deci"), None)
            .await
            .unwrap();

        assert_eq!(content, Some("".to_string()));
        validate_weather_tool_calls(&result, &[("Dallas", "TX"), ("Orlando", "FL")]);
    }

    #[tokio::test]
    async fn test_parallel_nemotron_format_three_cities() {
        let input = r#"<TOOLCALL>[
    {"name": "get_current_weather", "arguments": {"city": "Dallas", "state": "TX", "unit": "fahrenheit"}},
    {"name": "get_current_weather", "arguments": {"city": "Orlando", "state": "FL", "unit": "fahrenheit"}},
    {"name": "get_current_weather", "arguments": {"city": "Seattle", "state": "WA", "unit": "fahrenheit"}}
]</TOOLCALL>"#;

        let (result, content) = detect_and_parse_tool_call(input, Some("nemotron_deci"), None)
            .await
            .unwrap();

        assert_eq!(content, Some("".to_string()));
        validate_weather_tool_calls(
            &result,
            &[("Dallas", "TX"), ("Orlando", "FL"), ("Seattle", "WA")],
        );
    }

    #[tokio::test]
    async fn test_parallel_nemotron_format_with_normal_text() {
        let input = r#"I'll help you get the weather for both cities. <TOOLCALL>[
    {"name": "get_current_weather", "arguments": {"city": "Dallas", "state": "TX", "unit": "fahrenheit"}},
    {"name": "get_current_weather", "arguments": {"city": "Orlando", "state": "FL", "unit": "fahrenheit"}}
]</TOOLCALL>"#;

        let (result, content) = detect_and_parse_tool_call(input, Some("nemotron_deci"), None)
            .await
            .unwrap();

        assert_eq!(
            content,
            Some("I'll help you get the weather for both cities.".to_string())
        );
        validate_weather_tool_calls(&result, &[("Dallas", "TX"), ("Orlando", "FL")]);
    }

    // =================================================
    // =================================================

    #[tokio::test]
    async fn test_parallel_qwen3coder_format_two_cities() {
        let input = r#"<tool_call>
<function=get_current_weather>
<parameter=city>
Dallas
</parameter>
<parameter=state>
TX
</parameter>
<parameter=unit>
fahrenheit
</parameter>
</function>
</tool_call>
<tool_call>
<function=get_current_weather>
<parameter=city>
Orlando
</parameter>
<parameter=state>
FL
</parameter>
<parameter=unit>
fahrenheit
</parameter>
</function>
</tool_call>"#;

        let (result, content) = detect_and_parse_tool_call(input, Some("qwen3_coder"), None)
            .await
            .unwrap();

        assert_eq!(content, Some("".to_string()));
        validate_weather_tool_calls(&result, &[("Dallas", "TX"), ("Orlando", "FL")]);
    }

    // =============================================================================
    // =============================================================================

    #[tokio::test]
    async fn test_parallel_xlam_format_pure_json() {
        let input = r#"[{"name": "get_current_weather", "arguments": {"city": "Dallas", "state": "TX", "unit": "fahrenheit"}}, {"name": "get_current_weather", "arguments": {"city": "Orlando", "state": "FL", "unit": "fahrenheit"}}]"#;

        let (result, content) = detect_and_parse_tool_call(input, Some("mistral"), None)
            .await
            .unwrap();

        assert_eq!(content, Some("".to_string()));
        validate_weather_tool_calls(&result, &[("Dallas", "TX"), ("Orlando", "FL")]);
    }

    #[tokio::test]
    async fn test_parallel_xlam_format_with_whitespace() {
        let input = r#"[
    {"name": "get_current_weather", "arguments": {"city": "Dallas", "state": "TX", "unit": "fahrenheit"}},
    {"name": "get_current_weather", "arguments": {"city": "Orlando", "state": "FL", "unit": "fahrenheit"}}
]"#;

        let (result, content) = detect_and_parse_tool_call(input, Some("mistral"), None)
            .await
            .unwrap();

        assert_eq!(content, Some("".to_string()));
        validate_weather_tool_calls(&result, &[("Dallas", "TX"), ("Orlando", "FL")]);
    }

    // =============================================================================
    // =============================================================================

    #[tokio::test]
    async fn test_parallel_minimax_format() {
        let _input = r#"<tool_calls>
{"name": "get_current_weather", "arguments": {"city": "Dallas", "state": "TX", "unit": "fahrenheit"}}
{"name": "get_current_weather", "arguments": {"city": "Orlando", "state": "FL", "unit": "fahrenheit"}}
</tool_calls>"#;

        let input_nemotron_format = r#"<TOOLCALL>[
{"name": "get_current_weather", "arguments": {"city": "Dallas", "state": "TX", "unit": "fahrenheit"}},
{"name": "get_current_weather", "arguments": {"city": "Orlando", "state": "FL", "unit": "fahrenheit"}}
]</TOOLCALL>"#;

        let (result, content) =
            detect_and_parse_tool_call(input_nemotron_format, Some("nemotron_deci"), None)
                .await
                .unwrap();

        assert_eq!(content, Some("".to_string()));
        validate_weather_tool_calls(&result, &[("Dallas", "TX"), ("Orlando", "FL")]);
    }

    // =============================================================================
    // =============================================================================

    #[tokio::test]
    async fn test_parallel_harmony_format_multiple_tools() {
        let input = r#"<|channel|>commentary to=functions.get_current_weather <|constrain|>json<|message|>{"city": "Dallas", "state": "TX", "unit": "fahrenheit"}<|call|><|start|>assistant<|channel|>commentary to=functions.get_current_weather <|constrain|>json<|message|>{"city": "Orlando", "state": "FL", "unit": "fahrenheit"}<|call|>"#;

        let (result, _content) = detect_and_parse_tool_call(input, Some("harmony"), None)
            .await
            .unwrap();

        assert!(!result.is_empty(), "Should parse at least one tool call");

        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_current_weather");
        assert!(args.get("city").is_some() || args.get("location").is_some());
    }

    // =============================================================================
    // =============================================================================

    #[tokio::test]
    async fn test_parallel_mixed_tool_types() {
        let input = r#"<TOOLCALL>[
    {"name": "get_current_weather", "arguments": {"city": "Dallas", "state": "TX", "unit": "fahrenheit"}},
    {"name": "web_search", "arguments": {"query": "Orlando Florida attractions", "max_results": 5}}
]</TOOLCALL>"#;

        let (result, content) = detect_and_parse_tool_call(input, Some("nemotron_deci"), None)
            .await
            .unwrap();

        assert_eq!(content, Some("".to_string()));
        assert_eq!(result.len(), 2);

        let (name1, args1) = extract_name_and_args(result[0].clone());
        assert_eq!(name1, "get_current_weather");
        assert_eq!(args1["city"], "Dallas");
        assert_eq!(args1["state"], "TX");
        assert_eq!(args1["unit"], "fahrenheit");

        let (name2, args2) = extract_name_and_args(result[1].clone());
        assert_eq!(name2, "web_search");
        assert_eq!(args2["query"], "Orlando Florida attractions");
        assert_eq!(args2["max_results"], 5);
    }

    // =============================================================================
    // =============================================================================

    #[tokio::test]
    async fn test_parallel_malformed_second_call() {
        let input = r#"<TOOLCALL>[
    {"name": "get_current_weather", "arguments": {"city": "Dallas", "state": "TX", "unit": "fahrenheit"}},
    {"name": "get_current_weather", "arguments": {"city": "Orlando", "invalid_field": 123}}
]</TOOLCALL>"#;

        let (result, _content) = detect_and_parse_tool_call(input, Some("nemotron_deci"), None)
            .await
            .unwrap();

        assert!(
            !result.is_empty(),
            "Should parse at least the valid tool call"
        );

        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_current_weather");
        assert_eq!(args["city"], "Dallas");
    }

    #[tokio::test]
    async fn test_parallel_empty_array() {
        let input = r#"<TOOLCALL>[]</TOOLCALL>"#;

        let (result, content) = detect_and_parse_tool_call(input, Some("nemotron_deci"), None)
            .await
            .unwrap();

        assert_eq!(
            result.len(),
            0,
            "Empty array should result in no tool calls"
        );
        assert_eq!(content, Some("".to_string()));
    }

    #[tokio::test]
    async fn test_parallel_single_call_in_array() {
        let input = r#"<TOOLCALL>[
    {"name": "get_current_weather", "arguments": {"city": "Dallas", "state": "TX", "unit": "fahrenheit"}}
]</TOOLCALL>"#;

        let (result, content) = detect_and_parse_tool_call(input, Some("nemotron_deci"), None)
            .await
            .unwrap();

        assert_eq!(content, Some("".to_string()));
        assert_eq!(result.len(), 1);
        validate_weather_tool_calls(&result, &[("Dallas", "TX")]);
    }

    // =============================================================================
    // =============================================================================

    #[tokio::test]
    async fn test_parallel_five_cities() {
        let input = r#"<TOOLCALL>[
    {"name": "get_current_weather", "arguments": {"city": "Dallas", "state": "TX", "unit": "fahrenheit"}},
    {"name": "get_current_weather", "arguments": {"city": "Orlando", "state": "FL", "unit": "fahrenheit"}},
    {"name": "get_current_weather", "arguments": {"city": "Seattle", "state": "WA", "unit": "fahrenheit"}},
    {"name": "get_current_weather", "arguments": {"city": "Denver", "state": "CO", "unit": "fahrenheit"}},
    {"name": "get_current_weather", "arguments": {"city": "Miami", "state": "FL", "unit": "fahrenheit"}}
]</TOOLCALL>"#;

        let (result, content) = detect_and_parse_tool_call(input, Some("nemotron_deci"), None)
            .await
            .unwrap();

        assert_eq!(content, Some("".to_string()));
        validate_weather_tool_calls(
            &result,
            &[
                ("Dallas", "TX"),
                ("Orlando", "FL"),
                ("Seattle", "WA"),
                ("Denver", "CO"),
                ("Miami", "FL"),
            ],
        );
    }

    // =============================================================================
    // =============================================================================

    #[tokio::test]
    async fn test_parallel_complex_arguments() {
        let input = r#"<TOOLCALL>[
    {
        "name": "get_weather_forecast",
        "arguments": {
            "location": {"city": "Dallas", "state": "TX", "country": "USA"},
            "days": 7,
            "units": "fahrenheit",
            "include_hourly": true,
            "alerts": ["severe_weather", "temperature_extreme"]
        }
    },
    {
        "name": "get_air_quality",
        "arguments": {
            "coordinates": {"lat": 32.7767, "lon": -96.7970},
            "metrics": ["pm2.5", "pm10", "ozone", "no2"],
            "radius_km": 50
        }
    }
]</TOOLCALL>"#;

        let (result, content) = detect_and_parse_tool_call(input, Some("nemotron_deci"), None)
            .await
            .unwrap();

        assert_eq!(content, Some("".to_string()));
        assert_eq!(result.len(), 2);

        let (name1, args1) = extract_name_and_args(result[0].clone());
        assert_eq!(name1, "get_weather_forecast");
        assert_eq!(args1["location"]["city"], "Dallas");
        assert_eq!(args1["days"], 7);
        assert_eq!(args1["include_hourly"], true);

        let (name2, args2) = extract_name_and_args(result[1].clone());
        assert_eq!(name2, "get_air_quality");
        assert_eq!(args2["coordinates"]["lat"], 32.7767);
        assert_eq!(args2["radius_km"], 50);
    }

    // =============================================================================
    // =============================================================================

    fn validate_tool_call_ids(result: &[ToolCallResponse]) {
        let mut ids = std::collections::HashSet::new();
        for (i, tool_call) in result.iter().enumerate() {
            assert!(
                tool_call.id.len() >= 9,
                "Tool call {} ID '{}' should be at least 9 characters",
                i,
                tool_call.id
            );

            assert!(
                ids.insert(&tool_call.id),
                "Tool call {} ID '{}' is not unique",
                i,
                tool_call.id
            );
        }
    }

    fn validate_openai_compatibility(result: &[ToolCallResponse]) {
        for (i, tool_call) in result.iter().enumerate() {
            assert_eq!(
                tool_call.tp,
                crate::tool_calling::response::ToolCallType::Function,
                "Tool call {} type should be 'function', got '{:?}'",
                i,
                tool_call.tp
            );

            assert!(
                !tool_call.function.name.is_empty(),
                "Tool call {} function name should not be empty",
                i
            );

            let _: serde_json::Value = serde_json::from_str(&tool_call.function.arguments)
                .unwrap_or_else(|_| panic!("Tool call {} arguments should be valid JSON", i));
        }
    }

    #[tokio::test]
    async fn test_parallel_tool_call_id_uniqueness() {
        let input = r#"<TOOLCALL>[
    {"name": "get_current_weather", "arguments": {"city": "Dallas", "state": "TX", "unit": "fahrenheit"}},
    {"name": "get_current_weather", "arguments": {"city": "Orlando", "state": "FL", "unit": "fahrenheit"}},
    {"name": "web_search", "arguments": {"query": "weather forecast", "max_results": 3}}
]</TOOLCALL>"#;

        let (result, _) = detect_and_parse_tool_call(input, Some("nemotron_deci"), None)
            .await
            .unwrap();

        assert_eq!(result.len(), 3);
        validate_tool_call_ids(&result);
        validate_openai_compatibility(&result);
    }

    #[tokio::test]
    async fn test_parallel_openai_compatibility_validation() {
        let input = r#"[TOOL_CALLS][
    {"name": "function_one", "arguments": {"param1": "value1", "param2": 42}},
    {"name": "function_two", "arguments": {"param3": true, "param4": [1, 2, 3]}},
    {"name": "function_three", "arguments": {"param5": {"nested": "object"}}}
][/TOOL_CALLS]"#;

        let (result, _) = detect_and_parse_tool_call(input, Some("mistral"), None)
            .await
            .unwrap();

        assert_eq!(result.len(), 3);
        validate_openai_compatibility(&result);

        let names: std::collections::HashSet<_> =
            result.iter().map(|tc| &tc.function.name).collect();
        assert_eq!(names.len(), 3, "All function names should be unique");
    }

    // =============================================================================
    // =============================================================================

    #[tokio::test]
    async fn test_parallel_performance_many_small_calls() {
        let mut tool_calls = Vec::new();
        for i in 0..20 {
            tool_calls.push(format!(
                r#"{{"name": "get_data_{}", "arguments": {{"id": {}, "type": "test"}}}}"#,
                i, i
            ));
        }

        let input = format!("<TOOLCALL>[{}]</TOOLCALL>", tool_calls.join(","));

        let start = std::time::Instant::now();
        let (result, _) = detect_and_parse_tool_call(&input, Some("nemotron_deci"), None)
            .await
            .unwrap();
        let duration = start.elapsed();

        assert_eq!(result.len(), 20);
        assert!(
            duration < std::time::Duration::from_millis(100),
            "Parsing 20 tool calls should take less than 100ms, took {:?}",
            duration
        );

        validate_tool_call_ids(&result);
        validate_openai_compatibility(&result);
    }

    #[tokio::test]
    async fn test_parallel_large_arguments() {
        let large_data = "x".repeat(1000); // 1KB of data
        let input = format!(
            r#"<TOOLCALL>[
    {{"name": "process_large_data", "arguments": {{"data": "{}", "size": 1000}}}},
    {{"name": "backup_data", "arguments": {{"backup_data": "{}", "timestamp": "2024-01-01T00:00:00Z"}}}}
]</TOOLCALL>"#,
            large_data, large_data
        );

        let (result, _) = detect_and_parse_tool_call(&input, Some("nemotron_deci"), None)
            .await
            .unwrap();

        assert_eq!(result.len(), 2);

        for tool_call in &result {
            let args: serde_json::Value =
                serde_json::from_str(&tool_call.function.arguments).unwrap();
            if tool_call.function.name == "process_large_data" {
                assert_eq!(args["data"].as_str().unwrap().len(), 1000);
                assert_eq!(args["size"], 1000);
            }
        }
    }

    // =============================================================================
    // =============================================================================

    #[tokio::test]
    async fn test_parallel_unicode_and_special_characters() {
        let input = r#"<TOOLCALL>[
    {"name": "translate_text", "arguments": {"text": "Hello 世界! 🌍", "from": "en", "to": "zh"}},
    {"name": "analyze_emoji", "arguments": {"emoji": "🚀💫⭐", "context": "space exploration"}},
    {"name": "process_unicode", "arguments": {"data": "café naïve résumé", "encoding": "utf-8"}}
]</TOOLCALL>"#;

        let (result, _) = detect_and_parse_tool_call(input, Some("nemotron_deci"), None)
            .await
            .unwrap();

        assert_eq!(result.len(), 3);

        let (name1, args1) = extract_name_and_args(result[0].clone());
        assert_eq!(name1, "translate_text");
        assert_eq!(args1["text"], "Hello 世界! 🌍");

        let (name2, args2) = extract_name_and_args(result[1].clone());
        assert_eq!(name2, "analyze_emoji");
        assert_eq!(args2["emoji"], "🚀💫⭐");

        let (name3, args3) = extract_name_and_args(result[2].clone());
        assert_eq!(name3, "process_unicode");
        assert_eq!(args3["data"], "café naïve résumé");
    }

    #[tokio::test]
    async fn test_parallel_json_escaping_and_quotes() {
        let input = r#"<TOOLCALL>[
    {"name": "process_json", "arguments": {"json_string": "{\"key\": \"value with \\\"quotes\\\"\"}", "format": "strict"}},
    {"name": "handle_paths", "arguments": {"windows_path": "C:\\Users\\Test\\Documents\\file.txt", "unix_path": "/home/user/file.txt"}},
    {"name": "regex_pattern", "arguments": {"pattern": "\\d{3}-\\d{3}-\\d{4}", "test_string": "Phone: 123-456-7890"}}
]</TOOLCALL>"#;

        let (result, _) = detect_and_parse_tool_call(input, Some("nemotron_deci"), None)
            .await
            .unwrap();

        assert_eq!(result.len(), 3);

        let (name1, _args1) = extract_name_and_args(result[0].clone());
        assert_eq!(name1, "process_json");

        let (name2, _args2) = extract_name_and_args(result[1].clone());
        assert_eq!(name2, "handle_paths");

        let (name3, _args3) = extract_name_and_args(result[2].clone());
        assert_eq!(name3, "regex_pattern");
    }

    #[tokio::test]
    async fn test_parallel_mixed_argument_types() {
        let input = r#"<TOOLCALL>[
    {"name": "type_test", "arguments": {"string": "text", "number": 42, "float": 2.718281828459045, "boolean": true, "null_value": null}},
    {"name": "array_test", "arguments": {"empty_array": [], "string_array": ["a", "b", "c"], "mixed_array": [1, "two", true, null]}},
    {"name": "object_test", "arguments": {"empty_object": {}, "nested": {"level1": {"level2": {"value": "deep"}}}}}
]</TOOLCALL>"#;

        let (result, _) = detect_and_parse_tool_call(input, Some("nemotron_deci"), None)
            .await
            .unwrap();

        assert_eq!(result.len(), 3);

        let (name1, args1) = extract_name_and_args(result[0].clone());
        assert_eq!(name1, "type_test");
        assert_eq!(args1["string"], "text");
        assert_eq!(args1["number"], 42);
        assert_eq!(args1["float"], std::f64::consts::E);
        assert_eq!(args1["boolean"], true);
        assert!(args1["null_value"].is_null());

        let (name2, args2) = extract_name_and_args(result[1].clone());
        assert_eq!(name2, "array_test");
        assert!(args2["empty_array"].is_array());
        assert_eq!(args2["empty_array"].as_array().unwrap().len(), 0);
        assert_eq!(args2["string_array"].as_array().unwrap().len(), 3);
        assert_eq!(args2["mixed_array"].as_array().unwrap().len(), 4);

        let (name3, args3) = extract_name_and_args(result[2].clone());
        assert_eq!(name3, "object_test");
        assert!(args3["empty_object"].is_object());
        assert_eq!(args3["nested"]["level1"]["level2"]["value"], "deep");
    }

    #[tokio::test]
    async fn test_parallel_whitespace_variations() {
        let input = r#"<TOOLCALL>[
    {
        "name": "spaced_function",
        "arguments": {
            "param1": "value1",
            "param2": "value2"
        }
    },
    {"name":"compact_function","arguments":{"param":"value"}},
    {
      "name"  :  "weird_spacing",
      "arguments"  :  {
        "key"  :  "value"
      }
    }
]</TOOLCALL>"#;

        let (result, _) = detect_and_parse_tool_call(input, Some("nemotron_deci"), None)
            .await
            .unwrap();

        assert_eq!(result.len(), 3);
        validate_openai_compatibility(&result);

        let names: Vec<_> = result.iter().map(|tc| &tc.function.name).collect();
        assert!(names.contains(&&"spaced_function".to_string()));
        assert!(names.contains(&&"compact_function".to_string()));
        assert!(names.contains(&&"weird_spacing".to_string()));
    }

    #[tokio::test]
    async fn test_parallel_cross_parser_compatibility() {
        let base_calls = r#"[
    {"name": "get_weather", "arguments": {"city": "Dallas", "unit": "fahrenheit"}},
    {"name": "get_weather", "arguments": {"city": "Orlando", "unit": "fahrenheit"}}
]"#;

        let test_cases = vec![
            (
                format!("<TOOLCALL>{}</TOOLCALL>", base_calls),
                "nemotron_deci",
            ),
            (
                format!("[TOOL_CALLS]{}[/TOOL_CALLS]", base_calls),
                "mistral",
            ),
            (base_calls.to_string(), "mistral"), // Raw JSON
        ];

        for (input, parser) in test_cases {
            let (result, _) = detect_and_parse_tool_call(&input, Some(parser), None)
                .await
                .unwrap_or_else(|e| panic!("Failed to parse with {}: {}", parser, e));
            assert_eq!(
                result.len(),
                2,
                "Parser {} should produce 2 tool calls",
                parser
            );

            for tool_call in &result {
                assert_eq!(tool_call.function.name, "get_weather");
                let args: serde_json::Value =
                    serde_json::from_str(&tool_call.function.arguments).unwrap();
                assert!(args["city"].is_string());
                assert_eq!(args["unit"], "fahrenheit");
            }
        }
    }

    #[tokio::test]
    async fn test_parallel_boundary_conditions() {
        let input_single = r#"<TOOLCALL>[
    {"name": "single_call", "arguments": {"test": true}}
]</TOOLCALL>"#;

        let (result, _) = detect_and_parse_tool_call(input_single, Some("nemotron_deci"), None)
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].function.name, "single_call");

        let mut many_calls = Vec::new();
        for i in 0..50 {
            many_calls.push(format!(
                r#"{{"name": "call_{}", "arguments": {{"index": {}}}}}"#,
                i, i
            ));
        }

        let input_many = format!("<TOOLCALL>[{}]</TOOLCALL>", many_calls.join(","));

        let (result, _) = detect_and_parse_tool_call(&input_many, Some("nemotron_deci"), None)
            .await
            .unwrap();

        assert_eq!(result.len(), 50);
        validate_tool_call_ids(&result);

        for (i, tool_call) in result.iter().enumerate() {
            assert_eq!(tool_call.function.name, format!("call_{}", i));
            let args: serde_json::Value =
                serde_json::from_str(&tool_call.function.arguments).unwrap();
            assert_eq!(args["index"], i);
        }
    }

    #[tokio::test]
    async fn test_parallel_malformed_recovery() {
        let input = r#"<TOOLCALL>[
    {"name": "good_call_1", "arguments": {"param": "value1"}},
    {"malformed": "missing_name_and_arguments"},
    {"name": "good_call_2", "arguments": {"param": "value2"}},
    {"name": "missing_args"},
    {"name": "good_call_3", "arguments": {"param": "value3"}},
    "completely_invalid_json",
    {"name": "good_call_4", "arguments": {"param": "value4"}}
]</TOOLCALL>"#;

        let (result, _) = detect_and_parse_tool_call(input, Some("nemotron_deci"), None)
            .await
            .unwrap();

        assert!(
            !result.is_empty(),
            "Should parse at least some valid tool calls"
        );

        let valid_calls: Vec<_> = result
            .iter()
            .filter(|tc| tc.function.name.starts_with("good_call"))
            .collect();

        assert!(
            valid_calls.len() >= 2,
            "Should parse at least 2 valid tool calls"
        );

        for tool_call in valid_calls {
            assert!(tool_call.function.name.starts_with("good_call"));
            let args: serde_json::Value =
                serde_json::from_str(&tool_call.function.arguments).unwrap();
            assert!(args["param"].is_string());
        }
    }

    // === 端到端探测测试（仅验证整体流程；细节由各解析器单测覆盖） ===

    #[test]
    fn test_e2e_detect_tool_call_start_harmony() {
        let text = r#"<|start|>assistant<|channel|>commentary to=functions.get_current_weather <|constrain|>json"#;
        let result = detect_tool_call_start(text, Some("harmony")).unwrap();
        assert!(result);
    }

    #[test]
    fn test_e2e_detect_tool_call_start_hermes() {
        let text = r#"{"name": "get_current_weather", "parameters": {"location": "Tokyo"}}"#;
        let result = detect_tool_call_start(text, Some("hermes")).unwrap();
        assert!(result);
    }

    #[test]
    fn test_e2e_detect_tool_call_start_qwen25() {
        let text = r#"<tool_call>
{"name": "get_current_weather", "arguments": {"location": "Tokyo"}}
</tool_call>"#;
        let result = detect_tool_call_start(text, Some("qwen25")).unwrap();
        assert!(result);
    }

    #[test]
    fn test_e2e_detect_tool_call_start_pythonic() {
        let text = r#"foo(a=1, b=2), bar(x=3)]"#;
        let result = detect_tool_call_start(text, Some("pythonic")).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_e2e_detect_tool_call_start_nemotron_deci() {
        let text = r#"<TOOLCALL>[{"name": "get_current_weather", "parameters": {"location": "Tokyo"}}]</TOOLCALL>"#;
        let result = detect_tool_call_start(text, Some("nemotron_deci")).unwrap();
        assert!(result);
    }

    #[test]
    fn test_e2e_detect_tool_call_start_phi4() {
        let text =
            r#"functools{"name": "get_current_weather", "parameters": {"location": "Tokyo"}}"#;
        let result = detect_tool_call_start(text, Some("phi4")).unwrap();
        assert!(result);
    }

    #[test]
    fn test_e2e_detect_tool_call_start_llama3_json() {
        let text = r#"<|python_tag|>{ "name": "get_current_weather", "parameters": {"location": "Tokyo"}}"#;
        let result = detect_tool_call_start(text, Some("llama3_json")).unwrap();
        assert!(result);
    }

    #[test]
    fn test_e2e_detect_tool_call_start_mistral() {
        let text =
            r#"[TOOL_CALLS]{"name": "get_current_weather", "parameters": {"location": "Tokyo"}}"#;
        let result = detect_tool_call_start(text, Some("mistral")).unwrap();
        assert!(result);
    }

    #[test]
    fn test_e2e_detect_incomplete_tool_call_start_deepseek_v3() {
        let text = r#"<｜tool▁call▁begin｜>function<｜tool▁sep｜>get_current_weather
```json
{"location": "Tokyo"}
```<｜tool▁call▁end｜>"#;
        let result = detect_tool_call_start(text, Some("deepseek_v3")).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_e2e_detect_tool_call_start_deepseek_v3() {
        let text = r#"<｜tool▁calls▁begin｜><｜tool▁call▁begin｜>function<｜tool▁sep｜>get_current_weather
```json
{"location": "Tokyo"}
```<｜tool▁call▁end｜>"#;
        let result = detect_tool_call_start(text, Some("deepseek_v3")).unwrap();
        assert!(result);
    }

    #[test]
    fn test_e2e_detect_incomplete_tool_call_start_deepseek_v3_1() {
        let text = r#"<｜tool▁call▁begin｜>get_current_weather<｜tool▁sep｜>{"location": "Tokyo"}<｜tool▁call▁end｜>"#;
        let result = detect_tool_call_start(text, Some("deepseek_v3_1")).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_e2e_detect_tool_call_start_deepseek_v3_1() {
        let text = r#"<｜tool▁calls▁begin｜><｜tool▁call▁begin｜>get_current_weather<｜tool▁sep｜>{"location": "Tokyo"}<｜tool▁call▁end｜>"#;
        let result = detect_tool_call_start(text, Some("deepseek_v3_1")).unwrap();
        assert!(result);
    }

    #[test]
    fn test_e2e_detect_tool_call_start_xml() {
        let text = r#"<tool_call><function=get_weather><parameter=city>Dallas</parameter></function></tool_call>"#;
        let result = detect_tool_call_start(text, Some("qwen3_coder")).unwrap();
        assert!(result);
    }

    #[test]
    fn test_e2e_detect_tool_call_start_xml_partial() {
        let text = r#"<tool_c"#; // Partial start token
        let result = detect_tool_call_start(text, Some("qwen3_coder")).unwrap();
        assert!(result);
    }


    #[tokio::test]
    async fn test_qwen3_coder_simple_tool_call() {
        let input = r#"<tool_call>
<function=execute_bash>
<parameter=command>
pwd && ls
</parameter>
</function>
</tool_call>"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("qwen3_coder"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "execute_bash");
        assert_eq!(args["command"], "pwd && ls");
    }

    #[tokio::test]
    async fn test_qwen3_coder_multiple_parameters() {
        let input = r#"<tool_call>
<function=get_current_weather>
<parameter=city>
Dallas
</parameter>
<parameter=state>
TX
</parameter>
<parameter=unit>
fahrenheit
</parameter>
</function>
</tool_call>"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("qwen3_coder"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_current_weather");
        assert_eq!(args["city"], "Dallas");
        assert_eq!(args["state"], "TX");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_qwen3_coder_with_normal_text() {
        let input = r#"I'll help you check the weather. <tool_call>
<function=get_current_weather>
<parameter=city>
San Francisco
</parameter>
<parameter=unit>
fahrenheit
</parameter>
</function>
</tool_call> Let me get that information for you."#;
        let (result, content) = detect_and_parse_tool_call(input, Some("qwen3_coder"), None)
            .await
            .unwrap();
        assert_eq!(
            content,
            Some("I'll help you check the weather. ".to_string())
        );
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_current_weather");
        assert_eq!(args["city"], "San Francisco");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_qwen3_coder_parallel_tool_calls() {
        let input = r#"<tool_call>
<function=get_current_weather>
<parameter=city>
Dallas
</parameter>
<parameter=state>
TX
</parameter>
<parameter=unit>
fahrenheit
</parameter>
</function>
</tool_call>
<tool_call>
<function=get_current_weather>
<parameter=city>
Orlando
</parameter>
<parameter=state>
FL
</parameter>
<parameter=unit>
fahrenheit
</parameter>
</function>
</tool_call>"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("qwen3_coder"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert_eq!(result.len(), 2);

        let (name1, args1) = extract_name_and_args(result[0].clone());
        assert_eq!(name1, "get_current_weather");
        assert_eq!(args1["city"], "Dallas");
        assert_eq!(args1["state"], "TX");
        assert_eq!(args1["unit"], "fahrenheit");

        let (name2, args2) = extract_name_and_args(result[1].clone());
        assert_eq!(name2, "get_current_weather");
        assert_eq!(args2["city"], "Orlando");
        assert_eq!(args2["state"], "FL");
        assert_eq!(args2["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_qwen3_coder_json_parameter_value() {
        let input = r#"<tool_call>
<function=process_data>
<parameter=config>
{"timeout": 30, "retries": 3}
</parameter>
</function>
</tool_call>"#;
        let tools = vec![ToolDefinition {
            name: "process_data".to_string(),
            parameters: Some(serde_json::json!({
                "properties": {
                    "config": {
                        "type": "array"
                    }
                }
            })),
        }];
        let (result, content) =
            detect_and_parse_tool_call(input, Some("qwen3_coder"), Some(&tools))
                .await
                .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "process_data");
        assert!(args["config"].is_object());
        assert_eq!(args["config"]["timeout"], 30);
        assert_eq!(args["config"]["retries"], 3);
    }

    #[tokio::test]
    async fn test_qwen3_coder_numeric_parameters() {
        let input = r#"<tool_call>
<function=calculate>
<parameter=x>
42
</parameter>
<parameter=y>
3.15
</parameter>
<parameter=enabled>
true
</parameter>
</function>
</tool_call>"#;
        let tools = vec![ToolDefinition {
            name: "calculate".to_string(),
            parameters: Some(serde_json::json!({
                "properties": {
                    "x": {"type": "int"},
                    "y": {"type": "float"},
                    "enabled": {"type": "bool"},
                }
            })),
        }];
        let (result, _) = detect_and_parse_tool_call(input, Some("qwen3_coder"), Some(&tools))
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "calculate");
        assert_eq!(args["x"], 42);
        assert_eq!(args["y"], 3.15);
        assert_eq!(args["enabled"], true);
    }

    #[tokio::test]
    async fn test_qwen3_coder_no_tool_calls() {
        let input = "This is just normal text without any tool calls.";
        let (result, content) = detect_and_parse_tool_call(input, Some("qwen3_coder"), None)
            .await
            .unwrap();
        assert_eq!(result.len(), 0);
        assert_eq!(content, Some(input.to_string()));
    }

    #[tokio::test]
    async fn test_qwen3_coder_compact_format() {
        let input = r#"<tool_call><function=search><parameter=query>rust programming</parameter><parameter=limit>10</parameter></function></tool_call>"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("qwen3_coder"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "search");
        assert_eq!(args["query"], "rust programming");
        assert_eq!(args["limit"], "10");
    }

    #[tokio::test]
    async fn test_qwen3_coder_html_entities() {
        let input = r#"<tool_call>
<function=print_message>
<parameter=text>
&lt;div&gt;Hello &amp; Welcome&lt;/div&gt;
</parameter>
</function>
</tool_call>"#;
        let (result, _) = detect_and_parse_tool_call(input, Some("qwen3_coder"), None)
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "print_message");
        assert_eq!(args["text"], "<div>Hello & Welcome</div>");
    }

    #[tokio::test]
    async fn test_qwen3_coder_three_parallel_calls() {
        let input = r#"<tool_call>
<function=get_current_weather>
<parameter=city>
Dallas
</parameter>
</function>
</tool_call>
<tool_call>
<function=get_current_weather>
<parameter=city>
Orlando
</parameter>
</function>
</tool_call>
<tool_call>
<function=get_current_weather>
<parameter=city>
Seattle
</parameter>
</function>
</tool_call>"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("qwen3_coder"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert_eq!(result.len(), 3);

        let cities = ["Dallas", "Orlando", "Seattle"];
        for (i, expected_city) in cities.iter().enumerate() {
            let (name, args) = extract_name_and_args(result[i].clone());
            assert_eq!(name, "get_current_weather");
            assert_eq!(args["city"], *expected_city);
        }
    }

    #[tokio::test]
    async fn test_qwen3_coder_mixed_tool_types() {
        let input = r#"<tool_call>
<function=get_current_weather>
<parameter=city>
Dallas
</parameter>
<parameter=unit>
fahrenheit
</parameter>
</function>
</tool_call>
<tool_call>
<function=web_search>
<parameter=query>
weather forecasting
</parameter>
<parameter=max_results>
5
</parameter>
</function>
</tool_call>"#;
        let tools = vec![ToolDefinition {
            name: "web_search".to_string(),
            parameters: Some(serde_json::json!({
                "properties": {
                    "max_results": {
                        "type": "uint"
                    }
                }
            })),
        }];
        let (result, content) =
            detect_and_parse_tool_call(input, Some("qwen3_coder"), Some(&tools))
                .await
                .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert_eq!(result.len(), 2);

        let (name1, args1) = extract_name_and_args(result[0].clone());
        assert_eq!(name1, "get_current_weather");
        assert_eq!(args1["city"], "Dallas");
        assert_eq!(args1["unit"], "fahrenheit");

        let (name2, args2) = extract_name_and_args(result[1].clone());
        assert_eq!(name2, "web_search");
        assert_eq!(args2["query"], "weather forecasting");
        assert_eq!(args2["max_results"], 5);
    }

    #[tokio::test]
    async fn test_qwen3_coder_array_parameter_value_without_tool_definition() {
        let input = r#"<tool_call>
<function=process_list>
<parameter=items>
[1, 2, 3, 4, 5]
</parameter>
</function>
</tool_call>"#;
        let (result, _) = detect_and_parse_tool_call(input, Some("qwen3_coder"), None)
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "process_list");
        assert_eq!(args["items"], serde_json::json!("[1, 2, 3, 4, 5]"));
    }

    #[tokio::test]
    async fn test_qwen3_coder_array_parameter_value_with_tool_definition() {
        let input = r#"<tool_call>
<function=process_list>
<parameter=items>
[1, 2, 3, 4, 5]
</parameter>
</function>
</tool_call>"#;
        let tools = vec![ToolDefinition {
            name: "process_list".to_string(),
            parameters: Some(serde_json::json!({
                "properties": {
                    "items": {
                        "type": "array"
                    }
                }
            })),
        }];
        let (result, _) = detect_and_parse_tool_call(input, Some("qwen3_coder"), Some(&tools))
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "process_list");
        assert!(args["items"].is_array());
        assert_eq!(args["items"], serde_json::json!([1, 2, 3, 4, 5]));
    }

    #[tokio::test]
    async fn test_minimax_m2_simple_tool_call() {
        let input = r#"<minimax:tool_call>
<invoke name="get_weather">
<parameter name="location">San Francisco</parameter>
<parameter name="unit">celsius</parameter>
</invoke>
</minimax:tool_call>"#;
        let (result, content) = detect_and_parse_tool_call(input, Some("minimax_m2"), None)
            .await
            .unwrap();
        assert_eq!(content, Some("".to_string()));
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco");
        assert_eq!(args["unit"], "celsius");
    }

    #[tokio::test]
    async fn test_minimax_m2_multiple_tool_calls() {
        let input = r#"<minimax:tool_call>
<invoke name="search_web">
<parameter name="query_tag">["technology", "events"]</parameter>
<parameter name="query_list">["OpenAI", "latest", "release"]</parameter>
</invoke>
<invoke name="search_web">
<parameter name="query_tag">["technology", "events"]</parameter>
<parameter name="query_list">["Gemini", "latest", "release"]</parameter>
</invoke>
</minimax:tool_call>"#;
        let tools = vec![ToolDefinition {
            name: "search_web".to_string(),
            parameters: Some(serde_json::json!({
                "properties": {
                    "query_tag": {"type": "array"},
                    "query_list": {"type": "array"}
                }
            })),
        }];
        let (result, _) = detect_and_parse_tool_call(input, Some("minimax_m2"), Some(&tools))
            .await
            .unwrap();
        assert_eq!(result.len(), 2);

        let (name1, args1) = extract_name_and_args(result[0].clone());
        assert_eq!(name1, "search_web");
        assert!(args1["query_tag"].is_array());
        assert_eq!(
            args1["query_tag"],
            serde_json::json!(["technology", "events"])
        );
        assert!(args1["query_list"].is_array());
        assert_eq!(
            args1["query_list"],
            serde_json::json!(["OpenAI", "latest", "release"])
        );

        let (name2, args2) = extract_name_and_args(result[1].clone());
        assert_eq!(name2, "search_web");
        assert!(args2["query_tag"].is_array());
        assert_eq!(
            args2["query_tag"],
            serde_json::json!(["technology", "events"])
        );
        assert!(args2["query_list"].is_array());
        assert_eq!(
            args2["query_list"],
            serde_json::json!(["Gemini", "latest", "release"])
        );
    }

    #[tokio::test]
    async fn test_minimax_m2_with_normal_text() {
        let input = r#"I'll help you check the weather. <minimax:tool_call>
<invoke name="get_weather">
<parameter name="location">Tokyo</parameter>
<parameter name="unit">fahrenheit</parameter>
</invoke>
</minimax:tool_call> Let me get that information for you."#;
        let (result, content) = detect_and_parse_tool_call(input, Some("minimax_m2"), None)
            .await
            .unwrap();
        assert!(content.is_some());
        assert!(
            content
                .unwrap()
                .contains("I'll help you check the weather.")
        );
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "Tokyo");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test]
    async fn test_minimax_m2_empty_parameters() {
        let input = r#"<minimax:tool_call>
<invoke name="get_time">
</invoke>
</minimax:tool_call>"#;
        let (result, _) = detect_and_parse_tool_call(input, Some("minimax_m2"), None)
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_time");
        assert_eq!(args, serde_json::json!({}));
    }

    #[tokio::test]
    async fn test_minimax_m2_with_type_conversion() {
        let input = r#"<minimax:tool_call>
<invoke name="process_data">
<parameter name="count">42</parameter>
<parameter name="temperature">98.6</parameter>
<parameter name="enabled">true</parameter>
</invoke>
</minimax:tool_call>"#;
        let tools = vec![ToolDefinition {
            name: "process_data".to_string(),
            parameters: Some(serde_json::json!({
                "properties": {
                    "count": {"type": "integer"},
                    "temperature": {"type": "number"},
                    "enabled": {"type": "boolean"}
                }
            })),
        }];
        let (result, _) = detect_and_parse_tool_call(input, Some("minimax_m2"), Some(&tools))
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "process_data");
        assert_eq!(args["count"], 42);
        assert_eq!(args["temperature"], 98.6);
        assert_eq!(args["enabled"], true);
    }

    #[tokio::test]
    async fn test_minimax_m2_array_parameter() {
        let input = r#"<minimax:tool_call>
<invoke name="batch_process">
<parameter name="items">[1, 2, 3, 4, 5]</parameter>
</invoke>
</minimax:tool_call>"#;
        let tools = vec![ToolDefinition {
            name: "batch_process".to_string(),
            parameters: Some(serde_json::json!({
                "properties": {
                    "items": {"type": "array"}
                }
            })),
        }];
        let (result, _) = detect_and_parse_tool_call(input, Some("minimax_m2"), Some(&tools))
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "batch_process");
        assert!(args["items"].is_array());
        assert_eq!(args["items"], serde_json::json!([1, 2, 3, 4, 5]));
    }
}
