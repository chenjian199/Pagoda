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
//!   的名称、签名与可观察行为保持不变。
//!
//! ## 实现要点
//! - 分派函数仅承担「按类型选择实现」职责，不含具体解析逻辑。
//!   边界推进策略，其余解析器一律视为整段。

pub mod base_json_parser;
pub mod deepseek_v3_1_parser;
pub mod deepseek_v3_parser;

pub use super::config::JsonParserConfig;
pub use super::response::ToolCallResponse;
pub use super::{config, response};
pub use base_json_parser::{detect_tool_call_start_basic_json, try_tool_call_parse_basic_json};
pub use deepseek_v3_1_parser::{
    detect_tool_call_start_deepseek_v3_1, parse_tool_calls_deepseek_v3_1,
};
pub use deepseek_v3_parser::{detect_tool_call_start_deepseek_v3, parse_tool_calls_deepseek_v3};


#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Default)]
pub enum JsonParserType {
    #[default]
    Basic,
    DeepseekV3,
    DeepseekV31,
}


pub fn try_tool_call_parse_json(
    message: &str,
    config: &JsonParserConfig,
    tools: Option<&[super::ToolDefinition]>,
) -> anyhow::Result<(Vec<ToolCallResponse>, Option<String>)> {
    match config.parser_type {
        JsonParserType::Basic => try_tool_call_parse_basic_json(message, config, tools),
        JsonParserType::DeepseekV3 => parse_tool_calls_deepseek_v3(message, config, tools),
        JsonParserType::DeepseekV31 => parse_tool_calls_deepseek_v3_1(message, config, tools),
    }
}

pub fn detect_tool_call_start_json(chunk: &str, config: &JsonParserConfig) -> bool {
    match config.parser_type {
        JsonParserType::Basic => detect_tool_call_start_basic_json(chunk, config),
        JsonParserType::DeepseekV3 => detect_tool_call_start_deepseek_v3(chunk, config),
        JsonParserType::DeepseekV31 => detect_tool_call_start_deepseek_v3_1(chunk, config),
    }
}


///
/// 返回最终游标位置；遇到不完整的后续块时停在上一个完整块结束处。
fn advance_past_consecutive_blocks(
    chunk: &str,
    start_token: Option<&str>,
    end_token: &str,
) -> usize {
    let Some(first_end) = chunk.find(end_token) else {
        return chunk.len();
    };
    let mut cursor = first_end + end_token.len();

    let Some(start_tok) = start_token else {
        return cursor;
    };

    loop {
        let rest = &chunk[cursor..];
        let trimmed = rest.trim_start();
        if !trimmed.starts_with(start_tok) {
            break;
        }
        let leading_ws = rest.len() - trimmed.len();
        let search_from = cursor + leading_ws + start_tok.len();
        match chunk[search_from..].find(end_token) {
            Some(end_pos) => cursor = search_from + end_pos + end_token.len(),
            None => break,
        }
    }
    cursor
}

pub fn find_tool_call_end_position_json(
    chunk: &str,
    parser: &str,
    config: &JsonParserConfig,
) -> usize {
    match parser {
        "hermes" | "nemotron_deci" => match config.tool_call_end_tokens.first() {
            Some(end_token) => {
                let start_token = config.tool_call_start_tokens.first().map(String::as_str);
                advance_past_consecutive_blocks(chunk, start_token, end_token.as_str())
            }
            None => chunk.len(),
        },
        "mistral" | "phi4" => match chunk.rfind(']') {
            Some(pos) => pos + 1,
            None => chunk.len(),
        },
        _ => chunk.len(),
    }
}


#[cfg(test)]
mod tests {
    //! ## 测试过程
    //! 围绕两类对外可观察行为构造用例：
    //!
    //! ## 意义

    use super::*;

    fn nemotron_deci_config() -> JsonParserConfig {
        JsonParserConfig {
            tool_call_start_tokens: vec!["<TOOLCALL>".to_string()],
            tool_call_end_tokens: vec!["</TOOLCALL>".to_string()],
            ..Default::default()
        }
    }

    fn first_args(calls: &[ToolCallResponse]) -> serde_json::Value {
        serde_json::from_str(&calls[0].function.arguments).unwrap()
    }

    #[test]
    fn end_position_captures_parallel_and_stops_on_incomplete() {
        let config = JsonParserConfig {
            tool_call_start_tokens: vec!["<tool_call>".to_string()],
            tool_call_end_tokens: vec!["</tool_call>".to_string()],
            ..Default::default()
        };

        let two = concat!(
            "<tool_call>{\"name\": \"foo\", \"arguments\": {\"x\": 1}}</tool_call>",
            "<tool_call>{\"name\": \"bar\", \"arguments\": {\"y\": 2}}</tool_call>",
            "trailing"
        );
        let pos = find_tool_call_end_position_json(two, "hermes", &config);
        assert!(two[..pos].ends_with("</tool_call>"));
        assert_eq!(&two[pos..], "trailing");

        // 三个以换行分隔的并行调用：空白不应中断推进。
        let three = concat!(
            "<tool_call>{\"name\": \"a\"}</tool_call>\n",
            "<tool_call>{\"name\": \"b\"}</tool_call>\n",
            "<tool_call>{\"name\": \"c\"}</tool_call> done"
        );
        let pos3 = find_tool_call_end_position_json(three, "hermes", &config);
        assert!(three[..pos3].ends_with("</tool_call>"));
        assert_eq!(three[pos3..].trim(), "done");

        // 第二个块不完整：应停在第一个完整块结束处。
        let incomplete = concat!(
            "<tool_call>{\"name\": \"a\"}</tool_call>",
            "<tool_call>{\"name\": \"b\""
        );
        let pos_inc = find_tool_call_end_position_json(incomplete, "hermes", &config);
        assert_eq!(pos_inc, "<tool_call>{\"name\": \"a\"}</tool_call>".len());
    }

    #[test]
    fn parse_recovers_when_outer_close_missing() {
        let config = JsonParserConfig {
            allow_eof_recovery: true,
            ..nemotron_deci_config()
        };
        let input = r#"<TOOLCALL>[{"name":"get_weather","arguments":{"city":"NYC"}}]"#;
        let (calls, _) = try_tool_call_parse_json(input, &config, None).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "get_weather");
        assert_eq!(first_args(&calls)["city"], "NYC");
    }

    #[test]
    fn parse_recovers_truncated_json_args() {
        let config = JsonParserConfig {
            allow_eof_recovery: true,
            ..nemotron_deci_config()
        };
        let input = r#"<TOOLCALL>[{"name":"get_weather","arguments":{"city":"NYC</TOOLCALL>"#;
        let (calls, _) = try_tool_call_parse_json(input, &config, None).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "get_weather");
        assert_eq!(first_args(&calls)["city"], "NYC");
    }

    #[test]
    fn parse_multiple_calls_in_single_block() {
        let config = nemotron_deci_config();
        let input = r#"<TOOLCALL>[{"name":"get_weather","arguments":{"city":"NYC"}},{"name":"get_time","arguments":{"tz":"EST"}}]</TOOLCALL>"#;
        let (calls, normal) = try_tool_call_parse_json(input, &config, None).unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].function.name, "get_weather");
        assert_eq!(calls[1].function.name, "get_time");
        assert_eq!(normal, Some(String::new()));
    }

    #[test]
    fn parse_empty_args_keeps_function_name() {
        let config = nemotron_deci_config();
        let input = r#"<TOOLCALL>[{"name":"current_time","arguments":{}}]</TOOLCALL>"#;
        let (calls, _) = try_tool_call_parse_json(input, &config, None).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "current_time");
        assert_eq!(first_args(&calls), serde_json::json!({}));
    }

    #[test]
    fn parse_empty_or_whitespace_yields_no_calls() {
        let config = nemotron_deci_config();
        for input in ["", " ", "\n", "\t\n  \t"] {
            let (calls, normal) = try_tool_call_parse_json(input, &config, None).unwrap();
            assert!(calls.is_empty(), "input={input:?} 应无工具调用");
            assert_eq!(normal.as_deref(), Some(""), "input={input:?} 应折叠为空文本");
        }
    }

    #[test]
    fn parse_duplicate_names_get_distinct_ids() {
        let config = nemotron_deci_config();
        let input = r#"<TOOLCALL>[{"name":"get_weather","arguments":{"city":"NYC"}},{"name":"get_weather","arguments":{"city":"LA"}}]</TOOLCALL>"#;
        let (calls, _) = try_tool_call_parse_json(input, &config, None).unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].function.name, "get_weather");
        assert_eq!(calls[1].function.name, "get_weather");
        assert_ne!(calls[0].id, calls[1].id);
        let args0: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
        let args1: serde_json::Value = serde_json::from_str(&calls[1].function.arguments).unwrap();
        assert_eq!(args0["city"], "NYC");
        assert_eq!(args1["city"], "LA");
    }

    #[test]
    fn parse_output_independent_of_upstream_finish() {
        let config = nemotron_deci_config();
        let input = r#"<TOOLCALL>[{"name":"get_weather","arguments":{"city":"NYC"}}]</TOOLCALL>"#;
        let (calls, _) = try_tool_call_parse_json(input, &config, None).unwrap();
        assert_eq!(calls.len(), 1);
    }
}
