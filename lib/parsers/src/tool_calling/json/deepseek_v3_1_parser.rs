// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # tool_calling::json::deepseek_v3_1_parser
//!
//! ## 设计意图
//! 解析 DeepSeek V3.1 工具调用格式：外层 `<｜tool▁calls▁begin｜>...<｜tool▁calls▁end｜>`
//! 包裹，内层每个调用形如
//! `<｜tool▁call▁begin｜>{name}<｜tool▁sep｜>{args}<｜tool▁call▁end｜>`（参数直接跟在分隔符后，
//! 不再包 ```json 围栏）。
//!
//! ## 外部契约
//! - `parse_tool_calls_deepseek_v3_1`：返回 `(Vec<ToolCallResponse>, Option<String>)`，
//!   仅在出现结束 token 时解析；失败时整体回退为普通文本。
//! - `detect_tool_call_start_deepseek_v3_1`：完整或部分起始 token 命中时返回 true。
//!
//! ## 实现要点
//! - 抽取阶段用正则按内层 begin/end token 切块，保留多行参数空白。
//! - 解析阶段按分隔 token 一分为二取出名称与参数；参数解析失败时合并多行重试一次。

use regex::RegexBuilder;
use serde_json::Value;
use uuid::Uuid;

use super::super::ToolDefinition;
use super::config::JsonParserConfig;
use super::response::{CalledFunction, ToolCallResponse, ToolCallType};

// === SECTION: 调用块抽取 ===

/// 从输入中抽取 DeepSeek V3.1 的每个工具调用块，返回块内容（未去空白）列表。
///
/// DeepSeek V3.1 format: <｜tool▁call▁begin｜>{name}<｜tool▁sep｜>{args}<｜tool▁call▁end｜>
///
/// DeepSeek 使用嵌套 token：
/// - 外层包裹：<｜tool▁calls▁begin｜> ... <｜tool▁calls▁end｜>
/// - 内层单调用：<｜tool▁call▁begin｜> ... <｜tool▁call▁end｜>
fn extract_tool_call_blocks_v3_1(
    input: &str,
    start_tokens: &[String],
    end_tokens: &[String],
) -> Vec<String> {
    // 只保留内层单调用标记（排除外层 "calls" 包裹标记）
    let is_individual_begin =
        |t: &&String| t.contains("tool_call_begin") || t.contains("tool▁call▁begin");
    let is_individual_end =
        |t: &&String| t.contains("tool_call_end") || t.contains("tool▁call▁end");

    let individual_start_tokens: Vec<&String> =
        start_tokens.iter().filter(is_individual_begin).collect();
    let individual_end_tokens: Vec<&String> =
        end_tokens.iter().filter(is_individual_end).collect();

    // 逐个 begin/end token 组合尝试，首个命中即返回
    for start_token in &individual_start_tokens {
        for end_token in &individual_end_tokens {
            if start_token.is_empty() || end_token.is_empty() {
                continue;
            }

            let pattern = format!(
                r"{}(.*?){}",
                regex::escape(start_token),
                regex::escape(end_token)
            );
            let Ok(regex) = RegexBuilder::new(&pattern).dot_matches_new_line(true).build() else {
                continue;
            };

            // 不要 trim 内容，保留多行参数空白
            let blocks: Vec<String> = regex
                .captures_iter(input)
                .filter_map(|capture| capture.get(1))
                .map(|m| m.as_str().to_string())
                .filter(|content| !content.trim().is_empty())
                .collect();

            if !blocks.is_empty() {
                return blocks;
            }
        }
    }

    Vec::new()
}

// === SECTION: 单块解析 ===

/// 解析单个调用块（函数名与参数由分隔 token 分隔），返回 `(函数名, 参数 JSON)`。
///
/// Format: {function_name}<｜tool▁sep｜>{json_arguments}
fn parse_single_tool_call_v3_1(
    block: &str,
    separator_tokens: &[String],
) -> Option<(String, Value)> {
    for sep_token in separator_tokens {
        if sep_token.is_empty() {
            continue;
        }

        let Some((name_part, args_part)) = block.split_once(sep_token) else {
            continue;
        };
        let function_name = name_part.trim();
        let args_str = args_part.trim();

        // 函数名非空且不应含 JSON 括号字符
        if function_name.is_empty() || function_name.contains(['{', '}', '[', ']']) {
            continue;
        }

        // 先原样解析
        if let Ok(arguments) = serde_json::from_str::<Value>(args_str) {
            return Some((function_name.to_string(), arguments));
        }

        // 失败则把多行合并成单行做一次宽松重试
        let normalized = args_str
            .lines()
            .map(str::trim_start)
            .collect::<Vec<_>>()
            .join(" ");
        if let Ok(arguments) = serde_json::from_str::<Value>(&normalized) {
            return Some((function_name.to_string(), arguments));
        }
    }

    None
}

// === SECTION: 顶层解析入口 ===

pub fn parse_tool_calls_deepseek_v3_1(
    message: &str,
    config: &JsonParserConfig,
    _tools: Option<&[ToolDefinition]>,
) -> anyhow::Result<(Vec<ToolCallResponse>, Option<String>)> {
    // Format Structure:
    // <｜tool▁calls▁begin｜><｜tool▁call▁begin｜>{function_name}<｜tool▁sep｜>{json_arguments}<｜tool▁call▁end｜><｜tool▁calls▁end｜>
    let trimmed = message.trim();

    // 空输入直接返回
    if trimmed.is_empty() {
        return Ok((vec![], Some(String::new())));
    }

    // DeepSeek_v3_1 以外层 <｜tool▁calls▁begin｜>...<｜tool▁calls▁end｜> 为整体，
    // 仅看到结束 token 才开始解析；否则按 call(s) 全量 token 解析会错误分组：
    // 1. <｜tool▁calls▁begin｜><｜tool▁call▁begin｜>...<｜tool▁call▁end｜>
    // 2. <｜tool▁calls▁end｜>
    // 其中 2. 因缺起始 token 不会被识别为调用块的一部分而被遗漏。
    let has_end_token = config
        .tool_call_end_tokens
        .iter()
        .any(|token| !token.is_empty() && trimmed.contains(token));
    if !has_end_token {
        return Ok((vec![], Some(trimmed.to_string())));
    }

    let mut tool_call_start_tokens = config.tool_call_start_tokens.clone();
    tool_call_start_tokens.push("<｜tool▁call▁begin｜>".to_string());
    let mut tool_call_end_tokens = config.tool_call_end_tokens.clone();
    tool_call_end_tokens.push("<｜tool▁call▁end｜>".to_string());
    let separator_tokens = &config.tool_call_separator_tokens;

    // 未配置必要 token 时回退为普通文本
    if tool_call_start_tokens.is_empty() || separator_tokens.is_empty() {
        return Ok((vec![], Some(trimmed.to_string())));
    }

    // 未检测到起始 token 则回退
    if !detect_tool_call_start_deepseek_v3_1(trimmed, config) {
        return Ok((vec![], Some(trimmed.to_string())));
    }

    // 提取普通文本（首个外层 <｜tool▁calls▁begin｜> 之前的内容，注意是 "calls" 不是 "call"）
    let wrapper_tokens: Vec<&String> = tool_call_start_tokens
        .iter()
        .filter(|t| t.contains("tool_calls_begin") || t.contains("tool▁calls▁begin"))
        .collect();

    let normal_text = if !wrapper_tokens.is_empty() {
        wrapper_tokens
            .iter()
            .find_map(|token| {
                trimmed
                    .find(token.as_str())
                    .map(|idx| trimmed[..idx].to_string())
            })
            .unwrap_or_default()
    } else {
        // 无外层包裹时回退到首个内层调用 token
        tool_call_start_tokens
            .iter()
            .filter(|token| !token.is_empty())
            .find_map(|token| trimmed.find(token).map(|idx| trimmed[..idx].to_string()))
            .unwrap_or_default()
    };

    // 抽取每个调用块
    let blocks =
        extract_tool_call_blocks_v3_1(trimmed, &tool_call_start_tokens, &tool_call_end_tokens);

    if blocks.is_empty() {
        // 有起始 token 但无有效调用块
        return Ok((vec![], Some(trimmed.to_string())));
    }

    // 逐块解析出函数名与参数
    let mut tool_calls: Vec<ToolCallResponse> = Vec::new();
    for block in blocks {
        if let Some((function_name, arguments)) =
            parse_single_tool_call_v3_1(&block, separator_tokens)
        {
            tool_calls.push(ToolCallResponse {
                id: format!("call-{}", Uuid::new_v4()),
                tp: ToolCallType::Function,
                function: CalledFunction {
                    name: function_name,
                    arguments: serde_json::to_string(&arguments)?,
                },
            });
        }
    }

    // 没有任何有效调用则整体回退为普通文本
    if tool_calls.is_empty() {
        return Ok((vec![], Some(trimmed.to_string())));
    }

    Ok((tool_calls, Some(normal_text)))
}

// === SECTION: 起始探测（流式） ===

pub fn detect_tool_call_start_deepseek_v3_1(chunk: &str, config: &JsonParserConfig) -> bool {
    let trimmed = chunk.trim();
    if trimmed.is_empty() {
        return false;
    }

    // 先看完整起始 token
    let has_complete_token = config
        .tool_call_start_tokens
        .iter()
        .any(|token| !token.is_empty() && trimmed.contains(token));

    if has_complete_token {
        return true;
    }

    // 再看部分起始 token（流式分块，含 Unicode 字符）
    config.tool_call_start_tokens.iter().any(|token| {
        if token.is_empty() {
            return false;
        }
        // 逐字符长度构造前缀，正确处理 Unicode 边界
        token.char_indices().any(|(byte_idx, ch)| {
            let prefix_str = &token[..byte_idx + ch.len_utf8()];
            trimmed == prefix_str || trimmed.ends_with(prefix_str)
        })
    })
}

#[cfg(test)]
mod tests {
    //! ## 测试过程
    //! 用 `ToolCallConfig::deepseek_v3_1()` 取得真实配置，覆盖单调用、带普通文本、多调用、
    //! 多行参数等正常路径，以及非法 JSON 的回退路径；探测部分覆盖完整 token、中间出现 token、
    //! 逐级 Unicode 部分前缀与无关文本。
    //!
    //! ## 意义
    //! 确认解析器对 DeepSeek V3.1 的无围栏参数格式、外层包裹/内层调用结构与流式部分 token
    //! 的处理满足外部契约，非法输入整体退化为普通文本。

    use super::super::config::{ParserConfig, ToolCallConfig};
    use super::*;

    /// 取得 DeepSeek V3.1 的 JSON 解析配置。
    fn deepseek_v3_1_config() -> JsonParserConfig {
        match ToolCallConfig::deepseek_v3_1().parser_config {
            ParserConfig::Json(cfg) => cfg,
            _ => panic!("Expected JSON parser config"),
        }
    }

    /// 从一次调用结果中取出函数名与解析后的参数。
    fn extract_name_and_args(call: ToolCallResponse) -> (String, serde_json::Value) {
        let args: serde_json::Value = serde_json::from_str(&call.function.arguments).unwrap();
        (call.function.name, args)
    }

    #[test] // PARSER.batch.2
    fn test_parse_tool_calls_deepseek_v3_1_basic() {
        let text = r#"<｜tool▁calls▁begin｜><｜tool▁call▁begin｜>get_current_weather<｜tool▁sep｜>{"location": "Tokyo"}<｜tool▁call▁end｜><｜tool▁call▁begin｜>get_current_weather<｜tool▁sep｜>{"location": "Paris"}<｜tool▁call▁end｜><｜tool▁calls▁end｜><｜end▁of▁sentence｜>"#;
        let config = deepseek_v3_1_config();
        let (result, content) = parse_tool_calls_deepseek_v3_1(text, &config, None).unwrap();
        assert_eq!(content, Some("".to_string()));
        assert_eq!(result.len(), 2);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_current_weather");
        assert_eq!(args["location"], "Tokyo");
        let (name, args) = extract_name_and_args(result[1].clone());
        assert_eq!(name, "get_current_weather");
        assert_eq!(args["location"], "Paris");
    }

    #[test] // PARSER.batch.8
    fn test_parse_tool_calls_deepseek_v3_1_with_normal_text() {
        let text = r#"The following tool call retrieves weather information: <｜tool▁calls▁begin｜><｜tool▁call▁begin｜>get_current_weather<｜tool▁sep｜>{"location": "New York"}<｜tool▁call▁end｜><｜tool▁calls▁end｜><｜end▁of▁sentence｜>"#;
        let config = deepseek_v3_1_config();
        let (result, content) = parse_tool_calls_deepseek_v3_1(text, &config, None).unwrap();
        assert_eq!(
            content,
            Some("The following tool call retrieves weather information: ".to_string())
        );
        assert_eq!(result.len(), 1);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_current_weather");
        assert_eq!(args["location"], "New York");
    }

    #[test] // PARSER.batch.4 — recovery from missing start
    fn test_parse_tool_calls_deepseek_v3_1_without_tool_call_start_token() {
        let text = r#"<｜tool▁call▁begin｜>get_current_weather宽带}{location": "Tokyo"}<｜tool▁call▁end｜><｜tool▁calls▁end｜>"#;
        let config = deepseek_v3_1_config();
        let (result, content) = parse_tool_calls_deepseek_v3_1(text, &config, None).unwrap();
        assert_eq!(content, Some(text.to_string()));
        assert_eq!(result.len(), 0);
    }

    #[test] // PARSER.batch.2, PARSER.batch.7
    fn test_parse_tool_calls_deepseek_v3_1_with_multi_tool_calls_with_multiple_args() {
        let text = r#"<｜tool▁calls▁begin｜><｜tool▁call▁begin｜>get_current_weather<｜tool▁sep｜>{"location": "Berlin", "units": "metric"}<｜tool▁call▁end｜><｜tool▁call▁begin｜>get_weather_forecast<｜tool▁sep｜>{"location": "Berlin", "days": 7, "units": "imperial"}<｜tool▁call▁end｜><｜tool▁call▁begin｜>get_air_quality<｜tool▁sep｜>{"location": "Berlin", "radius": 50}<｜tool▁call▁end｜><｜tool▁calls▁end｜><｜end▁of▁sentence｜>"#;
        let config = deepseek_v3_1_config();
        let (result, content) = parse_tool_calls_deepseek_v3_1(text, &config, None).unwrap();
        assert_eq!(content, Some("".to_string()));
        assert_eq!(result.len(), 3);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "get_current_weather");
        assert_eq!(args["location"], "Berlin");
        assert_eq!(args["units"], "metric");
        let (name, args) = extract_name_and_args(result[1].clone());
        assert_eq!(name, "get_weather_forecast");
        assert_eq!(args["location"], "Berlin");
        assert_eq!(args["days"], 7);
        assert_eq!(args["units"], "imperial");
        let (name, args) = extract_name_and_args(result[2].clone());
        assert_eq!(name, "get_air_quality");
        assert_eq!(args["location"], "Berlin");
        assert_eq!(args["radius"], 50);
    }

    #[test] // PARSER.batch.4
    fn test_parse_tool_calls_deepseek_v3_1_with_invalid_json() {
        // 非法 JSON 时整体作为普通文本
        let text = r#"<｜tool▁calls▁begin｜><｜tool▁call▁begin｜>get_current_weather}{location": "Tokyo"}<｜tool▁call▁end｜><｜tool▁calls▁end｜>"#;
        let config = deepseek_v3_1_config();
        let (result, content) = parse_tool_calls_deepseek_v3_1(text, &config, None).unwrap();
        assert_eq!(content, Some(text.trim().to_string()));
        assert_eq!(result.len(), 0);
    }

    #[test] // PARSER.batch.2, PARSER.batch.8
    fn test_parse_tool_calls_deepseek_v3_1_with_multi_tool_calls_with_normal_text() {
        // 非法 JSON 时整体作为普通文本
        let text = r#"The following tool calls retrieve weather information: <｜tool▁calls▁begin｜><｜tool▁call▁begin｜>get_current_weather宽带}{location": "Tokyo"}<｜tool▁call▁end｜><｜tool▁call▁begin｜>get_weather_forecast宽带}{location": "Berlin", "days": 7, "units": "imperial"}<｜tool▁call▁end｜><｜tool▁call▁begin｜>get_air_quality宽带}{location": "Berlin", "radius": 50}<｜tool▁call▁end｜><｜tool▁calls▁end｜>"#;
        let config = deepseek_v3_1_config();
        let (result, content) = parse_tool_calls_deepseek_v3_1(text, &config, None).unwrap();
        assert_eq!(content, Some(text.trim().to_string()));
        assert_eq!(result.len(), 0);
    }

    #[test] // PARSER.batch.7, PARSER.fmt.2
    fn test_parse_tool_calls_deepseek_v3_1_with_multiline_json() {
        let text = r#"I'll help you understand this codebase. Let me start by exploring the structure and key
  files to provide you with a comprehensive
  explanation.<｜tool▁calls▁begin｜><｜tool▁call▁begin｜>TodoWrite<｜tool▁sep｜>{"todos":
  [{"content": "Explore the root directory structure", "status": "in_progress", "activeForm":
   "Exploring the root directory structure"}, {"content": "Examine package.json and
  configuration files", "status": "pending", "activeForm": "Examining package.json and
  configuration files"}, {"content": "Analyze source code structure and key modules",
  "status": "pending", "activeForm": "Analyzing source code structure and key modules"},
  {"content": "Identify main entry points and architectural patterns", "status": "pending",
  "activeForm": "Identifying main entry points and architectural patterns"}, {"content":
  "Summarize the codebase purpose and functionality", "status": "pending", "activeForm":
  "Summarizing the codebase purpose and
  functionality"}]}<｜tool▁call▁end｜><｜tool▁calls▁end｜>"#;
        let config = deepseek_v3_1_config();

        let (tool_call_results, normal_content) =
            parse_tool_calls_deepseek_v3_1(text, &config, None).unwrap();

        assert_eq!(tool_call_results.len(), 1);

        let (name, args) = extract_name_and_args(tool_call_results[0].clone());
        assert_eq!(name, "TodoWrite");
        assert_eq!(tool_call_results[0].tp, ToolCallType::Function);

        let todos_array = args["todos"].as_array().unwrap();
        assert_eq!(todos_array.len(), 5);

        assert_eq!(
            todos_array[0]["content"],
            "Explore the root directory structure"
        );
        assert_eq!(todos_array[0]["status"], "in_progress");
        assert_eq!(
            todos_array[0]["activeForm"],
            "Exploring the root directory structure"
        );

        assert_eq!(
            todos_array[1]["content"],
            "Examine package.json and configuration files"
        );
        assert_eq!(todos_array[1]["status"], "pending");

        assert_eq!(
            todos_array[4]["content"],
            "Summarize the codebase purpose and functionality"
        );
        assert_eq!(todos_array[4]["status"], "pending");

        assert_eq!(
            normal_content,
            Some("I'll help you understand this codebase. Let me start by exploring the structure and key\n  files to provide you with a comprehensive\n  explanation.".to_string())
        );
    }

    // === 起始探测 ===

    #[test] // helper
    fn test_detect_tool_call_start_deepseek_v3_1_chunk_with_tool_call_start_token() {
        let text = r#"<｜tool▁calls▁begin｜><｜tool▁call▁begin｜>get_current_weather宽带}"#;
        let config = deepseek_v3_1_config();
        assert!(detect_tool_call_start_deepseek_v3_1(text, &config));
    }

    #[test] // helper
    fn test_detect_tool_call_start_deepseek_v3_1_chunk_without_tool_call_start_token() {
        let text = r#"<｜tool▁call▁begin｜>get_current_weather宽带}"#;
        let config = deepseek_v3_1_config();
        assert!(!detect_tool_call_start_deepseek_v3_1(text, &config));
    }

    #[test] // helper
    fn test_detect_tool_call_start_deepseek_v3_1_chunk_with_tool_call_start_token_in_middle() {
        let text = r#"The following tool calls retrieve weather information: <｜tool▁calls▁begin｜><｜tool▁call▁begin｜>get_current_weather宽带}"#;
        let config = deepseek_v3_1_config();
        assert!(detect_tool_call_start_deepseek_v3_1(text, &config));
    }

    #[test] // helper, PARSER.stream.3
    fn test_detect_tool_call_start_deepseek_v3_1_partial_tokens() {
        // 流式场景下的部分 token 探测（含 Unicode 字符）
        let config = deepseek_v3_1_config();

        // 各级部分前缀应被识别
        for prefix in ["<", "<｜", "<｜tool", "<｜tool▁calls"] {
            assert!(
                detect_tool_call_start_deepseek_v3_1(prefix, &config),
                "{:?} should be detected as potential start",
                prefix
            );
        }

        // 无关文本不应被识别
        for unrelated in ["hello world", "xyz"] {
            assert!(
                !detect_tool_call_start_deepseek_v3_1(unrelated, &config),
                "{:?} should not be detected",
                unrelated
            );
        }
    }
}

