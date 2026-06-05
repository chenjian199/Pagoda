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

use super::super::ToolDefinition;
use super::super::json::base_json_parser::try_repair_truncated_json;
use super::config::JsonParserConfig;
use super::response::{CalledFunction, ToolCallResponse, ToolCallType};
use openai_harmony::chat::{Content::Text, Role};
use openai_harmony::{HarmonyEncoding, HarmonyEncodingName, load_harmony_encoding};
use regex::Regex;
use serde_json::Value;
use std::sync::OnceLock;


static COMMENTARY_BLOCK_REGEX: OnceLock<Regex> = OnceLock::new();

/// 最坏情况是漏掉某个调用，绝不会凭空捏造（要求完整结构特征）。
fn commentary_block_regex() -> &'static Regex {
    COMMENTARY_BLOCK_REGEX.get_or_init(|| {
        Regex::new(
            r"(?s)<\|channel\|>commentary to=functions\.(?P<name>[\w.\-]+).*?<\|message\|>(?P<args>.*?)(?:<\|call\|>|\z)",
        )
        .expect("commentary block regex")
    })
}

/// 予以保留以免丢失分析散文与非工具后缀。
fn extract_calls_via_regex(text: &str) -> (Vec<ToolCallResponse>, String) {
    let mut out = Vec::new();
    let mut residual = String::new();
    let mut cursor = 0;
    for (i, cap) in commentary_block_regex().captures_iter(text).enumerate() {
        let m = cap.get(0).expect("regex match has full span");
        residual.push_str(&text[cursor..m.start()]);
        cursor = m.end();

        let name = cap.name("name").map_or("", |x| x.as_str());
        if name.is_empty() {
            continue;
        }
        let raw_args = cap.name("args").map_or("{}", |x| x.as_str().trim());

        let args_json = serde_json::from_str::<Value>(raw_args)
            .ok()
            .or_else(|| {
                try_repair_truncated_json(raw_args)
                    .and_then(|r| serde_json::from_str::<Value>(&r).ok())
            })
            .and_then(|v| serde_json::to_string(&v).ok())
            .unwrap_or_else(|| raw_args.to_string());

        out.push(ToolCallResponse {
            id: format!("call-{}", i + 1),
            tp: ToolCallType::Function,
            function: CalledFunction {
                name: name.to_string(),
                arguments: args_json,
            },
        });
    }
    residual.push_str(&text[cursor..]);
    (out, residual.trim().to_string())
}


static GLOBAL_HARMONY_GPTOSS_ENCODING: tokio::sync::OnceCell<
    Result<HarmonyEncoding, anyhow::Error>,
> = tokio::sync::OnceCell::const_new();

pub async fn get_harmony_encoding() -> &'static Result<HarmonyEncoding, anyhow::Error> {
    GLOBAL_HARMONY_GPTOSS_ENCODING
        .get_or_init(|| async {
            tokio::task::spawn_blocking(|| load_harmony_encoding(HarmonyEncodingName::HarmonyGptOss))
                .await
                .map_err(anyhow::Error::msg)
                .flatten()
        })
        .await
}


///
///
///
///
pub async fn parse_tool_calls_harmony_complete(
    text: &str,
    config: &JsonParserConfig,
    _tools: Option<&[ToolDefinition]>,
) -> anyhow::Result<(Vec<ToolCallResponse>, Option<String>)> {
    let enc = match get_harmony_encoding().await.as_ref() {
        Ok(e) => e,
        Err(e) => {
            tracing::debug!("Failed to load harmony encoding: {e}. Tool calls will not be parsed.");
            return Ok((vec![], Some(text.to_string())));
        }
    };

    let tokens: Vec<u32> = enc.tokenizer().encode_with_special_tokens(text);
    let messages = match enc.parse_messages_from_completion_tokens(tokens, Some(Role::Assistant)) {
        Ok(messages) => messages,
        Err(e) => {
            tracing::debug!(
                "Failed to parse messages from completion tokens: {e}. Falling back to regex extraction."
            );
            // 中途拒绝）不会抽出半截调用。
            if config.allow_eof_recovery {
                let (calls, residual) = extract_calls_via_regex(text);
                if !calls.is_empty() {
                    return Ok((calls, Some(residual)));
                }
            }
            return Ok((vec![], Some(text.to_string())));
        }
    };

    let mut normal_text = String::new();
    let mut res = Vec::with_capacity(messages.len());
    let mut call_idx = 0; // 工具调用序号

    for message in messages.iter() {
        if message.author.role != Role::Assistant {
            continue;
        }

        let channel = message.channel.as_deref();
        let recipient = message.recipient.as_deref().unwrap_or_default();

        if channel == Some("commentary") && recipient.starts_with("functions.") {
            let Some(fname) = message
                .recipient
                .as_ref()
                .and_then(|r| r.split('.').nth(1))
                .filter(|s| !s.is_empty())
                .map(str::to_string)
            else {
                continue;
            };

            let args = match message.content.first() {
                Some(Text(text)) => {
                    let trimmed = text.text.trim();
                    serde_json::from_str::<Value>(trimmed).unwrap_or_else(|_| {
                        // 受控以免流式提前退出抽出带合成闭合符的半截调用。
                        if config.allow_eof_recovery {
                            try_repair_truncated_json(trimmed)
                                .and_then(|r| serde_json::from_str::<Value>(&r).ok())
                                .unwrap_or(Value::Null)
                        } else {
                            Value::Null
                        }
                    })
                }
                _ => Value::Null, // 非文本内容则视为 null
            };
            if !args.is_null() {
                call_idx += 1;
                res.push(ToolCallResponse {
                    id: format!("call-{}", call_idx),
                    tp: ToolCallType::Function,
                    function: CalledFunction {
                        name: fname.to_string(),
                        arguments: serde_json::to_string(&args).unwrap(),
                    },
                });
            }
        } else if channel == Some("analysis") {
            normal_text.push_str(match &message.content[0] {
                Text(t) => &t.text,
                _ => "",
            });
        }
    }
    Ok((res, Some(normal_text.to_string())))
}


fn matches_partial_start_token(trimmed: &str, tokens: &[String]) -> bool {
    tokens.iter().any(|token| {
        if token.is_empty() {
            return false;
        }
        // 逐字符增长前缀，避免在多字节字符中间切断
        let mut prefix = String::new();
        for ch in token.chars() {
            prefix.push(ch);
            if trimmed == prefix || trimmed.ends_with(prefix.as_str()) {
                return true;
            }
        }
        false
    })
}

pub fn detect_tool_call_start_harmony(chunk: &str, config: &JsonParserConfig, strict: bool) -> bool {
    let trimmed = chunk.trim();
    if trimmed.is_empty() {
        return false;
    }

    let has_complete_token = config
        .tool_call_start_tokens
        .iter()
        .any(|token| !token.is_empty() && trimmed.contains(token));
    if has_complete_token {
        return true;
    }

    let has_partial_token = matches_partial_start_token(trimmed, &config.tool_call_start_tokens);

    if strict {
        has_partial_token
    } else {
        has_partial_token || trimmed.contains("<|channel|>")
    }
}

#[cfg(test)]
mod tests {
    //! ## 测试过程
    //!
    //! ## 意义
    use super::*;

    fn extract_name_and_args(call: ToolCallResponse) -> (String, serde_json::Value) {
        let args: serde_json::Value = serde_json::from_str(&call.function.arguments).unwrap();
        (call.function.name, args)
    }

    #[tokio::test] // PARSER.batch.1, PARSER.harmony.2
    async fn test_parse_tool_calls_harmony_complete_basic() {
        let text = r#"<|channel|>commentary to=functions.get_current_weather <|constrain|>json<|message|>{"format":"celsius","location":"San Francisco"}"#;
        let (tool_calls, normal_content) =
            parse_tool_calls_harmony_complete(text, &Default::default(), None)
                .await
                .unwrap();
        assert_eq!(normal_content, Some("".to_string()));
        let (name, args) = extract_name_and_args(tool_calls[0].clone());
        assert_eq!(name, "get_current_weather");
        assert_eq!(args["location"], "San Francisco");
        assert_eq!(args["format"], "celsius");
    }

    #[tokio::test] // PARSER.batch.4, PARSER.harmony.2
    async fn test_parse_tools_harmony_without_start_token() {
        let text = r#"<|channel|>analysis<|message|>Need to use function get_current_weather.<|end|><|message|>{"location":"San Francisco"}<|call|>"#;
        let (tool_calls, normal_content) =
            parse_tool_calls_harmony_complete(text, &Default::default(), None)
                .await
                .unwrap();
        assert_eq!(normal_content, Some(text.trim().to_string()));
        assert_eq!(tool_calls.len(), 0);
    }

    #[tokio::test] // PARSER.batch.7, PARSER.batch.8, PARSER.harmony.2
    async fn test_parse_tool_calls_harmony_with_multi_args() {
        let text = r#"<|channel|>analysis<|message|>Need to use function get_current_weather.<|end|><|start|>assistant<|channel|>commentary to=functions.get_current_weather <|constrain|>json<|message|>{"location":"San Francisco", "unit":"fahrenheit"}<|call|>"#;
        let (tool_calls, normal_content) =
            parse_tool_calls_harmony_complete(text, &Default::default(), None)
                .await
                .unwrap();
        assert_eq!(
            normal_content,
            Some("Need to use function get_current_weather.".to_string())
        );
        assert_eq!(tool_calls.len(), 1);
        let (name, args) = extract_name_and_args(tool_calls[0].clone());
        assert_eq!(name, "get_current_weather");
        assert_eq!(args["location"], "San Francisco");
        assert_eq!(args["unit"], "fahrenheit");
    }

    #[tokio::test] // PARSER.batch.8, PARSER.batch.8, PARSER.harmony.2
    async fn test_parse_tool_calls_harmony_with_normal_text() {
        let text = r#"<|channel|>analysis<|message|>Need to use function get_current_weather.<|end|><|start|>assistant<|channel|>commentary to=functions.get_current_weather <|constrain|>json<|message|>{"location":"San Francisco"}<|call|>"#;
        let (tool_calls, normal_content) =
            parse_tool_calls_harmony_complete(text, &Default::default(), None)
                .await
                .unwrap();
        assert_eq!(
            normal_content,
            Some("Need to use function get_current_weather.".to_string())
        );
        assert_eq!(tool_calls.len(), 1);
        let (name, args) = extract_name_and_args(tool_calls[0].clone());
        assert_eq!(name, "get_current_weather");
        assert_eq!(args["location"], "San Francisco");
    }

    #[tokio::test] // PARSER.batch.2 — gpt-oss
    async fn test_parse_harmony_multiple_calls_recovers() {
        let text = r#"<|start|>assistant<|channel|>commentary to=functions.a <|constrain|>json<|message|>{"x":1}<|call|><|start|>assistant<|channel|>commentary to=functions.b <|constrain|>json<|message|>{"y":2}<|call|>"#;
        let (tool_calls, _normal) = parse_tool_calls_harmony_complete(
            text,
            &JsonParserConfig {
                allow_eof_recovery: true,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        assert_eq!(tool_calls.len(), 2);
        let (n0, a0) = extract_name_and_args(tool_calls[0].clone());
        let (n1, a1) = extract_name_and_args(tool_calls[1].clone());
        assert_eq!(n0, "a");
        assert_eq!(a0["x"], 1);
        assert_eq!(n1, "b");
        assert_eq!(a1["y"], 2);
    }

    #[tokio::test] // PARSER.batch.4 — gpt-oss
    async fn test_parse_harmony_truncated_json_recovers() {
        let text = r#"<|start|>assistant<|channel|>commentary to=functions.get_weather <|constrain|>json<|message|>{"location":"NYC<|call|>"#;
        let (tool_calls, _normal) = parse_tool_calls_harmony_complete(
            text,
            &JsonParserConfig {
                allow_eof_recovery: true,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        assert_eq!(tool_calls.len(), 1);
        let (name, args) = extract_name_and_args(tool_calls[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "NYC");
    }

    #[tokio::test] // PARSER.batch.5 — gpt-oss
    async fn test_parse_harmony_bare_envelope_no_call_token_recovers() {
        let text = r#"<|start|>assistant<|channel|>commentary to=functions.get_weather <|constrain|>json<|message|>{"location":"NYC"}"#;
        let (tool_calls, _normal) = parse_tool_calls_harmony_complete(
            text,
            &JsonParserConfig {
                allow_eof_recovery: true,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        assert_eq!(tool_calls.len(), 1);
        let (name, args) = extract_name_and_args(tool_calls[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "NYC");
    }

    #[tokio::test]
    async fn test_parse_harmony_regex_fallback_preserves_residual_text() {
        let text = r#"PREFIX <|start|>assistant<|channel|>commentary to=functions.a <|constrain|>json<|message|>{"x":1}<|call|> SUFFIX"#;
        let (tool_calls, normal) = parse_tool_calls_harmony_complete(
            text,
            &JsonParserConfig {
                allow_eof_recovery: true,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        assert_eq!(tool_calls.len(), 1);
        let normal = normal.unwrap_or_default();
        assert!(
            normal.contains("PREFIX"),
            "normal must keep prefix: {normal:?}"
        );
        assert!(
            normal.contains("SUFFIX"),
            "normal must keep suffix: {normal:?}"
        );
    }

    #[tokio::test] // PARSER.batch.4, PARSER.batch.5, PARSER.harmony.2
    async fn test_parse_tool_calls_harmony_without_call_token() {
        let text = r#"<|channel|>analysis<|message|>We need to call get_weather function. The user asks "What's the weather like in San Francisco in Celsius?" So location: "San Francisco, CA" unit: "celsius". Let's call function.<|end|><|start|>assistant<|channel|>commentary to=functions.get_weather <|constrain|>json<|message|>{"location":"San Francisco, CA","unit":"celsius"}"#;
        let (tool_calls, normal_content) =
            parse_tool_calls_harmony_complete(text, &Default::default(), None)
                .await
                .unwrap();
        assert_eq!(normal_content, Some("We need to call get_weather function. The user asks \"What's the weather like in San Francisco in Celsius?\" So location: \"San Francisco, CA\" unit: \"celsius\". Let's call function.".to_string()));
        assert_eq!(tool_calls.len(), 1);
        let (name, args) = extract_name_and_args(tool_calls[0].clone());
        assert_eq!(name, "get_weather");
        assert_eq!(args["location"], "San Francisco, CA");
        assert_eq!(args["unit"], "celsius");
    }

    #[tokio::test]
    async fn test_harmony_parser_output_independent_of_upstream_finish() {
        let text = r#"<|channel|>commentary to=functions.get_current_weather <|constrain|>json<|message|>{"location":"NYC"}"#;
        let (tool_calls, _) = parse_tool_calls_harmony_complete(text, &Default::default(), None)
            .await
            .unwrap();
        assert_eq!(tool_calls.len(), 1);
    }

    #[tokio::test] // PARSER.batch.6 — gpt-oss
    async fn test_parse_harmony_empty_args() {
        let text =
            r#"<|channel|>commentary to=functions.current_time <|constrain|>json<|message|>{}"#;
        let (tool_calls, _) = parse_tool_calls_harmony_complete(text, &Default::default(), None)
            .await
            .unwrap();
        assert_eq!(tool_calls.len(), 1);
        let (name, args) = extract_name_and_args(tool_calls[0].clone());
        assert_eq!(name, "current_time");
        assert_eq!(args, serde_json::json!({}));
    }

    #[tokio::test] // PARSER.batch.9 — gpt-oss
    async fn test_parse_harmony_empty_and_whitespace_inputs() {
        for input in &["", " ", "\n", "\t\n  \t"] {
            let (tool_calls, normal) =
                parse_tool_calls_harmony_complete(input, &Default::default(), None)
                    .await
                    .unwrap();
            assert!(
                tool_calls.is_empty(),
                "Empty/whitespace input must yield no calls (input={:?})",
                input
            );
            assert_eq!(
                normal.as_deref(),
                Some(*input),
                "harmony passes empty/whitespace input verbatim to normal_text (input={:?})",
                input
            );
        }
    }

    #[tokio::test] // PARSER.batch.10 — gpt-oss
    async fn test_parse_harmony_duplicate_calls_same_name() {
        let text = r#"<|channel|>commentary to=functions.get_weather <|constrain|>json<|message|>{"city":"NYC"}<|call|><|start|>assistant<|channel|>commentary to=functions.get_weather <|constrain|>json<|message|>{"city":"LA"}<|call|>"#;
        let (tool_calls, _) = parse_tool_calls_harmony_complete(text, &Default::default(), None)
            .await
            .unwrap();
        assert_eq!(
            tool_calls.len(),
            2,
            "Both duplicate-name calls must be returned"
        );
        assert_ne!(
            tool_calls[0].id, tool_calls[1].id,
            "Duplicate calls must have distinct ids"
        );
        let (name0, args0) = extract_name_and_args(tool_calls[0].clone());
        let (name1, args1) = extract_name_and_args(tool_calls[1].clone());
        assert_eq!(name0, "get_weather");
        assert_eq!(name1, "get_weather");
        assert_eq!(args0["city"], "NYC");
        assert_eq!(args1["city"], "LA");
    }

    #[test] // helper
    fn test_detect_tool_call_start_harmony_chunk_with_tool_call_start_token() {
        let text = r#"<|start|>assistant<|channel|>commentary to=functions.get_current_weather <|constrain|>json"#;
        let config = JsonParserConfig {
            tool_call_start_tokens: vec!["<|start|>assistant<|channel|>commentary".to_string()],
            tool_call_end_tokens: vec!["<|call|>".to_string()],
            ..Default::default()
        };
        let result = detect_tool_call_start_harmony(text, &config, false);
        assert!(result);
    }

    #[test] // helper
    fn test_detect_tool_call_start_harmony_chunk_without_tool_call_start_token() {
        let text = r#"<|channel|>commentary to=functions.get_current_weather"#;
        let config = JsonParserConfig {
            tool_call_start_tokens: vec!["<|start|>assistant<|channel|>commentary".to_string()],
            tool_call_end_tokens: vec!["<|call|>".to_string()],
            ..Default::default()
        };
        let result = detect_tool_call_start_harmony(text, &config, false);
        assert!(result);
    }

    #[test] // helper, PARSER.stream.3
    fn test_detect_tool_call_start_harmony_partial_tokens() {
        let config = JsonParserConfig {
            tool_call_start_tokens: vec!["<|start|>assistant<|channel|>commentary".to_string()],
            tool_call_end_tokens: vec!["<|call|>".to_string()],
            ..Default::default()
        };

        assert!(
            detect_tool_call_start_harmony("<", &config, true),
            "'<' should be detected as potential start"
        );
        assert!(
            detect_tool_call_start_harmony("<|", &config, true),
            "'<|' should be detected as potential start"
        );
        assert!(
            detect_tool_call_start_harmony("<|start|>", &config, true),
            "'<|start|>' should be detected as potential start"
        );
        assert!(
            detect_tool_call_start_harmony("<|start|>assistant", &config, true),
            "'<|start|>assistant' should be detected as potential start"
        );

        assert!(
            !detect_tool_call_start_harmony("hello world", &config, true),
            "'hello world' should not be detected in strict mode"
        );
        assert!(
            !detect_tool_call_start_harmony("xyz", &config, true),
            "'xyz' should not be detected in strict mode"
        );
    }
}
