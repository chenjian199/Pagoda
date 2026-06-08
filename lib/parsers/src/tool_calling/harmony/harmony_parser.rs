// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-License-Identifier: Apache-2.0

//! # tool_calling::harmony::harmony_parser
//!
//! ## 设计意图
//! 解析 GPT-OSS 的 Harmony Format 工具调用。优先用 `openai_harmony` 的严格分词器把
//! 完整文本解析为消息，再从 `commentary` 通道、`functions.*` 收件人中抽取工具调用；
//! 分词器拒绝（截断 JSON、相邻并列 commentary 块等）时退化为正则抽取。
//!
//! ## 外部契约
//! - `get_harmony_encoding()`（异步、全局缓存）、`parse_tool_calls_harmony_complete(text, config, tools)`、
//!   `detect_tool_call_start_harmony(chunk, config, strict)`，签名与返回形态不变。
//! - 调用 id 形如 `call-<n>`（从 1 起）；`analysis` 通道内容并入 normal_text。
//! - 正则回退仅在 `config.allow_eof_recovery` 为真时启用，避免流式中途抽出半截调用。
//!
//! ## 实现要点
//! - 正则回退保留未被 commentary 块消费的残余文本作为 normal_text。
//! - 截断 JSON 通过 `try_repair_truncated_json` 尽力修复后重试。

use super::super::ToolDefinition;
use super::super::json::base_json_parser::try_repair_truncated_json;
use super::config::JsonParserConfig;
use super::response::{CalledFunction, ToolCallResponse, ToolCallType};
use openai_harmony::chat::{Content::Text, Role};
use openai_harmony::{HarmonyEncoding, HarmonyEncodingName, load_harmony_encoding};
use regex::Regex;
use serde_json::Value;
use std::sync::OnceLock;

// === SECTION: 正则回退 ===

static COMMENTARY_BLOCK_REGEX: OnceLock<Regex> = OnceLock::new();

/// 仅当 `openai_harmony` 分词器拒绝输入时使用的正则回退——此路径上的另一选择是静默丢弃。
/// 最坏情况是漏掉某个调用，绝不会凭空捏造（要求完整结构特征）。
fn commentary_block_regex() -> &'static Regex {
    COMMENTARY_BLOCK_REGEX.get_or_init(|| {
        // name 为 `[\w.\-]+`（字母数字 / 点 / 连字符 / 下划线）。
        // name 与 `<|message|>` 之间用非贪婪 `.*?` 容忍可选的 `<|constrain|>json` 与空白。
        // args 在 `<|call|>`（正常闭合）或字符串结尾（`\z`，即模型在 EOS / max_tokens
        // 前未发出 `<|call|>` 的 bare-envelope PARSER.batch.5 变体）处结束。
        Regex::new(
            r"(?s)<\|channel\|>commentary to=functions\.(?P<name>[\w.\-]+).*?<\|message\|>(?P<args>.*?)(?:<\|call\|>|\z)",
        )
        .expect("commentary block regex")
    })
}

/// 当 harmony 严格分词器拒绝输入（截断 JSON、相邻多 commentary 块等）时用正则抽取调用。
/// 返回 `(calls, residual_text)`，其中 residual_text 为未被匹配 commentary 块消费的全部文本——
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

        // 先直接解析；失败则尝试修复截断 JSON 后再解析；仍失败则保留原始串
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

// === SECTION: Harmony 编码全局缓存 ===

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

// === SECTION: 完整文本解析 ===

/// 使用直接 token 解析，从完整的 Harmony Format 文本片段中解析工具调用。
///
/// 该函数针对“内容已经一次性完整可用”的文本片段进行优化。
/// 它使用 `parse_messages_from_completion_tokens` 将所有 token 直接解析为
/// Harmony Format 消息，然后从 channel 为 `"commentary"` 且 recipient 为
/// `"functions.*"` 的消息中提取工具调用。
///
/// 该函数不会执行起始 token 检测，也不会进行逐 token 的流式解析，
/// 因此在处理完整文本片段时效率更高。
///
/// # 参数
/// * `text` - 要解析的完整 Harmony-format 字符串，不包含末尾的 stop token。
///   示例：
///   `<|channel|>commentary to=functions.get_current_weather <|constrain|>json<|message|>{"location":"San Francisco"}`
/// * `_config` - 解析器配置。目前未使用，但为了保持 API 一致性而保留。
///
/// # 返回
/// * `Ok((tool_calls, normal_text))` - 包含已提取工具调用和普通文本的元组。
/// * `Err(e)` - 如果由于编码或 tokenization 错误导致解析失败。
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

    // 用 harmony 编码把文本编码为 token
    let tokens: Vec<u32> = enc.tokenizer().encode_with_special_tokens(text);
    let messages = match enc.parse_messages_from_completion_tokens(tokens, Some(Role::Assistant)) {
        Ok(messages) => messages,
        Err(e) => {
            tracing::debug!(
                "Failed to parse messages from completion tokens: {e}. Falling back to regex extraction."
            );
            // 恢复路径：harmony 会拒绝并列 commentary 块与截断 JSON。
            // 受 `allow_eof_recovery` 控制，使流式栅栏（分词器常在所有 token 到齐前
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

        // 处理 commentary 通道
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
                        // 截断恢复：平衡未闭合的字符串 / 花括号（max_tokens / EOS 形态）后重试。
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
            // args 为有效 JSON 才加入结果
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
        // 处理 reasoning(analysis) 通道
        } else if channel == Some("analysis") {
            normal_text.push_str(match &message.content[0] {
                Text(t) => &t.text,
                _ => "",
            });
        }
    }
    Ok((res, Some(normal_text.to_string())))
}

// === SECTION: 起始 token 探测 ===

/// 判断 `chunk` 是否可能是某个起始 token 的前缀（按 Unicode 字符边界，
/// 整体等于前缀或以前缀结尾即命中）。供 strict / 非 strict 两路共用。
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

    // 优先检查完整起始 token
    let has_complete_token = config
        .tool_call_start_tokens
        .iter()
        .any(|token| !token.is_empty() && trimmed.contains(token));
    if has_complete_token {
        return true;
    }

    // 检查部分起始 token（流式场景，起始 token 跨多个 chunk）
    let has_partial_token = matches_partial_start_token(trimmed, &config.tool_call_start_tokens);

    if strict {
        has_partial_token
    } else {
        // 非 strict 模式额外放宽：命中已知模式 `<|channel|>`
        has_partial_token || trimmed.contains("<|channel|>")
    }
}

#[cfg(test)]
mod tests {
    //! ## 测试过程
    //! 围绕 Harmony 公开 API（`parse_tool_calls_harmony_complete`、
    //! `detect_tool_call_start_harmony`）覆盖：单/多调用、analysis 文本并入、
    //! 截断与 bare-envelope 正则恢复、残余文本保留、空/空白输入、重复同名调用，
    //! 以及 strict / 非 strict 起始 token 探测。
    //!
    //! ## 意义
    //! 锁定 harmony 引擎在严格分词与正则回退两条路径下的可观察行为。
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
