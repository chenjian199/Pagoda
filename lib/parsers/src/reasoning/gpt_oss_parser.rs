// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-License-Identifier: Apache-2.0

//! # reasoning::gpt_oss_parser
//!
//! ## 设计意图
//! 基于 `openai_harmony` 的 `StreamableParser` 解析 GPT-OSS 的 Harmony 推理输出，
//! 把 `analysis` 通道归为 reasoning、`final` 通道归为 normal_text，并在 `commentary`
//! 通道恢复被解析器剥除的元数据头供下游工具解析。
//!
//! ## 外部契约
//! - `GptOssReasoningParser`（`new() -> anyhow::Result<Self>`）实现 `ReasoningParser`。
//! - 入参 `token_ids` 为空时用 harmony 编码把 `text` 转为 token；harmony 出错则返回默认空结果。
//! - 流式按 token 处理，返回本次增量；commentary 通道一次性恢复完整内容。
//!
//! ## 实现要点
//! - harmony 编码全局只加载一次（`OnceLock`），且在独立 OS 线程加载以避免在异步上下文丢弃内层 Runtime。

use std::fmt::Debug;
use std::sync::OnceLock;

use crate::ParserResult;
use crate::ReasoningParser;

use openai_harmony::StreamableParser;
use openai_harmony::chat::TextContent;
use openai_harmony::{HarmonyEncoding, HarmonyEncodingName, chat::Role, load_harmony_encoding};

// === SECTION: Harmony 编码全局加载 ===

static GLOBAL_HARMONY_GPTOSS_ENCODING: OnceLock<Result<HarmonyEncoding, anyhow::Error>> =
    OnceLock::new();

/// 全局加载（至多一次）harmony 编码。
///
/// `load_harmony_encoding` 内部会构造 `reqwest::blocking::Client`（创建并丢弃一个
/// Tokio Runtime）。若首次调用恰在异步上下文（如 HTTP 请求处理）中，丢弃 Runtime 会 panic：
/// "Cannot drop a runtime in a context where blocking is not allowed"。因此在一条全新 OS
/// 线程上执行加载，使内层 Runtime 在任何异步上下文之外被丢弃。
fn get_harmony_encoding() -> &'static Result<HarmonyEncoding, anyhow::Error> {
    GLOBAL_HARMONY_GPTOSS_ENCODING.get_or_init(|| {
        std::thread::spawn(|| load_harmony_encoding(HarmonyEncodingName::HarmonyGptOss))
            .join()
            .unwrap_or_else(|_| Err(anyhow::anyhow!("harmony encoding loader thread panicked")))
    })
}

fn encode_text_to_tokens(text: &str) -> anyhow::Result<Vec<u32>> {
    let enc = get_harmony_encoding()
        .as_ref()
        .map_err(|e| anyhow::anyhow!("Failed to get harmony encoding: {e}"))?;
    Ok(enc.tokenizer().encode_with_special_tokens(text))
}

// === SECTION: 解析器类型 ===

pub struct GptOssReasoningParser {
    parser: StreamableParser,
}

/// 因 `StreamableParser` 未实现 Debug，单独为 `GptOssReasoningParser` 实现。
impl Debug for GptOssReasoningParser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GptOssReasoningParser")
            .field("parser", &self.parser.state_json())
            .finish()
    }
}

impl GptOssReasoningParser {
    pub fn new() -> anyhow::Result<Self> {
        let enc = get_harmony_encoding().as_ref().map_err(|e| {
            tracing::warn!("Failed to load Harmony encoding for GPT OSS: {e}");
            anyhow::anyhow!("Failed to load Harmony encoding: {e}")
        })?;
        let parser = StreamableParser::new(enc.clone(), Some(Role::Assistant)).map_err(|e| {
            tracing::warn!("Harmony StreamableParser init failed for GPT OSS: {e}");
            anyhow::anyhow!("Failed to load Harmony StreamableParser: {e}")
        })?;
        Ok(Self { parser })
    }
}

/// 从 Harmony 消息内容中取出首段文本（若为文本内容）。
fn first_text(msg: &openai_harmony::chat::Message) -> Option<&str> {
    match msg.content.first() {
        Some(openai_harmony::chat::Content::Text(TextContent { text })) => Some(text),
        _ => None,
    }
}

impl ReasoningParser for GptOssReasoningParser {
    fn detect_and_parse_reasoning(&mut self, text: &str, token_ids: &[u32]) -> ParserResult {
        // token_ids 为空时用 harmony 编码把 text 转 token（WAR：转向纯文本推理解析）
        let owned;
        let token_ids: &[u32] = if token_ids.is_empty() {
            owned = match encode_text_to_tokens(text) {
                Ok(tokens) => tokens,
                Err(err) => {
                    tracing::warn!("Failed to encode Harmony tokens: {err}");
                    return ParserResult::default();
                }
            };
            &owned
        } else {
            token_ids
        };

        let parser = &mut self.parser;

        for (i, token_id) in token_ids.iter().enumerate() {
            tracing::debug!(
                "Processing token {} of {}: {}",
                i + 1,
                token_ids.len(),
                token_id
            );
            if let Err(e) = parser.process(*token_id) {
                tracing::warn!("Harmony parse error for token_id {token_id}: {e}");
                return ParserResult::default();
            }
        }

        let output_msgs = parser.messages();
        tracing::debug!("Parser has {} output messages", output_msgs.len());

        // 0 条消息：内容仍在缓冲，全部当作推理
        let Some((last_msg, earlier_msgs)) = output_msgs.split_last() else {
            tracing::debug!("No output messages, using current content");
            return ParserResult {
                normal_text: String::new(),
                reasoning_text: parser.current_content().unwrap_or_default(),
            };
        };

        // 末条消息之前的所有消息归为推理
        let mut reasoning_text = String::new();
        for parse_msg in earlier_msgs {
            if let Some(text) = first_text(parse_msg) {
                reasoning_text.push_str(text);
            }
        }

        if earlier_msgs.is_empty() {
            // 仅 1 条消息：该消息为推理，正常文本取自当前缓冲内容
            if let Some(text) = first_text(last_msg) {
                reasoning_text.push_str(text);
            }
            ParserResult {
                normal_text: parser.current_content().unwrap_or_default(),
                reasoning_text,
            }
        } else {
            // 多条消息：末条为正常文本
            let normal_text = first_text(last_msg).unwrap_or_default().to_string();
            ParserResult {
                normal_text,
                reasoning_text,
            }
        }
    }

    fn parse_reasoning_streaming_incremental(
        &mut self,
        text: &str,
        token_ids: &[u32],
    ) -> ParserResult {
        // token_ids 为空时用 harmony 编码把 text 转 token
        let owned;
        let token_ids: &[u32] = if token_ids.is_empty() {
            owned = match encode_text_to_tokens(text) {
                Ok(tokens) => tokens,
                Err(err) => {
                    tracing::warn!("Failed to encode Harmony tokens: {err}");
                    return ParserResult::default();
                }
            };
            &owned
        } else {
            token_ids
        };

        let parser: &mut StreamableParser = &mut self.parser;
        let mut normal_delta = String::new();
        let mut reasoning_delta = String::new();

        for (i, token_id) in token_ids.iter().enumerate() {
            tracing::debug!(
                "Processing streaming token {} of {}: {}",
                i + 1,
                token_ids.len(),
                token_id
            );
            if let Err(e) = parser.process(*token_id) {
                tracing::warn!("Harmony parse error for token_id {token_id}: {e}");
                return ParserResult::default();
            }

            if let (Some(delta), Some(channel)) = (
                parser.last_content_delta().unwrap_or_default(),
                parser.current_channel(),
            ) {
                // `last_content_delta` 仅暴露最新 token 片段，故 `final`/`analysis`
                // 立即转发；commentary 需要被剥除的元数据，在下方回退路径重建。
                match channel.as_str() {
                    "final" => normal_delta.push_str(&delta),
                    "analysis" => reasoning_delta.push_str(&delta),
                    _ => {}
                }
            }
        }

        if !normal_delta.is_empty() || !reasoning_delta.is_empty() {
            tracing::debug!(
                "Returning aggregated deltas: normal: {} chars, reasoning: {} chars",
                normal_delta.len(),
                reasoning_delta.len()
            );
            return ParserResult {
                normal_text: normal_delta,
                reasoning_text: reasoning_delta,
            };
        }

        // 无增量：处理 commentary 通道的内容恢复
        match parser.current_channel().as_deref() {
            Some("commentary") => {
                tracing::debug!("In commentary channel, recovering full content");
                // 在 commentary 通道：返回原始 token 内容并恢复被解析器消费的内容，
                // 以便工具解析器正确处理。
                if let Ok(enc) = get_harmony_encoding() {
                    let current_content = parser.current_content().unwrap_or_default();

                    // 恢复被解析器消费的 commentary 元数据头，使工具调用解析器拿到
                    // 通道、目标、约束元数据连同消息负载。
                    //
                    // 示例：
                    //   解析前：
                    //   "<|start|>assistant<|channel|>commentary to=functions.get_current_weather <|constrain|>json<|message|>{\"format\":\"celsius\",\"location\":\"San Francisco\"}<|call|>"
                    //   解析后头部被剥除，需重建为：
                    //   "<|channel|>commentary to=functions.get_current_weather <|constrain|>json<|message|>"
                    //
                    // 恢复仅在 `current_content` 为空时进行一次。
                    let final_text = if current_content.is_empty() {
                        let tokens = parser.tokens();

                        let channel_token_id = enc
                            .tokenizer()
                            .encode_with_special_tokens("<|channel|>")
                            .last()
                            .copied();

                        // 在 tokens 中定位最后一个 <|channel|>（id 20005）
                        let last_channel_token_idx = channel_token_id
                            .and_then(|token_id| tokens.iter().rposition(|t| *t == token_id))
                            .unwrap_or(0);

                        // 取从最后一个 <|channel|> 到 parser.tokens() 末尾的生成文本
                        let end_token_idx = parser.tokens().len();
                        enc.tokenizer()
                            .decode_utf8(&parser.tokens()[last_channel_token_idx..end_token_idx])
                            .unwrap_or_default()
                    } else {
                        text.to_string()
                    };

                    return ParserResult {
                        normal_text: final_text,
                        reasoning_text: String::new(),
                    };
                }
            }
            Some(channel) => {
                tracing::warn!("Shouldn't be delta content after in channel: {}", channel);
            }
            None => {}
        }

        tracing::debug!("No deltas to return, returning empty result");
        ParserResult::default()
    }
}

#[cfg(test)]
mod tests {
    //! ## 测试过程
    //! 围绕 `GptOssReasoningParser` 的非流式与流式接口，验证：analysis→reasoning /
    //! final→normal 的通道归类、多种切块粒度（逐块/逐 token/变长块）下的增量聚合一致性，
    //! 以及 commentary 通道元数据头的恢复。
    //!
    //! ## 意义
    //! 锁定基于 harmony StreamableParser 的 GPT-OSS 推理切分与工具元数据恢复行为。
    use super::*;

    #[test] // REASONING.batch.1, PARSER.harmony.1
    fn test_gpt_oss_reasoning_parser() {
        let mut parser = GptOssReasoningParser::new().expect("Failed to create parser");
        let text = "<|channel|>analysis<|message|>The user asks a simple factual question: capital of Brazil. The answer is Brasília. No additional explanation needed.<|end|><|start|>assistant<|channel|>final<|message|>The capital of Brazil is Brasília.";
        let result = parser.detect_and_parse_reasoning(text, &[]);
        assert!(result.normal_text == "The capital of Brazil is Brasília.");
        assert!(
            result.reasoning_text
                == "The user asks a simple factual question: capital of Brazil. The answer is Brasília. No additional explanation needed."
        );
    }

    #[test] // REASONING.stream.3, REASONING.batch.1, PARSER.harmony.1
    fn test_gpt_oss_reasoning_parser_streaming() {
        let mut parser = GptOssReasoningParser::new().expect("Failed to create parser");
        let chunks = vec![
            "<|channel|>",
            "analysis<|message|>The user asks a simple factual question: capital of Brazil.",
            " The answer is Brasília. No additional explanation needed.",
            "<|end|><|start|>assistant<|channel|>final<|message|>",
            "The capital of Brazil is Brasília.",
        ];
        let mut reasoning_text_incr = String::new();
        let mut normal_text_incr = String::new();
        for chunk in chunks {
            let result = parser.parse_reasoning_streaming_incremental(chunk, &[]);
            normal_text_incr.push_str(&result.normal_text);
            reasoning_text_incr.push_str(&result.reasoning_text);
        }
        assert!(normal_text_incr == "The capital of Brazil is Brasília.");
        assert!(
            reasoning_text_incr
                == "The user asks a simple factual question: capital of Brazil. The answer is Brasília. No additional explanation needed."
        );
    }

    #[test] // REASONING.stream.3, REASONING.batch.1, PARSER.harmony.1
    fn test_gpt_oss_reasoning_parser_streaming_chunked() {
        let mut parser = GptOssReasoningParser::new().expect("Failed to create parser");
        let enc = get_harmony_encoding()
            .as_ref()
            .expect("Failed to get encoding");
        let text = "<|channel|>analysis<|message|>The user asks a simple factual question: capital of Brazil. The answer is Brasília. No additional explanation needed.<|end|><|start|>assistant<|channel|>final<|message|>The capital of Brazil is Brasília.";
        let token_ids = enc.tokenizer().encode_with_special_tokens(text);
        let mut reasoning_text_incr = String::new();
        let mut normal_text_incr = String::new();

        let mut idx = 0;
        let chunk_size = 4;
        while idx < token_ids.len() {
            let end = (idx + chunk_size).min(token_ids.len());
            let result =
                parser.parse_reasoning_streaming_incremental("Test text", &token_ids[idx..end]);
            normal_text_incr.push_str(&result.normal_text);
            reasoning_text_incr.push_str(&result.reasoning_text);
            idx = end;
        }

        assert_eq!(normal_text_incr, "The capital of Brazil is Brasília.");
        assert_eq!(
            reasoning_text_incr,
            "The user asks a simple factual question: capital of Brazil. The answer is Brasília. No additional explanation needed."
        );
    }

    #[test] // REASONING.stream.3, REASONING.batch.1, PARSER.harmony.1
    fn test_gpt_oss_reasoning_parser_streaming_variable_length_chunks() {
        let text = "<|channel|>analysis<|message|>User asks: \"Hey, quick check: is everything up and running?\" We should check system health using the provided function get_system_health. Use function.<|end|><|start|>assistant<|channel|>commentary to=functions.get_system_health <|constrain|>json<|message|>{}";
        let enc = get_harmony_encoding()
            .as_ref()
            .expect("Failed to get encoding");
        let token_ids = enc.tokenizer().encode_with_special_tokens(text);

        {
            let mut parser = GptOssReasoningParser::new().expect("Failed to create parser");
            let mut reasoning_text_incr = String::new();
            let mut normal_text_incr = String::new();
            for token in token_ids.iter() {
                let result = parser.parse_reasoning_streaming_incremental("", &[(*token)]);
                normal_text_incr.push_str(&result.normal_text);
                reasoning_text_incr.push_str(&result.reasoning_text);
            }
            assert_eq!(
                reasoning_text_incr,
                "User asks: \"Hey, quick check: is everything up and running?\" We should check system health using the provided function get_system_health. Use function."
            );
            assert_eq!(
                normal_text_incr,
                "<|channel|>commentary to=functions.get_system_health <|constrain|>json<|message|>"
            );
        }

        {
            let mut parser = GptOssReasoningParser::new().expect("Failed to create parser");
            let mut reasoning_text_incr = String::new();
            let mut normal_text_incr = String::new();
            let chunk_tokens = [
                vec![200005],
                vec![35644, 200008, 1844, 31064, 25, 392, 25216, 11, 4853],
                vec![2371, 25, 382, 5519, 869, 326, 6788, 16842, 1416, 1757],
                vec![2371, 2420, 3230, 2360, 290, 5181, 1114, 717, 39303, 126214],
                vec![
                    13, 7649, 1114, 13, 200007, 200006, 173781, 200005, 12606, 815,
                ],
                vec![
                    316, 28, 44580, 775, 39303, 126214, 220, 200003, 4108, 200008,
                ],
                vec![12083],
            ];
            let concatenated: Vec<u32> = chunk_tokens.iter().flatten().copied().collect();
            assert_eq!(concatenated, token_ids);

            for token in chunk_tokens.iter() {
                let result = parser.parse_reasoning_streaming_incremental("", token);
                normal_text_incr.push_str(&result.normal_text);
                reasoning_text_incr.push_str(&result.reasoning_text);
            }
            assert_eq!(
                reasoning_text_incr,
                "User asks: \"Hey, quick check: is everything up and running?\" We should check system health using the provided function get_system_health. Use function."
            );
            assert_eq!(
                normal_text_incr,
                "<|channel|>commentary to=functions.get_system_health <|constrain|>json<|message|>"
            );
        }
    }
}
