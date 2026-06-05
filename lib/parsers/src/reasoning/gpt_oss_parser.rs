// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-License-Identifier: Apache-2.0
//
// API-compatible implementation based on the public interfaces and behavioral
// contracts of NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
// Implementation rewritten by PAGODA.

//!
//! ## 设计意图
//! 通道恢复被解析器剥除的元数据头供下游工具解析。
//!
//! ## 外部契约
//!
//! ## 实现要点

use std::fmt::Debug;
use std::sync::OnceLock;

use crate::ParserResult;
use crate::ReasoningParser;

use openai_harmony::StreamableParser;
use openai_harmony::chat::TextContent;
use openai_harmony::{HarmonyEncoding, HarmonyEncodingName, chat::Role, load_harmony_encoding};


static GLOBAL_HARMONY_GPTOSS_ENCODING: OnceLock<Result<HarmonyEncoding, anyhow::Error>> =
    OnceLock::new();

///
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


pub struct GptOssReasoningParser {
    parser: StreamableParser,
}

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

fn first_text(msg: &openai_harmony::chat::Message) -> Option<&str> {
    match msg.content.first() {
        Some(openai_harmony::chat::Content::Text(TextContent { text })) => Some(text),
        _ => None,
    }
}

impl ReasoningParser for GptOssReasoningParser {
    fn detect_and_parse_reasoning(&mut self, text: &str, token_ids: &[u32]) -> ParserResult {
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

        match parser.current_channel().as_deref() {
            Some("commentary") => {
                tracing::debug!("In commentary channel, recovering full content");
                // 以便工具解析器正确处理。
                if let Ok(enc) = get_harmony_encoding() {
                    let current_content = parser.current_content().unwrap_or_default();

                    // 通道、目标、约束元数据连同消息负载。
                    //
                    // 示例：
                    //   解析前：
                    //   解析后头部被剥除，需重建为：
                    //
                    let final_text = if current_content.is_empty() {
                        let tokens = parser.tokens();

                        let channel_token_id = enc
                            .tokenizer()
                            .encode_with_special_tokens("<|channel|>")
                            .last()
                            .copied();

                        let last_channel_token_idx = channel_token_id
                            .and_then(|token_id| tokens.iter().rposition(|t| *t == token_id))
                            .unwrap_or(0);

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
    //!
    //! ## 意义
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
