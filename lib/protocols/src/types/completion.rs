// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
// 重新导出上游 async-openai 的 completion 类型，并定义推理服务扩展。

//! 旧版 Completions 协议类型。
//!
//! ## 设计意图
//! 在上游 completion 类型基础上叠加推理服务所需的扩展（预计算 embeddings、严格 bool 校验
//! 的 echo、带连续用量统计的流式选项），其余部分尽量复用上游。
//!
//! ## 外部契约
//! - 重新导出上游 `CreateCompletionResponse`（与上游一致）。
//! - 公开 `CreateCompletionRequest` 及其 builder `CreateCompletionRequestArgs`。
//! - 公开类型别名 `CompletionResponseStream`。
//! - `echo` 字段只接受布尔值或 null，拒绝整数与字符串。
//!
//! ## 实现要点
//! `echo` 的严格校验通过两级 serde Visitor 实现：外层处理 Option，内层只放行 bool，
//! 其余输入类型经由 serde 默认行为产出「invalid type」错误，从而保留可观测的错误文案。

use std::collections::HashMap;
use std::pin::Pin;

use derive_builder::Builder;
use futures::Stream;
use serde::{Deserialize, Serialize};

use crate::error::OpenAIError;

use super::{ChatCompletionStreamOptions, Prompt, Stop};

// 从上游重新导出响应类型（完全一致）
pub use async_openai::types::completions::CreateCompletionResponse;

// === SECTION: echo 字段的严格 bool 反序列化 ===

/// echo 参数的自定义反序列化器，只接受布尔值。
/// 对整数与字符串给出清晰的错误信息予以拒绝。
fn deserialize_echo_bool<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    // 该期望文案在内外两层 Visitor 中复用，保证错误信息一致。
    const EXPECTED: &str = "echo parameter to be a boolean (true or false) or null";

    // 外层 Visitor：负责区分 null/缺失与有值。
    struct OptionLayer;

    impl<'de> serde::de::Visitor<'de> for OptionLayer {
        type Value = Option<bool>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str(EXPECTED)
        }

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            deserializer.deserialize_any(BoolLayer)
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(None)
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(None)
        }
    }

    // 内层 Visitor：只放行 bool；字符串显式拒绝，整数由默认实现拒绝。
    struct BoolLayer;

    impl<'de> serde::de::Visitor<'de> for BoolLayer {
        type Value = Option<bool>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str(EXPECTED)
        }

        fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(Some(value))
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Err(E::invalid_type(serde::de::Unexpected::Str(value), &EXPECTED))
        }
    }

    deserializer.deserialize_option(OptionLayer)
}

// === SECTION: Completion 请求类型 ===

/// 带推理服务扩展的 completion 请求。
///
/// 在上游 `CreateCompletionRequest` 基础上扩展了：
/// - `prompt_embeds`：base64 编码的 PyTorch 张量，用于预计算 embeddings
/// - `echo`：严格 bool 校验（拒绝整数/字符串）
/// - `stream_options`：使用我们扩展的 `ChatCompletionStreamOptions`（含 `continuous_usage_stats`）
#[derive(Clone, Serialize, Deserialize, Default, Debug, Builder, PartialEq)]
#[builder(name = "CreateCompletionRequestArgs")]
#[builder(pattern = "mutable")]
#[builder(setter(into, strip_option), default)]
#[builder(derive(Debug))]
#[builder(build_fn(error = "OpenAIError"))]
pub struct CreateCompletionRequest {
    pub model: String,
    pub prompt: Prompt,
    /// base64 编码的 PyTorch 张量，包含预计算 embeddings。
    /// prompt 与 prompt_embeds 至少需提供其一。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_embeds: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suffix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub n: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_options: Option<ChatCompletionStreamOptions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logprobs: Option<u8>,
    /// 在返回 completion 的同时回显 prompt。
    /// 严格 bool 校验 —— 拒绝整数与字符串。
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default, deserialize_with = "deserialize_echo_bool")]
    pub echo: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop: Option<Stop>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub presence_penalty: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frequency_penalty: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub best_of: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logit_bias: Option<HashMap<String, serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seed: Option<i64>,
}

/// 解析后的服务端事件流，直至收到服务器发来的 \[DONE\]。
pub type CompletionResponseStream =
    Pin<Box<dyn Stream<Item = Result<CreateCompletionResponse, OpenAIError>> + Send>>;

// === SECTION: 测试 ===

#[cfg(test)]
mod tests {
    //! completion 类型的统一测试模块。
    //!
    //! ## 测试过程
    //! 覆盖 echo 严格校验、Choice 序列化形态、Stop 多形态解析以及 builder 对上游 Stop 的兼容。
    //!
    //! ## 意义
    //! 这些用例既是协议标准契约的回归基线，也验证了本地扩展字段的可观测行为不变。

    use super::*;

    #[test]
    fn echo_rejects_integer() {
        //! ## 测试过程
        //! 传入整数形态的 echo，断言反序列化失败且错误文案包含 integer 与 echo parameter。
        //!
        //! ## 意义
        //! 保证整数被拒绝，且错误信息对客户端友好。
        let json = r#"{"model": "test_model", "prompt": "test", "echo": 1}"#;
        let result: Result<CreateCompletionRequest, _> = serde_json::from_str(json);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("invalid type"));
        assert!(err_msg.contains("integer"));
        assert!(err_msg.contains("echo parameter"));
    }

    #[test]
    fn echo_rejects_string() {
        //! ## 测试过程
        //! 传入字符串形态的 echo，断言反序列化失败且错误文案包含 string 与 echo parameter。
        //!
        //! ## 意义
        //! 保证字符串被显式拒绝，区别于布尔值。
        let json = r#"{"model": "test_model", "prompt": "test", "echo": "null"}"#;
        let result: Result<CreateCompletionRequest, _> = serde_json::from_str(json);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("invalid type"));
        assert!(err_msg.contains("string"));
        assert!(err_msg.contains("echo parameter"));
    }

    #[test]
    fn completion_choice_serializes_openai_shape() {
        //! ## 测试过程
        //! 构造一个 Choice 并序列化，断言 finish_reason 与 text 字段符合 OpenAI 形态。
        //!
        //! ## 意义
        //! 保证 Choice 的 wire 格式与 OpenAI 规范一致。
        use crate::types::{Choice, CompletionFinishReason};

        let choice = Choice {
            text: "hello".to_string(),
            index: 0,
            logprobs: None,
            finish_reason: Some(CompletionFinishReason::Stop),
        };

        let value = serde_json::to_value(choice).expect("serialize choice");

        assert_eq!(value["finish_reason"], "stop");
        assert_eq!(value["text"], "hello");
    }

    #[test]
    fn stop_accepts_token_id_array() {
        //! ## 测试过程
        //! 传入 token id 数组形态的 stop，断言解析为 `Stop::TokenIdArray`。
        //!
        //! ## 意义
        //! 验证 Stop 对 token id 数组的扩展支持。
        let json = r#"{"model": "test_model", "prompt": [1, 2, 3], "stop": [32, 34]}"#;
        let request: CreateCompletionRequest = serde_json::from_str(json).unwrap();

        assert_eq!(request.stop, Some(Stop::TokenIdArray(vec![32, 34])));
    }

    #[test]
    fn stop_accepts_string_and_string_array() {
        //! ## 测试过程
        //! 分别传入单字符串与字符串数组形态的 stop，断言解析为对应变体。
        //!
        //! ## 意义
        //! 验证 Stop 的字符串与字符串数组两种标准形态。
        let one_stop = r#"{"model": "test_model", "prompt": "hello", "stop": " The"}"#;
        let request: CreateCompletionRequest = serde_json::from_str(one_stop).unwrap();

        assert_eq!(request.stop, Some(Stop::String(" The".to_string())));

        let many_stops = r#"{"model": "test_model", "prompt": "hello", "stop": ["A", "B"]}"#;
        let request: CreateCompletionRequest = serde_json::from_str(many_stops).unwrap();

        assert_eq!(
            request.stop,
            Some(Stop::StringArray(vec!["A".to_string(), "B".to_string()]))
        );
    }

    #[test]
    fn stop_token_id_display_string_remains_string_stop() {
        //! ## 测试过程
        //! 传入形如 "token_id:576" 的字符串 stop，断言其仍按字符串形态解析而非误判为 token id。
        //!
        //! ## 意义
        //! 防止字符串形态的 token id 表示被错误解读为整数 token id。
        let json = r#"{"model": "test_model", "prompt": [1, 2, 3], "stop": "token_id:576"}"#;
        let request: CreateCompletionRequest = serde_json::from_str(json).unwrap();

        assert_eq!(request.stop, Some(Stop::String("token_id:576".to_string())));

        let json = r#"{"model": "test_model", "prompt": [1, 2, 3], "stop": ["token_id:576"]}"#;
        let request: CreateCompletionRequest = serde_json::from_str(json).unwrap();

        assert_eq!(
            request.stop,
            Some(Stop::StringArray(vec!["token_id:576".to_string()]))
        );
    }

    #[test]
    fn builder_accepts_upstream_stop_configuration() {
        //! ## 测试过程
        //! 用上游 `StopConfiguration` 通过 builder 设置 stop，断言转换为本地 `Stop::String`。
        //!
        //! ## 意义
        //! 验证 builder 对上游 Stop 配置的向后兼容。
        let upstream_stop = async_openai::types::chat::StopConfiguration::String("END".to_string());

        let request = CreateCompletionRequestArgs::default()
            .model("test_model")
            .prompt(Prompt::String("hello".to_string()))
            .stop(upstream_stop)
            .build()
            .unwrap();

        assert_eq!(request.stop, Some(Stop::String("END".to_string())));
    }

    #[test]
    fn stop_rejects_single_token_id() {
        //! ## 测试过程
        //! 传入单个裸整数形态的 stop，断言解析失败。
        //!
        //! ## 意义
        //! 单个 token id 必须以数组形态提供，裸整数应被拒绝。
        let json = r#"{"model": "test_model", "prompt": [1, 2, 3], "stop": 576}"#;
        let result: Result<CreateCompletionRequest, _> = serde_json::from_str(json);

        assert!(result.is_err());
    }
}
