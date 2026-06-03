// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
// 重新导出上游 async-openai 的 chat 类型，并在其上定义推理服务扩展。
// 以 Pagoda 扩展为前缀、或完全不在上游规范中的类型，均附有说明扩展理由的文档。

//! Chat Completions 协议类型。
//!
//! ## 设计意图
//! 默认复用上游 chat 类型，仅对需要多模态、推理内容、灵活 `arguments`、连续用量
//! 统计等推理服务特性的类型进行本地自有与扩展。
//!
//! ## 外部契约
//! - 重新导出大量与上游结构一致的 chat 类型。
//! - 本地自有 `Stop`、`FunctionCall`、`ChatCompletionMessageToolCall`、
//!   `CreateChatCompletionRequest`、`ChatCompletionResponseMessage` 等类型，其名称/签名/wire 形态与协议标准一致。
//!
//! ## 实现要点
//! `arguments` 字段通过自定义反序列化同时接受 JSON 字符串与 JSON 对象，并统一归一化为字符串；
//! `Stop` 在标准形态之外额外接受 token id 数组。

use std::pin::Pin;

use derive_builder::Builder;
use futures::Stream;
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use crate::error::OpenAIError;

// === SECTION: 上游 async-openai 重新导出（类型不变） ===
// 以下类型与上游定义在结构上完全一致。
// 调用方仍应像以往一样通过 `dynamo_protocols::types::*` 使用它们。

pub use async_openai::types::chat::{
    ChatChoiceLogprobs,
    ChatCompletionAudio,
    ChatCompletionAudioFormat,
    ChatCompletionAudioVoice,
    ChatCompletionFunctionCall,
    ChatCompletionFunctions,
    ChatCompletionFunctionsArgs,
    ChatCompletionRequestAssistantMessageAudio,
    ChatCompletionRequestAssistantMessageContent,
    ChatCompletionRequestAssistantMessageContentPart,
    ChatCompletionRequestDeveloperMessage,
    ChatCompletionRequestDeveloperMessageArgs,
    ChatCompletionRequestDeveloperMessageContent,
    ChatCompletionRequestFunctionMessage,
    ChatCompletionRequestFunctionMessageArgs,
    ChatCompletionRequestMessageContentPartAudio,
    ChatCompletionRequestMessageContentPartRefusal,
    ChatCompletionRequestMessageContentPartText,
    ChatCompletionRequestSystemMessage,
    // Builder 类型（由 derive_builder 生成）
    ChatCompletionRequestSystemMessageArgs,
    ChatCompletionRequestSystemMessageContent,
    ChatCompletionRequestSystemMessageContentPart,
    ChatCompletionRequestToolMessage,
    ChatCompletionRequestToolMessageArgs,
    ChatCompletionRequestToolMessageContent,
    ChatCompletionRequestToolMessageContentPart,
    ChatCompletionResponseMessageAudio,
    ChatCompletionTokenLogprob,
    Choice,
    CompletionFinishReason,
    CompletionTokensDetails,
    CompletionUsage,
    FunctionObject,
    FunctionObjectArgs,
    InputAudio,
    InputAudioFormat,
    Logprobs,
    PredictionContent,
    PredictionContentContent,
    Prompt,
    PromptTokensDetails,
    ReasoningEffort,
    ResponseFormat,
    ResponseFormatJsonSchema,
    Role,
    ServiceTier,
    TopLogprobs,
    WebSearchContextSize,
    WebSearchLocation,
    WebSearchOptions,
    WebSearchUserLocation,
    WebSearchUserLocationType,
};

// === SECTION: Stop 条件（含 token id 扩展） ===

/// OpenAI 的 stop 配置，附加 Dynamo 的 token id stop 扩展。
///
/// 标准 OpenAI 形态接受字符串或字符串数组。Dynamo 另外接受整数数组，
/// 如 `"stop": [576]`，用于在 token 化输入/输出场景中表达 token id stop 条件。
/// 形如 `"token_id:576"` 的字符串仍为普通字符串 stop；`token_id:<id>` 格式仅是
/// logprobs 的输出展示格式。
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(untagged)]
pub enum Stop {
    String(String),
    StringArray(Vec<String>),
    TokenIdArray(Vec<u32>),
}

impl Stop {
    /// 返回字符串形态的 stop 列表；token id 数组返回 `None`。
    pub fn strings(&self) -> Option<Vec<String>> {
        match self {
            Stop::String(s) => Some(vec![s.clone()]),
            Stop::StringArray(arr) => Some(arr.clone()),
            Stop::TokenIdArray(_) => None,
        }
    }

    /// 返回 token id 形态的 stop 列表；字符串形态返回 `None`。
    pub fn token_ids(&self) -> Option<Vec<u32>> {
        match self {
            Stop::TokenIdArray(arr) => Some(arr.clone()),
            Stop::String(_) | Stop::StringArray(_) => None,
        }
    }
}

impl From<String> for Stop {
    fn from(value: String) -> Self {
        Stop::String(value)
    }
}

impl From<&str> for Stop {
    fn from(value: &str) -> Self {
        Stop::String(value.to_string())
    }
}

impl From<Vec<String>> for Stop {
    fn from(value: Vec<String>) -> Self {
        Stop::StringArray(value)
    }
}

impl From<Vec<u32>> for Stop {
    fn from(value: Vec<u32>) -> Self {
        Stop::TokenIdArray(value)
    }
}

impl From<async_openai::types::chat::StopConfiguration> for Stop {
    fn from(value: async_openai::types::chat::StopConfiguration) -> Self {
        match value {
            async_openai::types::chat::StopConfiguration::String(value) => Stop::String(value),
            async_openai::types::chat::StopConfiguration::StringArray(value) => {
                Stop::StringArray(value)
            }
        }
    }
}

// 上游重命名了 FinishReason（流式）—— 重新导出
pub use async_openai::types::chat::FinishReason;

// 上游使用 FunctionType，而我们曾使用 ChatCompletionToolType。
// 为兼容起见，两个名字都重新导出。
pub use async_openai::types::chat::FunctionType;

// === SECTION: 灵活的 arguments 反序列化辅助函数 ===
// 某些 agent 框架（如 LangChain、自定义 harness）会把工具调用的 arguments 以
// 预解析的 JSON 对象发送，而非规范的 JSON 字符串。下面的辅助函数把两种表示
// 归一化为 `String`，使下游代码无需针对 wire 格式分支。

/// 将必填的 arguments 归一化为 JSON 字符串；只接受字符串或对象。
fn deserialize_arguments<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::String(s) => Ok(s),
        v @ serde_json::Value::Object(_) => {
            // 对 Value 调用 serde_json::to_string 不会失败
            Ok(serde_json::to_string(&v).unwrap())
        }
        other => Err(D::Error::custom(format!(
            "expected string or object for `arguments`, got {other}"
        ))),
    }
}

/// 将可选的 arguments 归一化为 `Option<String>`；null/缺失 返回 `None`。
fn deserialize_arguments_opt<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    match value {
        None => Ok(None),
        Some(serde_json::Value::String(s)) => Ok(Some(s)),
        Some(v @ serde_json::Value::Object(_)) => serde_json::to_string(&v)
            .map(Some)
            .map_err(|e| D::Error::custom(e.to_string())),
        Some(other) => Err(D::Error::custom(format!(
            "expected string or object for `arguments`, got {other}"
        ))),
    }
}

// === SECTION: FunctionCall / FunctionCallStream（本地定义 + 灵活反序列化） ===
// 上游 `async-openai` 的 `arguments` 仅接受 JSON 字符串。
// 我们在本地定义这些类型，以便附加 `#[serde(deserialize_with)]`，
// 在 wire 上同时接受字符串与对象两种表示。

/// 应被调用的函数的名称与参数。
///
/// `arguments` 可为 JSON 字符串（`"{\"key\":\"value\"}"`）或 JSON 对象
/// （`{"key": "value"}`）；二者在反序列化时都归一化为 JSON 字符串，
/// 使调用方始终看到规范形式。
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Default)]
pub struct FunctionCall {
    pub name: String,
    #[serde(deserialize_with = "deserialize_arguments")]
    pub arguments: String,
}

/// [`FunctionCall`] 的流式变体，两个字段均为可选。
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Default)]
pub struct FunctionCallStream {
    pub name: Option<String>,
    #[serde(default, deserialize_with = "deserialize_arguments_opt")]
    pub arguments: Option<String>,
}

/// 流式工具调用块。
///
/// 在本地定义（而非从上游重新导出），因为其 `function` 字段引用了
/// 我们本地的 [`FunctionCallStream`]（携带灵活的 `arguments` 反序列化器）。
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Default)]
pub struct ChatCompletionMessageToolCallChunk {
    pub index: u32,
    pub id: Option<String>,
    pub r#type: Option<FunctionType>,
    pub function: Option<FunctionCallStream>,
}

// === SECTION: 与上游存在结构差异的类型（本地保留） ===

/// 图像细节级别。本地保留，因为上游在 ImageUrl 中使用了不同的字段类型。
#[derive(Debug, Serialize, Deserialize, Default, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ImageDetail {
    #[default]
    Auto,
    Low,
    High,
}

/// 图像内容片段 —— 使用我们扩展的 `ImageUrl`（含 `url::Url` 与 `uuid`）。
#[derive(Debug, Serialize, Deserialize, Clone, Builder, PartialEq)]
#[builder(name = "ChatCompletionRequestMessageContentPartImageArgs")]
#[builder(pattern = "mutable")]
#[builder(setter(into, strip_option))]
#[builder(derive(Debug))]
#[builder(build_fn(error = "OpenAIError"))]
pub struct ChatCompletionRequestMessageContentPartImage {
    pub image_url: ImageUrl,
}

/// 使用 `url::Url` 类型与可选 UUID 的图像 URL。
///
/// 与上游的差异：使用 `url::Url` 而非 `String`，并增加 `uuid` 字段
/// 以便在管道中跟踪多模态资产。
#[derive(Debug, Serialize, Deserialize, Clone, Builder, PartialEq)]
#[builder(name = "ImageUrlArgs")]
#[builder(pattern = "mutable")]
#[builder(setter(into, strip_option))]
#[builder(derive(Debug))]
#[builder(build_fn(error = "OpenAIError"))]
pub struct ImageUrl {
    pub url: Url,
    pub detail: Option<ImageDetail>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uuid: Option<Uuid>,
}

#[derive(Clone, Serialize, Default, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ChatCompletionToolType {
    #[default]
    Function,
}

#[derive(Clone, Serialize, Default, Debug, Deserialize, PartialEq)]
pub struct FunctionName {
    pub name: String,
}

#[derive(Clone, Serialize, Default, Debug, Deserialize, PartialEq)]
pub struct ChatCompletionNamedToolChoice {
    pub r#type: ChatCompletionToolType,
    pub function: FunctionName,
}

fn default_function_type() -> FunctionType {
    FunctionType::Function
}

/// 本地保留的工具调用，用于在一次性请求/响应负载中保留 `type: "function"`。
///
/// 与上游的差异：`type` 默认会被序列化，且反序列化时若缺失则默认为
/// `function`，从而同时兼容 Dynamo 历史 wire 格式与上游符合规范的输入。
#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct ChatCompletionMessageToolCall {
    pub id: String,
    #[serde(default = "default_function_type")]
    pub r#type: FunctionType,
    pub function: FunctionCall,
}

/// 本地保留的 tool choice 枚举，因为上游更改了各变体的名称。
#[derive(Clone, Serialize, Default, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ChatCompletionToolChoiceOption {
    #[default]
    None,
    Auto,
    Required,
    #[serde(untagged)]
    Named(ChatCompletionNamedToolChoice),
}

#[derive(Clone, Serialize, Default, Debug, Builder, Deserialize, PartialEq)]
#[builder(name = "ChatCompletionToolArgs")]
#[builder(pattern = "mutable")]
#[builder(setter(into, strip_option), default)]
#[builder(derive(Debug))]
#[builder(build_fn(error = "OpenAIError"))]
pub struct ChatCompletionTool {
    #[builder(default = "ChatCompletionToolType::Function")]
    pub r#type: ChatCompletionToolType,
    pub function: FunctionObject,
}

// === SECTION: 推理服务扩展（上游不存在） ===

/// 后端上报的命中 stop 条件。
///
/// 推理后端（vLLM、SGLang）会上报是哪个 stop 条件被触发：
/// - `String`：命中的用户提供的 stop 序列
/// - `Int`：命中的 stop token ID
/// - `IntArray`：以序列形式上报的命中 stop token ID
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(untagged)]
pub enum StopReason {
    String(String),
    Int(i64),
    IntArray(Vec<i64>),
}

/// 来自上一轮 assistant 的推理内容。
///
/// 可从以下两种形式反序列化：
/// - 普通字符串：`"reasoning_content": "thinking..."` -> `Text("thinking...")`
/// - 字符串数组：`"reasoning_content": ["seg1", "seg2"]` -> `Segments(["seg1", "seg2"])`
///
/// `Segments` 变体保留了 KV 缓存正确的上下文重建所需的交错推理顺序。
/// `segments[i]` 是在 `tool_calls[i]` 之前的推理；`segments[tool_calls.len()]`
/// 是最后一次工具调用之后的尾随推理。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(untagged)]
pub enum ReasoningContent {
    /// 扁平字符串 —— 单个推理块或遗留的向后兼容形式。
    Text(String),
    /// 交错片段。segments[i] 位于 tool_calls[i] 之前；
    /// segments[N] 是最后一次工具调用之后的尾随推理。
    Segments(Vec<String>),
}

impl ReasoningContent {
    /// 将所有片段（或原样返回文本）拼接为单个扁平字符串。
    pub fn to_flat_string(&self) -> String {
        match self {
            ReasoningContent::Text(s) => s.clone(),
            ReasoningContent::Segments(segs) => segs
                .iter()
                .filter(|s| !s.is_empty())
                .cloned()
                .collect::<Vec<_>>()
                .join("\n"),
        }
    }

    /// 若为 `Segments` 变体则返回其片段；`Text` 返回 `None`。
    pub fn segments(&self) -> Option<&[String]> {
        match self {
            ReasoningContent::Segments(segs) => Some(segs),
            ReasoningContent::Text(_) => None,
        }
    }
}

// === SECTION: 响应用多模态内容类型（上游不存在） ===

/// assistant 消息中的文本响应内容片段
#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct ChatCompletionResponseContentPartText {
    pub text: String,
}

/// assistant 消息中的图像 URL 响应内容片段
#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct ChatCompletionResponseContentPartImageUrl {
    pub image_url: ImageUrlResponse,
}

/// assistant 消息中的视频 URL 响应内容片段
#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct ChatCompletionResponseContentPartVideoUrl {
    pub video_url: VideoUrlResponse,
}

/// assistant 消息中的音频 URL 响应内容片段
#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct ChatCompletionResponseContentPartAudioUrl {
    pub audio_url: AudioUrlResponse,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct ImageUrlResponse {
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct VideoUrlResponse {
    pub url: String,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct AudioUrlResponse {
    pub url: String,
}

/// 支持多种模态的 assistant 响应内容片段
#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChatCompletionResponseContentPart {
    Text(ChatCompletionResponseContentPartText),
    ImageUrl(ChatCompletionResponseContentPartImageUrl),
    VideoUrl(ChatCompletionResponseContentPartVideoUrl),
    AudioUrl(ChatCompletionResponseContentPartAudioUrl),
}

/// assistant 消息内容 —— 可为简单字符串，也可为多模态内容片段。
///
/// 上游的 content 字段使用 `Option<String>`。我们将其扩展以支持来自
/// vLLM 等后端返回的多模态响应（文本 + 图像 + 视频 + 音频）。
#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum ChatCompletionMessageContent {
    /// 简单文本内容（向后兼容）
    Text(String),
    /// 内容片段数组（用于多模态响应）
    Parts(Vec<ChatCompletionResponseContentPart>),
}

// === SECTION: 多模态输入类型（视频/音频 URL 支持，上游不存在） ===

#[derive(Debug, Serialize, Deserialize, Clone, Builder, PartialEq)]
#[builder(name = "VideoUrlArgs")]
#[builder(pattern = "mutable")]
#[builder(setter(into, strip_option))]
#[builder(derive(Debug))]
#[builder(build_fn(error = "OpenAIError"))]
pub struct VideoUrl {
    pub url: Url,
    pub detail: Option<ImageDetail>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uuid: Option<Uuid>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Builder, PartialEq)]
#[builder(name = "ChatCompletionRequestMessageContentPartVideoArgs")]
#[builder(pattern = "mutable")]
#[builder(setter(into, strip_option))]
#[builder(derive(Debug))]
#[builder(build_fn(error = "OpenAIError"))]
pub struct ChatCompletionRequestMessageContentPartVideo {
    pub video_url: VideoUrl,
}

#[derive(Debug, Serialize, Deserialize, Clone, Builder, PartialEq)]
#[builder(name = "AudioUrlArgs")]
#[builder(pattern = "mutable")]
#[builder(setter(into, strip_option))]
#[builder(derive(Debug))]
#[builder(build_fn(error = "OpenAIError"))]
pub struct AudioUrl {
    pub url: Url,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uuid: Option<Uuid>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Builder, PartialEq)]
#[builder(name = "ChatCompletionRequestMessageContentPartAudioUrlArgs")]
#[builder(pattern = "mutable")]
#[builder(setter(into, strip_option))]
#[builder(derive(Debug))]
#[builder(build_fn(error = "OpenAIError"))]
pub struct ChatCompletionRequestMessageContentPartAudioUrl {
    pub audio_url: AudioUrl,
}

// === SECTION: 扩展的请求/响应类型 ===

/// 用户消息内容 —— 引用我们扩展的内容片段枚举。
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(untagged)]
pub enum ChatCompletionRequestUserMessageContent {
    Text(String),
    Array(Vec<ChatCompletionRequestUserMessageContentPart>),
}

#[derive(Debug, Serialize, Deserialize, Default, Clone, Builder, PartialEq)]
#[builder(name = "ChatCompletionRequestUserMessageArgs")]
#[builder(pattern = "mutable")]
#[builder(setter(into, strip_option), default)]
#[builder(derive(Debug))]
#[builder(build_fn(error = "OpenAIError"))]
pub struct ChatCompletionRequestUserMessage {
    pub content: ChatCompletionRequestUserMessageContent,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl Default for ChatCompletionRequestUserMessageContent {
    fn default() -> Self {
        Self::Text(String::new())
    }
}

impl From<&str> for ChatCompletionRequestUserMessageContent {
    fn from(value: &str) -> Self {
        Self::Text(value.into())
    }
}

impl From<String> for ChatCompletionRequestUserMessageContent {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<Vec<ChatCompletionRequestUserMessageContentPart>>
    for ChatCompletionRequestUserMessageContent
{
    fn from(value: Vec<ChatCompletionRequestUserMessageContentPart>) -> Self {
        Self::Array(value)
    }
}

/// 支持视频与音频 URL 的用户消息内容片段。
///
/// 在上游 `ChatCompletionRequestUserMessageContentPart` 基础上扩展：
/// - `VideoUrl`：多模态模型的视频输入
/// - `AudioUrl`：音频 URL 输入（与 base64 的 InputAudio 区分）
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum ChatCompletionRequestUserMessageContentPart {
    Text(ChatCompletionRequestMessageContentPartText),
    ImageUrl(ChatCompletionRequestMessageContentPartImage),
    VideoUrl(ChatCompletionRequestMessageContentPartVideo),
    AudioUrl(ChatCompletionRequestMessageContentPartAudioUrl),
    InputAudio(ChatCompletionRequestMessageContentPartAudio),
}

/// 支持推理内容的 assistant 消息。
///
/// 在上游 `ChatCompletionRequestAssistantMessage` 基础上扩展：
/// - `reasoning_content`：为 KV 缓存正确性保留的交错推理片段
///   （DeepSeek-R1、QwQ 模型）
#[derive(Debug, Serialize, Deserialize, Default, Clone, Builder, PartialEq)]
#[builder(name = "ChatCompletionRequestAssistantMessageArgs")]
#[builder(pattern = "mutable")]
#[builder(setter(into, strip_option), default)]
#[builder(derive(Debug))]
#[builder(build_fn(error = "OpenAIError"))]
pub struct ChatCompletionRequestAssistantMessage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<ChatCompletionRequestAssistantMessageContent>,
    /// 来自上一轮 assistant 的推理内容。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<ReasoningContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refusal: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio: Option<ChatCompletionRequestAssistantMessageAudio>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ChatCompletionMessageToolCall>>,
    #[deprecated]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function_call: Option<FunctionCall>,
}

/// Chat completion 请求消息枚举。
///
/// 重新定义以使用我们扩展的 `ChatCompletionRequestAssistantMessage`
/// （含 reasoning_content）与 `ChatCompletionRequestUserMessage`
/// （其引用了我们含视频/音频的扩展内容片段）。
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(tag = "role")]
#[serde(rename_all = "lowercase")]
pub enum ChatCompletionRequestMessage {
    Developer(ChatCompletionRequestDeveloperMessage),
    System(ChatCompletionRequestSystemMessage),
    User(ChatCompletionRequestUserMessage),
    Assistant(ChatCompletionRequestAssistantMessage),
    Tool(ChatCompletionRequestToolMessage),
    Function(ChatCompletionRequestFunctionMessage),
}

/// 响应侧的服务等级枚举（与请求侧的 `ServiceTier` 区分）。
///
/// 上游不存在 —— 后端上报实际服务该请求的等级。
#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ServiceTierResponse {
    Scale,
    Default,
    Flex,
    Priority,
}

/// 携带多模态内容与推理的 chat completion 响应消息。
///
/// 在上游 `ChatCompletionResponseMessage` 基础上扩展：
/// - `content`：使用 `Option<ChatCompletionMessageContent>`（多模态）而非 `Option<String>`
/// - `reasoning_content`：模型推理输出（DeepSeek-R1、QwQ）
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct ChatCompletionResponseMessage {
    /// 始终被序列化（为 None 时以 `null` 输出），以便客户端可依赖
    /// `content` 键与 `reasoning_content` 或 `tool_calls` 同时存在。
    /// 与上游 OpenAI API 形态一致（DGH-651）。
    pub content: Option<ChatCompletionMessageContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refusal: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ChatCompletionMessageToolCall>>,
    pub role: Role,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[deprecated]
    pub function_call: Option<FunctionCall>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio: Option<ChatCompletionResponseMessageAudio>,
    /// 模型产生的推理内容（DeepSeek-R1、QwQ）。
    pub reasoning_content: Option<String>,
}

/// 支持逐 chunk 上报用量的流式选项。
///
/// 在上游 `ChatCompletionStreamOptions` 基础上扩展：
/// - `continuous_usage_stats`：在每个 chunk 中都输出用量，而非仅最后一个
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
pub struct ChatCompletionStreamOptions {
    pub include_usage: bool,
    /// 为 true 时，每个流式 chunk 都包含用量统计。
    /// vLLM/SGLang 等后端支持此项以实现实时 token 计数。
    #[serde(default)]
    pub continuous_usage_stats: bool,
}

/// 支持多模态处理器的 chat completion 请求。
///
/// 在上游 `CreateChatCompletionRequest` 基础上扩展：
/// - `mm_processor_kwargs`：多模态处理器配置（vLLM 专有）
/// - 使用我们扩展的 `ChatCompletionRequestMessage`（含推理、视频/音频）
/// - 使用我们扩展的 `ChatCompletionStreamOptions`（含 continuous_usage_stats）
#[derive(Clone, Serialize, Default, Debug, Builder, Deserialize, PartialEq)]
#[builder(name = "CreateChatCompletionRequestArgs")]
#[builder(pattern = "mutable")]
#[builder(setter(into, strip_option), default)]
#[builder(derive(Debug))]
#[builder(build_fn(error = "OpenAIError"))]
pub struct CreateChatCompletionRequest {
    pub messages: Vec<ChatCompletionRequestMessage>,
    pub model: String,
    /// 多模态处理器配置（vLLM 专有）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mm_processor_kwargs: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub store: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frequency_penalty: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logit_bias: Option<std::collections::HashMap<String, serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logprobs: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_logprobs: Option<u8>,
    #[deprecated]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_completion_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub n: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modalities: Option<Vec<async_openai::types::chat::ResponseModalities>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prediction: Option<PredictionContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio: Option<ChatCompletionAudio>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub presence_penalty: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_format: Option<ResponseFormat>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seed: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<ServiceTier>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop: Option<Stop>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_options: Option<ChatCompletionStreamOptions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<ChatCompletionTool>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<ChatCompletionToolChoiceOption>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallel_tool_calls: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[deprecated]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function_call: Option<ChatCompletionFunctionCall>,
    #[deprecated]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub functions: Option<Vec<ChatCompletionFunctions>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub web_search_options: Option<WebSearchOptions>,
}

/// 携带扩展响应消息的 chat choice。
///
/// 使用我们的 `ChatCompletionResponseMessage`（多模态内容 + 推理）。
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct ChatChoice {
    pub index: u32,
    pub message: ChatCompletionResponseMessage,
    pub finish_reason: Option<FinishReason>,
    pub logprobs: Option<ChatChoiceLogprobs>,
}

/// 非流式 chat completion 响应。
#[derive(Debug, Deserialize, Clone, PartialEq, Serialize)]
pub struct CreateChatCompletionResponse {
    pub id: String,
    pub choices: Vec<ChatChoice>,
    pub created: u32,
    pub model: String,
    pub service_tier: Option<ServiceTierResponse>,
    pub system_fingerprint: Option<String>,
    pub object: String,
    pub usage: Option<CompletionUsage>,
}

pub type ChatCompletionResponseStream =
    Pin<Box<dyn Stream<Item = Result<CreateChatCompletionStreamResponse, OpenAIError>> + Send>>;

/// 携带推理内容的流式 delta。
///
/// 在上游 `ChatCompletionStreamResponseDelta` 基础上扩展：
/// - `content`：`Option<ChatCompletionMessageContent>`（多模态）而非 `Option<String>`
/// - `reasoning_content`：流式推理 token（DeepSeek-R1、QwQ）
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct ChatCompletionStreamResponseDelta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<ChatCompletionMessageContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function_call: Option<ChatCompletionStreamResponseDeltaFunctionCall>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ChatCompletionMessageToolCallChunk>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<Role>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refusal: Option<String>,
    /// 流式推理内容（DeepSeek-R1、QwQ 模型）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct ChatCompletionStreamResponseDeltaFunctionCall {
    pub name: Option<String>,
    #[serde(default, deserialize_with = "deserialize_arguments_opt")]
    pub arguments: Option<String>,
}

/// 流式 chat choice。
#[derive(Debug, Deserialize, Clone, PartialEq, Serialize)]
pub struct ChatChoiceStream {
    pub index: u32,
    pub delta: ChatCompletionStreamResponseDelta,
    pub finish_reason: Option<FinishReason>,
    pub logprobs: Option<ChatChoiceLogprobs>,
}

/// 携带扩展 choice 的流式 chat completion 响应。
#[derive(Debug, Deserialize, Clone, PartialEq, Serialize)]
pub struct CreateChatCompletionStreamResponse {
    pub id: String,
    pub choices: Vec<ChatChoiceStream>,
    pub created: u32,
    pub model: String,
    pub service_tier: Option<ServiceTierResponse>,
    pub system_fingerprint: Option<String>,
    pub object: String,
    pub usage: Option<CompletionUsage>,
}

// === SECTION: \u6d4b\u8bd5 ===
// \u672c\u6a21\u5757\u662f\u552f\u4e00\u7684 `mod tests`\uff0c\u65e2\u8986\u76d6\u91cd\u5199\u540e\u7684\u8def\u5f84\uff0c\u4e5f\u4fdd\u7559 协议标准 \u7684\u6807\u51c6\u6d4b\u8bd5\n// \u4f5c\u4e3a\u56de\u5f52\u57fa\u7ebf\u3002\u6240\u6709 JSON \u6d4b\u8bd5\u6570\u636e\u4e0e\u65ad\u8a00\u6587\u672c\u4fdd\u6301\u539f\u6837\uff08\u5c5e\u4e8e\u53ef\u89c2\u5bdf\u884c\u4e3a\uff09\u3002\n#[cfg(test)]
mod tests {
    use super::*;

    /// ## \u6d4b\u8bd5\u8fc7\u7a0b\n    /// \u5c06\u7eaf\u6574\u6570\u6570\u7ec4\u53cd\u5e8f\u5217\u5316\u4e3a `Stop`\u3002\n    /// ## \u610f\u4e49\n    /// \u9a8c\u8bc1 token id stop \u6269\u5c55\uff1a\u6574\u6570\u6570\u7ec4\u88ab\u8bc6\u522b\u4e3a `TokenIdArray`\u3002\n    #[test]
    fn stop_accepts_token_id_array() {
        let stop: Stop = serde_json::from_value(serde_json::json!([32, 34])).unwrap();

        assert_eq!(stop, Stop::TokenIdArray(vec![32, 34]));
    }

    /// ## \u6d4b\u8bd5\u8fc7\u7a0b
    /// \u5206\u522b\u53cd\u5e8f\u5217\u5316\u5355\u4e2a\u5b57\u7b26\u4e32\u4e0e\u5b57\u7b26\u4e32\u6570\u7ec4\u3002
    /// ## \u610f\u4e49
    /// \u9a8c\u8bc1\u6807\u51c6 OpenAI stop \u5f62\u6001\uff1a\u5b57\u7b26\u4e32 -> `String`\uff0c\u6570\u7ec4 -> `StringArray`\u3002
    #[test]
    fn stop_accepts_string_and_string_array() {
        let stop: Stop = serde_json::from_value(serde_json::json!(" The")).unwrap();

        assert_eq!(stop, Stop::String(" The".to_string()));

        let stop: Stop = serde_json::from_value(serde_json::json!(["A", "B"])).unwrap();

        assert_eq!(
            stop,
            Stop::StringArray(vec!["A".to_string(), "B".to_string()])
        );
    }

    /// ## 测试过程
    /// 反序列化形如 `"token_id:576"` 的字符串与其数组。
    /// ## 意义
    /// 验证 `token_id:<id>` 仅是输出展示格式，仍被当作普通字符串 stop。
    #[test]
    fn stop_token_id_display_string_remains_string_stop() {
        let stop: Stop = serde_json::from_value(serde_json::json!("token_id:576")).unwrap();

        assert_eq!(stop, Stop::String("token_id:576".to_string()));

        let stop: Stop = serde_json::from_value(serde_json::json!(["token_id:576"])).unwrap();

        assert_eq!(stop, Stop::StringArray(vec!["token_id:576".to_string()]));
    }

    /// ## 测试过程
    /// 将单个裸整数 `576` 反序列化为 `Stop`。
    /// ## 意义
    /// 验证单个 token id（非数组）被拒绝，保持 untagged 匹配的严格性。
    #[test]
    fn stop_rejects_single_token_id() {
        let result = serde_json::from_value::<Stop>(serde_json::json!(576));

        assert!(result.is_err());
    }

    /// ## 测试过程
    /// 从上游 `StopConfiguration` 转换为本地 `Stop`。
    /// ## 意义
    /// 验证与上游类型的互操作性（From 实现）。
    #[test]
    fn stop_converts_from_upstream_stop_configuration() {
        let upstream =
            async_openai::types::chat::StopConfiguration::StringArray(vec!["END".to_string()]);

        assert_eq!(
            Stop::from(upstream),
            Stop::StringArray(vec!["END".to_string()])
        );
    }

    /// ## 测试过程
    /// 反序列化缺失 `type` 字段的工具调用。
    /// ## 意义
    /// 验证 `type` 缺失时默认为 `FunctionType::Function`。
    #[test]
    fn tool_call_defaults_type_on_deserialize() {
        let tool_call: ChatCompletionMessageToolCall = serde_json::from_value(serde_json::json!({
            "id": "call_123",
            "function": {
                "name": "get_weather",
                "arguments": "{\"location\":\"SF\"}"
            }
        }))
        .unwrap();

        assert_eq!(tool_call.r#type, FunctionType::Function);
    }

    /// ## 测试过程
    /// 序列化工具调用并检查 wire 输出。
    /// ## 意义
    /// 验证 `type` 默认被序列化为 `"function"`，保障 wire 兼容性。
    #[test]
    fn tool_call_serializes_type_for_wire_compat() {
        let tool_call = ChatCompletionMessageToolCall {
            id: "call_123".into(),
            r#type: FunctionType::Function,
            function: FunctionCall {
                name: "get_weather".into(),
                arguments: "{\"location\":\"SF\"}".into(),
            },
        };

        let json = serde_json::to_value(tool_call).unwrap();
        assert_eq!(json["type"], "function");
    }

    // -- arguments 为 dict 格式的测试 --

    /// ## 测试过程
    /// arguments 以 JSON 字符串传入。
    /// ## 意义
    /// 验证字符串形式的 arguments 原样保留。
    #[test]
    fn function_call_accepts_string_arguments() {
        let fc: FunctionCall = serde_json::from_value(serde_json::json!({
            "name": "get_weather",
            "arguments": "{\"location\":\"SF\"}"
        }))
        .unwrap();
        assert_eq!(fc.arguments, "{\"location\":\"SF\"}");
    }

    /// ## 测试过程
    /// arguments 以 JSON 对象传入。
    /// ## 意义
    /// 验证对象形式被归一化为 JSON 字符串。
    #[test]
    fn function_call_accepts_dict_arguments() {
        let fc: FunctionCall = serde_json::from_value(serde_json::json!({
            "name": "get_weather",
            "arguments": {"location": "SF"}
        }))
        .unwrap();
        assert_eq!(fc.arguments, "{\"location\":\"SF\"}");
    }

    /// ## 测试过程
    /// arguments 传入整数。
    /// ## 意义
    /// 验证非字符串/非对象的整数被拒绝。
    #[test]
    fn function_call_rejects_integer_arguments() {
        let result = serde_json::from_value::<FunctionCall>(serde_json::json!({
            "name": "f",
            "arguments": 42
        }));
        assert!(result.is_err());
    }

    /// ## 测试过程
    /// arguments 传入布尔值。
    /// ## 意义
    /// 验证布尔值被拒绝。
    #[test]
    fn function_call_rejects_boolean_arguments() {
        let result = serde_json::from_value::<FunctionCall>(serde_json::json!({
            "name": "f",
            "arguments": true
        }));
        assert!(result.is_err());
    }

    /// ## 测试过程
    /// arguments 传入 null。
    /// ## 意义
    /// 验证必填的 arguments 拒绝 null。
    #[test]
    fn function_call_rejects_null_arguments() {
        let result = serde_json::from_value::<FunctionCall>(serde_json::json!({
            "name": "f",
            "arguments": null
        }));
        assert!(result.is_err());
    }

    /// ## 测试过程
    /// arguments 传入数组。
    /// ## 意义
    /// 验证数组（非对象）被拒绝。
    #[test]
    fn function_call_rejects_array_arguments() {
        let result = serde_json::from_value::<FunctionCall>(serde_json::json!({
            "name": "f",
            "arguments": [1, 2, 3]
        }));
        assert!(result.is_err());
    }

    /// ## 测试过程
    /// 流式 arguments 传入 null。
    /// ## 意义
    /// 验证流式变体允许 null并产生 `None`。
    #[test]
    fn function_call_stream_null_arguments_produces_none() {
        let fcs: FunctionCallStream = serde_json::from_value(serde_json::json!({
            "name": "f",
            "arguments": null
        }))
        .unwrap();
        assert_eq!(fcs.arguments, None);
    }

    /// ## 测试过程
    /// 流式 arguments 传入整数。
    /// ## 意义
    /// 验证流式变体仍拒绝整数。
    #[test]
    fn function_call_stream_rejects_integer_arguments() {
        let result = serde_json::from_value::<FunctionCallStream>(serde_json::json!({
            "name": "f",
            "arguments": 42
        }));
        assert!(result.is_err());
    }

    /// ## 测试过程
    /// 流式 arguments 传入布尔值。
    /// ## 意义
    /// 验证流式变体拒绝布尔值。
    #[test]
    fn function_call_stream_rejects_boolean_arguments() {
        let result = serde_json::from_value::<FunctionCallStream>(serde_json::json!({
            "name": "f",
            "arguments": true
        }));
        assert!(result.is_err());
    }

    /// ## 测试过程
    /// 流式 arguments 传入 dict。
    /// ## 意义
    /// 验证流式变体将对象归一化为 JSON 字符串。
    #[test]
    fn function_call_stream_accepts_dict_arguments() {
        let fcs: FunctionCallStream = serde_json::from_value(serde_json::json!({
            "name": "get_weather",
            "arguments": {"location": "SF"}
        }))
        .unwrap();
        assert_eq!(fcs.arguments.as_deref(), Some("{\"location\":\"SF\"}"));
    }

    /// ## 测试过程
    /// 流式载荷完全缺失 arguments。
    /// ## 意义
    /// 验证缺失 arguments 产生 `None`。
    #[test]
    fn function_call_stream_accepts_null_arguments() {
        let fcs: FunctionCallStream = serde_json::from_value(serde_json::json!({
            "name": "get_weather"
        }))
        .unwrap();
        assert_eq!(fcs.arguments, None);
    }

    /// ## 测试过程
    /// 以 dict 形式 arguments 反序列化工具调用后再序列化。
    /// ## 意义
    /// 验证 dict -> 字符串归一化后语义保持，且重新序列化输出为字符串而非对象。
    #[test]
    fn tool_call_with_dict_arguments_roundtrip() {
        let tc: ChatCompletionMessageToolCall = serde_json::from_value(serde_json::json!({
            "id": "call_abc",
            "type": "function",
            "function": {
                "name": "search",
                "arguments": {"query": "hello", "limit": 10}
            }
        }))
        .unwrap();
        // 由于键顺序不确定，按解析后的 JSON 值进行比较
        let parsed: serde_json::Value = serde_json::from_str(&tc.function.arguments).unwrap();
        assert_eq!(parsed, serde_json::json!({"query": "hello", "limit": 10}));
        // 重新序列化产生字符串而非对象
        let json = serde_json::to_value(&tc).unwrap();
        assert!(json["function"]["arguments"].is_string());
    }

    /// ## 测试过程
    /// 流式 delta 的 function_call.arguments 传入 dict。
    /// ## 意义
    /// 验证流式 delta 路径同样将对象归一化为 JSON 字符串。
    #[test]
    fn stream_delta_function_call_accepts_dict_arguments() {
        let delta: ChatCompletionStreamResponseDeltaFunctionCall =
            serde_json::from_value(serde_json::json!({
                "name": "get_weather",
                "arguments": {"location": "SF"}
            }))
            .unwrap();
        assert_eq!(delta.arguments.as_deref(), Some("{\"location\":\"SF\"}"));
    }
}
