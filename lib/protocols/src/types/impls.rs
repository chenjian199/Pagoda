// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
// 本地定义类型的便捷 trait 实现。
// 从上游 async-openai 重新导出的类型已自带各自的 impl。

//! 本地类型的便捷转换实现。
//!
//! ## 设计意图
//! 为本地定义的消息、工具选择、内容片段与 URL 类型集中提供 `From` 转换，
//! 降低上层构造请求时的样板代码。
//!
//! ## 外部契约
//! 提供从 `&str`/`String`/上游消息类型到本地枚举的 `From` 实现，转换语义与协议标准一致。
//!
//! ## 实现要点
//! URL 系列转换以声明宏统一生成，避免重复样板；非法 URL 仍以 `expect("Invalid URL")` 触发 panic，
//! 保持可观测行为不变。

use std::fmt::Display;

use super::{
    AudioUrl, ChatCompletionNamedToolChoice, ChatCompletionRequestAssistantMessage,
    ChatCompletionRequestAssistantMessageContent, ChatCompletionRequestMessage,
    ChatCompletionRequestMessageContentPartAudio, ChatCompletionRequestMessageContentPartAudioUrl,
    ChatCompletionRequestMessageContentPartImage, ChatCompletionRequestMessageContentPartText,
    ChatCompletionRequestMessageContentPartVideo, ChatCompletionRequestUserMessageContentPart,
    ChatCompletionToolChoiceOption, ChatCompletionToolType, FunctionName, ImageUrl, VideoUrl,
};

use crate::error::OpenAIError;

// === SECTION: 函数名与工具选择 ===

impl From<&str> for FunctionName {
    fn from(value: &str) -> Self {
        Self { name: value.into() }
    }
}

impl From<String> for FunctionName {
    fn from(value: String) -> Self {
        Self { name: value }
    }
}

impl From<&str> for ChatCompletionNamedToolChoice {
    fn from(value: &str) -> Self {
        Self {
            r#type: ChatCompletionToolType::Function,
            function: value.into(),
        }
    }
}

impl From<String> for ChatCompletionNamedToolChoice {
    fn from(value: String) -> Self {
        Self {
            r#type: ChatCompletionToolType::Function,
            function: value.into(),
        }
    }
}

impl From<&str> for ChatCompletionToolChoiceOption {
    fn from(value: &str) -> Self {
        match value {
            "auto" => Self::Auto,
            "none" => Self::None,
            _ => Self::Named(value.into()),
        }
    }
}

impl From<String> for ChatCompletionToolChoiceOption {
    fn from(value: String) -> Self {
        match value.as_str() {
            "auto" => Self::Auto,
            "none" => Self::None,
            _ => Self::Named(value.into()),
        }
    }
}

// === SECTION: 消息类型到 ChatCompletionRequestMessage 枚举的转换 ===

// 说明：上游类型（SystemMessage、ToolMessage 等）需要为本地的
// ChatCompletionRequestMessage 枚举提供 From 实现。

impl From<super::ChatCompletionRequestUserMessage> for ChatCompletionRequestMessage {
    fn from(value: super::ChatCompletionRequestUserMessage) -> Self {
        Self::User(value)
    }
}

impl From<async_openai::types::chat::ChatCompletionRequestSystemMessage>
    for ChatCompletionRequestMessage
{
    fn from(value: async_openai::types::chat::ChatCompletionRequestSystemMessage) -> Self {
        Self::System(value)
    }
}

impl From<async_openai::types::chat::ChatCompletionRequestDeveloperMessage>
    for ChatCompletionRequestMessage
{
    fn from(value: async_openai::types::chat::ChatCompletionRequestDeveloperMessage) -> Self {
        Self::Developer(value)
    }
}

impl From<async_openai::types::chat::ChatCompletionRequestToolMessage>
    for ChatCompletionRequestMessage
{
    fn from(value: async_openai::types::chat::ChatCompletionRequestToolMessage) -> Self {
        Self::Tool(value)
    }
}

impl From<async_openai::types::chat::ChatCompletionRequestFunctionMessage>
    for ChatCompletionRequestMessage
{
    fn from(value: async_openai::types::chat::ChatCompletionRequestFunctionMessage) -> Self {
        Self::Function(value)
    }
}

impl From<ChatCompletionRequestAssistantMessage> for ChatCompletionRequestMessage {
    fn from(value: ChatCompletionRequestAssistantMessage) -> Self {
        Self::Assistant(value)
    }
}

impl From<ChatCompletionRequestAssistantMessageContent> for ChatCompletionRequestAssistantMessage {
    fn from(value: ChatCompletionRequestAssistantMessageContent) -> Self {
        Self {
            content: Some(value),
            ..Default::default()
        }
    }
}

impl From<&str> for ChatCompletionRequestAssistantMessage {
    fn from(value: &str) -> Self {
        ChatCompletionRequestAssistantMessageContent::Text(value.into()).into()
    }
}

impl From<String> for ChatCompletionRequestAssistantMessage {
    fn from(value: String) -> Self {
        value.as_str().into()
    }
}

// === SECTION: 内容片段到 UserMessageContentPart 枚举的转换 ===

impl From<ChatCompletionRequestMessageContentPartText>
    for ChatCompletionRequestUserMessageContentPart
{
    fn from(value: ChatCompletionRequestMessageContentPartText) -> Self {
        ChatCompletionRequestUserMessageContentPart::Text(value)
    }
}

impl From<ChatCompletionRequestMessageContentPartImage>
    for ChatCompletionRequestUserMessageContentPart
{
    fn from(value: ChatCompletionRequestMessageContentPartImage) -> Self {
        ChatCompletionRequestUserMessageContentPart::ImageUrl(value)
    }
}

impl From<ChatCompletionRequestMessageContentPartAudio>
    for ChatCompletionRequestUserMessageContentPart
{
    fn from(value: ChatCompletionRequestMessageContentPartAudio) -> Self {
        ChatCompletionRequestUserMessageContentPart::InputAudio(value)
    }
}

impl From<ChatCompletionRequestMessageContentPartVideo>
    for ChatCompletionRequestUserMessageContentPart
{
    fn from(value: ChatCompletionRequestMessageContentPartVideo) -> Self {
        ChatCompletionRequestUserMessageContentPart::VideoUrl(value)
    }
}

impl From<ChatCompletionRequestMessageContentPartAudioUrl>
    for ChatCompletionRequestUserMessageContentPart
{
    fn from(value: ChatCompletionRequestMessageContentPartAudioUrl) -> Self {
        ChatCompletionRequestUserMessageContentPart::AudioUrl(value)
    }
}

// === SECTION: URL 类型转换 ===

/// 为带 `detail` 字段的 URL 类型批量生成 `From<&str>` 与 `From<String>`。
macro_rules! impl_url_from_with_detail {
    ($ty:ty) => {
        impl From<&str> for $ty {
            fn from(value: &str) -> Self {
                Self {
                    url: value.parse().expect("Invalid URL"),
                    detail: Default::default(),
                    uuid: None,
                }
            }
        }

        impl From<String> for $ty {
            fn from(value: String) -> Self {
                Self {
                    url: value.parse().expect("Invalid URL"),
                    detail: Default::default(),
                    uuid: None,
                }
            }
        }
    };
}

impl_url_from_with_detail!(ImageUrl);
impl_url_from_with_detail!(VideoUrl);

// AudioUrl 不含 detail 字段，单独实现。
impl From<&str> for AudioUrl {
    fn from(value: &str) -> Self {
        Self {
            url: value.parse().expect("Invalid URL"),
            uuid: None,
        }
    }
}

impl From<String> for AudioUrl {
    fn from(value: String) -> Self {
        Self {
            url: value.parse().expect("Invalid URL"),
            uuid: None,
        }
    }
}
