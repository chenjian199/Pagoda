// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Anthropic Messages API 类型。
//!
//! ## 设计意图
//! 为 `/v1/messages` 端点提供纯协议类型 —— 请求、响应、流式事件、
//! 错误形态与 count-tokens 类型。
//!
//! ## 外部契约
//! 完全自有的 Anthropic 类型集。公开类型名、字段与 wire 形态与协议标准规范一致，
//! 以保证 `/v1/messages` 的 JSON 兼容性。
//!
//! ## 实现要点
//! 内容块采用自定义反序列化，未知块类型被保留为 `Other(Value)` 而不报错；
//! `cache_control` 的 TTL 被限制在 [300, 3600] 区间内。

use serde::{Deserialize, Serialize};

// === SECTION: 缓存控制 ===

/// Anthropic 风格的缓存控制提示，用于带 TTL 的前缀钉住。
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
pub struct CacheControl {
    #[serde(rename = "type")]
    pub control_type: CacheControlType,
    /// TTL，以秒（整数）或简写（"5m" = 300s，"1h" = 3600s）表示。限制在 [300, 3600]。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum CacheControlType {
    #[default]
    Ephemeral,
    #[serde(other)]
    Unknown,
}

const MIN_TTL_SECONDS: u64 = 300;
const MAX_TTL_SECONDS: u64 = 3600;

impl CacheControl {
    /// 将 TTL 字符串解析为秒，限制在 [300, 3600]。
    ///
    /// 接受整数秒（"120"、"600"）或简写（"5m"、"1h"）。
    /// 低于 300 的值被限制为 300；高于 3600 的值被限制为 3600。
    /// 无法识别的字符串默认为 300s。
    pub fn ttl_seconds(&self) -> u64 {
        let raw = match self.ttl.as_deref() {
            None => return MIN_TTL_SECONDS,
            Some("5m") => 300,
            Some("1h") => 3600,
            Some(other) => match other.parse::<u64>() {
                Ok(secs) => secs,
                Err(_) => {
                    tracing::warn!("Unrecognized TTL '{}', defaulting to 300s", other);
                    return MIN_TTL_SECONDS;
                }
            },
        };
        raw.clamp(MIN_TTL_SECONDS, MAX_TTL_SECONDS)
    }
}
// === SECTION: 系统提示与请求体 ===

/// 解析后的系统提示内容，从块数组中保留 cache_control。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemContent {
    /// 所有系统块拼接后的文本（或普通字符串）。
    pub text: String,
    /// 来自最后一个携带 cache_control 的系统块。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_control: Option<CacheControl>,
}

/// 从普通字符串或文本块数组反序列化 `system`。
/// Anthropic API 同时接受 `"system": "text"` 与
/// `"system": [{"type": "text", "text": "...", "cache_control": {...}}]`。
fn deserialize_system_prompt<'de, D>(deserializer: D) -> Result<Option<SystemContent>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum SystemPrompt {
        Text(String),
        Blocks(Vec<SystemBlock>),
    }

    #[derive(Deserialize)]
    struct SystemBlock {
        text: String,
        #[serde(default)]
        cache_control: Option<CacheControl>,
    }

    let maybe: Option<SystemPrompt> = Option::deserialize(deserializer)?;
    Ok(maybe.map(|sp| match sp {
        SystemPrompt::Text(s) => SystemContent {
            text: s,
            cache_control: None,
        },
        SystemPrompt::Blocks(blocks) => {
            let cache_control = blocks.iter().rev().find_map(|b| b.cache_control.clone());
            let text = blocks
                .into_iter()
                .map(|b| b.text)
                .collect::<Vec<_>>()
                .join("\n");
            SystemContent {
                text,
                cache_control,
            }
        }
    }))
}
/// `POST /v1/messages` 的顶层请求体。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnthropicCreateMessageRequest {
    /// 要使用的模型（如 "claude-sonnet-4-20250514"）。
    pub model: String,

    /// 要生成的最大 token 数。
    pub max_tokens: u32,

    /// 对话消息。
    pub messages: Vec<AnthropicMessage>,

    /// 可选系统提示（字符串或 `{"type":"text","text":"..."}` 块数组）。
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_system_prompt"
    )]
    pub system: Option<SystemContent>,

    /// 采样温度（0.0 - 1.0）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,

    /// 核采样参数。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,

    /// Top-K 采样参数。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_k: Option<u32>,

    /// 自定义 stop 序列。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_sequences: Option<Vec<String>>,

    /// 是否流式返回响应。
    #[serde(default)]
    pub stream: bool,

    /// 可选元数据（如 user_id）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,

    /// 模型可调用的工具。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<AnthropicTool>>,

    /// 模型应如何选择要调用的工具。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<AnthropicToolChoice>,

    /// 用于自动提示前缀缓存的顶层缓存控制。
    /// 存在时，系统缓存直至最后一个可缓存块为止的所有内容。
    /// 与 Anthropic Messages API 的自动缓存模式一致。
    /// 参见：https://docs.anthropic.com/en/docs/build-with-claude/prompt-caching#automatic-caching
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_control: Option<CacheControl>,

    /// 扩展思考配置。启用时，模型会在最终响应之前产生包含其内部推理的
    /// `thinking` 内容块。`budget_tokens` 字段控制模型可用于思考的 token 数
    /// （必须 >= 1024 且 < max_tokens）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking: Option<ThinkingConfig>,

    /// 服务等级选择：`"auto"` 或 `"standard_only"`。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<String>,

    /// 有状态沙箱会话的容器标识符。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container: Option<String>,

    /// 输出配置：努力程度与可选 JSON schema 格式。
    /// `effort` 可为 `"low"`、`"medium"`、`"high"` 或 `"max"`。
    /// `format` 指定结构化 JSON 输出约束。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_config: Option<serde_json::Value>,
}

// === SECTION: 消息与内容块 ===

/// 请求的扩展思考配置。
///
/// 当 `type` 为 `"enabled"` 时，模型会产生包含其内部推理的 `thinking`
/// 内容块。`budget_tokens` 控制可用于思考的最大 token 数（最少 1024，
/// 且必须小于 `max_tokens`）。当 `type` 为 `"disabled"` 时不产生思考块。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThinkingConfig {
    /// `"enabled"` 或 `"disabled"`。
    #[serde(rename = "type")]
    pub thinking_type: String,
    /// 内部推理的最大 token 数。仅在 type 为 "enabled" 时相关。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub budget_tokens: Option<u32>,
}

/// 对话中的单条消息。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnthropicMessage {
    pub role: AnthropicRole,
    #[serde(flatten)]
    pub content: AnthropicMessageContent,
}

/// 消息发送者的角色。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AnthropicRole {
    User,
    Assistant,
}

/// 消息内容 —— 要么是普通字符串，要么是内容块数组。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AnthropicMessageContent {
    /// 纯文本内容。
    Text { content: String },
    /// 结构化内容块数组。
    Blocks { content: Vec<AnthropicContentBlock> },
}

/// 消息中的单个内容块。
///
/// 使用自定义反序列化器，以便未知块类型（如 `citations`、
/// `server_tool_use`、`redacted_thinking`）被捕获为 `Other(Value)`，而非导致
/// 硬性反序列化失败。这很重要，因为 Claude Code 可能发送我们尚未处理的块类型。
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum AnthropicContentBlock {
    /// 文本内容块。可选包含 `citations` —— 支撑该文本内容的源文档引用。
    /// 引用由模型在提供文档/PDF 内容且启用引用模式时生成。
    #[serde(rename = "text")]
    Text {
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        citations: Option<Vec<serde_json::Value>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cache_control: Option<CacheControl>,
    },
    /// 图像内容块。
    #[serde(rename = "image")]
    Image { source: AnthropicImageSource },
    /// 来自 assistant 的工具调用请求。
    #[serde(rename = "tool_use")]
    ToolUse {
        id: String,
        name: String,
        input: serde_json::Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cache_control: Option<CacheControl>,
    },
    /// 来自用户的工具结果。
    #[serde(rename = "tool_result")]
    ToolResult {
        tool_use_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content: Option<ToolResultContent>,
        #[serde(skip_serializing_if = "Option::is_none")]
        is_error: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cache_control: Option<CacheControl>,
    },
    /// 来自 assistant 的思考内容块（扩展思考 / 推理）。
    #[serde(rename = "thinking")]
    Thinking {
        thinking: String,
        signature: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cache_control: Option<CacheControl>,
    },
    /// 来自 assistant 的脱敏思考块。包含加密的推理数据，对客户端不透明，但必须
    /// 在多轮对话中原样传回，以便模型维持其思维链。
    #[serde(rename = "redacted_thinking")]
    RedactedThinking { data: String },
    /// 服务端发起的工具使用块。表示由 API 在服务端执行的工具调用
    /// （如 web 搜索）。客户端通过对应的 `web_search_tool_result` 或类似块收到结果。
    #[serde(rename = "server_tool_use")]
    ServerToolUse {
        id: String,
        name: String,
        #[serde(default)]
        input: serde_json::Value,
    },
    /// 来自服务端发起的工具的结果（如 web 搜索结果）。
    /// 包含服务端工具执行返回的结构化内容。
    #[serde(rename = "web_search_tool_result")]
    WebSearchToolResult {
        tool_use_id: String,
        #[serde(default)]
        content: serde_json::Value,
    },
    /// 未识别块类型的兑底项。保留完整 JSON 值，使新的 Anthropic 特性不会破坏
    /// 端点，且可被原样回传或检查。
    #[serde(untagged)]
    Other(serde_json::Value),
}

/// `tool_result` 块的内容 —— 要么是普通字符串，要么是内容块数组
/// （Anthropic API 两者皆接受）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ToolResultContent {
    Text(String),
    Blocks(Vec<ToolResultContentBlock>),
}

impl ToolResultContent {
    /// 提取文本内容，必要时拼接数组块。
    pub fn into_text(self) -> String {
        match self {
            ToolResultContent::Text(s) => s,
            ToolResultContent::Blocks(blocks) => blocks
                .into_iter()
                .filter_map(|b| match b {
                    ToolResultContentBlock::Text { text } => Some(text),
                    ToolResultContentBlock::Other(_) => None,
                })
                .collect::<Vec<_>>()
                .join(""),
        }
    }
}

/// `tool_result.content` 数组中的内容块。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ToolResultContentBlock {
    Text {
        text: String,
    },
    /// 工具结果中非文本块（图像等）的兑底项。
    Other(serde_json::Value),
}

/// `AnthropicContentBlock` 的自定义反序列化器，优雅处理未知类型。
/// 由于 serde 的 `#[serde(other)]` 在内部标记枚举上不受支持，我们先反序列化
/// 为 `Value` 再手动分发。
impl<'de> Deserialize<'de> for AnthropicContentBlock {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        let block_type = value
            .get("type")
            .and_then(|t| t.as_str())
            .unwrap_or("")
            .to_string();

        match block_type.as_str() {
            "text" => {
                let text = value
                    .get("text")
                    .and_then(|t| t.as_str())
                    .ok_or_else(|| serde::de::Error::missing_field("text"))?
                    .to_string();
                let citations: Option<Vec<serde_json::Value>> = value
                    .get("citations")
                    .cloned()
                    .and_then(|v| serde_json::from_value(v).ok());
                let cache_control: Option<CacheControl> = value
                    .get("cache_control")
                    .cloned()
                    .and_then(|v| serde_json::from_value(v).ok());
                Ok(AnthropicContentBlock::Text {
                    text,
                    citations,
                    cache_control,
                })
            }
            "image" => {
                let source: AnthropicImageSource =
                    serde_json::from_value(value.get("source").cloned().unwrap_or_default())
                        .map_err(serde::de::Error::custom)?;
                Ok(AnthropicContentBlock::Image { source })
            }
            "tool_use" => {
                let id = value
                    .get("id")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| serde::de::Error::missing_field("id"))?
                    .to_string();
                let name = value
                    .get("name")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| serde::de::Error::missing_field("name"))?
                    .to_string();
                let input = value.get("input").cloned().unwrap_or(serde_json::json!({}));
                let cache_control: Option<CacheControl> = value
                    .get("cache_control")
                    .cloned()
                    .and_then(|v| serde_json::from_value(v).ok());
                Ok(AnthropicContentBlock::ToolUse {
                    id,
                    name,
                    input,
                    cache_control,
                })
            }
            "tool_result" => {
                let tool_use_id = value
                    .get("tool_use_id")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| serde::de::Error::missing_field("tool_use_id"))?
                    .to_string();
                let content: Option<ToolResultContent> = value
                    .get("content")
                    .cloned()
                    .and_then(|v| serde_json::from_value(v).ok());
                let is_error = value.get("is_error").and_then(|v| v.as_bool());
                let cache_control: Option<CacheControl> = value
                    .get("cache_control")
                    .cloned()
                    .and_then(|v| serde_json::from_value(v).ok());
                Ok(AnthropicContentBlock::ToolResult {
                    tool_use_id,
                    content,
                    is_error,
                    cache_control,
                })
            }
            "thinking" => {
                let thinking = value
                    .get("thinking")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| serde::de::Error::missing_field("thinking"))?
                    .to_string();
                let signature = value
                    .get("signature")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| serde::de::Error::missing_field("signature"))?
                    .to_string();
                let cache_control: Option<CacheControl> = value
                    .get("cache_control")
                    .cloned()
                    .and_then(|v| serde_json::from_value(v).ok());
                Ok(AnthropicContentBlock::Thinking {
                    thinking,
                    signature,
                    cache_control,
                })
            }
            "redacted_thinking" => {
                let data = value
                    .get("data")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| serde::de::Error::missing_field("data"))?
                    .to_string();
                Ok(AnthropicContentBlock::RedactedThinking { data })
            }
            "server_tool_use" => {
                let id = value
                    .get("id")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| serde::de::Error::missing_field("id"))?
                    .to_string();
                let name = value
                    .get("name")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| serde::de::Error::missing_field("name"))?
                    .to_string();
                let input = value.get("input").cloned().unwrap_or(serde_json::json!({}));
                Ok(AnthropicContentBlock::ServerToolUse { id, name, input })
            }
            "web_search_tool_result" => {
                let tool_use_id = value
                    .get("tool_use_id")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| serde::de::Error::missing_field("tool_use_id"))?
                    .to_string();
                let content = value
                    .get("content")
                    .cloned()
                    .unwrap_or(serde_json::json!([]));
                Ok(AnthropicContentBlock::WebSearchToolResult {
                    tool_use_id,
                    content,
                })
            }
            other => {
                tracing::debug!(
                    "Unrecognized Anthropic content block type '{}', preserving as Other",
                    other
                );
                Ok(AnthropicContentBlock::Other(value))
            }
        }
    }
}

/// 图像内容块的图像源。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnthropicImageSource {
    #[serde(rename = "type")]
    pub source_type: String,
    pub media_type: String,
    pub data: String,
}

// === SECTION: 工具定义与选择 ===

/// 工具定义。
///
/// 客户端工具（custom）需要 `name` + `input_schema`。服务端工具
/// （web_search、bash、text_editor、code_execution 等）通过其 `type` 字段
/// 区分（如 `"web_search_20260209"`），且可能没有 `input_schema`。我们将
/// `name` 之外的所有字段保持为可选，使两类工具都能成功反序列化并透传到后端。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnthropicTool {
    /// 工具名（客户端工具必需，服务端工具上同样存在）。
    pub name: String,
    /// 工具类型判别符。客户端工具使用 `"custom"`（或省略）。
    /// 服务端工具使用带版本号的类型，如 `"web_search_20260209"`。
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub tool_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// 工具输入的 JSON Schema。客户端工具必需，服务端工具上缺省
    /// （服务端工具在服务端自行定义输入结构）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_schema: Option<serde_json::Value>,
    /// 此工具定义上的缓存控制断点。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_control: Option<CacheControl>,
}

/// 工具选择规格。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AnthropicToolChoice {
    /// 具名工具：`{type: "tool", name: "..."}`
    /// 必须列在 Simple 之前，使 serde 先尝试更严格的形态。
    Named(AnthropicToolChoiceNamed),
    /// 简单模式："auto"、"any" 或 "none"。
    Simple(AnthropicToolChoiceSimple),
}

/// 简单工具选择模式。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnthropicToolChoiceSimple {
    #[serde(rename = "type")]
    pub choice_type: AnthropicToolChoiceMode,
    /// 为 true 时，模型一次只调用一个工具，而非在单次响应中可能发起多个工具调用。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disable_parallel_tool_use: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AnthropicToolChoiceMode {
    Auto,
    Any,
    None,
    Tool,
}

/// 具名工具选择。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnthropicToolChoiceNamed {
    #[serde(rename = "type")]
    pub choice_type: AnthropicToolChoiceMode,
    pub name: String,
    /// 为 true 时，模型一次只调用一个工具，而非在单次响应中可能发起多个工具调用。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disable_parallel_tool_use: Option<bool>,
}

// === SECTION: 响应类型 ===

/// `POST /v1/messages` 的响应体（非流式）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnthropicMessageResponse {
    pub id: String,
    #[serde(rename = "type")]
    pub object_type: String,
    pub role: String,
    pub content: Vec<AnthropicResponseContentBlock>,
    pub model: String,
    pub stop_reason: Option<AnthropicStopReason>,
    pub stop_sequence: Option<String>,
    pub usage: AnthropicUsage,
}

/// 响应中的内容块。
///
/// Anthropic API 最多返回 12 种不同的块类型。我们显式建模常见类型，
/// 其余以 `Other` 兜底，使代理可以无损转发。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AnthropicResponseContentBlock {
    #[serde(rename = "thinking")]
    Thinking { thinking: String, signature: String },
    #[serde(rename = "text")]
    Text {
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        citations: Option<Vec<serde_json::Value>>,
    },
    #[serde(rename = "tool_use")]
    ToolUse {
        id: String,
        name: String,
        input: serde_json::Value,
    },
    #[serde(rename = "redacted_thinking")]
    RedactedThinking { data: String },
    #[serde(rename = "server_tool_use")]
    ServerToolUse {
        id: String,
        name: String,
        #[serde(default)]
        input: serde_json::Value,
    },
    #[serde(rename = "web_search_tool_result")]
    WebSearchToolResult {
        tool_use_id: String,
        #[serde(default)]
        content: serde_json::Value,
    },
    /// 新型/少见块类型（web_fetch_tool_result、code_execution_tool_result、
    /// container_upload 等）的兜底项，使代理可无损序列化回去。
    #[serde(untagged)]
    Other(serde_json::Value),
}

/// Token 用量信息。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AnthropicUsage {
    pub input_tokens: u32,
    pub output_tokens: u32,
    /// 创建新缓存条目所用的输入 token 数。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_creation_input_tokens: Option<u32>,
    /// 从提示缓存读取的输入 token 数（前缀缓存命中）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_read_input_tokens: Option<u32>,
}

/// 模型停止生成的原因。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AnthropicStopReason {
    EndTurn,
    MaxTokens,
    StopSequence,
    ToolUse,
    /// 模型在 agentic 循环中暂停以让出控制权，打算在后续轮次继续。
    /// 与扩展思考 / 工具使用配合使用。
    PauseTurn,
    /// 模型拒绝生成内容（安全拒绝）。
    Refusal,
}

// === SECTION: 流式事件 ===

/// Anthropic 流式 API 的 SSE 事件类型。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AnthropicStreamEvent {
    #[serde(rename = "message_start")]
    MessageStart { message: AnthropicMessageResponse },

    #[serde(rename = "content_block_start")]
    ContentBlockStart {
        index: u32,
        content_block: AnthropicResponseContentBlock,
    },

    #[serde(rename = "content_block_delta")]
    ContentBlockDelta { index: u32, delta: AnthropicDelta },

    #[serde(rename = "content_block_stop")]
    ContentBlockStop { index: u32 },

    #[serde(rename = "message_delta")]
    MessageDelta {
        delta: AnthropicMessageDeltaBody,
        usage: AnthropicUsage,
    },

    #[serde(rename = "message_stop")]
    MessageStop {},

    #[serde(rename = "ping")]
    Ping {},

    #[serde(rename = "error")]
    Error { error: AnthropicErrorBody },
}

/// 流式 content_block_delta 事件中的增量内容。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AnthropicDelta {
    #[serde(rename = "thinking_delta")]
    ThinkingDelta { thinking: String },
    #[serde(rename = "text_delta")]
    TextDelta { text: String },
    #[serde(rename = "input_json_delta")]
    InputJsonDelta { partial_json: String },
    /// 思考块的增量签名（在末尾发送）。
    #[serde(rename = "signature_delta")]
    SignatureDelta { signature: String },
    /// 附加到文本块的增量引用。
    #[serde(rename = "citations_delta")]
    CitationsDelta { citation: serde_json::Value },
}

/// message_delta 事件中的增量主体。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnthropicMessageDeltaBody {
    pub stop_reason: Option<AnthropicStopReason>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_sequence: Option<String>,
}

// === SECTION: 错误类型 ===

/// Anthropic API 错误响应包装。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnthropicErrorResponse {
    #[serde(rename = "type")]
    pub object_type: String,
    pub error: AnthropicErrorBody,
}

/// 错误响应中的错误主体。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnthropicErrorBody {
    #[serde(rename = "type")]
    pub error_type: String,
    pub message: String,
}

impl AnthropicErrorResponse {
    /// 创建 `invalid_request_error` 响应。
    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self {
            object_type: "error".to_string(),
            error: AnthropicErrorBody {
                error_type: "invalid_request_error".to_string(),
                message: message.into(),
            },
        }
    }

    /// 创建 `api_error`（内部服务器错误）响应。
    pub fn api_error(message: impl Into<String>) -> Self {
        Self {
            object_type: "error".to_string(),
            error: AnthropicErrorBody {
                error_type: "api_error".to_string(),
                message: message.into(),
            },
        }
    }

    /// 创建 `not_found_error` 响应。
    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            object_type: "error".to_string(),
            error: AnthropicErrorBody {
                error_type: "not_found_error".to_string(),
                message: message.into(),
            },
        }
    }
}
// === SECTION: count-tokens 类型 ===

/// `POST /v1/messages/count_tokens` 的请求体。
#[derive(Debug, Clone, Deserialize)]
pub struct AnthropicCountTokensRequest {
    pub model: String,
    pub messages: Vec<AnthropicMessage>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_system_prompt"
    )]
    pub system: Option<SystemContent>,
    #[serde(default)]
    pub tools: Option<Vec<AnthropicTool>>,
}

/// `POST /v1/messages/count_tokens` 的响应体。
#[derive(Debug, Clone, Serialize)]
pub struct AnthropicCountTokensResponse {
    pub input_tokens: u32,
}

impl AnthropicCountTokensRequest {
    /// 使用 `len/3` 启发式估算输入 token 数。
    pub fn estimate_tokens(&self) -> u32 {
        let mut total_len: usize = 0;

        if let Some(system) = &self.system {
            total_len += system.text.len();
        }

        for msg in &self.messages {
            // 计入角色
            total_len += match msg.role {
                AnthropicRole::User => 4,
                AnthropicRole::Assistant => 9,
            };
            // 计入内容
            match &msg.content {
                AnthropicMessageContent::Text { content } => total_len += content.len(),
                AnthropicMessageContent::Blocks { content } => {
                    for block in content {
                        total_len += estimate_block_len(block);
                    }
                }
            }
        }

        if let Some(tools) = &self.tools {
            for tool in tools {
                total_len += tool.name.len();
                if let Some(desc) = &tool.description {
                    total_len += desc.len();
                }
                if let Some(schema) = &tool.input_schema {
                    total_len += schema.to_string().len();
                }
            }
        }

        let tokens = total_len / 3;
        if tokens == 0 && total_len > 0 {
            1
        } else {
            tokens as u32
        }
    }
}

fn estimate_block_len(block: &AnthropicContentBlock) -> usize {
    match block {
        AnthropicContentBlock::Text { text, .. } => text.len(),
        AnthropicContentBlock::ToolUse { name, input, .. } => name.len() + input.to_string().len(),
        AnthropicContentBlock::ToolResult { content, .. } => content
            .as_ref()
            .map(|c| match c {
                ToolResultContent::Text(s) => s.len(),
                ToolResultContent::Blocks(blocks) => blocks
                    .iter()
                    .map(|b| match b {
                        ToolResultContentBlock::Text { text } => text.len(),
                        ToolResultContentBlock::Other(v) => v.to_string().len(),
                    })
                    .sum(),
            })
            .unwrap_or(0),
        AnthropicContentBlock::Thinking { thinking, .. } => thinking.len(),
        AnthropicContentBlock::RedactedThinking { data, .. } => data.len(),
        AnthropicContentBlock::ServerToolUse { name, input, .. } => {
            name.len() + input.to_string().len()
        }
        AnthropicContentBlock::WebSearchToolResult { content, .. } => content.to_string().len(),
        AnthropicContentBlock::Image { .. } => 256, // 图像元数据的粗略估算
        AnthropicContentBlock::Other(v) => v.to_string().len(),
    }
}
