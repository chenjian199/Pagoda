// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
// Pagoda 自有 Responses API 输入侧的类型链。其余全部（输出侧类型、
// 流式事件、单个工具调用载荷等）均以上游 async-openai 为来源。
//
// 之所以自有输入链，是因为上游把一些字段标记为必填，而现实中的
// 客户端（OpenAI Agents SDK、Codex 等）在将上一轮 assistant 输出回传作为输入时
// 经常省略它们：
//   - `OutputMessage.id` / `.status` —— 回显之前输出时省略
//   - `OutputTextContent.annotations` —— 片段未携带注释时省略
// 上游放宽这些限制的速度较慢（参见姊妹问题 64bit/async-openai#535 对
// `ReasoningItem.id` 的修复，截至撰写时仍未合并）；而 OpenAI 自托管 API
// 无论如何都在输入侧接受这些放宽后的形态。
//
// 这与 `crate::types::chat` 中的模式一致：Dynamo 自有需要扩展或放宽的
// 请求类型，而原样重新导出上游类型库的其余部分。
//
// 命名：放宽后的 assistant 输入消息叫 `InputOutputMessage`（其内容片段则为
// `InputOutputMessageContent` / `InputOutputTextContent`），以避免与上游的
// `OutputMessage` 冲突 —— 后者仍是 *输出侧* 响应构造（`OutputItem`、
// `Response.output`）的权威类型。`MessageItem`、`Item`、`InputItem`、
// `InputParam`、`CreateResponse` 均为输入专用，遮蔽上游同名类型而不冲突。

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

// === SECTION: 上游类型重新导出与别名 ===

// 重新导出全部上游 response 类型（如 ResponseUsage、工具调用 item 类型、
// 流式事件等共享结构）。下方我们自有的类型在不产生双侧冲突的前提下
// 遮蔽其上游同名类型。
pub use async_openai::types::responses::*;

// 以显式别名重新导出上游被遮蔽前的 `InputContent`。
// 因为 `FunctionCallOutput::Content` 与 `EasyInputContent::ContentList` 是非自有的
// 上游类型，它们内联携带了上游原始的 `InputContent`，因此下游调用方偶尔
// 需要在本模块下方定义的 Dynamo 自有遮蔽类型之外同时引用它。
pub use async_openai::types::responses::InputContent as UpstreamInputContent;

// 从父模块重新导出以保持向后兼容。
pub use crate::types::ImageDetail;
pub use crate::types::ReasoningEffort;
pub use crate::types::ResponseFormatJsonSchema;

// 供 Dynamo 调用方代码迁移用的向后兼容类型别名。
pub type Input = InputParam;
pub type PromptConfig = Prompt;
pub type TextConfig = ResponseTextParam;
pub type TextResponseFormat = TextResponseFormatConfiguration;

/// 响应事件流。
pub type ResponseStream = std::pin::Pin<
    Box<dyn futures::Stream<Item = Result<ResponseStreamEvent, crate::error::OpenAIError>> + Send>,
>;

/// 上游 `Response` 上那些 OpenResponses 规范要求为 `T | null`，但 async-openai 声明为
/// `Option<T>` 并加 `skip_serializing_if = Option::is_none` 的字段 —— 这意味着 `None`
/// 会从 wire 形态中消失，而规范期望明确的 `null`。
///
/// 放在这里（紧靠上游 `Response` 重新导出）而非
/// `lib/llm/src/protocols/openai/responses/mod.rs`，是为了当上游 `Response`
/// 新增可为空的必填字段时，编辑本模块的评审者能直接看到这份权威清单。
/// 请保持按字母顺序排列；条目必须与 `Response` 上的 serde 字段名完全一致。
///
/// 任何我们在构造响应时无条件自行填充的字段（如 `metadata`、
/// `parallel_tool_calls`、`temperature`、`text`、`tool_choice`、`tools`、`top_p`、
/// `top_logprobs`、`truncation`、`service_tier`、`background`）均故意不列入 ——
/// 它们始终出现在 wire 上，列在这里反而是噪声。
pub const SPEC_NULLABLE_REQUIRED_RESPONSE_FIELDS: &[&str] = &[
    "billing",
    "completed_at",
    "conversation",
    "error",
    "incomplete_details",
    "instructions",
    "max_output_tokens",
    "max_tool_calls",
    "previous_response_id",
    "prompt",
    "prompt_cache_key",
    "prompt_cache_retention",
    "reasoning",
    "safety_identifier",
    "usage",
];

// === SECTION: 输入侧 assistant 消息（相对上游 OutputMessage 放宽） ===

/// 将 `null` 或缺失字段反序列化为默认的空 `Vec`。普通的
/// `#[serde(default)]` 仅在字段缺失时生效；显式 `null` 否则会让
/// `Vec::deserialize` 失败。已观察到客户端（尤其某些 Agents SDK 变体）
/// 发送 `"annotations": null`，故将缺失与显式 null 同等处理。
fn deserialize_null_as_empty_vec<'de, T, D>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    T: Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    Option::<Vec<T>>::deserialize(deserializer).map(Option::unwrap_or_default)
}

/// 将 `null` 或缺失字段反序列化为 `T::default()`。为
/// `deserialize_null_as_empty_vec` 的标量对应物 —— 普通的 `#[serde(default)]`
/// 拒绝显式 `null`，因为 serde 会试图将 null 反序列化为 `T` 而失败。现实
/// 客户端会对未设置的类枚举字段发出 `null`（如 OpenAI Agents SDK 在
/// `input_image` 片段上发送 `"detail": null`）。
fn deserialize_null_as_default<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    T: Deserialize<'de> + Default,
    D: serde::Deserializer<'de>,
{
    Option::<T>::deserialize(deserializer).map(Option::unwrap_or_default)
}

/// 上游 `OutputTextContent` 面向输入侧内容的放宽对应物。
/// `annotations` 同时容忍缺失与显式 `null`；上游要求其为存在的非空数组。
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct InputOutputTextContent {
    #[serde(default, deserialize_with = "deserialize_null_as_empty_vec")]
    pub annotations: Vec<Annotation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logprobs: Option<Vec<LogProb>>,
    pub text: String,
}

/// 作为输入呈现的上一轮 assistant 消息的内容片段。
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InputOutputMessageContent {
    OutputText(InputOutputTextContent),
    Refusal(RefusalContent),
}

/// 被回传作为下一轮输入的 assistant 消息。相比上游 `OutputMessage` 更加
/// 放宽：`id`、`status` 与 `content` 均为可选。某些客户端会发送不带任何
/// `content` 的裸 assistant 壳（`{"type":"message","role":"assistant"}`），通常
/// 发生在纯工具调用轮次；将缺失的 `content` 视为空 vec，与处理缺失的
/// `id`/`status` 方式相同。
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct InputOutputMessage {
    #[serde(default, deserialize_with = "deserialize_null_as_empty_vec")]
    pub content: Vec<InputOutputMessageContent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub role: AssistantRole,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<MessagePhase>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<OutputStatus>,
}

// === SECTION: 输入侧图像 / 内容 / 消息（遮蔽上游，放宽形态） ===

/// 上游 `InputImageContent` 的放宽对应物。`detail` 在客户端省略时默认为
/// `ImageDetail::Auto` —— OpenAI 托管 API 与 OpenResponses 规范都接受这种形态，
/// 但上游的结构体把 `detail` 标记为必填。
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct InputImageContent {
    #[serde(default, deserialize_with = "deserialize_null_as_default")]
    pub detail: ImageDetail,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_url: Option<String>,
}

/// 输入消息的内容片段：文本、图像或文件。镜像上游 `InputContent`，
/// 但将 `InputImage` 路由到上方 Dynamo 自有的放宽型 `InputImageContent`。
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InputContent {
    InputText(InputTextContent),
    InputImage(InputImageContent),
    InputFile(InputFileContent),
}

/// 用户 / 系统 / 开发者输入消息。遮蔽上游 `InputMessage`，以便路由过
/// Dynamo 自有的 `InputContent` 链。
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub struct InputMessage {
    pub content: Vec<InputContent>,
    pub role: InputRole,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<OutputStatus>,
}

// === SECTION: 输入侧 Item / Message / InputItem / InputParam（遮蔽上游） ===

/// `Item` 内的消息 item。untagged，凭 `role` 字段区分：`Output` 变体要求
/// `role: "assistant"`（通过单变体枚举 `AssistantRole`），`Input` 要求 `role`
/// 为 `"user" | "system" | "developer"`（通过 `InputRole`）。携带未知 role
/// （如 `"tool"`）或缺失 `role` 的载荷会产生通用的 untagged 枚举错误 ——
/// 调用方应发送合法 role。若在此类型上看到 "data did not match any variant
/// of untagged enum" 失败，几乎总是 role 不匹配。
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(untagged)]
pub enum MessageItem {
    /// 被回传的上一轮 assistant 输出（role: assistant）。优先尝试 —— 其
    /// `role` 约束排除了 user/system/developer 输入。
    Output(InputOutputMessage),
    /// 用户 / 系统 / 开发者输入消息。
    Input(InputMessage),
}

/// 结构化输入/输出 item，由 `type` 区分。逐变体镜像上游 `Item`；
/// 仅 `Message` 使用 Dynamo 自有类型。
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Item {
    Message(MessageItem),
    FileSearchCall(FileSearchToolCall),
    ComputerCall(ComputerToolCall),
    ComputerCallOutput(ComputerCallOutputItemParam),
    WebSearchCall(WebSearchToolCall),
    FunctionCall(FunctionToolCall),
    FunctionCallOutput(FunctionCallOutputItemParam),
    ToolSearchCall(ToolSearchCallItemParam),
    ToolSearchOutput(ToolSearchOutputItemParam),
    Reasoning(ReasoningItem),
    Compaction(CompactionSummaryItemParam),
    ImageGenerationCall(ImageGenToolCall),
    CodeInterpreterCall(CodeInterpreterToolCall),
    LocalShellCall(LocalShellToolCall),
    LocalShellCallOutput(LocalShellToolCallOutput),
    ShellCall(FunctionShellCallItemParam),
    ShellCallOutput(FunctionShellCallOutputItemParam),
    ApplyPatchCall(ApplyPatchToolCallItemParam),
    ApplyPatchCallOutput(ApplyPatchToolCallOutputItemParam),
    McpListTools(MCPListTools),
    McpApprovalRequest(MCPApprovalRequest),
    McpApprovalResponse(MCPApprovalResponse),
    McpCall(MCPToolCall),
    CustomToolCallOutput(CustomToolCallOutput),
    CustomToolCall(CustomToolCall),
}

/// 单个输入 item。untagged，顺序重要（最具体者优先）。
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(untagged)]
pub enum InputItem {
    ItemReference(ItemReference),
    Item(Item),
    EasyMessage(EasyInputMessage),
}

/// `POST /v1/responses` 请求的输入。
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(untagged)]
pub enum InputParam {
    Text(String),
    Items(Vec<InputItem>),
}

impl Default for InputParam {
    fn default() -> Self {
        Self::Text(String::new())
    }
}

// === SECTION: CreateResponse（自有，使用 Dynamo 自有的 InputParam） ===

/// `POST /v1/responses` 的请求体。逐字段镜像上游 `CreateResponse`，但使用
/// Dynamo 自有的 `InputParam`（其传递地接受本模块头部描述的放宽输入形态）。
/// 其余字段均原样引用上游类型。
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub struct CreateResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub background: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conversation: Option<ConversationParam>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include: Option<Vec<IncludeEnum>>,
    pub input: InputParam,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tool_calls: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallel_tool_calls: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_response_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt: Option<Prompt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_cache_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_cache_retention: Option<PromptCacheRetention>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<Reasoning>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub safety_identifier: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<ServiceTier>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub store: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_options: Option<ResponseStreamOptions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<ResponseTextParam>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<ToolChoiceParam>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<Tool>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_logprobs: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncation: Option<Truncation>,
}

// === SECTION: 测试 ===
// 唯一的 `mod tests`，既覆盖输入侧放宽路径，也保留标准协议测试作为
// 回归基线。所有 JSON 测试数据与断言/panic 文本保持原样（可观察行为）。
#[cfg(test)]
mod tests {
    use super::*;

    /// ## 测试过程
    /// 反序列化缺失 id/status 的 assistant 消息。
    /// ## 意义
    /// 验证放宽后的 `InputOutputMessage` 接受缺失 id/status。
    #[test]
    fn relaxed_assistant_message_without_id_or_status() {
        let json = serde_json::json!({
            "type": "message",
            "role": "assistant",
            "content": [{"type": "output_text", "text": "hi"}]
        });
        let item: InputItem = serde_json::from_value(json).unwrap();
        match item {
            InputItem::Item(Item::Message(MessageItem::Output(out))) => {
                assert_eq!(out.role, AssistantRole::Assistant);
                assert!(out.id.is_none());
                assert!(out.status.is_none());
            }
            other => panic!("expected Item::Message(Output), got {other:?}"),
        }
    }

    /// ## 测试过程
    /// 反序列化缺失 detail 的 input_image。
    /// ## 意义
    /// 验证 detail 缺失时默认为 `ImageDetail::Auto`。
    #[test]
    fn input_image_without_detail_defaults_to_auto() {
        let json = serde_json::json!({
            "type": "input_image",
            "image_url": "https://example.com/cat.jpg"
        });
        let content: InputContent = serde_json::from_value(json).unwrap();
        match content {
            InputContent::InputImage(img) => assert_eq!(img.detail, ImageDetail::Auto),
            other => panic!("expected InputImage, got {other:?}"),
        }
    }

    /// ## 测试过程
    /// detail 显式为 null 时反序列化。
    /// ## 意义
    /// 验证显式 null 与缺失同等，默认为 `Auto`。
    #[test]
    fn input_image_with_explicit_null_detail_defaults_to_auto() {
        let json = serde_json::json!({
            "type": "input_image",
            "image_url": "https://example.com/cat.jpg",
            "detail": null
        });
        let content: InputContent = serde_json::from_value(json).unwrap();
        match content {
            InputContent::InputImage(img) => assert_eq!(img.detail, ImageDetail::Auto),
            other => panic!("expected InputImage, got {other:?}"),
        }
    }

    /// ## 测试过程
    /// 反序列化不带 content 字段的 assistant 消息。
    /// ## 意义
    /// 验证缺失 content 产生空 vec。
    #[test]
    fn assistant_message_without_content_field_deserializes() {
        // 裸 assistant 壳 —— 完全没有 `content` 字段。在真实的 Codex/Agents-SDK
        // 流量中出现于纯工具调用轮次。`content` 上的 `#[serde(default)]`
        // 必须接受省略并产生空 vec。
        let json = serde_json::json!({
            "type": "message",
            "role": "assistant"
        });
        let item: InputItem = serde_json::from_value(json).unwrap();
        match item {
            InputItem::Item(Item::Message(MessageItem::Output(out))) => {
                assert_eq!(out.role, AssistantRole::Assistant);
                assert!(out.content.is_empty());
                assert!(out.id.is_none());
                assert!(out.status.is_none());
            }
            other => panic!("expected Item::Message(Output), got {other:?}"),
        }
    }

    /// ## 测试过程
    /// content 显式为 null 时反序列化 assistant 消息。
    /// ## 意义
    /// 验证 content 也需 `deserialize_null_as_empty_vec` 以接受显式 null。
    #[test]
    fn assistant_message_with_explicit_null_content_deserializes() {
        // 与 `annotations: null` 情形一致：某些序列化器会为缺失字段输出 JSON
        // null 而非省略。`Vec::deserialize` 拒绝 null，故 `content` 也需
        // `deserialize_null_as_empty_vec`。
        let json = serde_json::json!({
            "type": "message",
            "role": "assistant",
            "content": null
        });
        let item: InputItem = serde_json::from_value(json).unwrap();
        match item {
            InputItem::Item(Item::Message(MessageItem::Output(out))) => {
                assert!(out.content.is_empty());
            }
            other => panic!("expected Item::Message(Output), got {other:?}"),
        }
    }

    /// ## 测试过程
    /// 反序列化 mcp_call item。
    /// ## 意义
    /// 防止 Item 变体相对上游漂移 —— MCP item 类型是后加的。
    #[test]
    fn mcp_call_item_deserializes() {
        // 防止 Item 变体相对上游漂移 —— MCP item 类型是在初始自有 `Item`
        // 链落地之后才新增的。
        let json = serde_json::json!({
            "type": "mcp_call",
            "id": "mcp_1",
            "server_label": "srv",
            "name": "t",
            "arguments": "{}"
        });
        let item: InputItem = serde_json::from_value(json).unwrap();
        assert!(matches!(item, InputItem::Item(Item::McpCall(_))));
    }

    /// ## 测试过程
    /// 反序列化同时携带 id/status/annotations 的严格形态消息。
    /// ## 意义
    /// 验证放宽后仍兼容上游完整形态。
    #[test]
    fn strict_assistant_message_still_deserializes() {
        let json = serde_json::json!({
            "type": "message",
            "role": "assistant",
            "id": "msg_1",
            "status": "completed",
            "content": [{"type": "output_text", "text": "hi", "annotations": []}]
        });
        let item: InputItem = serde_json::from_value(json).unwrap();
        match item {
            InputItem::Item(Item::Message(MessageItem::Output(out))) => {
                assert_eq!(out.id.as_deref(), Some("msg_1"));
                assert_eq!(out.status, Some(OutputStatus::Completed));
            }
            other => panic!("expected Item::Message(Output), got {other:?}"),
        }
    }

    /// ## 测试过程
    /// 反序列化 role: user 的消息。
    /// ## 意义
    /// 验证 user role 路由到 `MessageItem::Input` 变体。
    #[test]
    fn user_message_routes_to_input_variant() {
        let json = serde_json::json!({
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "hi"}]
        });
        let item: InputItem = serde_json::from_value(json).unwrap();
        assert!(matches!(
            item,
            InputItem::Item(Item::Message(MessageItem::Input(_)))
        ));
    }

    /// ## 测试过程
    /// 反序列化 function_call item。
    /// ## 意义
    /// 验证上游 function_call 变体仍可正常反序列化。
    #[test]
    fn function_call_item_still_deserializes() {
        let json = serde_json::json!({
            "type": "function_call",
            "call_id": "c",
            "name": "f",
            "arguments": "{}"
        });
        let item: InputItem = serde_json::from_value(json).unwrap();
        assert!(matches!(item, InputItem::Item(Item::FunctionCall(_))));
    }

    /// ## 测试过程
    /// content 为字符串的简化消息。
    /// ## 意义
    /// 验证字符串 content 路由到 `EasyMessage` 变体。
    #[test]
    fn easy_message_string_content_routes_to_easymessage() {
        let json = serde_json::json!({"role": "assistant", "content": "x"});
        let item: InputItem = serde_json::from_value(json).unwrap();
        assert!(matches!(item, InputItem::EasyMessage(_)));
    }

    /// ## 测试过程
    /// 反序列化缺失 annotations 的 output_text。
    /// ## 意义
    /// 验证缺失 annotations 默认为空。
    #[test]
    fn output_text_without_annotations_defaults_empty() {
        let json = serde_json::json!({"type": "output_text", "text": "hi"});
        let part: InputOutputMessageContent = serde_json::from_value(json).unwrap();
        match part {
            InputOutputMessageContent::OutputText(t) => {
                assert!(t.annotations.is_empty());
            }
            _ => panic!("expected OutputText"),
        }
    }

    /// ## 测试过程
    /// annotations 显式为 null 时反序列化。
    /// ## 意义
    /// 验证显式 null 与缺失同等，产生空数组。
    #[test]
    fn output_text_with_explicit_null_annotations_deserializes_as_empty() {
        // 某些客户端将缺失字段序列化为 JSON null 而非省略。`Vec::deserialize`
        // 会拒绝 null；自定义反序列化器将显式 null 与缺失字段同等处理。
        let json = serde_json::json!({"type": "output_text", "text": "hi", "annotations": null});
        let part: InputOutputMessageContent = serde_json::from_value(json).unwrap();
        match part {
            InputOutputMessageContent::OutputText(t) => {
                assert!(t.annotations.is_empty());
            }
            _ => panic!("expected OutputText"),
        }
    }

    /// ## 测试过程
    /// id/status 显式为 null 时反序列化。
    /// ## 意义
    /// 验证 `Option<T>` 原生接受 null，锁定该行为防止意外回归。
    #[test]
    fn assistant_message_with_explicit_null_id_and_status_deserializes() {
        // `Option<T>` 原生将 null 接受为 `None`，因此这些显式 null 字段无需
        // 自定义反序列化器即可通过。本测试锁定该行为，防止意外回归
        // （如有人把字段类型从 `Option<_>` 改掉）。
        let json = serde_json::json!({
            "type": "message",
            "role": "assistant",
            "id": null,
            "status": null,
            "content": [{"type": "output_text", "text": "hi", "annotations": null}]
        });
        let item: InputItem = serde_json::from_value(json).unwrap();
        match item {
            InputItem::Item(Item::Message(MessageItem::Output(out))) => {
                assert!(out.id.is_none());
                assert!(out.status.is_none());
                assert_eq!(out.content.len(), 1);
            }
            other => panic!("expected Item::Message(Output), got {other:?}"),
        }
    }

    /// ## 测试过程
    /// 反序列化含多种 item 且携带放宽输入的 CreateResponse。
    /// ## 意义
    /// 验证 CreateResponse 端到端接受混合 item 并正确路由各变体。
    #[test]
    fn create_response_roundtrip_with_relaxed_input() {
        let body = serde_json::json!({
            "model": "m",
            "input": [
                {"type": "message", "role": "user", "content": [
                    {"type": "input_text", "text": "hi"}
                ]},
                {"type": "function_call", "call_id": "c", "name": "f", "arguments": "{}"},
                {"type": "message", "role": "assistant", "content": [
                    {"type": "output_text", "text": "\n\n"}
                ]},
                {"type": "function_call_output", "call_id": "c", "output": "x"}
            ]
        });

        let req: CreateResponse = serde_json::from_value(body).unwrap();
        let items = match &req.input {
            InputParam::Items(items) => items,
            _ => panic!("expected Items"),
        };
        assert_eq!(items.len(), 4);
        assert!(matches!(
            items[2],
            InputItem::Item(Item::Message(MessageItem::Output(_)))
        ));
    }
}
