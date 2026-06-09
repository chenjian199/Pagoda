// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # tool_calling::config
//!
//! ## 设计意图
//! 集中描述各类模型工具调用格式所需的解析配置：JSON、XML、DSML、GLM-4.7、Kimi-K2、Gemma4。
//! 每种格式有独立的配置结构与默认值，再由顶层 [`ParserConfig`] 枚举统一承载，
//! 并由 [`ToolCallConfig`] 提供面向具体模型家族的预设工厂方法。
//!
//! ## 外部契约
//! - 各配置结构的字段名、类型、serde（`default` / `alias` / `tag` / `rename_all`）规则保持不变。
//! - [`ParserConfig`] 以内部标签 `type`（snake_case）序列化。
//! - 预设工厂方法（`hermes` / `mistral` / `deepseek_v3` 等）的名称、签名与产出配置保持不变。

use super::json::JsonParserType;

// === SECTION: JSON 解析配置 ===

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct JsonParserConfig {
    /// 单个工具调用的起始 token（如 `<TOOLCALL>`）
    pub tool_call_start_tokens: Vec<String>,
    /// 单个工具调用的结束 token（如 `</TOOLCALL>`）
    pub tool_call_end_tokens: Vec<String>,
    /// 函数名与参数之间的分隔 token
    /// （如 DeepSeek v3.1 的 `"<｜tool▁sep｜>"`）
    /// 某些模型使用它分隔函数名与参数
    pub tool_call_separator_tokens: Vec<String>,
    /// 工具调用中函数名的 key
    /// 如 `{"name": "function", "arguments": {...}}` 则为 `"name"`
    pub function_name_keys: Vec<String>,
    /// 工具调用中参数的 key
    /// 如 `{"name": "function", "arguments": {...}}` 则为 `"arguments"`
    pub arguments_keys: Vec<String>,

    /// JSON 解析器类型
    #[serde(default)]
    pub parser_type: JsonParserType,

    /// 将输入当作裸 JSON（`{...}` 对象或 `[...]` 数组）解析，无外层包裹标记。
    /// 适用于后端仅发出原始 JSON 形态的引导解码路径。
    /// 为 true 时忽略 `tool_call_start_tokens` / `tool_call_end_tokens`。
    #[serde(default)]
    pub bare_json_mode: bool,

    /// 允许在外层结束 token 缺失时恢复（max_tokens / EOS 截断场景）。
    /// 流式 jail 必须保持 `false`——否则解析器在结束 token 实际到达前就宣称"工具调用完整"，
    /// 导致 `should_exit_jail_early` 过早触发。
    /// 收尾 / 聚合路径设为 `true`，使真实但未闭合的调用不被静默丢弃。
    #[serde(default)]
    pub allow_eof_recovery: bool,
}

impl Default for JsonParserConfig {
    fn default() -> Self {
        // 默认走 nemotron/llama 双起始 token 形态：`<TOOLCALL>` 与 `<|python_tag|>`。
        Self {
            tool_call_start_tokens: vec!["<TOOLCALL>".to_string(), "<|python_tag|>".to_string()],
            tool_call_end_tokens: vec!["</TOOLCALL>".to_string(), "".to_string()],
            tool_call_separator_tokens: vec![],
            function_name_keys: vec!["name".to_string()],
            arguments_keys: vec!["arguments".to_string(), "parameters".to_string()],
            parser_type: JsonParserType::Basic,
            bare_json_mode: false,
            allow_eof_recovery: false,
        }
    }
}

// === SECTION: XML 解析配置 ===

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct XmlParserConfig {
    /// 单个工具调用的起始 token（如 `"<tool_call>"`）
    pub tool_call_start_token: String,
    /// 单个工具调用的结束 token（如 `"</tool_call>"`）
    pub tool_call_end_token: String,
    /// 函数名的起始 token（如 `"<function="`）
    pub function_start_token: String,
    /// 函数名的结束 token（如 `"</function>"`）
    pub function_end_token: String,
    /// 参数的起始 token（如 `"<parameter="`）
    pub parameter_start_token: String,
    /// 参数的结束 token（如 `"</parameter>"`）
    pub parameter_end_token: String,

    /// 参见 [`JsonParserConfig::allow_eof_recovery`]。流式 jail 必须保持 `false`。
    #[serde(default)]
    pub allow_eof_recovery: bool,
}

impl Default for XmlParserConfig {
    fn default() -> Self {
        // 默认对应 qwen3-coder 形态：`<tool_call>` / `<function=` / `<parameter=`。
        Self {
            tool_call_start_token: "<tool_call>".to_string(),
            tool_call_end_token: "</tool_call>".to_string(),
            function_start_token: "<function=".to_string(),
            function_end_token: "</function>".to_string(),
            parameter_start_token: "<parameter=".to_string(),
            parameter_end_token: "</parameter>".to_string(),
            allow_eof_recovery: false,
        }
    }
}

// === SECTION: DSML 解析配置 ===

/// DSML 风格工具调用解析器配置（DeepSeek V3.2+）
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct DsmlParserConfig {
    /// DSML 块的起始 token（如 `"<｜DSML｜function_calls>"` 或 `"<｜DSML｜tool_calls>"`）
    #[serde(alias = "function_calls_start")]
    pub block_start: String,
    /// DSML 块的结束 token（如 `"</｜DSML｜function_calls>"` 或 `"</｜DSML｜tool_calls>"`）
    #[serde(alias = "function_calls_end")]
    pub block_end: String,
    /// invoke 的起始前缀（如 `"<｜DSML｜invoke name="`）
    pub invoke_start_prefix: String,
    /// invoke 的结束 token（如 `"</｜DSML｜invoke>"`）
    pub invoke_end: String,
    /// 参数的起始前缀（如 `"<｜DSML｜parameter name="`）
    pub parameter_prefix: String,
    /// 参数的结束 token（如 `"</｜DSML｜parameter>"`）
    pub parameter_end: String,
}

impl Default for DsmlParserConfig {
    fn default() -> Self {
        Self {
            block_start: "<｜DSML｜function_calls>".to_string(),
            block_end: "</｜DSML｜function_calls>".to_string(),
            invoke_start_prefix: "<｜DSML｜invoke name=".to_string(),
            invoke_end: "</｜DSML｜invoke>".to_string(),
            parameter_prefix: "<｜DSML｜parameter name=".to_string(),
            parameter_end: "</｜DSML｜parameter>".to_string(),
        }
    }
}

// === SECTION: GLM-4.7 解析配置 ===

/// GLM-4.7 风格工具调用解析器配置
/// 格式：`<tool_call>function_name<arg_key>param</arg_key><arg_value>value</arg_value></tool_call>`
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Glm47ParserConfig {
    /// 工具调用块的起始 token（如 `"<tool_call>"`）
    pub tool_call_start: String,
    /// 工具调用块的结束 token（如 `"</tool_call>"`）
    pub tool_call_end: String,
    /// 参数 key 的起始 token（如 `"<arg_key>"`）
    pub arg_key_start: String,
    /// 参数 key 的结束 token（如 `"</arg_key>"`）
    pub arg_key_end: String,
    /// 参数 value 的起始 token（如 `"<arg_value>"`）
    pub arg_value_start: String,
    /// 参数 value 的结束 token（如 `"</arg_value>"`）
    pub arg_value_end: String,

    /// 参见 [`JsonParserConfig::allow_eof_recovery`]。流式 jail 必须保持 `false`。
    #[serde(default)]
    pub allow_eof_recovery: bool,
}

impl Default for Glm47ParserConfig {
    fn default() -> Self {
        Self {
            tool_call_start: "<tool_call>".to_string(),
            tool_call_end: "</tool_call>".to_string(),
            arg_key_start: "<arg_key>".to_string(),
            arg_key_end: "</arg_key>".to_string(),
            arg_value_start: "<arg_value>".to_string(),
            arg_value_end: "</arg_value>".to_string(),
            allow_eof_recovery: false,
        }
    }
}

// === SECTION: Kimi-K2 解析配置 ===

/// Kimi K2 工具调用解析器配置
///
/// 格式：
/// ```text
/// <|tool_calls_section_begin|>
/// <|tool_call_begin|>functions.{name}:{index}<|tool_call_argument_begin|>{json_args}<|tool_call_end|>
/// <|tool_calls_section_end|>
/// ```
///
/// 模型可能输出复数或单数形式的 section token
/// （如 `<|tool_calls_section_begin|>` 或 `<|tool_call_section_begin|>`）。
/// 两种形式均通过 `section_start_variants` 与 `section_end_variants` 字段支持。
/// 参考 vllm `kimi_k2_tool_parser.py`。
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct KimiK2ParserConfig {
    /// 工具调用区段的主起始 token
    pub section_start: String,
    /// 工具调用区段的主结束 token
    pub section_end: String,
    /// 工具调用区段所有识别的起始 token（含单数变体）
    pub section_start_variants: Vec<String>,
    /// 工具调用区段所有识别的结束 token（含单数变体）
    pub section_end_variants: Vec<String>,
    /// 单个工具调用的起始 token（如 `"<|tool_call_begin|>"`）
    pub call_start: String,
    /// 单个工具调用的结束 token（如 `"<|tool_call_end|>"`）
    pub call_end: String,
    /// 分隔函数 ID 与 JSON 参数的 token（如 `"<|tool_call_argument_begin|>"`）
    pub argument_begin: String,
}

impl Default for KimiK2ParserConfig {
    fn default() -> Self {
        // section_*_variants 同时收录复数与单数形态，兼容模型可能输出的两种 section 标签。
        Self {
            section_start: "<|tool_calls_section_begin|>".to_string(),
            section_end: "<|tool_calls_section_end|>".to_string(),
            section_start_variants: vec![
                "<|tool_calls_section_begin|>".to_string(),
                "<|tool_call_section_begin|>".to_string(),
            ],
            section_end_variants: vec![
                "<|tool_calls_section_end|>".to_string(),
                "<|tool_call_section_end|>".to_string(),
            ],
            call_start: "<|tool_call_begin|>".to_string(),
            call_end: "<|tool_call_end|>".to_string(),
            argument_begin: "<|tool_call_argument_begin|>".to_string(),
        }
    }
}

// === SECTION: 顶层解析配置枚举 ===

/// 解析器专属配置
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ParserConfig {
    Json(JsonParserConfig),
    Xml(XmlParserConfig),
    Pythonic,
    Harmony(JsonParserConfig),
    Typescript,
    Dsml(DsmlParserConfig),
    KimiK2(KimiK2ParserConfig),
    Glm47(Glm47ParserConfig),
    /// Gemma 4 使用自定义非 JSON 语法，包含裸 key、`<|"|>` 字符串定界符
    /// 以及固定的 `<|tool_call>...<tool_call|>` 标记。
    /// 运行时无需配置——标记不是用户可调的。
    Gemma4,
}

impl ParserConfig {
    /// 获取本解析器配置的工具调用起始 token
    /// 返回标识工具调用开始的起始 token 列表
    pub fn tool_call_start_tokens(&self) -> Vec<String> {
        match self {
            // JSON 与 Harmony 直接复用各自配置里的起始 token 列表。
            ParserConfig::Json(config) | ParserConfig::Harmony(config) => {
                config.tool_call_start_tokens.clone()
            }
            ParserConfig::Xml(config) => vec![config.tool_call_start_token.clone()],
            ParserConfig::Dsml(config) => vec![config.block_start.clone()],
            ParserConfig::Glm47(config) => vec![config.tool_call_start.clone()],
            ParserConfig::KimiK2(config) => config.section_start_variants.clone(),
            ParserConfig::Gemma4 => vec![crate::tool_calling::gemma4::TOOL_CALL_START.to_string()],
            // Pythonic / Typescript 无独立起始 token。
            ParserConfig::Pythonic | ParserConfig::Typescript => vec![],
        }
    }

    /// 获取本解析器配置的工具调用结束 token
    /// 返回标识工具调用结束的结束 token 列表
    pub fn tool_call_end_tokens(&self) -> Vec<String> {
        match self {
            ParserConfig::Json(config) | ParserConfig::Harmony(config) => {
                config.tool_call_end_tokens.clone()
            }
            ParserConfig::Xml(config) => vec![config.tool_call_end_token.clone()],
            ParserConfig::Dsml(config) => vec![config.block_end.clone()],
            ParserConfig::Glm47(config) => vec![config.tool_call_end.clone()],
            ParserConfig::KimiK2(config) => config.section_end_variants.clone(),
            ParserConfig::Gemma4 => vec![crate::tool_calling::gemma4::TOOL_CALL_END.to_string()],
            ParserConfig::Pythonic | ParserConfig::Typescript => vec![],
        }
    }
}

// === SECTION: 顶层工具调用配置与模型预设 ===

/// 解析不同格式工具调用的聚合配置
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ToolCallConfig {
    /// 具体解析器的配置。
    pub parser_config: ParserConfig,
}

impl Default for ToolCallConfig {
    fn default() -> Self {
        Self {
            parser_config: ParserConfig::Json(JsonParserConfig::default()),
        }
    }
}

/// 私有助手：把「起始/结束 token + 其余字段取默认」这一最常见的 JSON 预设
/// 形态集中表达，供下面多个仅在起止 token 上有差异的工厂方法复用。
fn json_preset(start: Vec<&str>, end: Vec<&str>) -> ToolCallConfig {
    ToolCallConfig {
        parser_config: ParserConfig::Json(JsonParserConfig {
            tool_call_start_tokens: start.into_iter().map(str::to_string).collect(),
            tool_call_end_tokens: end.into_iter().map(str::to_string).collect(),
            ..Default::default()
        }),
    }
}

impl ToolCallConfig {
    /// hermess 工具调用的默认配置
    /// `<tool_call>{"name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit"}}\n</tool_call>`
    pub fn hermes() -> Self {
        json_preset(vec!["<tool_call>"], vec!["</tool_call>"])
    }

    /// nemotron 工具调用的默认配置
    /// `<TOOLCALL>[{"name": "get_weather", "arguments": ...}]</TOOLCALL>`
    pub fn nemotron_deci() -> Self {
        json_preset(vec!["<TOOLCALL>"], vec!["</TOOLCALL>"])
    }

    pub fn llama3_json() -> Self {
        // <|python_tag|>{ "name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit"} }
        // or { "name": "get_weather", "arguments": {"location": "San Francisco, CA", "unit": "fahrenheit"} }
        json_preset(vec!["<|python_tag|>"], vec![""])
    }

    pub fn mistral() -> Self {
        json_preset(vec!["[TOOL_CALLS]"], vec!["[/TOOL_CALLS]", ""])
    }

    pub fn phi4() -> Self {
        json_preset(vec!["functools"], vec![""])
    }

    pub fn pythonic() -> Self {
        Self {
            parser_config: ParserConfig::Pythonic,
        }
    }

    pub fn harmony() -> Self {
        Self {
            parser_config: ParserConfig::Harmony(JsonParserConfig {
                tool_call_start_tokens: vec!["<|start|>assistant<|channel|>commentary".to_string()],
                tool_call_end_tokens: vec!["<|call|>".to_string()],
                ..Default::default()
            }),
        }
    }

    pub fn deepseek_v3_1() -> Self {
        // 整个工具调用块包裹在
        // <｜tool▁calls▁begin｜> ... <｜tool▁calls▁end｜>
        // 之间，无论包含多少个工具调用。对外使用此配置时，
        // 我们希望仅在整个块级别操作，
        // 以便工具解析器能正确消费所有工具调用 token。
        // https://huggingface.co/deepseek-ai/DeepSeek-V3.1#toolcall
        Self {
            parser_config: ParserConfig::Json(JsonParserConfig {
                tool_call_start_tokens: vec![
                    "<｜tool▁calls▁begin｜>".to_string(),
                    // "<｜tool▁call▁begin｜>".to_string(),
                ],
                tool_call_end_tokens: vec![
                    "<｜tool▁calls▁end｜>".to_string(),
                    // "<｜tool▁call▁end｜>".to_string(),
                ],
                tool_call_separator_tokens: vec!["<｜tool▁sep｜>".to_string()],
                parser_type: JsonParserType::DeepseekV31,
                ..Default::default()
            }),
        }
    }

    pub fn deepseek_v3() -> Self {
        // DeepSeek V3 格式：
        // <｜tool▁calls▁begin｜><｜tool▁call▁begin｜>{type}<｜tool▁sep｜>{function_name}\n```json\n{arguments}\n```<｜tool▁call▁end｜><｜tool▁calls▁end｜>
        // DeepSeek V3 与 DeepSeek V3.1 之间存在一些差异
        Self {
            parser_config: ParserConfig::Json(JsonParserConfig {
                tool_call_start_tokens: vec!["<｜tool▁calls▁begin｜>".to_string()],
                tool_call_end_tokens: vec!["<｜tool▁calls▁end｜>".to_string()],
                tool_call_separator_tokens: vec!["<｜tool▁sep｜>".to_string()],
                parser_type: JsonParserType::DeepseekV3,
                ..Default::default()
            }),
        }
    }

    pub fn qwen3_coder() -> Self {
        // <tool_call><function=name><parameter=key>value</parameter></function></tool_call>
        Self {
            parser_config: ParserConfig::Xml(XmlParserConfig::default()),
        }
    }

    pub fn jamba() -> Self {
        json_preset(vec!["<tool_calls>"], vec!["</tool_calls>"])
    }

    fn deepseek_dsml(block_name: &str) -> Self {
        Self {
            parser_config: ParserConfig::Dsml(DsmlParserConfig {
                block_start: format!("<｜DSML｜{}>", block_name),
                block_end: format!("</｜DSML｜{}>", block_name),
                ..Default::default()
            }),
        }
    }

    pub fn deepseek_v3_2() -> Self {
        // DeepSeek V3.2 format (DSML):
        // <｜DSML｜function_calls>
        // <｜DSML｜invoke name="function_name">
        // <｜DSML｜parameter name="param_name" string="true|false">value</｜DSML｜parameter>
        // </｜DSML｜invoke>
        // </｜DSML｜function_calls>
        Self::deepseek_dsml("function_calls")
    }

    pub fn deepseek_v4() -> Self {
        // DeepSeek V4 format (DSML):
        // <｜DSML｜tool_calls>
        // <｜DSML｜invoke name="function_name">
        // <｜DSML｜parameter name="param_name" string="true|false">value</｜DSML｜parameter>
        // </｜DSML｜invoke>
        // </｜DSML｜tool_calls>
        Self::deepseek_dsml("tool_calls")
    }

    pub fn minimax_m2() -> Self {
        // MiniMax-M2.1 格式：
        // <minimax:tool_call>
        // <invoke name="function_name">
        // <parameter name="param_name">value</parameter>
        // </invoke>
        // </minimax:tool_call>
        // 参考：https://huggingface.co/MiniMaxAI/MiniMax-M2.1/blob/main/docs/tool_calling_guide.md
        Self {
            parser_config: ParserConfig::Xml(XmlParserConfig {
                tool_call_start_token: "<minimax:tool_call>".to_string(),
                tool_call_end_token: "</minimax:tool_call>".to_string(),
                function_start_token: "<invoke name=".to_string(),
                function_end_token: "</invoke>".to_string(),
                parameter_start_token: "<parameter name=".to_string(),
                parameter_end_token: "</parameter>".to_string(),
                allow_eof_recovery: false,
            }),
        }
    }

    pub fn glm47() -> Self {
        // GLM-4.7 格式：
        // <tool_call>function_name<arg_key>param1</arg_key><arg_value>value1</arg_value></tool_call>
        // 参考：https://huggingface.co/zai-org/GLM-4.7/blob/main/chat_template.jinja
        Self {
            parser_config: ParserConfig::Glm47(Glm47ParserConfig::default()),
        }
    }

    pub fn kimi_k2() -> Self {
        // Kimi K2 格式：
        // <|tool_calls_section_begin|>
        // <|tool_call_begin|>functions.{name}:{index}<|tool_call_argument_begin|>{json_args}<|tool_call_end|>
        // <|tool_calls_section_end|>
        // 参考：https://huggingface.co/moonshotai/Kimi-K2-Instruct/blob/main/docs/tool_call_guidance.md
        Self {
            parser_config: ParserConfig::KimiK2(KimiK2ParserConfig::default()),
        }
    }

    /// Gemma 4 工具调用格式（自定义非 JSON 语法）：
    ///
    /// ```text
    /// <|tool_call>call:func_name{location:<|"|>Tokyo<|"|>,count:42}<tool_call|>
    /// ```
    ///
    /// 裸无引号 key，`<|"|>` 定界字符串，支持嵌套对象 / 数组。
    /// 多个工具调用首尾相接无分隔符。
    pub fn gemma4() -> Self {
        Self {
            parser_config: ParserConfig::Gemma4,
        }
    }
}

// === SECTION: tests ===

#[cfg(test)]
mod tests {
    //! ## 测试过程
    //! 从对外可观察契约出发覆盖四类点：
    //! 1. DSML 配置的 serde 旧别名（`function_calls_start/end`）能正确反序列化；
    //! 2. DSML 工厂（v3.2 / v4）产出的 block 起止 token 文案正确；
    //! 3. 简单 JSON 预设工厂的起止 token 与默认字段契约一致；
    //! 4. `ParserConfig` 的起止 token 访问器按变体返回正确结果。
    //!
    //! ## 意义
    //! 这些断言锁定的是对外配置形态（字段值、serde 别名、工厂输出、访问器行为），
    //! 是各解析器据以工作的输入边界，必须随实现重写而保持稳定。

    use super::*;

    /// 从 `ParserConfig` 中取出 JSON 配置，否则 panic（仅测试辅助）。
    fn expect_json(cfg: ToolCallConfig) -> JsonParserConfig {
        match cfg.parser_config {
            ParserConfig::Json(c) => c,
            other => panic!("expected Json variant, got {other:?}"),
        }
    }

    /// 从 `ParserConfig` 中取出 DSML 配置，否则 panic（仅测试辅助）。
    fn expect_dsml(cfg: ToolCallConfig) -> DsmlParserConfig {
        match cfg.parser_config {
            ParserConfig::Dsml(c) => c,
            other => panic!("expected Dsml variant, got {other:?}"),
        }
    }

    #[test]
    fn dsml_config_deserializes_legacy_function_calls_aliases() {
        let legacy = serde_json::json!({
            "function_calls_start": "<｜DSML｜function_calls>",
            "function_calls_end": "</｜DSML｜function_calls>",
            "invoke_start_prefix": "<｜DSML｜invoke name=",
            "invoke_end": "</｜DSML｜invoke>",
            "parameter_prefix": "<｜DSML｜parameter name=",
            "parameter_end": "</｜DSML｜parameter>",
        });
        let cfg: DsmlParserConfig = serde_json::from_value(legacy).unwrap();
        assert_eq!(cfg.block_start, "<｜DSML｜function_calls>");
        assert_eq!(cfg.block_end, "</｜DSML｜function_calls>");
        assert_eq!(cfg.invoke_start_prefix, "<｜DSML｜invoke name=");
    }

    #[test]
    fn deepseek_dsml_factory_produces_expected_block_tokens() {
        let v3_2 = expect_dsml(ToolCallConfig::deepseek_v3_2());
        let v4 = expect_dsml(ToolCallConfig::deepseek_v4());
        assert_eq!(v3_2.block_start, "<｜DSML｜function_calls>");
        assert_eq!(v3_2.block_end, "</｜DSML｜function_calls>");
        assert_eq!(v4.block_start, "<｜DSML｜tool_calls>");
        assert_eq!(v4.block_end, "</｜DSML｜tool_calls>");
    }

    #[test]
    fn json_presets_carry_expected_boundary_tokens() {
        let hermes = expect_json(ToolCallConfig::hermes());
        assert_eq!(hermes.tool_call_start_tokens, vec!["<tool_call>"]);
        assert_eq!(hermes.tool_call_end_tokens, vec!["</tool_call>"]);

        let mistral = expect_json(ToolCallConfig::mistral());
        assert_eq!(mistral.tool_call_start_tokens, vec!["[TOOL_CALLS]"]);
        assert_eq!(mistral.tool_call_end_tokens, vec!["[/TOOL_CALLS]", ""]);

        // 简单预设应保留默认的 parser_type 与 arguments_keys。
        assert!(matches!(hermes.parser_type, JsonParserType::Basic));
        assert_eq!(hermes.arguments_keys, vec!["arguments", "parameters"]);
    }

    #[test]
    fn parser_config_boundary_token_accessors_match_variant() {
        let kimi = ToolCallConfig::kimi_k2().parser_config;
        assert_eq!(
            kimi.tool_call_start_tokens(),
            vec![
                "<|tool_calls_section_begin|>".to_string(),
                "<|tool_call_section_begin|>".to_string(),
            ]
        );

        // Pythonic 无独立起止 token。
        let pythonic = ToolCallConfig::pythonic().parser_config;
        assert!(pythonic.tool_call_start_tokens().is_empty());
        assert!(pythonic.tool_call_end_tokens().is_empty());
    }
}
