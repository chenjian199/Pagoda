// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
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


#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct JsonParserConfig {
    pub tool_call_start_tokens: Vec<String>,
    pub tool_call_end_tokens: Vec<String>,
    pub tool_call_separator_tokens: Vec<String>,
    pub function_name_keys: Vec<String>,
    pub arguments_keys: Vec<String>,

    #[serde(default)]
    pub parser_type: JsonParserType,

    #[serde(default)]
    pub bare_json_mode: bool,

    #[serde(default)]
    pub allow_eof_recovery: bool,
}

impl Default for JsonParserConfig {
    fn default() -> Self {
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


#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct XmlParserConfig {
    pub tool_call_start_token: String,
    pub tool_call_end_token: String,
    pub function_start_token: String,
    pub function_end_token: String,
    pub parameter_start_token: String,
    pub parameter_end_token: String,

    #[serde(default)]
    pub allow_eof_recovery: bool,
}

impl Default for XmlParserConfig {
    fn default() -> Self {
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


#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct DsmlParserConfig {
    #[serde(alias = "function_calls_start")]
    pub block_start: String,
    #[serde(alias = "function_calls_end")]
    pub block_end: String,
    pub invoke_start_prefix: String,
    pub invoke_end: String,
    pub parameter_prefix: String,
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


#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Glm47ParserConfig {
    pub tool_call_start: String,
    pub tool_call_end: String,
    pub arg_key_start: String,
    pub arg_key_end: String,
    pub arg_value_start: String,
    pub arg_value_end: String,

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

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct KimiK2ParserConfig {
    pub section_start: String,
    pub section_end: String,
    pub section_start_variants: Vec<String>,
    pub section_end_variants: Vec<String>,
    pub call_start: String,
    pub call_end: String,
    pub argument_begin: String,
}

impl Default for KimiK2ParserConfig {
    fn default() -> Self {
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
    Gemma4,
}

impl ParserConfig {
    pub fn tool_call_start_tokens(&self) -> Vec<String> {
        match self {
            ParserConfig::Json(config) | ParserConfig::Harmony(config) => {
                config.tool_call_start_tokens.clone()
            }
            ParserConfig::Xml(config) => vec![config.tool_call_start_token.clone()],
            ParserConfig::Dsml(config) => vec![config.block_start.clone()],
            ParserConfig::Glm47(config) => vec![config.tool_call_start.clone()],
            ParserConfig::KimiK2(config) => config.section_start_variants.clone(),
            ParserConfig::Gemma4 => vec![crate::tool_calling::gemma4::TOOL_CALL_START.to_string()],
            ParserConfig::Pythonic | ParserConfig::Typescript => vec![],
        }
    }

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


#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ToolCallConfig {
    pub parser_config: ParserConfig,
}

impl Default for ToolCallConfig {
    fn default() -> Self {
        Self {
            parser_config: ParserConfig::Json(JsonParserConfig::default()),
        }
    }
}

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
    pub fn hermes() -> Self {
        json_preset(vec!["<tool_call>"], vec!["</tool_call>"])
    }

    pub fn nemotron_deci() -> Self {
        json_preset(vec!["<TOOLCALL>"], vec!["</TOOLCALL>"])
    }

    pub fn llama3_json() -> Self {
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
        Self {
            parser_config: ParserConfig::Json(JsonParserConfig {
                tool_call_start_tokens: vec![
                    "<｜tool▁calls▁begin｜>".to_string(),
                ],
                tool_call_end_tokens: vec![
                    "<｜tool▁calls▁end｜>".to_string(),
                ],
                tool_call_separator_tokens: vec!["<｜tool▁sep｜>".to_string()],
                parser_type: JsonParserType::DeepseekV31,
                ..Default::default()
            }),
        }
    }

    pub fn deepseek_v3() -> Self {
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
        Self::deepseek_dsml("function_calls")
    }

    pub fn deepseek_v4() -> Self {
        Self::deepseek_dsml("tool_calls")
    }

    pub fn minimax_m2() -> Self {
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
        Self {
            parser_config: ParserConfig::Glm47(Glm47ParserConfig::default()),
        }
    }

    pub fn kimi_k2() -> Self {
        Self {
            parser_config: ParserConfig::KimiK2(KimiK2ParserConfig::default()),
        }
    }
    pub fn gemma4() -> Self {
        Self {
            parser_config: ParserConfig::Gemma4,
        }
    }
}


#[cfg(test)]
mod tests {
    //! ## 测试过程
    //! 从对外可观察契约出发覆盖四类点：
    //!
    //! ## 意义
    //! 是各解析器据以工作的输入边界，必须随实现重写而保持稳定。

    use super::*;

    fn expect_json(cfg: ToolCallConfig) -> JsonParserConfig {
        match cfg.parser_config {
            ParserConfig::Json(c) => c,
            other => panic!("expected Json variant, got {other:?}"),
        }
    }

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

        let pythonic = ToolCallConfig::pythonic().parser_config;
        assert!(pythonic.tool_call_start_tokens().is_empty());
        assert!(pythonic.tool_call_end_tokens().is_empty());
    }
}
