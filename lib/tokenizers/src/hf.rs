// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! HuggingFace 分词器后端。
//!
//! ## 设计意图
//! 把 `tokenizers` 官方库的 `Tokenizer` 包一层薄封装，使其满足本 crate 统一的
//! `Encoder` / `Decoder` / `Tokenizer` trait 契约，从而能与其他后端互换使用。
//!
//! ## 外部契约
//! - `HuggingFaceTokenizer`：暴露 `from_file`、`from_tokenizer`，并实现 `Encoder`、
//!   `Decoder`、`Tokenizer`，以及 `From<HfTokenizer>`。
//! - 编码统一返回 `Encoding::Hf`；解码经由库内置的字节回退逻辑产生 `DecodeResult`。
//!
//! ## 实现要点
//! 全部能力委托给底层库，本模块只负责错误信息的转换与类型包装；
//! 解码结果交给 `DecodeResult::from_decoded` 判定是否以 U+FFFD 结尾。

use tokenizers::tokenizer::Tokenizer as HfTokenizer;

use super::{
    Encoding, Error, Result, TokenIdType,
    traits::{DecodeResult, Decoder, Encoder, Tokenizer},
};

// === SECTION: 类型定义 ===

/// 基于 HuggingFace `tokenizers` 库的分词器封装。
pub struct HuggingFaceTokenizer {
    /// 被包装的底层库分词器实例。
    inner: HfTokenizer,
}

// === SECTION: 构造入口 ===

impl HuggingFaceTokenizer {
    /// 从磁盘上的 `tokenizer.json` 文件加载分词器。
    pub fn from_file(model_name: &str) -> Result<Self> {
        let inner = HfTokenizer::from_file(model_name)
            .map_err(|err| Error::msg(format!("Error loading tokenizer: {}", err)))?;

        Ok(Self { inner })
    }

    /// 由一个已构建好的库内分词器对象直接封装。
    pub fn from_tokenizer(tokenizer: HfTokenizer) -> Self {
        Self { inner: tokenizer }
    }
}

// === SECTION: 编码实现 ===

impl Encoder for HuggingFaceTokenizer {
    fn encode(&self, input: &str) -> Result<Encoding> {
        // 直接调用底层库完成单条编码。
        let encoding = self
            .inner
            .encode(input, false)
            .map_err(|err| Error::msg(format!("Error tokenizing input: {err}")))?;

        Ok(Encoding::Hf(Box::new(encoding)))
    }

    fn encode_batch(&self, inputs: &[&str]) -> Result<Vec<Encoding>> {
        let hf_encodings = self
            .inner
            .encode_batch(inputs.to_vec(), false)
            .map_err(|err| Error::msg(format!("Error batch tokenizing input: {err}")))?;

        Ok(hf_encodings
            .into_iter()
            .map(|enc| Encoding::Hf(Box::new(enc)))
            .collect())
    }
}

// === SECTION: 解码实现 ===

impl Decoder for HuggingFaceTokenizer {
    fn decode(&self, token_ids: &[TokenIdType], skip_special_tokens: bool) -> Result<DecodeResult> {
        // 调用底层库解码，库内已处理字节回退，结果交由 DecodeResult 判定是否完整。
        let text = self
            .inner
            .decode(token_ids, skip_special_tokens)
            .map_err(|err| Error::msg(format!("Error de-tokenizing input: {err}")))?;

        Ok(text.into())
    }
}

impl Tokenizer for HuggingFaceTokenizer {}

// === SECTION: 类型转换 ===

impl From<HfTokenizer> for HuggingFaceTokenizer {
    fn from(tokenizer: HfTokenizer) -> Self {
        Self { inner: tokenizer }
    }
}
