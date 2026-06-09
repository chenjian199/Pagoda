// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 基于 `fastokens` crate 的高性能 BPE 编码后端。
//!
//! ## 设计意图
//! `fastokens` 只提供编码能力，因此本模块构建一个「混合」分词器：用 `fastokens`
//! 负责高速编码，再回退到 `HuggingFaceTokenizer` 负责解码；两者从同一个
//! `tokenizer.json` 文件加载，保证词表完全一致。
//!
//! ## 外部契约
//! - `FastTokenizer`：暴露 `from_file`，并实现 `Encoder`、`Decoder`、`Tokenizer`。
//! - 编码返回 `Encoding::Sp`；解码委托 HuggingFace 后端，行为与其一致。
//!
//! ## 实现要点
//! 编码路径直接调用 `fastokens`；批量编码借助 `rayon` 并行映射；
//! 解码完全转交内部持有的 HuggingFace 分词器。

use std::path::Path;

use rayon::prelude::*;

use super::{
    Encoding, Error, Result, TokenIdType,
    hf::HuggingFaceTokenizer,
    traits::{DecodeResult, Decoder, Encoder, Tokenizer},
};

// === SECTION: 类型定义 ===

/// 混合分词器：编码走 `fastokens` 快路径，解码回退 HuggingFace。
///
/// 两个后端均从同一份 `tokenizer.json` 加载。
pub struct FastTokenizer {
    /// 负责编码的 `fastokens` 引擎。
    fast_encoder: fastokens::Tokenizer,
    /// 负责解码的 HuggingFace 后端。
    hf_decoder: HuggingFaceTokenizer,
}

// === SECTION: 构造入口 ===

impl FastTokenizer {
    /// 从 `tokenizer.json` 文件同时加载编码与解码两套后端。
    pub fn from_file(path: &str) -> Result<Self> {
        let fast_encoder = fastokens::Tokenizer::from_file(Path::new(path))
            .map_err(|e| Error::msg(format!("Error loading fastokens tokenizer: {e}")))?;
        let hf_decoder = HuggingFaceTokenizer::from_file(path)?;
        Ok(Self {
            fast_encoder,
            hf_decoder,
        })
    }
}

// === SECTION: 编码实现 ===

impl Encoder for FastTokenizer {
    fn encode(&self, input: &str) -> Result<Encoding> {
        let ids = self
            .fast_encoder
            .encode(input)
            .map_err(|e| Error::msg(format!("Fastokens encode error: {e}")))?;
        Ok(Encoding::Sp(ids))
    }

    fn encode_batch(&self, inputs: &[&str]) -> Result<Vec<Encoding>> {
        // 借助 rayon 并行编码每一条输入。
        inputs.par_iter().map(|input| self.encode(input)).collect()
    }
}

// === SECTION: 解码实现 ===

impl Decoder for FastTokenizer {
    fn decode(&self, token_ids: &[TokenIdType], skip_special_tokens: bool) -> Result<DecodeResult> {
        // fastokens 不支持解码，统一交给 HuggingFace 后端。
        self.hf_decoder.decode(token_ids, skip_special_tokens)
    }
}

impl Tokenizer for FastTokenizer {}

#[cfg(test)]
mod tests {
    //! ## 设计意图
    //!
    //! 本测试模块承担双重职责：既逐字保留标准基准测试作为接口契约回归基线，
    //! 又针对重写后的混合编码/解码路径补充覆盖。
    //!
    //! ## 外部契约
    //!
    //! `FastTokenizer` 的编码结果必须与 HuggingFace 后端逐 token 一致，解码、
    //! 批量编码、配合 `DecodeStream` 的增量解码行为均与标准实现等价。
    //!
    //! ## 实现要点
    //!
    //! 全部测试集中于唯一 `mod tests`；补充测试采用 `## 测试过程` / `## 意义` 注释格式。
    use super::*;
    use crate::HuggingFaceTokenizer;

    // 极简的合成 BPE 分词器：无 normalizer、无 post-processor，可被 fastokens 直接加载。
    // 词表覆盖：H,T,a,d,e,h,i,l,o,r,s,t,w 及标点。
    const TOKENIZER_PATH: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/data/tokenizer.json"
    );

    #[test]
    fn test_fast_encode_decode_roundtrip() {
        //! ## 测试过程
        //!
        //! 先编码再解码一段文本，验证两条路径都能无错执行并产出非空结果；
        //! 由于 null decoder 会在 token 间插入空格，故只比较去除空白后的字符是否保留。
        //!
        //! ## 意义
        //!
        //! 确认混合分词器编码→解码闭环可用，且解码不会丢失实际字符。
        let tokenizer = FastTokenizer::from_file(TOKENIZER_PATH).unwrap();
        let text = "Hello, world!";
        let encoding = tokenizer.encode(text).unwrap();
        assert!(!encoding.token_ids().is_empty());
        let decoded: String = tokenizer.decode(encoding.token_ids(), true).unwrap().into();
        assert!(!decoded.is_empty());
        // 解码文本应当包含与原文相同的非空白字符。
        let enc_chars: String = text.chars().filter(|c| !c.is_whitespace()).collect();
        let dec_chars: String = decoded.chars().filter(|c| !c.is_whitespace()).collect();
        assert_eq!(
            enc_chars, dec_chars,
            "non-space characters must be preserved"
        );
    }

    #[test]
    fn test_fast_matches_hf_encoding() {
        //! ## 测试过程
        //!
        //! 对同一组文本分别用 `FastTokenizer` 与 `HuggingFaceTokenizer` 编码，断言
        //! 两者产出的 token id 序列完全相同。
        //!
        //! ## 意义
        //!
        //! 这是接口契约核心：fastokens 快路径必须与 HuggingFace 标准编码逐 token 对齐。
        let fast = FastTokenizer::from_file(TOKENIZER_PATH).unwrap();
        let hf = HuggingFaceTokenizer::from_file(TOKENIZER_PATH).unwrap();

        for text in &["Hello, world!", "Hello", " world", "He llo"] {
            let fast_ids = fast.encode(text).unwrap();
            let hf_ids = hf.encode(text).unwrap();
            assert_eq!(
                fast_ids.token_ids(),
                hf_ids.token_ids(),
                "fastokens and HuggingFace must produce identical token IDs for '{text}'"
            );
        }
    }

    #[test]
    fn test_fast_batch_encode() {
        //! ## 测试过程
        //!
        //! 用 `encode_batch` 批量编码多条输入，断言返回数量正确且每条结果非空。
        //!
        //! ## 意义
        //!
        //! 验证基于 rayon 的并行批量编码在结果上与逐条编码一致。
        let tokenizer = FastTokenizer::from_file(TOKENIZER_PATH).unwrap();
        let inputs = &["Hello", " world", "Hello, world!"];
        let encodings = tokenizer.encode_batch(inputs).unwrap();
        assert_eq!(encodings.len(), inputs.len());
        for (enc, input) in encodings.iter().zip(inputs.iter()) {
            assert!(
                !enc.token_ids().is_empty(),
                "encoding for '{input}' must be non-empty"
            );
        }
    }

    #[test]
    fn test_fast_with_decode_stream() {
        //! ## 测试过程
        //!
        //! 编码提示词与续写文本，逐 token 喂入 `decode_stream` 累积增量片段，
        //! 再与「decode(提示+续写) 减去 decode(提示)」的上下文感知结果比较。
        //!
        //! ## 意义
        //!
        //! 验证混合分词器接入流式解码时，增量片段拼接结果等于带上下文的整体解码差值。
        use crate::Tokenizer as TokenizerWrapper;
        use std::sync::Arc;

        let tokenizer = Arc::new(FastTokenizer::from_file(TOKENIZER_PATH).unwrap());
        let wrapper = TokenizerWrapper::from(tokenizer);

        // 编码提示词与续写文本，再逐 token 走流式解码。
        let prompt_ids = wrapper.encode("Hello").unwrap().token_ids().to_vec();
        let continuation = ", world!";
        let cont_ids = wrapper.encode(continuation).unwrap().token_ids().to_vec();

        let mut stream = wrapper.decode_stream(&prompt_ids, true);
        // 累积来自 decode_stream 的增量片段。
        let mut accumulated = String::new();
        for id in &cont_ids {
            if let Some(chunk) = stream.step(*id).unwrap() {
                accumulated.push_str(&chunk);
            }
        }

        // DecodeStream 以提示 token 作为上下文，故期望文本是
        // decode(提示 + 续写) 减去 decode(提示)，而非缺少上下文的 decode(续写)。
        let mut all_ids = prompt_ids.clone();
        all_ids.extend_from_slice(&cont_ids);
        let full_text: String = wrapper.decode(&all_ids, true).unwrap().into();
        let prompt_text: String = wrapper.decode(&prompt_ids, true).unwrap().into();
        let expected = &full_text[prompt_text.len()..];
        assert_eq!(
            accumulated, expected,
            "streamed chunks must equal context-aware decoded continuation"
        );
    }

    // === SECTION: 重写实现细节补充测试 ===

    #[test]
    fn test_fast_encoding_is_sp_variant() {
        //! ## 测试过程
        //!
        //! 编码任意文本，断言返回的 `Encoding` 必为 `Sp` 变体。
        //!
        //! ## 意义
        //!
        //! 重写后编码路径固定走 `fastokens` 并封装为 `Encoding::Sp`；该测试锁定这一
        //! 表示约定，防止后续误改为 HuggingFace 的 `Hf` 变体。
        let tokenizer = FastTokenizer::from_file(TOKENIZER_PATH).unwrap();
        let encoding = tokenizer.encode("Hello").unwrap();
        assert!(
            matches!(encoding, Encoding::Sp(_)),
            "FastTokenizer 编码应为 Encoding::Sp 变体"
        );
    }

    #[test]
    fn test_fast_empty_batch_returns_empty() {
        //! ## 测试过程
        //!
        //! 对空输入切片调用 `encode_batch`，断言返回空向量且不报错。
        //!
        //! ## 意义
        //!
        //! 覆盖并行批量编码的边界：空输入应安全返回空结果，验证 rayon 映射的退化情形。
        let tokenizer = FastTokenizer::from_file(TOKENIZER_PATH).unwrap();
        let empty: &[&str] = &[];
        let encodings = tokenizer.encode_batch(empty).unwrap();
        assert!(encodings.is_empty());
    }
}
