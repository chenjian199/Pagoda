// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! tiktoken BPE 分词器后端。
//!
//! ## 设计意图
//! 加载 tiktoken 风格的 `.model` / `.tiktoken` 词表文件（每行「base64 token + rank」），
//! 配合 BPE 正则与特殊 token 表构建 `CoreBPE`，对外提供统一的编解码能力。
//!
//! ## 外部契约
//! - `TikTokenTokenizer`：暴露 `from_file`、`from_file_auto`，并实现 `Encoder`、
//!   `Decoder`、`Tokenizer`。
//! - 编码返回 `Encoding::Sp`；解码对不完整多字节序列产出 U+FFFD 而非报错。
//! - `from_file_auto` 自动从 `config.json` 推断 BPE 正则、从 `tokenizer_config.json`
//!   读取特殊 token，未命名的保留位按绝对 ID 命名为 `<|reserved_token_{id}|>`。
//!
//! ## 实现要点
//! 解码先尝试严格 UTF-8 快路径（零额外分配），失败再退回 lossy 转换并按「是否以
//! U+FFFD 结尾」判定 `Complete` / `Partial`；批量编码借助 `rayon` 并行。

use std::collections::HashSet;
use std::path::Path;

use base64::Engine as _;
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use tiktoken_rs::CoreBPE;

use super::{
    Encoding, Error, Result, TokenIdType,
    traits::{DecodeResult, Decoder, Encoder, Tokenizer},
};

// === SECTION: 常量定义 ===

/// 填充词表空隙时生成的保留特殊 token 槽位数量。
/// 大多数基于 tiktoken 的模型会在基础词表之上保留 256 个 ID 用于特殊 token。
const DEFAULT_NUM_RESERVED_SPECIAL_TOKENS: u32 = 256;

/// 来自 moonshotai/Kimi-K2-Instruct/tokenization_kimi.py 的 Kimi BPE 正则。
const KIMI_PATTERN: &str = r#"[\p{Han}]+|[^\r\n\p{L}\p{N}]?[\p{Lu}\p{Lt}\p{Lm}\p{Lo}\p{M}&&[^\p{Han}]]*[\p{Ll}\p{Lm}\p{Lo}\p{M}&&[^\p{Han}]]+(?i:'s|'t|'re|'ve|'m|'ll|'d)?|[^\r\n\p{L}\p{N}]?[\p{Lu}\p{Lt}\p{Lm}\p{Lo}\p{M}&&[^\p{Han}]]+[\p{Ll}\p{Lm}\p{Lo}\p{M}&&[^\p{Han}]]*(?i:'s|'t|'re|'ve|'m|'ll|'d)?|\p{N}{1,3}| ?[^\s\p{L}\p{N}]+[\r\n]*|\s*[\r\n]+|\s+(?!\S)|\s+"#;

// === SECTION: 类型定义 ===

/// 基于 tiktoken `CoreBPE` 的分词器。
pub struct TikTokenTokenizer {
    /// 底层 BPE 编解码核心。
    bpe: CoreBPE,
    /// 全部特殊 token 的 ID 集合，用于解码时按需剔除。
    special_token_ids: HashSet<u32>,
}

// === SECTION: 构造入口 ===

impl TikTokenTokenizer {
    /// 由 tiktoken 模型文件创建分词器。
    ///
    /// # Arguments
    /// * `path` - `.model` 或 `.tiktoken` 文件路径（每行 base64 token + rank 格式）
    /// * `pattern` - BPE 正则模式字符串
    /// * `special_tokens` - 特殊 token 字符串到其 ID 的映射
    pub fn from_file(
        path: &str,
        pattern: &str,
        special_tokens: FxHashMap<String, u32>,
    ) -> Result<Self> {
        let encoder = parse_tiktoken_file(path)?;
        let special_token_ids: HashSet<u32> = special_tokens.values().copied().collect();

        let bpe = CoreBPE::new(encoder, special_tokens, pattern)
            .map_err(|err| Error::msg(format!("Error creating tiktoken BPE: {err}")))?;

        Ok(Self {
            bpe,
            special_token_ids,
        })
    }

    /// 由 tiktoken 模型文件创建分词器，并自动从 `config.json` 推断 BPE 正则、
    /// 从 `tokenizer_config.json` 读取特殊 token。
    ///
    /// tiktoken 文件与各配置文件必须位于同一目录。
    pub fn from_file_auto(path: &str) -> Result<Self> {
        let file_path = Path::new(path);
        let directory = file_path
            .parent()
            .ok_or_else(|| Error::msg("Cannot determine parent directory of tiktoken file"))?;

        let pattern = detect_bpe_pattern(directory)?;
        let encoder = parse_tiktoken_file(path)?;
        // 用「最大 rank + 1」而非 len，避免稀疏/非连续 rank 造成 ID 冲突。
        let num_base_tokens = encoder.values().max().map_or(0, |&m| m + 1) as usize;
        let special_tokens = load_special_tokens(directory, num_base_tokens)?;
        let special_token_ids: HashSet<u32> = special_tokens.values().copied().collect();

        let bpe = CoreBPE::new(encoder, special_tokens, pattern)
            .map_err(|err| Error::msg(format!("Error creating tiktoken BPE: {err}")))?;

        Ok(Self {
            bpe,
            special_token_ids,
        })
    }
}

// === SECTION: 编码实现 ===

impl Encoder for TikTokenTokenizer {
    fn encode(&self, input: &str) -> Result<Encoding> {
        let token_ids: Vec<u32> = self.bpe.encode_with_special_tokens(input);
        Ok(Encoding::Sp(token_ids))
    }

    fn encode_batch(&self, inputs: &[&str]) -> Result<Vec<Encoding>> {
        // 借助 rayon 并行编码每一条输入。
        inputs.par_iter().map(|input| self.encode(input)).collect()
    }
}

// === SECTION: 解码实现 ===

impl Decoder for TikTokenTokenizer {
    fn decode(&self, token_ids: &[TokenIdType], skip_special_tokens: bool) -> Result<DecodeResult> {
        // 按需过滤特殊 token；保留时直接复制原序列。
        let ids: Vec<u32> = if skip_special_tokens {
            token_ids
                .iter()
                .copied()
                .filter(|id| !self.special_token_ids.contains(id))
                .collect()
        } else {
            token_ids.to_vec()
        };


        // 先尝试严格 UTF-8：合法字节直接得到 `Complete` 且零额外分配（取走 Vec 所有权）。
        // 这能正确处理那些原始字节恰为 EF BF BD（合法 U+FFFD）的词表 token —— 它们是合法
        // UTF-8，绝不能与「不完整多字节序列」混淆。
        //
        // 失败时退回 lossy 转换，让残缺多字节序列变为 U+FFFD，再用「尾部是否为 FFFD」启发式
        // 分类。该分支仅在字节回退 token 的增量解码过程中才会触及。
        let bytes: Vec<u8> = self.bpe._decode_native_and_split(ids).flatten().collect();
        match String::from_utf8(bytes) {
            Ok(text) => Ok(DecodeResult::Complete(text)),
            Err(e) => {
                let text = String::from_utf8_lossy(e.as_bytes()).into_owned();
                Ok(DecodeResult::from_decoded(text))
            }
        }
    }
}

impl Tokenizer for TikTokenTokenizer {}

// === SECTION: 词表文件解析 ===

/// 解析 tiktoken 模型文件（每行「base64 编码 token + rank」）。
fn parse_tiktoken_file(path: &str) -> Result<FxHashMap<Vec<u8>, u32>> {
    let contents = std::fs::read_to_string(path)
        .map_err(|err| Error::msg(format!("Failed to read tiktoken file '{path}': {err}")))?;

    let engine = base64::engine::general_purpose::STANDARD;
    let mut encoder = FxHashMap::default();

    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut parts = line.split_whitespace();
        let token_b64 = parts
            .next()
            .ok_or_else(|| Error::msg(format!("Invalid tiktoken line (no token): {line}")))?;
        let rank_str = parts
            .next()
            .ok_or_else(|| Error::msg(format!("Invalid tiktoken line (no rank): {line}")))?;

        let token_bytes = engine
            .decode(token_b64)
            .map_err(|err| Error::msg(format!("Invalid base64 in tiktoken file: {err}")))?;
        let rank: u32 = rank_str
            .parse()
            .map_err(|err| Error::msg(format!("Invalid rank in tiktoken file: {err}")))?;

        encoder.insert(token_bytes, rank);
    }

    Ok(encoder)
}

// === SECTION: BPE 正则推断 ===

/// 通过读取 `config.json` 中的 `model_type` 字段推断模型对应的 BPE 正则。
fn detect_bpe_pattern(directory: &Path) -> Result<&'static str> {
    let model_type: String = crate::file_json_field(&directory.join("config.json"), "model_type")
        .map_err(|err| {
        Error::msg(format!("Failed to read model_type from config.json: {err}"))
    })?;

    match model_type.as_str() {
        // baseten-admin/Kimi-2.5-text-nvfp4-v3 模型的 config.json 中 model_type 为 "deepseek_v3"，
        // 因为 Kimi K2.5 构建于 DeepSeek V3 架构之上。
        // 它仍随附 Kimi 的 tiktoken 词表文件，故 KIMI_PATTERN 才是正确的 BPE 正则。
        // 纯 DeepSeek V3 模型不使用 tiktoken.model 文件（改用 tokenizer.json），因此此匹配安全。
        "kimi" | "kimi_k2" | "kimi_k25" | "deepseek_v3" => Ok(KIMI_PATTERN),
        _ => Err(Error::msg(format!(
            "Unsupported tiktoken model_type '{model_type}'. \
             Currently supported: kimi, kimi_k2, kimi_k25, deepseek_v3. \
             To add a new model type, extend detect_bpe_pattern() in lib/tokenizers/src/tiktoken.rs \
             with the appropriate BPE regex pattern. \
             Alternatively, provide a tokenizer.json (HuggingFace format) instead."
        ))),
    }
}

// === SECTION: 特殊 token 加载 ===

/// 从模型目录下的 `tokenizer_config.json` 加载特殊 token。
///
/// 读取 `added_tokens_decoder` 字段（字符串 token ID 到 token 定义的映射）；
/// 对未映射的 ID 回退生成 `<|reserved_token_{id}|>` 名称。
fn load_special_tokens(directory: &Path, num_base_tokens: usize) -> Result<FxHashMap<String, u32>> {
    let config_path = directory.join("tokenizer_config.json");

    // 当配置缺失或无 added_tokens_decoder 字段时，统一生成默认保留 token。
    let fill_reserved = |map: &mut FxHashMap<String, u32>| {
        let used_ids: HashSet<u32> = map.values().copied().collect();
        for i in 0..DEFAULT_NUM_RESERVED_SPECIAL_TOKENS {
            let id = num_base_tokens as u32 + i;
            if !used_ids.contains(&id) {
                map.insert(format!("<|reserved_token_{id}|>"), id);
            }
        }
    };

    let mut special_tokens = FxHashMap::default();

    if !config_path.exists() {
        // 没有 tokenizer_config.json —— 生成默认保留 token。
        fill_reserved(&mut special_tokens);
        return Ok(special_tokens);
    }

    let contents = std::fs::read_to_string(&config_path)
        .map_err(|err| Error::msg(format!("Failed to read tokenizer_config.json: {err}")))?;

    let config: serde_json::Value = serde_json::from_str(&contents)
        .map_err(|err| Error::msg(format!("Failed to parse tokenizer_config.json: {err}")))?;

    match config
        .get("added_tokens_decoder")
        .and_then(|v| v.as_object())
    {
        Some(added_tokens) => {
            for (id_str, token_def) in added_tokens {
                let id: u32 = id_str.parse().map_err(|err| {
                    Error::msg(format!(
                        "Invalid token ID '{id_str}' in added_tokens_decoder: {err}"
                    ))
                })?;

                let content = token_def
                    .get("content")
                    .and_then(|v| v.as_str())
                    .unwrap_or_else(|| {
                        // 良构配置中不应发生，此处优雅兜底。
                        tracing::warn!("Missing 'content' field for token ID {id}");
                        ""
                    });

                if !content.is_empty() {
                    special_tokens.insert(content.to_string(), id);
                }
            }

            // 用保留 token 填补预期区间内的空隙。
            fill_reserved(&mut special_tokens);
        }
        None => {
            // 没有 added_tokens_decoder —— 生成默认保留 token。
            fill_reserved(&mut special_tokens);
        }
    }

    Ok(special_tokens)
}

#[cfg(test)]
mod tests {
    //! ## 设计意图
    //!
    //! 本测试模块承担双重职责：逐项保留标准基准测试作为接口契约回归基线，
    //! 同时覆盖重写后的解析、特殊 token 加载、字节回退解码与增量重组等路径。
    //!
    //! ## 外部契约
    //!
    //! 编解码往返、特殊 token 跳过、保留 token 的绝对 ID 命名、不完整多字节序列的
    //! `Partial`/`Complete` 判定等行为均须与标准实现一致。
    //!
    //! ## 实现要点
    //!
    //! 全部测试集中于唯一 `mod tests`；测试注释统一采用 `## 测试过程` / `## 意义` 格式。
    use super::*;
    use crate::DecodeStream;
    use std::io::Write;
    use std::sync::Arc;

    fn create_test_tiktoken_file(dir: &Path) -> String {
        let engine = base64::engine::general_purpose::STANDARD;
        let mut content = String::new();

        // 构造若干简单 token 条目：单字节配以递增的 rank。
        let tokens: Vec<(&[u8], u32)> = vec![
            (b"h", 0),
            (b"e", 1),
            (b"l", 2),
            (b"o", 3),
            (b" ", 4),
            (b"w", 5),
            (b"r", 6),
            (b"d", 7),
            (b"he", 8),
            (b"ll", 9),
            (b"lo", 10),
            (b"wo", 11),
            (b"rl", 12),
            (b"hel", 13),
            (b"llo", 14),
            (b"wor", 15),
            (b"hell", 16),
            (b"ello", 17),
            (b"worl", 18),
            (b"hello", 19),
            (b"world", 20),
        ];

        for (token, rank) in tokens {
            let encoded = engine.encode(token);
            content.push_str(&format!("{encoded} {rank}\n"));
        }

        let file_path = dir.join("tiktoken.model");
        let mut file = std::fs::File::create(&file_path).unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file_path.to_str().unwrap().to_string()
    }

    fn create_test_config(dir: &Path, model_type: &str) {
        let config = serde_json::json!({
            "model_type": model_type,
            "max_position_embeddings": 32768,
            "eos_token_id": [21]
        });
        let file_path = dir.join("config.json");
        std::fs::write(file_path, serde_json::to_string_pretty(&config).unwrap()).unwrap();
    }

    fn create_test_tokenizer_config(dir: &Path, num_base_tokens: usize) {
        let mut added_tokens = serde_json::Map::new();
        let bos_id = num_base_tokens;
        let eos_id = num_base_tokens + 1;

        added_tokens.insert(
            bos_id.to_string(),
            serde_json::json!({"content": "[BOS]", "special": true}),
        );
        added_tokens.insert(
            eos_id.to_string(),
            serde_json::json!({"content": "[EOS]", "special": true}),
        );

        let config = serde_json::json!({
            "added_tokens_decoder": added_tokens
        });

        let file_path = dir.join("tokenizer_config.json");
        std::fs::write(file_path, serde_json::to_string_pretty(&config).unwrap()).unwrap();
    }

    #[test]
    fn test_parse_tiktoken_file() {
        //! ## 测试过程
        //!
        //! 写出一个合成 tiktoken 文件并解析，断言条目数与若干 token 的 rank 正确。
        //!
        //! ## 意义
        //!
        //! 验证词表文件解析器对 base64 token + rank 行格式的正确还原。
        let dir = tempfile::tempdir().unwrap();
        let file_path = create_test_tiktoken_file(dir.path());
        let encoder = parse_tiktoken_file(&file_path).unwrap();
        assert_eq!(encoder.len(), 21);
        assert_eq!(encoder[b"hello".as_slice()], 19);
        assert_eq!(encoder[b"world".as_slice()], 20);
    }

    #[test]
    fn test_parse_tiktoken_file_missing() {
        //! ## 测试过程
        //!
        //! 解析一个不存在的路径，断言返回错误。
        //!
        //! ## 意义
        //!
        //! 确认文件缺失时解析器以 `Err` 而非 panic 形式优雅失败。
        let result = parse_tiktoken_file("/nonexistent/path/tiktoken.model");
        assert!(result.is_err());
    }

    #[test]
    fn test_tiktoken_from_file() {
        //! ## 测试过程
        //!
        //! 由合成文件与显式特殊 token、简单正则构造分词器，做一次编码并解码往返。
        //!
        //! ## 意义
        //!
        //! 验证 `from_file` 路径下编解码闭环可无损还原原文。
        let dir = tempfile::tempdir().unwrap();
        let file_path = create_test_tiktoken_file(dir.path());

        let mut special_tokens = FxHashMap::default();
        special_tokens.insert("[BOS]".to_string(), 21_u32);
        special_tokens.insert("[EOS]".to_string(), 22_u32);

        // 测试用的简单正则。
        let pattern = r"[\w]+|[^\w\s]+|\s+";

        let tokenizer = TikTokenTokenizer::from_file(&file_path, pattern, special_tokens).unwrap();

        // 测试编码。
        let encoding = tokenizer.encode("hello world").unwrap();
        let ids = encoding.token_ids();
        assert!(!ids.is_empty());

        // 测试解码往返。
        let decoded: String = tokenizer.decode(ids, false).unwrap().into();
        assert_eq!(decoded, "hello world");
    }

    #[test]
    fn test_tiktoken_encoding_variant() {
        //! ## 测试过程
        //!
        //! 编码一段文本，断言返回的 `Encoding` 为 `Sp` 变体。
        //!
        //! ## 意义
        //!
        //! 锁定 tiktoken 后端的编码表示约定。
        let dir = tempfile::tempdir().unwrap();
        let file_path = create_test_tiktoken_file(dir.path());

        let special_tokens = FxHashMap::default();
        let pattern = r"[\w]+|[^\w\s]+|\s+";

        let tokenizer = TikTokenTokenizer::from_file(&file_path, pattern, special_tokens).unwrap();
        let encoding = tokenizer.encode("hello").unwrap();

        // 验证产出 Sp 变体。
        match &encoding {
            Encoding::Sp(_) => {}
            other => panic!("Expected Encoding::Sp, got {:?}", other),
        }
    }

    #[test]
    fn test_tiktoken_skip_special_tokens() {
        //! ## 测试过程
        //!
        //! 在编码结果前后拼接 BOS/EOS，分别以 skip=true / false 解码，断言前者剔除
        //! 特殊 token、后者保留。
        //!
        //! ## 意义
        //!
        //! 验证 `skip_special_tokens` 对解码输出的过滤语义符合契约。
        let dir = tempfile::tempdir().unwrap();
        let file_path = create_test_tiktoken_file(dir.path());

        let mut special_tokens = FxHashMap::default();
        special_tokens.insert("[BOS]".to_string(), 21_u32);
        special_tokens.insert("[EOS]".to_string(), 22_u32);

        let pattern = r"[\w]+|[^\w\s]+|\s+";

        let tokenizer = TikTokenTokenizer::from_file(&file_path, pattern, special_tokens).unwrap();

        // 编码 hello，并在首尾拼接特殊 token。
        let encoding = tokenizer.encode("hello").unwrap();
        let mut ids = vec![21u32]; // [BOS]
        ids.extend(encoding.token_ids());
        ids.push(22); // [EOS]

        // skip_special_tokens=true 应剔除特殊 token。
        let decoded_skip: String = tokenizer.decode(&ids, true).unwrap().into();
        assert_eq!(decoded_skip, "hello");

        // skip_special_tokens=false 应保留特殊 token。
        let decoded_all: String = tokenizer.decode(&ids, false).unwrap().into();
        assert!(decoded_all.contains("hello"));
    }

    #[test]
    fn test_tiktoken_from_file_auto() {
        //! ## 测试过程
        //!
        //! 准备 config.json 与 tokenizer_config.json，用 `from_file_auto` 自动构造，
        //! 做一次编解码往返。
        //!
        //! ## 意义
        //!
        //! 验证自动推断正则与加载特殊 token 的整链路可用。
        let dir = tempfile::tempdir().unwrap();
        let file_path = create_test_tiktoken_file(dir.path());

        create_test_config(dir.path(), "kimi");
        create_test_tokenizer_config(dir.path(), 21);

        let tokenizer = TikTokenTokenizer::from_file_auto(&file_path).unwrap();

        // 基本的编解码往返。
        let encoding = tokenizer.encode("hello world").unwrap();
        let ids = encoding.token_ids();
        assert!(!ids.is_empty());

        let decoded: String = tokenizer.decode(ids, false).unwrap().into();
        assert_eq!(decoded, "hello world");
    }

    #[test]
    fn test_detect_bpe_pattern_unknown() {
        //! ## 测试过程
        //!
        //! 写入未知 model_type 的 config.json，调用 `detect_bpe_pattern`，断言报错。
        //!
        //! ## 意义
        //!
        //! 验证不受支持的模型类型会被明确拒绝。
        let dir = tempfile::tempdir().unwrap();
        create_test_config(dir.path(), "unknown_model");
        let result = detect_bpe_pattern(dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_load_special_tokens_no_config() {
        //! ## 测试过程
        //!
        //! 在无 tokenizer_config.json 的目录调用 `load_special_tokens`，断言生成 256 个
        //! 保留 token 且首尾 ID 命名正确。
        //!
        //! ## 意义
        //!
        //! 验证配置缺失时默认保留 token 的数量与绝对 ID 命名。
        let dir = tempfile::tempdir().unwrap();
        let tokens = load_special_tokens(dir.path(), 100).unwrap();
        assert_eq!(tokens.len(), 256);
        assert_eq!(tokens["<|reserved_token_100|>"], 100);
        assert_eq!(tokens["<|reserved_token_355|>"], 355);
    }

    #[test]
    fn test_load_special_tokens_with_config() {
        //! ## 测试过程
        //!
        //! 准备含 BOS/EOS 的 tokenizer_config.json，调用 `load_special_tokens`，断言
        //! 命名映射正确且保留 token 填补空隙。
        //!
        //! ## 意义
        //!
        //! 验证有配置时既读取显式 token 又用保留 token 填满预期区间。
        let dir = tempfile::tempdir().unwrap();
        create_test_tokenizer_config(dir.path(), 100);
        let tokens = load_special_tokens(dir.path(), 100).unwrap();
        assert_eq!(tokens["[BOS]"], 100);
        assert_eq!(tokens["[EOS]"], 101);
        // 还应包含填补空隙的保留 token。
        assert!(tokens.len() > 2);
    }

    /// 辅助函数：构造包含原始字节 token（字节回退 token）的 tiktoken 文件。
    fn create_test_tiktoken_file_with_byte_tokens(dir: &Path) -> String {
        let engine = base64::engine::general_purpose::STANDARD;
        let mut content = String::new();

        let tokens: Vec<(&[u8], u32)> = vec![
            (b"h", 0),
            (b"e", 1),
            (b"l", 2),
            (b"o", 3),
            (b" ", 4),
            (b"hello", 5),
        ];

        for (token, rank) in &tokens {
            let encoded = engine.encode(token);
            content.push_str(&format!("{encoded} {rank}\n"));
        }

        // 字节回退 token：构成 CJK 字符「你」(U+4F60) 的各个独立字节。
        // UTF-8 编码：0xE4 0xBD 0xA0
        let byte_tokens: Vec<(Vec<u8>, u32)> =
            vec![(vec![0xE4], 100), (vec![0xBD], 101), (vec![0xA0], 102)];

        for (token, rank) in &byte_tokens {
            let encoded = engine.encode(token);
            content.push_str(&format!("{encoded} {rank}\n"));
        }

        // 表情符号「😀」(U+1F600) 的字节 —— 4 字节 UTF-8：0xF0 0x9F 0x98 0x80
        let emoji_tokens: Vec<(Vec<u8>, u32)> = vec![
            (vec![0xF0], 200),
            (vec![0x9F], 201),
            (vec![0x98], 202),
            (vec![0x80], 203),
        ];

        for (token, rank) in &emoji_tokens {
            let encoded = engine.encode(token);
            content.push_str(&format!("{encoded} {rank}\n"));
        }

        // 合法的 U+FFFD token：有效 UTF-8 字节 EF BF BD（替换字符作为真实词表条目，
        // 而非 lossy 转换的产物）。
        let fffd_token: Vec<(Vec<u8>, u32)> = vec![(vec![0xEF, 0xBF, 0xBD], 300)];

        for (token, rank) in &fffd_token {
            let encoded = engine.encode(token);
            content.push_str(&format!("{encoded} {rank}\n"));
        }

        let file_path = dir.join("tiktoken.model");
        let mut file = std::fs::File::create(&file_path).unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file_path.to_str().unwrap().to_string()
    }

    fn create_byte_token_tokenizer(dir: &Path) -> TikTokenTokenizer {
        let file_path = create_test_tiktoken_file_with_byte_tokens(dir);
        let special_tokens = FxHashMap::default();
        let pattern = r"[\w]+|[^\w\s]+|\s+";
        TikTokenTokenizer::from_file(&file_path, pattern, special_tokens).unwrap()
    }

    #[test]
    fn test_decode_single_incomplete_utf8_byte_does_not_error() {
        //! ## 测试过程
        //!
        //! 解码单个属于多字节字符一部分的字节回退 token，断言不报错且结果为 `Partial`。
        //!
        //! ## 意义
        //!
        //! 复现并锁定原始 panic 的修复：对不完整 UTF-8 字节应产出 `Partial` 而非错误。
        let dir = tempfile::tempdir().unwrap();
        let tokenizer = create_byte_token_tokenizer(dir.path());

        let result = tokenizer.decode(&[100], false);
        assert!(
            result.is_ok(),
            "decode() should not error on incomplete UTF-8 bytes"
        );
        let decode_result = result.unwrap();
        assert!(
            decode_result.is_partial(),
            "incomplete UTF-8 byte should produce DecodeResult::Partial, got: {:?}",
            decode_result
        );
    }

    #[test]
    fn test_decode_two_of_three_utf8_bytes_does_not_error() {
        //! ## 测试过程
        //!
        //! 解码三字节字符的前两个字节，断言不报错且结果为 `Partial`。
        //!
        //! ## 意义
        //!
        //! 验证 2/3 不完整序列被正确标记为待续，而非触发底层解码错误。
        let dir = tempfile::tempdir().unwrap();
        let tokenizer = create_byte_token_tokenizer(dir.path());

        let result = tokenizer.decode(&[100, 101], false);
        assert!(result.is_ok());
        let decode_result = result.unwrap();
        assert!(
            decode_result.is_partial(),
            "incomplete 2-of-3 UTF-8 bytes should produce DecodeResult::Partial, got: {:?}",
            decode_result
        );
    }

    #[test]
    fn test_decode_complete_multibyte_utf8_produces_correct_char() {
        //! ## 测试过程
        //!
        //! 解码「你」的全部三个字节，断言还原为正确字符。
        //!
        //! ## 意义
        //!
        //! 作为正确性校验：lossy 转换不得破坏完整的多字节字符。
        let dir = tempfile::tempdir().unwrap();
        let tokenizer = create_byte_token_tokenizer(dir.path());

        let result = tokenizer.decode(&[100, 101, 102], false);
        assert!(result.is_ok());
        assert_eq!(String::from(result.unwrap()), "你");
    }

    #[test]
    fn test_decode_complete_4byte_emoji_from_byte_tokens() {
        //! ## 测试过程
        //!
        //! 解码表情符号的全部四个字节，断言还原为正确字符。
        //!
        //! ## 意义
        //!
        //! 验证完整 4 字节序列在 lossy 路径下也不会被改写。
        let dir = tempfile::tempdir().unwrap();
        let tokenizer = create_byte_token_tokenizer(dir.path());

        let result = tokenizer.decode(&[200, 201, 202, 203], false);
        assert!(result.is_ok());
        assert_eq!(String::from(result.unwrap()), "😀");
    }

    #[test]
    fn test_decode_legitimate_replacement_char_token_is_complete() {
        //! ## 测试过程
        //!
        //! 解码原始字节恰为 EF BF BD 的合法词表 token，断言结果为 `Complete` 且文本为 U+FFFD。
        //!
        //! ## 意义
        //!
        //! 回归测试：合法的 U+FFFD 词表 token 必须判为 `Complete`，避免增量解码误抑制。
        let dir = tempfile::tempdir().unwrap();
        let tokenizer = create_byte_token_tokenizer(dir.path());

        let result = tokenizer.decode(&[300], false);
        assert!(result.is_ok());
        let decode_result = result.unwrap();
        assert!(
            decode_result.is_complete(),
            "legitimate U+FFFD vocab token must be Complete, got: {:?}",
            decode_result
        );
        assert_eq!(decode_result.as_str(), "\u{FFFD}");
    }

    #[test]
    fn test_decode_partial_emoji_does_not_error() {
        //! ## 测试过程
        //!
        //! 仅解码表情符号的首个字节，断言不报错且结果为 `Partial`。
        //!
        //! ## 意义
        //!
        //! 验证 4 字节字符的残缺前缀被正确标记为待续。
        let dir = tempfile::tempdir().unwrap();
        let tokenizer = create_byte_token_tokenizer(dir.path());

        let result = tokenizer.decode(&[200], false);
        assert!(result.is_ok());
        assert!(result.unwrap().is_partial());
    }

    #[test]
    fn test_decode_mixed_ascii_and_incomplete_bytes() {
        //! ## 测试过程
        //!
        //! 解码「ASCII 前缀 + 残缺字节」组合，断言结果为 `Partial` 且文本以 hello 开头。
        //!
        //! ## 意义
        //!
        //! 验证尾部残缺字节会让整体判为待续，但已有 ASCII 内容仍正确保留。
        let dir = tempfile::tempdir().unwrap();
        let tokenizer = create_byte_token_tokenizer(dir.path());

        let result = tokenizer.decode(&[5, 100], false);
        assert!(result.is_ok());
        let decode_result = result.unwrap();
        assert!(
            decode_result.is_partial(),
            "trailing incomplete byte should produce DecodeResult::Partial"
        );
        let text: String = decode_result.into();
        assert!(
            text.starts_with("hello"),
            "should start with 'hello', got: {:?}",
            text
        );
    }

    #[test]
    fn test_decode_stream_incremental_multibyte_reassembly() {
        //! ## 测试过程
        //!
        //! 通过 `DecodeStream` 逐字节喂入「你」的三个字节，断言前两次缓冲、第三次输出完整字符。
        //!
        //! ## 意义
        //!
        //! 端到端验证增量解码会缓冲残缺字节、待字符补全后再产出。
        let dir = tempfile::tempdir().unwrap();
        let tokenizer = create_byte_token_tokenizer(dir.path());
        let tokenizer_arc: Arc<dyn crate::traits::Tokenizer> = Arc::new(tokenizer);

        let mut stream = DecodeStream::new(tokenizer_arc, &[5], false);

        let r1 = stream.step(100).unwrap();
        assert_eq!(r1, None, "first byte of 3-byte char should be buffered");

        let r2 = stream.step(101).unwrap();
        assert_eq!(r2, None, "second byte of 3-byte char should be buffered");

        let r3 = stream.step(102).unwrap();
        assert!(r3.is_some(), "third byte should complete the character");
        assert_eq!(r3.unwrap(), "你");
    }

    #[test]
    fn test_decode_stream_incremental_emoji_reassembly() {
        //! ## 测试过程
        //!
        //! 通过 `DecodeStream` 逐字节喂入表情符号的四个字节，断言前三次缓冲、第四次输出完整字符。
        //!
        //! ## 意义
        //!
        //! 验证 4 字节字符的增量重组逻辑同样正确。
        let dir = tempfile::tempdir().unwrap();
        let tokenizer = create_byte_token_tokenizer(dir.path());
        let tokenizer_arc: Arc<dyn crate::traits::Tokenizer> = Arc::new(tokenizer);

        let mut stream = DecodeStream::new(tokenizer_arc, &[5], false);

        let r1 = stream.step(200).unwrap();
        assert_eq!(r1, None, "byte 1/4 of emoji should be buffered");

        let r2 = stream.step(201).unwrap();
        assert_eq!(r2, None, "byte 2/4 of emoji should be buffered");

        let r3 = stream.step(202).unwrap();
        assert_eq!(r3, None, "byte 3/4 of emoji should be buffered");

        let r4 = stream.step(203).unwrap();
        assert!(r4.is_some(), "byte 4/4 should complete the emoji");
        assert_eq!(r4.unwrap(), "😀");
    }

    #[test]
    fn test_tiktoken_encode_batch() {
        //! ## 测试过程
        //!
        //! 批量编码多条输入并逐条解码往返，断言数量与还原文本均正确。
        //!
        //! ## 意义
        //!
        //! 验证并行批量编码与逐条解码的组合行为一致。
        let dir = tempfile::tempdir().unwrap();
        let file_path = create_test_tiktoken_file(dir.path());

        let special_tokens = FxHashMap::default();
        let pattern = r"[\w]+|[^\w\s]+|\s+";

        let tokenizer = TikTokenTokenizer::from_file(&file_path, pattern, special_tokens).unwrap();

        let inputs = &["hello", "world"];
        let encodings = tokenizer.encode_batch(inputs).unwrap();
        assert_eq!(encodings.len(), 2);

        for (encoding, input) in encodings.iter().zip(inputs.iter()) {
            let decoded: String = tokenizer
                .decode(encoding.token_ids(), false)
                .unwrap()
                .into();
            assert_eq!(decoded, *input);
        }
    }

    /// 辅助函数：构造包含全部 256 个单字节 token（rank 0..255）的 tiktoken 文件。
    /// 由此得到完整的字节级基础词表，使任意 ASCII 字符串都可被编码。
    fn create_byte_level_tiktoken_file(dir: &Path) -> String {
        let engine = base64::engine::general_purpose::STANDARD;
        let mut content = String::new();
        for byte_val in 0u16..256 {
            let encoded = engine.encode([byte_val as u8]);
            content.push_str(&format!("{encoded} {byte_val}\n"));
        }
        let file_path = dir.join("tiktoken.model");
        std::fs::write(&file_path, &content).unwrap();
        file_path.to_str().unwrap().to_string()
    }

    #[test]
    fn test_reserved_token_absolute_id_naming_kimi_k25_regression() {
        //! ## 测试过程
        //!
        //! 构造字节级词表与 kimi 配置，断言未命名保留 token `<|reserved_token_258|>` 被识别
        //! 为单个特殊 token，且连续多个保留 token 各自编码为 1 个 token。
        //!
        //! ## 意义
        //!
        //! 回归测试 Kimi K2.5 的 token 膨胀问题：保留 token 必须按绝对 ID 命名，
        //! 否则提示词会被拆成多个 BPE token 而被 TRT-LLM 拒绝。
        let dir = tempfile::tempdir().unwrap();
        let file_path = create_byte_level_tiktoken_file(dir.path());

        // 含 kimi model type 的 config.json —— 触发 KIMI_PATTERN。
        create_test_config(dir.path(), "kimi");

        // tokenizer_config.json：BOS 为 256，EOS 为 257。基础词表为 ID 0..255。
        create_test_tokenizer_config(dir.path(), 256);

        let tokenizer = TikTokenTokenizer::from_file_auto(&file_path).unwrap();

        // ID 256 = [BOS]，ID 257 = [EOS]。
        // ID 258 = 第一个未命名保留 token。
        //   修复后：命名为 <|reserved_token_258|>（绝对 ID）
        //   修复前：命名为 <|reserved_token_2|>（相对偏移）

        // 单个未命名保留 token 应被识别为 1 个特殊 token。
        let single = "<|reserved_token_258|>";
        let enc = tokenizer.encode(single).unwrap();
        assert_eq!(
            enc.token_ids().len(),
            1,
            "'{single}' should be 1 special token, got {} tokens: {:?}. \
             This means fallback naming still uses relative offsets instead of absolute IDs.",
            enc.token_ids().len(),
            enc.token_ids()
        );
        assert_eq!(enc.token_ids()[0], 258);

        // 序列中的多个未命名保留 token（基准用例的迷你版）。
        // ID 258..268 全部未命名；修复后它们是 <|reserved_token_258|>..267。
        let multi: String = (258u32..268)
            .map(|id| format!("<|reserved_token_{id}|>"))
            .collect();
        let enc_multi = tokenizer.encode(&multi).unwrap();
        assert_eq!(
            enc_multi.token_ids().len(),
            10,
            "10 reserved token strings should produce exactly 10 tokens, got {}: {:?}",
            enc_multi.token_ids().len(),
            enc_multi.token_ids()
        );
        let expected_ids: Vec<u32> = (258..268).collect();
        assert_eq!(enc_multi.token_ids(), &expected_ids);
    }

    #[test]
    fn test_relative_offset_naming_causes_inflation() {
        //! ## 测试过程
        //!
        //! 手工构造采用错误「相对偏移」命名的特殊 token 表，断言同样的保留 token 字符串
        //! 会被拆成多个 BPE token，复现膨胀。
        //!
        //! ## 意义
        //!
        //! 反向印证绝对 ID 命名的必要性：错误命名会导致 token 数明显膨胀。
        let dir = tempfile::tempdir().unwrap();
        let file_path = create_byte_level_tiktoken_file(dir.path());

        let _encoder = parse_tiktoken_file(&file_path).unwrap();
        let num_base_tokens = 256usize;

        // 用旧的（有缺陷的）相对偏移命名构造特殊 token 表。
        let mut bad_special_tokens: FxHashMap<String, u32> = FxHashMap::default();
        bad_special_tokens.insert("[BOS]".to_string(), 256);
        bad_special_tokens.insert("[EOS]".to_string(), 257);
        for i in 0..DEFAULT_NUM_RESERVED_SPECIAL_TOKENS {
            let id = num_base_tokens as u32 + i;
            if id != 256 && id != 257 {
                // 旧命名：用相对偏移 i，而非绝对 id。
                bad_special_tokens.insert(format!("<|reserved_token_{i}|>"), id);
            }
        }

        let bad_tokenizer =
            TikTokenTokenizer::from_file(&file_path, KIMI_PATTERN, bad_special_tokens).unwrap();

        // 命名错误时，<|reserved_token_258|> 不会被识别为特殊 token，
        // 它会被拆成字节级 BPE token —— 数量远多于 1。
        let input = "<|reserved_token_258|>";
        let enc = bad_tokenizer.encode(input).unwrap();
        assert!(
            enc.token_ids().len() > 1,
            "With buggy relative-offset naming, '{}' should NOT be recognized as a \
             single special token. Got {} token(s): {:?}",
            input,
            enc.token_ids().len(),
            enc.token_ids()
        );

        // 展示膨胀：10 个保留 token 会产出远多于 10 个 BPE token。
        let multi: String = (258u32..268)
            .map(|id| format!("<|reserved_token_{id}|>"))
            .collect();
        let enc_multi = bad_tokenizer.encode(&multi).unwrap();
        assert!(
            enc_multi.token_ids().len() > 10,
            "With buggy naming, 10 reserved token strings should inflate beyond 10 tokens. \
             Got {}",
            enc_multi.token_ids().len(),
        );
    }
}
