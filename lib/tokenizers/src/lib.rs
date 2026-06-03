// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 统一分词器门面与流式解码状态机。
//!
//! ## 设计意图
//! 为 HuggingFace、tiktoken、fastokens 等多种后端提供一致的编解码接口，并在其上
//! 封装增量解码（`DecodeStream`）、序列状态（`Sequence`）与停止序列检测
//! （`StopSequenceDecoder`），供上层推理流程直接复用。
//!
//! ## 外部契约
//! - 公开类型：`TokenizerType`、`Encoding`、`Tokenizer`、`DecodeStream`、`Sequence`、
//!   `SequenceDecoderOutput`、`StopSequenceDecoder`、`StopSequenceDecoderBuilder`。
//! - 公开 trait：`traits::{Encoder, Decoder, Tokenizer}` 与 `DecodeResult`。
//! - 公开函数：`file_json_field`、`log_json_err`、`create_tokenizer_from_file`。
//! - 文件扩展名决定后端：`.json` → HuggingFace；`.model`/`.tiktoken` → tiktoken。
//!
//! ## 实现要点
//! 增量解码沿用 HuggingFace TGI 的 prefix/read 双偏移策略；停止检测在 token 级与
//! 序列级双重匹配，隐藏优先于可见。

pub mod fastokens;
pub mod hf;
pub mod tiktoken;

// TODO: 增加分词器基准测试
// TODO: 将 README.md 作为模块文档启用
// #[doc = include_str!("../README.md")]

use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use std::{fs::File, io::BufReader, ops::Deref, path::Path};

use anyhow::Context as _;
pub use anyhow::{Error, Result};

pub use fastokens::FastTokenizer;
pub use hf::HuggingFaceTokenizer;
pub use tiktoken::TikTokenTokenizer;
pub use traits::DecodeResult;

/// token id 的统一类型别名。
pub type TokenIdType = u32;

// === SECTION: 分词器类型标识 ===

/// 表示当前使用的分词器类型。
#[derive(Debug)]
pub enum TokenizerType {
    /// HuggingFace 分词器，携带模型路径。
    HuggingFace(String),
    /// TikToken 分词器，携带模型路径。
    TikToken(String),
}

/// 原始文本中的字符偏移区间。
pub type Offsets = (usize, usize);

// === SECTION: 编码结果 ===

/// 分词结果容器：包含 token id（以及可能的字符串 token 与跨度信息）。
#[derive(Debug, Clone)]
pub enum Encoding {
    /// Hugging Face 后端的编码结果。
    Hf(Box<tokenizers::tokenizer::Encoding>),
    /// Sentence Piece / tiktoken 等后端的纯 id 编码结果。
    Sp(Vec<TokenIdType>),
}

impl Encoding {
    /// 返回本次编码得到的 token id 切片。
    pub fn token_ids(&self) -> &[u32] {
        match self {
            Encoding::Hf(inner) => inner.get_ids(),
            Encoding::Sp(inner) => inner,
        }
    }
}

impl Hash for Encoding {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.token_ids().hash(state);
    }
}

// === SECTION: 编解码 trait 契约 ===

/// 统一的编解码 trait 定义。
pub mod traits {
    use super::*;

    /// 编码器：将文本转换为 [`Encoding`]。
    pub trait Encoder: Send + Sync {
        /// 编码单条文本。
        fn encode(&self, input: &str) -> Result<Encoding>;
        /// 批量编码多条文本。
        fn encode_batch(&self, inputs: &[&str]) -> Result<Vec<Encoding>>;
    }

    /// 将 token id 解码为文本的结果。
    ///
    /// 区分「完全合法的 UTF-8 输出」与「尾部含不完整多字节序列（以 U+FFFD 表示）的输出」。
    /// 这让 `DecodeStream::step()` 等调用方无需依赖硬编码的替换字符串比较，即可决定
    /// 立即输出还是先缓冲。
    #[derive(Debug, Clone, PartialEq, Eq, strum::EnumIs)]
    pub enum DecodeResult {
        /// 尾部不含不完整多字节序列（文本不以 U+FFFD 结尾）。
        /// 注意：字符串内部仍可能因中途的非法字节序列含有 U+FFFD，此处只跟踪尾部状态。
        Complete(String),
        /// 解码字符串以 U+FFFD 结尾，表明存在可能被后续 token 补全的不完整尾部多字节序列。
        Partial(String),
    }

    impl DecodeResult {
        /// 返回内部字符串的引用。
        pub fn as_str(&self) -> &str {
            match self {
                DecodeResult::Complete(s) | DecodeResult::Partial(s) => s,
            }
        }

        /// 由解码字符串构造：以 U+FFFD 结尾则为 `Partial`，否则为 `Complete`。
        pub fn from_decoded(text: String) -> Self {
            if text.ends_with('\u{FFFD}') {
                DecodeResult::Partial(text)
            } else {
                DecodeResult::Complete(text)
            }
        }
    }

    impl From<String> for DecodeResult {
        fn from(text: String) -> Self {
            DecodeResult::from_decoded(text)
        }
    }

    impl From<DecodeResult> for String {
        fn from(result: DecodeResult) -> Self {
            match result {
                DecodeResult::Complete(s) | DecodeResult::Partial(s) => s,
            }
        }
    }

    /// 实现者必须保证：残缺多字节序列在输出中产生 U+FFFD（`\u{FFFD}`）而非返回 `Err`。
    /// 通常通过 `String::from_utf8_lossy`（tiktoken）或库内置的字节回退处理（HuggingFace）
    /// 实现。`DecodeStream::step()` 依赖 `DecodeResult::Partial` 来检测不完整序列，并在完整
    /// 字符到达前缓冲 token。
    pub trait Decoder: Send + Sync {
        /// 将 token id 解码为文本。
        fn decode(
            &self,
            token_ids: &[TokenIdType],
            skip_special_tokens: bool,
        ) -> Result<DecodeResult>;
    }

    /// 同时具备编码与解码能力的分词器。
    pub trait Tokenizer: Encoder + Decoder {
        // fn get_vocab_size(&self) -> usize;
        // fn make_unique_clone(&self) -> Box<dyn Tokenizer>;
    }
}

// === SECTION: JSON 辅助工具 ===

/// 从 JSON 文件中读取并反序列化指定字段。
pub fn file_json_field<T: serde::de::DeserializeOwned>(
    json_file_path: &Path,
    field_name: &str,
) -> anyhow::Result<T> {
    let file = File::open(json_file_path)
        .with_context(|| format!("Failed to open file: {:?}", json_file_path))?;
    let reader = BufReader::new(file);

    let json_data: serde_json::Value = serde_json::from_reader(reader)
        .with_context(|| format!("Failed to parse JSON from file: {:?}", json_file_path))?;

    let map = json_data.as_object().ok_or_else(|| {
        anyhow::anyhow!("JSON root is not an object in file: {:?}", json_file_path)
    })?;

    let field_value = map.get(field_name).ok_or_else(|| {
        anyhow::anyhow!(
            "Field '{}' not found in JSON file: {:?}",
            field_name,
            json_file_path
        )
    })?;

    serde_json::from_value(field_value.clone()).with_context(|| {
        format!(
            "Failed to deserialize field '{}' (value: {:?}) to the expected type from file: {:?}",
            field_name, field_value, json_file_path
        )
    })
}

/// 解析 JSON 失败时，输出带行号与列指示的上下文错误日志。
pub fn log_json_err(filename: &str, json: &str, err: &serde_json::Error) {
    const ERROR_PREFIX: &str = ">>     ";

    if !(err.is_syntax() || err.is_data()) {
        return;
    }

    let line = err.line().saturating_sub(1);
    let column = err.column().saturating_sub(1);

    let json_lines: Vec<&str> = json.lines().collect();
    if json_lines.is_empty() {
        tracing::error!("JSON parsing error in {filename}: File is empty.");
        return;
    }

    let start_index = line.saturating_sub(2);
    let end_index = line.saturating_add(3).min(json_lines.len());

    let mut context_lines: Vec<String> = (start_index..end_index)
        .map(|i| {
            if i == line {
                format!("{ERROR_PREFIX}{}", json_lines[i])
            } else {
                format!("{:06} {}", i + 1, json_lines[i])
            }
        })
        .collect();

    let col_indicator = "_".to_string().repeat(column + ERROR_PREFIX.len()) + "^";
    let error_in_context_idx = line - start_index;
    if error_in_context_idx < context_lines.len() {
        context_lines.insert(error_in_context_idx + 1, col_indicator);
    }

    tracing::error!(
        "JSON parsing error in {filename}: Line {}, column {}:\n{}",
        err.line(),
        err.column(),
        context_lines.join("\n")
    );
}

impl Encoding {
    /// 基于 token id 序列计算稳定哈希值。
    pub fn get_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

// === SECTION: 分词器门面 ===

/// 主分词器封装，为不同后端实现提供统一接口。
#[derive(Clone)]
pub struct Tokenizer(Arc<dyn traits::Tokenizer>);

impl Tokenizer {
    /// 从分词器文件路径创建 [`Tokenizer`]。
    pub fn from_file(file_path: &str) -> Result<Tokenizer> {
        Ok(Tokenizer(create_tokenizer_from_file(file_path)?))
    }

    /// 创建有状态的序列对象，用于把 token id 流式解码为文本。
    pub fn decode_stream(
        &self,
        prompt_token_ids: &[TokenIdType],
        skip_special_tokens: bool,
    ) -> DecodeStream {
        DecodeStream::new(self.0.clone(), prompt_token_ids, skip_special_tokens)
    }
}

impl Deref for Tokenizer {
    type Target = Arc<dyn traits::Tokenizer>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Arc<dyn traits::Tokenizer>> for Tokenizer {
    fn from(tokenizer: Arc<dyn traits::Tokenizer>) -> Self {
        Tokenizer(tokenizer)
    }
}

impl<T> From<Arc<T>> for Tokenizer
where
    T: traits::Tokenizer + 'static, // 'static 用于保证 T 能被安全放入 Arc
{
    fn from(tokenizer: Arc<T>) -> Self {
        Tokenizer(tokenizer)
    }
}

/// 根据分词器文件路径创建分词器。
/// 由文件扩展名决定分词器类型，支持的类型有：
/// - json：HuggingFace 分词器
/// - model、tiktoken：tiktoken BPE 分词器（要求同目录下有携带受支持 `model_type` 的
///   `config.json`；当前支持：kimi、kimi_k2、kimi_k25）
pub fn create_tokenizer_from_file(file_path: &str) -> Result<Arc<dyn traits::Tokenizer>> {
    let path = Path::new(file_path);
    let extension = path
        .extension()
        .and_then(std::ffi::OsStr::to_str)
        .ok_or_else(|| Error::msg("Failed to read file extension".to_string()))?;

    match extension {
        "json" => {
            let tokenizer = HuggingFaceTokenizer::from_file(file_path)?;
            Ok(Arc::new(tokenizer))
        }
        "model" | "tiktoken" => {
            let tokenizer = TikTokenTokenizer::from_file_auto(file_path)?;
            Ok(Arc::new(tokenizer))
        }
        _ => Err(Error::msg(format!(
            "Unsupported tokenizer file type: .{extension}"
        ))),
    }
}

// === SECTION: 流式增量解码 ===

// 在增量去标记化时，处理首批解码 token 需要顾及上下文末尾的若干 token。
// 这是从上下文末尾向前回退、开始解码的初始偏移量。
// HuggingFace TGI 与 vLLM 均使用相同的取值。
// 参见：https://github.com/huggingface/text-generation-inference/blob/24c2bff65924801ddf90fa24fcc72752d4f45538/server/text_generation_server/models/mamba.py#L169
// 以及 https://github.com/vllm-project/vllm/blob/da2705198fa19030a25d0bea437f7be6547d47d4/vllm/transformers_utils/detokenizer_utils.py#L51
const INITIAL_INCREMENTAL_DETOKENIZATION_OFFSET: usize = 5;

/// DecodeStream 维护必要状态，能在 token id 输入流上逐块产出字符串片段。
///
/// 之所以需要它，是因为一般的解码无法独立完成这件事：字符串依赖周围的 id 才能形成
/// 合法结果（典型如去除多余空格）。
pub struct DecodeStream {
    /// 用于解码 token id 的分词器。
    tokenizer: Arc<dyn traits::Tokenizer>,

    skip_special_tokens: bool,
    /// 产生合法字符串片段所需 token id 的临时缓冲区。
    /// 它通常包含三部分：
    ///  - read（已读）
    ///  - prefix（前缀）
    ///  - rest（其余）
    ///
    /// read 是包住 prefix 所需的部分，使得对整段 id 解码能产出合法前缀。
    /// prefix 是上一次产出的字符串，保留它以便从下一个合法片段中裁掉。
    all_token_ids: Vec<u32>,

    prefix_offset: usize,

    read_offset: usize,
}

impl DecodeStream {
    /// 构造一个新的流式解码器，以给定提示 token 作为上下文。
    pub fn new(
        tokenizer: Arc<dyn traits::Tokenizer>,
        prompt_token_ids: &[TokenIdType],
        skip_special_tokens: bool,
    ) -> Self {
        let num_input_tokens = prompt_token_ids.len();
        let prompt_token_ids = prompt_token_ids.to_vec();
        Self {
            tokenizer,
            skip_special_tokens,
            all_token_ids: prompt_token_ids,
            prefix_offset: num_input_tokens
                .saturating_sub(INITIAL_INCREMENTAL_DETOKENIZATION_OFFSET),
            read_offset: num_input_tokens,
        }
    }

    /// step 将一个 token id 追加到内部状态，并尝试产出一个文本片段。
    ///
    /// 实现直接照搬自 HuggingFace 的 TGI：
    /// https://github.com/huggingface/text-generation-inference/blob/24c2bff65924801ddf90fa24fcc72752d4f45538/server/text_generation_server/models/model.py#L144
    ///
    /// 返回 `None` 表示给定 id 尚不足以产出片段。
    /// 这在 `byte_fallback` 选项下很常见：某些 token 并不构成合法 UTF-8，
    /// 只有后续 token id 才能帮助产出合法片段。
    pub fn step(&mut self, id: u32) -> Result<Option<String>> {
        self.all_token_ids.push(id);

        let prefix_text: String = self
            .tokenizer
            .decode(
                &self.all_token_ids[self.prefix_offset..self.read_offset],
                self.skip_special_tokens,
            )?
            .into();

        let new_result = self.tokenizer.decode(
            &self.all_token_ids[self.prefix_offset..],
            self.skip_special_tokens,
        )?;

        let new_text = new_result.as_str();
        if new_text.len() > prefix_text.len() && !new_result.is_partial() {
            let emitted = new_text[prefix_text.len()..].to_string();

            self.prefix_offset = self.read_offset;
            self.read_offset = self.all_token_ids.len();

            Ok(Some(emitted))
        } else {
            Ok(None)
        }
    }
}

// === SECTION: 序列状态 ===

/// 维护一段持续 token 序列及其解码文本的状态。
pub struct Sequence {
    /// 文本 -> token id 的编码器。
    tokenizer: Tokenizer,

    /// 当前的 token id 序列。
    token_ids: Vec<TokenIdType>,

    /// 上一个已解码 token 在当前序列中完成的位置。
    prefix_offset: usize,

    /// 当前在序列中的位置。
    read_offset: usize,
}

impl std::fmt::Debug for Sequence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sequence")
            .field("tokenizer", &"Arc<dyn Tokenizer>")
            .field(
                "token_ids",
                &format_args!("{}", {
                    let token_ids = self.token_ids();
                    if token_ids.len() <= 20 {
                        format!("{:?}", token_ids)
                    } else {
                        let first_ten = &token_ids[..10];
                        let last_ten = &token_ids[token_ids.len() - 10..];
                        format!("{:?} ... {:?}", first_ten, last_ten)
                    }
                }),
            )
            .field("prefix_offset", &self.prefix_offset)
            .field("read_offset", &self.read_offset)
            .field("token count", &self.token_ids.len())
            .finish()
    }
}

impl Sequence {
    pub fn new(tokenizer: Tokenizer) -> Self {
        Self {
            tokenizer,
            token_ids: Vec::new(),
            prefix_offset: 0,
            read_offset: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.token_ids.is_empty()
    }

    pub fn len(&self) -> usize {
        self.token_ids.len()
    }

    pub fn clear(&mut self) {
        self.token_ids.clear();
        self.prefix_offset = 0;
        self.read_offset = 0;
    }

    pub fn append_text(&mut self, input: &str) -> Result<()> {
        let encoding = self.tokenizer.encode(input)?;
        self.token_ids.extend(encoding.token_ids());
        Ok(())
    }

    // 移植自
    // https://github.com/huggingface/text-generation-inference/blob/v0.9.4/server/text_generation_server/models/model.py#L62C9-L62C15
    // 遵循 Apache 2.0 许可证
    pub fn append_token_id(&mut self, token_id: TokenIdType) -> Result<String> {
        self.token_ids.push(token_id);

        let prefix_text: String = self
            .tokenizer
            .decode(&self.token_ids[self.prefix_offset..self.read_offset], false)?
            .into();

        let new_result = self
            .tokenizer
            .decode(&self.token_ids[self.prefix_offset..], false)?;

        let new_text = new_result.as_str();

        // 如果上一次返回序列的末尾字符是多字节字符，
        // 则不能在该字节偏移处切分文本，需要回退到该字符起始处的字节偏移。
        let mut prefix_text_len = prefix_text.len();
        while !new_text.is_char_boundary(prefix_text_len) && prefix_text_len > 0 {
            prefix_text_len -= 1;
        }
        let prefix_text_len = prefix_text_len;

        if new_text.len() > prefix_text.len() {
            if new_result.is_partial() {
                return Ok("".to_string());
            } else {
                // 偏移并更新状态
                let new_text = new_text[prefix_text_len..]
                    .to_string()
                    .replace('\u{FFFD}', "");
                self.prefix_offset = self.read_offset;
                self.read_offset = self.token_ids.len();
                return Ok(new_text);
            }
        }

        Ok("".to_string())
    }

    pub fn tokenizer(&self) -> Tokenizer {
        self.tokenizer.clone()
    }

    pub fn token_ids(&self) -> &[TokenIdType] {
        &self.token_ids
    }

    pub fn text(&self) -> Result<String> {
        Ok(self.tokenizer.decode(&self.token_ids, false)?.into())
    }
}

// === SECTION: 停止序列检测 ===

/// `StopSequenceDecoder::append_token_id` 操作产出的输出条件/值。
/// 表示解码一个 token 的结果：要么产出文本，要么命中停止条件。
pub enum SequenceDecoderOutput {
    /// 追加 token_id 对应的文本。
    Text(String),

    /// 某个 token_id 序列已部分匹配某条停止序列，因此文本被暂存，
    /// 直到完全匹配或出现分歧。
    Held,

    /// 表示已匹配到停止序列且解码器已停止。
    /// 后续调用 append_token_id 将返回错误。
    Stopped,

    /// 表示已匹配到停止 token_id 且解码器已停止。
    /// 后续调用 append_token_id 将返回错误。
    /// 返回该停止 token_id 对应的文本。
    StoppedWithText(String),
}

/// 用于将 token id 流解码为文本并检测停止序列的序列解码器。
/// 停止序列既可以是匹配的 token_id，也可以是匹配的文本字符串序列。
/// 匹配先发生在 token 级，再发生在序列级。隐藏优先于可见——例如，若把同一个
/// token_id 同时放入 `stop_token_ids_visible` 与 `stop_token_ids_hidden`，
/// 则该 token_id 会被当作隐藏处理。
#[derive(Debug)]
pub struct StopSequenceDecoder {
    // 当前的 token id 序列
    sequence: Sequence,

    // 停止 token —— 出现其中任意一个都应触发停止
    // 命中后会返回匹配 token 对应的文本
    stop_token_ids_visible: Vec<TokenIdType>,

    // 停止 token —— 出现其中任意一个都应触发停止
    // 命中后不会返回匹配 token 对应的文本
    stop_token_ids_hidden: Vec<TokenIdType>,

    // 停止词 —— 出现其中任意一个都应触发停止
    // 命中后会返回匹配文本
    #[allow(dead_code)]
    stop_sequences_visible: Vec<String>,

    // 停止词 —— 出现其中任意一个都应触发停止
    // 命中后不会返回匹配文本
    stop_sequences_hidden: Vec<String>,

    // 若解码器已观察到并返回过停止类 SequenceDecoderOutput，
    // 则后续调用 append_token_id 将返回错误
    stopped: bool,

    // 文本暂存区 —— 若正在观察一个部分匹配的停止序列，则暂存文本，
    // 直到停止序列被完全匹配，或因出现分歧而重置序列
    state: String,
}

impl StopSequenceDecoder {
    /// 用于配置 StopSequenceDecoder 的构建器对象。
    pub fn builder(tokenizer: Tokenizer) -> StopSequenceDecoderBuilder {
        StopSequenceDecoderBuilder::new(tokenizer)
    }

    /// 向序列追加一个 token_id 并返回 SequenceDecoderOutput。
    pub fn append_token_id(&mut self, token_id: TokenIdType) -> Result<SequenceDecoderOutput> {
        if self.stopped {
            return Err(Error::msg("Decoder is stopped"));
        }

        // 更新序列
        let text = self.sequence.append_token_id(token_id)?;

        // 将文本追加到状态
        self.state.push_str(text.as_str());

        let mut stop: bool = false;
        let mut visible: bool = false;

        if self.stop_token_ids_visible.contains(&token_id) {
            stop = true;
            visible = true;
        }

        if self.stop_token_ids_hidden.contains(&token_id) {
            stop = true;
            visible = false;
        }

        if stop {
            self.stopped = true;
            let state = std::mem::take(&mut self.state);
            if visible {
                return Ok(SequenceDecoderOutput::StoppedWithText(state));
            }
            return Ok(SequenceDecoderOutput::Stopped);
        }

        // 判断当前状态是否匹配任一停止序列
        for stop_sequence in self.stop_sequences_hidden.iter() {
            if stop_sequence.starts_with(&self.state) {
                if stop_sequence == &self.state {
                    // 命中停止序列时，不返回被暂存的停止序列文本
                    self.stopped = true;
                    return Ok(SequenceDecoderOutput::Stopped);
                } else {
                    return Ok(SequenceDecoderOutput::Held);
                }
            }
        }

        let state = std::mem::take(&mut self.state);
        Ok(SequenceDecoderOutput::Text(state))
    }

    pub fn is_empty(&self) -> bool {
        self.sequence.token_ids.is_empty()
    }

    pub fn len(&self) -> usize {
        self.sequence.token_ids.len()
    }

    pub fn is_complete(&self) -> bool {
        self.stopped
    }

    pub fn close(&mut self) {
        self.stopped = true;
    }
}

pub struct StopSequenceDecoderBuilder {
    tokenizer: Tokenizer,
    stop_token_ids_visible: Vec<TokenIdType>,
    stop_token_ids_hidden: Vec<TokenIdType>,
    stop_sequences_visible: Vec<String>,
    stop_sequences_hidden: Vec<String>,
}

impl StopSequenceDecoderBuilder {
    /// 创建一个新的构建器。
    pub fn new(tokenizer: Tokenizer) -> Self {
        Self {
            tokenizer,
            stop_token_ids_visible: Vec::new(),
            stop_token_ids_hidden: Vec::new(),
            stop_sequences_visible: Vec::new(),
            stop_sequences_hidden: Vec::new(),
        }
    }

    /// 向 StopSequenceDecoder 添加一个可见停止 token id。
    pub fn add_stop_token_id_visible(mut self, token_id: TokenIdType) -> Self {
        self.stop_token_ids_visible.push(token_id);
        self
    }

    /// 向 StopSequenceDecoder 添加一组可见停止 token id。
    /// 每个 token_id 都按单独匹配项加入。
    pub fn add_stop_token_ids_visible(mut self, token_ids: &[TokenIdType]) -> Self {
        self.stop_token_ids_visible.extend(token_ids);
        self
    }

    /// 向 StopSequenceDecoder 添加一个隐藏停止 token id。
    pub fn add_stop_token_id_hidden(mut self, token_id: TokenIdType) -> Self {
        self.stop_token_ids_hidden.push(token_id);
        self
    }

    /// 向 StopSequenceDecoder 添加一组隐藏停止 token id。
    /// 每个 token_id 都按单独匹配项加入。
    pub fn add_stop_token_ids_hidden(mut self, token_ids: &[TokenIdType]) -> Self {
        self.stop_token_ids_hidden.extend(token_ids);
        self
    }

    /// 添加一条可见停止序列文本。
    pub fn add_stop_sequence_visible(mut self, text: &str) -> Self {
        self.stop_sequences_visible.push(text.to_string());
        self
    }

    /// 添加一组可见停止序列文本。
    pub fn add_stop_sequences_visible(mut self, strings: &[&str]) -> Self {
        self.stop_sequences_visible
            .extend(strings.iter().map(|text| text.to_string()));
        self
    }

    /// 添加一条隐藏停止序列文本。
    pub fn add_stop_sequence_hidden(mut self, text: &str) -> Self {
        self.stop_sequences_hidden.push(text.to_string());
        self
    }

    /// 添加一组隐藏停止序列文本。
    pub fn add_stop_sequences_hidden(mut self, strings: &[&str]) -> Self {
        self.stop_sequences_hidden
            .extend(strings.iter().map(|text| text.to_string()));
        self
    }

    /// 构建最终的 StopSequenceDecoder。
    pub fn build(self) -> Result<StopSequenceDecoder> {
        Ok(StopSequenceDecoder {
            sequence: Sequence::new(self.tokenizer.clone()),
            stop_token_ids_visible: self.stop_token_ids_visible,
            stop_token_ids_hidden: self.stop_token_ids_hidden,
            stop_sequences_visible: self.stop_sequences_visible,
            stop_sequences_hidden: self.stop_sequences_hidden,
            stopped: false,
            state: String::new(),
        })
    }
}

// === SECTION: 测试 ===

#[cfg(test)]
mod tests {
    //! 门面层（lib.rs）的单一统一测试模块。
    //!
    //! ## 测试过程
    //! 本模块作为门面层公开契约的回归基线，覆盖 `DecodeResult` 状态判定、`Encoding` 哈希、分词器分派、
    //! JSON 字段读取、`Sequence` 增量解码以及停止序列检测等重写路径。
    //!
    //! ## 意义
    //! 保证门面层公开 API 与可观测行为在重写后保持稳定，防止增量解码与停止检测逻辑回退。

    use super::*;

    /// 指向最小 BPE 分词器的测试夹具路径。
    const TOKENIZER_PATH: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../llm/tests/data/sample-models/minimal-bpe/tokenizer.json"
    );

    /// 构建一个可用于测试的门面分词器。
    fn build_tokenizer() -> Tokenizer {
        Tokenizer::from_file(TOKENIZER_PATH).unwrap()
    }

    #[test]
    fn test_decode_result_complete_vs_partial() {
        //! ## 测试过程
        //! 分别用普通字符串与以 U+FFFD 结尾的字符串构造 `DecodeResult`，
        //! 检查 `from_decoded` 的分类、`is_partial`/`is_complete` 判定以及 `as_str`。
        //!
        //! ## 意义
        //! `DecodeStream::step` 依赖该分类决定立即输出还是缓冲，必须保持准确。
        let complete = DecodeResult::from_decoded("hello".to_string());
        assert!(complete.is_complete());
        assert_eq!(complete.as_str(), "hello");

        let partial = DecodeResult::from_decoded("hel\u{FFFD}".to_string());
        assert!(partial.is_partial());
        assert_eq!(partial.as_str(), "hel\u{FFFD}");
    }

    #[test]
    fn test_decode_result_string_roundtrip() {
        //! ## 测试过程
        //! 验证 `From<String>` 与 `From<DecodeResult> for String` 互逆。
        //!
        //! ## 意义
        //! 保证字符串与解码结果之间的转换不丢失内容。
        let original = "round-trip".to_string();
        let result: DecodeResult = original.clone().into();
        let back: String = result.into();
        assert_eq!(original, back);
    }

    #[test]
    fn test_encoding_token_ids_and_hash_determinism() {
        //! ## 测试过程
        //! 构造两个相同 id 的 `Encoding::Sp`，比较 `token_ids` 与 `get_hash`；
        //! 再构造不同序列以确认哈希区分能力。
        //!
        //! ## 意义
        //! `get_hash` 用于缓存/去重，必须对相同序列稳定、对不同序列可区分。
        let a = Encoding::Sp(vec![1, 2, 3]);
        let b = Encoding::Sp(vec![1, 2, 3]);
        let c = Encoding::Sp(vec![3, 2, 1]);

        assert_eq!(a.token_ids(), &[1, 2, 3]);
        assert_eq!(a.get_hash(), b.get_hash());
        assert_ne!(a.get_hash(), c.get_hash());
    }

    #[test]
    fn test_create_tokenizer_unsupported_extension() {
        //! ## 测试过程
        //! 以非法扩展名调用 `create_tokenizer_from_file`，断言返回错误且消息匹配。
        //!
        //! ## 意义
        //! 扩展名分派是门面层的对外契约，错误路径必须保持稳定。
        let result = create_tokenizer_from_file("/tmp/model.unknown");
        let err = match result {
            Ok(_) => panic!("expected error for unsupported extension"),
            Err(e) => e,
        };
        assert!(
            err.to_string()
                .contains("Unsupported tokenizer file type: .unknown")
        );
    }

    #[test]
    fn test_create_tokenizer_json_dispatch() {
        //! ## 测试过程
        //! 以 `.json` 文件创建分词器并执行一次编码，确认分派到 HuggingFace 后端。
        //!
        //! ## 意义
        //! 验证扩展名 → 后端的映射对 HuggingFace 路径有效。
        let tokenizer = create_tokenizer_from_file(TOKENIZER_PATH).unwrap();
        let encoding = tokenizer.encode("hello world").unwrap();
        assert!(!encoding.token_ids().is_empty());
    }

    #[test]
    fn test_file_json_field_reads_field() {
        //! ## 测试过程
        //! 从 minimal-bpe 的 tokenizer.json 中读取顶层 `version` 字段。
        //!
        //! ## 意义
        //! `file_json_field` 是 tiktoken/HF 配置读取的通用工具，需正确反序列化字段。
        let version: String =
            file_json_field(Path::new(TOKENIZER_PATH), "version").unwrap();
        assert!(!version.is_empty());
    }

    #[test]
    fn test_file_json_field_missing_field_errors() {
        //! ## 测试过程
        //! 读取一个不存在的字段，断言返回错误。
        //!
        //! ## 意义
        //! 缺失字段必须以错误而非 panic 形式暴露，保证调用方可处理。
        let result: Result<String> =
            file_json_field(Path::new(TOKENIZER_PATH), "__not_a_real_field__");
        assert!(result.is_err());
    }

    #[test]
    fn test_sequence_append_text_and_clear() {
        //! ## 测试过程
        //! 向 `Sequence` 追加文本后检查非空与长度，clear 后检查重置为空。
        //!
        //! ## 意义
        //! `Sequence` 的状态管理是流式解码的基础，append/clear 必须正确维护 token 列表。
        let tokenizer = build_tokenizer();
        let mut seq = Sequence::new(tokenizer);
        assert!(seq.is_empty());

        seq.append_text("hello").unwrap();
        assert!(!seq.is_empty());
        assert!(seq.len() > 0);

        seq.clear();
        assert!(seq.is_empty());
        assert_eq!(seq.len(), 0);
    }

    #[test]
    fn test_sequence_append_token_id_roundtrip_text() {
        //! ## 测试过程
        //! 先把 "hello world" 编码成 token id，再逐个 `append_token_id`，
        //! 拼接增量片段并与 `text()` 的整体解码结果比较。
        //!
        //! ## 意义
        //! 验证增量解码（prefix/read 双偏移 + 字符边界回退）拼出的文本与一次性解码一致。
        let tokenizer = build_tokenizer();
        let ids = tokenizer.encode("hello world").unwrap().token_ids().to_vec();

        let mut seq = Sequence::new(tokenizer);
        let mut incremental = String::new();
        for id in &ids {
            incremental.push_str(&seq.append_token_id(*id).unwrap());
        }

        assert_eq!(incremental, seq.text().unwrap());
        assert_eq!(seq.token_ids(), ids.as_slice());
    }

    #[test]
    fn test_decode_stream_matches_full_decode() {
        //! ## 测试过程
        //! 用空上下文创建 `DecodeStream`，逐个 step 喂入 token id，
        //! 拼接产出的片段并与分词器整体解码结果比较。
        //!
        //! ## 意义
        //! `DecodeStream` 是对外的流式解码入口，其增量结果须与整体解码一致。
        let tokenizer = build_tokenizer();
        let ids = tokenizer.encode("incremental decode").unwrap().token_ids().to_vec();

        let mut stream = tokenizer.decode_stream(&[], false);
        let mut streamed = String::new();
        for id in &ids {
            if let Some(chunk) = stream.step(*id).unwrap() {
                streamed.push_str(&chunk);
            }
        }

        let full: String = tokenizer.decode(&ids, false).unwrap().into();
        assert_eq!(streamed, full);
    }

    #[test]
    fn test_stop_decoder_visible_stop_token_returns_text() {
        //! ## 测试过程
        //! 配置一个可见停止 token，喂入该 token，断言返回 `StoppedWithText` 且解码器停止。
        //!
        //! ## 意义
        //! 可见停止 token 需在停止时仍返回其文本，并阻止后续追加。
        let tokenizer = build_tokenizer();
        let stop_id = tokenizer.encode("world").unwrap().token_ids()[0];

        let mut decoder = StopSequenceDecoder::builder(tokenizer)
            .add_stop_token_id_visible(stop_id)
            .build()
            .unwrap();

        let output = decoder.append_token_id(stop_id).unwrap();
        assert!(matches!(output, SequenceDecoderOutput::StoppedWithText(_)));
        assert!(decoder.is_complete());
        assert!(decoder.append_token_id(stop_id).is_err());
    }

    #[test]
    fn test_stop_decoder_hidden_stop_token_suppresses_text() {
        //! ## 测试过程
        //! 配置一个隐藏停止 token，喂入后断言返回 `Stopped`（不含文本）。
        //!
        //! ## 意义
        //! 隐藏停止 token 命中时必须抑制其文本输出，区别于可见停止 token。
        let tokenizer = build_tokenizer();
        let stop_id = tokenizer.encode("world").unwrap().token_ids()[0];

        let mut decoder = StopSequenceDecoder::builder(tokenizer)
            .add_stop_token_id_hidden(stop_id)
            .build()
            .unwrap();

        let output = decoder.append_token_id(stop_id).unwrap();
        assert!(matches!(output, SequenceDecoderOutput::Stopped));
        assert!(decoder.is_complete());
    }
}
