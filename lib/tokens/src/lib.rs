// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! token 序列的类型与工具：分块、哈希以及位置感知派生哈希。
//!
//! ## 设计意图
//! 把一串 token 切成定长的「块」，为每个块计算可用于前缀缓存命中的多种哈希；
//! 并将「位置」信息打包进哈希里，得到既能定位又能比较的紧凑标识。
//!
//! ## 外部契约
//! - 基础别名：[`Token`]、[`Salt`]、[`SaltHash`]、[`BlockHash`]、[`SequenceHash`]。
//! - 哈希函数：[`compute_hash_v2`]（xxh3 带种子）。
//! - 位置感知哈希：[`PositionalSequenceHash`]、[`PositionalLineageHash`]，二者均实现 [`PositionalHash`]。
//! - 序列容器：[`Tokens`]、[`PartialTokenBlock`]、[`TokenBlock`]、[`TokenBlockSequence`]。
//! - 子模块：[`blocks`]（块身份）与基数树 [`PositionalRadixTree`]。
//!
//! ## 实现要点
//! 位置感知哈希采用「模式表」驱动的位打包方案，按位置大小自动选择位宽组合；
//! 128 位标识通过定长大端字节进行序列化，确保跨 msgpack/JSON 等格式稳定往返。

use bytemuck::cast_slice;
use derive_getters::Dissolve;
use std::ops::Range;

pub mod blocks;
mod radix;
pub use radix::PositionalRadixTree;

// === SECTION: 位置感知 trait ===

/// 携带位置信息的哈希所实现的 trait。
pub trait PositionalHash {
    /// 返回该哈希所对应的位置。
    fn position(&self) -> u64;
}

// === SECTION: 基础类型别名 ===

/// token 以 32 位无符号整数表示。
pub type Token = u32;

/// 用于哈希的盐，表示为字节向量。
/// 可用于编码模型结构、权重、PEFT 等信息。
pub type Salt = Vec<u8>;

/// 盐的 64 位哈希，由 [`compute_hash_v2`] 以种子 0 计算得到。
/// 作为后续块哈希的初始种子。
pub type SaltHash = u64;

/// 仅由单个块内 token 计算出的 64 位哈希。
/// 以 [`SaltHash`] 作为种子调用 [`compute_hash_v2`]。
pub type BlockHash = u64;

/// 序列感知的 64 位哈希。
/// 由上一块的 [`SequenceHash`]（首块则为 [`SaltHash`]）与当前块的 [`BlockHash`]
/// 经 [`compute_hash_v2`]（同样以 [`SaltHash`] 为种子）组合而成。
pub type SequenceHash = u64;

// === SECTION: 哈希函数 ===

/// 以给定种子计算数据的哈希。
pub fn compute_hash_v2(data: &[u8], seed: u64) -> u64 {
    xxhash_rust::xxh3::xxh3_64_with_seed(data, seed)
}

// === SECTION: 位运算助手 ===

/// 取低 `bits` 位的 64 位掩码（要求 `bits < 64`）。
#[inline(always)]
fn low_mask_u64(bits: u32) -> u64 {
    (1u64 << bits) - 1
}

/// 取低 `bits` 位的 128 位掩码（要求 `bits < 128`）。
#[inline(always)]
fn low_mask_u128(bits: u32) -> u128 {
    (1u128 << bits) - 1
}

// === SECTION: u128 的字节序列编解码 ===

/// 把 `u128` 编码为 16 字节大端序列的自定义 serde 编解码器。
///
/// ## 设计意图
/// MessagePack（`rmp-serde`）没有原生的 128 位整数类型，直接 derive `u128` 无法稳定往返。
/// 改用定长字节序列后，可在 msgpack、JSON、CBOR 等格式间一致地序列化与反序列化。
///
/// ## 实现要点
/// 序列化时写出 `to_be_bytes`；反序列化时既接受 msgpack 的二进制块，也接受由 16 个 `u8`
/// 组成的序列（例如 JSON 数组），二者都还原为同一个 `u128`。
mod serde_bytes_u128 {
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(val: &u128, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&val.to_be_bytes())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<u128, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::{self, SeqAccess, Visitor};
        use std::fmt;

        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = [u8; 16];

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("16 bytes (msgpack bin) or a sequence of 16 u8 values")
            }

            fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<[u8; 16], E> {
                v.try_into()
                    .map_err(|_| E::invalid_length(v.len(), &"16 bytes"))
            }

            fn visit_borrowed_bytes<E: de::Error>(self, v: &'de [u8]) -> Result<[u8; 16], E> {
                self.visit_bytes(v)
            }

            fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<[u8; 16], E> {
                self.visit_bytes(&v)
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<[u8; 16], A::Error> {
                let mut arr = [0u8; 16];
                for (i, slot) in arr.iter_mut().enumerate() {
                    *slot = seq
                        .next_element()?
                        .ok_or_else(|| de::Error::invalid_length(i, &"16 u8 elements"))?;
                }
                Ok(arr)
            }
        }

        let arr = deserializer.deserialize_bytes(V)?;
        Ok(u128::from_be_bytes(arr))
    }
}

// === SECTION: 位置序列哈希（PositionalSequenceHash） ===

/// 把传统序列哈希与位置信息合二为一的 128 位位置序列哈希。
///
/// ## 设计意图
/// 在单个 128 位整数里同时容纳「序列哈希」「位置」「局部块哈希片段」，
/// 既能直接当作映射键，又能从中还原出位置以供路由。
///
/// ## 外部契约
/// 布局：
/// - 低 64 位：传统 [`SequenceHash`]。
/// - 高 64 位：2 位 mode + 位置 + 局部块哈希（LBH）片段。
///
/// 模式（按位置自动选取最省的表示）：
/// - Mode 00：8 位位置（≤255）+ 54 位 LBH
/// - Mode 01：16 位位置（≤65,535）+ 46 位 LBH
/// - Mode 10：24 位位置（≤16,777,215）+ 38 位 LBH
/// - Mode 11：31 位位置（≤2,147,483,647）+ 31 位 LBH
///
/// ## 实现要点
/// 各模式的位宽集中在 [`Self::LAYOUT`] 表中，打包/解包共用同一张表，避免 match 分支重复。
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct PositionalSequenceHash(#[serde(with = "serde_bytes_u128")] u128);

impl PositionalSequenceHash {
    /// 每个模式对应的 `(位置位宽, LBH 位宽)`；2 位 mode 前缀使三者恰好补满 64 位。
    const LAYOUT: [(u32, u32); 4] = [(8, 54), (16, 46), (24, 38), (31, 31)];

    /// 由各组成部分构造一个 `PositionalSequenceHash`。
    ///
    /// 模式会依据位置值自动选取，使用能容纳该位置的最小表示。
    pub fn new(sequence_hash: SequenceHash, position: u64, local_block_hash: BlockHash) -> Self {
        let mode = Self::select_mode(position);
        let upper = Self::pack_upper(mode, position, local_block_hash);
        PositionalSequenceHash(((upper as u128) << 64) | (sequence_hash as u128))
    }

    /// 返回序列哈希分量（低 64 位）。
    pub fn sequence_hash(&self) -> SequenceHash {
        self.0 as u64
    }

    /// 返回块位置。
    pub fn position(&self) -> u64 {
        self.unpack_upper().1
    }

    /// 返回局部块哈希（LBH）分量。
    pub fn local_block_hash(&self) -> BlockHash {
        self.unpack_upper().2
    }

    /// 返回编码所用的模式（0、1、2 或 3）。
    pub fn mode(&self) -> u8 {
        self.unpack_upper().0
    }

    /// 返回内部 128 位原始值。
    #[inline(always)]
    pub fn as_u128(&self) -> u128 {
        self.0
    }

    /// 选取能表示给定位置的最小模式。
    fn select_mode(position: u64) -> u8 {
        // 与 LAYOUT 中各位置位宽一一对应的上界，逐级比较。
        for (mode, &(position_bits, _)) in Self::LAYOUT.iter().enumerate() {
            if position < (1u64 << position_bits) {
                return mode as u8;
            }
        }
        panic!(
            "Position {} exceeds maximum supported value (2^31 - 1)",
            position
        );
    }

    /// 由 mode、位置与局部块哈希打包出高 64 位：`[mode:2][position:X][lbh:R]`。
    fn pack_upper(mode: u8, position: u64, local_block_hash: u64) -> u64 {
        let (position_bits, lbh_bits) = Self::LAYOUT[mode as usize];
        let position_part = (position & low_mask_u64(position_bits)) << lbh_bits;
        let lbh_part = local_block_hash & low_mask_u64(lbh_bits);
        ((mode as u64) << 62) | position_part | lbh_part
    }

    /// 把高 64 位解包为 `(mode, 位置, 局部块哈希)`。
    fn unpack_upper(&self) -> (u8, u64, u64) {
        let upper = (self.0 >> 64) as u64;
        let mode = (upper >> 62) as u8;
        let (position_bits, lbh_bits) = Self::LAYOUT[mode as usize];
        let lbh = upper & low_mask_u64(lbh_bits);
        let position = (upper >> lbh_bits) & low_mask_u64(position_bits);
        (mode, position, lbh)
    }
}

impl std::fmt::Debug for PositionalSequenceHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PositionalSequenceHash")
            .field("sequence_hash", &self.sequence_hash())
            .field("local_block_hash", &self.local_block_hash())
            .field("position", &self.position())
            .finish()
    }
}

// === SECTION: 位置血缘哈希（PositionalLineageHash） ===

/// 编码父子血缘、用于树形回溯的 128 位位置血缘哈希。
///
/// ## 设计意图
/// 在一个 128 位整数里同时记录「位置」「父序列哈希片段」「当前序列哈希片段」，
/// 使得在基数树里可以通过「位置 N 的当前片段 == 位置 N+1 的父片段」实现反向回溯。
///
/// ## 外部契约
/// 布局（占满 128 位）：
/// - mode（2 位）：决定位置字段宽度。
/// - 位置（8/16/24 位）：块在序列中的位置。
/// - 父片段（可变位宽）：父序列哈希的低位片段。
/// - 当前片段（可变位宽）：当前序列哈希的低位片段。
///
/// 模式（按位置自动选取）：
/// - Mode 00：8 位位置（≤255）+ 59 位父 + 59 位当前
/// - Mode 01：16 位位置（≤65,535）+ 55 位父 + 55 位当前
/// - Mode 10：24 位位置（≤16,777,215）+ 51 位父 + 51 位当前
///
/// ## 实现要点
/// 构造时把当前片段对齐到「下一位置模式」的父片段位宽，保证跨模式边界处片段仍互为子集。
/// 各模式位宽集中在 [`Self::LAYOUT`] 表中。
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct PositionalLineageHash(#[serde(with = "serde_bytes_u128")] u128);

impl PositionalLineageHash {
    /// 每个模式对应的 `(位置位宽, 父位宽, 当前位宽)`；连同 2 位 mode 前缀恰好补满 128 位。
    const LAYOUT: [(u32, u32, u32); 3] = [(8, 59, 59), (16, 55, 55), (24, 51, 51)];

    /// 由各组成部分构造一个 `PositionalLineageHash`。
    ///
    /// 模式依据位置自动选取，使用能容纳该位置的最小表示。
    ///
    /// # Arguments
    ///
    /// * `current_seq_hash` - 当前块的序列哈希
    /// * `parent_seq_hash` - 父块的序列哈希（根块为 None）
    /// * `position` - 块在序列中的位置
    ///
    /// # Panics
    ///
    /// 当 position >= 2^24（16,777,216）时 panic。
    pub fn new(
        current_seq_hash: SequenceHash,
        parent_seq_hash: Option<SequenceHash>,
        position: u64,
    ) -> Self {
        if position >= (1u64 << 24) {
            panic!(
                "Position {} exceeds maximum supported value (2^24 - 1 = 16,777,215)",
                position
            );
        }

        let mode = Self::select_mode(position);
        let (position_bits, parent_bits, current_bits) = Self::LAYOUT[mode as usize];

        // 关键：为跨模式边界匹配，把当前片段对齐到「下一位置」可用的父片段位宽，
        // 这样当 position+1 把我们的当前片段当作父片段存储时，两者必然吻合。
        let next_mode = Self::select_mode(position + 1);
        let next_parent_bits = Self::LAYOUT[next_mode as usize].1;
        let aligned_current_bits = current_bits.min(next_parent_bits);

        // 片段一律低位对齐，以保证跨模式时小片段是大片段的子集。
        let position_part = (position as u128) & low_mask_u128(position_bits);
        let parent_part = (parent_seq_hash.unwrap_or(0) as u128) & low_mask_u128(parent_bits);
        let current_part = (current_seq_hash as u128) & low_mask_u128(aligned_current_bits);

        // 打包：[mode(2)][position(P)][parent(M)][current(N)]。
        // 注意布局仍按 current_bits 预留空间，但只写入 aligned_current_bits 位有效数据。
        let value = ((mode as u128) << 126)
            | (position_part << (parent_bits + current_bits))
            | (parent_part << current_bits)
            | current_part;

        PositionalLineageHash(value)
    }

    /// 返回块位置。
    pub fn position(&self) -> u64 {
        let (position_bits, parent_bits, current_bits) = Self::LAYOUT[self.mode() as usize];
        ((self.0 >> (parent_bits + current_bits)) & low_mask_u128(position_bits)) as u64
    }

    /// 返回当前序列哈希片段。
    pub fn current_hash_fragment(&self) -> u64 {
        let (_, _, current_bits) = Self::LAYOUT[self.mode() as usize];
        (self.0 & low_mask_u128(current_bits)) as u64
    }

    /// 返回父序列哈希片段。
    pub fn parent_hash_fragment(&self) -> u64 {
        let (_, parent_bits, current_bits) = Self::LAYOUT[self.mode() as usize];
        ((self.0 >> current_bits) & low_mask_u128(parent_bits)) as u64
    }

    /// 返回编码所用的模式（0、1 或 2）。
    pub fn mode(&self) -> u8 {
        (self.0 >> 126) as u8
    }

    /// 返回内部 128 位原始值。
    #[inline(always)]
    pub fn as_u128(&self) -> u128 {
        self.0
    }

    /// 选取能表示给定位置的最小模式。
    fn select_mode(position: u64) -> u8 {
        match position {
            p if p < (1u64 << 8) => 0,
            p if p < (1u64 << 16) => 1,
            _ => 2,
        }
    }
}

// === SECTION: 血缘哈希的格式化与排序 ===

impl PositionalLineageHash {
    fn format_impl(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let position = self.position();
        let current_hash = self.current_hash_fragment();
        let current_hash_b58 = bs58::encode(current_hash.to_be_bytes()).into_string();

        if position == 0 {
            write!(f, "{}:{}", position, current_hash_b58)
        } else {
            let parent_hash = self.parent_hash_fragment();
            let parent_hash_b58 = bs58::encode(parent_hash.to_be_bytes()).into_string();
            write!(f, "{}:{}:{}", position, current_hash_b58, parent_hash_b58)
        }
    }
}

impl std::fmt::Debug for PositionalLineageHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.format_impl(f)
    }
}

impl std::fmt::Display for PositionalLineageHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.format_impl(f)
    }
}

impl std::cmp::PartialOrd for PositionalLineageHash {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::cmp::Ord for PositionalLineageHash {
    /// 字典序：先比 [`Self::position`]，再比 [`Self::current_hash_fragment`]，
    /// 最后比完整打包值 [`Self::as_u128`]，确保是与 [`Eq`] 一致的全序。
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.position()
            .cmp(&other.position())
            .then_with(|| {
                self.current_hash_fragment()
                    .cmp(&other.current_hash_fragment())
            })
            .then_with(|| self.0.cmp(&other.0))
    }
}

// === SECTION: Tokens 容器 ===

/// 一组 token，内部以 `Vec<Token>` 表示。
///
/// ## 设计意图
/// 在裸 `Vec<Token>` 之上提供一层薄封装，附带各类转换与切片访问的便捷实现，
/// 同时可无缝地与切片、向量进行相等比较。
///
/// ## 外部契约
/// 实现 `AsRef`/`Deref`/`Borrow` 为 `[Token]`，以及与 `Vec<Token>`、`&[Token]`、`Vec<usize>`、
/// `Vec<i32>`、`&[i32]` 之间的 `From` 转换；提供 [`Tokens::into_sequence`] 切块入口。
#[derive(Debug, Clone, Dissolve, Default, Eq)]
pub struct Tokens(Vec<Token>);

impl AsRef<[Token]> for Tokens {
    fn as_ref(&self) -> &[Token] {
        &self.0
    }
}

impl std::ops::Deref for Tokens {
    type Target = [Token];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::borrow::Borrow<[Token]> for Tokens {
    fn borrow(&self) -> &[Token] {
        &self.0
    }
}

impl From<Vec<Token>> for Tokens {
    fn from(tokens: Vec<Token>) -> Self {
        Tokens(tokens)
    }
}

impl From<&[Token]> for Tokens {
    fn from(tokens: &[Token]) -> Self {
        Tokens(tokens.to_vec())
    }
}

impl From<Vec<usize>> for Tokens {
    fn from(tokens: Vec<usize>) -> Self {
        Tokens(
            tokens
                .into_iter()
                .map(|t| t.try_into().expect("Token ID exceeds u32::MAX"))
                .collect(),
        )
    }
}

impl From<Vec<i32>> for Tokens {
    /// 将 `Vec<i32>` 转换为 `Tokens`，并把每个 `i32` 转为 `u32`。
    fn from(tokens: Vec<i32>) -> Self {
        Tokens(tokens.into_iter().map(|t| t as u32).collect())
    }
}

impl From<&[i32]> for Tokens {
    /// 将 `&[i32]` 转换为 `Tokens`，并把每个 `i32` 转为 `u32`。
    fn from(tokens: &[i32]) -> Self {
        Tokens(tokens.iter().map(|&t| t as u32).collect())
    }
}

impl From<Tokens> for Vec<Token> {
    fn from(tokens: Tokens) -> Self {
        tokens.0
    }
}

// 用于让 `Tokens` 与 `Vec<Token>`、`&[Token]` 直接比较相等。
// 显式实现比派生实现更便于控制比较语义。
impl PartialEq<Vec<Token>> for Tokens {
    fn eq(&self, other: &Vec<Token>) -> bool {
        self.0 == *other
    }
}

impl PartialEq<Tokens> for Vec<Token> {
    fn eq(&self, other: &Tokens) -> bool {
        *self == other.0
    }
}

impl PartialEq<[Token]> for Tokens {
    fn eq(&self, other: &[Token]) -> bool {
        self.0.as_slice() == other
    }
}

impl PartialEq<Tokens> for &[Token] {
    fn eq(&self, other: &Tokens) -> bool {
        *self == other.0.as_slice()
    }
}

impl PartialEq for Tokens {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

// 也可以更泛化地实现 `PartialEq<&[T]>`，但这里针对 `&[Token]` 已足够。
impl PartialEq<&[Token]> for Tokens {
    fn eq(&self, other: &&[Token]) -> bool {
        self.0.as_slice() == *other
    }
}

impl Tokens {
    fn with_capacity(capacity: usize) -> Self {
        Tokens(Vec::with_capacity(capacity))
    }

    /// 消费 [`Tokens`] 并构造一个 [`TokenBlockSequence`]。
    ///
    /// 序列以给定 token 初始化，按 `block_size` 切块，使用给定的 `salt_hash`（`None` 时为 0）。
    ///
    /// # Arguments
    ///
    /// * `block_size` - 每个 [`TokenBlock`] 的固定大小。
    /// * `salt_hash` - 可选的哈希基种子 [`SaltHash`]，默认 0。
    pub fn into_sequence(self, block_size: u32, salt_hash: Option<SaltHash>) -> TokenBlockSequence {
        TokenBlockSequence::new(self, block_size, salt_hash)
    }
}

// === SECTION: 块操作错误 ===

/// [`PartialTokenBlock`] 操作期间可能发生的错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TokenBlockError {
    /// 块已满，操作无法完成。
    #[error("TokenBlock is full")]
    Full,

    /// 操作要求一个完整块，但当前块尚未填满。
    #[error("TokenBlock is incomplete")]
    Incomplete,

    /// 块为空，操作无法完成。
    #[error("TokenBlock is empty")]
    Empty,

    /// 操作所需的 token 数超过了块中现有数量。
    #[error("TokenBlock has insufficient tokens")]
    InsufficientTokens,
}

// === SECTION: 局部块 PartialTokenBlock ===

/// 序列中一个尚未填满的 token 块。
///
/// ## 设计意图
/// 作为「正在累积」的块缓冲区，不断吸纳 token，直到达到 `block_size` 后即可
/// [`commit`](PartialTokenBlock::commit) 为一个完整的 [`TokenBlock`]。
///
/// ## 实现要点
/// 不实现 `Clone`：一个局部块在序列中应当是唯一的。`position` 记录其提交后将占据的位置。
#[derive(Debug, PartialEq)]
pub struct PartialTokenBlock {
    tokens: Tokens,
    block_size: u32,
    salt_hash: SaltHash,
    parent_sequence_hash: Option<SequenceHash>,
    position: usize, // The position this block will have when committed
}

impl PartialTokenBlock {
    /// 为新序列创建第一个（根）局部块。
    ///
    /// # Arguments
    ///
    /// * `block_size` - 该序列中块的固定大小。
    /// * `salt_hash` - 该序列使用的 [`SaltHash`]。
    pub(crate) fn create_sequence_root(block_size: u32, salt_hash: SaltHash) -> Self {
        Self {
            tokens: Tokens::with_capacity(block_size as usize),
            block_size,
            salt_hash,
            parent_sequence_hash: None, // 根块没有父块。
            position: 0,                // 首块的位置为 0。
        }
    }

    /// 尝试从一个 [`Tokens`] 中向块内压入多个 token。
    ///
    /// 持续添加直至块满或输入耗尽。
    ///
    /// # Arguments
    ///
    /// * `tokens` - 待压入的 [`Tokens`]。
    ///
    /// # Returns
    ///
    /// 返回一个包含「未能放下」token 的新 [`Tokens`]；若全部放下则返回空对象。
    pub(crate) fn push_tokens(&mut self, tokens: Tokens) -> Tokens {
        let remaining_space = self.remaining();

        if remaining_space == 0 {
            return tokens; // 块已经满了。
        }

        if tokens.0.len() <= remaining_space {
            // 所有 token 都能放下。
            self.tokens.0.extend(tokens.0);
            Tokens::default() // 没有剩余 token。
        } else {
            // 只有一部分 token 能放下。
            let (to_add, remaining) = tokens.0.split_at(remaining_space);
            self.tokens.0.extend_from_slice(to_add);
            Tokens(remaining.to_vec()) // 返回剩余 token。
        }
    }

    /// 尝试向块内压入单个 token。
    ///
    /// # Returns
    ///
    /// * `Ok(())` - 成功添加。
    /// * `Err(TokenBlockError::Full)` - 块中已含 `block_size` 个 token。
    pub(crate) fn push_token(&mut self, token: Token) -> Result<(), TokenBlockError> {
        if self.tokens.0.len() >= self.block_size as usize {
            return Err(TokenBlockError::Full);
        }
        self.tokens.0.push(token);
        Ok(())
    }

    /// 尝试从块尾移除最后 `count` 个 token。
    ///
    /// # Arguments
    ///
    /// * `count` - 要移除的 token 数量。
    ///
    /// # Returns
    ///
    /// * `Ok(())` - 成功移除指定数量。
    /// * `Err(TokenBlockError::InsufficientTokens)` - `count` 超过块内 token 数。
    pub(crate) fn pop_tokens(&mut self, count: usize) -> Result<(), TokenBlockError> {
        if self.tokens.0.len() < count {
            return Err(TokenBlockError::InsufficientTokens);
        }
        self.tokens.0.truncate(self.tokens.0.len() - count);
        Ok(())
    }

    /// 尝试将当前局部块提交为一个完整的 [`TokenBlock`]。
    ///
    /// 该操作消费局部块内的 token；提交成功后，本 `PartialTokenBlock` 会被重置为序列中
    /// 的「下一个」局部块，并继承刚提交块的序列哈希。
    ///
    /// # Returns
    ///
    /// * `Ok(TokenBlock)` - 新生成的完整 [`TokenBlock`]。
    /// * `Err(TokenBlockError::Incomplete)` - 块内 token 数不等于 `block_size`。
    pub fn commit(&mut self) -> Result<TokenBlock, TokenBlockError> {
        if self.tokens.0.len() != self.block_size as usize {
            // 检查是否恰好满足提交所需的长度。
            return Err(TokenBlockError::Incomplete);
        }

        // 取出 token，保留内部缓冲为空。
        let tokens = std::mem::replace(
            &mut self.tokens,
            Tokens::with_capacity(self.block_size as usize),
        );

        let chunk = TokenBlockChunk::new(tokens, self.salt_hash);
        let block = TokenBlock::from_chunk(chunk, self.parent_sequence_hash, self.position);

        // 将自身重置为序列中的下一个块。
        self.parent_sequence_hash = Some(block.sequence_hash());
        self.position += 1; // 下一个块的位置加一。
        // self.block_size 和 self.salt_hash 保持不变。

        Ok(block)
    }

    /// 返回填满该块还需补充的 token 数。
    pub fn remaining(&self) -> usize {
        // 使用 saturating_sub，避免长度异常时发生下溢。
        (self.block_size as usize).saturating_sub(self.tokens.0.len())
    }

    /// 返回块内当前的 token 数。
    pub fn len(&self) -> usize {
        self.tokens.0.len()
    }

    /// 当块内没有任何 token 时返回 `true`。
    pub fn is_empty(&self) -> bool {
        self.tokens.0.is_empty()
    }

    /// 返回块内当前 token 的引用。
    pub fn tokens(&self) -> &Tokens {
        &self.tokens
    }
}

// === SECTION: 局部块的只读切片视图 ===

// 通过 Deref 把 `&PartialTokenBlock` 当作 `&Tokens` 做只读访问。
impl std::ops::Deref for PartialTokenBlock {
    type Target = Tokens;

    fn deref(&self) -> &Self::Target {
        &self.tokens
    }
}

// === SECTION: 块分片中间体 TokenBlockChunk ===

/// 承载一段「即将成为 [`TokenBlock`]」的 token 分片的中间结构。
///
/// ## 设计意图
/// 只计算 [`BlockHash`] 而不计算最终的 [`SequenceHash`]，以便各分片可独立（如并行）处理。
#[derive(Debug)]
struct TokenBlockChunk {
    tokens: Tokens,
    salt_hash: SaltHash,
    block_hash: BlockHash,
}

impl TokenBlockChunk {
    /// 由 [`Tokens`] 创建分片并计算其 [`BlockHash`]。
    fn new(tokens: Tokens, salt_hash: SaltHash) -> Self {
        let block_hash = compute_hash_v2(cast_slice(&tokens), salt_hash);
        Self {
            tokens,
            salt_hash,
            block_hash,
        }
    }

    /// 由 `&[Token]` 切片创建分片并计算其 [`BlockHash`]。
    fn from_tokens(tokens: &[Token], salt_hash: SaltHash) -> Self {
        let block_hash = compute_hash_v2(cast_slice(tokens), salt_hash);
        Self {
            tokens: tokens.into(), // 将切片转换为拥有所有权的 `Tokens`。
            salt_hash,
            block_hash,
        }
    }
}

// === SECTION: 完整块 TokenBlock ===

/// 已定型、不可变、且附带各类哈希的完整 token 块。
///
/// ## 外部契约
/// 恰好包含 `block_size` 个 token，并携带 [`SaltHash`]、[`BlockHash`]、[`SequenceHash`]、
/// [`PositionalSequenceHash`]、[`PositionalLineageHash`]，以及可选的父 [`SequenceHash`]。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TokenBlock {
    tokens: Tokens,
    salt_hash: SaltHash,
    block_hash: BlockHash,
    sequence_hash: SequenceHash,
    parent_sequence_hash: Option<SequenceHash>,
    positional_sequence_hash: PositionalSequenceHash,
    positional_lineage_hash: PositionalLineageHash,
}

impl TokenBlock {
    /// 创建一个紧随本块之后的 [`PartialTokenBlock`]。
    ///
    /// 新局部块会带上正确的 `parent_sequence_hash` 与 `position`。
    pub fn next_block(&self) -> PartialTokenBlock {
        PartialTokenBlock {
            tokens: Tokens::with_capacity(self.tokens.len()),
            block_size: self.tokens.len() as u32, // Should be == self.block_size
            salt_hash: self.salt_hash,
            parent_sequence_hash: Some(self.sequence_hash), // Link to this block
            position: self.position() as usize + 1,         // Next position
        }
    }

    /// 由 [`TokenBlockChunk`]、父序列哈希与位置定型出一个 [`TokenBlock`]。
    ///
    /// 该过程计算块最终的 [`SequenceHash`]、[`PositionalSequenceHash`] 与 [`PositionalLineageHash`]。
    fn from_chunk(
        chunk: TokenBlockChunk,
        parent_sequence_hash: Option<SequenceHash>,
        position: usize,
    ) -> Self {
        let sequence_hash = match parent_sequence_hash {
            Some(parent) => {
                // 组合父序列哈希和当前块哈希。
                compute_hash_v2(cast_slice(&[parent, chunk.block_hash]), chunk.salt_hash)
            }
            None => {
                // 首块：序列哈希就是块哈希。
                chunk.block_hash
            }
        };

        let positional_sequence_hash = PositionalSequenceHash::new(
            sequence_hash,
            position as u64,
            chunk.block_hash, // 局部块哈希与块哈希相同。
        );

        let positional_lineage_hash =
            PositionalLineageHash::new(sequence_hash, parent_sequence_hash, position as u64);

        Self {
            tokens: chunk.tokens,
            salt_hash: chunk.salt_hash,
            block_hash: chunk.block_hash,
            sequence_hash,
            parent_sequence_hash,
            positional_sequence_hash,
            positional_lineage_hash,
        }
    }

    /// 返回本块 token 的引用。
    pub fn tokens(&self) -> &Tokens {
        &self.tokens
    }

    /// 返回本块哈希所用的盐哈希。
    pub fn salt_hash(&self) -> SaltHash {
        self.salt_hash
    }

    /// 返回仅由本块内 token 计算的哈希。
    pub fn block_hash(&self) -> BlockHash {
        self.block_hash
    }

    /// 返回本块的序列感知哈希。
    pub fn sequence_hash(&self) -> SequenceHash {
        self.sequence_hash
    }

    /// 返回前一块的序列哈希（若有）。
    pub fn parent_sequence_hash(&self) -> Option<SequenceHash> {
        self.parent_sequence_hash
    }

    /// 返回本块中的 token 数。
    pub fn block_size(&self) -> usize {
        self.tokens.0.len()
    }

    /// 返回本块的位置序列哈希。
    pub fn positional_sequence_hash(&self) -> PositionalSequenceHash {
        self.positional_sequence_hash
    }

    /// 返回本块的位置血缘哈希。
    pub fn positional_lineage_hash(&self) -> PositionalLineageHash {
        self.positional_lineage_hash
    }

    /// 返回本块在序列中的位置。
    pub fn position(&self) -> u64 {
        self.positional_sequence_hash.position()
    }
}

// === SECTION: PositionalHash trait 实现 ===

impl PositionalHash for PositionalSequenceHash {
    fn position(&self) -> u64 {
        self.position()
    }
}

impl PositionalHash for PositionalLineageHash {
    fn position(&self) -> u64 {
        self.position()
    }
}

// === SECTION: 序列容器 TokenBlockSequence ===

/// 被切分为定长、已哈希块的 token 序列。
///
/// ## 设计意图
/// 维护一系列已完成的 [`TokenBlock`] 与一个用于累积新 token 的 [`PartialTokenBlock`]。
///
/// ## 外部契约
/// 提供追加（`append`、`extend`）、移除（`pop`、`truncate`、`unwind`）以及序列信息访问等方法。
/// 哈希计算融入初始 [`SaltHash`]，以在不同上下文（如不同模型、PEFT）间保证唯一性。
///
/// ## 实现要点
/// 关键哈希：
/// - [`BlockHash`]：单块内 token 的哈希（以 [`SaltHash`] 为种子）。
/// - [`SequenceHash`]：由上一块的 [`SequenceHash`] 与当前块的 [`BlockHash`] 组合（同样以 [`SaltHash`] 为种子）。
#[derive(Debug, PartialEq)]
pub struct TokenBlockSequence {
    blocks: Vec<TokenBlock>,
    current_block: PartialTokenBlock,
    salt_hash: SaltHash,
    block_size: usize,
}

impl TokenBlockSequence {
    /// 由一组初始 token 创建新的 [`TokenBlockSequence`]。
    ///
    /// token 按 `block_size` 切块，剩余 token 构成初始 `current_block`。
    ///
    /// # Arguments
    ///
    /// * `tokens` - 序列的初始 [`Tokens`]。
    /// * `block_size` - 每个 [`TokenBlock`] 的固定大小，必须大于 0。
    /// * `salt_hash` - 可选 [`SaltHash`]，`None` 时为 0。
    ///
    /// # Panics
    ///
    /// 当 `block_size` 为 0 时 panic。
    pub fn new(tokens: Tokens, block_size: u32, salt_hash: Option<SaltHash>) -> Self {
        assert!(block_size > 0, "block_size must be greater than 0");
        let salt_hash = salt_hash.unwrap_or(0);
        let (blocks, current_block) = Self::split_tokens(&tokens, block_size, salt_hash);

        Self {
            blocks,
            current_block,
            salt_hash,
            block_size: block_size as usize,
        }
    }

    /// 用给定 token 扩展序列，可能一次性完成多个块。
    ///
    /// 该方法处理输入 [`Tokens`] 中的全部 token；若填满一个或多个块，则提交并加入已完成块列表。
    ///
    /// # Arguments
    ///
    /// * `tokens` - 用于扩展序列的 [`Tokens`]。
    ///
    /// # Returns
    ///
    /// * `Ok(Some(Range<usize>))` - 本次操作中完成的块在 `blocks` 中的下标区间。
    /// * `Ok(None)` - 未完成任何块。
    /// * `Err(TokenBlockError)` - 提交过程中发生内部错误。
    pub fn extend(&mut self, tokens: Tokens) -> Result<Option<Range<usize>>, TokenBlockError> {
        let start_block_index = self.blocks.len();
        let mut tokens_to_append = tokens;

        while !tokens_to_append.is_empty() {
            let remaining_in_current = self.current_block.remaining();

            if remaining_in_current == 0 {
                // 当前块已满，先提交它。
                let new_block = self.current_block.commit()?;
                self.blocks.push(new_block);
                // 继续循环，把 token 加入新的 current_block。
            }

            // 尽可能多地将 token 压入当前（可能是新建的）块。
            let available_tokens = tokens_to_append;
            tokens_to_append = self.current_block.push_tokens(available_tokens);

            // 检查推入后当前块是否变满。
            if self.current_block.remaining() == 0 {
                // 如果它已经变满且仍有更多 token 要追加，就立即提交。
                // 这样下一轮循环会从一个新块开始。
                let new_block = self.current_block.commit()?;
                self.blocks.push(new_block);
            }
        }

        let end_block_index = self.blocks.len();
        if start_block_index == end_block_index {
            Ok(None) // No blocks were completed
        } else {
            Ok(Some(start_block_index..end_block_index))
        }
    }

    /// 向序列追加单个 token。
    ///
    /// 若该 token 填满当前局部块，则提交该块并返回新完成块的下标。
    ///
    /// 等价于用单个 token 的 [`Tokens`] 调用 [`extend`]。
    ///
    /// # Arguments
    ///
    /// * `token` - 要追加的 [`Token`]。
    ///
    /// # Returns
    ///
    /// * `Ok(Some(usize))` - 刚刚完成的块的下标。
    /// * `Ok(None)` - 本次追加未完成任何块。
    /// * `Err(TokenBlockError)` - 处理中发生内部错误。
    pub fn append(&mut self, token: Token) -> Result<Option<usize>, TokenBlockError> {
        if self.current_block.remaining() == 0 {
            let new_block = self.current_block.commit()?;
            self.blocks.push(new_block);
        }

        self.current_block.push_token(token)?;
        if self.current_block.remaining() != 0 {
            return Ok(None);
        }

        let completed_idx = self.blocks.len();
        let new_block = self.current_block.commit()?;
        self.blocks.push(new_block);
        Ok(Some(completed_idx))
    }

    /// 截短序列，保留前 `len` 个 token，丢弃其余。
    ///
    /// 若 `len` 大于当前长度则无效果。该操作类似 `Vec::truncate`，
    /// 可能涉及从当前局部块移除 token、移除整个完成块，并调整局部块以反映新的序列尾部。
    ///
    /// # Arguments
    ///
    /// * `len` - 要保留的 token 数。
    ///
    /// # Returns
    ///
    /// * `Ok(())` - 截短成功。
    /// * `Err(TokenBlockError::InsufficientTokens)` - 若 `len` 已正确校验则理应不会出现，但底层 `pop_tokens` 仍可能返回。
    pub fn truncate(&mut self, len: usize) -> Result<(), TokenBlockError> {
        let current_total_len = self.total_tokens();
        if len >= current_total_len {
            return Ok(()); // Nothing to truncate
        }

        let n = current_total_len - len; // Number of tokens to remove

        // 这个内部代码块根据要移除的 `n` 个 token 执行实际删除逻辑。
        {
            let current_len = self.current_block.len();
            // 避免在 block_size 不知为何为 0 时出现除零（尽管 new 已断言）。
            let block_size = self.current_block.block_size.max(1);

            if n <= current_len {
                // 只需从当前局部块中弹出。
                self.current_block.pop_tokens(n)?;
            } else {
                // 也需要从完整块中弹出。
                let tokens_to_pop_from_blocks = n - current_len;

                // 计算受影响的块数（包括那个被部分弹出的块）。
                let num_blocks_to_affect = tokens_to_pop_from_blocks.div_ceil(block_size as usize);

                // 检查是否需要弹出的块数超过现有数量（这应已被最初的长度检查阻止）。
                if num_blocks_to_affect > self.blocks.len() {
                    // 这表明 total_tokens() 与内部状态之间存在不一致。
                    debug_assert!(
                        false,
                        "Truncate calculation error: trying to pop too many blocks."
                    );
                    return Err(TokenBlockError::InsufficientTokens);
                }

                // 确定将作为新局部块来源的那个块的索引。
                let source_block_index = self.blocks.len() - num_blocks_to_affect;

                // 计算要从该来源块中保留多少个 token。
                let num_full_blocks_completely_popped = num_blocks_to_affect - 1;
                let num_tokens_to_pop_from_source_block = tokens_to_pop_from_blocks
                    - num_full_blocks_completely_popped * block_size as usize;
                let num_tokens_to_keep_in_new_partial =
                    (block_size as usize).saturating_sub(num_tokens_to_pop_from_source_block);

                // 取出新局部块所需的 token。
                let new_partial_tokens = if num_tokens_to_keep_in_new_partial > 0 {
                    self.blocks[source_block_index].tokens().as_ref()
                        [..num_tokens_to_keep_in_new_partial]
                        .to_vec()
                } else {
                    Vec::new()
                };

                // 截短 blocks 向量，移除已弹出的块。
                self.blocks.truncate(source_block_index);

                // 更新 current_block 状态。
                self.current_block.tokens = Tokens(new_partial_tokens);
                // 根据新的最后一个块正确设置父哈希。
                self.current_block.parent_sequence_hash =
                    self.blocks.last().map(|b| b.sequence_hash());
                // 更新位置，使其与完整块数量一致。
                self.current_block.position = self.blocks.len();
                // current_block 的 salt_hash 和 block_size 保持不变。
            }
        }
        Ok(())
    }

    /// 从序列尾部移除最后 `count` 个 token。
    ///
    /// 便捷方法：计算应保留长度后调用 [`truncate`]。
    ///
    /// # Arguments
    ///
    /// * `count` - 要从尾部移除的 token 数。
    ///
    /// # Returns
    ///
    /// * `Ok(())` - 移除成功。
    /// * `Err(TokenBlockError::InsufficientTokens)` - `count` 大于序列中的 token 总数。
    pub fn unwind(&mut self, count: usize) -> Result<(), TokenBlockError> {
        let current_total_len = self.total_tokens();
        if count > current_total_len {
            // 允许 count == current_total_len，这会截短到 0。
            return Err(TokenBlockError::InsufficientTokens);
        }

        // 撤销给定数量后，序列中剩余的 token 数。
        let len = current_total_len - count;
        self.truncate(len)
    }

    /// 将序列重置为初始状态。
    pub fn reset(&mut self) {
        self.blocks.clear();
        self.current_block =
            PartialTokenBlock::create_sequence_root(self.block_size as u32, self.salt_hash);
    }

    /// 移除并返回序列的最后一个 token；序列为空时返回 [`None`]。
    ///
    /// 该操作类似 `Vec::pop`。
    ///
    /// # Returns
    ///
    /// * `Some(Token)` - 序列非空时的最后一个 token。
    /// * `None` - 序列为空。
    pub fn pop(&mut self) -> Option<Token> {
        let current_total_len = self.total_tokens();
        if current_total_len == 0 {
            return None;
        }

        // 确定最后一个 token。如果 current_block 非空，它必须在 current_block 中。
        // 如果 current_block 为空，则它必须是最后一个完整块中的最后一个 token。
        let last_token = if !self.current_block.tokens.is_empty() {
            // 最后一个 token 在局部块中。
            *self
                .current_block
                .tokens
                .last()
                .expect("Current block checked for non-empty")
        } else {
            // 当前块为空，但序列不为空；它必须在最后一个完整块中。
            let last_block = self
                .blocks
                .last()
                .expect("Sequence is not empty but has no blocks and empty current block?");
            *last_block
                .tokens()
                .last()
                .expect("Last block cannot be empty")
        };

        // 将序列截短一个元素。
        // 由于我们已知长度大于 0，所以这里应当成功。
        match self.truncate(current_total_len - 1) {
            Ok(_) => Some(last_token),
            Err(_) => {
                // 如果 total_tokens() 和 truncate() 正确，这在逻辑上不应发生。
                // 调试模式下 panic，发布模式下退回 None，但这表明存在 bug。
                debug_assert!(
                    false,
                    "truncate failed unexpectedly after checking length in pop"
                );
                None
            }
        }
    }

    /// 返回包含序列中全部已完成 [`TokenBlock`] 的切片。
    pub fn blocks(&self) -> &[TokenBlock] {
        &self.blocks
    }

    /// 返回序列中最后一个已完成 [`TokenBlock`] 的引用（若有）。
    pub fn last_complete_block(&self) -> Option<&TokenBlock> {
        self.blocks.last()
    }

    /// 返回当前用于接纳新 token 的 [`PartialTokenBlock`] 的引用。
    pub fn current_block(&self) -> &PartialTokenBlock {
        &self.current_block
    }

    /// 消费序列并返回其组成：已完成块的 `Vec` 与最终局部块。
    pub fn into_parts(self) -> (Vec<TokenBlock>, PartialTokenBlock) {
        (self.blocks, self.current_block)
    }

    /// 返回本序列使用的块大小。
    pub fn block_size(&self) -> usize {
        self.block_size
    }

    /// 返回本序列使用的 [`SaltHash`]。
    pub fn salt_hash(&self) -> SaltHash {
        self.salt_hash
    }

    /// 返回序列中的 token 总数（所有已完成块中 token 数与当前局部块 token 数之和）。
    pub fn total_tokens(&self) -> usize {
        let block_size = self.current_block.block_size as usize;
        (self.blocks.len() * block_size) + self.current_block.len()
    }

    /// 抽取给定区间内的 token。
    pub fn tokens_at(&self, range: Range<usize>) -> Tokens {
        let total = self.total_tokens();

        // 校验区间；无效区间返回空 token 集合。
        if range.start > range.end || range.end > total {
            return Tokens::default();
        }

        // 处理空区间。
        if range.is_empty() {
            return Tokens::default();
        }

        let mut result = Vec::with_capacity(range.len());

        for i in range {
            if i < self.blocks.len() * self.block_size {
                // Token 位于已完成块中。
                let block_index = i / self.block_size;
                let token_index = i % self.block_size;
                result.push(self.blocks[block_index].tokens()[token_index]);
            } else {
                // Token 位于当前局部块中。
                let current_block_index = i - (self.blocks.len() * self.block_size);
                result.push(self.current_block.tokens()[current_block_index]);
            }
        }

        Tokens::from(result)
    }

    /// 将一个 [`Tokens`] 切分为已完成块向量与最终局部块。
    ///
    /// 主要供 [`TokenBlockSequence::new`] 内部使用，也可外部调用。
    ///
    /// # Arguments
    ///
    /// * `tokens` - 待切分的 [`Tokens`]。
    /// * `block_size` - 每个块的大小。
    /// * `salt_hash` - 哈希所用的 [`SaltHash`]。
    ///
    /// # Returns
    ///
    /// 元组 `(Vec<TokenBlock>, PartialTokenBlock)`。
    ///
    /// # Panics
    ///
    /// 当 `block_size` 为 0 时 panic。
    pub fn split_tokens(
        tokens: &[Token],
        block_size: u32,
        salt_hash: u64,
    ) -> (Vec<TokenBlock>, PartialTokenBlock) {
        assert!(block_size > 0, "block_size must be greater than 0");
        let chunks: Vec<TokenBlockChunk> = tokens
            .as_ref()
            .chunks_exact(block_size as usize)
            .map(|chunk| TokenBlockChunk::from_tokens(chunk, salt_hash))
            .collect();

        let mut result_blocks = Vec::with_capacity(chunks.len());
        let mut last_sequence_hash: Option<SequenceHash> = None;

        // 依次组合分片以计算序列哈希。
        for (position, chunk) in chunks.into_iter().enumerate() {
            let new_block = TokenBlock::from_chunk(chunk, last_sequence_hash, position);
            last_sequence_hash = Some(new_block.sequence_hash());
            result_blocks.push(new_block);
        }

        // 处理所有剩余的 token。
        let remainder = tokens
            .as_ref()
            .chunks_exact(block_size as usize)
            .remainder();

        let next_position = result_blocks.len(); // Position for the next block to be committed

        let mut partial_tokens = Tokens::with_capacity(block_size as usize);
        partial_tokens.0.extend_from_slice(remainder);

        let current_block = PartialTokenBlock {
            tokens: partial_tokens,
            block_size,
            salt_hash,
            // 父哈希就是最后一个完整块计算出的序列哈希。
            parent_sequence_hash: last_sequence_hash,
            position: next_position,
        };

        (result_blocks, current_block)
    }

    /// 从 token 切片创建新的 [`TokenBlockSequence`]。
    ///
    /// token 按 `block_size` 切块，剩余 token 构成初始 `current_block`。
    ///
    /// # Arguments
    ///
    /// * `tokens` - 用于创建序列的 token 切片。
    /// * `block_size` - 每个块的大小。
    /// * `salt_hash` - 哈希所用的 [`SaltHash`]。
    pub fn from_slice(tokens: &[Token], block_size: u32, salt_hash: Option<SaltHash>) -> Self {
        assert!(block_size > 0, "block_size must be greater than 0");
        let salt_hash = salt_hash.unwrap_or(0);
        let (blocks, current_block) = Self::split_tokens(tokens, block_size, salt_hash);

        Self {
            blocks,
            current_block,
            salt_hash,
            block_size: block_size as usize,
        }
    }
}

#[cfg(test)]
mod tests {
    //! ## 设计意图
    //!
    //! 本测试模块承担双重职责：既作为「接口契约验证」（逐字保留标准基准
    //! 测试，作为回归基线，确保重写后的可观察行为与标准实现完全一致），
    //! 又作为「实现细节测试」（针对本次重写引入的表驱动位布局、掩码助手、
    //! 以及 radix 树等内部路径补充覆盖）。
    //!
    //! ## 外部契约
    //!
    //! 全部标准测试项名与断言保持不变；哈希常量、panic 信息、Debug/Display 格式、
    //! Ord 语义与 serde 往返均与标准实现逐位对齐。
    //!
    //! ## 实现要点
    //!
    //! 所有测试集中于单一 `mod tests`；本次新增的补充测试采用 `## 测试过程` /
    //! `## 意义` 注释格式，集中放在文件末尾的补充测试分区中。
    use super::*;
    use bytemuck::cast_slice;

    // 用于测试的序列构造辅助函数。
    fn create_test_sequence(
        initial_tokens: &[Token],
        block_size: u32,
        salt_hash: Option<SaltHash>,
    ) -> TokenBlockSequence {
        TokenBlockSequence::new(Tokens::from(initial_tokens), block_size, salt_hash)
    }

    // 用于获取期望哈希的辅助常量（如有需要可替换为实际计算值）。
    const TEST_SALT_HASH: SaltHash = 1337;
    const HASH_1_4: BlockHash = 14643705804678351452; // hash([1,2,3,4], 1337)
    const SEQ_HASH_1_4: SequenceHash = HASH_1_4;
    const HASH_5_8: BlockHash = 16777012769546811212; // hash([5,6,7,8], 1337)
    const SEQ_HASH_5_8: SequenceHash = 4945711292740353085; // hash([SEQ_HASH_1_4, HASH_5_8], 1337)
    const HASH_9_12: BlockHash = 483935686894639516; // hash([9,10,11,12], 1337)
    const SEQ_HASH_9_12: SequenceHash = 12583592247330656132; // hash([SEQ_HASH_5_8, HASH_9_12], 1337)

    impl PartialTokenBlock {
        /// 尝试从块中移除最后一个 token。
        ///
        /// # Returns
        ///
        /// * `Ok(())` - 成功移除 token。
        /// * `Err(TokenBlockError::Empty)` - 如果块已经为空。
        pub fn pop_token(&mut self) -> Result<(), TokenBlockError> {
            if self.tokens.0.is_empty() {
                return Err(TokenBlockError::Empty);
            }
            self.tokens.0.pop();
            Ok(())
        }
    }

    #[test]
    fn test_validate_hash_constants() {
        let salt = TEST_SALT_HASH;

        // Block 1: [1, 2, 3, 4]
        let tokens_1_4 = &[1u32, 2, 3, 4];
        let computed_hash_1_4 = compute_hash_v2(cast_slice(tokens_1_4), salt);
        assert_eq!(computed_hash_1_4, HASH_1_4, "Mismatch for HASH_1_4");
        // 首块的序列哈希就是它自己的块哈希。
        assert_eq!(computed_hash_1_4, SEQ_HASH_1_4, "Mismatch for SEQ_HASH_1_4");

        // Block 2: [5, 6, 7, 8]
        let tokens_5_8 = &[5u32, 6, 7, 8];
        let computed_hash_5_8 = compute_hash_v2(cast_slice(tokens_5_8), salt);
        assert_eq!(computed_hash_5_8, HASH_5_8, "Mismatch for HASH_5_8");
        let computed_seq_hash_5_8 = compute_hash_v2(cast_slice(&[SEQ_HASH_1_4, HASH_5_8]), salt);
        assert_eq!(
            computed_seq_hash_5_8, SEQ_HASH_5_8,
            "Mismatch for SEQ_HASH_5_8"
        );

        // Block 3: [9, 10, 11, 12]
        let tokens_9_12 = &[9u32, 10, 11, 12];
        let computed_hash_9_12 = compute_hash_v2(cast_slice(tokens_9_12), salt);
        assert_eq!(computed_hash_9_12, HASH_9_12, "Mismatch for HASH_9_12");
        let computed_seq_hash_9_12 = compute_hash_v2(cast_slice(&[SEQ_HASH_5_8, HASH_9_12]), salt);
        assert_eq!(
            computed_seq_hash_9_12, SEQ_HASH_9_12,
            "Mismatch for SEQ_HASH_9_12"
        );
    }

    #[test]
    fn test_positional_sequence_hash_encoding_decoding() {
        // 测试模式 0：位置可容纳在 8 位中（< 256）。
        let seq_hash_0 = 0x1234567890ABCDEF;
        let position_0 = 100;
        let lbh_0 = 0xFEDCBA9876543210;
        let psh_0 = PositionalSequenceHash::new(seq_hash_0, position_0, lbh_0);

        assert_eq!(psh_0.mode(), 0, "Position 100 should use mode 0");
        assert_eq!(psh_0.sequence_hash(), seq_hash_0);
        assert_eq!(psh_0.position(), position_0);
        // 模式 0 中 LBH 会截断为 54 位。
        assert_eq!(
            psh_0.local_block_hash(),
            lbh_0 & ((1u64 << 54) - 1),
            "LBH should be truncated to 54 bits"
        );

        // 测试模式 1：位置可容纳在 16 位中（256 <= pos < 65536）。
        let position_1 = 1000;
        let psh_1 = PositionalSequenceHash::new(seq_hash_0, position_1, lbh_0);

        assert_eq!(psh_1.mode(), 1, "Position 1000 should use mode 1");
        assert_eq!(psh_1.sequence_hash(), seq_hash_0);
        assert_eq!(psh_1.position(), position_1);
        // 模式 1 中 LBH 会截断为 46 位。
        assert_eq!(
            psh_1.local_block_hash(),
            lbh_0 & ((1u64 << 46) - 1),
            "LBH should be truncated to 46 bits"
        );

        // 测试模式 2：位置可容纳在 24 位中（65536 <= pos < 16777216）。
        let position_2 = 100_000;
        let psh_2 = PositionalSequenceHash::new(seq_hash_0, position_2, lbh_0);

        assert_eq!(psh_2.mode(), 2, "Position 100,000 should use mode 2");
        assert_eq!(psh_2.sequence_hash(), seq_hash_0);
        assert_eq!(psh_2.position(), position_2);
        // 模式 2 中 LBH 会截断为 38 位。
        assert_eq!(
            psh_2.local_block_hash(),
            lbh_0 & ((1u64 << 38) - 1),
            "LBH should be truncated to 38 bits"
        );

        // 测试模式 3：位置可容纳在 31 位中（16777216 <= pos < 2^31）。
        let position_3 = 20_000_000;
        let psh_3 = PositionalSequenceHash::new(seq_hash_0, position_3, lbh_0);

        assert_eq!(psh_3.mode(), 3, "Position 20,000,000 should use mode 3");
        assert_eq!(psh_3.sequence_hash(), seq_hash_0);
        assert_eq!(psh_3.position(), position_3);
        // 模式 3 中 LBH 会截断为 31 位。
        assert_eq!(
            psh_3.local_block_hash(),
            lbh_0 & ((1u64 << 31) - 1),
            "LBH should be truncated to 31 bits"
        );

        // 测试边界情况：位置正好卡在边界上。
        let position_255 = 255;
        let psh_255 = PositionalSequenceHash::new(seq_hash_0, position_255, lbh_0);
        assert_eq!(psh_255.mode(), 0, "Position 255 should use mode 0");
        assert_eq!(psh_255.position(), position_255);

        let position_256 = 256;
        let psh_256 = PositionalSequenceHash::new(seq_hash_0, position_256, lbh_0);
        assert_eq!(psh_256.mode(), 1, "Position 256 should use mode 1");
        assert_eq!(psh_256.position(), position_256);
    }

    #[test]
    fn test_positional_lineage_hash() {
        // 测试模式 0：位置可容纳在 8 位中（< 256）。
        let current_hash_0 = 0x1234567890ABCDEF;
        let parent_hash_0 = 0xFEDCBA9876543210;
        let position_0 = 100;
        let plh_0 = PositionalLineageHash::new(current_hash_0, Some(parent_hash_0), position_0);

        assert_eq!(plh_0.mode(), 0, "Position 100 should use mode 0");
        assert_eq!(plh_0.position(), position_0);
        // 当前哈希和父哈希在模式 0 中都会截断为 59 位。
        assert_eq!(
            plh_0.current_hash_fragment(),
            current_hash_0 & ((1u64 << 59) - 1),
            "Current hash should be truncated to 59 bits"
        );
        assert_eq!(
            plh_0.parent_hash_fragment(),
            parent_hash_0 & ((1u64 << 59) - 1),
            "Parent hash should be truncated to 59 bits"
        );

        // 测试模式 1：位置可容纳在 16 位中（256 <= pos < 65536）。
        let position_1 = 1000;
        let plh_1 = PositionalLineageHash::new(current_hash_0, Some(parent_hash_0), position_1);

        assert_eq!(plh_1.mode(), 1, "Position 1000 should use mode 1");
        assert_eq!(plh_1.position(), position_1);
        // 当前哈希和父哈希在模式 1 中都会截断为 55 位。
        assert_eq!(
            plh_1.current_hash_fragment(),
            current_hash_0 & ((1u64 << 55) - 1),
            "Current hash should be truncated to 55 bits"
        );
        assert_eq!(
            plh_1.parent_hash_fragment(),
            parent_hash_0 & ((1u64 << 55) - 1),
            "Parent hash should be truncated to 55 bits"
        );

        // 测试模式 2：位置可容纳在 24 位中（65536 <= pos < 16777216）。
        let position_2 = 100_000;
        let plh_2 = PositionalLineageHash::new(current_hash_0, Some(parent_hash_0), position_2);

        assert_eq!(plh_2.mode(), 2, "Position 100,000 should use mode 2");
        assert_eq!(plh_2.position(), position_2);
        // 当前哈希和父哈希在模式 2 中都会截断为 51 位。
        assert_eq!(
            plh_2.current_hash_fragment(),
            current_hash_0 & ((1u64 << 51) - 1),
            "Current hash should be truncated to 51 bits"
        );
        assert_eq!(
            plh_2.parent_hash_fragment(),
            parent_hash_0 & ((1u64 << 51) - 1),
            "Parent hash should be truncated to 51 bits"
        );

        // 测试边界情况：位置正好卡在边界上。
        let position_255 = 255;
        let plh_255 = PositionalLineageHash::new(current_hash_0, Some(parent_hash_0), position_255);
        assert_eq!(plh_255.mode(), 0, "Position 255 should use mode 0");
        assert_eq!(plh_255.position(), position_255);

        let position_256 = 256;
        let plh_256 = PositionalLineageHash::new(current_hash_0, Some(parent_hash_0), position_256);
        assert_eq!(plh_256.mode(), 1, "Position 256 should use mode 1");
        assert_eq!(plh_256.position(), position_256);

        let position_65535 = 65535;
        let plh_65535 =
            PositionalLineageHash::new(current_hash_0, Some(parent_hash_0), position_65535);
        assert_eq!(plh_65535.mode(), 1, "Position 65535 should use mode 1");
        assert_eq!(plh_65535.position(), position_65535);

        let position_65536 = 65536;
        let plh_65536 =
            PositionalLineageHash::new(current_hash_0, Some(parent_hash_0), position_65536);
        assert_eq!(plh_65536.mode(), 2, "Position 65536 should use mode 2");
        assert_eq!(plh_65536.position(), position_65536);

        // 测试父块为 None 的情况（根块）。
        let plh_root = PositionalLineageHash::new(current_hash_0, None, 0);
        assert_eq!(plh_root.mode(), 0);
        assert_eq!(plh_root.position(), 0);
        assert_eq!(
            plh_root.parent_hash_fragment(),
            0,
            "Root should have zero parent hash"
        );
        assert_eq!(
            plh_root.current_hash_fragment(),
            current_hash_0 & ((1u64 << 59) - 1)
        );

        // 测试低位对齐：验证较小模式的片段是较大模式片段的子集。
        let position_small = 100; // 模式 0：59 位。
        let position_large = 1000; // 模式 1：55 位。
        let plh_small =
            PositionalLineageHash::new(current_hash_0, Some(parent_hash_0), position_small);
        let plh_large =
            PositionalLineageHash::new(current_hash_0, Some(parent_hash_0), position_large);

        // 模式 1 的 55 位片段应与模式 0 的 59 位片段低 55 位一致。
        let mask_55 = (1u64 << 55) - 1;
        assert_eq!(
            plh_large.current_hash_fragment(),
            plh_small.current_hash_fragment() & mask_55,
            "LSB alignment: mode 1 fragment should be subset of mode 0 fragment"
        );
    }

    #[test]
    #[should_panic(expected = "Position 16777216 exceeds maximum supported value")]
    fn test_positional_lineage_hash_panic_on_large_position() {
        let current_hash = 0x1234567890ABCDEF;
        let parent_hash = 0xFEDCBA9876543210;
        let position = 1u64 << 24; // 2^24 = 16,777,216
        let _ = PositionalLineageHash::new(current_hash, Some(parent_hash), position);
    }

    #[test]
    fn test_positional_lineage_hash_mode_boundary_alignment() {
        // 测试哈希片段在模式边界处能正确对齐。
        // 这对基数树的反向遍历至关重要。

        let parent_hash = 0xFEDCBA9876543210;
        let current_hash_255 = 0x1234567890ABCDEF;
        let current_hash_256 = 0xABCDEF0123456789;

        // 位置 255：模式 0（边界前的最后一个位置）。
        let plh_255 = PositionalLineageHash::new(current_hash_255, Some(parent_hash), 255);
        assert_eq!(plh_255.mode(), 0);

        // 位置 256：模式 1（边界后的第一个位置）。
        // 这应当把位置 255 的 current_hash 作为其父片段。
        let plh_256 = PositionalLineageHash::new(current_hash_256, Some(current_hash_255), 256);
        assert_eq!(plh_256.mode(), 1);

        // 关键测试：位置 256 的父片段应与位置 255 的当前片段一致，以支持反向遍历。
        // 两者都应截断为 55 位（边界处可用的最小位宽）。
        let mask_55 = (1u64 << 55) - 1;
        assert_eq!(
            plh_256.parent_hash_fragment(),
            plh_255.current_hash_fragment() & mask_55,
            "Mode boundary: position 256's parent fragment should match position 255's current fragment (55 bits)"
        );

        // 验证位置 255 的当前片段已经预先截断为 55 位。
        // 不是模式 0 理论上可支持的完整 59 位。
        assert_eq!(
            plh_255.current_hash_fragment(),
            current_hash_255 & mask_55,
            "Position 255 should pre-truncate current hash to 55 bits for next mode compatibility"
        );

        // 测试另一条边界：65535 -> 65536（模式 1 -> 模式 2）。
        let current_hash_65535 = 0x1111222233334444;
        let current_hash_65536 = 0x5555666677778888;

        let plh_65535 = PositionalLineageHash::new(current_hash_65535, Some(parent_hash), 65535);
        assert_eq!(plh_65535.mode(), 1);

        let plh_65536 =
            PositionalLineageHash::new(current_hash_65536, Some(current_hash_65535), 65536);
        assert_eq!(plh_65536.mode(), 2);

        // 两者都应对齐到 51 位（模式 2 的容量）。
        let mask_51 = (1u64 << 51) - 1;
        assert_eq!(
            plh_65536.parent_hash_fragment(),
            plh_65535.current_hash_fragment() & mask_51,
            "Mode boundary: position 65536's parent fragment should match position 65535's current fragment (51 bits)"
        );

        assert_eq!(
            plh_65535.current_hash_fragment(),
            current_hash_65535 & mask_51,
            "Position 65535 should pre-truncate current hash to 51 bits for next mode compatibility"
        );
    }

    #[test]
    fn test_tokens_from() {
        let vec_u32: Vec<u32> = vec![1, 2, 3];
        let tokens_u32: Tokens = vec_u32.clone().into();
        assert_eq!(tokens_u32.0, vec_u32);

        let slice_u32: &[u32] = &[4, 5];
        let tokens_slice_u32: Tokens = slice_u32.into();
        assert_eq!(tokens_slice_u32.0, vec![4, 5]);

        let vec_i32: Vec<i32> = vec![-1, 0, 1]; // Note: -1 becomes large u32
        let tokens_i32: Tokens = vec_i32.into();
        assert_eq!(tokens_i32.0, vec![u32::MAX, 0, 1]);

        let slice_i32: &[i32] = &[100, 200];
        let tokens_slice_i32: Tokens = slice_i32.into();
        assert_eq!(tokens_slice_i32.0, vec![100, 200]);

        let into_vec: Vec<u32> = tokens_slice_i32.into();
        assert_eq!(into_vec, vec![100, 200]);
    }

    #[test]
    fn test_tokens_equality() {
        let tokens = Tokens::from(vec![1, 2, 3]);
        assert_eq!(tokens, vec![1, 2, 3]);
        assert_eq!(vec![1, 2, 3], tokens);
        assert_eq!(tokens, &[1, 2, 3][..]);
        assert_eq!(&[1, 2, 3][..], tokens);
        assert_eq!(tokens, Tokens::from(vec![1, 2, 3]));
        assert_ne!(tokens, Tokens::from(vec![1, 2, 4]));
    }

    #[test]
    fn test_tokens_deref_asref() {
        let tokens = Tokens::from(vec![10, 20, 30]);

        // 通过 Deref 转成 `&[Token]`。
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[1], 20);
        let slice: &[Token] = &tokens;
        assert_eq!(slice, &[10, 20, 30]);

        // AsRef<[Token]>。
        let as_ref_slice: &[Token] = tokens.as_ref();
        assert_eq!(as_ref_slice, &[10, 20, 30]);

        // Borrow<[Token]>。
        let borrowed_slice: &[Token] = std::borrow::Borrow::borrow(&tokens);
        assert_eq!(borrowed_slice, &[10, 20, 30]);
    }

    #[test]
    fn test_tokens_into_sequence() {
        let tokens = Tokens::from(vec![1, 2, 3, 4, 5]);
        let seq = tokens.into_sequence(3, Some(TEST_SALT_HASH));
        assert_eq!(seq.blocks().len(), 1);
        assert_eq!(seq.blocks[0].tokens().as_ref(), &[1, 2, 3]);
        assert_eq!(seq.current_block().tokens().as_ref(), &[4, 5]);
        assert_eq!(seq.salt_hash(), TEST_SALT_HASH);
    }

    #[test]
    fn test_partial_block_ops() {
        let mut partial = PartialTokenBlock::create_sequence_root(3, TEST_SALT_HASH);
        assert_eq!(partial.len(), 0);
        assert_eq!(partial.remaining(), 3);
        assert!(partial.is_empty());

        // 推入 token。
        assert!(partial.push_token(1).is_ok());
        assert_eq!(partial.len(), 1);
        assert_eq!(partial.remaining(), 2);
        let remaining = partial.push_tokens(Tokens::from(vec![2, 3, 4]));
        assert_eq!(partial.len(), 3);
        assert_eq!(partial.remaining(), 0);
        assert_eq!(remaining.as_ref(), &[4]); // Token 4 didn't fit
        assert_eq!(partial.tokens().as_ref(), &[1, 2, 3]);

        // 已满时推入。
        assert_eq!(partial.push_token(5), Err(TokenBlockError::Full));
        let remaining_full = partial.push_tokens(Tokens::from(vec![5]));
        assert_eq!(remaining_full.as_ref(), &[5]);

        // 弹出 token。
        assert!(partial.pop_token().is_ok());
        assert_eq!(partial.len(), 2);
        assert_eq!(partial.tokens().as_ref(), &[1, 2]);
        assert!(partial.pop_tokens(2).is_ok());
        assert!(partial.is_empty());

        // 为空时弹出。
        assert_eq!(partial.pop_token(), Err(TokenBlockError::Empty));
        assert_eq!(
            partial.pop_tokens(1),
            Err(TokenBlockError::InsufficientTokens)
        );

        // 提交未满块。
        assert!(partial.push_token(10).is_ok());
        assert_eq!(partial.commit(), Err(TokenBlockError::Incomplete));

        // 提交完整块。
        assert!(partial.push_token(11).is_ok());
        assert!(partial.push_token(12).is_ok());
        assert_eq!(partial.len(), 3);
        let commit_result = partial.commit();
        assert!(commit_result.is_ok());
        let committed_block = commit_result.unwrap();
        assert_eq!(committed_block.tokens().as_ref(), &[10, 11, 12]);

        // 检查提交后的状态（局部块现在变成下一个块）。
        assert!(partial.is_empty());
        assert_eq!(
            partial.parent_sequence_hash,
            Some(committed_block.sequence_hash())
        );
        assert_eq!(partial.block_size, 3);
    }

    #[test]
    fn test_token_block_creation_and_hashes() {
        let salt = TEST_SALT_HASH;
        let tokens1 = Tokens::from(vec![1, 2, 3, 4]);
        let chunk1 = TokenBlockChunk::new(tokens1.clone(), salt);
        let block1 = TokenBlock::from_chunk(chunk1, None, 0);

        assert_eq!(block1.tokens(), &tokens1);
        assert_eq!(block1.salt_hash(), salt);
        assert_eq!(block1.parent_sequence_hash(), None);
        assert_eq!(block1.block_hash(), HASH_1_4);
        assert_eq!(block1.sequence_hash(), SEQ_HASH_1_4); // 首块的 seq_hash 等于 block_hash。
        assert_eq!(block1.position(), 0); // 首块的位置为 0。

        // 验证块 1 的位置血缘哈希。
        let plh1 = block1.positional_lineage_hash();
        assert_eq!(plh1.position(), 0);
        assert_eq!(plh1.parent_hash_fragment(), 0); // 根块没有父块。
        assert_eq!(
            plh1.current_hash_fragment(),
            SEQ_HASH_1_4 & ((1u64 << 59) - 1)
        ); // 模式 0：59 位。

        let tokens2 = Tokens::from(vec![5, 6, 7, 8]);
        let chunk2 = TokenBlockChunk::new(tokens2.clone(), salt);
        let block2 = TokenBlock::from_chunk(chunk2, block1.parent_sequence_hash(), 1); // 父块不正确。
        // 如果父块错误，序列哈希应当不同。
        assert_ne!(block2.sequence_hash(), SEQ_HASH_5_8);

        let chunk2_correct = TokenBlockChunk::new(tokens2.clone(), salt);
        let block2_correct =
            TokenBlock::from_chunk(chunk2_correct, Some(block1.sequence_hash()), 1);

        assert_eq!(block2_correct.tokens(), &tokens2);
        assert_eq!(block2_correct.salt_hash(), salt);
        assert_eq!(
            block2_correct.parent_sequence_hash(),
            Some(block1.sequence_hash())
        );
        assert_eq!(block2_correct.block_hash(), HASH_5_8);
        assert_eq!(block2_correct.sequence_hash(), SEQ_HASH_5_8);
        assert_eq!(block2_correct.position(), 1); // Second block is at position 1

        // 验证块 2 的位置血缘哈希。
        let plh2 = block2_correct.positional_lineage_hash();
        assert_eq!(plh2.position(), 1);
        assert_eq!(
            plh2.parent_hash_fragment(),
            SEQ_HASH_1_4 & ((1u64 << 59) - 1)
        ); // 父片段与 block1 的序列哈希一致。
        assert_eq!(
            plh2.current_hash_fragment(),
            SEQ_HASH_5_8 & ((1u64 << 59) - 1)
        ); // 模式 0：59 位。
    }

    #[test]
    fn test_new_sequence() {
        // 空初始 token。
        let seq_empty = create_test_sequence(&[], 4, Some(TEST_SALT_HASH));
        assert!(seq_empty.blocks().is_empty());
        assert!(seq_empty.current_block().is_empty());
        assert_eq!(seq_empty.total_tokens(), 0);
        assert_eq!(seq_empty.salt_hash(), TEST_SALT_HASH);
        assert_eq!(seq_empty.current_block().parent_sequence_hash, None);

        // 少于一个块。
        let seq_partial = create_test_sequence(&[1, 2], 4, Some(TEST_SALT_HASH));
        assert!(seq_partial.blocks().is_empty());
        assert_eq!(seq_partial.current_block().tokens().as_ref(), &[1, 2]);
        assert_eq!(seq_partial.total_tokens(), 2);
        assert_eq!(seq_partial.current_block().parent_sequence_hash, None);

        // 恰好一个块。
        let seq_one_block = create_test_sequence(&[1, 2, 3, 4], 4, Some(TEST_SALT_HASH));
        assert_eq!(seq_one_block.blocks().len(), 1);
        assert!(seq_one_block.current_block().is_empty());
        assert_eq!(seq_one_block.total_tokens(), 4);
        assert_eq!(seq_one_block.blocks[0].tokens().as_ref(), &[1, 2, 3, 4]);
        assert_eq!(seq_one_block.blocks[0].sequence_hash(), SEQ_HASH_1_4);
        assert_eq!(
            seq_one_block.current_block().parent_sequence_hash,
            Some(SEQ_HASH_1_4)
        );

        // 多于一个块。
        let seq_multi = create_test_sequence(&[1, 2, 3, 4, 5, 6, 7, 8, 9], 4, Some(TEST_SALT_HASH));
        assert_eq!(seq_multi.blocks().len(), 2);
        assert_eq!(seq_multi.current_block().tokens().as_ref(), &[9]);
        assert_eq!(seq_multi.total_tokens(), 9);
        assert_eq!(seq_multi.blocks[0].sequence_hash(), SEQ_HASH_1_4);
        assert_eq!(seq_multi.blocks[1].sequence_hash(), SEQ_HASH_5_8);
        assert_eq!(
            seq_multi.current_block().parent_sequence_hash,
            Some(SEQ_HASH_5_8)
        );

        // 测试跨完整块和局部块的 tokens_at。
        assert_eq!(seq_multi.tokens_at(0..4).as_ref(), &[1, 2, 3, 4]); // First complete block
        assert_eq!(seq_multi.tokens_at(4..8).as_ref(), &[5, 6, 7, 8]); // Second complete block
        assert_eq!(seq_multi.tokens_at(8..9).as_ref(), &[9]); // Current partial block
        assert_eq!(seq_multi.tokens_at(2..6).as_ref(), &[3, 4, 5, 6]); // Spanning blocks
        assert_eq!(seq_multi.tokens_at(6..9).as_ref(), &[7, 8, 9]); // Spanning to partial
        assert_eq!(seq_multi.tokens_at(5..5).as_ref(), &[0u32; 0]); // Empty range
        assert_eq!(seq_multi.tokens_at(10..15).as_ref(), &[0u32; 0]); // Out of bounds

        // 无 salt hash。
        let seq_no_salt = create_test_sequence(&[1, 2, 3, 4, 5], 4, None);
        assert_eq!(seq_no_salt.salt_hash(), 0);
        assert_eq!(seq_no_salt.blocks().len(), 1);
        assert_ne!(seq_no_salt.blocks[0].block_hash(), HASH_1_4); // Hash differs with salt 0
        assert_eq!(seq_no_salt.current_block().tokens().as_ref(), &[5]);
    }

    #[test]
    #[should_panic]
    fn test_new_sequence_zero_block_size() {
        let _ = create_test_sequence(&[1], 0, None);
    }

    #[test]
    fn test_append_single_token() {
        let mut sequence =
            create_test_sequence(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 4, Some(TEST_SALT_HASH));
        assert_eq!(sequence.blocks().len(), 2);
        assert_eq!(sequence.current_block().tokens.len(), 2);
        assert_eq!(sequence.current_block().tokens, vec![9, 10]);
        assert_eq!(
            sequence.current_block().parent_sequence_hash,
            Some(SEQ_HASH_5_8)
        );

        // 追加 token 11 - 不应完成一个块。
        let completed_idx = sequence.append(11).unwrap();
        assert_eq!(completed_idx, None);
        assert_eq!(sequence.blocks().len(), 2);
        assert_eq!(sequence.current_block().tokens.as_ref(), &[9, 10, 11]);

        // 追加 token 12 - 应完成第 2 个块（索引 2）。
        // 这也会提交块 2。
        let completed_idx = sequence.append(12).unwrap();
        assert_eq!(completed_idx, Some(2));
        assert_eq!(sequence.blocks().len(), 3);
        assert_eq!(sequence.current_block.tokens.as_ref(), &[0u32; 0]);
        assert_eq!(sequence.current_block.remaining(), 4);
        assert_eq!(
            sequence.current_block().parent_sequence_hash,
            Some(SEQ_HASH_9_12)
        ); // Still linked to block 1

        // 追加 token 13 - 不应完成一个块。
        let completed_idx_13 = sequence.append(13).unwrap();
        assert_eq!(completed_idx_13, None);
        assert_eq!(sequence.blocks().len(), 3);
        assert_eq!(sequence.blocks[2].tokens().as_ref(), &[9, 10, 11, 12]);
        assert_eq!(sequence.blocks[2].sequence_hash(), SEQ_HASH_9_12);
        assert_eq!(sequence.current_block.tokens.as_ref(), &[13]); // New current block has 13
        assert_eq!(sequence.current_block.remaining(), 3);
        assert_eq!(
            sequence.current_block.parent_sequence_hash,
            Some(SEQ_HASH_9_12)
        ); // Linked to new block 2
    }

    #[test]
    fn test_extend() {
        let block_size = 4;
        let salt_hash = Some(TEST_SALT_HASH);

        // 情况 1：追加少于块大小的 token。
        let mut seq1 = create_test_sequence(&[], block_size, salt_hash);
        let tokens1 = Tokens::from(vec![1, 2]);
        let completed1 = seq1.extend(tokens1).unwrap();
        assert_eq!(completed1, None); // No blocks completed
        assert_eq!(seq1.blocks.len(), 0);
        assert_eq!(seq1.current_block.tokens.as_ref(), &[1, 2]);
        assert_eq!(seq1.current_block.remaining(), 2);
        assert_eq!(seq1.current_block.parent_sequence_hash, None); // Still the root block

        // 情况 2：追加恰好等于块大小的 token。
        let mut seq2 = create_test_sequence(&[], block_size, salt_hash);
        let tokens2 = Tokens::from(vec![1, 2, 3, 4]);
        let completed2 = seq2.extend(tokens2).unwrap();
        assert_eq!(completed2, Some(0..1));
        assert_eq!(seq2.blocks.len(), 1);
        assert_eq!(seq2.current_block.tokens.as_ref(), &[0u32; 0]); // Current block is empty
        assert_eq!(seq2.current_block.remaining(), 4);
        assert_eq!(seq2.current_block.parent_sequence_hash, Some(SEQ_HASH_1_4)); // Still the root block

        // 情况 3：追加多于块大小但少于两个块的 token。
        let mut seq3 = create_test_sequence(&[], block_size, salt_hash);
        let tokens3 = Tokens::from(vec![1, 2, 3, 4, 5, 6]);
        let completed3 = seq3.extend(tokens3).unwrap();
        assert_eq!(completed3, Some(0..1)); // Block at index 0 completed
        assert_eq!(seq3.blocks.len(), 1);
        assert_eq!(seq3.current_block.tokens.as_ref(), &[5, 6]); // Partial block has remainder
        assert_eq!(seq3.blocks[0].tokens().as_ref(), &[1, 2, 3, 4]);
        assert_eq!(seq3.current_block.parent_sequence_hash, Some(SEQ_HASH_1_4));
        assert_eq!(seq3.current_block.remaining(), 2);

        // 情况 4：追加恰好两个块的 token。
        let mut seq4 = create_test_sequence(&[], block_size, salt_hash);
        let tokens4 = Tokens::from(vec![1, 2, 3, 4, 5, 6, 7, 8]);
        let completed4 = seq4.extend(tokens4).unwrap();
        assert_eq!(completed4, Some(0..2)); // Only block 0 is committed
        assert_eq!(seq4.blocks.len(), 2); // Only 1 block committed
        assert_eq!(seq4.current_block.tokens.as_ref(), &[0u32; 0]);
        assert_eq!(seq4.current_block.remaining(), 4);
        assert_eq!(seq4.blocks[0].tokens().as_ref(), &[1, 2, 3, 4]);
        assert_eq!(seq4.blocks[0].sequence_hash(), SEQ_HASH_1_4);
        assert_eq!(seq4.current_block.parent_sequence_hash, Some(SEQ_HASH_5_8)); // Parent is the first block

        // 情况 5：多次追加，跨调用完成块。
        let mut seq5 = create_test_sequence(&[], block_size, salt_hash);
        let tokens5a = Tokens::from(vec![1, 2]);
        let completed5a = seq5.extend(tokens5a).unwrap();
        assert_eq!(completed5a, None);
        assert_eq!(seq5.blocks.len(), 0);
        assert_eq!(seq5.current_block.tokens.as_ref(), &[1, 2]);

        let tokens5b = Tokens::from(vec![3, 4, 5]);
        let completed5b = seq5.extend(tokens5b).unwrap();
        assert_eq!(completed5b, Some(0..1)); // Block at index 0 completed
        assert_eq!(seq5.blocks.len(), 1);
        assert_eq!(seq5.current_block.tokens.as_ref(), &[5]);
        assert_eq!(seq5.blocks[0].tokens().as_ref(), &[1, 2, 3, 4]);
        assert_eq!(seq5.current_block.parent_sequence_hash, Some(SEQ_HASH_1_4));
        assert_eq!(seq5.current_block.remaining(), 3);

        let tokens5c = Tokens::from(vec![6, 7, 8, 9, 10]);
        let completed5c = seq5.extend(tokens5c).unwrap();
        assert_eq!(completed5c, Some(1..2)); // Block at index 1 completed
        assert_eq!(seq5.blocks.len(), 2);
        assert_eq!(seq5.current_block.tokens.as_ref(), &[9, 10]);
        assert_eq!(seq5.blocks[1].tokens().as_ref(), &[5, 6, 7, 8]);
        assert_eq!(seq5.current_block.parent_sequence_hash, Some(SEQ_HASH_5_8));
        assert_eq!(seq5.current_block.remaining(), 2);

        // 情况 6：追加空 token 集合。
        let mut seq6 = create_test_sequence(&[1], block_size, salt_hash);
        let completed6 = seq6.extend(Tokens::default()).unwrap();
        assert_eq!(completed6, None);
        assert_eq!(seq6.blocks.len(), 0);
        assert_eq!(seq6.current_block.tokens.as_ref(), &[1]);
        assert_eq!(seq6.total_tokens(), 1);

        // 情况 7：追加后刚好填满当前块，没有剩余。
        let mut seq7 = create_test_sequence(&[1, 2], block_size, salt_hash);
        let tokens7 = Tokens::from(vec![3, 4]);
        let completed7 = seq7.extend(tokens7).unwrap();
        assert_eq!(completed7, Some(0..1)); // Block is full but not committed yet
        assert_eq!(seq7.blocks.len(), 1);
        assert_eq!(seq7.current_block.tokens.as_ref(), &[0u32; 0]); // Current block is full
        assert_eq!(seq7.current_block.remaining(), 4);
        assert_eq!(seq7.total_tokens(), 4);
        assert_eq!(seq7.current_block.parent_sequence_hash, Some(SEQ_HASH_1_4)); // Still the root block

        // 测试 tokens_at 提取。
        assert_eq!(seq7.tokens_at(0..2).as_ref(), &[1, 2]);
        assert_eq!(seq7.tokens_at(1..3).as_ref(), &[2, 3]);
        assert_eq!(seq7.tokens_at(0..4).as_ref(), &[1, 2, 3, 4]);
        assert_eq!(seq7.tokens_at(2..2).as_ref(), &[0u32; 0]); // Empty range
    }

    #[test]
    fn test_truncate() {
        let block_size = 4;
        let salt_hash = Some(TEST_SALT_HASH);
        let initial_tokens = &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]; // 10 tokens

        // 情况 1：在当前块内截短（长度 9）。
        let mut seq1 = create_test_sequence(initial_tokens, block_size, salt_hash);
        assert!(seq1.truncate(9).is_ok());
        assert_eq!(seq1.total_tokens(), 9);
        assert_eq!(seq1.blocks().len(), 2);
        assert_eq!(seq1.current_block().tokens.as_ref(), &[9]);
        assert_eq!(
            seq1.current_block().parent_sequence_hash,
            Some(SEQ_HASH_5_8)
        );

        // 情况 2：截短到恰好块边界（长度 8）。
        let mut seq2 = create_test_sequence(initial_tokens, block_size, salt_hash);
        assert!(seq2.truncate(8).is_ok());
        assert_eq!(seq2.total_tokens(), 8);
        assert_eq!(seq2.blocks().len(), 2);
        assert!(seq2.current_block().tokens.is_empty());
        assert_eq!(
            seq2.current_block().parent_sequence_hash,
            Some(SEQ_HASH_5_8)
        );

        // 情况 3：截短到最后一个完整块内部（长度 7）。
        let mut seq3 = create_test_sequence(initial_tokens, block_size, salt_hash);
        assert!(seq3.truncate(7).is_ok());
        assert_eq!(seq3.total_tokens(), 7);
        assert_eq!(seq3.blocks().len(), 1); // Block [5,6,7,8] removed conceptually
        assert_eq!(seq3.current_block().tokens.as_ref(), &[5, 6, 7]); // Kept 3 from [5,6,7,8]
        assert_eq!(
            seq3.current_block().parent_sequence_hash,
            Some(SEQ_HASH_1_4)
        ); // Parent is hash of [1,2,3,4]
        assert_eq!(seq3.blocks()[0].tokens().as_ref(), &[1, 2, 3, 4]);

        // 情况 4：恰好移除完整块（长度 4）。
        let mut seq4 = create_test_sequence(initial_tokens, block_size, salt_hash);
        assert!(seq4.truncate(4).is_ok());
        assert_eq!(seq4.total_tokens(), 4);
        assert_eq!(seq4.blocks().len(), 1); // Block [5,6,7,8] removed
        assert!(seq4.current_block().tokens.is_empty()); // New partial based on block [1,2,3,4]
        assert_eq!(
            seq4.current_block().parent_sequence_hash,
            Some(SEQ_HASH_1_4)
        );
        assert_eq!(seq4.blocks()[0].tokens().as_ref(), &[1, 2, 3, 4]);

        // 情况 5：截短到第一个块内部（长度 3）。
        let mut seq5 = create_test_sequence(initial_tokens, block_size, salt_hash);
        assert!(seq5.truncate(3).is_ok());
        assert_eq!(seq5.total_tokens(), 3);
        assert!(seq5.blocks().is_empty()); // Both blocks removed conceptually
        assert_eq!(seq5.current_block().tokens.as_ref(), &[1, 2, 3]); // Kept 3 from [1,2,3,4]
        assert_eq!(seq5.current_block().parent_sequence_hash, None); // No parent

        // 情况 6：截短到长度 0。
        let mut seq6 = create_test_sequence(initial_tokens, block_size, salt_hash);
        assert!(seq6.truncate(0).is_ok());
        assert_eq!(seq6.total_tokens(), 0);
        assert!(seq6.blocks().is_empty());
        assert!(seq6.current_block().tokens.is_empty());
        assert_eq!(seq6.current_block().parent_sequence_hash, None);

        // 情况 7：截短到大于当前长度（长度 11）。
        let mut seq7 = create_test_sequence(initial_tokens, block_size, salt_hash);
        let original_state = (seq7.blocks.clone(), seq7.current_block.tokens.clone()); // Clone for state check
        assert!(seq7.truncate(11).is_ok()); // Should have no effect
        assert_eq!(seq7.total_tokens(), 10);
        assert_eq!(seq7.blocks, original_state.0);
        assert_eq!(seq7.current_block.tokens, original_state.1);

        // 情况 8：截短到当前长度（长度 10）。
        let mut seq8 = create_test_sequence(initial_tokens, block_size, salt_hash);
        let original_state = (seq8.blocks.clone(), seq8.current_block.tokens.clone());
        assert!(seq8.truncate(10).is_ok());
        assert_eq!(seq8.total_tokens(), 10);
        assert_eq!(seq8.blocks, original_state.0);
        assert_eq!(seq8.current_block.tokens, original_state.1);

        // 情况 9：将空序列截短到 0。
        let mut seq9 = create_test_sequence(&[], block_size, salt_hash);
        assert!(seq9.truncate(0).is_ok());
        assert_eq!(seq9.total_tokens(), 0);
        assert!(seq9.blocks().is_empty());
        assert!(seq9.current_block().tokens.is_empty());

        // 情况 10：当前块为空时，截短到恰好块边界（长度 4）。
        let tokens10 = &[1, 2, 3, 4, 5, 6, 7, 8]; // 8 tokens
        let mut seq10 = create_test_sequence(tokens10, block_size, salt_hash);
        assert_eq!(seq10.total_tokens(), 8);
        assert!(seq10.current_block().is_empty());
        assert!(seq10.truncate(4).is_ok()); // Remove block [5, 6, 7, 8]
        assert_eq!(seq10.total_tokens(), 4);
        assert_eq!(seq10.blocks().len(), 1);
        assert!(seq10.current_block().tokens.is_empty());
        assert_eq!(
            seq10.current_block().parent_sequence_hash,
            Some(SEQ_HASH_1_4)
        );

        // 情况 11：当前块为空时，截短到第一个块内部（长度 3）。
        let tokens11 = &[1, 2, 3, 4, 5, 6, 7, 8]; // 8 tokens
        let mut seq11 = create_test_sequence(tokens11, block_size, salt_hash);
        assert!(seq11.truncate(3).is_ok()); // Pop block [5,6,7,8] + 1 from [1,2,3,4]
        assert_eq!(seq11.total_tokens(), 3);
        assert!(seq11.blocks().is_empty());
        assert_eq!(seq11.current_block().tokens.as_ref(), &[1, 2, 3]); // Kept 3 from [1,2,3,4]
        assert_eq!(seq11.current_block().parent_sequence_hash, None);
    }

    #[test]
    fn test_unwind() {
        let block_size = 4;
        let salt_hash = Some(TEST_SALT_HASH);
        let initial_tokens = &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]; // 10 tokens

        // Unwind 0
        let mut seq = create_test_sequence(initial_tokens, block_size, salt_hash);
        assert!(seq.unwind(0).is_ok());
        assert_eq!(seq.total_tokens(), 10);

        // Unwind 1
        let mut seq = create_test_sequence(initial_tokens, block_size, salt_hash);
        assert!(seq.unwind(1).is_ok());
        assert_eq!(seq.total_tokens(), 9);
        assert_eq!(seq.current_block.tokens.as_ref(), &[9]);

        // 回退 3 个 token（跨越边界）。
        let mut seq = create_test_sequence(initial_tokens, block_size, salt_hash);
        assert!(seq.unwind(3).is_ok());
        assert_eq!(seq.total_tokens(), 7);
        assert_eq!(seq.blocks.len(), 1);
        assert_eq!(seq.current_block.tokens.as_ref(), &[5, 6, 7]);

        // Unwind all (10)
        let mut seq = create_test_sequence(initial_tokens, block_size, salt_hash);
        assert!(seq.unwind(10).is_ok());
        assert_eq!(seq.total_tokens(), 0);
        assert!(seq.blocks.is_empty());
        assert!(seq.current_block.is_empty());

        // 回退超过可用数量（11）。
        let mut seq = create_test_sequence(initial_tokens, block_size, salt_hash);
        assert_eq!(seq.unwind(11), Err(TokenBlockError::InsufficientTokens));
        assert_eq!(seq.total_tokens(), 10); // State unchanged

        // 从空序列回退。
        let mut seq_empty = create_test_sequence(&[], block_size, salt_hash);
        assert_eq!(
            seq_empty.unwind(1),
            Err(TokenBlockError::InsufficientTokens)
        );
    }

    #[test]
    fn test_pop() {
        let block_size = 4;
        let salt_hash = Some(TEST_SALT_HASH);
        let initial_tokens = &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]; // 10 tokens

        let mut seq = create_test_sequence(initial_tokens, block_size, salt_hash);

        // Pop 10
        assert_eq!(seq.pop(), Some(10));
        assert_eq!(seq.total_tokens(), 9);
        assert_eq!(seq.current_block.tokens.as_ref(), &[9]);
        assert_eq!(seq.blocks.len(), 2);

        // Pop 9
        assert_eq!(seq.pop(), Some(9));
        assert_eq!(seq.total_tokens(), 8);
        assert!(seq.current_block.is_empty());
        assert_eq!(seq.blocks.len(), 2);
        assert_eq!(seq.current_block.parent_sequence_hash, Some(SEQ_HASH_5_8));

        // 弹出 8 个 token（跨越边界）。
        assert_eq!(seq.pop(), Some(8));
        assert_eq!(seq.total_tokens(), 7);
        assert_eq!(seq.current_block.tokens.as_ref(), &[5, 6, 7]);
        assert_eq!(seq.blocks.len(), 1);
        assert_eq!(seq.current_block.parent_sequence_hash, Some(SEQ_HASH_1_4));

        // 弹出剩余局部部分（7、6、5）。
        assert_eq!(seq.pop(), Some(7));
        assert_eq!(seq.pop(), Some(6));
        assert_eq!(seq.pop(), Some(5));
        assert_eq!(seq.total_tokens(), 4);
        assert!(seq.current_block.is_empty());
        assert_eq!(seq.blocks.len(), 1);
        assert_eq!(seq.current_block.parent_sequence_hash, Some(SEQ_HASH_1_4));

        // 弹出 4 个 token（跨越边界）。
        assert_eq!(seq.pop(), Some(4));
        assert_eq!(seq.total_tokens(), 3);
        assert_eq!(seq.current_block.tokens.as_ref(), &[1, 2, 3]);
        assert!(seq.blocks.is_empty());
        assert_eq!(seq.current_block.parent_sequence_hash, None);

        // Pop 3, 2, 1
        assert_eq!(seq.pop(), Some(3));
        assert_eq!(seq.pop(), Some(2));
        assert_eq!(seq.pop(), Some(1));
        assert_eq!(seq.total_tokens(), 0);
        assert!(seq.current_block.is_empty());
        assert!(seq.blocks.is_empty());

        // 从空序列弹出。
        assert_eq!(seq.pop(), None);
        assert_eq!(seq.total_tokens(), 0);
    }

    #[test]
    fn test_total_tokens() {
        let block_size = 3;
        let salt_hash = Some(TEST_SALT_HASH);

        let mut seq = create_test_sequence(&[], block_size, salt_hash);
        assert_eq!(seq.total_tokens(), 0);

        seq.extend(Tokens::from(vec![1, 2])).unwrap();
        assert_eq!(seq.total_tokens(), 2);

        seq.append(3).unwrap(); // Completes block 0
        assert_eq!(seq.total_tokens(), 3);

        seq.extend(Tokens::from(vec![4, 5, 6, 7])).unwrap(); // Completes block 1, partial [7]
        assert_eq!(seq.total_tokens(), 7);

        seq.pop().unwrap(); // Removes 7
        assert_eq!(seq.total_tokens(), 6);

        seq.truncate(4).unwrap(); // Keep [1,2,3,4]
        assert_eq!(seq.total_tokens(), 4);

        seq.unwind(2).unwrap(); // Keep [1,2]
        assert_eq!(seq.total_tokens(), 2);
    }

    #[test]
    fn test_push_tokens_partial_block() {
        let mut partial = PartialTokenBlock::create_sequence_root(4, 1337);

        let tokens = Tokens(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

        let remaining = partial.push_tokens(tokens);
        assert_eq!(partial.tokens.len(), 4);
        assert_eq!(remaining.len(), 6);
    }

    #[test]
    fn test_positional_radix_tree_basic_operations() {
        use crate::PositionalRadixTree;

        // 测试 new() 和 is_empty()。
        let tree: PositionalRadixTree<String> = PositionalRadixTree::new();
        assert!(tree.is_empty());
        assert_eq!(tree.len(), 0);

        // 测试 default()。
        let tree2: PositionalRadixTree<i32> = PositionalRadixTree::default();
        assert!(tree2.is_empty());

        // 测试 prefix() 和插入。
        let psh1 = PositionalSequenceHash::new(0x1234, 0, 0xABCD);
        let psh2 = PositionalSequenceHash::new(0x5678, 0, 0xEF01);
        let psh3 = PositionalSequenceHash::new(0x9ABC, 1, 0x2345);

        tree.prefix(&psh1).insert(psh1, "value1".to_string());
        assert!(!tree.is_empty());
        assert_eq!(tree.len(), 1);

        tree.prefix(&psh2).insert(psh2, "value2".to_string());
        assert_eq!(tree.len(), 2);

        tree.prefix(&psh3).insert(psh3, "value3".to_string());
        assert_eq!(tree.len(), 3);

        // 测试检索。
        assert_eq!(
            tree.prefix(&psh1).get(&psh1).map(|v| v.clone()),
            Some("value1".to_string())
        );
    }

    #[test]
    fn test_positional_radix_tree_with_lineage_hash() {
        use crate::PositionalRadixTree;

        // 测试 PositionalLineageHash 的泛型用法。
        let tree: PositionalRadixTree<u32, PositionalLineageHash> = PositionalRadixTree::new();
        assert!(tree.is_empty());

        let plh1 = PositionalLineageHash::new(0x1234, None, 0);
        let plh2 = PositionalLineageHash::new(0x5678, Some(0x1234), 1);

        tree.prefix(&plh1).insert(plh1, 100);
        tree.prefix(&plh2).insert(plh2, 200);

        assert_eq!(tree.len(), 2);
        assert_eq!(tree.prefix(&plh1).get(&plh1).map(|v| *v), Some(100));
        assert_eq!(tree.prefix(&plh2).get(&plh2).map(|v| *v), Some(200));
    }

    #[test]
    fn test_positional_radix_tree_position_lookup() {
        use crate::PositionalRadixTree;

        let tree: PositionalRadixTree<String> = PositionalRadixTree::new();

        // 在不同位置插入。
        let psh0 = PositionalSequenceHash::new(0x1111, 0, 0xAAAA);
        let psh1 = PositionalSequenceHash::new(0x2222, 1, 0xBBBB);
        let psh2 = PositionalSequenceHash::new(0x3333, 2, 0xCCCC);

        tree.prefix(&psh0).insert(psh0, "pos0".to_string());
        tree.prefix(&psh1).insert(psh1, "pos1".to_string());
        tree.prefix(&psh2).insert(psh2, "pos2".to_string());

        // 测试 position() 方法。
        assert!(tree.position(0).is_some());
        assert!(tree.position(1).is_some());
        assert!(tree.position(2).is_some());
        assert!(tree.position(3).is_none()); // No entries at position 3

        // 验证按位置查找会返回正确的子映射。
        let pos0_map = tree.position(0).unwrap();
        assert_eq!(pos0_map.len(), 1);
    }

    // === PositionalSequenceHash 补充测试 ===

    #[test]
    fn test_positional_sequence_hash_mode_2_and_3() {
        // 模式 2：位置可容纳在 24 位中（65536 <= pos < 16777216）。
        let position_mode2 = 100_000u64;
        let seq_hash = 0x1234567890ABCDEF;
        let block_hash = 0xFEDCBA9876543210;

        let psh_mode2 = PositionalSequenceHash::new(seq_hash, position_mode2, block_hash);
        assert_eq!(psh_mode2.mode(), 2, "Position 100,000 should use mode 2");
        assert_eq!(psh_mode2.position(), position_mode2);
        assert_eq!(psh_mode2.sequence_hash(), seq_hash);
        // 模式 2 中局部块哈希会截断为 38 位。
        assert_eq!(
            psh_mode2.local_block_hash(),
            block_hash & ((1u64 << 38) - 1)
        );

        // 模式 3：位置可容纳在 31 位中（16777216 <= pos < 2147483648）。
        let position_mode3 = 100_000_000u64;
        let psh_mode3 = PositionalSequenceHash::new(seq_hash, position_mode3, block_hash);
        assert_eq!(
            psh_mode3.mode(),
            3,
            "Position 100,000,000 should use mode 3"
        );
        assert_eq!(psh_mode3.position(), position_mode3);
        assert_eq!(psh_mode3.sequence_hash(), seq_hash);
        // 模式 3 中局部块哈希会截断为 31 位。
        assert_eq!(
            psh_mode3.local_block_hash(),
            block_hash & ((1u64 << 31) - 1)
        );
    }

    #[test]
    fn test_positional_sequence_hash_as_u128() {
        let psh = PositionalSequenceHash::new(0x1234, 100, 0xABCD);
        let raw = psh.as_u128();

        // 验证可以从原始值重建。
        assert_eq!(raw & 0xFFFF_FFFF_FFFF_FFFF, 0x1234);
        assert!(raw > 0); // Non-zero

        // 再构造一个并比较。
        let psh2 = PositionalSequenceHash::new(0x1234, 100, 0xABCD);
        assert_eq!(psh.as_u128(), psh2.as_u128());
    }

    #[test]
    fn test_positional_sequence_hash_debug() {
        let psh = PositionalSequenceHash::new(0x1234567890ABCDEF, 42, 0xFEDCBA98);
        let debug_str = format!("{:?}", psh);

        // Debug 输出应包含字段名和值。
        assert!(debug_str.contains("PositionalSequenceHash"));
        assert!(debug_str.contains("sequence_hash"));
        assert!(debug_str.contains("local_block_hash"));
        assert!(debug_str.contains("position"));
    }

    // === PositionalLineageHash 补充测试 ===

    #[test]
    fn test_positional_lineage_hash_debug_and_display() {
        // 测试位置 0（不显示父块）。
        let plh_root = PositionalLineageHash::new(0x123456789ABCDEF0, None, 0);
        let debug_root = format!("{:?}", plh_root);
        let display_root = format!("{}", plh_root);

        // Debug 和 Display 都应显示位置 0。
        assert!(debug_root.starts_with("0:"));
        assert!(display_root.starts_with("0:"));
        // 位置 0 不应显示父块。
        assert_eq!(debug_root.matches(':').count(), 1);
        assert_eq!(display_root.matches(':').count(), 1);

        // 测试位置大于 0（显示父块）。
        let plh_child = PositionalLineageHash::new(0xABCDEF0123456789, Some(0x123456789ABCDEF0), 5);
        let debug_child = format!("{:?}", plh_child);
        let display_child = format!("{}", plh_child);

        // 应显示 position:current:parent。
        assert!(debug_child.starts_with("5:"));
        assert!(display_child.starts_with("5:"));
        // 位置大于 0 时应显示父块（3 部分）。
        assert_eq!(debug_child.matches(':').count(), 2);
        assert_eq!(display_child.matches(':').count(), 2);
    }

    #[test]
    fn test_positional_lineage_hash_as_u128() {
        let plh = PositionalLineageHash::new(0x1234, Some(0x5678), 10);
        let raw = plh.as_u128();

        assert!(raw > 0);

        // 再用相同参数构造一个并比较。
        let plh2 = PositionalLineageHash::new(0x1234, Some(0x5678), 10);
        assert_eq!(plh.as_u128(), plh2.as_u128());

        // 不同参数应得到不同哈希。
        let plh3 = PositionalLineageHash::new(0x1234, Some(0x5678), 11);
        assert_ne!(plh.as_u128(), plh3.as_u128());
    }

    #[test]
    fn test_positional_lineage_hash_ord_by_position_then_current_fragment() {
        let at_5_low = PositionalLineageHash::new(0x10, Some(0x1111), 5);
        let at_5_high = PositionalLineageHash::new(0x20, Some(0x1111), 5);
        assert!(
            at_5_low.current_hash_fragment() < at_5_high.current_hash_fragment(),
            "test assumes distinct current fragments at the same position"
        );
        assert!(at_5_low < at_5_high);
        assert!(at_5_high > at_5_low);

        let at_3 = PositionalLineageHash::new(0x99, Some(0x2222), 3);
        assert!(at_3 < at_5_low);
        assert!(at_5_high < PositionalLineageHash::new(0x01, Some(0x3333), 6));
    }

    #[test]
    fn test_positional_lineage_hash_ord_tiebreak_parent_via_packed_u128() {
        let same_pos_same_current = PositionalLineageHash::new(0x1234, Some(0x100), 10);
        let same_pos_same_current_other_parent =
            PositionalLineageHash::new(0x1234, Some(0x200), 10);
        assert_eq!(same_pos_same_current.position(), 10);
        assert_eq!(
            same_pos_same_current.position(),
            same_pos_same_current_other_parent.position()
        );
        assert_eq!(
            same_pos_same_current.current_hash_fragment(),
            same_pos_same_current_other_parent.current_hash_fragment()
        );
        assert_ne!(same_pos_same_current, same_pos_same_current_other_parent);
        assert_ne!(
            same_pos_same_current.cmp(&same_pos_same_current_other_parent),
            std::cmp::Ordering::Equal
        );
    }

    #[test]
    fn test_positional_lineage_hash_vec_sort_matches_ord() {
        let a = PositionalLineageHash::new(0x30, None, 0);
        let b = PositionalLineageHash::new(0x10, Some(0x30), 2);
        let c = PositionalLineageHash::new(0x20, Some(0x30), 2);
        let mut v = vec![b, a, c];
        v.sort();
        assert_eq!(v, vec![a, b, c]);
    }

    #[test]
    fn test_positional_lineage_hash_itertools_sorted() {
        use itertools::Itertools;

        let a = PositionalLineageHash::new(0x30, None, 0);
        let b = PositionalLineageHash::new(0x10, Some(0x30), 2);
        let c = PositionalLineageHash::new(0x20, Some(0x30), 2);
        let sorted: Vec<_> = vec![b, a, c].into_iter().sorted().collect();
        assert_eq!(sorted, vec![a, b, c]);
    }

    // === Tokens 的 From 实现测试 ===

    #[test]
    fn test_tokens_from_vec_usize() {
        let usize_vec: Vec<usize> = vec![1, 2, 3, 4, 5];
        let tokens = Tokens::from(usize_vec);

        assert_eq!(tokens.as_ref(), &[1u32, 2, 3, 4, 5]);
        assert_eq!(tokens.len(), 5);
    }

    #[test]
    fn test_tokens_partial_eq_slice_ref() {
        let tokens = Tokens::from(vec![1u32, 2, 3, 4]);
        let slice: &[Token] = &[1, 2, 3, 4];

        // 测试 Tokens 对 `&[Token]` 的 PartialEq 实现。
        assert!(tokens == slice);

        let different_slice: &[Token] = &[1, 2, 3, 5];
        assert!(tokens != different_slice);
    }

    // === TokenBlock 访问器测试 ===

    #[test]
    fn test_token_block_accessors() {
        let tokens = Tokens::from(vec![1u32, 2, 3, 4]);
        let seq = TokenBlockSequence::new(tokens, 4, Some(1337));

        let block = &seq.blocks()[0];

        // 测试 block_size()。
        assert_eq!(block.block_size(), 4);

        // 测试 positional_sequence_hash()。
        let psh = block.positional_sequence_hash();
        assert_eq!(psh.position(), 0);

        // 测试 positional_lineage_hash()。
        let plh = block.positional_lineage_hash();
        assert_eq!(plh.position(), 0);
        assert_eq!(plh.parent_hash_fragment(), 0); // 根块没有父块。
    }

    #[test]
    fn test_positional_hash_trait_impls() {
        use crate::PositionalHash;

        // 测试 PositionalSequenceHash 的 PositionalHash 实现。
        let psh = PositionalSequenceHash::new(0x1234, 42, 0xABCD);
        assert_eq!(PositionalHash::position(&psh), 42);

        // 测试 PositionalLineageHash 的 PositionalHash 实现。
        let plh = PositionalLineageHash::new(0x1234, None, 99);
        assert_eq!(PositionalHash::position(&plh), 99);
    }

    // === TokenBlockSequence 边界情况测试 ===

    #[test]
    fn test_sequence_pop_from_full_block() {
        // 测试当前局部块为空时的 pop（必须从完整块中弹出）。
        let tokens = Tokens::from(vec![1u32, 2, 3, 4, 5, 6, 7, 8]);
        let mut seq = TokenBlockSequence::new(tokens, 4, Some(TEST_SALT_HASH));

        // 当前块应为空，所有 token 都应位于已完成块中。
        assert!(seq.current_block().is_empty());
        assert_eq!(seq.blocks().len(), 2);
        assert_eq!(seq.total_tokens(), 8);

        // pop 应该从最后一个完整块中移除 token。
        let popped = seq.pop();
        assert_eq!(popped, Some(8));
        assert_eq!(seq.total_tokens(), 7);
        assert_eq!(seq.blocks().len(), 1);
        assert_eq!(seq.current_block().tokens.as_ref(), &[5, 6, 7]);
    }

    #[test]
    #[allow(clippy::reversed_empty_ranges)] // so we can explicitly test invalid ranges
    fn test_sequence_tokens_at_edge_cases() {
        let tokens = Tokens::from(vec![1u32, 2, 3, 4, 5]);
        let seq = TokenBlockSequence::new(tokens, 4, Some(TEST_SALT_HASH));

        // 起始位置大于结束位置（无效区间）。
        assert!(seq.tokens_at(3..2).is_empty());

        // 结束位置大于总长度（越界）。
        assert!(seq.tokens_at(0..10).is_empty());

        // 有效边界情况：正好在边界上。
        assert_eq!(seq.tokens_at(0..4).as_ref(), &[1, 2, 3, 4]);
        assert_eq!(seq.tokens_at(4..5).as_ref(), &[5]);
    }

    #[test]
    fn test_sequence_next_block() {
        let tokens = Tokens::from(vec![1u32, 2, 3, 4]);
        let seq = TokenBlockSequence::new(tokens, 4, Some(1337));

        let block = &seq.blocks()[0];
        let next_partial = block.next_block();

        // next_block 应创建一个链接到当前块的局部块。
        assert!(next_partial.is_empty());
        assert_eq!(next_partial.remaining(), 4);
        assert_eq!(
            next_partial.parent_sequence_hash,
            Some(block.sequence_hash())
        );
        assert_eq!(next_partial.position, 1);
    }

    #[test]
    fn test_sequence_reset() {
        let tokens = Tokens::from(vec![1u32, 2, 3, 4, 5, 6, 7, 8, 9]);
        let mut seq = TokenBlockSequence::new(tokens, 4, Some(1337));

        assert_eq!(seq.blocks().len(), 2);
        assert_eq!(seq.total_tokens(), 9);

        seq.reset();

        assert!(seq.blocks().is_empty());
        assert!(seq.current_block().is_empty());
        assert_eq!(seq.total_tokens(), 0);
        assert_eq!(seq.current_block().parent_sequence_hash, None);
    }

    #[test]
    fn test_sequence_into_parts() {
        let tokens = Tokens::from(vec![1u32, 2, 3, 4, 5]);
        let seq = TokenBlockSequence::new(tokens, 4, Some(1337));

        let (blocks, partial) = seq.into_parts();

        assert_eq!(blocks.len(), 1);
        assert_eq!(partial.tokens.as_ref(), &[5]);
    }

    #[test]
    fn test_sequence_last_complete_block() {
        // Empty sequence
        let seq_empty = TokenBlockSequence::new(Tokens::default(), 4, None);
        assert!(seq_empty.last_complete_block().is_none());

        // With blocks
        let tokens = Tokens::from(vec![1u32, 2, 3, 4, 5, 6, 7, 8]);
        let seq = TokenBlockSequence::new(tokens, 4, Some(1337));
        let last = seq.last_complete_block();
        assert!(last.is_some());
        assert_eq!(last.unwrap().tokens().as_ref(), &[5, 6, 7, 8]);
    }

    #[test]
    fn test_positional_hashes_msgpack_roundtrip() {
        let psh = PositionalSequenceHash::new(0xDEAD_BEEF_CAFE_BABE, 12345, 0x0123_4567_89AB_CDEF);
        let bytes = rmp_serde::to_vec(&psh).expect("psh serialize");
        let decoded: PositionalSequenceHash =
            rmp_serde::from_slice(&bytes).expect("psh deserialize");
        assert_eq!(psh, decoded);
        assert_eq!(psh.as_u128(), decoded.as_u128());

        let plh =
            PositionalLineageHash::new(0x1111_2222_3333_4444, Some(0x5555_6666_7777_8888), 256);
        let bytes = rmp_serde::to_vec(&plh).expect("plh serialize");
        let decoded: PositionalLineageHash =
            rmp_serde::from_slice(&bytes).expect("plh deserialize");
        assert_eq!(plh, decoded);
        assert_eq!(plh.as_u128(), decoded.as_u128());

        // Vec 往返验证，覆盖容器内部的编解码路径。
        let vec = vec![psh, PositionalSequenceHash::default(), psh];
        let bytes = rmp_serde::to_vec(&vec).expect("vec serialize");
        let decoded: Vec<PositionalSequenceHash> =
            rmp_serde::from_slice(&bytes).expect("vec deserialize");
        assert_eq!(vec, decoded);
    }

    #[test]
    fn test_positional_hashes_json_roundtrip() {
        // 验证字节数组编解码器也能通过 JSON（u8 数组）往返。
        let psh = PositionalSequenceHash::new(0xAAAA_BBBB_CCCC_DDDD, 7, 0xEEEE_FFFF_0000_1111);
        let json = serde_json::to_string(&psh).expect("psh json serialize");
        let decoded: PositionalSequenceHash =
            serde_json::from_str(&json).expect("psh json deserialize");
        assert_eq!(psh, decoded);

        let plh = PositionalLineageHash::new(0x1234_5678, Some(0xABCD_EF01), 42);
        let json = serde_json::to_string(&plh).expect("plh json serialize");
        let decoded: PositionalLineageHash =
            serde_json::from_str(&json).expect("plh json deserialize");
        assert_eq!(plh, decoded);
    }

    // === SECTION: 重写实现细节补充测试 ===
    // 以下测试专门覆盖本次重写引入的内部机制（表驱动位布局、位掩码助手、
    // radix 的 fold 求长、默认 UUID 唯一性、上半段 pack/unpack 往返等），
    // 与上方逐字保留的标准基准测试相互独立、互为补充。

    #[test]
    fn test_low_mask_helpers() {
        //! ## 测试过程
        //!
        //! 对位掩码助手 `low_mask_u64` / `low_mask_u128` 取若干代表性位宽，
        //! 验证其等于「低 n 位全 1」这一定义，并覆盖与位布局表相关的关键宽度。
        //!
        //! ## 意义
        //!
        //! 重写以独立的掩码自由函数替换了原先内联的 `(1<<bits)-1` 算术；
        //! 该测试确保替换后的助手在所有被使用的位宽上与原内联表达式数值等价。
        assert_eq!(low_mask_u64(0), 0);
        assert_eq!(low_mask_u64(1), 0b1);
        assert_eq!(low_mask_u64(8), 0xFF);
        assert_eq!(low_mask_u64(31), (1u64 << 31) - 1);
        assert_eq!(low_mask_u64(54), (1u64 << 54) - 1);

        assert_eq!(low_mask_u128(0), 0);
        assert_eq!(low_mask_u128(59), (1u128 << 59) - 1);
        assert_eq!(low_mask_u128(126), (1u128 << 126) - 1);
    }

    #[test]
    fn test_positional_sequence_hash_table_driven_mode_boundaries() {
        //! ## 测试过程
        //!
        //! 在表驱动 `LAYOUT` 选择的每个 mode 边界位置（254、65534、16777214、
        //! 2^31 - 1）构造 `PositionalSequenceHash`，断言解码出的 `position` 与
        //! `local_block_hash` 均能无损还原。
        //!
        //! ## 意义
        //!
        //! 验证以 `const LAYOUT` 表驱动方式替换原 match 分支后，模式切换阈值
        //! 与位宽分配保持完全一致，确保位级编解码的可逆性。
        let cases: [(u64, u64); 4] = [
            (0x12, 254),
            (0x3456, 65534),
            (0x789ABC, 16_777_214),
            (0x1234_5678, (1u64 << 31) - 1),
        ];
        for (lbh, pos) in cases {
            let h = PositionalSequenceHash::new(0xDEAD_BEEF_CAFE_F00D, pos, lbh);
            assert_eq!(h.position(), pos, "position 还原失败 @ {pos}");
            assert_eq!(h.local_block_hash(), lbh, "local_block_hash 还原失败 @ {pos}");
        }
    }

    #[test]
    fn test_positional_lineage_hash_pack_unpack_roundtrip() {
        //! ## 测试过程
        //!
        //! 在 `PositionalLineageHash` 的三个 mode 区间各取代表位置，分别构造带父
        //! 哈希与不带父哈希的实例，断言 position / current / parent 片段能按表驱动
        //! 布局正确解包。
        //!
        //! ## 意义
        //!
        //! 重写将血缘哈希的位分配迁移到 `const LAYOUT` 表；该测试确认打包与解包
        //! 在各模式下保持自洽，且 `mode()` 取高 2 位的结果与位置区间相符。
        for (pos, expected_mode) in [(200usize, 0u8), (60000, 1), (3_000_000, 2)] {
            let with_parent = PositionalLineageHash::new(0x1357, Some(0x2468), pos as u64);
            assert_eq!(with_parent.position(), pos as u64);
            assert_eq!(with_parent.mode(), expected_mode);

            let without_parent = PositionalLineageHash::new(0x1357, None, pos as u64);
            assert_eq!(without_parent.position(), pos as u64);
            assert_eq!(without_parent.parent_hash_fragment(), 0);
        }
    }

    #[test]
    fn test_radix_len_via_fold_consistency() {
        //! ## 测试过程
        //!
        //! 向 `PositionalRadixTree` 插入若干条不同 position 的 (hash, value) 记录，
        //! 断言 `len()` 等于插入条数、`is_empty()` 行为正确，并能按 position 检索。
        //!
        //! ## 意义
        //!
        //! 重写将内部存储字段更名为 `levels` 并用 `fold` 累加各层长度求总长；
        //! 该测试保证新的求长方式与逐条插入的真实计数保持一致。
        let tree: PositionalRadixTree<u32> = PositionalRadixTree::new();
        assert!(tree.is_empty());

        let h0 = PositionalSequenceHash::new(0x11, 0, 0x11);
        let h1 = PositionalSequenceHash::new(0x22, 1, 0x22);
        let h2 = PositionalSequenceHash::new(0x33, 2, 0x33);
        tree.prefix(&h0).insert(h0, 10);
        tree.prefix(&h1).insert(h1, 20);
        tree.prefix(&h2).insert(h2, 30);

        assert_eq!(tree.len(), 3);
        assert!(!tree.is_empty());
        assert_eq!(tree.position(1).and_then(|m| m.get(&h1).map(|v| *v)), Some(20));
    }

    #[test]
    fn test_blocks_default_uuid_uniqueness() {
        //! ## 测试过程
        //!
        //! 连续构造多个 `UniqueBlock` 默认值，断言它们彼此不相等（默认走
        //! `PartialBlock(Uuid::new_v4())` 分支），并验证 `FullBlock` 按内部哈希判等。
        //!
        //! ## 意义
        //!
        //! 覆盖 `blocks` 模块重写后保留的 `Default` 语义：默认值应是全新随机
        //! UUID，从而保证不同局部块身份互不冲突。
        use crate::blocks::UniqueBlock;
        let a = UniqueBlock::default();
        let b = UniqueBlock::default();
        assert_ne!(a, b, "两个默认 UniqueBlock 不应相等");
        assert!(matches!(a, UniqueBlock::PartialBlock(_)));

        let f1 = UniqueBlock::FullBlock(42);
        let f2 = UniqueBlock::FullBlock(42);
        assert_eq!(f1, f2, "相同哈希的 FullBlock 应相等");
    }
}
