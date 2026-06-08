// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-License-Identifier: Apache-2.0

//! # 活跃序列（ActiveSequence）
//!
//! ## 设计意图
//! 表示「正在构建中的」一个请求序列：可不断追加 token，并在跨越 block 边界时
//! 把部分块（partial block）提升为完整块（full block），产出供 KV 管理层消费的
//! [`MoveBlock`] 信号。
//!
//! ## 外部契约
//! - [`ActiveSequence`] 的字段集合（经 `derive_getters` 暴露的只读访问器）、
//!   `block_size` 的 `min = 2` 校验、构造时的 `1337` 哈希种子、默认 `block_size = 64`
//!   均保持稳定。
//! - 全部公开方法的签名与可观察行为（包括 panic 文案 `"invalid ActiveSequence"`、
//!   `"Cannot generate more tokens: reached max_output_tokens limit"`、`"Token push failed."`、
//!   `"Cannot have a partial block as parent"`）与上游一致。
//!
//! ## 实现要点
//! - 启用前缀缓存时块身份用真实 `sequence_hash` / `positional_lineage_hash`；
//!   关闭时用随机值，保证相同 prompt 也不会错误共享块。
//! - 块缓存（`unique_blocks` / `block_hashes` / `plhs`）随 push / pop 增量维护，
//!   避免重复全量重算。

use derive_getters::Getters;
use pagoda_tokens::blocks::UniqueBlock;
use pagoda_tokens::{PositionalLineageHash, TokenBlockSequence, Tokens};
use rand::random;
use validator::Validate;

use crate::common::protocols::MoveBlock;

/// 构造时使用的固定哈希种子。
const SEQUENCE_HASH_SEED: u64 = 1337;
/// 未显式指定时的默认 block 大小。
const DEFAULT_BLOCK_SIZE: usize = 64;

// === SECTION: 块缓存初始化 ===

/// 由 [`TokenBlockSequence`] 派生 unique block、block hash 与位置-血缘哈希三组缓存。
fn create_sequence_cache(
    tokens: &TokenBlockSequence,
    block_size: usize,
    enable_prefix_caching: bool,
) -> (Vec<UniqueBlock>, Vec<u64>, Vec<PositionalLineageHash>) {
    let block_count = tokens.blocks().len();
    let mut unique_blocks = Vec::with_capacity(block_count + 1);
    let mut block_hashes = Vec::with_capacity(block_count);
    let mut plhs = Vec::with_capacity(block_count);

    for (pos, block) in tokens.blocks().iter().enumerate() {
        block_hashes.push(block.block_hash());
        // 启用前缀缓存：用真实身份；否则用随机身份避免相同 prompt 共享块。
        let (unique, plh) = if enable_prefix_caching {
            (
                UniqueBlock::FullBlock(block.sequence_hash()),
                block.positional_lineage_hash(),
            )
        } else {
            (
                UniqueBlock::FullBlock(random::<u64>()),
                PositionalLineageHash::new(random::<u64>(), None, pos as u64),
            )
        };
        unique_blocks.push(unique);
        plhs.push(plh);
    }

    // 仅当 token 总数不是 block_size 的整数倍时，才追加一个 partial block。
    if !tokens.total_tokens().is_multiple_of(block_size) {
        unique_blocks.push(UniqueBlock::default());
    }
    (unique_blocks, block_hashes, plhs)
}

// === SECTION: ActiveSequence 定义 ===

/// 正在构建中的序列：可追加 token 并将块提交为哈希。
/// TODO: reuse tokens
#[derive(Debug, Getters, Validate)]
pub struct ActiveSequence {
    unique_blocks: Vec<UniqueBlock>,
    block_hashes: Vec<u64>,
    plhs: Vec<PositionalLineageHash>,

    tokens: TokenBlockSequence,

    #[getter(copy)]
    #[validate(range(min = 2))]
    block_size: usize,

    #[getter(copy)]
    max_output_tokens: usize,

    #[getter(copy)]
    generated_tokens: usize,

    #[getter(copy)]
    num_input_tokens: usize,

    #[getter(copy)]
    num_allocated_tokens: usize,

    #[getter(copy)]
    enable_prefix_caching: bool,

    #[getter(copy)]
    emit_token_ids: bool,
}

impl ActiveSequence {
    /// 用给定 token 创建一个新的 ActiveSequence。
    pub fn new(
        tokens: Vec<u32>,
        max_output_tokens: usize,
        block_size: Option<usize>,
        enable_prefix_caching: bool,
        emit_token_ids: bool,
    ) -> Self {
        let block_size = block_size.unwrap_or(DEFAULT_BLOCK_SIZE);
        let num_input_tokens = tokens.len();

        let tokens = Tokens::from(tokens).into_sequence(block_size as u32, Some(SEQUENCE_HASH_SEED));
        let (unique_blocks, block_hashes, plhs) =
            create_sequence_cache(&tokens, block_size, enable_prefix_caching);

        let seq = Self {
            unique_blocks,
            block_hashes,
            plhs,
            tokens,
            block_size,
            max_output_tokens,
            generated_tokens: 0,
            num_input_tokens,
            num_allocated_tokens: 0,
            enable_prefix_caching,
            emit_token_ids,
        };
        seq.validate().expect("invalid ActiveSequence");
        seq
    }

    pub fn extra_tokens(&self) -> u32 {
        (self.len() % self.block_size) as u32
    }

    pub fn len(&self) -> usize {
        self.tokens.total_tokens()
    }

    pub fn is_empty(&self) -> bool {
        self.tokens.total_tokens() == 0
    }

    // === SECTION: 分配信号 ===

    /// 为「累计到 `cumulative_tokens` 个 token」所需的块构建 [`MoveBlock::Use`] 信号，
    /// 但不改动内部状态；若无需新块则返回 `None`。信号成功处理后须调用 `commit_allocation`。
    pub fn prepare_allocation(&self, cumulative_tokens: usize) -> Option<MoveBlock> {
        let prev_blocks = self
            .num_allocated_tokens
            .div_ceil(self.block_size)
            .min(self.unique_blocks.len());
        let target_blocks = cumulative_tokens
            .div_ceil(self.block_size)
            .min(self.unique_blocks.len());
        if target_blocks <= prev_blocks {
            return None;
        }

        let blocks = self.unique_blocks[prev_blocks..target_blocks].to_vec();

        // block_hashes / plhs 只覆盖完整块，故需各自夹取上界。
        let hash_start = prev_blocks.min(self.block_hashes.len());
        let hash_end = target_blocks.min(self.block_hashes.len());
        let hashes = self.block_hashes[hash_start..hash_end].to_vec();
        let plhs = self.plhs[hash_start..hash_end].to_vec();

        let token_ids = if self.emit_token_ids && hash_start < hash_end {
            Some(
                self.tokens.blocks()[hash_start..hash_end]
                    .iter()
                    .map(|b| b.tokens().to_vec())
                    .collect(),
            )
        } else {
            None
        };

        let parent = prev_blocks
            .checked_sub(1)
            .map(|idx| self.unique_blocks[idx].clone());
        Some(MoveBlock::Use(blocks, hashes, plhs, token_ids, parent))
    }

    /// 序列中所有完整块的位置-血缘哈希。对应 `block_hashes()`，但返回 kvbm-logical 使用的 PLH 身份。
    pub fn positional_lineage_hashes(&self) -> &[PositionalLineageHash] {
        &self.plhs
    }

    pub fn block_token_ids(&self) -> Vec<Vec<u32>> {
        self.tokens
            .blocks()
            .iter()
            .map(|block| block.tokens().to_vec())
            .collect()
    }

    /// 推进 `num_allocated_tokens`，确认一次成功的分配。
    pub fn commit_allocation(&mut self, cumulative_tokens: usize) {
        self.num_allocated_tokens = cumulative_tokens;
    }

    /// 一次完成 prepare + commit（用于不会失败的路径）。
    pub fn allocate_blocks_for_chunk(&mut self, cumulative_tokens: usize) -> Option<MoveBlock> {
        let signal = self.prepare_allocation(cumulative_tokens);
        self.commit_allocation(cumulative_tokens);
        signal
    }

    /// 一次性分配剩余全部块（向后兼容）。
    pub fn take_creation_signal(&mut self) -> Option<MoveBlock> {
        self.allocate_blocks_for_chunk(self.len())
    }

    /// 创建新的 ActiveSequence 并返回创建信号。
    pub fn new_with_signal(
        tokens: Vec<u32>,
        max_output_tokens: usize,
        block_size: Option<usize>,
        enable_prefix_caching: bool,
    ) -> (Self, Option<MoveBlock>) {
        let mut sequence = Self::new(
            tokens,
            max_output_tokens,
            block_size,
            enable_prefix_caching,
            false,
        );
        let signal = sequence.take_creation_signal();
        (sequence, signal)
    }

    // === SECTION: 追加与生成 ===

    /// 向序列追加一个 token。
    pub fn push(&mut self, token: u32) -> Option<Vec<MoveBlock>> {
        self.tokens.append(token).expect("Token push failed.");
        self.generated_tokens += 1;

        // 仅当新 token 是某个新部分块的首 token 时才产出信号。
        if self.len() % self.block_size != 1 {
            return None;
        }

        let mut signals = Vec::new();

        // 若上一个块是部分块，则先将其提升为完整块。
        if let Some(UniqueBlock::PartialBlock(uuid)) = self.unique_blocks.last().cloned() {
            let last_complete = self.tokens.last_complete_block().unwrap();
            let last_seq_hash = if self.enable_prefix_caching {
                last_complete.sequence_hash()
            } else {
                random::<u64>()
            };
            let last_block_hash = last_complete.block_hash();
            // 与 `last_seq_hash` 同理：关闭前缀缓存时两个相同 prompt 不得共享块，
            // 故提升所用的 PLH 也必须唯一，否则 `process_promote` 的
            // `match_blocks(&[plh])` 查找会复用其他请求的块。
            let last_plh = if self.enable_prefix_caching {
                last_complete.positional_lineage_hash()
            } else {
                PositionalLineageHash::new(random::<u64>(), None, self.block_hashes.len() as u64)
            };
            let promote_token_ids = if self.emit_token_ids {
                Some(last_complete.tokens().to_vec())
            } else {
                None
            };
            self.block_hashes.push(last_block_hash);
            self.plhs.push(last_plh);
            self.unique_blocks.pop();

            // pop 之后栈顶即父块。
            let second_to_last_hash = self.unique_blocks.last().map(|block| match block {
                UniqueBlock::FullBlock(hash) => *hash,
                UniqueBlock::PartialBlock(_) => panic!("Cannot have a partial block as parent"),
            });

            self.unique_blocks
                .push(UniqueBlock::FullBlock(last_seq_hash));
            signals.push(MoveBlock::Promote(
                uuid,
                last_seq_hash,
                second_to_last_hash,
                last_block_hash,
                last_plh,
                promote_token_ids,
            ));
        }

        // 为新的部分块申请空间。
        let new_partial_block = UniqueBlock::default();
        self.unique_blocks.push(new_partial_block.clone());
        signals.push(MoveBlock::Use(
            vec![new_partial_block],
            vec![],
            vec![],
            None,
            None,
        ));
        Some(signals)
    }

    /// 随机生成一个 token，追加到序列并递增生成计数。
    ///
    /// 该函数会：
    /// - 生成随机 token 并加入当前序列；
    /// - 按需申请新部分块，或将已有部分块提升为完整块；
    /// - 返回供 KvManager 处理的相应信号。
    ///
    /// # Panics
    ///
    /// 在已达到 max_output_tokens 后调用会 panic。调用前务必检查
    /// `generated_tokens < max_output_tokens`。
    pub fn generate(&mut self) -> Vec<MoveBlock> {
        assert!(
            self.generated_tokens < self.max_output_tokens,
            "Cannot generate more tokens: reached max_output_tokens limit"
        );

        let token = random::<u32>();
        let mut signals = Vec::new();

        if let Some(move_blocks) = self.push(token) {
            signals.extend(move_blocks);
        }

        // 未达上限则直接返回 push 产出的信号。
        if self.generated_tokens != self.max_output_tokens {
            return signals;
        }

        // 达到上限时释放全部块。
        signals.extend(self.free_signal_for_tokens(self.len()));
        signals
    }

    fn free_signal_for_tokens(&self, active_tokens: usize) -> Vec<MoveBlock> {
        let active_blocks = active_tokens
            .div_ceil(self.block_size)
            .min(self.unique_blocks.len());
        self.unique_blocks[..active_blocks]
            .iter()
            .rev()
            .map(|block| match block {
                UniqueBlock::PartialBlock(uuid) => {
                    MoveBlock::Deref(vec![UniqueBlock::PartialBlock(*uuid)])
                }
                UniqueBlock::FullBlock(hash) => {
                    MoveBlock::Deref(vec![UniqueBlock::FullBlock(*hash)])
                }
            })
            .collect()
    }

    /// 释放当前活跃分配占用的块。
    pub fn free_signal(&self) -> Vec<MoveBlock> {
        self.free_signal_for_tokens(self.num_allocated_tokens)
    }

    /// 将请求置为被抢占状态，并返回释放当前块的信号。
    /// 抢占后序列保留 decode 阶段已生成的 token（若有）；
    /// 重置 `num_allocated_tokens`，以便重新准入时从头分配。
    pub fn reset_with_signal(&mut self) -> Vec<MoveBlock> {
        let free_signal = self.free_signal();
        self.num_allocated_tokens = 0;
        free_signal
    }

    /// 弹出序列中最后一个 token。
    ///
    /// 仅用于在分配/抢占失败路径上撤销一个刚生成的 decode token。基于该不变式，
    /// 被移除的 token 必在当前部分块中，因此只需在序列长度回到块边界时丢弃尾部的
    /// 部分 `UniqueBlock`。用它回退任意 prompt 历史是不正确的。
    pub fn pop(&mut self) {
        self.tokens.pop();
        self.generated_tokens = self.generated_tokens.saturating_sub(1);

        // 回退到上一个完整块。
        if self.tokens.total_tokens().is_multiple_of(self.block_size) {
            self.unique_blocks.pop();
        }
    }
}

#[cfg(test)]
mod tests {
    //! ## 测试过程
    //! 从公开 API 与可观察的 [`MoveBlock`] 信号出发，覆盖：初始部分块创建、跨块边界的
    //! 提升与新部分块申请、等价历史的块身份一致性、父块选取、抢占重置、生成到上限时的
    //! 释放、分块准备的稳定性，以及 promote/pop 后块哈希缓存的同步。
    //!
    //! ## 意义
    //! 这些断言锁定 ActiveSequence 对外暴露的信号序列与块身份语义，确保 KV 管理层在
    //! 前缀缓存、抢占与重放路径下得到正确输入。
    use super::*;

    /// 从底层 token 序列直接抽取各块的 block_hash，作为缓存校验的预期值。
    fn collect_block_hashes(seq: &ActiveSequence) -> Vec<u64> {
        seq.tokens
            .blocks()
            .iter()
            .map(|block| block.block_hash())
            .collect()
    }

    /// 校验缓存的 block_hashes 与已提升完整块一一对应。
    fn check_hash_cache_in_sync(seq: &ActiveSequence) {
        let full_count = seq
            .unique_blocks()
            .iter()
            .filter(|block| matches!(block, UniqueBlock::FullBlock(_)))
            .count();
        assert_eq!(
            seq.block_hashes().as_slice(),
            &collect_block_hashes(seq)[..full_count],
            "cached block hashes should match the promoted full blocks"
        );
    }

    /// 断言一个 Use 信号的块与哈希切片与预期相等。
    fn expect_use(signal: &MoveBlock, blocks: &[UniqueBlock], hashes: &[u64]) {
        match signal {
            MoveBlock::Use(b, h, ..) => {
                assert_eq!(b, blocks);
                assert_eq!(h, hashes);
            }
            other => panic!("expected MoveBlock::Use, got {other:?}"),
        }
    }

    /// 断言一个 Use 信号只申请了单个部分块、无哈希。
    fn expect_partial_use(signal: &MoveBlock) {
        match signal {
            MoveBlock::Use(blocks, hashes, ..) => {
                assert_eq!(blocks.len(), 1);
                assert!(matches!(blocks[0], UniqueBlock::PartialBlock(_)));
                assert!(hashes.is_empty());
            }
            other => panic!("expected partial-block Use, got {other:?}"),
        }
    }

    /// 断言一个 Promote 信号的父块哈希。
    fn expect_promote_parent(signal: &MoveBlock, parent: Option<u64>) {
        match signal {
            MoveBlock::Promote(_, _, parent_hash, ..) => assert_eq!(*parent_hash, parent),
            other => panic!("expected MoveBlock::Promote, got {other:?}"),
        }
    }

    /// 断言一个 Deref 信号释放的是单个部分块。
    fn expect_deref_partial(signal: &MoveBlock) {
        match signal {
            MoveBlock::Deref(blocks) => {
                assert_eq!(blocks.len(), 1);
                assert!(matches!(blocks[0], UniqueBlock::PartialBlock(_)));
            }
            other => panic!("expected partial-block Deref, got {other:?}"),
        }
    }

    /// 断言一个 Deref 信号释放的是单个完整块。
    fn expect_deref_full(signal: &MoveBlock) {
        match signal {
            MoveBlock::Deref(blocks) => {
                assert_eq!(blocks.len(), 1);
                assert!(matches!(blocks[0], UniqueBlock::FullBlock(_)));
            }
            other => panic!("expected full-block Deref, got {other:?}"),
        }
    }

    #[test]
    fn initial_signal_opens_a_single_partial_block() {
        let prompt: Vec<u32> = (0..15).collect();
        let (seq, signal) = ActiveSequence::new_with_signal(prompt, 100, Some(16), true);

        assert_eq!(seq.num_input_tokens(), 15);
        assert_eq!(seq.len(), 15);
        expect_partial_use(signal.as_ref().expect("expected initial Use signal"));
    }

    #[test]
    fn crossing_block_boundary_promotes_then_opens_partial() {
        let prompt: Vec<u32> = (0..15).collect();
        let (mut seq, _) = ActiveSequence::new_with_signal(prompt, 100, Some(16), true);

        // 第 16 个 token 填满块，但本身不产出信号。
        assert!(
            seq.push(15).is_none(),
            "completing a block should not emit signals"
        );

        // 第 17 个 token 开启新部分块，触发 Promote + Use。
        let signals = seq.push(16).expect("expected boundary-crossing signals");
        assert_eq!(signals.len(), 2);
        expect_promote_parent(&signals[0], None);
        expect_partial_use(&signals[1]);

        assert_eq!(seq.unique_blocks().len(), 2);
        assert_eq!(seq.len() % seq.block_size(), 1);
    }

    #[test]
    fn equivalent_histories_share_full_block_identity() {
        let prompt: Vec<u32> = (0..15).collect();
        let (mut a, _) = ActiveSequence::new_with_signal(prompt, 100, Some(16), true);
        a.push(15);
        a.push(16);

        let longer: Vec<u32> = (0..16).collect();
        let (mut b, _) = ActiveSequence::new_with_signal(longer, 100, Some(16), true);
        b.push(16);
        b.pop();
        b.push(16);

        assert_eq!(a.unique_blocks()[0], b.unique_blocks()[0]);
        assert_ne!(a.unique_blocks()[1], b.unique_blocks()[1]);
    }

    #[test]
    fn promote_picks_previous_full_block_as_parent() {
        let prompt: Vec<u32> = (0..15).collect();
        let (mut seq, _) = ActiveSequence::new_with_signal(prompt, 100, Some(16), true);
        seq.push(15);
        seq.push(16);

        seq.push(17);
        seq.pop();
        seq.pop();
        seq.push(16);

        let longer: Vec<u32> = (0..16).collect();
        let (mut twin, _) = ActiveSequence::new_with_signal(longer, 100, Some(16), true);
        twin.push(16);
        twin.pop();
        twin.push(16);
        for token in 17..33 {
            seq.push(token);
            twin.push(token);
        }

        assert_eq!(
            &seq.unique_blocks()[0..2],
            &twin.unique_blocks()[0..2],
            "first two full blocks should remain identical"
        );

        for token in 33..48 {
            seq.push(token);
        }

        let signal = seq
            .push(48)
            .expect("expected promote when opening next partial");

        let UniqueBlock::FullBlock(expected_hash) = seq.unique_blocks()[1] else {
            panic!("unique_blocks[1] should be a full block");
        };
        expect_promote_parent(&signal[0], Some(expected_hash));
        expect_partial_use(&signal[1]);
    }

    #[test]
    fn reset_frees_blocks_and_clears_allocation() {
        let prompt: Vec<u32> = (0..15).collect();
        let (mut seq, _) = ActiveSequence::new_with_signal(prompt, 100, Some(16), true);
        seq.push(15);
        seq.push(16);
        seq.commit_allocation(seq.len());

        let frees = seq.reset_with_signal();

        assert!(!frees.is_empty());
        assert_eq!(seq.num_allocated_tokens(), 0);
        assert_eq!(seq.generated_tokens(), 2);
    }

    #[test]
    fn generate_emits_promote_use_then_deref_at_limit() {
        // block_size = 16，max_output_tokens = 5，初始 14 个 token。
        let prompt: Vec<u32> = (0..14).collect();
        let (mut seq, signal) = ActiveSequence::new_with_signal(prompt, 5, Some(16), true);

        expect_partial_use(signal.as_ref().expect("expected initial Use signal"));

        // 前两个生成 token 不触发信号。
        seq.generate();
        assert_eq!(seq.generate().len(), 0);

        // 第三个生成 token 填满块，触发 Promote + Use。
        let filled = seq.generate();
        assert_eq!(filled.len(), 2);
        expect_promote_parent(&filled[0], None);
        expect_partial_use(&filled[1]);

        // 第四个 token 落入部分块，不触发信号。
        assert_eq!(seq.generate().len(), 0);

        // 最后一个 token 到达上限，触发两个 Deref（部分块 + 完整块）。
        let last = seq.generate();
        assert_eq!(last.len(), 2);
        expect_deref_partial(&last[0]);
        expect_deref_full(&last[1]);
    }

    #[test]
    fn prepare_allocation_slices_full_and_partial_blocks() {
        let prompt: Vec<u32> = (0..10).collect();
        let seq = ActiveSequence::new(prompt, 4, Some(4), true, false);

        let first = seq.prepare_allocation(4).unwrap();
        expect_use(&first, &seq.unique_blocks()[0..1], &seq.block_hashes()[0..1]);

        let second = seq.prepare_allocation(8).unwrap();
        expect_use(&second, &seq.unique_blocks()[0..2], &seq.block_hashes()[0..2]);

        let third = seq.prepare_allocation(10).unwrap();
        expect_use(&third, &seq.unique_blocks()[0..3], &seq.block_hashes()[0..2]);
    }

    #[test]
    fn prepare_allocation_is_stable_until_commit() {
        let prompt: Vec<u32> = (0..10).collect();
        let mut seq = ActiveSequence::new(prompt, 4, Some(4), true, false);

        let first = seq.prepare_allocation(4).unwrap();
        let again = seq.prepare_allocation(4).unwrap();
        assert_eq!(first, again);

        seq.commit_allocation(4);
        let next = seq.prepare_allocation(8).unwrap();
        expect_use(&next, &seq.unique_blocks()[1..2], &seq.block_hashes()[1..2]);
    }

    #[test]
    fn hash_cache_tracks_promote_and_pop() {
        let prompt: Vec<u32> = (0..15).collect();
        let (mut seq, _) = ActiveSequence::new_with_signal(prompt, 4, Some(16), true);

        check_hash_cache_in_sync(&seq);

        seq.push(15);
        check_hash_cache_in_sync(&seq);

        let promote = seq.push(16).unwrap();
        assert_eq!(promote.len(), 2);
        check_hash_cache_in_sync(&seq);

        // pop 仅对撤销当前部分块的新生成 token 合法，即抢占/重放路径。
        seq.pop();
        check_hash_cache_in_sync(&seq);
    }
}
