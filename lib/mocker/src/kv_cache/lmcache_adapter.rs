// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-License-Identifier: Apache-2.0
//
// API-compatible implementation based on the public interfaces and behavioral
// contracts of NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
// Implementation rewritten by PAGODA.

//! # LMCache 外部缓存模拟
//!
//! ## 设计意图
//! 模拟一个 **跨 worker 共享** 的外部 KV 缓存（LMCache）。它是一张共享命中表：
//! 任一 worker 在生成完整块后把块元数据写入 LMCache，其它 worker 在做 prefill
//! 成本估算时即可命中该前缀，从而减少需要重算的 token 数（降低预测时延）。
//! 第一阶段不接真实 LMCache server、不搬运真实 KV，只维护元数据与命中统计。
//!
//! ## 外部契约
//! [`LmCacheMockAdapter`] 通过 `Arc<Mutex<..>>`（见 [`SharedLmCache`]）在多个
//! [`super::LocalVllmKvCache`] 之间共享。对外暴露：
//! - [`LmCacheMockAdapter::lookup_prefix`]：从序列首块起统计连续命中的前缀；
//! - [`LmCacheMockAdapter::store_block`]：写入一个完整块元数据；
//! - [`LmCacheMockAdapter::batch_lookup`]：按块哈希批量查询共享元数据（供 router 评分）；
//! - [`LmCacheMockAdapter::remove_block`]：删除一个块（模拟外部淘汰）；
//! - [`LmCacheMockAdapter::stats`]：读取命中/写入统计。
//! 命中分层 [`LmCacheTier`] 区分 L1 / L2 / Miss，且 L1 与 L2 的命中时延不同。
//!
//! ## 实现要点
//! - L1 / L2 各是一张 `SequenceHash -> 块元数据` 表，写入按配置可同时落两层。
//! - 命中时延：L1 比 L2 快；`lookup_prefix` 返回的整体 `tier` 取所经过块中最慢的一层
//!   （只要前缀里有任一块只在 L2，则整体按 L2 计），`latency_ms` 为各命中块时延之和。
//! - 每层可设容量上限（0 表示不限）；超限时按写入顺序淘汰最旧块（FIFO 近似 LRU）。

use std::sync::{Arc, Mutex};

use pagoda_tokens::blocks::UniqueBlock;
use pagoda_tokens::{BlockHash, SequenceHash};
use rustc_hash::FxHashMap;

use crate::common::sequence::ActiveSequence;

// === SECTION: 共享句柄类型别名 ===

/// 跨 worker 共享的 LMCache 句柄。多个 [`super::LocalVllmKvCache`] 持有同一个
/// `Arc<Mutex<LmCacheMockAdapter>>`，从而看到彼此写入的块。
pub type SharedLmCache = Arc<Mutex<LmCacheMockAdapter>>;

/// 把一个 [`LmCacheMockAdapter`] 包装为可共享句柄。
pub fn shared(adapter: LmCacheMockAdapter) -> SharedLmCache {
    Arc::new(Mutex::new(adapter))
}

// === SECTION: 配置 ===

/// ## 外部契约
/// LMCache 模拟配置。`enable_l1` / `enable_l2` 控制写入与命中所涉层级；
/// `*_hit_latency_ms` 决定命中时延；`save_latency_ms` 模拟保存开销；
/// `*_capacity_blocks` 为每层容量上限（0 表示不限）。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LmCacheMockConfig {
    pub enable_l1: bool,
    pub enable_l2: bool,
    pub l1_capacity_blocks: usize,
    pub l2_capacity_blocks: usize,
    pub l1_hit_latency_ms: f64,
    pub l2_hit_latency_ms: f64,
    pub save_latency_ms: f64,
}

impl Default for LmCacheMockConfig {
    fn default() -> Self {
        Self {
            enable_l1: true,
            enable_l2: true,
            l1_capacity_blocks: 0,
            l2_capacity_blocks: 0,
            l1_hit_latency_ms: 0.5,
            l2_hit_latency_ms: 2.0,
            save_latency_ms: 0.2,
        }
    }
}

// === SECTION: 命中分层 ===

/// LMCache 命中所在层级。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LmCacheTier {
    L1,
    L2,
    Miss,
}

// === SECTION: 块元数据 ===

/// ## 外部契约
/// 写入 LMCache 的完整块元数据。第一阶段不持有真实 KV，仅元数据。
#[derive(Debug, Clone)]
pub struct LmCacheBlockMeta {
    pub sequence_hash: SequenceHash,
    pub local_hash: BlockHash,
    pub parent_hash: Option<SequenceHash>,
    pub token_ids: Option<Vec<u32>>,
    pub stored_at_ms: Option<f64>,
}

// === SECTION: 查询结果 ===

/// 前缀命中结果。`matched_blocks` 为从序列首块起连续命中的块数，
/// `matched_tokens` 为对应可复用 token 数，`tier` 为整体命中层级，
/// `latency_ms` 为各命中块时延之和。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LmCachePrefixHit {
    pub matched_blocks: usize,
    pub matched_tokens: usize,
    pub tier: LmCacheTier,
    pub latency_ms: f64,
}

/// 共享元数据批量查询的单条结果（供 router 评分，不返回真实 KV）。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LmCacheSharedHit {
    pub sequence_hash: SequenceHash,
    pub tier: LmCacheTier,
    pub estimated_latency_ms: f64,
}

// === SECTION: 统计 ===

/// LMCache 命中/写入统计。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LmCacheMockStats {
    pub l1_hits: u64,
    pub l2_hits: u64,
    pub misses: u64,
    pub stores: u64,
    pub removals: u64,
}

// === SECTION: 单层存储 ===

/// 单层缓存：`SequenceHash -> (写入序号, 元数据)`。写入序号用于容量超限时的 FIFO 淘汰。
#[derive(Default)]
struct CacheTier {
    blocks: FxHashMap<SequenceHash, (u64, LmCacheBlockMeta)>,
    clock: u64,
}

impl CacheTier {
    fn contains(&self, seq_hash: &SequenceHash) -> bool {
        self.blocks.contains_key(seq_hash)
    }

    fn get(&self, seq_hash: &SequenceHash) -> Option<&LmCacheBlockMeta> {
        self.blocks.get(seq_hash).map(|(_, meta)| meta)
    }

    /// 写入一个块，超容量时淘汰最旧块。`capacity == 0` 表示不限容量。
    fn insert(&mut self, capacity: usize, meta: LmCacheBlockMeta) {
        self.clock += 1;
        self.blocks
            .insert(meta.sequence_hash, (self.clock, meta));
        if capacity != 0 && self.blocks.len() > capacity {
            if let Some(oldest) = self
                .blocks
                .iter()
                .min_by_key(|(_, (tick, _))| *tick)
                .map(|(k, _)| *k)
            {
                self.blocks.remove(&oldest);
            }
        }
    }

    fn remove(&mut self, seq_hash: &SequenceHash) -> bool {
        self.blocks.remove(seq_hash).is_some()
    }

    fn len(&self) -> usize {
        self.blocks.len()
    }
}

// === SECTION: LmCacheMockAdapter ===

/// ## 外部契约
/// LMCache 外部缓存模拟器。通过 [`SharedLmCache`] 在多个 worker 间共享。
pub struct LmCacheMockAdapter {
    config: LmCacheMockConfig,
    l1: CacheTier,
    l2: CacheTier,
    stats: LmCacheMockStats,
}

impl LmCacheMockAdapter {
    /// 以指定配置构造。
    pub fn new(config: LmCacheMockConfig) -> Self {
        Self {
            config,
            l1: CacheTier::default(),
            l2: CacheTier::default(),
            stats: LmCacheMockStats::default(),
        }
    }

    /// 以默认配置构造。
    pub fn with_default() -> Self {
        Self::new(LmCacheMockConfig::default())
    }

    /// 读取当前配置。
    pub fn config(&self) -> LmCacheMockConfig {
        self.config
    }

    /// 读取命中/写入统计快照。
    pub fn stats(&self) -> LmCacheMockStats {
        self.stats
    }

    /// 当前各层块数 `(l1, l2)`。
    pub fn len(&self) -> (usize, usize) {
        (self.l1.len(), self.l2.len())
    }

    /// 是否为空（两层皆空）。
    pub fn is_empty(&self) -> bool {
        self.l1.len() == 0 && self.l2.len() == 0
    }

    // === SECTION: 写入 ===

    /// 写入一个完整块。按配置可同时落 L1 / L2。
    pub fn store_block(&mut self, meta: LmCacheBlockMeta) {
        let mut stored = false;
        if self.config.enable_l1 {
            self.l1.insert(self.config.l1_capacity_blocks, meta.clone());
            stored = true;
        }
        if self.config.enable_l2 {
            self.l2.insert(self.config.l2_capacity_blocks, meta.clone());
            stored = true;
        }
        if stored {
            self.stats.stores += 1;
        }
    }

    // === SECTION: 删除 ===

    /// 删除一个块（模拟外部淘汰）。任一层删除成功即计一次 removal。
    pub fn remove_block(&mut self, sequence_hash: SequenceHash) {
        let r1 = self.l1.remove(&sequence_hash);
        let r2 = self.l2.remove(&sequence_hash);
        if r1 || r2 {
            self.stats.removals += 1;
        }
    }

    // === SECTION: 单块命中判定 ===

    /// 判断某个块是否在 LMCache 中（任一层命中即可），并返回命中层级。
    /// L1 优先于 L2。不更新统计。
    pub fn tier_of(&self, sequence_hash: &SequenceHash) -> LmCacheTier {
        if self.config.enable_l1 && self.l1.contains(sequence_hash) {
            LmCacheTier::L1
        } else if self.config.enable_l2 && self.l2.contains(sequence_hash) {
            LmCacheTier::L2
        } else {
            LmCacheTier::Miss
        }
    }

    /// 是否存在某个块（任一层）。不更新统计。
    pub fn contains_block(&self, sequence_hash: &SequenceHash) -> bool {
        self.tier_of(sequence_hash) != LmCacheTier::Miss
    }

    /// 某层级的命中时延。
    fn tier_latency(&self, tier: LmCacheTier) -> f64 {
        match tier {
            LmCacheTier::L1 => self.config.l1_hit_latency_ms,
            LmCacheTier::L2 => self.config.l2_hit_latency_ms,
            LmCacheTier::Miss => 0.0,
        }
    }

    // === SECTION: 前缀查询 ===

    /// 从序列首块起统计连续命中的前缀。遇到第一个 miss 或 partial 块即停止。
    ///
    /// 关闭 prefix caching 时，块哈希为随机值，永远不可能命中，直接返回 Miss。
    /// 整体 `tier` 取所经过块中最慢的一层；`latency_ms` 为各命中块时延之和。
    /// 会更新命中/未命中统计。
    pub fn lookup_prefix(&mut self, sequence: &ActiveSequence) -> LmCachePrefixHit {
        if !sequence.enable_prefix_caching() {
            self.stats.misses += 1;
            return LmCachePrefixHit {
                matched_blocks: 0,
                matched_tokens: 0,
                tier: LmCacheTier::Miss,
                latency_ms: 0.0,
            };
        }

        let mut matched_blocks = 0usize;
        let mut latency_ms = 0.0f64;
        let mut overall_tier = LmCacheTier::Miss;
        let mut l1_hits = 0u64;
        let mut l2_hits = 0u64;

        for block in sequence.unique_blocks().iter() {
            match block {
                UniqueBlock::FullBlock(seq_hash) => {
                    let tier = self.tier_of(seq_hash);
                    if tier == LmCacheTier::Miss {
                        break;
                    }
                    matched_blocks += 1;
                    latency_ms += self.tier_latency(tier);
                    match tier {
                        LmCacheTier::L1 => l1_hits += 1,
                        LmCacheTier::L2 => l2_hits += 1,
                        LmCacheTier::Miss => {}
                    }
                    // 整体层级取最慢：一旦经过 L2，则整体不优于 L2。
                    overall_tier = match (overall_tier, tier) {
                        (LmCacheTier::Miss, t) => t,
                        (LmCacheTier::L1, LmCacheTier::L2) => LmCacheTier::L2,
                        (acc, _) => acc,
                    };
                }
                UniqueBlock::PartialBlock(_) => break,
            }
        }

        if matched_blocks == 0 {
            self.stats.misses += 1;
        } else {
            self.stats.l1_hits += l1_hits;
            self.stats.l2_hits += l2_hits;
        }

        let matched_tokens =
            (matched_blocks * sequence.block_size()).min(sequence.num_input_tokens());

        LmCachePrefixHit {
            matched_blocks,
            matched_tokens,
            tier: overall_tier,
            latency_ms,
        }
    }

    // === SECTION: 共享元数据批量查询 ===

    /// 按块哈希批量查询共享元数据。命中的块返回其层级与估计加载时延（供 router 评分）。
    /// 未命中的块不出现在返回值中。不更新统计。
    pub fn batch_lookup(&self, block_hashes: &[SequenceHash]) -> Vec<LmCacheSharedHit> {
        block_hashes
            .iter()
            .filter_map(|seq_hash| {
                let tier = self.tier_of(seq_hash);
                if tier == LmCacheTier::Miss {
                    None
                } else {
                    Some(LmCacheSharedHit {
                        sequence_hash: *seq_hash,
                        tier,
                        estimated_latency_ms: self.tier_latency(tier),
                    })
                }
            })
            .collect()
    }

    /// 读取某个块的元数据（任一层，L1 优先）。
    pub fn block_meta(&self, sequence_hash: &SequenceHash) -> Option<&LmCacheBlockMeta> {
        self.l1
            .get(sequence_hash)
            .or_else(|| self.l2.get(sequence_hash))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 从一个 `ActiveSequence` 中取出全部完整块的 `SequenceHash`（按顺序）。
    fn full_block_hashes(seq: &ActiveSequence) -> Vec<SequenceHash> {
        seq.unique_blocks()
            .iter()
            .filter_map(|b| match b {
                UniqueBlock::FullBlock(h) => Some(*h),
                UniqueBlock::PartialBlock(_) => None,
            })
            .collect()
    }

    fn meta(seq_hash: SequenceHash) -> LmCacheBlockMeta {
        LmCacheBlockMeta {
            sequence_hash: seq_hash,
            local_hash: 0,
            parent_hash: None,
            token_ids: None,
            stored_at_ms: None,
        }
    }

    fn make_seq(num_blocks: u32, block_size: usize) -> ActiveSequence {
        let tokens: Vec<u32> = (0..(num_blocks * block_size as u32)).collect();
        ActiveSequence::new(tokens, 10, Some(block_size), true, false)
    }

    #[test]
    fn test_store_and_contains_both_tiers() {
        // ## 测试过程
        // 默认配置（L1+L2 均启用）下写入一个块，检查命中层级与各层块数。
        // ## 意义
        // 验证 store_block 默认同时落 L1 与 L2，且 L1 优先命中。
        let mut a = LmCacheMockAdapter::with_default();
        a.store_block(meta(42));
        assert!(a.contains_block(&42));
        assert_eq!(a.tier_of(&42), LmCacheTier::L1);
        assert_eq!(a.len(), (1, 1));
        assert_eq!(a.stats().stores, 1);
    }

    #[test]
    fn test_l1_disabled_falls_back_to_l2() {
        // ## 测试过程
        // 关闭 L1、仅启用 L2，写入并查询。
        // ## 意义
        // 验证仅 L2 时命中层级为 L2，且 L1 为空。
        let cfg = LmCacheMockConfig {
            enable_l1: false,
            ..LmCacheMockConfig::default()
        };
        let mut a = LmCacheMockAdapter::new(cfg);
        a.store_block(meta(7));
        assert_eq!(a.tier_of(&7), LmCacheTier::L2);
        assert_eq!(a.len(), (0, 1));
    }

    #[test]
    fn test_lookup_prefix_continuous_hit() {
        // ## 测试过程
        // 写入序列的前 2 个完整块（共 3 个），查询整条序列的前缀命中。
        // ## 意义
        // 验证 lookup_prefix 只统计从首块起的连续命中，遇首个 miss 即停止。
        let block_size = 16;
        let seq = make_seq(3, block_size);
        let hashes = full_block_hashes(&seq);
        assert_eq!(hashes.len(), 3);

        let mut a = LmCacheMockAdapter::with_default();
        a.store_block(meta(hashes[0]));
        a.store_block(meta(hashes[1]));

        let hit = a.lookup_prefix(&seq);
        assert_eq!(hit.matched_blocks, 2);
        assert_eq!(hit.matched_tokens, 2 * block_size);
        assert_eq!(hit.tier, LmCacheTier::L1);
    }

    #[test]
    fn test_lookup_prefix_stops_at_gap() {
        // ## 测试过程
        // 只写入序列的第 2 个块（首块缺失），查询前缀命中。
        // ## 意义
        // 验证首块未命中时连续前缀长度为 0，不会跳过缺口去命中后面的块。
        let block_size = 16;
        let seq = make_seq(3, block_size);
        let hashes = full_block_hashes(&seq);

        let mut a = LmCacheMockAdapter::with_default();
        a.store_block(meta(hashes[1]));

        let hit = a.lookup_prefix(&seq);
        assert_eq!(hit.matched_blocks, 0);
        assert_eq!(hit.tier, LmCacheTier::Miss);
        assert_eq!(a.stats().misses, 1);
    }

    #[test]
    fn test_lookup_prefix_tier_is_slowest() {
        // ## 测试过程
        // 让首块只在 L2、次块在 L1+L2，查询前缀。
        // ## 意义
        // 验证整体命中层级取所经过块中最慢的一层（含 L2 即 L2），时延为各块之和。
        let block_size = 16;
        let seq = make_seq(2, block_size);
        let hashes = full_block_hashes(&seq);

        let mut a = LmCacheMockAdapter::new(LmCacheMockConfig::default());
        // 两块都写入（默认 L1+L2），再从 L1 删除块0，使块0 只在 L2、块1 在 L1+L2。
        a.store_block(meta(hashes[0]));
        a.store_block(meta(hashes[1]));
        a.l1.remove(&hashes[0]);

        let hit = a.lookup_prefix(&seq);
        assert_eq!(hit.matched_blocks, 2);
        assert_eq!(hit.tier, LmCacheTier::L2);
        let expected = a.config().l2_hit_latency_ms + a.config().l1_hit_latency_ms;
        assert!((hit.latency_ms - expected).abs() < 1e-9);
    }

    #[test]
    fn test_lookup_prefix_disabled_caching_is_miss() {
        // ## 测试过程
        // 对关闭 prefix caching 的序列调用 lookup_prefix。
        // ## 意义
        // 验证禁用前缀缓存时直接返回 Miss，不做任何命中。
        let block_size = 16;
        let tokens: Vec<u32> = (0..(block_size as u32 * 3)).collect();
        let seq = ActiveSequence::new(tokens, 10, Some(block_size), false, false);
        let mut a = LmCacheMockAdapter::with_default();
        let hit = a.lookup_prefix(&seq);
        assert_eq!(hit.matched_blocks, 0);
        assert_eq!(hit.tier, LmCacheTier::Miss);
    }

    #[test]
    fn test_batch_lookup_returns_only_hits() {
        // ## 测试过程
        // 写入两个块，批量查询三个哈希（其中一个不存在）。
        // ## 意义
        // 验证 batch_lookup 仅返回命中的块及其层级与估计时延，未命中不出现。
        let mut a = LmCacheMockAdapter::with_default();
        a.store_block(meta(1));
        a.store_block(meta(2));
        let hits = a.batch_lookup(&[1, 2, 3]);
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().all(|h| h.tier == LmCacheTier::L1));
        assert!(hits.iter().any(|h| h.sequence_hash == 1));
        assert!(hits.iter().any(|h| h.sequence_hash == 2));
    }

    #[test]
    fn test_remove_block() {
        // ## 测试过程
        // 写入后删除一个块。
        // ## 意义
        // 验证 remove_block 从两层删除该块并累加 removals 统计。
        let mut a = LmCacheMockAdapter::with_default();
        a.store_block(meta(5));
        a.remove_block(5);
        assert!(!a.contains_block(&5));
        assert_eq!(a.stats().removals, 1);
        assert!(a.is_empty());
    }

    #[test]
    fn test_capacity_eviction_evicts_oldest() {
        // ## 测试过程
        // L1 容量设为 2，连续写入 3 个块。
        // ## 意义
        // 验证超容量时淘汰最旧写入的块，最新两个块保留。
        let cfg = LmCacheMockConfig {
            enable_l2: false,
            l1_capacity_blocks: 2,
            ..LmCacheMockConfig::default()
        };
        let mut a = LmCacheMockAdapter::new(cfg);
        a.store_block(meta(1));
        a.store_block(meta(2));
        a.store_block(meta(3));
        assert_eq!(a.len().0, 2);
        assert!(!a.contains_block(&1));
        assert!(a.contains_block(&2));
        assert!(a.contains_block(&3));
    }
}

