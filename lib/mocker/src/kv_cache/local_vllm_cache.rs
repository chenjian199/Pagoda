// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-License-Identifier: Apache-2.0

//! # Local vLLM KV cache（本地 GPU paged KV 缓存模拟）
//!
//! ## 设计意图
//! 模拟单个 vLLM worker 的本地 paged KV cache：block 的分配 / 复用 / 释放 /
//! 晋升 / prefix 命中 / 淘汰，以及对应的 KV event 发布。该层不接入任何真实
//! GPU 内存，也不依赖外部 block manager；用纯内存数据结构复刻上述语义。
//!
//! ## 外部契约
//! [`LocalVllmKvCache`] 对 vLLM scheduler 暴露的方法面与行为，必须与上游
//! `KvManager` 完全一致：`process(&MoveBlock) -> usize`、
//! `get_prefill_cost(&ActiveSequence) -> PrefillCost`，以及一组只读计数
//! （`num_active_blocks` / `num_active_block_refs` / `num_inactive_blocks` /
//! `get_active_perc` / `max_capacity` / `block_size` / `dp_rank`）。`MoveBlock`
//! 的三种指令语义如下：
//!
//! - **Use**：请求需要使用若干 block。完整块先查 active，再查 inactive
//!   （prefix 复用），都没有则新分配；局部块按 UUID 复用或新分配。容量耗尽时
//!   返回已成功处理的部分数量，由 scheduler 触发抢占。
//! - **Deref**：释放一份引用。局部块直接归还；完整块引用计数减一，归零后转入
//!   inactive prefix cache 供后续复用。
//! - **Promote**：局部块填满，晋升为完整块；若已存在相同完整块则复用，否则
//!   注册新块并发布 `Stored` 事件。
//!
//! ## 实现要点
//! - `active_full` 以 `SequenceHash` 为键，值为「引用计数 + 该块的 PLH」。引用计数
//!   复刻上游「每次 `Use` clone 一个句柄、每次 `Deref` pop 一个句柄」的行为；保存
//!   PLH 是为了在 `Deref` 归零时把块放回 inactive 池（`Deref` 指令本身只携带
//!   `SequenceHash`）。
//! - `inactive` 是一个容量受限的 LRU 池，键为 PLH。它复刻上游 inactive pool 的
//!   两项行为：`match_blocks(plh)` 命中复用、容量不足时按最近最少使用淘汰。
//! - `registered_blocks` 记录每个注册过且尚未被淘汰的块（无论 active 还是
//!   inactive），用于 `get_prefill_cost` 判定 inactive 命中，以及构造 `Removed`
//!   事件所需的元数据。
//! - 物理容量记账：`max_capacity` 个槽位被「distinct active 块 + inactive 块」占用，
//!   `num_active_blocks` 返回 distinct active 块数；与上游
//!   `total_blocks - available_blocks` 等价。

use pagoda_kv_router::protocols::StorageTier;
use pagoda_tokens::blocks::UniqueBlock;
use pagoda_tokens::{BlockHash, PositionalLineageHash, SequenceHash};
use rustc_hash::FxHashMap;
use uuid::Uuid;

use super::events;
use super::lmcache_adapter::{LmCacheBlockMeta, SharedLmCache};
use crate::common::kv_cache_trace;
use crate::common::protocols::{KvEventPublishers, MockerEvictionBackend, MoveBlock, PrefillCost};
use crate::common::sequence::ActiveSequence;

// === SECTION: 注册块元数据 ===

/// ## 设计意图
/// 保存构造 router 事件（Stored / Removed）所需的块元数据。一个块只要还在
/// `registered_blocks` 中，就视为「本地可见」：active 命中或 inactive 命中都依赖它。
#[derive(Clone)]
struct RegisteredBlockInfo {
    #[allow(dead_code)]
    seq_hash: SequenceHash,
    #[allow(dead_code)]
    parent_hash: Option<SequenceHash>,
    #[allow(dead_code)]
    local_hash: BlockHash,
    #[allow(dead_code)]
    token_ids: Option<Vec<u32>>,
}

// === SECTION: active full 引用计数条目 ===

/// ## 设计意图
/// 复刻上游「`active_full: HashMap<SequenceHash, Vec<ImmutableBlock>>`」中向量长度
/// 充当引用计数的行为：这里只保留计数本身，并额外记下块的 PLH，使 `Deref` 归零时
/// 能把块放回 inactive 池。
struct ActiveFullEntry {
    refcount: usize,
    plh: PositionalLineageHash,
}

// === SECTION: 本地 inactive LRU 池 ===

/// ## 设计意图
/// 模拟本地 GPU prefix cache 中「可复用但当前无请求持有」的完整块集合。
///
/// ## 外部契约
/// 第一阶段仅实现最简单的 LRU 策略（见 [`MockerEvictionBackend`]）。命中即复用，
/// 容量不足时淘汰最久未使用的块。
///
/// ## 实现要点
/// 以「单调递增的访问序号」近似 LRU：`touch` / `insert` 时给块打上当前序号，
/// 淘汰时选序号最小者。块身份用 PLH 表示。
#[derive(Default)]
struct LocalEvictionCache {
    /// PLH → (访问序号, 该块的 SequenceHash)。
    blocks: FxHashMap<PositionalLineageHash, (u64, SequenceHash)>,
    /// 单调递增的访问时钟，越大越新。
    clock: u64,
}

impl LocalEvictionCache {
    fn len(&self) -> usize {
        self.blocks.len()
    }

    /// 命中复用：若该 PLH 在 inactive 池中，则移出并返回其 `SequenceHash`。
    fn match_block(&mut self, plh: &PositionalLineageHash) -> Option<SequenceHash> {
        self.blocks.remove(plh).map(|(_, seq_hash)| seq_hash)
    }

    /// 把一个块放入 inactive 池（refcount 归零的完整块）。
    fn insert(&mut self, plh: PositionalLineageHash, seq_hash: SequenceHash) {
        self.clock += 1;
        self.blocks.insert(plh, (self.clock, seq_hash));
    }

    /// 淘汰最久未使用（访问序号最小）的块，返回其 `(PLH, SequenceHash)`。
    fn evict_lru(&mut self) -> Option<(PositionalLineageHash, SequenceHash)> {
        let victim = self
            .blocks
            .iter()
            .min_by_key(|(_, (tick, _))| *tick)
            .map(|(plh, (_, seq_hash))| (*plh, *seq_hash));
        if let Some((plh, _)) = victim {
            self.blocks.remove(&plh);
        }
        victim
    }
}

// === SECTION: Use 单块处理结果 ===

/// 对 `Use` 中每个块的归类。
///
/// - `ActiveHit`：块已在 `active_full` / `active_partial`，只需提升本地引用计数。
/// - `InactiveHit`：块在 inactive 池中，被重新激活。
/// - `NewStore`：块是全新分配并注册的。
///
/// router 的 radix tree 已经知道 `ActiveHit` 与 `InactiveHit`（只有显式 `Removed`
/// 才会让它遗忘），因此只有 `NewStore` 才发布 `Stored` 事件；两种命中仍会推进
/// parent 游标，使后续 `NewStore` 批次锚定到最后一个被复用的完整块。
enum UseOutcome {
    ActiveHit,
    InactiveHit,
    NewStore,
}

// === SECTION: LocalVllmKvCache ===

/// ## 外部契约
/// 同步式本地 vLLM KV block 管理器，方法面与上游 `KvManager` 一致。
pub struct LocalVllmKvCache {
    max_capacity: usize,
    block_size: usize,
    kv_event_publishers: KvEventPublishers,
    dp_rank: u32,
    next_event_id: u64,

    /// 正在填充 token、尚未形成完整块的局部块；只关心存在性。
    active_partial: FxHashMap<Uuid, ()>,

    /// 当前被请求持有的完整块，键为 `SequenceHash`，值含引用计数与 PLH。
    active_full: FxHashMap<SequenceHash, ActiveFullEntry>,

    /// 注册过且尚未被淘汰的块的影子表，键为 PLH。
    registered_blocks: FxHashMap<PositionalLineageHash, RegisteredBlockInfo>,

    /// 本地 inactive prefix cache（LRU）。
    inactive: LocalEvictionCache,

    /// 可选的跨 worker 共享外部缓存（LMCache）。命中可减少 prefill 重算量；
    /// 新块形成时写入，供其它 worker 命中。
    lmcache: Option<SharedLmCache>,
}

impl LocalVllmKvCache {
    // === SECTION: 构造器 ===

    /// 以事件 sink 构造，默认淘汰策略。
    pub fn new_with_event_sink(
        max_capacity: usize,
        block_size: usize,
        kv_event_publishers: KvEventPublishers,
        dp_rank: u32,
    ) -> Self {
        Self::new_with_eviction_backend(
            max_capacity,
            block_size,
            kv_event_publishers,
            dp_rank,
            MockerEvictionBackend::default(),
        )
    }

    /// 以指定淘汰策略构造。
    ///
    /// ## 实现要点
    /// 第一阶段本地 inactive 池只实现 LRU，[`MockerEvictionBackend`] 仅作为对外
    /// 兼容的参数保留；当前所有取值都走同一套 LRU 路径。`eviction_backend` 仍参与
    /// 初始化日志，以保持可观察行为一致。
    pub fn new_with_eviction_backend(
        max_capacity: usize,
        block_size: usize,
        kv_event_publishers: KvEventPublishers,
        dp_rank: u32,
        eviction_backend: MockerEvictionBackend,
    ) -> Self {
        debug_assert!(max_capacity > 0, "max_capacity must be > 0");

        if !kv_event_publishers.is_empty() {
            tracing::info!(
                "KvManager initialized with event sink for DP rank {dp_rank} with block_size {block_size}, eviction={eviction_backend:?}"
            );
        }

        Self {
            max_capacity,
            block_size,
            kv_event_publishers,
            dp_rank,
            next_event_id: 0,
            active_partial: FxHashMap::default(),
            active_full: FxHashMap::default(),
            registered_blocks: FxHashMap::default(),
            inactive: LocalEvictionCache::default(),
            lmcache: None,
        }
    }

    /// 接入一个跨 worker 共享的 LMCache（链式调用）。
    ///
    /// ## 外部契约
    /// 接入后，[`Self::get_prefill_cost`] 在本地 prefix 命中之后会继续向 LMCache
    /// 延伸连续前缀命中（计入 `cached_tokens`，从而降低预测 prefill 时延）；新形成的
    /// 完整块会写入 LMCache，供其它共享同一句柄的 worker 命中。
    pub fn with_lmcache(mut self, lmcache: SharedLmCache) -> Self {
        self.lmcache = Some(lmcache);
        self
    }

    /// 接入或解除 LMCache。
    pub fn set_lmcache(&mut self, lmcache: Option<SharedLmCache>) {
        self.lmcache = lmcache;
    }

    // === SECTION: MoveBlock 入口 ===

    /// 同步处理一条 `MoveBlock` 指令。
    ///
    /// 对 `MoveBlock::Use` 返回成功分配的 block 数；部分失败时，`0..N` 已提交而
    /// 第 `N+1` 个因容量耗尽未能分配，scheduler 据此触发抢占。
    /// 对 `Deref` / `Promote` 成功返回 1，遇非法状态时 panic。
    pub fn process(&mut self, event: &MoveBlock) -> usize {
        match event {
            MoveBlock::Use(blocks, local_hashes, plhs, token_ids, parent) => self.process_use(
                blocks,
                local_hashes,
                plhs,
                token_ids.as_deref(),
                parent.as_ref(),
            ),
            MoveBlock::Deref(hashes) => {
                self.process_deref(hashes);
                1
            }
            MoveBlock::Promote(uuid, seq_hash, parent_hash, local_hash, plh, token_ids) => {
                self.process_promote(
                    *uuid,
                    *seq_hash,
                    *parent_hash,
                    *local_hash,
                    *plh,
                    token_ids.clone(),
                );
                1
            }
        }
    }

    // === SECTION: 容量与槽位分配 ===

    /// 当前被占用的物理槽位数 = distinct active 块 + inactive 块。
    fn used_slots(&self) -> usize {
        self.active_full.len() + self.active_partial.len() + self.inactive.len()
    }

    /// 尝试腾出一个物理槽位。若已满，则从 inactive 池淘汰一个 LRU 块（发布
    /// `Removed` 事件）。返回是否成功获得空槽。
    fn reserve_one_slot(&mut self) -> bool {
        if self.used_slots() < self.max_capacity {
            return true;
        }
        // 容量已满：尝试淘汰一个 inactive 块以腾出空间。
        if let Some((plh, seq_hash)) = self.inactive.evict_lru() {
            self.registered_blocks.remove(&plh);
            self.publish_kv_event(vec![seq_hash], &[], None, false, None);
            return true;
        }
        // 无 inactive 可淘汰：分配失败。
        false
    }

    // === SECTION: Use ===

    fn process_use(
        &mut self,
        blocks: &[UniqueBlock],
        local_hashes: &[BlockHash],
        plhs: &[PositionalLineageHash],
        token_ids: Option<&[Vec<u32>]>,
        parent: Option<&UniqueBlock>,
    ) -> usize {
        // 上游不变式：调用方必须为 `blocks` 中每个 FullBlock 提供恰好一个 PLH。
        let expected_full_blocks = blocks
            .iter()
            .filter(|b| matches!(b, UniqueBlock::FullBlock(_)))
            .count();
        assert_eq!(
            plhs.len(),
            expected_full_blocks,
            "Use: plhs.len() must match FullBlock count in blocks"
        );
        assert!(
            local_hashes.is_empty() || local_hashes.len() == expected_full_blocks,
            "Use: local_hashes must be empty or match FullBlock count ({} vs {})",
            local_hashes.len(),
            expected_full_blocks,
        );
        assert!(
            token_ids.is_none_or(|ids| ids.len() == expected_full_blocks),
            "Use: token_ids must be absent or match FullBlock count ({} vs {})",
            token_ids.map_or(0, |ids| ids.len()),
            expected_full_blocks,
        );

        let mut blocks_stored = Vec::<SequenceHash>::new();
        let mut stored_local_hashes = Vec::<BlockHash>::new();
        let mut stored_token_ids: Option<Vec<Vec<u32>>> = token_ids.map(|_| Vec::new());

        let mut parent_block: Option<&UniqueBlock> = parent;
        let mut metadata_parent_hash: Option<SequenceHash> = match parent {
            None => None,
            Some(UniqueBlock::FullBlock(block)) => Some(*block),
            Some(UniqueBlock::PartialBlock(_)) => panic!("parent block cannot be partial"),
        };
        let mut plh_idx = 0usize;
        let mut allocated = 0usize;

        for block in blocks.iter() {
            let mut current_full_idx: Option<usize> = None;
            let outcome = match block {
                UniqueBlock::FullBlock(seq_hash) => {
                    let full_idx = plh_idx;
                    current_full_idx = Some(full_idx);
                    // active 命中：提升引用计数。
                    if let Some(entry) = self.active_full.get_mut(seq_hash) {
                        entry.refcount += 1;
                        plh_idx += 1;
                        UseOutcome::ActiveHit
                    } else {
                        // 非 active：先查 inactive（PLH 复用），否则新分配。
                        let plh = plhs[plh_idx];
                        plh_idx += 1;
                        if self.inactive.match_block(&plh).is_some() {
                            self.active_full.insert(
                                *seq_hash,
                                ActiveFullEntry {
                                    refcount: 1,
                                    plh,
                                },
                            );
                            UseOutcome::InactiveHit
                        } else if self.reserve_one_slot() {
                            self.active_full.insert(
                                *seq_hash,
                                ActiveFullEntry {
                                    refcount: 1,
                                    plh,
                                },
                            );
                            let block_local_hash =
                                local_hashes.get(full_idx).copied().unwrap_or_default();
                            let block_token_ids =
                                token_ids.and_then(|ids| ids.get(full_idx).cloned());
                            self.registered_blocks.insert(
                                plh,
                                RegisteredBlockInfo {
                                    seq_hash: *seq_hash,
                                    parent_hash: metadata_parent_hash,
                                    local_hash: block_local_hash,
                                    token_ids: block_token_ids.clone(),
                                },
                            );
                            // 新块写入共享 LMCache，供其它 worker 命中。
                            self.save_to_lmcache(
                                *seq_hash,
                                block_local_hash,
                                metadata_parent_hash,
                                block_token_ids,
                            );
                            UseOutcome::NewStore
                        } else {
                            break; // 容量耗尽；scheduler 将抢占
                        }
                    }
                }
                UniqueBlock::PartialBlock(uuid) => {
                    if self.active_partial.contains_key(uuid) {
                        UseOutcome::ActiveHit
                    } else if self.reserve_one_slot() {
                        self.active_partial.insert(*uuid, ());
                        UseOutcome::ActiveHit
                    } else {
                        break;
                    }
                }
            };

            match outcome {
                UseOutcome::ActiveHit | UseOutcome::InactiveHit => {
                    // router 已知该块；不发 `Stored` 事件。把 parent 游标推进过被复用
                    // 的前缀，使后续 `NewStore` 批次锚定到最后一个被复用的完整块。
                    if matches!(block, UniqueBlock::FullBlock(_)) {
                        parent_block = Some(block);
                    }
                }
                UseOutcome::NewStore => {
                    // 全新注册：通告 router。
                    // 注意：此处不推进 `parent_block` —— 在同一个 `Stored` 事件内，
                    // 相邻块通过它们在 `blocks[]` 中的位置链式相连，因此 `parent_hash`
                    // 必须保持为第一个新建块之前的那个块。
                    if let UniqueBlock::FullBlock(seq_hash) = block {
                        blocks_stored.push(*seq_hash);
                        let full_idx =
                            current_full_idx.expect("NewStore is only emitted for full blocks");
                        if let Some(lh) = local_hashes.get(full_idx) {
                            stored_local_hashes.push(*lh);
                        }
                        if let (Some(ref mut stids), Some(ids)) =
                            (stored_token_ids.as_mut(), token_ids)
                        {
                            stids.push(ids[full_idx].clone());
                        }
                    }
                }
            }
            if let UniqueBlock::FullBlock(seq_hash) = block {
                metadata_parent_hash = Some(*seq_hash);
            }
            allocated += 1;
        }

        let parent_hash = match parent_block {
            None => None,
            Some(UniqueBlock::FullBlock(block)) => Some(*block),
            Some(UniqueBlock::PartialBlock(_)) => panic!("parent block cannot be partial"),
        };
        self.publish_kv_event(
            blocks_stored,
            &stored_local_hashes,
            parent_hash,
            true,
            stored_token_ids,
        );

        allocated
    }

    // === SECTION: Deref ===

    fn process_deref(&mut self, blocks: &[UniqueBlock]) {
        for block in blocks {
            match block {
                UniqueBlock::PartialBlock(uuid) => {
                    self.active_partial
                        .remove(uuid)
                        .expect("Deref: partial block not in active pool");
                }
                UniqueBlock::FullBlock(seq_hash) => {
                    let entry = self
                        .active_full
                        .get_mut(seq_hash)
                        .expect("Deref: full block not in active pool");
                    entry.refcount -= 1;
                    if entry.refcount == 0 {
                        let plh = entry.plh;
                        self.active_full.remove(seq_hash);
                        // 引用归零：块转入 inactive prefix cache 供后续复用。
                        self.inactive.insert(plh, *seq_hash);
                    }
                }
            }
        }
    }

    // === SECTION: Promote ===

    fn process_promote(
        &mut self,
        uuid: Uuid,
        seq_hash: SequenceHash,
        parent_hash: Option<u64>,
        local_hash: BlockHash,
        plh: PositionalLineageHash,
        token_ids: Option<Vec<u32>>,
    ) {
        self.active_partial
            .remove(&uuid)
            .expect("Promote: partial block not found");

        // 检测碰撞：seq_hash 已有注册句柄（active 或 inactive）。
        let is_new = if let Some(entry) = self.active_full.get_mut(&seq_hash) {
            // active 池碰撞 —— 提升引用计数。
            entry.refcount += 1;
            false
        } else if self.inactive.match_block(&plh).is_some() {
            // inactive 池碰撞 —— 重新激活。
            self.active_full
                .insert(seq_hash, ActiveFullEntry { refcount: 1, plh });
            false
        } else {
            // 全新注册。
            self.active_full
                .insert(seq_hash, ActiveFullEntry { refcount: 1, plh });
            self.registered_blocks.insert(
                plh,
                RegisteredBlockInfo {
                    seq_hash,
                    parent_hash,
                    local_hash,
                    token_ids: token_ids.clone(),
                },
            );
            // 晋升出的新完整块写入共享 LMCache。
            self.save_to_lmcache(seq_hash, local_hash, parent_hash, token_ids.clone());
            true
        };

        if is_new {
            self.publish_kv_event(
                vec![seq_hash],
                &[local_hash],
                parent_hash,
                true,
                token_ids.map(|t| vec![t]),
            );
        }
    }

    // === SECTION: 只读计数 ===

    /// 当前被 mocker 持有（不可淘汰）的 **distinct** 物理 KV 块数。
    pub fn num_active_blocks(&self) -> usize {
        self.active_full.len() + self.active_partial.len()
    }

    /// 持有的 RAII 句柄总数（引用计数式）：局部块每块计一，完整块按引用计数累加。
    /// 共享前缀复用会使该值高于 distinct 块数。
    pub fn num_active_block_refs(&self) -> usize {
        self.active_partial.len()
            + self
                .active_full
                .values()
                .map(|e| e.refcount)
                .sum::<usize>()
    }

    pub fn get_active_perc(&self) -> f64 {
        self.num_active_blocks() as f64 / self.max_capacity as f64
    }

    pub fn num_inactive_blocks(&self) -> usize {
        self.inactive.len()
    }

    pub fn max_capacity(&self) -> usize {
        self.max_capacity
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    pub fn dp_rank(&self) -> u32 {
        self.dp_rank
    }

    // === SECTION: prefill cost ===

    /// 按顺序扫描 `unique_blocks`，统计被缓存（active 或 inactive）的最长前缀。
    /// 遇到第一个 miss 即停止 —— KV 状态是顺序计算的，miss 之后的一切都必须重算。
    ///
    /// ## 实现要点
    /// 若接入了 LMCache：在本地命中链断裂处，继续向 LMCache 延伸**连续**前缀命中
    /// （仍要求逐块连续，中间不能跳过 miss）。LMCache 命中的块同样计入 `cached_tokens`，
    /// 从而降低预测 prefill 时延。LMCache 查询是只读的，不改其命中统计（命中统计由
    /// 显式的 `lookup_prefix` / `batch_lookup` 负责）。
    pub fn get_prefill_cost(&self, sequence: &ActiveSequence) -> PrefillCost {
        let seq_blocks = sequence.unique_blocks();

        // 关闭 prefix caching 时，每个 `UniqueBlock::FullBlock` 携带随机哈希，跨请求
        // 不可能命中缓存 —— 跳过 PLH 查询（PLH 由 token 确定）以与「不复用」契约一致。
        let overlap_blocks = if sequence.enable_prefix_caching() {
            let plhs = sequence.positional_lineage_hashes();
            let lmcache_guard = self.lmcache.as_ref().map(|c| c.lock().unwrap());
            let mut overlap = 0;
            for (i, block) in seq_blocks.iter().enumerate() {
                match block {
                    UniqueBlock::FullBlock(seq_hash) => {
                        if self.active_full.contains_key(seq_hash) {
                            overlap += 1;
                            continue;
                        }
                        if let Some(plh) = plhs.get(i)
                            && self.registered_blocks.contains_key(plh)
                        {
                            overlap += 1;
                            continue;
                        }
                        // 本地未命中：尝试向共享 LMCache 延伸连续前缀。
                        if let Some(ref lm) = lmcache_guard
                            && lm.contains_block(seq_hash)
                        {
                            overlap += 1;
                            continue;
                        }
                        break;
                    }
                    UniqueBlock::PartialBlock(_) => break,
                }
            }
            overlap
        } else {
            0
        };

        let new_blocks = seq_blocks.len() - overlap_blocks;
        let cached_tokens = (overlap_blocks * self.block_size).min(sequence.num_input_tokens());
        let new_tokens = sequence.num_input_tokens() - cached_tokens;

        PrefillCost {
            new_blocks,
            new_tokens,
            cached_tokens,
        }
    }

    // === SECTION: LMCache 写入 ===

    /// 把一个新形成的完整块元数据写入共享 LMCache（若已接入）。
    fn save_to_lmcache(
        &mut self,
        seq_hash: SequenceHash,
        local_hash: BlockHash,
        parent_hash: Option<SequenceHash>,
        token_ids: Option<Vec<u32>>,
    ) {
        if let Some(lm) = self.lmcache.as_ref() {
            lm.lock().unwrap().store_block(LmCacheBlockMeta {
                sequence_hash: seq_hash,
                local_hash,
                parent_hash,
                token_ids,
                stored_at_ms: None,
            });
        }
    }

    // === SECTION: KV 事件发布 ===

    /// 发布一批 `Stored` / `Removed` 事件（storage tier 固定为 `Device`）。
    fn publish_kv_event(
        &mut self,
        full_blocks: Vec<SequenceHash>,
        local_hashes: &[BlockHash],
        parent_hash: Option<u64>,
        is_store: bool,
        token_ids: Option<Vec<Vec<u32>>>,
    ) {
        if full_blocks.is_empty() {
            return;
        }

        kv_cache_trace::log_vllm_trace(
            if is_store { "allocation" } else { "eviction" },
            self.dp_rank,
            self.block_size,
            self.num_active_blocks(),
            self.num_inactive_blocks(),
            self.max_capacity,
        );

        if self.kv_event_publishers.is_empty() {
            return;
        }

        let event_data = if is_store {
            events::build_stored_event_data(parent_hash, &full_blocks, local_hashes)
        } else {
            events::build_removed_event_data(&full_blocks)
        };

        let event_id = self.next_event_id;
        self.next_event_id += 1;

        let event = events::wrap_event(event_id, self.dp_rank, event_data);

        if let Err(e) = self.kv_event_publishers.publish_with_storage_tier(
            event,
            token_ids.as_deref(),
            StorageTier::Device,
        ) {
            tracing::warn!("Failed to publish KV event: {e}");
        }
    }
}
