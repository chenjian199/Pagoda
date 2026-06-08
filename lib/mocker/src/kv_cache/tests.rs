// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-License-Identifier: Apache-2.0
//
// API-compatible implementation based on the public interfaces and behavioral
// contracts of NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
// Implementation rewritten by PAGODA.

//! `LocalVllmKvCache` 的行为测试，全部围绕公开 API（`process` / `get_prefill_cost`
//! 与一组只读计数）与对外可观察的 KV 事件流展开。

use std::sync::{Arc, Mutex};

use pagoda_kv_router::protocols::{KvCacheEvent, KvCacheEventData};
use pagoda_tokens::blocks::UniqueBlock;
use pagoda_tokens::PositionalLineageHash;
use uuid::Uuid;

use super::LocalVllmKvCache;
use crate::common::protocols::{KvCacheEventSink, KvEventPublishers, MoveBlock};
use crate::common::sequence::ActiveSequence;
use crate::kv_cache::{shared_lmcache, LmCacheMockAdapter, SharedLmCache};

// === SECTION: 测试脚手架 ===

/// 捕获已发布事件的 sink，用于断言 router 通告行为。
#[derive(Default)]
struct CapturingSink {
    events: Mutex<Vec<KvCacheEvent>>,
}

impl KvCacheEventSink for CapturingSink {
    fn publish(&self, event: KvCacheEvent) -> anyhow::Result<()> {
        self.events.lock().unwrap().push(event);
        Ok(())
    }
}

fn make_cache(capacity: usize, block_size: usize) -> LocalVllmKvCache {
    LocalVllmKvCache::new_with_event_sink(capacity, block_size, KvEventPublishers::default(), 0)
}

fn make_cache_capturing(capacity: usize, block_size: usize) -> (LocalVllmKvCache, Arc<CapturingSink>) {
    let sink = Arc::new(CapturingSink::default());
    let publishers = KvEventPublishers::new(Some(sink.clone() as _), None);
    (
        LocalVllmKvCache::new_with_event_sink(capacity, block_size, publishers, 0),
        sink,
    )
}

fn plh(v: u64) -> PositionalLineageHash {
    PositionalLineageHash::new(v, None, 0)
}

fn use_full(cache: &mut LocalVllmKvCache, seq_hash: u64, p: PositionalLineageHash) -> usize {
    cache.process(&MoveBlock::Use(
        vec![UniqueBlock::FullBlock(seq_hash)],
        vec![],
        vec![p],
        None,
        None,
    ))
}

fn use_partial(cache: &mut LocalVllmKvCache, uuid: Uuid) -> usize {
    cache.process(&MoveBlock::Use(
        vec![UniqueBlock::PartialBlock(uuid)],
        vec![],
        vec![],
        None,
        None,
    ))
}

fn deref_full(cache: &mut LocalVllmKvCache, seq_hash: u64) {
    cache.process(&MoveBlock::Deref(vec![UniqueBlock::FullBlock(seq_hash)]));
}

fn deref_partial(cache: &mut LocalVllmKvCache, uuid: Uuid) {
    cache.process(&MoveBlock::Deref(vec![UniqueBlock::PartialBlock(uuid)]));
}

fn count_stored(sink: &CapturingSink) -> usize {
    sink.events
        .lock()
        .unwrap()
        .iter()
        .filter(|e| matches!(e.data, KvCacheEventData::Stored(_)))
        .count()
}

fn count_removed(sink: &CapturingSink) -> usize {
    sink.events
        .lock()
        .unwrap()
        .iter()
        .filter(|e| matches!(e.data, KvCacheEventData::Removed(_)))
        .count()
}

// === SECTION: Use 基本语义 ===

#[test]
fn test_use_single_full_block() {
    // ## 测试过程
    // 对空 cache 使用一个完整块。
    // ## 意义
    // 验证最基本的 Use 路径：新分配一个块，distinct active 计数为 1。
    let mut cache = make_cache(10, 16);
    assert_eq!(use_full(&mut cache, 1, plh(100)), 1);
    assert_eq!(cache.num_active_blocks(), 1);
}

#[test]
fn test_duplicate_use_bumps_refcount() {
    // ## 测试过程
    // 同一 seq_hash 连续 Use 两次。
    // ## 意义
    // 验证共享前缀复用：物理块仍只有一个，但引用计数升到 2。
    let mut cache = make_cache(10, 16);
    use_full(&mut cache, 1, plh(100));
    use_full(&mut cache, 1, plh(100));
    assert_eq!(cache.num_active_blocks(), 1);
    assert_eq!(cache.num_active_block_refs(), 2);
}

#[test]
fn use_rejects_short_token_ids_before_mutating_state() {
    // ## 测试过程
    // 提供与 FullBlock 数量不匹配的 token_ids，触发断言 panic。
    // ## 意义
    // 验证前置校验在改动任何状态、发布任何事件之前失败。
    let (mut cache, sink) = make_cache_capturing(10, 4);

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        cache.process(&MoveBlock::Use(
            vec![UniqueBlock::FullBlock(1), UniqueBlock::FullBlock(2)],
            vec![101, 102],
            vec![plh(100), plh(200)],
            Some(vec![vec![1, 2, 3, 4]]),
            None,
        ));
    }));

    assert!(result.is_err());
    assert_eq!(cache.num_active_blocks(), 0);
    assert!(sink.events.lock().unwrap().is_empty());
}

#[test]
fn test_capacity_exhaustion_returns_partial() {
    // ## 测试过程
    // 容量为 4 的 cache 填满后再分配第五个块。
    // ## 意义
    // 验证容量耗尽且无 inactive 可淘汰时，Use 返回 0。
    let mut cache = make_cache(4, 16);
    for i in 0..4 {
        assert_eq!(use_full(&mut cache, i, plh(i + 100)), 1);
    }
    assert_eq!(use_full(&mut cache, 4, plh(500)), 0);
}

// === SECTION: Deref 与 inactive 复用 ===

#[test]
fn test_deref_returns_to_inactive() {
    // ## 测试过程
    // Use 一个完整块后 Deref。
    // ## 意义
    // 验证引用归零后块离开 active 池，转入 inactive。
    let mut cache = make_cache(4, 16);
    use_full(&mut cache, 1, plh(100));
    deref_full(&mut cache, 1);
    assert_eq!(cache.num_active_blocks(), 0);
    assert_eq!(cache.num_inactive_blocks(), 1);
}

#[test]
fn test_inactive_reuse_via_match_blocks() {
    // ## 测试过程
    // Use 后 Deref，再用相同 PLH 复用。
    // ## 意义
    // 验证 inactive 池能按 PLH 命中复用，无需新分配槽位。
    let mut cache = make_cache(10, 16);
    let p = plh(100);
    use_full(&mut cache, 1, p);
    deref_full(&mut cache, 1);
    assert_eq!(cache.num_inactive_blocks(), 1);
    assert_eq!(use_full(&mut cache, 2, p), 1);
    assert_eq!(cache.num_inactive_blocks(), 0);
    assert_eq!(cache.num_active_blocks(), 1);
}

#[test]
fn test_eviction_frees_inactive_for_new_allocation() {
    // ## 测试过程
    // 填满 4 个块、全部 Deref 进 inactive，再分配 4 个全新块。
    // ## 意义
    // 验证容量满时按 LRU 淘汰 inactive 块以容纳新分配，最终 active 为 4。
    let mut cache = make_cache(4, 16);
    for i in 0..4 {
        use_full(&mut cache, i, plh(i + 100));
    }
    for i in 0..4 {
        deref_full(&mut cache, i);
    }
    for i in 10..14 {
        assert_eq!(use_full(&mut cache, i, plh(i + 1000)), 1);
    }
    assert_eq!(cache.num_active_blocks(), 4);
    assert_eq!(cache.num_inactive_blocks(), 0);
}

#[test]
fn test_deref_partial_returns_to_reset() {
    // ## 测试过程
    // Use 一个局部块后 Deref。
    // ## 意义
    // 验证局部块释放后离开 active 池，引用计数归零。
    let mut cache = make_cache(10, 16);
    let uuid = Uuid::new_v4();
    use_partial(&mut cache, uuid);
    assert_eq!(cache.num_active_blocks(), 1);
    deref_partial(&mut cache, uuid);
    assert_eq!(cache.num_active_block_refs(), 0);
    assert_eq!(cache.num_active_blocks(), 0);
}

// === SECTION: Promote ===

#[test]
fn test_promote_basic() {
    // ## 测试过程
    // Use 一个局部块，随后 Promote 为完整块。
    // ## 意义
    // 验证局部块晋升后变为可被前缀复用的完整 active 块。
    let mut cache = make_cache(10, 16);
    let uuid = Uuid::new_v4();
    use_partial(&mut cache, uuid);
    cache.process(&MoveBlock::Promote(uuid, 42, None, 0, plh(500), None));
    assert_eq!(cache.num_active_blocks(), 1);
}

#[test]
#[should_panic(expected = "Promote: partial block not found")]
fn test_promote_nonexistent_panics() {
    // ## 测试过程
    // 在没有对应局部块时直接 Promote。
    // ## 意义
    // 验证非法 Promote 立即 panic，错误文案与契约一致。
    let mut cache = make_cache(10, 16);
    cache.process(&MoveBlock::Promote(
        Uuid::new_v4(),
        42,
        None,
        0,
        plh(500),
        None,
    ));
}

// === SECTION: prefill cost ===

#[test]
fn test_prefill_cost_no_overlap() {
    // ## 测试过程
    // 对空 cache 计算一个全新序列的 prefill cost。
    // ## 意义
    // 验证无任何缓存命中时，所有块都是新块、所有 token 都需重算。
    let cache = make_cache(10, 16);
    let tokens: Vec<u32> = (0..35).collect();
    let seq = ActiveSequence::new(tokens, 10, Some(16), true, false);
    let cost = cache.get_prefill_cost(&seq);
    assert_eq!(cost.new_blocks, seq.unique_blocks().len());
    assert_eq!(cost.new_tokens, 35);
    assert_eq!(cost.cached_tokens, 0);
}

#[test]
fn test_prefill_cost_counts_active_and_inactive_prefix() {
    // ## 测试过程
    // 先注册同一序列的前缀块（部分 active、部分 Deref 进 inactive），再次计算其 cost。
    // ## 意义
    // 验证前缀命中同时覆盖 active 与 inactive 来源，且遇首个 miss 即停止。
    let block_size = 16;
    let mut cache = make_cache(10, block_size);
    let tokens: Vec<u32> = (0..(block_size as u32 * 2)).collect();
    let mut seq = ActiveSequence::new(tokens.clone(), 10, Some(block_size), true, false);

    // 注册整个 prompt 的两个完整块。
    let signal = seq.take_creation_signal().unwrap();
    let full_blocks = match &signal {
        MoveBlock::Use(blocks, ..) => {
            blocks.iter().filter(|b| matches!(b, UniqueBlock::FullBlock(_))).count()
        }
        _ => 0,
    };
    assert!(full_blocks >= 2);
    cache.process(&signal);

    // 第二个相同序列应在前缀上完全命中。
    let seq2 = ActiveSequence::new(tokens, 10, Some(block_size), true, false);
    let cost = cache.get_prefill_cost(&seq2);
    assert_eq!(cost.cached_tokens, full_blocks * block_size);
    assert_eq!(cost.new_blocks, seq2.unique_blocks().len() - full_blocks);
}

#[test]
fn test_prefill_cost_zero_when_prefix_caching_disabled() {
    // ## 测试过程
    // 关闭 prefix caching 后计算 cost。
    // ## 意义
    // 验证禁用前缀缓存时永不命中，cached_tokens 恒为 0。
    let cache = make_cache(10, 16);
    let tokens: Vec<u32> = (0..40).collect();
    let seq = ActiveSequence::new(tokens, 10, Some(16), false, false);
    let cost = cache.get_prefill_cost(&seq);
    assert_eq!(cost.cached_tokens, 0);
}

// === SECTION: KV 事件发布 ===

#[test]
fn test_new_full_block_emits_stored_event() {
    // ## 测试过程
    // 使用一个全新完整块。
    // ## 意义
    // 验证仅 NewStore 才发布 Stored 事件，且只发一次。
    let (mut cache, sink) = make_cache_capturing(10, 16);
    use_full(&mut cache, 1, plh(100));
    assert_eq!(count_stored(&sink), 1);
}

#[test]
fn test_active_hit_does_not_emit_stored_event() {
    // ## 测试过程
    // 同一块 Use 两次。
    // ## 意义
    // 验证第二次 Use 命中 active，不再发布 Stored 事件。
    let (mut cache, sink) = make_cache_capturing(10, 16);
    use_full(&mut cache, 1, plh(100));
    use_full(&mut cache, 1, plh(100));
    assert_eq!(count_stored(&sink), 1);
}

#[test]
fn test_eviction_emits_removed_event() {
    // ## 测试过程
    // 填满容量并全部 Deref 进 inactive，再分配新块迫使淘汰。
    // ## 意义
    // 验证 inactive 块被淘汰时发布 Removed 事件。
    let (mut cache, sink) = make_cache_capturing(2, 16);
    use_full(&mut cache, 1, plh(100));
    use_full(&mut cache, 2, plh(200));
    deref_full(&mut cache, 1);
    deref_full(&mut cache, 2);
    // 两个 inactive 块；分配两个新块需淘汰这两个。
    use_full(&mut cache, 3, plh(300));
    use_full(&mut cache, 4, plh(400));
    assert!(count_removed(&sink) >= 1);
}

// === SECTION: 只读计数 ===

#[test]
fn test_block_size_and_capacity_accessors() {
    // ## 测试过程
    // 读取 max_capacity / block_size / dp_rank。
    // ## 意义
    // 验证只读访问器返回构造时的配置值。
    let cache = make_cache(7, 32);
    assert_eq!(cache.max_capacity(), 7);
    assert_eq!(cache.block_size(), 32);
    assert_eq!(cache.dp_rank(), 0);
    assert_eq!(cache.get_active_perc(), 0.0);
}

// === SECTION: LMCache 跨 worker 集成 ===

/// 构造接入共享 LMCache 的本地缓存（独立 sink，模拟一个 worker）。
fn make_worker_with_lmcache(
    capacity: usize,
    block_size: usize,
    dp_rank: u32,
    lmcache: SharedLmCache,
) -> LocalVllmKvCache {
    LocalVllmKvCache::new_with_event_sink(
        capacity,
        block_size,
        KvEventPublishers::default(),
        dp_rank,
    )
    .with_lmcache(lmcache)
}

#[test]
fn test_lmcache_cross_worker_prefix_hit_reduces_cost() {
    // ## 测试过程
    // worker A 与 worker B 共享同一个 LMCache。A 处理一个 prompt 形成完整块（写入
    // LMCache），随后 B（本地缓存为空）对相同 token 序列计算 prefill cost。
    // ## 意义
    // 验证「跨 worker 命中」：A 写入的前缀块让 B 在本地未命中的情况下仍能通过 LMCache
    // 命中，从而降低 cached_tokens（即降低预测 prefill 时延）。
    let block_size = 16;
    let lmcache = shared_lmcache(LmCacheMockAdapter::with_default());

    let mut worker_a = make_worker_with_lmcache(64, block_size, 0, lmcache.clone());
    let worker_b = make_worker_with_lmcache(64, block_size, 1, lmcache.clone());

    let tokens: Vec<u32> = (0..(block_size as u32 * 3)).collect();

    // worker A：注册整个 prompt 的完整块。
    let mut seq_a = ActiveSequence::new(tokens.clone(), 10, Some(block_size), true, false);
    let signal = seq_a.take_creation_signal().unwrap();
    let full_blocks = match &signal {
        MoveBlock::Use(blocks, ..) => blocks
            .iter()
            .filter(|b| matches!(b, UniqueBlock::FullBlock(_)))
            .count(),
        _ => 0,
    };
    assert!(full_blocks >= 3);
    worker_a.process(&signal);

    // worker B：本地缓存为空，但应通过共享 LMCache 命中相同前缀。
    let seq_b = ActiveSequence::new(tokens, 10, Some(block_size), true, false);
    let cost = worker_b.get_prefill_cost(&seq_b);
    assert_eq!(cost.cached_tokens, full_blocks * block_size);
    assert_eq!(cost.new_blocks, seq_b.unique_blocks().len() - full_blocks);
}

#[test]
fn test_lmcache_absent_no_cross_worker_hit() {
    // ## 测试过程
    // 两个 worker 各自独立、未接入 LMCache。A 注册前缀后，B 计算相同序列的 cost。
    // ## 意义
    // 作为对照：没有共享 LMCache 时不存在跨 worker 命中，B 的 cached_tokens 为 0。
    let block_size = 16;
    let mut worker_a = make_cache(64, block_size);
    let worker_b = make_cache(64, block_size);

    let tokens: Vec<u32> = (0..(block_size as u32 * 3)).collect();
    let mut seq_a = ActiveSequence::new(tokens.clone(), 10, Some(block_size), true, false);
    worker_a.process(&seq_a.take_creation_signal().unwrap());

    let seq_b = ActiveSequence::new(tokens, 10, Some(block_size), true, false);
    let cost = worker_b.get_prefill_cost(&seq_b);
    assert_eq!(cost.cached_tokens, 0);
}

#[test]
fn test_lmcache_records_stores_on_new_blocks() {
    // ## 测试过程
    // 接入 LMCache 的 worker 处理一个含多个完整块的 prompt。
    // ## 意义
    // 验证新完整块会写入共享 LMCache（stores 统计 > 0，且块数与完整块数一致）。
    let block_size = 16;
    let lmcache = shared_lmcache(LmCacheMockAdapter::with_default());
    let mut worker = make_worker_with_lmcache(64, block_size, 0, lmcache.clone());

    let tokens: Vec<u32> = (0..(block_size as u32 * 2)).collect();
    let mut seq = ActiveSequence::new(tokens, 10, Some(block_size), true, false);
    let signal = seq.take_creation_signal().unwrap();
    let full_blocks = match &signal {
        MoveBlock::Use(blocks, ..) => blocks
            .iter()
            .filter(|b| matches!(b, UniqueBlock::FullBlock(_)))
            .count(),
        _ => 0,
    };
    worker.process(&signal);

    let guard = lmcache.lock().unwrap();
    assert_eq!(guard.stats().stores as usize, full_blocks);
    let (l1, l2) = guard.len();
    assert_eq!(l1, full_blocks);
    assert_eq!(l2, full_blocks);
}

#[test]
fn test_lmcache_disabled_prefix_caching_no_hit() {
    // ## 测试过程
    // worker A 关闭 prefix caching 注册块；worker B（共享 LMCache，关闭 prefix caching）
    // 计算相同 token 的 cost。
    // ## 意义
    // 验证关闭 prefix caching 时块哈希随机，即便共享 LMCache 也不可能跨 worker 命中。
    let block_size = 16;
    let lmcache = shared_lmcache(LmCacheMockAdapter::with_default());
    let mut worker_a = make_worker_with_lmcache(64, block_size, 0, lmcache.clone());
    let worker_b = make_worker_with_lmcache(64, block_size, 1, lmcache.clone());

    let tokens: Vec<u32> = (0..(block_size as u32 * 3)).collect();
    let mut seq_a = ActiveSequence::new(tokens.clone(), 10, Some(block_size), false, false);
    worker_a.process(&seq_a.take_creation_signal().unwrap());

    let seq_b = ActiveSequence::new(tokens, 10, Some(block_size), false, false);
    let cost = worker_b.get_prefill_cost(&seq_b);
    assert_eq!(cost.cached_tokens, 0);
}
