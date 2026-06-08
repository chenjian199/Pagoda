// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-License-Identifier: Apache-2.0
//
// API-compatible implementation based on the public interfaces and behavioral
// contracts of NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
// Implementation rewritten by PAGODA.

//! # KV 事件构造（Stored / Removed）
//!
//! ## 设计意图
//! 把 `Stored` / `Removed` 两类 router 事件的构造逻辑集中到一处，避免散落在缓存
//! 操作里。本地 GPU 缓存与 LMCache 外部缓存都复用这些构造器，只在调用时指定不同的
//! `StorageTier`。
//!
//! ## 外部契约
//! 构造出的 [`KvCacheEvent`] 字段布局、`block_hash` / `tokens_hash` / `parent_hash`
//! 的语义，必须与上游 router 事件协议一致：`Stored` 携带按顺序链接的块列表，
//! `parent_hash` 指向本批新块之前最后一个已知前缀块；`Removed` 仅携带被淘汰块的哈希。
//!
//! ## 实现要点
//! 这些函数是纯构造器，不持有状态、不做发布；`event_id` 与 `dp_rank` 由调用方提供，
//! `StorageTier` 在实际发布时再附加。

use pagoda_kv_router::protocols::{
    ExternalSequenceBlockHash, KvCacheEvent, KvCacheEventData, KvCacheRemoveData, KvCacheStoreData,
    KvCacheStoredBlockData, LocalBlockHash,
};
use pagoda_tokens::{BlockHash, SequenceHash};

// === SECTION: Stored 事件 ===

/// 构造一个 `Stored` 事件体。
///
/// `local_hashes` 要么为空（调用方没有可发布的 token 派生哈希），要么与
/// `full_blocks` 1:1 对应；缺失时回退为 `LocalBlockHash::default()`。
pub(super) fn build_stored_event_data(
    parent_hash: Option<u64>,
    full_blocks: &[SequenceHash],
    local_hashes: &[BlockHash],
) -> KvCacheEventData {
    debug_assert!(
        local_hashes.is_empty() || local_hashes.len() == full_blocks.len(),
        "build_stored_event_data: local_hashes must be empty or 1:1 with full_blocks ({} vs {})",
        local_hashes.len(),
        full_blocks.len(),
    );

    KvCacheEventData::Stored(KvCacheStoreData {
        parent_hash: parent_hash.map(ExternalSequenceBlockHash),
        start_position: None,
        blocks: full_blocks
            .iter()
            .enumerate()
            .map(|(i, global_hash)| KvCacheStoredBlockData {
                block_hash: ExternalSequenceBlockHash(*global_hash),
                tokens_hash: LocalBlockHash(local_hashes.get(i).copied().unwrap_or_default()),
                mm_extra_info: None,
            })
            .collect(),
    })
}

// === SECTION: Removed 事件 ===

/// 构造一个 `Removed` 事件体。
pub(super) fn build_removed_event_data(block_hashes: &[SequenceHash]) -> KvCacheEventData {
    KvCacheEventData::Removed(KvCacheRemoveData {
        block_hashes: block_hashes
            .iter()
            .copied()
            .map(ExternalSequenceBlockHash)
            .collect(),
    })
}

// === SECTION: 事件封装 ===

/// 给一个事件体套上 `event_id` 与 `dp_rank`，形成完整的 [`KvCacheEvent`]。
pub(super) fn wrap_event(event_id: u64, dp_rank: u32, data: KvCacheEventData) -> KvCacheEvent {
    KvCacheEvent {
        event_id,
        data,
        dp_rank,
    }
}
