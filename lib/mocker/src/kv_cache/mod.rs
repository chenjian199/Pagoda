// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-License-Identifier: Apache-2.0
//
// API-compatible implementation based on the public interfaces and behavioral
// contracts of NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
// Implementation rewritten by PAGODA.

//! # KV cache 模拟层
//!
//! ## 设计意图
//! 替代上游 `kv_manager` 的 KVBM 依赖，用纯内存数据结构模拟 vLLM 本地 paged KV
//! cache、prefix cache、block 生命周期与 KV event 发布。
//!
//! ## 外部契约
//! 对外导出 [`LocalVllmKvCache`]，其方法面与行为兼容上游 `KvManager`；并新增
//! [`LmCacheMockAdapter`] 等 LMCache 外部缓存模拟类型（Pagoda 扩展）。

pub mod events;
pub mod lmcache_adapter;
pub mod local_vllm_cache;

pub use lmcache_adapter::{
    shared as shared_lmcache, LmCacheBlockMeta, LmCacheMockAdapter, LmCacheMockConfig,
    LmCacheMockStats, LmCachePrefixHit, LmCacheSharedHit, LmCacheTier, SharedLmCache,
};
pub use local_vllm_cache::LocalVllmKvCache;

#[cfg(test)]
mod tests;
