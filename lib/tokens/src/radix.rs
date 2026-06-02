// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 位置稀疏基数树。
//!
//! ## 设计意图
//! 以「位置」为第一层索引，把同一位置下的位置感知哈希聚到同一个子映射里，
//! 形成一棵两层的并发字典树。这样既能按位置批量取值，又能在位置维度上保持稀疏。
//!
//! ## 外部契约
//! - `PositionalRadixTree<V, K = PositionalSequenceHash>`，要求 `K: PositionalHash + Hash + Eq + Clone`。
//! - 方法：`new`、`prefix`、`position`、`len`、`is_empty`，以及 `Default`，derive `Clone`。
//! - `prefix` 返回该位置层级的可变入口（不存在则按需创建）；`position` 返回只读子映射（可能为空）。
//!
//! ## 实现要点
//! 外层 `DashMap<u64, _>` 以位置为键，内层 `DashMap<K, V>` 存放该位置上的全部条目；
//! 计数通过对各层 `len` 求和得到。

use dashmap::DashMap;
use std::hash::Hash;

use crate::{PositionalHash, PositionalSequenceHash};

// === SECTION: 数据结构 ===

/// 用于高效索引 [位置感知哈希][`crate::PositionalSequenceHash`] 的位置稀疏基数树。
#[derive(Clone)]
pub struct PositionalRadixTree<V, K = PositionalSequenceHash>
where
    K: PositionalHash + Hash + Eq + Clone,
{
    /// 外层按位置分桶，内层保存该位置下的键值条目。
    levels: DashMap<u64, DashMap<K, V>>,
}

// === SECTION: 核心操作 ===

impl<V, K> PositionalRadixTree<V, K>
where
    K: PositionalHash + Hash + Eq + Clone,
{
    /// 创建一棵空的 [`PositionalRadixTree`]。
    pub fn new() -> Self {
        Self::default()
    }

    /// 取出 `key` 所在位置层级的可变入口，必要时即时创建该层级。
    pub fn prefix(&self, key: &K) -> dashmap::mapref::one::RefMut<'_, u64, DashMap<K, V>> {
        self.levels.entry(key.position()).or_default()
    }

    /// 返回给定位置上的全部条目所组成的子映射（不存在则为 `None`）。
    pub fn position(
        &self,
        position: u64,
    ) -> Option<dashmap::mapref::one::RefMut<'_, u64, DashMap<K, V>>> {
        self.levels.get_mut(&position)
    }

    /// 返回 [`PositionalRadixTree`] 中的条目总数。
    pub fn len(&self) -> usize {
        self.levels
            .iter()
            .fold(0usize, |acc, level| acc + level.len())
    }

    /// 当 [`PositionalRadixTree`] 没有任何条目时返回 `true`。
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// === SECTION: 默认值 ===

impl<V, K> Default for PositionalRadixTree<V, K>
where
    K: PositionalHash + Hash + Eq + Clone,
{
    fn default() -> Self {
        Self {
            levels: DashMap::new(),
        }
    }
}
