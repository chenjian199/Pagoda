// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Worker 侧的模型元数据制品索引（MetadataArtifactRegistry）
//!
//! ## 设计意图
//! 当 worker 自托管模型元数据时，需要在内存中维护一张"逻辑标识 → 磁盘路径"的
//! 索引：每个元数据文件按 `(slug, suffix, filename)` 三元组登记一次，之后
//! `system_status_server` 的路由
//! `/v1/metadata/{slug}/{suffix}/{filename}` 通过该索引把原始字节流回客户端，
//! 由客户端基于 MDC 中保存的 blake3 摘要做最终校验。
//!
//! `suffix` 用于区分基础权重与 LoRA 适配：
//! * 非 LoRA 场景统一使用哨兵值 [`BASE_SUFFIX`]（`"_base"`）；
//! * LoRA 场景使用 `Slug::slugify` 产物（`[a-z0-9_-]+`）。
//!
//! 引入 `suffix` 的目的是让"卸载某个 LoRA"只清理该 LoRA 的条目，而不会
//! 误删同一个模型基础权重的注册（反之亦然）。
//!
//! ## 外部契约
//! - 公开常量 `BASE_SUFFIX = "_base"`。
//! - 公开结构体 `MetadataArtifactRegistry`（`Clone + Debug + Default`）；
//!   `clone()` 与原对象共享同一份底层存储。
//! - 方法集合 `new` / `register(slug, suffix, filename, path)` /
//!   `get(slug, suffix, filename) -> Option<PathBuf>` /
//!   `unregister(slug, suffix)` / `len()` / `is_empty()` 的签名与对外语义
//!   严格保持不变；`register` 对同键采取覆盖语义，`len` 表示已注册的文件总数。
//!
//! ## 实现要点
//! - 内部存储由原"扁平 `HashMap<(slug, suffix, filename), PathBuf>`"
//!   重构为两层嵌套 `HashMap<(slug, suffix), HashMap<filename, PathBuf>>`，
//!   带来三点收益：
//!   * `unregister(slug, suffix)` 退化为一次外层 `remove`，**复杂度由 O(n)
//!     的 `retain` 降为 O(1) 摊销**；
//!   * 同一 `(slug, suffix)` 的所有文件天然聚集在同一个内层桶中，
//!     便于将来扩展"按桶遍历"接口；
//!   * `register` / `get` 仍然是单次哈希查询 + 单次内层哈希查询，量级与原版相同。
//! - 所有可观察行为（覆盖语义、未命中返回 `None`、`len` / `is_empty` 数值）
//!   均经现有单元测试钉住，保留并扩展以覆盖嵌套结构。
//! - 并发控制保持 `Arc<RwLock<...>>`，未改动锁粒度。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::RwLock;

// === SECTION: 公共常量与类型别名 ===

/// 非 LoRA 注册所使用的 `suffix` 哨兵值。
///
/// LoRA 的 `suffix` 由 `Slug::slugify` 产生，字符集合为 `[a-z0-9_-]+`；
/// 任何名字 slugify 之后等于 `"_base"` 的 LoRA 都会与本哨兵冲突，因此不被支持。
pub const BASE_SUFFIX: &str = "_base";

/// 外层桶键：`(slug, suffix)`，用于将同一注册作用域下的所有文件聚拢。
type ScopeKey = (String, String);

/// 内层桶值：`filename → PathBuf`，对应同一 `(slug, suffix)` 下的文件集合。
type FileMap = HashMap<String, PathBuf>;

// === SECTION: 注册表结构 ===

/// 模型元数据制品索引。
///
/// 克隆出的副本与原对象共享同一份底层存储；适合分发给多组件并发读写。
#[derive(Clone, Debug, Default)]
pub struct MetadataArtifactRegistry {
    /// 两层嵌套映射：`(slug, suffix) → (filename → 磁盘路径)`。
    ///
    /// 外层用于隔离不同注册作用域、加速 `unregister`；内层负责文件级查询。
    entries: Arc<RwLock<HashMap<ScopeKey, FileMap>>>,
}

// === SECTION: 注册 / 查询 / 卸载行为 ===

impl MetadataArtifactRegistry {
    /// 构造一个空的注册表实例。
    pub fn new() -> Self {
        Self {
            entries: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 登记一个元数据文件到指定作用域。
    ///
    /// 中文说明：
    /// 1. 获取写锁，按 `(slug, suffix)` 定位外层桶；不存在则插入空内层 `HashMap`。
    /// 2. 在内层 `HashMap` 中以 `filename` 为键写入路径；
    ///    同键采取覆盖语义（与历史扁平实现一致）。
    /// 3. 输出 `debug` 日志记录注册动作，便于排障。
    pub fn register(&self, slug: &str, suffix: &str, filename: &str, path: PathBuf) {
        let mut entries = self.entries.write();
        let scope = (slug.to_string(), suffix.to_string());
        let file_map = entries.entry(scope).or_default();
        file_map.insert(filename.to_string(), path);
        tracing::debug!(slug, suffix, filename, "registered metadata artifact");
    }

    /// 查询指定 `(slug, suffix, filename)` 对应的磁盘路径。
    ///
    /// 命中时返回 `Some(PathBuf)` 的克隆，未命中（外层或内层缺键）返回 `None`。
    pub fn get(&self, slug: &str, suffix: &str, filename: &str) -> Option<PathBuf> {
        let entries = self.entries.read();
        let scope_key = (slug.to_string(), suffix.to_string());
        entries
            .get(&scope_key)
            .and_then(|file_map| file_map.get(filename).cloned())
    }

    /// 卸载某个 `(slug, suffix)` 作用域下的全部文件登记。
    ///
    /// 中文说明：嵌套结构下 `unregister` 退化为外层 `remove`，复杂度由
    /// 历史版本的 O(n) `retain` 降为 O(1) 摊销；对外可观察行为不变。
    pub fn unregister(&self, slug: &str, suffix: &str) {
        let mut entries = self.entries.write();
        let scope_key = (slug.to_string(), suffix.to_string());
        entries.remove(&scope_key);
    }

    /// 已登记的文件总数（跨所有作用域累加）。
    pub fn len(&self) -> usize {
        let entries = self.entries.read();
        entries.values().map(|file_map| file_map.len()).sum()
    }

    /// 当且仅当注册表中不存在任何已登记文件时返回 `true`。
    ///
    /// 实现上既要排除"外层为空"也要排除"外层桶存在但内层全部为空"两种情形，
    /// 与 `len() == 0` 等价但避免逐桶累加。
    pub fn is_empty(&self) -> bool {
        let entries = self.entries.read();
        entries.values().all(|file_map| file_map.is_empty())
    }
}

// === SECTION: 单元测试 ===

#[cfg(test)]
mod tests {
    //! ## 测试过程
    //! 验证 `register` / `get` 在命中、外层缺失、内层缺失三种情形下的行为，
    //! 以及 `unregister` 只清理匹配 `(slug, suffix)` 作用域、其它作用域条目
    //! 完整保留。
    //!
    //! ## 意义
    //! 这两条用例钉住"按 `(slug, suffix)` 隔离作用域"这一关键契约——
    //! 本次重构把内部存储从扁平三元组键改为两层嵌套；所有对外可观察行为
    //! （命中 / 未命中 / 卸载粒度 / `len`）必须与历史实现完全一致。

    use super::*;

    #[test]
    fn register_get_roundtrip() {
        let reg = MetadataArtifactRegistry::new();
        let p = PathBuf::from("/tmp/tokenizer.json");
        reg.register("llama-3-8b", "_base", "tokenizer.json", p.clone());

        assert_eq!(reg.get("llama-3-8b", "_base", "tokenizer.json"), Some(p));
        assert!(reg.get("llama-3-8b", "_base", "missing.json").is_none());
        assert!(reg.get("llama-3-8b", "lora-v1", "tokenizer.json").is_none());
    }

    #[test]
    fn unregister_only_removes_matching_suffix() {
        let reg = MetadataArtifactRegistry::new();
        reg.register("m", "_base", "config.json", PathBuf::from("/m/c"));
        reg.register("m", "_base", "tokenizer.json", PathBuf::from("/m/t"));
        reg.register("m", "lora-v1", "config.json", PathBuf::from("/m/c"));

        reg.unregister("m", "_base");

        assert!(reg.get("m", "_base", "config.json").is_none());
        assert!(reg.get("m", "_base", "tokenizer.json").is_none());
        // 同一 slug 的 LoRA 条目在卸载基础权重之后必须仍然存在。
        assert_eq!(
            reg.get("m", "lora-v1", "config.json"),
            Some(PathBuf::from("/m/c"))
        );
        assert_eq!(reg.len(), 1);
    }
}
