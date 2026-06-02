// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::registry` —— 类型擦除的共享 / 独占对象注册表
//!
//! ## 设计意图
//! 在 pipeline 装配过程中经常需要"按字符串 key 注入随后取回某个类型化对象"的能力：
//! 共享的（多份消费者读到同一个 `Arc<T>`）与独占的（只允许 take 一次，或可选 clone）。
//! 本模块用 `HashMap<String, Box<dyn Any>>` / `HashMap<String, Arc<dyn Any>>` 两路并存
//! 提供这一能力，并把"类型不匹配"与"key 不存在"两种错误以人类可读的字符串返回。
//!
//! ## 外部契约
//! - `Registry::new() -> Self`：构造空注册表。
//! - `contains_shared(&self, key) -> bool` / `contains_unique(&self, key) -> bool`：存在性判断。
//! - `insert_shared<K: ToString, U: Send + Sync + 'static>(&mut self, key, value)`：
//!   注入一个共享对象（包装为 `Arc<dyn Any + Send + Sync>`）。
//! - `get_shared<V: Send + Sync + 'static>(&self, key) -> Result<Arc<V>, String>`：
//!   按 key+目标类型取共享对象的 `Arc<V>` 克隆；
//!   失败原因（字符串）："Failed to downcast to the requested type for shared key: <key>"
//!   或 "Shared key not found: <key>"。
//! - `insert_unique<K: ToString, U: Send + Sync + 'static>(&mut self, key, value)`：
//!   注入一个独占对象（`Box<dyn Any + Send + Sync>`）。
//! - `take_unique<V: Send + Sync + 'static>(&mut self, key) -> Result<V, String>`：
//!   按 key+目标类型 **取出并移除** 对象；下一次 take 同 key 返回 "Takable key not found: <key>"。
//! - `clone_unique<V: Clone + Send + Sync + 'static>(&self, key) -> Result<V, String>`：
//!   非破坏性地克隆一份；要求目标类型实现 `Clone`。被 clone 的对象仍可后续被 take。
//! - `#[derive(Debug, Default)]`：`Default` 等价于 `Registry::new()`。
//!
//! ## 实现要点
//! - 两个内部 HashMap 严格分离：共享对象走 `Arc<dyn Any>`、独占对象走 `Box<dyn Any>`；
//!   不复用同一张表是为避免"取共享时意外消耗"或"取独占时漏 clone"。
//! - 失败信息保留既有字符串原文，便于上层日志/测试 grep 对齐。
//! - 文档示例（doc test）保留：演示典型工作流（insert / get / take / clone / 再 take）。

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

/// 同时管理共享对象和独占对象的注册表结构体。
///
/// # 示例
///
/// ```
/// use pagoda_runtime::pipeline::registry::Registry;
///
/// let mut registry = Registry::new();
///
/// // 插入并取回共享对象
/// registry.insert_shared("shared1", 42);
/// assert_eq!(*registry.get_shared::<i32>("shared1").unwrap(), 42);
///
/// // 插入并取出独占对象
/// registry.insert_unique("unique1", "Hello".to_string());
/// assert_eq!(registry.take_unique::<String>("unique1").unwrap(), "Hello");
///
/// // 再次取出同一个独占对象应当失败，因为它已经被移除
/// assert!(registry.take_unique::<String>("unique1").is_err());
///
/// // 插入并克隆独占对象
/// registry.insert_unique("unique2", "World".to_string());
/// assert_eq!(registry.clone_unique::<String>("unique2").unwrap(), "World");
///
/// // 取出同一个已克隆的独占对象是允许的
/// assert!(registry.take_unique::<String>("unique2").is_ok());
///
/// ```
#[derive(Debug, Default)]
pub struct Registry {
    shared_storage: HashMap<String, Arc<dyn Any + Send + Sync>>, // 共享对象
    unique_storage: HashMap<String, Box<dyn Any + Send + Sync>>, // 可取出的对象
}

// === SECTION: Registry inherent methods ===

impl Registry {
    /// 创建一个空注册表。
    pub fn new() -> Self {
        Registry {
            shared_storage: HashMap::new(),
            unique_storage: HashMap::new(),
        }
    }

    /// 按 key 检查共享对象是否存在于注册表中。
    pub fn contains_shared(&self, key: &str) -> bool {
        self.shared_storage.contains_key(key)
    }

    /// 以指定 key 将共享对象插入注册表。
    pub fn insert_shared<K: ToString, U: Send + Sync + 'static>(&mut self, key: K, value: U) {
        self.shared_storage.insert(
            key.to_string(),
            Arc::new(value) as Arc<dyn Any + Send + Sync>,
        );
    }

    /// 按 key 和类型从注册表中取回共享对象。
    pub fn get_shared<V: Send + Sync + 'static>(&self, key: &str) -> Result<Arc<V>, String> {
        match self.shared_storage.get(key) {
            Some(boxed) => boxed.clone().downcast::<V>().map_err(|_| {
                format!(
                    "Failed to downcast to the requested type for shared key: {}",
                    key
                )
            }),
            None => Err(format!("Shared key not found: {}", key)),
        }
    }

    /// 按 key 检查独占对象是否存在于注册表中。
    pub fn contains_unique(&self, key: &str) -> bool {
        self.unique_storage.contains_key(key)
    }

    /// 以指定 key 将独占对象插入注册表。
    pub fn insert_unique<K: ToString, U: Send + Sync + 'static>(&mut self, key: K, value: U) {
        self.unique_storage.insert(
            key.to_string(),
            Box::new(value) as Box<dyn Any + Send + Sync>,
        );
    }

    /// 按 key 和类型从注册表中取出独占对象，并将其从注册表里移除。
    pub fn take_unique<V: Send + Sync + 'static>(&mut self, key: &str) -> Result<V, String> {
        match self.unique_storage.remove(key) {
            Some(boxed) => boxed.downcast::<V>().map(|b| *b).map_err(|_| {
                format!(
                    "Failed to downcast to the requested type for unique key: {}",
                    key
                )
            }),
            None => Err(format!("Takable key not found: {}", key)),
        }
    }

    /// 如果独占对象实现了 `Clone`，则从注册表中克隆一份。
    pub fn clone_unique<V: Clone + Send + Sync + 'static>(&self, key: &str) -> Result<V, String> {
        match self.unique_storage.get(key) {
            Some(boxed) => boxed.downcast_ref::<V>().cloned().ok_or_else(|| {
                format!(
                    "Failed to downcast to the requested type for unique key: {}",
                    key
                )
            }),
            None => Err(format!("Takable key not found: {}", key)),
        }
    }
}

// === SECTION: tests ===

#[cfg(test)]
mod tests {
    //! ## 测试矩阵
    //!
    //! | 测试名 | 覆盖维度 |
    //! |---|---|
    //! | `test_insert_and_get_shared` | 共享对象 insert/get + 类型不匹配失败 |
    //! | `test_insert_and_take_unique` | 独占对象 insert/take + 二次 take 必失败 |
    //! | `test_insert_and_clone_then_take_unique` | clone 不消耗，clone 后仍可 take |
    //! | `test_failed_take_after_cloning` | clone→take 成功后再次 take 失败 |
    //!
    //! ## 测试过程
    //! 四个测试覆盖：
    //! 1. `test_insert_and_get_shared` — 共享对象插入/取回 + 类型不匹配的失败路径
    //! 2. `test_insert_and_take_unique` — 独占对象插入/取出 + 二次取出必失败
    //! 3. `test_insert_and_clone_then_take_unique` — 克隆不消耗，克隆后仍可取出
    //! 4. `test_failed_take_after_cloning` — 克隆 → 取出成功后，再次取出失败
    //!
    //! ## 意义
    //! 覆盖共享/独占两条独立存储 × 插入/取回/取出/克隆 × 成功/失败 的
    //! 组合，为 pipeline 装配阶段依赖 `Registry` 注入参数的行为提供回归保证。

    use super::*;

    #[test]
    fn test_insert_and_get_shared() {
        let mut registry = Registry::new();
        registry.insert_shared("shared1", 42);
        assert_eq!(*registry.get_shared::<i32>("shared1").unwrap(), 42);
        assert!(registry.get_shared::<f64>("shared1").is_err()); // 测试 downcast 失败
    }

    #[test]
    fn test_insert_and_take_unique() {
        let mut registry = Registry::new();
        registry.insert_unique("unique1", "Hello".to_string());
        assert_eq!(registry.take_unique::<String>("unique1").unwrap(), "Hello");
        assert!(registry.take_unique::<String>("unique1").is_err()); // 该 key 已不存在
    }

    #[test]
    fn test_insert_and_clone_then_take_unique() {
        let mut registry = Registry::new();

        registry.insert_unique("unique2", "World".to_string());

        assert_eq!(registry.clone_unique::<String>("unique2").unwrap(), "World");

        // 克隆后，该对象仍应可被取出
        assert!(registry.take_unique::<String>("unique2").is_ok());
    }

    #[test]
    fn test_failed_take_after_cloning() {
        let mut registry = Registry::new();

        registry.insert_unique("unique3", "Another".to_string());
        assert_eq!(
            registry.clone_unique::<String>("unique3").unwrap(),
            "Another"
        );

        // 先克隆，再取出是允许的
        assert_eq!(
            registry.take_unique::<String>("unique3").unwrap(),
            "Another"
        );

        // 取出后再次取出应当失败
        assert!(registry.take_unique::<String>("unique3").is_err());
    }
}
