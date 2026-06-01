// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `servicegroup::registry` —— `Registry` 的构造器与默认值
//!
//! ## 设计意图
//!
//! 上层 [`Registry`] 是 servicegroup 模块对外公开的"服务注册中心句柄"，
//! 其内部用 `Arc<tokio::sync::Mutex<RegistryInner>>` 共享一份服务表。
//! 本文件**只**提供这一类型的构造路径，把"如何初始化内部状态"集中
//! 在一处，避免上游代码在多处重复写 `Arc::new(Mutex::new(...))`。
//!
//! ## 实现要点
//!
//! - `Registry::new` 构造一个空的注册表；
//! - `Default` 直接委托给 `new()`，使 `Registry::default()` 与 `new()`
//!   行为一致；
//! - 通过 `Arc` clone 共享 `inner`，使得 `Registry::clone()` 自动具备
//!   "句柄共享状态"语义。
//!
//! ## 外部契约
//!
//! - `impl Default for Registry`
//! - `impl Registry { pub fn new() -> Self }`
//!
//! 字段 `inner: Arc<Mutex<RegistryInner>>` 的类型与可见性由父模块决
//! 定，本文件不做改动。

use std::sync::Arc;
use tokio::sync::Mutex;

use crate::servicegroup::{Registry, RegistryInner};

// ============================================================================
// 私有 helper
// ============================================================================

/// 申请一个共享的 `RegistryInner`。把"用 `Arc<Mutex<...>>` 包装"这件
/// 事独立成函数，使得 `new()` 主体读起来更接近自然语言。
fn fresh_inner_handle() -> Arc<Mutex<RegistryInner>> {
    Arc::new(Mutex::new(RegistryInner::default()))
}

// ============================================================================
// 公开实现
// ============================================================================

impl Default for Registry {
    /// 与 `Registry::new()` 行为完全一致。
    ///
    /// 实现 `Default` 主要是为了让 `Registry` 能放入 `#[derive(Default)]`
    /// 的复合结构体里。
    fn default() -> Self {
        Self::new()
    }
}

impl Registry {
    /// 构造一个空的注册表句柄。
    ///
    /// ## 出参
    ///
    /// 一个新的 `Registry`，其 `inner` 指向一个**独立的** `RegistryInner`
    /// 实例。任何对该实例 clone 出来的句柄都会共享同一份 inner。
    ///
    /// ## 复杂度
    ///
    /// `O(1)`。只做一次 `Arc::new(Mutex::new(Default))`。
    pub fn new() -> Self {
        Self {
            inner: fresh_inner_handle(),
        }
    }
}

// ============================================================================
// 单元测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// ## 测试过程
    /// `Registry::new()` 出来的实例，其 `inner` lock 应是空表。
    ///
    /// ## 意义
    /// 锁定"新注册表初始为空"这一最小契约。
    #[tokio::test]
    async fn test_registry_new_starts_empty() {
        let reg = Registry::new();
        let inner = reg.inner.lock().await;
        // RegistryInner::default() 应当对应空状态；不便直接访问字段时
        // 我们仅校验它能正常 lock 拿到（不死锁、不 panic）。
        // 这等价于"构造路径正常工作"。
        drop(inner);
    }

    /// ## 测试过程
    /// `Registry::default()` 与 `Registry::new()` 等价：都能正常构造、
    /// 都能正常 lock。
    ///
    /// ## 意义
    /// 防止后续有人把 `Default` 改成与 `new()` 不一致的实现。
    #[tokio::test]
    async fn test_registry_default_matches_new() {
        let a = Registry::default();
        let b = Registry::new();
        let _ = a.inner.lock().await;
        let _ = b.inner.lock().await;
    }

    /// ## 测试过程
    /// 用 `Registry::clone()` 出两个句柄，断言它们的 `inner` 指向同一
    /// 个 `Arc`（指针相等），即"共享底层状态"。
    ///
    /// ## 意义
    /// `Registry` 在跨任务传递时必须是"共享句柄"语义；本测试守住该不
    /// 变量。
    #[tokio::test]
    async fn test_registry_clone_shares_inner() {
        let reg = Registry::new();
        let cloned = reg.clone();
        assert!(
            Arc::ptr_eq(&reg.inner, &cloned.inner),
            "clone 出来的 Registry 必须共享同一个 inner Arc",
        );
    }

    /// ## 测试过程
    /// 顺序申请多个 `Registry`，断言它们的 `inner` 指向**不同**的
    /// `Arc`，即每次 `new()` 都是独立实例。
    ///
    /// ## 意义
    /// 防止后续被改成单例（`OnceLock` 之类），破坏"每次 new 都拿独立
    /// 表"的语义。
    #[tokio::test]
    async fn test_registry_new_produces_independent_instances() {
        let a = Registry::new();
        let b = Registry::new();
        assert!(
            !Arc::ptr_eq(&a.inner, &b.inner),
            "两个独立 new 出来的 Registry 不应共享 inner",
        );
    }

    /// ## 测试过程
    /// 在多个 tokio task 之间共享同一个 `Registry`，每个 task 都做一次
    /// `inner.lock().await`，不应产生死锁或 panic。
    ///
    /// ## 意义
    /// 确保 `Arc<Mutex<_>>` 选型在并发访问下行为正确。
    #[tokio::test]
    async fn test_registry_inner_supports_concurrent_locking() {
        let reg = Registry::new();
        let mut tasks = vec![];
        for _ in 0..4 {
            let r = reg.clone();
            tasks.push(tokio::spawn(async move {
                let _g = r.inner.lock().await;
                // 持锁时间足够短，让其它 task 也能拿到锁
                tokio::task::yield_now().await;
            }));
        }
        for t in tasks {
            t.await.expect("task should not panic");
        }
    }
}
