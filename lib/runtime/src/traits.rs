// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 运行时统一访问 trait
//!
//! ## 设计意图
//! 为需要访问运行时的类型提供两个最小接口：
//! * [`RuntimeProvider`] —— 返回本地 [`Runtime`] 引用；
//! * [`DistributedRuntimeProvider`] —— 返回 [`DistributedRuntime`] 引用。
//!
//! 上层代码可以面向这两个 trait 编写与实现解耦的运行时交互逻辑。
//!
//! ## 外部契约
//! - 公开 trait `RuntimeProvider::rt(&self) -> &Runtime` 与
//!   `DistributedRuntimeProvider::drt(&self) -> &DistributedRuntime` 的签名保持不变。
//! - 为 [`DistributedRuntime`] 提供的两个 `impl` 语义不变：`rt()` 转发给 `self.runtime()`；
//!   `drt()` 返回 `self` 本身的引用。
//! - **不**为这两个 trait 提供 blanket impl；也**不**新增默认方法。
//!
//! ## 实现要点
//! 本文件只包含 trait 声明与针对 [`DistributedRuntime`] 的组装实现，以及验证这两个
//! 实现是否返回预期引用的单元测试。

use crate::{DistributedRuntime, Runtime};

// === SECTION: trait 声明 ===

/// 为可访问 [Runtime] 的对象提供统一接口。
pub trait RuntimeProvider {
    fn rt(&self) -> &Runtime;
}

/// 为可访问 [DistributedRuntime] 的对象提供统一接口。
pub trait DistributedRuntimeProvider {
    fn drt(&self) -> &DistributedRuntime;
}

// === SECTION: DistributedRuntime 的 trait 实现 ===

impl RuntimeProvider for DistributedRuntime {
    /// 返回 `DistributedRuntime` 内部持有的本地运行时引用。
    fn rt(&self) -> &Runtime {
        let runtime_ref = self.runtime();
        runtime_ref
    }
}

// 该实现让 `DistributedRuntime` 在需要 `DistributedRuntimeProvider` 的上下文中
// 可以直接把自身暴露出去，供组件、命名空间和端点对象访问其分布式运行时。
impl DistributedRuntimeProvider for DistributedRuntime {
    /// 直接返回当前分布式运行时自身的引用。
    fn drt(&self) -> &DistributedRuntime {
        let distributed_runtime: &DistributedRuntime = self;
        distributed_runtime
    }
}

// === SECTION: 单元测试 ===

#[cfg(test)]
mod tests {
    //! ## 测试过程
    //! 创建一个进程内 `DistributedRuntime`，分别验证两个 trait 实现的返回引用
    //! 是否与预期对象在同一块内存上（`std::ptr::eq`）。
    //!
    //! ## 意义
    //! 这两个 trait 是上层代码访问 Runtime / DistributedRuntime 的唯一送出点；
    //! 任何重构都必须保证“调用 trait 拿到的引用与直接访问拿到的引用是同一个对象”。

    use super::*;

    /// 创建一个进程内测试用的 `DistributedRuntime`，供各 trait 测试复用。
    async fn create_test_drt() -> DistributedRuntime {
        let rt = crate::Runtime::from_current().unwrap();
        let config = crate::distributed::DistributedConfig::process_local();

        DistributedRuntime::new(rt, config).await.unwrap()
    }

    #[tokio::test]
    /// 测试：`RuntimeProvider` 会返回 `DistributedRuntime` 持有的原始运行时引用。
    async fn test_runtime_provider_returns_runtime_reference() {
        let drt = create_test_drt().await;

        assert!(std::ptr::eq(RuntimeProvider::rt(&drt), drt.runtime()));
    }

    #[tokio::test]
    /// 测试：`DistributedRuntimeProvider` 会返回对象自身的引用。
    async fn test_distributed_runtime_provider_returns_self() {
        let drt = create_test_drt().await;

        assert!(std::ptr::eq(DistributedRuntimeProvider::drt(&drt), &drt));
    }
}
