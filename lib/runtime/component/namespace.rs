// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `component::namespace` —— Namespace 的 `MetricsHierarchy` 实现
//!
//! ## 设计意图
//!
//! [`Namespace`] 在数据模型上是一棵树：
//!
//! ```text
//! DRT (root)
//!  └── Namespace A
//!       └── Namespace A.B
//!            └── Namespace A.B.C
//! ```
//!
//! 而 Prometheus 指标在落盘时需要的是"从根到当前节点"的**有序**祖
//! 先链表（`Vec<&dyn MetricsHierarchy>`），用于拼装 `metric_name` 前缀
//! 和 label 继承。本文件把"Namespace 树形结构 → 自根至叶有序祖先链"
//! 这一转换收敛在一处。
//!
//! ## 实现要点
//!
//! - `basename` 仅返回当前节点名（树形拼装由 trait 内默认实现完成）；
//! - `parent_hierarchies` 必须把 `Arc<DistributedRuntime>` 作为列表
//!   首元（根），然后再按"祖父 → 父"顺序追加各级 `Namespace`；
//! - `get_metrics_registry` 与 `connection_id` 只是字段透传。
//!
//! ## 外部契约
//!
//! 仅实现 `MetricsHierarchy for Namespace`，不暴露任何新公开符号。
//! 行为必须与重构前在所有调用点（指标命名 / 上报）严格一致。

use crate::component::Namespace;
use crate::metrics::{MetricsHierarchy, MetricsRegistry};

// ============================================================================
// 私有 helper：祖先链遍历
// ============================================================================

/// 自当前节点向上收集祖先 `Namespace` 列表。
///
/// ## 入参
///
/// - `start`：起始节点（一般是 `self.parent.as_deref()`，已经向上一跳一
///   级）。
///
/// ## 出参
///
/// `Vec<&Namespace>`：按"父 → 祖父 → 曾祖父 …"自下而上的顺序。
/// 调用方需要的"根 → 叶"顺序由 `parent_hierarchies` 中的 `.rev()` 完成，
/// 这样本 helper 自身可以保持最直观的语义。
fn ancestor_namespaces(start: Option<&Namespace>) -> Vec<&Namespace> {
    std::iter::successors(start, |ns| ns.parent.as_deref()).collect()
}

// ============================================================================
// trait 实现
// ============================================================================

impl MetricsHierarchy for Namespace {
    /// 当前节点在指标层级中的"基名"。
    ///
    /// 直接返回 `self.name`（owned `String`），由调用方负责拼装前缀。
    fn basename(&self) -> String {
        self.name.clone()
    }

    /// 返回当前节点的祖先链，自根至叶。
    ///
    /// ## 实现步骤
    ///
    /// 1. 向上遍历，收集自身往上所有 `Namespace`（先得到的是父，再
    ///    祖父）；
    /// 2. 把 `DistributedRuntime` 作为根放入返回值首位；
    /// 3. 把祖先链 `.rev()`，按"根 → 叶"顺序追加。
    ///
    /// ## 出参
    ///
    /// `Vec<&dyn MetricsHierarchy>`：第 0 个元素一定是 DRT；后续若
    /// 干元素是父 namespace 链；本节点自身**不在**返回值里。
    fn parent_hierarchies(&self) -> Vec<&dyn MetricsHierarchy> {
        let chain = ancestor_namespaces(self.parent.as_deref());

        // 预估容量：DRT(1) + 祖先链
        let mut out: Vec<&dyn MetricsHierarchy> = Vec::with_capacity(1 + chain.len());

        // 根：DRT
        out.push(&*self.runtime as &dyn MetricsHierarchy);

        // 中间各层：自根而下
        for parent in chain.iter().rev() {
            out.push(*parent as &dyn MetricsHierarchy);
        }
        out
    }

    /// 透传当前节点持有的 `MetricsRegistry`。
    fn get_metrics_registry(&self) -> &MetricsRegistry {
        &self.metrics_registry
    }

    /// 透传当前节点所在 DRT 的 `connection_id`。
    ///
    /// 用 `Option` 包装是为了与 trait 约定保持一致——其它实现者可能
    /// 没有可用的 `connection_id`。
    fn connection_id(&self) -> Option<u64> {
        use crate::traits::DistributedRuntimeProvider;
        Some(self.drt().connection_id())
    }
}

// ============================================================================
// 单元测试
//
// 仅覆盖纯结构化行为：祖先链顺序、根节点身份。涉及真实 Prometheus
// 上报的端到端断言由 `tests/`（集成测试）负责。
// ============================================================================

// 注：以下测试需要真实构造 `DistributedRuntime`，因此与 lib 中其它依赖
// `create_test_drt_async` 的测试一致，只在 `integration` feature 下编译。
#[cfg(all(test, feature = "integration"))]
mod tests {
    use super::*;
    use crate::distributed::distributed_test_utils::create_test_drt_async;

    /// ## 测试过程
    /// 1. 通过 `create_test_drt_async()` 构造一个测试 DRT；
    /// 2. 创建根 namespace `"root"`；
    /// 3. 取其 `parent_hierarchies()`；
    /// 4. 断言长度为 1，且首元 connection_id 与 DRT 一致。
    ///
    /// ## 意义
    /// 锁住"根 namespace 的祖先链只有 DRT"这一最小契约。
    #[tokio::test]
    async fn test_root_namespace_parents_contain_only_drt() {
        let drt = create_test_drt_async().await;
        let ns = drt.namespace("root").expect("root namespace");
        let parents = ns.parent_hierarchies();
        assert_eq!(parents.len(), 1, "根 namespace 的祖先链只应含 DRT 一项");
        assert_eq!(
            parents[0].connection_id(),
            Some(drt.connection_id()),
            "首元必须是 DRT",
        );
    }

    /// ## 测试过程
    /// 构造嵌套 `root → child → grandchild`，断言 grandchild 的祖先链
    /// 长度为 3 且首项是 DRT。
    ///
    /// ## 意义
    /// 验证多级嵌套时 `parent_hierarchies` 顺序与计数都正确。
    #[tokio::test]
    async fn test_nested_namespace_parents_have_root_to_leaf_order() {
        let drt = create_test_drt_async().await;
        let root = drt.namespace("root").unwrap();
        let child = root.namespace("child").unwrap();
        let grandchild = child.namespace("grand").unwrap();

        let parents = grandchild.parent_hierarchies();
        assert_eq!(parents.len(), 3, "应有 DRT + 2 级 namespace");
        assert_eq!(parents[0].connection_id(), Some(drt.connection_id()));
    }

    /// ## 测试过程
    /// 断言 `basename` 直接返回 `self.name`。
    ///
    /// ## 意义
    /// 防止后续被改成 `parent.name + "_" + self.name` 这类拼装，破坏
    /// 指标命名契约。
    #[tokio::test]
    async fn test_basename_is_raw_node_name() {
        let drt = create_test_drt_async().await;
        let ns = drt.namespace("alpha").unwrap();
        assert_eq!(ns.basename(), "alpha");
    }

    /// ## 测试过程
    /// 多层嵌套场景下 `connection_id()` 应始终返回 DRT 的 connection_id。
    ///
    /// ## 意义
    /// connection_id 在指标 label 中作为 worker 维度，本测试保证它在
    /// 树形结构中始终透传。
    #[tokio::test]
    async fn test_connection_id_propagates_from_drt() {
        let drt = create_test_drt_async().await;
        let leaf = drt
            .namespace("a")
            .unwrap()
            .namespace("b")
            .unwrap()
            .namespace("c")
            .unwrap();
        assert_eq!(leaf.connection_id(), Some(drt.connection_id()));
    }

    /// ## 测试过程
    /// 走一遍 helper `ancestor_namespaces`：根节点应返回空列表；嵌套节
    /// 点应返回"父 → 祖父"顺序。
    ///
    /// ## 意义
    /// 把"祖先链遍历"作为可独立验证的最小单元锁住，避免 trait 实现里
    /// 被悄悄改坏。
    #[tokio::test]
    async fn test_ancestor_namespaces_helper_orders_bottom_up() {
        let drt = create_test_drt_async().await;
        let root = drt.namespace("r").unwrap();
        let mid = root.namespace("m").unwrap();
        let leaf = mid.namespace("l").unwrap();

        assert!(ancestor_namespaces(root.parent.as_deref()).is_empty());

        let chain = ancestor_namespaces(leaf.parent.as_deref());
        assert_eq!(chain.len(), 2);
        assert_eq!(chain[0].name, "m");
        assert_eq!(chain[1].name, "r");
    }
}
