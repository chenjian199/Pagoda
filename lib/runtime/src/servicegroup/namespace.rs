// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Namespace 的 MetricsHierarchy 实现。

use crate::metrics::{MetricsHierarchy, MetricsRegistry};
use crate::servicegroup::Namespace;

impl MetricsHierarchy for Namespace {
    fn basename(&self) -> &str {
        &self.name
    }

    /// 从根到直接父节点的名称列表（由内而外遍历，反转后根在前）。
    fn parent_hierarchies(&self) -> Vec<&str> {
        let mut names: Vec<&str> = Vec::new();
        let mut current: Option<&Namespace> = self.parent.as_deref();
        while let Some(ns) = current {
            names.push(ns.name.as_str());
            current = ns.parent.as_deref();
        }
        names.reverse();
        names
    }

    fn get_metrics_registry(&self) -> &MetricsRegistry {
        &self.metrics_registry
    }
}
