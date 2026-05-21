// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! ServiceGroup 辅助实现（MetricsHierarchy 等）。

use crate::metrics::{MetricsHierarchy, MetricsRegistry};
use crate::servicegroup::ServiceGroup;

impl MetricsHierarchy for ServiceGroup {
    fn basename(&self) -> &str {
        &self.name
    }

    /// 父层级 = namespace 的完整层级链，再加上 namespace 自身。
    fn parent_hierarchies(&self) -> Vec<&str> {
        let mut names = self.namespace.parent_hierarchies();
        names.push(self.namespace.basename());
        names
    }

    fn get_metrics_registry(&self) -> &MetricsRegistry {
        &self.metrics_registry
    }
}
