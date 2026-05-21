// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 基础 Provider trait，使通用工具函数可接受任意持有运行时引用的类型。

use crate::distributed::DistributedRuntime;
use crate::runtime::Runtime;

/// 提供本地 `Runtime` 访问的 trait。
///
/// 由 Namespace / ServiceGroup / PortName 等实现。
pub trait RuntimeProvider {
    fn rt(&self) -> &Runtime;
}

/// 提供 `DistributedRuntime` 访问的 trait。
///
/// 由 Namespace / ServiceGroup / PortName 等实现。
pub trait DistributedRuntimeProvider: RuntimeProvider {
    fn drt(&self) -> &DistributedRuntime;
}
