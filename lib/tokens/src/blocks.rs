// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 块标识类型。
//!
//! ## 设计意图
//! 为 token 序列中的「块」提供一个统一的身份标识。块在生命周期里存在两种状态：
//! 尚未填满的「局部块」与已经定型的「完整块」，二者需要可哈希、可比较、可序列化，
//! 以便用作各类映射的键。
//!
//! ## 外部契约
//! - `GlobalHash`：完整块的 64 位标识别名。
//! - `UniqueBlock` 枚举：`PartialBlock(Uuid)` 与 `FullBlock(GlobalHash)` 两个变体，
//!   derive `Debug/Clone/Hash/Eq/PartialEq/Serialize/Deserialize`。
//! - `Default`：返回携带随机 UUID 的 `PartialBlock`。
//!
//! ## 实现要点
//! 局部块以随机 UUID 区分，保证默认值之间互不相等；完整块以内容哈希区分。

use serde::{Deserialize, Serialize};
use uuid::Uuid;

// === SECTION: 类型别名 ===

/// 完整块的全局哈希标识。
pub type GlobalHash = u64;

// === SECTION: 块身份枚举 ===

/// 正在构建或已经定型的块的唯一标识。
#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub enum UniqueBlock {
    /// 以 UUID 标识的局部（未填满）块。
    PartialBlock(Uuid),
    /// 以内容哈希标识的完整块。
    FullBlock(GlobalHash),
}

// === SECTION: 默认值构造 ===

impl Default for UniqueBlock {
    /// ## 设计意图
    /// 默认值代表「一个全新的、尚无内容的局部块」，因此每次都分配一枚随机 UUID，
    /// 使两个默认实例天然不相等。
    fn default() -> Self {
        Self::PartialBlock(Uuid::new_v4())
    }
}
