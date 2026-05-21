// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! `MaybeError<T, E>`：可能包含错误的流 item。
//!
//! 与 `Result<T, E>` 不同，`MaybeError` 不中断流，允许后续 item 继续发送。

use serde::{Deserialize, Serialize};

/// 流中的可选错误 item。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MaybeError<T, E> {
    /// 正常数据。
    Value(T),
    /// 该 item 出错，但流可继续。
    Error(E),
}

impl<T, E> MaybeError<T, E> {
    pub fn is_value(&self) -> bool {
        matches!(self, Self::Value(_))
    }

    pub fn is_error(&self) -> bool {
        matches!(self, Self::Error(_))
    }

    pub fn into_value(self) -> Option<T> {
        match self {
            Self::Value(v) => Some(v),
            Self::Error(_) => None,
        }
    }

    pub fn into_error(self) -> Option<E> {
        match self {
            Self::Value(_) => None,
            Self::Error(e) => Some(e),
        }
    }
}
