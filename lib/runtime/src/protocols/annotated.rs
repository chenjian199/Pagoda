// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! `Annotated<T>`：带注解的流式响应 token 包装。
//!
//! 用于在 `ManyOut<Annotated<T>>` 流中携带 SSE 事件类型、序列 ID 等元数据，
//! 同时标记流结束。

use serde::{Deserialize, Serialize};

/// 带注解的流式响应 token。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Annotated<T> {
    /// 实际数据（None 表示流已结束）。
    pub data: Option<T>,
    /// SSE 事件类型（如 `"done"`）。
    pub event: Option<String>,
    /// 序列 ID。
    pub id: Option<String>,
    /// 注释字段。
    pub comment: Option<String>,
}

impl<T> Annotated<T> {
    /// 从数据构造普通 token。
    pub fn from_data(data: T) -> Self {
        Self {
            data: Some(data),
            event: None,
            id: None,
            comment: None,
        }
    }

    /// 构造流结束标记。
    pub fn new_done() -> Self {
        Self {
            data: None,
            event: Some("done".to_string()),
            id: None,
            comment: None,
        }
    }

    /// 检查是否为流结束标记。
    pub fn is_final(&self) -> bool {
        self.data.is_none() && self.event.as_deref() == Some("done")
    }
}
