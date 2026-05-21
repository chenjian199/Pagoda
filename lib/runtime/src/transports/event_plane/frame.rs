// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 事件帧定义。
//!
//! `Frame` 是事件平面的最小传输单元，包含 subject 和序列化后的 payload。

/// 事件帧 — 事件平面传输的基本单元。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// 事件 subject（路由键）
    pub subject: String,
    /// 序列化后的载荷（msgpack 编码）
    pub payload: Vec<u8>,
}

impl Frame {
    /// 创建新的事件帧。
    pub fn new(subject: impl Into<String>, payload: Vec<u8>) -> Self {
        Self {
            subject: subject.into(),
            payload,
        }
    }

    /// 创建空载荷帧。
    pub fn empty(subject: impl Into<String>) -> Self {
        Self {
            subject: subject.into(),
            payload: Vec::new(),
        }
    }

    /// 帧载荷大小（字节）。
    pub fn payload_len(&self) -> usize {
        self.payload.len()
    }

    /// 帧总大小（subject + payload 字节数）。
    pub fn total_size(&self) -> usize {
        self.subject.len() + self.payload.len()
    }
}
