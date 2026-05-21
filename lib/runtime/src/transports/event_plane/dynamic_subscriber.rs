// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 动态订阅者 — 运行时动态管理事件 subject 订阅。

use std::collections::HashSet;
use super::frame::Frame;
use super::transport::EventTransportRx;

/// 动态订阅者，支持运行时增减 subject 订阅。
pub struct DynamicSubscriber {
    transport: Box<dyn EventTransportRx>,
    active_subjects: HashSet<String>,
}

impl DynamicSubscriber {
    /// 创建动态订阅者。
    pub fn new(transport: Box<dyn EventTransportRx>) -> Self {
        Self {
            transport,
            active_subjects: HashSet::new(),
        }
    }

    /// 添加 subject 订阅。
    pub async fn add_subject(&mut self, subject: &str) -> Result<(), crate::error::PagodaError> {
        if self.active_subjects.insert(subject.to_string()) {
            self.transport.subscribe(subject).await?;
        }
        Ok(())
    }

    /// 移除 subject 订阅。
    pub async fn remove_subject(
        &mut self,
        subject: &str,
    ) -> Result<(), crate::error::PagodaError> {
        if self.active_subjects.remove(subject) {
            self.transport.unsubscribe(subject).await?;
        }
        Ok(())
    }

    /// 批量设置 subjects（替换当前所有订阅）。
    pub async fn set_subjects(
        &mut self,
        subjects: &[String],
    ) -> Result<(), crate::error::PagodaError> {
        let desired: std::collections::HashSet<String> = subjects.iter().cloned().collect();
        // 取消不再需要的 subjects
        let to_remove: Vec<String> = self.active_subjects.difference(&desired).cloned().collect();
        for s in &to_remove {
            self.transport.unsubscribe(s).await?;
        }
        // 添加新增的 subjects
        let to_add: Vec<String> = desired.difference(&self.active_subjects).cloned().collect();
        for s in &to_add {
            self.transport.subscribe(s).await?;
        }
        self.active_subjects = desired;
        Ok(())
    }

    /// 接收下一帧事件。
    pub async fn recv(&mut self) -> Option<Result<Frame, crate::error::PagodaError>> {
        self.transport.recv().await
    }

    /// 获取当前活跃的 subject 集合。
    pub fn active_subjects(&self) -> &HashSet<String> {
        &self.active_subjects
    }

    /// 当前订阅数量。
    pub fn subscription_count(&self) -> usize {
        self.active_subjects.len()
    }
}
