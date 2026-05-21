// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 任务追踪池：防止后台任务泄漏。

use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use tokio::task::JoinHandle;

/// 后台任务追踪器。
///
/// 管理所有后台任务句柄，`join_all()` 等待所有任务退出，
/// `abort_all()` 强制取消。防止进程退出时未等待任务导致资源未释放。
pub struct TaskTracker {
    tasks: DashMap<u64, JoinHandle<()>>,
    next_id: AtomicU64,
}

impl TaskTracker {
    pub fn new() -> Self {
        Self {
            tasks: DashMap::new(),
            next_id: AtomicU64::new(0),
        }
    }

    /// 注册一个后台任务。
    pub fn track(&self, handle: JoinHandle<()>) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.tasks.insert(id, handle);
        id
    }

    /// 移除已完成的任务。
    pub fn remove(&self, id: u64) {
        self.tasks.remove(&id);
    }

    /// 等待所有任务退出。
    pub async fn join_all(&self) {
        let handles: Vec<_> = self
            .tasks
            .iter()
            .map(|entry| entry.key().to_owned())
            .collect();

        for id in handles {
            if let Some((_, handle)) = self.tasks.remove(&id) {
                let _ = handle.await;
            }
        }
    }

    /// 强制取消所有任务。
    pub fn abort_all(&self) {
        for entry in self.tasks.iter() {
            entry.value().abort();
        }
        self.tasks.clear();
    }

    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }
}

impl Default for TaskTracker {
    fn default() -> Self {
        Self::new()
    }
}
