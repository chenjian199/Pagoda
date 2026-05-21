// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 进程内 PortName 直连注册表：同进程前后端绕过 TCP/NATS 直接调用引擎。
//!
//! 对比网络路径（序列化 → socket → 反序列化，约 100-200μs），
//! 直接调用 Rust 函数延迟约 1μs，差距两个数量级。

use std::sync::Arc;

use dashmap::DashMap;

use crate::engine::{AsyncEngine, ManyOut, SingleIn};
use crate::protocols::annotated::Annotated;
use crate::servicegroup::PortNameId;

/// 本地直调引擎类型：与网络路径保持相同接口，使路由层对两条路径完全透明。
///
/// - `SingleIn<serde_json::Value>`：单一 JSON 请求
/// - `ManyOut<Annotated<serde_json::Value>>`：流式 JSON 响应
/// - `anyhow::Error`：引擎层面的错误类型
pub type LocalAsyncEngine = Arc<
    dyn AsyncEngine<
            SingleIn<serde_json::Value>,
            ManyOut<Annotated<serde_json::Value>>,
            anyhow::Error,
        > + Send
        + Sync,
>;

/// 进程内 PortName 注册表（同进程直连优化）。
///
/// 以 `PortNameId`（三段式全限定名）为键，支持同进程多 namespace/servicegroup 部署场景。
/// 作为 `DistributedRuntime` 的字段随 DRT 廉价克隆，所有克隆共享同一实例。
#[derive(Clone, Default)]
pub struct LocalPortNameRegistry {
    engines: Arc<DashMap<PortNameId, LocalAsyncEngine>>,
}

impl LocalPortNameRegistry {
    /// 创建空注册表。
    pub fn new() -> Self {
        Self {
            engines: Arc::new(DashMap::new()),
        }
    }

    /// 注册本地引擎。
    ///
    /// `id` 为三段式全限定 PortName ID，若已存在则覆盖（允许模型热重载）。
    pub fn register(&self, id: PortNameId, engine: LocalAsyncEngine) {
        tracing::debug!("Registering local portname: {}", id);
        self.engines.insert(id, engine);
    }

    /// 查找本地引擎，返回 `Arc` 克隆。
    ///
    /// 返回克隆后立即释放 DashMap 分片读锁，调用方可安全地 `await`。
    /// 未注册时返回 `None`，路由层据此 fallback 到网络路径（TCP/NATS）。
    pub fn get(&self, id: &PortNameId) -> Option<LocalAsyncEngine> {
        self.engines.get(id).map(|e| e.clone())
    }

    /// 注销本地引擎。
    pub fn unregister(&self, id: &PortNameId) -> bool {
        self.engines.remove(id).is_some()
    }

    /// 检查端点是否已注册。
    pub fn contains(&self, id: &PortNameId) -> bool {
        self.engines.contains_key(id)
    }

    /// 返回已注册端点数量。
    pub fn len(&self) -> usize {
        self.engines.len()
    }

    /// 注册表是否为空。
    pub fn is_empty(&self) -> bool {
        self.engines.is_empty()
    }
}

impl std::fmt::Debug for LocalPortNameRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalPortNameRegistry")
            .field("count", &self.engines.len())
            .finish()
    }
}
