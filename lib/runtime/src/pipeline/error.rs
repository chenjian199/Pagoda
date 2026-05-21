// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 管道子系统内部错误类型。
//!
//! `PipelineError` 是 pipeline 子系统内部使用的错误枚举，覆盖传输故障、编解码错误、
//! 超时、取消和引擎错误等场景。它不跨网络传输，不可序列化。
//!
//! 与 [`crate::error::PagodaError`] 的区别：
//!
//! - **`PipelineError`**：管道内部，`thiserror` 派生，含 `source` 错误链，不可序列化
//! - **`PagodaError`**：框架级，`serde` 可序列化，跨网络传输，供路由层策略决策
//!
//! 在 egress 路径上，`PipelineError` 可通过 `From<PipelineError> for PagodaError`
//! 转换为 `PagodaError` 后跨网络传递给调用方。

use std::time::Duration;

use crate::error::{ErrorType, PagodaError};

/// 管道子系统内部错误。
///
/// 覆盖 transport / encoding / timeout / cancel / engine / internal 六类场景，
/// 供 ingress/egress 路径统一处理和指标上报。
#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    /// Network or transport-level failure.
    #[error("transport error: {message}")]
    Transport {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Encoding/decoding failure (codec layer).
    #[error("encoding error: {message}")]
    Encoding {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Request timed out.
    #[error("timeout after {elapsed:?} (limit: {limit:?})")]
    Timeout { elapsed: Duration, limit: Duration },

    /// Request was cancelled (e.g. client disconnected).
    #[error("request cancelled: {reason}")]
    Cancelled { reason: String },

    /// The downstream engine returned an error.
    #[error("engine error: {0}")]
    Engine(#[from] crate::engine::EngineError),

    /// Catch-all for internal errors.
    #[error("internal pipeline error: {0}")]
    Internal(#[from] anyhow::Error),
}

impl PipelineError {
    pub fn transport(msg: impl Into<String>) -> Self {
        Self::Transport {
            message: msg.into(),
            source: None,
        }
    }

    pub fn encoding(msg: impl Into<String>) -> Self {
        Self::Encoding {
            message: msg.into(),
            source: None,
        }
    }

    pub fn cancelled(reason: impl Into<String>) -> Self {
        Self::Cancelled {
            reason: reason.into(),
        }
    }
}

/// 从 `PipelineError` 到 `PagodaError` 的转换，用于跨网络边界传播。
impl From<PipelineError> for PagodaError {
    fn from(err: PipelineError) -> Self {
        let error_type = match &err {
            PipelineError::Transport { .. } => ErrorType::CannotConnect,
            PipelineError::Encoding { .. } => ErrorType::Unknown,
            PipelineError::Timeout { .. } => ErrorType::ConnectionTimeout,
            PipelineError::Cancelled { .. } => ErrorType::Cancelled,
            PipelineError::Engine(_) => ErrorType::Backend(crate::error::BackendError::EngineError),
            PipelineError::Internal(_) => ErrorType::Unknown,
        };
        PagodaError::new(error_type, err.to_string())
    }
}
