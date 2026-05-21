// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 框架级统一错误类型：可分类、可序列化、可链式追踪。
//!
//! `PagodaError` 是跨网络边界传输的顶层错误类型，用于 Worker ↔ Router 之间的
//! 错误传播和路由层策略决策。它与 [`crate::pipeline::error::PipelineError`] 是
//! 两个独立的错误类型：
//!
//! - **`PagodaError`**：框架级，跨网络序列化传输，供路由层做重试/熔断决策
//! - **`PipelineError`**：管道子系统内部，不跨网络，供 ingress/egress 路径处理

use serde::{Deserialize, Serialize};

/// 错误分类枚举，供路由层做策略决策。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorType {
    Unknown,
    InvalidArgument,
    CannotConnect,
    Disconnected,
    ConnectionTimeout,
    Cancelled,
    Backend(BackendError),
}

/// 后端引擎错误子分类。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackendError {
    EngineError,
    ResourceExhausted,
    ModelNotFound,
    InternalError,
}

/// Pagoda 框架统一错误。
///
/// 可序列化/反序列化，支持跨网络传输；附带错误链用于分布式追踪。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PagodaError {
    pub error_type: ErrorType,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caused_by: Option<Box<PagodaError>>,
}

impl PagodaError {
    pub fn new(error_type: ErrorType, message: impl Into<String>) -> Self {
        Self {
            error_type,
            message: message.into(),
            caused_by: None,
        }
    }

    pub fn with_cause(mut self, cause: PagodaError) -> Self {
        self.caused_by = Some(Box::new(cause));
        self
    }

    pub fn unknown(message: impl Into<String>) -> Self {
        Self::new(ErrorType::Unknown, message)
    }

    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(ErrorType::InvalidArgument, message)
    }

    pub fn cannot_connect(message: impl Into<String>) -> Self {
        Self::new(ErrorType::CannotConnect, message)
    }

    pub fn cancelled() -> Self {
        Self::new(ErrorType::Cancelled, "Request cancelled")
    }

    pub fn is_retryable(&self) -> bool {
        matches!(
            self.error_type,
            ErrorType::CannotConnect | ErrorType::Disconnected | ErrorType::ConnectionTimeout
        )
    }

    pub fn is_cancelled(&self) -> bool {
        self.error_type == ErrorType::Cancelled
    }
}

impl std::fmt::Display for PagodaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{:?}] {}", self.error_type, self.message)?;
        if let Some(cause) = &self.caused_by {
            write!(f, "\n  caused by: {cause}")?;
        }
        Ok(())
    }
}

impl std::error::Error for PagodaError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.caused_by.as_ref().map(|e| e.as_ref() as &dyn std::error::Error)
    }
}

impl From<anyhow::Error> for PagodaError {
    fn from(err: anyhow::Error) -> Self {
        Self::unknown(err.to_string())
    }
}
