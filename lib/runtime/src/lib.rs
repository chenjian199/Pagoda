// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! `pagoda-runtime` — Pagoda 分布式推理框架的核心基础设施层。
//!
//! 提供：
//! - 本地与分布式异步执行上下文（Tokio Runtime + Rayon ComputePool）
//! - 跨节点服务注册与发现（Kubernetes 原生资源）
//! - 统一请求平面传输抽象（TCP / NATS / HTTP）
//! - 可组合的流式引擎框架（AsyncEngine / Pipeline）
//! - 可观测性接入点（Prometheus 指标树、W3C 链路追踪、健康检查）
//! - 进程生命周期管理（信号处理、三阶段优雅关闭）

// ── 进程入口层 ──
pub mod worker;
pub mod runtime;
pub mod config;

// ── 分布式运行时层 ──
pub mod distributed;
pub mod traits;

// ── 服务模型层（新三段式）──
pub mod servicegroup;

// ── 引擎与管道层 ──
pub mod engine;
pub mod engine_routes;
pub mod pipeline;

// ── 服务发现层 ──
pub mod discovery;

// ── 传输协议层 ──
pub mod transports;

// ── CPU 计算隔离层 ──
pub mod compute;

// ── 可观测性层 ──
pub mod logging;
pub mod metrics;
pub mod system_status_server;
pub mod health_check;
pub mod system_health;

// ── 协议辅助类型 ──
pub mod protocols;

// ── 通用工具与错误 ──
pub mod error;
pub mod service;
pub mod local_endpoint_registry; // 保留：兼容旧引用，内部重定向至 local_portname_registry
pub mod local_portname_registry;
pub mod runnable;
pub mod slug;
pub mod timeline;
pub mod utils;

// ── 公开 prelude ──
pub mod prelude;

// ── 顶层 re-export ──
pub use worker::Worker;
pub use runtime::Runtime;
pub use distributed::{DistributedRuntime, DistributedConfig, RequestPlaneMode, DiscoveryBackend};
pub use config::RuntimeConfig;
pub use error::PagodaError;
pub use pipeline::error::PipelineError;
pub use metrics::MetricsRegistry;
pub use system_health::{SystemHealth, HealthStatus};
pub use engine::{AsyncEngine, AsyncEngineContext, ResponseStream};
pub use servicegroup::{Namespace, ServiceGroup, PortName, PortNameId, Instance, TransportType};
pub use local_portname_registry::{LocalPortNameRegistry, LocalAsyncEngine};
pub use servicegroup::client::list_all_instances;

pub use tokio_util::sync::CancellationToken;
