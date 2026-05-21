// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 运行时配置：figment 多层合并，环境变量 > TOML > 默认值。

pub mod environment_names;

use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::system_health::HealthStatus;

/// 运行时全量配置。
#[derive(Serialize, Deserialize, Validate, Debug, Clone)]
pub struct RuntimeConfig {
    // === Tokio 线程配置 ===
    pub num_worker_threads: Option<usize>,
    pub max_blocking_threads: usize,

    // === 系统状态服务器 ===
    pub system_host: String,
    pub system_port: i16,
    pub starting_health_status: HealthStatus,
    pub system_health_path: String,
    pub system_live_path: String,

    // === 计算线程池 ===
    pub compute_threads: Option<usize>,

    // === 健康检查 ===
    pub health_check_enabled: bool,
    pub canary_wait_time_secs: u64,
    pub health_check_request_timeout_secs: u64,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            num_worker_threads: None,
            max_blocking_threads: num_cpus(),
            system_host: "0.0.0.0".to_string(),
            system_port: -1,
            starting_health_status: HealthStatus::NotReady,
            system_health_path: "/health".to_string(),
            system_live_path: "/live".to_string(),
            compute_threads: None,
            health_check_enabled: false,
            canary_wait_time_secs: 30,
            health_check_request_timeout_secs: 10,
        }
    }
}

impl RuntimeConfig {
    /// 从环境变量和配置文件加载配置。
    ///
    /// 优先级：环境变量 > TOML 配置文件 > 默认值。
    pub fn from_settings() -> anyhow::Result<Self> {
        use figment::providers::{Env, Format, Serialized, Toml};
        use figment::Figment;

        let config: Self = Figment::new()
            .merge(Serialized::defaults(Self::default()))
            .merge(Toml::file("/opt/pagoda/defaults/runtime.toml"))
            .merge(Toml::file("/opt/pagoda/etc/runtime.toml"))
            .merge(Env::prefixed("PGD_RUNTIME_").split("_"))
            .extract()?;

        config.validate()?;
        Ok(config)
    }

    /// 根据配置创建 Tokio Runtime。
    pub fn create_runtime(&self) -> anyhow::Result<tokio::runtime::Runtime> {
        let mut builder = tokio::runtime::Builder::new_multi_thread();

        if let Some(threads) = self.num_worker_threads {
            builder.worker_threads(threads);
        }

        builder
            .max_blocking_threads(self.max_blocking_threads)
            .enable_all()
            .thread_name("pagoda-worker")
            .build()
            .map_err(Into::into)
    }
}

/// 工作进程级配置。
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WorkerConfig {
    pub graceful_shutdown_timeout_secs: u64,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        let default_secs: u64 = if cfg!(debug_assertions) { 5 } else { 30 };
        let graceful_shutdown_timeout_secs = std::env::var(
            crate::config::environment_names::PGD_WORKER_GRACEFUL_SHUTDOWN_TIMEOUT,
        )
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default_secs);
        Self { graceful_shutdown_timeout_secs }
    }
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}
