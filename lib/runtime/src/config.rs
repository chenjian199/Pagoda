// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `config` —— 运行时配置加载与布尔语义工具
//!
//! ## 设计意图
//!
//! 本模块是 Pagoda runtime 的"配置加载层"，承担三类职责：
//!
//! 1. **结构化配置类型** —— [`WorkerConfig`] / [`RuntimeConfig`] 描述
//!    可被运行时直接消费的字段集合，并实现 `Serialize` / `Deserialize`
//!    供 Figment 解析；
//! 2. **多源加载策略** —— [`RuntimeConfig::figment`] 把"内置默认 →
//!    打包 toml → 本地 toml → 五组前缀环境变量"按优先级层叠起来，并
//!    在 [`RuntimeConfig::from_settings`] 出口处做 `validate`；
//! 3. **布尔解析 helpers** —— 上层日志 / 计费 / 健康检查等模块普遍依
//!    赖"读环境变量并把字符串解释为 bool"的能力，集中放在本文件以
//!    便统一真值/假值集合。
//!
//! ## 外部契约
//!
//! 以下符号均保持原有签名 / 字段 / derive 不变：
//!
//! - `pub struct WorkerConfig { graceful_shutdown_timeout: u64 }`；
//! - `pub enum HealthStatus { Ready, NotReady }`（serde `lowercase`）；
//! - `pub struct RuntimeConfig { ... }`（derive_builder + validator）
//!   及方法 `builder` / `figment(pub(crate))` / `from_settings` /
//!   `system_server_enabled` / `single_threaded` / `create_runtime(pub(crate))`；
//! - `impl RuntimeConfigBuilder { pub fn build }`；
//! - 公开函数 `is_truthy` / `is_falsey` / `parse_bool` /
//!   `env_is_truthy` / `env_is_falsey` / `jsonl_logging_enabled` /
//!   `disable_ansi_logging` / `use_local_timezone` /
//!   `span_events_enabled`；
//! - 公开常量 `DEFAULT_CANARY_WAIT_TIME_SECS` /
//!   `DEFAULT_HEALTH_CHECK_REQUEST_TIMEOUT_SECS`；
//! - `pub mod environment_names`。
//!
//! ## 实现要点
//!
//! - 五个环境变量前缀的"读 + 非空过滤 + key 映射"模式集中到
//!   [`prefixed_env_provider`]；
//! - `single_threaded` 与 `Default` 共享 [`base_runtime_defaults`]
//!   骨架，避免两份字段表偏移；
//! - 真值 / 假值集合分别用 [`TRUTHY_VALUES`] / [`FALSEY_VALUES`] 声明。

use anyhow::Result;
use derive_builder::Builder;
use figment::{
    Figment,
    providers::{Env, Format, Serialized, Toml},
};
use serde::{Deserialize, Serialize};
use std::fmt;
use validator::Validate;

pub mod environment_names;

// ============================================================================
// 常量
// ============================================================================

const DEFAULT_SYSTEM_HOST: &str = "0.0.0.0";
const DEFAULT_SYSTEM_PORT: i16 = -1;
const DEFAULT_SYSTEM_HEALTH_PATH: &str = "/health";
const DEFAULT_SYSTEM_LIVE_PATH: &str = "/live";

/// canary 健康探测的默认等候时间（秒）。
pub const DEFAULT_CANARY_WAIT_TIME_SECS: u64 = 10;

/// 单次健康检查请求的默认超时（秒）。
pub const DEFAULT_HEALTH_CHECK_REQUEST_TIMEOUT_SECS: u64 = 3;

const DEFAULT_COMPUTE_STACK_SIZE: usize = 2 * 1024 * 1024;
const DEFAULT_COMPUTE_THREAD_PREFIX: &str = "compute";

const DEBUG_WORKER_SHUTDOWN_SECS: u64 = 1;
const RELEASE_WORKER_SHUTDOWN_SECS: u64 = 30;

const TRUTHY_VALUES: &[&str] = &["1", "true", "on", "yes"];
const FALSEY_VALUES: &[&str] = &["0", "false", "off", "no"];

// ============================================================================
// WorkerConfig
// ============================================================================

/// worker 进程级别配置（目前仅承载优雅关闭超时）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    /// 进程收到关闭信号后给服务的清理时间（秒）。
    pub graceful_shutdown_timeout: u64,
}

impl WorkerConfig {
    /// 从默认值 + `PGD_WORKER_` 前缀环境变量加载。
    ///
    /// 配置非法时直接 panic——本函数仅在进程启动期被调用。
    pub fn from_settings() -> Self {
        Figment::new()
            .merge(Serialized::defaults(Self::default()))
            .merge(Env::prefixed("PGD_WORKER_"))
            .extract()
            .unwrap()
    }
}

impl Default for WorkerConfig {
    fn default() -> Self {
        let graceful_shutdown_timeout = if cfg!(debug_assertions) {
            DEBUG_WORKER_SHUTDOWN_SECS
        } else {
            RELEASE_WORKER_SHUTDOWN_SECS
        };
        Self {
            graceful_shutdown_timeout,
        }
    }
}

// ============================================================================
// HealthStatus
// ============================================================================

/// 健康状态的两态枚举。serde 上 `rename_all = "lowercase"` 决定它在
/// toml / 环境变量里写作 `ready` / `notready`。
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    Ready,
    NotReady,
}

// ============================================================================
// RuntimeConfig
// ============================================================================

/// Tokio runtime / system 服务 / compute 池 / 健康检查的聚合配置。
#[derive(Serialize, Deserialize, Validate, Debug, Builder, Clone)]
#[builder(build_fn(private, name = "build_internal"), derive(Debug, Serialize))]
pub struct RuntimeConfig {
    /// async worker 线程数；`Some(1)` 表示单线程模式。
    #[validate(range(min = 1))]
    #[builder_field_attr(serde(skip_serializing_if = "Option::is_none"))]
    pub num_worker_threads: Option<usize>,

    /// blocking 线程池上限，默认 512。
    #[validate(range(min = 1))]
    #[builder(default = "512")]
    #[builder_field_attr(serde(skip_serializing_if = "Option::is_none"))]
    pub max_blocking_threads: usize,

    /// system 服务监听 host。
    #[builder(default = "DEFAULT_SYSTEM_HOST.to_string()")]
    #[builder_field_attr(serde(skip_serializing_if = "Option::is_none"))]
    pub system_host: String,

    /// system 服务端口：`-1` 禁用、`0` OS 随机、正数绑定该端口。
    #[builder(default = "DEFAULT_SYSTEM_PORT")]
    #[builder_field_attr(serde(skip_serializing_if = "Option::is_none"))]
    pub system_port: i16,

    /// 已废弃；保留以兼容旧 toml。新代码使用 `system_port`。
    #[deprecated(
        note = "Use system_port instead. Set PGD_SYSTEM_PORT to enable the system metrics server."
    )]
    #[builder(default = "false")]
    #[builder_field_attr(serde(skip_serializing_if = "Option::is_none"))]
    pub system_enabled: bool,

    /// 进程启动时初始健康状态。
    #[builder(default = "HealthStatus::NotReady")]
    #[builder_field_attr(serde(skip_serializing_if = "Option::is_none"))]
    pub starting_health_status: HealthStatus,

    /// 已废弃；旧版本用来声明"哪些 portname 参与汇总健康判定"。
    #[builder(default = "vec![]")]
    #[builder_field_attr(serde(skip_serializing_if = "Option::is_none"))]
    pub use_portname_health_status: Vec<String>,

    /// `/health` 路径。
    #[builder(default = "DEFAULT_SYSTEM_HEALTH_PATH.to_string()")]
    #[builder_field_attr(serde(skip_serializing_if = "Option::is_none"))]
    pub system_health_path: String,

    /// `/live` 路径。
    #[builder(default = "DEFAULT_SYSTEM_LIVE_PATH.to_string()")]
    #[builder_field_attr(serde(skip_serializing_if = "Option::is_none"))]
    pub system_live_path: String,

    /// compute 池线程数；`None` 表示自动推断。
    #[builder(default = "None")]
    #[builder_field_attr(serde(skip_serializing_if = "Option::is_none"))]
    pub compute_threads: Option<usize>,

    /// compute 池线程栈大小（字节）。
    #[builder(default = "Some(2 * 1024 * 1024)")]
    #[builder_field_attr(serde(skip_serializing_if = "Option::is_none"))]
    pub compute_stack_size: Option<usize>,

    /// compute 池线程前缀。
    #[builder(default = "\"compute\".to_string()")]
    #[builder_field_attr(serde(skip_serializing_if = "Option::is_none"))]
    pub compute_thread_prefix: String,

    /// 是否启用主动健康检查 payload。
    #[builder(default = "false")]
    #[builder_field_attr(serde(skip_serializing_if = "Option::is_none"))]
    pub health_check_enabled: bool,

    /// canary 等候时间（秒）。
    #[builder(default = "DEFAULT_CANARY_WAIT_TIME_SECS")]
    #[builder_field_attr(serde(skip_serializing_if = "Option::is_none"))]
    pub canary_wait_time_secs: u64,

    /// 单次健康检查请求超时（秒）。
    #[builder(default = "DEFAULT_HEALTH_CHECK_REQUEST_TIMEOUT_SECS")]
    #[builder_field_attr(serde(skip_serializing_if = "Option::is_none"))]
    pub health_check_request_timeout_secs: u64,
}

impl fmt::Display for RuntimeConfig {
    /// 字段顺序与历史日志格式保持一致——上层日志检索可能依赖此格式。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.num_worker_threads {
            Some(n) => write!(f, "num_worker_threads={n}, ")?,
            None => f.write_str("num_worker_threads=default (num_cores), ")?,
        }
        write!(f, "max_blocking_threads={}, ", self.max_blocking_threads)?;
        write!(f, "system_host={}, ", self.system_host)?;
        write!(f, "system_port={}, ", self.system_port)?;
        write!(
            f,
            "use_portname_health_status={:?}",
            self.use_portname_health_status
        )?;
        write!(f, "starting_health_status={:?}", self.starting_health_status)?;
        write!(f, ", system_health_path={}", self.system_health_path)?;
        write!(f, ", system_live_path={}", self.system_live_path)?;
        write!(f, ", health_check_enabled={}", self.health_check_enabled)?;
        write!(f, ", canary_wait_time_secs={}", self.canary_wait_time_secs)?;
        write!(
            f,
            ", health_check_request_timeout_secs={}",
            self.health_check_request_timeout_secs
        )
    }
}

impl RuntimeConfig {
    /// 返回一个默认 builder。
    pub fn builder() -> RuntimeConfigBuilder {
        RuntimeConfigBuilder::default()
    }

    /// 构造完整的多源 `Figment`。
    ///
    /// 优先级（高 → 低）：环境变量 > 本地 toml > 打包 toml > 默认值。
    pub(crate) fn figment() -> Figment {
        Figment::new()
            .merge(Serialized::defaults(RuntimeConfig::default()))
            .merge(Toml::file("/opt/pagoda/defaults/runtime.toml"))
            .merge(Toml::file("/opt/pagoda/etc/runtime.toml"))
            .merge(prefixed_env_provider("PGD_RUNTIME_", identity_key))
            .merge(prefixed_env_provider("PGD_SYSTEM_", map_system_key))
            .merge(prefixed_env_provider("PGD_COMPUTE_", map_compute_key))
            .merge(prefixed_env_provider(
                "PGD_HEALTH_CHECK_",
                map_health_check_key,
            ))
            .merge(prefixed_env_provider("PGD_CANARY_", map_canary_key))
    }

    /// 运行时配置主入口：`figment + validate + deprecation warnings`。
    pub fn from_settings() -> Result<RuntimeConfig> {
        warn_if_deprecated_env_set();
        let config: RuntimeConfig = Self::figment().extract()?;
        config.validate()?;
        Ok(config)
    }

    /// `port >= 0` 即视为启用（`0` 表示由 OS 随机分配端口）。
    pub fn system_server_enabled(&self) -> bool {
        self.system_port >= 0
    }

    /// 单线程模式——常用于测试 / 简单嵌入场景。
    pub fn single_threaded() -> Self {
        let mut cfg = base_runtime_defaults();
        cfg.num_worker_threads = Some(1);
        cfg.max_blocking_threads = 1;
        cfg.compute_threads = Some(1);
        cfg
    }

    /// 按当前配置实例化一个 Tokio multi-thread runtime。
    pub(crate) fn create_runtime(&self) -> std::io::Result<tokio::runtime::Runtime> {
        let worker_threads = self
            .num_worker_threads
            .unwrap_or_else(|| std::thread::available_parallelism().unwrap().get());

        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder
            .worker_threads(worker_threads)
            .max_blocking_threads(self.max_blocking_threads)
            .enable_all();

        if env_is_truthy(environment_names::runtime::PGD_ENABLE_POLL_HISTOGRAM) {
            tracing::info!(
                "Tokio poll-time histogram enabled (PGD_ENABLE_POLL_HISTOGRAM); \
                 expect ~2× Instant::now() overhead per task poll"
            );
            builder.enable_metrics_poll_time_histogram();
        }

        builder.build()
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        let num_cores = std::thread::available_parallelism().unwrap().get();
        let mut cfg = base_runtime_defaults();
        cfg.num_worker_threads = Some(num_cores);
        cfg.max_blocking_threads = num_cores;
        cfg
    }
}

impl RuntimeConfigBuilder {
    /// 构建并校验。任何字段不满足 `validator` 约束都会以 `Err` 返回。
    pub fn build(&self) -> Result<RuntimeConfig> {
        let built = self.build_internal()?;
        built.validate()?;
        Ok(built)
    }
}

// ============================================================================
// 私有 helper：Default / single_threaded 共用骨架
// ============================================================================

/// `RuntimeConfig` 字段的"中性骨架"——只填那些与线程数无关的字段。
fn base_runtime_defaults() -> RuntimeConfig {
    #[allow(deprecated)]
    RuntimeConfig {
        num_worker_threads: None,
        max_blocking_threads: 0, // 由调用方覆盖
        system_host: DEFAULT_SYSTEM_HOST.to_string(),
        system_port: DEFAULT_SYSTEM_PORT,
        system_enabled: false,
        starting_health_status: HealthStatus::NotReady,
        use_portname_health_status: Vec::new(),
        system_health_path: DEFAULT_SYSTEM_HEALTH_PATH.to_string(),
        system_live_path: DEFAULT_SYSTEM_LIVE_PATH.to_string(),
        compute_threads: None,
        compute_stack_size: Some(DEFAULT_COMPUTE_STACK_SIZE),
        compute_thread_prefix: DEFAULT_COMPUTE_THREAD_PREFIX.to_string(),
        health_check_enabled: false,
        canary_wait_time_secs: DEFAULT_CANARY_WAIT_TIME_SECS,
        health_check_request_timeout_secs: DEFAULT_HEALTH_CHECK_REQUEST_TIMEOUT_SECS,
    }
}

// ============================================================================
// 私有 helper：环境变量 figment 提供者
// ============================================================================

/// 通用前缀型 env 提供者：读取以 `prefix` 开头的所有变量，丢弃空值，
/// 再用 `map` 把"前缀剥掉之后的 key"转写成 struct 字段名。
fn prefixed_env_provider<F>(prefix: &'static str, map: F) -> Env
where
    F: Fn(&str) -> String + Clone + Send + Sync + 'static,
{
    Env::prefixed(prefix).filter_map(move |k| {
        let full_key = format!("{prefix}{}", k.as_str());
        match std::env::var(&full_key) {
            Ok(v) if !v.is_empty() => Some(map(k.as_str()).into()),
            _ => None,
        }
    })
}

fn identity_key(key: &str) -> String {
    key.to_string()
}

fn map_system_key(key: &str) -> String {
    match key {
        "HOST" => "system_host",
        "PORT" => "system_port",
        "ENABLED" => "system_enabled",
        "USE_ENDPOINT_HEALTH_STATUS" => "use_portname_health_status",
        "STARTING_HEALTH_STATUS" => "starting_health_status",
        "HEALTH_PATH" => "system_health_path",
        "LIVE_PATH" => "system_live_path",
        other => other,
    }
    .to_string()
}

fn map_compute_key(key: &str) -> String {
    match key {
        "THREADS" => "compute_threads",
        "STACK_SIZE" => "compute_stack_size",
        "THREAD_PREFIX" => "compute_thread_prefix",
        other => other,
    }
    .to_string()
}

fn map_health_check_key(key: &str) -> String {
    match key {
        "ENABLED" => "health_check_enabled",
        "REQUEST_TIMEOUT" => "health_check_request_timeout_secs",
        other => other,
    }
    .to_string()
}

fn map_canary_key(key: &str) -> String {
    match key {
        "WAIT_TIME" => "canary_wait_time_secs",
        other => other,
    }
    .to_string()
}

fn warn_if_deprecated_env_set() {
    use environment_names::runtime::system as env_system;
    if std::env::var(env_system::PGD_SYSTEM_USE_ENDPOINT_HEALTH_STATUS).is_ok() {
        tracing::warn!(
            "PGD_SYSTEM_USE_ENDPOINT_HEALTH_STATUS is deprecated and no longer used. \
             System health is now determined by portnames that register with health check payloads. \
             Please update your configuration to register health check payloads directly on portnames."
        );
    }
    if std::env::var(env_system::PGD_SYSTEM_ENABLED).is_ok() {
        tracing::warn!(
            "PGD_SYSTEM_ENABLED is deprecated. \
             System metrics server is now controlled solely by PGD_SYSTEM_PORT. \
             Set PGD_SYSTEM_PORT to a positive value to enable the server, or set to -1 to disable (default)."
        );
    }
}

// ============================================================================
// 布尔解析工具
// ============================================================================

/// 判断字符串是否表示"真"。大小写不敏感，集合见 [`TRUTHY_VALUES`]。
pub fn is_truthy(val: &str) -> bool {
    let n = val.to_ascii_lowercase();
    TRUTHY_VALUES.iter().any(|t| *t == n.as_str())
}

/// 判断字符串是否表示"假"。大小写不敏感，集合见 [`FALSEY_VALUES`]。
pub fn is_falsey(val: &str) -> bool {
    let n = val.to_ascii_lowercase();
    FALSEY_VALUES.iter().any(|t| *t == n.as_str())
}

/// 把字符串严格解析为 bool；不属于任一集合则报错。
pub fn parse_bool(val: &str) -> anyhow::Result<bool> {
    if is_truthy(val) {
        Ok(true)
    } else if is_falsey(val) {
        Ok(false)
    } else {
        anyhow::bail!(
            "Invalid boolean value: '{}'. Expected one of: true/false, 1/0, on/off, yes/no",
            val
        )
    }
}

/// 环境变量取值是否为真——变量缺失或不可识别都视为非真。
pub fn env_is_truthy(env: &str) -> bool {
    std::env::var(env)
        .ok()
        .map(|v| is_truthy(&v))
        .unwrap_or(false)
}

/// 环境变量取值是否为假——变量缺失或不可识别都视为非假。
pub fn env_is_falsey(env: &str) -> bool {
    std::env::var(env)
        .ok()
        .map(|v| is_falsey(&v))
        .unwrap_or(false)
}

// ----------------------------------------------------------------------------
// 日志相关的 env-bool 快捷查询
// ----------------------------------------------------------------------------

/// 是否启用 JSONL 日志格式（`PGD_LOGGING_JSONL`）。
pub fn jsonl_logging_enabled() -> bool {
    env_is_truthy(environment_names::logging::PGD_LOGGING_JSONL)
}

/// 是否关闭 ANSI 颜色（`PGD_SDK_DISABLE_ANSI_LOGGING`）。
pub fn disable_ansi_logging() -> bool {
    env_is_truthy(environment_names::logging::PGD_SDK_DISABLE_ANSI_LOGGING)
}

/// 日志时间戳是否使用本地时区（`PGD_LOG_USE_LOCAL_TZ`）。
pub fn use_local_timezone() -> bool {
    env_is_truthy(environment_names::logging::PGD_LOG_USE_LOCAL_TZ)
}

/// 是否开启 span event 日志（`PGD_LOGGING_SPAN_EVENTS`）。
pub fn span_events_enabled() -> bool {
    env_is_truthy(environment_names::logging::PGD_LOGGING_SPAN_EVENTS)
}

// ============================================================================
// 单元测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runtime_config_with_env_vars() -> Result<()> {
        use environment_names::runtime;
        temp_env::with_vars(
            vec![
                (runtime::PGD_RUNTIME_NUM_WORKER_THREADS, Some("24")),
                (runtime::PGD_RUNTIME_MAX_BLOCKING_THREADS, Some("32")),
            ],
            || {
                let config = RuntimeConfig::from_settings()?;
                assert_eq!(config.num_worker_threads, Some(24));
                assert_eq!(config.max_blocking_threads, 32);
                Ok(())
            },
        )
    }

    #[test]
    fn test_runtime_config_defaults() -> Result<()> {
        use environment_names::runtime;
        temp_env::with_vars(
            vec![
                (runtime::PGD_RUNTIME_NUM_WORKER_THREADS, None::<&str>),
                (runtime::PGD_RUNTIME_MAX_BLOCKING_THREADS, Some("")),
            ],
            || {
                let config = RuntimeConfig::from_settings()?;
                let default_config = RuntimeConfig::default();
                assert_eq!(config.num_worker_threads, default_config.num_worker_threads);
                assert_eq!(
                    config.max_blocking_threads,
                    default_config.max_blocking_threads
                );
                Ok(())
            },
        )
    }

    #[test]
    fn test_runtime_config_rejects_invalid_thread_count() -> Result<()> {
        use environment_names::runtime;
        temp_env::with_vars(
            vec![
                (runtime::PGD_RUNTIME_NUM_WORKER_THREADS, Some("0")),
                (runtime::PGD_RUNTIME_MAX_BLOCKING_THREADS, Some("0")),
            ],
            || {
                let err = RuntimeConfig::from_settings().unwrap_err().to_string();
                assert!(err.contains("num_worker_threads: Validation error"));
                assert!(err.contains("max_blocking_threads: Validation error"));
                Ok(())
            },
        )
    }

    #[test]
    fn test_runtime_config_system_server_env_vars() -> Result<()> {
        use environment_names::runtime::system;
        temp_env::with_vars(
            vec![
                (system::PGD_SYSTEM_HOST, Some("127.0.0.1")),
                (system::PGD_SYSTEM_PORT, Some("9090")),
            ],
            || {
                let config = RuntimeConfig::from_settings()?;
                assert_eq!(config.system_host, "127.0.0.1");
                assert_eq!(config.system_port, 9090);
                Ok(())
            },
        )
    }

    #[test]
    fn test_system_server_disabled_by_default() {
        use environment_names::runtime::system;
        temp_env::with_vars(vec![(system::PGD_SYSTEM_PORT, None::<&str>)], || {
            let config = RuntimeConfig::from_settings().unwrap();
            assert!(!config.system_server_enabled());
            assert_eq!(config.system_port, -1);
        });
    }

    #[test]
    fn test_system_server_disabled_with_negative_port() {
        use environment_names::runtime::system;
        temp_env::with_vars(vec![(system::PGD_SYSTEM_PORT, Some("-1"))], || {
            let config = RuntimeConfig::from_settings().unwrap();
            assert!(!config.system_server_enabled());
            assert_eq!(config.system_port, -1);
        });
    }

    #[test]
    fn test_system_server_enabled_with_port() {
        use environment_names::runtime::system;
        temp_env::with_vars(vec![(system::PGD_SYSTEM_PORT, Some("9527"))], || {
            let config = RuntimeConfig::from_settings().unwrap();
            assert!(config.system_server_enabled());
            assert_eq!(config.system_port, 9527);
        });
    }

    #[test]
    fn test_system_server_starting_health_status_ready() {
        use environment_names::runtime::system;
        temp_env::with_vars(
            vec![(system::PGD_SYSTEM_STARTING_HEALTH_STATUS, Some("ready"))],
            || {
                let config = RuntimeConfig::from_settings().unwrap();
                assert_eq!(config.starting_health_status, HealthStatus::Ready);
            },
        );
    }

    #[test]
    fn test_system_use_portname_health_status() {
        use environment_names::runtime::system;
        temp_env::with_vars(
            vec![(
                system::PGD_SYSTEM_USE_ENDPOINT_HEALTH_STATUS,
                Some("[\"ready\"]"),
            )],
            || {
                let config = RuntimeConfig::from_settings().unwrap();
                assert_eq!(config.use_portname_health_status, vec!["ready"]);
            },
        );
    }

    #[test]
    fn test_system_health_endpoint_path_default() {
        use environment_names::runtime::system;
        temp_env::with_vars(vec![(system::PGD_SYSTEM_HEALTH_PATH, None::<&str>)], || {
            let c = RuntimeConfig::from_settings().unwrap();
            assert_eq!(c.system_health_path, DEFAULT_SYSTEM_HEALTH_PATH);
        });
        temp_env::with_vars(vec![(system::PGD_SYSTEM_LIVE_PATH, None::<&str>)], || {
            let c = RuntimeConfig::from_settings().unwrap();
            assert_eq!(c.system_live_path, DEFAULT_SYSTEM_LIVE_PATH);
        });
    }

    #[test]
    fn test_system_health_endpoint_path_custom() {
        use environment_names::runtime::system;
        temp_env::with_vars(
            vec![(system::PGD_SYSTEM_HEALTH_PATH, Some("/custom/health"))],
            || {
                let c = RuntimeConfig::from_settings().unwrap();
                assert_eq!(c.system_health_path, "/custom/health");
            },
        );
        temp_env::with_vars(
            vec![(system::PGD_SYSTEM_LIVE_PATH, Some("/custom/live"))],
            || {
                let c = RuntimeConfig::from_settings().unwrap();
                assert_eq!(c.system_live_path, "/custom/live");
            },
        );
    }

    #[test]
    fn test_is_truthy_and_falsey() {
        for v in ["1", "true", "TRUE", "on", "yes"] {
            assert!(is_truthy(v), "{v} 应被识别为真");
        }
        for v in ["0", "false", "FALSE", "off", "no"] {
            assert!(is_falsey(v), "{v} 应被识别为假");
        }
        assert!(!is_truthy("0"));
        assert!(!is_falsey("1"));

        temp_env::with_vars(vec![("TEST_TRUTHY", Some("true"))], || {
            assert!(env_is_truthy("TEST_TRUTHY"));
            assert!(!env_is_falsey("TEST_TRUTHY"));
        });
        temp_env::with_vars(vec![("TEST_FALSEY", Some("false"))], || {
            assert!(!env_is_truthy("TEST_FALSEY"));
            assert!(env_is_falsey("TEST_FALSEY"));
        });
        temp_env::with_vars(vec![("TEST_MISSING", None::<&str>)], || {
            assert!(!env_is_truthy("TEST_MISSING"));
            assert!(!env_is_falsey("TEST_MISSING"));
        });
    }

    // === SECTION: 补充测试 ===
    use super::*;

    /// ## 测试过程
    /// 1. 不显式覆盖任何字段（仅给 `num_worker_threads` 传 `None`）；
    /// 2. 断言 builder 产出的对象与各 `DEFAULT_*` 常量一致；
    /// 3. 把 `num_worker_threads(0)` 与 `max_blocking_threads(0)` 塞进
    ///    builder，预期 `build()` 返回包含两个字段名的错误。
    ///
    /// ## 意义
    /// 锁定 `RuntimeConfigBuilder::build` 的"默认值表 + validator 校
    /// 验"双重契约。
    #[test]
    fn test_supplemental_runtime_config_builder_defaults_and_validation() {
        let config = RuntimeConfig::builder()
            .num_worker_threads(None)
            .build()
            .unwrap();

        assert_eq!(config.num_worker_threads, None);
        assert_eq!(config.max_blocking_threads, 512);
        assert_eq!(config.system_host, DEFAULT_SYSTEM_HOST);
        assert_eq!(config.system_port, DEFAULT_SYSTEM_PORT);
        assert_eq!(config.starting_health_status, HealthStatus::NotReady);
        assert!(config.use_portname_health_status.is_empty());
        assert_eq!(config.system_health_path, DEFAULT_SYSTEM_HEALTH_PATH);
        assert_eq!(config.system_live_path, DEFAULT_SYSTEM_LIVE_PATH);
        assert_eq!(config.compute_threads, None);
        assert_eq!(config.compute_stack_size, Some(2 * 1024 * 1024));
        assert_eq!(config.compute_thread_prefix, "compute");
        assert!(!config.health_check_enabled);
        assert_eq!(config.canary_wait_time_secs, DEFAULT_CANARY_WAIT_TIME_SECS);
        assert_eq!(
            config.health_check_request_timeout_secs,
            DEFAULT_HEALTH_CHECK_REQUEST_TIMEOUT_SECS
        );

        let mut invalid = RuntimeConfig::builder();
        invalid.num_worker_threads(Some(0));
        invalid.max_blocking_threads(0);
        let err = invalid.build().unwrap_err().to_string();
        assert!(err.contains("num_worker_threads"));
        assert!(err.contains("max_blocking_threads"));
    }

    /// ## 测试过程
    /// 用 builder 给每一个字段塞自定义值，然后断言：
    /// 1. struct 字段值与输入一致；
    /// 2. `Display` 输出包含约定的 `key=value` 子串；
    /// 3. 没有显式设置 `num_worker_threads` 时 Display 走"default
    ///    (num_cores)"分支。
    ///
    /// ## 意义
    /// 锁定 Display 字段顺序与 token 格式——日志 / 运维监控依赖该格式。
    #[test]
    fn test_supplemental_runtime_config_builder_custom_values_and_display() {
        let mut b = RuntimeConfig::builder();
        b.num_worker_threads(Some(4));
        b.max_blocking_threads(9);
        b.system_host("127.0.0.1".to_string());
        b.system_port(0);
        b.starting_health_status(HealthStatus::Ready);
        b.use_portname_health_status(vec!["ready".to_string(), "live".to_string()]);
        b.system_health_path("/healthz".to_string());
        b.system_live_path("/livez".to_string());
        b.compute_threads(Some(3));
        b.compute_stack_size(Some(4096));
        b.compute_thread_prefix("batch".to_string());
        b.health_check_enabled(true);
        b.canary_wait_time_secs(12);
        b.health_check_request_timeout_secs(7);

        let c = b.build().unwrap();
        assert_eq!(c.num_worker_threads, Some(4));
        assert_eq!(c.max_blocking_threads, 9);

        let s = c.to_string();
        for tag in [
            "num_worker_threads=4",
            "max_blocking_threads=9",
            "system_host=127.0.0.1",
            "system_port=0",
            "use_portname_health_status=[\"ready\", \"live\"]",
            "starting_health_status=Ready",
            "system_health_path=/healthz",
            "system_live_path=/livez",
            "health_check_enabled=true",
            "canary_wait_time_secs=12",
            "health_check_request_timeout_secs=7",
        ] {
            assert!(s.contains(tag), "Display 缺少 {tag}: {s}");
        }

        let default_render = RuntimeConfig::builder()
            .num_worker_threads(None)
            .build()
            .unwrap()
            .to_string();
        assert!(default_render.contains("num_worker_threads=default (num_cores)"));
    }

    /// ## 测试过程
    /// 1. `RuntimeConfig::default()` 的 worker / blocking 线程数应等
    ///    于机器并行度；
    /// 2. `single_threaded()` 把三类线程数都压到 1；
    /// 3. 两者的非线程字段都应等于各 `DEFAULT_*` 常量。
    #[test]
    fn test_supplemental_runtime_config_default_and_single_threaded_values() {
        let cores = std::thread::available_parallelism().unwrap().get();
        let d = RuntimeConfig::default();
        assert_eq!(d.num_worker_threads, Some(cores));
        assert_eq!(d.max_blocking_threads, cores);
        assert_eq!(d.system_host, DEFAULT_SYSTEM_HOST);
        assert_eq!(d.system_port, DEFAULT_SYSTEM_PORT);
        assert_eq!(d.compute_stack_size, Some(2 * 1024 * 1024));
        assert!(!d.system_server_enabled());

        let s = RuntimeConfig::single_threaded();
        assert_eq!(s.num_worker_threads, Some(1));
        assert_eq!(s.max_blocking_threads, 1);
        assert_eq!(s.compute_threads, Some(1));
        assert_eq!(s.system_host, DEFAULT_SYSTEM_HOST);
        assert!(!s.system_server_enabled());
    }

    /// ## 测试过程
    /// 设置五个前缀里的多个环境变量（含一个空字符串变量验证"非空过
    /// 滤"），调用 `figment().extract()` 直接拿到配置，逐字段对比期
    /// 望值。
    #[test]
    fn test_supplemental_runtime_config_figment_maps_and_filters_env_vars() {
        use environment_names::runtime;
        use environment_names::runtime::canary;
        use environment_names::runtime::system;

        temp_env::with_vars(
            vec![
                (runtime::PGD_RUNTIME_NUM_WORKER_THREADS, Some("7")),
                (runtime::PGD_RUNTIME_MAX_BLOCKING_THREADS, Some("")),
                (system::PGD_SYSTEM_HOST, Some("127.0.0.1")),
                (system::PGD_SYSTEM_PORT, Some("0")),
                (system::PGD_SYSTEM_HEALTH_PATH, Some("/healthz")),
                (system::PGD_SYSTEM_LIVE_PATH, Some("")),
                ("PGD_COMPUTE_THREADS", Some("6")),
                ("PGD_COMPUTE_STACK_SIZE", Some("4096")),
                ("PGD_COMPUTE_THREAD_PREFIX", Some("batch")),
                ("PGD_HEALTH_CHECK_ENABLED", Some("true")),
                ("PGD_HEALTH_CHECK_REQUEST_TIMEOUT", Some("9")),
                (canary::PGD_CANARY_WAIT_TIME, Some("12")),
            ],
            || {
                let c = RuntimeConfig::figment().extract::<RuntimeConfig>().unwrap();
                assert_eq!(c.num_worker_threads, Some(7));
                assert_eq!(
                    c.max_blocking_threads,
                    RuntimeConfig::default().max_blocking_threads
                );
                assert_eq!(c.system_host, "127.0.0.1");
                assert_eq!(c.system_port, 0);
                assert_eq!(c.system_health_path, "/healthz");
                assert_eq!(c.system_live_path, DEFAULT_SYSTEM_LIVE_PATH);
                assert_eq!(c.compute_threads, Some(6));
                assert_eq!(c.compute_stack_size, Some(4096));
                assert_eq!(c.compute_thread_prefix, "batch");
                assert!(c.health_check_enabled);
                assert_eq!(c.canary_wait_time_secs, 12);
                assert_eq!(c.health_check_request_timeout_secs, 9);
            },
        );
    }

    /// 与上一个测试类似，但走 `from_settings`（含已废弃 warn 路径与
    /// validate）。验证 port=0 时 `system_server_enabled()==true`。
    #[test]
    fn test_supplemental_runtime_config_from_settings_zero_port_and_extra_envs() -> Result<()> {
        use environment_names::runtime::canary;
        use environment_names::runtime::system;

        temp_env::with_vars(
            vec![
                (system::PGD_SYSTEM_ENABLED, Some("true")),
                (system::PGD_SYSTEM_PORT, Some("0")),
                (system::PGD_SYSTEM_STARTING_HEALTH_STATUS, Some("ready")),
                (
                    system::PGD_SYSTEM_USE_ENDPOINT_HEALTH_STATUS,
                    Some("[\"ready\",\"live\"]"),
                ),
                ("PGD_COMPUTE_THREADS", Some("6")),
                ("PGD_COMPUTE_STACK_SIZE", Some("1048576")),
                ("PGD_COMPUTE_THREAD_PREFIX", Some("ray")),
                ("PGD_HEALTH_CHECK_ENABLED", Some("true")),
                ("PGD_HEALTH_CHECK_REQUEST_TIMEOUT", Some("9")),
                (canary::PGD_CANARY_WAIT_TIME, Some("12")),
            ],
            || {
                let c = RuntimeConfig::from_settings()?;
                assert!(c.system_server_enabled());
                assert_eq!(c.system_port, 0);
                assert_eq!(c.starting_health_status, HealthStatus::Ready);
                assert_eq!(
                    c.use_portname_health_status,
                    vec!["ready".to_string(), "live".to_string()]
                );
                assert_eq!(c.compute_threads, Some(6));
                assert_eq!(c.compute_stack_size, Some(1_048_576));
                assert_eq!(c.compute_thread_prefix, "ray");
                assert!(c.health_check_enabled);
                assert_eq!(c.health_check_request_timeout_secs, 9);
                assert_eq!(c.canary_wait_time_secs, 12);
                Ok(())
            },
        )
    }

    /// `port = 0` 也应视为启用。
    #[test]
    fn test_supplemental_system_server_enabled_zero_port() {
        use environment_names::runtime::system;
        temp_env::with_vars(vec![(system::PGD_SYSTEM_PORT, Some("0"))], || {
            let c = RuntimeConfig::from_settings().unwrap();
            assert!(c.system_server_enabled());
            assert_eq!(c.system_port, 0);
        });
    }

    /// ## 测试过程
    /// 分别在"未设置 poll histogram"和"设置为 true"两种场景下创建
    /// runtime，跑一个 spawn 看是否能正常 await 出值。
    #[test]
    fn test_supplemental_runtime_config_create_runtime_with_poll_histogram_variants() {
        use environment_names::runtime;
        temp_env::with_vars(
            vec![(runtime::PGD_ENABLE_POLL_HISTOGRAM, None::<&str>)],
            || {
                let rt = RuntimeConfig::single_threaded().create_runtime().unwrap();
                let r = rt.block_on(async { tokio::spawn(async { 5usize }).await.unwrap() });
                assert_eq!(r, 5);
            },
        );
        temp_env::with_vars(
            vec![(runtime::PGD_ENABLE_POLL_HISTOGRAM, Some("true"))],
            || {
                let rt = RuntimeConfig::single_threaded().create_runtime().unwrap();
                let r = rt.block_on(async { tokio::spawn(async { 7usize }).await.unwrap() });
                assert_eq!(r, 7);
            },
        );
    }

    /// 覆盖 `parse_bool` 三分支 + 四个日志 helper 的真假切换。
    #[test]
    fn test_supplemental_parse_bool_logging_helpers_and_invalid_env_values() {
        use environment_names::logging;

        assert!(parse_bool("true").unwrap());
        assert!(!parse_bool("OFF").unwrap());
        let err = parse_bool("maybe").unwrap_err();
        assert!(err.to_string().contains("Invalid boolean value"));

        temp_env::with_vars(vec![("TEST_INVALID_BOOL", Some("maybe"))], || {
            assert!(!env_is_truthy("TEST_INVALID_BOOL"));
            assert!(!env_is_falsey("TEST_INVALID_BOOL"));
        });

        temp_env::with_vars(
            vec![
                (logging::PGD_LOGGING_JSONL, Some("yes")),
                (logging::PGD_SDK_DISABLE_ANSI_LOGGING, Some("1")),
                (logging::PGD_LOG_USE_LOCAL_TZ, Some("true")),
                (logging::PGD_LOGGING_SPAN_EVENTS, Some("on")),
            ],
            || {
                assert!(jsonl_logging_enabled());
                assert!(disable_ansi_logging());
                assert!(use_local_timezone());
                assert!(span_events_enabled());
            },
        );
        temp_env::with_vars(
            vec![
                (logging::PGD_LOGGING_JSONL, Some("0")),
                (logging::PGD_SDK_DISABLE_ANSI_LOGGING, Some("false")),
                (logging::PGD_LOG_USE_LOCAL_TZ, None::<&str>),
                (logging::PGD_LOGGING_SPAN_EVENTS, Some("no")),
            ],
            || {
                assert!(!jsonl_logging_enabled());
                assert!(!disable_ansi_logging());
                assert!(!use_local_timezone());
                assert!(!span_events_enabled());
            },
        );
    }

    /// `WorkerConfig` 走默认值、空环境变量、自定义值四个场景。
    #[test]
    fn test_supplemental_worker_config_defaults_and_env_override() {
        use environment_names::worker;
        let expected = if cfg!(debug_assertions) { 1 } else { 30 };
        assert_eq!(WorkerConfig::default().graceful_shutdown_timeout, expected);

        temp_env::with_vars(
            vec![(worker::PGD_WORKER_GRACEFUL_SHUTDOWN_TIMEOUT, None::<&str>)],
            || {
                assert_eq!(
                    WorkerConfig::from_settings().graceful_shutdown_timeout,
                    expected
                );
            },
        );
        temp_env::with_vars(
            vec![(worker::PGD_WORKER_GRACEFUL_SHUTDOWN_TIMEOUT, Some("42"))],
            || {
                assert_eq!(WorkerConfig::from_settings().graceful_shutdown_timeout, 42);
            },
        );
    }

    /// 非法 worker 环境变量应在 `from_settings` 中 panic（启动期暴露）。
    #[test]
    fn test_supplemental_worker_config_panics_on_invalid_env() {
        use environment_names::worker;
        temp_env::with_vars(
            vec![(worker::PGD_WORKER_GRACEFUL_SHUTDOWN_TIMEOUT, Some("invalid"))],
            || {
                let r = std::panic::catch_unwind(WorkerConfig::from_settings);
                assert!(r.is_err());
            },
        );
    }
}

// ============================================================================
// 补充测试：覆盖 builder / Display / figment 映射 / runtime 创建等扩展面
// ============================================================================

