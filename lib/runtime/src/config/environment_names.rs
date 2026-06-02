// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `config::environment_names` —— 环境变量名集中表
//!
//! ## 设计意图
//!
//! Pagoda 在十几个子系统里读取了上百个环境变量。这些字符串散落
//! 在各处，曾出现"代码里写 `PGD_LOG_USE_LOCAL_TZ`，文档里写
//! `PGD_LOGGING_USE_LOCAL_TZ`"这种悄无声息的拼写漂移。本模块的目的
//! 只有一个——**用 `pub const` 把所有变量名集中声明一次**，让每一处
//! 引用都强制经过编译器名称解析，永远拼不错、永远改得动。
//!
//! ## 实现要点
//!
//! 绝大多数常量是"标识符与字面值完全相同"的纯样板（例如
//! `pub const PGD_LOG: &str = "PGD_LOG";`）。为消除噪声、突出**例
//! 外项**（少数几个 `_PREFIX`），引入一个私有 [`mirror!`] 宏：
//!
//! ```ignore
//! mirror! {
//!     /// Log level
//!     PGD_LOG,
//!     /// JSONL logging
//!     PGD_LOGGING_JSONL,
//! }
//! ```
//!
//! 展开后等价于：
//!
//! ```ignore
//! pub const PGD_LOG: &str = "PGD_LOG";
//! pub const PGD_LOGGING_JSONL: &str = "PGD_LOGGING_JSONL";
//! ```
//!
//! 三个真正的"前缀型"常量（`PGD_COMPUTE_` / `PGD_KVBM_NIXL_BACKEND_`
//! / `PGD_HISTOGRAM_`）继续写成普通 `pub const`——它们**不应**遵循
//! "name == value"模式。
//!
//! ## 外部契约
//!
//! 所有常量的**名字**、**值**、**所在子模块路径**都保持稳定——其他子
//! 项目以 `use pagoda_runtime::config::environment_names::xxx::YYY;`
//! 形式直接消费这些常量。

// ============================================================================
// 私有 helper 宏：批量声明"标识符 == 值"的常量
// ============================================================================

/// 把若干"name == value"的 `pub const` 折叠成一行一个的紧凑形式。
///
/// 仅在本模块内部使用，故标 `pub(self)` 等价的限制——通过 `macro_rules!`
/// 默认就是文件内可见。
macro_rules! mirror {
    ( $( $(#[$attr:meta])* $name:ident ),+ $(,)? ) => {
        $(
            $(#[$attr])*
            pub const $name: &str = stringify!($name);
        )+
    };
}

// ============================================================================
// logging —— 日志与追踪
// ============================================================================

/// 日志与追踪环境变量。
pub mod logging {
    mirror! {
        /// 日志级别（例如 "debug" / "info" / "warn" / "error"）。
        PGD_LOG,
        /// 日志配置文件路径。
        PGD_LOGGING_CONFIG_PATH,
        /// 启用 JSONL 输出格式。
        PGD_LOGGING_JSONL,
        /// 关闭 ANSI 颜色 / 控制字符。
        PGD_SDK_DISABLE_ANSI_LOGGING,
        /// 使用本地时区而不是 UTC。
        PGD_LOG_USE_LOCAL_TZ,
        /// 启用 span event 日志（create / close）。
        PGD_LOGGING_SPAN_EVENTS,
    }

    /// OTLP（OpenTelemetry Protocol）相关。
    pub mod otlp {
        mirror! {
            /// 是否启用 traces / logs 的 OTLP 导出（"1" 启用）。
            OTEL_EXPORT_ENABLED,
            /// OTLP traces 导出端点 URL。
            OTEL_EXPORTER_OTLP_TRACES_ENDPOINT,
            /// OTLP logs 导出端点 URL（不设则沿用 traces 端点）。
            OTEL_EXPORTER_OTLP_LOGS_ENDPOINT,
            /// OTLP 服务名。
            OTEL_SERVICE_NAME,
        }
    }
}

// ============================================================================
// runtime —— Tokio runtime / system 服务
// ============================================================================

/// Tokio runtime、system 服务、worker 行为相关。
pub mod runtime {
    mirror! {
        /// async worker 线程数。
        PGD_RUNTIME_NUM_WORKER_THREADS,
        /// blocking 线程数上限。
        PGD_RUNTIME_MAX_BLOCKING_THREADS,
        /// 启用 Tokio poll-time histogram。
        PGD_ENABLE_POLL_HISTOGRAM,
    }

    /// system 状态服务配置。
    pub mod system {
        mirror! {
            /// 启用 system 状态服务。⚠️ 已废弃。
            PGD_SYSTEM_ENABLED,
            /// system 状态服务 host。
            PGD_SYSTEM_HOST,
            /// system 状态服务端口。
            PGD_SYSTEM_PORT,
            /// 已废弃：曾用于声明哪些 portname 参与汇总健康判定。
            PGD_SYSTEM_USE_ENDPOINT_HEALTH_STATUS,
            /// 进程启动初始健康状态。
            PGD_SYSTEM_STARTING_HEALTH_STATUS,
            /// `/health` 路径。
            PGD_SYSTEM_HEALTH_PATH,
            /// `/live` 路径。
            PGD_SYSTEM_LIVE_PATH,
        }
    }

    /// compute 子系统。
    pub mod compute {
        /// `PGD_COMPUTE_*` 系列环境变量的前缀。
        ///
        /// 例外：该常量是"前缀"而不是某个具体变量名，故不能用
        /// `mirror!` 自动生成。
        pub const PREFIX: &str = "PGD_COMPUTE_";
    }

    /// canary 部署。
    pub mod canary {
        mirror! {
            /// canary 等候时间（秒）。
            PGD_CANARY_WAIT_TIME,
        }
    }
}

// ============================================================================
// worker —— 进程生命周期
// ============================================================================

/// worker 生命周期。
pub mod worker {
    mirror! {
        /// worker 优雅关闭超时（秒）。
        PGD_WORKER_GRACEFUL_SHUTDOWN_TIMEOUT,
    }
}

// ============================================================================
// nats / etcd —— 传输与发现
// ============================================================================

/// NATS 传输。
pub mod nats {
    mirror! {
        /// NATS 服务地址（例如 "nats://localhost:4222"）。
        NATS_SERVER,
    }

    /// NATS 认证（按优先级匹配）。
    pub mod auth {
        mirror! {
            /// 用户名（配合 NATS_AUTH_PASSWORD）。
            NATS_AUTH_USERNAME,
            /// 密码（配合 NATS_AUTH_USERNAME）。
            NATS_AUTH_PASSWORD,
            /// Token 认证。
            NATS_AUTH_TOKEN,
            /// NKey 认证。
            NATS_AUTH_NKEY,
            /// 凭据文件路径。
            NATS_AUTH_CREDENTIALS_FILE,
        }
    }

    /// NATS 流配置。
    pub mod stream {
        mirror! {
            /// NATS 流消息最大保留时长（秒）。
            PGD_NATS_STREAM_MAX_AGE,
        }
    }
}

/// ETCD 传输。
pub mod etcd {
    mirror! {
        /// ETCD portnames（逗号分隔 URL 列表）。
        ETCD_ENDPOINTS,
    }

    /// ETCD 认证。
    pub mod auth {
        mirror! {
            /// 用户名。
            ETCD_AUTH_USERNAME,
            /// 密码。
            ETCD_AUTH_PASSWORD,
            /// CA 证书路径。
            ETCD_AUTH_CA,
            /// 客户端证书路径。
            ETCD_AUTH_CLIENT_CERT,
            /// 客户端密钥路径。
            ETCD_AUTH_CLIENT_KEY,
        }
    }
}

// ============================================================================
// kvbm —— Key-Value Block Manager
// ============================================================================

/// KVBM 相关。
pub mod kvbm {
    mirror! {
        /// 启用 KVBM 指标 portname。
        PGD_KVBM_METRICS,
        /// KVBM 指标 portname 端口。
        PGD_KVBM_METRICS_PORT,
        /// 启用 KVBM 调试录制。
        PGD_KVBM_ENABLE_RECORD,
        /// 关闭磁盘 offload 过滤器。
        PGD_KVBM_DISABLE_DISK_OFFLOAD_FILTER,
    }

    /// CPU 缓存。
    pub mod cpu_cache {
        mirror! {
            /// CPU 缓存大小（GB）。
            PGD_KVBM_CPU_CACHE_GB,
            /// CPU 缓存块数覆写。
            PGD_KVBM_CPU_CACHE_OVERRIDE_NUM_BLOCKS,
        }
    }

    /// 磁盘缓存。
    pub mod disk_cache {
        mirror! {
            /// 磁盘缓存大小（GB）。
            PGD_KVBM_DISK_CACHE_GB,
            /// 磁盘缓存块数覆写。
            PGD_KVBM_DISK_CACHE_OVERRIDE_NUM_BLOCKS,
        }
    }

    /// 对象存储。
    pub mod object_storage {
        mirror! {
            /// 启用对象存储（"1" 启用）。
            PGD_KVBM_OBJECT_ENABLED,
            /// bucket 名（支持 `{worker_id}` 模板）。
            PGD_KVBM_OBJECT_BUCKET,
            /// portname。
            PGD_KVBM_OBJECT_ENDPOINT,
            /// region。
            PGD_KVBM_OBJECT_REGION,
            /// access key。
            PGD_KVBM_OBJECT_ACCESS_KEY,
            /// secret key。
            PGD_KVBM_OBJECT_SECRET_KEY,
            /// 存储块数。
            PGD_KVBM_OBJECT_NUM_BLOCKS,
        }
    }

    /// 传输。
    pub mod transfer {
        mirror! {
            /// 单批最大块数。
            PGD_KVBM_TRANSFER_BATCH_SIZE,
        }
    }

    /// KVBM leader（分布式模式）。
    pub mod leader {
        mirror! {
            /// leader/worker 初始化超时（秒）。
            PGD_KVBM_LEADER_WORKER_INIT_TIMEOUT_SECS,
            /// ZMQ host。
            PGD_KVBM_LEADER_ZMQ_HOST,
            /// ZMQ pub 端口。
            PGD_KVBM_LEADER_ZMQ_PUB_PORT,
            /// ZMQ ack 端口。
            PGD_KVBM_LEADER_ZMQ_ACK_PORT,
        }
    }

    /// NIXL backend。
    pub mod nixl {
        /// `PGD_KVBM_NIXL_BACKEND_*` 系列环境变量的前缀。
        ///
        /// 例外：前缀型常量，不走 `mirror!`。
        pub const PREFIX: &str = "PGD_KVBM_NIXL_BACKEND_";
    }
}

// ============================================================================
// llm —— LLM 推理 / metrics / audit / agent trace
// ============================================================================

/// LLM 推理。
pub mod llm {
    mirror! {
        /// HTTP body 体积上限（MB）。
        PGD_HTTP_BODY_LIMIT_MB,
        /// HTTP 优雅关闭超时（秒）。
        PGD_HTTP_GRACEFUL_SHUTDOWN_TIMEOUT_SECS,
        /// 启用 LoRA 适配器。
        PGD_LORA_ENABLED,
        /// LoRA 缓存目录。
        PGD_LORA_PATH,
        /// 启用实验性 Anthropic Messages API。
        PGD_ENABLE_ANTHROPIC_API,
        /// 是否剥离 Claude Code 计费 preamble。
        PGD_STRIP_ANTHROPIC_PREAMBLE,
        /// 启用流式工具调用分发。
        PGD_ENABLE_STREAMING_TOOL_DISPATCH,
        /// 启用流式 reasoning 分发。
        PGD_ENABLE_STREAMING_REASONING_DISPATCH,
        /// 后端流空闲超时（秒）。
        PGD_HTTP_BACKEND_STREAM_TIMEOUT_SECS,
        /// 启用 LoRA 分配控制器。
        PGD_LORA_ALLOCATION_ENABLED,
        /// LoRA 分配算法（"hrw" / "random"）。
        PGD_LORA_ALLOCATION_ALGORITHM,
        /// LoRA 分配重算间隔（秒）。
        PGD_LORA_ALLOCATION_TIMESTEP_SECS,
        /// LoRA 副本缩容 cooldown 周期数。
        PGD_LORA_ALLOCATION_SCALE_DOWN_COOLDOWN_TICKS,
        /// 速率窗口相对周期的倍率。
        PGD_LORA_ALLOCATION_RATE_WINDOW_MULTIPLIER,
        /// `BucketedRateCounter` 每秒桶数。
        PGD_LORA_ALLOCATION_BUCKETS_PER_SECOND,
        /// 负载预测器类型（"none" / "ema"）。
        PGD_LORA_ALLOCATION_PREDICTOR_TYPE,
        /// EMA 平滑系数 alpha。
        PGD_LORA_ALLOCATION_EMA_ALPHA,
    }

    /// Metrics 配置。
    pub mod metrics {
        mirror! {
            /// 自定义 metrics 前缀（覆盖默认 "pagoda_frontend"）。
            PGD_METRICS_PREFIX,
        }

        /// 直方图前缀。
        ///
        /// 例外：前缀型常量。
        pub const HISTOGRAM_PREFIX: &str = "PGD_HISTOGRAM_";
    }

    /// Audit sink 配置。
    pub mod audit {
        mirror! {
            /// audit sink 选择（逗号分隔：`stderr`/`nats`/`jsonl`/`jsonl_gz`）。
            PGD_AUDIT_SINKS,
            /// 强制 audit 即使 `store=false`。
            PGD_AUDIT_FORCE_LOGGING,
            /// 进程内 audit bus 容量。
            PGD_AUDIT_CAPACITY,
            /// JetStream audit sink 的 NATS 主题。
            PGD_AUDIT_NATS_SUBJECT,
            /// 本地 audit 输出路径。
            PGD_AUDIT_OUTPUT_PATH,
            /// JSONL audit sink 缓冲区字节数。
            PGD_AUDIT_JSONL_BUFFER_BYTES,
            /// JSONL audit sink 周期 flush 间隔（毫秒）。
            PGD_AUDIT_JSONL_FLUSH_INTERVAL_MS,
            /// 轮转 gz audit sink 阈值（未压缩字节数）。
            PGD_AUDIT_JSONL_GZ_ROLL_BYTES,
            /// 轮转 gz audit sink 阈值（记录行数）。
            PGD_AUDIT_JSONL_GZ_ROLL_LINES,
        }
    }

    /// Agent trace。
    pub mod agent_trace {
        mirror! {
            /// trace sink 选择。
            PGD_AGENT_TRACE_SINKS,
            /// 本地输出路径。
            PGD_AGENT_TRACE_OUTPUT_PATH,
            /// 进程内 trace bus 容量。
            PGD_AGENT_TRACE_CAPACITY,
            /// JSONL sink 缓冲区字节数。
            PGD_AGENT_TRACE_JSONL_BUFFER_BYTES,
            /// JSONL sink 周期 flush 间隔（毫秒）。
            PGD_AGENT_TRACE_JSONL_FLUSH_INTERVAL_MS,
            /// 轮转 gz sink 阈值（未压缩字节数）。
            PGD_AGENT_TRACE_JSONL_GZ_ROLL_BYTES,
            /// 轮转 gz sink 阈值（记录行数）。
            PGD_AGENT_TRACE_JSONL_GZ_ROLL_LINES,
            /// 启用 replay prompt block 哈希。
            PGD_AGENT_TRACE_REPLAY_HASHES,
            /// harness tool 事件本地 ZMQ PULL portname。
            PGD_AGENT_TRACE_TOOL_EVENTS_ZMQ_ENDPOINT,
            /// harness tool 事件可选 ZMQ topic 过滤。
            PGD_AGENT_TRACE_TOOL_EVENTS_ZMQ_TOPIC,
        }
    }
}

// ============================================================================
// model / router / 其他子系统
// ============================================================================

/// 模型加载与缓存。
pub mod model {
    /// Model Express。
    pub mod model_express {
        mirror! {
            /// Model Express 服务 URL。
            MODEL_EXPRESS_URL,
            /// Model Express 本地缓存路径。
            MODEL_EXPRESS_CACHE_PATH,
        }
    }

    /// Hugging Face。
    pub mod huggingface {
        mirror! {
            /// HF Token。
            HF_TOKEN,
            /// HF Hub 缓存目录。
            HF_HUB_CACHE,
            /// HF home 目录。
            HF_HOME,
            /// 离线模式。
            HF_HUB_OFFLINE,
        }
    }
}

/// KV Router。
pub mod router {
    mirror! {
        /// prefill 负载缩放因子。
        PGD_ROUTER_PREFILL_LOAD_SCALE,
        /// prefill token 容量排队阈值。
        PGD_ROUTER_QUEUE_THRESHOLD,
        /// 路由队列调度策略（"fcfs" / "wspt"）。
        PGD_ROUTER_QUEUE_POLICY,
    }
}

/// TCP 响应流服务（CallHome listener）。
pub mod tcp_response_stream {
    mirror! {
        /// TCP 响应流服务端口；未设或 0 时由 OS 分配。
        PGD_TCP_RESPONSE_STREAM_PORT,
        /// host/interface；未设时自动探测可路由 IP。
        PGD_TCP_RESPONSE_STREAM_HOST,
    }
}

/// Event Plane 传输。
pub mod event_plane {
    mirror! {
        /// 传输选择（"zmq" / "nats"）。
        PGD_EVENT_PLANE,
        /// 编解码（"json" / "msgpack"）。
        PGD_EVENT_PLANE_CODEC,
    }
}

/// ZMQ Broker。
pub mod zmq_broker {
    mirror! {
        /// 显式 ZMQ broker URL。
        PGD_ZMQ_BROKER_URL,
        /// 启用 ZMQ broker 发现模式。
        PGD_ZMQ_BROKER_ENABLED,
        /// XSUB 绑定地址（broker 二进制）。
        ZMQ_BROKER_XSUB_BIND,
        /// XPUB 绑定地址（broker 二进制）。
        ZMQ_BROKER_XPUB_BIND,
        /// broker 发现注册命名空间。
        ZMQ_BROKER_NAMESPACE,
    }
}

/// 发现后端。
pub mod discovery {
    mirror! {
        /// 发现后端（"kubernetes" / "etcd"）。
        PGD_DISCOVERY_BACKEND,
        /// kube 发现模式（"pod" / "container"）。
        PGD_KUBE_DISCOVERY_MODE,
    }
}

/// CUDA / GPU。
pub mod cuda {
    mirror! {
        /// 自定义 CUDA fatbin 文件路径。
        PGD_FATBIN_PATH,
    }
}

/// 构建期环境变量。
pub mod build {
    mirror! {
        /// Cargo 输出目录。
        OUT_DIR,
    }
}

/// Mocker（mock scheduler / KV manager）。
pub mod mocker {
    mirror! {
        /// 启用 KV cache 分配 / 淘汰结构化 trace 日志。
        PGD_MOCKER_KV_CACHE_TRACE,
        /// 使用原始 direct() 路径（存在启动期竞争，不建议常开）。
        PGD_MOCKER_SYNC_DIRECT,
    }
}

/// 测试相关。
pub mod testing {
    mirror! {
        /// 启用排队式请求处理。
        PGD_QUEUED_UP_PROCESSING,
        /// soak 测试时长（例如 "3s" / "5m"）。
        PGD_SOAK_RUN_DURATION,
        /// soak 测试批量负载。
        PGD_SOAK_BATCH_LOAD,
    }
}

// ============================================================================
// 单元测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// ## 测试过程
    /// 把所有公开常量塞进一个数组，扫一遍 `HashSet` 看是否重复。
    ///
    /// ## 意义
    /// 防止后续维护时不小心把同一 env 名复制到两个子模块下，造成
    /// 静默歧义。
    #[test]
    fn test_no_duplicate_env_var_names() {
        use std::collections::HashSet;

        let mut seen = HashSet::new();
        let vars = [
            // Logging
            logging::PGD_LOG,
            logging::PGD_LOGGING_CONFIG_PATH,
            logging::PGD_LOGGING_JSONL,
            logging::PGD_SDK_DISABLE_ANSI_LOGGING,
            logging::PGD_LOG_USE_LOCAL_TZ,
            logging::PGD_LOGGING_SPAN_EVENTS,
            logging::otlp::OTEL_EXPORT_ENABLED,
            logging::otlp::OTEL_EXPORTER_OTLP_TRACES_ENDPOINT,
            logging::otlp::OTEL_SERVICE_NAME,
            logging::otlp::OTEL_EXPORTER_OTLP_LOGS_ENDPOINT,
            // Runtime
            runtime::PGD_RUNTIME_NUM_WORKER_THREADS,
            runtime::PGD_RUNTIME_MAX_BLOCKING_THREADS,
            runtime::system::PGD_SYSTEM_ENABLED,
            runtime::system::PGD_SYSTEM_HOST,
            runtime::system::PGD_SYSTEM_PORT,
            runtime::system::PGD_SYSTEM_USE_ENDPOINT_HEALTH_STATUS,
            runtime::system::PGD_SYSTEM_STARTING_HEALTH_STATUS,
            runtime::system::PGD_SYSTEM_HEALTH_PATH,
            runtime::system::PGD_SYSTEM_LIVE_PATH,
            runtime::canary::PGD_CANARY_WAIT_TIME,
            // Worker
            worker::PGD_WORKER_GRACEFUL_SHUTDOWN_TIMEOUT,
            // NATS
            nats::NATS_SERVER,
            nats::auth::NATS_AUTH_USERNAME,
            nats::auth::NATS_AUTH_PASSWORD,
            nats::auth::NATS_AUTH_TOKEN,
            nats::auth::NATS_AUTH_NKEY,
            nats::auth::NATS_AUTH_CREDENTIALS_FILE,
            nats::stream::PGD_NATS_STREAM_MAX_AGE,
            // ETCD
            etcd::ETCD_ENDPOINTS,
            etcd::auth::ETCD_AUTH_USERNAME,
            etcd::auth::ETCD_AUTH_PASSWORD,
            etcd::auth::ETCD_AUTH_CA,
            etcd::auth::ETCD_AUTH_CLIENT_CERT,
            etcd::auth::ETCD_AUTH_CLIENT_KEY,
            // KVBM
            kvbm::PGD_KVBM_METRICS,
            kvbm::PGD_KVBM_METRICS_PORT,
            kvbm::PGD_KVBM_ENABLE_RECORD,
            kvbm::PGD_KVBM_DISABLE_DISK_OFFLOAD_FILTER,
            kvbm::cpu_cache::PGD_KVBM_CPU_CACHE_GB,
            kvbm::cpu_cache::PGD_KVBM_CPU_CACHE_OVERRIDE_NUM_BLOCKS,
            kvbm::disk_cache::PGD_KVBM_DISK_CACHE_GB,
            kvbm::disk_cache::PGD_KVBM_DISK_CACHE_OVERRIDE_NUM_BLOCKS,
            kvbm::leader::PGD_KVBM_LEADER_WORKER_INIT_TIMEOUT_SECS,
            kvbm::leader::PGD_KVBM_LEADER_ZMQ_HOST,
            kvbm::leader::PGD_KVBM_LEADER_ZMQ_PUB_PORT,
            kvbm::leader::PGD_KVBM_LEADER_ZMQ_ACK_PORT,
            // LLM
            llm::PGD_HTTP_BODY_LIMIT_MB,
            llm::PGD_HTTP_BACKEND_STREAM_TIMEOUT_SECS,
            llm::PGD_LORA_ENABLED,
            llm::PGD_LORA_PATH,
            llm::PGD_ENABLE_ANTHROPIC_API,
            llm::PGD_STRIP_ANTHROPIC_PREAMBLE,
            llm::PGD_ENABLE_STREAMING_TOOL_DISPATCH,
            llm::PGD_ENABLE_STREAMING_REASONING_DISPATCH,
            llm::PGD_LORA_ALLOCATION_ENABLED,
            llm::PGD_LORA_ALLOCATION_ALGORITHM,
            llm::PGD_LORA_ALLOCATION_TIMESTEP_SECS,
            llm::PGD_LORA_ALLOCATION_SCALE_DOWN_COOLDOWN_TICKS,
            llm::PGD_LORA_ALLOCATION_RATE_WINDOW_MULTIPLIER,
            llm::PGD_LORA_ALLOCATION_BUCKETS_PER_SECOND,
            llm::PGD_LORA_ALLOCATION_PREDICTOR_TYPE,
            llm::PGD_LORA_ALLOCATION_EMA_ALPHA,
            llm::metrics::PGD_METRICS_PREFIX,
            llm::audit::PGD_AUDIT_SINKS,
            llm::audit::PGD_AUDIT_FORCE_LOGGING,
            llm::audit::PGD_AUDIT_CAPACITY,
            llm::audit::PGD_AUDIT_NATS_SUBJECT,
            llm::audit::PGD_AUDIT_OUTPUT_PATH,
            llm::audit::PGD_AUDIT_JSONL_BUFFER_BYTES,
            llm::audit::PGD_AUDIT_JSONL_FLUSH_INTERVAL_MS,
            llm::audit::PGD_AUDIT_JSONL_GZ_ROLL_BYTES,
            llm::audit::PGD_AUDIT_JSONL_GZ_ROLL_LINES,
            llm::agent_trace::PGD_AGENT_TRACE_SINKS,
            llm::agent_trace::PGD_AGENT_TRACE_OUTPUT_PATH,
            llm::agent_trace::PGD_AGENT_TRACE_CAPACITY,
            llm::agent_trace::PGD_AGENT_TRACE_JSONL_BUFFER_BYTES,
            llm::agent_trace::PGD_AGENT_TRACE_JSONL_FLUSH_INTERVAL_MS,
            llm::agent_trace::PGD_AGENT_TRACE_JSONL_GZ_ROLL_BYTES,
            llm::agent_trace::PGD_AGENT_TRACE_JSONL_GZ_ROLL_LINES,
            llm::agent_trace::PGD_AGENT_TRACE_REPLAY_HASHES,
            llm::agent_trace::PGD_AGENT_TRACE_TOOL_EVENTS_ZMQ_ENDPOINT,
            llm::agent_trace::PGD_AGENT_TRACE_TOOL_EVENTS_ZMQ_TOPIC,
            // Model
            model::model_express::MODEL_EXPRESS_URL,
            model::model_express::MODEL_EXPRESS_CACHE_PATH,
            model::huggingface::HF_TOKEN,
            model::huggingface::HF_HUB_CACHE,
            model::huggingface::HF_HOME,
            model::huggingface::HF_HUB_OFFLINE,
            // Router
            router::PGD_ROUTER_PREFILL_LOAD_SCALE,
            router::PGD_ROUTER_QUEUE_THRESHOLD,
            router::PGD_ROUTER_QUEUE_POLICY,
            // TCP Response Stream
            tcp_response_stream::PGD_TCP_RESPONSE_STREAM_PORT,
            tcp_response_stream::PGD_TCP_RESPONSE_STREAM_HOST,
            // Event Plane
            event_plane::PGD_EVENT_PLANE,
            event_plane::PGD_EVENT_PLANE_CODEC,
            // ZMQ Broker
            zmq_broker::PGD_ZMQ_BROKER_URL,
            zmq_broker::PGD_ZMQ_BROKER_ENABLED,
            zmq_broker::ZMQ_BROKER_XSUB_BIND,
            zmq_broker::ZMQ_BROKER_XPUB_BIND,
            zmq_broker::ZMQ_BROKER_NAMESPACE,
            // Discovery
            discovery::PGD_DISCOVERY_BACKEND,
            discovery::PGD_KUBE_DISCOVERY_MODE,
            // CUDA
            cuda::PGD_FATBIN_PATH,
            // Build
            build::OUT_DIR,
            // Mocker
            mocker::PGD_MOCKER_KV_CACHE_TRACE,
            mocker::PGD_MOCKER_SYNC_DIRECT,
            // Testing
            testing::PGD_QUEUED_UP_PROCESSING,
            testing::PGD_SOAK_RUN_DURATION,
            testing::PGD_SOAK_BATCH_LOAD,
        ];

        for var in &vars {
            assert!(
                seen.insert(*var),
                "Duplicate environment variable name: {var}"
            );
        }
    }

    /// ## 测试过程
    /// 逐厂家断言常量字符串前缀符合命名约定（PGD_ / NATS_ / ETCD_ /
    /// OTEL_）。
    ///
    /// ## 意义
    /// 防止维护时把不属于本厂家的变量名错放进对应子模块。
    #[test]
    fn test_naming_conventions() {
        assert!(runtime::PGD_RUNTIME_NUM_WORKER_THREADS.starts_with("PGD_"));
        assert!(runtime::system::PGD_SYSTEM_ENABLED.starts_with("PGD_"));
        assert!(kvbm::PGD_KVBM_METRICS.starts_with("PGD_"));
        assert!(worker::PGD_WORKER_GRACEFUL_SHUTDOWN_TIMEOUT.starts_with("PGD_"));

        assert!(nats::NATS_SERVER.starts_with("NATS_"));
        assert!(nats::auth::NATS_AUTH_USERNAME.starts_with("NATS_AUTH_"));

        assert!(etcd::ETCD_ENDPOINTS.starts_with("ETCD_"));
        assert!(etcd::auth::ETCD_AUTH_USERNAME.starts_with("ETCD_AUTH_"));

        assert!(logging::otlp::OTEL_EXPORT_ENABLED.starts_with("OTEL_"));
        assert!(logging::otlp::OTEL_SERVICE_NAME.starts_with("OTEL_"));
    }

    /// ## 测试过程
    /// 抽样校验 `mirror!` 展开后：常量字面值与标识符同名。
    ///
    /// ## 意义
    /// 防止未来不小心把宏调用从 `mirror!` 改成手写后值漂移。
    #[test]
    fn test_mirror_macro_value_equals_identifier() {
        assert_eq!(logging::PGD_LOG, "PGD_LOG");
        assert_eq!(
            runtime::system::PGD_SYSTEM_STARTING_HEALTH_STATUS,
            "PGD_SYSTEM_STARTING_HEALTH_STATUS"
        );
        assert_eq!(model::huggingface::HF_TOKEN, "HF_TOKEN");
        assert_eq!(build::OUT_DIR, "OUT_DIR");
    }

    /// ## 测试过程
    /// 三个例外型前缀常量必须保留其历史字面值，不能被未来误改。
    ///
    /// ## 意义
    /// 上层代码会用这些前缀字符串去枚举形如 `PGD_COMPUTE_*` 的环境
    /// 变量；前缀错了会静默丢失大量配置。
    #[test]
    fn test_prefix_constants_match_history() {
        assert_eq!(runtime::compute::PREFIX, "PGD_COMPUTE_");
        assert_eq!(kvbm::nixl::PREFIX, "PGD_KVBM_NIXL_BACKEND_");
        assert_eq!(llm::metrics::HISTOGRAM_PREFIX, "PGD_HISTOGRAM_");
    }
}
