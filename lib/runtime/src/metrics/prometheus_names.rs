// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # Prometheus 命名常量与名字净化
//!
//! ## 设计意图
//!
//! 项目里所有 Prometheus 指标/标签名都集中收口在本模块，避免散落在各 crate
//! 的字面量字符串造成命名漂移、Python 绑定漂移。同时提供一组小型工具：
//! 在跨进程或动态拼接场景下把任意字符串“净化”到 Prometheus 规范允许的字符集。
//!
//! ## 外部契约
//!
//! - `name_prefix` / `labels` / `servicegroup_names`：固定前缀与公共标签。
//! - `frontend_service` / `work_handler` / `tokio_perf` / `request_plane` /
//!   `transport` / `routing_overhead` / `router` 等子模块：按功能域聚合的
//!   指标名常量；所有常量是公开 API，**改名属于 ABI 破坏性变更**。
//! - 工具函数：
//!   * [`sanitize_prometheus_name`] / [`sanitize_prometheus_label`] —— 把任意
//!     输入清洗到合法 Prometheus 名（metric 允许 `:`，label 不允许，且禁
//!     止 `__` 前缀）；
//!   * [`sanitize_frontend_prometheus_prefix`] —— 失败时回退到
//!     [`name_prefix::FRONTEND`]；
//!   * [`build_servicegroup_metric_name`] —— 给组件前缀拼指标名；
//!   * [`clamp_u64_to_i64`] —— 给 IntGauge 做安全截断。
//!
//! ## 命名约定（速查）
//!
//! 全局规则：`{prefix}_{name}_{suffix}`。后缀语义：
//! - 单位：`_seconds` / `_bytes` / `_ms` / `_percent` / `_messages` / `_connections`
//! - Counter：以 `_total` 结尾（**不要**写成前缀 `total_`）
//! - Gauge：无 `_total` 后缀
//! - 禁用模糊词：`_counter` / `_gauge` / `_time` / `_size`
//!
//! 详见 prometheus 官方约定。
//!
//! ## 实现要点
//!
//! - 净化函数**不依赖 regex**，改用 `char` 迭代器逐字符判定。
//!   只用栈上 buffer 与 `Vec::with_capacity(raw.len())`，无全局初始化开销，分支也更直白。
//! - 三套规则统一封装在 [`CharSpec`] 里：metric 允许 `[A-Za-z0-9_:]`，
//!   label 允许 `[A-Za-z0-9_]`，首字符额外要求字母或下划线。
//! - 全下划线结果一律视作“没有有效信息”→ 返回错误。
//! - label 净化额外去掉前导 `__`（Prometheus 保留给内部用），剩余再过一次
//!   首字符校正。
//!
//! ## Python 同步
//!
//! ⚠️ 修改任意常量后需要重生成 Python 绑定：
//! `cargo run -p pagoda-codegen --bin gen-python-prometheus-names`，
//! 它会更新 `lib/bindings/python/src/pagoda/prometheus_names.py`。

// =============================================================================
// === 前缀 / 标签 / 组件名 ====================================================
// =============================================================================

/// 指标前缀；详见模块级文档的命名约定。
pub mod name_prefix {
    /// servicegroup 作用域指标的前缀，自动携带 namespace/portname 标签。
    pub const SERVICEGROUP: &str = "pagoda_servicegroup";

    /// 前端 HTTP 服务指标的前缀（requests、TTFT、ITL、断连）。
    pub const FRONTEND: &str = "pagoda_frontend";

    /// KV 路由实例指标的前缀（携带 `router_id` 标签）。
    pub const ROUTER: &str = "pagoda_router";

    /// 独立 KV indexer 指标的前缀
    pub const KVINDEXER: &str = "pagoda_kvindexer";

    /// AddressedPushRouter 处 request-plane 指标的前缀。
    /// 与传输无关：衡量请求生命周期延迟与并发度
    /// （queue → send → roundtrip TTFT，inflight gauge）。
    pub const REQUEST_PLANE: &str = "pagoda_request_plane";

    /// 传输层指标的前缀（TCP / NATS）。
    /// 与协议相关：衡量线路级健康状况（发送/接收字节数、错误计数）。
    pub const TRANSPORT: &str = "pagoda_transport";

    /// work-handler 传输拆解指标的前缀（后端侧）
    pub const WORK_HANDLER: &str = "pagoda_work_handler";

    /// tokio 运行时指标的前缀（poll 耗时、队列深度、停顿）。
    pub const TOKIO: &str = "pagoda_tokio";

    /// 逐阶段路由开销延迟的前缀（哈希、调度）。
    /// 原始 Prometheus 指标，非 servicegroup 作用域。
    pub const ROUTING_OVERHEAD: &str = "pagoda_routing_overhead";
}

/// 由层级系统自动注入的公共标签。Python codegen 会同步导出这些常量。
pub mod labels {
    /// 用于标识 servicegroup 的标签
    pub const SERVICEGROUP: &str = "pagoda_servicegroup";

    /// 用于标识 namespace 的标签
    pub const NAMESPACE: &str = "pagoda_namespace";

    /// 用于标识 portname 的标签
    pub const PORTNAME: &str = "pagoda_portname";

    /// 用于 worker 数据并行 rank 的标签（非自动注入）。
    pub const DP_RANK: &str = "dp_rank";

    /// 用于 worker 实例 ID 的标签（etcd lease ID）。
    pub const WORKER_ID: &str = "worker_id";

    /// 用于模型名称/路径的标签（OpenAI API 标准，由 Pagoda 注入）
    pub const MODEL: &str = "model";

    /// 用于模型名称/路径的标签（引擎原生的替代项）。
    pub const MODEL_NAME: &str = "model_name";

    /// 用于 worker 类型的标签（例如 "aggregated"、"prefill"、"decode"、"encoder"）。
    pub const WORKER_TYPE: &str = "worker_type";

    /// 用于路由实例的标签（前端的 discovery.instance_id()）
    pub const ROUTER_ID: &str = "router_id";
}

/// 常见的 `pagoda_servicegroup` 标签取值。
pub mod servicegroup_names {
    /// KV 路由的 ServiceGroup 名称（前端侧请求路由）。
    pub const ROUTER: &str = "router";
}

// =============================================================================
// === Frontend HTTP service ===================================================
// =============================================================================

/// 前端服务指标（LLM HTTP 服务）
pub mod frontend_service {
    /// 用于覆盖默认指标前缀的环境变量
    pub const METRICS_PREFIX_ENV: &str = "PGD_METRICS_PREFIX";

    /// 已处理的 LLM 请求总数
    pub const REQUESTS_TOTAL: &str = "requests_total";

    /// 在收到首个响应前于 HTTP 队列中等待的请求数（gauge）
    pub const QUEUED_REQUESTS: &str = "queued_requests";

    /// 发往引擎的 inflight/并发请求数
    pub const INFLIGHT_REQUESTS: &str = "inflight_requests";

    /// 前端当前正在处理的请求数
    pub const ACTIVE_REQUESTS: &str = "active_requests";

    /// 断开连接的客户端数（gauge）
    pub const DISCONNECTED_CLIENTS: &str = "disconnected_clients";

    /// LLM 请求的耗时
    pub const REQUEST_DURATION_SECONDS: &str = "request_duration_seconds";

    /// 输入序列长度（token 数）
    pub const INPUT_SEQUENCE_TOKENS: &str = "input_sequence_tokens";

    /// 输出序列长度（token 数）
    pub const OUTPUT_SEQUENCE_TOKENS: &str = "output_sequence_tokens";

    /// 路由时预测的 KV 缓存命中率（0.0-1.0）
    pub const KV_HIT_RATE: &str = "kv_hit_rate";

    /// KV 缓存传输延迟的上界估计（分离式）
    pub const KV_TRANSFER_ESTIMATED_LATENCY_SECONDS: &str = "kv_transfer_estimated_latency_seconds";

    /// 共享缓存命中率（0.0-1.0）
    pub const SHARED_CACHE_HIT_RATE: &str = "shared_cache_hit_rate";

    /// 所选 worker 上超出设备重叠部分的共享缓存块数
    pub const SHARED_CACHE_BEYOND_BLOCKS: &str = "shared_cache_beyond_blocks";

    /// 每个请求的已缓存 token 数（前缀缓存命中）
    pub const CACHED_TOKENS: &str = "cached_tokens";

    /// Tokenizer 延迟（毫秒）
    pub const TOKENIZER_LATENCY_MS: &str = "tokenizer_latency_ms";

    /// 生成的输出 token 总数
    pub const OUTPUT_TOKENS_TOTAL: &str = "output_tokens_total";

    /// 首 token 耗时（秒）
    pub const TIME_TO_FIRST_TOKEN_SECONDS: &str = "time_to_first_token_seconds";

    /// token 间延迟（秒）
    pub const INTER_TOKEN_LATENCY_SECONDS: &str = "inter_token_latency_seconds";

    /// worker 可用的 KV 块总数
    pub const MODEL_TOTAL_KV_BLOCKS: &str = "model_total_kv_blocks";

    /// worker 的最大序列数（运行时配置）
    pub const MODEL_MAX_NUM_SEQS: &str = "model_max_num_seqs";

    /// worker 的最大批处理 token 数（运行时配置）
    pub const MODEL_MAX_NUM_BATCHED_TOKENS: &str = "model_max_num_batched_tokens";

    /// worker 的最大上下文长度（MDC）
    pub const MODEL_CONTEXT_LENGTH: &str = "model_context_length";

    /// worker 的 KV 缓存块大小（MDC）
    pub const MODEL_KV_CACHE_BLOCK_SIZE: &str = "model_kv_cache_block_size";

    /// worker 的请求迁移上限（MDC）
    pub const MODEL_MIGRATION_LIMIT: &str = "model_migration_limit";

    /// 因 worker 不可用而发生的请求迁移总数
    pub const MODEL_MIGRATION_TOTAL: &str = "model_migration_total";

    /// 因序列长度超过配置的 max_seq_len 上限
    /// 而禁用迁移的总次数
    pub const MODEL_MIGRATION_MAX_SEQ_LEN_EXCEEDED_TOTAL: &str =
        "model_migration_max_seq_len_exceeded_total";

    /// 请求取消总数
    pub const MODEL_CANCELLATION_TOTAL: &str = "model_cancellation_total";

    /// 因资源耗尽而被拒绝的请求总数
    pub const MODEL_REJECTION_TOTAL: &str = "model_rejection_total";

    /// 每个 worker 的活跃 decode 块数
    pub const WORKER_ACTIVE_DECODE_BLOCKS: &str = "worker_active_decode_blocks";

    /// 每个 worker 的活跃 prefill token 数
    pub const WORKER_ACTIVE_PREFILL_TOKENS: &str = "worker_active_prefill_tokens";

    /// 每个 worker 最近观测到的首 token 耗时（秒）
    pub const WORKER_LAST_TIME_TO_FIRST_TOKEN_SECONDS: &str =
        "worker_last_time_to_first_token_seconds";

    /// 每个 worker 最近观测到的输入序列 token 数
    pub const WORKER_LAST_INPUT_SEQUENCE_TOKENS: &str = "worker_last_input_sequence_tokens";

    /// 每个 worker 最近观测到的 token 间延迟（秒）
    pub const WORKER_LAST_INTER_TOKEN_LATENCY_SECONDS: &str =
        "worker_last_inter_token_latency_seconds";

    /// 路由器调度队列中待处理的请求数
    pub const ROUTER_QUEUE_PENDING_REQUESTS: &str = "router_queue_pending_requests";

    /// 为某个 LoRA adapter 分配的副本数
    pub const LORA_REPLICA_FACTOR: &str = "lora_replica_factor";

    /// 某个 LoRA adapter 是否正在接收流量
    pub const LORA_IS_ACTIVE: &str = "lora_is_active";

    /// 某个 LoRA adapter 的估计负载（窗口内请求计数）
    pub const LORA_ESTIMATED_LOAD: &str = "lora_estimated_load";

    /// 某个 LoRA adapter 的原始到达计数（窗口内速率计数器）
    pub const LORA_RAW_ARRIVAL_COUNT: &str = "lora_raw_arrival_count";

    /// 某个 LoRA adapter 的 in-flight（活跃）请求数
    pub const LORA_ACTIVE_REQUESTS: &str = "lora_active_requests";

    /// 迁移类型的标签名
    pub const MIGRATION_TYPE_LABEL: &str = "migration_type";

    /// tokenizer 操作的标签名
    pub const OPERATION_LABEL: &str = "operation";

    pub mod operation {
        pub const TOKENIZE: &str = "tokenize";
        pub const DETOKENIZE: &str = "detokenize";
    }

    pub mod migration_type {
        pub const NEW_REQUEST: &str = "new_request";
        pub const ONGOING_REQUEST: &str = "ongoing_request";
    }

    pub mod status {
        pub const SUCCESS: &str = "success";
        pub const ERROR: &str = "error";
    }

    pub mod request_type {
        pub const STREAM: &str = "stream";
        pub const UNARY: &str = "unary";
    }

    pub mod error_type {
        pub const NONE: &str = "";
        pub const VALIDATION: &str = "validation";
        pub const NOT_FOUND: &str = "not_found";
        pub const OVERLOAD: &str = "overload";
        pub const CANCELLED: &str = "cancelled";
        pub const RESPONSE_TIMEOUT: &str = "response_timeout";
        pub const INTERNAL: &str = "internal";
        pub const NOT_IMPLEMENTED: &str = "not_implemented";
    }
}

// =============================================================================
// === Work handler / Task tracker / DistributedRuntime ========================
// =============================================================================

/// work handler 的 Prometheus 指标名称
pub mod work_handler {
    pub const REQUESTS_TOTAL: &str = "requests_total";
    pub const REQUEST_BYTES_TOTAL: &str = "request_bytes_total";
    pub const RESPONSE_BYTES_TOTAL: &str = "response_bytes_total";
    pub const INFLIGHT_REQUESTS: &str = "inflight_requests";
    pub const REQUEST_DURATION_SECONDS: &str = "request_duration_seconds";
    pub const ERRORS_TOTAL: &str = "errors_total";
    pub const CANCELLATION_TOTAL: &str = "cancellation_total";
    pub const NETWORK_TRANSIT_SECONDS: &str = "network_transit_seconds";
    pub const TIME_TO_FIRST_RESPONSE_SECONDS: &str = "time_to_first_response_seconds";
    pub const QUEUE_DEPTH: &str = "queue_depth";
    pub const QUEUE_CAPACITY: &str = "queue_capacity";
    pub const ENQUEUE_REJECTED_TOTAL: &str = "enqueue_rejected_total";
    pub const PERMIT_WAIT_SECONDS: &str = "permit_wait_seconds";
    pub const POOL_ACTIVE_TASKS: &str = "pool_active_tasks";
    pub const POOL_CAPACITY: &str = "pool_capacity";
    pub const ERROR_TYPE_LABEL: &str = "error_type";

    pub mod error_types {
        pub const DESERIALIZATION: &str = "deserialization";
        pub const INVALID_MESSAGE: &str = "invalid_message";
        pub const RESPONSE_STREAM: &str = "response_stream";
        pub const GENERATE: &str = "generate";
        pub const PUBLISH_RESPONSE: &str = "publish_response";
        pub const PUBLISH_FINAL: &str = "publish_final";
    }
}

/// task tracker 的 Prometheus 指标名后缀
pub mod task_tracker {
    pub const TASKS_ISSUED_TOTAL: &str = "tasks_issued_total";
    pub const TASKS_STARTED_TOTAL: &str = "tasks_started_total";
    pub const TASKS_SUCCESS_TOTAL: &str = "tasks_success_total";
    pub const TASKS_CANCELLED_TOTAL: &str = "tasks_cancelled_total";
    pub const TASKS_FAILED_TOTAL: &str = "tasks_failed_total";
    pub const TASKS_REJECTED_TOTAL: &str = "tasks_rejected_total";
}

/// DistributedRuntime 核心指标
pub mod distributed_runtime {
    pub const UPTIME_SECONDS: &str = "uptime_seconds";
}

// =============================================================================
// === KVBM / Router / Routing overhead ========================================
// =============================================================================

/// KVBM
pub mod kvbm {
    pub const OFFLOAD_BLOCKS_D2H: &str = "offload_blocks_d2h";
    pub const OFFLOAD_BLOCKS_H2D: &str = "offload_blocks_h2d";
    pub const OFFLOAD_BLOCKS_D2D: &str = "offload_blocks_d2d";
    pub const ONBOARD_BLOCKS_H2D: &str = "onboard_blocks_h2d";
    pub const ONBOARD_BLOCKS_D2D: &str = "onboard_blocks_d2d";
    pub const MATCHED_TOKENS: &str = "matched_tokens";
    pub const HOST_CACHE_HIT_RATE: &str = "host_cache_hit_rate";
    pub const DISK_CACHE_HIT_RATE: &str = "disk_cache_hit_rate";
    pub const OBJECT_CACHE_HIT_RATE: &str = "object_cache_hit_rate";
    pub const OFFLOAD_BLOCKS_D2O: &str = "offload_blocks_d2o";
    pub const ONBOARD_BLOCKS_O2D: &str = "onboard_blocks_o2d";
    pub const OFFLOAD_BYTES_OBJECT: &str = "offload_bytes_object";
    pub const ONBOARD_BYTES_OBJECT: &str = "onboard_bytes_object";
    pub const OBJECT_READ_FAILURES: &str = "object_read_failures";
    pub const OBJECT_WRITE_FAILURES: &str = "object_write_failures";
}

/// 每个请求的路由指标（通过 `MetricsHierarchy` 进行 servicegroup 作用域化）。
pub mod router_request {
    /// 拼接在 `frontend_service::*` 名称前、用于组成路由指标名的前缀。
    pub const METRIC_PREFIX: &str = "router_";
}

/// 路由开销各阶段延迟直方图后缀。
pub mod routing_overhead {
    pub const BLOCK_HASHING_MS: &str = "overhead_block_hashing_ms";
    pub const INDEXER_FIND_MATCHES_MS: &str = "overhead_indexer_find_matches_ms";
    pub const SEQ_HASHING_MS: &str = "overhead_seq_hashing_ms";
    pub const SCHEDULING_MS: &str = "overhead_scheduling_ms";
    pub const TOTAL_MS: &str = "overhead_total_ms";
    pub const SHARED_CACHE_QUERY_MS: &str = "overhead_shared_cache_query_ms";
    pub const SHARED_CACHE_ERRORS_TOTAL: &str = "shared_cache_errors_total";
}

/// 路由请求指标（servicegroup 作用域的聚合直方图 + 计数器）
pub mod router {
    pub const REQUESTS_TOTAL: &str = "router_requests_total";
    pub const REMOTE_INDEXER_QUERY_FAILURES_TOTAL: &str =
        "router_remote_indexer_query_failures_total";
    pub const REMOTE_INDEXER_WRITE_FAILURES_TOTAL: &str =
        "router_remote_indexer_write_failures_total";
    pub const TIME_TO_FIRST_TOKEN_SECONDS: &str = "router_time_to_first_token_seconds";
    pub const INTER_TOKEN_LATENCY_SECONDS: &str = "router_inter_token_latency_seconds";
    pub const INPUT_SEQUENCE_TOKENS: &str = "router_input_sequence_tokens";
    pub const OUTPUT_SEQUENCE_TOKENS: &str = "router_output_sequence_tokens";
    pub const KV_HIT_RATE: &str = "router_kv_hit_rate";
    pub const SHARED_CACHE_HIT_RATE: &str = "router_shared_cache_hit_rate";
    pub const SHARED_CACHE_BEYOND_BLOCKS: &str = "router_shared_cache_beyond_blocks";
}

// =============================================================================
// === Frontend perf / Tokio perf / Indexer / Request plane / Transport ========
// =============================================================================

/// 前端流水线阶段与事件循环指标
pub mod frontend_perf {
    pub const STAGE_DURATION_SECONDS: &str = "stage_duration_seconds";
    pub const STAGE_REQUESTS: &str = "stage_requests";

    pub const STAGE_PREPROCESS: &str = "preprocess";
    pub const STAGE_ROUTE: &str = "route";
    pub const STAGE_DISPATCH: &str = "dispatch";

    pub const TOKENIZE_SECONDS: &str = "tokenize_seconds";
    pub const TEMPLATE_SECONDS: &str = "template_seconds";
    pub const DETOKENIZE_TOTAL_US: &str = "detokenize_total_us";
    pub const DETOKENIZE_TOKEN_COUNT: &str = "detokenize_token_count";
    pub const EVENT_LOOP_DELAY_SECONDS: &str = "event_loop_delay_seconds";
    pub const EVENT_LOOP_STALL_TOTAL: &str = "event_loop_stall_total";
}

/// tokio 运行时指标
pub mod tokio_perf {
    pub const WORKER_MEAN_POLL_TIME_NS: &str = "worker_mean_poll_time_ns";
    pub const GLOBAL_QUEUE_DEPTH: &str = "global_queue_depth";
    pub const BUDGET_FORCED_YIELD_TOTAL: &str = "budget_forced_yield_total";
    pub const WORKER_BUSY_RATIO: &str = "worker_busy_ratio";
    pub const WORKER_PARK_COUNT_TOTAL: &str = "worker_park_count_total";
    pub const WORKER_LOCAL_QUEUE_DEPTH: &str = "worker_local_queue_depth";
    pub const WORKER_STEAL_COUNT_TOTAL: &str = "worker_steal_count_total";
    pub const WORKER_OVERFLOW_COUNT_TOTAL: &str = "worker_overflow_count_total";
    pub const BLOCKING_THREADS: &str = "blocking_threads";
    pub const BLOCKING_IDLE_THREADS: &str = "blocking_idle_threads";
    pub const BLOCKING_QUEUE_DEPTH: &str = "blocking_queue_depth";
    pub const ALIVE_TASKS: &str = "alive_tasks";
}

/// 独立 KV indexer 的 HTTP 服务指标
pub mod kvindexer {
    pub const REQUEST_DURATION_SECONDS: &str = "request_duration_seconds";
    pub const REQUESTS_TOTAL: &str = "requests_total";
    pub const ERRORS_TOTAL: &str = "errors_total";
    pub const MODELS: &str = "models";
    pub const WORKERS: &str = "workers";
}

/// AddressedPushRouter 处的 request plane 指标
pub mod request_plane {
    pub const QUEUE_SECONDS: &str = "queue_seconds";
    pub const SEND_SECONDS: &str = "send_seconds";
    pub const ROUNDTRIP_TTFT_SECONDS: &str = "roundtrip_ttft_seconds";
    pub const INFLIGHT_REQUESTS: &str = "inflight_requests";
}

/// 特定传输方式的指标（TCP / NATS）
pub mod transport {
    pub mod tcp {
        pub const POOL_ACTIVE: &str = "tcp_pool_active";
        pub const POOL_IDLE: &str = "tcp_pool_idle";
        pub const BYTES_SENT_TOTAL: &str = "tcp_bytes_sent_total";
        pub const BYTES_RECEIVED_TOTAL: &str = "tcp_bytes_received_total";
        pub const ERRORS_TOTAL: &str = "tcp_errors_total";
        pub const SERVER_QUEUE_DEPTH: &str = "tcp_server_queue_depth";
    }
    pub mod nats {
        pub const ERRORS_TOTAL: &str = "nats_errors_total";
    }
}

/// KvRouter（含 KvIndexer）
pub mod kvrouter {
    pub const KV_CACHE_EVENTS_APPLIED: &str = "kv_cache_events_applied";
}

/// KV Publisher 指标
pub mod kv_publisher {
    pub const ENGINES_DROPPED_EVENTS_TOTAL: &str = "kv_publisher_engines_dropped_events_total";
    pub const ZMQ_EVENTS_TOTAL: &str = "kv_publisher_zmq_events_total";
    pub const ZMQ_FILTERED_EVENTS_TOTAL: &str = "kv_publisher_zmq_filtered_events_total";
    pub const ZMQ_CONVERSION_ISSUES_TOTAL: &str = "kv_publisher_zmq_conversion_issues_total";
    pub const ZMQ_SUSPICIOUS_EVENTS_TOTAL: &str = "kv_publisher_zmq_suspicious_events_total";
}

/// 引擎原生指标之外、额外补充的 TRT-LLM worker 指标。
pub mod trtllm_additional {
    pub const NUM_ABORTED_REQUESTS_TOTAL: &str = "trtllm_num_aborted_requests_total";
    pub const REQUEST_TYPE_IMAGE_TOTAL: &str = "trtllm_request_type_image_total";
    pub const REQUEST_TYPE_STRUCTURED_OUTPUT_TOTAL: &str =
        "trtllm_request_type_structured_output_total";
    pub const KV_TRANSFER_SUCCESS_TOTAL: &str = "trtllm_kv_transfer_success_total";
    pub const KV_TRANSFER_LATENCY_SECONDS: &str = "trtllm_kv_transfer_latency_seconds";
    pub const KV_TRANSFER_BYTES: &str = "trtllm_kv_transfer_bytes";
    pub const KV_TRANSFER_SPEED_GB_S: &str = "trtllm_kv_transfer_speed_gb_s";
}

/// KV 缓存统计指标
pub mod kvstats {
    pub const TOTAL_BLOCKS: &str = "total_blocks";
    pub const GPU_CACHE_USAGE_PERCENT: &str = "gpu_cache_usage_percent";
}

/// 模型信息指标
pub mod model_info {
    pub const LOAD_TIME_SECONDS: &str = "model_load_time_seconds";
}

// =============================================================================
// === 名字净化（无 regex 实现）================================================
// =============================================================================

/// 不同 Prometheus 元素的字符集判定。
///
/// metric 名允许冒号，label 名不允许；二者首字符都必须是字母或下划线。
#[derive(Clone, Copy)]
struct CharSpec {
    allow_colon: bool,
}

impl CharSpec {
    const METRIC: Self = Self { allow_colon: true };
    const LABEL: Self = Self { allow_colon: false };

    /// 内部字符是否合法。
    fn body_ok(self, c: char) -> bool {
        c.is_ascii_alphanumeric() || c == '_' || (self.allow_colon && c == ':')
    }

    /// 首字符是否合法（数字与冒号一律不允许）。
    fn first_ok(c: char) -> bool {
        c.is_ascii_alphabetic() || c == '_'
    }
}

/// 通用清洗骨架：非法字符 → `_`，首字符不合法 → 前补 `_`。
/// **不**处理“全下划线”这一终态——交给调用方按 metric/label 差异决定。
fn scrub(raw: &str, spec: CharSpec) -> String {
    let mut out = String::with_capacity(raw.len() + 1);
    for c in raw.chars() {
        out.push(if spec.body_ok(c) { c } else { '_' });
    }
    let needs_prefix = out
        .chars()
        .next()
        .map(|c| !CharSpec::first_ok(c))
        .unwrap_or(true);
    if needs_prefix {
        out.insert(0, '_');
    }
    out
}

fn all_underscores(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c == '_')
}

/// 把任意字符串清洗为合法 Prometheus 指标名。
///
/// **规则**：`[a-zA-Z_:][a-zA-Z0-9_:]*`，允许 `:` 与 `__`。
pub fn sanitize_prometheus_name(raw: &str) -> anyhow::Result<String> {
    if raw.is_empty() {
        anyhow::bail!("Cannot sanitize empty string into valid Prometheus name");
    }
    let scrubbed = scrub(raw, CharSpec::METRIC);
    if all_underscores(&scrubbed) {
        anyhow::bail!(
            "Input '{}' contains only invalid characters and cannot be sanitized into a valid Prometheus name",
            raw
        );
    }
    Ok(scrubbed)
}

/// 把任意字符串清洗为合法 Prometheus 标签名。
///
/// **规则**：`[a-zA-Z_][a-zA-Z0-9_]*`，不允许 `:`；不允许 `__` 开头
/// （前缀被 Prometheus 预留）。
pub fn sanitize_prometheus_label(raw: &str) -> anyhow::Result<String> {
    if raw.is_empty() {
        anyhow::bail!("Cannot sanitize empty string into valid Prometheus label");
    }
    let mut scrubbed = scrub(raw, CharSpec::LABEL);

    // 反复剥离 `__` 前缀直到首字符合法或耗尽。
    if scrubbed.starts_with("__") {
        scrubbed.drain(..2);
        let needs_prefix = scrubbed
            .chars()
            .next()
            .map(|c| !c.is_ascii_alphabetic())
            .unwrap_or(true);
        if needs_prefix {
            scrubbed.insert(0, '_');
        }
    }

    if all_underscores(&scrubbed) {
        anyhow::bail!(
            "Input '{}' contains only invalid characters and cannot be sanitized into a valid Prometheus label",
            raw
        );
    }
    Ok(scrubbed)
}

/// Frontend 前缀清洗：失败/空输入回退到 [`name_prefix::FRONTEND`]。
pub fn sanitize_frontend_prometheus_prefix(raw: &str) -> String {
    if raw.is_empty() {
        return name_prefix::FRONTEND.to_string();
    }
    sanitize_prometheus_name(raw).unwrap_or_else(|_| name_prefix::FRONTEND.to_string())
}

/// 组件指标全名：`pagoda_servicegroup_{sanitized(metric_name)}`。
///
/// 清洗失败直接 panic——调用方按设计应保证 metric_name 至少包含一个合法字符。
pub fn build_servicegroup_metric_name(metric_name: &str) -> String {
    let sanitized =
        sanitize_prometheus_name(metric_name).expect("metric name should be valid or sanitizable");
    let mut out = String::with_capacity(name_prefix::SERVICEGROUP.len() + 1 + sanitized.len());
    out.push_str(name_prefix::SERVICEGROUP);
    out.push('_');
    out.push_str(&sanitized);
    out
}

/// 把 `u64` 安全地截断到 `i64` 范围内，供 IntGauge 使用。
///
/// # Examples
/// ```
/// use pagoda_runtime::metrics::prometheus_names::clamp_u64_to_i64;
///
/// assert_eq!(clamp_u64_to_i64(100), 100);
/// assert_eq!(clamp_u64_to_i64(u64::MAX), i64::MAX);
/// ```
pub fn clamp_u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

// =============================================================================
// === 单元测试 ================================================================
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_frontend_prometheus_prefix() {
        assert_eq!(
            sanitize_frontend_prometheus_prefix("pagoda_frontend"),
            "pagoda_frontend"
        );
        assert_eq!(
            sanitize_frontend_prometheus_prefix("custom_prefix"),
            "custom_prefix"
        );
        assert_eq!(sanitize_frontend_prometheus_prefix("test123"), "test123");

        assert_eq!(sanitize_frontend_prometheus_prefix("test prefix"), "test_prefix");
        assert_eq!(sanitize_frontend_prometheus_prefix("test.prefix"), "test_prefix");
        assert_eq!(sanitize_frontend_prometheus_prefix("test@prefix"), "test_prefix");
        assert_eq!(sanitize_frontend_prometheus_prefix("test-prefix"), "test_prefix");

        assert_eq!(sanitize_frontend_prometheus_prefix("123test"), "_123test");
        assert_eq!(sanitize_frontend_prometheus_prefix("@test"), "_test");

        assert_eq!(sanitize_frontend_prometheus_prefix(""), name_prefix::FRONTEND);
    }

    #[test]
    fn test_sanitize_prometheus_name() {
        assert_eq!(sanitize_prometheus_name("valid_name").unwrap(), "valid_name");
        assert_eq!(sanitize_prometheus_name("test123").unwrap(), "test123");
        assert_eq!(sanitize_prometheus_name("test_name_123").unwrap(), "test_name_123");
        assert_eq!(sanitize_prometheus_name("test:name").unwrap(), "test:name");

        assert_eq!(sanitize_prometheus_name("test name").unwrap(), "test_name");
        assert_eq!(sanitize_prometheus_name("test.name").unwrap(), "test_name");
        assert_eq!(sanitize_prometheus_name("test@name").unwrap(), "test_name");
        assert_eq!(sanitize_prometheus_name("test-name").unwrap(), "test_name");
        assert_eq!(sanitize_prometheus_name("test$name#123").unwrap(), "test_name_123");

        assert_eq!(sanitize_prometheus_name("test__name").unwrap(), "test__name");
        assert_eq!(sanitize_prometheus_name("test___name").unwrap(), "test___name");
        assert_eq!(sanitize_prometheus_name("__test").unwrap(), "__test");

        assert_eq!(sanitize_prometheus_name("123test").unwrap(), "_123test");
        assert_eq!(sanitize_prometheus_name("@test").unwrap(), "_test");
        assert_eq!(sanitize_prometheus_name("-test").unwrap(), "_test");
        assert_eq!(sanitize_prometheus_name(".test").unwrap(), "_test");

        assert!(sanitize_prometheus_name("").is_err());

        assert_eq!(
            sanitize_prometheus_name("123.test-name@domain").unwrap(),
            "_123_test_name_domain"
        );

        assert!(sanitize_prometheus_name("@#$%").is_err());
        assert!(sanitize_prometheus_name("!!!!").is_err());
    }

    #[test]
    fn test_sanitize_prometheus_label() {
        assert_eq!(sanitize_prometheus_label("valid_label").unwrap(), "valid_label");
        assert_eq!(sanitize_prometheus_label("test123").unwrap(), "test123");
        assert_eq!(sanitize_prometheus_label("test_label_123").unwrap(), "test_label_123");

        assert_eq!(sanitize_prometheus_label("test:label").unwrap(), "test_label");

        assert_eq!(sanitize_prometheus_label("test label").unwrap(), "test_label");
        assert_eq!(sanitize_prometheus_label("test.label").unwrap(), "test_label");
        assert_eq!(sanitize_prometheus_label("test@label").unwrap(), "test_label");
        assert_eq!(sanitize_prometheus_label("test-label").unwrap(), "test_label");
        assert_eq!(
            sanitize_prometheus_label("test$label#123").unwrap(),
            "test_label_123"
        );

        assert_eq!(sanitize_prometheus_label("test__label").unwrap(), "test__label");
        assert_eq!(sanitize_prometheus_label("test___label").unwrap(), "test___label");
        assert_eq!(sanitize_prometheus_label("test____label").unwrap(), "test____label");
        assert_eq!(sanitize_prometheus_label("__test").unwrap(), "test");
        assert!(sanitize_prometheus_label("____").is_err());

        assert_eq!(sanitize_prometheus_label("123test").unwrap(), "_123test");
        assert_eq!(sanitize_prometheus_label("@test").unwrap(), "_test");
        assert_eq!(sanitize_prometheus_label(":test").unwrap(), "_test");
        assert_eq!(sanitize_prometheus_label("-test").unwrap(), "_test");

        assert!(sanitize_prometheus_label("").is_err());

        assert_eq!(
            sanitize_prometheus_label("123:test-label@domain").unwrap(),
            "_123_test_label_domain"
        );

        assert!(sanitize_prometheus_label("@#$%").is_err());
        assert!(sanitize_prometheus_label("!!!!").is_err());
    }

    #[test]
    fn test_build_servicegroup_metric_name() {
        assert_eq!(
            build_servicegroup_metric_name("test_metric"),
            "pagoda_servicegroup_test_metric"
        );
        assert_eq!(
            build_servicegroup_metric_name("requests_total"),
            "pagoda_servicegroup_requests_total"
        );

        assert_eq!(
            build_servicegroup_metric_name("test metric"),
            "pagoda_servicegroup_test_metric"
        );
        assert_eq!(
            build_servicegroup_metric_name("test.metric"),
            "pagoda_servicegroup_test_metric"
        );
        assert_eq!(
            build_servicegroup_metric_name("test@metric"),
            "pagoda_servicegroup_test_metric"
        );

        assert_eq!(
            build_servicegroup_metric_name("123metric"),
            "pagoda_servicegroup__123metric"
        );
    }

    #[test]
    #[should_panic(expected = "metric name should be valid or sanitizable")]
    fn test_build_servicegroup_metric_name_panics_on_invalid_input() {
        build_servicegroup_metric_name("@#$%");
    }

    #[test]
    #[should_panic(expected = "metric name should be valid or sanitizable")]
    fn test_build_servicegroup_metric_name_panics_on_empty_input() {
        build_servicegroup_metric_name("");
    }

    #[test]
    fn test_clamp_u64_to_i64() {
        assert_eq!(clamp_u64_to_i64(0), 0);
        assert_eq!(clamp_u64_to_i64(100), 100);
        assert_eq!(clamp_u64_to_i64(1000000), 1000000);

        assert_eq!(clamp_u64_to_i64(i64::MAX as u64), i64::MAX);

        assert_eq!(clamp_u64_to_i64(u64::MAX), i64::MAX);
        assert_eq!(clamp_u64_to_i64((i64::MAX as u64) + 1), i64::MAX);
        assert_eq!(clamp_u64_to_i64((i64::MAX as u64) + 1000), i64::MAX);
    }

    /// ## 测试过程
    /// 覆盖指标名边界：合法冒号+双下划线、前导冒号需补 `_`、空白被替换、
    /// 斜杠被替换；空串与全无效串返回错误。
    /// ## 意义
    /// 锁定 metric 名规则与错误语义。
    #[test]
    fn test_supplemental_sanitize_prometheus_name_edge_cases() {
        assert_eq!(
            sanitize_prometheus_name("queue:depth__current").unwrap(),
            "queue:depth__current"
        );
        assert_eq!(
            sanitize_prometheus_name(":leading_colon").unwrap(),
            "_:leading_colon"
        );
        assert_eq!(sanitize_prometheus_name(" name ").unwrap(), "_name_");
        assert_eq!(
            sanitize_prometheus_name("metric/with/slashes").unwrap(),
            "metric_with_slashes"
        );

        let empty_err = sanitize_prometheus_name("").unwrap_err().to_string();
        assert!(empty_err.contains("Cannot sanitize empty string"));

        let invalid_err = sanitize_prometheus_name("___").unwrap_err().to_string();
        assert!(invalid_err.contains("contains only invalid characters"));

        let punctuation_err = sanitize_prometheus_name("!!!").unwrap_err().to_string();
        assert!(punctuation_err.contains("contains only invalid characters"));
    }

    /// ## 测试过程
    /// 覆盖标签名边界：前导 `_`、`__` 剥离、`___` 剥离 2 个后剩 `_` + 数字、
    /// 斜杠替换；空串、单 `_`、全冒号返回错误。
    /// ## 意义
    /// 锁定 label 比 metric 更严的“无冒号 + 无 `__` 前缀”规则。
    #[test]
    fn test_supplemental_sanitize_prometheus_label_edge_cases() {
        assert_eq!(sanitize_prometheus_label("_valid").unwrap(), "_valid");
        assert_eq!(
            sanitize_prometheus_label("__label__suffix").unwrap(),
            "label__suffix"
        );
        assert_eq!(sanitize_prometheus_label("__1label").unwrap(), "_1label");
        assert_eq!(sanitize_prometheus_label("___1label").unwrap(), "__1label");
        assert_eq!(
            sanitize_prometheus_label("label/with/slashes").unwrap(),
            "label_with_slashes"
        );

        let empty_err = sanitize_prometheus_label("").unwrap_err().to_string();
        assert!(empty_err.contains("Cannot sanitize empty string"));

        let underscore_err = sanitize_prometheus_label("_").unwrap_err().to_string();
        assert!(underscore_err.contains("contains only invalid characters"));

        let punctuation_err = sanitize_prometheus_label(":::").unwrap_err().to_string();
        assert!(punctuation_err.contains("contains only invalid characters"));
    }

    /// ## 测试过程
    /// 验证 frontend 前缀清洗的成功 / 替换 / 回退三种路径。
    /// ## 意义
    /// 保证业务侧设置错前缀时不会让指标系统崩溃，而是稳定回退到默认名。
    #[test]
    fn test_supplemental_sanitize_frontend_prometheus_prefix_fallbacks() {
        assert_eq!(sanitize_frontend_prometheus_prefix("frontend-1"), "frontend_1");
        assert_eq!(sanitize_frontend_prometheus_prefix(":router"), "_:router");
        assert_eq!(sanitize_frontend_prometheus_prefix("!!!"), name_prefix::FRONTEND);
        assert_eq!(sanitize_frontend_prometheus_prefix("___"), name_prefix::FRONTEND);
    }

    /// ## 测试过程
    /// 组件指标名拼接 + clamp 的临界值（MAX-1 / MAX / MAX+2）。
    /// ## 意义
    /// 一次性把组件命名与 u64→i64 截断这两段独立逻辑的临界行为锁住。
    #[test]
    fn test_supplemental_build_servicegroup_metric_name_and_clamp_boundaries() {
        assert_eq!(
            build_servicegroup_metric_name("queue:depth__current"),
            "pagoda_servicegroup_queue:depth__current"
        );
        assert_eq!(
            build_servicegroup_metric_name(":leading"),
            "pagoda_servicegroup__:leading"
        );
        assert_eq!(
            build_servicegroup_metric_name("metric/with/slashes"),
            "pagoda_servicegroup_metric_with_slashes"
        );

        assert_eq!(clamp_u64_to_i64((i64::MAX as u64) - 1), i64::MAX - 1);
        assert_eq!(clamp_u64_to_i64(i64::MAX as u64), i64::MAX);
        assert_eq!(clamp_u64_to_i64((i64::MAX as u64) + 2), i64::MAX);
    }
}
