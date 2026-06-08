// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-License-Identifier: Apache-2.0

//! # mocker 协议与配置类型
//!
//! ## 设计意图
//! 汇集调度器与 KV 管理层共用的数据契约：块移动信号、KV 事件发布抽象、前向度量快照、
//! prefill 成本、输出信号，以及核心配置 [`MockEngineArgs`] 及其 JSON 解析。
//!
//! ## 外部契约
//! - 所有公开类型/字段/枚举变体名、`serde` 标签与默认值、`derive_builder` 默认值、
//!   `validator` 约束、以及 JSON 形态均保持稳定。
//! - [`MockEngineArgs::from_json_str`] 的合法字段集合、错误串、归一化行为
//!   （`block_size == 0` 归一为 64，校验失败信息）与上游一致。
//!
//! ## 实现要点
//! - 事件发布以 trait object 形式抽象运行时依赖，使 mocker 组件保持通用。
//! - 配置仅保留单引擎（vLLM）与单淘汰策略（LRU），并预留扩展位。

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use derive_builder::Builder;
use pagoda_kv_router::config::RouterQueuePolicy;
use pagoda_kv_router::protocols::{KvCacheEvent, StorageTier};
use pagoda_tokens::blocks::UniqueBlock;
use pagoda_tokens::{BlockHash, PositionalLineageHash, SequenceHash, Token};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use validator::Validate;

use crate::common::perf_model::PerfModel;

// === SECTION: 淘汰策略 ===

/// 本地 inactive KV 缓存的淘汰策略。
///
/// 第一阶段仅实现最简单的 LRU 策略；保留枚举形态以便后续扩展更多策略，
/// 当前唯一取值 `Lru` 同时是默认值。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub enum MockerEvictionBackend {
    #[default]
    Lru,
}

// === SECTION: KV 事件发布 ===

/// 发布 KV 缓存事件的 trait。抽象运行时依赖，使 mocker 组件保持通用。
pub trait KvCacheEventSink: Send + Sync {
    fn publish(&self, event: KvCacheEvent) -> anyhow::Result<()>;

    fn publish_with_storage_tier(
        &self,
        event: KvCacheEvent,
        _storage_tier: StorageTier,
    ) -> anyhow::Result<()> {
        self.publish(event)
    }
}

/// 传输层相关发布器（如 vLLM 原生 ZMQ 事件流）使用的原始 KV 事件载荷。
#[derive(Debug, Clone)]
pub struct RawKvEvent {
    pub event: KvCacheEvent,
    pub block_token_ids: Option<Vec<Vec<u32>>>,
    pub storage_tier: StorageTier,
}

/// 发布传输层相关原始 KV 事件载荷的 trait。
pub trait RawKvEventSink: Send + Sync {
    fn publish(&self, event: RawKvEvent) -> anyhow::Result<()>;
}

/// 调度器与 KV 管理层共用的 KV 事件发布器集合。
#[derive(Clone, Default)]
pub struct KvEventPublishers {
    event_sink: Option<Arc<dyn KvCacheEventSink>>,
    raw_sink: Option<Arc<dyn RawKvEventSink>>,
}

impl KvEventPublishers {
    pub fn new(
        event_sink: Option<Arc<dyn KvCacheEventSink>>,
        raw_sink: Option<Arc<dyn RawKvEventSink>>,
    ) -> Self {
        Self {
            event_sink,
            raw_sink,
        }
    }

    pub fn raw_enabled(&self) -> bool {
        self.raw_sink.is_some()
    }

    pub fn is_empty(&self) -> bool {
        self.event_sink.is_none() && self.raw_sink.is_none()
    }

    pub fn publish(
        &self,
        event: KvCacheEvent,
        block_token_ids: Option<&[Vec<u32>]>,
    ) -> anyhow::Result<()> {
        self.publish_with_storage_tier(event, block_token_ids, StorageTier::Device)
    }

    pub fn publish_with_storage_tier(
        &self,
        event: KvCacheEvent,
        block_token_ids: Option<&[Vec<u32>]>,
        storage_tier: StorageTier,
    ) -> anyhow::Result<()> {
        if let Some(sink) = self.event_sink.as_ref() {
            sink.publish_with_storage_tier(event.clone(), storage_tier)?;
        }

        if let Some(sink) = self.raw_sink.as_ref() {
            sink.publish(RawKvEvent {
                event,
                block_token_ids: block_token_ids.map(|token_ids| token_ids.to_vec()),
                storage_tier,
            })?;
        }

        Ok(())
    }
}

// === SECTION: 前向度量快照 ===

/// 每轮前向计算的度量快照，对应 Python 中
/// `components/src/dynamo/common/forward_pass_metrics.py` 的 `ForwardPassMetrics`。
///
/// 由调度器核心在每次 `execute_pass_internal()` 后产出。运行时依赖层（`lib/llm`）
/// 会补充身份字段（worker_id、dp_rank、counter_id），并序列化为 msgpack 送入事件面。
#[derive(Debug, Clone, Default)]
pub struct ForwardPassSnapshot {
    // -- 本轮已调度（执行）的请求 --
    pub num_prefill_requests: u32,
    pub sum_prefill_tokens: u64,
    pub var_prefill_length: f64,
    pub sum_prefill_kv_tokens: u64,
    pub num_decode_requests: u32,
    pub sum_decode_kv_tokens: u64,
    pub var_decode_kv_tokens: f64,
    // -- 排队（等待，未调度）的请求 --
    pub num_queued_prefill: u32,
    pub sum_queued_prefill_tokens: u64,
    pub var_queued_prefill_length: f64,
    pub num_queued_decode: u32,
    pub sum_queued_decode_kv_tokens: u64,
    pub var_queued_decode_kv_tokens: f64,
    // -- 计时 --
    pub wall_time_secs: f64,
}

/// 发布前向度量快照的 trait，抽象 FPM 发布管线使调度器保持通用。
pub trait FpmSink: Send + Sync {
    fn publish(&self, snapshot: ForwardPassSnapshot) -> anyhow::Result<()>;
}

/// 调度器使用的可选 FPM 发布器，包装 `Option<Arc<dyn FpmSink>>` 以便传递与空操作默认行为。
#[derive(Clone, Default)]
pub struct FpmPublisher {
    sink: Option<Arc<dyn FpmSink>>,
}

impl FpmPublisher {
    pub fn new(sink: Option<Arc<dyn FpmSink>>) -> Self {
        Self { sink }
    }

    pub fn publish(&self, snapshot: ForwardPassSnapshot) -> anyhow::Result<()> {
        if let Some(sink) = &self.sink {
            sink.publish(snapshot)?;
        }
        Ok(())
    }
}

pub type NumBlocks = usize;

// === SECTION: 块移动信号 ===

/// 缓存中的不同块移动操作。
/// 对 Use 与 Promote 变体附带块哈希，用于 KV 事件发布。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MoveBlock {
    Use(
        Vec<UniqueBlock>,
        Vec<BlockHash>,
        Vec<PositionalLineageHash>,
        Option<Vec<Vec<u32>>>,
        Option<UniqueBlock>,
    ),
    Deref(Vec<UniqueBlock>),
    Promote(
        Uuid,
        SequenceHash,
        Option<u64>,
        BlockHash,
        PositionalLineageHash,
        Option<Vec<u32>>,
    ),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MoveBlockResponse {
    Store(Vec<SequenceHash>, Option<u64>),
    Remove(Vec<SequenceHash>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectRequest {
    pub tokens: Vec<Token>,
    pub max_output_tokens: usize,
    pub uuid: Option<Uuid>,
    pub dp_rank: u32,
    pub arrival_timestamp_ms: Option<f64>,
}

// === SECTION: prefill 成本与输出信号 ===

/// 缓存中预填充内容的成本。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrefillCost {
    pub new_blocks: usize,
    pub new_tokens: usize,
    /// 已缓存（前缀命中）的 token 数。isl = cached_tokens + new_tokens。
    pub cached_tokens: usize,
}

impl PrefillCost {
    pub fn predict_prefill_compute(
        &self,
        new_tokens: Option<usize>,
        perf_model: &PerfModel,
    ) -> f64 {
        let tokens = new_tokens.unwrap_or(self.new_tokens);
        let isl = self.cached_tokens + tokens;
        perf_model.predict_prefill_time(1, isl, self.cached_tokens)
    }
}

/// 带完成状态的输出 token 生成信号。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputSignal {
    pub uuid: Uuid,
    pub completed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handoff_delay_ms: Option<f64>,
}

// === SECTION: 抢占 / 引擎 / worker 枚举 ===

/// 内存压力下驱逐 decode 请求的抢占策略。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum PreemptionMode {
    /// 驱逐最新的请求（与 vLLM v1 默认一致）。
    #[default]
    Lifo,
    /// 驱逐最旧的请求。
    Fifo,
}

/// 用于选择调度与 KV 缓存模拟行为的引擎类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum EngineType {
    /// vLLM 风格调度，基于哈希的块 KV 缓存。
    #[default]
    Vllm,
}

/// 分离式部署配置下的 worker 类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum WorkerType {
    /// 同时处理 prefill 与 decode 的标准聚合 worker。
    #[default]
    Aggregated,
    /// 分离式模式下的专用 prefill worker。
    Prefill,
    /// 分离式模式下的专用 decode worker。
    Decode,
}

// === SECTION: 推理（thinking）配置 ===

/// mocker 中推理/思考 token 输出的配置。
///
/// 设置后，mocker 会将每个响应的首段包裹在思考边界 token 中：
/// `[start_token, random..., end_token, random...]`。
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct ReasoningConfig {
    pub start_thinking_token_id: u32,
    pub end_thinking_token_id: u32,
    #[validate(range(min = 0.0, max = 1.0))]
    pub thinking_ratio: f64,
}

impl ReasoningConfig {
    /// 给定 osl 时思考 token 数（含 start/end 边界）。
    /// osl < 2 时返回 0（思考关闭），否则夹取到 [2, osl]。
    pub fn num_thinking_tokens(&self, max_output_tokens: usize) -> usize {
        if max_output_tokens < 2 {
            return 0;
        }
        let raw = (max_output_tokens as f64 * self.thinking_ratio).floor() as usize;
        if raw == 0 {
            return 0;
        }
        raw.max(2).min(max_output_tokens)
    }

    /// 思考块之后的响应 token 数。
    pub fn num_response_tokens(&self, max_output_tokens: usize) -> usize {
        max_output_tokens.saturating_sub(self.num_thinking_tokens(max_output_tokens))
    }
}

// === SECTION: MockEngineArgs 配置 ===

/// MockEngine 的配置参数。
#[derive(Debug, Clone, Serialize, Deserialize, Builder, Validate)]
#[builder(pattern = "owned", build_fn(public))]
pub struct MockEngineArgs {
    /// 引擎类型：vLLM 或 SGLang 模拟。
    #[builder(default = "EngineType::Vllm")]
    pub engine_type: EngineType,

    #[builder(default = "16384")]
    #[validate(range(min = 1))]
    pub num_gpu_blocks: usize,

    #[builder(default = "0")]
    pub block_size: usize,

    // 历史上曾为 1024，后回退为 256
    #[builder(default = Some(256))]
    #[validate(range(min = 1))]
    pub max_num_seqs: Option<usize>,

    // open api server 的默认值；llm 类则为 16384
    #[builder(default = Some(8192))]
    #[validate(range(min = 1))]
    pub max_num_batched_tokens: Option<usize>,

    #[builder(default = true)]
    pub enable_prefix_caching: bool,

    #[builder(default = true)]
    pub enable_chunked_prefill: bool,

    #[builder(default = "1.0")]
    #[validate(range(min = 0.0))]
    pub speedup_ratio: f64,

    /// 仅作用于 decode 步的额外加速倍率。
    /// 建模投机解码（如 Eagle）：在不影响 prefill 时延的前提下提升 decode 吞吐。
    /// 有效 decode 加速为 `speedup_ratio * decode_speedup_ratio`。
    #[builder(default = "1.0")]
    #[validate(range(min = 0.0))]
    pub decode_speedup_ratio: f64,

    #[builder(default = "1")]
    #[validate(range(min = 1))]
    pub dp_size: u32,

    /// 可选启动耗时（秒），用于模拟引擎初始化延迟。
    #[builder(default = "None")]
    #[validate(range(min = 0.0))]
    pub startup_time: Option<f64>,

    /// 分离式部署的 worker 类型（Aggregated、Prefill 或 Decode）。
    #[builder(default = "WorkerType::Aggregated")]
    pub worker_type: WorkerType,

    /// 用于物化 `perf_model` 的 planner profile NPZ 路径。
    #[builder(default = "None")]
    pub planner_profile_data: Option<PathBuf>,

    /// 时序预测的性能模型（不序列化，由 planner_profile_data 加载）。
    #[serde(skip)]
    #[builder(default = "Arc::new(PerfModel::default())")]
    pub perf_model: Arc<PerfModel>,

    /// 若设置，表示应使用直接 AIC SDK 调用。
    /// 值为后端名（如 "sglang"、"vllm"）。
    /// Python 层读取它并以 Aiconfigurator 回调覆盖 perf_model。
    #[serde(skip)]
    #[builder(default = "None")]
    pub aic_backend: Option<String>,

    /// AIC GPU 系统名（如 "h200_sxm"）。设置 aic_backend 时必填。
    #[serde(skip)]
    #[builder(default = "None")]
    pub aic_system: Option<String>,

    /// AIC 后端引擎版本（如 vLLM 的 "0.12.0"、SGLang 的 "0.5.6.post2"）。
    /// 为 None 时使用该后端默认版本。
    #[serde(skip)]
    #[builder(default = "None")]
    pub aic_backend_version: Option<String>,

    /// AIC 时延预测的张量并行规模。仅影响 AIC 性能模型查表，不影响 mocker 调度。
    #[serde(skip)]
    #[builder(default = "None")]
    pub aic_tp_size: Option<usize>,

    /// AIC 时延预测的 HuggingFace 模型路径（如 "nvidia/Llama-3.1-8B-Instruct-FP8"）。
    #[serde(skip)]
    #[builder(default = "None")]
    pub aic_model_path: Option<String>,

    /// AIC 时延预测的 MoE 张量并行规模（如纯 MoE-TP 时为 4）。
    /// MoE 模型必填；须满足：aic_tp_size * aic_attention_dp_size == aic_moe_tp_size * aic_moe_ep_size。
    #[serde(skip)]
    #[builder(default = "None")]
    pub aic_moe_tp_size: Option<usize>,

    /// AIC 时延预测的 MoE 专家并行规模（如纯 EP 时为 4）。
    /// MoE 模型必填；须满足：aic_tp_size * aic_attention_dp_size == aic_moe_tp_size * aic_moe_ep_size。
    #[serde(skip)]
    #[builder(default = "None")]
    pub aic_moe_ep_size: Option<usize>,

    /// AIC 时延预测的注意力数据并行规模（默认 1）。
    /// 对应 AIC CLI 输出中的 `dp` 维度。
    /// 须满足：aic_tp_size * aic_attention_dp_size == aic_moe_tp_size * aic_moe_ep_size。
    #[serde(skip)]
    #[builder(default = "None")]
    pub aic_attention_dp_size: Option<usize>,

    /// 启用 worker 本地 KV indexer，用于追踪本 worker 自身的 KV 缓存状态。
    #[builder(default = "false")]
    pub enable_local_indexer: bool,

    /// 分离式部署 rendezvous 的 bootstrap 端口。
    /// prefill worker 监听此端口；decode worker 连接它。None 时禁用 bootstrap rendezvous。
    #[builder(default = "None")]
    pub bootstrap_port: Option<u16>,

    /// 每 token 的 KV 缓存字节数，由 Python CLI 从模型配置自动计算。
    /// 公式：num_layers * 2 * num_kv_heads * head_dim * dtype_bytes。
    #[builder(default = "None")]
    pub kv_bytes_per_token: Option<usize>,

    /// 分离式部署时延模拟用的 KV 缓存传输带宽（GB/s）。
    /// 默认 64.0（跨节点 InfiniBand）。设为 0 关闭 KV 传输延迟。节点内 NVLink 典型值约 450。
    #[builder(default = "None")]
    #[validate(range(min = 0.0))]
    pub kv_transfer_bandwidth: Option<f64>,

    /// 推理/思考 token 配置。设置后 mocker 用思考边界 token 包裹输出。
    #[builder(default = "None")]
    pub reasoning: Option<ReasoningConfig>,

    /// 以 vLLM 原生线格发布 KV 事件的 ZMQ 端口。
    /// 设置后调度器发布到 ZMQ PUB socket 而非直接发往 NATS；由 KvEventPublisher 中继订阅并转发。
    #[builder(default = "None")]
    pub zmq_kv_events_port: Option<u16>,

    /// 重放缓冲 KV 事件批次的 ZMQ ROUTER 端口。
    /// 与 `zmq_kv_events_port` 一同设置时，mocker 绑定 ROUTER socket，
    /// 按请求的序号回流缓冲批次。端口按 dp_rank 偏移（replay_port + dp_rank）。
    #[builder(default = "None")]
    pub zmq_replay_port: Option<u16>,

    /// 内存压力下 decode 驱逐的抢占模式。Lifo（默认）驱逐最新请求；Fifo 驱逐最旧请求。
    #[builder(default)]
    pub preemption_mode: PreemptionMode,

    /// 仅重放路径可选的 router 队列策略覆盖。
    #[builder(default = "None")]
    pub router_queue_policy: Option<RouterQueuePolicy>,
}

impl Default for MockEngineArgs {
    fn default() -> MockEngineArgs {
        MockEngineArgsBuilder::default()
            .build()
            .expect("Failed to build default MockEngineArgs")
            .normalized()
            .expect("Failed to normalize default MockEngineArgs")
    }
}

impl MockEngineArgs {
    const DEFAULT_VLLM_BLOCK_SIZE: usize = 64;

    pub fn builder() -> MockEngineArgsBuilder {
        MockEngineArgsBuilder::default()
    }

    pub fn normalized(mut self) -> anyhow::Result<Self> {
        match self.engine_type {
            EngineType::Vllm => {
                if self.block_size == 0 {
                    self.block_size = Self::DEFAULT_VLLM_BLOCK_SIZE;
                }
            }
        }

        self.validate()
            .map_err(|error| anyhow::anyhow!("Failed to validate MockEngineArgs: {error}"))?;
        if self.block_size == 0 {
            return Err(anyhow::anyhow!("block_size must be greater than 0"));
        }

        Ok(self)
    }

    pub fn is_prefill(&self) -> bool {
        self.worker_type == WorkerType::Prefill
    }

    pub fn is_decode(&self) -> bool {
        self.worker_type == WorkerType::Decode
    }

    pub fn needs_kv_publisher(&self) -> bool {
        self.enable_prefix_caching && !self.is_decode()
    }

    /// 从含额外引擎参数的 JSON 文件构造 MockEngineArgs。
    pub fn from_json_file(path: &Path) -> anyhow::Result<Self> {
        let file_content = std::fs::read_to_string(path)?;
        Self::from_json_str(&file_content)
    }

    pub fn from_json_str(content: &str) -> anyhow::Result<Self> {
        let mut builder = Self::builder();
        let extra_args: HashMap<String, serde_json::Value> = serde_json::from_str(content)?;

        // 合法字段名集合。
        let valid_fields: HashSet<&str> = [
            "engine_type",
            "num_gpu_blocks",
            "block_size",
            "max_num_seqs",
            "max_num_batched_tokens",
            "enable_prefix_caching",
            "enable_chunked_prefill",
            "speedup_ratio",
            "decode_speedup_ratio",
            "dp_size",
            "startup_time",
            "worker_type",
            "is_prefill",
            "is_decode",
            "planner_profile_data",
            "aic_backend",
            "aic_system",
            "aic_backend_version",
            "aic_tp_size",
            "aic_model_path",
            "aic_moe_tp_size",
            "aic_moe_ep_size",
            "aic_attention_dp_size",
            "enable_local_indexer",
            "bootstrap_port",
            "kv_bytes_per_token",
            "kv_transfer_bandwidth",
            "reasoning",
            "zmq_kv_events_port",
            "zmq_replay_port",
            "preemption_mode",
            "router_queue_policy",
            "has_perf_model",
        ]
        .iter()
        .cloned()
        .collect();

        // 检查非法参数。
        let invalid_args: Vec<String> = extra_args
            .keys()
            .filter(|key| !valid_fields.contains(key.as_str()))
            .cloned()
            .collect();

        if !invalid_args.is_empty() {
            return Err(anyhow::anyhow!(
                "Invalid arguments found in JSON file: {}. Valid arguments are: {:?}",
                invalid_args.join(", "),
                valid_fields
            ));
        }

        // 将每个额外参数应用到 builder。
        if let Some(value) = extra_args.get("engine_type")
            && let Some(s) = value.as_str()
        {
            let engine_type = match s {
                "vllm" => EngineType::Vllm,
                other => {
                    return Err(anyhow::anyhow!(
                        "Invalid engine_type '{}'. Must be 'vllm' or 'sglang'.",
                        other
                    ));
                }
            };
            builder = builder.engine_type(engine_type);
        }

        if let Some(value) = extra_args.get("num_gpu_blocks")
            && let Some(num) = value.as_u64()
        {
            builder = builder.num_gpu_blocks(num as usize);
        }

        if let Some(value) = extra_args.get("block_size")
            && let Some(num) = value.as_u64()
        {
            builder = builder.block_size(num as usize);
        }

        if let Some(value) = extra_args.get("max_num_seqs") {
            if value.is_null() {
                builder = builder.max_num_seqs(None);
            } else if let Some(num) = value.as_u64() {
                builder = builder.max_num_seqs(Some(num as usize));
            }
        }

        if let Some(value) = extra_args.get("max_num_batched_tokens") {
            if value.is_null() {
                builder = builder.max_num_batched_tokens(None);
            } else if let Some(num) = value.as_u64() {
                builder = builder.max_num_batched_tokens(Some(num as usize));
            }
        }

        if let Some(value) = extra_args.get("enable_prefix_caching")
            && let Some(enabled) = value.as_bool()
        {
            builder = builder.enable_prefix_caching(enabled);
        }

        if let Some(value) = extra_args.get("enable_chunked_prefill")
            && let Some(enabled) = value.as_bool()
        {
            builder = builder.enable_chunked_prefill(enabled);
        }

        if let Some(value) = extra_args.get("speedup_ratio")
            && let Some(num) = value.as_f64()
        {
            builder = builder.speedup_ratio(num);
        }

        if let Some(value) = extra_args.get("decode_speedup_ratio")
            && let Some(num) = value.as_f64()
        {
            builder = builder.decode_speedup_ratio(num);
        }

        if let Some(value) = extra_args.get("dp_size")
            && let Some(num) = value.as_u64()
        {
            builder = builder.dp_size(num as u32);
        }

        if let Some(value) = extra_args.get("startup_time")
            && let Some(num) = value.as_f64()
        {
            builder = builder.startup_time(Some(num));
        }

        if let Some(value) = extra_args.get("enable_local_indexer")
            && let Some(enabled) = value.as_bool()
        {
            builder = builder.enable_local_indexer(enabled);
        }

        if let Some(value) = extra_args.get("bootstrap_port")
            && let Some(port) = value.as_u64()
        {
            builder = builder.bootstrap_port(Some(port as u16));
        }

        if let Some(value) = extra_args.get("kv_bytes_per_token")
            && let Some(num) = value.as_u64()
        {
            builder = builder.kv_bytes_per_token(Some(num as usize));
        }

        if let Some(value) = extra_args.get("kv_transfer_bandwidth")
            && let Some(num) = value.as_f64()
        {
            builder = builder.kv_transfer_bandwidth(Some(num));
        }

        if let Some(value) = extra_args.get("reasoning")
            && !value.is_null()
        {
            let cfg: ReasoningConfig = serde_json::from_value(value.clone())
                .map_err(|e| anyhow::anyhow!("Failed to parse reasoning config: {}", e))?;
            builder = builder.reasoning(Some(cfg));
        }

        if let Some(value) = extra_args.get("zmq_kv_events_port")
            && let Some(port) = value.as_u64()
        {
            builder = builder.zmq_kv_events_port(Some(port as u16));
        }

        if let Some(value) = extra_args.get("zmq_replay_port")
            && let Some(port) = value.as_u64()
        {
            builder = builder.zmq_replay_port(Some(port as u16));
        }

        if let Some(value) = extra_args.get("preemption_mode")
            && let Some(mode_str) = value.as_str()
        {
            let mode = match mode_str {
                "lifo" => PreemptionMode::Lifo,
                "fifo" => PreemptionMode::Fifo,
                _ => {
                    return Err(anyhow::anyhow!(
                        "Invalid preemption_mode: '{}'. Must be 'lifo' or 'fifo'.",
                        mode_str
                    ));
                }
            };
            builder = builder.preemption_mode(mode);
        }

        if let Some(value) = extra_args.get("router_queue_policy")
            && let Some(policy_str) = value.as_str()
        {
            let policy = policy_str.parse().map_err(|e: String| anyhow::anyhow!(e))?;
            builder = builder.router_queue_policy(Some(policy));
        }

        let worker_type = if let Some(value) = extra_args.get("worker_type") {
            match value.as_str() {
                Some("aggregated") => WorkerType::Aggregated,
                Some("prefill") => WorkerType::Prefill,
                Some("decode") => WorkerType::Decode,
                Some(other) => {
                    return Err(anyhow::anyhow!(
                        "Invalid worker_type '{}'. Must be 'aggregated', 'prefill', or 'decode'.",
                        other
                    ));
                }
                None => {
                    return Err(anyhow::anyhow!(
                        "Invalid worker_type: expected string value."
                    ));
                }
            }
        } else {
            let is_prefill = extra_args
                .get("is_prefill")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let is_decode = extra_args
                .get("is_decode")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            match (is_prefill, is_decode) {
                (false, false) => WorkerType::Aggregated,
                (true, false) => WorkerType::Prefill,
                (false, true) => WorkerType::Decode,
                (true, true) => {
                    return Err(anyhow::anyhow!(
                        "Invalid worker configuration: is_prefill and is_decode cannot both be true."
                    ));
                }
            }
        };
        builder = builder.worker_type(worker_type);

        // 提供时从 NPZ 文件加载性能模型。
        let perf_model = if let Some(path_str) = extra_args.get("planner_profile_data")
            && let Some(path_str) = path_str.as_str()
        {
            let npz_path = PathBuf::from(path_str);
            builder = builder.planner_profile_data(Some(npz_path.clone()));
            match PerfModel::from_npz(&npz_path) {
                Ok(model) => {
                    tracing::info!("Successfully loaded performance model from: {:?}", npz_path);
                    Arc::new(model)
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to load performance model from {:?}: {}. Falling back to polynomial model.",
                        npz_path,
                        e
                    );
                    Arc::new(PerfModel::default())
                }
            }
        } else {
            Arc::new(PerfModel::default())
        };
        builder = builder.perf_model(perf_model);

        // 检查 AIC 直连模式字段。
        if let Some(backend) = extra_args.get("aic_backend")
            && let Some(backend_str) = backend.as_str()
        {
            builder = builder.aic_backend(Some(backend_str.to_string()));
        }
        if let Some(system) = extra_args.get("aic_system")
            && let Some(s) = system.as_str()
        {
            builder = builder.aic_system(Some(s.to_string()));
        }
        if let Some(version) = extra_args.get("aic_backend_version")
            && let Some(s) = version.as_str()
        {
            builder = builder.aic_backend_version(Some(s.to_string()));
        }
        if let Some(tp) = extra_args.get("aic_tp_size")
            && let Some(n) = tp.as_u64()
        {
            builder = builder.aic_tp_size(Some(n as usize));
        }
        if let Some(mp) = extra_args.get("aic_model_path")
            && let Some(s) = mp.as_str()
        {
            builder = builder.aic_model_path(Some(s.to_string()));
        }
        if let Some(v) = extra_args.get("aic_moe_tp_size")
            && let Some(n) = v.as_u64()
        {
            builder = builder.aic_moe_tp_size(Some(n as usize));
        }
        if let Some(v) = extra_args.get("aic_moe_ep_size")
            && let Some(n) = v.as_u64()
        {
            builder = builder.aic_moe_ep_size(Some(n as usize));
        }
        if let Some(v) = extra_args.get("aic_attention_dp_size")
            && let Some(n) = v.as_u64()
        {
            builder = builder.aic_attention_dp_size(Some(n as usize));
        }
        // 用默认值或被覆盖的值构建 MockEngineArgs。
        builder
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build MockEngineArgs: {}", e))
            .and_then(Self::normalized)
    }
}

#[cfg(test)]
mod tests {
    //! ## 测试过程
    //! 1. 用 builder 构造一组带 Decode worker、`None` 选项的参数，序列化为 JSON 再经
    //!    `from_json_str` 还原，验证 worker_type 与 `None` 字段的往返一致；
    //! 2. 连续创建多个默认 `UniqueBlock`，验证其 UUID 互不相同。
    //!
    //! ## 意义
    //! 锁定 `MockEngineArgs` 的 JSON 解析契约（字段名、worker_type 取值、null 处理）与
    //! 默认部分块身份的唯一性，二者是路由与缓存正确性的前置保证。
    use super::*;

    #[test]
    fn json_round_trip_keeps_worker_type_and_null_options() {
        let args = MockEngineArgs::builder()
            .worker_type(WorkerType::Decode)
            .max_num_seqs(None)
            .max_num_batched_tokens(None)
            .reasoning(None)
            .build()
            .unwrap()
            .normalized()
            .unwrap();

        let payload = serde_json::json!({
            "engine_type": "vllm",
            "num_gpu_blocks": args.num_gpu_blocks,
            "block_size": args.block_size,
            "max_num_seqs": args.max_num_seqs,
            "max_num_batched_tokens": args.max_num_batched_tokens,
            "enable_prefix_caching": args.enable_prefix_caching,
            "enable_chunked_prefill": args.enable_chunked_prefill,
            "speedup_ratio": args.speedup_ratio,
            "decode_speedup_ratio": args.decode_speedup_ratio,
            "dp_size": args.dp_size,
            "startup_time": args.startup_time,
            "worker_type": "decode",
            "planner_profile_data": args.planner_profile_data,
            "aic_backend": args.aic_backend,
            "aic_system": args.aic_system,
            "aic_backend_version": args.aic_backend_version,
            "aic_tp_size": args.aic_tp_size,
            "aic_model_path": args.aic_model_path,
            "enable_local_indexer": args.enable_local_indexer,
            "bootstrap_port": args.bootstrap_port,
            "kv_bytes_per_token": args.kv_bytes_per_token,
            "kv_transfer_bandwidth": args.kv_transfer_bandwidth,
            "reasoning": args.reasoning,
            "zmq_kv_events_port": args.zmq_kv_events_port,
            "zmq_replay_port": args.zmq_replay_port,
            "preemption_mode": "lifo",
            "router_queue_policy": args.router_queue_policy.map(|policy| policy.to_string()),
            "has_perf_model": true,
        });

        let restored = MockEngineArgs::from_json_str(&payload.to_string()).unwrap();

        assert_eq!(restored.worker_type, WorkerType::Decode);
        assert_eq!(restored.max_num_seqs, None);
        assert_eq!(restored.max_num_batched_tokens, None);
    }

    #[test]
    fn default_unique_blocks_have_distinct_uuids() {
        // 收集 10 个默认部分块的 UUID。
        let uuids: Vec<Uuid> = (0..10)
            .map(|_| match UniqueBlock::default() {
                UniqueBlock::PartialBlock(uuid) => uuid,
                _ => panic!("Expected UuidIdentifier variant"),
            })
            .collect();

        // 任意两两不相等。
        for i in 0..uuids.len() {
            for j in i + 1..uuids.len() {
                assert_ne!(
                    uuids[i], uuids[j],
                    "UUID at index {} and {} are identical",
                    i, j
                );
            }
        }
    }
}
