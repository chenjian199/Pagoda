// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-License-Identifier: Apache-2.0

//! # KV cache trace 日志
//!
//! ## 设计意图
//! 为 KV cache 的分配 / 淘汰提供结构化 trace 日志，便于离线分析缓存利用率。
//! 默认关闭，通过环境变量 `DYN_MOCKER_KV_CACHE_TRACE=1`（或 `true`，大小写不敏感）开启。
//!
//! ## 外部契约
//! - 环境变量名 `DYN_MOCKER_KV_CACHE_TRACE` 与启用取值（`"1"` 或忽略大小写的 `"true"`）保持一致。
//! - [`KV_CACHE_TRACE_ENABLED`] 暴露为 `LazyLock<bool>`。
//! - [`log_vllm_trace`] 的参数面与发出的 tracing 字段（`engine_type="vllm"`、`event`、
//!   `timestamp_ms`、`dp_rank`、`block_size`、`free_blocks`、`active_blocks`、
//!   `inactive_blocks`、`total_blocks`、`utilization`，消息 `"KV cache trace"`）必须保持一致。
//!
//! ## 实现要点
//! 空闲块数为 `total - active - inactive`（饱和减，避免下溢）；利用率为
//! `(active + inactive) / total`，`total` 为 0 时取 0.0。时间戳为自 Unix 纪元的毫秒数。

use std::env;
use std::sync::LazyLock;
use std::time::{SystemTime, UNIX_EPOCH};

// === SECTION: 开关 ===

const DYN_MOCKER_KV_CACHE_TRACE: &str = "DYN_MOCKER_KV_CACHE_TRACE";

/// 是否启用 KV cache 分配 / 淘汰 trace（由环境变量决定，进程内只解析一次）。
pub static KV_CACHE_TRACE_ENABLED: LazyLock<bool> = LazyLock::new(|| {
    match env::var(DYN_MOCKER_KV_CACHE_TRACE) {
        Ok(value) => value == "1" || value.eq_ignore_ascii_case("true"),
        Err(_) => false,
    }
});

// === SECTION: 时间戳 ===

/// 当前 Unix 时间（毫秒）。时钟异常时回退为 0。
fn timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// === SECTION: vLLM trace ===

/// 发出一条 vLLM KV cache trace 日志（trace 未启用时直接返回，零开销）。
pub fn log_vllm_trace(
    event: &str,
    dp_rank: u32,
    block_size: usize,
    active_blocks: usize,
    inactive_blocks: usize,
    total_blocks: usize,
) {
    if !*KV_CACHE_TRACE_ENABLED {
        return;
    }

    let used_blocks = active_blocks + inactive_blocks;
    let free_blocks = total_blocks.saturating_sub(used_blocks);
    let utilization = if total_blocks > 0 {
        used_blocks as f64 / total_blocks as f64
    } else {
        0.0
    };

    tracing::info!(
        engine_type = "vllm",
        event,
        timestamp_ms = timestamp_ms(),
        dp_rank,
        block_size,
        free_blocks,
        active_blocks,
        inactive_blocks,
        total_blocks,
        utilization,
        "KV cache trace"
    );
}
