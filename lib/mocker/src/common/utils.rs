// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-License-Identifier: Apache-2.0

//! # 时序与 KV 传输延迟工具
//!
//! ## 设计意图
//! 为分离式（disaggregated）部署模拟 prefill→decode 之间的 KV 搬运延迟，并提供
//! 高精度的异步睡眠原语供仿真循环使用。
//!
//! ## 外部契约
//! - [`compute_prefill_handoff_delay_ms`]：仅当 worker 为 `Prefill` 且 `completed`
//!   为真、且带宽与每 token 字节数均已配置且带宽 > 0 时返回延迟（毫秒），否则 `None`。
//!   延迟公式 `tokens * bytes_per_token / (bw * 1e9) * 1000`（带宽单位 GB/s）必须保持一致。
//! - [`compute_kv_transfer_delay`]：把上述毫秒延迟换算为 [`Duration`]，禁用时返回 `None`。
//! - [`sleep_precise`] / [`sleep_until_precise`]：高精度睡眠；Linux 上优先用 timerfd。
//!
//! ## 实现要点
//! `completed` 在 [`compute_kv_transfer_delay`] 中恒为真——只有请求收尾才触发 KV 搬运。
//! Linux 平台用 `tokio_timerfd::Delay` 提升定时精度，失败或非 Linux 回退到 tokio 定时器。

use std::time::{Duration, Instant};

use crate::common::protocols::{MockEngineArgs, WorkerType};

// === SECTION: KV 搬运延迟计算 ===

/// 计算 prefill worker 发出终止 token 后的 KV 交接延迟（毫秒）。
///
/// 该模型只关心 decode 侧可见的 TTFT —— 即客户端实际观察到的延迟，因此把延迟建模为
/// prefill→decode 的交接耗时即可，并不精确刻画 prefill 内部的 TTFT。
pub fn compute_prefill_handoff_delay_ms(
    worker_type: WorkerType,
    completed: bool,
    num_input_tokens: usize,
    kv_transfer_bandwidth: Option<f64>,
    kv_bytes_per_token: Option<usize>,
) -> Option<f64> {
    // 非 prefill worker 或请求尚未收尾：不产生交接延迟。
    if worker_type != WorkerType::Prefill || !completed {
        return None;
    }

    // 带宽与每 token 字节数都配置好、且带宽为正时才有意义。
    let (Some(bandwidth_gb_s), Some(bytes_per_token)) =
        (kv_transfer_bandwidth, kv_bytes_per_token)
    else {
        return None;
    };
    if bandwidth_gb_s <= 0.0 {
        return None;
    }

    let kv_bytes = num_input_tokens as f64 * bytes_per_token as f64;
    let delay_ms = kv_bytes / (bandwidth_gb_s * 1e9) * 1000.0;
    tracing::debug!(
        num_input_tokens,
        kv_bytes,
        bandwidth_gb_s,
        delay_ms = format!("{delay_ms:.2}"),
        "KV handoff delay for prefill completion"
    );
    Some(delay_ms)
}

/// 把给定输入 token 数对应的 KV 搬运延迟换算为 [`Duration`]。
///
/// KV 传输模拟被禁用（带宽为 0 或未配置）时返回 `None`。
pub fn compute_kv_transfer_delay(
    args: &MockEngineArgs,
    num_input_tokens: usize,
) -> Option<Duration> {
    let delay_ms = compute_prefill_handoff_delay_ms(
        args.worker_type,
        true,
        num_input_tokens,
        args.kv_transfer_bandwidth,
        args.kv_bytes_per_token,
    )?;
    Some(Duration::from_secs_f64(delay_ms / 1000.0))
}

// === SECTION: 高精度睡眠 ===

/// 睡眠指定时长；Linux 上借助 timerfd 提升精度。
pub async fn sleep_precise(duration: Duration) {
    sleep_until_precise(Instant::now() + duration).await;
}

/// 睡眠至指定截止时刻。
///
/// 与 [`sleep_precise`] 不同，本函数以绝对截止时刻为准，会自动扣除自参考点以来已
/// 流逝的时间，适合需要把计算耗时从睡眠中扣除的仿真循环。
pub async fn sleep_until_precise(deadline: Instant) {
    #[cfg(target_os = "linux")]
    {
        match tokio_timerfd::Delay::new(deadline) {
            Ok(delay) => {
                let _ = delay.await;
            }
            Err(_) => {
                tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
    }
}

// === SECTION: 测试 ===

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handoff_delay_requires_completed_prefill_worker() {
        // ## 测试过程
        // 用 1 GB/s 带宽、每 token 1e6 字节、128 token 计算交接延迟；再分别用未完成
        // 与 Decode worker 两种情形对照。
        // ## 意义
        // 验证延迟公式（128 * 1e6 / 1e9 * 1000 = 128ms）与触发条件：仅「Prefill 且 completed」
        // 才返回延迟，其余返回 None。
        let delay_ms = compute_prefill_handoff_delay_ms(
            WorkerType::Prefill,
            true,
            128,
            Some(1.0),
            Some(1_000_000),
        )
        .expect("completed prefill should yield a handoff delay");
        assert!((delay_ms - 128.0).abs() < 1e-9);

        assert!(
            compute_prefill_handoff_delay_ms(
                WorkerType::Prefill,
                false,
                128,
                Some(1.0),
                Some(1_000_000),
            )
            .is_none()
        );
        assert!(
            compute_prefill_handoff_delay_ms(
                WorkerType::Decode,
                true,
                128,
                Some(1.0),
                Some(1_000_000),
            )
            .is_none()
        );
    }

    #[test]
    fn handoff_delay_none_when_bandwidth_unset_or_zero() {
        // ## 测试过程
        // 分别在带宽缺失、每 token 字节缺失、带宽为 0 三种情形下计算。
        // ## 意义
        // 验证任一必要参数缺失或带宽非正时禁用 KV 传输模拟，返回 None。
        assert!(
            compute_prefill_handoff_delay_ms(WorkerType::Prefill, true, 64, None, Some(1_000))
                .is_none()
        );
        assert!(
            compute_prefill_handoff_delay_ms(WorkerType::Prefill, true, 64, Some(10.0), None)
                .is_none()
        );
        assert!(
            compute_prefill_handoff_delay_ms(
                WorkerType::Prefill,
                true,
                64,
                Some(0.0),
                Some(1_000),
            )
            .is_none()
        );
    }
}
