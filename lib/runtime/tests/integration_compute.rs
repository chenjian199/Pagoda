// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Result, anyhow};
use dynamo_runtime::engine::AsyncEngine;
use futures::StreamExt;

mod common;
use common::contract::{
    acquire_contract_test_lock, make_echo_engine, process_local_runtime, round_robin_router,
    serve_endpoint_with_engine, shutdown_runtime, unique_name,
};

fn cpu_burn(duration: Duration) -> u64 {
    let start = Instant::now();
    let mut acc = 1u64;
    while start.elapsed() < duration {
        acc = acc.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
    }
    acc
}

// 目的/场景：compute pool 承载 CPU 密集任务时，Tokio reactor 与 TCP request plane 仍可响应。
//
// 生产逻辑：`ComputePool::execute` 经 tokio-rayon 卸载到 Rayon 线程；Tokio worker 继续调度 I/O
// 与 RPC handler（`compute/pool.rs` / `runtime.rs`）。
//
// 测试计划：后台 `pool.execute` 长 burn → 并发 heartbeat sleep + echo RPC。
//
// 关键断言：RPC 在 3s 内完成；heartbeat tick ≥ 15。
#[tokio::test]
async fn compute_pool_offloads_without_blocking_request_plane() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    let (rt, drt) = process_local_runtime().await?;
    let pool = rt
        .compute_pool()
        .ok_or_else(|| anyhow!("integration harness should initialize compute pool"))?
        .clone();

    let ticks = Arc::new(AtomicUsize::new(0));
    let ticks_clone = Arc::clone(&ticks);
    let compute_handle = rt.primary().spawn({
        let pool = pool.clone();
        async move { pool.execute(move || cpu_burn(Duration::from_millis(300))).await }
    });
    let heartbeat = rt.primary().spawn(async move {
        let deadline = Instant::now() + Duration::from_millis(350);
        while Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
            ticks_clone.fetch_add(1, Ordering::Relaxed);
        }
    });

    tokio::time::sleep(Duration::from_millis(20)).await;

    let endpoint = drt
        .namespace(unique_name("compute-rpc"))?
        .component("backend")?
        .endpoint("generate");
    let (client, endpoint_task) =
        serve_endpoint_with_engine(endpoint, make_echo_engine()).await?;
    let router = round_robin_router(client).await?;

    let payload = "rpc-during-compute";
    let rpc_start = Instant::now();
    let mut response = router.generate(payload.to_string().into()).await?;
    let item = response
        .next()
        .await
        .ok_or_else(|| anyhow!("missing echo response during compute offload"))?;
    assert_eq!(item.data.as_deref(), Some(payload));
    assert!(
        rpc_start.elapsed() < Duration::from_secs(3),
        "RPC should not be blocked by compute pool offload"
    );

    compute_handle.await??;
    heartbeat.await?;
    assert!(
        ticks.load(Ordering::Relaxed) >= 15,
        "Tokio reactor should keep scheduling during rayon compute work"
    );

    shutdown_runtime(rt, Some(endpoint_task)).await?;
    Ok(())
}

// 目的/场景：`Runtime::compute_pool()` 执行任务后 `ComputeMetrics` 计数递增。
//
// 生产逻辑：`ComputePool::execute` 调用 `record_task_start` / `record_task_completion`
//（`compute/metrics.rs`）。
//
// 测试计划：`tasks_total==0` → `execute` 一次 burn → 断言 total/active/duration。
//
// 关键断言：`tasks_total==1`；`tasks_active==0`；`max_task_duration_us > 0`。
#[tokio::test]
async fn compute_pool_metrics_increment_on_task() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    let (rt, _drt) = process_local_runtime().await?;
    let pool = rt
        .compute_pool()
        .ok_or_else(|| anyhow!("integration harness should initialize compute pool"))?;

    let metrics = pool.metrics();
    assert_eq!(metrics.tasks_total(), 0);
    assert_eq!(metrics.tasks_active(), 0);

    let value = pool.execute(|| cpu_burn(Duration::from_millis(50))).await?;
    assert!(value > 0);
    assert_eq!(metrics.tasks_total(), 1);
    assert_eq!(metrics.tasks_active(), 0);
    assert!(metrics.max_task_duration_us() > 0);

    shutdown_runtime(rt, None).await?;
    Ok(())
}
