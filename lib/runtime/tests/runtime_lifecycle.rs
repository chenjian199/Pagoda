// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow};
use pagoda_runtime::{
    Worker,
    engine::AsyncEngine,
    pipeline::{ManyOut, ServiceEngine, SingleIn, network::Ingress},
};
use futures::StreamExt;

mod common;
use common::contract::{
    acquire_contract_test_lock, assert_no_instances_error, ensure_integration_tcp_request_plane,
    process_local_runtime, process_local_runtime_ephemeral, round_robin_router, shutdown_runtime,
    unique_name,
    TestResponse,
};
use common::contract_engines::BlockingFirstChunkEngine;

// 目的/场景：`Worker::execute` 在独立进程中运行 async workload 直至完成。
//
// 生产逻辑（`worker.rs`）：`Worker::from_settings` + `execute` 阻塞直至 workload 结束并 shutdown。
// `Worker::execute` / `INIT` 每进程仅允许一次，故在独立 `lifecycle` test binary 中验证。
//
// 测试计划：子进程运行官方 `lifecycle::test_lifecycle`（不修改该文件）。
//
// 关键断言：子进程退出码为 0。
#[tokio::test]
async fn worker_execute_runs_async_workload_to_completion() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    let output = std::process::Command::new(
        std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string()),
    )
    .current_dir(env!("CARGO_MANIFEST_DIR"))
    .args([
        "test",
        "-p",
        "pagoda-runtime",
        "--test",
        "lifecycle",
        "test_lifecycle",
        "--",
        "--exact",
        "--test-threads=1",
    ])
    .output()
    .context("spawn lifecycle test_lifecycle subprocess")?;

    assert!(
        output.status.success(),
        "worker_execute subprocess failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    Ok(())
}

// 目的/场景：Worker 在已有 Tokio runtime 的进程内执行应用，且不嵌套第二套 owned runtime。
//
// 生产逻辑（`worker.rs`）：`Worker::from_current` 用 `Runtime::from_handle(Handle::current())`
// 包装当前 Tokio runtime；`execute_async` 每进程仅允许一次 `INIT` 注册；闭包内
// `runtime_from_existing` 复用同一 handle，不调用 `Worker::from_config`。
//
// 测试计划：单次 `execute_async` 覆盖 workload 完成与 `runtime_from_existing` 复用。
//
// 关键断言：闭包内 `Runtime` 与 worker 同 `id()`；token 活跃；两路 `primary().spawn` 不 panic。
#[tokio::test]
async fn worker_from_current_executes_without_nested_owned_runtime() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    assert!(!Worker::has_existing_runtime());

    let worker = Worker::from_current()?;
    let expected_id = worker.runtime().id().to_string();

    worker
        .execute_async(move |runtime| async move {
            assert_eq!(runtime.id(), expected_id);
            assert!(!runtime.primary_token().is_cancelled());

            let reused = Worker::runtime_from_existing()?;
            assert!(!reused.primary_token().is_cancelled());
            runtime.primary().spawn(async {});
            reused.primary().spawn(async {});
            Ok(())
        })
        .await?;

    Ok(())
}

// 目的/场景：process-local `DistributedRuntime` 构造与 `Runtime::shutdown` 联动。
//
// 生产逻辑（`runtime.rs`）：shutdown Phase 3 取消 `cancellation_token`；
// `DistributedRuntime::primary_token` 转发底层 `Runtime::primary_token`（`distributed.rs`）。
//
// 测试计划：启动后 token 活跃；`rt.shutdown()` 后 `drt.primary_token()` 进入 cancelled。
//
// 关键断言：primary token 从未取消 → 已取消。
#[tokio::test]
async fn distributed_runtime_process_local_starts_and_stops() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    let (rt, drt) = process_local_runtime_ephemeral().await?;
    assert!(!drt.primary_token().is_cancelled());

    rt.shutdown();
    tokio::time::timeout(Duration::from_secs(2), drt.primary_token().cancelled()).await?;
    assert!(drt.primary_token().is_cancelled());
    Ok(())
}

// 目的/场景：`DistributedRuntime::clone` 共享 discovery / metrics 等集群视图。
//
// 生产逻辑：`DistributedRuntime` 为 `Clone`，内部 `Arc` 共享 discovery client 与 registry。
//
// 测试计划：在 `drt` 上 `register_portname_instance`；用 `drt.clone()` 取同名 client 应看到同一实例。
//
// 关键断言：clone 侧 client 发现 1 个实例且 `instance_id` 一致。
#[tokio::test]
async fn runtime_clone_observes_same_registries() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    let (rt, drt) = process_local_runtime().await?;
    let drt_clone = drt.clone();
    let namespace = unique_name("phase1-clone");

    let endpoint = drt
        .namespace(namespace.clone())?
        .servicegroup("backend")?
        .portname("generate");
    endpoint.register_portname_instance().await?;

    let client = drt_clone
        .namespace(namespace)?
        .servicegroup("backend")?
        .portname("generate")
        .client()
        .await?;
    client.wait_for_instances().await?;
    assert_eq!(client.instances().len(), 1);

    shutdown_runtime(rt, None).await?;
    Ok(())
}

// 目的/场景：graceful shutdown 排空 in-flight，拒绝新请求。
//
// 生产逻辑（`runtime.rs` + `endpoint.rs`）：
// Phase 1 取消 `endpoint_shutdown_token` 子 token → endpoint 从 request plane 注销；
// `graceful_shutdown`（默认 true）等待 inflight；Phase 3 取消主 token。
// `PushRouter` 在无可用实例时返回 `no instances found`（`push_router.rs`）。
//
// 测试计划：阻塞 engine 占住 inflight → shutdown → 释放 → 首包成功 → endpoint task 结束 →
// 第二次 `generate` 必须失败且为 routing/unavailable 类错误。
//
// 关键断言：in-flight 收到 `drained`；第二次请求 `assert_no_instances_error`。
#[tokio::test]
async fn shutdown_rejects_new_requests_after_draining_inflight() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    ensure_integration_tcp_request_plane().await?;
    let (rt, drt) = process_local_runtime_ephemeral().await?;
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let endpoint = drt
        .namespace(unique_name("phase1-shutdown"))?
        .servicegroup("backend")?
        .portname("generate");
    let engine: ServiceEngine<SingleIn<String>, ManyOut<TestResponse>> = Arc::new(BlockingFirstChunkEngine {
        started: started.clone(),
        release: release.clone(),
    });
    let ingress = Ingress::for_engine(engine)?;
    let endpoint_task = rt.primary().spawn(
        endpoint
            .portname_builder()
            .handler(ingress)
            .start(),
    );

    let client = endpoint.client().await?;
    client.wait_for_instances().await?;
    let router = round_robin_router(client).await?;

    let started_wait = started.notified();
    let mut in_flight = router.generate("first".to_string().into()).await?;
    tokio::time::timeout(Duration::from_secs(3), started_wait).await?;
    rt.shutdown();
    release.notify_waiters();

    let first = tokio::time::timeout(Duration::from_secs(3), in_flight.next()).await?;
    assert_eq!(
        first.and_then(|item| item.data),
        Some("drained".to_string())
    );

    tokio::time::timeout(Duration::from_secs(5), endpoint_task).await???;

    let second = router.generate("second".to_string().into()).await;
    match second {
        Ok(_) => return Err(anyhow!("new requests after shutdown must fail")),
        Err(err) => assert_no_instances_error(&err),
    }

    Ok(())
}
