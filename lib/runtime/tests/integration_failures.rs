// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::{sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use futures::StreamExt;
use tempfile::TempDir;
use pagoda_runtime::{
    CancellationToken,
    engine::AsyncEngine,
    pipeline::{
        ManyOut, ServiceBackend, ServiceFrontend, SingleIn, Source,
        network::codec::TcpRequestMessage,
    },
    utils::task::CriticalTaskExecutionHandle,
};

mod common;
use common::contract::{
    acquire_contract_test_lock, collect_stream_chunks, confirm_tcp_rpc_ready, file_backed_runtime,
    generate_expect_no_instances, make_echo_engine, process_local_runtime, round_robin_router,
    serve_endpoint_with_engine, serve_streaming_endpoint, shutdown_runtime, unique_name,
    wait_for_instances_empty, wipe_file_discovery_namespace,
};
use common::contract_engines::make_error_service_engine;

// 目的/场景：backend `Err` 与 critical task panic 只影响当前请求/任务，runtime 仍可继续服务其它 endpoint。
//
// 生产逻辑：`Ingress`/`push_handler.rs` 将 `generate` 错误写入 prologue；`CriticalTaskExecutionHandle`
// 在 panic/`Err` 时 cancel parent token（`utils/tasks/critical.rs`）；handler 在独立 task 中执行。
//
// 测试计划：进程内 ErrorEngine `generate` 失败 → critical task panic → TCP echo endpoint 成功。
//
// 关键断言：`backend contract error`；panic 后 parent cancelled；echo 返回 payload。
#[tokio::test]
async fn handler_panic_or_error_is_reported_without_killing_runtime() -> Result<()> {
    let _guard = acquire_contract_test_lock();

    let frontend = ServiceFrontend::<SingleIn<String>, ManyOut<common::contract::TestResponse>>::new();
    let return_frontend = frontend.clone();
    let backend = ServiceBackend::from_engine(make_error_service_engine());
    let service: std::sync::Arc<
        ServiceFrontend<SingleIn<String>, ManyOut<common::contract::TestResponse>>,
    > = frontend.link(backend)?.link(return_frontend)?;
    let err = match service.generate("fail-local".to_string().into()).await {
        Ok(_) => anyhow!("backend error should fail"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("backend contract error"));

    let parent = CancellationToken::new();
    let handle = CriticalTaskExecutionHandle::new(
        |_token| async {
            panic!("simulated ingress handler panic");
        },
        parent.clone(),
        "integration-handler-panic",
    )?;
    let join_result = tokio::time::timeout(Duration::from_secs(3), handle.join()).await;
    assert!(join_result.is_ok() && join_result.unwrap().is_err());
    assert!(parent.is_cancelled());

    let (rt, drt) = process_local_runtime().await?;
    let echo_endpoint = drt
        .namespace(unique_name("fail-echo"))?
        .servicegroup("backend")?
        .portname("generate");
    let (echo_client, echo_task) =
        serve_endpoint_with_engine(echo_endpoint, make_echo_engine()).await?;
    let echo_router = round_robin_router(echo_client).await?;
    let mut echo_stream = echo_router.generate("runtime-ok".to_string().into()).await?;
    let echo_item = echo_stream
        .next()
        .await
        .ok_or_else(|| anyhow!("expected echo after failures"))?;
    assert_eq!(echo_item.data.as_deref(), Some("runtime-ok"));

    shutdown_runtime(rt, Some(echo_task)).await?;
    Ok(())
}

// 目的/场景：critical task 失败时 owner 的 parent cancellation token 被触发。
//
// 生产逻辑：`CriticalTaskExecutionHandle` monitor 在 task `Err`/panic 时立刻
// `parent_token.cancel()`（`utils/tasks/critical.rs`）。
//
// 测试计划：parent token + 返回 `Err` 的 critical task → `join()` 失败且 parent 已 cancel。
//
// 关键断言：`join()` 为 Err；`parent.is_cancelled()`。
#[tokio::test]
async fn critical_task_failure_is_visible_to_owner() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    let parent = CancellationToken::new();
    let handle = CriticalTaskExecutionHandle::new(
        |_token| async { Err::<(), _>(anyhow!("critical contract failure")) },
        parent.clone(),
        "integration-critical-failure",
    )?;

    let join_result = handle.join().await;
    assert!(join_result.is_err());
    assert!(
        parent.is_cancelled(),
        "critical failure should cancel the owner token"
    );
    Ok(())
}

// 目的/场景：并发 register/unregister 后 discovery 视图最终一致，不出现幽灵副本。
//
// 生产逻辑：`Endpoint::register_portname_instance` / `unregister_portname_instance` 经
// discovery KV 覆盖或删除（`endpoint.rs` + `discovery/kv_store.rs`）。
//
// 测试计划：serve endpoint → 并发 8 次 register/unregister 交错 → 最终 `instances()` 为空。
//
// 关键断言：`wait_for_instances_empty`；endpoint task 仍可正常 shutdown。
#[tokio::test]
async fn concurrent_register_unregister_is_eventually_consistent() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    let (rt, drt) = process_local_runtime().await?;
    let endpoint = drt
        .namespace(unique_name("fail-churn"))?
        .servicegroup("backend")?
        .portname("generate");
    let (client, endpoint_task) = serve_streaming_endpoint(endpoint.clone()).await?;
    client.wait_for_instances().await?;
    assert_eq!(client.instances().len(), 1);

    let endpoint = Arc::new(endpoint);
    let mut tasks = Vec::new();
    for round in 0..8 {
        let endpoint = endpoint.clone();
        tasks.push(tokio::spawn(async move {
            endpoint.register_portname_instance().await?;
            if round % 2 == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            endpoint.unregister_portname_instance().await
        }));
    }
    for task in tasks {
        task.await??;
    }

    wait_for_instances_empty(&client).await?;
    shutdown_runtime(rt, Some(endpoint_task)).await?;
    Ok(())
}

// 目的/场景：TCP codec 解码失败只影响坏帧；合法 RPC 在 decode 错误后仍可成功。
//
// 生产逻辑：`TcpRequestMessage::decode` 对截断/非法帧返回 `InvalidData`（`codec.rs`）；
// request plane worker 对单流 decode 错误发 Kill 而不 panic（`tcp/server.rs` 单测）。
//
// 测试计划：合法 RPC → 截断帧 decode 必失败 → 同一 router 再次合法 RPC。
//
// 关键断言：decode Err；两次合法 RPC 均返回预期 echo。
#[tokio::test]
async fn transport_decode_error_closes_bad_request_only() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    let (rt, drt) = process_local_runtime().await?;
    let endpoint = drt
        .namespace(unique_name("fail-decode"))?
        .servicegroup("backend")?
        .portname("generate");
    let (client, endpoint_task) =
        serve_endpoint_with_engine(endpoint, make_echo_engine()).await?;
    let router = round_robin_router(client).await?;

    let before = router.generate("before-decode".to_string().into()).await?;
    assert_eq!(
        collect_stream_chunks(before).await,
        vec!["before-decode"]
    );

    let msg = TcpRequestMessage::new("truncated".to_string(), bytes::Bytes::from_static(b"bad"));
    let encoded = msg.encode()?;
    let truncated = encoded.slice(..encoded.len().saturating_sub(2));
    let decode_err = TcpRequestMessage::decode(&truncated)
        .expect_err("truncated TCP frame should fail decode");
    assert!(
        matches!(
            decode_err.kind(),
            std::io::ErrorKind::InvalidData | std::io::ErrorKind::UnexpectedEof
        ),
        "unexpected decode error kind: {decode_err:?}"
    );

    let after = router.generate("after-decode".to_string().into()).await?;
    assert_eq!(collect_stream_chunks(after).await, vec!["after-decode"]);

    shutdown_runtime(rt, Some(endpoint_task)).await?;
    Ok(())
}

// 目的/场景：外部 discovery 存储短暂不可用时路由失败，恢复注册后请求再次成功。
//
// 生产逻辑：file KV discovery watch 将 Delete 转为 Removed；`Client::instances` 收敛后
// `PushRouter` 返回 no instances（`component/client.rs` + `push_router.rs`）。
//
// 测试计划：file-backed serve echo → RPC 成功 → 清空 namespace discovery 文件 → RPC 失败
// → `register_portname_instance` 恢复 → RPC 成功。
//
// 关键断言：outage 期间 `generate_expect_no_instances`；恢复后 echo payload 一致。
#[tokio::test]
async fn external_discovery_outage_returns_unavailable_then_recovers() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    let temp = TempDir::new()?;
    let kv_path = temp.path().to_path_buf();
    let (rt, drt) = file_backed_runtime(kv_path.clone()).await?;
    let namespace = unique_name("fail-outage");
    let endpoint = drt
        .namespace(namespace.clone())?
        .servicegroup("backend")?
        .portname("generate");
    let (client, endpoint_task) =
        serve_endpoint_with_engine(endpoint.clone(), make_echo_engine()).await?;
    let router = round_robin_router(client.clone()).await?;

    let mut healthy = router.generate("before-outage".to_string().into()).await?;
    let item = healthy
        .next()
        .await
        .ok_or_else(|| anyhow!("expected echo before discovery outage"))?;
    assert_eq!(item.data.as_deref(), Some("before-outage"));

    wipe_file_discovery_namespace(&kv_path, &namespace)?;
    wait_for_instances_empty(&client).await?;
    generate_expect_no_instances(&router, "during-outage").await?;

    endpoint.register_portname_instance().await?;
    client.wait_for_instances().await?;
    confirm_tcp_rpc_ready(client.clone()).await?;
    let router = round_robin_router(client.clone()).await?;

    let mut recovered = router.generate("after-recovery".to_string().into()).await?;
    let item = recovered
        .next()
        .await
        .ok_or_else(|| anyhow!("expected echo after discovery recovery"))?;
    assert_eq!(item.data.as_deref(), Some("after-recovery"));

    shutdown_runtime(rt, Some(endpoint_task)).await?;
    Ok(())
}
