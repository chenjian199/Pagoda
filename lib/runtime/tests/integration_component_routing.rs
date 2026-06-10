// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashSet, sync::Arc};

use anyhow::{Result, anyhow};
use dynamo_runtime::{DistributedRuntime, engine::AsyncEngine};
use futures::StreamExt;

mod common;
use common::contract::{
    acquire_contract_test_lock, collect_stream_chunks, file_backed_config, generate_expect_no_instances,
    ensure_integration_tcp_request_plane, init_integration_test_env, list_endpoint_models,
    model_discovery_spec, process_local_runtime, round_robin_router, serve_streaming_endpoint,
    shared_integration_runtime, shutdown_runtime, unique_name,
    wait_for_instance_count, wait_for_instances_empty,
};
use common::contract_engines::InstanceTagEngine;

// 目的/场景：endpoint 注册 → discovery → RPC → 显式注销 的完整公共 API 闭环。
//
// 生产逻辑：`Endpoint::start` 注册 discovery；`unregister_endpoint_instance` 调用
// `discovery.unregister`（`endpoint.rs`）；client watch 收敛后 `instances()` 为空；
// `PushRouter` 无实例时失败。
//
// 测试计划：serve 流式 endpoint → 调用成功 → `unregister_endpoint_instance` →
// 等待 discovery 空 → 再调用必须失败。
//
// 关键断言：注销前 `a,b,c`；注销后 `generate_expect_no_instances`。
#[tokio::test]
async fn register_discover_call_unregister_roundtrip() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    let (rt, drt) = process_local_runtime().await?;
    let endpoint = drt
        .namespace(unique_name("phase1"))?
        .component("backend")?
        .endpoint("generate");
    let (client, endpoint_task) = serve_streaming_endpoint(endpoint.clone()).await?;

    let router = round_robin_router(client.clone()).await?;
    let response = router.generate("abc".to_string().into()).await?;
    assert_eq!(collect_stream_chunks(response).await, vec!["a", "b", "c"]);

    endpoint.unregister_endpoint_instance().await?;
    wait_for_instances_empty(&client).await?;
    generate_expect_no_instances(&router, "after-unregister").await?;

    shutdown_runtime(rt, Some(endpoint_task)).await?;
    Ok(())
}

// 目的/场景：namespace 为 discovery 与 RPC 路由的隔离边界。
//
// 生产逻辑：discovery key 含 namespace（`kv_store.rs`）；不同 namespace 的 client
// 只 watch 各自前缀；对无实例 namespace 发 RPC 得到 `no instances found`。
//
// 测试计划：仅在 namespace A serve；namespace B 无服务；向 B 发 RPC。
//
// 关键断言：A 有 1 实例且可流式响应；B discovery 为空且 RPC 失败。
#[tokio::test]
async fn same_endpoint_different_namespace_does_not_mix() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    let (rt, drt) = process_local_runtime().await?;
    let endpoint_a = drt
        .namespace(unique_name("phase1-a"))?
        .component("backend")?
        .endpoint("generate");
    let endpoint_b = drt
        .namespace(unique_name("phase1-b"))?
        .component("backend")?
        .endpoint("generate");

    let (client_a, task_a) = serve_streaming_endpoint(endpoint_a).await?;
    let client_b = endpoint_b.client().await?;

    assert_eq!(client_a.instances().len(), 1);
    assert!(client_b.instances().is_empty());

    let router_a = round_robin_router(client_a).await?;
    let chunks = collect_stream_chunks(router_a.generate("xy".to_string().into()).await?).await;
    assert_eq!(chunks, vec!["x", "y"]);

    let router_b = round_robin_router(client_b).await?;
    generate_expect_no_instances(&router_b, "cross-ns").await?;

    shutdown_runtime(rt, Some(task_a)).await?;
    Ok(())
}

// 目的/场景：同 namespace/component/endpoint 多 worker 形成副本池，RR 可打到不同 instance。
//
// 生产逻辑：file KV 共享 discovery；各 `DistributedRuntime` 的 `connection_id` 不同；
// `PushRouter::round_robin` 在 `instance_ids_avail()` 间轮询（`push_router.rs`）。
//
// 测试计划：两个 DRT 各 serve `InstanceTagEngine`（响应 `tag-{connection_id}`）；
// 发多次 RPC，收集到的 tag 应覆盖两个 worker。
//
// 关键断言：discovery 2 个不同 instance_id；RPC 响应 tag 至少 2 种。
#[tokio::test]
async fn same_endpoint_same_namespace_forms_replica_pool() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    init_integration_test_env();
    ensure_integration_tcp_request_plane().await?;
    let tmp = tempfile::tempdir()?;
    let kv_path = tmp.path().to_path_buf();
    let config = || file_backed_config(kv_path.clone());

    let rt = shared_integration_runtime();
    let drt1 = DistributedRuntime::new(rt.clone(), config()).await?;
    let drt2 = DistributedRuntime::new(rt.clone(), config()).await?;
    let namespace = unique_name("phase1-replicas");

    let endpoint1 = drt1
        .namespace(namespace.clone())?
        .component("backend")?
        .endpoint("generate");
    let endpoint2 = drt2
        .namespace(namespace)?
        .component("backend")?
        .endpoint("generate");

    let id1 = drt1.connection_id();
    let id2 = drt2.connection_id();
    let (_c1, task1) = common::contract::serve_endpoint_with_engine(
        endpoint1.clone(),
        Arc::new(InstanceTagEngine { instance_id: id1 }),
    )
    .await?;
    let (_c2, task2) = common::contract::serve_endpoint_with_engine(
        endpoint2,
        Arc::new(InstanceTagEngine { instance_id: id2 }),
    )
    .await?;

    let client = endpoint1.client().await?;
    wait_for_instance_count(&client, 2).await?;

    let router = round_robin_router(client).await?;
    let mut tags = HashSet::new();
    for _ in 0..24 {
        let mut stream = router.generate("ping".to_string().into()).await?;
        let item = stream
            .next()
            .await
            .ok_or_else(|| anyhow!("missing replica response"))?;
        tags.insert(item.data.unwrap());
    }
    assert!(tags.len() >= 2, "round-robin should hit multiple replicas, got tags: {tags:?}");

    shutdown_runtime(rt.clone(), Some(task1)).await?;
    shutdown_runtime(rt, Some(task2)).await?;
    Ok(())
}

// 目的/场景：component 是路由隔离维度（同 namespace、不同 component 不共享实例）。
//
// 生产逻辑：discovery key 含 component 层级；client 仅 watch 匹配 component 前缀。
//
// 测试计划：仅 `component-a` serve；`component-b` 同 endpoint 名无实例；向 B RPC 失败。
//
// 关键断言：client-b `instances()` 为空；RPC `generate_expect_no_instances`。
#[tokio::test]
async fn same_namespace_different_component_is_isolated() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    let (rt, drt) = process_local_runtime().await?;
    let ns = unique_name("phase1-comp");

    let (_client_a, task_a) = serve_streaming_endpoint(
        drt.namespace(ns.clone())?
            .component("component-a")?
            .endpoint("generate"),
    )
    .await?;

    let client_b = drt
        .namespace(ns)?
        .component("component-b")?
        .endpoint("generate")
        .client()
        .await?;
    assert!(client_b.instances().is_empty());
    generate_expect_no_instances(&round_robin_router(client_b).await?, "comp-b").await?;

    shutdown_runtime(rt, Some(task_a)).await?;
    Ok(())
}

// 目的/场景：endpoint 名是路由隔离维度（同 namespace/component、不同 endpoint 不共享）。
//
// 生产逻辑：discovery key 含 endpoint 名；同 component 下不同 endpoint 独立实例池。
//
// 测试计划：仅 `generate-a` serve；`generate-b` client 无实例 → RPC 失败。
//
// 关键断言：client-b 空实例；`generate_expect_no_instances`。
#[tokio::test]
async fn same_namespace_same_component_different_endpoint_is_isolated() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    let (rt, drt) = process_local_runtime().await?;
    let ns = unique_name("phase1-ep");

    let (_client, task) = serve_streaming_endpoint(
        drt.namespace(ns.clone())?
            .component("backend")?
            .endpoint("generate-a"),
    )
    .await?;

    let client_b = drt
        .namespace(ns)?
        .component("backend")?
        .endpoint("generate-b")
        .client()
        .await?;
    assert!(client_b.instances().is_empty());
    generate_expect_no_instances(&round_robin_router(client_b).await?, "ep-b").await?;

    shutdown_runtime(rt, Some(task)).await?;
    Ok(())
}

// 目的/场景：从未注册的 endpoint 调用必须快速失败。
//
// 生产逻辑：`PushRouter::round_robin` 在 `instance_ids_avail().is_empty()` 时报
// `no instances found for endpoint`。
//
// 测试计划：未 serve 的 endpoint 取 client → 直接 `generate`。
//
// 关键断言：`generate_expect_no_instances`。
#[tokio::test]
async fn calling_unregistered_endpoint_returns_unavailable() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    let (rt, drt) = process_local_runtime().await?;
    let endpoint = drt
        .namespace(unique_name("phase1-empty"))?
        .component("backend")?
        .endpoint("generate");
    let client = endpoint.client().await?;
    generate_expect_no_instances(&round_robin_router(client).await?, "abc").await?;
    shutdown_runtime(rt, None).await?;
    Ok(())
}

// 目的/场景：注销后调用失败（与“从未注册”区分，走真实 unregister API）。
//
// 生产逻辑：`unregister_endpoint_instance` → discovery Removed → client watch 收敛。
//
// 测试计划：serve → 成功 RPC → unregister → 同一 router 再 RPC。
//
// 关键断言：注销前 RPC 成功；注销后 `generate_expect_no_instances`。
#[tokio::test]
async fn calling_endpoint_after_unregister_returns_unavailable() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    let (rt, drt) = process_local_runtime().await?;
    let endpoint = drt
        .namespace(unique_name("phase1-unreg"))?
        .component("backend")?
        .endpoint("generate");
    let (client, task) = serve_streaming_endpoint(endpoint.clone()).await?;
    let router = round_robin_router(client.clone()).await?;
    assert!(router.generate("ok".to_string().into()).await?.next().await.is_some());

    endpoint.unregister_endpoint_instance().await?;
    wait_for_instances_empty(&client).await?;
    generate_expect_no_instances(&router, "after").await?;

    shutdown_runtime(rt, Some(task)).await?;
    Ok(())
}

// 目的/场景：同一 worker 重复 `register_endpoint_instance` 行为稳定（幂等覆盖，不膨胀副本）。
//
// 生产逻辑：KV `insert` 同一 instance key 覆盖；client 仍应只见 1 个本 worker 实例。
//
// 测试计划：serve 后连续两次 `register_endpoint_instance` → `wait_for_instances`。
//
// 关键断言：`client.instances().len() == 1`。
#[tokio::test]
async fn duplicate_instance_registration_is_idempotent_or_rejected_consistently() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    let (rt, drt) = process_local_runtime().await?;
    let endpoint = drt
        .namespace(unique_name("phase1-dup"))?
        .component("backend")?
        .endpoint("generate");
    let (client, task) = serve_streaming_endpoint(endpoint.clone()).await?;

    endpoint.register_endpoint_instance().await?;
    endpoint.register_endpoint_instance().await?;
    client.wait_for_instances().await?;
    assert_eq!(client.instances().len(), 1);

    shutdown_runtime(rt, Some(task)).await?;
    Ok(())
}

// 目的/场景：同一逻辑 endpoint 不允许注册冲突的 model 名称。
//
// 生产逻辑：`Discovery::register` 对 `DiscoverySpec::Model` 检查
// `find_conflicting_model_name`（`discovery/mod.rs`），与 mock 测试一致。
//
// 测试计划：同 endpoint 注册 model-a → 再注册 model-b。
//
// 关键断言：第二次 register Err 含 `Cannot register model 'model-b'`；list 仍 1 条。
#[tokio::test]
async fn different_models_same_logical_endpoint_are_rejected() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    let (rt, drt) = process_local_runtime().await?;
    let namespace = unique_name("phase1-model");
    let discovery = drt.discovery();

    discovery
        .register(model_discovery_spec(&namespace, "backend", "generate", "model-a"))
        .await?;
    let err = discovery
        .register(model_discovery_spec(&namespace, "backend", "generate", "model-b"))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("Cannot register model 'model-b'"));
    assert_eq!(list_endpoint_models(&drt, &namespace).await?, 1);

    shutdown_runtime(rt, None).await?;
    Ok(())
}
