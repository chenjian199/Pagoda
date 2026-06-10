// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

mod common;

mod tcp {
    use std::{
        collections::HashMap,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    use anyhow::{Result, anyhow};
    use dynamo_runtime::{
        engine::AsyncEngine,
        metrics::{
            request_plane::REQUEST_PLANE_INFLIGHT,
            transport_metrics::{TCP_BYTES_SENT_TOTAL, TCP_ERRORS_TOTAL},
        },
        pipeline::{ManyOut, ServiceEngine, SingleIn, network::Ingress},
    };
    use futures::{StreamExt, future::join_all};
    use tokio::sync::Mutex;

    use super::common::contract::{
        TestResponse, acquire_contract_test_lock, collect_stream_chunks,
        ensure_integration_tcp_request_plane, generate_expect_no_instances, make_echo_engine,
        process_local_runtime, round_robin_router, serve_endpoint_with_engine,
        serve_streaming_endpoint, shutdown_runtime, unique_name,
    };
    use super::common::contract_engines::{
        CancellableEngine, ContextEchoEngine, HighVolumeStreamingEngine,
    };

    // 目的/场景：TCP request plane 单包 payload 原样往返（Phase 1 `tcp_roundtrip_payload_and_response`）。
    //
    // 生产逻辑：`Ingress::for_engine` → TCP codec → echo engine 单分片响应。
    //
    // 测试计划：部署 `make_echo_engine`；`PushRouter::generate` 一次；断言单 chunk == 请求串。
    //
    // 关键断言：单 chunk echo 等于 payload；无第二 chunk。
    #[tokio::test]
    async fn tcp_roundtrip_payload_and_response() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let (rt, drt) = process_local_runtime().await?;
        let endpoint = drt
            .namespace(unique_name("phase1-tcp-echo"))?
            .component("backend")?
            .endpoint("generate");
        let (client, endpoint_task) =
            serve_endpoint_with_engine(endpoint, make_echo_engine()).await?;
        let router = round_robin_router(client).await?;

        let payload = "tcp-payload-roundtrip";
        let mut response = router.generate(payload.to_string().into()).await?;
        let item = response
            .next()
            .await
            .ok_or_else(|| anyhow!("missing echo response"))?;
        assert_eq!(item.data.as_deref(), Some(payload));
        assert!(response.next().await.is_none());

        shutdown_runtime(rt, Some(endpoint_task)).await?;
        Ok(())
    }

    // 目的/场景：request plane 流式响应顺序与 engine emit 顺序一致。
    //
    // 生产逻辑：`make_streaming_engine` 按字符 emit；TCP 流按序交付。
    //
    // 测试计划：streaming endpoint → `generate("stream")` → 收集全部 chunk。
    //
    // 关键断言：chunks 为 `["s","t","r","e","a","m"]` 顺序。
    #[tokio::test]
    async fn tcp_client_streaming_response_preserves_order() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let (rt, drt) = process_local_runtime().await?;
        let endpoint = drt
            .namespace(unique_name("phase1-stream"))?
            .component("backend")?
            .endpoint("generate");
        let (client, endpoint_task) = serve_streaming_endpoint(endpoint).await?;
        let router = round_robin_router(client).await?;

        let response = router.generate("stream".to_string().into()).await?;
        let chunks = collect_stream_chunks(response).await;

        assert_eq!(chunks, vec!["s", "t", "r", "e", "a", "m"]);
        shutdown_runtime(rt, Some(endpoint_task)).await?;
        Ok(())
    }

    // 目的/场景：client 丢弃响应流时，backend 通过 `AsyncEngineContext::kill()` 或 channel 关闭感知取消。
    //
    // 生产逻辑（`tcp/client.rs`）：client drop → reader/writer 结束 → `context.kill()`；
    // engine 侧应观察 `is_killed()` 或 emit 失败（`contract.rs` `make_streaming_engine` 模式）。
    //
    // 测试计划：`CancellableEngine` 在循环中检查 `engine_ctx.is_killed()`；client 读一片后 drop。
    //
    // 关键断言：`cancelled` 在超时内为 true。
    #[tokio::test]
    async fn tcp_client_drop_cancels_handler() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        ensure_integration_tcp_request_plane().await?;
        let (rt, drt) = process_local_runtime().await?;
        let started = Arc::new(tokio::sync::Notify::new());
        let cancelled = Arc::new(AtomicBool::new(false));
        let endpoint = drt
            .namespace(unique_name("phase1-cancel"))?
            .component("backend")?
            .endpoint("generate");
        let engine: ServiceEngine<SingleIn<String>, ManyOut<TestResponse>> =
            Arc::new(CancellableEngine {
                started: started.clone(),
                cancelled: cancelled.clone(),
            });
        let (client, endpoint_task) = serve_endpoint_with_engine(endpoint, engine).await?;
        let router = round_robin_router(client).await?;

        let started_wait = started.notified();
        let mut response = router.generate("cancel".to_string().into()).await?;
        tokio::time::timeout(Duration::from_secs(3), started_wait).await?;
        assert!(response.next().await.is_some());
        drop(response);

        tokio::time::timeout(Duration::from_secs(3), async {
            while !cancelled.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;

        shutdown_runtime(rt, Some(endpoint_task)).await?;
        Ok(())
    }

    // 目的/场景：并发 RPC 的 request id 与 payload 在 engine 侧可区分。
    //
    // 生产逻辑：`SingleIn::with_id` 将 id 写入 `AsyncEngineContext`；TCP 请求头携带 request-id。
    //
    // 测试计划：16 路并发 RPC，每路唯一 payload 与 request-id → `ContextEchoEngine` 记录。
    //
    // 关键断言：每路响应为 `{payload}:{request_id}`；engine 侧见到 16 个不同 id。
    #[tokio::test]
    async fn tcp_concurrent_requests_preserve_context_isolation() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        ensure_integration_tcp_request_plane().await?;
        let (rt, drt) = process_local_runtime().await?;
        let seen = Arc::new(Mutex::new(HashMap::new()));
        let endpoint = drt
            .namespace(unique_name("phase1-concurrent"))?
            .component("backend")?
            .endpoint("generate");
        let engine: ServiceEngine<SingleIn<String>, ManyOut<TestResponse>> =
            Arc::new(ContextEchoEngine { seen: seen.clone() });
        let ingress = Ingress::for_engine(engine)?;
        let endpoint_task = rt.primary().spawn(
            endpoint
                .endpoint_builder()
                .handler(ingress)
                .start(),
        );

        let client = endpoint.client().await?;
        client.wait_for_instances().await?;
        let router = Arc::new(round_robin_router(client).await?);

        let tasks = (0..16usize).map(|idx| {
            let router = router.clone();
            async move {
                let payload = format!("payload-{idx}");
                let request_id = format!("request-{idx}");
                let request = SingleIn::with_id(payload.clone(), request_id.clone());
                let mut response = router.generate(request).await?;
                let item = response
                    .next()
                    .await
                    .ok_or_else(|| anyhow!("missing response item"))?;
                Ok::<_, anyhow::Error>((payload, request_id, item.data.unwrap()))
            }
        });

        let results = join_all(tasks).await;
        for result in results {
            let (payload, request_id, actual) = result?;
            assert_eq!(actual, format!("{payload}:{request_id}"));
        }
        assert_eq!(seen.lock().await.len(), 16);

        shutdown_runtime(rt, Some(endpoint_task)).await?;
        Ok(())
    }

    // 目的/场景：慢消费者读取 TCP 流式响应时不丢 frame、顺序不变。
    //
    // 生产逻辑：小 outbound channel + TCP 流式传输；client 侧延迟消费应触发 backpressure 而非丢 chunk。
    //
    // 测试计划：`HighVolumeStreamingEngine` 32 chunk → client 每 chunk sleep 5ms 慢读。
    //
    // 关键断言：收到 chunk-0..31 全部 32 条且顺序正确。
    #[tokio::test]
    async fn tcp_backpressure_does_not_drop_frames() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let (rt, drt) = process_local_runtime().await?;
        let chunk_count = 32usize;
        let endpoint = drt
            .namespace(unique_name("phase1-backpressure"))?
            .component("backend")?
            .endpoint("generate");
        let engine: ServiceEngine<SingleIn<String>, ManyOut<TestResponse>> =
            Arc::new(HighVolumeStreamingEngine { chunk_count });
        let (client, endpoint_task) = serve_endpoint_with_engine(endpoint, engine).await?;
        let router = round_robin_router(client).await?;

        let mut response = router.generate("backpressure".to_string().into()).await?;
        let mut chunks = Vec::with_capacity(chunk_count);
        while let Some(item) = response.next().await {
            chunks.push(item.data.unwrap());
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let expected: Vec<String> = (0..chunk_count)
            .map(|idx| format!("chunk-{idx}"))
            .collect();
        assert_eq!(chunks, expected);

        shutdown_runtime(rt, Some(endpoint_task)).await?;
        Ok(())
    }

    // 目的/场景：成功 RPC 更新 transport/request-plane metrics；路由失败不污染 inflight/transport 计数。
    //
    // 生产逻辑：`tcp_client.rs` 更新 bytes counters；`addressed_router.rs` 维护 `REQUEST_PLANE_INFLIGHT`。
    //
    // 测试计划：成功 echo RPC → 断言 bytes/inflight → 无实例 endpoint RPC 失败 → 再查 counters。
    //
    // 关键断言：`TCP_BYTES_SENT` 增加；inflight 不泄漏；路由失败不增 TCP_ERRORS。
    #[tokio::test]
    async fn tcp_metrics_record_success_and_failure() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let (rt, drt) = process_local_runtime().await?;
        let endpoint = drt
            .namespace(unique_name("phase1-tcp-metrics"))?
            .component("backend")?
            .endpoint("generate");
        let (client, endpoint_task) =
            serve_endpoint_with_engine(endpoint, make_echo_engine()).await?;
        let router = round_robin_router(client.clone()).await?;

        let inflight_before = REQUEST_PLANE_INFLIGHT.get();
        let sent_before = TCP_BYTES_SENT_TOTAL.get();
        let errors_before = TCP_ERRORS_TOTAL.get();

        {
            let mut response = router.generate("metrics-probe".to_string().into()).await?;
            let item = response
                .next()
                .await
                .ok_or_else(|| anyhow!("missing echo response"))?;
            assert_eq!(item.data.as_deref(), Some("metrics-probe"));
            assert!(response.next().await.is_none());
        }

        assert!(
            TCP_BYTES_SENT_TOTAL.get() > sent_before,
            "successful TCP RPC should increment bytes sent"
        );
        assert_eq!(
            REQUEST_PLANE_INFLIGHT.get(),
            inflight_before,
            "completed RPC should not leak inflight gauge"
        );

        let unregistered = drt
            .namespace(unique_name("phase1-tcp-metrics-miss"))?
            .component("backend")?
            .endpoint("generate")
            .client()
            .await?;
        let miss_router = round_robin_router(unregistered).await?;
        generate_expect_no_instances(&miss_router, "missing").await?;
        assert_eq!(
            REQUEST_PLANE_INFLIGHT.get(),
            inflight_before,
            "routing failure should not leak inflight gauge"
        );
        assert_eq!(
            TCP_ERRORS_TOTAL.get(),
            errors_before,
            "routing failure before transport should not increment TCP error counter"
        );

        shutdown_runtime(rt, Some(endpoint_task)).await?;
        Ok(())
    }
}

mod nats {
    use std::{collections::HashSet, sync::Arc, time::Duration};

    use anyhow::{Context, Result, anyhow};
    use dynamo_runtime::{component::TransportType, engine::AsyncEngine};
    use futures::StreamExt;
    use temp_env::async_with_vars;
    use tempfile::TempDir;

    use super::common::contract::{
        acquire_contract_test_lock, additional_nats_file_backed_runtime, confirm_nats_rpc_ready,
        make_echo_engine, nats_broker_stop_outage_and_start, nats_file_backed_runtime,
        nats_runtime, probe_nats_rpc_failure_during_outage, round_robin_router,
        wait_for_nats_broker_ready,
        serve_endpoint_with_engine_nats, serve_streaming_endpoint_nats, shutdown_runtime,
        start_served_endpoint_nats, unique_name,
    };
    use super::common::contract_engines::{BlockingFirstChunkEngine, InstanceTagEngine};

    // 目的/场景：NATS request plane 单包 payload 原样往返（与 TCP echo 契约对称）。
    //
    // 生产逻辑：`RequestPlaneMode::Nats` → `NatsRequestClient::send_request` / `NatsMultiplexedServer`。
    //
    // 测试计划：`nats_runtime` + echo engine → `PushRouter::generate` 一次。
    //
    // 关键断言：单 chunk 等于请求串。
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires NATS broker (Nightly); set NATS_SERVER and run with --include-ignored"]
    async fn nats_roundtrip_payload_and_response() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let (rt, drt) = nats_runtime().await?;
        let endpoint = drt
            .namespace(unique_name("nats-echo"))?
            .component("backend")?
            .endpoint("generate");
        let (client, endpoint_task) =
            serve_endpoint_with_engine_nats(endpoint, make_echo_engine()).await?;
        let router = round_robin_router(client).await?;

        let payload = "nats-payload-roundtrip";
        let mut response = router.generate(payload.to_string().into()).await?;
        let item = response
            .next()
            .await
            .ok_or_else(|| anyhow!("missing echo response"))?;
        assert_eq!(item.data.as_deref(), Some(payload));
        assert!(response.next().await.is_none());

        shutdown_runtime(rt, Some(endpoint_task)).await?;
        Ok(())
    }

    // 目的/场景：discovery 中 NATS transport 的 subject 含路由身份（namespace/component/endpoint）。
    //
    // 生产逻辑：`build_transport_type` → `nats::instance_subject`（`endpoint.rs` / `nats.rs`）。
    //
    // 测试计划：serve endpoint → 读 `client.instances()` 中 `TransportType::Nats`。
    //
    // 关键断言：subject 含 namespace、component、endpoint 名。
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires NATS broker (Nightly); set NATS_SERVER and run with --include-ignored"]
    async fn nats_subject_includes_routing_identity() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let (rt, drt) = nats_runtime().await?;
        let namespace = unique_name("nats-subject");
        let component = "backend";
        let endpoint_name = "generate";
        let endpoint = drt
            .namespace(namespace.clone())?
            .component(component)?
            .endpoint(endpoint_name);
        let (client, endpoint_task) = serve_streaming_endpoint_nats(endpoint).await?;

        let instances = client.instances();
        assert_eq!(instances.len(), 1);
        let TransportType::Nats(subject) = &instances[0].transport else {
            return Err(anyhow!(
                "expected NATS transport, got {:?}",
                instances[0].transport
            ));
        };
        assert!(
            subject.contains(&namespace),
            "subject should include namespace: {subject}"
        );
        assert!(
            subject.contains(component),
            "subject should include component: {subject}"
        );
        assert!(
            subject.contains(endpoint_name),
            "subject should include endpoint: {subject}"
        );
        assert!(
            subject.contains(&format!("{:x}", drt.connection_id())),
            "subject should include instance id: {subject}"
        );

        shutdown_runtime(rt, Some(endpoint_task)).await?;
        Ok(())
    }

    // 目的/场景：共享 discovery 下多副本经 NATS service group 可被调度到不同 worker。
    //
    // 生产逻辑：两 DRT 共享 file KV；`NatsMultiplexedServer` 按 component service group 注册副本。
    //
    // 测试计划：两 `InstanceTagEngine` replica → 多次 RPC 收集 tag。
    //
    // 关键断言：discovery 2 实例；响应 tag 至少 2 种。
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires NATS broker (Nightly); set NATS_SERVER and run with --include-ignored"]
    async fn nats_queue_group_balances_replicas() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let temp = TempDir::new()?;
        let kv_path = temp.path().to_path_buf();
        let (rt, drt1) = nats_file_backed_runtime(kv_path.clone()).await?;
        let drt2 = additional_nats_file_backed_runtime(rt.clone(), &kv_path).await?;
        let namespace = unique_name("nats-replicas");

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
        let (_c1, task1) = serve_endpoint_with_engine_nats(
            endpoint1.clone(),
            Arc::new(InstanceTagEngine { instance_id: id1 }),
        )
        .await?;
        let (_c2, task2) = serve_endpoint_with_engine_nats(
            endpoint2,
            Arc::new(InstanceTagEngine { instance_id: id2 }),
        )
        .await?;

        let client = endpoint1.client().await?;
        super::common::contract::wait_for_instance_count(&client, 2).await?;
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
        assert!(
            tags.len() >= 2,
            "NATS routing should hit multiple replicas, got tags: {tags:?}"
        );

        shutdown_runtime(rt.clone(), Some(task1)).await?;
        shutdown_runtime(rt, Some(task2)).await?;
        Ok(())
    }

    // 目的/场景：handler 已接受请求但长时间不产出 chunk 时，客户端在 inactivity timeout 后得到错误。
    //
    // 生产逻辑：`PushRouter` 对 `ManyOut` 流应用 `DYN_HTTP_BACKEND_STREAM_TIMEOUT_SECS`。
    //
    // 测试计划：短 timeout env + `BlockingFirstChunkEngine`（永不 release 首包）→ 流空闲超时。
    //
    // 关键断言：流式响应含 timeout 相关错误项。
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires NATS broker (Nightly); set NATS_SERVER and run with --include-ignored"]
    async fn nats_request_timeout_returns_error() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let (rt, drt) = nats_runtime().await?;
        let endpoint = drt
            .namespace(unique_name("nats-timeout"))?
            .component("backend")?
            .endpoint("generate");
        let (client, endpoint_task) = start_served_endpoint_nats(
            endpoint,
            Arc::new(BlockingFirstChunkEngine {
                started: Arc::new(tokio::sync::Notify::new()),
                release: Arc::new(tokio::sync::Notify::new()),
            }),
        )
        .await?;

        async_with_vars(
            [("DYN_HTTP_BACKEND_STREAM_TIMEOUT_SECS", Some("1"))],
            async {
                let router = round_robin_router(client).await?;

                let mut stream = router.generate("wait".to_string().into()).await?;
                let item = tokio::time::timeout(Duration::from_secs(5), stream.next())
                    .await
                    .map_err(|_| anyhow!("timed out waiting for inactivity timeout error"))?
                    .ok_or_else(|| anyhow!("expected timeout error item on stream"))?;
                let err_text = format!("{item:?}").to_lowercase();
                assert!(
                    err_text.contains("timeout") || err_text.contains("error"),
                    "expected timeout/error stream item, got: {item:?}"
                );

                Ok::<(), anyhow::Error>(())
            },
        )
        .await?;

        shutdown_runtime(rt, Some(endpoint_task)).await?;
        Ok(())
    }

    // 目的/场景：NATS broker 短暂断连（docker restart）后，runtime request plane 自动恢复 RPC。
    //
    // 生产逻辑：DRT / `NatsRequestClient` 共用 `async_nats` 长连接；broker stop/start 断开所有客户端，
    // async-nats 在 broker 恢复后自动重连（`transports/nats.rs` connect path）。
    //
    // 测试计划：echo RPC 成功 → docker stop 2s start → outage 期间 RPC 失败 → broker 就绪
    // → `confirm_nats_rpc_ready` → 再次 RPC。
    //
    // 关键断言：outage 期间至少一次 RPC 失败；恢复后 payload 原样返回。
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires dockerized NATS on NATS_SERVER (Nightly); run with --include-ignored"]
    async fn nats_reconnect_restores_request_path() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let (rt, drt) = nats_runtime().await?;
        let endpoint = drt
            .namespace(unique_name("nats-reconnect"))?
            .component("backend")?
            .endpoint("generate");
        let (client, endpoint_task) =
            serve_endpoint_with_engine_nats(endpoint, make_echo_engine()).await?;
        let router = round_robin_router(client.clone()).await?;

        let mut before = router.generate("before-reconnect".to_string().into()).await?;
        let item = before
            .next()
            .await
            .ok_or_else(|| anyhow!("missing echo before reconnect"))?;
        assert_eq!(item.data.as_deref(), Some("before-reconnect"));

        let outage = Duration::from_secs(2);
        let router_for_outage = round_robin_router(client.clone()).await?;
        let outage_task = tokio::spawn(async move {
            nats_broker_stop_outage_and_start(outage).await
        });
        tokio::time::sleep(Duration::from_millis(800)).await;
        let saw_outage_failure =
            probe_nats_rpc_failure_during_outage(&router_for_outage, 30).await;
        outage_task.await??;
        wait_for_nats_broker_ready(Duration::from_secs(30)).await?;
        assert!(
            saw_outage_failure,
            "expected at least one RPC failure while NATS broker was restarting"
        );

        confirm_nats_rpc_ready(client.clone())
            .await
            .context("NATS request plane should recover after broker restart")?;

        let mut after = router.generate("after-reconnect".to_string().into()).await?;
        let item = after
            .next()
            .await
            .ok_or_else(|| anyhow!("missing echo after reconnect"))?;
        assert_eq!(item.data.as_deref(), Some("after-reconnect"));

        shutdown_runtime(rt, Some(endpoint_task)).await?;
        Ok(())
    }

    // 目的/场景：NATS microservice registry 在 component 上注册多个 endpoint。
    //
    // 生产逻辑：`ComponentBuilder::build` → `register_nats_service`；`NatsMultiplexedServer`
    // 为每个 served endpoint 在 service group 下创建 NATS endpoint（`nats_server.rs`）。
    //
    // 测试计划：同一 component serve `generate` + `health` → `$SRV.STATS` 拉取 endpoints。
    //
    // 关键断言：至少 2 条 endpoint；name 分别含 `generate` 与 `health`。
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires NATS broker (Nightly); set NATS_SERVER and run with --include-ignored"]
    async fn nats_service_registers_component_endpoints() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let temp = TempDir::new()?;
        let kv_path = temp.path().to_path_buf();
        let (rt, drt1) = nats_file_backed_runtime(kv_path.clone()).await?;
        let drt2 = additional_nats_file_backed_runtime(rt.clone(), &kv_path).await?;
        let namespace = unique_name("nats-svc");
        let component1 = drt1.namespace(namespace.clone())?.component("backend")?;
        let component2 = drt2.namespace(namespace)?.component("backend")?;
        let service_name = super::common::contract::component_service_name(&component1);

        let (_c1, task1) = start_served_endpoint_nats(
            component1.endpoint("generate"),
            make_echo_engine(),
        )
        .await?;
        let (_c2, task2) = start_served_endpoint_nats(
            component2.endpoint("health"),
            make_echo_engine(),
        )
        .await?;

        let endpoints = super::common::contract::wait_for_nats_service_endpoints(
            &service_name,
            2,
            Duration::from_secs(15),
        )
        .await?;
        let names: Vec<&str> = endpoints.iter().map(|e| e.name.as_str()).collect();
        assert!(
            names.iter().any(|n| n.contains("generate")),
            "expected generate endpoint in NATS service registry, got: {names:?}"
        );
        assert!(
            names.iter().any(|n| n.contains("health")),
            "expected health endpoint in NATS service registry, got: {names:?}"
        );

        shutdown_runtime(rt.clone(), Some(task1)).await?;
        shutdown_runtime(rt, Some(task2)).await?;
        Ok(())
    }

    // 目的/场景：NATS service endpoint subject 含 namespace/component/endpoint/instance 身份。
    //
    // 生产逻辑：service group 名 = slugify `{namespace}_{component}`；endpoint subject =
    // `{service}.{endpoint}-{instance_id_hex}`（`nats_server.rs` / `Component::service_name`）。
    //
    // 测试计划：serve 单 endpoint → `$SRV.STATS` 解码 subject。
    //
    // 关键断言：subject 含 slug 化 service 名、endpoint 名、connection_id 十六进制。
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires NATS broker (Nightly); set NATS_SERVER and run with --include-ignored"]
    async fn nats_service_subject_matches_routing_identity() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let (rt, drt) = nats_runtime().await?;
        let namespace = unique_name("nats-svc-subj");
        let component_name = "backend";
        let endpoint_name = "generate";
        let component = drt.namespace(namespace.clone())?.component(component_name)?;
        let service_name = super::common::contract::component_service_name(&component);
        let instance_id = format!("{:x}", drt.connection_id());

        let endpoint = component.endpoint(endpoint_name);
        let (_client, endpoint_task) =
            serve_endpoint_with_engine_nats(endpoint, make_echo_engine()).await?;

        let endpoints = super::common::contract::wait_for_nats_service_endpoints(
            &service_name,
            1,
            Duration::from_secs(15),
        )
        .await?;
        let info = endpoints
            .first()
            .ok_or_else(|| anyhow!("missing NATS service endpoint stats"))?;

        assert!(
            info.subject.contains(&service_name),
            "subject should include service name {service_name}, got: {}",
            info.subject
        );
        assert!(
            info.subject.contains(endpoint_name),
            "subject should include endpoint name {endpoint_name}, got: {}",
            info.subject
        );
        assert!(
            info.subject.contains(&instance_id),
            "subject should include instance id {instance_id}, got: {}",
            info.subject
        );
        assert!(
            info.name.contains(endpoint_name),
            "endpoint name should include {endpoint_name}, got: {}",
            info.name
        );

        shutdown_runtime(rt, Some(endpoint_task)).await?;
        Ok(())
    }
}
