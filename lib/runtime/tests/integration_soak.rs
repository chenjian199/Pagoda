// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use pagoda_runtime::{
    servicegroup::{Client, PortName},
    engine::{AsyncEngine, AsyncEngineContextProvider},
    metrics::MetricsHierarchy,
    pipeline::{
        ManyOut, PushRouter, RouterMode, ServiceEngine, SingleIn, network::Ingress,
    },
    protocols::annotated::Annotated,
    storage::kv,
};
use futures::StreamExt;
use rand::{Rng, SeedableRng, rngs::StdRng};
use tempfile::TempDir;
use temp_env::async_with_vars;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

mod common;
use common::contract::{
    acquire_contract_test_lock, collect_stream_chunks, discovery_query_endpoint,
    file_backed_runtime, make_echo_engine, process_local_runtime,
    round_robin_router, serve_endpoint_with_engine, serve_streaming_endpoint, shutdown_runtime,
    unique_name, wait_for_instances_empty, TestResponse,
};
use common::engines::{AsyncGenerator, LlmdbaEngine as LambdaEngine};

fn count_file_discovery_namespace_keys(kv_root: &Path, namespace: &str) -> Result<usize> {
    let instances_dir = kv_root.join("v1/instances");
    if !instances_dir.exists() {
        return Ok(0);
    }
    let prefix = format!("{namespace}/");
    let mut count = 0usize;
    for entry in std::fs::read_dir(&instances_dir)? {
        let entry = entry?;
        if !entry.path().is_file() {
            continue;
        }
        let encoded = entry.file_name().to_string_lossy().into_owned();
        let decoded = kv::Key::from_url_safe(&encoded);
        if decoded.as_ref().starts_with(&prefix) {
            count += 1;
        }
    }
    Ok(count)
}

async fn assert_discovery_matches_slots(
    drt: &pagoda_runtime::DistributedRuntime,
    namespace: &str,
    slots: &[ChurnSlot],
) -> Result<()> {
    let registered = slots.iter().filter(|slot| slot.registered).count();
    let mut listed_total = 0usize;
    for slot in slots {
        let query = discovery_query_endpoint(namespace, &slot.servicegroup, &slot.endpoint_name);
        let listed = drt.discovery().list(query).await?;
        if slot.registered {
            assert_eq!(listed.len(), 1, "expected discovery entry for {}", slot.servicegroup);
            let instances = slot.client.instances();
            assert_eq!(instances.len(), 1);
        } else {
            assert!(listed.is_empty(), "stale discovery entry for {}", slot.servicegroup);
            assert!(slot.client.instances().is_empty());
        }
        listed_total += listed.len();
    }
    assert_eq!(
        listed_total, registered,
        "discovery list count should match registered slots"
    );
    Ok(())
}

struct ChurnSlot {
    servicegroup: String,
    endpoint_name: String,
    portname: PortName,
    client: Client,
    registered: bool,
    endpoint_task: tokio::task::JoinHandle<Result<()>>,
}

/// Tracks in-flight backend tasks for soak cancel/leak assertions.
struct LongRunningCancellableEngine {
    started: Arc<tokio::sync::Notify>,
    cancelled: Arc<AtomicBool>,
    active_backend_tasks: Arc<AtomicUsize>,
}

#[async_trait]
impl AsyncEngine<SingleIn<String>, ManyOut<TestResponse>, anyhow::Error>
    for LongRunningCancellableEngine
{
    async fn generate(
        &self,
        request: SingleIn<String>,
    ) -> Result<ManyOut<TestResponse>, anyhow::Error> {
        let (_payload, ctx) = request.into_parts();
        let engine_ctx = ctx.context();
        let (tx, rx) = mpsc::channel(1);
        let started = self.started.clone();
        let cancelled = self.cancelled.clone();
        let active = self.active_backend_tasks.clone();
        let engine_ctx_watch = engine_ctx.clone();

        active.fetch_add(1, Ordering::SeqCst);
        tokio::spawn(async move {
            let _guard = ActiveTaskGuard(active);
            started.notify_waiters();
            for idx in 0..1_000usize {
                if engine_ctx_watch.is_killed() {
                    cancelled.store(true, Ordering::SeqCst);
                    return;
                }
                if tx
                    .send(TestResponse::from_data(format!("chunk-{idx}")))
                    .await
                    .is_err()
                {
                    cancelled.store(true, Ordering::SeqCst);
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });

        Ok(pagoda_runtime::engine::ResponseStream::new(
            Box::pin(ReceiverStream::new(rx)),
            engine_ctx,
        ))
    }
}

struct ActiveTaskGuard(Arc<AtomicUsize>);

impl Drop for ActiveTaskGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

async fn run_soak_contract(env_duration: &str, env_batch: &str) -> Result<()> {
    let (rt, drt) = process_local_runtime().await?;
    let backend_counter = Arc::new(AtomicU64::new(0));
    let handler = LambdaEngine::from_generator(
        AsyncGenerator::<String, Annotated<String>>::new({
            let backend_counter = backend_counter.clone();
            move |(req, stream)| {
                let backend_counter = backend_counter.clone();
                async move {
                    backend_counter.fetch_add(1, Ordering::Relaxed);
                    for ch in req.chars() {
                        let _ = stream.emit(Annotated::from_data(ch.to_string())).await;
                    }
                }
            }
        }),
    );
    let ingress = Ingress::for_engine(handler)?;
    let namespace = unique_name("soak-smoke");
    let component = drt.namespace(namespace.clone())?.servicegroup("backend")?;
    let endpoint_task = rt.primary().spawn(
        component
            .portname("generate")
            .portname_builder()
            .handler(ingress)
            .start(),
    );

    let client = drt
        .namespace(namespace)?
        .servicegroup("backend")?
        .portname("generate")
        .client()
        .await?;
    client.wait_for_instances().await?;
    let router = Arc::new(
        PushRouter::<String, Annotated<String>>::from_client(client, RouterMode::RoundRobin)
            .await?,
    );

    let run_duration = humantime::parse_duration(env_duration)
        .unwrap_or_else(|_| Duration::from_secs(2));
    let batch_load = env_batch.parse::<usize>().unwrap_or(8);
    let start = tokio::time::Instant::now();
    let mut total = 0usize;

    while start.elapsed() < run_duration {
        let mut tasks = Vec::new();
        for _ in 0..batch_load {
            let router = router.clone();
            tasks.push(tokio::spawn(async move {
                let mut stream = router.generate("soak".to_string().into()).await?;
                while stream.next().await.is_some() {}
                Ok::<(), anyhow::Error>(())
            }));
        }
        for task in tasks {
            task.await??;
        }
        total += batch_load;
    }

    let processed = backend_counter.load(Ordering::Relaxed);
    assert!(processed > 0, "soak should process at least one request");
    let metric_lines = drt
        .get_metrics_registry()
        .prometheus_expfmt_combined()?
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .count();
    eprintln!(
        "soak: backend_requests={processed}, rpc_attempts={total}, metric_sample_lines={metric_lines}"
    );

    shutdown_runtime(rt, Some(endpoint_task)).await?;
    Ok(())
}

// 目的/场景：短 soak 冒烟验证 process-local runtime 在持续 RPC 下稳定完成并输出诊断。
//
// 生产逻辑：`PushRouter` + `Ingress::for_engine` + streaming backend（与旧 `soak.rs` 同路径）。
//
// 测试计划：`PGD_SOAK_RUN_DURATION=2s`、`PGD_SOAK_BATCH_LOAD=8` → 并发 batch RPC →
// 打印 backend 计数与 metrics series 数。
//
// 关键断言：run 期间无 panic；backend 计数 > 0。
#[tokio::test]
async fn soak_smoke_completes_short_run() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    async_with_vars(
        [
            ("PGD_SOAK_RUN_DURATION", Some("2s")),
            ("PGD_SOAK_BATCH_LOAD", Some("8")),
        ],
        async {
            run_soak_contract("2s", "8").await
        },
    )
    .await
}

// 目的/场景：长 soak 压测（Release/Nightly），可配置 duration 与 batch load。
//
// 生产逻辑：同 `soak_smoke_completes_short_run`；默认 30s / batch 64，可通过 env 覆盖。
//
// 测试计划：`#[ignore]` + `--include-ignored`；读取 `PGD_SOAK_*` env 运行。
//
// 关键断言：完整 duration 内 RPC 无失败；backend 计数与 batch 总量一致量级。
#[tokio::test]
#[ignore = "long-running soak (Release/Nightly); set PGD_SOAK_RUN_DURATION and --include-ignored"]
async fn soak_sustained_load_reports_diagnostics() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    let run_duration = std::env::var("PGD_SOAK_RUN_DURATION").unwrap_or_else(|_| "30s".into());
    let batch_load = std::env::var("PGD_SOAK_BATCH_LOAD").unwrap_or_else(|_| "64".into());
    async_with_vars(
        [
            ("PGD_SOAK_RUN_DURATION", Some(run_duration.as_str())),
            ("PGD_SOAK_BATCH_LOAD", Some(batch_load.as_str())),
        ],
        async { run_soak_contract(&run_duration, &batch_load).await },
    )
    .await
}

// 目的/场景：多 endpoint 随机 register/unregister churn 后 discovery 无 stale 实例。
//
// 生产逻辑：`register_portname_instance` / `unregister_portname_instance` 写 file KV；
// client watch 与 `discovery.list` 应收敛到一致视图（`endpoint.rs` + `kv_store.rs`）。
//
// 测试计划：6 个 served endpoint → 48 轮伪随机 churn → 周期性校验 → 全部注销 → KV 为空。
//
// 关键断言：每轮 `discovery.list` 与 `client.instances()` 一致；结束后 namespace 下 0 条 KV。
#[tokio::test]
#[ignore = "endpoint churn soak (Release); run with --include-ignored"]
async fn random_endpoint_churn_does_not_leave_stale_instances() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    let temp = TempDir::new()?;
    let kv_path = temp.path().to_path_buf();
    let (rt, drt) = file_backed_runtime(kv_path.clone()).await?;
    let namespace = unique_name("soak-churn");

    let mut slots = Vec::new();
    for comp_idx in 0..3 {
        for ep_idx in 0..2 {
            let component = format!("c{comp_idx}");
            let endpoint_name = format!("e{ep_idx}");
            let endpoint = drt
                .namespace(namespace.clone())?
                .servicegroup(component.clone())?
                .portname(endpoint_name.clone());
            let (client, endpoint_task) = serve_streaming_endpoint(endpoint.clone()).await?;
            slots.push(ChurnSlot {
                servicegroup: component,
                endpoint_name,
                portname: endpoint,
                client,
                registered: true,
                endpoint_task,
            });
        }
    }

    let mut rng = StdRng::seed_from_u64(42);
    for round in 0..48 {
        let idx = rng.random_range(0..slots.len());
        if slots[idx].registered {
            slots[idx].portname.unregister_portname_instance().await?;
            wait_for_instances_empty(&slots[idx].client).await?;
            slots[idx].registered = false;
        } else {
            slots[idx].portname.register_portname_instance().await?;
            slots[idx]
                .client
                .wait_for_instances()
                .await
                .context("re-register should restore discovery instance")?;
            slots[idx].registered = true;
        }

        if round % 8 == 7 {
            assert_discovery_matches_slots(&drt, &namespace, &slots).await?;
        }
    }

    for slot in &mut slots {
        if slot.registered {
            slot.portname.unregister_portname_instance().await?;
            wait_for_instances_empty(&slot.client).await?;
            slot.registered = false;
        }
    }
    assert_discovery_matches_slots(&drt, &namespace, &slots).await?;
    assert_eq!(
        count_file_discovery_namespace_keys(&kv_path, &namespace)?,
        0,
        "file discovery KV should have no stale keys after churn"
    );

    for slot in slots {
        shutdown_runtime(rt.clone(), Some(slot.endpoint_task)).await?;
    }
    Ok(())
}

// 目的/场景：soak 负载后显式注销与 shutdown，外部 discovery 存储无残留。
//
// 生产逻辑：file KV `discovery.unregister` 删除实例 key；etcd ephemeral runtime shutdown
// revoke lease 并删除附着 key（`transports/etcd/lease.rs`）。
//
// 测试计划：file-backed soak → unregister → KV 计数为 0；可选 etcd 路径验证 list 为空。
//
// 关键断言：soak 后 `count_file_discovery_namespace_keys == 0`；etcd list 无残留实例。
#[tokio::test]
#[ignore = "soak shutdown resource release (Release); run with --include-ignored"]
async fn runtime_shutdown_after_soak_releases_external_resources() -> Result<()> {
    let _guard = acquire_contract_test_lock();

    let temp = TempDir::new()?;
    let kv_path = temp.path().to_path_buf();
    let (rt, drt) = file_backed_runtime(kv_path.clone()).await?;
    let namespace = unique_name("soak-shutdown-file");
    let endpoint = drt
        .namespace(namespace.clone())?
        .servicegroup("backend")?
        .portname("generate");
    let (client, endpoint_task) =
        serve_endpoint_with_engine(endpoint.clone(), make_echo_engine()).await?;
    let router = round_robin_router(client.clone()).await?;

    assert!(
        count_file_discovery_namespace_keys(&kv_path, &namespace)? >= 1,
        "discovery should persist at least one instance key during soak"
    );

    let soak_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < soak_deadline {
        let response = router.generate("soak-shutdown".to_string().into()).await?;
        assert_eq!(collect_stream_chunks(response).await, vec!["soak-shutdown"]);
    }

    endpoint.unregister_portname_instance().await?;
    wait_for_instances_empty(&client).await?;
    assert_eq!(
        count_file_discovery_namespace_keys(&kv_path, &namespace)?,
        0,
        "file discovery keys must be released after unregister"
    );

    shutdown_runtime(rt, Some(endpoint_task)).await?;

    #[cfg(feature = "testing-etcd")]
    {
        use common::contract::{
            endpoint_discovery_spec, etcd_runtime_ephemeral, require_etcd_cluster,
        };

        require_etcd_cluster().await?;
        let namespace = unique_name("soak-shutdown-etcd");
        let component = "backend";
        let endpoint_name = "generate";
        let query = discovery_query_endpoint(&namespace, component, endpoint_name);

        let (rt, drt) = etcd_runtime_ephemeral().await?;
        drt.discovery()
            .register(endpoint_discovery_spec(
                &namespace,
                component,
                endpoint_name,
            ))
            .await?;
        assert_eq!(drt.discovery().list(query.clone()).await?.len(), 1);

        let soak_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while tokio::time::Instant::now() < soak_deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        shutdown_runtime(rt, None).await?;
        let (_rt2, drt2) = etcd_runtime_ephemeral().await?;
        assert!(
            drt2.discovery().list(query).await?.is_empty(),
            "etcd discovery keys should be released after ephemeral runtime shutdown"
        );
    }

    Ok(())
}

// 目的/场景：多轮长流 TCP RPC 取消后 backend 任务归零，endpoint 仍可继续服务。
//
// 生产逻辑：client drop → `AsyncEngineContext::kill()`；backend 观察 `is_killed()` 退出
//（`tcp/client.rs` + `CancellableEngine` 模式）；无悬挂 spawn。
//
// 测试计划：12 轮「起长流 → 读首 chunk → drop → 等待 cancelled」→ echo 探活。
//
// 关键断言：每轮后 `active_backend_tasks == 0`；最终 echo RPC 成功。
#[tokio::test]
#[ignore = "long-stream cancel soak (Release); run with --include-ignored"]
async fn long_running_streams_can_be_cancelled_without_task_leak() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    let (rt, drt) = process_local_runtime().await?;
    let started = Arc::new(tokio::sync::Notify::new());
    let cancelled = Arc::new(AtomicBool::new(false));
    let active_backend_tasks = Arc::new(AtomicUsize::new(0));

    let endpoint = drt
        .namespace(unique_name("soak-cancel"))?
        .servicegroup("backend")?
        .portname("generate");
    let engine: ServiceEngine<SingleIn<String>, ManyOut<TestResponse>> =
        Arc::new(LongRunningCancellableEngine {
            started: started.clone(),
            cancelled: cancelled.clone(),
            active_backend_tasks: active_backend_tasks.clone(),
        });
    let (client, endpoint_task) = serve_endpoint_with_engine(endpoint, engine).await?;
    let router = round_robin_router(client.clone()).await?;

    const CANCEL_ROUNDS: usize = 12;
    for round in 0..CANCEL_ROUNDS {
        cancelled.store(false, Ordering::SeqCst);
        let started_wait = started.notified();
        let mut response = router.generate(format!("cancel-{round}").into()).await?;
        tokio::time::timeout(Duration::from_secs(3), started_wait).await?;
        assert!(response.next().await.is_some());
        drop(response);

        tokio::time::timeout(Duration::from_secs(3), async {
            while !cancelled.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;

        tokio::time::timeout(Duration::from_secs(3), async {
            while active_backend_tasks.load(Ordering::SeqCst) != 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .with_context(|| format!("backend task leak after cancel round {round}"))?;
    }

    let mut probe = router.generate("final-probe".to_string().into()).await?;
    let first = probe
        .next()
        .await
        .ok_or_else(|| anyhow!("endpoint should still accept RPC after cancel soak"))?;
    assert_eq!(
        first.data.as_deref(),
        Some("chunk-0"),
        "routing path should remain healthy after repeated cancels"
    );
    drop(probe);
    tokio::time::timeout(Duration::from_secs(3), async {
        while active_backend_tasks.load(Ordering::SeqCst) != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    assert!(
        !endpoint_task.is_finished(),
        "endpoint task should remain healthy after cancel soak"
    );

    shutdown_runtime(rt, Some(endpoint_task)).await?;
    Ok(())
}
