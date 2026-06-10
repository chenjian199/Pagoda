// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

#![allow(dead_code)]

use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::{Mutex, MutexGuard, Once, OnceLock},
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use pagoda_runtime::{
    DistributedRuntime, Runtime,
    servicegroup::{Client, Instance, PortName, ServiceGroup, TransportType},
    config::environment_names::{nats as env_nats, runtime::system as env_system},
    discovery::{DiscoveryEvent, DiscoveryQuery, DiscoverySpec, EventTransportKind},
    distributed::{DiscoveryBackend, DistributedConfig, RequestPlaneMode},
    engine::AsyncEngine,
    pipeline::{ManyOut, RouterMode, ServiceEngine, SingleIn, network::Ingress},
    protocols::annotated::Annotated,
    service::{PortNameInfo, ServiceClient},
    storage::kv,
    system_status_server::SystemStatusServerInfo,
    traits::DistributedRuntimeProvider,
    transports::{etcd, nats},
};
use futures::StreamExt;
#[cfg(feature = "integration-kube")]
use temp_env::async_with_vars;

use super::engines::{AsyncGenerator, LlmdbaEngine as LambdaEngine};

pub type TestResponse = Annotated<String>;

/// Serializes integration tests that share process-wide request-plane state.
///
/// Use a std mutex (not `tokio::sync::Mutex`): each `#[tokio::test]` runs on its own Tokio
/// runtime, so an async mutex does not reliably serialize tests across threads.
static CONTRACT_TEST_LOCK: Mutex<()> = Mutex::new(());

static INTEGRATION_TEST_ENV: Once = Once::new();

/// Process-wide Tokio + Pagoda runtime. The TCP accept loop (`GLOBAL_TCP_SERVER` in
/// `manager.rs`) is spawned on this runtime so it survives `#[tokio::test]` teardown.
struct ProcessTestHarness {
    runtime: Runtime,
}

static PROCESS_HARNESS: OnceLock<ProcessTestHarness> = OnceLock::new();
static PROCESS_TCP_REQUEST_PLANE: OnceLock<()> = OnceLock::new();

fn process_harness() -> &'static ProcessTestHarness {
    PROCESS_HARNESS.get_or_init(|| {
        init_integration_test_env();
        let runtime = Runtime::from_settings()
            .expect("failed to create integration test runtime from settings");
        ProcessTestHarness { runtime }
    })
}

fn is_shared_process_runtime(rt: &Runtime) -> bool {
    rt.id() == process_harness().runtime.id()
}

/// Clone of the process-wide `Runtime` backing `GLOBAL_TCP_SERVER`.
///
/// Multiple `DistributedRuntime` instances on this handle get distinct `connection_id`s
/// (replica-pool tests).
pub fn shared_integration_runtime() -> Runtime {
    process_harness().runtime.clone()
}

/// Bind `GLOBAL_TCP_SERVER` on the process-wide runtime (once per test binary).
pub async fn ensure_integration_tcp_request_plane() -> Result<()> {
    if PROCESS_TCP_REQUEST_PLANE.get().is_some() {
        return Ok(());
    }
    let harness = process_harness();
    let runtime = harness.runtime.clone();
    let (tx, rx) = tokio::sync::oneshot::channel();
    harness.runtime.primary().spawn(async move {
        let result: Result<()> = async {
            let drt = DistributedRuntime::new(runtime, DistributedConfig::process_local()).await?;
            let _ = drt.request_plane_server().await?;
            Ok(())
        }
        .await;
        let _ = tx.send(result);
    });
    rx.await
        .map_err(|_| anyhow!("process integration runtime exited before TCP server started"))??;
    let _ = PROCESS_TCP_REQUEST_PLANE.set(());
    Ok(())
}

/// Hold for the entire test body so only one integration test touches global state at a time.
pub fn acquire_contract_test_lock() -> MutexGuard<'static, ()> {
    CONTRACT_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Force loopback for TCP/HTTP request plane before the process-wide server binds.
///
/// Production defaults resolve a LAN IP (`ip_resolver.rs`). On some hosts that address is not
/// listening or is unreachable from the same process, which yields `Connection refused (111)`
/// in integration tests even though discovery already shows instances.
///
/// Must run before the first `request_plane_server()` in the process (global `OnceCell`).
pub fn init_integration_test_env() {
    INTEGRATION_TEST_ENV.call_once(|| {
        // SAFETY: called once before any parallel test work.
        unsafe {
            std::env::set_var("PGD_TCP_RPC_HOST", "127.0.0.1");
            std::env::set_var("PGD_HTTP_RPC_HOST", "127.0.0.1");
            // Response-stream server expects a network interface *name*, not an IP.
            // Binding `lo` keeps call-home streams on loopback (see `TcpStreamServer`).
            #[cfg(target_os = "linux")]
            std::env::set_var("PGD_TCP_RESPONSE_STREAM_HOST", "lo");
            #[cfg(target_os = "macos")]
            std::env::set_var("PGD_TCP_RESPONSE_STREAM_HOST", "lo0");
            std::env::set_var("PGD_COMPUTE_THREADS", "2");
        }
    });
}

fn is_connection_refused(err: &anyhow::Error) -> bool {
    let mut current: Option<&dyn std::error::Error> = Some(err.as_ref());
    while let Some(e) = current {
        if e.to_string().contains("Connection refused") || e.to_string().contains("os error 111")
        {
            return true;
        }
        current = e.source();
    }
    false
}

fn is_routing_unavailable_error(err: &anyhow::Error) -> bool {
    let msg = err.to_string();
    msg.contains("no instances found") || msg.contains("not found")
}

/// Wait until `instance_avail` reflects a non-empty discovery snapshot.
///
/// `wait_for_instances` reads `instance_source` directly; `PushRouter` routes via
/// `instance_avail`, which is updated asynchronously by `Client::monitor_instance_source`.
pub async fn wait_for_instance_avail(client: &Client) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if !client.instance_ids_avail().is_empty() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?
}

/// After discovery shows instances, probe the TCP request plane (retries spawn/start races).
pub async fn confirm_tcp_rpc_ready(client: Client) -> Result<()> {
    wait_for_instance_avail(&client).await?;
    let router = round_robin_router(client).await?;
    for attempt in 0..50 {
        match router.generate("__tcp_probe__".to_string().into()).await {
            Ok(mut stream) => {
                let _ = stream.next().await;
                return Ok(());
            }
            Err(err)
                if (is_connection_refused(&err) || is_routing_unavailable_error(&err))
                    && attempt + 1 < 50 =>
            {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(err) => return Err(err),
        }
    }
    Err(anyhow!(
        "TCP request plane not reachable on 127.0.0.1 after discovery reported instances"
    ))
}

pub fn unique_name(prefix: &str) -> String {
    format!("{}-{}", prefix, uuid::Uuid::new_v4().simple())
}

/// Process-shared `Runtime` + fresh in-memory `DistributedRuntime` per test.
///
/// Discovery is isolated per `DistributedRuntime`. `GLOBAL_TCP_SERVER` is started once on
/// the process harness runtime and reused across tests in the same binary.
pub async fn process_local_runtime() -> Result<(Runtime, DistributedRuntime)> {
    init_integration_test_env();
    ensure_integration_tcp_request_plane().await?;
    let rt = shared_integration_runtime();
    let drt = DistributedRuntime::new(rt.clone(), DistributedConfig::process_local()).await?;
    Ok((rt, drt))
}

/// `Runtime` tied to the current `#[tokio::test]` handle — for shutdown/lifecycle tests only.
///
/// Do not use for TCP RPC integration: `request_plane_server()` would bind
/// `GLOBAL_TCP_SERVER` on the test runtime, which is destroyed when the test ends.
pub async fn process_local_runtime_ephemeral() -> Result<(Runtime, DistributedRuntime)> {
    init_integration_test_env();
    let rt = Runtime::from_current()?;
    let drt = DistributedRuntime::new(rt.clone(), DistributedConfig::process_local()).await?;
    Ok((rt, drt))
}

pub fn file_backed_config(kv_path: PathBuf) -> DistributedConfig {
    DistributedConfig {
        discovery_backend: DiscoveryBackend::KvStore(kv::Selector::File(kv_path)),
        nats_config: None,
        request_plane: RequestPlaneMode::Tcp,
        event_transport_kind: pagoda_runtime::discovery::EventTransportKind::Zmq,
    }
}

/// Process-shared runtime + file-backed `DistributedRuntime` for cross-DRT discovery tests.
pub async fn file_backed_runtime(kv_path: PathBuf) -> Result<(Runtime, DistributedRuntime)> {
    init_integration_test_env();
    let rt = shared_integration_runtime();
    let drt = DistributedRuntime::new(rt.clone(), file_backed_config(kv_path)).await?;
    Ok((rt, drt))
}

/// Additional DRT on the same file-backed KV path (distinct `connection_id`s).
pub async fn additional_file_backed_runtime(
    rt: Runtime,
    kv_path: &Path,
) -> Result<DistributedRuntime> {
    DistributedRuntime::new(rt, file_backed_config(kv_path.to_path_buf())).await
}

pub fn endpoint_discovery_spec(
    namespace: &str,
    servicegroup: &str,
    endpoint: &str,
) -> DiscoverySpec {
    DiscoverySpec::PortName {
        namespace: namespace.to_string(),
        servicegroup: servicegroup.to_string(),
        portname: endpoint.to_string(),
        transport: TransportType::Tcp("127.0.0.1:8080".to_string()),
        device_type: None,
    }
}

pub fn discovery_query_endpoint(
    namespace: &str,
    servicegroup: &str,
    endpoint: &str,
) -> DiscoveryQuery {
    DiscoveryQuery::PortName {
        namespace: namespace.to_string(),
        servicegroup: servicegroup.to_string(),
        portname: endpoint.to_string(),
    }
}

pub async fn wait_for_discovery_event(
    stream: &mut pagoda_runtime::discovery::DiscoveryStream,
    predicate: impl Fn(&DiscoveryEvent) -> bool,
) -> Result<DiscoveryEvent> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let event = stream
                .next()
                .await
                .ok_or_else(|| anyhow!("discovery watch stream ended"))??;
            if predicate(&event) {
                return Ok(event);
            }
        }
    })
    .await?
}

/// Stop endpoint tasks. Shuts down ephemeral runtimes only; the process harness runtime
/// is left running so `GLOBAL_TCP_SERVER` stays reachable for later tests.
pub async fn shutdown_runtime(
    rt: Runtime,
    endpoint_task: Option<tokio::task::JoinHandle<Result<()>>>,
) -> Result<()> {
    if let Some(task) = endpoint_task {
        let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
    }
    if is_shared_process_runtime(&rt) {
        return Ok(());
    }
    rt.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(2), rt.primary_token().cancelled()).await;
    Ok(())
}

pub async fn wait_for_instance_count(client: &Client, expected: usize) -> Result<Vec<Instance>> {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let instances = client.instances();
            if instances.len() == expected {
                return Ok(instances);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?
}

pub fn make_streaming_engine() -> ServiceEngine<SingleIn<String>, ManyOut<TestResponse>> {
    LambdaEngine::from_generator(AsyncGenerator::<String, TestResponse>::new(
        |(req, stream)| async move {
            for ch in req.chars() {
                if stream
                    .emit(TestResponse::from_data(ch.to_string()))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        },
    ))
}

pub async fn round_robin_router(
    client: Client,
) -> Result<pagoda_runtime::pipeline::PushRouter<String, TestResponse>> {
    pagoda_runtime::pipeline::PushRouter::<String, TestResponse>::from_client(
        client,
        RouterMode::RoundRobin,
    )
    .await
}

pub async fn collect_stream_chunks(mut response: ManyOut<TestResponse>) -> Vec<String> {
    let mut chunks = Vec::new();
    while let Some(item) = response.next().await {
        chunks.push(item.data.unwrap());
    }
    chunks
}

pub async fn serve_streaming_endpoint(
    portname: PortName,
) -> Result<(Client, tokio::task::JoinHandle<Result<()>>)> {
    serve_endpoint_with_engine(portname, make_streaming_engine()).await
}

fn is_nats_request_failure(err: &anyhow::Error) -> bool {
    let msg = err.to_string().to_lowercase();
    msg.contains("nats")
        || msg.contains("timeout")
        || msg.contains("no responders")
        || msg.contains("cannot connect")
}

async fn start_served_endpoint(
    portname: PortName,
    engine: ServiceEngine<SingleIn<String>, ManyOut<TestResponse>>,
) -> Result<(Client, tokio::task::JoinHandle<Result<()>>)> {
    init_integration_test_env();
    ensure_integration_tcp_request_plane().await?;
    let _ = portname.drt().request_plane_server().await?;
    let ingress = Ingress::for_engine(engine)?;
    // Schedule on the same Tokio runtime as `GLOBAL_TCP_SERVER` (see `ProcessTestHarness`).
    let endpoint_task = portname
        .drt()
        .runtime()
        .primary()
        .spawn(portname.portname_builder().handler(ingress).start());
    let client = portname.client().await.context("create endpoint client")?;
    client
        .wait_for_instances()
        .await
        .context("wait for discovery instances")?;
    Ok((client, endpoint_task))
}

pub async fn serve_endpoint_with_engine(
    portname: PortName,
    engine: ServiceEngine<SingleIn<String>, ManyOut<TestResponse>>,
) -> Result<(Client, tokio::task::JoinHandle<Result<()>>)> {
    let (client, endpoint_task) = start_served_endpoint(portname, engine).await?;
    confirm_tcp_rpc_ready(client.clone())
        .await
        .context("confirm TCP request plane is reachable")?;
    Ok((client, endpoint_task))
}

/// Like [`serve_endpoint_with_engine`] but skips the TCP probe RPC.
///
/// `CancellableEngine` defers its first stream item to a spawned task; the probe would block
/// until that task runs and is unnecessary when the test immediately drives RPC itself.
pub async fn serve_endpoint_with_engine_no_tcp_probe(
    portname: PortName,
    engine: ServiceEngine<SingleIn<String>, ManyOut<TestResponse>>,
) -> Result<(Client, tokio::task::JoinHandle<Result<()>>)> {
    start_served_endpoint(portname, engine).await
}

/// Single-chunk echo: response data equals request string (TCP payload roundtrip contract).
pub fn make_echo_engine() -> ServiceEngine<SingleIn<String>, ManyOut<TestResponse>> {
    LambdaEngine::from_generator(AsyncGenerator::<String, TestResponse>::new(
        |(req, stream)| async move {
            let _ = stream.emit(TestResponse::from_data(req)).await;
        },
    ))
}

pub fn model_discovery_spec(
    namespace: &str,
    servicegroup: &str,
    endpoint: &str,
    model_name: &str,
) -> DiscoverySpec {
    DiscoverySpec::Model {
        namespace: namespace.to_string(),
        servicegroup: servicegroup.to_string(),
        portname: endpoint.to_string(),
        card_json: serde_json::json!({ "display_name": model_name }),
        model_suffix: None,
    }
}

pub fn assert_no_instances_error(err: &anyhow::Error) {
    let msg = err.to_string();
    assert!(
        msg.contains("no instances found") || msg.contains("not found"),
        "expected unavailable routing error, got: {msg}"
    );
}

pub async fn wait_for_instances_empty(client: &Client) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if client.instances().is_empty() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?
}

pub async fn generate_expect_no_instances(
    router: &pagoda_runtime::pipeline::PushRouter<String, TestResponse>,
    payload: &str,
) -> Result<()> {
    let err = match router.generate(payload.to_string().into()).await {
        Ok(_) => return Err(anyhow!("expected routing failure for payload={payload}")),
        Err(err) => err,
    };
    assert_no_instances_error(&err);
    Ok(())
}

pub async fn list_endpoint_models(drt: &DistributedRuntime, namespace: &str) -> Result<usize> {
    let discovery = drt.discovery();
    let instances = discovery
        .list(DiscoveryQuery::PortNameModels {
            namespace: namespace.to_string(),
            servicegroup: "backend".to_string(),
            portname: "generate".to_string(),
        })
        .await?;
    Ok(instances.len())
}

/// Env vars to start system status HTTP on loopback with an OS-assigned port.
pub fn system_status_server_env() -> [(&'static str, Option<&'static str>); 2] {
    [
        (env_system::PGD_SYSTEM_PORT, Some("0")),
        (env_system::PGD_SYSTEM_HOST, Some("127.0.0.1")),
    ]
}

/// System status HTTP server started by `DistributedRuntime::new` when port env is set.
pub fn require_system_status_server(
    drt: &DistributedRuntime,
) -> Result<std::sync::Arc<SystemStatusServerInfo>> {
    drt.system_status_server_info().ok_or_else(|| {
        anyhow!(
            "system status server not running; set {}=0 before creating DRT",
            env_system::PGD_SYSTEM_PORT
        )
    })
}

/// GET `http://127.0.0.1:{port}{path}`; returns HTTP status code and response body text.
pub async fn system_status_http_get(
    addr: std::net::SocketAddr,
    path: &str,
) -> Result<(u16, String)> {
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{}{path}", addr.port());
    let response = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    Ok((response.status().as_u16(), response.text().await?))
}

/// POST JSON to `http://127.0.0.1:{port}{path}`; returns HTTP status code and response body text.
pub async fn system_status_http_post(
    addr: std::net::SocketAddr,
    path: &str,
    body: &serde_json::Value,
) -> Result<(u16, String)> {
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{}{path}", addr.port());
    let response = client
        .post(&url)
        .json(body)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;
    Ok((response.status().as_u16(), response.text().await?))
}

/// Default NATS URL for Nightly integration tests (`NATS_SERVER` overrides).
pub fn nats_server_url() -> String {
    std::env::var(env_nats::NATS_SERVER).unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string())
}

/// Probes broker reachability before Nightly NATS tests run.
pub async fn require_nats_broker() -> Result<()> {
    let url = nats_server_url();
    let opts = nats::ClientOptions::builder()
        .server(url.clone())
        .build()
        .map_err(|e| anyhow!("invalid NATS options for {url}: {e}"))?;
    opts.connect()
        .await
        .with_context(|| format!("NATS broker not reachable at {url}"))?;
    Ok(())
}

/// NATS microservice stats client for service-registry contract tests.
pub async fn nats_service_client() -> Result<ServiceClient> {
    require_nats_broker().await?;
    let url = nats_server_url();
    let opts = nats::ClientOptions::builder()
        .server(url)
        .build()
        .map_err(|e| anyhow!("invalid NATS options: {e}"))?;
    Ok(ServiceClient::new(opts.connect().await?))
}

/// Poll `$SRV.STATS.<service_name>` until at least `min_endpoints` NATS service endpoints appear.
pub async fn wait_for_nats_service_endpoints(
    service_name: &str,
    min_endpoints: usize,
    timeout: Duration,
) -> Result<Vec<PortNameInfo>> {
    let client = nats_service_client().await?;
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let set = client
            .collect_services(service_name, Duration::from_secs(2))
            .await?;
        let endpoints: Vec<PortNameInfo> = set.into_portnames().collect();
        if endpoints.len() >= min_endpoints {
            return Ok(endpoints);
        }
        if std::time::Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting for {min_endpoints} NATS service endpoints on {service_name}; got {}",
                endpoints.len()
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Slugified NATS service name for a component (`Component::service_name` contract).
pub fn servicegroup_service_name(component: &ServiceGroup) -> String {
    component.service_name()
}

/// Client port from `NATS_SERVER` (default 4222).
pub fn nats_broker_port() -> Result<u16> {
    let raw = nats_server_url();
    if let Ok(url) = url::Url::parse(&raw) {
        return Ok(url.port().unwrap_or(4222));
    }
    raw.rsplit(':')
        .next()
        .and_then(|p| p.trim_end_matches('/').parse().ok())
        .ok_or_else(|| anyhow!("invalid NATS_SERVER url: {raw}"))
}

fn docker_container_name_for_port(port: u16) -> Result<String> {
    let ps = Command::new("docker")
        .args([
            "ps",
            "--filter",
            &format!("publish={port}"),
            "--format",
            "{{.Names}}",
        ])
        .output()
        .context("docker ps for NATS broker")?;
    let ps_stdout = String::from_utf8_lossy(&ps.stdout);
    let names: Vec<&str> = ps_stdout
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect();
    if names.len() == 1 {
        return Ok(names[0].to_string());
    }
    anyhow::bail!(
        "expected exactly one docker container publishing port {port}, found {}",
        names.len()
    );
}

fn docker_run(args: &[&str]) -> Result<()> {
    let status = Command::new("docker")
        .args(args)
        .status()
        .with_context(|| format!("docker {}", args.join(" ")))?;
    if status.success() {
        Ok(())
    } else {
        anyhow::bail!("docker {} failed with {status}", args.join(" "));
    }
}

/// Stop the broker, hold an outage window, then start it again (drops all client TCP sessions).
fn nats_broker_stop_outage_start(port: u16, outage: Duration) -> Result<()> {
    let container = docker_container_name_for_port(port)?;
    docker_run(&["stop", &container])?;
    std::thread::sleep(outage);
    docker_run(&["start", &container])?;
    Ok(())
}

/// Poll until `NATS_SERVER` accepts new connections (after broker restart).
pub async fn wait_for_nats_broker_ready(timeout: Duration) -> Result<()> {
    tokio::time::timeout(timeout, async {
        loop {
            if require_nats_broker().await.is_ok() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .map_err(|_| anyhow!("timed out waiting for NATS broker at {}", nats_server_url()))?
}

/// Broker outage fixture: `docker stop` → hold → `docker start` (all NATS clients disconnected).
pub async fn nats_broker_stop_outage_and_start(outage: Duration) -> Result<()> {
    let port = nats_broker_port()?;
    tokio::task::spawn_blocking(move || nats_broker_stop_outage_start(port, outage))
        .await
        .context("NATS broker stop/start task join")??;
    Ok(())
}

/// Full outage cycle: stop/start docker broker, then poll until `NATS_SERVER` accepts clients.
pub async fn nats_broker_outage_and_recovery(outage: Duration) -> Result<()> {
    nats_broker_stop_outage_and_start(outage).await?;
    wait_for_nats_broker_ready(Duration::from_secs(30)).await
}

/// Returns true if any RPC attempt fails while the broker is down (generate Err or stream timeout).
pub async fn probe_nats_rpc_failure_during_outage(
    router: &pagoda_runtime::pipeline::PushRouter<String, TestResponse>,
    attempts: usize,
) -> bool {
    for _ in 0..attempts {
        let outcome: Result<Result<TestResponse, anyhow::Error>, tokio::time::error::Elapsed> =
            tokio::time::timeout(Duration::from_secs(2), async {
                let mut stream = router
                    .generate("__nats_outage_probe__".to_string().into())
                    .await?;
                stream
                    .next()
                    .await
                    .ok_or_else(|| anyhow!("empty outage probe stream"))
            })
            .await;

        match outcome {
            Err(_) => return true,
            Ok(Err(e)) if is_nats_request_failure(&e) => return true,
            Ok(Err(_)) => return true,
            Ok(Ok(_)) => {}
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

pub fn nats_distributed_config(discovery_backend: DiscoveryBackend) -> DistributedConfig {
    DistributedConfig {
        discovery_backend,
        nats_config: Some(nats::ClientOptions::default()),
        request_plane: RequestPlaneMode::Nats,
        event_transport_kind: EventTransportKind::Zmq,
    }
}

pub fn nats_file_backed_config(kv_path: PathBuf) -> DistributedConfig {
    nats_distributed_config(DiscoveryBackend::KvStore(kv::Selector::File(kv_path)))
}

/// Process-shared runtime + in-memory discovery + NATS request plane.
pub async fn nats_runtime() -> Result<(Runtime, DistributedRuntime)> {
    require_nats_broker().await?;
    init_integration_test_env();
    let rt = shared_integration_runtime();
    let drt = DistributedRuntime::new(
        rt.clone(),
        nats_distributed_config(DiscoveryBackend::KvStore(kv::Selector::Memory)),
    )
    .await?;
    Ok((rt, drt))
}

/// Shared file KV + NATS request plane (cross-DRT replica tests).
pub async fn nats_file_backed_runtime(kv_path: PathBuf) -> Result<(Runtime, DistributedRuntime)> {
    require_nats_broker().await?;
    init_integration_test_env();
    let rt = shared_integration_runtime();
    let drt = DistributedRuntime::new(rt.clone(), nats_file_backed_config(kv_path)).await?;
    Ok((rt, drt))
}

pub async fn additional_nats_file_backed_runtime(
    rt: Runtime,
    kv_path: &Path,
) -> Result<DistributedRuntime> {
    require_nats_broker().await?;
    DistributedRuntime::new(rt, nats_file_backed_config(kv_path.to_path_buf())).await
}

/// Start a NATS endpoint and wait for discovery registration only (no RPC probe).
pub async fn start_served_endpoint_nats(
    portname: PortName,
    engine: ServiceEngine<SingleIn<String>, ManyOut<TestResponse>>,
) -> Result<(Client, tokio::task::JoinHandle<Result<()>>)> {
    require_nats_broker().await?;
    init_integration_test_env();
    let _ = portname.drt().request_plane_server().await?;
    let ingress = Ingress::for_engine(engine)?;
    let endpoint_task = portname
        .drt()
        .runtime()
        .primary()
        .spawn(portname.portname_builder().handler(ingress).start());
    let client = portname.client().await.context("create endpoint client")?;
    client
        .wait_for_instances()
        .await
        .context("wait for discovery instances")?;
    Ok((client, endpoint_task))
}

pub async fn confirm_nats_rpc_ready(client: Client) -> Result<()> {
    let router = round_robin_router(client).await?;
    for attempt in 0..50 {
        match router.generate("__nats_probe__".to_string().into()).await {
            Ok(mut stream) => {
                let _ = stream.next().await;
                return Ok(());
            }
            Err(err) if is_nats_request_failure(&err) && attempt + 1 < 50 => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(err) => return Err(err),
        }
    }
    Err(anyhow!("NATS request plane not ready after discovery reported instances"))
}

pub async fn serve_streaming_endpoint_nats(
    portname: PortName,
) -> Result<(Client, tokio::task::JoinHandle<Result<()>>)> {
    serve_endpoint_with_engine_nats(portname, make_streaming_engine()).await
}

pub async fn serve_endpoint_with_engine_nats(
    portname: PortName,
    engine: ServiceEngine<SingleIn<String>, ManyOut<TestResponse>>,
) -> Result<(Client, tokio::task::JoinHandle<Result<()>>)> {
    let (client, endpoint_task) = start_served_endpoint_nats(portname, engine).await?;
    confirm_nats_rpc_ready(client.clone())
        .await
        .context("confirm NATS request plane is reachable")?;
    Ok((client, endpoint_task))
}

pub fn etcd_distributed_config(attach_lease: bool) -> DistributedConfig {
    let mut etcd_opts = etcd::ClientOptions::default();
    etcd_opts.attach_lease = attach_lease;
    DistributedConfig {
        discovery_backend: DiscoveryBackend::KvStore(kv::Selector::Etcd(Box::new(etcd_opts))),
        nats_config: None,
        request_plane: RequestPlaneMode::Tcp,
        event_transport_kind: EventTransportKind::Zmq,
    }
}

/// Probes etcd before Nightly etcd integration tests run.
pub async fn require_etcd_cluster() -> Result<()> {
    let rt = Runtime::from_current()?;
    let opts = etcd::ClientOptions::default();
    let _client = etcd::Client::new(opts, rt)
        .await
        .context("etcd cluster not reachable (set ETCD_ENDPOINTS or start localhost:2379)")?;
    Ok(())
}

/// Process-shared runtime + etcd-backed discovery (lease attached).
pub async fn etcd_runtime() -> Result<(Runtime, DistributedRuntime)> {
    require_etcd_cluster().await?;
    init_integration_test_env();
    ensure_integration_tcp_request_plane().await?;
    let rt = shared_integration_runtime();
    let drt = DistributedRuntime::new(rt.clone(), etcd_distributed_config(true)).await?;
    Ok((rt, drt))
}

/// Ephemeral runtime + etcd discovery; dropping `Runtime` cancels lease keep-alive.
pub async fn etcd_runtime_ephemeral() -> Result<(Runtime, DistributedRuntime)> {
    require_etcd_cluster().await?;
    init_integration_test_env();
    let rt = Runtime::from_current()?;
    let drt = DistributedRuntime::new(rt.clone(), etcd_distributed_config(true)).await?;
    Ok((rt, drt))
}

/// Direct etcd client for discovery contract tests (no lease on keys).
pub async fn etcd_test_client() -> Result<etcd::Client> {
    require_etcd_cluster().await?;
    let rt = Runtime::from_current()?;
    etcd::Client::new(
        etcd::ClientOptions {
            attach_lease: false,
            ..Default::default()
        },
        rt,
    )
    .await
    .map_err(Into::into)
}

/// Prefix-delete all discovery instance keys for a namespace in etcd.
pub async fn etcd_delete_discovery_namespace_prefix(namespace: &str) -> Result<()> {
    use etcd_client::DeleteOptions;

    let client = etcd_test_client().await?;
    let prefix = format!("v1/instances/{namespace}/");
    client
        .kv_delete(prefix, Some(DeleteOptions::new().with_prefix()))
        .await?;
    Ok(())
}

/// Remove all file-backed discovery keys for a namespace (simulates external store outage).
///
/// Instance keys are flat url-encoded files under `v1/instances/` (not nested directories).
pub fn wipe_file_discovery_namespace(kv_root: &Path, namespace: &str) -> Result<()> {
    let instances_dir = kv_root.join("v1/instances");
    if !instances_dir.exists() {
        return Ok(());
    }
    let prefix = format!("{namespace}/");
    for entry in std::fs::read_dir(&instances_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let encoded = entry.file_name().to_string_lossy().into_owned();
        let decoded = kv::Key::from_url_safe(&encoded);
        if decoded.as_ref().starts_with(&prefix) {
            std::fs::remove_file(&path)
                .with_context(|| format!("remove discovery file {}", path.display()))?;
        }
    }
    Ok(())
}

/// NATS KV bucket for storage contract tests (uses public `NATSStore` + `Store` trait).
pub async fn nats_kv_test_bucket() -> Result<Box<dyn kv::Bucket>> {
    use pagoda_runtime::storage::kv::{NATSStore, Store};

    require_nats_broker().await?;
    let client = nats::ClientOptions::default().connect().await?;
    let endpoint = pagoda_runtime::protocols::PortNameId {
        namespace: unique_name("nats-kv"),
        servicegroup: "integration".to_string(),
        name: "store".to_string(),
    };
    let store = NATSStore::new(client, endpoint);
    let bucket_name = format!("it-nats-{}", unique_name("kv"));
    store
        .get_or_create_bucket(&bucket_name, None)
        .await
        .map(|bucket| Box::new(bucket) as Box<dyn kv::Bucket>)
        .map_err(Into::into)
}

/// Direct etcd `kv::Manager` for storage contract tests (no lease on client).
pub async fn etcd_kv_manager() -> Result<kv::Manager> {
    require_etcd_cluster().await?;
    let rt = Runtime::from_current()?;
    let etcd_client = etcd::Client::new(
        etcd::ClientOptions {
            attach_lease: false,
            ..Default::default()
        },
        rt,
    )
    .await?;
    Ok(kv::Manager::etcd(etcd_client))
}

#[cfg(feature = "integration-kube")]
const KUBE_DISCOVERY_BACKEND_LABEL: &str = "nvidia.com/pagoda-discovery-backend";
#[cfg(feature = "integration-kube")]
const KUBE_DISCOVERY_ENABLED_LABEL: &str = "nvidia.com/pagoda-discovery-enabled";
/// Matches `DEBOUNCE_DURATION` in `discovery/kube/daemon.rs`.
#[cfg(feature = "integration-kube")]
const KUBE_DISCOVERY_DAEMON_DEBOUNCE: Duration = Duration::from_millis(500);
#[cfg(feature = "integration-kube")]
const KUBE_DISCOVERY_LIST_TIMEOUT: Duration = Duration::from_secs(10);

#[cfg(feature = "integration-kube")]
pub fn kube_distributed_config() -> DistributedConfig {
    DistributedConfig {
        discovery_backend: DiscoveryBackend::Kubernetes,
        nats_config: None,
        request_plane: RequestPlaneMode::Tcp,
        event_transport_kind: EventTransportKind::Zmq,
    }
}

/// Synthetic pod identity for out-of-cluster Release tests (`POD_*` env overrides).
#[cfg(feature = "integration-kube")]
pub fn kube_test_pod_identity() -> (String, String, String) {
    let pod_name = std::env::var("POD_NAME").unwrap_or_else(|_| unique_name("kube-itest"));
    let pod_uid = std::env::var("POD_UID").unwrap_or_else(|_| format!("uid-{}", unique_name("kube")));
    let pod_namespace =
        std::env::var("POD_NAMESPACE").unwrap_or_else(|_| "default".to_string());
    (pod_name, pod_uid, pod_namespace)
}

/// Probes Kubernetes API before Release kube integration tests run.
#[cfg(feature = "integration-kube")]
pub async fn require_kube_cluster() -> Result<()> {
    kube::Client::try_default()
        .await
        .context(
            "Kubernetes API not reachable (set KUBECONFIG, install PagodaWorkerMetadata CRD, or run in-cluster)",
        )?;
    Ok(())
}

/// Marks the test pod as ready in an EndpointSlice so the discovery daemon includes CR metadata.
///
/// Production correlates `PagodaWorkerMetadata` CRs with ready EndpointSlice entries
/// (`discovery/kube/daemon.rs`). Out-of-cluster tests must install this fixture explicitly,
/// including a real Pod so `PagodaWorkerMetadata` ownerReferences are not garbage-collected.
#[cfg(feature = "integration-kube")]
pub struct KubeReadinessFixture {
    client: kube::Client,
    slice_name: Option<String>,
    pod_name: Option<String>,
    pod_uid: String,
    pod_namespace: Option<String>,
    cr_name: Option<String>,
    /// When true, teardown deletes the Pod created for this fixture.
    created_pod: bool,
}

#[cfg(feature = "integration-kube")]
fn kube_discovery_resource_labels() -> std::collections::BTreeMap<String, String> {
    [
        (
            KUBE_DISCOVERY_BACKEND_LABEL.to_string(),
            "kubernetes".to_string(),
        ),
        (KUBE_DISCOVERY_ENABLED_LABEL.to_string(), "true".to_string()),
    ]
    .into_iter()
    .collect()
}

#[cfg(feature = "integration-kube")]
async fn ensure_kube_test_pod(
    client: &kube::Client,
    pod_name: &str,
    pod_namespace: &str,
    container_name: &str,
    with_discovery_labels: bool,
) -> Result<(String, bool)> {
    use k8s_openapi::api::core::v1::{Container, Pod, PodSpec};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use kube::Api;
    use kube::api::{DeleteParams, PostParams};

    let pod_api: Api<Pod> = Api::namespaced(client.clone(), pod_namespace);
    let mut labels = None;
    if with_discovery_labels {
        labels = Some(kube_discovery_resource_labels());
    }

    match pod_api.get(pod_name).await {
        Ok(existing) => Ok((
            existing
                .metadata
                .uid
                .context("existing pod missing metadata.uid")?,
            false,
        )),
        Err(kube::Error::Api(err)) if err.code == 404 => {
            let _ = pod_api.delete(pod_name, &DeleteParams::default()).await;
            let created = pod_api
                .create(
                    &PostParams::default(),
                    &Pod {
                        metadata: ObjectMeta {
                            name: Some(pod_name.to_string()),
                            namespace: Some(pod_namespace.to_string()),
                            labels,
                            ..Default::default()
                        },
                        spec: Some(PodSpec {
                            containers: vec![Container {
                                name: container_name.to_string(),
                                image: Some("registry.k8s.io/pause:3.9".to_string()),
                                ..Default::default()
                            }],
                            restart_policy: Some("Never".into()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                )
                .await
                .with_context(|| format!("create Pod {pod_name} in {pod_namespace}"))?;
            Ok((
                created
                    .metadata
                    .uid
                    .context("created pod missing metadata.uid")?,
                true,
            ))
        }
        Err(e) => Err(e.into()),
    }
}

#[cfg(feature = "integration-kube")]
async fn create_endpoint_slice_for_pod(
    client: &kube::Client,
    pod_name: &str,
    pod_namespace: &str,
) -> Result<String> {
    use k8s_openapi::api::core::v1::ObjectReference;
    use k8s_openapi::api::discovery::v1::{Endpoint, EndpointConditions, EndpointPort, EndpointSlice};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use kube::Api;
    use kube::api::{DeleteParams, PostParams};

    let api: Api<EndpointSlice> = Api::namespaced(client.clone(), pod_namespace);
    let slice_name = format!("{pod_name}-itest-eps");
    let service_name = format!("{pod_name}-itest-svc");
    let _ = api.delete(&slice_name, &DeleteParams::default()).await;

    let mut labels = kube_discovery_resource_labels();
    labels.insert("kubernetes.io/service-name".to_string(), service_name);

    let slice = EndpointSlice {
        metadata: ObjectMeta {
            name: Some(slice_name.clone()),
            namespace: Some(pod_namespace.to_string()),
            labels: Some(labels),
            ..Default::default()
        },
        address_type: "IPv4".to_string(),
        endpoints: vec![Endpoint {
            addresses: vec!["10.0.0.1".to_string()],
            conditions: Some(EndpointConditions {
                ready: Some(true),
                ..Default::default()
            }),
            target_ref: Some(ObjectReference {
                kind: Some("Pod".to_string()),
                name: Some(pod_name.to_string()),
                namespace: Some(pod_namespace.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }],
        ports: Some(vec![EndpointPort {
            port: Some(8080),
            protocol: Some("TCP".to_string()),
            ..Default::default()
        }]),
        ..Default::default()
    };

    api.create(&PostParams::default(), &slice)
        .await
        .with_context(|| format!("create EndpointSlice {slice_name} in {pod_namespace}"))?;
    Ok(slice_name)
}

#[cfg(feature = "integration-kube")]
async fn patch_pod_container_ready(
    client: &kube::Client,
    pod_name: &str,
    pod_namespace: &str,
    container_name: &str,
) -> Result<()> {
    use k8s_openapi::api::core::v1::Pod;
    use kube::Api;
    use kube::api::{Patch, PatchParams};

    let pod_api: Api<Pod> = Api::namespaced(client.clone(), pod_namespace);
    let status = serde_json::json!({
        "status": {
            "phase": "Running",
            "containerStatuses": [{
                "name": container_name,
                "ready": true,
                "image": "registry.k8s.io/pause:3.9"
            }]
        }
    });
    pod_api
        .patch_status(
            pod_name,
            &PatchParams::default(),
            &Patch::Merge(&status),
        )
        .await
        .with_context(|| format!("patch Pod {pod_name} status in {pod_namespace}"))?;
    Ok(())
}

#[cfg(feature = "integration-kube")]
impl KubeReadinessFixture {
    pub fn pod_uid(&self) -> &str {
        &self.pod_uid
    }

    pub fn pod_name(&self) -> &str {
        self.pod_name.as_deref().unwrap_or("")
    }

    pub fn pod_namespace(&self) -> &str {
        self.pod_namespace.as_deref().unwrap_or("default")
    }

    pub fn client(&self) -> &kube::Client {
        &self.client
    }

    /// Pod + ready EndpointSlice (default pod-mode discovery readiness).
    pub async fn install(pod_name: &str, pod_namespace: &str) -> Result<Self> {
        let client = kube::Client::try_default().await?;
        let (pod_uid, created_pod) =
            ensure_kube_test_pod(&client, pod_name, pod_namespace, "main", false).await?;
        let slice_name = create_endpoint_slice_for_pod(&client, pod_name, pod_namespace).await?;
        Ok(Self {
            client,
            slice_name: Some(slice_name),
            pod_name: Some(pod_name.to_string()),
            pod_uid,
            pod_namespace: Some(pod_namespace.to_string()),
            cr_name: Some(pod_name.to_string()),
            created_pod,
        })
    }

    /// Pod only — no EndpointSlice yet (for ready-endpoint + CR correlation tests).
    pub async fn install_pod_only(pod_name: &str, pod_namespace: &str) -> Result<Self> {
        let client = kube::Client::try_default().await?;
        let (pod_uid, created_pod) =
            ensure_kube_test_pod(&client, pod_name, pod_namespace, "main", false).await?;
        Ok(Self {
            client,
            slice_name: None,
            pod_name: Some(pod_name.to_string()),
            pod_uid,
            pod_namespace: Some(pod_namespace.to_string()),
            cr_name: Some(pod_name.to_string()),
            created_pod,
        })
    }

    /// Container-mode readiness: labeled Pod with `containerStatuses.ready=true` (no EndpointSlice).
    pub async fn install_container_mode(
        pod_name: &str,
        pod_namespace: &str,
        container_name: &str,
    ) -> Result<Self> {
        let client = kube::Client::try_default().await?;
        let (pod_uid, created_pod) = ensure_kube_test_pod(
            &client,
            pod_name,
            pod_namespace,
            container_name,
            true,
        )
        .await?;
        patch_pod_container_ready(&client, pod_name, pod_namespace, container_name).await?;
        Ok(Self {
            client,
            slice_name: None,
            pod_name: Some(pod_name.to_string()),
            pod_uid,
            pod_namespace: Some(pod_namespace.to_string()),
            cr_name: Some(pod_name.to_string()),
            created_pod,
        })
    }

    /// Add a ready EndpointSlice for an existing fixture pod (pod-mode discovery).
    pub async fn install_endpoint_slice(&mut self) -> Result<()> {
        if self.slice_name.is_some() {
            return Ok(());
        }
        let pod_name = self
            .pod_name
            .as_deref()
            .context("fixture missing pod name")?;
        let pod_namespace = self.pod_namespace();
        let slice_name =
            create_endpoint_slice_for_pod(&self.client, pod_name, pod_namespace).await?;
        self.slice_name = Some(slice_name);
        Ok(())
    }

    /// Delete readiness resources and the pod; CR is garbage-collected via ownerReference.
    pub async fn delete_pod_and_clear_readiness(&mut self) -> Result<()> {
        let pod_name = self.pod_name.clone().unwrap_or_default();
        let pod_namespace = self.pod_namespace.clone().unwrap_or_default();
        let cr_name = self.cr_name.clone().unwrap_or_default();
        let slice_name = self.slice_name.take();
        let created_pod = self.created_pod;
        teardown_kube_readiness_fixture(
            self.client.clone(),
            slice_name.as_deref(),
            &pod_name,
            &pod_namespace,
            &cr_name,
            created_pod,
        )
        .await?;
        self.created_pod = false;
        Ok(())
    }

    pub async fn teardown(mut self) -> Result<()> {
        let slice_name = self.slice_name.take();
        let pod_name = self.pod_name.take().unwrap_or_default();
        let pod_namespace = self.pod_namespace.take().unwrap_or_default();
        let cr_name = self.cr_name.take().unwrap_or_default();
        let created_pod = self.created_pod;
        if slice_name.is_none() && !created_pod {
            return Ok(());
        }
        teardown_kube_readiness_fixture(
            self.client.clone(),
            slice_name.as_deref(),
            &pod_name,
            &pod_namespace,
            &cr_name,
            created_pod,
        )
        .await
    }
}

/// Two pods with independent readiness fixtures for cross-pod discovery tests.
#[cfg(feature = "integration-kube")]
pub struct KubeDualPodFixture {
    pub pod_a: KubeReadinessFixture,
    pub pod_b: KubeReadinessFixture,
}

#[cfg(feature = "integration-kube")]
impl KubeDualPodFixture {
    pub async fn install(
        pod_a_name: &str,
        pod_b_name: &str,
        pod_namespace: &str,
    ) -> Result<Self> {
        let pod_a = KubeReadinessFixture::install(pod_a_name, pod_namespace).await?;
        let pod_b = KubeReadinessFixture::install(pod_b_name, pod_namespace).await?;
        Ok(Self { pod_a, pod_b })
    }

    pub async fn teardown(self) -> Result<()> {
        self.pod_a.teardown().await?;
        self.pod_b.teardown().await?;
        Ok(())
    }
}

#[cfg(feature = "integration-kube")]
impl Drop for KubeReadinessFixture {
    fn drop(&mut self) {
        let slice_name = self.slice_name.take();
        let created_pod = self.created_pod;
        if slice_name.is_none() && !created_pod {
            return;
        }
        let pod_name = self.pod_name.take().unwrap_or_default();
        let pod_namespace = self.pod_namespace.take().unwrap_or_default();
        let cr_name = self.cr_name.take().unwrap_or_default();
        let client = self.client.clone();
        // Cannot block_on the #[tokio::test] runtime; spawn a short-lived thread instead.
        let _ = std::thread::Builder::new()
            .name("kube-fixture-teardown".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build();
                if let Ok(rt) = rt {
                    if let Err(e) = rt.block_on(teardown_kube_readiness_fixture(
                        client,
                        slice_name.as_deref(),
                        &pod_name,
                        &pod_namespace,
                        &cr_name,
                        created_pod,
                    )) {
                        eprintln!("KubeReadinessFixture teardown failed: {e:#}");
                    }
                }
            });
    }
}

#[cfg(feature = "integration-kube")]
async fn teardown_kube_readiness_fixture(
    client: kube::Client,
    slice_name: Option<&str>,
    pod_name: &str,
    pod_namespace: &str,
    cr_name: &str,
    delete_pod: bool,
) -> Result<()> {
    use k8s_openapi::api::core::v1::Pod;
    use k8s_openapi::api::discovery::v1::EndpointSlice;
    use kube::Api;
    use kube::api::DeleteParams;
    use kube::core::{ApiResource, DynamicObject, GroupVersionKind};

    if let Some(slice_name) = slice_name {
        let slice_api: Api<EndpointSlice> = Api::namespaced(client.clone(), pod_namespace);
        match slice_api.delete(slice_name, &DeleteParams::default()).await {
            Ok(_) => {}
            Err(kube::Error::Api(err)) if err.code == 404 => {}
            Err(e) => return Err(e.into()),
        }
    }

    let gvk = GroupVersionKind::gvk("nvidia.com", "v1alpha1", "PagodaWorkerMetadata");
    let ar = ApiResource::from_gvk(&gvk);
    let cr_api: Api<DynamicObject> = Api::namespaced_with(client.clone(), pod_namespace, &ar);
    match cr_api.delete(cr_name, &DeleteParams::default()).await {
        Ok(_) => {}
        Err(kube::Error::Api(err)) if err.code == 404 => {}
        Err(e) => return Err(e.into()),
    }

    if delete_pod && !pod_name.is_empty() {
        let pod_api: Api<Pod> = Api::namespaced(client, pod_namespace);
        match pod_api.delete(pod_name, &DeleteParams::default()).await {
            Ok(_) => {}
            Err(kube::Error::Api(err)) if err.code == 404 => {}
            Err(e) => return Err(e.into()),
        }
    }

    Ok(())
}

/// Poll until `list` returns the expected number of instances (daemon debounce + CR watch).
#[cfg(feature = "integration-kube")]
pub async fn wait_for_discovery_list(
    drt: &DistributedRuntime,
    query: DiscoveryQuery,
    expected_len: usize,
) -> Result<Vec<pagoda_runtime::discovery::DiscoveryInstance>> {
    let deadline = tokio::time::Instant::now() + KUBE_DISCOVERY_LIST_TIMEOUT;
    loop {
        let listed = drt.discovery().list(query.clone()).await?;
        if listed.len() == expected_len {
            return Ok(listed);
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "discovery list for {query:?} returned {} instances, expected {expected_len}",
                listed.len()
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[cfg(feature = "integration-kube")]
pub async fn kube_wait_for_daemon_settle() {
    tokio::time::sleep(KUBE_DISCOVERY_DAEMON_DEBOUNCE + Duration::from_millis(200)).await;
}

/// Build a `DistributedRuntime` with explicit pod/container identity for kube discovery.
#[cfg(feature = "integration-kube")]
pub async fn kube_runtime_for_identity(
    pod_name: &str,
    pod_uid: &str,
    pod_namespace: &str,
    mode: &str,
    container_name: Option<&str>,
) -> Result<(Runtime, DistributedRuntime)> {
    require_kube_cluster().await?;
    init_integration_test_env();
    ensure_integration_tcp_request_plane().await?;

    let mut vars: Vec<(&str, Option<&str>)> = vec![
        ("POD_NAME", Some(pod_name)),
        ("POD_UID", Some(pod_uid)),
        ("POD_NAMESPACE", Some(pod_namespace)),
        ("PGD_KUBE_DISCOVERY_MODE", Some(mode)),
    ];
    if let Some(name) = container_name {
        vars.push(("CONTAINER_NAME", Some(name)));
    }

    let (rt, drt) = async_with_vars(vars, async {
        let rt = shared_integration_runtime();
        let drt = DistributedRuntime::new(rt.clone(), kube_distributed_config()).await?;
        Ok::<_, anyhow::Error>((rt, drt))
    })
    .await?;
    kube_wait_for_daemon_settle().await;
    Ok((rt, drt))
}

/// Two kube-backed DRTs on distinct pod identities (shared process `Runtime`).
#[cfg(feature = "integration-kube")]
pub async fn kube_dual_pod_runtimes(
    fixture: &KubeDualPodFixture,
) -> Result<(Runtime, DistributedRuntime, DistributedRuntime)> {
    let pod_namespace = fixture.pod_a.pod_namespace();
    let (rt, drt_a) = kube_runtime_for_identity(
        fixture.pod_a.pod_name(),
        fixture.pod_a.pod_uid(),
        pod_namespace,
        "pod",
        None,
    )
    .await?;
    let drt_b = async_with_vars(
        [
            ("POD_NAME", Some(fixture.pod_b.pod_name())),
            ("POD_UID", Some(fixture.pod_b.pod_uid())),
            ("POD_NAMESPACE", Some(pod_namespace)),
            ("PGD_KUBE_DISCOVERY_MODE", Some("pod")),
        ],
        async {
            DistributedRuntime::new(rt.clone(), kube_distributed_config()).await
        },
    )
    .await?;
    kube_wait_for_daemon_settle().await;
    Ok((rt, drt_a, drt_b))
}

/// Apply a `PagodaWorkerMetadata` CR whose `spec.data` cannot deserialize to `DiscoveryMetadata`.
#[cfg(feature = "integration-kube")]
pub async fn kube_apply_invalid_worker_metadata_cr(
    client: &kube::Client,
    namespace: &str,
    cr_name: &str,
    pod_name: &str,
    pod_uid: &str,
) -> Result<()> {
    use kube::Api;
    use kube::api::{Patch, PatchParams};
    use kube::core::{ApiResource, DynamicObject, GroupVersionKind};

    let cr = serde_json::json!({
        "apiVersion": "nvidia.com/v1alpha1",
        "kind": "PagodaWorkerMetadata",
        "metadata": {
            "name": cr_name,
            "ownerReferences": [{
                "apiVersion": "v1",
                "kind": "Pod",
                "name": pod_name,
                "uid": pod_uid,
                "controller": true,
                "blockOwnerDeletion": false
            }]
        },
        "spec": {
            // CRD requires `data` to be an object; use wrong field types so the API accepts
            // the CR but `DiscoveryMetadata` deserialization fails in the daemon.
            "data": {
                "endpoints": "not-a-hash-map",
                "model_cards": {},
                "event_channels": {}
            }
        }
    });
    let gvk = GroupVersionKind::gvk("nvidia.com", "v1alpha1", "PagodaWorkerMetadata");
    let ar = ApiResource::from_gvk(&gvk);
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), namespace, &ar);
    let params = PatchParams::apply("pagoda-integration-test").force();
    api.patch(cr_name, &params, &Patch::Apply(&cr))
        .await
        .with_context(|| format!("apply invalid PagodaWorkerMetadata CR {cr_name}"))?;
    Ok(())
}

/// Process-shared runtime + `KubeDiscoveryClient` backend (requires pod identity env).
#[cfg(feature = "integration-kube")]
pub async fn kube_runtime() -> Result<(Runtime, DistributedRuntime, KubeReadinessFixture)> {
    let (pod_name, _synthetic_pod_uid, pod_namespace) = kube_test_pod_identity();
    let fixture = KubeReadinessFixture::install(&pod_name, &pod_namespace).await?;
    let pod_uid = fixture.pod_uid().to_string();
    let (rt, drt) =
        kube_runtime_for_identity(&pod_name, &pod_uid, &pod_namespace, "pod", None).await?;
    Ok((rt, drt, fixture))
}
