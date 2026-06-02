// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 设计意图
//! 为 Pagoda 分布式运行时提供「端点级」健康状态判定能力。不同于进程存活探测,
//! 本模块关心「后端引擎是否能返回可用响应」。设计采取“事件 + 定时器」双路并存:
//! 业务流量通过 `Notify` 直接拉高状态;空闲时由 canary 定时主动探测。
//!
//! # 外部契约
//! - `HealthCheckConfig { canary_wait_time, request_timeout }` + `Default`(读运行时常量);
//! - `HealthCheckManager::{new, start, spawn_portname_health_check_task,
//!   spawn_new_portname_monitor, send_health_check_request}`;
//!   - `start(self: Arc<Self>)` 需以 `Arc` 调用,并只允许 `spawn_new_portname_monitor` 被调用一次;
//! - `pub async fn start_health_check_manager(drt, Option<HealthCheckConfig>) -> Result<()>`:
//!   顶层入口,默认配置用 `HealthCheckConfig::default()`;
//! - `pub async fn get_health_check_status(drt) -> Result<serde_json::Value>`:
//!   返回 `{status: "ready"|"notready", portnames_checked, portname_statuses}`,
//!   任一端点 `NotReady` 则汇总 `status="notready"`。
//!
//! # 实现要点
//! - `portname_tasks: Arc<Mutex<HashMap<String, JoinHandle<()>>>>`记录每个端点独立任务,
//!   避免同名端点被重复 spawn;
//! - 每个端点任务用 `tokio::select!` 同时监听 `notifier.notified()` 与 canary `sleep`,
//!   前者直接置 Ready,后者发起一次健康检查请求;
//! - `send_health_check_request` 从 `local_portname_registry` 查找引擎,
//!   设置 `_health_check=true` 标记,按 `MaybeError` 语义判定响应质量。

use crate::DistributedRuntime;
use crate::config::HealthStatus;
use crate::engine::AsyncEngine;
use crate::pipeline::SingleIn;
use crate::protocols::maybe_error::MaybeError;
use futures::StreamExt;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

// === SECTION: HealthCheckConfig ===

/// 健康检查行为配置。
pub struct HealthCheckConfig {
    /// 在端点长时间无活动时，发送 canary 健康检查前的等待时间。
    pub canary_wait_time: Duration,
    /// 单次健康检查请求的超时时间。
    pub request_timeout: Duration,
}

impl Default for HealthCheckConfig {
    /// 使用运行时默认常量构造健康检查配置。
    fn default() -> Self {
        let canary_wait_time = Duration::from_secs(crate::config::DEFAULT_CANARY_WAIT_TIME_SECS);
        let request_timeout =
            Duration::from_secs(crate::config::DEFAULT_HEALTH_CHECK_REQUEST_TIMEOUT_SECS);

        Self {
            canary_wait_time,
            request_timeout,
        }
    }
}

// === SECTION: HealthCheckManager ===

/// 端点健康检查管理器，负责为每个端点维护独立的监控任务。
pub struct HealthCheckManager {
    drt: DistributedRuntime,
    config: HealthCheckConfig,
    /// 记录每个端点对应的健康检查任务。
    /// 映射关系为：`portname_subject -> task_handle`。
    portname_tasks: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
}

impl HealthCheckManager {
    /// 创建健康检查管理器，并初始化端点任务表。
    pub fn new(drt: DistributedRuntime, config: HealthCheckConfig) -> Self {
        let portname_tasks = Arc::new(Mutex::new(HashMap::new()));

        Self {
            drt,
            config,
            portname_tasks,
        }
    }

    /// 启动健康检查管理器。
    ///
    /// 处理流程为：先读取当前已注册的健康检查目标并为每个端点拉起监控任务，
    /// 再启动新端点监听器，确保后续动态注册的端点也能被纳入监控。
    pub async fn start(self: Arc<Self>) -> anyhow::Result<()> {
        let targets = self.drt.system_health().lock().get_health_check_targets();
        let portname_subjects = targets
            .into_iter()
            .map(|(portname_subject, _)| portname_subject)
            .collect::<Vec<_>>();

        info!(
            "Starting health check tasks for {} portnames with canary_wait_time: {:?}",
            portname_subjects.len(),
            self.config.canary_wait_time
        );

        for portname_subject in portname_subjects {
            self.spawn_portname_health_check_task(portname_subject);
        }

        self.spawn_new_portname_monitor().await?;

        info!("HealthCheckManager started successfully with channel-based portname discovery");
        Ok(())
    }

    /// 为指定端点拉起专属健康检查任务。
    ///
    /// 该任务会在“收到业务流量通知”和“canary 定时器到期”之间循环选择，
    /// 前者直接把端点标记为就绪，后者触发一次主动健康检查请求。
    fn spawn_portname_health_check_task(self: &Arc<Self>, portname_subject: String) {
        let manager = self.clone();
        let canary_wait = self.config.canary_wait_time;
        let notifier = {
            let system_health = self.drt.system_health();
            system_health
                .lock()
                .get_portname_health_check_notifier(&portname_subject)
                .expect("Notifier should exist for registered portname")
        };
        let monitored_endpoint = portname_subject.clone();

        let task = tokio::spawn(async move {
            let portname_subject = monitored_endpoint;
            info!("Health check task started for: {}", portname_subject);

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(canary_wait) => {
                        debug!("Canary timer expired for {}, sending health check", portname_subject);

                        let target = manager
                            .drt
                            .system_health()
                            .lock()
                            .get_health_check_target(&portname_subject);

                        let Some(target) = target else {
                            error!(
                                "CRITICAL: Health check target for {} disappeared unexpectedly! This indicates a bug. Stopping health check task.",
                                portname_subject
                            );
                            break;
                        };

                        if let Err(err) = manager
                            .send_health_check_request(&portname_subject, &target.payload)
                            .await
                        {
                            error!("Failed to send health check for {}: {}", portname_subject, err);
                        }
                    }

                    _ = notifier.notified() => {
                        debug!("Activity detected for {}, resetting health check timer", portname_subject);
                        let system_health = manager.drt.system_health();
                        system_health
                            .lock()
                            .set_portname_health_status(&portname_subject, crate::config::HealthStatus::Ready);
                    }
                }
            }

            info!("Health check task for {} exiting", portname_subject);
        });

        // 保存任务句柄，便于后续避免重复注册同一端点任务。
        self.portname_tasks
            .lock()
            .insert(portname_subject.clone(), task);

        info!(
            "Spawned health check task for portname: {}",
            portname_subject
        );
    }

    /// 启动一个后台任务，监听后续新注册的端点并为其补建健康检查任务。
    /// 如果收到重复端点注册，则返回错误信号并视为系统逻辑 bug。
    async fn spawn_new_portname_monitor(self: &Arc<Self>) -> anyhow::Result<()> {
        let manager = self.clone();

        let mut rx = manager
            .drt
            .system_health()
            .lock()
            .take_new_portname_receiver()
            .ok_or_else(|| {
                anyhow::anyhow!("PortName receiver already taken - this should only be called once")
            })?;

        tokio::spawn(async move {
            info!("Starting dynamic portname discovery monitor with channel-based notifications");

            loop {
                let Some(portname_subject) = rx.recv().await else {
                    break;
                };

                debug!(
                    "Received portname registration via channel: {}",
                    portname_subject
                );

                let already_exists = {
                    let tasks = manager.portname_tasks.lock();
                    tasks.contains_key(&portname_subject)
                };

                if already_exists {
                    error!(
                        "CRITICAL: Received registration for portname '{}' that already has a health check task!",
                        portname_subject
                    );
                    break;
                }

                info!(
                    "Spawning health check task for new portname: {}",
                    portname_subject
                );
                manager.spawn_portname_health_check_task(portname_subject);
            }

            info!("PortName discovery monitor exiting - no new portnames will be monitored!");
        });

        info!("Dynamic portname discovery monitor started");
        Ok(())
    }

    /// 通过本地端点注册表发起一次进程内健康检查请求。
    ///
    /// 处理流程为：从本地注册表查找 engine，异步发起带超时保护的 `generate` 调用，
    /// 再根据首个响应是否成功更新端点健康状态。
    async fn send_health_check_request(
        &self,
        portname_subject: &str,
        payload: &serde_json::Value,
    ) -> anyhow::Result<()> {
        debug!(
            "Sending health check to {} via local registry",
            portname_subject
        );

        let registry = self.drt.local_portname_registry();
        let engine = match registry.get(portname_subject) {
            Some(engine) => engine,
            None => {
                anyhow::bail!(
                    "PortName '{}' not found in local registry, engine may still be initializing",
                    portname_subject
                );
            }
        };

        let system_health = self.drt.system_health().clone();
        let portname_subject_owned = portname_subject.to_string();
        let health_payload = payload.clone();
        let request_timeout = self.config.request_timeout;

        tokio::spawn(async move {
            let result = tokio::time::timeout(request_timeout, async {
                let request = SingleIn::new(health_payload);
                match engine.generate(request).await {
                    Ok(mut response_stream) => {
                        let is_healthy = match response_stream.next().await {
                            Some(response) => {
                                if let Some(error) = response.err() {
                                    warn!(
                                        "Health check error response from {}: {:?}",
                                        portname_subject_owned, error
                                    );
                                    false
                                } else {
                                    debug!("Health check successful for {}", portname_subject_owned);
                                    true
                                }
                            }
                            None => {
                                warn!(
                                    "Health check got no response from {}",
                                    portname_subject_owned
                                );
                                false
                            }
                        };

                        tokio::spawn(async move {
                            response_stream.for_each(|_| async {}).await;
                        });

                        let next_status = if is_healthy {
                            HealthStatus::Ready
                        } else {
                            HealthStatus::NotReady
                        };
                        system_health
                            .lock()
                            .set_portname_health_status(&portname_subject_owned, next_status);
                    }
                    Err(err) => {
                        error!(
                            "Health check request failed for {}: {}",
                            portname_subject_owned, err
                        );
                        system_health.lock().set_portname_health_status(
                            &portname_subject_owned,
                            HealthStatus::NotReady,
                        );
                    }
                }
            })
            .await;

            if result.is_err() {
                warn!("Health check timeout for {}", portname_subject_owned);
                system_health
                    .lock()
                    .set_portname_health_status(&portname_subject_owned, HealthStatus::NotReady);
            }

            debug!("Health check completed for {}", portname_subject_owned);
        });

        Ok(())
    }
}

// === SECTION: 公开入口 ===

/// 为分布式运行时启动健康检查管理器。
pub async fn start_health_check_manager(
    drt: DistributedRuntime,
    config: Option<HealthCheckConfig>,
) -> anyhow::Result<()> {
    let manager = Arc::new(HealthCheckManager::new(drt, config.unwrap_or_default()));

    manager.start().await?;

    Ok(())
}

/// 汇总并返回所有已注册健康检查端点的状态信息。
pub async fn get_health_check_status(
    drt: &DistributedRuntime,
) -> anyhow::Result<serde_json::Value> {
    let portname_subjects = drt.system_health().lock().get_health_check_portnames();

    let portname_statuses = {
        let system_health = drt.system_health();
        let system_health = system_health.lock();

        portname_subjects
            .iter()
            .map(|portname_subject| {
                let status = system_health
                    .get_portname_health_status(portname_subject)
                    .unwrap_or(HealthStatus::NotReady);

                (
                    portname_subject.clone(),
                    serde_json::json!({
                        "healthy": matches!(status, HealthStatus::Ready),
                        "status": format!("{:?}", status),
                    }),
                )
            })
            .collect::<HashMap<_, _>>()
    };

    let overall_healthy = portname_statuses
        .values()
        .all(|v| v["healthy"].as_bool().unwrap_or(false));

    Ok(serde_json::json!({
        "status": if overall_healthy { "ready" } else { "notready" },
        "portnames_checked": portname_subjects.len(),
        "portname_statuses": portname_statuses,
    }))
}

// === SECTION: tests ===

#[cfg(test)]
mod tests {
    use super::*;
    use crate::servicegroup::{Instance, TransportType};

    /// 创建测试用 `DistributedRuntime`，供健康检查单元测试复用。
    async fn create_test_drt() -> DistributedRuntime {
        let rt = crate::Runtime::from_current().unwrap();
        let config = crate::distributed::DistributedConfig::process_local();

        DistributedRuntime::new(rt, config).await.unwrap()
    }

    /// 向 `SystemHealth` 注册一个带 payload 的健康检查目标，并返回该 payload。
    fn register_target(drt: &DistributedRuntime, portname: &str) -> serde_json::Value {
        let payload = serde_json::json!({"prompt": "health", "_health_check": true});
        drt.system_health().lock().register_health_check_target(
            portname,
            Instance {
                servicegroup: "servicegroup".to_string(),
                portname: portname.to_string(),
                namespace: "namespace".to_string(),
                instance_id: 1,
                transport: TransportType::Nats(portname.to_string()),
                device_type: None,
            },
            payload.clone(),
        );
        payload
    }

    #[test]
    /// 测试：默认健康检查配置会使用运行时常量中的等待时间和超时时间。
    fn test_health_check_config_default_uses_runtime_constants() {
        let config = HealthCheckConfig::default();

        assert_eq!(
            config.canary_wait_time,
            Duration::from_secs(crate::config::DEFAULT_CANARY_WAIT_TIME_SECS)
        );
        assert_eq!(
            config.request_timeout,
            Duration::from_secs(crate::config::DEFAULT_HEALTH_CHECK_REQUEST_TIMEOUT_SECS)
        );
    }

    #[tokio::test]
    /// 测试：使用默认配置启动健康检查管理器可以成功完成初始化。
    async fn test_start_health_check_manager_with_default_config_succeeds() {
        let drt = create_test_drt().await;

        start_health_check_manager(drt, None).await.unwrap();
    }

    #[tokio::test]
    /// 测试：当本地注册表里没有目标 engine 时，发送健康检查请求会返回错误。
    async fn test_send_health_check_request_returns_error_when_engine_missing() {
        let drt = create_test_drt().await;
        let portname = "test.health.missing";
        let payload = register_target(&drt, portname);

        let manager = HealthCheckManager::new(drt, HealthCheckConfig::default());
        let error = manager
            .send_health_check_request(portname, &payload)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("not found in local registry"));
    }

    #[tokio::test]
    /// 测试：新端点监听器的接收端只能被获取一次。
    async fn test_spawn_new_portname_monitor_can_only_take_receiver_once() {
        let drt = create_test_drt().await;
        let manager = Arc::new(HealthCheckManager::new(drt, HealthCheckConfig::default()));

        manager.spawn_new_portname_monitor().await.unwrap();
        assert!(manager.spawn_new_portname_monitor().await.is_err());
    }

    #[tokio::test]
    /// 测试：健康检查状态汇总结果会正确反映所有已注册端点的就绪状态。
    async fn test_get_health_check_status_summarizes_registered_portnames() {
        let drt = create_test_drt().await;
        register_target(&drt, "test.health.one");
        register_target(&drt, "test.health.two");
        drt.system_health()
            .lock()
            .set_portname_health_status("test.health.one", HealthStatus::Ready);
        drt.system_health()
            .lock()
            .set_portname_health_status("test.health.two", HealthStatus::NotReady);

        let status = get_health_check_status(&drt).await.unwrap();

        assert_eq!(status["status"], "notready");
        assert_eq!(status["portnames_checked"], 2);
        assert_eq!(status["portname_statuses"]["test.health.one"]["healthy"], true);
        assert_eq!(status["portname_statuses"]["test.health.two"]["healthy"], false);
    }

    // === SECTION: 合并自原 mod push_handler_notify_tests ===
    // 全链路测试：push_handler → notify → HealthCheckManager
    // 这些测试使用真实的 HealthCheckManager（spawn_portname_health_check_task）
    // 以及真实的 push_handler pipeline（TwoPartCodec + TCP + engine.generate()）。
    #[cfg(feature = "integration")]
    mod push_handler_notify_tests {
    use super::super::*;
    use crate::servicegroup::{Instance, TransportType};
    use crate::config::HealthStatus;
    use crate::distributed::distributed_test_utils::create_test_drt_async;
    use crate::engine::{AsyncEngine, AsyncEngineContextProvider};
    use crate::local_portname_registry::LocalAsyncEngine;
    use crate::pipeline::network::codec::{TwoPartCodec, TwoPartMessage};
    use crate::pipeline::network::tcp::server::{ServerOptions, TcpStreamServer};
    use crate::pipeline::network::{
        ConnectionInfo, Ingress, PushWorkHandler, ResponseService, StreamOptions,
    };
    use crate::pipeline::{ManyOut, ResponseStream, SingleIn};
    use crate::protocols::annotated::Annotated;
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream;
    use std::sync::Arc;
    use std::time::Duration;

    type TestRequest = serde_json::Value;
    type TestResponse = Annotated<serde_json::Value>;

    /// 可配置成功块和错误块分布的模拟流式引擎。
    /// 它既作为 push_handler pipeline 的引擎，也会注册到本地端点表里处理健康检查请求。
    struct MockStreamingEngine {
        num_chunks: usize,
        /// 指定哪些 chunk 索引会返回错误响应。
        error_indices: Vec<usize>,
    }

    impl MockStreamingEngine {
        /// 构造一个所有 chunk 都成功的模拟引擎。
        fn success(num_chunks: usize) -> Arc<Self> {
            Arc::new(Self {
                num_chunks,
                error_indices: vec![],
            })
        }

        /// 构造一个所有 chunk 都返回错误的模拟引擎。
        fn all_errors(num_chunks: usize) -> Arc<Self> {
            Arc::new(Self {
                num_chunks,
                error_indices: (0..num_chunks).collect(),
            })
        }

        /// 构造一个在指定索引返回错误、其余位置返回成功的模拟引擎。
        fn with_error_at(num_chunks: usize, error_indices: Vec<usize>) -> Arc<Self> {
            Arc::new(Self {
                num_chunks,
                error_indices,
            })
        }
    }

    #[async_trait]
    impl AsyncEngine<SingleIn<TestRequest>, ManyOut<TestResponse>, anyhow::Error>
        for MockStreamingEngine
    {
        /// 按预设 chunk 分布生成响应流，用于模拟成功和失败混合的流式输出。
        async fn generate(
            &self,
            input: SingleIn<TestRequest>,
        ) -> anyhow::Result<ManyOut<TestResponse>> {
            let (_data, ctx) = input.into_parts();
            let chunks: Vec<TestResponse> = (0..self.num_chunks)
                .map(|i| {
                    if self.error_indices.contains(&i) {
                        Annotated::from_error(format!("mock error at chunk {i}"))
                    } else {
                        Annotated::from_data(serde_json::json!({"token": i}))
                    }
                })
                .collect();
            Ok(ResponseStream::new(
                Box::pin(stream::iter(chunks)),
                ctx.context(),
            ))
        }
    }

    /// 使用给定连接信息把请求编码为 `TwoPartCodec` 二进制负载。
    fn encode_request(
        request_id: &str,
        connection_info: ConnectionInfo,
        request_body: &serde_json::Value,
    ) -> Bytes {
        let control = serde_json::json!({
            "id": request_id,
            "request_type": "single_in",
            "response_type": "many_out",
            "connection_info": connection_info,
        });
        let header = serde_json::to_vec(&control).unwrap();
        let data = serde_json::to_vec(request_body).unwrap();
        let msg = TwoPartMessage::new(Bytes::from(header), Bytes::from(data));
        TwoPartCodec::default().encode_message(msg).unwrap()
    }

    /// 搭建 TCP 接收端，并注册响应流供 push_handler 反向连接写回结果。
    async fn setup_tcp_receiver(request_id: &str) -> (Arc<TcpStreamServer>, ConnectionInfo) {
        let options = ServerOptions::builder().port(0).build().unwrap();
        let server = TcpStreamServer::new(options).await.unwrap();

        let context = crate::pipeline::Context::with_id((), request_id.to_string());
        let stream_options = StreamOptions::builder()
            .context(context.context())
            .enable_request_stream(false)
            .enable_response_stream(true)
            .build()
            .unwrap();

        let pending = server.register(stream_options).await;
        let connection_info = pending
            .recv_stream
            .as_ref()
            .unwrap()
            .connection_info
            .clone();

        (server, connection_info)
    }

    /// 在 DRT 中注册一个端点及其本地 engine。
    /// 返回真实 `HealthCheckManager` 会监听的 notifier。
    fn register_portname(
        drt: &crate::DistributedRuntime,
        portname_name: &str,
        local_engine: LocalAsyncEngine,
    ) -> Arc<tokio::sync::Notify> {
        let payload = serde_json::json!({
            "prompt": "health",
            "_health_check": true
        });
        drt.system_health().lock().register_health_check_target(
            portname_name,
            Instance {
                servicegroup: "test_servicegroup".to_string(),
                portname: portname_name.to_string(),
                namespace: "test_namespace".to_string(),
                instance_id: 0,
                transport: TransportType::Nats(portname_name.to_string()),
                device_type: None,
            },
            payload,
        );

        drt.local_portname_registry()
            .register(portname_name.to_string(), local_engine);

        drt.system_health()
            .lock()
            .get_portname_health_check_notifier(portname_name)
            .expect("Notifier should exist for registered portname")
    }

    /// 测试辅助函数：通过 ingress pipeline 发送一条请求。
    async fn send_request(ingress: &Ingress<SingleIn<TestRequest>, ManyOut<TestResponse>>) {
        let request_id = uuid::Uuid::new_v4().to_string();
        let (_server, connection_info) = setup_tcp_receiver(&request_id).await;
        let payload = encode_request(
            &request_id,
            connection_info,
            &serde_json::json!({"prompt": "test"}),
        );
        let result = ingress.handle_payload(payload, Some(request_id)).await;
        assert!(result.is_ok(), "handle_payload should succeed");
    }

    /// 测试辅助函数：断言指定端点当前的健康状态。
    fn assert_status(
        drt: &crate::DistributedRuntime,
        portname_name: &str,
        expected: HealthStatus,
        msg: &str,
    ) {
        let status = drt
            .system_health()
            .lock()
            .get_portname_health_status(portname_name);
        assert_eq!(status, Some(expected), "{msg}");
    }

    /// 测试辅助函数：使用指定引擎和 notifier 构造 ingress pipeline。
    fn create_ingress(
        engine: Arc<MockStreamingEngine>,
        notifier: Arc<tokio::sync::Notify>,
    ) -> Arc<Ingress<SingleIn<TestRequest>, ManyOut<TestResponse>>> {
        let ingress =
            Ingress::<SingleIn<TestRequest>, ManyOut<TestResponse>>::for_engine(engine).unwrap();
        ingress
            .set_portname_health_check_notifier(notifier)
            .unwrap();
        ingress
    }

    /// 测试辅助函数：用指定 canary 等待时间启动 `HealthCheckManager`。
    async fn start_manager(drt: &crate::DistributedRuntime, canary_wait_ms: u64) {
        let config = HealthCheckConfig {
            canary_wait_time: Duration::from_millis(canary_wait_ms),
            request_timeout: Duration::from_secs(1),
        };
        let manager = Arc::new(HealthCheckManager::new(drt.clone(), config));
        manager.start().await.unwrap();
    }

    // =================================================================
    // 测试 1：成功流式输出 → 触发通知 → 状态变为 Ready
    // Canary 引擎本身返回错误，因此 Ready 只能来自通知路径。
    // =================================================================
    #[tokio::test]
    /// 测试：成功流式输出会通过通知路径把端点状态置为 Ready。
    async fn test_successful_streaming_sets_ready() {
        let drt = create_test_drt_async().await;
        let portname = "test.successful_streaming";

        let notifier = register_portname(&drt, portname, MockStreamingEngine::all_errors(1));
        assert_status(&drt, portname, HealthStatus::NotReady, "initial");

        let ingress = create_ingress(MockStreamingEngine::success(5), notifier);
        start_manager(&drt, 500).await;

        send_request(&ingress).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Ready 只能来自通知路径，因为 canary 引擎始终返回错误。
        assert_status(
            &drt,
            portname,
            HealthStatus::Ready,
            "successful streaming should set Ready via notification path",
        );
    }

    // =================================================================
    // 测试 2：端点空闲 → 触发 canary → 健康检查成功 → 状态变为 Ready
    // =================================================================
    #[tokio::test]
    /// 测试：空闲端点会在 canary 触发后通过成功探测变为 Ready。
    async fn test_canary_fires_on_idle_engine() {
        let drt = create_test_drt_async().await;
        let portname = "test.canary_idle";

        let _notifier = register_portname(&drt, portname, MockStreamingEngine::success(1));
        assert_status(&drt, portname, HealthStatus::NotReady, "initial");

        start_manager(&drt, 50).await;
        tokio::time::sleep(Duration::from_millis(300)).await;

        // 未发送业务请求，Ready 完全来自 canary 探测成功。
        assert_status(
            &drt,
            portname,
            HealthStatus::Ready,
            "canary should fire and set Ready on idle engine",
        );
    }

    // =================================================================
    // 测试 3：流式输出全错误 → 无通知 → canary 也失败 → 保持 NotReady
    // =================================================================
    #[tokio::test]
    /// 测试：错误流式输出不会触发 Ready，且失败 canary 会让状态保持 NotReady。
    async fn test_error_streaming_stays_not_ready() {
        let drt = create_test_drt_async().await;
        let portname = "test.error_streaming";

        let notifier = register_portname(&drt, portname, MockStreamingEngine::all_errors(1));
        assert_status(&drt, portname, HealthStatus::NotReady, "initial");

        // Pipeline 全部输出错误，因此不会发出就绪通知。
        let ingress = create_ingress(MockStreamingEngine::all_errors(3), notifier);
        start_manager(&drt, 50).await;

        send_request(&ingress).await;
        // 等待 canary 触发并完成一次失败探测。
        tokio::time::sleep(Duration::from_millis(300)).await;

        // 错误流未通知，且 canary 引擎也返回错误，因此状态应保持 NotReady。
        assert_status(
            &drt,
            portname,
            HealthStatus::NotReady,
            "error streaming should not notify, canary also errors — stays NotReady",
        );
    }

    // =================================================================
    // 测试 4：端点空闲 → 触发 canary → 健康检查失败 → 保持 NotReady
    // =================================================================
    #[tokio::test]
    /// 测试：空闲端点在 canary 失败时不会被误标为 Ready。
    async fn test_idle_engine_with_failing_canary() {
        let drt = create_test_drt_async().await;
        let portname = "test.canary_fails";

        let _notifier = register_portname(&drt, portname, MockStreamingEngine::all_errors(1));
        assert_status(&drt, portname, HealthStatus::NotReady, "initial");

        start_manager(&drt, 50).await;
        tokio::time::sleep(Duration::from_millis(300)).await;

        // 未发送请求，状态完全由失败的 canary 结果决定。
        assert_status(
            &drt,
            portname,
            HealthStatus::NotReady,
            "canary fired but engine errored, status stays NotReady",
        );
    }

    // =================================================================
    // 测试 5：混合流（先成功后错误）→ 最终仍为 Ready
    // 成功 chunk 会先发出通知，因此即使尾部有错误，状态仍会变成 Ready。
    // Canary 引擎本身返回错误，用于证明 Ready 来自通知路径。
    // =================================================================
    #[tokio::test]
    /// 测试：只要前面已有成功 chunk 发出通知，尾部错误不会阻止状态变为 Ready。
    async fn test_mixed_streaming_sets_ready() {
        let drt = create_test_drt_async().await;
        let portname = "test.mixed_streaming";

        let notifier = register_portname(&drt, portname, MockStreamingEngine::all_errors(1));
        assert_status(&drt, portname, HealthStatus::NotReady, "initial");

        // 5 个 chunk：前 4 个成功，第 5 个返回错误。
        let ingress = create_ingress(MockStreamingEngine::with_error_at(5, vec![4]), notifier);
        start_manager(&drt, 500).await;

        send_request(&ingress).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        // 错误 chunk 之前已经有成功通知，因此最终应保持 Ready。
        assert_status(
            &drt,
            portname,
            HealthStatus::Ready,
            "successful chunks should set Ready despite trailing error",
        );
    }
}

    // === SECTION: 合并自原 mod integration_tests ===
    // 集成测试（需要真实 DRT）
    #[cfg(feature = "integration")]
    mod integration_tests {
    use super::super::*;
    use crate::distributed::distributed_test_utils::create_test_drt_async;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    /// 测试：管理器初始化后会正确保存配置参数。
    async fn test_initialization() {
        let drt = create_test_drt_async().await;

        let canary_wait_time = Duration::from_secs(5);
        let request_timeout = Duration::from_secs(3);

        let config = HealthCheckConfig {
            canary_wait_time,
            request_timeout,
        };

        let manager = HealthCheckManager::new(drt.clone(), config);

        assert_eq!(manager.config.canary_wait_time, canary_wait_time);
        assert_eq!(manager.config.request_timeout, request_timeout);
    }

    #[tokio::test]
    /// 测试：健康检查 payload 注册后可被查询，并出现在端点列表中。
    async fn test_payload_registration() {
        let drt = create_test_drt_async().await;

        let portname = "test.portname";
        let payload = serde_json::json!({
            "prompt": "test",
            "_health_check": true
        });

        drt.system_health().lock().register_health_check_target(
            portname,
            crate::servicegroup::Instance {
                servicegroup: "test_servicegroup".to_string(),
                portname: "test_portname".to_string(),
                namespace: "test_namespace".to_string(),
                instance_id: 12345,
                transport: crate::servicegroup::TransportType::Nats(portname.to_string()),
                device_type: None,
            },
            payload.clone(),
        );

        let retrieved = drt
            .system_health()
            .lock()
            .get_health_check_target(portname)
            .map(|t| t.payload);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), payload);

        // 确认端点已出现在健康检查端点列表中。
        let portnames = drt.system_health().lock().get_health_check_portnames();
        assert!(portnames.contains(&portname.to_string()));
    }

    #[tokio::test]
    /// 测试：启动管理器后，会为每个已注册端点生成独立健康检查任务。
    async fn test_spawn_per_portname_tasks() {
        let drt = create_test_drt_async().await;

        for i in 0..3 {
            let portname = format!("test.portname.{}", i);
            let payload = serde_json::json!({
                "prompt": format!("test{}", i),
                "_health_check": true
            });
            drt.system_health().lock().register_health_check_target(
                &portname,
                crate::servicegroup::Instance {
                    servicegroup: "test_servicegroup".to_string(),
                    portname: format!("test_portname_{}", i),
                    namespace: "test_namespace".to_string(),
                    instance_id: i,
                    transport: crate::servicegroup::TransportType::Nats(portname.clone()),
                    device_type: None,
                },
                payload,
            );
        }

        let config = HealthCheckConfig {
            canary_wait_time: Duration::from_secs(5),
            request_timeout: Duration::from_secs(1),
        };

        let manager = Arc::new(HealthCheckManager::new(drt.clone(), config));
        manager.clone().start().await.unwrap();

        // 确认所有端点都拥有各自独立的健康检查任务。
        let tasks = manager.portname_tasks.lock();
        // 预期有 3 个任务，对应 3 个端点。
        assert_eq!(tasks.len(), 3);
        // 确认所有端点都出现在任务映射中。
        let portnames: Vec<String> = tasks.keys().cloned().collect();
        assert!(portnames.contains(&"test.portname.0".to_string()));
        assert!(portnames.contains(&"test.portname.1".to_string()));
        assert!(portnames.contains(&"test.portname.2".to_string()));
    }

    #[tokio::test]
    /// 测试：注册健康检查目标时会同步创建端点级 notifier。
    async fn test_portname_health_check_notifier_created() {
        let drt = create_test_drt_async().await;

        let portname = "test.portname.notifier";
        let payload = serde_json::json!({
            "prompt": "test",
            "_health_check": true
        });

        // 先注册带健康检查 payload 的端点。
        drt.system_health().lock().register_health_check_target(
            portname,
            crate::servicegroup::Instance {
                servicegroup: "test_servicegroup".to_string(),
                portname: "test_portname_notifier".to_string(),
                namespace: "test_namespace".to_string(),
                instance_id: 999,
                transport: crate::servicegroup::TransportType::Nats(portname.to_string()),
                device_type: None,
            },
            payload.clone(),
        );

        // 确认该端点对应的 notifier 已经被创建。
        let notifier = drt
            .system_health()
            .lock()
            .get_portname_health_check_notifier(portname);

        assert!(
            notifier.is_some(),
            "PortName should have a notifier created"
        );

        // 验证可以在不触发 panic 的情况下通知它。
        if let Some(notifier) = notifier {
            notifier.notify_one();
        }

        // 初始状态下，端点名称应为 Ready（注册后的默认状态）。
        let status = drt
            .system_health()
            .lock()
            .get_portname_health_status(portname);
        assert_eq!(status, Some(HealthStatus::NotReady));
    }
}
}
