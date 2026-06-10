// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use pagoda_runtime::{
    config::{
        HealthStatus,
        environment_names::runtime::{canary as env_canary, system as env_system},
    },
    engine::{AsyncEngine, AsyncEngineContextProvider, ResponseStream},
    local_portname_registry::LocalAsyncEngine,
    metrics::{MetricsHierarchy, prometheus_names::{
        build_servicegroup_metric_name, name_prefix, request_plane, sanitize_prometheus_label,
        sanitize_prometheus_name, transport,
    }},
    pipeline::{ManyOut, SingleIn, network::Ingress},
    protocols::annotated::Annotated,
};
use futures::stream;
use serde_json::json;
use temp_env::async_with_vars;

mod common;
use common::contract::{
    acquire_contract_test_lock, ensure_integration_tcp_request_plane, make_streaming_engine,
    process_local_runtime, shutdown_runtime, unique_name,
};

struct LocalHealthEngine;

#[async_trait]
impl AsyncEngine<SingleIn<serde_json::Value>, ManyOut<Annotated<serde_json::Value>>, anyhow::Error>
    for LocalHealthEngine
{
    async fn generate(
        &self,
        request: SingleIn<serde_json::Value>,
    ) -> Result<ManyOut<Annotated<serde_json::Value>>, anyhow::Error> {
        let (_payload, ctx) = request.into_parts();
        Ok(ResponseStream::new(
            Box::pin(stream::iter(vec![Annotated::from_data(json!({"ok": true}))])),
            ctx.context(),
        ))
    }
}

// 目的/场景：endpoint 注册 health check target 时创建 notifier 并通知 HealthCheckManager。
//
// 生产逻辑：`Endpoint::start` → `register_health_check_target` 写入 target/notifier 并经
// `new_endpoint_tx` 通知已启动的 `HealthCheckManager`（`system_health.rs` / `health_check.rs`）。
//
// 测试计划：`DYN_HEALTH_CHECK_ENABLED` → 启动 DRT（manager 已运行）→ serve endpoint +
// `health_check_payload` → 断言 target/notifier/endpoint 列表。
//
// 关键断言：注册后 `get_health_check_target` 与 `get_portname_health_check_notifier` 均存在。
#[tokio::test]
async fn health_check_target_registration_notifies_manager() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    async_with_vars(
        [
            ("PGD_HEALTH_CHECK_ENABLED", Some("true")),
            (env_canary::PGD_CANARY_WAIT_TIME, Some("60")),
        ],
        async {
            ensure_integration_tcp_request_plane().await?;
            let (rt, drt) = process_local_runtime().await?;
            let endpoint_name = "generate";
            let endpoint = drt
                .namespace(unique_name("phase1-hc-notify"))?
                .servicegroup("backend")?
                .portname(endpoint_name);
            let ingress = Ingress::for_engine(make_streaming_engine())?;
            let local_engine: LocalAsyncEngine = Arc::new(LocalHealthEngine);
            let endpoint_task = rt.primary().spawn(
                endpoint
                    .portname_builder()
                    .handler(ingress)
                    .health_check_payload(json!("ping"))
                    .register_local_engine(local_engine)?
                    .start(),
            );

            let client = endpoint.client().await?;
            client.wait_for_instances().await?;

            let system_health = drt.system_health();
            let guard = system_health.lock();
            assert!(
                guard
                    .get_health_check_portnames()
                    .contains(&endpoint_name.to_string()),
                "registered endpoint should appear in health check endpoint list"
            );
            assert!(
                guard.get_health_check_target(endpoint_name).is_some(),
                "health check target should be registered for endpoint"
            );
            assert!(
                guard
                    .get_portname_health_check_notifier(endpoint_name)
                    .is_some(),
                "notifier should exist for registered health check target"
            );
            drop(guard);

            tokio::time::sleep(Duration::from_millis(200)).await;

            shutdown_runtime(rt, Some(endpoint_task)).await?;
            Ok::<(), anyhow::Error>(())
        },
    )
    .await
}

// 目的/场景：endpoint 启动后 system health 记录 endpoint 注册（Phase 1 health 面）。
//
// 生产逻辑：`Endpoint::start` → request plane `set_endpoint_registered`（`shared_tcp_endpoint.rs`）；
// shutdown 时设为 NotReady（`push_endpoint.rs`）。
//
// 测试计划：带 handler 的 endpoint 启动；检查 `SystemHealth` 中 endpoint 状态为 Ready。
//
// 关键断言：`get_portname_health_status("generate") == Ready`。
#[tokio::test]
async fn endpoint_health_tracks_registration() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    let (rt, drt) = process_local_runtime().await?;
    let endpoint_name = "generate";
    let endpoint = drt
        .namespace(unique_name("phase1-health"))?
        .servicegroup("backend")?
        .portname(endpoint_name);
    let ingress = Ingress::for_engine(make_streaming_engine())?;
    let endpoint_task = rt.primary().spawn(
        endpoint
            .portname_builder()
            .handler(ingress)
            .start(),
    );

    let client = endpoint.client().await?;
    client.wait_for_instances().await?;

    let system_health = drt.system_health();
    let health = system_health.lock();
    assert_eq!(
        health.get_portname_health_status(endpoint_name),
        Some(HealthStatus::Ready)
    );

    shutdown_runtime(rt, Some(endpoint_task)).await?;
    Ok(())
}

// 目的/场景：canary 空闲探测成功后 endpoint health 变为 Ready。
//
// 生产逻辑：`HealthCheckManager` 经 local registry 发 canary；成功响应后 `set_endpoint_health_status(Ready)`。
//
// 测试计划：`DYN_HEALTH_CHECK_ENABLED` + canary wait → register local engine → 轮询 health 直至 Ready。
//
// 关键断言：初始 NotReady；5s 内变为 Ready。
#[tokio::test]
async fn endpoint_health_tracks_canary_result() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    async_with_vars(
        [
            ("PGD_HEALTH_CHECK_ENABLED", Some("true")),
            (env_canary::PGD_CANARY_WAIT_TIME, Some("1")),
        ],
        async {
            ensure_integration_tcp_request_plane().await?;
            let (rt, drt) = process_local_runtime().await?;
            let endpoint_name = "generate";
            let endpoint = drt
                .namespace(unique_name("phase1-canary"))?
                .servicegroup("backend")?
                .portname(endpoint_name);
            let ingress = Ingress::for_engine(make_streaming_engine())?;
            let local_engine: LocalAsyncEngine = Arc::new(LocalHealthEngine);
            let endpoint_task = rt.primary().spawn(
                endpoint
                    .portname_builder()
                    .handler(ingress)
                    .health_check_payload(json!("ping"))
                    .register_local_engine(local_engine)?
                    .start(),
            );

            let client = endpoint.client().await?;
            client.wait_for_instances().await?;

            let system_health = drt.system_health();
            assert_eq!(
                system_health
                    .lock()
                    .get_portname_health_status(endpoint_name),
                Some(HealthStatus::NotReady),
                "canary-enabled endpoints start NotReady until verified"
            );

            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if system_health
                        .lock()
                        .get_portname_health_status(endpoint_name)
                        == Some(HealthStatus::Ready)
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await?;

            shutdown_runtime(rt, Some(endpoint_task)).await?;
            Ok::<(), anyhow::Error>(())
        },
    )
    .await
}

// 目的/场景：配置 `use_endpoint_health_status` 后，系统健康由指定 endpoint 聚合。
//
// 生产逻辑：`SystemHealth::get_health_status` 在 `use_endpoint_health_status` 非空时按 endpoint 列表聚合。
//
// 测试计划：配置 `[\"generate\"]` → 注册前查 system health → serve endpoint → 再查聚合状态。
//
// 关键断言：注册前 `healthy==false`；注册后 `healthy==true` 且 endpoints 含 `generate: ready`。
#[tokio::test]
async fn system_health_uses_endpoint_status_when_configured() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    const ENDPOINT_NAME: &str = "generate";
    async_with_vars(
        [
            (
                env_system::PGD_SYSTEM_USE_ENDPOINT_HEALTH_STATUS,
                Some("[\"generate\"]"),
            ),
            (env_system::PGD_SYSTEM_STARTING_HEALTH_STATUS, Some("notready")),
        ],
        async {
            let (rt, drt) = process_local_runtime().await?;
            let (healthy_before, _) = drt.system_health().lock().get_health_status();
            assert!(
                !healthy_before,
                "configured endpoint should start notready before registration"
            );

            let endpoint = drt
                .namespace(unique_name("phase1-sys-health"))?
                .servicegroup("backend")?
                .portname(ENDPOINT_NAME);
            let ingress = Ingress::for_engine(make_streaming_engine())?;
            let endpoint_task = rt.primary().spawn(
                endpoint
                    .portname_builder()
                    .handler(ingress)
                    .start(),
            );

            let client = endpoint.client().await?;
            client.wait_for_instances().await?;

            let (healthy_after, endpoints) = drt.system_health().lock().get_health_status();
            assert!(healthy_after, "system health should follow configured endpoint Ready");
            assert_eq!(
                endpoints.get(ENDPOINT_NAME),
                Some(&"ready".to_string())
            );

            shutdown_runtime(rt, Some(endpoint_task)).await?;
            Ok::<(), anyhow::Error>(())
        },
    )
    .await
}

// 目的/场景：metrics 导出包含 endpoint 层级身份标签（Phase 1 metrics 面）。
//
// 生产逻辑：`Ingress::add_metrics` → `WorkHandlerMetrics::from_endpoint` 写入
// namespace/component/endpoint 等 label；`prometheus_expfmt_combined` 可抓取。
//
// 测试计划：启动 endpoint 后 scrape；断言文本含 component 与 endpoint 名。
//
// 关键断言：`prometheus_expfmt_combined` 含 component、endpoint 及 runtime 指标族前缀。
#[tokio::test]
async fn metrics_labels_include_runtime_identity() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    let (rt, drt) = process_local_runtime().await?;
    // Prometheus label values sanitize `-` to `_` (see work-handler metrics).
    let component = "metrics_backend";
    let endpoint_name = "generate";
    let endpoint = drt
        .namespace(unique_name("phase1-metrics"))?
        .servicegroup(component)?
        .portname(endpoint_name);
    let ingress = Ingress::for_engine(make_streaming_engine())?;
    let endpoint_task = rt.primary().spawn(
        endpoint
            .portname_builder()
            .handler(ingress)
            .health_check_payload(json!("ping"))
            .start(),
    );

    let client = endpoint.client().await?;
    client.wait_for_instances().await?;

    let metrics = drt.get_metrics_registry().prometheus_expfmt_combined()?;
    assert!(
        metrics.contains(component),
        "metrics should include component label/value, got: {metrics}"
    );
    assert!(
        metrics.contains(endpoint_name),
        "metrics should include endpoint label/value, got: {metrics}"
    );
    assert!(
        metrics.contains("pagoda") || metrics.contains("runtime"),
        "metrics scrape should contain runtime families"
    );

    shutdown_runtime(rt, Some(endpoint_task)).await?;
    Ok(())
}

// 目的/场景：runtime 导出的 Prometheus 指标名与 label 名符合命名规范。
//
// 生产逻辑：`metrics/prometheus_names.rs` 的 sanitize/build 规则；scrape 输出不应含非法 token。
//
// 测试计划：启动 endpoint → 校验已知 metric 名 → 逐行 scrape 校验 name 与 label。
//
// 关键断言：内置 metric 名 sanitize 通过；scrape 每行 name/label 均 prometheus-safe。
#[tokio::test]
async fn metrics_names_are_prometheus_safe() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    let (rt, drt) = process_local_runtime().await?;
    let endpoint = drt
        .namespace(unique_name("phase1-metric-names"))?
        .servicegroup("metrics_backend")?
        .portname("generate");
    let ingress = Ingress::for_engine(make_streaming_engine())?;
    let endpoint_task = rt.primary().spawn(
        endpoint
            .portname_builder()
            .handler(ingress)
            .start(),
    );

    let client = endpoint.client().await?;
    client.wait_for_instances().await?;

    for name in [
        build_servicegroup_metric_name("requests_total"),
        format!(
            "{}_{}",
            name_prefix::TRANSPORT,
            transport::tcp::BYTES_SENT_TOTAL
        ),
        format!(
            "{}_{}",
            name_prefix::REQUEST_PLANE,
            request_plane::INFLIGHT_REQUESTS
        ),
    ] {
        assert!(
            sanitize_prometheus_name(&name).is_ok(),
            "known metric name should be valid: {name}"
        );
    }

    let metrics = drt.get_metrics_registry().prometheus_expfmt_combined()?;
    for line in metrics.lines() {
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        let name = line
            .split(['{', ' '])
            .next()
            .unwrap_or(line)
            .trim();
        if name.is_empty() {
            continue;
        }
        assert!(
            sanitize_prometheus_name(name).is_ok(),
            "scraped metric name should be prometheus-safe: {name}"
        );
        if let Some(label_section) = line.split('{').nth(1) {
            let labels = label_section.trim_end_matches('}');
            for pair in labels.split(',') {
                let label_name = pair.split('=').next().unwrap_or("").trim();
                if !label_name.is_empty() {
                    assert!(
                        sanitize_prometheus_label(label_name).is_ok(),
                        "scraped label name should be prometheus-safe: {label_name}"
                    );
                }
            }
        }
    }

    shutdown_runtime(rt, Some(endpoint_task)).await?;
    Ok(())
}
