// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use pagoda_runtime::config::environment_names::runtime::system as env_system;
use serde_json::Value;
use temp_env::async_with_vars;

mod common;
use common::contract::{
    acquire_contract_test_lock, process_local_runtime, require_system_status_server,
    serve_streaming_endpoint, shutdown_runtime, system_status_http_get, system_status_server_env,
    unique_name,
};

// 目的/场景：DRT 启动链在配置 system port 后暴露 `/live` 进程存活探测。
//
// 生产逻辑：`DistributedRuntime::new` 读取 `RuntimeConfig::from_settings`，
// `system_server_enabled()` 时 `spawn_system_status_server` 绑定 OS 端口（`system_status_server.rs`）。
//
// 测试计划：`PGD_SYSTEM_PORT=0` + `starting_health_status=ready` → GET `/live`。
//
// 关键断言：HTTP 200；body 含 `"status":"ready"`。
#[tokio::test]
async fn live_endpoint_reports_process_liveness() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    async_with_vars(
        [
            system_status_server_env()[0],
            system_status_server_env()[1],
            (env_system::PGD_SYSTEM_STARTING_HEALTH_STATUS, Some("ready")),
        ],
        async {
            let (rt, drt) = process_local_runtime().await?;
            let info = require_system_status_server(&drt)?;
            let (status, body) = system_status_http_get(info.socket_addr, "/live").await?;
            assert_eq!(status, 200, "body={body}");
            assert!(
                body.contains("\"status\":\"ready\""),
                "expected ready liveness payload, got: {body}"
            );
            shutdown_runtime(rt, None).await?;
            Ok::<(), anyhow::Error>(())
        },
    )
    .await
}

// 目的/场景：`/health` 反映 system/endpoint 聚合健康，随 endpoint 注册变化。
//
// 生产逻辑：`health_handler` 调用 `SystemHealth::get_health_status`（`system_status_server.rs`）；
// `PGD_SYSTEM_USE_ENDPOINT_HEALTH_STATUS` 时未注册 endpoint 为 notready。
//
// 测试计划：初始 GET `/health` 为 503 → serve endpoint → 再 GET 为 200 ready。
//
// 关键断言：503 + notready；注册后 200 + ready 且 endpoints map 含 `generate`。
#[tokio::test]
async fn health_endpoint_reports_aggregated_health() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    const ENDPOINT_NAME: &str = "generate";
    async_with_vars(
        [
            system_status_server_env()[0],
            system_status_server_env()[1],
            (env_system::PGD_SYSTEM_STARTING_HEALTH_STATUS, Some("notready")),
            (
                env_system::PGD_SYSTEM_USE_ENDPOINT_HEALTH_STATUS,
                Some("[\"generate\"]"),
            ),
        ],
        async {
            let (rt, drt) = process_local_runtime().await?;
            let info = require_system_status_server(&drt)?;
            let addr = info.socket_addr;

            let (status_before, body_before) = system_status_http_get(addr, "/health").await?;
            assert_eq!(status_before, 503, "body={body_before}");
            assert!(body_before.contains("\"status\":\"notready\""));

            let endpoint = drt
                .namespace(unique_name("sys-status-health"))?
                .servicegroup("backend")?
                .portname(ENDPOINT_NAME);
            let (_client, endpoint_task) = serve_streaming_endpoint(endpoint).await?;

            let (status_after, body_after) = system_status_http_get(addr, "/health").await?;
            assert_eq!(status_after, 200, "body={body_after}");
            assert!(body_after.contains("\"status\":\"ready\""));

            let parsed: Value = serde_json::from_str(&body_after)?;
            let portnames = parsed
                .get("portnames")
                .and_then(Value::as_object)
                .ok_or_else(|| anyhow::anyhow!("health body missing portnames: {body_after}"))?;
            assert_eq!(
                portnames.get(ENDPOINT_NAME).and_then(Value::as_str),
                Some("ready")
            );

            shutdown_runtime(rt, Some(endpoint_task)).await?;
            Ok::<(), anyhow::Error>(())
        },
    )
    .await
}

// 目的/场景：system status HTTP 的 health JSON 列出已注册 endpoint 状态（无独立 `/status` 路由）。
//
// 生产逻辑：`health_handler` 的 `endpoints` 字段来自 `SystemHealth` 跟踪的 endpoint 注册
//（`push_endpoint.rs` / request plane 回调）。
//
// 测试计划：serve 流式 endpoint → GET `/health` → 解析 `endpoints` map。
//
// 关键断言：`endpoints.generate == "ready"`。
#[tokio::test]
async fn status_endpoint_includes_registered_endpoints() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    const ENDPOINT_NAME: &str = "generate";
    async_with_vars(
        [
            system_status_server_env()[0],
            system_status_server_env()[1],
            (env_system::PGD_SYSTEM_STARTING_HEALTH_STATUS, Some("ready")),
        ],
        async {
            let (rt, drt) = process_local_runtime().await?;
            let info = require_system_status_server(&drt)?;

            let endpoint = drt
                .namespace(unique_name("sys-status-endpoints"))?
                .servicegroup("backend")?
                .portname(ENDPOINT_NAME);
            let (_client, endpoint_task) = serve_streaming_endpoint(endpoint).await?;

            let (status, body) =
                system_status_http_get(info.socket_addr, "/health").await?;
            assert_eq!(status, 200, "body={body}");

            let parsed: Value = serde_json::from_str(&body)?;
            let portnames = parsed
                .get("portnames")
                .and_then(Value::as_object)
                .ok_or_else(|| anyhow::anyhow!("health body missing portnames: {body}"))?;
            assert_eq!(
                portnames.get(ENDPOINT_NAME).and_then(Value::as_str),
                Some("ready"),
                "registered endpoint should appear in health status JSON"
            );

            shutdown_runtime(rt, Some(endpoint_task)).await?;
            Ok::<(), anyhow::Error>(())
        },
    )
    .await
}

// 目的/场景：自定义 health/live 路径生效，默认路径返回 404。
//
// 生产逻辑：`SystemHealth` 保存 `system_health_path` / `system_live_path`（`config.rs` env）；
// router 仅注册配置路径（`spawn_system_status_server`）。
//
// 测试计划：设置 `DYN_SYSTEM_*_PATH` 自定义路径 → 默认 `/health`/`/live` 404，自定义 200。
//
// 关键断言：默认路径 404；`/custom/health` 与 `/custom/live` 200 + ready。
#[tokio::test]
async fn custom_health_and_live_paths_are_honored() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    const CUSTOM_HEALTH: &str = "/custom/health";
    const CUSTOM_LIVE: &str = "/custom/live";
    async_with_vars(
        [
            system_status_server_env()[0],
            system_status_server_env()[1],
            (env_system::PGD_SYSTEM_STARTING_HEALTH_STATUS, Some("ready")),
            (env_system::PGD_SYSTEM_HEALTH_PATH, Some(CUSTOM_HEALTH)),
            (env_system::PGD_SYSTEM_LIVE_PATH, Some(CUSTOM_LIVE)),
        ],
        async {
            let (rt, drt) = process_local_runtime().await?;
            let info = require_system_status_server(&drt)?;
            let addr = info.socket_addr;

            for default_path in ["/health", "/live"] {
                let (status, body) = system_status_http_get(addr, default_path).await?;
                assert_eq!(status, 404, "path={default_path} body={body}");
                assert!(body.contains("Route not found"), "body={body}");
            }

            for custom_path in [CUSTOM_HEALTH, CUSTOM_LIVE] {
                let (status, body) = system_status_http_get(addr, custom_path).await?;
                assert_eq!(status, 200, "path={custom_path} body={body}");
                assert!(
                    body.contains("\"status\":\"ready\""),
                    "path={custom_path} body={body}"
                );
            }

            shutdown_runtime(rt, None).await?;
            Ok::<(), anyhow::Error>(())
        },
    )
    .await
}

mod engine {
    use std::sync::Arc;

    use anyhow::{Result, anyhow};
    use pagoda_runtime::engine_routes::EngineRouteCallback;
    use serde_json::{Value, json};
    use temp_env::async_with_vars;

    use super::common::contract::{
        acquire_contract_test_lock, process_local_runtime, require_system_status_server,
        shutdown_runtime, system_status_http_get, system_status_http_post,
        system_status_server_env, unique_name,
    };

    // 目的/场景：`engine_routes().register` 后 HTTP GET/POST `/engine/{path}` JSON 往返。
    //
    // 生产逻辑：`engine_route_handler` 查 `EngineRouteRegistry`、解析 body、调用 callback
    // 返回 JSON（`system_status_server.rs` / `engine_routes.rs`）。
    //
    // 测试计划：注册 echo route → POST 带 payload → GET 空 body。
    //
    // 关键断言：POST 200 且 echo 字段匹配；GET 200 且 `empty: true`。
    #[tokio::test]
    async fn engine_route_register_and_json_roundtrip() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let route = unique_name("echo");
        async_with_vars(system_status_server_env(), async {
            let (rt, drt) = process_local_runtime().await?;
            let info = require_system_status_server(&drt)?;
            let addr = info.socket_addr;

            let callback: EngineRouteCallback = Arc::new(|body| {
                Box::pin(async move {
                    if body.as_object().is_some_and(|o| o.is_empty()) {
                        Ok(json!({"empty": true}))
                    } else {
                        Ok(json!({"echo": body}))
                    }
                })
            });
            drt.engine_routes().register(&route, callback);

            let request = json!({"input": "contract-test"});
            let (post_status, post_body) =
                system_status_http_post(addr, &format!("/engine/{route}"), &request).await?;
            assert_eq!(post_status, 200, "body={post_body}");
            let post_json: Value = serde_json::from_str(&post_body)?;
            assert_eq!(post_json.get("echo"), Some(&request));

            let (get_status, get_body) =
                system_status_http_get(addr, &format!("/engine/{route}")).await?;
            assert_eq!(get_status, 200, "body={get_body}");
            let get_json: Value = serde_json::from_str(&get_body)?;
            assert_eq!(get_json.get("empty"), Some(&json!(true)));

            shutdown_runtime(rt, None).await?;
            Ok::<(), anyhow::Error>(())
        })
        .await
    }

    // 目的/场景：未注册的 `/engine/*` 路径返回 404 JSON。
    //
    // 生产逻辑：`engine_route_handler` 在 registry miss 时返回 NOT_FOUND + error JSON。
    //
    // 测试计划：GET 不存在的 engine path。
    //
    // 关键断言：HTTP 404；body 含 `Route not found`。
    #[tokio::test]
    async fn engine_route_unknown_path_returns_404() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let missing = unique_name("missing-route");
        async_with_vars(system_status_server_env(), async {
            let (rt, drt) = process_local_runtime().await?;
            let info = require_system_status_server(&drt)?;
            let addr = info.socket_addr;

            let (status, body) =
                system_status_http_get(addr, &format!("/engine/{missing}")).await?;
            assert_eq!(status, 404, "body={body}");
            assert!(
                body.contains("Route not found"),
                "expected route-not-found payload, got: {body}"
            );

            shutdown_runtime(rt, None).await?;
            Ok::<(), anyhow::Error>(())
        })
        .await
    }

    // 目的/场景：engine route callback 返回 Err 时 HTTP 500 且错误信息透传。
    //
    // 生产逻辑：`engine_route_handler` 将 callback Err 映射为 500 + Handler error JSON。
    //
    // 测试计划：注册必然失败的 callback → POST。
    //
    // 关键断言：HTTP 500；body 含 `Handler error` 与 callback 错误文本。
    #[tokio::test]
    async fn engine_route_handler_error_propagates() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let route = unique_name("fail");
        const ERR_MSG: &str = "engine callback failed intentionally";
        async_with_vars(system_status_server_env(), async {
            let (rt, drt) = process_local_runtime().await?;
            let info = require_system_status_server(&drt)?;
            let addr = info.socket_addr;

            let callback: EngineRouteCallback =
                Arc::new(|_| Box::pin(async { Err(anyhow!(ERR_MSG)) }));
            drt.engine_routes().register(&route, callback);

            let (status, body) = system_status_http_post(
                addr,
                &format!("/engine/{route}"),
                &json!({"probe": true}),
            )
            .await?;
            assert_eq!(status, 500, "body={body}");
            assert!(
                body.contains("Handler error"),
                "expected handler error envelope, got: {body}"
            );
            assert!(
                body.contains(ERR_MSG),
                "expected callback error text in body, got: {body}"
            );

            shutdown_runtime(rt, None).await?;
            Ok::<(), anyhow::Error>(())
        })
        .await
    }
}

mod local {
    use std::sync::Arc;

    use anyhow::Result;
    use async_trait::async_trait;
    use pagoda_runtime::{
        engine::{AsyncEngine, AsyncEngineContextProvider, ResponseStream},
        engine_routes::EngineRouteCallback,
        local_portname_registry::LocalAsyncEngine,
        pipeline::{ManyOut, SingleIn},
        protocols::annotated::Annotated,
    };
    use futures::stream;
    use serde_json::json;
    use temp_env::async_with_vars;

    use super::common::contract::{
        acquire_contract_test_lock, process_local_runtime, shutdown_runtime,
        system_status_server_env, unique_name,
    };

    struct EchoLocalEngine {
        tag: String,
    }

    #[async_trait]
    impl AsyncEngine<SingleIn<serde_json::Value>, ManyOut<Annotated<serde_json::Value>>, anyhow::Error>
        for EchoLocalEngine
    {
        async fn generate(
            &self,
            request: SingleIn<serde_json::Value>,
        ) -> Result<ManyOut<Annotated<serde_json::Value>>, anyhow::Error> {
            let (_payload, ctx) = request.into_parts();
            Ok(ResponseStream::new(
                Box::pin(stream::iter(vec![Annotated::from_data(
                    json!({"engine": self.tag}),
                )])),
                ctx.context(),
            ))
        }
    }

    // 目的/场景：`LocalEndpointRegistry` register/get 语义与 `EngineRouteRegistry::routes` 列表。
    //
    // 生产逻辑：`local_portname_registry::register` 按 endpoint 名索引 engine；
    // `engine_routes::routes` 列出已注册 engine HTTP 路径（`distributed.rs`）。
    //
    // 测试计划：注册两个 local engine + 两个 engine route → 断言 get/routes。
    //
    // 关键断言：已注册名 get 为 Some；未知名为 None；routes 含两条路径。
    #[tokio::test]
    async fn local_portname_registry_lists_registered_engines() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let name_a = unique_name("local-a");
        let name_b = unique_name("local-b");
        let route_a = unique_name("route-a");
        let route_b = unique_name("route-b");
        async_with_vars(system_status_server_env(), async {
            let (rt, drt) = process_local_runtime().await?;

            let registry = drt.local_portname_registry();
            registry.register(
                name_a.clone(),
                Arc::new(EchoLocalEngine {
                    tag: name_a.clone(),
                }) as LocalAsyncEngine,
            );
            registry.register(
                name_b.clone(),
                Arc::new(EchoLocalEngine {
                    tag: name_b.clone(),
                }) as LocalAsyncEngine,
            );

            assert!(registry.get(&name_a).is_some());
            assert!(registry.get(&name_b).is_some());
            assert!(registry.get("definitely-missing-endpoint").is_none());

            let ok_callback: EngineRouteCallback =
                Arc::new(|_| Box::pin(async { Ok(json!({"ok": true})) }));
            drt.engine_routes().register(&route_a, ok_callback.clone());
            drt.engine_routes().register(&route_b, ok_callback);
            let mut routes = drt.engine_routes().routes();
            routes.sort();
            let mut expected = vec![route_a, route_b];
            expected.sort();
            assert_eq!(routes, expected);

            shutdown_runtime(rt, None).await?;
            Ok::<(), anyhow::Error>(())
        })
        .await
    }
}
