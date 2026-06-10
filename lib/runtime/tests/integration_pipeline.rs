// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

mod common;

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, anyhow};
use dynamo_runtime::{
    engine::{AsyncEngine, AsyncEngineContextProvider},
    pipeline::{ManyOut, ServiceBackend, ServiceFrontend, SingleIn, Source},
};
use futures::StreamExt;

use common::contract::{TestResponse, acquire_contract_test_lock};
use common::contract_engines::{
    make_cancellable_pipeline_service, make_error_service_engine, make_pipeline_contract_service,
};

// 目的/场景：进程内 pipeline link 顺序契约（frontend → node → operator → backend → post）。
//
// 生产逻辑：`ServiceFrontend::link` 与 `make_pipeline_contract_service` 中各 stage 的 map 顺序。
//
// 测试计划：单次 `generate`；断言最终 data 字符串。
//
// 关键断言：单 chunk data 为 `input-node-pre-post-op`。
#[tokio::test]
async fn pipeline_frontend_operator_backend_postprocessor_roundtrip() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    let service = make_pipeline_contract_service()?;
    let mut response = service.generate("input".to_string().into()).await?;
    let item = response.next().await.expect("pipeline should emit one item");

    assert_eq!(item.data.as_deref(), Some("input-node-pre-post-op"));
    assert!(response.next().await.is_none());
    Ok(())
}

// 目的/场景：`SingleIn::with_id` 的 request id 必须贯穿节点链并在响应流 context 上可读。
//
// 生产逻辑：各 `link` stage 通过 `ResponseStream::new(..., ctx)` 传递同一 `AsyncEngineContext`。
//
// 测试计划：带 id 请求完整 pipeline；读取 `response.context().id()`。
//
// 关键断言：`response.context().id()` 等于 `ctx-survives`。
#[tokio::test]
async fn pipeline_stream_context_survives_node_chain() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    let service = make_pipeline_contract_service()?;
    let request = SingleIn::with_id("input".to_string(), "ctx-survives".to_string());
    let mut response = service.generate(request).await?;
    let _ = response.next().await.expect("one item");
    assert_eq!(response.context().id(), "ctx-survives");
    Ok(())
}

// 目的/场景：backend `Err` 必须传播到 `ServiceFrontend::generate` 调用方。
//
// 生产逻辑：`ServiceBackend::from_engine` 将 engine 错误向上返回，不转为空流。
//
// 测试计划：ErrorEngine 链接 pipeline → `generate` 一次。
//
// 关键断言：`generate` 返回 Err 且含 `backend contract error`。
#[tokio::test]
async fn pipeline_backend_error_propagates_to_client() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    let frontend = ServiceFrontend::<SingleIn<String>, ManyOut<TestResponse>>::new();
    let return_frontend = frontend.clone();
    let backend = ServiceBackend::from_engine(make_error_service_engine());
    let service: Arc<ServiceFrontend<SingleIn<String>, ManyOut<TestResponse>>> =
        frontend.link(backend)?.link(return_frontend)?;

    let err = match service.generate("input".to_string().into()).await {
        Ok(_) => anyhow!("backend error should propagate"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("backend contract error"));
    Ok(())
}

// 目的/场景：client 丢弃进程内 pipeline 响应流后，backend 应停止继续 emit。
//
// 生产逻辑：`ResponseStream` drop → `AsyncEngineContext::kill()` → backend 观察 `is_killed()`。
//
// 测试计划：cancellable service → 读首 chunk 后 drop response → 等待 cancelled flag。
//
// 关键断言：3s 内 `cancelled` 变为 true。
#[tokio::test]
async fn pipeline_cancel_stops_downstream_stream() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    let started = Arc::new(tokio::sync::Notify::new());
    let cancelled = Arc::new(AtomicBool::new(false));
    let service = make_cancellable_pipeline_service(started.clone(), cancelled.clone())?;

    let started_wait = started.notified();
    let mut response = service.generate("cancel".to_string().into()).await?;
    tokio::time::timeout(Duration::from_secs(3), started_wait).await?;
    assert!(response.next().await.is_some());
    drop(response);

    tokio::time::timeout(Duration::from_secs(3), async {
        while !cancelled.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;

    Ok(())
}

mod disaggregated {
    use anyhow::{Result, anyhow};
    use dynamo_runtime::{
        engine::AsyncEngine,
        pipeline::{SegmentSource, ServiceBackend, Source},
    };
    use futures::StreamExt;

    use super::common::contract::{acquire_contract_test_lock, make_echo_engine};
    use super::common::contract_engines::make_disaggregated_mock_network;

    // 目的/场景：disaggregated pipeline 经 MockNetworkTransport 跨“节点”完成 echo 往返。
    //
    // 生产逻辑：Node0 `MockNetworkEgress`；Node1 `MockNetworkIngress` → `SegmentSource` →
    // `ServiceBackend`（`tests/common/mock.rs`）。`SegmentSink::attach` 仍受
    // `AsyncEngineStream: Sync` 约束，此处直接驱动 egress 覆盖跨节点 segment 路径。
    //
    // 测试计划：spawn ingress 执行 loop → egress `generate` → remote segment_source → echo backend。
    //
    // 关键断言：响应流单 chunk 等于请求 payload。
    #[tokio::test]
    async fn disaggregated_pipeline_cross_node_roundtrip() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let segment_source = SegmentSource::new();
        let backend = ServiceBackend::from_engine(make_echo_engine());
        let _node1 = segment_source
            .link(backend)?
            .link(segment_source.clone())?;

        let (egress, ingress) = make_disaggregated_mock_network();
        ingress.segment(segment_source)?;
        let ingress_task = tokio::spawn(async move { ingress.execute().await });

        let payload = "disagg-echo";
        let mut response = egress.generate(payload.to_string().into()).await?;
        let item = response
            .next()
            .await
            .ok_or_else(|| anyhow!("missing disaggregated echo response"))?;
        assert_eq!(item.data.as_deref(), Some(payload));
        assert!(response.next().await.is_none());

        ingress_task.abort();
        let _ = ingress_task.await;
        Ok(())
    }
}
