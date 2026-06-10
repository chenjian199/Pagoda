// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

#![allow(dead_code)]

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use pagoda_runtime::{
    engine::{AsyncEngineContextProvider, ResponseStream},
    pipeline::{
        AsyncEngine, Error, ManyOut, Operator, PipelineError, PipelineNode, PipelineOperator,
        ServiceBackend, ServiceEngine, ServiceFrontend, SingleIn, Source,
    },
};
use futures::stream;
use futures::StreamExt;
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;

use super::contract::TestResponse;
use super::engines::{AsyncGenerator, LlmdbaEngine as LambdaEngine};
use super::mock::{MockNetworkIngress, MockNetworkOptions, MockNetworkTransport};

/// Never completes a request; used with a short response inactivity timeout.
pub struct HangForeverEngine;

/// Panics when the request payload matches `panic_on`; otherwise echoes the payload.
pub struct PanicOnPayloadEngine {
    pub panic_on: String,
}

#[async_trait]
impl AsyncEngine<SingleIn<String>, ManyOut<TestResponse>, Error> for PanicOnPayloadEngine {
    async fn generate(&self, request: SingleIn<String>) -> Result<ManyOut<TestResponse>, Error> {
        let (payload, ctx) = request.into_parts();
        if payload == self.panic_on {
            panic!("contract handler panic");
        }
        Ok(ResponseStream::new(
            Box::pin(stream::iter(vec![TestResponse::from_data(payload)])),
            ctx.context(),
        ))
    }
}

/// Mock network pair for disaggregated pipeline integration tests.
pub fn make_disaggregated_mock_network(
) -> (
    std::sync::Arc<super::mock::MockNetworkEgress<SingleIn<String>, ManyOut<TestResponse>>>,
    MockNetworkIngress<SingleIn<String>, ManyOut<TestResponse>>,
) {
    MockNetworkTransport::<SingleIn<String>, ManyOut<TestResponse>>::new_egress_ingress(
        MockNetworkOptions::default(),
    )
}

#[async_trait]
impl AsyncEngine<SingleIn<String>, ManyOut<TestResponse>, Error> for HangForeverEngine {
    async fn generate(&self, _request: SingleIn<String>) -> Result<ManyOut<TestResponse>, Error> {
        std::future::pending().await
    }
}

/// Echoes `tag-{instance_id}` so replica-pool tests can observe which worker served the RPC.
pub struct InstanceTagEngine {
    pub instance_id: u64,
}

#[async_trait]
impl AsyncEngine<SingleIn<String>, ManyOut<TestResponse>, Error> for InstanceTagEngine {
    async fn generate(&self, request: SingleIn<String>) -> Result<ManyOut<TestResponse>, Error> {
        let (_payload, ctx) = request.into_parts();
        let tag = format!("tag-{}", self.instance_id);
        Ok(ResponseStream::new(
            Box::pin(stream::iter(vec![TestResponse::from_data(tag)])),
            ctx.context(),
        ))
    }
}

pub struct ContextEchoEngine {
    pub seen: Arc<Mutex<HashMap<String, String>>>,
}

#[async_trait]
impl AsyncEngine<SingleIn<String>, ManyOut<TestResponse>, Error> for ContextEchoEngine {
    async fn generate(&self, request: SingleIn<String>) -> Result<ManyOut<TestResponse>, Error> {
        let request_id = request.id().to_string();
        let (payload, ctx) = request.into_parts();
        self.seen
            .lock()
            .await
            .insert(request_id.clone(), payload.clone());

        let response = TestResponse::from_data(format!("{payload}:{request_id}"));
        Ok(ResponseStream::new(
            Box::pin(stream::iter(vec![response])),
            ctx.context(),
        ))
    }
}

pub struct CancellableEngine {
    pub started: Arc<tokio::sync::Notify>,
    /// Set when the outbound stream is closed (mpsc send error) or `AsyncEngineContext::kill()`.
    pub cancelled: Arc<AtomicBool>,
}

#[async_trait]
impl AsyncEngine<SingleIn<String>, ManyOut<TestResponse>, Error> for CancellableEngine {
    async fn generate(&self, request: SingleIn<String>) -> Result<ManyOut<TestResponse>, Error> {
        let (_payload, ctx) = request.into_parts();
        let engine_ctx = ctx.context();
        let (tx, rx) = mpsc::channel(1);
        let started = self.started.clone();
        let cancelled = self.cancelled.clone();
        let engine_ctx_watch = engine_ctx.clone();

        tokio::spawn(async move {
            started.notify_waiters();
            for idx in 0..100usize {
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
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        Ok(ResponseStream::new(
            Box::pin(ReceiverStream::new(rx)),
            engine_ctx,
        ))
    }
}

pub struct ErrorEngine;

#[async_trait]
impl AsyncEngine<SingleIn<String>, ManyOut<TestResponse>, Error> for ErrorEngine {
    async fn generate(&self, _request: SingleIn<String>) -> Result<ManyOut<TestResponse>, Error> {
        Err(anyhow!("backend contract error"))
    }
}

pub fn make_error_service_engine() -> ServiceEngine<SingleIn<String>, ManyOut<TestResponse>> {
    Arc::new(ErrorEngine)
}

pub struct BlockingFirstChunkEngine {
    pub started: Arc<tokio::sync::Notify>,
    pub release: Arc<tokio::sync::Notify>,
}

/// Streams a fixed number of chunks through a small outbound channel to exercise backpressure.
pub struct HighVolumeStreamingEngine {
    pub chunk_count: usize,
}

#[async_trait]
impl AsyncEngine<SingleIn<String>, ManyOut<TestResponse>, Error> for HighVolumeStreamingEngine {
    async fn generate(&self, request: SingleIn<String>) -> Result<ManyOut<TestResponse>, Error> {
        let (_payload, ctx) = request.into_parts();
        let (tx, rx) = mpsc::channel(4);
        let chunk_count = self.chunk_count;

        tokio::spawn(async move {
            for idx in 0..chunk_count {
                if tx
                    .send(TestResponse::from_data(format!("chunk-{idx}")))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });

        Ok(ResponseStream::new(
            Box::pin(ReceiverStream::new(rx)),
            ctx.context(),
        ))
    }
}

#[async_trait]
impl AsyncEngine<SingleIn<String>, ManyOut<TestResponse>, Error> for BlockingFirstChunkEngine {
    async fn generate(&self, request: SingleIn<String>) -> Result<ManyOut<TestResponse>, Error> {
        let (_payload, ctx) = request.into_parts();
        let engine_ctx = ctx.context();
        let (tx, rx) = mpsc::channel(1);
        let started = self.started.clone();
        let release = self.release.clone();

        tokio::spawn(async move {
            started.notify_waiters();
            release.notified().await;
            let _ = tx.send(TestResponse::from_data("drained".to_string())).await;
        });

        Ok(ResponseStream::new(
            Box::pin(ReceiverStream::new(rx)),
            engine_ctx,
        ))
    }
}

struct PreprocessOperator;

#[async_trait]
impl Operator<SingleIn<String>, ManyOut<TestResponse>, SingleIn<String>, ManyOut<TestResponse>>
    for PreprocessOperator
{
    async fn generate(
        &self,
        request: SingleIn<String>,
        next: Arc<dyn AsyncEngine<SingleIn<String>, ManyOut<TestResponse>, Error>>,
    ) -> Result<ManyOut<TestResponse>, Error> {
        let request = request.map(|payload| format!("{payload}-pre"));
        let stream = next.generate(request).await?;
        let ctx = stream.context();
        let prefixed = stream.map(|item| match item.data {
            Some(data) => TestResponse::from_data(format!("{data}-op")),
            None => item,
        });
        Ok(ResponseStream::new(Box::pin(prefixed), ctx))
    }
}

/// Process-local pipeline whose backend streams until cancelled or the outbound channel closes.
pub fn make_cancellable_pipeline_service(
    started: Arc<tokio::sync::Notify>,
    cancelled: Arc<AtomicBool>,
) -> Result<ServiceEngine<SingleIn<String>, ManyOut<TestResponse>>, PipelineError> {
    let frontend = ServiceFrontend::<SingleIn<String>, ManyOut<TestResponse>>::new();
    let return_frontend = frontend.clone();
    let backend = ServiceBackend::from_engine(Arc::new(CancellableEngine { started, cancelled }));
    let service: Arc<ServiceFrontend<SingleIn<String>, ManyOut<TestResponse>>> =
        frontend.link(backend)?.link(return_frontend)?;
    Ok(service)
}

pub fn make_pipeline_contract_service()
-> Result<ServiceEngine<SingleIn<String>, ManyOut<TestResponse>>, PipelineError> {
    let frontend = ServiceFrontend::<SingleIn<String>, ManyOut<TestResponse>>::new();
    let return_frontend = frontend.clone();
    let preprocess = PipelineNode::<SingleIn<String>, SingleIn<String>>::new(Box::new(|req| {
        Ok(req.map(|payload| format!("{payload}-node")))
    }));
    let postprocess =
        PipelineNode::<ManyOut<TestResponse>, ManyOut<TestResponse>>::new(Box::new(|stream| {
            let ctx = stream.context();
            let mapped = stream.map(|item| match item.data {
                Some(data) => TestResponse::from_data(format!("{data}-post")),
                None => item,
            });
            Ok(ResponseStream::new(Box::pin(mapped), ctx))
        }));
    let backend = ServiceBackend::from_engine(LambdaEngine::from_generator(
        AsyncGenerator::<String, TestResponse>::new(|(req, stream)| async move {
            let _ = stream.emit(TestResponse::from_data(req)).await;
        }),
    ));
    let operator = PipelineOperator::new(Arc::new(PreprocessOperator));

    let service: Arc<ServiceFrontend<SingleIn<String>, ManyOut<TestResponse>>> = frontend
        .link(preprocess)?
        .link(operator.forward_edge())?
        .link(backend)?
        .link(postprocess)?
        .link(operator.backward_edge())?
        .link(return_frontend)?;

    Ok(service)
}
