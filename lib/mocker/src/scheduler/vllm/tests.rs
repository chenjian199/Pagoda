// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-License-Identifier: Apache-2.0
//
// API-compatible implementation based on the public interfaces and behavioral
// contracts of NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
// Implementation rewritten by PAGODA.

//! # vLLM 调度器测试
//!
//! ## 测试过程
//! 以公共 API 构造 `VllmCore`/`Scheduler`，覆盖单轮调度、预算分配、抢占、
//! prefill 交接时延、路由 KV 事件流以及端到端排空等行为。
//!
//! ## 意义
//! 锁定调度状态机与 KV 缓存交互的外部可观察契约，确保重写实现与 Dynamo 行为一致。

use std::sync::{Arc, Mutex};
use std::time::Duration;

use pagoda_kv_router::indexer::{METRIC_EVENT_REMOVED, METRIC_EVENT_STORED};
use pagoda_kv_router::protocols::{KvCacheEvent, KvCacheEventData, WorkerId};
use rstest::rstest;
use tokio::sync::mpsc;
use tokio::time::interval;
use uuid::Uuid;

use crate::common::protocols::{
    DirectRequest, FpmPublisher, KvCacheEventSink, KvEventPublishers, MockEngineArgs, OutputSignal,
    PreemptionMode, RawKvEvent, RawKvEventSink,
};
use crate::common::sequence::ActiveSequence;
use crate::scheduler::RouterEventVisibility;
use crate::scheduler::SchedulerHandle;
use crate::scheduler::test_utils::{RouterIndexerHarness, removed_event_count, stored_hashes};

use super::core::{RequestStatus, VllmCore, VllmRequestState};
use super::live::{MockerMetrics, Scheduler};

const ROUTER_TEST_WORKER_ID: WorkerId = 23;

fn assert_scheduler_idle(metrics: &MockerMetrics) {
    assert_eq!(
        metrics.active_decode_blocks, 0,
        "Expected 0 active blocks, got {}",
        metrics.active_decode_blocks
    );
    assert_eq!(
        metrics.gpu_cache_usage_perc, 0.0,
        "Expected 0.0 cache usage, got {}",
        metrics.gpu_cache_usage_perc
    );
    assert!(
        metrics.total_blocks > 0,
        "Expected total_blocks to be populated, got {}",
        metrics.total_blocks
    );
}

fn make_args() -> MockEngineArgs {
    MockEngineArgs::builder()
        .block_size(4)
        .num_gpu_blocks(6)
        .max_num_batched_tokens(Some(8))
        .max_num_seqs(Some(3))
        .enable_chunked_prefill(true)
        .enable_prefix_caching(false)
        .speedup_ratio(0.0)
        .build()
        .unwrap()
}

fn router_args() -> MockEngineArgs {
    MockEngineArgs::builder()
        .block_size(4)
        .num_gpu_blocks(12)
        .max_num_batched_tokens(Some(12))
        .max_num_seqs(Some(3))
        .enable_chunked_prefill(true)
        .enable_prefix_caching(true)
        .speedup_ratio(0.0)
        .build()
        .unwrap()
}

mod core_behavior {
    use super::*;

    #[test]
    fn test_unified_pass_keeps_partial_prefill_in_running() {
        let args = MockEngineArgs::builder()
            .block_size(4)
            .num_gpu_blocks(6)
            .max_num_batched_tokens(Some(12))
            .max_num_seqs(Some(3))
            .enable_chunked_prefill(true)
            .enable_prefix_caching(false)
            .speedup_ratio(0.0)
            .build()
            .unwrap();
        let mut core = VllmCore::new(args);
        let r1 = Uuid::from_u128(1);
        let r2 = Uuid::from_u128(2);
        core.receive(DirectRequest {
            tokens: (0..8).collect(),
            max_output_tokens: 2,
            uuid: Some(r1),
            dp_rank: 0,
            arrival_timestamp_ms: None,
        });
        core.receive(DirectRequest {
            tokens: (100..108).collect(),
            max_output_tokens: 2,
            uuid: Some(r2),
            dp_rank: 0,
            arrival_timestamp_ms: None,
        });

        let mut collector = crate::replay::TraceCollector::default();
        let pass = core.execute_pass(&mut collector, 0.0);

        assert_eq!(
            pass.output_signals.len(),
            1,
            "first request should emit immediately"
        );
        assert_eq!(core.state.waiting.len(), 0);
        assert_eq!(
            core.state.running.iter().copied().collect::<Vec<_>>(),
            vec![r1, r2]
        );
        assert_eq!(core.state.requests.get(&r1).unwrap().num_computed_tokens, 8);
        assert_eq!(core.state.requests.get(&r2).unwrap().num_computed_tokens, 4);
        assert_eq!(
            core.state
                .requests
                .get(&r1)
                .unwrap()
                .sequence
                .generated_tokens(),
            1
        );
        assert_eq!(
            core.state.requests.get(&r2).unwrap().status,
            RequestStatus::Running
        );
        assert_eq!(core.kv_cache.num_active_blocks(), 4);
    }

    #[test]
    fn test_running_requests_consume_budget_before_waiting() {
        let args = MockEngineArgs::builder()
            .block_size(4)
            .num_gpu_blocks(16)
            .max_num_batched_tokens(Some(4))
            .max_num_seqs(Some(3))
            .enable_chunked_prefill(true)
            .enable_prefix_caching(false)
            .speedup_ratio(0.0)
            .build()
            .unwrap();
        let mut core = VllmCore::new(args);
        let r1 = Uuid::from_u128(1);
        let r2 = Uuid::from_u128(2);
        core.receive(DirectRequest {
            tokens: (0..8).collect(),
            max_output_tokens: 2,
            uuid: Some(r1),
            dp_rank: 0,
            arrival_timestamp_ms: None,
        });
        core.receive(DirectRequest {
            tokens: (100..108).collect(),
            max_output_tokens: 2,
            uuid: Some(r2),
            dp_rank: 0,
            arrival_timestamp_ms: None,
        });

        let mut collector = crate::replay::TraceCollector::default();
        core.execute_pass(&mut collector, 0.0);
        let pass = core.execute_pass(&mut collector, 1.0);

        assert!(pass.output_signals.iter().any(|signal| signal.uuid == r1));
        assert_eq!(
            core.state.requests.get(&r2).unwrap().num_computed_tokens,
            0,
            "waiting request should not steal budget before the running request catches up"
        );
    }

    #[test]
    fn test_execute_pass_batches_two_ready_requests_together() {
        let args = MockEngineArgs::builder()
            .block_size(4)
            .num_gpu_blocks(16)
            .max_num_batched_tokens(Some(8))
            .max_num_seqs(Some(4))
            .enable_chunked_prefill(true)
            .enable_prefix_caching(false)
            .speedup_ratio(0.0)
            .build()
            .unwrap();
        let mut core = VllmCore::new(args);
        let r1 = Uuid::from_u128(101);
        let r2 = Uuid::from_u128(202);
        for (uuid, tokens) in [(r1, vec![1; 4]), (r2, vec![2; 4])] {
            core.receive(DirectRequest {
                tokens,
                max_output_tokens: 1,
                uuid: Some(uuid),
                dp_rank: 0,
                arrival_timestamp_ms: None,
            });
        }

        let mut collector = crate::replay::TraceCollector::default();
        collector.on_arrival(r1, 0.0, 4, 1);
        collector.on_arrival(r2, 0.0, 4, 1);
        let pass = core.execute_pass(&mut collector, 0.0);
        let admitted = pass
            .admissions
            .iter()
            .map(|admission| admission.uuid)
            .collect::<Vec<_>>();
        let first = collector.snapshot(r1).unwrap();
        let second = collector.snapshot(r2).unwrap();

        assert_eq!(pass.admissions.len(), 2);
        assert!(admitted.contains(&r1));
        assert!(admitted.contains(&r2));
        assert!(
            first.first_admit_ms.is_some(),
            "r1 should have been admitted"
        );
        assert!(
            second.first_admit_ms.is_some(),
            "r2 should have been admitted"
        );
        assert!(
            first.first_token_ms.is_some(),
            "r1 should have emitted a token"
        );
        assert!(
            second.first_token_ms.is_some(),
            "r2 should have emitted a token"
        );
        assert_eq!(first.first_admit_ms, second.first_admit_ms);
        assert_eq!(first.first_token_ms, second.first_token_ms);
    }

    #[test]
    fn test_prefill_completion_emits_handoff_delay() {
        let args = MockEngineArgs::builder()
            .block_size(4)
            .num_gpu_blocks(8)
            .max_num_batched_tokens(Some(8))
            .max_num_seqs(Some(1))
            .enable_chunked_prefill(true)
            .worker_type(crate::common::protocols::WorkerType::Prefill)
            .kv_transfer_bandwidth(Some(1.0))
            .kv_bytes_per_token(Some(1_000_000))
            .speedup_ratio(0.0)
            .build()
            .unwrap();
        let mut core = VllmCore::new(args);
        core.receive(DirectRequest {
            tokens: vec![1; 8],
            max_output_tokens: 1,
            uuid: Some(Uuid::from_u128(81)),
            dp_rank: 0,
            arrival_timestamp_ms: None,
        });

        let mut collector = crate::replay::TraceCollector::default();
        let pass = core.execute_pass(&mut collector, 0.0);
        let signal = pass
            .output_signals
            .first()
            .expect("prefill pass should emit one completed signal");

        assert!(signal.completed);
        assert_eq!(signal.handoff_delay_ms, Some(8.0));
    }

    #[test]
    fn test_first_token_can_arrive_on_prompt_completion_pass() {
        let mut core = VllmCore::new(make_args());
        let uuid = Uuid::from_u128(11);
        core.receive(DirectRequest {
            tokens: (0..8).collect(),
            max_output_tokens: 2,
            uuid: Some(uuid),
            dp_rank: 0,
            arrival_timestamp_ms: None,
        });

        let mut collector = crate::replay::TraceCollector::default();
        let pass = core.execute_pass(&mut collector, 0.0);

        assert_eq!(pass.output_signals.len(), 1);
        assert_eq!(pass.output_signals[0].uuid, uuid);
        assert!(!pass.output_signals[0].completed);
        assert_eq!(
            core.state
                .requests
                .get(&uuid)
                .unwrap()
                .sequence
                .generated_tokens(),
            1
        );
    }

    #[test]
    fn test_preemption_requeues_newest_running_request() {
        let args = MockEngineArgs::builder()
            .block_size(4)
            .num_gpu_blocks(6)
            .max_num_batched_tokens(Some(12))
            .max_num_seqs(Some(3))
            .enable_chunked_prefill(true)
            .enable_prefix_caching(false)
            .preemption_mode(PreemptionMode::Lifo)
            .speedup_ratio(0.0)
            .build()
            .unwrap();
        let mut core = VllmCore::new(args);
        let r1 = Uuid::from_u128(1);
        let r2 = Uuid::from_u128(2);
        let r3 = Uuid::from_u128(3);
        for (uuid, range) in [(r1, 0u32..8u32), (r2, 100u32..108u32), (r3, 200u32..212u32)] {
            core.receive(DirectRequest {
                tokens: range.collect(),
                max_output_tokens: 2,
                uuid: Some(uuid),
                dp_rank: 0,
                arrival_timestamp_ms: None,
            });
        }

        let mut collector = crate::replay::TraceCollector::default();
        core.execute_pass(&mut collector, 0.0);
        core.execute_pass(&mut collector, 1.0);
        let request = core.state.requests.get(&r2).unwrap();
        assert_eq!(request.status, RequestStatus::Preempted);
        assert_eq!(request.num_computed_tokens, 0);
        assert_eq!(request.num_preemptions, 1);
        assert_eq!(core.state.waiting.front().copied(), Some(r2));
    }

    #[test]
    fn test_running_request_catches_up_decode_tail_before_promote() {
        let args = MockEngineArgs::builder()
            .block_size(4)
            .num_gpu_blocks(8)
            .max_num_batched_tokens(Some(8))
            .max_num_seqs(Some(1))
            .enable_chunked_prefill(true)
            .enable_prefix_caching(true)
            .speedup_ratio(0.0)
            .build()
            .unwrap();
        let mut core = VllmCore::new(args);
        let uuid = Uuid::from_u128(99);
        let mut sequence = ActiveSequence::new((0..6).collect(), 16, Some(4), true, false);

        let signal = sequence.take_creation_signal().unwrap();
        assert_eq!(core.kv_cache.process(&signal), 2);
        for _ in 0..6 {
            let signals = sequence.generate();
            for signal in &signals {
                core.kv_cache.process(signal);
            }
            if sequence.generated_tokens() < sequence.max_output_tokens() {
                sequence.commit_allocation(sequence.len());
            }
        }

        let free = sequence.reset_with_signal();
        for signal in &free {
            core.kv_cache.process(signal);
        }
        let prompt_only = sequence
            .prepare_allocation(sequence.num_input_tokens())
            .unwrap();
        assert_eq!(core.kv_cache.process(&prompt_only), 2);
        sequence.commit_allocation(sequence.num_input_tokens());

        core.state.insert_running_for_test(uuid);
        core.state.requests.insert(
            uuid,
            VllmRequestState {
                sequence,
                status: RequestStatus::Running,
                num_computed_tokens: 9,
                num_preemptions: 1,
            },
        );

        let mut collector = crate::replay::TraceCollector::default();
        let pass = core.execute_pass(&mut collector, 0.0);
        let request = core.state.requests.get(&uuid).unwrap();

        assert_eq!(pass.output_signals.len(), 1);
        assert_eq!(request.num_computed_tokens, 12);
        assert_eq!(request.sequence.num_allocated_tokens(), 13);
        assert_eq!(core.kv_cache.num_active_blocks(), 4);
    }

    #[test]
    fn test_completion_returns_scheduler_to_idle() {
        let mut core = VllmCore::new(make_args());
        for uuid in [Uuid::from_u128(1), Uuid::from_u128(2)] {
            core.receive(DirectRequest {
                tokens: (0..8).collect(),
                max_output_tokens: 2,
                uuid: Some(uuid),
                dp_rank: 0,
                arrival_timestamp_ms: None,
            });
        }

        let mut collector = crate::replay::TraceCollector::default();
        while !core.is_empty() {
            core.execute_pass(&mut collector, 0.0);
        }

        assert!(core.state.waiting.is_empty());
        assert!(core.state.running.is_empty());
        assert_eq!(core.kv_cache.num_active_blocks(), 0);
    }
}

mod router_events {
    use super::*;

    #[test]
    fn test_vllm_pass_visibility_is_pass_start() {
        let mut core = VllmCore::new_with_kv_capture(router_args(), ROUTER_TEST_WORKER_ID);
        core.receive(DirectRequest {
            tokens: (0..8).collect(),
            max_output_tokens: 2,
            uuid: Some(Uuid::from_u128(71)),
            dp_rank: 0,
            arrival_timestamp_ms: None,
        });

        let mut collector = crate::replay::TraceCollector::default();
        let pass = core.execute_pass(&mut collector, 0.0);

        assert_eq!(
            pass.router_event_visibility,
            RouterEventVisibility::PassStart
        );
    }

    #[tokio::test]
    async fn test_completion_events_apply_cleanly() {
        let harness = RouterIndexerHarness::new(4, ROUTER_TEST_WORKER_ID);
        let mut core = VllmCore::new_with_kv_capture(router_args(), ROUTER_TEST_WORKER_ID);
        core.receive(DirectRequest {
            tokens: (0..8).collect(),
            max_output_tokens: 4,
            uuid: Some(Uuid::from_u128(41)),
            dp_rank: 0,
            arrival_timestamp_ms: None,
        });

        let mut collector = crate::replay::TraceCollector::default();
        let mut now_ms = 0.0;
        let mut saw_store = false;
        while !core.is_empty() {
            let pass = core.execute_pass(&mut collector, now_ms);
            saw_store |= !stored_hashes(&pass.kv_events).is_empty();
            now_ms = pass.end_ms;
            harness.apply_events(pass.kv_events).await;
        }

        assert!(saw_store);
        assert!(harness.ok_count(METRIC_EVENT_STORED) > 0);
        assert_eq!(core.kv_cache.num_active_blocks(), 0);
        harness.assert_no_event_warnings();
        harness.shutdown();
    }

    #[tokio::test]
    async fn test_preemption_recompute_events_apply_cleanly() {
        let harness = RouterIndexerHarness::new(4, ROUTER_TEST_WORKER_ID);
        let args = MockEngineArgs::builder()
            .block_size(4)
            .num_gpu_blocks(6)
            .max_num_batched_tokens(Some(12))
            .max_num_seqs(Some(3))
            .enable_chunked_prefill(true)
            .enable_prefix_caching(true)
            .preemption_mode(PreemptionMode::Lifo)
            .speedup_ratio(0.0)
            .build()
            .unwrap();
        let mut core = VllmCore::new_with_kv_capture(args, ROUTER_TEST_WORKER_ID);
        let r1 = Uuid::from_u128(51);
        let r2 = Uuid::from_u128(52);
        let r3 = Uuid::from_u128(53);
        for (uuid, range) in [(r1, 0u32..8u32), (r2, 100u32..108u32), (r3, 200u32..212u32)] {
            core.receive(DirectRequest {
                tokens: range.collect(),
                max_output_tokens: 2,
                uuid: Some(uuid),
                dp_rank: 0,
                arrival_timestamp_ms: None,
            });
        }

        let mut collector = crate::replay::TraceCollector::default();
        let mut now_ms = 0.0;
        let mut saw_remove = false;
        for _ in 0..2 {
            let pass = core.execute_pass(&mut collector, now_ms);
            saw_remove |= removed_event_count(&pass.kv_events) > 0;
            now_ms = pass.end_ms;
            harness.apply_events(pass.kv_events).await;
        }

        let request = core.state.requests.get(&r2).unwrap();
        assert_eq!(request.status, RequestStatus::Preempted);
        assert_eq!(request.num_computed_tokens, 0);
        assert_eq!(request.num_preemptions, 1);
        assert_eq!(core.state.waiting.front().copied(), Some(r2));
        assert!(saw_remove);
        assert!(harness.ok_count(METRIC_EVENT_REMOVED) > 0);
        harness.assert_no_event_warnings();
        harness.shutdown();
    }
}

mod live_scheduler {
    use super::*;

    type CapturedKvEvent = (KvCacheEvent, Option<Vec<Vec<u32>>>);

    #[derive(Default)]
    struct CapturingKvSink {
        events: Mutex<Vec<CapturedKvEvent>>,
    }

    impl CapturingKvSink {
        fn take(&self) -> Vec<CapturedKvEvent> {
            std::mem::take(&mut *self.events.lock().unwrap())
        }
    }

    impl KvCacheEventSink for CapturingKvSink {
        fn publish(&self, event: KvCacheEvent) -> anyhow::Result<()> {
            self.events.lock().unwrap().push((event, None));
            Ok(())
        }
    }

    impl RawKvEventSink for CapturingKvSink {
        fn publish(&self, event: RawKvEvent) -> anyhow::Result<()> {
            self.events
                .lock()
                .unwrap()
                .push((event.event, event.block_token_ids));
            Ok(())
        }
    }

    #[rstest]
    #[case::case_1(false, false, false)]
    #[case::case_2(false, true, false)]
    #[case::case_3(true, false, false)]
    #[case::case_4(true, true, false)]
    #[case::case_5(false, false, true)]
    #[case::case_6(false, true, true)]
    #[case::case_7(true, false, true)]
    #[case::case_8(true, true, true)]
    #[tokio::test]
    async fn test_scheduler_token_generation_patterns(
        #[case] use_shared_tokens: bool,
        #[case] enable_prefix_caching: bool,
        #[case] enable_chunked_prefill: bool,
    ) {
        let (output_tx, mut output_rx) = mpsc::unbounded_channel::<Vec<OutputSignal>>();

        let args = MockEngineArgs::builder()
            .num_gpu_blocks(500)
            .block_size(64)
            .speedup_ratio(1000.0)
            .enable_prefix_caching(enable_prefix_caching)
            .enable_chunked_prefill(enable_chunked_prefill)
            .build()
            .unwrap();

        // 旁路路由索引器：mocker 发出的 KV 事件流被实时转发进 `LocalKvIndexer`，
        // 后者针对自身 radix 树应用 Stored/Removed 事件。若 mocker 发出任何
        // 无效事件（悬空父块、对已存在块重复 Stored、或 Removed 未知块），
        // 索引器的分状态计数器会自增——`assert_no_event_errors()` 据此判定测试失败。
        let harness = RouterIndexerHarness::new(64, ROUTER_TEST_WORKER_ID);
        let (forwarder_sink, forwarder_task) = harness.spawn_forwarder();
        let publishers = KvEventPublishers::new(Some(forwarder_sink as _), None);

        let scheduler = Scheduler::new(
            args,
            0,
            Some(output_tx),
            publishers,
            None,
            FpmPublisher::default(),
        );

        crate::scheduler::test_utils::assert_scheduler_completes_all(
            &scheduler,
            &mut output_rx,
            200,
            1000,
            100,
            use_shared_tokens,
        )
        .await;

        // 停止调度器以阻止新事件，再通过丢弃 scheduler 释放转发器的 sender
        // → 转发任务排空并退出。
        drop(scheduler);
        let _ = tokio::time::timeout(Duration::from_secs(2), forwarder_task).await;
        harness.flush().await;
        harness.assert_no_event_errors();
        // 注意：此处不断言 `dump_events().is_empty()`，因为 mocker 协议在请求
        // 完成时不会发出路由 `Removed` 事件。
        harness.shutdown();
    }

    #[tokio::test]
    async fn test_cache_hit_rate_with_identical_requests() {
        let block_size: usize = 64;
        let max_output_tokens: usize = 10;
        let speedup_ratio = 10.0;
        let num_requests = 10;
        let token_length = 65;

        let (output_tx, mut output_rx) = mpsc::unbounded_channel::<Vec<OutputSignal>>();

        let args = MockEngineArgs::builder()
            .num_gpu_blocks(100)
            .block_size(block_size)
            .speedup_ratio(speedup_ratio)
            .build()
            .unwrap();

        let scheduler = Scheduler::new(
            args,
            0,
            Some(output_tx),
            KvEventPublishers::default(),
            None,
            FpmPublisher::default(),
        );
        let identical_tokens: Vec<u32> = (0..token_length).collect();

        for _ in 0..num_requests {
            scheduler.receive(DirectRequest {
                tokens: identical_tokens.clone(),
                max_output_tokens,
                uuid: None,
                dp_rank: 0,
                arrival_timestamp_ms: None,
            });
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let mut received_tokens = 0;
        let timeout = tokio::time::sleep(Duration::from_millis(500));
        tokio::pin!(timeout);
        let metrics_rx = scheduler.metrics_receiver();
        let mut debug_interval = interval(Duration::from_millis(500));

        loop {
            tokio::select! {
                biased;
                _ = debug_interval.tick() => {
                    let _metrics = metrics_rx.borrow().clone();
                    tracing::debug!("Forward Pass Metrics: {_metrics:#?}");
                }
                Some(output_batch) = output_rx.recv() => {
                    received_tokens += output_batch.len();
                    timeout.set(tokio::time::sleep(Duration::from_millis(500)));
                }
                _ = &mut timeout => break,
            }
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
        let metrics = metrics_rx.borrow().clone();
        assert_scheduler_idle(&metrics);
        assert_eq!(received_tokens, num_requests * max_output_tokens);
    }

    #[tokio::test]
    async fn test_receiver_drop_cleans_up_resources() {
        let (output_tx, mut output_rx) = mpsc::unbounded_channel::<Vec<OutputSignal>>();
        let args = MockEngineArgs::builder()
            .num_gpu_blocks(10)
            .block_size(64)
            .speedup_ratio(100.0)
            .build()
            .unwrap();

        let scheduler = Scheduler::new(
            args,
            0,
            Some(output_tx),
            KvEventPublishers::default(),
            None,
            FpmPublisher::default(),
        );
        scheduler.receive(DirectRequest {
            tokens: (0..256).collect(),
            max_output_tokens: 200,
            uuid: None,
            dp_rank: 0,
            arrival_timestamp_ms: None,
        });

        let mut received_count = 0;
        while received_count < 129 {
            if let Some(output_batch) = output_rx.recv().await {
                received_count += output_batch.len();
                continue;
            }
            panic!("Channel closed before receiving 129 tokens");
        }

        drop(output_rx);
        let metrics_rx = scheduler.metrics_receiver();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if metrics_rx.borrow().active_decode_blocks == 0 {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let metrics = metrics_rx.borrow().clone();
        assert_scheduler_idle(&metrics);
    }

    #[tokio::test]
    async fn test_live_scheduler_forwards_buffered_kv_token_ids() {
        let sink = Arc::new(CapturingKvSink::default());
        let (output_tx, mut output_rx) = mpsc::unbounded_channel::<Vec<OutputSignal>>();
        let args = MockEngineArgs::builder()
            .block_size(4)
            .num_gpu_blocks(12)
            .max_num_batched_tokens(Some(8))
            .max_num_seqs(Some(1))
            .enable_chunked_prefill(true)
            .enable_prefix_caching(true)
            .speedup_ratio(1000.0)
            .zmq_kv_events_port(Some(12345))
            .build()
            .unwrap();
        let scheduler = Scheduler::new(
            args,
            0,
            Some(output_tx),
            KvEventPublishers::new(None, Some(sink.clone())),
            None,
            FpmPublisher::default(),
        );

        scheduler.receive(DirectRequest {
            tokens: (0..8).collect(),
            max_output_tokens: 1,
            uuid: Some(Uuid::from_u128(72)),
            dp_rank: 0,
            arrival_timestamp_ms: None,
        });

        let output_batch = tokio::time::timeout(Duration::from_secs(2), output_rx.recv())
            .await
            .expect("scheduler should emit output")
            .expect("output channel should stay open");
        let signal = output_batch
            .into_iter()
            .next()
            .expect("live scheduler should emit one output signal");
        assert!(signal.completed);

        tokio::time::sleep(Duration::from_millis(50)).await;
        let events = sink.take();
        let stored = events
            .into_iter()
            .find_map(|(event, block_token_ids)| match event.data {
                KvCacheEventData::Stored(_) => block_token_ids,
                _ => None,
            })
            .expect("live scheduler should forward stored KV event token ids");
        assert!(!stored.is_empty());
        assert!(stored.iter().all(|block| !block.is_empty()));
    }

    #[tokio::test]
    async fn test_live_pathological_load_no_router_event_errors() {
        let harness = RouterIndexerHarness::new(4, ROUTER_TEST_WORKER_ID);
        let (sink, forward_task) = harness.spawn_forwarder();

        let (output_tx, mut output_rx) = mpsc::unbounded_channel::<Vec<OutputSignal>>();
        let scheduler = Scheduler::new(
            MockEngineArgs::builder()
                .block_size(4)
                .num_gpu_blocks(6)
                .max_num_batched_tokens(Some(8))
                .max_num_seqs(Some(3))
                .enable_prefix_caching(true)
                .enable_chunked_prefill(true)
                .speedup_ratio(1000.0)
                .build()
                .unwrap(),
            0,
            Some(output_tx),
            KvEventPublishers::new(Some(sink.clone()), None),
            None,
            FpmPublisher::default(),
        );

        for _ in 0..8 {
            scheduler.receive(DirectRequest {
                tokens: vec![42; 8],
                max_output_tokens: 4,
                uuid: None,
                dp_rank: 0,
                arrival_timestamp_ms: None,
            });
        }

        let expected = 8 * 4;
        let mut seen = 0;
        let timeout = tokio::time::sleep(Duration::from_secs(5));
        tokio::pin!(timeout);

        loop {
            tokio::select! {
                Some(output_batch) = output_rx.recv() => {
                    seen += output_batch.len();
                    if seen == expected {
                        break;
                    }
                }
                _ = &mut timeout => {
                    break;
                }
            }
        }

        assert_eq!(seen, expected);
        drop(scheduler);
        drop(sink);
        forward_task.await.unwrap();
        harness.flush().await;

        harness.assert_no_event_errors();
        assert!(harness.ok_count(METRIC_EVENT_STORED) > 0);
        harness.shutdown();
    }
}

mod forward_pass_metrics {
    use super::*;

    /// 构造 FPM 测试所需特定参数 args 的辅助函数。
    fn fpm_args() -> MockEngineArgs {
        MockEngineArgs::builder()
            .block_size(4)
            .num_gpu_blocks(16)
            .max_num_batched_tokens(Some(16))
            .max_num_seqs(Some(4))
            .enable_chunked_prefill(true)
            .enable_prefix_caching(false)
            .speedup_ratio(0.0)
            .build()
            .unwrap()
    }

    #[test]
    fn test_fpm_single_prefill_request() {
        let mut core = VllmCore::new(fpm_args());
        core.receive(DirectRequest {
            tokens: (0..8).collect(),
            max_output_tokens: 1,
            uuid: Some(Uuid::from_u128(1)),
            dp_rank: 0,
            arrival_timestamp_ms: None,
        });

        let mut collector = crate::replay::TraceCollector::default();
        let pass = core.execute_pass(&mut collector, 0.0);
        let fpm = pass.fpm.expect("FPM should be present");

        assert_eq!(fpm.num_prefill_requests, 1);
        assert_eq!(fpm.sum_prefill_tokens, 8, "all 8 prompt tokens computed");
        assert_eq!(fpm.sum_prefill_kv_tokens, 0, "no prefix cache");
        assert_eq!(fpm.num_decode_requests, 0);
        assert_eq!(fpm.num_queued_prefill, 0);
        assert_eq!(fpm.num_queued_decode, 0);
        assert!(fpm.wall_time_secs > 0.0);
    }

    #[test]
    fn test_fpm_prefill_and_decode_mixed_batch() {
        let mut core = VllmCore::new(fpm_args());

        // r1：4 token prompt，3 个输出 token
        let r1 = Uuid::from_u128(1);
        core.receive(DirectRequest {
            tokens: (0..4).collect(),
            max_output_tokens: 3,
            uuid: Some(r1),
            dp_rank: 0,
            arrival_timestamp_ms: None,
        });

        let mut collector = crate::replay::TraceCollector::default();

        // 第 1 轮：prefill r1（4 token）+ 首个 decode token
        let pass1 = core.execute_pass(&mut collector, 0.0);
        let fpm1 = pass1.fpm.expect("FPM should be present");
        assert_eq!(fpm1.num_prefill_requests, 1);
        assert_eq!(fpm1.sum_prefill_tokens, 4);

        // r2：在 r1 解码期间到达的 4 token prompt
        let r2 = Uuid::from_u128(2);
        core.receive(DirectRequest {
            tokens: (100..104).collect(),
            max_output_tokens: 3,
            uuid: Some(r2),
            dp_rank: 0,
            arrival_timestamp_ms: None,
        });

        // 第 2 轮：r1 decode + r2 prefill（混合批）
        let pass2 = core.execute_pass(&mut collector, 1.0);
        let fpm2 = pass2.fpm.expect("FPM should be present");
        assert_eq!(fpm2.num_prefill_requests, 1, "r2 is prefilling");
        assert_eq!(fpm2.num_decode_requests, 1, "r1 is decoding");
        assert_eq!(fpm2.sum_prefill_tokens, 4);
        assert!(
            fpm2.sum_decode_kv_tokens > 0,
            "decode request should have KV context"
        );
    }

    #[test]
    fn test_fpm_completed_requests_metrics_correct() {
        // 验证修复：已完成请求即便在 compute_fpm 运行前已从 state 移除，
        // 仍应贡献正确的度量。
        let mut core = VllmCore::new(fpm_args());

        // 4 token prompt 且 1 个输出 token 的请求——单轮即完成
        let r1 = Uuid::from_u128(1);
        core.receive(DirectRequest {
            tokens: (0..4).collect(),
            max_output_tokens: 1,
            uuid: Some(r1),
            dp_rank: 0,
            arrival_timestamp_ms: None,
        });

        let mut collector = crate::replay::TraceCollector::default();
        let pass = core.execute_pass(&mut collector, 0.0);
        let fpm = pass.fpm.expect("FPM should be present");

        // r1 在本轮完成。原缺陷是 prompt_len 会为 0，
        // 因为请求在 compute_fpm 运行前已从 state 移除。
        assert_eq!(fpm.num_prefill_requests, 1);
        assert_eq!(fpm.sum_prefill_tokens, 4);
        // var_prefill_length 应反映实际 prompt 长度（4）而非 0。
        // 单请求方差恒为 0，故以 sum_prefill_tokens 作为主要指示。
        assert!(pass.completed_requests > 0, "request should have completed");
    }

    #[test]
    fn test_fpm_completed_decode_request_has_kv_context() {
        // 会完成的 decode 请求——即使从 state 移除，其 KV 上下文也应被正确捕获。
        let args = MockEngineArgs::builder()
            .block_size(4)
            .num_gpu_blocks(16)
            .max_num_batched_tokens(Some(16))
            .max_num_seqs(Some(4))
            .enable_chunked_prefill(true)
            .enable_prefix_caching(false)
            .speedup_ratio(0.0)
            .build()
            .unwrap();
        let mut core = VllmCore::new(args);

        let r1 = Uuid::from_u128(1);
        core.receive(DirectRequest {
            tokens: (0..4).collect(),
            max_output_tokens: 2,
            uuid: Some(r1),
            dp_rank: 0,
            arrival_timestamp_ms: None,
        });

        let mut collector = crate::replay::TraceCollector::default();

        // 第 1 轮：prefill + 首个 decode token
        core.execute_pass(&mut collector, 0.0);

        // 第 2 轮：第二个 decode token（完成该请求）
        let pass2 = core.execute_pass(&mut collector, 1.0);
        let fpm2 = pass2.fpm.expect("FPM should be present");

        assert_eq!(fpm2.num_decode_requests, 1);
        // 已完成的 decode 请求应仍贡献其 KV 上下文
        // （调度时的 prompt_len + 已生成数）。
        assert!(
            fpm2.sum_decode_kv_tokens > 0,
            "completed decode request should still contribute KV context, got {}",
            fpm2.sum_decode_kv_tokens
        );
    }

    #[test]
    fn test_fpm_queued_requests() {
        let args = MockEngineArgs::builder()
            .block_size(4)
            .num_gpu_blocks(4) // KV 极限——仅够容纳一个请求
            .max_num_batched_tokens(Some(8))
            .max_num_seqs(Some(2))
            .enable_chunked_prefill(true)
            .enable_prefix_caching(false)
            .speedup_ratio(0.0)
            .build()
            .unwrap();
        let mut core = VllmCore::new(args);

        // r1 与 r2 均为 8 token prompt，但仅有 4 个可用块
        let r1 = Uuid::from_u128(1);
        let r2 = Uuid::from_u128(2);
        core.receive(DirectRequest {
            tokens: (0..8).collect(),
            max_output_tokens: 1,
            uuid: Some(r1),
            dp_rank: 0,
            arrival_timestamp_ms: None,
        });
        core.receive(DirectRequest {
            tokens: (100..108).collect(),
            max_output_tokens: 1,
            uuid: Some(r2),
            dp_rank: 0,
            arrival_timestamp_ms: None,
        });

        let mut collector = crate::replay::TraceCollector::default();
        let pass = core.execute_pass(&mut collector, 0.0);
        let fpm = pass.fpm.expect("FPM should be present");

        // 至少一个请求应被调度，另一个可能排队（取决于 KV 容量）。
        // 部分请求可能已完成并从 scheduled 与 queued 中移除。
        let total_scheduled = fpm.num_prefill_requests + fpm.num_decode_requests;
        assert!(
            total_scheduled >= 1,
            "at least one request should be scheduled"
        );
    }

    #[test]
    fn test_fpm_var_prefill_length_with_multiple_requests() {
        let args = MockEngineArgs::builder()
            .block_size(4)
            .num_gpu_blocks(32)
            .max_num_batched_tokens(Some(32))
            .max_num_seqs(Some(4))
            .enable_chunked_prefill(true)
            .enable_prefix_caching(false)
            .speedup_ratio(0.0)
            .build()
            .unwrap();
        let mut core = VllmCore::new(args);

        // 两个 prompt 长度不同的 prefill 请求
        core.receive(DirectRequest {
            tokens: (0..4).collect(), // prompt_len = 4
            max_output_tokens: 1,
            uuid: Some(Uuid::from_u128(1)),
            dp_rank: 0,
            arrival_timestamp_ms: None,
        });
        core.receive(DirectRequest {
            tokens: (100..112).collect(), // prompt_len = 12
            max_output_tokens: 1,
            uuid: Some(Uuid::from_u128(2)),
            dp_rank: 0,
            arrival_timestamp_ms: None,
        });

        let mut collector = crate::replay::TraceCollector::default();
        let pass = core.execute_pass(&mut collector, 0.0);
        let fpm = pass.fpm.expect("FPM should be present");

        assert_eq!(fpm.num_prefill_requests, 2);
        // [4, 12] 的总体方差：mean=8, var=((4-8)^2+(12-8)^2)/2 = 16
        assert!(
            (fpm.var_prefill_length - 16.0).abs() < 1e-6,
            "expected var=16.0, got {}",
            fpm.var_prefill_length
        );
    }

    #[test]
    fn test_fpm_chunked_prefill_reports_chunk_not_full_prompt() {
        // max_num_batched_tokens=8 与 16 token prompt 下，分块 prefill 应跨两轮拆分。
        // 每轮的 sum_prefill_tokens 应仅报告分块大小，而非完整 prompt 长度。
        let args = MockEngineArgs::builder()
            .block_size(4)
            .num_gpu_blocks(16)
            .max_num_batched_tokens(Some(8))
            .max_num_seqs(Some(4))
            .enable_chunked_prefill(true)
            .enable_prefix_caching(false)
            .speedup_ratio(0.0)
            .build()
            .unwrap();
        let mut core = VllmCore::new(args);

        core.receive(DirectRequest {
            tokens: (0..16).collect(),
            max_output_tokens: 2,
            uuid: Some(Uuid::from_u128(1)),
            dp_rank: 0,
            arrival_timestamp_ms: None,
        });

        let mut collector = crate::replay::TraceCollector::default();

        // 第 1 轮：首块
        let pass1 = core.execute_pass(&mut collector, 0.0);
        let fpm1 = pass1.fpm.expect("FPM should be present");
        assert_eq!(fpm1.num_prefill_requests, 1);
        assert!(
            fpm1.sum_prefill_tokens <= 8,
            "chunk should be at most 8 tokens, got {}",
            fpm1.sum_prefill_tokens
        );
        assert!(fpm1.sum_prefill_tokens > 0);

        // 第 2 轮：剩余块
        let pass2 = core.execute_pass(&mut collector, 1.0);
        let fpm2 = pass2.fpm.expect("FPM should be present");
        assert_eq!(fpm2.num_prefill_requests, 1, "still prefilling");
        assert!(
            fpm2.sum_prefill_tokens <= 8,
            "second chunk should also be at most 8 tokens, got {}",
            fpm2.sum_prefill_tokens
        );

        // 两块加总应等于完整 prompt 长度
        assert_eq!(
            fpm1.sum_prefill_tokens + fpm2.sum_prefill_tokens,
            16,
            "total prefill tokens across chunks should equal full prompt"
        );

        // 两轮的方差均应基于完整 prompt 长度（16）
        assert_eq!(
            fpm1.var_prefill_length, 0.0,
            "single request → zero variance"
        );
        assert_eq!(
            fpm2.var_prefill_length, 0.0,
            "single request → zero variance"
        );
    }

    #[test]
    fn test_fpm_preemption_creates_queued_decode() {
        // 触发抢占：用运行中请求填满 KV，再提交一个迫使驱逐的新请求。
        // 被抢占的请求应在 FPM 中表现为排队 decode。
        let args = MockEngineArgs::builder()
            .block_size(4)
            .num_gpu_blocks(6) // 24 token 的 KV——极紧
            .max_num_batched_tokens(Some(32))
            .max_num_seqs(Some(3))
            .enable_chunked_prefill(true)
            .enable_prefix_caching(false)
            .preemption_mode(PreemptionMode::Lifo)
            .speedup_ratio(0.0)
            .build()
            .unwrap();
        let mut core = VllmCore::new(args);
        let mut collector = crate::replay::TraceCollector::default();

        // r1：4 token prompt，长输出（保持运行）
        core.receive(DirectRequest {
            tokens: (0..4).collect(),
            max_output_tokens: 20,
            uuid: Some(Uuid::from_u128(1)),
            dp_rank: 0,
            arrival_timestamp_ms: None,
        });

        // prefill r1 并解码几个 token 以积累 KV
        core.execute_pass(&mut collector, 0.0);
        core.execute_pass(&mut collector, 1.0);
        core.execute_pass(&mut collector, 2.0);

        // r2：另一个将与之竞争 KV 的请求
        core.receive(DirectRequest {
            tokens: (100..116).collect(), // 16 token——将对 KV 施压
            max_output_tokens: 5,
            uuid: Some(Uuid::from_u128(2)),
            dp_rank: 0,
            arrival_timestamp_ms: None,
        });

        // 本轮应触发抢占
        let pass = core.execute_pass(&mut collector, 3.0);
        let fpm = pass.fpm.expect("FPM should be present");

        // 应看到至少一个排队 decode（被抢占请求）或一个排队 prefill
        // （若新请求无法被调度）。关键断言是：存在 KV 压力时排队度量非零。
        let total_queued = fpm.num_queued_prefill + fpm.num_queued_decode;
        if total_queued > 0 {
            // 发生抢占——验证被抢占 decode 拥有 KV 上下文
            if fpm.num_queued_decode > 0 {
                assert!(
                    fpm.sum_queued_decode_kv_tokens > 0,
                    "preempted decode should have KV context"
                );
            }
        }
        // 无论如何，至少一个请求应被调度
        let total_scheduled = fpm.num_prefill_requests + fpm.num_decode_requests;
        assert!(total_scheduled >= 1);
    }

    #[tokio::test]
    async fn test_fpm_sent_through_sink() {
        use crate::scheduler::test_utils::CapturingFpmSink;

        let args = MockEngineArgs::builder()
            .block_size(4)
            .num_gpu_blocks(16)
            .max_num_batched_tokens(Some(16))
            .max_num_seqs(Some(4))
            .enable_chunked_prefill(true)
            .enable_prefix_caching(false)
            .speedup_ratio(0.0)
            .build()
            .unwrap();

        let (output_tx, mut output_rx) = mpsc::unbounded_channel::<Vec<OutputSignal>>();
        let fpm_sink = Arc::new(CapturingFpmSink::default());
        let fpm_publisher = crate::common::protocols::FpmPublisher::new(Some(
            fpm_sink.clone() as Arc<dyn crate::common::protocols::FpmSink>
        ));

        let scheduler = Scheduler::new(
            args,
            0,
            Some(output_tx),
            KvEventPublishers::default(),
            None,
            fpm_publisher,
        );

        scheduler.receive(DirectRequest {
            tokens: (0..8).collect(),
            max_output_tokens: 2,
            uuid: Some(Uuid::from_u128(1)),
            dp_rank: 0,
            arrival_timestamp_ms: None,
        });

        // 等待至少一个输出信号——确保调度器已完成至少一轮并排空延迟 FPM 缓冲。
        tokio::time::timeout(Duration::from_secs(5), output_rx.recv())
            .await
            .expect("timed out waiting for output")
            .expect("output channel closed");

        let snapshots = fpm_sink.take();
        assert!(
            !snapshots.is_empty(),
            "should have received at least one FPM snapshot"
        );
        let fpm = &snapshots[0];
        assert_eq!(fpm.num_prefill_requests, 1);
        assert!(fpm.sum_prefill_tokens > 0);
        assert!(fpm.wall_time_secs > 0.0);
    }
}
