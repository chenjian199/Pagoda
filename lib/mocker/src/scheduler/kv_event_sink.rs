// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-License-Identifier: Apache-2.0
//
// API-compatible implementation based on the public interfaces and behavioral
// contracts of NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
// Implementation rewritten by PAGODA.

//! # 调度器侧的 KV 事件缓冲/发布
//!
//! ## 设计意图
//! 为离线重放、调度器测试与 live 调度器提供「捕获 / 延迟转发」KV 事件与 FPM 快照的工具，
//! 使调用方能控制事件在前向轮的哪个阶段对外可见。
//!
//! ## 外部契约
//! - 对 crate 内导出的缓冲类型、捕获/发布函数（`capture_router_event_sink`、
//!   `capture_deferred_kv_publish_sink`、`publish_deferred_kv_events`、`publish_deferred_fpm`）
//!   及其签名保持稳定；警告日志文案保持原文。
//!
//! ## 实现要点
//! - router 捕获路径立即将原始 KV 事件转为 `RouterEvent`（只需 worker 标记，不需 token-id）；
//! - 延迟发布路径保留 `block_token_ids`，以支持 ZMQ 等需要原始 token-id 的 sink。

use std::sync::{Arc, Mutex};

use anyhow::Result;
use pagoda_kv_router::protocols::{KvCacheEvent, RouterEvent, StorageTier, WorkerId};

use crate::common::protocols::{
    ForwardPassSnapshot, FpmPublisher, KvCacheEventSink, KvEventPublishers, RawKvEvent,
    RawKvEventSink,
};

// === SECTION: router 事件捕获 ===

/// 为离线重放与调度器测试捕获 router 就绪事件。
///
/// 此路径立即把原始 KV 事件转为 `RouterEvent`，因为调用方只需带 worker 标记的 router 事件，
/// 而非 live 发布路径所用的原始 token-id 载荷。
#[derive(Clone, Default)]
pub(crate) struct CapturedRouterEventBuffer {
    events: Arc<Mutex<Vec<RouterEvent>>>,
}

impl CapturedRouterEventBuffer {
    pub(crate) fn push(&self, event: RouterEvent) {
        self.events.lock().unwrap().push(event);
    }

    pub(crate) fn drain(&self) -> Vec<RouterEvent> {
        std::mem::take(&mut *self.events.lock().unwrap())
    }
}

/// 将 `RouterEvent` 记入 `CapturedRouterEventBuffer` 的 sink 实现。
#[derive(Clone)]
struct RouterEventCaptureSink {
    worker_id: WorkerId,
    buffer: CapturedRouterEventBuffer,
}

impl KvCacheEventSink for RouterEventCaptureSink {
    fn publish(&self, event: KvCacheEvent) -> Result<()> {
        self.buffer.push(RouterEvent::new(self.worker_id, event));
        Ok(())
    }

    fn publish_with_storage_tier(
        &self,
        event: KvCacheEvent,
        storage_tier: StorageTier,
    ) -> Result<()> {
        self.buffer.push(RouterEvent::with_storage_tier(
            self.worker_id,
            event,
            storage_tier,
        ));
        Ok(())
    }
}

/// 返回捕获缓冲与可传入调度器核心（用于离线重放或测试）的 sink 句柄。
pub(crate) fn capture_router_event_sink(
    worker_id: WorkerId,
) -> (CapturedRouterEventBuffer, Arc<dyn KvCacheEventSink>) {
    let buffer = CapturedRouterEventBuffer::default();
    let sink: Arc<dyn KvCacheEventSink> = Arc::new(RouterEventCaptureSink {
        worker_id,
        buffer: buffer.clone(),
    });
    (buffer, sink)
}

// === SECTION: 延迟 KV 发布 ===

/// live 调度器缓冲的原始 KV 事件载荷，以便在正确的轮次阶段转发给真实 sink。
#[derive(Debug, Clone)]
pub(crate) struct DeferredKvPublish {
    pub(crate) event: KvCacheEvent,
    pub(crate) block_token_ids: Option<Vec<Vec<u32>>>,
    pub(crate) storage_tier: StorageTier,
}

/// 为 live `python -m dynamo.mocker` 与在线重放路径捕获原始 KV 发布。
///
/// 与 `CapturedRouterEventBuffer` 不同，此处保留 `block_token_ids`，使延迟转发
/// 对 ZMQ 等需要原始 token-id 载荷的 sink 仍然有效。
#[derive(Clone, Default)]
pub(crate) struct DeferredKvPublishBuffer {
    events: Arc<Mutex<Vec<DeferredKvPublish>>>,
}

impl DeferredKvPublishBuffer {
    pub(crate) fn push(
        &self,
        event: KvCacheEvent,
        block_token_ids: Option<Vec<Vec<u32>>>,
        storage_tier: StorageTier,
    ) {
        self.events.lock().unwrap().push(DeferredKvPublish {
            event,
            block_token_ids,
            storage_tier,
        });
    }

    pub(crate) fn drain(&self) -> Vec<DeferredKvPublish> {
        std::mem::take(&mut *self.events.lock().unwrap())
    }
}

/// 将原始 KV 发布记入 `DeferredKvPublishBuffer` 而非立即转发的 sink 实现。
#[derive(Clone, Default)]
struct DeferredKvEventSink {
    buffer: DeferredKvPublishBuffer,
}

impl KvCacheEventSink for DeferredKvEventSink {
    fn publish(&self, event: KvCacheEvent) -> Result<()> {
        self.buffer.push(event, None, StorageTier::Device);
        Ok(())
    }

    fn publish_with_storage_tier(
        &self,
        event: KvCacheEvent,
        storage_tier: StorageTier,
    ) -> Result<()> {
        self.buffer.push(event, None, storage_tier);
        Ok(())
    }
}

#[derive(Clone, Default)]
struct DeferredRawKvEventSink {
    buffer: DeferredKvPublishBuffer,
}

impl RawKvEventSink for DeferredRawKvEventSink {
    fn publish(&self, event: RawKvEvent) -> Result<()> {
        let mut events = self.buffer.events.lock().unwrap();
        // 与上一条同 id/dp_rank/tier 时，仅更新 token-id 载荷，避免重复入队。
        if let Some(last) = events.last_mut()
            && last.event.event_id == event.event.event_id
            && last.event.dp_rank == event.event.dp_rank
            && last.storage_tier == event.storage_tier
        {
            last.block_token_ids = event.block_token_ids;
            return Ok(());
        }

        events.push(DeferredKvPublish {
            event: event.event,
            block_token_ids: event.block_token_ids,
            storage_tier: event.storage_tier,
        });
        Ok(())
    }
}

/// 返回延迟发布缓冲与可传入 live 调度器核心的 sink 句柄；
/// `live.rs` 保留对缓冲事件何时转发给真实 sink 的控制权。
pub(crate) fn capture_deferred_kv_publish_sink(
    capture_raw: bool,
) -> (DeferredKvPublishBuffer, KvEventPublishers) {
    let buffer = DeferredKvPublishBuffer::default();
    let event_sink: Arc<dyn KvCacheEventSink> = Arc::new(DeferredKvEventSink {
        buffer: buffer.clone(),
    });
    let raw_sink = capture_raw.then(|| {
        Arc::new(DeferredRawKvEventSink {
            buffer: buffer.clone(),
        }) as Arc<dyn RawKvEventSink>
    });
    (buffer, KvEventPublishers::new(Some(event_sink), raw_sink))
}

/// 在前向轮到达配置的可见点后，把缓冲的 live 调度器 KV 事件转发给真实 sink。
pub(crate) fn publish_deferred_kv_events(
    sinks: &KvEventPublishers,
    events: Vec<DeferredKvPublish>,
) {
    for event in events {
        if let Err(error) = sinks.publish_with_storage_tier(
            event.event,
            event.block_token_ids.as_deref(),
            event.storage_tier,
        ) {
            tracing::warn!("Failed to forward buffered KV event: {error}");
        }
    }
}

// === SECTION: 延迟 FPM 快照 ===

/// 为 live 调度器捕获 FPM 快照，使其能在正确的轮次阶段 flush，与延迟 KV 事件模式一致。
#[derive(Clone, Default)]
pub(crate) struct DeferredFpmBuffer {
    snapshots: Arc<Mutex<Vec<ForwardPassSnapshot>>>,
}

impl DeferredFpmBuffer {
    pub(crate) fn push(&self, snapshot: ForwardPassSnapshot) {
        self.snapshots.lock().unwrap().push(snapshot);
    }

    pub(crate) fn drain(&self) -> Vec<ForwardPassSnapshot> {
        std::mem::take(&mut *self.snapshots.lock().unwrap())
    }
}

/// 在前向轮到达配置的可见点后，把缓冲的 FPM 快照转发给真实 sink。
pub(crate) fn publish_deferred_fpm(sink: &FpmPublisher, snapshots: Vec<ForwardPassSnapshot>) {
    for snapshot in snapshots {
        if let Err(error) = sink.publish(snapshot) {
            tracing::warn!("Failed to forward buffered FPM snapshot: {error}");
        }
    }
}
