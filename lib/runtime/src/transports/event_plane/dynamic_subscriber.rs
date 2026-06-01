// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 动态订阅器：跟随 discovery 自动连接 / 断开 ZMQ publisher
//!
//! ## 设计意图
//! 静态 pub/sub 在分布式场景下不够用 —— publisher 实例会**随时上下线**。本组件
//! 监听 [`Discovery`] 上的事件流，按"Added/Removed"动作动态维护"对每个 publisher
//! 的 ZMQ SUB 连接"：
//!
//! - **Added**：发现新 publisher → 拿到它的 ZMQ portname → 启动一条 `consume_endpoint_stream`
//!   后台任务把 SUB 流的字节灌进汇聚通道。
//! - **Removed**：publisher 下线 → 触发该 portname 的 cancel token → 后台任务退出 → 删表项。
//!
//! 所有 portname 流汇聚到**单个 mpsc 通道**，对外暴露为一个 [`WireStream`]。
//!
//! ## 外部契约
//! - [`DynamicSubscriber::new(discovery, query, topic)`]
//! - [`DynamicSubscriber::start_zmq(self: Arc<Self>) -> Result<WireStream>`]
//! - [`DynamicSubscriber::cancel(&self)`]
//! - `Drop` 触发 cancel —— "丢一定停"。
//!
//! ## 实现要点
//! 差异：
//! 1. **存储结构**：active portname 表从 `RwLock<HashMap<String, (String, CancellationToken)>>`
//!    换成 [`DashMap`]，去掉读写锁热路径。
//! 2. **代码切分**：lib-copy 把整个 watch loop 塞进 `start_zmq` 一个超长 async
//!    block 里；本实现拆成 [`Self::watch_loop`] / [`Self::handle_added`] /
//!    [`Self::handle_removed`] / [`Self::cancel_all_portnames`] 四个小方法，
//!    每个都不超过一屏 —— 测试和阅读都更友好。
//! 3. **取消时清理**：lib-copy 在 watch loop 退出时**逐个 cancel** 然后让后台
//!    任务自清理；本实现额外在 `cancel_all_portnames` 里 `clear()` 表，避免
//!    被 cancel 后表项还短暂残留。
//! 4. **错误传播**：consume 内部把 `Some(Err)` 视为致命退出 —— 与 lib-copy 一致；
//!    但日志改为带 portname 字段，便于排查。

use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use futures::stream::StreamExt;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::transport::{EventTransportRx, WireStream};
use super::zmq_transport::ZmqSubTransport;
use crate::discovery::{
    Discovery, DiscoveryEvent, DiscoveryInstance, DiscoveryQuery, EventTransport,
};

// =============================================================================
// === DynamicSubscriber =======================================================
// =============================================================================

/// 一个 portname 表项：portname 字符串 + 其取消令牌。
type PortNameSlot = (String, CancellationToken);

/// instance_id (作为 String) → portname slot 的活跃表。
type ActivePortNames = Arc<DashMap<String, PortNameSlot>>;

pub struct DynamicSubscriber {
    discovery: Arc<dyn Discovery>,
    query: DiscoveryQuery,
    topic: String,
    cancel_token: CancellationToken,
}

impl DynamicSubscriber {
    pub fn new(discovery: Arc<dyn Discovery>, query: DiscoveryQuery, topic: String) -> Self {
        Self {
            discovery,
            query,
            topic,
            cancel_token: CancellationToken::new(),
        }
    }

    // ---------------------------------------------------------------------
    // === 启动入口 =========================================================
    // ---------------------------------------------------------------------

    /// 启动 discovery watch + ZMQ 动态订阅，返回字节汇聚流。
    pub async fn start_zmq(self: Arc<Self>) -> Result<WireStream> {
        let (event_tx, event_rx) = mpsc::unbounded_channel::<Bytes>();
        let active: ActivePortNames = Arc::new(DashMap::new());

        // 启动后台 watch 任务
        let driver_self = Arc::clone(&self);
        tokio::spawn(Self::watch_loop(driver_self, active, event_tx));

        // 把"持有 self 的 Arc"嵌进 stream，确保订阅器存活期间不被释放
        let keep_alive = Arc::clone(&self);
        let stream = async_stream::stream! {
            let _hold = keep_alive;
            let mut rx = event_rx;
            while let Some(bytes) = rx.recv().await {
                yield Ok(bytes);
            }
        };

        Ok(Box::pin(stream))
    }

    // ---------------------------------------------------------------------
    // === 主循环 ===========================================================
    // ---------------------------------------------------------------------

    /// 监听 discovery 事件流并按事件分派。退出条件：cancel / discovery 出错 / 流结束。
    async fn watch_loop(
        this: Arc<Self>,
        active: ActivePortNames,
        event_tx: mpsc::UnboundedSender<Bytes>,
    ) {
        let query = this.query.clone();
        let cancel = this.cancel_token.clone();
        let zmq_topic = this.topic.clone();

        tracing::debug!(
            ?query,
            cancel_cancelled = cancel.is_cancelled(),
            "Attempting to start discovery watch"
        );

        let mut watch_stream = match this.discovery.list_and_watch(query.clone(), None).await {
            Ok(s) => {
                tracing::debug!("Successfully obtained discovery watch stream");
                s
            }
            Err(e) => {
                tracing::error!(error = %e, "Failed to start discovery watch");
                return;
            }
        };

        tracing::info!(?query, "Started dynamic discovery watch for ZMQ publishers");

        while let Some(event_result) = watch_stream.next().await {
            tracing::debug!("Received discovery event: {:?}", event_result);
            if cancel.is_cancelled() {
                tracing::info!("Dynamic subscriber cancelled, stopping watch");
                break;
            }

            match event_result {
                Ok(DiscoveryEvent::Added(instance)) => {
                    Self::handle_added(&active, &event_tx, &zmq_topic, instance);
                }
                Ok(DiscoveryEvent::Removed(id)) => {
                    Self::handle_removed(&active, id.instance_id().to_string());
                }
                Err(e) => {
                    tracing::error!(error = %e, "Discovery watch error");
                    break;
                }
            }
        }

        Self::cancel_all_portnames(&active);
        tracing::info!("Discovery watch stream ended");
    }

    fn handle_added(
        active: &ActivePortNames,
        event_tx: &mpsc::UnboundedSender<Bytes>,
        zmq_topic: &str,
        instance: DiscoveryInstance,
    ) {
        tracing::info!(instance = ?instance, "Discovery Added event received");
        let instance_id = instance.instance_id().to_string();

        let Some(portname) = Self::extract_zmq_endpoint(&instance) else {
            tracing::warn!(
                instance = ?instance,
                "Discovery Added event did not contain a ZMQ portname"
            );
            return;
        };

        // 已连接的同 instance_id 直接跳过
        if active.contains_key(&instance_id) {
            tracing::debug!(
                portname = %portname,
                instance_id = %instance_id,
                "Already connected to ZMQ publisher"
            );
            return;
        }

        tracing::info!(
            portname = %portname,
            instance_id = %instance_id,
            "Connecting to new ZMQ publisher"
        );

        let portname_cancel = CancellationToken::new();
        active.insert(instance_id.clone(), (portname.clone(), portname_cancel.clone()));

        let event_tx = event_tx.clone();
        let zmq_topic = zmq_topic.to_string();
        let portname_for_task = portname.clone();
        let active_for_cleanup = Arc::clone(active);
        let id_for_cleanup = instance_id.clone();

        tokio::spawn(async move {
            if let Err(e) = Self::consume_endpoint_stream(
                &portname_for_task,
                &zmq_topic,
                event_tx,
                portname_cancel,
            )
            .await
            {
                tracing::warn!(
                    portname = %portname_for_task,
                    error = %e,
                    "Error consuming ZMQ portname stream"
                );
            }
            // 退出后从活跃表里摘除
            active_for_cleanup.remove(&id_for_cleanup);
        });
    }

    fn handle_removed(active: &ActivePortNames, id_str: String) {
        tracing::info!(
            instance_id = %id_str,
            "ZMQ publisher removed from discovery, cancelling portname stream"
        );
        match active.remove(&id_str) {
            Some((_, (_portname, cancel))) => {
                cancel.cancel();
                tracing::info!(instance_id = %id_str, "Cancelled portname stream");
            }
            None => {
                tracing::warn!(
                    instance_id = %id_str,
                    "No active portname found for removed stream instance"
                );
            }
        }
    }

    fn cancel_all_portnames(active: &ActivePortNames) {
        for entry in active.iter() {
            entry.value().1.cancel();
        }
        active.clear();
    }

    // ---------------------------------------------------------------------
    // === 单 portname 消费 ================================================
    // ---------------------------------------------------------------------

    /// 仅供外部测试可见：从 discovery instance 里抽 ZMQ portname。
    fn extract_zmq_endpoint(instance: &DiscoveryInstance) -> Option<String> {
        if let DiscoveryInstance::EventChannel { transport, .. } = instance
            && let EventTransport::Zmq { portname } = transport
        {
            return Some(portname.clone());
        }
        None
    }

    /// 把单个 portname 的 SUB 流抽进汇聚 channel。
    async fn consume_endpoint_stream(
        portname: &str,
        zmq_topic: &str,
        event_tx: mpsc::UnboundedSender<Bytes>,
        cancel_token: CancellationToken,
    ) -> Result<()> {
        let sub_transport = ZmqSubTransport::connect(portname, zmq_topic).await?;
        let mut stream = sub_transport.subscribe(zmq_topic).await?;

        tracing::info!(
            portname = %portname,
            topic = %zmq_topic,
            "Started consuming ZMQ portname stream"
        );

        loop {
            let next_event = tokio::select! {
                _ = cancel_token.cancelled() => {
                    tracing::info!(portname = %portname, "PortName stream cancelled");
                    break;
                }
                e = stream.next() => e,
            };

            match next_event {
                Some(Ok(bytes)) => {
                    if event_tx.send(bytes).is_err() {
                        tracing::warn!(
                            portname = %portname,
                            "Event channel closed, stopping portname stream"
                        );
                        break;
                    }
                }
                Some(Err(error)) => {
                    tracing::error!(
                        portname = %portname,
                        error = %error,
                        "Error receiving from ZMQ portname"
                    );
                    break;
                }
                None => {
                    tracing::info!(portname = %portname, "ZMQ portname stream ended");
                    break;
                }
            }
        }

        Ok(())
    }

    // ---------------------------------------------------------------------
    // === 取消 ============================================================
    // ---------------------------------------------------------------------

    /// 显式取消订阅器。后台 watch 与所有 portname 任务都会观察到该信号并退出。
    pub fn cancel(&self) {
        self.cancel_token.cancel();
    }
}

impl Drop for DynamicSubscriber {
    fn drop(&mut self) {
        // 兜底取消：即使调用方忘了 cancel，对象释放也会停掉后台任务。
        self.cancel_token.cancel();
    }
}

// =============================================================================
// === 单元测试（保留 lib-copy 补充集，标注新增项）============================
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discovery::{
        DiscoverySpec, EventChannelQuery, MockDiscovery, SharedMockRegistry,
    };
    use crate::transports::event_plane::transport::EventTransportTx;
    use crate::transports::event_plane::{
        EventEnvelope, MsgpackCodec, WireStream, ZmqPubTransport,
    };
    use bytes::Bytes;
    use futures::StreamExt;
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tokio::time::{Duration, sleep, timeout};
    use tokio_util::sync::CancellationToken;

    const NAMESPACE: &str = "supplemental-ns";
    const COMPONENT: &str = "supplemental-servicegroup";
    const TOPIC: &str = "supplemental-topic";

    fn discovery_query() -> DiscoveryQuery {
        DiscoveryQuery::EventChannels(EventChannelQuery::topic(NAMESPACE, COMPONENT, TOPIC))
    }

    fn event_channel_instance(instance_id: u64, transport: EventTransport) -> DiscoveryInstance {
        DiscoveryInstance::EventChannel {
            namespace: NAMESPACE.to_string(),
            servicegroup: COMPONENT.to_string(),
            topic: TOPIC.to_string(),
            instance_id,
            transport,
        }
    }

    async fn bind_test_publisher(topic: &str) -> anyhow::Result<(ZmqPubTransport, String)> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        drop(listener);
        let portname = format!("tcp://127.0.0.1:{port}");
        ZmqPubTransport::bind(&portname, topic).await
    }

    fn encode_envelope(topic: &str, sequence: u64, payload: &[u8]) -> Bytes {
        let envelope = EventEnvelope {
            publisher_id: 17,
            sequence,
            published_at: 1_700_000_000_000,
            topic: topic.to_string(),
            payload: Bytes::copy_from_slice(payload),
        };
        MsgpackCodec
            .encode_envelope(&envelope)
            .expect("envelope encoding should succeed")
    }

    async fn publish_envelope(
        publisher: &ZmqPubTransport,
        topic: &str,
        sequence: u64,
        payload: &[u8],
    ) {
        let encoded = encode_envelope(topic, sequence, payload);
        publisher
            .publish(topic, encoded)
            .await
            .expect("publishing test envelope should succeed");
    }

    async fn expect_no_more_messages(stream: &mut WireStream) {
        match timeout(Duration::from_millis(500), stream.next()).await {
            Ok(Some(Ok(bytes))) => panic!("unexpected extra message: {bytes:?}"),
            Ok(Some(Err(error))) => panic!("unexpected stream error: {error}"),
            Ok(None) | Err(_) => {}
        }
    }

    /// ## 测试过程
    /// 构造 subscriber，断言初始字段、cancel_token 未触发。
    /// ## 意义
    /// 锁定构造器副作用 = 0。
    #[test]
    fn new_initializes_fields_and_cancel_token() {
        let registry = SharedMockRegistry::new();
        let discovery: Arc<dyn Discovery> = Arc::new(MockDiscovery::new(Some(41), registry));
        let query = discovery_query();
        let subscriber =
            DynamicSubscriber::new(discovery.clone(), query.clone(), TOPIC.to_string());
        assert_eq!(subscriber.topic, TOPIC);
        assert_eq!(subscriber.query, query);
        assert_eq!(subscriber.discovery.instance_id(), 41);
        assert!(!subscriber.cancel_token.is_cancelled());
    }

    /// ## 测试过程
    /// 调 cancel 后内部 token 被触发。
    /// ## 意义
    /// 锁定 cancel API 的语义。
    #[test]
    fn cancel_sets_internal_token() {
        let registry = SharedMockRegistry::new();
        let discovery: Arc<dyn Discovery> = Arc::new(MockDiscovery::new(Some(7), registry));
        let subscriber = DynamicSubscriber::new(discovery, discovery_query(), TOPIC.to_string());
        let token = subscriber.cancel_token.clone();
        subscriber.cancel();
        assert!(token.is_cancelled());
    }

    /// ## 测试过程
    /// drop subscriber 后内部 token 被触发。
    /// ## 意义
    /// 锁定 Drop 兜底契约。
    #[test]
    fn drop_sets_internal_token() {
        let registry = SharedMockRegistry::new();
        let discovery: Arc<dyn Discovery> = Arc::new(MockDiscovery::new(Some(8), registry));
        let subscriber = DynamicSubscriber::new(discovery, discovery_query(), TOPIC.to_string());
        let token = subscriber.cancel_token.clone();
        drop(subscriber);
        assert!(token.is_cancelled());
    }

    /// ## 测试过程
    /// 用 Zmq / Nats / ZmqBroker 三种 EventTransport 调 extract_zmq_endpoint。
    /// ## 意义
    /// 锁定"仅识别直连 ZMQ"的过滤规则。
    #[test]
    fn extract_zmq_endpoint_only_accepts_direct_zmq_transport() {
        let zmq_instance =
            event_channel_instance(1, EventTransport::zmq("tcp://127.0.0.1:23456"));
        assert_eq!(
            DynamicSubscriber::extract_zmq_endpoint(&zmq_instance),
            Some("tcp://127.0.0.1:23456".to_string())
        );

        let nats_instance = event_channel_instance(2, EventTransport::nats("test-subject"));
        assert_eq!(
            DynamicSubscriber::extract_zmq_endpoint(&nats_instance),
            None
        );

        let broker_instance = event_channel_instance(
            3,
            EventTransport::ZmqBroker {
                xsub_endpoints: vec!["tcp://127.0.0.1:11111".to_string()],
                xpub_endpoints: vec!["tcp://127.0.0.1:22222".to_string()],
            },
        );
        assert_eq!(
            DynamicSubscriber::extract_zmq_endpoint(&broker_instance),
            None
        );
    }

    /// ## 测试过程
    /// 启 consume_endpoint_stream 后台任务，publish 一条消息，期望从 mpsc 收到；
    /// 然后 cancel，期望任务正常退出。
    /// ## 意义
    /// 锁定单 portname 消费循环的"转发 + 取消"行为。
    #[tokio::test(flavor = "multi_thread")]
    async fn consume_endpoint_stream_forwards_bytes_until_cancelled() {
        let (publisher, portname) = bind_test_publisher(TOPIC)
            .await
            .expect("publisher should bind on localhost");
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        let cancel_token = CancellationToken::new();
        let task_cancel = cancel_token.clone();

        let task = tokio::spawn(async move {
            DynamicSubscriber::consume_endpoint_stream(&portname, TOPIC, event_tx, task_cancel)
                .await
        });

        sleep(Duration::from_millis(150)).await;
        publish_envelope(&publisher, TOPIC, 1, b"first").await;

        let received = timeout(Duration::from_secs(5), event_rx.recv())
            .await
            .expect("should receive forwarded bytes")
            .expect("channel should stay open for the first message");
        assert_eq!(received, encode_envelope(TOPIC, 1, b"first"));

        cancel_token.cancel();
        let join_result = timeout(Duration::from_secs(5), task)
            .await
            .expect("consume task should finish after cancellation");
        join_result
            .expect("consume task should not panic")
            .expect("consume should return Ok");
    }

    /// ## 测试过程
    /// 提前 drop event_rx，再 publish。consume 应在尝试发送时观察到通道关闭并退出。
    /// ## 意义
    /// 锁定下游断开时的优雅退出契约。
    #[tokio::test(flavor = "multi_thread")]
    async fn consume_endpoint_stream_stops_when_receiver_is_closed() {
        let (publisher, portname) = bind_test_publisher(TOPIC)
            .await
            .expect("publisher should bind on localhost");
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        drop(event_rx);
        let cancel_token = CancellationToken::new();

        let task = tokio::spawn(async move {
            DynamicSubscriber::consume_endpoint_stream(&portname, TOPIC, event_tx, cancel_token)
                .await
        });

        sleep(Duration::from_millis(150)).await;
        publish_envelope(&publisher, TOPIC, 2, b"dropped").await;

        let join_result = timeout(Duration::from_secs(5), task)
            .await
            .expect("consume task should stop once the receiver is closed");
        join_result
            .expect("consume task should not panic")
            .expect("consume should return Ok");
    }

    /// ## 测试过程
    /// 注册 publisher → 期望流里出现第一帧；注销 → 之后再 publish 不应被流接收。
    /// ## 意义
    /// 锁定"Added 即连、Removed 即停"的核心动态行为。
    #[tokio::test(flavor = "multi_thread")]
    async fn start_zmq_forwards_added_endpoints_and_stops_after_removed_events() {
        let registry = SharedMockRegistry::new();
        let discovery: Arc<dyn Discovery> =
            Arc::new(MockDiscovery::new(Some(11), registry.clone()));
        let subscriber = Arc::new(DynamicSubscriber::new(
            discovery.clone(),
            discovery_query(),
            TOPIC.to_string(),
        ));

        let (publisher, portname) = bind_test_publisher(TOPIC)
            .await
            .expect("publisher should bind on localhost");

        let mut stream = Arc::clone(&subscriber)
            .start_zmq()
            .await
            .expect("start_zmq should return a stream");

        let instance = discovery
            .register(DiscoverySpec::EventChannel {
                namespace: NAMESPACE.to_string(),
                servicegroup: COMPONENT.to_string(),
                topic: TOPIC.to_string(),
                transport: EventTransport::zmq(portname.clone()),
            })
            .await
            .expect("registering the event channel should succeed");

        sleep(Duration::from_millis(250)).await;
        publish_envelope(&publisher, TOPIC, 1, b"first").await;

        let first_message = timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("subscriber should forward the first message")
            .expect("stream should stay open for the first message")
            .expect("stream should not produce an error");
        assert_eq!(first_message, encode_envelope(TOPIC, 1, b"first"));

        discovery
            .unregister(instance)
            .await
            .expect("unregistering the event channel should succeed");

        sleep(Duration::from_millis(250)).await;
        publish_envelope(&publisher, TOPIC, 2, b"second").await;

        expect_no_more_messages(&mut stream).await;
    }

    /// ## 测试过程
    /// 在注册 publisher 之前先 cancel；之后即使消息发出来，流也应不再产出。
    /// ## 意义
    /// 锁定"先取消、再注册"场景下的不投递契约。
    #[tokio::test(flavor = "multi_thread")]
    async fn start_zmq_respects_cancellation_before_delivery() {
        let registry = SharedMockRegistry::new();
        let discovery: Arc<dyn Discovery> =
            Arc::new(MockDiscovery::new(Some(12), registry.clone()));
        let subscriber = Arc::new(DynamicSubscriber::new(
            discovery.clone(),
            discovery_query(),
            TOPIC.to_string(),
        ));

        let (publisher, portname) = bind_test_publisher(TOPIC)
            .await
            .expect("publisher should bind on localhost");

        let mut stream = Arc::clone(&subscriber)
            .start_zmq()
            .await
            .expect("start_zmq should return a stream");

        subscriber.cancel();

        let instance = discovery
            .register(DiscoverySpec::EventChannel {
                namespace: NAMESPACE.to_string(),
                servicegroup: COMPONENT.to_string(),
                topic: TOPIC.to_string(),
                transport: EventTransport::zmq(portname.clone()),
            })
            .await
            .expect("registering the event channel should succeed");

        sleep(Duration::from_millis(250)).await;
        publish_envelope(&publisher, TOPIC, 1, b"cancelled").await;

        expect_no_more_messages(&mut stream).await;

        discovery
            .unregister(instance)
            .await
            .expect("cleanup unregister should succeed");
    }
}
