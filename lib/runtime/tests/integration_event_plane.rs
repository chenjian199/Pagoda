// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

mod common;

mod zmq {
    use std::time::Duration;

    use anyhow::{Result, anyhow};
    use dynamo_runtime::{
        discovery::EventTransportKind,
        transports::event_plane::{EventPublisher, EventSubscriber, MsgpackCodec},
    };
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    use super::common::contract::{acquire_contract_test_lock, process_local_runtime, unique_name};

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct ProbeEvent {
        message: String,
    }

    // 目的/场景：ZMQ event plane 经 discovery 直连 publish/subscribe 往返。
    //
    // 生产逻辑：`EventPublisher`/`EventSubscriber` + `DynamicSubscriber` + Msgpack codec
    //（`transports/event_plane/mod.rs`）。
    //
    // 测试计划：subscriber 先启动 → publisher 注册并发事件 → 收 envelope。
    //
    // 关键断言：topic 匹配；payload 反序列化等于发送值。
    #[tokio::test(flavor = "multi_thread")]
    async fn zmq_publish_subscribe_roundtrip() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let (_rt, drt) = process_local_runtime().await?;
        let component = drt
            .namespace(unique_name("zmq-roundtrip"))?
            .component("backend")?;
        const TOPIC: &str = "metrics";

        let mut subscriber = EventSubscriber::for_component_with_transport(
            &component,
            TOPIC,
            EventTransportKind::Zmq,
        )
        .await?;
        tokio::time::sleep(Duration::from_millis(150)).await;

        let publisher = EventPublisher::for_component_with_transport(
            &component,
            TOPIC,
            EventTransportKind::Zmq,
        )
        .await?;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let event = ProbeEvent {
            message: "zmq-contract-payload".to_string(),
        };
        publisher.publish(&event).await?;

        let envelope = tokio::time::timeout(Duration::from_secs(5), subscriber.next())
            .await
            .map_err(|_| anyhow!("timed out waiting for ZMQ event"))?
            .ok_or_else(|| anyhow!("ZMQ subscriber stream ended"))??;

        assert_eq!(envelope.topic, TOPIC);
        let decoded: ProbeEvent = MsgpackCodec.decode_payload(&envelope.payload)?;
        assert_eq!(decoded, event);

        Ok(())
    }

    // 目的/场景：ZMQ dynamic subscriber 按 topic 过滤，不接收邻域 channel。
    //
    // 生产逻辑：`EventSubscriber` 在 envelope decode 后按 `topic_filter` 丢弃非目标 topic；
    // `DynamicSubscriber` 仅连接 discovery 中匹配的 publisher（`dynamic_subscriber.rs`）。
    //
    // 测试计划：订阅 `alpha` → 发布 `alpha` 与 `beta` → 仅收到 `alpha`。
    //
    // 关键断言：收到的 topic 列表仅为 `["alpha"]`。
    #[tokio::test(flavor = "multi_thread")]
    async fn zmq_dynamic_subscriber_filters_channel() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let (_rt, drt) = process_local_runtime().await?;
        let component = drt
            .namespace(unique_name("zmq-filter"))?
            .component("backend")?;

        let mut subscriber = EventSubscriber::for_component_with_transport(
            &component,
            "alpha",
            EventTransportKind::Zmq,
        )
        .await?;
        tokio::time::sleep(Duration::from_millis(150)).await;

        let pub_alpha = EventPublisher::for_component_with_transport(
            &component,
            "alpha",
            EventTransportKind::Zmq,
        )
        .await?;
        let pub_beta = EventPublisher::for_component_with_transport(
            &component,
            "beta",
            EventTransportKind::Zmq,
        )
        .await?;
        tokio::time::sleep(Duration::from_millis(300)).await;

        pub_alpha.publish(&json!({"channel": "alpha"})).await?;
        pub_beta.publish(&json!({"channel": "beta"})).await?;

        let mut received_topics = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(500), subscriber.next()).await {
                Ok(Some(Ok(envelope))) => received_topics.push(envelope.topic),
                Ok(Some(Err(err))) => return Err(err),
                Ok(None) => break,
                Err(_) if received_topics.is_empty() => continue,
                Err(_) => break,
            }
        }

        assert_eq!(
            received_topics,
            vec!["alpha".to_string()],
            "subscriber should only receive alpha channel events"
        );

        Ok(())
    }
}

mod nats {
    use std::time::Duration;

    use anyhow::{Result, anyhow};
    use dynamo_runtime::{
        discovery::EventTransportKind,
        transports::event_plane::{EventPublisher, EventSubscriber, MsgpackCodec},
    };
    use serde::{Deserialize, Serialize};

    use super::common::contract::{acquire_contract_test_lock, nats_runtime, unique_name};

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct ProbeEvent {
        message: String,
    }

    // 目的/场景：NATS event plane publish/subscribe 与 Msgpack codec 往返。
    //
    // 生产逻辑：`NatsTransport` 经 `kv_router_nats_publish` / `kv_router_nats_subscribe`
    //（`event_plane/nats_transport.rs`）。
    //
    // 测试计划：NATS DRT + `EventTransportKind::Nats` → publish → subscriber 收包。
    //
    // 关键断言：topic 与 payload 匹配。
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires NATS broker (Nightly); set NATS_SERVER and run with --include-ignored"]
    async fn nats_event_transport_roundtrip() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let (_rt, drt) = nats_runtime().await?;
        let component = drt
            .namespace(unique_name("nats-event"))?
            .component("backend")?;
        const TOPIC: &str = "router-events";

        let mut subscriber = EventSubscriber::for_component_with_transport(
            &component,
            TOPIC,
            EventTransportKind::Nats,
        )
        .await?;

        let publisher = EventPublisher::for_component_with_transport(
            &component,
            TOPIC,
            EventTransportKind::Nats,
        )
        .await?;

        let event = ProbeEvent {
            message: "nats-event-contract".to_string(),
        };
        publisher.publish(&event).await?;

        let envelope = tokio::time::timeout(Duration::from_secs(5), subscriber.next())
            .await
            .map_err(|_| anyhow!("timed out waiting for NATS event"))?
            .ok_or_else(|| anyhow!("NATS subscriber stream ended"))??;

        assert_eq!(envelope.topic, TOPIC);
        let decoded: ProbeEvent = MsgpackCodec.decode_payload(&envelope.payload)?;
        assert_eq!(decoded, event);

        Ok(())
    }
}
