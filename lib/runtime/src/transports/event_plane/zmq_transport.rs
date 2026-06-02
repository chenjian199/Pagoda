// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 事件平面：ZMQ PUB/SUB transport
//!
//! ## 设计意图
//! 在事件平面之下，**ZMQ PUB/SUB** 是面向"高吞吐、低延迟、单向广播"的低层
//! 通路。本文件分别封装 [`ZmqPubTransport`]（发布端）与 [`ZmqSubTransport`]
//! （订阅端），并把每条消息打成一个**四帧 multipart**：
//!
//! ```text
//!   Frame 0: topic 字符串          —— 给 ZMQ 自己做 SUB 过滤
//!   Frame 1: publisher_id (u64 BE) —— 让 dedup 不必解 envelope
//!   Frame 2: sequence (u64 BE)     —— 同上
//!   Frame 3: 5 字节 frame 头 + envelope 字节
//! ```
//!
//! 这样订阅侧在"想做 dedup / 计数 / 限流"时不需要先 msgpack 解 envelope，
//! 直接读 Frame 1/2 就够 —— 热路径上能省可观的 CPU。
//!
//! ## 外部契约
//! - [`ZmqPubTransport`]：`bind / connect / connect_multiple / topic`；
//!   `EventTransportTx::publish`；`kind() == Zmq`。
//! - [`ZmqSubTransport`]：`connect / connect_broker / connect_multiple /
//!   connect_broker_multiple`；`EventTransportRx::subscribe`；
//!   `kind() == Zmq`。
//! - 内部字段名 `broadcast_tx` / `_socket_pump_handle` / `socket` / `topic`
//!   以及 `start_socket_pump` / `multipart_message` 仍然可被同 crate 内的
//!   补充测试访问（必须保持不变）。
//!
//! ## 实现要点
//! 差异：
//! 1. **抽取 `decode_pump_message`** —— 把"解析四帧 + 解析 Frame"的 11 个
//!    早返回点从 `start_socket_pump` 的主循环里挪出来，作为 `Result<Bytes,
//!    PumpDecodeError>`；主循环变成"recv → decode → broadcast"三步直叙。
//! 2. **常量 `EXPECTED_FRAME_COUNT = 4`**，告别裸 4。
//! 3. **空 portname 列表错误信息**为固定字面值（"Cannot connect to
//!    zero portnames"）—— 补充测试在断言该字符串，不能改动。
//! 4. publisher 的 `Mutex<Publish>` 保留 —— ZMQ socket 对 send 不是 Sync 安全，
//!    必须串行化。这是 ZMQ 协议本身的约束，无法发散。

use anyhow::{Result, anyhow};
use async_stream::stream;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tmq::{
    AsZmqSocket, Context, Multipart, SocketBuilder,
    publish::{Publish, publish},
    subscribe::{Subscribe, subscribe},
};
use tokio::sync::{Mutex, broadcast};

use super::codec::MsgpackCodec;
use super::frame::Frame;
use super::transport::{EventTransportRx, EventTransportTx, WireStream};
use crate::discovery::EventTransportKind;

// =============================================================================
// === 常量 ====================================================================
// =============================================================================

/// PUB / SUB socket 的高水位线（HWM）—— 默认 ZMQ 仅 1000，对大规模事件流不够。
const ZMQ_SNDHWM: i32 = 100_000;
const ZMQ_RCVHWM: i32 = 100_000;
/// publisher 发送超时（ms）。0 = 立刻失败，背压下不积压。
const ZMQ_SNDTIMEOUT_MS: i32 = 0;
/// subscriber 接收超时（ms）。短超时避免 pump task 永久阻塞在 recv 上。
const ZMQ_RCVTIMEOUT_MS: i32 = 100;
/// 单条消息预期的 multipart frame 数：topic + pub_id + seq + frame。
const EXPECTED_FRAME_COUNT: usize = 4;
/// 本地 broadcast 通道容量。
const BROADCAST_CAPACITY: usize = 1024;

// =============================================================================
// === socket 配置辅助 =========================================================
// =============================================================================

fn configure_publish_builder<T>(builder: SocketBuilder<T>) -> SocketBuilder<T>
where
    T: tmq::FromZmqSocket<T>,
{
    builder
        .set_sndhwm(ZMQ_SNDHWM)
        .set_sndtimeo(ZMQ_SNDTIMEOUT_MS)
}

fn configure_subscribe_builder<T>(builder: SocketBuilder<T>) -> SocketBuilder<T>
where
    T: tmq::FromZmqSocket<T>,
{
    builder
        .set_rcvhwm(ZMQ_RCVHWM)
        .set_rcvtimeo(ZMQ_RCVTIMEOUT_MS)
}

/// 把 `Multipart` 转成 `Vec<Vec<u8>>`，便于按下标随机访问。
fn multipart_message(multipart: Multipart) -> Vec<Vec<u8>> {
    multipart.into_iter().map(|frame| frame.to_vec()).collect()
}

// =============================================================================
// === ZmqPubTransport =========================================================
// =============================================================================

/// ZMQ PUB 端。Clone 不可（持有 socket 锁）；但内部 `Arc<Mutex<_>>` 允许同
/// `Self` 多任务共享。
pub struct ZmqPubTransport {
    socket: Arc<Mutex<Publish>>,
    topic: String,
}

impl ZmqPubTransport {
    /// 在 `portname` 上 bind 一个 PUB socket。
    ///
    /// 若 portname 以 `:0` 结尾，先用 tokio TcpListener 占一个空闲端口、释放、
    /// 再让 ZMQ bind 上去 —— 解决了 ZMQ 没有 "实际绑定端口" API 的痛点。
    /// 返回 `(transport, actual_endpoint)`。
    pub async fn bind(portname: &str, topic: &str) -> Result<(Self, String)> {
        let actual_endpoint = if portname.ends_with(":0") {
            let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await?;
            let port = listener.local_addr()?.port();
            drop(listener);
            format!("tcp://0.0.0.0:{port}")
        } else {
            portname.to_string()
        };

        let ctx = Context::new();
        let socket =
            configure_publish_builder(publish(&ctx)).bind(&actual_endpoint)?;

        tracing::info!(
            portname = %actual_endpoint,
            topic = %topic,
            sndhwm = ZMQ_SNDHWM,
            "ZMQ PUB transport bound with configured HWM"
        );

        Ok((
            Self {
                socket: Arc::new(Mutex::new(socket)),
                topic: topic.to_string(),
            },
            actual_endpoint,
        ))
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// 连接到 broker 的 XSUB 端点（broker 模式发布端）。
    pub async fn connect(xsub_endpoint: &str, topic: &str) -> Result<Self> {
        let ctx = Context::new();
        let socket =
            configure_publish_builder(publish(&ctx)).connect(xsub_endpoint)?;

        tracing::info!(
            portname = %xsub_endpoint,
            topic = %topic,
            sndhwm = ZMQ_SNDHWM,
            "ZMQ PUB transport connected to broker XSUB"
        );

        Ok(Self {
            socket: Arc::new(Mutex::new(socket)),
            topic: topic.to_string(),
        })
    }

    /// 同时连接多个 broker XSUB 端点（HA 模式）。`portnames` 不能为空。
    pub async fn connect_multiple(xsub_endpoints: &[String], topic: &str) -> Result<Self> {
        let mut iter = xsub_endpoints.iter();
        let Some(first) = iter.next() else {
            anyhow::bail!("Cannot connect to zero portnames");
        };

        let ctx = Context::new();
        let socket = configure_publish_builder(publish(&ctx)).connect(first)?;

        for ep in iter {
            socket.get_socket().connect(ep)?;
            tracing::debug!(portname = %ep, "ZMQ PUB connected to broker XSUB");
        }

        tracing::info!(
            num_portnames = xsub_endpoints.len(),
            topic = %topic,
            sndhwm = ZMQ_SNDHWM,
            "ZMQ PUB transport connected to multiple broker XSUBs with configured HWM"
        );

        Ok(Self {
            socket: Arc::new(Mutex::new(socket)),
            topic: topic.to_string(),
        })
    }
}

#[async_trait]
impl EventTransportTx for ZmqPubTransport {
    /// 把 envelope 字节按四帧 multipart 发出。`_subject` 被忽略 —— PUB 端用
    /// 自身 `topic`，subject 用于 NATS 那种语义。
    async fn publish(&self, _subject: &str, envelope_bytes: Bytes) -> Result<()> {
        // 先解 envelope 拿 publisher_id / sequence，这样 dedup 信息可直接进
        // multipart frame 1/2，订阅侧不必再解 envelope。
        let codec = MsgpackCodec;
        let envelope = codec.decode_envelope(&envelope_bytes)?;

        let frame = Frame::new(envelope_bytes);
        let frames = vec![
            self.topic.as_bytes().to_vec(),
            envelope.publisher_id.to_be_bytes().to_vec(),
            envelope.sequence.to_be_bytes().to_vec(),
            frame.encode().to_vec(),
        ];

        self.socket
            .lock()
            .await
            .send(Multipart::from(frames))
            .await?;
        Ok(())
    }

    fn kind(&self) -> EventTransportKind {
        EventTransportKind::Zmq
    }
}

// =============================================================================
// === ZmqSubTransport =========================================================
// =============================================================================

/// 解析 pump 收到的 multipart 时的失败类型。所有失败都会 log 警告并被忽略 ——
/// 故意做成 enum 而不是 anyhow，是为了让 caller（pump 主循环）能精确分流。
#[derive(Debug)]
enum PumpDecodeError {
    WrongFrameCount(usize),
    BadPublisherIdLen(usize),
    BadSequenceLen(usize),
    BadFramePayload(anyhow::Error),
}

/// ZMQ SUB 端。后台用一个 pump task 从 socket 拉多帧、解码出 envelope payload、
/// 通过本地 broadcast channel 扇出给多个 `subscribe()` 流。
pub struct ZmqSubTransport {
    broadcast_tx: broadcast::Sender<Bytes>,
    _socket_pump_handle: tokio::task::JoinHandle<()>,
}

impl ZmqSubTransport {
    /// 连接单个 publisher 端点。
    pub async fn connect(portname: &str, topic: &str) -> Result<Self> {
        let ctx = Context::new();
        let socket = configure_subscribe_builder(subscribe(&ctx))
            .connect(portname)?
            .subscribe(topic.as_bytes())?;

        tracing::info!(
            portname = %portname,
            topic = %topic,
            rcvhwm = ZMQ_RCVHWM,
            "ZMQ SUB transport connected with configured HWM"
        );

        let (broadcast_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        let pump_handle = Self::start_socket_pump(socket, broadcast_tx.clone());

        Ok(Self {
            broadcast_tx,
            _socket_pump_handle: pump_handle,
        })
    }

    /// 连接到 broker 的 XPUB 端点（语义上与 `connect` 等价的别名）。
    pub async fn connect_broker(xpub_endpoint: &str, topic: &str) -> Result<Self> {
        Self::connect(xpub_endpoint, topic).await
    }

    /// 连接多个 broker XPUB 端点（HA）。委托给 [`Self::connect_multiple`]。
    pub async fn connect_broker_multiple(xpub_endpoints: &[String], topic: &str) -> Result<Self> {
        Self::connect_multiple(xpub_endpoints, topic).await
    }

    /// 同时连接多个 publisher 端点做 fan-in。`portnames` 不能为空。
    pub async fn connect_multiple(portnames: &[String], topic: &str) -> Result<Self> {
        let mut iter = portnames.iter();
        let Some(first) = iter.next() else {
            anyhow::bail!("Cannot connect to zero portnames");
        };

        let ctx = Context::new();
        let socket = configure_subscribe_builder(subscribe(&ctx))
            .connect(first)?
            .subscribe(topic.as_bytes())?;

        for ep in iter {
            socket.get_socket().connect(ep)?;
            tracing::debug!(portname = %ep, "ZMQ SUB connected to portname");
        }

        tracing::info!(
            num_portnames = portnames.len(),
            topic = %topic,
            rcvhwm = ZMQ_RCVHWM,
            "ZMQ SUB transport connected to multiple portnames with configured HWM"
        );

        let (broadcast_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        let pump_handle = Self::start_socket_pump(socket, broadcast_tx.clone());

        Ok(Self {
            broadcast_tx,
            _socket_pump_handle: pump_handle,
        })
    }

    /// 从一条 multipart 里把 envelope payload 抽出来。所有结构性问题都返回
    /// `PumpDecodeError` 让上层决定记日志的级别。
    fn decode_pump_message(frames: Vec<Vec<u8>>) -> Result<Bytes, PumpDecodeError> {
        if frames.len() != EXPECTED_FRAME_COUNT {
            return Err(PumpDecodeError::WrongFrameCount(frames.len()));
        }

        let publisher_id_bytes = &frames[1];
        if publisher_id_bytes.len() != 8 {
            return Err(PumpDecodeError::BadPublisherIdLen(publisher_id_bytes.len()));
        }
        let publisher_id = u64::from_be_bytes(publisher_id_bytes.as_slice().try_into().unwrap());

        let sequence_bytes = &frames[2];
        if sequence_bytes.len() != 8 {
            return Err(PumpDecodeError::BadSequenceLen(sequence_bytes.len()));
        }
        let sequence = u64::from_be_bytes(sequence_bytes.as_slice().try_into().unwrap());

        tracing::trace!(
            publisher_id,
            sequence,
            "Socket pump received ZMQ message"
        );

        let frame_bytes = Bytes::from(frames[3].clone());
        Frame::decode(frame_bytes)
            .map(|f| f.payload)
            .map_err(|e| PumpDecodeError::BadFramePayload(anyhow!(e)))
    }

    /// 启动后台 pump 任务：socket → broadcast。
    fn start_socket_pump(
        mut socket: Subscribe,
        broadcast_tx: broadcast::Sender<Bytes>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let Some(result) = socket.next().await else {
                    tracing::info!("ZMQ socket stream ended");
                    break;
                };

                let frames = match result {
                    Ok(m) => multipart_message(m),
                    Err(error) => {
                        tracing::error!(error = %error, "ZMQ receive error in socket pump");
                        break;
                    }
                };

                match Self::decode_pump_message(frames) {
                    Ok(payload) => {
                        let _ = broadcast_tx.send(payload);
                    }
                    Err(PumpDecodeError::WrongFrameCount(n)) => {
                        tracing::warn!(
                            frame_count = n,
                            "Unexpected multipart frame count in socket pump"
                        );
                    }
                    Err(PumpDecodeError::BadPublisherIdLen(n)) => {
                        tracing::warn!(actual = n, "Invalid publisher_id frame in socket pump");
                    }
                    Err(PumpDecodeError::BadSequenceLen(n)) => {
                        tracing::warn!(actual = n, "Invalid sequence frame in socket pump");
                    }
                    Err(PumpDecodeError::BadFramePayload(error)) => {
                        tracing::warn!(error = %error, "Failed to decode ZMQ frame in socket pump");
                    }
                }
            }

            tracing::info!("ZMQ socket pump task terminated");
        })
    }
}

#[async_trait]
impl EventTransportRx for ZmqSubTransport {
    /// 订阅本地 broadcast；多次调用返回独立 receiver。`_subject` 被忽略 ——
    /// SUB 端的 topic 过滤已经在 socket 层完成。
    async fn subscribe(&self, _subject: &str) -> Result<WireStream> {
        let mut receiver = self.broadcast_tx.subscribe();

        let stream = stream! {
            loop {
                match receiver.recv().await {
                    Ok(payload) => yield Ok(payload),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(skipped, "Subscriber lagged behind, skipped messages");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        tracing::info!("Broadcast channel closed");
                        break;
                    }
                }
            }
        };

        Ok(Box::pin(stream))
    }

    fn kind(&self) -> EventTransportKind {
        EventTransportKind::Zmq
    }
}

// =============================================================================
// === 单元测试 ================================================================
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transports::event_plane::{EventEnvelope, MsgpackCodec};
    use tokio::time::{Duration, timeout};

    /// ## 测试过程
    /// 单 publisher + 单 subscriber 在固定端口上发 1 条消息，断言收到。
    /// ## 意义
    /// 锁定 PUB/SUB 的端到端最小路径。
    #[tokio::test]
    async fn test_zmq_pubsub_basic() {
        let port = 25555;
        let portname = format!("tcp://127.0.0.1:{port}");
        let topic = "test-topic";

        let (publisher, _actual_endpoint) = ZmqPubTransport::bind(&portname, topic)
            .await
            .expect("Failed to create publisher");

        tokio::time::sleep(Duration::from_millis(100)).await;

        let subscriber = ZmqSubTransport::connect(&portname, topic)
            .await
            .expect("Failed to create subscriber");

        let mut stream = subscriber
            .subscribe(topic)
            .await
            .expect("Failed to create subscription");

        tokio::time::sleep(Duration::from_millis(100)).await;

        let codec = MsgpackCodec;
        let envelope = EventEnvelope {
            publisher_id: 12345,
            sequence: 1,
            published_at: 1700000000000,
            topic: topic.to_string(),
            payload: Bytes::from("test payload"),
        };

        let envelope_bytes = codec.encode_envelope(&envelope).unwrap();
        publisher.publish(topic, envelope_bytes).await.unwrap();

        let result = timeout(Duration::from_secs(2), stream.next()).await;
        assert!(result.is_ok(), "Timeout waiting for message");

        let received_bytes = result.unwrap().unwrap().unwrap();
        let decoded = codec.decode_envelope(&received_bytes).unwrap();

        assert_eq!(decoded.publisher_id, 12345);
        assert_eq!(decoded.sequence, 1);
        assert_eq!(decoded.topic, topic);
    }

    /// ## 测试过程
    /// 连发 5 条不同 sequence 的消息，断言顺序收到。
    /// ## 意义
    /// 锁定多消息有序传输契约。
    #[tokio::test]
    async fn test_zmq_multiple_messages() {
        let port = 25556;
        let portname = format!("tcp://127.0.0.1:{port}");
        let topic = "multi-test";

        let (publisher, _) = ZmqPubTransport::bind(&portname, topic).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let subscriber = ZmqSubTransport::connect(&portname, topic).await.unwrap();
        let mut stream = subscriber.subscribe(topic).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let codec = MsgpackCodec;

        for i in 0..5 {
            let envelope = EventEnvelope {
                publisher_id: 99999,
                sequence: i,
                published_at: 1700000000000 + i,
                topic: topic.to_string(),
                payload: Bytes::from(format!("message {i}")),
            };
            let bytes = codec.encode_envelope(&envelope).unwrap();
            publisher.publish(topic, bytes).await.unwrap();
        }

        for i in 0..5 {
            let result = timeout(Duration::from_secs(2), stream.next()).await;
            assert!(result.is_ok(), "Timeout on message {i}");

            let received = result.unwrap().unwrap().unwrap();
            let decoded = codec.decode_envelope(&received).unwrap();
            assert_eq!(decoded.sequence, i);
            assert_eq!(decoded.topic, topic);
        }
    }

    // === SECTION: 合并自 supplemental_tests 模块 ===

    async fn reserve_tcp_endpoint() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("should reserve a free local port");
        let port = listener
            .local_addr()
            .expect("reserved listener should have a local addr")
            .port();
        drop(listener);
        format!("tcp://127.0.0.1:{port}")
    }

    async fn make_valid_envelope_bytes(topic: &str, sequence: u64) -> Bytes {
        let codec = MsgpackCodec;
        let envelope = EventEnvelope {
            publisher_id: 4242,
            sequence,
            published_at: 1700000000000 + sequence,
            topic: topic.to_string(),
            payload: Bytes::from(format!("payload-{sequence}")),
        };
        codec
            .encode_envelope(&envelope)
            .expect("encoding envelope should succeed")
    }

    /// ## 测试过程
    /// 给 multipart 4 帧，断言 multipart_message 把每帧字节抽成 Vec。
    /// ## 意义
    /// 锁定低层 multipart → Vec<Vec<u8>> 的逐字节兼容。
    #[test]
    fn multipart_message_converts_frames_to_vecs() {
        let multipart = Multipart::from(vec![
            b"topic".to_vec(),
            vec![1, 2, 3],
            vec![4, 5],
            vec![6],
        ]);

        let out = multipart_message(multipart);
        assert_eq!(out.len(), 4);
        assert_eq!(out[0], b"topic".to_vec());
        assert_eq!(out[1], vec![1, 2, 3]);
        assert_eq!(out[2], vec![4, 5]);
        assert_eq!(out[3], vec![6]);
    }

    /// ## 测试过程
    /// bind `tcp://0.0.0.0:0`，断言返回的 portname 是真实端口，且 topic 字段写对。
    /// ## 意义
    /// 锁定 ":0 端口" 自动分配契约。
    #[tokio::test]
    async fn zmq_pub_bind_with_zero_port_returns_real_endpoint_and_topic() {
        let (publisher, portname) = ZmqPubTransport::bind("tcp://0.0.0.0:0", "topic-a")
            .await
            .expect("bind with :0 should succeed");

        assert!(portname.starts_with("tcp://0.0.0.0:"));
        assert_eq!(publisher.topic(), "topic-a");
    }

    /// ## 测试过程
    /// 空 portname 列表传给 connect_multiple，断言报特定字符串错误。
    /// ## 意义
    /// 锁定错误信息字面值。
    #[tokio::test]
    async fn zmq_pub_connect_multiple_rejects_empty_input() {
        let err = ZmqPubTransport::connect_multiple(&[], "topic")
            .await
            .err()
            .expect("connecting to zero portnames should fail");
        assert!(err.to_string().contains("Cannot connect to zero portnames"));
    }

    /// ## 测试过程
    /// 同上，SUB 侧 connect_multiple 空列表。
    /// ## 意义
    /// 锁定 SUB 侧错误信息一致性。
    #[tokio::test]
    async fn zmq_sub_connect_multiple_rejects_empty_input() {
        let err = ZmqSubTransport::connect_multiple(&[], "topic")
            .await
            .err()
            .expect("connecting to zero portnames should fail");
        assert!(err.to_string().contains("Cannot connect to zero portnames"));
    }

    /// ## 测试过程
    /// 给 publish 传一段非 msgpack envelope 的字节。
    /// ## 意义
    /// 锁定 publish 的"先 decode envelope"前置条件。
    #[tokio::test]
    async fn zmq_pub_publish_rejects_invalid_envelope_bytes() {
        let portname = reserve_tcp_endpoint().await;
        let (publisher, _portname) = ZmqPubTransport::bind(&portname, "topic-invalid")
            .await
            .expect("publisher bind should succeed");

        let err = publisher
            .publish("ignored", Bytes::from_static(b"not-msgpack-envelope"))
            .await
            .err()
            .expect("publish should fail when envelope bytes are invalid");
        assert!(!err.to_string().is_empty());
    }

    /// ## 测试过程
    /// 跑通 bind / connect / connect_broker / connect_multiple /
    /// connect_broker_multiple 全套连接 API，断言 kind() 都是 Zmq。
    /// ## 意义
    /// 锁定连接 API 矩阵的完备性 + kind 自报家门。
    #[tokio::test]
    async fn zmq_pub_sub_connect_variants_and_kind_work() {
        let portname = reserve_tcp_endpoint().await;
        let topic = "variant-topic";

        let (_publisher, _actual_endpoint) = ZmqPubTransport::bind(&portname, topic)
            .await
            .expect("publisher bind should succeed");
        tokio::time::sleep(Duration::from_millis(80)).await;

        let pub_client = ZmqPubTransport::connect(&portname, topic)
            .await
            .expect("pub connect should succeed");
        assert_eq!(EventTransportTx::kind(&pub_client), EventTransportKind::Zmq);

        let sub_direct = ZmqSubTransport::connect(&portname, topic)
            .await
            .expect("sub connect should succeed");
        assert_eq!(EventTransportRx::kind(&sub_direct), EventTransportKind::Zmq);

        let _sub_broker = ZmqSubTransport::connect_broker(&portname, topic)
            .await
            .expect("connect_broker should delegate to connect");

        let _pub_multi =
            ZmqPubTransport::connect_multiple(std::slice::from_ref(&portname), topic)
                .await
                .expect("pub connect_multiple with one portname should succeed");

        let _sub_multi =
            ZmqSubTransport::connect_broker_multiple(std::slice::from_ref(&portname), topic)
                .await
                .expect("sub connect_broker_multiple with one portname should succeed");
    }

    /// ## 测试过程
    /// 旁路用一个原生 PUB socket 发若干**畸形** multipart：错帧数 / 错 pub_id
    /// 长度 / 错 seq 长度 / 错 frame 头。300ms 内 stream 不出消息；之后发一帧
    /// 合法的，应收到对应 envelope 字节。
    /// ## 意义
    /// 锁定 pump 任务对畸形输入的"静默丢弃"契约。
    #[tokio::test]
    async fn socket_pump_ignores_malformed_frames_and_forwards_valid_frame_payload() {
        let portname = reserve_tcp_endpoint().await;
        let topic = "pump-topic";

        let ctx_pub = Context::new();
        let mut raw_pub = publish(&ctx_pub)
            .bind(&portname)
            .expect("raw publisher bind should succeed");

        tokio::time::sleep(Duration::from_millis(80)).await;

        let ctx_sub = Context::new();
        let sub_socket = configure_subscribe_builder(subscribe(&ctx_sub))
            .connect(&portname)
            .expect("subscriber connect should succeed")
            .subscribe(topic.as_bytes())
            .expect("subscriber topic subscribe should succeed");

        let (broadcast_tx, _) = broadcast::channel(128);
        let _pump = ZmqSubTransport::start_socket_pump(sub_socket, broadcast_tx.clone());
        let zst = ZmqSubTransport {
            broadcast_tx,
            _socket_pump_handle: tokio::spawn(async {}),
        };
        let mut stream = zst
            .subscribe(topic)
            .await
            .expect("local broadcast subscription should succeed");

        tokio::time::sleep(Duration::from_millis(200)).await;

        let bad_len = Multipart::from(vec![topic.as_bytes().to_vec(), vec![1], vec![2]]);
        raw_pub
            .send(bad_len)
            .await
            .expect("sending malformed frame count should succeed");

        let bad_publisher_id = Multipart::from(vec![
            topic.as_bytes().to_vec(),
            vec![1, 2, 3],
            1_u64.to_be_bytes().to_vec(),
            Frame::new(Bytes::from_static(b"x")).encode().to_vec(),
        ]);
        raw_pub
            .send(bad_publisher_id)
            .await
            .expect("sending malformed publisher_id frame should succeed");

        let bad_sequence = Multipart::from(vec![
            topic.as_bytes().to_vec(),
            7_u64.to_be_bytes().to_vec(),
            vec![9],
            Frame::new(Bytes::from_static(b"y")).encode().to_vec(),
        ]);
        raw_pub
            .send(bad_sequence)
            .await
            .expect("sending malformed sequence frame should succeed");

        let bad_frame_payload = Multipart::from(vec![
            topic.as_bytes().to_vec(),
            7_u64.to_be_bytes().to_vec(),
            8_u64.to_be_bytes().to_vec(),
            vec![0xff, 0x00, 0x01],
        ]);
        raw_pub
            .send(bad_frame_payload)
            .await
            .expect("sending malformed frame payload should succeed");

        let no_payload_from_bad_frames =
            timeout(Duration::from_millis(300), stream.next()).await;
        assert!(
            no_payload_from_bad_frames.is_err(),
            "malformed frames should not produce forwarded payloads"
        );

        let envelope_bytes = make_valid_envelope_bytes(topic, 11).await;
        let good = Multipart::from(vec![
            topic.as_bytes().to_vec(),
            4242_u64.to_be_bytes().to_vec(),
            11_u64.to_be_bytes().to_vec(),
            Frame::new(envelope_bytes.clone()).encode().to_vec(),
        ]);
        raw_pub
            .send(good)
            .await
            .expect("sending valid frame should succeed");
        tokio::time::sleep(Duration::from_millis(100)).await;
        raw_pub
            .send(Multipart::from(vec![
                topic.as_bytes().to_vec(),
                4242_u64.to_be_bytes().to_vec(),
                11_u64.to_be_bytes().to_vec(),
                Frame::new(envelope_bytes.clone()).encode().to_vec(),
            ]))
            .await
            .expect("sending valid frame retry should succeed");

        let received = timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("should receive forwarded payload")
            .expect("stream item should exist")
            .expect("stream item should be Ok");
        assert_eq!(received, envelope_bytes);
    }

    /// ## 测试过程
    /// 用容量为 1 的 broadcast 制造 Lagged；再 drop sender 触发 Closed。
    /// ## 意义
    /// 锁定 subscribe stream 在 Lagged / Closed 两条退化路径下的行为
    /// （Lagged 跳过、Closed 终止）。
    #[tokio::test]
    async fn subscribe_stream_handles_lagged_and_closed_paths() {
        let (tx, _) = broadcast::channel::<Bytes>(1);
        let zst = ZmqSubTransport {
            broadcast_tx: tx.clone(),
            _socket_pump_handle: tokio::spawn(async {}),
        };

        let mut stream = zst
            .subscribe("ignored")
            .await
            .expect("subscribe should create stream");
        drop(zst);

        let _ = tx.send(Bytes::from_static(b"m1"));
        let _ = tx.send(Bytes::from_static(b"m2"));
        let _ = tx.send(Bytes::from_static(b"m3"));

        let first = timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("should emit after lagged")
            .expect("stream should yield item")
            .expect("item should be Ok");
        assert_eq!(first, Bytes::from_static(b"m3"));

        drop(tx);

        let ended = timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("stream should terminate after channel close");
        assert!(ended.is_none());
    }
}

