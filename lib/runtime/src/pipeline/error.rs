// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::error` —— pipeline 错误类型与下转扩展
//!
//! ## 设计意图
//! pipeline 的错误路径跨多个概念层（本地图 / 远程 NATS / TCP 传输 / 序列化），
//! 需要一种“通用错误类型”能够在 trait 边界上被封装为 `anyhow::Error` 传递，
//! 同时上层调用者仍能“取出原始类型”做细化处理。本文件提供：
//! ¹ `PipelineError` 枚举；² `PipelineErrorExt` 扩展 trait，把 `anyhow::Error`
//! 下转为 `PipelineError`；³ `TwoPartCodec` 所需的 `TwoPartCodecError` 枚举。
//!
//! ## 外部契约
//! - `pub use anyhow::{Context, Error, Result, anyhow, anyhow as error, bail, ensure}`：
//!   重导出 anyhow 的公共名称，pipeline 下游可直接从本模块拿到完整错误面。
//! - `trait PipelineErrorExt for Error`：
//!   - `try_into_pipeline_error(self) -> Result<PipelineError, Error>` 返回 `Ok` 表示
//!     原始错误是 `PipelineError`。
//!   - `either_pipeline_error(self) -> either::Either<PipelineError, Error>` 提供双分支取回。
//! - `enum PipelineError`：全部变体与其 `#[error("...")]` 消息都属于契约；
//!   不同场景返回哪个错误变体也是契约的一部分。
//! - `enum TwoPartCodecError`：仅供 TwoPartCodec 传输业务使用。
//!
//! ## 实现要点
//! - `try_into_pipeline_error` 直接委托给 `self.downcast::<PipelineError>()`，
//!   不额外生成中间变量，保持稳定。
//! - `either_pipeline_error` 复用 `self.downcast::<PipelineError>()`，而不是再转一层
//!   `try_into_pipeline_error()`，避免未来修改引入不必要的间接调用。
//! - `PipelineError` 中各个 `#[from]` 会转接 NATS / IO / Prometheus / IP 等错误类型，
//!   让业务代码可以直接用 `?` 向上抛，避免手写 `map_err`。

use async_nats::error::Error as NatsError;

pub use anyhow::{Context, Error, Result, anyhow, anyhow as error, bail, ensure};

// === SECTION: PipelineErrorExt 下转扩展 ===

pub trait PipelineErrorExt {
    /// 将 [`Error`] 下转为 [`PipelineError`]。
    fn try_into_pipeline_error(self) -> Result<PipelineError, Error>;

    /// 如果 [`Error`] 能下转为 [`PipelineError`]，则返回左分支；
    /// 否则返回右分支。
    fn either_pipeline_error(self) -> either::Either<PipelineError, Error>;
}

impl PipelineErrorExt for Error {
    fn try_into_pipeline_error(self) -> Result<PipelineError, Error> {
        self.downcast::<PipelineError>()
    }

    fn either_pipeline_error(self) -> either::Either<PipelineError, Error> {
        match self.downcast::<PipelineError>() {
            Ok(err) => either::Left(err),
            Err(err) => either::Right(err),
        }
    }
}

// === SECTION: PipelineError 枚举 ===

#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    /// 起始占位，待代码成熟后移除。
    #[error("Generic error: {0}")]
    Generic(String),

    /// 边只能设置一次。后续再次设置时会抛出该错误。
    #[error("Link failed: Edge already set")]
    EdgeAlreadySet,

    /// 源节点没有连接到任何边。
    #[error("Disconnected source; no edge on which to send data")]
    NoEdge,

    #[error("SegmentSink is not connected to an EgressPort")]
    NoNetworkEdge,

    /// 在请求发出到流收到之间，请求任务被丢弃。
    /// 这可能是 pipeline 的逻辑错误；未来可能升级为 panic / 致命错误。
    /// 当终止型 sink 的 `on_data` 无法找到 `oneshot` 发送端，或对应接收端已被丢弃时，
    /// 就会抛出这个错误。
    #[error("Unlinked request; initiating request task was dropped or cancelled")]
    DetachedStreamReceiver,

    // 在响应产生到流收到之间，流的 Sender 被丢弃。
    // 这可能是 pipeline 的逻辑错误；未来可能升级为 panic / 致命错误。
    #[error("Unlinked response; response task was dropped or cancelled")]
    DetachedStreamSender,

    #[error("Serialzation Error: {0}")]
    SerializationError(String),

    #[error("Deserialization Error: {0}")]
    DeserializationError(String),

    #[error("Failed to issue request to the control plane: {0}")]
    ControlPlaneRequestError(String),

    #[error("Failed to establish a streaming connection: {0}")]
    ConnectionFailed(String),

    #[error("Generate Error: {0}")]
    GenerateError(Error),

    #[error("An portname URL must have the format: namespace/servicegroup/portname")]
    InvalidPortNameFormat,

    #[error("NATS Request Error: {0}")]
    NatsRequestError(#[from] NatsError<async_nats::jetstream::context::RequestErrorKind>),

    #[error("NATS Get Stream Error: {0}")]
    NatsGetStreamError(#[from] NatsError<async_nats::jetstream::context::GetStreamErrorKind>),

    #[error("NATS Create Stream Error: {0}")]
    NatsCreateStreamError(#[from] NatsError<async_nats::jetstream::context::CreateStreamErrorKind>),

    #[error("NATS Consumer Error: {0}")]
    NatsConsumerError(#[from] NatsError<async_nats::jetstream::stream::ConsumerErrorKind>),

    #[error("NATS Batch Error: {0}")]
    NatsBatchError(#[from] NatsError<async_nats::jetstream::consumer::pull::BatchErrorKind>),

    #[error("NATS Publish Error: {0}")]
    NatsPublishError(#[from] NatsError<async_nats::client::PublishErrorKind>),

    #[error("NATS Connect Error: {0}")]
    NatsConnectError(#[from] NatsError<async_nats::ConnectErrorKind>),

    #[error("NATS Subscriber Error: {0}")]
    NatsSubscriberError(#[from] async_nats::SubscribeError),

    #[error("Local IP Address Error: {0}")]
    LocalIpAddressError(#[from] local_ip_address::Error),

    #[error("Prometheus Error: {0}")]
    PrometheusError(#[from] prometheus::Error),

    #[error("Other NATS Error: {0}")]
    NatsError(#[from] Box<dyn std::error::Error + Send + Sync>),

    #[error("Two Part Codec Error: {0}")]
    TwoPartCodec(#[from] TwoPartCodecError),

    #[error("Serde Json Error: {0}")]
    SerdeJsonError(#[from] serde_json::Error),

    #[error("NATS KV Err: {0} for bucket '{1}")]
    KeyValueError(String, String),

    /// 所有实例都很忙，无法处理新请求。
    #[error("Service temporarily unavailable: {0}")]
    ServiceOverloaded(String),
}

#[derive(Debug, thiserror::Error)]
pub enum TwoPartCodecError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Message size {0} exceeds the maximum allowed size of {1} bytes")]
    MessageTooLarge(usize, usize),

    #[error("Invalid message: {0}")]
    InvalidMessage(String),

    #[error("Checksum mismatch")]
    ChecksumMismatch,
}
