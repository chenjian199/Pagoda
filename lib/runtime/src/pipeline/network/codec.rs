// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::network::codec` —— TwoPart 协议帧编解码器
//!
//! ## 设计意图
//! 本模块定义"请求平面"与"响应平面"在 TCP 字节流上的帧协议：
//! - `TcpRequestMessage` 描述一条带端点路径 + 头部 + 负载的请求帧；
//! - `TcpResponseMessage` 描述纯字节负载的响应帧；
//! - `TcpResponseCodec` 是 `tokio_util::codec` 风格的有状态解码器，
//!   配合 `Framed` 在异步流上做 zero-copy 拆帧；
//! - 顶层 `pub use` 把 [`two_part`] 与 [`zero_copy_decoder`] 子模块的核心类型
//!   再次导出，让上层 `network::codec::TwoPartCodec` 这样的短路径仍然可用。
//!
//! ## 外部契约
//! - 公开符号（顺序严格一致）：
//!   `pub mod zero_copy_decoder`、`pub use two_part::{TwoPartCodec, TwoPartMessage,
//!   TwoPartMessageType}`、`pub use zero_copy_decoder::{TcpRequestMessageZeroCopy,
//!   ZeroCopyTcpDecoder}`、`pub struct TcpRequestMessage { pub portname_path:
//!   String, pub headers: std::collections::HashMap<String, String>, pub payload:
//!   Bytes }`、`pub struct TcpResponseMessage { pub data: Bytes }`、
//!   `pub struct TcpResponseCodec`。
//! - 公开方法签名（**不可** 修改）：
//!   - `TcpRequestMessage::new(portname_path: String, payload: Bytes) -> Self`
//!   - `TcpRequestMessage::with_headers(portname_path: String, headers:
//!     std::collections::HashMap<String, String>, payload: Bytes) -> Self`
//!   - `TcpRequestMessage::encode(&self) -> Result<Bytes, std::io::Error>`
//!   - `TcpRequestMessage::decode(bytes: &Bytes) -> Result<Self, std::io::Error>`
//!   - `TcpResponseMessage::new(data: Bytes) -> Self`（**不可** 改为 `impl Into<Bytes>`）
//!   - `TcpResponseMessage::empty() -> Self`
//!   - `TcpResponseMessage::encode(&self) -> Result<Bytes, std::io::Error>`
//!   - `TcpResponseMessage::decode(bytes: &Bytes) -> Result<Self, std::io::Error>`
//!   - `TcpResponseCodec::new(max_message_size: Option<usize>) -> Self`
//! - `pub headers: std::collections::HashMap<String, String>` —— 必须使用全路径
//!   `std::collections::HashMap`，与 lib-copy 保持完全一致的 import 风格。
//! - 二进制线协议字段顺序与字节序为跨进程契约：
//!   `u16 BE portname_len | portname_bytes | u16 BE headers_len | headers_json |
//!    u32 BE payload_len | payload_bytes`；
//!   响应帧：`u32 BE data_len | data_bytes`。
//! - `TcpResponseCodec` 既要在 `Encoder` 上拒绝超 `max_message_size`，也要在 `Decoder`
//!   上同步拒绝（双向防御）；`max_message_size` 与 `total_len = 4 + data_len` 对比。
//! - 错误语义：
//!   - 长度字段（portname / headers / payload）超出对应整数宽度 → `InvalidInput`；
//!   - 缓冲不够长 / 缺字节 → `UnexpectedEof`；
//!   - UTF-8 失败 / JSON 失败 / 超 `max_message_size` → `InvalidData`。
//!
//! ## 实现要点
//! - **集中常量**：所有线协议长度字段宽度归到 [`wire`] 私有模块，并在 `const _: ()`
//!   断言里把"u16 表头 + u32 长度"等隐含不变式编译期固化。
//! - **统一游标**：[`FrameCursor`] 是 `&[u8] + pos` 的极薄抽象，把"读 u16 BE / 读 u32
//!   BE / 读切片 / 检查剩余字节"四种动作收敛到一处，消除散落在解码路径里的
//!   `if bytes.len() < x { Err(...) } u16::from_be_bytes([bytes[0], bytes[1]])`
//!   重复模式。
//! - **布局结构**：[`TcpRequestLayout`] 用 `Range<usize>` 直接表达每段在缓冲中的范围，
//!   解码器拿到布局后直接 `bytes.slice(range)` 一行完成 zero-copy 切片。
//! - **错误工厂**：[`truncated`] / [`length_overflow`] / [`oversized`] 三个私有构造器
//!   收敛 `std::io::Error` 创建，减少散落的 `format!("...")`。
//! - **解码不消耗输入**：`TcpRequestMessage::decode` 与 `TcpResponseMessage::decode`
//!   都接受 `&Bytes`，原地 `slice(range)` 共享底层 `Arc`，不拷贝负载。
//! - **不**改变 lib-copy 已有的行为：例如响应帧 `decode` 不强制 `max_message_size`
//!   —— 该检查只在 `TcpResponseCodec::decode/encode` 上做（lib-copy 原始语义保留）。

//! Codec Module
//!
//! Codec map structure into blobs of bytes and streams of bytes.
//!
//! In this module, we define three primary codec used to issue single, two-part or multi-part messages,
//! on a byte stream.

use bytes::Bytes;
use tokio_util::{
    bytes::{Buf, BufMut, BytesMut},
    codec::{Decoder, Encoder},
};

mod two_part;
pub mod zero_copy_decoder;

pub use two_part::{TwoPartCodec, TwoPartMessage, TwoPartMessageType};
pub use zero_copy_decoder::{TcpRequestMessageZeroCopy, ZeroCopyTcpDecoder};

// === SECTION: 线协议常量与编译期不变式 ===

/// 线协议字段宽度集中定义；任何对这些常量的修改都会破坏跨进程兼容性。
mod wire {
    /// 请求帧 portname 长度字段（u16 BE）。
    pub(super) const ENDPOINT_LEN_WIDTH: usize = 2;
    /// 请求帧 headers 长度字段（u16 BE）。
    pub(super) const HEADERS_LEN_WIDTH: usize = 2;
    /// 请求帧 payload 长度字段（u32 BE）。
    pub(super) const PAYLOAD_LEN_WIDTH: usize = 4;
    /// 响应帧 data 长度字段（u32 BE）。
    pub(super) const RESPONSE_LEN_WIDTH: usize = 4;

    // 编译期固化：长度字段宽度必须与读写时使用的整数宽度匹配。
    const _: () = assert!(ENDPOINT_LEN_WIDTH == size_of::<u16>());
    const _: () = assert!(HEADERS_LEN_WIDTH == size_of::<u16>());
    const _: () = assert!(PAYLOAD_LEN_WIDTH == size_of::<u32>());
    const _: () = assert!(RESPONSE_LEN_WIDTH == size_of::<u32>());
}

// 旧名重新导出供本文件内部短路径使用；不影响 pub 表面。
const TCP_REQUEST_ENDPOINT_LEN_WIDTH: usize = wire::ENDPOINT_LEN_WIDTH;
const TCP_REQUEST_HEADERS_LEN_WIDTH: usize = wire::HEADERS_LEN_WIDTH;
const TCP_REQUEST_PAYLOAD_LEN_WIDTH: usize = wire::PAYLOAD_LEN_WIDTH;

// === SECTION: 私有错误工厂 ===

#[inline]
fn truncated(reason: &'static str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::UnexpectedEof, reason)
}

#[inline]
fn truncated_owned(reason: String) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::UnexpectedEof, reason)
}

#[inline]
fn length_overflow(field: &'static str, len: usize) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!("{field}: {len} bytes"),
    )
}

#[inline]
fn oversized(total_len: usize, max: usize) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("message too large: {total_len} bytes (max: {max} bytes)"),
    )
}

#[inline]
fn invalid_data(reason: String) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, reason)
}

// === SECTION: 私有游标 FrameCursor ===

/// 极薄的"位置跟踪"读游标；把"边界检查 + 大端读 + 切片"统一到一处。
///
/// 所有方法都使用相对当前 `pos` 的偏移：调用者无需关心绝对索引。
struct FrameCursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> FrameCursor<'a> {
    #[inline]
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    #[inline]
    fn pos(&self) -> usize {
        self.pos
    }

    /// 确保剩余至少 `need` 字节，否则返回 truncation 错误。
    #[inline]
    fn require(&self, need: usize, reason: &'static str) -> Result<(), std::io::Error> {
        if self.buf.len().saturating_sub(self.pos) < need {
            Err(truncated(reason))
        } else {
            Ok(())
        }
    }

    /// 读取一个 u16 BE 并推进 2 字节。
    #[inline]
    fn read_u16_be(&mut self, reason: &'static str) -> Result<u16, std::io::Error> {
        self.require(wire::ENDPOINT_LEN_WIDTH, reason)?;
        let v = u16::from_be_bytes([self.buf[self.pos], self.buf[self.pos + 1]]);
        self.pos += wire::ENDPOINT_LEN_WIDTH;
        Ok(v)
    }

    /// 读取一个 u32 BE 并推进 4 字节。
    #[inline]
    fn read_u32_be(&mut self, reason: &'static str) -> Result<u32, std::io::Error> {
        self.require(wire::PAYLOAD_LEN_WIDTH, reason)?;
        let v = u32::from_be_bytes([
            self.buf[self.pos],
            self.buf[self.pos + 1],
            self.buf[self.pos + 2],
            self.buf[self.pos + 3],
        ]);
        self.pos += wire::PAYLOAD_LEN_WIDTH;
        Ok(v)
    }

    /// 跳过 `len` 字节并返回对应的相对区间 `[start, end)`。
    /// 区间用于后续 `Bytes::slice` 做 zero-copy 切片。
    #[inline]
    fn skip(
        &mut self,
        len: usize,
        reason: &'static str,
    ) -> Result<std::ops::Range<usize>, std::io::Error> {
        self.require(len, reason)?;
        let start = self.pos;
        self.pos += len;
        Ok(start..self.pos)
    }
}

// === SECTION: 请求帧布局 TcpRequestLayout ===

#[derive(Debug, Clone, PartialEq, Eq)]
struct TcpRequestLayout {
    portname: std::ops::Range<usize>,
    headers: std::ops::Range<usize>,
    payload: std::ops::Range<usize>,
    total_len: usize,
}

/// 编码侧的"先检查再加和"：拒绝任何会被 `as u16 / as u32` 截断的字段。
///
/// 通过校验后返回最终序列化所需的总字节数，用于一次性 `BytesMut::with_capacity`。
fn validate_request_encode_lengths(
    portname_len: usize,
    headers_len: usize,
    payload_len: usize,
) -> Result<usize, std::io::Error> {
    if portname_len > u16::MAX as usize {
        return Err(length_overflow("PortName path too long", portname_len));
    }
    if headers_len > u16::MAX as usize {
        return Err(length_overflow("Headers too large", headers_len));
    }
    if payload_len > u32::MAX as usize {
        return Err(length_overflow("Payload too large", payload_len));
    }

    let header_overhead = wire::ENDPOINT_LEN_WIDTH
        + portname_len
        + wire::HEADERS_LEN_WIDTH
        + headers_len
        + wire::PAYLOAD_LEN_WIDTH;

    header_overhead.checked_add(payload_len).ok_or_else(|| {
        invalid_data("TCP request message length overflow".to_string())
    })
}

/// 解码侧的"逐字段游标推进"：在不复制的前提下把帧切成三段范围。
///
/// 仅产出范围；UTF-8 与 JSON 解析延后到 [`TcpRequestMessage::decode`]，
/// 这样在只关心负载的快路径（zero-copy decoder）里可以跳过昂贵步骤。
fn parse_request_layout(bytes: &[u8]) -> Result<TcpRequestLayout, std::io::Error> {
    let mut cur = FrameCursor::new(bytes);

    let portname_len = cur.read_u16_be("Not enough bytes for portname path length")? as usize;
    let portname = cur.skip(portname_len, "Not enough bytes for portname path")?;

    let headers_len = cur.read_u16_be("Not enough bytes for headers length")? as usize;
    let headers = cur.skip(headers_len, "Not enough bytes for headers")?;

    let payload_len = cur.read_u32_be("Not enough bytes for payload length")? as usize;
    let payload_start = cur.pos();
    let total_len = payload_start.checked_add(payload_len).ok_or_else(|| {
        invalid_data("TCP request message length overflow".to_string())
    })?;

    if bytes.len() < total_len {
        return Err(truncated_owned(format!(
            "Not enough bytes for payload: expected {payload_len}, got {}",
            bytes.len().saturating_sub(payload_start)
        )));
    }

    Ok(TcpRequestLayout {
        portname,
        headers,
        payload: payload_start..total_len,
        total_len,
    })
}

// === SECTION: TcpRequestMessage ===

/// TCP request plane protocol message with portname routing and trace headers
///
/// Wire format:
/// - portname_path_len: u16 (big-endian)
/// - portname_path: UTF-8 string
/// - headers_len: u16 (big-endian)
/// - headers: JSON-encoded HashMap<String, String>
/// - payload_len: u32 (big-endian)
/// - payload: bytes
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpRequestMessage {
    pub portname_path: String,
    pub headers: std::collections::HashMap<String, String>,
    pub payload: Bytes,
}

impl TcpRequestMessage {
    pub fn new(portname_path: String, payload: Bytes) -> Self {
        Self {
            portname_path,
            headers: std::collections::HashMap::new(),
            payload,
        }
    }

    pub fn with_headers(
        portname_path: String,
        headers: std::collections::HashMap<String, String>,
        payload: Bytes,
    ) -> Self {
        Self {
            portname_path,
            headers,
            payload,
        }
    }

    /// Encode message to bytes
    pub fn encode(&self) -> Result<Bytes, std::io::Error> {
        let portname_bytes = self.portname_path.as_bytes();
        let portname_len = portname_bytes.len();

        // 头部 JSON 化一次，长度同时用于校验与一次性 reserve。
        let headers_json = serde_json::to_vec(&self.headers).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Failed to encode headers: {}", e),
            )
        })?;
        let headers_len = headers_json.len();

        let total_len =
            validate_request_encode_lengths(portname_len, headers_len, self.payload.len())?;

        let mut buf = BytesMut::with_capacity(total_len);
        buf.put_u16(portname_len as u16);
        buf.put_slice(portname_bytes);
        buf.put_u16(headers_len as u16);
        buf.put_slice(&headers_json);
        buf.put_u32(self.payload.len() as u32);
        buf.put_slice(&self.payload);

        Ok(buf.freeze())
    }

    /// Decode message from bytes (for backward compatibility, zero-copy when possible)
    pub fn decode(bytes: &Bytes) -> Result<Self, std::io::Error> {
        let layout = parse_request_layout(bytes)?;

        // portname：必须复制做 UTF-8 校验，但只复制 portname 段（通常很短）。
        let portname_path = String::from_utf8(bytes[layout.portname.clone()].to_vec())
            .map_err(|e| invalid_data(format!("Invalid UTF-8 in portname path: {e}")))?;

        // headers：JSON 反序列化必然要消费切片，但 payload 段完全 zero-copy。
        let headers: std::collections::HashMap<String, String> =
            serde_json::from_slice(&bytes[layout.headers.clone()])
                .map_err(|e| invalid_data(format!("Invalid JSON in headers: {e}")))?;

        let payload = bytes.slice(layout.payload);

        Ok(Self {
            portname_path,
            headers,
            payload,
        })
    }
}

// === SECTION: TcpResponseMessage ===

/// TCP response message (acknowledgment or error)
///
/// Wire format:
/// - length: u32 (big-endian)
/// - data: bytes
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpResponseMessage {
    pub data: Bytes,
}

impl TcpResponseMessage {
    pub fn new(data: Bytes) -> Self {
        Self { data }
    }

    pub fn empty() -> Self {
        Self { data: Bytes::new() }
    }

    /// Encode response to bytes (for backward compatibility)
    pub fn encode(&self) -> Result<Bytes, std::io::Error> {
        if self.data.len() > u32::MAX as usize {
            return Err(length_overflow("Response too large", self.data.len()));
        }

        let mut buf = BytesMut::with_capacity(wire::RESPONSE_LEN_WIDTH + self.data.len());
        buf.put_u32(self.data.len() as u32);
        buf.put_slice(&self.data);
        Ok(buf.freeze())
    }

    /// Decode response from bytes (for backward compatibility, zero-copy when possible)
    pub fn decode(bytes: &Bytes) -> Result<Self, std::io::Error> {
        let mut cur = FrameCursor::new(bytes);
        let len = cur.read_u32_be("Not enough bytes for response length")? as usize;
        let data_start = cur.pos();
        let data_end = data_start.checked_add(len).ok_or_else(|| {
            invalid_data("Response length overflow".to_string())
        })?;

        if bytes.len() < data_end {
            return Err(truncated_owned(format!(
                "Not enough bytes for response: expected {len}, got {}",
                bytes.len() - data_start
            )));
        }

        Ok(Self {
            data: bytes.slice(data_start..data_end),
        })
    }
}

// === SECTION: TcpResponseCodec ===

/// Codec for encoding/decoding TcpResponseMessage
/// Supports max_message_size enforcement
#[derive(Clone, Default)]
pub struct TcpResponseCodec {
    max_message_size: Option<usize>,
}

impl TcpResponseCodec {
    pub fn new(max_message_size: Option<usize>) -> Self {
        Self { max_message_size }
    }

    /// 共享检查：拒绝总长 `total_len > max_message_size`（如果设了上限）。
    #[inline]
    fn enforce_max(&self, total_len: usize) -> Result<(), std::io::Error> {
        if let Some(max) = self.max_message_size
            && total_len > max
        {
            return Err(oversized(total_len, max));
        }
        Ok(())
    }
}

impl Decoder for TcpResponseCodec {
    type Item = TcpResponseMessage;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // 至少需要 4 字节解析长度前缀。
        if src.len() < wire::RESPONSE_LEN_WIDTH {
            return Ok(None);
        }

        // peek 长度但不消费，这样 max-size 超限或缓冲不全时都能"原地"决策。
        let data_len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
        let total_len = wire::RESPONSE_LEN_WIDTH + data_len;

        self.enforce_max(total_len)?;

        if src.len() < total_len {
            return Ok(None);
        }

        src.advance(wire::RESPONSE_LEN_WIDTH);
        let data = src.split_to(data_len).freeze();
        Ok(Some(TcpResponseMessage { data }))
    }
}

impl Encoder<TcpResponseMessage> for TcpResponseCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: TcpResponseMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        if item.data.len() > u32::MAX as usize {
            return Err(length_overflow("Response too large", item.data.len()));
        }

        let total_len = wire::RESPONSE_LEN_WIDTH + item.data.len();
        self.enforce_max(total_len)?;

        dst.reserve(total_len);
        dst.put_u32(item.data.len() as u32);
        dst.put_slice(&item.data);
        Ok(())
    }
}

// === SECTION: 向 zero_copy_decoder 暴露的私有 helper（旧名桥接） ===
//
// 这些条目供同级子模块 `zero_copy_decoder` 在它的"头部 peek + 增量读"路径中复用。
// 待 `zero_copy_decoder.rs` 同步精细化后将切换到新内部 API 并移除这一节。

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpRequestWireHeader {
    portname_len: usize,
    headers_len: usize,
    payload_len: usize,
    header_size: usize,
    total_len: usize,
}

impl TcpRequestWireHeader {
    fn portname_start(&self) -> usize {
        wire::ENDPOINT_LEN_WIDTH
    }

    fn portname_end(&self) -> usize {
        self.portname_start() + self.portname_len
    }

    fn headers_start(&self) -> usize {
        self.portname_end() + wire::HEADERS_LEN_WIDTH
    }

    fn headers_end(&self) -> usize {
        self.headers_start() + self.headers_len
    }

    fn payload_start(&self) -> usize {
        self.header_size
    }
}

fn tcp_request_portname_len(bytes: &[u8]) -> Result<usize, std::io::Error> {
    let mut cur = FrameCursor::new(bytes);
    Ok(cur.read_u16_be("Not enough bytes for portname path length")? as usize)
}

fn tcp_request_headers_len(
    bytes: &[u8],
    portname_len: usize,
) -> Result<usize, std::io::Error> {
    let mut cur = FrameCursor::new(bytes);
    let _ = cur.read_u16_be("Not enough bytes for portname path length")?;
    cur.skip(portname_len, "Not enough bytes for portname path")?;
    cur.read_u16_be("Not enough bytes for headers length")
        .map(|v| v as usize)
}

fn tcp_request_header_size(portname_len: usize, headers_len: usize) -> usize {
    wire::ENDPOINT_LEN_WIDTH
        + portname_len
        + wire::HEADERS_LEN_WIDTH
        + headers_len
        + wire::PAYLOAD_LEN_WIDTH
}

/// 仅解析"帧头部"，不强制验证 payload 是否齐备 —— 给 zero_copy_decoder 在
/// "增量读 + 判定何时再去 read 更多字节"路径使用。
fn parse_tcp_request_frame_header(
    bytes: &[u8],
) -> Result<TcpRequestWireHeader, std::io::Error> {
    let mut cur = FrameCursor::new(bytes);
    let portname_len = cur.read_u16_be("Not enough bytes for portname path length")? as usize;
    cur.skip(portname_len, "Not enough bytes for portname path")?;
    let headers_len = cur.read_u16_be("Not enough bytes for headers length")? as usize;
    cur.skip(headers_len, "Not enough bytes for headers")?;
    let payload_len = cur.read_u32_be("Not enough bytes for payload length")? as usize;
    let header_size = cur.pos();
    let total_len = header_size.checked_add(payload_len).ok_or_else(|| {
        invalid_data("TCP request message length overflow".to_string())
    })?;

    Ok(TcpRequestWireHeader {
        portname_len,
        headers_len,
        payload_len,
        header_size,
        total_len,
    })
}

fn check_tcp_request_max_message_size(
    total_len: usize,
    max_message_size: usize,
) -> Result<(), std::io::Error> {
    if total_len > max_message_size {
        Err(oversized(total_len, max_message_size))
    } else {
        Ok(())
    }
}

// === SECTION: tests ===

#[cfg(test)]
mod tests {
    //! ## 测试矩阵
    //!
    //! | 用例 | 覆盖目标 |
    //! |------|----------|
    //! | `test_tcp_request_encode_decode` | request 完整 round-trip |
    //! | `test_tcp_request_empty_payload` | payload 长度=0 |
    //! | `test_tcp_request_large_payload` | 1MB payload |
    //! | `test_tcp_request_decode_truncated` | 末尾截断 → UnexpectedEof |
    //! | `test_tcp_request_decode_invalid_portname_utf8` | UTF-8 错误 → InvalidData |
    //! | `test_tcp_request_decode_invalid_headers_json` | JSON 错误 → InvalidData |
    //! | `test_tcp_request_empty_portname_path` | portname=""（lib-copy 名） |
    //! | `test_tcp_response_encode_decode` | response round-trip（lib-copy 名） |
    //! | `test_tcp_response_empty` | data=""（lib-copy 名） |
    //! | `test_tcp_response_decode_truncated` | 4 字节长度都不够（lib-copy 名） |
    //! | `test_tcp_request_unicode_portname` | portname 多字节字符（lib-copy 名） |
    //! | `test_tcp_response_codec` | codec encode + decode（lib-copy 名） |
    //! | `test_tcp_response_codec_partial` | 分片喂入返回 None（lib-copy 名） |
    //! | `test_tcp_response_codec_max_size` | encode 端 max-size 拒绝（lib-copy 名） |
    //! | `test_tcp_request_with_headers_round_trip` | 多 header 完整保留 |
    //! | `test_tcp_request_portname_length_overflow` | portname > u16::MAX → InvalidInput |
    //! | `test_tcp_request_headers_length_overflow` | headers > u16::MAX → InvalidInput |
    //! | `test_tcp_request_decode_trailing_bytes_ignored` | 末尾多余字节不破坏 decode |
    //! | `test_tcp_request_decode_byte_by_byte` | 流式逐字节，仅完整时才能 decode |
    //! | `test_tcp_response_codec_round_trip_multiple` | 单 buffer 多帧顺序消费 |
    //! | `test_tcp_response_codec_max_size_exact_boundary` | total_len == max 通过、+1 拒绝 |
    //! | `test_tcp_response_codec_decode_max_size` | decoder 也强制 max-size |
    //! | `test_tcp_response_decode_length_only` | 仅长度字段而无数据 → UnexpectedEof |
    //!
    //! ## 意义
    //! 14 条 lib-copy 名锁定 P2 行为；9 条新增覆盖：长度字段宽度边界、多头部、
    //! trailing bytes 兼容性、`Decoder` 状态机分片喂入、多帧 buffer 顺序消费、
    //! `max_message_size` 在双向（encode/decode）上的严格执行。
    //! 任何一条失败都意味着线协议或状态机被破坏。

    use super::*;

    // -------- lib-copy 原 14 条（语义不变） --------

    #[test]
    fn test_tcp_request_encode_decode() {
        let msg = TcpRequestMessage::new(
            "test.portname".to_string(),
            Bytes::from(vec![1, 2, 3, 4, 5]),
        );
        let encoded = msg.encode().unwrap();
        let decoded = TcpRequestMessage::decode(&encoded).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn test_tcp_request_empty_payload() {
        let msg = TcpRequestMessage::new("test".to_string(), Bytes::new());
        let encoded = msg.encode().unwrap();
        let decoded = TcpRequestMessage::decode(&encoded).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn test_tcp_request_large_payload() {
        let payload = Bytes::from(vec![42u8; 1024 * 1024]);
        let msg = TcpRequestMessage::new("large".to_string(), payload);
        let encoded = msg.encode().unwrap();
        let decoded = TcpRequestMessage::decode(&encoded).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn test_tcp_request_decode_truncated() {
        let msg = TcpRequestMessage::new("test".to_string(), Bytes::from(vec![1, 2, 3, 4, 5]));
        let encoded = msg.encode().unwrap();
        let truncated = encoded.slice(..encoded.len() - 2);
        assert!(TcpRequestMessage::decode(&truncated).is_err());
    }

    #[test]
    fn test_tcp_request_decode_invalid_portname_utf8() {
        let mut encoded = BytesMut::new();
        encoded.put_u16(2);
        encoded.put_slice(&[0xff, 0xff]);
        encoded.put_u16(2);
        encoded.put_slice(b"{}");
        encoded.put_u32(0);

        let err = TcpRequestMessage::decode(&encoded.freeze()).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("Invalid UTF-8"));
    }

    #[test]
    fn test_tcp_request_decode_invalid_headers_json() {
        let mut encoded = BytesMut::new();
        encoded.put_u16(4);
        encoded.put_slice(b"test");
        encoded.put_u16(1);
        encoded.put_slice(b"{");
        encoded.put_u32(0);

        let err = TcpRequestMessage::decode(&encoded.freeze()).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("Invalid JSON"));
    }

    #[test]
    fn test_tcp_request_empty_portname_path() {
        let msg = TcpRequestMessage::new(String::new(), Bytes::from_static(b"payload"));
        let encoded = msg.encode().unwrap();
        let decoded = TcpRequestMessage::decode(&encoded).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn test_tcp_response_encode_decode() {
        let msg = TcpResponseMessage::new(Bytes::from(vec![1, 2, 3, 4, 5]));
        let encoded = msg.encode().unwrap();
        let decoded = TcpResponseMessage::decode(&encoded).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn test_tcp_response_empty() {
        let msg = TcpResponseMessage::empty();
        let encoded = msg.encode().unwrap();
        let decoded = TcpResponseMessage::decode(&encoded).unwrap();
        assert_eq!(decoded, msg);
        assert_eq!(decoded.data.len(), 0);
    }

    #[test]
    fn test_tcp_response_decode_truncated() {
        let msg = TcpResponseMessage::new(Bytes::from(vec![1, 2, 3, 4, 5]));
        let encoded = msg.encode().unwrap();
        let truncated = encoded.slice(..3);
        assert!(TcpResponseMessage::decode(&truncated).is_err());
    }

    #[test]
    fn test_tcp_request_unicode_portname() {
        let msg = TcpRequestMessage::new("тест.端点".to_string(), Bytes::from(vec![1, 2, 3]));
        let encoded = msg.encode().unwrap();
        let decoded = TcpRequestMessage::decode(&encoded).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn test_tcp_response_codec() {
        let msg = TcpResponseMessage::new(Bytes::from(vec![1, 2, 3, 4, 5]));
        let mut codec = TcpResponseCodec::new(None);
        let mut buf = BytesMut::new();
        codec.encode(msg.clone(), &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn test_tcp_response_codec_partial() {
        let msg = TcpResponseMessage::new(Bytes::from(vec![1, 2, 3, 4, 5]));
        let encoded = msg.encode().unwrap();
        let mut codec = TcpResponseCodec::new(None);

        let mut buf = BytesMut::from(&encoded[..3]);
        assert!(codec.decode(&mut buf).unwrap().is_none());

        buf.extend_from_slice(&encoded[3..]);
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn test_tcp_response_codec_max_size() {
        let msg = TcpResponseMessage::new(Bytes::from(vec![1, 2, 3, 4, 5]));
        let mut codec = TcpResponseCodec::new(Some(5));
        let mut buf = BytesMut::new();
        assert!(codec.encode(msg, &mut buf).is_err());
    }

    // -------- 新增（按测试矩阵第 15~23 条） --------

    #[test]
    fn test_tcp_request_with_headers_round_trip() {
        let mut headers = std::collections::HashMap::new();
        headers.insert("trace-id".to_string(), "abc123".to_string());
        headers.insert("x-pagoda-request-id".to_string(), "req-7".to_string());
        headers.insert("multi-byte".to_string(), "值\u{1F600}".to_string());

        let msg = TcpRequestMessage::with_headers(
            "ep.path".to_string(),
            headers,
            Bytes::from_static(b"body"),
        );
        let encoded = msg.encode().unwrap();
        let decoded = TcpRequestMessage::decode(&encoded).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn test_tcp_request_portname_length_overflow() {
        // 直接构造一条 portname 超 u16::MAX 的请求；encode 必须 InvalidInput。
        let portname = "x".repeat(u16::MAX as usize + 1);
        let msg = TcpRequestMessage::new(portname, Bytes::new());
        let err = msg.encode().unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("PortName path too long"));
    }

    #[test]
    fn test_tcp_request_headers_length_overflow() {
        // 构造一个 header value 长到 JSON 序列化后超 u16::MAX。
        let mut headers = std::collections::HashMap::new();
        headers.insert("k".to_string(), "x".repeat(u16::MAX as usize + 16));

        let msg = TcpRequestMessage::with_headers("ep".to_string(), headers, Bytes::new());
        let err = msg.encode().unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("Headers too large"));
    }

    #[test]
    fn test_tcp_request_decode_trailing_bytes_ignored() {
        let msg = TcpRequestMessage::new("ep".to_string(), Bytes::from_static(b"body"));
        let mut buf = BytesMut::from(&msg.encode().unwrap()[..]);
        buf.extend_from_slice(b"--trailing--");

        let decoded = TcpRequestMessage::decode(&buf.freeze()).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn test_tcp_request_decode_byte_by_byte() {
        let msg = TcpRequestMessage::new(
            "ep.byte".to_string(),
            Bytes::from(vec![9, 8, 7, 6, 5, 4, 3, 2, 1]),
        );
        let encoded = msg.encode().unwrap();

        // 长度不足时必须返回错误；只有 == 完整长度时才成功。
        for prefix_len in 0..encoded.len() {
            let partial = encoded.slice(..prefix_len);
            assert!(
                TcpRequestMessage::decode(&partial).is_err(),
                "prefix_len={prefix_len} should fail"
            );
        }
        let full = TcpRequestMessage::decode(&encoded).unwrap();
        assert_eq!(full, msg);
    }

    #[test]
    fn test_tcp_response_codec_round_trip_multiple() {
        let a = TcpResponseMessage::new(Bytes::from_static(b"first"));
        let b = TcpResponseMessage::new(Bytes::from_static(b"second-longer"));
        let c = TcpResponseMessage::empty();

        let mut codec = TcpResponseCodec::new(None);
        let mut buf = BytesMut::new();
        codec.encode(a.clone(), &mut buf).unwrap();
        codec.encode(b.clone(), &mut buf).unwrap();
        codec.encode(c.clone(), &mut buf).unwrap();

        assert_eq!(codec.decode(&mut buf).unwrap().unwrap(), a);
        assert_eq!(codec.decode(&mut buf).unwrap().unwrap(), b);
        assert_eq!(codec.decode(&mut buf).unwrap().unwrap(), c);
        assert!(codec.decode(&mut buf).unwrap().is_none());
        assert!(buf.is_empty());
    }

    #[test]
    fn test_tcp_response_codec_max_size_exact_boundary() {
        let data_len = 7usize;
        let total_len = wire::RESPONSE_LEN_WIDTH + data_len;

        let msg = TcpResponseMessage::new(Bytes::from(vec![1u8; data_len]));

        // 上限 = total_len：应该通过。
        let mut codec_ok = TcpResponseCodec::new(Some(total_len));
        let mut buf_ok = BytesMut::new();
        codec_ok.encode(msg.clone(), &mut buf_ok).unwrap();
        let decoded_ok = codec_ok.decode(&mut buf_ok).unwrap().unwrap();
        assert_eq!(decoded_ok, msg);

        // 上限 = total_len - 1：应当拒绝。
        let mut codec_no = TcpResponseCodec::new(Some(total_len - 1));
        let mut buf_no = BytesMut::new();
        assert!(codec_no.encode(msg, &mut buf_no).is_err());
    }

    #[test]
    fn test_tcp_response_codec_decode_max_size() {
        // 用宽松 codec 编出超长帧，再用紧凑 codec 解码，验证 decoder 也强制 max。
        let big = TcpResponseMessage::new(Bytes::from(vec![7u8; 64]));
        let mut wide = TcpResponseCodec::new(None);
        let mut buf = BytesMut::new();
        wide.encode(big, &mut buf).unwrap();

        let mut tight = TcpResponseCodec::new(Some(8));
        let err = tight.decode(&mut buf).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("message too large"));
    }

    #[test]
    fn test_tcp_response_decode_length_only() {
        // 仅含 4 字节长度前缀但无 data → 必须 UnexpectedEof。
        let mut buf = BytesMut::new();
        buf.put_u32(8);
        let err = TcpResponseMessage::decode(&buf.freeze()).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }
}
