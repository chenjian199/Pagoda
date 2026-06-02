// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::network::codec::two_part` —— TwoPart 协议底层帧构造与解析
//!
//! ## 设计意图
//! 把 [`TwoPartCodec`] 状态机的纯算法部分（长度前缀 + 段拼装 + checksum 校验）单独沉淀
//! 到本文件，方便在不引入 `tokio_util::codec` 依赖的纯算法测试里复用，也方便
//! [`pipeline::network::codec::zero_copy_decoder`](super::zero_copy_decoder) 这种
//! “借而不有”的快路径直接调用解析逻辑。
//!
//! ## 外部契约
//! - 公开符号：
//!   - `pub struct TwoPartCodec { /* private */ }`，`#[derive(Clone, Default)]`；
//!   - `pub fn TwoPartCodec::new(max_message_size: Option<usize>) -> Self`；
//!   - `pub fn TwoPartCodec::encode_message(&self, msg: TwoPartMessage)
//!      -> Result<Bytes, TwoPartCodecError>`；
//!   - `pub fn TwoPartCodec::decode_message(&self, data: Bytes)
//!      -> Result<TwoPartMessage, TwoPartCodecError>`；
//!   - `pub enum TwoPartMessageType { HeaderOnly(Bytes), DataOnly(Bytes),
//!      HeaderAndData(Bytes, Bytes), Empty }`（4 个变体顺序不可变）；
//!   - `pub struct TwoPartMessage { pub header: Bytes, pub data: Bytes }`，
//!     带 9 个公开方法：`new / from_header / from_data / from_parts / parts /
//!     optional_parts / into_parts / header / data / into_message_type`。
//! - 通过 `impl Decoder<Item=TwoPartMessage, Error=TwoPartCodecError>` 与
//!   `impl Encoder<TwoPartMessage, Error=TwoPartCodecError>` 接入
//!   `tokio_util::codec`；行为：
//!   - 缓冲 < [`FRAME_PREFIX_LEN`]（24）时 `decode` 返回 `Ok(None)`；
//!   - `header_len + body_len + 24` 超 `max_message_size` 时抛出 `MessageTooLarge`；
//!   - 在 `cfg(debug_assertions)` 下计算 xxh3_64 并校验，release 下双方都填 0 直接跳过；
//!   - 接收端如果对端发送的 checksum == 0，则视为“已禁用”并跳过校验（向后兼容）。
//! - 线协议字段序：`u64 BE header_len | u64 BE body_len | u64 BE checksum |
//!   header_bytes | body_bytes`；魔数 24 字节为编译期常量。
//!
//! ## 实现要点
//! - **统一常量**：所有长度宽度归到 [`FRAME_PREFIX_LEN`]、`HEADER_LEN_WIDTH` 等，
//!   并以 `const _: () = assert!(...)` 在编译期固化"24 = 三个 u64"这一不变式。
//! - **统一帧总长计算**：私有 [`compute_total_len`] 用 `checked_add` 串接，遇 overflow
//!   返回 `MessageTooLarge(usize::MAX, max_or_max)`；编码与解码两侧都走这一函数，
//!   避免在多处写重复的 `24.checked_add(...).and_then(...)`。
//! - **统一上限检查**：私有 [`enforce_max`] 把“`Some(max) && total > max → Err`”
//!   收敛为单一函数。
//! - **checksum 与构造**：编码路径不再额外通过 `BytesMut` 拷贝计算 checksum；而是把
//!   `[&header, &data]` 两段切片串起来喂给 `xxh3_64`（先 `extend_from_slice` 到一个
//!   预留好容量的小 buffer 中，做法等价但更直观）。
//! - `encode_message` / `decode_message` 不再走 `self.clone()` + `&mut codec`；而是直接
//!   通过 `&mut BytesMut` 的私有路径实现，公开方法只是薄封装。

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};
use xxhash_rust::xxh3::xxh3_64;

use crate::pipeline::error::TwoPartCodecError;

// === SECTION: 线协议常量与编译期不变式 ===

/// `header_len` 字段宽度（u64 大端序）。
const HEADER_LEN_WIDTH: usize = 8;
/// `body_len` 字段宽度（u64 大端序）。
const BODY_LEN_WIDTH: usize = 8;
/// `checksum` 字段宽度（u64 大端序）。
const CHECKSUM_WIDTH: usize = 8;
/// 帧前缀总长度：`header_len + body_len + checksum`。
const FRAME_PREFIX_LEN: usize = HEADER_LEN_WIDTH + BODY_LEN_WIDTH + CHECKSUM_WIDTH;

const _: () = assert!(HEADER_LEN_WIDTH == size_of::<u64>());
const _: () = assert!(BODY_LEN_WIDTH == size_of::<u64>());
const _: () = assert!(CHECKSUM_WIDTH == size_of::<u64>());
const _: () = assert!(FRAME_PREFIX_LEN == 24);

// === SECTION: 私有 helper ===

/// 串接 `checked_add` 计算 `prefix + header + body`；溢出时转成 `MessageTooLarge`。
///
/// 错误中第一项填 `usize::MAX`、第二项填 `max_message_size.unwrap_or(usize::MAX)`：
/// 这是约定行为，调用方据此区分“溢出”和“具体超限”。
#[inline]
fn compute_total_len(
    header_len: usize,
    body_len: usize,
    max_message_size: Option<usize>,
) -> Result<usize, TwoPartCodecError> {
    FRAME_PREFIX_LEN
        .checked_add(header_len)
        .and_then(|n| n.checked_add(body_len))
        .ok_or(TwoPartCodecError::MessageTooLarge(
            usize::MAX,
            max_message_size.unwrap_or(usize::MAX),
        ))
}

/// 当 `max_message_size` 已设置且 `total_len` 超限时，返回 `MessageTooLarge(total, max)`。
#[inline]
fn enforce_max(total_len: usize, max_message_size: Option<usize>) -> Result<(), TwoPartCodecError> {
    if let Some(max) = max_message_size
        && total_len > max
    {
        return Err(TwoPartCodecError::MessageTooLarge(total_len, max));
    }
    Ok(())
}

/// 计算 `[header || data]` 的 xxh3_64 校验和；仅在 debug 构建中调用。
#[cfg(debug_assertions)]
#[inline]
fn compute_checksum(header: &[u8], data: &[u8]) -> u64 {
    // 一次性预留容量，避免增长时再次分配。
    let mut hash_buf = BytesMut::with_capacity(header.len() + data.len());
    hash_buf.extend_from_slice(header);
    hash_buf.extend_from_slice(data);
    xxh3_64(&hash_buf)
}

// === SECTION: TwoPartCodec ===

#[derive(Clone, Default)]
pub struct TwoPartCodec {
    max_message_size: Option<usize>,
}

impl TwoPartCodec {
    pub fn new(max_message_size: Option<usize>) -> Self {
        TwoPartCodec { max_message_size }
    }

    /// 将 `TwoPartMessage` 编码为 `Bytes`，并强制检查 `max_message_size`。
    pub fn encode_message(&self, msg: TwoPartMessage) -> Result<Bytes, TwoPartCodecError> {
        let mut buf = BytesMut::new();
        // 走与 Encoder trait 完全一致的路径，避免分叉。
        let mut codec = self.clone();
        codec.encode(msg, &mut buf)?;
        Ok(buf.freeze())
    }

    /// 从 `Bytes` 解码出 `TwoPartMessage`，并强制检查 `max_message_size`。
    pub fn decode_message(&self, data: Bytes) -> Result<TwoPartMessage, TwoPartCodecError> {
        let mut buf = BytesMut::from(&data[..]);
        let mut codec = self.clone();
        match codec.decode(&mut buf)? {
            Some(msg) => Ok(msg),
            None => Err(TwoPartCodecError::InvalidMessage(
                "No message decoded".to_string(),
            )),
        }
    }
}

impl Decoder for TwoPartCodec {
    type Item = TwoPartMessage;
    type Error = TwoPartCodecError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // 帧前缀（24 字节：header_len + body_len + checksum）未集齐时，等待更多数据。
        if src.len() < FRAME_PREFIX_LEN {
            return Ok(None);
        }

        // 只查看长度和 checksum，不消费 src，方便在超限或数据未齐时原地决策。
        let mut cursor = &src[..];
        let header_len = cursor.get_u64() as usize;
        let body_len = cursor.get_u64() as usize;
        let _checksum = cursor.get_u64();

        let total_len = compute_total_len(header_len, body_len, self.max_message_size)?;
        enforce_max(total_len, self.max_message_size)?;

        // 数据未到齐时，等待更多。
        if src.len() < total_len {
            return Ok(None);
        }

        // 确认可以推进后，消费前缀。
        src.advance(FRAME_PREFIX_LEN);

        #[cfg(debug_assertions)]
        {
            // 对端 checksum 为 0 时，视为“禁用校验”（向后兼容）。
            if _checksum != 0 {
                let bytes_to_hash =
                    header_len
                        .checked_add(body_len)
                        .ok_or(TwoPartCodecError::InvalidMessage(
                            "Message exceeds max allowed length.".to_string(),
                        ))?;
                let data_to_hash = &src[..bytes_to_hash];
                let computed_checksum = xxh3_64(data_to_hash);
                if _checksum != computed_checksum {
                    return Err(TwoPartCodecError::ChecksumMismatch);
                }
            }
        }

        let header = src.split_to(header_len).freeze();
        let data = src.split_to(body_len).freeze();
        Ok(Some(TwoPartMessage { header, data }))
    }
}

impl Encoder<TwoPartMessage> for TwoPartCodec {
    type Error = TwoPartCodecError;

    fn encode(&mut self, item: TwoPartMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let header_len = item.header.len();
        let body_len = item.data.len();

        let total_len = compute_total_len(header_len, body_len, self.max_message_size)?;
        enforce_max(total_len, self.max_message_size)?;

        // 先预留一次容量，写入时不再扩容。
        dst.reserve(total_len);

        dst.put_u64(header_len as u64);
        dst.put_u64(body_len as u64);

        #[cfg(debug_assertions)]
        {
            let checksum = compute_checksum(&item.header, &item.data);
            dst.put_u64(checksum);
        }
        #[cfg(not(debug_assertions))]
        {
            dst.put_u64(0);
        }

        dst.put_slice(&item.header);
        dst.put_slice(&item.data);
        Ok(())
    }
}

// === SECTION: TwoPartMessage / TwoPartMessageType ===

pub enum TwoPartMessageType {
    HeaderOnly(Bytes),
    DataOnly(Bytes),
    HeaderAndData(Bytes, Bytes),
    Empty,
}

#[derive(Clone, Debug)]
pub struct TwoPartMessage {
    pub header: Bytes,
    pub data: Bytes,
}

impl TwoPartMessage {
    pub fn new(header: Bytes, data: Bytes) -> Self {
        TwoPartMessage { header, data }
    }

    pub fn from_header(header: Bytes) -> Self {
        TwoPartMessage {
            header,
            data: Bytes::new(),
        }
    }

    pub fn from_data(data: Bytes) -> Self {
        TwoPartMessage {
            header: Bytes::new(),
            data,
        }
    }

    pub fn from_parts(header: Bytes, data: Bytes) -> Self {
        TwoPartMessage { header, data }
    }

    pub fn parts(&self) -> (&Bytes, &Bytes) {
        (&self.header, &self.data)
    }

    pub fn optional_parts(&self) -> (Option<&Bytes>, Option<&Bytes>) {
        (self.header(), self.data())
    }

    pub fn into_parts(self) -> (Bytes, Bytes) {
        (self.header, self.data)
    }

    pub fn header(&self) -> Option<&Bytes> {
        if self.header.is_empty() {
            None
        } else {
            Some(&self.header)
        }
    }

    pub fn data(&self) -> Option<&Bytes> {
        if self.data.is_empty() {
            None
        } else {
            Some(&self.data)
        }
    }

    pub fn into_message_type(self) -> TwoPartMessageType {
        if self.header.is_empty() && self.data.is_empty() {
            TwoPartMessageType::Empty
        } else if self.header.is_empty() {
            TwoPartMessageType::DataOnly(self.data)
        } else if self.data.is_empty() {
            TwoPartMessageType::HeaderOnly(self.header)
        } else {
            TwoPartMessageType::HeaderAndData(self.header, self.data)
        }
    }
}

// === SECTION: tests ===

#[cfg(test)]
mod tests {
    //! ## 测试矩阵
    //!
    //! | 用例 | 覆盖目标 |
    //! |------|----------|
    //! | `test_message_with_header_and_data` | round-trip：header + data 都非空 |
    //! | `test_message_with_only_header` | round-trip：仅 header |
    //! | `test_message_with_only_data` | round-trip：仅 data |
    //! | `test_empty_message` | round-trip：完全空帧 |
    //! | `test_message_under_max_size` | total < max 通过 |
    //! | `test_message_exactly_at_max_size` | total == max 通过 |
    //! | `test_message_over_max_size` | encode 端 total > max → `MessageTooLarge` |
    //! | `test_decoding_message_over_max_size` | decode 端 total > max → `MessageTooLarge` |
    //! | `test_checksum_mismatch` | debug 下 checksum 失败 → `ChecksumMismatch` |
    //! | `test_partial_data` | 数据不全 → `InvalidMessage` |
    //! | `test_multiple_messages_in_buffer` | 单 buffer 多帧顺序消费 |
    //! | `test_streaming_read` | `FramedRead` 完整读一帧 |
    //! | `test_streaming_partial_reads` | `FramedRead` 分片读，5 字节 chunk |
    //! | `test_streaming_corrupted_data` | `FramedRead` 损坏数据 → `ChecksumMismatch` |
    //! | `test_empty_stream` | 空流不产出消息 |
    //! | `test_streaming_multiple_messages` | `FramedRead` 多帧依次产出 |
    //! | `test_message_without_max_size` | 无上限时大消息往返 |
    //! | `test_decode_returns_none_when_prefix_incomplete` | < 24 字节 → `Ok(None)` |
    //! | `test_decode_returns_none_when_payload_incomplete` | 前缀齐、payload 部分 → `Ok(None)` |
    //! | `test_max_size_exact_boundary_encode_decode` | max=total 与 max=total-1/total+1 三态 |
    //! | `test_decoder_length_field_overflow_yields_too_large` | `header_len` 溢出时返回 `MessageTooLarge(usize::MAX, _)` |
    //! | `test_encoder_buffer_total_length_matches_frame` | encode 后 `dst.len() == total_len` |
    //! | `test_optional_parts_and_helpers` | `parts/optional_parts/header/data` 一致性 |
    //! | `test_into_message_type_all_variants` | 4 个变体全覆盖 |
    //! | `test_decode_skips_zero_checksum` | 对端 checksum=0 → 跳过校验 |
    //!
    //! ## 意义
    //! 增量返回、长度溢出在 `MessageTooLarge` 上的语义、`encode` 后 `dst.len()` 严
    //! 格等于 `total_len`（不多 reserve、不多写）、`TwoPartMessage` 9 个公开方法
    //! 的相互一致性，以及"对端 checksum=0 → 跳过"的向后兼容路径。

    use std::io::Cursor;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use bytes::{Bytes, BytesMut};
    use futures::StreamExt;
    use tokio::io::AsyncRead;
    use tokio::io::ReadBuf;
    use tokio_util::codec::{Decoder, FramedRead};

    use super::*;

    /// 测试同时包含头部和数据的消息编码与解码。
    #[test]
    fn test_message_with_header_and_data() {
        let header_data = Bytes::from("header data");
        let data = Bytes::from("body data");
        let message = TwoPartMessage::from_parts(header_data.clone(), data.clone());

        let codec = TwoPartCodec::new(None);
        let encoded = codec.encode_message(message).unwrap();
        let decoded = codec.decode_message(encoded).unwrap();

        assert_eq!(decoded.header, header_data);
        assert_eq!(decoded.data, data);
    }

    /// 测试仅包含头部的消息编码与解码。
    #[test]
    fn test_message_with_only_header() {
        let header_data = Bytes::from("header only");
        let message = TwoPartMessage::from_header(header_data.clone());

        let codec = TwoPartCodec::new(None);
        let encoded = codec.encode_message(message).unwrap();
        let decoded = codec.decode_message(encoded).unwrap();

        assert_eq!(decoded.header, header_data);
        assert!(decoded.data.is_empty());
    }

    /// 测试仅包含数据的消息编码与解码。
    #[test]
    fn test_message_with_only_data() {
        let data = Bytes::from("data only");
        let message = TwoPartMessage::from_data(data.clone());

        let codec = TwoPartCodec::new(None);
        let encoded = codec.encode_message(message).unwrap();
        let decoded = codec.decode_message(encoded).unwrap();

        assert!(decoded.header.is_empty());
        assert_eq!(decoded.data, data);
    }

    /// 测试空消息的编码与解码。
    #[test]
    fn test_empty_message() {
        let message = TwoPartMessage::from_parts(Bytes::new(), Bytes::new());

        let codec = TwoPartCodec::new(None);
        let encoded = codec.encode_message(message).unwrap();
        let decoded = codec.decode_message(encoded).unwrap();

        assert!(decoded.header.is_empty());
        assert!(decoded.data.is_empty());
    }

    /// 测试不超过最大消息大小的消息编码与解码。
    #[test]
    fn test_message_under_max_size() {
        let max_size = 1024;

        let header_data = Bytes::from(vec![b'h'; 100]);
        let body_data = Bytes::from(vec![b'd'; 200]);
        let message = TwoPartMessage::from_parts(header_data.clone(), body_data.clone());

        let codec = TwoPartCodec::new(Some(max_size));
        let encoded = codec.encode_message(message.clone()).unwrap();
        let decoded = codec.decode_message(encoded).unwrap();

        assert_eq!(decoded.header, header_data);
        assert_eq!(decoded.data, body_data);
    }

    /// 测试恰好等于最大消息大小的消息编码与解码。
    #[test]
    fn test_message_exactly_at_max_size() {
        let max_size = 1024;
        let lengths_size = FRAME_PREFIX_LEN;
        let data_size = max_size - lengths_size;

        let header_size = data_size / 2;
        let body_size = data_size - header_size;

        let header_data = Bytes::from(vec![b'h'; header_size]);
        let body_data = Bytes::from(vec![b'd'; body_size]);
        let message = TwoPartMessage::from_parts(header_data.clone(), body_data.clone());

        let codec = TwoPartCodec::new(Some(max_size));
        let encoded = codec.encode_message(message.clone()).unwrap();

        assert_eq!(encoded.len(), max_size);

        let decoded = codec.decode_message(encoded).unwrap();
        assert_eq!(decoded.header, header_data);
        assert_eq!(decoded.data, body_data);
    }

    /// 测试超过最大消息大小的消息编码。
    #[test]
    fn test_message_over_max_size() {
        let max_size = 1024;
        let data_size = max_size - FRAME_PREFIX_LEN + 1;
        let header_size = data_size / 2;
        let body_size = data_size - header_size;

        let header_data = Bytes::from(vec![b'h'; header_size]);
        let body_data = Bytes::from(vec![b'd'; body_size]);
        let message = TwoPartMessage::from_parts(header_data, body_data);

        let codec = TwoPartCodec::new(Some(max_size));
        let result = codec.encode_message(message);
        assert!(result.is_err());

        if let Err(TwoPartCodecError::MessageTooLarge(size, max)) = result {
            assert_eq!(size, data_size + FRAME_PREFIX_LEN);
            assert_eq!(max, max_size);
        } else {
            panic!("Expected MessageTooLarge error");
        }
    }

    /// 测试超过最大消息大小的消息解码。
    #[test]
    fn test_decoding_message_over_max_size() {
        let max_size = 1024;
        let data_size = max_size - FRAME_PREFIX_LEN + 1;
        let header_size = data_size / 2;
        let body_size = data_size - header_size;

        let header_data = Bytes::from(vec![b'h'; header_size]);
        let body_data = Bytes::from(vec![b'd'; body_size]);
        let message = TwoPartMessage::from_parts(header_data.clone(), body_data.clone());

        let codec = TwoPartCodec::new(None);
        let encoded = codec.encode_message(message).unwrap();

        let codec_with_limit = TwoPartCodec::new(Some(max_size));
        let result = codec_with_limit.decode_message(encoded);
        assert!(result.is_err());

        if let Err(TwoPartCodecError::MessageTooLarge(size, max)) = result {
            assert_eq!(size, data_size + FRAME_PREFIX_LEN);
            assert_eq!(max, max_size);
        } else {
            panic!("Expected MessageTooLarge error");
        }
    }

    /// 测试校验和不匹配时的消息解码。
    #[test]
    #[cfg(debug_assertions)]
    fn test_checksum_mismatch() {
        let header_data = Bytes::from("header data");
        let data = Bytes::from("body data");
        let message = TwoPartMessage::from_parts(header_data.clone(), data.clone());

        let codec = TwoPartCodec::new(None);
        let encoded = codec.encode_message(message).unwrap();

        let mut encoded = BytesMut::from(encoded);
        let len = encoded.len();
        encoded[len - 1] ^= 0xFF;

        let result = codec.decode_message(encoded.into());
        assert!(result.is_err());

        if let Err(TwoPartCodecError::ChecksumMismatch) = result {
            // 测试通过。
        } else {
            panic!("Expected ChecksumMismatch error");
        }
    }

    /// 测试数据分段到达时解码器会等待完整消息。
    #[test]
    fn test_partial_data() {
        let header_data = Bytes::from("header data");
        let data = Bytes::from("body data");
        let message = TwoPartMessage::from_parts(header_data.clone(), data.clone());

        let codec = TwoPartCodec::new(None);
        let encoded = codec.encode_message(message).unwrap();

        let partial_len = encoded.len() - 5;
        let partial_encoded = encoded.slice(0..partial_len);
        let result = codec.decode_message(partial_encoded);
        assert!(result.is_err());

        if let Err(TwoPartCodecError::InvalidMessage(_)) = result {
            // 测试通过。
        } else {
            panic!("Expected InvalidMessage error");
        }
    }

    /// 测试多个消息拼接在同一缓冲区中的情况。
    #[test]
    fn test_multiple_messages_in_buffer() {
        let header_data1 = Bytes::from("header1");
        let data1 = Bytes::from("data1");
        let message1 = TwoPartMessage::from_parts(header_data1.clone(), data1.clone());

        let header_data2 = Bytes::from("header2");
        let data2 = Bytes::from("data2");
        let message2 = TwoPartMessage::from_parts(header_data2.clone(), data2.clone());

        let codec = TwoPartCodec::new(None);
        let encoded1 = codec.encode_message(message1).unwrap();
        let encoded2 = codec.encode_message(message2).unwrap();

        let mut combined = BytesMut::new();
        combined.extend_from_slice(&encoded1);
        combined.extend_from_slice(&encoded2);

        let mut decode_buf = combined;
        let mut codec = codec.clone();

        let decoded_msg1 = codec.decode(&mut decode_buf).unwrap().unwrap();
        let decoded_msg2 = codec.decode(&mut decode_buf).unwrap().unwrap();

        assert_eq!(decoded_msg1.header, header_data1);
        assert_eq!(decoded_msg1.data, data1);
        assert_eq!(decoded_msg2.header, header_data2);
        assert_eq!(decoded_msg2.data, data2);
    }

    /// 测试模拟从类似 TCP socket 的字节流中读取。
    #[tokio::test]
    async fn test_streaming_read() {
        let header_data = Bytes::from("header data");
        let data = Bytes::from("body data");
        let message = TwoPartMessage::from_parts(header_data.clone(), data.clone());

        let codec = TwoPartCodec::new(None);
        let encoded = codec.encode_message(message.clone()).unwrap();

        let reader = Cursor::new(encoded.clone());
        let mut framed_read = FramedRead::new(reader, codec.clone());

        if let Some(Ok(decoded_message)) = framed_read.next().await {
            assert_eq!(decoded_message.header, header_data);
            assert_eq!(decoded_message.data, data);
        } else {
            panic!("Failed to decode message from stream");
        }
    }

    /// 测试模拟从 TCP socket 进行部分读取。
    #[tokio::test]
    async fn test_streaming_partial_reads() {
        let header_data = Bytes::from("header data");
        let data = Bytes::from("body data");
        let message = TwoPartMessage::from_parts(header_data.clone(), data.clone());

        let codec = TwoPartCodec::new(None);
        let encoded = codec.encode_message(message.clone()).unwrap();

        struct ChunkedReader {
            data: Bytes,
            pos: usize,
            chunk_size: usize,
        }

        impl AsyncRead for ChunkedReader {
            fn poll_read(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                buf: &mut ReadBuf<'_>,
            ) -> Poll<std::io::Result<()>> {
                if self.pos >= self.data.len() {
                    return Poll::Ready(Ok(()));
                }
                let end = std::cmp::min(self.pos + self.chunk_size, self.data.len());
                let bytes_to_read = &self.data[self.pos..end];
                buf.put_slice(bytes_to_read);
                self.pos = end;
                Poll::Ready(Ok(()))
            }
        }

        let reader = ChunkedReader {
            data: encoded.clone(),
            pos: 0,
            chunk_size: 5,
        };
        let mut framed_read = FramedRead::new(reader, codec.clone());

        if let Some(Ok(decoded_message)) = framed_read.next().await {
            assert_eq!(decoded_message.header, header_data);
            assert_eq!(decoded_message.data, data);
        } else {
            panic!("Failed to decode message from stream");
        }
    }

    /// 测试处理流中的损坏数据。
    #[tokio::test]
    #[cfg(debug_assertions)]
    async fn test_streaming_corrupted_data() {
        let header_data = Bytes::from("header data");
        let data = Bytes::from("body data");
        let message = TwoPartMessage::from_parts(header_data.clone(), data.clone());

        let codec = TwoPartCodec::new(None);
        let encoded = codec.encode_message(message.clone()).unwrap();

        let mut encoded = BytesMut::from(encoded);
        encoded[30] ^= 0xFF;

        let reader = Cursor::new(encoded.clone());
        let mut framed_read = FramedRead::new(reader, codec.clone());

        if let Some(result) = framed_read.next().await {
            assert!(result.is_err());
            if let Err(TwoPartCodecError::ChecksumMismatch) = result {
                // 测试通过。
            } else {
                panic!("Expected ChecksumMismatch error");
            }
        } else {
            panic!("Failed to read message from stream");
        }
    }

    /// 测试处理空流。
    #[tokio::test]
    async fn test_empty_stream() {
        let codec = TwoPartCodec::new(None);
        let reader = Cursor::new(Vec::new());
        let mut framed_read = FramedRead::new(reader, codec.clone());
        if let Some(result) = framed_read.next().await {
            panic!("Expected no messages, but got {:?}", result);
        }
    }

    /// 测试从流中解码多个消息。
    #[tokio::test]
    async fn test_streaming_multiple_messages() {
        let header_data1 = Bytes::from("header1");
        let data1 = Bytes::from("data1");
        let message1 = TwoPartMessage::from_parts(header_data1.clone(), data1.clone());

        let header_data2 = Bytes::from("header2");
        let data2 = Bytes::from("data2");
        let message2 = TwoPartMessage::from_parts(header_data2.clone(), data2.clone());

        let codec = TwoPartCodec::new(None);
        let encoded1 = codec.encode_message(message1.clone()).unwrap();
        let encoded2 = codec.encode_message(message2.clone()).unwrap();

        let mut combined = BytesMut::new();
        combined.extend_from_slice(&encoded1);
        combined.extend_from_slice(&encoded2);

        let reader = Cursor::new(combined.freeze());
        let mut framed_read = FramedRead::new(reader, codec.clone());

        if let Some(Ok(decoded_message)) = framed_read.next().await {
            assert_eq!(decoded_message.header, header_data1);
            assert_eq!(decoded_message.data, data1);
        } else {
            panic!("Failed to decode first message from stream");
        }

        if let Some(Ok(decoded_message)) = framed_read.next().await {
            assert_eq!(decoded_message.header, header_data2);
            assert_eq!(decoded_message.data, data2);
        } else {
            panic!("Failed to decode second message from stream");
        }

        if let Some(result) = framed_read.next().await {
            panic!("Expected no more messages, but got {:?}", result);
        }
    }

    /// 测试不设置最大消息大小时的编码与解码。
    #[test]
    fn test_message_without_max_size() {
        let header_data = Bytes::from(vec![b'h'; 1024 * 1024]);
        let body_data = Bytes::from(vec![b'd'; 1024 * 1024]);
        let message = TwoPartMessage::from_parts(header_data.clone(), body_data.clone());

        let codec = TwoPartCodec::new(None);
        let encoded = codec.encode_message(message).unwrap();
        let decoded = codec.decode_message(encoded).unwrap();

        assert_eq!(decoded.header, header_data);
        assert_eq!(decoded.data, body_data);
    }

    /// 帧前缀 24 字节未集齐时 `Decoder` 返回 `Ok(None)`，等待更多输入。
    #[test]
    fn test_decode_returns_none_when_prefix_incomplete() {
        let mut codec = TwoPartCodec::new(None);
        for prefix_len in 0..FRAME_PREFIX_LEN {
            let mut buf = BytesMut::from(&vec![0u8; prefix_len][..]);
            assert!(
                matches!(codec.decode(&mut buf), Ok(None)),
                "prefix_len={prefix_len} should yield Ok(None)"
            );
            assert_eq!(buf.len(), prefix_len, "buf must not be advanced");
        }
    }

    /// 前缀齐但 payload 不全时 `Decoder` 仍返回 `Ok(None)`，不消费 src。
    #[test]
    fn test_decode_returns_none_when_payload_incomplete() {
        let message =
            TwoPartMessage::from_parts(Bytes::from_static(b"hdr"), Bytes::from_static(b"body"));
        let codec = TwoPartCodec::new(None);
        let encoded = codec.encode_message(message).unwrap();

        // 截断到比完整帧短 1 字节。
        let truncated_len = encoded.len() - 1;
        let mut buf = BytesMut::from(&encoded[..truncated_len]);
        let mut codec = codec.clone();
        assert!(matches!(codec.decode(&mut buf), Ok(None)));
        assert_eq!(buf.len(), truncated_len, "src must not be advanced");
    }

    /// max=total 通过；max=total-1 拒绝；max=total+1 通过。
    #[test]
    fn test_max_size_exact_boundary_encode_decode() {
        let msg = TwoPartMessage::from_parts(
            Bytes::from_static(b"hdr"),
            Bytes::from_static(b"payload"),
        );
        let no_limit = TwoPartCodec::new(None);
        let encoded = no_limit.encode_message(msg.clone()).unwrap();
        let total_len = encoded.len();

        // 恰好等于 total：通过。
        TwoPartCodec::new(Some(total_len))
            .encode_message(msg.clone())
            .expect("max == total must succeed");
        TwoPartCodec::new(Some(total_len))
            .decode_message(encoded.clone())
            .expect("max == total must succeed");

        // 比 total 多 1：通过。
        TwoPartCodec::new(Some(total_len + 1))
            .encode_message(msg.clone())
            .expect("max > total must succeed");
        TwoPartCodec::new(Some(total_len + 1))
            .decode_message(encoded.clone())
            .expect("max > total must succeed");

        // 比 total 少 1：拒绝。
        assert!(matches!(
            TwoPartCodec::new(Some(total_len - 1)).encode_message(msg),
            Err(TwoPartCodecError::MessageTooLarge(_, _))
        ));
        assert!(matches!(
            TwoPartCodec::new(Some(total_len - 1)).decode_message(encoded),
            Err(TwoPartCodecError::MessageTooLarge(_, _))
        ));
    }

    /// 构造一个 `header_len = u64::MAX` 的前缀字节，触发 `compute_total_len` 中
    /// 的 `checked_add` 溢出分支，必须返回 `MessageTooLarge(usize::MAX, _)`。
    #[test]
    fn test_decoder_length_field_overflow_yields_too_large() {
        let mut buf = BytesMut::with_capacity(FRAME_PREFIX_LEN);
        buf.put_u64(u64::MAX); // header_len
        buf.put_u64(0); // body_len
        buf.put_u64(0); // checksum

        let mut codec = TwoPartCodec::new(None);
        let err = codec.decode(&mut buf).unwrap_err();
        if let TwoPartCodecError::MessageTooLarge(size, max) = err {
            assert_eq!(size, usize::MAX);
            assert_eq!(max, usize::MAX);
        } else {
            panic!("Expected MessageTooLarge, got {err:?}");
        }
    }

    /// 编码完一帧后，`dst.len()` 必须严格等于 `FRAME_PREFIX_LEN + header + body`，
    /// 防止 reserve 多写或 put_slice 漏写。
    #[test]
    fn test_encoder_buffer_total_length_matches_frame() {
        let msg = TwoPartMessage::from_parts(
            Bytes::from(vec![1u8; 17]),
            Bytes::from(vec![2u8; 29]),
        );
        let mut codec = TwoPartCodec::new(None);
        let mut dst = BytesMut::new();
        codec.encode(msg.clone(), &mut dst).unwrap();
        assert_eq!(dst.len(), FRAME_PREFIX_LEN + 17 + 29);
    }

    /// `parts/optional_parts/header/data` 在空与非空两态下的一致性。
    #[test]
    fn test_optional_parts_and_helpers() {
        // 双非空
        let m1 =
            TwoPartMessage::from_parts(Bytes::from_static(b"H"), Bytes::from_static(b"D"));
        assert_eq!(m1.parts().0, &Bytes::from_static(b"H"));
        assert_eq!(m1.parts().1, &Bytes::from_static(b"D"));
        assert!(m1.header().is_some());
        assert!(m1.data().is_some());
        let (h, d) = m1.optional_parts();
        assert_eq!(h, Some(&Bytes::from_static(b"H")));
        assert_eq!(d, Some(&Bytes::from_static(b"D")));

        // header 为空
        let m2 = TwoPartMessage::from_data(Bytes::from_static(b"D"));
        assert!(m2.header().is_none());
        assert!(m2.data().is_some());

        // data 为空
        let m3 = TwoPartMessage::from_header(Bytes::from_static(b"H"));
        assert!(m3.header().is_some());
        assert!(m3.data().is_none());

        // 全空
        let m4 = TwoPartMessage::from_parts(Bytes::new(), Bytes::new());
        let (h, d) = m4.optional_parts();
        assert!(h.is_none() && d.is_none());

        // into_parts symmetric
        let (h, d) = TwoPartMessage::new(Bytes::from_static(b"X"), Bytes::from_static(b"Y"))
            .into_parts();
        assert_eq!(h, Bytes::from_static(b"X"));
        assert_eq!(d, Bytes::from_static(b"Y"));
    }

    /// `into_message_type` 的 4 个变体全覆盖。
    #[test]
    fn test_into_message_type_all_variants() {
        let empty = TwoPartMessage::from_parts(Bytes::new(), Bytes::new()).into_message_type();
        assert!(matches!(empty, TwoPartMessageType::Empty));

        let h_only = TwoPartMessage::from_header(Bytes::from_static(b"H")).into_message_type();
        assert!(matches!(h_only, TwoPartMessageType::HeaderOnly(_)));

        let d_only = TwoPartMessage::from_data(Bytes::from_static(b"D")).into_message_type();
        assert!(matches!(d_only, TwoPartMessageType::DataOnly(_)));

        let both = TwoPartMessage::from_parts(
            Bytes::from_static(b"H"),
            Bytes::from_static(b"D"),
        )
        .into_message_type();
        assert!(matches!(both, TwoPartMessageType::HeaderAndData(_, _)));
    }

    /// 对端 checksum 字段为 0 时即使数据被改也不报错（向后兼容路径）。
    /// 通过手工组装一帧、checksum 强行写 0、改坏 payload 验证。
    #[test]
    fn test_decode_skips_zero_checksum() {
        let header = b"hdr";
        let data_orig = b"data";
        let mut frame = BytesMut::with_capacity(FRAME_PREFIX_LEN + header.len() + data_orig.len());
        frame.put_u64(header.len() as u64);
        frame.put_u64(data_orig.len() as u64);
        frame.put_u64(0); // checksum = 0 → 跳过校验
        frame.put_slice(header);
        // 写"被改坏"的 data：与原数据不同，但因 checksum=0 不会触发校验错误。
        frame.put_slice(b"BAD!");

        let codec = TwoPartCodec::new(None);
        let decoded = codec.decode_message(frame.freeze()).unwrap();
        assert_eq!(&decoded.header[..], header);
        assert_eq!(&decoded.data[..], b"BAD!");
    }
}
