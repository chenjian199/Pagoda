// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::network::codec::zero_copy_decoder` —— TCP 请求帧的零拷贝解码器
//!
//! ## 设计意图
//! - 与 [`super::TcpRequest`] / [`super::TwoPartCodec`] 走的"拥有式"路径互补：本模块持有
//!   一个 **可重用** 的 [`BytesMut`] 读缓冲，通过 `split_to(n).freeze()` 将完整帧切出为
//!   `Bytes`（Arc 引用计数），然后用 **借用切片** 形式（`&[u8]` / `Bytes::slice(..)`)
//!   暴露 portname / headers / payload。整条路径 **0 次内存拷贝**，clone 也仅是 Arc 增引。
//! - 缓冲在 **空且容量过大** 时主动收缩回 [`INITIAL_BUFFER_SIZE`]，避免长尾大消息把缓冲
//!   永久撑大；收缩阈值可通过环境变量 `PGD_TCP_SHRINK_MESSAGE_SIZE` 调整。
//!
//! ## 外部契约
//! - `pub struct ZeroCopyTcpDecoder` + `new() / with_capacity(usize) / read_message<R> /
//!   buffer_capacity() / buffered_len() / Default`
//! - `pub struct TcpRequestMessageZeroCopy: Clone + Debug` + `portname_path() ->
//!   Result<&str, Utf8Error> / portname_path_bytes() / headers_bytes() / headers() /
//!   payload() -> Bytes / total_size() / raw_bytes() -> &Bytes`
//! - 错误语义：
//!   - 连接尚未发出任何字节就关闭 → [`io::ErrorKind::UnexpectedEof`] + `"connection closed"`
//!   - 已读了部分字节但帧未拼完 → `UnexpectedEof` + `"incomplete message header"` /
//!     `"incomplete message: expected … got …"`
//!   - 帧声明长度超过 `max_message_size` → 沿用 [`super::check_tcp_request_max_message_size`]
//!     给出的 `InvalidData + "message too large"`
//! - `headers()` 在 JSON 解析失败时返回空 `HashMap`（`unwrap_or_default()`），不抛错。
//!
//! ## 实现要点
//! - 旧实现把"等够 N 字节，否则继续 `read_buf`，遇到 0 即报错"的循环写了 **4 次**：
//!   现统一收敛到私有 helper [`ZeroCopyTcpDecoder::fill_at_least`]，由 [`FillStage`] 决定
//!   不同阶段的 EOF 错误消息，状态机更易审计。
//! - `parse_tcp_request_frame_header` / `check_tcp_request_max_message_size` 等帧头校验
//!   仍委托给 `super::` 中的 **桥接 helper**，与 `codec.rs` 端的精细化结构对齐；待这边
//!   长期稳定后，可一并切换到 `super::TcpRequestLayout`。
//! - `try_shrink_after_split` 把"空 + 容量超阈值 → 重建 BytesMut"的判定独立化，便于测试
//!   覆盖。

//! Zero-copy TCP message decoder for high-concurrency scenarios
//!
//! This decoder eliminates message reconstruction copies by:
//! 1. Reading into a reusable buffer
//! 2. Parsing headers in-place
//! 3. Splitting off exact message sizes (zero-copy via Bytes::split_to)
//! 4. Returning Arc-counted Bytes that can be cloned cheaply

// ─────────────────────────────────────────────────────────────────────────────
// === SECTION: [1] 依赖导入
// ─────────────────────────────────────────────────────────────────────────────

use super::{
    check_tcp_request_max_message_size, parse_tcp_request_frame_header, tcp_request_portname_len,
    tcp_request_header_size, tcp_request_headers_len,
};
use crate::pipeline::network::get_tcp_max_message_size;
use bytes::{Bytes, BytesMut};
use std::io;
use std::sync::OnceLock;
use tokio::io::{AsyncRead, AsyncReadExt};

// ─────────────────────────────────────────────────────────────────────────────
// === SECTION: [2] 常量、编译期不变式与全局收缩阈值缓存
// ─────────────────────────────────────────────────────────────────────────────

/// 默认初始读缓冲容量：256 KiB。
///
/// 选取依据：覆盖 LLM 请求中的绝大多数小帧（path + 数 KiB JSON header + ≤数十 KiB payload），
/// 避免热路径上反复 grow。
const INITIAL_BUFFER_SIZE: usize = 262_144; // 256 KiB

/// 缺省的"读完后是否收缩"阈值：8 MiB。
///
/// 当 `read_buffer.capacity() > shrink_threshold` 且缓冲已空时，重新分配回
/// [`INITIAL_BUFFER_SIZE`]，避免一次大请求长期占用大段内存。
const DEFAULT_SHRINK_SIZE: usize = 8 * 1024 * 1024; // 8 MiB

// —— 编译期不变式 —— //
// INITIAL_BUFFER_SIZE 必须 ≤ DEFAULT_SHRINK_SIZE，否则收缩动作会立刻把容量"放大"，
// `resolve_shrink_message_size` 的 .max(INITIAL_BUFFER_SIZE) 也将失去意义。
const _: () = assert!(
    INITIAL_BUFFER_SIZE <= DEFAULT_SHRINK_SIZE,
    "INITIAL_BUFFER_SIZE 必须 ≤ DEFAULT_SHRINK_SIZE"
);
// 两个常量都必须严格为正。
const _: () = assert!(INITIAL_BUFFER_SIZE > 0);
const _: () = assert!(DEFAULT_SHRINK_SIZE > 0);

/// 进程内 once-init 的收缩阈值缓存（受 `get_tcp_max_message_size()` 与环境变量影响）。
static SHRINK_MESSAGE_SIZE: OnceLock<usize> = OnceLock::new();

/// 读取环境变量 `PGD_TCP_SHRINK_MESSAGE_SIZE`，与 `max_message_size`、`INITIAL_BUFFER_SIZE`
/// 共同决定最终收缩阈值，并把结果缓存到 [`SHRINK_MESSAGE_SIZE`]。
///
/// 该函数仅在首次调用时做解析与日志告警，后续调用走 `OnceLock::get_or_init` 的快路径。
fn get_shrink_message_size() -> usize {
    *SHRINK_MESSAGE_SIZE.get_or_init(|| {
        let max_size = get_tcp_max_message_size();

        // Check for environment variable override
        let env_result = std::env::var("PGD_TCP_SHRINK_MESSAGE_SIZE");
        let env_shrink_size = env_result.as_ref().ok().and_then(|s| {
            s.parse::<usize>().ok().or_else(|| {
                tracing::warn!(
                    env_var = "PGD_TCP_SHRINK_MESSAGE_SIZE",
                    value = %s,
                    "Invalid value for PGD_TCP_SHRINK_MESSAGE_SIZE, using default"
                );
                None
            })
        });

        let resolved = resolve_shrink_message_size(max_size, env_shrink_size);

        // Warn if the configured value was clamped
        if let Some(configured) = env_shrink_size
            && configured != resolved
        {
            tracing::warn!(
                configured_size = configured,
                resolved_size = resolved,
                max_size = max_size,
                initial_buffer_size = INITIAL_BUFFER_SIZE,
                "PGD_TCP_SHRINK_MESSAGE_SIZE was clamped to valid range. Note the size is in bytes."
            );
        }

        resolved
    })
}

/// 把"环境变量给出的期望收缩阈值"夹紧到合法区间 `[INITIAL_BUFFER_SIZE, max_size]`。
///
/// 规则：
/// 1. 若环境变量未设置，使用 [`DEFAULT_SHRINK_SIZE`]；
/// 2. 上限取 `max_size`（不允许收缩阈值大于最大消息体——否则永不触发收缩）；
/// 3. 下限取 [`INITIAL_BUFFER_SIZE`]（小于该值会把"收缩"等价于"扩大"，违反语义）。
///
/// 该函数 **纯函数**、易测试，单独抽离便于覆盖各类极端入参。
fn resolve_shrink_message_size(max_size: usize, env_shrink_size: Option<usize>) -> usize {
    let configured_size = env_shrink_size.unwrap_or(DEFAULT_SHRINK_SIZE);

    configured_size
        .min(max_size) // 不超过最大消息体
        .max(INITIAL_BUFFER_SIZE) // 不低于初始缓冲
}

// ─────────────────────────────────────────────────────────────────────────────
// === SECTION: [3] 填充状态机 —— 用枚举把"等够 N 字节失败时的 EOF 错误消息"集中表达
// ─────────────────────────────────────────────────────────────────────────────

/// 当前填充阶段，仅用于在 `fill_at_least` 内部根据语义生成精确的 EOF 错误消息。
///
/// 状态切换路径：
/// `PortNameLen -> PortNameAndHeadersLen -> FullHeader -> FullMessage { total }`
#[derive(Debug, Clone, Copy)]
enum FillStage {
    /// 还在等首 2 字节的 path_len。若此时连接被关闭：
    /// - 缓冲为空 → `"connection closed"`（对端正常关闭，无入站请求）；
    /// - 缓冲非空 → `"incomplete message header"`（开头几个字节后就断）。
    PortNameLen,
    /// 已知 path_len，正等 path 与 headers_len。EOF 一律视为头部不完整。
    PortNameAndHeadersLen,
    /// 已知 headers_len，正等 headers 与 payload_len（即整个帧头完整）。EOF 视为头部不完整。
    FullHeader,
    /// 帧头完整、已知 payload 全长 `total`，正等剩余 payload。EOF 给出"期望 N 实际 M"。
    FullMessage { total: usize },
}

impl FillStage {
    /// 把当前阶段与"读到 0 字节"的事实翻译成具体 [`io::Error`]。
    fn into_eof_error(self, buffered_now: usize) -> io::Error {
        match self {
            FillStage::PortNameLen if buffered_now == 0 => {
                io::Error::new(io::ErrorKind::UnexpectedEof, "connection closed")
            }
            FillStage::PortNameLen
            | FillStage::PortNameAndHeadersLen
            | FillStage::FullHeader => {
                io::Error::new(io::ErrorKind::UnexpectedEof, "incomplete message header")
            }
            FillStage::FullMessage { total } => io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "incomplete message: expected {} bytes, got {}",
                    total, buffered_now
                ),
            ),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// === SECTION: [4] 解码器主体
// ─────────────────────────────────────────────────────────────────────────────

/// Zero-copy streaming decoder that reuses buffers
///
/// This decoder maintains an internal buffer and only allocates when necessary.
/// Messages are returned as Arc-counted Bytes slices, making cloning extremely cheap.
/// The reusable buffer resets back to INITIAL_BUFFER_SIZE only when unread data
/// is empty and capacity exceeds PGD_TCP_SHRINK_MESSAGE_SIZE.
pub struct ZeroCopyTcpDecoder {
    /// Reusable read buffer - grows as needed, shrinks when empty and oversized
    read_buffer: BytesMut,
    /// Maximum allowed message size
    max_message_size: usize,
    /// Threshold for shrinking buffer back to initial size when empty
    shrink_threshold: usize,
}

impl ZeroCopyTcpDecoder {
    /// Create a new decoder with default buffer size
    pub fn new() -> Self {
        Self::with_capacity(INITIAL_BUFFER_SIZE)
    }

    /// Create a new decoder with specific initial capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            read_buffer: BytesMut::with_capacity(capacity),
            max_message_size: get_tcp_max_message_size(),
            shrink_threshold: get_shrink_message_size(),
        }
    }

    /// Read one complete message with ZERO copies
    ///
    /// This method:
    /// 1. Ensures headers are buffered
    /// 2. Parses headers in-place (no allocation)
    /// 3. Ensures entire message is buffered
    /// 4. Splits off exact message size (zero-copy pointer arithmetic)
    /// 5. Returns Arc-counted Bytes (cheap to clone)
    pub async fn read_message<R: AsyncRead + Unpin>(
        &mut self,
        reader: &mut R,
    ) -> io::Result<TcpRequestMessageZeroCopy> {
        // ── Stage 1：等到至少读出 path_len（前 2 字节） ──
        self.fill_at_least(reader, super::TCP_REQUEST_ENDPOINT_LEN_WIDTH, FillStage::PortNameLen)
            .await?;
        let path_len = tcp_request_portname_len(&self.read_buffer)?;

        // ── Stage 2：等到 path + headers_len 全部到达 ──
        let initial_header_size =
            super::TCP_REQUEST_ENDPOINT_LEN_WIDTH + path_len + super::TCP_REQUEST_HEADERS_LEN_WIDTH;
        self.fill_at_least(reader, initial_header_size, FillStage::PortNameAndHeadersLen)
            .await?;
        let headers_len = tcp_request_headers_len(&self.read_buffer, path_len)?;

        // ── Stage 3：等到完整帧头（含 payload_len） ──
        let full_header_size = tcp_request_header_size(path_len, headers_len);
        self.fill_at_least(reader, full_header_size, FillStage::FullHeader)
            .await?;

        let parsed = parse_tcp_request_frame_header(&self.read_buffer)?;

        // ── 帧总长度上限校验（先于等待 payload，免得对端故意发巨大长度撑爆缓冲） ──
        check_tcp_request_max_message_size(parsed.total_len, self.max_message_size)?;

        // ── Stage 4：等到整条帧（含 payload）落地 ──
        self.fill_at_least(
            reader,
            parsed.total_len,
            FillStage::FullMessage { total: parsed.total_len },
        )
        .await?;

        // 真正切出该帧 —— 仅是指针/长度的拆分，无内存拷贝。
        let message_bytes = self.read_buffer.split_to(parsed.total_len).freeze();

        // 必要时收缩读缓冲，避免一次大请求长期占住大段内存。
        self.try_shrink_after_split();

        Ok(TcpRequestMessageZeroCopy::new(message_bytes, parsed))
    }

    /// Get the current buffer capacity
    pub fn buffer_capacity(&self) -> usize {
        self.read_buffer.capacity()
    }

    /// Get the current buffered data size
    pub fn buffered_len(&self) -> usize {
        self.read_buffer.len()
    }

    // ── 内部私有 helper（不暴露） ────────────────────────────────────────────

    /// 反复 `read_buf` 直到 `self.read_buffer.len() >= need`；若中途读到 0 字节
    /// （EOF），按当前 `stage` 翻译成精确的 [`io::Error`] 返回。
    ///
    /// 将旧实现中 4 处几乎完全相同的循环统一收敛至此，行为完全等价但易审计、易测试。
    async fn fill_at_least<R: AsyncRead + Unpin>(
        &mut self,
        reader: &mut R,
        need: usize,
        stage: FillStage,
    ) -> io::Result<()> {
        while self.read_buffer.len() < need {
            let n = reader.read_buf(&mut self.read_buffer).await?;
            if n == 0 {
                return Err(stage.into_eof_error(self.read_buffer.len()));
            }
        }
        Ok(())
    }

    /// 帧切出后判定是否需要把读缓冲收回初始容量。
    ///
    /// 触发条件（两者皆需）：
    /// 1. `read_buffer` 已被消费空（避免把后续 pipeline 数据误丢）；
    /// 2. 当前容量超过 `shrink_threshold`（足够大才值得重建——避免抖动）。
    #[inline]
    fn try_shrink_after_split(&mut self) {
        if self.read_buffer.is_empty() && self.read_buffer.capacity() > self.shrink_threshold {
            self.read_buffer = BytesMut::with_capacity(INITIAL_BUFFER_SIZE);
        }
    }
}

impl Default for ZeroCopyTcpDecoder {
    fn default() -> Self {
        Self::new()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// === SECTION: [5] 零拷贝消息视图
// ─────────────────────────────────────────────────────────────────────────────

/// Zero-copy message representation
///
/// This struct holds an Arc-counted Bytes buffer containing the entire message.
/// All accessors return zero-copy slices or references into this buffer.
#[derive(Clone)]
pub struct TcpRequestMessageZeroCopy {
    /// Entire message as Arc-counted buffer
    /// Format: [path_len(2)][path(var)][headers_len(2)][headers(var)][payload_len(4)][payload(var)]
    raw: Bytes,
    parsed: super::TcpRequestWireHeader,
}

impl TcpRequestMessageZeroCopy {
    /// Create a new zero-copy message from raw bytes
    fn new(raw: Bytes, parsed: super::TcpRequestWireHeader) -> Self {
        Self { raw, parsed }
    }

    /// Get portname path as a string slice (zero-copy)
    ///
    /// This returns a reference into the message buffer, no allocation.
    pub fn portname_path(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(self.portname_path_bytes())
    }

    /// Get portname path as bytes (zero-copy)
    pub fn portname_path_bytes(&self) -> &[u8] {
        &self.raw[self.parsed.portname_start()..self.parsed.portname_end()]
    }

    /// Get headers as bytes (zero-copy)
    pub fn headers_bytes(&self) -> &[u8] {
        &self.raw[self.parsed.headers_start()..self.parsed.headers_end()]
    }

    /// Get headers as a HashMap (requires parsing)
    pub fn headers(&self) -> std::collections::HashMap<String, String> {
        let headers_bytes = self.headers_bytes();
        if headers_bytes.is_empty() {
            return std::collections::HashMap::new();
        }

        // Parse headers from JSON format
        serde_json::from_slice(headers_bytes).unwrap_or_default()
    }

    /// Get the payload length
    #[inline]
    fn payload_len(&self) -> usize {
        self.parsed.payload_len
    }

    /// Get payload as zero-copy Bytes
    ///
    /// This returns an Arc-counted slice of the message buffer.
    /// Cloning the returned Bytes is extremely cheap (just Arc clone).
    pub fn payload(&self) -> Bytes {
        self.raw.slice(self.parsed.payload_start()..) // ZERO COPY! Just Arc clone + offset
    }

    /// Get total message size in bytes
    pub fn total_size(&self) -> usize {
        self.raw.len()
    }

    /// Get the raw message bytes (for debugging)
    pub fn raw_bytes(&self) -> &Bytes {
        &self.raw
    }
}

impl std::fmt::Debug for TcpRequestMessageZeroCopy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpRequestMessageZeroCopy")
            .field("total_size", &self.total_size())
            .field("portname_path", &self.portname_path().ok())
            .field("payload_len", &self.payload_len())
            .finish()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// === SECTION: [6] 测试
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    //! ## 测试矩阵
    //!
    //! | 测试名 | 覆盖维度 |
    //! |---|---|
    //! | `test_resolve_shrink_message_size_edge_cases` | （lib-copy）阈值在 `[INITIAL_BUFFER_SIZE, max_size]` 的夹紧逻辑 |
    //! | `test_resolve_shrink_message_size_below_initial_buffer` | env=0 → 抬升到 `INITIAL_BUFFER_SIZE`（边界） |
    //! | `test_decoder_default_equals_new_initial_state` | `Default::default()` 与 `new()` 等价（契约面） |
    //! | `test_buffered_len_zero_after_full_read` | 完整读完后 `buffered_len()` 归零（不变式） |
    //! | `test_zero_copy_decoder_basic` | （lib-copy）单帧 happy path |
    //! | `test_zero_copy_decoder_allows_empty_and_long_portname_paths` | （lib-copy）空 path 与 2 KiB path 边界 |
    //! | `test_zero_copy_decoder_large_payload` | （lib-copy）200 KiB payload，触发 buffer grow |
    //! | `test_zero_copy_decoder_total_size_limit` | （lib-copy）`max_message_size` 越界 → `InvalidData` |
    //! | `test_zero_copy_decoder_with_headers` | （lib-copy）JSON headers 解析 + `headers_bytes()` 视图 |
    //! | `test_zero_copy_decoder_empty_vs_populated_headers` | （lib-copy）同一 decoder 连续两帧、headers 形态切换 |
    //! | `test_zero_copy_decoder_buffer_shrinking` | （lib-copy）大帧读后缓冲收缩到 `INITIAL_BUFFER_SIZE` |
    //! | `test_read_message_eof_at_zero_bytes` | 对端未发任何字节就关闭 → `"connection closed"` |
    //! | `test_read_message_eof_mid_prefix` | 仅发 1 字节后断 → `"incomplete message header"` |
    //! | `test_read_message_eof_mid_payload` | 帧头完整但 payload 截断 → `"incomplete message: expected … got …"` |
    //! | `test_headers_invalid_json_returns_empty_map` | 非法 JSON headers → `headers()` 返回空 map（不抛错） |
    //! | `test_payload_is_zero_copy_slice_of_raw` | `payload()` 与 `raw_bytes()` 共享同一底层 buffer |
    //! | `test_portname_path_utf8_error_propagates` | 非 UTF-8 portname → `portname_path()` 返回 Err |
    //! | `test_debug_impl_contains_total_size_and_portname` | `Debug` 输出含 `total_size` / `portname_path` 字段（可观测性） |
    //! | `test_read_message_fragmented_byte_by_byte` | 输入按 1 字节切片分多次到达 —— 状态机鲁棒性 |

    use super::*;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncWriteExt, ReadBuf};

    // ── 测试辅助：构造一帧合法 TCP 请求字节序列 ────────────────────────────────

    fn build_frame(portname: &str, headers: &[u8], payload: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(portname.len() as u16).to_be_bytes());
        buf.extend_from_slice(portname.as_bytes());
        buf.extend_from_slice(&(headers.len() as u16).to_be_bytes());
        buf.extend_from_slice(headers);
        buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(payload);
        buf
    }

    /// 单字节挤牙膏的 `AsyncRead` 包装，用于覆盖"碎片化输入下状态机仍正确"。
    struct ByteByByteReader<'a> {
        data: &'a [u8],
        pos: usize,
    }

    impl<'a> AsyncRead for ByteByByteReader<'a> {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let me = self.get_mut();
            if me.pos < me.data.len() && buf.remaining() > 0 {
                buf.put_slice(&me.data[me.pos..me.pos + 1]);
                me.pos += 1;
            }
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn test_resolve_shrink_message_size_edge_cases() {
        // Test case: max_size = 10MB (larger than DEFAULT_SHRINK_SIZE)
        let max_size_10mb = 10 * 1024 * 1024;
        let result = resolve_shrink_message_size(max_size_10mb, None);
        assert_eq!(
            result, DEFAULT_SHRINK_SIZE,
            "10MB max should return default 8MB"
        );

        // Test case: max_size < DEFAULT_SHRINK_SIZE
        let max_size_1mb = 1024 * 1024;
        let result = resolve_shrink_message_size(max_size_1mb, None);
        assert_eq!(result, max_size_1mb, "1MB max should be capped to 1MB");

        // Test case: max_size = DEFAULT_SHRINK_SIZE
        let result = resolve_shrink_message_size(DEFAULT_SHRINK_SIZE, None);
        assert_eq!(
            result, DEFAULT_SHRINK_SIZE,
            "exact match should return default"
        );

        // Test case: env_shrink_size provided and within bounds
        let env_size = 2 * 1024 * 1024;
        let result = resolve_shrink_message_size(max_size_10mb, Some(env_size));
        assert_eq!(result, env_size, "env var should be used when within bounds");

        // Test case: env_shrink_size exceeds max_size
        let env_size_large = 20 * 1024 * 1024;
        let result = resolve_shrink_message_size(max_size_10mb, Some(env_size_large));
        assert_eq!(
            result, max_size_10mb,
            "env var should be capped to max_size"
        );

        // Test case: env_shrink_size below INITIAL_BUFFER_SIZE
        let env_size_small = 100 * 1024;
        let result = resolve_shrink_message_size(max_size_10mb, Some(env_size_small));
        assert_eq!(
            result, INITIAL_BUFFER_SIZE,
            "env var should be clamped to INITIAL_BUFFER_SIZE"
        );

        // Test case: max_size below INITIAL_BUFFER_SIZE
        let max_size_small = 100 * 1024;
        let result = resolve_shrink_message_size(max_size_small, None);
        assert_eq!(
            result, INITIAL_BUFFER_SIZE,
            "result should be clamped to INITIAL_BUFFER_SIZE"
        );
    }

    #[test]
    fn test_resolve_shrink_message_size_below_initial_buffer() {
        // env 显式为 0：应被抬升回 INITIAL_BUFFER_SIZE
        let r = resolve_shrink_message_size(10 * 1024 * 1024, Some(0));
        assert_eq!(r, INITIAL_BUFFER_SIZE);
        // env = INITIAL_BUFFER_SIZE - 1：恰好低于下限
        let r = resolve_shrink_message_size(10 * 1024 * 1024, Some(INITIAL_BUFFER_SIZE - 1));
        assert_eq!(r, INITIAL_BUFFER_SIZE);
        // env == INITIAL_BUFFER_SIZE：恰好等于下限，保留原值
        let r = resolve_shrink_message_size(10 * 1024 * 1024, Some(INITIAL_BUFFER_SIZE));
        assert_eq!(r, INITIAL_BUFFER_SIZE);
    }

    #[test]
    fn test_decoder_default_equals_new_initial_state() {
        let a = ZeroCopyTcpDecoder::default();
        let b = ZeroCopyTcpDecoder::new();
        assert_eq!(a.buffered_len(), 0);
        assert_eq!(b.buffered_len(), 0);
        assert!(a.buffer_capacity() >= INITIAL_BUFFER_SIZE);
        assert!(b.buffer_capacity() >= INITIAL_BUFFER_SIZE);
        assert_eq!(a.max_message_size, b.max_message_size);
        assert_eq!(a.shrink_threshold, b.shrink_threshold);
    }

    #[tokio::test]
    async fn test_buffered_len_zero_after_full_read() {
        let frame = build_frame("p", &[], b"abc");
        let mut decoder = ZeroCopyTcpDecoder::new();
        let mut reader = &frame[..];
        let _msg = decoder.read_message(&mut reader).await.unwrap();
        assert_eq!(decoder.buffered_len(), 0, "split_to 应消费掉整个帧");
    }

    #[tokio::test]
    async fn test_zero_copy_decoder_basic() {
        let portname = "test/portname";
        let payload = b"Hello, World!";
        let message = build_frame(portname, &[], payload);

        let mut reader = &message[..];
        let mut decoder = ZeroCopyTcpDecoder::new();
        let msg = decoder.read_message(&mut reader).await.unwrap();

        assert_eq!(msg.portname_path().unwrap(), portname);
        assert_eq!(msg.payload().as_ref(), payload);
        assert_eq!(msg.total_size(), message.len());
        assert_eq!(msg.headers().len(), 0);
    }

    #[tokio::test]
    async fn test_zero_copy_decoder_allows_empty_and_long_portname_paths() {
        for portname in [String::new(), "x".repeat(2048)] {
            let payload = b"payload";
            let message = build_frame(portname.as_str(), &[], payload);

            let mut reader = &message[..];
            let mut decoder = ZeroCopyTcpDecoder::new();
            let msg = decoder.read_message(&mut reader).await.unwrap();

            assert_eq!(msg.portname_path().unwrap(), portname.as_str());
            assert_eq!(msg.payload().as_ref(), payload);
        }
    }

    #[tokio::test]
    async fn test_zero_copy_decoder_large_payload() {
        let portname = "large/portname";
        let payload = vec![0x42u8; 200 * 1024];
        let message = build_frame(portname, &[], &payload);

        let mut reader = &message[..];
        let mut decoder = ZeroCopyTcpDecoder::new();
        let msg = decoder.read_message(&mut reader).await.unwrap();

        assert_eq!(msg.portname_path().unwrap(), portname);
        assert_eq!(msg.payload().len(), payload.len());
    }

    #[tokio::test]
    async fn test_zero_copy_decoder_total_size_limit() {
        let max_size = 1024;
        let mut decoder = ZeroCopyTcpDecoder::with_capacity(256);
        decoder.max_message_size = max_size;

        let portname = "test/portname";
        let payload = vec![0x42u8; max_size]; // 单独 payload 已等于 max
        let message = build_frame(portname, &[], &payload);

        // total_len = 2 + 13 + 2 + 0 + 4 + 1024 = 1045 > 1024
        let mut reader = &message[..];
        let err = decoder.read_message(&mut reader).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        let msg = err.to_string();
        assert!(msg.contains("message too large"));
        assert!(msg.contains("1045"));
        assert!(msg.contains("1024"));
    }

    #[tokio::test]
    async fn test_zero_copy_decoder_with_headers() {
        let portname = "api/v1/inference";
        let payload = b"Request payload data";

        let mut headers_map = std::collections::HashMap::new();
        headers_map.insert("traceparent".to_string(), "00-abc123-def456-01".to_string());
        headers_map.insert("user-agent".to_string(), "test-client/1.0".to_string());
        headers_map.insert("request-id".to_string(), "req-12345".to_string());

        let headers_json = serde_json::to_vec(&headers_map).unwrap();
        let message = build_frame(portname, &headers_json, payload);

        let mut reader = &message[..];
        let mut decoder = ZeroCopyTcpDecoder::new();
        let msg = decoder.read_message(&mut reader).await.unwrap();

        assert_eq!(msg.portname_path().unwrap(), portname);
        assert_eq!(msg.payload().as_ref(), payload);
        assert_eq!(msg.total_size(), message.len());

        let decoded_headers = msg.headers();
        assert_eq!(decoded_headers.len(), 3);
        assert_eq!(
            decoded_headers.get("traceparent").unwrap(),
            "00-abc123-def456-01"
        );
        assert_eq!(
            decoded_headers.get("user-agent").unwrap(),
            "test-client/1.0"
        );
        assert_eq!(decoded_headers.get("request-id").unwrap(), "req-12345");

        assert_eq!(msg.headers_bytes(), &headers_json[..]);
    }

    #[tokio::test]
    async fn test_zero_copy_decoder_empty_vs_populated_headers() {
        let portname = "test/portname";
        let payload = b"test data";

        // Round 1：empty headers
        let message_empty = build_frame(portname, &[], payload);
        let mut decoder = ZeroCopyTcpDecoder::new();
        let mut reader = &message_empty[..];
        let msg = decoder.read_message(&mut reader).await.unwrap();
        assert_eq!(msg.portname_path().unwrap(), portname);
        assert_eq!(msg.payload().as_ref(), payload);
        assert_eq!(msg.headers().len(), 0);
        assert_eq!(msg.headers_bytes().len(), 0);

        // Round 2：populated headers，同一 decoder 复用
        let mut headers_map = std::collections::HashMap::new();
        headers_map.insert("x-test-header".to_string(), "test-value".to_string());
        let headers_json = serde_json::to_vec(&headers_map).unwrap();
        let message_with_headers = build_frame(portname, &headers_json, payload);
        let mut reader = &message_with_headers[..];
        let msg = decoder.read_message(&mut reader).await.unwrap();
        assert_eq!(msg.portname_path().unwrap(), portname);
        assert_eq!(msg.payload().as_ref(), payload);
        assert_eq!(msg.headers().len(), 1);
        assert_eq!(msg.headers().get("x-test-header").unwrap(), "test-value");
    }

    #[tokio::test]
    async fn test_zero_copy_decoder_buffer_shrinking() {
        let portname = "test/portname";
        let small_payload = b"small";
        let large_payload = vec![0x42u8; 1024 * 1024]; // 1 MiB

        let mut decoder = ZeroCopyTcpDecoder::with_capacity(INITIAL_BUFFER_SIZE);
        decoder.max_message_size = 2 * 1024 * 1024;
        decoder.shrink_threshold = 512 * 1024;

        assert!(decoder.buffer_capacity() <= INITIAL_BUFFER_SIZE);

        let large_message = build_frame(portname, &[], &large_payload);
        let mut reader = &large_message[..];
        decoder.read_message(&mut reader).await.unwrap();

        assert!(
            decoder.buffer_capacity() <= INITIAL_BUFFER_SIZE,
            "buffer should shrink after large message, got capacity {}",
            decoder.buffer_capacity()
        );
        assert_eq!(decoder.buffered_len(), 0, "buffer should be empty after read");

        let small_message = build_frame(portname, &[], small_payload);
        let mut reader = &small_message[..];
        let msg = decoder.read_message(&mut reader).await.unwrap();
        assert_eq!(msg.payload().as_ref(), small_payload);
    }

    // ── 新增：EOF 错误语义 ──────────────────────────────────────────────────

    #[tokio::test]
    async fn test_read_message_eof_at_zero_bytes() {
        // 通过 duplex：写端立刻关闭、读端尚未收到任何字节
        let (mut w, mut r) = tokio::io::duplex(64);
        w.shutdown().await.unwrap();
        drop(w);

        let mut decoder = ZeroCopyTcpDecoder::new();
        let err = decoder.read_message(&mut r).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(err.to_string(), "connection closed");
    }

    #[tokio::test]
    async fn test_read_message_eof_mid_prefix() {
        // 只发 1 个字节就关闭：path_len 都没拼齐
        let (mut w, mut r) = tokio::io::duplex(64);
        w.write_all(&[0u8]).await.unwrap();
        w.shutdown().await.unwrap();
        drop(w);

        let mut decoder = ZeroCopyTcpDecoder::new();
        let err = decoder.read_message(&mut r).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(err.to_string(), "incomplete message header");
    }

    #[tokio::test]
    async fn test_read_message_eof_mid_payload() {
        // 帧头完整，但 payload 只发了一半
        let portname = "ep";
        let payload = b"FULL-PAYLOAD";
        let frame = build_frame(portname, &[], payload);
        // 只发前 N 字节（截掉 payload 末尾几个字节）
        let truncated = &frame[..frame.len() - 4];

        let (mut w, mut r) = tokio::io::duplex(128);
        w.write_all(truncated).await.unwrap();
        w.shutdown().await.unwrap();
        drop(w);

        let mut decoder = ZeroCopyTcpDecoder::new();
        let err = decoder.read_message(&mut r).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        let msg = err.to_string();
        assert!(msg.contains("incomplete message"), "got: {msg}");
        assert!(msg.contains(&format!("expected {}", frame.len())), "got: {msg}");
    }

    // ── 新增：消息视图的语义 ────────────────────────────────────────────────

    #[tokio::test]
    async fn test_headers_invalid_json_returns_empty_map() {
        // 非法 JSON header 字节：headers() 应回退到空 map（不抛错）
        let portname = "ep";
        let bad_headers = b"\xff\xff\xff not-json"; // 非法 UTF-8 / 非法 JSON
        let payload = b"x";
        let frame = build_frame(portname, bad_headers, payload);

        let mut reader = &frame[..];
        let mut decoder = ZeroCopyTcpDecoder::new();
        let msg = decoder.read_message(&mut reader).await.unwrap();
        assert_eq!(msg.headers_bytes(), bad_headers);
        assert!(msg.headers().is_empty());
    }

    #[tokio::test]
    async fn test_payload_is_zero_copy_slice_of_raw() {
        // payload() 是 raw 的 Bytes::slice：指针应落在 raw 区间内、长度匹配 payload_len
        let portname = "ep";
        let payload = b"PAYLOAD-XYZ";
        let frame = build_frame(portname, &[], payload);
        let mut decoder = ZeroCopyTcpDecoder::new();
        let mut reader = &frame[..];
        let msg = decoder.read_message(&mut reader).await.unwrap();

        let raw = msg.raw_bytes();
        let p = msg.payload();
        // 长度匹配
        assert_eq!(p.len(), payload.len());
        assert_eq!(p.as_ref(), payload);
        // 偏移正确：raw.len() - payload.len() 应等于 payload_start
        assert_eq!(raw.len() - p.len(), msg.parsed.payload_start());
        // 共享同一底层 Arc：raw.slice(..) 不应拷贝
        let p_ptr = p.as_ptr() as usize;
        let raw_ptr = raw.as_ptr() as usize;
        assert_eq!(p_ptr - raw_ptr, msg.parsed.payload_start());
    }

    #[tokio::test]
    async fn test_portname_path_utf8_error_propagates() {
        // 构造合法长度但非 UTF-8 的 portname 字节
        let bad_endpoint = [0xffu8, 0xfe, 0xfd];
        let mut frame = Vec::new();
        frame.extend_from_slice(&(bad_endpoint.len() as u16).to_be_bytes());
        frame.extend_from_slice(&bad_endpoint);
        frame.extend_from_slice(&(0u16).to_be_bytes());
        frame.extend_from_slice(&(0u32).to_be_bytes());

        let mut decoder = ZeroCopyTcpDecoder::new();
        let mut reader = &frame[..];
        let msg = decoder.read_message(&mut reader).await.unwrap();
        assert_eq!(msg.portname_path_bytes(), &bad_endpoint);
        assert!(msg.portname_path().is_err());
    }

    #[tokio::test]
    async fn test_debug_impl_contains_total_size_and_portname() {
        let portname = "dbg/ep";
        let payload = b"d";
        let frame = build_frame(portname, &[], payload);
        let mut decoder = ZeroCopyTcpDecoder::new();
        let mut reader = &frame[..];
        let msg = decoder.read_message(&mut reader).await.unwrap();

        let s = format!("{:?}", msg);
        assert!(s.contains("TcpRequestMessageZeroCopy"));
        assert!(s.contains("total_size"));
        assert!(s.contains("portname_path"));
        assert!(s.contains(&format!("{}", frame.len())));
    }

    // ── 新增：碎片化输入下的状态机鲁棒性 ────────────────────────────────────

    #[tokio::test]
    async fn test_read_message_fragmented_byte_by_byte() {
        let portname = "fragmented/path";
        let mut headers_map = std::collections::HashMap::new();
        headers_map.insert("k".to_string(), "v".to_string());
        let headers_json = serde_json::to_vec(&headers_map).unwrap();
        let payload = b"HELLO-FRAGMENTED-WORLD";

        let frame = build_frame(portname, &headers_json, payload);
        let mut reader = ByteByByteReader { data: &frame, pos: 0 };

        let mut decoder = ZeroCopyTcpDecoder::new();
        let msg = decoder.read_message(&mut reader).await.unwrap();
        assert_eq!(msg.portname_path().unwrap(), portname);
        assert_eq!(msg.payload().as_ref(), payload);
        assert_eq!(msg.headers().get("k").unwrap(), "v");
        assert_eq!(msg.total_size(), frame.len());
    }
}
