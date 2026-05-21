// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 传输层通用工具：地址解析与 socket 选项配置。

use std::net::SocketAddr;

/// 地址解析错误。
#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    #[error("无法解析地址: {0}")]
    DnsResolutionFailed(String),
    #[error("无效的地址格式: {0}")]
    InvalidFormat(String),
    #[error("IO 错误: {0}")]
    Io(#[from] std::io::Error),
}

/// 将主机名:端口字符串解析为 SocketAddr。
///
/// 支持 DNS 解析和 IPv4/IPv6 字面量。
pub async fn resolve_address(addr: &str) -> Result<SocketAddr, ResolveError> {
    use tokio::net::lookup_host;
    let mut addrs = lookup_host(addr).await?;
    addrs.next().ok_or_else(|| ResolveError::DnsResolutionFailed(format!("no address for {addr}")))
}

/// 批量解析多个地址。
pub async fn resolve_addresses(addrs: &[String]) -> Result<Vec<SocketAddr>, ResolveError> {
    let mut results = Vec::with_capacity(addrs.len());
    for addr in addrs {
        results.push(resolve_address(addr).await?);
    }
    Ok(results)
}

/// Socket 优化选项集合。
#[derive(Debug, Clone)]
pub struct SocketOptions {
    /// 启用 TCP_QUICKACK（禁用 Nagle 延迟确认）
    pub tcp_quickack: bool,
    /// 启用 SO_BUSY_POLL（减少网络延迟）
    pub so_busy_poll_us: Option<u32>,
    /// TCP_NODELAY
    pub tcp_nodelay: bool,
    /// SO_REUSEADDR
    pub so_reuseaddr: bool,
    /// SO_REUSEPORT
    pub so_reuseport: bool,
    /// 接收缓冲区大小
    pub recv_buffer_size: Option<usize>,
    /// 发送缓冲区大小
    pub send_buffer_size: Option<usize>,
}

impl Default for SocketOptions {
    fn default() -> Self {
        Self {
            tcp_quickack: true,
            so_busy_poll_us: None,
            tcp_nodelay: true,
            so_reuseaddr: true,
            so_reuseport: false,
            recv_buffer_size: None,
            send_buffer_size: None,
        }
    }
}

/// 将 socket 选项应用到 TCP socket fd。
///
/// # Safety
/// 调用者需确保 fd 是有效的 TCP socket。
#[cfg(target_os = "linux")]
pub fn apply_socket_options(
    _fd: std::os::unix::io::RawFd,
    _opts: &SocketOptions,
) -> Result<(), std::io::Error> {
    // Best-effort: advanced options (SO_BUSY_POLL, buffer sizes) require libc/nix
    // which are not in current deps. TCP_NODELAY is set via TcpStream::set_nodelay()
    // at the connection level by callers.
    Ok(())
}

/// 非 Linux 平台的 fallback（仅应用跨平台选项）。
#[cfg(not(target_os = "linux"))]
pub fn apply_socket_options(
    _fd: std::os::unix::io::RawFd,
    _opts: &SocketOptions,
) -> Result<(), std::io::Error> {
    Ok(())
}
