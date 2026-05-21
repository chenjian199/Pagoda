// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 主机名/网络接口名/IP 字符串解析为 IpAddr，以及本地通告地址构建。

use std::net::IpAddr;

// ── IpResolver trait（依赖注入，便于测试）─────────────────────────

pub trait IpResolver: Send + Sync {
    fn local_ip(&self) -> Result<IpAddr, LocalIpError>;
    fn local_ipv6(&self) -> Result<IpAddr, LocalIpError>;
}

#[derive(Debug)]
pub struct LocalIpError;

/// 默认 resolver：通过 UDP connect 探测本机出口 IP。
pub struct DefaultIpResolver;

impl IpResolver for DefaultIpResolver {
    fn local_ip(&self) -> Result<IpAddr, LocalIpError> {
        use std::net::UdpSocket;
        let socket = UdpSocket::bind("0.0.0.0:0").map_err(|_| LocalIpError)?;
        socket.connect("8.8.8.8:80").map_err(|_| LocalIpError)?;
        Ok(socket.local_addr().map_err(|_| LocalIpError)?.ip())
    }

    fn local_ipv6(&self) -> Result<IpAddr, LocalIpError> {
        use std::net::UdpSocket;
        let socket = UdpSocket::bind("[::]:0").map_err(|_| LocalIpError)?;
        socket.connect("[2001:4860:4860::8888]:80").map_err(|_| LocalIpError)?;
        Ok(socket.local_addr().map_err(|_| LocalIpError)?.ip())
    }
}

// ── 私有辅助 ─────────────────────────────────────────────────────

fn resolve_local_ip_with_resolver<R: IpResolver>(resolver: R) -> IpAddr {
    resolver.local_ip()
        .or_else(|_| resolver.local_ipv6())
        .unwrap_or(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST))
}

fn format_ip_for_url(addr: IpAddr) -> String {
    match addr {
        IpAddr::V6(v6) => format!("[{v6}]"),
        IpAddr::V4(v4) => v4.to_string(),
    }
}

/// 读取 Linux /proc/net/if_inet6 解析 IPv6 接口地址，
/// 同时通过解析 /proc/net/fib_trie 获取 IPv4 接口地址（fallback: ip addr 命令）。
fn get_if_addrs() -> Vec<(String, IpAddr)> {
    let mut result = Vec::new();

    // IPv4: 解析 /proc/net/fib_trie（LOCAL 节点）
    if let Ok(content) = std::fs::read_to_string("/proc/net/fib_trie") {
        parse_fib_trie(&content, &mut result);
    }

    // IPv6: 解析 /proc/net/if_inet6
    if let Ok(content) = std::fs::read_to_string("/proc/net/if_inet6") {
        for line in content.lines() {
            let cols: Vec<&str> = line.split_whitespace().collect();
            if cols.len() < 6 { continue; }
            let addr_hex = cols[0];
            let iface = cols[5];
            if addr_hex.len() == 32 {
                if let Ok(ip) = parse_ipv6_hex(addr_hex) {
                    result.push((iface.to_string(), IpAddr::V6(ip)));
                }
            }
        }
    }

    result
}

fn parse_fib_trie(content: &str, result: &mut Vec<(String, IpAddr)>) {
    // 简化：通过 `ip -4 addr show` 命令
    if let Ok(output) = std::process::Command::new("ip").args(["-4", "addr", "show"]).output() {
        if let Ok(s) = std::str::from_utf8(&output.stdout) {
            let _ = content; // suppress unused
            let mut current_iface = String::new();
            for line in s.lines() {
                let line = line.trim();
                if line.starts_with(|c: char| c.is_ascii_digit()) {
                    if let Some(name_part) = line.split(':').nth(1) {
                        current_iface = name_part.trim().split('@').next().unwrap_or("").to_string();
                    }
                } else if line.starts_with("inet ") {
                    if let Some(addr_part) = line.split_whitespace().nth(1) {
                        if let Some(ip_str) = addr_part.split('/').next() {
                            if let Ok(ip) = ip_str.parse::<std::net::Ipv4Addr>() {
                                result.push((current_iface.clone(), IpAddr::V4(ip)));
                            }
                        }
                    }
                }
            }
        }
    }
}

fn parse_ipv6_hex(hex: &str) -> Result<std::net::Ipv6Addr, ()> {
    if hex.len() != 32 { return Err(()); }
    let mut bytes = [0u8; 16];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let s = std::str::from_utf8(chunk).map_err(|_| ())?;
        bytes[i] = u8::from_str_radix(s, 16).map_err(|_| ())?;
    }
    Ok(std::net::Ipv6Addr::from(bytes))
}

// ── 公开 IP 解析 API ──────────────────────────────────────────────

/// 将主机名、网络接口名或 IP 字符串解析为 IpAddr。
pub fn resolve_address(input: &str) -> anyhow::Result<IpAddr> {
    if let Ok(ip) = input.parse::<IpAddr>() {
        return Ok(ip);
    }
    use std::net::ToSocketAddrs;
    if let Ok(mut addrs) = (input, 0u16).to_socket_addrs() {
        if let Some(addr) = addrs.next() {
            return Ok(addr.ip());
        }
    }
    // 尝试作为网络接口名解析
    for (iface, ip) in get_if_addrs() {
        if iface == input {
            return Ok(ip);
        }
    }
    anyhow::bail!("cannot resolve address or interface: '{input}'")
}

// ── 通告地址构建 ──────────────────────────────────────────────────

pub fn get_local_ip_for_advertise_with_resolver<R: IpResolver>(resolver: R) -> String {
    format_ip_for_url(resolve_local_ip_with_resolver(resolver))
}

pub fn get_local_ip_for_advertise() -> String {
    get_local_ip_for_advertise_with_resolver(DefaultIpResolver)
}

pub fn get_http_rpc_host_with_resolver<R: IpResolver>(resolver: R) -> String {
    format_ip_for_url(resolve_local_ip_with_resolver(resolver))
}

pub fn get_http_rpc_host() -> String {
    get_http_rpc_host_from_env()
}

pub fn get_http_rpc_host_from_env() -> String {
    std::env::var("PGD_HTTP_RPC_HOST")
        .unwrap_or_else(|_| get_http_rpc_host_with_resolver(DefaultIpResolver))
}

pub fn get_tcp_rpc_host_from_env() -> String {
    std::env::var("PGD_TCP_RPC_HOST")
        .unwrap_or_else(|_| get_local_ip_for_advertise())
}
