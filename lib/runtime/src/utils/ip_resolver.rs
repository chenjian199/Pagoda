// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 设计意图
//! 给运行时所有需要「对外宣告本机地址」的子系统提供统一入口：HTTP RPC、
//! TCP RPC、TLS 注册都从这里取到一个稳定的 host 字符串，避免每个调用点
//! 各自实现 IPv4/IPv6/loopback 选择策略。
//!
//! # 外部契约
//! - `get_local_ip_for_advertise()` / `get_local_ip_for_advertise_with_resolver(R)`：
//!   返回 `String`，IPv6 自动套 `[]`；
//! - `get_http_rpc_host()` / `get_http_rpc_host_with_resolver(R)`：HTTP 用 host；
//! - `get_http_rpc_host_from_env()` / `get_tcp_rpc_host_from_env()`：
//!   优先读取对应 `*_HOST` 环境变量，缺失时回退到本机 IP；
//! - 任意失败都不会向调用方抛错，而是退化为 `127.0.0.1`。
//!
//! # 实现要点
//! - 解析顺序：IPv4 → IPv6（仅 `LocalIpAddressNotFound` 触发回退）→ loopback；
//! - `format_ip_for_url` 统一处理 IPv6 的方括号；
//! - 解析逻辑抽成泛型 `with_resolver` 版本，方便测试注入 mock `IpResolver`。

use crate::pipeline::network::tcp::server::{DefaultIpResolver, IpResolver};
use local_ip_address::Error;
use std::net::IpAddr;

// === SECTION: 内部辅助 ===

/// 使用给定解析器获取本地 IP，并按 IPv4 -> IPv6 -> 回环地址 的顺序回退。
fn resolve_local_ip_with_resolver<R: IpResolver>(resolver: R) -> IpAddr {
    let primary = resolver.local_ip();
    let candidate = match primary {
        Ok(addr) => Ok(addr),
        Err(Error::LocalIpAddressNotFound) => resolver.local_ipv6(),
        Err(err) => Err(err),
    };

    match candidate {
        Ok(addr) => addr,
        Err(_) => IpAddr::from([127, 0, 0, 1]),
    }
}

/// 将 IP 地址格式化为可安全拼接到 URL 中的主机字符串。
///
/// 对 IPv6 会自动补上方括号，IPv4 则直接转字符串。
fn format_ip_for_url(addr: IpAddr) -> String {
    if matches!(addr, IpAddr::V6(_)) {
        return format!("[{addr}]");
    }

    addr.to_string()
}

/// 使用给定解析器获取用于对外广播的本地 IP。
///
/// 处理流程是先解析本机地址，失败时回退到 `127.0.0.1`，最后按 URL 安全格式输出主机字符串。
// === SECTION: 公开 API ===

pub fn get_local_ip_for_advertise_with_resolver<R: IpResolver>(resolver: R) -> String {
    let resolved = resolve_local_ip_with_resolver(resolver);
    format_ip_for_url(resolved)
}

/// 使用默认解析器获取用于对外广播的本地 IP。
pub fn get_local_ip_for_advertise() -> String {
    let resolver = DefaultIpResolver;
    get_local_ip_for_advertise_with_resolver(resolver)
}

/// 使用给定解析器获取 HTTP RPC 绑定主机。
///
/// 处理流程与广播地址一致，只是语义上用于 HTTP RPC 监听配置。
pub fn get_http_rpc_host_with_resolver<R: IpResolver>(resolver: R) -> String {
    let host = get_local_ip_for_advertise_with_resolver(resolver);
    host
}

/// 使用默认解析器获取 HTTP RPC 绑定主机。
pub fn get_http_rpc_host() -> String {
    let resolver = DefaultIpResolver;
    get_http_rpc_host_with_resolver(resolver)
}

/// 优先从环境变量读取 HTTP RPC 主机，缺失时回退到本地 IP 解析。
pub fn get_http_rpc_host_from_env() -> String {
    match std::env::var("PGD_HTTP_RPC_HOST") {
        Ok(value) => value,
        Err(_) => get_http_rpc_host(),
    }
}

/// 优先从环境变量读取 TCP RPC 主机，缺失时回退到本地 IP 解析。
pub fn get_tcp_rpc_host_from_env() -> String {
    match std::env::var("PGD_TCP_RPC_HOST") {
        Ok(value) => value,
        Err(_) => get_http_rpc_host(),
    }
}

// === SECTION: tests ===

#[cfg(test)]
mod tests {
    use super::*;
    use local_ip_address::Error;

    // 用于测试的假解析器。
    struct MockIpResolver {
        ipv4_result: Result<IpAddr, Error>,
        ipv6_result: Result<IpAddr, Error>,
    }

    impl IpResolver for MockIpResolver {
        /// 返回测试用 IPv4 解析结果。
        fn local_ip(&self) -> Result<IpAddr, Error> {
            match &self.ipv4_result {
                Ok(addr) => Ok(*addr),
                Err(Error::LocalIpAddressNotFound) => Err(Error::LocalIpAddressNotFound),
                Err(_) => Err(Error::LocalIpAddressNotFound), // Simplify for testing
            }
        }

        /// 返回测试用 IPv6 解析结果。
        fn local_ipv6(&self) -> Result<IpAddr, Error> {
            match &self.ipv6_result {
                Ok(addr) => Ok(*addr),
                Err(Error::LocalIpAddressNotFound) => Err(Error::LocalIpAddressNotFound),
                Err(_) => Err(Error::LocalIpAddressNotFound), // Simplify for testing
            }
        }
    }

    #[test]
    fn test_get_http_rpc_host_with_successful_ipv4() {
        // 测试优先返回成功解析到的 IPv4 地址。
        let resolver = MockIpResolver {
            ipv4_result: Ok(IpAddr::from([192, 168, 1, 100])),
            ipv6_result: Ok(IpAddr::from([0, 0, 0, 0, 0, 0, 0, 1])),
        };

        let result = get_http_rpc_host_with_resolver(resolver);
        assert_eq!(result, "192.168.1.100");
    }

    #[test]
    fn test_get_http_rpc_host_with_ipv4_fail_ipv6_success() {
        // 测试 IPv4 失败后会回退到 IPv6，并带上方括号。
        let resolver = MockIpResolver {
            ipv4_result: Err(Error::LocalIpAddressNotFound),
            ipv6_result: Ok(IpAddr::from([0x2001, 0xdb8, 0, 0, 0, 0, 0, 1])),
        };

        let result = get_http_rpc_host_with_resolver(resolver);
        // IPv6 地址需要带方括号，才能安全拼接 URL。
        assert_eq!(result, "[2001:db8::1]");
    }

    #[test]
    fn test_get_http_rpc_host_with_both_fail() {
        // 测试 IPv4 和 IPv6 都失败时回退到回环地址。
        let resolver = MockIpResolver {
            ipv4_result: Err(Error::LocalIpAddressNotFound),
            ipv6_result: Err(Error::LocalIpAddressNotFound),
        };

        let result = get_http_rpc_host_with_resolver(resolver);
        assert_eq!(result, "127.0.0.1");
    }

    #[test]
    fn test_get_http_rpc_host_from_env_with_env_var() {
        // 测试优先读取环境变量。
        unsafe {
            std::env::set_var("PGD_HTTP_RPC_HOST", "10.0.0.1");
        }

        let result = get_http_rpc_host_from_env();
        assert_eq!(result, "10.0.0.1");

        // 清理环境变量，避免影响后续测试。
        unsafe {
            std::env::remove_var("PGD_HTTP_RPC_HOST");
        }
    }

    #[test]
    fn test_get_http_rpc_host_from_env_without_env_var() {
        // 测试未配置环境变量时会回退到自动解析结果。

        let result = get_http_rpc_host_from_env();
        // 这里应该得到一个非空主机地址。
        assert!(!result.is_empty());

        // 去掉 IPv6 方括号后应能正常解析成 IP。
        let ip_str = result.trim_start_matches('[').trim_end_matches(']');
        let _: IpAddr = ip_str.parse().expect("Should be a valid IP address");
    }

    #[test]
    fn test_ipv6_address_is_bracketed() {
        // 测试 IPv6 地址输出时会自动带方括号。
        let resolver = MockIpResolver {
            ipv4_result: Err(Error::LocalIpAddressNotFound),
            ipv6_result: Ok(IpAddr::from([0xfd00, 0xdead, 0xbeef, 0, 0, 0, 0, 2])),
        };

        let result = get_http_rpc_host_with_resolver(resolver);
        // IPv6 必须带方括号，才能安全用于 URL 主机部分。
        assert!(result.starts_with('['), "IPv6 should start with '['");
        assert!(result.ends_with(']'), "IPv6 should end with ']'");
        assert_eq!(result, "[fd00:dead:beef::2]");
    }

    #[test]
    fn test_ipv4_address_not_bracketed() {
        // 测试 IPv4 地址输出时不会带方括号。
        let resolver = MockIpResolver {
            ipv4_result: Ok(IpAddr::from([10, 0, 0, 1])),
            ipv6_result: Err(Error::LocalIpAddressNotFound),
        };

        let result = get_http_rpc_host_with_resolver(resolver);
        // IPv4 不应该带方括号。
        assert!(!result.contains('['), "IPv4 should not contain '['");
        assert_eq!(result, "10.0.0.1");
    }

    #[test]
    fn test_get_local_ip_for_advertise_uses_same_resolution_logic() {
        // 测试对外广播地址与 HTTP 主机使用同一套解析逻辑。
        let resolver = MockIpResolver {
            ipv4_result: Ok(IpAddr::from([172, 16, 0, 5])),
            ipv6_result: Err(Error::LocalIpAddressNotFound),
        };

        assert_eq!(get_local_ip_for_advertise_with_resolver(resolver), "172.16.0.5");
    }

    #[test]
    fn test_get_tcp_rpc_host_from_env_with_env_var() {
        // 测试 TCP RPC 主机也会优先读取环境变量。
        unsafe {
            std::env::set_var("PGD_TCP_RPC_HOST", "10.1.2.3");
        }

        let result = get_tcp_rpc_host_from_env();
        assert_eq!(result, "10.1.2.3");

        unsafe {
            std::env::remove_var("PGD_TCP_RPC_HOST");
        }
    }
}
