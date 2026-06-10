// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::str::FromStr;

use anyhow::Result;
use pagoda_runtime::{
    distributed::{DistributedConfig, RequestPlaneMode},
    transports::{etcd, nats},
    utils::{get_http_rpc_host_from_env, get_tcp_rpc_host_from_env},
};
use temp_env::async_with_vars;

mod common;
use common::contract::{acquire_contract_test_lock, unique_name};

// 目的/场景：运行时配置从环境变量读取并覆盖默认值（公共配置入口）。
//
// 生产逻辑：`DistributedConfig::from_settings` 解析 `DYN_REQUEST_PLANE`；
// `get_tcp_rpc_host_from_env` 读取 `DYN_TCP_RPC_HOST`（`distributed.rs` / `ip_resolver.rs`）。
//
// 测试计划：`temp-env` 设置 http + 自定义 TCP host → 读取配置字段。
//
// 关键断言：`request_plane == Http`；TCP host 字符串匹配 env。
#[tokio::test]
async fn runtime_config_reads_env_over_defaults() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    async_with_vars(
        [
            ("PGD_REQUEST_PLANE", Some("http")),
            ("PGD_TCP_RPC_HOST", Some("10.0.0.5")),
            ("PGD_DISCOVERY_BACKEND", Some("mem")),
        ],
        async {
            let config = DistributedConfig::from_settings();
            assert_eq!(config.request_plane, RequestPlaneMode::Http);
            assert_eq!(get_tcp_rpc_host_from_env(), "10.0.0.5");
            Ok::<(), anyhow::Error>(())
        },
    )
    .await
}

// 目的/场景：非法 request plane 模式在显式解析时返回明确配置错误。
//
// 生产逻辑：`RequestPlaneMode::from_str` 对未知值返回 `Err`（`distributed.rs`）；
// `from_env` 对非法值静默回退 default，集成层验证严格解析路径。
//
// 测试计划：对非法字符串调用 `from_str`；可选读取 env 后 parse。
//
// 关键断言：`Err` 且错误信息含 `Invalid request plane`。
#[tokio::test]
async fn invalid_env_value_returns_configuration_error() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    async_with_vars([("PGD_REQUEST_PLANE", Some("not-a-valid-plane"))], async {
        let parse_direct = RequestPlaneMode::from_str("not-a-valid-plane");
        assert!(parse_direct.is_err());
        let msg = parse_direct.unwrap_err().to_string();
        assert!(
            msg.contains("Invalid request plane"),
            "unexpected error: {msg}"
        );

        let env_value = std::env::var("PGD_REQUEST_PLANE")?;
        assert!(RequestPlaneMode::from_str(&env_value).is_err());

        Ok::<(), anyhow::Error>(())
    })
    .await
}

// 目的/场景：串行契约锁 + `temp-env` 使并行测试不会互相污染进程环境。
//
// 生产逻辑：集成测试共享进程；`acquire_contract_test_lock` 串行化全局 env 变更。
//
// 测试计划：两段 `async_with_vars` 写入不同唯一 env 值 → 各段内读取应匹配本段设置。
//
// 关键断言：第一段读到 value-a；第二段读到 value-b（非 value-a）。
#[tokio::test]
async fn test_env_isolation_prevents_parallel_pollution() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    let key = format!("DYN_ITEST_ISOLATION_{}", unique_name("env"));
    let value_a = unique_name("a");
    let value_b = unique_name("b");

    async_with_vars([(key.as_str(), Some(value_a.as_str()))], async {
        assert_eq!(std::env::var(&key)?, value_a);
        Ok::<(), anyhow::Error>(())
    })
    .await?;

    async_with_vars([(key.as_str(), Some(value_b.as_str()))], async {
        assert_eq!(std::env::var(&key)?, value_b);
        assert_ne!(std::env::var(&key)?, value_a);
        Ok::<(), anyhow::Error>(())
    })
    .await?;

    Ok(())
}

// 目的/场景：HTTP/TCP advertise host 环境变量优先于自动探测。
//
// 生产逻辑：`get_http_rpc_host_from_env` / `get_tcp_rpc_host_from_env` 读取
// `DYN_HTTP_RPC_HOST` / `DYN_TCP_RPC_HOST`（`utils/ip_resolver.rs`）。
//
// 测试计划：`temp-env` 设置 HTTP 与 TCP host → 调用 env 读取函数。
//
// 关键断言：返回值与 env 字符串完全一致。
#[tokio::test]
async fn network_advertise_host_uses_env_when_set() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    async_with_vars(
        [
            ("PGD_HTTP_RPC_HOST", Some("192.168.10.1")),
            ("PGD_TCP_RPC_HOST", Some("192.168.10.2")),
        ],
        async {
            assert_eq!(get_http_rpc_host_from_env(), "192.168.10.1");
            assert_eq!(get_tcp_rpc_host_from_env(), "192.168.10.2");
            Ok::<(), anyhow::Error>(())
        },
    )
    .await
}

// 目的/场景：NATS / ETCD 认证相关环境变量进入 client options。
//
// 生产逻辑：`nats::ClientOptions::default` / `NatsAuth::default` 与
// `etcd::ClientOptions::default` 从 env 构造认证配置（`transports/nats.rs` / `etcd.rs`）。
//
// 测试计划：设置 username/password env → 构建 default options（不连接真实服务）。
//
// 关键断言：NATS options Debug 含用户名；ETCD `etcd_connect_options` 已设置。
#[tokio::test]
async fn nats_and_etcd_auth_env_are_applied() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    async_with_vars(
        [
            ("NATS_AUTH_USERNAME", Some("nats-itest-user")),
            ("NATS_AUTH_PASSWORD", Some("nats-itest-pass")),
            ("ETCD_AUTH_USERNAME", Some("etcd-itest-user")),
            ("ETCD_AUTH_PASSWORD", Some("etcd-itest-pass")),
        ],
        async {
            let nats_opts = nats::ClientOptions::builder()
                .build()
                .map_err(|e| anyhow::anyhow!("build nats options: {e}"))?;
            let nats_debug = format!("{nats_opts:?}");
            assert!(
                nats_debug.contains("nats-itest-user"),
                "NATS auth username should be read from env, got: {nats_debug}"
            );

            let etcd_opts = etcd::ClientOptions::default();
            let connect = etcd_opts
                .etcd_connect_options
                .as_ref()
                .expect("ETCD auth env should populate connect options");
            let connect_debug = format!("{connect:?}");
            assert!(
                connect_debug.contains("etcd-itest-user"),
                "ETCD auth username should be read from env, got: {connect_debug}"
            );

            Ok::<(), anyhow::Error>(())
        },
    )
    .await
}
