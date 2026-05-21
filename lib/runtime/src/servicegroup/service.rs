// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! NATS micro service 构建与注册。
//!
//! 每个 ServiceGroup 在 NATS 请求平面模式下需要注册一个 micro service，
//! 框架通过 `$SRV.PING`、`$SRV.INFO`、`$SRV.STATS` 等 subject 自动响应运维查询。

use crate::servicegroup::ServiceGroup;

pub const PROJECT_NAME: &str = "Pagoda";
const SERVICE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// 构建并注册 NATS micro service。
///
/// - `nats_client`：NATS 连接
/// - `servicegroup`：所属服务组（提供命名信息）
/// - `description`：可选描述字符串
///
/// 注册成功后将 `async_nats::service::Service` 存入 DRT 的 `servicegroup_registry`。
pub async fn build_nats_service(
    nats_client: &crate::transports::nats::Client,
    servicegroup: &ServiceGroup,
    description: Option<String>,
) -> anyhow::Result<async_nats::service::Service> {
    // 服务名："{project}.{namespace}.{servicegroup}"
    let service_name = format!(
        "{}.{}.{}",
        PROJECT_NAME,
        servicegroup.namespace().name(),
        servicegroup.name(),
    );

    let desc = description.unwrap_or_else(|| {
        format!(
            "{} servicegroup {} in namespace {}",
            PROJECT_NAME,
            servicegroup.name(),
            servicegroup.namespace().name(),
        )
    });
    let config = async_nats::service::Config {
        name: service_name.clone(),
        version: SERVICE_VERSION.to_string(),
        description: Some(desc),
        metadata: Some(std::collections::HashMap::from([
            ("namespace".to_string(), servicegroup.namespace().name().to_string()),
            ("servicegroup".to_string(), servicegroup.name().to_string()),
        ])),
        stats_handler: None,
        queue_group: None,
    };

    use async_nats::service::ServiceExt;
    let service = nats_client
        .inner()
        .add_service(config)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create NATS micro service '{service_name}': {e}"))?;

    tracing::info!(
        service_name = %service_name,
        version = %SERVICE_VERSION,
        "NATS micro service registered"
    );

    Ok(service)
}

/// 构建 NATS micro service 并注册到 DRT 的 Registry。
pub async fn register_nats_service(
    nats_client: &crate::transports::nats::Client,
    servicegroup: &ServiceGroup,
    description: Option<String>,
) -> anyhow::Result<()> {
    use crate::traits::DistributedRuntimeProvider;

    let drt = servicegroup.drt();
    let ns_name = servicegroup.namespace().name().to_string();
    let sg_name = servicegroup.name().to_string();
    let registry_key = format!("{ns_name}/{sg_name}");

    // 幂等：已注册则跳过
    if drt.servicegroup_registry().contains(&registry_key).await {
        tracing::debug!(key = %registry_key, "NATS service already registered");
        return Ok(());
    }

    let service = build_nats_service(nats_client, servicegroup, description).await?;
    drt.servicegroup_registry().register(registry_key, service).await;
    Ok(())
}
