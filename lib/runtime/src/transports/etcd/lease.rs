// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 设计意图
//! 提供"创建一个绑定到 cancellation token 的 etcd lease 并自动续期"的高层 API。
//! 续期逻辑必须能容忍 etcd 短暂不可用——通过 [`super::connector::Connector::reconnect`]
//! 在 deadline 之前不断重试建立 keep-alive 流；只有当 deadline 用尽或 lease 真正过期时
//! 才向上抛错并触发外层 token.cancel()。
//!
//! # 外部契约
//! - 公开入口：`create_lease(connector, ttl, token) -> Result<u64>` 同步返回 lease id，
//!   并在后台 spawn 续期任务；任务终止时若是错误路径会取消传入的 token；
//! - 私有但 *测试可见* 的辅助函数 `keep_alive`、`new_keep_alive_stream`、
//!   `keep_alive_with_stream` 的名字与签名被 supplemental 测试直接引用，不可改名；
//! - `new_keep_alive_stream` 返回 `Ok(None)` 表示在重连等待中被取消，
//!   `Err(...)` 表示无法在 deadline 前重连；`Ok(Some(_))` 表示流就绪；
//! - `keep_alive_with_stream` 返回 `Ok(false)` 表示已被取消（含撤销 lease），
//!   `Ok(true)` 表示流意外结束需要外层重新建流，`Err(...)` 表示 lease 真的过期。
//!
//! # 实现要点
//! - 续期点选在 `deadline.saturating_duration_since(now) / 2`，保证在 TTL 中点前就送心跳；
//! - 取消路径会主动调用 `revoke` 释放服务器侧资源（失败仅警告，不阻塞退出）；
//! - 所有 IO 用 `tokio::select!` 同时挂在"流消息 / 取消 / 续期定时器"三个臂上，
//!   `biased` 让"流响应"和"取消"始终优先于定时器。

use super::connector::Connector;
use etcd_client::{LeaseKeepAliveStream, LeaseKeeper};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

// === SECTION: public entry ===

/// 申请一个 TTL 为 `ttl` 秒的 etcd lease，绑定给定 token，并启动后台续期任务。
///
/// 返回 lease id（u64）；若后台续期失败，会调用 `token.cancel()` 通知上层。
pub async fn create_lease(
    connector: Arc<Connector>,
    ttl: u64,
    token: CancellationToken,
) -> anyhow::Result<u64> {
    // 申请 lease。
    let mut lease_client = connector.get_client().lease_client();
    let lease = lease_client.grant(ttl as i64, None).await?;
    let id = lease.id() as u64;
    let granted_ttl = lease.ttl() as u64;

    // 用 child_token 让后台任务能感知外部取消，同时其内部失败也能反向取消外部 token。
    let child = token.child_token();
    let outer = token;
    tokio::spawn(async move {
        match keep_alive(connector, id, granted_ttl, child).await {
            Ok(()) => tracing::trace!("keep alive task exited successfully"),
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "Unable to maintain lease. Check etcd server status"
                );
                outer.cancel();
            }
        }
    });

    Ok(id)
}

// === SECTION: keep-alive loop ===

/// 顶层续期循环：负责"建流 → 守流 → 流意外断开后重建"的反复切换。
///
/// 错误向上抛会触发 `create_lease` 中的 `outer.cancel()`。
async fn keep_alive(
    connector: Arc<Connector>,
    lease_id: u64,
    ttl: u64,
    token: CancellationToken,
) -> anyhow::Result<()> {
    // 初始 deadline 设为 lease 到期时间；后续在 `keep_alive_with_stream` 中按响应更新。
    let mut deadline = Instant::now() + Duration::from_secs(ttl);

    loop {
        let stream = new_keep_alive_stream(&connector, lease_id, &deadline, &token).await?;
        let Some((sender, receiver)) = stream else {
            // 取消触发——干净退出。
            return Ok(());
        };

        let should_reconnect = keep_alive_with_stream(
            &connector,
            sender,
            receiver,
            lease_id,
            &mut deadline,
            &token,
        )
        .await?;

        if !should_reconnect {
            return Ok(());
        }
    }
}

/// 建立 keep-alive 流；若建流失败则按 deadline 触发 reconnect。
///
/// 返回值见模块顶部"外部契约"。
async fn new_keep_alive_stream(
    connector: &Arc<Connector>,
    lease_id: u64,
    deadline: &Instant,
    token: &CancellationToken,
) -> anyhow::Result<Option<(LeaseKeeper, LeaseKeepAliveStream)>> {
    loop {
        let mut lease_client = connector.get_client().lease_client();
        match lease_client.keep_alive(lease_id as i64).await {
            Ok((sender, receiver)) => {
                tracing::debug!(lease_id, "Established keep-alive stream");
                return Ok(Some((sender, receiver)));
            }
            Err(e) => {
                tracing::warn!(
                    lease_id,
                    error = %e,
                    "Failed to establish keep-alive stream"
                );

                // 失败时并发等待 (reconnect, cancel)，谁先就绪谁决定走向。
                tokio::select! {
                    biased;

                    reconnect_result = connector.reconnect(*deadline) => {
                        if let Err(err) = reconnect_result {
                            return Err(err);
                        }
                        // 重连成功，下一轮 loop 会用新客户端重新建流。
                    }

                    _ = token.cancelled() => {
                        tracing::debug!(
                            lease_id,
                            "Cancellation token triggered during reconnection"
                        );
                        return Ok(None);
                    }
                }
            }
        }
    }
}

/// 持有已建立的 keep-alive 流，按 `deadline/2` 周期发送心跳并消费响应。
///
/// - `Ok(true)`：流被对端关闭/出错，应由外层重建（recoverable）；
/// - `Ok(false)`：检测到取消信号，已尽力 revoke 后退出；
/// - `Err(_)`：lease 已被 etcd 视为过期或已撤销（unrecoverable）。
async fn keep_alive_with_stream(
    connector: &Arc<Connector>,
    mut sender: LeaseKeeper,
    mut receiver: LeaseKeepAliveStream,
    lease_id: u64,
    deadline: &mut Instant,
    token: &CancellationToken,
) -> anyhow::Result<bool> {
    loop {
        // 距离 deadline 一半时刻续期，给网络抖动留下足够余量。
        let next_renewal = deadline
            .saturating_duration_since(Instant::now())
            .div_f64(2.0);

        tokio::select! {
            biased;

            status = receiver.message() => {
                match status {
                    Ok(Some(resp)) => {
                        tracing::trace!(lease_id, "keep alive response received: {:?}", resp);
                        let ttl = resp.ttl();
                        if ttl <= 0 {
                            tracing::error!(lease_id, "Keep-alive lease expired");
                            anyhow::bail!(
                                "Unable to maintain lease - expired or revoked. \
                                 Check etcd server status"
                            );
                        }
                        *deadline = Instant::now() + Duration::from_secs(ttl as u64);
                    }
                    Ok(None) => {
                        tracing::warn!(lease_id, "Keep-alive stream unexpectedly ended");
                        return Ok(true);
                    }
                    Err(e) => {
                        tracing::warn!(lease_id, error = %e, "Keep-alive stream error");
                        return Ok(true);
                    }
                }
            }

            _ = token.cancelled() => {
                tracing::debug!(lease_id, "cancellation token triggered; revoking lease");
                let mut lease_client = connector.get_client().lease_client();
                if let Err(e) = lease_client.revoke(lease_id as i64).await {
                    tracing::warn!(
                        lease_id,
                        error = %e,
                        "Failed to revoke lease during cancellation. Cleanup may be incomplete."
                    );
                }
                return Ok(false);
            }

            _ = tokio::time::sleep(next_renewal) => {
                tracing::trace!(lease_id, "sending keep alive");
                if let Err(e) = sender.keep_alive().await {
                    tracing::warn!(
                        lease_id,
                        error = %e,
                        "Unable to send lease heartbeat. Check etcd server status"
                    );
                }
            }
        }
    }
}

// === SECTION: tests ===

#[cfg(test)]
mod tests {
    //! ## 测试过程
    //! 在存在本地 etcd 的前提下，逐一验证 `create_lease`、`keep_alive`、
    //! `new_keep_alive_stream`、`keep_alive_with_stream` 的取消/过期/撤销路径；
    //! 没有 etcd 时打印跳过信息直接返回。
    //!
    //! ## 意义
    //! 这些测试直接调用私有续期函数，是检测续期状态机不被静默破坏的最后防线。

    use super::*;
    use crate::transports::etcd::connector::Connector;
    use std::sync::Arc;

    fn test_etcd_urls() -> Vec<String> {
        let url = std::env::var("PAGODA_TEST_ETCD_URL")
            .unwrap_or_else(|_| "http://localhost:2379".to_string());
        vec![url]
    }

    async fn maybe_ready_connector() -> Option<Arc<Connector>> {
        let connector = Connector::new(test_etcd_urls(), None).await.ok()?;
        let mut lease_client = connector.get_client().lease_client();
        let lease = lease_client.grant(1, None).await.ok()?;
        let _ = lease_client.revoke(lease.id()).await;
        Some(connector)
    }

    async fn grant_lease(connector: &Arc<Connector>, ttl: i64) -> Option<u64> {
        let mut lease_client = connector.get_client().lease_client();
        lease_client
            .grant(ttl, None)
            .await
            .ok()
            .map(|lease| lease.id() as u64)
    }

    #[tokio::test]
    async fn test_supplemental_create_lease_returns_id_when_etcd_available() {
        let Some(connector) = maybe_ready_connector().await else {
            eprintln!("Skipping etcd-dependent lease test: no local etcd available");
            return;
        };

        let token = CancellationToken::new();
        let lease_id = create_lease(connector.clone(), 5, token.clone())
            .await
            .expect("create_lease should succeed with a live etcd server");
        assert!(lease_id > 0);

        token.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(1), token.cancelled())
            .await
            .expect("cancellation should be observed promptly");
    }

    #[tokio::test]
    async fn test_supplemental_create_lease_zero_ttl_behaviour_when_etcd_available() {
        let Some(connector) = maybe_ready_connector().await else {
            eprintln!("Skipping etcd-dependent lease test: no local etcd available");
            return;
        };

        let token = CancellationToken::new();
        match create_lease(connector.clone(), 0, token.clone()).await {
            Ok(lease_id) => {
                assert!(lease_id > 0);
                token.cancel();
            }
            Err(err) => {
                let err = err.to_string();
                assert!(!err.is_empty());
            }
        }
    }

    #[tokio::test]
    async fn test_supplemental_keep_alive_returns_ok_when_cancelled_when_etcd_available() {
        let Some(connector) = maybe_ready_connector().await else {
            eprintln!("Skipping etcd-dependent lease test: no local etcd available");
            return;
        };

        let lease_id = grant_lease(&connector, 5)
            .await
            .expect("expected a lease id from live etcd");

        let token = CancellationToken::new();
        token.cancel();

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            keep_alive(connector.clone(), lease_id, 5, token.clone()),
        )
        .await
        .expect("keep_alive should finish promptly when cancelled")
        .expect("keep_alive cancellation path should not error");

        let _ = result;
    }

    #[tokio::test]
    async fn test_supplemental_new_keep_alive_stream_returns_none_when_cancelled_when_etcd_available()
     {
        let Some(connector) = maybe_ready_connector().await else {
            eprintln!("Skipping etcd-dependent lease test: no local etcd available");
            return;
        };

        let lease_id = grant_lease(&connector, 5)
            .await
            .expect("expected a lease id from live etcd");

        connector
            .get_client()
            .lease_client()
            .revoke(lease_id as i64)
            .await
            .expect("expected revoke to succeed");

        let token = CancellationToken::new();
        token.cancel();
        let deadline = Instant::now() + Duration::from_secs(2);

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            new_keep_alive_stream(&connector, lease_id, &deadline, &token),
        )
        .await
        .expect("new_keep_alive_stream should finish promptly when cancelled")
        .expect("cancellation path should not error");

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_supplemental_new_keep_alive_stream_deadline_exceeded_when_etcd_available() {
        let Some(connector) = maybe_ready_connector().await else {
            eprintln!("Skipping etcd-dependent lease test: no local etcd available");
            return;
        };

        let lease_id = grant_lease(&connector, 5)
            .await
            .expect("expected a lease id from live etcd");

        connector
            .get_client()
            .lease_client()
            .revoke(lease_id as i64)
            .await
            .expect("expected revoke to succeed");

        let token = CancellationToken::new();
        let deadline = Instant::now() - Duration::from_millis(1);

        let err = tokio::time::timeout(
            Duration::from_secs(5),
            new_keep_alive_stream(&connector, lease_id, &deadline, &token),
        )
        .await
        .expect("new_keep_alive_stream should finish promptly")
        .expect_err("deadline-exceeded path should error")
        .to_string();

        assert!(err.contains("deadline exceeded"));
    }

    #[tokio::test]
    async fn test_supplemental_keep_alive_with_stream_returns_false_when_cancelled_when_etcd_available()
     {
        let Some(connector) = maybe_ready_connector().await else {
            eprintln!("Skipping etcd-dependent lease test: no local etcd available");
            return;
        };

        let lease_id = grant_lease(&connector, 5)
            .await
            .expect("expected a lease id from live etcd");

        let deadline = Instant::now() + Duration::from_secs(2);
        let token = CancellationToken::new();
        token.cancel();

        let (sender, receiver) = new_keep_alive_stream(&connector, lease_id, &deadline, &token)
            .await
            .expect("new_keep_alive_stream should not error with a cancelled token and live lease")
            .expect("expected a keep-alive stream");

        let mut deadline = Instant::now() + Duration::from_secs(2);
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            keep_alive_with_stream(
                &connector,
                sender,
                receiver,
                lease_id,
                &mut deadline,
                &token,
            ),
        )
        .await
        .expect("keep_alive_with_stream should finish promptly when cancelled")
        .expect("cancellation path should not error");

        assert!(!result);
    }

    #[tokio::test]
    async fn test_supplemental_keep_alive_with_stream_returns_true_when_stream_is_revoked_when_etcd_available()
     {
        let Some(connector) = maybe_ready_connector().await else {
            eprintln!("Skipping etcd-dependent lease test: no local etcd available");
            return;
        };

        let lease_id = grant_lease(&connector, 10)
            .await
            .expect("expected a lease id from live etcd");

        let token = CancellationToken::new();
        let deadline = Instant::now() + Duration::from_secs(2);
        let (sender, receiver) = new_keep_alive_stream(&connector, lease_id, &deadline, &token)
            .await
            .expect("new_keep_alive_stream should succeed for a live lease")
            .expect("expected a keep-alive stream");

        let revoker = {
            let connector = connector.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let mut lease_client = connector.get_client().lease_client();
                let _ = lease_client.revoke(lease_id as i64).await;
            })
        };

        let mut deadline = Instant::now() + Duration::from_secs(2);
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            keep_alive_with_stream(
                &connector,
                sender,
                receiver,
                lease_id,
                &mut deadline,
                &token,
            ),
        )
        .await
        .expect("keep_alive_with_stream should finish promptly after revocation")
        .expect("stream revocation should not be an error");

        revoker.await.expect("revoker task should not panic");
        assert!(result);
    }
}
