// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 设计意图
//! 本模块封装一个 *可重连* 的 etcd 客户端持有者 [`Connector`]：
//! 业务层不直接持有 [`etcd_client::Client`]，而是从 [`Connector::get_client`]
//! 拿到一份廉价克隆；当底层连接断开时，由 [`Connector::reconnect`] 在原地热替换
//! 内部客户端，保证旧持有者下一次重新拉取时即可获得新连接。
//!
//! # 外部契约
//! - `Connector::new(urls, options) -> Result<Arc<Self>>` 同步建立首次连接；
//! - `Connector::get_client() -> etcd_client::Client` 永远返回当前客户端的克隆；
//! - `Connector::reconnect(deadline)` 持有内部 mutex 串行化重连尝试，受 deadline 约束；
//! - `Connector::etcd_urls() / connect_options()` 提供只读访问；
//! - 私有 `Connector::connect` 必须以 `"Unable to connect to etcd server at {urls}"`
//!   作为上下文包装错误（supplemental 测试与运维日志均依赖该前缀）；
//! - 私有 `BackoffState` 字段命名 `initial_backoff / min_backoff / max_backoff /
//!   current_backoff / last_connect_attempt` 不可改名（同模块测试直接读写）。
//!
//! # 实现要点
//! - 客户端用 [`parking_lot::RwLock`] 包裹：读路径无 async，写路径仅在 reconnect 成功时短暂持锁；
//! - 退避状态独立用 [`tokio::sync::Mutex`] 持有，保证多个并发重连协程串行；
//! - `apply_backoff` 把 sleep 时长压在 `[min, min(max, remaining/2)]` 区间，
//!   再把下一次的 `current_backoff` 翻倍，避免单次睡眠耗尽全部 deadline；
//! - `attempt_reset` 仅在距上次尝试超过 `current_backoff` 时把退避归零，
//!   防止短时间内连续 reconnect 调用被立即重置。

use anyhow::{Context, Result};
use etcd_client::ConnectOptions;
use parking_lot::RwLock;
use std::{sync::Arc, time::Duration};
use tokio::{sync::Mutex, time::sleep};

// === SECTION: Connector ===

/// 维护一个可热替换的 etcd 客户端句柄，对外暴露稳定的访问 API。
///
/// 注意：调用 [`Connector::get_client`] 仅返回内部句柄的克隆，不会持锁；不要在持有
/// 读锁（如 reconnect 过程的回调）的同一线程中再次获取读锁，以免触发 parking_lot
/// 的可重入死锁。
pub struct Connector {
    /// 当前活动的 etcd 客户端；reconnect 成功后会在写锁内整体替换。
    client: RwLock<etcd_client::Client>,
    /// 集群端点 URL 列表（仅 reconnect/accessor 使用）。
    etcd_urls: Vec<String>,
    /// 透传给 `etcd_client::Client::connect` 的可选连接参数。
    connect_options: Option<ConnectOptions>,
    /// 重连退避状态；用异步互斥保证 reconnect 串行执行。
    backoff_state: Mutex<BackoffState>,
}

impl Connector {
    /// 建立首次连接并返回共享句柄。
    pub async fn new(
        etcd_urls: Vec<String>,
        connect_options: Option<ConnectOptions>,
    ) -> Result<Arc<Self>> {
        let client = Self::connect(&etcd_urls, &connect_options).await?;
        Ok(Arc::new(Self {
            client: RwLock::new(client),
            etcd_urls,
            connect_options,
            backoff_state: Mutex::new(BackoffState::default()),
        }))
    }

    /// 私有：执行一次 etcd 连接，并把端点列表注入错误上下文。
    async fn connect(
        etcd_urls: &[String],
        connect_options: &Option<ConnectOptions>,
    ) -> Result<etcd_client::Client> {
        let portnames = etcd_urls.to_vec();
        let options = connect_options.clone();
        etcd_client::Client::connect(portnames, options)
            .await
            .with_context(|| {
                format!(
                    "Unable to connect to etcd server at {}. Check etcd server status",
                    etcd_urls.join(", ")
                )
            })
    }

    /// 返回当前 etcd 客户端的一个克隆；调用方持有副本，不影响后续热替换。
    pub fn get_client(&self) -> etcd_client::Client {
        self.client.read().clone()
    }

    /// 在 `deadline` 之前持续重试重连。
    ///
    /// 行为：
    /// 1. 全局只允许一个 reconnect 同时进行（通过 backoff_state 的 Mutex 实现）；
    /// 2. 进入时根据时间窗口决定是否把 `current_backoff` 归零；
    /// 3. 每轮先睡眠（首轮通常 0），睡完再校验 deadline，最后尝试连接；
    /// 4. 成功时在写锁内替换内部客户端并立即返回；失败仅记录日志并进入下一轮。
    pub async fn reconnect(&self, deadline: std::time::Instant) -> Result<()> {
        let mut backoff_state = self.backoff_state.lock().await;

        tracing::warn!("Reconnecting to ETCD cluster at: {:?}", self.etcd_urls);
        backoff_state.attempt_reset();

        loop {
            backoff_state.apply_backoff(deadline).await;
            if std::time::Instant::now() >= deadline {
                anyhow::bail!("Unable to reconnect to ETCD cluster: deadline exceeded");
            }

            match Self::connect(&self.etcd_urls, &self.connect_options).await {
                Ok(new_client) => {
                    tracing::info!("Successfully reconnected to ETCD cluster");
                    *self.client.write() = new_client;
                    return Ok(());
                }
                Err(e) => {
                    let remaining =
                        deadline.saturating_duration_since(std::time::Instant::now());
                    tracing::warn!(
                        "Reconnection failed (remaining time: {:?}): {}",
                        remaining,
                        e
                    );
                }
            }
        }
    }

    /// 返回构造时记录的 etcd 端点列表。
    pub fn etcd_urls(&self) -> &[String] {
        &self.etcd_urls
    }

    /// 返回构造时传入的连接参数。
    pub fn connect_options(&self) -> &Option<ConnectOptions> {
        &self.connect_options
    }
}

// === SECTION: BackoffState ===

/// 重连退避计算的内部状态。
///
/// 注：字段被同模块的 `supplemental_tests` 直接读写，请勿改名/改可见性。
#[derive(Debug)]
struct BackoffState {
    /// 当 `current_backoff` 为 0 时，apply_backoff 一次后会被设为该值。
    pub initial_backoff: Duration,
    /// 单次睡眠时长的下限（避免热路径死循环）。
    pub min_backoff: Duration,
    /// 单次睡眠时长的上限。
    pub max_backoff: Duration,
    /// 下一次 apply_backoff 期望的"基准"睡眠时长。
    current_backoff: Duration,
    /// 最近一次尝试连接的时间戳，attempt_reset 用其决定是否清零。
    last_connect_attempt: std::time::Instant,
}

impl Default for BackoffState {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_millis(500),
            min_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(5),
            current_backoff: Duration::ZERO,
            last_connect_attempt: std::time::Instant::now(),
        }
    }
}

impl BackoffState {
    /// 如果距上次尝试已超过 `current_backoff`，把退避归零（视为"新一轮"重连）。
    pub fn attempt_reset(&mut self) {
        let now = std::time::Instant::now();
        if now > self.last_connect_attempt + self.current_backoff {
            tracing::debug!("Resetting backoff to 0 (first reconnect or enough time has passed)");
            self.current_backoff = Duration::ZERO;
        }
    }

    /// 应用一次退避：
    /// - 若 `current_backoff > 0`，按 `min(current, remaining/2, max)` 后再 `max(_, min)`
    ///   作为本次睡眠，下一次 `current_backoff` 翻倍；
    /// - 若 `current_backoff == 0`，跳过睡眠，仅把 `current_backoff` 设为 `initial_backoff`。
    pub async fn apply_backoff(&mut self, deadline: std::time::Instant) {
        if self.current_backoff > Duration::ZERO {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            // 阶梯式裁剪：先按 remaining/2 截断，再夹到 [min, max]。
            let mut backoff = std::cmp::min(self.current_backoff, remaining / 2);
            backoff = std::cmp::min(backoff, self.max_backoff);
            backoff = std::cmp::max(backoff, self.min_backoff);
            self.current_backoff = backoff * 2;

            tracing::debug!(
                "Applying backoff of {:?} (remaining time: {:?})",
                backoff,
                remaining
            );
            sleep(backoff).await;
        } else {
            self.current_backoff = self.initial_backoff;
        }
        self.last_connect_attempt = std::time::Instant::now();
    }
}

// === SECTION: tests ===

#[cfg(test)]
mod tests {
    //! ## 测试过程
    //! 综合验证 `Connector::connect` 的错误上下文格式、`BackoffState` 的退避数学，
    //! 以及当本地存在 etcd 时，accessor 与 `reconnect(now)` 的行为。
    //!
    //! ## 意义
    //! 这些测试既覆盖纯逻辑路径（退避计算），又通过 `PAGODA_TEST_ETCD_URL` 触发
    //! 真实 etcd 路径（accessor / 过期 deadline 立即失败），确保重写不破坏现有契约。

    use super::*;

    fn test_etcd_urls() -> Vec<String> {
        let url = std::env::var("PAGODA_TEST_ETCD_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:2379".to_string());
        vec![url]
    }

    async fn maybe_live_connector() -> Option<Arc<Connector>> {
        Connector::new(test_etcd_urls(), None).await.ok()
    }

    #[tokio::test]
    async fn test_supplemental_connect_error_contains_portname_context() {
        let urls = vec!["bad://etcd-portname".to_string()];
        match Connector::connect(&urls, &None).await {
            Ok(_client) => {
                // etcd-client may lazily connect and succeed immediately.
            }
            Err(err) => {
                let err = err.to_string();
                assert!(err.contains("Unable to connect to etcd server at"));
                assert!(err.contains("bad://etcd-portname"));
            }
        }
    }

    #[tokio::test]
    async fn test_supplemental_new_error_bubbles_connect_context() {
        let urls = vec!["bad://etcd-portname".to_string()];
        match Connector::new(urls, None).await {
            Ok(_connector) => {
                // etcd-client may lazily connect and succeed immediately.
            }
            Err(err) => {
                let err = err.to_string();
                assert!(err.contains("Unable to connect to etcd server at"));
                assert!(err.contains("bad://etcd-portname"));
            }
        }
    }

    #[test]
    fn test_supplemental_backoff_default_values() {
        let state = BackoffState::default();
        assert_eq!(state.initial_backoff, Duration::from_millis(500));
        assert_eq!(state.min_backoff, Duration::from_millis(50));
        assert_eq!(state.max_backoff, Duration::from_secs(5));
        assert_eq!(state.current_backoff, Duration::ZERO);
        assert!(state.last_connect_attempt <= std::time::Instant::now());
    }

    #[test]
    fn test_supplemental_attempt_reset_resets_after_backoff_window() {
        let mut state = BackoffState::default();
        state.current_backoff = Duration::from_millis(100);
        state.last_connect_attempt = std::time::Instant::now() - Duration::from_millis(500);

        state.attempt_reset();
        assert_eq!(state.current_backoff, Duration::ZERO);
    }

    #[test]
    fn test_supplemental_attempt_reset_does_not_reset_within_backoff_window() {
        let mut state = BackoffState::default();
        state.current_backoff = Duration::from_millis(200);
        state.last_connect_attempt = std::time::Instant::now();

        state.attempt_reset();
        assert_eq!(state.current_backoff, Duration::from_millis(200));
    }

    #[tokio::test]
    async fn test_supplemental_apply_backoff_zero_sets_initial_without_sleep() {
        let mut state = BackoffState::default();
        state.current_backoff = Duration::ZERO;

        let before = std::time::Instant::now();
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        state.apply_backoff(deadline).await;
        let elapsed = before.elapsed();

        assert_eq!(state.current_backoff, Duration::from_millis(500));
        assert!(elapsed < Duration::from_millis(50));
    }

    #[tokio::test]
    async fn test_supplemental_apply_backoff_respects_min_and_doubles_state() {
        let mut state = BackoffState::default();
        state.current_backoff = Duration::from_millis(1);
        state.min_backoff = Duration::from_millis(20);
        state.max_backoff = Duration::from_secs(1);

        let before = std::time::Instant::now();
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        state.apply_backoff(deadline).await;
        let elapsed = before.elapsed();

        assert!(elapsed >= Duration::from_millis(20));
        assert_eq!(state.current_backoff, Duration::from_millis(40));
    }

    #[tokio::test]
    async fn test_supplemental_apply_backoff_respects_max_and_doubles_state() {
        let mut state = BackoffState::default();
        state.current_backoff = Duration::from_millis(100);
        state.min_backoff = Duration::from_millis(1);
        state.max_backoff = Duration::from_millis(2);

        let before = std::time::Instant::now();
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        state.apply_backoff(deadline).await;
        let elapsed = before.elapsed();

        assert!(elapsed >= Duration::from_millis(2));
        assert_eq!(state.current_backoff, Duration::from_millis(4));
    }

    #[tokio::test]
    async fn test_supplemental_apply_backoff_respects_remaining_half_window() {
        let mut state = BackoffState::default();
        state.current_backoff = Duration::from_millis(100);
        state.min_backoff = Duration::from_millis(1);
        state.max_backoff = Duration::from_secs(1);

        let before = std::time::Instant::now();
        let deadline = std::time::Instant::now() + Duration::from_millis(20);
        state.apply_backoff(deadline).await;
        let elapsed = before.elapsed();

        assert!(elapsed >= Duration::from_millis(8));
        assert!(elapsed < Duration::from_millis(40));
        assert!(state.current_backoff >= Duration::from_millis(18));
        assert!(state.current_backoff <= Duration::from_millis(22));
    }

    #[tokio::test]
    async fn test_supplemental_accessors_and_client_clone_when_etcd_available() {
        let Some(connector) = maybe_live_connector().await else {
            eprintln!("Skipping etcd-dependent accessor test: no local etcd available");
            return;
        };

        assert_eq!(connector.etcd_urls(), test_etcd_urls().as_slice());
        assert!(connector.connect_options().is_none());

        let _client_clone = connector.get_client();
    }

    #[tokio::test]
    async fn test_supplemental_reconnect_respects_past_deadline_when_etcd_available() {
        let Some(connector) = maybe_live_connector().await else {
            eprintln!("Skipping etcd-dependent reconnect test: no local etcd available");
            return;
        };

        let err = connector
            .reconnect(std::time::Instant::now())
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("deadline exceeded"));
    }
}
