// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 设计意图
//! 为运行时流式 RPC 提供「截止时间包装器」：一旦给定时刻到达，
//! 整个流立即视为结束，下游消费者无需另外处理超时。
//!
//! # 外部契约
//! - `DeadlineStream<S>`：实现 `Stream`，元素类型与 `S::Item` 一致；
//! - `until_deadline(stream, deadline)`：把任意 `Stream + Unpin` 包成
//!   带截止时间的版本，截止后返回 `Poll::Ready(None)` 表示终止；
//! - 超时不会丢弃当前已就绪元素，仅阻断后续轮询。
//!
//! # 实现要点
//! - 用 `tokio::time::Sleep` 表达截止时刻，`Pin<Box<Sleep>>` 便于挪进 struct；
//! - `poll_next` 先轮询 `sleep`，未触发再轮询底层 stream；
//! - 这种「先超时后数据」的顺序保证不会出现“超时后再吐数据”的歧义。

use futures::stream::{Stream, StreamExt};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use tokio::time::{self, Duration, Instant, Sleep, sleep_until};

// === SECTION: DeadlineStream ===

/// 在截止时间前持续转发底层流中的元素。
pub struct DeadlineStream<S> {
    stream: S,
    sleep: Pin<Box<Sleep>>,
}

impl<S: Stream + Unpin> Stream for DeadlineStream<S> {
    type Item = S::Item;

    /// 轮询下一个流元素，并在截止时间到达时终止整个流。
    ///
    /// 处理流程是先检查定时器是否触发；若已超时则返回结束，否则继续轮询底层流。
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let deadline_elapsed = Pin::new(&mut self.sleep).poll(cx).is_ready();
        if deadline_elapsed {
            return Poll::Ready(None);
        }

        let next_item = self.as_mut().stream.poll_next_unpin(cx);
        match &next_item {
            Poll::Ready(Some(_)) => tracing::trace!("DeadlineStream: received item"),
            Poll::Ready(None) => tracing::trace!("DeadlineStream: underlying stream ended"),
            Poll::Pending => tracing::trace!("DeadlineStream: waiting for next item"),
        }
        next_item
    }
}

/// 为任意流包装一个截止时间控制器。
///
/// 处理流程是创建指向截止时刻的休眠器，并与原始流组合成 `DeadlineStream`。
// === SECTION: 公开构造器 ===

pub fn until_deadline<S: Stream + Unpin>(stream: S, deadline: Instant) -> DeadlineStream<S> {
    let sleep = Box::pin(sleep_until(deadline));

    DeadlineStream { stream, sleep }
}

// === SECTION: tests ===

#[cfg(test)]
mod tests {
    use futures::stream::{self, Stream, StreamExt};
    use tokio::pin;

    use super::*;

    // 运行截止时间测试的辅助函数。
    async fn run_deadline_test(sleep_times_ms: Vec<u64>, deadline_ms: u64) -> Vec<u64> {
        let stream = stream::iter(sleep_times_ms);
        let stream = stream.then(|x| {
            let sleep = time::sleep(Duration::from_millis(x));
            async move {
                sleep.await;
                x
            }
        });

        let deadline = Instant::now() + Duration::from_millis(deadline_ms);
        let mut result = Vec::new();

        pin!(stream);
        let mut stream = until_deadline(stream, deadline);

        while let Some(x) = stream.next().await {
            result.push(x);
        }

        result
    }

    #[tokio::test]
    async fn test_deadline_exceeded() {
        // 测试截止时间超出后只返回已完成的元素。
        let sleep_times_ms = vec![100, 100, 200, 50];
        let deadline_ms = 300;

        let result = run_deadline_test(sleep_times_ms, deadline_ms).await;
        assert_eq!(result, vec![100, 100]);
    }

    #[tokio::test]
    async fn test_complete_before_deadline() {
        // 测试在截止时间之前可以完整消费全部元素。
        let sleep_times_ms = vec![100, 50, 50];
        let deadline_ms = 300;

        let result = run_deadline_test(sleep_times_ms, deadline_ms).await;
        assert_eq!(result, vec![100, 50, 50]);
    }

    #[tokio::test]
    async fn test_deadline_immediately_returns_none() {
        // 测试已过期的截止时间会立即结束流。
        let stream = stream::iter(vec![1_u8, 2, 3]);
        let deadline = Instant::now() - Duration::from_millis(1);
        let mut stream = until_deadline(stream, deadline);

        assert_eq!(stream.next().await, None);
    }
}
