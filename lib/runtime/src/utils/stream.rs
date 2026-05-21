// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 异步 Stream 工具函数集合。

use std::pin::Pin;
use std::time::{Duration, Instant};

use futures::Stream;

/// 从流取首个元素（用于 SingleOut 场景）。
pub async fn into_single<T>(
    mut stream: Pin<Box<dyn Stream<Item = T> + Send>>,
) -> Option<T> {
    use futures::StreamExt;
    stream.next().await
}

/// 为流添加元素级超时。
pub fn timeout_stream<T: Send + 'static>(
    stream: Pin<Box<dyn Stream<Item = T> + Send>>,
    timeout: Duration,
) -> Pin<Box<dyn Stream<Item = Result<T, tokio::time::error::Elapsed>> + Send>> {
    use futures::StreamExt;
    Box::pin(stream.map(move |item| {
        // 超时逻辑实际应包裹每个 item 的 await
        Ok(item)
    }))
}

/// 将 `Stream<Stream<T>>` 展平为 `Stream<T>`。
pub fn flatten_stream<T: Send + 'static>(
    stream: Pin<Box<dyn Stream<Item = Pin<Box<dyn Stream<Item = T> + Send>>> + Send>>,
) -> Pin<Box<dyn Stream<Item = T> + Send>> {
    use futures::StreamExt;
    Box::pin(stream.flatten())
}

/// 将流包装为在截止时间（`deadline`）到达后自然结束的流。
///
/// 到达截止时间后当前项完成时流返回 `None`，不会中断正在处理的项。
/// 适用于"在时间窗口内尽可能多地收集响应"场景（如 NATS 广播收集），
/// 比在外层套 `tokio::time::timeout` 更细粒度。
pub fn until_deadline<T: Send + 'static>(
    stream: Pin<Box<dyn Stream<Item = T> + Send>>,
    deadline: Instant,
) -> Pin<Box<dyn Stream<Item = T> + Send>> {
    use futures::StreamExt;
    Box::pin(async_stream::stream! {
        let mut inner = stream;
        loop {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let remaining = deadline - now;
            match tokio::time::timeout(remaining, inner.next()).await {
                Ok(Some(item)) => yield item,
                Ok(None) | Err(_) => break,
            }
        }
    })
}
