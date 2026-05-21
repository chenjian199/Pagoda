// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! `TypedPrefixWatcher<T>`：在 etcd PrefixWatcher 基础上增加自动反序列化层。

use std::pin::Pin;

use futures::Stream;
use serde::de::DeserializeOwned;

/// 带自动反序列化的 etcd 前缀 watch。
///
/// `watch(prefix) → Stream<(key, Option<T>)>`（None 表示 key 被删除）。
pub struct TypedPrefixWatcher<T> {
    _phantom: std::marker::PhantomData<T>,
}

impl<T: DeserializeOwned + Send + 'static> TypedPrefixWatcher<T> {
    /// 创建 watcher 并返回事件流。
    ///
    /// 当前为存根实现：etcd 集成尚未完成，返回空流。
    pub async fn watch(
        _prefix: &str,
    ) -> anyhow::Result<Pin<Box<dyn Stream<Item = (String, Option<T>)> + Send>>> {
        let stream = futures::stream::empty();
        Ok(Box::pin(stream))
    }
}
