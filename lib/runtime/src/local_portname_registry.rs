// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 本地端点注册表（Local PortName Registry）
//!
//! ## 设计意图
//! 为运行在同一进程内的调用方提供一张"端点名 → 异步引擎"的查找表，
//! 让本地代码可以直接拿到引擎对象并调用，绕过完整的网络传输栈。这样既能在
//! 单元测试 / 进程内集成场景下复用 `AsyncEngine` 抽象，也能避免为本地路径
//! 单独维护一套调用约定。
//!
//! ## 外部契约
//! - 公开类型别名 `LocalAsyncEngine`：固定为
//!   `Arc<dyn AsyncEngine<SingleIn<Value>, ManyOut<Annotated<Value>>, anyhow::Error> + Send + Sync>`，
//!   被多个上层模块作为引擎槽位类型直接使用，**签名不可变**。
//! - 公开结构体 `LocalPortNameRegistry`：必须保持 `Clone + Default`，
//!   且 `clone()` 之后的副本与原对象共享同一份底层存储（写入相互可见）。
//! - 方法集合 `new` / `register(String, LocalAsyncEngine)` / `get(&str) -> Option<LocalAsyncEngine>`
//!   的签名与语义保持不变；`register` 对同名端点采取覆盖语义。
//!
//! ## 实现要点
//! - 底层使用 `Arc<DashMap<String, LocalAsyncEngine>>`：`Arc` 用来在多份克隆之间
//!   共享同一张表，`DashMap` 提供并发安全的读写。
//! - 注册时输出一条 `debug` 日志，便于排查端点注册顺序问题。
//! - 读取时返回内部存储 `Arc<...>` 的克隆，避免把 `DashMap` 句柄泄露到外部、
//!   防止调用方在持有句柄期间阻塞其它写入。

use crate::engine::AsyncEngine;
use dashmap::DashMap;
use std::sync::Arc;

// === SECTION: 公共类型别名 ===

/// 本地端点统一使用的异步引擎句柄类型。
///
/// 该别名固定了输入 / 输出 / 错误的具体形态，便于在不同模块之间传递引擎对象
/// 而不必每处重复书写完整的 `Arc<dyn AsyncEngine<...>>` 签名。
pub type LocalAsyncEngine = Arc<
    dyn AsyncEngine<
            crate::pipeline::SingleIn<serde_json::Value>,
            crate::pipeline::ManyOut<crate::protocols::annotated::Annotated<serde_json::Value>>,
            anyhow::Error,
        > + Send
        + Sync,
>;

// === SECTION: 注册表结构 ===

/// 进程内本地端点注册表。
///
/// 用于保存"端点名称 → 异步引擎"的映射；同进程内的调用方可以通过 `get` 取出
/// 引擎对象并直接驱动，无需走网络层。`Clone` 出来的副本与原对象共享同一份
/// 内部存储，因此可以放心地把它分发给多个组件。
#[derive(Clone, Default)]
pub struct LocalPortNameRegistry {
    /// 端点名 → 引擎的并发映射；外层 `Arc` 负责跨克隆共享同一张底表。
    engines: Arc<DashMap<String, LocalAsyncEngine>>,
}

// === SECTION: 注册 / 查询行为 ===

impl LocalPortNameRegistry {
    /// 构造一张全新的、内部为空的本地端点注册表。
    ///
    /// 中文说明：
    /// 1. 先构造一张空的 `DashMap`，再用 `Arc` 包起来，使后续克隆出的副本能共享同一份状态。
    /// 2. 把这张共享映射表填入 `LocalPortNameRegistry` 并返回。
    pub fn new() -> Self {
        let engines = Arc::new(DashMap::new());

        Self { engines }
    }

    /// 注册一个本地端点。
    ///
    /// # 参数
    /// * `portname_name` —— 端点名（例如 `"generate"`、`"load_lora"`）。
    /// * `engine` —— 处理该端点请求的异步引擎句柄。
    ///
    /// 中文说明：
    /// 1. 借用内部共享映射表的引用，明确写入目标即为当前注册表持有的 `DashMap`。
    /// 2. 写入前输出一条 `debug` 日志，便于排查端点注册顺序。
    /// 3. 调用 `DashMap::insert`，对同名端点采取覆盖语义。
    pub fn register(&self, portname_name: String, engine: LocalAsyncEngine) {
        let registry = &self.engines;

        tracing::debug!("Registering local portname: {portname_name}");
        registry.insert(portname_name, engine);
    }

    /// 查询已注册的本地端点。
    ///
    /// 找到时返回 `Some(LocalAsyncEngine)`；未注册时返回 `None`。
    ///
    /// 中文说明：
    /// 1. 通过 `DashMap::get` 取出只读临时句柄。
    /// 2. 命中时克隆内部 `Arc` 引擎并返回，避免把 `DashMap` 内部句柄泄露到调用方。
    /// 3. 未命中时返回 `None`，表示当前注册表里没有该端点。
    pub fn get(&self, portname_name: &str) -> Option<LocalAsyncEngine> {
        let maybe_engine = self.engines.get(portname_name);

        if let Some(engine) = maybe_engine {
            return Some(engine.clone());
        }

        None
    }
}

// === SECTION: 单元测试 ===

#[cfg(test)]
mod tests {
    //! ## 测试过程
    //! 通过一个最小化的 `MockLocalEngine` 实现 `AsyncEngine`，分别验证：
    //! 空注册表的初始状态、注册 / 查询的等价性、同名端点的覆盖语义，
    //! 以及克隆出的注册表与原对象共享同一份底层存储。
    //!
    //! ## 意义
    //! 这些用例固定了 `LocalPortNameRegistry` 的对外可观察行为——
    //! 任何后续重构（容器替换、并发策略变更等）都必须使这些断言继续成立，
    //! 从而保证上层使用方的契约不被破坏。

    use super::*;
    use crate::engine::{AsyncEngineContextProvider, async_trait};
    use crate::pipeline::{Context, ResponseStream};
    use crate::protocols::annotated::Annotated;
    use futures::{stream, StreamExt};
    use serde_json::{Value, json};

    /// 用于测试的最小本地引擎：把端点名称与请求载荷一并回显。
    struct MockLocalEngine {
        name: &'static str,
    }

    #[async_trait]
    impl AsyncEngine<
        crate::pipeline::SingleIn<Value>,
        crate::pipeline::ManyOut<Annotated<Value>>,
        anyhow::Error,
    > for MockLocalEngine
    {
        async fn generate(
            &self,
            request: crate::pipeline::SingleIn<Value>,
        ) -> Result<crate::pipeline::ManyOut<Annotated<Value>>, anyhow::Error> {
            let ctx = request.context();
            let response = Annotated::from_data(json!({
                "engine": self.name,
                "input": request.content().clone(),
            }));
            Ok(ResponseStream::new(Box::pin(stream::iter(vec![response])), ctx))
        }
    }

    /// 工具函数：驱动给定引擎、收集其输出流为 `Vec`。
    async fn collect_output(engine: &LocalAsyncEngine, payload: Value) -> Vec<Annotated<Value>> {
        engine
            .generate(Context::new(payload))
            .await
            .unwrap()
            .collect()
            .await
    }

    #[test]
    fn test_new_and_default_start_empty() {
        let registry = LocalPortNameRegistry::new();
        let default_registry = LocalPortNameRegistry::default();

        assert!(registry.engines.is_empty());
        assert!(default_registry.engines.is_empty());
        assert!(registry.get("missing").is_none());
        assert!(default_registry.get("missing").is_none());
    }

    #[tokio::test]
    async fn test_register_and_get_return_same_engine() {
        let registry = LocalPortNameRegistry::new();
        let engine: LocalAsyncEngine = Arc::new(MockLocalEngine { name: "alpha" });

        registry.register("generate".to_string(), engine.clone());

        let retrieved1 = registry.get("generate").expect("engine should exist");
        let retrieved2 = registry.get("generate").expect("engine should still exist");

        assert!(Arc::ptr_eq(&engine, &retrieved1));
        assert!(Arc::ptr_eq(&retrieved1, &retrieved2));

        let output = collect_output(&retrieved1, json!({ "prompt": "hello" })).await;
        assert_eq!(output.len(), 1);
        assert_eq!(
            output[0].data,
            Some(json!({
                "engine": "alpha",
                "input": { "prompt": "hello" },
            }))
        );
        assert!(output[0].event.is_none());
        assert!(output[0].error.is_none());
    }

    #[tokio::test]
    async fn test_register_overwrites_existing_portname() {
        let registry = LocalPortNameRegistry::new();
        let first: LocalAsyncEngine = Arc::new(MockLocalEngine { name: "first" });
        let second: LocalAsyncEngine = Arc::new(MockLocalEngine { name: "second" });

        registry.register("generate".to_string(), first.clone());
        registry.register("generate".to_string(), second.clone());

        let retrieved = registry.get("generate").expect("overwritten engine should exist");

        assert!(!Arc::ptr_eq(&retrieved, &first));
        assert!(Arc::ptr_eq(&retrieved, &second));

        let output = collect_output(&retrieved, json!("payload")).await;
        assert_eq!(
            output[0].data,
            Some(json!({
                "engine": "second",
                "input": "payload",
            }))
        );
    }

    #[tokio::test]
    async fn test_clone_shares_underlying_registry() {
        let registry = LocalPortNameRegistry::new();
        let cloned = registry.clone();
        let engine: LocalAsyncEngine = Arc::new(MockLocalEngine { name: "shared" });

        assert!(Arc::ptr_eq(&registry.engines, &cloned.engines));

        cloned.register("health".to_string(), engine.clone());

        let from_original = registry.get("health").expect("original should see clone writes");
        let from_clone = cloned.get("health").expect("clone should see its writes");

        assert!(Arc::ptr_eq(&from_original, &engine));
        assert!(Arc::ptr_eq(&from_original, &from_clone));

        let output = collect_output(&from_original, json!([1, 2, 3])).await;
        assert_eq!(
            output[0].data,
            Some(json!({
                "engine": "shared",
                "input": [1, 2, 3],
            }))
        );
    }
}
