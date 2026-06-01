// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 设计意图
//! 为 `/engine/*` HTTP 路由提供一个**进程级、可克隆共享**的回调注册表.
//! Python 侧通过 `runtime.register_engine_route()` 把任意 JSON->JSON 异步函数
//! 挂进来,Rust 侧的 axum 路由层从同一份注册表里查找并触发回调.
//!
//! # 外部契约
//! - `pub type EngineRouteCallback`: `Arc<dyn Fn(Value) -> Pin<Box<dyn Future<Output=Result<Value>> + Send>> + Send + Sync>`;
//! - `pub struct EngineRouteRegistry { ... }` + `#[derive(Clone, Default)]`;
//! - 方法:`new()` / `register(&self, route, callback)` / `get(&self, route) -> Option<_>` / `routes(&self) -> Vec<String>`;
//! - **共享语义**: `clone()` 必须返回与原实例共享底层存储的句柄,任一侧写入都立即可见;
//! - **同名注册**: 重复注册同名路由时,新回调覆盖旧回调,且 `routes()` 数量不增.
//!
//! # 实现要点
//! - 底层选用 `Arc<RwLock<HashMap<...>>>`:写注册稀疏、读查找频繁,RwLock 优于 Mutex;
//! - `register` 时 `tracing::debug!` 输出完整路径 `/engine/<route>`,便于排查注册顺序;
//! - 抽出 `read_table` / `write_table` 内联助手统一处理 `unwrap()`,避免散落的 `poisoned lock` 表达.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

// === SECTION: 类型别名 ===

/// 引擎路由回调.接受 JSON body,异步返回 JSON 结果或错误.
pub type EngineRouteCallback = Arc<
    dyn Fn(
            serde_json::Value,
        ) -> Pin<Box<dyn Future<Output = anyhow::Result<serde_json::Value>> + Send>>
        + Send
        + Sync,
>;

// === SECTION: EngineRouteRegistry ===

/// `/engine/*` 路由回调注册表.
///
/// 派生 `Clone` 后底层 `Arc` 引用计数被复制,所有副本共享同一份路由表.
#[derive(Clone, Default)]
pub struct EngineRouteRegistry {
    routes: Arc<RwLock<HashMap<String, EngineRouteCallback>>>,
}

impl EngineRouteRegistry {
    /// 创建空注册表.
    pub fn new() -> Self {
        Self::default()
    }

    /// 注册回调到指定路由名.同名重复注册会覆盖旧值.
    pub fn register(&self, route: &str, callback: EngineRouteCallback) {
        self.write_table().insert(route.to_string(), callback);
        tracing::debug!("Registered engine route: /engine/{route}");
    }

    /// 查询路由对应的回调.
    pub fn get(&self, route: &str) -> Option<EngineRouteCallback> {
        self.read_table().get(route).cloned()
    }

    /// 列出所有已注册路由名(顺序不保证).
    pub fn routes(&self) -> Vec<String> {
        self.read_table().keys().cloned().collect()
    }

    // ── 内联读写助手 ──

    #[inline]
    fn read_table(&self) -> RwLockReadGuard<'_, HashMap<String, EngineRouteCallback>> {
        // poisoned 锁意味着上游已 panic,这里 unwrap 让错误尽早暴露.
        self.routes.read().expect("EngineRouteRegistry read lock poisoned")
    }

    #[inline]
    fn write_table(&self) -> RwLockWriteGuard<'_, HashMap<String, EngineRouteCallback>> {
        self.routes.write().expect("EngineRouteRegistry write lock poisoned")
    }
}

// === SECTION: tests ===

#[cfg(test)]
mod tests {
    use super::*;

    // ── 辅助函数 ─────────────────────────────────────────────────────────────

    /// 固定返回 `{"ok": true}` 的回调
    fn ok_callback() -> EngineRouteCallback {
        Arc::new(|_| Box::pin(async { Ok(serde_json::json!({"ok": true})) }))
    }

    /// 返回 `{"echo": <body>}` 的回调
    fn echo_callback() -> EngineRouteCallback {
        Arc::new(|body| Box::pin(async move { Ok(serde_json::json!({"echo": body})) }))
    }

    /// 始终返回错误的回调
    fn err_callback(msg: &'static str) -> EngineRouteCallback {
        Arc::new(move |_| Box::pin(async move { Err(anyhow::anyhow!("{}", msg)) }))
    }

    // ── 空注册表初始状态 ─────────────────────────────────────────────────────

    /// 新建注册表：get 返回 None，routes 为空
    #[test]
    fn new_registry_is_empty() {
        let r = EngineRouteRegistry::new();
        assert!(r.get("anything").is_none());
        assert!(r.routes().is_empty());
    }

    /// Default trait 与 new() 行为一致
    #[test]
    fn default_equals_new() {
        let r: EngineRouteRegistry = Default::default();
        assert!(r.routes().is_empty());
    }

    // ── 基本注册与查询 ───────────────────────────────────────────────────────

    /// 注册后 get 返回 Some，未注册路由返回 None，routes 列出所有路由名
    #[tokio::test]
    async fn test_registry_basic() {
        let registry = EngineRouteRegistry::new();
        registry.register("test", echo_callback());

        assert!(registry.get("test").is_some());
        assert!(registry.get("nonexistent").is_none());

        let routes = registry.routes();
        assert_eq!(routes.len(), 1);
        assert!(routes.contains(&"test".to_string()));
    }

    /// 注册多条路由，routes() 全部返回
    #[test]
    fn routes_lists_all_keys() {
        let r = EngineRouteRegistry::new();
        r.register("a", ok_callback());
        r.register("b", ok_callback());
        r.register("c", ok_callback());
        let mut names = r.routes();
        names.sort();
        assert_eq!(names, vec!["a", "b", "c"]);
    }

    // ── 覆盖注册 ─────────────────────────────────────────────────────────────

    /// 同名路由重复注册：get 返回最新回调，routes 仍只有一条
    #[tokio::test]
    async fn overwrite_replaces_callback() {
        let r = EngineRouteRegistry::new();
        r.register("route", Arc::new(|_| Box::pin(async { Ok(serde_json::json!("v1")) })));
        let v1 = r.get("route").unwrap()(serde_json::json!(null)).await.unwrap();
        assert_eq!(v1, "v1");

        r.register("route", Arc::new(|_| Box::pin(async { Ok(serde_json::json!("v2")) })));
        let v2 = r.get("route").unwrap()(serde_json::json!(null)).await.unwrap();
        assert_eq!(v2, "v2");
        assert_eq!(r.routes().len(), 1);
    }

    // ── 回调执行 ─────────────────────────────────────────────────────────────

    /// 回调可正确接收 body 并返回预期结果
    #[tokio::test]
    async fn test_callback_execution() {
        let registry = EngineRouteRegistry::new();
        let callback: EngineRouteCallback = Arc::new(|body| {
            Box::pin(async move {
                let input = body.get("input").and_then(|v| v.as_str()).unwrap_or("");
                Ok(serde_json::json!({ "output": format!("processed: {}", input) }))
            })
        });
        registry.register("process", callback);

        let cb = registry.get("process").unwrap();
        let result = cb(serde_json::json!({"input": "test"})).await.unwrap();
        assert_eq!(result["output"], "processed: test");
    }

    /// 回调返回错误时，调用方收到 Err
    #[tokio::test]
    async fn callback_returning_error_propagates() {
        let r = EngineRouteRegistry::new();
        r.register("fail", err_callback("something went wrong"));

        let result = r.get("fail").unwrap()(serde_json::json!({})).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("something went wrong"));
    }

    /// 对同一回调连续调用两次，结果各自独立
    #[tokio::test]
    async fn callback_can_be_called_multiple_times() {
        let r = EngineRouteRegistry::new();
        r.register("echo", echo_callback());

        let cb = r.get("echo").unwrap();
        let r1 = cb(serde_json::json!("first")).await.unwrap();
        let r2 = cb(serde_json::json!("second")).await.unwrap();
        assert_eq!(r1["echo"], "first");
        assert_eq!(r2["echo"], "second");
    }

    // ── Clone 共享语义 ───────────────────────────────────────────────────────

    /// clone 共享底层存储：一方注册对另一方立即可见
    #[tokio::test]
    async fn test_clone_shares_routes() {
        let registry = EngineRouteRegistry::new();
        registry.register("test", ok_callback());

        let cloned = registry.clone();
        assert!(registry.get("test").is_some());
        assert!(cloned.get("test").is_some());

        // 克隆体新增路由，原实例也能看到
        cloned.register("test2", ok_callback());
        assert!(registry.get("test2").is_some());
    }

    /// 克隆体注册后，原实例 routes() 数量正确
    #[test]
    fn clone_register_reflects_in_original_routes_count() {
        let r = EngineRouteRegistry::new();
        let c = r.clone();
        r.register("x", ok_callback());
        c.register("y", ok_callback());
        assert_eq!(r.routes().len(), 2);
        assert_eq!(c.routes().len(), 2);
    }

    // ── 并发注册 ─────────────────────────────────────────────────────────────

    /// 多线程并发注册不同路由，最终数量正确
    #[tokio::test]
    async fn concurrent_register_correct_count() {
        let r = EngineRouteRegistry::new();
        let handles: Vec<_> = (0..20usize).map(|i| {
            let r = r.clone();
            tokio::spawn(async move {
                r.register(&format!("route{}", i), ok_callback());
            })
        }).collect();
        for h in handles { h.await.unwrap(); }
        assert_eq!(r.routes().len(), 20);
    }

    /// 并发注册相同路由名，最终只有一条路由（无 panic）
    #[tokio::test]
    async fn concurrent_overwrite_same_route_no_panic() {
        let r = EngineRouteRegistry::new();
        let handles: Vec<_> = (0..10usize).map(|_| {
            let r = r.clone();
            tokio::spawn(async move {
                r.register("shared", ok_callback());
            })
        }).collect();
        for h in handles { h.await.unwrap(); }
        assert_eq!(r.routes().len(), 1);
        assert!(r.get("shared").is_some());
    }

    /// 并发读写混合：写入的路由最终可被读取
    #[tokio::test]
    async fn concurrent_read_write_consistent() {
        let r = EngineRouteRegistry::new();
        // 先注册好路由
        for i in 0..5usize {
            r.register(&format!("r{}", i), ok_callback());
        }
        // 并发读取
        let handles: Vec<_> = (0..5usize).map(|i| {
            let r = r.clone();
            tokio::spawn(async move {
                assert!(r.get(&format!("r{}", i)).is_some());
            })
        }).collect();
        for h in handles { h.await.unwrap(); }
    }
}
