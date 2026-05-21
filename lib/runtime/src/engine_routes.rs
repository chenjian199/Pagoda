// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 引擎路由注册表 — 将 `/engine/*` HTTP 路径映射到异步回调函数。
//!
//! 系统状态服务器（`SystemStatusServer`）在 `/engine/{route}` 下保留路由槽，
//! 推理引擎在初始化时通过 `EngineRouteRegistry::register()` 注册处理函数。
//! 框架与引擎之间通过此注册表解耦，框架无需感知具体引擎路由。

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

/// 引擎路由回调函数类型。
///
/// 接受 `serde_json::Value` 作为请求体，返回异步 `serde_json::Value` 响应。
/// 使用 JSON 而非强类型是为了跨语言兼容（Python/PyO3 绑定）和前向兼容性。
pub type EngineRouteCallback = Arc<
    dyn Fn(
            serde_json::Value,
        ) -> Pin<Box<dyn Future<Output = anyhow::Result<serde_json::Value>> + Send>>
        + Send
        + Sync,
>;

/// 引擎路由注册表。
///
/// 在 `DistributedRuntime` 中作为字段存在，随 DRT 廉价克隆。
/// 所有克隆共享同一份路由表（`Arc<RwLock<HashMap<...>>>`）。
#[derive(Clone, Default)]
pub struct EngineRouteRegistry {
    routes: Arc<RwLock<HashMap<String, EngineRouteCallback>>>,
}

impl EngineRouteRegistry {
    /// 创建空注册表。
    pub fn new() -> Self {
        Self {
            routes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 注册引擎路由回调。
    ///
    /// `route` 为路由名称（如 `"start_profile"`），不含 `/engine/` 前缀。
    /// 若已存在同名路由，将被覆盖（允许模型热重载时替换旧处理函数）。
    pub fn register(&self, route: &str, callback: EngineRouteCallback) {
        let mut routes = self.routes.write().unwrap();
        routes.insert(route.to_string(), callback);
        tracing::debug!("Registered engine route: /engine/{route}");
    }

    /// 查找路由回调。
    ///
    /// 返回回调的 `Arc` 克隆后立即释放读锁，调用方可安全地 `await` 回调。
    /// 路由不存在时返回 `None`，HTTP 服务器据此响应 `404 Not Found`。
    pub fn get(&self, route: &str) -> Option<EngineRouteCallback> {
        let routes = self.routes.read().unwrap();
        routes.get(route).cloned()
    }

    /// 返回所有已注册路由名称列表（不含 `/engine/` 前缀）。
    pub fn routes(&self) -> Vec<String> {
        let routes = self.routes.read().unwrap();
        routes.keys().cloned().collect()
    }

    /// 注销指定路由。
    pub fn unregister(&self, route: &str) -> bool {
        let mut routes = self.routes.write().unwrap();
        routes.remove(route).is_some()
    }

    /// 返回已注册路由数量。
    pub fn len(&self) -> usize {
        let routes = self.routes.read().unwrap();
        routes.len()
    }

    /// 注册表是否为空。
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_registry_basic() {
        let registry = EngineRouteRegistry::new();

        let cb: EngineRouteCallback = Arc::new(|_req| {
            Box::pin(async { Ok(serde_json::json!({"status": "ok"})) })
        });

        registry.register("test_route", cb);

        assert!(registry.get("test_route").is_some());
        assert!(registry.get("nonexistent").is_none());

        let routes = registry.routes();
        assert!(routes.contains(&"test_route".to_string()));
    }

    #[tokio::test]
    async fn test_callback_execution() {
        let registry = EngineRouteRegistry::new();

        let cb: EngineRouteCallback = Arc::new(|req: serde_json::Value| {
            Box::pin(async move {
                let name = req["name"].as_str().unwrap_or("world").to_string();
                Ok(serde_json::json!({ "greeting": format!("Hello, {name}!") }))
            })
        });

        registry.register("greet", cb);

        let handler = registry.get("greet").unwrap();
        let result = handler(serde_json::json!({ "name": "Pagoda" })).await.unwrap();

        assert_eq!(result["greeting"], "Hello, Pagoda!");
    }

    #[tokio::test]
    async fn test_clone_shares_routes() {
        let registry = EngineRouteRegistry::new();
        let cloned = registry.clone();

        let cb: EngineRouteCallback =
            Arc::new(|_req| Box::pin(async { Ok(serde_json::Value::Null) }));

        cloned.register("shared_route", cb);

        assert!(registry.get("shared_route").is_some(),
            "原始注册表应能看到克隆上注册的路由");
    }
}
