// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pipeline::context` —— 请求上下文 Context / Controller / Registry
//!
//! ## 设计意图
//! `Context<T>` 是流过 pipeline 的请求载体：在保留载荷 `T` 的同时挂载
//! 唯一 id、stage 链路、controller（取消信号）与 registry（类型化 K/V 存储），
//! 让任意算子既能读到"我在处理什么"，又能取消下游、共享辅助数据。
//!
//! ## 外部契约
//! - `pub struct Context<T>`、`pub struct Controller`、`pub struct Registry`、
//!   `pub trait AsyncEngineContext` 均暴露；
//! - `Context::new(id: String)`、`with_id(current, id: String)`、`add_stage(&str)`、
//!   `insert<K: ToString, U: Send + Sync + 'static>`、`insert_unique`、
//!   `get<V: Send + Sync + 'static>(&str) -> Result<Arc<V>, String>`、
//!   `clone_unique<V: Clone + Send + Sync + 'static>`、
//!   `take_unique<V: Send + Sync + 'static>`、`stages() -> &Vec<String>` —— 签名均为
//!   契约：**不可**加 `Any` 约束、**不可**改为 `impl Into<String>`、**不可**返回 `&[String]`；
//! - `map<U: Send + Sync + 'static, F>` / `try_map<U, F, E>` / `transfer<U>` / `rejoin<U>`
//!   要求 `U: Send + Sync + 'static`（**不是** `U: Data`），保持比 `Data` 更宽的接受面。
//!
//! ## 实现要点
//! - 三段式头之外不引入任何额外 use；正文保持避免破坏 controller
//!   通过 `Mutex<HashMap<TypeId, ...>>` 实现的类型擦除存储语义。

use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use super::{AsyncEngineContext, AsyncEngineContextProvider, Data};
use crate::engine::AsyncEngineController;
use async_trait::async_trait;

use super::registry::Registry;

// === SECTION: Context<T> 数据载体 ===

pub struct Context<T: Data> {
    current: T,
    controller: Arc<Controller>, // TODO：以 Arc 形式持有
    registry: Registry,
    stages: Vec<String>,
}

impl<T: Send + Sync + 'static> Context<T> {
    // 使用初始数据创建一个新的上下文。
    pub fn new(current: T) -> Self {
        Context {
            current,
            controller: Arc::new(Controller::default()),
            registry: Registry::new(),
            stages: Vec::new(),
        }
    }

    pub fn rejoin<U: Send + Sync + 'static>(current: T, context: Context<U>) -> Self {
        Context {
            current,
            controller: context.controller,
            registry: context.registry,
            stages: context.stages,
        }
    }

    pub fn with_controller(current: T, controller: Controller) -> Self {
        Context {
            current,
            controller: Arc::new(controller),
            registry: Registry::new(),
            stages: Vec::new(),
        }
    }

    pub fn with_id(current: T, id: String) -> Self {
        Context {
            current,
            controller: Arc::new(Controller::new(id)),
            registry: Registry::new(),
            stages: Vec::new(),
        }
    }

    /// 获取上下文的 id。
    pub fn id(&self) -> &str {
        self.controller.id()
    }

    /// 获取上下文的内容。
    pub fn content(&self) -> &T {
        &self.current
    }

    pub fn controller(&self) -> &Controller {
        &self.controller
    }

    /// 以指定的 key 向 registry 中插入一个对象。
    pub fn insert<K: ToString, U: Send + Sync + 'static>(&mut self, key: K, value: U) {
        self.registry.insert_shared(key, value);
    }

    /// 以指定的 key 向 registry 中插入一个独占且可取出的对象。
    pub fn insert_unique<K: ToString, U: Send + Sync + 'static>(&mut self, key: K, value: U) {
        self.registry.insert_unique(key, value);
    }

    /// 按 key 与类型从 registry 中取回一个对象。
    pub fn get<V: Send + Sync + 'static>(&self, key: &str) -> Result<Arc<V>, String> {
        self.registry.get_shared(key)
    }

    /// 按 key 与类型从 registry 中克隆一个独占对象。
    pub fn clone_unique<V: Clone + Send + Sync + 'static>(&self, key: &str) -> Result<V, String> {
        self.registry.clone_unique(key)
    }

    /// 按 key 与类型从 registry 中取出一个独占对象。
    pub fn take_unique<V: Send + Sync + 'static>(&mut self, key: &str) -> Result<V, String> {
        self.registry.take_unique(key)
    }

    /// 在不更新 registry 的前提下将 Context 转移到一个新对象上。
    /// 返回一个元组，包含原对象与新的 Context。
    pub fn transfer<U: Send + Sync + 'static>(self, new_current: U) -> (T, Context<U>) {
        (
            self.current,
            Context {
                current: new_current,
                controller: self.controller,
                registry: self.registry,
                stages: self.stages,
            },
        )
    }

    /// 将当前对象与上下文分离开来。
    pub fn into_parts(self) -> (T, Context<()>) {
        self.transfer(())
    }

    pub fn stages(&self) -> &Vec<String> {
        &self.stages
    }

    pub fn add_stage(&mut self, stage: &str) {
        self.stages.push(stage.to_string());
    }

    /// 使用提供的函数将当前上下文变换为另一种类型。
    pub fn map<U: Send + Sync + 'static, F>(self, f: F) -> Context<U>
    where
        F: FnOnce(T) -> U,
    {
        // 使用 transfer 方法把当前值移出。
        let (current, temp_context) = self.transfer(());

        // 对当前值应用变换函数。
        let new_current = f(current);

        // 再次使用 transfer 以变换后的类型创建新上下文。
        temp_context.transfer(new_current).1
    }

    pub fn try_map<U, F, E>(self, f: F) -> Result<Context<U>, E>
    where
        F: FnOnce(T) -> Result<U, E>,
        U: Send + Sync + 'static,
    {
        // 使用 transfer 方法把当前值移出。
        let (current, temp_context) = self.transfer(());

        // 对当前值应用变换函数。
        let new_current = f(current)?;

        // 再次使用 transfer 以变换后的类型创建新上下文。
        Ok(temp_context.transfer(new_current).1)
    }
}

impl<T: Data> std::fmt::Debug for Context<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Context")
            .field("id", &self.controller.id())
            .finish()
    }
}

// 实现 Deref，使 Context<T> 可以像 &T 一样使用。
impl<T: Data> Deref for Context<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.current
    }
}

// 实现 DerefMut，使 Context<T> 可以像 &mut T 一样使用。
impl<T: Data> DerefMut for Context<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.current
    }
}

// 为 Context<T> 实现自定义 trait。
impl<T> From<T> for Context<T>
where
    T: Send + Sync + 'static,
{
    fn from(current: T) -> Self {
        Context::new(current)
    }
}

// 定义一个用于把 Context<T> 转换为 Context<U> 的自定义 trait。
pub trait IntoContext<U: Data> {
    fn into_context(self) -> Context<U>;
}

// 为将 Context<T> 转换为 Context<U> 实现这个自定义 trait。
impl<T, U> IntoContext<U> for Context<T>
where
    T: Send + Sync + 'static + Into<U>,
    U: Send + Sync + 'static,
{
    fn into_context(self) -> Context<U> {
        self.map(|current| current.into())
    }
}

impl<T: Data> AsyncEngineContextProvider for Context<T> {
    fn context(&self) -> Arc<dyn AsyncEngineContext> {
        self.controller.clone()
    }
}

// === SECTION: StreamContext 流式上下文 ===

#[derive(Debug, Clone)]
pub struct StreamContext {
    controller: Arc<Controller>,
    registry: Arc<Registry>,
    stages: Vec<String>,
}

impl StreamContext {
    fn new(controller: Arc<Controller>, registry: Registry) -> Self {
        StreamContext {
            controller,
            registry: Arc::new(registry),
            stages: Vec::new(),
        }
    }

    /// 按 key 与类型从 registry 中取回一个对象。
    pub fn get<V: Send + Sync + 'static>(&self, key: &str) -> Result<Arc<V>, String> {
        self.registry.get_shared(key)
    }

    /// 按 key 与类型从 registry 中克隆一个独占对象。
    pub fn clone_unique<V: Clone + Send + Sync + 'static>(&self, key: &str) -> Result<V, String> {
        self.registry.clone_unique(key)
    }

    pub fn registry(&self) -> Arc<Registry> {
        self.registry.clone()
    }

    pub fn stages(&self) -> &Vec<String> {
        &self.stages
    }

    pub fn add_stage(&mut self, stage: &str) {
        self.stages.push(stage.to_string());
    }
}

#[async_trait]
impl AsyncEngineContext for StreamContext {
    fn id(&self) -> &str {
        self.controller.id()
    }

    fn stop(&self) {
        self.controller.stop();
    }

    fn kill(&self) {
        self.controller.kill();
    }

    fn stop_generating(&self) {
        self.controller.stop_generating();
    }

    fn is_stopped(&self) -> bool {
        self.controller.is_stopped()
    }

    fn is_killed(&self) -> bool {
        self.controller.is_killed()
    }

    async fn stopped(&self) {
        self.controller.stopped().await
    }

    async fn killed(&self) {
        self.controller.killed().await
    }

    fn link_child(&self, child: Arc<dyn AsyncEngineContext>) {
        self.controller.link_child(child);
    }
}

impl AsyncEngineContextProvider for StreamContext {
    fn context(&self) -> Arc<dyn AsyncEngineContext> {
        self.controller.clone()
    }
}

impl<T: Send + Sync + 'static> From<Context<T>> for StreamContext {
    fn from(value: Context<T>) -> Self {
        StreamContext::new(value.controller, value.registry)
    }
}

// 此处待重构——取消传播的上下文控制逻辑后续可进一步抽象

use tokio::sync::watch::{Receiver, Sender, channel};

#[derive(Debug, Eq, PartialEq)]
enum State {
    Live,
    Stopped,
    Killed,
}

// === SECTION: Controller 取消传播控制器 ===

/// 带有取消传播能力的上下文实现。
#[derive(Debug)]
pub struct Controller {
    id: String,
    tx: Sender<State>,
    rx: Receiver<State>,
    child_context: Mutex<Vec<Arc<dyn AsyncEngineContext>>>,
}

impl Controller {
    pub fn new(id: String) -> Self {
        let (tx, rx) = channel(State::Live);
        Self {
            id,
            tx,
            rx,
            child_context: Mutex::new(Vec::new()),
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }
}

impl Default for Controller {
    fn default() -> Self {
        Self::new(uuid::Uuid::new_v4().to_string())
    }
}

impl AsyncEngineController for Controller {}

#[async_trait]
impl AsyncEngineContext for Controller {
    fn id(&self) -> &str {
        &self.id
    }

    fn is_stopped(&self) -> bool {
        *self.rx.borrow() != State::Live
    }

    fn is_killed(&self) -> bool {
        *self.rx.borrow() == State::Killed
    }

    async fn stopped(&self) {
        let mut rx = self.rx.clone();
        loop {
            if *rx.borrow_and_update() != State::Live || rx.changed().await.is_err() {
                return;
            }
        }
    }

    async fn killed(&self) {
        let mut rx = self.rx.clone();
        loop {
            if *rx.borrow_and_update() == State::Killed || rx.changed().await.is_err() {
                return;
            }
        }
    }

    fn stop_generating(&self) {
        // 复制 child 的 Arc，避免父节点意外挂到子节点下面时产生死锁。
        let children = self
            .child_context
            .lock()
            .expect("Failed to lock child context")
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        for child in children {
            child.stop_generating();
        }

        let _ = self.tx.send(State::Stopped);
    }

    fn stop(&self) {
        // 复制 child 的 Arc，避免父节点意外挂到子节点下面时产生死锁。
        let children = self
            .child_context
            .lock()
            .expect("Failed to lock child context")
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        for child in children {
            child.stop();
        }

        let _ = self.tx.send(State::Stopped);
    }

    fn kill(&self) {
        // 复制 child 的 Arc，避免父节点意外挂到子节点下面时产生死锁。
        let children = self
            .child_context
            .lock()
            .expect("Failed to lock child context")
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        for child in children {
            child.kill();
        }

        let _ = self.tx.send(State::Killed);
    }

    fn link_child(&self, child: Arc<dyn AsyncEngineContext>) {
        self.child_context
            .lock()
            .expect("Failed to lock child context")
            .push(child);
    }
}

// === SECTION: tests ===

#[cfg(test)]
mod tests {
    //! ## 测试矩阵
    //!
    //! | 测试名 | 覆盖维度 |
    //! |---|---|
    //! | `test_insert_and_get` | Registry 在 Context 内 insert/get 共享对象 |
    //! | `test_transfer` | 跨 Context 的 registry/controller 转移语义 |
    //! | `test_map` | `Context::map` 保留 controller/registry 同时变换载荷类型 |
    //! | `test_into_context` | `IntoContext` trait 将旧 Context 转为新载荷类型 |

    use super::*;

    #[derive(Debug, Clone)]
    struct Input {
        value: String,
    }

    #[derive(Debug, Clone)]
    struct Processed {
        length: usize,
    }

    #[derive(Debug, Clone)]
    struct Final {
        message: String,
    }

    impl From<Input> for Processed {
        fn from(input: Input) -> Self {
            Processed {
                length: input.value.len(),
            }
        }
    }

    impl From<Processed> for Final {
        fn from(processed: Processed) -> Self {
            Final {
                message: format!("Processed length: {}", processed.length),
            }
        }
    }

    #[test]
    fn test_insert_and_get() {
        let mut ctx = Context::new(Input {
            value: "Hello".to_string(),
        });

        ctx.insert("key1", 42);
        ctx.insert("key2", "some data".to_string());

        assert_eq!(*ctx.get::<i32>("key1").unwrap(), 42);
        assert_eq!(*ctx.get::<String>("key2").unwrap(), "some data");
        assert!(ctx.get::<f64>("key1").is_err()); // Testing a downcast failure
    }

    #[test]
    fn test_transfer() {
        let ctx = Context::new(Input {
            value: "Hello".to_string(),
        });

        let (input, ctx) = ctx.transfer(Processed { length: 5 });

        assert_eq!(input.value, "Hello");
        assert_eq!(ctx.length, 5);
    }

    #[test]
    fn test_map() {
        let ctx = Context::new(Input {
            value: "Hello".to_string(),
        });

        let ctx: Context<Processed> = ctx.map(|input| input.into());
        let ctx: Context<Final> = ctx.map(|processed| processed.into());

        assert_eq!(ctx.current.message, "Processed length: 5");
    }

    #[test]
    fn test_into_context() {
        let ctx = Context::new(Input {
            value: "Hello".to_string(),
        });

        let ctx: Context<Processed> = ctx.into_context();
        let ctx: Context<Final> = ctx.into_context();

        assert_eq!(ctx.current.message, "Processed length: 5");
    }
}
