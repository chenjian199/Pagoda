// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 设计意图
//! 提供「异步 / 同步」两套通用对象池，复用昂贵构造的实例（如 KV-cache 块、
//! 大张量缓冲）。设计强调 `Drop` 自动归还，调用方零运行时心智负担。
//!
//! # 外部契约
//! - `trait Returnable + 'static`：池元素接口，`on_return` 提供归还回调；
//! - `trait ReturnHandle<T>`：归还器接口，负责把 `PoolValue<T>` 放回容器；
//! - `Pool<T>` / `SyncPool<T>`：分别是 `tokio::Notify` 与 `Condvar` 版本；
//! - `PoolItem<T>` / `SyncPoolItem<T>` / `SharedPoolItem<T>`：
//!   池借出句柄，Drop 时自动归还；`SharedPoolItem` 是只读共享变体；
//! - `PoolExt<T>`：扩展 trait，统一 `acquire` 异步 API。
//!
//! # 实现要点
//! - 内部容器统一为 `VecDeque<PoolValue<T>>`，允许 `Boxed` / `Direct` 混存；
//! - 异步版借助 `Notify::notified()` 实现“无 spin 等待”，同步版用
//!   `Condvar::wait_while` 阻塞；
//! - 私有 `mod private` 用来封装 `Sealed` 模式，防止 `PoolExt` 被外部实现；
//! - 归还路径在 `Drop` 中调用 `ReturnHandle`，避免业务代码忘记归还。

use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::{Condvar, Mutex};
use tokio::sync::Notify;

// === SECTION: 基础 trait 与值容器 ===

/// 可被放回对象池的元素接口。
pub trait Returnable: Send + Sync + 'static {
    /// 元素归还到池中时触发的回调。
    fn on_return(&mut self) {}
}

/// 对象归还句柄接口。
pub trait ReturnHandle<T: Returnable>: Send + Sync + 'static {
    /// 把元素归还到池中。
    fn return_to_pool(&self, value: PoolValue<T>);
}

/// 统一持有 `Box<T>` 或直接值的包装类型。
pub enum PoolValue<T: Returnable> {
    Boxed(Box<T>),
    Direct(T),
}

impl<T: Returnable> PoolValue<T> {
    /// 从装箱元素构造 `PoolValue`。
    pub fn from_boxed(value: Box<T>) -> Self {
        Self::Boxed(value)
    }

    /// 从直接值构造 `PoolValue`。
    pub fn from_direct(value: T) -> Self {
        Self::Direct(value)
    }

    /// 读取底层元素的不可变引用。
    pub fn get(&self) -> &T {
        match self {
            Self::Boxed(boxed) => boxed.as_ref(),
            Self::Direct(direct) => direct,
        }
    }

    /// 读取底层元素的可变引用。
    pub fn get_mut(&mut self) -> &mut T {
        match self {
            Self::Boxed(boxed) => boxed.as_mut(),
            Self::Direct(direct) => direct,
        }
    }

    /// 在底层元素上执行归还回调。
    pub fn on_return(&mut self) {
        let value = self.get_mut();
        value.on_return();
    }
}

impl<T: Returnable> Deref for PoolValue<T> {
    type Target = T;

    /// 将包装值透明地解引用到底层元素。
    fn deref(&self) -> &Self::Target {
        self.get()
    }
}

impl<T: Returnable> DerefMut for PoolValue<T> {
    /// 将包装值透明地可变解引用到底层元素。
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.get_mut()
    }
}

// 私有模块，用于限制 PoolItem 的构造权限。
mod private {
    // 该类型只能在本模块内部构造。
    #[derive(Clone, Copy)]
    pub struct PoolItemToken(());

    impl PoolItemToken {
        pub(super) fn new() -> Self {
            PoolItemToken(())
        }
    }
}

/// 定义对象池扩展能力的核心 trait。
// === SECTION: PoolExt 扩展 trait ===

pub trait PoolExt<T: Returnable>: Send + Sync + 'static {
    /// 为实现者提供受控的 `PoolItem` 构造入口。
    fn create_pool_item(
        &self,
        value: PoolValue<T>,
        handle: Arc<dyn ReturnHandle<T>>,
    ) -> PoolItem<T> {
        let item = PoolItem::new(value, handle);
        item
    }
}

/// 从对象池借出的独占元素。
pub struct PoolItem<T: Returnable> {
    value: Option<PoolValue<T>>,
    handle: Arc<dyn ReturnHandle<T>>,
    _token: private::PoolItemToken,
}

impl<T: Returnable> PoolItem<T> {
    /// 在模块内部构造一个新的池元素句柄。
    fn new(value: PoolValue<T>, handle: Arc<dyn ReturnHandle<T>>) -> Self {
        let token = private::PoolItemToken::new();

        Self {
            value: Some(value),
            handle,
            _token: token,
        }
    }

    /// 把独占池元素转换为共享引用版本。
    pub fn into_shared(self) -> SharedPoolItem<T> {
        let inner = Arc::new(self);
        SharedPoolItem { inner }
    }

    /// 判断当前句柄中是否仍持有实际值。
    pub fn has_value(&self) -> bool {
        matches!(self.value, Some(_))
    }
}

impl<T: Returnable> Deref for PoolItem<T> {
    type Target = T;

    /// 透明解引用到底层池元素。
    fn deref(&self) -> &Self::Target {
        self.value.as_ref().unwrap().get()
    }
}

impl<T: Returnable> DerefMut for PoolItem<T> {
    /// 透明可变解引用到底层池元素。
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.value.as_mut().unwrap().get_mut()
    }
}

impl<T: Returnable> Drop for PoolItem<T> {
    /// 在独占句柄销毁时执行归还回调并把元素放回池中。
    fn drop(&mut self) {
        if let Some(mut value) = self.value.take() {
            value.on_return();
            self.handle.return_to_pool(value);
        }
    }
}

/// 对池元素的共享引用包装。
pub struct SharedPoolItem<T: Returnable> {
    inner: Arc<PoolItem<T>>,
}

impl<T: Returnable> Clone for SharedPoolItem<T> {
    /// 克隆共享池元素句柄，增加共享引用计数。
    fn clone(&self) -> Self {
        let inner = Arc::clone(&self.inner);
        Self { inner }
    }
}

impl<T: Returnable> SharedPoolItem<T> {
    /// 获取底层池元素引用。
    pub fn get(&self) -> &T {
        self.inner
            .value
            .as_ref()
            .expect("shared pool item should always contain a value")
            .get()
    }

    /// 返回当前共享句柄的强引用计数。
    pub fn strong_count(&self) -> usize {
        let count = Arc::strong_count(&self.inner);
        count
    }
}

impl<T: Returnable> Deref for SharedPoolItem<T> {
    type Target = T;

    /// 透明解引用到底层共享元素。
    fn deref(&self) -> &Self::Target {
        self.inner.value.as_ref().unwrap().get()
    }
}

/// 异步对象池实现。
// === SECTION: 异步对象池 Pool ===

pub struct Pool<T: Returnable> {
    state: Arc<PoolState<T>>,
    capacity: usize,
}

struct PoolState<T: Returnable> {
    pool: Arc<Mutex<VecDeque<PoolValue<T>>>>,
    available: Arc<Notify>,
}

impl<T: Returnable> ReturnHandle<T> for PoolState<T> {
    /// 把归还的元素重新压回队列，并通知一个等待者。
    fn return_to_pool(&self, value: PoolValue<T>) {
        let mut pool = self.pool.lock().unwrap();
        pool.push_back(value);
        self.available.notify_one();
    }
}

impl<T: Returnable> Pool<T> {
    /// 使用初始元素集合创建对象池。
    pub fn new(initial_elements: Vec<PoolValue<T>>) -> Self {
        let capacity = initial_elements.len();
        let pool = initial_elements.into_iter().collect::<VecDeque<_>>();
        let pool = Arc::new(Mutex::new(pool));
        let available = Arc::new(Notify::new());
        let state = Arc::new(PoolState { pool, available });

        Self { state, capacity }
    }

    /// 使用装箱元素创建对象池。
    pub fn new_boxed(initial_elements: Vec<Box<T>>) -> Self {
        let initial_values = initial_elements.into_iter().map(PoolValue::from_boxed).collect();
        Self::new(initial_values)
    }

    /// 使用直接值元素创建对象池。
    pub fn new_direct(initial_elements: Vec<T>) -> Self {
        let initial_values = initial_elements.into_iter().map(PoolValue::from_direct).collect();
        Self::new(initial_values)
    }

    /// 尝试非阻塞地获取一个池元素。
    async fn try_acquire(&self) -> Option<PoolItem<T>> {
        let mut pool = self.state.pool.lock().unwrap();
        pool.pop_front().map(|value| {
            let handle: Arc<dyn ReturnHandle<T>> = self.state.clone();
            PoolItem::new(value, handle)
        })
    }

    /// 等待直到对象池中出现可用元素。
    async fn acquire(&self) -> PoolItem<T> {
        loop {
            if let Some(guard) = self.try_acquire().await {
                return guard;
            }
            self.state.available.notified().await;
        }
    }

    /// 主动通知有元素已归还。
    fn notify_return(&self) {
        let available = &self.state.available;
        available.notify_one();
    }

    /// 返回对象池容量。
    fn capacity(&self) -> usize {
        let capacity = self.capacity;
        capacity
    }
}

impl<T: Returnable> PoolExt<T> for Pool<T> {}

impl<T: Returnable> Clone for Pool<T> {
    /// 克隆对象池句柄，共享底层状态。
    fn clone(&self) -> Self {
        let state = Arc::clone(&self.state);
        let capacity = self.capacity;

        Self { state, capacity }
    }
}

/// 同步对象池实现。
// === SECTION: 同步对象池 SyncPool ===

pub struct SyncPool<T: Returnable> {
    state: Arc<SyncPoolState<T>>,
    capacity: usize,
}

struct SyncPoolState<T: Returnable> {
    pool: Mutex<VecDeque<PoolValue<T>>>,
    available: Condvar,
}

impl<T: Returnable> SyncPool<T> {
    /// 使用初始元素集合创建同步对象池。
    pub fn new(initial_elements: Vec<PoolValue<T>>) -> Self {
        let capacity = initial_elements.len();
        let pool = initial_elements.into_iter().collect::<VecDeque<_>>();
        let state = Arc::new(SyncPoolState {
            pool: Mutex::new(pool),
            available: Condvar::new(),
        });

        Self { state, capacity }
    }

    /// 使用直接值元素创建同步对象池。
    pub fn new_direct(initial_elements: Vec<T>) -> Self {
        let initial_values = initial_elements.into_iter().map(PoolValue::from_direct).collect();
        Self::new(initial_values)
    }

    /// 尝试非阻塞地获取一个同步池元素。
    pub fn try_acquire(&self) -> Option<SyncPoolItem<T>> {
        let mut pool = self.state.pool.lock().unwrap();
        pool.pop_front().map(|value| SyncPoolItem::new(value, Arc::clone(&self.state)))
    }

    /// 阻塞当前线程，直到拿到一个同步池元素。
    pub fn acquire_blocking(&self) -> SyncPoolItem<T> {
        let mut pool = self.state.pool.lock().unwrap();

        while pool.is_empty() {
            tracing::debug!("SyncPool: waiting for available resource (pool empty)");
            pool = self.state.available.wait(pool).unwrap();
            tracing::debug!(
                "SyncPool: woke up, checking pool again (size: {})",
                pool.len()
            );
        }

        let value = pool.pop_front().unwrap();
        tracing::debug!("SyncPool: acquired resource, pool size now: {}", pool.len());
        SyncPoolItem::new(value, Arc::clone(&self.state))
    }

    /// 返回同步对象池容量。
    pub fn capacity(&self) -> usize {
        let capacity = self.capacity;
        capacity
    }
}

impl<T: Returnable> Clone for SyncPool<T> {
    /// 克隆同步对象池句柄，共享底层状态。
    fn clone(&self) -> Self {
        let state = Arc::clone(&self.state);
        let capacity = self.capacity;

        Self { state, capacity }
    }
}

/// 同步对象池中借出的元素句柄。
pub struct SyncPoolItem<T: Returnable> {
    value: Option<PoolValue<T>>,
    state: Arc<SyncPoolState<T>>,
}

impl<T: Returnable> SyncPoolItem<T> {
    /// 在模块内部构造同步池元素句柄。
    fn new(value: PoolValue<T>, state: Arc<SyncPoolState<T>>) -> Self {
        Self { value: Some(value), state }
    }
}

impl<T: Returnable> Deref for SyncPoolItem<T> {
    type Target = T;

    /// 透明解引用到底层同步池元素。
    fn deref(&self) -> &Self::Target {
        self.value.as_ref().unwrap().get()
    }
}

impl<T: Returnable> DerefMut for SyncPoolItem<T> {
    /// 透明可变解引用到底层同步池元素。
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.value.as_mut().unwrap().get_mut()
    }
}

impl<T: Returnable> Drop for SyncPoolItem<T> {
    /// 在同步池元素释放时归还对象并唤醒等待线程。
    fn drop(&mut self) {
        if let Some(mut value) = self.value.take() {
            value.on_return();

            let mut pool = self.state.pool.lock().unwrap();
            pool.push_back(value);
            tracing::debug!(
                "SyncPool: returned resource, pool size now: {}, notifying waiters",
                pool.len()
            );

            self.state.available.notify_one();
        }
    }
}
// === SECTION: tests ===

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use tokio::time::{Duration, timeout};

    // 为 u32 提供测试用 Returnable 实现。
    impl Returnable for u32 {
        /// 归还时把测试值重置为 0。
        fn on_return(&mut self) {
            *self = 0;
            tracing::debug!("Resetting u32 to 0");
        }
    }

    #[tokio::test]
    async fn test_acquire_release() {
        // 测试异步对象池的获取、修改、归还和再次获取流程。
        let initial_elements = vec![
            PoolValue::Direct(1),
            PoolValue::Direct(2),
            PoolValue::Direct(3),
            PoolValue::Direct(4),
            PoolValue::Direct(5),
        ];
        let pool = Pool::new(initial_elements);

        if let Some(mut item) = pool.try_acquire().await {
            assert_eq!(*item, 1); // It should be the first element we put in

            *item += 10;
            assert_eq!(*item, 11);

        }

        let mut values = Vec::new();
        let mut items = Vec::new();
        while let Some(item) = pool.try_acquire().await {
            values.push(*item);
            items.push(item);
        }

        assert_eq!(values, vec![2, 3, 4, 5, 0]);

        let pool_clone = pool.clone();
        let task = tokio::spawn(async move {
            let first_acquired = pool_clone.acquire().await;
            assert_eq!(*first_acquired, 0);
        });

        timeout(Duration::from_secs(1), task)
            .await
            .expect_err("Expected timeout");

        items.clear();

        let pool_clone = pool.clone();
        let task = tokio::spawn(async move {
            let first_acquired = pool_clone.acquire().await;
            assert_eq!(*first_acquired, 0);
        });

        timeout(Duration::from_secs(1), task)
            .await
            .expect("Task did not complete in time")
            .unwrap();
    }

    #[tokio::test]
    async fn test_shared_items() {
        // 测试共享池元素在多引用场景下的生命周期与归还行为。
        let initial_elements = vec![
            PoolValue::Direct(1),
            // PoolValue::Direct(2),
            // PoolValue::Direct(3),
        ];
        let pool = Pool::new(initial_elements);

        let mut item = pool.acquire().await;
        *item += 10; // Modify before sharing
        let shared = item.into_shared();
        assert_eq!(*shared, 11);

        let shared_clone = shared.clone();
        assert_eq!(*shared_clone, 11);

        drop(shared);

        assert_eq!(*shared_clone, 11);

        drop(shared_clone);

        let item = pool.acquire().await;
        assert_eq!(*item, 0); // Value should be on_return
    }

    #[tokio::test]
    async fn test_boxed_values() {
        // 测试装箱元素在对象池中的获取与归还。
        let initial_elements = vec![
            PoolValue::Boxed(Box::new(1)),
            // PoolValue::Boxed(Box::new(2)),
            // PoolValue::Boxed(Box::new(3)),
        ];
        let pool = Pool::new(initial_elements);

        let mut item = pool.acquire().await;
        assert_eq!(*item, 1);

        *item += 10;
        drop(item);

        let item = pool.acquire().await;
        assert_eq!(*item, 0);
    }

    #[tokio::test]
    async fn test_pool_item_creation() {
        // 测试池元素只能通过对象池创建。
        let pool = Pool::new(vec![PoolValue::Direct(1)]);

        let item = pool.acquire().await;
        assert_eq!(*item, 1);

        // let invalid_item = PoolItem {
        //     value: Some(PoolValue::Direct(2)),
        //     pool: pool.clone(),
        //     _token: /* can't create this */
        // };
    }

    #[tokio::test]
    async fn test_pool_helper_methods_and_shared_counts() {
        // 测试辅助方法与共享引用计数。
        let pool = Pool::new_boxed(vec![Box::new(7_u32)]);

        assert_eq!(pool.capacity(), 1);

        let item = pool.acquire().await;
        assert!(item.has_value());

        let shared = item.into_shared();
        assert_eq!(shared.strong_count(), 1);
        let shared_clone = shared.clone();
        assert_eq!(shared.strong_count(), 2);
        assert_eq!(*shared.get(), 7);

        drop(shared_clone);
        drop(shared);

        let reacquired = pool.acquire().await;
        assert_eq!(*reacquired, 0);
    }

    #[tokio::test]
    async fn test_pool_value_accessors_for_boxed_and_direct() {
        // 测试 PoolValue 在装箱和值类型下的访问器行为。
        let mut direct = PoolValue::from_direct(5_u32);
        let mut boxed = PoolValue::from_boxed(Box::new(9_u32));

        assert_eq!(*direct.get(), 5);
        assert_eq!(*boxed.get(), 9);

        *direct.get_mut() = 6;
        *boxed.get_mut() = 10;

        direct.on_return();
        boxed.on_return();

        assert_eq!(*direct.get(), 0);
        assert_eq!(*boxed.get(), 0);
    }

    #[test]
    fn test_sync_pool_capacity_accessor() {
        // 测试同步对象池容量读取。
        let pool = SyncPool::new_direct(vec![1_u32, 2_u32, 3_u32]);

        assert_eq!(pool.capacity(), 3);
    }

    #[test]
    fn test_sync_pool_basic_acquire_release() {
        // 测试同步对象池的基础获取与归还。
        let initial_elements = vec![1u32, 2, 3];
        let pool = SyncPool::new_direct(initial_elements);

        let item1 = pool.try_acquire().unwrap();
        assert_eq!(*item1, 1);

        let item2 = pool.try_acquire().unwrap();
        assert_eq!(*item2, 2);

        let item3 = pool.try_acquire().unwrap();
        assert_eq!(*item3, 3);

        assert!(pool.try_acquire().is_none());

        drop(item1); // Returns 0 (after on_return)
        drop(item2); // Returns 0 (after on_return)
        drop(item3); // Returns 0 (after on_return)

        let item = pool.try_acquire().unwrap();
        assert_eq!(*item, 0); // Value was reset by on_return
    }

    #[test]
    fn test_sync_pool_blocking_acquire() {
        // 测试同步对象池在资源不足时会阻塞等待。
        let pool = SyncPool::new_direct(vec![42u32]);

        let item = pool.acquire_blocking();
        assert_eq!(*item, 42);

        let pool_clone = pool.clone();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let handle = thread::spawn(move || {
            counter_clone.store(1, Ordering::SeqCst); // Mark that we're waiting
            let waiting_item = pool_clone.acquire_blocking(); // This will block
            counter_clone.store(2, Ordering::SeqCst); // Mark that we got it
            assert_eq!(*waiting_item, 0); // Should be reset value
        });

        thread::sleep(Duration::from_millis(10));
        assert_eq!(counter.load(Ordering::SeqCst), 1); // Should be waiting

        drop(item);

        handle.join().unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 2); // Should have completed
    }

    #[test]
    fn test_sync_pool_multiple_waiters() {
        // 测试多个等待线程会被依次唤醒并完成获取。
        let pool = SyncPool::new_direct(vec![1u32]);

        let item = pool.acquire_blocking();

        let pool_clone1 = pool.clone();
        let pool_clone2 = pool.clone();
        let completed = Arc::new(AtomicUsize::new(0));
        let completed1 = completed.clone();
        let completed2 = completed.clone();

        let handle1 = thread::spawn(move || {
            let _item = pool_clone1.acquire_blocking(); // Will block
            completed1.fetch_add(1, Ordering::SeqCst); // Mark completion
            // Item drops here, potentially waking thread 2
        });

        let handle2 = thread::spawn(move || {
            let _item = pool_clone2.acquire_blocking(); // Will block
            completed2.fetch_add(1, Ordering::SeqCst); // Mark completion
            // Item drops here
        });

        thread::sleep(Duration::from_millis(50));
        assert_eq!(completed.load(Ordering::SeqCst), 0); // Both should be waiting

        drop(item);

        handle1.join().unwrap();
        handle2.join().unwrap();

        assert_eq!(completed.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn test_sync_vs_async_pool_compatibility() {
        // 测试同步池与异步池能复用同一 Returnable 类型。
        let async_pool = Pool::new_direct(vec![1u32, 2u32]);
        let sync_pool = SyncPool::new_direct(vec![3u32, 4u32]);

        let async_rt = tokio::runtime::Runtime::new().unwrap();
        let async_item = async_rt.block_on(async { async_pool.acquire().await });
        assert_eq!(*async_item, 1);

        let sync_item = sync_pool.acquire_blocking();
        assert_eq!(*sync_item, 3);

        drop(async_item); // Should reset to 0
        drop(sync_item); // Should reset to 0
    }

    #[test]
    fn test_sync_pool_condvar_performance() {
        // 测试同步对象池在高频获取/归还下的基本性能表现。
        let pool = SyncPool::new_direct((0..10).collect::<Vec<u32>>());
        let start = std::time::Instant::now();

        for _ in 0..1000 {
            let item = pool.acquire_blocking();
            let _ = *item + 1;
            drop(item);
        }

        let duration = start.elapsed();
        println!("1000 sync pool operations took {:?}", duration);

        // 这里仅做粗粒度上限约束，避免测试环境抖动导致误报。
        assert!(duration < Duration::from_millis(200));
    }
}
