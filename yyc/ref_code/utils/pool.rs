use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};

pub trait Returnable: Send + Sync + 'static {
    fn on_return(&mut self) {}
}

pub enum PoolValue<T: Returnable> {
    Boxed(Box<T>),
    Direct(T),
}

impl<T: Returnable> PoolValue<T> {
    pub fn from_boxed(value: Box<T>) -> Self {
        Self::Boxed(value)
    }

    pub fn from_direct(value: T) -> Self {
        Self::Direct(value)
    }

    pub fn get(&self) -> &T {
        match self {
            Self::Boxed(value) => value.as_ref(),
            Self::Direct(value) => value,
        }
    }

    pub fn get_mut(&mut self) -> &mut T {
        match self {
            Self::Boxed(value) => value.as_mut(),
            Self::Direct(value) => value,
        }
    }

    pub fn on_return(&mut self) {
        self.get_mut().on_return();
    }
}

pub trait ReturnHandle<T: Returnable>: Send + Sync + 'static {
    fn return_to_pool(&self, value: PoolValue<T>);
}

pub trait PoolExt<T: Returnable>: Send + Sync + 'static {
    fn create_pool_item(
        &self,
        value: PoolValue<T>,
        handle: Arc<dyn ReturnHandle<T>>,
    ) -> PoolItem<T> {
        PoolItem::new(value, handle)
    }
}

struct PoolItemInner<T: Returnable> {
    value: Mutex<Option<PoolValue<T>>>,
    handle: Arc<dyn ReturnHandle<T>>,
    returned: AtomicBool,
}

impl<T: Returnable> PoolItemInner<T> {
    fn return_once(&self) {
        if self.returned.swap(true, Ordering::SeqCst) {
            return;
        }
        if let Some(mut value) = self.value.lock().expect("pool item poisoned").take() {
            value.on_return();
            self.handle.return_to_pool(value);
        }
    }
}

pub struct PoolItem<T: Returnable> {
    inner: Arc<PoolItemInner<T>>,
    consumed: bool,
}

impl<T: Returnable> PoolItem<T> {
    pub fn new(value: PoolValue<T>, handle: Arc<dyn ReturnHandle<T>>) -> Self {
        Self {
            inner: Arc::new(PoolItemInner {
                value: Mutex::new(Some(value)),
                handle,
                returned: AtomicBool::new(false),
            }),
            consumed: false,
        }
    }

    pub fn into_shared(mut self) -> SharedPoolItem<T> {
        self.consumed = true;
        SharedPoolItem {
            inner: self.inner.clone(),
        }
    }

    pub fn has_value(&self) -> bool {
        self.inner.value.lock().expect("pool item poisoned").is_some()
    }

    pub fn get(&self) -> PoolItemReadGuard<'_, T> {
        PoolItemReadGuard {
            guard: self.inner.value.lock().expect("pool item poisoned"),
        }
    }
}

impl<T: Returnable> Drop for PoolItem<T> {
    fn drop(&mut self) {
        if !self.consumed {
            self.inner.return_once();
        }
    }
}

pub struct PoolItemReadGuard<'a, T: Returnable> {
    guard: std::sync::MutexGuard<'a, Option<PoolValue<T>>>,
}

impl<'a, T: Returnable> std::ops::Deref for PoolItemReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.guard.as_ref().expect("pool item missing value").get()
    }
}

pub struct SharedPoolItem<T: Returnable> {
    inner: Arc<PoolItemInner<T>>,
}

impl<T: Returnable> SharedPoolItem<T> {
    pub fn get(&self) -> PoolItemReadGuard<'_, T> {
        PoolItemReadGuard {
            guard: self.inner.value.lock().expect("shared pool item poisoned"),
        }
    }

    pub fn strong_count(&self) -> usize {
        Arc::strong_count(&self.inner)
    }
}

impl<T: Returnable> Clone for SharedPoolItem<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: Returnable> Drop for SharedPoolItem<T> {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) == 1 {
            self.inner.return_once();
        }
    }
}

struct PoolState<T: Returnable> {
    pool: Arc<Mutex<VecDeque<PoolValue<T>>>>,
    available: Arc<Condvar>,
}

#[derive(Clone)]
struct QueueReturnHandle<T: Returnable> {
    state: Arc<PoolState<T>>,
}

impl<T: Returnable> ReturnHandle<T> for QueueReturnHandle<T> {
    fn return_to_pool(&self, value: PoolValue<T>) {
        let mut pool = self.state.pool.lock().expect("pool poisoned");
        pool.push_back(value);
        self.state.available.notify_one();
    }
}

pub struct Pool<T: Returnable> {
    state: Arc<PoolState<T>>,
}

impl<T: Returnable> PoolExt<T> for Pool<T> {}

impl<T: Returnable> Pool<T> {
    pub fn new(initial_elements: Vec<PoolValue<T>>) -> Self {
        Self {
            state: Arc::new(PoolState {
                pool: Arc::new(Mutex::new(initial_elements.into())),
                available: Arc::new(Condvar::new()),
            }),
        }
    }

    pub fn new_boxed(initial_elements: Vec<Box<T>>) -> Self {
        Self::new(initial_elements.into_iter().map(PoolValue::from_boxed).collect())
    }

    pub fn new_direct(initial_elements: Vec<T>) -> Self {
        Self::new(initial_elements.into_iter().map(PoolValue::from_direct).collect())
    }

    pub(crate) fn try_acquire(&self) -> Option<PoolItem<T>> {
        let mut pool = self.state.pool.lock().expect("pool poisoned");
        pool.pop_front().map(|value| {
            self.create_pool_item(value, Arc::new(QueueReturnHandle { state: self.state.clone() }))
        })
    }

    pub(crate) fn acquire(&self) -> PoolItem<T> {
        let mut pool = self.state.pool.lock().expect("pool poisoned");
        loop {
            if let Some(value) = pool.pop_front() {
                return self.create_pool_item(
                    value,
                    Arc::new(QueueReturnHandle { state: self.state.clone() }),
                );
            }
            pool = self.state.available.wait(pool).expect("pool poisoned");
        }
    }
    pub fn acquire_for_test(&self) -> PoolItem<T> {
        self.acquire()
    }

    pub fn try_acquire_for_test(&self) -> Option<PoolItem<T>> {
        self.try_acquire()
    }
}

struct SyncPoolState<T: Returnable> {
    pool: Mutex<VecDeque<PoolValue<T>>>,
    available: Condvar,
}

pub struct SyncPool<T: Returnable> {
    state: Arc<SyncPoolState<T>>,
    capacity: usize,
}

pub struct SyncPoolItem<T: Returnable> {
    value: Option<PoolValue<T>>,
    state: Arc<SyncPoolState<T>>,
}

impl<T: Returnable> SyncPool<T> {
    pub fn new(initial_elements: Vec<PoolValue<T>>) -> Self {
        let capacity = initial_elements.len();
        Self {
            state: Arc::new(SyncPoolState {
                pool: Mutex::new(initial_elements.into()),
                available: Condvar::new(),
            }),
            capacity,
        }
    }

    pub fn new_direct(initial_elements: Vec<T>) -> Self {
        Self::new(initial_elements.into_iter().map(PoolValue::from_direct).collect())
    }

    pub fn try_acquire(&self) -> Option<SyncPoolItem<T>> {
        let mut pool = self.state.pool.lock().expect("sync pool poisoned");
        pool.pop_front().map(|value| SyncPoolItem {
            value: Some(value),
            state: self.state.clone(),
        })
    }

    pub fn acquire_blocking(&self) -> SyncPoolItem<T> {
        let mut pool = self.state.pool.lock().expect("sync pool poisoned");
        loop {
            if let Some(value) = pool.pop_front() {
                return SyncPoolItem {
                    value: Some(value),
                    state: self.state.clone(),
                };
            }
            pool = self.state.available.wait(pool).expect("sync pool poisoned");
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

impl<T: Returnable> SyncPoolItem<T> {
    pub fn get(&self) -> &T {
        self.value.as_ref().expect("sync pool item missing value").get()
    }
}

impl<T: Returnable> Drop for SyncPoolItem<T> {
    fn drop(&mut self) {
        if let Some(mut value) = self.value.take() {
            value.on_return();
            let mut pool = self.state.pool.lock().expect("sync pool poisoned");
            pool.push_back(value);
            self.state.available.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct SampleValue {
        value: usize,
        returned: Arc<AtomicBool>,
    }

    impl Returnable for SampleValue {
        fn on_return(&mut self) {
            self.returned.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn pool_item_returns_value_on_drop() {
        let returned = Arc::new(AtomicBool::new(false));
        let pool = Pool::new_direct(vec![SampleValue {
            value: 7,
            returned: returned.clone(),
        }]);

        {
            let item = pool.acquire_for_test();
            assert_eq!(item.get().value, 7);
        }

        assert!(returned.load(Ordering::SeqCst));
        assert!(pool.try_acquire_for_test().is_some());
    }

    #[test]
    fn shared_pool_item_keeps_value_until_last_reference() {
        let returned = Arc::new(AtomicBool::new(false));
        let pool = Pool::new_direct(vec![SampleValue {
            value: 3,
            returned: returned.clone(),
        }]);
        let shared = pool.acquire_for_test().into_shared();
        let clone = shared.clone();

        assert_eq!(shared.get().value, 3);
        assert_eq!(clone.strong_count(), 2);
        drop(shared);
        assert!(!returned.load(Ordering::SeqCst));
        drop(clone);
        assert!(returned.load(Ordering::SeqCst));
    }

    #[test]
    fn sync_pool_blocks_and_returns_values() {
        let returned = Arc::new(AtomicBool::new(false));
        let pool = SyncPool::new_direct(vec![SampleValue {
            value: 9,
            returned: returned.clone(),
        }]);

        {
            let item = pool.acquire_blocking();
            assert_eq!(item.get().value, 9);
        }

        assert!(returned.load(Ordering::SeqCst));
    }
}