// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 通用 RAII 对象池。

use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

/// 对象归还接口。
pub trait Returnable {
    fn return_to_pool(self);
}

/// 对象归还句柄。
pub trait ReturnHandle {
    type Item;
    fn return_item(&self, item: Self::Item);
}

/// 通用对象池。
pub struct Pool<T> {
    items: Arc<Mutex<Vec<T>>>,
}

impl<T> Pool<T> {
    pub fn new() -> Self {
        Self {
            items: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            items: Arc::new(Mutex::new(Vec::with_capacity(capacity))),
        }
    }

    /// 从池中借出一个对象（无空闲则返回 None）。
    pub fn checkout(&self) -> Option<PoolGuard<T>> {
        let mut items = self.items.lock().unwrap();
        items.pop().map(|item| PoolGuard {
            item: Some(item),
            pool: Arc::clone(&self.items),
        })
    }

    /// 归还对象到池中。
    pub fn checkin(&self, item: T) {
        let mut items = self.items.lock().unwrap();
        items.push(item);
    }

    pub fn available(&self) -> usize {
        self.items.lock().unwrap().len()
    }
}

impl<T> Default for Pool<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Clone for Pool<T> {
    fn clone(&self) -> Self {
        Self {
            items: Arc::clone(&self.items),
        }
    }
}

/// RAII 借出句柄，drop 时自动归还。
pub struct PoolGuard<T> {
    item: Option<T>,
    pool: Arc<Mutex<Vec<T>>>,
}

impl<T> Deref for PoolGuard<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.item.as_ref().unwrap()
    }
}

impl<T> DerefMut for PoolGuard<T> {
    fn deref_mut(&mut self) -> &mut T {
        self.item.as_mut().unwrap()
    }
}

impl<T> Drop for PoolGuard<T> {
    fn drop(&mut self) {
        if let Some(item) = self.item.take() {
            let mut items = self.pool.lock().unwrap();
            items.push(item);
        }
    }
}
