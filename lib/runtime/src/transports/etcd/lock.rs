// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 设计意图
//! 在 etcd 上实现一个进程间的"读写互斥锁"：多个 reader 可并存，但 writer 排他。
//! 所有原子性都借助 etcd 的事务 (`Txn::when(...).and_then(...)`) 完成，避免 TOCTOU。
//!
//! # 外部契约
//! - `DistributedRWLock { lock_prefix }` 公开字段语义稳定，等价 `Clone`；
//! - 锁键空间使用 *wire-level* 固定布局，是跨版本兼容的承诺：
//!     - 写锁：`v1/{lock_prefix}/writer`
//!     - 读锁：`v1/{lock_prefix}/readers/{reader_id}`
//! - `try_write_lock(client)` 非阻塞，返回 `Option<WriteLockGuard<'_>>`；
//! - `read_lock_with_wait(client, reader_id, timeout)` 按 100ms 轮询，超时返回 Err；
//! - Drop 实现必须在 tokio runtime 内 spawn 清理任务；当不在 runtime 中时 *只能* 记录
//!   错误，不能阻塞，依赖 etcd lease 过期兜底；
//! - `DEFAULT_READ_LOCK_TIMEOUT_SECS = 30` 当 `timeout = None` 时使用。
//!
//! # 实现要点
//! - 所有 etcd 写入都绑定到当前 client 的 lease，断连后由 etcd 自动清理；
//! - `try_write_lock` 采用 "先 PUT writer / 再扫描 readers / 检测到读者则回滚" 的策略——
//!   存在亚毫秒级竞争窗口，但配合 reader 的原子事务可保证一致性；
//! - `read_lock_with_wait` 使用 `Txn::when(writer.version == 0).and_then(put reader)`
//!   单原子操作完成检测 + 写入，writer 一旦出现就会让事务失败并重试。

use std::time::Duration;

use anyhow::Result;
use etcd_client::{Compare, CompareOp, PutOptions, Txn, TxnOp};

use super::Client;

/// 缺省读锁超时（秒）。当调用方传入 `None` 时使用该值。
const DEFAULT_READ_LOCK_TIMEOUT_SECS: u64 = 30;

// === SECTION: DistributedRWLock ===

/// 跨进程读写锁的句柄。本身没有状态，仅持有键前缀；锁状态由 etcd 维护。
#[derive(Clone)]
pub struct DistributedRWLock {
    lock_prefix: String,
}

impl DistributedRWLock {
    /// 构造新的句柄；`lock_prefix` 决定 etcd 中的命名空间。
    ///
    /// 实际写入的键见模块顶部"外部契约"。
    pub fn new(lock_prefix: String) -> Self {
        Self { lock_prefix }
    }

    /// 内部：返回 writer 键的完整路径。
    #[inline]
    fn writer_key(&self) -> String {
        format!("v1/{}/writer", self.lock_prefix)
    }

    /// 内部：返回 reader 前缀（含末尾斜杠）。
    #[inline]
    fn readers_prefix(&self) -> String {
        format!("v1/{}/readers/", self.lock_prefix)
    }

    /// 内部：返回单个 reader 的完整键。
    #[inline]
    fn reader_key(&self, reader_id: &str) -> String {
        format!("v1/{}/readers/{reader_id}", self.lock_prefix)
    }

    /// 非阻塞地尝试获取写锁。
    ///
    /// 步骤：
    /// 1. 原子事务：当且仅当 writer 键不存在时写入（绑定到 client 的 lease）；
    /// 2. 事务成功后立即扫描 readers 前缀；若存在则回滚写入并返回 `None`；
    /// 3. 扫描失败时同样回滚以确保安全；
    /// 4. 事务失败说明已有 writer，直接返回 `None`。
    pub async fn try_write_lock<'a>(
        &'a self,
        etcd_client: &'a Client,
    ) -> Option<WriteLockGuard<'a>> {
        let write_key = self.writer_key();
        let lease_id = etcd_client.lease_id();
        let put_options = PutOptions::new().with_lease(lease_id as i64);

        // 步骤 1: 原子地"writer 不存在则写入"。
        let txn = Txn::new()
            .when(vec![Compare::version(
                write_key.as_str(),
                CompareOp::Equal,
                0,
            )])
            .and_then(vec![TxnOp::put(
                write_key.as_str(),
                b"writing",
                Some(put_options),
            )]);

        let txn_resp = match etcd_client.etcd_client().kv_client().txn(txn).await {
            Ok(resp) => resp,
            Err(e) => {
                tracing::warn!("Failed to execute write lock transaction: {e:?}");
                return None;
            }
        };

        if !txn_resp.succeeded() {
            tracing::debug!("Write lock already exists, transaction failed");
            return None;
        }

        // 步骤 2: 已写入 writer——检查 readers，必要时回滚。
        let reader_prefix = self.readers_prefix();
        match etcd_client.kv_get_prefix(&reader_prefix).await {
            Ok(readers) if !readers.is_empty() => {
                tracing::debug!(
                    "Found {} reader(s) after acquiring write lock, rolling back",
                    readers.len()
                );
                self.rollback_writer(etcd_client, &write_key).await;
                None
            }
            Ok(_) => {
                tracing::debug!("Successfully acquired write lock with no readers");
                Some(WriteLockGuard {
                    rwlock: self,
                    etcd_client,
                })
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to check for readers, rolling back write lock: {e:?}"
                );
                self.rollback_writer(etcd_client, &write_key).await;
                None
            }
        }
    }

    /// 内部：尽力删除已写入但需要回滚的 writer 键；删除失败仅打 warn。
    async fn rollback_writer(&self, etcd_client: &Client, write_key: &str) {
        if let Err(e) = etcd_client.kv_delete(write_key, None).await {
            tracing::warn!("Failed to rollback write lock: {e:?}");
        }
    }

    /// 以 100ms 周期轮询，直至获取共享读锁或超时。
    ///
    /// `timeout = None` 时使用 `DEFAULT_READ_LOCK_TIMEOUT_SECS`。
    /// 单次轮询采用 `when(writer.version == 0).and_then(put reader)` 的原子事务，
    /// writer 一旦出现立刻让事务失败。
    pub async fn read_lock_with_wait<'a>(
        &'a self,
        etcd_client: &'a Client,
        reader_id: &str,
        timeout: Option<Duration>,
    ) -> Result<ReadLockGuard<'a>> {
        let timeout =
            timeout.unwrap_or(Duration::from_secs(DEFAULT_READ_LOCK_TIMEOUT_SECS));
        let write_key = self.writer_key();
        let reader_key = self.reader_key(reader_id);
        let deadline = tokio::time::Instant::now() + timeout;
        let lease_id = etcd_client.lease_id();

        loop {
            if tokio::time::Instant::now() > deadline {
                anyhow::bail!("Timeout waiting for read lock after {:?}", timeout);
            }

            let put_options = PutOptions::new().with_lease(lease_id as i64);
            let txn = Txn::new()
                .when(vec![Compare::version(
                    write_key.as_str(),
                    CompareOp::Equal,
                    0,
                )])
                .and_then(vec![TxnOp::put(
                    reader_key.as_str(),
                    b"reading",
                    Some(put_options),
                )]);

            match etcd_client.etcd_client().kv_client().txn(txn).await {
                Ok(resp) if resp.succeeded() => {
                    tracing::debug!("Acquired read lock for reader {reader_id}");
                    return Ok(ReadLockGuard {
                        rwlock: self,
                        etcd_client,
                        reader_id: reader_id.to_string(),
                    });
                }
                Ok(_) => {
                    tracing::trace!(
                        "Write lock exists or was created, retrying after delay"
                    );
                }
                Err(e) => {
                    tracing::warn!("Failed to execute read lock transaction: {e:?}");
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

// === SECTION: guards ===

/// 写锁守卫。析构时在当前 tokio runtime 中 spawn 删除 writer 键的任务。
pub struct WriteLockGuard<'a> {
    rwlock: &'a DistributedRWLock,
    etcd_client: &'a Client,
}

impl Drop for WriteLockGuard<'_> {
    fn drop(&mut self) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::error!(
                "WriteLockGuard dropped outside tokio runtime - lock not released! \
                 Lock will be cleaned up when etcd lease expires."
            );
            return;
        };
        let rwlock = self.rwlock.clone();
        let etcd_client = self.etcd_client.clone();
        handle.spawn(async move {
            let write_key = rwlock.writer_key();
            if let Err(e) = etcd_client.kv_delete(write_key.as_str(), None).await {
                tracing::warn!("Failed to release write lock in drop: {e:?}");
            }
        });
    }
}

/// 读锁守卫。析构时在当前 tokio runtime 中 spawn 删除该 reader 键的任务。
pub struct ReadLockGuard<'a> {
    rwlock: &'a DistributedRWLock,
    etcd_client: &'a Client,
    reader_id: String,
}

impl Drop for ReadLockGuard<'_> {
    fn drop(&mut self) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::error!(
                "ReadLockGuard dropped outside tokio runtime - lock not released! \
                 Lock will be cleaned up when etcd lease expires."
            );
            return;
        };
        let rwlock = self.rwlock.clone();
        let etcd_client = self.etcd_client.clone();
        let reader_id = self.reader_id.clone();
        handle.spawn(async move {
            let reader_key = rwlock.reader_key(&reader_id);
            if let Err(e) = etcd_client.kv_delete(reader_key.as_str(), None).await {
                tracing::warn!("Failed to release read lock in drop: {e:?}");
            }
        });
    }
}

// === SECTION: tests ===

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Runtime;
    use std::sync::Arc;
    use tokio::sync::Barrier;

    /// Test the DistributedRWLock behavior
    ///
    /// This test verifies:
    /// 1. Multiple readers can acquire read locks simultaneously
    /// 2. Write lock fails when readers are active
    /// 3. Write lock succeeds when no locks are held
    /// 4. Read lock waits for write lock to be released
    #[cfg(feature = "testing-etcd")]
    #[tokio::test]
    async fn test_distributed_rwlock() {
        // Setup: Create etcd client
        let runtime = Runtime::from_settings().unwrap();
        let etcd_client = Client::builder()
            .etcd_url(vec!["http://localhost:2379".to_string()])
            .build()
            .unwrap();
        let etcd_client = Client::new(etcd_client, runtime).await.unwrap();

        // Prevent runtime from being dropped in async context at end of test
        let etcd_client = std::mem::ManuallyDrop::new(etcd_client);

        // Create RWLock with unique prefix for this test
        let test_id = uuid::Uuid::new_v4();
        let lock_prefix = format!("/test/rwlock/{}", test_id);
        let rwlock = DistributedRWLock::new(lock_prefix.clone());

        // Step 1: Acquire first read lock
        let _reader1_guard = rwlock
            .read_lock_with_wait(&etcd_client, "reader1", Some(Duration::from_secs(5)))
            .await
            .expect("First read lock should succeed");
        println!("✓ Acquired first read lock");

        // Step 2: Acquire second read lock (should succeed - multiple readers allowed)
        let _reader2_guard = rwlock
            .read_lock_with_wait(&etcd_client, "reader2", Some(Duration::from_secs(5)))
            .await
            .expect("Second read lock should succeed");
        println!("✓ Acquired second read lock");

        // Step 3: Try to acquire write lock (should fail - readers are active)
        let write_result = rwlock.try_write_lock(&etcd_client).await;
        assert!(
            write_result.is_none(),
            "Write lock should fail when readers are active"
        );
        println!("✓ Write lock correctly failed with active readers");

        // Step 4: Drop first read lock
        drop(_reader1_guard);
        tokio::time::sleep(Duration::from_millis(50)).await; // Give time for async drop
        println!("✓ Released first read lock");

        // Verify write lock still fails with one reader active
        let write_result_with_one_reader = rwlock.try_write_lock(&etcd_client).await;
        assert!(
            write_result_with_one_reader.is_none(),
            "Write lock should still fail when one reader is active"
        );
        println!("✓ Write lock correctly failed with one reader still active");

        drop(_reader2_guard);
        tokio::time::sleep(Duration::from_millis(50)).await; // Give time for async drop
        println!("✓ Released second read lock");

        // Give etcd a moment to process the deletions
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Step 5: Acquire write lock (should succeed now - no locks held)
        let _write_guard = rwlock
            .try_write_lock(&etcd_client)
            .await
            .expect("Write lock should succeed with no readers");
        println!("✓ Acquired write lock");

        // Step 5a: Try to acquire write lock again (should fail immediately - already held)
        let write_result_already_held = rwlock.try_write_lock(&etcd_client).await;
        assert!(
            write_result_already_held.is_none(),
            "Write lock should fail when another write lock is already held"
        );
        println!("✓ Write lock correctly failed when already held");

        // Step 6: Spawn background task to acquire read lock
        // It should wait because write lock is held
        let barrier = Arc::new(Barrier::new(2));
        let barrier_clone = barrier.clone();
        let rwlock_clone = rwlock.clone();
        let etcd_client_clone = etcd_client.clone();

        let read_task = tokio::spawn(async move {
            println!("→ Background: Attempting to acquire read lock (should wait)...");
            barrier_clone.wait().await; // Signal that we've started

            let start = std::time::Instant::now();
            let _guard = rwlock_clone
                .read_lock_with_wait(
                    &etcd_client_clone,
                    "reader3",
                    Some(Duration::from_secs(10)),
                )
                .await
                .expect("Read lock should eventually succeed");

            let elapsed = start.elapsed();
            println!("✓ Background: Acquired read lock after {:?}", elapsed);

            // Verify it actually waited (should be > 100ms since we sleep before releasing write lock)
            assert!(
                elapsed > Duration::from_millis(50),
                "Read lock should have waited for write lock to be released"
            );

            // Guard will be dropped here, releasing the lock
        });

        // Wait for background task to start
        barrier.wait().await;

        // Give the background task a moment to start polling
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Step 7: Release write lock by dropping guard
        println!("→ Releasing write lock...");
        drop(_write_guard);
        tokio::time::sleep(Duration::from_millis(50)).await; // Give time for async drop
        println!("✓ Released write lock");

        // Step 8: Background task should now succeed
        read_task
            .await
            .expect("Background task should complete successfully");

        // Final cleanup: verify all locks are released
        tokio::time::sleep(Duration::from_millis(100)).await;
        let remaining_locks = etcd_client
            .kv_get_prefix(&format!("v1/{lock_prefix}"))
            .await
            .expect("Should be able to check remaining locks");
        assert!(
            remaining_locks.is_empty(),
            "All locks should be released at end of test"
        );
        println!("✓ All locks cleaned up successfully");

        println!("\n🎉 All DistributedRWLock tests passed!");
    }

    // === SECTION: 合并自原 mod supplemental_tests ===
    // ## 测试过程
    // 覆盖四种边界：
    // 1. writer 持有时 reader 必须超时；
    // 2. reader 存在时 `try_write_lock` 必须回滚并清除 writer 键；
    // 3. ReadLockGuard 析构后对应 reader 键被异步清理；
    // 4. WriteLockGuard 析构后 writer 键被异步清理。
    //
    // ## 意义
    // 直接断言 etcd 上残留键的状态，是验证 Drop 异步清理路径不退化的最强手段。

    async fn maybe_etcd_client() -> Option<std::mem::ManuallyDrop<Client>> {
        let runtime = Runtime::from_settings().ok()?;
        let client_options = Client::builder()
            .etcd_url(vec!["http://localhost:2379".to_string()])
            .build()
            .ok()?;

        let etcd_client = Client::new(client_options, runtime).await.ok()?;

        Some(std::mem::ManuallyDrop::new(etcd_client))
    }

    #[tokio::test]
    async fn test_supplemental_read_lock_times_out_when_writer_held() {
        let Some(etcd_client) = maybe_etcd_client().await else {
            eprintln!("Skipping testing-etcd supplemental lock test: etcd is unavailable");
            return;
        };
        let lock_prefix = format!("/test/rwlock/timeout-{}", uuid::Uuid::new_v4());
        let rwlock = DistributedRWLock::new(lock_prefix.clone());

        let write_guard = rwlock
            .try_write_lock(&etcd_client)
            .await
            .expect("write lock should be acquirable for timeout test");

        let err = rwlock
            .read_lock_with_wait(
                &etcd_client,
                "timeout-reader",
                Some(Duration::from_millis(150)),
            )
            .await
            .err()
            .expect("read lock should time out while writer is held")
            .to_string();
        assert!(err.contains("Timeout waiting for read lock"));

        drop(write_guard);
        tokio::time::sleep(Duration::from_millis(80)).await;

        let remaining = etcd_client
            .kv_get_prefix(format!("v1/{lock_prefix}"))
            .await
            .expect("should list lock keys");
        assert!(remaining.is_empty());
    }

    #[tokio::test]
    async fn test_supplemental_try_write_lock_rolls_back_when_reader_exists() {
        let Some(etcd_client) = maybe_etcd_client().await else {
            eprintln!("Skipping testing-etcd supplemental lock test: etcd is unavailable");
            return;
        };
        let lock_prefix = format!("/test/rwlock/rollback-{}", uuid::Uuid::new_v4());
        let rwlock = DistributedRWLock::new(lock_prefix.clone());

        let reader_guard = rwlock
            .read_lock_with_wait(&etcd_client, "reader-a", Some(Duration::from_secs(3)))
            .await
            .expect("reader lock should be acquirable");

        let write_attempt = rwlock.try_write_lock(&etcd_client).await;
        assert!(
            write_attempt.is_none(),
            "write lock should fail and roll back when readers exist"
        );

        tokio::time::sleep(Duration::from_millis(80)).await;

        let writer_key = format!("v1/{}/writer", lock_prefix);
        let writer_entries = etcd_client
            .kv_get(writer_key.as_bytes().to_vec(), None)
            .await
            .expect("writer key lookup should succeed");
        assert!(
            writer_entries.is_empty(),
            "writer key must not remain after rollback"
        );

        let readers = etcd_client
            .kv_get_prefix(format!("v1/{}/readers/", lock_prefix))
            .await
            .expect("reader prefix lookup should succeed");
        assert_eq!(readers.len(), 1);

        drop(reader_guard);
    }

    #[tokio::test]
    async fn test_supplemental_read_guard_drop_cleans_up_reader_key() {
        let Some(etcd_client) = maybe_etcd_client().await else {
            eprintln!("Skipping testing-etcd supplemental lock test: etcd is unavailable");
            return;
        };
        let lock_prefix = format!("/test/rwlock/read-drop-{}", uuid::Uuid::new_v4());
        let rwlock = DistributedRWLock::new(lock_prefix.clone());

        let reader_id = "cleanup-reader";
        let guard = rwlock
            .read_lock_with_wait(&etcd_client, reader_id, Some(Duration::from_secs(3)))
            .await
            .expect("read lock should be acquired");

        let reader_key = format!("v1/{}/readers/{}", lock_prefix, reader_id);
        let before_drop = etcd_client
            .kv_get(reader_key.as_bytes().to_vec(), None)
            .await
            .expect("reader key should be queryable");
        assert_eq!(before_drop.len(), 1);

        drop(guard);
        tokio::time::sleep(Duration::from_millis(80)).await;

        let after_drop = etcd_client
            .kv_get(reader_key.as_bytes().to_vec(), None)
            .await
            .expect("reader key should be queryable after drop");
        assert!(after_drop.is_empty());
    }

    #[tokio::test]
    async fn test_supplemental_write_guard_drop_cleans_up_writer_key() {
        let Some(etcd_client) = maybe_etcd_client().await else {
            eprintln!("Skipping testing-etcd supplemental lock test: etcd is unavailable");
            return;
        };
        let lock_prefix = format!("/test/rwlock/write-drop-{}", uuid::Uuid::new_v4());
        let rwlock = DistributedRWLock::new(lock_prefix.clone());

        let guard = rwlock
            .try_write_lock(&etcd_client)
            .await
            .expect("write lock should be acquired");

        let writer_key = format!("v1/{}/writer", lock_prefix);
        let before_drop = etcd_client
            .kv_get(writer_key.as_bytes().to_vec(), None)
            .await
            .expect("writer key should be queryable");
        assert_eq!(before_drop.len(), 1);

        drop(guard);
        tokio::time::sleep(Duration::from_millis(80)).await;

        let after_drop = etcd_client
            .kv_get(writer_key.as_bytes().to_vec(), None)
            .await
            .expect("writer key should be queryable after drop");
        assert!(after_drop.is_empty());
    }
}
