// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

mod common;

use anyhow::Result;
use pagoda_runtime::{
    storage::kv::{self, StoreError},
};
use serde::{Deserialize, Serialize};

use common::contract::acquire_contract_test_lock;

const TEST_BUCKET: &str = "integration-test-bucket";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct TestRecord {
    value: String,
    #[serde(default)]
    revision: u64,
}

impl kv::Versioned for TestRecord {
    fn revision(&self) -> u64 {
        self.revision
    }

    fn set_revision(&mut self, revision: u64) {
        self.revision = revision;
    }
}

// 目的/场景：typed load 遇到非法 JSON 时不影响 bucket 中其它有效条目。
//
// 生产逻辑：`Manager::load` 反序列化失败返回 `JSONDecodeError`；合法 key 仍可 load。
//
// 测试计划：bucket 写入 valid JSON 与 invalid 字节 → 分别 `load` 两条 key。
//
// 关键断言：invalid key 返回 `JSONDecodeError`；valid key 反序列化为预期 `TestRecord`。
#[tokio::test]
async fn typed_prefix_watcher_ignores_invalid_values() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    let manager = kv::Manager::memory();
    let bucket = manager.get_or_create_bucket(TEST_BUCKET, None).await?;

    bucket
        .insert(
            &kv::Key::from("valid"),
            bytes::Bytes::from_static(br#"{"value":"ok","revision":0}"#),
            0,
        )
        .await?;
    bucket
        .insert(
            &kv::Key::from("invalid"),
            bytes::Bytes::from_static(b"not-json"),
            0,
        )
        .await?;

    let invalid = manager
        .load::<TestRecord>(TEST_BUCKET, &kv::Key::from("invalid"))
        .await;
    assert!(matches!(invalid, Err(StoreError::JSONDecodeError(_))));

    let valid = manager
        .load::<TestRecord>(TEST_BUCKET, &kv::Key::from("valid"))
        .await?
        .expect("valid record should load");
    assert_eq!(valid, TestRecord {
        value: "ok".to_string(),
        revision: 0,
    });

    Ok(())
}

mod memory {
    use anyhow::{Result, anyhow};
    use pagoda_runtime::storage::kv::{self, Key, StoreOutcome, WatchEvent};
    use futures::StreamExt;

    use super::common::contract::acquire_contract_test_lock;
    use super::TEST_BUCKET;

    // 目的/场景：memory KV 公共 API 支持 CRUD 与 watch（新 key Put + Delete）。
    //
    // 生产逻辑：`Bucket::insert/get/delete/watch`；memory 后端对已有 key 的 revision 更新不发 watch 事件。
    //
    // 测试计划：Put key-a → watch Put → 同 key revision 更新（用 get 验证）→ Put key-b → Delete key-a。
    //
    // 关键断言：watch 收到 key-a Put、key-b Put、key-a Delete；delete 后 get 为 None。
    #[tokio::test]
    async fn memory_kv_crud_and_watch() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let manager = kv::Manager::memory();
        let bucket = manager.get_or_create_bucket(TEST_BUCKET, None).await?;

        let key_a = Key::from("record-a");
        let key_b = Key::from("record-b");
        let mut watch = bucket.watch().await?;

        let outcome = bucket
            .insert(&key_a, bytes::Bytes::from_static(b"value-a"), 0)
            .await?;
        assert_eq!(outcome, StoreOutcome::Created(0));

        let first = watch
            .next()
            .await
            .ok_or_else(|| anyhow!("watch stream ended before first put"))?;
        assert_eq!(
            first,
            WatchEvent::Put(kv::KeyValue::new(
                key_a.clone(),
                bytes::Bytes::from_static(b"value-a")
            ))
        );

        bucket
            .insert(&key_a, bytes::Bytes::from_static(b"value-a-v2"), 1)
            .await?;
        let got = bucket.get(&key_a).await?.expect("updated value should exist");
        assert_eq!(got.as_ref(), b"value-a-v2");

        bucket
            .insert(&key_b, bytes::Bytes::from_static(b"value-b"), 0)
            .await?;
        let second = watch
            .next()
            .await
            .ok_or_else(|| anyhow!("watch stream ended before second put"))?;
        assert_eq!(
            second,
            WatchEvent::Put(kv::KeyValue::new(
                key_b.clone(),
                bytes::Bytes::from_static(b"value-b")
            ))
        );

        bucket.delete(&key_a).await?;
        let deleted = watch
            .next()
            .await
            .ok_or_else(|| anyhow!("watch stream ended before delete"))?;
        assert_eq!(deleted, WatchEvent::Delete(key_a.clone()));
        assert!(bucket.get(&key_a).await?.is_none());

        Ok(())
    }
}

mod file {
    use anyhow::Result;
    use pagoda_runtime::{CancellationToken, storage::kv::{self, Key}};
    use tempfile::TempDir;

    use super::common::contract::acquire_contract_test_lock;
    use super::TEST_BUCKET;

    // 目的/场景：file KV 关闭后重开，数据仍可读取。
    //
    // 生产逻辑：`Manager::file(cancel_token, root)` 将 bucket 持久化到目录。
    //
    // 测试计划：首次 Manager 写入 → drop scope 结束 → 同 path 新建 Manager 读取。
    //
    // 关键断言：重开后 `get("persist-me")` 仍为 `survives-reopen`。
    #[tokio::test]
    async fn file_kv_persists_across_reopen() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let temp = TempDir::new()?;
        let root = temp.path().to_path_buf();
        let cancel = CancellationToken::new();

        {
            let manager = kv::Manager::file(cancel.clone(), root.clone());
            let bucket = manager.get_or_create_bucket(TEST_BUCKET, None).await?;
            bucket
                .insert(
                    &Key::from("persist-me"),
                    bytes::Bytes::from_static(b"survives-reopen"),
                    0,
                )
                .await?;
        }

        let manager = kv::Manager::file(cancel, root);
        let bucket = manager.get_or_create_bucket(TEST_BUCKET, None).await?;
        let got = bucket
            .get(&Key::from("persist-me"))
            .await?
            .expect("value should persist across Manager reopen");
        assert_eq!(got.as_ref(), b"survives-reopen");

        Ok(())
    }
}

#[cfg(feature = "testing-etcd")]
mod etcd {
    use anyhow::{Result, anyhow};
    use pagoda_runtime::storage::kv::{self, Key, WatchEvent};
    use futures::StreamExt;

    use super::common::contract::{acquire_contract_test_lock, etcd_kv_manager, unique_name};

    // 目的/场景：etcd KV 公共 API 支持 CRUD、watch 与按前缀批量删除。
    //
    // 生产逻辑：`Manager::etcd` → `EtcdBucket::{insert,get,delete,watch,entries}`（`storage/kv/etcd.rs`）。
    //
    // 测试计划：Put 两 key → watch 收 Put → 按 prefix 删 `group/` 下全部 key → watch 收 Delete。
    //
    // 关键断言：CRUD 值正确；Delete 后 `get` 为 None；watch 顺序 Put, Put, Delete, Delete。
    #[tokio::test]
    #[ignore = "requires etcd (Nightly); set ETCD_ENDPOINTS and run with --features testing-etcd --include-ignored"]
    async fn etcd_kv_crud_watch_and_prefix_delete() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let manager = etcd_kv_manager().await?;
        let bucket_name = format!("it-etcd-{}", unique_name("kv"));
        let bucket = manager.get_or_create_bucket(&bucket_name, None).await?;

        let key_a = Key::from("group/record-a");
        let key_b = Key::from("group/record-b");
        let mut watch = bucket.watch().await?;

        bucket
            .insert(&key_a, bytes::Bytes::from_static(b"value-a"), 0)
            .await?;
        let first = watch
            .next()
            .await
            .ok_or_else(|| anyhow!("watch ended before first put"))?;
        assert!(matches!(first, WatchEvent::Put(_)));

        bucket
            .insert(&key_b, bytes::Bytes::from_static(b"value-b"), 0)
            .await?;
        let second = watch
            .next()
            .await
            .ok_or_else(|| anyhow!("watch ended before second put"))?;
        assert!(matches!(second, WatchEvent::Put(_)));

        assert_eq!(
            bucket.get(&key_a).await?.as_deref(),
            Some(b"value-a".as_ref())
        );

        // `entries()` keys include the bucket prefix; `delete()` expects logical keys only.
        let bucket_prefix = format!("{bucket_name}/");
        let group_prefix = "group/";
        let to_delete: Vec<Key> = bucket
            .entries()
            .await?
            .into_keys()
            .filter(|k| {
                k.as_ref()
                    .strip_prefix(&bucket_prefix)
                    .is_some_and(|relative| relative.starts_with(group_prefix))
            })
            .collect();
        assert_eq!(to_delete.len(), 2);
        for key in &to_delete {
            let logical = key
                .as_ref()
                .strip_prefix(&bucket_prefix)
                .expect("entry key should include bucket prefix");
            bucket.delete(&Key::from(logical)).await?;
        }

        for _ in 0..2 {
            let event = watch
                .next()
                .await
                .ok_or_else(|| anyhow!("watch ended before prefix delete"))?;
            assert!(matches!(event, WatchEvent::Delete(_)));
        }
        assert!(bucket.get(&key_a).await?.is_none());
        assert!(bucket.get(&key_b).await?.is_none());

        Ok(())
    }
}

mod nats {
    use anyhow::{Result, anyhow};
    use pagoda_runtime::storage::kv::{Key, StoreOutcome, WatchEvent};
    use futures::StreamExt;

    use super::common::contract::{acquire_contract_test_lock, nats_kv_test_bucket};

    // 目的/场景：NATS KV 公共 API 支持 CRUD 与 watch（与 memory 后端契约对称）。
    //
    // 生产逻辑：`NATSStore` / `NATSBucket::{insert,get,delete,watch}`（`storage/kv/nats.rs`）。
    //
    // 测试计划：Put key-a → watch Put → Put key-b → Delete key-a。
    //
    // 关键断言：watch 收到 key-a Put、key-b Put、key-a Delete；delete 后 get 为 None。
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires NATS broker (Nightly); set NATS_SERVER and run with --include-ignored"]
    async fn nats_kv_crud_and_watch() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let bucket = nats_kv_test_bucket().await?;

        let key_a = Key::from("record-a");
        let key_b = Key::from("record-b");
        let mut watch = bucket.watch().await?;

        let outcome = bucket
            .insert(&key_a, bytes::Bytes::from_static(b"value-a"), 0)
            .await?;
        assert_eq!(outcome, StoreOutcome::Created(1));

        let first = watch
            .next()
            .await
            .ok_or_else(|| anyhow!("watch stream ended before first put"))?;
        assert!(matches!(first, WatchEvent::Put(_)));

        bucket
            .insert(&key_b, bytes::Bytes::from_static(b"value-b"), 0)
            .await?;
        let second = watch
            .next()
            .await
            .ok_or_else(|| anyhow!("watch stream ended before second put"))?;
        assert!(matches!(second, WatchEvent::Put(_)));

        assert_eq!(
            bucket.get(&key_a).await?.as_deref(),
            Some(b"value-a".as_ref())
        );

        bucket.delete(&key_a).await?;
        let deleted = watch
            .next()
            .await
            .ok_or_else(|| anyhow!("watch stream ended before delete"))?;
        assert!(matches!(deleted, WatchEvent::Delete(_)));
        assert!(bucket.get(&key_a).await?.is_none());

        Ok(())
    }
}
