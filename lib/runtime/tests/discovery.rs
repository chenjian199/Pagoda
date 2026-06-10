// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

mod common;

use std::fs;

use anyhow::{Context, Result};
use pagoda_runtime::metadata_registry::BASE_SUFFIX;
use tempfile::TempDir;

use common::contract::{
    acquire_contract_test_lock, discovery_query_endpoint, endpoint_discovery_spec,
    file_backed_runtime, unique_name,
};

// 目的/场景：`MetadataArtifactRegistry` register/get/unregister roundtrip。
//
// 生产逻辑：`DistributedRuntime::metadata_artifacts()` 维护 worker 侧 metadata 路径索引。
//
// 测试计划：写临时 artifact → `register` → `get` → `unregister` → 再次 `get`。
//
// 关键断言：register 后 `get` 返回路径且 `len()==1`；unregister 后 `is_empty()` 且 `get` 为 None。
#[tokio::test]
async fn metadata_registry_roundtrips_endpoint_metadata() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    let temp = TempDir::new()?;
    let (_rt, drt) = file_backed_runtime(temp.path().to_path_buf()).await?;

    let artifact_path = temp.path().join("model.bin");
    fs::write(&artifact_path, b"metadata-bytes")?;

    let registry = drt.metadata_artifacts();
    let slug = "my-model";
    let filename = "weights.bin";
    registry.register(slug, BASE_SUFFIX, filename, artifact_path.clone());

    assert_eq!(
        registry.get(slug, BASE_SUFFIX, filename),
        Some(artifact_path.clone())
    );
    assert_eq!(registry.len(), 1);

    registry.unregister(slug, BASE_SUFFIX);
    assert!(registry.is_empty());
    assert!(registry.get(slug, BASE_SUFFIX, filename).is_none());

    Ok(())
}

// 目的/场景：discovery list/watch 遇到无法反序列化的 KV 值时不污染有效实例集合。
//
// 生产逻辑：`KVStoreDiscovery::list` / watch parse 失败时 warn 并 skip（同 typed watcher 语义）。
//
// 测试计划：合法 register → 同 prefix 下写入 corrupt JSON 文件 → `list` 查询。
//
// 关键断言：list 仍仅 1 条且 instance_id 与合法注册一致。
#[tokio::test]
async fn discovery_list_ignores_invalid_values() -> Result<()> {
    let _guard = acquire_contract_test_lock();
    let temp = TempDir::new()?;
    let kv_root = temp.path().to_path_buf();
    let (_rt, drt) = file_backed_runtime(kv_root.clone()).await?;

    let namespace = unique_name("disc-invalid");
    let component = "backend";
    let endpoint = "generate";

    let instance = drt
        .discovery()
        .register(endpoint_discovery_spec(&namespace, component, endpoint))
        .await?;

    let corrupt_key_dir = kv_root
        .join("v1/instances")
        .join(&namespace)
        .join(component)
        .join(endpoint);
    fs::create_dir_all(&corrupt_key_dir).context("create corrupt key directory")?;
    let corrupt_file = corrupt_key_dir.join("deadbeef");
    fs::write(&corrupt_file, b"not-valid-discovery-json")?;

    let listed = drt
        .discovery()
        .list(discovery_query_endpoint(&namespace, component, endpoint))
        .await?;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].instance_id(), instance.instance_id());

    Ok(())
}

mod file {
    use std::time::Duration;

    use anyhow::{Result, anyhow};
    use pagoda_runtime::discovery::{DiscoveryEvent, DiscoveryInstance, DiscoveryInstanceId};
    use tempfile::TempDir;

    use super::common::contract::{
        acquire_contract_test_lock, additional_file_backed_runtime, discovery_query_endpoint,
        endpoint_discovery_spec, file_backed_runtime, unique_name, wait_for_discovery_event,
    };

    // 目的/场景：file KV discovery watch 可观察 register/unregister 事件。
    //
    // 生产逻辑：`KVStoreDiscovery::list_and_watch` 将 bucket watch 转为 Added/Removed。
    //
    // 测试计划：`list_and_watch` → `register` → 等待 Added → `unregister` → 等待 Removed。
    //
    // 关键断言：Added 事件 instance_id 与 register 返回值一致；Removed 匹配同一 endpoint。
    #[tokio::test]
    async fn file_discovery_watch_sees_register_and_unregister() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let temp = TempDir::new()?;
        let (_rt, drt) = file_backed_runtime(temp.path().to_path_buf()).await?;

        let namespace = unique_name("disc-watch");
        let component = "backend";
        let endpoint = "generate";
        let query = discovery_query_endpoint(&namespace, component, endpoint);

        let mut stream = drt.discovery().list_and_watch(query.clone(), None).await?;

        let spec = endpoint_discovery_spec(&namespace, component, endpoint);
        let instance = drt.discovery().register(spec).await?;

        let added = wait_for_discovery_event(&mut stream, |event| {
            matches!(
                event,
                DiscoveryEvent::Added(DiscoveryInstance::PortName(inst))
                    if inst.namespace == namespace
                        && inst.servicegroup == component
                        && inst.portname == endpoint
            )
        })
        .await?;
        let DiscoveryEvent::Added(DiscoveryInstance::PortName(added_inst)) = added else {
            return Err(anyhow!("expected Added endpoint event"));
        };
        assert_eq!(added_inst.instance_id, instance.instance_id());

        drt.discovery().unregister(instance).await?;

        let removed = wait_for_discovery_event(&mut stream, |event| {
            matches!(
                event,
                DiscoveryEvent::Removed(DiscoveryInstanceId::PortName(id))
                    if id.namespace == namespace
                        && id.servicegroup == component
                        && id.portname == endpoint
                        && id.instance_id == added_inst.instance_id
            )
        })
        .await?;
        assert!(matches!(removed, DiscoveryEvent::Removed(_)));

        Ok(())
    }

    // 目的/场景：两个 DRT 共享 file KV 时可互相看到 discovery 注册。
    //
    // 生产逻辑：`DiscoveryBackend::KvStore(File)` 跨 runtime 共享同一 store root。
    //
    // 测试计划：DRT-A `register` → DRT-B `list` 轮询 → DRT-B endpoint client `wait_for_instances`。
    //
    // 关键断言：DRT-B discovery list 与 client 均见 1 个实例。
    #[tokio::test]
    async fn file_discovery_cross_runtime_shares_instances() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let temp = TempDir::new()?;
        let kv_path = temp.path().to_path_buf();

        let (rt, drt_a) = file_backed_runtime(kv_path.clone()).await?;
        let drt_b = additional_file_backed_runtime(rt, &kv_path).await?;

        let namespace = unique_name("disc-shared");
        let component = "backend";
        let endpoint = "generate";

        let spec = endpoint_discovery_spec(&namespace, component, endpoint);
        drt_a.discovery().register(spec).await?;

        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let instances = drt_b
                    .discovery()
                    .list(discovery_query_endpoint(&namespace, component, endpoint))
                    .await?;
                if instances.len() == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Ok::<(), anyhow::Error>(())
        })
        .await??;

        let client = drt_b
            .namespace(&namespace)?
            .servicegroup(component)?
            .portname(endpoint)
            .client()
            .await?;
        client.wait_for_instances().await?;
        assert_eq!(client.instances().len(), 1);

        Ok(())
    }

    // 目的/场景：file KV discovery query 必须精确匹配 namespace/component/endpoint。
    //
    // 生产逻辑：`KVStoreDiscovery::query_prefix` 按层级前缀过滤。
    //
    // 测试计划：注册 comp-a/ep-a 与 comp-b/ep-a → 精确 query、错 endpoint、错 namespace 各 list 一次。
    //
    // 关键断言：精确 query 1 条；错 endpoint / 错 namespace 均为空。
    #[tokio::test]
    async fn file_discovery_filters_exact_namespace_component_endpoint() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let temp = TempDir::new()?;
        let (_rt, drt) = file_backed_runtime(temp.path().to_path_buf()).await?;

        let namespace = unique_name("disc-filter");
        drt.discovery()
            .register(endpoint_discovery_spec(&namespace, "comp-a", "ep-a"))
            .await?;
        drt.discovery()
            .register(endpoint_discovery_spec(&namespace, "comp-b", "ep-a"))
            .await?;

        let exact = drt
            .discovery()
            .list(discovery_query_endpoint(&namespace, "comp-a", "ep-a"))
            .await?;
        assert_eq!(exact.len(), 1);

        let wrong_component = drt
            .discovery()
            .list(discovery_query_endpoint(&namespace, "comp-a", "ep-b"))
            .await?;
        assert!(wrong_component.is_empty());

        let wrong_namespace = drt
            .discovery()
            .list(discovery_query_endpoint(
                &format!("{namespace}-other"),
                "comp-a",
                "ep-a",
            ))
            .await?;
        assert!(wrong_namespace.is_empty());

        Ok(())
    }

    // 目的/场景：discovery watch 断开后重连，list 视图不重复实例。
    //
    // 生产逻辑：`kv::Manager::watch` 重连时 replay 已有 key 为 Put；`EndpointDiscoverySource`
    // 以 instance_id 为 key 去重（`component/client.rs`）。
    //
    // 测试计划：register → watch Added → drop stream → 新 watch replay Added → `list`。
    //
    // 关键断言：两次 Added 后 `list` 仍仅 1 条且 instance_id 不变。
    #[tokio::test]
    async fn watch_reconnect_recovers_without_duplicate_instances() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let temp = TempDir::new()?;
        let (_rt, drt) = file_backed_runtime(temp.path().to_path_buf()).await?;

        let namespace = unique_name("disc-reconnect");
        let component = "backend";
        let endpoint = "generate";
        let query = discovery_query_endpoint(&namespace, component, endpoint);

        let spec = endpoint_discovery_spec(&namespace, component, endpoint);
        let instance = drt.discovery().register(spec).await?;

        let mut first_watch = drt.discovery().list_and_watch(query.clone(), None).await?;
        let first_added = wait_for_discovery_event(&mut first_watch, |event| {
            matches!(
                event,
                DiscoveryEvent::Added(DiscoveryInstance::PortName(inst))
                    if inst.namespace == namespace
                        && inst.servicegroup == component
                        && inst.portname == endpoint
            )
        })
        .await?;
        let DiscoveryEvent::Added(DiscoveryInstance::PortName(first_inst)) = first_added else {
            return Err(anyhow!("expected Added from first watch"));
        };
        assert_eq!(first_inst.instance_id, instance.instance_id());
        drop(first_watch);

        let mut second_watch = drt.discovery().list_and_watch(query.clone(), None).await?;
        let second_added = wait_for_discovery_event(&mut second_watch, |event| {
            matches!(
                event,
                DiscoveryEvent::Added(DiscoveryInstance::PortName(inst))
                    if inst.instance_id == instance.instance_id()
            )
        })
        .await?;
        assert!(matches!(second_added, DiscoveryEvent::Added(_)));
        drop(second_watch);

        let listed = drt.discovery().list(query).await?;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].instance_id(), instance.instance_id());

        Ok(())
    }
}

#[cfg(feature = "testing-etcd")]
mod etcd {
    use std::time::Duration;

    use anyhow::{Result, anyhow};
    use pagoda_runtime::discovery::{
        DiscoveryEvent, DiscoveryInstance, DiscoveryInstanceId, DiscoveryQuery,
    };
    use futures::StreamExt;

    use super::common::contract::{
        acquire_contract_test_lock, discovery_query_endpoint, endpoint_discovery_spec,
        etcd_delete_discovery_namespace_prefix, etcd_runtime, etcd_runtime_ephemeral,
        shutdown_runtime, unique_name, wait_for_discovery_event, wait_for_instances_empty,
    };

    // 目的/场景：etcd discovery watch 在已有 key 上重建初始快照（Put → Added）。
    //
    // 生产逻辑：`kv::Manager::watch` 先 replay `entries()` 再跟增量（`kv.rs`）；`KVStoreDiscovery::list_and_watch`。
    //
    // 测试计划：先 register → 再 `list_and_watch` → 首事件为 Added。
    //
    // 关键断言：Added 事件 namespace/component/endpoint 与注册一致。
    #[tokio::test]
    #[ignore = "requires etcd (Nightly); set ETCD_ENDPOINTS and run with --features testing-etcd --include-ignored"]
    async fn etcd_discovery_watch_rebuilds_initial_snapshot() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let (_rt, drt) = etcd_runtime().await?;

        let namespace = unique_name("etcd-watch");
        let component = "backend";
        let endpoint = "generate";
        let query = discovery_query_endpoint(&namespace, component, endpoint);

        let spec = endpoint_discovery_spec(&namespace, component, endpoint);
        let instance = drt.discovery().register(spec).await?;

        let mut stream = drt.discovery().list_and_watch(query, None).await?;
        let first = wait_for_discovery_event(&mut stream, |event| {
            matches!(
                event,
                DiscoveryEvent::Added(DiscoveryInstance::PortName(inst))
                    if inst.namespace == namespace
                        && inst.servicegroup == component
                        && inst.portname == endpoint
            )
        })
        .await?;
        let DiscoveryEvent::Added(DiscoveryInstance::PortName(added)) = first else {
            return Err(anyhow!("expected Added from initial watch snapshot"));
        };
        assert_eq!(added.instance_id, instance.instance_id());

        Ok(())
    }

    // 目的/场景：etcd lease 过期后 discovery 中的实例被移除。
    //
    // 生产逻辑：`etcd::Client` `attach_lease` + keep-alive；runtime 取消后 lease TTL 到期删除 key。
    //
    // 测试计划：ephemeral DRT 注册 → shutdown → 等待 TTL → 新 DRT list 为空。
    //
    // 关键断言：注册后 list 有 1 条；lease 过期后 list 为 0。
    #[tokio::test]
    #[ignore = "requires etcd (Nightly); set ETCD_ENDPOINTS and run with --features testing-etcd --include-ignored"]
    async fn etcd_lease_expiry_removes_instance() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let namespace = unique_name("etcd-lease");
        let component = "backend";
        let endpoint = "generate";
        let query = discovery_query_endpoint(&namespace, component, endpoint);

        let (rt, drt) = etcd_runtime_ephemeral().await?;
        drt.discovery()
            .register(endpoint_discovery_spec(&namespace, component, endpoint))
            .await?;
        assert_eq!(drt.discovery().list(query.clone()).await?.len(), 1);

        shutdown_runtime(rt, None).await?;
        // Default etcd client lease TTL is 10s (`create_lease` in `transports/etcd.rs`).
        tokio::time::sleep(Duration::from_secs(12)).await;

        let (_rt2, drt2) = etcd_runtime_ephemeral().await?;
        assert!(
            drt2.discovery().list(query.clone()).await?.is_empty(),
            "instance should be removed after lease expiry"
        );

        let client = drt2
            .namespace(namespace)?
            .servicegroup(component)?
            .portname(endpoint)
            .client()
            .await?;
        wait_for_instances_empty(&client).await?;

        Ok(())
    }

    // 目的/场景：etcd prefix 删除会反映到 discovery watcher，移除所有子实例。
    //
    // 生产逻辑：`KVStoreDiscovery::list_and_watch` 将 bucket Delete 转为 `DiscoveryEvent::Removed`
    //（`discovery/kv_store.rs`）；etcd `kv_delete` with prefix 批量删 key。
    //
    // 测试计划：同 namespace 注册 2 endpoint → `NamespacedEndpoints` watch → prefix delete。
    //
    // 关键断言：watch 收 2 条 Removed；`list` 为空。
    #[tokio::test]
    #[ignore = "requires etcd (Nightly); set ETCD_ENDPOINTS and run with --features testing-etcd --include-ignored"]
    async fn prefix_delete_removes_all_child_instances() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let (_rt, drt) = etcd_runtime().await?;

        let namespace = unique_name("etcd-prefix");
        drt.discovery()
            .register(endpoint_discovery_spec(&namespace, "comp-a", "ep-a"))
            .await?;
        drt.discovery()
            .register(endpoint_discovery_spec(&namespace, "comp-b", "ep-a"))
            .await?;

        let query = DiscoveryQuery::NamespacedPortNames {
            namespace: namespace.clone(),
        };
        assert_eq!(drt.discovery().list(query.clone()).await?.len(), 2);

        let mut stream = drt.discovery().list_and_watch(query.clone(), None).await?;

        etcd_delete_discovery_namespace_prefix(&namespace).await?;

        let mut removed = 0usize;
        tokio::time::timeout(Duration::from_secs(10), async {
            while removed < 2 {
                let event = stream
                    .next()
                    .await
                    .ok_or_else(|| anyhow!("discovery watch ended before prefix delete removals"))??;
                if matches!(event, DiscoveryEvent::Removed(DiscoveryInstanceId::PortName(_))) {
                    removed += 1;
                }
            }
            Ok::<(), anyhow::Error>(())
        })
        .await??;
        assert_eq!(removed, 2);

        assert!(
            drt.discovery().list(query).await?.is_empty(),
            "prefix delete should remove all child discovery instances"
        );

        Ok(())
    }
}

#[cfg(feature = "integration-kube")]
mod kube {
    use anyhow::{Result, anyhow};
    use pagoda_runtime::discovery::{
        DiscoveryEvent, DiscoveryInstance, DiscoveryInstanceId, hash_pod_name,
    };

    use super::common::contract::{
        KubeDualPodFixture, KubeReadinessFixture, acquire_contract_test_lock,
        discovery_query_endpoint, endpoint_discovery_spec, kube_apply_invalid_worker_metadata_cr,
        kube_dual_pod_runtimes, kube_runtime, kube_runtime_for_identity,
        kube_wait_for_daemon_settle, unique_name, wait_for_discovery_event,
        wait_for_discovery_list,
    };

    // 目的/场景：Kubernetes discovery 经 `register`/`list` 完成单 pod 元数据往返。
    //
    // 生产逻辑：`KubeDiscoveryClient::register_internal` 写本地 snapshot 并 `apply_cr`
    // 持久化 `PagodaWorkerMetadata`（`discovery/kube.rs` / `crd.rs`）。
    //
    // 测试计划：`kube_runtime`（含 EndpointSlice fixture）→ `register` → `wait_for_discovery_list`。
    //
    // 关键断言：list 返回 1 条；`instance_id` 与 register 返回值一致。
    #[tokio::test]
    #[ignore = "requires Kubernetes + PagodaWorkerMetadata CRD (Release); run with --features integration-kube --include-ignored"]
    async fn kube_discovery_register_list_roundtrip() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let (_rt, drt, fixture) = kube_runtime().await?;

        let namespace = unique_name("kube-list");
        let component = "backend";
        let endpoint = "generate";
        let query = discovery_query_endpoint(&namespace, component, endpoint);

        let spec = endpoint_discovery_spec(&namespace, component, endpoint);
        let instance = drt.discovery().register(spec).await?;

        let listed = wait_for_discovery_list(&drt, query, 1).await?;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].instance_id(), instance.instance_id());

        drt.discovery().unregister(instance).await?;
        fixture.teardown().await?;
        Ok(())
    }

    // 目的/场景：Kubernetes discovery watch 可观察 register/unregister 的 Added/Removed 事件。
    //
    // 生产逻辑：`KubeDiscoveryClient::list_and_watch` 先 replay 当前 snapshot 再跟
    // `metadata_watch` 增量（`discovery/kube.rs`）。
    //
    // 测试计划：`list_and_watch` → `register` → Added → `unregister` → Removed。
    //
    // 关键断言：Added/Removed 事件的 namespace/component/endpoint 与注册一致。
    #[tokio::test]
    #[ignore = "requires Kubernetes + PagodaWorkerMetadata CRD (Release); run with --features integration-kube --include-ignored"]
    async fn kube_discovery_watch_sees_register_and_unregister() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let (_rt, drt, fixture) = kube_runtime().await?;

        let namespace = unique_name("kube-watch");
        let component = "backend";
        let endpoint = "generate";
        let query = discovery_query_endpoint(&namespace, component, endpoint);

        let mut stream = drt.discovery().list_and_watch(query.clone(), None).await?;

        let spec = endpoint_discovery_spec(&namespace, component, endpoint);
        let instance = drt.discovery().register(spec).await?;

        let added = wait_for_discovery_event(&mut stream, |event| {
            matches!(
                event,
                DiscoveryEvent::Added(DiscoveryInstance::PortName(inst))
                    if inst.namespace == namespace
                        && inst.servicegroup == component
                        && inst.portname == endpoint
            )
        })
        .await?;
        let DiscoveryEvent::Added(DiscoveryInstance::PortName(added_inst)) = added else {
            return Err(anyhow!("expected Added endpoint event"));
        };
        assert_eq!(added_inst.instance_id, instance.instance_id());

        drt.discovery().unregister(instance).await?;

        let removed = wait_for_discovery_event(&mut stream, |event| {
            matches!(
                event,
                DiscoveryEvent::Removed(DiscoveryInstanceId::PortName(id))
                    if id.namespace == namespace
                        && id.servicegroup == component
                        && id.portname == endpoint
                        && id.instance_id == added_inst.instance_id
            )
        })
        .await?;
        assert!(matches!(removed, DiscoveryEvent::Removed(_)));

        fixture.teardown().await?;
        Ok(())
    }

    // 目的/场景：Kubernetes discovery query 必须精确匹配 namespace/component/endpoint。
    //
    // 生产逻辑：`MetadataSnapshot::filter` 按 `DiscoveryQuery::PortName` 三元组过滤
    //（`discovery/metadata.rs`）；daemon 聚合的 snapshot 不泄漏邻域。
    //
    // 测试计划：注册 comp-a/ep-a 与 comp-b/ep-a → 精确 query、错 endpoint、错 namespace 各 list。
    //
    // 关键断言：精确 query 1 条；错 endpoint / 错 namespace 均为空。
    #[tokio::test]
    #[ignore = "requires Kubernetes + PagodaWorkerMetadata CRD (Release); run with --features integration-kube --include-ignored"]
    async fn kube_discovery_filters_exact_namespace_component_endpoint() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let (_rt, drt, fixture) = kube_runtime().await?;

        let namespace = unique_name("kube-filter");
        drt.discovery()
            .register(endpoint_discovery_spec(&namespace, "comp-a", "ep-a"))
            .await?;
        drt.discovery()
            .register(endpoint_discovery_spec(&namespace, "comp-b", "ep-a"))
            .await?;

        let exact = wait_for_discovery_list(
            &drt,
            discovery_query_endpoint(&namespace, "comp-a", "ep-a"),
            1,
        )
        .await?;
        assert_eq!(exact.len(), 1);

        let wrong_component = wait_for_discovery_list(
            &drt,
            discovery_query_endpoint(&namespace, "comp-a", "ep-b"),
            0,
        )
        .await?;
        assert!(wrong_component.is_empty());

        let wrong_namespace = wait_for_discovery_list(
            &drt,
            discovery_query_endpoint(&format!("{namespace}-other"), "comp-a", "ep-a"),
            0,
        )
        .await?;
        assert!(wrong_namespace.is_empty());

        fixture.teardown().await?;
        Ok(())
    }

    // 目的/场景：worker A 在 pod A 注册后，pod B 上的 discovery 能 list 到该实例。
    //
    // 生产逻辑：`DiscoveryDaemon` 聚合 namespace 内全部 ready EndpointSlice + CR；
    // 各 pod 的 `KubeDiscoveryClient` 共享同一集群 snapshot。
    //
    // 测试计划：双 Pod fixture → DRT-A register → DRT-B `wait_for_discovery_list`。
    //
    // 关键断言：B 侧 list 1 条；`instance_id == hash_pod_name(pod_a)`。
    #[tokio::test]
    #[ignore = "requires Kubernetes + PagodaWorkerMetadata CRD (Release); run with --features integration-kube --include-ignored"]
    async fn kube_discovery_cross_pod_shares_instances() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let pod_namespace =
            std::env::var("POD_NAMESPACE").unwrap_or_else(|_| "default".to_string());
        let pod_a_name = unique_name("kube-pod-a");
        let pod_b_name = unique_name("kube-pod-b");
        let dual = KubeDualPodFixture::install(&pod_a_name, &pod_b_name, &pod_namespace).await?;
        let (_rt, drt_a, drt_b) = kube_dual_pod_runtimes(&dual).await?;

        let namespace = unique_name("kube-cross");
        let component = "backend";
        let endpoint = "generate";
        let query = discovery_query_endpoint(&namespace, component, endpoint);

        let spec = endpoint_discovery_spec(&namespace, component, endpoint);
        let instance = drt_a.discovery().register(spec).await?;
        let expected_id = hash_pod_name(&pod_a_name);
        assert_eq!(instance.instance_id(), expected_id);

        let listed = wait_for_discovery_list(&drt_b, query.clone(), 1).await?;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].instance_id(), expected_id);

        drt_a.discovery().unregister(instance).await?;
        dual.teardown().await?;
        Ok(())
    }

    // 目的/场景：daemon 反序列化失败的 CR 不污染 discovery list。
    //
    // 生产逻辑：`DiscoveryDaemon::aggregate_snapshot` 对非法 `spec.data` warn 并 skip
    //（`discovery/kube/daemon.rs`）。
    //
    // 测试计划：invalid pod ready + 坏 CR → 合法 pod register → list 仅含合法实例。
    //
    // 关键断言：list 1 条且 `instance_id` 为合法 pod。
    #[tokio::test]
    #[ignore = "requires Kubernetes + PagodaWorkerMetadata CRD (Release); run with --features integration-kube --include-ignored"]
    async fn kube_discovery_list_ignores_invalid_cr_data() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let pod_namespace =
            std::env::var("POD_NAMESPACE").unwrap_or_else(|_| "default".to_string());
        let good_pod = unique_name("kube-valid");
        let bad_pod = unique_name("kube-invalid");
        let good_fixture = KubeReadinessFixture::install(&good_pod, &pod_namespace).await?;
        let bad_fixture = KubeReadinessFixture::install(&bad_pod, &pod_namespace).await?;
        kube_apply_invalid_worker_metadata_cr(
            bad_fixture.client(),
            &pod_namespace,
            &bad_pod,
            &bad_pod,
            bad_fixture.pod_uid(),
        )
        .await?;
        kube_wait_for_daemon_settle().await;

        let (rt, drt) = kube_runtime_for_identity(
            good_fixture.pod_name(),
            good_fixture.pod_uid(),
            &pod_namespace,
            "pod",
            None,
        )
        .await?;

        let namespace = unique_name("kube-bad-cr");
        let query = discovery_query_endpoint(&namespace, "backend", "generate");
        drt.discovery()
            .register(endpoint_discovery_spec(&namespace, "backend", "generate"))
            .await?;

        let listed = wait_for_discovery_list(&drt, query, 1).await?;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].instance_id(), hash_pod_name(&good_pod));

        drop(drt);
        drop(rt);
        good_fixture.teardown().await?;
        bad_fixture.teardown().await?;
        Ok(())
    }

    // 目的/场景：实例仅当 ready EndpointSlice 与 CR 同时存在时才进入 snapshot。
    //
    // 生产逻辑：daemon 对 ready entry 无 CR 时 skip（`aggregate_snapshot`）。
    //
    // 测试计划：仅 Pod → register → list 空 → 补 EndpointSlice → list 1 条。
    //
    // 关键断言：无 slice 时空；补 slice 后非空且 instance_id 一致。
    #[tokio::test]
    #[ignore = "requires Kubernetes + PagodaWorkerMetadata CRD (Release); run with --features integration-kube --include-ignored"]
    async fn kube_discovery_requires_ready_endpoint_and_cr() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let pod_namespace =
            std::env::var("POD_NAMESPACE").unwrap_or_else(|_| "default".to_string());
        let pod_name = unique_name("kube-ready");
        let mut fixture = KubeReadinessFixture::install_pod_only(&pod_name, &pod_namespace).await?;
        let (rt, drt) = kube_runtime_for_identity(
            fixture.pod_name(),
            fixture.pod_uid(),
            &pod_namespace,
            "pod",
            None,
        )
        .await?;

        let namespace = unique_name("kube-ready-req");
        let query = discovery_query_endpoint(&namespace, "backend", "generate");
        let spec = endpoint_discovery_spec(&namespace, "backend", "generate");
        let instance = drt.discovery().register(spec).await?;

        let before = wait_for_discovery_list(&drt, query.clone(), 0).await?;
        assert!(before.is_empty());

        fixture.install_endpoint_slice().await?;
        kube_wait_for_daemon_settle().await;

        let after = wait_for_discovery_list(&drt, query, 1).await?;
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].instance_id(), instance.instance_id());

        drt.discovery().unregister(instance).await?;
        drop(drt);
        drop(rt);
        fixture.teardown().await?;
        Ok(())
    }

    // 目的/场景：container 模式下 register/list 往返（`DYN_KUBE_DISCOVERY_MODE=container`）。
    //
    // 生产逻辑：daemon 监视 Pod `containerStatuses.ready`；`CONTAINER_NAME=main` 使用 pod 级 CR 名。
    //
    // 测试计划：container fixture → `kube_runtime_for_identity` container 模式 → register/list。
    //
    // 关键断言：list 1 条；`instance_id == hash_pod_name(pod)`。
    #[tokio::test]
    #[ignore = "requires Kubernetes + PagodaWorkerMetadata CRD (Release); run with --features integration-kube --include-ignored"]
    async fn kube_container_mode_register_list_roundtrip() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let pod_namespace =
            std::env::var("POD_NAMESPACE").unwrap_or_else(|_| "default".to_string());
        let pod_name = unique_name("kube-ctr");
        let fixture =
            KubeReadinessFixture::install_container_mode(&pod_name, &pod_namespace, "main")
                .await?;
        let (_rt, drt) = kube_runtime_for_identity(
            fixture.pod_name(),
            fixture.pod_uid(),
            &pod_namespace,
            "container",
            Some("main"),
        )
        .await?;

        let namespace = unique_name("kube-ctr-ns");
        let query = discovery_query_endpoint(&namespace, "backend", "generate");
        let spec = endpoint_discovery_spec(&namespace, "backend", "generate");
        let instance = drt.discovery().register(spec).await?;

        let listed = wait_for_discovery_list(&drt, query, 1).await?;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].instance_id(), instance.instance_id());
        assert_eq!(instance.instance_id(), hash_pod_name(&pod_name));

        drt.discovery().unregister(instance).await?;
        fixture.teardown().await?;
        Ok(())
    }

    // 目的/场景：Pod 删除后 discovery watch 收到 Removed。
    //
    // 生产逻辑：Pod 删除 → CR ownerReference GC → ready entry 消失 → snapshot 移除实例。
    //
    // 测试计划：register → watch Added → 删 Pod/readiness → watch Removed。
    //
    // 关键断言：Removed 事件 `instance_id` 与注册一致。
    #[tokio::test]
    #[ignore = "requires Kubernetes + PagodaWorkerMetadata CRD (Release); run with --features integration-kube --include-ignored"]
    async fn kube_discovery_pod_delete_removes_from_watch() -> Result<()> {
        let _guard = acquire_contract_test_lock();
        let (_rt, drt, mut fixture) = kube_runtime().await?;

        let namespace = unique_name("kube-del");
        let component = "backend";
        let endpoint = "generate";
        let query = discovery_query_endpoint(&namespace, component, endpoint);

        let mut stream = drt.discovery().list_and_watch(query.clone(), None).await?;
        let spec = endpoint_discovery_spec(&namespace, component, endpoint);
        let instance = drt.discovery().register(spec).await?;

        let added = wait_for_discovery_event(&mut stream, |event| {
            matches!(
                event,
                DiscoveryEvent::Added(DiscoveryInstance::PortName(inst))
                    if inst.instance_id == instance.instance_id()
            )
        })
        .await?;
        let DiscoveryEvent::Added(DiscoveryInstance::PortName(added_inst)) = added else {
            return Err(anyhow!("expected Added endpoint event"));
        };

        fixture.delete_pod_and_clear_readiness().await?;
        kube_wait_for_daemon_settle().await;

        let removed = wait_for_discovery_event(&mut stream, |event| {
            matches!(
                event,
                DiscoveryEvent::Removed(DiscoveryInstanceId::PortName(id))
                    if id.namespace == namespace
                        && id.servicegroup == component
                        && id.portname == endpoint
                        && id.instance_id == added_inst.instance_id
            )
        })
        .await?;
        assert!(matches!(removed, DiscoveryEvent::Removed(_)));

        Ok(())
    }
}
