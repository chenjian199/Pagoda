# Pagoda Runtime 测试目录

本文档对 `lib/runtime/tests/` 下各测试文件及用例进行说明，便于理解覆盖范围、生产代码对应关系与运行方式。内容与仓库内合约测试源码注释同步维护。

## 目录结构概览

| 类别 | 文件 | 运行方式 | 外部依赖 |
|------|------|----------|----------|
| 集成契约测试 | `*.rs`| PR 必跑（部分 `#[ignore]`） | 多数无；etcd/NATS/K8s 见各文件 |
| 单元/示例测试 | `pipeline.rs`、`pool.rs`、`lifecycle.rs` | `cargo test --test <name>` | 无 |
| 旧版 soak | `soak.rs` | `--features integration` | 无 |
| 公共 harness | `common/` | 被集成测试引用 | — |

### 集成用例统计（按 `#[tokio::test]` 计）

| 文件 | PR | `#[ignore]` | 合计 |
|------|-----|-------------|------|
| `runtime_lifecycle` | 5 | 0 | 5 |
| `component_routing` | 9 | 0 | 9 |
| `request_plane`（tcp + nats） | 6 | 7 | 13 |
| `event_plane`（zmq + nats） | 2 | 1 | 3 |
| `pipeline`（含 `disaggregated`） | 5 | 0 | 5 |
| `discovery`（file + etcd + kube） | 6 | 11 | 17 |
| `storage` | 3 | 2 | 5 |
| `health_metrics` | 6 | 0 | 6 |
| `system_status`（含 engine/local） | 8 | 0 | 8 |
| `compute` | 2 | 0 | 2 |
| `config_env` | 5 | 0 | 5 |
| `failures` | 5 | 0 | 5 |
| `soak` | 1 | 4 | 5 |
| **合计** | **63** | **25** | **88** |

**通用运行示例：**

```bash
# PR 层：全部 integration bin（跳过 ignored）
cargo test -p pagoda-runtime --tests -- --test-threads=1

# 单个集成测试文件
cargo test -p pagoda-runtime --test discovery -- --test-threads=1

# Nightly：etcd discovery
ETCD_ENDPOINTS=http://127.0.0.1:2379 \
  cargo test -p pagoda-runtime --test discovery \
  --features testing-etcd -- --test-threads=1 --include-ignored

# Release：K8s discovery（需集群 + PagodaWorkerMetadata CRD）
cargo test -p pagoda-runtime --test discovery \
  --features integration-kube -- --test-threads=1 --include-ignored

# Nightly：NATS request plane + event plane
NATS_SERVER=nats://127.0.0.1:4222 \
  cargo test -p pagoda-runtime --test request_plane \
  --test event_plane -- --test-threads=1 --include-ignored
```

集成测试通过 `common/contract.rs` 中的 `acquire_contract_test_lock()` 串行化全局环境变更，避免并行污染。TCP request plane 使用进程级 `shared_integration_runtime()`，与 `#[tokio::test]` 生命周期解耦。

---

## 集成契约测试

### `runtime_lifecycle.rs`

验证 `Worker`、`Runtime`、`DistributedRuntime` 的生命周期与 shutdown 语义。

| 用例 | 目的/场景 | 生产逻辑 | 关键断言 |
|------|-----------|----------|----------|
| `worker_execute_runs_async_workload_to_completion` | `Worker::execute` 在独立进程中运行 async workload 直至完成 | `worker.rs`：`from_settings` + `execute` 阻塞至 workload 结束；`INIT` 每进程仅一次 | 子进程运行 `--test lifecycle test_lifecycle` 退出码为 0 |
| `worker_from_current_executes_without_nested_owned_runtime` | 在已有 Tokio runtime 的进程内，`Worker::from_current` 执行应用且不嵌套第二套 owned runtime | `worker.rs`：`from_current` 包装当前 handle；`execute_async` 每进程仅一次 `INIT` | 闭包内 `Runtime` 与 worker 同 `id()`；`runtime_from_existing` 复用同一 handle |
| `distributed_runtime_process_local_starts_and_stops` | process-local DRT 构造与 `Runtime::shutdown` 联动 | `runtime.rs` Phase 3 取消 token；`distributed.rs` 转发 `primary_token` | shutdown 前 token 活跃；shutdown 后 `primary_token` 已取消 |
| `runtime_clone_observes_same_registries` | `DistributedRuntime::clone` 共享 discovery / metrics 视图 | `DistributedRuntime` 内部 `Arc` 共享 | clone 侧 client 可见同一 `instance_id` |
| `shutdown_rejects_new_requests_after_draining_inflight` | graceful shutdown 排空 in-flight 后拒绝新请求 | `runtime.rs` + `portname.rs`：Phase 1 注销 portname；等待 inflight；Phase 3 取消主 token | in-flight 收到 `drained`；第二次 `generate` 返回 no instances 类错误 |

---

### `component_routing.rs`

验证 `namespace → servicegroup → portname` 路由隔离、discovery 注册/注销与副本池行为。

| 用例 | 目的/场景 | 生产逻辑 | 关键断言 |
|------|-----------|----------|----------|
| `register_discover_call_unregister_roundtrip` | 注册 → discovery → RPC → 注销完整闭环 | `PortName::start` 注册；`unregister_portname_instance` 调用 `discovery.unregister` | 注销前流式响应 `a,b,c`；注销后 RPC 失败 |
| `same_endpoint_different_namespace_does_not_mix` | namespace 为 discovery 与 RPC 隔离边界 | discovery key 含 namespace 层级 | A 有实例可调用；B 空实例且 RPC 失败 |
| `same_endpoint_same_namespace_forms_replica_pool` | 同逻辑 portname 多 worker 形成副本池 | file KV 共享 discovery；`PushRouter::round_robin` 轮询 | discovery 2 实例；RPC tag 至少 2 种 |
| `same_namespace_different_component_is_isolated` | servicegroup 为路由隔离维度 | discovery key 含 servicegroup | `servicegroup-b` 无实例 |
| `same_namespace_same_component_different_endpoint_is_isolated` | portname 为路由隔离维度 | 同 servicegroup 下不同 portname 独立池 | `generate-b` 无实例 |
| `calling_unregistered_endpoint_returns_unavailable` | 从未注册的 portname 快速失败 | `PushRouter` 在 `instance_ids_avail().is_empty()` 时报错 | `generate_expect_no_instances` |
| `calling_endpoint_after_unregister_returns_unavailable` | 注销后调用失败（区别于从未注册） | unregister → discovery Removed → watch 收敛 | 注销前成功；注销后失败 |
| `duplicate_instance_registration_is_idempotent_or_rejected_consistently` | 重复 `register_portname_instance` 行为稳定 | KV `insert` 同 key 覆盖 | `client.instances().len() == 1` |
| `different_models_same_logical_endpoint_are_rejected` | 同一逻辑 portname 不允许冲突 model 名 | `Discovery::register` 检查 `find_conflicting_model_name` | 第二次 register Err 含 `Cannot register model 'model-b'` |

---

### `request_plane.rs`

验证 TCP（及 NATS）request plane 的往返、流式、取消、并发、背压与 metrics。

#### `mod tcp`（PR 必跑）

| 用例 | 目的/场景 | 生产逻辑 | 关键断言 |
|------|-----------|----------|----------|
| `tcp_roundtrip_payload_and_response` | TCP 单包 payload 原样往返 | `Ingress::for_engine` → TCP codec → echo engine | 单 chunk 等于 payload |
| `tcp_client_streaming_response_preserves_order` | 流式响应顺序与 engine emit 一致 | streaming engine 按字符 emit | chunks 为 `s,t,r,e,a,m` |
| `tcp_client_drop_cancels_handler` | client 丢弃响应流时 backend 感知取消 | client drop → `context.kill()`；`CancellableEngine` 检查 `is_killed()` | 3s 内 `cancelled == true` |
| `tcp_concurrent_requests_preserve_context_isolation` | 并发 RPC 的 request id 与 payload 可区分 | `SingleIn::with_id` 写入 `AsyncEngineContext` | 16 路响应为 `{payload}:{request_id}` |
| `tcp_backpressure_does_not_drop_frames` | 慢消费者不丢 frame | 小 outbound channel + 流式 TCP | 32 chunk 顺序完整 |
| `tcp_metrics_record_success_and_failure` | 成功 RPC 更新 metrics；路由失败不污染计数 | `tcp_client.rs` bytes counters；`REQUEST_PLANE_INFLIGHT` | bytes 增加；inflight 不泄漏；路由失败不增 TCP_ERRORS |

#### `mod nats`（`#[ignore]`，需 `NATS_SERVER`）

| 用例 | 目的/场景 | 生产逻辑 | 关键断言 |
|------|-----------|----------|----------|
| `nats_roundtrip_payload_and_response` | NATS 单包 echo 往返 | `RequestPlaneMode::Nats` | 单 chunk 等于 payload |
| `nats_subject_includes_routing_identity` | NATS subject 含路由身份 | `build_transport_type` → `nats::instance_subject` | subject 含 namespace/servicegroup/portname/instance id |
| `nats_queue_group_balances_replicas` | 多副本经 NATS service group 调度 | 两 DRT 共享 file KV；`NatsMultiplexedServer` | discovery 2 实例；tag 至少 2 种 |
| `nats_request_timeout_returns_error` | handler 长时间不产出时客户端超时 | `PGD_HTTP_BACKEND_STREAM_TIMEOUT_SECS` | 流含 timeout/error 项 |
| `nats_reconnect_restores_request_path` | NATS broker 短暂断连后 request plane 恢复 | docker stop/start NATS；`async_nats` 自动重连 | outage 期间 RPC 失败；恢复后 echo 成功 |
| `nats_service_registers_component_endpoints` | NATS microservice 注册 servicegroup 多 portname | `register_nats_service` + `NatsMultiplexedServer`；`$SRV.STATS` | ≥2 portname；name 含 generate/health |
| `nats_service_subject_matches_routing_identity` | service portname subject 含路由身份 | slugify `{ns}_{servicegroup}` + `{portname}-{instance_hex}` | subject/name 含 service、portname、instance id |

---

### `event_plane.rs`

验证 event plane pub/sub、Msgpack codec 与 dynamic subscriber 过滤。

#### `mod zmq`（PR 必跑）

| 用例 | 目的/场景 | 生产逻辑 | 关键断言 |
|------|-----------|----------|----------|
| `zmq_publish_subscribe_roundtrip` | ZMQ 直连 publish/subscribe 往返 | `EventPublisher`/`EventSubscriber` + discovery | topic 匹配；Msgpack payload 相等 |
| `zmq_dynamic_subscriber_filters_channel` | dynamic subscriber 按 topic 过滤 | `DynamicSubscriber` + envelope topic filter | 仅收到 `alpha` |

#### `mod nats`（`#[ignore]`，需 `NATS_SERVER`）

| 用例 | 目的/场景 | 生产逻辑 | 关键断言 |
|------|-----------|----------|----------|
| `nats_event_transport_roundtrip` | NATS event plane 往返 | `NatsTransport` publish/subscribe | topic 与 Msgpack payload 匹配 |

---

### `pipeline.rs`

验证进程内 pipeline link 与跨节点 disaggregated 路径。

| 用例 | 目的/场景 | 生产逻辑 | 关键断言 |
|------|-----------|----------|----------|
| `pipeline_frontend_operator_backend_postprocessor_roundtrip` | frontend → node → operator → backend → post 顺序契约 | `ServiceFrontend::link` 各 stage map 顺序 | 单 chunk data 为 `input-node-pre-post-op` |
| `pipeline_stream_context_survives_node_chain` | request id 贯穿节点链 | 各 stage 通过 `ResponseStream::new(..., ctx)` 传递 context | `response.context().id() == "ctx-survives"` |
| `pipeline_backend_error_propagates_to_client` | backend `Err` 传播到调用方 | `ServiceBackend::from_engine` 不吞错误 | `generate` Err 含 `backend contract error` |
| `pipeline_cancel_stops_downstream_stream` | client 丢弃响应流后 backend 停止 emit | drop → `kill()` → `is_killed()` | 3s 内 `cancelled == true` |
| `disaggregated_pipeline_cross_node_roundtrip`（`mod disaggregated`） | MockNetworkTransport 跨节点 echo 往返 | Node0 `MockNetworkEgress`；Node1 ingress → `SegmentSource` → `ServiceBackend` | 响应单 chunk 等于 payload |

---

### `discovery.rs`

验证 discovery 的 list/watch、跨 runtime 共享、query 过滤及 metadata registry；按后端分 `file` / `etcd` / `kube` 模块。

#### 顶层（file 后端，PR 必跑）

| 用例 | 目的/场景 | 生产逻辑 | 关键断言 |
|------|-----------|----------|----------|
| `metadata_registry_roundtrips_endpoint_metadata` | `MetadataArtifactRegistry` register/get/unregister | `DistributedRuntime::metadata_artifacts()` | register 后 `get` 有路径；unregister 后 `is_empty()` |
| `discovery_list_ignores_invalid_values` | list 遇无法反序列化 KV 时不污染有效实例 | `KVStoreDiscovery::list` parse 失败 skip | list 仍仅 1 条合法实例 |

#### `mod file`

| 用例 | 目的/场景 | 生产逻辑 | 关键断言 |
|------|-----------|----------|----------|
| `file_discovery_watch_sees_register_and_unregister` | file KV watch 观察 Added/Removed | `KVStoreDiscovery::list_and_watch` | Added instance_id 一致；Removed 匹配 portname |
| `file_discovery_cross_runtime_shares_instances` | 两 DRT 共享 file KV 互相可见注册 | `DiscoveryBackend::KvStore(File)` | DRT-B list 与 client 均见 1 实例 |
| `file_discovery_filters_exact_namespace_component_endpoint` | query 精确匹配三元组 | `KVStoreDiscovery::query_prefix` | 精确 1 条；错 portname/namespace 为空 |
| `watch_reconnect_recovers_without_duplicate_instances` | watch 断开后重连不重复实例 | drop stream → 新 watch replay Added；`list` 去重 | 两次 Added 后 `list` 仍 1 条 |

#### `mod etcd`（`#[ignore]`，`--features testing-etcd`，`ETCD_ENDPOINTS`）

| 用例 | 目的/场景 | 生产逻辑 | 关键断言 |
|------|-----------|----------|----------|
| `etcd_discovery_watch_rebuilds_initial_snapshot` | watch 在已有 key 上重建初始快照 | `kv::Manager::watch` replay + 增量 | 首事件为 Added 且 instance_id 一致 |
| `etcd_lease_expiry_removes_instance` | lease 过期后实例移除 | `etcd::Client` attach_lease + TTL | shutdown 后 12s list 为空 |
| `prefix_delete_removes_all_child_instances` | etcd prefix 删除反映到 discovery watcher | `KVStoreDiscovery::list_and_watch` + `kv_delete` prefix | watch 收 2 Removed；`list` 为空 |

#### `mod kube`（`#[ignore]`，`--features integration-kube`，K8s + CRD）

Harness：`kube_runtime()`（单 Pod + EndpointSlice）、`KubeDualPodFixture`、`KubeReadinessFixture::{install,install_pod_only,install_container_mode}`、`wait_for_discovery_list`（轮询 daemon 收敛）、`fixture.teardown()` 清理 Pod/Slice。

| 用例 | 目的/场景 | 生产逻辑 | 关键断言 |
|------|-----------|----------|----------|
| `kube_discovery_register_list_roundtrip` | K8s register/list 往返 | `KubeDiscoveryClient::register_internal` + `apply_cr` | `wait_for_discovery_list` 1 条；instance_id 一致 |
| `kube_discovery_watch_sees_register_and_unregister` | K8s watch Added/Removed | `list_and_watch` + `metadata_watch` | Added/Removed 三元组一致 |
| `kube_discovery_filters_exact_namespace_component_endpoint` | K8s query 精确过滤 | `MetadataSnapshot::filter` | 精确 query 1 条；错 portname/namespace `wait_for_discovery_list` 为 0 |
| `kube_discovery_cross_pod_shares_instances` | 跨 Pod 发现 | `DiscoveryDaemon` 聚合 namespace snapshot；`kube_dual_pod_runtimes` | pod-B `wait_for_discovery_list` 见 pod-A；`instance_id == hash_pod_name(pod_a)` |
| `kube_discovery_list_ignores_invalid_cr_data` | 非法 CR 不污染 list | `aggregate_snapshot` deserialize skip | 坏 CR + 合法 register 后 list 仅 1 条合法实例 |
| `kube_discovery_requires_ready_endpoint_and_cr` | ready EndpointSlice + CR 缺一不可 | ready entry 无 slice 时 skip | `install_pod_only` 后 list 空；`install_endpoint_slice` 后 1 条 |
| `kube_container_mode_register_list_roundtrip` | container 模式 register/list | `PGD_KUBE_DISCOVERY_MODE=container`；Pod `containerStatuses.ready` | list 1 条；`instance_id == hash_pod_name(pod)` |
| `kube_discovery_pod_delete_removes_from_watch` | Pod 删除触发 Removed | Pod 删 → CR GC → ready 消失 | watch 收 Removed；`instance_id` 一致 |

---

### `storage.rs`

验证 `storage::kv::Manager` 公共 API（memory / file / etcd）。

| 用例 | 模块 | 目的/场景 | 关键断言 |
|------|------|-----------|----------|
| `typed_prefix_watcher_ignores_invalid_values` | 顶层 | typed load 遇非法 JSON 不影响其它条目 | invalid → `JSONDecodeError`；valid 正常反序列化 |
| `memory_kv_crud_and_watch` | `memory` | memory KV CRUD + watch | watch 收 Put/Put/Delete；delete 后 get 为 None |
| `file_kv_persists_across_reopen` | `file` | file KV 关闭重开数据仍在 | 重开后 `get("persist-me")` 值不变 |
| `etcd_kv_crud_watch_and_prefix_delete` | `etcd`（ignore） | etcd CRUD、watch、前缀批量删 | watch Put,Put,Delete,Delete；删后 get 为 None |
| `nats_kv_crud_and_watch` | `nats`（ignore） | NATS KV CRUD + watch | watch Put,Put,Delete；delete 后 get 为 None |

---

### `health_metrics.rs`

验证 portname/system health 与 Prometheus metrics 导出。

| 用例 | 目的/场景 | 生产逻辑 | 关键断言 |
|------|-----------|----------|----------|
| `health_check_target_registration_notifies_manager` | 注册 health check target 时通知 `HealthCheckManager` | `register_health_check_target` + `new_portname_tx`（`PGD_HEALTH_CHECK_ENABLED`） | `get_health_check_target` / `get_portname_health_check_notifier` 均存在 |
| `endpoint_health_tracks_registration` | portname 启动后 system health 记录 Ready | `PortName::start` → `set_portname_registered` | `get_portname_health_status == Ready` |
| `endpoint_health_tracks_canary_result` | canary 成功后 health 变 Ready | `HealthCheckManager` + local registry | 初始 NotReady；5s 内变 Ready |
| `system_health_uses_endpoint_status_when_configured` | 配置后系统健康由 portname 聚合 | `PGD_SYSTEM_USE_PORTNAME_HEALTH_STATUS` | 注册前 unhealthy；注册后 healthy 且含 `generate: ready` |
| `metrics_labels_include_runtime_identity` | metrics 含 portname 层级标签 | `WorkHandlerMetrics::from_portname` | scrape 含 servicegroup、portname |
| `metrics_names_are_prometheus_safe` | 指标名与 label 符合 Prometheus 规范 | `metrics/prometheus_names.rs` sanitize | 内置名与 scrape 每行均通过校验 |

---

### `system_status.rs`

验证 system status HTTP 服务（`/live`、`/health` 及自定义路径）。

| 用例 | 目的/场景 | 生产逻辑 | 关键断言 |
|------|-----------|----------|----------|
| `live_endpoint_reports_process_liveness` | `/live` 进程存活探测 | `spawn_system_status_server` | HTTP 200；body 含 `"status":"ready"` |
| `health_endpoint_reports_aggregated_health` | `/health` 随 portname 注册变化 | `health_handler` + `SystemHealth` | 注册前 503 notready；注册后 200 ready |
| `status_endpoint_includes_registered_endpoints` | health JSON 列出已注册 portname | `portnames` 字段来自 `SystemHealth` | `portnames.generate == "ready"` |
| `custom_health_and_live_paths_are_honored` | 自定义 health/live 路径生效 | `PGD_SYSTEM_*_PATH` env | 默认路径 404；自定义路径 200 |

#### `mod engine`（PR 必跑）

| 用例 | 目的/场景 | 生产逻辑 | 关键断言 |
|------|-----------|----------|----------|
| `engine_route_register_and_json_roundtrip` | `/engine/*` 注册后 GET/POST JSON 往返 | `engine_route_handler` + `EngineRouteRegistry` | POST echo 200；GET 空 body 200 |
| `engine_route_unknown_path_returns_404` | 未注册 engine path 返回 404 | registry miss → NOT_FOUND JSON | HTTP 404；`Route not found` |
| `engine_route_handler_error_propagates` | callback Err 映射为 HTTP 500 | callback Err → Handler error JSON | HTTP 500；含 callback 错误文本 |

#### `mod local`（PR 必跑）

| 用例 | 目的/场景 | 生产逻辑 | 关键断言 |
|------|-----------|----------|----------|
| `local_endpoint_registry_lists_registered_engines` | local registry register/get 与 engine routes 列表 | `LocalPortnameRegistry` + `EngineRouteRegistry::routes` | 已注册 get 为 Some；routes 含两条路径 |

---

### `compute.rs`

验证 `Runtime::compute_pool()` 与 TCP request plane 共存及 `ComputeMetrics` 可观测性。

| 用例 | 目的/场景 | 生产逻辑 | 关键断言 |
|------|-----------|----------|----------|
| `compute_pool_offloads_without_blocking_request_plane` | CPU 密集 offload 时 reactor/RPC 仍响应 | `ComputePool::execute` + tokio-rayon | RPC < 3s；heartbeat tick ≥ 15 |
| `compute_pool_metrics_increment_on_task` | 执行任务后 metrics 递增 | `record_task_start` / `record_task_completion` | `tasks_total==1`；`max_task_duration_us > 0` |

> Process harness 经 `Runtime::from_settings` + `PGD_COMPUTE_THREADS=2` 初始化 compute pool。

---

### `config_env.rs`

验证运行时配置从环境变量读取及测试隔离。

| 用例 | 目的/场景 | 生产逻辑 | 关键断言 |
|------|-----------|----------|----------|
| `runtime_config_reads_env_over_defaults` | env 覆盖默认配置 | `DistributedConfig::from_settings`；`get_tcp_rpc_host_from_env`；`PGD_DISCOVERY_BACKEND=mem` | `request_plane == Http`；TCP host 匹配 env |
| `invalid_env_value_returns_configuration_error` | 非法 request plane 返回明确错误 | `RequestPlaneMode::from_str`（严格解析路径） | Err 含 `Invalid request plane` |
| `test_env_isolation_prevents_parallel_pollution` | 串行锁 + temp-env 防并行污染 | `acquire_contract_test_lock` + `async_with_vars` | 两段 env 各自读到 value-a / value-b |
| `network_advertise_host_uses_env_when_set` | HTTP/TCP advertise host 优先 env | `get_http_rpc_host_from_env` / `get_tcp_rpc_host_from_env` | 返回值与 `PGD_HTTP_RPC_HOST` / `PGD_TCP_RPC_HOST` 一致 |
| `nats_and_etcd_auth_env_are_applied` | NATS/ETCD 认证 env 进入 client options | `nats::ClientOptions::builder`；`etcd::ClientOptions::default` | Debug 含 `NATS_AUTH_*` / `ETCD_AUTH_*` 用户名 |

---

### `failures.rs`

验证错误隔离、critical task、并发 churn 与 transport decode 错误。

| 用例 | 目的/场景 | 生产逻辑 | 关键断言 |
|------|-----------|----------|----------|
| `handler_panic_or_error_is_reported_without_killing_runtime` | backend 错误与 critical panic 不杀死 runtime | `Ingress` 错误写 prologue；`CriticalTaskExecutionHandle` | 本地 Err；panic 后 parent cancelled；echo 仍成功 |
| `critical_task_failure_is_visible_to_owner` | critical task 失败触发 parent cancel | `utils/tasks/critical.rs` | `join()` Err；`parent.is_cancelled()` |
| `concurrent_register_unregister_is_eventually_consistent` | 并发 register/unregister 最终一致 | discovery KV 覆盖/删除 | `wait_for_instances_empty` |
| `transport_decode_error_closes_bad_request_only` | TCP decode 失败只影响坏帧 | `TcpRequestMessage::decode` | 截断帧 decode Err；前后合法 RPC 均成功 |
| `external_discovery_outage_returns_unavailable_then_recovers` | 外部 discovery 短暂不可用后恢复 | `wipe_file_discovery_namespace` 模拟 outage；`register_portname_instance` 恢复 | outage 期间 no instances；恢复后 echo 成功 |

---

### `soak.rs`

契约框架下的短/长 soak 压测。

| 用例 | 目的/场景 | 运行 | 关键断言 |
|------|-----------|------|----------|
| `soak_smoke_completes_short_run` | 2s 冒烟：持续 RPC 稳定完成 | PR 必跑；`PGD_SOAK_RUN_DURATION=2s`，batch=8 | backend 计数 > 0；无 panic |
| `soak_sustained_load_reports_diagnostics` | 长 soak（默认 30s，batch 64） | `#[ignore]` + `--include-ignored` | 完整 duration 内 RPC 无失败 |
| `random_endpoint_churn_does_not_leave_stale_instances` | 6 portname 伪随机 register/unregister churn | `#[ignore]` + `--include-ignored` | `discovery.list` 与 `client.instances()` 一致；file KV 0 残留 |
| `runtime_shutdown_after_soak_releases_external_resources` | soak 后注销释放外部 discovery 存储 | `#[ignore]` + `--include-ignored`；可选 `testing-etcd` | file KV 计数归零；etcd list 空 |
| `long_running_streams_can_be_cancelled_without_task_leak` | 12 轮 TCP 长流 cancel，无 backend task 泄漏 | `#[ignore]` + `--include-ignored` | 每轮 `active_backend_tasks==0`；探活 RPC 成功 |

---

## 单元与遗留测试

### `pipeline.rs`

进程内 pipeline 链路的早期单元测试，使用 mock engine 与 `Annotated` 流类型。

| 用例 | 说明 | 状态 |
|------|------|------|
| `test_service_source_sink` | `ServiceFrontend` 直连 `ServiceBackend`，输入 `"test"` 产生 4 个输出 chunk | 通过 |
| `test_service_source_node_sink` | 含 pre/post processor 的完整链路，20 个输出 | 通过 |
| `test_disaggregated_service` | 跨节点 `SegmentSink`/`SegmentSource` + MockNetwork | **忽略**：`AsyncEngineStream` 缺 `Sync` supertrait |
| `test_service_source_node_sink_with_operator` | 含 `PreprocesOperator` 的 operator 链路；1 条 Comment + 48 条 Data | 通过 |

---

### `pool.rs`

`IndexedPool` 自定义池实现：归还时按序插入，验证 acquire 取最小元素与归还后排序。

| 用例 | 说明 |
|------|------|
| `test_indexed_pool_sorting` | 初始排序；acquire 取 1、2；修改后归还验证插入位置（3,4,5,10 与 3,4,4,5,10） |

---

### `lifecycle.rs`

最小 Worker 生命周期冒烟。

| 用例 | 说明 |
|------|------|
| `test_lifecycle` | `Worker::from_settings()` → `execute(hello_world)` 同步测试 |

---

### `soak.rs`（遗留）

需 `--features integration` 的端到端 soak，通过 `Worker` 启动完整 DRT + backend/client。

| 用例 | 说明 |
|------|------|
| `integration::main` | 读 `PGD_SOAK_RUN_DURATION`、`PGD_SOAK_BATCH_LOAD`、`PGD_QUEUED_UP_PROCESSING`；并发 batch RPC 测吞吐 |

运行示例：

```bash
export PGD_SOAK_BATCH_LOAD=10000
export PGD_SOAK_RUN_DURATION=60s
cargo test --test soak integration::main --features integration -- --nocapture
```

---

## 公共测试基础设施（`common/`）

| 模块 | 职责 |
|------|------|
| `contract.rs` | 集成测试主 harness：`process_local_runtime`、`shared_integration_runtime`、`file_backed_runtime`、`etcd_runtime`、`nats_*`、`serve_portname_*`、`shutdown_runtime`、`acquire_contract_test_lock`、`unique_name` 等 |
| `contract.rs`（K8s，`feature integration-kube`） | `KubeReadinessFixture`、`KubeDualPodFixture`、`kube_runtime` / `kube_runtime_for_identity`、`kube_dual_pod_runtimes`、`wait_for_discovery_list`、`kube_apply_invalid_worker_metadata_cr` |
| `contract_engines.rs` | 契约用 mock engine：`CancellableEngine`、`InstanceTagEngine`、`BlockingFirstChunkEngine`、pipeline 契约 service 工厂 |
| `engines.rs` | 通用 `AsyncGenerator`、`LlmdbaEngine`；内含 `test_async_processor`、`test_generator` 单元测试 |
| `mock.rs` | `MockNetworkTransport` 等网络 mock（disaggregated pipeline） |

## Feature 与 Ignore 对照

| Feature / 环境变量 | 影响的用例 |
|--------------------|------------|
| （无，默认） | 绝大多数合约测试顶层与 `mod file` / `mod tcp` / `mod zmq` / `mod memory` 用例 |
| `testing-etcd` + `ETCD_ENDPOINTS` + `--include-ignored` | `discovery::etcd::*`（3）；`storage::etcd::*`（1） |
| `integration-kube` + K8s + `PagodaWorkerMetadata` CRD + `--include-ignored` | `discovery::kube::*`（8） |
| `NATS_SERVER` + `--include-ignored` | `request_plane::nats::*`（7）；`storage::nats::*`（1）；`event_plane::nats::*`（1） |
| `--include-ignored`（Release soak） | `soak::{soak_sustained_load_*,random_endpoint_churn_*,runtime_shutdown_after_soak_*,long_running_streams_*}`；`PGD_SOAK_*` 仅影响 sustained load |
| `integration` feature | `soak.rs` 中的 `integration::main` |

