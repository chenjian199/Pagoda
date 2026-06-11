# integration_soak 间歇性挂死 — 错误报告

| 字段 | 内容 |
|------|------|
| **状态** | 已定位根因；修复方案已验证，当前代码库为未修复状态 |
| **影响范围** | `pagoda-runtime` file-backed discovery；`integration_soak` 全套及所有调用 `Client::wait_for_instances()` 的路径 |
| **严重度** | 测试：高（假死、无超时）；生产：中（file KV + 高频 churn 场景下可能路由不到实例） |
| **与 upstream 重构关系** | **无关**（重命名/测试拆分未改变 churn 业务逻辑） |

---

## 1. 摘要

`cargo test --test integration_soak -- --test-threads=1 --include-ignored` 在 `random_endpoint_churn_does_not_leave_stale_instances` 处**间歇性永久挂起**（进程 `futex_wait`，CPU ≈ 0，无进一步输出）。

**根因**：file-backed discovery 的 inotify watch 路径在高压下**可能丢弃事件**；`Client::wait_for_instances()` **仅**依赖 `watch::Receiver::changed()` 且无超时，事件丢失后永远等不到实例通知。

**非根因**：Pagoda-upstream 的 `component`→`servicegroup` 等重构；soak 测试的一致性断言逻辑。

---

## 2. 现象

### 2.1 典型终端输出

```
test long_running_streams_can_be_cancelled_without_task_leak ... ok
test random_endpoint_churn_does_not_leave_stale_instances ...
```

- 停在上面的 `...` 后，**数分钟到数小时无 `ok`**
- 单独过滤该用例有时 ~30s 通过；跑**全套** `integration_soak` 更容易挂
- `Finished in 0.6s` 表示未重编译，可能跑的是旧二进制

### 2.2 进程状态

```bash
ps -p <PID> -o stat,wchan,cmd
# STAT=Sl+  WCHAN=futex_wait_queue_me
```

挂死线程多在 Tokio 异步等待（`wait_for_instances` → `rx.changed().await`）。

---

## 3. 非根因说明（避免误判）

| 常被怀疑的方向 | 结论 |
|----------------|------|
| Pagoda-upstream 重构（`serve_streaming_portname`→`serve_streaming_endpoint` 等） | **否**。与 Pagoda 主分支 churn 流程等价；Pagoda 同命令可 ~87s 全过 |
| soak 测试断言错误 | **否**。`discovery.list` 与 `client.instances()` 一致性校验合理 |
| upstream 独有 runtime 逻辑回归 | **否**。挂死相关 `src/` 与 Pagoda 在修复前一致 |
| 仅并行测试抢锁 | **部分相关**。`--test-threads=1` 仍可能挂；并行会加剧但不唯一 |

---

## 4. 根因链（自底向上）

```
integration_soak::random_endpoint_churn
  └─ serve_streaming_endpoint / re-register
       └─ start_served_endpoint / client.wait_for_instances()   [无超时]
            └─ instance_source: watch::Receiver::changed().await  [永久阻塞]
                 └─ portname_watcher 仅消费 discovery_stream      [无 list() 对账]
                      └─ KVStoreDiscovery::list_and_watch         [无 discovery 层初始 list]
                           └─ kv::Manager::watch                  [后续事件 try_send 可丢]
                                └─ FileStore::watch               [inotify → try_send，cap=256]
```

### 4.1 File 层 — 事件可丢

**文件**：`lib/runtime/src/storage/kv/file.rs`

- `WATCH_CHANNEL_CAPACITY = 256`
- notify 回调使用 `try_send`；channel 满时 **丢弃 inotify 事件**（仅打 warn 日志）

### 4.2 Manager 层 — 下游可丢

**文件**：`lib/runtime/src/storage/kv.rs`（`Manager::watch`）

- 启动时会 `bucket.entries()` 回放已有 key（初始快照 **有**）
- 后续新事件使用 `try_send`；消费者慢时 **可丢事件**

### 4.3 Discovery 层 — 无额外快照

**文件**：`lib/runtime/src/discovery/kv_store.rs`（`list_and_watch`）

- 直接 `store.watch()` 后消费 stream
- **未**在 discovery 层先 `list(query)` 再 yield `Added`（与 Dynamo 的 kube 路径不同；Dynamo 在 Manager 层回放已较强）

### 4.4 Client 层 — 无兜底、无超时

**文件**：`lib/runtime/src/servicegroup/client.rs`

**`wait_for_instances()`**（约 396–414 行）：

```rust
loop {
    let instances = rx.borrow_and_update().to_vec();
    if !instances.is_empty() { return Ok(instances); }
    rx.changed().await?;  // watch 不通知 → 永久等待
}
```

**`portname_watcher` 后台 task**（约 567–616 行）：

- 只 `select!` `discovery_stream.next()` 与 `watch_tx.closed()`
- **无**周期性 `discovery.list()` 对账

### 4.5 测试 harness — 放大假死

**文件**：`lib/runtime/tests/common/contract.rs`（`start_served_endpoint`）

- 每个 served endpoint 调用 `client.wait_for_instances().await?`（无超时）

**文件**：`lib/runtime/tests/integration_soak.rs`（churn 循环）

- re-register 分支同样调用 `client.wait_for_instances()`（无超时）
- `wait_for_instances_empty` 有 3s 超时，故 unregister 路径会失败而非假死

---

## 5. 为何「像只有 Pagoda-upstream 会挂」

| 因素 | 说明 |
|------|------|
| **间歇性 race** | 同一套代码 Pagoda 也可通过；upstream 更容易在开发中反复踩中 |
| **僵尸进程清理不全** | upstream 测试二进制名为 `discovery-*`、`failures-*` 等；`pkill -f integration_` **无法清除** |
| **开发习惯** | 多在 upstream 跑 `run_tests.sh` / 多文件测试，inotify / 端口 / 后台 task 残留更多 |
| **与 Dynamo 对比** | Dynamo `wait_for_instances` 同样无兜底，但 file 层用 `blocking_send`、`Manager` 用 `send_timeout` + 16K channel，**更难丢事件** |

---

## 6. 复现步骤

### 6.1 标准复现（全套 soak）

```bash
cd Pagoda-upstream

# 建议先清环境（注意 upstream 要清整个 deps 目录，不只是 integration_）
pkill -9 -f 'Pagoda-upstream/target/debug/deps/' 2>/dev/null || true
pkill -9 -f 'dynamo/target/debug/deps/' 2>/dev/null || true
pgrep -af 'target/debug/deps'   # 应为空

cargo test -p pagoda-runtime --test integration_soak -- \
  --test-threads=1 --include-ignored --nocapture
```

**预期（未修复）**：间歇性在 `random_endpoint_churn_does_not_leave_stale_instances` 挂死。

### 6.2 最小用例（单测，通过率更高）

```bash
cargo test -p pagoda-runtime --test integration_soak \
  random_endpoint_churn_does_not_leave_stale_instances -- \
  --test-threads=1 --include-ignored --nocapture
```

单独跑往往 ~30s 通过；**不能**据此认为问题不存在。

### 6.3 提高复现率（污染环境后）

1. 先跑一批 upstream 集成测试：`./lib/runtime/tests/run_tests.sh pr` 或 `cargo test -p pagoda-runtime --tests`
2. 故意不杀 `discovery-*` / `request_plane-*` 僵尸进程
3. 再跑 6.1 全套 soak

### 6.4 对比实验（证明非 upstream 重构问题）

```bash
# Pagoda 主分支 — 同一命令
cd ../Pagoda
cargo test -p pagoda-runtime --test integration_soak -- \
  --test-threads=1 --include-ignored
```

Pagoda 通常 ~87s 全过；多跑几次或污染环境后 Pagoda **也可能**挂。

---

## 7. 排查手册

### 7.1 确认卡在哪个测试

```bash
pgrep -af 'integration_soak-'
ps -p <PID> -o stat,wchan,etime,cmd
```

### 7.2 看内核栈（需权限）

```bash
sudo cat /proc/<PID>/stack
# 常见：futex_wait_queue_me
```

### 7.3 确认是否有僵尸测试

```bash
pgrep -af 'target/debug/deps'
# upstream 关注：discovery-、failures-、request_plane-、integration_soak-
```

### 7.4 确认是否用了旧二进制

```bash
cargo test -p pagoda-runtime --test integration_soak -- ... 2>&1 | head -5
# 若只有 "Finished ... in 0.xxs" 而无 "Compiling pagoda-runtime"，强制重编译：
touch lib/runtime/tests/integration_soak.rs && cargo test ...
```

### 7.5 运行时日志

```bash
RUST_LOG=pagoda_runtime::discovery=debug,pagoda_runtime::storage=warn \
  cargo test -p pagoda-runtime --test integration_soak \
  random_endpoint_churn -- --test-threads=1 --include-ignored --nocapture
```

关注：

- `FileStore watch channel saturated, dropping event`
- `watch downstream saturated, dropping event`

### 7.6 区分「慢」与「挂死」

| 情况 | 耗时 | 行为 |
|------|------|------|
| 正常 churn | 全套 ~87s；单测 ~30s | 有进度，最终 `ok` |
| 真挂死 | >5min 无输出 | `futex_wait`，CPU ≈ 0 |

---

## 8. 解决方案

### 8.1 方案 A — Runtime 加固（推荐，治本）

| 位置 | 改动 |
|------|------|
| `servicegroup/client.rs` | `wait_for_instances()` 每 ~200ms 轮询 `discovery.list()`，查到实例即返回 |
| `servicegroup/client.rs` | `portname_watcher` 增加周期性 `discovery.list()` 对账 |
| `discovery/kv_store.rs` | `list_and_watch` 先 `list(query)` yield 初始 `Added` 事件 |

已验证：上述三处改动后 upstream 连续 3+ 次全套 soak ~87s 通过。

### 8.2 方案 B — Storage 对齐 Dynamo（治本，改动面较大）

| 位置 | Pagoda 现状 | Dynamo 做法 |
|------|-------------|-------------|
| `storage/kv/file.rs` | `try_send`，cap=256 | `blocking_send`，cap=128 |
| `storage/kv.rs` `Manager::watch` | 新事件 `try_send` | `send_timeout(1s)`，cap=16384 |

减少事件丢失概率，从根源降低 watch 静默；**仍需**配合 Client 兜底更安全。

### 8.3 方案 C — 测试侧加固（治标，改善可观测性）

| 位置 | 改动 |
|------|------|
| `integration_soak.rs` | re-register 用 `wait_for_instances_nonempty(client, 10s)` 替代裸 `wait_for_instances()` |
| `integration_soak.rs` | 增加 `eprintln!("churn: ...")` 进度 |
| `integration_soak.rs` | `long_running` 结束 unregister；churn 结束 `drt.discovery().shutdown()` |
| `run_tests.sh` | 默认 `TEST_THREADS=1`；文档说明 upstream 清理命令 |

挂死变 **超时失败**，便于 CI 发现；不单独解决生产路径。

### 8.4 方案 D — 环境/操作（不修代码）

```bash
# 清 upstream 全部测试僵尸（不要用 integration_  alone）
pkill -9 -f 'Pagoda-upstream/target/debug/deps/' 2>/dev/null

# 单线程跑 soak
cargo test -p pagoda-runtime --test integration_soak -- \
  --test-threads=1 --include-ignored
```

可降低概率，**不能消除** race。

### 建议组合

- **短期**：D + C（能跑、能诊断）
- **长期**：A 或 A+B（生产与测试均可靠）

---

## 9. 验证清单

修复或清理环境后，以下均应通过：

```bash
# 1. 单测 churn
cargo test -p pagoda-runtime --test integration_soak \
  random_endpoint_churn -- --test-threads=1 --include-ignored

# 2. 全套 soak（7 tests）
cargo test -p pagoda-runtime --test integration_soak -- \
  --test-threads=1 --include-ignored

# 3. 连续 3 次（稳定性）
for i in 1 2 3; do
  cargo test -p pagoda-runtime --test integration_soak -- \
    --test-threads=1 --include-ignored 2>&1 | tail -1
done
# 期望：三次均为 ok. 7 passed; ... finished in ~87s
```

---

## 10. 相关文件索引

| 文件 | 角色 |
|------|------|
| `lib/runtime/tests/integration_soak.rs` | 触发用例 `random_endpoint_churn_*` |
| `lib/runtime/tests/common/contract.rs` | `start_served_endpoint` → `wait_for_instances` |
| `lib/runtime/src/servicegroup/client.rs` | **挂死直接点** `wait_for_instances`、portname watcher |
| `lib/runtime/src/discovery/kv_store.rs` | file discovery `list_and_watch` |
| `lib/runtime/src/storage/kv.rs` | Manager watch 回放与 `try_send` |
| `lib/runtime/src/storage/kv/file.rs` | inotify `try_send`、channel 256 |
| `lib/runtime/tests/run_tests.sh` | 集成测试入口、`TEST_THREADS` |

---

## 11. 参考对比（Dynamo）

| 能力 | Pagoda-upstream（当前） | Dynamo |
|------|-------------------------|--------|
| `wait_for_instances` list 兜底 | 否 | 否 |
| portname/endpoint watcher 周期 list | 否 | 否 |
| `kv_store::list_and_watch` 初始 list | 否 | 否 |
| `Manager::watch` 启动回放 | 是 | 是 |
| file 层转发 | `try_send` 易丢 | `blocking_send` 较稳 |
| Manager 下游转发 | `try_send` 易丢 | `send_timeout` 较稳 |

Dynamo soak 能通过，主要因为 **storage 更稳**，而非 client 层已修复。

---

## 12. 修订记录

| 日期 | 说明 |
|------|------|
| 2026-06-11 | 初版：基于 Pagoda-upstream 挂死调查与 Dynamo/Pagoda 对比 |
