# `transports` 模块设计文档

`transports` 模块是 Pagoda Runtime 的通信基础层，负责封装所有节点间、节点与协调服务之间的网络通信。它向上层提供统一的抽象，隐藏 etcd、NATS、ZeroMQ、TCP 四种不同传输协议的差异。

**源码位置**：`lib/runtime/src/transports/`

---

## 一、设计背景与模块职责

Pagoda 是一个分布式推理框架，运行时中存在三类通信需求：

1. **协调通信**：节点向 etcd 注册自身信息、监听集群状态变化、申请租约以实现心跳。这类通信要求强一致性，但频率低、数据量小。
2. **消息队列通信**：调度器向工作节点分发推理任务、工作节点汇报事件。这类通信要求高吞吐、持久化、支持多消费者竞争，选用 NATS JetStream 实现。
3. **流式数据通信**：推理请求的输入输出需要流式传输（尤其是 token streaming）。这类通信要求极低延迟、支持取消，选用 ZeroMQ Router/Dealer 模式实现。

`transports` 模块不包含任何业务逻辑，只做一件事：**让上层代码可以通过干净的 Rust API 使用这些协议，而无需了解底层客户端库的细节**。

---

## 二、工具函数：`build_in_runtime`

**源文件**：`utils.rs`

### 为什么需要

与 etcd、NATS 建立连接是耗时操作（TCP 握手、TLS 握手、租约申请），不能在调用方的 Tokio 运行时中直接执行，否则会阻塞事件循环。更关键的是，etcd 租约续约和 Watch 后台任务需要一个**持续存活的独立运行时**——如果这些任务跑在调用方的运行时里，调用方一旦调度繁忙或关闭，后台任务就会被影响甚至终止。

### 函数签名

```rust
pub async fn build_in_runtime<T, F>(f: F, num_threads: usize)
    -> Result<(T, Arc<tokio::runtime::Runtime>)>
where
    T: Send + Sync + 'static,
    F: Future<Output = Result<T>> + Send + 'static,
```

**参数说明**：
- `f`：需要在专用运行时中执行的初始化 Future，例如建立 etcd 连接并申请租约。
- `num_threads`：专用运行时的工作线程数。etcd 传 1（续约是轻量 IO 操作），NATS 传 4（消息处理需要更高并发）。

**返回值**：`(T, Arc<Runtime>)` —— 初始化结果和运行时的 Arc 引用。调用方持有 Arc，Arc 归零时运行时自然销毁，其上运行的所有后台任务也随之终止。

### 实现原理

函数创建专用 Tokio 运行时后，在独立 OS 线程中用 `block_on` 驱动一个 async 块。该 async 块先 `await f` 完成初始化，通过 oneshot channel 将结果传回调用方，然后执行 `std::future::pending::<()>().await` 永不退出。`pending()` 永不完成，使 `block_on` 一直阻塞在 OS 线程上，运行时因此持续存活，其中的后台任务（续约、Watch）可以正常运行。

**关键约束**：在专用运行时内绝不能 `await` 调用方（主运行时）的 Future。两个不同 Tokio 运行时之间没有 waker 协作机制，交叉 `await` 会导致两运行时相互等待，永久死锁。

---

## 三、etcd 传输子系统

etcd 子系统分布在五个文件中，每个文件承担独立职责，整体遵循"**连接、续约、锁、缓存各自独立**"的分层原则。

### 3.1 `Connector`：连接管理器

**源文件**：`etcd/connector.rs`

#### 为什么需要

etcd 是分布式系统的协调核心，任何 KV 操作、Watch 操作都依赖一个活跃的连接。网络抖动时连接会断开，如果每个业务方法都自行处理断线重连，代码会充斥重复的重试逻辑且难以维护。`Connector` 将连接管理完全收归一处：业务代码只需调用 `get_client()` 获取连接，断线时调用 `reconnect()` 等待恢复，重连细节对上层透明。

#### 结构体字段

```rust
pub struct Connector {
    client: parking_lot::RwLock<etcd_client::Client>,
    etcd_urls: Vec<String>,
    connect_options: Option<ConnectOptions>,
    backoff_state: tokio::sync::Mutex<BackoffState>,
}
```

**`client: parking_lot::RwLock<etcd_client::Client>`**

持有底层 etcd 客户端，用读写锁保护。选用 `parking_lot::RwLock`（同步锁，非 async 锁）而非 `tokio::sync::RwLock`，原因是 `get_client()` 只需 clone 一个 tonic channel 引用（引用计数加一，微秒级操作），不涉及任何 await，同步锁完全够用且开销更低。正常情况下多个 KV 操作同时持读锁并发 clone，互不阻塞；重连成功时短暂持写锁替换为新 client。

**`etcd_urls: Vec<String>`**

etcd 服务器地址列表，支持多节点（etcd 集群通常部署 3 或 5 个节点）。重连时依然使用这个列表，让 tonic 从中选择可达的节点。

**`connect_options: Option<ConnectOptions>`**

etcd 连接选项，包含 TLS 证书、用户名/密码等认证信息。`Option` 表示无认证时可为 `None`（本地开发场景）。重连时需要传入相同的 options，因此保存在这里而非只在构造时使用。

**`backoff_state: tokio::sync::Mutex<BackoffState>`**

退避状态，使用 `tokio::sync::Mutex`（async 锁）有两个作用：一是重连操作本身是 async 的（需要 await etcd 握手），必须使用 async 锁；二是利用互斥语义**串行化重连**——若多个 KV 操作同时检测到连接断开，只有第一个进入 `reconnect()` 的操作会真正执行重连，其他操作在 lock await 处等待，重连完成后它们 lock 拿到时发现连接已恢复，直接开始下一次重试，避免了多个并发重连操作同时轰炸 etcd 服务器。

#### 方法说明

**`new(etcd_urls, connect_options) -> Result<Arc<Self>>`**

构造函数，立即尝试建立连接。返回 `Arc<Self>` 而非 `Self`，因为 `Connector` 需要在 `Client`、后台任务等多处共享，`Arc` 是唯一合理的所有权模型。构造失败（etcd 不可达）直接返回 `Err`，不做静默重试——启动阶段失败应该快速暴露。

**`get_client() -> etcd_client::Client`**

获取当前 etcd 客户端的 clone。持读锁的时间等于 `Clone::clone` 的时间（微秒级），不做任何 IO。调用方拿到 clone 后可以调用 `.kv_client()`、`.watch_client()` 等方法执行具体操作。若此时连接已断开，操作会返回错误，调用方随即调用 `reconnect()` 等待恢复。

**`reconnect(deadline: Instant) -> Result<()>`**

在给定截止时间前反复尝试重建连接。流程：
1. 获取 `backoff_state` 的 async 锁（此步骤串行化所有并发重连请求）
2. 调用 `attempt_reset()` 判断是否需要重置退避（如果距上次尝试已过去足够长时间，说明这是一次全新的失败，重置退避让下次立即尝试）
3. 进入循环：先 `apply_backoff(deadline).await` 等待退避时间，再尝试 `connect()`；成功则写锁替换 `client` 字段并返回 `Ok`，失败则继续循环直到 deadline 到期

**`etcd_urls() -> &[String]`** / **`connect_options() -> &Option<ConnectOptions>`**

纯访问器，供租约模块（`lease.rs`）在需要时读取连接参数。

---

#### `BackoffState`：指数退避状态机

**为什么需要独立结构体**

退避逻辑有多个参数和状态，如果散落在 `reconnect` 函数中会使该函数变得臃肿，也无法方便地独立测试。抽成 `BackoffState` 后，`reconnect` 只关心重连时序，退避计算完全委托给 `BackoffState`。

**字段说明**

```rust
struct BackoffState {
    initial_backoff: Duration,       // 500ms，第一次重试失败后的等待时间
    min_backoff: Duration,           // 50ms，退避时间的下限（避免过于激进）
    max_backoff: Duration,           // 5s，退避时间的上限（避免等待过久）
    current_backoff: Duration,       // 当前应用的退避值，从 ZERO 开始
    last_connect_attempt: Instant,   // 上次发起连接尝试的时刻
}
```

- `initial_backoff`：500ms，第一次重试失败后等 500ms 再试。设计上允许 etcd 短暂不可达（如滚动重启）后快速恢复。
- `min_backoff`：50ms，即便计算出的退避值很小，也至少等 50ms，防止在 deadline 临近时过于密集地轰炸 etcd。
- `max_backoff`：5s，退避上限，防止指数增长后等待时间过长（etcd 恢复后希望尽快重连）。
- `current_backoff`：动态变化，初始为 `Duration::ZERO`（第一次立即尝试），每次 `apply_backoff` 后翻倍。
- `last_connect_attempt`：记录上次尝试时刻，`attempt_reset` 用它判断是否应该重置退避。

**`attempt_reset(&mut self)`**

如果 `now > last_connect_attempt + current_backoff`，说明距上次尝试已过去超过当前退避时长，即进程长时间正常运行后偶发断线——此时应将 `current_backoff` 重置为 0，下次立即重试而不是按上次断线时积累的退避值等待。

**`apply_backoff(&mut self, deadline: Instant) -> (async)`**

计算本次实际等待时间：取 `current_backoff`、`deadline 剩余时间 / 2`、`max_backoff` 三者的最小值，再取与 `min_backoff` 的最大值，然后 `sleep(backoff).await`。将退避时间限制在"剩余时间的一半"是为了确保在 deadline 前还有机会做至少一次真正的重连尝试，而不是把时间全耗在等待上。等待后将 `current_backoff` 翻倍（为下次 `apply_backoff` 准备），并更新 `last_connect_attempt`。

**`Default` 实现**

```rust
impl Default for BackoffState {
    fn default() -> Self
    // 使用 500ms / 50ms / 5s 作为默认退避参数；
    // `current_backoff` 从 `Duration::ZERO` 开始，表示第一次重连可立即尝试；
    // `last_connect_attempt` 初始化为 `Instant::now()`，作为后续 `attempt_reset()` 的时间基线
}
```

将这些常量集中在 `Default` 中，而不是分散写在 `Connector::new` 或 `reconnect()` 里，有两个好处：一是所有新建 `BackoffState` 的路径都能得到一致的初始行为；二是后续若要统一调整重连策略，例如把最大退避从 5 秒改成 10 秒，只需修改一个地方。`current_backoff = Duration::ZERO` 也表明第一次重连失败后的行为是“先立即试一次，再进入指数退避”，而不是启动后就无条件等待 500ms。
---

### 3.2 `lease.rs`：租约与心跳

**源文件**：`etcd/lease.rs`

#### 为什么需要

etcd 租约（Lease）是 Pagoda 服务发现的核心机制：每个节点启动时申请一个 TTL 为 10s 的租约，将自身的注册信息（地址、推理能力）以 KV 形式绑定到该租约上。节点进程正常退出或崩溃后，etcd 服务器检测到租约 TTL 到期，自动删除所有绑定的键，其他节点通过 Watch 感知到 Delete 事件，从而从服务列表中移除该节点。这套机制实现了**无需显式注销的服务发现**——节点活着，键就存在；节点消失，键自动消失。

但租约必须持续续约（Keep-Alive），否则 10s 后 etcd 认为进程已宕机。续约是个周期性 IO 操作，必须在后台持续运行，不能阻塞业务代码。

#### `create_lease(connector, ttl, token) -> Result<u64>`（公开入口）

这是 `lease.rs` 唯一的公开函数，是整个租约子系统的入口。

**参数**：
- `connector`：用于与 etcd 通信，申请租约和后续续约都通过它。
- `ttl`：租约 TTL（秒），传入的是 10，实际上 etcd 可能返回一个略有调整的实际 TTL。
- `token`：Runtime 的主 CancellationToken，用于在续约失败时触发整个 Runtime 关闭。

**流程**：
1. 调用 etcd lease grant API 申请租约，获得 `lease_id`（一个 i64）
2. 从 `token` 派生出一个子 token 传给后台续约任务（子 token 可以被单独取消而不影响父 token）
3. `tokio::spawn` 后台续约任务 `keep_alive`；注意 spawn 闭包捕获的是**父 `token`**，若续约任务失败，调用父 token 的 `cancel()`，触发整个 Runtime 优雅关闭
4. 返回 `lease_id` 供调用方绑定 KV 键使用

**设计意图**：续约是"租约存活"的基础设施，不应该要求调用方手动管理。函数申请完租约就立即 spawn 续约任务并返回，让调用方无感知地使用 lease ID。

#### `keep_alive(connector, lease_id, ttl, token)`（私有）

外层循环，负责在 KeepAlive 双向流断开后重建。每次循环调用 `new_keep_alive_stream` 建立新流，再把流交给 `keep_alive_with_stream` 处理。`keep_alive_with_stream` 返回 `Ok(true)` 表示流断开需要重连，返回 `Ok(false)` 表示正常取消，返回 `Err` 表示租约已过期不可恢复。

初始 `deadline` 设为 `Instant::now() + ttl`，此后每次收到服务端续约响应时，根据服务端返回的新 TTL 重新设置 deadline，确保 deadline 始终反映最新的租约到期时间。

#### `new_keep_alive_stream(connector, lease_id, deadline, token) -> Result<Option<(LeaseKeeper, LeaseKeepAliveStream)>>`（私有）

负责建立 KeepAlive 双向流。尝试调用 `lease_client.keep_alive(lease_id)` 建立流：
- 成功：返回 `Ok(Some((sender, receiver)))`，`sender` 用于发送心跳，`receiver` 用于接收服务端响应
- 失败：尝试重连 etcd，重连成功后继续重试建立流；若在重连 await 期间收到取消信号，返回 `Ok(None)` 表示正常停止；若 deadline 超时，返回 `Err`

#### `keep_alive_with_stream(connector, sender, receiver, lease_id, deadline, token) -> Result<bool>`（私有）

核心续约循环，用 `select! biased` 同时监听三个事件源，`biased` 关键字表示按声明顺序优先级处理（非随机选择）：

**臂1（最高优先级）：服务端响应 `receiver.message()`**

服务端对每次心跳的响应，包含新的 TTL。收到响应后更新 `*deadline = Instant::now() + new_ttl`，延长本地对租约到期时间的估计。若服务端返回 TTL ≤ 0，表示租约已在服务端过期（可能是心跳太久没发到），此时调用方持有的 lease ID 已失效，所有绑定的 KV 键已被删除，必须 `bail!` 报错让上层处理（通常是触发 Runtime 关闭重启）。若流正常结束（`Ok(None)`）或返回错误，说明连接断开，返回 `Ok(true)` 触发外层的重连循环。

**臂2（中优先级）：取消信号 `token.cancelled()`**

收到取消信号表示 Runtime 正在关闭。此时主动向 etcd 调用 `lease_client.revoke(lease_id)` 撤销租约，立即释放所有绑定的键，让其他节点尽快感知到本节点下线（而不是等 TTL 自然过期）。若 revoke 失败（etcd 已不可达），记录 warn 日志但不报错，因为进程已在关闭流程中。然后返回 `Ok(false)` 正常停止。

**臂3（最低优先级）：心跳定时器 `sleep(next_renewal)`**

`next_renewal = (deadline - now) / 2`：在租约剩余时间的一半时发送心跳，而非固定间隔。这样做的好处是：服务端返回的 TTL 可能变化（etcd 服务器负载高时可能调小 TTL），以剩余时间的动态一半为间隔，既不会让心跳过于频繁，又确保在 deadline 前有足够时间发送和接收续约响应。心跳发送失败只记录 warn，不立即报错，因为下次心跳前可能网络就恢复了。

---

### 3.3 `lock.rs`：分布式读写锁

**源文件**：`etcd/lock.rs`

#### 为什么需要

Pagoda 中某些操作需要跨进程互斥，典型场景是**集群状态快照**：快照操作需要读取并持久化整个集群的 KV 状态，期间不允许有并发写入，否则快照状态不一致。普通内存锁（`std::sync::Mutex`）只在单进程内有效，跨进程需要借助外部协调服务。etcd 的原子事务（CAS 操作）天然适合实现跨进程锁：写锁键存在 = 有进程持锁，键不存在 = 无锁，事务保证检查和创建的原子性。

#### `DistributedRWLock`

```rust
#[derive(Clone)]
pub struct DistributedRWLock {
    lock_prefix: String,
}
```

**`lock_prefix: String`**

etcd 中用于表示锁状态的键前缀。写锁键为 `v1/{prefix}/writer`，读锁键为 `v1/{prefix}/readers/{reader_id}`。`DistributedRWLock` 只持有这个字符串，**不持有 etcd 连接**，etcd 连接在调用锁方法时以 `&Client` 参数传入。

这种设计使 `DistributedRWLock` 成为一个轻量的无状态对象，可以自由 `Clone` 传递。锁的真实状态在 etcd 中，任何知道 `lock_prefix` 的进程都可以参与同一个锁协议。

**`new(lock_prefix: String) -> Self`**

构造函数，不做任何 IO。

**`try_write_lock<'a>(&'a self, etcd_client: &'a Client) -> Option<WriteLockGuard<'a>>`**

尝试获取写锁，**非阻塞**：立即返回 `Some(guard)` 或 `None`，不等待。调用方若需要阻塞等待写锁，需自行循环重试。

流程两步：
1. **原子 CAS**：构造 etcd 事务 `WHEN version(writer_key) == 0 THEN put(writer_key, "writing", lease)`。事务成功表示写锁键成功创建（原本不存在），事务失败表示已有进程持有写锁。
2. **检查读者**：即便 CAS 成功，仍需检查 `v1/{prefix}/readers/` 前缀下是否有存活的读锁键（即是否有进程正在读取）。若有读者，将刚创建的 writer 键删除（回滚），返回 `None`。若检查读者时出错，同样回滚并返回 `None`。

两步之间存在**亚毫秒级竞争窗口**：CAS 成功后检查读者前，可能有新读者通过事务创建读锁键（此时 writer 键已存在，读者事务会失败，所以这个窗口实际上不会造成真正的并发问题）。代码注释说明这个窗口在快照场景下可以接受。

**`read_lock_with_wait<'a>(&'a self, etcd_client: &'a Client, reader_id: &str, timeout: Option<Duration>) -> Result<ReadLockGuard<'a>>`**

阻塞获取读锁，轮询等待直到成功或超时（默认 30s，由 `DEFAULT_READ_LOCK_TIMEOUT_SECS = 30` 控制）。

`reader_id` 是调用方自己提供的唯一标识符，作为读锁键的后缀（`v1/{prefix}/readers/{reader_id}`），用于区分多个读者。

循环中每次构造事务：`WHEN version(writer_key) == 0 THEN put(reader_key, "reading", lease)`。这个事务将"检查写锁不存在"和"创建读锁键"**原子合并**，消除了先检查后创建之间的竞争窗口——这与写锁的两步操作形成鲜明对比，读锁做到了真正的原子性。事务失败（有写者）则等待 100ms 后重试。

---

#### `WriteLockGuard<'a>` 与 `ReadLockGuard<'a>`

```rust
pub struct WriteLockGuard<'a> {
    rwlock: &'a DistributedRWLock,
    etcd_client: &'a Client,
}

pub struct ReadLockGuard<'a> {
    rwlock: &'a DistributedRWLock,
    etcd_client: &'a Client,
    reader_id: String,
}
```

**`rwlock: &'a DistributedRWLock`**

对锁对象的引用，`drop` 时需要知道 `lock_prefix` 来构造要删除的键名。生命周期 `'a` 确保 guard 存活期间锁对象不会被销毁。

**`etcd_client: &'a Client`**

对 etcd 客户端的引用，`drop` 时需要它来执行删除操作。生命周期 `'a` 确保 guard 存活期间 etcd 客户端不会被销毁。

**`reader_id: String`**（仅 `ReadLockGuard`）

持有读锁时注册的 `reader_id`，`drop` 时用于构造要删除的读锁键（`v1/{prefix}/readers/{reader_id}`）。写锁没有这个字段，因为写锁键是固定的（`v1/{prefix}/writer`）。

**`impl Drop`**

两种 guard 的 `drop` 实现逻辑相同：调用 `tokio::runtime::Handle::try_current()` 获取当前 Tokio 运行时句柄，然后 `handle.spawn(async move { etcd_client.kv_delete(lock_key).await })` 删除对应的 etcd 锁键。

`try_current()` 可能失败——当 guard 在 tokio 运行时以外的上下文（如普通同步线程）中 drop 时。此时无法 spawn async 任务，只能记录 error 日志：`"XXLockGuard dropped outside tokio runtime - lock not released! Lock will be cleaned up when etcd lease expires."`。这种情况下依赖租约到期机制兜底：锁键绑定到进程租约，进程消失后锁键自动删除。

---

### 3.4 `etcd::Client`：业务操作入口

**源文件**：`etcd.rs`

#### 为什么需要

`Connector` 封装连接管理，`lease.rs` 封装续约，但上层代码操作 etcd 时不应关心这些底层细节，只需要一个"能做 KV 操作"的客户端。`Client` 作为门面（Facade），将连接、租约、KV 操作整合成一个干净的接口。

#### 结构体字段

```rust
#[derive(Clone)]
pub struct Client {
    connector: Arc<Connector>,
    primary_lease: u64,
    runtime: Runtime,
    rt: Arc<tokio::runtime::Runtime>,
}
```

**`Debug` 实现**

```rust
impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    // 仅输出 `primary_lease`，避免把连接句柄、专用 runtime 和内部状态细节灌入日志
}
```

这里使用手写 `Debug`，而不是直接派生，是因为 `Client` 持有的 `Connector`、`Runtime` 和专用 Tokio runtime 都属于“体积大但定位价值有限”的内部状态。把这些字段完整打印出来既噪声大，也不利于稳定比对日志；相反，`primary_lease` 是最有排障价值的业务标识，能帮助开发者快速判断某条日志对应的是哪一个 etcd 会话与租约实例。


**`connector: Arc<Connector>`**

共享的连接管理器。`Arc` 允许 `Client` 廉价 Clone——多个 `Client` 副本共享同一个底层连接，重连操作天然对所有副本生效。

**`primary_lease: u64`**

本进程的主租约 ID，由 `create_lease` 申请。所有需要跟随进程生命周期的 KV 键（如服务注册信息）都绑定到这个租约。KV 操作方法中 `lease_id: Option<u64>` 参数为 `None` 时默认使用此字段。进程退出（正常或崩溃）后，`primary_lease` 过期，绑定的键自动删除。

**`runtime: Runtime`**

Pagoda Runtime 句柄，存放在这里主要是为了在 `Client::new` 中获取 `runtime.primary_token()`，用于传给 `create_lease`：租约续约失败时取消 primary token，触发 Runtime 关闭。

**`rt: Arc<tokio::runtime::Runtime>`**

Watch 后台任务的运行时（由 `build_in_runtime` 创建的 1 线程运行时）。`watch_internal` 调用 `self.rt.spawn` 将 Watch 后台任务放在这个运行时上，而不是调用方的运行时。这样即使调用方的运行时关闭，Watch 任务仍然存活（直到 `PrefixWatcher` 的 `rx` 端被 drop）。

#### `Client::builder() -> ClientOptionsBuilder`

工厂方法，返回构建器，支持链式调用设置 `etcd_url`、`attach_lease` 等选项，然后调用 `.build()?` 得到 `ClientOptions`，再调用 `.new(options, runtime).await` 创建 `Client`。

#### `Client::new(config: ClientOptions, runtime: Runtime) -> Result<Self>`

真正的构造函数。通过 `build_in_runtime(..., 1)` 在专用运行时中完成两步：先通过 `Connector::new` 建立连接，再通过 `create_lease` 申请主租约。整个过程在 1 线程专用运行时中运行，该运行时随后被保存在 `rt` 字段中持续运行后台续约任务。

#### KV 操作方法

**`lease_id() -> u64`**

返回 `primary_lease` 字段。供需要显式指定 lease ID 的上层代码使用。

**`kv_put(key, value, lease_id) -> Result<()>`**

无条件覆盖写。`lease_id` 为 `None` 时使用 `primary_lease`，写入的键绑定到进程租约，随进程生命周期管理。

**`kv_put_with_options(key, value, options) -> Result<PutResponse>`**

带完整选项的写操作，返回 `PutResponse`（含 `prev_key` 字段，即写入前的旧值）。供需要读-改-写语义的上层代码使用，如 Storage 的 update 操作需要确认旧值再决定如何处理。

**`kv_get(key, options) -> Result<Vec<KeyValue>>`**

获取单个或多个键。`options` 可传 `None`（精确匹配）或 `GetOptions::new().with_prefix()`（前缀匹配）等。返回 `Vec<KeyValue>` 而非 `Option<KeyValue>`，因为 etcd get 操作在使用前缀等选项时可能返回多个结果。

**`kv_get_prefix(prefix) -> Result<Vec<KeyValue>>`**

`kv_get` 的便捷封装，固定使用 `GetOptions::new().with_prefix()`。调用方无需手动构造 `GetOptions`。

**`kv_delete(key, options) -> Result<u64>`**

删除键，返回实际删除的键数量。`options` 可传 `None`（精确删除）或带前缀等选项。返回数量供调用方验证删除是否符合预期（例如期望删除 1 个但实际删除了 0 个说明键不存在）。

**`kv_create(key, value, lease_id) -> Result<Option<u64>>`**

幂等原子创建，是 `kv_put` 的增强版，用于服务注册等需要"首次创建"语义的场景。

底层使用 etcd 事务：`WHEN version(key) == 0 THEN put(key, value) ELSE get(key)`。

- 事务成功（键不存在，成功创建）→ 返回 `Ok(None)`，表示"新建"
- 事务失败（键已存在，进入 ELSE 分支获取现有版本号）→ 返回 `Ok(Some(version))`，表示"键已存在"
- etcd 操作出错 → 返回 `Err`

早期版本键存在时返回 `Err`，导致并发注册同一服务键时第二个进程误认为注册失败（PR #4212 修复）。现在调用方可以区分"注册成功"和"键已存在（可能是自己之前的注册或并发进程的注册）"，而不会把正常的"键已存在"误判为错误。

**`kv_create_or_validate(key, value, lease_id) -> Result<()>`**

比 `kv_create` 更严格：若键已存在，额外验证其值是否与传入的 `value` 一致。使用嵌套事务实现：外层检查键是否存在，内层（ELSE 分支）检查已有值是否与期望值相等。若值不一致返回 `Err`，用于需要确保集群中同一键只有一个权威值的场景（例如全局配置注册）。

这一组方法共同构成 `Client` 的“前缀监听入口”。它们不直接把 etcd 的 `WatchStream` 暴露给上层，而是先补齐快照、revision 续接、断线重连和事件转发，再统一交给 `PrefixWatcher` 消费。

**`kv_watch_prefix(prefix) -> Result<PrefixWatcher>`**

只监听“从现在开始”的增量变化。它只是 `watch_internal(prefix, false)` 的公开包装，不会先把已有键值回放给调用方，适合只关心未来变更的场景。

**`kv_get_and_watch_prefix(prefix) -> Result<PrefixWatcher>`**

在建立 Watch 之前，先把当前前缀下已经存在的键值作为初始快照补齐，再继续监听后续增量事件。对调用方来说，返回的 `PrefixWatcher` 会先吐出一批 `WatchEvent::Put`，随后无缝切换到实时 Watch 流，适合构建本地缓存或服务发现视图。

**`watch_internal(prefix, include_existing) -> Result<PrefixWatcher>`**

这是两种公开 API 的共享实现，负责把“快照 + 增量 Watch”拼成一个连续的数据流。执行步骤如下：

1. 调用 `get_start_revision`，通过一次前缀 GET 取回当前 revision，并在需要时顺手带回已有键值。
2. 按“已有键数量 + 额外余量”创建 `mpsc` channel，避免在返回 `PrefixWatcher` 之前预装快照事件时把发送端自己堵死。
3. 若 `include_existing = true`，先把现有键值逐条转成 `WatchEvent::Put` 写入 channel，让调用方一拿到 watcher 就能立刻消费到完整初始状态。
4. 在 `self.rt` 持有的专用 Tokio runtime 上启动后台任务，循环执行“建立 watch 流 -> 监控 watch 流 -> 视情况重连”的流程；专用 runtime 的意义在于，即便调用方自己的运行时很忙或已经准备退出，etcd Watch 的后台任务仍能独立存活。
5. 最终返回 `PrefixWatcher { prefix, rx }`，把底层流细节完全收口在内部。

**`get_start_revision(prefix, include_existing) -> Result<(i64, Option<Vec<KeyValue>>)>`**

这个辅助方法解决的是“从哪个 revision 开始续接 Watch”以及“要不要带初始快照”两个问题。它先执行一次带前缀的 GET，从响应头里提取当前 revision，再把本地起始位点设置为 `revision + 1`，确保后续 Watch 只接收 GET 之后的新变化；若响应头缺失，则直接报错，因为此时无法保证 Watch 续接不丢事件。`include_existing = true` 时，它还会把 GET 返回的键值列表一并交回给 `watch_internal`，作为初始化阶段的快照来源。

**`new_watch_stream(connector, prefix, start_revision) -> Result<WatchStream>`**

负责建立真正的 etcd Watch 流，并把重连细节封装掉。建立 Watch 时固定附带三项语义：

- `with_prefix()`：监听整个前缀，而不是单个键；
- `with_start_revision(start_revision)`：从上次确认过的 revision 之后继续接；
- `with_prev_key()`：让删除事件也能携带旧值，便于上层识别到底是哪一个实例或哪一条注册信息被删掉。

若第一次建流失败，它不会立刻放弃，而是先要求 `Connector` 在 10 秒窗口内尝试重连 etcd；只有重连也失败时，才把错误上抛给外层结束整个 watcher。

**`monitor_watch_stream(watch_stream, prefix, start_revision, tx) -> bool`**

这个循环只做两件事：消费 etcd 返回的 watch 响应，以及观察下游接收方是否已经消失。每收到一批响应，它都会先从响应头中读取最新 revision，并把 `start_revision` 推进到 `header.revision + 1`，这样一旦网络中断，外层就能从“最后成功处理的位置之后”重新建流；随后再把这一批事件交给 `process_watch_events` 转发到 channel。

返回值里的 `bool` 用来表达错误的可恢复性：网络错误、流意外关闭等情况返回 `true`，表示外层应该重连；响应头缺失、channel 转发失败、或者接收方已经全部 drop 等情况返回 `false`，表示这个 watcher 已经没有继续运行的价值，应当停止后台任务。

**`process_watch_events(events, tx) -> Result<()>`**

这是 etcd 事件到 `WatchEvent` 的最后一道适配层。它会跳过不带 `KeyValue` 的事件，只保留真正可被上层消费的条目；`Put` 事件转成 `WatchEvent::Put`，`Delete` 事件转成 `WatchEvent::Delete`。一旦 channel 发送失败，就说明 watcher 的消费侧已经不可用，此时立即返回错误，让 `monitor_watch_stream` 停止继续推进。这样可以避免后台任务在无人消费时还持续空转。


**`lock(key, lease_id) -> Result<LockResponse>`** / **`unlock(lock_key) -> Result<()>`**

直接使用 etcd 原生 Lock/Unlock API（与 `DistributedRWLock` 不同，这是 etcd 内置的互斥锁）。适用于只需要简单互斥（无读写分离）的场景。

---

### 3.5 `ClientOptions`：etcd 连接配置

```rust
#[derive(Debug, Clone, Builder, Validate)]
pub struct ClientOptions {
    #[validate(length(min = 1))]
    pub etcd_url: Vec<String>,
    pub etcd_connect_options: Option<ConnectOptions>,
    #[builder(default = "true")]
    pub attach_lease: bool,
}
```

**`etcd_url: Vec<String>`**

etcd 服务器地址列表，至少一个（`validate(length(min = 1))`）。默认情况下由 `default_servers()` 负责装配：先读取 `ETCD_PORTNAMES` 环境变量并按逗号拆分，未设置时再回退到 `["http://localhost:2379"]`（标准 etcd 本地端口）。

**`etcd_connect_options: Option<ConnectOptions>`**

连接选项，封装了认证信息。`Default` 实现按优先级读取环境变量：优先用户名/密码（`ETCD_AUTH_USERNAME`/`ETCD_AUTH_PASSWORD`），其次 TLS 证书（`ETCD_AUTH_CA`/`ETCD_AUTH_CLIENT_CERT`/`ETCD_AUTH_CLIENT_KEY`），均未配置则为 `None`（无认证，适合本地开发）。

**`attach_lease: bool`**

控制是否在连接建立后申请主租约。生产环境中几乎总是 `true`（默认值）；测试或只需读取 etcd 数据而不注册自身的场景可以设为 `false`，避免不必要的租约开销。

**`Default` 实现**

`ClientOptions::default()` 会同时决定“去哪里连”和“用什么方式认证”。它先把 `connect_options` 初始化为 `None`，随后按优先级检查认证环境变量：如果同时存在 `ETCD_AUTH_USERNAME` 与 `ETCD_AUTH_PASSWORD`，就生成用户名密码认证；否则再尝试读取 `ETCD_AUTH_CA`、`ETCD_AUTH_CLIENT_CERT`、`ETCD_AUTH_CLIENT_KEY` 三元组，组装 TLS 证书配置。两类认证都缺失时，默认保持无认证连接，方便本地开发或测试环境直接接入。

在返回的默认配置中，`etcd_url` 总是通过 `default_servers()` 生成，`etcd_connect_options` 则复用上面解析出的认证结果，而 `attach_lease` 固定为 `true`。这说明 etcd 客户端的默认姿态不是“只读连接器”，而是面向 Runtime 正常启动路径的“可注册、可续约”的完整客户端。

**`default_servers() -> Vec<String>`**

这个辅助函数只负责装配 etcd 地址列表，不关心认证与租约策略。它优先读取 `ETCD_PORTNAMES`，并将逗号分隔的地址串拆成 `Vec<String>`，从而支持一次注入多个 etcd 节点地址；若环境变量不存在，则回退到单节点本地地址 `http://localhost:2379`。这样一来，开发环境几乎零配置即可启动，而生产环境也能通过环境变量直接切换到多节点集群。
---

### 3.6 `PrefixWatcher`：前缀变化监听器

**为什么需要**

etcd Watch API 原始接口返回 `WatchStream`，调用方需要自己处理：流断开后重连、按 revision 续接（不遗漏断连期间的事件）、解析事件类型。这些是通用逻辑，与业务无关。`PrefixWatcher` 将其封装，对外只暴露一个 `mpsc::Receiver<WatchEvent>`，调用方无需关心任何底层细节。

```rust
#[derive(Dissolve)]
pub struct PrefixWatcher {
    prefix: String,
    rx: mpsc::Receiver<WatchEvent>,
}
```

**`prefix: String`**

被监听的 etcd 键前缀，仅用于日志记录（标识这个 watcher 在监听什么），不参与任何业务逻辑。

**`rx: mpsc::Receiver<WatchEvent>`**

调用方从中接收变化事件的通道接收端。后台 watch 任务持有对应的 `tx` 发送端，将 etcd 推送的事件转发进来。当调用方 drop `rx` 时，后台任务的 `tx.closed()` 触发，任务自动退出，无需显式关闭。

**`#[derive(Dissolve)]`** 生成 `.dissolve()` 方法，将结构体解构为 `(String, mpsc::Receiver<WatchEvent>)`，供 `KvCache` 内部使用（直接取出 `rx` 而不持有整个 `PrefixWatcher`）。

#### `WatchEvent` 枚举

```rust
pub enum WatchEvent {
    Put(KeyValue),
    Delete(KeyValue),
}
```

`Put` 对应键的创建或更新，`Delete` 对应键的删除。`Delete` 变体中的 `KeyValue` 包含被删除键的**旧值**（需要在建立 watch 时指定 `WatchOptions::with_prev_key()`，已在 `new_watch_stream` 中处理），这对于需要知道"谁被删除了"的场景（如服务注销）至关重要。

#### Watch 如何建立（`watch_internal`，私有）

这是 `kv_watch_prefix`（只看未来变化）和 `kv_get_and_watch_prefix`（先获取现有状态再看未来变化）的共同实现，通过 `include_existing: bool` 参数区分。

建立过程确保快照与事件流无缝衔接：
1. 执行前缀 GET，同时读取 etcd `revision` 值 R
2. `start_revision = R + 1`：Watch 从 R 之后的变化开始
3. 若 `include_existing`，将 GET 结果作为初始 `Put` 事件发送到 channel
4. 在 `self.rt` 上 spawn 后台任务，循环调用 `new_watch_stream(start_revision)` 建立流，再用 `monitor_watch_stream` 消费流

这样 R 之前（含）的状态由步骤 3 的 GET 结果覆盖，R 之后的变化由 Watch 事件覆盖，完全没有遗漏也没有重复。

#### Watch 后台任务（`monitor_watch_stream`，私有静态）

消费 `WatchStream`，在接收到每个事件批次时：
- 更新 `start_revision = response.header.revision + 1`（记录最新处理位置）
- 调用 `process_watch_events` 将事件逐条发送到 `mpsc::Sender<WatchEvent>`
- 若 `tx` 已关闭（接收方 drop 了 `rx`），返回 `false` 停止任务
- 若流出现错误或意外关闭，返回 `true` 触发外层重连

重连时以最新的 `start_revision` 重建 Watch 流（`new_watch_stream`），etcd 服务器从该 revision 开始重放历史事件，确保断连期间的事件不丢失。




---

### 3.7 `KvCache`：本地键值缓存

**为什么需要**

服务发现场景中，同一组键（如所有工作节点的地址）需要被频繁读取，但写入（节点上下线）相对罕见。如果每次读取都查询 etcd，不仅延迟高（网络 RTT），还会给 etcd 增加不必要的负担。`KvCache` 在本地维护一份 etcd 前缀数据的副本，读取操作完全在本地完成（纳秒级），写入操作同时更新 etcd 和本地缓存，通过 Watch 保持最终一致性。

```rust
pub struct KvCache {
    client: Client,
    pub prefix: String,
    cache: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    watcher: Option<PrefixWatcher>,
}
```

**`client: Client`**

etcd 客户端，用于执行写操作（`put`、`delete`）和初始化时的 GET 操作。读操作（`get`、`get_all`）不使用这个字段，直接读本地缓存。

**`prefix: String`**（`pub`）

被缓存的 etcd 键前缀，标为 `pub` 是因为上层代码有时需要知道完整键名（前缀 + 相对键名）。

**`cache: Arc<RwLock<HashMap<String, Vec<u8>>>>`**

本地缓存，键是完整 etcd 路径（含前缀），值是原始字节。`tokio::sync::RwLock` 允许多个 `get`/`get_all` 并发读取；Watch 后台任务更新时持写锁，时间极短（仅 HashMap 插入/删除操作）。`Arc` 使缓存可以被 Watch 后台任务和 `KvCache` 实例共同引用，是跨任务共享的标准模式。

**`watcher: Option<PrefixWatcher>`**

构造完成后此字段被 `take()` 置为 `None`，其内部的 `rx` 被移入后台任务。`Option` 只是构造过程中的中间状态，设计上这个字段在 `KvCache` 正常运行时始终为 `None`。

#### `new(client, prefix, initial_values) -> Result<Self>`

构造时执行四步初始化：
1. **拉取现有数据**：`kv_get_prefix` 获取 etcd 中已有的键值，填充本地 cache。这确保 `KvCache` 从一致的初始状态开始。
2. **写入初始值**：遍历 `initial_values`，对 cache 中缺失的键调用 `kv_put` 写入 etcd，同时插入本地 cache。仅写缺失的键，不覆盖已有键（避免并发进程互相覆盖）。
3. **建立 Watch**：调用 `kv_get_and_watch_prefix` 建立带初始快照的 watcher（初始快照与步骤 1 的数据时间点一致，不会重复，因为步骤 1 和 watch 都用同一 revision 作为分界）。
4. **Spawn 后台任务**：调用 `start_watcher()` 将 Watch 事件流接入本地 cache 更新逻辑。

#### `start_watcher(&mut self) -> Result<()>`（私有）

从 `self.watcher` 取出 `PrefixWatcher`（`take()` 置为 `None`），解构出 `rx`，clone `self.cache` 的 `Arc`，spawn 后台任务：持续从 `rx.recv()` 接收 `WatchEvent`，`Put` 事件更新 cache，`Delete` 事件从 cache 删除对应键。任务在 `rx` 关闭时（即 `KvCache` 被 drop，导致后台任务持有的 `cache Arc` 释放）自动退出。

#### `get(key: &str) -> Option<Vec<u8>>`

读本地缓存，`key` 是相对键名（不含前缀），函数内部拼接 `format!("{}{}", self.prefix, key)` 后查 HashMap。返回 `Option<Vec<u8>>`，`None` 表示键不存在。

#### `get_all() -> HashMap<String, Vec<u8>>`

返回本地缓存的完整 clone。返回的 HashMap 中键是**完整 etcd 路径**（含前缀）。调用方若需要相对键名，需自行去掉前缀。

#### `put(key: &str, value: Vec<u8>, lease_id: Option<u64>) -> Result<()>`

先 `kv_put` 写 etcd，成功后再更新本地 cache。顺序很重要：etcd 是权威数据源，写 etcd 失败则 `?` 提前返回，本地 cache 不更新，保持与 etcd 一致。

#### `delete(key: &str) -> Result<()>`

先 `kv_delete` 删 etcd，成功后再从本地 cache 删除。理由同 `put`。

---

## 四、NATS 传输子系统

**源文件**：`nats.rs`

### 4.1 设计背景

NATS JetStream 承担 Pagoda 的任务分发职责：调度器将推理任务发布到 JetStream 流，工作节点消费任务并执行推理，执行结果通过另一个 NATS 主题回传。

选择 NATS JetStream 而非 etcd 做任务队列，原因是：etcd 不是消息队列，大量消息写入会给 etcd Raft 协议带来不必要的压力；NATS JetStream 原生支持消费者竞争（多 Worker 自动负载均衡）、消息持久化（Worker 崩溃重启后任务不丢失）、消息确认（ACK）。

**常量**：
- `URL_PREFIX: &str = "nats://"` — NATS URL 的标准前缀，用于格式校验和解析
- `NATS_WORKER_THREADS: usize = 4` — NATS 专用运行时的线程数，高于 etcd 的 1 线程，因为消息处理需要更多并发能力

---

### 4.2 `NatsAuth`：认证策略枚举

**为什么需要**

不同部署环境使用不同的 NATS 认证方式：开发环境用用户名密码，生产环境用证书文件或 NKey。用枚举统一表示，`Default` 实现按优先级自动从环境变量中选择，避免调用方针对不同环境写不同的初始化代码。

```rust
pub enum NatsAuth {
    UserPass(String, String),  // 用户名, 密码
    Token(String),             // Bearer token
    NKey(String),              // Ed25519 公钥认证
    CredentialsFile(PathBuf),  // .creds 文件路径（含公钥+私钥）
}
```

**`Default` 实现的优先级**：
1. `NATS_AUTH_USERNAME` + `NATS_AUTH_PASSWORD` 均存在 → `UserPass`
2. `NATS_AUTH_TOKEN` 存在 → `Token`
3. `NATS_AUTH_NKEY` 存在 → `NKey`
4. `NATS_AUTH_CREDENTIALS_FILE` 存在 → `CredentialsFile`
5. 均无 → `UserPass("user", "user")`（匹配默认 docker-compose 配置，仅用于本地开发）

**手写 `Debug` 实现**：`UserPass` 只打印用户名不打印密码（`<redacted>`），`Token` 和 `NKey` 整体 `<redacted>`，`CredentialsFile` 打印路径（路径本身不敏感）。使用派生 `#[derive(Debug)]` 会把密码、token 等完整打印到日志，存在安全风险。




---

### 4.3 `ClientOptions`：NATS 连接配置

```rust
#[derive(Debug, Clone, Builder, Validate)]
pub struct ClientOptions {
    #[validate(custom(function = "validate_nats_server"))]
    server: String,
    auth: NatsAuth,
}
```

**`server: String`**

NATS 服务器地址，格式为 `nats://host:port`。`Default` 读取 `NATS_SERVER` 环境变量，未设置则回退到 `"nats://localhost:4222"`（标准 NATS 端口）。`validate_nats_server` 校验函数确保地址以 `"nats://"` 开头，在 `connect()` 前捕获配置错误，避免难以排查的运行时失败。

这里实际上对应两个很小但很关键的辅助逻辑。`default_server()` 负责提供默认地址来源：优先读取 `NATS_SERVER`，若环境变量不存在，就回退到本地开发默认值 `nats://localhost:4222`。这让 `ClientOptions::default()` 在开发环境下可以零配置启动，同时又保留了通过环境变量切换远端 NATS 集群的能力。

`validate_nats_server(server)` 则承担最前置的格式兜底，只检查一件事：地址是否以 `nats://` 开头。这个校验并不尝试替代完整的 URL 解析，而是用一个足够便宜、足够明确的规则，把最常见的配置错误提前拦在构建阶段，避免调用方把裸 `host:port` 或其他协议地址带到真正的连接流程里才暴露问题。

**`auth: NatsAuth`**

认证策略，`Default` 为自动从环境变量选择（见 `NatsAuth::default()`）。

**`Default` 实现**

`ClientOptions::default()` 的默认姿态可以概括为“本地可直接跑、生产可由环境接管”。`server` 字段来自 `default_server()`，`auth` 字段来自 `NatsAuth::default()` 自动判定的认证策略，因此调用方即使完全不手动拼装 builder，也能得到一个语义完整的 NATS 连接配置对象。

**`connect(self) -> Result<Client>`**

将 `NatsAuth` 转换为 `async_nats::ConnectOptions`，然后调用 `build_in_runtime` 在 4 线程专用运行时中建立连接，返回包含 Core NATS 客户端和 JetStream 上下文的 `Client`。

---

### 4.4 `nats::Client`：NATS 统一客户端

```rust
#[derive(Clone)]
pub struct Client {
    client: client::Client,
    js_ctx: jetstream::Context,
}
```

**`client: client::Client`**（`async_nats::client::Client`）

Core NATS 客户端，支持轻量的 Pub/Sub（不持久化）。主要用于：`scrape_service` 中的一次性请求-响应（服务健康检查）。实现 `Clone`（内部基于 Arc，clone 廉价）。

**`js_ctx: jetstream::Context`**

JetStream 上下文，提供持久化消息流操作。所有 `NatsQueue` 和 Object Store 操作都通过此字段。与 `client` 字段使用同一底层连接，是其 JetStream 视图。

**`builder() -> ClientOptionsBuilder`**

工厂方法，入口点。

**`client() -> &client::Client`** / **`jetstream() -> &jetstream::Context`**

字段访问器，供需要直接操作底层对象的代码（如 `NatsQueue`）使用。

**`addr() -> String`**

返回连接的 NATS 服务器地址，格式 `"host:port"`，从 `server_info()` 读取（`server_info` 是建立连接后服务器推送的元数据）。用于日志和监控。

**`list_streams() -> Result<Vec<String>>`** / **`list_consumers(stream_name) -> Result<Vec<String>>`**

列出 JetStream 中的流名称或某个流的消费者名称。`list_consumers` 先获取指定流对象，再迭代收集消费者名称。主要用于监控、调试和 `NatsQueue::list_consumers` 的内部实现。

**`stream_info(stream_name) -> Result<jetstream::stream::State>`**

获取指定流的统计信息（消息数量、字节数、消费者数等）。`stream.info()` 需要 `&mut stream`（会更新缓存的 info），因此需要 `let mut stream`。

**`get_stream(name) -> Result<jetstream::stream::Stream>`**

获取流对象，供需要直接操作流（如 `NatsQueue::purge_acknowledged` 中遍历消费者）的方法使用。

**`scrape_service(service_name) -> Result<Subscriber>`**

向 NATS 服务框架的标准 stats 主题（`$SRV.STATS.{name}`）发送请求，用于获取 NATS 服务（service）的统计数据。实现细节：**必须先订阅**回复主题再发布请求，否则响应可能在订阅前到达而被丢弃。返回 `Subscriber`，调用方自行设置超时后从中读取响应。

---

### 4.5 Object Store 方法组

etcd 不适合存储大对象（默认值大小限制约 1.5MB），NATS Object Store 基于 JetStream 实现，支持分块存储任意大小的对象，适合模型权重、初始化数据等大文件。以 `nats://host/bucket/key` 格式统一寻址，由 `url_to_bucket_and_key(url)` 解析。

**`get_or_create_bucket(bucket_name, create_if_not_found) -> Result<ObjectStore>`**（私有）

尝试获取已存在的 bucket，不存在时根据 `create_if_not_found` 决定是否创建。通过字符串匹配 `"stream not found"` 来判断 bucket 是否存在（`async_nats` 错误类型层层嵌套，无法直接模式匹配错误码，这是已知的权宜之计）。upload 类操作传 `true`（允许自动创建），download 类操作传 `false`（文件必须已存在，否则 bail!）。

**`object_store_upload(filepath, nats_url) -> Result<()>`**

从文件系统读取文件，流式上传到 NATS Object Store。使用 `tokio::fs::File`（异步文件 IO）避免阻塞事件循环。

**`object_store_download(nats_url, filepath) -> Result<()>`**

从 NATS Object Store 下载对象，流式写入文件系统。下载不创建 bucket（`create_if_not_found = false`）。

**`object_store_delete_bucket(bucket_name) -> Result<()>`**

删除整个 bucket（含其中所有对象）。若 bucket 已不存在，静默成功（幂等），因为调用方只关心"bucket 不再存在"这个结果，而不关心是自己删的还是别人已经删了。

**`object_store_upload_data<T: Serialize>(data, nats_url) -> Result<()>`** / **`object_store_download_data<T: DeserializeOwned>(nats_url) -> Result<T>`**

序列化/反序列化 Rust 结构体到 Object Store。使用 `bincode`（二进制格式）而非 JSON：bincode 生成的数据更紧凑，序列化/反序列化更快，适合内部服务间传输结构化数据（如 KV 缓存索引）。




---

### 4.6 `NatsQueue`：任务队列会话

`NatsQueue` 代表一个进程与特定 JetStream 流的交互会话，封装了"连接 → 创建流 → 创建消费者 → 收发消息 → 关闭"的完整生命周期。

#### 结构体字段

```rust
pub struct NatsQueue {
    stream_name: String,
    nats_server: String,
    dequeue_timeout: time::Duration,
    client: Option<Client>,
    subject: String,
    subscriber: Option<jetstream::consumer::PullConsumer>,
    consumer_name: Option<String>,
    message_stream: Option<jetstream::consumer::pull::Stream>,
}
```

**`stream_name: String`**

经过 `Slug::slugify` 规范化的 JetStream 流名称，只含字母、数字、连字符，符合 NATS 流名称格式要求。规范化在构造函数中完成，后续代码无需再关心名称合法性。

**`nats_server: String`**

NATS 服务器地址，在 `connect_with_reset` 时用于创建 `ClientOptions`。存在这里是因为 `NatsQueue` 可以在需要时重新连接（目前 `connect` 是幂等的，但将来可能支持重连）。

**`dequeue_timeout: time::Duration`**

`dequeue_task` 的默认超时时间，在构造时设定。当 `dequeue_task(None)` 被调用时使用此值。允许调用方覆盖（传 `Some(timeout)`），但默认值统一管理在这里，避免上层代码散落硬编码的超时值。

**`client: Option<Client>`**

底层 NATS 客户端，`None` 表示尚未连接。所有操作方法都通过 `ensure_connection()` 保证此字段在操作前已被填充。使用 `Option` 而非直接持有 `Client` 是为了支持**懒连接**——构造时不立即建立网络连接，首次操作时才连接。这对测试和部分场景（只需要对象存在但不立即操作）很有价值。

**`subject: String`**

消息发布的 NATS 主题，格式为 `{stream_name}.*`。通配符 `*` 用于让流捕获该前缀下的所有子主题（实际发布的主题如 `{stream_name}.queue`、`{stream_name}.{event_name}`）。

**`subscriber: Option<jetstream::consumer::PullConsumer>`**

JetStream 拉取型消费者对象。`None` 表示未连接或 `consumer_name` 为 `None`（纯发布模式）。`consumer_name` 非空时在 `connect_with_reset` 中创建。持有此对象是因为关闭时需要通过它删除消费者（`shutdown`）。

**`consumer_name: Option<String>`**

消费者名称，控制消费模式：
- `Some("worker-group")`（`new` 构造，竞争消费）：多个进程使用同一消费者名称，NATS 将消息分发给其中一个，实现负载均衡。
- `Some(唯一名称)`（`new_with_consumer` 构造，广播）：每个进程用自己的唯一名称创建独立消费者，各自独立消费所有消息。
- `None`（`new_without_consumer` 构造，纯发布）：不创建消费者，`message_stream` 也为 `None`，只能发布不能消费。

**`message_stream: Option<jetstream::consumer::pull::Stream>`**

从消费者建立的消息拉取流，是实际的 `AsyncIterator`，`dequeue_task` 从中拉取消息。`connect_with_reset` 中通过 `subscriber.messages().await` 创建。

#### 三种构造函数

**`new(stream_name, nats_server, dequeue_timeout) -> Self`**：竞争消费模式，`consumer_name = Some("worker-group")`。多个工作节点用相同配置创建 `NatsQueue` 时，NATS 自动将任务分发，实现水平扩展。

**`new_without_consumer(stream_name, nats_server, dequeue_timeout) -> Self`**：纯发布模式，`consumer_name = None`。调度器只需发布任务，不需要消费，使用此构造函数避免不必要的消费者资源开销。

**`new_with_consumer(stream_name, nats_server, dequeue_timeout, consumer_name) -> Self`**：广播模式，调用方提供唯一 `consumer_name`（通常是进程/节点 ID）。多个观察者可以各自消费全量消息，用于事件订阅场景（如多个监控节点都需要收到所有推理事件）。

#### 连接管理方法

**`connect() -> Result<()>`**

`connect_with_reset(false)` 的便捷包装，正常连接，不清空流。

**`connect_with_reset(reset_stream: bool) -> Result<()>`**

核心连接方法，幂等（`client.is_some()` 时直接返回）。执行步骤：
1. 创建 NATS 客户端（通过 `build_in_runtime`）
2. 从环境变量读取 `PGD_NATS_STREAM_MAX_AGE`（默认 1 小时）作为流的消息保留时长
3. `get_or_create_stream`：若流存在则获取，不存在则创建（幂等）
4. 若 `reset_stream = true`：清空流中所有消息（用于测试重置或故障恢复场景）
5. 若 `consumer_name` 非空：创建持久化（durable）消费者，`inactive_threshold = 300s`（5 分钟无活跃则服务器自动清理元数据，防止僵尸消费者堆积），然后建立消息拉取流

**`ensure_connection() -> Result<()>`**

懒连接守卫：`client.is_none()` 时调用 `connect()`，否则直接返回。所有读写方法在执行前都调用此方法。

**`close() -> Result<()>`**

只释放本地资源（将 `message_stream`、`subscriber`、`client` 均置为 `None`），不删除服务器端的消费者元数据。下次 `connect()` 时消费者依然存在，可从上次消费位点（ack floor）恢复，不丢失消息。

**`shutdown(consumer_name: Option<String>) -> Result<()>`**

真正删除消费者：从 NATS 服务器删除指定消费者的元数据，然后调用 `close()` 释放本地资源。`consumer_name` 为 `None` 时删除自身的消费者（`self.consumer_name`）；传入具体名称时删除指定消费者（用于管理员清理场景）。

#### 任务操作方法

`NatsQueue` 在这一组里除了真正的收发方法，也补了两个偏运维和诊断的查询接口，用来观察当前流背后挂了多少消费者、它们分别叫什么名字。这类接口不走消息热路径，但对调试消费拓扑、排查僵尸 consumer 或验证广播/竞争消费模式是否按预期创建非常有用。

**`count_consumers() -> Result<usize>`**

返回当前 JetStream 流上的消费者数量。实现上它会先调用 `ensure_connection()`，保证底层客户端已经建立，然后通过 `client.jetstream().get_stream(&self.stream_name)` 取到流对象，再读取 `stream.info().await?.state.consumer_count`。这里读的是服务端维护的流元信息，而不是本地 `subscriber` 字段，因此拿到的是“这个流在 NATS 服务器视角下总共有多少 consumer”，更适合做监控、测试断言和管理界面的统计展示。

**`list_consumers() -> Result<Vec<String>>`**

列出当前流下所有消费者名称。它同样先经过 `ensure_connection()` 做懒连接兜底，随后直接复用上层 `Client::list_consumers(&self.stream_name)` 的能力，把枚举和收集 consumer 名称的细节下沉到统一客户端里。相比 `count_consumers()`，这个方法返回的是具体名字而不是数量，因此更适合排查“当前到底有哪些 worker-group 或独立观察者挂在这个流上”的问题。

这两个方法在 `ensure_connection()` 之后理论上不应再遇到 `self.client = None`，源码里保留的 `Client not connected` 分支更像是一道防御式保险，用来覆盖极端状态不一致的情况，而不是常规控制流的一部分。

**`enqueue_task(task_data: Bytes) -> Result<()>`**

发布一条任务消息到 `{stream_name}.queue` 主题。JetStream 流订阅了 `{stream_name}.*`，所以 `.queue` 后缀的消息会被持久化到流中，等待消费者拉取。

**`dequeue_task(timeout: Option<Duration>) -> Result<Option<Bytes>>`**

从消息流中拉取一条消息，立即 ACK（at-most-once 语义），返回消息 payload。超时返回 `Ok(None)`（表示暂时没有消息，可重试），错误返回 `Err`（连接问题）。

选择 at-most-once 而非 at-least-once（消费失败重投）的原因：Pagoda 的推理任务失败处理逻辑在上层（如超时重试），不依赖 NATS 的重投机制，立即 ACK 简化了错误处理路径。

**`get_queue_size() -> Result<u64>`**

获取未消费消息数（`num_pending`），即已进入流但尚未被本队列的消费者 ACK 的消息数。用于监控任务积压情况。

**`get_stream_messages() -> Result<u64>`**

获取流中总消息数（含已消费未清理的消息）。与 `get_queue_size` 不同：`queue_size` 是消费者视角的未处理数，`stream_messages` 是存储视角的总数。

**`purge_up_to_sequence(sequence: u64) -> Result<()>`**

删除流中序号小于 `sequence` 的消息（NATS JetStream 语义：序号 `sequence` 本身**不**被删除），释放存储空间。由 `purge_acknowledged` 计算出安全的清理位点后调用。

**`purge_acknowledged() -> Result<()>`**

智能清理：遍历所有消费者，找到最小的 `ack_floor.stream_sequence`（即所有消费者都已确认的最高序号），然后调用 `purge_up_to_sequence(min_ack + 1)` 清理该序号之前的消息。这保证了：已被**所有**消费者确认的消息才被清理，不会导致任何消费者丢失未消费的消息。

#### 事件发布方法

**`event_subject() -> String`**

返回 `self.stream_name`，是事件主题的基础部分。实际事件主题为 `{stream_name}.{event_name}`。

**`publish_event(event_name, event: &impl Serialize) -> Result<()>`**

将 Rust 结构体序列化为 JSON 后发布到 `{stream_name}.{event_name}` 主题。与 `enqueue_task` 的区别：任务发布到 `.queue`（被消费者竞争消费），事件发布到自定义主题（被订阅者广播接收）。

**`publish_event_bytes(event_name, bytes) -> Result<()>`**

`publish_event` 的底层实现，直接发布原始字节。这两个方法**不调用 `ensure_connection`**，要求调用方已预先连接——事件发布在热路径上（推理完成后立即发布），每次都检查连接状态会增加不必要的分支开销。

---

### 4.7 模块级辅助函数

**`url_to_bucket_and_key(url: &Url) -> Result<(String, String)>`**

解析 `nats://host/bucket/key` 格式的 URL，提取 bucket 名称和 key。若 URL 缺少路径、bucket 或 key，返回描述性错误（包含完整 URL 便于调试）。

**`instance_subject(portname_id: &PortNameId, instance_id: u64) -> String`**

为特定推理实例生成唯一的 NATS 主题名称，格式为 `{namespace}_{servicegroup}.{name}-{instance_id:x}`（`instance_id` 以十六进制表示）。例：`"prod_worker.inference-1a2b3c4d"`。供上层代码构造实例级消息主题使用，避免不同实例的消息互相干扰。

---

## 五、Event Plane 事件平面子系统

**源文件**：`event_plane/mod.rs`、`event_plane/codec.rs`、`event_plane/frame.rs`、`event_plane/traits.rs`、`event_plane/transport.rs`、`event_plane/dynamic_subscriber.rs`、`event_plane/nats_transport.rs`、`event_plane/zmq_transport.rs`

### 5.1 为什么需要 Event Plane

前面的 NATS 队列和 `zmq.rs` 路由服务分别解决了两类问题：

- NATS Queue 负责**任务投递**，强调的是持久化、排队和消费者竞争；
- `zmq.rs` 负责**请求/响应式流传输**，强调的是双向流式 RPC。

但 Runtime 里还存在第三类通信：**单向、广播式、跨 ServiceGroup 观察者模式的事件分发**。例如：工作节点状态变更、路由器内部状态广播、监控 ServiceGroup 订阅某类事件、多个观察者同时消费同一类运行时信号。这类通信不需要像队列那样做 ACK/重投，也不适合塞进请求响应通道。于是 `event_plane` 被单独抽出，提供一个**与底层传输无关的发布/订阅抽象**。

它的核心目标有四个：

1. **统一 API**：调用方只面对 `EventPublisher` / `EventSubscriber`，无需关心底层到底是 NATS 还是 ZMQ。
2. **统一事件封装**：所有后端都使用相同的 `EventEnvelope`，字段包括 publisher_id、sequence、topic、payload。
3. **统一序列化协议**：当前统一使用 MessagePack 编码，便于不同传输后端共享一套编解码逻辑。
4. **统一发现与订阅模型**：在 ZMQ 直连模式下，Subscriber 通过 Discovery 动态发现发布者；在 broker 模式或 NATS 模式下，则通过固定传输基础设施订阅。

换句话说，`event_plane` 不是某一种具体协议，而是架在 NATS / ZMQ 之上的一层**事件分发抽象层**。

---

### 5.2 `EventScope`：事件作用域

```rust
pub enum EventScope {
        Namespace { name: String },
    ServiceGroup { namespace: String, servicegroup: String },
}
```

事件平面需要先回答一个问题：**一条事件是发给整个 namespace 看的，还是只发给某个 servicegroup 内部看的？** 这就是 `EventScope` 的职责。

#### 变体说明

**`Namespace { name }`**

命名空间级事件。主题前缀形如 `namespace.{name}`。适合多个 ServiceGroup 共享的广播事件，例如一个 namespace 内的统一状态广播。

**`ServiceGroup { namespace, servicegroup }`**

ServiceGroup 级事件。主题前缀形如 `namespace.{namespace}.servicegroup.{servicegroup}`。适合一个 ServiceGroup 内部或围绕一个 ServiceGroup 的观察者消费的事件。

#### 方法说明

**`subject_prefix() -> String`**

把作用域转换为传输层使用的主题前缀。NATS 模式直接拿这个前缀拼 subject，ZMQ 模式虽然最终用 topic 过滤，但 Discovery 注册时也要记录这个作用域信息。

**`namespace() -> &str`**

统一提取 namespace 名称。无论是 namespace-scope 还是 servicegroup-scope，后续注册 DiscoverySpec 时都要写 namespace，因此单独提供访问器。

**`servicegroup() -> Option<&str>`**

仅在 servicegroup-scope 下返回 ServiceGroup 名。namespace-scope 下返回 `None`。这样 Discovery 注册可以统一写成 `scope.servicegroup().unwrap_or("")`，避免为两种作用域写分支。

---

### 5.3 `EventEnvelope`：统一事件信封

**来源**：`traits.rs`

```rust
pub struct EventEnvelope {
        pub publisher_id: u64,
        pub sequence: u64,
        pub published_at: u64,
        pub topic: String,
        pub payload: Bytes,
}
```

`EventEnvelope` 是整个事件平面的中心类型。底层传输可以换，但线上传输的“事件长什么样”必须一致，否则发布端和订阅端就无法跨后端互操作。

#### 字段说明

**`publisher_id: u64`**

发布者唯一标识，通常取自 Discovery 的 `instance_id`。这个字段的关键用途不是业务消费，而是**去重**：在 ZMQ broker HA 模式下，同一事件可能从多个 broker 副本重复送达，Subscriber 用 `(publisher_id, sequence)` 判断是否为同一事件。

**`sequence: u64`**

同一个发布者上的单调递增序号。`EventPublisher` 内部用 `AtomicU64` 每发送一条事件就加一。与 `publisher_id` 组合后可形成稳定的幂等键。

**`published_at: u64`**

发布时间戳，单位是毫秒 Unix Epoch。主要用于调试、监控、延迟分析，业务侧可用它估算事件从发布到消费的端到端延迟。

**`topic: String`**

事件的业务 topic。即使某些传输（如 NATS）已在 subject 中编码 topic，Envelope 里仍保留一份显式 topic，这样所有后端都能用统一逻辑做过滤和日志记录。

**`payload: Bytes`**

实际业务数据，已经序列化成字节。Envelope 本身不关心 payload 的结构，只负责携带它。

#### `bytes_serde` 辅助模块

`Bytes` 默认不适合直接以 MessagePack 的普通结构序列化，因此 `traits.rs` 内嵌了 `bytes_serde`，把 `Bytes` 序列化为 byte array，再在反序列化时重建为 `Bytes`。这一步使 `EventEnvelope` 可以无缝走 `serde + rmp-serde` 这套统一路径。

#### 类型别名

**`EventStream`**

`Pin<Box<dyn Stream<Item = Result<EventEnvelope>> + Send>>`。表示“原始事件信封流”。

**`TypedEventStream<T>`**

`Pin<Box<dyn Stream<Item = Result<(EventEnvelope, T)>> + Send>>`。表示“已反序列化业务 payload 的 typed stream”。Envelope 和 typed payload 一起返回，是为了让调用方既能拿到业务对象，又能保留 sequence、publisher_id、topic 等元信息。

---

### 5.4 `Codec` / `MsgpackCodec`：统一编解码层

**来源**：`codec.rs`

#### 为什么要单独抽出 codec

如果让每个传输后端自己决定如何序列化 payload，就会出现两个问题：

1. 不同后端（NATS / ZMQ）之间的事件格式不一致，切换 transport 会影响业务代码；
2. Envelope 和业务 payload 的编码路径散落在多个文件，后续扩展新的编码格式（如 JSON、Protobuf）会非常痛苦。

因此 `Codec` 被抽出来，作为事件平面的“统一序列化策略对象”。

```rust
pub enum Codec {
        Msgpack(MsgpackCodec),
}
```

当前虽然只有一种实现，但仍然先定义成 enum，是为未来扩展预留统一入口。

#### `Codec` 方法

**`encode_envelope(&EventEnvelope) -> Result<Bytes>`** / **`decode_envelope(&Bytes) -> Result<EventEnvelope>`**

对整个事件信封做编解码。发布端在真正发消息前把 Envelope 转成 wire bytes；订阅端收到 bytes 后反解为 Envelope。

**`encode_payload<T: Serialize>(&T) -> Result<Bytes>`** / **`decode_payload<T: DeserializeOwned>(&Bytes) -> Result<T>`**

只处理业务 payload，不处理 Envelope。`EventPublisher::publish<T>` 先把 `T` 编成 payload bytes，再嵌入 Envelope；`TypedEventSubscriber<T>` 则在取到 Envelope 后只解码 `payload` 字段。

**`name() -> &'static str`**

返回编码器名，当前是 `"msgpack"`，主要用于调试和日志。

#### `MsgpackCodec`

底层使用 `rmp_serde::{to_vec_named, from_slice}`。选择 MessagePack 而不是 JSON 的原因与 NATS Object Store 中使用 bincode 的原因类似：

- 更紧凑，减少消息体积；
- 保持 `serde` 生态兼容；
- 结构化数据跨语言时也比自定义二进制格式更稳。

这里没有选 bincode，是因为 MessagePack 更偏“协议格式”，可读性和互操作性更好，适合事件载荷这类潜在跨语言场景。

---

### 5.5 `FrameHeader` / `Frame`：ZMQ 二进制帧层

**来源**：`frame.rs`

NATS 天然有消息边界，收到的就是完整消息；ZMQ 的 multipart message 虽然也有帧边界，但 `event_plane` 仍然在数据帧内部再定义了一层 `Frame`，目的是把 EventEnvelope 包上一层**版本化、长度自描述**的二进制协议。

#### 协议常量

**`FRAME_VERSION: u8 = 1`**

帧协议版本。后续若需要在 ZMQ 数据帧里增加压缩标记、校验字段等，可以通过 bump version 做兼容演进。

**`FRAME_HEADER_SIZE: usize = 5`**

固定 5 字节：1 字节版本号 + 4 字节 payload 长度。

#### `FrameError`

定义了四类失败：

- `IncompleteHeader`：头不够 5 字节；
- `IncompletePayload`：头里宣称的 payload 长度大于实际剩余字节；
- `UnsupportedVersion`：版本不匹配；
- `FrameTooLarge`：预留给大帧保护（当前 `Frame::decode` 未显式用到，但错误类型先定义好）。

#### `FrameHeader`

```rust
pub struct FrameHeader {
        pub version: u8,
        pub payload_len: u32,
}
```

**`version`** 表示协议版本；**`payload_len`** 表示后续 payload 长度。因为 ZMQ message 已经提供总边界，所以这里的长度字段主要用于**自校验与协议演进**，而不是为了解决粘包拆包问题。

**`encode(&mut BytesMut)`**

把 header 依次写入 buffer：先 version 再 payload_len。

**`decode(&mut impl Buf)`**

从 buffer 中读头部，并立刻校验版本号。这样订阅端可以在读到第一字节时就发现协议不兼容，而不是等到 payload 解析失败后才暴露问题。

**`frame_size() -> usize`**

返回 `header + payload` 总长度，便于提前分配 buffer 容量。

#### `Frame`

```rust
pub struct Frame {
        pub header: FrameHeader,
        pub payload: Bytes,
}
```

**`new(payload)`**

根据 payload 自动生成 header，填入当前 `FRAME_VERSION` 和 payload 长度。

**`encode() -> Bytes`**

把 header 与 payload 拼接成最终 wire format。

**`decode(buf) -> Result<Self, FrameError>`**

先解 header，再按 header 里的 `payload_len` 读 payload。这样即使上层 ZMQ 帧里混进了损坏数据，也能在这一层明确报出“头损坏”还是“payload 不完整”。

**`size() -> usize`**

返回完整帧大小，主要用于测试和调试。

---

### 5.6 `EventTransportTx` / `EventTransportRx`：后端抽象接口

**来源**：`transport.rs`

```rust
pub trait EventTransportTx {
        async fn publish(&self, subject: &str, envelope_bytes: Bytes) -> Result<()>;
        fn kind(&self) -> EventTransportKind;
}

pub trait EventTransportRx {
        async fn subscribe(&self, subject: &str) -> Result<WireStream>;
        fn kind(&self) -> EventTransportKind;
}
```

这是 Event Plane 最关键的抽象边界。

#### 为什么要把接口压到“raw bytes”层

接口没有暴露 `EventEnvelope`，而是暴露 `Bytes`，原因是：

- 传输层只关心“发送什么字节”和“按什么 subject 订阅”；
- Envelope 编解码属于 event-plane 的通用逻辑，不属于某个具体传输后端；
- 这样 NATS 和 ZMQ 实现都可以非常薄，最大限度复用上层逻辑。

#### `WireStream`

`Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>`，表示“订阅得到的一串原始字节消息”。`EventSubscriber` 再把它提升为 `EventStream`。

#### `kind()`

返回 `EventTransportKind`，用于上层日志和调试。虽然 `EventPublisher`/`EventSubscriber` 构造时就知道自己选了哪种 transport，但保留 `kind()` 让 trait object 仍可自描述。

---

### 5.7 `NatsTransport`：基于 KV Router NATS 的事件后端

**来源**：`nats_transport.rs`

```rust
pub struct NatsTransport {
        drt: DistributedRuntime,
}
```

这个实现非常薄，它本质上不是自己管理 NATS 连接，而是借用 `DistributedRuntime` 已有的 NATS 发布/订阅能力。

#### 字段说明

**`drt: DistributedRuntime`**

持有 Runtime 句柄，从中调用 `kv_router_nats_publish` 和 `kv_router_nats_subscribe`。这样 Event Plane 不需要重复创建 NATS 客户端，避免连接与配置重复。

#### 方法说明

**`new(drt) -> Self`**

简单构造函数。

#### `impl EventTransportTx`

**`publish(subject, envelope_bytes)`**

直接把 envelope bytes 发布到 NATS subject。subject 一般是 `scope.subject_prefix() + "." + topic`。NATS 在这里承担的是天然的 broker 角色，不需要 Discovery 动态连 publisher。

#### `impl EventTransportRx`

**`subscribe(subject)`**

订阅 subject，取出每条消息的 `payload` 作为 `WireStream` 元素返回。NATS 消息已有完整边界，因此无需像 ZMQ 那样再做 socket pump。

---

### 5.8 `ZmqPubTransport` / `ZmqSubTransport`：基于 ZMQ PUB/SUB 的事件后端

**来源**：`zmq_transport.rs`

这里的 ZMQ 用的是 PUB/SUB，不是 `zmq.rs` 里的 ROUTER/DEALER。两者解决的问题不同：

- ROUTER/DEALER 用于双向请求/响应流；
- PUB/SUB 用于单向广播事件。

#### 为什么 ZMQ 事件消息要做成 multipart

ZMQ PUB/SUB 原生支持**按首帧前缀做订阅过滤**。为此本模块把一条消息拆成四帧：

1. `topic`：字符串，用于 socket 级过滤；
2. `publisher_id`：8 字节大端 u64，用于快速去重；
3. `sequence`：8 字节大端 u64，用于快速去重；
4. `frame_bytes`：内部 `Frame` 编码后的 EventEnvelope。

这样订阅端在 broker HA 模式下无需先解整个 Envelope 就能拿到 `(publisher_id, sequence)` 做高效去重。

#### 常量说明

**`ZMQ_SNDHWM = 100_000`** / **`ZMQ_RCVHWM = 100_000`**

把 ZMQ 默认 1000 的高水位放大到 100K，是为了应对事件风暴或短时消费者抖动，减少因默认队列太小导致的丢消息风险。

**`ZMQ_RCVTIMEOUT_MS = 100`**

接收超时 100ms，避免后台 pump 永远阻塞在 `recv` 上。这一点对测试和优雅关闭尤其重要。

#### `ZmqPubTransport`

```rust
pub struct ZmqPubTransport {
        socket: Arc<Mutex<zmq::Socket>>,
        topic: String,
}
```

**`socket`** 用 `Arc<Mutex<_>>` 包裹，因为 ZMQ socket 只能在同步阻塞代码里安全使用，publish 通过 `spawn_blocking` 把发送操作转到阻塞线程池，再短暂持锁发 multipart message。

**`topic`** 是 publisher 固定广播的 topic。虽然 `publish()` 签名保留了 `_subject` 参数以满足 trait 接口，但 ZMQ 实现实际以内部 `topic` 为准。

**`bind(portname, topic)`**

直连模式下创建 PUB socket 并 bind。若 portname 以 `:0` 结尾，先借助 `tokio::net::TcpListener` 找空闲端口，再让 ZMQ bind 到该端口。返回值包含 `actual_portname`，因为实际端口可能是动态分配出来的。

**`connect(xsub_portname, topic)`** / **`connect_multiple(xsub_portnames, topic)`**

broker 模式下 publisher 不再 bind 自己的公开端口，而是主动 connect 到 broker 的 XSUB 入口。多 broker 时 connect 多个 XSUB，ZMQ 负责底层负载均衡和冗余投递。

**`topic() -> &str`**

返回当前绑定 topic，多用于测试和日志。

**`publish(_subject, envelope_bytes)`**

先把 envelope 解出来，提取 `publisher_id` 和 `sequence`，再重新包成四帧 multipart message 发出去。这里“先 decode 再 send”看似多做一步，其实是为了把去重字段提前放到独立帧里，让订阅端可在不解完整 payload 的情况下快速去重。

**`configure_publish_builder(builder) -> SocketBuilder<T>`** / **`configure_subscribe_builder(builder) -> SocketBuilder<T>`**

这两个辅助函数承担的是同一类职责：在真正执行 `bind(...)`、`connect(...)` 之前，先把 ZMQ socket builder 调整到事件平面需要的运行参数。之所以单独抽成两个小函数，而不是把 `set_sndhwm`、`set_sndtimeo`、`set_rcvhwm`、`set_rcvtimeo` 分散写进每条构造路径，是为了让“创建哪种 socket”和“给 socket 套什么传输参数”两层逻辑保持分离，也便于后续统一调整默认值。

`configure_publish_builder(...)` 面向发布侧 builder，主要注入发送高水位与发送超时配置。前者决定发布端在突发事件流量下最多允许积压多少待发送消息，后者则避免发送调用在下游长时间不可用时无限阻塞。这样发布端即使遇到 broker 抖动或订阅端消费迟缓，也能以受控方式承受背压，而不是把阻塞扩散到整个上层事件发布路径。

`configure_subscribe_builder(...)` 面向订阅侧 builder，负责设置接收高水位和接收超时。接收高水位用于给短时间事件峰值留出缓冲空间，降低因默认队列过小导致的丢消息概率；接收超时则与后面的 socket pump 配合，让接收循环即使暂时没有新消息，也能定期从阻塞态返回，从而更及时地响应取消、关闭和测试中的生命周期控制。

这两个函数都保留在泛型 `SocketBuilder<T>` 层，而不是写死到某个具体 socket 类型上，说明它们的目标并不是参与业务语义，而是为所有符合 `tmq::FromZmqSocket` 约束的构造路径提供统一的底层调优入口。

**`multipart_message(multipart) -> Vec<Vec<u8>>`**

这也是一个纯粹的辅助函数，职责是把 `tmq` 返回的 `Multipart` 逐帧转换成拥有所有权的字节数组集合。这样做的意义不在于改变协议语义，而在于把“底层库给出的帧迭代器表示”统一整理成更容易继续处理的 `Vec<Vec<u8>>` 结构，方便后续代码按固定位置读取 topic、`publisher_id`、`sequence` 和数据帧。

把转换集中到这一层还有一个实际好处：后面的多帧校验、日志记录、错误分支和跨任务传递，都不必反复关心 `Multipart` 自身的迭代器和 buffer 表示，只处理普通的 owned bytes 即可。代价当然是每一帧都会做一次复制，但考虑到这里的事件消息本来就是固定数量的小帧，这个开销是可接受的，换来的则是后续处理逻辑明显更直接、更稳定。

#### `ZmqSubTransport`

```rust
pub struct ZmqSubTransport {
        socket: Arc<Mutex<zmq::Socket>>,
        broadcast_tx: tokio::sync::broadcast::Sender<Bytes>,
        _socket_pump_handle: tokio::task::JoinHandle<()>,
}
```

**`socket`** 持有底层 SUB socket；**`broadcast_tx`** 用于把收到的消息扇出给多个 Rust 层订阅者；**`_socket_pump_handle`** 持有后台任务句柄，确保 pump 生命周期和 transport 绑定。

**`connect(portname, topic)`**

直连单个 publisher。连接后立即 `set_subscribe(topic.as_bytes())`，让 ZMQ 在 socket 层做 topic 过滤。

**`connect_multiple(portnames, topic)`**

直连多个 publisher，形成 fan-in。一个 SUB socket connect 多个 PortName，收到的事件统一汇入一个 pump。

**`connect_broker(xpub_portname, topic)`** / **`connect_broker_multiple(xpub_portnames, topic)`**

broker 模式下订阅 broker 的 XPUB 出口，本质上复用了 `connect` / `connect_multiple`。

**`start_socket_pump(socket, broadcast_tx)`**（私有）

后台循环从 ZMQ socket 收四帧 multipart message。流程如下：

1. 通过 `spawn_blocking` 执行阻塞 `recv_bytes`；
2. 先收 topic，再收 publisher_id，再收 sequence，再收 data；
3. 把 data 作为 `Frame` 解码，取出内部 payload（即 envelope bytes）；
4. 通过 `broadcast_tx.send(frame.payload)` 广播给所有 Rust 层订阅者。

如果订阅者消费落后，broadcast channel 可能发生 lagged，后续在 `subscribe()` 返回的 stream 中只记 warn，不会把整个订阅流终止。

**`subscribe(_subject)`**

并不直接从 socket 读，而是从 `broadcast_tx.subscribe()` 创建一个新的 broadcast receiver，包装成 `WireStream` 返回。这是本实现最重要的设计点：**socket pump 唯一持有 ZMQ socket，业务订阅者只消费内存广播流**。这样多个订阅者不会争夺同一个 ZMQ socket，也不会长时间占用 socket 锁。

---

### 5.9 `DynamicSubscriber`：ZMQ 直连模式下的动态发现订阅器

**来源**：`dynamic_subscriber.rs`

NATS 模式下，broker 就是 NATS server；ZMQ broker 模式下，broker 是 XPUB/XSUB；但 ZMQ **直连模式**没有中心 broker，因此 Subscriber 必须自己观察 Discovery，感知有哪些 Publisher 上线/下线，并与之建立或断开连接。`DynamicSubscriber` 就是这层控制器。

```rust
pub struct DynamicSubscriber {
        discovery: Arc<dyn Discovery>,
        query: DiscoveryQuery,
        topic: String,
        cancel_token: CancellationToken,
}
```

#### 字段说明

**`discovery`**

发现后端，用于 `list_and_watch` 事件通道实例。

**`query`**

订阅条件。namespace-scope 和 servicegroup-scope 会生成不同的 `DiscoveryQuery::EventChannels(...)`，决定要看哪一组 publisher。

**`topic`**

目标业务 topic。它不仅用于上层逻辑过滤，也用于 ZMQ `set_subscribe` 的原生前缀过滤。

**`cancel_token`**

停止信号。调用 `cancel()` 或对象 drop 时统一关闭 discovery watch 和所有 portname stream。

#### 方法说明

**`new(discovery, query, topic) -> Self`**

构造器。

**`start_zmq(self: Arc<Self>) -> Result<WireStream>`**

这是主入口。它创建一个 `mpsc::unbounded_channel<Bytes>` 作为多 publisher 合流通道，再启动后台 discovery watch：

- 收到 `DiscoveryEvent::Added(instance)`：
    - 提取出该实例的 ZMQ portname；
    - 若此前未连接过，则为该 portname 创建一个独立 `CancellationToken`；
    - spawn 一个 `consume_portname_stream()` 任务，把该 publisher 的事件转发进合流 channel。
- 收到 `DiscoveryEvent::Removed(instance_id)`：
    - 找到对应 portname 的 cancellation token；
    - 取消对应的消费任务并从 active map 移除。

`active_portnames` 使用 `HashMap<String, (String, CancellationToken)>` 记录“instance_id → (portname, cancel token)”关系，防止重复连接同一 publisher。

**`extract_zmq_portname(instance) -> Option<String>`**（私有）

只接受 `DiscoveryInstance::EventChannel` 且其中 transport 为 `EventTransport::Zmq { portname }` 的实例。其他 transport 类型在直连订阅路径上都被忽略。

**`consume_portname_stream(portname, zmq_topic, event_tx, cancel_token)`**（私有）

与单个 portname 建立 `ZmqSubTransport`，然后在循环中：要么等待 cancel，要么从 stream 读取 bytes 并转发到合流 channel。任何 portname 级错误只会终止这一条连接，不会拖垮整个 DynamicSubscriber。

**`cancel()`** / **`Drop`**

两者都会触发 `cancel_token.cancel()`，确保后台任务不会泄漏。

---

### 5.10 Broker 解析与去重：`resolve_zmq_broker` / `DeduplicatingStream`

**来源**：`mod.rs`

Event Plane 的 ZMQ 不是只有一种部署方式，而是有两种：

1. **直连模式**：Publisher 自己 bind，Subscriber 通过 Discovery 动态发现并逐个 connect；
2. **Broker 模式**：Publisher 和 Subscriber 都 connect 到中间 broker（XSUB/XPUB）。

#### `resolve_zmq_broker(drt, scope) -> Result<Option<BrokerPortNames>>`

按优先级解析 broker 配置：

**优先级 1：`PGD_ZMQ_BROKER_URL`**

若显式设置 broker URL，就直接解析，不依赖 Discovery。字符串格式是：

`xsub=tcp://host1:5555;tcp://host2:5555 , xpub=tcp://host1:5556;tcp://host2:5556`

**优先级 2：`PGD_ZMQ_BROKER_ENABLED=true` + Discovery 查找**

若启用了 broker 模式但未手填 URL，就通过 `DiscoveryQuery::EventChannels(EventChannelQuery::servicegroup(namespace, "zmq_broker"))` 查找 broker ServiceGroup 发布的事件通道实例，并提取其中 `EventTransport::ZmqBroker { xsub_portnames, xpub_portnames }`。

**都没有** → 返回 `Ok(None)`，表示走直连模式。

#### `parse_broker_url(url)`

只做字符串解析和校验，确保 xsub/xpub 两侧都至少有一个 portname。

#### `BrokerPortNames`

`BrokerPortNames` 是 broker 模式下的一个简单载体，专门把 broker 暴露出来的两组 PortName 成对保存起来：`xsub_portnames` 给发布端使用，`xpub_portnames` 给订阅端使用。它本身不包含任何行为，只负责把“同一组 broker 的上下行入口地址”以结构化方式交给后续初始化流程。

Publisher 用 xsub PortName 列表去 connect；Subscriber 用 xpub PortName 列表去 connect。

#### `DeduplicatingStream`

`DeduplicatingStream` 是包在原始 `WireStream` 外面的一层轻量适配器，内部由底层字节流、共享编解码器和一张最近已见事件的 LRU 表组成。它存在的目的很明确：在不改变上层消费接口的前提下，把多 broker HA 场景里可能出现的重复事件拦在进入业务流之前。

多 broker HA 模式下，同一事件可能从多个 broker 同时到达 Subscriber。为解决重复投递，`DeduplicatingStream` 包装原始 `WireStream`，在把事件真正交给上层之前先做一次基于 `(publisher_id, sequence)` 的幂等过滤。

#### 字段说明

**`inner: WireStream`**

底层原始字节流，是 `DeduplicatingStream` 真正轮询的数据来源。这个字段不负责解释事件，只负责把 broker 层送上来的 `Bytes` 顺序交给去重逻辑处理；一旦它返回 `Err`、`None` 或 `Pending`，包装层也会按原样向上传递。

**`codec: Arc<Codec>`**

统一编解码器，用来把 `Bytes` 反解成 `EventEnvelope`，从而提取 `publisher_id` 和 `sequence`。去重必须依赖这两个字段，因此这里不能只把消息体当作匿名字节透传；使用 `Arc` 则是为了让多个订阅链路可以共享同一套 codec 配置，而不必重复构造。

**`seen_events: LruCache<(u64, u64), ()>`**

最近已见事件表，键是 `(publisher_id, sequence)`，值是空元组 `()`。这种设计明确表达了去重层只关心“某个事件键是否已经出现过”，而不需要附加状态。之所以用 LRU，是为了在维持幂等过滤能力的同时，把内存占用限制在一个固定窗口内。

**`new(inner, codec, cache_size) -> Self`**

构造函数只做一件事：把外部传入的流、编解码器和缓存容量封装起来，并用 `NonZeroUsize` 保证 LRU 容量必须大于 0。这个约束很直接，因为“容量为 0 的去重缓存”在语义上等价于根本不做去重，反而会让多 broker 模式表现得像未开启保护一样。

**`impl Stream for DeduplicatingStream`**

它对外仍然实现为 `Stream<Item = Result<Bytes>>`，也就是说去重层不会改变上传输抽象，只会过滤和透传。`poll_next` 的控制流可以概括为四步：

1. 先轮询内部 `inner` 流；
2. 收到 `Ok(bytes)` 后，用 `codec.decode_envelope(&bytes)` 解析出事件信封；
3. 用 `(envelope.publisher_id, envelope.sequence)` 作为幂等键查询 `seen_events`，命中过则记录一条 debug 日志并继续循环，直到拿到下一条未重复事件；
4. 若未命中，则把该键写入 LRU 缓存，再把原始 `bytes` 返回给上游。

如果解码失败，它不会盲目把这条消息放过去，而是先记一条 warn 日志，再把错误原样向上游返回。这样做的原因是：一旦连 envelope 都无法解析，就无法可靠提取 `publisher_id` 和 `sequence`，此时继续“假装去重成功”只会掩盖底层协议或数据损坏问题。至于 `inner` 流本身返回的 `Err`、`None` 和 `Pending`，这个包装层都保持原样透传，不引入额外语义。

之所以用 LRU 而不是 HashSet，是为了让去重表保持有界大小。默认缓存 100,000 条，足以覆盖短时间内重复投递窗口，同时避免长期运行导致内存无界增长。

---

### 5.11 `EventPublisher`：发布端统一入口

**来源**：`mod.rs`

```rust
pub struct EventPublisher {
        transport_kind: EventTransportKind,
        scope: EventScope,
        topic: String,
        publisher_id: u64,
        sequence: AtomicU64,
        tx: Arc<dyn EventTransportTx>,
        codec: Arc<Codec>,
        runtime_handle: tokio::runtime::Handle,
        discovery_client: Option<Arc<dyn Discovery>>,
        discovery_instance: Option<DiscoveryInstance>,
}
```

#### 字段说明

**`transport_kind`**

当前实际使用的传输后端（NATS / ZMQ），用于日志、自描述和调试。

**`scope`**

事件作用域，决定 subject 前缀和 Discovery 注册维度。

**`topic`**

业务 topic 名。最终 subject 通常是 `scope.subject_prefix() + "." + topic`。

**`publisher_id`**

发布者 ID，取自 `drt.discovery().instance_id()`。这样同一 Runtime 进程创建的 publisher 有稳定身份。

**`sequence`**

原子自增计数器。每发布一条事件就 `fetch_add(1, Ordering::SeqCst)`，保证并发 publish 下序号仍单调增长。

**`tx`**

具体传输实现的 trait object，真正负责把 envelope bytes 发出去。

**`codec`**

统一编解码器，当前固定是 Msgpack。

**`runtime_handle`**

用于 Drop 时异步执行 unregister。Drop 可能发生在没有当前 Tokio 上下文的线程（例如 PyO3 finalizer），因此必须保存创建时 Runtime 的 handle，而不能依赖 `Handle::current()`。

**`discovery_client` / `discovery_instance`**

用于发布端自动注册 / 注销 Discovery。broker 模式下通常不注册，因此这两个字段可能为 `None`。

#### 构造函数

**`for_servicegroup(comp, topic)`** / **`for_namespace(ns, topic)`**

便捷入口，transport 从 `EventTransportKind::from_env_or_default()` 获取。

**`for_servicegroup_with_transport(...)`** / **`for_namespace_with_transport(...)`**

显式指定 transport，适合测试和需要强制选择后端的场景。

**`new_internal(drt, scope, topic, transport_kind)`**（私有核心）

这是整个发布端初始化的核心逻辑：

- 若是 **NATS**：
    - 创建 `NatsTransport`；
    - 构造 `EventTransport::nats(scope.subject_prefix())`；
    - 通过 Discovery 注册 `DiscoverySpec::EventChannel`。

- 若是 **ZMQ + broker 模式**：
    - 解析 broker portnames；
    - 创建 `ZmqPubTransport::connect(...)` 或 `connect_multiple(...)`；
    - **不注册 Discovery**，因为 broker 本身已是固定基础设施，Subscriber 不需要通过 Discovery 找每个 publisher。

- 若是 **ZMQ + 直连模式**：
    - 在独立线程里起临时 Tokio runtime 调 `ZmqPubTransport::bind("tcp://0.0.0.0:0", topic)`；
    - 得到实际端口后，通过 `get_local_ip_for_advertise()` 生成可对外公开的 portname；
    - 用 `EventTransport::zmq(public_portname)` 注册到 Discovery。

这里在 ZMQ 直连模式下使用独立线程创建 runtime 的原因是：ZMQ bind 初始化路径依赖 async 代码，而构造阶段又希望把“绑定 socket 并得到实际地址”完整同步地做完，避免半初始化状态泄露给调用方。

#### 发布方法

**`publish<T: Serialize>(&self, event: &T)`**

先把 typed payload 编成 bytes，再复用 `publish_bytes`。

**`publish_bytes(&self, bytes: Vec<u8>)`**

构造 `EventEnvelope`：填 publisher_id、sequence、published_at、topic、payload；随后编码 envelope，并把它发到 `scope.subject_prefix() + "." + topic` 对应的 subject。

#### 访问器

**`publisher_id()`**、**`topic()`**、**`transport_kind()`** 都是纯访问器，供上层做诊断和日志。

#### `Drop`

若当前 publisher 曾注册过 Discovery，则在 drop 时异步调用 `discovery.unregister(instance)`。注意这里不是同步阻塞等待注销完成，而是 best-effort 地在保存下来的 runtime 上 spawn 一个任务：

- 成功 spawn → 后台注销；
- runtime 已不可用 → warn 日志后放弃。

这符合事件发布端的生命周期需求：注销很重要，但不值得为了它在析构路径上阻塞线程甚至 panic。

---

### 5.12 `EventSubscriber` / `TypedEventSubscriber<T>`：订阅端统一入口

**来源**：`mod.rs`

```rust
pub struct EventSubscriber {
        stream: EventStream,
        scope: EventScope,
        topic: String,
        codec: Arc<Codec>,
}
```

`EventSubscriber` 封装了“订阅底层 transport + 解 envelope + 过滤 topic”这一整套流程。

#### 字段说明

**`stream`**

已经提升到 `EventEnvelope` 层的事件流。构造阶段底层先得到 `WireStream`，再解码、过滤后存入这里。

**`scope`** / **`topic`**

虽然当前大多只用于日志和调试，但保留它们让 `EventSubscriber` 始终知道自己在订阅什么。

**`codec`**

用于 `typed()` 路径再解业务 payload。

#### 构造函数

与 `EventPublisher` 对称：有 servicegroup/namespace 两类入口，也有“自动选 transport”和“显式指定 transport”两套接口。

**`for_servicegroup(comp, topic) -> Result<Self>`**

创建一个 ServiceGroup 级 topic 的订阅端，并自动选择事件传输后端。它本质上只是显式版本的便捷包装：先从 `comp.drt()` 读取 `default_event_transport_kind()`，再把结果交给 `for_servicegroup_with_transport(...)`。因此这条入口更适合“业务只关心订阅，不关心底层走 NATS 还是 ZMQ”的常规调用路径。

**`for_servicegroup_with_transport(comp, topic, transport_kind) -> Result<Self>`**

这是 ServiceGroup 级订阅的显式控制版本。它会从 ServiceGroup 取出所属 `DistributedRuntime`，再构造 `EventScope::ServiceGroup { namespace, servicegroup }`，最后统一委托给 `new_internal(...)` 完成真正初始化。把作用域拼装放在这里，而不是散落到 `new_internal(...)` 内部，可以让“订阅哪个范围”和“如何订阅”这两层职责保持分离。

**`for_namespace(ns, topic) -> Result<Self>`**

创建 namespace 级 topic 的订阅端，并自动决定传输方式。自动选择规则与 ServiceGroup 入口一致：优先尊重 `PGD_EVENT_PLANE` 这类外部配置，若没有显式覆盖，则退回 Runtime 的默认策略，也就是本地后端更偏向 ZMQ、分布式后端更偏向 NATS。这样上层业务代码只表达“我要订阅一个 namespace 级事件”，而不必把部署环境差异硬编码进调用点。

**`for_namespace_with_transport(ns, topic, transport_kind) -> Result<Self>`**

这是 namespace 级订阅的显式版本。它从 `ns` 中提取 `DistributedRuntime`，构造 `EventScope::Namespace { name }`，再把 topic 与选定的 `transport_kind` 一并交给 `new_internal(...)`。和 ServiceGroup 版本相同，这条入口主要给测试、调试和需要强制绑定某个传输后端的场景使用。


#### `new_internal(...)`

订阅端初始化逻辑如下：

- **NATS**：
    - 直接订阅 `scope.subject_prefix() + "." + topic`；
    - NATS 自带 broker，因此无需 Discovery。

- **ZMQ + broker 单实例**：
    - connect broker 的单个 XPUB portname；
    - 直接返回 stream，无需去重。

- **ZMQ + broker 多实例（HA）**：
    - connect 多个 XPUB portname；
    - 用 `DeduplicatingStream` 包装 stream，按 `(publisher_id, sequence)` 去重。

- **ZMQ + 直连模式**：
    - 根据 scope 构造 `DiscoveryQuery::EventChannels(...)`；
    - 创建 `DynamicSubscriber`；
    - 由它 watch discovery 并动态连接/断开 publisher。

最后无论哪种 transport，都会统一经过一层 `filter_map`：

- `decode_envelope(&bytes)`；
- 若 `envelope.topic == topic_filter`，才向上游产出；
- 否则丢弃。

这一步保证了即使底层 transport 不能完全靠原生 topic 过滤，Event Plane 的语义仍保持一致。

#### 消费方法

**`next(&mut self) -> Option<Result<EventEnvelope>>`**

逐条取下一条事件信封。

**`typed<T>(self) -> TypedEventSubscriber<T>`**

把当前 subscriber 提升为 typed subscriber。这里按值消耗 `self`，避免同时维护“原始 envelope stream”和“typed stream”两个并发读取同一底层流的句柄。

#### `TypedEventSubscriber<T>`

```rust
pub struct TypedEventSubscriber<T> {
        stream: EventStream,
        codec: Arc<Codec>,
        _marker: PhantomData<T>,
}
```

**`next()`**

先从底层流取到一个 `EventEnvelope`，再对其中 `payload` 做 `decode_payload<T>()`。成功时返回 `(envelope, typed_payload)`，失败时返回 `Err`。

这层设计让调用方可以选择：

- 要么只消费原始 envelope，自己决定何时/如何解码；
- 要么直接以 typed 方式消费，少写样板代码。

**`current_timestamp_ms() -> u64`**

这是 Event Plane 内部用于生成 `published_at` 的辅助函数，职责很单一：把当前系统时间转换成 Unix Epoch 以来的毫秒时间戳。实现上直接读取 `SystemTime::now()`，再计算相对 `UNIX_EPOCH` 的毫秒差值并转成 `u64`，从而为 `EventEnvelope` 提供统一的发布时间基线。

这里对异常路径采用了保守兜底策略：如果系统时间异常地早于 Unix Epoch，函数不会把错误继续上抛，而是返回 `0`。原因是 `published_at` 主要用于观测、日志和延迟分析，不属于事件投递成功与否的核心语义；即使时间源偶发异常，也不应该因此打断整个事件发布流程。
---

### 5.13 Event Plane 的整体工作流

把前面所有类型串起来，Event Plane 的典型执行链如下：

1. 调用方在 `Namespace` 或 `ServiceGroup` 上创建 `EventPublisher` / `EventSubscriber`；
2. 构造阶段根据 `EventTransportKind` 选择 NATS 或 ZMQ；
3. 发布端用 `Codec` 把业务 payload 编成 bytes，再封进 `EventEnvelope`；
4. 若是 ZMQ，则 envelope bytes 再被 `Frame` 包装后发成 multipart message；
5. 订阅端收到 `WireStream` 后统一解回 `EventEnvelope`；
6. 若调用方选择 typed 订阅，再由 `TypedEventSubscriber<T>` 解 payload。

从架构上看，这使 `event_plane` 成为一个非常干净的分层：

- **业务层**：只看 typed event；
- **事件平面层**：看 `EventEnvelope`；
- **传输抽象层**：看 `Bytes`；
- **具体传输层**：看 NATS subject 或 ZMQ multipart/frame。

这种分层最大好处是：未来若新增第三种事件传输（例如 Redis Pub/Sub 或 Kafka），只需实现 `EventTransportTx/Rx` 并接进 `EventPublisher/EventSubscriber` 的初始化分支，而无需重写整个事件模型。

---

## 六、ZeroMQ 传输子系统

**源文件**：`zmq.rs`

### 5.1 为什么选择 ZeroMQ

流式推理（token streaming）对延迟极其敏感，且一个请求对应多个响应帧（每个 token 一帧）。NATS 的额外 broker 跳数会引入不必要的延迟。ZMQ Router/Dealer 模式实现**直接点对点通信**，无 broker 中转。更重要的是，Dealer socket 天然支持多路复用——多个并发推理请求共享同一 TCP 连接，通过帧中的 `request_id` 区分各自的响应流，避免了为每个请求建立独立 TCP 连接的开销。

### 5.2 内部协议类型

这些类型仅在 `zmq.rs` 内部使用，不对外暴露，但它们定义了 ZMQ 通信的协议格式，是理解整个子系统的基础。

#### `ControlMessage` 枚举

```rust
enum ControlMessage {
    Cancel    { request_id: String },
    CancelAck { request_id: String },
    Error     { request_id: String, error: String },
    Complete  { request_id: String },
}
```

控制平面消息，用于管理推理流的生命周期：

- `Cancel`：客户端请求取消正在进行的推理（用户中途停止生成）
- `CancelAck`：服务端确认收到取消请求
- `Error`：服务端推理过程中发生错误，携带错误信息
- `Complete`：服务端通知客户端推理已完成（所有 token 已发送）

`Serialize`/`Deserialize` 用于在 ZMQ 帧中传输（序列化为字节）。

#### `MessageType` 枚举

```rust
enum MessageType {
    Data(Vec<u8>),
    Control(ControlMessage),
}
```

将数据帧和控制帧统一表示，便于路由逻辑中区分处理方式。`Data` 帧直接转发到数据 channel，`Control` 帧转发到控制 channel。

#### `StreamAction` 枚举

```rust
enum StreamAction {
    SendEager(usize),    // try_send 非阻塞成功，携带字节数（用于计数器 TODO）
    SendDelayed(usize),  // 背压后阻塞 send 成功，携带字节数（用于计数器 TODO）
    Close,               // channel 已关闭，需从路由表移除该 request_id
}
```

表示向 channel 发送单个帧的结果，供后续决策（记录指标或清理路由表）使用。`SendEager` 和 `SendDelayed` 携带字节数是为了将来实现 bytes_received 等计数器，当前标注为 TODO。

---

### 5.3 `RouterState`：流路由表

**为什么需要**

ZMQ Router socket 从多个 Dealer 收到混合帧流，需要按 `request_id` 将帧路由到正确的接收方。接收方不在同一个任务中（推理任务和路由任务分离），必须通过 channel 传递。`RouterState` 管理 `request_id → channel sender` 的映射关系。

```rust
struct RouterState {
    active_streams: HashMap<String, mpsc::Sender<Bytes>>,
    control_channels: HashMap<String, mpsc::Sender<ControlMessage>>,
}
```

**`active_streams: HashMap<String, mpsc::Sender<Bytes>>`**

数据平面路由表：`request_id` 映射到数据 channel 的发送端。推理服务收到客户端请求后，创建 channel，将接收端交给处理该请求的任务，将发送端通过 `register_stream` 注册到这里。之后 Router 收到该 `request_id` 的数据帧时，直接 `tx.send(frame)` 转发。

**`control_channels: HashMap<String, mpsc::Sender<ControlMessage>>`**

控制平面路由表：`request_id` 映射到控制 channel 的发送端。控制消息（取消、错误、完成）通过独立 channel 传递，避免与数据流混在一起增加处理复杂度。

**`new() -> Self`**

创建空路由表。

**`register_stream(request_id, data_tx, control_tx)`**

同时向两张 HashMap 插入同一 `request_id` 的 channel。原子操作（在同一个 `&mut self` 调用中完成），保证两张表的一致性——要么都有这个 `request_id`，要么都没有。

**`remove_stream(request_id: &str)`**

从两张 HashMap 中删除指定 `request_id`，在流关闭时（channel sender 已关闭）清理路由表。

---

### 5.4 `Server`：ZMQ 路由服务

```rust
#[derive(Clone, Dissolve)]
pub struct Server {
    state: Arc<Mutex<RouterState>>,
    cancel_token: CancellationToken,
    fd: i32,
}
```

**`state: Arc<Mutex<RouterState>>`**

路由表的共享引用。`Server` clone 后多个副本共享同一路由表，任何副本都可以注册新的流（`state.lock().await.register_stream(...)`）。`tokio::sync::Mutex` 保护并发访问——路由任务（`Server::run`）持锁查找 channel，注册任务持锁添加新流，二者可能并发。

**`cancel_token: CancellationToken`**

子 token（从创建者的 token 派生），用于停止此 `Server`。调用 `cancel()` 后，`Server::run` 中的 `select!` 取消臂被触发，路由循环退出。使用子 token 而非父 token，确保只停止这一个 Server 实例，不影响父级的生命周期。

**`fd: i32`**

ZMQ Router socket 的 OS 文件描述符。ZMQ socket 可读事件会反映在此 fd 上，外部代码可以用 `epoll`/`kqueue` 等机制轮询该 fd，在有数据时才调用 ZMQ 接收操作，避免 Tokio 运行时对 ZMQ socket 做 busy-polling（ZMQ socket 不是原生的 tokio-compatible IO）。

**`Dissolve` 宏**生成 `.dissolve() -> (Arc<Mutex<RouterState>>, CancellationToken, i32)`，允许上层在需要时解构 Server 的所有字段。

#### `Server::new(context, address, cancel_token) -> Result<(Self, ServerExecutionHandle)>`

创建 ZMQ Router socket 并绑定到指定地址，返回 `(Server, ServerExecutionHandle)` 对：
- `Server`：持有路由表和 cancel token，供上层注册新的流
- `ServerExecutionHandle`：持有后台任务句柄，供上层管理任务生命周期

内部启动**双层任务**：
- `primary_task = tokio::spawn(Self::run(...))`：执行实际路由逻辑
- `watch_task = tokio::spawn(async { primary_task.await.inspect_err(|e| cancel_token.cancel())? })`：监控 `primary_task`，无论是 panic（`JoinError`）还是返回 `Err`，都调用父 `cancel_token.cancel()` 向上传播错误

双层任务确保 ZMQ 传输层的任何崩溃都不会被静默吞掉，都会触发父 Runtime 的优雅关闭。

#### `Server::run(router, state, token) -> Result<()>`（私有静态）

路由循环主体，`select! biased` 同时等待两个事件：

**臂1：新帧到达 `router.next()`**

成功收到帧后，**首先校验帧数**：ZMQ Router/Dealer 协议要求恰好 3 帧（identity、request_id、payload）。帧数不等于 3 时调用 `anyhow::bail!` 返回 `Err`，这是协议层的 panic-equivalent——调用方代码存在 bug，应立即暴露而非静默处理。

路由帧：从路由表中查找 `request_id` 对应的 channel sender，按三步策略发送：
1. `try_send`（非阻塞）：成功则 `SendEager`，最快路径
2. `try_send` 返回 `Full`（channel 满，消费者跟不上）：退化为阻塞 `send().await`，成功则 `SendDelayed`
3. `try_send` 返回 `Closed` 或 `send` 失败：channel 已关闭，`Close`，从路由表移除

`SendDelayed`（阻塞 `send().await`）会**阻塞整个路由循环**，影响所有其他 `request_id` 的帧处理。这是已知的设计缺陷，代码注释标注 TODO：应加超时，超时后强制关闭该流而非无限阻塞。

若 `request_id` 不在路由表中（未注册的流），静默丢弃帧（并记录 trace 日志）。

**臂2：取消信号 `token.cancelled()`**

退出路由循环，返回 `Ok(())`，任务正常结束。

---

### 5.5 `ServerExecutionHandle`：任务生命周期控制

```rust
pub struct ServerExecutionHandle {
    task: JoinHandle<Result<()>>,
    cancel_token: CancellationToken,
}
```

不实现 `Clone`，确保只有一处持有 `JoinHandle`，`join()` 只被调用一次（`JoinHandle::await` 消耗所有权）。

**`task: JoinHandle<Result<()>>`**

`watch_task` 的句柄（不是 `primary_task`，`watch_task` 包含错误传播逻辑）。`join()` 等待任务结束并返回最终结果。

**`cancel_token: CancellationToken`**

与 `Server` 中同一个子 token，调用 `cancel()` 可触发路由循环退出。

**`is_finished() -> bool`**：任务是否已结束（正常或出错）。用于轮询任务状态而无需阻塞等待。

**`is_cancelled() -> bool`**：cancel token 是否已被触发。

**`cancel()`**：触发 cancel token，通知路由循环退出。

**`join(self) -> Result<()>`**：消耗 handle，阻塞等待任务结束，传播任务的 `Result`。

---

### 5.6 `zmq::Client`：Dealer socket 封装

```rust
pub struct Client {
    dealer: Dealer<IntoIter<Vec<u8>>, Vec<u8>>,
}
```

不实现 `Clone`（ZMQ socket 有连接状态和内部队列，不能被多个持有者并发使用）。

**`dealer: Dealer<IntoIter<Vec<u8>>, Vec<u8>>`**

底层 ZMQ Dealer socket。Dealer 是 ZMQ Router 的对端：Dealer 发送的帧在 Router 接收时自动被 Router 附加 identity 帧（标识发送方），因此 Dealer 只需发送 `[request_id, payload]` 两帧，Router 看到的是三帧。

**`new(context, address) -> Result<Self>`**（私有关联函数）

创建 Dealer socket 并连接到指定 Router 地址。私有是因为目前 ZMQ Client 的创建方式对上层是不透明的，外部通过更高层的工厂函数创建（具体机制在 `zmq.rs` 的上层模块中）。

**`dealer() -> &mut Dealer<...>`**

返回底层 socket 的可变引用。调用方可以：
- `dealer.send(vec![request_id_bytes, payload_bytes]).await`：发送请求帧
- `dealer.next().await`：接收响应帧（对应 Router 发回的帧）

高层封装方法（如 `send_data(request_id, data)`、`send_control(request_id, ctrl)`、`receive() -> Frame`）当前标注为 TODO，未来会在此基础上实现完整的客户端协议。

---

## 七、TCP 传输

**源文件**：`tcp.rs`（全文只有一行）

```rust
pub use crate::pipeline::network::tcp::{client, server};
```

TCP 传输的具体实现在 `pipeline::network::tcp` 中（用于 HTTP/gRPC 等上层协议的连接管理）。`transports` 模块只做 re-export，使所有传输方式可以从 `transports::tcp` 统一访问，保持 `transports` 作为"所有传输协议单一入口"的设计定位，上层代码不需要了解实现分布在哪些子模块中。

---

## 八、设计决策总结

| 决策 | 问题背景 | 解决方案 | 权衡说明 |
|------|---------|---------|---------|
| 专用运行时（`build_in_runtime`） | etcd/NATS 后台任务需要独立、持续存活的运行时 | 独立 OS 线程 + `pending().await` 永不退出 | 增加 OS 线程开销，换取后台任务生命周期可控 |
| `Connector` 持 sync RwLock | `get_client()` 高频调用，不需要 async | `parking_lot::RwLock` 比 `tokio::sync::RwLock` 开销更低 | 代价：不能在 async 闭包中跨 await 持锁 |
| `Connector` 持 async Mutex（backoff） | 重连是 async 操作，且需要串行化 | `tokio::sync::Mutex` 同时满足 async 和串行化需求 | 一个重连操作阻塞所有并发重连请求，但共享成功结果 |
| 租约 TTL/2 心跳频率 | 固定间隔无法适应服务端动态调整的 TTL | 以剩余 TTL 的一半动态计算下次心跳时间 | 略增计算复杂度，换取更好的适应性 |
| 读锁事务原子性 | 写锁两步操作有竞争窗口，但读锁必须无竞争 | 读锁将"检查写锁不存在"和"创建读锁键"合并到一个 etcd 事务 | 相比写锁多一次 etcd 事务往返，但消除了竞争窗口 |
| `kv_create` 幂等语义 | 多进程并发注册同一键时第二个进程收到 `Err` 误认为失败 | 返回 `Ok(Some(version))` 表示键已存在 | API 语义更复杂（三态返回），但调用方能区分"新建"和"已存在" |
| at-most-once ACK | 推理任务有自己的超时重试逻辑 | `dequeue_task` 立即 ACK，不依赖 NATS 重投 | 消费后处理失败不会重投，需上层负责重试 |
| Event Plane 统一信封 | 同一事件需要跨 NATS / ZMQ 传输且保持语义一致 | 所有后端统一传 `EventEnvelope { publisher_id, sequence, topic, payload }` | 增加一层封装，但换来后端无关的事件模型 |
| ZMQ 事件 broker 双模式 | 有的部署想零依赖直连，有的部署想通过 broker 做集中转发/HA | 通过环境变量和 Discovery 在“直连模式 / broker 模式”间切换 | 初始化逻辑更复杂，但部署形态更灵活 |
| 多 broker 去重 | HA broker 会造成同一事件重复送达 | 用 `(publisher_id, sequence)` 作为幂等键，`DeduplicatingStream` 做 LRU 去重 | 需要额外缓存内存，但避免重复消费 |
| ZMQ `bail!` on protocol error | 帧数不等于 3 是调用方代码 bug，应快速暴露 | `bail!` 触发 `watch_task` 取消父 token，Runtime 关闭 | 激进的 fail-fast 策略，不适用于生产数据错误，仅用于协议层 |
| 租约绑定锁键 | 持锁进程崩溃会导致分布式锁死锁 | 所有锁键（读锁/写锁）绑定到进程主租约 | 进程崩溃后锁最多等待 10s 自动释放，有短暂不可用窗口 |
