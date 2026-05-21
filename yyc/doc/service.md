# `service` 模块设计文档

**源码位置**：`lib/runtime/src/service.rs` · `lib/runtime/src/servicegroup/service.rs`

---

## 一、设计背景

Pagoda 的 Worker 进程通过 NATS 内置服务协议暴露自身服务信息与统计数据。Router、分布式运行时和健康检查相关逻辑需要定期查询集群中所有活跃 Worker，获得它们的端点列表和处理指标。

若直接使用 `async_nats` 的底层 API，调用方需要自己处理以下细节：

- 广播 `$SRV.STATS.<service_name>` 请求并收集多个响应
- 在超时窗口内结束收集
- 跳过空 payload
- 反序列化服务 JSON
- 容忍部分节点返回异常格式
- 对单个 subject 执行 request-reply

`service` 模块将这些能力封装为 `ServiceClient`、`ServiceSet`、`ServiceInfo`、`PortnameInfo` 和 `NatsStatsMetrics`，同时在 [lib/runtime/src/servicegroup/service.rs](lib/runtime/src/servicegroup/service.rs) 中提供构建 NATS service 的辅助入口。

---

## 二、核心类型定义

### `ServiceClient`

```rust
pub struct ServiceClient {
    nats_client: nats::Client,
}
```

`ServiceClient` 是服务发现与单播请求的统一入口，内部只持有一个 `nats::Client`。

### `ServiceSet`

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceSet {
    services: Vec<ServiceInfo>,
}
```

`ServiceSet` 表示一次服务收集得到的所有服务实例集合。

### `ServiceInfo`

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceInfo {
    pub name: String,
    pub id: String,
    pub version: String,
    pub started: String,
    pub Portnames: Vec<PortnameInfo>,
}
```

该结构直接映射 NATS `$SRV.STATS.<service_name>` 的单个服务响应。

### `PortnameInfo`

```rust
#[derive(Debug, Clone, Serialize, Deserialize, Dissolve)]
pub struct PortnameInfo {
    pub name: String,
    pub subject: String,
    #[serde(flatten)]
    pub data: Option<NatsStatsMetrics>,
}
```

每个 `ServiceInfo` 下会携带多个端点；统计字段通过 `#[serde(flatten)]` 与端点元数据处于同一 JSON 层级。

### `NatsStatsMetrics`

```rust
#[derive(Debug, Clone, Serialize, Deserialize, Dissolve)]
pub struct NatsStatsMetrics {
    pub average_processing_time: u64,
    pub last_error: String,
    pub num_errors: u64,
    pub num_requests: u64,
    pub processing_time: u64,
    pub queue_group: String,
    pub data: serde_json::Value,
}
```

这是一份对 NATS service stats 响应的本地建模，主要为了便于反序列化标准字段和承载 worker 自定义统计数据。

---

## 三、`ServiceClient` 的方法定义

### 构造函数 `new`

```rust
pub fn new(nats_client: nats::Client) -> Self
```

创建一个 `ServiceClient`，将传入的 NATS 客户端保存为内部字段。该类型本身很轻，只承担协议封装责任，不维护额外缓存。

### 单播请求 `unary`

```rust
pub async fn unary(
    &self,
    subject: impl Into<String>,
    payload: impl Into<Bytes>,
) -> Result<Message>
```

`unary()` 是对 NATS request-reply 模式的轻量封装。它通过底层 `self.nats_client.client().request(...)` 向单个 subject 发送请求，并等待一个回复消息。

这个接口适用于定向操作，而不是广播式服务发现。

### 广播收集 `collect_services`

```rust
pub async fn collect_services(
    &self,
    service_name: &str,
    timeout: Duration,
) -> Result<ServiceSet>
```

`collect_services()` 用于向 `$SRV.STATS.<service_name>` 发起服务统计广播，并在给定超时窗口内收集所有响应，最终返回 `ServiceSet`。

源码中的关键逻辑包括：

- 通过 `self.nats_client.scrape_service(service_name).await?` 获取响应流。
- 对 `timeout.is_zero()` 和 `timeout > 10s` 做 warning 日志提示。
- 使用 `utils::stream::until_deadline(sub, deadline)` 包装流，在截止时间到达时自然结束。
- 跳过空 payload，并记录 `trace` 日志。
- 对每条非空消息执行 `serde_json::from_slice::<ServiceInfo>`。
- 若某个响应解析失败，只记 `debug` 日志，不中断整个收集。

这种设计的核心目标是让服务发现具备更强的容错性和滚动升级兼容性。

### 为什么按截止时间收集响应

NATS 服务统计是广播语义，调用方通常不知道当前会有多少实例回复，因此不能按固定条数收集。按截止时间收集可以在“完整性”和“调用延迟”之间取得平衡。

### 为什么空消息不报错

源码注释已说明：当 worker 侧 KV metrics 尚未启动时，NATS 可能返回空 payload。该状态是启动阶段的瞬时现象，不应导致整次服务发现失败。

---

## 四、`ServiceSet` 的方法定义

### 扁平化端点 `into_Portnames`

```rust
pub fn into_Portnames(self) -> impl Iterator<Item = PortnameInfo>
```

该方法消费整个 `ServiceSet`，并把所有 `ServiceInfo` 下的 `Portnames` 扁平化为单个迭代器。它适合“只关心所有活跃端点”的调用方，避免额外克隆。

### 只读访问服务数组 `services`

```rust
pub fn services(&self) -> &[ServiceInfo]
```

该方法返回内部 `services` 切片，使调用方仍能在服务粒度上访问 `name`、`id`、`version`、`started` 等元信息。

---

## 五、`PortnameInfo` 与 `NatsStatsMetrics` 的方法定义

### `PortnameInfo::id`

```rust
pub fn id(&self) -> Result<i64>
```

该方法从 `subject` 的最后一个 `-` 分段中提取十六进制实例 ID，并转换为 `i64`。

实现逻辑是：

- 先按 `-` 分割 subject
- 取最后一段
- 使用 `i64::from_str_radix(id, 16)` 解析

若 subject 中不存在 `-` 分段，或最后一段不是合法十六进制字符串，则返回错误。

这个 ID 抽取能力通常用于路由与实例识别。

### `NatsStatsMetrics::decode`

```rust
pub fn decode<T: for<'de> Deserialize<'de>>(self) -> Result<T>
```

`decode()` 将 `NatsStatsMetrics.data` 中保存的动态 JSON 进一步反序列化为调用方指定的具体类型 `T`。

这使标准 NATS 指标字段与业务自定义指标字段可以共存：

- 标准字段保留在 `NatsStatsMetrics`
- 自定义字段通过 `decode::<T>()` 转成强类型结构

---

## 六、字段语义与协议映射

### `ServiceInfo` 字段

- `name`：服务名称，例如 `pagoda_backend`
- `id`：服务实例唯一 ID
- `version`：服务版本号
- `started`：服务启动时间字符串
- `Portnames`：该服务实例暴露的端点列表

源码将 `started` 保持为字符串，而不是时间类型，原因是这里主要用于透传、展示和日志，而不是时间运算。

### `PortnameInfo` 字段

- `name`：端点名称
- `subject`：NATS subject
- `data`：端点统计信息；可能为空

这里把 `data` 设计为 `Option<NatsStatsMetrics>`，是为了容忍部分端点在刚启动时还未完整上报统计字段。

### `NatsStatsMetrics` 字段

- `average_processing_time`：平均处理时长，单位纳秒
- `last_error`：最后一次错误字符串
- `num_errors`：错误数
- `num_requests`：请求数
- `processing_time`：累计处理时间，单位纳秒
- `queue_group`：NATS queue group
- `data`：自定义 JSON 统计数据

其中 `processing_time` 与 `average_processing_time` 的单位都来自 NATS 标准定义，为纳秒。

---

## 七、与 `utils::stream::until_deadline` 的协作

`collect_services()` 使用 `utils::stream::until_deadline` 包装订阅流，使其在 deadline 到达后自然返回 `None` 结束迭代。相比把整个收集流程包在 `tokio::time::timeout` 外层，这种写法更适合“在一段时间内尽可能多地收集响应”的语义。

---

## 八、NATS Service 构建辅助入口

除服务发现外，当前运行时还在 [lib/runtime/src/servicegroup/service.rs](lib/runtime/src/servicegroup/service.rs) 中提供了一个公开辅助函数，用于为组件创建 NATS service：

```rust
pub const PROJECT_NAME: &str = "Pagoda"
```

该常量用于构造默认 description 文本。

```rust
pub async fn build_nats_service(
    nats_client: &crate::transports::nats::Client,
    servicegroupnt: &ServiceGroup,
    description: Option<String>,
) -> anyhow::Result<NatsService>
```

`build_nats_service()` 的职责是：

- 从 `servicegroupnt.service_name()` 生成 NATS service name
- 若调用方未传 description，则自动生成默认描述文本
- 调用 `service_builder().description(...).start(...)` 启动 NATS service
- 将底层错误包装成 `anyhow` 错误返回

这个函数主要服务于 legacy NATS request plane，源码注释已经说明：待组件全部迁移到 TCP request plane 后，该入口计划移除。

---

## 九、补充：注册端与抓取端是两份实现

如果只看 [lib/runtime/src/service.rs](lib/runtime/src/service.rs)，很容易误以为 `service` 模块同时负责 NATS service 的创建与查询；但当前实现实际上拆成了两部分：

- [lib/runtime/src/servicegroup/service.rs](lib/runtime/src/servicegroup/service.rs) 负责把 servicegroup 启动成 NATS service；
- [lib/runtime/src/service.rs](lib/runtime/src/service.rs) 负责通过 `$SRV.STATS.<service_name>` 抓取这些 service 的 stats 响应，并做聚合与反序列化。

也就是说，`src/service.rs` 更准确地说是“**stats 抓取与聚合工具层**”，而不是完整的 NATS micro service client 封装。

---

## 十、补充：当前源码中的真实类型名

为保持本文现有叙述风格，前文沿用了 `PortnameInfo` / `Portnames` 这套写法；但和当前源码逐项对照时，需要补充两个事实：

1. [lib/runtime/src/service.rs](lib/runtime/src/service.rs) 里真实的结构体名是 `EndpointInfo`，对应字段名是 `portnames`；
2. `ServiceSet` 上真实的方法名是 `into_portnames(self)`，不是 `into_Portnames(self)`。

这意味着当前实现对外返回的语义已经是“portname 视角”，只是本文为了保持既有文档命名，没有整体改写前文段落。

同样地，当前源码中的 `ServiceInfo` 只反序列化以下字段：

- `name`
- `id`
- `version`
- `started`
- `portnames`

像旧版说明里常见的 `description`、`metadata`、`stats` 等字段，在当前 `ServiceInfo` 结构里都没有被建模。

---

## 十一、补充：`collect_services()` 的容错边界

除了前文已经提到的广播收集逻辑，当前实现还有几条值得单独写清楚的边界：

1. `timeout == 0` 和 `timeout > 10s` 都只会记录 warning，不会强行报错；
2. 收集窗口通过 `until_deadline(stream, now + timeout)` 自然结束，而不是按条数或固定响应数结束；
3. 单条响应反序列化失败时，只记 `debug` 日志并继续处理后续消息；
4. 空 payload 会被直接跳过，因为 worker 启动早期可能还在等待 metrics 准备完毕。

这组设计说明该接口追求的是“**尽可能收全可用响应**”，而不是“任何一个实例异常就让整次抓取失败”。

从调用语义上看，它更接近一次性巡检：

- 调用方发出一次 `$SRV.STATS.<service_name>` 请求；
- 在本地设定的时间窗口里尽量收集回复；
- 最后把这段时间内收到的有效 stats 响应整理成一个 `ServiceSet`。

---

## 十二、补充：注册端 `build_nats_service()` 的实际行为

当前注册逻辑位于 [lib/runtime/src/servicegroup/service.rs](lib/runtime/src/servicegroup/service.rs)，其实现边界可以概括成：

1. 根据 servicegroup 计算 `service_name`；
2. 若调用方未显式传入 `description`，则自动生成默认描述文本；
3. 调用 `service_builder().description(...).start(service_name, SERVICE_VERSION)` 启动 NATS service；
4. 返回启动后的 `NatsService` 句柄。
