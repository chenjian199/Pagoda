# `protocols` 模块设计

**源码**：`src/protocols.rs` · `src/protocols/annotated.rs` · `src/protocols/maybe_error.rs`

---

## 一、模块定位

`protocols` 模块是 Pagoda 管道中消息传输的**基础协议层**，承担三个核心职责：

1. **端点寻址**：`PortNameId` 以三段式 `namespace/servicegroup/name` 字符串唯一标识一个端点，并提供从多种字符串格式（`/`、`.` 分隔，含 `dyn://` scheme）解析的容错逻辑；
2. **流式响应信封**：`Annotated<R>` 是所有流式响应的通用信封，同一个泛型结构可以承载数据帧（`data`）、事件标记（`event`）、调试注解（`comment`）或错误（`error`），使单一 stream channel 支持多路复用的消息语义；
3. **错误可携带性**：`MaybeError` trait 统一约定"在 stream item 中内嵌错误"的接口，`Annotated<R>` 实现该 trait，使管道中任意节点可以向下游传递结构化错误而无需中断 stream。

---

## 二、文件结构与可见性

```
src/protocols.rs
  — pub type:    LeaseId（i64 别名）
  — pub const:   PORTNAME_SCHEME（"dyn://"）
  — pub struct:  ServiceGroup / PortNameId
  — pub mod:     annotated / maybe_error

src/protocols/
  ├── annotated.rs
  │   — pub trait:  AnnotationsProvider
  │   — pub struct: Annotated<R>
  │   — impl:       MaybeError for Annotated<R>
  └── maybe_error.rs
      — pub trait:  MaybeError
```

---

## 三、类型详解

---

### 3.1 `LeaseId` — 租约 ID 类型别名

**来源**：`src/protocols.rs`

```rust
pub type LeaseId = i64;
// etcd 租约 ID 的类型别名；在 discovery 模块中与租约续期 / 撤销配合使用。
// 使用具名别名而非裸 i64 提高代码可读性，并为未来类型替换提供单一修改点。
```

---

### 3.2 `PORTNAME_SCHEME` — 端点 URL 前缀常量

**来源**：`src/protocols.rs`

```rust
pub const PORTNAME_SCHEME: &str = "dyn://";
// PortNameId URL 格式的 scheme 前缀，如 "dyn://ns.comp.ep"。
// 包含 "://" 是为了在字符串拼接时省略中间步骤，
// 代价是与标准 URL scheme 定义（scheme 不含 "://"）存在语义差异，
// 调用方使用时需注意这一惯例。
```

---

### 3.3 `ServiceGroup` — 命名空间 + 组件名对

**来源**：`src/protocols.rs`

**设计意图**：在 discovery 等模块中，有时只需要 namespace + servicegroup 两段定位一个组件，而不需要具体的 portname。`ServiceGroup` 提供了这个二段组合的轻量表示，与 `PortNameId` 的三段表示形成体系配合。

```rust
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct ServiceGroup {
    pub name:      String,   // 组件名，如 "worker"
    pub namespace: String,   // 所属命名空间，如 "llm"
}
```

---

### 3.4 `PortNameId` — 三段式端点标识符

**来源**：`src/protocols.rs`

**设计意图** Pagoda 的端点通过 `namespace / servicegroup / name` 三段路径唯一寻址，类似于 Kubernetes 的 `namespace/resource` 模型。将三字段结构化（而非裸 String）有三个好处：（1）在 PartialEq 实现中可以与 `&[&str; 3]` / `Vec<&str>` 直接比较，测试代码更简洁；（2）`Display` 实现提供标准化字符串形式；（3）`From<&str>` 中的容错解析对只提供一个或两个段的短路径做 fallback 填充默认值，避免调用方写大量 `split` 逻辑。

```rust
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct PortNameId {
    pub namespace: String,   // 默认值："NS"
    pub servicegroup: String,   // 默认值："C"
    pub name:      String,   // 默认值："E"
}
```

**实现的 trait**：

| Trait | 来源 | 实现细节 |
| --- | --- | --- |
| `Display` | 手写 | `"{namespace}/{servicegroup}/{name}"` |
| `Default` | 手写 | 三个字段分别填充 `"NS"` / `"C"` / `"E"` |
| `From<&str>` | 手写 | 见下方"解析规则" |
| `FromStr` | 手写 | 委托 `From<&str>`，错误类型为 `Infallible` |
| `PartialEq<Vec<&str>>` | 手写 | 长度必须为 3，逐字段比较 |
| `PartialEq<[&str; 3]>` | 手写 | 逐字段比较 |
| 反向 `PartialEq` | 手写 | 允许 `vec == port_id` 写法 |

**自身方法**：

```rust
impl PortNameId {
    pub fn as_url(&self) -> String
    // 返回 URL 格式字符串，如 "dyn://ns.comp.ep"（段间用 '.' 而非 '/'）
    // 注意：as_url() 使用 '.' 分隔，但 Display 使用 '/'；
    // From<&str> 同时支持 '.' 和 '/' 分隔，两种格式均可解析回来。
}
```

**`From<&str>` 解析规则**：

1. 先去掉 `"dyn://"` 前缀（如果存在）
2. 去除首尾空格、`/`、`.`
3. 以 `.` 或 `/` 为分隔符分割，过滤空段
4. 按段数填充：

| 段数 | namespace | servicegroup | name |
| --- | --- | --- | --- |
| 0 | `"NS"` | `"C"` | `"E"` |
| 1 | `"NS"` | 第 1 段 | `"E"` |
| 2 | 第 1 段 | 第 2 段 | `"E"` |
| ≥3 | 第 1 段 | 第 2 段 | 第 3 段及后续段用 `'_'` 连接 |

```
"servicegroup"                        → ["NS", "servicegroup", "E"]
"namespace.servicegroup"              → ["namespace", "servicegroup", "E"]
"namespace/servicegroup/portname"     → ["namespace", "servicegroup", "portname"]
"namespace.servicegroup.ep.a.b"       → ["namespace", "servicegroup", "ep_a_b"]
"dyn://ns/cp/ep"                   → ["ns", "cp", "ep"]
```

---

### 3.5 `MaybeError` trait — 内嵌错误协议接口

**来源**：`src/protocols/maybe_error.rs`

**设计意图**：在 Pagoda 的 stream 协议中，管道节点之间通过异步 channel 传递 `Annotated<R>` 序列。当某个节点遇到错误时，不能直接让 stream 结束（下游可能还有未读数据），也不能用 `Result<Annotated<R>, Error>` 包装（会与 channel 错误混淆）。`MaybeError` 定义了"在 item 内部携带错误"的最小接口——`from_err` 构造一个错误态 item，`err` 检查并提取错误——使任意 stream item 类型都可以遵循这个约定，而无需了解 `Annotated` 的具体字段结构。

```rust
pub trait MaybeError {
    fn from_err(err: impl std::error::Error + 'static) -> Self;
    // 将任意 std::error::Error 封装为该类型的错误实例。
    // err 会被转换为 PagodaError（Box<dyn Error> 包装），保留原始错误链。

    fn err(&self) -> Option<PagodaError>;
    // 若当前实例表示错误状态，返回 Some(PagodaError)；否则返回 None。
    // is_ok / is_err 的 blanket 实现均基于此方法。

    fn is_ok(&self) -> bool { !self.is_err() }   // blanket，可 override
    fn is_err(&self) -> bool { self.err().is_some() }  // blanket，可 override
}
```

**已实现的类型**：`Annotated<R>`（`annotated.rs` 中实现）。

---

### 3.6 `AnnotationsProvider` trait — 注解读取接口

**来源**：`src/protocols/annotated.rs`

**设计意图**：`Annotated<R>` 的 `comment` 字段存储一组字符串注解，主要用于调试和性能剖析（如打点时间戳、路由路径）。`AnnotationsProvider` 将注解的读取操作抽象为 trait，使管道中的节点可以以多态方式检查注解，而不需要知道 item 的具体类型。`has_annotation` 提供了带默认实现的便捷检查方法，减少调用方的样板代码。

```rust
pub trait AnnotationsProvider {
    fn annotations(&self) -> Option<Vec<String>>;
    // 返回注解列表（comment 字段的内容）；无注解时返回 None。

    fn has_annotation(&self, annotation: &str) -> bool {
        self.annotations()
            .map(|annotations| annotations.iter().any(|a| a == annotation))
            .unwrap_or(false)
    }
    // 默认实现：检查注解列表中是否包含指定字符串。
    // 调用方示例：if item.has_annotation("ttft") { ... }
}
```

---

### 3.7 `Annotated<R>` — 流式响应通用信封

**来源**：`src/protocols/annotated.rs`

**设计意图**：Pagoda 的推理 stream 需要在同一个 channel 中传输三种语义不同的 item：

| item 类型 | 触发条件 | 如何识别 |
| --- | --- | --- |
| 数据帧 | 正常响应 token | `data.is_some()`, `event.is_none()` |
| 错误帧 | 管道节点失败 | `event == Some("error")` |
| 注解帧 | 调试 / 性能打点 | `event.is_some()` 且 `event != "error"` |

使用单一泛型结构而非 enum 的原因：`event` 字段的字符串值允许开放扩展（新增事件类型不需要修改 enum variant），同时 `skip_serializing_if = "Option::is_none"` 使线上传输的 JSON 只包含非空字段，降低带宽开销。

```rust
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Annotated<R> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<R>,
    // 响应数据；None 表示这是事件帧或错误帧

    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    // 可选的帧 ID，用于请求追踪或关联

    #[serde(skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
    // 事件类型标记：None = 数据帧，"error" = 错误帧，其他值 = 自定义注解帧

    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<Vec<String>>,
    // 注解内容：注解帧时存放 JSON 序列化的注解值；错误帧时存放备用错误描述

    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<PagodaError>,
    // 结构化错误：错误帧时优先使用此字段（优先级高于 comment）
}
```

**构造方法**：

```rust
impl<R> Annotated<R> {

    pub fn from_data(data: R) -> Self
    // 构造数据帧：data = Some(data)，其余字段为 None。
    // 最常用的构造路径，对应推理 stream 中的正常 token 帧。

    pub fn from_error(error: String) -> Self
    // 构造错误帧：event = Some("error")，error 字段为空，错误信息通过 comment 携带。
    // 注意：不如 from_err（通过 MaybeError trait）精确——推荐使用 MaybeError::from_err
    // 以保留完整的 PagodaError 结构。

    pub fn from_annotation<S: Serialize>(
        name: impl Into<String>,
        value: &S,
    ) -> Result<Self, serde_json::Error>
    // 构造注解帧：event = Some(name)，comment = Some(vec![serde_json::to_string(value)?])。
    // 用于在 stream 中插入调试信息（如 TTFT 时间戳、路由决策）。

    pub fn ok(self) -> Result<Self, String>
    // 若当前帧不是错误帧，返回 Ok(self)；
    // 若是错误帧，依次检查 error 字段 → comment 字段，返回 Err(错误描述)。
    // 用于在接收端快速展开检查：stream.map(|item| item.ok()).try_collect()。

    pub fn is_ok(&self) -> bool
    pub fn is_err(&self) -> bool
    pub fn is_event(&self) -> bool
    pub fn is_error(&self) -> bool
    // 状态检查便捷方法：
    // is_event():  event.is_some()（含错误帧）
    // is_error():  event == Some("error")（仅错误帧，与 is_err() 语义等同）
    // is_ok():     !is_error()
    // is_err():    is_error()

    pub fn transfer<U: Serialize>(self, data: Option<U>) -> Annotated<U>
    // 保留 id / event / comment / error，替换 data 字段类型。
    // 用于管道节点将上游 item 的元信息（注解、错误）转发给下游，同时替换数据类型。

    pub fn map_data<U, F>(self, transform: F) -> Annotated<U>
    where F: FnOnce(R) -> Result<U, String>
    // 对 data 字段应用变换：成功时产生新数据帧，失败时产生错误帧（通过 from_error）。
    // 注意：仅变换 data 字段，其余字段（event / comment / error）被丢弃；
    // 若需保留元信息，应先调用 transfer 再手动处理。

    pub fn into_result(self) -> Result<Option<R>>
    // 将信封拆解为标准 Result：
    // - data 帧 → Ok(Some(data))
    // - 空帧（data=None, event=None）→ Ok(None)
    // - 错误帧 → Err(anyhow::Error)（依次从 error 字段 → comment 字段提取消息）
}
```

**`MaybeError` 实现**（`Annotated<R: for<'de> Deserialize<'de>>`）：

```rust
impl<R> MaybeError for Annotated<R>
where R: for<'de> Deserialize<'de>
{
    fn from_err(err: impl std::error::Error + 'static) -> Self
    // 构造错误帧：event = Some("error")，error = Some(PagodaError::from(Box::new(err)))。
    // 与 from_error(String) 的区别：此方法保留完整的 PagodaError 结构（含错误链），
    // from_error 仅以字符串作为 comment，精度较低。

    fn err(&self) -> Option<PagodaError>
    // 若是错误帧，依次返回 error 字段 → comment 拼接字符串 → 默认 "unknown error"；
    // 非错误帧返回 None。
}
```

---

## 四、设计决策

### D-01：`From<&str>` 中的容错解析（段数不足时填充默认值）

`PortNameId::from` 对只有 1 段或 2 段的字符串使用默认值填充缺失字段，而非返回 `Err`。这一设计选择的背景是：在早期快速开发阶段，许多地方只传递 servicegroup 名称作为路由键，强制三段格式会产生大量临时适配代码。容错解析把这种"不完整"的使用模式纳入协议本身，代价是调用方可能无意间依赖了默认值（`"NS"` / `"C"` / `"E"`），调试时才发现路由不符预期。

潜在改进：当输入段数不足三段时打印 `warn` 日志（当前不打），使问题更早暴露。

### D-02：`Annotated<R>` 使用 `Option` 字段而非 enum variant

流式响应信封的另一个常见设计是使用 enum：

```rust
enum StreamItem<R> {
    Data(R),
    Event { name: String, payload: String },
    Error(PagodaError),
}
```

Pagoda 选择 struct + Option 字段的原因：（1）序列化格式对外兼容性更好（JSON 结构固定，不依赖 serde enum tag）；（2）`skip_serializing_if = "Option::is_none"` 使传输负载最小；（3）`event` 字段的开放字符串值允许在不修改类型定义的前提下增加新事件类型。代价是类型系统无法静态保证"不会同时设置 data 和 error"，需要调用方通过约定维护这一不变量。

### D-03：`MaybeError` 与 `Annotated::from_error` 并存

`Annotated<R>` 提供了两条错误构造路径：`from_error(String)` 和 `MaybeError::from_err(impl Error)`。前者是历史遗留 API（错误以字符串存入 comment），后者是更精确的新 API（错误以 `PagodaError` 存入 error 字段）。两者并存是为了向后兼容，新代码应优先使用 `MaybeError::from_err`。

---

## 五、模块依赖

**`protocols` 使用**：

```
serde          — Serialize / Deserialize（PortNameId / ServiceGroup / Annotated<R> 的序列化）
serde_json     — to_string()（from_annotation 中序列化注解值）
anyhow         — Result / anyhow!（into_result 的错误路径）
crate::error   — PagodaError（Annotated::error 字段类型；MaybeError::err 返回类型）
```

**`protocols` 被使用**：

```
pipeline/                — Annotated<R> 作为所有管道 stream 的 item 类型
pipeline/addressed_push_router.rs — PortNameId 作为路由目标
pipeline/work_handler.rs — MaybeError::from_err 构造错误帧传递给下游
servicegroup/portname.rs    — PortNameId 标识本端点的寻址信息
discovery/               — LeaseId 用于 etcd 租约管理
llm/frontend.rs          — Annotated<R> 包装推理响应流
llm/http_service.rs      — 拆解 Annotated<R> 为 OpenAI SSE 格式
bindings/python/rust/lib.rs — PortNameId 暴露为 Python 类型（parse / as_url）
```

---

## 七、补充：`ServiceGroup` 与 `PortNameId` 的协议层角色

除了 `Annotated<R>` 和 `MaybeError` 之外，当前 `protocols.rs` 里还有两个基础协议类型值得单独强调：

1. `ServiceGroup`
    用于在网络消息里携带 `namespace + servicegroup` 这组二段标识；它是协议层轻量对象，不等同于运行时里的 `crate::servicegroup::ServiceGroup` 服务模型对象。
2. `PortNameId`
    用于把 `namespace / servicegroup / name` 三段逻辑端点标识收敛成统一类型，并提供 `Display`、`Default`、`From<&str>`、`FromStr`、`Hash` 等协议与容错能力。

其中 `as_url()` 的输出格式仍然是：

```text
dyn://namespace.servicegroup.portname
```

也就是：

- 外层有 `dyn://` 前缀；
- 三段内部使用 `.` 分隔；
- 而 `Display` 形式则使用 `/` 分隔。

这是一个刻意保留的双表示策略：输入解析允许同时接受 `.` 和 `/` 两种风格，输出则统一走固定格式。

---

## 八、补充：`Annotated<R>` 的实际方法语义

结合当前实现，有几条方法行为需要额外写清楚：

1. `from_error(error: String)` 当前会把 `error` 字段直接设为 `Some(PagodaError::msg(error))`，而不是只把字符串塞进 `comment`；
2. `ok(self)` 在判断到 `event == "error"` 后，会优先从 `error` 字段取结构化错误，再回退到 `comment`；
3. `map_data()` 在变换成功时会保留原有的 `id / event / comment / error` 元信息，不会把这些字段丢掉；
4. `into_result()` 的返回语义是：
    - `Some(data)` -> `Ok(Some(data))`
    - 错误帧 -> `Err(anyhow::Error)`
    - 非错误且无数据 -> `Ok(None)`

这几条很重要，因为它们决定了 `Annotated<R>` 不只是一个“附带注释的 payload 容器”，而是 runtime 里真正被拿来承载数据帧、事件帧和错误帧的统一流协议外壳。

---

## 九、补充：`MaybeError` 的一进一出语义

`MaybeError` 的两个核心函数可以直接理解成两个方向：

- `from_err(...) -> Self`：把外部 Rust 错误包装进当前类型；
- `err(&self) -> Option<PagodaError>`：把当前对象内部可能携带的错误再提取出来。

在 `Annotated<R>` 上，这套协议的效果是：

1. 上游节点可以把普通 `std::error::Error` 转成一帧标准错误帧；
2. 下游节点则统一用 `is_err()` / `err()` 判断这帧是不是错误、以及错误内容是什么；
3. 因为错误是“帧内携带”的，而不是 `Result<StreamItem, E>` 这种外层返回值，所以整条流不必在第一个错误点立刻中止。

这也是它特别适合流式协议的原因：错误可以作为流中的一个事件存在，而不是被迫升级成“整条流已经崩了”。
