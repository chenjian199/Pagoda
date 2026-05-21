# `error` 模块设计文档

**源码位置**：`lib/runtime/src/error.rs`（约 496 行）

---

## 一、设计背景与模块职责

Pagoda 是一个分布式系统，错误会跨越多个边界传播：引擎内部错误 → Worker 节点 → 通过网络序列化传输 → Router 节点 → 最终到达用户。在这条链路上，错误处理面临三个挑战：

1. **错误的语义决策**：路由层需要根据错误类型决定策略——`CannotConnect` 应该重试其他 Worker，`InvalidArgument` 不应重试（重试也是同样错误），`Cancelled` 表示客户端主动取消无需报告。若所有错误都用 `anyhow::Error` 或字符串表示，路由层无法可靠地区分这些情况，只能用正则匹配错误消息，极不可靠。

2. **错误的跨网络传输**：Pagoda 的 Worker 和 Router 通过 TCP/NATS 通信，错误信息需要序列化传输。标准库的 `std::io::Error`、`anyhow::Error` 都不支持序列化，无法直接跨网络边界传递。

3. **错误的来源追踪**：调试分布式系统时，需要知道错误链：Router 层的"连接失败"是因为 Worker 层的"引擎崩溃"还是因为"网络超时"？标准的错误链（`Error::source()`）机制提供了这个能力，但需要在整条链路上保持一致的实现。

`error` 模块提供了 `PagodaError` 作为框架统一的错误类型，一次性解决这三个问题：可分类（`ErrorType` 枚举）、可序列化（`serde` 支持）、可链式追踪（`source()` + `caused_by` 字段）。

---

## 二、`ErrorType` 枚举

### 为什么需要独立的错误类型枚举

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorType {
    Unknown,
    InvalidArgument,
    CannotConnect,
    Disconnected,
    ConnectionTimeout,
    Cancelled,
    Backend(BackendError),
}
```

枚举的变体设计与 gRPC Status Code 和 HTTP 状态码的思路一致：给错误一个机器可读的分类，让消费方通过 `match error_type { ... }` 做策略决策，而非解析错误消息字符串。

**变体设计原则**：每个变体对应一类需要不同处理策略的错误情形：

- `Unknown`：无法分类的错误，只能记录日志并向上传播，不做特殊处理；
- `InvalidArgument`：请求本身有问题，重试无意义，应直接返回给用户；
- `CannotConnect`：目标 Worker 暂时不可达，可以尝试路由到其他 Worker；
- `Disconnected`：连接中途断开，可能是 Worker 重启，需要重建连接；
- `ConnectionTimeout`：请求超时，可以重试，但需要调整超时策略；
- `Cancelled`：客户端主动取消，无需向上报告为错误，静默清理资源；
- `Backend(BackendError)`：引擎内部错误，用嵌套枚举进一步细分。

**`#[derive(Copy)]`**：`ErrorType` 只包含枚举变体（无 String 等堆上数据，`BackendError` 也是纯 Copy 枚举），Copy 使得在 `match` 表达式后、比较操作中无需显式 clone，使用更自然。序列化/反序列化（`serde`）支持跨网络传输。

---

### `BackendError` 枚举

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackendError {
    Unknown,
    InvalidArgument,
    CannotConnect,
    Disconnected,
    ConnectionTimeout,
    Cancelled,
    EngineShutdown,
    StreamIncomplete,
}
```

**为什么将后端错误单独列为一个嵌套枚举**

后端（推理引擎）的错误与网络/基础设施错误具有不同的语义，即使表面上名称相似：

- `ErrorType::CannotConnect`：框架层无法连接到某个 Worker（网络问题）；
- `BackendError::CannotConnect`：Worker 内部无法连接到推理引擎（本地进程问题）。

前者路由层应重试其他 Worker，后者说明这个 Worker 整体有问题，应将其标记为不健康。若不区分来源，路由层无法正确处理。

`Backend(BackendError)` 这种嵌套设计在 `Display` 实现中表现为 `"BackendCannotConnect"`，使日志中的错误类型字符串既包含"来源"（Backend）又包含"类型"（CannotConnect），便于日志搜索和告警规则编写。

**`EngineShutdown`** 和 **`StreamIncomplete`** 是引擎特有的两种错误，无法归入通用的 `ErrorType`：
- `EngineShutdown`：引擎进程崩溃或主动关闭，Worker 需要重启引擎；
- `StreamIncomplete`：流中途终止（引擎 mid-stream drop），调用方已开始消费但未结束，需要特殊处理（通知用户响应不完整）。

---

## 三、`PagodaError` 结构体

### 为什么需要独立的错误结构体

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PagodaError {
    error_type: ErrorType,
    message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    caused_by: Option<Box<PagodaError>>,
}
```

**`error_type: ErrorType`**：机器可读的错误分类（见上节）。`ErrorType` 是 `Copy + Serialize`，直接存储值，无需包裹。

**`message: String`**：人类可读的错误消息，用于日志和向用户展示。与 `error_type` 分离：`error_type` 供代码逻辑判断，`message` 供人类阅读，两者职责不重叠。

**`caused_by: Option<Box<PagodaError>>`**：错误链的 Rust 实现。选用 `Box<PagodaError>` 而非 `Box<dyn std::error::Error>` 的原因：

1. **序列化约束**：`Box<dyn std::error::Error>` 不支持 `serde`，无法序列化跨网络传输；`Box<PagodaError>` 是具体类型，完整支持 serde；
2. **保留类型信息**：若因果链中的某个错误本来是 `PagodaError`（有 `error_type` 信息），存为 `Box<dyn Error>` 会丢失 `error_type`，只剩 `Display` 字符串；存为 `Box<PagodaError>` 则完整保留。

**`#[serde(skip_serializing_if = "Option::is_none")]`**：序列化时若无因果链，不输出 `"caused_by": null` 字段，使 JSON 更紧凑，也使没有 cause 的简单错误和有 cause 的错误在序列化格式上有明显区别。

**`#[derive(Clone)]`**：Pagoda 的错误需要在多处传递（例如同时记录日志和返回给调用方）。标准库的 `std::io::Error` 不实现 `Clone`，使得 `Box<dyn Error>` 也无法 Clone。`PagodaError` 的所有字段（`ErrorType` 是 Copy，`String` 和 `Box<PagodaError>` 支持 Clone）都可以安全克隆，`#[derive(Clone)]` 是自然选择。

---

## 四、`PagodaError` 方法

### `PagodaError::msg(message)`

```rust
pub fn msg(message: impl Into<String>) -> Self {
    Self::builder().message(message).build()
}
```

最常见的使用场景：快速创建一个 `Unknown` 类型的错误。等价于 `PagodaError::builder().message(msg).build()`，但无需链式调用。类比 `anyhow::anyhow!("...")` 的便利性，适合一次性错误创建。

### `PagodaError::builder() -> PagodaErrorBuilder`

使用 builder 模式构造更复杂的错误（有类型、有 cause）。Builder 模式的优势：参数可选（不需要提供所有参数），链式调用可读性强，扩展新字段不破坏已有调用方。

---

## 五、`From` 转换：与 Rust 标准错误生态兼容

`PagodaError` 实现了两个 `From` 转换，使其可以从任意 `std::error::Error` 转换而来：

### `From<&'a (dyn std::error::Error + 'static)>` —— 递归转换

```rust
impl<'a> From<&'a (dyn std::error::Error + 'static)> for PagodaError {
    fn from(err: &'a (dyn std::error::Error + 'static)) -> Self {
        if let Some(pagoda_err) = err.downcast_ref::<PagodaError>() {
            return pagoda_err.clone();
        }
        Self {
            error_type: ErrorType::Unknown,
            message: err.to_string(),
            caused_by: err.source().map(|s| Box::new(PagodaError::from(s))),
        }
    }
}
```

**设计决策分析**：

1. **优先 downcast**：若错误已经是 `PagodaError`，Clone 返回，保留原有的 `error_type` 和 `message`，不降级为 `Unknown`；
2. **递归转换因果链**：`err.source()` 返回标准错误链的下一个节点，递归转换使整条因果链都变成 `PagodaError`，序列化时因果链完整保留；
3. **非 PagodaError 节点包装为 Unknown**：只保留 `to_string()` 作为 `message`，丢失了原始类型信息，但这已是从 `dyn Error` 能提取的最大信息量。

### `From<Box<dyn std::error::Error + 'static>>` —— 所有权转换

```rust
impl From<Box<dyn std::error::Error + 'static>> for PagodaError {
    fn from(err: Box<dyn std::error::Error + 'static>) -> Self {
        match err.downcast::<PagodaError>() {
            Ok(pagoda_err) => *pagoda_err,   // 解包，无需 clone
            Err(err) => PagodaError::from(&*err as &(dyn std::error::Error + 'static)),
        }
    }
}
```

**为什么需要独立的 `From<Box<...>>`**：持有 `Box<dyn Error>` 时，可以尝试 `downcast::<PagodaError>()` 取出所有权，避免 clone——比引用版本效率更高，`PagodaError` 无需在堆上重新分配。若 downcast 失败（不是 `PagodaError`），退回引用转换路径。

---

## 六、`PagodaErrorBuilder`

```rust
#[derive(Default)]
pub struct PagodaErrorBuilder {
    error_type: Option<ErrorType>,
    message: Option<String>,
    caused_by: Option<Box<PagodaError>>,
}
```

**为什么每个字段是 `Option`**：Builder 的设计原则是"所有参数可选"。`build()` 为未设置的字段提供默认值（`error_type` 默认 `Unknown`，`message` 默认空字符串，`caused_by` 默认 `None`），使调用方可以只设置关心的字段。

**`cause(impl std::error::Error + 'static)`**：接受任意实现 `std::error::Error` 的类型，内部转换为 `PagodaError`（保留 PagodaError 类型信息，或包装为 Unknown）。这使调用方可以直接传入原始错误，无需手动转换：

```rust
let io_err = std::io::Error::other("disk full");
PagodaError::builder()
    .error_type(ErrorType::Backend(BackendError::Unknown))
    .message("failed to write checkpoint")
    .cause(io_err)
    .build();
```

---

## 七、`match_error_chain` 工具函数

### 为什么需要

```rust
pub fn match_error_chain(
    err: &(dyn std::error::Error + 'static),
    match_set: &[ErrorType],
    exclude_set: &[ErrorType],
) -> bool
```

路由层在处理错误时需要做复杂的策略判断：

- "这个错误链中是否包含 `Disconnected`"（决定是否重试）；
- "这个错误链中包含 `Cancelled` 但不包含 `Backend(EngineShutdown)`"（区分主动取消和引擎崩溃中的取消）。

这种"在错误链中匹配并排除特定类型"的逻辑如果在每个调用方重复实现，会产生大量样板代码且容易出错（遍历 `source()` 链需要正确的循环逻辑，还需要处理 downcast）。`match_error_chain` 将这个通用模式封装成一个复用的工具函数。

**`exclude_set` 的优先级高于 `match_set`**：一旦发现任何 exclude 类型立即返回 `false`，即使后面有 match 类型。设计逻辑：排除条件是"禁止触发某策略的否决权"，具有最高优先级。例如"有 Disconnected 但没有 Cancelled"表达"意外断开（重试），但不包括主动取消后的断开（不重试）"。

**遍历实现**：

```rust
let mut current: Option<&(dyn std::error::Error + 'static)> = Some(err);
while let Some(e) = current {
    if let Some(pagoda_err) = e.downcast_ref::<PagodaError>() {
        // 检查 exclude 和 match
    }
    current = e.source();
}
```

`downcast_ref::<PagodaError>()` 尝试将每个节点转换为 `PagodaError`，失败则跳过（该节点是非 PagodaError 的标准错误，没有 `ErrorType` 信息，不参与判断）。遍历直到链末尾（`source()` 返回 `None`）。

---

## 八、测试设计

测试覆盖以下关键路径：

**编译期 trait 约束验证**（`_` 常量中的函数）：

```rust
const _: () = {
    fn assert_stderror<T: std::error::Error>() {}
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}
    fn assert_static<T: 'static>() {}
    fn assert_all() {
        assert_stderror::<PagodaError>();
        assert_send::<PagodaError>();
        assert_sync::<PagodaError>();
        assert_static::<PagodaError>();
    }
};
```

这些空函数在编译时静态断言 `PagodaError` 满足所有必要的 trait bound。若未来某次修改（如引入 `Arc<Mutex<...>>`）破坏了 `Sync` 约束，编译会直接报错，而非在运行时才发现问题。

**序列化往返测试**（`test_serialization_roundtrip`）：验证带有完整因果链的 `PagodaError` 序列化为 JSON 再反序列化后，所有字段值保持一致。这是跨网络传输正确性的核心保证。

**Display 测试**（`test_display_shows_only_current_error`）：验证 `Display` 只显示当前层错误（`"Unknown: operation failed"`），不自动展开因果链。这符合 Rust 的错误 Display 惯例（`source()` 链由调用方决定是否展开），避免日志中出现超长嵌套错误消息。

**`From` 转换所有权测试**（`test_from_boxed_takes_ownership_of_pagoda_error`）：验证 `From<Box<dyn Error>>` 对 `PagodaError` 取所有权而非 clone，通过检查结果值与原始值相同来间接验证（直接验证内存地址需要 unsafe）。
