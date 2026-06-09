# `timeline` 模块设计文档

**源码位置**：`lib/runtime/src/timeline/`（多文件模块）

---

## 一、设计背景

LLM 推理链路中，性能分析工具（如 NVIDIA Nsight Systems、华为 Ascend Insight 等）需要在时间线上标出有意义的区间（range），让性能工程师直观看到 `tokenize`、`prefill`、`decode` 等阶段的耗时分布，而不是面对一片无业务语义的底层内核调用。

`timeline` 模块的职责就是提供这一层**统一的时间线标注能力**。对外暴露的是不变的 timeline 语义（push/pop/range/name_thread）；底层则可以挂载不同的分析器后端（NVTX、华为 Ascend、以及其它厂商），做到**上层代码零改动切换分析后端**。

时间线标注在生产部署中通常不需要，且底层 C API 调用存在固定开销。因此 Pagoda 采用**两级门控**：编译期开关 + 运行时开关。未启用时，标注能力要么在编译期被完全消除，要么只保留一次极轻量的原子读判断。

---

## 二、文件结构（一个分析器一个文件）

```
lib/runtime/src/timeline/
├── mod.rs              # 模块入口：trait 定义、门控逻辑、公开宏
├── guard.rs            # TimelineRangeGuard — RAII 区间守护
├── backends/
│   ├── mod.rs          # 后端注册与 dispatch
│   ├── nvtx.rs         # NVIDIA NVTX 后端 (Nsight Systems)
│   ├── ascend.rs       # 华为 Ascend 后端 (Ascend Insight / msprof)
│   └── noop.rs         # 空后端：编译占位，所有调用均为空操作
└── README.md           # （可选）模块使用说明
```

**设计原则**：
- `mod.rs` 只包含 trait 定义、`AtomicBool` 门控、`init()`、以及四个公开宏——**不包含任何后端特定代码**。
- 每个后端独立一个文件，实现同一个 `TimelineBackend` trait。
- 新增后端只需添加一个新文件 + 在 `backends/mod.rs` 中注册一个 `#[cfg(feature = "...")]` 编译条件分支。
- 公开宏的签名和行为**永不改变**，业务代码零感知。

---

## 三、`TimelineBackend` trait — 统一后端契约

所有分析器后端实现同一个 trait，位于 `mod.rs`：

```rust
/// 时间线分析后端的统一抽象。
///
/// 每个后端实现者只需提供三个函数：
/// - `range_push` / `range_pop`：在线程局部栈上压入/弹出命名区间；
/// - `name_os_thread`：给 OS 线程赋予可读名称。
///
/// 这些函数假设调用方已经完成了"是否启用"的门控判断，
/// 因此后端实现不需要再检查 `TIMELINE_ENABLED`。
pub trait TimelineBackend: Send + Sync + 'static {
    /// 压入一个命名区间，返回一个不透明 id（后端用于匹配 push/pop）。
    fn range_push(name: &str) -> u64;

    /// 弹出最内层区间。
    fn range_pop();

    /// 给指定 OS 线程赋予可读名称。
    fn name_os_thread(tid: u32, name: &str);
}
```

**设计考量**：
- trait 方法均为关联函数（无 `&self`），因为底层 profiler API（NVTX、msprof 等）都是基于线程局部状态的 C 调用，不需要实例状态。
- `range_push` 返回 `u64` 作为 opaque id：某些后端（如 NVTX）确实返回 id 用于校验；不需要此 id 的后端直接返回 0 即可。
- trait 绑定 `Send + Sync + 'static`，确保可以在多线程运行时环境下安全持有后端句柄（尽管当前设计不需要持有实例）。

---

## 四、后端选择：编译期 feature gate

后端通过 Cargo feature 在编译期选定，不同 feature 之间**互斥**：

```toml
# Cargo.toml (runtime)
[features]
timeline = []              # 总开关：启用 timeline 模块（不含具体后端，默认选中 noop）
timeline-nvtx = ["timeline", "dep:cudarc"]
timeline-ascend = ["timeline"]
```

`backends/mod.rs` 中通过条件编译选择后端：

```rust
#[cfg(feature = "timeline-nvtx")]
mod nvtx;
#[cfg(feature = "timeline-nvtx")]
pub(crate) use nvtx::NvtxBackend as ActiveBackend;

#[cfg(feature = "timeline-ascend")]
mod ascend;
#[cfg(feature = "timeline-ascend")]
pub(crate) use ascend::AscendBackend as ActiveBackend;

// 默认：无具体后端选中时使用空后端
#[cfg(not(any(feature = "timeline-nvtx", feature = "timeline-ascend")))]
mod noop;
#[cfg(not(any(feature = "timeline-nvtx", feature = "timeline-ascend")))]
pub(crate) use noop::NoopBackend as ActiveBackend;
```

`mod.rs` 中的 `push_impl` / `pop_impl` / `name_current_thread_impl` 统一委托给 `ActiveBackend`：

```rust
#[cfg(feature = "timeline")]
mod backends;

#[inline(always)]
pub fn push_impl(name: &str) {
    #[cfg(feature = "timeline")]
    {
        if TIMELINE_ENABLED.load(Ordering::Relaxed) {
            backends::ActiveBackend::range_push(name);
        }
    }
}
```

这样每个后端的代码完全隔离在自己的文件中，新增后端不影响已有后端，也不会改动 `mod.rs` 的核心流程。

---

## 五、两级门控机制（不变）

### 第一级：Cargo feature gate

`timeline` feature 未启用时，所有 `#[cfg(feature = "timeline")]` 代码块在编译期消除。四个公开宏展开为 `{}`，不生成任何指令。

### 第二级：运行时环境变量

```rust
static TIMELINE_ENABLED: AtomicBool = AtomicBool::new(false);

pub fn init() {
    #[cfg(feature = "timeline")]
    {
        let enabled = std::env::var("PGD_ENABLE_RUST_TIMELINE")
            .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false);
        TIMELINE_ENABLED.store(enabled, Ordering::Relaxed);
    }
}
```

`Ordering::Relaxed` 原因：开关在进程启动时写入一次，后续只有读取，不承担跨线程同步职责。

---

## 六、公开 API（对外契约，保持不变）

### `init()`

`Runtime::new()` 自动调用，一般不需要手工初始化。

### `push_impl` / `pop_impl`

```rust
#[inline(always)]
pub fn push_impl(name: &str) { /* -> ActiveBackend::range_push */ }

#[inline(always)]
pub fn pop_impl() { /* -> ActiveBackend::range_pop */ }
```

### `name_current_thread_impl`

```rust
pub fn name_current_thread_impl(name: &str) {
    #[cfg(feature = "timeline")]
    {
        if TIMELINE_ENABLED.load(Ordering::Relaxed) {
            #[cfg(target_os = "linux")]
            let tid = unsafe { libc::syscall(libc::SYS_gettid) as u32 };
            backends::ActiveBackend::name_os_thread(tid, name);
        }
    }
}
```

### `TimelineRangeGuard`（`guard.rs`）

RAII 守护，构造时 push，`Drop` 时 pop。`timeline` feature 关闭时为零大小类型。

```rust
#[cfg(feature = "timeline")]
pub struct TimelineRangeGuard { active: bool }

#[cfg(not(feature = "timeline"))]
pub struct TimelineRangeGuard;
```

### 宏接口（不变）

```rust
let _r = pagoda_timeline_range!("preprocess.tokenize");

pagoda_timeline_push!("codec.encode");
// ...
pagoda_timeline_pop!();

pagoda_timeline_name_thread!("decode-worker-0");
```

---

## 七、各后端实现概要

### 7.1 NVTX 后端 (`backends/nvtx.rs`)

- **目标分析器**：NVIDIA Nsight Systems
- **底层依赖**：`cudarc::nvtx::result::range_push` / `range_pop` / `name_os_thread`
- **运行时依赖**：`libnvToolsExt.so`（来自 CUDA Toolkit 或 NVHPC）
- **编译**：`cargo build --features timeline-nvtx`
- **实现要点**：直接委托 `cudarc::nvtx`，无额外逻辑。

```rust
// backends/nvtx.rs (示意)
use cudarc::nvtx::result::{range_push, range_pop, name_os_thread};

pub struct NvtxBackend;

impl TimelineBackend for NvtxBackend {
    fn range_push(name: &str) -> u64 {
        range_push(name)
    }
    fn range_pop() {
        range_pop();
    }
    fn name_os_thread(tid: u32, name: &str) {
        name_os_thread(tid, name);
    }
}
```

### 7.2 华为 Ascend 后端 (`backends/ascend.rs`)

- **目标分析器**：Ascend Insight / msprof（华为昇腾性能分析工具链）
- **底层依赖**：`libmsprof.so` 的 C API，通过 FFI 绑定调用
- **运行时依赖**：`libmsprof.so`（来自 Ascend Toolkit）
- **编译**：`cargo build --features timeline-ascend`
- **实现要点**：
  - 通过 Rust FFI 声明 `extern "C"` 函数链接 `libmsprof.so` 中的标注接口；
  - 使用 `libloading` 延迟加载，避免在没有 Ascend 环境的节点上启动失败；
  - 若动态库加载失败，静默降级为空操作。

```rust
// backends/ascend.rs (示意)
use std::sync::OnceLock;
use std::ffi::c_char;

static MSPROF_FNS: OnceLock<Option<MsprofFnTable>> = OnceLock::new();

struct MsprofFnTable {
    range_push: unsafe extern "C" fn(*const c_char) -> u64,
    range_pop:  unsafe extern "C" fn(),
    name_thread: unsafe extern "C" fn(u32, *const c_char),
}

fn get_fns() -> Option<&'static MsprofFnTable> {
    MSPROF_FNS.get_or_init(|| {
        // libloading 加载 libmsprof.so，查找符号；失败返回 None
        None
    }).as_ref()
}

pub struct AscendBackend;

impl TimelineBackend for AscendBackend {
    fn range_push(name: &str) -> u64 {
        if let Some(fns) = get_fns() {
            let c_name = std::ffi::CString::new(name).unwrap();
            unsafe { (fns.range_push)(c_name.as_ptr()) }
        } else {
            0
        }
    }
    fn range_pop() {
        if let Some(fns) = get_fns() {
            unsafe { (fns.range_pop)() }
        }
    }
    fn name_os_thread(tid: u32, name: &str) {
        if let Some(fns) = get_fns() {
            let c_name = std::ffi::CString::new(name).unwrap();
            unsafe { (fns.name_thread)(tid, c_name.as_ptr()) }
        }
    }
}
```

### 7.3 空后端 (`backends/noop.rs`)

- **用途**：编译占位。当只启用了 `timeline` feature 但未选择任何具体后端时使用。
- **实现**：所有方法为空操作，`range_push` 返回 0。

```rust
// backends/noop.rs (示意)
pub struct NoopBackend;

impl TimelineBackend for NoopBackend {
    fn range_push(_name: &str) -> u64 { 0 }
    fn range_pop() {}
    fn name_os_thread(_tid: u32, _name: &str) {}
}
```

### 7.4 新增后端的步骤

1. 在 `lib/runtime/src/timeline/backends/` 下新建 `xxx.rs`；
2. 实现 `TimelineBackend` trait；
3. 在 `Cargo.toml` 中添加对应的 `timeline-xxx` feature（可选依赖）；
4. 在 `backends/mod.rs` 中添加 `#[cfg(feature = "timeline-xxx")]` 条件编译分支；
5. 更新本设计文档的后端列表。

整个过程不需要修改 `mod.rs` 中的宏、门控逻辑及任何公开 API。

---

## 八、构建命令速查

| 场景 | 命令 |
|------|------|
| 生产构建（无 timeline） | `cargo build --release` |
| 分析构建 + NVTX | `cargo build --profile profiling --features timeline-nvtx` |
| 分析构建 + Ascend | `cargo build --profile profiling --features timeline-ascend` |
| 仅编译 timeline 框架（空后端） | `cargo build --features timeline` |

---

## 九、设计决策记录

1. **trait 使用关联函数而非实例方法**：底层 profiler C API 都是线程局部状态，不需要实例。若未来某后端需要实例状态（如连接远程 profiler daemon），可在后端文件内部使用 `OnceLock` 管理单例，trait 签名不受影响。

2. **后端 feature 互斥**：同一二进制只需要一种 profiler 后端。互斥设计避免了运行时后端切换的复杂度。若未来出现同时需要多种后端的场景（如混合 GPU 集群），可将 `ActiveBackend` 改为后端链表遍历，但当前不需要。

3. **升腾后端延迟加载**：因为 Ascend 节点的部署比例可能远低于 NVIDIA 节点，采用 `libloading` 动态加载而非链接时依赖，避免在没有 Ascend 工具链的节点上因缺少 `.so` 而无法启动。

4. **公开宏保持不变**：`pagoda_timeline_range!` 等四个宏的签名是永久性公开契约，无论底层后端如何扩展，业务代码不需要任何修改。
