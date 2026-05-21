# `timeline` 模块设计文档

**源码位置**：`lib/runtime/src/timeline.rs`（当前实现规模约 200 行）

---

## 一、设计背景

LLM 推理链路中，性能分析工具（如 NVIDIA Nsight Systems）需要在时间线上标出有意义的区间（range），让性能工程师直观看到 `tokenize`、`prefill`、`decode` 等阶段的耗时分布，而不是面对一片无业务语义的 CUDA 内核调用。

`timeline` 模块的职责就是提供这一层**时间线标注能力**。在 Pagoda 的目标设计里，对外暴露的是统一的 timeline 抽象；底层仍可继续委托 NVTX 后端（例如 `cudarc::nvtx`）向 Nsight Systems 发出标注。这样做的好处是：上层代码只依赖“时间线事件”语义，不直接耦合具体分析后端名称。

但时间线标注在生产部署中通常不需要，且底层 NVTX C API 调用存在固定开销。因此 Pagoda 采用**两级门控**：编译期开关 + 运行时开关。未启用时，标注能力要么在编译期被完全消除，要么只保留一次极轻量的原子读判断。

---

## 二、两级门控机制

### 第一级：Cargo feature gate

当 `timeline` Cargo feature 未启用时，`AtomicBool`、`cudarc` 依赖以及所有 `#[cfg(feature = "timeline")]` 代码块都在编译期被消除。四个公开宏全部展开为空 `{}`，优化后不生成任何实际指令，因此普通生产构建（不带 `--features timeline`）对推理吞吐量没有影响。

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

当 `timeline` feature 已编译进二进制但未设置环境变量时，`TIMELINE_ENABLED` 保持 `false`。每次标注调用只需一次 `Ordering::Relaxed` 原子读取，代价接近一次普通内存读。只有当 `PGD_ENABLE_RUST_TIMELINE=1`（或 `true` / `yes` / `on`）时，底层后端才真正发出时间线标注。

这种两级设计允许同一个带 `timeline` feature 的二进制在“日常运行”和“临时性能分析”之间切换，而无需重新编译，适合线上节点临时打开分析窗口的场景。

**为什么使用 `Relaxed` ordering**：这个开关在进程启动时写入一次，后续只有读取，不承担跨线程同步其他状态的职责，因此不需要 acquire/release 语义。

---

## 三、公开 API

### `init()` — 在运行时启动阶段初始化

`init()` 从环境变量读取开关并设置 `TIMELINE_ENABLED`。`Runtime::new()` 在构造时自动调用 `timeline::init()`，因此大多数调用方不需要手工初始化。非 `timeline` feature 场景下，`init()` 是空函数。

### `push_impl` / `pop_impl` — 线程局部区间栈

时间线后端在每个 OS 线程上维护一个 range 栈。`push_impl(name)` 将命名区间压入当前线程，`pop_impl()` 弹出最内层区间。性能分析器据此构建可视化的嵌套时间线。

两者都应标注 `#[inline(always)]`，这样在 feature 关闭时可以被完全内联并消除，避免残留函数调用成本。

### `name_current_thread_impl` — 当前线程命名

```rust
pub fn name_current_thread_impl(name: &str) {
    #[cfg(feature = "timeline")]
    {
        if TIMELINE_ENABLED.load(Ordering::Relaxed) {
            #[cfg(target_os = "linux")]
            let tid = unsafe { libc::syscall(libc::SYS_gettid) as u32 };
            timeline_backend::name_os_thread(tid, name);
        }
    }
}
```

性能分析器默认只显示线程 ID，不利于识别 Tokio worker、IO 线程、控制线程等角色。通过 `name_current_thread_impl` 给线程打上稳定名称后，时间线就能直接体现线程职责。

在 Linux 下应使用 `SYS_gettid` 取得内核级线程 ID；因为底层后端命名接口面向 OS 线程而不是 pthread 抽象。

---

## 四、`TimelineRangeGuard` — RAII 防止 push/pop 不匹配

手工调用 `push_impl` / `pop_impl` 的最大风险在于：如果中间路径发生早退、`return`、`?` 传播错误，`pop_impl()` 可能不会执行，导致线程局部 range 栈失衡，进而让时间线显示错乱。

因此模块提供 `TimelineRangeGuard`：

```rust
#[cfg(feature = "timeline")]
pub struct TimelineRangeGuard { active: bool }

#[cfg(not(feature = "timeline"))]
pub struct TimelineRangeGuard;
```

构造时若开关已启用则压栈，并把 `active` 记录下来；`Drop` 时仅在 `active = true` 的情况下弹栈。这样既能防止忘记 `pop`，又能避免在 `init()` 之前误创建 guard 时产生额外的弹栈操作。

当 `timeline` feature 关闭时，`TimelineRangeGuard` 是零大小类型，编译器会把它和相关逻辑全部优化掉。

---

## 五、宏接口

四个宏是该模块面向业务代码的主要入口：

```rust
let _r = pagoda_timeline_range!("preprocess.tokenize");

pagoda_timeline_push!("codec.encode");
// ...
pagoda_timeline_pop!();

pagoda_timeline_name_thread!("decode-worker-0");
```

- `pagoda_timeline_range!`：推荐默认使用，依赖 RAII 自动闭合区间；
- `pagoda_timeline_push!` / `pagoda_timeline_pop!`：用于跨函数边界、无法自然包裹作用域的场景；
- `pagoda_timeline_name_thread!`：给当前线程写入可读名称。

选择宏而不是普通函数的原因是：当 `timeline` feature 关闭时，宏可以直接展开为空，不仅函数调用被消除，连参数求值也不会发生，从而实现真正的零开销。

---

## 六、与 NVTX 后端的关系

Pagoda 对外暴露的是 `timeline` 语义，但在 NVIDIA GPU 性能分析场景下，底层仍然可以委托给 NVTX 后端实现，例如 `cudarc::nvtx::result::range_push`、`range_pop` 和 `name_os_thread`。

这种分层有两个意义：

1. **上层命名稳定**：业务代码统一依赖 `timeline`，不把后端技术名暴露到公共接口里；
2. **后端可替换**：未来若需要接入其它 profiling 标注机制，模块边界和宏接口不需要整体重命名。

启用分析构建时可使用：

```bash
cargo build --profile profiling --features timeline
```

若底层仍采用 NVTX 后端，运行时仍需要 `libnvToolsExt.so`（来自 CUDA Toolkit 或 NVHPC）。