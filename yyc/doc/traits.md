# `traits` 与 `prelude` 模块设计文档

**源码位置**：`lib/runtime/src/traits.rs`（28 行）· `lib/runtime/src/prelude.rs`（4 行）

---

## 一、设计背景

`Runtime` 是 Pagoda 中所有 ServiceGroup、Namespace、PortName 访问共享资源的统一入口，`DistributedRuntime` 是分布式服务发现层的入口。许多工具函数（如请求路由、服务发现、健康检查）需要访问这两者之一，但这些函数不应该要求调用方持有具体的 `Runtime` 或 `DistributedRuntime` 实例——那样会使函数签名与具体类型耦合，难以在 `ServiceGroup`、`Namespace`、`DistributedRuntime` 等不同层级复用。

`traits` 模块用两个最小 trait 解决这个问题，使泛型代码可以接受"任何能提供 `Runtime` 引用的类型"，而无需关心具体类型。

---

## 二、`RuntimeProvider` trait

```rust
pub trait RuntimeProvider {
    fn rt(&self) -> &Runtime;
}
```

任何持有或代理 `Runtime` 的类型都可以实现此 trait。返回 `&Runtime` 而非 `Arc<Runtime>` 或 `Runtime` clone 的原因：调用方通常只需要临时借用来访问线程池 handle 或取消 token，不需要延长 `Runtime` 的生命周期。返回引用是最低开销的选择（零克隆），且 Rust 借用检查器确保引用有效期不超过 `self`。

**`DistributedRuntime` 实现**：

```rust
impl RuntimeProvider for DistributedRuntime {
    fn rt(&self) -> &Runtime {
        self.runtime()
    }
}
```

`DistributedRuntime` 内部持有一个 `Runtime`，通过 `runtime()` 方法访问。此实现使得接受 `impl RuntimeProvider` 的函数可以直接传入 `DistributedRuntime`，无需先提取内部的 `Runtime`。

---

## 三、`DistributedRuntimeProvider` trait

```rust
pub trait DistributedRuntimeProvider {
    fn drt(&self) -> &DistributedRuntime;
}
```

语义与 `RuntimeProvider` 对称。函数名 `.drt()` 是 `distributed_runtime` 的缩写，刻意保持简短，因为它在 `ServiceGroup`、`Namespace`、`PortName` 的实现代码中会被频繁调用。

**`DistributedRuntime` 自身的实现**：

```rust
impl DistributedRuntimeProvider for DistributedRuntime {
    fn drt(&self) -> &DistributedRuntime {
        self
    }
}
```

`DistributedRuntime` 实现自身的 Provider，使得泛型代码 `fn foo(drt: &impl DistributedRuntimeProvider)` 可以直接传入 `DistributedRuntime` 本身，而 `Namespace` 和 `ServiceGroup`（内部持有 `DistributedRuntime` 引用）实现此 trait 后也可以传入相同函数。这是 Provider 模式的核心价值：函数签名稳定，可接受的类型集合随实现增长，调用方和被调用方解耦。

代码注释也明确说明了这一意图："ServiceGroups, Namespaces, and PortNames use this trait to access their DRT."——这些类型可以通过实现 trait 透明地暴露各自持有的 `DistributedRuntime`，消费方无需了解它们的内部结构。

---

## 四、`prelude` 模块

```rust
// src/prelude.rs
pub use crate::traits::*;
```

`prelude` 是惯例命名的"便捷导入门面"。Pagoda 的用户代码通常需要 `use pagoda_runtime::prelude::*` 一次性导入所有常用 trait，而不是逐个 `use pagoda_runtime::traits::RuntimeProvider`。

目前 `prelude` 只重导出 `traits::*`，保持极简。未来若有更多"用户代码几乎总是需要"的 trait，可以在不破坏现有导入的前提下添加到 `prelude`。

---

## 五、补充：当前源码里只有 `DistributedRuntime` 的 impl 写在 `traits.rs`

从 [lib/runtime/src/traits.rs](lib/runtime/src/traits.rs) 本身来看，当前文件里直接写出来的只有两组实现：

```rust
impl RuntimeProvider for DistributedRuntime { ... }
impl DistributedRuntimeProvider for DistributedRuntime { ... }
```

而 `Namespace`、`ServiceGroup`、`PortName` 这些层级对象上的 trait 实现，并不在这个文件里，而是分散写在 [lib/runtime/src/servicegroup.rs](lib/runtime/src/servicegroup.rs) 及相关子模块中。

这点很值得单独说明，因为它决定了 `traits.rs` 的职责边界：

- trait 定义本身在这里；
- `DistributedRuntime` 的最基础 impl 也在这里；
- 更高层服务模型对象的委托实现，则跟着各自类型定义放在别处。

---

## 六、补充：层级穿透的真实路径

把当前实现和服务模型层级放在一起看，这两个 trait 的穿透关系可以概括成：

- `DistributedRuntime::rt()` -> `self.runtime()`
- `DistributedRuntime::drt()` -> `self`
- `ServiceGroup::rt()` -> `self.drt.rt()`
- `ServiceGroup::drt()` -> `&self.drt`
- `PortName` / `Namespace` 等更深层对象则继续沿组件层级向上委托

因此它们的价值不只是“少写几个参数”，而是让服务模型中的不同层级对象都能暴露统一的运行时访问方式，同时保证最终指向的是同一个底层 `Runtime` / `DistributedRuntime` 实例。

---

## 七、补充：`prelude` 的职责非常窄

[lib/runtime/src/prelude.rs](lib/runtime/src/prelude.rs) 当前只有一行核心导出：

```rust
pub use crate::traits::*;
```

这说明它现在还不是一个“大而全”的公共门面，而只是 traits 的便捷再导出层。也就是说：

- 使用者通过 `prelude::*` 主要拿到的是运行时访问 trait；
- 它当前并不额外重导出 runtime、servicegroup 或其他协议类型；
- 这种克制的做法让 `prelude` 的公共面保持得很窄，后续若要扩展，也可以按“最常用 trait 优先”的原则逐步增加。
