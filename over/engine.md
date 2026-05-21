# `engine` 模块设计文档

**源码位置**：`lib/runtime/src/engine.rs`（约 510 行）

---

## 一、设计背景与模块职责

Pagoda 的推理引擎是一个异步流式处理系统：前端接收用户请求，路由到某个具体的推理引擎（TensorRT-LLM、vLLM、自定义后端），引擎返回一个 token 流，前端将 token 逐个发回给用户。

这个架构要求：

1. **统一接口**：不同后端的引擎（TensorRT-LLM 引擎、Mock 引擎、代理引擎）必须实现相同的接口，才能被上层的路由器统一调度。
2. **生命周期控制**：用户随时可以取消请求（关闭连接），引擎必须感知取消信号并停止生成，避免浪费 GPU 资源。
3. **异构集合管理**：一个 Pagoda 节点可能同时管理多种引擎（不同模型、不同精度），需要将它们存储在同一个集合中并在运行时按需取出正确类型的引擎。

`engine` 模块为上述三个需求提供了 Rust 层面的抽象：

1. `AsyncEngine` trait：统一的引擎接口；
2. `AsyncEngineContext` trait：生命周期控制接口；
3. 类型擦除系统（`AnyAsyncEngine`、`AsAnyAsyncEngine`、`DowncastAnyAsyncEngine`）：异构集合管理。

---

## 二、`Data` trait：请求/响应类型的边界约束

### 为什么需要

```rust
pub trait Data: Send + Sync + 'static {}
impl<T: Send + Sync + 'static> Data for T {}
```

`AsyncEngine<Req, Resp, E>` 的三个类型参数需要满足特定约束，这些约束在 trait 定义中反复出现。若直接在 trait 定义处写 `Req: Send + Sync + 'static`，每个 `impl AsyncEngine<...>` 都需要重复这些约束，代码冗长且容易遗漏。

`Data` 将这组约束收归为一个具名 trait，语义更清晰（"这是引擎可以处理的数据类型"），同时通过毯毯实现（blanket impl）自动覆盖所有满足约束的类型，无需手动 impl。

**约束含义**：
- `Send`：请求和响应需要在 Tokio 线程池的多个线程间传递（任务调度）；
- `Sync`：多线程并发访问同一个引擎实例时，请求/响应对象可能被多线程引用；
- `'static`：异步任务可能超过创建者的生命周期（spawn 后调用方返回），数据不能包含短生命周期引用。

---

## 三、类型别名组

### 为什么需要这些别名

```rust
pub type DataUnary<T>  = Pin<Box<dyn Future<Output = T> + Send>>;
pub type DataStream<T> = Pin<Box<dyn Stream<Item = T> + Send>>;

pub type Engine<Req, Resp, E>  = Arc<dyn AsyncEngine<Req, Resp, E>>;
pub type EngineUnary<Resp>     = Pin<Box<dyn AsyncEngineUnary<Resp>>>;
pub type EngineStream<Resp>    = Pin<Box<dyn AsyncEngineStream<Resp>>>;
pub type Context               = Arc<dyn AsyncEngineContext>;
```

这些别名是系统中最高频出现的类型组合，展开后非常冗长：

- `Pin<Box<dyn Stream<Item = T> + Send>>` 是所有异步流的标准形式——`Pin` 保证流对象在内存中固定（async/await 状态机的要求），`Box` 使 trait 对象有确定大小，`dyn Stream + Send` 支持不同流实现的多态；
- `Arc<dyn AsyncEngine<...>>` 是引擎的所有权模型——`Arc` 允许多个调用方共享同一个引擎实例，`dyn` 支持多种引擎实现。

**`DataStream` 与 `EngineStream` 的区别**：`DataStream<T>` 是纯数据流，不携带上下文；`EngineStream<Resp>` 是引擎流，必须同时实现 `AsyncEngineContextProvider`（携带可取消的上下文）。转换关系体现在：

```rust
impl<T: Data> From<EngineStream<T>> for DataStream<T> {
    fn from(stream: EngineStream<T>) -> Self { Box::pin(stream) }
}
```

引擎处理链（如中间件、流转换）可以将 `EngineStream` 拆成纯数据流进行处理（用 Stream combinator），再通过 `ResponseStream::new(data_stream, ctx)` 重新封装成带上下文的 `EngineStream`。这种设计使标准的 Stream 处理工具（`futures::StreamExt`）都可以直接用于引擎流，无需为引擎专门实现 adapter。

---

## 四、`AsyncEngineContext` trait：生命周期控制

### 为什么需要独立的 Context 对象

LLM 推理是计算密集且耗时的操作——生成一个 1000-token 的响应可能需要数秒。用户（尤其是 API 调用方）随时可能中断请求（网络断开、明确取消）。若不及时通知引擎停止，引擎会继续占用 GPU 直到生成完毕，造成资源浪费。

`AsyncEngineContext` 提供两级终止控制，解决不同严重程度的取消需求：

```rust
pub trait AsyncEngineController: Send + Sync {}
#[async_trait]
pub trait AsyncEngineContext: Send + Sync + Debug {
    fn id(&self) -> &str;

    fn is_stopped(&self) -> bool;
    fn is_killed(&self) -> bool;
    async fn stopped(&self);
    async fn killed(&self);

    fn stop_generating(&self);
    fn stop(&self);
    fn kill(&self);

    fn link_child(&self, child: Arc<dyn AsyncEngineContext>);
}
```

**`stop_generating()` / `stop()`（优雅停止）**：通知引擎"完成当前 token 后不要再生成新 token"。适合用户主动发送停止信号的场景。引擎可以保留已生成但未发送的 token，调用方可以选择继续 drain 流或直接 drop。`stop()` 是 `stop_generating()` 的别名，两者语义相同。

**`kill()`（立即终止）**：强制终止，通知引擎丢弃所有未发送内容，尽快退出。适合请求超时或网络断开（连消费流的机会都没有）的场景。通常与 `.take_while(!ctx.is_killed())` stream combinator 配合：流的最下游插入这个 combinator，一旦 `kill()` 被调用流立即结束，不等 drain。

**`is_stopped()` / `is_killed()`**：同步轮询方法，供非 async 的流实现（如 `poll_next`）查询状态，避免 async 方法的调用开销。

**`stopped()` / `killed()`（async 等待）**：等待状态变为已停止/已终止，供需要 "等待取消完成" 的场景使用（如 supervisor 等待所有子任务结束后再退出）。

**`id() -> &str`**：唯一标识一个推理请求。用于日志中关联同一请求的多条记录（从接收到路由到实际生成），方便追踪请求全链路。

**`link_child(child: Arc<dyn AsyncEngineContext>)`**：将子 Context 链接到父 Context。当父 Context 的 `stop/kill` 被调用时，框架自动按链接顺序对所有子 Context 调用相同方法。这是组合式取消的核心机制——一个请求可能经过多级引擎（Router → Worker，Worker 内部还有子流），所有层级的上下文通过 `link_child` 形成树，取消信号从根节点广播到所有叶节点。

---

## 五、`AsyncEngineContextProvider` trait

### 为什么需要

```rust
pub trait AsyncEngineContextProvider: Send + Debug {
    fn context(&self) -> Arc<dyn AsyncEngineContext>;
}
```

这个 trait 是一个"标记接口"：凡是实现了它的类型，都可以提供一个与自身关联的 `AsyncEngineContext`。

**为什么需要此 trait 而非直接访问字段**：`AsyncEngine::generate()` 的返回值是 `Resp`（一个泛型类型），框架层需要从 `Resp` 中提取 `Context` 来完成取消操作。若用字段访问，框架需要知道 `Resp` 的具体类型；用 trait 则只需 `resp.context()`，框架对 `Resp` 的内部结构一无所知。这使任何自定义的响应类型（只要实现了 `AsyncEngineContextProvider`）都可以直接用于 `AsyncEngine`，无需修改框架代码。

**`AsyncEngineUnary` 和 `AsyncEngineStream` 对 `AsyncEngineContextProvider` 的要求**：两个 operation trait 都继承了 `AsyncEngineContextProvider`：

```rust
pub trait AsyncEngineUnary<Resp: Data>:
    Future<Output = Resp> + AsyncEngineContextProvider + Send {}

pub trait AsyncEngineStream<Resp: Data>:
    Stream<Item = Resp> + AsyncEngineContextProvider + Send {}
```

这保证任何 unary 操作（一次性请求）和流操作都携带可控的上下文，框架可以统一地对它们发出取消信号。

---

## 六、`AsyncEngine` trait：核心引擎接口

### 为什么需要

```rust
#[async_trait]
pub trait AsyncEngine<Req: Send + Sync + 'static, Resp: AsyncEngineContextProvider, E: Data>:
    Send + Sync
{
    async fn generate(&self, request: Req) -> Result<Resp, E>;
}
```

**单方法设计的原因**：`generate` 是引擎唯一需要对外暴露的核心操作——接收请求，返回响应（或错误）。框架所有的路由、健康检查、取消都通过 `Context`（嵌入在 `Resp` 中）实现，不需要引擎暴露额外方法。单方法 trait 使 impl 最简洁，也使类型擦除系统（见下文）的设计更清晰。

**`Resp: AsyncEngineContextProvider`（而非 `Resp: Data`）**：要求响应必须携带 context。引擎不允许返回"无法取消"的响应——这是框架对所有引擎实现的强制约定，保证框架层的取消机制对所有引擎统一有效。

**`#[async_trait]`**：Rust 的 trait 目前不稳定地支持 async fn（async trait 在 Rust 1.75 稳定，但 trait 对象的 async fn 仍有限制）。`async_trait` 宏将 async fn 展开为 `fn ... -> Pin<Box<dyn Future<...>>>` 的形式，使 trait 对象（`dyn AsyncEngine`）可以调用 async 方法。

---

## 七、`ResponseStream`：将 DataStream 封装为 EngineStream

### 为什么需要

```rust
pub struct ResponseStream<R: Data> {
    stream: DataStream<R>,
    ctx: Arc<dyn AsyncEngineContext>,
}
```

引擎实现中的常见模式：用 `futures::StreamExt` 的 `.map()`、`.filter()`、`.take_while()` 等 combinator 对原始生成流进行变换，这些 combinator 产生的是 `DataStream<T>`（无上下文的纯数据流）。但 `AsyncEngine::generate()` 要求返回 `Resp: AsyncEngineContextProvider`，即必须携带上下文。

`ResponseStream` 是这个"重新封装"步骤的专用类型：

```rust
impl<R: Data> ResponseStream<R> {
    pub fn new(stream: DataStream<R>, ctx: Arc<dyn AsyncEngineContext>) -> Pin<Box<Self>> {
        Box::pin(Self { stream, ctx })
    }
}
```

传入已变换的 `DataStream` 和原始 `Context`（从引擎创建流时保存），得到一个既满足 `Stream` 语义又满足 `AsyncEngineContextProvider` 的 `EngineStream`。这个设计使引擎实现可以自由使用所有 Stream combinator，而不受框架要求的约束——只在最终返回时做一次封装。

### `Stream for ResponseStream<R>`

```rust
impl<R: Data> Stream for ResponseStream<R> {
    type Item = R;

    #[inline]
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }
}
```

这个实现把 `ResponseStream` 变成标准的 Rust `Stream`。核心逻辑没有再包一层状态机，而是直接把 `poll_next()` 委托给内部的 `stream` 字段。

**为什么是简单转发**：`ResponseStream` 的职责不是改变流语义，而是把“数据流”与“取消上下文”绑定在一起。真正的数据生成、背压处理、结束条件都由内部 `DataStream<R>` 决定，这里只负责把它暴露成外层可消费的 `Stream` 接口。

**`#[inline]` 的意义**：这是一个极薄的包装层，内联提示有助于编译器消掉这层委托开销，避免每次 `poll_next()` 都多一次无意义的函数跳转。

### `AsyncEngineStream<R> for ResponseStream<R>`

```rust
impl<R: Data> AsyncEngineStream<R> for ResponseStream<R> {}
```

这是一个标记性实现。`AsyncEngineStream<R>` 自身没有新增方法，它表达的是“这个类型既是 `Stream<Item = R>`，又能提供 `AsyncEngineContext`”。

**为什么需要这个空 impl**：框架的很多接口并不想只接受普通 `Stream`，而是要求“可取消的引擎输出流”。`ResponseStream` 已经实现了 `Stream` 和 `AsyncEngineContextProvider`，补上这个空实现后，就能被统一当成 `EngineStream<R>` 使用。

### `AsyncEngineContextProvider for ResponseStream<R>`

```rust
impl<R: Data> AsyncEngineContextProvider for ResponseStream<R> {
    fn context(&self) -> Arc<dyn AsyncEngineContext> {
        self.ctx.clone()
    }
}
```

这个实现让框架层可以从 `ResponseStream` 中取回关联的上下文对象。

**为什么返回 `Arc` 克隆**：上下文需要被多个位置共享持有，例如网络层、路由层、调用方取消逻辑都可能同时引用同一个请求的 `Context`。返回 `self.ctx.clone()` 只增加引用计数，不复制底层状态，既便宜又满足共享语义。

### `Debug for ResponseStream<R>`

```rust
impl<R: Data> Debug for ResponseStream<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResponseStream")
            // todo: add debug for stream - possibly propagate some information about what
            // engine created the stream
            // .field("stream", &self.stream)
            .field("ctx", &self.ctx)
            .finish()
    }
}
```

手动实现 `Debug`，只输出 `ctx`，不输出内部 `stream`。

**为什么跳过 `stream`**：`DataStream` 是 trait 对象，通常不实现 `Debug`，即使勉强输出也往往只有一串缺乏意义的类型擦除信息。相反，`ctx` 往往包含请求 ID 等真正有诊断价值的内容。

**为什么保留 `todo` 注释**：它表明作者并不是忽略 `stream` 的可观测性，而是明确承认当前调试信息还不完整，未来可能补上“由哪个引擎创建了这个流”之类的来源信息。

### `RequestStream<R> = ResponseStream<R>`

```rust
pub type RequestStream<R> = ResponseStream<R>;
```

这是一个输入侧别名：底层结构完全相同，仍然是 `(stream, ctx)` 二元组，只是调用点的语义不同。

**为什么不单独再定义一个结构体**：请求流和响应流在运行时行为上没有本质区别，差别只在于“这个流被放在引擎的输入位置还是输出位置”。用类型别名而不是重复定义结构体，可以复用全部实现，同时在调用点保留更清晰的业务语义。

### Boxed `AsyncEngineUnary` / `AsyncEngineStream` 的上下文转发

```rust
impl<T: Data> AsyncEngineContextProvider for Pin<Box<dyn AsyncEngineUnary<T>>> {
    fn context(&self) -> Arc<dyn AsyncEngineContext> {
        AsyncEngineContextProvider::context(&**self)
    }
}

impl<T: Data> AsyncEngineContextProvider for Pin<Box<dyn AsyncEngineStream<T>>> {
    fn context(&self) -> Arc<dyn AsyncEngineContext> {
        AsyncEngineContextProvider::context(&**self)
    }
}
```

这两个实现解决的是“trait 对象再包一层 `Pin<Box<...>>` 后，如何继续访问内部 context”的问题。

**为什么需要转发 impl**：`EngineUnary<T>` 和 `EngineStream<T>` 在前文里都被定义成 `Pin<Box<dyn ...>>` 类型别名。调用方拿到的往往正是这个 boxed 形式，而不是底层具体类型；如果没有这里的转发实现，框架层就得先手动解引用，再显式调用内部 trait 方法，使用上会非常别扭。

**`&**self` 的含义**：先从 `Pin<Box<...>>` 解到 `Box<...>`，再解到内部 `dyn AsyncEngineUnary<T>` / `dyn AsyncEngineStream<T>`，最后把 `context()` 调用委托给真正的底层对象。这个模式和前面的 `poll_next` 转发本质一致：外层包装不增加语义，只负责把内部能力继续暴露出来。


---

## 八、类型擦除系统

### 为什么需要类型擦除

`AsyncEngine<Req, Resp, E>` 有三个泛型参数。在 Rust 中，`Arc<dyn AsyncEngine<String, RespA, ()>>` 和 `Arc<dyn AsyncEngine<Vec<u8>, RespB, ErrorB>>` 是两个完全不同的类型，不能存入同一个集合（如 `HashMap<String, Arc<dyn ???>>`）。

但 Pagoda 的引擎管理器需要做到这一点——在运行时根据配置从集合中取出某种具体类型的引擎。若不进行类型擦除，管理器就需要对每种引擎类型组合维护独立的 `HashMap`，且必须在编译期知道所有可能的类型组合，无法支持插件式的引擎动态注册。

类型擦除将"类型信息的知识"延迟到运行时，允许编译期不知道具体类型的代码在运行时安全地恢复类型并使用。

---

### `AnyAsyncEngine` trait：类型擦除的 trait 对象接口

```rust
pub trait AnyAsyncEngine: Send + Sync {
    fn request_type_id(&self) -> TypeId;
    fn response_type_id(&self) -> TypeId;
    fn error_type_id(&self) -> TypeId;
    fn as_any(&self) -> &dyn Any;
}
```

**为什么需要三个 `TypeId` 方法**：类型擦除后，类型信息被"藏"了起来。要安全地取回原始类型，必须先验证"你期望的类型"和"实际存储的类型"是否匹配。三个 TypeId 分别对应 `Req`、`Resp`、`E`——三者都匹配才说明是同一种引擎，少验证任何一个都可能导致内存不安全的强制转换。

**`as_any() -> &dyn Any`**：提供向 `dyn Any` 的降级，为 `downcast_ref::<T>()` 铺路。只有通过 `dyn Any` 的标准 downcast 路径，Rust 才会执行安全的运行时类型检查（对比存储的实际 TypeId 和期望的 TypeId），而非不安全的强制转换。

**为什么不直接用 `dyn Any`**：`dyn Any` 只存储了具体类型（`AnyEngineWrapper<Req, Resp, E>`）的 TypeId，downcast 时需要知道这个具体类型。但调用方只知道 `Req`、`Resp`、`E` 三个参数，不知道 `AnyEngineWrapper<...>` 这个内部包装类型。三个独立的 `TypeId` 方法允许调用方先比对参数类型，再安全地执行 downcast，而无需了解内部实现。

---

### `AnyEngineWrapper`：类型信息的保存者

```rust
struct AnyEngineWrapper<Req, Resp, E>
where
    Req: Data,
    Resp: Data + AsyncEngineContextProvider,
    E: Data,
{
    engine: Arc<dyn AsyncEngine<Req, Resp, E>>,
    _phantom: PhantomData<fn(Req, Resp, E)>,
}
```

**为什么用 `PhantomData<fn(Req, Resp, E)>` 而非 `PhantomData<(Req, Resp, E)>`**：

`PhantomData<(Req, Resp, E)>` 会让编译器认为 `AnyEngineWrapper` 直接拥有 `Req`、`Resp`、`E` 的值（影响 Drop 顺序、协变/逆变分析、自动实现 Send/Sync 的判断）。而 `PhantomData<fn(Req, Resp, E)>` 表示"这个结构体在逻辑上是一个接受 Req/Resp/E 参数的函数类型的模拟持有者"，让编译器以函数指针的变型规则（逆变输入参数，协变返回值）分析类型，更准确地反映实际使用场景（引擎接收请求，返回响应）。在实践中，这两者对 Send/Sync 的影响在此场景下等价，但 `fn(...)` 形式是 Rust 社区处理"需要类型参数但不直接存储"场景的惯用写法，可读性更强。

**`AnyAsyncEngine for AnyEngineWrapper` 的实现**：

```rust
fn request_type_id(&self) -> TypeId { TypeId::of::<Req>() }
fn response_type_id(&self) -> TypeId { TypeId::of::<Resp>() }
fn error_type_id(&self) -> TypeId { TypeId::of::<E>() }
fn as_any(&self) -> &dyn Any { &self.engine }
```

关键点：`as_any()` 返回的是 `&self.engine`（`Arc<dyn AsyncEngine<Req, Resp, E>>` 的引用），而不是 `self`。这是因为 downcast 的目标类型是 `Arc<dyn AsyncEngine<Req, Resp, E>>`（调用方期望取回的类型），而非 `AnyEngineWrapper` 本身。若返回 `self`，downcast 时需要知道 `AnyEngineWrapper` 的具体类型，调用方不应感知这个内部实现。

---

### `AsAnyAsyncEngine`：类型擦除的入口

```rust
pub trait AsAnyAsyncEngine {
    fn into_any_engine(self) -> Arc<dyn AnyAsyncEngine>;
}

impl<Req, Resp, E> AsAnyAsyncEngine for Arc<dyn AsyncEngine<Req, Resp, E>>
where
    Req: Data,
    Resp: Data + AsyncEngineContextProvider,
    E: Data,
{
    fn into_any_engine(self) -> Arc<dyn AnyAsyncEngine> {
        Arc::new(AnyEngineWrapper {
            engine: self,
            _phantom: PhantomData,
        })
    }
}
```

**为什么设计为扩展 trait（extension trait）而非关联方法**：若将 `into_any_engine` 放入 `AsyncEngine` trait，`AsyncEngine` trait 就需要了解 `AnyAsyncEngine` 和 `AnyEngineWrapper`，产生循环依赖。扩展 trait 允许在 `AsyncEngine` 定义之外为其添加方法，保持 `AsyncEngine` 专注于业务接口，类型擦除机制在单独的 trait 中实现，关注点分离。

---

### `DowncastAnyAsyncEngine`：类型擦除的出口

```rust
pub trait DowncastAnyAsyncEngine {
    fn downcast<Req, Resp, E>(&self) -> Option<Arc<dyn AsyncEngine<Req, Resp, E>>>
    where
        Req: Data,
        Resp: Data + AsyncEngineContextProvider,
        E: Data;
}

impl DowncastAnyAsyncEngine for Arc<dyn AnyAsyncEngine> {
    fn downcast<Req, Resp, E>(&self) -> Option<Arc<dyn AsyncEngine<Req, Resp, E>>> {
        if self.request_type_id() == TypeId::of::<Req>()
            && self.response_type_id() == TypeId::of::<Resp>()
            && self.error_type_id() == TypeId::of::<E>()
        {
            self.as_any()
                .downcast_ref::<Arc<dyn AsyncEngine<Req, Resp, E>>>()
                .cloned()
        } else {
            None
        }
    }
}
```

**为什么类型不匹配时返回 `None` 而非 panic**：调用方不一定知道集合中存储的具体引擎类型（例如按名字取出一个引擎，配置错误导致类型不匹配）。返回 `None` 允许调用方优雅地处理类型不匹配，而 panic 会终止整个进程，代价过高。类型擦除系统被设计为"安全的"（safe），不使用 `unsafe` 代码，所有路径都有明确的失败返回。

**downcast 流程**：

1. 比对三个 TypeId（`O(1)` 整数比较）——这是廉价的快速验证；
2. 若三者相等，调用 `as_any().downcast_ref::<Arc<dyn AsyncEngine<Req, Resp, E>>>()`——Rust 标准库的安全 downcast，内部也是 TypeId 比较，但比对的是 `Any` 的实际类型（`Arc<dyn AsyncEngine<Req, Resp, E>>`），和步骤 1 是双重验证；
3. `.cloned()` 克隆 `Arc`（只递增引用计数），返回调用方可以独立持有的强引用。

步骤 1 的三个 TypeId 比较和步骤 2 的 `downcast_ref` 在数学上是等价的，但两层验证提供了更清晰的代码意图：`downcast_ref` 才是真正安全保证所在，步骤 1 的 if 条件使意图显式可读（"检查三个类型参数都匹配"）。

---

## 九、完整数据流示意

```
用户请求到达
    │
    ▼
AsyncEngine::generate(request)
    │  返回 Result<Resp, E>
    │  Resp 实现 AsyncEngineContextProvider
    ▼
框架提取 ctx = resp.context()         ←─── ctx 链接到取消令牌
    │
    ▼
ResponseStream::new(data_stream, ctx)  ←─── 或直接使用 Resp
    │
    ├─► 用户断开连接 → ctx.kill()       ──► .take_while(!ctx.is_killed()) 终止流
    │
    └─► 流式发送 token 给用户
            │
            ▼
        流结束 (EOS token) 或 ctx.stop_generating()
```

类型擦除路径（异构引擎集合）：

```
Arc<dyn AsyncEngine<Req, Resp, E>>
    │ .into_any_engine()
    ▼
Arc<dyn AnyAsyncEngine>  ──存入──► HashMap<String, Arc<dyn AnyAsyncEngine>>
    │ .downcast::<Req, Resp, E>()
    ▼
Arc<dyn AsyncEngine<Req, Resp, E>>  ──► .generate(request)
```
