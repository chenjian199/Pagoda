// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 设计意图
//! 为 Dynamo 异步流式引擎提供统一抽象与类型擦除支持。核心目标:
//! 1. 用 `AsyncEngine<Req, Resp, Err>` 描述"请求 -> 响应流"语义,屏蔽不同后端实现差异;
//! 2. 通过 `AnyAsyncEngine` 把不同泛型实参的引擎装进同一集合
//!    (例: `HashMap<String, Arc<dyn AnyAsyncEngine>>`),供运行时按名查找;
//! 3. 在保留类型安全的前提下支持运行时下转 (downcast) 回原始具体泛型。
//!
//! # 外部契约
//! - `trait AsyncEngine<Req, Resp, Err>`: 异步 `generate(Req) -> Result<Resp, Err>`;
//! - `trait AnyAsyncEngine`: 类型擦除句柄,暴露请求/响应 `TypeId` 与 `&dyn Any` 视图;
//! - `trait AsAnyAsyncEngine` / `trait DowncastAnyAsyncEngine`:
//!   将具体引擎升入擦除态、再按目标泛型还原;若 `TypeId` 不匹配返回 `None`;
//! - 配套契约类型 `Data` (blanket impl for `Send + Sync + 'static`),
//!   `AsyncEngineContext` / `AsyncEngineContextProvider` 描述请求级上下文;
//! - `ResponseStream::new(stream, ctx)` 构造受控响应流并随上下文取消而终止。
//!
//! # 实现要点
//! - 类型擦除依赖 `std::any::{Any, TypeId}` 在运行时严格匹配 `(Req, Resp, Err)`;
//! - blanket impl `impl<T: Send + Sync + 'static> Data for T` 是擦除前提,改动需同步评审;
//! - 下转走 `Arc<dyn Any + Send + Sync>::downcast` 零拷贝路径,失败回退 `None`;
//! - 上下文取消通过 `AsyncEngineContext` 串通到响应流,避免泄漏未消费的后端任务。
//!
//! # 使用示例
//! ```rust,ignore
//! use std::collections::HashMap;
//! use std::sync::Arc;
//! use crate::engine::{AsyncEngine, AsAnyAsyncEngine, DowncastAnyAsyncEngine};
//!
//! let string_engine: Arc<dyn AsyncEngine<String, String, ()>> = Arc::new(MyStringEngine::new());
//! let int_engine: Arc<dyn AsyncEngine<i32, i32, ()>> = Arc::new(MyIntEngine::new());
//!
//! let mut engines: HashMap<String, Arc<dyn AnyAsyncEngine>> = HashMap::new();
//! engines.insert("string".into(), string_engine.into_any_engine());
//! engines.insert("int".into(), int_engine.into_any_engine());
//!
//! if let Some(typed) = engines.get("string").unwrap().downcast::<String, String, ()>() {
//!     let _ = typed.generate("hello".to_string()).await;
//! }
//! ```

use std::{
    any::{Any, TypeId},
    fmt::Debug,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
};

pub use async_trait::async_trait;
use futures::stream::Stream;

// === SECTION: Data 与上下文 trait ===

/// All [`Send`] + [`Sync`] + `'static` types can be used as [`AsyncEngine`] request and response types.
///
/// This is implemented as a blanket implementation for all types that meet the bounds.
/// **Do not manually implement this trait** - the blanket implementation covers all valid types.
pub trait Data: Send + Sync + 'static {}
impl<T: Send + Sync + 'static> Data for T {}

/// [`DataStream`] is a type alias for a stream of [`Data`] items. This can be adapted to a [`ResponseStream`]
/// by associating it with a [`AsyncEngineContext`].
pub type DataUnary<T> = Pin<Box<dyn Future<Output = T> + Send>>;
pub type DataStream<T> = Pin<Box<dyn Stream<Item = T> + Send>>;

pub type Engine<Req, Resp, E> = Arc<dyn AsyncEngine<Req, Resp, E>>;
pub type EngineUnary<Resp> = Pin<Box<dyn AsyncEngineUnary<Resp>>>;
/// Trait-object alias for an [`AsyncEngineStream`] — used on both sides of an
/// engine: the input side via [`crate::pipeline::ManyIn`] and the output side via
/// [`crate::pipeline::ManyOut`]. The directional names exist
/// at the [`crate::pipeline`] alias layer for documentary clarity at use sites.
pub type EngineStream<T> = Pin<Box<dyn AsyncEngineStream<T>>>;
pub type Context = Arc<dyn AsyncEngineContext>;

impl<T: Data> From<EngineStream<T>> for DataStream<T> {
    // 中文说明：
    // 1. 这个转换函数把带引擎语义的 `EngineStream` 适配成更通用的 `DataStream`。
    // 2. 代码通过 `Box::pin` 保持底层流处于固定地址，满足异步流对 `Pin` 的要求。
    // 3. 返回值仍然指向同一条底层流，只是对外暴露成更基础的数据流类型。
    fn from(stream: EngineStream<T>) -> Self {
        let data_stream = Box::pin(stream);
        data_stream
    }
}

// The Controller and the Context when https://github.com/rust-lang/rust/issues/65991 becomes stable
pub trait AsyncEngineController: Send + Sync {}

/// The [`AsyncEngineContext`] trait defines the interface to control the resulting stream
/// produced by the engine.
///
/// This trait provides lifecycle management for async operations, including:
/// - Stream identification via unique IDs
/// - Graceful shutdown capabilities (`stop_generating`)
/// - Immediate termination capabilities (`kill`)
/// - Status checking for stopped/killed states
///
/// Implementations should ensure thread-safety and proper state management
/// across concurrent access patterns.
#[async_trait]
pub trait AsyncEngineContext: Send + Sync + Debug {
    /// Unique ID for the Stream
    // 中文说明：
    // 1. 返回当前上下文对应流的唯一标识。
    // 2. 调用方可以用这个标识做日志追踪、关联关系记录或调试输出。
    fn id(&self) -> &str;

    /// Returns true if `stop_generating()` has been called; otherwise, false.
    // 中文说明：
    // 1. 查询当前上下文是否已经收到“停止继续生成”的信号。
    // 2. 返回 `true` 表示上游应尽快停止产生新结果，但不一定立刻中断现有输出。
    fn is_stopped(&self) -> bool;

    /// Returns true if `kill()` has been called; otherwise, false.
    /// This can be used with a `.take_while()` stream combinator to immediately terminate
    /// the stream.
    ///
    /// An ideal location for a `[.take_while(!ctx.is_killed())]` stream combinator is on
    /// the most downstream  return stream.
    // 中文说明：
    // 1. 查询当前上下文是否已经收到“立即终止”的信号。
    // 2. 返回 `true` 时，调用方通常应停止继续消费并尽快结束整条流。
    fn is_killed(&self) -> bool;

    /// Calling this method when [`AsyncEngineContext::is_stopped`] is `true` will return
    /// immediately; otherwise, it will [`AsyncEngineContext::is_stopped`] will return true.
    // 中文说明：
    // 1. 异步等待上下文进入 stopped 状态。
    // 2. 如果当前已经停止，则立即返回；否则一直等到实现方把状态切换为已停止。
    async fn stopped(&self);

    /// Calling this method when [`AsyncEngineContext::is_killed`] is `true` will return
    /// immediately; otherwise, it will [`AsyncEngineContext::is_killed`] will return true.
    // 中文说明：
    // 1. 异步等待上下文进入 killed 状态。
    // 2. 如果当前已经被标记为终止，则立即返回；否则等待终止信号真正到达。
    async fn killed(&self);

    // Controller

    /// Informs the [`AsyncEngine`] to stop producing results for this particular stream.
    /// This method is idempotent. This method does not invalidate results current in the
    /// stream. It might take some time for the engine to stop producing results. The caller
    /// can decided to drain the stream or drop the stream.
    // 中文说明：
    // 1. 通知引擎停止继续为当前流生成新的结果。
    // 2. 该操作应当是幂等的，多次调用不会产生额外副作用。
    fn stop_generating(&self);

    /// See [`AsyncEngineContext::stop_generating`].
    // 中文说明：
    // 1. 这是 `stop_generating` 的便捷别名。
    // 2. 实现方通常会把它映射到相同的停止逻辑。
    fn stop(&self);

    /// Extends the [`AsyncEngineContext::stop_generating`] also indicates a preference to
    /// terminate without draining the remaining items in the stream. This is implementation
    /// specific and may not be supported by all engines.
    // 中文说明：
    // 1. 请求引擎尽快终止当前流，而不是只做“优雅停止生成”。
    // 2. 是否立即丢弃未消费结果取决于具体实现，但语义上比 `stop` 更强。
    fn kill(&self);

    /// Links child AsyncEngineContext to this AsyncEngineContext. If the `stop_generating`, `stop`
    /// or `kill` on this AsyncEngineContext is called, the same method is called on all linked
    /// child AsyncEngineContext, in the order they are linked, and then the method on this
    /// AsyncEngineContext continues.
    // 中文说明：
    // 1. 把一个子上下文挂接到当前上下文上，形成联动的生命周期控制关系。
    // 2. 之后父上下文收到 stop 或 kill 类命令时，子上下文也会按链接顺序收到同样的命令。
    fn link_child(&self, child: Arc<dyn AsyncEngineContext>);
}

/// Provides access to the [`AsyncEngineContext`] associated with an engine operation.
///
/// This trait is implemented by both unary and streaming engine results, allowing
/// uniform access to context information regardless of the operation type.
pub trait AsyncEngineContextProvider: Send + Debug {
    // 中文说明：
    // 1. 返回当前对象关联的 `AsyncEngineContext`。
    // 2. 调用方可以借此统一访问流或单次响应背后的生命周期控制对象。
    fn context(&self) -> Arc<dyn AsyncEngineContext>;
}

/// A unary (single-response) asynchronous engine operation.
///
/// This trait combines `Future` semantics with context provider capabilities,
/// representing a single async operation that produces one result.
pub trait AsyncEngineUnary<Resp: Data>:
    Future<Output = Resp> + AsyncEngineContextProvider + Send
{
}

/// A streaming asynchronous engine operation.
///
/// This trait combines `Stream` semantics with context provider capabilities,
/// representing a continuous async operation that produces multiple messages over time.
///
/// - **Output side:** wrapped as [`EngineStream<T>`] = `crate::pipeline::ManyOut<T>`
///   — the stream of response chunks an engine emits.
/// - **Input side:** same `EngineStream<T>` shape, exposed as
///   `crate::pipeline::ManyIn<T>` for documentary clarity at the call site.
///
/// [`ResponseStream`] is the canonical concrete implementor; [`RequestStream`]
/// is a type alias of it for the input side.
pub trait AsyncEngineStream<T: Data>: Stream<Item = T> + AsyncEngineContextProvider + Send {}

/// Engine is a trait that defines the interface for a streaming engine.
/// The synchronous Engine version is does not need to be awaited.
///
/// This is the core trait for all async engine implementations. It provides:
/// - Generic type parameters for request, response, and error types
/// - Async generation capabilities with proper error handling
/// - Thread-safe design with `Send + Sync` bounds
///
/// ## Type Parameters
/// - `Req`: The request type — required to be `Send + 'static`. The `Sync`
///   bound was removed from `Req` for convenience: forcing `Sync` on `Req`
///   propagates a `+ Sync` constraint onto every type that flows in (in
///   particular, every input-side trait-object alias), and no
///   existing implementation of `AsyncEngine` relies on the `Sync` nature of
///   the request. Revisit if a future implementation genuinely needs
///   shared-reference access to a request value across threads.
/// - `Resp`: The response type that implements `AsyncEngineContextProvider`
/// - `E`: The error type that implements `Data`
///
/// ## Implementation Notes
/// Implementations should ensure proper error handling and resource management.
/// The `generate` method should be cancellable via the response's context provider.
#[async_trait]
// === SECTION: AsyncEngine 与响应流 ===

pub trait AsyncEngine<Req: Send + 'static, Resp: AsyncEngineContextProvider, E: Data>:
    Send + Sync
{
    /// Generate a stream of completion responses.
    // 中文说明：
    // 1. 接收一个请求对象并异步生成结果。
    // 2. 成功时返回实现了上下文提供能力的响应对象，失败时返回对应错误类型。
    async fn generate(&self, request: Req) -> Result<Resp, E>;
}

/// Adapter for a [`DataStream`] to a [`ResponseStream`].
///
/// A common pattern is to consume the [`ResponseStream`] with standard stream combinators
/// which produces a [`DataStream`] stream, then form a [`ResponseStream`] by propagating the
/// original [`AsyncEngineContext`].
pub struct ResponseStream<R: Data> {
    stream: DataStream<R>,
    ctx: Arc<dyn AsyncEngineContext>,
}

impl<R: Data> ResponseStream<R> {
    // 中文说明：
    // 1. 这个构造函数把一个普通数据流和对应上下文组合成 `ResponseStream`。
    // 2. 先构造结构体本身，把流和上下文绑定在一起。
    // 3. 然后再把它放进 `Pin<Box<_>>` 中，满足异步流在轮询时对固定地址的要求。
    pub fn new(stream: DataStream<R>, ctx: Arc<dyn AsyncEngineContext>) -> Pin<Box<Self>> {
        let response_stream = Self { stream, ctx };
        Box::pin(response_stream)
    }
}

impl<R: Data> Stream for ResponseStream<R> {
    type Item = R;

    #[inline]
    // 中文说明：
    // 1. 这个函数是 `ResponseStream` 的核心轮询入口，用于从底层流拉取下一项数据。
    // 2. 代码先通过 `get_mut` 拿到当前结构体的可变引用，再定位到内部保存的 `stream` 字段。
    // 3. 最后把轮询工作直接委托给底层数据流，并把结果原样返回给调用方。
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();
        let stream = &mut this.stream;

        Pin::new(stream).poll_next(cx)
    }
}

impl<R: Data> AsyncEngineStream<R> for ResponseStream<R> {}

impl<R: Data> AsyncEngineContextProvider for ResponseStream<R> {
    // 中文说明：
    // 1. 这个函数返回响应流绑定的上下文对象。
    // 2. 由于上下文字段是 `Arc`，这里通过 `Arc::clone` 增加一个共享引用计数。
    // 3. 返回后的调用方可以独立持有上下文，而不会影响原始流对象的所有权。
    fn context(&self) -> Arc<dyn AsyncEngineContext> {
        Arc::clone(&self.ctx)
    }
}

impl<R: Data> Debug for ResponseStream<R> {
    // 中文说明：
    // 1. 这个函数定义 `ResponseStream` 的调试输出格式。
    // 2. 它先创建一个名为 `ResponseStream` 的 debug builder。
    // 3. 当前实现只把上下文字段放进调试信息里，底层流本身暂时不展开打印。
    // 4. 最后调用 `finish` 生成完整的调试结构输出。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("ResponseStream");
        // todo: add debug for stream - possibly propagate some information about what
        // engine created the stream
        // debug.field("stream", &self.stream);
        debug.field("ctx", &self.ctx);
        debug.finish()
    }
}

/// Input-side type alias of [`ResponseStream`] — same struct, different name to
/// signal role at the call site.
///
/// The shape is identical: a `(stream, ctx)` pair that implements [`Stream`],
/// [`AsyncEngineContextProvider`], and [`AsyncEngineStream`]. Use `RequestStream`
/// when you're constructing a value to feed into the `Req` slot of an engine,
/// and [`ResponseStream`] when constructing a value to emit from the `Resp` slot.
/// Functionally interchangeable.
pub type RequestStream<R> = ResponseStream<R>;

impl<T: Data> AsyncEngineContextProvider for Pin<Box<dyn AsyncEngineUnary<T>>> {
    // 中文说明：
    // 1. 这个函数为装箱后的 unary engine 结果提供统一的上下文访问方式。
    // 2. 代码先把双层指针解引用成底层 trait object 引用。
    // 3. 然后把真正的 `context()` 调用委托给底层对象实现。
    fn context(&self) -> Arc<dyn AsyncEngineContext> {
        let provider = &**self;
        provider.context()
    }
}

impl<T: Data> AsyncEngineContextProvider for Pin<Box<dyn AsyncEngineStream<T>>> {
    // 中文说明：
    // 1. 这个函数为装箱后的 stream engine 结果提供统一的上下文访问方式。
    // 2. 它先取得底层 trait object 的引用。
    // 3. 再把上下文访问请求转发给底层实现，保证 boxed 形式与原始对象行为一致。
    fn context(&self) -> Arc<dyn AsyncEngineContext> {
        let provider = &**self;
        provider.context()
    }
}

/// A type-erased `AsyncEngine`.
///
/// This trait enables storing heterogeneous `AsyncEngine` implementations in collections
/// by erasing their specific generic type parameters. It provides runtime type information
/// and safe downcasting capabilities.
///
/// ## Type Erasure Mechanism
/// The trait uses `std::any::TypeId` to preserve type information at runtime, allowing
/// safe downcasting back to the original `AsyncEngine<Req, Resp, E>` types.
///
/// ## Safety Guarantees
/// - Type IDs are preserved exactly as they were during type erasure
/// - Downcasting is only possible to the original type combination
/// - Incorrect downcasts return `None` rather than panicking
///
/// ## Implementation Notes
/// This trait is implemented by the internal `AnyEngineWrapper` struct. Users should
/// not implement this trait directly - use the `AsAnyAsyncEngine` extension trait instead.
// === SECTION: 类型擦除 AnyAsyncEngine ===

pub trait AnyAsyncEngine: Send + Sync {
    /// Returns the `TypeId` of the request type used by this engine.
    // 中文说明：
    // 1. 返回当前被擦除引擎对应请求类型的运行时类型标识。
    // 2. 调用方可用它在运行时做安全的类型匹配和下转型前检查。
    fn request_type_id(&self) -> TypeId;

    /// Returns the `TypeId` of the response type used by this engine.
    // 中文说明：
    // 1. 返回当前被擦除引擎对应响应类型的运行时类型标识。
    // 2. 它和请求、错误类型标识一起构成下转型的判定依据。
    fn response_type_id(&self) -> TypeId;

    /// Returns the `TypeId` of the error type used by this engine.
    // 中文说明：
    // 1. 返回当前被擦除引擎对应错误类型的运行时类型标识。
    // 2. 下转型逻辑会把它与目标错误类型一起做精确匹配。
    fn error_type_id(&self) -> TypeId;

    /// Provides access to the underlying engine as a `dyn Any` for downcasting.
    // 中文说明：
    // 1. 以 `dyn Any` 的形式暴露底层被包装引擎。
    // 2. 调用方随后可以在类型 id 校验通过后继续做安全下转型。
    fn as_any(&self) -> &dyn Any;
}

/// An internal wrapper to hold a typed `AsyncEngine` behind the `AnyAsyncEngine` trait object.
///
/// This struct uses `PhantomData<fn(Req, Resp, E)>` to maintain the type relationship
/// without storing the types directly, enabling the type-erasure mechanism.
///
/// ## PhantomData Usage
/// The `PhantomData<fn(Req, Resp, E)>` ensures that the compiler knows about the
/// generic type parameters without requiring them to be `'static`, which would
/// prevent storing non-static types in the engine.
struct AnyEngineWrapper<Req, Resp, E>
where
    Req: Data,
    Resp: Data + AsyncEngineContextProvider,
    E: Data,
{
    engine: Arc<dyn AsyncEngine<Req, Resp, E>>,
    _phantom: PhantomData<fn(Req, Resp, E)>,
}

impl<Req, Resp, E> AnyAsyncEngine for AnyEngineWrapper<Req, Resp, E>
where
    Req: Data,
    Resp: Data + AsyncEngineContextProvider,
    E: Data,
{
    // 中文说明：
    // 1. 返回包装器记录的请求类型 `Req` 的 `TypeId`。
    // 2. 该值用于运行时确认当前被擦除引擎原本接收什么请求类型。
    fn request_type_id(&self) -> TypeId {
        let request_type_id = TypeId::of::<Req>();
        request_type_id
    }

    // 中文说明：
    // 1. 返回包装器记录的响应类型 `Resp` 的 `TypeId`。
    // 2. 运行时下转型会用它判断目标响应类型是否与原始引擎一致。
    fn response_type_id(&self) -> TypeId {
        let response_type_id = TypeId::of::<Resp>();
        response_type_id
    }

    // 中文说明：
    // 1. 返回包装器记录的错误类型 `E` 的 `TypeId`。
    // 2. 这保证错误类型也必须严格匹配，避免错误的类型擦除恢复。
    fn error_type_id(&self) -> TypeId {
        let error_type_id = TypeId::of::<E>();
        error_type_id
    }

    // 中文说明：
    // 1. 以 `dyn Any` 视角返回内部真正保存的引擎对象。
    // 2. 这样外层的下转型逻辑就可以在确认类型匹配后，把它还原成具体的 `Arc<dyn AsyncEngine<...>>`。
    fn as_any(&self) -> &dyn Any {
        let engine = &self.engine;
        engine
    }
}

/// An extension trait that provides a convenient way to type-erase an `AsyncEngine`.
///
/// This trait provides the `.into_any_engine()` method on any `Arc<dyn AsyncEngine<...>>`,
/// enabling ergonomic type erasure without explicit wrapper construction.
///
/// ## Usage
/// ```rust,ignore
/// use crate::engine::AsAnyAsyncEngine;
///
/// let typed_engine: Arc<dyn AsyncEngine<String, String, ()>> = Arc::new(MyEngine::new());
/// let any_engine = typed_engine.into_any_engine();
/// ```
// === SECTION: 升入与下转辅助 trait ===

pub trait AsAnyAsyncEngine {
    /// Converts a typed `AsyncEngine` into a type-erased `AnyAsyncEngine`.
    // 中文说明：
    // 1. 把一个带具体泛型参数的引擎转换成类型擦除后的统一接口。
    // 2. 调用方可以借此把不同类型的引擎放进同一个容器中管理。
    fn into_any_engine(self) -> Arc<dyn AnyAsyncEngine>;
}

impl<Req, Resp, E> AsAnyAsyncEngine for Arc<dyn AsyncEngine<Req, Resp, E>>
where
    Req: Data,
    Resp: Data + AsyncEngineContextProvider,
    E: Data,
{
    // 中文说明：
    // 1. 这个函数把具体类型的引擎包装进 `AnyEngineWrapper` 中。
    // 2. 包装器会保留底层引擎和类型关系信息，从而支持后续安全下转型。
    // 3. 最终返回的是一个 `Arc<dyn AnyAsyncEngine>`，便于放入异构集合。
    fn into_any_engine(self) -> Arc<dyn AnyAsyncEngine> {
        let wrapper = AnyEngineWrapper {
            engine: self,
            _phantom: PhantomData,
        };

        Arc::new(wrapper)
    }
}

/// An extension trait that provides a convenient method to downcast an `AnyAsyncEngine`.
///
/// This trait provides the `.downcast<Req, Resp, E>()` method on `Arc<dyn AnyAsyncEngine>`,
/// enabling safe downcasting back to the original typed engine.
///
/// ## Safety
/// The downcast method performs runtime type checking using `TypeId` comparison.
/// It will only succeed if the type parameters exactly match the original engine's types.
///
/// ## Usage
/// ```rust,ignore
/// use crate::engine::DowncastAnyAsyncEngine;
///
/// let any_engine: Arc<dyn AnyAsyncEngine> = // ... from collection
/// if let Some(typed_engine) = any_engine.downcast::<String, String, ()>() {
///     // Use the typed engine
///     let result = typed_engine.generate("hello".to_string()).await;
/// }
/// ```
pub trait DowncastAnyAsyncEngine {
    /// Attempts to downcast an `AnyAsyncEngine` to a specific `AsyncEngine` type.
    ///
    /// Returns `Some(engine)` if the type parameters match the original engine,
    /// or `None` if the types don't match.
    // 中文说明：
    // 1. 尝试把类型擦除后的引擎恢复成指定泛型参数的具体引擎接口。
    // 2. 只有请求、响应和错误三种类型都与原始引擎完全一致时才会成功。
    fn downcast<Req, Resp, E>(&self) -> Option<Arc<dyn AsyncEngine<Req, Resp, E>>>
    where
        Req: Data,
        Resp: Data + AsyncEngineContextProvider,
        E: Data;
}

impl DowncastAnyAsyncEngine for Arc<dyn AnyAsyncEngine> {
    // 中文说明：
    // 1. 这个函数先分别比较请求、响应和错误三种类型的 `TypeId`，判断当前被擦除引擎是否与目标类型完全匹配。
    // 2. 如果任意一项不匹配，就立即返回 `None`，避免错误下转型。
    // 3. 只有三项都匹配时，才继续通过 `as_any()` 取得底层对象并执行具体的 `downcast_ref`。
    // 4. 下转型成功后克隆内部 `Arc` 返回，让调用方重新拿到可用的具体引擎对象。
    fn downcast<Req, Resp, E>(&self) -> Option<Arc<dyn AsyncEngine<Req, Resp, E>>>
    where
        Req: Data,
        Resp: Data + AsyncEngineContextProvider,
        E: Data,
    {
        let request_matches = self.request_type_id() == TypeId::of::<Req>();
        let response_matches = self.response_type_id() == TypeId::of::<Resp>();
        let error_matches = self.error_type_id() == TypeId::of::<E>();
        let types_match = request_matches && response_matches && error_matches;

        if !types_match {
            return None;
        }

        let typed_engine = self
            .as_any()
            .downcast_ref::<Arc<dyn AsyncEngine<Req, Resp, E>>>();

        typed_engine.cloned()
    }
}

// === SECTION: tests ===

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // 1. Define mock data structures
    #[derive(Debug, PartialEq)]
    struct Req1(String);

    #[derive(Debug, PartialEq)]
    struct Resp1(String);

    // Dummy context provider implementation for the response
    impl AsyncEngineContextProvider for Resp1 {
        fn context(&self) -> Arc<dyn AsyncEngineContext> {
            // For this test, we don't need a real context.
            unimplemented!()
        }
    }

    #[derive(Debug)]
    struct Err1;

    // A different set of types for testing failure cases
    #[derive(Debug)]
    struct Req2;
    #[derive(Debug)]
    struct Resp2;
    impl AsyncEngineContextProvider for Resp2 {
        fn context(&self) -> Arc<dyn AsyncEngineContext> {
            unimplemented!()
        }
    }

    // 2. Define a mock engine
    struct MockEngine;

    #[async_trait]
    impl AsyncEngine<Req1, Resp1, Err1> for MockEngine {
        async fn generate(&self, request: Req1) -> Result<Resp1, Err1> {
            Ok(Resp1(format!("response to {}", request.0)))
        }
    }

    #[tokio::test]
    async fn test_engine_type_erasure_and_downcast() {
        // 3. Create a typed engine
        let typed_engine: Arc<dyn AsyncEngine<Req1, Resp1, Err1>> = Arc::new(MockEngine);

        // 4. Use the extension trait to erase the type
        let any_engine = typed_engine.into_any_engine();

        // Check type IDs are preserved
        assert_eq!(any_engine.request_type_id(), TypeId::of::<Req1>());
        assert_eq!(any_engine.response_type_id(), TypeId::of::<Resp1>());
        assert_eq!(any_engine.error_type_id(), TypeId::of::<Err1>());

        // 5. Use the new downcast method on the Arc
        let downcasted_engine = any_engine.downcast::<Req1, Resp1, Err1>();

        // 6. Assert success
        assert!(downcasted_engine.is_some());

        // We can even use the downcasted engine
        let response = downcasted_engine
            .unwrap()
            .generate(Req1("hello".to_string()))
            .await;
        assert_eq!(response.unwrap(), Resp1("response to hello".to_string()));

        // 7. Assert failure for wrong types
        let failed_downcast = any_engine.downcast::<Req2, Resp2, Err1>();
        assert!(failed_downcast.is_none());

        // 8. HashMap usage test
        let mut engine_map: HashMap<String, Arc<dyn AnyAsyncEngine>> = HashMap::new();
        engine_map.insert("mock".to_string(), any_engine);

        let retrieved_engine = engine_map.get("mock").unwrap();
        let final_engine = retrieved_engine.downcast::<Req1, Resp1, Err1>().unwrap();
        let final_response = final_engine.generate(Req1("world".to_string())).await;
        assert_eq!(
            final_response.unwrap(),
            Resp1("response to world".to_string())
        );
    }

    // === SECTION: 合并自原 mod supplemental_tests ===
    use futures::{stream, StreamExt};
    use std::task::{Context as TaskContext, Poll};

    #[derive(Debug)]
    struct TestContext {
        id: String,
    }

    #[async_trait]
    impl AsyncEngineContext for TestContext {
        fn id(&self) -> &str {
            &self.id
        }

        fn is_stopped(&self) -> bool {
            false
        }

        fn is_killed(&self) -> bool {
            false
        }

        async fn stopped(&self) {}

        async fn killed(&self) {}

        fn stop_generating(&self) {}

        fn stop(&self) {}

        fn kill(&self) {}

        fn link_child(&self, _child: Arc<dyn AsyncEngineContext>) {}
    }

    #[derive(Debug)]
    struct TestUnary {
        value: Option<i32>,
        ctx: Arc<dyn AsyncEngineContext>,
    }

    impl Future for TestUnary {
        type Output = i32;

        fn poll(mut self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
            Poll::Ready(self.value.take().unwrap())
        }
    }

    impl AsyncEngineContextProvider for TestUnary {
        fn context(&self) -> Arc<dyn AsyncEngineContext> {
            self.ctx.clone()
        }
    }

    impl AsyncEngineUnary<i32> for TestUnary {}

    #[derive(Debug)]
    struct EchoResp {
        message: String,
        ctx: Arc<dyn AsyncEngineContext>,
    }

    impl AsyncEngineContextProvider for EchoResp {
        fn context(&self) -> Arc<dyn AsyncEngineContext> {
            self.ctx.clone()
        }
    }

    #[derive(Debug)]
    struct EchoErr;

    #[derive(Debug)]
    struct OtherResp;

    impl AsyncEngineContextProvider for OtherResp {
        fn context(&self) -> Arc<dyn AsyncEngineContext> {
            Arc::new(TestContext {
                id: "other-resp".to_string(),
            })
        }
    }

    #[derive(Debug)]
    struct OtherErr;

    struct EchoEngine {
        ctx: Arc<dyn AsyncEngineContext>,
    }

    #[async_trait]
    impl AsyncEngine<String, EchoResp, EchoErr> for EchoEngine {
        async fn generate(&self, request: String) -> Result<EchoResp, EchoErr> {
            Ok(EchoResp {
                message: format!("echo:{request}"),
                ctx: self.ctx.clone(),
            })
        }
    }

    #[tokio::test]
    async fn test_supplemental_response_stream_new_context_debug_and_items() {
        let ctx: Arc<dyn AsyncEngineContext> = Arc::new(TestContext {
            id: "response-stream".to_string(),
        });

        let response_stream = ResponseStream::new(Box::pin(stream::iter(vec![1, 2, 3])), ctx.clone());

        assert!(Arc::ptr_eq(&response_stream.as_ref().get_ref().context(), &ctx));
        assert_eq!(response_stream.as_ref().get_ref().context().id(), "response-stream");

        let debug_repr = format!("{:?}", response_stream.as_ref().get_ref());
        assert!(debug_repr.contains("ResponseStream"));
        assert!(debug_repr.contains("response-stream"));

        let collected: Vec<_> = response_stream.collect().await;
        assert_eq!(collected, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_supplemental_engine_stream_conversion_and_boxed_stream_context() {
        let ctx: Arc<dyn AsyncEngineContext> = Arc::new(TestContext {
            id: "boxed-stream".to_string(),
        });

        let stream_obj: Pin<Box<dyn AsyncEngineStream<i32>>> =
            ResponseStream::new(Box::pin(stream::iter(vec![4, 5])), ctx.clone());
        assert!(Arc::ptr_eq(&stream_obj.context(), &ctx));

        let collected_from_trait_object: Vec<_> = stream_obj.collect().await;
        assert_eq!(collected_from_trait_object, vec![4, 5]);

        let engine_stream: EngineStream<i32> =
            ResponseStream::new(Box::pin(stream::iter(vec![7, 8, 9])), ctx.clone());
        let data_stream: DataStream<i32> = engine_stream.into();
        let collected_from_data_stream: Vec<_> = data_stream.collect().await;
        assert_eq!(collected_from_data_stream, vec![7, 8, 9]);

        let request_stream: Pin<Box<RequestStream<i32>>> =
            ResponseStream::new(Box::pin(stream::iter(vec![11, 12])), ctx.clone());
        let collected_from_request_stream: Vec<_> = request_stream.collect().await;
        assert_eq!(collected_from_request_stream, vec![11, 12]);
    }

    #[tokio::test]
    async fn test_supplemental_boxed_unary_context_provider() {
        let ctx: Arc<dyn AsyncEngineContext> = Arc::new(TestContext {
            id: "boxed-unary".to_string(),
        });

        let unary: Pin<Box<dyn AsyncEngineUnary<i32>>> = Box::pin(TestUnary {
            value: Some(13),
            ctx: ctx.clone(),
        });

        assert!(Arc::ptr_eq(&unary.context(), &ctx));
        assert_eq!(unary.context().id(), "boxed-unary");

        let value = unary.await;
        assert_eq!(value, 13);
    }

    #[tokio::test]
    async fn test_supplemental_any_engine_as_any_and_downcast_mismatch_matrix() {
        let ctx: Arc<dyn AsyncEngineContext> = Arc::new(TestContext {
            id: "engine".to_string(),
        });
        let typed_engine: Arc<dyn AsyncEngine<String, EchoResp, EchoErr>> =
            Arc::new(EchoEngine { ctx: ctx.clone() });

        let any_engine = typed_engine.into_any_engine();

        assert!(any_engine
            .as_any()
            .is::<Arc<dyn AsyncEngine<String, EchoResp, EchoErr>>>());
        assert!(!any_engine
            .as_any()
            .is::<Arc<dyn AsyncEngine<String, OtherResp, EchoErr>>>());

        let downcasted = any_engine.downcast::<String, EchoResp, EchoErr>();
        assert!(downcasted.is_some());

        let response = downcasted.unwrap().generate("hello".to_string()).await.unwrap();
        assert_eq!(response.message, "echo:hello");
        assert!(Arc::ptr_eq(&response.context(), &ctx));

        let wrong_request = any_engine.downcast::<u64, EchoResp, EchoErr>();
        let wrong_response = any_engine.downcast::<String, OtherResp, EchoErr>();
        let wrong_error = any_engine.downcast::<String, EchoResp, OtherErr>();

        assert!(wrong_request.is_none());
        assert!(wrong_response.is_none());
        assert!(wrong_error.is_none());
    }
}

