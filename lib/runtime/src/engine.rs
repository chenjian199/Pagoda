// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 设计意图
//! 为 Pagoda 异步流式引擎提供统一抽象与类型擦除支持。核心目标:
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

/// 任何满足 [`Send`] + [`Sync`] + `'static` 的类型都可用作 [`AsyncEngine`] 的请求与响应类型。
///
/// 该 trait 以“毯子实现”（blanket impl）的方式为所有符合约束的类型自动提供。
/// **不要手动实现此 trait** —— 毯子实现已涵盖所有合法类型。
pub trait Data: Send + Sync + 'static {}
impl<T: Send + Sync + 'static> Data for T {}

/// [`DataStream`] 是 [`Data`] 项流的类型别名。通过关联一个 [`AsyncEngineContext`]，
/// 可以将其适配为 [`ResponseStream`]。
pub type DataUnary<T> = Pin<Box<dyn Future<Output = T> + Send>>;
pub type DataStream<T> = Pin<Box<dyn Stream<Item = T> + Send>>;

pub type Engine<Req, Resp, E> = Arc<dyn AsyncEngine<Req, Resp, E>>;
pub type EngineUnary<Resp> = Pin<Box<dyn AsyncEngineUnary<Resp>>>;
/// [`AsyncEngineStream`] 的 trait 对象别名 —— 在引擎的两侧都会用到：
/// 输入侧通过 [`crate::pipeline::ManyIn`]，输出侧通过
/// [`crate::pipeline::ManyOut`]。这些方向性名称存在于
/// [`crate::pipeline`] 别名层，仅为了在使用点上提升文档可读性。
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

// 当 https://github.com/rust-lang/rust/issues/65991 稳定后，再合并 Controller 与 Context
pub trait AsyncEngineController: Send + Sync {}

/// [`AsyncEngineContext`] trait 定义了控制引擎所产生结果流的接口。
///
/// 该 trait 为异步操作提供生命周期管理，包括：
/// - 通过唯一 ID 标识流；
/// - 优雅停止能力（`stop_generating`）；
/// - 立即终止能力（`kill`）；
/// - 对 stopped/killed 状态的查询。
///
/// 实现方应保证线程安全与并发访问下的状态管理正确性。
#[async_trait]
pub trait AsyncEngineContext: Send + Sync + Debug {
    /// 返回当前上下文对应流的唯一标识，可用于日志追踪、关联记录或调试输出。
    fn id(&self) -> &str;

    /// 查询当前上下文是否已收到“停止继续生成”信号；
    /// 返回 `true` 表示上游应尽快停止产生新结果，但不一定立即中断现有输出。
    fn is_stopped(&self) -> bool;

    /// 查询当前上下文是否已收到“立即终止”信号。
    /// 可配合 `.take_while()` 流组合子在最下游返回流上立即终止整条流。
    /// 返回 `true` 时，调用方通常应停止继续消费并尽快结束整条流。
    fn is_killed(&self) -> bool;

    /// 异步等待上下文进入 stopped 状态：若当前已停止则立即返回，
    /// 否则一直等到实现方把状态切换为已停止。
    async fn stopped(&self);

    /// 异步等待上下文进入 killed 状态：若当前已被标记为终止则立即返回，
    /// 否则等待终止信号真正到达。
    async fn killed(&self);

    // 控制器

    /// 通知 [`AsyncEngine`] 停止继续为当前流生成新结果。
    /// 该操作是幂等的，且不会使流中已有结果失效；引擎可能需要一段时间才真正停止，
    /// 调用方可以选择继续排空（drain）该流或直接将其丢弃。
    fn stop_generating(&self);

    /// `stop_generating` 的便捷别名，参见 [`AsyncEngineContext::stop_generating`]；
    /// 实现方通常会把它映射到相同的停止逻辑。
    fn stop(&self);

    /// 在 [`AsyncEngineContext::stop_generating`] 基础上，进一步表达“终止时不排空剩余项”的意图；
    /// 是否立即丢弃未消费结果取决于具体实现，并非所有引擎都支持，但语义上比 `stop` 更强。
    fn kill(&self);

    /// 把一个子 `AsyncEngineContext` 挂接到当前上下文，形成联动的生命周期控制关系：
    /// 当对当前上下文调用 `stop_generating`、`stop` 或 `kill` 时，会按链接顺序对所有
    /// 子上下文调用相同方法，随后当前上下文自身的方法再继续执行。
    fn link_child(&self, child: Arc<dyn AsyncEngineContext>);
}

/// 提供对某次引擎操作所关联 [`AsyncEngineContext`] 的访问能力。
///
/// 单次（unary）响应与流式响应都会实现该 trait，从而无论操作类型如何，
/// 都能以统一方式访问上下文信息。
pub trait AsyncEngineContextProvider: Send + Debug {
    /// 返回当前对象关联的 `AsyncEngineContext`，
    /// 调用方可借此统一访问流或单次响应背后的生命周期控制对象。
    fn context(&self) -> Arc<dyn AsyncEngineContext>;
}

/// 一次性（单响应）的异步引擎操作。
///
/// 该 trait 把 `Future` 语义与上下文提供能力结合在一起，
/// 表示一次只产生单个结果的异步操作。
pub trait AsyncEngineUnary<Resp: Data>:
    Future<Output = Resp> + AsyncEngineContextProvider + Send
{
}

/// 流式异步引擎操作。
///
/// 该 trait 把 `Stream` 语义与上下文提供能力结合在一起，
/// 表示一个持续产生多条消息的异步操作。
///
/// - **输出侧：** 包装为 [`EngineStream<T>`] = `crate::pipeline::ManyOut<T>`，
///   即引擎对外发出的响应分片流。
/// - **输入侧：** 形状与之相同的 `EngineStream<T>`，在使用点上以
///   `crate::pipeline::ManyIn<T>` 暴露，仅为提升文档可读性。
///
/// [`ResponseStream`] 是其规范的具体实现；[`RequestStream`] 则是它在输入侧的类型别名。
pub trait AsyncEngineStream<T: Data>: Stream<Item = T> + AsyncEngineContextProvider + Send {}

/// 定义流式引擎接口的 trait；其同步版本无需 `await`。
///
/// 这是所有异步引擎实现的核心 trait，提供：
/// - 请求、响应、错误三种类型的泛型参数；
/// - 带完善错误处理的异步生成能力；
/// - 通过 `Send + Sync` 约束保证的线程安全设计。
///
/// ## 类型参数
/// - `Req`：请求类型，要求满足 `Send + 'static`。出于便利，`Req` 上去掉了 `Sync`
///   约束：若强制要求 `Req: Sync`，会把 `+ Sync` 约束传播到所有流入的类型上
///   （尤其是每一个输入侧 trait 对象别名），而现有 `AsyncEngine` 实现都不依赖请求的
///   `Sync` 特性。如果将来确有实现需要跨线程共享引用访问请求值，再重新评估。
/// - `Resp`：实现了 `AsyncEngineContextProvider` 的响应类型；
/// - `E`：实现了 `Data` 的错误类型。
///
/// ## 实现注意事项
/// 实现方应保证完善的错误处理与资源管理，`generate` 方法应可通过响应的上下文提供者取消。
#[async_trait]
// === SECTION: AsyncEngine 与响应流 ===

pub trait AsyncEngine<Req: Send + 'static, Resp: AsyncEngineContextProvider, E: Data>:
    Send + Sync
{
    /// 接收一个请求对象并异步生成结果：
    /// 成功时返回实现了上下文提供能力的响应对象，失败时返回对应错误类型。
    async fn generate(&self, request: Req) -> Result<Resp, E>;
}

/// 把 [`DataStream`] 适配为 [`ResponseStream`] 的适配器。
///
/// 常见用法是：先用标准流组合子消费 [`ResponseStream`] 得到一个 [`DataStream`]，
/// 再通过传递原始的 [`AsyncEngineContext`] 重新构成 [`ResponseStream`]。
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
        // 待办：为 stream 补充调试输出，可考虑携带“由哪个引擎创建该流”等信息。
        debug.field("ctx", &self.ctx);
        debug.finish()
    }
}

/// [`ResponseStream`] 在输入侧的类型别名 —— 同一个结构体，仅以不同名称在使用点上标明角色。
///
/// 其形状完全一致：一个 `(stream, ctx)` 组合，实现了 [`Stream`]、
/// [`AsyncEngineContextProvider`] 和 [`AsyncEngineStream`]。当构造一个值要喂入引擎的
/// `Req` 槽位时使用 `RequestStream`，当构造一个值要从 `Resp` 槽位发出时使用
/// [`ResponseStream`]。二者功能上可互换。
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

/// 类型擦除后的 `AsyncEngine`。
///
/// 该 trait 通过擦除具体泛型参数，使不同类型的 `AsyncEngine` 实现能够被放入同一集合；
/// 它提供运行时类型信息与安全的下转型能力。
///
/// ## 类型擦除机制
/// 该 trait 使用 `std::any::TypeId` 在运行时保留类型信息，
/// 从而允许安全地下转型回原始的 `AsyncEngine<Req, Resp, E>` 类型。
///
/// ## 安全保证
/// - 类型 ID 在类型擦除过程中被原样保留；
/// - 只能下转型回原始的类型组合；
/// - 错误的下转型返回 `None` 而不是 panic。
///
/// ## 实现注意事项
/// 该 trait 由内部的 `AnyEngineWrapper` 结构体实现。使用者不应直接实现该 trait，
/// 而应使用 `AsAnyAsyncEngine` 扩展 trait。
// === SECTION: 类型擦除 AnyAsyncEngine ===

pub trait AnyAsyncEngine: Send + Sync {
    /// 返回当前被擦除引擎对应请求类型的运行时类型标识（`TypeId`），
    /// 调用方可用它在运行时做安全的类型匹配与下转型前检查。
    fn request_type_id(&self) -> TypeId;

    /// 返回当前被擦除引擎对应响应类型的 `TypeId`，
    /// 它与请求、错误类型标识一起构成下转型的判定依据。
    fn response_type_id(&self) -> TypeId;

    /// 返回当前被擦除引擎对应错误类型的 `TypeId`，
    /// 下转型逻辑会把它与目标错误类型一起做精确匹配。
    fn error_type_id(&self) -> TypeId;

    /// 以 `dyn Any` 的形式暴露底层被包装引擎，
    /// 调用方随后可在类型 ID 校验通过后继续做安全下转型。
    fn as_any(&self) -> &dyn Any;
}

/// 内部包装器，用于把带具体类型的 `AsyncEngine` 藏在 `AnyAsyncEngine` trait 对象背后。
///
/// 该结构体使用 `PhantomData<fn(Req, Resp, E)>` 在不直接存储类型的前提下保持类型关系，
/// 从而支撑类型擦除机制。
///
/// ## PhantomData 用法
/// `PhantomData<fn(Req, Resp, E)>` 让编译器能感知这些泛型参数，而又不要求它们为 `'static`，
/// 否则将无法在引擎中存储非 static 类型。
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

/// 一个扩展 trait，为 `AsyncEngine` 提供便捷的类型擦除方式。
///
/// 该 trait 为任意 `Arc<dyn AsyncEngine<...>>` 提供 `.into_any_engine()` 方法，
/// 从而无需显式构造包装器即可优雅地完成类型擦除。
///
/// ## 用法
/// ```rust,ignore
/// use crate::engine::AsAnyAsyncEngine;
///
/// let typed_engine: Arc<dyn AsyncEngine<String, String, ()>> = Arc::new(MyEngine::new());
/// let any_engine = typed_engine.into_any_engine();
/// ```
// === SECTION: 升入与下转辅助 trait ===

pub trait AsAnyAsyncEngine {
    /// 把一个带具体泛型参数的 `AsyncEngine` 转换为类型擦除后的 `AnyAsyncEngine`，
    /// 调用方可借此把不同类型的引擎放进同一个容器中统一管理。
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

/// 一个扩展 trait，为 `AnyAsyncEngine` 提供便捷的下转型方法。
///
/// 该 trait 为 `Arc<dyn AnyAsyncEngine>` 提供 `.downcast<Req, Resp, E>()` 方法，
/// 从而能安全地下转型回原始的具体引擎。
///
/// ## 安全性
/// 下转型方法通过比较 `TypeId` 在运行时做类型检查，
/// 仅当类型参数与原始引擎类型完全一致时才会成功。
///
/// ## 用法
/// ```rust,ignore
/// use crate::engine::DowncastAnyAsyncEngine;
///
/// let any_engine: Arc<dyn AnyAsyncEngine> = // ... 从集合中取出
/// if let Some(typed_engine) = any_engine.downcast::<String, String, ()>() {
///     // 使用还原后的具体引擎
///     let result = typed_engine.generate("hello".to_string()).await;
/// }
/// ```
pub trait DowncastAnyAsyncEngine {
    /// 尝试把 `AnyAsyncEngine` 下转型为指定的 `AsyncEngine` 类型：
    /// 若类型参数匹配原引擎则返回 `Some(engine)`，类型不匹配则返回 `None`。
    /// 只有请求、响应和错误三种类型都与原始引擎完全一致时才会成功。
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

    // 1. 定义模拟数据结构
    #[derive(Debug, PartialEq)]
    struct Req1(String);

    #[derive(Debug, PartialEq)]
    struct Resp1(String);

    // 为响应类型提供的占位用上下文提供者实现
    impl AsyncEngineContextProvider for Resp1 {
        fn context(&self) -> Arc<dyn AsyncEngineContext> {
            // 本测试不需要真正的上下文。
            unimplemented!()
        }
    }

    #[derive(Debug)]
    struct Err1;

    // 一组不同的类型，用于测试失败场景
    #[derive(Debug)]
    struct Req2;
    #[derive(Debug)]
    struct Resp2;
    impl AsyncEngineContextProvider for Resp2 {
        fn context(&self) -> Arc<dyn AsyncEngineContext> {
            unimplemented!()
        }
    }

    // 2. 定义一个模拟引擎
    struct MockEngine;

    #[async_trait]
    impl AsyncEngine<Req1, Resp1, Err1> for MockEngine {
        async fn generate(&self, request: Req1) -> Result<Resp1, Err1> {
            Ok(Resp1(format!("response to {}", request.0)))
        }
    }

    #[tokio::test]
    async fn test_engine_type_erasure_and_downcast() {
        // 3. 创建一个带类型的引擎
        let typed_engine: Arc<dyn AsyncEngine<Req1, Resp1, Err1>> = Arc::new(MockEngine);

        // 4. 使用扩展 trait 擦除类型
        let any_engine = typed_engine.into_any_engine();

        // 检查类型 ID 被保留
        assert_eq!(any_engine.request_type_id(), TypeId::of::<Req1>());
        assert_eq!(any_engine.response_type_id(), TypeId::of::<Resp1>());
        assert_eq!(any_engine.error_type_id(), TypeId::of::<Err1>());

        // 5. 在 Arc 上使用新的 downcast 方法
        let downcasted_engine = any_engine.downcast::<Req1, Resp1, Err1>();

        // 6. 断言成功
        assert!(downcasted_engine.is_some());

        // 甚至可以直接使用下转型后的引擎
        let response = downcasted_engine
            .unwrap()
            .generate(Req1("hello".to_string()))
            .await;
        assert_eq!(response.unwrap(), Resp1("response to hello".to_string()));

        // 7. 错误类型下转型应该失败
        let failed_downcast = any_engine.downcast::<Req2, Resp2, Err1>();
        assert!(failed_downcast.is_none());

        // 8. HashMap 使用测试
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

