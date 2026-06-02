// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 设计意图
//! `TaskTracker` 是一套可组合的任务管理体系，目标是为复杂业务场景提供
//! 「调度策略 + 失败处理 + 取消语义 + 度量上报」一体化的并发执行平台。
//! 体系强调可层次嵌套（父子 tracker 共享指标和取消信号），并通过策略对象
//! 解耦执行控制与业务逻辑。
//!
//! # 外部契约
//! - `TaskTracker`：核心执行器，对外暴露
//!   `spawn` / `spawn_cancellable` / `child` / `cancel` / `wait` 等方法；
//! - `TaskHandle<T>`：单任务句柄，支持 `await` 结果、取消、查询状态；
//! - `SchedulingPolicy`：`Unlimited` / `Semaphore(N)` 等调度策略；
//! - `OnErrorPolicy` + `OnErrorAction` + `ErrorPolicy` / `ErrorResponse`：
//!   决定任务失败后是停下、重试还是继续，并允许携带 `Continuation`；
//! - `FailedWithContinuation` / `FailedWithContinuationExt`：在 `anyhow::Error`
//!   上挂载续作对象，作为失败 → 恢复的标准载体；
//! - 度量名严格遵守 `metrics::prometheus_names::task_tracker` 暴露的常量。
//!
//! # 实现要点
//! - 调度走 `tokio::sync::Semaphore` 控制并发上限，`Unlimited` 用一个特殊
//!   值表示「绕过许可」；
//! - 取消信号通过 `tokio_util::sync::CancellationToken` 串通父子 tracker；
//! - 任务句柄通过 `tokio::task::JoinHandle` 与 `Continuation` 协作，
//!   失败 → 续作可在外层重新入队；
//! - 指标 (`tasks_issued/completed/failed/cancelled/active/queued`)
//!   通过 `AtomicU64` + Prometheus gauges 上报，按层级聚合。

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::metrics::MetricsHierarchy;
use crate::metrics::prometheus_names::task_tracker;
use anyhow::Result;
use async_trait::async_trait;
use derive_builder::Builder;
use std::collections::HashSet;
use std::sync::{Mutex, RwLock, Weak};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker as TokioTaskTracker;
use tracing::{Instrument, debug, error, warn};
use uuid::Uuid;

// === SECTION: TaskError ===

/// 任务执行结果的错误类型
///
/// 该枚举区分「任务取消」与「真实失败」，便于正确统计指标和处理错误。
#[derive(Error, Debug)]
pub enum TaskError {
    /// 任务被取消（来自取消 token 或 tracker 关停）
    #[error("Task was cancelled")]
    Cancelled,

    /// 任务以错误结束
    #[error(transparent)]
    Failed(#[from] anyhow::Error),

    /// 无法在已关闭的 tracker 上派发任务
    #[error("Cannot spawn task on a closed tracker")]
    TrackerClosed,
}

impl TaskError {
    /// 判断当前错误是否表示取消状态。
    pub fn is_cancellation(&self) -> bool {
        if let Self::Cancelled = self {
            return true;
        }

        false
    }

    /// 判断当前错误是否表示真实失败。
    pub fn is_failure(&self) -> bool {
        if let Self::Failed(_) = self {
            return true;
        }

        false
    }

    /// 把 `TaskError` 统一转换为 `anyhow::Error`。
    ///
    /// 处理流程是优先保留原始失败错误；取消和关闭状态则转换成统一文本错误。
    pub fn into_anyhow(self) -> anyhow::Error {
        if let Self::Failed(err) = self {
            return err;
        }

        if matches!(self, Self::Cancelled) {
            return anyhow::anyhow!("Task was cancelled");
        }

        anyhow::anyhow!("Cannot spawn task on a closed tracker")
    }
}

/// 指向已派发任务的句柄，同时提供 join 与取消控制能力
///
/// `TaskHandle` 包装一个 `JoinHandle`，并暴露该任务专属的取消 token，
/// 让调用方在保留熟悉的 `JoinHandle` API 的同时，对单个任务做细粒度控制。
///
/// # 示例
/// ```rust
/// # use pagoda_runtime::utils::tasks::tracker::*;
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let tracker = TaskTracker::new(UnlimitedScheduler::new(), LogOnlyPolicy::new())?;
/// let handle = tracker.spawn(async {
///     tokio::time::sleep(std::time::Duration::from_millis(100)).await;
///     Ok(42)
/// });
///
/// // 获取该任务的取消 token
/// let cancel_token = handle.cancellation_token();
///
/// // 可以取消这个特定任务
/// // cancel_token.cancel();
///
/// // 像普通 JoinHandle 一样 await 任务
/// let result = handle.await?;
/// assert_eq!(result?, 42);
/// # Ok(())
/// # }
/// ```
pub struct TaskHandle<T> {
    join_handle: JoinHandle<Result<T, TaskError>>,
    cancel_token: CancellationToken,
}

impl<T> TaskHandle<T> {
    /// 在内部用 `JoinHandle` 和取消 token 构造任务句柄。
    pub(crate) fn new(
        join_handle: JoinHandle<Result<T, TaskError>>,
        cancel_token: CancellationToken,
    ) -> Self {
        let handle = join_handle;
        let token = cancel_token;

        Self {
            join_handle: handle,
            cancel_token: token,
        }
    }

    /// 返回当前任务专属的取消 token。
    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.cancel_token
    }

    /// 立即中止底层 Tokio 任务。
    pub fn abort(&self) {
        self.join_handle.abort();
    }

    /// 判断底层任务是否已经结束。
    pub fn is_finished(&self) -> bool {
        self.join_handle.is_finished()
    }
}

impl<T> std::future::Future for TaskHandle<T> {
    type Output = Result<Result<T, TaskError>, tokio::task::JoinError>;

    /// 透传到底层 `JoinHandle` 的轮询逻辑。
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let join_handle = &mut self.join_handle;
        std::pin::Pin::new(join_handle).poll(cx)
    }
}

impl<T> std::fmt::Debug for TaskHandle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskHandle")
            .field("join_handle", &"<JoinHandle>")
            .field("cancel_token", &self.cancel_token)
            .finish()
    }
}

// === SECTION: Continuation 与续作 ===

/// 在任务失败后执行的续作任务 trait
///
/// 该 trait 让任务定义「失败之后该做什么」，从而免去复杂的类型擦除与
/// 执行器管理。任务实现此 trait 以提供清晰的续作逻辑。
#[async_trait]
pub trait Continuation: Send + Sync + std::fmt::Debug + std::any::Any {
    /// 在任务失败后执行续作任务
    ///
    /// 当某个任务失败且提供了续作时调用此方法。实现可以执行重试逻辑、
    /// 回退操作、结果转换或任何其他后续动作。
    /// 为保证灵活性，结果以类型擦除的 Box<dyn Any> 形式返回。
    async fn execute(
        &self,
        cancel_token: CancellationToken,
    ) -> TaskExecutionResult<Box<dyn std::any::Any + Send + 'static>>;
}

/// 表示任务失败但提供了续作的错误类型
///
/// 该错误类型携带一个可作为后续执行的续作任务。
/// 任务通过 Continuation trait 定义自身的续作逻辑。
#[derive(Error, Debug)]
#[error("Task failed with continuation: {source}")]
pub struct FailedWithContinuation {
    /// 导致任务失败的底层错误
    #[source]
    pub source: anyhow::Error,
    /// 用于后续执行的续作任务
    pub continuation: Arc<dyn Continuation + Send + Sync + 'static>,
}

impl FailedWithContinuation {
    /// 用原始错误和 continuation 构造延续执行错误。
    pub fn new(
        source: anyhow::Error,
        continuation: Arc<dyn Continuation + Send + Sync + 'static>,
    ) -> Self {
        let follow_up = continuation;

        Self {
            source,
            continuation: follow_up,
        }
    }

    /// 直接构造 `anyhow::Error` 形式的 continuation 错误。
    pub fn into_anyhow(
        source: anyhow::Error,
        continuation: Arc<dyn Continuation + Send + Sync + 'static>,
    ) -> anyhow::Error {
        let continuation_error = Self::new(source, continuation);
        anyhow::Error::new(continuation_error)
    }

    /// 从一个简单的异步函数创建 FailedWithContinuation（不支持取消）
    ///
    /// 这是一个便捷方法，用于从无需处理取消的简单异步闭包创建续作错误。
    /// 当续作被触发时会执行该函数。
    ///
    /// # 示例
    /// ```rust
    /// # use pagoda_runtime::utils::tasks::tracker::*;
    /// # use anyhow::anyhow;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let error = FailedWithContinuation::from_fn(
    ///     anyhow!("Initial task failed"),
    ///     || async {
    ///         println!("Retrying operation...");
    ///         Ok("retry_result".to_string())
    ///     }
    /// );
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_fn<F, Fut, T>(source: anyhow::Error, f: F) -> anyhow::Error
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<T, anyhow::Error>> + Send + 'static,
        T: Send + 'static,
    {
        let callback = Box::new(f);
        let continuation = Arc::new(FnContinuation { f: callback });
        Self::into_anyhow(source, continuation)
    }

    /// 从一个可取消的异步函数创建 FailedWithContinuation
    ///
    /// 这是一个便捷方法，用于从能处理取消的异步闭包创建续作错误。
    /// 该函数会收到一个 CancellationToken，应周期性地检查它以配合取消。
    ///
    /// # 示例
    /// ```rust
    /// # use pagoda_runtime::utils::tasks::tracker::*;
    /// # use anyhow::anyhow;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let error = FailedWithContinuation::from_cancellable(
    ///     anyhow!("Initial task failed"),
    ///     |cancel_token| async move {
    ///         if cancel_token.is_cancelled() {
    ///             return Err(anyhow!("Cancelled"));
    ///         }
    ///         println!("Retrying operation with cancellation support...");
    ///         Ok("retry_result".to_string())
    ///     }
    /// );
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_cancellable<F, Fut, T>(source: anyhow::Error, f: F) -> anyhow::Error
    where
        F: Fn(CancellationToken) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<T, anyhow::Error>> + Send + 'static,
        T: Send + 'static,
    {
        let callback = Box::new(f);
        let continuation = Arc::new(CancellableFnContinuation { f: callback });
        Self::into_anyhow(source, continuation)
    }
}

/// 从 anyhow::Error 中提取 FailedWithContinuation 的扩展 trait
///
/// 该 trait 提供从类型擦除的 anyhow::Error 体系中检测并提取续作任务的方法。
pub trait FailedWithContinuationExt {
    /// 若该错误携带续作则提取它
    ///
    /// 若错误是 FailedWithContinuation 则返回其续作任务，否则返回 None。
    fn extract_continuation(&self) -> Option<Arc<dyn Continuation + Send + Sync + 'static>>;

    /// 判断该错误是否携带续作
    fn has_continuation(&self) -> bool;
}

impl FailedWithContinuationExt for anyhow::Error {
    /// 从 `anyhow::Error` 中提取 continuation。
    fn extract_continuation(&self) -> Option<Arc<dyn Continuation + Send + Sync + 'static>> {
        self.downcast_ref::<FailedWithContinuation>()
            .map(|continuation_error| Arc::clone(&continuation_error.continuation))
    }

    /// 判断当前错误对象是否携带 continuation。
    fn has_continuation(&self) -> bool {
        self.extract_continuation().is_some()
    }
}

/// 针对简单异步函数的 Continuation 实现（不支持取消）
struct FnContinuation<F> {
    f: Box<F>,
}

impl<F> std::fmt::Debug for FnContinuation<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FnContinuation")
            .field("f", &"<closure>")
            .finish()
    }
}

#[async_trait]
impl<F, Fut, T> Continuation for FnContinuation<F>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<T, anyhow::Error>> + Send + 'static,
    T: Send + 'static,
{
    async fn execute(
        &self,
        _cancel_token: CancellationToken,
    ) -> TaskExecutionResult<Box<dyn std::any::Any + Send + 'static>> {
        let resolved = (self.f)().await;

        if let Ok(value) = resolved {
            return TaskExecutionResult::Success(Box::new(value));
        }

        TaskExecutionResult::Error(resolved.err().expect("error branch must exist"))
    }
}

/// 针对可取消异步函数的 Continuation 实现
struct CancellableFnContinuation<F> {
    f: Box<F>,
}

impl<F> std::fmt::Debug for CancellableFnContinuation<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CancellableFnContinuation")
            .field("f", &"<closure>")
            .finish()
    }
}

#[async_trait]
impl<F, Fut, T> Continuation for CancellableFnContinuation<F>
where
    F: Fn(CancellationToken) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<T, anyhow::Error>> + Send + 'static,
    T: Send + 'static,
{
    async fn execute(
        &self,
        cancel_token: CancellationToken,
    ) -> TaskExecutionResult<Box<dyn std::any::Any + Send + 'static>> {
        let resolved = (self.f)(cancel_token).await;

        if let Ok(value) = resolved {
            return TaskExecutionResult::Success(Box::new(value));
        }

        TaskExecutionResult::Error(resolved.err().expect("error branch must exist"))
    }
}

// === SECTION: SchedulingPolicy ===

/// \u4efb\u52a1\u6267\u884c\u7684\u5e38\u7528\u8c03\u5ea6\u7b56\u7565
///
/// \u8be5\u679a\u4e3e\u63d0\u4f9b\u5bf9\u5185\u7f6e\u8c03\u5ea6\u7b56\u7565\u7684\u4fbf\u6377\u8bbf\u95ee\uff0c\u65e0\u9700\u624b\u52a8\u6784\u9020\u7b56\u7565\u5bf9\u8c61\u3002
///
/// ## \u53d6\u6d88\u8bed\u4e49
///
/// \u6240\u6709\u8c03\u5ea6\u5668\u9075\u5faa\u76f8\u540c\u7684\u53d6\u6d88\u884c\u4e3a\uff1a
/// - \u5728\u5206\u914d\u8d44\u6e90\uff08\u8bb8\u53ef\u7b49\uff09\u4e4b\u524d\u5c0a\u91cd\u53d6\u6d88 token
/// - \u4e00\u65e6\u4efb\u52a1\u5f00\u59cb\u6267\u884c\uff0c\u59cb\u7ec8\u7b49\u5f85\u5176\u5b8c\u6210
/// - \u8ba9\u4efb\u52a1\u5728\u5185\u90e8\u81ea\u884c\u5904\u7406\u53d6\u6d88
#[derive(Debug, Clone)]
pub enum SchedulingPolicy {
    /// \u65e0\u5e76\u53d1\u9650\u5236 \u2014 \u7acb\u5373\u6267\u884c\u6240\u6709\u4efb\u52a1
    Unlimited,
    /// \u57fa\u4e8e\u4fe1\u53f7\u91cf\u7684\u5e76\u53d1\u9650\u5236
    Semaphore(usize),
}

/// 实现错误处理策略的 trait
///
/// 错误策略是轻量、同步的决策者，它分析任务失败并返回一个 ErrorResponse，
/// 告诉 TaskTracker 该采取何种动作。TaskTracker 根据策略的响应完成实际工作
/// （取消、指标统计等）。
///
/// ## 核心设计原则
/// - **同步**：策略不依赖异步操作即可快速决策
/// - **尽可能无状态**：TaskTracker 负责管理取消 token 与状态
/// - **可组合**：策略可以组合并在层级中嵌套
/// - **职责单一**：每个策略只处理一种特定的错误模式或策略
///
/// 单个任务的错误处理上下文
///
/// 为错误策略提供上下文信息与状态管理。
/// state 字段让策略在多次错误尝试间维护每任务状态。
pub struct OnErrorContext {
    /// 该任务已尝试的次数（从 1 开始）
    pub attempt_count: u32,
    /// 失败任务的唯一标识
    pub task_id: TaskId,
    /// 完整的执行上下文，可访问调度器、指标等
    pub execution_context: TaskExecutionContext,
    /// 由策略管理的可选每任务状态（无状态策略为 None）
    pub state: Option<Box<dyn std::any::Any + Send + 'static>>,
}

// === SECTION: OnErrorPolicy / ErrorPolicy ===

/// 任务失败的错误处理策略 trait
///
/// 策略定义 TaskTracker 如何响应任务失败。
/// 它们可以是无状态的（如 LogOnlyPolicy），也可以维护每任务状态
/// （如带每任务失败计数器的 ThresholdCancelPolicy）。
pub trait OnErrorPolicy: Send + Sync + std::fmt::Debug {
    /// 为子 tracker 创建子策略
    ///
    /// 该方法让策略保持层级关系，比如子取消 token 或共享的熔断器状态。
    fn create_child(&self) -> Arc<dyn OnErrorPolicy>;

    /// 创建每任务的上下文状态（策略无状态时返回 None）
    ///
    /// 当某任务首次出错时，每任务调用一次此方法。
    /// 无状态策略应返回 None，以避免不必要的堆分配。
    /// 有状态策略应返回 Some(Box::new(initial_state))。
    ///
    /// # 返回值
    /// * `None` — 策略不需要每任务状态（无堆分配）
    /// * `Some(state)` — 该任务的初始状态（按需堆分配）
    fn create_context(&self) -> Option<Box<dyn std::any::Any + Send + 'static>>;

    /// 处理任务失败并返回期望的响应
    ///
    /// # 参数
    /// * `error` — 发生的错误
    /// * `context` — 可变上下文，包含尝试次数、任务信息及可选状态
    ///
    /// # 返回值
    /// 表示 TaskTracker 应如何处理本次失败的 ErrorResponse
    fn on_error(&self, error: &anyhow::Error, context: &mut OnErrorContext) -> ErrorResponse;

    /// 是否允许针对本错误使用续作？
    ///
    /// 该方法在检查任务是否提供续作之前被调用，用于决定策略是否允许
    /// 基于续作的重试。若返回 `false`，任何 `FailedWithContinuation` 都会被忽略，
    /// 错误将按普通策略响应处理。
    ///
    /// # 参数
    /// * `error` — 发生的错误
    /// * `context` — 包含尝试次数与状态的每任务上下文
    ///
    /// # 返回值
    /// * `true` — 允许续作，检查 `FailedWithContinuation`（默认）
    /// * `false` — 拒绝续作，按普通策略响应处理
    fn allow_continuation(&self, _error: &anyhow::Error, _context: &OnErrorContext) -> bool {
        const ALLOW: bool = true;
        ALLOW
    }

    /// 该续作是否应重新经调度器调度？
    ///
    /// 当某续作即将执行时调用此方法，用于决定它是否需要再次走调度器的
    /// 资源获取流程，还是以当前执行许可立即执行。
    ///
    /// **含义：**
    /// - **不重新调度（`false`）**：以当前许可立即执行续作
    /// - **重新调度（`true`）**：释放当前许可，再次经调度器调度
    ///
    /// 重新调度意味着续作会再次受调度器策略约束（限流、并发限制、退避延迟等）。
    ///
    /// # 参数
    /// * `error` — 触发本次重试决策的错误
    /// * `context` — 包含尝试次数与状态的每任务上下文
    ///
    /// # 返回值
    /// * `false` — 立即执行续作（默认，高效）
    /// * `true` — 重新经调度器调度（用于延迟、限流、退避）
    fn should_reschedule(&self, _error: &anyhow::Error, _context: &OnErrorContext) -> bool {
        const RESCHEDULE: bool = false;
        RESCHEDULE
    }
}

/// 用于任务失败管理的常用错误处理策略
///
/// 该枚举提供对内置错误处理策略的便捷访问，无需手动构造策略对象。
#[derive(Debug, Clone)]
pub enum ErrorPolicy {
    /// 记录错误但继续执行 — 不取消
    LogOnly,
    /// 任何错误都取消所有任务（使用默认错误模式）
    CancelOnError,
    /// 遇到特定错误模式时取消所有任务
    CancelOnPatterns(Vec<String>),
    /// 失败次数超过阈值后取消
    CancelOnThreshold { max_failures: usize },
    /// 时间窗口内失败率超过阈值时取消
    CancelOnRate {
        max_failure_rate: f32,
        window_secs: u64,
    },
}

/// 错误处理策略的响应类型
///
/// 该枚举定义 TaskTracker 应如何响应任务失败。
#[derive(Debug)]
pub enum ErrorResponse {
    /// 仅让本任务失败 — 错误会被记录/计数，但 tracker 继续运行
    Fail,

    /// 关停本 tracker 及所有子 tracker
    Shutdown,

    /// 执行自定义错误处理逻辑，可完整访问上下文
    Custom(Box<dyn OnErrorAction>),
}

/// 用于实现自定义错误处理动作的 trait
///
/// 它提供对任务执行上下文的完整访问，用于那些不适合内置响应模式的复杂错误处理场景。
#[async_trait]
pub trait OnErrorAction: Send + Sync + std::fmt::Debug {
    /// 执行自定义错误处理逻辑
    ///
    /// # 参数
    /// * `error` — 导致任务失败的错误
    /// * `task_id` — 失败任务的唯一标识
    /// * `attempt_count` — 该任务已尝试的次数（从 1 开始）
    /// * `context` — 完整执行上下文，可访问调度器、指标等
    ///
    /// # 返回值
    /// 表示 TaskTracker 接下来该做什么的 ActionResult
    async fn execute(
        &self,
        error: &anyhow::Error,
        task_id: TaskId,
        attempt_count: u32,
        context: &TaskExecutionContext,
    ) -> ActionResult;
}

/// 任务重试循环中用于条件性重新获取资源的调度器执行护卫状态
///
/// 它控制续作是复用当前调度器执行许可，还是再次经历调度器的获取流程。
#[derive(Debug, Clone, PartialEq, Eq)]
enum GuardState {
    /// 保留当前调度器执行许可，以便立即续作
    ///
    /// 续作将立即执行，不再走调度器。
    /// 这对于无需延迟或限流的简单重试更高效。
    Keep,

    /// 释放当前许可，在续作前重新经调度器获取
    ///
    /// 续作将再次受调度器策略约束（并发限制、限流、退避延迟等）。
    /// 用于实现重试延迟，或需要调度器对重试尝试应用其策略的场景。
    Reschedule,
}

/// 自定义错误动作执行的结果
#[derive(Debug)]
pub enum ActionResult {
    /// 仅让本任务失败（错误已由策略记录/处理）
    ///
    /// 表示策略已适当处理该错误（如记录、更新指标等），
    /// 任务应以此错误失败。任务执行在此终止。
    Fail,

    /// 使用提供的任务继续执行
    ///
    /// 提供一个新的可执行体以继续重试循环。
    /// 任务执行将以提供的续作继续。
    Continue {
        continuation: Arc<dyn Continuation + Send + Sync + 'static>,
    },

    /// 关停本 tracker 及所有子 tracker
    ///
    /// 这会触发整个 tracker 层级的关停。
    /// 所有运行中和待处理的任务都将被取消。
    Shutdown,
}

/// 提供给自定义错误动作的执行上下文
///
/// 它让自定义动作完整访问任务执行环境，以实现复杂的错误处理场景。
pub struct TaskExecutionContext {
    /// 用于重新获取资源或检查状态的调度器
    pub scheduler: Arc<dyn TaskScheduler>,

    /// 用于自定义统计的指标
    pub metrics: Arc<dyn HierarchicalTaskMetrics>,
}

/// 任务执行结果 — 普通任务与可取消任务统一使用
#[derive(Debug)]
pub enum TaskExecutionResult<T> {
    /// 任务成功完成
    Success(T),
    /// 任务被取消（仅可取消任务可能出现）
    Cancelled,
    /// 任务以错误结束
    Error(anyhow::Error),
}

/// 以统一方式执行不同类型任务的 trait
#[async_trait]
trait TaskExecutor<T>: Send {
    /// 使用给定的取消 token 执行任务
    async fn execute(&mut self, cancel_token: CancellationToken) -> TaskExecutionResult<T>;
}

/// 针对普通（不可取消）任务的执行器
struct RegularTaskExecutor<F, T>
where
    F: Future<Output = Result<T>> + Send + 'static,
    T: Send + 'static,
{
    future: Option<F>,
    _phantom: std::marker::PhantomData<T>,
}

impl<F, T> RegularTaskExecutor<F, T>
where
    F: Future<Output = Result<T>> + Send + 'static,
    T: Send + 'static,
{
    /// 封装普通异步任务，以便接入统一执行循环。
    fn new(future: F) -> Self {
        let pending = Some(future);
        Self {
            future: pending,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<F, T> TaskExecutor<T> for RegularTaskExecutor<F, T>
where
    F: Future<Output = Result<T>> + Send + 'static,
    T: Send + 'static,
{
    /// 执行普通任务，并把返回值映射为统一执行结果。
    async fn execute(&mut self, _cancel_token: CancellationToken) -> TaskExecutionResult<T> {
        let Some(future) = self.future.take() else {
            return TaskExecutionResult::Error(anyhow::anyhow!("Regular task already consumed"));
        };

        let output = future.await;
        match output {
            Ok(value) => TaskExecutionResult::Success(value),
            Err(error) => TaskExecutionResult::Error(error),
        }
    }
}

/// 针对可取消任务的执行器
struct CancellableTaskExecutor<F, Fut, T>
where
    F: FnMut(CancellationToken) -> Fut + Send + 'static,
    Fut: Future<Output = CancellableTaskResult<T>> + Send + 'static,
    T: Send + 'static,
{
    task_fn: F,
}

impl<F, Fut, T> CancellableTaskExecutor<F, Fut, T>
where
    F: FnMut(CancellationToken) -> Fut + Send + 'static,
    Fut: Future<Output = CancellableTaskResult<T>> + Send + 'static,
    T: Send + 'static,
{
    /// 封装可取消任务回调，以便接入统一执行循环。
    fn new(task_fn: F) -> Self {
        let callback = task_fn;
        Self { task_fn: callback }
    }
}

#[async_trait]
impl<F, Fut, T> TaskExecutor<T> for CancellableTaskExecutor<F, Fut, T>
where
    F: FnMut(CancellationToken) -> Fut + Send + 'static,
    Fut: Future<Output = CancellableTaskResult<T>> + Send + 'static,
    T: Send + 'static,
{
    /// 执行可取消任务，并把三态结果映射为统一执行结果。
    async fn execute(&mut self, cancel_token: CancellationToken) -> TaskExecutionResult<T> {
        let execution = (self.task_fn)(cancel_token);
        match execution.await {
            CancellableTaskResult::Ok(value) => TaskExecutionResult::Success(value),
            CancellableTaskResult::Cancelled => TaskExecutionResult::Cancelled,
            CancellableTaskResult::Err(error) => TaskExecutionResult::Error(error),
        }
    }
}

/// 策略 Arc 构造的通用能力
///
/// 该 trait 为所有策略类型提供统一的 `new_arc()` 方法，
/// 免去在调用代码中手动 `Arc::new()`。
pub trait ArcPolicy: Sized + Send + Sync + 'static {
    /// 创建由 Arc 包装的策略实例
    fn new_arc(self) -> Arc<Self> {
        Arc::new(self)
    }
}

/// 任务的唯一标识
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskId(Uuid);

impl TaskId {
    fn new() -> Self {
        let uuid = Uuid::new_v4();
        Self(uuid)
    }
}

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "task-{}", self.0)
    }
}

/// 任务执行的结果状态
#[derive(Debug, Clone, PartialEq)]
pub enum CompletionStatus {
    /// 任务成功完成
    Ok,
    /// 任务在执行前或执行中被取消
    Cancelled,
    /// 任务以错误结束
    Failed(String),
}

/// 显式跟踪取消状态的可取消任务结果类型
#[derive(Debug)]
pub enum CancellableTaskResult<T> {
    /// 任务成功完成
    Ok(T),
    /// 任务被取消（来自 token 或关停）
    Cancelled,
    /// 任务以错误结束
    Err(anyhow::Error),
}

/// 调度任务的结果
#[derive(Debug)]
pub enum SchedulingResult<T> {
    /// 任务已执行并完成
    Execute(T),
    /// 任务在执行前被取消
    Cancelled,
    /// 任务因调度策略被拒绝
    Rejected(String),
}

/// 管理任务执行的资源护卫
///
/// 该 trait 通过将资源管理与任务执行分离来强制正确的取消语义。
/// 一旦获取护卫，任务执行就必须运行至完成。
/// 资源护卫代表从调度器获取、并需在任务执行期间持有的资源
/// （许可、槽位等）。护卫在被 drop 时自动释放资源，实现正确的 RAII 语义。
///
/// 护卫由 `TaskScheduler::acquire_execution_slot()` 返回，应在任务执行期间保持在作用域内，
/// 以确保资源保持分配。
pub trait ResourceGuard: Send + 'static {
    // 标记 trait — 资源通过具体类型的 Drop 释放
}

/// 用于实现任务调度策略的 trait
///
/// 该 trait 通过将资源获取（可取消）与任务执行（不可取消）拆分，
/// 来强制正确的取消语义。
///
/// ## 设计理念
///
/// 任务可能支持也可能不支持取消（取决于它是由 `spawn_cancellable` 还是
/// 普通 `spawn` 创建）。这种拆分设计确保：
///
/// - **资源获取**：可以尊重取消 token，避免不必要的分配
/// - **任务执行**：始终运行至完成；任务自行处理取消
///
/// 这使得无法意外地用 `tokio::select!` 中断任务执行。
#[async_trait]
pub trait TaskScheduler: Send + Sync + std::fmt::Debug {
    /// 获取任务执行所需的资源并返回一个护卫
    ///
    /// 该方法处理资源分配（许可、队列槽位等），并可尊重取消 token 以避免不必要的资源消耗。
    ///
    /// ## 取消行为
    ///
    /// `cancel_token` 用于调度器级别的取消（如「不要启动新工作」）。
    /// 若在资源获取前或过程中请求了取消，该方法应返回 `SchedulingResult::Cancelled`。
    ///
    /// # 参数
    /// * `cancel_token` — 用于调度器级别取消的 [`CancellationToken`]
    ///
    /// # 返回值
    /// * `SchedulingResult::Execute(guard)` — 资源已获取，可以执行
    /// * `SchedulingResult::Cancelled` — 在资源获取前或过程中被取消
    /// * `SchedulingResult::Rejected(reason)` — 资源不可用或违反策略
    async fn acquire_execution_slot(
        &self,
        cancel_token: CancellationToken,
    ) -> SchedulingResult<Box<dyn ResourceGuard>>;
}

// === SECTION: HierarchicalTaskMetrics / PrometheusTaskMetrics ===

/// 支持沿 tracker 树向上聚合的层级化任务指标 trait
///
/// 该 trait 为根 tracker 与子 tracker 提供不同实现：
/// - 根 tracker 与 Prometheus 指标集成以便可观测
/// - 子 tracker 将指标更新向上传递给父级以聚合
/// - 所有实现均保持线程安全的原子操作
pub trait HierarchicalTaskMetrics: Send + Sync + std::fmt::Debug {
    /// 递增已发布任务计数
    fn increment_issued(&self);

    /// 递增已启动任务计数
    fn increment_started(&self);

    /// 递增成功计数
    fn increment_success(&self);

    /// 递增取消计数
    fn increment_cancelled(&self);

    /// 递增失败计数
    fn increment_failed(&self);

    /// 递增拒绝计数
    fn increment_rejected(&self);

    /// 获取当前已发布计数（仅本 tracker）
    fn issued(&self) -> u64;

    /// 获取当前已启动计数（仅本 tracker）
    fn started(&self) -> u64;

    /// 获取当前成功计数（仅本 tracker）
    fn success(&self) -> u64;

    /// 获取当前取消计数（仅本 tracker）
    fn cancelled(&self) -> u64;

    /// 获取当前失败计数（仅本 tracker）
    fn failed(&self) -> u64;

    /// 获取当前拒绝计数（仅本 tracker）
    fn rejected(&self) -> u64;

    /// 获取已完成任务总数（成功 + 取消 + 失败 + 拒绝）
    fn total_completed(&self) -> u64 {
        self.success() + self.cancelled() + self.failed() + self.rejected()
    }

    /// 获取待处理任务数（已发布 - 已完成）
    fn pending(&self) -> u64 {
        self.issued().saturating_sub(self.total_completed())
    }

    /// 获取当前处于活跃状态的任务数（已开始 - 已完成）
    fn active(&self) -> u64 {
        self.started().saturating_sub(self.total_completed())
    }

    /// 获取在调度器中排队的任务数（已发布 - 已开始）
    fn queued(&self) -> u64 {
        self.issued().saturating_sub(self.started())
    }
}

/// 某个 tracker 的任务执行指标
#[derive(Debug, Default)]
pub struct TaskMetrics {
    /// 已发布/提交的任务数（通过 spawn 系列方法）
    pub issued_count: AtomicU64,
    /// 已开始执行的任务数
    pub started_count: AtomicU64,
    /// 成功完成的任务数
    pub success_count: AtomicU64,
    /// 被取消的任务数
    pub cancelled_count: AtomicU64,
    /// 失败的任务数
    pub failed_count: AtomicU64,
    /// 被（调度器）拒绝的任务数
    pub rejected_count: AtomicU64,
}

fn increment_counter(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

fn read_counter(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}

impl TaskMetrics {
    /// 创建新的指标实例
    pub fn new() -> Self {
        Default::default()
    }
}

impl HierarchicalTaskMetrics for TaskMetrics {
    /// 递增已发布任务计数
    fn increment_issued(&self) {
        increment_counter(&self.issued_count);
    }

    /// 递增已启动任务计数
    fn increment_started(&self) {
        increment_counter(&self.started_count);
    }

    /// 递增成功计数
    fn increment_success(&self) {
        increment_counter(&self.success_count);
    }

    /// 递增取消计数
    fn increment_cancelled(&self) {
        increment_counter(&self.cancelled_count);
    }

    /// 递增失败计数
    fn increment_failed(&self) {
        increment_counter(&self.failed_count);
    }

    /// 递增拒绝计数
    fn increment_rejected(&self) {
        increment_counter(&self.rejected_count);
    }

    /// 获取当前已发布计数
    fn issued(&self) -> u64 {
        read_counter(&self.issued_count)
    }

    /// 获取当前已启动计数
    fn started(&self) -> u64 {
        read_counter(&self.started_count)
    }

    /// 获取当前成功计数
    fn success(&self) -> u64 {
        read_counter(&self.success_count)
    }

    /// 获取当前取消计数
    fn cancelled(&self) -> u64 {
        read_counter(&self.cancelled_count)
    }

    /// 获取当前失败计数
    fn failed(&self) -> u64 {
        read_counter(&self.failed_count)
    }

    /// 获取当前拒绝计数
    fn rejected(&self) -> u64 {
        read_counter(&self.rejected_count)
    }
}

/// 带 Prometheus 集成的根 tracker 指标
///
/// 该实现维护本地计数器，并通过提供的 MetricsRegistry 将其暴露为 Prometheus 指标。
#[derive(Debug)]
pub struct PrometheusTaskMetrics {
    /// Prometheus 指标集成
    prometheus_issued: prometheus::IntCounter,
    prometheus_started: prometheus::IntCounter,
    prometheus_success: prometheus::IntCounter,
    prometheus_cancelled: prometheus::IntCounter,
    prometheus_failed: prometheus::IntCounter,
    prometheus_rejected: prometheus::IntCounter,
}

impl PrometheusTaskMetrics {
    /// 创建带 Prometheus 集成的新根指标
    ///
    /// # 参数
    /// * `registry` — 用于创建 Prometheus 指标的 MetricsRegistry
    /// * `servicegroup_name` — servicegroup/tracker 的名称（用于指标名）
    ///
    /// # 示例
    /// ```rust
    /// # use std::sync::Arc;
    /// # use pagoda_runtime::utils::tasks::tracker::PrometheusTaskMetrics;
    /// # use pagoda_runtime::DistributedRuntime;
    /// # fn example(drt: &DistributedRuntime) -> anyhow::Result<()> {
    /// let metrics = PrometheusTaskMetrics::new(drt, "main_tracker")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new<R: MetricsHierarchy>(registry: &R, servicegroup_name: &str) -> anyhow::Result<Self> {
        let metrics = registry.metrics();
        let issued_counter = metrics.create_intcounter(
            &format!("{}_{}", servicegroup_name, task_tracker::TASKS_ISSUED_TOTAL),
            "Total number of tasks issued/submitted",
            &[],
        )?;

        let started_counter = metrics.create_intcounter(
            &format!("{}_{}", servicegroup_name, task_tracker::TASKS_STARTED_TOTAL),
            "Total number of tasks started",
            &[],
        )?;

        let success_counter = metrics.create_intcounter(
            &format!("{}_{}", servicegroup_name, task_tracker::TASKS_SUCCESS_TOTAL),
            "Total number of successfully completed tasks",
            &[],
        )?;

        let cancelled_counter = metrics.create_intcounter(
            &format!("{}_{}", servicegroup_name, task_tracker::TASKS_CANCELLED_TOTAL),
            "Total number of cancelled tasks",
            &[],
        )?;

        let failed_counter = metrics.create_intcounter(
            &format!("{}_{}", servicegroup_name, task_tracker::TASKS_FAILED_TOTAL),
            "Total number of failed tasks",
            &[],
        )?;

        let rejected_counter = metrics.create_intcounter(
            &format!("{}_{}", servicegroup_name, task_tracker::TASKS_REJECTED_TOTAL),
            "Total number of rejected tasks",
            &[],
        )?;

        Ok(Self {
            prometheus_issued: issued_counter,
            prometheus_started: started_counter,
            prometheus_success: success_counter,
            prometheus_cancelled: cancelled_counter,
            prometheus_failed: failed_counter,
            prometheus_rejected: rejected_counter,
        })
    }
}

impl HierarchicalTaskMetrics for PrometheusTaskMetrics {
    fn increment_issued(&self) {
        self.prometheus_issued.inc();
    }

    fn increment_started(&self) {
        self.prometheus_started.inc();
    }

    fn increment_success(&self) {
        self.prometheus_success.inc();
    }

    fn increment_cancelled(&self) {
        self.prometheus_cancelled.inc();
    }

    fn increment_failed(&self) {
        self.prometheus_failed.inc();
    }

    fn increment_rejected(&self) {
        self.prometheus_rejected.inc();
    }

    fn issued(&self) -> u64 {
        self.prometheus_issued.get()
    }

    fn started(&self) -> u64 {
        self.prometheus_started.get()
    }

    fn success(&self) -> u64 {
        self.prometheus_success.get()
    }

    fn cancelled(&self) -> u64 {
        self.prometheus_cancelled.get()
    }

    fn failed(&self) -> u64 {
        self.prometheus_failed.get()
    }

    fn rejected(&self) -> u64 {
        self.prometheus_rejected.get()
    }
}

/// 将更新向上传递给父级的子 tracker 指标
///
/// 该实现维护本地计数器，并自动将所有指标更新转发给父 tracker 以进行层级聚合。
/// 持有对父指标的强引用以获得最佳性能。
#[derive(Debug)]
struct ChildTaskMetrics {
    /// 本 tracker 的本地指标
    local_metrics: TaskMetrics,
    /// 对父指标的强引用，用于快速传递
    /// 因为指标不拥有 tracker，不会形成循环引用，持有是安全的
    parent_metrics: Arc<dyn HierarchicalTaskMetrics>,
}

impl ChildTaskMetrics {
    fn new(parent_metrics: Arc<dyn HierarchicalTaskMetrics>) -> Self {
        let metrics = TaskMetrics::new();
        Self {
            local_metrics: metrics,
            parent_metrics,
        }
    }
}

impl HierarchicalTaskMetrics for ChildTaskMetrics {
    fn increment_issued(&self) {
        self.local_metrics.increment_issued();
        self.parent_metrics.increment_issued();
    }

    fn increment_started(&self) {
        self.local_metrics.increment_started();
        self.parent_metrics.increment_started();
    }

    fn increment_success(&self) {
        self.local_metrics.increment_success();
        self.parent_metrics.increment_success();
    }

    fn increment_cancelled(&self) {
        self.local_metrics.increment_cancelled();
        self.parent_metrics.increment_cancelled();
    }

    fn increment_failed(&self) {
        self.local_metrics.increment_failed();
        self.parent_metrics.increment_failed();
    }

    fn increment_rejected(&self) {
        self.local_metrics.increment_rejected();
        self.parent_metrics.increment_rejected();
    }

    fn issued(&self) -> u64 {
        self.local_metrics.issued()
    }

    fn started(&self) -> u64 {
        self.local_metrics.started()
    }

    fn success(&self) -> u64 {
        self.local_metrics.success()
    }

    fn cancelled(&self) -> u64 {
        self.local_metrics.cancelled()
    }

    fn failed(&self) -> u64 {
        self.local_metrics.failed()
    }

    fn rejected(&self) -> u64 {
        self.local_metrics.rejected()
    }
}

/// 用于创建带自定义策略的子 tracker 的构造器
///
/// 允许灵活定制子 tracker 的调度与错误处理策略，同时保持父子关系。
pub struct ChildTrackerBuilder<'parent> {
    parent: &'parent TaskTracker,
    scheduler: Option<Arc<dyn TaskScheduler>>,
    error_policy: Option<Arc<dyn OnErrorPolicy>>,
}

impl<'parent> ChildTrackerBuilder<'parent> {
    /// 创建一个新的 ChildTrackerBuilder
    pub fn new(parent: &'parent TaskTracker) -> Self {
        Self {
            parent,
            scheduler: None,
            error_policy: None,
        }
    }

    /// 为子 tracker 设置自定义调度器
    ///
    /// 若未设置，子 tracker 将继承父级的调度器。
    ///
    /// # 参数
    /// * `scheduler` — 该子 tracker 使用的调度器
    ///
    /// # 示例
    /// ```rust
    /// # use std::sync::Arc;
    /// # use tokio::sync::Semaphore;
    /// # use pagoda_runtime::utils::tasks::tracker::{TaskTracker, SemaphoreScheduler};
    /// # fn example(parent: &TaskTracker) {
    /// let child = parent.child_tracker_builder()
    ///     .scheduler(SemaphoreScheduler::with_permits(5))
    ///     .build().unwrap();
    /// # }
    /// ```
    pub fn scheduler(mut self, scheduler: Arc<dyn TaskScheduler>) -> Self {
        self.scheduler = Some(scheduler);
        self
    }

    /// 为子 tracker 设置自定义错误策略
    ///
    /// 若未设置，子 tracker 将从父级错误策略获得子策略
    /// （通过 `OnErrorPolicy::create_child()`）。
    ///
    /// # 参数
    /// * `error_policy` — 该子 tracker 使用的错误策略
    ///
    /// # 示例
    /// ```rust
    /// # use std::sync::Arc;
    /// # use pagoda_runtime::utils::tasks::tracker::{TaskTracker, LogOnlyPolicy};
    /// # fn example(parent: &TaskTracker) {
    /// let child = parent.child_tracker_builder()
    ///     .error_policy(LogOnlyPolicy::new())
    ///     .build().unwrap();
    /// # }
    /// ```
    pub fn error_policy(mut self, error_policy: Arc<dyn OnErrorPolicy>) -> Self {
        self.error_policy = Some(error_policy);
        self
    }

    /// 按指定配置构建子 tracker
    ///
    /// 创建一个新的子 tracker，具备：
    /// - 自定义或继承的调度器
    /// - 自定义或子错误策略
    /// - 向父级传递的层级化指标
    /// - 来自父级的子取消 token
    /// - 与父级独立的生命周期
    ///
    /// # 返回值
    /// 一个新的 `Arc<TaskTracker>`，配置为父级的子节点
    ///
    /// # 错误
    /// 若父 tracker 已关闭则返回错误
    pub fn build(self) -> anyhow::Result<TaskTracker> {
        if self.parent.is_closed() {
            return Err(anyhow::anyhow!(
                "Cannot create child tracker from closed parent tracker"
            ));
        }

        let parent_inner = Arc::clone(&self.parent.0);
        let scheduler = match self.scheduler {
            Some(custom_scheduler) => custom_scheduler,
            None => Arc::clone(&parent_inner.scheduler),
        };
        let error_policy = match self.error_policy {
            Some(custom_policy) => custom_policy,
            None => parent_inner.error_policy.create_child(),
        };
        let child_metrics = Arc::new(ChildTaskMetrics::new(Arc::clone(&parent_inner.metrics)));
        let child_cancel_token = parent_inner.cancel_token.child_token();

        let child = Arc::new(TaskTrackerInner {
            tokio_tracker: TokioTaskTracker::new(),
            parent: None, // 层级化操作无需父引用
            scheduler,
            error_policy,
            metrics: child_metrics,
            cancel_token: child_cancel_token,
            children: RwLock::new(Vec::new()),
        });

        // 将该子节点注册到父级，以便进行层级化操作
        parent_inner
            .children
            .write()
            .unwrap()
            .push(Arc::downgrade(&child));

        parent_inner.cleanup_dead_children();

        Ok(TaskTracker(child))
    }
}

/// TaskTracker 的内部数据
///
/// 该结构体包含 TaskTracker 的全部实际状态与功能。
/// TaskTracker 本身只是 Arc<TaskTrackerInner> 的包装。
struct TaskTrackerInner {
    /// 用于生命周期管理的 tokio 任务跟踪器
    tokio_tracker: TokioTaskTracker,
    /// 父 tracker（根节点为 None）
    parent: Option<Arc<TaskTrackerInner>>,
    /// 调度策略（默认与子节点共享）
    scheduler: Arc<dyn TaskScheduler>,
    /// 错误处理策略（通过 create_child 生成子节点专属策略）
    error_policy: Arc<dyn OnErrorPolicy>,
    /// 本 tracker 的指标
    metrics: Arc<dyn HierarchicalTaskMetrics>,
    /// 本 tracker 的取消 token（总是存在）
    cancel_token: CancellationToken,
    /// 用于层级化操作的子 tracker 列表
    children: RwLock<Vec<Weak<TaskTrackerInner>>>,
}

// === SECTION: TaskTracker ===

/// 带可插拔调度与错误策略的层级化任务跟踪器
///
/// TaskTracker 提供一套可组合的后台任务管理系统，具备：
/// - 通过 [`TaskScheduler`] 实现可配置调度
/// - 通过 [`OnErrorPolicy`] 实现灵活错误处理
/// - 带独立指标的父子关系
/// - 取消的传播与隔离
/// - 内置取消 token 支持
///
/// 基于 `tokio_util::task::TaskTracker` 构建，以获得稳健的任务生命周期管理。
///
/// # 示例
///
/// ```rust
/// # use std::sync::Arc;
/// # use tokio::sync::Semaphore;
/// # use pagoda_runtime::utils::tasks::tracker::{TaskTracker, SemaphoreScheduler, LogOnlyPolicy, CancellableTaskResult};
/// # async fn example() -> anyhow::Result<()> {
/// // 创建一个基于信号量调度的任务跟踪器
/// let scheduler = SemaphoreScheduler::with_permits(3);
/// let policy = LogOnlyPolicy::new();
/// let root = TaskTracker::builder()
///     .scheduler(scheduler)
///     .error_policy(policy)
///     .build()?;
///
/// // 派发一些任务
/// let handle1 = root.spawn(async { Ok(1) });
/// let handle2 = root.spawn(async { Ok(2) });
///
/// // 获取结果并 join 所有任务
/// let result1 = handle1.await.unwrap().unwrap();
/// let result2 = handle2.await.unwrap().unwrap();
/// assert_eq!(result1, 1);
/// assert_eq!(result2, 2);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct TaskTracker(Arc<TaskTrackerInner>);

/// TaskTracker 的构造器
#[derive(Default)]
pub struct TaskTrackerBuilder {
    scheduler: Option<Arc<dyn TaskScheduler>>,
    error_policy: Option<Arc<dyn OnErrorPolicy>>,
    metrics: Option<Arc<dyn HierarchicalTaskMetrics>>,
    cancel_token: Option<CancellationToken>,
}

impl TaskTrackerBuilder {
    /// 为该 TaskTracker 设置调度器
    pub fn scheduler(mut self, scheduler: Arc<dyn TaskScheduler>) -> Self {
        self.scheduler = Some(scheduler);
        self
    }

    /// 为该 TaskTracker 设置错误策略
    pub fn error_policy(mut self, error_policy: Arc<dyn OnErrorPolicy>) -> Self {
        self.error_policy = Some(error_policy);
        self
    }

    /// 为该 TaskTracker 设置自定义指标
    pub fn metrics(mut self, metrics: Arc<dyn HierarchicalTaskMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// 为该 TaskTracker 设置取消 token
    pub fn cancel_token(mut self, cancel_token: CancellationToken) -> Self {
        self.cancel_token = Some(cancel_token);
        self
    }

    /// 构建 TaskTracker
    pub fn build(self) -> anyhow::Result<TaskTracker> {
        let scheduler = match self.scheduler {
            Some(scheduler) => scheduler,
            None => return Err(anyhow::anyhow!("TaskTracker requires a scheduler")),
        };

        let error_policy = match self.error_policy {
            Some(policy) => policy,
            None => return Err(anyhow::anyhow!("TaskTracker requires an error policy")),
        };

        let metrics = self.metrics.unwrap_or_else(|| Arc::new(TaskMetrics::new()));
        let cancel_token = self.cancel_token.unwrap_or_else(CancellationToken::new);

        let inner = TaskTrackerInner {
            tokio_tracker: TokioTaskTracker::new(),
            parent: None,
            scheduler,
            error_policy,
            metrics,
            cancel_token,
            children: RwLock::new(Vec::new()),
        };

        Ok(TaskTracker(Arc::new(inner)))
    }
}

impl TaskTracker {
    /// 使用构造器模式创建一个新的根任务跟踪器
    ///
    /// 这是创建新任务跟踪器的首选方式。
    ///
    /// # 示例
    /// ```rust
    /// # use std::sync::Arc;
    /// # use tokio::sync::Semaphore;
    /// # use pagoda_runtime::utils::tasks::tracker::{TaskTracker, SemaphoreScheduler, LogOnlyPolicy};
    /// # fn main() -> anyhow::Result<()> {
    /// let scheduler = SemaphoreScheduler::with_permits(10);
    /// let error_policy = LogOnlyPolicy::new();
    /// let tracker = TaskTracker::builder()
    ///     .scheduler(scheduler)
    ///     .error_policy(error_policy)
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn builder() -> TaskTrackerBuilder {
        TaskTrackerBuilder::default()
    }

    /// 使用简单参数创建新的根任务跟踪器（旧接口）
    ///
    /// 保留该方法以兼容旧代码。新代码请使用 `builder()`。
    /// 使用默认指标（无 Prometheus 集成）。
    ///
    /// # 参数
    /// * `scheduler` — 用于所有任务的调度策略
    /// * `error_policy` — 该 tracker 的错误处理策略
    ///
    /// # 示例
    /// ```rust
    /// # use std::sync::Arc;
    /// # use tokio::sync::Semaphore;
    /// # use pagoda_runtime::utils::tasks::tracker::{TaskTracker, SemaphoreScheduler, LogOnlyPolicy};
    /// # fn main() -> anyhow::Result<()> {
    /// let scheduler = SemaphoreScheduler::with_permits(10);
    /// let error_policy = LogOnlyPolicy::new();
    /// let tracker = TaskTracker::new(scheduler, error_policy)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(
        scheduler: Arc<dyn TaskScheduler>,
        error_policy: Arc<dyn OnErrorPolicy>,
    ) -> anyhow::Result<Self> {
        let builder = Self::builder();
        builder.scheduler(scheduler).error_policy(error_policy).build()
    }

    /// 创建带 Prometheus 指标集成的新根任务跟踪器
    ///
    /// # 参数
    /// * `scheduler` — 用于所有任务的调度策略
    /// * `error_policy` — 该 tracker 的错误处理策略
    /// * `registry` — 用于 Prometheus 集成的 MetricsRegistry
    /// * `servicegroup_name` — 该 tracker servicegroup 的名称
    ///
    /// # 示例
    /// ```rust
    /// # use std::sync::Arc;
    /// # use tokio::sync::Semaphore;
    /// # use pagoda_runtime::utils::tasks::tracker::{TaskTracker, SemaphoreScheduler, LogOnlyPolicy};
    /// # use pagoda_runtime::DistributedRuntime;
    /// # fn example(drt: &DistributedRuntime) -> anyhow::Result<()> {
    /// let scheduler = SemaphoreScheduler::with_permits(10);
    /// let error_policy = LogOnlyPolicy::new();
    /// let tracker = TaskTracker::new_with_prometheus(
    ///     scheduler,
    ///     error_policy,
    ///     drt,
    ///     "main_tracker"
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new_with_prometheus<R: MetricsHierarchy>(
        scheduler: Arc<dyn TaskScheduler>,
        error_policy: Arc<dyn OnErrorPolicy>,
        registry: &R,
        servicegroup_name: &str,
    ) -> anyhow::Result<Self> {
        let metrics = PrometheusTaskMetrics::new(registry, servicegroup_name)?;
        let builder = Self::builder()
            .scheduler(scheduler)
            .error_policy(error_policy)
            .metrics(Arc::new(metrics));

        builder.build()
    }

    /// 创建一个继承调度策略的子 tracker
    ///
    /// 该子 tracker：
    /// - 拥有自己独立的 tokio TaskTracker
    /// - 继承父级的调度器
    /// - 通过 `create_child()` 获得子错误策略
    /// - 具备向父级传递的层级化指标
    /// - 从父级获得子取消 token
    /// - 取消互相独立（子节点取消不影响父级）
    ///
    /// # 错误
    /// 若父 tracker 已关闭则返回错误
    ///
    /// # 示例
    /// ```rust
    /// # use std::sync::Arc;
    /// # use pagoda_runtime::utils::tasks::tracker::TaskTracker;
    /// # fn example(root_tracker: TaskTracker) -> anyhow::Result<()> {
    /// let child_tracker = root_tracker.child_tracker()?;
    /// // 子节点继承父级策略，但拥有独立的指标与生命周期
    /// # Ok(())
    /// # }
    /// ```
    pub fn child_tracker(&self) -> anyhow::Result<TaskTracker> {
        let child = self.0.child_tracker()?;
        Ok(TaskTracker(child))
    }

    /// 创建子 tracker 构造器以进行灵活定制
    ///
    /// 该构造器允许你定制子 tracker 的调度与错误策略。
    /// 若未指定，策略从父级继承。
    ///
    /// # 示例
    /// ```rust
    /// # use std::sync::Arc;
    /// # use tokio::sync::Semaphore;
    /// # use pagoda_runtime::utils::tasks::tracker::{TaskTracker, SemaphoreScheduler, LogOnlyPolicy};
    /// # fn example(root_tracker: TaskTracker) {
    /// // 自定义调度器，继承错误策略
    /// let child1 = root_tracker.child_tracker_builder()
    ///     .scheduler(SemaphoreScheduler::with_permits(5))
    ///     .build().unwrap();
    ///
    /// // 自定义错误策略，继承调度器
    /// let child2 = root_tracker.child_tracker_builder()
    ///     .error_policy(LogOnlyPolicy::new())
    ///     .build().unwrap();
    ///
    /// // 两者都自定义
    /// let child3 = root_tracker.child_tracker_builder()
    ///     .scheduler(SemaphoreScheduler::with_permits(3))
    ///     .error_policy(LogOnlyPolicy::new())
    ///     .build().unwrap();
    /// # }
    /// ```
    /// 派发一个新任务
    ///
    /// 任务会被包装上调度与错误处理逻辑，然后按配置的策略执行。
    /// 对于需要检查取消 token 的任务，请改用 [`spawn_cancellable`]。
    ///
    /// # 参数
    /// * `future` — 要执行的异步任务
    ///
    /// # 返回值
    /// 一个 [`TaskHandle`]，可用于等待完成并访问任务的取消 token
    ///
    /// # Panics
    /// 若 tracker 已关闭则 panic。这表明在 tracker 生命周期结束后还在派发任务，属于编程错误。
    ///
    /// # 示例
    /// ```rust
    /// # use pagoda_runtime::utils::tasks::tracker::TaskTracker;
    /// # async fn example(tracker: TaskTracker) -> anyhow::Result<()> {
    /// let handle = tracker.spawn(async {
    ///     // 在此编写你的异步工作
    ///     tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    ///     Ok(42)
    /// });
    ///
    /// // 获取任务的取消 token
    /// let cancel_token = handle.cancellation_token();
    ///
    /// let result = handle.await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn spawn<F, T>(&self, future: F) -> TaskHandle<T>
    where
        F: Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        match self.0.spawn(future) {
            Ok(handle) => handle,
            Err(_) => panic!("TaskTracker must not be closed when spawning tasks"),
        }
    }

    /// 派发一个接收取消 token 的可取消任务
    ///
    /// 适用于需要检查取消 token 并在其逻辑内优雅处理取消的任务。
    /// 任务函数必须返回 `CancellableTaskResult`，以正确区分取消与错误。
    ///
    /// # 参数
    ///
    /// * `task_fn` — 接收取消 token 并返回解析为 `CancellableTaskResult<T>` 的 future 的函数
    ///
    /// # 返回值
    /// 一个 [`TaskHandle`]，可用于等待完成并访问任务的取消 token
    ///
    /// # Panics
    /// 若 tracker 已关闭则 panic。这表明在 tracker 生命周期结束后还在派发任务，属于编程错误。
    ///
    /// # 示例
    /// ```rust
    /// # use pagoda_runtime::utils::tasks::tracker::{TaskTracker, CancellableTaskResult};
    /// # async fn example(tracker: TaskTracker) -> anyhow::Result<()> {
    /// let handle = tracker.spawn_cancellable(|cancel_token| async move {
    ///     tokio::select! {
    ///         _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
    ///             CancellableTaskResult::Ok(42)
    ///         },
    ///         _ = cancel_token.cancelled() => CancellableTaskResult::Cancelled,
    ///     }
    /// });
    ///
    /// // 访问该任务自身的取消 token
    /// let task_cancel_token = handle.cancellation_token();
    ///
    /// let result = handle.await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn spawn_cancellable<F, Fut, T>(&self, task_fn: F) -> TaskHandle<T>
    where
        F: FnMut(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = CancellableTaskResult<T>> + Send + 'static,
        T: Send + 'static,
    {
        match self.0.spawn_cancellable(task_fn) {
            Ok(handle) => handle,
            Err(_) => panic!("TaskTracker must not be closed when spawning tasks"),
        }
    }

    /// 获取该 tracker 的指标
    ///
    /// 指标仅针对该 tracker，不包含父级或子 tracker 的指标。
    ///
    /// # 示例
    /// ```rust
    /// # use pagoda_runtime::utils::tasks::tracker::TaskTracker;
    /// # fn example(tracker: &TaskTracker) {
    /// let metrics = tracker.metrics();
    /// println!("Success: {}, Failed: {}", metrics.success(), metrics.failed());
    /// # }
    /// ```
    pub fn metrics(&self) -> &dyn HierarchicalTaskMetrics {
        self.0.metrics.as_ref()
    }

    /// 取消该 tracker 及其所有任务
    ///
    /// 这会向所有当前运行中的任务发出取消信号，并阻止派发新任务。
    /// 取消是立即且强制的。
    ///
    /// # 示例
    /// ```rust
    /// # use pagoda_runtime::utils::tasks::tracker::TaskTracker;
    /// # async fn example(tracker: TaskTracker) -> anyhow::Result<()> {
    /// // 派发一个长时间运行的任务
    /// let handle = tracker.spawn_cancellable(|cancel_token| async move {
    ///     tokio::select! {
    ///         _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
    ///             pagoda_runtime::utils::tasks::tracker::CancellableTaskResult::Ok(42)
    ///         }
    ///         _ = cancel_token.cancelled() => {
    ///             pagoda_runtime::utils::tasks::tracker::CancellableTaskResult::Cancelled
    ///         }
    ///     }
    /// }).await?;
    ///
    /// // 取消 tracker（进而取消任务）
    /// tracker.cancel();
    /// # Ok(())
    /// # }
    /// ```
    pub fn cancel(&self) {
        self.0.cancel();
    }

    /// 检查该 tracker 是否已关闭
    pub fn is_closed(&self) -> bool {
        self.0.is_closed()
    }

    /// 获取该 tracker 的取消 token
    ///
    /// 允许外部代码观察或触发该 tracker 的取消。
    ///
    /// # 示例
    /// ```rust
    /// # use pagoda_runtime::utils::tasks::tracker::TaskTracker;
    /// # fn example(tracker: &TaskTracker) {
    /// let token = tracker.cancellation_token();
    /// // 可检查取消状态或手动取消
    /// if !token.is_cancelled() {
    ///     token.cancel();
    /// }
    /// # }
    /// ```
    pub fn cancellation_token(&self) -> CancellationToken {
        let token = self.0.cancellation_token();
        token
    }

    /// 获取活跃子 tracker 的数量
    ///
    /// 仅统计仍存活（未被 drop）的子 tracker。
    /// 已 drop 的子 tracker 会被自动清理。
    ///
    /// # 示例
    /// ```rust
    /// # use pagoda_runtime::utils::tasks::tracker::TaskTracker;
    /// # fn example(tracker: &TaskTracker) {
    /// let child_count = tracker.child_count();
    /// println!("This tracker has {} active children", child_count);
    /// # }
    /// ```
    pub fn child_count(&self) -> usize {
        self.0.child_count()
    }

    /// 创建带自定义配置的子 tracker 构造器
    ///
    /// 它提供对子 tracker 创建的细粒度控制，允许你覆盖调度器或错误策略，
    /// 同时保持父子关系。
    ///
    /// # 示例
    /// ```rust
    /// # use std::sync::Arc;
    /// # use tokio::sync::Semaphore;
    /// # use pagoda_runtime::utils::tasks::tracker::{TaskTracker, SemaphoreScheduler, LogOnlyPolicy};
    /// # fn example(parent: &TaskTracker) {
    /// // 自定义调度器，继承错误策略
    /// let child1 = parent.child_tracker_builder()
    ///     .scheduler(SemaphoreScheduler::with_permits(5))
    ///     .build().unwrap();
    ///
    /// // 自定义错误策略，继承调度器
    /// let child2 = parent.child_tracker_builder()
    ///     .error_policy(LogOnlyPolicy::new())
    ///     .build().unwrap();
    ///
    /// // 从父级继承两种策略
    /// let child3 = parent.child_tracker_builder()
    ///     .build().unwrap();
    /// # }
    /// ```
    pub fn child_tracker_builder(&self) -> ChildTrackerBuilder<'_> {
        let tracker = self;
        ChildTrackerBuilder::new(tracker)
    }

    /// Join 该 tracker 及所有子 tracker
    ///
    /// 该方法通过以下步骤优雅关停整个 tracker 层级：
    /// 1. 关闭所有 tracker（阻止派发新任务）
    /// 2. 等待所有现有任务完成
    ///
    /// 使用栈安全的遍历避免深层层级中的栈溢出。
    /// 先处理子节点再处理父节点，以确保正确的关停顺序。
    ///
    /// **层级化行为：**
    /// - 先处理子节点再处理父节点，以确保正确的关停顺序
    /// - 每个 tracker 在等待前先关闭（Tokio 要求）
    /// - 叶节点 tracker 仅关闭并等待自身的任务
    ///
    /// # 示例
    /// ```rust
    /// # use pagoda_runtime::utils::tasks::tracker::TaskTracker;
    /// # async fn example(tracker: TaskTracker) {
    /// tracker.join().await;
    /// # }
    /// ```
    pub async fn join(&self) {
        let inner = &self.0;
        inner.join().await
    }
}

impl TaskTrackerInner {
    /// 创建子 tracker：继承调度器/策略、独立指标，并具备层级化取消
    fn child_tracker(self: &Arc<Self>) -> anyhow::Result<Arc<TaskTrackerInner>> {
        if self.is_closed() {
            return Err(anyhow::anyhow!(
                "Cannot create child tracker from closed parent tracker"
            ));
        }

        let child_cancel_token = self.cancel_token.child_token();
        let inherited_scheduler = Arc::clone(&self.scheduler);
        let child_policy = self.error_policy.create_child();
        let child_metrics = Arc::new(ChildTaskMetrics::new(Arc::clone(&self.metrics)));

        let child = Arc::new(TaskTrackerInner {
            tokio_tracker: TokioTaskTracker::new(),
            parent: Some(self.clone()),
            scheduler: inherited_scheduler,
            error_policy: child_policy,
            metrics: child_metrics,
            cancel_token: child_cancel_token,
            children: RwLock::new(Vec::new()),
        });

        self.children.write().unwrap().push(Arc::downgrade(&child));
        self.cleanup_dead_children();

        Ok(child)
    }

    /// spawn 实现 — 校验 tracker 状态、生成任务 ID、应用策略并跟踪执行
    fn spawn<F, T>(self: &Arc<Self>, future: F) -> Result<TaskHandle<T>, TaskError>
    where
        F: Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        if self.tokio_tracker.is_closed() {
            return Err(TaskError::TrackerClosed);
        }

        let task_id = self.generate_task_id();
        self.metrics.increment_issued();
        let task_cancel_token = self.cancel_token.child_token();
        let join_token = task_cancel_token.clone();
        let tracker = Arc::clone(self);
        let wrapped_future = async move {
            Self::execute_with_policies(task_id, future, join_token, tracker).await
        };
        let join_handle = self.tokio_tracker.spawn(wrapped_future);

        Ok(TaskHandle::new(join_handle, task_cancel_token))
    }

    /// spawn_cancellable 实现 — 校验状态、提供取消 token、处理 CancellableTaskResult
    fn spawn_cancellable<F, Fut, T>(
        self: &Arc<Self>,
        task_fn: F,
    ) -> Result<TaskHandle<T>, TaskError>
    where
        F: FnMut(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = CancellableTaskResult<T>> + Send + 'static,
        T: Send + 'static,
    {
        if self.tokio_tracker.is_closed() {
            return Err(TaskError::TrackerClosed);
        }

        let task_id = self.generate_task_id();
        self.metrics.increment_issued();
        let task_cancel_token = self.cancel_token.child_token();
        let join_token = task_cancel_token.clone();
        let tracker = Arc::clone(self);
        let wrapped_future = async move {
            Self::execute_cancellable_with_policies(task_id, task_fn, join_token, tracker).await
        };
        let join_handle = self.tokio_tracker.spawn(wrapped_future);

        Ok(TaskHandle::new(join_handle, task_cancel_token))
    }

    /// 取消该 tracker 及其所有任务 — 实现
    fn cancel(&self) {
        self.cancel_token.cancel();
        self.tokio_tracker.close();
    }

    /// 若底层 tokio tracker 已关闭则返回 true
    fn is_closed(&self) -> bool {
        self.tokio_tracker.is_closed()
    }

    /// 使用 TaskId::new() 生成唯一任务 ID
    fn generate_task_id(&self) -> TaskId {
        TaskId::new()
    }

    /// 从子节点列表中移除失效的弱引用，以防止内存泄漏
    fn cleanup_dead_children(&self) {
        let mut children_guard = self.children.write().unwrap();
        children_guard.retain(|child| child.strong_count() > 0);
    }

    /// 返回取消 token 的克隆
    fn cancellation_token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }

    /// 统计活跃子 tracker 数量（过滤失效的弱引用）
    fn child_count(&self) -> usize {
        let children_guard = self.children.read().unwrap();
        children_guard.iter().fold(0usize, |count, child| {
            if child.strong_count() > 0 {
                count + 1
            } else {
                count
            }
        })
    }

    /// join 实现 — 关闭层级中所有 tracker，然后用栈安全遍历等待任务完成
    async fn join(self: &Arc<Self>) {
        if self.children.read().unwrap().is_empty() {
            self.tokio_tracker.close();
            self.tokio_tracker.wait().await;
            return;
        }

        for tracker in self.collect_hierarchy() {
            tracker.tokio_tracker.close();
            tracker.tokio_tracker.wait().await;
        }
    }

    /// 用迭代 DFS 采集层级，以后序（子节点在父节点之前）返回 Vec，以便安全关停
    fn collect_hierarchy(self: &Arc<TaskTrackerInner>) -> Vec<Arc<TaskTrackerInner>> {
        let mut hierarchy = Vec::new();
        let mut stack = vec![self.clone()];
        let mut visited = HashSet::new();

        while let Some(tracker) = stack.pop() {
            let tracker_ptr = Arc::as_ptr(&tracker) as usize;
            if !visited.insert(tracker_ptr) {
                continue;
            }

            hierarchy.push(Arc::clone(&tracker));
            if let Ok(children_guard) = tracker.children.read() {
                stack.extend(children_guard.iter().filter_map(Weak::upgrade));
            }
        }

        hierarchy.reverse();
        hierarchy
    }

    /// 以调度与错误处理策略执行一个普通任务
    #[tracing::instrument(level = "debug", skip_all, fields(task_id = %task_id))]
    async fn execute_with_policies<F, T>(
        task_id: TaskId,
        future: F,
        task_cancel_token: CancellationToken,
        inner: Arc<TaskTrackerInner>,
    ) -> Result<T, TaskError>
    where
        F: Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        Self::execute_with_retry_loop(
            task_id,
            RegularTaskExecutor::new(future),
            task_cancel_token,
            inner,
        )
        .await
    }

    /// 以调度与错误处理策略执行一个可取消任务
    #[tracing::instrument(level = "debug", skip_all, fields(task_id = %task_id))]
    async fn execute_cancellable_with_policies<F, Fut, T>(
        task_id: TaskId,
        task_fn: F,
        task_cancel_token: CancellationToken,
        inner: Arc<TaskTrackerInner>,
    ) -> Result<T, TaskError>
    where
        F: FnMut(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = CancellableTaskResult<T>> + Send + 'static,
        T: Send + 'static,
    {
        Self::execute_with_retry_loop(
            task_id,
            CancellableTaskExecutor::new(task_fn),
            task_cancel_token,
            inner,
        )
        .await
    }

    /// 带重试支持的核心执行循环 — 两种任务类型统一使用
    #[tracing::instrument(level = "debug", skip_all, fields(task_id = %task_id))]
    async fn execute_with_retry_loop<E, T>(
        task_id: TaskId,
        initial_executor: E,
        task_cancellation_token: CancellationToken,
        inner: Arc<TaskTrackerInner>,
    ) -> Result<T, TaskError>
    where
        E: TaskExecutor<T> + Send + 'static,
        T: Send + 'static,
    {
        debug!("Starting task execution");

        // 活跃计数器的 RAII 护卫 — 创建时递增，drop 时递减
        struct ActiveCountGuard {
            metrics: Arc<dyn HierarchicalTaskMetrics>,
            is_active: bool,
        }

        impl ActiveCountGuard {
            fn new(metrics: Arc<dyn HierarchicalTaskMetrics>) -> Self {
                Self {
                    metrics,
                    is_active: false,
                }
            }

            fn activate(&mut self) {
                if !self.is_active {
                    self.metrics.increment_started();
                    self.is_active = true;
                }
            }
        }

        // 当前可执行体 — 要么是原始 TaskExecutor，要么是 Continuation
        enum CurrentExecutable<E>
        where
            E: Send + 'static,
        {
            TaskExecutor(E),
            Continuation(Arc<dyn Continuation + Send + Sync + 'static>),
        }

        let mut current_executable = CurrentExecutable::TaskExecutor(initial_executor);
        let mut active_guard = ActiveCountGuard::new(inner.metrics.clone());
        let mut error_context: Option<OnErrorContext> = None;
        let mut scheduler_guard_state = self::GuardState::Keep;
        let mut guard_result = inner
            .scheduler
            .acquire_execution_slot(task_cancellation_token.child_token())
            .instrument(tracing::debug_span!("scheduler_resource_reacquisition"))
            .await;

        loop {
            if scheduler_guard_state == self::GuardState::Reschedule {
                guard_result = inner
                    .scheduler
                    .acquire_execution_slot(inner.cancel_token.child_token())
                    .instrument(tracing::debug_span!("scheduler_resource_reacquisition"))
                    .await;
            }

            match &guard_result {
                SchedulingResult::Execute(_guard) => {
                    active_guard.activate();

                    let execution_result = async {
                        debug!("Executing task with acquired resources");
                        match &mut current_executable {
                            CurrentExecutable::TaskExecutor(executor) => {
                                executor.execute(inner.cancel_token.child_token()).await
                            }
                            CurrentExecutable::Continuation(continuation) => {
                                let continuation_result =
                                    continuation.execute(inner.cancel_token.child_token()).await;

                                match continuation_result {
                                    TaskExecutionResult::Success(value) => match value.downcast::<T>() {
                                        Ok(typed_value) => TaskExecutionResult::Success(*typed_value),
                                        Err(_) => {
                                            let type_error = anyhow::anyhow!(
                                                "Continuation task returned wrong type"
                                            );
                                            error!(
                                                ?type_error,
                                                "Type mismatch in continuation task result"
                                            );
                                            TaskExecutionResult::Error(type_error)
                                        }
                                    }
                                    TaskExecutionResult::Cancelled => TaskExecutionResult::Cancelled,
                                    TaskExecutionResult::Error(error) => TaskExecutionResult::Error(error),
                                }
                            }
                        }
                    }
                    .instrument(tracing::debug_span!("task_execution"))
                    .await;

                    match execution_result {
                        TaskExecutionResult::Success(value) => {
                            inner.metrics.increment_success();
                            debug!("Task completed successfully");
                            return Ok(value);
                        }
                        TaskExecutionResult::Cancelled => {
                            inner.metrics.increment_cancelled();
                            debug!("Task was cancelled during execution");
                            return Err(TaskError::Cancelled);
                        }
                        TaskExecutionResult::Error(error) => {
                            debug!("Task failed - handling error through policy - {error:?}");

                            let (action_result, guard_state) = Self::handle_task_error(
                                &error,
                                &mut error_context,
                                task_id,
                                &inner,
                            )
                            .await;

                            scheduler_guard_state = guard_state;

                            match action_result {
                                ActionResult::Fail => {
                                    inner.metrics.increment_failed();
                                    debug!("Policy accepted error - task failed {error:?}");
                                    return Err(TaskError::Failed(error));
                                }
                                ActionResult::Shutdown => {
                                    inner.metrics.increment_failed();
                                    warn!("Policy triggered shutdown - {error:?}");
                                    inner.cancel();
                                    return Err(TaskError::Failed(error));
                                }
                                ActionResult::Continue { continuation } => {
                                    debug!(
                                        "Policy provided next executable - continuing loop - {error:?}"
                                    );
                                    current_executable =
                                        CurrentExecutable::Continuation(continuation);
                                    continue;
                                }
                            }
                        }
                    }
                }
                SchedulingResult::Cancelled => {
                    inner.metrics.increment_cancelled();
                    debug!("Task was cancelled during resource acquisition");
                    return Err(TaskError::Cancelled);
                }
                SchedulingResult::Rejected(reason) => {
                    inner.metrics.increment_rejected();
                    debug!(reason, "Task was rejected by scheduler");
                    return Err(TaskError::Failed(anyhow::anyhow!(
                        "Task rejected: {}",
                        reason
                    )));
                }
            }
        }
    }

    /// 通过错误策略处理任务错误并返回要采取的动作
    async fn handle_task_error(
        error: &anyhow::Error,
        error_context: &mut Option<OnErrorContext>,
        task_id: TaskId,
        inner: &Arc<TaskTrackerInner>,
    ) -> (ActionResult, self::GuardState) {
        let context = error_context.get_or_insert_with(|| OnErrorContext {
            attempt_count: 0,
            task_id,
            execution_context: TaskExecutionContext {
                scheduler: inner.scheduler.clone(),
                metrics: inner.metrics.clone(),
            },
            state: inner.error_policy.create_context(),
        });

        context.attempt_count += 1;
        let current_attempt = context.attempt_count;

        if inner.error_policy.allow_continuation(error, context) {
            if let Some(continuation_err) = error.downcast_ref::<FailedWithContinuation>() {
                debug!(
                    task_id = %task_id,
                    attempt_count = current_attempt,
                    "Task provided FailedWithContinuation and policy allows continuations - {error:?}"
                );

                let continuation = continuation_err.continuation.clone();
                let guard_state = match inner.error_policy.should_reschedule(error, context) {
                    true => self::GuardState::Reschedule,
                    false => self::GuardState::Keep,
                };

                return (ActionResult::Continue { continuation }, guard_state);
            }
        } else {
            debug!(
                task_id = %task_id,
                attempt_count = current_attempt,
                "Policy rejected continuations, ignoring any FailedWithContinuation - {error:?}"
            );
        }

        let response = inner.error_policy.on_error(error, context);

        match response {
            ErrorResponse::Fail => (ActionResult::Fail, self::GuardState::Keep),
            ErrorResponse::Shutdown => (ActionResult::Shutdown, self::GuardState::Keep),
            ErrorResponse::Custom(action) => {
                debug!("Task failed - executing custom action - {error:?}");

                let action_result = action
                    .execute(error, task_id, current_attempt, &context.execution_context)
                    .await;
                debug!(?action_result, "Custom action completed");

                let guard_state = match &action_result {
                    ActionResult::Continue { .. } if inner.error_policy.should_reschedule(error, context) => {
                        self::GuardState::Reschedule
                    }
                    _ => self::GuardState::Keep,
                };

                (action_result, guard_state)
            }
        }
    }
}

// 针对所有调度器的概括实现
impl ArcPolicy for UnlimitedScheduler {}
impl ArcPolicy for SemaphoreScheduler {}

// 针对所有错误策略的概括实现
impl ArcPolicy for LogOnlyPolicy {}
impl ArcPolicy for CancelOnError {}
impl ArcPolicy for ThresholdCancelPolicy {}
impl ArcPolicy for RateCancelPolicy {}

/// 用于无限调度的资源护卫
///
/// 该护卫代表「无限」资源 — 没有实际的资源约束。
/// 因为无资源需要管理，该护卫本质上是空操作。
#[derive(Debug)]
pub struct UnlimitedGuard;

impl ResourceGuard for UnlimitedGuard {
    // 无资源需要管理 — 仅实现标记 trait
}

// === SECTION: 调度器实现 ===

/// 立即执行所有任务的无限任务调度器
///
/// 该调度器不设并发限制，立即执行所有提交的任务。
/// 适用于测试、高吞吐场景，或由外部系统提供并发控制的场景。
///
/// ## 取消行为
///
/// - 在资源获取前尊重取消 token
/// - 一旦开始执行（通过 ResourceGuard），始终等待任务完成
/// - 任务在内部自行处理取消（若由 `spawn_cancellable` 创建）
///
/// # 示例
/// ```rust
/// # use pagoda_runtime::utils::tasks::tracker::UnlimitedScheduler;
/// let scheduler = UnlimitedScheduler::new();
/// ```
#[derive(Debug)]
pub struct UnlimitedScheduler;

impl UnlimitedScheduler {
    /// 创建一个新的无限调度器并返回 Arc
    pub fn new() -> Arc<Self> {
        Self.new_arc()
    }
}

impl Default for UnlimitedScheduler {
    fn default() -> Self {
        UnlimitedScheduler
    }
}

#[async_trait]
impl TaskScheduler for UnlimitedScheduler {
    async fn acquire_execution_slot(
        &self,
        cancel_token: CancellationToken,
    ) -> SchedulingResult<Box<dyn ResourceGuard>> {
        debug!("Acquiring execution slot (unlimited scheduler)");

        if cancel_token.is_cancelled() {
            debug!("Task cancelled before acquiring execution slot");
            return SchedulingResult::Cancelled;
        }

        debug!("Execution slot acquired immediately");
        let guard: Box<dyn ResourceGuard> = Box::new(UnlimitedGuard);
        SchedulingResult::Execute(guard)
    }
}

/// 用于基于信号量调度的资源护卫
///
/// 该护卫持有一个信号量许可，并确保任务执行始终运行至完成。
/// 许可在护卫被 drop 时自动释放。
#[derive(Debug)]
pub struct SemaphoreGuard {
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl ResourceGuard for SemaphoreGuard {
    // 许可在护卫被 drop 时自动释放
}

/// 基于信号量的任务调度器
///
/// 使用 [`tokio::sync::Semaphore`] 限制并发任务执行。
/// 任务会等待可用许可后再执行。
///
/// ## 取消行为
///
/// - 在许可获取前与过程中尊重取消 token
/// - 一旦获取许可（通过 ResourceGuard），始终等待任务完成
/// - 持有许可直到任务完成（无论是否取消）
/// - 任务在内部自行处理取消（若由 `spawn_cancellable` 创建）
///
/// 这确保任务被取消时许可不会泄漏，同时仍允许可取消任务自行优雅终止。
///
/// # Example
/// ```rust
/// # use std::sync::Arc;
/// # use tokio::sync::Semaphore;
/// # use pagoda_runtime::utils::tasks::tracker::SemaphoreScheduler;
/// // 允许最多 5 个并发任务
/// let semaphore = Arc::new(Semaphore::new(5));
/// let scheduler = SemaphoreScheduler::new(semaphore);
/// ```
#[derive(Debug)]
pub struct SemaphoreScheduler {
    semaphore: Arc<Semaphore>,
}

impl SemaphoreScheduler {
    /// 创建一个新的信号量调度器
    ///
    /// # 参数
    /// * `semaphore` - 用于并发控制的信号量
    pub fn new(semaphore: Arc<Semaphore>) -> Self {
        let permits = semaphore;
        Self { semaphore: permits }
    }

    /// 以指定许可数创建信号量调度器，返回 Arc
    pub fn with_permits(permits: usize) -> Arc<Self> {
        let semaphore = Arc::new(Semaphore::new(permits));
        Self::new(semaphore).new_arc()
    }

    /// 获取可用许可数
    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }
}

#[async_trait]
impl TaskScheduler for SemaphoreScheduler {
    async fn acquire_execution_slot(
        &self,
        cancel_token: CancellationToken,
    ) -> SchedulingResult<Box<dyn ResourceGuard>> {
        debug!("Acquiring semaphore permit");

        if cancel_token.is_cancelled() {
            debug!("Task cancelled before acquiring semaphore permit");
            return SchedulingResult::Cancelled;
        }

        let owned_semaphore = Arc::clone(&self.semaphore);
        let permit = tokio::select! {
            result = owned_semaphore.acquire_owned() => {
                match result {
                    Ok(permit) => permit,
                    Err(_) => return SchedulingResult::Cancelled,
                }
            }
            _ = cancel_token.cancelled() => {
                debug!("Task cancelled while waiting for semaphore permit");
                return SchedulingResult::Cancelled;
            }
        };

        debug!("Acquired semaphore permit");
        let guard: Box<dyn ResourceGuard> = Box::new(SemaphoreGuard { _permit: permit });
        SchedulingResult::Execute(guard)
    }
}

/// 根据错误模式触发取消的错误策略
///
/// 该策略分析错误信息，并在以下情况返回 `ErrorResponse::Shutdown`：
/// - 未指定任何模式（任何错误都取消）
/// - 错误信息匹配指定模式之一
///
/// 实际取消由 TaskTracker 处理 — 该策略仅做决策。
///
/// # 示例
/// ```rust
/// # use pagoda_runtime::utils::tasks::tracker::CancelOnError;
/// // 对任何错误取消
/// let policy = CancelOnError::new();
///
/// // 仅对特定错误模式取消
/// let (policy, _token) = CancelOnError::with_patterns(
///     vec!["OutOfMemory".to_string(), "DeviceError".to_string()]
/// );
/// ```
#[derive(Debug)]
pub struct CancelOnError {
    error_patterns: Vec<String>,
}

impl CancelOnError {
    /// 创建一个对任何错误都取消的策略
    ///
    /// 返回一个无错误模式的策略，意味着任何任务失败都会取消 TaskTracker。
    pub fn new() -> Arc<Self> {
        Self {
            error_patterns: Vec::new(),
        }
        .new_arc()
    }

    /// 以自定义错误模式创建取消策略，返回 Arc 与 token
    ///
    /// # 参数
    /// * `error_patterns` - 触发取消的错误信息模式列表
    pub fn with_patterns(error_patterns: Vec<String>) -> (Arc<Self>, CancellationToken) {
        let token = CancellationToken::new();
        let policy = Self { error_patterns }.new_arc();
        (policy, token)
    }
}

#[async_trait]
impl OnErrorPolicy for CancelOnError {
    fn create_child(&self) -> Arc<dyn OnErrorPolicy> {
        CancelOnError {
            error_patterns: self.error_patterns.clone(),
        }
        .new_arc()
    }

    fn create_context(&self) -> Option<Box<dyn std::any::Any + Send + 'static>> {
        None // 无状态策略 — 无堆分配
    }

    fn on_error(&self, error: &anyhow::Error, context: &mut OnErrorContext) -> ErrorResponse {
        error!(?context.task_id, "Task failed - {error:?}");

        if self.error_patterns.is_empty() {
            return ErrorResponse::Shutdown;
        }

        let error_str = error.to_string();
        if self
            .error_patterns
            .iter()
            .any(|pattern| error_str.contains(pattern))
        {
            return ErrorResponse::Shutdown;
        }

        ErrorResponse::Fail
    }
}

// === SECTION: 错误策略实现 ===

/// 仅记录错误的简单错误策略
///
/// 该策略不触发取消，适用于非关键任务，或希望在外部处理错误的场景。
#[derive(Debug)]
pub struct LogOnlyPolicy;

impl LogOnlyPolicy {
    /// 创建一个新的仅记录策略，返回 Arc
    pub fn new() -> Arc<Self> {
        Self.new_arc()
    }
}

impl Default for LogOnlyPolicy {
    fn default() -> Self {
        LogOnlyPolicy
    }
}

impl OnErrorPolicy for LogOnlyPolicy {
    fn create_child(&self) -> Arc<dyn OnErrorPolicy> {
        LogOnlyPolicy.new_arc()
    }

    fn create_context(&self) -> Option<Box<dyn std::any::Any + Send + 'static>> {
        None // 无状态策略 — 无堆分配
    }

    fn on_error(&self, error: &anyhow::Error, context: &mut OnErrorContext) -> ErrorResponse {
        error!(?context.task_id, "Task failed - logging only - {error:?}");
        ErrorResponse::Fail
    }
}

/// 在失败次数达到阈值后取消任务的错误策略
///
/// 该策略跟踪失败任务数，并在失败计数超过指定阈值时触发取消。
/// 适用于防止分布式系统中的连锁故障。
///
/// # 示例
/// ```rust
/// # use pagoda_runtime::utils::tasks::tracker::ThresholdCancelPolicy;
/// // 5 次失败后取消
/// let policy = ThresholdCancelPolicy::with_threshold(5);
/// ```
#[derive(Debug)]
pub struct ThresholdCancelPolicy {
    max_failures: usize,
    failure_count: AtomicU64,
}

impl ThresholdCancelPolicy {
    /// 以指定失败阈值创建阈值取消策略，返回 Arc 与 token
    ///
    /// # 参数
    /// * `max_failures` - 取消前的最大失败次数
    pub fn with_threshold(max_failures: usize) -> Arc<Self> {
        Self {
            max_failures,
            failure_count: AtomicU64::new(0),
        }
        .new_arc()
    }

    /// 获取当前失败计数
    pub fn failure_count(&self) -> u64 {
        read_counter(&self.failure_count)
    }

    /// 将失败计数重置为零
    ///
    /// 这主要用于需要在测试用例间重置策略状态的测试场景。
    pub fn reset_failure_count(&self) {
        self.failure_count.store(0, Ordering::Relaxed);
    }
}

/// ThresholdCancelPolicy 的每任务状态
#[derive(Debug)]
struct ThresholdState {
    failure_count: u32,
}

impl OnErrorPolicy for ThresholdCancelPolicy {
    fn create_child(&self) -> Arc<dyn OnErrorPolicy> {
        ThresholdCancelPolicy {
            max_failures: self.max_failures,
            failure_count: AtomicU64::new(0),
        }
        .new_arc()
    }

    fn create_context(&self) -> Option<Box<dyn std::any::Any + Send + 'static>> {
        Some(Box::new(ThresholdState { failure_count: 0 }))
    }

    fn on_error(&self, error: &anyhow::Error, context: &mut OnErrorContext) -> ErrorResponse {
        error!(?context.task_id, "Task failed - {error:?}");

        let global_failures = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
        let state = context
            .state
            .as_mut()
            .expect("ThresholdCancelPolicy requires state")
            .downcast_mut::<ThresholdState>()
            .expect("Context type mismatch");

        state.failure_count += 1;
        let current_failures = state.failure_count;

        if current_failures >= self.max_failures as u32 {
            warn!(
                ?context.task_id,
                current_failures,
                global_failures,
                max_failures = self.max_failures,
                "Per-task failure threshold exceeded, triggering cancellation"
            );
            ErrorResponse::Shutdown
        } else {
            debug!(
                ?context.task_id,
                current_failures,
                global_failures,
                max_failures = self.max_failures,
                "Task failed, tracking per-task failure count"
            );
            ErrorResponse::Fail
        }
    }
}

/// 在时间窗口内失败率超过阈值时取消任务的错误策略
///
/// 该策略在滑动时间窗口内跟踪失败，并在失败率超过指定阈值时触发取消。
/// 相比简单的计数阈值更复杂，因为它考虑了时间维度。
///
/// # 示例
/// ```rust
/// # use pagoda_runtime::utils::tasks::tracker::RateCancelPolicy;
/// // 若任意 60 秒窗口内超过 50% 的任务失败则取消
/// let (policy, token) = RateCancelPolicy::builder()
///     .rate(0.5)
///     .window_secs(60)
///     .build();
/// ```
#[derive(Debug)]
pub struct RateCancelPolicy {
    cancel_token: CancellationToken,
    max_failure_rate: f32,
    window_secs: u64,
    // TODO: 需要时实现时间窗口跟踪
    // 目前这是一个定义了接口的占位结构
}

impl RateCancelPolicy {
    /// 创建基于失败率的取消策略构建器
    pub fn builder() -> RateCancelPolicyBuilder {
        RateCancelPolicyBuilder::new()
    }
}

/// RateCancelPolicy 的构建器
pub struct RateCancelPolicyBuilder {
    max_failure_rate: Option<f32>,
    window_secs: Option<u64>,
}

impl RateCancelPolicyBuilder {
    fn new() -> Self {
        RateCancelPolicyBuilder {
            max_failure_rate: None,
            window_secs: None,
        }
    }

    /// 设置取消前的最大失败率（0.0 到 1.0）
    pub fn rate(mut self, max_failure_rate: f32) -> Self {
        self.max_failure_rate = Some(max_failure_rate);
        self
    }

    /// 设置用于速率计算的时间窗口（秒）
    pub fn window_secs(mut self, window_secs: u64) -> Self {
        self.window_secs = Some(window_secs);
        self
    }

    /// 构建策略，返回 Arc 与取消 token
    pub fn build(self) -> (Arc<RateCancelPolicy>, CancellationToken) {
        let max_failure_rate = self.max_failure_rate.expect("rate must be set");
        let window_secs = self.window_secs.expect("window_secs must be set");

        let token = CancellationToken::new();
        let policy = RateCancelPolicy {
            cancel_token: token.clone(),
            max_failure_rate,
            window_secs,
        }
        .new_arc();
        (policy, token)
    }
}

#[async_trait]
impl OnErrorPolicy for RateCancelPolicy {
    fn create_child(&self) -> Arc<dyn OnErrorPolicy> {
        RateCancelPolicy {
            cancel_token: self.cancel_token.child_token(),
            max_failure_rate: self.max_failure_rate,
            window_secs: self.window_secs,
        }
        .new_arc()
    }

    fn create_context(&self) -> Option<Box<dyn std::any::Any + Send + 'static>> {
        None // 目前为无状态策略（TODO：添加时间窗口状态）
    }

    fn on_error(&self, error: &anyhow::Error, context: &mut OnErrorContext) -> ErrorResponse {
        error!(?context.task_id, "Task failed - {error:?}");

        // TODO: 实现时间窗口失败率计算
        // 目前仅记录错误并继续
        warn!(
            ?context.task_id,
            max_failure_rate = self.max_failure_rate,
            window_secs = self.window_secs,
            "Rate-based error policy - time window tracking not yet implemented"
        );

        ErrorResponse::Fail
    }
}

/// 执行时触发取消 token 的自定义动作
///
/// 该动作通过捕获外部取消 token 并在执行时触发它，
/// 演示了 ErrorResponse::Custom 的行为。
#[derive(Debug)]
pub struct TriggerCancellationTokenAction {
    cancel_token: CancellationToken,
}

impl TriggerCancellationTokenAction {
    pub fn new(cancel_token: CancellationToken) -> Self {
        let token = cancel_token;
        Self { cancel_token: token }
    }
}

#[async_trait]
impl OnErrorAction for TriggerCancellationTokenAction {
    async fn execute(
        &self,
        error: &anyhow::Error,
        task_id: TaskId,
        _attempt_count: u32,
        _context: &TaskExecutionContext,
    ) -> ActionResult {
        warn!(
            ?task_id,
            "Executing custom action: triggering cancellation token - {error:?}"
        );

        self.cancel_token.cancel();
        ActionResult::Shutdown
    }
}

/// 在任何错误时触发自定义取消 token 的测试错误策略
///
/// 该策略通过捕获外部取消 token 并在发生任何错误时触发它，
/// 演示了 ErrorResponse::Custom 的行为。用于测试自定义错误处理动作。
///
/// # 示例
/// ```rust
/// # use tokio_util::sync::CancellationToken;
/// # use pagoda_runtime::utils::tasks::tracker::TriggerCancellationTokenOnError;
/// let cancel_token = CancellationToken::new();
/// let policy = TriggerCancellationTokenOnError::new(cancel_token.clone());
///
/// // 策略会通过 ErrorResponse::Custom 在任何错误时触发该 token
/// ```
#[derive(Debug)]
pub struct TriggerCancellationTokenOnError {
    cancel_token: CancellationToken,
}

impl TriggerCancellationTokenOnError {
    /// 创建一个在错误时触发给定取消 token 的策略
    pub fn new(cancel_token: CancellationToken) -> Arc<Self> {
        Arc::new(Self { cancel_token })
    }
}

impl OnErrorPolicy for TriggerCancellationTokenOnError {
    fn create_child(&self) -> Arc<dyn OnErrorPolicy> {
        Arc::new(TriggerCancellationTokenOnError {
            cancel_token: self.cancel_token.clone(),
        })
    }

    fn create_context(&self) -> Option<Box<dyn std::any::Any + Send + 'static>> {
        None // 无状态策略 — 无堆分配
    }

    fn on_error(&self, error: &anyhow::Error, context: &mut OnErrorContext) -> ErrorResponse {
        error!(
            ?context.task_id,
            "Task failed - triggering custom cancellation token - {error:?}"
        );

        let action = TriggerCancellationTokenAction::new(self.cancel_token.clone());
        ErrorResponse::Custom(Box::new(action))
    }
}

// === SECTION: tests ===

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;
    use std::sync::atomic::AtomicU32;
    use std::time::Duration;

    // 使用 rstest 的测试 fixture
    #[fixture]
    fn semaphore_scheduler() -> Arc<SemaphoreScheduler> {
        Arc::new(SemaphoreScheduler::new(Arc::new(Semaphore::new(5))))
    }

    #[fixture]
    fn unlimited_scheduler() -> Arc<UnlimitedScheduler> {
        UnlimitedScheduler::new()
    }

    #[fixture]
    fn log_policy() -> Arc<LogOnlyPolicy> {
        LogOnlyPolicy::new()
    }

    #[fixture]
    fn cancel_policy() -> Arc<CancelOnError> {
        CancelOnError::new()
    }

    #[fixture]
    fn basic_tracker(
        unlimited_scheduler: Arc<UnlimitedScheduler>,
        log_policy: Arc<LogOnlyPolicy>,
    ) -> TaskTracker {
        TaskTracker::new(unlimited_scheduler, log_policy).unwrap()
    }

    #[rstest]
    #[tokio::test]
    async fn test_basic_task_execution(basic_tracker: TaskTracker) {
        // 测试基础任务执行与成功指标统计。
        let (tx, rx) = tokio::sync::oneshot::channel();
        let handle = basic_tracker.spawn(async {
            // 等待完成信号而非使用 sleep
            rx.await.ok();
            Ok(42)
        });

        // 通知任务完成
        tx.send(()).ok();

        // 验证任务成功完成
        let result = handle
            .await
            .expect("Task should complete")
            .expect("Task should succeed");
        assert_eq!(result, 42);

        // 验证指标
        assert_eq!(basic_tracker.metrics().success(), 1);
        assert_eq!(basic_tracker.metrics().failed(), 0);
        assert_eq!(basic_tracker.metrics().cancelled(), 0);
        assert_eq!(basic_tracker.metrics().active(), 0);
    }

    #[rstest]
    #[tokio::test]
    async fn test_task_failure(
        semaphore_scheduler: Arc<SemaphoreScheduler>,
        log_policy: Arc<LogOnlyPolicy>,
    ) {
        // 测试任务失败后的错误传播与指标统计。
        let tracker = TaskTracker::new(semaphore_scheduler, log_policy).unwrap();

        let handle = tracker.spawn(async { Err::<(), _>(anyhow::anyhow!("test error")) });

        let result = handle.await.unwrap();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TaskError::Failed(_)));

        // 验证指标
        assert_eq!(tracker.metrics().success(), 0);
        assert_eq!(tracker.metrics().failed(), 1);
        assert_eq!(tracker.metrics().cancelled(), 0);
    }

    #[rstest]
    #[tokio::test]
    async fn test_semaphore_concurrency_limit(log_policy: Arc<LogOnlyPolicy>) {
        // 测试信号量调度器的并发上限控制。
        let limited_scheduler = Arc::new(SemaphoreScheduler::new(Arc::new(Semaphore::new(2)))); // 仅允许 2 个并发任务
        let tracker = TaskTracker::new(limited_scheduler, log_policy).unwrap();

        let counter = Arc::new(AtomicU32::new(0));
        let max_concurrent = Arc::new(AtomicU32::new(0));

        // 使用广播通道协调所有任务
        let (tx, _) = tokio::sync::broadcast::channel(1);
        let mut handles = Vec::new();

        // 启动 5 个跟踪并发的任务
        for _ in 0..5 {
            let counter_clone = counter.clone();
            let max_clone = max_concurrent.clone();
            let mut rx = tx.subscribe();

            let handle = tracker.spawn(async move {
                // 递增活跃计数器
                let current = counter_clone.fetch_add(1, Ordering::Relaxed) + 1;

                // 跟踪最大并发数
                max_clone.fetch_max(current, Ordering::Relaxed);

                // 等待完成信号而非使用 sleep
                rx.recv().await.ok();

                // 完成时递减
                counter_clone.fetch_sub(1, Ordering::Relaxed);

                Ok(())
            });
            handles.push(handle);
        }

        // 给任务时间启动并登记并发
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // 通知所有任务完成
        tx.send(()).ok();

        // 等待所有任务完成
        for handle in handles {
            handle.await.unwrap().unwrap();
        }

        // 验证同时运行的任务不超过 2 个
        assert!(max_concurrent.load(Ordering::Relaxed) <= 2);

        // 验证所有任务成功完成
        assert_eq!(tracker.metrics().success(), 5);
        assert_eq!(tracker.metrics().failed(), 0);
    }

    #[rstest]
    #[tokio::test]
    async fn test_cancel_on_error_policy() {
        // 测试 CancelOnError 策略会触发 tracker 取消。
        let error_policy = cancel_policy();
        let scheduler = semaphore_scheduler();
        let tracker = TaskTracker::new(scheduler, error_policy).unwrap();

        // 启动一个会触发取消的任务
        let handle =
            tracker.spawn(async { Err::<(), _>(anyhow::anyhow!("OutOfMemory error occurred")) });

        // 等待错误发生
        let result = handle.await.unwrap();
        assert!(result.is_err());

        // 给取消时间传播
        tokio::time::sleep(Duration::from_millis(10)).await;

        // 验证取消 token 已被触发
        assert!(tracker.cancellation_token().is_cancelled());
    }

    #[rstest]
    #[tokio::test]
    async fn test_tracker_cancellation() {
        // 测试手动取消 tracker 后任务的表现。
        let error_policy = cancel_policy();
        let scheduler = semaphore_scheduler();
        let tracker = TaskTracker::new(scheduler, error_policy).unwrap();
        let cancel_token = tracker.cancellation_token().child_token();

        // 使用 oneshot 通道而非 sleep 以获得确定性时序
        let (_tx, rx) = tokio::sync::oneshot::channel::<()>();

        // 启动一个尊重取消的任务
        let handle = tracker.spawn({
            let cancel_token = cancel_token.clone();
            async move {
                tokio::select! {
                    _ = rx => Ok(()),
                    _ = cancel_token.cancelled() => Err(anyhow::anyhow!("Task was cancelled")),
                }
            }
        });

        // 取消 tracker
        tracker.cancel();

        // 任务应被取消
        let result = handle.await.unwrap();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TaskError::Cancelled));
    }

    #[rstest]
    #[tokio::test]
    async fn test_child_tracker_independence(
        semaphore_scheduler: Arc<SemaphoreScheduler>,
        log_policy: Arc<LogOnlyPolicy>,
    ) {
        // 测试子 tracker 生命周期独立于父 tracker。
        let parent = TaskTracker::new(semaphore_scheduler, log_policy).unwrap();

        let child = parent.child_tracker().unwrap();

        // 初始时两者均应可用
        assert!(!parent.is_closed());
        assert!(!child.is_closed());

        // 仅取消子节点
        child.cancel();

        // 父节点应保持可用
        assert!(!parent.is_closed());

        // 父节点仍可启动任务
        let handle = parent.spawn(async { Ok(42) });
        let result = handle.await.unwrap().unwrap();
        assert_eq!(result, 42);
    }

    #[rstest]
    #[tokio::test]
    async fn test_independent_metrics(
        semaphore_scheduler: Arc<SemaphoreScheduler>,
        log_policy: Arc<LogOnlyPolicy>,
    ) {
        // 测试父子 tracker 的指标隔离与汇总关系。
        let parent = TaskTracker::new(semaphore_scheduler, log_policy).unwrap();
        let child = parent.child_tracker().unwrap();

        // 在父节点运行任务
        let handle1 = parent.spawn(async { Ok(1) });
        handle1.await.unwrap().unwrap();

        // 在子节点运行任务
        let handle2 = child.spawn(async { Ok(2) });
        handle2.await.unwrap().unwrap();

        // 每个节点有自己的指标，但父节点看到的是汇总值
        assert_eq!(parent.metrics().success(), 2); // 父节点看到自身 + 子节点
        assert_eq!(child.metrics().success(), 1); // 子节点仅看到自身
        assert_eq!(parent.metrics().total_completed(), 2); // 父节点看到汇总总数
        assert_eq!(child.metrics().total_completed(), 1); // 子节点仅看到自身
    }

    #[rstest]
    #[tokio::test]
    async fn test_cancel_on_error_hierarchy() {
        // 测试子级错误策略触发的取消不会反向影响父级。
        let parent_error_policy = cancel_policy();
        let scheduler = semaphore_scheduler();
        let parent = TaskTracker::new(scheduler, parent_error_policy).unwrap();
        let parent_policy_token = parent.cancellation_token().child_token();
        let child = parent.child_tracker().unwrap();

        // 初始时不应有任何取消
        assert!(!parent_policy_token.is_cancelled());

        // 使用显式同步而非 sleep
        let (error_tx, error_rx) = tokio::sync::oneshot::channel();
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();

        // 启动一个监控任务观察父级策略 token 的取消
        let parent_token_monitor = parent_policy_token.clone();
        let monitor_handle = tokio::spawn(async move {
            tokio::select! {
                _ = parent_token_monitor.cancelled() => {
                    cancel_tx.send(true).ok();
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    cancel_tx.send(false).ok();
                }
            }
        });

        // 在子节点启动一个会触发取消的任务
        let handle = child.spawn(async move {
            let result = Err::<(), _>(anyhow::anyhow!("OutOfMemory in child"));
            error_tx.send(()).ok(); // 通知错误已发生
            result
        });

        // 等待错误发生
        let error_result = handle.await.unwrap();
        assert!(error_result.is_err());

        // 等待我们的错误信号
        error_rx.await.ok();

        // 检查父级策略 token 是否在超时内被取消
        let was_cancelled = cancel_rx.await.unwrap_or(false);
        monitor_handle.await.ok();

        // 基于层级化设计：子节点错误不应影响父节点。
        // 子节点获得带子 token 的自有策略，子节点取消
        // 不应向上传播到父级策略 token
        assert!(
            !was_cancelled,
            "Parent policy token should not be cancelled by child errors"
        );
        assert!(
            !parent_policy_token.is_cancelled(),
            "Parent policy token should remain active"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_graceful_shutdown(
        semaphore_scheduler: Arc<SemaphoreScheduler>,
        log_policy: Arc<LogOnlyPolicy>,
    ) {
        // 测试 join 会等待已有任务优雅完成。
        let tracker = TaskTracker::new(semaphore_scheduler, log_policy).unwrap();

        // 使用广播通道协调任务完成
        let (tx, _) = tokio::sync::broadcast::channel(1);
        let mut handles = Vec::new();

        // 启动一些任务
        for i in 0..3 {
            let mut rx = tx.subscribe();
            let handle = tracker.spawn(async move {
                // 等待信号而不是使用 sleep
                rx.recv().await.ok();
                Ok(i)
            });
            handles.push(handle);
        }

        // 在关闭前通知所有任务完成
        tx.send(()).ok();

        // 关闭 tracker 并等待完成
        tracker.join().await;

        // 所有任务应成功完成
        for handle in handles {
            let result = handle.await.unwrap().unwrap();
            assert!(result < 3);
        }

        // tracker 应已关闭
        assert!(tracker.is_closed());
    }

    #[rstest]
    #[tokio::test]
    async fn test_semaphore_scheduler_permit_tracking(log_policy: Arc<LogOnlyPolicy>) {
        // 测试信号量调度器的 permit 占用与释放追踪。
        let semaphore = Arc::new(Semaphore::new(3));
        let scheduler = Arc::new(SemaphoreScheduler::new(semaphore.clone()));
        let tracker = TaskTracker::new(scheduler.clone(), log_policy).unwrap();

        // 初始时所有许可应可用
        assert_eq!(scheduler.available_permits(), 3);

        // 使用广播通道协调任务完成
        let (tx, _) = tokio::sync::broadcast::channel(1);
        let mut handles = Vec::new();

        // 启动 3 个持有许可的任务
        for _ in 0..3 {
            let mut rx = tx.subscribe();
            let handle = tracker.spawn(async move {
                // 等待完成信号
                rx.recv().await.ok();
                Ok(())
            });
            handles.push(handle);
        }

        // 给任务时间获取许可
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // 所有许可应被占用
        assert_eq!(scheduler.available_permits(), 0);

        // 通知所有任务完成
        tx.send(()).ok();

        // 等待任务完成
        for handle in handles {
            handle.await.unwrap().unwrap();
        }

        // 所有许可应再次可用
        assert_eq!(scheduler.available_permits(), 3);
    }

    #[rstest]
    #[tokio::test]
    async fn test_builder_pattern(log_policy: Arc<LogOnlyPolicy>) {
        // 测试 TaskTracker builder 的创建流程。
        let scheduler = Arc::new(SemaphoreScheduler::new(Arc::new(Semaphore::new(5))));
        let error_policy = log_policy;

        let tracker = TaskTracker::builder()
            .scheduler(scheduler)
            .error_policy(error_policy)
            .build()
            .unwrap();

        // tracker 应拥有一个取消 token
        let token = tracker.cancellation_token();
        assert!(!token.is_cancelled());

        // 应能够启动任务
        let handle = tracker.spawn(async { Ok(42) });
        let result = handle.await.unwrap().unwrap();
        assert_eq!(result, 42);
    }

    #[rstest]
    #[tokio::test]
    async fn test_all_trackers_have_cancellation_tokens(log_policy: Arc<LogOnlyPolicy>) {
        // 测试根、子、孙 tracker 的取消 token 级联传播。
        let scheduler = Arc::new(SemaphoreScheduler::new(Arc::new(Semaphore::new(5))));
        let root = TaskTracker::new(scheduler, log_policy).unwrap();
        let child = root.child_tracker().unwrap();
        let grandchild = child.child_tracker().unwrap();

        // 所有节点都应拥有取消 token
        let root_token = root.cancellation_token();
        let child_token = child.cancellation_token();
        let grandchild_token = grandchild.cancellation_token();

        assert!(!root_token.is_cancelled());
        assert!(!child_token.is_cancelled());
        assert!(!grandchild_token.is_cancelled());

        // 子 token 应与父 token 不同
        // （无法直接比较 token，但可以测试行为）
        root_token.cancel();

        // 给取消时间传播
        tokio::time::sleep(Duration::from_millis(10)).await;

        // 根节点应被取消
        assert!(root_token.is_cancelled());
        // 子节点也应被取消（因为它们是子 token）
        assert!(child_token.is_cancelled());
        assert!(grandchild_token.is_cancelled());
    }

    #[rstest]
    #[tokio::test]
    async fn test_spawn_cancellable_task(log_policy: Arc<LogOnlyPolicy>) {
        // 测试可取消任务的派发、成功完成和取消路径。
        let scheduler = Arc::new(SemaphoreScheduler::new(Arc::new(Semaphore::new(5))));
        let tracker = TaskTracker::new(scheduler, log_policy).unwrap();

        // 测试成功完成
        let (tx, rx) = tokio::sync::oneshot::channel();
        let rx = Arc::new(tokio::sync::Mutex::new(Some(rx)));
        let handle = tracker.spawn_cancellable(move |_cancel_token| {
            let rx = rx.clone();
            async move {
                // 等待信号而非 sleep
                if let Some(rx) = rx.lock().await.take() {
                    rx.await.ok();
                }
                CancellableTaskResult::Ok(42)
            }
        });

        // 通知任务完成
        tx.send(()).ok();

        let result = handle.await.unwrap().unwrap();
        assert_eq!(result, 42);
        assert_eq!(tracker.metrics().success(), 1);

        // 测试取消处理
        let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
        let rx = Arc::new(tokio::sync::Mutex::new(Some(rx)));
        let handle = tracker.spawn_cancellable(move |cancel_token| {
            let rx = rx.clone();
            async move {
                tokio::select! {
                    _ = async {
                        if let Some(rx) = rx.lock().await.take() {
                            rx.await.ok();
                        }
                    } => CancellableTaskResult::Ok("should not complete"),
                _ = cancel_token.cancelled() => CancellableTaskResult::Cancelled,
                }
            }
        });

        // 取消 tracker
        tracker.cancel();

        let result = handle.await.unwrap();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TaskError::Cancelled));
    }

    #[rstest]
    #[tokio::test]
    async fn test_cancellable_task_metrics_tracking(log_policy: Arc<LogOnlyPolicy>) {
        // 测试可取消任务会计入 cancelled 而非 failed 指标。
        let scheduler = Arc::new(SemaphoreScheduler::new(Arc::new(Semaphore::new(5))));
        let tracker = TaskTracker::new(scheduler, log_policy).unwrap();

        // 基准指标
        assert_eq!(tracker.metrics().cancelled(), 0);
        assert_eq!(tracker.metrics().failed(), 0);
        assert_eq!(tracker.metrics().success(), 0);

        // 测试 1：任务先执行，随后在执行中被取消
        let (start_tx, start_rx) = tokio::sync::oneshot::channel::<()>();
        let (_continue_tx, continue_rx) = tokio::sync::oneshot::channel::<()>();

        let start_tx_shared = Arc::new(tokio::sync::Mutex::new(Some(start_tx)));
        let continue_rx_shared = Arc::new(tokio::sync::Mutex::new(Some(continue_rx)));

        let start_tx_for_task = start_tx_shared.clone();
        let continue_rx_for_task = continue_rx_shared.clone();

        let handle = tracker.spawn_cancellable(move |cancel_token| {
            let start_tx = start_tx_for_task.clone();
            let continue_rx = continue_rx_for_task.clone();
            async move {
                // 通知我们已开始执行
                if let Some(tx) = start_tx.lock().await.take() {
                    tx.send(()).ok();
                }

                // 等待继续信号或取消
                tokio::select! {
                    _ = async {
                        if let Some(rx) = continue_rx.lock().await.take() {
                            rx.await.ok();
                        }
                    } => CancellableTaskResult::Ok("completed normally"),
                _ = cancel_token.cancelled() => {
                    println!("Task detected cancellation and is returning Cancelled");
                    CancellableTaskResult::Cancelled
                },
                }
            }
        });

        // 等待任务开始执行
        start_rx.await.ok();

        // 现在在任务运行时取消
        println!("Cancelling tracker while task is executing...");
        tracker.cancel();

        // 等待任务完成
        let result = handle.await.unwrap();

        // 调试输出
        println!("Task result: {:?}", result);
        println!(
            "Cancelled: {}, Failed: {}, Success: {}",
            tracker.metrics().cancelled(),
            tracker.metrics().failed(),
            tracker.metrics().success()
        );

        // 该任务应被正确取消并正确计数
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TaskError::Cancelled));

        // 验证指标正确：应计入 cancelled 而非 failed
        assert_eq!(
            tracker.metrics().cancelled(),
            1,
            "Properly cancelled task should increment cancelled count"
        );
        assert_eq!(
            tracker.metrics().failed(),
            0,
            "Properly cancelled task should NOT increment failed count"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_cancellable_vs_error_metrics_distinction(log_policy: Arc<LogOnlyPolicy>) {
        // 测试取消与真实错误在指标上的区分。
        let scheduler = Arc::new(SemaphoreScheduler::new(Arc::new(Semaphore::new(5))));
        let tracker = TaskTracker::new(scheduler, log_policy).unwrap();

        // 测试 1：真实错误应计入 failed
        let handle1 = tracker.spawn_cancellable(|_cancel_token| async move {
            CancellableTaskResult::<i32>::Err(anyhow::anyhow!("This is a real error"))
        });

        let result1 = handle1.await.unwrap();
        assert!(result1.is_err());
        assert!(matches!(result1.unwrap_err(), TaskError::Failed(_)));
        assert_eq!(tracker.metrics().failed(), 1);
        assert_eq!(tracker.metrics().cancelled(), 0);

        // 测试 2：取消应计入 cancelled
        let handle2 = tracker.spawn_cancellable(|_cancel_token| async move {
            CancellableTaskResult::<i32>::Cancelled
        });

        let result2 = handle2.await.unwrap();
        assert!(result2.is_err());
        assert!(matches!(result2.unwrap_err(), TaskError::Cancelled));
        assert_eq!(tracker.metrics().failed(), 1); // 仍为之前的 1
        assert_eq!(tracker.metrics().cancelled(), 1); // 现在因取消变为 1
    }

    #[rstest]
    #[tokio::test]
    async fn test_spawn_cancellable_error_handling(log_policy: Arc<LogOnlyPolicy>) {
        // 测试可取消任务返回错误时的处理路径。
        let scheduler = Arc::new(SemaphoreScheduler::new(Arc::new(Semaphore::new(5))));
        let tracker = TaskTracker::new(scheduler, log_policy).unwrap();

        // 测试错误结果
        let handle = tracker.spawn_cancellable(|_cancel_token| async move {
            CancellableTaskResult::<i32>::Err(anyhow::anyhow!("test error"))
        });

        let result = handle.await.unwrap();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TaskError::Failed(_)));
        assert_eq!(tracker.metrics().failed(), 1);
    }

    #[rstest]
    #[tokio::test]
    async fn test_cancellation_before_execution(log_policy: Arc<LogOnlyPolicy>) {
        // 测试关闭后的 tracker 再派发任务会触发 panic。
        let scheduler = Arc::new(SemaphoreScheduler::new(Arc::new(Semaphore::new(1))));
        let tracker = TaskTracker::new(scheduler, log_policy).unwrap();

        // 先取消 tracker
        tracker.cancel();

        // 给取消时间传播到内部 tracker
        tokio::time::sleep(Duration::from_millis(5)).await;

        // 现在尝试派发任务 — 由于 tracker 已关闭应 panic
        let panic_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tracker.spawn(async { Ok(42) })
        }));

        // 使用新 API 应发生 panic
        assert!(
            panic_result.is_err(),
            "spawn() should panic when tracker is closed"
        );

        // 验证 panic 信息包含预期文本
        if let Err(panic_payload) = panic_result {
            if let Some(panic_msg) = panic_payload.downcast_ref::<String>() {
                assert!(
                    panic_msg.contains("TaskTracker must not be closed"),
                    "Panic message should indicate tracker is closed: {}",
                    panic_msg
                );
            } else if let Some(panic_msg) = panic_payload.downcast_ref::<&str>() {
                assert!(
                    panic_msg.contains("TaskTracker must not be closed"),
                    "Panic message should indicate tracker is closed: {}",
                    panic_msg
                );
            }
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_semaphore_scheduler_with_cancellation(log_policy: Arc<LogOnlyPolicy>) {
        // 测试等待 permit 的任务会响应取消信号。
        let scheduler = Arc::new(SemaphoreScheduler::new(Arc::new(Semaphore::new(1))));
        let tracker = TaskTracker::new(scheduler, log_policy).unwrap();

        // 启动一个长时任务以占用信号量
        let blocker_token = tracker.cancellation_token();
        let _blocker_handle = tracker.spawn(async move {
            // 等待取消
            blocker_token.cancelled().await;
            Ok(())
        });

        // 给阻塞任务时间获取 permit
        tokio::task::yield_now().await;

        // 为第二个任务使用 oneshot 通道
        let (_tx, rx) = tokio::sync::oneshot::channel::<()>();

        // 启动另一个将等待信号量的任务
        let handle = tracker.spawn(async {
            rx.await.ok();
            Ok(42)
        });

        // 在第二个任务等待 permit 时取消 tracker
        tracker.cancel();

        // 等待中的任务应被取消
        let result = handle.await.unwrap();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TaskError::Cancelled));
    }

    #[rstest]
    #[tokio::test]
    async fn test_child_tracker_cancellation_independence(
        semaphore_scheduler: Arc<SemaphoreScheduler>,
        log_policy: Arc<LogOnlyPolicy>,
    ) {
        // 测试取消子 tracker 不会影响父 tracker。
        let parent = TaskTracker::new(semaphore_scheduler, log_policy).unwrap();
        let child = parent.child_tracker().unwrap();

        // 仅取消子节点
        child.cancel();

        // 父节点应仍可用
        let parent_token = parent.cancellation_token();
        assert!(!parent_token.is_cancelled());

        // 父节点仍可启动任务
        let handle = parent.spawn(async { Ok(42) });
        let result = handle.await.unwrap().unwrap();
        assert_eq!(result, 42);

        // 子节点应被取消
        let child_token = child.cancellation_token();
        assert!(child_token.is_cancelled());
    }

    #[rstest]
    #[tokio::test]
    async fn test_parent_cancellation_propagates_to_children(
        semaphore_scheduler: Arc<SemaphoreScheduler>,
        log_policy: Arc<LogOnlyPolicy>,
    ) {
        // 测试父 tracker 的取消会向所有子级传播。
        let parent = TaskTracker::new(semaphore_scheduler, log_policy).unwrap();
        let child1 = parent.child_tracker().unwrap();
        let child2 = parent.child_tracker().unwrap();
        let grandchild = child1.child_tracker().unwrap();

        // 取消父节点
        parent.cancel();

        // 给取消时间传播
        tokio::time::sleep(Duration::from_millis(10)).await;

        // 所有节点应被取消
        assert!(parent.cancellation_token().is_cancelled());
        assert!(child1.cancellation_token().is_cancelled());
        assert!(child2.cancellation_token().is_cancelled());
        assert!(grandchild.cancellation_token().is_cancelled());
    }

    #[rstest]
    #[tokio::test]
    async fn test_issued_counter_tracking(log_policy: Arc<LogOnlyPolicy>) {
        // 测试 issued、pending 等计数器的更新。
        let scheduler = Arc::new(SemaphoreScheduler::new(Arc::new(Semaphore::new(2))));
        let tracker = TaskTracker::new(scheduler, log_policy).unwrap();

        // 初始时未发出任务
        assert_eq!(tracker.metrics().issued(), 0);
        assert_eq!(tracker.metrics().pending(), 0);

        // 启动一些任务
        let handle1 = tracker.spawn(async { Ok(1) });
        let handle2 = tracker.spawn(async { Ok(2) });
        let handle3 = tracker.spawn_cancellable(|_| async { CancellableTaskResult::Ok(3) });

        // issued 计数器应立即递增
        assert_eq!(tracker.metrics().issued(), 3);
        assert_eq!(tracker.metrics().pending(), 3); // 尚未有任务完成

        // 完成任务
        assert_eq!(handle1.await.unwrap().unwrap(), 1);
        assert_eq!(handle2.await.unwrap().unwrap(), 2);
        assert_eq!(handle3.await.unwrap().unwrap(), 3);

        // 检查最终计数
        assert_eq!(tracker.metrics().issued(), 3);
        assert_eq!(tracker.metrics().success(), 3);
        assert_eq!(tracker.metrics().total_completed(), 3);
        assert_eq!(tracker.metrics().pending(), 0); // 全部完成

        // 测试层级计数
        let child = tracker.child_tracker().unwrap();
        let child_handle = child.spawn(async { Ok(42) });

        // 父子都应看到发出的任务
        assert_eq!(child.metrics().issued(), 1);
        assert_eq!(tracker.metrics().issued(), 4); // 父节点看到全部

        child_handle.await.unwrap().unwrap();

        // 最终层级检查
        assert_eq!(child.metrics().pending(), 0);
        assert_eq!(tracker.metrics().pending(), 0);
        assert_eq!(tracker.metrics().success(), 4); // 父节点看到所有成功
    }

    #[rstest]
    #[tokio::test]
    async fn test_child_tracker_builder(log_policy: Arc<LogOnlyPolicy>) {
        // 测试子 tracker builder 能覆写策略并正常工作。
        let parent_scheduler = Arc::new(SemaphoreScheduler::new(Arc::new(Semaphore::new(10))));
        let parent = TaskTracker::new(parent_scheduler, log_policy).unwrap();

        // 以自定义错误策略创建子节点
        let child_error_policy = CancelOnError::new();
        let child = parent
            .child_tracker_builder()
            .error_policy(child_error_policy)
            .build()
            .unwrap();

        // 测试子节点可用
        let handle = child.spawn(async { Ok(42) });
        let result = handle.await.unwrap().unwrap();
        assert_eq!(result, 42);

        // 子节点应拥有自己的指标
        assert_eq!(child.metrics().success(), 1);
        assert_eq!(parent.metrics().total_completed(), 1); // 父节点看到汇总
    }

    #[rstest]
    #[tokio::test]
    async fn test_hierarchical_metrics_aggregation(log_policy: Arc<LogOnlyPolicy>) {
        // 测试多个子 tracker 的指标会向父级聚合。
        let scheduler = Arc::new(SemaphoreScheduler::new(Arc::new(Semaphore::new(10))));
        let parent = TaskTracker::new(scheduler, log_policy.clone()).unwrap();

        // 以默认设置创建 child1
        let child1 = parent.child_tracker().unwrap();

        // 以自定义错误策略创建 child2
        let child_error_policy = CancelOnError::new();
        let child2 = parent
            .child_tracker_builder()
            .error_policy(child_error_policy)
            .build()
            .unwrap();

        // 同时测试自定义调度器与策略
        let another_scheduler = Arc::new(SemaphoreScheduler::new(Arc::new(Semaphore::new(3))));
        let another_error_policy = CancelOnError::new();
        let child3 = parent
            .child_tracker_builder()
            .scheduler(another_scheduler)
            .error_policy(another_error_policy)
            .build()
            .unwrap();

        // 测试所有子节点都被正确注册
        assert_eq!(parent.child_count(), 3);

        // 测试自定义调度器可用
        let handle1 = child1.spawn(async { Ok(1) });
        let handle2 = child2.spawn(async { Ok(2) });
        let handle3 = child3.spawn(async { Ok(3) });

        assert_eq!(handle1.await.unwrap().unwrap(), 1);
        assert_eq!(handle2.await.unwrap().unwrap(), 2);
        assert_eq!(handle3.await.unwrap().unwrap(), 3);

        // 验证指标仍然正常
        assert_eq!(parent.metrics().success(), 3); // 所有子节点的成功向上汇总
        assert_eq!(child1.metrics().success(), 1);
        assert_eq!(child2.metrics().success(), 1);
        assert_eq!(child3.metrics().success(), 1);
    }

    #[rstest]
    #[tokio::test]
    async fn test_scheduler_queue_depth_calculation(log_policy: Arc<LogOnlyPolicy>) {
        // 测试 active、queued、pending 等排队指标的计算。
        let scheduler = Arc::new(SemaphoreScheduler::new(Arc::new(Semaphore::new(2)))); // 仅允许 2 个并发任务
        let tracker = TaskTracker::new(scheduler, log_policy).unwrap();

        // 初始时无任务
        assert_eq!(tracker.metrics().issued(), 0);
        assert_eq!(tracker.metrics().active(), 0);
        assert_eq!(tracker.metrics().queued(), 0);
        assert_eq!(tracker.metrics().pending(), 0);

        // 使用通道控制任务何时完成
        let (complete_tx, _complete_rx) = tokio::sync::broadcast::channel(1);

        // 启动 2 个持有信号量 permit 的任务
        let handle1 = tracker.spawn({
            let mut rx = complete_tx.subscribe();
            async move {
                // 等待完成信号
                rx.recv().await.ok();
                Ok(1)
            }
        });
        let handle2 = tracker.spawn({
            let mut rx = complete_tx.subscribe();
            async move {
                // 等待完成信号
                rx.recv().await.ok();
                Ok(2)
            }
        });

        // 给任务时间启动并获取 permit
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // 应有 2 个活跃任务，0 个排队
        assert_eq!(tracker.metrics().issued(), 2);
        assert_eq!(tracker.metrics().active(), 2);
        assert_eq!(tracker.metrics().queued(), 0);
        assert_eq!(tracker.metrics().pending(), 2);

        // 启动第三个任务 — 由于信号量已满应被排队
        let handle3 = tracker.spawn(async move { Ok(3) });

        // 给任务时间进入排队
        tokio::task::yield_now().await;

        // 应有 2 个活跃，1 个排队
        assert_eq!(tracker.metrics().issued(), 3);
        assert_eq!(tracker.metrics().active(), 2);
        assert_eq!(
            tracker.metrics().queued(),
            tracker.metrics().pending() - tracker.metrics().active()
        );
        assert_eq!(tracker.metrics().pending(), 3);

        // 通过发送信号完成所有任务
        complete_tx.send(()).ok();

        let result1 = handle1.await.unwrap().unwrap();
        let result2 = handle2.await.unwrap().unwrap();
        let result3 = handle3.await.unwrap().unwrap();

        assert_eq!(result1, 1);
        assert_eq!(result2, 2);
        assert_eq!(result3, 3);

        // 所有任务应完成
        assert_eq!(tracker.metrics().success(), 3);
        assert_eq!(tracker.metrics().active(), 0);
        assert_eq!(tracker.metrics().queued(), 0);
        assert_eq!(tracker.metrics().pending(), 0);
    }

    #[rstest]
    #[tokio::test]
    async fn test_hierarchical_metrics_failure_aggregation(
        semaphore_scheduler: Arc<SemaphoreScheduler>,
        log_policy: Arc<LogOnlyPolicy>,
    ) {
        // 测试失败指标也会向父级聚合。
        let parent = TaskTracker::new(semaphore_scheduler, log_policy).unwrap();
        let child = parent.child_tracker().unwrap();

        // 运行一些成功与失败的任务
        let success_handle = child.spawn(async { Ok(42) });
        let failure_handle = child.spawn(async { Err::<(), _>(anyhow::anyhow!("test error")) });

        // 等待任务完成
        let _success_result = success_handle.await.unwrap().unwrap();
        let _failure_result = failure_handle.await.unwrap().unwrap_err();

        // 检查子节点指标
        assert_eq!(child.metrics().success(), 1, "Child should have 1 success");
        assert_eq!(child.metrics().failed(), 1, "Child should have 1 failure");

        // 父节点应看到汇总指标
        // 注：由于层级聚合，这些指标会向上传播
    }

    #[rstest]
    #[tokio::test]
    async fn test_metrics_independence_between_tracker_instances(
        semaphore_scheduler: Arc<SemaphoreScheduler>,
        log_policy: Arc<LogOnlyPolicy>,
    ) {
        // 测试不同 tracker 实例之间的指标互不影响。
        let tracker1 = TaskTracker::new(semaphore_scheduler.clone(), log_policy.clone()).unwrap();
        let tracker2 = TaskTracker::new(semaphore_scheduler, log_policy).unwrap();

        // 在两个 tracker 中运行任务
        let handle1 = tracker1.spawn(async { Ok(1) });
        let handle2 = tracker2.spawn(async { Ok(2) });

        handle1.await.unwrap().unwrap();
        handle2.await.unwrap().unwrap();

        // 每个 tracker 应只看到自身的指标
        assert_eq!(tracker1.metrics().success(), 1);
        assert_eq!(tracker2.metrics().success(), 1);
        assert_eq!(tracker1.metrics().total_completed(), 1);
        assert_eq!(tracker2.metrics().total_completed(), 1);
    }

    #[rstest]
    #[tokio::test]
    async fn test_hierarchical_join_waits_for_all(log_policy: Arc<LogOnlyPolicy>) {
        // 测试父级 join 会等待整棵层级树中的任务全部完成。
        let scheduler = Arc::new(SemaphoreScheduler::new(Arc::new(Semaphore::new(10))));
        let parent = TaskTracker::new(scheduler, log_policy).unwrap();
        let child1 = parent.child_tracker().unwrap();
        let child2 = parent.child_tracker().unwrap();
        let grandchild = child1.child_tracker().unwrap();

        // 验证父节点跟踪子节点
        assert_eq!(parent.child_count(), 2);
        assert_eq!(child1.child_count(), 1);
        assert_eq!(child2.child_count(), 0);
        assert_eq!(grandchild.child_count(), 0);

        // 跟踪完成顺序
        let completion_order = Arc::new(Mutex::new(Vec::new()));

        // 启动不同耗时的任务
        let order_clone = completion_order.clone();
        let parent_handle = parent.spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            order_clone.lock().unwrap().push("parent");
            Ok(())
        });

        let order_clone = completion_order.clone();
        let child1_handle = child1.spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            order_clone.lock().unwrap().push("child1");
            Ok(())
        });

        let order_clone = completion_order.clone();
        let child2_handle = child2.spawn(async move {
            tokio::time::sleep(Duration::from_millis(75)).await;
            order_clone.lock().unwrap().push("child2");
            Ok(())
        });

        let order_clone = completion_order.clone();
        let grandchild_handle = grandchild.spawn(async move {
            tokio::time::sleep(Duration::from_millis(125)).await;
            order_clone.lock().unwrap().push("grandchild");
            Ok(())
        });

        // 测试层级 join — 应等待层级中所有任务
        println!("[TEST] About to call parent.join()");
        let start = std::time::Instant::now();
        parent.join().await; // 应等待所有任务
        let elapsed = start.elapsed();
        println!("[TEST] parent.join() completed in {:?}", elapsed);

        // 应等待最长任务（grandchild 耗时 125ms）
        assert!(
            elapsed >= Duration::from_millis(120),
            "Hierarchical join should wait for longest task"
        );

        // 所有任务应完成
        assert!(parent_handle.is_finished());
        assert!(child1_handle.is_finished());
        assert!(child2_handle.is_finished());
        assert!(grandchild_handle.is_finished());

        // 验证所有任务已完成
        let final_order = completion_order.lock().unwrap();
        assert_eq!(final_order.len(), 4);
        assert!(final_order.contains(&"parent"));
        assert!(final_order.contains(&"child1"));
        assert!(final_order.contains(&"child2"));
        assert!(final_order.contains(&"grandchild"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_hierarchical_join_waits_for_children(
        semaphore_scheduler: Arc<SemaphoreScheduler>,
        log_policy: Arc<LogOnlyPolicy>,
    ) {
        // 测试 join 会等待子 tracker 中较慢的任务完成。
        let parent = TaskTracker::new(semaphore_scheduler, log_policy).unwrap();
        let child = parent.child_tracker().unwrap();

        // 启动一个快速的父任务和一个较慢的子任务
        let _parent_handle = parent.spawn(async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok(())
        });

        let _child_handle = child.spawn(async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(())
        });

        // 层级 join 应等待父与子任务
        let start = std::time::Instant::now();
        parent.join().await; // 应等待两者（默认层级化）
        let elapsed = start.elapsed();

        // 应等待较长的子任务（100ms）
        assert!(
            elapsed >= Duration::from_millis(90),
            "Hierarchical join should wait for all child tasks"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_hierarchical_join_operations(
        semaphore_scheduler: Arc<SemaphoreScheduler>,
        log_policy: Arc<LogOnlyPolicy>,
    ) {
        // 测试父级 join 会关闭整条子孙层级。
        let parent = TaskTracker::new(semaphore_scheduler, log_policy).unwrap();
        let child = parent.child_tracker().unwrap();
        let grandchild = child.child_tracker().unwrap();

        // 验证 tracker 初始为开启状态
        assert!(!parent.is_closed());
        assert!(!child.is_closed());
        assert!(!grandchild.is_closed());

        // join 父节点（默认层级化 — 关闭并等待所有）
        parent.join().await;

        // 所有节点应已关闭（由于 parent 已被移动，检查子 tracker）
        assert!(child.is_closed());
        assert!(grandchild.is_closed());
    }

    #[rstest]
    #[tokio::test]
    async fn test_unlimited_scheduler() {
        // 测试无限制调度器会立即执行任务。
        let scheduler = UnlimitedScheduler::new();
        let error_policy = LogOnlyPolicy::new();
        let tracker = TaskTracker::new(scheduler, error_policy).unwrap();

        let (tx, rx) = tokio::sync::oneshot::channel();
        let handle = tracker.spawn(async {
            rx.await.ok();
            Ok(42)
        });

        // 任务应可立即执行（无并发限制）
        tx.send(()).ok();
        let result = handle.await.unwrap().unwrap();
        assert_eq!(result, 42);

        assert_eq!(tracker.metrics().success(), 1);
    }

    #[rstest]
    #[tokio::test]
    async fn test_threshold_cancel_policy(semaphore_scheduler: Arc<SemaphoreScheduler>) {
        // 测试阈值策略的按任务失败计数行为。
        let error_policy = ThresholdCancelPolicy::with_threshold(2); // 每任务 2 次失败后取消
        let tracker = TaskTracker::new(semaphore_scheduler, error_policy.clone()).unwrap();
        let cancel_token = tracker.cancellation_token().child_token();

        // 采用每任务上下文后，单个任务的失败不会累加
        // 每个任务以 failure_count = 0 开始，因此单次失败不会触发取消
        let _handle1 = tracker.spawn(async { Err::<(), _>(anyhow::anyhow!("First failure")) });
        tokio::task::yield_now().await;
        assert!(!cancel_token.is_cancelled());
        assert_eq!(error_policy.failure_count(), 1); // 全局计数器仍递增

        // 来自另一任务的第二次失败 — 仍不会触发取消
        let _handle2 = tracker.spawn(async { Err::<(), _>(anyhow::anyhow!("Second failure")) });
        tokio::task::yield_now().await;
        assert!(!cancel_token.is_cancelled()); // 每任务上下文阻止了取消
        assert_eq!(error_policy.failure_count(), 2); // 全局计数器递增

        // 要触发取消，单个任务需要通过 continuation 多次失败
        // （这需要更复杂的测试设置）
    }

    #[tokio::test]
    async fn test_policy_constructors() {
        // 测试常用策略构造器的 API 形态。
        let _unlimited = UnlimitedScheduler::new();
        let _semaphore = SemaphoreScheduler::with_permits(5);
        let _log_only = LogOnlyPolicy::new();
        let _cancel_policy = CancelOnError::new();
        let _threshold_policy = ThresholdCancelPolicy::with_threshold(3);
        let _rate_policy = RateCancelPolicy::builder()
            .rate(0.5)
            .window_secs(60)
            .build();

        // 所有构造器直接返回 Arc — 不再需要繁琐的 ::new_arc 模式
        // 本测试确保简洁的 API 减少样板代码
    }

    #[rstest]
    #[tokio::test]
    async fn test_child_creation_fails_after_join(
        semaphore_scheduler: Arc<SemaphoreScheduler>,
        log_policy: Arc<LogOnlyPolicy>,
    ) {
        // 测试父 tracker 关闭后无法继续创建子级。
        let parent = TaskTracker::new(semaphore_scheduler, log_policy).unwrap();

        // 初始时创建子节点应可用
        let _child = parent.child_tracker().unwrap();

        // 关闭父 tracker
        let parent_clone = parent.clone();
        parent.join().await;
        assert!(parent_clone.is_closed());

        // 现在尝试创建子节点应失败
        let result = parent_clone.child_tracker();
        assert!(result.is_err());
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("closed parent tracker")
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_child_builder_fails_after_join(
        semaphore_scheduler: Arc<SemaphoreScheduler>,
        log_policy: Arc<LogOnlyPolicy>,
    ) {
        // 测试父 tracker 关闭后 builder 也无法构建子级。
        let parent = TaskTracker::new(semaphore_scheduler, log_policy).unwrap();

        // 初始时用 builder 创建子节点应可用
        let _child = parent.child_tracker_builder().build().unwrap();

        // 关闭父 tracker
        let parent_clone = parent.clone();
        parent.join().await;
        assert!(parent_clone.is_closed());

        // 现在用 builder 创建子节点应失败
        let result = parent_clone.child_tracker_builder().build();
        assert!(result.is_err());
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("closed parent tracker")
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_child_creation_succeeds_before_join(
        semaphore_scheduler: Arc<SemaphoreScheduler>,
        log_policy: Arc<LogOnlyPolicy>,
    ) {
        // 测试父 tracker 关闭前可以正常创建多个子级。
        let parent = TaskTracker::new(semaphore_scheduler, log_policy).unwrap();

        // 关闭前应能创建多个子节点
        let child1 = parent.child_tracker().unwrap();
        let child2 = parent.child_tracker_builder().build().unwrap();

        // 验证子节点可启动任务
        let handle1 = child1.spawn(async { Ok(42) });
        let handle2 = child2.spawn(async { Ok(24) });

        let result1 = handle1.await.unwrap().unwrap();
        let result2 = handle2.await.unwrap().unwrap();

        assert_eq!(result1, 42);
        assert_eq!(result2, 24);
        assert_eq!(parent.metrics().success(), 2); // 父节点看到所有成功
    }

    #[rstest]
    #[tokio::test]
    async fn test_custom_error_response_with_cancellation_token(
        semaphore_scheduler: Arc<SemaphoreScheduler>,
    ) {
        // 测试自定义错误动作能够触发外部取消 token。
        // 验证 ErrorResponse::Custom 配合 TriggerCancellationTokenOnError 的行为

        // 创建一个自定义取消 token
        let custom_cancel_token = CancellationToken::new();

        // 创建用于触发自定义 token 的策略
        let error_policy = TriggerCancellationTokenOnError::new(custom_cancel_token.clone());

        // 使用构造器搭配自定义策略创建 tracker
        let tracker = TaskTracker::builder()
            .scheduler(semaphore_scheduler)
            .error_policy(error_policy)
            .cancel_token(custom_cancel_token.clone())
            .build()
            .unwrap();

        let child = tracker.child_tracker().unwrap();

        // 初始时自定义 token 不应被取消
        assert!(!custom_cancel_token.is_cancelled());

        // 启动一个会失败的任务
        let handle = child.spawn(async {
            Err::<(), _>(anyhow::anyhow!("Test error to trigger custom response"))
        });

        // 等待任务完成（它会失败）
        let result = handle.await.unwrap();
        assert!(result.is_err());

        // 等待超时截止或取消 token 被触发
        // 预期任务失败后取消 token 被触发，命中超时则为失败
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                panic!("Task should have failed, but hit the deadline");
            }
            _ = custom_cancel_token.cancelled() => {
                // 任务应已失败，取消 token 应被触发
            }
        }

        // 自定义取消 token 现在应由我们的策略触发
        assert!(
            custom_cancel_token.is_cancelled(),
            "Custom cancellation token should be triggered by ErrorResponse::Custom"
        );

        assert!(tracker.cancellation_token().is_cancelled());
        assert!(child.cancellation_token().is_cancelled());

        // 验证错误已计数
        assert_eq!(tracker.metrics().failed(), 1);
    }

    #[test]
    fn test_action_result_variants() {
        // 测试 ActionResult 各变体的构造与匹配。

        // 测试 Fail 变体
        let fail_result = ActionResult::Fail;
        match fail_result {
            ActionResult::Fail => {} // 预期
            _ => panic!("Expected Fail variant"),
        }

        // 测试 Shutdown 变体
        let shutdown_result = ActionResult::Shutdown;
        match shutdown_result {
            ActionResult::Shutdown => {} // 预期
            _ => panic!("Expected Shutdown variant"),
        }

        // 测试带 Continuation 的 Continue 变体
        #[derive(Debug)]
        struct TestRestartable;

        #[async_trait]
        impl Continuation for TestRestartable {
            async fn execute(
                &self,
                _cancel_token: CancellationToken,
            ) -> TaskExecutionResult<Box<dyn std::any::Any + Send + 'static>> {
                TaskExecutionResult::Success(Box::new("test_result".to_string()))
            }
        }

        let test_restartable = Arc::new(TestRestartable);
        let continue_result = ActionResult::Continue {
            continuation: test_restartable,
        };

        match continue_result {
            ActionResult::Continue { continuation } => {
                // 验证我们拥有一个有效的 Continuation
                assert!(format!("{:?}", continuation).contains("TestRestartable"));
            }
            _ => panic!("Expected Continue variant"),
        }
    }

    #[test]
    fn test_continuation_error_creation() {
        // 测试 continuation 错误对象的创建与转换。

        // 创建一个用于测试的虚拟 continuation 任务
        #[derive(Debug)]
        struct DummyRestartable;

        #[async_trait]
        impl Continuation for DummyRestartable {
            async fn execute(
                &self,
                _cancel_token: CancellationToken,
            ) -> TaskExecutionResult<Box<dyn std::any::Any + Send + 'static>> {
                TaskExecutionResult::Success(Box::new("restarted_result".to_string()))
            }
        }

        let dummy_restartable = Arc::new(DummyRestartable);
        let source_error = anyhow::anyhow!("Original task failed");

        // 测试 FailedWithContinuation::new
        let continuation_error = FailedWithContinuation::new(source_error, dummy_restartable);

        // 验证错误能正确显示
        let error_string = format!("{}", continuation_error);
        assert!(error_string.contains("Task failed with continuation"));
        assert!(error_string.contains("Original task failed"));

        // 测试转换为 anyhow::Error
        let anyhow_error = anyhow::Error::new(continuation_error);
        assert!(
            anyhow_error
                .to_string()
                .contains("Task failed with continuation")
        );
    }

    #[test]
    fn test_continuation_error_ext_trait() {
        // 测试 continuation 错误扩展 trait 的提取能力。

        // 使用普通 anyhow::Error（不可重启）测试
        let regular_error = anyhow::anyhow!("Regular error");
        assert!(!regular_error.has_continuation());
        let extracted = regular_error.extract_continuation();
        assert!(extracted.is_none());

        // 使用 RestartableError 测试
        #[derive(Debug)]
        struct TestRestartable;

        #[async_trait]
        impl Continuation for TestRestartable {
            async fn execute(
                &self,
                _cancel_token: CancellationToken,
            ) -> TaskExecutionResult<Box<dyn std::any::Any + Send + 'static>> {
                TaskExecutionResult::Success(Box::new("test_result".to_string()))
            }
        }

        let test_restartable = Arc::new(TestRestartable);
        let source_error = anyhow::anyhow!("Source error");
        let continuation_error = FailedWithContinuation::new(source_error, test_restartable);

        let anyhow_error = anyhow::Error::new(continuation_error);
        assert!(anyhow_error.has_continuation());

        // 测试提取可重启任务
        let extracted = anyhow_error.extract_continuation();
        assert!(extracted.is_some());
    }

    #[test]
    fn test_continuation_error_into_anyhow_helper() {
        // 测试 continuation 错误转换为 anyhow 的辅助路径。

        // 目前用简单类型测试类型擦除概念
        struct MockExecutor;

        let _source_error = anyhow::anyhow!("Mock task failed");

        // 目前还无法测试 FailedWithContinuation::into_anyhow，因为它需要
        // 真实的 TaskExecutor<T>。这将在第三阶段测试。
        // 目前仅通过手动构造验证概念可行。

        #[derive(Debug)]
        struct MockRestartable;

        #[async_trait]
        impl Continuation for MockRestartable {
            async fn execute(
                &self,
                _cancel_token: CancellationToken,
            ) -> TaskExecutionResult<Box<dyn std::any::Any + Send + 'static>> {
                TaskExecutionResult::Success(Box::new("mock_result".to_string()))
            }
        }

        let mock_restartable = Arc::new(MockRestartable);
        let continuation_error =
            FailedWithContinuation::new(anyhow::anyhow!("Mock task failed"), mock_restartable);

        let anyhow_error = anyhow::Error::new(continuation_error);
        assert!(anyhow_error.has_continuation());
    }

    #[test]
    fn test_continuation_error_with_task_executor() {
        // 测试 continuation 错误与任务执行器协作时的基本行为。

        #[derive(Debug)]
        struct TestRestartableTask;

        #[async_trait]
        impl Continuation for TestRestartableTask {
            async fn execute(
                &self,
                _cancel_token: CancellationToken,
            ) -> TaskExecutionResult<Box<dyn std::any::Any + Send + 'static>> {
                TaskExecutionResult::Success(Box::new("test_result".to_string()))
            }
        }

        let restartable_task = Arc::new(TestRestartableTask);
        let source_error = anyhow::anyhow!("Task failed");

        // 测试 FailedWithContinuation::new 与 Restartable
        let continuation_error = FailedWithContinuation::new(source_error, restartable_task);

        // 验证错误能正确显示
        let error_string = format!("{}", continuation_error);
        assert!(error_string.contains("Task failed with continuation"));
        assert!(error_string.contains("Task failed"));

        // 测试转换为 anyhow::Error
        let anyhow_error = anyhow::Error::new(continuation_error);
        assert!(anyhow_error.has_continuation());

        // 测试提取（现在应能通过 Restartable trait 成功）
        let extracted = anyhow_error.extract_continuation();
        assert!(extracted.is_some()); // 应成功提取 Restartable
    }

    #[test]
    fn test_continuation_error_into_anyhow_convenience() {
        // 测试 continuation 错误的便捷构造方法。

        #[derive(Debug)]
        struct ConvenienceRestartable;

        #[async_trait]
        impl Continuation for ConvenienceRestartable {
            async fn execute(
                &self,
                _cancel_token: CancellationToken,
            ) -> TaskExecutionResult<Box<dyn std::any::Any + Send + 'static>> {
                TaskExecutionResult::Success(Box::new(42u32))
            }
        }

        let restartable_task = Arc::new(ConvenienceRestartable);
        let source_error = anyhow::anyhow!("Computation failed");

        // 测试 FailedWithContinuation::into_anyhow 便捷方法
        let anyhow_error = FailedWithContinuation::into_anyhow(source_error, restartable_task);

        assert!(anyhow_error.has_continuation());
        assert!(
            anyhow_error
                .to_string()
                .contains("Task failed with continuation")
        );
        assert!(anyhow_error.to_string().contains("Computation failed"));
    }

    #[test]
    fn test_handle_task_error_with_continuation_error() {
        // 测试 handle_task_error 对 continuation 错误的识别。

        // 创建一个虚拟 Restartable 任务
        #[derive(Debug)]
        struct MockRestartableTask;

        #[async_trait]
        impl Continuation for MockRestartableTask {
            async fn execute(
                &self,
                _cancel_token: CancellationToken,
            ) -> TaskExecutionResult<Box<dyn std::any::Any + Send + 'static>> {
                TaskExecutionResult::Success(Box::new("retry_result".to_string()))
            }
        }

        let restartable_task = Arc::new(MockRestartableTask);

        // 创建 RestartableError
        let source_error = anyhow::anyhow!("Task failed, but can retry");
        let continuation_error = FailedWithContinuation::new(source_error, restartable_task);
        let anyhow_error = anyhow::Error::new(continuation_error);

        // 验证其被识别为可重启
        assert!(anyhow_error.has_continuation());

        // 验证可以向下转换为 FailedWithContinuation
        let continuation_ref = anyhow_error.downcast_ref::<FailedWithContinuation>();
        assert!(continuation_ref.is_some());

        // 验证 continuation 任务存在
        let continuation = continuation_ref.unwrap();
        // 注：可通过检查 Arc::strong_count > 0 验证 Arc 有效
        assert!(Arc::strong_count(&continuation.continuation) > 0);
    }

    #[test]
    fn test_handle_task_error_with_regular_error() {
        // 测试 handle_task_error 对普通错误的处理分支。

        let regular_error = anyhow::anyhow!("Regular task failure");

        // 验证其未被识别为可重启
        assert!(!regular_error.has_continuation());

        // 验证无法向下转换为 FailedWithContinuation
        let continuation_ref = regular_error.downcast_ref::<FailedWithContinuation>();
        assert!(continuation_ref.is_none());
    }

    // ========================================
    // 端到端 ACTIONRESULT 测试
    // ========================================

    #[rstest]
    #[tokio::test]
    async fn test_end_to_end_continuation_execution(
        unlimited_scheduler: Arc<UnlimitedScheduler>,
        log_policy: Arc<LogOnlyPolicy>,
    ) {
        // 测试 continuation 的端到端执行成功路径。
        let tracker = TaskTracker::new(unlimited_scheduler, log_policy).unwrap();

        // 用于跟踪执行的共享状态
        let execution_log = Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));
        let log_clone = execution_log.clone();

        // 创建一个记录自身执行的 continuation
        #[derive(Debug)]
        struct LoggingContinuation {
            log: Arc<tokio::sync::Mutex<Vec<String>>>,
            result: String,
        }

        #[async_trait]
        impl Continuation for LoggingContinuation {
            async fn execute(
                &self,
                _cancel_token: CancellationToken,
            ) -> TaskExecutionResult<Box<dyn std::any::Any + Send + 'static>> {
                self.log
                    .lock()
                    .await
                    .push("continuation_executed".to_string());
                TaskExecutionResult::Success(Box::new(self.result.clone()))
            }
        }

        let continuation = Arc::new(LoggingContinuation {
            log: log_clone,
            result: "continuation_result".to_string(),
        });

        // 使用 continuation 失败的任务
        let log_for_task = execution_log.clone();
        let handle = tracker.spawn(async move {
            log_for_task
                .lock()
                .await
                .push("original_task_executed".to_string());

            // 返回 FailedWithContinuation
            let error = anyhow::anyhow!("Original task failed");
            let result: Result<String, anyhow::Error> =
                Err(FailedWithContinuation::into_anyhow(error, continuation));
            result
        });

        // 执行并验证 continuation 被调用
        let result = handle.await.expect("Task should complete");
        assert!(result.is_ok(), "Continuation should succeed");

        // 验证执行顺序
        let log = execution_log.lock().await;
        assert_eq!(log.len(), 2);
        assert_eq!(log[0], "original_task_executed");
        assert_eq!(log[1], "continuation_executed");

        // 验证指标——应显示 1 次成功（来自 continuation）
        assert_eq!(tracker.metrics().success(), 1);
        assert_eq!(tracker.metrics().failed(), 0); // continuation 成功
        assert_eq!(tracker.metrics().cancelled(), 0);
    }

    #[rstest]
    #[tokio::test]
    async fn test_end_to_end_multiple_continuations(
        unlimited_scheduler: Arc<UnlimitedScheduler>,
        log_policy: Arc<LogOnlyPolicy>,
    ) {
        // 测试多个 continuation 串联重试的成功路径。
        let tracker = TaskTracker::new(unlimited_scheduler, log_policy).unwrap();

        let execution_log = Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));
        let attempt_count = Arc::new(std::sync::atomic::AtomicU32::new(0));

        // 先失败两次、随后成功的 continuation
        #[derive(Debug)]
        struct RetryingContinuation {
            log: Arc<tokio::sync::Mutex<Vec<String>>>,
            attempt_count: Arc<std::sync::atomic::AtomicU32>,
        }

        #[async_trait]
        impl Continuation for RetryingContinuation {
            async fn execute(
                &self,
                _cancel_token: CancellationToken,
            ) -> TaskExecutionResult<Box<dyn std::any::Any + Send + 'static>> {
                let attempt = self
                    .attempt_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    + 1;
                self.log
                    .lock()
                    .await
                    .push(format!("continuation_attempt_{}", attempt));

                if attempt < 3 {
                    // 以另一个 continuation 失败
                    let next_continuation = Arc::new(RetryingContinuation {
                        log: self.log.clone(),
                        attempt_count: self.attempt_count.clone(),
                    });
                    let error = anyhow::anyhow!("Continuation attempt {} failed", attempt);
                    TaskExecutionResult::Error(FailedWithContinuation::into_anyhow(
                        error,
                        next_continuation,
                    ))
                } else {
                    // 第三次尝试成功
                    TaskExecutionResult::Success(Box::new(format!(
                        "success_on_attempt_{}",
                        attempt
                    )))
                }
            }
        }

        let initial_continuation = Arc::new(RetryingContinuation {
            log: execution_log.clone(),
            attempt_count: attempt_count.clone(),
        });

        // 立即以 continuation 失败的任务
        let handle = tracker.spawn(async move {
            let error = anyhow::anyhow!("Original task failed");
            let result: Result<String, anyhow::Error> = Err(FailedWithContinuation::into_anyhow(
                error,
                initial_continuation,
            ));
            result
        });

        // 执行并验证多个 continuation
        let result = handle.await.expect("Task should complete");
        assert!(result.is_ok(), "Final continuation should succeed");

        // 验证所有尝试都已执行
        let log = execution_log.lock().await;
        assert_eq!(log.len(), 3);
        assert_eq!(log[0], "continuation_attempt_1");
        assert_eq!(log[1], "continuation_attempt_2");
        assert_eq!(log[2], "continuation_attempt_3");

        // 验证最终尝试次数
        assert_eq!(attempt_count.load(std::sync::atomic::Ordering::Relaxed), 3);

        // 验证指标——应显示 1 次成功（最终 continuation）
        assert_eq!(tracker.metrics().success(), 1);
        assert_eq!(tracker.metrics().failed(), 0);
    }

    #[rstest]
    #[tokio::test]
    async fn test_end_to_end_continuation_failure(
        unlimited_scheduler: Arc<UnlimitedScheduler>,
        log_policy: Arc<LogOnlyPolicy>,
    ) {
        // 测试 continuation 最终失败时的端到端路径。
        let tracker = TaskTracker::new(unlimited_scheduler, log_policy).unwrap();

        let execution_log = Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));
        let log_clone = execution_log.clone();

        // 失败且不提供另一个 continuation 的 continuation
        #[derive(Debug)]
        struct FailingContinuation {
            log: Arc<tokio::sync::Mutex<Vec<String>>>,
        }

        #[async_trait]
        impl Continuation for FailingContinuation {
            async fn execute(
                &self,
                _cancel_token: CancellationToken,
            ) -> TaskExecutionResult<Box<dyn std::any::Any + Send + 'static>> {
                self.log
                    .lock()
                    .await
                    .push("continuation_failed".to_string());
                TaskExecutionResult::Error(anyhow::anyhow!("Continuation failed permanently"))
            }
        }

        let continuation = Arc::new(FailingContinuation { log: log_clone });

        // 使用 continuation 失败的任务
        let log_for_task = execution_log.clone();
        let handle = tracker.spawn(async move {
            log_for_task
                .lock()
                .await
                .push("original_task_executed".to_string());

            let error = anyhow::anyhow!("Original task failed");
            let result: Result<String, anyhow::Error> =
                Err(FailedWithContinuation::into_anyhow(error, continuation));
            result
        });

        // 执行并验证 continuation 失败
        let result = handle.await.expect("Task should complete");
        assert!(result.is_err(), "Continuation should fail");

        // 验证执行顺序
        let log = execution_log.lock().await;
        assert_eq!(log.len(), 2);
        assert_eq!(log[0], "original_task_executed");
        assert_eq!(log[1], "continuation_failed");

        // 验证指标——应显示 1 次失败（来自 continuation）
        assert_eq!(tracker.metrics().success(), 0);
        assert_eq!(tracker.metrics().failed(), 1);
        assert_eq!(tracker.metrics().cancelled(), 0);
    }

    #[rstest]
    #[tokio::test]
    async fn test_end_to_end_all_action_result_variants(
        unlimited_scheduler: Arc<UnlimitedScheduler>,
    ) {
        // 测试 Fail、Shutdown、Continue 三种动作结果的端到端表现。

        // 测试 1：ActionResult::Fail（通过 LogOnlyPolicy）
        {
            let tracker =
                TaskTracker::new(unlimited_scheduler.clone(), LogOnlyPolicy::new()).unwrap();
            let handle = tracker.spawn(async {
                let result: Result<String, anyhow::Error> = Err(anyhow::anyhow!("Test error"));
                result
            });
            let result = handle.await.expect("Task should complete");
            assert!(result.is_err(), "LogOnly should let error through");
            assert_eq!(tracker.metrics().failed(), 1);
        }

        // 测试 2：ActionResult::Shutdown（通过 CancelOnError）
        {
            let tracker =
                TaskTracker::new(unlimited_scheduler.clone(), CancelOnError::new()).unwrap();
            let handle = tracker.spawn(async {
                let result: Result<String, anyhow::Error> = Err(anyhow::anyhow!("Test error"));
                result
            });
            let result = handle.await.expect("Task should complete");
            assert!(result.is_err(), "CancelOnError should fail task");
            assert!(
                tracker.cancellation_token().is_cancelled(),
                "Should cancel tracker"
            );
            assert_eq!(tracker.metrics().failed(), 1);
        }

        // 测试 3：ActionResult::Continue（通过 FailedWithContinuation）
        {
            let tracker =
                TaskTracker::new(unlimited_scheduler.clone(), LogOnlyPolicy::new()).unwrap();

            #[derive(Debug)]
            struct TestContinuation;

            #[async_trait]
            impl Continuation for TestContinuation {
                async fn execute(
                    &self,
                    _cancel_token: CancellationToken,
                ) -> TaskExecutionResult<Box<dyn std::any::Any + Send + 'static>> {
                    TaskExecutionResult::Success(Box::new("continuation_success".to_string()))
                }
            }

            let continuation = Arc::new(TestContinuation);
            let handle = tracker.spawn(async move {
                let error = anyhow::anyhow!("Original failure");
                let result: Result<String, anyhow::Error> =
                    Err(FailedWithContinuation::into_anyhow(error, continuation));
                result
            });

            let result = handle.await.expect("Task should complete");
            assert!(result.is_ok(), "Continuation should succeed");
            assert_eq!(tracker.metrics().success(), 1);
            assert_eq!(tracker.metrics().failed(), 0);
        }
    }

    // ========================================
    // 循环行为与策略交互测试
    // ========================================
    //
    // 这些测试展示当前的 ActionResult 体系，并指出
    // 未来可改进的方向：
    //
    // ✅ 已生效的能力：
    // - 所有 ActionResult 变体（Continue、Cancel、ExecuteNext）均被测试
    // - 任务驱动的 continuation 正常工作
    // - 策略驱动的 continuation 正常工作
    // - 混合 continuation 来源正常工作
    // - 带资源管理的循环行为正常工作
    //
    // 🔄 当前的局限：
    // - ThresholdCancelPolicy 全局跟踪失败，而非按任务跟踪
    // - OnErrorPolicy 不接收 attempt_count 参数
    // - 有状态重试策略缺乏按任务的上下文
    //
    // 🚀 已识别的未来改进：
    // - 为 OnErrorPolicy 增加 OnErrorContext 关联类型以保存按任务状态
    // - 向 OnErrorPolicy::on_error 传递 attempt_count
    // - 启用按任务的失败跟踪、退避计时器等
    //
    // 下面的测试同时展示了当前能力与局限。

    /// 测试不同策略与 continuation 次数下的重试循环行为
    ///
    /// 本测试验证：
    /// 1. 任务能够按顺序提供多个 continuation
    /// 2. 不同错误策略可限制 continuation 尝试次数
    /// 3. 重试循环能正确处理策略关于何时停止的决定
    ///
    /// 关键要点：策略仅对普通错误起作用，而非 FailedWithContinuation。
    /// 因此需要最终以普通错误失败的 continuation 来测试策略限制。
    ///
    /// 设计局限：当前 ThresholdCancelPolicy 在所有任务间全局跟踪失败，
    /// 而非按任务跟踪。本测试展示当前行为，但对重试循环测试并不理想。
    ///
    /// 未来改进：为 OnErrorPolicy 增加 OnErrorContext 关联类型：
    /// ```rust
    /// trait OnErrorPolicy {
    ///     type Context: Default + Send + Sync;
    ///     fn on_error(&self, error: &anyhow::Error, task_id: TaskId,
    ///                 attempt_count: u32, context: &mut Self::Context) -> ErrorResponse;
    /// }
    /// ```
    /// 这将启用按任务的失败跟踪、退避计时器等。
    ///
    /// 注：每个测试用例使用全新的策略实例，以避免全局状态干扰。
    #[rstest]
    #[case(
        1,
        false,
        "Global policy with max_failures=1 should stop after first regular error"
    )]
    #[case(
        2,
        false,  // 实际上会失败 — ActionResult::Fail 接受该错误并使任务失败
        "Global policy with max_failures=2 allows error but ActionResult::Fail still fails the task"
    )]
    #[tokio::test]
    async fn test_continuation_loop_with_global_threshold_policy(
        unlimited_scheduler: Arc<UnlimitedScheduler>,
        #[case] max_failures: usize,
        #[case] should_succeed: bool,
        #[case] description: &str,
    ) {
        // 测试全局阈值策略与 continuation 循环的交互。

        let execution_log = Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));
        let attempt_counter = Arc::new(std::sync::atomic::AtomicU32::new(0));

        // 创建一个以普通错误（非 FailedWithContinuation）失败的 continuation，
        // 这样策略就会被咨询并可能停止重试
        #[derive(Debug)]
        struct PolicyTestContinuation {
            log: Arc<tokio::sync::Mutex<Vec<String>>>,
            attempt_counter: Arc<std::sync::atomic::AtomicU32>,
            max_attempts_before_success: u32,
        }

        #[async_trait]
        impl Continuation for PolicyTestContinuation {
            async fn execute(
                &self,
                _cancel_token: CancellationToken,
            ) -> TaskExecutionResult<Box<dyn std::any::Any + Send + 'static>> {
                let attempt = self
                    .attempt_counter
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    + 1;
                self.log
                    .lock()
                    .await
                    .push(format!("continuation_attempt_{}", attempt));

                if attempt < self.max_attempts_before_success {
                    // 以普通错误失败——策略会看到这个错误
                    TaskExecutionResult::Error(anyhow::anyhow!(
                        "Continuation attempt {} failed (regular error)",
                        attempt
                    ))
                } else {
                    // 足够次数后成功
                    TaskExecutionResult::Success(Box::new(format!(
                        "success_on_attempt_{}",
                        attempt
                    )))
                }
            }
        }

        // 每个测试用例创建全新策略实例，以避免全局状态干扰
        let policy = ThresholdCancelPolicy::with_threshold(max_failures);
        let tracker = TaskTracker::new(unlimited_scheduler, policy).unwrap();

        // 以 continuation 失败的原始任务
        let log_for_task = execution_log.clone();
        // 设置 max_attempts_before_success 使得：
        // - max_failures=1：continuation 失败 1 次（第 1 次尝试），策略在 1 次失败后取消
        // - max_failures=2：continuation 失败 1 次（第 1 次尝试），第 2 次成功
        let continuation = Arc::new(PolicyTestContinuation {
            log: execution_log.clone(),
            attempt_counter: attempt_counter.clone(),
            max_attempts_before_success: 2, // 总是在第 1 次失败，第 2 次成功
        });

        let handle = tracker.spawn(async move {
            log_for_task
                .lock()
                .await
                .push("original_task_executed".to_string());
            let error = anyhow::anyhow!("Original task failed");
            let result: Result<String, anyhow::Error> =
                Err(FailedWithContinuation::into_anyhow(error, continuation));
            result
        });

        // 根据策略执行并检查结果
        let result = handle.await.expect("Task should complete");

        // 调试：打印实际结果
        let log = execution_log.lock().await;
        let final_attempt_count = attempt_counter.load(std::sync::atomic::Ordering::Relaxed);
        println!(
            "Test case: max_failures={}, should_succeed={}",
            max_failures, should_succeed
        );
        println!("Result: {:?}", result.is_ok());
        println!("Log entries: {:?}", log);
        println!("Attempt count: {}", final_attempt_count);
        println!(
            "Metrics: success={}, failed={}",
            tracker.metrics().success(),
            tracker.metrics().failed()
        );
        drop(log); // 释放锁

        // 两个测试用例都应失败，因为 ActionResult::Fail 接受错误并使任务失败
        assert!(result.is_err(), "{}: Task should fail", description);
        assert_eq!(
            tracker.metrics().success(),
            0,
            "{}: Should have 0 successes",
            description
        );
        assert_eq!(
            tracker.metrics().failed(),
            1,
            "{}: Should have 1 failure",
            description
        );

        // 应在 1 次 continuation 尝试后停止，因为 ActionResult::Fail 使任务失败
        let log = execution_log.lock().await;
        assert_eq!(
            log.len(),
            2,
            "{}: Should have 2 log entries (original + 1 continuation attempt)",
            description
        );
        assert_eq!(log[0], "original_task_executed");
        assert_eq!(log[1], "continuation_attempt_1");

        assert_eq!(
            attempt_counter.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "{}: Should have made 1 continuation attempt",
            description
        );

        // 关键区别在于 tracker 是否被取消
        if max_failures == 1 {
            assert!(
                tracker.cancellation_token().is_cancelled(),
                "Tracker should be cancelled with max_failures=1"
            );
        } else {
            assert!(
                !tracker.cancellation_token().is_cancelled(),
                "Tracker should NOT be cancelled with max_failures=2 (policy allows the error)"
            );
        }
    }

    /// 用于理解 ThresholdCancelPolicy 在按任务上下文下行为的简单测试
    #[rstest]
    #[tokio::test]
    async fn test_simple_threshold_policy_behavior(unlimited_scheduler: Arc<UnlimitedScheduler>) {
        // 测试阈值策略在不同任务上的基础行为。
        // 使用 max_failures=2 测试——现在采用按任务的失败计数
        let policy = ThresholdCancelPolicy::with_threshold(2);
        let tracker = TaskTracker::new(unlimited_scheduler, policy.clone()).unwrap();

        // 任务 1：应失败但不触发取消（按任务失败计数 = 1）
        let handle1 = tracker.spawn(async {
            let result: Result<String, anyhow::Error> = Err(anyhow::anyhow!("First failure"));
            result
        });
        let result1 = handle1.await.expect("Task should complete");
        assert!(result1.is_err(), "First task should fail");
        assert!(
            !tracker.cancellation_token().is_cancelled(),
            "Should not be cancelled after 1 failure"
        );

        // 任务 2：应失败但不触发取消（不同任务，按任务失败计数 = 1）
        let handle2 = tracker.spawn(async {
            let result: Result<String, anyhow::Error> = Err(anyhow::anyhow!("Second failure"));
            result
        });
        let result2 = handle2.await.expect("Task should complete");
        assert!(result2.is_err(), "Second task should fail");
        assert!(
            !tracker.cancellation_token().is_cancelled(),
            "Should NOT be cancelled - per-task context prevents global accumulation"
        );

        println!("Policy global failure count: {}", policy.failure_count());
        assert_eq!(
            policy.failure_count(),
            2,
            "Policy should have counted 2 failures globally (for backwards compatibility)"
        );
    }

    /// 展示按任务错误上下文如何解决全局失败跟踪问题的测试
    ///
    /// 本测试表明借助 OnErrorContext，每个任务拥有独立的失败跟踪。
    #[rstest]
    #[tokio::test]
    async fn test_per_task_context_limitation_demo(unlimited_scheduler: Arc<UnlimitedScheduler>) {
        // 测试按任务上下文隔离失败预算的效果。
        // 创建一个每任务允许 2 次失败的策略
        let policy = ThresholdCancelPolicy::with_threshold(2);
        let tracker = TaskTracker::new(unlimited_scheduler, policy.clone()).unwrap();

        // 任务 1：失败一次（按任务失败计数 = 1，低于阈值）
        let handle1 = tracker.spawn(async {
            let result: Result<String, anyhow::Error> = Err(anyhow::anyhow!("Task 1 failure"));
            result
        });
        let result1 = handle1.await.expect("Task should complete");
        assert!(result1.is_err(), "Task 1 should fail");

        // 任务 2：也失败一次（按任务失败计数 = 1，低于阈值）
        // 有了按任务上下文，这不会干扰任务 1 的失败预算
        let handle2 = tracker.spawn(async {
            let result: Result<String, anyhow::Error> = Err(anyhow::anyhow!("Task 2 failure"));
            result
        });
        let result2 = handle2.await.expect("Task should complete");
        assert!(result2.is_err(), "Task 2 should fail");

        // 有了按任务上下文，tracker 不应被取消
        // 每个任务仅失败一次，低于阈值 2
        assert!(
            !tracker.cancellation_token().is_cancelled(),
            "Tracker should NOT be cancelled - per-task context prevents premature cancellation"
        );

        println!("Global failure count: {}", policy.failure_count());
        assert_eq!(
            policy.failure_count(),
            2,
            "Global policy counted 2 failures across different tasks"
        );

        // 这展示了局限：无法测试按任务的重试行为，
        // 因为不同任务的失败会彼此影响重试预算
    }

    /// 测试基于尝试次数逻辑的 allow_continuation() 策略方法
    ///
    /// 本测试验证：
    /// 1. 策略可根据上下文有条件地允许/拒绝 continuation
    /// 2. 当 allow_continuation() 返回 false 时，FailedWithContinuation 被忽略
    /// 3. 当 allow_continuation() 返回 true 时，FailedWithContinuation 正常处理
    /// 4. 策略的决定优先于任务提供的 continuation
    #[rstest]
    #[case(
        3,
        true,
        "Policy allows continuations up to 3 attempts - should succeed"
    )]
    #[case(
        2,
        true,
        "Policy allows continuations up to 2 attempts - should succeed"
    )]
    #[case(0, false, "Policy allows 0 attempts - should fail immediately")]
    #[tokio::test]
    async fn test_allow_continuation_policy_control(
        unlimited_scheduler: Arc<UnlimitedScheduler>,
        #[case] max_attempts: u32,
        #[case] should_succeed: bool,
        #[case] description: &str,
    ) {
        // 测试错误策略对 continuation 放行与拒绝的控制。
        // 仅允许直到 max_attempts 之前的 continuation 的策略
        #[derive(Debug)]
        struct AttemptLimitPolicy {
            max_attempts: u32,
        }

        impl OnErrorPolicy for AttemptLimitPolicy {
            fn create_child(&self) -> Arc<dyn OnErrorPolicy> {
                Arc::new(AttemptLimitPolicy {
                    max_attempts: self.max_attempts,
                })
            }

            fn create_context(&self) -> Option<Box<dyn std::any::Any + Send + 'static>> {
                None // 无状态策略
            }

            fn allow_continuation(&self, _error: &anyhow::Error, context: &OnErrorContext) -> bool {
                context.attempt_count <= self.max_attempts
            }

            fn on_error(
                &self,
                _error: &anyhow::Error,
                _context: &mut OnErrorContext,
            ) -> ErrorResponse {
                ErrorResponse::Fail // 不允许 continuation 时直接失败
            }
        }

        let policy = Arc::new(AttemptLimitPolicy { max_attempts });
        let tracker = TaskTracker::new(unlimited_scheduler, policy).unwrap();
        let execution_log = Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));

        // 总是尝试重试的 continuation
        #[derive(Debug)]
        struct AlwaysRetryContinuation {
            log: Arc<tokio::sync::Mutex<Vec<String>>>,
            attempt: u32,
        }

        #[async_trait]
        impl Continuation for AlwaysRetryContinuation {
            async fn execute(
                &self,
                _cancel_token: CancellationToken,
            ) -> TaskExecutionResult<Box<dyn std::any::Any + Send + 'static>> {
                self.log
                    .lock()
                    .await
                    .push(format!("continuation_attempt_{}", self.attempt));

                if self.attempt >= 2 {
                    // 2 次尝试后成功
                    TaskExecutionResult::Success(Box::new("final_success".to_string()))
                } else {
                    // 尝试以另一个 continuation 继续
                    let next_continuation = Arc::new(AlwaysRetryContinuation {
                        log: self.log.clone(),
                        attempt: self.attempt + 1,
                    });
                    let error = anyhow::anyhow!("Continuation attempt {} failed", self.attempt);
                    TaskExecutionResult::Error(FailedWithContinuation::into_anyhow(
                        error,
                        next_continuation,
                    ))
                }
            }
        }

        // 立即以 continuation 失败的任务
        let initial_continuation = Arc::new(AlwaysRetryContinuation {
            log: execution_log.clone(),
            attempt: 1,
        });

        let log_for_task = execution_log.clone();
        let handle = tracker.spawn(async move {
            log_for_task
                .lock()
                .await
                .push("initial_task_failure".to_string());
            let error = anyhow::anyhow!("Initial task failure");
            let result: Result<String, anyhow::Error> = Err(FailedWithContinuation::into_anyhow(
                error,
                initial_continuation,
            ));
            result
        });

        let result = handle.await.expect("Task should complete");

        if should_succeed {
            assert!(result.is_ok(), "{}: Task should succeed", description);
            assert_eq!(
                tracker.metrics().success(),
                1,
                "{}: Should have 1 success",
                description
            );

            // 应已执行多个 continuation
            let log = execution_log.lock().await;
            assert!(
                log.len() > 2,
                "{}: Should have multiple log entries",
                description
            );
            assert!(log.contains(&"continuation_attempt_1".to_string()));
        } else {
            assert!(result.is_err(), "{}: Task should fail", description);
            assert_eq!(
                tracker.metrics().failed(),
                1,
                "{}: Should have 1 failure",
                description
            );

            // 应因策略拒绝而提前停止
            let log = execution_log.lock().await;
            assert_eq!(
                log.len(),
                1,
                "{}: Should only have initial task entry",
                description
            );
            assert_eq!(log[0], "initial_task_failure");
            // 不应包含 continuation 尝试，因为策略拒绝了它们
            assert!(
                !log.iter()
                    .any(|entry| entry.contains("continuation_attempt")),
                "{}: Should not have continuation attempts, but got: {:?}",
                description,
                *log
            );
        }
    }

    /// 测试 TaskHandle 功能
    ///
    /// 本测试验证：
    /// 1. TaskHandle 可像 JoinHandle 一样被 await
    /// 2. TaskHandle 提供对任务取消 token 的访问
    /// 3. 单个任务取消正常工作
    /// 4. TaskHandle 方法（abort、is_finished）按预期工作
    #[tokio::test]
    async fn test_task_handle_functionality() {
        // 测试 TaskHandle 的等待、取消、终止与状态方法。
        let tracker = TaskTracker::new(UnlimitedScheduler::new(), LogOnlyPolicy::new()).unwrap();

        // 测试基本功能——TaskHandle 可被 await
        let handle1 = tracker.spawn(async {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            Ok("completed".to_string())
        });

        // 验证可以访问取消 token
        let cancel_token = handle1.cancellation_token();
        assert!(
            !cancel_token.is_cancelled(),
            "Token should not be cancelled initially"
        );

        // 等待任务
        let result1 = handle1.await.expect("Task should complete");
        assert!(result1.is_ok(), "Task should succeed");
        assert_eq!(result1.unwrap(), "completed");

        // 测试单个任务取消
        let handle2 = tracker.spawn_cancellable(|cancel_token| async move {
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    CancellableTaskResult::Ok("task_was_not_cancelled".to_string())
                },
                _ = cancel_token.cancelled() => {
                    CancellableTaskResult::Cancelled
                },

            }
        });

        let cancel_token2 = handle2.cancellation_token();

        // 取消这个特定任务
        cancel_token2.cancel();

        // 任务应被取消
        let result2 = handle2.await.expect("Task should complete");
        assert!(result2.is_err(), "Task should be cancelled");
        assert!(
            result2.unwrap_err().is_cancellation(),
            "Should be a cancellation error"
        );

        // 测试其他任务不受影响
        let handle3 = tracker.spawn(async { Ok("not_cancelled".to_string()) });

        let result3 = handle3.await.expect("Task should complete");
        assert!(result3.is_ok(), "Other tasks should not be affected");
        assert_eq!(result3.unwrap(), "not_cancelled");

        // 测试 abort 功能
        let handle4 = tracker.spawn(async {
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            Ok("should_be_aborted".to_string())
        });

        // 在 abort 前检查 is_finished
        assert!(!handle4.is_finished(), "Task should not be finished yet");

        // 终止任务
        handle4.abort();

        // 任务应被终止（JoinError）
        let result4 = handle4.await;
        assert!(result4.is_err(), "Aborted task should return JoinError");

        // 验证指标
        assert_eq!(
            tracker.metrics().success(),
            2,
            "Should have 2 successful tasks"
        );
        assert_eq!(
            tracker.metrics().cancelled(),
            1,
            "Should have 1 cancelled task"
        );
        // 注：被 abort 的任务在指标中不计为已取消
    }

    /// 测试 TaskHandle 与可取消任务的配合
    #[tokio::test]
    async fn test_task_handle_with_cancellable_tasks() {
        // 测试 TaskHandle 与可取消任务的配合。
        let tracker = TaskTracker::new(UnlimitedScheduler::new(), LogOnlyPolicy::new()).unwrap();

        // 测试带 TaskHandle 的可取消任务
        let handle = tracker.spawn_cancellable(|cancel_token| async move {
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                    CancellableTaskResult::Ok("completed".to_string())
                },
                _ = cancel_token.cancelled() => CancellableTaskResult::Cancelled,
            }
        });

        // 验证可以访问任务的独立取消 token
        let task_cancel_token = handle.cancellation_token();
        assert!(
            !task_cancel_token.is_cancelled(),
            "Task token should not be cancelled initially"
        );

        // 让任务正常完成
        let result = handle.await.expect("Task should complete");
        assert!(result.is_ok(), "Task should succeed");
        assert_eq!(result.unwrap(), "completed");

        // 测试可取消任务的取消
        let handle2 = tracker.spawn_cancellable(|cancel_token| async move {
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    CancellableTaskResult::Ok("should_not_complete".to_string())
                },
                _ = cancel_token.cancelled() => CancellableTaskResult::Cancelled,
            }
        });

        // 取消特定任务
        handle2.cancellation_token().cancel();

        let result2 = handle2.await.expect("Task should complete");
        assert!(result2.is_err(), "Task should be cancelled");
        assert!(
            result2.unwrap_err().is_cancellation(),
            "Should be a cancellation error"
        );

        // 验证指标
        assert_eq!(
            tracker.metrics().success(),
            1,
            "Should have 1 successful task"
        );
        assert_eq!(
            tracker.metrics().cancelled(),
            1,
            "Should have 1 cancelled task"
        );
    }

    /// 测试 FailedWithContinuation 辅助方法
    ///
    /// 本测试验证：
    /// 1. from_fn 从简单闭包创建可用的 continuation
    /// 2. from_cancellable 从可取消闭包创建可用的 continuation
    /// 3. 两个辅助方法都能与任务执行系统正确集成
    #[tokio::test]
    async fn test_continuation_helpers() {
        // 测试 from_fn 与 from_cancellable 两个 continuation 辅助方法。
        let tracker = TaskTracker::new(UnlimitedScheduler::new(), LogOnlyPolicy::new()).unwrap();

        // 测试 from_fn 辅助方法
        let handle1 = tracker.spawn(async {
            let error =
                FailedWithContinuation::from_fn(anyhow::anyhow!("Initial failure"), || async {
                    Ok("Success from from_fn".to_string())
                });
            let result: Result<String, anyhow::Error> = Err(error);
            result
        });

        let result1 = handle1.await.expect("Task should complete");
        assert!(
            result1.is_ok(),
            "Task with from_fn continuation should succeed"
        );
        assert_eq!(result1.unwrap(), "Success from from_fn");

        // 测试 from_cancellable 辅助方法
        let handle2 = tracker.spawn(async {
            let error = FailedWithContinuation::from_cancellable(
                anyhow::anyhow!("Initial failure"),
                |_cancel_token| async move { Ok("Success from from_cancellable".to_string()) },
            );
            let result: Result<String, anyhow::Error> = Err(error);
            result
        });

        let result2 = handle2.await.expect("Task should complete");
        assert!(
            result2.is_ok(),
            "Task with from_cancellable continuation should succeed"
        );
        assert_eq!(result2.unwrap(), "Success from from_cancellable");

        // 验证指标
        assert_eq!(
            tracker.metrics().success(),
            2,
            "Should have 2 successful tasks"
        );
        assert_eq!(tracker.metrics().failed(), 0, "Should have 0 failed tasks");
    }

    /// 测试带 mock 调度器跟踪的 should_reschedule() 策略方法
    ///
    /// 本测试验证：
    /// 1. 当 should_reschedule() 返回 false 时，guard 被复用（高效）
    /// 2. 当 should_reschedule() 返回 true 时，guard 通过调度器重新获取
    /// 3. 调度器的 acquire_execution_slot 被调用预期次数
    /// 4. 重调度对任务驱动与策略驱动的 continuation 都生效
    #[rstest]
    #[case(false, 1, "Policy requests no rescheduling - should reuse guard")]
    #[case(true, 2, "Policy requests rescheduling - should re-acquire guard")]
    #[tokio::test]
    async fn test_should_reschedule_policy_control(
        #[case] should_reschedule: bool,
        #[case] expected_acquisitions: u32,
        #[case] description: &str,
    ) {
        // 测试策略要求重调度时，调度器资源申请次数的变化。
        // 跟踪资源申请调用的 mock 调度器
        #[derive(Debug)]
        struct MockScheduler {
            acquisition_count: Arc<AtomicU32>,
        }

        impl MockScheduler {
            fn new() -> Self {
                Self {
                    acquisition_count: Arc::new(AtomicU32::new(0)),
                }
            }

            fn acquisition_count(&self) -> u32 {
                self.acquisition_count.load(Ordering::Relaxed)
            }
        }

        #[async_trait]
        impl TaskScheduler for MockScheduler {
            async fn acquire_execution_slot(
                &self,
                _cancel_token: CancellationToken,
            ) -> SchedulingResult<Box<dyn ResourceGuard>> {
                self.acquisition_count.fetch_add(1, Ordering::Relaxed);
                SchedulingResult::Execute(Box::new(UnlimitedGuard))
            }
        }

        // 控制重调度行为的策略
        #[derive(Debug)]
        struct RescheduleTestPolicy {
            should_reschedule: bool,
        }

        impl OnErrorPolicy for RescheduleTestPolicy {
            fn create_child(&self) -> Arc<dyn OnErrorPolicy> {
                Arc::new(RescheduleTestPolicy {
                    should_reschedule: self.should_reschedule,
                })
            }

            fn create_context(&self) -> Option<Box<dyn std::any::Any + Send + 'static>> {
                None // 无状态策略
            }

            fn allow_continuation(
                &self,
                _error: &anyhow::Error,
                _context: &OnErrorContext,
            ) -> bool {
                true // 本测试始终允许 continuation
            }

            fn should_reschedule(&self, _error: &anyhow::Error, _context: &OnErrorContext) -> bool {
                self.should_reschedule
            }

            fn on_error(
                &self,
                _error: &anyhow::Error,
                _context: &mut OnErrorContext,
            ) -> ErrorResponse {
                ErrorResponse::Fail // 不允许 continuation 时直接失败
            }
        }

        let mock_scheduler = Arc::new(MockScheduler::new());
        let policy = Arc::new(RescheduleTestPolicy { should_reschedule });
        let tracker = TaskTracker::new(mock_scheduler.clone(), policy).unwrap();
        let execution_log = Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));

        // 第二次尝试成功的简单 continuation
        #[derive(Debug)]
        struct SimpleRetryContinuation {
            log: Arc<tokio::sync::Mutex<Vec<String>>>,
        }

        #[async_trait]
        impl Continuation for SimpleRetryContinuation {
            async fn execute(
                &self,
                _cancel_token: CancellationToken,
            ) -> TaskExecutionResult<Box<dyn std::any::Any + Send + 'static>> {
                self.log
                    .lock()
                    .await
                    .push("continuation_executed".to_string());

                // 立即成功
                TaskExecutionResult::Success(Box::new("continuation_success".to_string()))
            }
        }

        // 以 continuation 失败的任务
        let continuation = Arc::new(SimpleRetryContinuation {
            log: execution_log.clone(),
        });

        let log_for_task = execution_log.clone();
        let handle = tracker.spawn(async move {
            log_for_task
                .lock()
                .await
                .push("initial_task_failure".to_string());
            let error = anyhow::anyhow!("Initial task failure");
            let result: Result<String, anyhow::Error> =
                Err(FailedWithContinuation::into_anyhow(error, continuation));
            result
        });

        let result = handle.await.expect("Task should complete");

        // 无论重调度行为如何，任务都应成功
        assert!(result.is_ok(), "{}: Task should succeed", description);
        assert_eq!(
            tracker.metrics().success(),
            1,
            "{}: Should have 1 success",
            description
        );

        // 验证执行日志
        let log = execution_log.lock().await;
        assert_eq!(
            log.len(),
            2,
            "{}: Should have initial task + continuation",
            description
        );
        assert_eq!(log[0], "initial_task_failure");
        assert_eq!(log[1], "continuation_executed");

        // 最关键：验证调度器资源申请次数
        let actual_acquisitions = mock_scheduler.acquisition_count();
        assert_eq!(
            actual_acquisitions, expected_acquisitions,
            "{}: Expected {} scheduler acquisitions, got {}",
            description, expected_acquisitions, actual_acquisitions
        );
    }

    /// 测试带自定义动作策略的 continuation 循环
    ///
    /// 本测试验证自定义错误动作也能提供 continuation，
    /// 并且循环行为在策略提供的 continuation 下正常工作
    ///
    /// 注：使用全新的策略/动作实例，以避免全局状态干扰。
    #[rstest]
    #[case(1, true, "Custom action with 1 retry should succeed")]
    #[case(3, true, "Custom action with 3 retries should succeed")]
    #[tokio::test]
    async fn test_continuation_loop_with_custom_action_policy(
        unlimited_scheduler: Arc<UnlimitedScheduler>,
        #[case] max_retries: u32,
        #[case] should_succeed: bool,
        #[case] description: &str,
    ) {
        // 测试自定义错误动作驱动 continuation 重试循环。
        let execution_log = Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));
        let retry_count = Arc::new(std::sync::atomic::AtomicU32::new(0));

        // 提供直到 max_retries 之前 continuation 的自定义动作
        #[derive(Debug)]
        struct RetryAction {
            log: Arc<tokio::sync::Mutex<Vec<String>>>,
            retry_count: Arc<std::sync::atomic::AtomicU32>,
            max_retries: u32,
        }

        #[async_trait]
        impl OnErrorAction for RetryAction {
            async fn execute(
                &self,
                _error: &anyhow::Error,
                _task_id: TaskId,
                _attempt_count: u32,
                _context: &TaskExecutionContext,
            ) -> ActionResult {
                let current_retry = self
                    .retry_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    + 1;
                self.log
                    .lock()
                    .await
                    .push(format!("custom_action_retry_{}", current_retry));

                if current_retry <= self.max_retries {
                    // 提供一个 continuation，若为最后一次重试则成功
                    #[derive(Debug)]
                    struct RetryContinuation {
                        log: Arc<tokio::sync::Mutex<Vec<String>>>,
                        retry_number: u32,
                        max_retries: u32,
                    }

                    #[async_trait]
                    impl Continuation for RetryContinuation {
                        async fn execute(
                            &self,
                            _cancel_token: CancellationToken,
                        ) -> TaskExecutionResult<Box<dyn std::any::Any + Send + 'static>>
                        {
                            self.log
                                .lock()
                                .await
                                .push(format!("retry_continuation_{}", self.retry_number));

                            if self.retry_number >= self.max_retries {
                                // 最后一次重试成功
                                TaskExecutionResult::Success(Box::new(format!(
                                    "success_after_{}_retries",
                                    self.retry_number
                                )))
                            } else {
                                // 仍需更多重试，以普通错误（非 FailedWithContinuation）失败
                                // 这会再次触发自定义动作
                                TaskExecutionResult::Error(anyhow::anyhow!(
                                    "Retry {} still failing",
                                    self.retry_number
                                ))
                            }
                        }
                    }

                    let continuation = Arc::new(RetryContinuation {
                        log: self.log.clone(),
                        retry_number: current_retry,
                        max_retries: self.max_retries,
                    });

                    ActionResult::Continue { continuation }
                } else {
                    // 超过最大重试次数，取消
                    ActionResult::Shutdown
                }
            }
        }

        // 使用重试动作的自定义策略
        #[derive(Debug)]
        struct CustomRetryPolicy {
            action: Arc<RetryAction>,
        }

        impl OnErrorPolicy for CustomRetryPolicy {
            fn create_child(&self) -> Arc<dyn OnErrorPolicy> {
                Arc::new(CustomRetryPolicy {
                    action: self.action.clone(),
                })
            }

            fn create_context(&self) -> Option<Box<dyn std::any::Any + Send + 'static>> {
                None // 无状态策略——无堆分配
            }

            fn on_error(
                &self,
                _error: &anyhow::Error,
                _context: &mut OnErrorContext,
            ) -> ErrorResponse {
                ErrorResponse::Custom(Box::new(RetryAction {
                    log: self.action.log.clone(),
                    retry_count: self.action.retry_count.clone(),
                    max_retries: self.action.max_retries,
                }))
            }
        }

        let action = Arc::new(RetryAction {
            log: execution_log.clone(),
            retry_count: retry_count.clone(),
            max_retries,
        });
        let policy = Arc::new(CustomRetryPolicy { action });
        let tracker = TaskTracker::new(unlimited_scheduler, policy).unwrap();

        // 总是以普通错误（非 FailedWithContinuation）失败的任务
        let log_for_task = execution_log.clone();
        let handle = tracker.spawn(async move {
            log_for_task
                .lock()
                .await
                .push("original_task_failed".to_string());
            let result: Result<String, anyhow::Error> =
                Err(anyhow::anyhow!("Original task failure"));
            result
        });

        // 执行并验证结果
        let result = handle.await.expect("Task should complete");

        if should_succeed {
            assert!(result.is_ok(), "{}: Task should succeed", description);
            assert_eq!(
                tracker.metrics().success(),
                1,
                "{}: Should have 1 success",
                description
            );

            // 验证重试序列
            let log = execution_log.lock().await;
            let expected_entries = 1 + (max_retries * 2); // 原始 + 每次重试（动作 + continuation）
            assert_eq!(
                log.len(),
                expected_entries as usize,
                "{}: Should have {} log entries",
                description,
                expected_entries
            );

            assert_eq!(
                retry_count.load(std::sync::atomic::Ordering::Relaxed),
                max_retries,
                "{}: Should have made {} retry attempts",
                description,
                max_retries
            );
        } else {
            assert!(result.is_err(), "{}: Task should fail", description);
            assert!(
                tracker.cancellation_token().is_cancelled(),
                "{}: Should be cancelled",
                description
            );

            // 应在达到 max_retries 后停止
            let final_retry_count = retry_count.load(std::sync::atomic::Ordering::Relaxed);
            assert!(
                final_retry_count > max_retries,
                "{}: Should have exceeded max_retries ({}), got {}",
                description,
                max_retries,
                final_retry_count
            );
        }
    }

    /// 测试混合来源的 continuation（任务驱动 + 策略驱动）
    ///
    /// 本测试验证任务提供的 continuation 与策略提供的 continuation
    /// 能在同一执行流中协同工作
    #[rstest]
    #[tokio::test]
    async fn test_mixed_continuation_sources(
        unlimited_scheduler: Arc<UnlimitedScheduler>,
        log_policy: Arc<LogOnlyPolicy>,
    ) {
        // 测试任务自身与策略侧 continuation 混合出现时的执行顺序。
        let execution_log = Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));
        let tracker = TaskTracker::new(unlimited_scheduler, log_policy).unwrap();

        // 提供 continuation、随后以普通错误失败的任务
        let log_for_task = execution_log.clone();
        let log_for_continuation = execution_log.clone();

        #[derive(Debug)]
        struct MixedContinuation {
            log: Arc<tokio::sync::Mutex<Vec<String>>>,
        }

        #[async_trait]
        impl Continuation for MixedContinuation {
            async fn execute(
                &self,
                _cancel_token: CancellationToken,
            ) -> TaskExecutionResult<Box<dyn std::any::Any + Send + 'static>> {
                self.log
                    .lock()
                    .await
                    .push("task_continuation_executed".to_string());
                // 该 continuation 以普通错误（非 FailedWithContinuation）失败
                // 因此由策略处理（LogOnlyPolicy 仅继续）
                TaskExecutionResult::Error(anyhow::anyhow!("Task continuation failed"))
            }
        }

        let continuation = Arc::new(MixedContinuation {
            log: log_for_continuation,
        });

        let handle = tracker.spawn(async move {
            log_for_task
                .lock()
                .await
                .push("original_task_executed".to_string());

            // 任务提供 continuation
            let error = anyhow::anyhow!("Original task failed");
            let result: Result<String, anyhow::Error> =
                Err(FailedWithContinuation::into_anyhow(error, continuation));
            result
        });

        // 执行——应失败，因为 continuation 失败且 LogOnlyPolicy 仅记录
        let result = handle.await.expect("Task should complete");
        assert!(
            result.is_err(),
            "Should fail because continuation fails and policy just logs"
        );

        // 验证执行顺序
        let log = execution_log.lock().await;
        assert_eq!(log.len(), 2);
        assert_eq!(log[0], "original_task_executed");
        assert_eq!(log[1], "task_continuation_executed");

        // 验证指标——应显示来自 continuation 的失败
        assert_eq!(tracker.metrics().success(), 0);
        assert_eq!(tracker.metrics().failed(), 1);
    }

    /// 调试用例，用于理解阈值策略在重试循环中的行为
    #[rstest]
    #[tokio::test]
    async fn debug_threshold_policy_in_retry_loop(unlimited_scheduler: Arc<UnlimitedScheduler>) {
        // 调试并观察阈值策略在重试循环中的行为。
        let policy = ThresholdCancelPolicy::with_threshold(2);
        let tracker = TaskTracker::new(unlimited_scheduler, policy.clone()).unwrap();

        // 总是以普通错误失败的简单 continuation
        #[derive(Debug)]
        struct AlwaysFailContinuation {
            attempt: Arc<std::sync::atomic::AtomicU32>,
        }

        #[async_trait]
        impl Continuation for AlwaysFailContinuation {
            async fn execute(
                &self,
                _cancel_token: CancellationToken,
            ) -> TaskExecutionResult<Box<dyn std::any::Any + Send + 'static>> {
                let attempt_num = self
                    .attempt
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    + 1;
                println!("Continuation attempt {}", attempt_num);
                TaskExecutionResult::Error(anyhow::anyhow!(
                    "Continuation attempt {} failed",
                    attempt_num
                ))
            }
        }

        let attempt_counter = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let continuation = Arc::new(AlwaysFailContinuation {
            attempt: attempt_counter.clone(),
        });

        let handle = tracker.spawn(async move {
            println!("Original task executing");
            let error = anyhow::anyhow!("Original task failed");
            let result: Result<String, anyhow::Error> =
                Err(FailedWithContinuation::into_anyhow(error, continuation));
            result
        });

        let result = handle.await.expect("Task should complete");
        println!("Final result: {:?}", result.is_ok());
        println!("Policy failure count: {}", policy.failure_count());
        println!(
            "Continuation attempts: {}",
            attempt_counter.load(std::sync::atomic::Ordering::Relaxed)
        );
        println!(
            "Tracker cancelled: {}",
            tracker.cancellation_token().is_cancelled()
        );
        println!(
            "Metrics: success={}, failed={}",
            tracker.metrics().success(),
            tracker.metrics().failed()
        );

        // 这有助于我们理解正在发生的情况
    }
}
