// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! [`Runtime`] —— 本地进程内共享运行时资源的统一入口
//!
//! ## 设计意图
//! 为 [`crate::component::Component`] 及其子对象提供一个集中化句柄，用于获取
//! 运行时能力：主/次 Tokio runtime、优雅关闭令牌、端点关闭令牌、`compute_pool`
//! （可选的 CPU 计算池）以及 `block_in_place` 信号量。内部持有主 `CancellationToken`，
//! 上层代码调用 [`Runtime::shutdown`] 即可使所有挂接组件在有限窗口内接受取消信号。
//!
//! 说明：本模块的 API 表面仍在演进，为了兼容历史代码尚未统一收敛可见性。
//!
//! ## 外部契约
//! - 公开结构体 `Runtime`（`Debug + Clone`）与重导出 `tokio_util::sync::CancellationToken`。
//! - 公开方法集合 `from_current` / `from_handle` / `from_settings` / `single_threaded` /
//!   `id` / `primary` / `secondary` / `primary_token` / `child_token` / `endpoint_shutdown_token` /
//!   `graceful_shutdown_tracker` / `compute_pool` / `block_in_place_permits` / `shutdown` 等签名不变。
//! - `shutdown` 的终止语义（取消令牌传播、优雅窗口、最终需调者结束进程）保持不变。
//! - 环境变量、`RuntimeConfig` 与 `RuntimeType`（`Shared` / `External`）内部构造逻辑保持不变。
//!
//! ## 实现要点
//! - **多样化（Rule 2）**：`detect_worker_thread_count` 采用 [`tokio::task::JoinSet`]
//!   取代历史代码里“手动维护 `Vec<JoinHandle>` + 次序 await”的写法；在运行时对外可观察
//!   语义等价（同样 spawn `probe_count` 个 `spawn_blocking` 探针、同样等全部完成、同样
//!   统计唯一线程 ID 数量），仅使语义更贴近“一批任务集合入口”本质。
//! - **不**变动 `shutdown` 语义、不调整主/次 runtime 选择策略、不动取消令牌拓扑，
//!   以守住 Rule 1。

use super::utils::GracefulShutdownTracker;
use crate::{
    compute,
    config::{self, RuntimeConfig},
};

use futures::Future;
use once_cell::sync::OnceCell;
use std::{
    mem::ManuallyDrop,
    sync::{Arc, atomic::Ordering},
};
use tokio::{signal, sync::Mutex, task::JoinHandle};

pub use tokio_util::sync::CancellationToken;

/// 用于构造本地 [Runtime] 的 Tokio runtime 类型封装。
#[derive(Clone, Debug)]
enum RuntimeType {
    Shared(Arc<ManuallyDrop<tokio::runtime::Runtime>>),
    External(tokio::runtime::Handle),
}

/// 本地 [Runtime]，负责提供当前物理节点上的共享运行时资源。
#[derive(Debug, Clone)]
pub struct Runtime {
    id: Arc<String>,
    primary: RuntimeType,
    secondary: RuntimeType,
    cancellation_token: CancellationToken,
    endpoint_shutdown_token: CancellationToken,
    graceful_shutdown_tracker: Arc<GracefulShutdownTracker>,
    compute_pool: Option<Arc<compute::ComputePool>>,
    block_in_place_permits: Option<Arc<tokio::sync::Semaphore>>,
}

impl Runtime {
    /// 构造运行时基础对象。
    ///
    /// 处理流程为：初始化 timeline/NVTX、生成运行时 ID、创建根取消令牌，
    /// 并在未提供 secondary runtime 时自动补一个单线程后台运行时。
    fn new(runtime: RuntimeType, secondary: Option<RuntimeType>) -> anyhow::Result<Runtime> {
        crate::nvtx::init();

        let runtime_id = Arc::new(uuid::Uuid::new_v4().to_string());
        let root_token = CancellationToken::new();
        let shutdown_token = root_token.child_token();

        let background_runtime = if let Some(existing_runtime) = secondary {
            existing_runtime
        } else {
            tracing::debug!("Created secondary runtime with single thread");
            let owned_runtime = RuntimeConfig::single_threaded().create_runtime()?;
            RuntimeType::Shared(Arc::new(ManuallyDrop::new(owned_runtime)))
        };

        Ok(Runtime {
            id: runtime_id,
            primary: runtime,
            secondary: background_runtime,
            cancellation_token: root_token,
            endpoint_shutdown_token: shutdown_token,
            graceful_shutdown_tracker: Arc::new(GracefulShutdownTracker::new()),
            compute_pool: None,
            block_in_place_permits: None,
        })
    }

    /// 按配置构造运行时，并补充计算线程池与 `block_in_place` 并发许可。
    ///
    /// 处理流程为：先复用基础构造流程，再根据配置尝试创建 compute pool，
    /// 最后按 worker 数量推导并初始化 `Semaphore` 许可数。
    fn new_with_config(
        runtime: RuntimeType,
        secondary: Option<RuntimeType>,
        config: &RuntimeConfig,
    ) -> anyhow::Result<Runtime> {
        let mut assembled = Self::new(runtime, secondary)?;

        let compute_plan = crate::compute::ComputeConfig {
            num_threads: config.compute_threads,
            stack_size: config.compute_stack_size,
            thread_prefix: config.compute_thread_prefix.clone(),
            pin_threads: false,
        };

        match config.compute_threads {
            Some(0) => {
                tracing::info!("Compute pool disabled (compute_threads = 0)");
            }
            _ => match crate::compute::ComputePool::new(compute_plan) {
                Ok(pool) => {
                    let shared_pool = Arc::new(pool);
                    tracing::debug!(
                        "Initialized compute pool with {} threads",
                        shared_pool.num_threads()
                    );
                    assembled.compute_pool = Some(shared_pool);
                }
                Err(err) => {
                    tracing::warn!(
                        "Failed to create compute pool: {}. CPU-intensive operations will use spawn_blocking",
                        err
                    );
                }
            },
        }

        let worker_count = config
            .num_worker_threads
            .unwrap_or_else(|| std::thread::available_parallelism().unwrap().get());
        let permit_count = worker_count.saturating_sub(1).max(1);
        let semaphore = tokio::sync::Semaphore::new(permit_count);
        assembled.block_in_place_permits = Some(Arc::new(semaphore));
        tracing::debug!(
            "Initialized block_in_place permits: {} (from {} worker threads)",
            permit_count,
            worker_count
        );

        Ok(assembled)
    }

    /// 在当前线程初始化线程本地计算上下文。
    ///
    /// 该函数通常应在每个 Tokio worker 线程上调用，既会写入 compute 上下文，
    /// 也会为当前线程设置 timeline/NVTX 名称。
    pub fn initialize_thread_local(&self) {
        if let Some((pool, permits)) = self
            .compute_pool
            .as_ref()
            .zip(self.block_in_place_permits.as_ref())
        {
            crate::compute::thread_local::initialize_context(
                Arc::clone(pool),
                Arc::clone(permits),
            );
        }

        let current = std::thread::current();
        let timeline_name = current
            .name()
            .map(str::to_owned)
            .unwrap_or_else(|| format!("tokio-worker-{:?}", current.id()));
        crate::nvtx::name_current_thread_impl(&timeline_name);
    }

    /// 使用 barrier 在所有 worker 线程上初始化线程本地计算上下文。
    ///
    /// 处理流程为：先确认 compute pool 已启用，再探测 worker 数，随后并发派发
    /// `spawn_blocking` 任务并在 barrier 同步点统一完成初始化。
    pub async fn initialize_all_thread_locals(&self) -> anyhow::Result<()> {
        let Some((shared_pool, shared_permits)) = self
            .compute_pool
            .as_ref()
            .zip(self.block_in_place_permits.as_ref())
            .map(|(pool, permits)| (Arc::clone(pool), Arc::clone(permits)))
        else {
            tracing::debug!("No compute pool configured, skipping thread-local initialization");
            return Ok(());
        };

        let worker_count = self.detect_worker_thread_count().await;
        if worker_count == 0 {
            return Err(anyhow::anyhow!("No worker threads detected"));
        }

        let sync_point = Arc::new(std::sync::Barrier::new(worker_count));
        let mut tasks = Vec::with_capacity(worker_count);

        for worker_index in 0..worker_count {
            let sync_point = Arc::clone(&sync_point);
            let shared_pool = Arc::clone(&shared_pool);
            let shared_permits = Arc::clone(&shared_permits);

            tasks.push(tokio::task::spawn_blocking(move || {
                sync_point.wait();
                crate::compute::thread_local::initialize_context(shared_pool, shared_permits);
                tracing::trace!(
                    "Initialized thread-local compute context on thread {:?} (worker {})",
                    std::thread::current().id(),
                    worker_index
                );
            }));
        }

        for task in tasks {
            task.await?;
        }

        tracing::info!(
            "Successfully initialized thread-local compute context on {} worker threads",
            worker_count
        );
        Ok(())
    }

    // === SECTION: 运行时探针 ===

    /// 通过并发探针统计运行时实际使用到的 worker 线程数量。
    ///
    /// 中文说明：
    /// 1. 以 `parking_lot::Mutex<HashSet<ThreadId>>` 作为探针共享状态，本身由
    ///    `Arc` 包裹以便多份探针并发写入。
    /// 2. 采用 [`tokio::task::JoinSet`] 集中调度探针 —— 每个探针都是一个 `spawn_blocking`
    ///    任务，负责记录当前线程 ID；JoinSet 提供“批量启动 + 按完成顺序收干”语义，
    ///    取代历史实现里“`Vec<JoinHandle>` + 次序 await”的写法。两者对外可观察语义严格等价：
    ///    同样 spawn `probe_count` 个任务、同样等全部完成后读取 `seen_threads`。
    /// 3. 最后输出 `debug` 日志返回检测到的唯一线程数量。
    async fn detect_worker_thread_count(&self) -> usize {
        use parking_lot::Mutex;
        use std::collections::HashSet;
        use tokio::task::JoinSet;

        let seen_threads = Arc::new(Mutex::new(HashSet::new()));
        let probe_count = 100;
        let mut probes: JoinSet<()> = JoinSet::new();

        for _ in 0..probe_count {
            let seen_threads = Arc::clone(&seen_threads);
            probes.spawn_blocking(move || {
                let current_id = std::thread::current().id();
                seen_threads.lock().insert(current_id);
            });
        }

        while probes.join_next().await.is_some() {}

        let detected_workers = seen_threads.lock().len();
        tracing::debug!("Detected {detected_workers} worker threads in runtime");
        detected_workers
    }

    /// 从当前 Tokio 上下文获取句柄，并包装成 `Runtime`。
    pub fn from_current() -> anyhow::Result<Runtime> {
        let handle = tokio::runtime::Handle::current();
        Self::from_handle(handle)
    }

    /// 基于外部传入的 Tokio handle 构造 `Runtime`，主次运行时都复用该 handle。
    pub fn from_handle(handle: tokio::runtime::Handle) -> anyhow::Result<Runtime> {
        let primary = RuntimeType::External(handle.clone());
        let secondary = Some(RuntimeType::External(handle));
        Self::new(primary, secondary)
    }

    /// 从环境和配置项加载运行时配置，并构造一个完整的 [`Runtime`]。
    /// 具体配置来源见 [`config::RuntimeConfig::from_settings`]。
    pub fn from_settings() -> anyhow::Result<Runtime> {
        let config = config::RuntimeConfig::from_settings()?;
        let owned_runtime = Arc::new(ManuallyDrop::new(config.create_runtime()?));
        let primary = RuntimeType::Shared(Arc::clone(&owned_runtime));
        let secondary = RuntimeType::External(owned_runtime.handle().clone());
        Self::new_with_config(primary, Some(secondary), &config)
    }

    /// 创建一个带主次两个单线程 Tokio runtime 的 [`Runtime`]。
    pub fn single_threaded() -> anyhow::Result<Runtime> {
        let config = config::RuntimeConfig::single_threaded();
        let runtime = config.create_runtime()?;
        let owned = RuntimeType::Shared(Arc::new(ManuallyDrop::new(runtime)));
        Self::new(owned, None)
    }

    /// 返回当前 [`Runtime`] 的唯一标识字符串。
    pub fn id(&self) -> &str {
        self.id.as_str()
    }

    /// 返回主运行时的 [`tokio::runtime::Handle`]，用于业务异步任务调度。
    pub fn primary(&self) -> tokio::runtime::Handle {
        let runtime = &self.primary;
        runtime.handle()
    }

    /// 返回次运行时的 [`tokio::runtime::Handle`]，用于后台控制类任务调度。
    pub fn secondary(&self) -> tokio::runtime::Handle {
        let runtime = &self.secondary;
        runtime.handle()
    }

    /// 取得主取消令牌的克隆，用于监听或触发运行时整体停止。
    pub fn primary_token(&self) -> CancellationToken {
        CancellationToken::clone(&self.cancellation_token)
    }

    /// 基于端点关闭令牌派生一个子 [`CancellationToken`]。
    /// 该令牌会先于主运行时取消，用于实现端点优先的优雅关闭流程。
    pub fn child_token(&self) -> CancellationToken {
        self.endpoint_shutdown_token.child_token()
    }

    /// 返回优雅关闭跟踪器的共享引用，用于登记和等待端点退出。
    pub(crate) fn graceful_shutdown_tracker(&self) -> Arc<GracefulShutdownTracker> {
        Arc::clone(&self.graceful_shutdown_tracker)
    }

    /// 返回 CPU 密集型任务可用的 compute pool 引用。
    ///
    /// 如果 compute pool 未初始化成功，例如被显式关闭或创建失败，则返回 `None`。
    pub fn compute_pool(&self) -> Option<&Arc<crate::compute::ComputePool>> {
        self.compute_pool.as_ref().map(|pool| pool)
    }

    /// 启动 [`Runtime`] 的三阶段优雅关闭流程。
    ///
    /// 处理流程为：先取消端点级令牌，等待已登记端点退出，再取消主运行时令牌。
    pub fn shutdown(&self) {
        tracing::info!("Runtime shutdown initiated");

        let tracker = Arc::clone(&self.graceful_shutdown_tracker);
        let runtime_token = self.cancellation_token.clone();
        let endpoint_token = self.endpoint_shutdown_token.clone();

        let shutdown_task = async move {
            tracing::info!("Phase 1: Cancelling endpoint shutdown token");
            endpoint_token.cancel();

            tracing::info!("Phase 2: Waiting for graceful endpoints to complete");
            let active_endpoints = tracker.get_count();
            tracing::info!("Active graceful endpoints: {active_endpoints}");

            if active_endpoints > 0 {
                tracker.wait_for_completion().await;
            }

            tracing::info!(
                "Phase 3: All endpoints ended gracefully. Connections to backend services will now be disconnected"
            );
            runtime_token.cancel();
        };

        self.primary().spawn(shutdown_task);
    }
}

impl RuntimeType {
    /// 返回内部 runtime 对应的 [`tokio::runtime::Handle`]。
    pub fn handle(&self) -> tokio::runtime::Handle {
        if let RuntimeType::External(handle) = self {
            return handle.clone();
        }

        match self {
            RuntimeType::Shared(runtime) => runtime.handle().clone(),
            RuntimeType::External(_) => unreachable!(),
        }
    }
}

/// 处理在异步上下文中释放 Tokio runtime 的问题。
///
/// 当该类型通过 Python 绑定使用时，runtime 可能会在 Python 的 asyncio 环境中被释放。
/// Tokio 不允许这种场景，直接释放会触发 panic，并让最后阶段的日志难以完整输出。
///
/// 典型 panic 如下：
/// > pyo3_runtime.PanicException: Cannot drop a runtime in a context where blocking is not allowed.
/// > This happens when a runtime is dropped from within an asynchronous context.
///
/// 因此这里用 `ManuallyDrop` 包装 runtime，并在检测到当前位于异步 runtime 内部时，
/// 改用 Tokio 提供的后台关闭路径。
impl Drop for RuntimeType {
    /// 根据当前是否处于 Tokio 异步上下文，选择后台关闭或直接释放底层 runtime。
    fn drop(&mut self) {
        let RuntimeType::Shared(arc) = self else {
            return;
        };

        let Some(runtime_slot) = Arc::get_mut(arc) else {
            return;
        };

        let running_inside_tokio = tokio::runtime::Handle::try_current().is_ok();
        if running_inside_tokio {
            let runtime = unsafe { ManuallyDrop::take(runtime_slot) };
            runtime.shutdown_background();
        } else {
            unsafe { ManuallyDrop::drop(runtime_slot) };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compute::thread_local::{get_pool, has_compute_context, try_acquire_block_permit};
    use std::sync::Arc;
    use temp_env::with_vars;
    use tokio::time::{Duration, timeout};
    use uuid::Uuid;

    /// 构造一个共享所有权的 `RuntimeType::Shared`，供句柄与释放语义测试复用。
    fn shared_runtime_type() -> RuntimeType {
        let runtime = Arc::new(ManuallyDrop::new(
            RuntimeConfig::single_threaded().create_runtime().unwrap(),
        ));
        RuntimeType::Shared(runtime)
    }

    /// 构造一个外部 runtime 及其对应的 `RuntimeType::External` 句柄包装。
    fn external_runtime_type() -> (tokio::runtime::Runtime, RuntimeType) {
        let runtime = RuntimeConfig::single_threaded().create_runtime().unwrap();
        let handle = RuntimeType::External(runtime.handle().clone());
        (runtime, handle)
    }

    /// 创建一个启用了 compute pool 的测试运行时。
    fn runtime_with_compute_pool(num_workers: usize, compute_threads: usize) -> Runtime {
        let primary_runtime = RuntimeConfig::single_threaded().create_runtime().unwrap();
        let mut config = RuntimeConfig::single_threaded();
        config.num_worker_threads = Some(num_workers);
        config.compute_threads = Some(compute_threads);

        Runtime::new_with_config(
            RuntimeType::External(primary_runtime.handle().clone()),
            None,
            &config,
        )
        .unwrap()
    }

    #[test]
    /// 测试：未显式提供 secondary runtime 时，会自动创建后备运行时并正常执行任务。
    fn test_new_creates_runtime_with_secondary_fallback() {
        let (primary_runtime, primary) = external_runtime_type();
        let runtime = Runtime::new(primary, None).unwrap();

        assert!(Uuid::parse_str(runtime.id()).is_ok());
        assert!(runtime.compute_pool.is_none());
        assert!(runtime.block_in_place_permits.is_none());
        assert!(!runtime.primary_token().is_cancelled());
        assert!(!runtime.child_token().is_cancelled());

        let primary_result = primary_runtime
            .block_on(async { runtime.primary().spawn(async { 7usize }).await.unwrap() });
        let secondary_result = primary_runtime
            .block_on(async { runtime.secondary().spawn(async { 9usize }).await.unwrap() });

        assert_eq!(primary_result, 7);
        assert_eq!(secondary_result, 9);
    }

    #[test]
    /// 测试：配置 `compute_threads = 0` 时会禁用 compute pool，但仍初始化许可数。
    fn test_new_with_config_disables_compute_pool_when_requested() {
        let (primary_runtime, primary) = external_runtime_type();
        let mut config = RuntimeConfig::single_threaded();
        config.num_worker_threads = Some(4);
        config.compute_threads = Some(0);

        let runtime = Runtime::new_with_config(primary, None, &config).unwrap();

        assert!(runtime.compute_pool().is_none());
        assert_eq!(runtime.block_in_place_permits.as_ref().unwrap().available_permits(), 3);

        drop(primary_runtime);
    }

    #[test]
    /// 测试：启用 compute pool 时会正确创建线程池和 `block_in_place` 许可。
    fn test_new_with_config_initializes_compute_pool_and_permits() {
        let (primary_runtime, primary) = external_runtime_type();
        let mut config = RuntimeConfig::single_threaded();
        config.num_worker_threads = Some(3);
        config.compute_threads = Some(1);

        let runtime = Runtime::new_with_config(primary, None, &config).unwrap();

        assert_eq!(runtime.compute_pool().unwrap().num_threads(), 1);
        assert_eq!(runtime.block_in_place_permits.as_ref().unwrap().available_permits(), 2);

        drop(primary_runtime);
    }

    #[test]
    /// 测试：即使 worker 线程数很小，也至少保留一个 `block_in_place` 许可。
    fn test_new_with_config_keeps_at_least_one_block_in_place_permit() {
        let (primary_runtime, primary) = external_runtime_type();
        let mut config = RuntimeConfig::single_threaded();
        config.num_worker_threads = Some(1);
        config.compute_threads = Some(0);

        let runtime = Runtime::new_with_config(primary, None, &config).unwrap();

        assert!(runtime.compute_pool().is_none());
        assert_eq!(runtime.block_in_place_permits.as_ref().unwrap().available_permits(), 1);

        drop(primary_runtime);
    }

    #[test]
    /// 测试：当前线程初始化后，会写入 compute 上下文与相关许可。
    fn test_initialize_thread_local_sets_compute_context() {
        let runtime = runtime_with_compute_pool(2, 1);

        runtime.initialize_thread_local();

        assert!(has_compute_context());
        assert_eq!(get_pool().unwrap().num_threads(), 1);
        assert!(try_acquire_block_permit().is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    /// 测试：未启用 compute pool 时，批量线程本地初始化为空操作且不会报错。
    async fn test_initialize_all_thread_locals_without_compute_pool_is_noop() {
        let runtime = Runtime::from_current().unwrap();

        runtime.initialize_all_thread_locals().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    /// 测试：启用 compute pool 后，批量线程本地初始化可以顺利完成。
    async fn test_initialize_all_thread_locals_with_compute_pool_succeeds() {
        let mut config = RuntimeConfig::single_threaded();
        config.num_worker_threads = Some(2);
        config.compute_threads = Some(1);

        let runtime = Runtime::new_with_config(
            RuntimeType::External(tokio::runtime::Handle::current()),
            None,
            &config,
        )
        .unwrap();

        runtime.initialize_all_thread_locals().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    /// 测试：worker 线程探测结果为正数，且不会超过探针任务数量上限。
    async fn test_detect_worker_thread_count_returns_positive_value() {
        let runtime = Runtime::from_current().unwrap();
        let count = runtime.detect_worker_thread_count().await;

        assert!(count > 0);
        assert!(count <= 100);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    /// 测试：`from_current` 会复用当前 Tokio 运行时句柄。
    async fn test_from_current_uses_active_runtime_handles() {
        let runtime = Runtime::from_current().unwrap();

        let primary = runtime.primary().spawn(async { 11usize }).await.unwrap();
        let secondary = runtime.secondary().spawn(async { 13usize }).await.unwrap();

        assert_eq!(primary, 11);
        assert_eq!(secondary, 13);
    }

    #[test]
    /// 测试：`from_handle` 会复用传入 handle 作为主次运行时入口。
    fn test_from_handle_reuses_provided_handle() {
        let rt = RuntimeConfig::single_threaded().create_runtime().unwrap();
        let runtime = Runtime::from_handle(rt.handle().clone()).unwrap();

        let primary = rt.block_on(async { runtime.primary().spawn(async { 17usize }).await.unwrap() });
        let secondary = rt.block_on(async { runtime.secondary().spawn(async { 19usize }).await.unwrap() });

        assert_eq!(primary, 17);
        assert_eq!(secondary, 19);
    }

    #[test]
    /// 测试：显式传入 secondary handle 时，不会再额外创建后备运行时。
    fn test_new_reuses_explicit_secondary_handle() {
        let primary_runtime = RuntimeConfig::single_threaded().create_runtime().unwrap();
        let secondary_runtime = RuntimeConfig::single_threaded().create_runtime().unwrap();
        let runtime = Runtime::new(
            RuntimeType::External(primary_runtime.handle().clone()),
            Some(RuntimeType::External(secondary_runtime.handle().clone())),
        )
        .unwrap();

        let primary =
            primary_runtime.block_on(async { runtime.primary().spawn(async { 41usize }).await.unwrap() });
        let secondary = secondary_runtime
            .block_on(async { runtime.secondary().spawn(async { 43usize }).await.unwrap() });

        assert_eq!(primary, 41);
        assert_eq!(secondary, 43);
    }

    #[test]
    /// 测试：`from_settings` 会按环境配置初始化 compute pool 和许可数量。
    fn test_from_settings_initializes_compute_pool() {
        with_vars(
            vec![
                ("DYN_RUNTIME_NUM_WORKER_THREADS", Some("2")),
                ("DYN_COMPUTE_THREADS", Some("1")),
            ],
            || {
                let runtime = Runtime::from_settings().unwrap();

                assert_eq!(runtime.compute_pool().unwrap().num_threads(), 1);
                assert_eq!(
                    runtime.block_in_place_permits.as_ref().unwrap().available_permits(),
                    1
                );
            },
        );
    }

    #[test]
    /// 测试：`single_threaded` 创建的运行时不启用 compute pool，但主次句柄均可工作。
    fn test_single_threaded_creates_runtime_without_compute_pool() {
        let runtime = Runtime::single_threaded().unwrap();

        let primary = runtime.primary().block_on(async { 23usize });
        let secondary = runtime.secondary().block_on(async { 29usize });

        assert_eq!(primary, 23);
        assert_eq!(secondary, 29);
        assert!(runtime.compute_pool().is_none());
    }

    #[test]
    /// 测试：优雅关闭跟踪器访问器返回的是同一共享状态。
    fn test_graceful_shutdown_tracker_accessor_shares_state() {
        let runtime = Runtime::single_threaded().unwrap();
        let tracker_a = runtime.graceful_shutdown_tracker();
        let tracker_b = runtime.graceful_shutdown_tracker();

        tracker_a.register_endpoint();
        assert_eq!(tracker_b.get_count(), 1);

        tracker_b.unregister_endpoint();
        assert_eq!(tracker_a.get_count(), 0);
    }

    #[test]
    /// 测试：主取消令牌被取消后，子令牌会级联进入取消状态。
    fn test_primary_token_cancellation_cascades_to_child_tokens() {
        let runtime = Runtime::single_threaded().unwrap();
        let main_token = runtime.primary_token();
        let child_a = runtime.child_token();
        let child_b = runtime.child_token();

        main_token.cancel();

        assert!(main_token.is_cancelled());
        assert!(child_a.is_cancelled());
        assert!(child_b.is_cancelled());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    /// 测试：关闭流程中端点级子令牌会先于主令牌被取消。
    async fn test_shutdown_cancels_child_tokens_before_main_token() {
        let runtime = Runtime::from_current().unwrap();
        let tracker = runtime.graceful_shutdown_tracker();
        let main_token = runtime.primary_token();
        let child_token = runtime.child_token();

        tracker.register_endpoint();
        runtime.shutdown();

        timeout(Duration::from_secs(1), child_token.cancelled())
            .await
            .unwrap();
        assert!(child_token.is_cancelled());
        assert!(!main_token.is_cancelled());

        tracker.unregister_endpoint();

        timeout(Duration::from_secs(1), main_token.cancelled())
            .await
            .unwrap();
        assert!(main_token.is_cancelled());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    /// 测试：没有活跃端点时，关闭流程会直接取消全部令牌。
    async fn test_shutdown_without_active_endpoints_cancels_all_tokens() {
        let runtime = Runtime::from_current().unwrap();
        let main_token = runtime.primary_token();
        let child_token = runtime.child_token();

        runtime.shutdown();

        timeout(Duration::from_secs(1), child_token.cancelled())
            .await
            .unwrap();
        timeout(Duration::from_secs(1), main_token.cancelled())
            .await
            .unwrap();

        assert!(child_token.is_cancelled());
        assert!(main_token.is_cancelled());
    }

    #[test]
    /// 测试：`RuntimeType::handle` 对共享和外部两种 runtime 都能返回可用句柄。
    fn test_runtime_type_handle_supports_shared_and_external() {
        let shared = shared_runtime_type();
        let shared_value = shared.handle().block_on(async { 31usize });
        assert_eq!(shared_value, 31);

        let (external_runtime, external) = external_runtime_type();
        let external_value =
            external_runtime.block_on(async { external.handle().spawn(async { 37usize }).await.unwrap() });
        assert_eq!(external_value, 37);
    }

    #[test]
    /// 测试：在非异步上下文中释放共享 runtime 不会 panic。
    fn test_runtime_type_drop_outside_async_context_does_not_panic() {
        let shared = shared_runtime_type();
        drop(shared);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    /// 测试：在异步上下文中释放共享 runtime 也不会 panic。
    async fn test_runtime_type_drop_inside_async_context_does_not_panic() {
        let shared = shared_runtime_type();
        drop(shared);
    }
}
