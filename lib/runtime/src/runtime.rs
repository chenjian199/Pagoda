// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 本地运行时封装：双 Tokio 线程池 + 取消令牌树 + 可选 Rayon 计算池。

use std::mem::ManuallyDrop;
use std::sync::Arc;

use tokio::runtime as tokio_rt;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::compute::ComputePool;
use crate::config::RuntimeConfig;
use crate::utils::graceful_shutdown::GracefulShutdownTracker;

// ─── RuntimeType ──────────────────────────────────────────────────

/// Tokio Runtime 所有权模型。
///
/// - `Shared`：由本 `Runtime` 自建，通过 `Arc<ManuallyDrop<...>>` 管理所有权。
///   ManuallyDrop 防止在 async 上下文中 drop 时 panic；Arc 防止多个 `RuntimeType`
///   clone 之间争抢 drop。
/// - `External`：借用外部 Handle，不负责底层 Runtime 生命周期（Python PyO3 嵌入等场景）。
enum RuntimeType {
    /// Worker 自建。
    Shared(Arc<ManuallyDrop<tokio_rt::Runtime>>),
    /// 借用外部 Handle，不负责 drop。
    External(tokio_rt::Handle),
}

impl RuntimeType {
    fn handle(&self) -> tokio_rt::Handle {
        match self {
            Self::Shared(rt) => rt.handle().clone(),
            Self::External(h) => h.clone(),
        }
    }

    fn is_shared(&self) -> bool {
        matches!(self, Self::Shared(_))
    }
}

impl Drop for RuntimeType {
    fn drop(&mut self) {
        if let Self::Shared(rt) = self {
            // 仅最后一个 Arc 持有者负责 drop；其余克隆放弃。
            if let Ok(owned) = Arc::try_unwrap(Arc::clone(rt)) {
                let runtime = ManuallyDrop::into_inner(owned);
                if tokio_rt::Handle::try_current().is_ok() {
                    // 在 async 上下文中直接 drop tokio::Runtime 会 panic，offload 到裸 OS 线程。
                    std::thread::spawn(move || drop(runtime));
                } else {
                    drop(runtime);
                }
            }
            // 其他 Arc 持有者存在时由最后一个处理。
        }
    }
}

// ─── Runtime ──────────────────────────────────────────────────────

/// Pagoda 本地运行时，封装双 Tokio 线程池、取消令牌树和计算池。
///
/// Clone 只增加 Arc 引用计数，可随意传入 `tokio::spawn`。
#[derive(Clone)]
pub struct Runtime {
    /// 运行时实例唯一 ID（UUID v4）。
    id: Arc<String>,
    /// 应用任务：请求处理、推理、API 服务。
    primary: Arc<RuntimeType>,
    /// 后台任务：etcd watch、NATS 心跳、服务发现。
    /// 嵌入场景下与 primary 共享同一 Tokio pool。
    secondary: Arc<RuntimeType>,

    /// 全局根取消令牌。
    cancellation_token: CancellationToken,
    /// PortName 关闭子令牌（Phase 1：停止接受新请求）。
    portname_shutdown_token: CancellationToken,
    /// 优雅关闭追踪器（Phase 2：等待 in-flight 请求完成）。
    graceful_shutdown_tracker: Arc<GracefulShutdownTracker>,

    /// 可选 Rayon CPU 计算池。
    compute_pool: Option<Arc<ComputePool>>,
    /// 限制 `block_in_place` 并发数量，防止所有 Tokio worker 线程被阻塞。
    block_in_place_permits: Option<Arc<tokio::sync::Semaphore>>,
}

impl Runtime {
    // ── 基础内部构造 ────────────────────────────────────────────────

    /// 从已创建好的 RuntimeType 构建 Runtime（不含 compute 初始化）。
    ///
    /// `secondary` 为 `None` 时自动创建单线程后台 runtime。
    fn new_from_type(primary: RuntimeType, secondary: Option<RuntimeType>) -> anyhow::Result<Self> {
        crate::timeline::init();

        let cancellation_token = CancellationToken::new();
        let portname_shutdown_token = cancellation_token.child_token();

        let secondary = secondary.unwrap_or_else(|| {
            let rt = tokio_rt::Builder::new_current_thread()
                .enable_all()
                .thread_name("pagoda-secondary")
                .build()
                .expect("failed to build secondary Tokio runtime");
            RuntimeType::Shared(Arc::new(ManuallyDrop::new(rt)))
        });

        Ok(Self {
            id: Arc::new(Uuid::new_v4().to_string()),
            primary: Arc::new(primary),
            secondary: Arc::new(secondary),
            cancellation_token,
            portname_shutdown_token,
            graceful_shutdown_tracker: Arc::new(GracefulShutdownTracker::new()),
            compute_pool: None,
            block_in_place_permits: None,
        })
    }

    /// 在 `new_from_type` 基础上按 `RuntimeConfig` 初始化计算池。
    fn new_with_config(
        primary: RuntimeType,
        secondary: Option<RuntimeType>,
        config: &RuntimeConfig,
    ) -> anyhow::Result<Self> {
        let mut rt = Self::new_from_type(primary, secondary)?;

        // Rayon 计算池
        if config.compute_threads != Some(0) {
            let mut cfg = crate::compute::ComputeConfig::default();
            cfg.num_threads = config.compute_threads;
            match ComputePool::new(cfg) {
                Ok(pool) => rt.compute_pool = Some(Arc::new(pool)),
                Err(e) => {
                    tracing::warn!(
                        "Failed to create ComputePool, falling back to spawn_blocking: {e}"
                    );
                }
            }
        }

        // block_in_place 许可数
        let num_workers = config.num_worker_threads.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        });
        let permits = num_workers.saturating_sub(1).max(1);
        rt.block_in_place_permits = Some(Arc::new(tokio::sync::Semaphore::new(permits)));

        Ok(rt)
    }

    // ── 对外构造接口 ────────────────────────────────────────────────

    /// 从当前 Tokio 上下文获取 Handle（嵌入场景 / 测试场景）。
    ///
    /// primary 与 secondary 共用同一个 Handle，不创建新线程池。
    pub fn from_current() -> anyhow::Result<Self> {
        let handle = tokio_rt::Handle::try_current()
            .map_err(|_| anyhow::anyhow!("No active Tokio runtime found"))?;
        Self::from_handle(handle)
    }

    /// 借用外部 Tokio Handle（Python PyO3 等嵌入场景）。
    pub fn from_handle(handle: tokio_rt::Handle) -> anyhow::Result<Self> {
        let primary = RuntimeType::External(handle.clone());
        let secondary = RuntimeType::External(handle);
        Self::new_from_type(primary, Some(secondary))
    }

    /// 从环境变量读取 `RuntimeConfig`，创建自有 primary + 后台 secondary runtime。
    pub fn from_settings() -> anyhow::Result<Self> {
        let config = RuntimeConfig::from_settings()?;
        Self::from_config(&config)
    }

    /// 按给定配置创建完整 Runtime（含 compute 池）。
    ///
    /// # 注意
    /// 此方法新建 Tokio runtime，调用方不应在已有 Tokio 上下文中使用。
    /// Worker::from_config 内部通过此接口创建运行时。
    pub fn from_config(config: &RuntimeConfig) -> anyhow::Result<Self> {
        let tokio_rt_inst = config.create_runtime()?;
        let primary = RuntimeType::Shared(Arc::new(ManuallyDrop::new(tokio_rt_inst)));
        Self::new_with_config(primary, None, config)
    }

    /// 由 Worker 调用：接管已创建的 Tokio Runtime Arc（避免重复创建）。
    pub(crate) fn new_shared(
        tokio_runtime: Arc<ManuallyDrop<tokio_rt::Runtime>>,
        config: &RuntimeConfig,
    ) -> anyhow::Result<Self> {
        let primary = RuntimeType::Shared(tokio_runtime);
        Self::new_with_config(primary, None, config)
    }

    // ── 线程池访问 ──────────────────────────────────────────────────

    /// 主 Tokio runtime handle（应用任务）。
    pub fn primary(&self) -> tokio_rt::Handle {
        self.primary.handle()
    }

    /// 后台 Tokio runtime handle（框架控制任务）。
    pub fn secondary(&self) -> tokio_rt::Handle {
        self.secondary.handle()
    }

    /// primary 是否为 Worker 自建（Shared 类型）。
    pub fn is_shared(&self) -> bool {
        self.primary.is_shared()
    }

    // ── 取消令牌 ────────────────────────────────────────────────────

    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.cancellation_token
    }

    /// 主令牌克隆，供后台任务监听关闭信号。
    pub fn primary_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    /// PortName 关闭子令牌（Phase 1：停止接受新请求）。
    pub fn portname_shutdown_token(&self) -> &CancellationToken {
        &self.portname_shutdown_token
    }

    /// 创建主令牌的子令牌（不影响主令牌本身）。
    pub fn child_token(&self) -> CancellationToken {
        self.cancellation_token.child_token()
    }

    /// 触发三阶段优雅关闭。
    ///
    /// 1. **Phase 1**：立即取消 `portname_shutdown_token`，各 PortName 停止接受新请求。
    /// 2. **Phase 2**：在 `primary` 上等待 `GracefulShutdownTracker` 计数归零（in-flight 请求完成）。
    /// 3. **Phase 3**：取消根 `cancellation_token`，断开 NATS/etcd 等后端连接。
    ///
    /// 方法立即返回；协调任务以 fire-and-forget 方式在 primary runtime 运行。
    pub fn shutdown(&self) {
        // Phase 1：立即停止接受新请求
        self.portname_shutdown_token.cancel();

        let tracker = self.graceful_shutdown_tracker.clone();
        let root_token = self.cancellation_token.clone();

        // Phase 2 + 3：在 primary 上协调
        self.primary().spawn(async move {
            // Phase 2：等待 in-flight 请求全部完成
            tracker.wait_for_completion().await;
            // Phase 3：断开后端连接
            root_token.cancel();
        });
    }

    // ── 关闭追踪 ────────────────────────────────────────────────────

    pub fn graceful_shutdown_tracker(&self) -> &Arc<GracefulShutdownTracker> {
        &self.graceful_shutdown_tracker
    }

    // ── 计算池 ──────────────────────────────────────────────────────

    pub fn compute_pool(&self) -> Option<&Arc<ComputePool>> {
        self.compute_pool.as_ref()
    }

    pub fn block_in_place_permits(&self) -> Option<&Arc<tokio::sync::Semaphore>> {
        self.block_in_place_permits.as_ref()
    }

    // ── 标识 ────────────────────────────────────────────────────────

    pub fn id(&self) -> &str {
        &self.id
    }

    // ── thread-local 预热 ───────────────────────────────────────────

    /// 在每个 Tokio worker 线程上预热 thread-local 引用，避免热路径锁竞争。
    pub fn initialize_all_thread_locals(&self) {
        if let Some(pool) = &self.compute_pool {
            let permits = Arc::new(tokio::sync::Semaphore::new(
                (pool.num_threads() / 2).max(1),
            ));
            crate::compute::thread_local::initialize_context(Arc::clone(pool), permits);
        }
    }
}

impl std::fmt::Debug for Runtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runtime")
            .field("id", &self.id.as_str())
            .field("is_shared", &self.is_shared())
            .finish_non_exhaustive()
    }
}
