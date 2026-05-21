// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 进程入口与生命周期管理。
//!
//! `Worker` 封装 Tokio Runtime 的创建、OS 信号处理、优雅关闭超时逻辑，
//! 使用户的 `main()` 只需 `Worker::from_settings()?.execute(|rt| async { ... })`。

use std::future::Future;
use std::mem::ManuallyDrop;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use once_cell::sync::OnceCell;
use tokio::runtime as tokio_rt;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::{RuntimeConfig, WorkerConfig};
use crate::runtime::Runtime;

// ── 进程内全局唯一性保证 ─────────────────────────────────────────────

/// 进程唯一 Tokio Runtime 本体（ManuallyDrop 防止 async 上下文 drop panic）。
static RT: OnceCell<Arc<ManuallyDrop<tokio_rt::Runtime>>> = OnceCell::new();
/// 进程内已发布的 Tokio Handle（用于 has_existing_runtime 检测）。
static RTHANDLE: OnceCell<tokio_rt::Handle> = OnceCell::new();
/// 用户应用任务句柄（只允许被消费一次）。
static INIT: OnceCell<Mutex<Option<JoinHandle<anyhow::Result<()>>>>> = OnceCell::new();

// ── Worker ───────────────────────────────────────────────────────

/// Pagoda 进程入口，管理整个进程生命周期。
///
/// 未来可能演化为 `#[pagoda::main]` 过程宏。
#[derive(Debug, Clone)]
pub struct Worker {
    config: RuntimeConfig,
    worker_config: WorkerConfig,
    runtime: Runtime,
}

impl Worker {
    /// 从环境变量和配置文件构建 Worker。
    pub fn from_settings() -> anyhow::Result<Self> {
        let config = RuntimeConfig::from_settings()?;
        Self::from_config(config)
    }

    /// 从指定配置构建 Worker（进程唯一性由 OnceCell 保证）。
    pub fn from_config(config: RuntimeConfig) -> anyhow::Result<Self> {
        // 快速失败检查（非竞争场景）
        if RT.get().is_some() || RTHANDLE.get().is_some() {
            anyhow::bail!("Worker already initialized — only one Worker per process is allowed");
        }

        // 创建 Tokio Runtime
        let tokio_rt_inst = config.create_runtime()?;
        let handle = tokio_rt_inst.handle().clone();
        let rt_arc = Arc::new(ManuallyDrop::new(tokio_rt_inst));

        // 通过 OnceCell 保证竞争安全（set 失败 = 其他线程抢先）
        RT.set(Arc::clone(&rt_arc))
            .map_err(|_| anyhow::anyhow!("Worker already initialized (concurrent attempt)"))?;
        RTHANDLE
            .set(handle)
            .map_err(|_| anyhow::anyhow!("Worker RTHANDLE already set"))?;

        let runtime = Runtime::new_shared(rt_arc, &config)?;
        let worker_config = WorkerConfig::default();

        Ok(Self {
            config,
            worker_config,
            runtime,
        })
    }

    /// 复用调用方已有的 Tokio runtime（嵌入式场景 / Python bindings）。
    ///
    /// 不向全局 RT 注册，仅借用当前上下文的 Handle。
    pub fn from_current() -> anyhow::Result<Self> {
        let config = RuntimeConfig::from_settings()?;
        let worker_config = WorkerConfig::default();
        let runtime = Runtime::from_current()?;
        Ok(Self {
            config,
            worker_config,
            runtime,
        })
    }

    // ── 执行入口 ─────────────────────────────────────────────────────

    /// 启动用户应用，**阻塞** main 线程直到应用完成或收到终止信号。
    ///
    /// 内部在 secondary pool 上等待；primary pool 运行用户 app_fn。
    pub fn execute<F, Fut>(self, f: F) -> anyhow::Result<()>
    where
        F: FnOnce(Runtime) -> Fut + Send + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let secondary = self.runtime.secondary();
        let join_handle = self.execute_internal(f);

        // main 线程在 secondary pool 上阻塞等待
        secondary.block_on(async move {
            join_handle.await.unwrap_or_else(|e| {
                Err(anyhow::anyhow!("Worker task panicked: {e}"))
            })
        })
    }

    /// 供已在异步上下文中使用（Python bindings）。
    pub async fn execute_async<F, Fut>(self, f: F) -> anyhow::Result<()>
    where
        F: FnOnce(Runtime) -> Fut + Send + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let join_handle = self.execute_internal(f);
        join_handle.await.unwrap_or_else(|e| {
            Err(anyhow::anyhow!("Worker task panicked: {e}"))
        })
    }

    /// 核心实现：在 secondary pool 上编排信号处理 + 优雅关闭 + 用户应用执行。
    ///
    /// 返回值为应用任务的 JoinHandle，由调用方负责 await。
    fn execute_internal<F, Fut>(self, f: F) -> JoinHandle<anyhow::Result<()>>
    where
        F: FnOnce(Runtime) -> Fut + Send + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let runtime = self.runtime;
        // Phase 1 令牌：PortName 停止接受新请求
        let phase1_token = runtime.portname_shutdown_token().clone();
        // Phase 3 令牌：根令牌，后端连接断开
        let root_token = runtime.primary_token();
        let timeout_secs = self.worker_config.graceful_shutdown_timeout_secs;

        let primary = runtime.primary();
        let secondary = runtime.secondary();

        // secondary pool 上运行整个编排逻辑
        let handle = secondary.spawn(async move {
            // 初始化日志
            crate::logging::init();

            // 信号处理器触发 Phase 1 令牌（portname_shutdown_token），
            // runtime.shutdown() 会负责 Phase 2/3 协调。
            let signal_token = phase1_token.clone();
            tokio::spawn(signal_handler(signal_token));

            // oneshot 用于检测应用是否自行退出（_rx drop 时 tx.closed() 触发）
            let (mut tx, rx) = oneshot::channel::<()>();

            // 在 primary pool 上运行用户应用
            let app_task = primary.spawn(async move {
                let _rx = rx; // 保持 rx 存活，应用退出时自动 drop
                f(runtime).await
            });

            // 等待：收到关闭信号（Phase 3 根令牌）OR 应用自行退出
            tokio::select! {
                _ = root_token.cancelled() => {
                    tracing::debug!("Shutdown phase 3 triggered, waiting for application to finish");
                }
                _ = tx.closed() => {
                    tracing::debug!("Application exited on its own");
                }
            }

            // 等待应用任务完成，超时后强制退出
            let result = tokio::select! {
                result = app_task => {
                    result.unwrap_or_else(|e| Err(anyhow::anyhow!("App task panicked: {e}")))
                }
                _ = tokio::time::sleep(Duration::from_secs(timeout_secs)) => {
                    tracing::debug!("Application did not shutdown in time; terminating");
                    std::process::exit(911);
                }
            };

            result
        });

        // 将 JoinHandle 存入全局 INIT（只允许消费一次）
        INIT.get_or_init(|| Mutex::new(Some(handle)));

        // 取出 JoinHandle 并返回
        INIT.get()
            .expect("INIT should be set")
            .lock()
            .expect("INIT lock poisoned")
            .take()
            .expect("Worker.execute() can only be called once")
    }

    // ── 访问器 ───────────────────────────────────────────────────────

    pub fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    pub fn config(&self) -> &RuntimeConfig {
        &self.config
    }

    /// 返回进程级 Tokio runtime Arc 引用（若通过 from_config 创建则可用）。
    pub fn tokio_runtime() -> Option<Arc<ManuallyDrop<tokio_rt::Runtime>>> {
        RT.get().cloned()
    }

    /// 判断当前进程是否已存在运行时。
    pub fn has_existing_runtime() -> bool {
        RT.get().is_some() || RTHANDLE.get().is_some()
    }

    /// 从已有全局状态获取 Runtime（不触发新建），供 Python 绑定等静态访问使用。
    pub fn runtime_from_existing() -> anyhow::Result<Runtime> {
        if let Some(rt) = RT.get() {
            Runtime::new_shared(Arc::clone(rt), &RuntimeConfig::from_settings()?)
        } else if let Some(handle) = RTHANDLE.get() {
            Runtime::from_handle(handle.clone())
        } else {
            // 兜底：从当前上下文构建，并回填 RTHANDLE
            let runtime = Runtime::from_current()?;
            let _ = RTHANDLE.set(runtime.primary());
            Ok(runtime)
        }
    }
}

// ── 信号处理器 ───────────────────────────────────────────────────

/// 监听 SIGINT / SIGTERM / 程序内部取消，统一触发 cancel_token。
async fn signal_handler(cancel_token: CancellationToken) -> anyhow::Result<()> {
    use tokio::signal;

    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate())?;

        tokio::select! {
            result = signal::ctrl_c() => {
                result?;
                tracing::debug!("Received SIGINT (Ctrl+C)");
            }
            _ = sigterm.recv() => {
                tracing::debug!("Received SIGTERM");
            }
            _ = cancel_token.cancelled() => {
                tracing::debug!("Cancellation token triggered internally");
            }
        }
    }

    #[cfg(not(unix))]
    {
        tokio::select! {
            result = signal::ctrl_c() => {
                result?;
                tracing::debug!("Received Ctrl+C");
            }
            _ = cancel_token.cancelled() => {
                tracing::debug!("Cancellation token triggered internally");
            }
        }
    }

    cancel_token.cancel();
    Ok(())
}
