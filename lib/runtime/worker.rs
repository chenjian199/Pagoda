// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 进程级运行入口：[`Worker`] 封装 [`Runtime`] 构造与应用执行
//!
//! ## 设计意图
//! [`Worker`] 是对"创建 Tokio runtime → 启动用户应用 → 处理关闭信号"全过程的
//! 便捷封装。它对应于其他生态中的 `#[tokio::main]`：调用方只需要在 `main`
//! 中调用一次 [`Worker::execute`]，即可获得：
//! * 全局唯一、可被进程内任意位置复用的 Tokio runtime；
//! * 自动注册的 `SIGINT` / `SIGTERM` 信号处理与统一取消令牌；
//! * 由环境变量 `DYN_WORKER_GRACEFUL_SHUTDOWN_TIMEOUT` 控制的优雅关闭窗口，
//!   超时则以 exit code 911 强制退出。
//!
//! 默认超时在 debug 与 release 构建下分别为
//! [`DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_DEBUG`] 与 [`DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_RELEASE`]。
//!
//! ## 外部契约
//! - 公开结构体 `Worker`（`Debug + Clone`）与公开常量
//!   `DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_DEBUG` / `DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_RELEASE` 不变。
//! - 公开方法集合 `from_settings` / `from_config` / `from_current` / `runtime_from_existing` /
//!   `has_existing_runtime` / `tokio_runtime` / `runtime` / `execute` / `execute_async`
//!   的签名、错误信息（"Worker already initialized" / "Worker not initialized"）与行为不变。
//! - 全局 `OnceCell` `RT` / `RTHANDLE` / `INIT` 的初始化与单例语义保持原状：
//!   `from_*` 互斥、`runtime_from_existing` 在两者皆空时回退到 `Runtime::from_settings`
//!   并把主句柄写入 `RTHANDLE`。
//! - `execute` 收到取消信号后必须在 `timeout` 秒内等待应用任务完成，超时则
//!   `std::process::exit(911)`；所有日志 message 与历史实现一致。
//!
//! ## 实现要点
//! - 子进程式测试：所有依赖全局 OnceCell 的用例都走 `subprocess_*` helper，
//!   以隔离的子进程方式运行，避免单元测试之间互相污染单例。
//! - 信号处理改用私有枚举 `SignalSource` 取代历史实现中的 `u8` 编码，让 `select!`
//!   直接产出语义化结果，消除"码值 → 日志"的隐式映射。
//! - 关闭流程在 `secondary` runtime 上以单一 `spawn` 完成，最终 JoinHandle
//!   存入 `INIT` 单例，确保 `execute` 在多次调用时只能由一处消费。

use super::{CancellationToken, Runtime, RuntimeConfig};

use futures::Future;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use std::time::Duration;
use tokio::{signal, task::JoinHandle};

static RT: OnceCell<tokio::runtime::Runtime> = OnceCell::new();
static RTHANDLE: OnceCell<tokio::runtime::Handle> = OnceCell::new();
static INIT: OnceCell<Mutex<Option<tokio::task::JoinHandle<anyhow::Result<()>>>>> = OnceCell::new();

use crate::config::environment_names::worker as env_worker;

// === SECTION: 全局单例与常量 ===

const SHUTDOWN_MESSAGE: &str =
    "Application received shutdown signal; attempting to gracefully shutdown";
const SHUTDOWN_TIMEOUT_MESSAGE: &str =
    "Use DYN_WORKER_GRACEFUL_SHUTDOWN_TIMEOUT to control the graceful shutdown timeout";

/// Default graceful shutdown timeout in seconds in debug mode
pub const DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_DEBUG: u64 = 5;

/// Default graceful shutdown timeout in seconds in release mode
pub const DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_RELEASE: u64 = 30;

// === SECTION: Worker 结构 & 构造 / 访问 / 执行接口 ===

#[derive(Debug, Clone)]
pub struct Worker {
    runtime: Runtime,
    config: RuntimeConfig,
}

impl Worker {
    /// Create a new [Worker] instance from [RuntimeConfig] settings which is sourced from the environment
    // 中文说明：
    // 1. 这个函数从环境配置创建一个新的 Worker。
    // 2. 它先调用 RuntimeConfig::from_settings 读取当前进程里的运行时配置。
    // 3. 随后把配置继续交给 from_config，复用统一的 Worker 构造逻辑。
    pub fn from_settings() -> anyhow::Result<Worker> {
        let config = RuntimeConfig::from_settings()?;
        let worker = Self::from_config(config)?;

        Ok(worker)
    }

    /// Create a new [Worker] instance from a provided [RuntimeConfig]
    // 中文说明：
    // 1. 这个函数根据传入的 RuntimeConfig 真正构造 Worker。
    // 2. 开始会先检查全局 OnceCell，避免在同一进程里重复初始化多个 Worker/runtime。
    // 3. 如果尚未初始化，就按配置创建 Tokio runtime，并尝试写入全局 RT 单例。
    // 4. 写入成功后，再从该 runtime 句柄构造 Runtime 包装对象。
    // 5. 最终把 Runtime 和原始配置一起组装进 Worker 返回。
    pub fn from_config(config: RuntimeConfig) -> anyhow::Result<Worker> {
        let already_initialized = RT.get().is_some() || RTHANDLE.get().is_some();
        if already_initialized {
            return Err(anyhow::anyhow!("Worker already initialized"));
        }

        let created_runtime = config.create_runtime()?;
        let inserted_runtime = RT.try_insert(created_runtime).map_err(|_| {
            anyhow::anyhow!("Failed to create worker; Only a single Worker should ever be created")
        })?;
        let runtime_handle = inserted_runtime.handle().clone();
        let runtime = Runtime::from_handle(runtime_handle)?;
        let worker = Worker { runtime, config };

        Ok(worker)
    }

    // 中文说明：
    // 1. 这个函数尝试从当前进程里已经存在的 runtime 句柄恢复一个 Runtime 对象。
    // 2. 如果全局 RT 已经存在，就直接复用它的 handle。
    // 3. 如果只有 RTHANDLE 已存在，就从那个全局 handle 构造 Runtime。
    // 4. 如果两者都不存在，则回退到 Runtime::from_settings 创建一个新的 Runtime，并把它的主句柄发布到 RTHANDLE。
    // 5. 无论走哪条路径，最后都返回一个可继续使用的 Runtime 包装对象。
    pub fn runtime_from_existing() -> anyhow::Result<Runtime> {
        let runtime = if let Some(existing_runtime) = RT.get() {
            let handle = existing_runtime.handle().clone();
            Runtime::from_handle(handle)?
        } else if let Some(existing_handle) = RTHANDLE.get() {
            Runtime::from_handle(existing_handle.clone())?
        } else {
            let runtime = Runtime::from_settings()?;
            let primary_handle = runtime.primary();
            let _ = RTHANDLE.set(primary_handle);
            runtime
        };

        Ok(runtime)
    }

    /// Whether a process-wide runtime has already been initialized
    /// (RT populated by Worker::from_*, or RTHANDLE populated by
    /// runtime_from_existing's fallback / external callers).
    /// Does not fall back to Runtime::from_settings().
    // 中文说明：
    // 1. 这个函数只做一个轻量查询，用来判断当前进程里是否已经存在全局 runtime。
    // 2. 它分别检查真正的 Tokio runtime 单例 RT 和单独保存的 handle 单例 RTHANDLE。
    // 3. 任意一个存在，就返回 true；否则返回 false。
    pub fn has_existing_runtime() -> bool {
        let has_runtime = RT.get().is_some();
        let has_runtime_handle = RTHANDLE.get().is_some();

        has_runtime || has_runtime_handle
    }

    // 中文说明：
    // 1. 这个函数返回当前 Worker 绑定的底层 Tokio runtime 静态引用。
    // 2. 它先从全局 RT 单例里读取运行时对象。
    // 3. 如果 Worker 尚未初始化导致 RT 为空，就返回一个明确的错误；否则返回 runtime 引用。
    pub fn tokio_runtime(&self) -> anyhow::Result<&'static tokio::runtime::Runtime> {
        let runtime = RT.get();

        runtime.ok_or_else(|| anyhow::anyhow!("Worker not initialized"))
    }

    // 中文说明：
    // 1. 这个函数把 Worker 内部保存的 Runtime 借给调用方使用。
    // 2. 返回的是引用而不是克隆，因此不会创建新的 Runtime 实例。
    pub fn runtime(&self) -> &Runtime {
        let runtime = &self.runtime;
        runtime
    }

    // 中文说明：
    // 1. 这个函数以阻塞方式执行用户提供的应用逻辑。
    // 2. 它先克隆一份 Runtime，避免后续 move 影响关闭流程。
    // 3. 然后调用 execute_internal 创建实际执行任务，并在 secondary runtime 上阻塞等待它结束。
    // 4. 用户任务完成后，函数会触发 runtime.shutdown 做收尾清理。
    // 5. 整个流程成功结束时返回 Ok(())。
    pub fn execute<F, Fut>(self, f: F) -> anyhow::Result<()>
    where
        F: FnOnce(Runtime) -> Fut + Send + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let runtime = self.runtime.clone();
        let execution = self.execute_internal(f);
        let execution_result = runtime.secondary().block_on(execution);

        execution_result??;
        runtime.shutdown();

        Ok(())
    }

    // 中文说明：
    // 1. 这个函数是 execute 的异步版本，用于已经处在异步上下文中的调用场景。
    // 2. 它同样会先准备一份 Runtime 克隆，并通过 execute_internal 创建实际执行任务。
    // 3. 区别在于这里直接 await 任务结果，而不是用 block_on 阻塞线程。
    // 4. 任务完成后依旧会执行 runtime.shutdown，保证关闭流程一致。
    pub async fn execute_async<F, Fut>(self, f: F) -> anyhow::Result<()>
    where
        F: FnOnce(Runtime) -> Fut + Send + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let runtime = self.runtime.clone();
        let execution = self.execute_internal(f);
        let execution_result = execution.await;

        execution_result??;
        runtime.shutdown();

        Ok(())
    }

    /// Executes the provided application/closure on the [Runtime].
    /// This is designed to be called once from main and will block the calling thread until the application completes.
    // 中文说明：
    // 1. 这个内部函数负责真正创建并返回执行用户应用的 JoinHandle。
    // 2. 它先拆出 primary 和 secondary 两个执行句柄，再计算优雅关闭超时时间；该时间优先取环境变量，否则根据 debug/release 默认值决定。
    // 3. 随后在 secondary runtime 上启动一个异步任务，这个任务会先拉起信号处理器，再创建子取消令牌与应用执行任务。
    // 4. 应用任务运行期间，代码会等待两类事件：一类是取消令牌触发，另一类是应用侧 oneshot 通道关闭。
    // 5. 一旦进入收尾阶段，又会在“应用任务自然结束”和“超过优雅关闭超时”之间做一次 select；若超时则直接退出进程。
    // 6. 应用任务结束后会记录成功或失败日志，并把结果继续向上传递。
    // 7. 最后该 JoinHandle 会被放入 INIT 单例中，只允许 Worker.execute 流程消费一次，避免并发重复等待。
    fn execute_internal<F, Fut>(self, f: F) -> JoinHandle<anyhow::Result<()>>
    where
        F: FnOnce(Runtime) -> Fut + Send + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let runtime = self.runtime.clone();
        let primary = runtime.primary();
        let secondary = runtime.secondary();

        let default_timeout = if cfg!(debug_assertions) {
            DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_DEBUG
        } else {
            DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_RELEASE
        };
        let configured_timeout = std::env::var(env_worker::DYN_WORKER_GRACEFUL_SHUTDOWN_TIMEOUT)
            .ok()
            .and_then(|value| value.parse::<u64>().ok());
        let timeout = configured_timeout.unwrap_or(default_timeout);
        let shutdown_task = secondary.spawn(async move {
            tokio::spawn(signal_handler(runtime.primary_token().clone()));

            let cancel_token = runtime.child_token();
            let (mut app_tx, app_rx) = tokio::sync::oneshot::channel::<()>();
            let app_task: JoinHandle<anyhow::Result<()>> = primary.spawn(async move {
                let _rx = app_rx;
                f(runtime).await
            });

            tokio::select! {
                _ = cancel_token.cancelled() => {
                    tracing::debug!("{SHUTDOWN_MESSAGE}");
                    tracing::debug!("{} {} seconds", SHUTDOWN_TIMEOUT_MESSAGE, timeout);
                }
                _ = app_tx.closed() => {}
            };

            let timeout_duration = Duration::from_secs(timeout);
            let join_result = tokio::select! {
                result = app_task => result,
                _ = tokio::time::sleep(timeout_duration) => {
                    tracing::debug!("Application did not shutdown in time; terminating");
                    std::process::exit(911);
                }
            }?;

            match &join_result {
                Ok(_) => tracing::debug!("Application shutdown successfully"),
                Err(error) => tracing::error!("Application shutdown with error: {:?}", error),
            }

            join_result
        });

        let init_task = Mutex::new(Some(shutdown_task));
        INIT.set(init_task)
            .expect("Failed to spawn application task");

        let init = INIT.get().expect("Application task not initialized");
        let mut guard = init.lock();

        guard.take().expect(
            "Application initialized; but another thread is awaiting it; Worker.execute() can only be called once",
        )
    }

    // 中文说明：
    // 1. 这个函数用于在当前线程已经处于 Tokio runtime 内部时创建 Worker。
    // 2. 它仍然会先检查全局初始化状态，防止重复创建 Worker。
    // 3. 然后通过 Runtime::from_current 绑定当前运行中的 runtime，并读取环境配置。
    // 4. 最后把这两部分组装成 Worker 返回。
    pub fn from_current() -> anyhow::Result<Worker> {
        let already_initialized = RT.get().is_some() || RTHANDLE.get().is_some();
        if already_initialized {
            return Err(anyhow::anyhow!("Worker already initialized"));
        }

        let runtime = Runtime::from_current()?;
        let config = RuntimeConfig::from_settings()?;
        let worker = Worker { runtime, config };

        Ok(worker)
    }
}

// === SECTION: 关闭信号监听 ===

/// 关闭事件来源的语义化枚举：取代历史实现里的 `u8` 魔术码。
///
/// 三类来源各自对应一条日志，便于排障时区分是外部信号还是内部取消。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SignalSource {
    CtrlC,
    SigTerm,
    Cancelled,
}

impl SignalSource {
    /// 输出与历史实现一致的日志文本，避免日志格式发生漂移。
    fn log(self) {
        match self {
            SignalSource::CtrlC => {
                tracing::info!("Ctrl+C received, starting graceful shutdown")
            }
            SignalSource::SigTerm => {
                tracing::info!("SIGTERM received, starting graceful shutdown")
            }
            SignalSource::Cancelled => {
                tracing::debug!("CancellationToken triggered; shutting down")
            }
        }
    }
}

/// 监听进程级关闭信号并把它转换为 Runtime 的取消信号。
///
/// 中文说明：
/// 1. 分别构造 `Ctrl+C` 与 `SIGTERM` 的等待 future；两者均保留
///    "在 future 内部完成信号注册"的原始时序，错误传播路径与历史实现一致。
/// 2. `tokio::select!` 同时等待 Ctrl+C、SIGTERM 与外部 `CancellationToken`
///    三种事件；select 直接返回 `SignalSource` 枚举，取代历史实现里的
///    `u8` 临时编码 + `match` 解码。
/// 3. 通过 `SignalSource::log` 输出与历史实现一致的日志文本。
/// 4. 无论命中哪个分支，最终都调用 `cancel_token.cancel()` 触发统一的关闭链路。
async fn signal_handler(cancel_token: CancellationToken) -> anyhow::Result<()> {
    let ctrl_c = async {
        signal::ctrl_c().await?;
        anyhow::Ok(())
    };

    let sigterm = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())?
            .recv()
            .await;
        anyhow::Ok(())
    };

    let source = tokio::select! {
        _ = ctrl_c => SignalSource::CtrlC,
        _ = sigterm => SignalSource::SigTerm,
        _ = cancel_token.cancelled() => SignalSource::Cancelled,
    };

    source.log();

    cancel_token.cancel();

    Ok(())
}


// === SECTION: 单元测试 ===

#[cfg(test)]
mod tests {
    //! ## 测试过程
    //! 所有触碰全局 `RT` / `RTHANDLE` / `INIT` 单例的用例都通过 `run_worker_subprocess`
    //! 在隔离的子进程里运行（实际执行体即标记为 `#[ignore]` 的 `subprocess_*` 帮助函数），
    //! 避免单元测试之间因共享 OnceCell 而互相污染；`signal_handler` 的取消路径
    //! 则以 `tokio::test` 直接驱动 `CancellationToken::cancel`。
    //!
    //! ## 意义
    //! 这些用例固定了 Worker 的对外契约：
    //! * 重复初始化必须返回 "Worker already initialized" 错误；
    //! * `runtime_from_existing` 在两个单例都为空时会回退到 `Runtime::from_settings`
    //!   并把主句柄写入 `RTHANDLE`；
    //! * `execute` / `execute_async` / `execute_internal` 在用户任务结束后
    //!   会触发 `runtime.shutdown` 并使主取消令牌生效。
    //!
    //! 本次重构（中文文档化、`SignalSource` 枚举、SECTION 归并）必须保留这些断言全部为真。

    use super::*;
    use std::process::Command;
    use std::sync::{Arc, Mutex as StdMutex};

    fn run_worker_subprocess(test_name: &str) {
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test_name, "--ignored", "--nocapture"])
            .output()
            .unwrap();

        assert!(
            output.status.success(),
            "subprocess test failed: {}\nstdout:\n{}\nstderr:\n{}",
            test_name,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    #[test]
    fn test_from_settings_and_accessors_via_subprocess() {
        run_worker_subprocess(
            "worker::tests::subprocess_from_settings_and_accessors",
        );
    }

    #[test]
    fn test_from_config_and_duplicate_init_errors_via_subprocess() {
        run_worker_subprocess(
            "worker::tests::subprocess_from_config_and_duplicate_init_errors",
        );
    }

    #[test]
    fn test_runtime_from_existing_fallback_via_subprocess() {
        run_worker_subprocess(
            "worker::tests::subprocess_runtime_from_existing_fallback",
        );
    }

    #[test]
    fn test_from_current_success_via_subprocess() {
        run_worker_subprocess(
            "worker::tests::subprocess_from_current_success",
        );
    }

    #[test]
    fn test_execute_success_via_subprocess() {
        run_worker_subprocess("worker::tests::subprocess_execute_success");
    }

    #[test]
    fn test_execute_async_success_via_subprocess() {
        run_worker_subprocess("worker::tests::subprocess_execute_async_success");
    }

    #[test]
    fn test_execute_internal_success_via_subprocess() {
        run_worker_subprocess(
            "worker::tests::subprocess_execute_internal_success",
        );
    }

    #[tokio::test]
    async fn test_signal_handler_cancellation_path() {
        let token = CancellationToken::new();
        let handler = tokio::spawn(signal_handler(token.clone()));

        token.cancel();

        let result = handler.await.unwrap();
        assert!(result.is_ok());
        assert!(token.is_cancelled());
    }

    #[test]
    #[ignore = "subprocess helper"]
    fn subprocess_from_settings_and_accessors() {
        use crate::config::environment_names::runtime;
        use crate::config::environment_names::runtime::system;

        temp_env::with_vars(
            vec![
                (runtime::DYN_RUNTIME_NUM_WORKER_THREADS, Some("1")),
                (runtime::DYN_RUNTIME_MAX_BLOCKING_THREADS, Some("1")),
                (system::DYN_SYSTEM_PORT, Some("-1")),
            ],
            || {
                assert!(!Worker::has_existing_runtime());

                let worker = Worker::from_settings().unwrap();

                assert!(Worker::has_existing_runtime());
                assert_eq!(worker.config.system_port, -1);
                assert_eq!(worker.config.num_worker_threads, Some(1));
                assert_eq!(worker.config.max_blocking_threads, 1);
                assert!(std::ptr::eq(worker.tokio_runtime().unwrap(), RT.get().unwrap()));
                assert!(!worker.runtime().primary_token().is_cancelled());

                let from_existing = Worker::runtime_from_existing().unwrap();
                let value = from_existing.primary().block_on(async { 17usize });
                assert_eq!(value, 17);
            },
        );
    }

    #[test]
    #[ignore = "subprocess helper"]
    fn subprocess_from_config_and_duplicate_init_errors() {
        let config = RuntimeConfig::single_threaded();
        assert!(!Worker::has_existing_runtime());

        let worker = Worker::from_config(config.clone()).unwrap();

        assert!(Worker::has_existing_runtime());
        assert_eq!(worker.config.system_port, config.system_port);
        assert_eq!(worker.config.num_worker_threads, config.num_worker_threads);
        assert_eq!(worker.config.max_blocking_threads, config.max_blocking_threads);
        assert!(std::ptr::eq(worker.tokio_runtime().unwrap(), RT.get().unwrap()));
        assert!(!worker.runtime().primary_token().is_cancelled());

        let duplicate_from_config = Worker::from_config(RuntimeConfig::single_threaded())
            .unwrap_err()
            .to_string();
        assert!(duplicate_from_config.contains("Worker already initialized"));

        let duplicate_from_settings = Worker::from_settings().unwrap_err().to_string();
        assert!(duplicate_from_settings.contains("Worker already initialized"));

        let duplicate_from_current = Worker::from_current().unwrap_err().to_string();
        assert!(duplicate_from_current.contains("Worker already initialized"));
    }

    #[test]
    #[ignore = "subprocess helper"]
    fn subprocess_runtime_from_existing_fallback() {
        assert!(!Worker::has_existing_runtime());
        assert!(RT.get().is_none());
        assert!(RTHANDLE.get().is_none());

        let runtime1 = Worker::runtime_from_existing().unwrap();
        assert!(Worker::has_existing_runtime());
        assert!(RT.get().is_none());
        assert!(RTHANDLE.get().is_some());

        let value1 = runtime1.primary().block_on(async { 23usize });
        assert_eq!(value1, 23);

        let runtime2 = Worker::runtime_from_existing().unwrap();
        let value2 = runtime2.primary().block_on(async { 29usize });
        assert_eq!(value2, 29);

        let from_config_err = Worker::from_config(RuntimeConfig::single_threaded())
            .unwrap_err()
            .to_string();
        assert!(from_config_err.contains("Worker already initialized"));

        let from_current_err = Worker::from_current().unwrap_err().to_string();
        assert!(from_current_err.contains("Worker already initialized"));
    }

    #[test]
    #[ignore = "subprocess helper"]
    fn subprocess_from_current_success() {
        use crate::config::environment_names::runtime::system;

        temp_env::with_vars(vec![(system::DYN_SYSTEM_PORT, Some("-1"))], || {
            assert!(!Worker::has_existing_runtime());

            let external = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            external.block_on(async {
                let worker = Worker::from_current().unwrap();
                assert!(!Worker::has_existing_runtime());
                assert_eq!(worker.config.system_port, -1);
                assert!(worker.tokio_runtime().is_err());

                let handle = worker.runtime().primary();
                let value = handle.spawn(async { 31usize }).await.unwrap();
                assert_eq!(value, 31);
                assert!(!worker.runtime().primary_token().is_cancelled());
            });
        });
    }

    #[test]
    #[ignore = "subprocess helper"]
    fn subprocess_execute_success() {
        use crate::config::environment_names::worker as env_worker_test;

        temp_env::with_vars(
            vec![(env_worker_test::DYN_WORKER_GRACEFUL_SHUTDOWN_TIMEOUT, Some("1"))],
            || {
                let worker = Worker::from_config(RuntimeConfig::single_threaded()).unwrap();
                let token_slot = Arc::new(StdMutex::new(None::<CancellationToken>));
                let token_slot_closure = token_slot.clone();

                worker
                    .execute(move |runtime| {
                        let token_slot = token_slot_closure.clone();
                        async move {
                            *token_slot.lock().unwrap() = Some(runtime.primary_token());
                            tokio::task::yield_now().await;
                            Ok(())
                        }
                    })
                    .unwrap();

                let token = token_slot.lock().unwrap().clone().unwrap();
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(async {
                        tokio::time::timeout(Duration::from_secs(1), token.cancelled())
                            .await
                            .unwrap();
                    });
            },
        );
    }

    #[test]
    #[ignore = "subprocess helper"]
    fn subprocess_execute_async_success() {
        use crate::config::environment_names::worker as env_worker_test;

        temp_env::with_vars(
            vec![(env_worker_test::DYN_WORKER_GRACEFUL_SHUTDOWN_TIMEOUT, Some("1"))],
            || {
                let worker = Worker::from_config(RuntimeConfig::single_threaded()).unwrap();
                let token_slot = Arc::new(StdMutex::new(None::<CancellationToken>));
                let token_slot_closure = token_slot.clone();

                let external = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();

                external
                    .block_on(async move {
                        worker
                            .execute_async(move |runtime| {
                                let token_slot = token_slot_closure.clone();
                                async move {
                                    *token_slot.lock().unwrap() = Some(runtime.primary_token());
                                    tokio::task::yield_now().await;
                                    Ok(())
                                }
                            })
                            .await
                    })
                    .unwrap();

                let token = token_slot.lock().unwrap().clone().unwrap();
                external.block_on(async {
                    tokio::time::timeout(Duration::from_secs(1), token.cancelled())
                        .await
                        .unwrap();
                });
            },
        );
    }

    #[test]
    #[ignore = "subprocess helper"]
    fn subprocess_execute_internal_success() {
        use crate::config::environment_names::worker as env_worker_test;

        temp_env::with_vars(
            vec![(env_worker_test::DYN_WORKER_GRACEFUL_SHUTDOWN_TIMEOUT, Some("1"))],
            || {
                let worker = Worker::from_config(RuntimeConfig::single_threaded()).unwrap();
                let runtime = worker.runtime().clone();
                let token_slot = Arc::new(StdMutex::new(None::<CancellationToken>));
                let token_slot_closure = token_slot.clone();

                let handle = worker.execute_internal(move |runtime| {
                    let token_slot = token_slot_closure.clone();
                    async move {
                        *token_slot.lock().unwrap() = Some(runtime.primary_token());
                        Ok(())
                    }
                });

                let result = runtime.secondary().block_on(async { handle.await.unwrap() });
                assert!(result.is_ok());

                let token = token_slot.lock().unwrap().clone().unwrap();
                assert!(!token.is_cancelled());

                runtime.shutdown();
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(async {
                        tokio::time::timeout(Duration::from_secs(1), token.cancelled())
                            .await
                            .unwrap();
                    });
            },
        );
    }
}
