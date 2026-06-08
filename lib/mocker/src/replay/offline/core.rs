// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-License-Identifier: Apache-2.0

//! # 重放 worker 内核包装
//!
//! ## 设计意图
//! 封装底层引擎内核（如 VllmCore），为离线重放提供统一的接收/执行/查询接口。
//!
//! ## 外部契约
//! 提供 `ReplayWorkerCore`，通过 `new`/`new_with_kv_capture`/`receive`/`execute_pass` 等方法代理内核，行为与 Dynamo 一致。

use crate::common::protocols::MockEngineArgs;
use crate::replay::TraceCollector;
use crate::scheduler::{EngineCore, EnginePassResult, VllmCore};
use pagoda_kv_router::protocols::WorkerId;

pub(crate) struct ReplayWorkerCore {
    core: EngineCore,
}

impl ReplayWorkerCore {
    pub(crate) fn new(args: MockEngineArgs) -> Self {
        let core = match args.engine_type {
            crate::common::protocols::EngineType::Vllm => {
                let core = VllmCore::new(args);
                EngineCore::Vllm(core)
            }
        };
        Self { core }
    }

    pub(crate) fn new_with_kv_capture(args: MockEngineArgs, worker_id: WorkerId) -> Self {
        let core = match args.engine_type {
            crate::common::protocols::EngineType::Vllm => {
                let core = VllmCore::new_with_kv_capture(args, worker_id);
                EngineCore::Vllm(core)
            }
        };
        Self { core }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.core.is_empty()
    }

    pub(crate) fn receive(
        &mut self,
        request: crate::common::protocols::DirectRequest,
    ) -> uuid::Uuid {
        self.core.receive(request)
    }

    pub(crate) fn num_requests(&self) -> usize {
        self.core.num_requests()
    }

    pub(crate) fn execute_pass(
        &mut self,
        collector: &mut TraceCollector,
        now_ms: f64,
    ) -> EnginePassResult {
        self.core.execute_pass(collector, now_ms)
    }
}
