// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-License-Identifier: Apache-2.0

//! # 离线重放引擎组件
//!
//! ## 设计意图
//! 管理一组离线 worker 的生命周期（创建、启动延迟、扩缩容与消资移除），驱动就绪 worker 执行 forward pass 并产出效果。
//!
//! ## 外部契约
//! 提供 `EngineComponent`，方法 `apply_target_count`/`drive_ready`/`on_scheduled_completion` 的返回值与错误文案与 Dynamo 保持一致。

use std::collections::{BTreeMap, BTreeSet};

use anyhow::bail;

use super::super::events::SimulationWorkerStage;
use super::super::runtime_utils::WorkerCompletionPayload;
#[cfg(test)]
use super::super::state::OfflineWorkerSnapshot;
use super::super::state::OfflineWorkerState;
use super::{EngineEffects, EnginePassMode, ScheduledWorkerCompletion};
use crate::common::protocols::{DirectRequest, MockEngineArgs};
use crate::replay::TraceCollector;
use crate::scheduler::RouterEventVisibility;

pub(in crate::replay::offline) struct EngineComponent {
    stage: SimulationWorkerStage,
    pass_mode: EnginePassMode,
    /// 以稳定 ID（单调递增、从不复用）为键的 worker。
    workers: BTreeMap<usize, OfflineWorkerState>,
    /// 用于生成下一个稳定 worker ID 的计数器。
    next_id: usize,
    /// 被标记为待移除的 worker —— 被轮询跳过，排空后移除。
    pending_removal: BTreeSet<usize>,
    /// 仍在启动中的 worker —— 在就绪前被排除在活跃集合之外。
    pending_startup: BTreeSet<usize>,
    /// 扩容时用于构造新 worker 的引擎参数。
    args: MockEngineArgs,
    /// 新 worker 是否应捕获 KV 事件（存在 router 时为 true）。
    capture_kv_events: bool,
}

impl EngineComponent {
    pub(in crate::replay::offline) fn new(
        stage: SimulationWorkerStage,
        pass_mode: EnginePassMode,
        workers: Vec<OfflineWorkerState>,
    ) -> Self {
        let count = workers.len();
        let map: BTreeMap<usize, OfflineWorkerState> = workers.into_iter().enumerate().collect();
        Self {
            stage,
            pass_mode,
            workers: map,
            next_id: count,
            pending_removal: BTreeSet::new(),
            pending_startup: BTreeSet::new(),
            args: MockEngineArgs::default(),
            capture_kv_events: false,
        }
    }

    /// 设置动态添加 worker 时使用的引擎参数与 KV 捕获标志。
    pub(in crate::replay::offline) fn set_scaling_args(
        &mut self,
        args: MockEngineArgs,
        capture_kv_events: bool,
    ) {
        self.args = args;
        self.capture_kv_events = capture_kv_events;
    }

    /// 添加一个新 worker，返回其稳定 ID。
    pub(in crate::replay::offline) fn add_worker(&mut self) -> usize {
        let id = self.next_id;
        self.next_id += 1;
        let worker = OfflineWorkerState::new(id, self.args.clone(), self.capture_kv_events);
        self.workers.insert(id, worker);
        id
    }

    /// 标记一个 worker 待移除。它会被 `drive_ready` 跳过，
    /// 并在完全排空后移除。
    pub(in crate::replay::offline) fn mark_for_removal(&mut self, worker_id: usize) {
        self.pending_removal.insert(worker_id);
    }

    /// 移除所有已完全排空的被标记 worker，返回其 ID。
    pub(in crate::replay::offline) fn try_remove_drained(&mut self) -> Vec<usize> {
        let mut removed = Vec::new();
        self.pending_removal.retain(|&id| {
            if let Some(worker) = self.workers.get(&id) {
                if worker.is_drained() {
                    removed.push(id);
                    return false; // 从待移除集合中删除
                }
            } else {
                // worker 已不存在
                return false;
            }
            true // 保留在待移除集合中
        });
        for &id in &removed {
            self.workers.remove(&id);
        }
        removed
    }

    /// 应用目标 worker 数量：添加新 worker 或将多余 worker 标记为待移除。
    /// 返回 `(added_ids, newly_marked_ids)`，使调用方可立即更新
    /// router。新标记的 worker 应立即从 router 移除，以防新请求
    /// 落在它们上，即使这些 worker 本身在完全排空前仍留在引擎中。
    ///
    /// 有效计数为 `active + pending_startup` —— 即所有启动完成后
    /// 将活跃的 worker。缩容时会先取消待启动 worker（最低成本：
    /// 无在飞工作、无 router 注册），再将活跃 worker 标记为待移除。
    pub(in crate::replay::offline) fn apply_target_count(
        &mut self,
        target: usize,
    ) -> (Vec<usize>, Vec<usize>) {
        let active_ids = self.active_worker_ids();
        let effective = active_ids.len() + self.pending_startup.len();
        let mut added = Vec::new();
        let mut newly_marked = Vec::new();

        if target > effective {
            let has_startup_delay = self.startup_time_ms().is_some();
            for _ in 0..(target - effective) {
                let id = self.add_worker();
                if has_startup_delay {
                    self.pending_startup.insert(id);
                }
                added.push(id);
            }
        } else if target < effective {
            let excess = effective - target;

            // 先取消待启动 worker（逆序 = 最高 ID）。
            let to_cancel: Vec<usize> = self
                .pending_startup
                .iter()
                .copied()
                .rev()
                .take(excess)
                .collect();
            for &id in &to_cancel {
                self.pending_startup.remove(&id);
                self.workers.remove(&id);
            }

            // 若仍有多余，则将活跃 worker 标记为待移除。
            let remaining = excess - to_cancel.len();
            for &id in active_ids.iter().rev().take(remaining) {
                self.mark_for_removal(id);
                newly_marked.push(id);
            }
        }

        // 清理任何已经完全排空的 worker。
        self.try_remove_drained();
        (added, newly_marked)
    }

    /// 返回所有活跃 worker 的稳定 ID —— 同时排除待移除
    /// 与待启动的 worker。
    pub(in crate::replay::offline) fn active_worker_ids(&self) -> Vec<usize> {
        self.workers
            .keys()
            .filter(|id| !self.pending_removal.contains(id) && !self.pending_startup.contains(id))
            .copied()
            .collect()
    }

    /// 返回配置的启动延迟（毫秒），若有。
    pub(in crate::replay::offline) fn startup_time_ms(&self) -> Option<f64> {
        self.args
            .startup_time
            .filter(|&s| s > 0.0)
            .map(|s| s * 1000.0)
    }

    /// 将一个待启动 worker 标记为就绪。若该 worker 确实处于
    /// 待启动状态（且现已活跃）则返回 `true`；若该 worker
    /// 已被取消或未知（陈旧事件）则返回 `false`。
    pub(in crate::replay::offline) fn mark_worker_ready(&mut self, worker_id: usize) -> bool {
        self.pending_startup.remove(&worker_id) && self.workers.contains_key(&worker_id)
    }

    pub(in crate::replay::offline) fn dispatch(
        &mut self,
        worker_id: usize,
        request: DirectRequest,
    ) -> anyhow::Result<()> {
        let worker = self
            .workers
            .get_mut(&worker_id)
            .ok_or_else(|| anyhow::anyhow!("offline replay selected unknown worker {worker_id}"))?;
        worker.receive_request(request);
        Ok(())
    }

    pub(in crate::replay::offline) fn drive_ready(
        &mut self,
        now_ms: f64,
        mut collector: Option<&mut TraceCollector>,
    ) -> anyhow::Result<EngineEffects> {
        // 先收集 worker ID 以避免借用冲突。
        let worker_ids: Vec<usize> = self.workers.keys().copied().collect();
        for worker_id in worker_ids {
            let worker = self.workers.get(&worker_id).unwrap();
            if !worker.is_ready() {
                continue;
            }

            let executed = match self.pass_mode {
                EnginePassMode::Visible => {
                    let Some(collector) = collector.as_deref_mut() else {
                        bail!("offline replay visible engine pass requires a collector");
                    };
                    self.workers
                        .get_mut(&worker_id)
                        .unwrap()
                        .execute_pass(collector, now_ms)
                }
                EnginePassMode::Hidden => self
                    .workers
                    .get_mut(&worker_id)
                    .unwrap()
                    .execute_hidden_pass(now_ms),
            };

            let mut effects = EngineEffects {
                admissions: executed.admissions,
                ..EngineEffects::default()
            };
            if let Some(fpm) = executed.fpm {
                effects.fpm_snapshots.push((worker_id, fpm));
            }
            let completion_kv_events =
                if executed.router_event_visibility == RouterEventVisibility::PassStart {
                    effects.pass_start_kv_events = executed.kv_events;
                    Vec::new()
                } else {
                    executed.kv_events
                };
            let payload = WorkerCompletionPayload {
                stage: self.stage,
                worker_idx: worker_id,
                completed_requests: executed.completed_requests,
                output_signals: executed.output_signals,
                kv_events: completion_kv_events,
            };

            if executed.end_ms == now_ms {
                effects.immediate_completions.push(payload);
                return Ok(effects);
            }

            self.workers.get_mut(&worker_id).unwrap().mark_busy();
            effects
                .scheduled_completions
                .push(ScheduledWorkerCompletion {
                    at_ms: executed.end_ms,
                    payload,
                });
            return Ok(effects);
        }

        Ok(EngineEffects::default())
    }

    pub(in crate::replay::offline) fn on_scheduled_completion(
        &mut self,
        payload: WorkerCompletionPayload,
    ) -> anyhow::Result<WorkerCompletionPayload> {
        if payload.stage != self.stage {
            bail!(
                "offline replay completion stage mismatch: expected {:?}, got {:?}",
                self.stage,
                payload.stage
            );
        }
        let worker = self.workers.get_mut(&payload.worker_idx).ok_or_else(|| {
            anyhow::anyhow!(
                "offline replay completion for unknown worker {}",
                payload.worker_idx
            )
        })?;
        worker.mark_idle();
        worker.mark_completed(payload.completed_requests);
        // 立即清理处于待移除状态且已排空的 worker，以免在没有
        // 后续缩放事件触发 apply_target_count 时它们无限期滞留。
        if self.pending_removal.contains(&payload.worker_idx) {
            self.try_remove_drained();
        }
        Ok(payload)
    }

    pub(in crate::replay::offline) fn in_flight(&self) -> usize {
        self.workers
            .values()
            .map(OfflineWorkerState::in_flight)
            .sum()
    }

    pub(in crate::replay::offline) fn is_drained(&self) -> bool {
        self.workers.values().all(OfflineWorkerState::is_drained)
    }

    pub(in crate::replay::offline) fn worker_count(&self) -> usize {
        self.workers.len()
    }

    #[cfg(test)]
    pub(crate) fn debug_snapshots(&self) -> Vec<OfflineWorkerSnapshot> {
        self.workers
            .values()
            .map(OfflineWorkerState::debug_snapshot)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocols::MockEngineArgs;

    fn engine_with_startup(num_workers: usize, startup_time: Option<f64>) -> EngineComponent {
        let args = MockEngineArgs {
            startup_time,
            ..MockEngineArgs::default()
        };
        let workers: Vec<_> = (0..num_workers)
            .map(|i| OfflineWorkerState::new(i, args.clone(), false))
            .collect();
        let mut engine = EngineComponent::new(
            SimulationWorkerStage::Aggregated,
            EnginePassMode::Visible,
            workers,
        );
        engine.set_scaling_args(args, false);
        engine
    }

    #[test]
    fn test_apply_target_count_scale_up_with_startup() {
        let mut engine = engine_with_startup(2, Some(5.0));
        let (added, newly_marked) = engine.apply_target_count(4);

        assert_eq!(added.len(), 2);
        assert!(newly_marked.is_empty());
        // 新 worker 处于 pending_startup 中。
        assert_eq!(engine.active_worker_ids().len(), 2);
        assert_eq!(engine.worker_count(), 4);
    }

    #[test]
    fn test_apply_target_count_scale_up_without_startup() {
        let mut engine = engine_with_startup(2, None);
        let (added, newly_marked) = engine.apply_target_count(4);

        assert_eq!(added.len(), 2);
        assert!(newly_marked.is_empty());
        // 无启动延迟时，worker 立即活跃。
        assert_eq!(engine.active_worker_ids().len(), 4);
        assert_eq!(engine.worker_count(), 4);
    }

    #[test]
    fn test_scale_down_cancels_startup_before_active() {
        let mut engine = engine_with_startup(2, Some(5.0));

        // 扩容到 4 —— 在 pending_startup 中添加 2 个。
        engine.apply_target_count(4);
        assert_eq!(engine.active_worker_ids().len(), 2);
        assert_eq!(engine.worker_count(), 4);

        // 缩容到 3 —— 应取消 1 个启动 worker，不标记任何活跃 worker。
        let (_added, newly_marked) = engine.apply_target_count(3);
        assert!(newly_marked.is_empty());
        assert_eq!(engine.active_worker_ids().len(), 2);
        assert_eq!(engine.worker_count(), 3); // 2 活跃 + 1 仍在启动

        // 缩容到 2 —— 应取消剩余的启动 worker。
        let (_added, newly_marked) = engine.apply_target_count(2);
        assert!(newly_marked.is_empty());
        assert_eq!(engine.active_worker_ids().len(), 2);
        assert_eq!(engine.worker_count(), 2);
    }

    #[test]
    fn test_scale_down_past_startup_marks_active() {
        let mut engine = engine_with_startup(3, Some(5.0));

        // 扩容到 5 —— 在 pending_startup 中添加 2 个。
        engine.apply_target_count(5);

        // 缩容到 1 —— 应取消 2 个启动，标记 2 个活跃。
        let (_added, newly_marked) = engine.apply_target_count(1);
        assert_eq!(newly_marked.len(), 2);
        assert_eq!(engine.active_worker_ids().len(), 1);
    }

    #[test]
    fn test_mark_worker_ready_activates_pending() {
        let mut engine = engine_with_startup(1, Some(5.0));
        let (added, _) = engine.apply_target_count(2);
        let new_id = added[0];

        assert_eq!(engine.active_worker_ids().len(), 1);
        assert!(engine.mark_worker_ready(new_id));
        assert_eq!(engine.active_worker_ids().len(), 2);
    }

    #[test]
    fn test_mark_worker_ready_returns_false_for_cancelled() {
        let mut engine = engine_with_startup(1, Some(5.0));
        let (added, _) = engine.apply_target_count(2);
        let new_id = added[0];

        // 通过缩容取消。
        engine.apply_target_count(1);
        // worker 已从 pending_startup 与 workers map 中移除。
        assert!(!engine.mark_worker_ready(new_id));
    }

    #[test]
    fn test_startup_time_ms_conversion() {
        let engine = engine_with_startup(1, Some(5.0));
        assert_eq!(engine.startup_time_ms(), Some(5000.0));

        let engine = engine_with_startup(1, None);
        assert_eq!(engine.startup_time_ms(), None);

        let engine = engine_with_startup(1, Some(0.0));
        assert_eq!(engine.startup_time_ms(), None); // 0 视为无延迟
    }
}
