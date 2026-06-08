// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-License-Identifier: Apache-2.0

//! # 离线重放进度条
//!
//! ## 设计意图
//! 封装离线重放期间的终端进度条展示，按完成请求数推进并在结束/销毁时清理。
//!
//! ## 外部契约
//! 提供 `ReplayProgress`，提供 `new`/`inc_completed`/`finish`，并在 `Drop` 时自动收尾，行为与 Dynamo 一致。

use indicatif::{ProgressBar, ProgressStyle};

pub(super) struct ReplayProgress {
    bar: ProgressBar,
}

impl ReplayProgress {
    pub(super) fn new(total_requests: usize, label: &'static str) -> Self {
        let bar = ProgressBar::new(total_requests as u64);
        bar.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta}) {msg}",
            )
            .expect("progress bar template must be valid")
            .progress_chars("#>-"),
        );
        bar.set_message(label);
        Self { bar }
    }

    pub(super) fn inc_completed(&self) {
        self.bar.inc(1);
    }

    pub(super) fn finish(&self) {
        self.bar.finish_and_clear();
    }
}

impl Drop for ReplayProgress {
    fn drop(&mut self) {
        if !self.bar.is_finished() {
            self.bar.finish_and_clear();
        }
    }
}
