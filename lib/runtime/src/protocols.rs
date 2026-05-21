// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 协议辅助类型：流式响应标注与流内错误传播。

pub mod annotated;
pub mod maybe_error;

pub use annotated::Annotated;
pub use maybe_error::MaybeError;
