// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Sink nodes — exit points for data leaving a pipeline stage.

pub mod base;
pub mod pipeline;
pub mod segment;

pub use base::{Sink, SinkEdge};
pub use pipeline::ServiceBackend;
pub use segment::SegmentSink;
