// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Pipeline nodes — sources (ingress) and sinks (egress).

pub mod sinks;
pub mod sources;

pub use sinks::{ServiceBackend, SegmentSink, Sink, SinkEdge};
pub use sources::{Frontend, NetworkSourceNode, SegmentSource, ServiceFrontend, Source};
