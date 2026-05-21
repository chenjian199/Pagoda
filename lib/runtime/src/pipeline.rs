// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Pipeline module — composable request/response processing graphs.

pub mod context;
pub mod error;
pub mod network;
pub mod nodes;
pub mod registry;

pub use context::{Context, Controller, IntoContext, PipelineIO, StreamContext};
pub use error::PipelineError;
pub use network::manager::NetworkManager;
pub use registry::PipelineRegistry;
