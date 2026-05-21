// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 常用类型别名集合，供 `use pagoda_runtime::prelude::*` 一次性引入。

pub use crate::Worker;
pub use crate::Runtime;
pub use crate::DistributedRuntime;
pub use crate::RuntimeConfig;
pub use crate::PagodaError;
pub use crate::PipelineError;
pub use crate::MetricsRegistry;
pub use crate::SystemHealth;
pub use crate::HealthStatus;

pub use crate::engine::{
    AsyncEngine, AsyncEngineContext, AsyncEngineContextProvider,
    ResponseStream, Data, DataStream, DataUnary, EngineStream, EngineUnary,
    Context, Engine,
};

pub use crate::servicegroup::{
    Namespace, ServiceGroup, PortName, Instance, TransportType,
};

pub use crate::traits::{RuntimeProvider, DistributedRuntimeProvider};

pub use crate::protocols::{Annotated, MaybeError};

pub use tokio_util::sync::CancellationToken;
pub use anyhow::Result;
