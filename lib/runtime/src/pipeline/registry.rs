// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Pipeline registry — tracks active pipeline instances and their metadata.

use std::sync::Arc;

use dashmap::DashMap;
use tokio_util::sync::CancellationToken;

/// Metadata for a registered pipeline.
#[derive(Debug, Clone)]
pub struct PipelineEntry {
    /// Logical name of this pipeline.
    pub name: String,
    /// Cancellation token to shut this pipeline down.
    pub cancel: CancellationToken,
}

/// Central registry of all active pipelines in the runtime.
pub struct PipelineRegistry {
    entries: DashMap<String, Arc<PipelineEntry>>,
}

impl PipelineRegistry {
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
        }
    }

    /// Register a pipeline. Returns the entry for lifecycle management.
    pub fn register(&self, name: impl Into<String>) -> Arc<PipelineEntry> {
        let name = name.into();
        let entry = Arc::new(PipelineEntry {
            name: name.clone(),
            cancel: CancellationToken::new(),
        });
        self.entries.insert(name, Arc::clone(&entry));
        entry
    }

    /// Look up a pipeline by name.
    pub fn get(&self, name: &str) -> Option<Arc<PipelineEntry>> {
        self.entries.get(name).map(|r| Arc::clone(r.value()))
    }

    /// Remove a pipeline from the registry.
    pub fn remove(&self, name: &str) -> Option<Arc<PipelineEntry>> {
        self.entries.remove(name).map(|(_, v)| v)
    }

    /// Cancel all pipelines and clear the registry.
    pub fn shutdown_all(&self) {
        for entry in self.entries.iter() {
            entry.value().cancel.cancel();
        }
        self.entries.clear();
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Default for PipelineRegistry {
    fn default() -> Self {
        Self::new()
    }
}
