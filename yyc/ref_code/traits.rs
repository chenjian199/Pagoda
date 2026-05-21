#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Runtime {
    id: String,
}

impl Runtime {
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }

    pub fn id(&self) -> &str {
        &self.id
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DistributedRuntime {
    runtime: Runtime,
    cluster_name: String,
}

impl DistributedRuntime {
    pub fn new(runtime: Runtime, cluster_name: impl Into<String>) -> Self {
        Self {
            runtime,
            cluster_name: cluster_name.into(),
        }
    }

    pub fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    pub fn cluster_name(&self) -> &str {
        &self.cluster_name
    }
}

pub trait RuntimeProvider {
    fn rt(&self) -> &Runtime;
}

pub trait DistributedRuntimeProvider {
    fn drt(&self) -> &DistributedRuntime;
}

impl RuntimeProvider for DistributedRuntime {
    fn rt(&self) -> &Runtime {
        self.runtime()
    }
}

impl DistributedRuntimeProvider for DistributedRuntime {
    fn drt(&self) -> &DistributedRuntime {
        self
    }
}

#[derive(Clone, Debug)]
pub struct Namespace {
    drt: DistributedRuntime,
    name: String,
}

impl Namespace {
    pub fn new(drt: DistributedRuntime, name: impl Into<String>) -> Self {
        Self {
            drt,
            name: name.into(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl RuntimeProvider for Namespace {
    fn rt(&self) -> &Runtime {
        self.drt.runtime()
    }
}

impl DistributedRuntimeProvider for Namespace {
    fn drt(&self) -> &DistributedRuntime {
        &self.drt
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runtime_id(provider: &impl RuntimeProvider) -> &str {
        provider.rt().id()
    }

    fn cluster_name(provider: &impl DistributedRuntimeProvider) -> &str {
        provider.drt().cluster_name()
    }

    #[test]
    fn distributed_runtime_implements_both_providers() {
        let runtime = Runtime::new("rt-1");
        let drt = DistributedRuntime::new(runtime, "cluster-a");
        assert_eq!(runtime_id(&drt), "rt-1");
        assert_eq!(cluster_name(&drt), "cluster-a");
    }

    #[test]
    fn namespace_forwards_provider_access() {
        let runtime = Runtime::new("rt-2");
        let drt = DistributedRuntime::new(runtime, "cluster-b");
        let ns = Namespace::new(drt, "default");
        assert_eq!(runtime_id(&ns), "rt-2");
        assert_eq!(cluster_name(&ns), "cluster-b");
        assert_eq!(ns.name(), "default");
    }
}