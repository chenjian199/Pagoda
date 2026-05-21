pub mod node;
pub mod sources;

// Re-export node primitives so sibling modules can `use super::*;`.
pub use node::{
    AsyncEngine, Edge, NodeFn, Operator, PipelineIO, PipelineNode,
    PipelineOperator, PipelineOperatorBackwardEdge, PipelineOperatorForwardEdge,
    PipelineError, Sink, SinkEdge, Source,
};
