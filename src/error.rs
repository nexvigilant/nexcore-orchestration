//! Orchestration error types.

use crate::types::{AgentId, TaskGroupId};

/// Unified error type for the orchestration crate.
#[derive(Debug, nexcore_error::Error)]
pub enum OrcError {
    /// Agent not found in registry.
    #[error("agent not found: {0}")]
    AgentNotFound(AgentId),

    /// Task group not found.
    #[error("task group not found: {0}")]
    GroupNotFound(TaskGroupId),

    /// Agent is in an invalid state for the requested operation.
    #[error("invalid state transition for agent {id}: cannot move from {from} to {to}")]
    InvalidStateTransition {
        /// The agent ID.
        id: AgentId,
        /// Current state name.
        from: &'static str,
        /// Attempted target state name.
        to: &'static str,
    },

    /// Work queue is full (backpressure).
    #[error("queue full: capacity {capacity}, tried to enqueue item")]
    QueueFull {
        /// Maximum queue capacity.
        capacity: usize,
    },

    /// Work queue is closed and no longer accepting items.
    #[error("queue closed")]
    QueueClosed,

    /// Agent execution timed out.
    #[error("agent {0} timed out")]
    Timeout(AgentId),

    /// Agent task returned an error during execution.
    #[error("agent {id} execution failed: {reason}")]
    ExecutionFailed {
        /// The agent ID.
        id: AgentId,
        /// Failure reason.
        reason: String,
    },

    /// Consensus could not be reached within the deadline.
    #[error("consensus timeout: {achieved}/{required} agents agreed")]
    ConsensusTimeout {
        /// How many agents agreed.
        achieved: usize,
        /// How many were required.
        required: usize,
    },

    /// Could not bind the status server to any port in the range.
    #[error("no available port in range {start}..={end}")]
    NoAvailablePort {
        /// Range start (inclusive).
        start: u16,
        /// Range end (inclusive).
        end: u16,
    },

    /// IO error (filesystem, network).
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization error.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Concurrency limit reached.
    #[error("concurrency limit reached: max {max} agents")]
    ConcurrencyLimit {
        /// Maximum allowed concurrent agents.
        max: usize,
    },

    /// Context directory creation failed.
    #[error("context creation failed for agent {id}: {reason}")]
    ContextCreation {
        /// The agent ID.
        id: AgentId,
        /// Failure reason.
        reason: String,
    },
}

/// Convenience result alias.
pub type OrcResult<T> = Result<T, OrcError>;
