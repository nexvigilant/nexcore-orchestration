//! # NexVigilant Core — orchestration
//!
//! Multi-agent lifecycle, registry, consensus, work queue, and status server.
//!
//! Provides the foundation for orchestrating concurrent agent tasks with:
//! - **Agent Registry**: Thread-safe agent state tracking via `DashMap`
//! - **Work Queue**: Bounded priority queue with async backpressure
//! - **Supervisor**: Spawn, cancel, and monitor agent tasks with concurrency limits
//! - **Consensus**: Collect and evaluate results from multiple agents
//! - **Status Server**: HTTP endpoints for monitoring agent state
//! - **Guardian Integration**: Sensor/Actuator bridge for homeostasis control

#![forbid(unsafe_code)]
#![cfg_attr(not(test), deny(clippy::unwrap_used))]
#![cfg_attr(not(test), deny(clippy::expect_used))]
#![cfg_attr(not(test), deny(clippy::panic))]
#![warn(missing_docs)]

pub mod agent;
pub mod consensus;
pub mod error;
pub mod grounding;
pub mod integration;
pub mod queue;
pub mod status;
pub mod types;

pub use agent::registry::AgentRegistry;
pub use agent::{AgentRecord, AgentState, AgentTask};
pub use error::{OrcError, OrcResult};
pub use types::{AgentId, Priority, TaskGroupId};
