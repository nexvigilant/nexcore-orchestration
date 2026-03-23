//! Agent lifecycle types, task trait, and registry.

pub mod context;
pub mod registry;
pub mod supervisor;

use std::fmt;

use nexcore_chrono::DateTime;
use serde::{Deserialize, Serialize};

use crate::types::{AgentId, Priority, TaskGroupId};

/// Agent lifecycle state machine.
///
/// ```text
/// Queued → Acquiring → Executing → Reporting → Done
///                                             → Error
///                                  → Cancelled
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AgentState {
    /// Waiting in queue for execution slot.
    Queued,
    /// Acquiring concurrency permit and context.
    Acquiring,
    /// Task is actively executing.
    Executing,
    /// Task completed, reporting results.
    Reporting,
    /// Successfully completed.
    Done,
    /// Failed with error.
    Error,
    /// Cancelled by supervisor or user.
    Cancelled,
}

impl AgentState {
    /// Whether this state is terminal (no further transitions).
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Done | Self::Error | Self::Cancelled)
    }

    /// Human-readable state name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Acquiring => "acquiring",
            Self::Executing => "executing",
            Self::Reporting => "reporting",
            Self::Done => "done",
            Self::Error => "error",
            Self::Cancelled => "cancelled",
        }
    }
}

impl fmt::Display for AgentState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Record of an agent's full lifecycle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentRecord {
    /// Unique agent identifier.
    pub id: AgentId,
    /// Human-readable task name.
    pub name: String,
    /// Current lifecycle state.
    pub state: AgentState,
    /// Task priority.
    pub priority: Priority,
    /// Optional group membership.
    pub group: Option<TaskGroupId>,
    /// When the agent was created.
    pub created_at: DateTime,
    /// When the state last changed.
    pub updated_at: DateTime,
    /// Serialized result (populated on Done).
    pub result: Option<serde_json::Value>,
    /// Error message (populated on Error).
    pub error: Option<String>,
}

impl AgentRecord {
    /// Create a new agent record in Queued state.
    #[must_use]
    pub fn new(id: AgentId, name: String, priority: Priority, group: Option<TaskGroupId>) -> Self {
        let now = DateTime::now();
        Self {
            id,
            name,
            state: AgentState::Queued,
            priority,
            group,
            created_at: now,
            updated_at: now,
            result: None,
            error: None,
        }
    }
}

/// Trait for agent tasks that can be executed by the supervisor.
#[async_trait::async_trait]
pub trait AgentTask: Send + Sync + 'static {
    /// The output type produced on success.
    type Output: Send + Sync + Serialize + 'static;

    /// Execute the task within an isolated context.
    async fn execute(&self, context: &context::IsolatedContext) -> crate::OrcResult<Self::Output>;

    /// Human-readable task name.
    fn name(&self) -> &str;

    /// Task priority (default: Normal).
    fn priority(&self) -> Priority {
        Priority::Normal
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_state_terminal() {
        assert!(!AgentState::Queued.is_terminal());
        assert!(!AgentState::Acquiring.is_terminal());
        assert!(!AgentState::Executing.is_terminal());
        assert!(!AgentState::Reporting.is_terminal());
        assert!(AgentState::Done.is_terminal());
        assert!(AgentState::Error.is_terminal());
        assert!(AgentState::Cancelled.is_terminal());
    }

    #[test]
    fn agent_record_defaults_to_queued() {
        let record = AgentRecord::new(
            AgentId::new(),
            "test-task".to_string(),
            Priority::Normal,
            None,
        );
        assert_eq!(record.state, AgentState::Queued);
        assert!(record.result.is_none());
        assert!(record.error.is_none());
    }

    #[test]
    fn agent_state_display() {
        assert_eq!(format!("{}", AgentState::Executing), "executing");
        assert_eq!(format!("{}", AgentState::Done), "done");
    }
}
