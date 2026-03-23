//! Orchestration events for Vigil EventBus integration.

use nexcore_chrono::DateTime;
use serde::{Deserialize, Serialize};

use crate::agent::AgentState;
use crate::types::{AgentId, Priority, TaskGroupId};

/// Events emitted by the orchestration system for consumption by Vigil's EventBus.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrcEvent {
    /// An agent was spawned and registered.
    AgentSpawned {
        /// Agent identifier.
        id: AgentId,
        /// Task name.
        name: String,
        /// Task priority.
        priority: Priority,
        /// Optional group membership.
        group: Option<TaskGroupId>,
        /// When the agent was created.
        timestamp: DateTime,
    },

    /// An agent's state changed.
    AgentStateChanged {
        /// Agent identifier.
        id: AgentId,
        /// Previous state.
        from: AgentState,
        /// New state.
        to: AgentState,
        /// When the transition occurred.
        timestamp: DateTime,
    },

    /// An agent completed successfully.
    AgentCompleted {
        /// Agent identifier.
        id: AgentId,
        /// Serialized result.
        result: serde_json::Value,
        /// When it completed.
        timestamp: DateTime,
    },

    /// An agent failed.
    AgentFailed {
        /// Agent identifier.
        id: AgentId,
        /// Error message.
        error: String,
        /// When it failed.
        timestamp: DateTime,
    },

    /// An agent was cancelled.
    AgentCancelled {
        /// Agent identifier.
        id: AgentId,
        /// When it was cancelled.
        timestamp: DateTime,
    },

    /// Consensus was reached (or failed) for a group.
    ConsensusResult {
        /// Group identifier.
        group: TaskGroupId,
        /// Whether consensus was achieved.
        achieved: bool,
        /// Number of agreeing agents.
        agreement_count: usize,
        /// Total agents in the group.
        total: usize,
        /// When the consensus was evaluated.
        timestamp: DateTime,
    },

    /// Queue saturation warning.
    QueueSaturation {
        /// Current queue length.
        current: usize,
        /// Maximum capacity.
        capacity: usize,
        /// When detected.
        timestamp: DateTime,
    },
}

impl OrcEvent {
    /// Get the event type name for routing/filtering.
    #[must_use]
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::AgentSpawned { .. } => "orc.agent.spawned",
            Self::AgentStateChanged { .. } => "orc.agent.state_changed",
            Self::AgentCompleted { .. } => "orc.agent.completed",
            Self::AgentFailed { .. } => "orc.agent.failed",
            Self::AgentCancelled { .. } => "orc.agent.cancelled",
            Self::ConsensusResult { .. } => "orc.consensus.result",
            Self::QueueSaturation { .. } => "orc.queue.saturation",
        }
    }

    /// Get the timestamp of this event.
    #[must_use]
    pub fn timestamp(&self) -> DateTime {
        match self {
            Self::AgentSpawned { timestamp, .. }
            | Self::AgentStateChanged { timestamp, .. }
            | Self::AgentCompleted { timestamp, .. }
            | Self::AgentFailed { timestamp, .. }
            | Self::AgentCancelled { timestamp, .. }
            | Self::ConsensusResult { timestamp, .. }
            | Self::QueueSaturation { timestamp, .. } => *timestamp,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_type_names() {
        let evt = OrcEvent::AgentSpawned {
            id: AgentId::new(),
            name: "test".to_string(),
            priority: Priority::Normal,
            group: None,
            timestamp: DateTime::now(),
        };
        assert_eq!(evt.event_type(), "orc.agent.spawned");
    }

    #[test]
    fn event_serializes() {
        let evt = OrcEvent::AgentFailed {
            id: AgentId::new(),
            error: "boom".to_string(),
            timestamp: DateTime::now(),
        };
        let json = serde_json::to_string(&evt);
        assert!(json.is_ok());
    }

    #[test]
    fn event_timestamp_extraction() {
        let now = DateTime::now();
        let evt = OrcEvent::QueueSaturation {
            current: 90,
            capacity: 100,
            timestamp: now,
        };
        assert_eq!(evt.timestamp(), now);
    }
}
