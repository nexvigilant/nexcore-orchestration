//! Core newtypes and shared types for orchestration.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Unique identifier for an agent instance.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AgentId(pub nexcore_id::NexId);

impl AgentId {
    /// Create a new random agent ID.
    #[must_use]
    pub fn new() -> Self {
        Self(nexcore_id::NexId::v4())
    }

    /// Create from an existing UUID.
    #[must_use]
    pub fn from_uuid(id: nexcore_id::NexId) -> Self {
        Self(id)
    }
}

impl Default for AgentId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for AgentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "agent-{}", &self.0.to_string()[..8])
    }
}

/// Unique identifier for a group of related tasks.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskGroupId(pub nexcore_id::NexId);

impl TaskGroupId {
    /// Create a new random task group ID.
    #[must_use]
    pub fn new() -> Self {
        Self(nexcore_id::NexId::v4())
    }
}

impl Default for TaskGroupId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TaskGroupId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "group-{}", &self.0.to_string()[..8])
    }
}

/// Task priority levels, ordered from lowest to highest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Priority {
    /// Background work, no urgency.
    Low = 0,
    /// Standard priority (default).
    Normal = 1,
    /// Elevated priority, processed before Normal.
    High = 2,
    /// Must be processed immediately.
    Critical = 3,
}

impl Priority {
    /// Numeric weight for ordering (higher = more urgent).
    #[must_use]
    pub fn weight(self) -> u8 {
        self as u8
    }
}

impl Default for Priority {
    fn default() -> Self {
        Self::Normal
    }
}

impl PartialOrd for Priority {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Priority {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.weight().cmp(&other.weight())
    }
}

impl fmt::Display for Priority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Low => write!(f, "low"),
            Self::Normal => write!(f, "normal"),
            Self::High => write!(f, "high"),
            Self::Critical => write!(f, "critical"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_id_display_truncates() {
        let id = AgentId::new();
        let display = format!("{id}");
        assert!(display.starts_with("agent-"));
        assert_eq!(display.len(), 14); // "agent-" + 8 hex chars
    }

    #[test]
    fn task_group_id_display_truncates() {
        let id = TaskGroupId::new();
        let display = format!("{id}");
        assert!(display.starts_with("group-"));
        assert_eq!(display.len(), 14);
    }

    #[test]
    fn priority_ordering() {
        assert!(Priority::Critical > Priority::High);
        assert!(Priority::High > Priority::Normal);
        assert!(Priority::Normal > Priority::Low);
    }

    #[test]
    fn priority_default_is_normal() {
        assert_eq!(Priority::default(), Priority::Normal);
    }

    #[test]
    fn agent_id_equality() {
        let uuid = nexcore_id::NexId::v4();
        let a = AgentId::from_uuid(uuid);
        let b = AgentId::from_uuid(uuid);
        assert_eq!(a, b);
    }
}
