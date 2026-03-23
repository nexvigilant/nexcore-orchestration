//! Thread-safe agent registry backed by `DashMap`.

use dashmap::DashMap;

use crate::error::{OrcError, OrcResult};
use crate::types::{AgentId, TaskGroupId};

use super::{AgentRecord, AgentState};

/// Thread-safe registry of all agent records.
///
/// Uses `DashMap` for lock-free concurrent access from supervisor,
/// status server, and guardian integration.
#[derive(Debug)]
pub struct AgentRegistry {
    agents: DashMap<AgentId, AgentRecord>,
}

impl AgentRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            agents: DashMap::new(),
        }
    }

    /// Register a new agent. Returns error if ID already exists.
    pub fn register(&self, record: AgentRecord) -> OrcResult<()> {
        use dashmap::mapref::entry::Entry;
        match self.agents.entry(record.id.clone()) {
            Entry::Vacant(e) => {
                e.insert(record);
                Ok(())
            }
            Entry::Occupied(_) => {
                // Should never happen with UUID v4, but handle gracefully
                Err(OrcError::InvalidStateTransition {
                    id: record.id,
                    from: "exists",
                    to: "register",
                })
            }
        }
    }

    /// Get a snapshot (clone) of an agent record.
    #[must_use]
    pub fn get(&self, id: &AgentId) -> Option<AgentRecord> {
        self.agents.get(id).map(|r| r.clone())
    }

    /// Update agent state, setting `updated_at` to now.
    pub fn update_state(&self, id: &AgentId, new_state: AgentState) -> OrcResult<()> {
        let mut entry = self
            .agents
            .get_mut(id)
            .ok_or_else(|| OrcError::AgentNotFound(id.clone()))?;
        entry.state = new_state;
        entry.updated_at = nexcore_chrono::DateTime::now();
        Ok(())
    }

    /// Set the result JSON on a completed agent.
    pub fn set_result(&self, id: &AgentId, result: serde_json::Value) -> OrcResult<()> {
        let mut entry = self
            .agents
            .get_mut(id)
            .ok_or_else(|| OrcError::AgentNotFound(id.clone()))?;
        entry.result = Some(result);
        entry.updated_at = nexcore_chrono::DateTime::now();
        Ok(())
    }

    /// Set error message on a failed agent.
    pub fn set_error(&self, id: &AgentId, error: String) -> OrcResult<()> {
        let mut entry = self
            .agents
            .get_mut(id)
            .ok_or_else(|| OrcError::AgentNotFound(id.clone()))?;
        entry.error = Some(error);
        entry.updated_at = nexcore_chrono::DateTime::now();
        Ok(())
    }

    /// Remove an agent from the registry.
    pub fn remove(&self, id: &AgentId) -> Option<AgentRecord> {
        self.agents.remove(id).map(|(_, v)| v)
    }

    /// Get all agents belonging to a task group.
    #[must_use]
    pub fn get_group(&self, group_id: &TaskGroupId) -> Vec<AgentRecord> {
        self.agents
            .iter()
            .filter(|entry| entry.value().group.as_ref() == Some(group_id))
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get all agents in a specific state.
    #[must_use]
    pub fn by_state(&self, state: AgentState) -> Vec<AgentRecord> {
        self.agents
            .iter()
            .filter(|entry| entry.value().state == state)
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Snapshot all agent records (for status server).
    #[must_use]
    pub fn snapshot(&self) -> Vec<AgentRecord> {
        self.agents
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Count agents grouped by state.
    #[must_use]
    pub fn count_by_state(&self) -> std::collections::HashMap<AgentState, usize> {
        let mut counts = std::collections::HashMap::new();
        for entry in &self.agents {
            *counts.entry(entry.value().state).or_insert(0) += 1;
        }
        counts
    }

    /// Total number of registered agents.
    #[must_use]
    pub fn len(&self) -> usize {
        self.agents.len()
    }

    /// Whether the registry is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.agents.is_empty()
    }
}

impl Default for AgentRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Priority;

    fn make_record(name: &str, group: Option<TaskGroupId>) -> AgentRecord {
        AgentRecord::new(AgentId::new(), name.to_string(), Priority::Normal, group)
    }

    #[test]
    fn register_and_get() {
        let reg = AgentRegistry::new();
        let record = make_record("test", None);
        let id = record.id.clone();

        reg.register(record).ok();
        let fetched = reg.get(&id);
        assert!(fetched.is_some());
        assert_eq!(fetched.map(|r| r.name), Some("test".to_string()));
    }

    #[test]
    fn update_state() {
        let reg = AgentRegistry::new();
        let record = make_record("task-a", None);
        let id = record.id.clone();
        reg.register(record).ok();

        reg.update_state(&id, AgentState::Executing).ok();
        let fetched = reg.get(&id);
        assert_eq!(fetched.map(|r| r.state), Some(AgentState::Executing));
    }

    #[test]
    fn set_result_and_error() {
        let reg = AgentRegistry::new();
        let record = make_record("task-b", None);
        let id = record.id.clone();
        reg.register(record).ok();

        reg.set_result(&id, serde_json::json!({"score": 42})).ok();
        let fetched = reg.get(&id);
        assert!(fetched.is_some());
        assert_eq!(
            fetched.and_then(|r| r.result),
            Some(serde_json::json!({"score": 42}))
        );
    }

    #[test]
    fn remove_agent() {
        let reg = AgentRegistry::new();
        let record = make_record("task-c", None);
        let id = record.id.clone();
        reg.register(record).ok();

        let removed = reg.remove(&id);
        assert!(removed.is_some());
        assert!(reg.get(&id).is_none());
        assert!(reg.is_empty());
    }

    #[test]
    fn group_filtering() {
        let reg = AgentRegistry::new();
        let group = TaskGroupId::new();
        let r1 = make_record("in-group", Some(group.clone()));
        let r2 = make_record("no-group", None);
        reg.register(r1).ok();
        reg.register(r2).ok();

        let group_agents = reg.get_group(&group);
        assert_eq!(group_agents.len(), 1);
        assert_eq!(group_agents[0].name, "in-group");
    }

    #[test]
    fn by_state_filtering() {
        let reg = AgentRegistry::new();
        let r1 = make_record("a", None);
        let r2 = make_record("b", None);
        let id2 = r2.id.clone();
        reg.register(r1).ok();
        reg.register(r2).ok();

        reg.update_state(&id2, AgentState::Done).ok();

        let queued = reg.by_state(AgentState::Queued);
        let done = reg.by_state(AgentState::Done);
        assert_eq!(queued.len(), 1);
        assert_eq!(done.len(), 1);
    }

    #[test]
    fn snapshot_returns_all() {
        let reg = AgentRegistry::new();
        for i in 0..5 {
            reg.register(make_record(&format!("task-{i}"), None)).ok();
        }
        assert_eq!(reg.snapshot().len(), 5);
        assert_eq!(reg.len(), 5);
    }

    #[test]
    fn count_by_state_aggregation() {
        let reg = AgentRegistry::new();
        let r1 = make_record("a", None);
        let r2 = make_record("b", None);
        let r3 = make_record("c", None);
        let id2 = r2.id.clone();
        let id3 = r3.id.clone();
        reg.register(r1).ok();
        reg.register(r2).ok();
        reg.register(r3).ok();

        reg.update_state(&id2, AgentState::Done).ok();
        reg.update_state(&id3, AgentState::Done).ok();

        let counts = reg.count_by_state();
        assert_eq!(counts.get(&AgentState::Queued).copied().unwrap_or(0), 1);
        assert_eq!(counts.get(&AgentState::Done).copied().unwrap_or(0), 2);
    }

    #[test]
    fn not_found_error() {
        let reg = AgentRegistry::new();
        let fake_id = AgentId::new();
        let result = reg.update_state(&fake_id, AgentState::Done);
        assert!(result.is_err());
    }

    #[test]
    fn concurrent_access() {
        use std::sync::Arc;

        let reg = Arc::new(AgentRegistry::new());
        let mut handles = vec![];

        for i in 0..10 {
            let reg = reg.clone();
            handles.push(std::thread::spawn(move || {
                let record = make_record(&format!("concurrent-{i}"), None);
                reg.register(record).ok();
            }));
        }

        for h in handles {
            h.join().ok();
        }

        assert_eq!(reg.len(), 10);
    }
}
