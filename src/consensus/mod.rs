//! Consensus engine for multi-agent agreement.
//!
//! Collects results from N agents and evaluates them against configurable
//! agreement rules (majority, unanimous, quorum, etc.).

pub mod rules;

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::agent::AgentState;
use crate::agent::registry::AgentRegistry;
use crate::error::{OrcError, OrcResult};
use crate::types::{AgentId, TaskGroupId};

pub use rules::ConsensusRule;

/// Result of a consensus evaluation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusResult {
    /// Whether consensus was achieved.
    pub achieved: bool,
    /// The winning value (if consensus achieved).
    pub winning_value: Option<serde_json::Value>,
    /// Number of agents that agreed with the winning value.
    pub agreement_count: usize,
    /// Total agents that participated (submitted results).
    pub total_participants: usize,
    /// Agent IDs that dissented (different result or error).
    pub dissenting: Vec<AgentId>,
}

/// Engine that waits for agent results and evaluates consensus.
pub struct ConsensusEngine {
    registry: Arc<AgentRegistry>,
}

impl ConsensusEngine {
    /// Create a new consensus engine backed by the given registry.
    #[must_use]
    pub fn new(registry: Arc<AgentRegistry>) -> Self {
        Self { registry }
    }

    /// Await consensus from a group of agents.
    ///
    /// Polls the registry until all agents in the group reach terminal states,
    /// then evaluates results against the given rule. Times out if `timeout`
    /// elapses before all agents finish.
    pub async fn await_consensus(
        &self,
        group_id: &TaskGroupId,
        rule: &ConsensusRule,
        timeout: Duration,
    ) -> OrcResult<ConsensusResult> {
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            let agents = self.registry.get_group(group_id);
            if agents.is_empty() {
                return Err(OrcError::GroupNotFound(group_id.clone()));
            }

            let all_terminal = agents.iter().all(|a| a.state.is_terminal());

            if all_terminal {
                return Ok(self.evaluate(&agents, rule));
            }

            // Check timeout
            if tokio::time::Instant::now() >= deadline {
                // Evaluate with partial results
                let result = self.evaluate(&agents, rule);
                let done_count = agents
                    .iter()
                    .filter(|a| a.state == AgentState::Done)
                    .count();
                if result.achieved {
                    return Ok(result);
                }
                return Err(OrcError::ConsensusTimeout {
                    achieved: done_count,
                    required: agents.len(),
                });
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Evaluate consensus from a set of agent records.
    #[must_use]
    pub fn evaluate(
        &self,
        agents: &[crate::agent::AgentRecord],
        rule: &ConsensusRule,
    ) -> ConsensusResult {
        // Collect results from agents that completed successfully
        let mut value_counts: Vec<(serde_json::Value, Vec<AgentId>)> = Vec::new();
        let mut non_participants: Vec<AgentId> = Vec::new();

        for agent in agents {
            if agent.state == AgentState::Done {
                if let Some(ref result) = agent.result {
                    // Find matching value group
                    let found = value_counts.iter_mut().find(|(v, _)| v == result);
                    if let Some((_, ids)) = found {
                        ids.push(agent.id.clone());
                    } else {
                        value_counts.push((result.clone(), vec![agent.id.clone()]));
                    }
                } else {
                    non_participants.push(agent.id.clone());
                }
            } else {
                non_participants.push(agent.id.clone());
            }
        }

        // Find the most popular value
        value_counts.sort_by(|a, b| b.1.len().cmp(&a.1.len()));

        let (winning_value, agreeing_ids) = value_counts
            .first()
            .map(|(v, ids)| (Some(v.clone()), ids.clone()))
            .unwrap_or((None, vec![]));

        let agreement_count = agreeing_ids.len();
        let total_participants = agents
            .iter()
            .filter(|a| a.state == AgentState::Done && a.result.is_some())
            .count();

        // Determine dissenting agents (everyone not in the winning group)
        let dissenting: Vec<AgentId> = agents
            .iter()
            .filter(|a| !agreeing_ids.contains(&a.id))
            .map(|a| a.id.clone())
            .collect();

        let achieved = rule.is_satisfied(agreement_count, agents.len());

        ConsensusResult {
            achieved,
            winning_value,
            agreement_count,
            total_participants,
            dissenting,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::AgentRecord;
    use crate::types::Priority;

    fn done_agent(group: &TaskGroupId, result: serde_json::Value) -> AgentRecord {
        let mut r = AgentRecord::new(
            AgentId::new(),
            "test".to_string(),
            Priority::Normal,
            Some(group.clone()),
        );
        r.state = AgentState::Done;
        r.result = Some(result);
        r
    }

    fn error_agent(group: &TaskGroupId) -> AgentRecord {
        let mut r = AgentRecord::new(
            AgentId::new(),
            "test".to_string(),
            Priority::Normal,
            Some(group.clone()),
        );
        r.state = AgentState::Error;
        r.error = Some("failed".to_string());
        r
    }

    #[test]
    fn unanimous_all_agree() {
        let registry = Arc::new(AgentRegistry::new());
        let engine = ConsensusEngine::new(registry);
        let gid = TaskGroupId::new();

        let agents = vec![
            done_agent(&gid, serde_json::json!("yes")),
            done_agent(&gid, serde_json::json!("yes")),
            done_agent(&gid, serde_json::json!("yes")),
        ];

        let result = engine.evaluate(&agents, &ConsensusRule::Unanimous);
        assert!(result.achieved);
        assert_eq!(result.agreement_count, 3);
        assert_eq!(result.winning_value, Some(serde_json::json!("yes")));
    }

    #[test]
    fn unanimous_one_dissents() {
        let registry = Arc::new(AgentRegistry::new());
        let engine = ConsensusEngine::new(registry);
        let gid = TaskGroupId::new();

        let agents = vec![
            done_agent(&gid, serde_json::json!("yes")),
            done_agent(&gid, serde_json::json!("yes")),
            done_agent(&gid, serde_json::json!("no")),
        ];

        let result = engine.evaluate(&agents, &ConsensusRule::Unanimous);
        assert!(!result.achieved);
    }

    #[test]
    fn majority_achieved() {
        let registry = Arc::new(AgentRegistry::new());
        let engine = ConsensusEngine::new(registry);
        let gid = TaskGroupId::new();

        let agents = vec![
            done_agent(&gid, serde_json::json!(42)),
            done_agent(&gid, serde_json::json!(42)),
            done_agent(&gid, serde_json::json!(99)),
        ];

        let result = engine.evaluate(&agents, &ConsensusRule::Majority);
        assert!(result.achieved);
        assert_eq!(result.agreement_count, 2);
    }

    #[test]
    fn any_rule() {
        let registry = Arc::new(AgentRegistry::new());
        let engine = ConsensusEngine::new(registry);
        let gid = TaskGroupId::new();

        let agents = vec![
            done_agent(&gid, serde_json::json!("a")),
            error_agent(&gid),
            error_agent(&gid),
        ];

        let result = engine.evaluate(&agents, &ConsensusRule::Any);
        assert!(result.achieved);
    }

    #[test]
    fn min_agree_rule() {
        let registry = Arc::new(AgentRegistry::new());
        let engine = ConsensusEngine::new(registry);
        let gid = TaskGroupId::new();

        let agents = vec![
            done_agent(&gid, serde_json::json!("x")),
            done_agent(&gid, serde_json::json!("x")),
            done_agent(&gid, serde_json::json!("y")),
            done_agent(&gid, serde_json::json!("z")),
        ];

        let result = engine.evaluate(&agents, &ConsensusRule::MinAgree(2));
        assert!(result.achieved);

        let result = engine.evaluate(&agents, &ConsensusRule::MinAgree(3));
        assert!(!result.achieved);
    }

    #[test]
    fn quorum_rule() {
        let registry = Arc::new(AgentRegistry::new());
        let engine = ConsensusEngine::new(registry);
        let gid = TaskGroupId::new();

        let agents = vec![
            done_agent(&gid, serde_json::json!("v")),
            done_agent(&gid, serde_json::json!("v")),
            done_agent(&gid, serde_json::json!("v")),
            done_agent(&gid, serde_json::json!("other")),
            error_agent(&gid),
        ];

        // 3/5 = 60% agree, need 60%
        let result = engine.evaluate(&agents, &ConsensusRule::Quorum(0.6));
        assert!(result.achieved);

        // 3/5 = 60% agree, need 70%
        let result = engine.evaluate(&agents, &ConsensusRule::Quorum(0.7));
        assert!(!result.achieved);
    }
}
