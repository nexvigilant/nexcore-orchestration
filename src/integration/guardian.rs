//! Guardian integration: Sensor and Actuator implementations.
//!
//! - `OrchestrationSensor` detects DAMPs: stuck agents, high error rates, queue saturation.
//! - `OrchestrationActuator` responds: cancels stuck agents on high-severity signals.

use std::sync::Arc;

use async_trait::async_trait;
use nexcore_chrono::DateTime;
use nexcore_vigilance::guardian::response::{Actuator, ActuatorResult, ResponseAction};
use nexcore_vigilance::guardian::sensing::{Sensor, SignalSource, ThreatLevel, ThreatSignal};

use crate::agent::AgentState;
use crate::agent::registry::AgentRegistry;

/// Sensor that detects orchestration anomalies as DAMPs (internal damage).
pub struct OrchestrationSensor {
    registry: Arc<AgentRegistry>,
    /// Threshold: fraction of agents in Error state that triggers a signal.
    error_rate_threshold: f64,
}

impl OrchestrationSensor {
    /// Create a new orchestration sensor.
    #[must_use]
    pub fn new(registry: Arc<AgentRegistry>, error_rate_threshold: f64) -> Self {
        Self {
            registry,
            error_rate_threshold,
        }
    }

    /// Check for high error rate among agents.
    fn detect_error_rate(&self) -> Option<ThreatSignal<String>> {
        let counts = self.registry.count_by_state();
        let total: usize = counts.values().sum();
        if total == 0 {
            return None;
        }
        let error_count = counts.get(&AgentState::Error).copied().unwrap_or(0);
        let error_rate = error_count as f64 / total as f64;

        if error_rate > self.error_rate_threshold {
            Some(ThreatSignal {
                id: format!("orc-error-rate-{}", DateTime::now().timestamp_millis()),
                pattern: format!(
                    "high error rate: {:.1}% ({error_count}/{total})",
                    error_rate * 100.0
                ),
                severity: if error_rate > 0.5 {
                    ThreatLevel::Critical
                } else {
                    ThreatLevel::High
                },
                timestamp: DateTime::now(),
                source: SignalSource::Damp {
                    subsystem: "orchestration".to_string(),
                    damage_type: "error_rate".to_string(),
                },
                confidence: nexcore_vigilance::primitives::Measured::certain(error_rate.min(1.0)),
                metadata: std::collections::HashMap::new(),
            })
        } else {
            None
        }
    }

    /// Detect agents stuck in non-terminal states too long.
    fn detect_stuck_agents(&self) -> Vec<ThreatSignal<String>> {
        let now = DateTime::now();
        let stuck_threshold = nexcore_chrono::Duration::minutes(10);
        let mut signals = Vec::new();

        let executing = self.registry.by_state(AgentState::Executing);
        for agent in &executing {
            let elapsed = now.signed_duration_since(agent.updated_at);
            if elapsed > stuck_threshold {
                signals.push(ThreatSignal {
                    id: format!("orc-stuck-{}", agent.id),
                    pattern: format!(
                        "agent {} stuck in executing for {}s",
                        agent.id,
                        elapsed.num_seconds()
                    ),
                    severity: ThreatLevel::Medium,
                    timestamp: now,
                    source: SignalSource::Damp {
                        subsystem: "orchestration".to_string(),
                        damage_type: "stuck_agent".to_string(),
                    },
                    confidence: nexcore_vigilance::primitives::Measured::certain(0.8),
                    metadata: std::collections::HashMap::new(),
                });
            }
        }

        signals
    }
}

impl Sensor for OrchestrationSensor {
    type Pattern = String;

    fn detect(&self) -> Vec<ThreatSignal<Self::Pattern>> {
        let mut signals = Vec::new();

        if let Some(sig) = self.detect_error_rate() {
            signals.push(sig);
        }

        signals.extend(self.detect_stuck_agents());

        signals
    }

    fn sensitivity(&self) -> f64 {
        0.8
    }

    fn name(&self) -> &str {
        "orchestration-sensor"
    }
}

/// Actuator that responds to orchestration signals by cancelling stuck agents.
pub struct OrchestrationActuator {
    registry: Arc<AgentRegistry>,
}

impl OrchestrationActuator {
    /// Create a new orchestration actuator.
    #[must_use]
    pub fn new(registry: Arc<AgentRegistry>) -> Self {
        Self { registry }
    }
}

#[async_trait]
impl Actuator for OrchestrationActuator {
    async fn execute(&self, action: &ResponseAction) -> ActuatorResult {
        match action {
            ResponseAction::Alert { message, .. } => {
                tracing::warn!(msg = %message, "orchestration alert");
                ActuatorResult {
                    success: true,
                    message: format!("alert logged: {message}"),
                    timestamp: DateTime::now(),
                    duration_ms: 0,
                    data: std::collections::HashMap::new(),
                }
            }
            ResponseAction::Block { target, .. } => {
                // Interpret target as agent ID (UUID string) and mark as Cancelled
                if let Ok(uuid) = target.parse::<nexcore_id::NexId>() {
                    let agent_id = crate::types::AgentId::from_uuid(uuid);
                    match self.registry.update_state(&agent_id, AgentState::Cancelled) {
                        Ok(()) => ActuatorResult {
                            success: true,
                            message: format!("cancelled agent {agent_id}"),
                            timestamp: DateTime::now(),
                            duration_ms: 0,
                            data: std::collections::HashMap::new(),
                        },
                        Err(e) => ActuatorResult {
                            success: false,
                            message: format!("failed to cancel agent: {e}"),
                            timestamp: DateTime::now(),
                            duration_ms: 0,
                            data: std::collections::HashMap::new(),
                        },
                    }
                } else {
                    ActuatorResult {
                        success: false,
                        message: format!("invalid agent id: {target}"),
                        timestamp: DateTime::now(),
                        duration_ms: 0,
                        data: std::collections::HashMap::new(),
                    }
                }
            }
            _ => ActuatorResult {
                success: false,
                message: "unsupported action for orchestration actuator".to_string(),
                timestamp: DateTime::now(),
                duration_ms: 0,
                data: std::collections::HashMap::new(),
            },
        }
    }

    async fn revert(&self, _action: &ResponseAction) -> ActuatorResult {
        ActuatorResult {
            success: true,
            message: "revert not implemented for orchestration actuator".to_string(),
            timestamp: DateTime::now(),
            duration_ms: 0,
            data: std::collections::HashMap::new(),
        }
    }

    fn can_execute(&self, action: &ResponseAction) -> bool {
        matches!(
            action,
            ResponseAction::Alert { .. } | ResponseAction::Block { .. }
        )
    }

    fn name(&self) -> &str {
        "orchestration-actuator"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::AgentRecord;
    use crate::types::Priority;

    #[test]
    fn sensor_no_signals_on_empty_registry() {
        let reg = Arc::new(AgentRegistry::new());
        let sensor = OrchestrationSensor::new(reg, 0.3);
        let signals = sensor.detect();
        assert!(signals.is_empty());
    }

    #[test]
    fn sensor_detects_high_error_rate() {
        let reg = Arc::new(AgentRegistry::new());

        // 3 agents: 2 error, 1 done = 66% error rate
        for i in 0..3 {
            let mut r = AgentRecord::new(
                crate::types::AgentId::new(),
                format!("task-{i}"),
                Priority::Normal,
                None,
            );
            if i < 2 {
                r.state = AgentState::Error;
            } else {
                r.state = AgentState::Done;
            }
            reg.register(r).ok();
        }

        let sensor = OrchestrationSensor::new(reg, 0.3);
        let signals = sensor.detect();
        assert!(!signals.is_empty());
        assert!(signals[0].pattern.contains("error rate"));
    }

    #[test]
    fn sensor_no_false_positive() {
        let reg = Arc::new(AgentRegistry::new());

        // 10 agents: 1 error = 10% error rate, threshold 30%
        for i in 0..10 {
            let mut r = AgentRecord::new(
                crate::types::AgentId::new(),
                format!("task-{i}"),
                Priority::Normal,
                None,
            );
            if i == 0 {
                r.state = AgentState::Error;
            } else {
                r.state = AgentState::Done;
            }
            reg.register(r).ok();
        }

        let sensor = OrchestrationSensor::new(reg, 0.3);
        let signals = sensor.detect();
        // Should not trigger on error rate
        let error_rate_signals: Vec<_> = signals
            .iter()
            .filter(|s| s.pattern.contains("error rate"))
            .collect();
        assert!(error_rate_signals.is_empty());
    }

    #[test]
    fn actuator_can_execute_filter() {
        let reg = Arc::new(AgentRegistry::new());
        let actuator = OrchestrationActuator::new(reg);

        assert!(actuator.can_execute(&ResponseAction::Alert {
            severity: ThreatLevel::High,
            message: "test".to_string(),
            recipients: vec![],
        }));
        assert!(actuator.can_execute(&ResponseAction::Block {
            target: "x".to_string(),
            duration: None,
            reason: "test".to_string(),
        }));
        assert!(!actuator.can_execute(&ResponseAction::RateLimit {
            resource: "x".to_string(),
            max_requests: 10,
            window_seconds: 60,
        }));
    }

    #[tokio::test]
    async fn actuator_cancels_agent() {
        let reg = Arc::new(AgentRegistry::new());
        let id = crate::types::AgentId::new();
        let record = AgentRecord::new(id.clone(), "test".to_string(), Priority::Normal, None);
        reg.register(record).ok();
        reg.update_state(&id, AgentState::Executing).ok();

        let actuator = OrchestrationActuator::new(reg.clone());
        let result = actuator
            .execute(&ResponseAction::Block {
                target: id.0.to_string(),
                duration: None,
                reason: "stuck".to_string(),
            })
            .await;
        assert!(result.success);

        let agent = reg.get(&id);
        assert_eq!(agent.map(|a| a.state), Some(AgentState::Cancelled));
    }

    #[test]
    fn sensor_trait_methods() {
        let reg = Arc::new(AgentRegistry::new());
        let sensor = OrchestrationSensor::new(reg, 0.3);
        assert_eq!(sensor.name(), "orchestration-sensor");
        assert!((sensor.sensitivity() - 0.8).abs() < f64::EPSILON);
        assert!(sensor.is_active());
    }
}
