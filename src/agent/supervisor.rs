//! Agent supervisor — spawn, cancel, and monitor agent tasks.
//!
//! Manages agent lifecycle via tokio tasks with concurrency limiting
//! through a `Semaphore`. Each agent gets an `IsolatedContext` and
//! is tracked in the shared `AgentRegistry`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use super::context::IsolatedContext;
use super::registry::AgentRegistry;
use super::{AgentRecord, AgentState, AgentTask};
use crate::error::{OrcError, OrcResult};
use crate::types::{AgentId, TaskGroupId};

/// Configuration for the agent supervisor.
#[derive(Debug, Clone)]
pub struct SupervisorConfig {
    /// Maximum number of agents executing concurrently.
    pub max_concurrency: usize,
    /// Timeout for a single agent execution.
    pub agent_timeout: Duration,
    /// Base path for agent context directories.
    pub context_base_path: PathBuf,
    /// Whether to clean up context directories after completion.
    pub cleanup_contexts: bool,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            max_concurrency: 8,
            agent_timeout: Duration::from_secs(300),
            context_base_path: std::env::temp_dir().join("nexcore-orchestration"),
            cleanup_contexts: true,
        }
    }
}

/// Manages agent lifecycle: spawn, cancel, wait.
pub struct AgentSupervisor {
    registry: Arc<AgentRegistry>,
    config: SupervisorConfig,
    semaphore: Arc<Semaphore>,
    handles: Arc<tokio::sync::Mutex<HashMap<AgentId, JoinHandle<()>>>>,
}

impl AgentSupervisor {
    /// Create a new supervisor with the given registry and config.
    #[must_use]
    pub fn new(registry: Arc<AgentRegistry>, config: SupervisorConfig) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_concurrency));
        Self {
            registry,
            config,
            semaphore,
            handles: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Get a reference to the shared registry.
    #[must_use]
    pub fn registry(&self) -> &Arc<AgentRegistry> {
        &self.registry
    }

    /// Spawn a single agent task. Returns the assigned `AgentId`.
    pub async fn spawn<T: AgentTask>(
        &self,
        task: T,
        group: Option<TaskGroupId>,
    ) -> OrcResult<AgentId>
    where
        T::Output: serde::Serialize,
    {
        let id = AgentId::new();
        let priority = task.priority();
        let name = task.name().to_string();

        let record = AgentRecord::new(id.clone(), name.clone(), priority, group);
        self.registry.register(record)?;

        let handle = self.spawn_task_inner(id.clone(), task);
        self.handles.lock().await.insert(id.clone(), handle);

        info!(agent = %id, task = %name, "agent spawned");
        Ok(id)
    }

    /// Spawn a group of tasks with a shared `TaskGroupId`.
    /// Returns the group ID and individual agent IDs.
    pub async fn spawn_group<T: AgentTask>(
        &self,
        tasks: Vec<T>,
        group_id: Option<TaskGroupId>,
    ) -> OrcResult<(TaskGroupId, Vec<AgentId>)>
    where
        T::Output: serde::Serialize,
    {
        let gid = group_id.unwrap_or_default();
        let mut ids = Vec::with_capacity(tasks.len());

        for task in tasks {
            let id = self.spawn(task, Some(gid.clone())).await?;
            ids.push(id);
        }

        Ok((gid, ids))
    }

    /// Cancel a running agent by aborting its tokio task.
    pub async fn cancel(&self, id: &AgentId) -> OrcResult<()> {
        if let Some(handle) = self.handles.lock().await.remove(id) {
            handle.abort();
            self.registry.update_state(id, AgentState::Cancelled)?;
            info!(agent = %id, "agent cancelled");
            Ok(())
        } else {
            Err(OrcError::AgentNotFound(id.clone()))
        }
    }

    /// Cancel all agents in a group.
    pub async fn cancel_group(&self, group_id: &TaskGroupId) -> OrcResult<usize> {
        let agents = self.registry.get_group(group_id);
        let mut cancelled = 0;
        for agent in &agents {
            if !agent.state.is_terminal() {
                if self.cancel(&agent.id).await.is_ok() {
                    cancelled += 1;
                }
            }
        }
        Ok(cancelled)
    }

    /// Wait for a specific agent to reach a terminal state.
    pub async fn wait_for(&self, id: &AgentId) -> OrcResult<AgentRecord> {
        loop {
            if let Some(record) = self.registry.get(id) {
                if record.state.is_terminal() {
                    // Clean up handle
                    self.handles.lock().await.remove(id);
                    return Ok(record);
                }
            } else {
                return Err(OrcError::AgentNotFound(id.clone()));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Wait for all agents in a group to reach terminal states.
    pub async fn wait_for_group(&self, group_id: &TaskGroupId) -> OrcResult<Vec<AgentRecord>> {
        loop {
            let agents = self.registry.get_group(group_id);
            if agents.is_empty() {
                return Err(OrcError::GroupNotFound(group_id.clone()));
            }
            if agents.iter().all(|a| a.state.is_terminal()) {
                return Ok(agents);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Internal: spawn the tokio task that manages an agent's lifecycle.
    fn spawn_task_inner<T: AgentTask>(&self, id: AgentId, task: T) -> JoinHandle<()>
    where
        T::Output: serde::Serialize,
    {
        let registry = self.registry.clone();
        let semaphore = self.semaphore.clone();
        let timeout = self.config.agent_timeout;
        let base_path = self.config.context_base_path.clone();
        let cleanup = self.config.cleanup_contexts;

        tokio::spawn(async move {
            // Acquiring permit
            if registry.update_state(&id, AgentState::Acquiring).is_err() {
                return;
            }

            let _permit = match semaphore.acquire().await {
                Ok(p) => p,
                Err(_) => {
                    registry.update_state(&id, AgentState::Error).ok();
                    registry.set_error(&id, "semaphore closed".to_string()).ok();
                    return;
                }
            };

            // Create context
            let context = match IsolatedContext::create(&base_path, &id) {
                Ok(ctx) => ctx,
                Err(e) => {
                    registry.update_state(&id, AgentState::Error).ok();
                    registry.set_error(&id, e.to_string()).ok();
                    return;
                }
            };

            // Execute with timeout
            registry.update_state(&id, AgentState::Executing).ok();

            let result = tokio::time::timeout(timeout, task.execute(&context)).await;

            match result {
                Ok(Ok(output)) => {
                    registry.update_state(&id, AgentState::Reporting).ok();
                    match serde_json::to_value(&output) {
                        Ok(val) => {
                            registry.set_result(&id, val).ok();
                            registry.update_state(&id, AgentState::Done).ok();
                        }
                        Err(e) => {
                            registry.update_state(&id, AgentState::Error).ok();
                            registry.set_error(&id, format!("serialization: {e}")).ok();
                        }
                    }
                }
                Ok(Err(e)) => {
                    registry.update_state(&id, AgentState::Error).ok();
                    registry.set_error(&id, e.to_string()).ok();
                    error!(agent = %id, error = %e, "agent execution failed");
                }
                Err(_) => {
                    registry.update_state(&id, AgentState::Error).ok();
                    registry
                        .set_error(&id, "execution timed out".to_string())
                        .ok();
                    warn!(agent = %id, "agent timed out");
                }
            }

            // Cleanup context
            if cleanup {
                context.cleanup().ok();
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Simple test task that returns a fixed value.
    struct EchoTask {
        value: String,
    }

    #[async_trait::async_trait]
    impl AgentTask for EchoTask {
        type Output = serde_json::Value;

        async fn execute(&self, _context: &IsolatedContext) -> OrcResult<Self::Output> {
            Ok(serde_json::json!({ "echo": self.value }))
        }

        fn name(&self) -> &str {
            "echo-task"
        }
    }

    /// Task that sleeps then returns.
    struct SlowTask {
        delay_ms: u64,
    }

    #[async_trait::async_trait]
    impl AgentTask for SlowTask {
        type Output = serde_json::Value;

        async fn execute(&self, _context: &IsolatedContext) -> OrcResult<Self::Output> {
            tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
            Ok(serde_json::json!({ "slept_ms": self.delay_ms }))
        }

        fn name(&self) -> &str {
            "slow-task"
        }
    }

    /// Task that always fails.
    struct FailTask;

    #[async_trait::async_trait]
    impl AgentTask for FailTask {
        type Output = serde_json::Value;

        async fn execute(&self, _context: &IsolatedContext) -> OrcResult<Self::Output> {
            Err(OrcError::ExecutionFailed {
                id: AgentId::new(),
                reason: "intentional failure".to_string(),
            })
        }

        fn name(&self) -> &str {
            "fail-task"
        }
    }

    fn make_supervisor() -> AgentSupervisor {
        let registry = Arc::new(AgentRegistry::new());
        let config = SupervisorConfig {
            max_concurrency: 4,
            agent_timeout: Duration::from_secs(5),
            context_base_path: std::env::temp_dir().join("nexcore-orc-test"),
            cleanup_contexts: true,
        };
        AgentSupervisor::new(registry, config)
    }

    #[tokio::test]
    async fn spawn_and_wait() {
        let sup = make_supervisor();
        let task = EchoTask {
            value: "hello".to_string(),
        };
        let id = sup.spawn(task, None).await;
        assert!(id.is_ok());
        let id = id.ok();
        if let Some(ref id) = id {
            let record = sup.wait_for(id).await;
            assert!(record.is_ok());
            if let Ok(record) = record {
                assert_eq!(record.state, AgentState::Done);
                assert!(record.result.is_some());
            }
        }
    }

    #[tokio::test]
    async fn spawn_and_cancel() {
        let sup = make_supervisor();
        let task = SlowTask { delay_ms: 5000 };
        let id = sup.spawn(task, None).await;
        assert!(id.is_ok());
        if let Ok(ref id) = id {
            // Give it a moment to start
            tokio::time::sleep(Duration::from_millis(50)).await;
            let cancel_result = sup.cancel(id).await;
            assert!(cancel_result.is_ok());
            let record = sup.registry().get(id);
            assert_eq!(record.map(|r| r.state), Some(AgentState::Cancelled));
        }
    }

    #[tokio::test]
    async fn failed_task_sets_error_state() {
        let sup = make_supervisor();
        let id = sup.spawn(FailTask, None).await;
        assert!(id.is_ok());
        if let Ok(ref id) = id {
            let record = sup.wait_for(id).await;
            assert!(record.is_ok());
            if let Ok(record) = record {
                assert_eq!(record.state, AgentState::Error);
                assert!(record.error.is_some());
            }
        }
    }

    #[tokio::test]
    async fn timeout_sets_error() {
        let registry = Arc::new(AgentRegistry::new());
        let config = SupervisorConfig {
            max_concurrency: 2,
            agent_timeout: Duration::from_millis(50), // Very short timeout
            context_base_path: std::env::temp_dir().join("nexcore-orc-timeout-test"),
            cleanup_contexts: true,
        };
        let sup = AgentSupervisor::new(registry, config);
        let task = SlowTask { delay_ms: 5000 };
        let id = sup.spawn(task, None).await;
        assert!(id.is_ok());
        if let Ok(ref id) = id {
            let record = sup.wait_for(id).await;
            assert!(record.is_ok());
            if let Ok(record) = record {
                assert_eq!(record.state, AgentState::Error);
                let err_msg = record.error.unwrap_or_default();
                assert!(err_msg.contains("timed out"));
            }
        }
    }

    #[tokio::test]
    async fn group_spawn_and_wait() {
        let sup = make_supervisor();
        let tasks = vec![
            EchoTask {
                value: "a".to_string(),
            },
            EchoTask {
                value: "b".to_string(),
            },
            EchoTask {
                value: "c".to_string(),
            },
        ];
        let result = sup.spawn_group(tasks, None).await;
        assert!(result.is_ok());
        if let Ok((group_id, _ids)) = result {
            let records = sup.wait_for_group(&group_id).await;
            assert!(records.is_ok());
            if let Ok(records) = records {
                assert_eq!(records.len(), 3);
                assert!(records.iter().all(|r| r.state == AgentState::Done));
            }
        }
    }
}
