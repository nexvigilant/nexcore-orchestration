//! Isolated execution context for agents.
//!
//! Each agent gets a deterministic directory derived from its ID via SHA-256.

use std::path::{Path, PathBuf};

use nexcore_codec::hex;
use sha2::{Digest, Sha256};

use crate::error::{OrcError, OrcResult};
use crate::types::AgentId;

/// Isolated filesystem context for a single agent execution.
///
/// Provides a deterministic, unique directory for each agent to store
/// intermediate artifacts without colliding with other agents.
#[derive(Debug, Clone)]
pub struct IsolatedContext {
    /// Root directory for this agent's artifacts.
    root: PathBuf,
    /// The agent this context belongs to.
    agent_id: AgentId,
}

impl IsolatedContext {
    /// Create a new isolated context under `base_path`.
    ///
    /// The directory name is derived from the agent ID via SHA-256 to ensure
    /// deterministic, collision-free paths.
    pub fn create(base_path: &Path, agent_id: &AgentId) -> OrcResult<Self> {
        let dir_name = Self::hash_id(agent_id);
        let root = base_path.join(dir_name);
        std::fs::create_dir_all(&root).map_err(|e| OrcError::ContextCreation {
            id: agent_id.clone(),
            reason: e.to_string(),
        })?;
        Ok(Self {
            root,
            agent_id: agent_id.clone(),
        })
    }

    /// Get the root directory path.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Get the agent ID this context belongs to.
    #[must_use]
    pub fn agent_id(&self) -> &AgentId {
        &self.agent_id
    }

    /// Get a path for a named artifact within this context.
    #[must_use]
    pub fn artifact_path(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }

    /// Write artifact data to the context directory.
    pub fn write_artifact(&self, name: &str, data: &[u8]) -> OrcResult<PathBuf> {
        let path = self.artifact_path(name);
        std::fs::write(&path, data).map_err(OrcError::Io)?;
        Ok(path)
    }

    /// Read artifact data from the context directory.
    pub fn read_artifact(&self, name: &str) -> OrcResult<Vec<u8>> {
        let path = self.artifact_path(name);
        std::fs::read(&path).map_err(OrcError::Io)
    }

    /// Remove the entire context directory.
    pub fn cleanup(&self) -> OrcResult<()> {
        if self.root.exists() {
            std::fs::remove_dir_all(&self.root).map_err(OrcError::Io)?;
        }
        Ok(())
    }

    /// Deterministic SHA-256 hash of agent ID for directory naming.
    fn hash_id(agent_id: &AgentId) -> String {
        let mut hasher = Sha256::new();
        hasher.update(agent_id.0.as_bytes());
        hex::encode(hasher.finalize())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_path() {
        let id = AgentId::from_uuid(nexcore_id::NexId::NIL);
        let hash1 = IsolatedContext::hash_id(&id);
        let hash2 = IsolatedContext::hash_id(&id);
        assert_eq!(hash1, hash2);
        assert_eq!(hash1.len(), 64); // SHA-256 hex = 64 chars
    }

    #[test]
    fn create_and_cleanup() {
        let tmp = tempfile::tempdir().ok();
        let tmp = tmp.as_ref().map(|t| t.path());
        if let Some(base) = tmp {
            let id = AgentId::new();
            let ctx = IsolatedContext::create(base, &id);
            assert!(ctx.is_ok());
            let ctx = ctx.ok();
            if let Some(ref ctx) = ctx {
                assert!(ctx.root().exists());
                ctx.cleanup().ok();
                assert!(!ctx.root().exists());
            }
        }
    }

    #[test]
    fn write_and_read_artifact() {
        let tmp = tempfile::tempdir().ok();
        let tmp = tmp.as_ref().map(|t| t.path());
        if let Some(base) = tmp {
            let id = AgentId::new();
            if let Ok(ctx) = IsolatedContext::create(base, &id) {
                ctx.write_artifact("test.json", b"{\"ok\":true}").ok();
                let data = ctx.read_artifact("test.json");
                assert!(data.is_ok());
                assert_eq!(data.ok().as_deref(), Some(b"{\"ok\":true}".as_slice()));
                ctx.cleanup().ok();
            }
        }
    }

    #[test]
    fn different_agents_get_different_paths() {
        let a = AgentId::new();
        let b = AgentId::new();
        let hash_a = IsolatedContext::hash_id(&a);
        let hash_b = IsolatedContext::hash_id(&b);
        assert_ne!(hash_a, hash_b);
    }
}
