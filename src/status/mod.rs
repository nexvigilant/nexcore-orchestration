//! HTTP status server for monitoring agent state.

pub mod routes;

use std::sync::Arc;

use tokio::net::TcpListener;
use tracing::info;

use crate::agent::registry::AgentRegistry;
use crate::error::{OrcError, OrcResult};

/// Shared state for the status server.
#[derive(Debug, Clone)]
pub struct StatusState {
    /// The agent registry to query.
    pub registry: Arc<AgentRegistry>,
}

/// HTTP status server exposing agent state via REST endpoints.
pub struct StatusServer {
    /// Port the server is listening on.
    port: u16,
}

impl StatusServer {
    /// Start the status server, auto-discovering a port in 3100–3199.
    pub async fn start(registry: Arc<AgentRegistry>) -> OrcResult<Self> {
        let port = Self::find_available_port(3100, 3199).await?;
        let state = StatusState { registry };

        let app = routes::router(state);
        let listener = TcpListener::bind(format!("127.0.0.1:{port}"))
            .await
            .map_err(OrcError::Io)?;

        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });

        info!(port, "status server started");
        Ok(Self { port })
    }

    /// Start on a specific port (for testing).
    pub async fn start_on_port(registry: Arc<AgentRegistry>, port: u16) -> OrcResult<Self> {
        let state = StatusState { registry };
        let app = routes::router(state);
        let listener = TcpListener::bind(format!("127.0.0.1:{port}"))
            .await
            .map_err(OrcError::Io)?;

        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });

        Ok(Self { port })
    }

    /// The port the server is listening on.
    #[must_use]
    pub fn port(&self) -> u16 {
        self.port
    }

    /// The base URL of the server.
    #[must_use]
    pub fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// Find an available port by attempting to bind in sequence.
    async fn find_available_port(start: u16, end: u16) -> OrcResult<u16> {
        for port in start..=end {
            if TcpListener::bind(format!("127.0.0.1:{port}")).await.is_ok() {
                return Ok(port);
            }
        }
        Err(OrcError::NoAvailablePort { start, end })
    }
}
