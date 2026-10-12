use std::{
    collections::HashSet,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use tonic::transport::Endpoint;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Peer {
    pub id: u64,
    pub address: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub node_id: Option<u64>,
    pub grpc_bind: SocketAddr,
    pub http_bind: SocketAddr,
    pub data_dir: PathBuf,
    pub nodes: Vec<Peer>,
    pub heartbeat_interval_ms: u64,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub rpc_timeout_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            node_id: None,
            grpc_bind: "0.0.0.0:50051".parse().unwrap(),
            http_bind: "0.0.0.0:8080".parse().unwrap(),
            data_dir: "data".into(),
            nodes: Vec::new(),
            heartbeat_interval_ms: 150,
            election_timeout_min_ms: 750,
            election_timeout_max_ms: 1500,
            rpc_timeout_ms: 300,
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        serde_yaml::from_slice(&bytes).context("invalid YAML config")
    }

    pub fn id(&self) -> u64 {
        self.node_id.expect("config must be validated before use")
    }

    pub fn validate(&self) -> Result<()> {
        let id = self
            .node_id
            .context("node_id is required in config or --node-id / RAFT_NODE_ID")?;
        ensure!(!self.nodes.is_empty(), "nodes must not be empty");
        let mut ids = HashSet::new();
        let mut addresses = HashSet::new();
        for node in &self.nodes {
            ensure!(node.id > 0, "node IDs must be positive");
            ensure!(ids.insert(node.id), "duplicate node ID {}", node.id);
            ensure!(
                addresses.insert(&node.address),
                "duplicate node address {}",
                node.address
            );
            let endpoint =
                Endpoint::from_shared(node.address.clone()).context("invalid gRPC address")?;
            ensure!(
                endpoint.uri().scheme_str() == Some("http"),
                "addresses must use http:// (TLS is not configured)"
            );
            ensure!(
                endpoint.uri().authority().is_some(),
                "address must include a host"
            );
            ensure!(
                endpoint.uri().path() == "/",
                "gRPC address must not include a path"
            );
        }
        ensure!(ids.contains(&id), "node_id {id} is absent from nodes");
        ensure!(
            self.grpc_bind != self.http_bind,
            "gRPC and HTTP bind addresses must differ"
        );
        ensure!(
            self.heartbeat_interval_ms > 0,
            "heartbeat interval must be positive"
        );
        ensure!(self.rpc_timeout_ms > 0, "RPC timeout must be positive");
        ensure!(
            self.election_timeout_min_ms > self.heartbeat_interval_ms.saturating_mul(3),
            "minimum election timeout must exceed three heartbeat intervals"
        );
        ensure!(
            self.election_timeout_min_ms > self.rpc_timeout_ms,
            "minimum election timeout must exceed RPC timeout"
        );
        ensure!(
            self.election_timeout_max_ms > self.election_timeout_min_ms,
            "election timeout max must exceed min"
        );
        ensure!(
            self.election_timeout_max_ms <= 86_400_000,
            "election timeout must not exceed one day"
        );
        Ok(())
    }

    pub fn rpc_timeout(&self) -> Duration {
        Duration::from_millis(self.rpc_timeout_ms)
    }
}
