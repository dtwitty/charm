use crate::charm::config::CharmConfig;
use crate::charm::server::run_server;
use crate::charm::state_machine::CharmStateMachine;
use crate::raft::core::config::RaftConfigBuilder;
use crate::raft::run_raft;
use crate::raft::types::NodeId;
use crate::rng::CharmRng;
use derive_builder::Builder;
use std::future::pending;

#[derive(Debug, Clone, PartialEq, Eq, Builder)]
pub struct CharmPeer {
    pub host: String,
    pub charm_port: u16,
    pub raft_port: u16,
}

impl CharmPeer {
    pub(crate) fn to_node_id(&self) -> NodeId {
        NodeId {
            host: self.host.clone(),
            port: self.raft_port,
        }
    }

    #[must_use] pub fn raft_addr(&self) -> String {
        format!("http://{}:{}", self.host, self.raft_port)
    }

    #[must_use] pub fn charm_addr(&self) -> String {
        format!("http://{}:{}", self.host, self.charm_port)
    }
}

#[derive(Debug, Clone, Builder)]
pub struct CharmServerConfig {
    listen: CharmPeer,
    peers: Vec<CharmPeer>,
    rng_seed: u64,
    raft_storage_filename: String,
    raft_log_storage_filename: String,
}

pub async fn run_charm_server(config: CharmServerConfig) {
    assert!(!config.peers.contains(&config.listen), "a node must not be its own peer");
    let rng = CharmRng::new(config.rng_seed);
    let raft_node_id = config.listen.to_node_id();
    let other_nodes = config.peers.iter().map(CharmPeer::to_node_id).collect::<Vec<_>>();
    let raft_config = RaftConfigBuilder::default()
        .node_id(raft_node_id)
        .other_nodes(other_nodes)
        .raft_log_storage_filename(config.raft_log_storage_filename)
        .raft_storage_filename(config.raft_storage_filename)
        .build().unwrap();
    let charm_config = CharmConfig {
        listen: config.listen,
        peers: config.peers.clone(),
    };
    let sm = CharmStateMachine::new();
    let raft_handle = run_raft(raft_config, sm, rng.clone()).await;
    run_server(charm_config, raft_handle, rng);
    pending::<()>().await;
}