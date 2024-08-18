use std::time::Duration;

use derive_builder::Builder;

use crate::raft::types::NodeId;

#[derive(Debug, Clone, PartialEq, Builder)]
pub struct RaftConfig {
    pub node_id: NodeId,
    pub other_nodes: Vec<NodeId>,

    #[builder(default = "Duration::from_millis(150)")]
    pub election_timeout_min: Duration,

    #[builder(default = "Duration::from_millis(300)")]
    pub election_timeout_max: Duration,

    #[builder(default = "Duration::from_millis(100)")]
    pub heartbeat_interval: Duration,
}
