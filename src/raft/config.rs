use std::time::Duration;

use derive_builder::Builder;
use rand::Rng;

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

impl RaftConfig {
    pub fn get_election_timeout(&self) -> Duration {
        let mut rng = rand::thread_rng();
        let election_timeout_min = self.election_timeout_min;
        let election_timeout_max = self.election_timeout_max;
        rng.gen_range(election_timeout_min..election_timeout_max)
    }
}
