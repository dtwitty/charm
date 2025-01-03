use std::time::Duration;

use crate::raft::types::NodeId;
use crate::rng::CharmRng;
use derive_builder::Builder;
use rand::Rng;

#[derive(Debug, Clone, PartialEq, Builder)]
pub struct RaftConfig<I> {
    pub node_id: NodeId,
    pub other_nodes: Vec<NodeId>,
    pub raft_storage_filename: String,
    pub raft_log_storage_filename: String,

    #[builder(default = "Duration::from_millis(500)")]
    pub election_timeout_min: Duration,

    #[builder(default = "Duration::from_millis(1000)")]
    pub election_timeout_max: Duration,

    #[builder(default = "Duration::from_millis(100)")]
    pub heartbeat_interval: Duration,

    pub node_info: I
}

impl<I> RaftConfig<I> {
    pub fn get_election_timeout(&self, rng: &mut CharmRng) -> Duration {
        let election_timeout_min = self.election_timeout_min;
        let election_timeout_max = self.election_timeout_max;
        rng.gen_range(election_timeout_min..election_timeout_max)
    }
}
