use crate::raft::messages::Message;
use crate::raft::types::NodeId;

pub trait RaftDriver {
    fn majority(&self) -> usize {
        self.nodes().len() / 2 + 1
    }

    /// The nodes of this raft group.
    fn nodes(&self) -> Vec<NodeId>;

    /// Send a message to the given node.
    fn send(&self, to: &NodeId, message: &Message);

    /// Mark that we have received a message from a leader or candidate.
    fn reset_election_timer(&self);

    /// Mark that we are sending heartbeats.
    fn reset_heartbeat_timer(&self);
}
