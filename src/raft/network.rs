use crate::raft::messages::Message;
use crate::raft::types::NodeId;

pub trait Network {
    /// Send a message to the given node.
    fn send(&self, to: &NodeId, message: &Message);
}
