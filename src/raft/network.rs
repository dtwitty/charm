use crate::raft::messages::Message;
use crate::raft::types::NodeId;

pub trait Network {
    fn send(&self, node_id: &NodeId, msg: &Message);
}