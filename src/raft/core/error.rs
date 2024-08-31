use crate::raft::types::NodeId;

#[derive(Debug)]
pub enum RaftCoreError {
    NotLeader {
        leader_id: Option<NodeId>
    }
}
