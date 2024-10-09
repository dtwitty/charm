use crate::raft::types::NodeId;

#[derive(Debug)]
pub enum RaftCoreError {
    NotReady,
    NotLeader {
        leader_id: Option<NodeId>
    }
}
