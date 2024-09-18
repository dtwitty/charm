use crate::raft::types::RaftInfo;

tonic::include_proto!("charmpb");

impl From<RaftInfo> for ResponseHeader {
    fn from(raft_info: RaftInfo) -> Self {
        ResponseHeader {
            leader_id: raft_info.node_id.host,
            raft_term: raft_info.term.0,
            raft_index: raft_info.index.0,
        }
    }
}