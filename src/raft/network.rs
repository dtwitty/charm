use crate::raft::messages::*;
use crate::raft::types::NodeId;

enum NetworkError {
    NodeNotFound,
    Timeout,
}

pub trait Network<'a> {
    fn num_nodes() -> usize;
    fn nodes() -> impl Iterator<Item=&'a NodeId>;

    async fn send_append_entries_request(&self, to: &NodeId, rpc: AppendEntriesRequest) -> Result<AppendEntriesResponse, NetworkError>;
    async fn send_request_vote_request(&self, to: &NodeId, rpc: RequestVoteRequest) -> Result<RequestVoteResponse, NetworkError>;
}