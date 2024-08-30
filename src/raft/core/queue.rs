use crate::raft::messages::*;
use tokio::sync::oneshot;

#[derive(Debug)]
pub enum CoreQueueEntry<R: Send + 'static> {
    AppendEntriesRequest {
        request: AppendEntriesRequest,
        response_tx: oneshot::Sender<AppendEntriesResponse>,
    },

    RequestVoteRequest {
        request: RequestVoteRequest,
        response_tx: oneshot::Sender<RequestVoteResponse>,
    },

    AppendEntriesResponse(AppendEntriesResponse),
    RequestVoteResponse(RequestVoteResponse),

    Propose(R),
}