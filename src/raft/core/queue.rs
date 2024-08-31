use crate::raft::messages::*;
use tokio::sync::oneshot;
use crate::raft::core::error::RaftCoreError;

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

    Propose {
        proposal: R,
        error_tx: oneshot::Sender<Result<(), RaftCoreError>>,
    },
}
