use crate::raft::core::error::RaftCoreError;
use crate::raft::messages::{AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse};
use tokio::sync::oneshot;
use tracing::Span;

#[derive(Debug)]
pub enum CoreQueueEntry<R> {
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
        request: R,
        commit_tx: oneshot::Sender<Result<(), RaftCoreError>>,
        span: Span,
    },
}
