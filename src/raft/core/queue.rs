use crate::raft::core::error::RaftCoreError;
use crate::raft::messages::{AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse};
use tokio::sync::oneshot;
use tracing::Span;

#[derive(Debug)]
pub enum CoreQueueEntry<R> {
    AppendEntriesRequest {
        request: AppendEntriesRequest,
        response_tx: oneshot::Sender<Result<AppendEntriesResponse, RaftCoreError>>,
    },

    RequestVoteRequest {
        request: RequestVoteRequest,
        response_tx: oneshot::Sender<Result<RequestVoteResponse, RaftCoreError>>,
    },

    AppendEntriesResponse(AppendEntriesResponse),
    
    RequestVoteResponse(RequestVoteResponse),

    Propose {
        request: R,
        commit_tx: oneshot::Sender<Result<(), RaftCoreError>>,
        span: Span,
    },
}

impl<R> CoreQueueEntry<R> {
    pub fn respond_err(self, err: RaftCoreError) {
        match self {
            CoreQueueEntry::AppendEntriesRequest { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }

            CoreQueueEntry::RequestVoteRequest { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }

            CoreQueueEntry::Propose { commit_tx, .. } => {
                let _ = commit_tx.send(Err(err));
            }

            _ => {}
        }
    }
}
