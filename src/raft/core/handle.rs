use crate::raft::core::error::RaftCoreError;
use crate::raft::core::error::RaftCoreError::NotReady;
use crate::raft::core::queue::CoreQueueEntry;
use crate::raft::messages::{AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse};
use crate::raft::types::NodeId;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tracing::{instrument, Span};

#[derive(Debug)]
pub struct RaftCoreHandle<R: Send + 'static> {
    node_id: NodeId,
    tx: UnboundedSender<CoreQueueEntry<R>>,
}

impl <R: Send + 'static> Clone for RaftCoreHandle<R> {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id.clone(),
            tx: self.tx.clone(),
        }
    }
}

impl<R: Send + 'static> RaftCoreHandle<R> {
    #[must_use] pub fn new(node_id: NodeId, tx: UnboundedSender<CoreQueueEntry<R>>) -> Self {
        Self {
            node_id,
            tx,
        }
    }

    #[must_use] pub fn node_id(&self) -> NodeId {
        self.node_id.clone()
    }

    #[instrument(skip(self))]
    pub async fn append_entries_request(&self, request: AppendEntriesRequest) -> Result<AppendEntriesResponse, RaftCoreError> {
        let (response_tx, rx) = oneshot::channel();
        let entry = CoreQueueEntry::AppendEntriesRequest {
            request,
            response_tx,
        };
        if let Err(e) = self.tx.send(entry) {
            let _ = e.0.respond_err(NotReady);
        }
        rx.await.map_err(|_| NotReady)?
    }

    #[instrument(skip(self))]
    pub async fn request_vote_request(&self, request: RequestVoteRequest) -> Result<RequestVoteResponse, RaftCoreError> {
        let (response_tx, rx) = oneshot::channel();
        let entry = CoreQueueEntry::RequestVoteRequest {
            request,
            response_tx,
        };
        if let Err(e) = self.tx.send(entry) {
            let _ = e.0.respond_err(NotReady);
        }
        rx.await.map_err(|_| NotReady)?
    }

    #[instrument(skip(self))]
    pub fn append_entries_response(&self, response: AppendEntriesResponse) -> Result<(), RaftCoreError> {
        let entry = CoreQueueEntry::AppendEntriesResponse(response);
        self.tx.send(entry).map_err(|_| NotReady)
    }

    #[instrument(skip(self))]
    pub fn request_vote_response(&self, response: RequestVoteResponse) -> Result<(), RaftCoreError> {
        let entry = CoreQueueEntry::RequestVoteResponse(response);
        self.tx.send(entry).map_err(|_| NotReady)
    }

    #[instrument(skip_all)]
    pub async fn propose(&self, proposal: R) -> Result<(), RaftCoreError> {
        let (commit_tx, rx) = oneshot::channel();
        let entry = CoreQueueEntry::Propose {
            request: proposal,
            commit_tx,
            span: Span::current(),
        };
        if let Err(e) = self.tx.send(entry) {
            let _ = e.0.respond_err(NotReady);
        }
        rx.await.map_err(|_| NotReady)?
    }
}