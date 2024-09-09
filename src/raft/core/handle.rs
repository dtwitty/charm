use crate::raft::core::error::RaftCoreError;
use crate::raft::core::queue::CoreQueueEntry;
use crate::raft::messages::{AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse};
use crate::raft::types::NodeId;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Receiver;
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
    pub fn new(node_id: NodeId, tx: UnboundedSender<CoreQueueEntry<R>>) -> Self {
        Self {
            node_id,
            tx,
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id.clone()
    }

    #[instrument(skip(self))]
    pub fn append_entries_request(&self, request: AppendEntriesRequest) -> Receiver<AppendEntriesResponse> {
        let (response_tx, rx) = oneshot::channel();
        let entry = CoreQueueEntry::AppendEntriesRequest {
            request,
            response_tx,
        };
        self.tx.send(entry).unwrap();
        rx
    }

    #[instrument(skip(self))]
    pub fn request_vote_request(&self, request: RequestVoteRequest) -> Receiver<RequestVoteResponse> {
        let (response_tx, rx) = oneshot::channel();
        let entry = CoreQueueEntry::RequestVoteRequest {
            request,
            response_tx,
        };
        self.tx.send(entry).unwrap();
        rx
    }

    #[instrument(skip(self))]
    pub fn append_entries_response(&self, response: AppendEntriesResponse) {
        let entry = CoreQueueEntry::AppendEntriesResponse(response);
        self.tx.send(entry).unwrap();
    }

    #[instrument(skip(self))]
    pub fn request_vote_response(&self, response: RequestVoteResponse) {
        let entry = CoreQueueEntry::RequestVoteResponse(response);
        self.tx.send(entry).unwrap();
    }

    #[instrument(skip_all)]
    pub fn propose(&self, proposal: R) -> Receiver<Result<(), RaftCoreError>> {
        let (commit_tx, rx) = oneshot::channel();
        let entry = CoreQueueEntry::Propose {
            proposal,
            commit_tx,
            span: Span::current(),
        };
        self.tx.send(entry).unwrap();
        rx
    }
}