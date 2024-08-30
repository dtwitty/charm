use crate::raft::core::queue::CoreQueueEntry;
use crate::raft::messages::{AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Receiver;

#[derive(Debug, Clone)]
pub struct RaftCoreHandle {
    tx: UnboundedSender<CoreQueueEntry>,
}

impl RaftCoreHandle {
    pub fn append_entries_request(&self, request: AppendEntriesRequest) -> Receiver<AppendEntriesResponse> {
        let (response_tx, rx) = oneshot::channel();
        let entry = CoreQueueEntry::AppendEntriesRequest {
            request,
            response_tx,
        };
        self.tx.send(entry).unwrap();
        rx
    }

    pub fn request_vote_request(&self, request: RequestVoteRequest) -> Receiver<RequestVoteResponse> {
        let (response_tx, rx) = oneshot::channel();
        let entry = CoreQueueEntry::RequestVoteRequest {
            request,
            response_tx,
        };
        self.tx.send(entry).unwrap();
        rx
    }

    pub fn append_entries_response(&self, response: AppendEntriesResponse) {
        let entry = CoreQueueEntry::AppendEntriesResponse(response);
        self.tx.send(entry).unwrap();
    }

    pub fn request_vote_response(&self, response: RequestVoteResponse) {
        let entry = CoreQueueEntry::RequestVoteResponse(response);
        self.tx.send(entry).unwrap();
    }
}