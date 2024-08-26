use crate::raft::messages::{AppendEntriesRequest, AppendEntriesResponse, Message, RequestVoteRequest, RequestVoteResponse};
use crate::raft::network::charm::{AppendEntriesRequestPb, AppendEntriesResponsePb, LogEntryPb, RequestVoteRequestPb, RequestVoteResponsePb};
use crate::raft::types::{Data, Index, LogEntry, NodeId, Term};

pub mod charm {
    tonic::include_proto!("raftpb");
}

pub struct Network {
}


impl Network {
    pub fn send(&self, node_id: &NodeId, message: &Message) {
        todo!();
    }
}

fn append_entries_request_pb_to_message(request: AppendEntriesRequestPb) -> AppendEntriesRequest {
    AppendEntriesRequest {
        term: Term(request.term),
        leader_id: NodeId(request.leader_id),
        prev_log_index: Index(request.prev_log_index),
        prev_log_term: Term(request.prev_log_term),
        entries: request.entries.into_iter().map(|entry| LogEntry {
            term: Term(entry.term),
            data: Data(entry.data),
        }).collect(),
        leader_commit: Index(request.leader_commit),
    }
}

fn append_entries_request_message_to_pb(request: AppendEntriesRequest) -> AppendEntriesRequestPb {
    AppendEntriesRequestPb {
        term: request.term.0,
        leader_id: request.leader_id.0,
        prev_log_index: request.prev_log_index.0,
        prev_log_term: request.prev_log_term.0,
        entries: request.entries.into_iter().map(|entry| LogEntryPb {
            term: entry.term.0,
            data: entry.data.0,
        }).collect(),
        leader_commit: request.leader_commit.0,
    }
}

fn append_entries_response_pb_to_message(response: AppendEntriesResponsePb) -> AppendEntriesResponse {
    AppendEntriesResponse {
        node_id: NodeId(response.node_id),
        term: Term(response.term),
        success: response.success,
        last_log_index: Index(response.last_log_index),
    }
}

fn append_entries_response_message_to_pb(response: AppendEntriesResponse) -> AppendEntriesResponsePb {
    AppendEntriesResponsePb {
        node_id: response.node_id.0,
        term: response.term.0,
        success: response.success,
        last_log_index: response.last_log_index.0,
    }
}

fn request_vote_request_pb_to_message(request: RequestVoteRequestPb) -> RequestVoteRequest {
    RequestVoteRequest {
        term: Term(request.term),
        candidate_id: NodeId(request.candidate_id),
        last_log_index: Index(request.last_log_index),
        last_log_term: Term(request.last_log_term),
    }
}

fn request_vote_request_message_to_pb(request: RequestVoteRequest) -> RequestVoteRequestPb {
    RequestVoteRequestPb {
        term: request.term.0,
        candidate_id: request.candidate_id.0,
        last_log_index: request.last_log_index.0,
        last_log_term: request.last_log_term.0,
    }
}

fn request_vote_response_pb_to_message(response: RequestVoteResponsePb) -> RequestVoteResponse {
    RequestVoteResponse {
        node_id: NodeId(response.node_id),
        term: Term(response.term),
        vote_granted: response.vote_granted,
    }
}

fn request_vote_response_message_to_pb(response: RequestVoteResponse) -> RequestVoteResponsePb {
    RequestVoteResponsePb {
        node_id: response.node_id.0,
        term: response.term.0,
        vote_granted: response.vote_granted,
    }
}
