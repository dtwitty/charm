use std::collections::HashMap;

use crate::raft::messages::*;
use crate::raft::network::Network;
use crate::raft::node::Role::*;
use crate::raft::types::*;

struct Log {
    entries: Vec<LogEntry>,
}

impl Log {
    fn new() -> Log {
        Log {
            entries: vec![],
        }
    }

    fn append(&mut self, entry: LogEntry) {
        self.entries.push(entry);
    }

    fn get(&self, index: Index) -> Option<&LogEntry> {
        if index.0 == 0 {
            return None;
        }

        let idx = index.0 as usize - 1;
        self.entries.get(idx)
    }

    fn last_index(&self) -> Index {
        Index(self.entries.len() as u64)
    }

    fn last_log_term(&self) -> Term {
        self.entries.last().map(|entry| entry.term).unwrap_or(Term(0))
    }
}

struct CandidateState {
    /// The number of votes that the candidate has received in the current term.
    votes_received: u64,
}

struct LeaderState {
    /// For each server, index of the next log entry to send to that server.
    /// Initialized to leader's last log index + 1.
    next_index: HashMap<NodeId, Index>,

    /// For each server, index of the highest log entry known to be replicated on server.
    /// Initialized to 0, increases monotonically.
    match_index: HashMap<NodeId, Index>,
}

enum Role {
    Follower,
    Candidate(CandidateState),
    Leader(LeaderState),
}


struct RaftNode<N> {
    /// The unique identifier of this node.
    node_id: NodeId,

    /// The current term of the node.
    current_term: Term,

    /// The ID of the node that this node voted for in the current term.
    voted_for: Option<NodeId>,

    /// The log of entries that this node has received.
    log: Log,

    /// The index of the highest log entry known to be committed.
    /// Initialized to 0, increases monotonically.
    commit_index: Index,

    /// The index of the highest log entry applied to the state machine.
    /// Initialized to 0, increases monotonically.
    last_applied: Index,

    /// State that only exists if this node is a leader.
    /// If this node is not a leader, this field is `None`.
    role: Role,

    network: N,
}

impl<N: Network> RaftNode<N> {
    fn new(node_id: NodeId, network: N) -> RaftNode<N> {
        RaftNode {
            node_id,
            current_term: Term(0),
            voted_for: None,
            log: Log::new(),
            commit_index: Index(0),
            last_applied: Index(0),
            role: Follower,
            network,
        }
    }

    fn handle_message(&mut self, m: Message) {
        match m {
            Message::AppendEntriesRequest(req) => self.handle_append_entries_request(req),
            Message::AppendEntriesResponse(res) => self.handle_append_entries_response(res),
            Message::RequestVoteRequest(req) => self.handle_request_vote_request(req),
            Message::RequestVoteResponse(res) => self.handle_request_vote_response(res),
        }
    }

    fn handle_append_entries_request(&mut self, req: AppendEntriesRequest) {}

    fn handle_append_entries_response(&mut self, res: AppendEntriesResponse) {}

    fn handle_request_vote_request(&mut self, req: RequestVoteRequest) {
        /// TODO what happens if we are not a follower?
        if req.term < self.current_term {
            // This candidate is out of date.
            self.send_failed_vote_response(&req.candidate_id);
            return;
        }

        let can_vote = match self.voted_for {
            None => true,  // We haven't voted yet.
            Some(ref voted_for) => voted_for == *req.candidate_id,  // We already voted for this candidate.
        };
        if !can_vote {
            // We already voted for someone else.
            self.send_failed_vote_response(&req.candidate_id);
            return;
        }

        // Check last entry terms.
        let our_last_log_term = self.log.last_log_term();
        let candidate_last_log_term = req.last_log_term;
        if candidate_last_log_term > our_last_log_term {
            // The candidate has a higher term, so we approve!
            self.vote_for(&req.candidate_id);
            return;
        }

        // If the terms are the same, we need to check the log lengths.
        let our_last_log_index = self.log.last_index();
        let candidate_last_log_index = req.last_log_index;
        if candidate_last_log_index >= our_last_log_index {
            // The candidate has at least as much log as we do, so we approve!
            self.vote_for(&req.candidate_id);
            return;
        }

        // If we get here, the candidate's log is shorter than ours.
        self.send_failed_vote_response(&req.candidate_id);
    }

    fn send_failed_vote_response(&self, candidate_id: &NodeId) {
        let res = RequestVoteResponse {
            term: self.current_term,
            vote_granted: false,
        };
        self.network.send(candidate_id, &Message::RequestVoteResponse(res));
    }

    fn vote_for(&mut self, candidate_id: &NodeId) {
        self.voted_for = Some(candidate_id.clone());
        let res = RequestVoteResponse {
            term: self.current_term,
            vote_granted: true,
        };
        self.network.send(candidate_id, &Message::RequestVoteResponse(res));
    }

    fn handle_request_vote_response(&mut self, res: RequestVoteResponse) {
        if
    }
}