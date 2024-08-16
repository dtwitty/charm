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

    fn append<I: IntoIterator<Item=LogEntry>>(&mut self, entries: I, mut index: Index) {
        for entry in entries {
            let existing_entry = self.get(index);
            match existing_entry {
                Some(existing_entry) => {
                    if existing_entry.term != entry.term {
                        // Remove all entries from here on.
                        self.entries.truncate(index.0 as usize - 1);

                        self.entries.push(entry);
                    }

                    // The entry already exists, so we don't need to do anything.
                }

                _ => {
                    // We are at the end of the log. Go ahead and append the entry.
                    self.entries.push(entry);
                }
            }
            index = index.next();
        }
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

    fn handle_append_entries_request(&mut self, req: AppendEntriesRequest) {
        self.check_incoming_term(req.term);

        if req.term < self.current_term {
            // This request is out of date.
            return self.send_append_entries_response(&req.leader_id, false);
        }

        if let Candidate(_) = self.role {
            // Someone else in the same term became a leader before us.
            self.convert_to_follower();
        }

        let prev_log_entry = self.log.get(req.prev_log_index);
        if prev_log_entry.is_none() {
            // We don't have the previous log entry.
            return self.send_append_entries_response(&req.leader_id, false);
        }

        let prev_log_term = prev_log_entry.unwrap().term;
        if prev_log_term != req.prev_log_term {
            // The previous log term doesn't match.
            return self.send_append_entries_response(&req.leader_id, false);
        }

        // The previous log term matches. Append the new entries.
        self.log.append(req.entries, req.prev_log_index.next());

        if req.leader_commit > self.commit_index {
            self.commit_index = req.leader_commit.min(self.log.last_index());
        }

        self.send_append_entries_response(&req.leader_id, true);
    }

    fn send_append_entries_response(&self, leader_id: &NodeId, success: bool) {
        let res = AppendEntriesResponse {
            term: self.current_term,
            success,
        };
        self.network.send(leader_id, &Message::AppendEntriesResponse(res));
    }


    fn handle_append_entries_response(&mut self, res: AppendEntriesResponse) {
        self.check_incoming_term(res.term);
    }

    fn handle_request_vote_request(&mut self, req: RequestVoteRequest) {
        self.check_incoming_term(req.term);

        if req.term < self.current_term {
            // This candidate is out of date.
            return self.send_failed_vote_response(&req.candidate_id);
        }

        let can_vote = match self.voted_for {
            None => true,  // We haven't voted yet.
            Some(ref voted_for) => voted_for == *req.candidate_id,  // We already voted for this candidate.
        };
        if !can_vote {
            // We already voted for someone else.
            return self.send_failed_vote_response(&req.candidate_id);
        }

        // Check last entry terms.
        let our_last_log_term = self.log.last_log_term();
        let candidate_last_log_term = req.last_log_term;
        if candidate_last_log_term > our_last_log_term {
            // The candidate has a higher term, so we approve!
            return self.vote_for(&req.candidate_id);
        }

        // If the terms are the same, we need to check the log lengths.
        let our_last_log_index = self.log.last_index();
        let candidate_last_log_index = req.last_log_index;
        if candidate_last_log_index >= our_last_log_index {
            // The candidate has at least as much log as we do, so we approve!
            return self.vote_for(&req.candidate_id);
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
        self.role = Follower;
        let res = RequestVoteResponse {
            term: self.current_term,
            vote_granted: true,
        };
        self.network.send(candidate_id, &Message::RequestVoteResponse(res));
    }

    fn handle_request_vote_response(&mut self, res: RequestVoteResponse) {
        self.check_incoming_term(res.term);

        match self.role {
            Candidate(ref mut state) => {
                if res.term > self.current_term {
                    // We are out of date. Become a follower.
                    self.current_term = res.term;
                    self.role = Follower;
                    self.voted_for = None;
                } else if res.vote_granted {
                    state.votes_received += 1;

                    let majority_votes = (self.network.num_nodes() / 2) as u64 + 1;
                    if state.votes_received > majority_votes {
                        // Great success!
                        self.become_leader();
                    }
                }
            }

            _ => {
                // We are not currently a candidate. Don't care.
            },
        }
    }

    fn become_leader(&mut self) {
        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();

        for node_id in self.network.nodes() {
            next_index.insert(node_id.clone(), self.log.last_index().next());
            match_index.insert(node_id.clone(), Index(0));
        }

        self.role = Leader(LeaderState { next_index, match_index });

        // Send initial empty AppendEntriesRequests to all nodes.
        self.broadcast_heartbeat();
    }

    fn broadcast_heartbeat(&self) {
        for node_id in self.network.nodes() {
            self.heartbeat_node(node_id);
        }
    }

    fn heartbeat_node(&self, node_id: &NodeId) {
        self.send_append_entries(node_id, vec![]);
    }

    fn send_append_entries(&self, node_id: &NodeId, entries: Vec<LogEntry>) {
        if let Leader(ref leader_state) = self.role {
            let next_index = leader_state.next_index.get(node_id).expect(format!("next_index not found for node {}", node_id).as_str());
            let prev_log_index = next_index.prev();
            let prev_log_term = self.log.get(prev_log_index).map(|entry| entry.term).expect("prev_log_index should exist");

            let entries = self.log.entries[next_index.0 as usize..].to_vec();

            let req = AppendEntriesRequest {
                term: self.current_term,
                leader_id: self.node_id.clone(),
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            };

            self.network.send(node_id, &Message::AppendEntriesRequest(req));
        }
    }

    // Convert to follower if the term is higher than the current term.
    fn check_incoming_term(&mut self, term: Term) {
        if self.current_term < term {
            self.current_term = term;
            self.convert_to_follower();
        }
    }

    fn convert_to_follower(&mut self) {
        self.voted_for = None;
        self.role = Follower;
    }
}