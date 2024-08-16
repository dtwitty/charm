use std::collections::HashMap;

use crate::raft::events::Timer;
use crate::raft::messages::*;
use crate::raft::node::Role::*;
use crate::raft::raft_driver::RaftDriver;
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

struct PeerState {
    /// The index of the next log entry to send to that server.
    next_index: Index,

    /// The index of the highest log entry known to be replicated on server.
    match_index: Index,
}

struct LeaderState {
    peer_states: HashMap<NodeId, PeerState>,
}

enum Role {
    Follower,
    Candidate(CandidateState),
    Leader(LeaderState),
}

struct RaftNode<D> {
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

    /// The interface to the outside world.
    /// This is used to send messages and set timers.
    driver: D,
}

impl<D: RaftDriver> RaftNode<D> {
    fn new(node_id: NodeId, driver: D) -> RaftNode<D> {
        RaftNode {
            node_id,
            current_term: Term(0),
            voted_for: None,
            log: Log::new(),
            commit_index: Index(0),
            last_applied: Index(0),
            role: Follower,
            driver,
        }
    }

    fn handle_timer(&mut self, timer: Timer) {
        match timer {
            Timer::ElectionTimeout => self.handle_election_timeout(),
            Timer::HeartbeatTimeout => self.handle_heartbeat_timeout(),
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
            node_id: self.node_id.clone(),
            term: self.current_term,
            success,
            last_log_index: self.log.last_index(),
        };
        self.driver.send(leader_id, &Message::AppendEntriesResponse(res));
    }


    fn handle_append_entries_response(&mut self, res: AppendEntriesResponse) {
        self.check_incoming_term(res.term);

        // If we are not the leader, we don't care about these responses.
        if let Leader(ref mut leader_state) = self.role {
            if res.success {
                let peer_state = self.get_mut_peer_state(&res.node_id);
                peer_state.match_index = res.last_log_index;
                peer_state.next_index = res.last_log_index.next();
            } else {
                // The AppendEntriesRequest failed. Decrement the next_index. We will retry later.
                let peer_state = self.get_mut_peer_state(&res.node_id);
                peer_state.next_index = peer_state.next_index.prev();
            }
        }
    }

    fn get_mut_peer_state(&mut self, node_id: &NodeId) -> &mut PeerState {
        if let Leader(ref mut leader_state) = self.role {
            leader_state.peer_states.get_mut(node_id).expect("peer state not found")
        } else {
            panic!("not a leader")
        }
    }

    fn get_peer_state(&self, node_id: &NodeId) -> &PeerState {
        if let Leader(ref leader_state) = self.role {
            leader_state.peer_states.get(node_id).expect("peer state not found")
        } else {
            panic!("not a leader")
        }
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
            node_id: self.node_id.clone(),
            term: self.current_term,
            vote_granted: false,
        };
        self.driver.send(candidate_id, &Message::RequestVoteResponse(res));
    }

    fn vote_for(&mut self, candidate_id: &NodeId) {
        self.voted_for = Some(candidate_id.clone());
        self.role = Follower;
        let res = RequestVoteResponse {
            node_id: self.node_id.clone(),
            term: self.current_term,
            vote_granted: true,
        };
        self.driver.send(candidate_id, &Message::RequestVoteResponse(res));
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

                    let majority_votes = (self.driver.num_nodes() / 2) as u64 + 1;
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
        let mut peer_states = HashMap::new();

        for node_id in self.driver.nodes() {
            let next_index = self.log.last_index().next();
            let match_index = Index(0);
            peer_states.insert(node_id.clone(), PeerState { next_index, match_index });
        }

        self.role = Leader(LeaderState { peer_states });

        // Send initial empty AppendEntriesRequests to all nodes.
        self.broadcast_heartbeat();
    }

    fn broadcast_heartbeat(&self) {
        for node_id in self.driver.nodes() {
            self.heartbeat_node(node_id);
        }
    }

    fn heartbeat_node(&self, node_id: &NodeId) {
        self.send_append_entries(node_id, vec![]);
    }

    fn append_outstanding_entries(&self, node_id: &NodeId) {
        for node_id in self.driver.nodes() {
            let entries = self.get_outstanding_entries(node_id);
            self.send_append_entries(node_id, entries);
        }
    }

    fn send_append_entries(&self, node_id: &NodeId, entries: Vec<LogEntry>) {
        if let Leader(ref leader_state) = self.role {
            let peer_state = self.get_peer_state(node_id);
            let next_index = peer_state.next_index;
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

            self.driver.send(node_id, &Message::AppendEntriesRequest(req));
        }
    }

    fn get_outstanding_entries(&self, node_id: &NodeId) -> Vec<LogEntry> {
        if let Leader(ref leader_state) = self.role {
            let peer_state = self.get_peer_state(node_id);
            let next_index = peer_state.next_index;
            self.log.entries[next_index.0 as usize..].to_vec()
        } else {
            panic!("not a leader");
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

    fn handle_election_timeout(&mut self) {
        // Enter the next term.
        self.current_term = self.current_term.next();

        // Vote for ourselves.
        self.voted_for = Some(self.node_id.clone());

        // Become a candidate.
        self.role = Candidate(CandidateState { votes_received: 1 });

        // Send RequestVoteRequests to all nodes.
        let req = RequestVoteRequest {
            term: self.current_term,
            candidate_id: self.node_id.clone(),
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_log_term(),
        };

        for node_id in self.driver.nodes() {
            self.driver.send(&node_id, &Message::RequestVoteRequest(req.clone()));
        }
    }

    fn handle_heartbeat_timeout(&mut self) {
        /// We only do anything if we are the leader.
        if let Leader(_) = self.role {
            self.append_outstanding_entries(&self.node_id);
        }
    }
}