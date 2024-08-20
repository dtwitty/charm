use std::collections::HashMap;
use std::time::Duration;

use futures::future::Shared;
use futures::FutureExt;
use rand::Rng;
use tokio::select;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::watch;
use tokio::sync::watch::Sender;
use tokio::time::{sleep, Sleep};
use tracing::{debug, info, trace};

use crate::raft::commands::{NotLeaderError, ProposeRequest, ProposeResponse, RaftError, RaftRequest, RaftResult};
use crate::raft::config::RaftConfig;
use crate::raft::log::Log;
use crate::raft::messages::*;
use crate::raft::network::Network;
use crate::raft::node::Role::*;
use crate::raft::types::*;

struct FollowerState {
    /// The timer that will fire when the election timeout is reached.
    /// This is used to start a new election.
    election_timer: Shared<Sleep>
}

struct CandidateState {
    /// The number of votes that the candidate has received in the current term.
    votes_received: u64,

    /// The timer that will fire when the election timeout is reached.
    /// This is used to start a new election.
    election_timer: Shared<Sleep>,
}

struct PeerState {
    /// The index of the next log entry to send to that server.
    next_index: Index,

    /// The index of the highest log entry known to be replicated on server.
    match_index: Index,
}

struct LeaderState {
    peer_states: HashMap<NodeId, PeerState>,
    heartbeat_timer: Shared<Sleep>,
}

impl LeaderState {
    fn get_mut_peer_state(&mut self, node_id: &NodeId) -> &mut PeerState {
        self.peer_states.get_mut(node_id).expect("peer state not found")
    }

    fn get_peer_state(&self, node_id: &NodeId) -> &PeerState {
        self.peer_states.get(node_id).expect("peer state not found")
    }

    /// Returns the highest index that a majority of the cluster has reached.
    fn get_majority_match(&self) -> Index {
        let mut match_indexes = self.peer_states.values().map(|peer_state| peer_state.match_index).collect::<Vec<_>>();
        // Sort the matches in reverse order.
        match_indexes.sort_by(|a, b| b.cmp(a));
        // Get the match index of the N/2 most-advanced node.
        // The majority of the cluster (including this node) will be at least this advanced.
        match_indexes[match_indexes.len() / 2]
    }
}

enum Role {
    Follower(FollowerState),
    Candidate(CandidateState),
    Leader(LeaderState),
}

/*
PLAN:
Implement the `RaftNode` struct. It will interact directly with the sleeping system and use a
passed-in Network to send messages to other nodes. It will also have a `handle_message` method
that will be called when a message is received. There will finally be a `tick` method that waits for
either a timeout or a message, and then calls `handle_message` or `handle_timeout` as appropriate.

The driver method is simply a loop that calls `tick` and provides a Network object. Later it will provide
a handle to the state machine.

Next, implement a method to tell leaders to append logs.

This will be a good time to start testing. Exhaustively test any safety properties you find in the paper.

Implement applying logs to a state machine.


 */

pub struct RaftNode<N> {
    config: RaftConfig,

    /// The current term of the node.
    current_term: Term,

    /// The ID of the node that this node voted for in the current term.
    voted_for: Option<NodeId>,

    /// The log of entries that this node has received.
    log: Log,

    /// The index of the highest log entry known to be committed.
    /// Initialized to 0, increases monotonically.
    commit_index: Index,

    /// Used to watch for changes to the commit index, to complete requests.
    commit_index_watch_tx: watch::Sender<()>,
    commit_index_watch_rx: watch::Receiver<()>,

    /// The index of the highest log entry applied to the state machine.
    /// Initialized to 0, increases monotonically.
    last_applied: Index,

    /// State that only exists if this node is a leader.
    /// If this node is not a leader, this field is `None`.
    role: Role,

    /// Used to watch for changes to our role.
    role_watch_tx: watch::Sender<()>,
    role_watch_rx: watch::Receiver<()>,

    /// An interface for sending messages to other nodes.
    network: N,

    /// For receiving messages from other nodes.
    message_receiver: UnboundedReceiver<Message>,

    /// For handling requests.
    request_receiver: UnboundedReceiver<RaftRequest>,

}

impl<N: Network> RaftNode<N> {
    #[tracing::instrument(fields(node_id = config.node_id.0.clone()), skip_all)]
    pub async fn run(config: RaftConfig, network: N, message_receiver: UnboundedReceiver<Message>, request_receiver: UnboundedReceiver<RaftRequest>) {
        let mut node = RaftNode::new(config, network, message_receiver, request_receiver);
        loop {
            node.tick().await;
        }
    }

    pub fn new(config: RaftConfig, network: N, message_receiver: UnboundedReceiver<Message>, request_receiver: UnboundedReceiver<RaftRequest>) -> RaftNode<N> {
        let follower_state = FollowerState { election_timer: sleep(config.get_election_timeout()).shared() };
        let commit_index_watch = watch::channel(());
        let role_watch = watch::channel(());
        let mut node = RaftNode {
            config,
            current_term: Term(0),
            voted_for: None,
            log: Log::new(),
            commit_index: Index(0),
            commit_index_watch_tx: commit_index_watch.0,
            commit_index_watch_rx: commit_index_watch.1,
            last_applied: Index(0),
            role: Follower(follower_state),
            role_watch_tx: role_watch.0,
            role_watch_rx: role_watch.1,
            network,
            message_receiver,
            request_receiver,
        };

        node.reset_election_timer();
        node
    }

    async fn tick(&mut self) {
        trace!("Entering tick.");
        match self.role {
            Follower(ref follower_state) => {
                trace!("Running follower tick.");
                select! {
                     message = self.message_receiver.recv() => {
                          if let Some(message) = message {
                            self.handle_message(message);
                          }
                     }

                     _ = follower_state.election_timer.clone() => {
                          self.handle_election_timeout();
                     }
               }
            }

            Candidate(ref mut candidate_state) => {
                trace!("Running candidate tick.");
                select! {
                     message = self.message_receiver.recv() => {
                          if let Some(message) = message {
                            self.handle_message(message);
                          }
                     }

                     _ = candidate_state.election_timer.clone() => {
                          self.handle_election_timeout();
                     }
               }
            }

            Leader(ref mut leader_state) => {
                trace!("Running leader tick.");
                select! {
                     message = self.message_receiver.recv() => {
                          if let Some(message) = message {
                            self.handle_message(message);
                          }
                     }

                     _ = leader_state.heartbeat_timer.clone() => {
                          self.handle_heartbeat_timeout();
                     }
               }
            }
        }
    }

    fn handle_message(&mut self, message: Message) {
        match message {
            Message::AppendEntriesRequest(req) => {
                self.handle_append_entries_request(req);
            }

            Message::AppendEntriesResponse(res) => {
                self.handle_append_entries_response(res);
            }

            Message::RequestVoteRequest(req) => {
                self.handle_request_vote_request(req);
            }

            Message::RequestVoteResponse(res) => {
                self.handle_request_vote_response(res);
            }
        }
    }

    fn handle_election_timeout(&mut self) {
        info!("Hit election timeout! Starting election.");
        // Enter the next term.
        self.current_term = self.current_term.next();

        // Vote for ourselves.
        self.voted_for = Some(self.node_id().clone());

        // Become a candidate, and reset the election timer.
        let mut rng = rand::thread_rng();
        let election_timeout = rng.gen_range(self.config.election_timeout_min..self.config.election_timeout_max);
        let candidate_state = CandidateState { votes_received: 1, election_timer: sleep(election_timeout).shared() };
        self.update_role(Candidate(candidate_state));

        // Send RequestVoteRequests to all nodes.
        let req = RequestVoteRequest {
            term: self.current_term,
            candidate_id: self.node_id().clone(),
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_log_term(),
        };

        for node_id in &self.config.other_nodes {
            self.network.send(&node_id, &Message::RequestVoteRequest(req.clone()));
        }
    }

    fn handle_heartbeat_timeout(&mut self) {
        // We only do anything if we are the leader.
        if let Leader(ref mut leader_state) = self.role {
            debug!("Sending heartbeat on timeout.");
            leader_state.heartbeat_timer = sleep(self.config.heartbeat_interval).shared();
            self.broadcast_heartbeat();
        }
    }

    async fn handle_request(&mut self, req: RaftRequest) {
        debug!("Received RaftRequest: {:?}", req);
        match req {
            RaftRequest::Propose(propose_request, reply_to) => {
                tokio::spawn(async move {
                    let response = self.handle_propose_request(propose_request).await;
                    reply_to.send(response).unwrap();
                });
            }

            RaftRequest::Campaign(campaign_request, reply_to) => {}

            RaftRequest::GetEntries(get_entries_request, reply_to) => {}
        }
    }

    async fn handle_propose_request(&mut self, req: ProposeRequest) -> RaftResult<ProposeResponse> {
        let mut idx = Index(0);
        match self.role {
            Leader(ref mut leader_state) => {
                // We are the leader. Append the entry to the log.
                let entry = LogEntry {
                    term: self.current_term,
                    data: req.data,
                };

                idx = self.log.append(entry);
            }

            _ => {
                // We are not the leader. Respond with an error.
                return Err(RaftError::NotLeader(NotLeaderError { leader_id: None }));
            }
        }


        // If we got here, we were the leader, and we appended the entry.
        // Wait until either the commit index goes up to include `idx` or we lose leadership.
        loop {
            select! {
                _ = self.commit_index_watch_rx.changed() => {
                    if self.commit_index >= idx {
                        // The entry was committed.
                        return Ok(ProposeResponse { index: idx });
                    }
                }

                _ = self.role_watch_rx.changed() => {
                    if let Leader(_) = self.role {
                        // We are still the leader. Continue waiting.
                    } else {
                        // We are no longer the leader.
                        return Err(RaftError::NotLeader(NotLeaderError { leader_id: None }));
                    }
                }
            }
        }
    }

    fn handle_append_entries_request(&mut self, req: AppendEntriesRequest) {
        debug!("Received AppendEntriesRequest: {:?}", req);
        self.check_incoming_term(req.term);

        if req.term < self.current_term {
            // This request is out of date.
            debug!("Request has older term. Failing request.");
            return self.send_append_entries_response(&req.leader_id, false);
        }

        if let Candidate(_) = self.role {
            // Someone else in the same term became a leader before us.
            debug!("Another node became leader. Converting to follower.");
            self.convert_to_follower();
        }

        // Edge case: the log is empty.
        if self.log.last_index() == Index(0) {
            // The log is empty. We can accept any entries.
            debug!("Log is empty. Accepting request.");
            self.reset_election_timer();
            self.log.append_all(req.entries, Index(0));
            self.commit_index = req.leader_commit.min(self.log.last_index());
            return self.send_append_entries_response(&req.leader_id, true);
        }

        let prev_log_entry = self.log.get(req.prev_log_index);
        if prev_log_entry.is_none() {
            // We don't have the previous log entry.
            debug!("Previous log entry not found. Failing request.");
            return self.send_append_entries_response(&req.leader_id, false);
        }

        let prev_log_term = prev_log_entry.unwrap().term;
        if prev_log_term != req.prev_log_term {
            // The previous log term doesn't match.
            debug!("Previous log term doesn't match. Failing request.");
            return self.send_append_entries_response(&req.leader_id, false);
        }

        // The previous log term matches. Append the new entries.
        debug!("Accepting request.");
        self.reset_election_timer();
        self.log.append_all(req.entries, req.prev_log_index.next());

        if req.leader_commit > self.commit_index {
            self.commit_index = req.leader_commit.min(self.log.last_index());
        }

        debug!("Sending successful response.");
        self.send_append_entries_response(&req.leader_id, true);
    }

    fn send_append_entries_response(&self, leader_id: &NodeId, success: bool) {
        let res = AppendEntriesResponse {
            node_id: self.node_id().clone(),
            term: self.current_term,
            success,
            last_log_index: self.log.last_index(),
        };
        self.network.send(leader_id, &Message::AppendEntriesResponse(res));
    }

    fn handle_append_entries_response(&mut self, res: AppendEntriesResponse) {
        debug!("Received AppendEntriesResponse: {:?}", res);

        self.check_incoming_term(res.term);

        // If we are not the leader, we don't care about these responses.
        if let Leader(ref mut leader_state) = self.role {
            if res.success {
                debug!("Request was successful. Updating peer state for {:?}.", res.node_id);
                let peer_state = leader_state.get_mut_peer_state(&res.node_id);
                peer_state.match_index = res.last_log_index;
                peer_state.next_index = res.last_log_index.next();

                // Try to update the commit index.
                let majority_match = leader_state.get_majority_match();
                if majority_match > self.commit_index {
                    debug!("Cluster majority advanced to index {:?}. Updating commit index.", majority_match);
                    self.commit_index = majority_match;
                    self.commit_index_watch_tx.send(()).unwrap();
                }
            } else {
                // The AppendEntriesRequest failed. Decrement the next_index. We will retry later.
                debug!("Request failed. Decrementing next_index.");
                let peer_state = leader_state.get_mut_peer_state(&res.node_id);
                peer_state.next_index = peer_state.next_index.prev();
            }
        } else {
            // We are not the leader. Don't care.
            debug!("Not the leader. Ignoring response.");
        }
    }



    fn handle_request_vote_request(&mut self, req: RequestVoteRequest) {
        debug!("Received RequestVoteRequest: {:?}", req);

        self.check_incoming_term(req.term);

        if req.term < self.current_term {
            // This candidate is out of date.
            debug!("Candidate has older term. Failing vote request.");
            return self.send_failed_vote_response(&req.candidate_id);
        }

        let can_vote = match self.voted_for {
            None => true,  // We haven't voted yet.
            Some(ref voted_for) => voted_for == &req.candidate_id,  // We already voted for this candidate.
        };
        if !can_vote {
            // We already voted for someone else.
            debug!("Already voted for another candidate. Failing vote request.");
            return self.send_failed_vote_response(&req.candidate_id);
        }

        // Check last entry terms.
        let our_last_log_term = self.log.last_log_term();
        let candidate_last_log_term = req.last_log_term;
        if candidate_last_log_term > our_last_log_term {
            // The candidate has a higher term, so we approve!
            debug!("Candidate has higher term. Voting for candidate {:?}.", req.candidate_id);
            return self.vote_for(&req.candidate_id);
        }

        // If the terms are the same, we need to check the log lengths.
        let our_last_log_index = self.log.last_index();
        let candidate_last_log_index = req.last_log_index;
        if candidate_last_log_index >= our_last_log_index {
            // The candidate has at least as much log as we do, so we approve!
            debug!("Candidate has at least as much log as we do. Voting for candidate {:?}.", req.candidate_id);
            return self.vote_for(&req.candidate_id);
        }

        // If we get here, the candidate's log is shorter than ours.
        debug!("Candidate has shorter log. Failing vote request.");
        self.send_failed_vote_response(&req.candidate_id);
    }

    fn send_failed_vote_response(&self, candidate_id: &NodeId) {
        let res = RequestVoteResponse {
            node_id: self.node_id().clone(),
            term: self.current_term,
            vote_granted: false,
        };
        self.network.send(candidate_id, &Message::RequestVoteResponse(res));
    }

    fn vote_for(&mut self, candidate_id: &NodeId) {
        self.voted_for = Some(candidate_id.clone());

        let election_timeout = self.get_election_timeout();
        self.update_role(Follower(FollowerState { election_timer: sleep(election_timeout).shared() }));

        let res = RequestVoteResponse {
            node_id: self.node_id().clone(),
            term: self.current_term,
            vote_granted: true,
        };
        self.network.send(candidate_id, &Message::RequestVoteResponse(res));
    }

    fn handle_request_vote_response(&mut self, res: RequestVoteResponse) {
        debug!("Received RequestVoteResponse: {:?}", res);

        self.check_incoming_term(res.term);

        match self.role {
            Candidate(ref mut state) => {
                if res.vote_granted {
                    debug!("Vote granted by {:?}.", res.node_id);
                    state.votes_received += 1;


                    let majority = self.config.other_nodes.len() / 2 + 1;
                    if state.votes_received > majority as u64 {
                        // Great success!
                        self.become_leader();
                    }
                } else {
                    // The vote was not granted, just log it.
                    debug!("Vote not granted by {:?}.", res.node_id);
                }
            }

            _ => {
                // We are not currently a candidate. Don't care.
                debug!("Not a candidate. Ignoring response.");
            },
        }
    }

    fn become_leader(&mut self) {
        info!("Becoming leader!");
        let mut peer_states = HashMap::new();

        for node_id in &self.config.other_nodes {
            let next_index = self.log.last_index().next();
            let match_index = Index(0);
            peer_states.insert(node_id.clone(), PeerState { next_index, match_index });
        }

        let leader_state = LeaderState {
            peer_states,
            heartbeat_timer: sleep(self.config.heartbeat_interval).shared(),
        };
        self.update_role(Leader(leader_state));

        // Send initial empty AppendEntriesRequests to all nodes.
        self.broadcast_heartbeat();
    }

    fn broadcast_heartbeat(&mut self) {
        for node_id in &self.config.other_nodes {
            self.send_append_entries(&node_id);
        }
    }

    fn send_append_entries(&self, node_id: &NodeId) {
        if let Leader(ref leader_state) = self.role {
            let peer_state = leader_state.get_peer_state(node_id);
            let next_index = peer_state.next_index;
            let prev_log_index = next_index.prev();
            let prev_log_term = self.log.get(prev_log_index).map(|entry| entry.term).unwrap_or(Term(0));

            let entries = self.log.entries_from(next_index);

            let req = AppendEntriesRequest {
                term: self.current_term,
                leader_id: self.node_id().clone(),
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
            debug!("Received message with higher term. Converting to follower.");
            self.current_term = term;
            self.convert_to_follower();
        }
    }

    fn convert_to_follower(&mut self) {
        self.voted_for = None;
        let follower_state = FollowerState { election_timer: sleep(self.get_election_timeout()).shared() };
        self.update_role(Follower(follower_state));
    }

    fn get_election_timeout(&self) -> Duration {
        let mut rng = rand::thread_rng();
        let election_timeout_min = self.config.election_timeout_min;
        let election_timeout_max = self.config.election_timeout_max;
        rng.gen_range(election_timeout_min..election_timeout_max)
    }

    fn reset_election_timer(&mut self) {
        let election_timeout = self.get_election_timeout();
        match self.role {
            Follower(ref mut follower_state) => {
                follower_state.election_timer = sleep(election_timeout).shared();
            }

            Candidate(ref mut candidate_state) => {
                candidate_state.election_timer = sleep(election_timeout).shared();
            }

            Leader(_) => {
                // Leaders don't have election timers.
            }
        }
    }

    fn node_id(&self) -> &NodeId {
        &self.config.node_id
    }

    fn update_role(&mut self, role: Role) {
        self.role = role;
        self.role_watch_tx.send(()).unwrap();
    }
}

