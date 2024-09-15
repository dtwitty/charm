use crate::raft::core::error::RaftCoreError;
use crate::raft::core::queue::CoreQueueEntry;
use crate::raft::core::Role::{Candidate, Follower, Leader};
use crate::raft::messages::{AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse};
use crate::raft::network::outbound_network::OutboundNetworkHandle;
use crate::raft::state_machine::StateMachineHandle;
use crate::raft::types::{Data, Index, LogEntry, NodeId, Term};
use crate::rng::CharmRng;
use config::RaftConfig;
use futures::future::Shared;
use futures::FutureExt;
use log::Log;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::mem::swap;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot;
use tokio::time::{sleep, Sleep};
use tokio::{select, spawn};
use tracing::{debug, error, info, trace, warn, Instrument, Span};

pub mod handle;
mod queue;
mod log;
//pub mod node;
pub mod config;
pub mod error;
mod storage;

struct FollowerState {
    /// The timer that will fire when the election timeout is reached.
    /// This is used to start a new election.
    election_timer: Shared<Sleep>,

    /// Who we think the leader is.
    leader_id: Option<NodeId>,
}

struct CandidateState {
    /// The number of votes that the candidate has received in the current term.
    votes_received: u64,

    /// The timer that will fire when the election timeout is reached.
    /// This is used to start a new election.
    election_timer: Shared<Sleep>,
}

#[derive(Debug)]
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
    fn get_majority_match(&self, our_index: Index) -> Index {
        // Get the match indexes of peer nodes.
        let mut match_indexes = self.peer_states.values().map(|peer_state| peer_state.match_index).collect::<Vec<_>>();
        // Add our own index.
        match_indexes.push(our_index);

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

#[allow(dead_code)]
impl Role {
    pub fn is_leader(&self) -> bool {
        matches!(self, Leader(_))
    }

    pub fn is_follower(&self) -> bool {
        matches!(self, Follower(_))
    }

    pub fn is_candidate(&self) -> bool {
        matches!(self, Candidate(_))
    }
}

struct Proposal<R> {
    req: R,
    commit_tx: oneshot::Sender<Result<(), RaftCoreError>>,
}

pub struct RaftNode<R: Serialize + DeserializeOwned + Send + 'static> {
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

    /// The index of the last log entry applied to the state machine.
    /// Initialized to 0, increases monotonically.
    last_applied: Index,

    /// State that only exists if this node is a leader.
    /// If this node is not a leader, this field is `None`.
    role: Role,

    core_rx: UnboundedReceiver<CoreQueueEntry<R>>,
    outbound_network: OutboundNetworkHandle,
    state_machine: StateMachineHandle<R>,
    proposals: BTreeMap<Index, Proposal<R>>,
    rng: CharmRng,
}

impl<R: Serialize + DeserializeOwned + Send + 'static> RaftNode<R> {
    #[must_use] pub fn new(
        config: RaftConfig, core_rx: UnboundedReceiver<CoreQueueEntry<R>>, outbound_network: OutboundNetworkHandle, state_machine: StateMachineHandle<R>, mut rng: CharmRng) -> RaftNode<R> {
        let follower_state = FollowerState {
            election_timer: sleep(config.get_election_timeout(&mut rng)).shared(),
            leader_id: None,
        };
        let proposals = BTreeMap::new();

        let mut node = RaftNode {
            config,
            current_term: Term(0),
            voted_for: None,
            log: Log::new(),
            commit_index: Index(0),
            last_applied: Index(0),
            role: Follower(follower_state),
            core_rx,
            outbound_network,
            state_machine,
            proposals,
            rng,
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
                     cqe = self.core_rx.recv() => {
                          if let Some(cqe) = cqe {
                            self.handle_event(cqe);
                          }
                     }

                     () = follower_state.election_timer.clone() => {
                          self.handle_election_timeout();
                     }
               }
            }

            Candidate(ref mut candidate_state) => {
                trace!("Running candidate tick.");
                select! {
                     cqe = self.core_rx.recv() => {
                          if let Some(cqe) = cqe {
                            self.handle_event(cqe);
                          }
                     }

                     () = candidate_state.election_timer.clone() => {
                          self.handle_election_timeout();
                     }
               }
            }

            Leader(ref mut leader_state) => {
                trace!("Running leader tick.");
                select! {
                     cqe = self.core_rx.recv() => {
                          if let Some(cqe) = cqe {
                            self.handle_event(cqe);
                          }
                     }

                     () = leader_state.heartbeat_timer.clone() => {
                          self.handle_heartbeat_timeout();
                     }
               }
            }
        }
    }

    fn handle_event(&mut self, cqe: CoreQueueEntry<R>) {
        match cqe {
            CoreQueueEntry::AppendEntriesRequest { request, response_tx } => {
                let response = self.handle_append_entries_request(request);
                response_tx.send(response).unwrap();
            }

            CoreQueueEntry::RequestVoteRequest { request, response_tx } => {
                let response = self.handle_request_vote_request(request);
                response_tx.send(response).unwrap();
            }

            CoreQueueEntry::AppendEntriesResponse(response) => {
                self.handle_append_entries_response(response);
            }

            CoreQueueEntry::RequestVoteResponse(response) => {
                self.handle_request_vote_response(response);
            }

            CoreQueueEntry::Propose { proposal, commit_tx, span } => {
                span.in_scope(|| {
                    self.handle_propose(proposal, commit_tx);
                });
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
        let election_timeout = self.get_election_timeout();
        let candidate_state = CandidateState { votes_received: 1, election_timer: sleep(election_timeout).shared() };
        self.role = Candidate(candidate_state);

        // Send RequestVoteRequests to all nodes.
        let req = RequestVoteRequest {
            term: self.current_term,
            candidate_id: self.node_id().clone(),
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_log_term(),
        };

        for node_id in &self.config.other_nodes {
            self.outbound_network.request_vote(node_id.clone(), req.clone());
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

    fn handle_append_entries_request(&mut self, req: AppendEntriesRequest) -> AppendEntriesResponse {
        debug!("Received AppendEntriesRequest: {:?}", req);
        self.check_incoming_term(req.term);

        if req.term < self.current_term {
            // This request is out of date.
            warn!("Request from {:?} has term {:?}, which is older than our term {:?}. Failing request.", req.leader_id, req.term, self.current_term);
            return self.append_entries_response(false);
        }

        if let Leader(_) = self.role {
            error!("Another node {:?} claims to be leader with the same term! This should never happen! Converting to follower.", req.leader_id);
            self.convert_to_follower();
        }

        if let Candidate(_) = self.role {
            // Someone else in the same term became a leader before us.
            info!("Another node {:?} while we were a candidate became leader. Converting to follower.", req.leader_id);
            self.convert_to_follower();
        }

        // Edge case: the log is empty.
        if self.log.last_index() == Index(0) {
            // The log is empty. We can accept any entries.
            debug!("Log is empty. Accepting request.");
            self.reset_election_timer();
            for entry in req.entries {
                self.log.append(entry);
            }
            self.commit_index = req.leader_commit.min(self.log.last_index());
            self.update_leader(req.leader_id.clone());
            self.apply_committed();
            return self.append_entries_response(true);
        }

        let prev_log_entry = self.log.get(req.prev_log_index);
        if prev_log_entry.is_none() {
            // We don't have the previous log entry.
            warn!("Previous log entry not found. Failing request.");
            return self.append_entries_response(false);
        }

        let prev_log_term = prev_log_entry.unwrap().term;
        if prev_log_term != req.prev_log_term {
            // The previous log term doesn't match.
            warn!("Previous log term doesn't match. Failing request.");
            return self.append_entries_response(false);
        }

        // The previous log term matches. Append the new entries.
        debug!("Accepting request.");
        self.reset_election_timer();
        self.check_and_append(&req.leader_id, req.entries, req.prev_log_index.next());

        if req.leader_commit > self.commit_index {
            self.commit_index = req.leader_commit.min(self.log.last_index());
        }

        debug!("Sending successful response.");
        self.update_leader(req.leader_id.clone());
        self.apply_committed();
        self.append_entries_response(true)
    }

    fn check_and_append(&mut self, leader_id: &NodeId, entries: Vec<LogEntry>, mut prev_log_index: Index) {
        for entry in entries {
            if let Some(prev_entry) = self.log.get(prev_log_index) {
                if prev_entry.term != entry.term {
                    // There is a conflict. Truncate the log and append the new entry.
                    warn!("Log conflict detected at index {:?}. Truncating log.", prev_log_index);
                    self.log.truncate(prev_log_index);
                    // Fail the futures that got truncated.
                    let to_fail = self.proposals.split_off(&prev_log_index);
                    for (_, proposal) in to_fail {
                        proposal.commit_tx.send(Err(RaftCoreError::NotLeader {
                            leader_id: Some(leader_id.clone()),
                        })).unwrap();
                    }

                    self.log.append(entry);
                }
            } else {
                // No conflict, append the entry.
                self.log.append(entry);
            }

            prev_log_index = prev_log_index.next();
        }
    }

    fn append_entries_response(&self, success: bool) -> AppendEntriesResponse {
        AppendEntriesResponse {
            node_id: self.node_id().clone(),
            term: self.current_term,
            success,
            last_log_index: self.log.last_index(),
        }
    }

    fn handle_append_entries_response(&mut self, res: AppendEntriesResponse) {
        debug!("Received AppendEntriesResponse: {:?}", res);

        self.check_incoming_term(res.term);

        // If we are not the leader, we don't care about these responses.
        if let Leader(ref mut leader_state) = self.role {
            if res.success {
                let peer_state = leader_state.get_mut_peer_state(&res.node_id);
                peer_state.match_index = res.last_log_index;
                peer_state.next_index = res.last_log_index.next();
                debug!("Request was successful. Peer state for {:?} is now {:?}.", res.node_id, peer_state);

                // Try to update the commit index.
                let majority_match = leader_state.get_majority_match(self.log.last_index());
                if majority_match > self.commit_index {
                    debug!("Cluster majority advanced to index {:?}. Updating commit index.", majority_match);
                    self.commit_index = majority_match;

                    // Split off futures that can be completed.
                    let mut to_complete = self.proposals.split_off(&(majority_match.next()));
                    swap(&mut self.proposals, &mut to_complete);

                    // Complete the futures, and send them to the state machine.
                    for (_, proposal) in to_complete {
                        proposal.commit_tx.send(Ok(())).unwrap();
                        self.state_machine.apply(proposal.req);
                    }
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


    fn handle_request_vote_request(&mut self, req: RequestVoteRequest) -> RequestVoteResponse {
        debug!("Received RequestVoteRequest: {:?}", req);
        info!("Received vote request from {:?}.", req.candidate_id);

        self.check_incoming_term(req.term);

        if req.term < self.current_term {
            // This candidate is out of date.
            info!("Candidate has older term. Failing vote request.");
            return self.request_vote_response(false);
        }

        let can_vote = match self.voted_for {
            None => true,  // We haven't voted yet.
            Some(ref voted_for) => voted_for == &req.candidate_id,  // We already voted for this candidate.
        };
        if !can_vote {
            // We already voted for someone else.
            info!("Already voted for another candidate. Failing vote request.");
            return self.request_vote_response(false);
        }

        // Check last entry terms.
        let our_last_log_term = self.log.last_log_term();
        let candidate_last_log_term = req.last_log_term;
        if candidate_last_log_term > our_last_log_term {
            // The candidate has a higher term, so we approve!
            info!("Candidate has higher term. Voting for candidate {:?}.", req.candidate_id);
            self.vote_for(&req.candidate_id);
            return self.request_vote_response(true);
        }

        // If the terms are the same, we need to check the log lengths.
        let our_last_log_index = self.log.last_index();
        let candidate_last_log_index = req.last_log_index;
        if candidate_last_log_index >= our_last_log_index {
            // The candidate has at least as much log as we do, so we approve!
            info!("Candidate has at least as much log as we do. Voting for candidate {:?}.", req.candidate_id);
            self.vote_for(&req.candidate_id);
            return self.request_vote_response(true);
        }

        // If we get here, the candidate's log is shorter than ours.
        info!("Candidate has shorter log. Failing vote request.");
        self.request_vote_response(false)
    }

    fn request_vote_response(&self, vote_granted: bool) -> RequestVoteResponse {
        RequestVoteResponse {
            node_id: self.node_id().clone(),
            term: self.current_term,
            vote_granted,
        }
    }


    fn vote_for(&mut self, candidate_id: &NodeId) {
        self.voted_for = Some(candidate_id.clone());

        let election_timeout = self.get_election_timeout();
        let role = Follower(FollowerState { election_timer: sleep(election_timeout).shared(), leader_id: None });
        self.role = role;
    }

    fn handle_request_vote_response(&mut self, res: RequestVoteResponse) {
        debug!("Received RequestVoteResponse: {:?}", res);

        self.check_incoming_term(res.term);

        match self.role {
            Candidate(ref mut state) => {
                if res.vote_granted {
                    state.votes_received += 1;


                    let majority = self.config.other_nodes.len() / 2 + 1;
                    debug!("Vote granted by {:?}. We now have {:?} out of {:?} needed votes", res.node_id, state.votes_received, majority);
                    if state.votes_received > majority as u64 && !self.role.is_leader() {
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

    fn handle_propose(&mut self, request: R, commit_tx: oneshot::Sender<Result<(), RaftCoreError>>) {
        match self.role {
            Candidate(_) => {
                debug!("Not the leader, but a candidate. Failing proposal.");
                let res = Err(RaftCoreError::NotLeader { leader_id: None });
                commit_tx.send(res).unwrap();
            }

            Follower(ref follower_state) => {
                debug!("Not the leader, but a follower. Failing proposal.");
                let res = Err(RaftCoreError::NotLeader { leader_id: follower_state.leader_id.clone() });
                commit_tx.send(res).unwrap();
            }

            Leader(_) => {
                let serialized = serde_json::to_vec(&request).unwrap();
                let log_entry = LogEntry { term: self.current_term, data: Data(serialized) };
                let index = self.log.append(log_entry);
                debug!("Proposal appended to log at index {:?}.", index);
                self.proposals.insert(index, Proposal { req: request, commit_tx });
            }
        }
    }

    fn become_leader(&mut self) {
        info!("Becoming the leader in term {:?}!", self.current_term);
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
        let role = Leader(leader_state);
        self.role = role;

        // Send initial empty AppendEntriesRequests to all nodes.
        self.broadcast_heartbeat();
    }

    fn broadcast_heartbeat(&mut self) {
        for node_id in &self.config.other_nodes {
            self.send_append_entries(node_id);
        }
    }

    fn send_append_entries(&self, node_id: &NodeId) {
        if let Leader(ref leader_state) = self.role {
            let peer_state = leader_state.get_peer_state(node_id);
            let next_index = peer_state.next_index;
            let prev_log_index = next_index.prev();
            let prev_log_term = self.log.get(prev_log_index).map_or(Term(0), |entry| entry.term);

            let entries = self.log.entries_from(next_index);

            let req = AppendEntriesRequest {
                term: self.current_term,
                leader_id: self.node_id().clone(),
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            };

            self.outbound_network.append_entries(node_id.clone(), req);
        }
    }

    // Convert to follower if the term is higher than the current term.
    fn check_incoming_term(&mut self, term: Term) {
        if self.current_term < term {
            info!("Received message with term {:?} higher than our term {:?}. Converting to follower.", term, self.current_term);
            self.current_term = term;
            self.convert_to_follower();
        }
    }

    fn convert_to_follower(&mut self) {
        self.voted_for = None;
        let follower_state = FollowerState { election_timer: sleep(self.get_election_timeout()).shared(), leader_id: None };
        let role = Follower(follower_state);
        self.role = role;
    }

    fn update_leader(&mut self, leader_id: NodeId) {
        let election_timeout = self.get_election_timeout();
        if let Follower(ref mut follower_state) = self.role {
            if let Some(ref old_leader) = follower_state.leader_id {
                if old_leader != &leader_id {
                    info!("Leader changed from {:?} to {:?}.", old_leader, leader_id);
                }
            }
            follower_state.leader_id = Some(leader_id);
            follower_state.election_timer = sleep(election_timeout).shared();
        } else {
            panic!("Expected follower role.");
        }
    }

    fn apply_committed(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied = self.last_applied.next();
            let entry = self.log.get(self.last_applied).expect("entry not found");
            let req: R = serde_json::from_slice(&entry.data.0).expect("deserialization failed");
            self.state_machine.apply(req);
        }
    }

    fn get_election_timeout(&mut self) -> Duration {
        self.config.get_election_timeout(&mut self.rng)
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
}

#[tracing::instrument(skip_all)]
async fn run<R: Serialize + DeserializeOwned + Send + 'static>(config: RaftConfig, core_rx: UnboundedReceiver<CoreQueueEntry<R>>, outbound_network: OutboundNetworkHandle, state_machine: StateMachineHandle<R>, rng: CharmRng, span: Span) {
    async move {
        let mut node = RaftNode::new(config, core_rx, outbound_network, state_machine, rng);
        loop {
            node.tick().await;
        }
    }.instrument(span).await;
}

pub fn run_core<R: Serialize + DeserializeOwned + Send + 'static>(config: RaftConfig, core_rx: UnboundedReceiver<CoreQueueEntry<R>>, outbound_network: OutboundNetworkHandle, state_machine: StateMachineHandle<R>, rng: CharmRng) {
    let span = Span::current();
    spawn(run(config, core_rx, outbound_network, state_machine, rng, span));
}




