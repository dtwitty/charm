use crate::raft::core::config::RaftConfig;
use crate::raft::core::error::RaftCoreError;
use crate::raft::core::node::Role::{Candidate, Follower, Leader};
use crate::raft::core::queue::CoreQueueEntry;
use crate::raft::core::storage::{CoreStorage, LogStorage};
use crate::raft::messages::{AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse};
use crate::raft::network::outbound_network::OutboundNetworkHandle;
use crate::raft::state_machine::StateMachineHandle;
use crate::raft::types::{Data, Index, LogEntry, NodeId, RaftInfo, Term};
use crate::rng::CharmRng;
use futures::future::Shared;
use futures::FutureExt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::mem::swap;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot;
use tokio::time::{sleep, Sleep};
use tokio::{select, spawn};
use tracing::{debug, error, info, trace, warn, Instrument, Span};

struct FollowerState {
    /// The timer that will fire when the election timeout is reached.
    /// This is used to start a new election.
    election_timer: Shared<Sleep>,

    /// Who we think the leader is.
    leader_id: Option<NodeId>,
}

struct CandidateState {
    /// The nodes (including ourselves) who have voted for us.
    votes_received: BTreeSet<NodeId>,

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


struct Proposal<R> {
    request: R,
    commit_tx: oneshot::Sender<Result<(), RaftCoreError>>,
}

impl<R> Proposal<R> {
    // The proper way to access the request is to get a commit success.
    // We also return the request here because `send` consumes `commit_tx`.
    pub fn commit_success(self) -> R {
        let r = self.commit_tx.send(Ok(()));
        if r.is_err() {
            warn!("Failed to send commit notification. Client request may already be gone.");
        }
        self.request
    }

    pub fn commit_failure(self, err: RaftCoreError) {
        let r = self.commit_tx.send(Err(err));
        if r.is_err() {
            warn!("Failed to send commit failure notification. Client request may already be gone.");
        }
    }
}

struct LeaderState<R> {
    peer_states: HashMap<NodeId, PeerState>,
    heartbeat_timer: Shared<Sleep>,
    proposals: BTreeMap<Index, Proposal<R>>,
}

impl<R> LeaderState<R> {
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

enum Role<R> {
    Follower(FollowerState),
    Candidate(CandidateState),
    Leader(LeaderState<R>),
}

#[allow(dead_code)]
impl<R> Role<R> {
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

pub struct RaftNode<R, S, I> {
    config: RaftConfig<I>,

    /// The log of entries that this node has received.
    storage: S,

    /// The index of the highest log entry known to be committed.
    /// Initialized to 0, increases monotonically.
    commit_index: Index,

    /// The index of the last log entry applied to the state machine.
    /// Initialized to 0, increases monotonically.
    last_applied: Index,

    /// State that only exists if this node is a leader.
    /// If this node is not a leader, this field is `None`.
    role: Role<R>,

    /// For receiving events from the core queue, which is pretty much everything but timeouts.
    core_rx: UnboundedReceiver<CoreQueueEntry<R>>,

    /// For sending messages to other nodes.
    outbound_network: OutboundNetworkHandle,

    /// For applying commands to the state machine.
    state_machine: StateMachineHandle<R, I>,

    rng: CharmRng,
}

impl<R: Serialize + DeserializeOwned + Send + 'static, S: CoreStorage, I: Clone + Serialize + DeserializeOwned> RaftNode<R, S, I> {
    #[must_use] pub fn new(
        config: RaftConfig<I>,
        core_rx: UnboundedReceiver<CoreQueueEntry<R>>,
        storage: S,
        outbound_network: OutboundNetworkHandle,
        state_machine: StateMachineHandle<R, I>,
        mut rng: CharmRng) -> RaftNode<R, S, I> {
        let follower_state = FollowerState {
            election_timer: sleep(config.get_election_timeout(&mut rng)).shared(),
            leader_id: None,
        };

        let mut node = RaftNode {
            config,
            storage,
            commit_index: Index(0),
            last_applied: Index(0),
            role: Follower(follower_state),
            core_rx,
            outbound_network,
            state_machine,
            rng,
        };

        node.reset_election_timer();
        node
    }

    async fn tick(&mut self) {
        trace!("Entering tick.");

        self.apply_committed().await;

        match self.role {
            Follower(ref follower_state) => {
                trace!("Running follower tick.");
                select! {
                     cqe = self.core_rx.recv() => {
                          if let Some(cqe) = cqe {
                            self.handle_event(cqe).await;
                          }
                     }

                     () = follower_state.election_timer.clone() => {
                          self.handle_election_timeout().await;
                     }
               }
            }

            Candidate(ref mut candidate_state) => {
                trace!("Running candidate tick.");
                select! {
                     cqe = self.core_rx.recv() => {
                          if let Some(cqe) = cqe {
                            self.handle_event(cqe).await;
                          }
                     }

                     () = candidate_state.election_timer.clone() => {
                          self.handle_election_timeout().await;
                     }
               }
            }

            Leader(ref mut leader_state) => {
                trace!("Running leader tick.");
                select! {
                     cqe = self.core_rx.recv() => {
                          if let Some(cqe) = cqe {
                            self.handle_event(cqe).await;
                          }
                     }

                     () = leader_state.heartbeat_timer.clone() => {
                          self.handle_heartbeat_timeout().await;
                     }
               }
            }
        }
    }

    async fn handle_event(&mut self, cqe: CoreQueueEntry<R>) {
        match cqe {
            CoreQueueEntry::AppendEntriesRequest { request, response_tx } => {
                let response = self.handle_append_entries_request(request).await;
                let r = response_tx.send(response);
                if r.is_err() {
                    warn!("Failed to send AppendEntriesResponse. Client request may have timed out.");
                }
            }

            CoreQueueEntry::RequestVoteRequest { request, response_tx } => {
                let response = self.handle_request_vote_request(request).await;
                let r = response_tx.send(response);
                if r.is_err() {
                    warn!("Failed to send RequestVoteResponse. Client request may have timed out.");
                }
            }

            CoreQueueEntry::AppendEntriesResponse(response) => {
                self.handle_append_entries_response(response).await;
            }

            CoreQueueEntry::RequestVoteResponse(response) => {
                self.handle_request_vote_response(response).await;
            }

            CoreQueueEntry::Propose { request, commit_tx, span } => {
                span.in_scope(|| {
                    self.handle_propose(Proposal { request, commit_tx })
                }).await;
            }
        }
    }

    async fn handle_election_timeout(&mut self) {
        // Enter the next term.
        let current_term = self.get_current_term().await;
        let next_term = current_term.next();
        self.set_current_term(next_term).await;
        info!("Hit election timeout! Starting election in term {:?}.", next_term);

        // Vote for ourselves.
        self.set_voted_for(Some(self.node_id().clone())).await;

        // Become a candidate, and reset the election timer.
        let election_timeout = self.get_election_timeout();
        let mut candidate_state = CandidateState {
            votes_received: BTreeSet::new(),
            election_timer: sleep(election_timeout).shared(),
        };
        candidate_state.votes_received.insert(self.node_id().clone());
        self.role = Candidate(candidate_state);

        // Send RequestVoteRequests to all nodes.
        let last_log_index = self.get_log_storage().last_index().await;
        let last_log_term = self.get_log_storage().last_log_term().await;
        let req = RequestVoteRequest {
            term: next_term,
            candidate_id: self.node_id().clone(),
            last_log_index,
            last_log_term,
        };

        for node_id in &self.config.other_nodes {
            self.outbound_network.request_vote(node_id.clone(), req.clone());
        }
    }

    async fn handle_heartbeat_timeout(&mut self) {
        // We only do anything if we are the leader.
        if let Leader(ref mut leader_state) = self.role {
            debug!("Sending heartbeat on timeout.");
            leader_state.heartbeat_timer = sleep(self.config.heartbeat_interval).shared();
            self.broadcast_heartbeat().await;
        }
    }

    async fn handle_append_entries_request(&mut self, req: AppendEntriesRequest) -> AppendEntriesResponse {
        debug!("Received AppendEntriesRequest: {:?}", req);
        self.check_incoming_term(req.term).await;

        let current_term = self.get_current_term().await;
        if req.term < current_term {
            // This request is out of date.
            warn!("Request from {:?} has term {:?}, which is older than our term {:?}. Failing request.",
                req.leader_id, req.term, current_term);
            return self.append_entries_response(false).await;
        }

        if let Leader(_) = self.role {
            error!("Another node {:?} claims to be leader with the same term! This should never happen! Converting to follower.", req.leader_id);
            self.convert_to_follower().await;
        }

        if let Candidate(_) = self.role {
            // Someone else in the same term became a leader before us.
            info!("Another node {:?} became the leader while we were a candidate. Converting to follower.", req.leader_id);
            self.convert_to_follower().await;
        }

        // Edge case: this is the first entry.
        if req.prev_log_index == Index(0) {
            // The log is empty. We can accept any entries.
            debug!("Append entries from start of the log. Accepting request.");

            // Truncate the log in case there are any entries.
            self.get_log_storage().truncate(Index(0)).await;

            let mut last_index = Index(0);
            for entry in req.entries {
                last_index = self.get_log_storage().append(entry).await;
            }
            
            self.commit_index = req.leader_commit.min(last_index);
            debug!("Commit index is now {:?}.", self.commit_index);
            
            self.update_leader(req.leader_id.clone());

            self.reset_election_timer();
            return self.append_entries_response(true).await;
        }

        let prev_log_entry = self.get_log_storage().get(req.prev_log_index).await;
        if prev_log_entry.is_none() {
            // We don't have the previous log entry.
            let last_index = self.get_log_storage().last_index().await;
            warn!("Append request claimed log index {:?}, but we only have up to index {:?}.", req.prev_log_index, last_index);
            return self.append_entries_response(false).await;
        }

        let prev_log_term = prev_log_entry.unwrap().term;
        if prev_log_term != req.prev_log_term {
            // The previous log term doesn't match.
            warn!("Request said previous log term at index {:?} was {:?}, but we have {:?}. Rejecting request.", req.prev_log_index, req.prev_log_term, prev_log_term);
            return self.append_entries_response(false).await;
        }

        // The previous log term matches. Append the new entries.
        debug!("Accepting request.");
        self.reset_election_timer();
        self.check_and_append(req.entries, req.prev_log_index.next()).await;

        if req.leader_commit > self.commit_index {
            let last_index = self.get_log_storage().last_index().await;
            self.commit_index = req.leader_commit.min(last_index);
        }
        debug!("Commit index is now {:?}.", self.commit_index);

        debug!("Sending successful response.");
        self.update_leader(req.leader_id.clone());
        self.append_entries_response(true).await
    }

    async fn check_and_append(&mut self, entries: Vec<LogEntry>, mut prev_log_index: Index) {
        for entry in entries {
            if let Some(prev_entry) = self.get_log_storage().get(prev_log_index).await {
                if prev_entry.term != entry.term {
                    // There is a conflict. Truncate the log and append the new entry.
                    warn!("Log conflict detected at index {:?}. Request has term {:?} but we have term {:?}. Truncating log.", prev_log_index, entry.term, prev_entry.term);
                    self.get_log_storage().truncate(prev_log_index).await;

                    // The log has cleared the bad entry and everything after it.
                    // Now we can append the new entry.
                    self.get_log_storage().append(entry).await;
                }
            // Nothing to do - the entry already matches!
            } else {
                // No conflict, append the entry.
                self.get_log_storage().append(entry).await;
            }

            prev_log_index = prev_log_index.next();
        }
    }

    async fn append_entries_response(&self, success: bool) -> AppendEntriesResponse {
        AppendEntriesResponse {
            node_id: self.node_id().clone(),
            term: self.get_current_term().await,
            success,
            last_log_index: self.get_log_storage().last_index().await
        }
    }

    async fn handle_append_entries_response(&mut self, res: AppendEntriesResponse) {
        debug!("Received AppendEntriesResponse: {:?}", res);

        self.check_incoming_term(res.term).await;

        if !self.role.is_leader() {
            // We are not the leader. Don't care.
            debug!("Not the leader. Ignoring response.");
        }

        // Run this here to appease the borrow checker.
        let last_index = self.get_log_storage().last_index().await;

        // If we are not the leader, we don't care about these responses.
        if let Leader(ref mut leader_state) = self.role {
            if res.success {
                let peer_state = leader_state.get_mut_peer_state(&res.node_id);
                peer_state.match_index = res.last_log_index;
                peer_state.next_index = res.last_log_index.next();
                debug!("Request was successful. Peer state for {:?} is now {:?}.", res.node_id, peer_state);

                // Try to update the commit index.
                let majority_match = leader_state.get_majority_match(last_index);
                if majority_match > self.commit_index {
                    debug!("Cluster majority advanced to index {:?}. Updating commit index.", majority_match);
                    self.commit_index = majority_match;
                }
            } else {
                // The AppendEntriesRequest failed. Decrement the next_index. We will retry later.
                warn!("Request failed. Node {:?} claims it has up to index {:?}.", res.node_id, res.last_log_index);
                let peer_state = leader_state.get_mut_peer_state(&res.node_id);
                peer_state.next_index = res.last_log_index;
            }
        }
    }


    async fn handle_request_vote_request(&mut self, req: RequestVoteRequest) -> RequestVoteResponse {
        debug!("Received RequestVoteRequest: {:?}", req);
        info!("Received vote request from {:?}.", req.candidate_id);

        self.check_incoming_term(req.term).await;

        let current_term = self.get_current_term().await;
        if req.term < current_term {
            // This candidate is out of date.
            warn!("Candidate {:?} has term {:?}, but we already have term {:?}. Failing vote request.", req.candidate_id, req.term, current_term);
            return self.request_vote_response(false).await;
        }

        let voted_for = self.get_voted_for().await;
        let can_vote = match voted_for {
            None => true,  // We haven't voted yet.
            Some(ref voted_for) => voted_for == &req.candidate_id,  // We already voted for this candidate.
        };
        if !can_vote {
            // We already voted for someone else.
            info!("Cannot vote for candidate {:?} because we already voted for {:?}. Failing vote request.", req.candidate_id, voted_for);
            return self.request_vote_response(false).await;
        }

        // Check last entry terms.
        let our_last_log_term = self.get_log_storage().last_log_term().await;
        let candidate_last_log_term = req.last_log_term;
        if candidate_last_log_term > our_last_log_term {
            // The candidate has a higher term, so we approve!
            info!("Voting for candidate {:?} because it has term {:?} and we only have term {:?}.", req.candidate_id, candidate_last_log_term, our_last_log_term);
            self.vote_for(&req.candidate_id).await;
            return self.request_vote_response(true).await;
        }

        // If the terms are the same, we need to check the log lengths.
        let our_last_log_index = self.get_log_storage().last_index().await;
        let candidate_last_log_index = req.last_log_index;
        if candidate_last_log_index >= our_last_log_index {
            // The candidate has at least as much log as we do, so we approve!
            info!("Voting for candidate {:?} because it has last index {:?} and we have index {:?}.", req.candidate_id, candidate_last_log_index, our_last_log_index);
            self.vote_for(&req.candidate_id).await;
            return self.request_vote_response(true).await;
        }

        // If we get here, the candidate's log is shorter than ours.
        info!("Not voting for candidate {:?} because it has last index {:?} and we have index {:?}.", req.candidate_id, candidate_last_log_index, our_last_log_index);
        self.request_vote_response(false).await
    }

    async fn request_vote_response(&self, vote_granted: bool) -> RequestVoteResponse {
        let current_term = self.get_current_term().await;
        RequestVoteResponse {
            node_id: self.node_id().clone(),
            term: current_term,
            vote_granted,
        }
    }


    async fn vote_for(&mut self, candidate_id: &NodeId) {
        self.set_voted_for(Some(candidate_id.clone())).await;

        let election_timeout = self.get_election_timeout();
        let role = Follower(FollowerState { election_timer: sleep(election_timeout).shared(), leader_id: None });
        self.role = role;
    }

    async fn handle_request_vote_response(&mut self, res: RequestVoteResponse) {
        debug!("Received RequestVoteResponse: {:?}", res);

        self.check_incoming_term(res.term).await;

        match self.role {
            Candidate(ref mut state) => {
                if res.vote_granted {
                    if state.votes_received.contains(&res.node_id) {
                        // We already got a vote from this node. Ignore it.
                        debug!("Already got a vote from {:?}. Ignoring response.", res.node_id);
                        return;
                    }
                    
                    state.votes_received.insert(res.node_id.clone());
                    let total_nodes = self.config.other_nodes.len() + 1;
                    let majority = total_nodes / 2 + 1;
                    let num_votes = state.votes_received.len();
                    info!("Vote granted by {:?}. We now have {:?} out of {:?} needed votes", res.node_id, num_votes, majority);
                    
                    if num_votes >= majority && !self.role.is_leader() {
                        // Great success!
                        self.become_leader().await;
                    }
                } else {
                    // The vote was not granted, just log it.
                    info!("Vote not granted by {:?}.", res.node_id);
                }
            }

            _ => {
                // We are not currently a candidate. Don't care.
                debug!("Not a candidate. Ignoring response.");
            },
        }
    }

    async fn handle_propose(&mut self, proposal: Proposal<R>) {
        if let Candidate(_) = self.role {
                debug!("Not the leader, but a candidate. Failing proposal.");
            proposal.commit_failure(RaftCoreError::NotLeader { leader_id: None });
            return;
            }

        if let Follower(ref follower_state) = self.role {
                debug!("Not the leader, but a follower. Failing proposal.");
            proposal.commit_failure(RaftCoreError::NotLeader { leader_id: follower_state.leader_id.clone() });
            return;
            }

        // We do this stuff here instead of a match statement to make the borrow-checker happy.
        let serialized_data = serde_json::to_vec(&proposal.request).unwrap();
        let serialized_leader_info = serde_json::to_vec(&self.config.node_info).unwrap();
        let current_term = self.get_current_term().await;
        let log_entry = LogEntry {
            leader_info: serialized_leader_info,
            term: current_term,
            data: Data(serialized_data),
        };
        let index = self.get_log_storage().append(log_entry).await;
        if let Leader(ref mut leader_state) = self.role {
                debug!("Proposal appended to log at index {:?}.", index);
            leader_state.proposals.insert(index, proposal);
        }
    }

    async fn become_leader(&mut self) {
        let current_term = self.get_current_term().await;
        info!("Becoming the leader in term {:?}!", current_term);
        let mut peer_states = HashMap::new();

        for node_id in &self.config.other_nodes {
            let next_index = self.get_log_storage().last_index().await.next();
            let match_index = Index(0);
            peer_states.insert(node_id.clone(), PeerState { next_index, match_index });
        }

        let leader_state = LeaderState {
            peer_states,
            heartbeat_timer: sleep(self.config.heartbeat_interval).shared(),
            proposals: BTreeMap::new(),
        };
        let role = Leader(leader_state);
        self.role = role;

        // Send initial empty AppendEntriesRequests to all nodes.
        self.broadcast_heartbeat().await
    }

    async fn broadcast_heartbeat(&mut self) {
        for node_id in &self.config.other_nodes {
            self.send_append_entries(node_id).await
        }
    }

    async fn send_append_entries(&self, node_id: &NodeId) {
        if let Leader(ref leader_state) = self.role {
            let peer_state = leader_state.get_peer_state(node_id);
            let next_index = peer_state.next_index;
            let prev_log_index = next_index.prev();
            let prev_log_term = self
                .get_log_storage()
                .get(prev_log_index)
                .await
                .map_or(Term(0), |entry| entry.term);

            let entries = self.get_log_storage().entries_from(next_index).await;

            let req = AppendEntriesRequest {
                term: self.get_current_term().await,
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
    async fn check_incoming_term(&mut self, term: Term) {
        let current_term = self.get_current_term().await;
        if current_term < term {
            info!("Received message with term {:?} higher than our term {:?}. Converting to follower.", term, current_term);
            self.set_current_term(term).await;
            self.convert_to_follower().await;
        }
    }

    async fn convert_to_follower(&mut self) {
        self.set_voted_for(None).await;
        let follower_state = FollowerState { election_timer: sleep(self.get_election_timeout()).shared(), leader_id: None };
        let mut role = Follower(follower_state);
        swap(&mut self.role, &mut role);
        let prev_role = role;

        if let Leader(leader_state) = prev_role {
            // Drop all proposals.
            for (_, proposal) in leader_state.proposals {
                proposal.commit_failure(RaftCoreError::NotLeader { leader_id: None });
            }
        }
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

    async fn apply_committed(&mut self) {
        let current_term = self.get_current_term().await;
        while self.last_applied < self.commit_index {
            self.last_applied = self.last_applied.next();

            if let Leader(ref mut leader_state) = self.role {
                if let Some(proposal) = leader_state.proposals.remove(&self.last_applied) {
                    // Inform the client that the proposal was committed.
                    let request = proposal.commit_success();

                    let raft_info = RaftInfo {
                        leader_info: self.config.node_info.clone(),
                        term: current_term,
                        index: self.last_applied,
                    };
                    debug!("Applying cached proposal");
                    self.state_machine.apply(request, raft_info);
                    continue;
                }
            }

            let entry = self.get_log_storage().get(self.last_applied).await.unwrap();
            let req: R = serde_json::from_slice(&entry.data.0).unwrap();
            let leader_info: I = serde_json::from_slice(&entry.leader_info).unwrap();
            let raft_info = RaftInfo {
                leader_info,
                term: entry.term,
                index: self.last_applied,
            };
            debug!("Applying proposal from the log");
                self.state_machine.apply(req, raft_info);
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

    async fn get_current_term(&self) -> Term {
        self.storage.current_term().await
    }

    async fn set_current_term(&self, term: Term) {
        self.storage.set_current_term(term).await
    }

    async fn get_voted_for(&self) -> Option<NodeId> {
        self.storage.voted_for().await
    }

    async fn set_voted_for(&self, candidate_id: Option<NodeId>) {
        self.storage.set_voted_for(candidate_id).await
    }

    fn get_log_storage(&self) -> S::LogStorage {
        self.storage.log_storage()
    }
}

#[tracing::instrument(skip_all)]
async fn run<R, S, I>(config: RaftConfig<I>, core_rx: UnboundedReceiver<CoreQueueEntry<R>>, storage: S, outbound_network: OutboundNetworkHandle, state_machine: StateMachineHandle<R, I>, rng: CharmRng, span: Span)
where
    R: Serialize + DeserializeOwned + Send + Sync + 'static,
    S: CoreStorage + Send + Sync + 'static,
    S::LogStorage: Send + 'static,
    I: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    async move {
        let mut node = RaftNode::new(config, core_rx, storage, outbound_network, state_machine, rng);
        loop {
            node.tick().await;
        }
    }.instrument(span).await;
}

pub fn run_core<R, S, I>(config: RaftConfig<I>, core_rx: UnboundedReceiver<CoreQueueEntry<R>>, storage: S, outbound_network: OutboundNetworkHandle, state_machine: StateMachineHandle<R, I>, rng: CharmRng)
where
    R: Serialize + DeserializeOwned + Send + Sync + 'static,
    S: CoreStorage + Send + Sync + 'static,
    S::LogStorage: Send + 'static,
    I: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    let span = Span::current();
    spawn(run(config, core_rx, storage, outbound_network, state_machine, rng, span));
}