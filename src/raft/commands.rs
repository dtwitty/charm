use tokio::sync::oneshot::Sender;

use crate::raft::types::{Data, Index, NodeId};

pub type RaftResult<T> = Result<T, RaftError>;

#[derive(Debug)]
pub enum RaftRequest {
    /// Propose to append a new entry to the log.
    Propose(ProposeRequest, Sender<RaftResult<ProposeResponse>>),

    /// Try to become the new leader.
    Campaign(CampaignRequest, Sender<RaftResult<CampaignResponse>>),

    /// Get entries from the log.
    GetEntries(GetEntriesRequest, Sender<RaftResult<GetEntriesResponse>>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProposeRequest {
    /// The data to append to the log.
    pub data: Data,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProposeResponse {
    /// The index in the log where the entry was appended.
    pub index: Index,
}

#[derive(Debug)]
pub struct CampaignRequest {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CampaignResponse;

#[derive(Debug)]
pub struct GetEntriesRequest {
    /// The index of the first entry to get.
    pub from_inclusive: Index,

    /// The index just passed last entry to get.
    /// If `None`, get all entries from `from_inclusive` to the end of the log.
    pub to_exclusive: Option<Index>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetEntriesResponse {
    /// The (possibly empty or truncated) entries in the requested range.
    pub entries: Vec<Data>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftError {
    /// The node is not the leader.
    NotLeader(NotLeaderError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotLeaderError {
    /// Who we think the leader is.
    /// If `None`, we don't know who the leader is, but it's not us.
    pub leader_id: Option<NodeId>,
}
