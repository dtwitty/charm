use crate::raft::pb::{AppendEntriesRequestPb, AppendEntriesResponsePb, LogEntryPb, NodeIdPb, RequestVoteRequestPb, RequestVoteResponsePb};
use crate::raft::types::{Data, Index, LogEntry, NodeId, Term};

impl From<NodeIdPb> for NodeId {
    fn from(pb: NodeIdPb) -> Self {
        NodeId {
            host: pb.host,
            port: pb.port as u16,
        }
    }
}

impl From<NodeId> for NodeIdPb {
    fn from(node_id: NodeId) -> Self {
        NodeIdPb {
            host: node_id.host,
            port: node_id.port.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesRequest {
    /// The leader's term.
    pub term: Term,

    /// The leader's ID.
    pub leader_id: NodeId,

    /// The index of the log entry immediately preceding the new ones.
    pub prev_log_index: Index,

    /// The term of the entry at `prev_log_index`.
    pub prev_log_term: Term,

    /// The new log entries to append.
    pub entries: Vec<LogEntry>,

    /// The leader's commit index.
    pub leader_commit: Index,
}

impl AppendEntriesRequest {
    #[must_use] pub fn from_pb(pb: &AppendEntriesRequestPb) -> Self {
        let term = Term(pb.term);
        let leader_id = pb.leader_id.clone().unwrap().into();
        let prev_log_index = Index(pb.prev_log_index);
        let prev_log_term = Term(pb.prev_log_term);
        let entries = pb.entries.iter().map(|e|
            LogEntry {
                leader_info: e.leader_info.clone(),
                term: Term(e.term),
                data: Data(e.data.clone()),
            }
        ).collect();
        let leader_commit = Index(pb.leader_commit);

        Self {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        }
    }

    #[must_use] pub fn to_pb(&self) -> AppendEntriesRequestPb {
        let term = self.term.0;
        let leader_id = Some(self.leader_id.clone().into());
        let prev_log_index = self.prev_log_index.0;
        let prev_log_term = self.prev_log_term.0;
        let entries = self.entries.iter().map(|e|
            LogEntryPb {
                leader_info: e.leader_info.clone(),
                term: e.term.0,
                data: e.data.0.clone(),
            }
        ).collect();
        let leader_commit = self.leader_commit.0;

        AppendEntriesRequestPb {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesResponse {
    /// The id of the responding node.
    pub node_id: NodeId,

    /// The current term of the responding server, for the leader to update itself.
    pub term: Term,

    /// True if the follower contained an entry matching `prev_log_index` and `prev_log_term`.
    pub success: bool,

    /// The index of the last log entry.
    pub last_log_index: Index,
}

impl AppendEntriesResponse {
    #[must_use] pub fn from_pb(pb: &AppendEntriesResponsePb) -> Self {
        let node_id = pb.node_id.clone().unwrap().into();
        let term = Term(pb.term);
        let success = pb.success;
        let last_log_index = Index(pb.last_log_index);

        Self {
            node_id,
            term,
            success,
            last_log_index,
        }
    }

    #[must_use] pub fn to_pb(&self) -> AppendEntriesResponsePb {
        let node_id = Some(self.node_id.clone().into());
        let term = self.term.0;
        let success = self.success;
        let last_log_index = self.last_log_index.0;

        AppendEntriesResponsePb {
            node_id,
            term,
            success,
            last_log_index,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVoteRequest {
    /// The candidate's term.
    pub term: Term,

    /// The candidate's ID.
    pub candidate_id: NodeId,

    /// The index of the candidate's last log entry.
    pub last_log_index: Index,

    /// The term of the candidate's last log entry.
    pub last_log_term: Term,
}

impl RequestVoteRequest {
    #[must_use] pub fn from_pb(pb: &RequestVoteRequestPb) -> Self {
        let term = Term(pb.term);
        let candidate_id = pb.candidate_id.clone().unwrap().into();
        let last_log_index = Index(pb.last_log_index);
        let last_log_term = Term(pb.last_log_term);

        Self {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        }
    }

    #[must_use] pub fn to_pb(&self) -> RequestVoteRequestPb {
        let term = self.term.0;
        let candidate_id = Some(self.candidate_id.clone().into());
        let last_log_index = self.last_log_index.0;
        let last_log_term = self.last_log_term.0;

        RequestVoteRequestPb {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVoteResponse {
    /// The id of the responding node.
    pub node_id: NodeId,

    /// The current term of the responding server, for candidate to update itself.
    pub term: Term,

    /// True if the vote was granted.
    pub vote_granted: bool,
}

impl RequestVoteResponse {
    #[must_use] pub fn from_pb(pb: &RequestVoteResponsePb) -> Self {
        let node_id = pb.node_id.clone().unwrap().into();
        let term = Term(pb.term);
        let vote_granted = pb.vote_granted;

        Self {
            node_id,
            term,
            vote_granted,
        }
    }

    #[must_use] pub fn to_pb(&self) -> RequestVoteResponsePb {
        let node_id = Some(self.node_id.clone().into());
        let term = self.term.0;
        let vote_granted = self.vote_granted;

        RequestVoteResponsePb {
            node_id,
            term,
            vote_granted,
        }
    }
}