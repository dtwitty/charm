use crate::raft::types::*;

pub enum Message {
    AppendEntriesRequest(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
    RequestVoteRequest(RequestVoteRequest),
    RequestVoteResponse(RequestVoteResponse),
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesResponse {
    /// The current term of the responding server, for the leader to update itself.
    pub term: Term,

    /// True if the follower contained an entry matching `prev_log_index` and `prev_log_term`.
    pub success: bool,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVoteResponse {
    /// The current term of the responding server, for candidate to update itself.
    pub term: Term,

    /// True if the vote was granted.
    pub vote_granted: bool,
}