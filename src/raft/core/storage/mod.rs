pub mod sqlite_storage;

use std::fmt::Debug;
use crate::raft::types::{Index, LogEntry, NodeId, Term};
use tonic::async_trait;

#[async_trait]
pub trait LogStorage {
    /// The error type that this storage engine may return.
    type ErrorType: Debug;

    /// Append a new entry to the log, returning the index at which it was appended.
    async fn append(&mut self, entry: LogEntry) -> Result<Index, Self::ErrorType>;

    /// Get the entry at the given index, if it exists.
    async fn get(&self, index: Index) -> Result<Option<LogEntry>, Self::ErrorType>;

    /// Get the index of the last entry in the log.
    async fn last_index(&self) -> Result<Index, Self::ErrorType>;

    /// Get the term of the last entry in the log.
    async fn last_log_term(&self) -> Result<Term, Self::ErrorType>;

    /// Get all entries starting from the given index.
    async fn entries_from(&self, index: Index) -> Result<Vec<LogEntry>, Self::ErrorType>;

    /// Truncate the log so that it no longer includes any entries with an index greater than or equal to `index`.
    async fn truncate(&mut self, index: Index) -> Result<(), Self::ErrorType>;
}

#[async_trait]
pub trait CoreStorage {
    /// The log storage engine that this storage engine uses.
    type LogStorage: LogStorage;

    /// The error type that this storage engine may return.
    type ErrorType: Debug;

    /// Get the current term.
    async fn current_term(&self) -> Result<Term, Self::ErrorType>;

    /// Set the current term.
    async fn set_current_term(&self, term: Term) -> Result<(), Self::ErrorType>;

    /// Get the ID of the candidate that this node has voted for in the current term.
    async fn voted_for(&self) -> Result<Option<NodeId>, Self::ErrorType>;

    /// Set the ID of the candidate that this node has voted for in the current term.
    async fn set_voted_for(&self, candidate_id: Option<NodeId>) -> Result<(), Self::ErrorType>;

    /// Get the log storage engine.
    fn log_storage(&self) -> Self::LogStorage;
}

