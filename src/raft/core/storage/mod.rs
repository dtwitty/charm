pub mod sqlite;
pub mod in_mem;

use crate::raft::types::{Index, LogEntry, NodeId, Term};
use std::fmt::Debug;
use tonic::async_trait;

#[async_trait]
pub trait LogStorage {
    /// Append a new entry to the log, returning the index at which it was appended.
    async fn append(&self, entry: LogEntry) -> Index;

    /// Get the entry at the given index, if it exists.
    async fn get(&self, index: Index) -> Option<LogEntry>;

    /// Get the index of the last entry in the log.
    async fn last_index(&self) -> Index;

    /// Get the term of the last entry in the log.
    async fn last_log_term(&self) -> Term;

    /// Get all entries starting from the given index.
    async fn entries_from(&self, index: Index) -> Vec<LogEntry>;

    /// Truncate the log so that it no longer includes any entries with an index greater than or equal to `index`.
    async fn truncate(&self, index: Index) -> ();
}

#[async_trait]
pub trait CoreStorage {
    /// The log storage engine that this storage engine uses.
    type LogStorage: LogStorage;

    /// The error type that this storage engine may return.
    type ErrorType: Debug;

    /// Get the current term.
    async fn current_term(&self) -> Term;

    /// Set the current term.
    async fn set_current_term(&self, term: Term) -> ();

    /// Get the ID of the candidate that this node has voted for in the current term.
    async fn voted_for(&self) -> Option<NodeId>;

    /// Set the ID of the candidate that this node has voted for in the current term.
    async fn set_voted_for(&self, candidate_id: Option<NodeId>) -> ();

    /// Get the log storage engine.
    fn log_storage(&self) -> Self::LogStorage;
}

#[cfg(test)]
pub mod test {
    use crate::raft::core::storage::LogStorage;
    use crate::raft::types::{Data, Index, LogEntry, Term};

    pub async fn test_log_storage<L: LogStorage>(storage: L) {
        let entry1 = LogEntry {
            leader_info: "node1".into(),
            term: Term(1),
            data: Data(b"hello".to_vec()),
        };
        let entry2 = LogEntry {
            leader_info: "node2".into(),
            term: Term(1),
            data: Data(b"world".to_vec()),
        };

        let index1 = storage.append(entry1.clone()).await;
        let index2 = storage.append(entry2.clone()).await;

        assert_eq!(storage.get(index1).await, Some(entry1.clone()));
        assert_eq!(storage.get(index2).await, Some(entry2.clone()));

        assert_eq!(storage.last_index().await, index2);
        assert_eq!(storage.last_log_term().await, entry2.term);

        let entries = storage.entries_from(Index(1)).await;
        assert_eq!(entries, vec![entry1.clone(), entry2.clone()]);

        storage.truncate(Index(1)).await;
        assert_eq!(storage.get(index1).await, None);
        assert_eq!(storage.get(index2).await, None);
        assert_eq!(storage.last_index().await, Index(0));
    }
}

