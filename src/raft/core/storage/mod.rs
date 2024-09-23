pub mod sqlite;
pub mod in_mem;

use crate::raft::types::{Index, LogEntry, NodeId, Term};
use std::fmt::Debug;
use tonic::async_trait;

#[async_trait]
pub trait LogStorage {
    /// The error type that this storage engine may return.
    type ErrorType: Debug;

    /// Append a new entry to the log, returning the index at which it was appended.
    async fn append(&self, entry: LogEntry) -> Result<Index, Self::ErrorType>;

    /// Get the entry at the given index, if it exists.
    async fn get(&self, index: Index) -> Result<Option<LogEntry>, Self::ErrorType>;

    /// Get the index of the last entry in the log.
    async fn last_index(&self) -> Result<Index, Self::ErrorType>;

    /// Get the term of the last entry in the log.
    async fn last_log_term(&self) -> Result<Term, Self::ErrorType>;

    /// Get all entries starting from the given index.
    async fn entries_from(&self, index: Index) -> Result<Vec<LogEntry>, Self::ErrorType>;

    /// Truncate the log so that it no longer includes any entries with an index greater than or equal to `index`.
    async fn truncate(&self, index: Index) -> Result<(), Self::ErrorType>;
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

#[cfg(test)]
pub mod test {
    use crate::raft::core::storage::LogStorage;
    use crate::raft::types::{Data, Index, LogEntry, NodeId, Term};

    pub async fn test_log_storage<L: LogStorage>(storage: L) {
        let entry1 = LogEntry {
            leader_id: NodeId {
                host: "node1".to_string(),
                port: 1234,
            },
            term: Term(1),
            data: Data(b"hello".to_vec()),
        };
        let entry2 = LogEntry {
            leader_id: NodeId {
                host: "node2".to_string(),
                port: 5678,
            },
            term: Term(1),
            data: Data(b"world".to_vec()),
        };

        let index1 = storage.append(entry1.clone()).await.unwrap();
        let index2 = storage.append(entry2.clone()).await.unwrap();

        assert_eq!(storage.get(index1).await.unwrap(), Some(entry1.clone()));
        assert_eq!(storage.get(index2).await.unwrap(), Some(entry2.clone()));

        assert_eq!(storage.last_index().await.unwrap(), index2);
        assert_eq!(storage.last_log_term().await.unwrap(), entry2.term);

        let entries = storage.entries_from(Index(1)).await.unwrap();
        assert_eq!(entries, vec![entry1.clone(), entry2.clone()]);

        storage.truncate(Index(1)).await.unwrap();
        assert_eq!(storage.get(index1).await.unwrap(), None);
        assert_eq!(storage.get(index2).await.unwrap(), None);
        assert_eq!(storage.last_index().await.unwrap(), Index(0));
    }
}

