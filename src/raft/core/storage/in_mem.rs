use crate::raft::core::storage::{CoreStorage, LogStorage};
use crate::raft::types::{Index, LogEntry, NodeId, Term};
use std::sync::{Arc, RwLock};
use tonic::async_trait;

#[derive(Clone)]
pub struct InMemLogStorage {
    entries: Arc<RwLock<Vec<LogEntry>>>,
}

impl InMemLogStorage {
    pub fn new() -> Self {
        InMemLogStorage {
            entries: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

#[async_trait]
impl LogStorage for InMemLogStorage {
    type ErrorType = ();

    async fn append(&self, entry: LogEntry) -> Result<Index, Self::ErrorType> {
        let lock = self.entries.write();
        let mut entries = lock.unwrap();
        entries.push(entry);
        let index = entries.len() as u64;
        Ok(Index(index))
    }

    async fn get(&self, index: Index) -> Result<Option<LogEntry>, Self::ErrorType> {
        if index.0 == 0 {
            return Ok(None);
        }
        let index = index.0 as usize - 1;

        let lock = self.entries.read();
        let entries = lock.unwrap();
        if index < entries.len() {
            Ok(Some(entries[index].clone()))
        } else {
            Ok(None)
        }
    }

    async fn last_index(&self) -> Result<Index, Self::ErrorType> {
        let lock = self.entries.read();
        let entries = lock.unwrap();
        Ok(Index(entries.len() as u64))
    }

    async fn last_log_term(&self) -> Result<Term, Self::ErrorType> {
        let lock = self.entries.read();
        let entries = lock.unwrap();
        if let Some(entry) = entries.last() {
            Ok(entry.term)
        } else {
            Ok(Term(0))
        }
    }

    async fn entries_from(&self, index: Index) -> Result<Vec<LogEntry>, Self::ErrorType> {
        let lock = self.entries.read();
        let entries = lock.unwrap();

        if index.0 == 0 {
            return Ok(entries.clone());
        }

        let index = index.0 as usize - 1;
        if index < entries.len() {
            Ok(entries[index..].to_vec())
        } else {
            Ok(Vec::new())
        }
    }

    async fn truncate(&self, index: Index) -> Result<(), Self::ErrorType> {
        let lock = self.entries.write();
        let mut entries = lock.unwrap();

        if index.0 == 0 {
            entries.clear();
            return Ok(());
        }

        let index = index.0 as usize - 1;
        entries.truncate(index);
        Ok(())
    }
}

#[cfg(test)]
mod log_storage_tests {
    use super::*;
    use crate::raft::core::storage::test::test_log_storage;

    #[tokio::test]
    async fn test_in_mem_log_storage() {
        let storage = InMemLogStorage::new();
        test_log_storage(storage).await;
    }
}

struct InMemStorageInner {
    current_term: Term,
    voted_for: Option<NodeId>,
}

pub struct InMemStorage {
    log_storage: InMemLogStorage,
    inner: Arc<RwLock<InMemStorageInner>>,
}

impl InMemStorage {
    pub fn new() -> Self {
        InMemStorage {
            log_storage: InMemLogStorage::new(),
            inner: Arc::new(RwLock::new(InMemStorageInner {
                current_term: Term(0),
                voted_for: None,
            })),
        }
    }
}

#[async_trait]
impl CoreStorage for InMemStorage {
    type LogStorage = InMemLogStorage;
    type ErrorType = ();

    async fn current_term(&self) -> Result<Term, Self::ErrorType> {
        let inner = self.inner.read().unwrap();
        Ok(inner.current_term)
    }

    async fn set_current_term(&self, term: Term) -> Result<(), Self::ErrorType> {
        let mut inner = self.inner.write().unwrap();
        inner.current_term = term;
        Ok(())
    }

    async fn voted_for(&self) -> Result<Option<NodeId>, Self::ErrorType> {
        let inner = self.inner.read().unwrap();
        Ok(inner.voted_for.clone())
    }

    async fn set_voted_for(&self, candidate_id: Option<NodeId>) -> Result<(), Self::ErrorType> {
        let mut inner = self.inner.write().unwrap();
        inner.voted_for = candidate_id;
        Ok(())
    }

    fn log_storage(&self) -> Self::LogStorage {
        self.log_storage.clone()
    }
}

#[cfg(test)]
mod core_storage_tests {
    use super::*;

    #[tokio::test]
    async fn test_in_mem_storage() {
        let storage = InMemStorage::new();
        assert_eq!(storage.current_term().await.unwrap(), Term(0));
        assert_eq!(storage.voted_for().await.unwrap(), None);

        storage.set_current_term(Term(1)).await.unwrap();
        assert_eq!(storage.current_term().await.unwrap(), Term(1));

        storage.set_voted_for(Some(NodeId {
            host: "node1".to_string(),
            port: 1234,
        })).await.unwrap();
        assert_eq!(storage.voted_for().await.unwrap(), Some(NodeId {
            host: "node1".to_string(),
            port: 1234,
        }));
    }
}


