use crate::raft::core::storage::{CoreStorage, LogStorage};
use crate::raft::types::{Index, LogEntry, NodeId, Term};
use std::sync::{Arc, RwLock};
use tonic::async_trait;

#[derive(Clone)]
pub struct InMemLogStorage {
    entries: Arc<RwLock<Vec<LogEntry>>>,
}

impl Default for InMemLogStorage {
    fn default() -> Self {
        Self::new()
    }
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
    async fn append(&self, entry: LogEntry) -> Index {
        let lock = self.entries.write();
        let mut entries = lock.unwrap();
        entries.push(entry);
        let index = entries.len() as u64;
        Index(index)
    }

    async fn get(&self, index: Index) -> Option<LogEntry> {
        if index.0 == 0 {
            return None;
        }
        let index = index.0 as usize - 1;

        let lock = self.entries.read();
        let entries = lock.unwrap();
        if index < entries.len() {
            Some(entries[index].clone())
        } else {
            None
        }
    }

    async fn last_index(&self) -> Index {
        let lock = self.entries.read();
        let entries = lock.unwrap();
        Index(entries.len() as u64)
    }

    async fn last_log_term(&self) -> Term {
        let lock = self.entries.read();
        let entries = lock.unwrap();
        if let Some(entry) = entries.last() {
            entry.term
        } else {
            Term(0)
        }
    }

    async fn entries_from(&self, index: Index) -> Vec<LogEntry> {
        let lock = self.entries.read();
        let entries = lock.unwrap();

        if index.0 == 0 {
            return entries.clone();
        }

        let index = index.0 as usize - 1;
        if index < entries.len() {
            entries[index..].to_vec()
        } else {
            Vec::new()
        }
    }

    async fn truncate(&self, index: Index) -> () {
        let lock = self.entries.write();
        let mut entries = lock.unwrap();

        if index.0 == 0 {
            entries.clear();
            return ();
        }

        let index = index.0 as usize - 1;
        entries.truncate(index);
        ()
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

#[derive(Clone)]
pub struct InMemStorage {
    log_storage: InMemLogStorage,
    inner: Arc<RwLock<InMemStorageInner>>,
}

impl Default for InMemStorage {
    fn default() -> Self {
        Self::new()
    }
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

    async fn current_term(&self) -> Term {
        let inner = self.inner.read().unwrap();
        inner.current_term
    }

    async fn set_current_term(&self, term: Term) -> () {
        let mut inner = self.inner.write().unwrap();
        inner.current_term = term;
        ()
    }

    async fn voted_for(&self) -> Option<NodeId> {
        let inner = self.inner.read().unwrap();
        inner.voted_for.clone()
    }

    async fn set_voted_for(&self, candidate_id: Option<NodeId>) -> () {
        let mut inner = self.inner.write().unwrap();
        inner.voted_for = candidate_id;
        ()
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
        assert_eq!(storage.current_term().await, Term(0));
        assert_eq!(storage.voted_for().await, None);

        storage.set_current_term(Term(1)).await;
        assert_eq!(storage.current_term().await, Term(1));

        storage.set_voted_for(Some(NodeId {
            host: "node1".to_string(),
            port: 1234,
        })).await;
        assert_eq!(storage.voted_for().await, Some(NodeId {
            host: "node1".to_string(),
            port: 1234,
        }));
    }
}


