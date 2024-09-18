use crate::raft::core::storage::LogStorage;
use crate::raft::types::{Data, Index, LogEntry, NodeId, Term};
use anyhow::Context;
use tonic::async_trait;

struct SqliteLogStorage {
    pool: sqlx::SqlitePool,
}

impl SqliteLogStorage {
    pub async fn new(filename: &str) -> anyhow::Result<Self> {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect(filename)
            .await
            .context("Failed to create SQLite pool")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS log (
                id INTEGER PRIMARY KEY,
                leader_host TEXT NOT NULL,
                leader_port INTEGER NOT NULL,
                term INTEGER NOT NULL,
                data BLOB NOT NULL
            )
            "#,
        )
            .execute(&pool)
            .await
            .context("Failed to create table")?;

        Ok(Self { pool })
    }
}

#[async_trait]
impl LogStorage for SqliteLogStorage {
    type ErrorType = anyhow::Error;

    async fn append(&mut self, entry: LogEntry) -> Result<Index, Self::ErrorType> {
        let mut tx = self.pool.begin().await.context("Failed to start transaction")?;
        sqlx::query("INSERT INTO log (leader_host, leader_port, term, data) VALUES (?, ?, ?, ?)")
            .bind(entry.leader_id.host)
            .bind(entry.leader_id.port as i64)
            .bind(entry.term.0 as i64)
            .bind(entry.data.0)
            .execute(&mut *tx)
            .await
            .context("Failed to append entry")?;

        let id: i64 = sqlx::query_scalar("SELECT last_insert_rowid()")
            .fetch_one(&mut *tx)
            .await
            .context("Failed to get last insert ID")?;

        tx.commit().await.context("Failed to commit transaction")?;

        Ok(Index(id as u64))
    }

    async fn get(&self, index: Index) -> Result<Option<LogEntry>, Self::ErrorType> {
        let entry = sqlx::query_as::<_, (String, i64, i64, Vec<u8>)>("SELECT leader_host, leader_port, term, data FROM log WHERE id = ?")
            .bind(index.0 as i64)
            .fetch_optional(&self.pool)
            .await
            .context("Failed to fetch entry")?;

        Ok(entry.map(|(leader_host, leader_port, term, data)| LogEntry {
            leader_id: NodeId {
                host: leader_host,
                port: leader_port as u16,
            },
            term: Term(term as u64),
            data: Data(data),
        }))
    }

    async fn last_index(&self) -> Result<Index, Self::ErrorType> {
        sqlx::query_scalar("SELECT MAX(id) FROM log")
            .fetch_one(&self.pool)
            .await
            .map(|id: Option<i64>| Index(id.unwrap_or(0) as u64))
            .context("Failed to get last index")
    }

    async fn last_log_term(&self) -> Result<Term, Self::ErrorType> {
        sqlx::query_scalar("SELECT term FROM log ORDER BY id DESC LIMIT 1")
            .fetch_one(&self.pool)
            .await
            .map(|term: i64| Term(term as u64))
            .context("Failed to get last log term")
    }

    async fn entries_from(&self, index: Index) -> Result<Vec<LogEntry>, Self::ErrorType> {
        sqlx::query_as::<_, (String, i64, i64, Vec<u8>)>("SELECT leader_host, leader_port, term, data FROM log WHERE id >= ?")
            .bind(index.0 as i64)
            .fetch_all(&self.pool)
            .await
            .map(|entries| {
                entries.into_iter().map(|(leader_host, leader_port, term, data)| LogEntry {
                    leader_id: NodeId {
                        host: leader_host,
                        port: leader_port as u16,
                    },
                    term: Term(term as u64),
                    data: Data(data),
                }).collect()
            })
            .context("Failed to get entries from")
    }

    async fn truncate(&mut self, index: Index) -> Result<(), Self::ErrorType> {
        sqlx::query("DELETE FROM log WHERE id >= ?")
            .bind(index.0 as i64)
            .execute(&self.pool)
            .await
            .context("Failed to truncate log")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::raft::core::storage::sqlite_storage::SqliteLogStorage;
    use crate::raft::core::storage::LogStorage;
    use crate::raft::types::{Data, Index, LogEntry, NodeId, Term};
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_sqlite_log_storage() {
        let temp_file = NamedTempFile::new().unwrap();
        let filename = temp_file.path().to_str().unwrap();

        let mut storage = SqliteLogStorage::new(filename).await.unwrap();

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