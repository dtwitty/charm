use crate::raft::core::storage::{CoreStorage, LogStorage};
use crate::raft::types::{Data, Index, LogEntry, NodeId, Term};
use std::fs;
use std::fs::OpenOptions;
use std::path::Path;
use tonic::async_trait;

#[derive(Clone)]
pub struct SqliteLogStorage {
    pool: sqlx::SqlitePool,
}

impl SqliteLogStorage {
    pub async fn new(filename: &str) -> anyhow::Result<Self> {
        create_file_with_dirs(filename);
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect(filename)
            .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS log (
                id INTEGER PRIMARY KEY,
                leader_info BLOB NOT NULL,
                term INTEGER NOT NULL,
                data BLOB NOT NULL
            )
            "#,
        )
            .execute(&pool)
            .await?;

        Ok(Self { pool })
    }
}

#[async_trait]
impl LogStorage for SqliteLogStorage {
    type ErrorType = anyhow::Error;

    async fn append(&self, entry: LogEntry) -> Result<Index, Self::ErrorType> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("INSERT INTO log (leader_info, term, data) VALUES (?, ?, ?, ?)")
            .bind(entry.leader_info)
            .bind(entry.term.0 as i64)
            .bind(entry.data.0)
            .execute(&mut *tx)
            .await?;

        let id: i64 = sqlx::query_scalar("SELECT last_insert_rowid()")
            .fetch_one(&mut *tx)
            .await?;

        tx.commit().await?;

        Ok(Index(id as u64))
    }

    async fn get(&self, index: Index) -> Result<Option<LogEntry>, Self::ErrorType> {
        let entry = sqlx::query_as::<_, (Vec<u8>, i64, Vec<u8>)>("SELECT leader_info, term, data FROM log WHERE id = ?")
            .bind(index.0 as i64)
            .fetch_optional(&self.pool)
            .await?;

        Ok(entry.map(|(leader_info, term, data)| LogEntry {
            leader_info,
            term: Term(term as u64),
            data: Data(data),
        }))
    }

    async fn last_index(&self) -> Result<Index, Self::ErrorType> {
        sqlx::query_scalar("SELECT MAX(id) FROM log")
            .fetch_one(&self.pool)
            .await
            .map(|id: Option<i64>| Index(id.unwrap_or(0) as u64))
            .map_err(Into::into)
    }

    async fn last_log_term(&self) -> Result<Term, Self::ErrorType> {
        sqlx::query_scalar("SELECT term FROM log ORDER BY id DESC LIMIT 1")
            .fetch_optional(&self.pool)
            .await
            .map(|o| o.unwrap_or(0))
            .map(|term: i64| Term(term as u64))
            .map_err(Into::into)
    }

    async fn entries_from(&self, index: Index) -> Result<Vec<LogEntry>, Self::ErrorType> {
        sqlx::query_as::<_, (Vec<u8>, i64, Vec<u8>)>("SELECT leader_info, term, data FROM log WHERE id >= ?")
            .bind(index.0 as i64)
            .fetch_all(&self.pool)
            .await
            .map(|entries| {
                entries.into_iter().map(|(leader_info, term, data)| LogEntry {
                    leader_info,
                    term: Term(term as u64),
                    data: Data(data),
                }).collect()
            })
            .map_err(Into::into)
    }

    async fn truncate(&self, index: Index) -> Result<(), Self::ErrorType> {
        sqlx::query("DELETE FROM log WHERE id >= ?")
            .bind(index.0 as i64)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod log_storage_tests {
    use crate::raft::core::storage::sqlite::SqliteLogStorage;
    use crate::raft::core::storage::test::test_log_storage;
    use crate::raft::core::storage::LogStorage;
    use crate::raft::types::{Data, Index, LogEntry, Term};
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_sqlite_log_storage() {
        let temp_file = NamedTempFile::new().unwrap();
        let filename = temp_file.path().to_str().unwrap();
        let storage = SqliteLogStorage::new(filename).await.unwrap();
        test_log_storage(storage).await;
    }

    #[tokio::test]
    async fn test_sqlite_log_storage_persist() {
        let temp_file = NamedTempFile::new().unwrap();
        let filename = temp_file.path().to_str().unwrap();
        {
            let storage = SqliteLogStorage::new(filename).await.unwrap();

            let entry1 = LogEntry {
                leader_info: b"leader1".to_vec(),
                term: Term(1),
                data: Data(b"hello".to_vec()),
            };
            let entry2 = LogEntry {
                leader_info: b"leader2".to_vec(),
                term: Term(1),
                data: Data(b"world".to_vec()),
            };

            storage.append(entry1.clone()).await.unwrap();
            storage.append(entry2.clone()).await.unwrap();
        }

        let storage = SqliteLogStorage::new(filename).await.unwrap();
        assert!(storage.get(Index(1)).await.unwrap().is_some());
        assert!(storage.get(Index(2)).await.unwrap().is_some());
    }
}

pub struct SqliteCoreStorage {
    pool: sqlx::SqlitePool,
    log_storage: SqliteLogStorage,
}

impl SqliteCoreStorage {
    pub async fn new(state_filename: &str, log_filename: &str) -> anyhow::Result<Self> {
        create_file_with_dirs(state_filename);
        let log_storage = SqliteLogStorage::new(log_filename).await?;

        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect(state_filename)
            .await?;

        // Set up the table.
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS state (
                current_term INTEGER NOT NULL,
                voted_for_host TEXT,
                voted_for_port INTEGER
            )
            "#,
        )
            .execute(&pool)
            .await?;

        // Check if this is a new or existing database. If new, set initial values.
        let current_term: Option<i64> = sqlx::query_scalar("SELECT current_term FROM state")
            .fetch_optional(&pool)
            .await?;
        let is_set_up = current_term.is_some();

        if !is_set_up {
            sqlx::query("INSERT INTO state (current_term, voted_for_host, voted_for_port) VALUES (0, NULL, NULL)")
                .execute(&pool)
                .await?;
        }

        Ok(Self { pool, log_storage })
    }
}

#[async_trait]
impl CoreStorage for SqliteCoreStorage {
    type LogStorage = SqliteLogStorage;
    type ErrorType = anyhow::Error;

    async fn current_term(&self) -> Result<Term, Self::ErrorType> {
        sqlx::query_scalar("SELECT current_term FROM state")
            .fetch_one(&self.pool)
            .await
            .map(|term: i64| Term(term as u64))
            .map_err(Into::into)
    }

    async fn set_current_term(&self, term: Term) -> Result<(), Self::ErrorType> {
        sqlx::query("UPDATE state SET current_term = ?")
            .bind(term.0 as i64)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(Into::into)
    }

    async fn voted_for(&self) -> Result<Option<NodeId>, Self::ErrorType> {
        let (host, port): (Option<String>, Option<i64>) =
            sqlx::query_as("SELECT voted_for_host, voted_for_port FROM state")
                .fetch_one(&self.pool)
                .await?;

        if let (Some(host), Some(port)) = (host, port) {
            Ok(Some(NodeId {
                host,
                port: port as u16,
            }))
        } else {
            Ok(None)
        }
    }

    async fn set_voted_for(&self, candidate_id: Option<NodeId>) -> Result<(), Self::ErrorType> {
        sqlx::query("UPDATE state SET voted_for_host = ?, voted_for_port = ?")
            .bind(candidate_id.as_ref().map(|id| &id.host))
            .bind(candidate_id.as_ref().map(|id| id.port as i64))
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(Into::into)
    }

    fn log_storage(&self) -> Self::LogStorage {
        self.log_storage.clone()
    }
}

#[cfg(test)]
mod core_storage_tests {
    use crate::raft::core::storage::sqlite::SqliteCoreStorage;
    use crate::raft::core::storage::CoreStorage;
    use crate::raft::types::{NodeId, Term};
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_sqlite_core_storage() {
        let state_temp_file = NamedTempFile::new().unwrap();
        let state_filename = state_temp_file.path().to_str().unwrap();

        let log_temp_file = NamedTempFile::new().unwrap();
        let log_filename = log_temp_file.path().to_str().unwrap();

        // First run
        {
            let storage = SqliteCoreStorage::new(state_filename, log_filename).await.unwrap();

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

        // Second run
        {
            let storage = SqliteCoreStorage::new(state_filename, log_filename).await.unwrap();

            assert_eq!(storage.current_term().await.unwrap(), Term(1));
            assert_eq!(storage.voted_for().await.unwrap(), Some(NodeId {
                host: "node1".to_string(),
                port: 1234,
            }));
        }
    }
}

fn create_file_with_dirs<P: AsRef<Path>>(path: P) {
    if let Some(parent) = path.as_ref().parent() {
        // Create parent directories if they don't exist
        fs::create_dir_all(parent).unwrap();
    }
    // Create the file (or open if it exists)
    OpenOptions::new().create(true).write(true).open(path).unwrap();
}
