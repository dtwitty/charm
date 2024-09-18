use crate::storage::KvStore;
use anyhow::Context;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::SqlitePool;
use tonic::async_trait;

pub struct SqliteKv {
    pool: SqlitePool,
}

impl SqliteKv {
    pub async fn new(filename: &str) -> anyhow::Result<Self> {
        // Get a Sqlite Connection.
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(filename)
            .await
            .context("Failed to create SQLite pool")?;

        // Set up the table.
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS kv (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            "#,
        ).execute(&pool).await.context("Failed to create table")?;

        // Hand back the instance.
        Ok(Self { pool })
    }
}

#[async_trait]
impl KvStore for SqliteKv {
    type ErrorType = anyhow::Error;

    async fn get(&self, key: &str) -> Result<Option<String>, Self::ErrorType> {
        // Fetch the value from the database.
        sqlx::query_scalar("SELECT value FROM kv WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .context("Failed to fetch key")
    }

    async fn set(&self, key: &str, value: &str) -> Result<(), Self::ErrorType> {
        // Insert the key-value pair into the database.
        sqlx::query("INSERT INTO kv (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value")
            .bind(key)
            .bind(value)
            .execute(&self.pool)
            .await
            .context("Failed to set key-value pair")?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), Self::ErrorType> {
        // Delete the key-value pair from the database.
        sqlx::query("DELETE FROM kv WHERE key = ?")
            .bind(key)
            .execute(&self.pool)
            .await
            .context("Failed to delete key")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::sqlite_kv::SqliteKv;
    use crate::storage::KvStore;

    #[tokio::test]
    async fn test_sqlite_kv() {
        // When this is dropped, it will delete the file.
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let filename = temp_file.path().to_str().unwrap();

        let kv_store = SqliteKv::new(filename).await.unwrap();
        kv_store.set("hello", "world").await.unwrap();
        assert_eq!(kv_store.get("hello").await.unwrap(), Some("world".to_string()));
        kv_store.set("hello", "mom").await.unwrap();
        assert_eq!(kv_store.get("hello").await.unwrap(), Some("mom".to_string()));
        kv_store.delete("hello").await.unwrap();
        assert_eq!(kv_store.get("hello").await.unwrap(), None);
        kv_store.delete("hello").await.unwrap();
        assert_eq!(kv_store.get("hello").await.unwrap(), None);

        kv_store.set("hello", "world").await.unwrap();
        drop(kv_store);
        let kv_store_2 = SqliteKv::new(filename).await.unwrap();
        assert_eq!(kv_store_2.get("hello").await.unwrap(), Some("world".to_string()));
    }
}
