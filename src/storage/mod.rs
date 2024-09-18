pub mod sqlite_kv;

use tonic::async_trait;

#[async_trait]
pub trait KvStore {
    type ErrorType;

    async fn get(&self, key: &str) -> Result<Option<String>, Self::ErrorType>;
    async fn set(&self, key: &str, value: &str) -> Result<(), Self::ErrorType>;
    async fn delete(&self, key: &str) -> Result<(), Self::ErrorType>;
}
