use async_trait::async_trait;
use std::sync::Arc;
use turbo_vector_queue::{CasObjectStore, QueueError, VersionedBytes};
use turbo_vector_storage::ObjectStore;

#[derive(Clone)]
pub(crate) struct ObjectStoreCasAdapter {
    storage: Arc<dyn ObjectStore>,
}

impl ObjectStoreCasAdapter {
    pub(crate) fn new(storage: Arc<dyn ObjectStore>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl CasObjectStore for ObjectStoreCasAdapter {
    async fn read(&self, key: &str) -> turbo_vector_queue::Result<Option<VersionedBytes>> {
        let maybe = self
            .storage
            .get_bytes_with_version(key)
            .await
            .map_err(|e| QueueError::Storage(e.to_string()))?;
        Ok(maybe.map(|versioned| VersionedBytes {
            bytes: versioned.bytes,
            version: versioned.version,
        }))
    }

    async fn write_cas(
        &self,
        key: &str,
        data: &[u8],
        expected_version: Option<&str>,
    ) -> turbo_vector_queue::Result<bool> {
        self.storage
            .put_bytes_cas(key, data, expected_version)
            .await
            .map_err(|e| QueueError::Storage(e.to_string()))
    }
}
