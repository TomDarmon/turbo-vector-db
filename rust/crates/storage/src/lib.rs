use async_trait::async_trait;
use aws_credential_types::Credentials;
use aws_sdk_s3::{primitives::ByteStream, Client as AwsS3Client};
use aws_types::region::Region;
use google_cloud_storage::{
    client::{Storage as GcsStorageClient, StorageControl as GcsStorageControlClient},
    Error as GoogleCloudStorageError,
};
use std::{str::FromStr, sync::Arc, time::Duration};
use tokio::time::sleep;
use turbo_vector_core::{Result, TurboVectorError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedObject {
    pub bytes: Vec<u8>,
    pub version: String,
}

#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn put_bytes(&self, key: &str, data: &[u8]) -> Result<()>;
    async fn put_bytes_if_absent(&self, key: &str, data: &[u8]) -> Result<bool>;
    async fn get_bytes(&self, key: &str) -> Result<Vec<u8>>;
    async fn get_bytes_with_version(&self, key: &str) -> Result<Option<VersionedObject>>;
    async fn put_bytes_cas(
        &self,
        key: &str,
        data: &[u8],
        expected_version: Option<&str>,
    ) -> Result<bool>;
    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>>;
    async fn delete_bytes(&self, key: &str) -> Result<()>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloudObjectStoreProvider {
    S3,
    Gcs,
}

impl CloudObjectStoreProvider {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::S3 => "s3",
            Self::Gcs => "gcs",
        }
    }

    pub const fn default_endpoint(self) -> &'static str {
        match self {
            Self::S3 => "http://127.0.0.1:9000",
            Self::Gcs => "https://storage.googleapis.com",
        }
    }

    pub const fn default_region(self) -> &'static str {
        match self {
            Self::S3 => "us-east-1",
            Self::Gcs => "auto",
        }
    }
}

impl FromStr for CloudObjectStoreProvider {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "s3" | "aws" | "aws-s3" | "aws_s3" => Ok(Self::S3),
            "gcs" | "google" | "google-cloud-storage" | "google_cloud_storage" => Ok(Self::Gcs),
            _ => Err("expected one of: s3, gcs".to_string()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct S3Config {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub simulated_latency_ms: u64,
}

#[derive(Debug, Clone)]
pub struct GcsConfig {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub simulated_latency_ms: u64,
}

#[derive(Debug, Clone)]
pub enum CloudObjectStoreConfig {
    S3(S3Config),
    Gcs(GcsConfig),
}

impl CloudObjectStoreConfig {
    pub const fn provider(&self) -> CloudObjectStoreProvider {
        match self {
            Self::S3(_) => CloudObjectStoreProvider::S3,
            Self::Gcs(_) => CloudObjectStoreProvider::Gcs,
        }
    }
}

pub async fn build_object_store(config: CloudObjectStoreConfig) -> Result<Arc<dyn ObjectStore>> {
    match config {
        CloudObjectStoreConfig::S3(config) => Ok(Arc::new(S3ObjectStore::new(config).await?)),
        CloudObjectStoreConfig::Gcs(config) => Ok(Arc::new(GcsObjectStore::new(config).await?)),
    }
}

#[derive(Debug, Clone)]
pub struct S3ObjectStore {
    config: S3Config,
    client: AwsS3Client,
    simulated_latency: Duration,
}

impl S3ObjectStore {
    pub async fn new(config: S3Config) -> Result<Self> {
        Self::new_with_options(config, true, true).await
    }

    async fn new_with_options(
        config: S3Config,
        force_path_style: bool,
        create_bucket_if_missing: bool,
    ) -> Result<Self> {
        let creds = Credentials::new(
            config.access_key.clone(),
            config.secret_key.clone(),
            None,
            None,
            "turbo-vector-static",
        );
        let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(Region::new(config.region.clone()))
            .credentials_provider(creds)
            .endpoint_url(config.endpoint.clone())
            .load()
            .await;

        let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
            .endpoint_url(config.endpoint.clone())
            .force_path_style(force_path_style)
            .build();
        let client = AwsS3Client::from_conf(s3_config);

        let simulated_latency = Duration::from_millis(config.simulated_latency_ms);
        let store = Self {
            config,
            client,
            simulated_latency,
        };
        store.ensure_bucket(create_bucket_if_missing).await?;
        Ok(store)
    }

    pub fn config(&self) -> &S3Config {
        &self.config
    }

    async fn maybe_simulate_latency(&self) {
        if !self.simulated_latency.is_zero() {
            sleep(self.simulated_latency).await;
        }
    }

    async fn ensure_bucket(&self, create_bucket_if_missing: bool) -> Result<()> {
        self.maybe_simulate_latency().await;
        match self
            .client
            .head_bucket()
            .bucket(&self.config.bucket)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                let head_msg = format!("{e:?}");
                if !is_bucket_missing_error(&head_msg) {
                    return Err(TurboVectorError::Storage(head_msg));
                }

                if !create_bucket_if_missing {
                    return Err(TurboVectorError::Storage(format!(
                        "bucket '{}' was not found and auto-create is disabled",
                        self.config.bucket
                    )));
                }

                self.maybe_simulate_latency().await;
                match self
                    .client
                    .create_bucket()
                    .bucket(&self.config.bucket)
                    .send()
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(create_err) => {
                        let create_msg = format!("{create_err:?}");
                        if create_msg.contains("BucketAlreadyOwnedByYou")
                            || create_msg.contains("BucketAlreadyExists")
                        {
                            Ok(())
                        } else {
                            Err(TurboVectorError::Storage(create_msg))
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct GcsObjectStore {
    config: GcsConfig,
    data_client: GcsStorageClient,
    control_client: GcsStorageControlClient,
    bucket_resource: String,
    simulated_latency: Duration,
}

impl GcsObjectStore {
    pub async fn new(config: GcsConfig) -> Result<Self> {
        let data_client = GcsStorageClient::builder()
            .with_endpoint(config.endpoint.clone())
            .build()
            .await
            .map_err(|error| {
                if error.is_default_credentials() {
                    TurboVectorError::Storage(format!(
                        "failed to initialize GCS data client: missing Application Default Credentials ({error})"
                    ))
                } else {
                    TurboVectorError::Storage(format!(
                        "failed to initialize GCS data client: {error}"
                    ))
                }
            })?;
        let control_client = GcsStorageControlClient::builder()
            .with_endpoint(config.endpoint.clone())
            .build()
            .await
            .map_err(|error| {
                if error.is_default_credentials() {
                    TurboVectorError::Storage(format!(
                        "failed to initialize GCS control client: missing Application Default Credentials ({error})"
                    ))
                } else {
                    TurboVectorError::Storage(format!(
                        "failed to initialize GCS control client: {error}"
                    ))
                }
            })?;
        let simulated_latency = Duration::from_millis(config.simulated_latency_ms);
        let bucket_resource = format!("projects/_/buckets/{}", config.bucket);
        let store = Self {
            config,
            data_client,
            control_client,
            bucket_resource,
            simulated_latency,
        };
        store.ensure_bucket().await?;
        Ok(store)
    }

    pub fn config(&self) -> &GcsConfig {
        &self.config
    }

    async fn maybe_simulate_latency(&self) {
        if !self.simulated_latency.is_zero() {
            sleep(self.simulated_latency).await;
        }
    }

    async fn ensure_bucket(&self) -> Result<()> {
        self.maybe_simulate_latency().await;
        match self
            .control_client
            .get_bucket()
            .set_name(self.bucket_resource.clone())
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(error) => {
                if error.http_status_code() == Some(404) {
                    return Err(TurboVectorError::Storage(format!(
                        "bucket '{}' was not found in GCS and auto-create is disabled",
                        self.config.bucket
                    )));
                }
                Err(gcs_operation_error("bucket existence check", &error))
            }
        }
    }
}

#[async_trait]
impl ObjectStore for GcsObjectStore {
    async fn put_bytes(&self, key: &str, data: &[u8]) -> Result<()> {
        self.maybe_simulate_latency().await;
        self.data_client
            .write_object(
                self.bucket_resource.clone(),
                key,
                bytes::Bytes::copy_from_slice(data),
            )
            .send_buffered()
            .await
            .map_err(|error| gcs_operation_error("put object", &error))?;
        Ok(())
    }

    async fn put_bytes_if_absent(&self, key: &str, data: &[u8]) -> Result<bool> {
        self.maybe_simulate_latency().await;
        match self
            .data_client
            .write_object(
                self.bucket_resource.clone(),
                key,
                bytes::Bytes::copy_from_slice(data),
            )
            .set_if_generation_match(0_i64)
            .send_buffered()
            .await
        {
            Ok(_) => Ok(true),
            Err(error) if error.http_status_code() == Some(412) => Ok(false),
            Err(error) => Err(gcs_operation_error("put object if absent", &error)),
        }
    }

    async fn get_bytes(&self, key: &str) -> Result<Vec<u8>> {
        self.maybe_simulate_latency().await;
        let mut response = match self
            .data_client
            .read_object(self.bucket_resource.clone(), key)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) if error.http_status_code() == Some(404) => {
                return Err(TurboVectorError::NotFound(key.to_string()));
            }
            Err(error) => return Err(gcs_operation_error("get object bytes", &error)),
        };
        let mut bytes = Vec::new();
        while let Some(chunk) = response.next().await {
            let chunk = chunk.map_err(|error| gcs_operation_error("get object bytes", &error))?;
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }

    async fn get_bytes_with_version(&self, key: &str) -> Result<Option<VersionedObject>> {
        self.maybe_simulate_latency().await;
        let mut response = match self
            .data_client
            .read_object(self.bucket_resource.clone(), key)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) if error.http_status_code() == Some(404) => return Ok(None),
            Err(error) => {
                return Err(gcs_operation_error("get object bytes with version", &error));
            }
        };
        let version = response.object().generation.to_string();
        let mut bytes = Vec::new();
        while let Some(chunk) = response.next().await {
            let chunk = chunk
                .map_err(|error| gcs_operation_error("get object bytes with version", &error))?;
            bytes.extend_from_slice(&chunk);
        }
        Ok(Some(VersionedObject { bytes, version }))
    }

    async fn put_bytes_cas(
        &self,
        key: &str,
        data: &[u8],
        expected_version: Option<&str>,
    ) -> Result<bool> {
        let if_generation_match = match expected_version {
            Some(version) => parse_expected_generation(version)?,
            None => 0_i64,
        };
        self.maybe_simulate_latency().await;
        match self
            .data_client
            .write_object(
                self.bucket_resource.clone(),
                key,
                bytes::Bytes::copy_from_slice(data),
            )
            .set_if_generation_match(if_generation_match)
            .send_buffered()
            .await
        {
            Ok(_) => Ok(true),
            Err(error) if error.http_status_code() == Some(412) => Ok(false),
            Err(error) => Err(gcs_operation_error("put object CAS", &error)),
        }
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let mut out = Vec::new();
        let mut page_token = String::new();
        loop {
            self.maybe_simulate_latency().await;
            let mut request = self
                .control_client
                .list_objects()
                .set_parent(self.bucket_resource.clone())
                .set_prefix(prefix);
            if !page_token.is_empty() {
                request = request.set_page_token(page_token.clone());
            }
            let response = request
                .send()
                .await
                .map_err(|error| gcs_operation_error("list objects", &error))?;
            out.extend(
                response
                    .objects
                    .into_iter()
                    .filter_map(|object| (!object.name.is_empty()).then_some(object.name)),
            );
            if response.next_page_token.is_empty() {
                break;
            } else {
                page_token = response.next_page_token;
            }
        }
        out.sort();
        Ok(out)
    }

    async fn delete_bytes(&self, key: &str) -> Result<()> {
        self.maybe_simulate_latency().await;
        match self
            .control_client
            .delete_object()
            .set_bucket(self.bucket_resource.clone())
            .set_object(key)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if error.http_status_code() == Some(404) => Ok(()),
            Err(error) => Err(gcs_operation_error("delete object", &error)),
        }
    }
}

fn is_bucket_missing_error(error_message: &str) -> bool {
    let normalized = error_message.to_ascii_lowercase();
    normalized.contains("nosuchbucket")
        || normalized.contains("notfound")
        || normalized.contains("not found")
        || normalized.contains("404")
}

fn parse_expected_generation(version: &str) -> Result<i64> {
    let trimmed = version.trim();
    if trimmed.is_empty() {
        return Err(TurboVectorError::Storage(
            "expected non-empty CAS version for GCS object write".to_string(),
        ));
    }
    let generation = trimmed.parse::<i64>().map_err(|error| {
        TurboVectorError::Storage(format!(
            "expected numeric CAS version from GCS generation, got '{trimmed}': {error}"
        ))
    })?;
    if generation < 0 {
        return Err(TurboVectorError::Storage(format!(
            "expected non-negative CAS version from GCS generation, got '{trimmed}'"
        )));
    }
    Ok(generation)
}

fn gcs_operation_error(context: &str, error: &GoogleCloudStorageError) -> TurboVectorError {
    let Some(status) = error.http_status_code() else {
        return TurboVectorError::Storage(format!("GCS request failed ({context}): {error}"));
    };

    let payload = error
        .http_payload()
        .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
        .unwrap_or_else(|| error.to_string());
    let trimmed = payload.trim();
    let preview = if trimmed.is_empty() {
        "<empty>".to_string()
    } else {
        let mut sample: String = trimmed.chars().take(512).collect();
        if trimmed.chars().count() > 512 {
            sample.push_str("...");
        }
        sample
    };
    TurboVectorError::Storage(format!(
        "GCS request failed ({context}) with HTTP {status}: {preview}"
    ))
}

fn is_object_missing_error(error_message: &str) -> bool {
    let normalized = error_message.to_ascii_lowercase();
    normalized.contains("nosuchkey")
        || normalized.contains("notfound")
        || normalized.contains("not found")
        || normalized.contains("404")
}

fn is_precondition_failed_error(error_message: &str) -> bool {
    let normalized = error_message.to_ascii_lowercase();
    normalized.contains("preconditionfailed")
        || normalized.contains("conditionnotmet")
        || normalized.contains("412")
}

#[async_trait]
impl ObjectStore for S3ObjectStore {
    async fn put_bytes(&self, key: &str, data: &[u8]) -> Result<()> {
        self.maybe_simulate_latency().await;
        self.client
            .put_object()
            .bucket(&self.config.bucket)
            .key(key)
            .body(ByteStream::from(data.to_vec()))
            .send()
            .await
            .map_err(|e| TurboVectorError::Storage(format!("{e:?}")))?;
        Ok(())
    }

    async fn get_bytes(&self, key: &str) -> Result<Vec<u8>> {
        self.maybe_simulate_latency().await;
        let resp = self
            .client
            .get_object()
            .bucket(&self.config.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| {
                let msg = format!("{e:?}");
                if is_object_missing_error(&msg) {
                    TurboVectorError::NotFound(key.to_string())
                } else {
                    TurboVectorError::Storage(msg)
                }
            })?;

        let bytes = resp
            .body
            .collect()
            .await
            .map_err(|e| TurboVectorError::Storage(e.to_string()))?
            .into_bytes()
            .to_vec();
        Ok(bytes)
    }

    async fn get_bytes_with_version(&self, key: &str) -> Result<Option<VersionedObject>> {
        self.maybe_simulate_latency().await;
        let resp = match self
            .client
            .get_object()
            .bucket(&self.config.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                let msg = format!("{e:?}");
                if is_object_missing_error(&msg) {
                    return Ok(None);
                }
                return Err(TurboVectorError::Storage(msg));
            }
        };
        let version = resp.e_tag().unwrap_or_default().to_string();
        let bytes = resp
            .body
            .collect()
            .await
            .map_err(|e| TurboVectorError::Storage(e.to_string()))?
            .into_bytes()
            .to_vec();
        Ok(Some(VersionedObject { bytes, version }))
    }

    async fn put_bytes_if_absent(&self, key: &str, data: &[u8]) -> Result<bool> {
        self.maybe_simulate_latency().await;
        match self
            .client
            .put_object()
            .bucket(&self.config.bucket)
            .key(key)
            .if_none_match("*")
            .body(ByteStream::from(data.to_vec()))
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                let msg = format!("{e:?}");
                if is_precondition_failed_error(&msg) {
                    Ok(false)
                } else {
                    Err(TurboVectorError::Storage(msg))
                }
            }
        }
    }

    async fn put_bytes_cas(
        &self,
        key: &str,
        data: &[u8],
        expected_version: Option<&str>,
    ) -> Result<bool> {
        let mut req = self
            .client
            .put_object()
            .bucket(&self.config.bucket)
            .key(key)
            .body(ByteStream::from(data.to_vec()));
        req = if let Some(version) = expected_version {
            req.if_match(version)
        } else {
            req.if_none_match("*")
        };
        self.maybe_simulate_latency().await;
        match req.send().await {
            Ok(_) => Ok(true),
            Err(e) => {
                let msg = format!("{e:?}");
                if is_precondition_failed_error(&msg) {
                    Ok(false)
                } else {
                    Err(TurboVectorError::Storage(msg))
                }
            }
        }
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let mut out = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.config.bucket)
                .prefix(prefix);

            if let Some(token) = continuation_token.clone() {
                req = req.continuation_token(token);
            }

            self.maybe_simulate_latency().await;
            let resp = req
                .send()
                .await
                .map_err(|e| TurboVectorError::Storage(format!("{e:?}")))?;

            for obj in resp.contents() {
                if let Some(key) = obj.key() {
                    out.push(key.to_string());
                }
            }

            if resp.is_truncated() == Some(true) {
                continuation_token = resp.next_continuation_token().map(ToString::to_string);
            } else {
                break;
            }
        }

        out.sort();
        Ok(out)
    }

    async fn delete_bytes(&self, key: &str) -> Result<()> {
        self.maybe_simulate_latency().await;
        self.client
            .delete_object()
            .bucket(&self.config.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| TurboVectorError::Storage(format!("{e:?}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cloud_object_store_provider_aliases() {
        assert_eq!(
            "s3".parse::<CloudObjectStoreProvider>().unwrap(),
            CloudObjectStoreProvider::S3
        );
        assert_eq!(
            "aws-s3".parse::<CloudObjectStoreProvider>().unwrap(),
            CloudObjectStoreProvider::S3
        );
        assert_eq!(
            "gcs".parse::<CloudObjectStoreProvider>().unwrap(),
            CloudObjectStoreProvider::Gcs
        );
        assert_eq!(
            "google-cloud-storage"
                .parse::<CloudObjectStoreProvider>()
                .unwrap(),
            CloudObjectStoreProvider::Gcs
        );
    }

    #[test]
    fn rejects_unknown_cloud_object_store_provider() {
        let error = "azure".parse::<CloudObjectStoreProvider>().unwrap_err();
        assert!(
            error.contains("expected one of: s3, gcs"),
            "unexpected parse error: {error}"
        );
    }

    #[test]
    fn cloud_provider_defaults_match_provider() {
        assert_eq!(
            CloudObjectStoreProvider::S3.default_endpoint(),
            "http://127.0.0.1:9000"
        );
        assert_eq!(CloudObjectStoreProvider::S3.default_region(), "us-east-1");
        assert_eq!(
            CloudObjectStoreProvider::Gcs.default_endpoint(),
            "https://storage.googleapis.com"
        );
        assert_eq!(CloudObjectStoreProvider::Gcs.default_region(), "auto");
    }

    #[test]
    fn parses_expected_gcs_generation() {
        assert_eq!(parse_expected_generation("123").unwrap(), 123_i64);
        assert_eq!(parse_expected_generation(" 456 ").unwrap(), 456_i64);
    }

    #[test]
    fn rejects_invalid_expected_gcs_generation() {
        assert!(parse_expected_generation("").is_err());
        assert!(parse_expected_generation("abc").is_err());
        assert!(parse_expected_generation("-1").is_err());
    }
}
