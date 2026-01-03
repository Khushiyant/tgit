use crate::blobs;
use crate::errors::{Result, VektError};
use crate::storage::VektManifest;
use crate::validation::validate_s3_url;
use futures::stream::{self, StreamExt};
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use std::str::FromStr;
use tokio::fs::File;

pub struct RemoteClient {
    bucket: Bucket,
}

impl RemoteClient {
    pub fn new(url: &str) -> Result<Self> {
        // Validate S3 URL format
        let bucket_name = validate_s3_url(url)?;

        let region = std::env::var("AWS_REGION")
            .ok()
            .and_then(|r| Region::from_str(&r).ok())
            .unwrap_or(Region::UsEast1);

        // Validate credentials exist before proceeding
        let creds = Credentials::default()
            .map_err(|e| VektError::CredentialError(format!(
                "Failed to load AWS credentials. Ensure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set, or configure ~/.aws/credentials: {}",
                e
            )))?;

        // Verify credentials are not empty
        if creds.access_key.is_none() || creds.secret_key.is_none() {
            return Err(VektError::CredentialError(
                "AWS credentials are empty. Please configure valid credentials.".to_string(),
            ));
        }

        let bucket = *Bucket::new(&bucket_name, region, creds).map_err(|e| {
            VektError::RemoteError(format!("Failed to initialize S3 bucket: {}", e))
        })?;

        Ok(Self { bucket })
    }

    /// Validates bucket access by attempting a list operation
    pub async fn validate_access(&self) -> Result<()> {
        self.bucket
            .list("/".to_string(), Some("/".to_string()))
            .await
            .map_err(|e| {
                VektError::RemoteError(format!(
                    "Failed to access S3 bucket. Check bucket name and permissions: {}",
                    e
                ))
            })?;
        Ok(())
    }

    pub async fn push(&self, manifest: &VektManifest, manifest_name: &str) -> Result<()> {
        // Check for existing manifest and warn about conflicts
        let manifest_path = format!("manifests/{}", manifest_name);
        if let Ok((_, 200)) = self.bucket.head_object(&manifest_path).await {
            eprintln!(
                "Warning: Manifest '{}' already exists on remote. This will overwrite the existing version.",
                manifest_name
            );
        }

        println!("Pushing {} blobs to remote...", manifest.tensors.len());

        let mut uploaded = 0;
        let mut skipped = 0;

        let tasks = stream::iter(manifest.tensors.values())
            .map(|tensor| {
                let hash = tensor.hash.clone();
                async move {
                    let blob_path = blobs::get_blob_path(&hash);
                    let remote_path = format!("blobs/{}", hash);

                    // Check if blob already exists on remote (avoid re-upload)
                    match self.bucket.head_object(&remote_path).await {
                        Ok((_, 200)) => Ok::<(bool, String), VektError>((false, hash)),
                        _ => {
                            if !blob_path.exists() {
                                return Err(VektError::BlobNotFound(format!(
                                    "Blob {} not found locally for upload",
                                    hash
                                )));
                            }

                            let mut file = File::open(&blob_path).await.map_err(|e| {
                                VektError::Io(std::io::Error::other(format!(
                                    "Failed to open blob {}: {}",
                                    hash, e
                                )))
                            })?;

                            let response = self
                                .bucket
                                .put_object_stream(&mut file, &remote_path)
                                .await
                                .map_err(|e| {
                                    VektError::RemoteError(format!(
                                        "Failed to upload blob {}: {}",
                                        hash, e
                                    ))
                                })?;

                            if response.status_code() != 200 {
                                return Err(VektError::RemoteError(format!(
                                    "Failed to upload blob {}, status: {}",
                                    hash,
                                    response.status_code()
                                )));
                            }

                            Ok((true, hash))
                        }
                    }
                }
            })
            .buffer_unordered(10);

        let results: Vec<_> = tasks.collect().await;
        for res in results {
            let (was_uploaded, hash) = res?;
            if was_uploaded {
                uploaded += 1;
                println!("Uploaded blob {}", hash);
            } else {
                skipped += 1;
            }
        }

        println!(
            "Upload complete: {} uploaded, {} skipped (already on remote)",
            uploaded, skipped
        );

        // Upload manifest with atomic-like behavior (S3 PUT is atomic)
        let json = serde_json::to_string_pretty(manifest).map_err(VektError::Json)?;

        self.bucket
            .put_object(&manifest_path, json.as_bytes())
            .await
            .map_err(|e| {
                VektError::RemoteError(format!(
                    "Failed to upload manifest {}: {}",
                    manifest_name, e
                ))
            })?;

        println!("Uploaded manifest {}", manifest_name);
        Ok(())
    }

    pub async fn pull(&self, manifest_name: &str) -> Result<VektManifest> {
        let manifest_path = format!("manifests/{}", manifest_name);

        let response_data = self.bucket.get_object(&manifest_path).await.map_err(|e| {
            VektError::RemoteError(format!(
                "Failed to download manifest '{}': {}. Ensure the manifest exists on remote.",
                manifest_name, e
            ))
        })?;

        let bytes = response_data.bytes();
        let manifest: VektManifest = serde_json::from_slice(bytes).map_err(|e| {
            VektError::InvalidManifest(format!(
                "Failed to parse manifest '{}': {}",
                manifest_name, e
            ))
        })?;

        println!(
            "Downloading {} blobs from remote...",
            manifest.tensors.len()
        );

        let mut downloaded = 0;
        let mut skipped = 0;

        let tasks = stream::iter(manifest.tensors.values())
            .map(|tensor| {
                let hash = tensor.hash.clone();
                async move {
                    let blob_path = blobs::get_blob_path(&hash);

                    // Skip if blob already exists locally
                    if blob_path.exists() {
                        return Ok::<bool, VektError>(false);
                    }

                    let remote_path = format!("blobs/{}", hash);

                    let mut stream =
                        self.bucket
                            .get_object_stream(&remote_path)
                            .await
                            .map_err(|e| {
                                VektError::RemoteError(format!(
                                    "Failed to download blob {}: {}",
                                    hash, e
                                ))
                            })?;

                    // Write to temp file first, then rename for atomicity
                    let tmp_path = blob_path.with_extension("tmp");
                    let mut file = File::create(&tmp_path).await.map_err(|e| {
                        VektError::Io(std::io::Error::other(format!(
                            "Failed to create temp file for blob {}: {}",
                            hash, e
                        )))
                    })?;

                    tokio::io::copy(&mut stream, &mut file).await.map_err(|e| {
                        VektError::Io(std::io::Error::other(format!(
                            "Failed to write blob {}: {}",
                            hash, e
                        )))
                    })?;

                    // Ensure data is flushed
                    file.sync_all().await.map_err(|e| {
                        VektError::Io(std::io::Error::other(format!(
                            "Failed to sync blob {}: {}",
                            hash, e
                        )))
                    })?;

                    drop(file);

                    // Atomic rename
                    tokio::fs::rename(&tmp_path, &blob_path)
                        .await
                        .map_err(|e| {
                            VektError::Io(std::io::Error::other(format!(
                                "Failed to finalize blob {}: {}",
                                hash, e
                            )))
                        })?;

                    Ok(true)
                }
            })
            .buffer_unordered(10);

        let results: Vec<_> = tasks.collect().await;
        for res in results {
            if res? {
                downloaded += 1;
            } else {
                skipped += 1;
            }
        }

        println!(
            "Download complete: {} downloaded, {} skipped (already local)",
            downloaded, skipped
        );

        Ok(manifest)
    }
}
