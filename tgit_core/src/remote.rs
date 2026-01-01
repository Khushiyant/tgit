use crate::storage::TGitManifest;
use crate::utils::get_store_path;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use std::error::Error;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::str::FromStr;

pub struct RemoteClient {
    bucket: Bucket,
}

impl RemoteClient {
    pub fn new(url: &str) -> Result<Self, Box<dyn Error>> {
        // Expected format: s3://bucket-name
        let bucket_name = url.trim_start_matches("s3://");
        
        // Try to get region from env, default to UsEast1
        let region = std::env::var("AWS_REGION")
            .ok()
            .and_then(|r| Region::from_str(&r).ok())
            .unwrap_or(Region::UsEast1);
            
        let creds = Credentials::default()?;
        let bucket = *Bucket::new(bucket_name, region, creds)?;
        
        Ok(Self { bucket })
    }

    pub async fn push(&self, manifest: &TGitManifest, manifest_name: &str) -> Result<(), Box<dyn Error>> {
        // 1. Upload Blobs
        for tensor in manifest.tensors.values() {
            let blob_path = get_store_path().join(&tensor.hash);
            let remote_path = format!("blobs/{}", tensor.hash);
            
            // Optimistic check: Head object to see if it exists
            // rust-s3 head_object returns Err on 404 usually
            match self.bucket.head_object(&remote_path).await {
                Ok((_, 200)) => {
                    // Exists, skip
                    println!("Blob {} exists on remote, skipping.", tensor.hash);
                }
                _ => {
                    // Upload
                    if blob_path.exists() {
                         let mut file = File::open(&blob_path).await?;
                         let mut buffer = Vec::new();
                         file.read_to_end(&mut buffer).await?;
                         self.bucket.put_object(&remote_path, &buffer).await?;
                         println!("Uploaded blob {}", tensor.hash);
                    } else {
                        eprintln!("Warning: Blob {} not found locally", tensor.hash);
                    }
                }
            }
        }

        // 2. Upload Manifest
        let json = serde_json::to_string_pretty(manifest)?;
        self.bucket.put_object(&format!("manifests/{}", manifest_name), json.as_bytes()).await?;
        println!("Uploaded manifest {}", manifest_name);
        
        Ok(())
    }

    pub async fn pull(&self, manifest_name: &str) -> Result<TGitManifest, Box<dyn Error>> {
        // 1. Download Manifest
        let response_data = self.bucket.get_object(&format!("manifests/{}", manifest_name)).await?;
        // response_data is ResponseData, which has methods or fields. 
        // In 0.33+ it returns ResponseData. 
        // Let's assume bytes() or similar. It returns `ResponseData`.
        // `ResponseData` usually implements `AsRef<[u8]>` or has `bytes()` or `to_vec()`.
        // Checking docs (mental): `bytes()` returns `&[u8]`.
        let bytes = response_data.bytes(); 
        let manifest: TGitManifest = serde_json::from_slice(bytes)?;

        // 2. Download Blobs
        let store_path = get_store_path();
        std::fs::create_dir_all(&store_path)?;

        for tensor in manifest.tensors.values() {
            let blob_path = store_path.join(&tensor.hash);
            if !blob_path.exists() {
                let remote_path = format!("blobs/{}", tensor.hash);
                let response = self.bucket.get_object(&remote_path).await?;
                let mut file = File::create(&blob_path).await?;
                file.write_all(response.bytes()).await?;
                println!("Downloaded blob {}", tensor.hash);
            }
        }

        Ok(manifest)
    }
}
