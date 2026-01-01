use crate::storage::TGitManifest;
use crate::utils::get_store_path;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use std::error::Error;
use tokio::{fs::File, io::AsyncWriteExt};
use std::str::FromStr;
use futures::stream::{self, StreamExt};

pub struct RemoteClient {
    bucket: Bucket,
}

impl RemoteClient {
    pub fn new(url: &str) -> Result<Self, Box<dyn Error>> {
        let bucket_name = url.trim_start_matches("s3://");
        let region = std::env::var("AWS_REGION")
            .ok()
            .and_then(|r| Region::from_str(&r).ok())
            .unwrap_or(Region::UsEast1);
            
        let creds = Credentials::default()?;
        let bucket = *Bucket::new(bucket_name, region, creds)?;
        Ok(Self { bucket })
    }

    pub async fn push(&self, manifest: &TGitManifest, manifest_name: &str) -> Result<(), Box<dyn Error>> {
        println!("Pushing {} blobs...", manifest.tensors.len());

        let tasks = stream::iter(manifest.tensors.values())
            .map(|tensor| async move {
                let blob_path = get_store_path().join(&tensor.hash);
                let remote_path = format!("blobs/{}", tensor.hash);
                
                match self.bucket.head_object(&remote_path).await {
                    Ok((_, 200)) => Ok(()), 
                    _ => {
                        if blob_path.exists() {
                             let mut file = File::open(&blob_path).await?;
                             // put_object_stream takes &mut AsyncRead and path
                             let response = self.bucket.put_object_stream(&mut file, &remote_path).await?;
                             if response.status_code() != 200 {
                                 return Err(format!("Failed to upload blob {}, status: {}", tensor.hash, response.status_code()).into());
                             }
                             println!("Uploaded blob {}", tensor.hash);
                             Ok(())
                        } else {
                            println!("Warning: Blob {} not found locally", tensor.hash);
                            Ok(())
                        }
                    }
                }
            })
            .buffer_unordered(10); 

        let results: Vec<_> = tasks.collect().await;
        for res in results {
            if let Err(e) = res {
                return Err(e); 
            }
        }

        let json = serde_json::to_string_pretty(manifest)?;
        self.bucket.put_object(&format!("manifests/{}", manifest_name), json.as_bytes()).await?;
        println!("Uploaded manifest {}", manifest_name);
        Ok(())
    }

    pub async fn pull(&self, manifest_name: &str) -> Result<TGitManifest, Box<dyn Error>> {
        let response_data = self.bucket.get_object(&format!("manifests/{}", manifest_name)).await?;
        let bytes = response_data.bytes(); 
        let manifest: TGitManifest = serde_json::from_slice(bytes)?;

        let store_path = get_store_path();
        std::fs::create_dir_all(&store_path)?;

        println!("Pulling {} blobs...", manifest.tensors.len());

        let tasks = stream::iter(manifest.tensors.values())
            .map(|tensor| async move {
                let blob_path = get_store_path().join(&tensor.hash);
                if !blob_path.exists() {
                    let remote_path = format!("blobs/{}", tensor.hash);
                    
                    // Fallback to non-streaming download to avoid trait issues with ResponseDataStream
                    // TODO: Investigate get_object_to_writer or Stream impl for ResponseDataStream
                    let response = self.bucket.get_object(&remote_path).await?;
                    let mut file = File::create(&blob_path).await?;
                    file.write_all(response.bytes()).await?;
                    
                    println!("Downloaded blob {}", tensor.hash);
                }
                Ok::<_, Box<dyn Error + Send + Sync>>(())
            })
            .buffer_unordered(10); 

        let results: Vec<_> = tasks.collect().await;
        for res in results {
             if let Err(e) = res {
                return Err(e);
            }
        }

        Ok(manifest)
    }
}