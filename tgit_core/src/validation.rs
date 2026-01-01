use crate::errors::{TGitError, Result};
use std::path::Path;

/// Validates that a path doesn't contain path traversal attempts
pub fn validate_path_safe(path: &str) -> Result<()> {
    if path.contains("..") || path.starts_with('/') {
        return Err(TGitError::PathTraversal(path.to_string()));
    }
    Ok(())
}

/// Validates tensor name to prevent injection attacks
pub fn validate_tensor_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 256 {
        return Err(TGitError::InvalidTensorName(
            "Tensor name must be between 1 and 256 characters".to_string()
        ));
    }
    
    // Allow alphanumeric, dots, underscores, hyphens, and forward slashes
    if !name.chars().all(|c| c.is_alphanumeric() || c == '.' || c == '_' || c == '-' || c == '/') {
        return Err(TGitError::InvalidTensorName(
            format!("Invalid characters in tensor name: {}", name)
        ));
    }
    
    Ok(())
}

/// Validates S3 URL format
pub fn validate_s3_url(url: &str) -> Result<String> {
    if !url.starts_with("s3://") {
        return Err(TGitError::InvalidRemoteUrl(
            "URL must start with s3://".to_string()
        ));
    }
    
    let bucket_name = url.trim_start_matches("s3://").trim_end_matches('/');
    
    if bucket_name.is_empty() || bucket_name.len() > 63 {
        return Err(TGitError::InvalidRemoteUrl(
            "Bucket name must be between 1 and 63 characters".to_string()
        ));
    }
    
    // Validate bucket name (simplified S3 bucket naming rules)
    if !bucket_name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.') {
        return Err(TGitError::InvalidRemoteUrl(
            "Bucket name can only contain lowercase letters, numbers, hyphens, and dots".to_string()
        ));
    }
    
    Ok(bucket_name.to_string())
}

/// Verifies blob integrity by comparing hash
pub fn verify_blob_hash(data: &[u8], expected_hash: &str) -> Result<()> {
    let hash = blake3::hash(data);
    let actual_hash = hex::encode(hash.as_bytes());
    
    if actual_hash != expected_hash {
        return Err(TGitError::HashMismatch {
            expected: expected_hash.to_string(),
            actual: actual_hash,
        });
    }
    
    Ok(())
}

/// Validates that a file exists and is readable
pub fn validate_file_exists(path: &Path) -> Result<()> {
    if !path.exists() {
        return Err(TGitError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("File not found: {}", path.display())
        )));
    }
    
    if !path.is_file() {
        return Err(TGitError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Path is not a file: {}", path.display())
        )));
    }
    
    Ok(())
}
