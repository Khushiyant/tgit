use crate::blobs;
use crate::errors::{Result, VektError};
use regex::Regex;
use std::path::Path;
use std::sync::OnceLock;

// Compile regexes once and reuse them
static TENSOR_NAME_REGEX: OnceLock<Regex> = OnceLock::new();
static S3_BUCKET_REGEX: OnceLock<Regex> = OnceLock::new();

fn get_tensor_name_regex() -> &'static Regex {
    TENSOR_NAME_REGEX.get_or_init(|| Regex::new(r"^[a-zA-Z0-9._/-]+$").unwrap())
}

fn get_s3_bucket_regex() -> &'static Regex {
    S3_BUCKET_REGEX.get_or_init(|| {
        // S3 bucket naming rules: lowercase letters, numbers, hyphens, dots
        // Must be 1-63 characters
        Regex::new(r"^[a-z0-9][a-z0-9.-]{0,61}[a-z0-9]$").unwrap()
    })
}

/// Validates that a path doesn't contain path traversal attempts
pub fn validate_path_safe(path: &str) -> Result<()> {
    if path.contains("..") || path.starts_with('/') || path.starts_with('\\') {
        return Err(VektError::PathTraversal(format!(
            "Path contains unsafe characters or traversal attempt: {}",
            path
        )));
    }
    Ok(())
}

/// Validates tensor name to prevent injection attacks and path traversal
pub fn validate_tensor_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 256 {
        return Err(VektError::InvalidTensorName(
            "Tensor name must be between 1 and 256 characters".to_string(),
        ));
    }

    // Check for path traversal attempts
    if name.contains("..") || name.starts_with('/') || name.starts_with('\\') {
        return Err(VektError::PathTraversal(format!(
            "Tensor name contains path traversal attempt: {}",
            name
        )));
    }

    // Use regex for cleaner validation
    if !get_tensor_name_regex().is_match(name) {
        return Err(VektError::InvalidTensorName(format!(
            "Invalid characters in tensor name '{}'. Only alphanumeric, dots, underscores, hyphens, and forward slashes allowed.",
            name
        )));
    }

    Ok(())
}

/// Validates S3 URL format
pub fn validate_s3_url(url: &str) -> Result<String> {
    if !url.starts_with("s3://") {
        return Err(VektError::InvalidRemoteUrl(
            "URL must start with s3://".to_string(),
        ));
    }

    let bucket_name = url.trim_start_matches("s3://").trim_end_matches('/');

    // Use regex for cleaner S3 bucket name validation
    if !get_s3_bucket_regex().is_match(bucket_name) {
        return Err(VektError::InvalidRemoteUrl(format!(
            "Invalid S3 bucket name: '{}'. Must be 1-63 characters, lowercase letters, numbers, hyphens, and dots only",
            bucket_name
        )));
    }

    Ok(bucket_name.to_string())
}

/// Verifies blob integrity by comparing hash
pub fn verify_blob_hash(data: &[u8], expected_hash: &str) -> Result<()> {
    let actual_hash = blobs::compute_blob_hash(data);

    if actual_hash != expected_hash {
        return Err(VektError::HashMismatch {
            expected: expected_hash.to_string(),
            actual: actual_hash,
        });
    }

    Ok(())
}

/// Validates that a file exists and is readable
pub fn validate_file_exists(path: &Path) -> Result<()> {
    if !path.exists() {
        return Err(VektError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("File not found: {}", path.display()),
        )));
    }

    if !path.is_file() {
        return Err(VektError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Path is not a file: {}", path.display()),
        )));
    }

    Ok(())
}
