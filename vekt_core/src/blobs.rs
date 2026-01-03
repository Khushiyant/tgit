use crate::utils::get_store_path;
use std::fs::{self, File};
use std::io::Write;
/// Blob storage module - Single source of truth for all blob operations
use std::path::PathBuf;

/// Computes the blake3 hash of data and returns it as a hex string
/// Single source of truth for hash computation
pub fn compute_blob_hash(data: &[u8]) -> String {
    let hash = blake3::hash(data);
    hex::encode(hash.as_bytes())
}

/// Returns the full path to a blob given its hash
pub fn get_blob_path(hash: &str) -> PathBuf {
    get_store_path().join(hash)
}

/// Checks if a blob exists in storage
pub fn blob_exists(hash: &str) -> bool {
    get_blob_path(hash).exists()
}

/// Atomically writes data to a blob file using temp file + rename pattern
/// Returns the hash of the written data
/// Single source of truth for blob writing
pub fn write_blob_atomic(data: &[u8]) -> std::io::Result<String> {
    let hash = compute_blob_hash(data);
    let blob_path = get_blob_path(&hash);

    // Skip if already exists (deduplication)
    if blob_path.exists() {
        return Ok(hash);
    }

    // Ensure blobs directory exists
    let store_path = get_store_path();
    fs::create_dir_all(&store_path)?;

    // Atomic write: temp file + rename
    let tmp_path = blob_path.with_extension("tmp");
    let mut f = File::create(&tmp_path)?;
    f.write_all(data)?;
    f.sync_all()?;
    fs::rename(tmp_path, blob_path)?;

    Ok(hash)
}

/// Reads a blob from storage given its hash
pub fn read_blob(hash: &str) -> std::io::Result<Vec<u8>> {
    let blob_path = get_blob_path(hash);
    std::fs::read(&blob_path).map_err(|e| {
        std::io::Error::new(
            e.kind(),
            format!(
                "Failed to read blob {} from {}: {}",
                hash,
                blob_path.display(),
                e
            ),
        )
    })
}

/// Saves a blob only if it doesn't already exist (deduplication)
/// Returns the hash and whether it was newly written
pub fn save_blob_deduplicated(data: &[u8]) -> std::io::Result<(String, bool)> {
    let hash = compute_blob_hash(data);
    let existed = blob_exists(&hash);

    if !existed {
        write_blob_atomic(data)?;
    }

    Ok((hash, !existed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_blob_hash() {
        let data = b"test data";
        let hash1 = compute_blob_hash(data);
        let hash2 = compute_blob_hash(data);
        assert_eq!(hash1, hash2, "Hash should be deterministic");
        assert_eq!(hash1.len(), 64, "Blake3 hash should be 64 hex chars");
    }

    #[test]
    fn test_blob_deduplication() {
        let data = b"unique test data for dedup";

        // Cleanup before test to ensure fresh state
        let hash = compute_blob_hash(data);
        let _ = fs::remove_file(get_blob_path(&hash));

        let (hash1, written1) = save_blob_deduplicated(data).unwrap();
        let (hash2, written2) = save_blob_deduplicated(data).unwrap();

        assert_eq!(hash1, hash2);
        assert!(written1, "First write should occur");
        assert!(!written2, "Second write should be skipped (deduplicated)");
        assert!(blob_exists(&hash1));

        // Cleanup
        let _ = fs::remove_file(get_blob_path(&hash1));
    }

    #[test]
    fn test_write_and_read_blob() {
        let original_data = b"test blob content";
        let hash = write_blob_atomic(original_data).unwrap();
        let read_data = read_blob(&hash).unwrap();

        assert_eq!(original_data, &read_data[..]);

        // Cleanup
        let _ = fs::remove_file(get_blob_path(&hash));
    }
}
