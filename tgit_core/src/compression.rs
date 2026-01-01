use crate::errors::{TGitError, Result};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;

/// Compression level (0-9, where 9 is maximum compression)
const COMPRESSION_LEVEL: u32 = 6;

/// Compress data using zstd
pub fn compress_blob(data: &[u8]) -> Result<Vec<u8>> {
    zstd::encode_all(data, COMPRESSION_LEVEL as i32)
        .map_err(|e| TGitError::CompressionError(e.to_string()))
}

/// Decompress data using zstd
pub fn decompress_blob(compressed: &[u8]) -> Result<Vec<u8>> {
    zstd::decode_all(compressed)
        .map_err(|e| TGitError::DecompressionError(e.to_string()))
}

/// Save blob with optional compression
/// Returns true if compression was used
pub fn save_blob_with_compression(
    blob_path: &Path,
    data: &[u8],
    enable_compression: bool,
) -> Result<bool> {
    // Create parent directory if needed
    if let Some(parent) = blob_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let (final_data, compressed) = if enable_compression {
        let compressed_data = compress_blob(data)?;
        
        // Only use compression if it actually reduces size
        if compressed_data.len() < data.len() {
            (compressed_data, true)
        } else {
            (data.to_vec(), false)
        }
    } else {
        (data.to_vec(), false)
    };

    // Atomic write: write to temp file, then rename
    let tmp_path = blob_path.with_extension("tmp");
    let mut file = File::create(&tmp_path)?;
    
    // Write compression flag (1 byte) + data
    file.write_all(&[if compressed { 1u8 } else { 0u8 }])?;
    file.write_all(&final_data)?;
    file.sync_all()?;
    
    fs::rename(tmp_path, blob_path)?;
    
    Ok(compressed)
}

/// Load blob with automatic decompression
pub fn load_blob_with_decompression(blob_path: &Path) -> Result<Vec<u8>> {
    let mut file = File::open(blob_path)?;
    
    // Read compression flag
    let mut flag = [0u8; 1];
    file.read_exact(&mut flag)?;
    
    let is_compressed = flag[0] == 1;
    
    // Read rest of data
    let mut data = Vec::new();
    file.read_to_end(&mut data)?;
    
    if is_compressed {
        decompress_blob(&data)
    } else {
        Ok(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_roundtrip() {
        let original = vec![42u8; 10000]; // Highly compressible
        let compressed = compress_blob(&original).unwrap();
        let decompressed = decompress_blob(&compressed).unwrap();
        
        assert_eq!(original, decompressed);
        assert!(compressed.len() < original.len(), "Data should be compressed");
    }

    #[test]
    fn test_random_data_compression() {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let original: Vec<u8> = (0..1000).map(|_| rng.r#gen()).collect();
        
        let compressed = compress_blob(&original).unwrap();
        let decompressed = decompress_blob(&compressed).unwrap();
        
        assert_eq!(original, decompressed);
    }
}
