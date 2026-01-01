use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};
use indexmap::IndexMap;

use tgit_core::{SafetensorFile, ModelArchiver};
use tgit_core::storage::{TGitManifest, ManifestTensor};
use tgit_core::utils::get_store_path;

// Helper to create a dummy blob
fn create_blob(data: &[u8]) -> String {
    let hash = blake3::hash(data);
    let hash_hex = hex::encode(hash.as_bytes());
    let store_path = get_store_path();
    std::fs::create_dir_all(&store_path).unwrap();
    let blob_path = store_path.join(&hash_hex);
    if !blob_path.exists() {
        let mut f = File::create(blob_path).unwrap();
        f.write_all(data).unwrap();
    }
    hash_hex
}

#[test]
fn test_full_cycle_restore() -> Result<(), Box<dyn std::error::Error>> {
    let original_path = "test_cycle_original.safetensors";
    let restored_path = "test_cycle_restored.safetensors";
    
    {
        let mut file = File::create(original_path)?;
        let header_json = r#"{"test_tensor": {"dtype":"F32", "shape":[1], "data_offsets":[0, 4]}}"#;
        let header_len = header_json.len() as u64;
        file.write_all(&header_len.to_le_bytes())?;
        file.write_all(header_json.as_bytes())?;
        file.write_all(&[1u8, 2u8, 3u8, 4u8])?; // The data
    }

    let file = SafetensorFile::open(original_path)?;
    let manifest = file.process(true).unwrap(); // true = save blobs

    std::fs::remove_file(original_path)?;

    manifest.restore(std::path::Path::new(restored_path))?;

    let mut f = File::open(restored_path)?;
    let mut buffer = Vec::new();
    f.read_to_end(&mut buffer)?;
    
    // Size = 8 (len) + 79 (header) + 4 (data) = 91 bytes
    assert!(buffer.len() > 70); 
    
    std::fs::remove_file(restored_path)?;
    
    Ok(())
}

#[test]
fn test_shared_weights_deduplication() {
    // Unique data to avoid conflict
    let data = vec![11u8, 22u8, 33u8, 44u8]; 
    let hash = create_blob(&data); 

    let mut tensors = HashMap::new();
    
    tensors.insert("tensor_a".to_string(), ManifestTensor {
        shape: vec![4],
        dtype: "U8".to_string(),
        hash: hash.clone(),
        index: 0,
        extra: IndexMap::new(),
    });

    tensors.insert("tensor_b".to_string(), ManifestTensor {
        shape: vec![4],
        dtype: "U8".to_string(),
        hash: hash.clone(),
        index: 1,
        extra: IndexMap::new(),
    });

    let manifest = TGitManifest {
        tensors,
        version: "1.0".to_string(),
        total_size: 4, 
    };

    let output_path = std::path::Path::new("test_shared.safetensors");
    manifest.restore(output_path).unwrap();

    let mut file = File::open(output_path).unwrap();
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).unwrap();

    let header_len = u64::from_le_bytes(buffer[0..8].try_into().unwrap()) as usize;
    let header_json = &buffer[8..8+header_len];
    let header_str = std::str::from_utf8(header_json).unwrap();
    let header: serde_json::Value = serde_json::from_str(header_str).unwrap();

    let offset_a = header["tensor_a"]["data_offsets"].as_array().unwrap();
    let offset_b = header["tensor_b"]["data_offsets"].as_array().unwrap();
    
    assert_eq!(offset_a, offset_b, "Offsets for shared weights must be identical");

    std::fs::remove_file(output_path).unwrap();
    std::fs::remove_file(get_store_path().join(&hash)).ok();
}

#[test]
fn test_alignment_padding() {
    let data_a = vec![0xCC];
    let data_b = vec![0xDD];
    let hash_a = create_blob(&data_a);
    let hash_b = create_blob(&data_b);

    let mut tensors = HashMap::new();
    
    tensors.insert("tensor_a".to_string(), ManifestTensor {
        shape: vec![1],
        dtype: "U8".to_string(),
        hash: hash_a.clone(),
        index: 0,
        extra: IndexMap::new(),
    });

    tensors.insert("tensor_b".to_string(), ManifestTensor {
        shape: vec![1],
        dtype: "U8".to_string(),
        hash: hash_b.clone(),
        index: 1,
        extra: IndexMap::new(),
    });

    let manifest = TGitManifest {
        tensors,
        version: "1.0".to_string(),
        total_size: 2,
    };

    let output_path = std::path::Path::new("test_aligned.safetensors");
    manifest.restore(output_path).unwrap();

    let mut file = File::open(output_path).unwrap();
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).unwrap();

    let header_len = u64::from_le_bytes(buffer[0..8].try_into().unwrap()) as usize;
    let data_start = 8 + header_len;
    let data_section = &buffer[data_start..];

    assert_eq!(data_section[0], 0xCC);
    for i in 1..8 {
        assert_eq!(data_section[i], 0x00, "Padding mismatch");
    }
    assert_eq!(data_section[8], 0xDD);

    std::fs::remove_file(output_path).unwrap();
    std::fs::remove_file(get_store_path().join(&hash_a)).ok();
    std::fs::remove_file(get_store_path().join(&hash_b)).ok();
}

#[test]
fn test_extra_metadata_preservation() {
    let data = vec![0xFF];
    let hash = create_blob(&data);

    let mut extra = IndexMap::new();
    extra.insert("quantization".to_string(), serde_json::json!("int8"));
    extra.insert("license".to_string(), serde_json::json!("MIT"));

    let mut tensors = HashMap::new();
    tensors.insert("tensor_meta".to_string(), ManifestTensor {
        shape: vec![1],
        dtype: "U8".to_string(),
        hash: hash.clone(),
        index: 0,
        extra,
    });

    let manifest = TGitManifest {
        tensors,
        version: "1.0".to_string(),
        total_size: 1,
    };

    let output_path = std::path::Path::new("test_meta.safetensors");
    manifest.restore(output_path).unwrap();

    let mut file = File::open(output_path).unwrap();
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).unwrap();
    
    let header_len = u64::from_le_bytes(buffer[0..8].try_into().unwrap()) as usize;
    let header_str = std::str::from_utf8(&buffer[8..8+header_len]).unwrap();
    
    assert!(header_str.contains("\"quantization\":\"int8\""));
    
    std::fs::remove_file(output_path).unwrap();
    std::fs::remove_file(get_store_path().join(&hash)).ok();
}
