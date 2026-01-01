use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use memmap2::MmapOptions;
use indexmap::IndexMap;

// Import from the public API of the crate
use tgit_core::{SafetensorFile, ModelArchiver};
use tgit_core::storage::{RawHeader, RawTensorMetaData};

#[test]
fn test_safetensor_new() {
    let mmap = MmapOptions::new().len(1024).map_anon().unwrap().make_read_only().unwrap();
    let header: RawHeader = IndexMap::new();
    let header_len = 128;

    let safetensor_file = SafetensorFile::new(mmap, header, header_len);

    assert_eq!(safetensor_file.header_len, header_len);
}

#[test]
fn test_safetensor_open() -> Result<(), Box<dyn std::error::Error>> {
    let path = "test_model_basic.safetensors";
    {
        //  Dummy file
        let mut file = File::create(path)?;
        
        let header_json = r#"{
            "tensor1": {
                "dtype": "F32",
                "shape": [1, 1],
                "data_offsets": [0, 4]
            }
        }"#;
        
        let header_len = header_json.len() as u64;
        
        // Write 8-byte length (Little Endian)
        file.write_all(&header_len.to_le_bytes())?;
        // Write JSON
        file.write_all(header_json.as_bytes())?;
        // Write 4 bytes of dummy data (matching data_offsets [0, 4])
        file.write_all(&[0u8, 0u8, 0u8, 0u8])?; 
    }

    let safetensor_file = SafetensorFile::open(path)?;

    // Assertions
    assert_eq!(safetensor_file.header.len(), 1);
    assert!(safetensor_file.header.contains_key("tensor1"));
    
    let tensor_meta = safetensor_file.header.get("tensor1").unwrap();
    assert_eq!(tensor_meta.dtype, "F32");

    std::fs::remove_file(path)?;
    
    Ok(())
}

#[test]
fn test_safetensor_process() {
    let mmap = MmapOptions::new().len(1024).map_anon().unwrap().make_read_only().unwrap();
    let mut header: RawHeader = IndexMap::new();
    header.insert("tensor1".to_string(), RawTensorMetaData {
        shape: vec![1, 1],
        dtype: "F32".to_string(),
        data_offsets: (0, 4),
        extra: IndexMap::new(),
    });
    let header_len = 128;
    let safetensor_file = SafetensorFile::new(mmap, header, header_len);
    let manifest = safetensor_file.process(true).unwrap();
    assert_eq!(manifest.tensors.len(), 1);
    assert!(manifest.tensors.contains_key("tensor1"));
    assert_eq!(manifest.tensors["tensor1"].index, 0);
}
