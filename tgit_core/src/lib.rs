pub mod storage;
pub mod utils;

use std::collections::HashMap;
use std::fs::{self, File};
use rayon::prelude::*;
use blake3;
use hex;
use memmap2::Mmap;
use std::io::Write;

use storage::{RawHeader, TGitManifest, ManifestTensor};

pub struct SafetensorFile {
    pub header: RawHeader,
    pub mmap: Mmap,
    
    pub header_len: usize,
}

impl SafetensorFile {
    pub fn new(mmap: Mmap, header: RawHeader, header_len: usize) -> Self {
        SafetensorFile {
            header,
            mmap,
            header_len,
        }
    }
    pub fn open(path: &str) -> std::io::Result<Self> {
        // Open the file and create a memory-mapped buffer
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };

        // Read the header length (first 8 bytes)
        let header_len_bytes = &mmap[0..8];
        let header_len = usize::from_le_bytes(header_len_bytes.try_into().unwrap());

        // Read the header JSON
        let header_json_bytes = &mmap[8..8 + header_len];
        let header_json_str = std::str::from_utf8(header_json_bytes).unwrap();
        let header: RawHeader = serde_json::from_str(header_json_str).unwrap();

        Ok(SafetensorFile::new(mmap, header, header_len))
    }

    pub fn process(&self, save_blobs: bool) -> TGitManifest {

        let store_path = utils::get_store_path();
        let results: HashMap<String, ManifestTensor> = self.header
            .par_iter()
            .filter_map(
                |(tensor_name, tensor_meta)| {
                    let (start, end) = tensor_meta.data_offsets;
                    let absolute_start = self.header_len + 8 + start;
                    let absolute_end = self.header_len + 8 + end;

                
                    if absolute_end > self.mmap.len() {
                        eprintln!(
                            "Corrupt Tensor '{}': Ends at byte {}, but file is only {} bytes. Skipping.",
                            tensor_name, absolute_end, self.mmap.len()
                        );
                        return None;
                    }
                    let data_slice = &self.mmap[absolute_start..absolute_end];
                    let hash = blake3::hash(data_slice);
                    let hash_hex = hex::encode(hash.as_bytes());


                    if save_blobs {
                        let blob_path = store_path.join(&hash_hex);
                        // Only write if it doesn't exist (Deduplication!)
                        if !blob_path.exists() {
                            // We use a temporary file + rename for atomic writes (crash safety)
                            let tmp_path = blob_path.with_extension("tmp");
                            if let Ok(mut f) = File::create(&tmp_path) {
                                f.write_all(data_slice).unwrap();
                                fs::rename(tmp_path, blob_path).unwrap();
                            }
                        }
                    }

                    Some((
                        tensor_name.clone(),
                        ManifestTensor {
                            shape: tensor_meta.shape.clone(),
                            dtype: tensor_meta.dtype.clone(),
                            hash: hash_hex,
                        },
                    ))
                },
            )
            .collect();

        TGitManifest {
            tensors: results,
            version: "1.0".to_string(),
            total_size: self.mmap.len(),
        }
    }

}


#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use memmap2::MmapOptions;
    use storage::RawTensorMetaData;

    #[test]
    fn test_safetensor_new() {
        let mmap = MmapOptions::new().len(1024).map_anon().unwrap().make_read_only().unwrap();
        let header: RawHeader = HashMap::new();
        let header_len = 128;

        let safetensor_file = SafetensorFile::new(mmap, header, header_len);

        assert_eq!(safetensor_file.header_len, header_len);
    }

    #[test]
    fn test_safetensor_open() -> Result<(), Box<dyn std::error::Error>> {
        let path = "test_model.safetensors";
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
        let mut header: RawHeader = HashMap::new();
        header.insert("tensor1".to_string(), RawTensorMetaData {
            shape: vec![1, 1],
            dtype: "F32".to_string(),
            data_offsets: (0, 4),
        });
        let header_len = 128;
        let safetensor_file = SafetensorFile::new(mmap, header, header_len);
        let manifest = safetensor_file.process(true);
        assert_eq!(manifest.tensors.len(), 1);
        assert!(manifest.tensors.contains_key("tensor1"));
    }
}