use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use crate::utils::get_store_path;
use indexmap::IndexMap;

// Metadata for a single tensor in raw format in safetensor file
#[derive(Serialize, Deserialize, Debug)]
pub struct RawTensorMetaData {
    pub shape: Vec<usize>,
    pub dtype: String,
    pub data_offsets: (usize, usize),

    #[serde(flatten)]
    pub extra: IndexMap<String, serde_json::Value>,
}
// Header for safetensor file in raw format
pub type RawHeader = IndexMap<String, RawTensorMetaData>;


#[derive(Serialize, Deserialize, Debug)]
pub struct ManifestTensor {
    pub shape: Vec<usize>,
    pub dtype: String,
    pub hash: String,
    // Fix Issue #4: Preserve physical layout order
    pub index: usize,

    #[serde(default)]
    pub extra: IndexMap<String, serde_json::Value>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TGitManifest {
    // Fix Issue #1: Deterministic serialization for Git diffs
    pub tensors: BTreeMap<String, ManifestTensor>,
    pub version: String,

    // Total size of all tensors in bytes
    pub total_size: usize,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct TGitConfig {
    pub remotes: HashMap<String, String>,
}



impl TGitManifest {
    pub fn print_summary(&self) {
        println!("TGit Manifest Summary:");
        println!("Version: {}", self.version);
        println!("Total Tensors: {}", self.tensors.len());
        println!("Total Size: {} bytes", self.total_size);
        println!("Tensors:");
        
        let mut sorted_tensors: Vec<(&String, &ManifestTensor)> = self.tensors.iter().collect();
        sorted_tensors.sort_by_key(|k| k.1.index);
        
        for (name, tensor) in sorted_tensors {
            println!(
                "- [{}] {}: shape={:?}, dtype={}, hash={}",
                tensor.index, name, tensor.shape, tensor.dtype, tensor.hash
            );
        }
    }

    pub fn restore(&self, output_path: &std::path::Path, filter: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
        let store_path = get_store_path();

        let file = File::create(output_path)?;
        let mut writer = std::io::BufWriter::new(file);

        // Filter tensors
        let mut sorted_tensor_names: Vec<&String> = self.tensors.keys()
            .filter(|name| {
                if let Some(f) = filter {
                    // Simple logic: keep if name contains any of the comma-separated terms
                    f.split(',').any(|term| name.contains(term.trim()))
                } else {
                    true
                }
            })
            .collect();
        
        // Fix Issue #4: Sort by original index to ensure deterministic restoration
        sorted_tensor_names.sort_by_key(|name| self.tensors[*name].index);

        let mut header_map: RawHeader = IndexMap::new();
        let mut current_offset = 0;

        // Issue #2: Track written hashes to handle shared weights
        // Hash -> (start_offset, end_offset)
        let mut written_hashes: HashMap<String, (usize, usize)> = HashMap::new();

        // Pass 1: Build the Header (calculate offsets with alignment)
        for name in &sorted_tensor_names {
            let tensor = &self.tensors[*name];

            // Shared Weights Deduplication
            if let Some(&(start, end)) = written_hashes.get(&tensor.hash) {
                let meta = RawTensorMetaData {
                    shape: tensor.shape.clone(),
                    dtype: tensor.dtype.clone(),
                    data_offsets: (start, end),
                    extra: tensor.extra.clone(),
                };
                header_map.insert((*name).clone(), meta);
                continue;
            }

            // Issue #3: 8-byte Alignment
            let padding = (8 - (current_offset % 8)) % 8;
            current_offset += padding;

            let size = tensor.shape.iter().product::<usize>() * crate::utils::get_dtype_size(&tensor.dtype);
            let start = current_offset;
            let end = current_offset + size;

            let meta = RawTensorMetaData {
                shape: tensor.shape.clone(),
                dtype: tensor.dtype.clone(),
                data_offsets: (start, end),
                extra: tensor.extra.clone(),
            };
            header_map.insert((*name).clone(), meta);

            written_hashes.insert(tensor.hash.clone(), (start, end));
            current_offset += size;
        }

        let header_json = serde_json::to_string(&header_map)?;
        let header_len = header_json.len() as u64;
        let header_bytes = header_json.as_bytes();

        writer.write_all(&header_len.to_le_bytes())?;
        writer.write_all(header_bytes)?;

        // Pass 2: Write Data (with alignment padding and deduplication)
        written_hashes.clear(); // Reset to track what we have effectively written in this pass
        let mut current_write_pos = 0;

        for name in &sorted_tensor_names {
            let tensor = &self.tensors[*name];

            if written_hashes.contains_key(&tensor.hash) {
                // Data already written for this hash
                continue;
            }

            // Add Padding
            let padding = (8 - (current_write_pos % 8)) % 8;
            if padding > 0 {
                let zeros = vec![0u8; padding];
                writer.write_all(&zeros)?;
                current_write_pos += padding;
            }

            let blob_path = store_path.join(&tensor.hash);
            let mut blob_file = File::open(blob_path)?;
            let bytes_copied = std::io::copy(&mut blob_file, &mut writer)?;
            
            current_write_pos += bytes_copied as usize;
            written_hashes.insert(tensor.hash.clone(), (0, 0)); // Value irrelevant, just marking as written
        }

        writer.flush()?;

        Ok(())
    }
}

impl TGitConfig {
    pub fn load() -> Result<Self, Box<dyn std::error::Error>> {
        let path = std::env::current_dir()?.join(".tgit").join("config.json");
        if !path.exists() {
            return Ok(TGitConfig::default());
        }
        let file = File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let config = serde_json::from_reader(reader)?;
        Ok(config)
    }

    pub fn save(&self) -> Result<(), Box<dyn std::error::Error>> {
        let dir = std::env::current_dir()?.join(".tgit");
        if !dir.exists() {
            std::fs::create_dir_all(&dir)?;
        }
        let file = File::create(dir.join("config.json"))?;
        serde_json::to_writer_pretty(file, self)?;
        Ok(())
    }
    
    pub fn add_remote(&mut self, name: String, url: String) {
        self.remotes.insert(name, url);
    }
}