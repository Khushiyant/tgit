use crate::blobs;
use crate::errors::{Result, VektError};
use crate::utils::{ensure_vekt_dir, find_vekt_root, write_file_atomic};
use crate::validation::{validate_tensor_name, verify_blob_hash};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;

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
pub struct VektManifest {
    // Fix Issue #1: Deterministic serialization for Git diffs
    pub tensors: BTreeMap<String, ManifestTensor>,
    pub version: String,

    // Total size of all tensors in bytes
    pub total_size: usize,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct VektConfig {
    pub remotes: HashMap<String, String>,
}

impl VektManifest {
    /// Current manifest version
    pub const CURRENT_VERSION: &'static str = "1.0";

    /// Validates and migrates manifest to current version if needed
    pub fn validate_and_migrate(self) -> Result<Self> {
        match self.version.as_str() {
            "1.0" => Ok(self),
            // Future versions would be handled here
            // "2.0" => self.migrate_from_v2_to_current(),
            unknown => Err(VektError::InvalidManifest(format!(
                "Unsupported manifest version '{}'. Current version is '{}'. Please update vekt.",
                unknown,
                Self::CURRENT_VERSION
            ))),
        }
    }

    pub fn print_summary(&self) {
        println!("vekt Manifest Summary:");
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

    pub fn restore(&self, output_path: &std::path::Path, filter: Option<&str>) -> Result<()> {
        // Validate all tensor names before processing to prevent path traversal
        for name in self.tensors.keys() {
            validate_tensor_name(name)?;
        }

        let file = File::create(output_path)?;
        let mut writer = std::io::BufWriter::new(file);

        // Filter tensors
        let mut sorted_tensor_names: Vec<&String> = self
            .tensors
            .keys()
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

            let padding = (8 - (current_offset % 8)) % 8;
            current_offset += padding;

            let size = tensor.shape.iter().product::<usize>()
                * crate::utils::get_dtype_size(&tensor.dtype);
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

            // Use centralized blob path resolution
            let blob_path = blobs::get_blob_path(&tensor.hash);
            if !blob_path.exists() {
                return Err(VektError::BlobNotFound(format!(
                    "Blob {} not found for tensor '{}'",
                    tensor.hash, name
                )));
            }

            // CRITICAL: Verify blob hash to detect corruption
            let blob_data = std::fs::read(&blob_path).map_err(|e| {
                VektError::Io(std::io::Error::other(format!(
                    "Failed to read blob {}: {}",
                    tensor.hash, e
                )))
            })?;

            verify_blob_hash(&blob_data, &tensor.hash)?;

            // Write verified blob data
            writer.write_all(&blob_data)?;
            current_write_pos += blob_data.len();
            written_hashes.insert(tensor.hash.clone(), (0, 0)); // Value irrelevant, just marking as written
        }

        writer.flush()?;

        Ok(())
    }
}

impl VektConfig {
    pub fn load() -> Result<Self> {
        let root = find_vekt_root().ok_or(VektError::RepoNotFound)?;
        let path = root.join(".vekt").join("config.json");
        if !path.exists() {
            return Ok(VektConfig::default());
        }
        let file = File::open(&path).map_err(|e| {
            VektError::Io(std::io::Error::other(format!(
                "Failed to open config file at {}: {}",
                path.display(),
                e
            )))
        })?;
        let reader = std::io::BufReader::new(file);
        let config = serde_json::from_reader(reader).map_err(|e| {
            VektError::InvalidManifest(format!(
                "Failed to parse config file at {}: {}",
                path.display(),
                e
            ))
        })?;
        Ok(config)
    }

    pub fn save(&self) -> Result<()> {
        let dir = std::env::current_dir()?.join(".vekt");
        ensure_vekt_dir(&dir)?;
        let config_path = dir.join("config.json");
        let json = serde_json::to_string_pretty(self)?;
        write_file_atomic(&config_path, json.as_bytes()).map_err(|e| {
            VektError::Io(std::io::Error::other(format!(
                "Failed to write config file: {}",
                e
            )))
        })?;
        Ok(())
    }

    pub fn add_remote(&mut self, name: String, url: String) {
        self.remotes.insert(name, url);
    }
}
