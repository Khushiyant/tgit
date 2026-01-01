pub mod storage;
pub mod utils;
pub mod remote;
pub mod errors;
pub mod validation;
pub mod compression;
pub mod diff;

use std::collections::BTreeMap;
use std::fs::{self, File};
use rayon::prelude::*;
use blake3;
use hex;
use memmap2::Mmap;
use std::io::Write;

use storage::{RawHeader, TGitManifest, ManifestTensor};

pub trait ModelArchiver {
    fn process(&self, save_blobs: bool) -> Result<TGitManifest, Box<dyn std::error::Error>>;
    fn restore(manifest: &TGitManifest, output_path: &std::path::Path, filter: Option<&str>) -> Result<(), Box<dyn std::error::Error>>;
}

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
}

impl ModelArchiver for SafetensorFile {
    fn process(&self, save_blobs: bool) -> Result<TGitManifest, Box<dyn std::error::Error>> {

        let store_path = utils::get_store_path();
        let header_entries: Vec<(usize, &String, &storage::RawTensorMetaData)> = self.header.iter()
            .enumerate()
            .map(|(i, (k, v))| (i, k, v))
            .collect();

        let results: BTreeMap<String, ManifestTensor> = header_entries
            .par_iter()
            .filter_map(
                |(index, tensor_name, tensor_meta)| {
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
                        (*tensor_name).clone(),
                        ManifestTensor {
                            shape: tensor_meta.shape.clone(),
                            dtype: tensor_meta.dtype.clone(),
                            hash: hash_hex,
                            extra: tensor_meta.extra.clone(),
                            index: *index,
                        },
                    ))
                },
            )
            .collect();
        Ok(TGitManifest {
            tensors: results,
            version: "1.0".to_string(),
            total_size: self.mmap.len(),
        })
    }
    fn restore(manifest: &TGitManifest, output_path: &std::path::Path, filter: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
        // Delegate to the existing restore logic in TGitManifest
        // In a real multi-format system, this logic would likely live here or in a Safetensors-specific module
        manifest.restore(output_path, filter)
    }
}
