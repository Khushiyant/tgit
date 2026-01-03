pub mod blobs;
pub mod compression;
pub mod diff;
pub mod errors;
pub mod gc;
pub mod remote;
pub mod storage;
pub mod utils;
pub mod validation;

use crate::errors::{Result, VektError};
use memmap2::Mmap;
use rayon::prelude::*;
use std::collections::BTreeMap;

use storage::{ManifestTensor, RawHeader, VektManifest};

pub trait ModelArchiver {
    fn process(&self, save_blobs: bool) -> Result<VektManifest>;
    fn restore(
        manifest: &VektManifest,
        output_path: &std::path::Path,
        filter: Option<&str>,
    ) -> Result<()>;
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
    pub fn open(path: &str) -> Result<Self> {
        // Open the file and create a memory-mapped buffer
        use std::fs::File;
        let file = File::open(path)?;

        if file.metadata()?.len() < 8 {
            return Err(VektError::InvalidSafetensor("File too small".to_string()));
        }

        // Unsafe: Mmap assumes file doesn't change underneath.
        // Standard in this domain (huggingface/safetensors does this).
        let mmap = unsafe { Mmap::map(&file)? };

        // Read the header length (first 8 bytes)
        let header_len_bytes = &mmap[0..8];
        let header_len = usize::from_le_bytes(
            header_len_bytes
                .try_into()
                .map_err(|_| VektError::InvalidSafetensor("Invalid header length".to_string()))?,
        );

        // Read the header JSON
        if 8 + header_len > mmap.len() {
            return Err(VektError::InvalidSafetensor(
                "Header length exceeds file size".to_string(),
            ));
        }

        let header_json_bytes = &mmap[8..8 + header_len];
        let header_json_str = std::str::from_utf8(header_json_bytes).map_err(|e| {
            VektError::InvalidSafetensor(format!("Header is not valid UTF-8: {}", e))
        })?;
        let header: RawHeader = serde_json::from_str(header_json_str)
            .map_err(|e| VektError::InvalidSafetensor(format!("Invalid header JSON: {}", e)))?;

        Ok(SafetensorFile::new(mmap, header, header_len))
    }
}

impl ModelArchiver for SafetensorFile {
    fn process(&self, save_blobs: bool) -> Result<VektManifest> {
        let header_entries: Vec<(usize, &String, &storage::RawTensorMetaData)> = self
            .header
            .iter()
            .enumerate()
            .map(|(i, (k, v))| (i, k, v))
            .collect();

        // Step 1: Compute hashes in parallel (CPU Bound)
        let processed_tensors: Vec<Result<(String, ManifestTensor, usize, usize)>> = header_entries
            .par_iter()
            .map(|(index, tensor_name, tensor_meta)| {
                let (start, end) = tensor_meta.data_offsets;
                let absolute_start = self.header_len + 8 + start;
                let absolute_end = self.header_len + 8 + end;

                if absolute_end > self.mmap.len() {
                    return Err(VektError::TensorCorruption(format!(
                        "Tensor '{}': Ends at byte {}, but file is only {} bytes.",
                        tensor_name,
                        absolute_end,
                        self.mmap.len()
                    )));
                }

                let data_slice = &self.mmap[absolute_start..absolute_end];
                let hash_hex = blobs::compute_blob_hash(data_slice);

                Ok((
                    (*tensor_name).clone(),
                    ManifestTensor {
                        shape: tensor_meta.shape.clone(),
                        dtype: tensor_meta.dtype.clone(),
                        hash: hash_hex,
                        extra: tensor_meta.extra.clone(),
                        index: *index,
                    },
                    absolute_start,
                    absolute_end,
                ))
            })
            .collect();

        // Collect results and fail fast on error
        let mut results = BTreeMap::new();
        let mut valid_entries = Vec::new();

        for res in processed_tensors {
            let (name, tensor, start, end) = res?;
            results.insert(name, tensor);
            valid_entries.push((start, end));
        }

        // Step 2: Save blobs (IO Bound)
        // We use try_for_each to handle errors, and par_iter to potentially parallelize IO
        // (though disk IO is often better serialized or throttled, rayon handles this reasonably well)
        if save_blobs {
            valid_entries
                .par_iter()
                .try_for_each(|(start, end)| -> Result<()> {
                    let data = &self.mmap[*start..*end];
                    match blobs::save_blob_deduplicated(data) {
                        Ok(_) => Ok(()),
                        Err(e) => Err(VektError::Io(e)),
                    }
                })?;
        }

        Ok(VektManifest {
            tensors: results,
            version: "1.0".to_string(),
            total_size: self.mmap.len(),
        })
    }

    fn restore(
        manifest: &VektManifest,
        output_path: &std::path::Path,
        filter: Option<&str>,
    ) -> Result<()> {
        manifest.restore(output_path, filter)
    }
}
