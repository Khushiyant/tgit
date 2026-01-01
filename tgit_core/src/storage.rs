use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use crate::utils::get_store_path;

// Metadata for a single tensor in raw format in safetensor file
#[derive(Serialize, Deserialize, Debug)]
pub struct RawTensorMetaData {
    pub shape: Vec<usize>,
    pub dtype: String,
    pub data_offsets: (usize, usize),
}
// Header for safetensor file in raw format
pub type RawHeader = HashMap<String, RawTensorMetaData>;


#[derive(Serialize, Deserialize, Debug)]
pub struct ManifestTensor {
    pub shape: Vec<usize>,
    pub dtype: String,
    pub hash: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TGitManifest {
    pub tensors: HashMap<String, ManifestTensor>,
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
        for (name, tensor) in &self.tensors {
            println!(
                "- {}: shape={:?}, dtype={}, hash={}",
                name, tensor.shape, tensor.dtype, tensor.hash
            );
        }
    }

    pub fn restore(&self, output_path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
        let store_path = get_store_path();

        let file = File::create(output_path)?;
        let mut writer = std::io::BufWriter::new(file);

        let mut sorted_tensor_names: Vec<&String> = self.tensors.keys().collect();
        sorted_tensor_names.sort();

        let mut header_map: RawHeader = HashMap::new();
        let mut current_offset = 0;

        for name in &sorted_tensor_names {
            let tensor = &self.tensors[*name];
            let size = tensor.shape.iter().product::<usize>() * crate::utils::get_dtype_size(&tensor.dtype);

            let meta = RawTensorMetaData {
                shape: tensor.shape.clone(),
                dtype: tensor.dtype.clone(),
                data_offsets: (current_offset, current_offset + size),
            };
            header_map.insert((*name).clone(), meta);
            current_offset += size;
        }

        let header_json = serde_json::to_string(&header_map)?;
        let header_len = header_json.len() as u64;
        let header_bytes = header_json.as_bytes();

        writer.write_all(&header_len.to_le_bytes())?;
        writer.write_all(header_bytes)?;

        for name in &sorted_tensor_names {
            let tensor = &self.tensors[*name];
            let blob_path = store_path.join(&tensor.hash);
            let mut blob_file = File::open(blob_path)?;
            std::io::copy(&mut blob_file, &mut writer)?;
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


#[cfg(test)]
mod tests {
    use std::io::Read as _;

    use super::*;


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

        let file = crate::SafetensorFile::open(original_path)?;
        let manifest = file.process(true); // true = save blobs

        std::fs::remove_file(original_path)?;

        manifest.restore(std::path::Path::new(restored_path))?;

        let mut f = File::open(restored_path)?;
        let mut buffer = Vec::new();
        f.read_to_end(&mut buffer)?;
        
        // Size = 8 (len) + 79 (header) + 4 (data) = 91 bytes
        assert!(buffer.len() > 70); 
        
        // Clean up
        std::fs::remove_file(restored_path)?;
        
        Ok(())
    }
}