use std::path::PathBuf;
use std::fs::{self};
use std::io;

pub fn get_store_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    std::path::Path::new(&home).join(".tgit").join("blobs")
}

pub fn get_dtype_size(dtype: &str) -> usize {
    match dtype {
        "F32" => 4,
        "F16" => 2,
        "BF16" => 2,
        "I64" => 8,
        "I32" => 4,
        "I16" => 2,
        "I8" => 1,
        "U8" => 1,
        "BOOL" => 1,
        _ => 1, // Fallback
    }
}

pub struct LockFile {
    path: PathBuf,
}

impl LockFile {
    pub fn lock() -> Result<Self, io::Error> {
        let path = std::env::current_dir()?.join(".tgit").join("lock");
        
        // Ensure .tgit exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Try to create the file atomically. fails if exists.
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|_| io::Error::new(io::ErrorKind::AlreadyExists, "TGit is currently locked by another process."))?;

        Ok(LockFile { path })
    }
}

impl Drop for LockFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}
