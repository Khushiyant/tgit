use std::path::PathBuf;
use std::fs::{self};
use std::io;

/// Locates the root .tgit directory by traversing up from the current directory.
/// Returns the path containing .tgit (e.g., /path/to/repo).
pub fn find_tgit_root() -> Option<PathBuf> {
    let mut current = std::env::current_dir().ok()?;
    loop {
        let tgit_path = current.join(".tgit");
        if tgit_path.exists() && tgit_path.is_dir() {
            return Some(current);
        }
        if !current.pop() {
            break;
        }
    }
    None
}

/// Returns the path to the blobs directory.
/// Uses the local repository's .tgit/blobs if found, otherwise defaults to ./.tgit/blobs
pub fn get_store_path() -> PathBuf {
    match find_tgit_root() {
        Some(root) => root.join(".tgit").join("blobs"),
        None => std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")).join(".tgit").join("blobs"),
    }
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
        // Use the found root or current dir for locking
        let root = find_tgit_root().unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
        let path = root.join(".tgit").join("lock");
        
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
