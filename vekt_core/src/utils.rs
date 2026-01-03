use crate::errors::{Result, VektError};
use std::fs::{self};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Atomically writes data to a file using temp file + rename pattern
pub fn write_file_atomic(path: &Path, data: &[u8]) -> io::Result<()> {
    let tmp_path = path.with_extension("tmp");
    let mut f = fs::File::create(&tmp_path)?;
    f.write_all(data)?;
    f.sync_all()?;
    fs::rename(tmp_path, path)?;
    Ok(())
}

/// Ensures .vekt directory exists with proper .gitignore file
pub fn ensure_vekt_dir(vekt_path: &Path) -> io::Result<()> {
    if !vekt_path.exists() {
        fs::create_dir_all(vekt_path)?;
        // Create .gitignore to ignore everything in .vekt
        let gitignore_path = vekt_path.join(".gitignore");
        if !gitignore_path.exists() {
            fs::write(gitignore_path, "*\n")?;
        }
    }
    Ok(())
}

/// Locates the root .vekt directory by traversing up from the current directory.
/// Returns the path containing .vekt (e.g., /path/to/repo).
pub fn find_vekt_root() -> Option<PathBuf> {
    if let Ok(root) = std::env::var("VEKT_ROOT") {
        return Some(PathBuf::from(root));
    }
    let mut current = std::env::current_dir().ok()?;
    loop {
        let vekt_path = current.join(".vekt");
        if vekt_path.exists() && vekt_path.is_dir() {
            return Some(current);
        }
        if !current.pop() {
            break;
        }
    }
    None
}

/// Returns the path to the blobs directory.
/// Uses the local repository's .vekt/blobs if found, otherwise defaults to ./.vekt/blobs
/// Also ensures .vekt has a .gitignore file
pub fn get_store_path() -> PathBuf {
    let vekt_dir = match find_vekt_root() {
        Some(root) => root.join(".vekt"),
        None => std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(".vekt"),
    };

    // Ensure .vekt has .gitignore (ignore errors as this is best-effort)
    let _ = ensure_vekt_dir(&vekt_dir);

    vekt_dir.join("blobs")
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
    /// Maximum age of lock file before considering it stale (5 minutes)
    const STALE_LOCK_THRESHOLD_SECS: u64 = 300;

    pub fn lock() -> Result<Self> {
        // Use the found root or current dir for locking
        let root = find_vekt_root()
            .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
        let vekt_dir = root.join(".vekt");
        let path = vekt_dir.join("lock");

        // Ensure .vekt exists with .gitignore
        ensure_vekt_dir(&vekt_dir).map_err(|e| {
            VektError::Io(io::Error::other(format!(
                "Failed to create .vekt directory: {}",
                e
            )))
        })?;

        // Check for stale lock
        if path.exists()
            && let Ok(metadata) = fs::metadata(&path)
            && let Ok(modified) = metadata.modified()
            && let Ok(duration) = SystemTime::now().duration_since(modified)
        {
            let age_secs = duration.as_secs();
            if age_secs > Self::STALE_LOCK_THRESHOLD_SECS {
                // Remove stale lock
                eprintln!(
                    "Warning: Removing stale lock file (age: {} seconds). Previous process may have crashed.",
                    age_secs
                );
                fs::remove_file(&path).map_err(|e| {
                    VektError::Io(io::Error::other(format!(
                        "Failed to remove stale lock: {}",
                        e
                    )))
                })?;
            } else {
                return Err(VektError::LockExists);
            }
        }

        // Try to create the file atomically with PID
        let pid = std::process::id();
        let lock_content = format!(
            "{}\n{}",
            pid,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );

        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|_| VektError::LockExists)?;

        // Write PID to lock file
        fs::write(&path, lock_content).map_err(|e| {
            VektError::Io(io::Error::other(format!(
                "Failed to write lock file: {}",
                e
            )))
        })?;

        Ok(LockFile { path })
    }
}

impl Drop for LockFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}
