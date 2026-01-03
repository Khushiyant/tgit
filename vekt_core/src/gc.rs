use crate::errors::{Result, VektError};
use crate::storage::VektManifest;
use crate::utils::{LockFile, get_store_path};
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, Read};
use std::path::Path;

pub struct GcStats {
    pub deleted: usize,
    pub kept: usize,
}

pub fn run_gc(root_path: &Path) -> Result<GcStats> {
    // CRITICAL: Acquire lock for entire GC operation to prevent race conditions
    // This ensures no other vekt operations can modify manifests or blobs during GC
    let _lock = LockFile::lock()?;

    let store_path = get_store_path();
    if !store_path.exists() {
        return Ok(GcStats {
            deleted: 0,
            kept: 0,
        });
    }

    let mut referenced_hashes = HashSet::new();

    // Scan working tree
    scan_manifests(root_path, &mut referenced_hashes)?;

    // Scan git history
    scan_git_history(root_path, &mut referenced_hashes)?;

    let mut stats = GcStats {
        deleted: 0,
        kept: 0,
    };

    for entry in std::fs::read_dir(&store_path)? {
        let entry = entry?;
        let path = entry.path();
        if let Some(hash) = path.file_name().and_then(|n| n.to_str()) {
            // Skip temporary files
            if hash.ends_with(".tmp") {
                continue;
            }
            if !referenced_hashes.contains(hash) {
                std::fs::remove_file(&path)?;
                stats.deleted += 1;
            } else {
                stats.kept += 1;
            }
        }
    }

    Ok(stats)
}

fn scan_manifests(dir: &Path, hashes: &mut HashSet<String>) -> Result<()> {
    if dir.is_dir() {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                let name = path.file_name().unwrap_or_default().to_string_lossy();
                // Ignore common build/hidden dirs to avoid scanning too much or loops
                if name == ".git" || name == ".vekt" || name == "target" || name == "node_modules" {
                    continue;
                }
                scan_manifests(&path, hashes)?;
            } else if let Some(name) = path.file_name().and_then(|n| n.to_str())
                && name.ends_with(".vekt.json")
            {
                let f = File::open(&path)?;
                let reader = std::io::BufReader::new(f);
                match serde_json::from_reader::<_, VektManifest>(reader) {
                    Ok(manifest) => {
                        for tensor in manifest.tensors.values() {
                            hashes.insert(tensor.hash.clone());
                        }
                    }
                    Err(e) => {
                        // Log corrupted manifests but continue GC
                        // This prevents partial failures from blocking cleanup
                        eprintln!(
                            "Warning: Failed to parse manifest at {}: {}. Skipping this manifest.",
                            path.display(),
                            e
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

fn scan_git_history(repo_root: &Path, hashes: &mut HashSet<String>) -> Result<()> {
    // Check if this is a git repository
    let git_dir = repo_root.join(".git");
    if !git_dir.exists() {
        return Ok(());
    }

    // Use git rev-list with --all and --reflog to catch unreachable commits
    // --reflog includes commits that may have been deleted/force-pushed
    let rev_list_output = std::process::Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .arg("rev-list")
        .arg("--all")
        .arg("--reflog")
        .arg("--objects")
        .output()
        .map_err(|e| VektError::GitError(format!("Failed to run git rev-list: {}", e)))?;

    if !rev_list_output.status.success() {
        let stderr = String::from_utf8_lossy(&rev_list_output.stderr);
        eprintln!(
            "Warning: git rev-list failed: {}. Skipping git history scan.",
            stderr
        );
        return Ok(());
    }

    let objects = String::from_utf8_lossy(&rev_list_output.stdout);

    // Collect all object SHAs that are .vekt.json files
    let mut manifest_objects = Vec::new();
    for line in objects.lines() {
        // Format: "<sha> <path>" or just "<sha>" for commits
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() == 2 {
            let (sha, path) = (parts[0], parts[1]);
            if path.ends_with(".vekt.json") {
                manifest_objects.push(sha.to_string());
            }
        }
    }

    if manifest_objects.is_empty() {
        return Ok(());
    }

    // Use git cat-file --batch for efficient streaming of multiple objects
    let mut cat_file = std::process::Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .arg("cat-file")
        .arg("--batch")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| VektError::GitError(format!("Failed to spawn git cat-file: {}", e)))?;

    // Write all SHAs to stdin
    if let Some(mut stdin) = cat_file.stdin.take() {
        use std::io::Write;
        for sha in &manifest_objects {
            if let Err(e) = writeln!(stdin, "{}", sha) {
                eprintln!("Warning: Failed to write SHA to git cat-file: {}", e);
                break;
            }
        }
        // Close stdin to signal we're done
        drop(stdin);
    }

    // Read the batched output
    if let Some(stdout) = cat_file.stdout.take() {
        let mut reader = std::io::BufReader::new(stdout);

        loop {
            // Read header line: "<sha> <type> <size>"
            let mut header_line = String::new();
            let bytes_read = reader.read_line(&mut header_line).map_err(|e| {
                VektError::GitError(format!("Failed to read git cat-file output: {}", e))
            })?;
            if bytes_read == 0 {
                break; // EOF
            }

            let parts: Vec<&str> = header_line.split_whitespace().collect();
            if parts.len() != 3 {
                // Handle missing objects gracefully
                if header_line.contains("missing") {
                    // Skip the trailing newline for missing objects
                    let mut newline = [0u8; 1];
                    let _ = reader.read_exact(&mut newline);
                }
                continue;
            }

            let size: usize = match parts[2].parse() {
                Ok(s) => s,
                Err(_) => {
                    eprintln!("Warning: Invalid size in git cat-file output: {}", parts[2]);
                    continue;
                }
            };

            // Read exactly 'size' bytes (the actual file content)
            let mut content = vec![0u8; size];
            if let Err(e) = reader.read_exact(&mut content) {
                eprintln!("Warning: Failed to read git object content: {}", e);
                break;
            }

            // Read the trailing newline that git cat-file adds after each object
            let mut newline = [0u8; 1];
            let _ = reader.read_exact(&mut newline);

            // Try to parse as manifest - handle encoding issues gracefully
            match serde_json::from_slice::<VektManifest>(&content) {
                Ok(manifest) => {
                    for tensor in manifest.tensors.values() {
                        hashes.insert(tensor.hash.clone());
                    }
                }
                Err(e) => {
                    // Log but don't fail - file might be corrupted or not valid JSON
                    eprintln!("Warning: Failed to parse git object as manifest: {}", e);
                }
            }
        }
    }

    let _ = cat_file.wait();
    Ok(())
}
