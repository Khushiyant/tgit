use std::fs;
use std::path::PathBuf;
use std::fs::File;
use std::io::Write;

use std::env;

pub fn get_store_path() -> PathBuf {
    let current_dir = env::current_dir().expect("Failed to get current directory");
    let store_dir = current_dir.join(".tgit").join("blobs");
    
    if !store_dir.exists() {
        fs::create_dir_all(&store_dir).expect("Failed to create .tgit/blobs");
        
        let gitignore = current_dir.join(".tgit").join(".gitignore");
        if !gitignore.exists() {
             let mut f = File::create(gitignore).unwrap();
             writeln!(f, "*").unwrap(); 
        }
    }
    
    store_dir
}

pub fn get_dtype_size(dtype: &str) -> usize {
    match dtype {
        "F32" | "I32" => 4,
        "F16" | "I16" | "BF16" => 2,
        "F64" | "I64" => 8,
        "I8" | "U8" | "BOOL" => 1,
        _ => panic!("Unsupported dtype: {}", dtype), // For now, panic is fine
    }
}