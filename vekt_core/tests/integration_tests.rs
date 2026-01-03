use std::env;
use std::fs;
use std::path::PathBuf;

use vekt_core::ModelArchiver;
use vekt_core::SafetensorFile;
use vekt_core::gc;

fn setup_test_env() -> (PathBuf, PathBuf) {
    let mut dir = env::temp_dir();
    dir.push("vekt_test_run");
    let rnd: u64 = rand::random();
    dir.push(format!("{}", rnd)); // distinct dir
    fs::create_dir_all(&dir).unwrap();

    let root = dir.clone();
    unsafe {
        env::set_var("VEKT_ROOT", &root);
    }

    // Create .vekt structure
    let vekt_dir = root.join(".vekt");
    fs::create_dir(&vekt_dir).unwrap();
    fs::create_dir(vekt_dir.join("blobs")).unwrap();

    (root, vekt_dir)
}

fn cleanup(root: PathBuf) {
    let _ = fs::remove_dir_all(root);
    unsafe {
        env::remove_var("VEKT_ROOT");
    }
}

#[test]
fn test_full_workflow() {
    let (root, _vekt_dir) = setup_test_env();

    // Create dummy safetensors
    let model_path = root.join("model.safetensors");
    {
        // Minimal valid safetensors file
        // Header: {"t":...}
        let mut f = fs::File::create(&model_path).unwrap();
        // 8 bytes length
        let header_json = r#"{"t": {"dtype":"F32","shape":[1],"data_offsets":[0,4]}}"#;
        let len = header_json.len() as u64;
        use std::io::Write;
        f.write_all(&len.to_le_bytes()).unwrap();
        f.write_all(header_json.as_bytes()).unwrap();
        f.write_all(&[0u8, 1u8, 2u8, 3u8]).unwrap(); // 4 bytes data
    }

    // 1. Process (Archive)
    let file = SafetensorFile::open(model_path.to_str().unwrap()).expect("Failed to open");
    let manifest = file.process(true).expect("Failed to process");

    // Verify blob exists
    let hash = &manifest.tensors["t"].hash;
    let blob_path = root.join(".vekt").join("blobs").join(hash);
    assert!(blob_path.exists(), "Blob should be created");

    // Save manifest to simulate "vekt add"
    let manifest_path = root.join("model.vekt.json");
    let f = fs::File::create(&manifest_path).unwrap();
    serde_json::to_writer(f, &manifest).unwrap();

    // 2. Restore
    let restore_path = root.join("restored.safetensors");
    vekt_core::SafetensorFile::restore(&manifest, &restore_path, None).expect("Restore failed");

    assert!(restore_path.exists());

    // Verify restored content
    // We can't do simple byte comparison because JSON header key order might differ.
    // We verify the tensor data is correct.
    let restored_file =
        SafetensorFile::open(restore_path.to_str().unwrap()).expect("Failed to open restored");
    let meta = restored_file.header.get("t").expect("Tensor t missing");
    let (start, end) = meta.data_offsets;
    let absolute_start = restored_file.header_len + 8 + start;
    let absolute_end = restored_file.header_len + 8 + end;
    let restored_data = &restored_file.mmap[absolute_start..absolute_end];

    let expected_data = &[0u8, 1u8, 2u8, 3u8];
    assert_eq!(
        restored_data, expected_data,
        "Restored tensor data mismatch"
    );

    // 3. GC
    // If we delete manifest, GC should remove blob
    fs::remove_file(manifest_path).unwrap();

    let stats = gc::run_gc(&root).expect("GC failed");
    assert_eq!(stats.deleted, 1, "GC should delete 1 blob");
    assert!(!blob_path.exists(), "Blob should be gone");

    cleanup(root);
}
