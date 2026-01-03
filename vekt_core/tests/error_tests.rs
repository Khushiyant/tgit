use std::fs::File;
use std::io::Write;
use vekt_core::SafetensorFile;
use vekt_core::errors::VektError;

#[test]
fn test_open_too_small_file() -> Result<(), Box<dyn std::error::Error>> {
    let path = "test_too_small.safetensors";
    {
        let mut file = File::create(path)?;
        file.write_all(b"12345")?; // Only 5 bytes
    }

    let result = SafetensorFile::open(path);

    match result {
        Err(VektError::InvalidSafetensor(msg)) => {
            assert_eq!(msg, "File too small");
        }
        Err(e) => panic!("Expected InvalidSafetensor, got {:?}", e),
        Ok(_) => panic!("Expected error, got Ok"),
    }

    std::fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_open_invalid_header_len() -> Result<(), Box<dyn std::error::Error>> {
    let path = "test_invalid_header.safetensors";
    {
        let mut file = File::create(path)?;
        // Write huge header length
        let len: u64 = 1_000_000_000;
        file.write_all(&len.to_le_bytes())?;
        // Write a bit of data, but not enough for header
        file.write_all(b"{}")?;
    }

    let result = SafetensorFile::open(path);

    match result {
        Err(VektError::InvalidSafetensor(msg)) => {
            assert_eq!(msg, "Header length exceeds file size");
        }
        Err(e) => panic!("Expected InvalidSafetensor, got {:?}", e),
        Ok(_) => panic!("Expected error, got Ok"),
    }

    std::fs::remove_file(path)?;
    Ok(())
}
