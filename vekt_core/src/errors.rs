use thiserror::Error;

#[derive(Error, Debug)]
pub enum VektError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Repository not found. Run 'vekt init' first")]
    RepoNotFound,

    #[error("Repository already exists at this location")]
    RepoAlreadyExists,

    #[error("Lock file exists. Another vekt operation is in progress")]
    LockExists,

    #[error("Invalid safetensors file: {0}")]
    InvalidSafetensor(String),

    #[error("Tensor corruption detected: {0}")]
    TensorCorruption(String),

    #[error("Blob not found: {0}")]
    BlobNotFound(String),

    #[error("Hash mismatch: expected {expected}, got {actual}")]
    HashMismatch { expected: String, actual: String },

    #[error("Remote error: {0}")]
    RemoteError(String),

    #[error("Invalid remote URL: {0}")]
    InvalidRemoteUrl(String),

    #[error("Remote not found: {0}")]
    RemoteNotFound(String),

    #[error("Compression error: {0}")]
    CompressionError(String),

    #[error("Decompression error: {0}")]
    DecompressionError(String),

    #[error("Invalid manifest: {0}")]
    InvalidManifest(String),

    #[error("Path traversal detected: {0}")]
    PathTraversal(String),

    #[error("Invalid tensor name: {0}")]
    InvalidTensorName(String),

    #[error("Stale lock file detected. Process may have crashed. Lock age: {0} seconds")]
    StaleLock(u64),

    #[error("Credential validation failed: {0}")]
    CredentialError(String),

    #[error("Conflict detected: {0}")]
    ConflictDetected(String),

    #[error("Manifest corrupted: {0}")]
    ManifestCorrupted(String),

    #[error("Git operation failed: {0}")]
    GitError(String),

    #[error("Operation cancelled: {0}")]
    Cancelled(String),
}

pub type Result<T> = std::result::Result<T, VektError>;
