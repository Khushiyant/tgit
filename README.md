# vekt

A semantic engine for machine learning models.

## The Problem

ML models are huge and mostly redundant across versions. You fine-tune one layer, traditional storage duplicates everything. That's wasteful.

## What is vekt?

vekt is a semantic engine that decomposes safetensors models into individual tensors, stores each exactly once, and reconstructs them from lightweight JSON manifests.

When two models share layers, those layers occupy storage space once. The models themselves are just manifests pointing to shared tensor blobs.

Built on content-addressable storage (CAS), like Git's object database, but purpose-built for ML model structure and scale.

## Why vekt?

**Deduplication** - Store shared layers once. Fine-tune BERT ten ways, store the encoder once.

**Tensor-level granularity** - Restore specific layers. Work with exactly what you need.

**Remote sync** - Share tensor blobs via S3. Track tiny manifests in Git.

**Garbage collection** - Automatically reclaim space from orphaned blobs.

**Semantic diffing** - See which specific tensors changed, not just "files differ."

## Installation

Requires Rust 2024 edition or later.

```bash
git clone https://github.com/Khushiyant/vekt.git
cd vekt
cargo build --release
```

The binary will be available at `target/release/vekt`.

## Usage

### Initialize

```bash
vekt init
```

Creates a `.vekt` directory for tensor blobs and configuration.

### Archive a Model

```bash
vekt add model.safetensors
vekt add model.safetensors --compress  # with compression
```

Decomposes the model into content-addressed blobs and creates `model.vekt.json` manifest.

### Restore a Model

```bash
vekt restore model.vekt.json
vekt restore model.vekt.json --layers "encoder.*"  # selective
```

### Compare Models

Show differences between two model versions:

```bash
vekt diff old_model.safetensors new_model.safetensors
```

### Remote Storage

```bash
vekt remote add origin s3://your-bucket-name
vekt remote list
vekt push origin  # uploads only missing blobs
vekt pull origin
```

### Status and Cleanup

```bash
vekt status  # show tracked manifests and stats
vekt gc      # remove orphaned blobs
```

## How It Works

vekt uses content-addressable storage (CAS). Each tensor is hashed with BLAKE3, stored once as a blob, and referenced by lightweight JSON manifests.

**Archiving**: Memory-map safetensors → parallel tensor processing → BLAKE3 hashing → atomic blob writes → manifest generation

**Restoration**: Read manifest → lookup blobs → reconstruct safetensors byte-for-byte

**Deduplication**: Identical tensors share the same hash and blob. Automatic.

**Remote sync**: Only transfer missing blobs. Efficient.

## Configuration

AWS credentials via environment variables:

```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_REGION=us-east-1
```

Remotes are stored in `.vekt/config.json`. Use `vekt remote` commands to manage.

## Manifest Format

```json
{
  "version": "0.1.0",
  "total_size": 1234567890,
  "tensors": {
    "layer.weight": {
      "shape": [768, 768],
      "dtype": "F32",
      "hash": "blake3_hash",
      "index": 0
    }
  }
}
```

Lightweight JSON with tensor metadata and hash references. Deterministic ordering makes Git diffs clean.

## Performance

Memory-mapped I/O, parallel processing, atomic writes, streaming uploads. BLAKE3 hashing is typically I/O-bound.

## Architecture

**vekt_core** - Library handling storage, compression, diffing, remote ops, validation

**vekt_cli** - Command-line interface

## License and Contributing

vekt is released under the Apache-2.0 license. It's open source. Contributions are welcome. Found a bug? Have an idea? Open an issue or submit a pull request.

Created by Khushiyant Chauhan.
