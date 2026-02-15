# LSM-Tree Storage Engine

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Build Status](https://img.shields.io/github/actions/workflow/status/0Albiere/LSM-Tree_Storage_Engine_TEST/rust.yml?branch=main)](https://github.com/0Albiere/LSM-Tree_Storage_Engine_TEST/actions)

A high-performance Log-Structured Merge-Tree (LSM-Tree) storage engine implemented in Rust. Optimized for write-heavy workloads with background compaction and probabilistic read optimization.

## 🚀 Features

- **High-Performance Writes**: Log-structured design with Write-Ahead Log (WAL) and in-memory MemTable.
- **Persistence**: Efficient flushing of MemTables into immutable SSTables (Sorted String Tables).
- **Background Compaction**: Automatic merging of SSTables using a k-way merge algorithm to optimize space and read performance.
- **Read Optimization**: 
    - **Bloom Filters**: Fast membership checks to skip unnecessary disk I/O.
    - **Sparse Index**: Multi-level indexing for logarithmic search time in SSTables.
- **Reliability**: Crash recovery via WAL playback on engine startup.
- **Strict Correctness**: Property-based testing via `proptest` and high-concurrency safety using `parking_lot`.

## 🏗️ Architecture

```mermaid
graph TD
    subgraph MemTable_Layer [Memory Layer]
        WAL[Write-Ahead Log] -- Persistence --> DISK[Disk]
        MT[Active MemTable] -- Flushing --> SSTs
    end

    subgraph Storage_Layer [Storage Layer]
        SSTs[(SSTables L0...LN)]
        BF[Bloom Filters] -- Fast Miss --> GET
        IDX[Sparse Index] -- Fast Hit --> GET
    end

    USER[User API] -- Put/Delete --> WAL
    USER -- Put/Delete --> MT
    USER -- Get --> MT
    USER -- Get Query --> BF
    BF -- Cache Hit? --> IDX
    IDX -- Block Seek --> SSTs
    
    COMP[Compaction Thread] -- Multi-way Merge --> SSTs
```

## 📦 Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
lsm_storage_engine = { git = "https://github.com/0Albiere/LSM-Tree_Storage_Engine_TEST.git" }
```

## 🛠️ Usage

```rust
use lsm_storage_engine::Engine;

fn main() -> std::io::Result<()> {
    // Open the engine (directory, max_memtable_size)
    let engine = Engine::open("./data", 4 * 1024 * 1024)?; // 4MiB threshold
    
    // Insert/Update
    engine.put(b"user:123".to_vec(), b"Albiere".to_vec())?;
    
    // Retrieve
    if let Some(val) = engine.get(b"user:123")? {
        println!("User: {:?}", String::from_utf8(val));
    }
    
    // Delete (inserts a tombstone)
    engine.delete(b"user:123".to_vec())?;
    
    Ok(())
}
```

## 🧪 Testing & Benchmarks

Run the full test suite (including property-based tests):
```bash
cargo test
```

Run performance benchmarks:
```bash
cargo bench
```

## 📜 License

Distributed under the MIT License. See `LICENSE` for more information.
