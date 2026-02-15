# DESIGN: LSM-Tree Storage Engine

This document describes the internal architecture, on-disk format, and engineering trade-offs of the LSM-Tree Storage Engine.

## 🏗️ Architecture Overview

The engine follows a standard Log-Structured Merge-Tree design:

1.  **MemTable**: In-memory skip-list (implemented as `BTreeMap` in Rust for simplicity and performance) that stores recent writes.
2.  **WAL (Write-Ahead Log)**: A sequential file where every `Put` and `Delete` is appended before being applied to the MemTable.
3.  **SSTables (Sorted String Tables)**: Immutable on-disk files containing sorted key-value pairs, flushed from the MemTable.
4.  **Compaction**: A background process that merges multiple SSTables into a single one, removing duplicate keys and tombstones.

## 💾 On-Disk Format (SSTable)

SSTables are stored as `.sst` files with the following layout:

| Section | Description |
| :--- | :--- |
| **Data Block** | Sorted sequence of records: `[Key Len (4B)] [Key] [Value Len (4B)] [Value]` |
| **Bloom Filter** | Serialized probabilistic data structure for fast membership checks. |
| **Sparse Index** | A map of `Key` to `Offset` for every Nth record (specified by `sparse_interval`). |
| **Footer** | 36 bytes fixed-size metadata pointing to the locations of Bloom Filter and Index. |

### Footer Layout (36 Bytes)

All values are Little-Endian.

- `Bloom Filter Offset`: 8 bytes (u64)
- `Bloom Filter Size`: 8 bytes (u64)
- `Index Offset`: 8 bytes (u64)
- `Index Size`: 8 bytes (u64)
- `CRC32 Checksum`: 4 bytes (u32) - Covers Data Block + Bloom Filter + Index.

### Endianness & Types
- All lengths and offsets are stored as **Little-Endian**.
- `u32` for lengths (max 4GB per k/v).
- `u64` for offsets.
- `u32` for checksums.

## 🛡️ Reliability Features

- **Checksums**: Every SSTable contains a CRC32 checksum. Verification is performed on file open.
- **WAL Playback**: On startup, the engine reads the WAL to reconstruct the MemTable state from the last flush.
- **Tombstones**: Deletions are handled by inserting a special "tombstone" record (`Value Len = u32::MAX`).

## ⚙️ Engineering Trade-offs

- **Zero-Dependency**: The project uses only the Rust Standard Library to ensure maximum compatibility across environments (e.g., platforms without a C compiler).
- **No Threading Bloat**: Replaced `parking_lot` with `std::sync` to minimize external overhead.
- **Leveled Compaction (Simplified)**: Currently implements a size-tiered-like compaction where all SSTables are merged once a threshold is reached.
- **Sparse Index**: Instead of a full index, a sparse index is used to trade off disk I/O for memory. Each lookup might involve scanning a small portion of the disk (defined by `sparse_interval`).
