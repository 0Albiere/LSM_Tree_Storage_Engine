//! # LSM-Tree Storage Engine
//!
//! A high-performance Log-Structured Merge-Tree storage engine implemented in Rust.
//! This engine supports efficient writes (via WAL and MemTable), persistent storage (SSTables),
//! background compaction, and Bloom filters for optimized lookups.

pub mod bloom;
pub mod compaction;
pub mod engine;
pub mod memtable;
pub mod sstable;
pub mod wal;

pub use engine::Engine;
pub use memtable::MemTable;
