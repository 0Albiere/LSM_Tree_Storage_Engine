# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-02-15

### Added
- Core LSM-Tree engine with MemTable and SSTables.
- Write-Ahead Log (WAL) for persistent writes.
- Probabilistic read optimization using Bloom Filters.
- Sparse index for efficient SSTable lookups.
- Background compaction using k-way merge.
- Comprehensive unit and integration test suite.
- Minimal runnable example in `examples/basic_usage.rs`.
- GitHub Actions CI workflow for build, fmt, clippy, tests, and benchmarks.

### Changed
- Refactored internal dependencies to use only standard library for maximum compatibility.
- Improved documentation and README aesthetics.
