use crate::memtable::{Entry, MemTable};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Simple CRC32 implementation to avoid external dependencies.
fn crc32_update(mut crc: u32, data: &[u8]) -> u32 {
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xEDB88320;
            } else {
                crc >>= 1;
            }
        }
    }
    crc
}

use crate::bloom::BloomFilter;

/// A builder for creating immutable Sorted String Tables (SSTables).
pub struct SSTableBuilder {
    writer: BufWriter<File>,
    path: PathBuf,
    index: BTreeMap<Vec<u8>, u64>,
    record_count: usize,
    sparse_interval: usize,
    bloom: BloomFilter,
    checksum: u32,
}

impl SSTableBuilder {
    /// Creates a new `SSTableBuilder` at the specified path.
    pub fn new(path: impl AsRef<Path>, sparse_interval: usize) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;

        Ok(Self {
            writer: BufWriter::new(file),
            path,
            index: BTreeMap::new(),
            record_count: 0,
            sparse_interval,
            // Assuming average 1000 items per sstable for default bloom size,
            // but we can adjust this. 1% false positive.
            bloom: BloomFilter::new(1000, 0.01),
            checksum: 0xFFFFFFFF,
        })
    }

    /// Adds a key-value record to the `SSTable`.
    fn write_and_checksum(&mut self, buf: &[u8]) -> io::Result<()> {
        self.writer.write_all(buf)?;
        self.checksum = crc32_update(self.checksum, buf);
        Ok(())
    }

    /// Adds a key-value record to the `SSTable`.
    ///
    /// Records must be added in lexicographical order.
    pub fn add_record(&mut self, key: &[u8], entry: &Entry) -> io::Result<()> {
        let current_offset = self.writer.stream_position()?;

        // Sparse index
        if self.record_count.is_multiple_of(self.sparse_interval) {
            self.index.insert(key.to_vec(), current_offset);
        }

        // Bloom filter
        self.bloom.add(key);

        // Write record
        self.write_and_checksum(&(key.len() as u32).to_le_bytes())?;
        self.write_and_checksum(key)?;

        match entry {
            Entry::Value(v) => {
                self.write_and_checksum(&(v.len() as u32).to_le_bytes())?;
                self.write_and_checksum(v)?;
            }
            Entry::Tombstone => {
                self.write_and_checksum(&u32::MAX.to_le_bytes())?;
            }
        }

        self.record_count += 1;
        Ok(())
    }

    /// Finishes writing the `SSTable` by appending the bloom filter, index, and footer.
    pub fn finish(mut self) -> io::Result<u64> {
        // Write Bloom Filter
        let bloom_offset = self.writer.stream_position()?;
        let bloom_data = self.bloom.serialize();
        self.write_and_checksum(&bloom_data)?;
        let bloom_size = self.writer.stream_position()? - bloom_offset;

        // Write index
        let index_offset = self.writer.stream_position()?;
        let index_items: Vec<(Vec<u8>, u64)> = self.index.iter().map(|(k, v)| (k.clone(), *v)).collect();
        for (key, offset) in index_items {
            self.write_and_checksum(&(key.len() as u32).to_le_bytes())?;
            self.write_and_checksum(&key)?;
            self.write_and_checksum(&offset.to_le_bytes())?;
        }
        let index_size = self.writer.stream_position()? - index_offset;

        // Finalize checksum
        let final_checksum = !self.checksum;

        // Write footer (36 bytes: 8+8+8+8+4)
        self.writer.write_all(&bloom_offset.to_le_bytes())?;
        self.writer.write_all(&bloom_size.to_le_bytes())?;
        self.writer.write_all(&index_offset.to_le_bytes())?;
        self.writer.write_all(&index_size.to_le_bytes())?;
        self.writer.write_all(&final_checksum.to_le_bytes())?;

        self.writer.flush()?;
        Ok(index_offset)
    }

    /// Builds an `SSTable` from a `MemTable`.
    pub fn build(mut self, memtable: &MemTable) -> io::Result<SSTableMetadata> {
        let mut first_key = None;
        let mut last_key = None;

        for (key, entry) in memtable.iter() {
            if first_key.is_none() {
                first_key = Some(key.clone());
            }
            last_key = Some(key.clone());
            self.add_record(key, entry)?;
        }

        let path = self.path.clone();
        self.finish()?;

        Ok(SSTableMetadata {
            path,
            first_key: first_key.unwrap_or_default(),
            last_key: last_key.unwrap_or_default(),
        })
    }
}

/// Metadata for an `SSTable`.
pub struct SSTableMetadata {
    /// Path to the `SSTable` file.
    pub path: PathBuf,
    /// The first key in the table.
    pub first_key: Vec<u8>,
    /// The last key in the table.
    pub last_key: Vec<u8>,
}

/// A reader for Sorted String Tables (SSTables).
pub struct SSTable {
    file: File,
    index: BTreeMap<Vec<u8>, u64>,
    bloom: BloomFilter,
    path: PathBuf,
}

impl SSTable {
    /// Opens an existing `SSTable` file and loads its index and bloom filter.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let mut file = File::open(&path_buf)?;
        let _file_size = file.metadata()?.len();

        // Read footer (last 36 bytes)
        file.seek(SeekFrom::End(-36))?;
        let mut footer = [0u8; 36];
        file.read_exact(&mut footer)?;

        let bloom_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        let bloom_size = u64::from_le_bytes(footer[8..16].try_into().unwrap());
        let index_offset = u64::from_le_bytes(footer[16..24].try_into().unwrap());
        let index_size = u64::from_le_bytes(footer[24..32].try_into().unwrap());
        let expected_checksum = u32::from_le_bytes(footer[32..36].try_into().unwrap());

        // Verify Checksum
        let mut check_file = file.try_clone()?;
        check_file.seek(SeekFrom::Start(0))?;
        let mut hasher = 0xFFFFFFFFu32;
        let mut buffer = [0u8; 8192];
        let mut bytes_to_read = index_offset + index_size; // Records + Bloom + Index
        
        while bytes_to_read > 0 {
            let to_read = std::cmp::min(buffer.len() as u64, bytes_to_read) as usize;
            check_file.read_exact(&mut buffer[..to_read])?;
            hasher = crc32_update(hasher, &buffer[..to_read]);
            bytes_to_read -= to_read as u64;
        }

        if !hasher != expected_checksum {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "SSTable checksum mismatch"));
        }

        // Read Bloom Filter
        file.seek(SeekFrom::Start(bloom_offset))?;
        let mut bloom_data = vec![0u8; bloom_size as usize];
        file.read_exact(&mut bloom_data)?;
        let bloom = BloomFilter::deserialize(&bloom_data);

        // Read index
        file.seek(SeekFrom::Start(index_offset))?;
        let mut index_data = vec![0u8; index_size as usize];
        file.read_exact(&mut index_data)?;

        let mut index = BTreeMap::new();
        let mut cursor = io::Cursor::new(index_data);
        while cursor.position() < index_size {
            let mut len_buf = [0u8; 4];
            cursor.read_exact(&mut len_buf)?;
            let key_len = u32::from_le_bytes(len_buf) as usize;
            let mut key = vec![0u8; key_len];
            cursor.read_exact(&mut key)?;

            let mut offset_buf = [0u8; 8];
            cursor.read_exact(&mut offset_buf)?;
            let offset = u64::from_le_bytes(offset_buf);

            index.insert(key, offset);
        }

        Ok(Self { file, index, bloom, path: path_buf })
    }

    /// Returns the path to the `SSTable` file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Retrieves a value by its key from the `SSTable`.
    ///
    /// Uses the bloom filter and sparse index to minimize disk I/O.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        // 0. Bloom filter check
        if !self.bloom.contains(key) {
            return Ok(None);
        }

        // 1. Find the closest block in sparse index
        let mut range = self.index.range(..=key.to_vec());
        let block_offset = match range.next_back() {
            Some((_, offset)) => *offset,
            None => return Ok(None),
        };

        let file = &self.file;
        let mut block_file = file.try_clone()?;
        block_file.seek(SeekFrom::Start(block_offset))?;
        let mut reader = BufReader::new(block_file);

        loop {
            let mut len_buf = [0u8; 4];
            if reader.read_exact(&mut len_buf).is_err() {
                break;
            }
            let k_len = u32::from_le_bytes(len_buf) as usize;
            let mut k = vec![0u8; k_len];
            reader.read_exact(&mut k)?;

            // Read value len
            let mut v_len_buf = [0u8; 4];
            reader.read_exact(&mut v_len_buf)?;
            let v_len = u32::from_le_bytes(v_len_buf);

            if k == key {
                if v_len == u32::MAX {
                    return Ok(None); // Tombstone
                } else {
                    let mut v = vec![0u8; v_len as usize];
                    reader.read_exact(&mut v)?;
                    return Ok(Some(v));
                }
            } else if k.as_slice() > key {
                break;
            } else {
                // Skip value
                if v_len != u32::MAX {
                    io::copy(&mut reader.by_ref().take(v_len as u64), &mut io::sink())?;
                }
            }
        }
        Ok(None)
    }

    /// Returns an iterator over all records in the `SSTable`.
    pub fn iter(&self) -> io::Result<RecordIterator> {
        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(0))?;

        // Find bloom offset from footer to know where to stop
        file.seek(SeekFrom::End(-36))?;
        let mut footer = [0u8; 8];
        file.read_exact(&mut footer)?;
        let data_end_offset = u64::from_le_bytes(footer);

        file.seek(SeekFrom::Start(0))?;

        Ok(RecordIterator {
            reader: BufReader::new(file),
            data_end_offset,
            current_pos: 0,
        })
    }
}

/// An iterator over records in an `SSTable`.
pub struct RecordIterator {
    reader: BufReader<File>,
    data_end_offset: u64,
    current_pos: u64,
}

impl Iterator for RecordIterator {
    type Item = io::Result<(Vec<u8>, Entry)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_pos >= self.data_end_offset {
            return None;
        }

        let mut len_buf = [0u8; 4];
        if let Err(e) = self.reader.read_exact(&mut len_buf) {
            return Some(Err(e));
        }

        let k_len = u32::from_le_bytes(len_buf) as usize;
        let mut key = vec![0u8; k_len];
        if let Err(e) = self.reader.read_exact(&mut key) {
            return Some(Err(e));
        }

        let mut v_len_buf = [0u8; 4];
        if let Err(e) = self.reader.read_exact(&mut v_len_buf) {
            return Some(Err(e));
        }
        let v_len = u32::from_le_bytes(v_len_buf);

        let entry = if v_len == u32::MAX {
            Entry::Tombstone
        } else {
            let mut val = vec![0u8; v_len as usize];
            if let Err(e) = self.reader.read_exact(&mut val) {
                return Some(Err(e));
            }
            Entry::Value(val)
        };

        self.current_pos += 4 + k_len as u64 + 4 + if v_len == u32::MAX { 0 } else { v_len as u64 };
        Some(Ok((key, entry)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_test_dir(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("lsm_test_{}_{}", name, std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn test_build_and_get() {
        let dir = setup_test_dir("sst_build");
        let path = dir.join("test.sst");
        let mut mt = MemTable::new(1024);
        mt.put(b"k1".to_vec(), b"v1".to_vec());
        mt.put(b"k2".to_vec(), b"v2".to_vec());

        let builder = SSTableBuilder::new(&path, 1).unwrap();
        builder.build(&mt).unwrap();

        let sst = SSTable::open(&path).unwrap();
        assert_eq!(sst.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(sst.get(b"k2").unwrap(), Some(b"v2".to_vec()));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_get_nonexistent() {
        let dir = setup_test_dir("sst_nonexistent");
        let path = dir.join("test.sst");
        let mut mt = MemTable::new(1024);
        mt.put(b"k1".to_vec(), b"v1".to_vec());

        let builder = SSTableBuilder::new(&path, 1).unwrap();
        builder.build(&mt).unwrap();

        let sst = SSTable::open(&path).unwrap();
        assert_eq!(sst.get(b"k2").unwrap(), None);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_index_lookup() {
        let dir = setup_test_dir("sst_index");
        let path = dir.join("test.sst");
        let mut mt = MemTable::new(1024);
        for i in 0..10 {
            mt.put(vec![i as u8], vec![i as u8]);
        }

        let builder = SSTableBuilder::new(&path, 5).unwrap();
        builder.build(&mt).unwrap();

        let sst = SSTable::open(&path).unwrap();
        assert_eq!(sst.get(&[0]).unwrap(), Some(vec![0]));
        assert_eq!(sst.get(&[3]).unwrap(), Some(vec![3]));
        assert_eq!(sst.get(&[9]).unwrap(), Some(vec![9]));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_multiple_blocks() {
        let dir = setup_test_dir("sst_blocks");
        let path = dir.join("test.sst");
        let mut mt = MemTable::new(10000);
        for i in 0..100 {
            mt.put(format!("k{:03}", i).into_bytes(), vec![i as u8; 10]);
        }

        let builder = SSTableBuilder::new(&path, 10).unwrap();
        builder.build(&mt).unwrap();

        let sst = SSTable::open(&path).unwrap();
        assert_eq!(sst.get(b"k050").unwrap(), Some(vec![50; 10]));
        assert_eq!(sst.get(b"k099").unwrap(), Some(vec![99; 10]));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_tombstone_in_sstable() {
        let dir = setup_test_dir("sst_tombstone");
        let path = dir.join("test.sst");
        let mut mt = MemTable::new(1024);
        mt.delete(b"k1".to_vec());

        let builder = SSTableBuilder::new(&path, 1).unwrap();
        builder.build(&mt).unwrap();

        let sst = SSTable::open(&path).unwrap();
        assert_eq!(sst.get(b"k1").unwrap(), None);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_bloom_filter_integration() {
        let dir = setup_test_dir("sst_bloom");
        let path = dir.join("test.sst");
        let mut mt = MemTable::new(1024);
        mt.put(b"exist".to_vec(), b"val".to_vec());

        let builder = SSTableBuilder::new(&path, 1).unwrap();
        builder.build(&mt).unwrap();

        let sst = SSTable::open(&path).unwrap();
        assert!(sst.bloom.contains(b"exist"));
        let _ = std::fs::remove_dir_all(dir);
    }
}
